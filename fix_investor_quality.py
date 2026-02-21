"""
One-time backfill script to improve investor data quality.

Functions:
  1. relink_orphaned_to_firms()  — fuzzy-match orphaned investors to firms
  2. merge_duplicate_investors() — merge investors with same normalized name + firm
  3. backfill_deal_firm_links()  — create missing deal_firms rows from deal_investors
  4. report()                    — print before/after stats

Usage:
  python fix_investor_quality.py --dry-run   # preview changes
  python fix_investor_quality.py             # execute changes
"""

import argparse
import logging
import sys

from database import get_connection, _normalize_name
from scrapers.utils import company_names_match
from scrapers.news_scraper import _clean_investor_name, _is_valid_investor_name

logger = logging.getLogger(__name__)


def _stats(conn) -> dict:
    """Collect key quality metrics."""
    total_deals = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
    deals_with_investors = conn.execute(
        "SELECT COUNT(DISTINCT deal_id) FROM deal_investors"
    ).fetchone()[0]
    deals_with_firms = conn.execute(
        "SELECT COUNT(DISTINCT deal_id) FROM deal_firms"
    ).fetchone()[0]
    total_investors = conn.execute("SELECT COUNT(*) FROM investors").fetchone()[0]
    orphaned_investors = conn.execute(
        "SELECT COUNT(*) FROM investors WHERE firm_id IS NULL"
    ).fetchone()[0]
    total_firms = conn.execute("SELECT COUNT(*) FROM firms").fetchone()[0]
    return {
        "total_deals": total_deals,
        "deals_with_investors": deals_with_investors,
        "deals_with_firms": deals_with_firms,
        "total_investors": total_investors,
        "orphaned_investors": orphaned_investors,
        "total_firms": total_firms,
    }


def purge_junk_investors(conn, dry_run: bool = True) -> int:
    """Delete investor records whose names fail validation (sentence fragments, garbage)."""
    all_investors = conn.execute("SELECT id, name FROM investors").fetchall()

    purged = 0
    for inv in all_investors:
        cleaned = _clean_investor_name(inv["name"])
        if not _is_valid_investor_name(cleaned):
            if dry_run:
                logger.info(f"  [DRY] Would purge junk investor '{inv['name']}'")
            else:
                conn.execute("DELETE FROM deal_investors WHERE investor_id = ?", (inv["id"],))
                conn.execute("UPDATE deals SET lead_investor_id = NULL WHERE lead_investor_id = ?", (inv["id"],))
                conn.execute("DELETE FROM investors WHERE id = ?", (inv["id"],))
            purged += 1

    if not dry_run and purged:
        conn.commit()
    logger.info(f"purge_junk_investors: {purged} junk records {'would be' if dry_run else ''} purged")
    return purged


def relink_orphaned_to_firms(conn, dry_run: bool = True) -> int:
    """For investors with firm_id=NULL, clean names then fuzzy-match against firms table."""
    all_firms = conn.execute("SELECT id, name FROM firms").fetchall()
    orphans = conn.execute(
        "SELECT id, name FROM investors WHERE firm_id IS NULL"
    ).fetchall()

    linked = 0
    for inv in orphans:
        # Clean the investor name before matching
        cleaned = _clean_investor_name(inv["name"])
        if not _is_valid_investor_name(cleaned):
            continue

        # Try fuzzy match with cleaned name
        for firm in all_firms:
            if company_names_match(cleaned, firm["name"]):
                # Check if an investor with the cleaned name + firm already exists
                existing = conn.execute(
                    "SELECT id FROM investors WHERE name = ? AND firm_id IS ?",
                    (cleaned, firm["id"])
                ).fetchone()
                if dry_run:
                    action = "merge into" if existing else "link"
                    logger.info(f"  [DRY] Would {action} investor '{inv['name']}' (cleaned: '{cleaned}') -> firm '{firm['name']}'")
                else:
                    if existing:
                        # Merge: re-point deal links to existing, delete orphan
                        keeper_id = existing["id"]
                        conn.execute(
                            "UPDATE OR IGNORE deal_investors SET investor_id = ? WHERE investor_id = ?",
                            (keeper_id, inv["id"])
                        )
                        conn.execute("DELETE FROM deal_investors WHERE investor_id = ?", (inv["id"],))
                        conn.execute(
                            "UPDATE deals SET lead_investor_id = ? WHERE lead_investor_id = ?",
                            (keeper_id, inv["id"])
                        )
                        conn.execute("DELETE FROM investors WHERE id = ?", (inv["id"],))
                    else:
                        # Update in place
                        conn.execute(
                            "UPDATE investors SET firm_id = ?, name = ?, name_normalized = ? WHERE id = ?",
                            (firm["id"], cleaned, _normalize_name(cleaned), inv["id"])
                        )
                linked += 1
                break

    if not dry_run and linked:
        conn.commit()
    logger.info(f"relink_orphaned_to_firms: {linked} investors {'would be' if dry_run else ''} linked")
    return linked


def merge_duplicate_investors(conn, dry_run: bool = True) -> int:
    """Group by name_normalized + firm_id, keep the one with most deal links."""
    # Ensure all rows have name_normalized
    missing = conn.execute(
        "SELECT id, name FROM investors WHERE name_normalized IS NULL"
    ).fetchall()
    if missing and not dry_run:
        for row in missing:
            conn.execute(
                "UPDATE investors SET name_normalized = ? WHERE id = ?",
                (_normalize_name(row["name"]), row["id"])
            )
        conn.commit()
        logger.info(f"Backfilled name_normalized for {len(missing)} investors")

    # Find duplicate groups
    dupes = conn.execute("""
        SELECT name_normalized, firm_id, GROUP_CONCAT(id) as ids, COUNT(*) as cnt
        FROM investors
        WHERE name_normalized IS NOT NULL AND name_normalized != ''
        GROUP BY name_normalized, firm_id
        HAVING cnt > 1
    """).fetchall()

    merged = 0
    for group in dupes:
        ids = [int(x) for x in group["ids"].split(",")]

        # Pick the keeper: the one with the most deal_investors links
        best_id = ids[0]
        best_count = 0
        for inv_id in ids:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM deal_investors WHERE investor_id = ?",
                (inv_id,)
            ).fetchone()[0]
            if cnt > best_count:
                best_count = cnt
                best_id = inv_id

        others = [i for i in ids if i != best_id]
        if dry_run:
            logger.info(
                f"  [DRY] Would merge investors {others} into {best_id} "
                f"(norm='{group['name_normalized']}', firm_id={group['firm_id']})"
            )
        else:
            for old_id in others:
                # Re-point deal_investors (ignore conflicts — keeper may already be linked)
                conn.execute(
                    "UPDATE OR IGNORE deal_investors SET investor_id = ? WHERE investor_id = ?",
                    (best_id, old_id)
                )
                # Clean up any remaining rows that conflicted
                conn.execute(
                    "DELETE FROM deal_investors WHERE investor_id = ?",
                    (old_id,)
                )
                # Update lead_investor_id references on deals
                conn.execute(
                    "UPDATE deals SET lead_investor_id = ? WHERE lead_investor_id = ?",
                    (best_id, old_id)
                )
                # Delete the duplicate investor
                conn.execute("DELETE FROM investors WHERE id = ?", (old_id,))
        merged += len(others)

    if not dry_run and merged:
        conn.commit()
    logger.info(f"merge_duplicate_investors: {merged} duplicates {'would be' if dry_run else ''} merged")
    return merged


def backfill_deal_firm_links(conn, dry_run: bool = True) -> int:
    """For deals with deal_investors but no deal_firms, create missing firm links."""
    # Find deals that have investors but no firm links
    deals = conn.execute("""
        SELECT DISTINCT di.deal_id
        FROM deal_investors di
        LEFT JOIN deal_firms df ON di.deal_id = df.deal_id
        WHERE df.deal_id IS NULL
    """).fetchall()

    all_firms = conn.execute("SELECT id, name FROM firms").fetchall()
    created = 0

    for row in deals:
        deal_id = row["deal_id"]
        # Get investors for this deal
        investors = conn.execute("""
            SELECT i.name, i.firm_id
            FROM deal_investors di
            JOIN investors i ON di.investor_id = i.id
            WHERE di.deal_id = ?
        """, (deal_id,)).fetchall()

        for inv in investors:
            firm_id = inv["firm_id"]
            # If investor already has a firm, link it
            if firm_id:
                if dry_run:
                    logger.info(f"  [DRY] Would link deal {deal_id} -> firm {firm_id}")
                else:
                    conn.execute(
                        "INSERT OR IGNORE INTO deal_firms (deal_id, firm_id, role) VALUES (?, ?, 'participant')",
                        (deal_id, firm_id)
                    )
                created += 1
                continue

            # Try fuzzy matching cleaned investor name against firms
            cleaned = _clean_investor_name(inv["name"])
            if not _is_valid_investor_name(cleaned):
                continue
            for firm in all_firms:
                if company_names_match(cleaned, firm["name"]):
                    if dry_run:
                        logger.info(
                            f"  [DRY] Would link deal {deal_id} -> firm '{firm['name']}' "
                            f"(fuzzy match for investor '{cleaned}')"
                        )
                    else:
                        conn.execute(
                            "INSERT OR IGNORE INTO deal_firms (deal_id, firm_id, role) VALUES (?, ?, 'participant')",
                            (deal_id, firm["id"])
                        )
                    created += 1
                    break

    if not dry_run and created:
        conn.commit()
    logger.info(f"backfill_deal_firm_links: {created} links {'would be' if dry_run else ''} created")
    return created


def report(before: dict, after: dict):
    """Print before/after stats."""
    print("\n" + "=" * 60)
    print("Investor Data Quality Report")
    print("=" * 60)
    fmt = "  {:<30s} {:>8s} -> {:>8s}"
    print(fmt.format("Metric", "Before", "After"))
    print("  " + "-" * 50)
    for key in before:
        label = key.replace("_", " ").title()
        print(fmt.format(label, str(before[key]), str(after[key])))
    td = before["total_deals"] or 1
    print()
    print(f"  Deals with investor links: {before['deals_with_investors']*100//td}% -> {after['deals_with_investors']*100//td}%")
    print(f"  Deals with firm links:     {before['deals_with_firms']*100//td}% -> {after['deals_with_firms']*100//td}%")
    oi_b = before["orphaned_investors"] * 100 // max(before["total_investors"], 1)
    oi_a = after["orphaned_investors"] * 100 // max(after["total_investors"], 1)
    print(f"  Orphaned investors:        {oi_b}% -> {oi_a}%")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Fix investor data quality")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    conn = get_connection()
    before = _stats(conn)

    print(f"{'DRY RUN — no changes will be written' if args.dry_run else 'LIVE RUN — writing changes'}")
    print(f"Database: {before['total_deals']} deals, {before['total_investors']} investors, {before['total_firms']} firms")
    print()

    purge_junk_investors(conn, dry_run=args.dry_run)
    relink_orphaned_to_firms(conn, dry_run=args.dry_run)
    merge_duplicate_investors(conn, dry_run=args.dry_run)
    backfill_deal_firm_links(conn, dry_run=args.dry_run)

    after = _stats(conn) if not args.dry_run else before
    report(before, after)


if __name__ == "__main__":
    main()
