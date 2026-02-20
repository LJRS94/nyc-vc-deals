"""
One-time data quality fix script.
Addresses:
  2a. Re-link orphaned investors to deals
  2b. Normalize category taxonomy (composite → primary)
  2c. Flag low-confidence "other" source deals
  2d. Clean up orphaned records

Usage: python3 fix_data_quality.py
"""

import re
import logging
import sqlite3
from database import get_connection, _normalize_name
from scrapers.utils import normalize_company_name, parse_investors

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def relink_investors(conn):
    """2a. Re-extract investor names from raw_text and link to deals."""
    from database import upsert_investor, link_deal_investor, upsert_firm, link_deal_firm

    # Deals with raw_text but no investor links
    deals = conn.execute("""
        SELECT d.id, d.raw_text, d.company_name, d.source_type
        FROM deals d
        LEFT JOIN deal_investors di ON d.id = di.deal_id
        WHERE d.raw_text IS NOT NULL AND d.raw_text != ''
        GROUP BY d.id
        HAVING COUNT(di.investor_id) = 0
    """).fetchall()

    linked = 0
    for deal in deals:
        raw = deal["raw_text"]
        deal_id = deal["id"]

        # Try structured AlleyWatch fields first
        investors_text = None
        m = re.search(r"Investors?:\s*(.+?)(?:\n|$)", raw, re.I)
        if m:
            investors_text = m.group(1)
        else:
            # Try "led by X" or "from X, Y, Z" patterns
            m = re.search(r"(?:led by|from|backed by|investors? (?:include|including))\s+(.+?)(?:\.\s|$)", raw, re.I)
            if m:
                investors_text = m.group(1)

        if not investors_text:
            continue

        all_investors, lead = parse_investors(investors_text)
        if not all_investors:
            continue

        for inv_name in all_investors:
            inv_name = inv_name.strip()
            if not inv_name or len(inv_name) < 2 or len(inv_name) > 60:
                continue

            # Check if this investor name matches a known firm
            firm_row = conn.execute(
                "SELECT id FROM firms WHERE LOWER(name) = LOWER(?)", (inv_name,)
            ).fetchone()
            firm_id = firm_row["id"] if firm_row else None

            inv_id = upsert_investor(conn, name=inv_name, firm_id=firm_id)
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO deal_investors (deal_id, investor_id) VALUES (?, ?)",
                    (deal_id, inv_id)
                )
            except sqlite3.IntegrityError:
                pass

            if firm_id:
                role = "lead" if (lead and inv_name == lead) else "participant"
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO deal_firms (deal_id, firm_id, role) VALUES (?, ?, ?)",
                        (deal_id, firm_id, role)
                    )
                except sqlite3.IntegrityError:
                    pass

            linked += 1

    conn.commit()
    logger.info(f"Re-linked {linked} investor connections across {len(deals)} unlinked deals")
    return linked


def normalize_categories(conn):
    """2b. Map composite categories (e.g. 'AI/Enterprise') to primary category."""
    # Build a mapping of composite → primary
    category_map = {
        # AI composites
        "AI/Enterprise": "AI / Machine Learning",
        "AI/Fintech": "AI / Machine Learning",
        "AI/Health": "AI / Machine Learning",
        "AI/Cybersecurity": "AI / Machine Learning",
        "AI/HR": "AI / Machine Learning",
        "AI/Climate": "AI / Machine Learning",
        "AI/Legal": "AI / Machine Learning",
        "AI/Education": "AI / Machine Learning",
        "AI/Logistics": "AI / Machine Learning",
        # Fintech composites
        "Fintech/Crypto": "Fintech",
        "Fintech/Insurance": "Fintech",
        # Other composites
        "Health/AI": "Health & Biotech",
        "SaaS/AI": "SaaS / Enterprise",
        "Enterprise/AI": "SaaS / Enterprise",
        "Climate/Energy": "Climate / Cleantech",
        "Crypto/DeFi": "Web3 / Crypto",
    }

    # Find all composite categories (containing / but not our canonical ones)
    canonical = {
        "SaaS / Enterprise", "AI / Machine Learning", "Consumer / D2C",
        "Real Estate / Proptech", "Climate / Cleantech", "Media & Entertainment",
        "Food & Agriculture", "Hardware / Robotics", "Web3 / Crypto",
        "Logistics / Supply Chain", "Education / Edtech", "Insurance / Insurtech",
        "HR / Future of Work", "Health & Biotech",
    }

    composites = conn.execute(
        "SELECT id, name FROM categories WHERE name LIKE '%/%' OR name LIKE '%|%'"
    ).fetchall()

    updated = 0
    for cat in composites:
        cat_name = cat["name"]
        if cat_name in canonical:
            continue  # already a canonical category

        # Try exact map
        target_name = category_map.get(cat_name)

        # Fallback: use the first part before /
        if not target_name:
            first_part = cat_name.split("/")[0].strip()
            # Find best matching canonical category
            for canon in canonical:
                if first_part.lower() in canon.lower():
                    target_name = canon
                    break

        if not target_name:
            target_name = "Other"

        # Get target category id
        target = conn.execute(
            "SELECT id FROM categories WHERE name = ?", (target_name,)
        ).fetchone()
        if not target:
            continue

        # Remap deals from composite to primary
        moved = conn.execute(
            "UPDATE deals SET category_id = ? WHERE category_id = ?",
            (target["id"], cat["id"])
        ).rowcount
        updated += moved

        # Delete the composite category if no longer referenced
        remaining = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE category_id = ?", (cat["id"],)
        ).fetchone()[0]
        if remaining == 0:
            conn.execute("DELETE FROM categories WHERE id = ?", (cat["id"],))

    conn.commit()
    logger.info(f"Normalized {updated} deals from composite categories")
    return updated


def cleanup_orphans(conn):
    """2d. Delete investors and firms not linked to any deal."""
    # Orphaned investors (not in deal_investors)
    orphan_inv = conn.execute("""
        DELETE FROM investors
        WHERE id NOT IN (SELECT DISTINCT investor_id FROM deal_investors)
    """).rowcount

    # Orphaned firms (not in deal_firms, not referenced by investors or portfolio_companies)
    orphan_firm = conn.execute("""
        DELETE FROM firms
        WHERE id NOT IN (SELECT DISTINCT firm_id FROM deal_firms)
          AND id NOT IN (SELECT DISTINCT firm_id FROM investors WHERE firm_id IS NOT NULL)
          AND id NOT IN (SELECT DISTINCT firm_id FROM portfolio_companies)
    """).rowcount

    conn.commit()
    logger.info(f"Cleaned up {orphan_inv} orphaned investors, {orphan_firm} orphaned firms")
    return orphan_inv, orphan_firm


def report_low_confidence(conn):
    """2c. Report low-confidence 'other' source deals (informational)."""
    rows = conn.execute("""
        SELECT COUNT(*) as cnt, AVG(confidence_score) as avg_conf
        FROM deals WHERE source_type = 'other' AND confidence_score < 0.6
    """).fetchone()
    count = rows["cnt"]
    avg = rows["avg_conf"]
    if count:
        logger.info(f"Low-confidence 'other' deals: {count} with avg confidence {avg:.2f}")
        logger.info("These will be flagged in the API via confidence_score < 0.6 filter")
    return count


def main():
    conn = get_connection()

    # Pre-stats
    total_deals = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
    linked_before = conn.execute("SELECT COUNT(DISTINCT deal_id) FROM deal_investors").fetchone()[0]
    composites_before = conn.execute(
        "SELECT COUNT(*) FROM categories WHERE name LIKE '%/%' OR name LIKE '%|%'"
    ).fetchone()[0]

    logger.info(f"=== Data Quality Fix: {total_deals} deals ===")
    logger.info(f"Deals with investor links before: {linked_before}")
    logger.info(f"Composite categories before: {composites_before}")

    # Run fixes
    relink_investors(conn)
    normalize_categories(conn)
    report_low_confidence(conn)
    cleanup_orphans(conn)

    # Post-stats
    linked_after = conn.execute("SELECT COUNT(DISTINCT deal_id) FROM deal_investors").fetchone()[0]
    composites_after = conn.execute(
        "SELECT COUNT(*) FROM categories WHERE name LIKE '%/%' OR name LIKE '%|%'"
    ).fetchone()[0]

    logger.info(f"=== Done ===")
    logger.info(f"Deals with investor links: {linked_before} → {linked_after}")
    logger.info(f"Composite categories: {composites_before} → {composites_after}")


if __name__ == "__main__":
    main()
