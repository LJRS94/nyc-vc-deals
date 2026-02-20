"""
NYC VC Deal Scraper — Main Orchestrator
Coordinates all scrapers and runs on a bi-weekly schedule.
"""

import os
import sys
import json
import logging
import argparse
import schedule
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scrapers"))

from database import init_db, get_connection, migrate_db
from fetcher import clear_cache
from scrapers.firm_scraper import seed_firms, run_firm_scraper, run_team_scraper
from scrapers.news_scraper import run_news_scraper, run_google_batch
from scrapers.sec_scraper import run_sec_scraper
from scrapers.delaware_scraper import run_delaware_scraper
from scrapers.alleywatch_scraper import run_alleywatch_scraper
from scrapers.llm_extract import extract_deal_from_text, validate_company_name, clean_company_name
from scrapers.enrichment import run_web_enrichment
from scrapers.additional_sources import run_additional_sources

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log"),
    ]
)
logger = logging.getLogger("orchestrator")


def run_full_scrape(days_back=14):
    """Run all scrapers concurrently via ThreadPoolExecutor."""
    logger.info("=" * 60)
    logger.info(f"Starting full scrape at {datetime.now().isoformat()}")
    logger.info("=" * 60)

    start = time.time()
    errors = []

    scrapers = {
        "AlleyWatch":  lambda: run_alleywatch_scraper(days_back=days_back),
        "News":        lambda: run_news_scraper(days_back=days_back),
        "SEC EDGAR":   lambda: run_sec_scraper(days_back=days_back),
        "Delaware":    lambda: run_delaware_scraper(days_back=days_back),
        "Firm Sites":  lambda: run_firm_scraper(),
    }

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {}
        for name, fn in scrapers.items():
            logger.info(f"── Launching {name} scraper ──")
            futures[pool.submit(fn)] = name

        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
                logger.info(f"── {name} scraper finished ──")
            except Exception as e:
                logger.error(f"{name} scraper failed: {e}")
                errors.append(f"{name.lower()}: {e}")

    elapsed = time.time() - start
    logger.info(f"Full scrape completed in {elapsed:.1f}s with {len(errors)} errors")

    # Print summary
    print_summary()

    return errors


def print_summary():
    """Print a summary of the current database state."""
    conn = get_connection()
    stats = {
        "total_deals": conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0],
        "total_firms": conn.execute("SELECT COUNT(*) FROM firms").fetchone()[0],
        "total_investors": conn.execute("SELECT COUNT(*) FROM investors").fetchone()[0],
        "by_stage": {},
        "by_category": {},
        "recent_deals": [],
    }

    # By stage
    rows = conn.execute(
        "SELECT stage, COUNT(*) as cnt FROM deals GROUP BY stage ORDER BY cnt DESC"
    ).fetchall()
    for row in rows:
        stats["by_stage"][row["stage"]] = row["cnt"]

    # By category
    rows = conn.execute("""
        SELECT c.name, COUNT(*) as cnt
        FROM deals d
        JOIN categories c ON d.category_id = c.id
        GROUP BY c.name ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()
    for row in rows:
        stats["by_category"][row["name"]] = row["cnt"]

    # Recent deals
    rows = conn.execute("""
        SELECT company_name, stage, amount_usd, date_announced, source_type
        FROM deals ORDER BY created_at DESC LIMIT 10
    """).fetchall()
    for row in rows:
        stats["recent_deals"].append({
            "company": row["company_name"],
            "stage": row["stage"],
            "amount": row["amount_usd"],
            "date": row["date_announced"],
            "source": row["source_type"],
        })

    logger.info("\n" + "=" * 50)
    logger.info("DATABASE SUMMARY")
    logger.info("=" * 50)
    logger.info(f"  Deals:     {stats['total_deals']}")
    logger.info(f"  Firms:     {stats['total_firms']}")
    logger.info(f"  Investors: {stats['total_investors']}")
    logger.info(f"\n  By Stage:")
    for stage, count in stats["by_stage"].items():
        logger.info(f"    {stage}: {count}")
    logger.info(f"\n  Top Categories:")
    for cat, count in stats["by_category"].items():
        logger.info(f"    {cat}: {count}")
    if stats["recent_deals"]:
        logger.info(f"\n  Recent Deals:")
        for deal in stats["recent_deals"][:5]:
            amt = f"${deal['amount']:,.0f}" if deal["amount"] else "Undisclosed"
            logger.info(f"    {deal['company']} — {deal['stage']} — {amt}")


def setup_schedule():
    """Set up bi-weekly scraping schedule."""
    # Run every other Monday at 6 AM
    schedule.every(2).weeks.at("06:00").do(run_full_scrape)
    logger.info("Scheduler started: running every 2 weeks on Monday at 06:00")
    logger.info("Press Ctrl+C to stop")

    while True:
        schedule.run_pending()
        time.sleep(60)


def export_csv(output_path: str = "nyc_vc_deals_export.csv"):
    """Export all deals to CSV."""
    import csv
    conn = get_connection()
    rows = conn.execute("""
        SELECT
            d.company_name,
            d.stage,
            d.amount_usd,
            d.date_announced,
            d.source_type,
            d.source_url,
            d.confidence_score,
            c.name as category,
            GROUP_CONCAT(DISTINCT f.name) as firms,
            GROUP_CONCAT(DISTINCT i.name) as investors
        FROM deals d
        LEFT JOIN categories c ON d.category_id = c.id
        LEFT JOIN deal_firms df ON d.id = df.deal_id
        LEFT JOIN firms f ON df.firm_id = f.id
        LEFT JOIN deal_investors di ON d.id = di.deal_id
        LEFT JOIN investors i ON di.investor_id = i.id
        GROUP BY d.id
        ORDER BY d.date_announced DESC
    """).fetchall()

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Company", "Stage", "Amount (USD)", "Date Announced",
            "Source Type", "Source URL", "Confidence", "Category",
            "Firms", "Investors"
        ])
        for row in rows:
            writer.writerow([
                row["company_name"], row["stage"], row["amount_usd"],
                row["date_announced"], row["source_type"], row["source_url"],
                row["confidence_score"], row["category"],
                row["firms"], row["investors"]
            ])

    logger.info(f"Exported {len(rows)} deals to {output_path}")


def export_json(output_path: str = "nyc_vc_deals_export.json"):
    """Export all deals to JSON."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT
            d.id, d.company_name, d.company_website, d.company_description,
            d.stage, d.amount_usd, d.amount_disclosed,
            d.date_announced, d.date_closed,
            d.source_type, d.source_url, d.confidence_score,
            c.name as category
        FROM deals d
        LEFT JOIN categories c ON d.category_id = c.id
        ORDER BY d.date_announced DESC
    """).fetchall()

    deals = []
    for row in rows:
        deal_id = row["id"]

        # Get firms
        firms = conn.execute("""
            SELECT f.name, df.role
            FROM deal_firms df JOIN firms f ON df.firm_id = f.id
            WHERE df.deal_id = ?
        """, (deal_id,)).fetchall()

        # Get investors
        investors = conn.execute("""
            SELECT i.name, i.title
            FROM deal_investors di JOIN investors i ON di.investor_id = i.id
            WHERE di.deal_id = ?
        """, (deal_id,)).fetchall()

        deals.append({
            "id": deal_id,
            "company_name": row["company_name"],
            "company_website": row["company_website"],
            "description": row["company_description"],
            "stage": row["stage"],
            "amount_usd": row["amount_usd"],
            "amount_disclosed": bool(row["amount_disclosed"]),
            "date_announced": row["date_announced"],
            "category": row["category"],
            "source_type": row["source_type"],
            "source_url": row["source_url"],
            "confidence_score": row["confidence_score"],
            "firms": [{"name": f["name"], "role": f["role"]} for f in firms],
            "investors": [{"name": i["name"], "title": i["title"]} for i in investors],
        })

    with open(output_path, "w") as f:
        json.dump({"deals": deals, "exported_at": datetime.now().isoformat()}, f, indent=2)

    logger.info(f"Exported {len(deals)} deals to {output_path}")


def enrich_deals(limit: int = 200):
    """
    Backfill deals that have empty company_description.
    Sends raw_text through LLM and updates name/description/stage.
    """
    conn = get_connection()
    rows = conn.execute(
        """SELECT id, company_name, stage, raw_text, source_url
           FROM deals
           WHERE (company_description IS NULL OR company_description = '')
             AND raw_text IS NOT NULL AND raw_text != ''
           ORDER BY created_at DESC
           LIMIT ?""",
        (limit,)
    ).fetchall()

    if not rows:
        logger.info("No deals need enrichment")
        return

    logger.info(f"Enriching {len(rows)} deals via LLM...")
    updated = 0

    for row in rows:
        deal_id = row["id"]
        company_name = row["company_name"]
        raw_text = row["raw_text"]
        current_stage = row["stage"]

        result = extract_deal_from_text(company_name, raw_text)
        if not result:
            continue

        # Build update fields
        updates = {}

        # Update description
        desc = result.get("description")
        if desc and len(desc) > 10:
            updates["company_description"] = desc[:500]

        # Update company name if LLM gives a cleaner one
        llm_name = result.get("company_name")
        if llm_name:
            llm_name = clean_company_name(llm_name)
            if llm_name and validate_company_name(llm_name) and len(llm_name) < len(company_name):
                updates["company_name"] = llm_name

        # Update stage if currently Unknown
        llm_stage = result.get("stage")
        if current_stage == "Unknown" and llm_stage and llm_stage != "Unknown":
            if llm_stage in ("Pre-Seed", "Seed", "Series A", "Series B", "Series C+"):
                updates["stage"] = llm_stage

        if not updates:
            continue

        # Apply updates
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [deal_id]
        conn2 = get_connection()
        conn2.execute(
            f"UPDATE deals SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            values
        )
        conn2.commit()
        updated += 1

        changes = ", ".join(f"{k}={v!r:.40}" for k, v in updates.items())
        logger.info(f"  Enriched #{deal_id} {company_name}: {changes}")

    logger.info(f"Enrichment complete: {updated}/{len(rows)} deals updated")


FIRMS_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), "firms.json")

# Junk patterns — these are parsing artifacts, not real firm names
_JUNK_PATTERNS = [
    lambda n: len(n) < 4,
    lambda n: n.lower() in {"ceo", "cto", "cfo", "coo", "partner", "director", "managing director"},
    lambda n: n.lower().startswith("and "),
    lambda n: n.lower().startswith("co-founder"),
    lambda n: n.lower().startswith("founder"),
    lambda n: ";" in n,
    lambda n: "managing director" in n.lower(),
    lambda n: " - TechCrunch" in n or " - Crunchbase" in n,
    lambda n: any(t in n.lower() for t in ["co-founder of", "founder of", "ceo of"]),
]


def _is_junk_firm(name: str) -> bool:
    return any(test(name) for test in _JUNK_PATTERNS)


def _clean_firm_name(name: str) -> str:
    """Strip common parsing suffixes."""
    for suffix in [" - TechCrunch", " - Crunchbase", " - VentureBeat"]:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    name = name.strip().strip(",").strip(";").strip()
    if name.lower().startswith("and "):
        name = name[4:].strip()
    return name


def discover_firms(promote: bool = False):
    """Find VC firms mentioned in deals that aren't in the seed list."""
    from scrapers.firm_scraper import NYC_VC_FIRMS

    conn = get_connection()
    seed_names = {f["name"].lower() for f in NYC_VC_FIRMS}

    # Also load any already-promoted firms from firms.json
    promoted = []
    if os.path.exists(FIRMS_JSON):
        with open(FIRMS_JSON) as f:
            promoted = json.load(f)
    promoted_names = {f["name"].lower() for f in promoted}

    # Query all firms from deal_firms that aren't seeded or already promoted
    all_firms = conn.execute("""
        SELECT f.name, COUNT(DISTINCT df.deal_id) as deal_count,
               GROUP_CONCAT(DISTINCT d.stage) as stages
        FROM firms f
        JOIN deal_firms df ON f.id = df.firm_id
        JOIN deals d ON df.deal_id = d.id
        GROUP BY f.name
        ORDER BY deal_count DESC
    """).fetchall()

    candidates = []
    for row in all_firms:
        raw_name = row["name"]
        clean = _clean_firm_name(raw_name)
        if clean.lower() in seed_names or clean.lower() in promoted_names:
            continue
        if _is_junk_firm(raw_name) or _is_junk_firm(clean):
            continue
        candidates.append({
            "name": clean,
            "deal_count": row["deal_count"],
            "stages": row["stages"],
        })

    if not candidates:
        print("No new firms discovered.")
        return

    # Preview
    print(f"\n{'Name':<40} {'Deals':>5}  Stages")
    print("-" * 70)
    for c in candidates:
        print(f"{c['name']:<40} {c['deal_count']:>5}  {c['stages'] or ''}")
    print(f"\n{len(candidates)} candidate firm(s) found.\n")

    if not promote:
        print("Run with --promote to add these to firms.json")
        return

    # Promote: append to firms.json
    for c in candidates:
        promoted.append({
            "name": c["name"],
            "website": None,
            "portfolio_url": None,
            "focus_stages": json.dumps(sorted(set(
                s.strip() for s in (c["stages"] or "").split(",") if s.strip()
            ))),
            "focus_sectors": "[]",
        })

    with open(FIRMS_JSON, "w") as f:
        json.dump(promoted, f, indent=2)
    print(f"✓ Wrote {len(promoted)} firms to {FIRMS_JSON}")

    # Also seed them into the DB
    from scrapers.firm_scraper import seed_firms as _seed
    from database import batch_connection, upsert_firm
    with batch_connection() as conn:
        for firm in candidates:
            upsert_firm(conn, firm["name"])
    print(f"✓ {len(candidates)} new firm(s) added to database")


def backfill_investors_from_metadata(dry_run: bool = False) -> dict:
    """
    Backfill investor records from existing deal_metadata.
    Scans for 'crunchbase_investors' metadata keys and creates proper
    investor records + deal-investor links.
    """
    from database import upsert_investor, link_deal_investor

    stats = {"deals_scanned": 0, "investors_created": 0, "links_created": 0}
    conn = get_connection()

    rows = conn.execute(
        "SELECT deal_id, value FROM deal_metadata WHERE key = 'crunchbase_investors'"
    ).fetchall()

    logger.info(f"[Backfill] Found {len(rows)} deals with crunchbase_investors metadata (dry_run={dry_run})")

    for row in rows:
        deal_id = row["deal_id"]
        stats["deals_scanned"] += 1

        try:
            investor_names = json.loads(row["value"])
        except (json.JSONDecodeError, TypeError):
            continue

        if not isinstance(investor_names, list):
            continue

        for inv_name in investor_names:
            if not inv_name or not isinstance(inv_name, str):
                continue
            inv_name = inv_name.strip()
            if not inv_name:
                continue

            if dry_run:
                logger.info(f"  [DRY RUN] Would create investor '{inv_name}' for deal {deal_id}")
                stats["investors_created"] += 1
                continue

            try:
                # Check if this investor name matches a known firm
                firm_row = conn.execute(
                    "SELECT id FROM firms WHERE LOWER(name) = LOWER(?)",
                    (inv_name,)
                ).fetchone()
                firm_id = firm_row["id"] if firm_row else None

                inv_id = upsert_investor(conn, name=inv_name, firm_id=firm_id)
                stats["investors_created"] += 1

                link_deal_investor(conn, deal_id, inv_id)
                stats["links_created"] += 1
            except Exception as e:
                logger.debug(f"[Backfill] Failed to create investor '{inv_name}': {e}")

    if not dry_run:
        conn.commit()

    logger.info(
        f"[Backfill] Done: {stats['deals_scanned']} deals scanned, "
        f"{stats['investors_created']} investors created, "
        f"{stats['links_created']} links created"
    )
    return stats


def main():
    parser = argparse.ArgumentParser(description="NYC VC Deal Scraper & Organizer")
    sub = parser.add_subparsers(dest="command")

    # Top-level commands (backward compat)
    sub.add_parser("init", help="Initialize database and seed firms")
    sub.add_parser("migrate", help="Run database migrations")
    sub.add_parser("seed", help="Re-seed firms from registry")
    sub.add_parser("summary", help="Print database summary")
    sub.add_parser("schedule", help="Start bi-weekly scrape scheduler")

    scrape_p = sub.add_parser("scrape", help="Run all scrapers")
    scrape_p.add_argument("--days", type=int, default=14, help="Days to look back")
    scrape_p.add_argument("--clear-cache", action="store_true",
                          help="Clear HTTP cache before scraping")

    csv_p = sub.add_parser("export-csv", help="Export deals to CSV")
    csv_p.add_argument("--output", "-o", default="nyc_vc_deals_export.csv")

    json_p = sub.add_parser("export-json", help="Export deals to JSON")
    json_p.add_argument("--output", "-o", default="nyc_vc_deals_export.json")

    batch_p = sub.add_parser("scrape-batch", help="Run a small batch of Google News queries (for cron)")
    batch_p.add_argument("--size", type=int, default=15, help="Number of queries per batch")
    batch_p.add_argument("--days", type=int, default=450, help="Days to look back")

    enrich_p = sub.add_parser("enrich", help="Backfill descriptions via LLM")
    enrich_p.add_argument("--limit", type=int, default=200, help="Max deals to enrich")

    web_p = sub.add_parser("enrich-web", help="Enrich deals via Google CSE + Apollo.io")
    web_p.add_argument("--google-limit", type=int, default=95, help="Max Google CSE queries (default 95)")
    web_p.add_argument("--apollo-limit", type=int, default=95, help="Max Apollo enrichments (default 95)")
    web_p.add_argument("--skip-google", action="store_true", help="Skip Google CSE website lookup")
    web_p.add_argument("--skip-apollo", action="store_true", help="Skip Apollo.io org enrichment")
    web_p.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")

    extra_p = sub.add_parser("scrape-extra", help="Run additional data source scrapers")
    extra_p.add_argument("--days", type=int, default=30, help="Days to look back")
    extra_p.add_argument("--skip", nargs="*", default=[], help="Sources to skip (opencorporates crunchbase ny_dos sbir)")
    extra_p.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")

    firms_p = sub.add_parser("firms", help="Firm management")
    firms_sub = firms_p.add_subparsers(dest="firms_cmd")
    discover_p = firms_sub.add_parser("discover", help="Find new firms from scraped deals")
    discover_p.add_argument("--promote", action="store_true",
                            help="Add discovered firms to firms.json")

    team_p = sub.add_parser("scrape-team", help="Scrape firm team pages for investor names")
    team_p.add_argument("--limit", type=int, default=None, help="Max firms to scrape")
    team_p.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")

    backfill_p = sub.add_parser("backfill-investors", help="Backfill investor records from deal metadata")
    backfill_p.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    if args.command == "init":
        init_db()
        seed_firms()
        print("✓ Database initialized and firms seeded")

    elif args.command == "migrate":
        migrate_db()
        print("✓ Database migrated")

    elif args.command == "seed":
        seed_firms()
        print("✓ Firms seeded")

    elif args.command == "scrape":
        if args.clear_cache:
            clear_cache()
            print("✓ HTTP cache cleared")
        run_full_scrape(days_back=args.days)

    elif args.command == "scrape-batch":
        run_google_batch(batch_size=args.size, days_back=args.days)

    elif args.command == "schedule":
        run_full_scrape()
        setup_schedule()

    elif args.command == "summary":
        print_summary()

    elif args.command == "export-csv":
        export_csv(args.output)

    elif args.command == "export-json":
        export_json(args.output)

    elif args.command == "enrich":
        enrich_deals(limit=args.limit)

    elif args.command == "enrich-web":
        run_web_enrichment(
            google_limit=args.google_limit,
            apollo_limit=args.apollo_limit,
            skip_google=args.skip_google,
            skip_apollo=args.skip_apollo,
            dry_run=args.dry_run,
        )

    elif args.command == "scrape-extra":
        run_additional_sources(
            days_back=args.days,
            skip=args.skip or None,
            dry_run=args.dry_run,
        )

    elif args.command == "firms":
        if args.firms_cmd == "discover":
            discover_firms(promote=args.promote)
        else:
            firms_p.print_help()

    elif args.command == "scrape-team":
        stats = run_team_scraper(limit=args.limit, dry_run=args.dry_run)
        print(
            f"Team scraper: {stats['firms_scraped']} firms scraped, "
            f"{stats['team_members_found']} members found, "
            f"{stats['investors_created']} investors created"
        )

    elif args.command == "backfill-investors":
        stats = backfill_investors_from_metadata(dry_run=args.dry_run)
        print(
            f"Backfill: {stats['deals_scanned']} deals scanned, "
            f"{stats['investors_created']} investors created, "
            f"{stats['links_created']} links created"
        )


if __name__ == "__main__":
    main()


