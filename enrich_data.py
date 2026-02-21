#!/usr/bin/env python3
"""
One-time data enrichment script — 7 strategies to fill gaps in the deal database.

Strategies:
  1. Re-scrape source URLs for investors (LLM extraction)
  2. Portfolio-to-deal fuzzy matching (backfill websites + links)
  3. Company website backfill via Google CSE
  4. Descriptions & metadata via Apollo.io
  5. SEC EDGAR cross-reference (match Form D filings to existing deals)
  6. LinkedIn URL search for partners (Google CSE)
  7. Quarantine low-confidence deals (validate via web search)

Usage:
  python enrich_data.py --dry-run          # Preview all strategies
  python enrich_data.py                    # Run all strategies
  python enrich_data.py --strategy 1 2 5   # Run specific strategies
  python enrich_data.py --strategy 7 --dry-run
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urlparse

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scrapers"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from database import (
    get_connection, batch_connection, upsert_firm, upsert_investor,
    link_deal_firm, link_deal_investor, upsert_deal_metadata,
)
from fetcher import fetch
from scrapers.utils import (
    company_names_match, normalize_company_name, link_investors_to_deal,
)
from scrapers.llm_extract import extract_deal_from_text
from scrapers.sec_scraper import search_efts, fetch_form_d_details
from scrapers.enrichment import (
    enrich_websites, enrich_with_apollo, search_company_website,
    search_linkedin_profile,
)
from config import GOOGLE_CSE_DAILY_LIMIT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("enrich_data.log"),
    ],
)
logger = logging.getLogger("enrich_data")


# ═══════════════════════════════════════════════════════════════
#  STRATEGY 1: Re-scrape source URLs for investors
# ═══════════════════════════════════════════════════════════════

def strategy_1_rescrape_sources(dry_run: bool = False) -> Dict:
    """
    Re-fetch source URLs for deals that have a URL but no investors.
    Use LLM to extract investors and descriptions from the article text.
    """
    logger.info("=" * 60)
    logger.info("Strategy 1: Re-scrape source URLs for investors")
    logger.info("=" * 60)

    conn = get_connection()
    rows = conn.execute("""
        SELECT d.id, d.company_name, d.source_url, d.company_description
        FROM deals d
        LEFT JOIN deal_investors di ON d.id = di.deal_id
        WHERE d.source_url IS NOT NULL
          AND d.source_url != ''
          AND di.deal_id IS NULL
        GROUP BY d.id
        ORDER BY d.created_at DESC
    """).fetchall()

    stats = {"eligible": len(rows), "fetched": 0, "investors_added": 0,
             "descriptions_added": 0, "errors": 0}
    logger.info(f"Found {len(rows)} deals with source_url but no investors")

    for row in rows:
        deal_id = row["id"]
        company_name = row["company_name"]
        source_url = row["source_url"]
        has_description = bool(row["company_description"])

        try:
            resp = fetch(source_url, timeout=15)
            if resp.status_code != 200 or not resp.text.strip():
                stats["errors"] += 1
                continue

            stats["fetched"] += 1
            # Extract text from HTML
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.text, "html.parser")
                # Remove scripts and styles
                for tag in soup(["script", "style", "nav", "footer", "header"]):
                    tag.decompose()
                text = soup.get_text(separator=" ", strip=True)
            except Exception:
                text = resp.text

            if len(text) < 50:
                continue

            result = extract_deal_from_text(company_name, text)
            if not result:
                continue

            # Extract investors
            investors_raw = result.get("investors", [])
            lead = result.get("lead_investor")
            if investors_raw:
                investor_dicts = []
                for inv_name in investors_raw:
                    if not inv_name or not isinstance(inv_name, str):
                        continue
                    role = "lead" if inv_name == lead else "participant"
                    investor_dicts.append({"name": inv_name.strip(), "role": role})

                if investor_dicts:
                    if dry_run:
                        inv_names = [d["name"] for d in investor_dicts]
                        logger.info(
                            f"  [DRY RUN] {company_name}: would add {len(investor_dicts)} "
                            f"investors: {', '.join(inv_names[:5])}"
                        )
                    else:
                        link_investors_to_deal(
                            conn, deal_id, investor_dicts,
                            upsert_investor_fn=upsert_investor,
                            link_deal_investor_fn=link_deal_investor,
                            upsert_firm_fn=upsert_firm,
                            link_deal_firm_fn=link_deal_firm,
                        )
                        conn.commit()
                        logger.info(
                            f"  {company_name}: added {len(investor_dicts)} investors"
                        )
                    stats["investors_added"] += len(investor_dicts)

            # Backfill description if missing
            desc = result.get("description")
            if desc and not has_description and len(desc) > 10:
                if dry_run:
                    logger.info(f"  [DRY RUN] {company_name}: would set description")
                else:
                    conn.execute(
                        "UPDATE deals SET company_description = ?, "
                        "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (desc[:500], deal_id),
                    )
                    conn.commit()
                stats["descriptions_added"] += 1

            # Brief pause between LLM calls
            time.sleep(2)

        except Exception as e:
            logger.debug(f"  Error processing {company_name}: {e}")
            stats["errors"] += 1

    logger.info(f"Strategy 1 done: {stats}")
    return stats


# ═══════════════════════════════════════════════════════════════
#  STRATEGY 2: Portfolio-to-deal fuzzy matching
# ═══════════════════════════════════════════════════════════════

def strategy_2_portfolio_matching(dry_run: bool = False) -> Dict:
    """
    Match unlinked portfolio companies to deals using fuzzy name matching.
    Backfill company_website from portfolio and create deal_metadata links.
    """
    logger.info("=" * 60)
    logger.info("Strategy 2: Portfolio-to-deal fuzzy matching")
    logger.info("=" * 60)

    conn = get_connection()

    # Get portfolio companies that aren't linked to any deal yet
    portfolio = conn.execute("""
        SELECT pc.id, pc.company_name, pc.company_name_normalized,
               pc.company_website, pc.firm_id, f.name as firm_name
        FROM portfolio_companies pc
        JOIN firms f ON pc.firm_id = f.id
        LEFT JOIN deal_metadata dm ON dm.key = 'portfolio_company_id'
            AND dm.value = CAST(pc.id AS TEXT)
        WHERE dm.deal_id IS NULL
    """).fetchall()

    # Get all deals for matching
    deals = conn.execute("""
        SELECT id, company_name, company_name_normalized, company_website
        FROM deals
    """).fetchall()

    stats = {"portfolio_checked": len(portfolio), "matched": 0,
             "websites_backfilled": 0, "firm_links_added": 0}
    logger.info(f"Checking {len(portfolio)} unlinked portfolio companies against {len(deals)} deals")

    for pc in portfolio:
        pc_name = pc["company_name"]
        pc_norm = pc["company_name_normalized"] or normalize_company_name(pc_name)

        for deal in deals:
            deal_norm = deal["company_name_normalized"] or normalize_company_name(deal["company_name"])

            # Exact normalized match first (fast)
            if pc_norm and deal_norm and pc_norm == deal_norm:
                matched = True
            elif company_names_match(pc_name, deal["company_name"]):
                matched = True
            else:
                matched = False

            if not matched:
                continue

            deal_id = deal["id"]

            if dry_run:
                logger.info(
                    f"  [DRY RUN] Match: '{pc_name}' (portfolio of {pc['firm_name']}) "
                    f"-> deal #{deal_id} '{deal['company_name']}'"
                )
            else:
                # Link portfolio company to deal via metadata
                upsert_deal_metadata(conn, deal_id, "portfolio_company_id", str(pc["id"]))
                upsert_deal_metadata(conn, deal_id, "portfolio_firm", pc["firm_name"])

                # Backfill company_website from portfolio if deal lacks one
                if pc["company_website"] and not deal["company_website"]:
                    conn.execute(
                        "UPDATE deals SET company_website = ?, "
                        "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (pc["company_website"], deal_id),
                    )
                    stats["websites_backfilled"] += 1

                # Link the firm to the deal
                link_deal_firm(conn, deal_id, pc["firm_id"], role="participant")
                stats["firm_links_added"] += 1

                conn.commit()
                logger.info(
                    f"  Matched: '{pc_name}' -> deal #{deal_id} '{deal['company_name']}'"
                )

            stats["matched"] += 1
            break  # Only link to first matching deal

    logger.info(f"Strategy 2 done: {stats}")
    return stats


# ═══════════════════════════════════════════════════════════════
#  STRATEGY 3: Company website backfill via Google CSE
# ═══════════════════════════════════════════════════════════════

def strategy_3_website_backfill(dry_run: bool = False, limit: int = 50) -> Dict:
    """
    Run existing enrich_websites() to find company websites via Google CSE.
    Wraps the existing function with a configurable limit.
    """
    logger.info("=" * 60)
    logger.info("Strategy 3: Company website backfill via Google CSE")
    logger.info("=" * 60)

    stats = enrich_websites(limit=limit, dry_run=dry_run)
    logger.info(f"Strategy 3 done: {stats}")
    return stats


# ═══════════════════════════════════════════════════════════════
#  STRATEGY 4: Descriptions & metadata via Apollo.io
# ═══════════════════════════════════════════════════════════════

def strategy_4_apollo_enrichment(dry_run: bool = False, limit: int = 95) -> Dict:
    """
    Run existing enrich_with_apollo() to fill descriptions and metadata.
    Should run after Strategy 3 since Apollo needs a domain.
    """
    logger.info("=" * 60)
    logger.info("Strategy 4: Apollo.io enrichment")
    logger.info("=" * 60)

    stats = enrich_with_apollo(limit=limit, dry_run=dry_run)
    logger.info(f"Strategy 4 done: {stats}")
    return stats


# ═══════════════════════════════════════════════════════════════
#  STRATEGY 5: SEC EDGAR cross-reference
# ═══════════════════════════════════════════════════════════════

def strategy_5_sec_crossref(dry_run: bool = False) -> Dict:
    """
    For existing deals, search EDGAR for matching Form D filings.
    Extract investors from related_persons and link to deals.
    Also extract amount_sold to validate deal amounts.
    """
    logger.info("=" * 60)
    logger.info("Strategy 5: SEC EDGAR cross-reference")
    logger.info("=" * 60)

    conn = get_connection()

    # Get deals without investors that might have SEC filings
    rows = conn.execute("""
        SELECT d.id, d.company_name, d.amount_usd, d.date_announced
        FROM deals d
        LEFT JOIN deal_investors di ON d.id = di.deal_id
        LEFT JOIN deal_metadata dm ON d.id = dm.deal_id AND dm.key = 'sec_crossref'
        WHERE di.deal_id IS NULL
          AND dm.deal_id IS NULL
        GROUP BY d.id
        ORDER BY d.created_at DESC
        LIMIT 200
    """).fetchall()

    stats = {"deals_checked": 0, "sec_matches": 0, "investors_added": 0,
             "amounts_validated": 0, "errors": 0}
    logger.info(f"Checking {len(rows)} deals against EDGAR")

    for row in rows:
        deal_id = row["id"]
        company_name = row["company_name"]
        stats["deals_checked"] += 1

        try:
            # Search EDGAR for this company name
            search_results = search_efts(
                f'"{company_name}"', days_back=365, max_results=5
            )

            if not search_results:
                if not dry_run:
                    upsert_deal_metadata(conn, deal_id, "sec_crossref", "no_match")
                continue

            # Try to get Form D details from the first matching filing
            best_match = None
            for sr in search_results:
                # Verify name match
                sec_name = sr.get("company_name", "")
                if not company_names_match(company_name, sec_name):
                    continue
                best_match = sr
                break

            if not best_match:
                if not dry_run:
                    upsert_deal_metadata(conn, deal_id, "sec_crossref", "no_name_match")
                continue

            stats["sec_matches"] += 1

            # Fetch Form D details
            details = fetch_form_d_details(
                cik=best_match.get("cik"),
                accession=best_match.get("accession_number"),
            )

            if not details:
                if not dry_run:
                    upsert_deal_metadata(conn, deal_id, "sec_crossref", "no_details")
                continue

            # Extract investors from related_persons
            related_persons = details.get("related_persons", [])
            if related_persons:
                investor_dicts = []
                for person in related_persons[:15]:
                    name = person["name"] if isinstance(person, dict) else person
                    if name:
                        investor_dicts.append({"name": name, "role": "participant"})

                if investor_dicts:
                    if dry_run:
                        names = [d["name"] for d in investor_dicts]
                        logger.info(
                            f"  [DRY RUN] {company_name}: would add {len(investor_dicts)} "
                            f"SEC investors: {', '.join(names[:5])}"
                        )
                    else:
                        link_investors_to_deal(
                            conn, deal_id, investor_dicts,
                            upsert_investor_fn=upsert_investor,
                            link_deal_investor_fn=link_deal_investor,
                            upsert_firm_fn=upsert_firm,
                            link_deal_firm_fn=link_deal_firm,
                        )
                        logger.info(
                            f"  {company_name}: added {len(investor_dicts)} SEC investors"
                        )
                    stats["investors_added"] += len(investor_dicts)

            # Validate/backfill amount from SEC data
            sec_amount = details.get("amount_sold") or details.get("total_offering")
            if sec_amount and not row["amount_usd"]:
                if dry_run:
                    logger.info(
                        f"  [DRY RUN] {company_name}: would set amount ${sec_amount:,.0f}"
                    )
                else:
                    conn.execute(
                        "UPDATE deals SET amount_usd = ?, amount_disclosed = 1, "
                        "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (sec_amount, deal_id),
                    )
                stats["amounts_validated"] += 1

            if not dry_run:
                upsert_deal_metadata(
                    conn, deal_id, "sec_crossref",
                    json.dumps({
                        "cik": best_match.get("cik"),
                        "accession": best_match.get("accession_number"),
                        "investors_found": len(related_persons),
                    }),
                )
                conn.commit()

            # Rate limit SEC requests
            time.sleep(0.5)

        except Exception as e:
            logger.debug(f"  Error cross-referencing {company_name}: {e}")
            stats["errors"] += 1

    logger.info(f"Strategy 5 done: {stats}")
    return stats


# ═══════════════════════════════════════════════════════════════
#  STRATEGY 6: LinkedIn URL search for partners
# ═══════════════════════════════════════════════════════════════

def strategy_6_linkedin_search(dry_run: bool = False, limit: int = 45) -> Dict:
    """
    Search Google CSE for LinkedIn profiles of investors/partners
    who have a firm_name but no linkedin_url.
    """
    logger.info("=" * 60)
    logger.info("Strategy 6: LinkedIn URL search for partners")
    logger.info("=" * 60)

    api_key = os.environ.get("GOOGLE_CSE_API_KEY")
    cse_id = os.environ.get("GOOGLE_CSE_ID")

    if not api_key or not cse_id:
        logger.warning("GOOGLE_CSE_API_KEY or GOOGLE_CSE_ID not set — skipping")
        return {"searched": 0, "found": 0, "skipped": 0, "rate_limited": 0}

    conn = get_connection()
    rows = conn.execute("""
        SELECT i.id, i.name, f.name as firm_name
        FROM investors i
        JOIN firms f ON i.firm_id = f.id
        WHERE i.linkedin_url IS NULL
          AND f.name IS NOT NULL
        ORDER BY i.created_at DESC
        LIMIT ?
    """, (limit,)).fetchall()

    stats = {"searched": 0, "found": 0, "skipped": 0, "rate_limited": 0}
    logger.info(f"Searching LinkedIn for {len(rows)} investors (limit={limit})")

    for row in rows:
        investor_id = row["id"]
        name = row["name"]
        firm_name = row["firm_name"]

        result = search_linkedin_profile(name, firm_name, api_key, cse_id)
        stats["searched"] += 1

        if result == "RATE_LIMITED":
            stats["rate_limited"] += 1
            logger.warning(f"Rate limited — stopping after {stats['searched']} searches")
            break

        if result is None:
            stats["skipped"] += 1
        else:
            stats["found"] += 1
            if dry_run:
                logger.info(f"  [DRY RUN] {name} ({firm_name}): {result}")
            else:
                conn.execute(
                    "UPDATE investors SET linkedin_url = ? WHERE id = ?",
                    (result, investor_id),
                )

        if stats["searched"] % 20 == 0 and not dry_run:
            conn.commit()

        time.sleep(1)  # Rate-limit politeness

    if not dry_run:
        conn.commit()

    logger.info(f"Strategy 6 done: {stats}")
    return stats


# ═══════════════════════════════════════════════════════════════
#  STRATEGY 7: Quarantine low-confidence deals
# ═══════════════════════════════════════════════════════════════

def strategy_7_quarantine(dry_run: bool = False) -> Dict:
    """
    Flag low-confidence 'other' source deals, try to validate via web search.
    If a source article is found: update source_url, bump confidence, extract investors.
    If not found: quarantine the deal.
    """
    logger.info("=" * 60)
    logger.info("Strategy 7: Quarantine low-confidence deals")
    logger.info("=" * 60)

    conn = get_connection()
    rows = conn.execute("""
        SELECT d.id, d.company_name, d.stage, d.amount_usd, d.confidence_score
        FROM deals d
        LEFT JOIN deal_metadata dm ON d.id = dm.deal_id AND dm.key = 'quarantined'
        WHERE d.source_type = 'other'
          AND d.confidence_score < 0.7
          AND (d.source_url IS NULL OR d.source_url = '')
          AND dm.deal_id IS NULL
        ORDER BY d.created_at DESC
    """).fetchall()

    stats = {"eligible": len(rows), "validated": 0, "quarantined": 0,
             "investors_added": 0, "errors": 0}
    logger.info(f"Found {len(rows)} low-confidence 'other' deals to check")

    for row in rows:
        deal_id = row["id"]
        company_name = row["company_name"]
        stage = row["stage"]
        amount = row["amount_usd"]

        # Build search query
        query_parts = [f'"{company_name}"', "funding"]
        if stage and stage != "Unknown":
            query_parts.append(stage)
        if amount:
            if amount >= 1_000_000:
                query_parts.append(f"${amount / 1_000_000:.0f}M")
            elif amount >= 1_000:
                query_parts.append(f"${amount / 1_000:.0f}K")
        query = " ".join(query_parts)

        try:
            # Use Bing News RSS search (no API key needed)
            from urllib.parse import quote as url_quote
            encoded = url_quote(query)
            url = f"https://www.bing.com/news/search?q={encoded}&format=rss&count=5"

            resp = fetch(url, timeout=15)
            if resp.status_code != 200:
                stats["errors"] += 1
                continue

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "xml")
            items = soup.find_all("item")[:5]

            found_source = None
            for item in items:
                title = item.find("title")
                title_text = title.get_text(strip=True) if title else ""
                link = item.find("link")
                link_text = link.get_text(strip=True) if link else ""

                # Check if the article title mentions our company
                if company_name.lower() in title_text.lower():
                    # Resolve Bing redirect URL
                    if "bing.com/news/apiclick" in link_text:
                        from urllib.parse import parse_qs, urlparse as _urlparse, unquote
                        parsed = parse_qs(_urlparse(link_text).query)
                        if "url" in parsed:
                            link_text = unquote(parsed["url"][0])
                    found_source = link_text
                    break

            if found_source:
                stats["validated"] += 1
                if dry_run:
                    logger.info(
                        f"  [DRY RUN] {company_name}: found source -> {found_source[:80]}"
                    )
                else:
                    conn.execute(
                        "UPDATE deals SET source_url = ?, confidence_score = ?, "
                        "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (found_source, min(row["confidence_score"] + 0.3, 1.0), deal_id),
                    )
                    upsert_deal_metadata(conn, deal_id, "quarantine_status", "validated")

                    # Try to extract investors from the found article
                    try:
                        article_resp = fetch(found_source, timeout=15)
                        if article_resp.status_code == 200 and article_resp.text:
                            article_soup = BeautifulSoup(article_resp.text, "html.parser")
                            for tag in article_soup(["script", "style", "nav", "footer"]):
                                tag.decompose()
                            article_text = article_soup.get_text(separator=" ", strip=True)

                            if len(article_text) > 50:
                                result = extract_deal_from_text(company_name, article_text)
                                if result and result.get("investors"):
                                    investor_dicts = []
                                    lead = result.get("lead_investor")
                                    for inv in result["investors"]:
                                        if inv and isinstance(inv, str):
                                            role = "lead" if inv == lead else "participant"
                                            investor_dicts.append(
                                                {"name": inv.strip(), "role": role}
                                            )
                                    if investor_dicts:
                                        link_investors_to_deal(
                                            conn, deal_id, investor_dicts,
                                            upsert_investor_fn=upsert_investor,
                                            link_deal_investor_fn=link_deal_investor,
                                            upsert_firm_fn=upsert_firm,
                                            link_deal_firm_fn=link_deal_firm,
                                        )
                                        stats["investors_added"] += len(investor_dicts)
                                        logger.info(
                                            f"  {company_name}: validated + {len(investor_dicts)} investors"
                                        )
                    except Exception as e:
                        logger.debug(f"  Error extracting from found source: {e}")

                    conn.commit()
            else:
                stats["quarantined"] += 1
                if dry_run:
                    logger.info(f"  [DRY RUN] {company_name}: no source found, would quarantine")
                else:
                    upsert_deal_metadata(conn, deal_id, "quarantined", "no_source_found")
                    upsert_deal_metadata(
                        conn, deal_id, "quarantine_date",
                        datetime.now().strftime("%Y-%m-%d"),
                    )
                    conn.commit()

            # Brief pause between LLM calls
            time.sleep(2)

        except Exception as e:
            logger.debug(f"  Error checking {company_name}: {e}")
            stats["errors"] += 1

    logger.info(f"Strategy 7 done: {stats}")
    return stats


# ═══════════════════════════════════════════════════════════════
#  ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════

STRATEGIES = {
    1: ("Re-scrape source URLs for investors", strategy_1_rescrape_sources),
    2: ("Portfolio-to-deal fuzzy matching", strategy_2_portfolio_matching),
    3: ("Company website backfill (Google CSE)", strategy_3_website_backfill),
    4: ("Apollo.io enrichment", strategy_4_apollo_enrichment),
    5: ("SEC EDGAR cross-reference", strategy_5_sec_crossref),
    6: ("LinkedIn URL search for partners", strategy_6_linkedin_search),
    7: ("Quarantine low-confidence deals", strategy_7_quarantine),
}


def run_all(strategies: List[int] = None, dry_run: bool = False,
            google_limit: int = 50, apollo_limit: int = 95,
            linkedin_limit: int = 45) -> Dict:
    """Run selected (or all) strategies and return combined stats."""
    if strategies is None:
        strategies = list(STRATEGIES.keys())

    logger.info("=" * 60)
    logger.info(f"Data Enrichment — {len(strategies)} strategies, dry_run={dry_run}")
    logger.info(f"Started at {datetime.now().isoformat()}")
    logger.info("=" * 60)

    all_stats = {}
    start = time.time()

    for num in strategies:
        if num not in STRATEGIES:
            logger.warning(f"Unknown strategy {num}, skipping")
            continue

        name, fn = STRATEGIES[num]
        logger.info(f"\n{'─' * 50}")
        logger.info(f"Running Strategy {num}: {name}")
        logger.info(f"{'─' * 50}")

        try:
            if num == 3:
                result = fn(dry_run=dry_run, limit=google_limit)
            elif num == 4:
                result = fn(dry_run=dry_run, limit=apollo_limit)
            elif num == 6:
                result = fn(dry_run=dry_run, limit=linkedin_limit)
            else:
                result = fn(dry_run=dry_run)
            all_stats[f"strategy_{num}"] = result
        except Exception as e:
            logger.error(f"Strategy {num} failed: {e}")
            all_stats[f"strategy_{num}"] = {"error": str(e)}

    elapsed = time.time() - start

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("ENRICHMENT SUMMARY")
    logger.info("=" * 60)
    for key, stats in all_stats.items():
        logger.info(f"  {key}: {stats}")
    logger.info(f"  Total time: {elapsed:.1f}s")
    logger.info("=" * 60)

    return all_stats


def main():
    parser = argparse.ArgumentParser(
        description="NYC VC Data Enrichment — 7 strategies to fill data gaps",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Strategies:
  1  Re-scrape source URLs for investors (needs ANTHROPIC_API_KEY)
  2  Portfolio-to-deal fuzzy matching (no API keys needed)
  3  Company website backfill (needs GOOGLE_CSE_API_KEY)
  4  Apollo.io enrichment (needs APOLLO_API_KEY, run after 3)
  5  SEC EDGAR cross-reference (no API keys needed)
  6  LinkedIn URL search (needs GOOGLE_CSE_API_KEY, shares quota with 3)
  7  Quarantine low-confidence deals (needs ANTHROPIC_API_KEY)

Recommended execution order:
  Phase 1: python enrich_data.py --strategy 1 2 5 7   # No external API keys needed
  Phase 2: python enrich_data.py --strategy 3          # Google CSE
  Phase 3: python enrich_data.py --strategy 4          # Apollo (after websites found)
  Phase 4: python enrich_data.py --strategy 6          # LinkedIn (shares CSE quota)
        """,
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without writing to database",
    )
    parser.add_argument(
        "--strategy", "-s", type=int, nargs="+",
        help="Run specific strategies (default: all). E.g. --strategy 1 2 5",
    )
    parser.add_argument(
        "--google-limit", type=int, default=50,
        help="Max Google CSE queries for website search (default: 50)",
    )
    parser.add_argument(
        "--apollo-limit", type=int, default=95,
        help="Max Apollo enrichments (default: 95)",
    )
    parser.add_argument(
        "--linkedin-limit", type=int, default=45,
        help="Max LinkedIn searches (default: 45)",
    )

    args = parser.parse_args()

    run_all(
        strategies=args.strategy,
        dry_run=args.dry_run,
        google_limit=args.google_limit,
        apollo_limit=args.apollo_limit,
        linkedin_limit=args.linkedin_limit,
    )


if __name__ == "__main__":
    main()
