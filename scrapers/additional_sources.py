"""
Additional Data Source Scrapers
================================
Scrapers for supplemental data sources beyond the core pipeline:
  1. OpenCorporates API — recently registered NY companies
  2. Crunchbase Basic API — NYC funding rounds
  3. NY State DOS Entity Search — new NY corporation filings
  4. SBIR.gov Federal Grants — SBIR/STTR awards to NY companies
  5. Clearbit/HubSpot Enrichment — company metadata enrichment
  6. Hunter.io Domain Validation — verify company domains

All env vars are optional — each scraper warns and skips if its key is not set.
"""

import os
import re
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from database import (
    get_connection, batch_connection, insert_deal,
    upsert_deal_metadata, get_category_id,
    log_scrape, finish_scrape,
    upsert_investor, link_deal_investor,
)
from fetcher import fetch
from scrapers.utils import (
    normalize_company_name, classify_sector, classify_stage_from_amount,
    normalize_stage, parse_amount, should_skip_deal, validate_deal_amount,
)

logger = logging.getLogger(__name__)


# ── Shared constants ─────────────────────────────────────────

# Entity name patterns that indicate non-startup structures
_NON_STARTUP_RE = re.compile(
    r"""(?ix)
    (?:^|\s)
    (?:
        L\.?L\.?C\.?
      | L\.?P\.?
      | LTD\.?
      | Trust
      | REIT
      | DST
      | SPV
      | Holdings?
      | Partners(?:hip)?
      | Associates?
      | Fund(?:ing)?
      | Realty
      | Estate
      | Insurance
    )
    (?:\s|$|[.,])
    """,
)


def _looks_like_startup(name: str) -> bool:
    """Return True if the entity name does NOT match junk patterns."""
    if not name or len(name) < 3:
        return False
    return not _NON_STARTUP_RE.search(name)


def _existing_normalized_names(conn) -> set:
    """Load all normalized company names from the deals table for dedup."""
    rows = conn.execute(
        "SELECT DISTINCT company_name_normalized FROM deals "
        "WHERE company_name_normalized IS NOT NULL"
    ).fetchall()
    return {r["company_name_normalized"] for r in rows}


# ═══════════════════════════════════════════════════════════════
#  1. OpenCorporates API
# ═══════════════════════════════════════════════════════════════

OPENCORPORATES_SEARCH_TERMS = [
    "tech", "software", "ai", "health", "bio", "fintech",
    "platform", "labs", "data", "cloud", "analytics",
]

OPENCORPORATES_TTL = 86400 * 7  # 7 days — registrations don't change


def run_opencorporates_scraper(days_back: int = 30) -> dict:
    """
    Search OpenCorporates for recently registered companies in New York
    with startup-indicator terms. Requires OPENCORPORATES_API_KEY env var
    (the v0.4 API no longer allows unauthenticated requests).

    Returns stats dict: {found, new, skipped, errors}.
    """
    api_key = os.environ.get("OPENCORPORATES_API_KEY")
    if not api_key:
        logger.warning("[OpenCorporates] OPENCORPORATES_API_KEY not set — skipping (API requires authentication)")
        return {"found": 0, "new": 0, "skipped": 0, "errors": 0}

    stats = {"found": 0, "new": 0, "skipped": 0, "errors": 0}
    created_since = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    conn = get_connection()
    log_id = log_scrape(conn, "opencorporates")
    existing_names = _existing_normalized_names(conn)

    all_companies = []

    try:
        for term in OPENCORPORATES_SEARCH_TERMS:
            if len(all_companies) >= 200:
                break

            params = {
                "q": term,
                "jurisdiction_code": "us_ny",
                "created_since": created_since,
                "order": "score",
                "per_page": 30,
            }
            if api_key:
                params["api_token"] = api_key

            try:
                resp = fetch(
                    "https://api.opencorporates.com/v0.4/companies/search",
                    params=params,
                    timeout=15,
                    ttl=OPENCORPORATES_TTL,
                )

                if resp.status_code == 429:
                    logger.warning("[OpenCorporates] Rate limited — stopping")
                    break
                if resp.status_code != 200:
                    logger.debug(f"[OpenCorporates] HTTP {resp.status_code} for term '{term}'")
                    stats["errors"] += 1
                    continue

                data = resp.json()
                companies = data.get("results", {}).get("companies", [])

                for entry in companies:
                    company = entry.get("company", {})
                    all_companies.append(company)

                logger.info(f"[OpenCorporates] term='{term}': {len(companies)} results")

            except Exception as e:
                logger.warning(f"[OpenCorporates] Search failed for '{term}': {e}")
                stats["errors"] += 1

            # Rate limit: 1s between queries
            time.sleep(1)

        stats["found"] = len(all_companies)
        logger.info(f"[OpenCorporates] Collected {stats['found']} companies total")

        # Insert deals
        with batch_connection() as conn:
            for company in all_companies:
                try:
                    name = company.get("name", "").strip()
                    if not name:
                        continue

                    # Filter out non-startup entities
                    if not _looks_like_startup(name):
                        stats["skipped"] += 1
                        continue

                    # Dedup
                    norm = normalize_company_name(name)
                    if norm in existing_names:
                        stats["skipped"] += 1
                        continue

                    # Skip known VC firms
                    skip = should_skip_deal(conn, name)
                    if skip:
                        stats["skipped"] += 1
                        continue

                    inc_date = company.get("incorporation_date")
                    oc_url = company.get("opencorporates_url", "")

                    deal_id = insert_deal(
                        conn, name,
                        date_announced=inc_date,
                        source_url=oc_url,
                        source_type="other",
                        confidence_score=0.3,
                        raw_text=json.dumps({
                            "company_number": company.get("company_number"),
                            "jurisdiction": company.get("jurisdiction_code"),
                            "status": company.get("current_status"),
                            "registered_address": company.get("registered_address_in_full"),
                            "source": "opencorporates",
                        })[:2000],
                    )

                    if deal_id:
                        stats["new"] += 1
                        existing_names.add(norm)

                except Exception as e:
                    logger.debug(f"[OpenCorporates] Failed to insert '{name}': {e}")
                    stats["errors"] += 1

            finish_scrape(conn, log_id, "success", stats["found"], stats["new"])

    except Exception as e:
        logger.error(f"[OpenCorporates] Scraper error: {e}")
        try:
            conn_err = get_connection()
            finish_scrape(conn_err, log_id, "error", stats["found"], stats["new"], str(e))
        except Exception:
            pass

    logger.info(
        f"[OpenCorporates] Done: {stats['found']} found, {stats['new']} new, "
        f"{stats['skipped']} skipped, {stats['errors']} errors"
    )
    return stats


# ═══════════════════════════════════════════════════════════════
#  2. Crunchbase Basic API
# ═══════════════════════════════════════════════════════════════

CRUNCHBASE_TTL = 86400 * 3  # 3 days — funding rounds update frequently


def run_crunchbase_scraper(days_back: int = 30) -> dict:
    """
    Search Crunchbase API for recent NYC funding rounds.
    Requires CRUNCHBASE_API_KEY env var.

    Returns stats dict: {found, new, skipped, errors}.
    """
    api_key = os.environ.get("CRUNCHBASE_API_KEY")
    if not api_key:
        logger.warning("[Crunchbase] CRUNCHBASE_API_KEY not set — skipping")
        return {"found": 0, "new": 0, "skipped": 0, "errors": 0}

    stats = {"found": 0, "new": 0, "skipped": 0, "errors": 0}
    since_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    conn = get_connection()
    log_id = log_scrape(conn, "crunchbase_api")
    existing_names = _existing_normalized_names(conn)

    all_rounds = []

    try:
        # Search for funding rounds with NYC location signals
        search_queries = [
            "New York funding",
            "NYC startup funding",
            "Manhattan startup",
            "Brooklyn startup funding",
        ]

        for query in search_queries:
            if len(all_rounds) >= 100:
                break

            params = {
                "user_key": api_key,
                "query": query,
                "location_uuids": "528f5e3c-90d1-1f82-6672-7571f10e0c8c",  # New York City UUID
                "updated_since": since_date,
                "sort_order": "updated_at DESC",
                "per_page": 25,
            }

            try:
                resp = fetch(
                    "https://api.crunchbase.com/api/v4/searches/funding_rounds",
                    params=params,
                    headers={"X-cb-user-key": api_key},
                    timeout=20,
                    ttl=CRUNCHBASE_TTL,
                )

                if resp.status_code == 401:
                    logger.error("[Crunchbase] Invalid API key (401)")
                    break
                if resp.status_code == 429:
                    logger.warning("[Crunchbase] Rate limited — stopping")
                    break
                if resp.status_code != 200:
                    logger.debug(f"[Crunchbase] HTTP {resp.status_code} for query '{query}'")
                    stats["errors"] += 1
                    # Try the ODM endpoint as fallback
                    resp = fetch(
                        "https://api.crunchbase.com/api/v4/entities/funding_rounds",
                        params=params,
                        headers={"X-cb-user-key": api_key},
                        timeout=20,
                        ttl=CRUNCHBASE_TTL,
                    )
                    if resp.status_code != 200:
                        continue

                data = resp.json()
                entities = data.get("entities", data.get("items", []))

                for entity in entities:
                    props = entity.get("properties", entity)
                    all_rounds.append(props)

                logger.info(f"[Crunchbase] query='{query}': {len(entities)} results")

            except Exception as e:
                logger.warning(f"[Crunchbase] Search failed for '{query}': {e}")
                stats["errors"] += 1

            # Rate limit: 2s between queries
            time.sleep(2)

        stats["found"] = len(all_rounds)
        logger.info(f"[Crunchbase] Collected {stats['found']} funding rounds total")

        # Insert deals
        with batch_connection() as conn:
            for props in all_rounds:
                try:
                    company_name = (
                        props.get("funded_organization_identifier", {}).get("value")
                        or props.get("name", "")
                        or props.get("organization_name", "")
                    )
                    if isinstance(company_name, dict):
                        company_name = company_name.get("value", "")
                    company_name = company_name.strip()

                    if not company_name:
                        continue

                    # Dedup
                    norm = normalize_company_name(company_name)
                    if norm in existing_names:
                        stats["skipped"] += 1
                        continue

                    # Skip VC firms
                    skip = should_skip_deal(conn, company_name)
                    if skip:
                        stats["skipped"] += 1
                        continue

                    # Extract amount
                    money_raised = props.get("money_raised", {})
                    if isinstance(money_raised, dict):
                        amount = money_raised.get("value")
                        currency = money_raised.get("currency", "USD")
                        if currency != "USD":
                            amount = None  # Only track USD
                    else:
                        amount = parse_amount(str(money_raised)) if money_raised else None

                    # Extract stage
                    investment_type = props.get("investment_type") or props.get("funding_type") or ""
                    stage = normalize_stage(investment_type)
                    if stage == "Unknown" and amount:
                        stage = classify_stage_from_amount(amount)

                    # Validate amount
                    if amount and not validate_deal_amount(amount, stage):
                        amount = None

                    # Extract description
                    description = props.get("short_description") or props.get("description") or ""
                    if description:
                        description = description[:500]

                    # Extract investors
                    investor_names = []
                    investors_field = props.get("investor_identifiers", [])
                    if isinstance(investors_field, list):
                        for inv in investors_field:
                            if isinstance(inv, dict):
                                inv_name = inv.get("value", "")
                            else:
                                inv_name = str(inv)
                            if inv_name:
                                investor_names.append(inv_name)

                    # Determine date
                    announced_on = props.get("announced_on") or props.get("created_at", "")
                    if announced_on and "T" in str(announced_on):
                        announced_on = str(announced_on)[:10]

                    # Category
                    category_name = classify_sector(f"{company_name} {description}")
                    cat_id = get_category_id(conn, category_name) if category_name else None

                    # Crunchbase permalink for source URL
                    permalink = props.get("permalink") or props.get("cb_url") or ""
                    source_url = (
                        f"https://www.crunchbase.com/funding_round/{permalink}"
                        if permalink and not permalink.startswith("http")
                        else permalink
                    ) or f"https://www.crunchbase.com/organization/{norm}"

                    deal_id = insert_deal(
                        conn, company_name,
                        company_description=description or None,
                        stage=stage,
                        amount_usd=amount,
                        amount_disclosed=1 if amount else 0,
                        date_announced=announced_on or None,
                        source_url=source_url,
                        source_type="crunchbase",
                        category_id=cat_id,
                        confidence_score=0.9,
                        raw_text=json.dumps(props, default=str)[:2000],
                    )

                    if deal_id:
                        stats["new"] += 1
                        existing_names.add(norm)

                        # Store investor names as metadata
                        if investor_names:
                            upsert_deal_metadata(
                                conn, deal_id, "crunchbase_investors",
                                json.dumps(investor_names),
                            )
                            # Also create proper investor records
                            for inv_name in investor_names:
                                try:
                                    firm_row = conn.execute(
                                        "SELECT id FROM firms WHERE LOWER(name) = LOWER(?)",
                                        (inv_name,)
                                    ).fetchone()
                                    firm_id = firm_row["id"] if firm_row else None
                                    inv_id = upsert_investor(conn, name=inv_name, firm_id=firm_id)
                                    link_deal_investor(conn, deal_id, inv_id)
                                except Exception as e:
                                    logger.debug(f"[Crunchbase] Failed to create investor '{inv_name}': {e}")

                except Exception as e:
                    logger.debug(f"[Crunchbase] Failed to insert round: {e}")
                    stats["errors"] += 1

            finish_scrape(conn, log_id, "success", stats["found"], stats["new"])

    except Exception as e:
        logger.error(f"[Crunchbase] Scraper error: {e}")
        try:
            conn_err = get_connection()
            finish_scrape(conn_err, log_id, "error", stats["found"], stats["new"], str(e))
        except Exception:
            pass

    logger.info(
        f"[Crunchbase] Done: {stats['found']} found, {stats['new']} new, "
        f"{stats['skipped']} skipped, {stats['errors']} errors"
    )
    return stats


# ═══════════════════════════════════════════════════════════════
#  3. NY State DOS Entity Search
# ═══════════════════════════════════════════════════════════════

def run_ny_dos_scraper() -> dict:
    """
    NY State DOS entity search is currently disabled.
    The old Oracle Forms endpoint (appext20.dos.ny.gov) was retired and
    replaced with a JavaScript SPA at apps.dos.ny.gov/publicInquiry/ that
    requires browser automation (Playwright/Selenium) to scrape. Re-enable
    this when a headless browser dependency is added.

    Returns stats dict: {found, new, skipped, errors}.
    """
    logger.warning(
        "[NY DOS] Disabled — the old endpoint was retired. "
        "The new site (apps.dos.ny.gov/publicInquiry/) is a JavaScript SPA "
        "that requires browser automation to scrape."
    )
    return {"found": 0, "new": 0, "skipped": 0, "errors": 0}


# ═══════════════════════════════════════════════════════════════
#  4. SBIR.gov Federal Grants
# ═══════════════════════════════════════════════════════════════

SBIR_CSV_URL = "https://data.www.sbir.gov/awarddatapublic/award_data.csv"
SBIR_TTL = 86400 * 7  # 7 days — government data updates slowly


def run_sbir_scraper(days_back: int = 180) -> dict:
    """
    Download SBIR.gov bulk CSV of federal SBIR/STTR awards and filter for
    New York state companies. No API key needed — public government data.
    (The JSON API at api.www.sbir.gov is unreliable; the CSV bulk export
    at data.www.sbir.gov is the stable alternative.)

    Returns stats dict: {found, new, skipped, errors}.
    """
    import csv
    import io

    stats = {"found": 0, "new": 0, "skipped": 0, "errors": 0}

    conn = get_connection()
    log_id = log_scrape(conn, "sbir_gov")
    existing_names = _existing_normalized_names(conn)

    try:
        logger.info("[SBIR] Downloading bulk award CSV (this may take a moment)...")
        resp = fetch(
            SBIR_CSV_URL,
            headers={"User-Agent": "Mozilla/5.0 (NYC VC Scraper)"},
            timeout=120,
            ttl=SBIR_TTL,
        )

        if resp.status_code != 200:
            logger.warning(f"[SBIR] CSV download returned HTTP {resp.status_code}")
            finish_scrape(conn, log_id, "error", 0, 0, f"HTTP {resp.status_code}")
            return stats

        # Parse CSV
        cutoff = datetime.now() - timedelta(days=days_back)
        reader = csv.DictReader(io.StringIO(resp.text))

        all_awards = []
        for row in reader:
            # Filter to NY state only
            state = (row.get("Company State") or row.get("State") or "").strip().upper()
            if state != "NY":
                continue

            # Filter by date
            date_str = (row.get("Award Start Date") or row.get("Award Date") or "").strip()
            award_date = None
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"):
                try:
                    award_date = datetime.strptime(date_str[:19], fmt)
                    break
                except (ValueError, TypeError):
                    continue

            if award_date and award_date < cutoff:
                continue

            all_awards.append(row)
            if len(all_awards) >= 200:
                break

        stats["found"] = len(all_awards)
        logger.info(f"[SBIR] Found {stats['found']} NY awards in last {days_back} days")

        # Insert deals
        with batch_connection() as bconn:
            for award in all_awards:
                try:
                    company_name = (
                        award.get("Company Name") or award.get("Firm") or ""
                    ).strip()

                    if not company_name:
                        continue

                    # Dedup
                    norm = normalize_company_name(company_name)
                    if norm in existing_names:
                        stats["skipped"] += 1
                        continue

                    # Skip known VC firms
                    if should_skip_deal(bconn, company_name):
                        stats["skipped"] += 1
                        continue

                    # Extract amount
                    amount = None
                    raw_amount = (award.get("Award Amount") or award.get("Amount") or "").strip()
                    if raw_amount:
                        try:
                            amount = float(raw_amount.replace(",", "").replace("$", ""))
                        except (ValueError, TypeError):
                            pass

                    # Stage from amount
                    stage = classify_stage_from_amount(amount) if amount else "Unknown"
                    if amount and not validate_deal_amount(amount, stage):
                        amount = None
                        stage = "Unknown"

                    # Description
                    abstract = (
                        award.get("Abstract") or award.get("Award Title") or ""
                    ).strip()
                    description = abstract[:500] if abstract else None

                    # Category
                    category_text = f"{company_name} {description or ''}"
                    category_name = classify_sector(category_text)
                    cat_id = get_category_id(bconn, category_name) if category_name else None

                    # Date
                    date_announced = (
                        award.get("Award Start Date") or award.get("Award Date") or ""
                    ).strip()
                    if date_announced and "T" in date_announced:
                        date_announced = date_announced[:10]

                    # Agency info
                    agency = (award.get("Agency") or "").strip()
                    program = (award.get("Program") or award.get("Phase") or "").strip()
                    sbir_id = (award.get("Award ID") or award.get("Award Number") or "").strip()

                    source_url = (
                        f"https://www.sbir.gov/award/{sbir_id}"
                        if sbir_id else "https://www.sbir.gov"
                    )

                    deal_id = insert_deal(
                        bconn, company_name,
                        company_description=description,
                        stage=stage,
                        amount_usd=amount,
                        amount_disclosed=1 if amount else 0,
                        date_announced=date_announced[:10] if date_announced else None,
                        source_url=source_url,
                        source_type="other",
                        category_id=cat_id,
                        confidence_score=0.8,
                        raw_text=json.dumps({
                            "agency": agency,
                            "program": program,
                            "sbir_id": sbir_id,
                            "city": (award.get("Company City") or "").strip(),
                            "state": "NY",
                            "source": "sbir_gov_csv",
                        })[:2000],
                    )

                    if deal_id:
                        stats["new"] += 1
                        existing_names.add(norm)

                        if agency:
                            upsert_deal_metadata(bconn, deal_id, "sbir_agency", agency)
                        if program:
                            upsert_deal_metadata(bconn, deal_id, "sbir_program", program)

                except Exception as e:
                    logger.debug(f"[SBIR] Failed to insert award: {e}")
                    stats["errors"] += 1

            finish_scrape(bconn, log_id, "success", stats["found"], stats["new"])

    except Exception as e:
        logger.error(f"[SBIR] Scraper error: {e}")
        try:
            conn_err = get_connection()
            finish_scrape(conn_err, log_id, "error", stats["found"], stats["new"], str(e))
        except Exception:
            pass

    logger.info(
        f"[SBIR] Done: {stats['found']} found, {stats['new']} new, "
        f"{stats['skipped']} skipped, {stats['errors']} errors"
    )
    return stats


# ═══════════════════════════════════════════════════════════════
#  5. Clearbit/HubSpot Enrichment
# ═══════════════════════════════════════════════════════════════

CLEARBIT_TTL = 86400 * 14  # 14 days — company data changes slowly
CLEARBIT_FREE_LIMIT = 45   # buffer from 50/month free tier


def enrich_with_clearbit(limit: int = CLEARBIT_FREE_LIMIT, dry_run: bool = False) -> dict:
    """
    Enrich deals that have a company_website but haven't been Clearbit-enriched.
    Similar to Apollo enrichment — stores metadata via upsert_deal_metadata().
    Requires CLEARBIT_API_KEY env var.

    Returns stats dict: {enriched, no_data, skipped, rate_limited, auth_error}.
    """
    api_key = os.environ.get("CLEARBIT_API_KEY")
    if not api_key:
        logger.warning("[Clearbit] CLEARBIT_API_KEY not set — skipping")
        return {"enriched": 0, "no_data": 0, "skipped": 0, "rate_limited": 0, "auth_error": False}

    stats = {"enriched": 0, "no_data": 0, "skipped": 0, "rate_limited": 0, "auth_error": False}

    conn = get_connection()
    rows = conn.execute(
        """SELECT d.id, d.company_name, d.company_website, d.company_description
           FROM deals d
           LEFT JOIN deal_metadata dm ON d.id = dm.deal_id AND dm.key = 'clearbit_enriched'
           WHERE d.company_website IS NOT NULL
             AND dm.value IS NULL
           ORDER BY d.created_at DESC
           LIMIT ?""",
        (limit,)
    ).fetchall()

    if not rows:
        logger.info("[Clearbit] No deals need Clearbit enrichment")
        return stats

    logger.info(f"[Clearbit] Enriching {len(rows)} deals (dry_run={dry_run})")

    for i, row in enumerate(rows):
        deal_id = row["id"]
        name = row["company_name"]
        website = row["company_website"]

        # Extract domain from website URL
        try:
            parsed = urlparse(website)
            domain = parsed.netloc.lower().lstrip("www.") if parsed.netloc else website.lower().lstrip("www.")
        except Exception:
            domain = website.lower().lstrip("www.") if website else ""

        if not domain:
            stats["skipped"] += 1
            continue

        try:
            resp = fetch(
                "https://company.clearbit.com/v2/companies/find",
                params={"domain": domain},
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=15,
                ttl=CLEARBIT_TTL,
            )

            if resp.status_code == 429:
                stats["rate_limited"] += 1
                logger.warning(f"[Clearbit] Rate limited — stopping after {i} queries")
                break
            if resp.status_code == 401 or resp.status_code == 403:
                stats["auth_error"] = True
                logger.error("[Clearbit] Auth error — stopping")
                break
            if resp.status_code == 404:
                # Company not found in Clearbit
                stats["no_data"] += 1
                if not dry_run:
                    upsert_deal_metadata(conn, deal_id, "clearbit_enriched", "no_data")
                continue
            if resp.status_code != 200:
                logger.debug(f"[Clearbit] HTTP {resp.status_code} for {domain}")
                stats["no_data"] += 1
                continue

            data = resp.json()

            if not data or not any(data.get(k) for k in ("name", "description", "tech")):
                stats["no_data"] += 1
                if not dry_run:
                    upsert_deal_metadata(conn, deal_id, "clearbit_enriched", "no_data")
                continue

            stats["enriched"] += 1
            logger.info(
                f"[Clearbit] Enriched {name}: "
                f"{data.get('metrics', {}).get('employees')} employees, "
                f"{data.get('category', {}).get('industry')}"
            )

            if not dry_run:
                # Store tech stack
                tech = data.get("tech", [])
                if tech:
                    upsert_deal_metadata(
                        conn, deal_id, "tech_stack",
                        json.dumps(tech) if isinstance(tech, list) else str(tech),
                    )

                # Store revenue estimate
                metrics = data.get("metrics", {})
                if metrics:
                    revenue_range = metrics.get("estimatedAnnualRevenue") or metrics.get("annualRevenue")
                    if revenue_range:
                        upsert_deal_metadata(conn, deal_id, "revenue_estimate", str(revenue_range))

                    employees = metrics.get("employees") or metrics.get("employeesRange")
                    if employees:
                        upsert_deal_metadata(conn, deal_id, "employee_range", str(employees))

                # Store industry
                category = data.get("category", {})
                if category:
                    industry = category.get("industry") or category.get("sector")
                    if industry:
                        upsert_deal_metadata(conn, deal_id, "clearbit_industry", str(industry))

                # Update company_description if empty
                desc = data.get("description")
                if desc and not row["company_description"]:
                    conn.execute(
                        "UPDATE deals SET company_description = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (desc[:500], deal_id),
                    )

                # Mark as enriched
                upsert_deal_metadata(conn, deal_id, "clearbit_enriched", "yes")

        except Exception as e:
            logger.debug(f"[Clearbit] Failed to enrich {name} ({domain}): {e}")
            stats["no_data"] += 1

        # Commit every 10 deals
        if not dry_run and (i + 1) % 10 == 0:
            conn.commit()

        # Rate-limit politeness
        time.sleep(1.5)

    if not dry_run:
        conn.commit()

    logger.info(
        f"[Clearbit] Done: {stats['enriched']} enriched, {stats['no_data']} no_data, "
        f"{stats['skipped']} skipped, {stats['rate_limited']} rate_limited"
    )
    return stats


# ═══════════════════════════════════════════════════════════════
#  6. Hunter.io Domain Validation
# ═══════════════════════════════════════════════════════════════

HUNTER_TTL = 86400 * 30  # 30 days — domain validation is stable
HUNTER_FREE_LIMIT = 20    # buffer from 25/month free tier


def validate_domains_with_hunter(limit: int = HUNTER_FREE_LIMIT, dry_run: bool = False) -> dict:
    """
    Validate company domains found by Google CSE using Hunter.io.
    If Hunter returns 0 results for a domain, marks it as suspicious
    in deal_metadata.
    Requires HUNTER_API_KEY env var.

    Returns stats dict: {validated, suspicious, skipped, rate_limited, auth_error}.
    """
    api_key = os.environ.get("HUNTER_API_KEY")
    if not api_key:
        logger.warning("[Hunter] HUNTER_API_KEY not set — skipping")
        return {"validated": 0, "suspicious": 0, "skipped": 0, "rate_limited": 0, "auth_error": False}

    stats = {"validated": 0, "suspicious": 0, "skipped": 0, "rate_limited": 0, "auth_error": False}

    conn = get_connection()
    rows = conn.execute(
        """SELECT d.id, d.company_name, d.company_website
           FROM deals d
           LEFT JOIN deal_metadata dm ON d.id = dm.deal_id AND dm.key = 'hunter_validated'
           WHERE d.company_website IS NOT NULL
             AND dm.value IS NULL
           ORDER BY d.created_at DESC
           LIMIT ?""",
        (limit,)
    ).fetchall()

    if not rows:
        logger.info("[Hunter] No deals need domain validation")
        return stats

    logger.info(f"[Hunter] Validating {len(rows)} domains (dry_run={dry_run})")

    for i, row in enumerate(rows):
        deal_id = row["id"]
        name = row["company_name"]
        website = row["company_website"]

        # Extract domain
        try:
            parsed = urlparse(website)
            domain = parsed.netloc.lower().lstrip("www.") if parsed.netloc else website.lower().lstrip("www.")
        except Exception:
            domain = website.lower().lstrip("www.") if website else ""

        if not domain:
            stats["skipped"] += 1
            continue

        try:
            resp = fetch(
                "https://api.hunter.io/v2/domain-search",
                params={"domain": domain, "api_key": api_key},
                timeout=15,
                ttl=HUNTER_TTL,
            )

            if resp.status_code == 429:
                stats["rate_limited"] += 1
                logger.warning(f"[Hunter] Rate limited — stopping after {i} queries")
                break
            if resp.status_code == 401:
                stats["auth_error"] = True
                logger.error("[Hunter] Auth error — stopping")
                break
            if resp.status_code != 200:
                logger.debug(f"[Hunter] HTTP {resp.status_code} for {domain}")
                stats["skipped"] += 1
                continue

            data = resp.json()
            hunter_data = data.get("data", {})
            emails = hunter_data.get("emails", [])
            email_count = len(emails)

            if email_count == 0:
                # No emails found — domain may be suspicious
                stats["suspicious"] += 1
                logger.info(f"[Hunter] {name} ({domain}): 0 emails — marking suspicious")
                if not dry_run:
                    upsert_deal_metadata(conn, deal_id, "hunter_validated", "suspicious")
            else:
                stats["validated"] += 1
                logger.debug(f"[Hunter] {name} ({domain}): {email_count} emails — validated")
                if not dry_run:
                    upsert_deal_metadata(conn, deal_id, "hunter_validated", "yes")

                    # Store organization name from Hunter if available
                    org_name = hunter_data.get("organization")
                    if org_name:
                        upsert_deal_metadata(conn, deal_id, "hunter_org_name", str(org_name))

        except Exception as e:
            logger.debug(f"[Hunter] Failed to validate {name} ({domain}): {e}")
            stats["skipped"] += 1

        # Commit every 10 deals
        if not dry_run and (i + 1) % 10 == 0:
            conn.commit()

        # Rate-limit politeness
        time.sleep(1.5)

    if not dry_run:
        conn.commit()

    logger.info(
        f"[Hunter] Done: {stats['validated']} validated, {stats['suspicious']} suspicious, "
        f"{stats['skipped']} skipped, {stats['rate_limited']} rate_limited"
    )
    return stats


# ═══════════════════════════════════════════════════════════════
#  Orchestrator
# ═══════════════════════════════════════════════════════════════

# Source name -> (function, kwargs_override)
_SOURCE_REGISTRY = {
    "opencorporates": (run_opencorporates_scraper, {}),
    "crunchbase": (run_crunchbase_scraper, {}),
    "ny_dos": (run_ny_dos_scraper, {}),
    "sbir": (run_sbir_scraper, {}),
    "clearbit": (enrich_with_clearbit, {}),
    "hunter": (validate_domains_with_hunter, {}),
}


def run_additional_sources(
    days_back: int = 30,
    skip: Optional[List[str]] = None,
    dry_run: bool = False,
) -> dict:
    """
    Run all additional source scrapers in sequence.

    Args:
        days_back: How far back to search (for scrapers that support it).
        skip: List of source names to skip (e.g. ["crunchbase", "sbir"]).
        dry_run: If True, don't write to database.

    Returns:
        Dict of source_name -> stats dict for each source that ran.
    """
    skip_set = set(skip or [])
    results = {}

    logger.info("=" * 60)
    logger.info("Starting additional sources pipeline")
    logger.info(f"  days_back={days_back}, skip={skip_set or 'none'}, dry_run={dry_run}")
    logger.info("=" * 60)

    for source_name, (func, default_kwargs) in _SOURCE_REGISTRY.items():
        if source_name in skip_set:
            logger.info(f"[{source_name}] Skipped (in skip list)")
            results[source_name] = None
            continue

        logger.info(f"── Running {source_name} ──")
        try:
            # Build kwargs — pass days_back and dry_run where supported
            import inspect
            sig = inspect.signature(func)
            kwargs = dict(default_kwargs)
            if "days_back" in sig.parameters:
                kwargs["days_back"] = days_back
            if "dry_run" in sig.parameters:
                kwargs["dry_run"] = dry_run

            stats = func(**kwargs)
            results[source_name] = stats
        except Exception as e:
            logger.error(f"[{source_name}] Failed: {e}")
            results[source_name] = {"error": str(e)}

    # Summary
    logger.info("=" * 60)
    logger.info("Additional sources pipeline complete")
    for source_name, stats in results.items():
        if stats is None:
            logger.info(f"  {source_name}: skipped")
        elif "error" in stats:
            logger.info(f"  {source_name}: ERROR — {stats['error']}")
        elif "new" in stats:
            logger.info(f"  {source_name}: {stats.get('new', 0)} new deals")
        elif "enriched" in stats:
            logger.info(f"  {source_name}: {stats.get('enriched', 0)} enriched")
        elif "validated" in stats:
            logger.info(
                f"  {source_name}: {stats.get('validated', 0)} validated, "
                f"{stats.get('suspicious', 0)} suspicious"
            )
    logger.info("=" * 60)

    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    run_additional_sources()
