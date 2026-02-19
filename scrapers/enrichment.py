"""
Web enrichment: Google CSE website lookup + Apollo.io org enrichment.

Fills in company_website and company_description for deals that lack them.
Both APIs have free tiers — missing env vars gracefully skip that step.
"""

import os
import re
import time
import json
import logging
from urllib.parse import urlparse

from database import get_connection, upsert_deal_metadata
from fetcher import fetch

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────

GOOGLE_CSE_DAILY_LIMIT = 95   # buffer from 100/day free tier
APOLLO_MONTHLY_LIMIT = 95     # buffer from 100/month free tier
COMPANY_SEARCH_TTL = 86400 * 30  # 30 days — company domains don't change
APOLLO_TTL = 86400 * 14          # 14 days — org data changes slowly

# Domains that are NOT real company websites
DOMAIN_BLOCKLIST = {
    "linkedin.com", "crunchbase.com", "pitchbook.com", "techcrunch.com",
    "bloomberg.com", "reuters.com", "wsj.com", "nytimes.com",
    "wikipedia.org", "wikidata.org", "facebook.com", "twitter.com",
    "x.com", "instagram.com", "youtube.com", "tiktok.com",
    "github.com", "glassdoor.com", "indeed.com", "yelp.com",
    "bbb.org", "sec.gov", "dnb.com", "zoominfo.com",
    "apollo.io", "similarweb.com", "owler.com", "tracxn.com",
    "angel.co", "wellfound.com", "f6s.com", "dealroom.co",
}

# URL path patterns that indicate a profile page, not a company homepage
_PROFILE_PATH_RE = re.compile(
    r"/(company|org|profile|people|in|pub|user|biz)/", re.IGNORECASE
)


# ── Google CSE Website Lookup ─────────────────────────────────

def _is_blocked_url(url: str) -> bool:
    """Return True if URL is on blocklist or looks like a profile page."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().lstrip("www.")
        if any(domain == d or domain.endswith("." + d) for d in DOMAIN_BLOCKLIST):
            return True
        if _PROFILE_PATH_RE.search(parsed.path):
            return True
    except Exception:
        return True
    return False


def _clean_domain(url: str) -> str:
    """Extract clean https://domain from a URL."""
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if not host:
        return None
    return f"https://{host}"


def search_company_website(company_name: str, api_key: str, cse_id: str) -> str:
    """
    Query Google Custom Search for a company's official website.
    Returns clean https://domain, "RATE_LIMITED" sentinel, or None.
    Uses fetcher.fetch() for caching.
    """
    query = f'"{company_name}" official website'
    params = {
        "key": api_key,
        "cx": cse_id,
        "q": query,
        "num": 5,
    }

    resp = fetch(
        "https://www.googleapis.com/customsearch/v1",
        params=params,
        ttl=COMPANY_SEARCH_TTL,
    )

    if resp.status_code == 429:
        logger.warning("[Google CSE] Rate limited (429)")
        return "RATE_LIMITED"

    if resp.status_code != 200:
        logger.debug(f"[Google CSE] HTTP {resp.status_code} for '{company_name}'")
        return None

    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        return None

    for item in data.get("items", []):
        link = item.get("link", "")
        if _is_blocked_url(link):
            continue
        domain = _clean_domain(link)
        if domain:
            return domain

    return None


def enrich_websites(limit: int = GOOGLE_CSE_DAILY_LIMIT, dry_run: bool = False) -> dict:
    """
    Find company websites for deals that lack one via Google Custom Search.
    Returns stats dict: {searched, found, skipped, rate_limited}.
    """
    api_key = os.environ.get("GOOGLE_CSE_API_KEY")
    cse_id = os.environ.get("GOOGLE_CSE_ID")

    if not api_key or not cse_id:
        logger.warning("[Google CSE] GOOGLE_CSE_API_KEY or GOOGLE_CSE_ID not set — skipping")
        return {"searched": 0, "found": 0, "skipped": 0, "rate_limited": 0}

    conn = get_connection()
    rows = conn.execute(
        """SELECT id, company_name FROM deals
           WHERE company_website IS NULL
           ORDER BY created_at DESC
           LIMIT ?""",
        (limit,)
    ).fetchall()

    if not rows:
        logger.info("[Google CSE] No deals missing company_website")
        return {"searched": 0, "found": 0, "skipped": 0, "rate_limited": 0}

    logger.info(f"[Google CSE] Searching websites for {len(rows)} deals (dry_run={dry_run})")

    stats = {"searched": 0, "found": 0, "skipped": 0, "rate_limited": 0}

    for i, row in enumerate(rows):
        deal_id = row["id"]
        name = row["company_name"]

        result = search_company_website(name, api_key, cse_id)
        stats["searched"] += 1

        if result == "RATE_LIMITED":
            stats["rate_limited"] += 1
            logger.warning(f"[Google CSE] Rate limited — stopping after {stats['searched']} queries")
            break

        if result is None:
            stats["skipped"] += 1
            logger.debug(f"[Google CSE] No website found for '{name}'")
        else:
            stats["found"] += 1
            logger.info(f"[Google CSE] {name} -> {result}")
            if not dry_run:
                conn.execute(
                    "UPDATE deals SET company_website = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (result, deal_id)
                )

        # Commit every 20 deals
        if not dry_run and (i + 1) % 20 == 0:
            conn.commit()

        # Rate-limit politeness
        time.sleep(1)

    if not dry_run:
        conn.commit()

    logger.info(
        f"[Google CSE] Done: {stats['searched']} searched, {stats['found']} found, "
        f"{stats['skipped']} skipped, {stats['rate_limited']} rate_limited"
    )
    return stats


# ── Apollo.io Org Enrichment ──────────────────────────────────

def _apollo_enrich_org(domain: str, api_key: str) -> dict:
    """
    Call Apollo.io org enrichment API for a domain.
    Uses fetcher.fetch() for caching + retry with backoff.
    Returns metadata dict, or sentinel strings for errors.
    """
    resp = fetch(
        f"https://api.apollo.io/api/v1/organizations/enrich?domain={domain}",
        headers={"X-Api-Key": api_key, "Content-Type": "application/json"},
        ttl=APOLLO_TTL,
    )

    if resp.status_code == 429:
        logger.warning("[Apollo] Rate limited (429)")
        return "RATE_LIMITED"
    if resp.status_code == 401:
        logger.error("[Apollo] Invalid API key (401)")
        return "AUTH_ERROR"
    if resp.status_code != 200:
        logger.debug(f"[Apollo] HTTP {resp.status_code} for {domain}")
        return None

    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        return None

    org = data.get("organization") or data
    return {
        "short_description": org.get("short_description") or org.get("description"),
        "employee_count": org.get("estimated_num_employees"),
        "linkedin_url": org.get("linkedin_url"),
        "logo_url": org.get("logo_url"),
        "founded_year": org.get("founded_year"),
        "name": org.get("name"),
        "industry": org.get("industry"),
        "keywords": org.get("keywords"),
    }


def enrich_with_apollo(limit: int = APOLLO_MONTHLY_LIMIT, dry_run: bool = False) -> dict:
    """
    Enrich deals that have a company_website but haven't been Apollo-enriched yet.
    Returns stats dict: {enriched, no_data, skipped, rate_limited, auth_error}.
    """
    api_key = os.environ.get("APOLLO_API_KEY")
    if not api_key:
        logger.warning("[Apollo] APOLLO_API_KEY not set — skipping")
        return {"enriched": 0, "no_data": 0, "skipped": 0, "rate_limited": 0, "auth_error": False}

    conn = get_connection()
    rows = conn.execute(
        """SELECT d.id, d.company_name, d.company_website, d.company_description
           FROM deals d
           LEFT JOIN deal_metadata dm ON d.id = dm.deal_id AND dm.key = 'apollo_enriched'
           WHERE d.company_website IS NOT NULL
             AND dm.value IS NULL
           ORDER BY d.created_at DESC
           LIMIT ?""",
        (limit,)
    ).fetchall()

    if not rows:
        logger.info("[Apollo] No deals need Apollo enrichment")
        return {"enriched": 0, "no_data": 0, "skipped": 0, "rate_limited": 0, "auth_error": False}

    logger.info(f"[Apollo] Enriching {len(rows)} deals (dry_run={dry_run})")

    stats = {"enriched": 0, "no_data": 0, "skipped": 0, "rate_limited": 0, "auth_error": False}

    for i, row in enumerate(rows):
        deal_id = row["id"]
        name = row["company_name"]
        website = row["company_website"]

        # Extract domain from website URL
        parsed = urlparse(website)
        domain = parsed.netloc.lower().lstrip("www.") if parsed.netloc else website.lower().lstrip("www.")
        if not domain:
            stats["skipped"] += 1
            continue

        result = _apollo_enrich_org(domain, api_key)

        if result == "RATE_LIMITED":
            stats["rate_limited"] += 1
            logger.warning(f"[Apollo] Rate limited — stopping after {i} queries")
            break
        if result == "AUTH_ERROR":
            stats["auth_error"] = True
            logger.error("[Apollo] Auth error — stopping")
            break

        if result is None or not any(result.values()):
            stats["no_data"] += 1
            logger.debug(f"[Apollo] No data for {name} ({domain})")
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "apollo_enriched", "no_data")
        else:
            stats["enriched"] += 1
            logger.info(f"[Apollo] Enriched {name}: {result.get('employee_count')} employees, {result.get('industry')}")

            if not dry_run:
                # Store metadata keys
                for meta_key in ("employee_count", "logo_url", "linkedin_url", "founded_year", "industry", "keywords"):
                    val = result.get(meta_key)
                    if val is not None:
                        str_val = json.dumps(val) if isinstance(val, (list, dict)) else str(val)
                        upsert_deal_metadata(conn, deal_id, meta_key, str_val)

                # Update company_description if empty
                desc = result.get("short_description")
                if desc and not row["company_description"]:
                    conn.execute(
                        "UPDATE deals SET company_description = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (desc[:500], deal_id)
                    )

                upsert_deal_metadata(conn, deal_id, "apollo_enriched", "yes")

        # Commit every 10 deals
        if not dry_run and (i + 1) % 10 == 0:
            conn.commit()

        # Rate-limit politeness
        time.sleep(1.5)

    if not dry_run:
        conn.commit()

    logger.info(
        f"[Apollo] Done: {stats['enriched']} enriched, {stats['no_data']} no_data, "
        f"{stats['skipped']} skipped, {stats['rate_limited']} rate_limited"
    )
    return stats


# ── Orchestrator ──────────────────────────────────────────────

def run_web_enrichment(
    google_limit: int = GOOGLE_CSE_DAILY_LIMIT,
    apollo_limit: int = APOLLO_MONTHLY_LIMIT,
    skip_google: bool = False,
    skip_apollo: bool = False,
    dry_run: bool = False,
):
    """Run Google CSE website lookup, then Apollo org enrichment."""
    logger.info("=" * 50)
    logger.info("Starting web enrichment pipeline")
    logger.info("=" * 50)

    google_stats = None
    apollo_stats = None

    if not skip_google:
        google_stats = enrich_websites(limit=google_limit, dry_run=dry_run)
    else:
        logger.info("[Google CSE] Skipped (--skip-google)")

    if not skip_apollo:
        apollo_stats = enrich_with_apollo(limit=apollo_limit, dry_run=dry_run)
    else:
        logger.info("[Apollo] Skipped (--skip-apollo)")

    # Summary
    logger.info("=" * 50)
    logger.info("Web enrichment complete")
    if google_stats:
        logger.info(f"  Google CSE: {google_stats['found']}/{google_stats['searched']} websites found")
    if apollo_stats:
        logger.info(f"  Apollo:     {apollo_stats['enriched']} deals enriched")
    logger.info("=" * 50)

    return {"google": google_stats, "apollo": apollo_stats}
