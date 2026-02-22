"""
Enrichment cascade: 5 free/low-cost data sources for filling deal gaps.

Sources (run in dependency order):
  1. Clearbit Autocomplete — name -> domain (NO key needed)
  2. Google Knowledge Graph — website + description (needs GOOGLE_KG_API_KEY)
  3. YC-OSS Static Dataset — bulk match, website + description (NO key)
  4. Wikipedia API — descriptions (NO key)
  5. Wikidata SPARQL — website + investors (NO key)

The cascade order matters: Clearbit gets domains first, then later sources
can use those domains or fill remaining gaps.
"""

import re
import time
import json
import logging
from urllib.parse import urlparse, quote

from database import get_connection, upsert_deal_metadata, upsert_investor, link_deal_investor
from fetcher import fetch
from scrapers.enrichment import DOMAIN_BLOCKLIST, _is_blocked_url
from config import (
    CLEARBIT_AUTOCOMPLETE_TTL,
    GOOGLE_KG_TTL,
    GOOGLE_KG_API_KEY,
    GOOGLE_KG_DAILY_LIMIT,
    YC_OSS_TTL,
    WIKIPEDIA_TTL,
    WIKIPEDIA_RATE_DELAY,
    WIKIDATA_TTL,
    WIKIDATA_BATCH_SIZE,
)

logger = logging.getLogger(__name__)

_STRIP_RE = re.compile(r"[^a-z0-9\s]")
_CORP_SUFFIXES = {"inc", "corp", "llc", "ltd", "co", "company",
                  "incorporated", "limited", "group", "holdings"}


def _normalize_for_match(name: str) -> str:
    """Lowercase, strip punctuation (keep spaces) for fuzzy matching."""
    return _STRIP_RE.sub("", (name or "").lower()).strip()


def _name_similarity(a: str, b: str) -> float:
    """Simple token-overlap similarity ratio (0-1) between two normalized names."""
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    if not tokens_a or not tokens_b:
        return 0.0
    overlap = len(tokens_a & tokens_b)
    return (2.0 * overlap) / (len(tokens_a) + len(tokens_b))


def _log_gap_summary(conn, label: str):
    """Log current gap counts for deals."""
    total = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
    no_website = conn.execute(
        "SELECT COUNT(*) FROM deals WHERE company_website IS NULL"
    ).fetchone()[0]
    no_desc = conn.execute(
        "SELECT COUNT(*) FROM deals WHERE company_description IS NULL OR company_description = ''"
    ).fetchone()[0]
    no_investors = conn.execute("""
        SELECT COUNT(*) FROM deals d
        WHERE NOT EXISTS (
            SELECT 1 FROM deal_investors di WHERE di.deal_id = d.id
        )
    """).fetchone()[0]
    logger.info(
        f"[{label}] Gaps: {no_website}/{total} no website, "
        f"{no_desc}/{total} no description, "
        f"{no_investors}/{total} no investors"
    )


# ── Source 1: Clearbit Autocomplete ─────────────────────────────


def enrich_clearbit_autocomplete(limit: int = 500, dry_run: bool = False) -> dict:
    """
    Use Clearbit Autocomplete (free, no key) to find company domains.
    URL: https://autocomplete.clearbit.com/v1/companies/suggest?query={name}
    """
    logger.info("=" * 50)
    logger.info("[Clearbit Autocomplete] Starting domain lookup")

    conn = get_connection()
    rows = conn.execute(
        """SELECT d.id, d.company_name FROM deals d
           LEFT JOIN deal_metadata dm ON d.id = dm.deal_id AND dm.key = 'clearbit_auto_enriched'
           WHERE d.company_website IS NULL
             AND dm.value IS NULL
           ORDER BY d.created_at DESC
           LIMIT ?""",
        (limit,)
    ).fetchall()

    if not rows:
        logger.info("[Clearbit Autocomplete] No deals need domain lookup")
        return {"searched": 0, "found": 0, "no_data": 0}

    logger.info(f"[Clearbit Autocomplete] Looking up {len(rows)} deals (dry_run={dry_run})")
    stats = {"searched": 0, "found": 0, "no_data": 0}

    for i, row in enumerate(rows):
        deal_id = row["id"]
        name = row["company_name"]

        resp = fetch(
            f"https://autocomplete.clearbit.com/v1/companies/suggest?query={quote(name)}",
            ttl=CLEARBIT_AUTOCOMPLETE_TTL,
        )
        stats["searched"] += 1

        if resp.status_code != 200:
            stats["no_data"] += 1
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "clearbit_auto_enriched", "no_data")
            continue

        try:
            suggestions = resp.json()
        except (json.JSONDecodeError, ValueError):
            stats["no_data"] += 1
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "clearbit_auto_enriched", "no_data")
            continue

        domain = None
        query_norm = _normalize_for_match(name)
        for s in suggestions:
            d = s.get("domain", "")
            if not d or any(d == bl or d.endswith("." + bl) for bl in DOMAIN_BLOCKLIST):
                continue
            # Verify the suggestion name is close to the deal name
            suggestion_norm = _normalize_for_match(s.get("name", ""))
            if not suggestion_norm or not query_norm:
                continue
            # Token-level match: require strong name alignment.
            # Single-word queries are ambiguous, so require exact token match
            # (the suggestion must also be that single word, ignoring suffixes
            # like Inc/Corp/Ltd). Multi-word queries use token containment.
            query_tokens = set(query_norm.split())
            suggestion_tokens = set(suggestion_norm.split())
            suggestion_core = suggestion_tokens - _CORP_SUFFIXES
            if len(query_tokens) == 1:
                tokens_match = query_tokens == suggestion_core
            else:
                tokens_match = (query_tokens <= suggestion_tokens
                                or suggestion_core <= query_tokens)
            if (tokens_match
                    or _name_similarity(query_norm, suggestion_norm) >= 0.8):
                domain = d
                break
            else:
                logger.debug(
                    f"[Clearbit Autocomplete] Skipping '{s.get('name')}' for '{name}' — low similarity"
                )

        if domain:
            stats["found"] += 1
            website = f"https://{domain}"
            logger.info(f"[Clearbit Autocomplete] {name} -> {website}")
            if not dry_run:
                conn.execute(
                    "UPDATE deals SET company_website = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (website, deal_id)
                )
                upsert_deal_metadata(conn, deal_id, "clearbit_auto_enriched", "yes")
        else:
            stats["no_data"] += 1
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "clearbit_auto_enriched", "no_data")

        if not dry_run and (i + 1) % 20 == 0:
            conn.commit()

        time.sleep(0.5)

    if not dry_run:
        conn.commit()

    logger.info(
        f"[Clearbit Autocomplete] Done: {stats['searched']} searched, "
        f"{stats['found']} found, {stats['no_data']} no_data"
    )
    return stats


# ── Source 2: Google Knowledge Graph ────────────────────────────


def enrich_google_kg(limit: int = 500, dry_run: bool = False) -> dict:
    """
    Use Google Knowledge Graph API to find websites and descriptions.
    Requires GOOGLE_KG_API_KEY env var.
    """
    logger.info("=" * 50)
    logger.info("[Google KG] Starting enrichment")

    if not GOOGLE_KG_API_KEY:
        logger.warning("[Google KG] GOOGLE_KG_API_KEY not set — skipping")
        return {"searched": 0, "found": 0, "no_data": 0, "skipped": True}

    conn = get_connection()
    rows = conn.execute(
        """SELECT d.id, d.company_name, d.company_website, d.company_description
           FROM deals d
           LEFT JOIN deal_metadata dm ON d.id = dm.deal_id AND dm.key = 'kg_enriched'
           WHERE (d.company_website IS NULL
                  OR d.company_description IS NULL
                  OR d.company_description = '')
             AND dm.value IS NULL
           ORDER BY d.created_at DESC
           LIMIT ?""",
        (min(limit, GOOGLE_KG_DAILY_LIMIT),)
    ).fetchall()

    if not rows:
        logger.info("[Google KG] No deals need KG enrichment")
        return {"searched": 0, "found": 0, "no_data": 0, "skipped": False}

    logger.info(f"[Google KG] Enriching {len(rows)} deals (dry_run={dry_run})")
    stats = {"searched": 0, "found": 0, "no_data": 0, "skipped": False}

    for i, row in enumerate(rows):
        deal_id = row["id"]
        name = row["company_name"]

        resp = fetch(
            "https://kgsearch.googleapis.com/v1/entities:search",
            params={
                "query": name,
                "types": "Organization",
                "key": GOOGLE_KG_API_KEY,
                "limit": 3,
            },
            ttl=GOOGLE_KG_TTL,
        )
        stats["searched"] += 1

        if resp.status_code == 429:
            logger.warning("[Google KG] Rate limited — stopping")
            break

        if resp.status_code != 200:
            stats["no_data"] += 1
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "kg_enriched", "no_data")
            continue

        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError):
            stats["no_data"] += 1
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "kg_enriched", "no_data")
            continue

        found_something = False
        for item in data.get("itemListElement", []):
            result = item.get("result", {})

            # Website
            if not row["company_website"]:
                url = result.get("url")
                if url and not _is_blocked_url(url):
                    parsed = urlparse(url)
                    website = f"https://{parsed.netloc.lower()}"
                    if not dry_run:
                        conn.execute(
                            "UPDATE deals SET company_website = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                            (website, deal_id)
                        )
                    logger.info(f"[Google KG] {name} -> website: {website}")
                    found_something = True

            # Description
            if not row["company_description"]:
                desc_obj = result.get("detailedDescription", {})
                desc = desc_obj.get("articleBody", "")
                if desc and len(desc) > 10:
                    if not dry_run:
                        conn.execute(
                            "UPDATE deals SET company_description = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                            (desc[:500], deal_id)
                        )
                    logger.info(f"[Google KG] {name} -> description ({len(desc)} chars)")
                    found_something = True

            if found_something:
                break

        if found_something:
            stats["found"] += 1
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "kg_enriched", "yes")
        else:
            stats["no_data"] += 1
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "kg_enriched", "no_data")

        if not dry_run and (i + 1) % 20 == 0:
            conn.commit()

        time.sleep(0.2)

    if not dry_run:
        conn.commit()

    logger.info(
        f"[Google KG] Done: {stats['searched']} searched, "
        f"{stats['found']} found, {stats['no_data']} no_data"
    )
    return stats


# ── Source 3: YC-OSS Static Dataset ────────────────────────────


def enrich_yc_oss(dry_run: bool = False) -> dict:
    """
    Bulk match deals against Y Combinator's open-source company dataset.
    URL: https://yc-oss.github.io/api/companies/all.json (~5,690 companies)
    No API key needed.
    """
    logger.info("=" * 50)
    logger.info("[YC-OSS] Starting bulk match")

    resp = fetch(
        "https://yc-oss.github.io/api/companies/all.json",
        ttl=YC_OSS_TTL,
    )

    if resp.status_code != 200:
        logger.warning(f"[YC-OSS] Failed to fetch dataset: HTTP {resp.status_code}")
        return {"matched": 0, "no_match": 0, "error": True}

    try:
        yc_companies = resp.json()
    except (json.JSONDecodeError, ValueError):
        logger.warning("[YC-OSS] Failed to parse JSON")
        return {"matched": 0, "no_match": 0, "error": True}

    # Build lookup by normalized name
    yc_lookup = {}
    for company in yc_companies:
        name = company.get("name", "")
        if name:
            yc_lookup[_normalize_for_match(name)] = company

    logger.info(f"[YC-OSS] Loaded {len(yc_lookup)} companies from YC dataset")

    conn = get_connection()
    rows = conn.execute(
        """SELECT d.id, d.company_name, d.company_website, d.company_description
           FROM deals d
           LEFT JOIN deal_metadata dm ON d.id = dm.deal_id AND dm.key = 'yc_oss_enriched'
           WHERE (d.company_website IS NULL
                  OR d.company_description IS NULL
                  OR d.company_description = '')
             AND dm.value IS NULL
           ORDER BY d.created_at DESC"""
    ).fetchall()

    if not rows:
        logger.info("[YC-OSS] No deals need YC-OSS enrichment")
        return {"matched": 0, "no_match": 0, "error": False}

    logger.info(f"[YC-OSS] Checking {len(rows)} deals against YC dataset (dry_run={dry_run})")
    stats = {"matched": 0, "no_match": 0, "error": False}

    for i, row in enumerate(rows):
        deal_id = row["id"]
        name = row["company_name"]
        norm = _normalize_for_match(name)

        yc = yc_lookup.get(norm)
        if not yc:
            stats["no_match"] += 1
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "yc_oss_enriched", "no_match")
            continue

        stats["matched"] += 1
        logger.info(f"[YC-OSS] Matched: {name} -> {yc.get('name')}")

        if not dry_run:
            # Fill website if missing
            if not row["company_website"]:
                url = yc.get("url")
                if url and not _is_blocked_url(url):
                    conn.execute(
                        "UPDATE deals SET company_website = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (url, deal_id)
                    )

            # Fill description if missing
            if not row["company_description"]:
                one_liner = yc.get("one_liner", "")
                if one_liner and len(one_liner) > 5:
                    conn.execute(
                        "UPDATE deals SET company_description = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (one_liner[:500], deal_id)
                    )

            # Store YC-specific metadata
            for meta_key, yc_key in [
                ("yc_batch", "batch"),
                ("yc_status", "status"),
                ("yc_industries", "industries"),
                ("yc_team_size", "team_size"),
            ]:
                val = yc.get(yc_key)
                if val is not None:
                    str_val = json.dumps(val) if isinstance(val, (list, dict)) else str(val)
                    upsert_deal_metadata(conn, deal_id, meta_key, str_val)

            upsert_deal_metadata(conn, deal_id, "yc_oss_enriched", "yes")

        if not dry_run and (i + 1) % 50 == 0:
            conn.commit()

    if not dry_run:
        conn.commit()

    logger.info(
        f"[YC-OSS] Done: {stats['matched']} matched, {stats['no_match']} no_match "
        f"(out of {len(rows)} checked)"
    )
    return stats


# ── Source 4: Wikipedia API ─────────────────────────────────────

_WIKI_UA = "NYCVCScraper/1.0 (contact@example.com)"


def enrich_wikipedia(limit: int = 200, dry_run: bool = False) -> dict:
    """
    Use Wikipedia API to get company descriptions.
    Respects Wikimedia rate limits (1 req/sec).
    """
    logger.info("=" * 50)
    logger.info("[Wikipedia] Starting description enrichment")

    conn = get_connection()
    rows = conn.execute(
        """SELECT d.id, d.company_name
           FROM deals d
           LEFT JOIN deal_metadata dm ON d.id = dm.deal_id AND dm.key = 'wikipedia_enriched'
           WHERE (d.company_description IS NULL OR d.company_description = '')
             AND dm.value IS NULL
           ORDER BY d.created_at DESC
           LIMIT ?""",
        (limit,)
    ).fetchall()

    if not rows:
        logger.info("[Wikipedia] No deals need Wikipedia enrichment")
        return {"searched": 0, "found": 0, "no_article": 0}

    logger.info(f"[Wikipedia] Looking up {len(rows)} deals (dry_run={dry_run})")
    stats = {"searched": 0, "found": 0, "no_article": 0}

    for i, row in enumerate(rows):
        deal_id = row["id"]
        name = row["company_name"]

        resp = fetch(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "titles": name,
                "prop": "extracts",
                "exintro": "true",
                "explaintext": "true",
                "format": "json",
                "redirects": "1",
            },
            headers={"User-Agent": _WIKI_UA},
            ttl=WIKIPEDIA_TTL,
        )
        stats["searched"] += 1

        if resp.status_code != 200:
            stats["no_article"] += 1
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "wikipedia_enriched", "no_article")
            continue

        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError):
            stats["no_article"] += 1
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "wikipedia_enriched", "no_article")
            continue

        pages = data.get("query", {}).get("pages", {})
        found_something = False

        for page_id, page in pages.items():
            if page_id == "-1":
                continue

            # Extract intro text as description
            extract = page.get("extract", "")
            if extract and len(extract) > 20:
                # Take first paragraph or up to 500 chars
                desc = extract.split("\n\n")[0][:500]
                if not dry_run:
                    conn.execute(
                        "UPDATE deals SET company_description = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (desc, deal_id)
                    )
                logger.info(f"[Wikipedia] {name} -> description ({len(desc)} chars)")
                found_something = True

            break  # only use first page

        if found_something:
            stats["found"] += 1
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "wikipedia_enriched", "yes")
        else:
            stats["no_article"] += 1
            if not dry_run:
                upsert_deal_metadata(conn, deal_id, "wikipedia_enriched", "no_article")

        if not dry_run and (i + 1) % 20 == 0:
            conn.commit()

        time.sleep(WIKIPEDIA_RATE_DELAY)

    if not dry_run:
        conn.commit()

    logger.info(
        f"[Wikipedia] Done: {stats['searched']} searched, "
        f"{stats['found']} found, {stats['no_article']} no_article"
    )
    return stats


# ── Source 5: Wikidata SPARQL ───────────────────────────────────

_WIKIDATA_SPARQL_TEMPLATE = """
SELECT ?item ?itemLabel ?website ?investorLabel ?fundingLabel WHERE {{
  VALUES ?name {{ {values} }}
  ?item rdfs:label ?name .
  ?item wdt:P31/wdt:P279* wd:Q4830453 .
  OPTIONAL {{ ?item wdt:P856 ?website . }}
  OPTIONAL {{ ?item wdt:P1951 ?investor .
              ?investor rdfs:label ?investorLabel .
              FILTER(LANG(?investorLabel) = "en") }}
  OPTIONAL {{ ?item wdt:P4135 ?funding .
              ?funding rdfs:label ?fundingLabel .
              FILTER(LANG(?fundingLabel) = "en") }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
  FILTER(LANG(?name) = "en")
}}
"""


def enrich_wikidata(limit: int = 200, dry_run: bool = False) -> dict:
    """
    Use Wikidata SPARQL to find websites and investor relationships.
    Batches queries using VALUES clause (WIKIDATA_BATCH_SIZE per request).
    """
    logger.info("=" * 50)
    logger.info("[Wikidata] Starting SPARQL enrichment")

    conn = get_connection()
    rows = conn.execute(
        """SELECT d.id, d.company_name, d.company_website
           FROM deals d
           LEFT JOIN deal_metadata dm ON d.id = dm.deal_id AND dm.key = 'wikidata_enriched'
           WHERE dm.value IS NULL
           ORDER BY d.created_at DESC
           LIMIT ?""",
        (limit,)
    ).fetchall()

    if not rows:
        logger.info("[Wikidata] No deals need Wikidata enrichment")
        return {"searched": 0, "matched": 0, "investors_found": 0, "no_match": 0}

    logger.info(f"[Wikidata] Querying {len(rows)} deals in batches of {WIKIDATA_BATCH_SIZE} (dry_run={dry_run})")
    stats = {"searched": 0, "matched": 0, "investors_found": 0, "no_match": 0}

    # Process in batches
    for batch_start in range(0, len(rows), WIKIDATA_BATCH_SIZE):
        batch = rows[batch_start:batch_start + WIKIDATA_BATCH_SIZE]
        names = [row["company_name"] for row in batch]

        # Build SPARQL VALUES clause (escape quotes to prevent injection)
        values_str = " ".join(
            '"%s"@en' % n.replace("\\", "\\\\").replace('"', '\\"')
            for n in names
        )
        query = _WIKIDATA_SPARQL_TEMPLATE.format(values=values_str)

        resp = fetch(
            "https://query.wikidata.org/sparql",
            params={"query": query, "format": "json"},
            headers={
                "User-Agent": _WIKI_UA,
                "Accept": "application/sparql-results+json",
            },
            ttl=WIKIDATA_TTL,
        )

        if resp.status_code != 200:
            logger.warning(f"[Wikidata] SPARQL query failed: HTTP {resp.status_code}")
            # Mark batch as no_match
            for row in batch:
                stats["no_match"] += 1
                if not dry_run:
                    upsert_deal_metadata(conn, row["id"], "wikidata_enriched", "no_match")
            stats["searched"] += len(batch)
            time.sleep(2.0)
            continue

        try:
            data = resp.json()
        except (json.JSONDecodeError, ValueError):
            logger.warning("[Wikidata] Failed to parse SPARQL response")
            for row in batch:
                stats["no_match"] += 1
                if not dry_run:
                    upsert_deal_metadata(conn, row["id"], "wikidata_enriched", "no_match")
            stats["searched"] += len(batch)
            time.sleep(2.0)
            continue

        # Parse results: group by itemLabel
        results_by_name = {}
        for binding in data.get("results", {}).get("bindings", []):
            label = binding.get("itemLabel", {}).get("value", "")
            norm_label = _normalize_for_match(label)
            if norm_label not in results_by_name:
                results_by_name[norm_label] = {
                    "website": None,
                    "investors": set(),
                }
            entry = results_by_name[norm_label]

            website = binding.get("website", {}).get("value")
            if website and not entry["website"] and not _is_blocked_url(website):
                entry["website"] = website

            investor = binding.get("investorLabel", {}).get("value")
            if investor:
                entry["investors"].add(investor)

        stats["searched"] += len(batch)

        # Apply results to deals
        for row in batch:
            deal_id = row["id"]
            norm = _normalize_for_match(row["company_name"])
            wd = results_by_name.get(norm)

            if not wd:
                stats["no_match"] += 1
                if not dry_run:
                    upsert_deal_metadata(conn, deal_id, "wikidata_enriched", "no_match")
                continue

            stats["matched"] += 1

            if not dry_run:
                # Fill website if missing
                if not row["company_website"] and wd["website"]:
                    parsed = urlparse(wd["website"])
                    website = f"https://{parsed.netloc.lower()}"
                    conn.execute(
                        "UPDATE deals SET company_website = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (website, deal_id)
                    )
                    logger.info(f"[Wikidata] {row['company_name']} -> website: {website}")

                # Store investors
                if wd["investors"]:
                    investors_list = sorted(wd["investors"])
                    upsert_deal_metadata(
                        conn, deal_id, "wikidata_investors",
                        json.dumps(investors_list)
                    )
                    stats["investors_found"] += len(investors_list)
                    logger.info(
                        f"[Wikidata] {row['company_name']} -> "
                        f"{len(investors_list)} investors: {', '.join(investors_list[:3])}"
                    )

                    # Create investor records and link to deal
                    for inv_name in investors_list:
                        try:
                            inv_id = upsert_investor(conn, name=inv_name)
                            link_deal_investor(conn, deal_id, inv_id)
                        except Exception as e:
                            logger.debug(f"[Wikidata] Failed to create investor '{inv_name}': {e}")

                upsert_deal_metadata(conn, deal_id, "wikidata_enriched", "yes")

        if not dry_run:
            conn.commit()

        time.sleep(2.0)

    logger.info(
        f"[Wikidata] Done: {stats['searched']} searched, {stats['matched']} matched, "
        f"{stats['investors_found']} investors found, {stats['no_match']} no_match"
    )
    return stats


# ── Cascade Orchestrator ────────────────────────────────────────


def run_enrichment_cascade(
    skip: list = None,
    dry_run: bool = False,
    clearbit_limit: int = 500,
    kg_limit: int = 500,
    wikipedia_limit: int = 200,
    wikidata_limit: int = 200,
) -> dict:
    """
    Run all enrichment sources in cascade order.
    Sources: clearbit_autocomplete -> google_kg -> yc_oss -> wikipedia -> wikidata
    """
    skip = set(s.lower().replace("-", "_") for s in (skip or []))
    results = {}

    logger.info("=" * 60)
    logger.info("Starting enrichment cascade")
    logger.info(f"  dry_run={dry_run}, skip={skip or 'none'}")
    logger.info("=" * 60)

    conn = get_connection()
    _log_gap_summary(conn, "Before cascade")

    sources = [
        ("clearbit_autocomplete", lambda: enrich_clearbit_autocomplete(limit=clearbit_limit, dry_run=dry_run)),
        ("google_kg", lambda: enrich_google_kg(limit=kg_limit, dry_run=dry_run)),
        ("yc_oss", lambda: enrich_yc_oss(dry_run=dry_run)),
        ("wikipedia", lambda: enrich_wikipedia(limit=wikipedia_limit, dry_run=dry_run)),
        ("wikidata", lambda: enrich_wikidata(limit=wikidata_limit, dry_run=dry_run)),
    ]

    for name, fn in sources:
        if name in skip:
            logger.info(f"[Cascade] Skipping {name}")
            results[name] = {"skipped": True}
            continue

        try:
            results[name] = fn()
        except Exception as e:
            logger.error(f"[Cascade] {name} failed: {e}")
            results[name] = {"error": str(e)}

        _log_gap_summary(conn, f"After {name}")

    logger.info("=" * 60)
    logger.info("Enrichment cascade complete")
    for name, stats in results.items():
        logger.info(f"  {name}: {stats}")
    logger.info("=" * 60)

    return results
