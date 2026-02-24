"""
SEC EDGAR Form D Scraper (Fixed)
Three methods: EFTS full-text, Atom feed (NY), Atom feed (DE) + XML parse.
180-day window, pagination, normalized dedup.
"""

import re
import json
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup

from database import (
    get_connection, batch_connection, insert_deal, upsert_firm, upsert_investor,
    link_deal_firm, link_deal_investor, get_category_id,
    log_scrape, finish_scrape
)
from fetcher import fetch, SEC_HEADERS
from scrapers.utils import (
    classify_stage_from_amount, normalize_company_name, classify_sector,
    should_skip_deal, validate_deal_amount, link_investors_to_deal,
)
from quality_control import validate_deal

logger = logging.getLogger(__name__)

# ── Multi-city zip codes and keywords for address filtering ──────────
from config import CITY_REGISTRY, ENABLED_CITIES

# Build zip sets and keyword lists per city from the registry
_CITY_ZIPS = {}
_CITY_KEYWORDS = {}
for _cn, _cfg in CITY_REGISTRY.items():
    _zips = set()
    for prefix in _cfg.get("zip_prefixes", []):
        for i in range(100):
            _zips.add(f"{prefix}{i:02d}"[:5])
    _CITY_ZIPS[_cn] = _zips
    _CITY_KEYWORDS[_cn] = [ind.lower() for ind in _cfg.get("indicators", [])]

# Legacy aliases for backward compat
NYC_ZIPS = _CITY_ZIPS.get("New York", set())
NYC_KEYWORDS = _CITY_KEYWORDS.get("New York", [])


def _detect_city_from_address(text: str) -> str:
    """Detect city from an address/text. Returns city name or None."""
    t = text.lower()
    for city_name in ENABLED_CITIES:
        keywords = _CITY_KEYWORDS.get(city_name, [])
        if any(kw in t for kw in keywords):
            return city_name
    return None


def _is_nyc(text: str) -> bool:
    """Check if an address/text refers to NYC (backward compat)."""
    return _detect_city_from_address(text) == "New York"


# ── Junk-filing filters ─────────────────────────────────────────

# Entity suffixes that almost always indicate non-startup structures.
# NOTE: LLC and LP are intentionally NOT here — many legitimate startups
# file as LLCs/LPs. Fund-like LLCs are caught by _ENTITY_KEYWORD_BLOCKLIST
# (which checks for "Fund", "Holdings", "Partners", etc.).
_ENTITY_BLOCKLIST = re.compile(
    r"""(?ix)              # case-insensitive, verbose
    (?:^|\s)               # word boundary
    (?:
        REIT               # Real Estate Investment Trust
      | DST                # Delaware Statutory Trust
      | SPV                # Special Purpose Vehicle
      | PLC(?:/ADR)?       # PLC, PLC/ADR
      | /ADR               # American Depositary Receipts (public companies)
      | SCSp               # Luxembourg special limited partnership
      | SARL               # Luxembourg private limited company
      | SCA                # Partnership limited by shares
      | EB[\s-]?5          # EB-5 immigrant investor funds
    )
    (?:\s|$|[.,])          # word boundary / end
    """,
)

_ENTITY_KEYWORD_BLOCKLIST = re.compile(
    r"""(?ix)
    (?:
        \bFund\b                   # "Fund" alone (catches "ABC Fund LP", "Fund III")
      | \bFeeder\b                 # feeder fund
      | \bHoldings?\b
      | \bPartners(?:hip)?\b       # "Partners" or "Partnership"
      | \bAssociates?\b
      | \bInvestors?\b
      | \bRealty\b
      | \bEstate\b
      | \bVentures\s+Fund\b
      | \bCapital\s+Fund\b
      | \bCapital\s+Partners\b
      | \bCapital\s+Management\b
      | \bAcquisition\s+Corp\b
      | \bMaster\s+Fund\b
      | \bInvestment\b
      | \bPooled\b
      | \bInsurance\b
      | \bAnnuity\b
      | \bDiocese\b
      | \bTrust\b
      | \bSeries\s+[A-Z0-9IVX]+\b  # "Series III", "Series A-1" (fund series)
      | \bOffshore\b
      | \bOnshore\b
      | \bCo-Invest\b
      | \bSidecar\b
      | \bLP\s*-\s*Series\b         # "LP - Series III"
      | \bIDF\b                     # Interval/Drawdown Fund
      | \bAsset\s+Backed\b
      | \bCredit\s+Co\b
      | \bConglomerate\b
      | \bBDC\b                     # Business Development Company
      | \bCorp(?:oration)?\b(?=.*\b(?:Acquisition|Blank\s+Check|SPAC)\b)
    )
    """,
)

# Names that start with a street address (e.g. "130 Graham Funding L.P.", "102-43 Corona Ave")
_ADDRESS_NAME_RE = re.compile(r"^\d+[-\d]*\s+\w")

# CIK suffix appended by EDGAR (e.g. "Acme Inc (CIK 0002012881)")
_CIK_SUFFIX_RE = re.compile(r"\s*\(CIK\s*\d+\)\s*$", re.I)

# SEC industryGroupType values that are almost never startups
_JUNK_INDUSTRY_GROUPS = {
    "real estate",
    "pooled investment fund",
    "banking and financial services",
    "investing",
    "insurance",
}


def _clean_sec_name(name: str) -> str:
    """Strip CIK suffixes and extra whitespace from SEC entity names."""
    if not name:
        return name
    return _CIK_SUFFIX_RE.sub("", name).strip()


def _is_junk_sec_company(name: str) -> bool:
    """Return True if the company name matches junk entity patterns."""
    if not name:
        return True
    n = name.strip()
    if _ADDRESS_NAME_RE.match(n):
        return True
    if _ENTITY_BLOCKLIST.search(n):
        return True
    if _ENTITY_KEYWORD_BLOCKLIST.search(n):
        return True
    # ADR suffix (public company depositary receipts, not startups)
    if n.upper().endswith("/ADR") or n.upper().endswith(" ADR"):
        return True
    return False


def _should_keep_sec_filing(company_name: str, details: Optional[Dict], conn) -> bool:
    """
    Master gate — returns True only if the filing looks like a real startup deal.
    Combines name filter, industry filter, amount validation, and VC-firm check.
    """
    # 1. Name filter
    if _is_junk_sec_company(company_name):
        logger.debug(f"Skipping junk name: {company_name}")
        return False

    # 2. Entity type filter — only keep corporations (C-corps / S-corps)
    #    Funds file as Limited Partnership, LLC, etc. — not startups.
    if details:
        entity_type = (details.get("entity_type") or "").strip()
        if entity_type and "corporation" not in entity_type.lower():
            logger.debug(f"Skipping non-corp entity type '{entity_type}': {company_name}")
            return False

    # 3. Industry group filter (from Form D XML)
    if details:
        industry = (details.get("industry") or "").lower().strip()
        if industry in _JUNK_INDUSTRY_GROUPS:
            logger.debug(f"Skipping junk industry '{industry}': {company_name}")
            return False

    # 3. Amount validation
    amount = None
    if details:
        amount = details.get("amount_sold") or details.get("total_offering")
    stage = classify_stage_from_amount(amount)
    if not validate_deal_amount(amount, stage):
        logger.debug(f"Skipping invalid amount {amount}: {company_name}")
        return False

    # 4. Minimum enrichment — reject if we got zero useful data from the Form D XML
    if details:
        has_amount = bool(details.get("amount_sold") or details.get("total_offering"))
        has_indefinite = bool(details.get("amount_sold_indefinite") or details.get("total_offering_indefinite"))
        has_industry = bool((details.get("industry") or "").strip())
        has_investors = bool(details.get("investors_count"))
        has_city = bool((details.get("city") or "").strip())
        if not has_amount and not has_indefinite and not has_industry and not has_investors and not has_city:
            logger.debug(f"Skipping no-data filing: {company_name}")
            return False
    else:
        # No XML details at all — still allow if we have a valid name and filing info
        # (EFTS gives us company name, filing date, CIK even without XML)
        return True

    # 5. VC firm check — reject if the "company" is actually a known VC firm
    skip_reason = should_skip_deal(conn, company_name)
    if skip_reason:
        logger.debug(f"Skipping: {skip_reason}")
        return False

    return True


# ═══════════════════════════════════════════════════════════════
#  METHOD A: EDGAR EFTS Full-Text Search API
# ═══════════════════════════════════════════════════════════════

def search_efts(query: str, days_back: int = 180, max_results: int = 200) -> List[Dict]:
    """
    Search EDGAR full-text search for Form D filings.
    Paginates through all results (100 at a time).
    """
    results = []
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    offset = 0
    page_size = 100

    while offset < max_results:
        url = "https://efts.sec.gov/LATEST/search-index"
        params = {
            "q": query,
            "forms": "D,D/A",
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
            "from": offset,
            "size": min(page_size, max_results - offset),
        }

        try:
            resp = fetch(url, headers=SEC_HEADERS, params=params, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"EFTS returned HTTP {resp.status_code}")
                break

            ct = resp.headers.get("content-type", "")
            if "json" not in ct:
                logger.warning(f"EFTS returned non-JSON: {ct}")
                break

            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
            total = data.get("hits", {}).get("total", {})
            total_count = total.get("value", 0) if isinstance(total, dict) else total

            for hit in hits:
                src = hit.get("_source", {})
                # EFTS fields: display_names (array), ciks (array), adsh, file_date
                display_names = src.get("display_names", [])
                name = display_names[0] if display_names else "Unknown"
                ciks = src.get("ciks", [])
                cik = ciks[0].lstrip("0") if ciks else None
                biz_locs = src.get("biz_locations", [])
                biz_states = src.get("biz_states", [])
                results.append({
                    "company_name": name,
                    "filing_date": src.get("file_date"),
                    "accession_number": src.get("adsh"),
                    "cik": cik,
                    "biz_locations": biz_locs,
                    "biz_states": biz_states,
                    "source_method": "efts",
                })

            logger.info(f"EFTS '{query}': got {len(hits)} (offset {offset}, total {total_count})")

            if len(hits) < page_size or offset + page_size >= total_count:
                break
            offset += page_size
            time.sleep(0.5)

        except Exception as e:
            logger.warning(f"EFTS search failed: {e}")
            break

    return results


# ═══════════════════════════════════════════════════════════════
#  METHOD B: EDGAR EFTS Location-Based Search
# ═══════════════════════════════════════════════════════════════

def search_efts_by_location(location_query: str, days_back: int = 180,
                            max_results: int = 200) -> List[Dict]:
    """
    Search EDGAR EFTS for Form D filings matching a location string.
    This replaces the old Atom feed approach which returned a company directory
    (all entities in a state, alphabetically) rather than recent filings.

    Uses the same search-index endpoint as search_efts but with location-
    specific queries like '"Brooklyn, NY"' or '"Boston, MA"'.
    """
    results = []
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    offset = 0
    page_size = 100

    while offset < max_results:
        url = "https://efts.sec.gov/LATEST/search-index"
        params = {
            "q": location_query,
            "forms": "D,D/A",
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
            "from": offset,
            "size": min(page_size, max_results - offset),
        }

        try:
            resp = fetch(url, headers=SEC_HEADERS, params=params, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"EFTS location search HTTP {resp.status_code}")
                break

            ct = resp.headers.get("content-type", "")
            if "json" not in ct:
                logger.warning(f"EFTS location search non-JSON: {ct}")
                break

            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
            total = data.get("hits", {}).get("total", {})
            total_count = total.get("value", 0) if isinstance(total, dict) else total

            for hit in hits:
                src = hit.get("_source", {})
                display_names = src.get("display_names", [])
                name = display_names[0] if display_names else "Unknown"
                ciks = src.get("ciks", [])
                cik = ciks[0].lstrip("0") if ciks else None
                biz_locs = src.get("biz_locations", [])
                biz_states = src.get("biz_states", [])
                results.append({
                    "company_name": name,
                    "filing_date": src.get("file_date"),
                    "accession_number": src.get("adsh"),
                    "cik": cik,
                    "biz_locations": biz_locs,
                    "biz_states": biz_states,
                    "source_method": f"efts_loc",
                })

            logger.info(
                f"EFTS location '{location_query}': got {len(hits)} "
                f"(offset {offset}, total {total_count})"
            )

            if len(hits) < page_size or offset + page_size >= total_count:
                break
            offset += page_size
            time.sleep(0.5)

        except Exception as e:
            logger.warning(f"EFTS location search failed: {e}")
            break

    return results


# ═══════════════════════════════════════════════════════════════
#  METHOD C: Fetch & Parse Form D XML
# ═══════════════════════════════════════════════════════════════

def fetch_form_d_details(cik: str = None, accession: str = None) -> Optional[Dict]:
    """
    Fetch and parse a Form D XML filing for structured data.
    """
    if not cik and not accession:
        return None

    try:
        # Step 1: Find the filing index page
        if accession:
            acc_clean = accession.replace("-", "")
            if not cik:
                # CIK is the first 10 digits of a clean accession number
                cik = acc_clean[:10].lstrip("0") or "0"
            index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/"
        elif cik:
            search_url = (
                f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
                f"&CIK={cik}&type=D&dateb=&owner=include&count=1&output=atom"
            )
            resp = fetch(search_url, headers=SEC_HEADERS, timeout=15)
            if resp.status_code != 200:
                return None
            link_match = re.search(r'href="([^"]+)"', resp.text)
            if not link_match:
                return None
            index_url = link_match.group(1)
        else:
            return None

        # Step 2: Get the filing index
        resp = fetch(index_url, headers=SEC_HEADERS, timeout=15)
        if resp.status_code != 200:
            return None

        # Step 3: Find the XML document
        soup = BeautifulSoup(resp.text, "html.parser")
        xml_link = None
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.endswith(".xml") and "FilingSummary" not in href:
                xml_link = f"https://www.sec.gov{href}" if href.startswith("/") else href
                break

        if not xml_link:
            return None

        # Step 4: Parse the XML
        xml_resp = fetch(xml_link, headers=SEC_HEADERS, timeout=15)
        if xml_resp.status_code != 200:
            return None

        root = ET.fromstring(xml_resp.text)

        # Single-pass tag collection (avoids O(n²) repeated tree traversals)
        tag_values = {}
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag not in tag_values and elem.text and elem.text.strip():
                tag_values[tag] = elem.text.strip()

        def find_any(*paths):
            for p in paths:
                if p in tag_values:
                    return tag_values[p]
            return None

        result = {
            "company_name": find_any("issuerName", "entityName"),
            "state": find_any("issuerStateOrCountry", "stateOrCountry"),
            "city": find_any("city", "issuerCity"),
            "zip": find_any("zipCode", "issuerZipCode"),
            "street": find_any("street1", "issuerStreet1"),
            "industry": find_any("industryGroupType", "IndustryGroupType"),
            "entity_type": find_any("entityType", "EntityType"),
            "amount_sold": None,
            "total_offering": None,
            "investors_count": find_any("totalNumberAlreadyInvested", "numberInvested"),
            "related_persons": [],
        }

        # Parse amounts (handle "Indefinite" and other non-numeric values)
        for field, keys in [
            ("amount_sold", ["totalAmountSold"]),
            ("total_offering", ["totalOfferingAmount"]),
        ]:
            val = find_any(*keys)
            if val:
                cleaned = val.replace(",", "").strip()
                if cleaned.lower() == "indefinite":
                    # Indefinite offering = open-ended fund raise, still a valid filing
                    result[field] = None
                    result[f"{field}_indefinite"] = True
                else:
                    try:
                        result[field] = float(cleaned)
                    except ValueError:
                        pass

        # Parse related persons / investors (name + title)
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag in ("relatedPersonInfo", "RelatedPersonInfo"):
                name_parts = []
                title = None
                for child in elem.iter():
                    child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    if child_tag in ("relatedPersonName", "RelatedPersonName"):
                        for name_child in child:
                            if name_child.text:
                                name_parts.append(name_child.text.strip())
                    if child_tag in ("relatedPersonTitle", "RelatedPersonTitle",
                                     "relationshipClarification", "RelationshipClarification"):
                        if child.text and child.text.strip():
                            title = child.text.strip()
                if name_parts:
                    result["related_persons"].append({
                        "name": " ".join(name_parts),
                        "title": title,
                    })

        # Fallback: if no relatedPersonInfo containers found, try flat relatedPersonName
        if not result["related_persons"]:
            for elem in root.iter():
                tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                if tag in ("relatedPersonName", "RelatedPersonName"):
                    name_parts = []
                    for child in elem:
                        if child.text:
                            name_parts.append(child.text.strip())
                    if name_parts:
                        result["related_persons"].append({
                            "name": " ".join(name_parts),
                            "title": None,
                        })

        # Check city from address
        address = f"{result.get('city', '')} {result.get('state', '')} {result.get('zip', '')} {result.get('street', '')}"
        detected_city = _detect_city_from_address(address)
        result["is_nyc"] = detected_city == "New York"
        result["detected_city"] = detected_city

        return result

    except Exception as e:
        logger.debug(f"Form D XML parse failed: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
#  MAIN SCRAPER
# ═══════════════════════════════════════════════════════════════

def run_sec_scraper(days_back: int = 180):
    """
    Main entry point. Combines 3 search methods, deduplicates,
    enriches with XML details, inserts deals.
    """
    conn = get_connection()
    log_id = log_scrape(conn, "sec_edgar")

    total_found = 0
    total_new = 0

    try:
        from config import get_enabled_cities
        all_filings = []
        seen_names = set()

        # ── City-specific EFTS location queries ──────────────────
        # Build location queries per city: "City, ST" format matches
        # the biz_locations field in EFTS (e.g. "New York, NY")
        _CITY_LOCATION_QUERIES = {
            "New York": ['"New York, NY"', '"Brooklyn, NY"', '"Manhattan"'],
            "Boston": ['"Boston, MA"', '"Cambridge, MA"'],
            "Washington DC": ['"Washington, DC"', '"Arlington, VA"', '"Bethesda, MD"'],
            "San Francisco": ['"San Francisco, CA"', '"Palo Alto, CA"', '"Menlo Park, CA"'],
        }

        seen_queries = set()

        # Method A: EFTS full-text search — per-city keyword queries
        for city_cfg in get_enabled_cities():
            city_name = city_cfg["display_name"]
            for query in city_cfg.get("sec_efts_queries", []):
                if query in seen_queries:
                    continue
                seen_queries.add(query)
                results = search_efts(query, days_back=days_back, max_results=200)
                for r in results:
                    norm = normalize_company_name(r["company_name"])
                    if norm not in seen_names:
                        seen_names.add(norm)
                        r["expected_city"] = city_name
                        all_filings.append(r)

        # Method B: EFTS location-based search — "City, ST" format
        # This replaces the broken Atom feed which returned a company
        # directory instead of recent filings.
        for city_cfg in get_enabled_cities():
            city_name = city_cfg["display_name"]
            loc_queries = _CITY_LOCATION_QUERIES.get(city_name, [])
            for loc_q in loc_queries:
                if loc_q in seen_queries:
                    continue
                seen_queries.add(loc_q)
                loc_results = search_efts_by_location(
                    loc_q, days_back=days_back, max_results=200,
                )
                for r in loc_results:
                    norm = normalize_company_name(r["company_name"])
                    if norm not in seen_names:
                        seen_names.add(norm)
                        # Confirm city from biz_locations if available
                        r["expected_city"] = city_name
                        all_filings.append(r)

        total_found = len(all_filings)
        logger.info(f"Total unique filings: {total_found}")

        # Enrich with XML details (batch with rate limiting)
        enriched = []
        skipped_early = 0
        for i, filing in enumerate(all_filings):
            # Early name filter — skip obvious junk before expensive XML fetch
            raw_name = _clean_sec_name(filing["company_name"])
            filing["company_name"] = raw_name
            if _is_junk_sec_company(raw_name):
                skipped_early += 1
                continue

            cik = filing.get("cik")
            accession = filing.get("accession_number")
            details = None

            if cik or accession:
                details = fetch_form_d_details(cik=cik, accession=accession)
                if i % 10 == 0:
                    time.sleep(1)

            # Verify city from XML address — the EFTS full-text search matches
            # "New York" anywhere in the filing, not just the company address.
            # We must confirm the company is actually located in an enabled city.
            if details and details.get("detected_city"):
                filing["expected_city"] = details["detected_city"]
            elif details and not details.get("detected_city"):
                # XML parsed but address didn't match any enabled city — skip
                logger.debug(
                    f"Skipping non-matching city: {filing['company_name']} "
                    f"(city={details.get('city')}, state={details.get('state')})"
                )
                continue
            elif not details:
                # No XML — check biz_locations from EFTS response as fallback
                biz_locs = filing.get("biz_locations", [])
                detected = None
                for loc in biz_locs:
                    detected = _detect_city_from_address(loc)
                    if detected:
                        break
                if detected:
                    filing["expected_city"] = detected
                else:
                    # Can't confirm city at all — skip
                    continue

            enriched.append((filing, details))

        logger.info(
            f"Enriched filings (NYC confirmed): {len(enriched)} "
            f"(skipped {skipped_early} junk names early)"
        )

        # Insert deals via unified quality gate
        skipped_filter = 0
        with batch_connection() as conn:
            for filing, details in enriched:
                company_name = _clean_sec_name(filing["company_name"])
                if details and details.get("company_name"):
                    company_name = _clean_sec_name(details["company_name"])

                # Pre-filter: SEC-specific junk that QC gate doesn't know about
                if not _should_keep_sec_filing(company_name, details, conn):
                    skipped_filter += 1
                    continue

                amount = None
                if details:
                    amount = details.get("amount_sold") or details.get("total_offering")

                industry = details.get("industry", "") if details else ""
                category_name = classify_sector(f"{company_name} {industry}")
                cat_id = get_category_id(conn, category_name) if category_name else None

                # Build direct filing URL from CIK + accession when available
                filing_url = filing.get("filing_url")
                if not filing_url and filing.get("cik") and filing.get("accession_number"):
                    acc_clean = filing["accession_number"].replace("-", "")
                    filing_url = (
                        f"https://www.sec.gov/Archives/edgar/data/"
                        f"{filing['cik']}/{acc_clean}/"
                    )
                if not filing_url:
                    filing_url = (
                        f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
                        f"&CIK={filing.get('cik', '')}&type=D&dateb=&owner=include&count=10"
                    )

                raw_text = json.dumps({
                    "cik": filing.get("cik"),
                    "accession": filing.get("accession_number"),
                    "state": details.get("state") if details else filing.get("state"),
                    "city": details.get("city") if details else None,
                    "industry": industry,
                    "investors_count": details.get("investors_count") if details else None,
                    "source_method": filing.get("source_method"),
                })

                # Build description from industry group
                description = f"{industry} company" if industry else None

                # ── Unified Quality Gate ──
                deal_city = filing.get("expected_city", "New York")
                accepted, reason, cleaned = validate_deal(
                    conn,
                    company_name=company_name,
                    stage=classify_stage_from_amount(amount),
                    amount=amount,
                    date_announced=filing.get("filing_date"),
                    source_type="sec_filing",
                    description=description,
                    is_nyc=(deal_city == "New York"),
                    raw_text=raw_text,
                    source_url=filing_url,
                    category_id=cat_id,
                    city=deal_city,
                )

                if not accepted:
                    logger.debug(f"QC rejected SEC '{company_name}': {reason}")
                    continue

                deal_id = insert_deal(conn, cleaned.pop("company_name"), **cleaned)

                if deal_id:
                    total_new += 1

                    # Link investors via shared utility (creates both investor AND firm links)
                    if details and details.get("related_persons"):
                        investor_dicts = []
                        for person in details["related_persons"][:15]:
                            if isinstance(person, dict):
                                person_name = person["name"]
                            else:
                                person_name = person
                            investor_dicts.append({"name": person_name, "role": "participant"})
                        if investor_dicts:
                            link_investors_to_deal(
                                conn, deal_id, investor_dicts,
                                upsert_investor_fn=upsert_investor,
                                link_deal_investor_fn=link_deal_investor,
                                upsert_firm_fn=upsert_firm,
                                link_deal_firm_fn=link_deal_firm,
                            )

            finish_scrape(conn, log_id, "success", total_found, total_new)

        logger.info(f"Filtered out {skipped_filter} filings at insertion stage")

        logger.info(f"SEC scraper: {total_new} new deals from {total_found} filings")

    except Exception as e:
        try:
            conn_err = get_connection()
            finish_scrape(conn_err, log_id, "error", total_found, total_new, str(e))
        except Exception:
            pass
        logger.error(f"SEC scraper error: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_sec_scraper()
