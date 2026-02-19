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
    classify_stage_from_amount, normalize_company_name, classify_sector
)

logger = logging.getLogger(__name__)

# ── NYC zip codes and keywords for address filtering ──────────
NYC_ZIPS = set()
for prefix in ["100", "101", "102", "103", "104", "110", "111", "112", "113", "114", "116"]:
    for i in range(100):
        NYC_ZIPS.add(f"{prefix}{i:02d}"[:5])

NYC_KEYWORDS = [
    "new york", "manhattan", "brooklyn", "queens", "bronx",
    "staten island", "nyc", "ny 100", "ny 101", "ny 110", "ny 111",
]


def _is_nyc(text: str) -> bool:
    """Check if an address/text refers to NYC."""
    t = text.lower()
    return any(kw in t for kw in NYC_KEYWORDS)


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
                name = (
                    src.get("entity_name") or
                    (src.get("display_names", [None])[0] if src.get("display_names") else None) or
                    "Unknown"
                )
                results.append({
                    "company_name": name,
                    "filing_date": src.get("file_date"),
                    "accession_number": src.get("accession_no"),
                    "cik": src.get("entity_id"),
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
#  METHOD B: EDGAR Company Search Atom Feed
# ═══════════════════════════════════════════════════════════════

def search_atom_feed(state: str = "NY", days_back: int = 180, max_results: int = 200) -> List[Dict]:
    """
    Use EDGAR company search Atom feed to find Form D filings by state.
    Paginates through results (40 at a time).
    """
    results = []
    page_size = 40
    offset = 0

    while offset < max_results:
        url = "https://www.sec.gov/cgi-bin/browse-edgar"
        params = {
            "action": "getcompany",
            "State": state,
            "SIC": "",
            "type": "D",
            "dateb": "",
            "owner": "include",
            "count": page_size,
            "start": offset,
            "output": "atom",
        }

        try:
            resp = fetch(url, headers=SEC_HEADERS, params=params, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"Atom feed HTTP {resp.status_code} for state={state}")
                break

            root = ET.fromstring(resp.text)
            ns = {"atom": "http://www.w3.org/2005/Atom"}

            entries = root.findall(".//atom:entry", ns)
            if not entries:
                entries = root.findall(".//entry")

            for entry in entries:
                title = entry.findtext("atom:title", "", ns) or entry.findtext("title", "")
                updated = entry.findtext("atom:updated", "", ns) or entry.findtext("updated", "")
                link_el = entry.find("atom:link", ns) or entry.find("link")
                link = link_el.get("href", "") if link_el is not None else ""

                cik_match = re.search(r"CIK=(\d+)", link)
                cik = cik_match.group(1) if cik_match else None

                # Title format: "D - Company Name (CIK)" or "D/A - Company Name"
                name_match = re.match(r"^D(?:/A)?\s*-\s*(.+?)(?:\s*\(CIK|\s*$)", title)
                name = name_match.group(1).strip() if name_match else title.strip()

                # Filter by date
                if updated:
                    try:
                        file_date = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                        cutoff = datetime.now().astimezone() - timedelta(days=days_back)
                        if file_date < cutoff:
                            continue
                    except ValueError:
                        pass

                results.append({
                    "company_name": name,
                    "filing_date": updated[:10] if updated else None,
                    "accession_number": None,
                    "cik": cik,
                    "state": state,
                    "filing_url": link,
                    "source_method": f"atom_{state}",
                })

            logger.info(f"Atom {state}: got {len(entries)} entries (offset {offset})")

            if len(entries) < page_size:
                break
            offset += page_size
            time.sleep(1)  # SEC rate limit: 10 req/sec

        except Exception as e:
            logger.warning(f"Atom feed failed for state={state}: {e}")
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
            index_url = f"https://www.sec.gov/Archives/edgar/data/{cik or acc_clean[:10]}/{acc_clean}/"
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
            if href.endswith(".xml") and "primary" not in href.lower():
                xml_link = f"https://www.sec.gov{href}" if href.startswith("/") else href
                break

        if not xml_link:
            return None

        # Step 4: Parse the XML
        xml_resp = fetch(xml_link, headers=SEC_HEADERS, timeout=15)
        if xml_resp.status_code != 200:
            return None

        root = ET.fromstring(xml_resp.text)

        def find_any(*paths):
            """Find text by trying multiple tag names (namespace-agnostic)."""
            for elem in root.iter():
                tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                if tag in paths and elem.text:
                    return elem.text.strip()
            return None

        result = {
            "company_name": find_any("issuerName", "entityName"),
            "state": find_any("issuerStateOrCountry", "stateOrCountry"),
            "city": find_any("city", "issuerCity"),
            "zip": find_any("zipCode", "issuerZipCode"),
            "street": find_any("street1", "issuerStreet1"),
            "industry": find_any("industryGroupType", "IndustryGroupType"),
            "amount_sold": None,
            "total_offering": None,
            "investors_count": find_any("totalNumberAlreadyInvested", "numberInvested"),
            "related_persons": [],
        }

        # Parse amounts
        for field, keys in [
            ("amount_sold", ["totalAmountSold"]),
            ("total_offering", ["totalOfferingAmount"]),
        ]:
            val = find_any(*keys)
            if val:
                try:
                    result[field] = float(val.replace(",", ""))
                except ValueError:
                    pass

        # Parse related persons / investors
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag in ("relatedPersonName", "RelatedPersonName"):
                name_parts = []
                for child in elem:
                    if child.text:
                        name_parts.append(child.text.strip())
                if name_parts:
                    result["related_persons"].append(" ".join(name_parts))

        # Check if NYC
        address = f"{result.get('city', '')} {result.get('state', '')} {result.get('zip', '')} {result.get('street', '')}"
        result["is_nyc"] = _is_nyc(address)

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
    conn.close()

    total_found = 0
    total_new = 0

    try:
        all_filings = []
        seen_names = set()

        # Method A: EFTS full-text search for NYC mentions
        for query in ['"New York"', '"Manhattan"', '"Brooklyn"', "NYC startup"]:
            results = search_efts(query, days_back=days_back, max_results=100)
            for r in results:
                norm = normalize_company_name(r["company_name"])
                if norm not in seen_names:
                    seen_names.add(norm)
                    all_filings.append(r)

        # Method B: Atom feed for NY-registered entities
        ny_results = search_atom_feed(state="NY", days_back=days_back, max_results=200)
        for r in ny_results:
            norm = normalize_company_name(r["company_name"])
            if norm not in seen_names:
                seen_names.add(norm)
                all_filings.append(r)

        # Method C: Atom feed for DE-registered entities (most VC startups)
        de_results = search_atom_feed(state="DE", days_back=days_back, max_results=200)
        for r in de_results:
            norm = normalize_company_name(r["company_name"])
            if norm not in seen_names:
                seen_names.add(norm)
                r["needs_address_check"] = True
                all_filings.append(r)

        total_found = len(all_filings)
        logger.info(f"Total unique filings: {total_found}")

        # Enrich with XML details (batch with rate limiting)
        enriched = []
        for i, filing in enumerate(all_filings):
            cik = filing.get("cik")
            accession = filing.get("accession_number")
            details = None

            if cik or accession:
                details = fetch_form_d_details(cik=cik, accession=accession)
                if i % 10 == 0:
                    time.sleep(1)

            # For DE companies, skip if NOT in NYC
            if filing.get("needs_address_check") and details:
                if not details.get("is_nyc"):
                    continue
            elif filing.get("needs_address_check") and not details:
                continue

            enriched.append((filing, details))

        logger.info(f"Enriched filings (NYC confirmed): {len(enriched)}")

        # Insert deals
        with batch_connection() as conn:
            for filing, details in enriched:
                company_name = filing["company_name"]
                if details and details.get("company_name"):
                    company_name = details["company_name"]

                # Normalized dedup check
                norm = normalize_company_name(company_name)
                existing = conn.execute(
                    "SELECT id FROM deals WHERE company_name_normalized = ? AND source_type = 'sec_filing'",
                    (norm,)
                ).fetchone()
                if existing:
                    continue

                # Extract amount
                amount = None
                if details:
                    amount = details.get("amount_sold") or details.get("total_offering")

                # Classify
                stage = classify_stage_from_amount(amount)
                industry = details.get("industry", "") if details else ""
                category_name = classify_sector(f"{company_name} {industry}")
                cat_id = get_category_id(conn, category_name) if category_name else None

                # Skip very large rounds (not early stage)
                if amount and amount > 200_000_000:
                    continue

                filing_url = filing.get("filing_url") or (
                    f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
                    f"&company={company_name}&type=D&dateb=&owner=include&count=10"
                )

                deal_id = insert_deal(
                    conn, company_name,
                    stage=stage,
                    amount_usd=amount,
                    amount_disclosed=1 if amount else 0,
                    date_announced=filing.get("filing_date"),
                    source_url=filing_url,
                    source_type="sec_filing",
                    category_id=cat_id,
                    confidence_score=0.9 if amount else 0.5,
                    raw_text=json.dumps({
                        "cik": filing.get("cik"),
                        "accession": filing.get("accession_number"),
                        "state": details.get("state") if details else filing.get("state"),
                        "city": details.get("city") if details else None,
                        "industry": industry,
                        "investors_count": details.get("investors_count") if details else None,
                        "source_method": filing.get("source_method"),
                    }),
                )

                if deal_id:
                    total_new += 1

                    # Link investors
                    if details and details.get("related_persons"):
                        for person_name in details["related_persons"][:10]:
                            inv_id = upsert_investor(conn, person_name)
                            link_deal_investor(conn, deal_id, inv_id)

            finish_scrape(conn, log_id, "success", total_found, total_new)

        logger.info(f"SEC scraper: {total_new} new deals from {total_found} filings")

    except Exception as e:
        try:
            conn_err = get_connection()
            finish_scrape(conn_err, log_id, "error", total_found, total_new, str(e))
            conn_err.close()
        except Exception:
            pass
        logger.error(f"SEC scraper error: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_sec_scraper()
