"""
SEC EDGAR Form D Scraper
Form D filings are required when companies raise capital through private placements.
This is the most reliable source for actual funding data.
"""

import re
import json
import logging
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
from scrapers.utils import classify_stage_from_amount, normalize_company_name, should_skip_deal
from news_scraper import detect_category

logger = logging.getLogger(__name__)

# SEC EDGAR base URL
EDGAR_BASE = "https://efts.sec.gov/LATEST"
EDGAR_FILINGS = "https://www.sec.gov/cgi-bin/browse-edgar"
EDGAR_FULL_TEXT = "https://efts.sec.gov/LATEST/search-index"

# ── NY + DE State codes for filtering ─────────────────────────
# Most VC-backed startups incorporate in Delaware even if HQ'd in NYC
NY_STATE_CODES = ["NY"]
DE_STATE_CODE = "DE"
RELEVANT_STATE_CODES = ["NY", "DE"]


def search_form_d_filings(days_back: int = 180) -> List[Dict]:
    """
    Search EDGAR FULL-TEXT search API for recent Form D filings
    from New York-based companies.
    """
    results = []
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    try:
        # Method 1: EDGAR Full-Text Search
        search_url = f"https://efts.sec.gov/LATEST/search-index?q=%22form+D%22+%22New+York%22&forms=D,D/A&dateRange=custom&startdt={start_date}&enddt={end_date}"
        resp = fetch(search_url, headers=SEC_HEADERS, timeout=30)

        if resp.status_code == 200:
            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
            for hit in hits:
                source = hit.get("_source", {})
                results.append({
                    "company_name": source.get("display_names", ["Unknown"])[0] if source.get("display_names") else source.get("entity_name", "Unknown"),
                    "filing_date": source.get("file_date", ""),
                    "accession_number": source.get("accession_no", ""),
                    "filing_url": f"https://www.sec.gov/Archives/edgar/data/{source.get('entity_id', '')}/{source.get('accession_no', '').replace('-', '')}/",
                })
    except Exception as e:
        logger.warning(f"EDGAR full-text search failed: {e}")

    # Method 2: EDGAR XBRL API for structured Form D data
    try:
        xbrl_url = f"https://efts.sec.gov/LATEST/search-index?q=%22offering+amount%22&forms=D&dateRange=custom&startdt={start_date}&enddt={end_date}&from=0&size=50"
        resp = fetch(xbrl_url, headers=SEC_HEADERS, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            for hit in data.get("hits", {}).get("hits", []):
                source = hit.get("_source", {})
                name = source.get("entity_name", "")
                if name and name not in [r["company_name"] for r in results]:
                    results.append({
                        "company_name": name,
                        "filing_date": source.get("file_date", ""),
                        "accession_number": source.get("accession_no", ""),
                    })
    except Exception as e:
        logger.warning(f"EDGAR XBRL search failed: {e}")

    # Method 3: EDGAR company search for Form D
    try:
        company_url = "https://efts.sec.gov/LATEST/search-index"
        params = {
            "q": "New York venture capital",
            "forms": "D",
            "State": "NY",
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
        }
        resp = fetch(company_url, headers=SEC_HEADERS, params=params, timeout=30)
        if resp.status_code == 200 and resp.headers.get("content-type", "").startswith("application/json"):
            data = resp.json()
            for hit in data.get("hits", {}).get("hits", []):
                source = hit.get("_source", {})
                name = source.get("entity_name", "")
                if name and name not in [r["company_name"] for r in results]:
                    results.append({
                        "company_name": name,
                        "filing_date": source.get("file_date", ""),
                        "accession_number": source.get("accession_no", ""),
                    })
    except Exception as e:
        logger.warning(f"EDGAR company search failed: {e}")

    logger.info(f"Found {len(results)} Form D filings")
    return results


def fetch_form_d_xml(accession_number: str) -> Optional[Dict]:
    """
    Fetch and parse a Form D XML filing for structured data.
    """
    if not accession_number:
        return None

    # Normalize accession number
    acc_clean = accession_number.replace("-", "")

    try:
        # Try to get the primary document
        index_url = f"https://www.sec.gov/Archives/edgar/data/{acc_clean[:10]}/{acc_clean}/"
        resp = fetch(index_url, headers=SEC_HEADERS, timeout=15)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find the XML primary doc
        xml_link = None
        for link in soup.find_all("a", href=True):
            if link["href"].endswith(".xml"):
                xml_link = f"https://www.sec.gov{link['href']}"
                break

        if not xml_link:
            return None

        xml_resp = fetch(xml_link, headers=SEC_HEADERS, timeout=15)
        if xml_resp.status_code != 200:
            return None

        root = ET.fromstring(xml_resp.text)
        ns = {"": root.tag.split("}")[0].strip("{") if "}" in root.tag else ""}

        # Extract fields from Form D XML
        def find_text(path):
            el = root.find(f".//{path}", ns) if ns[""] else root.find(f".//{path}")
            if el is None:
                # Try without namespace
                for elem in root.iter():
                    if elem.tag.endswith(path.split("/")[-1]):
                        return elem.text
            return el.text if el is not None else None

        result = {
            "company_name": find_text("issuerName") or find_text("entityName"),
            "state": find_text("issuerStateOrCountry") or find_text("stateOrCountry"),
            "industry": find_text("industryGroupType"),
            "amount_sold": None,
            "total_offering": None,
            "investors_count": find_text("numberInvested"),
        }

        # Try to extract amounts
        amount_sold = find_text("totalAmountSold")
        total_offering = find_text("totalOfferingAmount")
        if amount_sold:
            try:
                result["amount_sold"] = float(amount_sold)
            except ValueError:
                pass
        if total_offering:
            try:
                result["total_offering"] = float(total_offering)
            except ValueError:
                pass

        # Extract related persons (investors)
        result["related_persons"] = []
        for person in root.iter():
            if "relatedPerson" in person.tag or "RelatedPerson" in person.tag:
                name_parts = []
                for child in person:
                    tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                    if "Name" in tag or "name" in tag:
                        if child.text:
                            name_parts.append(child.text)
                if name_parts:
                    result["related_persons"].append(" ".join(name_parts))

        return result

    except Exception as e:
        logger.warning(f"Failed to parse Form D XML {accession_number}: {e}")
        return None


def search_edgar_fulltext(query: str, days_back: int = 14) -> List[Dict]:
    """
    Use EDGAR full-text search (EFTS) API.
    """
    results = []
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    url = "https://efts.sec.gov/LATEST/search-index"
    params = {
        "q": query,
        "forms": "D,D/A",
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
        "from": 0,
        "size": 100,
    }

    try:
        resp = fetch(url, headers=SEC_HEADERS, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            for hit in data.get("hits", {}).get("hits", []):
                source = hit.get("_source", {})
                results.append({
                    "company_name": source.get("entity_name", "Unknown"),
                    "filing_date": source.get("file_date"),
                    "accession_number": source.get("accession_no"),
                    "cik": source.get("entity_id"),
                })
    except Exception as e:
        logger.warning(f"EFTS search failed for '{query}': {e}")

    return results


# classify_stage_from_amount imported from scrapers.utils


def run_sec_scraper(days_back: int = 180):
    """Main entry point for SEC EDGAR scraping."""
    conn = get_connection()
    log_id = log_scrape(conn, "sec_edgar")
    conn.close()

    total_found = 0
    total_new = 0

    try:
        # Search for Form D filings (HTTP phase)
        filings = search_form_d_filings(days_back=days_back)
        total_found = len(filings)

        # Pre-fetch Form D XML details (HTTP phase)
        enriched = []
        for filing in filings:
            company_name = filing.get("company_name", "")
            if not company_name or company_name == "Unknown":
                continue
            accession = filing.get("accession_number", "")
            details = fetch_form_d_xml(accession) if accession else None
            enriched.append((company_name, filing, details))

        # Batch insert (DB phase)
        with batch_connection() as conn:
            for company_name, filing, details in enriched:
                state = filing.get("state", "")

                if details:
                    state = details.get("state", state)
                    amount = details.get("amount_sold") or details.get("total_offering")
                    industry = details.get("industry", "")
                else:
                    amount = None
                    industry = ""

                # Filter for NY or DE (most NYC startups incorporate in Delaware)
                if state not in RELEVANT_STATE_CODES:
                    continue

                # Classify
                stage = classify_stage_from_amount(amount)
                category_name = detect_category(f"{company_name} {industry}")

                # Filter: only early-stage deals
                if stage not in ["Pre-Seed", "Seed", "Series A", "Series B", "Unknown"]:
                    continue
                if amount and amount > 50_000_000:
                    continue  # too large for early stage

                # Skip VC firms
                skip = should_skip_deal(conn, company_name, amount)
                if skip:
                    logger.debug(f"Skipping: {skip}")
                    continue

                # Check duplicates
                existing = conn.execute(
                    "SELECT id FROM deals WHERE company_name = ? AND source_type = 'sec_filing'",
                    (company_name,)
                ).fetchone()
                if existing:
                    continue

                cat_id = get_category_id(conn, category_name)
                filing_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={company_name}&type=D&dateb=&owner=include&count=10"

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
                )

                if deal_id:
                    total_new += 1

                    # Link investors from Form D
                    if details and details.get("related_persons"):
                        for person_name in details["related_persons"]:
                            inv_id = upsert_investor(conn, person_name)
                            link_deal_investor(conn, deal_id, inv_id)

            finish_scrape(conn, log_id, "success", total_found, total_new)

        logger.info(f"SEC scraper complete: {total_new} new deals from {total_found} filings")

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


