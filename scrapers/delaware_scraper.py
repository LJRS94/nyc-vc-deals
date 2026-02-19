"""
Delaware Division of Corporations Scraper
Most VC-backed startups incorporate in Delaware (DE) even if headquartered in NYC.
This scraper pulls new entity formations, amendments, and annual filings
from the Delaware ICIS system and cross-references with known NYC startups.

Sources:
  - Delaware Division of Corporations (ICIS) entity search
  - Delaware ECORP filing system
  - SEC EDGAR cross-reference (Form D filers registered in DE)
  - OpenCorporates API (fallback)
"""

import re
import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import urlencode, quote as url_quote

import requests
from bs4 import BeautifulSoup

from database import (
    get_connection, batch_connection, insert_deal, upsert_firm, upsert_investor,
    link_deal_firm, link_deal_investor, get_category_id,
    log_scrape, finish_scrape
)
from fetcher import fetch, SEC_HEADERS
from news_scraper import detect_category, is_nyc_related, extract_amount, detect_stage
from scrapers.utils import should_skip_deal

logger = logging.getLogger(__name__)

# Browser-like headers for Delaware ICIS (session-based scraping)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Delaware ICIS Entity Search ───────────────────────────────

DELAWARE_ECORP_BASE = "https://icis.corp.delaware.gov/ecorp/entitysearch"
DELAWARE_ENTITY_SEARCH = "https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx"


def search_delaware_entities(company_name: str) -> List[Dict]:
    """
    Search Delaware Division of Corporations ICIS system for an entity.
    Returns matching entities with file number, status, and incorporation date.
    """
    results = []
    try:
        # Step 1: Get the search page to capture viewstate tokens
        session = requests.Session()
        page = session.get(DELAWARE_ENTITY_SEARCH, headers=HEADERS, timeout=15)

        if page.status_code != 200:
            logger.warning(f"DE ICIS search page returned {page.status_code}")
            return results

        soup = BeautifulSoup(page.text, "html.parser")

        # Extract ASP.NET form tokens
        viewstate = soup.find("input", {"name": "__VIEWSTATE"})
        viewstate_gen = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
        event_validation = soup.find("input", {"name": "__EVENTVALIDATION"})

        if not viewstate:
            logger.warning("Could not find ASP.NET viewstate on DE search page")
            return results

        # Step 2: POST search request
        form_data = {
            "__VIEWSTATE": viewstate.get("value", ""),
            "__VIEWSTATEGENERATOR": viewstate_gen.get("value", "") if viewstate_gen else "",
            "__EVENTVALIDATION": event_validation.get("value", "") if event_validation else "",
            "ctl00$ContentPlaceHolder1$frmEntityName": company_name,
            "ctl00$ContentPlaceHolder1$frmFileNumber": "",
            "ctl00$ContentPlaceHolder1$btnSubmit": "Search",
        }

        search_resp = session.post(
            DELAWARE_ENTITY_SEARCH,
            data=form_data,
            headers={**HEADERS, "Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )

        if search_resp.status_code != 200:
            return results

        result_soup = BeautifulSoup(search_resp.text, "html.parser")

        # Step 3: Parse results table
        table = result_soup.find("table", {"id": lambda x: x and "SearchResults" in str(x)})
        if not table:
            # Try alternate table patterns
            table = result_soup.find("table", class_=re.compile(r"grid|results|data", re.I))

        if table:
            rows = table.find_all("tr")[1:]  # skip header
            for row in rows[:10]:  # limit results
                cells = row.find_all("td")
                if len(cells) >= 3:
                    entity_name = cells[0].get_text(strip=True)
                    file_number = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    inc_date = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                    status = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                    entity_type = cells[4].get_text(strip=True) if len(cells) > 4 else ""

                    results.append({
                        "entity_name": entity_name,
                        "file_number": file_number,
                        "incorporation_date": inc_date,
                        "status": status,
                        "entity_type": entity_type,
                        "state": "DE",
                        "source": "delaware_icis",
                    })

    except Exception as e:
        logger.warning(f"DE ICIS search failed for '{company_name}': {e}")

    return results


def get_delaware_entity_details(file_number: str) -> Optional[Dict]:
    """
    Get detailed filing information for a specific Delaware entity.
    """
    try:
        session = requests.Session()
        detail_url = f"https://icis.corp.delaware.gov/ecorp/entitysearch/EntityDetails.aspx?FileNumber={file_number}"
        resp = session.get(detail_url, headers=HEADERS, timeout=15)

        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        details = {
            "file_number": file_number,
            "registered_agent": None,
            "entity_type": None,
            "status": None,
            "formation_date": None,
            "state_country": "DE",
            "annual_reports": [],
        }

        # Parse detail fields
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)

                if "registered agent" in label:
                    details["registered_agent"] = value
                elif "entity type" in label or "entity kind" in label:
                    details["entity_type"] = value
                elif "status" in label or "state" in label:
                    details["status"] = value
                elif "formation" in label or "creation" in label:
                    details["formation_date"] = value

        return details

    except Exception as e:
        logger.warning(f"DE entity detail fetch failed for {file_number}: {e}")
        return None


# ── SEC EDGAR Cross-Reference: DE-incorporated + NY-operated ──

def search_sec_de_incorporated(days_back: int = 14) -> List[Dict]:
    """
    Search SEC EDGAR for Form D filings where the issuer is
    incorporated in Delaware but has a principal place of business in New York.
    This catches the classic VC startup pattern: DE corp, NYC HQ.
    """
    results = []
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    try:
        # EDGAR full-text search for Form D filings mentioning Delaware + New York
        search_url = (
            "https://efts.sec.gov/LATEST/search-index"
            f"?q=%22Delaware%22+%22New+York%22"
            f"&forms=D,D/A"
            f"&dateRange=custom&startdt={start_date}&enddt={end_date}"
            f"&from=0&size=50"
        )

        resp = fetch(search_url, headers=SEC_HEADERS, timeout=30)

        if resp.status_code == 200:
            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
            for hit in hits:
                source = hit.get("_source", {})
                results.append({
                    "company_name": source.get("entity_name", "Unknown"),
                    "filing_date": source.get("file_date"),
                    "accession_number": source.get("accession_no"),
                    "cik": source.get("entity_id"),
                    "source": "sec_edgar_de_ny",
                })
        else:
            logger.warning(f"EDGAR DE+NY search returned status {resp.status_code}")

    except Exception as e:
        logger.warning(f"EDGAR DE+NY search failed: {e}")

    # Also search specifically for Form D with "incorporated in Delaware"
    try:
        search_url2 = (
            "https://efts.sec.gov/LATEST/search-index"
            f"?q=%22incorporated+in+Delaware%22+%22New+York%22"
            f"&forms=D,D/A"
            f"&dateRange=custom&startdt={start_date}&enddt={end_date}"
            f"&from=0&size=50"
        )
        resp2 = fetch(search_url2, headers=SEC_HEADERS, timeout=30)

        if resp2.status_code == 200:
            data2 = resp2.json()
            existing_names = {r["company_name"] for r in results}
            for hit in data2.get("hits", {}).get("hits", []):
                source = hit.get("_source", {})
                name = source.get("entity_name", "Unknown")
                if name not in existing_names:
                    results.append({
                        "company_name": name,
                        "filing_date": source.get("file_date"),
                        "accession_number": source.get("accession_no"),
                        "cik": source.get("entity_id"),
                        "source": "sec_edgar_de_ny",
                    })

    except Exception as e:
        logger.warning(f"EDGAR DE incorporated search failed: {e}")

    logger.info(f"Found {len(results)} DE-incorporated + NY-based Form D filings")
    return results


# ── Parse Form D XML for DE incorporation details ─────────────

def parse_form_d_for_de_info(accession_number: str) -> Optional[Dict]:
    """
    Parse a Form D filing XML to extract:
    - State of incorporation (looking for DE)
    - State of principal business (looking for NY)
    - Offering amount
    - Industry group
    - Related persons (investors)
    """
    if not accession_number:
        return None

    acc_clean = accession_number.replace("-", "")

    try:
        from xml.etree import ElementTree as ET

        # Try to fetch the filing index
        # CIK is in the first 10 digits of accession for older filings,
        # but we may need to search
        index_url = f"https://www.sec.gov/Archives/edgar/data/{acc_clean}/"
        resp = fetch(index_url, headers=SEC_HEADERS, timeout=15)

        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find XML primary document
        xml_link = None
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.endswith(".xml") and "primary_doc" in href.lower():
                xml_link = f"https://www.sec.gov{href}"
                break
        if not xml_link:
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

        result = {
            "state_of_incorporation": None,
            "state_of_business": None,
            "offering_amount": None,
            "amount_sold": None,
            "industry": None,
            "entity_type": None,
            "investors": [],
        }

        # Walk all elements looking for key fields
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            text = (elem.text or "").strip()

            if not text:
                continue

            tag_lower = tag.lower()

            if "stateofincorporation" in tag_lower or "stateofinc" in tag_lower:
                result["state_of_incorporation"] = text
            elif "issuerstate" in tag_lower and "country" in tag_lower:
                result["state_of_business"] = text
            elif "stateorcountry" in tag_lower and not result["state_of_business"]:
                result["state_of_business"] = text
            elif "totalofferingamount" in tag_lower:
                try:
                    result["offering_amount"] = float(text)
                except ValueError:
                    pass
            elif "totalamountsold" in tag_lower:
                try:
                    result["amount_sold"] = float(text)
                except ValueError:
                    pass
            elif "industrygrouptype" in tag_lower:
                result["industry"] = text
            elif "entitytype" in tag_lower:
                result["entity_type"] = text

        # Extract related persons
        person_names = []
        current_person = {}
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            text = (elem.text or "").strip()
            tag_lower = tag.lower()

            if "relatedperson" in tag_lower and not text:
                if current_person.get("name"):
                    person_names.append(current_person)
                current_person = {}
            elif "firstname" in tag_lower or "first" in tag_lower:
                current_person["first"] = text
            elif "lastname" in tag_lower or "last" in tag_lower:
                current_person["last"] = text
                current_person["name"] = f"{current_person.get('first', '')} {text}".strip()
            elif "relatedpersonname" in tag_lower:
                current_person["name"] = text
            elif "relationshipclarification" in tag_lower:
                current_person["relationship"] = text

        if current_person.get("name"):
            person_names.append(current_person)

        result["investors"] = person_names
        return result

    except Exception as e:
        logger.warning(f"Form D DE parsing failed for {accession_number}: {e}")
        return None


# ── OpenCorporates Fallback ───────────────────────────────────

def search_opencorporates_de(company_name: str) -> Optional[Dict]:
    """
    Search OpenCorporates for Delaware-incorporated entities.
    Free tier: 50 requests/month, no API key needed for basic search.
    """
    try:
        url = "https://api.opencorporates.com/v0.4/companies/search"
        params = {
            "q": company_name,
            "jurisdiction_code": "us_de",
            "order": "score",
        }
        resp = fetch(url, params=params, timeout=15)

        if resp.status_code == 200:
            data = resp.json()
            companies = data.get("results", {}).get("companies", [])
            if companies:
                company = companies[0].get("company", {})
                return {
                    "name": company.get("name"),
                    "company_number": company.get("company_number"),
                    "incorporation_date": company.get("incorporation_date"),
                    "status": company.get("current_status"),
                    "registered_address": company.get("registered_address_in_full"),
                    "source": "opencorporates",
                    "opencorporates_url": company.get("opencorporates_url"),
                }

    except Exception as e:
        logger.debug(f"OpenCorporates search failed for '{company_name}': {e}")

    return None


# ── News Cross-Reference: DE Incorporation Announcements ──────

def scrape_de_incorporation_news() -> List[Dict]:
    """
    Search news for Delaware incorporation announcements that
    indicate new startup formations by NYC-based founders.
    """
    results = []
    queries = [
        "Delaware incorporated startup New York funding",
        "Delaware LLC startup NYC venture capital",
        "incorporated Delaware New York seed funding",
        "Delaware C-corp startup NYC series",
    ]

    for query in queries:
        encoded = url_quote(query)
        url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"

        try:
            resp = fetch(url, timeout=15)
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "xml")
            items = soup.find_all("item")[:10]

            for item in items:
                title = item.find("title").get_text(strip=True) if item.find("title") else ""
                link = item.find("link").get_text(strip=True) if item.find("link") else ""
                pub_date = item.find("pubDate").get_text(strip=True) if item.find("pubDate") else ""
                desc = item.find("description").get_text(strip=True) if item.find("description") else ""

                full_text = f"{title} {desc}"
                funding_keywords = [
                    "raises", "funding", "round", "seed", "series",
                    "investment", "backed", "secures", "closes",
                    "million", "venture", "pre-seed", "incorporated",
                ]
                if any(kw in full_text.lower() for kw in funding_keywords):
                    results.append({
                        "title": title,
                        "url": link,
                        "date": pub_date,
                        "description": desc,
                        "source_type": "news_article",
                        "de_related": True,
                    })

        except Exception as e:
            logger.debug(f"DE news search failed for '{query}': {e}")

    logger.info(f"Found {len(results)} DE-related news articles")
    return results


# ── Batch DE Verification ─────────────────────────────────────

def verify_de_incorporation(company_name: str) -> Dict:
    """
    Check if a company is incorporated in Delaware.
    Tries multiple sources: ICIS, OpenCorporates, name heuristics.
    Returns enrichment data.
    """
    result = {
        "is_de_incorporated": False,
        "file_number": None,
        "incorporation_date": None,
        "entity_type": None,
        "status": None,
        "source": None,
    }

    # Try 1: Delaware ICIS
    de_results = search_delaware_entities(company_name)
    if de_results:
        # Find best match
        for entity in de_results:
            name_lower = entity["entity_name"].lower()
            search_lower = company_name.lower()
            # Fuzzy match: entity name contains search term or vice versa
            if search_lower in name_lower or name_lower in search_lower:
                result.update({
                    "is_de_incorporated": True,
                    "file_number": entity.get("file_number"),
                    "incorporation_date": entity.get("incorporation_date"),
                    "entity_type": entity.get("entity_type"),
                    "status": entity.get("status"),
                    "source": "delaware_icis",
                    "de_entity_name": entity["entity_name"],
                })
                return result

    time.sleep(0.5)

    # Try 2: OpenCorporates
    oc_result = search_opencorporates_de(company_name)
    if oc_result:
        result.update({
            "is_de_incorporated": True,
            "incorporation_date": oc_result.get("incorporation_date"),
            "status": oc_result.get("status"),
            "source": "opencorporates",
            "de_entity_name": oc_result.get("name"),
        })
        return result

    # Common DE entity suffixes to try
    suffixes = [", Inc.", ", LLC", ", Corp.", " Inc", " LLC", " Corp"]
    for suffix in suffixes:
        variant = company_name + suffix
        de_results = search_delaware_entities(variant)
        if de_results:
            result.update({
                "is_de_incorporated": True,
                "file_number": de_results[0].get("file_number"),
                "incorporation_date": de_results[0].get("incorporation_date"),
                "entity_type": de_results[0].get("entity_type"),
                "status": de_results[0].get("status"),
                "source": "delaware_icis",
                "de_entity_name": de_results[0]["entity_name"],
            })
            return result
        time.sleep(0.3)

    return result


# ── Main Pipeline ─────────────────────────────────────────────

def process_de_filing(conn, company_name: str, filing_data: Dict) -> Optional[int]:
    """
    Process a single Delaware-related filing/entity and insert into database.
    """
    # Skip if already exists
    existing = conn.execute(
        "SELECT id FROM deals WHERE company_name = ? AND source_type = 'de_filing'",
        (company_name,)
    ).fetchone()
    if existing:
        return None

    amount = filing_data.get("offering_amount") or filing_data.get("amount_sold")
    industry = filing_data.get("industry", "")
    category_name = detect_category(f"{company_name} {industry}")

    # Determine stage from amount
    stage = "Unknown"
    if amount:
        if amount < 500_000:
            stage = "Pre-Seed"
        elif amount < 3_000_000:
            stage = "Seed"
        elif amount < 20_000_000:
            stage = "Series A"
        elif amount < 80_000_000:
            stage = "Series B"

    # Skip if it's a VC firm
    skip = should_skip_deal(conn, company_name, amount)
    if skip:
        return None

    cat_id = get_category_id(conn, category_name)

    source_url = filing_data.get("opencorporates_url") or \
                 f"https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx"

    deal_id = insert_deal(
        conn, company_name,
        stage=stage,
        amount_usd=amount,
        amount_disclosed=1 if amount else 0,
        date_announced=filing_data.get("filing_date") or filing_data.get("incorporation_date"),
        source_url=source_url,
        source_type="de_filing",
        category_id=cat_id,
        raw_text=json.dumps(filing_data)[:2000],
        confidence_score=0.75 if amount else 0.4,
    )

    # Link investors if available
    if deal_id and filing_data.get("investors"):
        for inv in filing_data["investors"]:
            inv_name = inv.get("name", "")
            if inv_name and len(inv_name) > 2:
                inv_id = upsert_investor(conn, inv_name)
                link_deal_investor(conn, deal_id, inv_id)

    return deal_id


def run_delaware_scraper(days_back: int = 14):
    """
    Main entry point for Delaware filings scraper.

    Strategy:
    1. Search SEC EDGAR for Form D filings with DE incorporation + NY business
    2. Parse Form D XML for amounts, investors, industry
    3. Cross-reference existing deals with DE incorporation status
    4. Scrape news for DE incorporation announcements
    """
    conn = get_connection()
    log_id = log_scrape(conn, "delaware_filings")

    total_found = 0
    total_new = 0

    try:
        # ═══ HTTP PHASE: Gather all data before touching DB ═══

        # ── Source 1: SEC EDGAR (DE incorporated + NY business) ──
        logger.info("── [DE-1] SEC EDGAR: DE incorporated + NY-based Form D filings ──")
        sec_filings = search_sec_de_incorporated(days_back=days_back)
        total_found += len(sec_filings)

        # Pre-fetch Form D XML details
        enriched_filings = []
        for filing in sec_filings:
            company_name = filing.get("company_name", "")
            if not company_name or company_name == "Unknown":
                continue
            accession = filing.get("accession_number")
            form_d_details = parse_form_d_for_de_info(accession) if accession else {}
            if form_d_details:
                state_inc = form_d_details.get("state_of_incorporation", "")
                state_biz = form_d_details.get("state_of_business", "")
                if state_inc == "DE" and state_biz == "NY":
                    filing_data = {
                        **filing,
                        "offering_amount": form_d_details.get("offering_amount"),
                        "amount_sold": form_d_details.get("amount_sold"),
                        "industry": form_d_details.get("industry"),
                        "investors": form_d_details.get("investors", []),
                        "state_of_incorporation": "DE",
                        "state_of_business": "NY",
                    }
                    enriched_filings.append((company_name, filing_data))

        # ── Source 2: Cross-reference existing deals for DE status ──
        logger.info("── [DE-2] Cross-referencing existing deals with DE incorporation ──")
        read_conn = get_connection()
        recent_deals = read_conn.execute("""
            SELECT id, company_name FROM deals
            ORDER BY created_at DESC
            LIMIT 20
        """).fetchall()

        de_updates = []
        for deal in recent_deals:
            company = deal["company_name"]
            de_info = verify_de_incorporation(company)
            if de_info["is_de_incorporated"]:
                de_updates.append((deal["id"], company, de_info))
                logger.info(f"  {company} -> DE incorporated ({de_info['source']})")
            else:
                logger.debug(f"  {company} -> not found in DE registry")

        # ── Source 3: DE incorporation news ──
        logger.info("── [DE-3] Scraping news for DE incorporation announcements ──")
        news_articles = scrape_de_incorporation_news()
        total_found += len(news_articles)

        # ═══ DB PHASE: Batch insert all results ═══
        from news_scraper import extract_company_name, extract_investors

        with batch_connection() as conn:
            # Insert Source 1 deals
            for company_name, filing_data in enriched_filings:
                deal_id = process_de_filing(conn, company_name, filing_data)
                if deal_id:
                    total_new += 1

            # Apply Source 2 DE verification updates
            for deal_id, company, de_info in de_updates:
                conn.execute("""
                    UPDATE deals
                    SET raw_text = raw_text || ? ,
                        updated_at = ?
                    WHERE id = ?
                """, (
                    f" [DE incorporated: {de_info.get('de_entity_name', '')},"
                    f" file#{de_info.get('file_number', 'N/A')}]",
                    datetime.utcnow().isoformat(),
                    deal_id,
                ))

            # Insert Source 3 news deals
            for article in news_articles:
                title = article.get("title", "")
                url = article.get("url", "")
                full_text = f"{title} {article.get('description', '')}"

                if not is_nyc_related(full_text):
                    continue

                company_name = extract_company_name(title)
                if not company_name:
                    continue

                amount = extract_amount(full_text)
                stage = detect_stage(full_text)
                category_name = detect_category(full_text)
                investors = extract_investors(full_text)

                # Skip VC firms and deals > $50M
                skip = should_skip_deal(conn, company_name, amount)
                if skip:
                    continue

                existing = conn.execute(
                    "SELECT id FROM deals WHERE company_name = ?",
                    (company_name,)
                ).fetchone()
                if existing:
                    continue

                cat_id = get_category_id(conn, category_name)
                deal_id = insert_deal(
                    conn, company_name,
                    stage=stage,
                    amount_usd=amount,
                    amount_disclosed=1 if amount else 0,
                    source_url=url,
                    source_type="de_filing",
                    category_id=cat_id,
                    raw_text=full_text[:2000],
                    confidence_score=0.6,
                )

                if deal_id:
                    total_new += 1
                    for inv in investors:
                        inv_name = inv["name"]
                        firm_row = conn.execute(
                            "SELECT id FROM firms WHERE LOWER(name) LIKE ?",
                            (f"%{inv_name.lower()}%",)
                        ).fetchone()
                        if firm_row:
                            link_deal_firm(conn, deal_id, firm_row["id"], inv["role"])
                        else:
                            firm_id = upsert_firm(conn, inv_name, location="Unknown")
                            link_deal_firm(conn, deal_id, firm_id, inv["role"])

            finish_scrape(conn, log_id, "success", total_found, total_new)

        logger.info(
            f"Delaware scraper complete: {total_new} new deals from "
            f"{total_found} sources examined"
        )

    except Exception as e:
        try:
            conn_err = get_connection()
            finish_scrape(conn_err, log_id, "error", total_found, total_new, str(e))
        except Exception:
            pass
        logger.error(f"Delaware scraper error: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_delaware_scraper()


