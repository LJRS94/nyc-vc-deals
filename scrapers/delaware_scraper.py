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
from datetime import datetime, timedelta, timezone
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
from scrapers.utils import (
    should_skip_deal, is_nyc_related, detect_city, classify_sector,
    normalize_stage, parse_amount, link_investors_to_deal,
)
from scrapers.news_scraper import extract_company_name, extract_investors
from quality_control import validate_deal

logger = logging.getLogger(__name__)

# CIK suffix appended by EDGAR (e.g. "Acme Inc (CIK 0002012881)")
_CIK_SUFFIX_RE = re.compile(r"\s*\(CIK\s*\d+\)\s*$", re.I)

# Fund vehicles and non-startup entities to filter out of DE filings
_DE_ENTITY_BLOCKLIST = re.compile(
    r"\b(Fund|Feeder|Offshore|Holdings|Capital Partners|Capital,?\s*L\.?P|"
    r"Equity Fund|Investment Fund|Coinvestment|"
    r"Aggregator|Master Portfolio|"
    r"Asset Backed|BDC|Ventures?\s+[IVXLC]+\b|"
    r"DST\b|REIT|Trust\b|"
    r"Investors?\b|Partners,?\s*L\.?P|Deep Value|"
    r"Bioventures|Private Equity|Public Markets|Selector)\b",
    re.I,
)


def _clean_de_entity_name(name: str) -> str:
    """Strip CIK suffix, legal suffixes, and extra whitespace."""
    if not name:
        return name
    # Remove CIK suffix
    name = _CIK_SUFFIX_RE.sub("", name).strip()
    # Remove trailing legal suffixes (keep as company_name without Inc/LLC noise)
    name = re.sub(r",?\s*(Inc\.?|LLC|L\.?P\.?|Corp\.?|Ltd\.?)$", "", name, flags=re.I).strip()
    return name


def _is_de_junk_entity(name: str) -> bool:
    """Return True if the entity looks like a fund vehicle, not a startup."""
    if not name:
        return True
    return bool(_DE_ENTITY_BLOCKLIST.search(name))


def _detect_category(text: str) -> str:
    """Wrapper around classify_sector matching old news_scraper.detect_category."""
    return classify_sector(text) or "Other"


# Browser-like headers for Delaware ICIS (session-based scraping)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Delaware ICIS Entity Search ───────────────────────────────

from config import DELAWARE_ECORP_BASE, DELAWARE_ENTITY_SEARCH


_ICIS_BLOCKED = False  # module-level flag to skip after first CAPTCHA hit


def search_delaware_entities(company_name: str) -> List[Dict]:
    """
    Search Delaware Division of Corporations ICIS system for an entity.
    Returns matching entities with file number, status, and incorporation date.

    NOTE: As of 2025, the ICIS site is behind AWS WAF CAPTCHA and cannot be
    scraped without a headless browser.  This function detects the CAPTCHA
    and returns an empty list (with a one-time warning) rather than burning
    time on 405 responses.
    """
    global _ICIS_BLOCKED

    if _ICIS_BLOCKED:
        return []

    results = []
    try:
        session = requests.Session()
        page = session.get(DELAWARE_ENTITY_SEARCH, headers=HEADERS, timeout=15)

        if page.status_code != 200:
            logger.warning(f"DE ICIS search page returned {page.status_code}")
            return results

        # Detect AWS WAF CAPTCHA
        if "x-amzn-waf-action" in page.headers or "awswaf" in page.text.lower() or "captcha" in page.text.lower():
            logger.warning(
                "DE ICIS is behind AWS WAF CAPTCHA — skipping direct entity search. "
                "Relying on SEC EDGAR cross-reference instead."
            )
            _ICIS_BLOCKED = True
            return results

        soup = BeautifulSoup(page.text, "html.parser")

        viewstate = soup.find("input", {"name": "__VIEWSTATE"})
        viewstate_gen = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
        event_validation = soup.find("input", {"name": "__EVENTVALIDATION"})

        if not viewstate:
            logger.warning("Could not find ASP.NET viewstate on DE search page")
            return results

        form_data = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
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
            headers={
                **HEADERS,
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://icis.corp.delaware.gov",
                "Referer": DELAWARE_ENTITY_SEARCH,
            },
            timeout=15,
        )

        # Detect CAPTCHA on the POST response
        if search_resp.status_code == 405 or "awswaf" in search_resp.text.lower():
            logger.warning("DE ICIS POST triggered AWS WAF CAPTCHA — disabling direct search")
            _ICIS_BLOCKED = True
            return results

        if search_resp.status_code != 200:
            return results

        result_soup = BeautifulSoup(search_resp.text, "html.parser")

        table = result_soup.find("table", {"id": lambda x: x and "SearchResults" in str(x)})
        if not table:
            table = result_soup.find("table", class_=re.compile(r"grid|results|data", re.I))

        if table:
            rows = table.find_all("tr")[1:]
            for row in rows[:10]:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    results.append({
                        "entity_name": cells[0].get_text(strip=True),
                        "file_number": cells[1].get_text(strip=True) if len(cells) > 1 else "",
                        "incorporation_date": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                        "status": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                        "entity_type": cells[4].get_text(strip=True) if len(cells) > 4 else "",
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
    incorporated in Delaware but has a principal place of business
    in one of the enabled cities.

    Uses EFTS search-index with inc_states=DE and biz_locations matching
    each city.  The EFTS response schema uses:
      display_names[], ciks[], adsh, file_date, biz_locations[], inc_states[]
    """
    from config import get_enabled_cities

    results = []
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    existing_names = set()

    # City-specific location queries that match EFTS biz_locations values
    _CITY_LOC_QUERIES = {
        "New York": ['"New York, NY"', '"Brooklyn, NY"'],
        "Boston": ['"Boston, MA"', '"Cambridge, MA"'],
        "Washington DC": ['"Washington, DC"'],
        "San Francisco": ['"San Francisco, CA"', '"Palo Alto, CA"'],
    }

    for city_cfg in get_enabled_cities():
        city_name = city_cfg["display_name"]
        loc_queries = _CITY_LOC_QUERIES.get(city_name, [])

        for loc_q in loc_queries:
            # Search for DE-incorporated filings with business in this city
            # The full-text search matches both inc_states and biz_locations
            query = f'"Delaware" {loc_q}'
            try:
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
                resp = fetch(url, headers=SEC_HEADERS, params=params, timeout=30)
                if resp.status_code != 200:
                    continue

                ct = resp.headers.get("content-type", "")
                if "json" not in ct:
                    continue

                data = resp.json()
                for hit in data.get("hits", {}).get("hits", []):
                    src = hit.get("_source", {})
                    display_names = src.get("display_names", [])
                    raw_name = display_names[0] if display_names else "Unknown"
                    name = _clean_de_entity_name(raw_name)
                    if not name or name in existing_names or name == "Unknown":
                        continue
                    if _is_de_junk_entity(raw_name):
                        continue
                    existing_names.add(name)

                    ciks = src.get("ciks", [])
                    cik = ciks[0].lstrip("0") if ciks else None
                    inc_states = src.get("inc_states", [])

                    # Only keep if actually DE-incorporated
                    if inc_states and "DE" not in inc_states:
                        continue

                    results.append({
                        "company_name": name,
                        "filing_date": src.get("file_date"),
                        "accession_number": src.get("adsh"),
                        "cik": cik,
                        "biz_locations": src.get("biz_locations", []),
                        "inc_states": inc_states,
                        "source": f"sec_edgar_de_{city_cfg['state_code'].lower()}",
                        "expected_city": city_name,
                    })

                total_count = data.get("hits", {}).get("total", {})
                if isinstance(total_count, dict):
                    total_count = total_count.get("value", 0)
                logger.info(f"EFTS DE+{loc_q}: {len(data.get('hits',{}).get('hits',[]))} hits (total {total_count})")

            except Exception as e:
                logger.warning(f"EDGAR DE+{loc_q} search failed: {e}")

    logger.info(f"Found {len(results)} DE-incorporated Form D filings across all cities")
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
    # Accession numbers have format {CIK_padded_10}-{YY}-{seq};
    # the first 10 digits of the cleaned number are the zero-padded CIK.
    cik = acc_clean[:10].lstrip("0") or "0"

    try:
        from xml.etree import ElementTree as ET

        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/"
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

            if "jurisdictionofinc" in tag_lower or "stateofincorporation" in tag_lower or "stateofinc" in tag_lower:
                result["state_of_incorporation"] = text
            elif tag_lower == "stateorcountry" and not result["state_of_business"]:
                # First stateOrCountry is the issuer's business address state
                result["state_of_business"] = text
            elif "totalofferingamount" in tag_lower:
                cleaned = text.replace(",", "").strip()
                if cleaned.lower() != "indefinite":
                    try:
                        result["offering_amount"] = float(cleaned)
                    except ValueError:
                        pass
            elif "totalamountsold" in tag_lower:
                cleaned = text.replace(",", "").strip()
                if cleaned.lower() != "indefinite":
                    try:
                        result["amount_sold"] = float(cleaned)
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
    Requires API key (set OPENCORPORATES_API_KEY env var).
    Returns None if no key or no results.
    """
    from config import OPENCORPORATES_API_KEY
    if not OPENCORPORATES_API_KEY:
        return None

    try:
        url = "https://api.opencorporates.com/v0.4/companies/search"
        params = {
            "q": company_name,
            "jurisdiction_code": "us_de",
            "order": "score",
            "api_token": OPENCORPORATES_API_KEY,
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
        elif resp.status_code == 401:
            logger.debug("OpenCorporates API key invalid or missing")

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

def process_de_filing(conn, company_name: str, filing_data: Dict, city: str = None) -> Optional[int]:
    """
    Process a single Delaware-related filing/entity and insert into database.
    Routes through validate_deal() quality gate.
    """
    amount = filing_data.get("offering_amount") or filing_data.get("amount_sold")
    industry = filing_data.get("industry", "")
    category_name = _detect_category(f"{company_name} {industry}")
    cat_id = get_category_id(conn, category_name)

    source_url = filing_data.get("opencorporates_url") or \
                 "https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx"

    deal_city = city or filing_data.get("expected_city")

    accepted, reason, cleaned = validate_deal(
        conn, company_name,
        amount=amount,
        date_announced=filing_data.get("filing_date") or filing_data.get("incorporation_date"),
        source_type="de_filing",
        raw_text=json.dumps(filing_data)[:2000],
        source_url=source_url,
        category_id=cat_id,
        city=deal_city,
    )
    if not accepted:
        return None

    deal_kwargs = {k: v for k, v in cleaned.items() if k != "company_name"}
    deal_id = insert_deal(conn, cleaned["company_name"], **deal_kwargs)

    # Link investors via shared utility (creates both investor AND firm links)
    if deal_id and filing_data.get("investors"):
        investor_dicts = []
        for inv in filing_data["investors"]:
            inv_name = inv.get("name", "")
            if inv_name and len(inv_name) > 2:
                investor_dicts.append({"name": inv_name, "role": "participant"})
        if investor_dicts:
            link_investors_to_deal(
                conn, deal_id, investor_dicts,
                upsert_investor, link_deal_investor,
                upsert_firm, link_deal_firm,
            )

    return deal_id


def run_delaware_scraper(days_back: int = 90):
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
                # Check if the business state matches any enabled city
                expected_city = filing.get("expected_city")
                # Filter non-corporation entities (funds are LPs/LLCs, not C-corps)
                entity_type = form_d_details.get("entity_type", "")
                if entity_type and "corporation" not in entity_type.lower():
                    logger.debug(f"Skipping non-corp entity '{entity_type}': {company_name}")
                    continue

                is_de = (
                    state_inc == "DE"
                    or state_inc.upper() == "DELAWARE"
                    or "delaware" in state_inc.lower()
                )
                if is_de and expected_city:
                    from config import get_city_config
                    city_cfg = get_city_config(expected_city)
                    if city_cfg and state_biz in (city_cfg.get("state_code"), city_cfg.get("state_name")):
                        filing_data = {
                            **filing,
                            "offering_amount": form_d_details.get("offering_amount"),
                            "amount_sold": form_d_details.get("amount_sold"),
                            "industry": form_d_details.get("industry"),
                            "investors": form_d_details.get("investors", []),
                            "state_of_incorporation": "DE",
                            "state_of_business": state_biz,
                            "expected_city": expected_city,
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
        with batch_connection() as conn:
            # Insert Source 1 deals
            for company_name, filing_data in enriched_filings:
                deal_id = process_de_filing(conn, company_name, filing_data)
                if deal_id:
                    total_new += 1

            # Apply Source 2 DE verification updates
            for deal_id, company, de_info in de_updates:
                de_entity = de_info.get("de_entity_name", "")
                # Validate: confirm entity name roughly matches the deal company
                if de_entity and company:
                    from scrapers.utils import normalize_company_name as _norm
                    norm_deal = _norm(company)
                    norm_de = _norm(de_entity)
                    if norm_deal and norm_de and (norm_deal not in norm_de and norm_de not in norm_deal):
                        logger.warning(
                            f"DE cross-ref mismatch: deal '{company}' vs DE entity '{de_entity}', skipping"
                        )
                        continue

                annotation = (
                    f" [DE incorporated: {de_entity},"
                    f" file#{de_info.get('file_number', 'N/A')}]"
                )
                logger.info(f"DE cross-ref update for deal #{deal_id} ({company}): {annotation.strip()}")
                conn.execute("""
                    UPDATE deals
                    SET raw_text = COALESCE(raw_text, '') || ? ,
                        updated_at = ?
                    WHERE id = ?
                """, (
                    annotation,
                    datetime.now(timezone.utc).isoformat(),
                    deal_id,
                ))

            # Insert Source 3 news deals (via quality gate)
            for article in news_articles:
                title = article.get("title", "")
                url = article.get("url", "")
                full_text = f"{title} {article.get('description', '')}"

                news_city = detect_city(full_text)
                if not news_city and not is_nyc_related(full_text):
                    continue
                if not news_city:
                    news_city = "New York"

                company_name = extract_company_name(title)
                if not company_name:
                    continue

                amount = parse_amount(full_text[:500])
                category_name = _detect_category(full_text)
                investors = extract_investors(full_text)
                cat_id = get_category_id(conn, category_name)

                accepted, reason, cleaned = validate_deal(
                    conn, company_name,
                    amount=amount,
                    source_type="de_filing",
                    source_url=url,
                    category_id=cat_id,
                    raw_text=full_text[:2000],
                    city=news_city,
                )
                if not accepted:
                    continue

                deal_kwargs = {k: v for k, v in cleaned.items() if k != "company_name"}
                deal_id = insert_deal(conn, cleaned["company_name"], **deal_kwargs)

                if deal_id:
                    total_new += 1
                    if investors:
                        link_investors_to_deal(
                            conn, deal_id, investors,
                            upsert_investor, link_deal_investor,
                            upsert_firm, link_deal_firm,
                        )

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


