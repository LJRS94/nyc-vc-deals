"""
AlleyWatch Daily Funding Report Scraper
========================================
Scrapes AlleyWatch's daily/weekly/monthly NYC startup funding reports.
Also pulls from Crunchbase News RSS and Google News for NYC deal flow.

Sources:
  1. AlleyWatch daily funding reports (alleywatch.com/category/funding/)
  2. AlleyWatch monthly roundups (top 12 largest NYC rounds)
  3. Google News RSS for "NYC startup funding" / "New York venture capital"
  4. Crunchbase News RSS for NYC-tagged deals

This is the highest-signal scraper because AlleyWatch publishes every
single NYC funding round daily with structured data: company, amount,
round type, investors, and description.
"""

import re
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from bs4 import BeautifulSoup

from database import (
    get_connection, batch_connection, insert_deal, upsert_firm, upsert_investor,
    link_deal_firm, link_deal_investor, get_category_id,
    log_scrape, finish_scrape
)
from fetcher import fetch, fetch_many
from scrapers.utils import (
    normalize_stage, parse_amount, classify_sector,
    parse_investors, normalize_company_name, should_skip_deal,
    validate_deal_amount,
)
from scrapers.llm_extract import (
    extract_alleywatch_deals, validate_company_name, clean_company_name,
)
from quality_control import validate_deal

logger = logging.getLogger(__name__)


def _parse_pub_date(date_str: str) -> Optional[str]:
    """Parse RSS pubDate like 'Wed, 12 Feb 2025 08:00:00 GMT' to 'YYYY-MM-DD'."""
    if not date_str:
        return None
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


#  Source 1: AlleyWatch Daily Funding Reports

ALLEYWATCH_DAILY_BASE = "https://www.alleywatch.com/category/funding/"
ALLEYWATCH_DAILY_PATTERN = re.compile(
    r"/\d{4}/\d{2}/the-alleywatch-startup-daily-funding-report"
)

def get_alleywatch_daily_urls(days_back: int = 14) -> List[str]:
    """
    Discover AlleyWatch daily funding report URLs from the /category/funding/ page.
    """
    urls = []
    try:
        for page in range(1, 4):
            url = ALLEYWATCH_DAILY_BASE if page == 1 else f"{ALLEYWATCH_DAILY_BASE}page/{page}/"
            resp = fetch(url, timeout=15)
            if resp.status_code != 200:
                break
            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.find_all("a", href=True)
            for link in links:
                href = link["href"]
                if ALLEYWATCH_DAILY_PATTERN.search(href):
                    if href not in urls:
                        urls.append(href)
    except Exception as e:
        logger.warning(f"Failed to get AlleyWatch daily URLs: {e}")

    # Filter by date range
    cutoff = datetime.now() - timedelta(days=days_back)
    filtered = []
    for u in urls:
        # Extract date from URL: /2026/02/the-alleywatch-...
        m = re.search(r"/(\d{4})/(\d{2})/", u)
        if m:
            year, month = int(m.group(1)), int(m.group(2))
            # Rough check — keep if month is within range
            url_date = datetime(year, month, 1)
            if url_date >= cutoff.replace(day=1):
                filtered.append(u)
        else:
            filtered.append(u)

    logger.info(f"Found {len(filtered)} AlleyWatch daily reports (last {days_back} days)")
    return filtered[:20]  # Cap at 20 to be polite


def parse_alleywatch_daily(url: str) -> List[Dict]:
    """
    Parse a single AlleyWatch daily funding report page.
    AlleyWatch format per deal:
      'CompanyName, a [description], has raised $XM in [Round] funding
       led by [Lead]. Founded by [Founders] in [Year], [Company]
       has now raised a total of $XM in reported equity funding.'
    """
    deals = []
    try:
        resp = fetch(url, timeout=15)
        if resp.status_code != 200:
            return deals
        soup = BeautifulSoup(resp.text, "html.parser")

        report_date = None
        # Try the page title
        title = soup.find("title")
        if title:
            dm = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", title.get_text())
            if dm:
                report_date = f"{dm.group(3)}-{dm.group(1).zfill(2)}-{dm.group(2).zfill(2)}"
        # Fallback: extract from URL
        if not report_date:
            dm = re.search(r"report-(\d{1,2})-(\d{1,2})-(\d{4})", url)
            if dm:
                report_date = f"{dm.group(3)}-{dm.group(1).zfill(2)}-{dm.group(2).zfill(2)}"
            else:
                dm = re.search(r"/(\d{4})/(\d{2})/", url)
                if dm:
                    report_date = f"{dm.group(1)}-{dm.group(2)}-01"

        # Find the main content area — multiple selector fallbacks for site redesigns
        content = (
            soup.find("div", class_=re.compile(r"entry-content|post-content|article")) or
            soup.find("div", class_=re.compile(r"td-post-content|tdb-block-inner")) or
            soup.find("article") or
            soup.find("main") or
            soup
        )

        # Each deal is typically in its own <p> or text block
        # Pattern: "CompanyName, a/an [description], has raised $XM in [Round] funding"
        text = content.get_text(separator="\n")
        lines = text.split("\n")

        # ── Try LLM extraction first (much more reliable than regex) ──
        llm_deals = extract_alleywatch_deals(text)
        if llm_deals:
            for ld in llm_deals:
                company = ld.get("company_name", "").strip()
                if not company or not validate_company_name(company):
                    continue
                company = clean_company_name(company)

                amount = ld.get("amount")
                stage_raw = ld.get("stage", "Unknown")
                stage = normalize_stage(stage_raw) if stage_raw != "Unknown" else "Unknown"
                if amount and not validate_deal_amount(amount, stage):
                    amount = None

                lead_inv = ld.get("lead_investor")
                all_investors = ld.get("investors", [])
                if lead_inv and lead_inv not in all_investors:
                    all_investors.insert(0, lead_inv)

                sector = ld.get("sector") or classify_sector(ld.get("description", "") + " " + company)

                deals.append({
                    "company_name": company,
                    "description": (ld.get("description") or "")[:500] or None,
                    "amount": amount,
                    "round_type": stage_raw,
                    "stage": stage,
                    "lead_investor": lead_inv,
                    "all_investors": all_investors,
                    "founders": None,
                    "founded_year": None,
                    "total_raised": None,
                    "sector": sector,
                    "source_url": url,
                    "date_announced": report_date,
                })
            logger.info(f"[AlleyWatch] LLM extracted {len(deals)} deals from {url}")
            return deals

        # ── Regex fallback (original logic) ──

        # Regex for funding announcements
        funding_pattern = re.compile(
            r"([A-Z][A-Za-z0-9\s\.&\-']+?)"    # Company name
            r",\s+(?:a|an)\s+"                   # ", a/an "
            r"(.+?)"                              # Description
            r",\s+has\s+raised\s+"               # ", has raised "
            r"\$?([\d,.]+)\s*([MBK]|million|billion)?" # Amount
            r"\s+in\s+"                           # " in "
            r"(\w[\w\s\-]+?)\s+funding"          # Round type + " funding"
            , re.I
        )

        # Also match: "led by X" and "Founded by Y in Z"
        lead_pattern = re.compile(r"led\s+by\s+([^.]+)", re.I)
        investors_pattern = re.compile(r"from\s+investors\s+(?:that\s+)?include\s+([^.]+)", re.I)
        founded_pattern = re.compile(r"[Ff]ounded\s+by\s+(.+?)\s+in\s+(\d{4})", re.I)
        total_raised_pattern = re.compile(r"raised\s+a\s+total\s+of\s+\$?([\d,.]+)\s*([MBK])", re.I)

        # Join lines and split by deal boundaries (look for "has raised")
        full_text = " ".join(l.strip() for l in lines if l.strip())
        # Split on company boundaries — each deal starts with a capitalized name
        deal_blocks = re.split(r"(?=\b[A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)*,\s+(?:a|an)\s+)", full_text)

        for block in deal_blocks:
            if "has raised" not in block.lower():
                continue

            fm = funding_pattern.search(block)
            if not fm:
                # Try simpler pattern
                simple = re.search(
                    r"([A-Z][A-Za-z0-9\s\.&\-']+?)[\s,]+has\s+raised\s+\$?([\d,.]+)\s*([MBK]|million)?\s+in\s+(\w[\w\s\-]+?)\s+funding",
                    block, re.I
                )
                if simple:
                    company = simple.group(1).strip()
                    amt_str = simple.group(2) + (simple.group(3) or "M")
                    round_type = simple.group(4).strip()
                    description = ""
                else:
                    continue
            else:
                company = fm.group(1).strip()
                description = fm.group(2).strip()
                amt_str = fm.group(3) + (fm.group(4) or "M")
                round_type = fm.group(5).strip()

            # Skip if company name is too short or looks like noise
            if len(company) < 2 or company.lower() in ("the", "this", "our", "we"):
                continue

            amount = parse_amount(amt_str)

            # Extract lead investor
            lead_match = lead_pattern.search(block)
            lead_investor = lead_match.group(1).strip().rstrip(".") if lead_match else None

            # Extract all investors
            inv_match = investors_pattern.search(block)
            all_investors_str = inv_match.group(1) if inv_match else (lead_investor or "")
            all_investors, _ = parse_investors(all_investors_str)
            if lead_investor and lead_investor not in all_investors:
                all_investors.insert(0, lead_investor)

            # Extract founders and year
            founded_match = founded_pattern.search(block)
            founders = founded_match.group(1).strip() if founded_match else None
            founded_year = int(founded_match.group(2)) if founded_match else None

            # Total raised
            total_match = total_raised_pattern.search(block)
            total_raised = parse_amount(total_match.group(1) + total_match.group(2)) if total_match else None

            # Sector from description + industry tags
            sector = classify_sector(description + " " + block[:500])

            deals.append({
                "company_name": company,
                "description": description[:500] if description else None,
                "amount": amount,
                "round_type": round_type,
                "stage": normalize_stage(round_type),
                "lead_investor": lead_investor,
                "all_investors": all_investors,
                "founders": founders,
                "founded_year": founded_year,
                "total_raised": total_raised,
                "sector": sector,
                "source_url": url,
                "date_announced": report_date,
            })

        logger.info(f"[AlleyWatch] Parsed {len(deals)} deals from {url}")
    except Exception as e:
        logger.warning(f"[AlleyWatch] Failed to parse {url}: {e}")

    return deals


#  Source 2: AlleyWatch Monthly Roundup (Top N largest rounds)

MONTHLY_ROUNDUP_PATTERN = re.compile(
    r"/\d{4}/\d{2}/nyc-startup-funding-top-largest-\w+-\d{4}-vc"
)

def get_monthly_roundup_urls() -> List[str]:
    """Find AlleyWatch's 'Top X Largest NYC Funding Rounds' monthly posts."""
    urls = []
    try:
        resp = fetch(ALLEYWATCH_DAILY_BASE, timeout=15)
        if resp.status_code != 200:
            return urls
        soup = BeautifulSoup(resp.text, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if MONTHLY_ROUNDUP_PATTERN.search(href) and href not in urls:
                urls.append(href)
    except Exception as e:
        logger.warning(f"Failed to find monthly roundups: {e}")
    return urls[:3]  # Last 3 months max


def parse_monthly_roundup(url: str) -> List[Dict]:
    """
    Parse AlleyWatch monthly roundup. These have structured sections per deal:
    ## N. CompanyName $XM
    Round: Series X
    Description: ...
    Investors in the round: ...
    Industry: ...
    Founders: ...
    Founded: YYYY
    Total equity funding raised: $XM
    """
    deals = []
    try:
        resp = fetch(url, timeout=15)
        if resp.status_code != 200:
            return deals
        soup = BeautifulSoup(resp.text, "html.parser")
        content = soup.find("div", class_=re.compile(r"entry-content|post-content")) or soup

        # Extract date from URL
        dm = re.search(r"/(\d{4})/(\d{2})/", url)
        month_str = f"{dm.group(1)}-{dm.group(2)}" if dm else None

        # Find deal headers: "## N. CompanyName $XM" or <h2> tags
        headers = content.find_all(["h2", "h3"])
        for header in headers:
            header_text = header.get_text(strip=True)

            # Pattern: "12. CompanyName $20.0M" or "12. CompanyName"
            hm = re.match(r"(\d+)\.\s*(.+?)(?:\s+\$?([\d,.]+)\s*([MBK]))?\s*$", header_text)
            if not hm:
                continue

            company = hm.group(2).strip()
            amt_str = (hm.group(3) or "") + (hm.group(4) or "")
            amount = parse_amount(amt_str) if amt_str else None

            # Grab all text between this header and the next header
            block_parts = []
            sibling = header.find_next_sibling()
            while sibling and sibling.name not in ("h2", "h3", "hr"):
                block_parts.append(sibling.get_text(strip=True))
                sibling = sibling.find_next_sibling()
            block = " ".join(block_parts)

            # Extract structured fields
            round_match = re.search(r"Round:\s*(.+?)(?:\s+Description:|$)", block, re.I)
            round_type = round_match.group(1).strip() if round_match else None
            # Clean round: "Series B – $54.0M" → "Series B"
            if round_type:
                round_type = re.sub(r"\s*[–\-]\s*\$[\d,.]+[MBK]?", "", round_type).strip()

            desc_match = re.search(r"Description:\s*(.+?)(?:\s+(?:Investors|Founded|Industry|Founders|Founding|Total|AlleyWatch))", block, re.I)
            description = desc_match.group(1).strip() if desc_match else None

            inv_match = re.search(r"Investors?\s+in\s+the\s+round:\s*(.+?)(?:\s+(?:Industry|Founded|Founders|Founding|Total|AlleyWatch|Month))", block, re.I)
            investors_str = inv_match.group(1).strip() if inv_match else ""
            all_investors, lead = parse_investors(investors_str)

            industry_match = re.search(r"Industry:\s*(.+?)(?:\s+(?:Founders?|Founding|Total|AlleyWatch))", block, re.I)
            industry = industry_match.group(1).strip() if industry_match else ""

            founders_match = re.search(r"Founders?:\s*(.+?)(?:\s+(?:Founding|Total|AlleyWatch))", block, re.I)
            founders = founders_match.group(1).strip() if founders_match else None

            year_match = re.search(r"(?:Founding\s+year|Founded):\s*(\d{4})", block, re.I)
            founded_year = int(year_match.group(1)) if year_match else None

            total_match = re.search(r"Total\s+equity\s+funding\s+raised:\s*\$?([\d,.]+)\s*([MBK])", block, re.I)
            total_raised = parse_amount(total_match.group(1) + total_match.group(2)) if total_match else None

            sector = classify_sector((description or "") + " " + industry)

            # If amount still not found, look in the block
            if not amount:
                am = re.search(r"\$?([\d,.]+)\s*([MBK]|million)", block, re.I)
                if am:
                    amount = parse_amount(am.group(1) + am.group(2))

            deals.append({
                "company_name": company,
                "description": description[:500] if description else None,
                "amount": amount,
                "round_type": round_type or "Unknown",
                "stage": normalize_stage(round_type or ""),
                "lead_investor": lead,
                "all_investors": all_investors,
                "founders": founders,
                "founded_year": founded_year,
                "total_raised": total_raised,
                "sector": sector,
                "source_url": url,
                "date_announced": f"{month_str}-15" if month_str else None,
            })

        logger.info(f"[AlleyWatch Roundup] Parsed {len(deals)} deals from {url}")
    except Exception as e:
        logger.warning(f"[AlleyWatch Roundup] Failed to parse {url}: {e}")

    return deals


#  Source 3: Google News RSS for NYC funding announcements

GOOGLE_NEWS_QUERIES = [
    "NYC startup funding raised",
    "New York startup Series seed venture",
    "New York City startup raises million",
    # LinkedIn — where 60% of seed deals are exclusively announced
    'site:linkedin.com "raised" "seed" NYC startup',
    'site:linkedin.com "raised" "million" "New York" startup',
    'site:linkedin.com "pre-seed" OR "seed round" NYC',
    'site:linkedin.com "series a" "New York" raised',
    # Crunchbase News
    "site:news.crunchbase.com New York funding",
    "site:news.crunchbase.com NYC raised seed series",
    # Borough-specific and extra
    "NYC startup closes seed round",
    "New York startup pre-seed funding",
    "Brooklyn startup raised million",
    "Manhattan startup funding announcement",
]

def scrape_google_news_deals(days_back: int = 14) -> List[Dict]:
    """Scrape Google News RSS for NYC funding announcements."""
    deals = []
    cutoff = datetime.now() - timedelta(days=days_back)
    seen_companies = set()

    for query in GOOGLE_NEWS_QUERIES:
        rss_url = f"https://news.google.com/rss/search?q={query.replace(' ', '+')}&hl=en-US&gl=US&ceid=US:en"
        try:
            resp = fetch(rss_url, timeout=10)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "xml")
            items = soup.find_all("item")

            for item in items[:15]:
                title = item.find("title").get_text(strip=True) if item.find("title") else ""
                link = item.find("link").get_text(strip=True) if item.find("link") else ""
                pub_date = item.find("pubDate").get_text(strip=True) if item.find("pubDate") else ""

                title_lower = title.lower()
                # Must mention funding
                if not any(kw in title_lower for kw in ["raises", "raised", "funding", "secures", "closes", "million", "series", "seed"]):
                    continue
                # Should mention NYC
                nyc_signal = any(kw in title_lower for kw in ["nyc", "new york", "manhattan", "brooklyn"])

                # Extract company name (usually first entity in title before "raises/secures")
                cm = re.match(r"^([A-Z][A-Za-z0-9\s\.&\-']+?)\s+(?:raises?|secures?|closes?|lands?)", title, re.I)
                if not cm:
                    continue
                company = cm.group(1).strip()
                if company in seen_companies:
                    continue
                seen_companies.add(company)

                # Extract amount
                am = re.search(r"\$?([\d,.]+)\s*(M|million|B|billion)", title, re.I)
                amount = parse_amount(am.group(1) + am.group(2)) if am else None

                # Extract round
                rm = re.search(r"(Series\s+[A-H]|Seed|Pre-Seed|Series\s+\w+)", title, re.I)
                round_type = rm.group(1) if rm else "Venture"

                sector = classify_sector(title)

                deals.append({
                    "company_name": company,
                    "description": title,
                    "amount": amount,
                    "round_type": round_type,
                    "stage": normalize_stage(round_type),
                    "lead_investor": None,
                    "all_investors": [],
                    "founders": None,
                    "founded_year": None,
                    "total_raised": None,
                    "sector": sector,
                    "source_url": link,
                    "date_announced": _parse_pub_date(pub_date),
                    "is_nyc_confirmed": nyc_signal,
                })

        except Exception as e:
            logger.warning(f"[Google News] Query '{query}' failed: {e}")

    logger.info(f"[Google News] Found {len(deals)} potential NYC deals")
    return deals


#  Deal insertion

def insert_parsed_deal(conn, deal: Dict) -> Optional[int]:
    """Insert a parsed deal into the database via unified quality gate."""
    company = deal["company_name"]
    amount = deal.get("amount")

    # Build raw_text for reference
    raw_parts = [f"Company: {company}"]
    if deal.get("description"):
        raw_parts.append(f"Description: {deal['description']}")
    if deal.get("founders"):
        raw_parts.append(f"Founders: {deal['founders']}")
    if deal.get("founded_year"):
        raw_parts.append(f"Founded: {deal['founded_year']}")
    if deal.get("total_raised"):
        raw_parts.append(f"Total raised: ${deal['total_raised']:,.0f}")
    if deal.get("all_investors"):
        raw_parts.append(f"Investors: {', '.join(deal['all_investors'])}")
    raw_text = "\n".join(raw_parts)

    src = "google_news" if deal.get("source_url", "").startswith("https://news.google") else "alleywatch"
    sector = deal.get("sector")
    category_id = get_category_id(conn, sector) if sector else None

    # ── Unified Quality Gate ──
    accepted, reason, cleaned = validate_deal(
        conn,
        company_name=company,
        stage=deal.get("stage", "Unknown"),
        amount=amount,
        date_announced=deal.get("date_announced"),
        source_type=src,
        description=deal.get("description"),
        is_nyc=deal.get("is_nyc_confirmed", True),
        raw_text=raw_text,
        source_url=deal.get("source_url"),
        category_id=category_id,
    )

    if not accepted:
        logger.debug(f"  QC rejected '{company}': {reason}")
        return None

    deal_id = insert_deal(conn, cleaned.pop("company_name"), **cleaned)

    # Link investors
    lead_name = deal.get("lead_investor")
    for inv_name in deal.get("all_investors", []):
        firm_row = conn.execute(
            "SELECT id FROM firms WHERE LOWER(name) = LOWER(?)", (inv_name,)
        ).fetchone()
        firm_id = firm_row["id"] if firm_row else None

        inv_id = upsert_investor(conn, name=inv_name, firm_id=firm_id)
        link_deal_investor(conn, deal_id, inv_id)

        if firm_id:
            role = "lead" if inv_name == lead_name else "participant"
            link_deal_firm(conn, deal_id, firm_id, role=role)

        if inv_name == lead_name:
            conn.execute(
                "UPDATE deals SET lead_investor_id = ? WHERE id = ?",
                (inv_id, deal_id)
            )

    logger.info(f"  ✓ Inserted: {company} — {deal.get('stage')} — ${amount:,.0f}" if amount else f"  ✓ Inserted: {company} — {deal.get('stage')} — undisclosed")
    return deal_id


#  Main entry point

def run_alleywatch_scraper(days_back: int = 14):
    """
    Run the full AlleyWatch + Google News deal scraper.
    Pipeline:
      1. Scrape AlleyWatch daily funding reports
      2. Scrape AlleyWatch monthly roundups
      3. Scrape Google News RSS for supplemental deals
      4. Deduplicate and insert into database (single transaction)
    """
    conn = get_connection()
    log_id = log_scrape(conn, "alleywatch")

    all_deals = []
    new_count = 0

    try:
        # ── Phase 1: AlleyWatch Daily Reports ──
        logger.info("── Phase 1: AlleyWatch Daily Funding Reports ──")
        daily_urls = get_alleywatch_daily_urls(days_back=days_back)
        for url in daily_urls:
            deals = parse_alleywatch_daily(url)
            all_deals.extend(deals)

        # ── Phase 2: AlleyWatch Monthly Roundups ──
        logger.info("── Phase 2: AlleyWatch Monthly Roundups ──")
        roundup_urls = get_monthly_roundup_urls()
        for url in roundup_urls:
            deals = parse_monthly_roundup(url)
            all_deals.extend(deals)

        # ── Phase 3: Google News RSS (disabled — consistently returns 503) ──
        # Bing News in news_scraper.py covers the same ground more reliably.
        logger.info("── Phase 3: Google News RSS (skipped — 503s) ──")

        # ── Phase 4: Deduplicate & Insert (single batch commit) ──
        logger.info(f"── Phase 4: Inserting {len(all_deals)} parsed deals ──")
        seen = set()
        with batch_connection() as conn:
            for deal in all_deals:
                key = normalize_company_name(deal["company_name"])
                if key in seen:
                    continue
                seen.add(key)
                result = insert_parsed_deal(conn, deal)
                if result:
                    new_count += 1

            finish_scrape(conn, log_id, "success", len(all_deals), new_count)
        logger.info(
            f"AlleyWatch scraper complete: {len(all_deals)} parsed, "
            f"{new_count} new deals inserted"
        )

    except Exception as e:
        try:
            conn_err = get_connection()
            finish_scrape(conn_err, log_id, "error", error_message=str(e))
        except Exception:
            pass
        logger.error(f"AlleyWatch scraper error: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_alleywatch_scraper(days_back=45)


