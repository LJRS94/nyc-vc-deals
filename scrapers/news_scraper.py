"""
News & Press Release Scraper
Scrapes tech/VC news sources for NYC early-stage funding announcements.
Sources: TechCrunch, Crunchbase News, Business Insider, PR Newswire, etc.
"""

import os
import re
import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import quote as url_quote, unquote, parse_qs, urlparse

from bs4 import BeautifulSoup

from database import (
    get_connection, batch_connection, insert_deal, upsert_firm, upsert_investor,
    link_deal_firm, link_deal_investor, get_category_id,
    log_scrape, finish_scrape
)
from fetcher import fetch, NEWS_TTL
from scrapers.utils import (
    classify_sector as _classify_sector, normalize_stage as _normalize_stage,
    parse_amount as _parse_amount, normalize_company_name, company_names_match,
)

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


# ── Stage Detection (delegates to shared utils) ──────────────

def detect_stage(text: str) -> str:
    """Wrapper around shared normalize_stage for backward compat."""
    return _normalize_stage(text)



# ── Amount Extraction (delegates to shared utils) ─────────────

def extract_amount(text: str, title: str = "") -> Optional[float]:
    """Extract funding amount, preferring the title over full body text.
    Caps at $10B — amounts above that are almost certainly parsing artifacts."""
    MAX_DEAL = 10_000_000_000  # $10B
    # Try title first (much more reliable)
    if title:
        amt = _parse_amount(title)
        if amt and amt <= MAX_DEAL:
            return amt
    # Fall back to first 500 chars of body (the lede paragraph)
    amt = _parse_amount(text[:500])
    if amt and amt <= MAX_DEAL:
        return amt
    return None



# ── Category Detection (delegates to shared utils) ────────────

def detect_category(text: str) -> str:
    """Wrapper around shared classify_sector for backward compat."""
    return _classify_sector(text) or "Other"



# ── NYC Detection ─────────────────────────────────────────────

NYC_INDICATORS = [
    "new york", "nyc", "manhattan", "brooklyn", "queens",
    "bronx", "staten island", "ny-based", "new york-based",
    "headquartered in new york", "based in nyc", "flatiron",
    "soho", "tribeca", "midtown", "wall street", "chelsea",
    "greenpoint", "williamsburg", "dumbo", "fidi",
]


def is_nyc_related(text: str) -> bool:
    text_lower = text.lower()
    return any(indicator in text_lower for indicator in NYC_INDICATORS)


# ── News Source Scrapers ──────────────────────────────────────

FUNDING_KEYWORDS = [
    "raises", "funding", "round", "seed", "series",
    "investment", "backed", "secures", "closes",
    "million", "venture", "pre-seed",
]

# ── Publication RSS Feeds ─────────────────────────────────────

PUBLICATION_FEEDS = [
    ("TechCrunch Venture", "https://techcrunch.com/category/venture/feed/"),
    ("TechCrunch Funding", "https://techcrunch.com/tag/funding/feed/"),
    ("VentureBeat Business", "https://venturebeat.com/category/business/feed/"),
    ("Crunchbase News", "https://news.crunchbase.com/feed/"),
    ("Fortune Venture", "https://fortune.com/feed/fortune-feeds/venture/"),
]


def scrape_publication_rss() -> List[Dict]:
    """
    Scrape dedicated RSS/Atom feeds from major tech publications
    for funding-related articles.
    """
    results = []

    for pub_name, feed_url in PUBLICATION_FEEDS:
        try:
            resp = fetch(feed_url, timeout=15)
            if resp.status_code != 200:
                logger.warning(f"{pub_name} RSS returned {resp.status_code}")
                continue

            soup = BeautifulSoup(resp.text, "xml")
            items = soup.find_all("item")[:30]

            for item in items:
                title = item.find("title").get_text(strip=True) if item.find("title") else ""
                link = item.find("link").get_text(strip=True) if item.find("link") else ""
                pub_date = item.find("pubDate").get_text(strip=True) if item.find("pubDate") else ""
                desc_el = item.find("description")
                desc = desc_el.get_text(strip=True) if desc_el else ""

                full_text = f"{title} {desc}"
                if any(kw in full_text.lower() for kw in FUNDING_KEYWORDS):
                    results.append({
                        "title": title,
                        "url": link,
                        "date": pub_date,
                        "description": desc,
                        "source_type": "news_article",
                        "publication": pub_name,
                    })

            logger.info(f"{pub_name}: found {len(items)} items")

        except Exception as e:
            logger.warning(f"{pub_name} RSS scrape failed: {e}")

    return results


# ── Crunchbase Recent Rounds ──────────────────────────────────

def scrape_crunchbase_recent() -> List[Dict]:
    """
    Scrape Crunchbase's public recent funding rounds page.
    Filters for NYC-based companies.
    """
    results = []
    url = "https://www.crunchbase.com/discover/funding_rounds"

    try:
        resp = fetch(url, timeout=20)
        if resp.status_code != 200:
            logger.warning(f"Crunchbase discover returned {resp.status_code}")
            return results

        soup = BeautifulSoup(resp.text, "html.parser")

        # Look for funding round cards/rows in the listing
        rows = (
            soup.select("grid-row") or
            soup.select("tr[class*='ng']") or
            soup.select("div[class*='result']") or
            soup.select("mat-row") or
            soup.select("a[href*='/funding_round/']")
        )

        for row in rows:
            text = row.get_text(separator=" ", strip=True)
            if not is_nyc_related(text):
                continue

            # Try to extract a link
            link_el = row.find("a", href=True) if row.name != "a" else row
            href = ""
            if link_el:
                href = link_el.get("href", "")
                if href.startswith("/"):
                    href = "https://www.crunchbase.com" + href

            title = text[:200]
            if any(kw in text.lower() for kw in FUNDING_KEYWORDS):
                results.append({
                    "title": title,
                    "url": href,
                    "description": text[:500],
                    "source_type": "crunchbase",
                })

        logger.info(f"Crunchbase discover: found {len(results)} NYC funding rounds")

    except Exception as e:
        logger.warning(f"Crunchbase discover scrape failed: {e}")

    return results


# ── News Source Scrapers ──────────────────────────────────────

def scrape_google_news(query: str = "NYC startup funding round 2025",
                       max_results: int = 20) -> List[Dict]:
    """
    Scrape Google News RSS for funding news.
    """
    results = []
    encoded_query = url_quote(query)
    url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

    try:
        resp = fetch(url, timeout=15, ttl=NEWS_TTL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "xml")

        items = soup.find_all("item")[:max_results]
        for item in items:
            title = item.find("title").get_text(strip=True) if item.find("title") else ""
            link = item.find("link").get_text(strip=True) if item.find("link") else ""
            pub_date = item.find("pubDate").get_text(strip=True) if item.find("pubDate") else ""
            desc = item.find("description").get_text(strip=True) if item.find("description") else ""

            # Filter: must be funding-related
            full_text = f"{title} {desc}"
            if any(kw in full_text.lower() for kw in FUNDING_KEYWORDS):
                results.append({
                    "title": title,
                    "url": link,
                    "date": pub_date,
                    "description": desc,
                    "source_type": "news_article",
                })

    except Exception as e:
        logger.warning(f"Google News scrape failed: {e}")

    return results


# ── Google Custom Search API (official, rate-limit-free) ─────

GOOGLE_CSE_API_KEY = os.environ.get("GOOGLE_CSE_API_KEY", "")
GOOGLE_CSE_CX = os.environ.get("GOOGLE_CSE_CX", "")  # Custom Search Engine ID


def scrape_google_cse(query: str, max_results: int = 10) -> List[Dict]:
    """
    Use Google Custom Search JSON API (100 free queries/day).
    Set GOOGLE_CSE_API_KEY and GOOGLE_CSE_CX env vars to enable.
    """
    if not GOOGLE_CSE_API_KEY or not GOOGLE_CSE_CX:
        return []  # not configured

    results = []
    try:
        url = "https://www.googleapis.com/customsearch/v1"
        resp = fetch(url, params={
            "key": GOOGLE_CSE_API_KEY,
            "cx": GOOGLE_CSE_CX,
            "q": query,
            "num": min(max_results, 10),
            "sort": "date",
        }, timeout=10, ttl=NEWS_TTL)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("items", []):
            title = item.get("title", "")
            link = item.get("link", "")
            snippet = item.get("snippet", "")
            full_text = f"{title} {snippet}"
            if any(kw in full_text.lower() for kw in FUNDING_KEYWORDS):
                results.append({
                    "title": title,
                    "url": link,
                    "description": snippet,
                    "source_type": "news_article",
                })

    except Exception as e:
        logger.warning(f"Google CSE failed: {e}")

    return results


def _resolve_bing_url(url: str) -> str:
    """Extract the real article URL from a Bing News redirect URL."""
    if "bing.com/news/apiclick" in url:
        parsed = parse_qs(urlparse(url).query)
        if "url" in parsed:
            return unquote(parsed["url"][0])
    return url


def scrape_bing_news(query: str, max_results: int = 30) -> List[Dict]:
    """
    Scrape Bing News RSS for funding news. More resilient to rate limiting than Google.
    """
    results = []
    encoded_query = url_quote(query)
    url = f"https://www.bing.com/news/search?q={encoded_query}&format=rss&count={max_results}"

    try:
        resp = fetch(url, timeout=15, ttl=NEWS_TTL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "xml")

        items = soup.find_all("item")[:max_results]
        for item in items:
            title = item.find("title").get_text(strip=True) if item.find("title") else ""
            link = item.find("link").get_text(strip=True) if item.find("link") else ""
            # Resolve Bing redirect to actual article URL
            link = _resolve_bing_url(link)
            pub_date = item.find("pubDate").get_text(strip=True) if item.find("pubDate") else ""
            desc = item.find("description").get_text(strip=True) if item.find("description") else ""

            full_text = f"{title} {desc}"
            if any(kw in full_text.lower() for kw in FUNDING_KEYWORDS):
                results.append({
                    "title": title,
                    "url": link,
                    "date": pub_date,
                    "description": desc,
                    "source_type": "news_article",
                })

    except Exception as e:
        logger.warning(f"Bing News scrape failed: {e}")

    return results


def scrape_prnewswire(max_pages: int = 3) -> List[Dict]:
    """
    Scrape PR Newswire for NYC venture funding press releases.
    """
    results = []
    base = "https://www.prnewswire.com"

    for page in range(1, max_pages + 1):
        url = (
            f"{base}/news-releases/news-releases-list/"
            f"?page={page}&pagesize=25&keyword=funding+new+york+startup"
        )
        try:
            resp = fetch(url, timeout=15)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")

            cards = soup.select("a.newsreleaseconsolidatelink") or \
                    soup.select("div.card h3 a") or \
                    soup.select("a[class*='news']")

            for card in cards:
                title = card.get_text(strip=True)
                href = card.get("href", "")
                if href.startswith("/"):
                    href = base + href

                full_text = title
                if any(kw in full_text.lower() for kw in FUNDING_KEYWORDS):
                    results.append({
                        "title": title,
                        "url": href,
                        "source_type": "press_release",
                    })

        except Exception as e:
            logger.warning(f"PR Newswire page {page} failed: {e}")

    return results


def fetch_article_details(url: str) -> Dict:
    """
    Fetch full article text for deeper extraction.
    """
    try:
        resp = fetch(url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove nav, footer, sidebar
        for tag in soup.find_all(["nav", "footer", "aside", "header"]):
            tag.decompose()

        # Get main article text
        article = soup.find("article") or soup.find("main") or soup.find("body")
        text = article.get_text(separator=" ", strip=True) if article else ""

        # Extract company name from title
        title_el = soup.find("title") or soup.find("h1")
        title = title_el.get_text(strip=True) if title_el else ""

        return {
            "title": title,
            "text": text[:5000],  # limit to 5k chars
            "url": url,
        }
    except Exception as e:
        logger.warning(f"Failed to fetch article {url}: {e}")
        return {"title": "", "text": "", "url": url}


# ── Company Name Extraction ───────────────────────────────────

def extract_company_name(title: str) -> Optional[str]:
    """
    Extract the startup/company name from a funding headline.
    Common patterns:
      - "CompanyName Raises $XM in Series A"
      - "CompanyName Secures $XM Seed Round"
      - "CompanyName Closes $XM Series B Led by FirmName"
      - "NYC-Based CompanyName Announces $XM Round"
    """
    # Remove common prefixes
    cleaned = re.sub(
        r"^(nyc[\-\s]based|new york[\-\s]based|ny[\-\s]based)\s+",
        "", title, flags=re.I
    )

    patterns = [
        r"^(.+?)\s+(?:raises?|secures?|closes?|announces?|lands?|nabs?|gets?|receives?)\s",
        r"^(.+?)\s+(?:has\s+raised|just\s+raised|is\s+raising)\s",
        r"^(.+?),?\s+(?:a\s+.+?\s+startup|the\s+.+?\s+company)",
        # "$XM for CompanyName" / "Series A for CompanyName"
        r"\$[\d.]+\s*[MBK]?\s+(?:for|to)\s+(.+?)(?:\s+to\s|\s+in\s|\s+from\s|$)",
        r"(?:series\s+[a-c]|seed|pre-seed)\s+(?:for|round\s+for|funding\s+for)\s+(.+?)(?:\s+led|\s+from|$)",
        # "Startup CompanyName gets/lands..."
        r"^(?:startup|fintech|healthtech|ai company)\s+(.+?)\s+(?:raises?|secures?|closes?|gets?)\s",
        # "CompanyName, which does X, raises..."
        r"^(.+?),\s+which\s+.+?,\s+(?:raises?|secures?|closes?)\s",
        # "CompanyName snags/bags/pulls in $XM"
        r"^(.+?)\s+(?:snags|bags|pulls\s+in|hauls\s+in|nets)\s",
    ]

    for pattern in patterns:
        match = re.match(pattern, cleaned, re.I)
        if match:
            name = match.group(1).strip()
            # Clean up: remove quotes, leading/trailing punctuation
            name = name.strip("'\"''""\u201c\u201d ")
            # Don't return if it's too long (probably not just a name)
            if len(name) < 60:
                return name

    return None


# ── Investor Extraction ───────────────────────────────────────

def extract_investors(text: str) -> List[Dict]:
    """
    Extract investor names from article text.
    Looks for patterns like "led by X", "with participation from X, Y, and Z"
    """
    investors = []
    text_str = text

    # "led by" pattern
    lead_match = re.search(
        r"led\s+by\s+([A-Z][A-Za-z\s&\.\-]+?)(?:\s*(?:,|and|with|\.|$))",
        text_str
    )
    if lead_match:
        lead_name = lead_match.group(1).strip().rstrip(".")
        investors.append({"name": lead_name, "role": "lead"})

    # "with participation from" pattern
    part_match = re.search(
        r"(?:with\s+)?participation\s+(?:from|by)\s+(.+?)(?:\.|$)",
        text_str, re.I
    )
    if part_match:
        participant_text = part_match.group(1)
        # Split on commas and "and"
        parts = re.split(r",\s*|\s+and\s+", participant_text)
        for p in parts:
            name = p.strip().rstrip(".")
            if name and len(name) > 2 and len(name) < 80:
                investors.append({"name": name, "role": "participant"})

    # "backed by" pattern
    backed_match = re.search(
        r"backed\s+by\s+(.+?)(?:\.|$)", text_str, re.I
    )
    if backed_match and not investors:
        parts = re.split(r",\s*|\s+and\s+", backed_match.group(1))
        for p in parts:
            name = p.strip().rstrip(".")
            if name and len(name) > 2 and len(name) < 80:
                investors.append({"name": name, "role": "participant"})

    return investors


# ── Main Pipeline ─────────────────────────────────────────────

def process_deal(conn, title: str, url: str, full_text: str,
                 source_type: str = "news_article",
                 date_announced: str = None,
                 nyc_confirmed: bool = False) -> Optional[int]:
    """
    Process a single deal from scraped content.
    Extract structured data and insert into database.
    """
    combined_text = f"{title} {full_text}"

    # Check if NYC-related (skip if query already targeted NYC)
    if not nyc_confirmed and not is_nyc_related(combined_text):
        return None

    # Extract fields
    company_name = extract_company_name(title)
    if not company_name:
        return None

    stage = detect_stage(combined_text)
    amount = extract_amount(full_text, title=title)
    category_name = detect_category(combined_text)
    investors = extract_investors(combined_text)

    # Check for duplicates
    existing = conn.execute(
        "SELECT id FROM deals WHERE company_name = ? AND stage = ?",
        (company_name, stage)
    ).fetchone()
    if existing:
        return None

    # Get category ID
    cat_id = get_category_id(conn, category_name)

    # Insert deal
    deal_id = insert_deal(
        conn, company_name,
        stage=stage,
        amount_usd=amount,
        amount_disclosed=1 if amount else 0,
        date_announced=date_announced,
        source_url=url,
        source_type=source_type,
        category_id=cat_id,
        raw_text=combined_text[:2000],
        confidence_score=0.7 if amount and stage != "Unknown" else 0.4,
    )

    # Link investors
    for inv_data in investors:
        inv_name = inv_data["name"]
        # Check if this investor matches a known firm
        firm_row = conn.execute(
            "SELECT id FROM firms WHERE LOWER(name) LIKE ?",
            (f"%{inv_name.lower()}%",)
        ).fetchone()

        if firm_row:
            link_deal_firm(conn, deal_id, firm_row["id"], inv_data["role"])
        else:
            # Create as new firm if it looks like a firm name
            firm_id = upsert_firm(conn, inv_name, location="Unknown")
            link_deal_firm(conn, deal_id, firm_id, inv_data["role"])

    return deal_id


def _iter_months(months_back: int):
    """Yield (year, month, month_start, month_end) for the last N months."""
    now = datetime.now()
    for i in range(months_back):
        year = now.year
        month = now.month - i
        while month <= 0:
            month += 12
            year -= 1
        month_start = datetime(year, month, 1)
        if month == 12:
            month_end = datetime(year + 1, 1, 1)
        else:
            month_end = datetime(year, month + 1, 1)
        yield year, month, month_start, month_end


def _generate_google_queries(months_back: int) -> List[str]:
    """Generate month-by-month Google News queries using after:/before: operators."""
    templates = [
        'NYC startup funding raises after:{after} before:{before}',
        '"New York" startup "Series A" OR "seed" OR "Series B" after:{after} before:{before}',
        '"New York" startup raises million after:{after} before:{before}',
        'NYC fintech OR healthtech OR "AI startup" funding after:{after} before:{before}',
        'NYC startup secures OR closes round after:{after} before:{before}',
        'Manhattan OR Brooklyn startup raises funding after:{after} before:{before}',
        'site:techcrunch.com "raises" "New York" OR "NYC" after:{after} before:{before}',
        'site:forbes.com "New York" startup raises after:{after} before:{before}',
    ]

    queries = []
    for year, month, month_start, month_end in _iter_months(months_back):
        after = month_start.strftime("%Y-%m-%d")
        before = month_end.strftime("%Y-%m-%d")
        for template in templates:
            queries.append(template.format(after=after, before=before))
    return queries


def _generate_diverse_queries() -> List[str]:
    """Generate diverse topic/sector queries to maximize unique article coverage."""
    # Core funding verbs × location combinations
    locations = ["NYC", '"New York"', "Manhattan", "Brooklyn"]
    verbs = ["raises", "secures", "closes", "funding"]
    stages = ["seed", "Series A", "Series B", "pre-seed", "venture"]
    sectors = [
        "fintech", "AI", "healthtech", "SaaS", "cybersecurity",
        "climate tech", "proptech", "edtech", "insurtech",
        "biotech", "consumer", "enterprise", "developer tools",
        "logistics", "food tech", "legal tech", "HR tech",
        "media", "crypto", "web3", "robotics",
    ]

    queries = []
    # Location × verb combinations
    for loc in locations:
        for verb in verbs:
            queries.append(f"{loc} startup {verb}")
    # Location × stage combinations
    for loc in locations:
        for stage in stages:
            queries.append(f"{loc} startup {stage} funding")
    # Location × sector combinations (biggest variety driver)
    for loc in locations[:2]:  # NYC and "New York" only to limit total
        for sector in sectors:
            queries.append(f"{loc} {sector} startup funding")
    # Amount-based queries
    for loc in locations[:2]:
        queries.append(f"{loc} startup raises million 2025")
        queries.append(f"{loc} startup raises million 2026")
        queries.append(f"{loc} startup raises $")
    # Publication-scoped queries
    for site in ["techcrunch.com", "forbes.com", "venturebeat.com",
                  "crunchbase.com", "businessinsider.com"]:
        queries.append(f"site:{site} NYC startup raises")
        queries.append(f'site:{site} "New York" startup funding')

    return queries


def run_news_scraper(days_back: int = 14):
    """Main entry point for news scraping."""
    conn = get_connection()
    log_id = log_scrape(conn, "news_press")
    conn.close()

    total_found = 0
    total_new = 0

    try:
        months_back = max(1, days_back // 30)
        all_articles = []

        # ── Bing News RSS — diverse topic queries (primary, no rate limiting) ──
        bing_queries = _generate_diverse_queries()
        logger.info(f"Bing News: {len(bing_queries)} diverse queries")
        for i, query in enumerate(bing_queries):
            articles = scrape_bing_news(query, max_results=30)
            for a in articles:
                a["nyc_confirmed"] = True
            all_articles.extend(articles)
            time.sleep(0.5)
        logger.info(f"Bing News: collected {len(all_articles)} articles")

        # ── Google News RSS — month by month (supplement, with rate protection) ──
        google_queries = _generate_google_queries(months_back)
        logger.info(f"Google News: {len(google_queries)} monthly queries")
        consecutive_fails = 0
        google_count = 0
        for i, query in enumerate(google_queries):
            if consecutive_fails >= 3:
                logger.warning(f"Google News rate-limited, stopping after {i}/{len(google_queries)} queries")
                break
            articles = scrape_google_news(query, max_results=20)
            if not articles:
                consecutive_fails += 1
            else:
                consecutive_fails = 0
                for a in articles:
                    a["nyc_confirmed"] = True
                all_articles.extend(articles)
                google_count += len(articles)
            delay = 2.0 + (consecutive_fails * 3.0)
            time.sleep(delay)
        logger.info(f"Google News: collected {google_count} articles")

        # ── Google Custom Search API (if configured) ──
        if GOOGLE_CSE_API_KEY:
            cse_queries = _generate_diverse_queries()[:20]  # limit to 20 queries (API quota)
            logger.info(f"Google CSE: running {len(cse_queries)} queries")
            cse_count = 0
            for query in cse_queries:
                articles = scrape_google_cse(query, max_results=10)
                for a in articles:
                    a["nyc_confirmed"] = True
                all_articles.extend(articles)
                cse_count += len(articles)
                time.sleep(0.5)
            logger.info(f"Google CSE: collected {cse_count} articles")

        # PR Newswire
        pr_articles = scrape_prnewswire(max_pages=2)
        all_articles.extend(pr_articles)

        # Publication RSS feeds
        logger.info("Scraping publication RSS feeds...")
        pub_articles = scrape_publication_rss()
        all_articles.extend(pub_articles)

        # Crunchbase recent rounds
        logger.info("Scraping Crunchbase recent rounds...")
        cb_articles = scrape_crunchbase_recent()
        all_articles.extend(cb_articles)

        # Deduplicate by URL before processing
        seen_urls = set()
        unique_articles = []
        for article in all_articles:
            url = article.get("url", "")
            if url and url in seen_urls:
                continue
            if url:
                seen_urls.add(url)
            unique_articles.append(article)

        total_found = len(unique_articles)
        logger.info(f"Found {len(all_articles)} raw articles, {total_found} unique after dedup")

        # Pre-fetch all article details (HTTP only, no DB)
        enriched = []
        for article in unique_articles:
            url = article.get("url", "")
            title = article.get("title", "")
            date = _parse_pub_date(article.get("date", ""))
            nyc_ok = article.get("nyc_confirmed", False)
            details = fetch_article_details(url) if url else {}
            full_text = details.get("text", article.get("description", ""))
            enriched.append((title, url, full_text, article.get("source_type", "news_article"), date, nyc_ok))

        # Batch insert deals (DB only, no HTTP)
        with batch_connection() as conn:
            for title, url, full_text, source_type, date, nyc_ok in enriched:
                try:
                    deal_id = process_deal(conn, title, url, full_text,
                                           source_type=source_type, date_announced=date,
                                           nyc_confirmed=nyc_ok)
                    if deal_id:
                        total_new += 1
                except Exception as e:
                    logger.warning(f"Failed to process article '{title[:60]}': {e}")

            finish_scrape(conn, log_id, "success", total_found, total_new)

        logger.info(f"News scraper complete: {total_new} new deals from {total_found} articles")

    except Exception as e:
        try:
            conn_err = get_connection()
            finish_scrape(conn_err, log_id, "error", total_found, total_new, str(e))
            conn_err.close()
        except Exception:
            pass
        logger.error(f"News scraper error: {e}")


BATCH_STATE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".google_batch_state.json")


def run_google_batch(batch_size: int = 15, days_back: int = 450):
    """
    Run a small batch of Google News queries — designed for staggered cron usage.
    Tracks which queries have been completed so subsequent calls pick up where we left off.
    Usage: call every hour via cron to drip-feed Google queries without rate-limiting.
    """
    months_back = max(1, days_back // 30)
    all_queries = _generate_google_queries(months_back)

    # Load batch state
    state = {"completed": [], "last_run": None}
    if os.path.exists(BATCH_STATE_FILE):
        with open(BATCH_STATE_FILE) as f:
            state = json.load(f)

    completed_set = set(state.get("completed", []))
    pending = [q for q in all_queries if q not in completed_set]

    if not pending:
        logger.info("All Google queries completed. Resetting batch state for next cycle.")
        state = {"completed": [], "last_run": datetime.now().isoformat()}
        with open(BATCH_STATE_FILE, "w") as f:
            json.dump(state, f)
        return

    batch = pending[:batch_size]
    logger.info(f"Google batch: running {len(batch)}/{len(pending)} remaining queries "
                f"({len(completed_set)} already done)")

    conn = get_connection()
    log_id = log_scrape(conn, "google_batch")
    conn.close()

    all_articles = []
    consecutive_fails = 0

    for query in batch:
        if consecutive_fails >= 3:
            logger.warning(f"Google rate-limited, stopping batch after {consecutive_fails} consecutive fails")
            break
        articles = scrape_google_news(query, max_results=20)
        if not articles:
            consecutive_fails += 1
        else:
            consecutive_fails = 0
            for a in articles:
                a["nyc_confirmed"] = True
            all_articles.extend(articles)
            completed_set.add(query)
        time.sleep(3.0 + (consecutive_fails * 5.0))

    # Process articles
    total_new = 0
    seen_urls = set()
    unique = []
    for a in all_articles:
        url = a.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique.append(a)

    enriched = []
    for article in unique:
        url = article.get("url", "")
        title = article.get("title", "")
        date = _parse_pub_date(article.get("date", ""))
        details = fetch_article_details(url) if url else {}
        full_text = details.get("text", article.get("description", ""))
        enriched.append((title, url, full_text, "news_article", date, True))

    with batch_connection() as conn:
        for title, url, full_text, source_type, date, nyc_ok in enriched:
            try:
                deal_id = process_deal(conn, title, url, full_text,
                                       source_type=source_type, date_announced=date,
                                       nyc_confirmed=nyc_ok)
                if deal_id:
                    total_new += 1
            except Exception as e:
                logger.warning(f"Batch: failed to process '{title[:60]}': {e}")
        finish_scrape(conn, log_id, "success", len(unique), total_new)

    # Save state
    state["completed"] = list(completed_set)
    state["last_run"] = datetime.now().isoformat()
    with open(BATCH_STATE_FILE, "w") as f:
        json.dump(state, f)

    logger.info(f"Google batch complete: {total_new} new deals from {len(unique)} articles. "
                f"{len(all_queries) - len(completed_set)} queries remaining.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_news_scraper()


