"""
NYC VC Firm Registry & Website Scraper
Maintains a list of known NYC-based VC firms and scrapes their portfolio/news pages.
"""

import json
import os
import re
import logging
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from database import (
    get_connection, batch_connection, upsert_firm, insert_deal, link_deal_firm,
    log_scrape, finish_scrape, get_category_id
)
from fetcher import fetch

logger = logging.getLogger(__name__)

# ── Known NYC VC Firms (seed list — expand as needed) ────────────
NYC_VC_FIRMS = [
    {
        "name": "Union Square Ventures",
        "website": "https://www.usv.com",
        "portfolio_url": "https://www.usv.com/portfolio",
        "focus_stages": '["Seed","Series A","Series B"]',
        "focus_sectors": '["Web3","Climate","AI","Health"]',
    },
    {
        "name": "Lerer Hippeau",
        "website": "https://www.lererhippeau.com",
        "portfolio_url": "https://www.lererhippeau.com/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["Consumer","D2C","Media","SaaS"]',
    },
    {
        "name": "FirstMark Capital",
        "website": "https://firstmarkcap.com",
        "portfolio_url": "https://firstmarkcap.com/portfolio",
        "focus_stages": '["Seed","Series A","Series B"]',
        "focus_sectors": '["Enterprise","SaaS","Fintech","AI"]',
    },
    {
        "name": "Insight Partners",
        "website": "https://www.insightpartners.com",
        "portfolio_url": "https://www.insightpartners.com/portfolio",
        "focus_stages": '["Series A","Series B"]',
        "focus_sectors": '["SaaS","Fintech","Cybersecurity","Data"]',
    },
    {
        "name": "Greycroft",
        "website": "https://www.greycroft.com",
        "portfolio_url": "https://www.greycroft.com/portfolio",
        "focus_stages": '["Seed","Series A","Series B"]',
        "focus_sectors": '["Consumer","Fintech","Health","Enterprise"]',
    },
    {
        "name": "BoxGroup",
        "website": "https://www.boxgroup.com",
        "portfolio_url": "https://www.boxgroup.com/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["Consumer","Fintech","Health","SaaS"]',
    },
    {
        "name": "Primary Venture Partners",
        "website": "https://www.primaryvc.com",
        "portfolio_url": "https://www.primaryvc.com/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["SaaS","Fintech","Health","Enterprise"]',
    },
    {
        "name": "Bowery Capital",
        "website": "https://www.bowerycap.com",
        "portfolio_url": "https://www.bowerycap.com/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["B2B SaaS","Enterprise"]',
    },
    {
        "name": "RRE Ventures",
        "website": "https://www.rre.com",
        "portfolio_url": "https://www.rre.com/portfolio",
        "focus_stages": '["Seed","Series A","Series B"]',
        "focus_sectors": '["Enterprise","Fintech","Health","AI"]',
    },
    {
        "name": "Flybridge Capital",
        "website": "https://www.flybridge.com",
        "portfolio_url": "https://www.flybridge.com/portfolio",
        "focus_stages": '["Pre-Seed","Seed","Series A"]',
        "focus_sectors": '["SaaS","AI","Health","Fintech"]',
    },
    {
        "name": "Thrive Capital",
        "website": "https://www.thrivecap.com",
        "portfolio_url": "https://www.thrivecap.com",
        "focus_stages": '["Seed","Series A","Series B"]',
        "focus_sectors": '["Consumer","SaaS","Fintech","Media"]',
    },
    {
        "name": "Notation Capital",
        "website": "https://www.notation.vc",
        "portfolio_url": "https://www.notation.vc/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["Developer Tools","AI","Fintech","SaaS"]',
    },
    {
        "name": "Work-Bench",
        "website": "https://www.work-bench.com",
        "portfolio_url": "https://www.work-bench.com/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["Enterprise","SaaS","Cybersecurity"]',
    },
    {
        "name": "Torch Capital",
        "website": "https://www.torch.vc",
        "portfolio_url": "https://www.torch.vc",
        "focus_stages": '["Pre-Seed","Seed","Series A"]',
        "focus_sectors": '["Consumer","Fintech","Health"]',
    },
    {
        "name": "Compound",
        "website": "https://www.compound.vc",
        "portfolio_url": "https://www.compound.vc",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["Fintech","SaaS","AI"]',
    },
    {
        "name": "Two Sigma Ventures",
        "website": "https://www.twosigmaventures.com",
        "portfolio_url": "https://www.twosigmaventures.com/portfolio",
        "focus_stages": '["Seed","Series A","Series B"]',
        "focus_sectors": '["AI","Data","Fintech","Health"]',
    },
    {
        "name": "Tiger Global Management",
        "website": "https://www.tigerglobal.com",
        "portfolio_url": "https://www.tigerglobal.com",
        "focus_stages": '["Series A","Series B"]',
        "focus_sectors": '["SaaS","Consumer","Fintech"]',
    },
    {
        "name": "Contour Venture Partners",
        "website": "https://www.contourventures.com",
        "portfolio_url": "https://www.contourventures.com/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["Fintech","Enterprise","Insurtech"]',
    },
    {
        "name": "ERA (Entrepreneurs Roundtable Accelerator)",
        "website": "https://www.era.co",
        "portfolio_url": "https://www.era.co/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["AI","SaaS","Health","Consumer"]',
    },
    {
        "name": "Tusk Venture Partners",
        "website": "https://www.tuskventurepartners.com",
        "portfolio_url": "https://www.tuskventurepartners.com/portfolio",
        "focus_stages": '["Seed","Series A","Series B"]',
        "focus_sectors": '["Regulated Industries","Fintech","Health","Transport"]',
    },
]


def seed_firms():
    """Insert all known NYC VC firms into the database, including firms.json."""
    firms_json = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "firms.json")
    extra = []
    if os.path.exists(firms_json):
        import json as _json
        with open(firms_json) as f:
            extra = _json.load(f)

    with batch_connection() as conn:
        for firm_data in NYC_VC_FIRMS:
            name = firm_data.pop("name")
            upsert_firm(conn, name, **firm_data)
            firm_data["name"] = name  # restore
        for firm_data in extra:
            name = firm_data.pop("name")
            kwargs = {k: v for k, v in firm_data.items() if v is not None}
            upsert_firm(conn, name, **kwargs)
            firm_data["name"] = name
    total = len(NYC_VC_FIRMS) + len(extra)
    logger.info(f"Seeded {total} NYC VC firms ({len(NYC_VC_FIRMS)} built-in + {len(extra)} from firms.json)")


def scrape_firm_portfolio(firm_name: str, portfolio_url: str) -> List[Dict]:
    """
    Generic portfolio page scraper.
    Looks for company names, descriptions, and links on a VC's portfolio page.
    Returns list of dicts with extracted portfolio company info.
    """
    results = []
    try:
        resp = fetch(portfolio_url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Strategy 1: Look for common portfolio card patterns
        # Many VC sites use cards/grid items with company info
        selectors = [
            "div.portfolio-company", "div.company-card", "article.portfolio",
            "div.portfolio-item", "li.portfolio", "div[class*='portfolio']",
            "div[class*='company']", "a[class*='portfolio']",
        ]

        cards = []
        for sel in selectors:
            cards = soup.select(sel)
            if cards:
                break

        if not cards:
            # Fallback: look for repeated link patterns in main content
            main = soup.find("main") or soup.find("div", {"role": "main"}) or soup
            links = main.find_all("a", href=True)
            seen = set()
            for link in links:
                text = link.get_text(strip=True)
                href = link.get("href", "")
                if (
                    text and len(text) > 2 and len(text) < 100
                    and text not in seen
                    and not href.startswith("#")
                    and "portfolio" not in text.lower()
                    and "home" not in text.lower()
                    and "about" not in text.lower()
                    and "team" not in text.lower()
                    and "contact" not in text.lower()
                ):
                    seen.add(text)
                    results.append({
                        "company_name": text,
                        "company_website": urljoin(portfolio_url, href) if href.startswith("/") else href,
                        "source_url": portfolio_url,
                    })
        else:
            for card in cards:
                name_el = (
                    card.find("h2") or card.find("h3") or
                    card.find("h4") or card.find("a") or card
                )
                name = name_el.get_text(strip=True) if name_el else None
                if not name:
                    continue
                desc_el = card.find("p")
                desc = desc_el.get_text(strip=True) if desc_el else None
                link_el = card.find("a", href=True)
                href = link_el["href"] if link_el else None
                if href and href.startswith("/"):
                    href = urljoin(portfolio_url, href)

                results.append({
                    "company_name": name,
                    "company_website": href,
                    "company_description": desc,
                    "source_url": portfolio_url,
                })

        logger.info(f"[{firm_name}] Found {len(results)} portfolio companies")
    except Exception as e:
        logger.warning(f"[{firm_name}] Portfolio scrape failed: {e}")

    return results


def scrape_firm_news(firm_name: str, website: str) -> List[Dict]:
    """
    Scrape a firm's news/blog page for recent announcements.
    """
    results = []
    news_paths = ["/news", "/blog", "/insights", "/updates", "/press"]

    for path in news_paths:
        url = website.rstrip("/") + path
        try:
            resp = fetch(url, timeout=10)
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            articles = soup.find_all("article") or soup.select("div[class*='post']")

            for article in articles[:10]:  # limit to 10 most recent
                title_el = article.find(["h1", "h2", "h3", "a"])
                title = title_el.get_text(strip=True) if title_el else ""
                link_el = article.find("a", href=True)
                link = link_el["href"] if link_el else ""
                if link and link.startswith("/"):
                    link = urljoin(website, link)
                date_el = article.find("time") or article.find(
                    ["span", "div"], class_=re.compile(r"date|time", re.I)
                )
                date_str = (
                    date_el.get("datetime") or date_el.get_text(strip=True)
                    if date_el else None
                )

                # Check if this looks like a funding announcement
                funding_keywords = [
                    "invest", "fund", "raise", "round", "seed", "series",
                    "pre-seed", "announce", "back", "lead", "capital",
                    "million", "closes", "secures"
                ]
                if title and any(kw in title.lower() for kw in funding_keywords):
                    results.append({
                        "title": title,
                        "url": link,
                        "date": date_str,
                        "source": f"{firm_name} website",
                    })

            if results:
                break  # found a working news page

        except Exception as e:
            continue

    logger.info(f"[{firm_name}] Found {len(results)} news items")
    return results


def run_firm_scraper():
    """Main entry point: scrape all firm websites."""
    conn = get_connection()
    log_id = log_scrape(conn, "firm_websites")
    total_found = 0
    total_new = 0

    try:
        firms = conn.execute("SELECT * FROM firms").fetchall()
        for firm in firms:
            firm_name = firm["name"]
            portfolio_url = firm["portfolio_url"]
            website = firm["website"]

            # Scrape portfolio page
            if portfolio_url:
                companies = scrape_firm_portfolio(firm_name, portfolio_url)
                total_found += len(companies)

            # Scrape news page
            if website:
                news = scrape_firm_news(firm_name, website)
                total_found += len(news)

        finish_scrape(conn, log_id, "success", total_found, total_new)
    except Exception as e:
        finish_scrape(conn, log_id, "error", error_message=str(e))
        logger.error(f"Firm scraper error: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    seed_firms()
    run_firm_scraper()


