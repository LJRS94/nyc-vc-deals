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
    log_scrape, finish_scrape, get_category_id, upsert_portfolio_company
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
    {
        "name": "Betaworks",
        "website": "https://www.betaworks.com",
        "portfolio_url": "https://www.betaworks.com/companies",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["AI","Consumer","Media"]',
    },
    {
        "name": "Founder Collective",
        "website": "https://www.foundercollective.com",
        "portfolio_url": "https://www.foundercollective.com/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["SaaS","Consumer","Fintech"]',
    },
    {
        "name": "Collaborative Fund",
        "website": "https://www.collaborativefund.com",
        "portfolio_url": "https://www.collaborativefund.com/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["Climate","Food","Health","Consumer"]',
    },
    {
        "name": "IA Ventures",
        "website": "https://www.iaventures.com",
        "portfolio_url": "https://www.iaventures.com/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["AI","Data","Fintech"]',
    },
    {
        "name": "Eniac Ventures",
        "website": "https://www.eniac.vc",
        "portfolio_url": "https://www.eniac.vc/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["AI","SaaS","Consumer"]',
    },
    {
        "name": "Harlem Capital",
        "website": "https://www.harlemcapital.com",
        "portfolio_url": "https://www.harlemcapital.com/portfolio",
        "focus_stages": '["Pre-Seed","Seed","Series A"]',
        "focus_sectors": '["Consumer","Health","Fintech","SaaS"]',
    },
    {
        "name": "BBG Ventures",
        "website": "https://www.bbgventures.com",
        "portfolio_url": "https://www.bbgventures.com/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["Consumer","Health","Fintech"]',
    },
    {
        "name": "645 Ventures",
        "website": "https://www.645ventures.com",
        "portfolio_url": "https://www.645ventures.com/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["SaaS","Enterprise","AI"]',
    },
    {
        "name": "Ground Up Ventures",
        "website": "https://www.groundup.vc",
        "portfolio_url": "https://www.groundup.vc/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["SaaS","Fintech","AI"]',
    },
    {
        "name": "Crossbeam Venture Partners",
        "website": "https://www.crossbeamvp.com",
        "portfolio_url": "https://www.crossbeamvp.com/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["SaaS","Fintech","Enterprise"]',
    },
    {
        "name": "Bling Capital",
        "website": "https://www.blingcap.com",
        "portfolio_url": "https://www.blingcap.com/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["Consumer","SaaS","AI"]',
    },
    {
        "name": "K5 Global",
        "website": "https://www.k5global.com",
        "portfolio_url": "https://www.k5global.com",
        "focus_stages": '["Seed","Series A","Series B"]',
        "focus_sectors": '["AI","Consumer","Enterprise"]',
    },
    {
        "name": "Boldstart Ventures",
        "website": "https://www.boldstart.vc",
        "portfolio_url": "https://www.boldstart.vc/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["Developer Tools","SaaS","Cybersecurity"]',
    },
    {
        "name": "High Line Venture Partners",
        "website": "https://www.hlvp.com",
        "portfolio_url": "https://www.hlvp.com/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["Fintech","SaaS","AI"]',
    },
    {
        "name": "Newark Venture Partners",
        "website": "https://www.newarkventurepartners.com",
        "portfolio_url": "https://www.newarkventurepartners.com/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["SaaS","Fintech","Health"]',
    },
    {
        "name": "Brooklyn Bridge Ventures",
        "website": "https://www.brooklynbridge.vc",
        "portfolio_url": "https://www.brooklynbridge.vc/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["Consumer","SaaS","Health"]',
    },
    {
        "name": "Gutter Capital",
        "website": "https://www.guttercapital.com",
        "portfolio_url": "https://www.guttercapital.com",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["SaaS","Consumer","AI"]',
    },
    {
        "name": "Vast Ventures",
        "website": "https://www.vast.vc",
        "portfolio_url": "https://www.vast.vc/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["AI","SaaS","Fintech"]',
    },
    {
        "name": "Human Ventures",
        "website": "https://www.humanventures.co",
        "portfolio_url": "https://www.humanventures.co/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["Health","Consumer","Education"]',
    },
    {
        "name": "Interplay",
        "website": "https://www.interplay.vc",
        "portfolio_url": "https://www.interplay.vc/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["SaaS","Enterprise","Fintech"]',
    },
    {
        "name": "Company Ventures",
        "website": "https://www.companyventures.co",
        "portfolio_url": "https://www.companyventures.co/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["Consumer","SaaS","Marketplace"]',
    },
    {
        "name": "Corigin Ventures",
        "website": "https://www.corigin.com",
        "portfolio_url": "https://www.corigin.com/ventures",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["Real Estate","SaaS","Fintech"]',
    },
    {
        "name": "NextView Ventures",
        "website": "https://www.nextviewventures.com",
        "portfolio_url": "https://www.nextviewventures.com/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["Consumer","SaaS","Marketplace"]',
    },
    {
        "name": "m]x[v",
        "website": "https://www.mxv.vc",
        "portfolio_url": "https://www.mxv.vc/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["SaaS","Consumer","AI"]',
    },
    {
        "name": "Precursor Ventures",
        "website": "https://www.precursorvc.com",
        "portfolio_url": "https://www.precursorvc.com/portfolio",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["SaaS","Consumer","Fintech"]',
    },
    {
        "name": "Techstars NYC",
        "website": "https://www.techstars.com",
        "portfolio_url": "https://www.techstars.com/portfolio?city=new-york",
        "focus_stages": '["Pre-Seed","Seed"]',
        "focus_sectors": '["AI","SaaS","Consumer","Fintech"]',
    },
    {
        "name": "Kauffman Fellows Fund",
        "website": "https://www.kauffmanfellows.org",
        "portfolio_url": "https://www.kauffmanfellows.org",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["SaaS","Health","Fintech"]',
    },
    {
        "name": "XYZ Venture Capital",
        "website": "https://www.xyzvc.com",
        "portfolio_url": "https://www.xyzvc.com/portfolio",
        "focus_stages": '["Pre-Seed","Seed","Series A"]',
        "focus_sectors": '["SaaS","Consumer","Fintech","AI"]',
    },
    {
        "name": "Slow Ventures",
        "website": "https://www.slow.co",
        "portfolio_url": "https://www.slow.co/portfolio",
        "focus_stages": '["Seed","Series A"]',
        "focus_sectors": '["Consumer","SaaS","AI","Media"]',
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
            name = firm_data.get("name")
            if not name:
                continue
            kwargs = {}
            if firm_data.get("website"):
                kwargs["website"] = firm_data["website"]
            if firm_data.get("portfolio_url"):
                kwargs["portfolio_url"] = firm_data["portfolio_url"]
            # Map "focus" array to "focus_sectors" JSON string
            if firm_data.get("focus"):
                kwargs["focus_sectors"] = json.dumps(firm_data["focus"])
            upsert_firm(conn, name, **kwargs)
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

                # Extract lead partner if mentioned
                lead_partner = None
                partner_patterns = [
                    card.find(string=re.compile(r"(?:Partner|Lead|Board):\s*(.+)", re.I)),
                    card.find(["span", "div"], class_=re.compile(r"partner|lead|board", re.I)),
                ]
                for pp in partner_patterns:
                    if pp:
                        ptext = pp.get_text(strip=True) if hasattr(pp, 'get_text') else str(pp)
                        m = re.search(r"(?:Partner|Lead|Board):\s*(.+)", ptext, re.I)
                        lead_partner = m.group(1).strip() if m else ptext.strip()
                        break

                # Extract sector if mentioned
                sector = None
                sector_el = card.find(["span", "div"], class_=re.compile(r"sector|category|industry|tag", re.I))
                if sector_el:
                    sector = sector_el.get_text(strip=True)

                results.append({
                    "company_name": name,
                    "company_website": href,
                    "description": desc,
                    "lead_partner": lead_partner,
                    "sector": sector,
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


def run_portfolio_scraper():
    """Scrape portfolio pages of top early-stage NYC VCs and store companies in DB."""
    import time

    conn = get_connection()
    log_id = log_scrape(conn, "portfolio_scraper")
    total_found = 0
    total_new = 0

    try:
        # Get all firms with portfolio URLs
        firms = conn.execute(
            "SELECT id, name, portfolio_url FROM firms WHERE portfolio_url IS NOT NULL"
        ).fetchall()

        for firm in firms:
            firm_id = firm["id"]
            firm_name = firm["name"]
            portfolio_url = firm["portfolio_url"]

            if not portfolio_url:
                continue

            companies = scrape_firm_portfolio(firm_name, portfolio_url)
            total_found += len(companies)

            for co in companies:
                kwargs = {}
                if co.get("company_website"):
                    kwargs["company_website"] = co["company_website"]
                if co.get("description"):
                    kwargs["description"] = co["description"]
                if co.get("lead_partner"):
                    kwargs["lead_partner"] = co["lead_partner"]
                if co.get("sector"):
                    kwargs["sector"] = co["sector"]
                if co.get("source_url"):
                    kwargs["source_url"] = co["source_url"]

                try:
                    pid = upsert_portfolio_company(conn, firm_id, co["company_name"], **kwargs)
                    if pid:
                        total_new += 1
                except Exception as e:
                    logger.warning(f"Failed to store portfolio company {co['company_name']}: {e}")

            # Brief delay between firms to be polite
            time.sleep(2)

        conn.commit()
        finish_scrape(conn, log_id, "success", total_found, total_new)
        logger.info(f"Portfolio scraper: found {total_found} companies, stored {total_new}")
    except Exception as e:
        finish_scrape(conn, log_id, "error", error_message=str(e))
        logger.error(f"Portfolio scraper error: {e}")
    finally:
        conn.close()


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

            # Scrape portfolio page and store in portfolio_companies table
            if portfolio_url:
                companies = scrape_firm_portfolio(firm_name, portfolio_url)
                total_found += len(companies)
                for co in companies:
                    kwargs = {}
                    if co.get("company_website"):
                        kwargs["company_website"] = co["company_website"]
                    if co.get("description"):
                        kwargs["description"] = co["description"]
                    if co.get("lead_partner"):
                        kwargs["lead_partner"] = co["lead_partner"]
                    if co.get("sector"):
                        kwargs["sector"] = co["sector"]
                    if co.get("source_url"):
                        kwargs["source_url"] = co["source_url"]
                    try:
                        upsert_portfolio_company(conn, firm["id"], co["company_name"], **kwargs)
                        total_new += 1
                    except Exception as e:
                        logger.warning(f"Failed to store {co['company_name']}: {e}")

            # Scrape news page
            if website:
                news = scrape_firm_news(firm_name, website)
                total_found += len(news)

        conn.commit()
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


