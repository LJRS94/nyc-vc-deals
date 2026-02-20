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
    log_scrape, finish_scrape, get_category_id, upsert_portfolio_company,
    upsert_investor,
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
            name = firm_data["name"]
            kwargs = {k: v for k, v in firm_data.items() if k != "name"}
            upsert_firm(conn, name, **kwargs)
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


# ── Team Page Scraping ─────────────────────────────────────────

# Common URL paths for team/people pages
_TEAM_PATHS = ["/team", "/about/team", "/people", "/about", "/partners", "/about-us", "/our-team"]

# Titles that indicate investment-relevant roles
_INVESTMENT_TITLES_RE = re.compile(
    r"""(?ix)
    (?:
        Partner
      | Managing\s+Director
      | Principal
      | Vice\s+President
      | \bVP\b
      | Analyst
      | \bAssociate\b
      | Venture\s+Partner
      | General\s+Partner
      | Founding\s+Partner
      | Managing\s+Partner
      | Investment\s+Director
      | Director
      | Managing\s+Member
      | Co-?Founder
      | Founder
    )
    """,
)

# Titles to skip (non-investment roles)
_SKIP_TITLES_RE = re.compile(
    r"""(?ix)
    (?:
        Office\s+Manager
      | Executive\s+Assistant
      | Administrative
      | Receptionist
      | Marketing\s+(?:Manager|Coordinator|Associate|Director)
      | Communications
      | Human\s+Resources
      | \bHR\b
      | Accounting
      | Legal\s+(?:Counsel|Assistant)
      | IT\s+(?:Manager|Director|Support)
      | Graphic\s+Design
      | Event
    )
    """,
)


def scrape_firm_team(firm_name: str, website: str, firm_id: int) -> List[Dict]:
    """
    Scrape a firm's team/people page for investor names, titles, and LinkedIn URLs.
    Returns list of dicts: {name, title, linkedin_url}.
    """
    results = []
    base = website.rstrip("/")

    # Step 1: Find a working team page
    team_html = None
    team_url = None
    for path in _TEAM_PATHS:
        url = base + path
        try:
            resp = fetch(url, timeout=15, ttl=86400 * 7)
            if resp.status_code == 200:
                team_html = resp.text
                team_url = url
                break
        except Exception:
            continue

    if not team_html:
        logger.debug(f"[{firm_name}] No team page found")
        return results

    soup = BeautifulSoup(team_html, "html.parser")

    # Step 2: Try JSON-LD Person schema first
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string or "")
            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if item.get("@type") == "Person":
                    name = item.get("name", "").strip()
                    title = item.get("jobTitle", "").strip() or None
                    linkedin = None
                    for link in (item.get("sameAs") or []):
                        if "linkedin.com/in/" in str(link):
                            linkedin = link
                            break
                    if name:
                        results.append({"name": name, "title": title, "linkedin_url": linkedin})
        except (json.JSONDecodeError, TypeError):
            continue

    if results:
        logger.info(f"[{firm_name}] Found {len(results)} team members via JSON-LD")
        return _filter_team_results(results)

    # Step 3: Find repeating team card containers
    card_selectors = [
        "div[class*='team-member']", "div[class*='team_member']",
        "div[class*='person']", "li[class*='team']",
        "div[class*='member']", "article[class*='team']",
        "div[class*='staff']", "div[class*='bio']",
        "div[class*='leader']", "div[class*='partner']",
    ]

    cards = []
    for sel in card_selectors:
        cards = soup.select(sel)
        if len(cards) >= 2:  # Need at least 2 to confirm it's a pattern
            break

    if not cards:
        # Fallback: look for sections with multiple h2/h3 headings that look like names
        cards = _find_name_card_containers(soup)

    for card in cards:
        name = None
        title = None
        linkedin_url = None

        # Extract name from heading
        name_el = card.find(["h2", "h3", "h4"])
        if name_el:
            name = name_el.get_text(strip=True)

        # If no heading, try first bold/strong text
        if not name:
            strong = card.find(["strong", "b"])
            if strong:
                name = strong.get_text(strip=True)

        if not name:
            continue

        # Skip if name doesn't look like a person name (too short, has digits, etc.)
        if len(name) < 3 or len(name) > 60 or re.search(r"\d", name):
            continue
        # Skip if name looks like a company/section header
        if any(kw in name.lower() for kw in ["portfolio", "team", "about", "contact", "investment"]):
            continue

        # Extract title from adjacent text elements
        for el in card.find_all(["p", "span", "div", "h5", "h6"]):
            text = el.get_text(strip=True)
            if text and text != name and _INVESTMENT_TITLES_RE.search(text):
                title = text
                break

        # Extract LinkedIn URL
        for a in card.find_all("a", href=True):
            href = a["href"]
            if "linkedin.com/in/" in href:
                linkedin_url = href
                break

        results.append({"name": name, "title": title, "linkedin_url": linkedin_url})

    logger.info(f"[{firm_name}] Found {len(results)} team members from {team_url}")
    return _filter_team_results(results)


def _find_name_card_containers(soup: BeautifulSoup) -> list:
    """
    Fallback: find parent containers that each have exactly one heading
    that looks like a person name (2-4 words, no digits).
    """
    name_re = re.compile(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}$")
    containers = []
    for heading in soup.find_all(["h2", "h3", "h4"]):
        text = heading.get_text(strip=True)
        if name_re.match(text) and heading.parent:
            containers.append(heading.parent)
    return containers


def _filter_team_results(results: List[Dict]) -> List[Dict]:
    """Filter team results to investment-relevant members."""
    filtered = []
    for r in results:
        title = r.get("title") or ""
        # If there's a title, check it's investment-relevant
        if title and _SKIP_TITLES_RE.search(title):
            continue
        # If there's no title, still include — we can't know for sure
        filtered.append(r)
    return filtered


def run_team_scraper(limit: Optional[int] = None, dry_run: bool = False) -> Dict:
    """
    Scrape team pages for all firms with websites. Creates investor records.

    Args:
        limit: Max number of firms to scrape (None = all).
        dry_run: If True, log but don't write to DB.

    Returns:
        Stats dict: {firms_scraped, team_members_found, investors_created}.
    """
    import time as _time

    stats = {"firms_scraped": 0, "team_members_found": 0, "investors_created": 0}
    conn = get_connection()

    query = "SELECT id, name, website FROM firms WHERE website IS NOT NULL"
    params = ()
    if limit:
        query += " LIMIT ?"
        params = (limit,)
    firms = conn.execute(query, params).fetchall()

    logger.info(f"[TeamScraper] Scanning {len(firms)} firms for team pages (dry_run={dry_run})")

    for firm in firms:
        firm_id = firm["id"]
        firm_name = firm["name"]
        website = firm["website"]

        if not website:
            continue

        members = scrape_firm_team(firm_name, website, firm_id)
        stats["firms_scraped"] += 1
        stats["team_members_found"] += len(members)

        if not dry_run:
            for m in members:
                kwargs = {}
                if m.get("title"):
                    kwargs["title"] = m["title"]
                if m.get("linkedin_url"):
                    kwargs["linkedin_url"] = m["linkedin_url"]
                try:
                    upsert_investor(conn, name=m["name"], firm_id=firm_id, **kwargs)
                    stats["investors_created"] += 1
                except Exception as e:
                    logger.debug(f"[TeamScraper] Failed to upsert investor {m['name']}: {e}")

        # Rate limit: 2s between firms
        _time.sleep(2)

    if not dry_run:
        conn.commit()

    logger.info(
        f"[TeamScraper] Done: {stats['firms_scraped']} firms scraped, "
        f"{stats['team_members_found']} members found, "
        f"{stats['investors_created']} investors created"
    )
    return stats


def run_portfolio_scraper():
    """Scrape portfolio pages of top early-stage NYC VCs and store companies in DB."""
    import time

    conn = get_connection()
    log_id = log_scrape(conn, "portfolio_scraper")
    total_found = 0
    total_new = 0

    try:
        # Get all firms with portfolio URLs, skip firms with consecutive failures
        firms = conn.execute(
            "SELECT id, name, portfolio_url, consecutive_failures FROM firms "
            "WHERE portfolio_url IS NOT NULL AND COALESCE(consecutive_failures, 0) < 3"
        ).fetchall()

        for firm in firms:
            firm_id = firm["id"]
            firm_name = firm["name"]
            portfolio_url = firm["portfolio_url"]

            if not portfolio_url:
                continue

            companies = scrape_firm_portfolio(firm_name, portfolio_url)

            # Track success/failure for skipping dead websites
            if not companies:
                conn.execute(
                    "UPDATE firms SET consecutive_failures = COALESCE(consecutive_failures, 0) + 1 WHERE id = ?",
                    (firm_id,))
            else:
                conn.execute(
                    "UPDATE firms SET consecutive_failures = 0, last_scraped_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (firm_id,))

            # Cap at 200 portfolio companies per firm to avoid nav-link pollution
            if len(companies) > 200:
                logger.warning(f"[{firm_name}] Capping {len(companies)} companies to 200")
                companies = companies[:200]

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


def run_firm_scraper():
    """Main entry point: scrape all firm websites."""
    conn = get_connection()
    log_id = log_scrape(conn, "firm_websites")
    total_found = 0
    total_new = 0

    try:
        firms = conn.execute(
            "SELECT * FROM firms WHERE COALESCE(consecutive_failures, 0) < 3"
        ).fetchall()
        for firm in firms:
            firm_name = firm["name"]
            portfolio_url = firm["portfolio_url"]
            website = firm["website"]

            # Scrape portfolio page and store in portfolio_companies table
            if portfolio_url:
                companies = scrape_firm_portfolio(firm_name, portfolio_url)

                # Track failures to skip dead websites next run
                if not companies and website:
                    conn.execute(
                        "UPDATE firms SET consecutive_failures = COALESCE(consecutive_failures, 0) + 1 WHERE id = ?",
                        (firm["id"],))
                elif companies:
                    conn.execute(
                        "UPDATE firms SET consecutive_failures = 0, last_scraped_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (firm["id"],))

                # Cap per-firm to avoid nav-link pollution
                if len(companies) > 200:
                    logger.warning(f"[{firm_name}] Capping {len(companies)} companies to 200")
                    companies = companies[:200]

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    seed_firms()
    run_firm_scraper()


