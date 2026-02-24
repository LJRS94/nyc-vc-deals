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


_JUNK_PORTFOLIO_RE = re.compile(
    r"^(GET IN TOUCH|Go-To-Market|View All|Load More|Show More|"
    r"Learn More|Read More|Visit Website|Visit Site|Back to Top|Contact Us|"
    r"About Us|About|Our Team|Our Portfolio|Our Startups|Our Mission|See All|See More|Subscribe|"
    r"Sign Up|Log In|Login|Sign In|Privacy Policy|Privacy|Privacy Center|"
    r"Terms of Service|Cookie Policy|Cookie Settings|"
    r"Filter|Sort|Search|Menu|Close|Open|All Companies|All|"
    r"Current Portfolio|Previous Portfolio|Active|Exited|"
    r"Limited Partner Login|Investor Portal|LP Portal|LP Log-In|"
    r"For Investors|For Founders|For LPs|For LP's|How We Invest|"
    r"Series [a-e]|Series [A-E]|Pre-Seed|Seed|IPO|"
    r"Gaming|Health|Health Tech|Consumer|Finance|Media|Software|Education|"
    r"Marketplace|Other|Resources|News|Blog|Press|Insights|"
    r"Team|Studio|LinkedIn|Twitter|Facebook|Instagram|Podcast|"
    r"FAQs?|Reset|Apply|Cancel|Submit|Back|Next|Previous|More|Less|"
    r"Fundraising|Founder Services|Investments|Partners|Network|"
    r"Careers|Events|Overview|Contact|Home|Stage|Spotlight|"
    r"Trending topics|Disclosures|Featured|Enterprise|Commerce|"
    r"Crypto|Robotics|Space|Hardware|Fintech|Cybersecurity|"
    r"AI Apps|AI Infrastructure & Developer Platforms|"
    r"Data, AI & Machine Learning|Energy & Infrastructure|"
    r"Enterprise Apps & Vertical AI|Infrastructure & Developer Tools|"
    r"Loading\.\.\.)$",
    re.I,
)

# Patterns that indicate metadata/junk anywhere in the string
_JUNK_ANYWHERE_RE = re.compile(
    r"(Founder\(s\)|Partner Since|Year of Investment|Investment Status|"
    r"Entry Stage|Country:|Industry:|Sector:|DISCLAIMER|Portfolio Highlights)",
    re.I,
)

_JUNK_CONTENT_RE = re.compile(
    r"(Published on|Exits?(true|false)$|Stage RTP|SectorFintech|"
    r"SectorAI|SectorSaaS|SectorE-commerce|SectorAgriculture|CustomerB2[BC]|"
    r"New York, NY.*Enterprise|Tel Aviv.*Enterprise|"
    r"San Francisco, CA.*Enterprise|"
    r"Status:?(Current|Exited|Active)|"
    r"AllMedia$|AllCommerce$|AllSaaS$|AllFinTech$|AllHealthcare$|"
    r"AllEducation$|AllHR$|AllPropTech$|AllSocial$|AllCrypto$|"
    r"CommerceAll$|FinTechAll$|HealthcareAll$|EducationAll$|SaaSAll$|"
    r"Link opens in new tab|"
    r"ENTERPRISE WEEKLY NEWSLETTER|BROWSE OUR|PLAY VIDEO|"
    r"VIEW LEGAL|NVP PROMISE|COMPANY↑|"
    r"Initial investment:|Entry Year:|Entry Stage:|Country:|"
    r"Marketplace:All)",
    re.I,
)


def _is_valid_portfolio_name(name: str) -> bool:
    """Return True if name looks like a real company name, not UI junk."""
    if not name or len(name.strip()) < 2:
        return False
    name = name.strip()
    # Too long for a company name
    if len(name) > 60:
        return False
    # Year-only
    if re.match(r"^\d{4}$", name):
        return False
    # Pure number
    if re.match(r"^\d+$", name):
        return False
    # Nav/UI junk (exact match)
    if _JUNK_PORTFOLIO_RE.match(name):
        return False
    # Metadata anywhere in string
    if _JUNK_ANYWHERE_RE.search(name):
        return False
    # Metadata/description junk
    if _JUNK_CONTENT_RE.search(name):
        return False
    # Contains sentence-like patterns (descriptions scraped as names)
    if len(name) > 40 and any(w in name.lower() for w in
            [" is a ", " is an ", " provides ", " delivers ", " develops ",
             " offers ", " enables ", " builds ", " allows ", " revolutionizes ",
             " partnering ", " dedicated to ", " bringing ", " powered by "]):
        return False
    # Starts with description-like phrases
    if re.match(r"^(AI-powered|An? investment|A specialty|An? AI|A platform|The leading|An? \w+ that)", name, re.I):
        return False
    # Concatenated metadata (e.g. "CONSUMERMedia2017", "RRE Invested2025")
    if re.search(r"(Consumer|Media|Health|Finance|Software|Education|Marketplace)\d{4}$", name):
        return False
    if re.search(r"Invested\d{4}$", name):
        return False
    # Category-only concatenations (e.g. "AICONSUMER", "CryptoFintech2018", "HealthcareFintech")
    _cats = {'AI', 'CONSUMER', 'Consumer', 'Fintech', 'Healthcare', 'Enterprise',
             'Saas', 'SaaS', 'Hardware', 'Robotics', 'Space', 'Media', 'Commerce',
             'Brands', 'Strategy', 'Featured', 'PropTech', 'Social', 'Crypto',
             'Climate', 'Security', 'Infrastructure', 'Logistics', 'Gaming', 'Education'}
    base = re.sub(r'\d{4}$', '', name).strip()
    remaining = base
    for cat in sorted(_cats, key=len, reverse=True):
        remaining = remaining.replace(cat, '')
    if len(remaining.replace('/', '').replace(' ', '').replace('&', '')) == 0 and len(base) > 3:
        return False
    # "AI" + year (e.g. "AI2024")
    if re.match(r'^AI\d{4}$', name):
        return False
    # Parenthetical UI junk
    if re.match(r"^\(", name):
        return False
    # "Acq:" prefix (acquisition tags like "SpotlightAcq: Enverus")
    if "Acq:" in name:
        return False
    # Stock ticker entries (e.g. "NASDAQ: CHYM", "NYSE: WEAV")
    if re.match(r'^(NASDAQ|NYSE)', name):
        return False
    # "Powered by X", "Design by X" — attribution, not portfolio
    if re.match(r'^(Design|Built|Made|Powered|Created) by ', name, re.I):
        return False
    # City + category concatenations
    if re.match(r'^(Austin|Boston|London|New York|San Francisco|Tel Aviv|Toronto|Washington)', name) and (',' in name or len(name) > 20):
        return False
    # Filter state concatenations (e.g. "FilterConsumerConsumerAI...")
    if re.match(r'^Filter', name) and len(name) > 10:
        return False
    return True


def scrape_firm_portfolio(firm_name: str, portfolio_url: str) -> List[Dict]:
    """
    Portfolio page scraper using site-specific extraction patterns
    with a generic fallback.

    Each VC site has unique HTML structure, so we detect the pattern
    and apply the right extraction logic.
    """
    results = []
    try:
        resp = fetch(portfolio_url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # ── Try site-specific extractors in order ──────────────

        # Pattern 1: m__list-row (USV-style table layout)
        rows = soup.select("div.m__list-row:not(.m__list-row--mobile)")
        if rows:
            seen = set()
            for row in rows:
                links = row.find_all("a", href=True)
                for link in links:
                    href = link.get("href", "")
                    text = link.get_text(strip=True)
                    if (
                        href.startswith("http")
                        and _is_valid_portfolio_name(text)
                        and "usv.com" not in href
                        and text not in seen
                    ):
                        seen.add(text)
                        results.append({
                            "company_name": text,
                            "company_website": href,
                            "source_url": portfolio_url,
                        })
            if results:
                logger.info(f"[{firm_name}] Found {len(results)} companies (list-row pattern)")
                return results

        # Pattern 2: portfolio-card (Greycroft-style cards)
        cards = soup.select("div.portfolio-card")
        if cards:
            for card in cards:
                container = card.select_one("div.container")
                if not container:
                    continue
                # Company name is in first text node of container
                name_el = container.find(["h2", "h3", "h4", "p", "span", "div"])
                name = name_el.get_text(strip=True) if name_el else ""
                if not _is_valid_portfolio_name(name):
                    continue
                if name.lower() in ("yearfilter", "filter", "all"):
                    continue
                # Extract website from "Visit Website" link
                link_el = card.select_one('a[href^="http"]')
                href = link_el.get("href", "") if link_el else ""
                # Skip internal links
                if href and portfolio_url.split("/")[2] in href:
                    href = ""
                desc_el = card.select_one("div.portfolio-card__accordion")
                desc = desc_el.get_text(strip=True)[:200] if desc_el else None
                results.append({
                    "company_name": name,
                    "company_website": href or None,
                    "description": desc,
                    "source_url": portfolio_url,
                })
            if results:
                logger.info(f"[{firm_name}] Found {len(results)} companies (portfolio-card pattern)")
                return results

        # Pattern 3: portfolio-wrap / portfolio-item (Lerer Hippeau / Webflow-style)
        wraps = soup.select("div.portfolio-wrap")
        if wraps:
            for wrap in wraps:
                title_el = (
                    wrap.select_one("div.portfolio-title div.h3")
                    or wrap.select_one("div.h3")
                    or wrap.select_one("h3")
                )
                if not title_el:
                    continue
                name = title_el.get_text(strip=True)
                if not _is_valid_portfolio_name(name):
                    continue
                desc_el = wrap.select_one("div.short-desc") or wrap.select_one("div.text-14px")
                desc = desc_el.get_text(strip=True)[:200] if desc_el else None
                # Clean "Read more." from descriptions
                if desc and desc.endswith("Read more."):
                    desc = desc[:-10].strip()
                results.append({
                    "company_name": name,
                    "company_website": None,
                    "description": desc,
                    "source_url": portfolio_url,
                })
            if results:
                logger.info(f"[{firm_name}] Found {len(results)} companies (portfolio-wrap pattern)")
                return results

        # Pattern 4: Repeating card containers with headings
        _card_selectors = [
            "div.portfolio-company", "div.company-card", "article.portfolio",
            "div.portfolio-item", "li.portfolio-item",
        ]
        cards = []
        for sel in _card_selectors:
            cards = soup.select(sel)
            if len(cards) >= 3:
                break

        if cards:
            for card in cards:
                name_el = card.find(["h2", "h3", "h4"])
                if not name_el:
                    name_el = card.find("a")
                name = name_el.get_text(strip=True) if name_el else None
                if not _is_valid_portfolio_name(name):
                    continue
                link_el = card.find("a", href=True)
                href = link_el["href"] if link_el else None
                if href and href.startswith("/"):
                    href = urljoin(portfolio_url, href)
                desc_el = card.find("p")
                desc = desc_el.get_text(strip=True)[:200] if desc_el else None
                results.append({
                    "company_name": name,
                    "company_website": href,
                    "description": desc,
                    "source_url": portfolio_url,
                })
            if results:
                logger.info(f"[{firm_name}] Found {len(results)} companies (card pattern)")
                return results

        # Pattern 5: External links with company-like names (generic fallback)
        # Collect all external links that look like company websites
        domain = portfolio_url.split("/")[2]
        seen = set()
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            text = link.get_text(strip=True)
            if (
                text
                and href.startswith("http")
                and domain not in href
                and _is_valid_portfolio_name(text)
                and not text.startswith("#")
                and text not in seen
                # Name should look like a company (starts with uppercase or has multiple words)
                and (text[0].isupper() or " " in text)
                # Reject social media / generic platform links
                and not any(s in href for s in [
                    "twitter.com", "linkedin.com", "facebook.com", "instagram.com",
                    "youtube.com", "github.com", "medium.com", "crunchbase.com",
                    "google.com", "apple.com/app", "play.google.com",
                ])
            ):
                seen.add(text)
                results.append({
                    "company_name": text,
                    "company_website": href,
                    "source_url": portfolio_url,
                })

        logger.info(f"[{firm_name}] Found {len(results)} companies (external-link fallback)")
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


