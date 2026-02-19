"""
Shared parsing utilities for all scrapers.
Canonical implementations — single source of truth.

V.06 changes:
  - Series C-H now map to "Series C+" instead of "Series B"
  - Added company_names_match() for fuzzy dedup
  - Added classify_stage_from_amount() (moved from sec_scraper)
  - Added "growth equity" / "expansion" stage mappings
"""

import re
from functools import lru_cache
from typing import Optional, List, Tuple


# ── Stage normalization ───────────────────────────────────────

STAGE_MAP = {
    "pre-seed": "Pre-Seed", "pre seed": "Pre-Seed", "angel": "Pre-Seed",
    "friends and family": "Pre-Seed",
    "seed": "Seed", "seed-stage": "Seed",
    "series a": "Series A", "series-a": "Series A",
    "series b": "Series B", "series-b": "Series B",
    # V.06 fix: C+ rounds are NOT Series B
    "series c": "Series C+", "series-c": "Series C+",
    "series d": "Series C+", "series-d": "Series C+",
    "series e": "Series C+", "series-e": "Series C+",
    "series f": "Series C+", "series-f": "Series C+",
    "series g": "Series C+", "series-g": "Series C+",
    "series h": "Series C+", "series-h": "Series C+",
    "venture": "Seed",
    "growth": "Series C+",
    "growth equity": "Series C+",
    "expansion": "Series C+",
    "late stage": "Series C+",
    "late-stage": "Series C+",
}


def normalize_stage(raw: str) -> str:
    """Normalize round labels to our 5-stage schema."""
    if not raw:
        return "Unknown"
    raw_lower = raw.lower().strip()
    for pattern, stage in STAGE_MAP.items():
        if pattern in raw_lower:
            return stage
    return "Unknown"


def classify_stage_from_amount(amount: Optional[float]) -> str:
    """Estimate funding stage from amount when no other signal exists."""
    if amount is None:
        return "Unknown"
    if amount < 500_000:
        return "Pre-Seed"
    if amount < 3_000_000:
        return "Seed"
    if amount < 20_000_000:
        return "Series A"
    if amount < 80_000_000:
        return "Series B"
    return "Series C+"  # V.06 fix: was "Unknown", losing data


# ── Amount parsing ────────────────────────────────────────────

def parse_amount(text: str) -> Optional[float]:
    """Extract dollar amount from strings like '$30M', '$4.5 million', '30000000'.
    Caps at $50M — this is an early-stage VC tracker. Billion-dollar amounts
    are almost always valuations, not round sizes."""
    if not text:
        return None
    clean = text.replace(",", "").strip()

    # Skip billion-dollar amounts entirely — these are valuations, not rounds
    m = re.search(r"\$\s*(\d[\d.]*)\s*(B|billion)", clean, re.I)
    if m:
        return None  # valuations, not deal sizes
    m = re.search(r"(\d[\d.]*)\s*(B|billion)", clean, re.I)
    if m:
        return None

    # Dollar-prefixed with M/million
    m = re.search(r"\$\s*(\d[\d.]*)\s*(M|million|mm)", clean, re.I)
    if m:
        val = float(m.group(1)) * 1_000_000
        return val if val <= MAX_EARLY_STAGE_AMOUNT else None
    m = re.search(r"\$\s*(\d[\d.]*)\s*(K|thousand)", clean, re.I)
    if m:
        return float(m.group(1)) * 1_000
    # Non-dollar-prefixed with multiplier
    m = re.search(r"(\d[\d.]*)\s*(M|million|mm)", clean, re.I)
    if m:
        val = float(m.group(1)) * 1_000_000
        return val if val <= MAX_EARLY_STAGE_AMOUNT else None
    m = re.search(r"(\d[\d.]*)\s*(K|thousand)", clean, re.I)
    if m:
        return float(m.group(1)) * 1_000
    # Bare dollar amount (requires $ sign to avoid matching years)
    m = re.search(r"\$\s*(\d[\d.]*)", clean)
    if m:
        val = float(m.group(1))
        if val > 100_000:
            return val if val <= MAX_EARLY_STAGE_AMOUNT else None
        if val > 100:
            val = val * 1_000_000
            return val if val <= MAX_EARLY_STAGE_AMOUNT else None
    return None


# ── Sector classification ─────────────────────────────────────

SECTOR_KEYWORDS = {
    "Fintech":            ["fintech", "financial services", "financial technology",
                           "payments", "banking", "lending", "personal finance",
                           "neobank", "defi", "credit", "wealth management"],
    "Health & Biotech":   ["health", "biotech", "healthcare", "medical", "clinical",
                           "wellness", "nutrition", "pharmaceutical", "mhealth",
                           "therapeutics", "telemedicine", "digital health",
                           "genomics", "women's", "pharma"],
    "AI / Machine Learning": ["artificial intelligence", "ai", "machine learning",
                              "generative ai", "llm", "deep learning", "nlp",
                              "computer vision", "foundation model"],
    "Cybersecurity":      ["cybersecurity", "cyber security", "security",
                           "threat detection", "identity", "infosec", "encryption"],
    "SaaS / Enterprise":  ["saas", "enterprise software", "enterprise", "b2b",
                           "productivity", "workflow", "crm", "cloud",
                           "platform", "software-as-a-service", "business software"],
    "Web3 / Crypto":      ["blockchain", "cryptocurrency", "crypto", "defi",
                           "web3", "decentralized", "stablecoin", "nft", "dao", "token"],
    "Real Estate / Proptech": ["real estate", "proptech", "property",
                               "self-storage", "commercial real estate",
                               "housing", "construction tech", "mortgage"],
    "Insurance / Insurtech": ["insurance", "insurtech", "underwriting",
                              "risk management", "claims", "policy"],
    "Consumer / D2C":     ["consumer", "d2c", "direct-to-consumer", "beauty",
                           "fashion", "e-commerce", "retail", "brand", "shopping"],
    "Developer Tools":    ["developer tools", "developer", "devtools", "code",
                           "software engineering", "infrastructure", "api",
                           "open source", "sdk", "ci/cd"],
    "Climate / Cleantech":["climate", "cleantech", "clean energy", "carbon",
                           "sustainability", "environmental", "renewable",
                           "energy", "green"],
    "Media & Entertainment": ["media", "news", "entertainment", "content",
                              "video", "creative"],
    "HR / Future of Work":["human resources", "hr", "recruiting",
                           "employee benefits", "reskilling", "career",
                           "remote work", "talent", "workforce", "hiring"],
    "Food & Agriculture": ["food", "restaurant", "agriculture", "delivery",
                           "agtech", "farming", "meal", "grocery"],
    "Marketplace":        ["marketplace", "platform", "two-sided",
                           "platform connecting", "matching", "gig economy"],
    "Legal Tech":         ["legal", "legaltech", "compliance", "regulatory",
                           "law", "contract"],
    "Logistics / Supply Chain": ["logistics", "supply chain", "shipping",
                                 "procurement", "warehouse", "freight"],
    "Education / Edtech": ["education", "edtech", "learning", "tutoring",
                           "school", "student"],
}


@lru_cache(maxsize=2048)
def classify_sector(text: str) -> Optional[str]:
    """Return the best-matching sector category for a deal description."""
    if not text:
        return None
    text_lower = text.lower()
    scores = {}
    for sector, keywords in SECTOR_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in text_lower)
        if score > 0:
            scores[sector] = score
    if scores:
        return max(scores, key=scores.get)
    return None


# ── Company name normalization (for dedup) ────────────────────

_STRIP_RE = re.compile(r"[^a-z0-9]")


def normalize_company_name(name: str) -> str:
    """
    Normalize a company name for dedup comparison.
    Strips punctuation, spaces, case — "Sixfold AI" → "sixfoldai".
    """
    if not name:
        return ""
    return _STRIP_RE.sub("", name.lower())


def company_names_match(a: str, b: str, threshold: float = 0.85) -> bool:
    """
    Fuzzy match two company names.
    V.06 addition — catches "Sixfold AI" vs "Sixfold", "FJ Labs" vs "FJLabs".
    Uses normalized containment + length ratio.
    """
    na, nb = normalize_company_name(a), normalize_company_name(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    # Containment: one is a substring of the other
    if na in nb or nb in na:
        shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
        ratio = len(shorter) / len(longer)
        return ratio >= threshold
    return False


# ── Deal filters ─────────────────────────────────────────────

MAX_EARLY_STAGE_AMOUNT = 50_000_000  # $50M cap for early-stage deals

_firm_names_cache = None  # cached set of normalized firm names


def _get_firm_names(conn) -> set:
    """Load and cache normalized firm names from DB."""
    global _firm_names_cache
    if _firm_names_cache is None:
        rows = conn.execute("SELECT name FROM firms").fetchall()
        _firm_names_cache = set()
        for r in rows:
            _firm_names_cache.add(normalize_company_name(r["name"]))
    return _firm_names_cache


def clear_firm_cache():
    """Clear the firm name cache (call after seeding firms)."""
    global _firm_names_cache
    _firm_names_cache = None


def is_vc_firm(conn, company_name: str) -> bool:
    """Check if a 'company' is actually a VC firm in our database."""
    if not company_name:
        return False
    norm = normalize_company_name(company_name)
    if not norm or len(norm) < 3:
        return False
    firm_names = _get_firm_names(conn)
    # Exact normalized match
    if norm in firm_names:
        return True
    # Containment match (e.g. "Insight Partners Fund" contains "insightpartners")
    for fn in firm_names:
        if len(fn) >= 5 and (fn in norm or norm in fn):
            shorter, longer = (fn, norm) if len(fn) <= len(norm) else (norm, fn)
            if len(shorter) / len(longer) >= 0.7:
                return True
    return False


def should_skip_deal(conn, company_name: str, amount: float = None) -> str:
    """
    Returns a reason string if the deal should be skipped, or None if it's OK.
    Checks:
    1. Company is a known VC firm (not a startup)
    2. Amount exceeds $50M (not early-stage)
    """
    if is_vc_firm(conn, company_name):
        return f"VC firm: {company_name}"
    if amount and amount > MAX_EARLY_STAGE_AMOUNT:
        return f"Amount ${amount/1e6:.0f}M exceeds $50M cap"
    return None


# ── Investor parsing ──────────────────────────────────────────

def parse_investors(text: str) -> Tuple[List[str], Optional[str]]:
    """
    Parse an investor string.
    Returns (all_investors, lead_investor).
    Handles 'led by X', 'from investors including X, Y, Z', etc.
    """
    if not text:
        return [], None

    lead = None
    m = re.search(r"led by\s+([^,.]+(?:,\s*[^,.]+)?)", text, re.I)
    if m:
        lead = m.group(1).strip()

    cleaned = re.sub(r"(from\s+)?investors?\s+(that\s+)?include\s*", "", text, flags=re.I)
    cleaned = re.sub(r"led\s+by\s+", "", cleaned, flags=re.I)
    cleaned = re.sub(r"with\s+participation\s+from\s+", ", ", cleaned, flags=re.I)
    cleaned = re.sub(r"\s+and\s+", ", ", cleaned, flags=re.I)

    investors = []
    for inv in cleaned.split(","):
        inv = inv.strip().rstrip(".")
        if inv and len(inv) > 1 and not inv.lower().startswith(("including", "with", "from")):
            investors.append(inv)

    if not lead and investors:
        lead = investors[0]

    return investors, lead

