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
import threading
from datetime import datetime
from functools import lru_cache
from typing import Optional, List, Tuple, Dict

from config import (
    DEDUP_DATE_GAP_DAYS, DEDUP_AMOUNT_RATIO, FUZZY_NAME_THRESHOLD,
    FIRM_NAME_MIN_LENGTH, FIRM_MATCH_RATIO,
)


# ── Stage normalization ───────────────────────────────────────

STAGE_MAP = {
    # Pre-Seed
    "pre-seed": "Pre-Seed", "pre seed": "Pre-Seed", "preseed": "Pre-Seed",
    "angel": "Pre-Seed", "angel round": "Pre-Seed",
    "friends and family": "Pre-Seed", "f&f round": "Pre-Seed",
    "convertible note": "Pre-Seed", "safe note": "Pre-Seed",
    "initial funding": "Pre-Seed", "pre-seed round": "Pre-Seed",
    # Seed
    "seed": "Seed", "seed-stage": "Seed", "seed round": "Seed",
    "seed funding": "Seed", "seed extension": "Seed",
    "venture": "Seed", "early stage": "Seed", "early-stage": "Seed",
    "bridge round": "Seed", "bridge financing": "Seed",
    # Series A
    "series a": "Series A", "series-a": "Series A",
    "round a": "Series A", "a round": "Series A",
    "series a1": "Series A", "series a2": "Series A",
    "series a extension": "Series A",
    # Series B
    "series b": "Series B", "series-b": "Series B",
    "round b": "Series B", "b round": "Series B",
    "series b1": "Series B", "series b2": "Series B",
    "series b extension": "Series B",
    # V.06 fix: C+ rounds are NOT Series B
    "series c": "Series C+", "series-c": "Series C+",
    "series d": "Series C+", "series-d": "Series C+",
    "series e": "Series C+", "series-e": "Series C+",
    "series f": "Series C+", "series-f": "Series C+",
    "series g": "Series C+", "series-g": "Series C+",
    "series h": "Series C+", "series-h": "Series C+",
    "round c": "Series C+", "round d": "Series C+",
    "round e": "Series C+", "round f": "Series C+",
    "c round": "Series C+", "d round": "Series C+",
    "growth": "Series C+", "growth equity": "Series C+",
    "growth round": "Series C+", "growth stage": "Series C+",
    "expansion": "Series C+", "expansion round": "Series C+",
    "late stage": "Series C+", "late-stage": "Series C+",
    "strategic round": "Series C+", "strategic investment": "Series C+",
    "mezzanine": "Series C+", "crossover round": "Series C+",
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
    """Estimate funding stage from amount when no other signal exists.
    Updated for 2025-2026 market: seed rounds routinely hit $5-8M,
    Series A is commonly $20-40M.
    """
    if amount is None:
        return "Unknown"
    if amount < 2_000_000:
        return "Pre-Seed"
    if amount < 8_000_000:
        return "Seed"
    if amount < 40_000_000:
        return "Series A"
    if amount < 100_000_000:
        return "Series B"
    return "Series C+"


# ── Amount parsing ────────────────────────────────────────────

def parse_amount(text: str) -> Optional[float]:
    """Extract dollar amount from strings like '$30M', '$4.5 million', '30000000'."""
    if not text:
        return None
    clean = text.replace(",", "").strip()
    # Prefer dollar-prefixed amounts with multiplier suffix
    m = re.search(r"\$\s*(\d[\d.]*)\s*(B|billion)", clean, re.I)
    if m:
        return float(m.group(1)) * 1_000_000_000
    m = re.search(r"\$\s*(\d[\d.]*)\s*(M|million|mm)", clean, re.I)
    if m:
        return float(m.group(1)) * 1_000_000
    m = re.search(r"\$\s*(\d[\d.]*)\s*(K|thousand)", clean, re.I)
    if m:
        return float(m.group(1)) * 1_000
    # Non-dollar-prefixed with explicit multiplier suffix
    m = re.search(r"(\d[\d.]*)\s*(B|billion)", clean, re.I)
    if m:
        return float(m.group(1)) * 1_000_000_000
    m = re.search(r"(\d[\d.]*)\s*(M|million|mm)", clean, re.I)
    if m:
        return float(m.group(1)) * 1_000_000
    m = re.search(r"(\d[\d.]*)\s*(K|thousand)", clean, re.I)
    if m:
        return float(m.group(1)) * 1_000
    # Bare dollar amount (requires $ sign to avoid matching years)
    m = re.search(r"\$\s*(\d[\d.]*)", clean)
    if m:
        val = float(m.group(1))
        # Already looks like a full amount (e.g. $5000000)
        if val >= 100_000:
            return val
        # Don't guess on bare dollar amounts without a multiplier suffix —
        # "$150" is ambiguous (could be $150 or $150M), so return None.
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
    "Robotics / Deep Tech": ["robotics", "robot", "deep tech", "deeptech",
                              "autonomous", "drone", "semiconductor", "quantum",
                              "lidar", "sensor", "actuator", "3d printing",
                              "additive manufacturing", "materials science",
                              "photonics", "nanotechnology", "space tech",
                              "advanced manufacturing", "industrial automation"],
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


def company_names_match(a: str, b: str, threshold: float = FUZZY_NAME_THRESHOLD) -> bool:
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

_firm_names_cache = None  # cached set of normalized firm names
_firm_cache_lock = threading.Lock()


def _get_firm_names(conn) -> set:
    """Load and cache normalized firm names from DB (thread-safe)."""
    global _firm_names_cache
    if _firm_names_cache is not None:
        return _firm_names_cache
    with _firm_cache_lock:
        if _firm_names_cache is not None:
            return _firm_names_cache  # another thread populated it
        rows = conn.execute("SELECT name FROM firms").fetchall()
        cache = set()
        for r in rows:
            cache.add(normalize_company_name(r["name"]))
        _firm_names_cache = cache
    return _firm_names_cache


def clear_firm_cache():
    """Clear the firm name cache (call after seeding firms)."""
    global _firm_names_cache
    with _firm_cache_lock:
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
        if len(fn) >= FIRM_NAME_MIN_LENGTH and (fn in norm or norm in fn):
            shorter, longer = (fn, norm) if len(fn) <= len(norm) else (norm, fn)
            if len(shorter) / len(longer) >= FIRM_MATCH_RATIO:
                return True
    return False


def should_skip_deal(conn, company_name: str, amount: float = None) -> str:
    """
    Returns a reason string if the deal should be skipped, or None if it's OK.
    Checks if the company is actually a known VC firm (not a startup).
    """
    if is_vc_firm(conn, company_name):
        return f"VC firm: {company_name}"
    return None


# ── Investor parsing ──────────────────────────────────────────

def validate_deal_amount(amount: Optional[float], stage: str = "Unknown") -> bool:
    """
    Stage-aware validation: reject amounts that are implausible for the given stage.
    Returns True if the amount is plausible, False if it should be rejected.
    """
    if amount is None:
        return True  # undisclosed is always OK

    if amount <= 0:
        return False

    # Global cap — $10B is unreasonable for any early-stage deal
    if amount > 10_000_000_000:
        return False

    # Stage-specific caps
    caps = {
        "Pre-Seed": 5_000_000,       # $5M
        "Seed": 25_000_000,           # $25M
        "Series A": 100_000_000,      # $100M
        "Series B": 500_000_000,      # $500M
        "Series C+": 5_000_000_000,   # $5B
        "Unknown": 500_000_000,       # $500M default
    }
    cap = caps.get(stage, 500_000_000)
    return amount <= cap


STAGE_ORDER = {"Pre-Seed": 0, "Seed": 1, "Series A": 2, "Series B": 3, "Series C+": 4, "Unknown": -1}


def _dates_close(date_a: Optional[str], date_b: Optional[str],
                  max_gap: int = DEDUP_DATE_GAP_DAYS) -> Optional[bool]:
    """Return True if dates are within max_gap days, False if farther apart,
    None if either date is missing or unparseable."""
    if not date_a or not date_b:
        return None
    try:
        d1 = datetime.strptime(date_a, "%Y-%m-%d")
        d2 = datetime.strptime(date_b, "%Y-%m-%d")
        return abs((d1 - d2).days) <= max_gap
    except (ValueError, TypeError):
        return None


def _amounts_similar(amt_a: Optional[float], amt_b: Optional[float],
                     ratio_threshold: float = DEDUP_AMOUNT_RATIO) -> Optional[bool]:
    """Return True if amounts are within ratio_threshold of each other,
    False if they differ significantly, None if either is missing."""
    if not amt_a or not amt_b:
        return None
    ratio = max(amt_a, amt_b) / max(min(amt_a, amt_b), 1)
    return ratio <= ratio_threshold


def is_duplicate_deal(conn, company_name: str, stage: str,
                      amount: Optional[float] = None,
                      date_announced: Optional[str] = None) -> bool:
    """
    Smart dedup: returns True if this deal is a duplicate of an existing one.
    Keeps legitimate multi-round deals while catching duplicates that have
    different stage labels (common when multiple sources classify differently).

    Rules:
    - Same company + same amount + close dates → duplicate (regardless of stage)
    - Same company + same stage + close dates → duplicate
    - Same company + different stage + different amount + far dates → NEW round
    - Same company + no distinguishing signals → duplicate (can't differentiate)
    """
    norm = normalize_company_name(company_name)
    if not norm:
        return False

    # Exact normalized match
    existing = conn.execute(
        "SELECT id, stage, amount_usd, date_announced, company_name_normalized FROM deals "
        "WHERE company_name_normalized = ?",
        (norm,)
    ).fetchall()

    # Fuzzy match: find deals where the normalized name contains or is contained by ours
    if not existing and len(norm) >= 4:
        fuzzy_rows = conn.execute(
            "SELECT id, stage, amount_usd, date_announced, company_name_normalized FROM deals "
            "WHERE company_name_normalized LIKE ? OR ? LIKE '%' || company_name_normalized || '%'",
            (f"%{norm}%", norm)
        ).fetchall()
        for row in fuzzy_rows:
            if company_names_match(company_name, row["company_name_normalized"]):
                existing.append(row)

    if not existing:
        return False

    for row in existing:
        ex_stage = row["stage"]
        ex_amount = row["amount_usd"]
        ex_date = row["date_announced"]

        dates_close = _dates_close(date_announced, ex_date)
        amounts_sim = _amounts_similar(amount, ex_amount)

        # ── Strong duplicate signal: same amount + close dates ──
        # Regardless of stage label, if the amount and date match,
        # it's the same deal reported with a different stage classification.
        if amounts_sim is True and dates_close is True:
            return True

        # ── Same amount + no date info → likely same deal ──
        if amounts_sim is True and dates_close is None:
            return True

        # ── Close dates + no amount info → likely same deal ──
        if dates_close is True and amounts_sim is None:
            # But only if stages are compatible (same, or one is Unknown)
            if stage == ex_stage or stage == "Unknown" or ex_stage == "Unknown":
                return True

        # ── Same stage explicitly ──
        if stage == ex_stage or stage == "Unknown" or ex_stage == "Unknown":
            # Far apart dates → new round
            if dates_close is False:
                continue
            # Significantly different amounts → different round
            if amounts_sim is False:
                continue
            # Same stage + close/unknown dates + similar/unknown amounts → duplicate
            return True

        # ── Different stages, no amount/date overlap → new round ──
        # Truly different stages with no contradicting signals — keep both
        continue

    return False


def parse_investors(text: str) -> Tuple[List[str], Optional[str]]:
    """
    Parse an investor string.
    Returns (all_investors, lead_investor).
    Handles 'led by X', 'from investors including X, Y, Z', etc.
    """
    if not text:
        return [], None

    lead = None
    m = re.search(r"led by\s+([^,.]+?)(?:\s+with\s+|\s+and\s+|,|\.|$)", text, re.I)
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

    # Only assign lead if explicitly stated with "led by" — don't guess
    return investors, lead


# ── RSS date parsing (shared by news_scraper, alleywatch_scraper) ──

def ensure_full_date(date_str: str) -> Optional[str]:
    """
    Ensure a date string is full YYYY-MM-DD format.
    If only YYYY-MM is provided, appends '-01'.
    Returns None for invalid/unparseable dates.
    """
    if not date_str:
        return None
    date_str = date_str.strip()
    # Already full YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return date_str
    # YYYY-MM only — append day 01
    if re.match(r"^\d{4}-\d{2}$", date_str):
        return date_str + "-01"
    return None


def parse_pub_date(date_str: str) -> Optional[str]:
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


# ── City detection (shared by news_scraper, delaware_scraper) ────────

NYC_INDICATORS = [
    "new york", "nyc", "manhattan", "brooklyn", "queens",
    "bronx", "staten island", "ny-based", "new york-based",
    "headquartered in new york", "based in nyc", "flatiron",
    "soho", "tribeca", "midtown", "wall street", "chelsea",
    "greenpoint", "williamsburg", "dumbo", "fidi",
]


def detect_city(text: str) -> Optional[str]:
    """Return the first matching city display name, or None.

    Checks all enabled cities from CITY_REGISTRY. First match wins.
    """
    from config import CITY_REGISTRY, ENABLED_CITIES
    text_lower = text.lower()
    for city_name in ENABLED_CITIES:
        cfg = CITY_REGISTRY.get(city_name)
        if not cfg:
            continue
        if any(ind in text_lower for ind in cfg["indicators"]):
            return city_name
    return None


def is_city_related(text: str, city_name: str) -> bool:
    """Return True if text contains indicators for the given city."""
    from config import CITY_REGISTRY
    cfg = CITY_REGISTRY.get(city_name, {})
    indicators = cfg.get("indicators", [])
    text_lower = text.lower()
    return any(ind in text_lower for ind in indicators)


def is_nyc_related(text: str) -> bool:
    """Return True if text contains NYC location indicators (backward compat)."""
    text_lower = text.lower()
    return any(indicator in text_lower for indicator in NYC_INDICATORS)


# ── Investor linking (shared by news_scraper, alleywatch_scraper) ───

_INDIVIDUAL_RE = re.compile(
    r"^[A-Z][a-z]+\s+[A-Z][a-z]+$"
)

_FIRM_KEYWORDS = (
    "capital", "ventures", "partners", "group", "labs", "fund",
    "invest", "vc", "advisors", "management", "equity", "holdings",
    "accelerator", "studio",
)


def link_investors_to_deal(conn, deal_id: int, investors: List[Dict],
                           upsert_investor_fn, link_deal_investor_fn,
                           upsert_firm_fn, link_deal_firm_fn):
    """
    Link investor and firm records to a deal.
    Accepts DB helper functions to avoid importing database at module level.
    """
    # Cache all firms once to avoid N queries for fuzzy matching
    all_firms = conn.execute("SELECT id, name FROM firms").fetchall()

    lead_investor_id = None
    for inv_data in investors:
        inv_name = inv_data["name"]
        role = inv_data.get("role", "participant")

        # Exact match (case-insensitive)
        firm_row = conn.execute(
            "SELECT id FROM firms WHERE LOWER(name) = LOWER(?)",
            (inv_name,)
        ).fetchone()
        firm_id = firm_row["id"] if firm_row else None

        # Fuzzy match fallback against cached firms
        if not firm_id:
            for firm in all_firms:
                if company_names_match(inv_name, firm["name"]):
                    firm_id = firm["id"]
                    break

        inv_id = upsert_investor_fn(conn, name=inv_name, firm_id=firm_id)
        link_deal_investor_fn(conn, deal_id, inv_id)

        if firm_id:
            link_deal_firm_fn(conn, deal_id, firm_id, role)
        else:
            # Auto-create firm if name looks like a firm, not an individual
            name_lower = inv_name.lower()
            if (any(kw in name_lower for kw in _FIRM_KEYWORDS)
                    and not _INDIVIDUAL_RE.match(inv_name)):
                new_firm_id = upsert_firm_fn(conn, inv_name, location="Unknown")
                link_deal_firm_fn(conn, deal_id, new_firm_id, role)
                # Add to cache so subsequent investors in same deal can match
                all_firms.append({"id": new_firm_id, "name": inv_name})

        if role == "lead" and lead_investor_id is None:
            lead_investor_id = inv_id

    if lead_investor_id:
        conn.execute(
            "UPDATE deals SET lead_investor_id = ? WHERE id = ?",
            (lead_investor_id, deal_id)
        )

