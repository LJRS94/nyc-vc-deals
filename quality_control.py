"""
Unified Quality Control System for NYC VC Deal Scraper.

ALL data — regardless of source or type — must pass through the appropriate
validation gate before insertion:
  - validate_deal()              — deals
  - validate_portfolio_company() — portfolio companies
  - validate_firm()              — VC firms

Features:
  - Unified validation for company name, stage, amount, date, NYC status
  - Smart deduplication (allows multi-round, blocks true duplicates)
  - Rejection logging for self-improvement
  - Post-ingestion audit that flags suspicious data (deals, portfolio, firms)
  - Quality metrics tracking over time
  - Cleanup functions to fix/remove junk entries
"""

import re
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

from config import MAX_COMPANY_NAME_LENGTH, DEDUP_DATE_GAP_DAYS
from scrapers.utils import (
    normalize_company_name, normalize_stage, classify_stage_from_amount,
    validate_deal_amount, is_duplicate_deal, company_names_match,
    should_skip_deal, classify_sector, ensure_full_date,
)
from scrapers.llm_extract import validate_company_name, clean_company_name

logger = logging.getLogger(__name__)

# ── Schema for QC tables ────────────────────────────────────────

QC_SCHEMA = """
CREATE TABLE IF NOT EXISTS qc_rejections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT,
    reason TEXT NOT NULL,
    source_type TEXT,
    raw_data TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_qc_reason ON qc_rejections(reason);

CREATE TABLE IF NOT EXISTS qc_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date DATE NOT NULL,
    source_type TEXT,
    deals_submitted INTEGER DEFAULT 0,
    deals_accepted INTEGER DEFAULT 0,
    deals_rejected INTEGER DEFAULT 0,
    rejection_reasons TEXT,  -- JSON: {"bad_name": 3, "duplicate": 5, ...}
    avg_confidence REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_qc_metrics_date ON qc_metrics(run_date);

CREATE TABLE IF NOT EXISTS qc_patterns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern_type TEXT NOT NULL,  -- 'bad_name', 'bad_amount', 'not_nyc', etc.
    pattern_value TEXT NOT NULL,
    hit_count INTEGER DEFAULT 1,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    auto_reject INTEGER DEFAULT 0,  -- 1 = auto-reject future matches
    UNIQUE(pattern_type, pattern_value)
);
"""


def init_qc_tables(conn):
    """Create QC tables if they don't exist, and migrate schema if needed."""
    conn.executescript(QC_SCHEMA)
    conn.commit()

    # Migration: add data_type column to existing tables
    _migrate_tables = ["qc_rejections", "qc_metrics", "qc_patterns"]
    for table in _migrate_tables:
        cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if "data_type" not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN data_type TEXT DEFAULT 'deal'")
            conn.commit()
            logger.info(f"Migrated {table}: added data_type column")


# ── Rejection Logging ───────────────────────────────────────────

def _log_rejection(conn, company_name: str, reason: str,
                   source_type: str = None, raw_data: str = None,
                   data_type: str = 'deal'):
    """Log a rejection for pattern analysis (atomic via savepoint)."""
    try:
        conn.execute("SAVEPOINT rejection_log")
        conn.execute(
            "INSERT INTO qc_rejections (company_name, reason, source_type, raw_data, data_type) "
            "VALUES (?, ?, ?, ?, ?)",
            (company_name, reason, source_type, (raw_data or "")[:500], data_type)
        )
        # Update pattern tracker
        pattern_value = _extract_pattern(company_name, reason)
        if pattern_value:
            conn.execute(
                "INSERT INTO qc_patterns (pattern_type, pattern_value, hit_count, last_seen, data_type) "
                "VALUES (?, ?, 1, CURRENT_TIMESTAMP, ?) "
                "ON CONFLICT(pattern_type, pattern_value) DO UPDATE SET "
                "hit_count = hit_count + 1, last_seen = CURRENT_TIMESTAMP",
                (reason, pattern_value, data_type)
            )
        conn.execute("RELEASE rejection_log")
    except Exception as e:
        conn.execute("ROLLBACK TO rejection_log")
        logging.getLogger(__name__).warning(f"Rejection log failed: {e}")


def _extract_pattern(company_name: str, reason: str) -> Optional[str]:
    """Extract a reusable pattern from a rejection for learning."""
    if not company_name:
        return None
    if reason == "bad_name_headline":
        # Extract the headline prefix pattern
        words = company_name.split()
        if len(words) >= 2:
            return " ".join(words[:2]).lower()
    if reason == "bad_name_too_long":
        return f"len>{len(company_name)}"
    if reason == "vc_firm":
        return normalize_company_name(company_name)
    return normalize_company_name(company_name)[:20] if company_name else None


# ── The Single Quality Gate ─────────────────────────────────────

_BAD_NAME_PATTERNS = [
    re.compile(r"^(this|that|a|an|my|our|some)\s", re.I),
    re.compile(r"\b(startup|company|firm|platform|tool|app|service|solution|product|system)\s*$", re.I),
    re.compile(r"^(top|breaking|exclusive|report|update|exploring|roundup)", re.I),
    re.compile(r"\b(said|told|announced|reported|raised|raises)\b", re.I),
    re.compile(r"[?!:]"),  # headlines have punctuation
    re.compile(r"[\u2019']s\s+(new|latest|next|first|big|recent)\b", re.I),  # possessive + adjective = headline
    re.compile(r"^\d+\s"),  # starts with number (street address or list)
    re.compile(r"^[\$']"),  # starts with $ or quote
    # Scraped metadata: "M Series B PREDICTION MARKETS Novig", "M BLOCKCHAIN Prometheum"
    re.compile(r"^M\s+(?:Series [A-E]|Pre-Seed|Seed|Venture|Growth)?\s*[A-Z]{2,}", re.I),
    # Location prefix: "Boston's Ginkgo Bioworks"
    re.compile(r"^(?:Boston|NYC|New York|SF|Chicago|LA|London|Berlin|Paris)(?:'s?\s)", re.I),
    # Person description instead of company: "Former Tesla exec"
    re.compile(r"^(?:Former|Ex-|Current|Longtime)\s+\w+\s+(?:exec|CEO|CTO|founder|employee)", re.I),
]


def validate_deal(conn, company_name: str, stage: str = "Unknown",
                  amount: float = None, date_announced: str = None,
                  source_type: str = "other", description: str = None,
                  is_nyc: bool = None, raw_text: str = None,
                  city: str = None,
                  **kwargs) -> Tuple[bool, str, Dict]:
    """
    THE single quality gate. ALL ingestion paths must call this.

    Returns:
        (accepted: bool, reason: str, cleaned_data: dict)

    cleaned_data contains normalized/cleaned versions of all fields:
        company_name, company_name_normalized, stage, amount_usd,
        date_announced, confidence_score, source_type
    """
    # ── 1. Company Name Validation ──
    if not company_name or len(company_name.strip()) < 2:
        _log_rejection(conn, company_name, "bad_name_empty", source_type, raw_text)
        return False, "empty_name", {}

    # Clean the name
    company_name = clean_company_name(company_name.strip())
    if not company_name or len(company_name.strip()) < 2:
        _log_rejection(conn, company_name, "bad_name_empty", source_type, raw_text)
        return False, "empty_name", {}

    # Length check
    if len(company_name) > MAX_COMPANY_NAME_LENGTH:
        _log_rejection(conn, company_name, "bad_name_too_long", source_type, raw_text)
        return False, "name_too_long", {}

    # Validate using the shared validator (checks verbs, headlines, etc.)
    if not validate_company_name(company_name):
        _log_rejection(conn, company_name, "bad_name_headline", source_type, raw_text)
        return False, "bad_name", {}

    # Additional bad name patterns
    for pattern in _BAD_NAME_PATTERNS:
        if pattern.search(company_name):
            _log_rejection(conn, company_name, "bad_name_pattern", source_type, raw_text)
            return False, "bad_name_pattern", {}

    # Check learned auto-reject patterns
    norm = normalize_company_name(company_name)
    auto_reject = conn.execute(
        "SELECT pattern_value FROM qc_patterns "
        "WHERE auto_reject = 1 AND pattern_type IN ('bad_name_headline', 'bad_name_pattern', 'vc_firm') "
        "AND pattern_value = ?",
        (norm[:20],)
    ).fetchone()
    if auto_reject:
        _log_rejection(conn, company_name, "auto_reject_pattern", source_type, raw_text)
        return False, "auto_reject", {}

    # ── 2. VC Firm Check ──
    skip = should_skip_deal(conn, company_name, amount)
    if skip:
        _log_rejection(conn, company_name, "vc_firm", source_type, raw_text)
        return False, f"vc_firm: {skip}", {}

    # ── 3. Stage Normalization & Validation ──
    if stage and stage not in ("Pre-Seed", "Seed", "Series A", "Series B", "Series C+", "Unknown"):
        stage = normalize_stage(stage)

    # Amount-based fallback for Unknown stage
    if stage == "Unknown" and amount:
        stage = classify_stage_from_amount(amount)

    # ── 4. Amount Validation ──
    if amount is not None:
        if amount <= 0:
            amount = None  # treat as undisclosed rather than rejecting
        elif not validate_deal_amount(amount, stage):
            _log_rejection(conn, company_name, "bad_amount",
                           source_type, f"amount={amount} stage={stage}")
            amount = None  # keep the deal but drop the bad amount

    # ── 5. Date Validation ──
    if date_announced:
        # Fix partial YYYY-MM dates by appending -01
        fixed = ensure_full_date(date_announced)
        if fixed:
            date_announced = fixed
        try:
            d = datetime.strptime(date_announced, "%Y-%m-%d")
            # Reject dates more than 1 year in the future
            if d > datetime.now() + timedelta(days=365):
                date_announced = None
            # Reject dates before 2000
            if d.year < 2000:
                date_announced = None
        except (ValueError, TypeError):
            date_announced = None

    # ── 6. Smart Deduplication ──
    if is_duplicate_deal(conn, company_name, stage, amount, date_announced):
        _log_rejection(conn, company_name, "duplicate", source_type,
                       f"stage={stage} amount={amount} date={date_announced}")
        return False, "duplicate", {}

    # ── 7. Confidence Scoring ──
    confidence = _compute_confidence(
        company_name=company_name,
        stage=stage,
        amount=amount,
        date_announced=date_announced,
        source_type=source_type,
        description=description,
        is_nyc=is_nyc,
        source_url=kwargs.get("source_url"),
        raw_text=raw_text,
    )

    # ── 8. Build cleaned data ──
    # Resolve city: explicit param > fallback from is_nyc flag
    resolved_city = city
    if not resolved_city and is_nyc:
        resolved_city = "New York"

    cleaned = {
        "company_name": company_name,
        "company_name_normalized": normalize_company_name(company_name),
        "stage": stage or "Unknown",
        "amount_usd": amount,
        "amount_disclosed": 1 if amount else 0,
        "date_announced": date_announced,
        "source_type": source_type,
        "confidence_score": confidence,
    }
    if resolved_city:
        cleaned["city"] = resolved_city
    # Pass through optional fields
    if description:
        cleaned["company_description"] = description
    if raw_text:
        cleaned["raw_text"] = raw_text[:2000]
    for k in ("company_website", "source_url", "category_id", "subcategory"):
        if k in kwargs and kwargs[k] is not None:
            cleaned[k] = kwargs[k]

    # ── 9. Provenance warning (non-blocking) ──
    if not kwargs.get("source_url"):
        logger.warning(f"Deal '{company_name}' accepted without source_url (source={source_type})")
    if not raw_text:
        logger.warning(f"Deal '{company_name}' accepted without raw_text (source={source_type})")

    return True, "accepted", cleaned


def _compute_confidence(company_name: str, stage: str, amount: float,
                        date_announced: str, source_type: str,
                        description: str, is_nyc: bool,
                        source_url: str = None, raw_text: str = None) -> float:
    """
    Unified confidence scoring.
    Source reliability + data completeness + provenance + NYC confirmation.
    """
    # Base by source reliability
    source_scores = {
        "crunchbase": 0.85,
        "pitchbook": 0.85,
        "press_release": 0.75,
        "news_article": 0.75,
        "sec_filing": 0.75,
        "alleywatch": 0.70,
        "alleywatch_roundup": 0.60,
        "firm_website": 0.60,
        "google_news": 0.55,
        "de_filing": 0.50,
        "other": 0.45,
    }
    base = source_scores.get(source_type, 0.45)

    # Completeness bonuses/penalties
    if amount:
        base += 0.05
    else:
        base -= 0.10
    if stage and stage != "Unknown":
        base += 0.05
    else:
        base -= 0.05
    if date_announced:
        base += 0.02
    if description and len(description) > 20:
        base += 0.03
    if is_nyc:
        base += 0.05

    # Provenance penalty — deals without source_url/raw_text are harder to verify
    if not source_url:
        base -= 0.10
    if not raw_text:
        base -= 0.05

    return max(0.1, min(1.0, round(base, 2)))


# ── Post-Ingestion Audit ────────────────────────────────────────

def run_audit(conn) -> Dict:
    """
    Run quality audit on all existing deals.
    Returns dict of issues found, grouped by type.
    """
    issues = {
        "duplicate_companies": [],
        "bad_names": [],
        "stage_amount_mismatch": [],
        "missing_critical": [],
        "stale_data": [],
        "low_confidence": [],
    }

    # 1. Find remaining duplicates
    rows = conn.execute("""
        SELECT company_name_normalized, GROUP_CONCAT(id) as ids,
               GROUP_CONCAT(stage, '|') as stages,
               GROUP_CONCAT(COALESCE(date_announced,'?'), '|') as dates,
               COUNT(*) as cnt
        FROM deals
        GROUP BY company_name_normalized
        HAVING COUNT(*) > 1
    """).fetchall()
    for r in rows:
        ids = r[1].split(",")
        stages = r[2].split("|")
        dates = r[3].split("|")
        # Check if these could be true duplicates (same stage + close dates)
        if len(set(stages)) == 1 and len(set(dates)) == 1:
            issues["duplicate_companies"].append({
                "normalized": r[0], "ids": ids,
                "stages": stages, "dates": dates,
            })

    # 2. Bad company names still in DB
    for row in conn.execute("SELECT id, company_name FROM deals").fetchall():
        name = row[1]
        if not validate_company_name(name):
            issues["bad_names"].append({"id": row[0], "name": name})
        elif len(name) > MAX_COMPANY_NAME_LENGTH:
            issues["bad_names"].append({"id": row[0], "name": name, "reason": "too_long"})

    # 3. Stage vs amount mismatches
    for row in conn.execute(
        "SELECT id, company_name, stage, amount_usd FROM deals "
        "WHERE amount_usd IS NOT NULL AND stage != 'Unknown'"
    ).fetchall():
        expected = classify_stage_from_amount(row[3])
        actual = row[2]
        # Flag if inferred stage is 2+ levels off from actual
        stage_order = {"Pre-Seed": 0, "Seed": 1, "Series A": 2, "Series B": 3, "Series C+": 4}
        diff = abs(stage_order.get(expected, -1) - stage_order.get(actual, -1))
        if diff >= 2:
            issues["stage_amount_mismatch"].append({
                "id": row[0], "name": row[1],
                "stage": actual, "amount": row[3],
                "expected_stage": expected,
            })

    # 4. Missing critical data
    for row in conn.execute(
        "SELECT id, company_name, stage, amount_usd, date_announced, confidence_score "
        "FROM deals WHERE amount_usd IS NULL AND stage = 'Unknown'"
    ).fetchall():
        issues["missing_critical"].append({
            "id": row[0], "name": row[1], "confidence": row[5],
        })

    # 5. Low confidence deals
    for row in conn.execute(
        "SELECT id, company_name, confidence_score, source_type "
        "FROM deals WHERE confidence_score < 0.5"
    ).fetchall():
        issues["low_confidence"].append({
            "id": row[0], "name": row[1],
            "confidence": row[2], "source": row[3],
        })

    # Summarize
    total_issues = sum(len(v) for v in issues.values())
    total_deals = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]

    return {
        "total_deals": total_deals,
        "total_issues": total_issues,
        "health_score": round(1 - (total_issues / max(total_deals, 1)), 2),
        "issues": issues,
    }


# ── Self-Improvement: Pattern Learning ──────────────────────────

def update_auto_reject_patterns(conn, min_hits: int = 5):
    """
    Promote frequently-rejected patterns to auto-reject.
    A pattern that's been rejected 5+ times is clearly junk.
    """
    updated = conn.execute(
        "UPDATE qc_patterns SET auto_reject = 1 "
        "WHERE hit_count >= ? AND auto_reject = 0",
        (min_hits,)
    ).rowcount
    if updated:
        conn.commit()
        logger.info(f"Auto-reject: promoted {updated} patterns (>={min_hits} hits)")
    return updated


def get_rejection_summary(conn, days: int = 30, data_type: str = None) -> Dict:
    """Get rejection stats for the last N days, optionally filtered by data_type."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    if data_type:
        rows = conn.execute(
            "SELECT reason, COUNT(*) as cnt FROM qc_rejections "
            "WHERE created_at >= ? AND data_type = ? GROUP BY reason ORDER BY cnt DESC",
            (cutoff, data_type)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT reason, COUNT(*) as cnt FROM qc_rejections "
            "WHERE created_at >= ? GROUP BY reason ORDER BY cnt DESC",
            (cutoff,)
        ).fetchall()
    return {r[0]: r[1] for r in rows}


def merge_cross_source_duplicates(conn) -> int:
    """
    Post-scrape dedup pass: find deals with the same normalized company name
    that are likely the same funding round. Works across sources AND within
    the same source. Handles the common case where the same deal gets
    different stage labels from different scrapers.

    Grouping: by company_name_normalized (ignoring stage).
    Within each group, cluster deals that look like the same round based on
    amount similarity and date proximity. Delete duplicates within each cluster.

    Returns number of duplicates removed.
    """
    from scrapers.utils import _dates_close, _amounts_similar

    # Get all companies with multiple deals
    groups = conn.execute("""
        SELECT company_name_normalized, COUNT(*) as cnt
        FROM deals
        WHERE company_name_normalized IS NOT NULL
          AND company_name_normalized != ''
        GROUP BY company_name_normalized
        HAVING COUNT(*) > 1
    """).fetchall()

    merged = 0
    for g in groups:
        norm = g["company_name_normalized"]
        deals = conn.execute(
            "SELECT id, stage, amount_usd, date_announced, source_type, "
            "confidence_score, company_website, company_description, city "
            "FROM deals WHERE company_name_normalized = ? ORDER BY id",
            (norm,)
        ).fetchall()
        deals = [dict(d) for d in deals]

        # Cluster deals into rounds: two deals are in the same cluster if
        # they have similar amounts AND close dates (or missing data makes
        # it ambiguous). Different amounts + far dates = different rounds.
        clusters = []  # list of lists of deal dicts
        for deal in deals:
            placed = False
            for cluster in clusters:
                rep = cluster[0]  # representative deal for cluster
                dc = _dates_close(deal["date_announced"], rep["date_announced"])
                ams = _amounts_similar(deal["amount_usd"], rep["amount_usd"])

                # Same amount + close dates → same round
                if ams is True and dc is not False:
                    cluster.append(deal)
                    placed = True
                    break
                # Close dates + no amount data → same round (if no contradicting signal)
                if dc is True and ams is None:
                    cluster.append(deal)
                    placed = True
                    break
                # Same amount + no date data → same round
                if ams is True and dc is None:
                    cluster.append(deal)
                    placed = True
                    break
                # Both have no amount AND close/no dates → assume duplicate
                if ams is None and (dc is True or dc is None):
                    # Only merge if stages are compatible
                    stages = {deal["stage"], rep["stage"]}
                    if len(stages - {"Unknown"}) <= 1:
                        cluster.append(deal)
                        placed = True
                        break

            if not placed:
                clusters.append([deal])

        # For each cluster with >1 deal, merge into the best one
        for cluster in clusters:
            if len(cluster) < 2:
                continue

            # Pick keeper: highest confidence → largest amount → has website → has description
            def score(d):
                return (
                    d["confidence_score"] or 0,
                    d["amount_usd"] or 0,
                    1 if d.get("company_website") else 0,
                    1 if d.get("company_description") else 0,
                )
            cluster.sort(key=score, reverse=True)
            keeper = cluster[0]
            losers = cluster[1:]

            keeper_id = keeper["id"]
            loser_ids = [d["id"] for d in losers]
            ph = ",".join(["?"] * len(loser_ids))

            # Merge investor/firm links from losers to keeper
            conn.execute(
                f"UPDATE OR IGNORE deal_firms SET deal_id = ? WHERE deal_id IN ({ph})",
                [keeper_id] + loser_ids,
            )
            conn.execute(
                f"UPDATE OR IGNORE deal_investors SET deal_id = ? WHERE deal_id IN ({ph})",
                [keeper_id] + loser_ids,
            )

            # Fill gaps in keeper from losers
            if not keeper["amount_usd"]:
                for loser in losers:
                    if loser["amount_usd"]:
                        conn.execute(
                            "UPDATE deals SET amount_usd = ?, amount_disclosed = 1 WHERE id = ?",
                            (loser["amount_usd"], keeper_id),
                        )
                        break
            if not keeper.get("company_website"):
                for loser in losers:
                    if loser.get("company_website"):
                        conn.execute(
                            "UPDATE deals SET company_website = ? WHERE id = ?",
                            (loser["company_website"], keeper_id),
                        )
                        break
            if not keeper.get("company_description"):
                for loser in losers:
                    if loser.get("company_description"):
                        conn.execute(
                            "UPDATE deals SET company_description = ? WHERE id = ?",
                            (loser["company_description"], keeper_id),
                        )
                        break

            # Clean up orphaned links then delete losers
            conn.execute(f"DELETE FROM deal_firms WHERE deal_id IN ({ph})", loser_ids)
            conn.execute(f"DELETE FROM deal_investors WHERE deal_id IN ({ph})", loser_ids)
            conn.execute(f"DELETE FROM deal_metadata WHERE deal_id IN ({ph})", loser_ids)
            conn.execute(f"DELETE FROM deals WHERE id IN ({ph})", loser_ids)
            merged += len(loser_ids)

    if merged:
        conn.commit()
        logger.info(f"Dedup pass: merged {merged} duplicate deals")
    return merged


def record_metrics(conn, source_type: str, submitted: int,
                   accepted: int, rejections: Dict, avg_confidence: float,
                   data_type: str = 'deal'):
    """Record quality metrics for a scrape run."""
    conn.execute(
        "INSERT INTO qc_metrics (run_date, source_type, deals_submitted, "
        "deals_accepted, deals_rejected, rejection_reasons, avg_confidence, data_type) "
        "VALUES (date('now'), ?, ?, ?, ?, ?, ?, ?)",
        (source_type, submitted, accepted, submitted - accepted,
         json.dumps(rejections), avg_confidence, data_type)
    )
    conn.commit()


# ── Portfolio Company Junk Patterns ────────────────────────────
# Moved from scrapers/firm_scraper.py for centralized QC

_JUNK_PORTFOLIO_RE = re.compile(
    r"^(GET IN TOUCH|Go-To-Market|View All|Load More|Show More|"
    r"Learn More|Read More|Visit Website|Visit Site|Back to Top|Contact Us|"
    r"About Us|About\b.*|Our Team|Our Portfolio|Our Startups|Our Mission|See All|See More|Subscribe|"
    r"Sign Up|Log In|Login|Sign In|Privacy Policy|Privacy|Privacy Center|"
    r"Terms of Service|Terms of Use|Terms|Cookie Policy|Cookie Settings|"
    r"Filter|Sort|Search|Menu|Close|Open|All Companies|All|"
    r"Current Portfolio|Previous Portfolio|Active|Exited|"
    r"Limited Partner Login|Investor Portal|LP Portal|LP Log-In|Investor Login|"
    r"For Investors|For Founders|For LPs|For LP's|How We Invest|"
    r"Series [a-e]|Series [A-E]|Pre-Seed|Seed|IPO|"
    r"Gaming|Health|Health Tech|Consumer|Finance|Media|Software|Education|"
    r"Marketplace|Other|Resources|News|Blog|Press|Insights|Newsletter|"
    r"Team|Studio|LinkedIn|Twitter|Facebook|Instagram|Podcast|"
    r"FAQs?|Reset|Apply|Cancel|Submit|Back|Next|Previous|More|Less|"
    r"Fundraising|Founder Services|Investments|Partners|Network|"
    r"Careers|Events|Overview|Contact|Home|Stage|Spotlight|"
    r"Trending topics|Disclosures|Featured|Enterprise|Commerce|"
    r"Crypto|Robotics|Space|Hardware|Fintech|Cybersecurity|"
    r"AI Apps|AI Infrastructure & Developer Platforms|"
    r"Data, AI & Machine Learning|Energy & Infrastructure|"
    r"Enterprise Apps & Vertical AI|Infrastructure & Developer Tools|"
    r"Jobs|Portfolio Jobs|Digital Health|Life Sciences|"
    r"Loading\.\.\.)$",
    re.I,
)

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

# Category tokens for detecting category-only concatenations
_CATEGORY_TOKENS = {
    'AI', 'CONSUMER', 'Consumer', 'Fintech', 'Healthcare', 'Enterprise',
    'Saas', 'SaaS', 'Hardware', 'Robotics', 'Space', 'Media', 'Commerce',
    'Brands', 'Strategy', 'Featured', 'PropTech', 'Social', 'Crypto',
    'Climate', 'Security', 'Infrastructure', 'Logistics', 'Gaming', 'Education',
}


def is_valid_portfolio_name(name: str) -> bool:
    """Return True if name looks like a real company name, not UI junk.

    Public API — used by both quality_control and firm_scraper.
    """
    if not name or len(name.strip()) < 2:
        return False
    name = name.strip()
    # Strip invisible unicode chars (zero-width joiners, etc.)
    name = re.sub(r'[\u200b-\u200f\u2028-\u202f\u2060\ufeff]', '', name)
    if len(name) > 60:
        return False
    if re.match(r"^\d{4}$", name):
        return False
    if re.match(r"^\d+$", name):
        return False
    # "Cognition-Developer of AI-powered coding assistant, Devin" pattern
    if re.search(r'-(?:Developer|Creator|Maker|Builder|Provider)\s+of\s', name):
        return False
    if _JUNK_PORTFOLIO_RE.match(name):
        return False
    if _JUNK_ANYWHERE_RE.search(name):
        return False
    if _JUNK_CONTENT_RE.search(name):
        return False

    name_lower = name.lower()

    # ── Exit/acquisition status labels (not company names) ──
    # "Acquired by X", "Exited to X", "IPOVentures", "AcquiredStudio"
    # Protect real companies like "Acquire.com", "Acquire", "Acquired.io"
    if re.match(r'^Acquired\s+by\s', name):
        return False
    if re.match(r'^Exited(\s+to\s|\s+Investments$)', name):
        return False
    if re.match(r'^(Acquired|IPO)[A-Z][a-z]', name) and len(name) < 25:
        return False
    if name == "Acquired":
        return False

    # ── Filter/UI state artifacts ──
    # "NYCALLother-falseConsumerALL", "Bay AreaALLother-false..."
    if 'other-' in name_lower:
        return False
    if re.search(r'[a-z]ALL[A-Z]|[A-Z]ALL[a-z]|ALL$', name) and len(name) > 10:
        return False
    # "StatusAll", "StatusLive", "StatusExited", "StatusCurrent"
    if re.match(r'^Status[A-Z]', name):
        return False
    # "ConsumerCurrent", "ConsumerExited", "EnterpriseCurrent", "EnterpriseExited"
    if re.match(r'^(Consumer|Enterprise|AI|Fintech|Healthcare|Commerce)(Current|Exited|Active|Live)$', name):
        return False
    # "SectorData Analytics & Infrastructure", "SectorEducation & Training, SaaS"
    if re.match(r'^Sector[A-Z]', name):
        return False
    # "Investment Year2025", "Investment Year2024"
    if re.match(r'^Investment\s+Year\d{4}$', name):
        return False
    # "SpyceFoodAll" — company + category + All/Current/Exited
    if re.search(r'(Food|Finance|Consumer|Enterprise|Health|Media|Commerce|Marketplace)(All|Current|Exited)$', name) and len(name) > 8:
        return False

    # ── Country/geography filter labels ──
    if re.match(r'^Country[A-Z]', name):
        return False

    # ── Single category/sector words (not company names) ──
    _SINGLE_CATEGORY_WORDS = {
        'ai', 'biotech', 'food', 'saas', 'fintech', 'healthcare', 'edtech',
        'cleantech', 'proptech', 'insurtech', 'crypto', 'web3', 'gaming',
        'media', 'commerce', 'ecommerce', 'logistics', 'robotics', 'defense',
        'energy', 'agriculture', 'education', 'legal', 'sports', 'entertainment',
        'climate', 'security', 'infrastructure', 'data', 'platform', 'marketplace',
        'software', 'hardware', 'social', 'travel', 'fitness', 'beauty', 'fashion',
        'music', 'b2b', 'b2c', 'consumer', 'enterprise', 'current', 'explore',
    }
    if name_lower in _SINGLE_CATEGORY_WORDS:
        return False

    # ── Single geography words ──
    _GEO_ONLY = {'bay area', 'new york', 'san francisco', 'los angeles', 'boston',
                 'austin', 'seattle', 'london', 'global'}
    if name_lower in _GEO_ONLY:
        return False

    # ── Meta/nav text about the portfolio itself ──
    if re.match(r'^(Select investments|Showing results|Showcasing |View (Research|Portfolio|All)|'
                r'Click here|Explore Our|MetaProp Portfolio|Hypothesis Portfolio|'
                r'Launched in |Simple consumer |Current$|'
                r'Backed at |See all backed|Our Advisors|Our Culture|'
                r'Explore CTEK|Visit Our|Visit our|'
                r'Diverse perspectives|Tag Field|hello@|'
                r'@\w+|Info$|Industries$|Grants$|Theses$|Writing$|Connect$|'
                r'No Results Found|Privacy Settings|'
                r'\(c\) \d{4}|copyright \d{4})', name, re.I):
        return False
    # Email addresses
    if re.match(r'^[\w.+-]+@[\w.-]+\.\w+$', name):
        return False
    # Twitter handles as names
    if re.match(r'^@\w+', name):
        return False
    # Icon labels ("Twitter | X Icon", "LinkedIn Icon", "Youtube Icon")
    if re.search(r'\bIcon$', name):
        return False

    # ── Concatenated multi-stage labels ──
    if re.search(r'(Pre-Seed|Series [A-Ea-e]|Seed)(Pre-Seed|Series [A-Ea-e]|Seed|IPO)', name):
        return False
    if re.match(r'^Initial Investment', name):
        return False

    # ── Concatenated multi-category (3+ sectors mashed together) ──
    _SECTOR_TOKENS_FOR_CONCAT = ['AI', 'SaaS', 'Fintech', 'Healthcare', 'Commerce',
                                  'Security', 'Industrials', 'Digital', 'Infra']
    if sum(1 for s in _SECTOR_TOKENS_FOR_CONCAT if s in name) >= 3 and len(name) > 20:
        return False

    # ── Strategy + category concatenations (StrategySoftware, StrategySustainability) ──
    if re.match(r'^Strategy[A-Z]', name):
        return False

    # ── "Founders' X Portfolio" meta text ──
    if re.search(r"Founders'.*Portfolio|Portfolio Companies$", name):
        return False

    # Sentence-like patterns (descriptions scraped as names)
    # Ends with period — it's a sentence
    if name.endswith('.') and len(name) > 10:
        return False
    # "The age of agents", "The future of work" — article + noun + prep + noun
    if re.match(r'^The\s+\w+\s+of\s+\w+', name, re.I) and len(name) < 30:
        return False
    if len(name) > 40 and any(w in name_lower for w in
            [" is a ", " is an ", " provides ", " delivers ", " develops ",
             " offers ", " enables ", " builds ", " allows ", " revolutionizes ",
             " partnering ", " dedicated to ", " bringing ", " powered by "]):
        return False

    # Description-like sentences (5+ words with mostly lowercase, > 35 chars)
    if len(name) > 35:
        words = name.split()
        if len(words) >= 5:
            lower_words = [w for w in words[1:] if w[0:1].islower() or w in (
                'in', 'for', 'of', 'the', 'and', 'a', 'to', 'an', '&')]
            if len(lower_words) >= len(words) - 2:
                return False

    # Description-like prefix
    if re.match(r"^(AI-powered|AI-Native|An? investment|A specialty|An? AI|A platform|The leading|An? \w+ that)", name, re.I):
        return False
    # Long description ending in generic words
    if len(name) > 30 and re.search(r'\s+(Platform|Solution|Solutions|Automation|Optimization|Apps)$', name):
        return False

    # Concatenated metadata
    if re.search(r"(Consumer|Media|Health|Finance|Software|Education|Marketplace)\d{4}$", name):
        return False
    if re.search(r"Invested\d{4}$", name):
        return False
    # Category-only concatenations
    base = re.sub(r'\d{4}$', '', name).strip()
    remaining = base
    for cat in sorted(_CATEGORY_TOKENS, key=len, reverse=True):
        remaining = remaining.replace(cat, '')
    if len(remaining.replace('/', '').replace(' ', '').replace('&', '')) == 0 and len(base) > 3:
        return False
    if re.match(r'^AI\d{4}$', name):
        return False
    if re.match(r"^\(", name):
        return False
    if "Acq:" in name:
        return False
    if re.match(r'^(NASDAQ|NYSE)', name):
        return False
    if re.match(r'^(Design|Built|Made|Powered|Created) by ', name, re.I):
        return False
    # City + category concatenations
    if re.match(r'^(Austin|Boston|London|New York|San Francisco|Tel Aviv|Toronto|Washington)', name) and (',' in name or len(name) > 20):
        return False
    if re.match(r'^Filter', name) and len(name) > 10:
        return False

    # ── Firm name + stage (e.g. "Social Starts Series A") ──
    if re.search(r'\s+Series [A-E]$', name) and len(name) > 15:
        # Check if it's just "firm name + Series X" by seeing if removing the stage
        # leaves something very short or looks like a firm name
        base_name = re.sub(r'\s+Series [A-E]$', '', name)
        if len(base_name) < 5:
            return False

    # ── Long text with sentence markers ──
    if len(name) > 20 and re.search(r'Together\.|Startups,', name):
        return False

    # ── Long sentence-like names with "for" / "of" / "and" (descriptions) ──
    if len(name) > 30 and re.search(r'\b(for|of)\b.*\b(for|of)\b', name_lower):
        return False
    if len(name) > 35 and re.match(r'^[A-Z]', name) and re.search(r'\bfor\s+[A-Z]', name):
        # "AI System of Record for In-House Legal Teams" pattern
        words = name.split()
        if len(words) >= 5:
            return False

    # ── Firm name + stage suffix ──
    if re.search(r'\s+Series [A-E]$', name):
        # Real portfolio names don't normally end with just "Series X"
        # Companies like "Series" exist but not "Foo Series A"
        base_name = re.sub(r'\s+Series [A-E]$', '', name)
        # If there are 2+ words before "Series X", likely junk
        if len(base_name.split()) >= 2:
            return False

    return True


# ── Portfolio Company Quality Gate ─────────────────────────────

def validate_portfolio_company(conn, firm_id: int, company_name: str,
                               **kwargs) -> Tuple[bool, str, Dict]:
    """
    Quality gate for portfolio companies. Same pattern as validate_deal().

    Returns:
        (accepted: bool, reason: str, cleaned_data: dict)
    """
    source_type = kwargs.get("source_type", "firm_website")

    # 1. Empty / too short / too long
    if not company_name or len(company_name.strip()) < 2:
        _log_rejection(conn, company_name, "bad_name_empty", source_type, data_type='portfolio')
        return False, "empty_name", {}
    company_name = company_name.strip()
    if len(company_name) > 60:
        _log_rejection(conn, company_name, "bad_name_too_long", source_type, data_type='portfolio')
        return False, "name_too_long", {}

    # 2. Junk pattern check (nav, UI, metadata, descriptions)
    if not is_valid_portfolio_name(company_name):
        _log_rejection(conn, company_name, "junk_pattern", source_type, data_type='portfolio')
        return False, "junk_pattern", {}

    # 3. Clean ExitsTrue/ExitsFalse suffixes (fix, don't reject)
    cleaned_name = re.sub(r"Exits?(true|false)$", "", company_name).strip()
    if not cleaned_name or len(cleaned_name) < 2:
        _log_rejection(conn, company_name, "bad_name_empty_after_clean", source_type, data_type='portfolio')
        return False, "empty_name", {}

    # 4. Clean (Acquired)/(Exited) tags (fix, don't reject)
    for tag in ['(Acquired)', '(Exited)']:
        cleaned_name = cleaned_name.replace(tag, '').strip()
    if not cleaned_name or len(cleaned_name) < 2:
        _log_rejection(conn, company_name, "bad_name_empty_after_clean", source_type, data_type='portfolio')
        return False, "empty_name", {}

    # 5. Auto-reject learned patterns
    norm = normalize_company_name(cleaned_name)
    auto_reject = conn.execute(
        "SELECT pattern_value FROM qc_patterns "
        "WHERE auto_reject = 1 AND data_type = 'portfolio' AND pattern_value = ?",
        (norm[:20],)
    ).fetchone()
    if auto_reject:
        _log_rejection(conn, cleaned_name, "auto_reject_pattern", source_type, data_type='portfolio')
        return False, "auto_reject", {}

    # 6. Duplicate check (same firm_id + normalized name already exists)
    existing = conn.execute(
        "SELECT id FROM portfolio_companies "
        "WHERE firm_id = ? AND company_name_normalized = ?",
        (firm_id, norm)
    ).fetchone()
    if existing:
        # Not a rejection — just skip silently (upsert will handle)
        pass

    # 7. Build cleaned_data dict
    cleaned = {
        "company_name": cleaned_name,
        "company_name_normalized": norm,
    }
    for k in ("company_website", "description", "lead_partner", "sector", "source_url"):
        if k in kwargs and kwargs[k] is not None:
            cleaned[k] = kwargs[k]

    return True, "accepted", cleaned


# ── Firm Quality Gate ──────────────────────────────────────────

_BAD_FIRM_NAME_PATTERNS = [
    re.compile(r"^(and|the|a|an)\s", re.I),
    re.compile(r"\b(Powered by|Designed by|Built by|Made by)\b", re.I),
    re.compile(r"\b(Source|via|from)\s*:?\s*$", re.I),
]


def validate_firm(conn, name: str, website: str = None,
                  portfolio_url: str = None,
                  **kwargs) -> Tuple[bool, str, Dict]:
    """
    Quality gate for VC firms. Same pattern as validate_deal().

    Returns:
        (accepted: bool, reason: str, cleaned_data: dict)
    """
    # 1. Empty / too short
    if not name or len(name.strip()) < 2:
        _log_rejection(conn, name, "bad_name_empty", data_type='firm')
        return False, "empty_name", {}
    name = name.strip()

    # 2. Junk name patterns
    for pattern in _BAD_FIRM_NAME_PATTERNS:
        if pattern.search(name):
            _log_rejection(conn, name, "bad_name_pattern", data_type='firm')
            return False, "bad_name_pattern", {}

    # 3. URL format validation
    url_re = re.compile(r"^https?://[^\s]+$")
    if website and not url_re.match(website):
        website = None  # drop bad URL, don't reject
    if portfolio_url and not url_re.match(portfolio_url):
        portfolio_url = None

    # 4. Fuzzy duplicate detection
    norm = normalize_company_name(name)
    existing = conn.execute(
        "SELECT id, name FROM firms WHERE name = ? OR "
        "REPLACE(REPLACE(REPLACE(LOWER(name), ' ', ''), '.', ''), ',', '') = ?",
        (name, norm)
    ).fetchone()
    if existing:
        # Return existing firm_id in reason for caller to decide
        return False, f"duplicate_firm:{existing['id']}:{existing['name']}", {}

    # 5. Build cleaned_data dict
    cleaned = {"name": name}
    if website:
        cleaned["website"] = website
    if portfolio_url:
        cleaned["portfolio_url"] = portfolio_url
    for k in ("focus_stages", "focus_sectors"):
        if k in kwargs and kwargs[k] is not None:
            cleaned[k] = kwargs[k]

    return True, "accepted", cleaned


# ── Portfolio Audit ────────────────────────────────────────────

def run_audit_portfolio(conn) -> Dict:
    """
    Run quality audit on all portfolio companies.
    Returns dict of issues found, grouped by type.
    """
    issues = {
        "junk_names": [],
        "duplicates": [],
        "too_long": [],
        "orphans": [],
    }

    # 1. Junk names still in DB
    for row in conn.execute("SELECT id, company_name, firm_id FROM portfolio_companies").fetchall():
        name = row["company_name"] if isinstance(row, dict) else row[1]
        rid = row["id"] if isinstance(row, dict) else row[0]
        firm_id = row["firm_id"] if isinstance(row, dict) else row[2]
        if not is_valid_portfolio_name(name):
            issues["junk_names"].append({"id": rid, "name": name, "firm_id": firm_id})
        elif len(name) > 60:
            issues["too_long"].append({"id": rid, "name": name, "firm_id": firm_id})

    # 2. Duplicates within the same firm
    rows = conn.execute("""
        SELECT firm_id, company_name_normalized, GROUP_CONCAT(id) as ids, COUNT(*) as cnt
        FROM portfolio_companies
        WHERE company_name_normalized IS NOT NULL AND company_name_normalized != ''
        GROUP BY firm_id, company_name_normalized
        HAVING COUNT(*) > 1
    """).fetchall()
    for r in rows:
        issues["duplicates"].append({
            "firm_id": r[0], "normalized": r[1],
            "ids": r[2].split(","), "count": r[3],
        })

    # 3. Orphans (firm_id references non-existent firm)
    orphans = conn.execute("""
        SELECT pc.id, pc.company_name, pc.firm_id
        FROM portfolio_companies pc
        LEFT JOIN firms f ON pc.firm_id = f.id
        WHERE f.id IS NULL
    """).fetchall()
    for r in orphans:
        issues["orphans"].append({"id": r[0], "name": r[1], "firm_id": r[2]})

    total_issues = sum(len(v) for v in issues.values())
    total = conn.execute("SELECT COUNT(*) FROM portfolio_companies").fetchone()[0]

    return {
        "total_portfolio_companies": total,
        "total_issues": total_issues,
        "health_score": round(1 - (total_issues / max(total, 1)), 2),
        "issues": issues,
    }


def run_audit_firms(conn) -> Dict:
    """
    Run quality audit on all firms.
    Returns dict of issues found, grouped by type.
    """
    issues = {
        "junk_names": [],
        "duplicates": [],
        "missing_website": [],
        "orphans": [],
    }

    # 1. Junk firm names
    for row in conn.execute("SELECT id, name, website FROM firms").fetchall():
        name = row["name"] if isinstance(row, dict) else row[1]
        rid = row["id"] if isinstance(row, dict) else row[0]
        if not name or len(name.strip()) < 2:
            issues["junk_names"].append({"id": rid, "name": name})
        else:
            for p in _BAD_FIRM_NAME_PATTERNS:
                if p.search(name):
                    issues["junk_names"].append({"id": rid, "name": name})
                    break

    # 2. Duplicate firms by normalized name
    rows = conn.execute("""
        SELECT REPLACE(REPLACE(REPLACE(LOWER(name), ' ', ''), '.', ''), ',', '') as norm,
               GROUP_CONCAT(id) as ids, GROUP_CONCAT(name, '|') as names, COUNT(*) as cnt
        FROM firms
        GROUP BY norm
        HAVING COUNT(*) > 1
    """).fetchall()
    for r in rows:
        issues["duplicates"].append({
            "normalized": r[0], "ids": r[1].split(","),
            "names": r[2].split("|"), "count": r[3],
        })

    # 3. Missing website
    for row in conn.execute(
        "SELECT id, name FROM firms WHERE website IS NULL OR TRIM(website) = ''"
    ).fetchall():
        issues["missing_website"].append({"id": row[0], "name": row[1]})

    # 4. Orphan firms (no deals, no portfolio companies)
    orphans = conn.execute("""
        SELECT f.id, f.name FROM firms f
        LEFT JOIN deal_firms df ON f.id = df.firm_id
        LEFT JOIN portfolio_companies pc ON f.id = pc.firm_id
        WHERE df.firm_id IS NULL AND pc.firm_id IS NULL
    """).fetchall()
    for r in orphans:
        issues["orphans"].append({"id": r[0], "name": r[1]})

    total_issues = sum(len(v) for v in issues.values())
    total = conn.execute("SELECT COUNT(*) FROM firms").fetchone()[0]

    return {
        "total_firms": total,
        "total_issues": total_issues,
        "health_score": round(1 - (total_issues / max(total, 1)), 2),
        "issues": issues,
    }


def run_audit_all(conn) -> Dict:
    """Run quality audit across all data types. Returns combined dict."""
    deals = run_audit(conn)
    portfolio = run_audit_portfolio(conn)
    firms = run_audit_firms(conn)
    return {
        "deals": deals,
        "portfolio": portfolio,
        "firms": firms,
    }


# ── Cleanup Functions ──────────────────────────────────────────

def clean_portfolio_companies(conn) -> int:
    """
    Delete/fix junk portfolio company entries.
    Consolidates all the SQL from api_server._run_data_cleanup().
    Returns number of entries removed/fixed.
    """
    from database import _normalize_name
    pc_removed = 0

    # 1. SQL-based junk conditions
    junk_conditions = [
        "company_name GLOB '[12][0-9][0-9][0-9]' AND LENGTH(company_name) = 4",
        "company_name LIKE '%Founder(s)%'",
        "company_name LIKE '%Partner Since%'",
        "company_name LIKE '%Exit' AND LENGTH(company_name) < 30",
        "LENGTH(company_name) > 60",
        "company_name LIKE '%DISCLAIMER%'",
        "company_name LIKE 'Country:%'",
        "company_name LIKE 'CountryUS%'",
        "company_name LIKE 'Investment Status:%'",
        "company_name LIKE 'Entry Stage:%'",
        "company_name LIKE 'Entry Year:%'",
        "company_name LIKE 'Industry:%'",
        "company_name LIKE 'Sector:%'",
        "company_name LIKE 'Year of Investment%'",
        "company_name LIKE '%Published on%'",
        "company_name LIKE '%Stage RTP%'",
        "company_name LIKE '%SectorFintech%'",
        "company_name LIKE '%SectorAI%'",
        "company_name LIKE '%SectorSaaS%'",
        "company_name LIKE '%SectorE-commerce%'",
        "company_name LIKE '%SectorAgriculture%'",
        "company_name LIKE '%CustomerB2%' AND LENGTH(company_name) > 50",
        "LENGTH(TRIM(company_name)) <= 1",
        "TRIM(company_name) = ''",
        "company_name LIKE 'Initial investment:%'",
        "company_name LIKE '%Status:Current%'",
        "company_name LIKE '%Status:Exited%'",
        "company_name LIKE '%StatusCurrent%'",
        "company_name LIKE '%StatusExited%'",
        "company_name LIKE '%AllMedia'",
        "company_name LIKE '%CommerceAll'",
        "company_name LIKE '%FinTechAll'",
        "company_name LIKE '%HealthcareAll'",
        "company_name LIKE '%EducationAll'",
        "company_name LIKE '%SaaSAll'",
        "company_name LIKE '%PropTechAll'",
        "company_name LIKE '%SocialAll'",
        "company_name LIKE '%AllHR'",
        "company_name LIKE '%AllPropTech'",
        "company_name LIKE '%AllSocial'",
        "company_name LIKE '%AllCommerce'",
        "company_name LIKE '%AllSaaS'",
        "company_name LIKE '%AllFinTech'",
        "company_name LIKE '%Link opens in new tab%'",
        "company_name LIKE 'Spotlight%' AND company_name LIKE '%:%'",
        "company_name LIKE 'Filter%' AND LENGTH(company_name) > 10",
        "company_name LIKE 'Austin, TX%' AND LENGTH(company_name) > 12",
        "company_name LIKE 'Boston, MA%' AND LENGTH(company_name) > 12",
        "company_name LIKE 'San Francisco, CA%' AND LENGTH(company_name) > 20",
        "company_name LIKE 'Tel Aviv, Israel%' AND LENGTH(company_name) > 18",
        "company_name LIKE 'New York, NY%' AND LENGTH(company_name) > 14",
        "company_name LIKE 'London, UK%' AND LENGTH(company_name) > 12",
        "company_name LIKE 'Toronto, Canada%' AND LENGTH(company_name) > 18",
        "company_name LIKE 'Washington DC%' AND LENGTH(company_name) > 15",
        "company_name LIKE 'NASDAQ%'",
        "company_name LIKE 'NYSE%'",
        "company_name LIKE '%Enterprise/Saas%'",
        "company_name IN ('COMPANY↑','PLAY VIDEO','VIEW LEGAL DISCLOSURES','BROWSE OUR—PORTFOLIO','NVP PROMISE','ENTERPRISE WEEKLY NEWSLETTER')",
    ]
    for cond in junk_conditions:
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM portfolio_companies WHERE " + cond
            ).fetchone()[0]
            if count > 0:
                conn.execute("DELETE FROM portfolio_companies WHERE " + cond)
                pc_removed += count
        except Exception:
            pass

    # 2. Nav/UI junk (exact match, case insensitive)
    nav_junk = [
        "GET IN TOUCH", "Go-To-Market Services", "View All", "Load More",
        "Show More", "Learn More", "Read More", "Visit Website", "Visit Site",
        "Contact Us", "About Us", "About", "Our Team", "Our Portfolio",
        "Our Startups", "Our Mission", "See All", "See More",
        "Privacy Policy", "Privacy", "Privacy Center", "Terms of Service",
        "Cookie Policy", "Cookie Settings", "Portfolio", "Subscribe",
        "Sign Up", "Log In", "Login", "Sign In", "Filter", "Active", "Exited",
        "Fundraising", "Founder Services", "Investments", "Partners",
        "Network", "AI Apps", "Cybersecurity", "Healthcare",
        "Data, AI & Machine Learning", "Enterprise Apps & Vertical AI",
        "Infrastructure & Developer Tools", "Commerce & Fintech",
        "Energy & Infrastructure", "AI Infrastructure & Developer Platforms",
        "All Companies", "All", "How We Invest", "For Investors",
        "For Founders", "For LPs", "For LP's", "Trending topics",
        "Disclosures", "Content", "Spotlight", "Stage", "Podcast",
        "Careers", "Events", "Overview", "Contact", "Home", "Resources",
        "News", "Blog", "Press", "Insights", "Featured", "Enterprise",
        "Commerce", "Crypto", "Robotics", "Space", "Hardware", "Fintech",
        "Loading...", "Canada",
    ]
    for junk_name in nav_junk:
        count = conn.execute(
            "SELECT COUNT(*) FROM portfolio_companies WHERE LOWER(TRIM(company_name)) = LOWER(?)",
            (junk_name,),
        ).fetchone()[0]
        if count > 0:
            conn.execute(
                "DELETE FROM portfolio_companies WHERE LOWER(TRIM(company_name)) = LOWER(?)",
                (junk_name,),
            )
            pc_removed += count

    # Commit after junk removal to release write lock
    if pc_removed:
        conn.commit()

    # 2b. Fix junk websites: "#close" (Bessemer), firm's own domain, bare domains
    # Bessemer: all websites are "#close" (modal close button scrape artifact)
    conn.execute(
        "UPDATE portfolio_companies SET company_website = NULL "
        "WHERE company_website = '#close'"
    )
    # Slow Ventures: websites pointing to firm's own domain
    conn.execute(
        "UPDATE portfolio_companies SET company_website = NULL "
        "WHERE company_website IN ('slow.co', 'https://www.slow.co', 'https://slow.co')"
    )
    # General Catalyst: websites pointing to firm's directory pages
    conn.execute(
        "UPDATE portfolio_companies SET company_website = NULL "
        "WHERE company_website LIKE '%generalcatalyst.com/companies/%'"
    )
    # Bain Capital: websites pointing to firm category pages
    conn.execute(
        "UPDATE portfolio_companies SET company_website = NULL "
        "WHERE company_website LIKE '%baincapitalventures.com/domain/%'"
    )
    # Bare domains without protocol — add https://
    bare_rows = conn.execute(
        "SELECT id, company_website FROM portfolio_companies "
        "WHERE company_website IS NOT NULL "
        "AND company_website NOT LIKE 'http%' "
        "AND company_website NOT LIKE '#%' "
        "AND company_website LIKE '%.%'"
    ).fetchall()
    for r in bare_rows:
        conn.execute(
            "UPDATE portfolio_companies SET company_website = ? WHERE id = ?",
            ("https://" + r["company_website"], r["id"]),
        )

    # 2c. Fix Great Oaks boolean concatenation: "CardlessFinancetrue" -> "Cardless"
    # Pattern: real_name + SectorCategory + true/false
    bool_rows = conn.execute(
        "SELECT id, company_name, firm_id FROM portfolio_companies "
        "WHERE company_name GLOB '*[a-z][A-Z]*true' OR company_name GLOB '*[a-z][A-Z]*false' "
        "OR company_name LIKE '%true' OR company_name LIKE '%false'"
    ).fetchall()
    _SECTORS_FOR_BOOL = {
        'Finance', 'Consumer', 'Marketplace', 'Artificial Intelligence',
        'Health', 'Healthcare', 'Enterprise', 'SaaS', 'AI', 'Fintech',
        'Commerce', 'Education', 'Media', 'Crypto', 'Climate', 'Security',
        'Food', 'Logistics', 'Robotics', 'Hardware', 'Software', 'Data',
        'Real Estate', 'Insurance', 'Legal', 'Sports', 'Gaming', 'Social',
    }
    for r in bool_rows:
        name = r["company_name"]
        # Try to strip sector+boolean suffix
        cleaned = re.sub(r'(' + '|'.join(re.escape(s) for s in _SECTORS_FOR_BOOL) + r')(true|false)$', '', name)
        if cleaned == name:
            # Fallback: strip just true/false if preceded by a category-like word
            cleaned = re.sub(r'[A-Z][a-z]+(true|false)$', '', name)
        if cleaned and cleaned != name and len(cleaned) >= 2:
            # Check if clean version already exists for this firm
            exists = conn.execute(
                "SELECT id FROM portfolio_companies WHERE firm_id = ? AND company_name = ?",
                (r["firm_id"], cleaned),
            ).fetchone()
            if exists:
                conn.execute("DELETE FROM portfolio_companies WHERE id = ?", (r["id"],))
            else:
                conn.execute(
                    "UPDATE portfolio_companies SET company_name = ?, company_name_normalized = ? WHERE id = ?",
                    (cleaned, _normalize_name(cleaned), r["id"]),
                )
            pc_removed += 1

    # Commit website + boolean fixes
    conn.commit()

    # 2d. Fix junk descriptions
    # SignalFire: "Exit" as description (status label, not description)
    conn.execute(
        "UPDATE portfolio_companies SET description = NULL "
        "WHERE TRIM(description) IN ('Exit', 'Sector', 'exit')"
    )
    # M13: concatenated field labels as descriptions
    conn.execute(
        "UPDATE portfolio_companies SET description = NULL "
        "WHERE description LIKE 'Initial investment:%'"
    )

    # 2e. Fix junk sectors (partner names, fund stages, concatenated junk)
    conn.execute(
        "UPDATE portfolio_companies SET sector = NULL "
        "WHERE sector LIKE '%Pre-Seed%' OR sector LIKE '%Partnered%' "
        "OR LENGTH(sector) > 60"
    )

    # 2f. Fix junk lead_partner values
    conn.execute(
        "UPDATE portfolio_companies SET lead_partner = NULL "
        "WHERE lead_partner LIKE 'Partnered%'"
    )

    # 2g. Regenerate missing normalized names
    missing_norm = conn.execute(
        "SELECT id, company_name FROM portfolio_companies "
        "WHERE company_name_normalized IS NULL OR company_name_normalized = ''"
    ).fetchall()
    for r in missing_norm:
        conn.execute(
            "UPDATE portfolio_companies SET company_name_normalized = ? WHERE id = ?",
            (_normalize_name(r["company_name"]), r["id"]),
        )

    # Commit description/sector/lead_partner/normalized name fixes
    conn.commit()

    # 3. Fix "ExitsTrue/ExitsFalse" suffixes
    exits_rows = conn.execute(
        "SELECT id, company_name, firm_id FROM portfolio_companies "
        "WHERE company_name LIKE '%Exitstrue' OR company_name LIKE '%Exitsfalse'"
    ).fetchall()
    for r in exits_rows:
        cleaned = re.sub(r"Exits?(true|false)$", "", r["company_name"]).strip()
        if cleaned and len(cleaned) > 1:
            exists = conn.execute(
                "SELECT id FROM portfolio_companies WHERE firm_id = ? AND company_name = ?",
                (r["firm_id"], cleaned),
            ).fetchone()
            if exists:
                conn.execute("DELETE FROM portfolio_companies WHERE id = ?", (r["id"],))
            else:
                conn.execute(
                    "UPDATE portfolio_companies SET company_name = ?, company_name_normalized = ? WHERE id = ?",
                    (cleaned, _normalize_name(cleaned), r["id"]),
                )
        else:
            conn.execute("DELETE FROM portfolio_companies WHERE id = ?", (r["id"],))
        pc_removed += 1

    # 4. Fix "(Acquired)" and "(Exited)" tags
    for tag in ['(Acquired)', '(Exited)']:
        tag_rows = conn.execute(
            "SELECT id, company_name, firm_id FROM portfolio_companies WHERE company_name LIKE ?",
            (f'%{tag}%',),
        ).fetchall()
        for r in tag_rows:
            cleaned = r["company_name"].replace(tag, '').strip()
            if cleaned and len(cleaned) > 1:
                exists = conn.execute(
                    "SELECT id FROM portfolio_companies WHERE firm_id = ? AND company_name = ? AND id != ?",
                    (r["firm_id"], cleaned, r["id"]),
                ).fetchone()
                if exists:
                    conn.execute("DELETE FROM portfolio_companies WHERE id = ?", (r["id"],))
                else:
                    conn.execute(
                        "UPDATE portfolio_companies SET company_name = ?, company_name_normalized = ? WHERE id = ?",
                        (cleaned, _normalize_name(cleaned), r["id"]),
                    )
            pc_removed += 1

    # 5. Delete description-like entries
    desc_rows = conn.execute(
        "SELECT id, company_name FROM portfolio_companies WHERE LENGTH(company_name) > 35"
    ).fetchall()
    for r in desc_rows:
        name = r["company_name"]
        words = name.split()
        if len(words) >= 5 and not re.search(r'\(', name):
            lower_words = [w for w in words[1:] if w[0].islower() or w in ('in', 'for', 'of', 'the', 'and', 'a', 'to', 'an', '&')]
            if len(lower_words) >= len(words) - 2:
                conn.execute("DELETE FROM portfolio_companies WHERE id = ?", (r["id"],))
                pc_removed += 1
        elif name.endswith(('Platform', 'Solution', 'Solutions')) and len(name) > 30:
            conn.execute("DELETE FROM portfolio_companies WHERE id = ?", (r["id"],))
            pc_removed += 1

    # 6. Catch-all: delete anything that fails is_valid_portfolio_name()
    #    This catches patterns the SQL conditions above missed.
    all_rows = conn.execute(
        "SELECT id, company_name FROM portfolio_companies"
    ).fetchall()
    for r in all_rows:
        if not is_valid_portfolio_name(r["company_name"]):
            conn.execute("DELETE FROM portfolio_companies WHERE id = ?", (r["id"],))
            pc_removed += 1

    # 7. Deduplicate within the same firm (keep lowest id)
    dupe_rows = conn.execute("""
        SELECT firm_id, company_name_normalized, GROUP_CONCAT(id) as ids, COUNT(*) as cnt
        FROM portfolio_companies
        WHERE company_name_normalized IS NOT NULL AND company_name_normalized != ''
        GROUP BY firm_id, company_name_normalized
        HAVING COUNT(*) > 1
    """).fetchall()
    for d in dupe_rows:
        ids = sorted(int(x) for x in d[2].split(","))
        loser_ids = ids[1:]  # keep the first (oldest)
        ph = ",".join(["?"] * len(loser_ids))
        conn.execute(f"DELETE FROM portfolio_companies WHERE id IN ({ph})", loser_ids)
        pc_removed += len(loser_ids)

    if pc_removed:
        conn.commit()
        logger.info(f"Portfolio cleanup: removed/fixed {pc_removed} junk entries")
    return pc_removed


def clean_firms(conn) -> int:
    """
    Remove junk firms, merge duplicates, and clean orphans.
    Returns number of entries removed/merged.
    """
    removed = 0

    # ── 1. Delete junk firm names ──
    _JUNK_FIRM_PATTERNS = [
        r"^\d+\s+investors?$",          # "50 investors", "1 investor"
        r"^Multiple investors",          # "Multiple investors (19 total)"
        r"^<UNKNOWN>",                   # placeholder
        r"^unknown$",
        r"^undisclosed",
        r"^Growth equity$",              # generic term
        r"^Private equity$",
        r"^Angel investor",
        r"^Various\b",
        r"^Several\b",
        r"^n/?a$",
    ]
    junk_re = re.compile("|".join(_JUNK_FIRM_PATTERNS), re.I)

    all_firms = conn.execute("SELECT id, name FROM firms").fetchall()
    junk_ids = []
    for f in all_firms:
        name = f["name"] if isinstance(f, dict) else f[1]
        fid = f["id"] if isinstance(f, dict) else f[0]
        if junk_re.search(name):
            junk_ids.append(fid)

    # ── 1b. Remove non-VC operating companies (not venture firms) ──
    _NON_VC_NAMES = {
        "openai", "stripe", "nvidia", "google", "paypal",
        "microsoft", "amazon", "apple", "meta", "tesla",
    }
    for f in all_firms:
        name = f["name"] if isinstance(f, dict) else f[1]
        fid = f["id"] if isinstance(f, dict) else f[0]
        if name.lower() in _NON_VC_NAMES and fid not in junk_ids:
            junk_ids.append(fid)

    if junk_ids:
        ph = ",".join(["?"] * len(junk_ids))
        conn.execute(f"DELETE FROM deal_firms WHERE firm_id IN ({ph})", junk_ids)
        conn.execute(f"DELETE FROM portfolio_companies WHERE firm_id IN ({ph})", junk_ids)
        conn.execute(f"DELETE FROM investors WHERE firm_id IN ({ph})", junk_ids)
        conn.execute(f"DELETE FROM firms WHERE id IN ({ph})", junk_ids)
        removed += len(junk_ids)
        logger.info(f"Firm cleanup: removed {len(junk_ids)} junk/non-VC firm entries")

    # ── 2. Split compound firm names ("X and Y") into individual firms ──
    compound_rows = conn.execute(
        "SELECT id, name FROM firms WHERE name LIKE '% and %'"
    ).fetchall()
    for row in compound_rows:
        fid = row["id"] if isinstance(row, dict) else row[0]
        name = row["name"] if isinstance(row, dict) else row[1]
        parts = name.split(" and ")
        if len(parts) == 2 and all(len(p.strip()) > 3 for p in parts):
            # Delete compound entry, deal_firms links go away with it
            conn.execute("DELETE FROM deal_firms WHERE firm_id = ?", (fid,))
            conn.execute("DELETE FROM firms WHERE id = ?", (fid,))
            removed += 1

    # ── 3. Merge duplicate firms (same normalized name) ──
    dupes = conn.execute("""
        SELECT REPLACE(REPLACE(REPLACE(LOWER(name), ' ', ''), '.', ''), ',', '') as norm,
               GROUP_CONCAT(id) as ids, COUNT(*) as cnt
        FROM firms
        GROUP BY norm
        HAVING COUNT(*) > 1
    """).fetchall()
    for d in dupes:
        ids = [int(x) for x in d[1].split(",")]
        keeper_id = ids[0]  # keep the first (oldest)
        loser_ids = ids[1:]
        ph = ",".join(["?"] * len(loser_ids))
        # Re-point portfolio companies and deal links to keeper
        conn.execute(
            f"UPDATE portfolio_companies SET firm_id = ? WHERE firm_id IN ({ph})",
            [keeper_id] + loser_ids,
        )
        conn.execute(
            f"UPDATE OR IGNORE deal_firms SET firm_id = ? WHERE firm_id IN ({ph})",
            [keeper_id] + loser_ids,
        )
        conn.execute(f"DELETE FROM deal_firms WHERE firm_id IN ({ph})", loser_ids)
        conn.execute(f"DELETE FROM firms WHERE id IN ({ph})", loser_ids)
        removed += len(loser_ids)

    # ── 4. Remove orphan firms (no deals AND no portfolio companies) ──
    orphan_ids = conn.execute("""
        SELECT f.id FROM firms f
        LEFT JOIN deal_firms df ON f.id = df.firm_id
        LEFT JOIN portfolio_companies pc ON f.id = pc.firm_id
        WHERE df.firm_id IS NULL AND pc.firm_id IS NULL
    """).fetchall()
    if orphan_ids:
        ids_list = [r[0] for r in orphan_ids]
        ph = ",".join(["?"] * len(ids_list))
        conn.execute(f"DELETE FROM firms WHERE id IN ({ph})", ids_list)
        removed += len(ids_list)
        logger.info(f"Firm cleanup: removed {len(ids_list)} orphan firms")

    if removed:
        conn.commit()
        logger.info(f"Firm cleanup: total removed/merged {removed} entries")
    return removed


# Keywords that indicate a VC firm, not an individual person
_INVESTOR_FIRM_KEYWORDS = (
    "capital", "ventures", "partners", "group", "labs", "fund",
    "invest", "vc", "advisors", "management", "equity", "holdings",
    "accelerator", "studio", "angels", "catalyst", "syndicate",
    "coalition", "bank", "pension", " inc", " llc", " ltd",
    "trading", "manufacturer", "combinator", "a16z", "growth",
    "global", "associates", "enterprise", "technology", "corporation",
    "residential", "hyundai", "aramco", "reuters", "sachs",
    "perkins", "atlantic", "alpha", "reserve", "life insurance",
)

# Well-known VC firms / orgs that look like person names (no keyword match)
_KNOWN_FIRM_NAMES = {
    "goldman sachs", "kleiner perkins", "general atlantic", "guardian life",
    "high alpha", "resilience reserve", "sequoia china", "silicon valley quad",
    "twelve below", "andreessen horowitz", "benchmark", "lightspeed",
    "greylock", "accel", "bessemer", "battery", "coatue", "ribbit",
    "greenoaks", "altimeter", "dragoneer", "lone pine", "d1",
}

# Matches person names: "First Last", "First M. Last", "First Middle Last",
# "First Middle Middle Last", "Sam Bankman-Fried".
# Allows accented chars, hyphens, apostrophes, ALL CAPS.
_PERSON_NAME_RE = re.compile(
    r"^[A-ZÀ-Ü][a-zà-ü\'-]+\s+"                         # First name
    r"(?:[A-ZÀ-Ü][a-zà-ü\'-]*\.?\s+){0,2}"              # 0-2 middle names/initials
    r"[A-ZÀ-Ü][A-Za-zà-ü\'-]+"                           # Last name
    r"(?:\s+(?:Jr|Sr|III?|IV)\.?)?$"                      # Optional suffix
    r"|^[A-Z][A-Z]+\s+[A-Z][A-Z]+$"                      # ALL CAPS: "LARS JOHANSSON"
    r"|^[A-Z][A-Z]\s+[A-Z][a-z]"                         # "DJ Seo" style
)

# Obvious junk patterns for investor names
_JUNK_INVESTOR_RE = re.compile(
    r"^(<UNKNOWN>|unknown|undisclosed|unnamed|various|multiple|several|angel)"
    r"|^\d+\s+investor"
    r"|^n/a\b"
    r"|^N/A\b"
    r"|^-\s"
    r"|^.*\b(CEO|CTO|CFO|COO|VP|Director|Manager)\s*$"
    r"|^(Multibillion|Multi-billion|Undisclosed|Various|Several)"
    r"|^U\.?S\.?\s+Government",
    re.I,
)


def _investor_looks_like_firm(name: str) -> bool:
    """Return True if an investor name looks like a firm, not a person."""
    if not name:
        return False
    name_lower = name.lower()
    # Known firm names that look like person names
    if name_lower in _KNOWN_FIRM_NAMES:
        return True
    # Contains firm keywords
    if any(kw in name_lower for kw in _INVESTOR_FIRM_KEYWORDS):
        return True
    # Single word, uppercase start, no spaces — likely a firm (Intel, AMD, Google)
    if " " not in name.strip() and name[0].isupper() and len(name) > 2:
        return True
    # Starts with digits — not a person name (e.g. "01A", "37 Angels")
    if name and name[0].isdigit():
        return True
    # Starts with lowercase — likely a brand (a16z, etc.)
    if name and name[0].islower():
        return True
    # Contains parentheses — likely an org abbreviation
    if "(" in name:
        return True
    # ALL CAPS abbreviations (3+ chars) — likely org (ICONIQ, ARENA, DST, DFJ, DXC)
    if re.match(r'^[A-Z]{2,}(\s|$)', name) and not _PERSON_NAME_RE.match(name):
        return True
    return False


def clean_investors(conn) -> Dict:
    """
    Remove firm-name entries and junk from the investors table.
    Fix title concatenation, embedded credentials, and name formatting.
    Re-links deal connections to firm records where possible.

    Returns dict with counts: {removed, relinked, junk_removed, fixed}.
    """
    from database import _normalize_name
    stats = {"removed": 0, "relinked": 0, "junk_removed": 0, "fixed": 0}

    # 0. Fix data quality issues on ALL investors before filtering
    all_rows = conn.execute("SELECT id, name, title, name_normalized FROM investors").fetchall()
    for r in all_rows:
        name = r["name"] or ""
        title = r["title"]
        updates = {}

        # Fix double spaces in names
        if "  " in name:
            updates["name"] = re.sub(r"\s+", " ", name).strip()

        # Strip embedded credentials (PhD, MD, JD) from names
        cred_re = re.compile(r",?\s*(?:Ph\.?D\.?|MD|JD|MBA|M\.?D\.?|D\.?O\.?)\.?\s*$", re.I)
        cleaned_name = cred_re.sub("", updates.get("name", name)).strip()
        if cleaned_name != (updates.get("name", name)):
            updates["name"] = cleaned_name

        # Fix HTML entities in titles
        if title and "&amp;" in title:
            updates["title"] = title.replace("&amp;", "&")

        # Fix name: strip concatenated title from name field
        # e.g. "Alexi BakerDirector" -> "Alexi Baker", "Anna BansalAnalyst" -> "Anna Bansal"
        n = updates.get("name", name)
        title_in_name = re.search(
            r'([A-Z][a-z]+)((?:Managing |General |Operating |Venture |Senior |Investment )?'
            r'(?:Partner|Director|Principal|Associate|Analyst|Founder|Co-Founder|VP)\b.*)', n)
        if title_in_name and len(title_in_name.group(1)) >= 2:
            updates["name"] = n[:title_in_name.start(2)].strip()
            # Move the extracted title to the title field if no title exists
            if not (updates.get("title", title)):
                updates["title"] = title_in_name.group(2).strip()

        # Fix title: strip person name prefix (name concatenated into title)
        t = updates.get("title", title)
        n = updates.get("name", name)
        if t and n and t.startswith(n):
            cleaned_title = t[len(n):].strip()
            # Handle double-name-prefix: "Name\nNameTitle" pattern
            if cleaned_title.startswith(n):
                cleaned_title = cleaned_title[len(n):].strip()
            updates["title"] = cleaned_title or None

        # Fix title: strip person name SUFFIX (e.g. "Managing PartnerBob Greene")
        t = updates.get("title", title)
        n = updates.get("name", name)
        if t and n and t.endswith(n):
            updates["title"] = t[:-len(n)].strip() or None
        elif t and n and n in t and len(n) > 5:
            # Name appears in the middle of a bio-title — strip it
            updates["title"] = t.replace(n, '').strip()

        # Fix title: strip "ExperienceEducation" suffix
        t = updates.get("title", title)
        if t and "ExperienceEducation" in t:
            updates["title"] = re.sub(r'ExperienceEducation$', '', t).strip() or None

        # Fix title: strip "Bio & More" suffix
        t = updates.get("title", title)
        if t and "Bio & More" in t:
            updates["title"] = re.sub(r'Bio & More.*$', '', t).strip() or None

        # Fix title: handle bios that START with "is a/an" (no name prefix)
        # e.g. "is an Associate at nvp capital, where he..."
        t = updates.get("title", title)
        if t and re.match(r'^is (?:a |an |the |Co-)', t, re.I):
            m = re.match(r'^is (?:a |an |the |Co-Founder)?\s*(.*?)(?:\s+at\s+|\s+of\s+|\s*,\s*where|\s*\.\s*)', t)
            if m:
                updates["title"] = m.group(1).strip() or None
            else:
                updates["title"] = None

        # Fix title: truncate bio paragraphs (keep first title-like phrase)
        t = updates.get("title", title)
        if t and len(t) > 100:
            # Extract just the title part before bio text
            m = re.match(r'^((?:Managing |General |Operating |Venture |Founding |Investment |Senior )?'
                         r'(?:Partner|Director|Principal|Associate|VP|Founder|Co-Founder|Analyst)'
                         r'(?:[,\s]+[\w&]+)*?)(?:\s*[A-Z][a-z]+ (?:is |was |has |joined ))', t)
            if m:
                updates["title"] = m.group(1).strip()
            else:
                # Look for a name-like phrase followed by bio text
                m2 = re.match(r"^([\w\s,&'-]+?)(?:'s (?:career|experience|work|background)|"
                              r"has (?:been|spent|served|worked)|"
                              r"(?:is |was |joined |leads? |manages? |focuses? ))", t)
                if m2 and len(m2.group(1).strip()) < 80:
                    updates["title"] = None  # Bio with no clear title — drop it
                else:
                    # Just take first sentence/clause
                    short = re.split(r'[.;]|\b(?:is a |was |has been |joined )', t)[0].strip()
                    if len(short) < len(t) and len(short) > 3:
                        updates["title"] = short
                    else:
                        updates["title"] = None  # Can't extract — drop rather than keep bio

        # Fix title: strip location suffix (e.g. "Partner, People & TalentNew York")
        t = updates.get("title", title)
        if t:
            t = re.sub(r'(?:New York|San Francisco|Boston|London|Menlo Park|Palo Alto)\s*$', '', t).strip()
            if t != (updates.get("title", title)):
                updates["title"] = t or None

        # Recalculate normalized name if name changed
        if "name" in updates:
            updates["name_normalized"] = _normalize_name(updates["name"])

        if updates:
            set_clause = ", ".join(f"{k} = ?" for k in updates)
            conn.execute(
                f"UPDATE investors SET {set_clause} WHERE id = ?",
                list(updates.values()) + [r["id"]],
            )
            stats["fixed"] += 1

    # Commit after batch fixes to release write lock
    if stats["fixed"]:
        conn.commit()

    # 1. Delete obvious junk entries (nav text, section headings, non-person names)
    _JUNK_INVESTOR_NAMES = re.compile(
        r"^(What We|Our Values|Our Focus|How We|Our Startups|Our Blog|"
        r"We Invest|We Are|Our Network|Founder Catalyst|Startup Weekend|"
        r"Meet Our|Our Model|Program Tracks|No Results|Privacy|"
        r"Our Advisors|Our Culture|View Bio|"
        r"connect$|Info$|Industries$|Kauffman Fellows)",
        re.I,
    )
    # Also check for firm names and generic labels in investor names
    _JUNK_EXACT_NAMES = {
        "partners", "slow ventures", "managing partner", "vice president",
        "partner", "general partner", "principal", "associate",
    }
    junk_rows = conn.execute("SELECT id, name FROM investors").fetchall()
    junk_ids = []
    for r in junk_rows:
        name = r["name"] or ""
        if _JUNK_INVESTOR_RE.search(name):
            junk_ids.append(r["id"])
        elif _JUNK_INVESTOR_NAMES.search(name):
            junk_ids.append(r["id"])
        elif name.lower().strip() in _JUNK_EXACT_NAMES:
            junk_ids.append(r["id"])

    if junk_ids:
        ph = ",".join(["?"] * len(junk_ids))
        # Clear lead_investor_id references
        conn.execute(f"UPDATE deals SET lead_investor_id = NULL WHERE lead_investor_id IN ({ph})", junk_ids)
        conn.execute(f"DELETE FROM deal_investors WHERE investor_id IN ({ph})", junk_ids)
        conn.execute(f"DELETE FROM investors WHERE id IN ({ph})", junk_ids)
        stats["junk_removed"] = len(junk_ids)

    # 2. Find investor records that are actually firm names
    all_investors = conn.execute(
        "SELECT id, name, firm_id FROM investors"
    ).fetchall()

    firm_investor_ids = []
    for inv in all_investors:
        name = inv["name"]

        # Check if it's a firm: keywords, known names, or DB match
        is_firm = _investor_looks_like_firm(name)

        # Check if it matches a known firm in the DB
        firm_row = conn.execute(
            "SELECT id FROM firms WHERE LOWER(name) = LOWER(?)", (name,)
        ).fetchone()

        if firm_row:
            # This investor record IS a firm — relink deals to the firm, then delete
            firm_id = firm_row["id"]
            deal_links = conn.execute(
                "SELECT deal_id FROM deal_investors WHERE investor_id = ?",
                (inv["id"],)
            ).fetchall()
            for dl in deal_links:
                # Create deal_firms link if it doesn't exist
                existing = conn.execute(
                    "SELECT 1 FROM deal_firms WHERE deal_id = ? AND firm_id = ?",
                    (dl["deal_id"], firm_id)
                ).fetchone()
                if not existing:
                    conn.execute(
                        "INSERT INTO deal_firms (deal_id, firm_id, role) VALUES (?, ?, 'participant')",
                        (dl["deal_id"], firm_id)
                    )
                    stats["relinked"] += 1
            firm_investor_ids.append(inv["id"])
        elif is_firm:
            # Matches firm keywords, known firm names, or structural patterns
            firm_investor_ids.append(inv["id"])
        elif _PERSON_NAME_RE.match(name):
            # Looks like a real person name — keep it
            continue
        else:
            # Doesn't match person or firm patterns — remove to be safe
            firm_investor_ids.append(inv["id"])

    if firm_investor_ids:
        ph = ",".join(["?"] * len(firm_investor_ids))
        conn.execute(f"UPDATE deals SET lead_investor_id = NULL WHERE lead_investor_id IN ({ph})", firm_investor_ids)
        conn.execute(f"DELETE FROM deal_investors WHERE investor_id IN ({ph})", firm_investor_ids)
        conn.execute(f"DELETE FROM investors WHERE id IN ({ph})", firm_investor_ids)
        stats["removed"] = len(firm_investor_ids)

    total = stats["removed"] + stats["junk_removed"]
    if total:
        conn.commit()
        logger.info(
            f"Investor cleanup: removed {stats['removed']} firm-name entries, "
            f"{stats['junk_removed']} junk entries, relinked {stats['relinked']} deals to firms"
        )
    return stats
