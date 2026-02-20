"""
Unified Quality Control System for NYC VC Deal Scraper.

ALL deal data — regardless of source — must pass through validate_deal()
before insertion. This is the single gate for data quality.

Features:
  - Unified validation for company name, stage, amount, date, NYC status
  - Smart deduplication (allows multi-round, blocks true duplicates)
  - Rejection logging for self-improvement
  - Post-ingestion audit that flags suspicious data
  - Quality metrics tracking over time
"""

import re
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

from scrapers.utils import (
    normalize_company_name, normalize_stage, classify_stage_from_amount,
    validate_deal_amount, is_duplicate_deal, company_names_match,
    should_skip_deal, classify_sector,
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
    """Create QC tables if they don't exist."""
    conn.executescript(QC_SCHEMA)
    conn.commit()


# ── Rejection Logging ───────────────────────────────────────────

def _log_rejection(conn, company_name: str, reason: str,
                   source_type: str = None, raw_data: str = None):
    """Log a rejection for pattern analysis."""
    conn.execute(
        "INSERT INTO qc_rejections (company_name, reason, source_type, raw_data) "
        "VALUES (?, ?, ?, ?)",
        (company_name, reason, source_type, (raw_data or "")[:500])
    )
    # Update pattern tracker
    pattern_value = _extract_pattern(company_name, reason)
    if pattern_value:
        conn.execute(
            "INSERT INTO qc_patterns (pattern_type, pattern_value, hit_count, last_seen) "
            "VALUES (?, ?, 1, CURRENT_TIMESTAMP) "
            "ON CONFLICT(pattern_type, pattern_value) DO UPDATE SET "
            "hit_count = hit_count + 1, last_seen = CURRENT_TIMESTAMP",
            (reason, pattern_value)
        )


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
    re.compile(r"\b(startup|company|firm|platform)\s*$", re.I),
    re.compile(r"^(top|breaking|exclusive|report|update|exploring|roundup)", re.I),
    re.compile(r"\b(said|told|announced|reported|raised|raises)\b", re.I),
    re.compile(r"[?!:]"),  # headlines have punctuation
    re.compile(r"^\d+\s"),  # starts with number (street address or list)
    re.compile(r"^[\$']"),  # starts with $ or quote
]


def validate_deal(conn, company_name: str, stage: str = "Unknown",
                  amount: float = None, date_announced: str = None,
                  source_type: str = "other", description: str = None,
                  is_nyc: bool = None, raw_text: str = None,
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

    # Length check (tightened from 60 to 45)
    if len(company_name) > 45:
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
    )

    # ── 8. Build cleaned data ──
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
    # Pass through optional fields
    if description:
        cleaned["company_description"] = description
    if raw_text:
        cleaned["raw_text"] = raw_text[:2000]
    for k in ("company_website", "source_url", "category_id", "subcategory"):
        if k in kwargs and kwargs[k] is not None:
            cleaned[k] = kwargs[k]

    return True, "accepted", cleaned


def _compute_confidence(company_name: str, stage: str, amount: float,
                        date_announced: str, source_type: str,
                        description: str, is_nyc: bool) -> float:
    """
    Unified confidence scoring.
    Source reliability + data completeness + NYC confirmation.
    """
    # Base by source reliability
    source_scores = {
        "alleywatch": 0.90,
        "crunchbase": 0.85,
        "pitchbook": 0.85,
        "press_release": 0.75,
        "news_article": 0.70,
        "sec_filing": 0.65,
        "firm_website": 0.60,
        "google_news": 0.55,
        "de_filing": 0.50,
        "other": 0.40,
    }
    base = source_scores.get(source_type, 0.40)

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
        elif len(name) > 45:
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


def get_rejection_summary(conn, days: int = 30) -> Dict:
    """Get rejection stats for the last N days."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute(
        "SELECT reason, COUNT(*) as cnt FROM qc_rejections "
        "WHERE created_at >= ? GROUP BY reason ORDER BY cnt DESC",
        (cutoff,)
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def record_metrics(conn, source_type: str, submitted: int,
                   accepted: int, rejections: Dict, avg_confidence: float):
    """Record quality metrics for a scrape run."""
    conn.execute(
        "INSERT INTO qc_metrics (run_date, source_type, deals_submitted, "
        "deals_accepted, deals_rejected, rejection_reasons, avg_confidence) "
        "VALUES (date('now'), ?, ?, ?, ?, ?, ?)",
        (source_type, submitted, accepted, submitted - accepted,
         json.dumps(rejections), avg_confidence)
    )
    conn.commit()
