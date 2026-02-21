"""
Core unit tests for NYC VC Deal Scraper.
Tests: company name validation/cleaning, amount parsing, stage detection,
       sector classification, dedup logic, investor parsing.
"""

import os
import sys
import sqlite3
import pytest

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.llm_extract import validate_company_name, clean_company_name
from scrapers.utils import (
    parse_amount, normalize_stage, classify_stage_from_amount,
    classify_sector, normalize_company_name, company_names_match,
    validate_deal_amount, parse_investors, is_nyc_related,
    ensure_full_date, parse_pub_date, is_duplicate_deal,
)


# ── Company Name Validation ──────────────────────────────────

class TestValidateCompanyName:
    def test_valid_names(self):
        assert validate_company_name("Ramp")
        assert validate_company_name("Stripe")
        assert validate_company_name("FJ Labs")
        assert validate_company_name("OpenAI")
        assert validate_company_name("Acme Corp")
        assert validate_company_name("23andMe")

    def test_rejects_empty(self):
        assert not validate_company_name("")
        assert not validate_company_name(None)

    def test_rejects_too_long(self):
        assert not validate_company_name("A" * 46)
        assert validate_company_name("A" * 45)

    def test_rejects_verbs(self):
        assert not validate_company_name("Company announced new round")
        assert not validate_company_name("Startup launched today")
        assert not validate_company_name("Firm reported earnings")

    def test_rejects_headlines(self):
        assert not validate_company_name("Exclusive: Big Tech News")
        assert not validate_company_name("Breaking: Startup Fails")
        assert not validate_company_name("Why This Matters")

    def test_rejects_headline_prefixes(self):
        assert not validate_company_name("AI startup raises money")
        assert not validate_company_name("NYC startup launches")

    def test_rejects_startup_keyword(self):
        assert not validate_company_name("fintech startup")

    def test_rejects_mostly_lowercase(self):
        assert not validate_company_name("this is a sentence not a name")


# ── Company Name Cleaning ────────────────────────────────────

class TestCleanCompanyName:
    def test_strips_comma_suffix(self):
        assert clean_company_name("Bedrock,") == "Bedrock"

    def test_strips_startup_description(self):
        assert clean_company_name("Bedrock, an A.I. Start-Up for Construction,") == "Bedrock"

    def test_strips_leading_qualifier(self):
        assert clean_company_name("AI startup Acme") == "Acme"
        assert clean_company_name("fintech company PayCo") == "PayCo"

    def test_strips_nyc_prefix(self):
        assert clean_company_name("NYC-based Ramp") == "Ramp"
        assert clean_company_name("New York-based Stripe") == "Stripe"

    def test_strips_quotes(self):
        assert clean_company_name('"Acme"') == "Acme"
        assert clean_company_name("'Acme'") == "Acme"

    def test_strips_formerly(self):
        assert clean_company_name("Acme (formerly OldCo)") == "Acme"
        assert clean_company_name("NewBrand (formerly Legacy Inc)") == "NewBrand"

    def test_strips_legal_suffixes(self):
        assert clean_company_name("Acme Inc.") == "Acme"
        assert clean_company_name("Acme, Inc.") == "Acme"
        assert clean_company_name("Acme LLC") == "Acme"
        assert clean_company_name("Acme Corp") == "Acme"
        assert clean_company_name("Acme Ltd.") == "Acme"

    def test_strips_series_suffix(self):
        assert clean_company_name("Acme — Series A") == "Acme"
        assert clean_company_name("Acme - Raises $50M") == "Acme"
        assert clean_company_name("Acme — Closes $30M Round") == "Acme"

    def test_preserves_normal_names(self):
        assert clean_company_name("Ramp") == "Ramp"
        assert clean_company_name("FJ Labs") == "FJ Labs"

    def test_empty_input(self):
        assert clean_company_name("") == ""
        assert clean_company_name(None) is None


# ── Amount Parsing ───────────────────────────────────────────

class TestParseAmount:
    def test_millions(self):
        assert parse_amount("$30M") == 30_000_000
        assert parse_amount("$4.5 million") == 4_500_000
        assert parse_amount("$150M") == 150_000_000

    def test_billions(self):
        assert parse_amount("$1.5B") == 1_500_000_000
        assert parse_amount("$2 billion") == 2_000_000_000

    def test_thousands(self):
        assert parse_amount("$500K") == 500_000
        assert parse_amount("$750 thousand") == 750_000

    def test_bare_large_dollar(self):
        assert parse_amount("$5000000") == 5_000_000

    def test_commas(self):
        assert parse_amount("$5,000,000") == 5_000_000

    def test_no_multiplier_small(self):
        # Bare small dollar amounts are ambiguous — should return None
        assert parse_amount("$150") is None

    def test_empty(self):
        assert parse_amount("") is None
        assert parse_amount(None) is None

    def test_non_dollar_with_suffix(self):
        assert parse_amount("raised 30M") == 30_000_000


# ── Stage Normalization ──────────────────────────────────────

class TestNormalizeStage:
    def test_standard_stages(self):
        assert normalize_stage("seed") == "Seed"
        assert normalize_stage("Series A") == "Series A"
        assert normalize_stage("Series B") == "Series B"
        assert normalize_stage("Series C") == "Series C+"
        assert normalize_stage("Series D") == "Series C+"

    def test_pre_seed_variants(self):
        assert normalize_stage("pre-seed") == "Pre-Seed"
        assert normalize_stage("angel round") == "Pre-Seed"
        assert normalize_stage("safe note") == "Pre-Seed"

    def test_growth_rounds(self):
        assert normalize_stage("growth equity") == "Series C+"
        assert normalize_stage("late stage") == "Series C+"
        assert normalize_stage("mezzanine") == "Series C+"

    def test_unknown(self):
        assert normalize_stage("") == "Unknown"
        assert normalize_stage(None) == "Unknown"
        assert normalize_stage("gibberish") == "Unknown"


# ── Stage from Amount (2025-2026 thresholds) ─────────────────

class TestClassifyStageFromAmount:
    def test_pre_seed(self):
        assert classify_stage_from_amount(500_000) == "Pre-Seed"
        assert classify_stage_from_amount(1_500_000) == "Pre-Seed"

    def test_seed(self):
        assert classify_stage_from_amount(2_000_000) == "Seed"
        assert classify_stage_from_amount(6_000_000) == "Seed"

    def test_series_a(self):
        assert classify_stage_from_amount(8_000_000) == "Series A"
        assert classify_stage_from_amount(25_000_000) == "Series A"

    def test_series_b(self):
        assert classify_stage_from_amount(40_000_000) == "Series B"
        assert classify_stage_from_amount(80_000_000) == "Series B"

    def test_series_c_plus(self):
        assert classify_stage_from_amount(100_000_000) == "Series C+"
        assert classify_stage_from_amount(500_000_000) == "Series C+"

    def test_none(self):
        assert classify_stage_from_amount(None) == "Unknown"


# ── Sector Classification ────────────────────────────────────

class TestClassifySector:
    def test_fintech(self):
        assert classify_sector("payment processing and banking platform") == "Fintech"

    def test_health(self):
        assert classify_sector("clinical trial management for healthcare") == "Health & Biotech"

    def test_ai(self):
        assert classify_sector("generative AI and machine learning tools") == "AI / Machine Learning"

    def test_cybersecurity(self):
        assert classify_sector("threat detection and encryption solutions") == "Cybersecurity"

    def test_none_for_empty(self):
        assert classify_sector("") is None
        assert classify_sector(None) is None


# ── Company Name Matching ────────────────────────────────────

class TestCompanyNamesMatch:
    def test_exact_match(self):
        assert company_names_match("Ramp", "Ramp")
        assert company_names_match("FJ Labs", "FJ Labs")

    def test_case_insensitive(self):
        assert company_names_match("ramp", "RAMP")

    def test_punctuation_stripped(self):
        assert company_names_match("F.J. Labs", "FJ Labs")

    def test_containment(self):
        # "Sixfold AI" vs "Sixfold" — ratio 7/9 = 0.78, below 0.85 threshold
        # This is expected: short suffixes like "AI" don't qualify
        assert not company_names_match("Sixfold AI", "Sixfold")
        # But very close names should match
        assert company_names_match("FJLabs", "FJ Labs")

    def test_no_match(self):
        assert not company_names_match("Ramp", "Stripe")
        assert not company_names_match("Apple", "Pineapple")

    def test_empty(self):
        assert not company_names_match("", "Ramp")
        assert not company_names_match("Ramp", "")


# ── Deal Amount Validation ───────────────────────────────────

class TestValidateDealAmount:
    def test_valid_amounts(self):
        assert validate_deal_amount(5_000_000, "Series A")
        assert validate_deal_amount(1_000_000, "Seed")
        assert validate_deal_amount(None, "Unknown")  # undisclosed

    def test_rejects_negative(self):
        assert not validate_deal_amount(-1, "Seed")
        assert not validate_deal_amount(0, "Seed")

    def test_rejects_over_cap(self):
        assert not validate_deal_amount(11_000_000_000, "Series C+")  # >$10B
        assert not validate_deal_amount(6_000_000, "Pre-Seed")  # >$5M for Pre-Seed


# ── Investor Parsing ─────────────────────────────────────────

class TestParseInvestors:
    def test_simple_list(self):
        investors, lead = parse_investors("Sequoia, Andreessen Horowitz, Thrive Capital")
        assert len(investors) == 3
        assert "Sequoia" in investors

    def test_led_by(self):
        investors, lead = parse_investors("led by Founders Fund with participation from Stripe")
        assert lead == "Founders Fund"
        assert "Stripe" in investors

    def test_empty(self):
        investors, lead = parse_investors("")
        assert investors == []
        assert lead is None

    def test_none(self):
        investors, lead = parse_investors(None)
        assert investors == []
        assert lead is None


# ── NYC Detection ────────────────────────────────────────────

class TestIsNYCRelated:
    def test_detects_nyc(self):
        assert is_nyc_related("Based in New York City")
        assert is_nyc_related("NYC-based startup")
        assert is_nyc_related("Headquartered in Manhattan")
        assert is_nyc_related("Brooklyn-based company")

    def test_rejects_non_nyc(self):
        assert not is_nyc_related("San Francisco based startup")
        assert not is_nyc_related("London fintech company")


# ── Date Helpers ─────────────────────────────────────────────

class TestEnsureFullDate:
    def test_full_date_passthrough(self):
        assert ensure_full_date("2025-01-15") == "2025-01-15"

    def test_month_only(self):
        assert ensure_full_date("2025-01") == "2025-01-01"

    def test_invalid(self):
        assert ensure_full_date("not a date") is None
        assert ensure_full_date("") is None
        assert ensure_full_date(None) is None


class TestParsePubDate:
    def test_rss_format(self):
        result = parse_pub_date("Wed, 12 Feb 2025 08:00:00 GMT")
        assert result == "2025-02-12"

    def test_iso_format(self):
        assert parse_pub_date("2025-02-12") == "2025-02-12"

    def test_invalid(self):
        assert parse_pub_date("not a date") is None
        assert parse_pub_date(None) is None


# ── Dedup with In-Memory DB ──────────────────────────────────

@pytest.fixture
def mem_db():
    """Create an in-memory SQLite DB with the deals table for dedup tests."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name TEXT NOT NULL,
            company_name_normalized TEXT,
            stage TEXT DEFAULT 'Unknown',
            amount_usd REAL,
            date_announced DATE
        )
    """)
    conn.execute("""
        CREATE TABLE firms (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
    """)
    conn.commit()
    return conn


class TestIsDuplicateDeal:
    def test_no_existing(self, mem_db):
        assert not is_duplicate_deal(mem_db, "Acme", "Seed")

    def test_exact_duplicate(self, mem_db):
        mem_db.execute(
            "INSERT INTO deals (company_name, company_name_normalized, stage, date_announced) "
            "VALUES (?, ?, ?, ?)",
            ("Acme", "acme", "Seed", "2025-01-15")
        )
        mem_db.commit()
        assert is_duplicate_deal(mem_db, "Acme", "Seed", date_announced="2025-01-15")

    def test_different_stage_is_new(self, mem_db):
        mem_db.execute(
            "INSERT INTO deals (company_name, company_name_normalized, stage, date_announced) "
            "VALUES (?, ?, ?, ?)",
            ("Acme", "acme", "Seed", "2025-01-15")
        )
        mem_db.commit()
        assert not is_duplicate_deal(mem_db, "Acme", "Series A", date_announced="2025-03-15")

    def test_same_stage_far_apart_is_new(self, mem_db):
        mem_db.execute(
            "INSERT INTO deals (company_name, company_name_normalized, stage, date_announced) "
            "VALUES (?, ?, ?, ?)",
            ("Acme", "acme", "Seed", "2024-01-15")
        )
        mem_db.commit()
        # >6 months apart — should be a new round
        assert not is_duplicate_deal(mem_db, "Acme", "Seed", date_announced="2025-01-15")

    def test_same_stage_close_dates_is_duplicate(self, mem_db):
        mem_db.execute(
            "INSERT INTO deals (company_name, company_name_normalized, stage, date_announced) "
            "VALUES (?, ?, ?, ?)",
            ("Acme", "acme", "Seed", "2025-01-15")
        )
        mem_db.commit()
        assert is_duplicate_deal(mem_db, "Acme", "Seed", date_announced="2025-02-01")
