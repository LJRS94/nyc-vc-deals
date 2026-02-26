"""
Centralized configuration for NYC VC Deal Scraper.
All magic numbers, URLs, thresholds, and env-var lookups in one place.
"""

import os
import secrets
import logging

# ── Database ─────────────────────────────────────────────────
DB_PATH = os.environ.get(
    "DATABASE_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "nyc_vc_deals.db"),
)

# ── Server ───────────────────────────────────────────────────
API_PORT = int(os.environ.get("PORT", 5000))
API_HOST = "0.0.0.0"
CORS_ORIGINS = [
    o.strip() for o in os.environ.get("CORS_ORIGINS", "").split(",") if o.strip()
]
SECRET_KEY = os.environ.get("SECRET_KEY")
if not SECRET_KEY:
    # In production (DATABASE_PATH is set to a non-local path), require SECRET_KEY
    if os.environ.get("DATABASE_PATH") and "/opt/" in os.environ.get("DATABASE_PATH", ""):
        raise RuntimeError(
            "SECRET_KEY environment variable is required in production. "
            "Set it to a random 64-character hex string."
        )
    SECRET_KEY = secrets.token_hex(32)
    logging.getLogger(__name__).warning(
        "SECRET_KEY not set — using random key (sessions will not survive restarts)"
    )

# ── HTTP / Fetcher ───────────────────────────────────────────
REQUEST_TIMEOUT = 15          # default seconds per request
DEFAULT_CACHE_TTL = 86400     # 24 hours
NEWS_CACHE_TTL = 86400 * 7    # 7 days — RSS feeds, news searches
FETCH_MAX_WORKERS = 8         # concurrent fetch threads

# ── Source-specific cache TTLs ───────────────────────────────
OPENCORPORATES_TTL = 86400 * 7    # 7 days — registrations stable
CRUNCHBASE_TTL = 86400 * 3        # 3 days — funding rounds change often
SBIR_TTL = 86400 * 7              # 7 days — government data slow
CLEARBIT_TTL = 86400 * 14         # 14 days — company data stable
HUNTER_TTL = 86400 * 30           # 30 days — domain validation stable
GOOGLE_CSE_SEARCH_TTL = 86400 * 30  # 30 days — website search stable
APOLLO_TTL = 86400 * 14            # 14 days — org enrichment stable

# ── Enrichment cascade TTLs ────────────────────────────────
CLEARBIT_AUTOCOMPLETE_TTL = 86400 * 30   # 30 days
GOOGLE_KG_TTL = 86400 * 30               # 30 days
YC_OSS_TTL = 86400 * 7                   # 7 days
WIKIPEDIA_TTL = 86400 * 30               # 30 days
WIKIDATA_TTL = 86400 * 14                # 14 days

# ── API Keys (all optional) ─────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_CSE_API_KEY = os.environ.get("GOOGLE_CSE_API_KEY", "")
GOOGLE_CSE_CX = os.environ.get("GOOGLE_CSE_CX", "")
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
CLEARBIT_API_KEY = os.environ.get("CLEARBIT_API_KEY", "")
HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
CRUNCHBASE_API_KEY = os.environ.get("CRUNCHBASE_API_KEY", "")
OPENCORPORATES_API_KEY = os.environ.get("OPENCORPORATES_API_KEY", "")
GOOGLE_KG_API_KEY = os.environ.get("GOOGLE_KG_API_KEY", "")

# ── Free-tier rate-limit buffers ─────────────────────────────
GOOGLE_CSE_DAILY_LIMIT = 95   # buffer from 100/day
APOLLO_MONTHLY_LIMIT = 95     # buffer from 100/month
CLEARBIT_FREE_LIMIT = 45      # buffer from 50/month
HUNTER_FREE_LIMIT = 20        # buffer from 25/month
GOOGLE_KG_DAILY_LIMIT = 500
WIKIPEDIA_RATE_DELAY = 1.0
WIKIDATA_BATCH_SIZE = 50

# ── Scraper base URLs ───────────────────────────────────────
ALLEYWATCH_DAILY_BASE = "https://www.alleywatch.com/category/funding/"
DELAWARE_ECORP_BASE = "https://icis.corp.delaware.gov/ecorp/entitysearch"
DELAWARE_ENTITY_SEARCH = "https://icis.corp.delaware.gov/ecorp/entitysearch/namesearch.aspx"
SBIR_CSV_URL = "https://data.www.sbir.gov/awarddatapublic/award_data.csv"
SEC_EFTS_BASE = "https://efts.sec.gov/LATEST/search-index"
NY_DOS_SODA_URL = "https://data.ny.gov/resource/n9v6-gdp6.json"
NY_DOS_APP_TOKEN = os.environ.get("NY_DOS_APP_TOKEN", "")

# ── Deal validation thresholds ───────────────────────────────
MAX_DEAL_AMOUNT = 10_000_000_000    # $10B global cap
MAX_EARLY_STAGE_AMOUNT = 50_000_000  # $50M early-stage filter
MAX_COMPANY_NAME_LENGTH = 45         # reject names longer than this (tightened from 60)
DEDUP_DATE_GAP_DAYS = 180            # >6 months apart = new round
DEDUP_AMOUNT_RATIO = 2.0             # >2x amount diff = different round
FUZZY_NAME_THRESHOLD = 0.85          # company name fuzzy-match cutoff
FIRM_NAME_MIN_LENGTH = 5             # min chars for firm containment match
FIRM_MATCH_RATIO = 0.7               # length ratio for firm name match

# ── LLM ─────────────────────────────────────────────────────
LLM_MODEL = "claude-haiku-4-5-20251001"
LLM_MAX_TEXT_LENGTH = 4000   # chars to send per extraction

# ── Scheduling ───────────────────────────────────────────────
STARTUP_SCRAPE_DELAY = 60            # seconds before first deal scrape
STARTUP_PORTFOLIO_DELAY = 120        # seconds before first portfolio scrape
SCRAPE_DEFAULT_DAYS_BACK = 14
GOOGLE_BATCH_SIZE = 15
GOOGLE_BATCH_DAYS_BACK = 450

# ── Pagination defaults ─────────────────────────────────────
DEFAULT_PAGE_SIZE = 25
FEED_MAX_RESULTS = 1000
TOP_INVESTORS_LIMIT = 30
SCRAPE_LOGS_LIMIT = 50

# ── Multi-City Registry ────────────────────────────────────
ENABLED_CITIES = [
    c.strip()
    for c in os.environ.get("ENABLED_CITIES", "New York,Boston,Washington DC,San Francisco").split(",")
    if c.strip()
]

CITY_REGISTRY = {
    "New York": {
        "display_name": "New York",
        "indicators": [
            "new york", "nyc", "manhattan", "brooklyn", "queens",
            "bronx", "staten island", "ny-based", "new york-based",
            "headquartered in new york", "based in nyc", "flatiron",
            "soho", "tribeca", "midtown", "wall street", "chelsea",
            "greenpoint", "williamsburg", "dumbo", "fidi",
        ],
        "zip_prefixes": ["100", "101", "102", "103", "104", "110", "111", "112", "113", "114", "116"],
        "counties": ["NEW YORK", "KINGS", "QUEENS", "BRONX", "RICHMOND"],
        "state_code": "NY",
        "state_name": "New York",
        "news_locations": ["NYC", '"New York"', "Manhattan", "Brooklyn"],
        "sec_efts_queries": ['"New York"', '"Manhattan"', '"Brooklyn"', "NYC startup"],
        "sec_atom_states": ["NY"],
        "opencorporates_jurisdiction": "us_ny",
    },
    "Boston": {
        "display_name": "Boston",
        "indicators": [
            "boston", "cambridge", "somerville", "massachusetts",
            "mass-based", "boston-based", "headquartered in boston",
            "based in boston", "back bay", "kendall square",
            "seaport district", "south boston",
        ],
        "zip_prefixes": ["021", "022", "024"],
        "counties": ["SUFFOLK", "MIDDLESEX", "NORFOLK"],
        "state_code": "MA",
        "state_name": "Massachusetts",
        "news_locations": ["Boston", '"Cambridge MA"', '"Massachusetts"'],
        "sec_efts_queries": ['"Boston"', '"Cambridge" "Massachusetts"'],
        "sec_atom_states": ["MA"],
        "opencorporates_jurisdiction": "us_ma",
    },
    "Washington DC": {
        "display_name": "Washington DC",
        "indicators": [
            "washington dc", "washington, d.c.", "washington d.c.",
            "dc-based", "washington-based", "headquartered in dc",
            "based in dc", "arlington", "bethesda", "tysons",
            "northern virginia", "nova", "capitol hill",
            "georgetown", "dupont circle",
        ],
        "zip_prefixes": ["200", "201", "202", "203", "204", "220", "221", "222"],
        "counties": ["DISTRICT OF COLUMBIA", "ARLINGTON", "FAIRFAX", "MONTGOMERY"],
        "state_code": "DC",
        "state_name": "District of Columbia",
        "news_locations": ["Washington DC", '"Washington D.C."', '"DC startup"'],
        "sec_efts_queries": ['"Washington" "D.C."', '"Washington DC"'],
        "sec_atom_states": ["DC", "VA", "MD"],
        "opencorporates_jurisdiction": "us_dc",
    },
    "San Francisco": {
        "display_name": "San Francisco",
        "indicators": [
            "san francisco", "sf-based", "san francisco-based",
            "headquartered in san francisco", "based in sf",
            "soma", "mission district", "bay area", "silicon valley",
            "palo alto", "menlo park", "mountain view", "sunnyvale",
            "south of market", "financial district sf",
        ],
        "zip_prefixes": ["941", "940", "943", "944", "945", "950", "951"],
        "counties": ["SAN FRANCISCO", "SAN MATEO", "SANTA CLARA"],
        "state_code": "CA",
        "state_name": "California",
        "news_locations": ["San Francisco", '"SF startup"', '"Bay Area"', '"Silicon Valley"'],
        "sec_efts_queries": ['"San Francisco"', '"Silicon Valley"', '"Palo Alto"'],
        "sec_atom_states": ["CA"],
        "opencorporates_jurisdiction": "us_ca",
    },
}


def get_enabled_cities() -> list:
    """Return list of city config dicts for all enabled cities."""
    return [
        CITY_REGISTRY[name]
        for name in ENABLED_CITIES
        if name in CITY_REGISTRY
    ]


def get_city_config(city_name: str) -> dict:
    """Return the config dict for a specific city, or empty dict."""
    return CITY_REGISTRY.get(city_name, {})
