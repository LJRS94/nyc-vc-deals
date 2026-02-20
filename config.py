"""
Centralized configuration for NYC VC Deal Scraper.
All magic numbers, URLs, thresholds, and env-var lookups in one place.
"""

import os

# ── Database ─────────────────────────────────────────────────
DB_PATH = os.environ.get(
    "DATABASE_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "nyc_vc_deals.db"),
)

# ── Server ───────────────────────────────────────────────────
API_PORT = int(os.environ.get("PORT", 5000))
API_HOST = "0.0.0.0"
SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-key-change-me")

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

# ── API Keys (all optional) ─────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_CSE_API_KEY = os.environ.get("GOOGLE_CSE_API_KEY", "")
GOOGLE_CSE_CX = os.environ.get("GOOGLE_CSE_CX", "")
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
CLEARBIT_API_KEY = os.environ.get("CLEARBIT_API_KEY", "")
HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
CRUNCHBASE_API_KEY = os.environ.get("CRUNCHBASE_API_KEY", "")
OPENCORPORATES_API_KEY = os.environ.get("OPENCORPORATES_API_KEY", "")

# ── Free-tier rate-limit buffers ─────────────────────────────
GOOGLE_CSE_DAILY_LIMIT = 95   # buffer from 100/day
APOLLO_MONTHLY_LIMIT = 95     # buffer from 100/month
CLEARBIT_FREE_LIMIT = 45      # buffer from 50/month
HUNTER_FREE_LIMIT = 20        # buffer from 25/month

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
MAX_COMPANY_NAME_LENGTH = 60         # reject names longer than this
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
