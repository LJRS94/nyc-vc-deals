# NYC VC Deal Scraper & Intelligence Platform

An automated pipeline that scrapes, categorizes, and organizes early-stage venture capital deals (Pre-Seed through Series B) from New York City–based firms and startups.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                       SCRAPER LAYER                          │
├─────────────┬────────────┬──────────────┬────────────────────┤
│  News/Press │  SEC EDGAR │  DE Div of   │  VC Firm Sites     │
│  Google News│  Form D    │  Corporations│  Portfolio pages    │
│  PR Newswire│  XBRL data │  ICIS + ECORP│  News/blog pages   │
│             │            │  OpenCorp API│                     │
└──────┬──────┴─────┬──────┴──────┬───────┴────────┬───────────┘
       │            │             │                │
       ▼            ▼             ▼                ▼
┌──────────────────────────────────────────────────────────────┐
│                  EXTRACTION & NLP LAYER                       │
│  • Company name extraction                                    │
│  • Stage detection (Pre-Seed → Series B)                      │
│  • Amount parsing ($X.XM / $X.XB)                             │
│  • Category classification (20 sectors)                       │
│  • Investor/firm extraction                                   │
│  • NYC location filtering                                     │
│  • DE incorporation cross-reference                           │
│  • Confidence scoring                                         │
└────────────────────────┬─────────────────────────────────────┘
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                      SQLite DATABASE                          │
│  deals · firms · investors · categories                       │
│  deal_firms · deal_investors · scrape_logs                    │
└────────────────────────┬─────────────────────────────────────┘
                         ▼
┌────────────────────────┴─────────────────────────────────────┐
│                   Flask REST API                              │
│  /api/deals · /api/firms · /api/stats · /api/deals/de-inc    │
├──────────────────────────────────────────────────────────────┤
│                   React Dashboard                             │
│  Overview · Deal Table · Firm Explorer · Source Breakdown      │
└──────────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Initialize database + seed NYC VC firms
python main.py init

# Run a full scrape (news + SEC + firm websites)
python main.py scrape

# Start bi-weekly scheduled scraping
python main.py schedule

# View database summary
python main.py summary

# Export data
python main.py export-csv -o deals.csv
python main.py export-json -o deals.json

# Start the API server (for dashboard)
python api_server.py
```

## Data Sources

| Source | What It Scrapes | Frequency |
|--------|----------------|-----------|
| **Google News RSS** | Funding announcements from tech press | Bi-weekly |
| **PR Newswire** | Official press releases | Bi-weekly |
| **SEC EDGAR** | Form D private placement filings | Bi-weekly (14-day lookback) |
| **Delaware Div of Corps** | ICIS entity search, ECORP filings, OpenCorporates | Bi-weekly |
| **VC Firm Websites** | Portfolio pages + news/blog sections | Bi-weekly |

### Why Delaware?

Most VC-backed startups incorporate in Delaware (even if headquartered in NYC) due to its
business-friendly corporate law, Court of Chancery, and established legal precedents.
The Delaware scraper catches deals that may not appear in NY state filings by:

1. **SEC EDGAR cross-reference** — Finds Form D filings where state of incorporation = DE
   and principal place of business = NY
2. **DE Division of Corporations (ICIS)** — Searches the Delaware entity registry to verify
   incorporation status for companies already in the database
3. **OpenCorporates API** — Fallback lookup for Delaware-registered entities
4. **Name variant matching** — Tries "Company, Inc.", "Company, LLC", "Company, Corp."
   since Delaware filings include the entity suffix

## Database Schema

### Core Tables
- **deals** — Every funding round tracked (company, stage, amount, category, source)
- **firms** — VC firms with focus areas and portfolio URLs
- **investors** — Individual partners/investors linked to firms
- **categories** — 20 sector categories (Fintech, AI, Health, etc.)

### Junction Tables
- **deal_firms** — Which firms participated in which deals (with lead/participant role)
- **deal_investors** — Which individual investors were involved
- **scrape_logs** — Audit trail of every scrape run

## Pre-Seeded NYC VC Firms (20)

Union Square Ventures, Lerer Hippeau, FirstMark Capital, Insight Partners, Greycroft, BoxGroup, Primary Venture Partners, Bowery Capital, RRE Ventures, Flybridge Capital, Thrive Capital, Notation Capital, Work-Bench, Torch Capital, Compound, Two Sigma Ventures, Tiger Global, Contour Venture Partners, ERA, Tusk Venture Partners

## NLP Extraction Features

- **Stage Detection** — Regex patterns for Pre-Seed, Seed, Series A, Series B
- **Amount Parsing** — Handles $XM, $X.XM, $XB, "X million dollars" formats
- **Category Classification** — Keyword scoring across 20 sectors with 180+ keywords
- **NYC Filtering** — Detects NY-based companies via 20+ location indicators
- **Company Name Extraction** — Parses headlines like "CompanyX Raises $10M in Series A"
- **Investor Extraction** — Finds "led by X", "with participation from Y, Z"
- **Confidence Scoring** — 0–1 score based on extraction quality

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/stats` | Dashboard KPIs (totals, averages, DE count) |
| `GET /api/deals` | Paginated deals with filters (`?stage=Seed&category=Fintech&q=search`) |
| `GET /api/deals/by-stage` | Aggregated by funding stage |
| `GET /api/deals/by-category` | Aggregated by sector category |
| `GET /api/deals/by-source` | Aggregated by data source (news, SEC, DE, firm sites) |
| `GET /api/deals/by-month` | Monthly deal volume timeline |
| `GET /api/deals/de-incorporated` | Delaware-incorporated deals only |
| `GET /api/firms` | All firms with deal counts |
| `GET /api/firms/:id` | Firm detail with deals and team |
| `GET /api/investors` | All investors with deal counts |
| `GET /api/categories` | Available categories |
| `GET /api/scrape-logs` | Recent scrape run history |

## Extending

### Add a new VC firm
Edit `scrapers/firm_scraper.py` → `NYC_VC_FIRMS` list:
```python
{
    "name": "New Firm Name",
    "website": "https://www.newfirm.com",
    "portfolio_url": "https://www.newfirm.com/portfolio",
    "focus_stages": '["Seed","Series A"]',
    "focus_sectors": '["Fintech","AI"]',
}
```

### Add a new scraper source
1. Create `scrapers/your_source_scraper.py`
2. Implement `run_your_source_scraper()` function
3. Add it to `main.py` → `run_full_scrape()`

### Add a new category
```sql
INSERT INTO categories (name) VALUES ('Your New Category');
```
Then add keywords to `news_scraper.py` → `CATEGORY_KEYWORDS`.

## Deployment Notes

- **Cron alternative**: Instead of `python main.py schedule`, use system cron:
  ```
  0 6 */14 * * cd /path/to/scraper && python main.py scrape >> cron.log 2>&1
  ```
- **SEC compliance**: The scraper identifies itself via User-Agent and respects rate limits (10 req/sec)
- **Politeness**: 1-2 second delays between requests to firm websites
- **Storage**: SQLite is fine for <100K deals. For larger scale, migrate to PostgreSQL.
