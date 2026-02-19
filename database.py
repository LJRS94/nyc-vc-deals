"""
NYC VC Deal Scraper — Database Layer
SQLite database with models for deals, firms, investors, and categories.
"""

import sqlite3
import os
import re
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

DB_PATH = os.environ.get(
    "DATABASE_PATH",
    os.path.join(os.path.dirname(__file__), "nyc_vc_deals.db")
)

_STRIP_RE = re.compile(r"[^a-z0-9]")
_BATCH_CONNS = set()  # connection ids currently in batch mode
_local = threading.local()  # thread-local connection pool


def _normalize_name(name: str) -> str:
    """Strip punctuation/spaces/case for dedup column."""
    return _STRIP_RE.sub("", (name or "").lower())


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Thread-local connection pool — reuses connections within the same thread."""
    key = f"conn_{db_path}"
    conn = getattr(_local, key, None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except (sqlite3.ProgrammingError, sqlite3.OperationalError):
            pass  # connection was closed, create new one
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=60000")
    setattr(_local, key, conn)
    return conn


def _is_batch(conn) -> bool:
    """Check if this connection is in batch mode (suppress per-row commits)."""
    return id(conn) in _BATCH_CONNS


def init_db(db_path: str = DB_PATH):
    """Create all tables if they don't exist. Handles migrations for existing DBs."""
    conn = get_connection(db_path)

    # Check if deals table already exists and needs migration
    existing_tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

    if "deals" in existing_tables:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(deals)").fetchall()]
        if "company_name_normalized" not in cols:
            # Run migration for existing DB, then return
            conn.close()
            migrate_db(db_path)
            print(f"[DB] Initialized database at {db_path}")
            return

    cursor = conn.cursor()

    cursor.executescript("""
    -- VC Firms
    CREATE TABLE IF NOT EXISTS firms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        website TEXT,
        location TEXT DEFAULT 'New York, NY',
        description TEXT,
        aum_range TEXT,
        focus_stages TEXT,  -- JSON array: ["Pre-Seed","Seed","Series A","Series B"]
        focus_sectors TEXT, -- JSON array: ["Fintech","Health","SaaS"]
        portfolio_url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Individual Investors / Partners
    CREATE TABLE IF NOT EXISTS investors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        firm_id INTEGER,
        title TEXT,
        linkedin_url TEXT,
        twitter_url TEXT,
        focus_areas TEXT,   -- JSON array
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (firm_id) REFERENCES firms(id)
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_investor_name_firm
        ON investors(name, firm_id);

    -- Deal Categories
    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        parent_id INTEGER,
        FOREIGN KEY (parent_id) REFERENCES categories(id)
    );

    -- Deals (the core table)
    CREATE TABLE IF NOT EXISTS deals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT NOT NULL,
        company_website TEXT,
        company_description TEXT,
        stage TEXT CHECK(stage IN (
            'Pre-Seed', 'Seed', 'Series A', 'Series B', 'Series C+', 'Unknown'
        )) DEFAULT 'Unknown',
        amount_usd REAL,           -- NULL if undisclosed
        amount_disclosed INTEGER DEFAULT 1,
        date_announced DATE,
        date_closed DATE,
        lead_investor_id INTEGER,
        category_id INTEGER,
        subcategory TEXT,
        source_url TEXT,
        source_type TEXT CHECK(source_type IN (
            'press_release', 'sec_filing', 'news_article',
            'firm_website', 'crunchbase', 'pitchbook',
            'de_filing', 'alleywatch', 'google_news', 'other'
        )),
        company_name_normalized TEXT,  -- lowercase, no punctuation/spaces for dedup
        raw_text TEXT,             -- original scraped text for reference
        confidence_score REAL DEFAULT 1.0,  -- 0-1, how sure we are about extraction
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (lead_investor_id) REFERENCES investors(id),
        FOREIGN KEY (category_id) REFERENCES categories(id)
    );
    CREATE INDEX IF NOT EXISTS idx_deals_stage ON deals(stage);
    CREATE INDEX IF NOT EXISTS idx_deals_date ON deals(date_announced);
    CREATE INDEX IF NOT EXISTS idx_deals_company ON deals(company_name);
    CREATE INDEX IF NOT EXISTS idx_deals_normalized ON deals(company_name_normalized);
    CREATE INDEX IF NOT EXISTS idx_deals_source_type ON deals(source_type);

    -- Many-to-many: deals <-> firms (multiple firms per deal)
    CREATE TABLE IF NOT EXISTS deal_firms (
        deal_id INTEGER,
        firm_id INTEGER,
        role TEXT DEFAULT 'participant', -- 'lead' or 'participant'
        PRIMARY KEY (deal_id, firm_id),
        FOREIGN KEY (deal_id) REFERENCES deals(id),
        FOREIGN KEY (firm_id) REFERENCES firms(id)
    );
    CREATE INDEX IF NOT EXISTS idx_deal_firms_firm ON deal_firms(firm_id);

    -- Many-to-many: deals <-> investors
    CREATE TABLE IF NOT EXISTS deal_investors (
        deal_id INTEGER,
        investor_id INTEGER,
        PRIMARY KEY (deal_id, investor_id),
        FOREIGN KEY (deal_id) REFERENCES deals(id),
        FOREIGN KEY (investor_id) REFERENCES investors(id)
    );
    CREATE INDEX IF NOT EXISTS idx_deal_investors_investor ON deal_investors(investor_id);

    -- Deal metadata (key-value pairs for extensible deal attributes)
    CREATE TABLE IF NOT EXISTS deal_metadata (
        deal_id INTEGER NOT NULL,
        key TEXT NOT NULL,
        value TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (deal_id, key),
        FOREIGN KEY (deal_id) REFERENCES deals(id)
    );

    -- Scrape logs for tracking runs
    CREATE TABLE IF NOT EXISTS scrape_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        finished_at TIMESTAMP,
        status TEXT DEFAULT 'running',  -- running, success, error
        deals_found INTEGER DEFAULT 0,
        deals_new INTEGER DEFAULT 0,
        error_message TEXT
    );

    -- Portfolio companies scraped from firm websites
    CREATE TABLE IF NOT EXISTS portfolio_companies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        firm_id INTEGER NOT NULL,
        company_name TEXT NOT NULL,
        company_website TEXT,
        description TEXT,
        lead_partner TEXT,
        sector TEXT,
        source_url TEXT,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (firm_id) REFERENCES firms(id),
        UNIQUE(firm_id, company_name)
    );
    CREATE INDEX IF NOT EXISTS idx_portfolio_firm ON portfolio_companies(firm_id);

    -- Seed default categories
    INSERT OR IGNORE INTO categories (name) VALUES
        ('Fintech'),
        ('Health & Biotech'),
        ('SaaS / Enterprise'),
        ('Consumer / D2C'),
        ('AI / Machine Learning'),
        ('Climate / Cleantech'),
        ('Real Estate / Proptech'),
        ('Education / Edtech'),
        ('Cybersecurity'),
        ('Logistics / Supply Chain'),
        ('Media & Entertainment'),
        ('Food & Agriculture'),
        ('Hardware / Robotics'),
        ('Web3 / Crypto'),
        ('Legal Tech'),
        ('HR / Future of Work'),
        ('Insurance / Insurtech'),
        ('Developer Tools'),
        ('Marketplace'),
        ('Other');
    """)

    conn.commit()
    conn.close()
    print(f"[DB] Initialized database at {db_path}")


@contextmanager
def batch_connection(db_path: str = DB_PATH):
    """
    Context manager that suppresses per-row commits.
    Commits once at the end instead of after every insert.
    Usage:
        with batch_connection() as conn:
            insert_deal(conn, ...)   # no per-call commit
            link_deal_firm(conn, ...)
    """
    conn = get_connection(db_path)
    _BATCH_CONNS.add(id(conn))
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _BATCH_CONNS.discard(id(conn))
        # Don't close — thread-local connection is reused by get_connection()


# ── CRUD helpers ──────────────────────────────────────────────

def upsert_firm(conn, name: str, **kwargs) -> int:
    """Insert or update a firm, return its ID."""
    existing = conn.execute(
        "SELECT id FROM firms WHERE name = ?", (name,)
    ).fetchone()
    if existing:
        if kwargs:
            sets = ", ".join(f"{k} = ?" for k in kwargs)
            conn.execute(
                f"UPDATE firms SET {sets}, updated_at = ? WHERE id = ?",
                (*kwargs.values(), datetime.utcnow().isoformat(), existing["id"])
            )
            if not _is_batch(conn):
                conn.commit()
        return existing["id"]
    cols = ["name"] + list(kwargs.keys())
    vals = [name] + list(kwargs.values())
    placeholders = ", ".join(["?"] * len(vals))
    cur = conn.execute(
        f"INSERT INTO firms ({', '.join(cols)}) VALUES ({placeholders})", vals
    )
    if not _is_batch(conn):
        conn.commit()
    return cur.lastrowid


def upsert_investor(conn, name: str, firm_id: Optional[int] = None, **kwargs) -> int:
    existing = conn.execute(
        "SELECT id FROM investors WHERE name = ? AND firm_id IS ?",
        (name, firm_id)
    ).fetchone()
    if existing:
        return existing["id"]
    cols = ["name", "firm_id"] + list(kwargs.keys())
    vals = [name, firm_id] + list(kwargs.values())
    placeholders = ", ".join(["?"] * len(vals))
    cur = conn.execute(
        f"INSERT INTO investors ({', '.join(cols)}) VALUES ({placeholders})", vals
    )
    if not _is_batch(conn):
        conn.commit()
    return cur.lastrowid


def insert_deal(conn, company_name: str, **kwargs) -> int:
    # Always set the normalized name for dedup
    if "company_name_normalized" not in kwargs:
        kwargs["company_name_normalized"] = _normalize_name(company_name)
    cols = ["company_name"] + list(kwargs.keys())
    vals = [company_name] + list(kwargs.values())
    placeholders = ", ".join(["?"] * len(vals))
    cur = conn.execute(
        f"INSERT INTO deals ({', '.join(cols)}) VALUES ({placeholders})", vals
    )
    if not _is_batch(conn):
        conn.commit()
    return cur.lastrowid


def link_deal_firm(conn, deal_id: int, firm_id: int, role: str = "participant"):
    conn.execute(
        "INSERT OR IGNORE INTO deal_firms (deal_id, firm_id, role) VALUES (?, ?, ?)",
        (deal_id, firm_id, role)
    )
    if not _is_batch(conn):
        conn.commit()


def link_deal_investor(conn, deal_id: int, investor_id: int):
    conn.execute(
        "INSERT OR IGNORE INTO deal_investors (deal_id, investor_id) VALUES (?, ?)",
        (deal_id, investor_id)
    )
    if not _is_batch(conn):
        conn.commit()


def upsert_portfolio_company(conn, firm_id: int, company_name: str, **kwargs) -> int:
    """Insert or update a portfolio company, return its ID."""
    existing = conn.execute(
        "SELECT id FROM portfolio_companies WHERE firm_id = ? AND company_name = ?",
        (firm_id, company_name)
    ).fetchone()
    if existing:
        if kwargs:
            sets = ", ".join(f"{k} = ?" for k in kwargs)
            conn.execute(
                f"UPDATE portfolio_companies SET {sets} WHERE id = ?",
                (*kwargs.values(), existing["id"])
            )
            if not _is_batch(conn):
                conn.commit()
        return existing["id"]
    cols = ["firm_id", "company_name"] + list(kwargs.keys())
    vals = [firm_id, company_name] + list(kwargs.values())
    placeholders = ", ".join(["?"] * len(vals))
    cur = conn.execute(
        f"INSERT INTO portfolio_companies ({', '.join(cols)}) VALUES ({placeholders})", vals
    )
    if not _is_batch(conn):
        conn.commit()
    return cur.lastrowid


def get_category_id(conn, name: str) -> Optional[int]:
    row = conn.execute(
        "SELECT id FROM categories WHERE LOWER(name) = LOWER(?)", (name,)
    ).fetchone()
    return row["id"] if row else None


def log_scrape(conn, source: str) -> int:
    cur = conn.execute(
        "INSERT INTO scrape_logs (source) VALUES (?)", (source,)
    )
    if not _is_batch(conn):
        conn.commit()
    return cur.lastrowid


def finish_scrape(conn, log_id: int, status: str, deals_found: int = 0,
                  deals_new: int = 0, error_message: str = None):
    conn.execute(
        """UPDATE scrape_logs
           SET finished_at = ?, status = ?, deals_found = ?,
               deals_new = ?, error_message = ?
           WHERE id = ?""",
        (datetime.utcnow().isoformat(), status, deals_found,
         deals_new, error_message, log_id)
    )
    if not _is_batch(conn):
        conn.commit()


def migrate_db(db_path: str = DB_PATH):
    """
    Run schema migrations on an existing database:
    1. Add 'alleywatch' to source_type CHECK constraint
    2. Add company_name_normalized column
    3. Backfill normalized names for existing rows
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Check if migration is needed by looking for company_name_normalized column
    cols = [row[1] for row in conn.execute("PRAGMA table_info(deals)").fetchall()]
    if "company_name_normalized" in cols:
        # Check if alleywatch is already allowed — try an insert/rollback
        try:
            conn.execute("SAVEPOINT migration_check")
            conn.execute(
                "INSERT INTO deals (company_name, source_type) VALUES ('__test__', 'alleywatch')"
            )
            conn.execute("ROLLBACK TO migration_check")
            conn.execute("RELEASE migration_check")
            conn.execute("DELETE FROM deals WHERE company_name = '__test__'")
            print("[DB] Schema already up to date")
            conn.close()
            return
        except sqlite3.IntegrityError:
            conn.execute("ROLLBACK TO migration_check")
            conn.execute("RELEASE migration_check")
            # Need to recreate the table for CHECK constraint
        except Exception:
            pass

    print("[DB] Running migration...")

    # SQLite doesn't support ALTER TABLE to modify CHECK constraints,
    # so we recreate the deals table.
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.executescript("""
    BEGIN;

    -- Rename old table
    ALTER TABLE deals RENAME TO deals_old;

    -- Create new table with updated CHECK and new column
    CREATE TABLE deals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT NOT NULL,
        company_website TEXT,
        company_description TEXT,
        stage TEXT CHECK(stage IN (
            'Pre-Seed', 'Seed', 'Series A', 'Series B', 'Series C+', 'Unknown'
        )) DEFAULT 'Unknown',
        amount_usd REAL,
        amount_disclosed INTEGER DEFAULT 1,
        date_announced DATE,
        date_closed DATE,
        lead_investor_id INTEGER,
        category_id INTEGER,
        subcategory TEXT,
        source_url TEXT,
        source_type TEXT CHECK(source_type IN (
            'press_release', 'sec_filing', 'news_article',
            'firm_website', 'crunchbase', 'pitchbook',
            'de_filing', 'alleywatch', 'google_news', 'other'
        )),
        company_name_normalized TEXT,
        raw_text TEXT,
        confidence_score REAL DEFAULT 1.0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (lead_investor_id) REFERENCES investors(id),
        FOREIGN KEY (category_id) REFERENCES categories(id)
    );

    -- Copy data (normalized column will be backfilled after)
    INSERT INTO deals (
        id, company_name, company_website, company_description,
        stage, amount_usd, amount_disclosed, date_announced, date_closed,
        lead_investor_id, category_id, subcategory, source_url, source_type,
        raw_text, confidence_score, created_at, updated_at
    )
    SELECT
        id, company_name, company_website, company_description,
        stage, amount_usd, amount_disclosed, date_announced, date_closed,
        lead_investor_id, category_id, subcategory, source_url, source_type,
        raw_text, confidence_score, created_at, updated_at
    FROM deals_old;

    -- Drop old table
    DROP TABLE deals_old;

    -- Recreate indices
    CREATE INDEX IF NOT EXISTS idx_deals_stage ON deals(stage);
    CREATE INDEX IF NOT EXISTS idx_deals_date ON deals(date_announced);
    CREATE INDEX IF NOT EXISTS idx_deals_company ON deals(company_name);
    CREATE INDEX IF NOT EXISTS idx_deals_normalized ON deals(company_name_normalized);

    COMMIT;
    """)

    conn.execute("PRAGMA foreign_keys=ON")

    # Backfill normalized names
    rows = conn.execute("SELECT id, company_name FROM deals").fetchall()
    for row in rows:
        normalized = _normalize_name(row["company_name"])
        conn.execute(
            "UPDATE deals SET company_name_normalized = ? WHERE id = ?",
            (normalized, row["id"])
        )
    conn.commit()
    conn.close()
    print(f"[DB] Migration complete: updated CHECK constraint, "
          f"added company_name_normalized, backfilled {len(rows)} rows")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "migrate":
        migrate_db()
    else:
        init_db()


