"""
NYC VC Deal Scraper — Database Layer
SQLite database with models for deals, firms, investors, and categories.
"""

import sqlite3
import os
import re
import logging
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

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
            migrate_db(db_path)
            logger.info(f"Initialized database at {db_path}")
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
        consecutive_failures INTEGER DEFAULT 0,
        last_scraped_at TIMESTAMP,
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
            'de_filing', 'alleywatch', 'google_news', 'ny_dos', 'other'
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
    CREATE INDEX IF NOT EXISTS idx_deals_category ON deals(category_id);
    CREATE INDEX IF NOT EXISTS idx_deals_amount ON deals(amount_usd);

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
    CREATE INDEX IF NOT EXISTS idx_deal_firms_deal ON deal_firms(deal_id);

    -- Many-to-many: deals <-> investors
    CREATE TABLE IF NOT EXISTS deal_investors (
        deal_id INTEGER,
        investor_id INTEGER,
        PRIMARY KEY (deal_id, investor_id),
        FOREIGN KEY (deal_id) REFERENCES deals(id),
        FOREIGN KEY (investor_id) REFERENCES investors(id)
    );
    CREATE INDEX IF NOT EXISTS idx_deal_investors_investor ON deal_investors(investor_id);
    CREATE INDEX IF NOT EXISTS idx_deal_investors_deal ON deal_investors(deal_id);

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
        company_name_normalized TEXT,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (firm_id) REFERENCES firms(id),
        UNIQUE(firm_id, company_name)
    );
    CREATE INDEX IF NOT EXISTS idx_portfolio_firm ON portfolio_companies(firm_id);
    CREATE INDEX IF NOT EXISTS idx_portfolio_normalized ON portfolio_companies(company_name_normalized);

    -- Users (username/password)
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        display_name TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_login_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- User preferences (key-value: sectors, stages, min_amount, max_amount)
    CREATE TABLE IF NOT EXISTS user_preferences (
        user_id INTEGER NOT NULL,
        key TEXT NOT NULL,
        value TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id, key),
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    -- Saved/bookmarked deals
    CREATE TABLE IF NOT EXISTS saved_deals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        deal_id INTEGER NOT NULL,
        folder TEXT DEFAULT 'Default',
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (deal_id) REFERENCES deals(id) ON DELETE CASCADE,
        UNIQUE(user_id, deal_id)
    );
    CREATE INDEX IF NOT EXISTS idx_saved_deals_user ON saved_deals(user_id);

    -- Notifications
    CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,           -- NULL = broadcast to all
        type TEXT NOT NULL,        -- 'follow_on', 'new_match', 'milestone'
        title TEXT NOT NULL,
        body TEXT,
        deal_id INTEGER,
        firm_id INTEGER,
        read INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        FOREIGN KEY (deal_id) REFERENCES deals(id),
        FOREIGN KEY (firm_id) REFERENCES firms(id)
    );
    CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, read);

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

    # Initialize QC tables
    try:
        from quality_control import init_qc_tables
        init_qc_tables(conn)
    except ImportError:
        logger.debug("QC module not yet available during early init")

    logger.info(f"Initialized database at {db_path}")


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


# ── Column whitelists (prevent SQL injection via kwargs keys) ──

_FIRMS_COLUMNS = {
    "website", "location", "description", "aum_range",
    "focus_stages", "focus_sectors", "portfolio_url",
}
_INVESTORS_COLUMNS = {
    "title", "linkedin_url", "twitter_url", "focus_areas",
}
_DEALS_COLUMNS = {
    "company_website", "company_description", "stage", "amount_usd",
    "amount_disclosed", "date_announced", "date_closed", "lead_investor_id",
    "category_id", "subcategory", "source_url", "source_type",
    "company_name_normalized", "raw_text", "confidence_score",
}
_PORTFOLIO_COLUMNS = {
    "company_website", "description", "lead_partner", "sector", "source_url",
}


def _validate_columns(kwargs: dict, allowed: set, table: str):
    """Raise ValueError if any kwargs key is not in the allowed set."""
    bad = set(kwargs.keys()) - allowed
    if bad:
        raise ValueError(f"Invalid column(s) for {table}: {bad}")


# ── CRUD helpers ──────────────────────────────────────────────

def upsert_firm(conn, name: str, **kwargs) -> int:
    """Insert or update a firm, return its ID."""
    _validate_columns(kwargs, _FIRMS_COLUMNS, "firms")
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
    _validate_columns(kwargs, _INVESTORS_COLUMNS, "investors")
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
    _validate_columns(kwargs, _DEALS_COLUMNS, "deals")
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


def upsert_deal_metadata(conn, deal_id: int, key: str, value: str):
    """Insert or update a key-value pair in deal_metadata."""
    conn.execute(
        "INSERT INTO deal_metadata (deal_id, key, value) VALUES (?, ?, ?) "
        "ON CONFLICT(deal_id, key) DO UPDATE SET value = excluded.value",
        (deal_id, key, value)
    )
    if not _is_batch(conn):
        conn.commit()


def get_deal_metadata(conn, deal_id: int, key: str = None) -> dict:
    """Return {key: value} dict for a deal. If key given, returns just that key."""
    if key is not None:
        row = conn.execute(
            "SELECT value FROM deal_metadata WHERE deal_id = ? AND key = ?",
            (deal_id, key)
        ).fetchone()
        return {key: row["value"]} if row else {}
    rows = conn.execute(
        "SELECT key, value FROM deal_metadata WHERE deal_id = ?", (deal_id,)
    ).fetchall()
    return {r["key"]: r["value"] for r in rows}


def upsert_portfolio_company(conn, firm_id: int, company_name: str, **kwargs) -> int:
    """Insert or update a portfolio company, return its ID."""
    _validate_columns(kwargs, _PORTFOLIO_COLUMNS, "portfolio_companies")
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


def create_user(conn, username: str, password_hash: str,
                display_name: str = None) -> dict:
    """Create a new user, return user dict."""
    cur = conn.execute(
        "INSERT INTO users (username, password_hash, display_name) VALUES (?, ?, ?)",
        (username, password_hash, display_name or username)
    )
    if not _is_batch(conn):
        conn.commit()
    return dict(conn.execute("SELECT * FROM users WHERE id = ?", (cur.lastrowid,)).fetchone())


def get_user_by_username(conn, username: str) -> Optional[dict]:
    """Look up a user by username. Returns dict or None."""
    row = conn.execute(
        "SELECT * FROM users WHERE username = ?", (username,)
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE users SET last_login_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), row["id"])
        )
        if not _is_batch(conn):
            conn.commit()
        return dict(row)
    return None


def get_user_preferences(conn, user_id: int) -> dict:
    """Get all preferences for a user as a dict of key->value (JSON-decoded)."""
    rows = conn.execute(
        "SELECT key, value FROM user_preferences WHERE user_id = ?", (user_id,)
    ).fetchall()
    import json as _json
    result = {}
    for row in rows:
        try:
            result[row["key"]] = _json.loads(row["value"])
        except (TypeError, ValueError):
            result[row["key"]] = row["value"]
    return result


def set_user_preferences(conn, user_id: int, prefs: dict):
    """Set multiple preferences for a user (upsert each key)."""
    import json as _json
    now = datetime.utcnow().isoformat()
    for key, value in prefs.items():
        encoded = _json.dumps(value) if not isinstance(value, str) else value
        conn.execute(
            "INSERT INTO user_preferences (user_id, key, value, updated_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(user_id, key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            (user_id, key, encoded, now)
        )
    if not _is_batch(conn):
        conn.commit()


def save_deal(conn, user_id: int, deal_id: int, folder: str = "Default",
              notes: str = None) -> int:
    """Bookmark a deal for a user. Returns saved_deals row id."""
    cur = conn.execute(
        "INSERT INTO saved_deals (user_id, deal_id, folder, notes) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(user_id, deal_id) DO UPDATE SET folder = excluded.folder, notes = excluded.notes",
        (user_id, deal_id, folder, notes)
    )
    if not _is_batch(conn):
        conn.commit()
    return cur.lastrowid


def unsave_deal(conn, user_id: int, deal_id: int):
    """Remove a bookmarked deal."""
    conn.execute(
        "DELETE FROM saved_deals WHERE user_id = ? AND deal_id = ?",
        (user_id, deal_id)
    )
    if not _is_batch(conn):
        conn.commit()


def update_saved_deal(conn, user_id: int, deal_id: int, folder: str = None,
                      notes: str = None):
    """Update folder/notes on a saved deal."""
    sets = []
    params = []
    if folder is not None:
        sets.append("folder = ?")
        params.append(folder)
    if notes is not None:
        sets.append("notes = ?")
        params.append(notes)
    if not sets:
        return
    params.extend([user_id, deal_id])
    conn.execute(
        f"UPDATE saved_deals SET {', '.join(sets)} WHERE user_id = ? AND deal_id = ?",
        params
    )
    if not _is_batch(conn):
        conn.commit()


def get_saved_deals(conn, user_id: int, folder: str = None) -> list:
    """Get saved deals for a user, optionally filtered by folder."""
    sql = """
        SELECT sd.*, d.company_name, d.company_description, d.stage,
               d.amount_usd, d.date_announced, d.source_type, d.source_url,
               c.name as category
        FROM saved_deals sd
        JOIN deals d ON sd.deal_id = d.id
        LEFT JOIN categories c ON d.category_id = c.id
        WHERE sd.user_id = ?
    """
    params = [user_id]
    if folder:
        sql += " AND sd.folder = ?"
        params.append(folder)
    sql += " ORDER BY sd.created_at DESC"
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def get_saved_deal_ids(conn, user_id: int) -> list:
    """Get just the deal IDs saved by a user (for rendering stars)."""
    rows = conn.execute(
        "SELECT deal_id FROM saved_deals WHERE user_id = ?", (user_id,)
    ).fetchall()
    return [r["deal_id"] for r in rows]


def get_saved_folders(conn, user_id: int) -> list:
    """Get folders with counts for a user."""
    rows = conn.execute(
        "SELECT folder, COUNT(*) as count FROM saved_deals WHERE user_id = ? GROUP BY folder ORDER BY folder",
        (user_id,)
    ).fetchall()
    return [dict(r) for r in rows]


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


def reset_stuck_scrape_logs(conn, max_age_hours: int = 2) -> int:
    """Reset scrape_logs stuck in 'running' status for more than max_age_hours."""
    updated = conn.execute(
        """UPDATE scrape_logs
           SET finished_at = ?, status = 'error',
               error_message = 'Reset from stuck state (exceeded timeout)'
           WHERE status = 'running'
             AND datetime(started_at) < datetime('now', ?)""",
        (datetime.utcnow().isoformat(), f"-{max_age_hours} hours")
    ).rowcount
    if updated:
        conn.commit()
        logger.info(f"Reset {updated} stuck scrape_log entries")
    return updated


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
        # Check if alleywatch is already allowed — try an insert inside savepoint
        try:
            conn.execute("SAVEPOINT migration_check")
            conn.execute(
                "INSERT INTO deals (company_name, source_type) VALUES ('__migration_probe__', 'alleywatch')"
            )
            # Rollback removes the probe row — no DELETE needed
            conn.execute("ROLLBACK TO migration_check")
            conn.execute("RELEASE migration_check")
            logger.info("Schema already up to date")
            conn.close()
            return
        except sqlite3.IntegrityError:
            conn.execute("ROLLBACK TO migration_check")
            conn.execute("RELEASE migration_check")
            # Need to recreate the table for CHECK constraint
        except sqlite3.Error as e:
            logger.debug(f"Migration check error: {e}")

    logger.info("Running migration...")

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
            'de_filing', 'alleywatch', 'google_news', 'ny_dos', 'other'
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
    logger.info(f"Migration complete: updated CHECK constraint, "
                f"added company_name_normalized, backfilled {len(rows)} rows")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "migrate":
        migrate_db()
    else:
        init_db()


