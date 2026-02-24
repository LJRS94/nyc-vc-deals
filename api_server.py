"""
NYC VC Deal Scraper — REST API Server
Serves deal data to the React dashboard.
"""

import os
import sys
import threading
import logging
from functools import wraps
from datetime import datetime, timedelta
from flask import Flask, g, jsonify, request, send_from_directory, session
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import SECRET_KEY, API_PORT, API_HOST, STARTUP_SCRAPE_DELAY, STARTUP_PORTFOLIO_DELAY
from database import (
    get_connection, init_db, create_user, get_user_by_username,
    get_user_preferences, set_user_preferences,
    save_deal, unsave_deal, update_saved_deal,
    get_saved_deals, get_saved_deal_ids, get_saved_folders,
    reset_stuck_scrape_logs, backup_db,
)
from werkzeug.security import generate_password_hash, check_password_hash

# ── Blueprints ──
from routes.deals import deals_bp
from routes.firms import firms_bp
from routes.feed import feed_bp
from routes.qc import qc_bp
from routes.verified import verified_bp

app = Flask(__name__)
CORS(app)
limiter = Limiter(get_remote_address, app=app, default_limits=["200 per hour"],
                  storage_uri="memory://")

# ── Request size limit (16 MB) ──
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

# ── Auth Configuration ──
app.secret_key = SECRET_KEY
app.config.update(
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=not os.environ.get("FLASK_DEBUG"),
)


# ── Per-request database connection via g.db ──
@app.before_request
def _open_db():
    g.db = get_connection()


@app.teardown_appcontext
def _close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        try:
            db.execute("SELECT 1")  # check if still alive before closing
        except Exception:
            pass

# Register blueprints
app.register_blueprint(deals_bp)
app.register_blueprint(firms_bp)
app.register_blueprint(feed_bp)
app.register_blueprint(qc_bp)
app.register_blueprint(verified_bp)


# ── Global JSON error handlers ──
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def internal_error(e):
    logger.error(f"Internal server error: {e}")
    return jsonify({"error": "Internal server error"}), 500

@app.errorhandler(400)
def bad_request(e):
    return jsonify({"error": str(e)}), 400


def login_required(f):
    """Decorator — returns 401 if no valid session. Only for new endpoints."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return jsonify({"error": "Login required"}), 401
        return f(*args, **kwargs)
    return wrapper


@app.route("/health")
def health_check():
    try:
        conn = g.db
        count = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
        # Last successful scrape
        last_scrape = conn.execute(
            "SELECT finished_at, deals_new FROM scrape_logs "
            "WHERE status = 'success' ORDER BY finished_at DESC LIMIT 1"
        ).fetchone()
        # Deal count trend (last 7 days vs prior 7 days)
        recent = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE created_at >= datetime('now', '-7 days')"
        ).fetchone()[0]
        prior = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE created_at >= datetime('now', '-14 days') "
            "AND created_at < datetime('now', '-7 days')"
        ).fetchone()[0]
        return jsonify({
            "status": "ok",
            "deals": count,
            "last_scrape": dict(last_scrape) if last_scrape else None,
            "deals_last_7d": recent,
            "deals_prior_7d": prior,
            "scrape_running": _scrape_status["running"],
        })
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 503


@app.route("/")
def serve_dashboard():
    return send_from_directory(os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates"), "dashboard.html")


# ── Auth Routes ──────────────────────────────────────────────

@app.route("/auth/register", methods=["POST"])
def auth_register():
    data = request.get_json(force=True)
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    display_name = (data.get("display_name") or "").strip() or username
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400
    if len(username) < 3:
        return jsonify({"error": "Username must be at least 3 characters"}), 400
    if len(password) < 8:
        return jsonify({"error": "Password must be at least 8 characters"}), 400
    conn = g.db
    existing = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
    if existing:
        return jsonify({"error": "Username already taken"}), 409
    user = create_user(conn, username, generate_password_hash(password), display_name)
    session["user_id"] = user["id"]
    session["user_name"] = user["display_name"]
    return jsonify({"ok": True, "user": {"id": user["id"], "username": user["username"], "name": user["display_name"]}})


@app.route("/auth/login", methods=["POST"])
def auth_login():
    data = request.get_json(force=True)
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400
    conn = g.db
    user = get_user_by_username(conn, username)
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"error": "Invalid username or password"}), 401
    session["user_id"] = user["id"]
    session["user_name"] = user["display_name"]
    return jsonify({"ok": True, "user": {"id": user["id"], "username": user["username"], "name": user["display_name"]}})


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/me")
def api_me():
    if "user_id" in session:
        return jsonify({
            "logged_in": True,
            "id": session["user_id"],
            "name": session.get("user_name"),
        })
    return jsonify({"logged_in": False})


# ── Preferences Endpoints ────────────────────────────────────

@app.route("/api/preferences", methods=["GET"])
@login_required
def api_get_preferences():
    conn = g.db
    prefs = get_user_preferences(conn, session["user_id"])
    return jsonify(prefs)


@app.route("/api/preferences", methods=["PUT"])
@login_required
def api_set_preferences():
    data = request.get_json(force=True)
    conn = g.db
    set_user_preferences(conn, session["user_id"], data)
    return jsonify({"ok": True})


# ── Saved Deals Endpoints ────────────────────────────────────

@app.route("/api/saved", methods=["GET"])
@login_required
def api_get_saved():
    folder = request.args.get("folder")
    conn = g.db
    deals = get_saved_deals(conn, session["user_id"], folder)
    return jsonify({"deals": deals})


@app.route("/api/saved", methods=["POST"])
@login_required
def api_save_deal():
    data = request.get_json(force=True)
    deal_id = data.get("deal_id")
    if not deal_id:
        return jsonify({"error": "deal_id required"}), 400
    conn = g.db
    row_id = save_deal(conn, session["user_id"], deal_id,
                       folder=data.get("folder", "Default"),
                       notes=data.get("notes"))
    return jsonify({"ok": True, "id": row_id})


@app.route("/api/saved/<int:deal_id>", methods=["PUT"])
@login_required
def api_update_saved(deal_id):
    data = request.get_json(force=True)
    conn = g.db
    update_saved_deal(conn, session["user_id"], deal_id,
                      folder=data.get("folder"),
                      notes=data.get("notes"))
    return jsonify({"ok": True})


@app.route("/api/saved/<int:deal_id>", methods=["DELETE"])
@login_required
def api_unsave_deal(deal_id):
    conn = g.db
    unsave_deal(conn, session["user_id"], deal_id)
    return jsonify({"ok": True})


@app.route("/api/saved/folders", methods=["GET"])
@login_required
def api_saved_folders():
    conn = g.db
    folders = get_saved_folders(conn, session["user_id"])
    return jsonify({"folders": folders})


@app.route("/api/saved/ids", methods=["GET"])
@login_required
def api_saved_ids():
    conn = g.db
    ids = get_saved_deal_ids(conn, session["user_id"])
    return jsonify({"ids": ids})


# ── Notifications ─────────────────────────────────────────────

@app.route("/api/notifications", methods=["GET"])
@login_required
def api_get_notifications():
    conn = g.db
    rows = conn.execute(
        "SELECT * FROM notifications WHERE user_id = ? OR user_id IS NULL "
        "ORDER BY created_at DESC LIMIT 50", (session["user_id"],)
    ).fetchall()
    unread = conn.execute(
        "SELECT COUNT(*) FROM notifications WHERE (user_id = ? OR user_id IS NULL) AND read = 0",
        (session["user_id"],)
    ).fetchone()[0]
    return jsonify({"notifications": [dict(r) for r in rows], "unread": unread})


@app.route("/api/notifications/read", methods=["POST"])
@login_required
def api_mark_notifications_read():
    conn = g.db
    conn.execute(
        "UPDATE notifications SET read = 1 WHERE (user_id = ? OR user_id IS NULL) AND read = 0",
        (session["user_id"],)
    )
    conn.commit()
    return jsonify({"ok": True})


# ── Background Scraping ──────────────────────────────────────

_scrape_lock = threading.Lock()
_scrape_status = {"running": False, "last_run": None, "last_result": None}
_scheduler_lock_fd = None  # held open to maintain file lock

logger = logging.getLogger("api_server")


def _enrich_firm_profiles(conn):
    """Auto-populate firm focus_sectors and focus_stages from their deal history."""
    import json
    rows = conn.execute("""
        SELECT f.id,
               GROUP_CONCAT(DISTINCT c.name) as sectors,
               GROUP_CONCAT(DISTINCT d.stage) as stages,
               COUNT(DISTINCT d.id) as deal_count,
               COALESCE(SUM(d.amount_usd), 0) as total_capital
        FROM firms f
        JOIN deal_firms df ON f.id = df.firm_id
        JOIN deals d ON df.deal_id = d.id
        LEFT JOIN categories c ON d.category_id = c.id
        GROUP BY f.id
        HAVING deal_count >= 1
    """).fetchall()
    updated = 0
    for r in rows:
        sectors = [s for s in (r["sectors"] or "").split(",") if s]
        stages = [s for s in (r["stages"] or "").split(",") if s]
        if sectors or stages:
            conn.execute("""
                UPDATE firms SET
                    focus_sectors = COALESCE(focus_sectors, ?),
                    focus_stages = COALESCE(focus_stages, ?),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND (focus_sectors IS NULL OR focus_stages IS NULL)
            """, (json.dumps(sectors), json.dumps(stages), r["id"]))
            updated += 1
    conn.commit()
    if updated:
        logger.info(f"Enriched {updated} firm profiles from deal patterns")


def _generate_notifications(conn):
    """Generate notifications: follow-on rounds for saved deals, new matches for preferences."""
    import json

    # 1. Follow-on rounds — notify users when a saved company raises again
    new_deals_24h = conn.execute("""
        SELECT d.id, d.company_name, d.company_name_normalized, d.stage, d.amount_usd
        FROM deals d WHERE d.created_at >= datetime('now', '-24 hours')
    """).fetchall()

    for deal in new_deals_24h:
        # Check if any user has a saved deal with the same normalized company name
        saved_users = conn.execute("""
            SELECT DISTINCT sd.user_id FROM saved_deals sd
            JOIN deals d2 ON sd.deal_id = d2.id
            WHERE d2.company_name_normalized = ? AND d2.id != ?
        """, (deal["company_name_normalized"], deal["id"])).fetchall()
        for u in saved_users:
            existing = conn.execute(
                "SELECT 1 FROM notifications WHERE user_id = ? AND deal_id = ? AND type = 'follow_on'",
                (u["user_id"], deal["id"])
            ).fetchone()
            if not existing:
                conn.execute(
                    "INSERT INTO notifications (user_id, type, title, body, deal_id) VALUES (?,?,?,?,?)",
                    (u["user_id"], "follow_on",
                     f"{deal['company_name']} raised {deal['stage']}",
                     f"A company in your saved list raised a new round" +
                     (f" — ${deal['amount_usd']/1e6:.0f}M" if deal["amount_usd"] else ""),
                     deal["id"])
                )

    # 2. Preference matches — notify users when new deals match their feed preferences
    users = conn.execute("SELECT DISTINCT user_id FROM user_preferences").fetchall()
    for u in users:
        uid = u["user_id"]
        prefs_rows = conn.execute(
            "SELECT key, value FROM user_preferences WHERE user_id = ?", (uid,)
        ).fetchall()
        prefs = {r["key"]: r["value"] for r in prefs_rows}
        sectors = json.loads(prefs.get("sectors", "[]"))
        stages = json.loads(prefs.get("stages", "[]"))
        if not sectors and not stages:
            continue
        for deal in new_deals_24h:
            cat = conn.execute(
                "SELECT c.name FROM categories c JOIN deals d ON d.category_id = c.id WHERE d.id = ?",
                (deal["id"],)
            ).fetchone()
            cat_name = cat["name"] if cat else None
            match = False
            if sectors and cat_name and cat_name in sectors:
                match = True
            if stages and deal["stage"] in stages:
                match = True
            if match:
                existing = conn.execute(
                    "SELECT 1 FROM notifications WHERE user_id = ? AND deal_id = ? AND type = 'new_match'",
                    (uid, deal["id"])
                ).fetchone()
                if not existing:
                    conn.execute(
                        "INSERT INTO notifications (user_id, type, title, body, deal_id) VALUES (?,?,?,?,?)",
                        (uid, "new_match",
                         f"New {deal['stage']}: {deal['company_name']}",
                         f"Matches your feed preferences" + (f" ({cat_name})" if cat_name else ""),
                         deal["id"])
                    )
    conn.commit()
    count = conn.execute(
        "SELECT COUNT(*) FROM notifications WHERE created_at >= datetime('now', '-24 hours')"
    ).fetchone()[0]
    if count:
        logger.info(f"Generated {count} notifications")


def _run_scrape_background(days_back: int = 30):
    """Run a scrape in a background thread."""
    if not _scrape_lock.acquire(blocking=False):
        return  # already running
    try:
        _scrape_status["running"] = True
        _scrape_status["last_run"] = datetime.now().isoformat()
        logger.info("Background scrape starting...")

        # Reset any stuck scrape_logs from prior crashes
        try:
            conn_reset = get_connection()
            reset_stuck_scrape_logs(conn_reset)
        except Exception as e:
            logger.debug(f"Stuck log reset warning: {e}")

        from scrapers.news_scraper import run_news_scraper
        from scrapers.alleywatch_scraper import run_alleywatch_scraper
        from scrapers.sec_scraper import run_sec_scraper
        from scrapers.firm_scraper import seed_firms
        from scrapers.utils import clear_firm_cache

        # Seed firms (including firms.json with 100 firms)
        try:
            seed_firms()
            clear_firm_cache()  # refresh cache after seeding
        except Exception as e:
            logger.warning(f"Firm seeding warning: {e}")

        # Clean up VC firms mistakenly listed as startups (single SQL using firm names)
        conn = get_connection()
        try:
            firm_names = [r["name"] for r in conn.execute("SELECT name FROM firms").fetchall()]
            if firm_names:
                from database import _normalize_name
                normalized = [_normalize_name(n) for n in firm_names]
                ph = ",".join(["?"] * len(normalized))
                vc_ids = [r[0] for r in conn.execute(
                    f"SELECT id FROM deals WHERE company_name_normalized IN ({ph})", normalized
                ).fetchall()]
                if vc_ids:
                    id_ph = ",".join(["?"] * len(vc_ids))
                    conn.execute(f"DELETE FROM deal_firms WHERE deal_id IN ({id_ph})", vc_ids)
                    conn.execute(f"DELETE FROM deal_investors WHERE deal_id IN ({id_ph})", vc_ids)
                    conn.execute(f"DELETE FROM deals WHERE id IN ({id_ph})", vc_ids)
                    conn.commit()
                    logger.info(f"Cleanup: removed {len(vc_ids)} VC-firm deals")
        except Exception as e:
            logger.warning(f"Cleanup warning: {e}")

        # Initialize QC tables
        try:
            from quality_control import init_qc_tables, run_audit, update_auto_reject_patterns, merge_cross_source_duplicates
            init_qc_tables(conn)
        except Exception as e:
            logger.warning(f"QC init warning: {e}")

        # Run the main scrapers (all funnel through validate_deal() quality gate)
        run_news_scraper(days_back=days_back)
        run_alleywatch_scraper(days_back=days_back)

        # SEC EDGAR Form D filings (free public data)
        try:
            run_sec_scraper(days_back=days_back)
        except Exception as e:
            logger.warning(f"SEC scraper warning: {e}")

        # Post-scrape: cross-source dedup + QC audit
        conn = get_connection()

        try:
            merged = merge_cross_source_duplicates(conn)
            if merged:
                logger.info(f"Cross-source dedup removed {merged} duplicates")
        except Exception as e:
            logger.warning(f"Cross-source dedup warning: {e}")

        deal_count = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]

        # Enrich firm profiles from deal patterns
        try:
            _enrich_firm_profiles(conn)
        except Exception as e:
            logger.warning(f"Firm enrichment warning: {e}")

        try:
            # Auto-promote frequently-rejected patterns to auto-reject
            update_auto_reject_patterns(conn)
            # Run audit and log results
            audit = run_audit(conn)
            logger.info(
                f"QC audit: {audit['total_deals']} deals, "
                f"{audit['total_issues']} issues, "
                f"health={audit['health_score']}"
            )
        except Exception as e:
            logger.warning(f"QC audit warning: {e}")

        # Generate notifications for follow-on rounds and preference matches
        try:
            _generate_notifications(conn)
        except Exception as e:
            logger.warning(f"Notification generation warning: {e}")

        # Backup DB after successful scrape
        try:
            backup_db()
        except Exception as e:
            logger.warning(f"Database backup warning: {e}")

        _scrape_status["last_result"] = f"Completed. {deal_count} total deals."
        logger.info(f"Background scrape complete: {deal_count} deals")

    except Exception as e:
        _scrape_status["last_result"] = f"Error: {e}"
        logger.error(f"Background scrape failed: {e}")
    finally:
        _scrape_status["running"] = False
        _scrape_lock.release()


def _run_portfolio_scrape():
    """Run portfolio scraper in a background thread."""
    if not _scrape_lock.acquire(blocking=False):
        return
    try:
        _scrape_status["running"] = True
        _scrape_status["last_run"] = datetime.now().isoformat()
        logger.info("Portfolio scrape starting...")

        from scrapers.firm_scraper import seed_firms, run_portfolio_scraper
        seed_firms()
        run_portfolio_scraper()

        conn = get_connection()
        pc_count = conn.execute("SELECT COUNT(*) FROM portfolio_companies").fetchone()[0]

        # Auto-verify deal-firm links against portfolio data
        try:
            from routes.verified import run_portfolio_verification
            vresult = run_portfolio_verification(conn)
            logger.info(
                f"Portfolio verification: {vresult['verified']} verified, "
                f"{vresult['inserted']} new links"
            )
        except Exception as e:
            logger.warning(f"Portfolio verification warning: {e}")

        # Backup DB after successful portfolio scrape
        try:
            backup_db()
        except Exception as e:
            logger.warning(f"Database backup warning: {e}")

        _scrape_status["last_result"] = f"Portfolio scrape done. {pc_count} companies."
        logger.info(f"Portfolio scrape complete: {pc_count} companies")

    except Exception as e:
        _scrape_status["last_result"] = f"Portfolio error: {e}"
        logger.error(f"Portfolio scrape failed: {e}")
    finally:
        _scrape_status["running"] = False
        _scrape_lock.release()


def _start_scheduler():
    """Start background threads: deals on startup + Sunday 9 PM EST, portfolio on Friday 9 PM EST.

    Uses a file lock so only one gunicorn worker runs the scheduler.
    """
    import time
    import fcntl

    # Only one worker should run the scheduler — use a file lock
    lock_path = os.path.join(
        os.environ.get("DATABASE_PATH", ""), "..", ".scheduler.lock"
    ) if os.environ.get("DATABASE_PATH") else "/tmp/.vc_scheduler.lock"
    lock_path = os.path.normpath(lock_path)
    global _scheduler_lock_fd
    try:
        _scheduler_lock_fd = open(lock_path, "w")
        fcntl.flock(_scheduler_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, OSError):
        logger.info("Another worker owns the scheduler — skipping")
        return

    def deals_scheduler():
        time.sleep(STARTUP_SCRAPE_DELAY)
        _run_scrape_background()

        while True:
            now = datetime.utcnow()
            days_until_monday = (7 - now.weekday()) % 7
            if days_until_monday == 0 and now.hour >= 2:
                days_until_monday = 7
            next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
            next_run = next_run + timedelta(days=days_until_monday)
            wait_seconds = (next_run - now).total_seconds()
            if wait_seconds < 0:
                wait_seconds += 7 * 24 * 3600
            logger.info(f"Next deal scrape in {wait_seconds/3600:.1f}h (Sunday 9 PM EST)")
            time.sleep(wait_seconds)
            _run_scrape_background()

    def portfolio_scheduler():
        time.sleep(STARTUP_PORTFOLIO_DELAY)
        _run_portfolio_scrape()

        while True:
            now = datetime.utcnow()
            days_until_saturday = (5 - now.weekday()) % 7
            if days_until_saturday == 0 and now.hour >= 2:
                days_until_saturday = 7
            next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
            next_run = next_run + timedelta(days=days_until_saturday)
            wait_seconds = (next_run - now).total_seconds()
            if wait_seconds < 0:
                wait_seconds += 7 * 24 * 3600
            logger.info(f"Next portfolio scrape in {wait_seconds/3600:.1f}h (Friday 9 PM EST)")
            time.sleep(wait_seconds)
            _run_portfolio_scrape()

    t1 = threading.Thread(target=deals_scheduler, daemon=True)
    t1.start()
    t2 = threading.Thread(target=portfolio_scheduler, daemon=True)
    t2.start()
    logger.info("Scheduled: deals (startup + Sunday 9 PM EST), portfolio (startup + Friday 9 PM EST)")


@app.route("/api/scrape", methods=["POST"])
@login_required
@limiter.limit("2 per hour")
def trigger_scrape():
    """Manually trigger a background scrape (requires login, rate limited)."""
    if _scrape_status["running"]:
        return jsonify({"status": "already_running", **_scrape_status}), 409
    data = request.get_json(silent=True) or {}
    days_back = min(int(data.get("days_back", 30)), 730)
    threading.Thread(target=_run_scrape_background, args=(days_back,), daemon=True).start()
    return jsonify({"status": "started", "message": f"Scrape started (days_back={days_back})"})


@app.route("/api/scrape/status", methods=["GET"])
def scrape_status():
    """Check the status of background scraping."""
    conn = g.db
    deal_count = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
    return jsonify({**_scrape_status, "total_deals": deal_count})


# ── One-time data cleanup migration ──────────────────────────
def _run_data_cleanup():
    """Clean junk entities and duplicates from the database.
    Safe to run multiple times — idempotent.
    """
    import re
    conn = get_connection()

    # 1. Strip CIK suffixes from company names
    cik_re = re.compile(r"\s*\(CIK\s*\d+\)\s*$", re.I)
    legal_re = re.compile(r",?\s*(Inc\.?|LLC|L\.?P\.?|Corp\.?|Ltd\.?)$", re.I)
    cik_deals = conn.execute(
        "SELECT id, company_name FROM deals WHERE company_name LIKE '%CIK%'"
    ).fetchall()
    if cik_deals:
        from database import _normalize_name
        for deal in cik_deals:
            name = cik_re.sub("", deal["company_name"]).strip()
            name = legal_re.sub("", name).strip()
            if name:
                conn.execute(
                    "UPDATE deals SET company_name = ?, company_name_normalized = ? WHERE id = ?",
                    (name, _normalize_name(name), deal["id"]),
                )
            else:
                conn.execute("DELETE FROM deals WHERE id = ?", (deal["id"],))
        conn.commit()
        logger.info(f"Cleanup: fixed {len(cik_deals)} CIK-tainted company names")

    # 2. Remove non-corporation entities (LPs, LLCs = fund vehicles, not startups)
    fund_re = re.compile(
        r"\b(Fund|Feeder|Offshore|Holdings|Capital Partners|Capital,?\s*L\.?P|"
        r"Equity Fund|Investment Fund|Coinvestment|"
        r"Aggregator|Master Portfolio|"
        r"Asset Backed|BDC|Ventures?\s+[IVXLC]+\b|"
        r"DST\b|REIT|Trust\b|"
        r"Investors?\b|Partners,?\s*L\.?P|Deep Value|"
        r"Bioventures|Private Equity|Public Markets|Selector)\b",
        re.I,
    )
    junk = conn.execute(
        "SELECT id, company_name FROM deals WHERE source_type IN ('sec_filing', 'de_filing')"
    ).fetchall()
    removed = 0
    for deal in junk:
        if fund_re.search(deal["company_name"]):
            conn.execute("DELETE FROM deal_firms WHERE deal_id = ?", (deal["id"],))
            conn.execute("DELETE FROM deal_investors WHERE deal_id = ?", (deal["id"],))
            conn.execute("DELETE FROM deal_metadata WHERE deal_id = ?", (deal["id"],))
            conn.execute("DELETE FROM deals WHERE id = ?", (deal["id"],))
            removed += 1
    if removed:
        conn.commit()
        logger.info(f"Cleanup: removed {removed} fund vehicle deals")

    # 3. Run cross-source dedup (handles same-deal-different-stage duplicates)
    try:
        from quality_control import merge_cross_source_duplicates
        merged = merge_cross_source_duplicates(conn)
        if merged:
            logger.info(f"Cleanup: dedup removed {merged} duplicate deals")
    except Exception as e:
        logger.warning(f"Dedup cleanup warning: {e}")

    # 4. Clean junk portfolio company entries
    junk_portfolio_conditions = [
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
        # Concatenated metadata patterns
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
        # Social/UI link junk
        "company_name LIKE '%Link opens in new tab%'",
        "company_name LIKE 'Spotlight%' AND company_name LIKE '%:%'",
        "company_name LIKE 'Filter%' AND LENGTH(company_name) > 10",
        # City+category concatenations
        "company_name LIKE 'Austin, TX%' AND LENGTH(company_name) > 12",
        "company_name LIKE 'Boston, MA%' AND LENGTH(company_name) > 12",
        "company_name LIKE 'San Francisco, CA%' AND LENGTH(company_name) > 20",
        "company_name LIKE 'Tel Aviv, Israel%' AND LENGTH(company_name) > 18",
        "company_name LIKE 'New York, NY%' AND LENGTH(company_name) > 14",
        "company_name LIKE 'London, UK%' AND LENGTH(company_name) > 12",
        "company_name LIKE 'Toronto, Canada%' AND LENGTH(company_name) > 18",
        "company_name LIKE 'Washington DC%' AND LENGTH(company_name) > 15",
        # Stock tickers
        "company_name LIKE 'NASDAQ%'",
        "company_name LIKE 'NYSE%'",
        # Enterprise/Saas concatenations
        "company_name LIKE '%Enterprise/Saas%'",
        # Uppercase UI junk
        "company_name IN ('COMPANY↑','PLAY VIDEO','VIEW LEGAL DISCLOSURES','BROWSE OUR—PORTFOLIO','NVP PROMISE','ENTERPRISE WEEKLY NEWSLETTER')",
    ]
    pc_removed = 0
    for cond in junk_portfolio_conditions:
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM portfolio_companies WHERE " + cond
            ).fetchone()[0]
            if count > 0:
                conn.execute("DELETE FROM portfolio_companies WHERE " + cond)
                pc_removed += count
        except Exception:
            pass

    # Clean nav/UI junk from portfolio (exact match, case insensitive)
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

    # Fix "ExitsTrue/ExitsFalse" suffixes on company names
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
                from database import _normalize_name
                conn.execute(
                    "UPDATE portfolio_companies SET company_name = ?, company_name_normalized = ? WHERE id = ?",
                    (cleaned, _normalize_name(cleaned), r["id"]),
                )
        else:
            conn.execute("DELETE FROM portfolio_companies WHERE id = ?", (r["id"],))
        pc_removed += 1

    # Fix "(Acquired)" and "(Exited)" tags on company names
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
                    from database import _normalize_name
                    conn.execute(
                        "UPDATE portfolio_companies SET company_name = ?, company_name_normalized = ? WHERE id = ?",
                        (cleaned, _normalize_name(cleaned), r["id"]),
                    )
            pc_removed += 1

    # Delete description-like entries (long phrases that are clearly not company names)
    desc_rows = conn.execute(
        "SELECT id, company_name FROM portfolio_companies WHERE LENGTH(company_name) > 35"
    ).fetchall()
    for r in desc_rows:
        name = r["company_name"]
        words = name.split()
        # Multi-word descriptive phrases (4+ words, starts with cap, rest lowercase)
        if len(words) >= 5 and not re.search(r'\(', name):
            lower_words = [w for w in words[1:] if w[0].islower() or w in ('in', 'for', 'of', 'the', 'and', 'a', 'to', 'an', '&')]
            if len(lower_words) >= len(words) - 2:
                conn.execute("DELETE FROM portfolio_companies WHERE id = ?", (r["id"],))
                pc_removed += 1
        # Description ending with common suffixes
        elif name.endswith(('Platform', 'Solution', 'Solutions')) and len(name) > 30:
            conn.execute("DELETE FROM portfolio_companies WHERE id = ?", (r["id"],))
            pc_removed += 1

    if pc_removed:
        conn.commit()
        logger.info(f"Cleanup: removed/fixed {pc_removed} junk portfolio entries")


# Start the scheduler when running under gunicorn (production)
if not os.environ.get("FLASK_DEBUG"):
    _run_data_cleanup()
    _start_scheduler()


if __name__ == "__main__":
    init_db()
    _run_data_cleanup()
    debug = os.environ.get("FLASK_DEBUG", "false").lower() in ("1", "true", "yes")
    if not debug:
        _start_scheduler()
    app.run(debug=debug, host=API_HOST, port=API_PORT)
