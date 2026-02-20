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
from flask import Flask, jsonify, request, send_from_directory, session
from flask_cors import CORS

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import SECRET_KEY, API_PORT, API_HOST
from database import (
    get_connection, init_db, create_user, get_user_by_username,
    get_user_preferences, set_user_preferences,
    save_deal, unsave_deal, update_saved_deal,
    get_saved_deals, get_saved_deal_ids, get_saved_folders,
)
from werkzeug.security import generate_password_hash, check_password_hash

# ── Blueprints ──
from routes.deals import deals_bp
from routes.firms import firms_bp
from routes.feed import feed_bp
from routes.qc import qc_bp

app = Flask(__name__)
CORS(app)

# ── Auth Configuration ──
app.secret_key = SECRET_KEY
app.config.update(
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=not os.environ.get("FLASK_DEBUG"),
)

# Register blueprints
app.register_blueprint(deals_bp)
app.register_blueprint(firms_bp)
app.register_blueprint(feed_bp)
app.register_blueprint(qc_bp)


def login_required(f):
    """Decorator — returns 401 if no valid session. Only for new endpoints."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return jsonify({"error": "Login required"}), 401
        return f(*args, **kwargs)
    return wrapper


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
    if len(password) < 4:
        return jsonify({"error": "Password must be at least 4 characters"}), 400
    conn = get_connection()
    existing = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
    if existing:
        conn.close()
        return jsonify({"error": "Username already taken"}), 409
    user = create_user(conn, username, generate_password_hash(password), display_name)
    conn.close()
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
    conn = get_connection()
    user = get_user_by_username(conn, username)
    conn.close()
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
    conn = get_connection()
    prefs = get_user_preferences(conn, session["user_id"])
    conn.close()
    return jsonify(prefs)


@app.route("/api/preferences", methods=["PUT"])
@login_required
def api_set_preferences():
    data = request.get_json(force=True)
    conn = get_connection()
    set_user_preferences(conn, session["user_id"], data)
    conn.close()
    return jsonify({"ok": True})


# ── Saved Deals Endpoints ────────────────────────────────────

@app.route("/api/saved", methods=["GET"])
@login_required
def api_get_saved():
    folder = request.args.get("folder")
    conn = get_connection()
    deals = get_saved_deals(conn, session["user_id"], folder)
    conn.close()
    return jsonify({"deals": deals})


@app.route("/api/saved", methods=["POST"])
@login_required
def api_save_deal():
    data = request.get_json(force=True)
    deal_id = data.get("deal_id")
    if not deal_id:
        return jsonify({"error": "deal_id required"}), 400
    conn = get_connection()
    row_id = save_deal(conn, session["user_id"], deal_id,
                       folder=data.get("folder", "Default"),
                       notes=data.get("notes"))
    conn.close()
    return jsonify({"ok": True, "id": row_id})


@app.route("/api/saved/<int:deal_id>", methods=["PUT"])
@login_required
def api_update_saved(deal_id):
    data = request.get_json(force=True)
    conn = get_connection()
    update_saved_deal(conn, session["user_id"], deal_id,
                      folder=data.get("folder"),
                      notes=data.get("notes"))
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/saved/<int:deal_id>", methods=["DELETE"])
@login_required
def api_unsave_deal(deal_id):
    conn = get_connection()
    unsave_deal(conn, session["user_id"], deal_id)
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/saved/folders", methods=["GET"])
@login_required
def api_saved_folders():
    conn = get_connection()
    folders = get_saved_folders(conn, session["user_id"])
    conn.close()
    return jsonify({"folders": folders})


@app.route("/api/saved/ids", methods=["GET"])
@login_required
def api_saved_ids():
    conn = get_connection()
    ids = get_saved_deal_ids(conn, session["user_id"])
    conn.close()
    return jsonify({"ids": ids})


# ── Background Scraping ──────────────────────────────────────

_scrape_lock = threading.Lock()
_scrape_status = {"running": False, "last_run": None, "last_result": None}

logger = logging.getLogger("api_server")


def _run_scrape_background():
    """Run a scrape in a background thread."""
    if not _scrape_lock.acquire(blocking=False):
        return  # already running
    try:
        _scrape_status["running"] = True
        _scrape_status["last_run"] = datetime.now().isoformat()
        logger.info("Background scrape starting...")

        from scrapers.news_scraper import run_news_scraper
        from scrapers.alleywatch_scraper import run_alleywatch_scraper
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
        finally:
            conn.close()

        # Initialize QC tables
        try:
            from quality_control import init_qc_tables, run_audit, update_auto_reject_patterns
            conn = get_connection()
            init_qc_tables(conn)
            conn.close()
        except Exception as e:
            logger.warning(f"QC init warning: {e}")

        # Run the main scrapers (all funnel through validate_deal() quality gate)
        run_news_scraper(days_back=180)
        run_alleywatch_scraper(days_back=180)

        # Post-scrape QC audit
        conn = get_connection()
        deal_count = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]

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
        finally:
            conn.close()

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
        conn.close()

        _scrape_status["last_result"] = f"Portfolio scrape done. {pc_count} companies."
        logger.info(f"Portfolio scrape complete: {pc_count} companies")

    except Exception as e:
        _scrape_status["last_result"] = f"Portfolio error: {e}"
        logger.error(f"Portfolio scrape failed: {e}")
    finally:
        _scrape_status["running"] = False
        _scrape_lock.release()


def _start_scheduler():
    """Start background threads: deals on startup + Sunday 9 PM EST, portfolio on Friday 9 PM EST."""
    import time

    def deals_scheduler():
        time.sleep(60)
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
        time.sleep(120)
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
def trigger_scrape():
    """Manually trigger a background scrape."""
    if _scrape_status["running"]:
        return jsonify({"status": "already_running", **_scrape_status}), 409
    threading.Thread(target=_run_scrape_background, daemon=True).start()
    return jsonify({"status": "started", "message": "Scrape started in background"})


@app.route("/api/scrape/status", methods=["GET"])
def scrape_status():
    """Check the status of background scraping."""
    conn = get_connection()
    deal_count = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
    conn.close()
    return jsonify({**_scrape_status, "total_deals": deal_count})


# Start the scheduler when running under gunicorn (production)
if not os.environ.get("FLASK_DEBUG"):
    _start_scheduler()


if __name__ == "__main__":
    init_db()
    debug = os.environ.get("FLASK_DEBUG", "false").lower() in ("1", "true", "yes")
    if not debug:
        _start_scheduler()
    app.run(debug=debug, host=API_HOST, port=API_PORT)
