"""
NYC VC Deal Scraper — REST API Server
Serves deal data to the React dashboard.
"""

import os
import sys
import json
import threading
import logging
from functools import wraps
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, send_from_directory, session
from flask_cors import CORS

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import (
    get_connection, init_db, create_user, get_user_by_username,
    get_user_preferences, set_user_preferences,
    save_deal, unsave_deal, update_saved_deal,
    get_saved_deals, get_saved_deal_ids, get_saved_folders,
)
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
CORS(app)

# ── Auth Configuration ──
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-me")
app.config.update(
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=not os.environ.get("FLASK_DEBUG"),
)


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


# ── Existing Public Endpoints ────────────────────────────────

@app.route("/api/stats", methods=["GET"])
def get_stats():
    """Dashboard overview stats."""
    conn = get_connection()
    stats = {
        "total_deals": conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0],
        "total_firms": conn.execute("SELECT COUNT(*) FROM firms").fetchone()[0],
        "total_investors": conn.execute("SELECT COUNT(*) FROM investors").fetchone()[0],
        "total_capital": conn.execute(
            "SELECT COALESCE(SUM(amount_usd), 0) FROM deals WHERE amount_usd IS NOT NULL"
        ).fetchone()[0],
        "avg_deal_size": conn.execute(
            "SELECT COALESCE(AVG(amount_usd), 0) FROM deals WHERE amount_usd IS NOT NULL"
        ).fetchone()[0],
        "de_incorporated_count": conn.execute(
            "SELECT COUNT(*) FROM deals WHERE source_type = 'de_filing' "
            "OR raw_text LIKE '%DE incorporated%' OR raw_text LIKE '%Delaware%'"
        ).fetchone()[0],
        "source_breakdown": {},
    }
    # Source breakdown
    for row in conn.execute(
        "SELECT source_type, COUNT(*) as cnt FROM deals GROUP BY source_type"
    ).fetchall():
        stats["source_breakdown"][row["source_type"]] = row["cnt"]
    conn.close()
    return jsonify(stats)


@app.route("/api/deals", methods=["GET"])
def get_deals():
    """List deals with filtering and pagination."""
    conn = get_connection()

    # Query params
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 25))
    stage = request.args.get("stage")
    category = request.args.get("category")
    firm = request.args.get("firm")
    search = request.args.get("q")
    sort_by = request.args.get("sort", "date_announced")
    sort_dir = request.args.get("dir", "DESC")

    # Build query
    where_clauses = []
    params = []

    if stage:
        where_clauses.append("d.stage = ?")
        params.append(stage)
    if category:
        where_clauses.append("c.name = ?")
        params.append(category)
    if firm:
        where_clauses.append("f.name LIKE ?")
        params.append(f"%{firm}%")
    if search:
        where_clauses.append("(d.company_name LIKE ? OR d.raw_text LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    # Count total
    count_sql = f"""
        SELECT COUNT(DISTINCT d.id)
        FROM deals d
        LEFT JOIN categories c ON d.category_id = c.id
        LEFT JOIN deal_firms df ON d.id = df.deal_id
        LEFT JOIN firms f ON df.firm_id = f.id
        WHERE {where_sql}
    """
    total = conn.execute(count_sql, params).fetchone()[0]

    # Allowed sort columns
    sort_map = {
        "date_announced": "d.date_announced",
        "amount": "d.amount_usd",
        "company": "d.company_name",
        "stage": "d.stage",
        "created": "d.created_at",
    }
    order_col = sort_map.get(sort_by, "d.created_at")
    order_dir = "ASC" if sort_dir.upper() == "ASC" else "DESC"

    offset = (page - 1) * per_page
    data_sql = f"""
        SELECT DISTINCT
            d.id, d.company_name, d.company_website, d.company_description,
            d.stage, d.amount_usd, d.amount_disclosed,
            d.date_announced, d.source_type, d.source_url,
            d.confidence_score, d.created_at,
            c.name as category
        FROM deals d
        LEFT JOIN categories c ON d.category_id = c.id
        LEFT JOIN deal_firms df ON d.id = df.deal_id
        LEFT JOIN firms f ON df.firm_id = f.id
        WHERE {where_sql}
        ORDER BY {order_col} {order_dir} NULLS LAST
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(data_sql, params + [per_page, offset]).fetchall()

    # Batch-fetch firms and investors for all deals (avoids N+1 queries)
    deal_ids = [row["id"] for row in rows]
    firms_by_deal = {}
    investors_by_deal = {}
    if deal_ids:
        ph = ",".join(["?"] * len(deal_ids))
        for fr in conn.execute(f"""
            SELECT df.deal_id, f.name, f.website, df.role
            FROM deal_firms df JOIN firms f ON df.firm_id = f.id
            WHERE df.deal_id IN ({ph})
        """, deal_ids).fetchall():
            firms_by_deal.setdefault(fr["deal_id"], []).append(
                {"name": fr["name"], "website": fr["website"], "role": fr["role"]})
        for ir in conn.execute(f"""
            SELECT di.deal_id, i.name, i.title, f.name as firm_name
            FROM deal_investors di
            JOIN investors i ON di.investor_id = i.id
            LEFT JOIN firms f ON i.firm_id = f.id
            WHERE di.deal_id IN ({ph})
        """, deal_ids).fetchall():
            investors_by_deal.setdefault(ir["deal_id"], []).append(
                {"name": ir["name"], "title": ir["title"], "firm": ir["firm_name"]})

    deals = []
    for row in rows:
        deal_id = row["id"]
        deals.append({
            "id": deal_id,
            "company_name": row["company_name"],
            "company_website": row["company_website"],
            "description": row["company_description"],
            "stage": row["stage"],
            "amount_usd": row["amount_usd"],
            "amount_disclosed": bool(row["amount_disclosed"]),
            "date_announced": row["date_announced"],
            "category": row["category"],
            "source_type": row["source_type"],
            "source_url": row["source_url"],
            "confidence_score": row["confidence_score"],
            "firms": firms_by_deal.get(deal_id, []),
            "investors": investors_by_deal.get(deal_id, []),
        })

    conn.close()
    return jsonify({
        "deals": deals,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
    })


@app.route("/api/deals/by-stage", methods=["GET"])
def deals_by_stage():
    conn = get_connection()
    rows = conn.execute("""
        SELECT stage, COUNT(*) as count,
               COALESCE(SUM(amount_usd), 0) as total_amount,
               COALESCE(AVG(amount_usd), 0) as avg_amount
        FROM deals
        GROUP BY stage ORDER BY count DESC
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/deals/by-category", methods=["GET"])
def deals_by_category():
    conn = get_connection()
    rows = conn.execute("""
        SELECT c.name as category, COUNT(*) as count,
               COALESCE(SUM(d.amount_usd), 0) as total_amount
        FROM deals d
        JOIN categories c ON d.category_id = c.id
        GROUP BY c.name ORDER BY count DESC
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/deals/by-month", methods=["GET"])
def deals_by_month():
    conn = get_connection()
    rows = conn.execute("""
        SELECT
            strftime('%Y-%m', COALESCE(date_announced, created_at)) as month,
            COUNT(*) as count,
            COALESCE(SUM(amount_usd), 0) as total_amount
        FROM deals
        WHERE date_announced IS NOT NULL
        GROUP BY month ORDER BY month
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/deals/by-source", methods=["GET"])
def deals_by_source():
    """Breakdown of deals by data source (news, SEC, DE filings, etc)."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT source_type, COUNT(*) as count,
               COALESCE(SUM(amount_usd), 0) as total_amount,
               COALESCE(AVG(confidence_score), 0) as avg_confidence
        FROM deals
        GROUP BY source_type ORDER BY count DESC
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/deals/de-incorporated", methods=["GET"])
def deals_de_incorporated():
    """Deals from Delaware-incorporated companies (most VC-backed startups)."""
    conn = get_connection()
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 25))
    offset = (page - 1) * per_page

    # Deals from DE filings source OR with DE mention in raw_text
    rows = conn.execute("""
        SELECT DISTINCT d.id, d.company_name, d.stage, d.amount_usd,
               d.date_announced, d.source_type, d.confidence_score,
               c.name as category
        FROM deals d
        LEFT JOIN categories c ON d.category_id = c.id
        WHERE d.source_type = 'de_filing'
           OR d.raw_text LIKE '%DE incorporated%'
           OR d.raw_text LIKE '%Delaware%'
        ORDER BY d.date_announced DESC
        LIMIT ? OFFSET ?
    """, (per_page, offset)).fetchall()

    total = conn.execute("""
        SELECT COUNT(DISTINCT id) FROM deals
        WHERE source_type = 'de_filing'
           OR raw_text LIKE '%DE incorporated%'
           OR raw_text LIKE '%Delaware%'
    """).fetchone()[0]

    conn.close()
    return jsonify({
        "deals": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
    })


@app.route("/api/firms", methods=["GET"])
def get_firms():
    conn = get_connection()
    search = request.args.get("q", "")

    sql = """
        SELECT f.*, COUNT(DISTINCT df.deal_id) as deal_count,
               COALESCE(SUM(d.amount_usd), 0) as total_invested,
               COUNT(DISTINCT CASE WHEN df.role = 'lead' THEN df.deal_id END) as lead_count,
               (SELECT COUNT(*) FROM investors WHERE firm_id = f.id) as team_count,
               (SELECT COUNT(*) FROM portfolio_companies WHERE firm_id = f.id) as portfolio_count
        FROM firms f
        LEFT JOIN deal_firms df ON f.id = df.firm_id
        LEFT JOIN deals d ON df.deal_id = d.id
    """
    params = []
    if search:
        sql += " WHERE f.name LIKE ?"
        params.append(f"%{search}%")
    sql += " GROUP BY f.id ORDER BY deal_count DESC"

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/firms/<int:firm_id>", methods=["GET"])
def get_firm(firm_id):
    conn = get_connection()
    firm = conn.execute("SELECT * FROM firms WHERE id = ?", (firm_id,)).fetchone()
    if not firm:
        return jsonify({"error": "Firm not found"}), 404

    deals = conn.execute("""
        SELECT d.*, c.name as category, df.role
        FROM deals d
        JOIN deal_firms df ON d.id = df.deal_id
        LEFT JOIN categories c ON d.category_id = c.id
        WHERE df.firm_id = ?
        ORDER BY d.date_announced DESC
    """, (firm_id,)).fetchall()

    investors = conn.execute("""
        SELECT * FROM investors WHERE firm_id = ?
    """, (firm_id,)).fetchall()

    conn.close()
    return jsonify({
        "firm": dict(firm),
        "deals": [dict(d) for d in deals],
        "investors": [dict(i) for i in investors],
    })


@app.route("/api/investors", methods=["GET"])
def get_investors():
    conn = get_connection()
    firm_id = request.args.get("firm_id")
    role_filter = request.args.get("role")

    sql = """
        SELECT i.*, f.name as firm_name, f.focus_sectors as firm_sectors,
               COUNT(DISTINCT di.deal_id) as deal_count
        FROM investors i
        LEFT JOIN firms f ON i.firm_id = f.id
        LEFT JOIN deal_investors di ON i.id = di.investor_id
    """
    params = []
    wheres = []

    if firm_id:
        wheres.append("i.firm_id = ?")
        params.append(int(firm_id))
    if role_filter:
        wheres.append("LOWER(i.title) LIKE ?")
        params.append(f"%{role_filter.lower()}%")

    if wheres:
        sql += " WHERE " + " AND ".join(wheres)

    sql += " GROUP BY i.id ORDER BY deal_count DESC, i.name"

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/investors/<int:investor_id>", methods=["GET"])
def get_investor(investor_id):
    """Get a single investor/partner with their deals."""
    conn = get_connection()
    inv = conn.execute("""
        SELECT i.*, f.name as firm_name, f.website as firm_website,
               f.focus_sectors as firm_sectors
        FROM investors i
        LEFT JOIN firms f ON i.firm_id = f.id
        WHERE i.id = ?
    """, (investor_id,)).fetchone()
    if not inv:
        return jsonify({"error": "Investor not found"}), 404

    deals = conn.execute("""
        SELECT d.*, c.name as category
        FROM deals d
        JOIN deal_investors di ON d.id = di.deal_id
        LEFT JOIN categories c ON d.category_id = c.id
        WHERE di.investor_id = ?
        ORDER BY d.date_announced DESC
    """, (investor_id,)).fetchall()

    conn.close()
    return jsonify({
        "investor": dict(inv),
        "deals": [dict(d) for d in deals],
    })


@app.route("/api/firms/<int:firm_id>/partners", methods=["GET"])
def get_firm_partners(firm_id):
    """Get all partners/GPs at a specific firm with their deal activity."""
    conn = get_connection()
    firm = conn.execute("SELECT * FROM firms WHERE id = ?", (firm_id,)).fetchone()
    if not firm:
        return jsonify({"error": "Firm not found"}), 404

    partners = conn.execute("""
        SELECT i.*,
               COUNT(DISTINCT di.deal_id) as deal_count,
               COALESCE(SUM(d.amount_usd), 0) as total_invested
        FROM investors i
        LEFT JOIN deal_investors di ON i.id = di.investor_id
        LEFT JOIN deals d ON di.deal_id = d.id
        WHERE i.firm_id = ?
        GROUP BY i.id
        ORDER BY deal_count DESC, i.name
    """, (firm_id,)).fetchall()

    partner_data = []
    for p in partners:
        cat_rows = conn.execute("""
            SELECT c.name as category, COUNT(*) as count
            FROM deals d
            JOIN deal_investors di ON d.id = di.deal_id
            LEFT JOIN categories c ON d.category_id = c.id
            WHERE di.investor_id = ?
            GROUP BY c.name ORDER BY count DESC
        """, (p["id"],)).fetchall()

        partner_data.append({
            **dict(p),
            "categories": [dict(c) for c in cat_rows],
        })

    conn.close()
    return jsonify({
        "firm": dict(firm),
        "partners": partner_data,
    })


@app.route("/api/firms/<int:firm_id>/profile")
def get_firm_profile(firm_id):
    """Comprehensive firm profile — one request for the full Firms tab detail view."""
    conn = get_connection()

    # 1. Firm record
    firm = conn.execute("SELECT * FROM firms WHERE id = ?", (firm_id,)).fetchone()
    if not firm:
        conn.close()
        return jsonify({"error": "Firm not found"}), 404

    # 2. Deals with category and role
    deals = conn.execute("""
        SELECT d.id, d.company_name, d.company_website, d.company_description,
               d.stage, d.amount_usd, d.date_announced, d.source_type,
               c.name as category, df.role
        FROM deals d
        JOIN deal_firms df ON d.id = df.deal_id
        LEFT JOIN categories c ON d.category_id = c.id
        WHERE df.firm_id = ?
        ORDER BY d.date_announced DESC
    """, (firm_id,)).fetchall()
    deals_list = [dict(d) for d in deals]

    # 3. Team members with deal counts
    team_rows = conn.execute("""
        SELECT i.id, i.name, i.title, i.linkedin_url,
               COUNT(DISTINCT di.deal_id) as deal_count
        FROM investors i
        LEFT JOIN deal_investors di ON i.id = di.investor_id
        WHERE i.firm_id = ?
        GROUP BY i.id
        ORDER BY deal_count DESC, i.name
    """, (firm_id,)).fetchall()

    # 4. Per-investor sector breakdown
    team = []
    for t in team_rows:
        sectors = conn.execute("""
            SELECT c.name, COUNT(*) as count
            FROM deals d
            JOIN deal_investors di ON d.id = di.deal_id
            LEFT JOIN categories c ON d.category_id = c.id
            WHERE di.investor_id = ?
            GROUP BY c.name ORDER BY count DESC
        """, (t["id"],)).fetchall()
        team.append({
            "id": t["id"], "name": t["name"], "title": t["title"],
            "linkedin_url": t["linkedin_url"], "deal_count": t["deal_count"],
            "sectors": [{"name": s["name"], "count": s["count"]} for s in sectors],
        })

    # 5. Portfolio companies
    portfolio = conn.execute("""
        SELECT id, company_name, company_website, description, sector, lead_partner
        FROM portfolio_companies WHERE firm_id = ?
        ORDER BY company_name
    """, (firm_id,)).fetchall()

    conn.close()

    # Compute KPIs
    deal_count = len(deals_list)
    total_invested = sum(d["amount_usd"] or 0 for d in deals_list)
    lead_count = sum(1 for d in deals_list if d.get("role") == "lead")

    # Sector breakdown
    sector_map = {}
    for d in deals_list:
        cat = d.get("category") or "Other"
        if cat not in sector_map:
            sector_map[cat] = {"name": cat, "deal_count": 0, "total_invested": 0}
        sector_map[cat]["deal_count"] += 1
        sector_map[cat]["total_invested"] += d.get("amount_usd") or 0
    sectors = sorted(sector_map.values(), key=lambda x: x["deal_count"], reverse=True)

    # Stage breakdown
    stage_map = {}
    for d in deals_list:
        st = d.get("stage") or "Unknown"
        stage_map[st] = stage_map.get(st, 0) + 1
    stage_breakdown = [{"stage": k, "count": v} for k, v in sorted(stage_map.items(), key=lambda x: x[1], reverse=True)]

    # Activity stats
    now = datetime.now()
    deals_last_90d = 0
    deals_last_year = 0
    latest_deal_date = None
    for d in deals_list:
        da = d.get("date_announced")
        if da:
            if not latest_deal_date or da > latest_deal_date:
                latest_deal_date = da
            try:
                dt = datetime.strptime(da, "%Y-%m-%d")
                if (now - dt).days <= 90:
                    deals_last_90d += 1
                if (now - dt).days <= 365:
                    deals_last_year += 1
            except Exception:
                pass

    return jsonify({
        "firm": dict(firm),
        "kpis": {
            "deal_count": deal_count,
            "total_invested": total_invested,
            "lead_count": lead_count,
            "team_count": len(team),
            "portfolio_count": len(portfolio),
        },
        "team": team,
        "portfolio": [dict(p) for p in portfolio],
        "deals": deals_list,
        "sectors": sectors,
        "stage_breakdown": stage_breakdown,
        "activity": {
            "deals_last_90d": deals_last_90d,
            "deals_last_year": deals_last_year,
            "latest_deal_date": latest_deal_date,
        },
    })


@app.route("/api/partners/by-category", methods=["GET"])
def partners_by_category():
    """Subcategorize all partners/GPs by the deal categories they invest in."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT
            c.name as category,
            i.name as investor_name,
            i.title as investor_title,
            f.name as firm_name,
            f.id as firm_id,
            i.id as investor_id,
            COUNT(DISTINCT di.deal_id) as deal_count,
            COALESCE(SUM(d.amount_usd), 0) as total_invested
        FROM investors i
        JOIN deal_investors di ON i.id = di.investor_id
        JOIN deals d ON di.deal_id = d.id
        LEFT JOIN categories c ON d.category_id = c.id
        LEFT JOIN firms f ON i.firm_id = f.id
        GROUP BY c.name, i.id
        ORDER BY c.name, deal_count DESC
    """).fetchall()
    conn.close()

    result = {}
    for row in rows:
        cat = row["category"] or "Uncategorized"
        if cat not in result:
            result[cat] = []
        result[cat].append({
            "investor_name": row["investor_name"],
            "investor_title": row["investor_title"],
            "firm_name": row["firm_name"],
            "firm_id": row["firm_id"],
            "investor_id": row["investor_id"],
            "deal_count": row["deal_count"],
            "total_invested": row["total_invested"],
        })

    return jsonify(result)


@app.route("/api/feed", methods=["GET"])
def get_deal_feed():
    """Live deal feed — most recent deals from all sources, timeline view."""
    import re as _re
    conn = get_connection()
    days = int(request.args.get("days", 9999))
    source = request.args.get("source")
    min_amount = request.args.get("min_amount", type=float)
    stage = request.args.get("stage")

    where = ["(d.date_announced >= date('now', ?) OR d.date_announced IS NULL)"]
    params = [f"-{days} days"]

    if source:
        where.append("d.source_type = ?")
        params.append(source)
    if min_amount:
        where.append("d.amount_usd >= ?")
        params.append(min_amount)
    if stage:
        where.append("d.stage = ?")
        params.append(stage)

    where_sql = " AND ".join(where)

    rows = conn.execute(f"""
        SELECT
            d.id, d.company_name, d.company_description, d.stage,
            d.amount_usd, d.amount_disclosed, d.date_announced,
            d.source_type, d.source_url, d.confidence_score, d.raw_text,
            c.name as category,
            GROUP_CONCAT(DISTINCT f.name) as firms,
            GROUP_CONCAT(DISTINCT
                CASE WHEN df.role = 'lead' THEN f.name END
            ) as lead_firms,
            GROUP_CONCAT(DISTINCT i.name) as investors
        FROM deals d
        LEFT JOIN categories c ON d.category_id = c.id
        LEFT JOIN deal_firms df ON d.id = df.deal_id
        LEFT JOIN firms f ON df.firm_id = f.id
        LEFT JOIN deal_investors di ON d.id = di.deal_id
        LEFT JOIN investors i ON di.investor_id = i.id
        WHERE {where_sql}
        GROUP BY d.id
        ORDER BY d.date_announced DESC, d.created_at DESC
        LIMIT 1000
    """, params).fetchall()

    deals = []
    for r in rows:
        raw = r["raw_text"] or ""
        founders = None
        total_raised = None
        fm = _re.search(r"Founders: (.+)", raw)
        if fm:
            founders = fm.group(1)
        tm = _re.search(r"Total raised: \$([\d,.]+)", raw)
        if tm:
            total_raised = float(tm.group(1).replace(",", ""))

        deals.append({
            "id": r["id"],
            "company_name": r["company_name"],
            "description": r["company_description"],
            "stage": r["stage"],
            "amount_usd": r["amount_usd"],
            "date_announced": r["date_announced"],
            "source_type": r["source_type"],
            "source_url": r["source_url"],
            "confidence_score": r["confidence_score"],
            "category": r["category"],
            "firms": r["firms"].split(",") if r["firms"] else [],
            "lead_firms": r["lead_firms"].split(",") if r["lead_firms"] else [],
            "investors": r["investors"].split(",") if r["investors"] else [],
            "founders": founders,
            "total_raised": total_raised,
        })

    total_capital = sum(d["amount_usd"] or 0 for d in deals)
    by_source = {}
    by_stage = {}
    by_week = {}
    for d in deals:
        src = d["source_type"] or "other"
        by_source[src] = by_source.get(src, 0) + 1
        st = d["stage"] or "Unknown"
        by_stage[st] = by_stage.get(st, 0) + 1
        if d["date_announced"]:
            try:
                date_obj = datetime.strptime(d["date_announced"], "%Y-%m-%d")
                week_key = date_obj.strftime("%Y-W%U")
                by_week[week_key] = by_week.get(week_key, 0) + 1
            except Exception:
                pass

    conn.close()
    return jsonify({
        "deals": deals,
        "summary": {
            "total_deals": len(deals),
            "total_capital": total_capital,
            "by_source": by_source,
            "by_stage": by_stage,
            "by_week": by_week,
            "period_days": days,
        }
    })


@app.route("/api/feed/timeline", methods=["GET"])
def get_deal_timeline():
    """Aggregated deal counts and capital by day for charting."""
    conn = get_connection()
    days = int(request.args.get("days", 9999))
    rows = conn.execute("""
        SELECT
            d.date_announced as date,
            COUNT(*) as deal_count,
            COALESCE(SUM(d.amount_usd), 0) as total_capital,
            COUNT(DISTINCT d.source_type) as source_count
        FROM deals d
        WHERE d.date_announced >= date('now', ?)
        GROUP BY d.date_announced
        ORDER BY d.date_announced ASC
    """, (f"-{days} days",)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/feed/top-investors", methods=["GET"])
def get_feed_top_investors():
    """Top investors by deal count in recent feed window."""
    conn = get_connection()
    days = int(request.args.get("days", 9999))
    rows = conn.execute("""
        SELECT
            i.name, i.title,
            f.name as firm_name,
            COUNT(DISTINCT di.deal_id) as deal_count,
            COALESCE(SUM(d.amount_usd), 0) as total_deployed,
            GROUP_CONCAT(DISTINCT d.company_name) as companies
        FROM investors i
        JOIN deal_investors di ON i.id = di.investor_id
        JOIN deals d ON di.deal_id = d.id
        LEFT JOIN firms f ON i.firm_id = f.id
        WHERE d.date_announced >= date('now', ?)
        GROUP BY i.id
        HAVING deal_count >= 1
        ORDER BY deal_count DESC, total_deployed DESC
        LIMIT 30
    """, (f"-{days} days",)).fetchall()
    conn.close()
    return jsonify([{
        **dict(r),
        "companies": r["companies"].split(",") if r["companies"] else []
    } for r in rows])


@app.route("/api/scrape-logs", methods=["GET"])
def get_scrape_logs():
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM scrape_logs ORDER BY started_at DESC LIMIT 50"
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/categories", methods=["GET"])
def get_categories():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM categories ORDER BY name").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/portfolio", methods=["GET"])
def get_portfolio():
    """Portfolio companies scraped from VC firm websites."""
    conn = get_connection()
    firm_id = request.args.get("firm_id", type=int)
    search = request.args.get("q", "")

    sql = """
        SELECT pc.*, f.name as firm_name, f.website as firm_website,
               f.focus_sectors as firm_sectors
        FROM portfolio_companies pc
        JOIN firms f ON pc.firm_id = f.id
    """
    params = []
    wheres = []

    if firm_id:
        wheres.append("pc.firm_id = ?")
        params.append(firm_id)
    if search:
        wheres.append("(pc.company_name LIKE ? OR f.name LIKE ? OR pc.sector LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    if wheres:
        sql += " WHERE " + " AND ".join(wheres)

    sql += " ORDER BY f.name, pc.company_name"

    rows = conn.execute(sql, params).fetchall()

    # Also get aggregates by firm
    firm_agg = conn.execute("""
        SELECT f.id, f.name, f.website, f.focus_sectors,
               COUNT(pc.id) as company_count
        FROM firms f
        JOIN portfolio_companies pc ON f.id = pc.firm_id
        GROUP BY f.id
        ORDER BY company_count DESC
    """).fetchall()

    conn.close()
    return jsonify({
        "companies": [dict(r) for r in rows],
        "total": len(rows),
        "firms": [dict(r) for r in firm_agg],
    })


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

        # Run the main scrapers
        run_news_scraper(days_back=180)
        run_alleywatch_scraper(days_back=180)

        conn = get_connection()
        deal_count = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
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
        # Run once on startup after 60s delay
        time.sleep(60)
        _run_scrape_background()

        # Then wait for Sunday 9 PM EST each week (= Monday 02:00 UTC)
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
        # Run portfolio scrape on startup after 120s delay (after deals scrape starts)
        time.sleep(120)
        _run_portfolio_scrape()

        # Then every Friday 9 PM EST (= Saturday 02:00 UTC)
        while True:
            now = datetime.utcnow()
            # Saturday = 5 in weekday()
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
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() in ("1", "true", "yes")
    if not debug:
        _start_scheduler()
    app.run(debug=debug, host="0.0.0.0", port=port)


