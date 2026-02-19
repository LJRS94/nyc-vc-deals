"""
NYC VC Deal Scraper — REST API Server
Serves deal data to the React dashboard.
"""

import os
import sys
import json
import threading
import logging
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import get_connection, init_db

app = Flask(__name__)
CORS(app)


@app.route("/")
def serve_dashboard():
    return send_from_directory(os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates"), "dashboard.html")


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

    deals = []
    for row in rows:
        deal_id = row["id"]

        firms_rows = conn.execute("""
            SELECT f.name, f.website, df.role
            FROM deal_firms df JOIN firms f ON df.firm_id = f.id
            WHERE df.deal_id = ?
        """, (deal_id,)).fetchall()

        investors_rows = conn.execute("""
            SELECT i.name, i.title, f.name as firm_name
            FROM deal_investors di
            JOIN investors i ON di.investor_id = i.id
            LEFT JOIN firms f ON i.firm_id = f.id
            WHERE di.deal_id = ?
        """, (deal_id,)).fetchall()

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
            "firms": [{"name": f["name"], "website": f["website"], "role": f["role"]} for f in firms_rows],
            "investors": [{"name": i["name"], "title": i["title"], "firm": i["firm_name"]} for i in investors_rows],
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
               COALESCE(SUM(d.amount_usd), 0) as total_invested
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
        LIMIT 200
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

        # Run the main scrapers (skip Google to avoid rate limits,
        # Bing + RSS + BuiltInNYC + Crunchbase News will run)
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


def _start_scheduler():
    """Start a background thread that runs scrapes every 2 hours."""
    import time

    def scheduler_loop():
        # Wait 60s after startup before first scrape
        time.sleep(60)
        while True:
            _run_scrape_background()
            # Sleep 2 hours between scrapes
            time.sleep(2 * 60 * 60)

    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
    logger.info("Background scraper scheduled: every 2 hours")


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


