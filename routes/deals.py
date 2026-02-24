"""Deal CRUD, stats, and analytics routes."""

import csv
import io
from datetime import datetime
from flask import Blueprint, g, jsonify, request, Response

from config import DEFAULT_PAGE_SIZE
from database import get_connection

deals_bp = Blueprint("deals", __name__)


def _safe_int(value, default, lo=1, hi=10000):
    """Parse an int query param, clamping to [lo, hi]."""
    try:
        return max(lo, min(int(value), hi))
    except (TypeError, ValueError):
        return default


@deals_bp.route("/api/stats", methods=["GET"])
def get_stats():
    """Dashboard overview stats — single combined query."""
    conn = g.db
    city = request.args.get("city")
    cf = "WHERE city = ?" if city else ""
    cf_and = "AND city = ?" if city else ""
    cp = [city] if city else []

    row = conn.execute(f"""
        SELECT
            (SELECT COUNT(*) FROM deals {cf}) as total_deals,
            (SELECT COUNT(*) FROM firms) as total_firms,
            (SELECT COUNT(*) FROM investors) as total_investors,
            (SELECT COALESCE(SUM(amount_usd), 0) FROM deals WHERE amount_usd IS NOT NULL {cf_and}) as total_capital,
            (SELECT COALESCE(AVG(amount_usd), 0) FROM deals WHERE amount_usd IS NOT NULL {cf_and}) as avg_deal_size,
            (SELECT COUNT(*) FROM deals WHERE (source_type = 'de_filing'
             OR raw_text LIKE '%DE incorporated%' OR raw_text LIKE '%Delaware%') {cf_and}) as de_incorporated_count,
            (SELECT MAX(COALESCE(date_announced, created_at)) FROM deals {cf}) as last_updated
    """, cp * 5 if city else []).fetchone()
    source_sql = "SELECT source_type, COUNT(*) as cnt FROM deals"
    if city:
        source_sql += " WHERE city = ?"
    source_sql += " GROUP BY source_type"
    source_rows = conn.execute(source_sql, cp).fetchall()
    return jsonify({
        "total_deals": row["total_deals"],
        "total_firms": row["total_firms"],
        "total_investors": row["total_investors"],
        "total_capital": row["total_capital"],
        "avg_deal_size": row["avg_deal_size"],
        "de_incorporated_count": row["de_incorporated_count"],
        "last_updated": row["last_updated"],
        "source_breakdown": {r["source_type"]: r["cnt"] for r in source_rows},
    })


@deals_bp.route("/api/deals", methods=["GET"])
def get_deals():
    """List deals with filtering and pagination."""
    conn = g.db

    page = _safe_int(request.args.get("page", 1), 1, 1, 10000)
    per_page = _safe_int(request.args.get("per_page", DEFAULT_PAGE_SIZE), DEFAULT_PAGE_SIZE, 1, 100)
    stage = request.args.get("stage")
    category = request.args.get("category")
    city = request.args.get("city")
    firm = request.args.get("firm")
    search = request.args.get("q")
    sort_by = request.args.get("sort", "date_announced")
    sort_dir = request.args.get("dir", "DESC")

    where_clauses = []
    params = []

    if stage:
        where_clauses.append("d.stage = ?")
        params.append(stage)
    if category:
        where_clauses.append("c.name = ?")
        params.append(category)
    if city:
        where_clauses.append("d.city = ?")
        params.append(city)
    if firm:
        where_clauses.append("f.name LIKE ?")
        params.append(f"%{firm}%")
    if search:
        where_clauses.append("(d.company_name LIKE ? OR d.raw_text LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    count_sql = f"""
        SELECT COUNT(DISTINCT d.id)
        FROM deals d
        LEFT JOIN categories c ON d.category_id = c.id
        LEFT JOIN deal_firms df ON d.id = df.deal_id
        LEFT JOIN firms f ON df.firm_id = f.id
        WHERE {where_sql}
    """
    total = conn.execute(count_sql, params).fetchone()[0]

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
            d.confidence_score, d.created_at, d.city,
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
            "city": row["city"],
            "firms": firms_by_deal.get(deal_id, []),
            "investors": investors_by_deal.get(deal_id, []),
        })

    return jsonify({
        "deals": deals,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
    })


@deals_bp.route("/api/deals/by-stage", methods=["GET"])
def deals_by_stage():
    conn = g.db
    rows = conn.execute("""
        SELECT stage, COUNT(*) as count,
               COALESCE(SUM(amount_usd), 0) as total_amount,
               COALESCE(AVG(amount_usd), 0) as avg_amount
        FROM deals
        GROUP BY stage ORDER BY count DESC
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@deals_bp.route("/api/deals/by-category", methods=["GET"])
def deals_by_category():
    conn = g.db
    rows = conn.execute("""
        SELECT c.name as category, COUNT(*) as count,
               COALESCE(SUM(d.amount_usd), 0) as total_amount
        FROM deals d
        JOIN categories c ON d.category_id = c.id
        GROUP BY c.name ORDER BY count DESC
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@deals_bp.route("/api/deals/by-month", methods=["GET"])
def deals_by_month():
    conn = g.db
    rows = conn.execute("""
        SELECT
            strftime('%Y-%m', COALESCE(date_announced, created_at)) as month,
            COUNT(*) as count,
            COALESCE(SUM(amount_usd), 0) as total_amount
        FROM deals
        WHERE date_announced IS NOT NULL
        GROUP BY month ORDER BY month
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@deals_bp.route("/api/deals/by-source", methods=["GET"])
def deals_by_source():
    """Breakdown of deals by data source (news, SEC, DE filings, etc)."""
    conn = g.db
    rows = conn.execute("""
        SELECT source_type, COUNT(*) as count,
               COALESCE(SUM(amount_usd), 0) as total_amount,
               COALESCE(AVG(confidence_score), 0) as avg_confidence
        FROM deals
        GROUP BY source_type ORDER BY count DESC
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@deals_bp.route("/api/deals/de-incorporated", methods=["GET"])
def deals_de_incorporated():
    """Deals from Delaware-incorporated companies (most VC-backed startups)."""
    conn = g.db
    page = _safe_int(request.args.get("page", 1), 1, 1, 10000)
    per_page = _safe_int(request.args.get("per_page", DEFAULT_PAGE_SIZE), DEFAULT_PAGE_SIZE, 1, 100)
    offset = (page - 1) * per_page

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

    return jsonify({
        "deals": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
    })


@deals_bp.route("/api/deals/velocity", methods=["GET"])
def deals_velocity():
    """Deal velocity — counts and capital for recent periods with trend vs prior period."""
    conn = g.db
    periods = {}
    for label, days in [("7d", 7), ("30d", 30), ("90d", 90)]:
        cur = conn.execute("""
            SELECT COUNT(*) as count,
                   COALESCE(SUM(amount_usd), 0) as capital
            FROM deals WHERE date_announced >= date('now', ?)
        """, (f"-{days} days",)).fetchone()
        prev = conn.execute("""
            SELECT COUNT(*) as count,
                   COALESCE(SUM(amount_usd), 0) as capital
            FROM deals WHERE date_announced >= date('now', ?)
              AND date_announced < date('now', ?)
        """, (f"-{days * 2} days", f"-{days} days")).fetchone()
        cur_count, cur_cap = cur["count"], cur["capital"]
        prev_count, prev_cap = prev["count"], prev["capital"]
        periods[label] = {
            "count": cur_count,
            "capital": cur_cap,
            "prev_count": prev_count,
            "prev_capital": prev_cap,
            "count_trend": round((cur_count - prev_count) / max(prev_count, 1) * 100),
            "capital_trend": round((cur_cap - prev_cap) / max(prev_cap, 1) * 100),
        }
    return jsonify(periods)


@deals_bp.route("/api/deals/followons", methods=["GET"])
def deals_followons():
    """Companies with multiple funding rounds (follow-on detection)."""
    conn = g.db
    rows = conn.execute("""
        SELECT company_name_normalized, company_name,
               GROUP_CONCAT(id) as ids,
               GROUP_CONCAT(stage, '|') as stages,
               GROUP_CONCAT(COALESCE(amount_usd, 0), '|') as amounts,
               GROUP_CONCAT(COALESCE(date_announced, ''), '|') as dates,
               COUNT(*) as round_count
        FROM deals
        WHERE company_name_normalized IS NOT NULL
        GROUP BY company_name_normalized
        HAVING COUNT(*) > 1
        ORDER BY round_count DESC, company_name
    """).fetchall()
    followons = []
    for r in rows:
        stages = r["stages"].split("|")
        amounts = r["amounts"].split("|")
        dates = r["dates"].split("|")
        rounds = []
        for i in range(len(stages)):
            amt = float(amounts[i]) if amounts[i] and amounts[i] != "0" else None
            rounds.append({"stage": stages[i], "amount_usd": amt, "date": dates[i] or None})
        rounds.sort(key=lambda x: x["date"] or "")
        followons.append({
            "company_name": r["company_name"],
            "round_count": r["round_count"],
            "rounds": rounds,
        })
    return jsonify(followons)


@deals_bp.route("/api/deals/completeness", methods=["GET"])
def deals_completeness():
    """Data completeness stats — % of deals with each key field filled."""
    conn = g.db
    total = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
    if total == 0:
        return jsonify({"total": 0, "fields": {}})
    fields = {}
    for col, label in [
        ("amount_usd", "amount"), ("company_website", "website"),
        ("company_description", "description"), ("date_announced", "date"),
        ("category_id", "sector"),
    ]:
        filled = conn.execute(
            f"SELECT COUNT(*) FROM deals WHERE {col} IS NOT NULL AND {col} != ''"
        ).fetchone()[0]
        fields[label] = {"filled": filled, "pct": round(filled / total * 100)}
    # Investors linked
    with_inv = conn.execute(
        "SELECT COUNT(DISTINCT deal_id) FROM deal_firms"
    ).fetchone()[0]
    fields["investors"] = {"filled": with_inv, "pct": round(with_inv / total * 100)}
    return jsonify({"total": total, "fields": fields})


@deals_bp.route("/api/firms/coinvestors", methods=["GET"])
def firms_coinvestors():
    """Co-investor matrix — pairs of firms that frequently co-invest."""
    conn = g.db
    rows = conn.execute("""
        SELECT f1.name as firm_a, f2.name as firm_b,
               COUNT(DISTINCT df1.deal_id) as shared_deals,
               COALESCE(SUM(d.amount_usd), 0) as shared_capital
        FROM deal_firms df1
        JOIN deal_firms df2 ON df1.deal_id = df2.deal_id AND df1.firm_id < df2.firm_id
        JOIN firms f1 ON df1.firm_id = f1.id
        JOIN firms f2 ON df2.firm_id = f2.id
        JOIN deals d ON df1.deal_id = d.id
        GROUP BY df1.firm_id, df2.firm_id
        HAVING shared_deals >= 2
        ORDER BY shared_deals DESC
        LIMIT 50
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@deals_bp.route("/api/deals/sector-trends", methods=["GET"])
def sector_trends():
    """Monthly deal count and capital by sector for trend charts."""
    conn = g.db
    rows = conn.execute("""
        SELECT
            strftime('%Y-%m', COALESCE(d.date_announced, d.created_at)) as month,
            c.name as sector,
            COUNT(*) as deal_count,
            COALESCE(SUM(d.amount_usd), 0) as total_capital,
            COALESCE(AVG(d.amount_usd), 0) as avg_size
        FROM deals d
        LEFT JOIN categories c ON d.category_id = c.id
        WHERE d.date_announced IS NOT NULL
        GROUP BY month, c.name
        ORDER BY month, deal_count DESC
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@deals_bp.route("/api/export/csv", methods=["GET"])
def export_csv():
    """Stream all deals as CSV, respecting current filter params."""
    conn = g.db

    stage = request.args.get("stage")
    category = request.args.get("category")
    city = request.args.get("city")
    search = request.args.get("q")

    where_clauses = []
    params = []
    if stage:
        where_clauses.append("d.stage = ?")
        params.append(stage)
    if category:
        where_clauses.append("c.name = ?")
        params.append(category)
    if city:
        where_clauses.append("d.city = ?")
        params.append(city)
    if search:
        where_clauses.append("(d.company_name LIKE ? OR d.raw_text LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    rows = conn.execute(f"""
        SELECT DISTINCT
            d.company_name, d.stage, d.amount_usd,
            d.date_announced, d.source_type, d.source_url,
            d.confidence_score, d.company_website, d.city,
            c.name as category,
            GROUP_CONCAT(DISTINCT f.name) as firms
        FROM deals d
        LEFT JOIN categories c ON d.category_id = c.id
        LEFT JOIN deal_firms df ON d.id = df.deal_id
        LEFT JOIN firms f ON df.firm_id = f.id
        WHERE {where_sql}
        GROUP BY d.id
        ORDER BY d.date_announced DESC
    """, params).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Company", "Stage", "Amount (USD)", "Date Announced",
        "Source", "Source URL", "Confidence", "Website", "City", "Category", "Firms"
    ])
    for r in rows:
        writer.writerow([
            r["company_name"], r["stage"], r["amount_usd"],
            r["date_announced"], r["source_type"], r["source_url"],
            r["confidence_score"], r["company_website"], r["city"],
            r["category"], r["firms"],
        ])

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=vc_deals.csv"},
    )


@deals_bp.route("/api/cities", methods=["GET"])
def get_cities():
    """City list with deal counts."""
    conn = g.db
    rows = conn.execute("""
        SELECT city, COUNT(*) as deal_count,
               COALESCE(SUM(amount_usd), 0) as total_capital
        FROM deals
        WHERE city IS NOT NULL
        GROUP BY city
        ORDER BY deal_count DESC
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@deals_bp.route("/api/categories", methods=["GET"])
def get_categories():
    conn = g.db
    rows = conn.execute("SELECT * FROM categories ORDER BY name").fetchall()
    return jsonify([dict(r) for r in rows])
