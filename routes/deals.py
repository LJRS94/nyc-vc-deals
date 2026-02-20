"""Deal CRUD, stats, and analytics routes."""

from datetime import datetime
from flask import Blueprint, jsonify, request

from config import DEFAULT_PAGE_SIZE
from database import get_connection

deals_bp = Blueprint("deals", __name__)


@deals_bp.route("/api/stats", methods=["GET"])
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
    for row in conn.execute(
        "SELECT source_type, COUNT(*) as cnt FROM deals GROUP BY source_type"
    ).fetchall():
        stats["source_breakdown"][row["source_type"]] = row["cnt"]
    conn.close()
    return jsonify(stats)


@deals_bp.route("/api/deals", methods=["GET"])
def get_deals():
    """List deals with filtering and pagination."""
    conn = get_connection()

    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", DEFAULT_PAGE_SIZE))
    stage = request.args.get("stage")
    category = request.args.get("category")
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


@deals_bp.route("/api/deals/by-stage", methods=["GET"])
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


@deals_bp.route("/api/deals/by-category", methods=["GET"])
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


@deals_bp.route("/api/deals/by-month", methods=["GET"])
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


@deals_bp.route("/api/deals/by-source", methods=["GET"])
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


@deals_bp.route("/api/deals/de-incorporated", methods=["GET"])
def deals_de_incorporated():
    """Deals from Delaware-incorporated companies (most VC-backed startups)."""
    conn = get_connection()
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", DEFAULT_PAGE_SIZE))
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

    conn.close()
    return jsonify({
        "deals": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
    })


@deals_bp.route("/api/categories", methods=["GET"])
def get_categories():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM categories ORDER BY name").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])
