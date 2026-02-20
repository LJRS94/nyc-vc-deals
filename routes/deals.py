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

    return jsonify({
        "deals": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
    })


@deals_bp.route("/api/deals/velocity", methods=["GET"])
def deals_velocity():
    """Deal velocity — counts and capital for recent periods with trend vs prior period."""
    conn = get_connection()
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
    conn = get_connection()
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
    conn = get_connection()
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
    conn = get_connection()
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


@deals_bp.route("/api/categories", methods=["GET"])
def get_categories():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM categories ORDER BY name").fetchall()
    return jsonify([dict(r) for r in rows])
