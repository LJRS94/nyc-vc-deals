"""Live deal feed and timeline routes."""

import re as _re
from datetime import datetime
from flask import Blueprint, jsonify, request

from config import FEED_MAX_RESULTS, TOP_INVESTORS_LIMIT
from database import get_connection

feed_bp = Blueprint("feed", __name__)


@feed_bp.route("/api/feed", methods=["GET"])
def get_deal_feed():
    """Live deal feed — most recent deals from all sources, timeline view."""
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
            ) as lead_firms
        FROM deals d
        LEFT JOIN categories c ON d.category_id = c.id
        LEFT JOIN deal_firms df ON d.id = df.deal_id
        LEFT JOIN firms f ON df.firm_id = f.id
        WHERE {where_sql}
        GROUP BY d.id
        ORDER BY d.date_announced DESC, d.created_at DESC
        LIMIT ?
    """, params + [FEED_MAX_RESULTS]).fetchall()

    # Batch-fetch investors and verification status for all deals
    deal_ids = [r["id"] for r in rows]
    investors_by_deal = {}
    verified_firms_by_deal = {}
    if deal_ids:
        ph = ",".join(["?"] * len(deal_ids))
        for ir in conn.execute(f"""
            SELECT di.deal_id, i.id as investor_id, i.name, i.title,
                   f.name as firm_name
            FROM deal_investors di
            JOIN investors i ON di.investor_id = i.id
            LEFT JOIN firms f ON i.firm_id = f.id
            WHERE di.deal_id IN ({ph})
        """, deal_ids).fetchall():
            investors_by_deal.setdefault(ir["deal_id"], []).append({
                "id": ir["investor_id"], "name": ir["name"],
                "title": ir["title"], "firm": ir["firm_name"],
            })
        for vr in conn.execute(f"""
            SELECT df.deal_id, f.name as firm_name
            FROM deal_firms df
            JOIN firms f ON df.firm_id = f.id
            WHERE df.deal_id IN ({ph}) AND df.verified = 1
        """, deal_ids).fetchall():
            verified_firms_by_deal.setdefault(vr["deal_id"], []).append(
                vr["firm_name"])

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
            "firms": [x for x in (r["firms"] or "").split(",") if x],
            "lead_firms": [x for x in (r["lead_firms"] or "").split(",") if x],
            "investors": investors_by_deal.get(r["id"], []),
            "verified_firms": verified_firms_by_deal.get(r["id"], []),
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


@feed_bp.route("/api/feed/timeline", methods=["GET"])
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
    return jsonify([dict(r) for r in rows])


@feed_bp.route("/api/feed/top-investors", methods=["GET"])
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
        LIMIT ?
    """, (f"-{days} days", TOP_INVESTORS_LIMIT)).fetchall()
    return jsonify([{
        **dict(r),
        "companies": [x for x in (r["companies"] or "").split(",") if x]
    } for r in rows])
