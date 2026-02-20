"""Firm list, profiles, partners, portfolio, and investor routes."""

from datetime import datetime
from flask import Blueprint, jsonify, request

from database import get_connection

firms_bp = Blueprint("firms", __name__)


@firms_bp.route("/api/firms", methods=["GET"])
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
    return jsonify([dict(r) for r in rows])


@firms_bp.route("/api/firms/<int:firm_id>", methods=["GET"])
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

    return jsonify({
        "firm": dict(firm),
        "deals": [dict(d) for d in deals],
        "investors": [dict(i) for i in investors],
    })


@firms_bp.route("/api/investors", methods=["GET"])
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
    return jsonify([dict(r) for r in rows])


@firms_bp.route("/api/investors/<int:investor_id>", methods=["GET"])
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

    return jsonify({
        "investor": dict(inv),
        "deals": [dict(d) for d in deals],
    })


@firms_bp.route("/api/firms/<int:firm_id>/partners", methods=["GET"])
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

    # Batch-fetch categories for all partners (avoids N+1)
    partner_ids = [p["id"] for p in partners]
    cats_by_partner = {}
    if partner_ids:
        ph = ",".join(["?"] * len(partner_ids))
        cat_rows = conn.execute(f"""
            SELECT di.investor_id, c.name as category, COUNT(*) as count
            FROM deals d
            JOIN deal_investors di ON d.id = di.deal_id
            LEFT JOIN categories c ON d.category_id = c.id
            WHERE di.investor_id IN ({ph})
            GROUP BY di.investor_id, c.name
            ORDER BY count DESC
        """, partner_ids).fetchall()
        for r in cat_rows:
            cats_by_partner.setdefault(r["investor_id"], []).append(dict(r))

    partner_data = [{**dict(p), "categories": cats_by_partner.get(p["id"], [])} for p in partners]

    return jsonify({
        "firm": dict(firm),
        "partners": partner_data,
    })


@firms_bp.route("/api/firms/<int:firm_id>/profile")
def get_firm_profile(firm_id):
    """Comprehensive firm profile — one request for the full Firms tab detail view."""
    conn = get_connection()

    firm = conn.execute("SELECT * FROM firms WHERE id = ?", (firm_id,)).fetchone()
    if not firm:
        return jsonify({"error": "Firm not found"}), 404

    deals = conn.execute("""
        SELECT d.id, d.company_name, d.company_name_normalized,
               d.company_website, d.company_description,
               d.stage, d.amount_usd, d.date_announced, d.source_type,
               c.name as category, df.role
        FROM deals d
        JOIN deal_firms df ON d.id = df.deal_id
        LEFT JOIN categories c ON d.category_id = c.id
        WHERE df.firm_id = ?
        ORDER BY d.date_announced DESC
    """, (firm_id,)).fetchall()
    deals_list = [dict(d) for d in deals]

    team_rows = conn.execute("""
        SELECT i.id, i.name, i.title, i.linkedin_url,
               COUNT(DISTINCT di.deal_id) as deal_count
        FROM investors i
        LEFT JOIN deal_investors di ON i.id = di.investor_id
        WHERE i.firm_id = ?
        GROUP BY i.id
        ORDER BY deal_count DESC, i.name
    """, (firm_id,)).fetchall()

    # Batch-fetch sectors for all team members (avoids N+1)
    team_ids = [t["id"] for t in team_rows]
    sectors_by_member = {}
    if team_ids:
        ph = ",".join(["?"] * len(team_ids))
        sector_rows = conn.execute(f"""
            SELECT di.investor_id, c.name, COUNT(*) as count
            FROM deals d
            JOIN deal_investors di ON d.id = di.deal_id
            LEFT JOIN categories c ON d.category_id = c.id
            WHERE di.investor_id IN ({ph})
            GROUP BY di.investor_id, c.name
            ORDER BY count DESC
        """, team_ids).fetchall()
        for r in sector_rows:
            sectors_by_member.setdefault(r["investor_id"], []).append(
                {"name": r["name"], "count": r["count"]})

    team = [{
        "id": t["id"], "name": t["name"], "title": t["title"],
        "linkedin_url": t["linkedin_url"], "deal_count": t["deal_count"],
        "sectors": sectors_by_member.get(t["id"], []),
    } for t in team_rows]

    portfolio = conn.execute("""
        SELECT id, company_name, company_website, description, sector, lead_partner
        FROM portfolio_companies WHERE firm_id = ?
        ORDER BY company_name
    """, (firm_id,)).fetchall()

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

    # Funding history — batch-fetch all rounds for all companies (avoids N+1)
    funding_history = {}
    norm_names = list(set(
        d.get("company_name_normalized") or (d["company_name"] or "").lower().replace(" ", "")
        for d in deals_list
    ))
    rounds_by_company = {}
    if norm_names:
        ph = ",".join(["?"] * len(norm_names))
        all_rounds = conn.execute(f"""
            SELECT d2.company_name_normalized, d2.company_name,
                   d2.stage, d2.amount_usd, d2.date_announced,
                   GROUP_CONCAT(DISTINCT f2.name) as firms
            FROM deals d2
            LEFT JOIN deal_firms df2 ON d2.id = df2.deal_id
            LEFT JOIN firms f2 ON df2.firm_id = f2.id
            WHERE d2.company_name_normalized IN ({ph})
            GROUP BY d2.id
            ORDER BY d2.date_announced
        """, norm_names).fetchall()
        for r in all_rounds:
            rounds_by_company.setdefault(r["company_name_normalized"], []).append(r)

    seen_companies = set()
    for d in deals_list:
        cn_norm = d.get("company_name_normalized") or (d["company_name"] or "").lower().replace(" ", "")
        if cn_norm in seen_companies:
            continue
        seen_companies.add(cn_norm)
        rounds = rounds_by_company.get(cn_norm, [])
        if len(rounds) > 1:
            funding_history[d["company_name"]] = [
                {"stage": r["stage"], "amount_usd": r["amount_usd"],
                 "date_announced": r["date_announced"], "firms": r["firms"]}
                for r in rounds
            ]

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
        "funding_history": funding_history,
        "activity": {
            "deals_last_90d": deals_last_90d,
            "deals_last_year": deals_last_year,
            "latest_deal_date": latest_deal_date,
        },
    })


@firms_bp.route("/api/partners/by-category", methods=["GET"])
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


@firms_bp.route("/api/portfolio/linked", methods=["GET"])
def get_portfolio_linked():
    """Portfolio companies matched to deals via normalized company name."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT pc.id, pc.company_name, pc.company_website, pc.sector,
               pc.lead_partner, f.name as firm_name,
               d.id as deal_id, d.stage, d.amount_usd, d.date_announced,
               c.name as category
        FROM portfolio_companies pc
        JOIN firms f ON pc.firm_id = f.id
        JOIN deals d ON LOWER(REPLACE(REPLACE(REPLACE(pc.company_name, ' ', ''), '.', ''), ',', ''))
                      = d.company_name_normalized
        LEFT JOIN categories c ON d.category_id = c.id
        ORDER BY d.date_announced DESC
    """).fetchall()
    return jsonify([dict(r) for r in rows])


@firms_bp.route("/api/portfolio", methods=["GET"])
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

    firm_agg = conn.execute("""
        SELECT f.id, f.name, f.website, f.focus_sectors,
               COUNT(pc.id) as company_count
        FROM firms f
        JOIN portfolio_companies pc ON f.id = pc.firm_id
        GROUP BY f.id
        ORDER BY company_count DESC
    """).fetchall()

    return jsonify({
        "companies": [dict(r) for r in rows],
        "total": len(rows),
        "firms": [dict(r) for r in firm_agg],
    })
