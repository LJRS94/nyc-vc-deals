"""Portfolio verification routes — firm-website-confirmed deal attributions."""

from flask import Blueprint, g, jsonify, request

from auth import admin_required
from database import get_connection

verified_bp = Blueprint("verified", __name__)


def run_portfolio_verification(conn):
    """Cross-reference portfolio_companies against deals, verify/insert links.

    Returns dict with verification stats.
    """
    # Ensure normalized names are populated
    conn.execute("""
        UPDATE portfolio_companies
        SET company_name_normalized = LOWER(REPLACE(REPLACE(REPLACE(
            REPLACE(REPLACE(company_name, ' ', ''), '.', ''), ',', ''), '-', ''), '''', ''))
        WHERE company_name_normalized IS NULL
    """)
    conn.commit()

    # Find all portfolio ↔ deal matches
    matches = conn.execute("""
        SELECT pc.company_name as pc_name, pc.firm_id, f.name as firm_name,
               d.id as deal_id, d.company_name as deal_company,
               d.confidence_score
        FROM portfolio_companies pc
        JOIN firms f ON pc.firm_id = f.id
        JOIN deals d ON pc.company_name_normalized = d.company_name_normalized
    """).fetchall()

    verified_count = 0
    inserted_count = 0
    boosted_count = 0

    for m in matches:
        deal_id, firm_id = m["deal_id"], m["firm_id"]

        existing = conn.execute(
            "SELECT verified FROM deal_firms WHERE deal_id = ? AND firm_id = ?",
            (deal_id, firm_id)
        ).fetchone()

        if existing:
            if not existing["verified"]:
                conn.execute(
                    "UPDATE deal_firms SET verified = 1, source = 'portfolio' "
                    "WHERE deal_id = ? AND firm_id = ?",
                    (deal_id, firm_id)
                )
                verified_count += 1
        else:
            conn.execute(
                "INSERT INTO deal_firms (deal_id, firm_id, role, verified, source) "
                "VALUES (?, ?, 'participant', 1, 'portfolio')",
                (deal_id, firm_id)
            )
            inserted_count += 1

        # Boost confidence for portfolio-verified deals
        if m["confidence_score"] and m["confidence_score"] < 0.85:
            new_conf = min(m["confidence_score"] + 0.15, 0.90)
            conn.execute(
                "UPDATE deals SET confidence_score = ? WHERE id = ? AND confidence_score < ?",
                (new_conf, deal_id, new_conf)
            )
            boosted_count += 1

    conn.commit()
    return {
        "matches": len(matches),
        "verified": verified_count,
        "inserted": inserted_count,
        "boosted": boosted_count,
    }


@verified_bp.route("/api/verified", methods=["GET"])
def get_verified():
    """Portfolio verification dashboard — stats and verified deal-firm links."""
    conn = g.db

    # Overall stats
    total_links = conn.execute("SELECT COUNT(*) FROM deal_firms").fetchone()[0]
    verified_links = conn.execute(
        "SELECT COUNT(*) FROM deal_firms WHERE verified = 1"
    ).fetchone()[0]
    total_deals = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
    deals_with_verified = conn.execute("""
        SELECT COUNT(DISTINCT deal_id) FROM deal_firms WHERE verified = 1
    """).fetchone()[0]
    portfolio_companies = conn.execute(
        "SELECT COUNT(*) FROM portfolio_companies"
    ).fetchone()[0]
    portfolio_firms = conn.execute(
        "SELECT COUNT(DISTINCT firm_id) FROM portfolio_companies"
    ).fetchone()[0]

    # Verified links with deal details
    verified_rows = conn.execute("""
        SELECT d.id as deal_id, d.company_name, d.stage, d.amount_usd,
               d.date_announced, d.confidence_score,
               f.id as firm_id, f.name as firm_name,
               df.role, df.source,
               c.name as category
        FROM deal_firms df
        JOIN deals d ON df.deal_id = d.id
        JOIN firms f ON df.firm_id = f.id
        LEFT JOIN categories c ON d.category_id = c.id
        WHERE df.verified = 1
        ORDER BY d.date_announced DESC
    """).fetchall()

    # Group by deal
    deals_map = {}
    for r in verified_rows:
        did = r["deal_id"]
        if did not in deals_map:
            deals_map[did] = {
                "deal_id": did,
                "company_name": r["company_name"],
                "stage": r["stage"],
                "amount_usd": r["amount_usd"],
                "date_announced": r["date_announced"],
                "confidence_score": r["confidence_score"],
                "category": r["category"],
                "verified_firms": [],
            }
        deals_map[did]["verified_firms"].append({
            "firm_id": r["firm_id"],
            "firm_name": r["firm_name"],
            "role": r["role"],
            "source": r["source"],
        })

    # Firms with most verified links
    firm_stats = conn.execute("""
        SELECT f.id, f.name,
               COUNT(*) as verified_links,
               (SELECT COUNT(*) FROM portfolio_companies WHERE firm_id = f.id) as portfolio_count,
               (SELECT COUNT(*) FROM deal_firms WHERE firm_id = f.id) as total_links
        FROM deal_firms df
        JOIN firms f ON df.firm_id = f.id
        WHERE df.verified = 1
        GROUP BY f.id
        ORDER BY verified_links DESC
    """).fetchall()

    # Unverified links (deals with firm links that aren't portfolio-confirmed)
    unverified_count = conn.execute("""
        SELECT COUNT(*) FROM deal_firms WHERE verified = 0
    """).fetchone()[0]

    return jsonify({
        "stats": {
            "total_deal_firm_links": total_links,
            "verified_links": verified_links,
            "unverified_links": total_links - verified_links,
            "verification_rate": round(verified_links / max(total_links, 1) * 100, 1),
            "total_deals": total_deals,
            "deals_with_verification": deals_with_verified,
            "portfolio_companies": portfolio_companies,
            "portfolio_firms": portfolio_firms,
        },
        "verified_deals": list(deals_map.values()),
        "firm_stats": [dict(r) for r in firm_stats],
    })


@verified_bp.route("/api/verified/run", methods=["POST"])
@admin_required
def run_verification():
    """Re-run portfolio verification (after new portfolio scrape, admin only)."""
    conn = g.db
    result = run_portfolio_verification(conn)
    return jsonify({"ok": True, **result})


@verified_bp.route("/api/verified/unmatched", methods=["GET"])
def get_unmatched():
    """Portfolio companies that DON'T match any deal — potential data gaps."""
    conn = g.db
    firm_id = request.args.get("firm_id", type=int)

    sql = """
        SELECT pc.id, pc.company_name, pc.company_website, pc.sector,
               pc.lead_partner, f.name as firm_name, f.id as firm_id
        FROM portfolio_companies pc
        JOIN firms f ON pc.firm_id = f.id
        LEFT JOIN deals d ON pc.company_name_normalized = d.company_name_normalized
        WHERE d.id IS NULL
    """
    params = []
    if firm_id:
        sql += " AND pc.firm_id = ?"
        params.append(firm_id)
    sql += " ORDER BY f.name, pc.company_name"

    rows = conn.execute(sql, params).fetchall()
    return jsonify({
        "unmatched": [dict(r) for r in rows],
        "total": len(rows),
    })
