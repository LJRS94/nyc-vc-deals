"""Quality control and scrape management routes."""

import logging
from flask import Blueprint, g, jsonify, request

from config import SCRAPE_LOGS_LIMIT
from database import get_connection

logger = logging.getLogger(__name__)
qc_bp = Blueprint("qc", __name__)


@qc_bp.route("/api/scrape-logs", methods=["GET"])
def get_scrape_logs():
    conn = g.db
    rows = conn.execute(
        "SELECT * FROM scrape_logs ORDER BY started_at DESC LIMIT ?", (SCRAPE_LOGS_LIMIT,)
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@qc_bp.route("/api/qc/audit", methods=["GET"])
def qc_audit():
    """Run a quality audit and return issues found.

    Query params:
        type: deal|portfolio|firm|all (default: deal)
    """
    try:
        from quality_control import (
            run_audit, run_audit_portfolio, run_audit_firms,
            run_audit_all, init_qc_tables,
        )
        conn = g.db
        init_qc_tables(conn)
        data_type = request.args.get("type", "deal")
        if data_type == "all":
            result = run_audit_all(conn)
        elif data_type == "portfolio":
            result = run_audit_portfolio(conn)
        elif data_type == "firm":
            result = run_audit_firms(conn)
        else:
            result = run_audit(conn)
        return jsonify(result)
    except (ImportError, OSError) as e:
        logger.error(f"QC audit failed: {e}")
        return jsonify({"error": str(e)}), 500


@qc_bp.route("/api/qc/rejections", methods=["GET"])
def qc_rejections():
    """Get recent rejection stats for self-improvement insights.

    Query params:
        days: number of days to look back (default: 30)
        type: deal|portfolio|firm (default: all types)
    """
    try:
        from quality_control import get_rejection_summary, init_qc_tables
        conn = g.db
        init_qc_tables(conn)
        days = request.args.get("days", 30, type=int)
        data_type = request.args.get("type")
        summary = get_rejection_summary(conn, days, data_type=data_type)

        pattern_query = "SELECT pattern_type, pattern_value, hit_count, auto_reject FROM qc_patterns"
        pattern_params = []
        if data_type:
            pattern_query += " WHERE data_type = ?"
            pattern_params.append(data_type)
        pattern_query += " ORDER BY hit_count DESC LIMIT 20"
        patterns = conn.execute(pattern_query, pattern_params).fetchall()
        return jsonify({
            "rejection_summary": summary,
            "top_patterns": [dict(p) for p in patterns],
        })
    except (ImportError, OSError) as e:
        logger.error(f"QC rejections query failed: {e}")
        return jsonify({"error": str(e)}), 500


@qc_bp.route("/api/qc/metrics", methods=["GET"])
def qc_metrics():
    """Get quality metrics over time.

    Query params:
        type: deal|portfolio|firm (default: all types)
    """
    try:
        from quality_control import init_qc_tables
        conn = g.db
        init_qc_tables(conn)
        data_type = request.args.get("type")
        if data_type:
            rows = conn.execute(
                "SELECT * FROM qc_metrics WHERE data_type = ? ORDER BY run_date DESC LIMIT 50",
                (data_type,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM qc_metrics ORDER BY run_date DESC LIMIT 50"
            ).fetchall()
        return jsonify([dict(r) for r in rows])
    except (ImportError, OSError) as e:
        logger.error(f"QC metrics query failed: {e}")
        return jsonify({"error": str(e)}), 500
