"""Quality control and scrape management routes."""

from flask import Blueprint, jsonify, request

from config import SCRAPE_LOGS_LIMIT
from database import get_connection

qc_bp = Blueprint("qc", __name__)


@qc_bp.route("/api/scrape-logs", methods=["GET"])
def get_scrape_logs():
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM scrape_logs ORDER BY started_at DESC LIMIT ?", (SCRAPE_LOGS_LIMIT,)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@qc_bp.route("/api/qc/audit", methods=["GET"])
def qc_audit():
    """Run a quality audit and return issues found."""
    try:
        from quality_control import run_audit, init_qc_tables
        conn = get_connection()
        init_qc_tables(conn)
        result = run_audit(conn)
        conn.close()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@qc_bp.route("/api/qc/rejections", methods=["GET"])
def qc_rejections():
    """Get recent rejection stats for self-improvement insights."""
    try:
        from quality_control import get_rejection_summary, init_qc_tables
        conn = get_connection()
        init_qc_tables(conn)
        days = request.args.get("days", 30, type=int)
        summary = get_rejection_summary(conn, days)

        patterns = conn.execute(
            "SELECT pattern_type, pattern_value, hit_count, auto_reject "
            "FROM qc_patterns ORDER BY hit_count DESC LIMIT 20"
        ).fetchall()
        conn.close()
        return jsonify({
            "rejection_summary": summary,
            "top_patterns": [dict(p) for p in patterns],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@qc_bp.route("/api/qc/metrics", methods=["GET"])
def qc_metrics():
    """Get quality metrics over time."""
    try:
        from quality_control import init_qc_tables
        conn = get_connection()
        init_qc_tables(conn)
        rows = conn.execute(
            "SELECT * FROM qc_metrics ORDER BY run_date DESC LIMIT 50"
        ).fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500
