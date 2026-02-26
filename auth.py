"""Shared auth decorators — avoids circular imports between api_server and blueprints."""

from functools import wraps
from flask import jsonify, session


def login_required(f):
    """Returns 401 if no valid session."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return jsonify({"error": "Login required"}), 401
        return f(*args, **kwargs)
    return wrapper


def admin_required(f):
    """Returns 401 if not logged in, 403 if not admin."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return jsonify({"error": "Login required"}), 401
        if not session.get("is_admin"):
            return jsonify({"error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return wrapper
