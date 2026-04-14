from __future__ import annotations

from functools import wraps
from typing import Any, Callable, TypeVar, cast

from flask import jsonify
from flask_jwt_extended import get_jwt, verify_jwt_in_request

F = TypeVar("F", bound=Callable[..., Any])


def admin_required(fn: F) -> F:
    @wraps(fn)
    def wrapper(*args: Any, **kwargs: Any):
        verify_jwt_in_request()
        claims = get_jwt() or {}
        role = claims.get("role")
        if role != "admin":
            return jsonify({"error": "Admin role required"}), 403
        return fn(*args, **kwargs)

    return cast(F, wrapper)
