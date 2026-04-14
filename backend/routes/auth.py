from __future__ import annotations

import os
import urllib.parse
import requests as http_requests
from datetime import timedelta

from flask import Blueprint, jsonify, request, redirect
from flask_jwt_extended import (
    create_access_token,
    jwt_required,
    get_jwt_identity,
    get_jwt,
)

from config import ADMIN_PASSWORD, ADMIN_USERNAME, JWT_ACCESS_TOKEN_EXPIRES_HOURS

auth_bp = Blueprint("auth", __name__)

# ---------------------------------------------------------------------------
# In-memory user store (admin user + any Google/signup users)
# ---------------------------------------------------------------------------
_users: dict[str, dict] = {}  # key = identity (username/email)


def _make_token(identity: str, role: str = "user", full_name: str = "") -> str:
    return create_access_token(
        identity=identity,
        additional_claims={"role": role, "full_name": full_name},
        expires_delta=timedelta(hours=int(os.getenv("JWT_ACCESS_TOKEN_EXPIRES_HOURS", str(JWT_ACCESS_TOKEN_EXPIRES_HOURS)))),
    )


# ---------------------------------------------------------------------------
# Standard Auth
# ---------------------------------------------------------------------------
@auth_bp.route("/auth/login", methods=["POST"])
def login():
    body = request.get_json(silent=True) or {}
    username = str(body.get("username", "") or body.get("email", "")).strip()
    password = str(body.get("password", ""))

    # Admin hardcoded check
    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        token = _make_token(username, role="admin", full_name="Administrator")
        return jsonify({"token": token, "user": {"email": username, "full_name": "Administrator"}}), 200

    # Registered user check
    if username in _users and _users[username].get("password") == password:
        u = _users[username]
        token = _make_token(username, role="user", full_name=u.get("full_name", ""))
        return jsonify({"token": token, "user": {"email": username, "full_name": u.get("full_name", "")}}), 200

    return jsonify({"error": "Invalid credentials"}), 401


@auth_bp.route("/auth/signup", methods=["POST"])
def signup():
    body = request.get_json(silent=True) or {}
    email = str(body.get("email", "")).strip().lower()
    password = str(body.get("password", ""))
    full_name = str(body.get("full_name", "")).strip()

    if not email or not password:
        return jsonify({"error": "email and password are required"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    if email in _users or email == ADMIN_USERNAME:
        return jsonify({"error": "User already exists"}), 409

    _users[email] = {"password": password, "full_name": full_name}
    token = _make_token(email, role="user", full_name=full_name)
    return jsonify({"token": token, "user": {"email": email, "full_name": full_name}}), 201


@auth_bp.route("/auth/me", methods=["GET"])
@jwt_required()
def me():
    identity = get_jwt_identity()
    claims = get_jwt()
    full_name = claims.get("full_name", "")
    if not full_name and identity == ADMIN_USERNAME:
        full_name = "Administrator"
    return jsonify({"user": {"email": identity, "full_name": full_name}}), 200


# ---------------------------------------------------------------------------
# Google OAuth
# ---------------------------------------------------------------------------
@auth_bp.route("/auth/google/start", methods=["GET"])
def google_start():
    client_id = os.getenv("GOOGLE_CLIENT_ID")
    frontend_url = os.getenv("FRONTEND_URL", "http://127.0.0.1:5001")

    if not client_id:
        token = _make_token("google_user", role="admin", full_name="Google Simulator")
        return redirect(f"{frontend_url}/auth/callback?token={token}")

    redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:5001/api/auth/google/callback")
    scope = "openid email profile"
    url = (
        f"https://accounts.google.com/o/oauth2/v2/auth"
        f"?client_id={client_id}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
        f"&response_type=code"
        f"&scope={urllib.parse.quote(scope)}"
    )
    return redirect(url)


@auth_bp.route("/auth/google/callback", methods=["GET"])
def google_callback():
    frontend_url = os.getenv("FRONTEND_URL", "http://127.0.0.1:5001")
    code = request.args.get("code")
    error = request.args.get("error")

    if error:
        return redirect(f"{frontend_url}/auth/callback?error=true&message={urllib.parse.quote(error)}")

    client_id = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
    redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "http://127.0.0.1:5001/api/auth/google/callback")

    if not client_id or not client_secret:
        return redirect(f"{frontend_url}/auth/callback?error=true&message=" + urllib.parse.quote("Server Google Auth is misconfigured."))

    # Exchange code for tokens
    r = http_requests.post("https://oauth2.googleapis.com/token", data={
        "code": code,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    })
    token_resp = r.json()

    if "error" in token_resp:
        err_msg = token_resp.get("error_description", token_resp.get("error"))
        return redirect(f"{frontend_url}/auth/callback?error=true&message={urllib.parse.quote(err_msg)}")

    access_token = token_resp.get("access_token")
    user_info = http_requests.get(
        "https://www.googleapis.com/oauth2/v1/userinfo",
        headers={"Authorization": f"Bearer {access_token}"}
    ).json()

    email = user_info.get("email", "unknown@google.com")
    full_name = user_info.get("name", "Google User")

    # Register in memory if not already
    if email not in _users:
        _users[email] = {"full_name": full_name, "password": None}

    token = _make_token(email, role="user", full_name=full_name)
    return redirect(f"{frontend_url}/auth/callback?token={token}")
