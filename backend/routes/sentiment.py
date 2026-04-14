from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from flask import Blueprint, jsonify

from config import DB_PATH, SENTIMENT_CACHE_HOURS
from models.sentiment_analyzer import get_symbol_sentiment
from services.data_pipeline import get_stock_history
from services.market_data import is_symbol_allowed
from services.news_scraper import run_sentiment_pipeline

sentiment_bp = Blueprint("sentiment", __name__)


def _has_recent_sentiment(symbol: str, hours: int) -> bool:
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat(sep=" ")
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM sentiment
            WHERE symbol = ? AND analyzed_at >= ?
            """,
            (symbol, cutoff),
        ).fetchone()
        return bool(row and row[0] > 0)
    finally:
        conn.close()


@sentiment_bp.route("/sentiment/<symbol>", methods=["GET"])
def get_sentiment(symbol: str):
    symbol = symbol.strip().upper()
    if not is_symbol_allowed(symbol):
        return jsonify({"error": f"Symbol {symbol} not configured in stocks_name list"}), 404

    history = get_stock_history(symbol, days=5000)
    if history.empty:
        return jsonify({"error": f"Symbol {symbol} not found"}), 404

    try:
        if not _has_recent_sentiment(symbol, SENTIMENT_CACHE_HOURS):
            run_sentiment_pipeline(symbol)

        payload = get_symbol_sentiment(symbol)
        return jsonify(payload), 200
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500
