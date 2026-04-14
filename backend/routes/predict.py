from __future__ import annotations

import threading
from typing import Any

from flask import Blueprint, jsonify, request

from config import ASYNC_MODE
from models.stock_predictor import predict_next_day, train_all_symbols, train_model
from security import admin_required
from services.data_pipeline import get_stock_history
from services.market_data import is_symbol_allowed

try:
    from tasks import train_symbols as train_symbols_task
except Exception:  # pragma: no cover - optional runtime dependency
    train_symbols_task = None

predict_bp = Blueprint("predict", __name__)


_training_state: dict[str, str] = {}
_training_lock = threading.Lock()


def _set_state(symbol: str, state: str) -> None:
    with _training_lock:
        _training_state[symbol] = state


def _get_state(symbol: str) -> str | None:
    with _training_lock:
        return _training_state.get(symbol)


def _train_symbol_background(symbol: str) -> None:
    try:
        _set_state(symbol, "training")
        train_model(symbol)
        _set_state(symbol, "completed")
    except Exception as exc:  # noqa: BLE001
        _set_state(symbol, f"failed: {exc}")


def _train_bulk_background(symbol: str) -> None:
    try:
        _set_state(symbol, "training")
        if symbol == "ALL":
            train_all_symbols()
        else:
            train_model(symbol)
        _set_state(symbol, "completed")
    except Exception as exc:  # noqa: BLE001
        _set_state(symbol, f"failed: {exc}")


@predict_bp.route("/predict/<symbol>", methods=["GET"])
def predict(symbol: str):
    symbol = symbol.strip().upper()
    if not is_symbol_allowed(symbol):
        return jsonify({"error": f"Symbol {symbol} not configured in stocks_name list"}), 404

    history = get_stock_history(symbol, days=5000)
    if history.empty:
        return jsonify({"error": f"Symbol {symbol} not found"}), 404

    state = _get_state(symbol)
    if state == "training":
        return jsonify({"status": "training", "retry_after": 30}), 202

    try:
        result = predict_next_day(symbol)
        return jsonify(result), 200
    except FileNotFoundError:
        threading.Thread(target=_train_symbol_background, args=(symbol,), daemon=True).start()
        return jsonify({"status": "training", "retry_after": 30}), 202
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@predict_bp.route("/train", methods=["POST"])
@admin_required
def trigger_training():
    data: dict[str, Any] = request.get_json(silent=True) or {}
    symbol = str(data.get("symbol", "ALL")).strip().upper()

    if symbol != "ALL":
        if not is_symbol_allowed(symbol):
            return jsonify({"error": f"Symbol {symbol} not configured in stocks_name list"}), 404
        history = get_stock_history(symbol, days=5000)
        if history.empty:
            return jsonify({"error": f"Symbol {symbol} not found"}), 404

    if _get_state(symbol) == "training":
        return jsonify({"status": "training_in_progress", "symbols": [symbol]}), 202

    if ASYNC_MODE == "celery" and train_symbols_task is not None:
        task = train_symbols_task.delay(symbol)
        _set_state(symbol, "queued")
        return jsonify({"status": "queued", "task_id": task.id, "symbols": [symbol]}), 202

    t = threading.Thread(target=_train_bulk_background, args=(symbol,), daemon=True)
    t.start()
    return jsonify({"status": "training_started", "symbols": [symbol]}), 202


@predict_bp.route("/train/status", methods=["GET"])
@admin_required
def training_status():
    return jsonify({"training": _training_state}), 200
