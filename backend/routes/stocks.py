from __future__ import annotations

from pathlib import Path
import time

from flask import Blueprint, jsonify, request

from config import ASYNC_MODE
from config import CSV_PATH
from database import db_cursor, ingest_csv, init_db
from security import admin_required
from services.incremental_training import append_and_quick_retrain
from services.data_pipeline import compute_technical_indicators, get_all_symbols, get_stock_history
from services.market_data import (
    get_cached_quotes_bulk,
    get_live_quote,
    get_live_quotes_bulk,
    is_symbol_allowed,
    load_watchlist,
)
from services.market_performers import get_market_performers

try:
    from tasks import refresh_live_prices as refresh_live_prices_task
except Exception:  # pragma: no cover - optional runtime dependency
    refresh_live_prices_task = None

stocks_bp = Blueprint("stocks", __name__)


def _format_float(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


@stocks_bp.route("/stocks", methods=["GET"])
def list_stocks():
    watch = load_watchlist()
    symbols = get_all_symbols()
    if watch:
        symbols = [s for s in symbols if s in watch]

    snapshots = []
    with db_cursor() as cur:
        rows = cur.execute(
            """
            SELECT s.symbol, s.close, s.change_pct
            FROM stocks s
            INNER JOIN (
                SELECT symbol, MAX(date) AS max_date
                FROM stocks
                GROUP BY symbol
            ) latest
                ON s.symbol = latest.symbol AND s.date = latest.max_date
            ORDER BY s.symbol ASC
            """
        ).fetchall()
        db_snapshots = [
            {
                "symbol": r["symbol"],
                "close": _format_float(r["close"]),
                "change_pct": _format_float(r["change_pct"]),
            }
            for r in rows
            if (not watch or r["symbol"] in watch)
        ]

    live_map = get_cached_quotes_bulk(symbols)
    db_map = {item["symbol"]: item for item in db_snapshots}

    snapshots = []
    for sym in symbols:
        base = db_map.get(sym, {"symbol": sym, "close": None, "change_pct": None})
        live = live_map.get(sym)
        snapshots.append(
            {
                "symbol": sym,
                "company": watch.get(sym, sym) if watch else sym,
                "close": _format_float((live or {}).get("close", base.get("close"))),
                "change_pct": _format_float((live or {}).get("change_pct", base.get("change_pct"))),
                "source": (live or {}).get("source", "db"),
                "fetched_at": (live or {}).get("fetched_at"),
            }
        )

    return jsonify({"symbols": symbols, "count": len(symbols), "snapshots": snapshots}), 200


@stocks_bp.route("/stock/<symbol>", methods=["GET"])
def get_stock(symbol: str):
    try:
        days = int(request.args.get("days", 365))
    except ValueError:
        return jsonify({"error": "Invalid days query parameter"}), 400

    symbol = symbol.strip().upper()
    if not is_symbol_allowed(symbol):
        return jsonify({"error": f"Symbol {symbol} not configured in stocks_name list"}), 404

    df = get_stock_history(symbol, days=days)
    if df.empty:
        return jsonify({"error": f"Symbol {symbol} not found"}), 404

    enriched = compute_technical_indicators(df)

    data = [
        {
            "date": row["date"].strftime("%Y-%m-%d"),
            "open": _format_float(row.get("open")),
            "high": _format_float(row.get("high")),
            "low": _format_float(row.get("low")),
            "close": _format_float(row.get("close")),
            "volume": _format_float(row.get("volume")),
            "ma_7": _format_float(row.get("MA_7")),
            "ma_20": _format_float(row.get("MA_20")),
            "ma_50": _format_float(row.get("MA_50")),
            "ema_10": _format_float(row.get("EMA_10")),
            "ema_20": _format_float(row.get("EMA_20")),
            "ema_50": _format_float(row.get("EMA_50")),
            "bb_upper": _format_float(row.get("BB_upper")),
            "bb_mid": _format_float(row.get("BB_mid")),
            "bb_lower": _format_float(row.get("BB_lower")),
        }
        for _, row in enriched.iterrows()
    ]

    latest = enriched.iloc[-1]
    live = get_live_quote(symbol)
    payload = {
        "symbol": symbol,
        "data": data,
        "latest": {
            "close": _format_float((live or {}).get("close", latest.get("close"))),
            "change": _format_float((live or {}).get("change", latest.get("change"))),
            "change_pct": _format_float((live or {}).get("change_pct", latest.get("change_pct"))),
            "volume": _format_float(latest.get("volume")),
            "source": (live or {}).get("source", "db"),
            "fetched_at": (live or {}).get("fetched_at"),
        },
        "technical": {
            "ma_7": _format_float(latest.get("MA_7")),
            "ma_20": _format_float(latest.get("MA_20")),
            "ema_10": _format_float(latest.get("EMA_10")),
            "ema_20": _format_float(latest.get("EMA_20")),
            "ema_50": _format_float(latest.get("EMA_50")),
            "rsi": _format_float(latest.get("RSI_14")),
            "macd": _format_float(latest.get("MACD")),
        },
    }
    return jsonify(payload), 200


@stocks_bp.route("/market-performers", methods=["GET"])
def market_performers():
    try:
        force_refresh = str(request.args.get("refresh", "false")).strip().lower() in {
            "1",
            "true",
            "yes",
        }
        payload = get_market_performers(force_refresh=force_refresh)
        return jsonify(payload), 200
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 502


@stocks_bp.route("/admin/upload-csv", methods=["POST"])
@admin_required
def upload_csv():
    if "file" not in request.files:
        return jsonify({"error": "No file part in request"}), 400

    file = request.files["file"]
    if not file or not file.filename:
        return jsonify({"error": "No file selected"}), 400
    if not file.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only .csv files are supported"}), 400

    reset_before_ingest = request.form.get("reset", "true").lower() == "true"

    try:
        CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        file.save(str(CSV_PATH))

        init_db()
        if reset_before_ingest:
            with db_cursor(commit=True) as cur:
                cur.execute("DELETE FROM stocks")
                cur.execute("DELETE FROM predictions")

        inserted = ingest_csv(Path(CSV_PATH))
        symbols = get_all_symbols()
        return (
            jsonify(
                {
                    "status": "ok",
                    "csv_path": str(CSV_PATH),
                    "inserted_rows": inserted,
                    "symbol_count": len(symbols),
                    "reset": reset_before_ingest,
                }
            ),
            200,
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@stocks_bp.route("/admin/refresh-live-prices", methods=["POST"])
@admin_required
def refresh_live_prices():
    try:
        body = request.get_json(silent=True) or {}
        requested_symbols = body.get("symbols")
        limit = int(body.get("limit", 80))
        if limit <= 0:
            limit = 80

        watch = load_watchlist()
        symbols = get_all_symbols()
        if watch:
            symbols = [s for s in symbols if s in watch]

        if isinstance(requested_symbols, list) and requested_symbols:
            requested = {str(s).strip().upper() for s in requested_symbols if str(s).strip()}
            symbols = [s for s in symbols if s in requested]

        symbols = symbols[:limit]

        if not symbols:
            return jsonify({"status": "no_symbols", "refreshed": 0, "failed": 0, "symbols": []}), 200

        if ASYNC_MODE == "celery" and refresh_live_prices_task is not None:
            task = refresh_live_prices_task.delay(symbols)
            return (
                jsonify(
                    {
                        "status": "queued",
                        "task_id": task.id,
                        "symbols": symbols,
                        "limit": limit,
                    }
                ),
                202,
            )

        started = time.perf_counter()
        quotes = get_live_quotes_bulk(symbols, force_refresh=True, allow_network=True)
        elapsed_ms = (time.perf_counter() - started) * 1000

        refreshed = len(quotes)
        failed = max(0, len(symbols) - refreshed)
        return (
            jsonify(
                {
                    "status": "ok",
                    "refreshed": refreshed,
                    "failed": failed,
                    "symbols": symbols,
                    "limit": limit,
                    "elapsed_ms": round(elapsed_ms, 2),
                }
            ),
            200,
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@stocks_bp.route("/admin/append-and-retrain", methods=["POST"])
@admin_required
def append_and_retrain():
    try:
        body = request.get_json(silent=True) or {}

        csv_path_raw = str(body.get("csv_path", "")).strip()
        csv_path = Path(csv_path_raw).resolve() if csv_path_raw else None

        payload = append_and_quick_retrain(
            csv_path=csv_path,
            max_symbols=int(body.get("max_symbols", 120)),
            min_rows=int(body.get("min_rows", 120)),
            quick_overrides={
                "lookback": int(body.get("lookback", 30)),
                "hidden_size": int(body.get("hidden_size", 96)),
                "num_layers": int(body.get("layers", 2)),
                "dropout": float(body.get("dropout", 0.2)),
                "max_epochs": int(body.get("epochs", 20)),
                "learning_rate": float(body.get("lr", 0.0009)),
                "batch_size": int(body.get("batch_size", 64)),
                "patience": int(body.get("patience", 6)),
                "class_loss_weight": float(body.get("direction_weight", 0.6)),
                "seed": int(body.get("seed", 42)),
                "model_variant": str(body.get("variant", "lstm")),
            },
        )

        return jsonify(payload), 200
    except FileNotFoundError as exc:
        return jsonify({"error": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500
