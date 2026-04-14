from __future__ import annotations

from celery_app import celery_app
from models.stock_predictor import train_all_symbols, train_model
from rag_chatbot.rag_pipeline import StockRAGPipeline
from services.market_data import get_live_quotes_bulk


@celery_app.task(name="tasks.train_symbols")
def train_symbols(symbol: str = "ALL") -> dict:
    symbol = str(symbol or "ALL").strip().upper()
    if symbol == "ALL":
        train_all_symbols()
        return {"status": "completed", "symbol": "ALL"}

    train_model(symbol)
    return {"status": "completed", "symbol": symbol}


@celery_app.task(name="tasks.refresh_live_prices")
def refresh_live_prices(symbols: list[str]) -> dict:
    normalized = [str(s).strip().upper() for s in symbols if str(s).strip()]
    quotes = get_live_quotes_bulk(normalized, force_refresh=True, allow_network=True)
    return {
        "status": "completed",
        "requested": len(normalized),
        "refreshed": len(quotes),
        "failed": max(0, len(normalized) - len(quotes)),
    }


@celery_app.task(name="tasks.refresh_rag_index")
def refresh_rag_index() -> dict:
    pipeline = StockRAGPipeline()
    stats = pipeline.reindex()
    return {
        "status": "completed",
        "indexed_chunks": int(stats.get("indexed_chunks", 0)),
        "stock_count": int(stats.get("stock_count", 0)),
    }
