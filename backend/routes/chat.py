"""Chat blueprint — wires the RAG chatbot into the Flask API server."""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from functools import lru_cache

from flask import Blueprint, jsonify, request

from config import DB_PATH

logger = logging.getLogger(__name__)

chat_bp = Blueprint("chat", __name__)


@lru_cache(maxsize=1)
def _get_pipeline():
    """Lazy-init and reindex once on first request."""
    from rag_chatbot.rag_pipeline import StockRAGPipeline

    pipeline = StockRAGPipeline()
    pipeline.reindex()
    return pipeline


def _get_pipeline_safe():
    """Return pipeline, triggering reindex on first call."""
    try:
        return _get_pipeline()
    except Exception as exc:
        logger.exception("Failed to initialise RAG pipeline")
        raise exc


@chat_bp.route("/chat", methods=["POST"])
def chat():
    body = request.get_json(silent=True) or {}
    stock = str(body.get("stock", "MARKET")).strip().upper()
    question = str(body.get("question", "")).strip()
    history_raw = body.get("history", [])
    top_k = min(int(body.get("top_k", 10)), 20)

    if not question:
        return jsonify({"error": "question is required"}), 400

    # Normalise history
    history = []
    for msg in history_raw[-12:]:
        role = str(msg.get("role", "user"))
        content = str(msg.get("content", "")).strip()
        if content:
            history.append({"role": role, "content": content})

    try:
        pipeline = _get_pipeline_safe()
        result = pipeline.ask(
            stock=stock,
            question=question,
            history=history,
            top_k=top_k,
        )

        # Build retrieval metadata for the frontend
        historical_chunks = sum(
            1 for r in result.retrieved
            if r.get("metadata", {}).get("doc_type") in ("historical", "trend", "price_action", "fundamentals")
        )
        news_chunks = sum(
            1 for r in result.retrieved
            if r.get("metadata", {}).get("doc_type") in ("news", "sentiment", "announcement")
        )

        sources = []
        seen_sources = set()
        for r in result.retrieved:
            src = r.get("metadata", {}).get("source", "unknown")
            doc_type = r.get("metadata", {}).get("doc_type", "general")
            key = f"{src}|{doc_type}"
            if key not in seen_sources:
                seen_sources.add(key)
                sources.append({"source": src, "doc_type": doc_type})

        return jsonify({
            "answer": result.answer,
            "sentiment": result.sentiment,
            "scope": stock,
            "retrieval": {
                "used_chunks": len(result.retrieved),
                "historical_chunks": historical_chunks,
                "news_chunks": news_chunks,
            },
            "sources": sources[:10],
        }), 200

    except Exception as exc:
        logger.exception("Chat request failed")
        return jsonify({"error": str(exc)}), 500


@chat_bp.route("/chat/reindex", methods=["POST"])
def reindex():
    """Force reindex of the RAG vector store."""
    try:
        pipeline = _get_pipeline_safe()
        stats = pipeline.reindex()
        return jsonify({
            "status": "ok",
            "indexed_chunks": stats.get("indexed_chunks", 0),
            "stock_count": stats.get("stock_count", 0),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }), 200
    except Exception as exc:
        logger.exception("Reindex failed")
        return jsonify({"error": str(exc)}), 500


@chat_bp.route("/user/chat-history/<symbol>", methods=["GET"])
def get_chat_history(symbol: str):
    """Stub for chat history retrieval — returns empty for now."""
    return jsonify({"symbol": symbol.upper(), "messages": []}), 200


@chat_bp.route("/user/chat-history/<symbol>", methods=["DELETE"])
def clear_chat_history(symbol: str):
    """Stub for clearing chat history."""
    return jsonify({"symbol": symbol.upper(), "cleared": True}), 200
