from __future__ import annotations

import re
from dataclasses import dataclass

from .chunker import chunk_text
from .config import RAG_CHUNK_OVERLAP, RAG_CHUNK_SIZE
from .data_ingestion import load_documents, save_mock_dataset_if_missing
from .embeddings import EmbeddingService
from .llm_groq import GroqClient
from .prompt_template import build_prompt
from .vector_store import VectorStore


@dataclass
class RAGResult:
    answer: str
    sentiment: str
    retrieved: list[dict]


# ---------------------------------------------------------------------------
# Question-type classification
# ---------------------------------------------------------------------------

_BUY_SELL_PATTERNS = re.compile(
    r"\b(buy|sell|hold|invest|entry|exit|position|trade|purchase|accumulate|book\s*profit|should\s*i)\b",
    re.IGNORECASE,
)
_OUTLOOK_PATTERNS = re.compile(
    r"\b(outlook|forecast|predict|expect|future|ahead|coming|next\s*week|next\s*month|tomorrow|this\s*week)\b",
    re.IGNORECASE,
)
_HISTORY_PATTERNS = re.compile(
    r"\b(histor|past|previous|trend|perform|return|track\s*record|how\s*(has|did|was)|weekly|monthly|last\s*\d+)\b",
    re.IGNORECASE,
)
_NEWS_PATTERNS = re.compile(
    r"\b(news|headline|sentiment|rumor|report|analyst|rating|upgrade|downgrade|announcement|filing|dividend)\b",
    re.IGNORECASE,
)
_RISK_PATTERNS = re.compile(
    r"\b(risk|danger|concern|warn|threat|downside|volatil|caution|red\s*flag|worst\s*case|stop\s*loss)\b",
    re.IGNORECASE,
)
_FUNDAMENTAL_PATTERNS = re.compile(
    r"\b(fundamental|eps|p/e|pe\s*ratio|revenue|profit|loss|book\s*value|dividend|market\s*cap|financ|balance\s*sheet|earning)\b",
    re.IGNORECASE,
)


def classify_question(question: str) -> str:
    """Classify a user question into a type for prompt routing."""
    q = question.lower().strip()
    if _BUY_SELL_PATTERNS.search(q):
        return "recommendation"
    if _FUNDAMENTAL_PATTERNS.search(q):
        return "fundamentals"
    if _OUTLOOK_PATTERNS.search(q):
        return "outlook"
    if _HISTORY_PATTERNS.search(q):
        return "historical"
    if _NEWS_PATTERNS.search(q):
        return "news_sentiment"
    if _RISK_PATTERNS.search(q):
        return "risk"
    return "general"


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class StockRAGPipeline:
    # Chroma L2 distance threshold — docs beyond this are noise
    RELEVANCE_THRESHOLD = 1.6

    def __init__(self) -> None:
        self.embedder = EmbeddingService()
        self.store = VectorStore()
        self.llm = GroqClient()

    # ── indexing ──────────────────────────────────────────────────────────

    def reindex(self) -> dict[str, int]:
        save_mock_dataset_if_missing()
        docs = load_documents()

        ids: list[str] = []
        chunks: list[str] = []
        metas: list[dict] = []

        for doc in docs:
            parts = chunk_text(doc["text"], chunk_size=RAG_CHUNK_SIZE, overlap=RAG_CHUNK_OVERLAP)
            for idx, part in enumerate(parts):
                published_at = doc.get("published_at")
                ids.append(f"{doc['id']}::chunk::{idx}")
                chunks.append(part)
                metas.append(
                    {
                        "stock": str(doc.get("stock", "GENERAL")).upper(),
                        "source": doc.get("source", "unknown"),
                        "doc_type": doc.get("doc_type", "general"),
                        "published_at": str(published_at) if published_at is not None else "",
                    }
                )

        vectors = self.embedder.embed(chunks)
        self.store.clear()
        self.store.upsert(ids=ids, documents=chunks, metadatas=metas, embeddings=vectors)

        unique_stocks = {m["stock"] for m in metas}
        return {
            "indexed_chunks": len(chunks),
            "stock_count": len(unique_stocks),
        }

    # ── retrieval helpers ──────────────────────────────────���──────────────

    @staticmethod
    def _tokenize(text: str) -> set[str]:
        return {
            tok
            for tok in " ".join((text or "").lower().split()).replace("|", " ").split(" ")
            if len(tok) >= 3
        }

    def _multi_query_retrieve(
        self,
        stock: str,
        question: str,
        question_type: str,
        history: list[dict],
        candidate_count: int,
    ) -> list[dict]:
        """Generate multiple query variants and merge results for better recall."""
        stock_filter = stock if stock and stock not in {"MARKET", "GENERAL", "ALL", ""} else None
        is_market = stock in {"MARKET", "GENERAL", "ALL", ""}

        # Build query variants based on question type
        queries = [f"{stock} stock: {question}"]

        if question_type == "recommendation":
            queries.append(f"{stock} trend analysis momentum buy sell signal support resistance key levels RSI")
            queries.append(f"{stock} sentiment news outlook risk volume analysis")
            queries.append(f"{stock} price action weekly performance recent sessions")
        elif question_type == "outlook":
            queries.append(f"{stock} trend momentum moving average direction weekly monthly")
            queries.append(f"{stock} news sentiment forecast upcoming catalyst")
            queries.append(f"{stock} key levels support resistance breakout")
        elif question_type == "historical":
            queries.append(f"{stock} historical profile price range performance returns all-time")
            queries.append(f"{stock} trend multi-timeframe 5 20 60 session return weekly monthly")
            queries.append(f"{stock} price action volume analysis historical volatility")
        elif question_type == "news_sentiment":
            queries.append(f"{stock} sentiment positive negative news headlines analysis trend")
            queries.append(f"{stock} latest news analyst report announcement PSX filing")
            queries.append(f"{stock} corporate announcement dividend earnings report")
        elif question_type == "risk":
            queries.append(f"{stock} risk volatility decline downside bearish stop loss")
            queries.append(f"{stock} sentiment negative concern warning threat")
            queries.append(f"{stock} support breakdown key levels volume decline")
        elif question_type == "fundamentals":
            queries.append(f"{stock} fundamentals EPS P/E ratio book value market cap revenue")
            queries.append(f"{stock} PSX company profile sector industry financial highlights")
            queries.append(f"{stock} dividend earnings announcement corporate action")
        else:
            queries.append(f"{stock} historical trend price close volume analysis")
            queries.append(f"{stock} sentiment news analysis latest headlines")
            queries.append(f"{stock} key levels support resistance fundamentals profile")

        # For MARKET scope, add market-wide queries
        if is_market:
            queries.append("PSX market breadth advancers decliners average change")
            queries.append("PSX top gainers losers most active volume")
            queries.append("market trend multi-day improving deteriorating")

        # Add conversational context query
        if history:
            last_msg = history[-1].get("content", "")
            if last_msg:
                queries.append(f"{stock} {last_msg[:200]}")

        # Embed all queries at once
        query_vecs = self.embedder.embed(queries)

        # Merge results from all queries, dedup by text
        seen_texts: set[str] = set()
        merged: list[dict] = []

        for qvec in query_vecs:
            results = self.store.query(qvec, top_k=candidate_count, stock_filter=stock_filter)
            for doc in results:
                text_key = str(doc.get("text", ""))[:120]
                if text_key not in seen_texts:
                    seen_texts.add(text_key)
                    merged.append(doc)

        # For MARKET scope, also query without filter to get market-wide docs
        if is_market and query_vecs:
            market_results = self.store.query(query_vecs[0], top_k=candidate_count, stock_filter=None)
            for doc in market_results:
                text_key = str(doc.get("text", ""))[:120]
                if text_key not in seen_texts:
                    seen_texts.add(text_key)
                    merged.append(doc)

        # Fallback without filter only if nothing found for this stock
        if not merged and stock_filter:
            results = self.store.query(query_vecs[0], top_k=candidate_count, stock_filter=None)
            for doc in results:
                doc.setdefault("metadata", {})["_unfiltered_fallback"] = True
                merged.append(doc)

        return merged

    def _rerank(
        self,
        *,
        question: str,
        stock: str,
        question_type: str,
        docs: list[dict],
        top_k: int,
    ) -> list[dict]:
        """Score and rerank retrieved docs using question-type-aware weights."""
        question_tokens = self._tokenize(question)
        stock = (stock or "").strip().upper()

        # Question-type-specific doc_type preferences (now includes new doc types)
        type_boosts: dict[str, dict[str, float]] = {
            "recommendation": {
                "trend": 0.25, "price_action": 0.20, "sentiment": 0.15,
                "historical": 0.10, "news": 0.12, "report": 0.08,
                "fundamentals": 0.12, "announcement": 0.10,
            },
            "outlook": {
                "trend": 0.25, "sentiment": 0.20, "news": 0.18,
                "price_action": 0.15, "historical": 0.08, "report": 0.06,
                "fundamentals": 0.08, "announcement": 0.10,
            },
            "historical": {
                "historical": 0.25, "trend": 0.22, "price_action": 0.18,
                "report": 0.10, "sentiment": 0.05, "news": 0.04,
                "fundamentals": 0.08, "announcement": 0.04,
            },
            "news_sentiment": {
                "news": 0.25, "sentiment": 0.25, "announcement": 0.18,
                "report": 0.12, "trend": 0.05, "historical": 0.04,
                "price_action": 0.03, "fundamentals": 0.08,
            },
            "risk": {
                "sentiment": 0.22, "news": 0.20, "trend": 0.18,
                "price_action": 0.15, "historical": 0.08, "report": 0.06,
                "fundamentals": 0.06, "announcement": 0.10,
            },
            "fundamentals": {
                "fundamentals": 0.30, "announcement": 0.22, "report": 0.15,
                "historical": 0.08, "trend": 0.05, "news": 0.08,
                "sentiment": 0.05, "price_action": 0.03,
            },
            "general": {
                "trend": 0.18, "historical": 0.15, "sentiment": 0.12,
                "news": 0.10, "price_action": 0.12, "report": 0.08,
                "fundamentals": 0.10, "announcement": 0.08,
            },
        }
        boosts = type_boosts.get(question_type, type_boosts["general"])

        ranked: list[tuple[float, dict]] = []
        for d in docs:
            metadata = d.get("metadata") or {}
            text = str(d.get("text") or "")
            text_tokens = self._tokenize(text)
            overlap = len(question_tokens.intersection(text_tokens))
            doc_type = str(metadata.get("doc_type") or "").lower()
            doc_stock = str(metadata.get("stock", "")).upper()

            # Stock match
            scope_match = 1.0 if stock and stock not in {"", "MARKET", "GENERAL"} and doc_stock == stock else 0.0
            # For MARKET queries, boost MARKET docs
            if stock in {"MARKET", "GENERAL", ""} and doc_stock == "MARKET":
                scope_match = 0.8

            # Fallback penalty
            is_fallback = metadata.get("_unfiltered_fallback", False)
            fallback_penalty = -0.4 if is_fallback and doc_stock != stock else 0.0

            # Question-type-aware doc_type boost
            priority = 0.0
            for dtype_key, boost_val in boosts.items():
                if dtype_key in doc_type:
                    priority += boost_val
                    break

            # Ticker mention in text
            ticker_in_text = 0.12 if stock and stock in text.upper() else 0.0

            # Vector similarity (Chroma L2: lower = better)
            vector_distance = float(d.get("score") or 0.0)
            vector_relevance = 1.0 / (1.0 + max(0.0, vector_distance))

            # Recency boost: prefer docs with published_at
            recency_boost = 0.03 if metadata.get("published_at") else 0.0

            score = (
                (vector_relevance * 0.35)
                + (0.05 * min(overlap, 6))
                + (0.25 * scope_match)
                + ticker_in_text
                + priority
                + fallback_penalty
                + recency_boost
            )
            ranked.append((score, d))

        ranked.sort(key=lambda item: item[0], reverse=True)
        return [doc for _, doc in ranked[:top_k]]

    # ── sentiment classification ──────────────────────────────────────────

    def _classify_sentiment(self, retrieved_docs: list[dict]) -> str:
        blob = " ".join(d.get("text", "") for d in retrieved_docs).lower()
        positives = [
            "growth", "strong", "resilient", "upgrade", "profit", "positive",
            "bullish", "surge", "outperform", "gain", "rally", "optimistic",
            "improving", "golden cross", "breakout", "accumulate",
        ]
        negatives = [
            "decline", "risk", "loss", "volatility", "downgrade", "negative",
            "bearish", "concern", "warning", "weak", "drop", "pessimistic",
            "deteriorating", "death cross", "breakdown", "distribution",
        ]

        pos = sum(blob.count(w) for w in positives)
        neg = sum(blob.count(w) for w in negatives)

        if pos > neg * 1.3:
            return "positive"
        if neg > pos * 1.3:
            return "negative"
        return "neutral"

    # ── context assembly ──────────────────────────────────────────────────

    @staticmethod
    def _assemble_context(retrieved: list[dict]) -> str:
        """Organize retrieved docs by type for clearer LLM context."""
        by_type: dict[str, list[str]] = {}
        for d in retrieved:
            doc_type = str(d.get("metadata", {}).get("doc_type", "other"))
            source = d.get("metadata", {}).get("source", "unknown")
            date = d.get("metadata", {}).get("published_at", "")
            entry = f"[{source} | {date}] {d['text']}"
            by_type.setdefault(doc_type, []).append(entry)

        type_labels = {
            "historical": "HISTORICAL DATA & PROFILE",
            "trend": "TREND & TECHNICAL ANALYSIS",
            "price_action": "RECENT PRICE ACTION & VOLUME",
            "sentiment": "SENTIMENT ANALYSIS",
            "news": "NEWS HEADLINES",
            "report": "ANALYST REPORTS",
            "fundamentals": "COMPANY FUNDAMENTALS (PSX)",
            "announcement": "CORPORATE ANNOUNCEMENTS & FILINGS",
        }

        sections: list[str] = []
        # Order: fundamentals first, then trend/price, then historical, sentiment, news, announcements, reports
        for dtype in [
            "fundamentals", "trend", "price_action", "historical",
            "sentiment", "news", "announcement", "report", "other",
        ]:
            entries = by_type.get(dtype, [])
            if not entries:
                continue
            label = type_labels.get(dtype, dtype.upper())
            sections.append(f"=== {label} ===")
            sections.extend(entries)
            sections.append("")

        return "\n".join(sections).strip()

    # ── main ask method ───────────────────────────────────────────────────

    def ask(self, stock: str, question: str, history: list[dict], top_k: int = 10) -> RAGResult:
        stock = (stock or "").strip().upper()
        question_type = classify_question(question)

        # Multi-query retrieval for better recall — fetch more candidates
        candidate_count = max(top_k * 5, 30)
        all_candidates = self._multi_query_retrieve(
            stock=stock,
            question=question,
            question_type=question_type,
            history=history,
            candidate_count=candidate_count,
        )

        # Filter out irrelevant docs by distance threshold
        all_candidates = [
            d for d in all_candidates
            if float(d.get("score", 999)) < self.RELEVANCE_THRESHOLD
        ]

        # Rerank with question-type awareness — retrieve more for richer context
        effective_top_k = max(top_k, 14)
        retrieved = self._rerank(
            question=question,
            stock=stock,
            question_type=question_type,
            docs=all_candidates,
            top_k=effective_top_k,
        )

        # Assemble structured context
        context_block = self._assemble_context(retrieved)
        history_block = "\n".join(
            [f"{m.get('role', 'user')}: {m.get('content', '')}" for m in history[-10:]]
        )

        prompt = build_prompt(
            stock_name=stock or "UNKNOWN",
            question_type=question_type,
            retrieved_docs=context_block or "No relevant context found for this stock.",
            chat_history=history_block or "No previous messages.",
            user_query=question,
        )

        answer = self.llm.generate(prompt)
        sentiment = self._classify_sentiment(retrieved)
        return RAGResult(answer=answer, sentiment=sentiment, retrieved=retrieved)
