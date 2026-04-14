from __future__ import annotations

from typing import Any

import chromadb
from chromadb.config import Settings

from .config import RAG_STORE_DIR


class VectorStore:
    def __init__(self, collection_name: str = "stock_rag_docs") -> None:
        self.client = chromadb.PersistentClient(
            path=str(RAG_STORE_DIR),
            settings=Settings(anonymized_telemetry=False),
        )
        self.collection = self.client.get_or_create_collection(name=collection_name)

    def clear(self) -> None:
        self.client.delete_collection(self.collection.name)
        self.collection = self.client.get_or_create_collection(name="stock_rag_docs")

    def upsert(
        self,
        ids: list[str],
        documents: list[str],
        metadatas: list[dict[str, Any]],
        embeddings: list[list[float]],
    ) -> None:
        if not ids:
            return
        self.collection.upsert(
            ids=ids,
            documents=documents,
            metadatas=metadatas,
            embeddings=embeddings,
        )

    def query(
        self,
        embedding: list[float],
        top_k: int,
        stock_filter: str | None = None,
        doc_type_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """Query the vector store with optional stock and doc_type filters."""
        where_clauses: list[dict[str, Any]] = []
        if stock_filter:
            where_clauses.append({"stock": stock_filter.upper()})
        if doc_type_filter:
            where_clauses.append({"doc_type": doc_type_filter})

        where: dict[str, Any] | None = None
        if len(where_clauses) == 1:
            where = where_clauses[0]
        elif len(where_clauses) > 1:
            where = {"$and": where_clauses}

        result = self.collection.query(
            query_embeddings=[embedding],
            n_results=top_k,
            where=where,
        )

        docs = result.get("documents", [[]])[0]
        metas = result.get("metadatas", [[]])[0]
        dists = result.get("distances", [[]])[0]

        output: list[dict[str, Any]] = []
        for text, meta, dist in zip(docs, metas, dists):
            output.append({"text": text, "metadata": meta or {}, "score": float(dist)})
        return output

    def count(self) -> int:
        """Return total number of documents in the collection."""
        return self.collection.count()
