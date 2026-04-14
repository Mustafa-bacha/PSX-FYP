from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
RAG_DOCS_DIR = DATA_DIR / "rag_docs"
RAG_STORE_DIR = DATA_DIR / "rag_vector_store"
RAG_CACHE_DIR = DATA_DIR / "rag_cache"

EMBEDDING_MODEL = os.getenv("RAG_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_BASE_URL = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1/chat/completions")

# Larger chunks preserve more context per retrieval hit
RAG_CHUNK_SIZE = int(os.getenv("RAG_CHUNK_SIZE", "1200"))
RAG_CHUNK_OVERLAP = int(os.getenv("RAG_CHUNK_OVERLAP", "200"))

for p in (RAG_DOCS_DIR, RAG_STORE_DIR, RAG_CACHE_DIR):
    p.mkdir(parents=True, exist_ok=True)
