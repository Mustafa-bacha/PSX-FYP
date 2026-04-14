from __future__ import annotations

import hashlib
import json

from sentence_transformers import SentenceTransformer

from .config import EMBEDDING_MODEL, RAG_CACHE_DIR

_CACHE_PATH = RAG_CACHE_DIR / "embedding_cache.json"


class EmbeddingService:
    def __init__(self) -> None:
        self.model = SentenceTransformer(EMBEDDING_MODEL)
        self.cache = self._load_cache()

    def _load_cache(self) -> dict[str, list[float]]:
        if _CACHE_PATH.exists():
            try:
                return json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                return {}
        return {}

    def _save_cache(self) -> None:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_PATH.write_text(json.dumps(self.cache), encoding="utf-8")

    @staticmethod
    def _key(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed a list of texts, using cache for previously seen inputs."""
        if not texts:
            return []

        to_compute: list[str] = []
        map_idx: list[int] = []
        vectors: list[list[float] | None] = [None] * len(texts)

        for i, text in enumerate(texts):
            key = self._key(text)
            cached = self.cache.get(key)
            if cached is not None:
                vectors[i] = cached
            else:
                to_compute.append(text)
                map_idx.append(i)

        if to_compute:
            # Batch encode for efficiency; normalize for cosine-style distance
            computed = self.model.encode(
                to_compute,
                normalize_embeddings=True,
                batch_size=64,
                show_progress_bar=False,
            ).tolist()
            for idx, vec in zip(map_idx, computed):
                key = self._key(texts[idx])
                self.cache[key] = vec
                vectors[idx] = vec
            self._save_cache()

        return [v for v in vectors if v is not None]

    def clear_cache(self) -> None:
        """Wipe the embedding cache to force recomputation."""
        self.cache = {}
        if _CACHE_PATH.exists():
            _CACHE_PATH.unlink()
