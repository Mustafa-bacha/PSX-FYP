from __future__ import annotations

import re


def chunk_text(text: str, chunk_size: int = 900, overlap: int = 150) -> list[str]:
    """Split text into chunks on sentence boundaries to preserve meaning."""
    cleaned = " ".join((text or "").split())
    if not cleaned:
        return []

    if chunk_size <= 0 or len(cleaned) <= chunk_size:
        return [cleaned]

    # Split into sentences (handles '. ', '? ', '! ', '| ')
    sentences = re.split(r'(?<=[.!?|])\s+', cleaned)
    sentences = [s.strip() for s in sentences if s.strip()]

    if not sentences:
        return [cleaned]

    chunks: list[str] = []
    current_chunk: list[str] = []
    current_len = 0

    for sentence in sentences:
        sentence_len = len(sentence)

        # If a single sentence exceeds chunk_size, split it by characters as fallback
        if sentence_len > chunk_size:
            # Flush current chunk first
            if current_chunk:
                chunks.append(" ".join(current_chunk))
                current_chunk = []
                current_len = 0
            # Character-level split for oversized sentences
            step = max(1, chunk_size - max(0, overlap))
            for i in range(0, sentence_len, step):
                chunks.append(sentence[i : i + chunk_size])
            continue

        # If adding this sentence would exceed chunk_size, flush
        new_len = current_len + (1 if current_chunk else 0) + sentence_len
        if new_len > chunk_size and current_chunk:
            chunks.append(" ".join(current_chunk))
            # Keep last few sentences for overlap
            overlap_chunk: list[str] = []
            overlap_len = 0
            for s in reversed(current_chunk):
                if overlap_len + len(s) + 1 > overlap:
                    break
                overlap_chunk.insert(0, s)
                overlap_len += len(s) + 1
            current_chunk = overlap_chunk
            current_len = sum(len(s) for s in current_chunk) + max(0, len(current_chunk) - 1)

        current_chunk.append(sentence)
        current_len += (1 if len(current_chunk) > 1 else 0) + sentence_len

    if current_chunk:
        chunks.append(" ".join(current_chunk))

    return chunks
