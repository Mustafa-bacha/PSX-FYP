from __future__ import annotations

from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    role: str = Field(..., description="user|assistant")
    content: str = Field(..., min_length=1)


class ChatRequest(BaseModel):
    stock: str = Field(..., description="Ticker or company name")
    question: str = Field(..., min_length=1)
    top_k: int = Field(default=10, ge=1, le=20)
    history: list[ChatMessage] = Field(default_factory=list)


class RetrievedChunk(BaseModel):
    text: str
    score: float
    metadata: dict


class ChatResponse(BaseModel):
    answer: str
    sentiment: str
    retrieved: list[RetrievedChunk]


class ReindexResponse(BaseModel):
    indexed_chunks: int
    stock_count: int
