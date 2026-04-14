from __future__ import annotations

from functools import lru_cache

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .rag_pipeline import StockRAGPipeline
from .schemas import ChatRequest, ChatResponse, ReindexResponse, RetrievedChunk


@lru_cache(maxsize=1)
def get_pipeline() -> StockRAGPipeline:
    pipeline = StockRAGPipeline()
    pipeline.reindex()
    return pipeline


app = FastAPI(title="PSX RAG Chatbot API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "rag-chatbot"}


@app.post("/reindex", response_model=ReindexResponse)
def reindex() -> ReindexResponse:
    try:
        stats = get_pipeline().reindex()
        return ReindexResponse(**stats)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    try:
        pipeline = get_pipeline()
        result = pipeline.ask(
            stock=req.stock,
            question=req.question,
            history=[m.model_dump() for m in req.history],
            top_k=req.top_k,
        )
        retrieved = [
            RetrievedChunk(text=x["text"], score=x["score"], metadata=x["metadata"])
            for x in result.retrieved
        ]
        return ChatResponse(answer=result.answer, sentiment=result.sentiment, retrieved=retrieved)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
