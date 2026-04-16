from __future__ import annotations

import requests

from .config import GROQ_API_KEY, GROQ_BASE_URL, GROQ_MODEL

_SYSTEM_PROMPT = (
    "You are an expert PSX (Pakistan Stock Exchange) financial analyst. "
    "You produce detailed, data-driven analysis grounded STRICTLY in the retrieved context provided by the user. "
    "NEVER fabricate numbers, dates, prices, or statistics. "
    "When the context contains specific data points (prices, returns, sentiment scores, volumes, MA levels), "
    "you MUST cite them explicitly and explain what they mean for the investor. "
    "Do not give vague platitudes like 'consider your risk tolerance' — instead, reference the actual "
    "volatility numbers, support/resistance levels, and momentum signals from the context. "
    "If the context is insufficient, say exactly what data is missing rather than guessing. "
    "Structure your response with clear sections and always ground every claim in a specific data point. "
    "IMPORTANT ESCAPE HATCH: If the user question is a simple greeting (like 'how are you'), "
    "conversational (like 'what's up'), general knowledge (like 'capital of pakistan', 'president of america'), "
    "math ('2+2'), or otherwise COMPLETELY UNRELATED to the PSX / stock analysis, YOU MUST REFUSE TO ANALYZE. "
    "Instead, politely reply: 'I am focused on PSX market analysis, so I cannot answer general or unrelated questions. "
    "How can I help you with stock data today?' and DO NOT output any analysis formatting."
)


class GroqClient:
    def __init__(self) -> None:
        self.api_key = GROQ_API_KEY
        self.base_url = GROQ_BASE_URL
        self.model = GROQ_MODEL

    def generate(self, prompt: str) -> str:
        if not self.api_key:
            raise RuntimeError("GROQ_API_KEY is missing. Add it to your .env file.")

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.12,
            "max_tokens": 2400,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        response = requests.post(self.base_url, json=payload, headers=headers, timeout=90)
        response.raise_for_status()
        data = response.json()
        return (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "No response generated.")
            .strip()
        )
