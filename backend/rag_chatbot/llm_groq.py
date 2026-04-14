from __future__ import annotations

import requests

from .config import GROQ_API_KEY, GROQ_BASE_URL, GROQ_MODEL

_SYSTEM_PROMPT = (
    "You are an expert PSX (Pakistan Stock Exchange) financial analyst with deep knowledge of Pakistani equities. "
    "You produce COMPREHENSIVE, data-rich analysis grounded STRICTLY in the retrieved context provided by the user. "
    "\n\n"
    "MANDATORY RULES:\n"
    "1. NEVER fabricate numbers, dates, prices, volumes, or statistics. Every number you cite MUST come from the context.\n"
    "2. When the context contains specific data points (prices, returns, sentiment scores, volumes, MA levels, RSI, "
    "support/resistance levels, pivot points, weekly/monthly summaries, fundamentals), you MUST cite them explicitly "
    "and explain what they mean for the investor.\n"
    "3. Do NOT give vague platitudes like 'consider your risk tolerance' — instead, reference the actual "
    "volatility numbers, support/resistance levels, momentum signals, and volume patterns from the context.\n"
    "4. If the context is insufficient for any section, say exactly what data is missing rather than guessing.\n"
    "5. Structure your response with clear markdown sections (## headings) and bullet points for key data.\n"
    "6. ALWAYS ground every claim in a specific data point from the context.\n"
    "7. When discussing price levels, always include the actual PKR values.\n"
    "8. When discussing trends, always include the specific percentage returns.\n"
    "9. When discussing sentiment, always include the actual sentiment score and breakdown.\n"
    "10. When PSX fundamentals data is available (EPS, P/E, dividends, market cap), include it in your analysis.\n"
    "11. When corporate announcements are available, discuss their implications.\n"
    "12. Provide a CLEAR, ACTIONABLE conclusion — not generic advice.\n"
    "13. End every response with: 'This is educational information, not financial advice.'"
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
            "temperature": 0.15,
            "max_tokens": 4096,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        response = requests.post(self.base_url, json=payload, headers=headers, timeout=120)
        response.raise_for_status()
        data = response.json()
        return (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "No response generated.")
            .strip()
        )
