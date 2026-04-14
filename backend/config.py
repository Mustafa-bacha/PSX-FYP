from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
BACKEND_DIR = BASE_DIR / "backend"
DATA_DIR = BASE_DIR / "data"
MODEL_DIR = BASE_DIR / "models"
DB_PATH = DATA_DIR / "psx_platform.db"
CSV_PATH = DATA_DIR / "psx_historical.csv"
STOCKS_NAME_PATH = DATA_DIR / "stocks_name.txt"
REPORTS_DIR = DATA_DIR / "reports"
MARKET_PERFORMERS_CACHE_PATH = DATA_DIR / "market_performers_cache.json"

FLASK_ENV = os.getenv("FLASK_ENV", "development")
DEBUG = FLASK_ENV == "development"
APP_NAME = os.getenv("APP_NAME", "psx-platform")
API_VERSION = os.getenv("API_VERSION", "v1")

DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DB_PATH}")

SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-me")
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", SECRET_KEY)
JWT_ACCESS_TOKEN_EXPIRES_HOURS = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRES_HOURS", "12"))

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")

# In development, allow all origins. In production, restrict via CORS_ORIGINS env var.
CORS_ORIGINS = (
    "*"
    if DEBUG
    else [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
)

NEWS_CACHE_HOURS = int(os.getenv("NEWS_CACHE_HOURS", "6"))
SENTIMENT_CACHE_HOURS = int(os.getenv("SENTIMENT_CACHE_HOURS", "6"))
LIVE_QUOTE_CACHE_MINUTES = int(os.getenv("LIVE_QUOTE_CACHE_MINUTES", "10"))
MARKET_PERFORMERS_CACHE_MINUTES = int(os.getenv("MARKET_PERFORMERS_CACHE_MINUTES", "30"))

ASYNC_MODE = os.getenv("ASYNC_MODE", "thread").strip().lower()  # thread | celery
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", CELERY_BROKER_URL)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
JSON_LOGS = os.getenv("JSON_LOGS", "false").strip().lower() in {"1", "true", "yes"}

for required_dir in (DATA_DIR, MODEL_DIR, REPORTS_DIR):
    required_dir.mkdir(parents=True, exist_ok=True)
