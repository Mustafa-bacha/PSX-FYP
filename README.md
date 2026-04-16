# PSX Insight — Stock Market Intelligence Platform

## New Full JS Stack (React + Node + Express)

This repo now includes a complete JavaScript stack implementation:

- Express API: `server/`
- React app (Vite): `client/`

Core JS endpoints:

- `GET /api/health`
- `POST /api/auth/signup`
- `POST /api/auth/login`
- `GET /api/auth/me` (Bearer token)
- `GET /api/auth/google/start` (Google OAuth redirect)
- `GET /api/stocks`
- `GET /api/stock/:symbol?days=365`
- `GET /api/stock/:symbol/insights`
- `GET /api/market-performers`
- `POST /api/chat` (Groq-backed conversational stock analysis)

### Run the JS stack

```bash
npm install
npm --prefix server install
npm --prefix client install
npm run dev
```

Open:

- React app: `http://127.0.0.1:5173`
- Express API: `http://127.0.0.1:5001/api/health`

Environment:

- Copy `.env.example` to `.env` and set `GROQ_API_KEY`.
- Set `JWT_SECRET_KEY` for production.
- For Google login, set `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, and `GOOGLE_REDIRECT_URI`.
- Copy `client/.env.example` to `client/.env` if you need custom API base URL.

### Production readiness checklist

- Use unique strong values for `SECRET_KEY`, `JWT_SECRET_KEY`, `ADMIN_PASSWORD`.
- Keep `.env` private (never commit secrets).
- Keep `GROQ_API_KEY`, `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET` in environment/secret manager only.
- Set strict `CORS_ORIGINS` and `FRONTEND_URL` for your deployment domains.
- Enable structured logs in production: `JSON_LOGS=true`, `LOG_LEVEL=INFO`.
- If using workers, switch `ASYNC_MODE=celery` and run Redis + Celery worker.
- Decide scheduler policy explicitly:
   - `STARTUP_HISTORICAL_SYNC`
   - `FOCUS_AUTO_REFRESH_ENABLED`
   - `SENTIMENT_AUTO_ENABLED`
   - `MARKET_SUMMARY_AUTO_REFRESH_ENABLED`
   - For constrained environments, set these to `false` and trigger jobs via API/cron.
- Set up periodic maintenance jobs for:
   - incremental append + retrain (`/api/models/append-retrain`)
   - RAG refresh (`python backend/run_rag_refresh.py`)
- Run tests/build in CI before deploy.

> Security: never commit real API keys in `.env.example` or source files.

Production-ready PSX analytics platform with:
- historical market data ingestion into SQLite
- technical indicators (pure pandas/numpy)
- per-symbol deep learning forecasting (LSTM hybrid for price + direction)
- FinBERT sentiment analysis over financial headlines
- responsive dashboard UI with Chart.js visualizations

## Prerequisites

- Python 3.11 (recommended; 3.10 may also work)
- `pip`
- Optional: Docker + Docker Compose

## Project Structure

```text
psx-platform/
├── backend/
├── frontend/
├── data/
├── models/
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Local Setup

From project root:

```bash
cd backend
pip install -r requirements.txt
cd ..
python backend/init_db.py
python backend/app.py
```

Open frontend directly:

```bash
cd frontend
python -m http.server 8080
```

Then browse:
- Home: `http://localhost:8080/index.html`
- Dashboard: `http://localhost:8080/dashboard.html`

### One-command Run (recommended)

From project root:

```bash
./run.sh
```

This script:
- starts backend with the configured venv interpreter
- starts frontend static server from the correct `frontend/` directory
- writes logs to `.runtime/backend.log` and `.runtime/frontend.log`
- stores PID files in `.runtime/`

Stop both services:

```bash
./stop.sh
```

### Environment Configuration

Copy the production template and update secrets:

```bash
cp .env.example .env
```

Key variables:
- `SECRET_KEY`, `JWT_SECRET_KEY`
- `ADMIN_USERNAME`, `ADMIN_PASSWORD`
- `CORS_ORIGINS`
- `ASYNC_MODE` (`thread` or `celery`)
- `JSON_LOGS` (`true/false`)

## Admin Authentication (JWT)

Protected endpoints now require an admin JWT token:
- `POST /train`
- `GET /train/status`
- `POST /admin/upload-csv`
- `POST /admin/refresh-live-prices`

Get a token:

```bash
curl -X POST http://localhost:5000/auth/login \
   -H "Content-Type: application/json" \
   -d '{"username":"admin","password":"admin123"}'
```

Use it in requests:

```bash
curl -X POST http://localhost:5000/train \
   -H "Content-Type: application/json" \
   -H "Authorization: Bearer <TOKEN>" \
   -d '{"symbol":"ALL"}'
```

## Docker Setup

```bash
docker-compose up --build
```

Services:
- Backend API: `http://localhost:5000`
- Frontend: `http://localhost:8080/dashboard.html`
- Redis: `localhost:6379`

If `ASYNC_MODE=celery`, start both `backend` and `worker` services.

## Observability

- Every HTTP response includes `X-Request-ID`.
- Request logs include method, path, status, and latency.
- Set `JSON_LOGS=true` for structured logs in production.

## AI Chatbot (Integrated in Dashboard)

This repository includes an integrated Groq-powered chatbot for stock analysis directly inside the React dashboard (below chart and prediction).

### Environment

Set your Groq key in `.env` (never hardcode):

```bash
GROQ_API_KEY=your_real_key_here
GROQ_MODEL=llama-3.1-8b-instant
```

After setting the key, open the main dashboard UI and use the embedded chat panel.

### Advanced RAG indexing (historical + trend + news + reports)

The Python RAG pipeline now builds a richer corpus from:

- full symbol history summaries from `data/psx_platform.db`
- multi-timeframe trend docs (5/20/60-session context)
- sentiment regime + recent headline docs
- local report artifacts under `data/reports/*`
- curated files in `data/rag_docs/`

Rebuild the vector index manually:

```bash
cd backend
python run_rag_refresh.py
```

Download full PSX history first, then refresh index:

```bash
cd backend
python run_rag_refresh.py --download-psx-history
```

Use shorter request timeouts if PSX site is slow/unresponsive:

```bash
cd backend
python run_rag_refresh.py --download-psx-history --download-timeout-sec 8
```

Note: when a symbol cannot be fetched live, the downloader now falls back to existing local CSV rows (if available) so the pipeline can continue.

Download history + quick retrain + refresh index in one flow:

```bash
cd backend
python run_rag_refresh.py --download-psx-history --train-mode quick
```

If your system `python3` misses packages, use project venv explicitly:

```bash
cd backend
/home/sertv2cs/Desktop/Mustafa/hassan/.venv/bin/python run_rag_refresh.py --download-psx-history --train-mode quick
```

Run as Celery task (if using async workers):

```bash
cd backend
celery -A celery_app.celery_app call tasks.refresh_rag_index
```

Recommended daily refresh cadence:

1. Run historical sync / append process.
2. Run sentiment/news ingestion cycle.
3. Run `python run_rag_refresh.py` to reindex latest evidence.

## Data Update Workflow

1. Run `backend/scraper.py` to append latest PSX rows to your CSV.
2. Ensure updated CSV is placed at `data/psx_historical.csv`.
   - If your source file is `new_psx_historical_.csv`, copy or rename it to `data/psx_historical.csv` before initialization.
3. Re-run initialization or call retraining endpoint:
   - `python backend/init_db.py`
   - `POST /train` with `{"symbol":"ALL"}`

## Incremental Append + Quick Retrain (Advanced Runtime Ops)

For daily operations, you can append only new rows and retrain only symbols impacted by new rows.

### CLI runner

Run from `psx-platform/backend`:

```bash
python run_incremental_update.py \
   --csv /home/sertv2cs/Desktop/hassan/new_psx_historical_.csv \
   --max-symbols 120 \
   --min-rows 120 \
   --epochs 20 \
   --variant lstm
```

### Admin API endpoint

`POST /admin/append-and-retrain` (admin JWT required)

Example body:

```json
{
   "csv_path": "/home/sertv2cs/Desktop/hassan/new_psx_historical_.csv",
   "max_symbols": 120,
   "min_rows": 120,
   "epochs": 20,
   "variant": "lstm"
}
```

Response includes:

- `inserted_rows`
- `changed_symbols`
- `symbols_selected_for_retrain`
- `trained_count` / `failed_count`
- per-symbol `trained_metrics`

This gives a practical production cadence:

1. append latest rows,
2. retrain only changed symbols quickly,
3. run full deep retrain (all symbols) overnight/weekly.

## Deep Training (LSTM Hybrid, Full Historical Data)

Use the deep-training runner to ingest full historical CSV and retrain the hybrid LSTM for all eligible symbols.

1. Keep your latest historical file at `new_psx_historical_.csv` (workspace root).
2. Run deep training from `psx-platform/backend`:

```bash
python train_hybrid_full.py \
   --csv /home/sertv2cs/Desktop/hassan/new_psx_historical_.csv \
   --symbol ALL \
   --min-rows 300 \
   --lookback 45 \
   --hidden-size 128 \
   --layers 2 \
   --dropout 0.25 \
   --epochs 80 \
   --patience 14 \
   --batch-size 64 \
   --lr 0.0007 \
   --direction-weight 0.65
```

This produces per-symbol model artifacts under `models/` and prints a JSON summary with average direction accuracy and error metrics.

### Resume after interruption (recommended)

Long bulk training can be interrupted manually (`Ctrl+C`) or by system limits. Use a fixed report file and `--resume` to continue from already completed symbols:

```bash
python train_hybrid_full.py \
   --csv /home/sertv2cs/Desktop/hassan/new_psx_historical_.csv \
   --symbol ALL \
   --min-rows 300 \
   --epochs 80 \
   --report-path /home/sertv2cs/Desktop/hassan/psx-platform/data/reports/hybrid_full_run.json \
   --save-every 5
```

Resume command:

```bash
python train_hybrid_full.py \
   --csv /home/sertv2cs/Desktop/hassan/new_psx_historical_.csv \
   --symbol ALL \
   --min-rows 300 \
   --epochs 80 \
   --resume \
   --report-path /home/sertv2cs/Desktop/hassan/psx-platform/data/reports/hybrid_full_run.json
```

> Tip: keep each line ending with `\` only once, and avoid extra pasted characters in the command.

### Full-history inspection + boosted heavy training (target accuracy)

The runner now inspects full historical DB coverage before training and includes a `data_profile` section in the JSON report.

Use boosted mode to retry lower-performing symbols with heavier variants (`bilstm`, `gru`) and keep the best attempt per symbol:

```bash
python train_hybrid_full.py \
   --csv /home/sertv2cs/Desktop/hassan/new_psx_historical_.csv \
   --symbol ALL \
   --min-rows 300 \
   --lookback 45 \
   --hidden-size 128 \
   --layers 2 \
   --dropout 0.25 \
   --epochs 80 \
   --patience 14 \
   --batch-size 64 \
   --lr 0.0007 \
   --direction-weight 0.65 \
   --base-variant lstm \
   --target-accuracy 0.80 \
   --boost \
   --boost-variants bilstm,gru \
   --boost-hidden-size 192 \
   --boost-layers 3 \
   --boost-epochs 140 \
   --boost-patience 22 \
   --boost-batch-size 96 \
   --boost-lr 0.0005 \
   --save-every 5 \
   --report-path /home/sertv2cs/Desktop/hassan/psx-platform/data/reports/hybrid_full_boosted.json
```

Resume the same run:

```bash
python train_hybrid_full.py \
   --csv /home/sertv2cs/Desktop/hassan/new_psx_historical_.csv \
   --symbol ALL \
   --min-rows 300 \
   --target-accuracy 0.80 \
   --boost \
   --resume \
   --report-path /home/sertv2cs/Desktop/hassan/psx-platform/data/reports/hybrid_full_boosted.json
```

> Practical expectation: a global 80% direction accuracy across all PSX symbols is usually not realistic because of noisy and regime-changing markets. Use `target-accuracy` as a per-symbol optimization goal and track hit-rate in the output report.

### Optional quick smoke run

Use a small symbol subset first:

```bash
python train_hybrid_full.py \
   --csv /home/sertv2cs/Desktop/hassan/new_psx_historical_.csv \
   --symbol ALL \
   --min-rows 300 \
   --limit 5 \
   --epochs 20
```

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Service health check |
| GET | `/stocks` | List all symbols |
| GET | `/market-performers` | Daily PSX top active, advancers, decliners (cached) |
| GET | `/stock/<symbol>?days=365` | Historical OHLCV + technical summary |
| GET | `/predict/<symbol>` | Next-day forecast (lazy training with 202 while training) |
| POST | `/train` | Trigger background training for ALL or one symbol |
| GET | `/train/status` | Current in-memory training states |
| GET | `/sentiment/<symbol>` | Cached/updated sentiment summary and headlines |

## Notes

- CORS is open in development and controlled in production via `CORS_ORIGINS`.
- Sentiment scraping is cached (6 hours) to reduce repeated requests.
- CSV ingestion is idempotent using unique `(symbol, date)` and `INSERT OR IGNORE`.
- Monetary values are returned rounded to 2 decimals where applicable.
- Homepage `index.html` now loads Market Performers from `https://dps.psx.com.pk/performers` via backend endpoint `/market-performers`.
- If live PSX fetch fails, endpoint returns last cached performers data when available.
