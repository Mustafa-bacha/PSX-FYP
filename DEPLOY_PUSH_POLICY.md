# Safe Push Policy for `psx-platform`

This project contains large runtime artifacts and local secrets. Use this policy before every push.

## Push these (source of truth)

- `backend/` (code + tests)
- `client/` and/or `frontend/` (choose the UI you deploy)
- `server/` (if using JS backend)
- `Dockerfile`, `docker-compose.yml`, `README.md`, `DESIGN.md`
- `.env.example`, `client/.env.example`
- lightweight static metadata like `data/stocks_name.txt`

## Do NOT push these

- Any `.env` file with real keys
- `models/` (trained `.pt` and metrics artifacts)
- SQLite runtime DB files: `data/psx_platform.db*`
- RAG runtime stores/caches: `data/rag_cache/`, `data/rag_vector_store/`
- Reports and downloaded PDFs in `data/reports/`
- Large CSV snapshots (`data/new_psx_historical_.csv`, `data/psx_historical.csv`)
- `node_modules/`, `__pycache__/`, `.pytest_cache/`, logs

## Critical security note

During local audit, non-empty secret values were detected in local `.env` files. If these were ever pushed anywhere, rotate immediately:

- `GROQ_API_KEY`
- `GOOGLE_CLIENT_SECRET`
- `SECRET_KEY`
- `JWT_SECRET_KEY`
- `ADMIN_PASSWORD`

## Recommended repo structure strategy

1. Keep code in GitHub.
2. Keep data/models in object storage or generated at runtime.
3. Store secrets only in deployment platform env vars.
4. Keep `ASYNC_MODE=thread` for free-tier beginner deployment.

## Before pushing: quick checklist

- [ ] Confirm `.env` is not staged
- [ ] Confirm `models/` is not staged
- [ ] Confirm `data/psx_platform.db*` is not staged
- [ ] Confirm no `node_modules/` is staged
- [ ] Confirm only source/config/docs are staged

## If you accidentally staged sensitive/large files

Use these commands from `psx-platform/`:

```bash
git rm -r --cached .
git add .
git status
```

Then verify staged files carefully and commit.

## If secrets were already pushed

1. Rotate compromised keys immediately.
2. Remove secrets from history (BFG or `git filter-repo`).
3. Force-push cleaned history.
4. Revoke old credentials.
