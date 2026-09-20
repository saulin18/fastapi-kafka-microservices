# Independent Kafka consumer (inbox / dedup lives here later)

Nested inside the monorepo, so use a **local** `.venv` (uv may otherwise pick the parent project):

```bash
cd independent_consumer
uv venv
UV_PROJECT_ENVIRONMENT=.venv uv sync
UV_PROJECT_ENVIRONMENT=.venv uv run fastapi dev --port 8002
```

Health: `GET http://localhost:8002/health`
