

## Open Finance-like technical test with FastAPI microservices: Kafka, idempotency, background processing, Redis, and Celery.

- English: [`../technical-test-brief.md`](../technical-test-brief.md)
- Español: [`../enunciado-prueba-tecnica.md`](../enunciado-prueba-tecnica.md)

## Requirements

- Docker + Docker Compose
- [uv](https://docs.astral.sh/uv/)
- Python 3.12+

## Docker Compose

```bash
cd microservices-technical-test
docker compose up -d
```

Runs Zookeeper, Kafka (`localhost:9092`, container name **`broker`**), Redis (`6379`), Postgres (host **`5433`** → container `5432`, container name **`postgres`**). Default DB credentials: `postgres` / `password`.

### Create Kafka topic

```bash
docker exec broker kafka-topics --create \
  --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

docker exec broker kafka-topics --describe \
  --topic transactions \
  --bootstrap-server localhost:9092
```


Optional separate databases (one DB per service is usual):

```bash
docker exec postgres psql -U postgres -c "CREATE DATABASE producer_outbox;"
docker exec postgres psql -U postgres -c "CREATE DATABASE consumer_inbox;"
```

## Env variables

**Producer** (`independent_producer/.env`):

```env
database_url=postgresql+asyncpg://postgres:password@localhost:5433/producer_outbox
kafka_url=localhost:9092
kafka_topic=transactions
redis_url=redis://localhost:6379/0
```

**Consumer** (`independent_consumer/.env`):

```env
database_url=postgresql+asyncpg://postgres:password@localhost:5433/consumer_inbox
kafka_url=localhost:9092
kafka_topic=transactions
kafka_group_id=antifraude
redis_url=redis://localhost:6379/0
```

- **Same** `kafka_url` and `kafka_topic` on both apps (shared cluster/topic).
- **DB** and **Redis** may differ per service (e.g. different Postgres DB name or Redis DB index).

## Running migrations

From each app root:

```bash
cd independent_producer
uv venv && UV_PROJECT_ENVIRONMENT=.venv uv sync
UV_PROJECT_ENVIRONMENT=.venv uv run alembic upgrade head

cd ../independent_consumer
uv venv && UV_PROJECT_ENVIRONMENT=.venv uv sync
UV_PROJECT_ENVIRONMENT=.venv uv run alembic upgrade head
```

`upgrade head` applies all pending revisions.

When you change the schema, from that app’s directory:

```bash
UV_PROJECT_ENVIRONMENT=.venv uv run alembic revision --autogenerate -m "describe_change"
UV_PROJECT_ENVIRONMENT=.venv uv run alembic upgrade head
```

## Running locally

Use the same style for both apps: run from the app folder (`main:app` / `ioc.celery_app`).

**Producer** (API + Celery worker/beat for outbox drain):

```bash
cd independent_producer
UV_PROJECT_ENVIRONMENT=.venv uv run uvicorn main:app --host 0.0.0.0 --port 8001
# other terminal:
UV_PROJECT_ENVIRONMENT=.venv uv run celery -A ioc.celery_app worker -B -l info
```

**Consumer** (API + Celery worker/beat for inbox drain):

```bash
cd independent_consumer
UV_PROJECT_ENVIRONMENT=.venv uv run uvicorn main:app --host 0.0.0.0 --port 8002
# other terminal:
UV_PROJECT_ENVIRONMENT=.venv uv run celery -A ioc.celery_app worker -B -l info
```

Health: `GET http://localhost:8001/health`, `GET http://localhost:8002/health`.
