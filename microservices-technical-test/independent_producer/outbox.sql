CREATE TABLE IF NOT EXISTS outbox (
    id VARCHAR(255) PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    aggregate_id VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    correlation_id VARCHAR(255) NOT NULL,
    processed_at TIMESTAMPTZ,
    occurred_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    trace_parent VARCHAR(255) NOT NULL DEFAULT '',
    attempts INT NOT NULL DEFAULT 0
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS outbox_available_idx
ON outbox (available_at, occurred_at)
WHERE processed_at IS NULL;
