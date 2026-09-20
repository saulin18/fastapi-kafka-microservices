CREATE TABLE IF NOT EXISTS inbox_messages (
    id VARCHAR(255) PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    content JSONB NOT NULL,
    received_on_utc TIMESTAMPTZ NOT NULL,
    processed_on_utc TIMESTAMPTZ NULL,
    error TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_inbox_messages_unprocessed
ON inbox_messages (received_on_utc, processed_on_utc)
INCLUDE (id, type, content)
WHERE processed_on_utc IS NULL;
