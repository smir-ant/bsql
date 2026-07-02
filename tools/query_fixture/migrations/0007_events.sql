-- A table whose columns exercise the dep-free bsql-native types the
-- compile-checked `query!` path now decodes: `uuid` (a real primary key),
-- `timestamptz` / `timestamp` audit columns. A SELECT over these columns
-- types to `bsql::Uuid` / `bsql::Timestamptz` / `bsql::Timestamp` — a schema
-- shape that was a `compile_error!` before the type widening.
CREATE TABLE events (
    id           UUID PRIMARY KEY,
    occurred_at  TIMESTAMPTZ NOT NULL,
    recorded_at  TIMESTAMP,
    prev_id      UUID,
    payload      JSONB NOT NULL,
    meta         JSON
);
