-- `events` — the subject of the `external_bridges` example. Its `id UUID` and
-- `occurred_at TIMESTAMPTZ` columns are decoded, by default, into bsql's own
-- dependency-free `bsql::Uuid` / `bsql::Timestamptz`. The example registers
-- build-time BRIDGES so they decode instead into the REAL `uuid::Uuid` /
-- `chrono::DateTime<Utc>` — with bsql depending on and forcing NOTHING.
--
-- PostgreSQL-ONLY (SQLite has no native uuid / timestamptz storage), so this
-- carrier does not run on SQLite.
CREATE TABLE events (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);
