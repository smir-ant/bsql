-- The `users` table — the starting point for the basic CRUD examples.
--
-- Every column here is a SQLite-PORTABLE type (BIGINT / TEXT), so a `query!`
-- typed against this table runs UNCHANGED on both PostgreSQL and SQLite (see
-- the `multi_backend` example). `bsql-build` replays this DDL at build time into
-- the catalog the `query!` macro types against — so `SELECT nope FROM users`
-- would be a COMPILE error, not a runtime one.
CREATE TABLE users (
    -- `BIGINT PRIMARY KEY` -> the record field decodes as `i64`, and PRIMARY KEY
    -- makes it NOT NULL, so the field is a bare `i64` (never `Option`).
    id    BIGINT PRIMARY KEY,
    -- `TEXT NOT NULL` -> `String` (owned) / `&str` (borrowed), never `Option`.
    email TEXT NOT NULL,
    -- Nullable `TEXT` -> `Option<String>` / `Option<&str>`. The `query!` inference
    -- engine tracks nullability per column; a NULL here decodes as `None`.
    name  TEXT
);
