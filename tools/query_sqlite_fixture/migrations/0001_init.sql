-- Initial schema, written in the SQLite-portable subset (no `ALTER COLUMN`
-- forms SQLite cannot execute): every column's final {type, nullability}
-- is declared in its `CREATE TABLE`. The PostgreSQL catalog replay and the
-- SQLite template replay both consume this verbatim.
CREATE TABLE users (
    id    BIGINT PRIMARY KEY,
    email TEXT NOT NULL,
    name  TEXT
);

CREATE TABLE orders (
    id      BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    total   INTEGER
);
