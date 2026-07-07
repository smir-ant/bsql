-- Minimal SQLite-portable schema (every column's final {type, nullability}
-- is declared in its CREATE TABLE — no ALTER form SQLite cannot replay), so a
-- `query!` over it types on PostgreSQL AND conforms on SQLite, which is what
-- lets one carrier gain BOTH `TypedQuery` and `SqliteTypedQuery`.
CREATE TABLE users (
    id    BIGINT PRIMARY KEY,   -- NOT NULL i64 (the PK)
    email TEXT NOT NULL,        -- NOT NULL &str / String
    name  TEXT                  -- nullable  Option<&str> / Option<String>
);

CREATE TABLE orders (
    id     BIGINT PRIMARY KEY,  -- NOT NULL i64
    ref_no TEXT NOT NULL        -- NOT NULL &str / String
);
