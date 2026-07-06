-- Every SQLite storage class in one table, in the SQLite-portable subset, so a
-- `query!` over it round-trips end-to-end through the typed runtime:
--   * INTEGER  — `id` (BIGINT, the PK → i64) and the nullable `count` (i64)
--   * TEXT     — `label` (NOT NULL → &str/String) and the nullable `note`
--   * REAL     — `weight` (DOUBLE PRECISION → f64; SQLite has one 8-byte REAL,
--                so `double precision` agrees on both backends, unlike `real`
--                which is PostgreSQL's 4-byte f32)
--   * BLOB     — `payload` (BYTEA → Vec<u8>/&[u8]; the PostgreSQL spelling gets
--                SQLite NUMERIC affinity but stores a bound BLOB verbatim, and
--                the conformance oracle accepts BYTEA as the lattice `bytea`)
--   * NULL     — the nullable `count` / `note` / `payload` carry a real NULL,
--                decoded as `Option::None`
CREATE TABLE measurements (
    id      BIGINT PRIMARY KEY,
    label   TEXT NOT NULL,
    weight  DOUBLE PRECISION NOT NULL,
    payload BYTEA,
    count   BIGINT,
    note    TEXT
);
