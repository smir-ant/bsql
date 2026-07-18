-- A dedicated table for the NULLABLE-parameter round-trip live tests
-- (`tests/nullable_param_live.rs`). It is NOT referenced by any other test's
-- runtime DDL: each test creates a PER-CONNECTION `CREATE TEMP TABLE np_rows
-- (...)` shadowing this migration table, so the tests are parallel-safe (a TEMP
-- table is session-local — only the test's own connection sees it).
--
-- `id` is NOT NULL (a PRIMARY KEY), so a `$N` bound INTO it is the base `i32`.
-- `note` (text) and `score` (int4) are NULLABLE, so a `$N` bound as a bare value
-- INTO them is `Option<&str>` / `Option<i32>`: `Some(x)` inserts x, `None`
-- inserts SQL NULL — the whole point of the typed nullable-param inference.
CREATE TABLE np_rows (
    id    INT4 PRIMARY KEY,
    note  TEXT,
    score INT4
);
