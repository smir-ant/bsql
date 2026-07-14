-- A dedicated table for the TYPED result-schema OID guard live tests
-- (`tests/query_oid_guard_live.rs`). It is NOT referenced by any other test's
-- runtime DDL: each guard test creates a PER-CONNECTION `CREATE TEMP TABLE
-- oidguard (...)` shadowing this migration table, so the tests are parallel-safe
-- (a TEMP table is session-local — only the test's own connection sees it).
--
-- The `varchar` / `bpchar` columns are deliberate: they exercise the guard's
-- text-family compatibility class. The macro types `text` / `varchar` / `bpchar`
-- all to Rust `String`/`&str` (row-OID marker = `text`, 25), but PostgreSQL
-- reports the DISTINCT native OIDs (`text` 25, `varchar` 1043, `bpchar` 1042) in
-- a RowDescription. All three share ONE wire decode (raw UTF-8), so a `varchar` /
-- `bpchar` runtime column must NOT be falsely rejected against the `text` marker
-- — the guard canonicalizes them to one class. The `n int4` column is a native
-- scalar baseline.
CREATE TABLE oidguard (
    tag TEXT    NOT NULL,
    vc  VARCHAR NOT NULL,
    bp  BPCHAR  NOT NULL,
    n   INT4    NOT NULL
);
