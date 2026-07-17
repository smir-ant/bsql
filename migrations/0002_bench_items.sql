-- Schema for the DEEP-benchmark by-PK read (concurrency throughput).
--
-- Replayed into the build-time `query!` catalog by `build.rs` so the concurrency
-- client's compile-checked `bsql::query!(ByPk, "SELECT id, name, val FROM
-- bench_items WHERE id = $1")` types against it — the SAME `bench_items` shape
-- `setup/pg_setup.sql` seeds and `scripts/xlang_measure_deep.sh` recreates in the
-- ephemeral server it stands up. This is catalog-only (the runner never APPLIES
-- these files); the ephemeral server is seeded directly by the script.
CREATE TABLE bench_items (
    id   int4 PRIMARY KEY,
    name text NOT NULL,
    val  int4 NOT NULL
);
