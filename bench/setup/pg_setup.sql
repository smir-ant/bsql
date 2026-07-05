-- One-shot setup for the bsql benchmark harness.
--
-- Run ONCE before measuring (the harness scripts do this for you):
--   psql -h 127.0.0.1 -U smir-ant -d postgres -f bench/setup/pg_setup.sql
--
-- Seeds a deterministic dataset and disables autovacuum on the bench tables so
-- a background vacuum cannot fire mid-measurement and skew a sample. A final
-- CHECKPOINT flushes dirty buffers so the first measured writes do not pay for a
-- checkpoint that a prior run dirtied. Kept out of the measured processes on
-- purpose: seeding 10k rows is a transient allocation spike that would inflate a
-- peak-RSS reading if it ran inside the measured binary.

-- Primary lookup table: 10k rows, three columns (int4 PK, text, int4).
DROP TABLE IF EXISTS bench_items;
CREATE TABLE bench_items (
    id   int4 PRIMARY KEY,
    name text NOT NULL,
    val  int4 NOT NULL
);
ALTER TABLE bench_items SET (autovacuum_enabled = false);
INSERT INTO bench_items
SELECT g, 'name_' || g, g * 2
FROM generate_series(1, 10000) AS g;

-- Category table for the JOIN + aggregation scenario. One row per distinct
-- `val` in bench_items (val = id*2, so 2..20000 even).
DROP TABLE IF EXISTS bench_cat;
CREATE TABLE bench_cat (
    val   int4 PRIMARY KEY,
    label text NOT NULL
);
ALTER TABLE bench_cat SET (autovacuum_enabled = false);
INSERT INTO bench_cat
SELECT DISTINCT val, 'cat_' || (val % 50)
FROM bench_items;

-- Unlogged sink for INSERT scenarios: unlogged removes WAL as a noise source
-- (fair — every client hits the same unlogged table), and TRUNCATE resets it so
-- repeated runs start from an empty table.
DROP TABLE IF EXISTS bench_ins;
CREATE UNLOGGED TABLE bench_ins (
    id   int8 PRIMARY KEY,
    name text NOT NULL,
    val  int4 NOT NULL
);
ALTER TABLE bench_ins SET (autovacuum_enabled = false);

ANALYZE bench_items;
ANALYZE bench_cat;
CHECKPOINT;
