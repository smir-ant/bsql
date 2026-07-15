-- A table for the WINDOWED heterogeneous-`pipeline` live witnesses. The
-- deadlock-repro batch pairs an EARLY command that returns a LARGE result with
-- LATER commands that carry LARGE `text` params, so the whole batch spans many
-- 64 KiB send windows — the exact shape that DEADLOCKS a stage-all-then-flush
-- pipeline (the client blocks writing the tail while the server blocks writing
-- the early result) and STREAMS deadlock-free through the windowed drive. A
-- dedicated table (not `ov_rows`, whose oversize tests run in parallel) so the
-- bulk INSERT/DELETE + whole-range counts never interfere.
CREATE TABLE pl_bulk (
    id      BIGINT PRIMARY KEY,
    payload TEXT NOT NULL
);
