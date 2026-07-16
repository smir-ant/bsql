-- Table for the homogeneous typed-RETURNING `query_batch` live witnesses.
-- `qb_rows` is a plain PK table the batch verb bulk-writes AND reads back (the
-- typed RETURNING rows are KEPT, one `Rows<Q>` per command, unlike `execute_batch`
-- which discards them). A dedicated table (not `eb_rows` / `accounts`) so the
-- query_batch tests' per-connection TEMP shadows + whole-table counts never
-- interfere with the parallel execute_batch / pipeline tests. The `label` TEXT
-- column lets a test verify a decoded RETURNING VALUE (not just a count).
CREATE TABLE qb_rows (
    id    BIGINT PRIMARY KEY,
    label TEXT   NOT NULL
);
