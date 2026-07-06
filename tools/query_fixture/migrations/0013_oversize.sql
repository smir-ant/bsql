-- A table for the INBOUND oversize-typed-row witness. A result row whose TEXT
-- payload (or the sum of its columns) exceeds the engine's inline read buffer
-- (READ_BUF_CAP = 4096) is streamed from the server as `RowChunk` pieces, which
-- the flagship typed `query!` path (Rows / query_one / query_each) now
-- REASSEMBLES and decodes exactly like an inline row — where it previously
-- errored with a hard `DriverError::OversizeRow` cap. `k` pins a deterministic
-- `ORDER BY` so an oversize row can be FOLLOWED by a small one (proving the
-- reassembly buffer resets) and multiple oversize rows can be ordered.
CREATE TABLE ov_rows (
    k     INTEGER NOT NULL,
    body  TEXT NOT NULL
);
