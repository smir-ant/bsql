-- A scratch table created and dropped within one migration, purely to prove
-- the destructive-migration acknowledgement gate END-TO-END through a real
-- consumer's build.rs: `bsql-build` replays this crate's migrations/, and the
-- DROP TABLE below would FAIL this crate's build without the
-- `-- bsql:ack-destructive` acknowledgement that directly precedes it.
--
-- The net effect on the catalog is nil (the table is created, then dropped),
-- so no `query!` references it and no other fixture test is affected — this
-- file exercises only the acknowledged-destructive build path.
CREATE TABLE scratch_legacy (
    id   BIGINT PRIMARY KEY,
    note TEXT
);

-- bsql:ack-destructive dropped in the same migration that created it, to
-- exercise the acknowledged-destructive build path (no data ever exists)
DROP TABLE scratch_legacy;
