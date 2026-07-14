-- Tables for the homogeneous `execute_batch` live witnesses. `eb_rows` is a
-- plain PK table the batch verb bulk-writes; `eb_uniq` mirrors `pl_deferred`
-- (its DEFERRABLE UNIQUE is a runtime property the commit-time-failure test
-- (re)creates the live table with — the migration only feeds the `query!` carrier's
-- catalog columns). Dedicated tables (not `accounts`) so the batch tests' DROP/
-- CREATE + whole-table counts never interfere with the parallel pipeline tests.
CREATE TABLE eb_rows (
    id      BIGINT PRIMARY KEY,
    balance BIGINT NOT NULL
);

CREATE TABLE eb_uniq (
    id  INTEGER PRIMARY KEY,
    tag INTEGER NOT NULL,
    CONSTRAINT eb_uniq_tag_uniq UNIQUE (tag) DEFERRABLE INITIALLY DEFERRED
);
