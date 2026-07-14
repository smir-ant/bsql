-- A table with a DEFERRABLE INITIALLY DEFERRED UNIQUE constraint, used by the
-- pipeline live tests to exercise a COMMIT-TIME failure: two inserts of the same
-- `tag` each SUCCEED at Execute (the uniqueness check is deferred), then the
-- implicit COMMIT at the batch's trailing Sync fails — a failure attributable to
-- no single command. The `query!` carrier (INSERT ... RETURNING id) validates
-- against this table's columns; the deferred constraint is a runtime property the
-- test (re)creates the live table with.
CREATE TABLE pl_deferred (
    id  INTEGER PRIMARY KEY,
    tag INTEGER NOT NULL,
    CONSTRAINT pl_deferred_tag_uniq UNIQUE (tag) DEFERRABLE INITIALLY DEFERRED
);
