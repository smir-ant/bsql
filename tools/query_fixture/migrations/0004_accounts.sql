-- A table that a later migration renames, proving RENAME TO re-keys the
-- catalog: the old name stops resolving, the new name starts resolving.
CREATE TABLE legacy_accounts (
    id      BIGINT PRIMARY KEY,
    balance BIGINT NOT NULL
);
