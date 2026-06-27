-- A table with a PostgreSQL `oid` column. `oid` is the one v1 leaf type
-- (Rust `u32`) the shared inference lattice supports but SQLite has no
-- equivalent for: a query projecting `thing_id` types on PostgreSQL but is
-- a loud conformance failure on SQLite. SQLite accepts `OID` as a free-form
-- column type name (it carries NUMERIC affinity), so the template replays
-- without error; the divergence surfaces only when a query selects it.
CREATE TABLE widgets (
    id       BIGINT PRIMARY KEY,
    thing_id OID NOT NULL
);
