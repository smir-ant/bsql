-- SQL VIEWS modeled as compile-checked relations (the view feature).
--
-- `bsql-build` INFERS each view's SELECT body against the catalog built so far
-- and registers the view like any relation, so a `query!` SELECTing from it
-- types its columns through the SAME path a base table uses — with NO new
-- consumer API. The live view tests (`tests/view_live_{async,sync}.rs`) shadow
-- these with session-local TEMP tables + TEMP views, so they are parallel-safe
-- (a TEMP object is visible only to the test's own connection), exactly as the
-- OID-guard tests shadow `0022`'s peer `0020_oidguard.sql`.

CREATE TABLE vaccount (
    id      BIGINT PRIMARY KEY,
    balance INTEGER NOT NULL,
    label   TEXT
);

CREATE TABLE vprofile (
    account_id BIGINT PRIMARY KEY,
    nickname   TEXT NOT NULL
);

-- A simple projection view: exposes id + balance ONLY (not label). A `query!`
-- naming `label` on this view is a loud UnknownColumn (the drift guarantee — a
-- column the view does not project does not resolve, even though the base table
-- has it), pinned by the `query_view_dropped_column` compile-fail golden.
CREATE VIEW vaccount_summary AS
    SELECT id, balance FROM vaccount;

-- A LEFT JOIN view: the right side's `NOT NULL` `nickname` becomes NULLABLE
-- through the view (an account with no matching profile row yields a NULL),
-- decoded into `Option<String>`. This is the load-bearing nullability-fidelity
-- proof: an under-nullify here would hand a `query!` a `T` where a real NULL
-- gives `UnexpectedNull`.
CREATE VIEW vaccount_profile AS
    SELECT a.id AS id, a.balance AS balance, p.nickname AS nickname
    FROM vaccount a
    LEFT JOIN vprofile p ON p.account_id = a.id;

-- A view OVER a view (replay is ordered, so `vaccount_summary` is already in
-- the catalog when this body is inferred).
CREATE VIEW vaccount_ids AS
    SELECT id FROM vaccount_summary;

-- A MATERIALIZED view models its column shape identically to a plain view.
CREATE MATERIALIZED VIEW vaccount_mat AS
    SELECT id, balance FROM vaccount;
