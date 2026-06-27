-- A deliberately WIDE table: enough sortable columns that a single
-- runtime `ORDER BY { ... }` allow-set can name more distinct orderings
-- than the compile-time budget (`MAX_ORDER_BY_VARIANTS = 16`) allows. The
-- budget golden orders by 17 of these columns, which exceeds 16 and is a
-- const-evaluation failure (`error[E0080]`) at the `query!` site.
CREATE TABLE wide (
    id  BIGINT PRIMARY KEY,
    c1  INTEGER,
    c2  INTEGER,
    c3  INTEGER,
    c4  INTEGER,
    c5  INTEGER,
    c6  INTEGER,
    c7  INTEGER,
    c8  INTEGER,
    c9  INTEGER,
    c10 INTEGER,
    c11 INTEGER,
    c12 INTEGER,
    c13 INTEGER,
    c14 INTEGER,
    c15 INTEGER,
    c16 INTEGER,
    c17 INTEGER
);
