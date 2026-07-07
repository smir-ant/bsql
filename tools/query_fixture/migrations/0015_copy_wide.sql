-- A deliberately WIDE table (20 columns) for the raised-arity witness: a
-- `copy!` carrier over all 20 columns exercises the `ParamsWriter` tuple impls
-- ABOVE the former 16-cap (proving typed binary COPY is no longer capped at 16
-- columns — the exact use case bulk-load exists for).
CREATE TABLE copy_wide (
    c01 INTEGER NOT NULL, c02 INTEGER NOT NULL, c03 INTEGER NOT NULL, c04 INTEGER NOT NULL,
    c05 INTEGER NOT NULL, c06 INTEGER NOT NULL, c07 INTEGER NOT NULL, c08 INTEGER NOT NULL,
    c09 INTEGER NOT NULL, c10 INTEGER NOT NULL, c11 INTEGER NOT NULL, c12 INTEGER NOT NULL,
    c13 INTEGER NOT NULL, c14 INTEGER NOT NULL, c15 INTEGER NOT NULL, c16 INTEGER NOT NULL,
    c17 INTEGER NOT NULL, c18 INTEGER NOT NULL, c19 INTEGER NOT NULL, c20 INTEGER NOT NULL
);
