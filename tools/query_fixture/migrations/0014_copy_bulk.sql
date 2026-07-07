-- A table shaped for the typed binary COPY (`copy!` + `copy_in_typed`) witness:
-- an integer key, a required text column, a NULLABLE text column, and a NULLABLE
-- integer column. The text columns carry values with embedded tabs / newlines /
-- quotes in the live test — the bytes that CORRUPT a text COPY but ride a binary
-- COPY verbatim — and the nullable columns exercise `Option<T>` / SQL NULL.
CREATE TABLE copy_bulk (
    id     BIGINT  NOT NULL,
    label  TEXT    NOT NULL,
    note   TEXT,
    amount INTEGER
);
