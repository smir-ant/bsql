-- A table whose columns exercise the compile-checked `query!` path's 1-D
-- array support: `int4[]` / `text[]` / `uuid[]` decode to `Vec<Option<T>>`
-- (an array element may always be NULL), and a NULLABLE array column decodes
-- to `Option<Vec<Option<T>>>`. A SELECT over these columns was a
-- `compile_error!` (a loud `UnsupportedPgType`) before array support landed.
CREATE TABLE array_rows (
    id      INTEGER PRIMARY KEY,
    ints    INT4[] NOT NULL,
    labels  TEXT[] NOT NULL,
    ids     UUID[] NOT NULL,
    tags    TEXT[]
);
