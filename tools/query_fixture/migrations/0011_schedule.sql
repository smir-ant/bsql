-- A table whose columns exercise the compile-checked `query!` path's dep-free
-- temporal family: a `date` column decodes to `bsql::Date`, a `time` to
-- `bsql::Time`, an `interval` to `bsql::Interval`, a nullable one to its
-- `Option<..>`, and the array forms to `Vec<Option<..>>`. A SELECT over these
-- columns was a `compile_error!` (a loud `UnsupportedPgType`) before temporal
-- support landed. A `date` off-by-one is a wrong calendar day, so these column
-- types are correctness-critical.
CREATE TABLE schedule (
    id        INTEGER PRIMARY KEY,
    day       DATE NOT NULL,
    at        TIME NOT NULL,
    span      INTERVAL NOT NULL,
    deadline  DATE,
    windows   INTERVAL[] NOT NULL
);
