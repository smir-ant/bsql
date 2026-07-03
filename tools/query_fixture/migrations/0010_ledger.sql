-- A table whose columns exercise the compile-checked `query!` path's exact
-- `numeric` / `decimal` support: a `numeric` column decodes to the dep-free,
-- arbitrary-precision `bsql::Numeric`, a nullable one to `Option<bsql::Numeric>`,
-- and a `numeric[]` column to `Vec<Option<bsql::Numeric>>`. A SELECT over these
-- columns was a `compile_error!` (a loud `UnsupportedPgType`) before numeric
-- support landed. `numeric` is money-shaped: a decode bug is silently-wrong
-- money, so this column type is precision-critical.
CREATE TABLE ledger (
    id       INTEGER PRIMARY KEY,
    amount   NUMERIC NOT NULL,
    fee      DECIMAL(12, 4),
    tranche  NUMERIC[] NOT NULL
);
