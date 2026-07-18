-- `orders` — used by the CRUD, aggregates, transactions, pipelining/batch, and
-- pooling examples. A row belongs to a user (logical FK -> users.id) and carries
-- a status + an optional integer total (in cents, say).
CREATE TABLE orders (
    id      BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    -- Nullable integer total -> `Option<i32>`.
    total   INTEGER,
    -- NOT NULL with a DEFAULT: the column decodes as a bare `String`. A DEFAULT
    -- lets an INSERT omit the column; the compile-checked catalog still sees it
    -- as NOT NULL.
    status  TEXT NOT NULL DEFAULT 'pending'
);
