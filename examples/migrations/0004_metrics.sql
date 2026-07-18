-- `metrics` — the bulk-load target for the `typed_copy` example
-- (`copy!` + `copy_in_typed`). Its column set matches the `copy!(…, (id, label,
-- note, amount))` carrier: two NOT NULL columns and two nullable ones.
CREATE TABLE metrics (
    id     BIGINT PRIMARY KEY,
    label  TEXT NOT NULL,
    -- Nullable -> the COPY row tuple field is `Option<&str>`.
    note   TEXT,
    -- Nullable -> `Option<i32>`.
    amount INTEGER
);
