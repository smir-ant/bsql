-- One-shot SQLite seed for the CROSS-LANGUAGE SQLite benchmark.
--
-- Run ONCE before measuring, producing a single file every client shares
-- (bsql's own `parity_sqlite` runner AND the four competitor clients under
-- `clients/{c-sqlite,go-sqlite,rust-sqlite,diesel-sqlite}`), so every client
-- reads BYTE-IDENTICAL data:
--
--   rm -f bench.db bench.db-wal bench.db-shm
--   sqlite3 bench.db < setup/sqlite_setup.sql
--
-- Then point every client at it via `BENCH_SQLITE_PATH=bench.db`.
--
-- This is the ORIGINAL bsql benchmark's `bench_users` / `bench_orders` schema
-- (restored from `bench/setup/sqlite_setup.sql`), the exact schema
-- `src/bin/parity_sqlite.rs` reads — so the competitor numbers are directly
-- diffable against bsql's own. It MIRRORS `setup/pg_setup.sql`'s historical
-- bench_users/bench_orders seed row-for-row (10k users, 100k orders, same
-- deterministic name/email/active pattern; amounts/scores use `random()`, but
-- every client hits the SAME seeded file, so the data is identical across
-- clients regardless).

DROP TABLE IF EXISTS bench_orders;
DROP TABLE IF EXISTS bench_users;

CREATE TABLE bench_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    score REAL NOT NULL DEFAULT 0.0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE bench_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES bench_users(id),
    amount REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Seed 10,000 users via recursive CTE (SQLite lacks generate_series).
-- name/email are DETERMINISTIC ('user_<i>'); active = (i % 5 != 0) exactly
-- mirrors the PG seed's `(i % 5 != 0)`.
WITH RECURSIVE cnt(i) AS (
    SELECT 1
    UNION ALL
    SELECT i + 1 FROM cnt WHERE i < 10000
)
INSERT INTO bench_users (name, email, active, score)
SELECT
    'user_' || i,
    'user_' || i || '@example.com',
    CASE WHEN i % 5 != 0 THEN 1 ELSE 0 END,
    abs(random() % 10000) / 100.0
FROM cnt;

-- Seed 100,000 orders (10 per user), user_id = (i % 10000) + 1.
WITH RECURSIVE cnt(i) AS (
    SELECT 1
    UNION ALL
    SELECT i + 1 FROM cnt WHERE i < 100000
)
INSERT INTO bench_orders (user_id, amount, status)
SELECT
    (i % 10000) + 1,
    abs(random() % 100000) / 100.0,
    CASE (i % 4)
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'completed'
        WHEN 2 THEN 'cancelled'
        ELSE 'refunded'
    END
FROM cnt;

-- Indexes the benchmark queries lean on (email lookup, order join/filter).
CREATE INDEX idx_bench_users_email ON bench_users(email);
CREATE INDEX idx_bench_orders_user_id ON bench_orders(user_id);
CREATE INDEX idx_bench_orders_status ON bench_orders(status);

ANALYZE;
