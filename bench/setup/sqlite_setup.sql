-- Benchmark schema for SQLite
-- Usage: sqlite3 bench.db < setup/sqlite_setup.sql

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

-- Seed 10,000 users via recursive CTE
-- SQLite lacks generate_series, so we use a recursive CTE
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

-- Seed 100,000 orders (10 per user)
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

-- Create indexes for benchmark queries
CREATE INDEX idx_bench_users_email ON bench_users(email);
CREATE INDEX idx_bench_orders_user_id ON bench_orders(user_id);
CREATE INDEX idx_bench_orders_status ON bench_orders(status);
