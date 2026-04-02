-- Benchmark schema for PostgreSQL
-- Usage: psql $BENCH_DATABASE_URL -f setup/pg_setup.sql

DROP TABLE IF EXISTS bench_orders;
DROP TABLE IF EXISTS bench_users;

CREATE TABLE bench_users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE bench_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES bench_users(id),
    amount DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed 10,000 users
INSERT INTO bench_users (name, email, active, score)
SELECT
    'user_' || i,
    'user_' || i || '@example.com',
    (i % 5 != 0),
    random() * 100
FROM generate_series(1, 10000) AS i;

-- Seed 100,000 orders (10 per user)
INSERT INTO bench_orders (user_id, amount, status)
SELECT
    (i % 10000) + 1,
    random() * 1000,
    CASE (i % 4)
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'completed'
        WHEN 2 THEN 'cancelled'
        ELSE 'refunded'
    END
FROM generate_series(1, 100000) AS i;

-- Create indexes for benchmark queries
CREATE INDEX idx_bench_users_email ON bench_users(email);
CREATE INDEX idx_bench_orders_user_id ON bench_orders(user_id);
CREATE INDEX idx_bench_orders_status ON bench_orders(status);
