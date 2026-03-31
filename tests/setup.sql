-- Test schema for sasql integration tests.
-- Run against sasql_test database before running tests.

DROP TABLE IF EXISTS ticket_events CASCADE;
DROP TABLE IF EXISTS tickets CASCADE;
DROP TABLE IF EXISTS users CASCADE;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    login TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    middle_name TEXT,                -- nullable
    email TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    score SMALLINT NOT NULL DEFAULT 0,
    rating REAL NOT NULL DEFAULT 0.0,
    balance DOUBLE PRECISION NOT NULL DEFAULT 0.0
);

CREATE TABLE tickets (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,               -- nullable
    status TEXT NOT NULL DEFAULT 'new',
    department_id INTEGER,          -- nullable
    assignee_id INTEGER,            -- nullable
    created_by_user_id INTEGER NOT NULL REFERENCES users(id),
    deleted_at TIMESTAMP WITH TIME ZONE  -- nullable
);

-- Seed data
INSERT INTO users (login, first_name, last_name, email, score, rating, balance) VALUES
    ('alice', 'Alice', 'Smith', 'alice@example.com', 42, 4.5, 100.50),
    ('bob', 'Bob', 'Jones', 'bob@example.com', 7, 3.2, 0.0);

INSERT INTO tickets (title, status, created_by_user_id) VALUES
    ('Fix login bug', 'new', 1),
    ('Add search feature', 'in_progress', 2);
