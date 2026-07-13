-- Schema catalog for the compile-checked `query!` PARITY carriers.
--
-- This MIRRORS the original bench harness's `bench_users` / `bench_orders`
-- schema (see the original `setup/pg_setup.sql`). The rebuild's `query!`
-- validates SQL against THIS file at build time (not a live DB), so the column
-- names, types, and NOT-NULL flags must match the live table the runner reads.
-- `SERIAL` is written as its underlying `int4` (the `query!` catalog cares about
-- the column type + nullability, not the sequence default).

CREATE TABLE bench_users (
    id         int4 PRIMARY KEY,
    name       text NOT NULL,
    email      text NOT NULL,
    active     boolean NOT NULL DEFAULT true,
    score      double precision NOT NULL DEFAULT 0.0,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE bench_orders (
    id         int4 PRIMARY KEY,
    user_id    int4 NOT NULL REFERENCES bench_users(id),
    amount     double precision NOT NULL,
    status     text NOT NULL DEFAULT 'pending',
    created_at timestamptz NOT NULL DEFAULT now()
);
