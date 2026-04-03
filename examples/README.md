# bsql Examples

Complete, runnable programs demonstrating bsql features. Each file is standalone with comments explaining every step.

## PostgreSQL

| Example | What it shows |
|---|---|
| [pg_basic.rs](pg_basic.rs) | CRUD: INSERT, SELECT (fetch / fetch_optional), UPDATE, DELETE |
| [pg_dynamic.rs](pg_dynamic.rs) | Optional WHERE clauses, sort enums, pagination |
| [pg_transactions.rs](pg_transactions.rs) | Transactions, savepoints, rollback, isolation levels |
| [pg_streaming.rs](pg_streaming.rs) | Streaming large result sets row-by-row with fetch_stream |
| [pg_listener.rs](pg_listener.rs) | Real-time LISTEN/NOTIFY for cache invalidation and job queues |

### Setup

```bash
export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb

psql "$BSQL_DATABASE_URL" <<'SQL'
CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, login TEXT NOT NULL, active BOOLEAN NOT NULL DEFAULT true);
CREATE TABLE IF NOT EXISTS tickets (id SERIAL PRIMARY KEY, title TEXT NOT NULL, department_id INT, assignee_id INT, priority INT NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), deleted_at TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS accounts (id SERIAL PRIMARY KEY, name TEXT NOT NULL, balance INT NOT NULL);
CREATE TABLE IF NOT EXISTS audit_log (id SERIAL PRIMARY KEY, account_id INT NOT NULL, delta INT NOT NULL, note TEXT);
CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY, kind TEXT NOT NULL, payload TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
SQL
```

### Run

```bash
cd examples/
BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --bin pg_basic
BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --bin pg_dynamic
BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --bin pg_transactions
BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --bin pg_streaming
BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --bin pg_listener
```

## SQLite

| Example | What it shows |
|---|---|
| [sqlite_basic.rs](sqlite_basic.rs) | CRUD with SQLite: fetch, fetch_optional, run (same `query!` macro as PostgreSQL) |
| [sqlite_dynamic.rs](sqlite_dynamic.rs) | Optional WHERE clauses and sort enums with SQLite |

### Setup

```bash
sqlite3 myapp.db <<'SQL'
CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, login TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS tickets (id INTEGER PRIMARY KEY, title TEXT NOT NULL, department_id INTEGER, assignee_id INTEGER, priority INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT (datetime('now')), deleted_at TEXT);
SQL
```

### Run

```bash
cd examples/
BSQL_DATABASE_URL=sqlite:./myapp.db cargo run --bin sqlite_basic
BSQL_DATABASE_URL=sqlite:./myapp.db cargo run --bin sqlite_dynamic
```

## Note

`BSQL_DATABASE_URL` is used at both compile time (by the `query!` macro) and runtime. The database must exist and have the expected schema before you run `cargo build`.
