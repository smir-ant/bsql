# Examples

Complete, runnable programs demonstrating bsql. Each file is a self-contained tutorial with comments explaining every step.

## Setup

### PostgreSQL

```bash
export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb

psql "$BSQL_DATABASE_URL" <<'SQL'
CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS tickets (id SERIAL PRIMARY KEY, title TEXT NOT NULL, department_id INT, assignee_id INT, priority INT NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), deleted_at TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS accounts (id SERIAL PRIMARY KEY, name TEXT NOT NULL, balance INT NOT NULL);
CREATE TABLE IF NOT EXISTS audit_log (id SERIAL PRIMARY KEY, account_id INT NOT NULL, delta INT NOT NULL, note TEXT);
CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY, kind TEXT NOT NULL, payload TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
SQL
```

### SQLite

```bash
sqlite3 myapp.db <<'SQL'
CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS tickets (id INTEGER PRIMARY KEY, title TEXT NOT NULL, department_id INTEGER, assignee_id INTEGER, priority INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT (datetime('now')), deleted_at TEXT);
SQL
```

## PostgreSQL

| Example | What it demonstrates |
|---|---|
| [pg_basic.rs](pg_basic.rs) | CRUD operations: `.fetch()`, `.run()`, `.pop()` for single-row lookups |
| [pg_dynamic.rs](pg_dynamic.rs) | Optional WHERE clauses, sort enums, pagination |
| [pg_transactions.rs](pg_transactions.rs) | Transactions with `.defer()` batching, savepoints, isolation levels |
| [pg_streaming.rs](pg_streaming.rs) | Processing large result sets with `fetch_stream` (constant memory) |
| [pg_batch_insert.rs](pg_batch_insert.rs) | Batch INSERT with `.defer()` pipelining (one round-trip for N inserts) |
| [pg_listener.rs](pg_listener.rs) | Real-time LISTEN/NOTIFY with `listener.recv()` |

```bash
cd examples/
cargo run --bin pg_basic
cargo run --bin pg_dynamic
cargo run --bin pg_transactions
cargo run --bin pg_streaming
cargo run --bin pg_batch_insert
cargo run --bin pg_listener
```

## SQLite

| Example | What it demonstrates |
|---|---|
| [sqlite_basic.rs](sqlite_basic.rs) | CRUD operations (same API as PostgreSQL) |
| [sqlite_dynamic.rs](sqlite_dynamic.rs) | Optional WHERE clauses and sort enums |

```bash
cd examples/
BSQL_DATABASE_URL=sqlite:./myapp.db cargo run --bin sqlite_basic
BSQL_DATABASE_URL=sqlite:./myapp.db cargo run --bin sqlite_dynamic
```

## Note

`BSQL_DATABASE_URL` is used at both compile time (by the `query!` macro) and runtime. The database must exist and have the expected schema before you run `cargo build`.
