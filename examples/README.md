# bsql Examples

Practical usage examples for the bsql library. Each file is a standalone program demonstrating a specific feature.

## PostgreSQL

| Example | What it covers |
|---------|---------------|
| [pg_basic.rs](pg_basic.rs) | CRUD operations: INSERT, SELECT (one/optional/all), UPDATE, DELETE |
| [pg_dynamic.rs](pg_dynamic.rs) | Optional WHERE clauses, sort enums, pagination |
| [pg_transactions.rs](pg_transactions.rs) | Transactions, savepoints, rollback, isolation levels |
| [pg_streaming.rs](pg_streaming.rs) | Streaming large result sets row-by-row with constant memory |
| [pg_listener.rs](pg_listener.rs) | Real-time LISTEN/NOTIFY for cache invalidation, job queues |

## SQLite

| Example | What it covers |
|---------|---------------|
| [sqlite_basic.rs](sqlite_basic.rs) | CRUD operations with SQLite (same `query!` macro as PostgreSQL) |
| [sqlite_dynamic.rs](sqlite_dynamic.rs) | Optional clauses and sort enums with SQLite |

## Running

These examples require a database for both runtime and compile-time validation. They will not compile without `BSQL_DATABASE_URL` pointing to a real database with the expected schema.

### PostgreSQL examples

```bash
# Set the database URL (used by the query! macro at compile time AND at runtime)
export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb

# Create the tables the examples expect
psql "$BSQL_DATABASE_URL" <<'SQL'
CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, login TEXT NOT NULL, active BOOLEAN NOT NULL DEFAULT true);
CREATE TABLE IF NOT EXISTS tickets (id SERIAL PRIMARY KEY, title TEXT NOT NULL, department_id INT, assignee_id INT, priority INT NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), deleted_at TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS accounts (id SERIAL PRIMARY KEY, name TEXT NOT NULL, balance INT NOT NULL);
CREATE TABLE IF NOT EXISTS audit_log (id SERIAL PRIMARY KEY, account_id INT NOT NULL, delta INT NOT NULL, note TEXT);
CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY, kind TEXT NOT NULL, payload TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
SQL

# Run an example
cd examples/
cargo run --bin pg_basic
cargo run --bin pg_dynamic
cargo run --bin pg_transactions
cargo run --bin pg_streaming
cargo run --bin pg_listener
```

### SQLite examples

```bash
# Create the database and tables
sqlite3 myapp.db <<'SQL'
CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, login TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS tickets (id INTEGER PRIMARY KEY, title TEXT NOT NULL, department_id INTEGER, assignee_id INTEGER, priority INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT (datetime('now')), deleted_at TEXT);
SQL

export BSQL_DATABASE_URL=sqlite:./myapp.db

cd examples/
cargo run --bin sqlite_basic
cargo run --bin sqlite_dynamic
```

## Note

These examples are documentation. They demonstrate API patterns and are not intended to be run as a test suite. The schema setup above is the minimum needed to make them compile and run.
