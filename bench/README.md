# bsql Benchmarks

Comparative benchmarks: **bsql** vs **sqlx** vs **diesel** on PostgreSQL and SQLite.

All three libraries execute the same SQL text via the same database. bsql uses
`query!` (compile-time validated), sqlx uses `query_as` (runtime), and diesel
uses `sql_query` with `QueryableByName` (runtime). This is an apples-to-apples
comparison of runtime query execution overhead.

## Machine specs

| Field         | Value                |
|---------------|----------------------|
| CPU           | _fill in_            |
| RAM           | _fill in_            |
| OS            | _fill in_            |
| Rust          | _fill in_            |
| PostgreSQL    | _fill in_            |
| SQLite        | _fill in_ (bundled)  |

## Prerequisites

- A running PostgreSQL instance with a dedicated benchmark database
- Rust toolchain (stable)
- `BSQL_DATABASE_URL` set at compile time (bsql requires it for `query!` validation)

## Setup

### PostgreSQL

```bash
# Create the benchmark database (if needed)
createdb bench_db

# Set the URL (used at both compile time and runtime)
export BENCH_DATABASE_URL=postgres://user:pass@localhost/bench_db
export BSQL_DATABASE_URL=$BENCH_DATABASE_URL

# Seed tables and indexes
psql "$BENCH_DATABASE_URL" -f setup/pg_setup.sql
```

### SQLite

```bash
# Seed the SQLite database
rm -f bench.db
sqlite3 bench.db < setup/sqlite_setup.sql

# Set paths
export BENCH_SQLITE_PATH=bench.db
export BSQL_DATABASE_URL=sqlite://bench.db
```

## Running benchmarks

### PostgreSQL

```bash
# Make sure BSQL_DATABASE_URL and BENCH_DATABASE_URL are set (see Setup above)

cargo bench --bench pg_fetch_one
cargo bench --bench pg_fetch_many
cargo bench --bench pg_insert
cargo bench --bench pg_complex
```

### SQLite

```bash
# Make sure BSQL_DATABASE_URL=sqlite://bench.db and BENCH_SQLITE_PATH=bench.db

cargo bench --bench sqlite_fetch_one
cargo bench --bench sqlite_fetch_many
cargo bench --bench sqlite_insert
cargo bench --bench sqlite_complex
```

### Run all at once

```bash
cargo bench
```

## Results

### PostgreSQL

| Benchmark              | bsql       | sqlx       | diesel     |
|------------------------|------------|------------|------------|
| fetch_one (PK lookup)  | _fill in_  | _fill in_  | _fill in_  |
| fetch_many (10 rows)   | _fill in_  | _fill in_  | _fill in_  |
| fetch_many (100 rows)  | _fill in_  | _fill in_  | _fill in_  |
| fetch_many (1K rows)   | _fill in_  | _fill in_  | _fill in_  |
| fetch_many (10K rows)  | _fill in_  | _fill in_  | _fill in_  |
| insert single          | _fill in_  | _fill in_  | _fill in_  |
| insert batch (100)     | _fill in_  | _fill in_  | _fill in_  |
| JOIN + aggregate       | _fill in_  | _fill in_  | _fill in_  |
| subquery               | _fill in_  | _fill in_  | _fill in_  |

### SQLite

| Benchmark              | bsql       | sqlx       | diesel     |
|------------------------|------------|------------|------------|
| fetch_one (PK lookup)  | _fill in_  | _fill in_  | _fill in_  |
| fetch_many (10 rows)   | _fill in_  | _fill in_  | _fill in_  |
| fetch_many (100 rows)  | _fill in_  | _fill in_  | _fill in_  |
| fetch_many (1K rows)   | _fill in_  | _fill in_  | _fill in_  |
| fetch_many (10K rows)  | _fill in_  | _fill in_  | _fill in_  |
| insert single          | _fill in_  | _fill in_  | _fill in_  |
| insert batch (100)     | _fill in_  | _fill in_  | _fill in_  |
| JOIN + aggregate       | _fill in_  | _fill in_  | _fill in_  |
| subquery               | _fill in_  | _fill in_  | _fill in_  |

## Notes

- **bsql** validates all SQL at compile time. There is zero runtime SQL parsing.
  The benchmark measures pure execution + deserialization overhead.
- **sqlx** `query_as` is used (not `query_as!`) to avoid requiring a compile-time
  database for the sqlx side. This is the common runtime usage pattern.
- **diesel** uses `sql_query` with raw SQL for an apples-to-apples comparison.
  This avoids diesel's DSL overhead and measures the same SQL as bsql and sqlx.
- **diesel is sync**. Its benchmarks run without `to_async()`. This is the fairest
  comparison since diesel is fundamentally synchronous.
- All benchmark functions share the same pool/connection configuration. Default
  pool sizes are used for both bsql and sqlx.
- INSERT benchmarks grow the database over time. Re-run `setup/pg_setup.sql` or
  `setup/sqlite_setup.sql` to reset to a clean state between runs.
- Criterion reports are saved to `target/criterion/`. Open
  `target/criterion/report/index.html` for interactive charts.
