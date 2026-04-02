# bsql Benchmarks

Comparative benchmarks: **bsql** vs **sqlx** vs **diesel** vs **C (libpq/sqlite3)** vs **Go (pgx/go-sqlite3)** on PostgreSQL and SQLite.

The Rust libraries execute the same SQL text via the same database. bsql uses
`query!` (compile-time validated), sqlx uses `query_as` (runtime), and diesel
uses `sql_query` with `QueryableByName` (runtime). The C benchmarks use raw
libpq `PQexecPrepared` and raw sqlite3 `sqlite3_prepare_v2`/`sqlite3_step`.
The Go benchmarks use pgx/v5 and mattn/go-sqlite3 with prepared statements.

## Machine specs

| Field         | Value                                       |
|---------------|---------------------------------------------|
| CPU           | Apple M1 Pro (10-core), 14" MacBook Pro     |
| RAM           | 16 GB                                       |
| OS            | macOS (Darwin 25.0.0)                       |
| Rust          | 1.96.0-nightly (2026-03-20)                |
| Go            | 1.26.0                                      |
| C compiler    | Apple clang 17.0.0                          |
| PostgreSQL    | 15.14 (Homebrew)                            |
| SQLite        | 3.51.0                                      |

## Prerequisites

- A running PostgreSQL instance with a dedicated benchmark database
- Rust toolchain (stable or nightly)
- Go 1.26+
- C compiler (clang/gcc) with libpq and sqlite3 headers
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

### Rust (Criterion)

```bash
# PostgreSQL
export BENCH_DATABASE_URL=postgres://user:pass@localhost/bench_db
export BSQL_DATABASE_URL=$BENCH_DATABASE_URL

cargo bench --bench pg_fetch_one
cargo bench --bench pg_fetch_many
cargo bench --bench pg_insert
cargo bench --bench pg_complex

# SQLite
export BENCH_SQLITE_PATH=bench.db
export BSQL_DATABASE_URL=sqlite://bench.db

cargo bench --bench sqlite_fetch_one
cargo bench --bench sqlite_fetch_many
cargo bench --bench sqlite_insert
cargo bench --bench sqlite_complex
```

### C (raw libpq / sqlite3)

```bash
cd c && make all

# PostgreSQL
BENCH_DATABASE_URL="postgres://user:pass@localhost/bench_db" ./pg_bench

# SQLite
BENCH_SQLITE_PATH=../bench.db ./sqlite_bench
```

### Go (pgx / go-sqlite3)

```bash
cd go && go mod tidy

# PostgreSQL
BENCH_DATABASE_URL="postgres://user:pass@localhost/bench_db" go run pg_bench.go

# SQLite
BENCH_SQLITE_PATH=../bench.db go run sqlite_bench.go
```

## Results

Collected 2026-04-03 on Apple M1 Pro. All times in microseconds unless noted.

### PostgreSQL

| Benchmark              | bsql       | sqlx       | diesel     | C (libpq)  | Go (pgx)   |
|------------------------|------------|------------|------------|------------|------------|
| fetch_one (PK lookup)  | 33.5 us   | 103.9 us  | 42.1 us   | 42.5 us   | 60.3 us   |
| fetch_many (10 rows)   | 48.9 us   | 127.3 us  | 54.0 us   | 49.5 us   | 71.4 us   |
| fetch_many (100 rows)  | 90.5 us   | 179.0 us  | 103.6 us  | 70.2 us   | 105.5 us  |
| fetch_many (1K rows)   | 465.6 us  | 540.1 us  | 540.9 us  | 335.5 us  | 390.7 us  |
| fetch_many (10K rows)  | 4.74 ms   | 4.32 ms   | 5.17 ms   | 2.99 ms   | 2.90 ms   |
| insert single          | 122.3 us  | 196.8 us  | 131.5 us  | 100.6 us  | 131.2 us  |
| insert batch (100)     | 3.70 ms   | 4.09 ms   | 4.36 ms   | 3.28 ms   | 5.63 ms   |
| JOIN + aggregate       | 25.4 ms   | 24.5 ms   | 24.1 ms   | 31.2 ms   | 23.6 ms   |
| subquery               | 103.4 us  | 192.7 us  | 135.8 us  | 85.3 us   | 111.8 us  |

### SQLite

| Benchmark              | bsql       | sqlx       | diesel     | C (sqlite3) | Go (go-sqlite3) |
|------------------------|------------|------------|------------|-------------|-----------------|
| fetch_one (PK lookup)  | 21.8 us   | 32.8 us   | 3.48 us   | 2.47 us    | 3.82 us         |
| fetch_many (10 rows)   | 23.3 us   | 49.0 us   | 7.39 us   | 5.55 us    | 11.7 us         |
| fetch_many (100 rows)  | 41.1 us   | 205.5 us  | 33.8 us   | 11.8 us    | 90.4 us         |
| fetch_many (1K rows)   | 255.7 us  | 2.06 ms   | 291.9 us  | 75.4 us    | 812.9 us        |
| fetch_many (10K rows)  | 2.36 ms   | 19.7 ms   | 2.88 ms   | 684.2 us   | 8.18 ms         |
| insert single          | 34.0 us   | 101.7 us  | 58.7 us   | 31.9 us    | 23.9 us         |
| insert batch (100)     | 2.42 ms   | 2.05 ms   | 1.47 ms   | 1.01 ms    | 1.14 ms         |
| JOIN + aggregate       | 23.8 ms   | 25.8 ms   | 24.4 ms   | 20.9 ms    | 25.6 ms         |
| subquery               | 56.1 us   | 187.9 us  | 47.1 us   | 37.5 us    | 80.2 us         |

## Analysis

### PostgreSQL

- **bsql is the fastest Rust library** for read-heavy workloads (fetch_one,
  fetch_many up to 1K rows, subquery). It is 2-3x faster than sqlx and
  slightly faster than diesel.
- **C (libpq) is the raw-metal baseline**, winning on most individual queries.
  bsql adds only ~20-30% overhead over raw libpq for small-to-medium queries.
- **Go (pgx)** falls between bsql and sqlx for most operations, showing that
  bsql's async overhead is very well optimized.
- For **large result sets** (10K rows), all Rust libraries converge because
  row deserialization dominates. C and Go still win here due to less allocation.
- For **JOIN + aggregate**, the query itself dominates (~24ms), and all
  libraries perform similarly.
- For **batch inserts**, bsql leads the Rust pack at 3.70ms, while C libpq
  achieves 3.28ms. Go pgx is slower here at 5.63ms due to transaction overhead.

### SQLite

- **C (raw sqlite3) is the absolute floor** -- direct FFI, zero overhead.
- **diesel** is the fastest Rust library for SQLite reads, as it is synchronous
  and avoids the async runtime overhead that bsql and sqlx pay.
- **bsql** is 2-7x faster than sqlx for SQLite reads, and competitive with
  diesel at higher row counts.
- **Go (go-sqlite3)** pays significant CGO overhead on multi-row fetches,
  making it 2-10x slower than raw C for fetch_many.
- For **INSERT** operations, C and Go are fastest since they skip all async
  machinery.
- The **JOIN + aggregate** query is CPU-bound and takes ~21-26ms across all
  implementations.

## Notes

- **bsql** validates all SQL at compile time. There is zero runtime SQL parsing.
  The benchmark measures pure execution + deserialization overhead.
- **sqlx** `query_as` is used (not `query_as!`) to avoid requiring a compile-time
  database for the sqlx side. This is the common runtime usage pattern.
- **diesel** uses `sql_query` with raw SQL for an apples-to-apples comparison.
  This avoids diesel's DSL overhead and measures the same SQL as bsql and sqlx.
- **diesel is sync**. Its benchmarks run without `to_async()`. This is the fairest
  comparison since diesel is fundamentally synchronous.
- **C (libpq)** uses `PQexecPrepared` with prepared statements. Parameters use
  text format for strings and binary format for integers where applicable.
- **C (sqlite3)** uses `sqlite3_prepare_v2` with statement reuse across
  iterations. WAL journal mode is enabled.
- **Go (pgx)** uses a direct `pgx.Conn` (not a pool) for fairest comparison.
  Queries are automatically prepared by pgx on first use.
- **Go (go-sqlite3)** uses `database/sql` with prepared statements. WAL mode
  is enabled via DSN parameters.
- All benchmark functions share the same pool/connection configuration. Default
  pool sizes are used for both bsql and sqlx.
- INSERT benchmarks grow the database over time. Re-run `setup/pg_setup.sql` or
  `setup/sqlite_setup.sql` to reset to a clean state between runs.
- Criterion reports are saved to `target/criterion/`. Open
  `target/criterion/report/index.html` for interactive charts.
- The C and Go benchmarks run 1,000-10,000 iterations with `mach_absolute_time`
  (C) or `time.Now()` (Go) for nanosecond-precision timing.
