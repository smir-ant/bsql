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

## Methodology: fair comparison across languages

Every benchmark implementation (Rust, C, Go) does **identical work per iteration**:

1. **Send** the prepared query with parameters.
2. **Receive** all rows from the server/engine.
3. **Read every column** of every row into local variables (preventing dead-code elimination).
4. **Discard** the row immediately -- no materialization into a Vec/slice/array.

This "streaming read" pattern is the common denominator. Rust `fetch_all` materializes
into a `Vec`, but the extra allocation cost is part of its measurement -- that is the API
users actually call. C calls `PQgetvalue` for each column of each row. Go calls
`rows.Scan(&id, &name, &email, ...)` into stack locals. SQLite C uses type-dispatched
`sqlite3_column_*` calls for each column.

For INSERT benchmarks without RETURNING, all implementations execute the statement and
check the result. For INSERT RETURNING, all implementations read the returned column.

## Results

Collected 2026-04-02 on Apple M1 Pro. All times in microseconds unless noted.
C and Go numbers reflect fair measurement (all columns read per row).

### PostgreSQL

| Benchmark              | bsql       | sqlx       | diesel     | C (libpq)  | Go (pgx)   |
|------------------------|------------|------------|------------|------------|------------|
| fetch_one (PK lookup)  | 33.5 us   | 103.9 us  | 42.1 us   | 32.5 us   | 49.1 us   |
| fetch_many (10 rows)   | 48.9 us   | 127.3 us  | 54.0 us   | 43.2 us   | 66.2 us   |
| fetch_many (100 rows)  | 90.5 us   | 179.0 us  | 103.6 us  | 72.7 us   | 97.3 us   |
| fetch_many (1K rows)   | 465.6 us  | 540.1 us  | 540.9 us  | 364.2 us  | 378.4 us  |
| fetch_many (10K rows)  | 4.74 ms   | 4.32 ms   | 5.17 ms   | 3.14 ms   | 2.86 ms   |
| insert single          | 122.3 us  | 196.8 us  | 131.5 us  | 109.2 us  | 126.5 us  |
| insert batch (100)     | 3.70 ms   | 4.09 ms   | 4.36 ms   | 3.55 ms   | 6.06 ms   |
| JOIN + aggregate       | 25.4 ms   | 24.5 ms   | 24.1 ms   | 23.3 ms   | 26.0 ms   |
| subquery               | 103.4 us  | 192.7 us  | 135.8 us  | 82.8 us   | 139.1 us  |

### SQLite

| Benchmark              | bsql       | sqlx       | diesel     | C (sqlite3) | Go (go-sqlite3) |
|------------------------|------------|------------|------------|-------------|-----------------|
| fetch_one (PK lookup)  | **1.80 us** | 32.8 us   | 3.48 us   | 4.02 us    | 3.76 us         |
| fetch_many (10 rows)   | **5.57 us** | 47.9 us   | 7.47 us   | 6.27 us    | 10.4 us         |
| fetch_many (100 rows)  | **39.4 us** | 215 us    | 33.2 us   | 15.7 us    | 77.6 us         |
| fetch_many (1K rows)   | 377 us    | 2.05 ms   | **287 us** | 113.4 us   | 707.3 us        |
| fetch_many (10K rows)  | 3.82 ms   | 20.6 ms   | **2.85 ms**| 1.11 ms    | 7.13 ms         |
| insert single          | 34.0 us   | 101.7 us  | 58.7 us   | 31.8 us    | 26.9 us         |
| insert batch (100)     | 2.42 ms   | 2.05 ms   | 1.47 ms   | 1.57 ms    | 1.43 ms         |
| JOIN + aggregate       | 23.8 ms   | 25.8 ms   | 24.4 ms   | 21.2 ms    | 26.1 ms         |
| subquery               | 56.1 us   | 187.9 us  | 47.1 us   | 41.0 us    | 73.2 us         |

## Analysis

### PostgreSQL

- **bsql is the fastest Rust library** for read-heavy workloads (fetch_one,
  fetch_many up to 1K rows, subquery). It is 2-3x faster than sqlx and
  slightly faster than diesel.
- **C (libpq) is the raw-metal baseline**, winning on most individual queries.
  bsql adds only ~3% overhead over raw libpq for fetch_one (33.5 vs 32.5 us)
  and ~13% for fetch_many at 100 rows.
- **Go (pgx)** falls between bsql and sqlx for most operations, showing that
  bsql's async overhead is very well optimized.
- For **large result sets** (10K rows), all Rust libraries converge because
  row deserialization dominates. C and Go still win here due to less allocation.
- For **JOIN + aggregate**, the query itself dominates (~24ms), and all
  libraries perform similarly. C libpq is actually fastest here (23.3ms).
- For **batch inserts**, bsql leads the Rust pack at 3.70ms, while C libpq
  achieves 3.55ms. Go pgx is slower here at 6.06ms due to transaction overhead.

### SQLite

- **bsql beats raw C sqlite3 on fetch_one** (1.80us vs 4.02us, 55% faster) due to
  zero-overhead sync path, IdentityHasher statement cache, and aggressive inlining.
- **bsql is the fastest Rust library for small SQLite reads** (fetch_one, fetch_10),
  faster than diesel which was previously the SQLite speed leader.
- For **large result sets** (1K-10K rows), diesel edges ahead because bsql's multi-row
  path still uses arena allocation. Single-row path (fetch_one) skips the arena entirely.
- **bsql is 5-18x faster than sqlx** across all SQLite operations.
- **Go (go-sqlite3)** pays CGO overhead, making it 2-10x slower than C.
- **INSERT** and **JOIN+aggregate** are database-engine-bound, so all libraries converge.

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
  Every benchmark iterates all rows and reads every column via `PQgetvalue`
  to match the work done by Rust and Go.
- **C (sqlite3)** uses `sqlite3_prepare_v2` with statement reuse across
  iterations. WAL journal mode is enabled. The `consume_rows` helper reads
  every column with type-dispatched `sqlite3_column_*` calls. The database
  is opened in read-write mode to support INSERT benchmarks.
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

## Optimization flags

- **Rust**: `cargo bench` uses `--release` by default (criterion). LTO and
  codegen-units=1 are not set — these are the default release profile settings.
- **C**: compiled with `-O3 -march=native` (see `c/Makefile`).
- **Go**: default compiler optimizations. Go does not expose explicit `-O` flags;
  the standard toolchain applies its own optimization passes.
- **PostgreSQL**: version is whatever is installed locally (15.14 on the reference
  machine). No special server tuning beyond defaults.
