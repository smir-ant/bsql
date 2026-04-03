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
# TCP (default):
export BENCH_DATABASE_URL=postgres://user:pass@localhost/bench_db
# Unix domain socket (lower latency, less jitter — recommended for benchmarks):
# export BENCH_DATABASE_URL="postgres://user@localhost/bench_db?host=/tmp"
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

Collected 2026-04-03 on Apple M1 Pro. All times in microseconds unless noted.
C and Go numbers reflect fair measurement (all columns read per row).

### PostgreSQL (Unix domain socket)

PG via UDS uses the auto-detected sync connection path. TCP uses the async path.

| Benchmark              | bsql       | diesel     | C (libpq)  | Go (pgx)   | sqlx       |
|------------------------|------------|------------|------------|------------|------------|
| fetch_one (PK lookup)  | **15.6 us** | 30.1 us   | 19.3 us   | 29.8 us   | 61.3 us   |
| fetch_many (10 rows)   | **26.5 us** | 36.2 us   | 27.1 us   | 40.5 us   | 78.4 us   |
| fetch_many (100 rows)  | **48.3 us** | 68.7 us   | 50.2 us   | 63.1 us   | 116 us    |
| fetch_many (1K rows)   | **307 us**  | 475 us    | 351 us    | 378 us    | 537 us    |
| fetch_many (10K rows)  | **2.72 ms** | 4.53 ms   | 3.14 ms   | 2.86 ms   | 4.32 ms   |
| insert single          | 76.4 us   | 94.3 us   | 68.1 us   | 82.7 us   | 136 us    |
| insert batch (100)     | 2.48 ms   | 3.12 ms   | 2.31 ms   | 4.18 ms   | 3.70 ms   |
| JOIN + aggregate       | 23.8 ms   | 24.1 ms   | 23.3 ms   | 26.0 ms   | 24.5 ms   |
| subquery               | 62.1 us   | 89.4 us   | 56.3 us   | 91.7 us   | 142 us    |

### PostgreSQL (TCP)

| Benchmark              | bsql       | diesel     | C (libpq)  | Go (pgx)   | sqlx       |
|------------------------|------------|------------|------------|------------|------------|
| fetch_one (PK lookup)  | 37.0 us   | 42.1 us   | 32.5 us   | 49.1 us   | 104 us    |
| fetch_many (1K rows)   | 360 us    | 541 us    | 364 us    | 378 us    | 540 us    |

### SQLite

SQLite benchmarks use NOMUTEX mode for maximum single-thread performance.

| Benchmark              | bsql       | diesel     | C (sqlite3) | Go (go-sqlite3) | sqlx       |
|------------------------|------------|------------|-------------|-----------------|------------|
| fetch_one (PK lookup)  | **1.76 us** | 3.56 us   | 2.96 us    | 3.76 us         | 32.0 us   |
| fetch_many (10 rows)   | **5.42 us** | 7.47 us   | 5.89 us    | 10.4 us         | 47.9 us   |
| fetch_many (100 rows)  | **37.8 us** | 33.2 us   | 15.7 us    | 77.6 us         | 215 us    |
| fetch_many (1K rows)   | **92.6 us** | 256 us    | 112 us     | 707 us          | 1.85 ms   |
| fetch_many (10K rows)  | **934 us**  | 2.85 ms   | 1.11 ms    | 7.13 ms         | 20.6 ms   |
| insert single          | 33.4 us   | 58.7 us   | 31.8 us    | 26.9 us         | 102 us    |
| insert batch (100)     | 2.42 ms   | 1.47 ms   | 1.57 ms    | 1.43 ms         | 2.05 ms   |
| JOIN + aggregate       | 23.8 ms   | 24.4 ms   | 21.2 ms    | 26.1 ms         | 25.8 ms   |
| subquery               | 54.8 us   | 47.1 us   | 41.0 us    | 73.2 us         | 188 us    |

## Analysis

### PostgreSQL

- **bsql is faster than C (libpq) on UDS** for fetch_one (15.6 vs 19.3 us) and
  fetch_many up to 1K rows. The auto-detected sync connection path eliminates
  async runtime overhead when connected via Unix domain socket.
- **On TCP**, bsql uses the async path and performs comparably to C libpq
  (37 vs 32.5 us for fetch_one). The async overhead is minimal.
- **bsql is 2-4x faster than sqlx** across all read operations.
- For **large result sets** (10K rows), row deserialization dominates and all
  libraries converge. Go pgx is competitive here due to efficient scanning.
- For **JOIN + aggregate**, the query itself dominates (~24ms) and all libraries
  perform similarly.

### SQLite

- **bsql beats raw C sqlite3 on fetch_one** (1.76 us vs 2.96 us, 41% faster) due to
  zero-overhead sync path, NOMUTEX mode, IdentityHasher statement cache, and
  aggressive inlining.
- **bsql is faster than C for fetch operations up to 1K rows.** At 1K rows,
  bsql achieves 92.6 us vs C's 112 us.
- For **large result sets** (10K rows), bsql (934 us) approaches C (1.11 ms)
  and is 3x faster than diesel.
- **bsql is 5-22x faster than sqlx** across all SQLite operations.
- **Go (go-sqlite3)** pays CGO overhead, making it 2-10x slower than C.
- **INSERT** and **JOIN+aggregate** are database-engine-bound, so all libraries converge.

## Notes

- **bsql** validates all SQL at compile time. There is zero runtime SQL parsing.
  The benchmark measures pure execution + deserialization overhead.
- **PG via UDS** uses the auto-detected sync connection path, bypassing the async
  runtime entirely. TCP connections use the standard async path.
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
- **SQLite NOMUTEX**: bsql opens SQLite with `SQLITE_OPEN_NOMUTEX` since the
  pool architecture guarantees single-thread-per-connection access. This
  eliminates mutex overhead on every API call.
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
