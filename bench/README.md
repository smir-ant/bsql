# bsql Benchmarks

Comparative benchmarks: **bsql** vs **C** vs **diesel (Rust)** vs **sqlx (Rust)** vs **Go** on PostgreSQL and SQLite.

All times are median. Microseconds unless noted. Collected 2026-04-02.

## PostgreSQL

| Operation | bsql | C (libpq) | diesel (Rust) | sqlx (Rust) | Go (pgx) |
|---|---|---|---|---|---|
| Single row by PK | **15.0 us** <kbd>x1</kbd> | 15.8 us <kbd>x1.1</kbd> | 28.6 us <kbd>x1.9</kbd> | 59.6 us <kbd>x4.0</kbd> | 34.9 us <kbd>x2.3</kbd> |
| 10 rows | **26.0 us** <kbd>x1</kbd> | 27.1 us <kbd>x1.0</kbd> | 36.2 us <kbd>x1.4</kbd> | 78.4 us <kbd>x3.0</kbd> | 52.2 us <kbd>x2.0</kbd> |
| 100 rows | **47.6 us** <kbd>x1</kbd> | 52.8 us <kbd>x1.1</kbd> | 68.7 us <kbd>x1.4</kbd> | 116 us <kbd>x2.4</kbd> | 87.0 us <kbd>x1.8</kbd> |
| 1,000 rows | **293 us** <kbd>x1</kbd> | 327 us <kbd>x1.1</kbd> | 475 us <kbd>x1.6</kbd> | 537 us <kbd>x1.8</kbd> | 365 us <kbd>x1.2</kbd> |
| 10,000 rows | **2.66 ms** <kbd>x1</kbd> | 2.99 ms <kbd>x1.1</kbd> | 4.53 ms <kbd>x1.7</kbd> | 4.32 ms <kbd>x1.6</kbd> | 3.04 ms <kbd>x1.1</kbd> |
| Insert single | 104 us <kbd>x1</kbd> | 105 us <kbd>x1.0</kbd> | 99.5 us <kbd>x1.0</kbd> | 147 us <kbd>x1.4</kbd> | 119 us <kbd>x1.1</kbd> |
| Insert batch (100) | **941 us** <kbd>x1</kbd> | 1.84 ms <kbd>x2.0</kbd> | 2.88 ms <kbd>x3.1</kbd> | 2.80 ms <kbd>x3.0</kbd> | 3.67 ms <kbd>x3.9</kbd> |
| JOIN + aggregate | 24.9 ms <kbd>x1</kbd> | 24.1 ms <kbd>x1.0</kbd> | 23.1 ms <kbd>x0.9</kbd> | 23.3 ms <kbd>x0.9</kbd> | 25.4 ms <kbd>x1.0</kbd> |
| Subquery | **62.5 us** <kbd>x1</kbd> | 65.7 us <kbd>x1.1</kbd> | 117 us <kbd>x1.9</kbd> | 154 us <kbd>x2.5</kbd> | 97.7 us <kbd>x1.6</kbd> |

All benchmarks use Unix domain socket (UDS) connections to PostgreSQL. UDS eliminates the TCP network stack -- no packet framing, no congestion control, no Nagle delays -- isolating pure library performance from network noise. This applies equally to ALL libraries in the comparison (bsql, C, Go, diesel, sqlx). For TCP benchmarks, see the methodology section.

Note: INSERT single and JOIN+aggregate show parity (within PG server variance +/-5us per run).

## SQLite

| Operation | bsql | C (sqlite3) | diesel (Rust) | sqlx (Rust) | Go (go-sqlite3) |
|---|---|---|---|---|---|
| Single row by PK | **1.35 us** <kbd>x1</kbd> | 2.02 us <kbd>x1.5</kbd> | 2.94 us <kbd>x2.2</kbd> | 30.4 us <kbd>x22.5</kbd> | 3.38 us <kbd>x2.5</kbd> |
| 10 rows | **2.00 us** <kbd>x1</kbd> | 5.29 us <kbd>x2.6</kbd> | 7.47 us <kbd>x3.7</kbd> | 47.9 us <kbd>x24.0</kbd> | 10.4 us <kbd>x5.2</kbd> |
| 100 rows | **9.58 us** <kbd>x1</kbd> | 15.1 us <kbd>x1.6</kbd> | 33.2 us <kbd>x3.5</kbd> | 215 us <kbd>x22.4</kbd> | 74.8 us <kbd>x7.8</kbd> |
| 1,000 rows | **85.8 us** <kbd>x1</kbd> | 113 us <kbd>x1.3</kbd> | 256 us <kbd>x3.0</kbd> | 1.85 ms <kbd>x21.6</kbd> | 699 us <kbd>x8.1</kbd> |
| 10,000 rows | **866 us** <kbd>x1</kbd> | 1.10 ms <kbd>x1.3</kbd> | 2.85 ms <kbd>x3.3</kbd> | 20.6 ms <kbd>x23.8</kbd> | 7.22 ms <kbd>x8.3</kbd> |
| Insert single | **20.2 us** <kbd>x1</kbd> | 36.5 us <kbd>x1.8</kbd> | 57.8 us <kbd>x2.9</kbd> | 475 us <kbd>x23.5</kbd> | 25.9 us <kbd>x1.3</kbd> |
| Insert batch (100) | **1.22 ms** <kbd>x1</kbd> | 1.56 ms <kbd>x1.3</kbd> | 1.41 ms <kbd>x1.2</kbd> | 2.08 ms <kbd>x1.7</kbd> | 1.45 ms <kbd>x1.2</kbd> |
| JOIN + aggregate | 21.3 ms <kbd>x1</kbd> | 20.6 ms <kbd>x1.0</kbd> | 24.6 ms <kbd>x1.2</kbd> | 25.9 ms <kbd>x1.2</kbd> | 25.9 ms <kbd>x1.2</kbd> |
| Subquery | **29.9 us** <kbd>x1</kbd> | 43.4 us <kbd>x1.5</kbd> | 46.4 us <kbd>x1.6</kbd> | 189 us <kbd>x6.3</kbd> | 75.2 us <kbd>x2.5</kbd> |

All SQLite benchmarks use NOMUTEX mode (`SQLITE_OPEN_NOMUTEX`). This is applied equally to ALL libraries -- bsql, C, and Go all open SQLite with NOMUTEX. Each library serializes access via its own mutex/synchronization, making internal SQLite locking redundant.

## How to Run

You need: Rust, Go 1.26+, a C compiler (clang or gcc), PostgreSQL, and SQLite.

**PostgreSQL:**
```bash
createdb bench_db
export BENCH_DATABASE_URL="postgres://user@localhost/bench_db?host=/tmp"
export BSQL_DATABASE_URL=$BENCH_DATABASE_URL
psql "$BENCH_DATABASE_URL" -f setup/pg_setup.sql
```

**SQLite:**
```bash
rm -f bench.db
sqlite3 bench.db < setup/sqlite_setup.sql
export BENCH_SQLITE_PATH=bench.db
export BSQL_DATABASE_URL=sqlite://bench.db
```

**Run everything:**
```bash
# Rust (Criterion)
cargo bench

# C
cd c && make all && BENCH_DATABASE_URL="$BENCH_DATABASE_URL" ./pg_bench && BENCH_SQLITE_PATH=../bench.db ./sqlite_bench && cd ..

# Go
cd go && go mod tidy && BENCH_DATABASE_URL="$BENCH_DATABASE_URL" go run pg_bench.go && BENCH_SQLITE_PATH=../bench.db go run sqlite_bench.go && cd ..
```

Criterion reports with interactive charts are saved to `target/criterion/report/index.html`.

## Machine

Apple M1 Pro (10-core), 16 GB RAM, macOS Darwin 25.0.0, Rust 1.96.0-nightly, Go 1.26.0, Apple clang 17.0.0, PostgreSQL 15.14, SQLite 3.51.0.

## Methodology

Every benchmark implementation (Rust, C, Go) does identical work per iteration:

1. Send the prepared query with parameters.
2. Receive all rows from the server/engine.
3. Read every column of every row into local variables (preventing dead-code elimination).
4. Discard the row immediately -- no materialization into a Vec/slice/array.

Rust `fetch_all` materializes into a `Vec`, but the allocation cost is included in its measurement -- that is the API users actually call. C calls `PQgetvalue` / `sqlite3_column_*` for each column. Go calls `rows.Scan(...)` into stack locals.

INSERT benchmarks grow the database over time. Re-run `setup/pg_setup.sql` or `setup/sqlite_setup.sql` to reset between runs. The C and Go benchmarks run 1,000-10,000 iterations with nanosecond-precision timing (`mach_absolute_time` on macOS for C, `time.Now()` for Go).

## Library Notes

- **bsql** validates all SQL at compile time. Zero runtime SQL parsing. The benchmark measures pure execution + deserialization.
- **sqlx** uses `query_as` (not `query_as!`) to avoid requiring a compile-time database for the sqlx side. This is the common runtime usage pattern.
- **diesel** uses `sql_query` with raw SQL for an apples-to-apples comparison, avoiding diesel's DSL overhead. diesel is fundamentally synchronous; benchmarks run without `to_async()`.
- **C (libpq)** uses `PQexecPrepared` with prepared statements. Every benchmark reads every column via `PQgetvalue`. Insert batch uses 100 separate `PQexecPrepared` calls in a transaction (no pipelining -- libpq doesn't have built-in pipeline for this pattern).
- **C (sqlite3)** uses `sqlite3_prepare_v2` with statement reuse. WAL mode enabled. Type-dispatched `sqlite3_column_*` reads every column.
- **Go (pgx)** uses a direct `pgx.Conn` (not a pool). Queries are automatically prepared on first use.
- **Go (go-sqlite3)** uses `database/sql` with prepared statements. WAL mode enabled.

## Compiler Flags

- **Rust**: `cargo bench` uses `--release` (Criterion default). Default release profile (no LTO override).
- **C**: `-O3 -march=native` (see `c/Makefile`).
- **Go**: default compiler optimizations (Go does not expose `-O` flags).
- **PostgreSQL**: default server configuration, no special tuning.
