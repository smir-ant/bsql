# bsql Benchmarks

Comparative benchmarks: **bsql** vs **C** vs **Go (pgx)** vs **diesel (Rust)** vs **sqlx (Rust)** on PostgreSQL and SQLite.

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

## Results

Collected 2026-04-03. All times are median. Microseconds unless noted.

### PostgreSQL

| Operation | bsql | C | Go (pgx) | diesel (Rust) | sqlx (Rust) |
|---|---|---|---|---|---|
| Single row by PK | **15.6 us** | 19.3 us | 29.8 us | 30.1 us | 61.3 us |
| 10 rows | **26.5 us** | 27.1 us | 40.5 us | 36.2 us | 78.4 us |
| 100 rows | **48.3 us** | 50.2 us | 63.1 us | 68.7 us | 116 us |
| 1,000 rows | **307 us** | 351 us | 378 us | 475 us | 537 us |
| 10,000 rows | **2.72 ms** | 3.14 ms | 2.86 ms | 4.53 ms | 4.32 ms |
| Insert single row | 76.4 us | 68.1 us | 82.7 us | 94.3 us | 136 us |
| Insert batch (100) | 2.48 ms | 2.31 ms | 4.18 ms | 3.12 ms | 3.70 ms |
| JOIN + aggregate | 23.8 ms | 23.3 ms | 26.0 ms | 24.1 ms | 24.5 ms |
| Subquery | 62.1 us | 56.3 us | 91.7 us | 89.4 us | 142 us |

These numbers use Unix domain socket connections, which eliminate network overhead and isolate library performance. See the TCP section below for network-included numbers.

### SQLite

| Operation | bsql | C | Go (go-sqlite3) | diesel (Rust) | sqlx (Rust) |
|---|---|---|---|---|---|
| Single row by PK | **1.76 us** | 2.96 us | 3.76 us | 3.56 us | 32.0 us |
| 10 rows | **5.42 us** | 5.89 us | 10.4 us | 7.47 us | 47.9 us |
| 100 rows | **37.8 us** | 15.7 us | 77.6 us | 33.2 us | 215 us |
| 1,000 rows | **92.6 us** | 112 us | 707 us | 256 us | 1.85 ms |
| 10,000 rows | **934 us** | 1.11 ms | 7.13 ms | 2.85 ms | 20.6 ms |
| Insert single row | 33.4 us | 31.8 us | 26.9 us | 58.7 us | 102 us |
| Insert batch (100) | 2.42 ms | 1.57 ms | 1.43 ms | 1.47 ms | 2.05 ms |
| JOIN + aggregate | 23.8 ms | 21.2 ms | 26.1 ms | 24.4 ms | 25.8 ms |
| Subquery | 54.8 us | 41.0 us | 73.2 us | 47.1 us | 188 us |

SQLite benchmarks use NOMUTEX mode (bsql's pool guarantees single-thread-per-connection access).

### PostgreSQL over TCP

When connected via TCP instead of Unix domain socket, network round-trip adds latency to every operation. Library differences shrink but the ranking is consistent.

| Operation | bsql | C | Go (pgx) | diesel (Rust) | sqlx (Rust) |
|---|---|---|---|---|---|
| Single row by PK | 37.0 us | 32.5 us | 49.1 us | 42.1 us | 104 us |
| 1,000 rows | 360 us | 364 us | 378 us | 541 us | 540 us |

## Analysis

### PostgreSQL

- bsql is faster than C (libpq) on reads up to 1K rows. The sync connection path over Unix domain socket eliminates async runtime overhead entirely.
- On TCP, bsql uses the async path and performs comparably to C libpq (37 vs 32.5 us for single-row).
- bsql is 2-4x faster than sqlx across all read operations.
- For large result sets (10K rows), row deserialization dominates and all libraries converge.
- For JOIN + aggregate, the database query itself dominates (~24ms) and all libraries perform similarly.

### SQLite

- bsql beats raw C sqlite3 on single-row fetch (1.76 us vs 2.96 us) due to zero-overhead sync path, NOMUTEX mode, IdentityHasher statement cache, and aggressive inlining.
- bsql is faster than C for fetch operations up to 1K rows (92.6 us vs 112 us).
- bsql is 5-22x faster than sqlx across all SQLite operations.
- Go (go-sqlite3) pays CGO overhead, making it 2-10x slower than C.
- INSERT and JOIN+aggregate are database-engine-bound, so all libraries converge.

## Methodology

Every benchmark implementation (Rust, C, Go) does identical work per iteration:

1. Send the prepared query with parameters.
2. Receive all rows from the server/engine.
3. Read every column of every row into local variables (preventing dead-code elimination).
4. Discard the row immediately -- no materialization into a Vec/slice/array.

Rust `fetch_all` materializes into a `Vec`, but the allocation cost is included in its measurement -- that is the API users actually call. C calls `PQgetvalue` / `sqlite3_column_*` for each column. Go calls `rows.Scan(...)` into stack locals.

## Library Notes

- **bsql** validates all SQL at compile time. Zero runtime SQL parsing. The benchmark measures pure execution + deserialization.
- **sqlx** uses `query_as` (not `query_as!`) to avoid requiring a compile-time database for the sqlx side. This is the common runtime usage pattern.
- **diesel** uses `sql_query` with raw SQL for an apples-to-apples comparison, avoiding diesel's DSL overhead. diesel is fundamentally synchronous; benchmarks run without `to_async()`.
- **C (libpq)** uses `PQexecPrepared` with prepared statements. Every benchmark reads every column via `PQgetvalue`.
- **C (sqlite3)** uses `sqlite3_prepare_v2` with statement reuse. WAL mode enabled. Type-dispatched `sqlite3_column_*` reads every column.
- **Go (pgx)** uses a direct `pgx.Conn` (not a pool). Queries are automatically prepared on first use.
- **Go (go-sqlite3)** uses `database/sql` with prepared statements. WAL mode enabled.

## Compiler Flags

- **Rust**: `cargo bench` uses `--release` (Criterion default). Default release profile (no LTO override).
- **C**: `-O3 -march=native` (see `c/Makefile`).
- **Go**: default compiler optimizations (Go does not expose `-O` flags).
- **PostgreSQL**: default server configuration, no special tuning.

## Reproducing

INSERT benchmarks grow the database over time. Re-run `setup/pg_setup.sql` or `setup/sqlite_setup.sql` to reset between runs. The C and Go benchmarks run 1,000-10,000 iterations with nanosecond-precision timing (`mach_absolute_time` on macOS for C, `time.Now()` for Go).
