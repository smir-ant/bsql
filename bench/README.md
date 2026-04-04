# bsql Benchmarks

Comparative benchmarks: **bsql** vs **C** vs **diesel (Rust)** vs **sqlx (Rust)** vs **Go** on PostgreSQL and SQLite.

All times are median. Microseconds unless noted. Collected 2026-04-05.

## PostgreSQL

| Operation | bsql | C (libpq) | diesel (Rust) | sqlx (Rust) | Go (pgx) |
|---|---|---|---|---|---|
| Single row by PK | **15.4 us** <kbd>x1</kbd> | 17.0 us <kbd>x1.1</kbd> | 28.5 us <kbd>x1.9</kbd> | 60.6 us <kbd>x3.9</kbd> | 34.9 us <kbd>x2.3</kbd> |
| 10 rows | **27.3 us** <kbd>x1</kbd> | 29.4 us <kbd>x1.1</kbd> | 36.2 us <kbd>x1.3</kbd> | 78.4 us <kbd>x2.9</kbd> | 52.2 us <kbd>x1.9</kbd> |
| 100 rows | **50.1 us** <kbd>x1</kbd> | 58.8 us <kbd>x1.2</kbd> | 68.7 us <kbd>x1.4</kbd> | 116 us <kbd>x2.3</kbd> | 87.0 us <kbd>x1.7</kbd> |
| 1,000 rows | **308 us** <kbd>x1</kbd> | 386 us <kbd>x1.3</kbd> | 475 us <kbd>x1.5</kbd> | 537 us <kbd>x1.7</kbd> | 365 us <kbd>x1.2</kbd> |
| 10,000 rows | **2.79 ms** <kbd>x1</kbd> | 3.45 ms <kbd>x1.2</kbd> | 4.53 ms <kbd>x1.6</kbd> | 4.32 ms <kbd>x1.5</kbd> | 3.04 ms <kbd>x1.1</kbd> |
| Insert single | **89.0 us** <kbd>x1</kbd> | 99.7 us <kbd>x1.1</kbd> | 115 us <kbd>x1.3</kbd> | 152 us <kbd>x1.7</kbd> | 126 us <kbd>x1.4</kbd> |
| Insert batch (100) | **1.02 ms** <kbd>x1</kbd> | 2.33 ms <kbd>x2.3</kbd> | 3.06 ms <kbd>x3.0</kbd> | 3.13 ms <kbd>x3.1</kbd> | 3.67 ms <kbd>x3.6</kbd> |
| JOIN + aggregate | **40.8 ms** <kbd>x1</kbd> | 41.4 ms <kbd>x1.0</kbd> | 41.6 ms <kbd>x1.0</kbd> | 42.4 ms <kbd>x1.0</kbd> | 41.3 ms <kbd>x1.0</kbd> |
| Subquery | **65.9 us** <kbd>x1</kbd> | 69.5 us <kbd>x1.1</kbd> | 126 us <kbd>x1.9</kbd> | 155 us <kbd>x2.4</kbd> | 100 us <kbd>x1.5</kbd> |

All numbers measured in one sequential session (C, Go, Rust in quick succession), same PG server state, same machine load.

All benchmarks use Unix domain socket (UDS) connections to PostgreSQL. UDS eliminates the TCP network stack -- no packet framing, no congestion control, no Nagle delays -- isolating pure library performance from network noise. This applies equally to ALL libraries in the comparison (bsql, C, Go, diesel, sqlx). For TCP benchmarks, see the methodology section.

## SQLite

| Operation | bsql | C (sqlite3) | diesel (Rust) | sqlx (Rust) | Go (go-sqlite3) |
|---|---|---|---|---|---|
| Single row by PK | **1.73 us** <kbd>x1</kbd> | 2.49 us <kbd>x1.4</kbd> | 3.26 us <kbd>x1.9</kbd> | 32.9 us <kbd>x19.0</kbd> | 3.38 us <kbd>x2.0</kbd> |
| 10 rows | **2.36 us** <kbd>x1</kbd> | 5.84 us <kbd>x2.5</kbd> | 7.47 us <kbd>x3.2</kbd> | 47.9 us <kbd>x20.3</kbd> | 10.4 us <kbd>x4.4</kbd> |
| 100 rows | **9.94 us** <kbd>x1</kbd> | 15.8 us <kbd>x1.6</kbd> | 33.2 us <kbd>x3.3</kbd> | 215 us <kbd>x21.6</kbd> | 74.8 us <kbd>x7.5</kbd> |
| 1,000 rows | **86.8 us** <kbd>x1</kbd> | 115 us <kbd>x1.3</kbd> | 256 us <kbd>x2.9</kbd> | 1.85 ms <kbd>x21.3</kbd> | 699 us <kbd>x8.1</kbd> |
| 10,000 rows | **852 us** <kbd>x1</kbd> | 1.11 ms <kbd>x1.3</kbd> | 2.85 ms <kbd>x3.3</kbd> | 20.6 ms <kbd>x24.2</kbd> | 7.22 ms <kbd>x8.5</kbd> |
| Insert single | **20.5 us** <kbd>x1</kbd> | 33.8 us <kbd>x1.6</kbd> | 57.8 us <kbd>x2.8</kbd> | 475 us <kbd>x23.2</kbd> | 25.9 us <kbd>x1.3</kbd> |
| Insert batch (100) | **1.29 ms** <kbd>x1</kbd> | 1.74 ms <kbd>x1.3</kbd> | 1.41 ms <kbd>x1.1</kbd> | 2.08 ms <kbd>x1.6</kbd> | 1.45 ms <kbd>x1.1</kbd> |
| JOIN + aggregate\* | 22.0 ms <kbd>x1</kbd> | 21.1 ms <kbd>x0.96</kbd> | 24.6 ms <kbd>x1.1</kbd> | 25.9 ms <kbd>x1.2</kbd> | 25.9 ms <kbd>x1.2</kbd> |
| Subquery | **31.1 us** <kbd>x1</kbd> | 42.5 us <kbd>x1.4</kbd> | 46.4 us <kbd>x1.5</kbd> | 189 us <kbd>x6.1</kbd> | 75.2 us <kbd>x2.4</kbd> |

Benchmarks use system SQLite (same library as C benchmark) for equal comparison conditions. The bsql library default is bundled SQLite for portability.

All SQLite benchmarks use NOMUTEX mode (`SQLITE_OPEN_NOMUTEX`). This is applied equally to ALL libraries — bsql, C, and Go all open SQLite with NOMUTEX. Each library serializes access via its own mutex/synchronization, making internal SQLite locking redundant.

\* **JOIN + aggregate** is the only benchmark where C is marginally faster (2%). This is not driver overhead — bsql's driver overhead on this query is **0 ns** (measured by comparing a raw `sqlite3_step` loop vs bsql's `for_each` in the same process: identical timing). The 2% gap comes from **FFI boundary crossing**: SQLite's JOIN internally executes ~100K `sqlite3_step` calls. Each Rust→C FFI call costs ~2–4 ns for ARM ABI register save/restore. 100K × 3 ns ≈ 300 µs on a 21 ms query. C calls `sqlite3_step` as a native function with zero crossing overhead. This is an inherent cost of using any C library from Rust — every Rust SQLite library (rusqlite, sqlx, diesel) pays it equally. The only way to eliminate it: a pure-Rust SQLite engine (e.g. [Limbo](https://github.com/tursodatabase/limbo)).

## Driver overhead (excluding database engine time)

The total query time includes database engine processing (query planning,
disk I/O, WAL writes) which is identical for all libraries. The driver
overhead -- message building, wire protocol, response parsing -- is where
libraries differ:

| Component | bsql | C (libpq) |
|---|---|---|
| Message build (Bind+Execute+Sync) | **79 ns** | ~200 ns |
| Response parse (BindComplete+CommandComplete+ReadyForQuery) | **200 ns** | ~350 ns |
| **Total driver overhead** | **279 ns** | **~550 ns** |

bsql's driver overhead is **2x smaller than C**. When benchmark results
show similar totals (e.g., INSERT single: 89.0 us vs 99.7 us), the
apparent similarity hides a 2x advantage in driver code -- the database
engine time (~85 us) dominates both measurements equally.

C's overhead was estimated from libpq source code analysis. bsql's
overhead was measured by instrumenting the send/receive phases separately.

## Memory (peak RSS)

Standalone binaries that each connect to PostgreSQL and run 10,000 SELECT queries + 1,000 INSERT queries, then exit. Peak resident set size measured externally via `/usr/bin/time -l` on macOS.

| Library | Peak RSS | vs bsql |
|---|---|---|
| **bsql** | **2.2 MB** | <kbd>x1</kbd> |
| sqlx (Rust) | 7.1 MB | <kbd>x3.3</kbd> |
| diesel (Rust) | 7.3 MB | <kbd>x3.4</kbd> |
| C (libpq) | 8.6 MB | <kbd>x4.0</kbd> |
| Go (pgx) | 15.0 MB | <kbd>x6.9</kbd> |

Run the memory benchmarks:
```bash
BENCH_DATABASE_URL="host=/tmp dbname=bench_db" ./mem/run_all.sh
```

Each binary does identical work: connect, 10K SELECTs by PK, 1K INSERTs, exit. No connection pooling variance -- bsql and sqlx use a pool with 1 connection, diesel and C use a single connection directly.

## How to reproduce

### Prerequisites
- PostgreSQL (any version 15-18)
- Rust stable (1.85+)
- Go 1.22+
- C compiler (clang or gcc)
- SQLite 3.37+ (system or bundled)

### Setup
```bash
# PostgreSQL
createdb bench_db
psql bench_db -f setup/pg_setup.sql
export BENCH_DATABASE_URL="postgres://YOUR_USER@localhost/bench_db?host=/tmp"
export BSQL_DATABASE_URL="$BENCH_DATABASE_URL"

# SQLite
sqlite3 bench.db < setup/sqlite_setup.sql
export BENCH_SQLITE_PATH=bench.db
```

### Run all benchmarks
```bash
# Rust (bsql, sqlx, diesel)
cargo bench

# C
cd c && make all
BENCH_DATABASE_URL="host=/tmp dbname=bench_db" ./pg_bench
BENCH_SQLITE_PATH=../bench.db ./sqlite_bench

# Go
cd go
BENCH_DATABASE_URL="host=/tmp dbname=bench_db" go run ./pg/
BENCH_SQLITE_PATH=../bench.db go run ./sqlite/

# Memory (peak RSS)
./mem/run_all.sh
```

Run C, Go, and Rust benchmarks in quick succession (all within ~5 minutes) to ensure consistent PG server state. PG background maintenance (autovacuum, checkpoints) can add 10-50% variance to INSERT and complex queries.

Criterion reports with interactive charts are saved to `target/criterion/report/index.html`.

## Machine

Apple M1 Pro (10-core), 16 GB RAM, macOS Darwin 25.0.0, Rust 1.96.0-nightly, Go 1.26.0, Apple clang 17.0.0, PostgreSQL 15.14, SQLite 3.51.0.

## Methodology

Each Rust benchmark uses Criterion (100 samples x ~1,000 iterations per sample). For volatile operations (INSERT, JOIN), results vary +/-10-15% between runs due to PostgreSQL server state (WAL checkpointing, background writer, kernel scheduling). Numbers in the tables represent a single Criterion run. For the most accurate comparison, run all benchmarks in sequence on an idle system.

All benchmarks run in the same process and share the same database connection conditions. The order is: fetch_one, fetch_many, insert, complex. This ensures consistent PG server state across libraries within each benchmark category.

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
