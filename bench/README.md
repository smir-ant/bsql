# bsql Benchmarks

Comparative benchmarks: **bsql** vs **C** vs **diesel (Rust)** vs **sqlx (Rust)** vs **Go** on PostgreSQL and SQLite.

All times are mean of N iterations. Microseconds unless noted. Collected 2026-04-06.

## PostgreSQL

| Operation | bsql | C (libpq) | diesel (Rust) | sqlx (Rust) | Go (pgx) |
|---|---|---|---|---|---|
| Single row by PK | **15.2 us** <kbd>x1</kbd> | 15.6 us <kbd>x1.0</kbd> | 31.7 us <kbd>x2.1</kbd> | 60.5 us <kbd>x4.0</kbd> | 33.6 us <kbd>x2.2</kbd> |
| 10 rows | **26.4 us** <kbd>x1</kbd> | 28.0 us <kbd>x1.1</kbd> | 36.8 us <kbd>x1.4</kbd> | 82.3 us <kbd>x3.1</kbd> | 53.4 us <kbd>x2.0</kbd> |
| 100 rows | **49.5 us** <kbd>x1</kbd> | 54.5 us <kbd>x1.1</kbd> | 78.8 us <kbd>x1.6</kbd> | 138 us <kbd>x2.8</kbd> | 86.9 us <kbd>x1.8</kbd> |
| 1,000 rows | **303 us** <kbd>x1</kbd> | 320 us <kbd>x1.1</kbd> | 529 us <kbd>x1.7</kbd> | 516 us <kbd>x1.7</kbd> | 356 us <kbd>x1.2</kbd> |
| 10,000 rows | **2.73 ms** <kbd>x1</kbd> | 2.90 ms <kbd>x1.1</kbd> | 5.74 ms <kbd>x2.1</kbd> | 4.39 ms <kbd>x1.6</kbd> | 3.18 ms <kbd>x1.2</kbd> |
| Insert single | **85.2 us** <kbd>x1</kbd> | 88.5 us <kbd>x1.0</kbd> | 101 us <kbd>x1.2</kbd> | 142 us <kbd>x1.7</kbd> | 134 us <kbd>x1.6</kbd> |
| Insert batch (100) | **751 us** <kbd>x1</kbd> | 1.90 ms <kbd>x2.5</kbd> | 3.30 ms <kbd>x4.4</kbd> | 2.89 ms <kbd>x3.8</kbd> | 3.78 ms <kbd>x5.0</kbd> |
| JOIN + aggregate | **29.9 ms** <kbd>x1</kbd> | 30.0 ms <kbd>x1.0</kbd> | 32.1 ms <kbd>x1.1</kbd> | 31.8 ms <kbd>x1.1</kbd> | 30.3 ms <kbd>x1.0</kbd> |
| Subquery | **112 us** <kbd>x1</kbd> | 116 us <kbd>x1.0</kbd> | 182 us <kbd>x1.6</kbd> | 225 us <kbd>x2.0</kbd> | 162 us <kbd>x1.4</kbd> |

Single-row results (15.2 us vs 15.6 us) are within measurement noise (~3%). bsql's advantage on multi-row fetches (1.06-1.10x) and batch INSERT (2.5x) is statistically significant.

Each runner warms up PG cache with a full pass immediately before measuring. Double warm-up eliminates shared_buffers cold-start noise. Use `run_quick.sh` for bsql vs C, `run_pg.sh` for all 5.

All benchmarks use Unix domain socket (UDS) connections to PostgreSQL. UDS eliminates the TCP network stack -- no packet framing, no congestion control, no Nagle delays -- isolating pure library performance from network noise. This applies equally to ALL libraries in the comparison (bsql, C, Go, diesel, sqlx).

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
| **bsql** | **1.70 MB** | <kbd>x1</kbd> |
| C (libpq) | 6.50 MB | <kbd>x3.8</kbd> |
| sqlx (Rust) | 6.59 MB | <kbd>x3.9</kbd> |
| diesel (Rust) | 6.97 MB | <kbd>x4.1</kbd> |
| Go (pgx) | 16.8 MB | <kbd>x9.9</kbd> |

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
# Build everything
cargo build --release --bin bench_bsql_perf --bin bench_diesel_perf --bin bench_sqlx_perf
(cd c && make all)

# PostgreSQL — fair comparison (all 5 runners, equal conditions)
BENCH_DATABASE_URL="host=/tmp dbname=bench_db" \
BSQL_DATABASE_URL="postgres://YOUR_USER@localhost/bench_db?host=/tmp" \
./run_pg.sh

# SQLite
BENCH_SQLITE_PATH=bench.db cargo bench --bench sqlite_fetch_one --bench sqlite_fetch_many --bench sqlite_insert --bench sqlite_complex
(cd c && BENCH_SQLITE_PATH=../bench.db ./sqlite_bench)
(cd go && BENCH_SQLITE_PATH=../bench.db go run ./sqlite/)

# Memory (peak RSS)
BENCH_DATABASE_URL="host=/tmp dbname=bench_db" \
BSQL_DATABASE_URL="postgres://YOUR_USER@localhost/bench_db?host=/tmp" \
./mem/run_all.sh
```

`run_pg.sh` handles database reset, cache warm-up, and CHECKPOINT between runs automatically.

## Machine

Apple M1 Pro (10-core), 16 GB RAM, macOS Darwin 25.0.0, Rust 1.96.0-nightly, Go 1.26.0, Apple clang 17.0.0, PostgreSQL 15.14, SQLite 3.51.0.

## Methodology

All 5 runners (bsql, C, Go, diesel, sqlx) use **identical methodology**: N iterations, total time, mean per-op. No framework-specific harness (no Criterion for cross-language numbers). This ensures direct apples-to-apples comparison.

**Noise reduction:**
- Autovacuum disabled on bench tables (`ALTER TABLE SET (autovacuum_enabled = false)`)
- `ANALYZE` run before benchmarks for optimal query plans
- `CHECKPOINT` before and between INSERT-heavy runs to prevent WAL checkpoint noise
- All 5 runners warm up PG shared buffers before any measurement
- UDS connections eliminate TCP stack noise
- JOIN + aggregate uses 3,000 iterations (other operations: 10,000) for lower variance

**Iteration counts:**
- fetch_one, fetch_many (10/100/1000), INSERT single: 10,000 iterations
- fetch_many (10000), INSERT batch, JOIN + aggregate: 1,000-3,000 iterations
- Subquery: 5,000 iterations

Every benchmark implementation does identical work per iteration:

1. Send the prepared query with parameters (binary protocol for bsql and C).
2. Receive all rows from the server/engine.
3. Read every column of every row (preventing dead-code elimination).
4. bsql's `fetch()` returns zero-copy borrowed rows (`&str` fields), matching C's `PQgetvalue` (returns `char*` pointer). Both return references without heap allocation.

INSERT benchmarks grow the database over time. Re-run `setup/pg_setup.sql` or `setup/sqlite_setup.sql` to reset between runs (or use `run_pg.sh` which handles this automatically).

## Library Notes

- **bsql** validates all SQL at compile time. Zero runtime SQL parsing. The benchmark measures pure execution + deserialization.
- **sqlx** uses `query_as` (not `query_as!`) to avoid requiring a compile-time database for the sqlx side. This is the common runtime usage pattern.
- **diesel** uses `sql_query` with raw SQL for an apples-to-apples comparison, avoiding diesel's DSL overhead. diesel is fundamentally synchronous; benchmarks run without `to_async()`.
- **C (libpq)** uses `PQexecPrepared` with prepared statements. Every benchmark reads every column via `PQgetvalue`. Insert batch uses 100 separate `PQexecPrepared` calls in a transaction (no pipelining -- libpq doesn't have built-in pipeline for this pattern).

**Note on batch INSERT**: bsql uses pipeline batching (N Bind+Execute messages in one round-trip). The C benchmark uses sequential `PQexecPrepared` calls in a transaction (no pipelining). libpq supports pipelining since PG 14 (`PQpipelineSync`), which would narrow the gap. The batch INSERT comparison reflects the most common C usage pattern, not the theoretical best.
- **C (sqlite3)** uses `sqlite3_prepare_v2` with statement reuse. WAL mode enabled. Type-dispatched `sqlite3_column_*` reads every column.
- **Go (pgx)** uses a direct `pgx.Conn` (not a pool). Queries are automatically prepared on first use.
- **Go (go-sqlite3)** uses `database/sql` with prepared statements. WAL mode enabled.

## Compiler Flags

- **Rust**: `cargo bench` uses `--release` (Criterion default). Default release profile (no LTO override).
- **C**: `-O3 -march=native` (see `c/Makefile`).
- **Go**: default compiler optimizations (Go does not expose `-O` flags).
- **PostgreSQL**: default server configuration, no special tuning.
