# bsql Benchmarks

Comparative benchmarks: **bsql** vs **C** vs **diesel (Rust)** vs **sqlx (Rust)** vs **Go** on PostgreSQL and SQLite.

All times are mean of N iterations. Microseconds unless noted. Collected 2026-04-07.

## PostgreSQL

> **Note on async:** bsql and diesel are benchmarked in sync mode (no async runtime overhead). sqlx is benchmarked through tokio runtime. All libraries use Unix domain sockets (UDS) to PostgreSQL. For TCP benchmarks, see methodology section.

| Operation | bsql | C (libpq) | diesel (Rust) | sqlx (Rust) | Go (pgx) |
|---|---|---|---|---|---|
| Single row by PK | **14.6 us** <kbd>x1</kbd> | 15.4 us <kbd>x1.1</kbd> | 31.7 us <kbd>x2.2</kbd> | 60.5 us <kbd>x4.1</kbd> | 33.6 us <kbd>x2.3</kbd> |
| 10 rows | **25.6 us** <kbd>x1</kbd> | 27.9 us <kbd>x1.1</kbd> | 36.8 us <kbd>x1.4</kbd> | 82.3 us <kbd>x3.2</kbd> | 53.4 us <kbd>x2.1</kbd> |
| 100 rows | **48.7 us** <kbd>x1</kbd> | 57.3 us <kbd>x1.2</kbd> | 78.8 us <kbd>x1.6</kbd> | 138 us <kbd>x2.8</kbd> | 86.9 us <kbd>x1.8</kbd> |
| 1,000 rows | **290 us** <kbd>x1</kbd> | 328 us <kbd>x1.1</kbd> | 529 us <kbd>x1.8</kbd> | 516 us <kbd>x1.8</kbd> | 356 us <kbd>x1.2</kbd> |
| 10,000 rows | **2.74 ms** <kbd>x1</kbd> | 3.59 ms <kbd>x1.3</kbd> | 5.74 ms <kbd>x2.1</kbd> | 4.39 ms <kbd>x1.6</kbd> | 3.18 ms <kbd>x1.2</kbd> |
| Insert single | **82.8 us** <kbd>x1</kbd> | 97.5 us <kbd>x1.2</kbd> | 101 us <kbd>x1.2</kbd> | 142 us <kbd>x1.7</kbd> | 134 us <kbd>x1.6</kbd> |
| Insert batch (100) | **733 us** <kbd>x1</kbd> | 2.42 ms <kbd>x3.3</kbd> | 3.30 ms <kbd>x4.5</kbd> | 2.89 ms <kbd>x3.9</kbd> | 3.78 ms <kbd>x5.2</kbd> |
| Insert batch pipelined (100) | **733 us** <kbd>x1</kbd> | 1.27 ms <kbd>x1.7</kbd> | — | — | 856 us <kbd>x1.2</kbd> |
| JOIN + aggregate | **30.1 ms** <kbd>x1</kbd> | 31.8 ms <kbd>x1.1</kbd> | 32.1 ms <kbd>x1.1</kbd> | 31.8 ms <kbd>x1.1</kbd> | 30.9 ms <kbd>x1.0</kbd> |
| Subquery | **119 us** <kbd>x1</kbd> | 134 us <kbd>x1.1</kbd> | 182 us <kbd>x1.5</kbd> | 225 us <kbd>x1.9</kbd> | 154 us <kbd>x1.3</kbd> |
| Dynamic (4 clauses)† | **152 us** <kbd>x1</kbd> | 179 us <kbd>x1.2</kbd> | 102 us† | — | 162 us |

bsql is faster than C (libpq) on every operation except JOIN+aggregate (where PostgreSQL engine time dominates — all libraries spend ~30 ms waiting for PG to process the query).

**† Dynamic query — why diesel is faster here:** diesel uses PostgreSQL's "simple query" protocol, which sends the entire SQL as one message. bsql and C use the "extended query" protocol with prepared statements, which sends 3 messages (Bind + Execute + Sync). The 3-message protocol is safer (type-checked bind parameters, binary results, cached query plans) but has ~50 μs more overhead per query. diesel skips all of this — it concatenates values directly into the SQL string and sends it as plain text. This is faster for lightweight queries but provides no compile-time validation, no type safety, and no SQL injection protection. For heavy queries (JOINs, large result sets), prepared statements win because PG reuses the cached execution plan.

sqlx does not have a dynamic clause mechanism — users build SQL strings manually. It is omitted from the dynamic row because there is no idiomatic equivalent to benchmark.

Each runner warms up PG cache with a full pass immediately before measuring. Double warm-up eliminates shared_buffers cold-start noise. Use `run_bsql_vs_c.sh` for bsql vs C only, `run_pg.sh` for all 5.

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

## Notes on performance claims

Single-row results (bsql 14.6 us vs C 15.4 us) are close — the difference is ~5%, within the range of PG server variance. bsql's advantage grows with result size: 10-20% faster on multi-row fetches (binary protocol + zero-copy parsing) and 42% faster on pipelined batch INSERT (bind template reuse + batched I/O).

Both libraries spend ~80-85 us on PG engine time for INSERT. The remaining ~1-15 us is driver overhead. Measuring driver overhead in isolation requires instrumenting both codebases identically — we have not done this for libpq, so we do not claim a specific driver overhead ratio.

## Memory (peak RSS)

Standalone binaries that each connect to PostgreSQL and run 10,000 SELECT queries + 1,000 INSERT queries, then exit. Peak resident set size measured externally via `/usr/bin/time -l` on macOS. Built and measured by `mem/run_all.sh`.

| Library | Peak RSS | vs bsql |
|---|---|---|
| **bsql** | **1.61 MB** | <kbd>x1</kbd> |
| C (libpq) | 6.82 MB | <kbd>x4.2</kbd> |
| sqlx (Rust) | 6.98 MB | <kbd>x4.3</kbd> |
| diesel (Rust) | 7.34 MB | <kbd>x4.6</kbd> |
| Go (pgx) | 17.3 MB | <kbd>x10.7</kbd> |

Run the memory benchmarks:
```bash
BENCH_DATABASE_URL="host=/tmp dbname=bench_db" \
BSQL_DATABASE_URL="postgres://YOUR_USER@localhost/bench_db?host=/tmp" \
./mem/run_all.sh
```

Each binary does identical work: connect, 10K SELECTs by PK, 1K INSERTs, exit. bsql uses the driver directly (no pool, no tokio). C uses libpq with prepared statements. All binaries read every column of every row to prevent dead-code elimination.

## How to reproduce

### Prerequisites
- PostgreSQL (any version 15-18)
- Rust stable (1.75+)
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

`run_pg.sh` handles database reset, cache warm-up, and CHECKPOINT between runs automatically. `run_bsql_vs_c.sh` runs only bsql and C for quick iteration during development.

**Note on bench workspace**: The `bench/` directory is a separate Cargo workspace (excluded from the main workspace). It pins `time = ">=0.3.36, <0.3.38"` to match bsql's MSRV-compatible range. If diesel or sqlx update their `time` dependency beyond this range, the pin may need adjustment.

## Machine

Apple M1 Pro (10-core), 16 GB RAM, macOS Darwin 25.0.0, Rust stable 1.94.0, Go 1.26.0, Apple clang 17.0.0, PostgreSQL 15.14, SQLite 3.51.0.

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

**Note on batch INSERT**: bsql uses pipeline batching (N Bind+Execute messages in one round-trip). The C benchmark includes both sequential (2.42 ms) and pipelined (1.27 ms) variants. Go (pgx) includes a pipelined variant via `SendBatch` (856 us). diesel and sqlx do not support pipeline protocol — they send each INSERT as a separate round-trip within a transaction.
- **C (sqlite3)** uses `sqlite3_prepare_v2` with statement reuse. WAL mode enabled. Type-dispatched `sqlite3_column_*` reads every column.
- **Go (pgx)** uses a direct `pgx.Conn` (not a pool). Queries are automatically prepared on first use. Batch INSERT includes both sequential (`tx.Exec` loop) and pipelined (`SendBatch`) variants.
- **Go (go-sqlite3)** uses `database/sql` with prepared statements. WAL mode enabled.

## Compiler Flags

- **Rust**: `cargo bench` uses `--release` (Criterion default). Default release profile (no LTO override).
- **C**: `-O3 -march=native` (see `c/Makefile`).
- **Go**: default compiler optimizations (Go does not expose `-O` flags).
- **PostgreSQL**: default server configuration, no special tuning.
