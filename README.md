# bsql — benchmark harness

This branch is the **benchmark project** for [bsql](https://github.com/smir-ant/bsql)
(the library lives on [`main`](https://github.com/smir-ant/bsql/tree/main); it is a
`git` dependency here, so this branch carries no copy). It measures bsql against the
same workload in the client field of **two databases**:

- **PostgreSQL** — bsql (async + sync), C/libpq, Go/pgx, tokio-postgres, sqlx, diesel.
- **SQLite** — bsql, C/sqlite3, diesel, Go/mattn, sqlx.

Every number is from an **actual run**, not hand-written; each client's source is
under [`clients/`](clients) and re-runs from committed files. The point is that you
don't have to trust the tables — [reproduce them](#reproduce-it-yourself).

> Looking for **what each library can *do*** (compile-checked SQL, N+1 detection,
> typed COPY, …)? That capability comparison lives on
> [`main`](https://github.com/smir-ant/bsql#how-it-compares) — this branch is only
> about speed and memory.

## Where it was measured

- **Device:** MacBook Pro 14″ (Apple **M1 Pro**), macOS **26.0.1**,
  `aarch64-apple-darwin`.
- **Servers:** PostgreSQL **15.14** (Homebrew), loopback TCP, trust auth; SQLite via
  the **bundled amalgamation** (one file DB), one connection per client.
- **Toolchains:** rustc **1.97.0** (stable; this bench is an independent `git`-dep
  project — the library itself pins **1.96.0** for its diagnostic goldens), Go
  **1.26.5**, Apple clang.
- **Max-performance build flags** (every client, so nobody is handicapped):

  | language | flags |
  |---|---|
  | **C** | `-O3 -march=native -flto` |
  | **Rust** (bsql + competitors) | `opt-level=3`, `lto="fat"`, `codegen-units=1`, `RUSTFLAGS="-C target-cpu=native"` |
  | **Go** | default optimized `go build` (arm64 has no `-march` knob; cgo on for the SQLite driver) |

- **Client libraries:** bsql **1.0.0-alpha**, tokio-postgres **0.7.18**, sqlx **0.8.6**,
  diesel / diesel-async **2.2 / 0.6**, jackc/pgx **v5.10**, mattn/go-sqlite3 **1.14**,
  libpq **14**.
- **SQLite engine versions** (each client prints its own at runtime): bsql, C and
  diesel link SQLite **3.50.2** (byte-identical engines — the C↔bsql delta is *pure
  wrapper overhead*); sqlx links 3.46.0, Go/mattn 3.46.1.

---

## PostgreSQL

Microseconds per operation (median of 3 warmed 7-rep runs), lower is better. **Bold**
is the fastest client in the row; the <kbd>×N</kbd> chip on every other cell is how
many times slower it is than that fastest. **bsql (sync)** and **bsql** are the *same
driver* in two modes — a blocking build and an async (tokio) build of one shared core,
not two libraries — shown side by side so each is compared like-for-like (see the notes
under the tables).

### PostgreSQL — latency (µs, lower better)
| scenario | bsql (sync) | bsql | C/libpq | sqlx | tokio-pg | diesel | Go/pgx |
|---|---|---|---|---|---|---|---|
| SELECT by-PK (1) | **24.6** <kbd>x1</kbd> | 26.2 <kbd>x1.1</kbd> | 25.5 <kbd>x1.0</kbd> | 27.9 <kbd>x1.1</kbd> | 39.8 <kbd>x1.6</kbd> | 41.3 <kbd>x1.7</kbd> | 52.1 <kbd>x2.1</kbd> |
| SELECT 10 rows | **37.2** <kbd>x1</kbd> | 40.0 <kbd>x1.1</kbd> | 39.7 <kbd>x1.1</kbd> | 42.6 <kbd>x1.1</kbd> | 53.0 <kbd>x1.4</kbd> | 55.4 <kbd>x1.5</kbd> | 74.8 <kbd>x2.0</kbd> |
| SELECT 100 rows | **49.7** <kbd>x1</kbd> | 52.6 <kbd>x1.1</kbd> | 54.7 <kbd>x1.1</kbd> | 70.1 <kbd>x1.4</kbd> | 72.0 <kbd>x1.4</kbd> | 78.6 <kbd>x1.6</kbd> | 78.5 <kbd>x1.6</kbd> |
| SELECT 1000 rows | **232.7** <kbd>x1</kbd> | 234.0 <kbd>x1.0</kbd> | 250.3 <kbd>x1.1</kbd> | 287.2 <kbd>x1.2</kbd> | 279.9 <kbd>x1.2</kbd> | 295.5 <kbd>x1.3</kbd> | 259.5 <kbd>x1.1</kbd> |
| INSERT single | 37.5 <kbd>x1.0</kbd> | 43.1 <kbd>x1.2</kbd> | **37.4** <kbd>x1</kbd> | 45.3 <kbd>x1.2</kbd> | 43.2 <kbd>x1.2</kbd> | 44.0 <kbd>x1.2</kbd> | 58.3 <kbd>x1.6</kbd> |
| JOIN + GROUP BY | **2.99 ms** <kbd>x1</kbd> | 3.01 ms <kbd>x1.0</kbd> | 3.04 ms <kbd>x1.0</kbd> | 3.03 ms <kbd>x1.0</kbd> | 3.05 ms <kbd>x1.0</kbd> | 3.03 ms <kbd>x1.0</kbd> | 3.05 ms <kbd>x1.0</kbd> |

### PostgreSQL — peak memory
Bytes from [`results/pg_rss.log`](results/pg_rss.log) ÷ 10⁶ (decimal MB).
| client | peak RSS |
|---|---|
| bsql (sync) | **1.69 MB** <kbd>x1</kbd> |
| bsql | 1.80 MB <kbd>x1.1</kbd> |
| tokio-postgres | 6.50 MB <kbd>x3.9</kbd> |
| sqlx | 6.73 MB <kbd>x4.0</kbd> |
| diesel | 7.01 MB <kbd>x4.2</kbd> |
| C/libpq | 13.25 MB <kbd>x7.9</kbd> |
| Go/pgx | 16.81 MB <kbd>x10.0</kbd> |

**How to read the two tables** — each note answers *"then why isn't bsql simply first
everywhere?"*

- **The fair blocking fight — bsql (sync) vs C/libpq.** Both are plain synchronous
  clients, so this is the true apples-to-apples row. bsql **wins every read** and ties
  INSERT — its safe, typed, zero-per-row-alloc decode costs essentially nothing over raw
  libpq.
- **The fair async fight — bsql vs tokio-postgres / sqlx.** bsql is the **fastest async
  driver here by a wide margin**. It runs ~1 µs behind *blocking* C on point reads — that
  gap is the reactor poll/park/wake every async runtime pays on a would-block read (a
  blocking client skips it) — and pulls **ahead** of C on larger results.
- **Compare within an execution model.** C/libpq and bsql (sync) block a thread; Go/pgx
  blocks a goroutine; the async Rust clients (bsql, tokio-postgres, sqlx) all run on a
  tokio **current-thread** runtime, equally — nobody is handicapped by the harness.
- **Memory — bsql is the leanest in this field.** A committed `rss_ceiling` gate fails the
  build if bsql's peak exceeds 2 MiB, so the figure is enforced, not aspirational. Beating
  even C is real: libpq links its whole client stack (TLS, ICU) while bsql holds a fixed
  ~4 KiB engine buffer + 16-byte row handles.

---

## SQLite

bsql's SQLite driver wraps the **same bundled engine** C links (3.50.2), so C is the
*engine-identical* reference — any C↔bsql gap is pure wrapper cost. Competitors that
can't express a bsql-only API variant emit a `SKIP` (never an invented number); the
apples-to-apples cells are the **prepared-reuse** rows (both sides reuse one compiled
statement).

### SQLite — latency (µs, lower better)
| scenario | bsql | C/sqlite3 | diesel | Go/mattn | sqlx |
|---|---|---|---|---|---|
| by-PK (prepared) | 1.56 <kbd>x1.0</kbd> | **1.53** <kbd>x1</kbd> | 1.89 <kbd>x1.2</kbd> | 3.20 <kbd>x2.1</kbd> | 5.96 <kbd>x3.9</kbd> |
| 10 rows (prepared) | **4.78** <kbd>x1</kbd> | 5.81 <kbd>x1.2</kbd> | 9.92 <kbd>x2.1</kbd> | 9.65 <kbd>x2.0</kbd> | 13.4 <kbd>x2.8</kbd> |
| 100 rows | 15.7 <kbd>x1.1</kbd> | **14.3** <kbd>x1</kbd> | 30.0 <kbd>x2.1</kbd> | 69.3 <kbd>x4.8</kbd> | 104.4 <kbd>x7.3</kbd> |
| 1000 rows | 107.8 <kbd>x1.1</kbd> | **97.5** <kbd>x1</kbd> | 225.5 <kbd>x2.3</kbd> | 656.5 <kbd>x6.7</kbd> | 1.42 ms <kbd>x14.5</kbd> |
| 10000 rows | 1.02 ms <kbd>x1.1</kbd> | **938.1** <kbd>x1</kbd> | 2.21 ms <kbd>x2.4</kbd> | 6.67 ms <kbd>x7.1</kbd> | 14.53 ms <kbd>x15.5</kbd> |
| INSERT single | 22.6 <kbd>x1.2</kbd> | **19.0** <kbd>x1</kbd> | 21.3 <kbd>x1.1</kbd> | 23.8 <kbd>x1.3</kbd> | 27.4 <kbd>x1.4</kbd> |
| INSERT batch (100) | **900.8** <kbd>x1</kbd> | 1.07 ms <kbd>x1.2</kbd> | 1.09 ms <kbd>x1.2</kbd> | 1.14 ms <kbd>x1.3</kbd> | 1.60 ms <kbd>x1.8</kbd> |
| Subquery | 29.6 <kbd>x1.1</kbd> | **26.4** <kbd>x1</kbd> | 42.1 <kbd>x1.6</kbd> | 66.3 <kbd>x2.5</kbd> | 109.5 <kbd>x4.1</kbd> |
| JOIN + aggregate | 33.34 ms <kbd>x1.0</kbd> | 33.62 ms <kbd>x1.0</kbd> | 33.42 ms <kbd>x1.0</kbd> | **33.27 ms** <kbd>x1</kbd> | 33.45 ms <kbd>x1.0</kbd> |

### SQLite — peak memory
| client | peak RSS |
|---|---|
| C/sqlite3 | **3.95 MB** <kbd>x1</kbd> |
| bsql | 4.01 MB <kbd>x1.0</kbd> |
| diesel | 4.26 MB <kbd>x1.1</kbd> |
| sqlx | 4.88 MB <kbd>x1.2</kbd> |
| Go/mattn | 17.45 MB <kbd>x4.4</kbd> |

**Read it right.** bsql rides at the **engine-identical C reference** (~1.0–1.2×
across the board, *faster* than C on the 10-row and batch-insert cells) — its safe,
typed, zero-per-row-alloc decode adds essentially nothing over raw `sqlite3_*` C. It
is far ahead of the other Rust/Go options: **diesel** ~2× (query-builder + materialize),
**Go/mattn** 2–7× (every column crosses the cgo boundary), **sqlx** 3–15× (it runs
SQLite on a dedicated background thread + channel, so every query crosses a thread
hop). RSS is a near-tie with C; only Go's runtime stands out (~17 MB).

†Footnotes: sqlx's compile-time SQL check needs a live DB or a committed offline cache
(bsql validates offline from migration files); diesel is a query-builder **DSL** (the
aggregate/subquery drop to raw `sql_query`) and its default `load()` materializes into
a `Vec` (no zero-per-row-alloc streaming). The `fetch_many/{100,1000,10000}` cells have
competitors reuse a prepared statement while bsql's *streaming* path per-call-prepares
(the honest prepared-vs-prepared cells are `by-PK (prepared)` and `10 rows (prepared)`;
the gap amortizes as N grows).

---

## Methodology

- **Same everything.** Identical SQL + schema, prepared once and reused (the cache-hit
  path a real workload lives on), every column of every row read so the driver actually
  decodes. Loopback TCP for all PostgreSQL clients (bsql *also* has a faster unix-socket
  transport it does not use here, so the field stays even).
- **Latency:** each client's own warmed timed loop — 2000-iteration warm-up, 7 reps,
  median ns/op — the same shape in every language, so C, Go and Rust are directly
  comparable. The tables show the median of **3** such runs. Server/engine-bound
  scenarios (the ~3 ms PG aggregate, the ~33 ms SQLite aggregate) converge because the
  cost is the database's, not the client's.
- **Execution model.** Blocking (C/libpq, bsql-sync, all SQLite C/Rust-sync clients),
  goroutine (Go), or async tokio **current-thread** (the async Rust clients, equally —
  a multi-thread runtime would only add a cross-thread `block_on` handoff irrelevant to
  a one-socket latency path). An async client pays an unavoidable reactor poll/park/wake
  a blocking client does not (~1 µs on cached reads, ~6 µs on the WAL-parked INSERT) —
  paid by *every* async client, bsql the fastest of them, and the price of concurrency
  this single-op serial micro-benchmark does not reward.
- **Memory:** one separate process per client (cold linked code can't pollute the
  figure), `getrusage(ru_maxrss)` after a fixed 10 000-read + 1 000-write workload.
- **Honesty:** numbers are captured from real runs, never hand-written; a client that
  can't run a scenario emits `SKIP <scenario> <reason>` (pointing at the equivalent
  cell), never an invented value. Each client decodes into real typed values; the C
  clients free every result (no leak inflating RSS).

## Reproduce it yourself

Prerequisites: a Rust toolchain, Go, a C compiler with `pg_config` on `PATH`, and (for
the PostgreSQL half) a local PostgreSQL you can reach. Both scripts use only paths
**relative to this repo** — no machine-specific editing needed; override the PG
connection with the standard `PGHOST` / `PGUSER` / `PGDATABASE` / `PGPORT` env vars.

```bash
git switch bench

# --- PostgreSQL (needs a local PG reachable at $PGHOST) ---
psql -h 127.0.0.1 -U "$USER" -d postgres -f setup/pg_setup.sql   # seed once
scripts/xlang_measure.sh all         # build all 7 clients + latency + RSS
# (or: scripts/xlang_measure.sh build | latency | rss  — one phase at a time)

# --- SQLite (self-contained; seeds a local bench.db) ---
scripts/xlang_measure_sqlite.sh all  # build + latency + RSS

# --- Deep benchmarks (self-contained; stands up its own ephemeral PG 15) ---
scripts/xlang_measure_deep.sh all    # concurrency throughput + constant-memory streaming

# --- or just the bsql side under criterion ---
cargo bench --bench e2e
```

Each `scripts/*_measure*.sh` builds every client with the max-perf flags above, runs
one client at a time (quiet machine), and prints the `LAT` / `SKIP` / `RSS` lines the
tables are built from. The exact outputs of the runs behind the tables above are
committed under [`results/`](results/) (see [`results/README.md`](results/README.md) for
which log backs which table, and how to re-derive any cell).

## Deeper benchmarks

Two benchmarks that probe a regime and a property the single-op matrix cannot. Both stand up
their **own dedicated ephemeral PostgreSQL 15** (own port + socket dir, `max_connections`
raised to 300, torn down on exit) — same version / machine / loopback TCP as the tables above,
so a deep run neither disturbs nor is disturbed by the shared server. Raw log:
[`results/deep_measure.log`](results/deep_measure.log). Reproduce:
`scripts/xlang_measure_deep.sh all` (or `concurrency` / `streaming`); pass `DEEP_PG_EXISTING=1`
to target a server you manage. Counts are env-tunable (`DEEP_WORKERS`, `DEEP_STREAM_ROWS`,
`CONC_WARMUP_MS`, `CONC_MEASURE_MS`).

### Concurrency — throughput and tail latency

The single-op table "penalises" bsql-async ~1 µs for the reactor poll/park/wake a blocking
client skips. Under concurrency that reactor **pays for itself**: one runtime multiplexes many
in-flight queries. **8 / 32 / 128 workers**, each holding one dedicated connection (the pgbench
`-c` model, so the figure reflects the driver + runtime, not pool-checkout policy), all looping
the by-PK read on a multi-thread tokio runtime — **bsql-async** (its `Pool`) vs **tokio-postgres**
vs **sqlx**.

**Raw throughput is a wash** — all three saturate the same loopback PostgreSQL, so aggregate
QPS lands within ~1 % of each other at 32 and 128 workers (bsql leads at 8). The real
separation is **tail latency**: bsql has the **lowest p99 at every level**, and the lowest p999
at 32 and 128 workers (within a microsecond of sqlx at 8) — its tighter per-op path is less
jitter under load.

#### Throughput — sustained QPS (higher better)
| workers | bsql-async | tokio-postgres | sqlx |
|---|---|---|---|
| 8 | **95.1k** <kbd>x1</kbd> | 90.3k <kbd>x1.05</kbd> | 92.2k <kbd>x1.03</kbd> |
| 32 | 118.3k <kbd>x1.01</kbd> | **119.2k** <kbd>x1</kbd> | 119.0k <kbd>x1.00</kbd> |
| 128 | 124.3k <kbd>x1.01</kbd> | 125.1k <kbd>x1.00</kbd> | **125.4k** <kbd>x1</kbd> |

#### Tail latency — p99 µs (lower better)
| workers | bsql-async | tokio-postgres | sqlx |
|---|---|---|---|
| 8 | **168** <kbd>x1</kbd> | 174 <kbd>x1.04</kbd> | 170 <kbd>x1.01</kbd> |
| 32 | **410** <kbd>x1</kbd> | 466 <kbd>x1.14</kbd> | 486 <kbd>x1.19</kbd> |
| 128 | **1632** <kbd>x1</kbd> | 1915 <kbd>x1.17</kbd> | 2037 <kbd>x1.25</kbd> |

### Constant-memory streaming — a property competitors structurally lack

bsql's `query_each` streams a colossal result in **O(1) resident memory with ~0 allocations per
row** (each row is a zero-copy `BorrowedRow`, nothing accumulates). A materialising client —
libpq's `PQexec`, tokio-postgres's `query()` — buffers the **whole** result first, so its RSS
grows **O(rows)**. Peak RSS reading a 1 M- and a 5 M-row result:

#### Peak RSS (lower better)
| rows | bsql `query_each` | tokio-postgres `query` | libpq `PQexec` |
|---|---|---|---|
| 1 M | **1.77 MB** <kbd>x1</kbd> | 197.8 MB <kbd>x112</kbd> | 105.5 MB <kbd>x60</kbd> |
| 5 M | **1.77 MB** <kbd>x1</kbd> | 962.3 MB <kbd>x544</kbd> | 466.4 MB <kbd>x264</kbd> |

**bsql's RSS is identical at 1 M and 5 M** — flat in row count — because it holds nothing:
streaming 5 M rows made **9 total heap allocations** (164 bytes), i.e. **0.000002
allocations/row**. The materialisers grow with the result — to ~962 MB (tokio-postgres) and
~466 MB (libpq) at 5 M rows, **~540×** and **~260×** bsql's footprint. This is the report over
tens of millions of rows that never grows memory — the thing `query_sql` / `PQexec` cannot do.
