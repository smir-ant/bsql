# bsql — benchmark harness

This branch is the **benchmark project** for [bsql](https://github.com/smir-ant/bsql)
(the library itself lives on [`main`](https://github.com/smir-ant/bsql/tree/main)).
It measures bsql against the same query workload in **seven clients across four
languages** — bsql (async + sync), C (libpq), Go (pgx), and the Rust field
(tokio-postgres, sqlx, diesel) — over one local PostgreSQL, plus a peak-memory
harness. bsql is depended on as a **git dependency** on `main`, so this branch
carries no copy of the library.

Every number below comes from an **actual run** captured in
[`results/xlang_measure.log`](results/xlang_measure.log); the runner is
[`scripts/xlang_measure.sh`](scripts/xlang_measure.sh) and each client's source is
under [`clients/`](clients). Re-run it yourself — the point is that you don't have
to trust the table.

## Where it was measured

- **Device:** MacBook Pro 14" (Apple **M1 Pro**), macOS, `aarch64-apple-darwin`.
- **Server:** PostgreSQL 15.14 (Homebrew), loopback TCP, trust auth, one direct
  connection per client.
- **Toolchains (latest at measurement time):** rustc **1.97.0**, Go **1.26.5**,
  Apple clang **17.0.0**.
- **Client libraries:** bsql **1.0.0-alpha.0**, tokio-postgres **0.7.18**, sqlx
  **0.8.6**, diesel-async **0.6.1**, jackc/pgx **v5.10.0**, libpq **14**.
- Quiet system (1-min load ≈ 2.6). Every client runs the identical work: prepare
  a statement once, then a warmed timed loop (2000-iter warm-up, 7 reps, median
  ns/op), reading every column of every row.

## Latency — bsql leads the field, on par with raw C

Microseconds per operation, lower is better. **bold** = the bsql drivers.

| scenario (rows)     | bsql        | bsql (sync) | C / libpq | Go / pgx | tokio-postgres | sqlx     | diesel   |
|---------------------|-------------|-------------|-----------|----------|----------------|----------|----------|
| SELECT by-PK (1)    | **29.0 µs** | **24.6 µs** | 26.6 µs   | 44.5 µs  | 58.1 µs        | 30.2 µs  | 61.7 µs  |
| SELECT 10 rows      | **42.7 µs** | **37.1 µs** | 40.7 µs   | 63.3 µs  | 79.4 µs        | 45.4 µs  | 83.5 µs  |
| SELECT 100 rows     | **54.9 µs** | **49.4 µs** | 57.0 µs   | 80.1 µs  | 81.9 µs        | 72.8 µs  | 90.2 µs  |
| SELECT 1000 rows    | **242 µs**  | **227 µs**  | 261 µs    | 268 µs   | 297 µs         | 302 µs   | 294 µs   |
| INSERT single       | **44.5 µs** | **38.3 µs** | 39.6 µs   | 49.8 µs  | 63.0 µs        | 49.6 µs  | 64.8 µs  |
| JOIN + GROUP BY agg | **3.01 ms** | **2.99 ms** | 3.13 ms   | 3.04 ms  | 3.06 ms        | 3.04 ms  | 3.08 ms  |

Relative to bsql (async) on the flagship **single-row by-PK** read, where driver
overhead dominates (lower = faster): **bsql (sync) 0.85× · C/libpq 0.92× · bsql
1.0× · sqlx 1.04× · Go/pgx 1.53× · tokio-postgres 2.00× · diesel 2.13×**.

- bsql is **on par with hand-written C over libpq** and ahead of every other
  client — the blocking driver even edges out C on point reads (within
  cross-implementation measurement variance; call it a tie with raw C).
- The lead is largest on **small results**, where per-round-trip client overhead
  dominates — where a lean driver should win. On the ~3 ms server-bound
  aggregate everything converges, as expected.
- **tokio-postgres and diesel are ~2× slower on point reads** — tokio-postgres
  runs the connection on a separate task (a scheduler hop per query on a
  current-thread runtime); diesel-async adds ORM layering on top.

## Peak memory — bsql is the leanest client, period

Peak resident memory (`getrusage` `ru_maxrss` — the RAM the process actually used)
over one connection doing 10 000 by-PK SELECTs + 1 000 INSERTs, **one separate
binary per client** so each measures only its own code. Deterministic (does not
move with machine load).

| client             | peak memory (RAM) | × vs bsql |
|--------------------|-------------------|-----------|
| **bsql (sync)**    | **1.61 MB**       | 0.94×     |
| **bsql** (async)   | **1.72 MB**       | 1.0×      |
| tokio-postgres     | 6.20 MB           | 3.6×      |
| sqlx               | 6.42 MB           | 3.7×      |
| diesel             | 6.69 MB           | 3.9×      |
| C / libpq          | 12.64 MB          | **7.4×**  |
| Go / pgx           | 16.03 MB          | **9.3×**  |

bsql is **~3.7× leaner than the Rust field, ~7× leaner than a C/libpq client, and
~9× leaner than Go/pgx.** Beating raw C on memory is not a typo: the standard
libpq the C client links pulls in its full client-side dependency set (TLS, ICU,
buffers), while bsql's arena-based decode holds a fixed ~4 KiB engine buffer and
16-byte row handles. That is per process, so across a fleet of instances or
containers the gap compounds directly. A committed gate (`tests/rss_ceiling.rs`)
fails if bsql's peak exceeds **2 MiB** — the sub-2 MB figure is enforced, not
aspirational.

## What each library can do

Speed is half the story; the other half is what the driver lets you *do*. Honest
comparison — competitors get credit for what they have.

| capability | bsql | tokio-postgres | sqlx | diesel |
|---|---|---|---|---|
| Compile-time SQL check vs real schema | ✅ `query!` replays migrations | ❌ unchecked string | ✅ `query!` | ◐ typed DSL only |
| …with **no live DB / cache** at build | ✅ offline from migration files | — | ❌ needs DB or committed cache | ◐ `schema.rs` usually from a DB |
| Plain SQL text (not a DSL) | ✅ | ✅ (unchecked) | ✅ | ❌ query-builder DSL |
| **N+1 query detection** | ✅ `conn.n1_report()`, zero-cost off | ❌ | ❌ | ❌ |
| Typed/safe **binary COPY** | ✅ `copy_in_typed`, compile-checked | ◐ hand-wired binary | ◐ text COPY only | ❌ no COPY |
| Build-time **migration safety** gate | ✅ destructive-op ack + checksum drift | ❌ | ◐ checksum drift | ◐ up/down by version |
| First-class **sync AND async**, one API | ✅ shared `Core<S>` | ❌ async only | ❌ async only | ❌ sync only |
| **Zero-per-row-alloc** streaming | ✅ `query_each`, O(1) RAM, 0 alloc/row | ◐ streams, allocs/row | ◐ streams, allocs/row | ◐ default materializes |
| Out-of-band query cancellation | ✅ detached `CancelToken` | ✅ `cancel_token()` | ◐ cancel-on-drop | ❌ |
| `#![forbid(unsafe_code)]` (shipped crates) | ✅ every crate | ◐ some `unsafe` | ◐ some `unsafe` | ❌ links libpq (C FFI) |
| Unix-domain socket transport | ✅ (~2.4–2.9× faster than TCP locally) | ✅ | ✅ | ✅ (libpq) |

## Methodology

- **Same everything.** Identical SQL, identical schema (`bench_items` / `bench_cat`
  / `bench_ins`), prepared once and reused (the cache-hit path a real workload
  lives on), every column of every row read so the driver actually decodes.
  Loopback TCP for all (in-kernel, no wire); bsql *also* has a unix-socket
  transport it does not use here, so the field stays even.
- **Latency:** each client's own warmed timed loop (2000-iter warm-up, 7 reps,
  median ns/op) — the same shape in every language so C, Go and Rust are directly
  comparable. Server-bound scenarios (the ~3 ms aggregate) converge because the
  cost is PostgreSQL's, not the client's.
- **Memory:** one separate process per client (so linked-but-cold code from
  another driver can't pollute the figure), `getrusage(ru_maxrss)` after a fixed
  10 000-read + 1 000-write workload.
- **Honesty:** numbers are captured from real runs (`results/xlang_measure.log`),
  never hand-written; a client that can't run a scenario is left out of that row,
  not given an invented value. The C client `PQclear`s every result (no leak
  inflating its RSS); each Rust/diesel client decodes into real typed values.

## Further measurements (designed, being added)

The single-op latency + memory matrix above is the foundation. These deeper
benchmarks are specified and will be measured with the same rigor (real runs,
captured logs) — several probe a capability a competitor structurally lacks:

- **Concurrency throughput** — sustained QPS + p99 under 8 / 32 / 128 workers on a
  fixed pool. A level playing field that rewards low per-op + pool overhead.
- **Constant-memory streaming of 5M rows** — RSS high-water *and* allocations/row.
  bsql's `query_each` holds O(1) RAM with **0 alloc/row**; diesel's default
  `load()` and libpq's `PQexec` are O(rows) and balloon; even the streaming
  competitors pay a per-row allocation bsql does not.
- **Typed binary COPY — 1M rows** — rows/s + MB/s + peak RSS. Binary COPY beats
  text COPY (no server-side parse) and crushes multi-row INSERT — and **diesel has
  no COPY at all**, a structural gap.
- **Transaction round-trip fusion** — RTTs per small transaction (counted with a
  loopback relay) + txns/s, paired with an honest **pipelining control** where
  tokio-postgres's real pipelining is the winner (reported truthfully).
- **Connection establishment cost** and **unix-socket vs loopback TCP**.
- **Detection/observability overhead** — the price of `n1-detect` / the diagnostics
  sink when on vs the proven zero cost when off.
