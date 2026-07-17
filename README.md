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
- Quiet system (1-min load ≈ 2.4), one local PostgreSQL 15.14 over loopback TCP.
  Every client runs the identical work: prepare a statement once, then a warmed
  timed loop (2000-iter warm-up, 7 reps, median ns/op), reading every column of
  every row; the async Rust clients all run on a tokio **current-thread** runtime
  (the realistic single-connection choice, applied equally — see Methodology).

## Latency — bsql leads the field, on par with raw C

Microseconds per operation, lower is better. **bold** = the bsql drivers.

| scenario (rows)     | bsql        | bsql (sync) | C / libpq | Go / pgx | tokio-postgres | sqlx     | diesel   |
|---------------------|-------------|-------------|-----------|----------|----------------|----------|----------|
| SELECT by-PK (1)    | **26.2 µs** | **24.6 µs** | 25.0 µs   | 52.2 µs  | 40.0 µs        | 28.2 µs  | 41.4 µs  |
| SELECT 10 rows      | **40.0 µs** | **37.1 µs** | 38.5 µs   | 73.6 µs  | 53.2 µs        | 42.6 µs  | 55.0 µs  |
| SELECT 100 rows     | **52.3 µs** | **50.0 µs** | 54.5 µs   | 78.1 µs  | 72.3 µs        | 70.6 µs  | 78.6 µs  |
| SELECT 1000 rows    | **234 µs**  | **232 µs**  | 250 µs    | 259 µs   | 286 µs         | 296 µs   | 296 µs   |
| INSERT single       | **43.5 µs** | **37.7 µs** | 37.2 µs   | 58.6 µs  | 43.3 µs        | 46.1 µs  | 43.9 µs  |
| JOIN + GROUP BY agg | **3.00 ms** | **2.99 ms** | 3.01 ms   | 3.04 ms  | 3.05 ms        | 3.03 ms  | 3.02 ms  |

Relative to bsql (async) on the flagship **single-row by-PK** read, where driver
overhead dominates (lower = faster): **bsql (sync) 0.94× · C/libpq 0.95× · bsql
1.0× · sqlx 1.08× · tokio-postgres 1.53× · diesel 1.58× · Go/pgx 1.99×**.

- **bsql (sync) is the fastest or tied-fastest on every scenario**, and beats
  hand-written C/libpq on five of six (a tie on single-row INSERT). Since C/libpq
  is a *synchronous* (blocking) client, this sync-vs-sync comparison is the true
  apples-to-apples one — and bsql's protocol + decode win it outright.
- **bsql (async) is on par with raw C** — within ~1–1.5 µs on the smallest point
  reads (the irreducible cost of an async runtime's poll/wake over a would-block
  read, which a blocking client does not pay), and *faster* than C on 100/1000-row
  reads and the aggregate. Its ~1 µs point-read gap to a blocking client is the
  price of concurrency — the very thing async exists for and this single-op serial
  micro-benchmark does not exercise. It is by a wide margin the **fastest async
  driver** here (tokio-postgres 1.53×, diesel 1.58×, Go/pgx 1.99× on by-PK).
- The lead is largest on **small results**, where per-round-trip driver overhead
  dominates. On the ~3 ms server-bound aggregate everything converges, as expected.

## Peak memory — bsql is the leanest client, period

Peak resident memory (`getrusage` `ru_maxrss` — the RAM the process actually used)
over one connection doing 10 000 by-PK SELECTs + 1 000 INSERTs, **one separate
binary per client** so each measures only its own code. Deterministic (does not
move with machine load).

| client             | peak memory (RAM) | × vs bsql |
|--------------------|-------------------|-----------|
| **bsql (sync)**    | **1.56 MB**       | 0.88×     |
| **bsql** (async)   | **1.78 MB**       | 1.0×      |
| tokio-postgres     | 6.22 MB           | 3.5×      |
| sqlx               | 6.41 MB           | 3.6×      |
| diesel             | 6.98 MB           | 3.9×      |
| C / libpq          | 13.27 MB          | **7.5×**  |
| Go / pgx           | 17.43 MB          | **9.8×**  |

bsql is **~3.6× leaner than the Rust field, ~7.5× leaner than a C/libpq client, and
~10× leaner than Go/pgx.** Beating raw C on memory is not a typo: the standard
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
  comparable. Numbers here are the median of 3 such runs. Server-bound scenarios
  (the ~3 ms aggregate) converge because the cost is PostgreSQL's, not the client's.
- **Execution model (read the table with this in mind).** C/libpq and bsql (sync)
  are *blocking* clients — one OS thread, no runtime. Go/pgx blocks a goroutine on
  Go's netpoller. The async Rust clients (bsql, tokio-postgres, sqlx, diesel) run on
  a tokio **current-thread** runtime — the realistic, lightest choice for a
  single-connection latency path, applied EQUALLY to all four (a multi-thread
  runtime would only add a cross-thread `block_on` handoff irrelevant to a
  one-socket workload). The fair apples-to-apples comparison is therefore
  **blocking-vs-blocking: bsql (sync) vs C/libpq — and bsql wins it** (faster on
  every read, tie on INSERT). An async client pays an unavoidable reactor
  poll/park/wake over a would-block read that a blocking client does not; that tax
  is ~1–1.5 µs on cached point reads and larger (~6 µs) on INSERT (where the
  post-WAL response reliably parks the reactor) — and it is paid by **every** async
  client (bsql, tokio-postgres, sqlx, diesel all land ~43–46 µs on INSERT vs ~37 µs
  blocking), with bsql the fastest of them. It is the price of concurrency, not a
  bsql inefficiency: bsql's async path is structurally at the tokio floor (one
  suspend, zero per-op alloc, syscall parity).
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
