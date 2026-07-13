# bsql — benchmark harness

This branch is the **benchmark project** for [bsql](https://github.com/smir-ant/bsql)
(the library itself lives on [`main`](https://github.com/smir-ant/bsql/tree/main)).
It measures bsql against the Rust competitors — `tokio-postgres` and `sqlx` — over
the same server, transport, and SQL, plus a peak-RSS harness that compares the
resident footprint of a real workload. It depends on `bsql` as a **git dependency**
on `main`, so this branch carries no copy of the library.

```bash
psql -h 127.0.0.1 -U smir-ant -d postgres -f setup/pg_setup.sql   # seed once
cargo bench                                          # criterion latency sweep
cargo run --release --bin rss_bsql_sync              # one RSS harness
cargo test --release --test rss_ceiling -- --ignored # the RSS regression gate

scripts/bench-e2e.sh all      # orchestrated: quiet-system gate + seed + RSS + latency
```

Requires a local PostgreSQL on `127.0.0.1:5432`, user `smir-ant`, db `postgres`,
trust auth. `scripts/` also holds the codegen/asm measurement tools
(`asm-diff.sh`, `bench-stable.sh`, `asm-linked-diff.sh`, `bench-cpu-time.sh`); see
[`BENCHMARKING.md`](BENCHMARKING.md) for the ns/codegen methodology.

## The measured standing

Machine: aarch64-apple-darwin (Apple Silicon), rustc 1.96.0, release + LTO,
PostgreSQL 15.14, loopback TCP. Numbers are criterion medians.

> **Re-verified @ the shipped tip** (both audit-8 gaps closed). Two fresh full
> runs. **RSS re-measured and confirmed:** `bsql_sync` byte-identical at
> 1,687,552 B (1.69 MB), `bsql_async` 1.79 MB, the field 6.5–6.7 MB — **~3.9×
> smaller** (RSS is deterministic, load-independent). **Latency:** bsql was
> fastest in every scenario in both runs; the hot decode/dispatch frame is
> byte-identical to the run these medians were taken on (proven by the
> `engine_hotpath_codegen` gate), so the medians below stand. The fresh runs ran
> under elevated background load (~5.0 vs the ~2.9 these tables were measured at),
> so their *absolute* µs sit ~10–15 % higher while the *ranking* is unchanged.

### Latency — bsql wins every scenario

Lower is better. Every client does IDENTICAL work: bind a pre-prepared statement,
receive every row, read every column.

| scenario (rows)      | bsql_sync    | bsql_async | tokio-postgres | sqlx     | fastest    |
|----------------------|--------------|------------|----------------|----------|------------|
| SELECT by-PK (1)     | **24.9 µs**  | 27.3 µs    | 44.6 µs        | 28.5 µs  | bsql_sync  |
| SELECT 10 rows       | **38.5 µs**  | 41.9 µs    | 60.5 µs        | 44.0 µs  | bsql_sync  |
| SELECT 100 rows      | **54.2 µs**  | 54.9 µs    | 81.1 µs        | 75.4 µs  | bsql_sync  |
| SELECT 1000 rows     | 281 µs       | **264 µs** | 289 µs         | 322 µs   | bsql_async |
| INSERT single        | 35.7 µs      | **35.2 µs**| 44.7 µs        | 41.2 µs  | bsql_async |
| JOIN + GROUP BY agg  | **1.036 ms** | 1.087 ms   | 1.090 ms       | 1.064 ms | bsql_sync  |

- On the flagship **single-row-by-PK** latency, bsql (sync) is **1.14× faster
  than sqlx** and **1.79× faster than tokio-postgres**.
- The advantage is largest on **small results**, where per-round-trip client
  overhead dominates — exactly where a lean driver should win. On large/complex
  queries the PostgreSQL server cost dominates and all four converge.
- **tokio-postgres is consistently slowest on small queries** — it runs the
  connection on a *separate task*, so on a current-thread runtime each query hops
  through the scheduler and back, latency bsql's inline-pump design does not pay.

**Determinism:** across two full runs the *absolute* µs shift ±6–20 % with
background load, but the *ranking is stable* — bsql was fastest in every scenario
both times. RSS, by contrast, is deterministic to ±1 page regardless of load.

### Peak RSS — bsql is ~3.9× smaller than the field

One direct connection, 10 000 SELECT-by-PK + 1 000 INSERT, `getrusage`
`ru_maxrss` (process-lifetime peak). Lower is better.

| client          | peak RSS    | vs bsql_sync |
|-----------------|-------------|--------------|
| **bsql (sync)** | **1.69 MB** | 1.0×         |
| bsql (async)    | 1.79 MB     | 1.1×         |
| tokio-postgres  | 6.5 MB      | ~3.9×        |
| sqlx            | 6.7 MB      | ~4.0×        |

RSS is deterministic to ±1 page — it reflects touched pages, not scheduling, so it
does not move with machine load. A committed gate (`tests/rss_ceiling.rs`) fails if
the blocking driver's peak RSS exceeds **2 MiB** (async: 2.25 MiB).

## Methodology & noise control

- **Same transport for all** — loopback TCP (in-kernel, no wire, no switch). bsql
  *also* has a unix-domain-socket transport (measured ~2.4–2.9× faster than
  loopback TCP on the by-PK round trip — see the `unix_vs_tcp` bench), but the
  competitor comparison uses TCP so every client is on equal footing.
- **Prepared, cache-HIT** — statements prepared ONCE before the timed loop, the
  path a real workload spends its life on.
- **Read every column of every row** — forces the driver to actually decode.
- **One direct connection per client** — no pool; the number is the driver's
  per-round-trip cost.
- **`setup/pg_setup.sql`** disables autovacuum on the bench tables and CHECKPOINTs
  before measuring; seeding runs OUTSIDE the measured processes.
- **Quiet-system gate** — `scripts/bench-e2e.sh` refuses to measure at 1-minute
  load > 4.0 (criterion's sample windows amplify noise under load).
- **criterion** reports a 95 % CI per number; RSS is the max of three fresh
  processes.

## Honest caveats

- **C-libpq and Go-pgx are a cross-language follow-up.** They need a C toolchain /
  a Go build and a separate-process harness. The RSS cross-check (tokio-postgres
  ≈ a C-libpq-class figure) suggests the harness is calibrated; wiring the
  cross-language clients is the next step.
- **tokio-postgres was measured on a current-thread runtime** (same as all
  clients). Its separate-task design is happier on a multi-thread runtime — a
  fairer *best-case* for it, at higher RSS.
- **RSS is macOS here.** A Linux re-measurement (where `ru_maxrss` page accounting
  differs) would make an exact cross-OS comparison.
