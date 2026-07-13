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
PostgreSQL 15.14, loopback TCP, quiet system. Numbers are criterion medians.
`bsql` is the async driver (the primary mode); `bsql (sync)` is the blocking one
(no tokio at all — a touch faster still).

### Latency — bsql wins every scenario

Lower is better. Every client does IDENTICAL work: bind a pre-prepared statement,
receive every row, read every column.

| scenario (rows)     | bsql         | bsql (sync)  | tokio-postgres | sqlx     |
|---------------------|--------------|--------------|----------------|----------|
| SELECT by-PK (1)    | **26.1 µs**  | **25.0 µs**  | 39.8 µs        | 29.4 µs  |
| SELECT 10 rows      | **44.2 µs**  | **40.0 µs**  | 59.4 µs        | 45.8 µs  |
| SELECT 100 rows     | **54.3 µs**  | **53.2 µs**  | 72.3 µs        | 75.1 µs  |
| SELECT 1000 rows    | **240.9 µs** | **240.3 µs** | 296.6 µs       | 330.0 µs |
| INSERT single       | **40.5 µs**  | **38.2 µs**  | 44.8 µs        | 44.4 µs  |
| JOIN + GROUP BY agg | **1.010 ms** | **1.003 ms** | 1.053 ms       | 1.039 ms |

- On the flagship **single-row-by-PK** latency, bsql is **1.13× faster than
  sqlx** and **1.52× faster than tokio-postgres**.
- The advantage is largest on **small results**, where per-round-trip client
  overhead dominates — exactly where a lean driver should win. On large/complex
  queries the PostgreSQL server cost dominates and the four converge.
- **tokio-postgres is consistently slowest on small queries** — it runs the
  connection on a *separate task*, so on a current-thread runtime each query hops
  through the scheduler and back, latency bsql's inline-pump design does not pay.

### Peak memory (RAM) — bsql is ~3.7× smaller than the field

Peak resident memory — the actual RAM the process used, via `getrusage`
`ru_maxrss` — over one connection doing 10 000 SELECT-by-PK + 1 000 INSERT. Unlike
latency, this is deterministic: it reflects touched pages, not scheduling, so it
does not move with machine load.

| client          | peak memory (RAM) |
|-----------------|-------------------|
| **bsql**        | **1.73 MB**       |
| **bsql (sync)** | **1.62 MB**       |
| tokio-postgres  | 6.25 MB           |
| sqlx            | 6.39 MB           |

That is **~3.6–3.9× leaner** — and it is per process, so across a fleet of service
instances or containers (each its own process) the gap compounds directly. A
committed gate (`tests/rss_ceiling.rs`) fails if bsql's peak exceeds **2 MiB**
(2.25 MiB for the async driver), so the sub-2 MB figure is enforced, not aspirational.

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
