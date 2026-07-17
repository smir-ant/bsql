# Raw measurement logs

The tables in the top-level [`README.md`](../README.md) are built **directly** from
these committed logs — nothing in the README is hand-written. Each log is the verbatim
stdout of the corresponding `scripts/*.sh` run on the machine described in the README's
"Where it was measured".

| log | produced by | backs which table |
|---|---|---|
| [`pg_latency.log`](pg_latency.log) | `scripts/xlang_measure.sh latency` (3 consecutive passes) | **PostgreSQL — latency** (the table shows the median of the 3 passes) |
| [`pg_rss.log`](pg_rss.log) | `scripts/xlang_measure.sh` (one full pass, latency + RSS) | **PostgreSQL — peak memory** (its `RSS`/`PEAK_RSS_BYTES` bytes ÷ 10⁶ = the MB column) |
| [`sqlite.log`](sqlite.log) | `scripts/xlang_measure_sqlite.sh all` (3 passes + one RSS block) | **SQLite — latency** (median of 3) and **SQLite — peak memory** |
| `concurrency.log` *(generated on run)* | `scripts/xlang_measure_deep.sh concurrency` | **Concurrency throughput** — the `CONC <client> workers=… qps=… p99_us=…` lines |
| `streaming.log` *(generated on run)* | `scripts/xlang_measure_deep.sh streaming` | **Constant-memory streaming** — the `STREAM <client> rows=… rss_bytes=… alloc_per_row=…` lines |

> **The two deep logs do not exist yet — they are produced on a quiet-machine run.**
> The deep-benchmark *harness* is committed and runnable (`scripts/xlang_measure_deep.sh`
> + the `concurrency_pg` / `stream_bsql` / `stream_tokio` clients and the C `stream_rss`
> mode), but their numbers are deliberately deferred to a run under no other load — a
> concurrency/RSS benchmark taken under contention is meaningless. On that run, capture
> stdout to `results/concurrency.log` / `results/streaming.log` (e.g.
> `scripts/xlang_measure_deep.sh concurrency | tee results/concurrency.log`) and fill the
> **Deeper benchmarks** tables in the top-level README from the `CONC` / `STREAM` lines.

## Re-deriving a table cell

- **Latency** cells are the median ns/op across the passes for that `client op`
  triple, ÷ 1000 for the µs the table shows. E.g. `pg_latency.log` has three
  `bsql_sync by_pk` lines (24622, 24596, 24574) → median 24596 ns → **24.6 µs**.
- **PostgreSQL RSS** cells are the log's raw byte count ÷ 10⁶ (decimal MB). E.g.
  `pg_rss.log` `bsql (sync)` `PEAK_RSS_BYTES 1687552` → **1.69 MB**. (The library's
  committed `rss_ceiling` gate additionally asserts this stays under 2 **MiB**.)
- **SKIP** lines in `sqlite.log` mark a scenario a competitor cannot express as the
  bsql variant (it points at the equivalent apples-to-apples cell) — never an
  invented number.

To regenerate all three from scratch, follow the README's
[Reproduce it yourself](../README.md#reproduce-it-yourself).
