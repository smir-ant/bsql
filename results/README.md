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
| [`deep_measure.log`](deep_measure.log) | `scripts/xlang_measure_deep.sh all` | **Deeper benchmarks** — the `CONC <client> workers=… qps=… p99_us=…` (concurrency) and `STREAM <client> rows=… rss_bytes=… alloc_per_row=…` (streaming) lines |

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
- **Deep** cells come from `deep_measure.log`: a `CONC` line's `qps=` (÷1000 → the
  `k` QPS column) and `p99_us=` (the p99 µs table); a `STREAM` line's `rss_bytes` ÷ 10⁶
  → the peak-RSS MB, and bsql's `stream_allocs` / `alloc_per_row` back the streaming note.

To regenerate all three from scratch, follow the README's
[Reproduce it yourself](../README.md#reproduce-it-yourself).
