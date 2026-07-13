#!/bin/sh
# Uniform SEQUENTIAL cross-language measurement — one client at a time, quiet machine.
# Every number printed here comes from an actual run against live PG.
CL=/private/tmp/claude-0/-Users-smir-ant-Code-bsql/b19972ab-7dc5-41e8-b466-b1cc24feb038/scratchpad/clients
WT=/private/tmp/claude-0/-Users-smir-ant-Code-bsql/b19972ab-7dc5-41e8-b466-b1cc24feb038/scratchpad/bench-wt
DL=/opt/homebrew/lib/postgresql@14
PSQL="psql -U smir-ant -d postgres -h 127.0.0.1 -tAq"
ins() { $PSQL -c "TRUNCATE bench_ins;" >/dev/null 2>&1; }

echo "### machine: $(uptime | sed 's/.*load/load/')"
echo "### rustc $(rustc +stable --version 2>/dev/null | awk '{print $2}')  go $(/usr/local/go/bin/go version 2>/dev/null | awk '{print $3}')  clang $(clang --version | head -1 | awk '{print $4}')"

echo "===== LATENCY (ns/op, self-timed 7-rep median) ====="
ins; echo "--- C/libpq ---";     ( cd $CL/c && DYLD_LIBRARY_PATH=$DL ./pg_bench latency )
ins; echo "--- Go/pgx ---";      ( cd $CL/go && ./pg_bench latency )
ins; echo "--- diesel ---";      ( cd $CL/diesel && ./target/release/bench-diesel latency )
for c in bsql bsql_sync tokio_postgres sqlx; do
  ins; echo "--- rust:$c ---"; ( cd $CL/rust && ./target/release/pg_bench $c latency )
done

echo "===== PEAK RSS (separate binary per client) ====="
ins; echo "--- C/libpq ---";     ( cd $CL/c && DYLD_LIBRARY_PATH=$DL ./pg_bench rss )
ins; echo "--- Go/pgx ---";      ( cd $CL/go && ./pg_bench rss )
ins; echo "--- diesel ---";      ( cd $CL/diesel && ./target/release/bench-diesel rss )
ins; echo "--- bsql (async) ---";  $WT/target/release/rss_bsql_async
ins; echo "--- bsql (sync) ---";   $WT/target/release/rss_bsql_sync
ins; echo "--- tokio-postgres ---";$WT/target/release/rss_tokio_postgres
ins; echo "--- sqlx ---";          $WT/target/release/rss_sqlx
echo "===== MEASURE_DONE ====="
