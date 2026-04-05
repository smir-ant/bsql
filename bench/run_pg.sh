#!/bin/bash
# Fair PostgreSQL performance benchmark — EQUAL conditions for ALL 5 runners.
#
# Methodology:
# 1. Reset bench_db (DROP + recreate + autovacuum off + ANALYZE + CHECKPOINT)
# 2. Warm up PG cache — ALL 5 runners execute a full pass (not measured)
# 3. CHECKPOINT after warm-up
# 4. Measure each on identical hot-cache state, CHECKPOINT between INSERT runs
#
# All 5 runners use the same approach: N iterations, total time, mean per-op.
# No Criterion, no adaptive sampling. Direct comparison.
#
# Usage:
#   BENCH_DATABASE_URL="host=/tmp dbname=bench_db" \
#   BSQL_DATABASE_URL="postgres://user:pass@localhost/bench_db?host=/tmp" \
#   ./run_pg.sh

set -e

DB=${BENCH_DATABASE_URL:?"BENCH_DATABASE_URL must be set"}
BSQL=${BSQL_DATABASE_URL:?"BSQL_DATABASE_URL must be set"}

cleanup() {
    psql -h /tmp bench_db -c "DELETE FROM bench_users WHERE id > 10000; CHECKPOINT;" -q 2>/dev/null
}

echo "=== 1. Reset database ==="
psql -h /tmp bench_db -f setup/pg_setup.sql -q 2>/dev/null
psql -h /tmp bench_db -c "GRANT ALL ON ALL TABLES IN SCHEMA public TO bsql; GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO bsql;" -q 2>/dev/null

echo "=== 2. Warm up PG cache (all 5 runners) ==="
echo "  C..."
BENCH_DATABASE_URL="$DB" ./c/pg_bench > /dev/null 2>&1; cleanup
echo "  Go..."
(cd go && BENCH_DATABASE_URL="$DB" go run ./pg/) > /dev/null 2>&1; cleanup
echo "  bsql..."
BENCH_DATABASE_URL="$BSQL" BSQL_DATABASE_URL="$BSQL" ./target/release/bench_bsql_perf > /dev/null 2>&1; cleanup
echo "  diesel..."
BENCH_DATABASE_URL="$BSQL" ./target/release/bench_diesel_perf > /dev/null 2>&1; cleanup
echo "  sqlx..."
BENCH_DATABASE_URL="$BSQL" ./target/release/bench_sqlx_perf > /dev/null 2>&1; cleanup
echo ""
echo "PG cache hot. CHECKPOINT done. Starting measurements."
echo ""

echo "=== C (libpq) ==="
BENCH_DATABASE_URL="$DB" ./c/pg_bench
cleanup

echo ""
echo "=== Go (pgx) ==="
(cd go && BENCH_DATABASE_URL="$DB" go run ./pg/)
cleanup

echo ""
echo "=== bsql (Rust) ==="
BENCH_DATABASE_URL="$BSQL" BSQL_DATABASE_URL="$BSQL" ./target/release/bench_bsql_perf
cleanup

echo ""
echo "=== diesel (Rust) ==="
BENCH_DATABASE_URL="$BSQL" ./target/release/bench_diesel_perf
cleanup

echo ""
echo "=== sqlx (Rust) ==="
BENCH_DATABASE_URL="$BSQL" ./target/release/bench_sqlx_perf
cleanup
