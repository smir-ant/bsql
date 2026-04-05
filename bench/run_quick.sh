#!/bin/bash
# Quick C vs bsql benchmark — ~2 minutes total.
#
# Two warm-up passes ensure PG cache is fully hot.
# JOIN uses 1000 iterations (not 3000) for speed.
#
# Usage:
#   BENCH_DATABASE_URL="host=/tmp dbname=bench_db" \
#   BSQL_DATABASE_URL="postgres://user:pass@localhost/bench_db?host=/tmp" \
#   ./run_quick.sh

set -e

DB=${BENCH_DATABASE_URL:?"BENCH_DATABASE_URL must be set"}
BSQL=${BSQL_DATABASE_URL:?"BSQL_DATABASE_URL must be set"}

cleanup() {
    psql -h /tmp bench_db -c "DELETE FROM bench_users WHERE id > 10000; CHECKPOINT;" -q 2>/dev/null
}

psql -h /tmp bench_db -f setup/pg_setup.sql -q 2>/dev/null
psql -h /tmp bench_db -c "GRANT ALL ON ALL TABLES IN SCHEMA public TO bsql; GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO bsql;" -q 2>/dev/null

echo "=== C (libpq) ==="
BENCH_DATABASE_URL="$DB" ./c/pg_bench > /dev/null 2>&1; cleanup
BENCH_DATABASE_URL="$DB" ./c/pg_bench > /dev/null 2>&1; cleanup
BENCH_DATABASE_URL="$DB" ./c/pg_bench
cleanup

echo ""
echo "=== bsql (Rust) ==="
BENCH_DATABASE_URL="$BSQL" BSQL_DATABASE_URL="$BSQL" ./target/release/bench_bsql_perf > /dev/null 2>&1; cleanup
BENCH_DATABASE_URL="$BSQL" BSQL_DATABASE_URL="$BSQL" ./target/release/bench_bsql_perf > /dev/null 2>&1; cleanup
BENCH_DATABASE_URL="$BSQL" BSQL_DATABASE_URL="$BSQL" ./target/release/bench_bsql_perf
cleanup
