#!/bin/bash
# Memory benchmark -- measures peak RSS for each library
#
# ALL binaries do IDENTICAL work:
#   connect → 10K SELECT by PK → 1K INSERT → exit
#
# Usage: BENCH_DATABASE_URL="host=/tmp dbname=bench_db" \
#        BSQL_DATABASE_URL="postgres://user:pass@localhost/bench_db?host=/tmp" \
#        ./mem/run_all.sh

set -e

echo "=== Building ==="
# Rust
cargo build --release --bin mem_bsql_pg --bin mem_sqlx_pg --bin mem_diesel_pg 2>/dev/null
# C
cc -O3 -o mem/mem_c_pg mem/c_pg.c -I$(pg_config --includedir) -L$(pg_config --libdir) -lpq
# Go
(cd go && go build -o ../mem/mem_go_pg ./mem/)

echo ""
echo "=== Peak RSS ==="
echo "All binaries: connect → 10K SELECT → 1K INSERT → exit"
echo ""

# Clean up between runs
psql -h /tmp ${PGDATABASE:-bench_db} -c "DELETE FROM bench_users WHERE name LIKE 'memtest_%'" -q 2>/dev/null || true

echo "bsql:"
/usr/bin/time -l ./target/release/mem_bsql_pg 2>&1 | grep "maximum resident"

psql -h /tmp ${PGDATABASE:-bench_db} -c "DELETE FROM bench_users WHERE name LIKE 'memtest_%'" -q 2>/dev/null || true

echo ""
echo "C (libpq):"
/usr/bin/time -l ./mem/mem_c_pg 2>&1 | grep "maximum resident"

psql -h /tmp ${PGDATABASE:-bench_db} -c "DELETE FROM bench_users WHERE name LIKE 'memtest_%'" -q 2>/dev/null || true

echo ""
echo "sqlx:"
/usr/bin/time -l ./target/release/mem_sqlx_pg 2>&1 | grep "maximum resident"

psql -h /tmp ${PGDATABASE:-bench_db} -c "DELETE FROM bench_users WHERE name LIKE 'memtest_%'" -q 2>/dev/null || true

echo ""
echo "diesel:"
/usr/bin/time -l ./target/release/mem_diesel_pg 2>&1 | grep "maximum resident"

psql -h /tmp ${PGDATABASE:-bench_db} -c "DELETE FROM bench_users WHERE name LIKE 'memtest_%'" -q 2>/dev/null || true

echo ""
echo "Go (pgx):"
/usr/bin/time -l ./mem/mem_go_pg 2>&1 | grep "maximum resident"

psql -h /tmp ${PGDATABASE:-bench_db} -c "DELETE FROM bench_users WHERE name LIKE 'memtest_%'" -q 2>/dev/null || true
