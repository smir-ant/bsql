#!/bin/sh
# Uniform SEQUENTIAL cross-language SQLite measurement — one client at a time,
# quiet machine. The SQLite peer of scripts/xlang_measure.sh (which does PG).
#
# Every client runs the IDENTICAL work over the SAME seeded bench.db (bsql's own
# `parity_sqlite` PLUS the four competitors under clients/{c,go,rust,diesel}-sqlite):
# prepare once, then a warmed timed loop (2000-warmup, 7-rep MEDIAN ns/op),
# reading every column of every row. Output per client: `VERSION`,
# `LAT <scenario> <ns>`, `SKIP <scenario> <reason>`, and (rss mode)
# `RSS <bytes>` / `PEAK_RSS <mb>`.
#
# The DB is seeded ONCE and shared (so every client hits BYTE-IDENTICAL data —
# the seed uses random() for amounts, so reseeding would change the data; do NOT
# reseed between clients). Each client self-cleans its inserted rows, and this
# script also cleans between clients (belt & suspenders), so every client's
# insert scenarios start from the clean 10k-user baseline.
#
# Usage:
#   scripts/xlang_measure_sqlite.sh build      # build all clients + parity_sqlite
#   scripts/xlang_measure_sqlite.sh latency    # latency table (seeds if absent)
#   scripts/xlang_measure_sqlite.sh rss        # peak-RSS table
#   scripts/xlang_measure_sqlite.sh all        # build + latency + rss (default)
# Env:
#   BENCH_SQLITE_PATH   db file (default <bench-root>/bench.db)
#   RESEED=1            force a fresh reseed (randomizes amounts — breaks
#                       cross-client identity if done mid-sweep)
set -eu

HERE=$(cd "$(dirname "$0")/.." && pwd)   # bench root
CL="$HERE/clients"
DB="${BENCH_SQLITE_PATH:-$HERE/bench.db}"
GO="${GO:-/usr/local/go/bin/go}"

seed() {
    rm -f "$DB" "$DB-wal" "$DB-shm"
    sqlite3 "$DB" < "$HERE/setup/sqlite_setup.sql"
    echo "### seeded $DB (10k users / 100k orders)"
}
ensure_seed() {
    if [ "${RESEED:-0}" = "1" ] || [ ! -f "$DB" ]; then seed; fi
}
clean() {
    sqlite3 "$DB" "DELETE FROM bench_users WHERE name='bench_insert' OR name LIKE 'batch_%';"
}

do_build() {
    echo "### building clients"
    ( cd "$CL/c-sqlite" && sh build.sh )
    ( cd "$CL/go-sqlite" && CGO_ENABLED=1 "$GO" build -o sqlite_bench . && echo "built go-sqlite" )
    ( cd "$CL/rust-sqlite" && RUSTFLAGS="-C target-cpu=native" cargo build --release -q && echo "built rust-sqlite (sqlx)" )
    ( cd "$CL/diesel-sqlite" && RUSTFLAGS="-C target-cpu=native" cargo build --release -q && echo "built diesel-sqlite" )
    # bsql's own reference runner (needs the git dep on `main`; built from the bench root).
    ( cd "$HERE" && cargo build --release -q --bin parity_sqlite && echo "built parity_sqlite (bsql)" ) \
        || echo "WARN: parity_sqlite build failed (needs the bsql git dep) — bsql column will be missing"
}

C_BIN="$CL/c-sqlite/sqlite_bench"
GO_BIN="$CL/go-sqlite/sqlite_bench"
SQLX_BIN="$CL/rust-sqlite/target/release/sqlx_sqlite_bench"
DIESEL_BIN="$CL/diesel-sqlite/target/release/diesel_sqlite_bench"
BSQL_BIN="$HERE/target/release/parity_sqlite"

run_mode() {  # $1 = latency|rss
    mode="$1"
    echo "--- bsql (parity_sqlite) ---"
    if [ -x "$BSQL_BIN" ]; then
        # parity_sqlite has only a latency matrix (KV format); no rss mode.
        if [ "$mode" = "latency" ]; then BENCH_SQLITE_PATH="$DB" "$BSQL_BIN"; clean
        else echo "SKIP bsql-sqlite-rss parity_sqlite_has_no_rss_mode(add_a_bsql_rss_binary_like_the_PG_side)"; fi
    else
        echo "SKIP bsql parity_sqlite_not_built(run: scripts/xlang_measure_sqlite.sh build)"
    fi
    echo "--- C / sqlite3 (bundled 3.50.2) ---"; BENCH_SQLITE_PATH="$DB" "$C_BIN" "$mode"; clean
    echo "--- Go / mattn ---";                   BENCH_SQLITE_PATH="$DB" "$GO_BIN" "$mode"; clean
    echo "--- Rust / sqlx ---";                  BENCH_SQLITE_PATH="$DB" "$SQLX_BIN" "$mode"; clean
    echo "--- Rust / diesel ---";                BENCH_SQLITE_PATH="$DB" "$DIESEL_BIN" "$mode"; clean
}

MODE="${1:-all}"

echo "### machine: $(uptime | sed 's/.*load/load/')"
echo "### rustc $(rustc --version 2>/dev/null | awk '{print $2}')  go $($GO version 2>/dev/null | awk '{print $3}')  clang $(clang --version | head -1 | awk '{print $4}')  system-sqlite3 $(sqlite3 --version | awk '{print $1}')"

case "$MODE" in
    build)   do_build ;;
    latency) ensure_seed; echo "===== SQLite LATENCY (ns/op, 7-rep median) ====="; run_mode latency ;;
    rss)     ensure_seed; echo "===== SQLite PEAK RSS (separate process per client) ====="; run_mode rss ;;
    all)
        do_build
        ensure_seed
        echo "===== SQLite LATENCY (ns/op, 7-rep median) ====="; run_mode latency
        echo "===== SQLite PEAK RSS (separate process per client) ====="; run_mode rss
        echo "===== MEASURE_DONE =====" ;;
    *) echo "usage: $0 build|latency|rss|all" >&2; exit 2 ;;
esac
