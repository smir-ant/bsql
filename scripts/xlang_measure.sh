#!/bin/sh
# Uniform SEQUENTIAL cross-language PostgreSQL measurement — one client at a
# time, quiet machine. The PG peer of scripts/xlang_measure_sqlite.sh.
#
# Every client runs the IDENTICAL work against the SAME live PostgreSQL (seeded
# once by setup/pg_setup.sql): prepare once, then a warmed timed loop (2000-warmup,
# 7-rep MEDIAN ns/op), reading every column of every row. Output per client:
# `VERSION`, `LAT <scenario> <ns>` (latency mode) and `RSS <bytes>` /
# `PEAK_RSS_BYTES <bytes>` (rss mode).
#
# Paths are all RELATIVE to this repo (no machine-specific hardcoding). The bsql
# Rust binaries are a `git` dependency on `main`, so a fresh checkout resolves the
# current library API — see the repo root Cargo.toml.
#
# Usage:
#   scripts/xlang_measure.sh build      # build all 7 clients + the bsql RSS bins
#   scripts/xlang_measure.sh latency    # latency table
#   scripts/xlang_measure.sh rss        # peak-RSS table
#   scripts/xlang_measure.sh all        # build + latency + rss (default)
# Env (all optional — sensible defaults):
#   PGHOST (127.0.0.1)  PGUSER ($USER)  PGDATABASE (postgres)  PGPORT (5432)
#   PG_LIBDIR   libpq lib dir for the C client (default: `pg_config --libdir`)
#   PG_INCDIR   libpq include dir       (default: `pg_config --includedir`)
#   CC (clang)  GO (go)
set -eu

HERE=$(cd "$(dirname "$0")/.." && pwd)   # bench root
CL="$HERE/clients"
CC="${CC:-clang}"
GO="${GO:-go}"
PGHOST="${PGHOST:-127.0.0.1}"
PGUSER="${PGUSER:-$USER}"
PGDATABASE="${PGDATABASE:-postgres}"
PGPORT="${PGPORT:-5432}"
export PGHOST PGUSER PGDATABASE PGPORT
PSQL="psql -h $PGHOST -U $PGUSER -d $PGDATABASE -p $PGPORT -tAq"

# libpq location for the C client (pg_config is the portable source of truth).
PG_LIBDIR="${PG_LIBDIR:-$(pg_config --libdir 2>/dev/null || echo /usr/lib)}"
PG_INCDIR="${PG_INCDIR:-$(pg_config --includedir 2>/dev/null || echo /usr/include)}"

ins() { $PSQL -c "TRUNCATE bench_ins;" >/dev/null 2>&1 || true; }

build() {
    echo "### building all clients (max-perf flags) ..."
    # C/libpq — the same -O3 -march=native -flto every client gets.
    ( cd "$CL/c" && "$CC" -O3 -march=native -flto -std=c11 \
        -I"$PG_INCDIR" -o pg_bench pg_bench.c -L"$PG_LIBDIR" -lpq )
    # Go/pgx — default optimized build (arm64 has no -march knob).
    ( cd "$CL/go" && "$GO" build -o pg_bench . )
    # Rust competitors + bsql clients (LTO fat / codegen-units=1 / target-cpu=native
    # are set in each client's Cargo.toml profile + .cargo/config or build.rs).
    ( cd "$CL/rust"   && cargo build --release --quiet )
    ( cd "$CL/diesel" && cargo build --release --quiet )
    # bsql + competitor peak-RSS harnesses live in the bench root (git-dep bsql).
    ( cd "$HERE" && cargo build --release --quiet \
        --bin rss_bsql_async --bin rss_bsql_sync \
        --bin rss_tokio_postgres --bin rss_sqlx )
    echo "### build done"
}

meta() {
    echo "### machine: $(uptime | sed 's/.*load/load/')"
    echo "### rustc $(rustc --version 2>/dev/null | awk '{print $2}')  go $($GO version 2>/dev/null | awk '{print $3}')  cc $($CC --version 2>/dev/null | head -1)"
}

latency() {
    echo "===== LATENCY (ns/op, self-timed 7-rep median) ====="
    ins; echo "--- C/libpq ---";        ( cd "$CL/c" && DYLD_LIBRARY_PATH="$PG_LIBDIR" LD_LIBRARY_PATH="$PG_LIBDIR" ./pg_bench latency )
    ins; echo "--- Go/pgx ---";         ( cd "$CL/go" && ./pg_bench latency )
    ins; echo "--- diesel ---";         ( cd "$CL/diesel" && ./target/release/bench-diesel latency )
    for c in bsql bsql_sync tokio_postgres sqlx; do
        ins; echo "--- rust:$c ---";    ( cd "$CL/rust" && ./target/release/pg_bench "$c" latency )
    done
}

rss() {
    echo "===== PEAK RSS (separate process per client) ====="
    ins; echo "--- C/libpq ---";        ( cd "$CL/c" && DYLD_LIBRARY_PATH="$PG_LIBDIR" LD_LIBRARY_PATH="$PG_LIBDIR" ./pg_bench rss )
    ins; echo "--- Go/pgx ---";         ( cd "$CL/go" && ./pg_bench rss )
    ins; echo "--- diesel ---";         ( cd "$CL/diesel" && ./target/release/bench-diesel rss )
    ins; echo "--- bsql (async) ---";   "$HERE/target/release/rss_bsql_async"
    ins; echo "--- bsql (sync) ---";    "$HERE/target/release/rss_bsql_sync"
    ins; echo "--- tokio-postgres ---"; "$HERE/target/release/rss_tokio_postgres"
    ins; echo "--- sqlx ---";           "$HERE/target/release/rss_sqlx"
    echo "===== MEASURE_DONE ====="
}

case "${1:-all}" in
    build)   build ;;
    latency) meta; latency ;;
    rss)     rss ;;
    all)     build; meta; latency; rss ;;
    *) echo "usage: $0 {build|latency|rss|all}" >&2; exit 2 ;;
esac
