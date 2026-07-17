#!/bin/sh
# DEEP benchmarks — the two probes the single-op latency/RSS matrix cannot show:
#
#   concurrency  Sustained QPS + p50/p99/p999 latency under 8 / 32 / 128
#                concurrent workers, each holding one connection, all running the
#                by-PK read. bsql-async (its Pool) vs tokio-postgres vs sqlx.
#   streaming    Peak RSS (and, for bsql, allocations/row) while consuming a
#                >=1M-row result. bsql's query_each streams in O(1) RAM with
#                ~0 alloc/row; libpq PQexec and tokio-postgres query() materialise
#                the whole result (O(rows)) — the RSS curve makes the gap concrete.
#
# WHY A DEDICATED SERVER: 128 HELD connections exceed a stock PostgreSQL's
# max_connections (100), and a large streaming result loads a server for seconds.
# So this script stands up its OWN ephemeral PostgreSQL (max_connections raised,
# its own port + socket dir, torn down on exit) — isolated, so it neither
# disturbs nor is disturbed by any concurrent use of a shared server, and it
# honours the exact 8/32/128 worker counts out of the box. Same PostgreSQL 15,
# same machine, loopback TCP as the single-op tables; the ephemeral server prints
# its own version so the log records exactly what was measured.
#
# All paths are RELATIVE to this repo (no machine-specific hardcoding). The bsql
# Rust bins resolve their `bsql` git dependency exactly as the other harnesses do.
#
# Usage:
#   scripts/xlang_measure_deep.sh build        # build every deep client
#   scripts/xlang_measure_deep.sh concurrency  # concurrency table (stands up PG)
#   scripts/xlang_measure_deep.sh streaming    # streaming table (stands up PG)
#   scripts/xlang_measure_deep.sh all          # build + concurrency + streaming
# Env (all optional):
#   DEEP_WORKERS       worker counts        (default "8 32 128")
#   DEEP_STREAM_ROWS   streaming row counts (default "1000000 5000000")
#   CONC_WARMUP_MS     per-run warm-up ms   (default 1500)
#   CONC_MEASURE_MS    per-run measure ms   (default 5000)
#   DEEP_PG_PORT       ephemeral PG port    (default 5433)
#   PG_BINDIR          PostgreSQL bin dir   (default: postgresql@15, else pg_config)
#   PG_LIBDIR/PG_INCDIR libpq dirs for the C stream client (default: pg_config)
#   DEEP_PG_EXISTING=1 use the server named by PGHOST/PGPORT/... instead of an
#                      ephemeral one (you must ensure max_connections fits)
#   CC (clang)
set -eu

HERE=$(cd "$(dirname "$0")/.." && pwd)   # bench root
CL="$HERE/clients"
CC="${CC:-clang}"

WORKERS="${DEEP_WORKERS:-8 32 128}"
STREAM_ROWS="${DEEP_STREAM_ROWS:-1000000 5000000}"
export CONC_WARMUP_MS="${CONC_WARMUP_MS:-1500}"
export CONC_MEASURE_MS="${CONC_MEASURE_MS:-5000}"

# libpq location for the C stream client.
PG_LIBDIR="${PG_LIBDIR:-$(pg_config --libdir 2>/dev/null || echo /usr/lib)}"
PG_INCDIR="${PG_INCDIR:-$(pg_config --includedir 2>/dev/null || echo /usr/include)}"

# ── PostgreSQL server binaries (prefer 15 to match the single-op tables) ──
resolve_bindir() {
    if [ -n "${PG_BINDIR:-}" ]; then echo "$PG_BINDIR"; return; fi
    if [ -x /opt/homebrew/opt/postgresql@15/bin/initdb ]; then
        echo /opt/homebrew/opt/postgresql@15/bin; return
    fi
    if [ -x /usr/lib/postgresql/15/bin/initdb ]; then
        echo /usr/lib/postgresql/15/bin; return
    fi
    pg_config --bindir 2>/dev/null || echo /usr/bin
}
PG_BINDIR="$(resolve_bindir)"

EPHEM_PORT="${DEEP_PG_PORT:-5433}"
EPHEM_USER="$(id -un)"
EPHEM_DATA=""   # set by ephem_start; removed by ephem_stop

# ── Ephemeral PostgreSQL lifecycle ─────────────────────────────────────────
ephem_stop() {
    [ -n "$EPHEM_DATA" ] || return 0
    "$PG_BINDIR/pg_ctl" -D "$EPHEM_DATA" -m immediate stop >/dev/null 2>&1 || true
    rm -rf "$EPHEM_DATA" 2>/dev/null || true
    EPHEM_DATA=""
}

ephem_start() {
    if [ "${DEEP_PG_EXISTING:-0}" = "1" ]; then
        echo "### using EXISTING server PGHOST=${PGHOST:-127.0.0.1} PGPORT=${PGPORT:-5432} (DEEP_PG_EXISTING=1)"
        return 0
    fi
    if [ ! -x "$PG_BINDIR/initdb" ]; then
        echo "### SKIP: no initdb at $PG_BINDIR (set PG_BINDIR); cannot stand up the ephemeral server" >&2
        exit 0
    fi
    EPHEM_DATA="$(mktemp -d "${TMPDIR:-/tmp}/bsql_deep_pg.XXXXXX")"
    trap ephem_stop EXIT INT TERM
    echo "### initdb ephemeral cluster at $EPHEM_DATA (user=$EPHEM_USER)"
    "$PG_BINDIR/initdb" -D "$EPHEM_DATA" -U "$EPHEM_USER" -A trust --encoding=UTF8 >/dev/null
    echo "### starting ephemeral PostgreSQL on 127.0.0.1:$EPHEM_PORT (max_connections=300)"
    "$PG_BINDIR/pg_ctl" -D "$EPHEM_DATA" -w -l "$EPHEM_DATA/server.log" \
        -o "-p $EPHEM_PORT -c listen_addresses=127.0.0.1 -c max_connections=300 -c unix_socket_directories=$EPHEM_DATA" \
        start
    # Point every client at the ephemeral server via the standard PG* env.
    export PGHOST=127.0.0.1 PGPORT="$EPHEM_PORT" PGUSER="$EPHEM_USER" PGDATABASE=postgres
    # Seed bench_items (10k) for the concurrency by-PK read; streaming needs no seed.
    "$PG_BINDIR/psql" -h 127.0.0.1 -p "$EPHEM_PORT" -U "$EPHEM_USER" -d postgres -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS bench_items;
CREATE TABLE bench_items (id int4 PRIMARY KEY, name text NOT NULL, val int4 NOT NULL);
INSERT INTO bench_items SELECT g, 'name_' || g, g * 2 FROM generate_series(1, 10000) AS g;
ANALYZE bench_items;
SQL
    echo "### ephemeral server: $("$PG_BINDIR/psql" -h 127.0.0.1 -p "$EPHEM_PORT" -U "$EPHEM_USER" -d postgres -tAq -c 'SHOW server_version')"
}

# ── Build every deep client with the max-perf flags ────────────────────────
build() {
    echo "### building deep clients (max-perf flags) ..."
    ( cd "$HERE" && cargo build --release --quiet \
        --bin concurrency_pg --bin stream_bsql --bin stream_tokio )
    # C/libpq streaming contrast — skip gracefully if the toolchain is absent.
    if command -v "$CC" >/dev/null 2>&1 && [ -f "$CL/c/pg_bench.c" ]; then
        ( cd "$CL/c" && "$CC" -O3 -march=native -flto -std=c11 \
            -I"$PG_INCDIR" -o pg_bench pg_bench.c -L"$PG_LIBDIR" -lpq ) \
            && echo "### C stream client built" \
            || echo "SKIP stream_libpq C build failed (see cc output)"
    else
        echo "SKIP stream_libpq no C compiler / pg_bench.c"
    fi
    echo "### build done"
}

meta() {
    echo "### machine: $(uptime | sed 's/.*load/load/')"
    echo "### rustc $(rustc --version 2>/dev/null | awk '{print $2}')  cc $($CC --version 2>/dev/null | head -1)"
    echo "### threads(available_parallelism) is printed per CONC line"
}

# ── Concurrency: QPS + p99 under N workers, each client ─────────────────────
concurrency() {
    echo "===== CONCURRENCY (QPS + p50/p99/p999, hold-one-connection-per-worker) ====="
    echo "### warmup=${CONC_WARMUP_MS}ms measure=${CONC_MEASURE_MS}ms  workers: $WORKERS"
    for w in $WORKERS; do
        for c in bsql tokio_postgres sqlx; do
            echo "--- $c workers=$w ---"
            "$HERE/target/release/concurrency_pg" "$c" "$w"
        done
    done
}

# ── Streaming: peak RSS (+ bsql alloc/row) at each row count ─────────────────
streaming() {
    echo "===== STREAMING (peak RSS, one process per client per row-count) ====="
    echo "### rows: $STREAM_ROWS"
    for n in $STREAM_ROWS; do
        echo "--- bsql (query_each, O(1)) rows=$n ---"
        "$HERE/target/release/stream_bsql" "$n"
        echo "--- tokio-postgres (query, O(rows)) rows=$n ---"
        "$HERE/target/release/stream_tokio" "$n"
        if [ -x "$CL/c/pg_bench" ]; then
            echo "--- libpq (PQexec, O(rows)) rows=$n ---"
            ( cd "$CL/c" && DYLD_LIBRARY_PATH="$PG_LIBDIR" LD_LIBRARY_PATH="$PG_LIBDIR" \
                ./pg_bench stream_rss "$n" )
        else
            echo "SKIP stream_libpq rows=$n (C client not built; run 'build' with a C toolchain)"
        fi
    done
    echo "===== MEASURE_DONE ====="
}

case "${1:-all}" in
    build)       build ;;
    concurrency) meta; ephem_start; concurrency ;;
    streaming)   meta; ephem_start; streaming ;;
    all)         build; meta; ephem_start; concurrency; streaming ;;
    *) echo "usage: $0 {build|concurrency|streaming|all}" >&2; exit 2 ;;
esac
