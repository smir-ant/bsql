#!/usr/bin/env bash
# Orchestrate the standalone `bench/` project (end-to-end latency + peak RSS)
# under the same noise-control discipline the in-process benches use.
#
# Companion to `bench-stable.sh` (which drives the proto crate's `hot_paths`
# criterion bench) and `bench-cpu-time.sh` (the generic /usr/bin/time -p CPU-vs-
# wall wrapper). This one covers the `bench/` targets those two do not:
#
#   - the criterion e2e latency sweep (bsql async + sync vs tokio-postgres, sqlx)
#   - the four peak-RSS harness binaries
#
# It applies the SAME quiet-system gate as `bench-stable.sh` (measurements are
# invalid under load), seeds PostgreSQL from `bench/setup/pg_setup.sql`
# (autovacuum-off + CHECKPOINT), and lowers process priority. `bench/` is its
# own cargo workspace, so all cargo commands run from inside it.
#
# # Usage
#
#   scripts/bench-e2e.sh setup            # (re)seed PostgreSQL only
#   scripts/bench-e2e.sh rss              # peak-RSS comparison, all four clients
#   scripts/bench-e2e.sh latency [filter] # criterion e2e sweep (optional filter)
#   scripts/bench-e2e.sh all              # setup + rss + latency
#
# Env:
#   BSQL_BENCH_FORCE=1   bypass the quiet-system gate (NOT recommended)
#   PGHOST/PGUSER/PGDATABASE   override the psql target (defaults match the repo)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCH="$REPO/bench"

PGHOST="${PGHOST:-127.0.0.1}"
PGUSER="${PGUSER:-smir-ant}"
PGDATABASE="${PGDATABASE:-postgres}"

usage() {
    sed -n '2,26p' "${BASH_SOURCE[0]}" >&2
    exit 1
}

# Same quiet-system gate as bench-stable.sh: a 1-minute load_avg over 4.0 makes
# criterion's sample windows noise-amplifying. Fail fast rather than produce
# statistically invalid numbers. The load-parse handles macOS/Linux/BSD `uptime`
# and the European decimal comma.
check_quiet_system() {
    local load_avg
    load_avg="$(uptime | awk -F'load averages?:' '{print $2}' | awk '{gsub(/,/, "."); print $1}')"
    if [[ -n "$load_avg" ]] && awk -v la="$load_avg" 'BEGIN { exit !(la+0 > 4.0) }'; then
        echo "ABORT (quiet-system gate): 1-minute load_avg=${load_avg} > 4.0." >&2
        echo "Close background work and retry, or BSQL_BENCH_FORCE=1 to override." >&2
        if [[ "${BSQL_BENCH_FORCE:-}" != "1" ]]; then
            exit 1
        fi
        echo "WARN: BSQL_BENCH_FORCE=1 — continuing despite load_avg=${load_avg}" >&2
    fi
}

# Lower priority so the scheduler treats the bench as background work.
LOWER_PRIORITY=()
if [[ "$(uname)" == "Darwin" ]] && command -v taskpolicy >/dev/null 2>&1; then
    LOWER_PRIORITY=(taskpolicy -c utility)
elif command -v nice >/dev/null 2>&1; then
    LOWER_PRIORITY=(nice -n 10)
fi

do_setup() {
    echo "[bench-e2e] seeding PostgreSQL (${PGUSER}@${PGHOST}/${PGDATABASE})..." >&2
    psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -q -f "$BENCH/setup/pg_setup.sql"
    echo "[bench-e2e] setup complete." >&2
}

do_rss() {
    echo "[bench-e2e] building RSS harnesses (release)..." >&2
    ( cd "$BENCH" && cargo build --release --bins )
    echo "" >&2
    printf '%-22s %14s %10s\n' "client" "peak_rss_bytes" "MiB" >&2
    printf '%-22s %14s %10s\n' "----------------------" "--------------" "----------" >&2
    for b in rss_bsql_sync rss_bsql_async rss_tokio_postgres rss_sqlx; do
        local bytes mib
        bytes="$("$BENCH/target/release/$b" 2>/dev/null | awk '/PEAK_RSS_BYTES/{print $2}')"
        if [[ -z "$bytes" ]]; then
            printf '%-22s %14s\n' "$b" "FAILED (PG?)" >&2
            continue
        fi
        mib="$(awk -v x="$bytes" 'BEGIN{printf "%.2f", x/1048576}')"
        printf '%-22s %14s %10s\n' "$b" "$bytes" "$mib"
    done
}

do_latency() {
    local filter="${1:-}"
    echo "[bench-e2e] running criterion e2e sweep under ${LOWER_PRIORITY[*]:-no-wrapper}..." >&2
    if [[ -n "$filter" ]]; then
        ( cd "$BENCH" && "${LOWER_PRIORITY[@]}" cargo bench --bench e2e -- "$filter" )
    else
        ( cd "$BENCH" && "${LOWER_PRIORITY[@]}" cargo bench --bench e2e )
    fi
}

CMD="${1:-}"
case "$CMD" in
    setup) do_setup ;;
    rss) check_quiet_system; do_setup; do_rss ;;
    latency) check_quiet_system; do_setup; do_latency "${2:-}" ;;
    all) check_quiet_system; do_setup; do_rss; do_latency ;;
    *) usage ;;
esac
