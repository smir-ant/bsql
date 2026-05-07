#!/usr/bin/env bash
# Run a bench (or arbitrary command) under `/usr/bin/time -p` and
# emit a wall-clock-vs-CPU-time confidence indicator.
#
# Companion to `bench-stable.sh` (criterion ns/op statistical
# layer): bench-stable measures *what the wall clock saw*, but
# wall-clock includes time when the OS preempted the bench
# process. CPU-time measurement separates "we got CPU for X ns"
# from "X ns of wall-clock elapsed".
#
# # The signal we extract
#
# - **real**  — wall-clock seconds (what bench-stable saw)
# - **user**  — seconds the process spent on-CPU in user mode
# - **sys**   — seconds the process spent on-CPU in kernel mode
# - **ratio** — (user + sys) / real
#
# Single-threaded bench expectations:
#   ratio ≥ 0.95   — quiet machine, bench-stable numbers reliable
#   0.80 ≤ ratio   — minor scheduler interference; numbers usable
#                    but with elevated noise
#   ratio < 0.80   — heavy interference; rerun on quieter machine
#                    or accept ±5%+ noise band
#
# Multi-threaded bench (criterion runs single-threaded by default,
# but if a future bench uses a thread pool):
#   ratio can exceed 1.0 — that's expected (multi-core sum)
#
# # Why /usr/bin/time -p, not Rust syscalls
#
# `/usr/bin/time -p` is POSIX (IEEE 1003.1) — same output format
# on macOS BSD `time(1)` and GNU `time(1)`. Three lines:
# `real <s>`, `user <s>`, `sys <s>`. Stable parser, no Rust dep.
# Internally `time` calls `wait4(2)` / `getrusage(2)` — same
# primitive a Rust wrapper would use, with no portability layer
# to maintain.
#
# # Usage
#
#   scripts/bench-cpu-time.sh -- <cmd> [args...]
#       Run <cmd> under /usr/bin/time -p, print real/user/sys/ratio.
#
#   scripts/bench-cpu-time.sh stable-wrap <baseline-name> [filter]
#       Run `bench-stable.sh save <baseline-name>` under CPU-time
#       wrapper; helpful for one-call "save baseline + measure
#       the meta-confidence" workflow.
#
#   scripts/bench-cpu-time.sh check
#       Quick sanity: run `sleep 0.5` under the wrapper. Confirms
#       /usr/bin/time -p is parseable on this host.
#
# # Examples
#
#   # Wrap an existing cargo bench run.
#   scripts/bench-cpu-time.sh -- cargo bench -p bsql-pg-proto --bench hot_paths
#
#   # Wrap bench-stable.sh save in one call.
#   scripts/bench-cpu-time.sh stable-wrap before-md5
#
#   # Sanity check.
#   scripts/bench-cpu-time.sh check

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$WORKSPACE"

usage() {
    cat >&2 <<'EOF'
Usage:
  scripts/bench-cpu-time.sh -- <cmd> [args...]
      Wrap <cmd> with /usr/bin/time -p; print real/user/sys/ratio.

  scripts/bench-cpu-time.sh stable-wrap <baseline-name> [filter]
      Wrap `scripts/bench-stable.sh save <name> [filter]`.

  scripts/bench-cpu-time.sh check
      Sanity check: run /usr/bin/time -p sleep 0.5.

Examples:
  scripts/bench-cpu-time.sh -- cargo bench -p bsql-pg-proto --bench hot_paths
  scripts/bench-cpu-time.sh stable-wrap before-md5
EOF
    exit 1
}

CMD="${1:-}"
[[ -z "$CMD" ]] && usage

# Verify /usr/bin/time exists. macOS ships it at /usr/bin/time
# (BSD time, supports -p). Linux ships GNU time at /usr/bin/time
# in most distros (also supports -p). Shell builtin `time` does
# NOT support -p uniformly — we explicitly invoke the binary.
if ! [[ -x /usr/bin/time ]]; then
    echo "[bench-cpu-time] FATAL: /usr/bin/time not found at expected path" >&2
    echo "  macOS: should be present by default (BSD time)" >&2
    echo "  Linux: install via 'apt-get install time' / 'dnf install time'" >&2
    exit 2
fi

# Capture /usr/bin/time -p output (which goes to stderr) into a
# temp file, while letting stdout pass through to the user. This
# preserves whatever the wrapped command printed — only the time
# summary lines are redirected for parsing.
#
# /usr/bin/time -p emits exactly three lines on stderr at end:
#   real <s>
#   user <s>
#   sys  <s>
# The wrapped command's own stderr is also written to the same
# stderr stream — we tee both, then grep the trailing 3 lines.

run_with_time() {
    # Args: the wrapped command + its args.
    local TIME_LOG
    TIME_LOG="$(mktemp -t bsql-cpu-time-XXXXXX.log)"
    # Trap-rm only the time-log; the wrapped command may produce
    # files we don't own — never touch those.
    trap "rm -f '$TIME_LOG'" RETURN

    echo "[bench-cpu-time] wrapping: $*" >&2
    echo "[bench-cpu-time] stderr being captured for time-stats parsing" >&2
    echo "" >&2

    # Run with /usr/bin/time. Stdout passes through verbatim.
    # Stderr from BOTH the wrapped command AND /usr/bin/time go
    # to TIME_LOG; we tee back to console for live progress.
    local RC=0
    if /usr/bin/time -p "$@" 2> >(tee "$TIME_LOG" >&2); then
        RC=0
    else
        RC=$?
    fi

    # Extract the trailing three time lines. /usr/bin/time prints
    # them last on stderr; tail safely picks them.
    local REAL USER SYS
    REAL="$(grep '^real ' "$TIME_LOG" | tail -1 | awk '{print $2}')"
    USER="$(grep '^user ' "$TIME_LOG" | tail -1 | awk '{print $2}')"
    SYS="$(grep '^sys '  "$TIME_LOG" | tail -1 | awk '{print $2}')"

    if [[ -z "$REAL" || -z "$USER" || -z "$SYS" ]]; then
        echo "" >&2
        echo "[bench-cpu-time] WARN: could not parse /usr/bin/time -p output" >&2
        echo "[bench-cpu-time] raw stderr at $TIME_LOG (preserved for inspection)" >&2
        # Don't exit — the wrapped command's own RC is the
        # primary signal. We just lose the meta-metric.
        trap - RETURN
        return "$RC"
    fi

    # Compute ratio = (user + sys) / real using awk for floats.
    # Awk's printf rounds to 3 decimals which is plenty.
    local RATIO
    RATIO="$(awk -v u="$USER" -v s="$SYS" -v r="$REAL" \
        'BEGIN { if (r > 0) printf "%.3f", (u + s) / r; else printf "n/a" }')"

    # Verdict line based on ratio. We classify the result for
    # downstream automation: "OK" / "WARN" / "FAIL" prefix.
    local VERDICT
    if [[ "$RATIO" == "n/a" ]]; then
        VERDICT="UNKNOWN (real=0)"
    else
        # Use awk for floating-point comparison (bash arithmetic
        # is integer-only).
        local TIER
        TIER="$(awk -v r="$RATIO" 'BEGIN {
            if (r >= 0.95) print "OK";
            else if (r >= 0.80) print "WARN";
            else print "FAIL";
        }')"
        case "$TIER" in
            OK)   VERDICT="OK (machine quiet, bench numbers reliable)" ;;
            WARN) VERDICT="WARN (scheduler interference; bench numbers ±elevated noise)" ;;
            FAIL) VERDICT="FAIL (heavy interference; rerun on quieter machine)" ;;
        esac
    fi

    echo "" >&2
    echo "============================================================" >&2
    echo "CPU-time stats for wrapped command" >&2
    echo "============================================================" >&2
    echo "  real (wall-clock):   $REAL s" >&2
    echo "  user (on-CPU):       $USER s" >&2
    echo "  sys  (on-CPU kern):  $SYS s" >&2
    echo "  ratio (cpu / wall):  $RATIO" >&2
    echo "  verdict:             $VERDICT" >&2
    echo "" >&2

    # Emit a machine-parseable summary line for downstream tools.
    echo "CPU_TIME_BENCH real=$REAL user=$USER sys=$SYS ratio=$RATIO verdict=${VERDICT%% *}" >&2

    trap - RETURN
    rm -f "$TIME_LOG"
    return "$RC"
}

case "$CMD" in
    --)
        # Shift away the `--` and pass the remaining args verbatim.
        shift
        if [[ "$#" -eq 0 ]]; then
            echo "[bench-cpu-time] '--': no command given after separator" >&2
            usage
        fi
        run_with_time "$@"
        ;;
    stable-wrap)
        BASELINE="${2:-}"
        FILTER="${3:-}"
        if [[ -z "$BASELINE" ]]; then
            echo "[bench-cpu-time] stable-wrap: <baseline-name> required" >&2
            usage
        fi
        if [[ -z "$FILTER" ]]; then
            run_with_time "$SCRIPT_DIR/bench-stable.sh" save "$BASELINE"
        else
            run_with_time "$SCRIPT_DIR/bench-stable.sh" save "$BASELINE" "$FILTER"
        fi
        ;;
    check)
        # CPU-bound check (NOT sleep — sleep uses no CPU and would
        # produce ratio=0, falsely indicating "FAIL" on a quiet
        # machine). A short busy-spin in awk gives ratio≈1.0 on
        # any single-core scheduler, confirming the wrapper +
        # parser path correctly attributes user-time.
        echo "[bench-cpu-time] sanity check: ~0.5s of CPU-bound awk" >&2
        run_with_time awk 'BEGIN { for (i = 0; i < 50000000; i++) { x = i * 2 } }'
        echo "[bench-cpu-time] sanity check OK (expect ratio close to 1.0)" >&2
        ;;
    *)
        echo "[bench-cpu-time] unknown command: $CMD" >&2
        usage
        ;;
esac
