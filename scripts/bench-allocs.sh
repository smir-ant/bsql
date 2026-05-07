#!/usr/bin/env bash
# Run the deterministic allocation-traffic bench and emit a
# pass/fail comparison vs a saved baseline.
#
# Companion to `bench-stable.sh` (criterion ns/op statistical
# layer). Whereas bench-stable answers "is the runtime delta
# beyond noise?", bench-allocs answers "did the alloc count
# change AT ALL?" — deterministic by construction.
#
# # Determinism
#
# `cargo bench --bench alloc_counts` invokes a tiny
# `fn main()` that runs each scenario exactly once with a
# `#[global_allocator]` wrapper counting `alloc` / `dealloc`
# calls. Same source + same scenario → exactly the same numbers,
# every run, every machine (modulo platform allocator quirks
# that we don't depend on — we forward to `System`).
#
# # Usage
#
#   scripts/bench-allocs.sh save <baseline-name>
#       Run alloc_counts, save the output as a named baseline.
#
#   scripts/bench-allocs.sh compare <baseline-name>
#       Run alloc_counts, line-by-line diff vs saved baseline.
#       Exit 1 on any difference (any scenario alloc count
#       changing is signal — not noise — for this layer).
#
#   scripts/bench-allocs.sh list
#       List saved baselines.
#
# # Output classification (compare mode)
#
# - `unchanged`  — same alloc / dealloc / bytes for the scenario
# - `regression` — alloc count went UP (new alloc surface)
# - `improvement`— alloc count went DOWN (alloc removed)
# - `appeared`   — scenario in current run, not in baseline
# - `disappeared`— scenario in baseline, not in current run
#
# Any non-`unchanged` outcome is a hard fail (exit 1) requiring
# explicit acknowledgement before re-baselining.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$WORKSPACE"

# Where we store saved baselines. Sibling to criterion's
# `target/criterion` for grep-discoverability; lives outside
# `target/criterion` so `cargo clean` doesn't eat them
# (mirrors bench-stable.sh's --save-baseline behaviour).
BASELINE_DIR="target/alloc_baselines"

# Warn loudly if working tree is dirty when SAVING a baseline.
# Mirrors `bench-stable.sh::warn_if_dirty_for_save` — the same
# race (parallel save + edit picks up edits in cargo bench's
# rebuild) applies here too.
warn_if_dirty_for_save() {
    local mode="$1"
    [[ "$mode" != "save" ]] && return 0
    if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        return 0
    fi
    local head_short
    head_short="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
    local dirty=0
    if ! git diff --quiet 2>/dev/null || ! git diff --cached --quiet 2>/dev/null; then
        dirty=1
    fi
    if [[ "$dirty" -eq 1 ]]; then
        echo "" >&2
        echo "============================================================" >&2
        echo "[bench-allocs] ⚠  WARNING: dirty working tree on save" >&2
        echo "============================================================" >&2
        echo "  HEAD:   $head_short" >&2
        echo "  STATE:  working tree differs from HEAD" >&2
        echo "" >&2
        echo "  Baseline will reflect your CURRENT working tree, NOT HEAD." >&2
        echo "  cargo bench rebuilds before run; parallel edits leak in." >&2
        echo "" >&2
        echo "  Recommended if you want HEAD baseline:" >&2
        echo "    Ctrl+C now → git stash → re-run → git stash pop" >&2
        echo "" >&2
        echo "  Continuing in 5 seconds (Ctrl+C to abort)..." >&2
        echo "============================================================" >&2
        sleep 5
    else
        echo "[bench-allocs] working tree CLEAN at $head_short — \
baseline will reflect HEAD" >&2
    fi
}

usage() {
    cat >&2 <<'EOF'
Usage:
  scripts/bench-allocs.sh save <baseline-name>
      Run alloc_counts bench, save output as a named baseline.

  scripts/bench-allocs.sh compare <baseline-name>
      Run alloc_counts, diff vs saved baseline. Exit 1 on any change.

  scripts/bench-allocs.sh list
      List saved baselines.

Examples:
  scripts/bench-allocs.sh save before-md5-refactor
  # ... edit code ...
  scripts/bench-allocs.sh compare before-md5-refactor
  # → prints diff lines + summary; exit 0 if all unchanged, 1 otherwise
EOF
    exit 1
}

CMD="${1:-}"
[[ -z "$CMD" ]] && usage

# Run the bench, capture only ALLOC_BENCH lines into a stable
# canonical form: one line per scenario, `name=<n> allocs=<a>
# deallocs=<d> bytes=<b>`. Order from the bench harness is
# stable (see alloc_counts.rs `fn main`).
run_alloc_bench() {
    cargo bench -p bsql-pg-proto --bench alloc_counts 2>&1 \
        | grep '^ALLOC_BENCH ' \
        | sed 's/^ALLOC_BENCH //'
}

ensure_baseline_dir() {
    mkdir -p "$BASELINE_DIR"
}

baseline_path() {
    local name="$1"
    echo "$BASELINE_DIR/$name.txt"
}

case "$CMD" in
    save)
        NAME="${2:-}"
        if [[ -z "$NAME" ]]; then
            echo "[bench-allocs] save: <baseline-name> required" >&2
            usage
        fi
        warn_if_dirty_for_save save
        ensure_baseline_dir
        BPATH="$(baseline_path "$NAME")"
        echo "[bench-allocs] running cargo bench --bench alloc_counts" >&2
        if ! run_alloc_bench > "$BPATH.tmp"; then
            rm -f "$BPATH.tmp"
            echo "[bench-allocs] FAIL: bench did not run to completion" >&2
            exit 2
        fi
        # Sanity: at least one ALLOC_BENCH line must be present.
        if [[ ! -s "$BPATH.tmp" ]]; then
            rm -f "$BPATH.tmp"
            echo "[bench-allocs] FAIL: no ALLOC_BENCH lines captured (build issue?)" >&2
            exit 2
        fi
        mv "$BPATH.tmp" "$BPATH"
        echo "[bench-allocs] baseline saved as '$NAME' ($BPATH)" >&2
        echo "" >&2
        echo "Captured scenarios:" >&2
        cat "$BPATH" >&2
        echo "" >&2
        echo "[bench-allocs] compare later: scripts/bench-allocs.sh compare $NAME" >&2
        ;;
    compare)
        NAME="${2:-}"
        if [[ -z "$NAME" ]]; then
            echo "[bench-allocs] compare: <baseline-name> required" >&2
            usage
        fi
        BPATH="$(baseline_path "$NAME")"
        if [[ ! -f "$BPATH" ]]; then
            echo "[bench-allocs] baseline '$NAME' not found at $BPATH" >&2
            echo "[bench-allocs] available:" >&2
            if [[ -d "$BASELINE_DIR" ]]; then
                find "$BASELINE_DIR" -maxdepth 1 -name '*.txt' -printf '  %f\n' 2>/dev/null \
                    | sed 's/\.txt$//' >&2 || true
            fi
            exit 2
        fi
        echo "[bench-allocs] running cargo bench --bench alloc_counts" >&2
        CURRENT_TMP="$(mktemp -t bsql-alloc-current-XXXXXX.txt)"
        # NOTE: do NOT trap-rm on EXIT — we want to keep the file
        # available for inspection if exit is non-zero. Clean up
        # only on success path.
        if ! run_alloc_bench > "$CURRENT_TMP"; then
            echo "[bench-allocs] FAIL: bench did not run to completion" >&2
            echo "[bench-allocs] partial output preserved at $CURRENT_TMP" >&2
            exit 2
        fi

        echo "" >&2
        echo "============================================================" >&2
        echo "Alloc-bench comparison vs baseline '$NAME'" >&2
        echo "============================================================" >&2
        echo "  baseline: $BPATH" >&2
        echo "  current:  $CURRENT_TMP" >&2
        echo "" >&2

        # Build associative arrays keyed by scenario name from each
        # file. Bash assoc-arrays require declare -A; we tolerate
        # bash 3 by falling back to grep-based per-line lookup.
        # Approach used here: per-name extraction via grep, robust
        # on bash 3.2 (macOS ships 3.2 by default).
        UNCHANGED=0
        REGRESSIONS=0
        IMPROVEMENTS=0
        APPEARED=0
        DISAPPEARED=0
        STATUS=0

        # All names from baseline + current, deduplicated.
        ALL_NAMES="$(sed -n 's/^name=\([^ ]*\).*/\1/p' "$BPATH" "$CURRENT_TMP" \
            | sort -u)"

        while IFS= read -r SC_NAME; do
            [[ -z "$SC_NAME" ]] && continue
            BASE_LINE="$(grep "^name=${SC_NAME} " "$BPATH" || true)"
            CURR_LINE="$(grep "^name=${SC_NAME} " "$CURRENT_TMP" || true)"

            if [[ -z "$BASE_LINE" ]]; then
                APPEARED=$((APPEARED + 1))
                STATUS=1
                echo "  APPEARED   ${SC_NAME}: ${CURR_LINE}" >&2
                continue
            fi
            if [[ -z "$CURR_LINE" ]]; then
                DISAPPEARED=$((DISAPPEARED + 1))
                STATUS=1
                echo "  DISAPPEARED ${SC_NAME}: ${BASE_LINE}" >&2
                continue
            fi
            if [[ "$BASE_LINE" == "$CURR_LINE" ]]; then
                UNCHANGED=$((UNCHANGED + 1))
                continue
            fi

            # Line differs — extract allocs to classify direction.
            BASE_A="$(echo "$BASE_LINE" | sed -n 's/.*allocs=\([0-9]*\).*/\1/p')"
            CURR_A="$(echo "$CURR_LINE" | sed -n 's/.*allocs=\([0-9]*\).*/\1/p')"

            if [[ "${CURR_A:-0}" -gt "${BASE_A:-0}" ]]; then
                REGRESSIONS=$((REGRESSIONS + 1))
                STATUS=1
                echo "  REGRESSION ${SC_NAME}:" >&2
                echo "    baseline: ${BASE_LINE}" >&2
                echo "    current : ${CURR_LINE}" >&2
            elif [[ "${CURR_A:-0}" -lt "${BASE_A:-0}" ]]; then
                IMPROVEMENTS=$((IMPROVEMENTS + 1))
                # Improvement is also a baseline-mismatch — caller
                # should re-baseline if the win is intentional, so
                # we still exit 1 to force explicit acknowledgement.
                STATUS=1
                echo "  IMPROVEMENT ${SC_NAME}:" >&2
                echo "    baseline: ${BASE_LINE}" >&2
                echo "    current : ${CURR_LINE}" >&2
            else
                # allocs equal but bytes / deallocs differ.
                REGRESSIONS=$((REGRESSIONS + 1))
                STATUS=1
                echo "  CHANGED    ${SC_NAME}: (allocs equal, bytes/deallocs differ)" >&2
                echo "    baseline: ${BASE_LINE}" >&2
                echo "    current : ${CURR_LINE}" >&2
            fi
        done <<< "$ALL_NAMES"

        echo "" >&2
        echo "Summary:" >&2
        echo "  unchanged:    $UNCHANGED" >&2
        echo "  regressions:  $REGRESSIONS" >&2
        echo "  improvements: $IMPROVEMENTS" >&2
        echo "  appeared:     $APPEARED" >&2
        echo "  disappeared:  $DISAPPEARED" >&2
        echo "" >&2

        if [[ "$STATUS" -eq 0 ]]; then
            echo "[bench-allocs] PASS: alloc traffic identical to baseline" >&2
            rm -f "$CURRENT_TMP"
            exit 0
        else
            echo "[bench-allocs] FAIL: alloc traffic diverged from baseline" >&2
            echo "[bench-allocs] current run preserved at $CURRENT_TMP for inspection" >&2
            echo "[bench-allocs] re-baseline (if change intentional):" >&2
            echo "    scripts/bench-allocs.sh save $NAME" >&2
            exit 1
        fi
        ;;
    list)
        if [[ ! -d "$BASELINE_DIR" ]]; then
            echo "[bench-allocs] no baselines saved ($BASELINE_DIR is empty)" >&2
            exit 0
        fi
        echo "Saved alloc baselines:" >&2
        find "$BASELINE_DIR" -maxdepth 1 -name '*.txt' -printf '  %f\n' 2>/dev/null \
            | sed 's/\.txt$//' | sort >&2 || true
        ;;
    *)
        echo "[bench-allocs] unknown command: $CMD" >&2
        usage
        ;;
esac
