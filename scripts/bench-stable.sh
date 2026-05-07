#!/usr/bin/env bash
# Run criterion benchmarks under stability-improving conditions
# and emit a pass/fail comparison vs a saved baseline.
#
# Foundation tool for tier-elevation perf-validation per
# `reforge.md` measurement-methodology section. Use this AFTER
# `asm-diff.sh` confirms codegen change — bench is the
# statistical follow-up to "did this change matter at runtime".
#
# # Stability mechanisms
#
# 1. `taskpolicy -c utility` (macOS only) lowers the QoS class so
#    the scheduler treats the bench process as background work.
#    Reduces preemption by foreground apps; not a guarantee under
#    high system load (use a quiet machine for best results).
# 2. `--measurement-time 30 --warm-up-time 10` extends criterion's
#    sample collection beyond the 5s/3s defaults. More samples →
#    tighter confidence intervals → smaller observable deltas.
# 3. `--noise-threshold 0.05` tells criterion to suppress reports
#    of changes < 5% (those would be measurement noise on most
#    consumer hardware).
# 4. Baseline persistence via `--save-baseline` / `--baseline` lets
#    runs be compared across commits even after target/ is wiped.
#
# # Usage
#
#   scripts/bench-stable.sh save <baseline-name> [bench-filter]
#       Run benches, save as named baseline.
#       `bench-filter` is a criterion regex (defaults to "" = all).
#
#   scripts/bench-stable.sh compare <baseline-name> [bench-filter]
#       Run benches, compare against saved baseline. Reports
#       changes > 5%; exit 1 if any benchmark regresses > 3%.
#
#   scripts/bench-stable.sh list
#       List saved baselines.
#
# # Examples
#
#   scripts/bench-stable.sh save before-md5-refactor
#   # ... edit code ...
#   scripts/bench-stable.sh compare before-md5-refactor
#   # → reports +/- changes; non-zero exit on regression > 3%

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$WORKSPACE"

usage() {
    cat >&2 <<'EOF'
Usage:
  scripts/bench-stable.sh save <baseline-name> [bench-filter]
      Run benches under controlled conditions, save as named baseline.
      `bench-filter` is a criterion regex (defaults to "" = all).

  scripts/bench-stable.sh compare <baseline-name> [bench-filter]
      Run benches, compare against saved baseline. Reports changes
      > 5% (criterion's noise threshold). Exit 1 on any regression.

  scripts/bench-stable.sh list
      List saved baselines.

Examples:
  scripts/bench-stable.sh save before-md5-refactor
  # ... edit code ...
  scripts/bench-stable.sh compare before-md5-refactor
EOF
    exit 1
}

CMD="${1:-}"
[[ -z "$CMD" ]] && usage

# Detect macOS for taskpolicy. On Linux the equivalent is `nice` or
# `chrt`; we use `nice -n 19` as the cross-platform fallback.
LOWER_PRIORITY=()
if [[ "$(uname)" == "Darwin" ]]; then
    if command -v taskpolicy >/dev/null 2>&1; then
        LOWER_PRIORITY=(taskpolicy -c utility)
    fi
elif command -v nice >/dev/null 2>&1; then
    LOWER_PRIORITY=(nice -n 19)
fi

# Criterion stability flags. `--measurement-time 30 --warm-up-time 10`
# extends from criterion's 5s/3s defaults; `--noise-threshold 0.05`
# is the band below which criterion treats a change as noise.
STABILITY_FLAGS=(
    --measurement-time 30
    --warm-up-time 10
    --noise-threshold 0.05
)

# Where criterion stores baselines.
BASELINE_DIR="target/criterion"

# Warn loudly if the working tree differs from HEAD when SAVING a
# baseline. The race we're protecting against:
#
#   user: save baseline (cargo bench takes ~3 min)
#   user: edits code in parallel
#   cargo bench: rebuilds — picks up the in-progress edits
#   result: "baseline" reflects edited code, not HEAD
#
# This bit users (the principal architect, 2026-05-07 DEF-207
# session) and the recovery requires manual `git stash` + delete
# polluted baseline + re-save + `git stash pop`.
#
# Best defence is loud warning + clear statement of WHAT was
# saved, so user sees the problem immediately rather than hours
# later when comparing.
warn_if_dirty_for_save() {
    # Only warn for save mode — compare mode legitimately wants
    # to measure working-tree changes against a saved reference.
    local mode="$1"
    [[ "$mode" != "save" ]] && return 0

    if ! git -C "$WORKSPACE" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        return 0  # not a git repo — skip check
    fi
    local head_short
    head_short="$(git -C "$WORKSPACE" rev-parse --short HEAD 2>/dev/null || echo unknown)"

    # `git status --porcelain` returns one line per file that
    # differs from HEAD: modified-tracked, staged, AND untracked.
    # We include untracked because an untracked .rs file in src/
    # is picked up by cargo bench's compile (verified empirically
    # 2026-05-07). `git diff --quiet` alone misses that case.
    local dirty=0
    local porcelain
    porcelain="$(git -C "$WORKSPACE" status --porcelain 2>/dev/null || echo "")"
    if [[ -n "$porcelain" ]]; then
        dirty=1
    fi

    if [[ "$dirty" -eq 1 ]]; then
        echo "" >&2
        echo "============================================================" >&2
        echo "[bench-stable] ⚠  WARNING: dirty working tree on save" >&2
        echo "============================================================" >&2
        echo "  HEAD:   $head_short" >&2
        echo "  STATE:  working tree differs from HEAD" >&2
        echo "" >&2
        echo "  The baseline being saved will reflect your CURRENT" >&2
        echo "  working tree (HEAD + uncommitted changes), NOT HEAD." >&2
        echo "  cargo bench rebuilds the crate before each run; if you" >&2
        echo "  edit code while this script is running, the rebuild" >&2
        echo "  picks up the in-progress edits and the baseline becomes" >&2
        echo "  invalid for measuring those edits." >&2
        echo "" >&2
        echo "  RECOMMENDED if you want a HEAD baseline:" >&2
        echo "    Ctrl+C now → git stash → re-run → git stash pop" >&2
        echo "" >&2
        echo "  Or: proceed if you intentionally want a working-tree" >&2
        echo "  baseline (e.g., snapshot post-change state for future" >&2
        echo "  comparisons against further edits)." >&2
        echo "" >&2
        echo "  Continuing in 5 seconds (Ctrl+C to abort)..." >&2
        echo "============================================================" >&2
        sleep 5
    else
        echo "[bench-stable] working tree CLEAN at $head_short — \
baseline will reflect HEAD" >&2
    fi
}

run_bench() {
    local mode="$1"   # "save" or "baseline"
    local name="$2"
    local filter="${3:-}"

    local crit_args=("${STABILITY_FLAGS[@]}")
    case "$mode" in
        save)
            crit_args+=(--save-baseline "$name")
            ;;
        baseline)
            crit_args+=(--baseline "$name")
            ;;
        *)
            echo "[bench-stable] internal: unknown mode '$mode'" >&2
            exit 2
            ;;
    esac

    echo "[bench-stable] running cargo bench under ${LOWER_PRIORITY[*]:-no-priority-wrapper}" >&2
    echo "[bench-stable] criterion flags: ${crit_args[*]}" >&2
    if [[ -n "$filter" ]]; then
        echo "[bench-stable] filter: $filter" >&2
    fi
    echo "[bench-stable] this will take ~30+ seconds per bench function" >&2
    echo "" >&2

    # `cargo bench --bench hot_paths -- ARGS` passes ARGS to the
    # criterion harness. The filter (if given) goes first, before
    # the criterion flags.
    if [[ -n "$filter" ]]; then
        "${LOWER_PRIORITY[@]}" cargo bench -p bsql-pg-proto \
            --bench hot_paths -- "$filter" "${crit_args[@]}"
    else
        "${LOWER_PRIORITY[@]}" cargo bench -p bsql-pg-proto \
            --bench hot_paths -- "${crit_args[@]}"
    fi
}

case "$CMD" in
    save)
        NAME="${2:-}"
        FILTER="${3:-}"
        if [[ -z "$NAME" ]]; then
            echo "[bench-stable] save: <baseline-name> required" >&2
            usage
        fi
        warn_if_dirty_for_save save
        run_bench save "$NAME" "$FILTER"
        echo "" >&2
        echo "[bench-stable] baseline saved as '$NAME'" >&2
        echo "[bench-stable] compare later: scripts/bench-stable.sh compare $NAME" >&2
        ;;
    compare)
        NAME="${2:-}"
        FILTER="${3:-}"
        if [[ -z "$NAME" ]]; then
            echo "[bench-stable] compare: <baseline-name> required" >&2
            usage
        fi
        if [[ ! -d "$BASELINE_DIR" ]]; then
            echo "[bench-stable] no baselines saved (target/criterion/ is empty)" >&2
            exit 2
        fi
        # Verify baseline exists for at least one bench group.
        if ! find "$BASELINE_DIR" -type d -name "$NAME" -print -quit | grep -q .; then
            echo "[bench-stable] baseline '$NAME' not found; available:" >&2
            find "$BASELINE_DIR" -mindepth 3 -maxdepth 3 -type d -printf '  %P\n' 2>/dev/null \
                | awk -F/ '{print $NF}' | sort -u | sed 's/^/  /' >&2 || true
            exit 2
        fi
        # Capture criterion's textual output so we can post-process.
        TMP_LOG="$(mktemp -t bsql-bench-stable-XXXXXX.log)"
        trap 'rm -f "$TMP_LOG"' EXIT

        # cargo bench output goes to stdout; tee to log for parsing.
        if run_bench baseline "$NAME" "$FILTER" 2>&1 | tee "$TMP_LOG"; then
            BENCH_RC=0
        else
            BENCH_RC=$?
        fi

        echo "" >&2
        echo "============================================================" >&2
        echo "Bench summary vs baseline '$NAME'" >&2
        echo "============================================================" >&2
        # Extract criterion's `change:` lines and classify.
        # Criterion prints lines like:
        #   change: [-2.5% -1.0% +0.5%] (p=0.42 > 0.05)
        #   No change in performance detected.
        #   Performance has improved.
        #   Performance has regressed.
        REGRESSIONS=0
        IMPROVEMENTS=0
        UNCHANGED=0
        while IFS= read -r line; do
            case "$line" in
                *"Performance has regressed"*)
                    REGRESSIONS=$((REGRESSIONS + 1))
                    ;;
                *"Performance has improved"*)
                    IMPROVEMENTS=$((IMPROVEMENTS + 1))
                    ;;
                *"No change in performance"*)
                    UNCHANGED=$((UNCHANGED + 1))
                    ;;
            esac
        done < "$TMP_LOG"

        echo "  unchanged:    $UNCHANGED" >&2
        echo "  improvements: $IMPROVEMENTS" >&2
        echo "  regressions:  $REGRESSIONS" >&2
        echo "" >&2

        if [[ "$REGRESSIONS" -gt 0 ]]; then
            echo "[bench-stable] FAIL: $REGRESSIONS bench(es) regressed beyond noise threshold" >&2
            echo "[bench-stable] inspect the criterion output above for specifics" >&2
            exit 1
        fi
        echo "[bench-stable] PASS: no regressions detected" >&2
        exit 0
        ;;
    list)
        if [[ ! -d "$BASELINE_DIR" ]]; then
            echo "[bench-stable] no baselines saved (target/criterion/ is empty)" >&2
            exit 0
        fi
        echo "Saved baselines:" >&2
        find "$BASELINE_DIR" -mindepth 3 -maxdepth 3 -type d 2>/dev/null \
            | awk -F/ '{print $NF}' | sort -u | sed 's/^/  /' >&2
        ;;
    *)
        echo "[bench-stable] unknown command: $CMD" >&2
        usage
        ;;
esac
