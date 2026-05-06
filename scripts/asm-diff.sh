#!/usr/bin/env bash
# Diff ASM for matching symbols between current working tree and
# a git reference (default: HEAD).
#
# Foundation tool for tier-elevation verification per
# `reforge.md` measurement-methodology section. Use this BEFORE
# `bench-stable.sh` — codegen change is the deterministic
# question; perf delta is the statistical follow-up.
#
# Usage:
#   scripts/asm-diff.sh <symbol-pattern> [git-ref]
#
# Examples:
#   scripts/asm-diff.sh materialise               # vs HEAD
#   scripts/asm-diff.sh compute_response_body main  # vs main branch
#
# Requires a clean tree OR a tree with uncommitted changes (uses
# `git stash` to compare current vs ref). If the stash/restore
# round-trip fails, the working tree is preserved in
# `git stash list` — recover with `git stash pop`.

set -euo pipefail

PATTERN="${1:-}"
REF="${2:-HEAD}"

if [[ -z "$PATTERN" ]]; then
    echo "usage: $0 <symbol-pattern> [git-ref]" >&2
    echo "example: $0 materialise" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$WORKSPACE"

# Verify ref exists.
if ! git rev-parse --verify --quiet "$REF" >/dev/null; then
    echo "[asm-diff] git ref '$REF' does not exist" >&2
    exit 1
fi

# Resolve to commit hash for clear logging.
REF_HASH="$(git rev-parse --short "$REF")"
echo "[asm-diff] comparing working tree vs $REF ($REF_HASH)" >&2

# Determine if working tree is dirty.
DIRTY=0
if ! git diff --quiet || ! git diff --cached --quiet; then
    DIRTY=1
fi

TMPDIR="$(mktemp -d -t bsql-asm-diff-XXXXXX)"
BEFORE="$TMPDIR/before.s"
AFTER="$TMPDIR/after.s"
trap 'rm -rf "$TMPDIR"' EXIT

# Snapshot current working tree → AFTER (the "with changes" state).
# Note on exit code 2 from asm-dump (= "no symbols match"): we treat
# this as fail-soft because asymmetric matches (symbol present at one
# side, absent at the other — e.g. inline-status flipped) are
# legitimate diffs to display. Empty `$AFTER` then differs from a
# non-empty `$BEFORE` cleanly.
echo "[asm-diff] dumping current state → $AFTER" >&2
# Bash's ERR trap fires on command failure independent of `set -e`,
# so we use `if ! cmd` to capture the exit code without triggering
# the trap (the conditional context is excluded from ERR per bash
# manual). The "(no cleanup_and_exit installed yet here, but the
# pattern is consistent across both dump calls.)"
DUMP_AFTER_RC=0
if ! "$SCRIPT_DIR/asm-dump.sh" "$PATTERN" "$AFTER"; then
    DUMP_AFTER_RC=$?
fi
if [[ $DUMP_AFTER_RC -ne 0 ]] && [[ $DUMP_AFTER_RC -ne 2 ]]; then
    echo "[asm-diff] asm-dump (current) failed with code $DUMP_AFTER_RC" >&2
    exit $DUMP_AFTER_RC
fi
# Ensure file exists even if dump found nothing.
[[ -e "$AFTER" ]] || echo "" > "$AFTER"

# If the user is comparing against HEAD AND the tree is clean, the
# diff would be empty — short-circuit.
if [[ "$REF" == "HEAD" ]] && [[ "$DIRTY" == "0" ]]; then
    echo "[asm-diff] working tree clean and ref is HEAD — nothing to diff." >&2
    echo "[asm-diff] hint: pass an older ref to compare against, e.g.:" >&2
    echo "  scripts/asm-diff.sh $PATTERN HEAD~1" >&2
    exit 0
fi

# Now snapshot the ref state. Strategy:
# - If working tree is dirty: stash, checkout ref, build, dump, restore.
# - If working tree is clean but ref != HEAD: just checkout ref temporarily.
#
# Both paths use a single `git stash` / `git checkout` round-trip
# with explicit error handling. If anything fails between save and
# restore, we surface the recovery command.

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
# Track the steps we need to roll back. Stored as plain strings,
# one per line, in `$NEED_ROLLBACK`. Using a string instead of an
# array keeps `set -u` semantics simple (empty string is fine,
# empty arrays trip "unbound variable" pre-Bash-4.4).
NEED_CHECKOUT_BACK=0
NEED_STASH_POP=0

cleanup_and_exit() {
    local rc=$?
    set +e
    if [[ "$NEED_CHECKOUT_BACK" == "1" ]]; then
        echo "[asm-diff] rolling back: git checkout $CURRENT_BRANCH" >&2
        git checkout --quiet "$CURRENT_BRANCH" || \
            echo "[asm-diff] WARNING: checkout rollback failed; tree at detached $REF" >&2
    fi
    if [[ "$NEED_STASH_POP" == "1" ]]; then
        echo "[asm-diff] rolling back: git stash pop" >&2
        git stash pop --quiet || {
            echo "[asm-diff] WARNING: stash pop failed; recover via:" >&2
            echo "  git stash list  # see saved entry" >&2
            echo "  git stash pop   # restore" >&2
        }
    fi
    exit "$rc"
}
trap cleanup_and_exit ERR

# Stash if dirty.
if [[ "$DIRTY" == "1" ]]; then
    echo "[asm-diff] working tree is dirty; stashing for ref dump" >&2
    # Don't `--include-untracked`: scripts/ itself is typically
    # untracked while we're authoring it, and stashing it would
    # remove asm-dump.sh from disk before we can call it from the
    # ref-side dump. Untracked files don't conflict with `git
    # checkout` (Git refuses to overwrite untracked unless the
    # checked-out commit explicitly creates a file of that name).
    git stash push --message "asm-diff: temp stash for $REF dump" >/dev/null
    NEED_STASH_POP=1
fi

# Checkout the ref (detached HEAD).
git checkout --quiet "$REF"
NEED_CHECKOUT_BACK=1

# Dump ASM at the ref. Same fail-soft rule for "no symbols": an
# absent symbol at the ref vs present in current is a legitimate
# asymmetric diff (e.g. function gained an `#[inline(never)]`,
# moving from inlined to standalone).
echo "[asm-diff] dumping ref state → $BEFORE" >&2
DUMP_BEFORE_RC=0
if ! "$SCRIPT_DIR/asm-dump.sh" "$PATTERN" "$BEFORE"; then
    DUMP_BEFORE_RC=$?
fi
if [[ $DUMP_BEFORE_RC -ne 0 ]] && [[ $DUMP_BEFORE_RC -ne 2 ]]; then
    echo "[asm-diff] asm-dump (ref) failed with code $DUMP_BEFORE_RC" >&2
    exit $DUMP_BEFORE_RC
fi
[[ -e "$BEFORE" ]] || echo "" > "$BEFORE"

# Roll back: checkout original branch, pop stash if any.
echo "[asm-diff] restoring working tree to $CURRENT_BRANCH" >&2
git checkout --quiet "$CURRENT_BRANCH"
NEED_CHECKOUT_BACK=0

if [[ "$NEED_STASH_POP" == "1" ]]; then
    echo "[asm-diff] popping stash" >&2
    git stash pop --quiet
    NEED_STASH_POP=0
fi
trap - ERR

# Diff.
echo "" >&2
echo "============================================================" >&2
echo "ASM diff: pattern='$PATTERN', ref=$REF ($REF_HASH)" >&2
echo "  before (ref): $BEFORE ($(wc -l < "$BEFORE" | tr -d ' ') lines)" >&2
echo "  after (current): $AFTER ($(wc -l < "$AFTER" | tr -d ' ') lines)" >&2
echo "============================================================" >&2
echo "" >&2

if diff -u "$BEFORE" "$AFTER"; then
    echo "" >&2
    echo "[asm-diff] no codegen change for pattern '$PATTERN'" >&2
    echo "[asm-diff] (the change is either ASM-neutral or matches no symbols)" >&2
    exit 0
fi
# diff returns 1 if differences found — that's our success signal.
exit 0
