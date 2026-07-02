#!/usr/bin/env bash
# Post-LTO codegen diff via the LINKED binary.
#
# Why this exists (and why asm-dump.sh is not enough): `cargo rustc
# --emit=asm` on the *lib* dumps PRE-LTO, per-CGU assembly, and under
# `lto = "fat"` the hot sans-IO functions (feed_bytes, dispatch,
# col_next, ...) are fully INLINED into their callers — they do not
# survive as standalone symbols in EITHER the lib asm OR the final
# binary. So you cannot isolate them by symbol. What you CAN do is
# disassemble the whole fully-optimized linked binary and diff a
# NORMALIZED instruction stream (addresses/offsets blanked). The
# release build is deterministic (codegen-units=1 + fat LTO), so a
# no-change rebuild diffs to nothing — therefore any non-empty diff is
# a REAL codegen change, not noise. This is the methodology the
# converged analysis (critic probe-5) locked in.
#
# Usage:
#   scripts/asm-linked-diff.sh [bench-name] [git-ref]
# Defaults: bench=hot_paths, ref=HEAD. Compares the current working
# tree (or HEAD) against <git-ref>. Works with a dirty tree (stashes).
set -uo pipefail

BENCH="${1:-hot_paths}"
REF="${2:-HEAD}"
# Package hosting the bench. Default is the proto crate's `hot_paths`; set
# `PKG=bsql-query-fixture` to target the `typed_decode` bench.
PKG="${PKG:-bsql-postgres-proto}"
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT" || exit 1
TMP="$(mktemp -d)"

norm() {
    # strip the leading address column, blank absolute hex operands and
    # any 6+ hex run (branch targets / embedded hashes) so only the
    # instruction/register structure remains.
    # Blank everything that is a function of binary LAYOUT (so only real
    # instruction/register changes remain): the leading address column,
    # the `adrp` page immediate (a decimal high-bits-of-address operand),
    # absolute hex operands, and any 6+ hex run (branch targets / mangled
    # hashes in `; symbol` comments).
    otool -tV "$1" \
        | sed -E 's/^[0-9a-f]{8,16}[[:space:]]+//; s/(adrp[[:space:]]+[a-z0-9]+,[[:space:]]*)[0-9]+/\1P/; s/0x[0-9a-f]+/0xA/g; s/\b[0-9a-f]{6,16}\b/HX/g'
}

build_and_dump() {
    # $1 = destination normalized dump
    cargo bench -p "$PKG" --bench "$BENCH" --no-run >/dev/null 2>&1 \
        || { echo "[asm-linked] build failed for $PKG/$BENCH" >&2; return 1; }
    local bin
    bin="$(ls -t "target/release/deps/${BENCH}"-* 2>/dev/null | grep -vE '\.(d|dSYM)$' | head -1)"
    [ -n "$bin" ] || { echo "[asm-linked] no $BENCH binary found" >&2; return 1; }
    norm "$bin" >"$1"
}

ORIG="$(git symbolic-ref -q --short HEAD || git rev-parse HEAD)"
STASHED=0

restore() {
    git checkout -q "$ORIG" 2>/dev/null || true
    [ "$STASHED" = 1 ] && git stash pop -q 2>/dev/null || true
}
trap restore EXIT

echo "[asm-linked] building current ($ORIG) ..." >&2
build_and_dump "$TMP/after.s" || exit 1

if ! git diff --quiet || ! git diff --cached --quiet; then
    git stash push -q -m "asm-linked-diff" && STASHED=1
fi

echo "[asm-linked] building ref ($REF) ..." >&2
git checkout -q "$REF" || { echo "[asm-linked] checkout $REF failed" >&2; exit 1; }
build_and_dump "$TMP/before.s" || exit 1
git checkout -q "$ORIG"
[ "$STASHED" = 1 ] && { git stash pop -q; STASHED=0; }
trap - EXIT

echo "============================================================"
echo "post-LTO codegen diff: pkg=$PKG bench=$BENCH  current=$ORIG  ref=$REF"
echo "  before: $(wc -l <"$TMP/before.s" | tr -d ' ') normalized lines"
echo "  after:  $(wc -l <"$TMP/after.s" | tr -d ' ') normalized lines"
echo "============================================================"
if diff -q "$TMP/before.s" "$TMP/after.s" >/dev/null; then
    echo "[asm-linked] IDENTICAL post-LTO codegen — change is ASM-neutral."
else
    n="$(diff "$TMP/before.s" "$TMP/after.s" | grep -cE '^[<>]')"
    echo "[asm-linked] CODEGEN CHANGED: $n differing normalized lines."
    diff -u "$TMP/before.s" "$TMP/after.s" | grep -E '^[+-]' | grep -vE '^[+-]{3}' | head -60
fi
