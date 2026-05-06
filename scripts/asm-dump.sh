#!/usr/bin/env bash
# Dump ASM for crate-internal symbols matching a pattern.
#
# Foundation tool for tier-elevation verification per
# `reforge.md` measurement-methodology section. ASM-diff is the
# PRIMARY (deterministic) check for codegen-relevant changes;
# `bench-stable.sh` is the SECONDARY (statistical) check.
#
# Usage:
#   scripts/asm-dump.sh <symbol-pattern> [output-file]
#
# Examples:
#   scripts/asm-dump.sh materialise
#   scripts/asm-dump.sh compute_response_body /tmp/before.s
#
# Pattern is a substring of the demangled symbol name (e.g.
# `materialise`, `compute_push_idle_only`, `parse_header`). Matching
# is case-sensitive.
#
# Output:
#   - Each matching function's body, with mangled-name hashes
#     stripped (`17h<16-hex>E` → `E`) and anonymous-data refs
#     normalised (`l_anon.<file-hash>.<n>` → `l_anon.<HASH>.<n>`)
#     so two snapshots from different builds diff cleanly.
#   - One symbol header + body per match, separated by blank lines.

set -euo pipefail

PATTERN="${1:-}"
OUT_FILE="${2:-}"

if [[ -z "$PATTERN" ]]; then
    echo "usage: $0 <symbol-pattern> [output-file]" >&2
    echo "example: $0 materialise" >&2
    exit 1
fi

# Locate workspace root (script may be invoked from anywhere).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$WORKSPACE"

# Build with --emit=asm in release mode to match production codegen.
# `-C debuginfo=0` keeps the .s file from being polluted with debug
# directives that obscure the actual instructions.
echo "[asm-dump] cargo rustc --release --emit=asm ..." >&2
cargo rustc -p bsql-pg-proto --release --lib -- --emit=asm \
    -C debuginfo=0 \
    >/dev/null 2>&1 || {
    echo "[asm-dump] cargo rustc failed; rerun without redirects:" >&2
    echo "  cargo rustc -p bsql-pg-proto --release --lib -- --emit=asm -C debuginfo=0" >&2
    exit 1
}

# Find the most recent .s file. cargo rustc emits one per crate,
# named with the metadata hash. Newest mtime is the one we just built.
ASM_FILE="$(ls -t target/release/deps/bsql_pg_proto-*.s 2>/dev/null | head -1)"
if [[ -z "$ASM_FILE" ]]; then
    echo "[asm-dump] No .s file found in target/release/deps/" >&2
    echo "[asm-dump] cargo rustc may have been a no-op cache hit. Touch source:" >&2
    echo "  touch crates/bsql-pg-proto/src/lib.rs && rerun" >&2
    exit 1
fi
echo "[asm-dump] using $ASM_FILE" >&2

# Locate matching symbols. Mangled symbols look like
# `__ZN13bsql_pg_proto8...11materialise17h<hash>E:` on macOS
# (double underscore) and `_ZN...E:` on Linux (single underscore).
# We anchor on the symbol declaration line (ends with `E:`).
SYMBOL_LINES="$(grep -E "^_+ZN.*${PATTERN}.*17h[0-9a-f]+E:\$" "$ASM_FILE" || true)"
if [[ -z "$SYMBOL_LINES" ]]; then
    echo "[asm-dump] no symbols match pattern '$PATTERN'" >&2
    echo "[asm-dump] hint: try a partial name like 'compute_response' or 'materialise'" >&2
    echo "[asm-dump] available symbols (sample):" >&2
    grep -E "^_+ZN.*17h[0-9a-f]+E:\$" "$ASM_FILE" | head -20 | sed 's/^/  /' >&2
    exit 2
fi

MATCH_COUNT=$(echo "$SYMBOL_LINES" | wc -l | tr -d ' ')
echo "[asm-dump] $MATCH_COUNT match(es)" >&2

# Extract function bodies. For each matching symbol:
# - Body starts at the symbol-declaration line.
# - Body ends at `.cfi_endproc` (macOS/Linux ABI cfi-style).
# - Normalize hashes: `17h<hex>E` → `E`, `l_anon.<hex>.<n>` → `l_anon.<HASH>.<n>`.
OUTPUT=$(awk -v pattern="$PATTERN" '
    BEGIN { in_func = 0; func_count = 0 }

    # Symbol-declaration line (mangled, with hash). Match either
    # `_ZN...E:` (Linux) or `__ZN...E:` (macOS) by allowing one or
    # more leading underscores.
    /^_+ZN.*17h[0-9a-f]+E:$/ {
        if (index($0, pattern) > 0) {
            if (in_func) {
                # Edge: previous function did not close with .cfi_endproc.
                # Insert a separator before starting next.
                print ""
            }
            # Normalize the hash in the symbol declaration.
            line = $0
            gsub(/17h[0-9a-f]+E:/, "E:", line)
            print line
            in_func = 1
            func_count++
            next
        } else {
            # Different symbol; if we were inside a matching one,
            # close it.
            if (in_func) {
                in_func = 0
                print ""
            }
        }
    }

    # End-of-function marker.
    /^[[:space:]]*\.cfi_endproc[[:space:]]*$/ {
        if (in_func) {
            print $0
            in_func = 0
            next
        }
    }

    # Inside function body — emit the line, normalising hashes.
    in_func == 1 {
        line = $0
        # Mangled-name hash inside `bl` calls / data refs.
        gsub(/17h[0-9a-f]+E/, "E", line)
        # Anonymous-data file-hash (per-build). Normalize the
        # 16+ hex chars after `l_anon.`. The trailing `.<num>`
        # stays — anonymous-data ordinals are stable.
        gsub(/l_anon\.[0-9a-f]+\./, "l_anon.<HASH>.", line)
        # Local-label hashes occasionally embed similar markers.
        # Normalise `Lloh<num>` (linker hint, build-specific).
        gsub(/Lloh[0-9]+/, "Lloh<N>", line)
        print line
    }
' "$ASM_FILE")

# Emit (file or stdout).
if [[ -n "$OUT_FILE" ]]; then
    echo "$OUTPUT" > "$OUT_FILE"
    BYTES=$(wc -c < "$OUT_FILE" | tr -d ' ')
    LINES=$(wc -l < "$OUT_FILE" | tr -d ' ')
    echo "[asm-dump] wrote $LINES lines / $BYTES bytes to $OUT_FILE" >&2
else
    echo "$OUTPUT"
fi
