#!/bin/sh
# Build the C / sqlite3 benchmark client against the SAME bundled SQLite
# amalgamation bsql's rusqlite/libsqlite3-sys 0.35.0 links (SQLite 3.50.2), with
# the SAME bundled compile defines — so the C engine is byte-for-byte bsql's and
# the C-vs-bsql delta is pure wrapper overhead, not an engine-version confound.
#
# The amalgamation is located in the local cargo registry (no download); it is
# copied next to this script (gitignored) so the compile is self-contained. If
# the registry copy is absent, override with AMAL_DIR=/path/to/sqlite3-amalgamation
# (a dir holding sqlite3.c + sqlite3.h) — or point it at the homebrew amalgamation.
#
# Usage:  sh build.sh          # produces ./sqlite_bench
set -eu
cd "$(dirname "$0")"

# 1. Locate the bundled amalgamation (SQLite 3.50.2, matching bsql).
AMAL_DIR="${AMAL_DIR:-}"
if [ -z "$AMAL_DIR" ]; then
    AMAL_DIR="$(find "$HOME/.cargo/registry/src" -maxdepth 2 -type d -name 'libsqlite3-sys-0.35.0' 2>/dev/null | head -1)/sqlite3"
fi
if [ ! -f "$AMAL_DIR/sqlite3.c" ]; then
    echo "ERROR: sqlite3.c not found under AMAL_DIR=$AMAL_DIR" >&2
    echo "  Install the dep once (cargo build in clients/rust-sqlite) or set AMAL_DIR." >&2
    exit 1
fi

# 2. Vendor it locally (gitignored) so the build is reproducible & self-contained.
cp -f "$AMAL_DIR/sqlite3.c" "$AMAL_DIR/sqlite3.h" .

# 3. The bundled compile defines libsqlite3-sys 0.35.0 uses (build.rs) — so the
#    C engine == bsql's engine (API_ARMOR bounds checks included, i.e. the C
#    client pays exactly what bsql pays).
DEFS="-DSQLITE_CORE \
-DSQLITE_DEFAULT_FOREIGN_KEYS=1 \
-DSQLITE_ENABLE_API_ARMOR \
-DSQLITE_ENABLE_COLUMN_METADATA \
-DSQLITE_ENABLE_DBSTAT_VTAB \
-DSQLITE_ENABLE_FTS3 \
-DSQLITE_ENABLE_FTS3_PARENTHESIS \
-DSQLITE_ENABLE_FTS5 \
-DSQLITE_ENABLE_JSON1 \
-DSQLITE_ENABLE_LOAD_EXTENSION=1 \
-DSQLITE_ENABLE_MEMORY_MANAGEMENT \
-DSQLITE_ENABLE_RTREE \
-DSQLITE_ENABLE_STAT4 \
-DSQLITE_SOUNDEX \
-DSQLITE_THREADSAFE=1 \
-DSQLITE_USE_URI"

# 4. Max-perf codegen ON TOP of the bundled defines (-O3 -march=native -flto).
#    (rusqlite's bundled build uses -O2 without -march=native, so the C engine's
#    codegen is at least as fast as bsql's — C is given every advantage.)
CC="${CC:-clang}"
"$CC" -O3 -march=native -flto -std=c11 $DEFS \
    -o sqlite_bench sqlite_bench.c sqlite3.c -lpthread -ldl -lm

echo "built ./sqlite_bench (SQLite $(grep -m1 '#define SQLITE_VERSION ' sqlite3.h | sed 's/.*"\(.*\)".*/\1/'))"
