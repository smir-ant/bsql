CC ?= clang
CFLAGS = -O3 -march=native -Wall -Wextra

# PostgreSQL (via pg_config)
PG_INCLUDEDIR := $(shell pg_config --includedir)
PG_LIBDIR     := $(shell pg_config --libdir)
LDFLAGS_PG     = -L$(PG_LIBDIR) -lpq -Wl,-rpath,$(PG_LIBDIR)
CFLAGS_PG      = -I$(PG_INCLUDEDIR)

# SQLite3 (homebrew or system)
SQLITE_INCLUDEDIR := $(shell pkg-config --cflags sqlite3 2>/dev/null || echo "")
SQLITE_LDFLAGS    := $(shell pkg-config --libs sqlite3 2>/dev/null || echo "-lsqlite3")

.PHONY: all clean

all: pg_bench sqlite_bench

pg_bench: pg_bench.c
	$(CC) $(CFLAGS) $(CFLAGS_PG) -o $@ $< $(LDFLAGS_PG)

sqlite_bench: sqlite_bench.c
	$(CC) $(CFLAGS) $(SQLITE_INCLUDEDIR) -o $@ $< $(SQLITE_LDFLAGS)

clean:
	rm -f pg_bench sqlite_bench
