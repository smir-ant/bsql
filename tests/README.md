# Tests

## How to run

```bash
# Unit tests (no database needed, fast)
cargo test --workspace --lib

# PG integration tests (requires live PostgreSQL)
BSQL_DATABASE_URL="postgres://bsql:bsql@localhost/bsql_test" cargo test -p bsql
BSQL_DATABASE_URL="postgres://bsql:bsql@localhost/bsql_test" cargo test -p bsql-driver-postgres --test integration

# SQLite integration tests (requires test DB file)
./tests/sqlite_setup.sh /tmp/bsql_test.db
BSQL_DATABASE_URL="sqlite:///tmp/bsql_test.db" cargo test -p bsql --test sqlite_query --features sqlite-bundled -- --test-threads=1
cargo test -p bsql --test sqlite_basic --features sqlite-bundled

# Stress tests (slow, not in normal CI)
BSQL_DATABASE_URL="postgres://bsql:bsql@localhost/bsql_test" cargo test -p bsql --test basic -- --ignored
BSQL_DATABASE_URL="postgres://bsql:bsql@localhost/bsql_test" cargo test -p bsql-driver-postgres --test integration -- --ignored

# All tests including stress
BSQL_DATABASE_URL="postgres://bsql:bsql@localhost/bsql_test" cargo test --workspace -- --include-ignored

# Compile-fail tests (validates error messages)
BSQL_DATABASE_URL="postgres://bsql:bsql@localhost/bsql_test" cargo test -p bsql --test compile_fail

# Property-based tests (more iterations)
PROPTEST_CASES=10000 cargo test -p bsql-driver-postgres --lib

# Doc tests
cargo test -p bsql-driver-postgres --doc

# CLI tests
cargo test -p bsql-cli

# Clippy + format check
cargo clippy --workspace -- -D warnings
cargo fmt --all --check
```

## PG setup

```bash
psql -U postgres -c "CREATE DATABASE bsql_test"
psql -U postgres -c "CREATE USER bsql WITH PASSWORD 'bsql'"
psql -U postgres -c "GRANT ALL ON DATABASE bsql_test TO bsql"
psql -h localhost -U bsql -d bsql_test -f tests/setup.sql
```

## Test groups

| Group | Count | Speed | Requires | What it tests |
|---|---|---|---|---|
| Unit (macros) | ~630 | <1s | nothing | codegen, SQL parsing, type mapping, validation |
| Unit (core) | ~380 | <1s | nothing | pool logic, error types, singleflight, stream |
| Unit (pg driver) | ~570 | 5s | nothing | codec, protocol, auth, statement cache |
| Unit (sqlite driver) | ~400 | <1s | nothing | FFI, codec, pool, arena |
| PG integration | ~160 | 5s | live PG | connection, prepared statements, COPY, pgbouncer |
| PG query! | ~110 | <1s | live PG | fetch, execute, types, joins, aggregates, params |
| PG transactions | ~38 | 5s | live PG | commit, rollback, savepoints, deferred, isolation |
| PG types | ~11 | <1s | live PG | enum, uuid, time, decimal |
| PG dynamic | ~19 | <1s | live PG | optional clauses, sort enums |
| PG other | ~55 | 50s | live PG | singleflight, listener, async, r/w split |
| SQLite query! | ~10 | <1s | test.db | fetch, execute, nullable, deref |
| SQLite driver-level | ~10 | <1s | nothing | pool, transactions, isolation |
| Compile-fail | ~32 | 3s | live PG | type errors, SQL errors, safety checks |
| CLI | ~46 | 75s | nothing | migrate, verify-cache, clean, cache format |
| Doc tests | ~13 | 3s | nothing | Config::from_url, hash_sql |
| Stress (#[ignore]) | ~6 | 30s+ | live PG | 10K rows, 16 threads, singleflight contention |
| **TOTAL** | **~2,500+** | | | |

## Test file map

```
crates/bsql/tests/
  basic.rs            — core CRUD, params, types, deref, JSONB, SQL constructs
  transactions.rs     — tx lifecycle, savepoints, deferred, isolation, errors
  dynamic.rs          — optional WHERE clauses, sort enums
  types.rs            — pg_enum, uuid, time, decimal (feature-gated)
  async_mode.rs       — async pool, concurrent queries
  singleflight.rs     — query coalescing
  read_write_split.rs — primary/replica routing
  listener.rs         — LISTEN/NOTIFY
  sqlite_basic.rs     — SQLite driver-level tests
  sqlite_query.rs     — SQLite query! macro tests
  compile_fail/       — 29 .rs files with expected .stderr snapshots

crates/bsql-driver-postgres/tests/
  integration.rs      — driver-level: connect, query, pool, errors, pgbouncer

crates/bsql-cli/src/
  main.rs, cache.rs, migrate.rs, verify.rs — inline #[cfg(test)] modules
```

## Coverage specification

See [COVERAGE.md](COVERAGE.md) — 476 scenarios across 32 sections.
Serves as test instruction for any backend.
