# bsql — Multi-backend SQL toolkit in Rust

## Workspace layout

```
crates/
  bsql/              — umbrella re-export (bsql::pg, bsql::pg_sync, bsql::sqlite)
  postgres/
    proto/           — sans-IO wire protocol state machine (no_std, ~49K LoC)
    core/            — shared Session + types (1010 LoC)
    async/           — tokio async driver (554 LoC)
    sync/            — std::net sync driver (564 LoC)
    derive/          — proc-macro for prepared! statements
  sqlite/
    driver/          — embedded SQLite driver (340 LoC)
```

## Build & test

```bash
cargo check --workspace              # full build
cargo test -p bsql-sqlite            # SQLite (no PG needed)
cargo test -p bsql-postgres-async --test sq_live -- --ignored    # async PG (needs local PG)
cargo test -p bsql-postgres-sync --test sync_live -- --ignored   # sync PG (needs local PG)
```

PG tests require: PostgreSQL on localhost:5432, user `smir-ant`, database `postgres`, trust auth.
SCRAM test requires: user `bsql_test_scram` with password `test_password_123` in pg_hba.conf.

## Architecture

- **Sans-IO core** (`bsql-pg-proto`): wire protocol with zero I/O dependencies. State machine driven by `feed_bytes`/`advance_one_frame`.
- **Session** (`bsql-postgres-core`): pump state machine wrapping PgProtocol. Both drivers use identical `pump_step() → PumpAction` loop.
- **Drivers** are thin I/O adapters (~15-line pump loop each). Difference: `.await` on async, blocking on sync.
- **Row** uses Arc-shared arena: 4 heap allocations per entire QueryResult (not per row). Row is 16 bytes, `'static + Clone + Send + Sync`.

## Safety invariants

- `#![forbid(unsafe_code)]` on all driver crates
- `#![deny(clippy::unwrap_used, clippy::expect_used)]` on all driver crates
- Static assertions: `Connection: Send`, `Row: Send + Sync + 'static`, `Pool: Send + Sync`
- NULL = `Option<NonZeroU32>` (compiler-enforced, no sentinel)
- `PreparedStatement` consumed by `close_statement(stmt)` — use-after-close is compile error
- Transactions via closure scope — no forgotten commits
- Passwords `Zeroizing<String>` — scrubbed on drop, redacted in Debug

## Conventions

- No `expect()` or `unwrap()` in production code
- Error types: `DriverError::Db(DbError)` for server errors with SQLSTATE, `DriverError::Config` for pre-connect validation, `DriverError::NoRows` for empty results
- `ConnectConfig` is `#[non_exhaustive]` — construct via `new()` + builder methods
- `SslMode::Prefer` warns in debug builds about SSL downgrade risk
