# bsql examples

A runnable, heavily-commented tour of bsql that doubles as a usage guide. Each
example is one focused program — read the source top-to-bottom to learn a
feature, or run it to see it work. Every file opens with a banner comment saying
what it demonstrates, which verbs/features it uses, whether it needs a database,
and its exact run command.

## Running

Every example is a Cargo binary, so run any of them with:

```bash
cargo run -p bsql-examples --bin <name>
```

- **SQLite examples run anywhere** — they use an in-memory database, so they need
  no server and no setup.
- **PostgreSQL examples need a live server.** Point the `BSQL_EXAMPLES_DSN`
  environment variable at it (a clear panic tells you if it is unset):

  ```bash
  export BSQL_EXAMPLES_DSN='postgres://USER@127.0.0.1:5432/postgres'
  ```

  The PostgreSQL examples that need tables apply the migrations themselves on
  startup (idempotently), so a fresh database just works. Examples that write use
  a session `TEMP` table where possible, so they are idempotent and never touch
  your real tables.

## The examples

| Example | What it teaches | Backend | Run |
|---|---|---|---|
| `basic_sqlite` | The zero-setup starting point: connect, `query!`, `query_one`, insert | SQLite (anywhere) | `cargo run -p bsql-examples --bin basic_sqlite` |
| `basic_pg_async` | The same CRUD against PostgreSQL, async (tokio) | PostgreSQL | `cargo run -p bsql-examples --bin basic_pg_async` |
| `basic_pg_sync` | The same, blocking driver — the async/sync symmetry | PostgreSQL | `cargo run -p bsql-examples --bin basic_pg_sync` |
| `multi_backend` | ONE `query!` carrier decoding on BOTH SQLite and PostgreSQL | SQLite + PostgreSQL | `cargo run -p bsql-examples --bin multi_backend` |
| `params_vs_raw` | The three input modes: typed `query!` vs `query_params` vs `query_raw`, and the injection distinction | SQLite (anywhere) | `cargo run -p bsql-examples --bin params_vs_raw` |
| `crud` | INSERT / UPDATE / DELETE / SELECT, `… RETURNING`, affected counts | PostgreSQL | `cargo run -p bsql-examples --bin crud` |
| `joins_aggregates` | A JOIN + GROUP BY + subquery; `LEFT JOIN` nullability inference | PostgreSQL | `cargo run -p bsql-examples --bin joins_aggregates` |
| `migrations` | Apply a versioned schema: embedded + directory sources, status, dry-run, idempotency | PostgreSQL | `cargo run -p bsql-examples --bin migrations` |
| `generated_types` | Rust types generated from your DDL: enum / composite / domain | PostgreSQL | `cargo run -p bsql-examples --bin generated_types` |
| `external_bridges` | Decode columns straight into `uuid::Uuid` / `chrono::DateTime<Utc>` | PostgreSQL | `cargo run -p bsql-examples --bin external_bridges` |
| `n1_detection` | Catch the N+1 anti-pattern with `conn.n1_report()`, then fix it | SQLite (anywhere) | `cargo run -p bsql-examples --features n1-detect --bin n1_detection` |
| `typed_copy` | Safe, fast bulk loading with `copy!` + `copy_in_typed` | PostgreSQL | `cargo run -p bsql-examples --bin typed_copy` |
| `pipeline_and_batch` | Atomic multi-command verbs: `pipeline`, `execute_batch`, `query_batch` | PostgreSQL | `cargo run -p bsql-examples --bin pipeline_and_batch` |
| `streaming` | Constant-memory reads over a colossal result with `query_each` | SQLite (anywhere) | `cargo run -p bsql-examples --bin streaming` |
| `transactions` | Closure-scoped transactions: commit on `Ok`, rollback on `Err` | SQLite (anywhere) | `cargo run -p bsql-examples --bin transactions` |
| `pooling` | A connection pool, diagnostics, and reconnect-vs-retry | PostgreSQL | `cargo run -p bsql-examples --bin pooling` |
| `schema_per_test` | Per-test schema isolation with `#[bsql::test]` (an integration test) | PostgreSQL | see below |

`n1_detection` needs the non-default `n1-detect` feature (shown in its run
command); without it the binary prints how to enable it.

`schema_per_test` is an integration test (in `tests/`), not a binary. It needs a
PostgreSQL named by `BSQL_TEST_DSN` (a *test* variable, since the harness creates
and drops schemas):

```bash
BSQL_TEST_DSN='postgres://USER@127.0.0.1:5432/postgres' \
  cargo test -p bsql-examples --test schema_per_test -- --ignored
```

## How these examples are wired

This crate is a normal bsql consumer — the wiring is exactly what your own
project uses:

- **`Cargo.toml`** depends on the single `bsql` umbrella crate with the features
  the examples need (`macros`, `postgres-async`, `postgres-sync`, `sqlite`,
  `test-harness`), and adds `bsql-build` as a `[build-dependencies]` helper. The
  `query!` proc-macro and the build-time SQL parser never enter the runtime
  binary.
- **`build.rs`** replays `migrations/*.sql` into the schema catalog the `query!`
  macros type against, registers the external-type bridges (chrono / uuid), and
  bakes the migration set for `embed_migrations!()`.
- **`migrations/`** is one shared, ordered, commented schema every example uses.
- **`src/lib.rs`** holds the shared helpers: the DSN reader, the migration
  runner wrappers, the SQLite schema string, the bridge converter functions, and
  the `user_types!()` invocation that generates `Mood` / `Address`.
- **`src/bin/*.rs`** is one file per example (Cargo auto-discovers them).

The takeaway: a real consumer adds ONE dependency (`bsql`) plus the `bsql-build`
build helper and a one-line `build.rs`, and reaches the whole compile-checked
query API, all three backends, and every feature above.
