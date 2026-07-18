//! Shared helpers for the bsql example suite.
//!
//! Everything reusable across the `src/bin/*.rs` examples lives here:
//!   * [`dsn`] — read the PostgreSQL DSN from the environment (a loud panic if
//!     unset), so every PostgreSQL example connects the same way;
//!   * [`ensure_schema_async`] / [`ensure_schema_sync`] — apply the shared
//!     migration set to a live PostgreSQL (idempotent via the runner's ledger),
//!     so a PostgreSQL example can create its tables on first run;
//!   * [`SQLITE_SCHEMA`] — the portable schema the in-memory SQLite examples apply;
//!   * [`to_chrono`] / [`to_uuid`] — the INFALLIBLE external-type bridge converters
//!     that `build.rs` names by string path (the `external_bridges` example);
//!   * `user_types!()` — generates `Mood` / `Address` from the migration DDL (the
//!     `generated_types` example imports them).
//!
//! Examples legitimately `unwrap`/`expect`/`panic` for brevity — a failed
//! `unwrap` in a demo IS the loud signal — so this crate opts out of the
//! workspace's production panic-class floor.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::manual_unwrap_or,
    clippy::manual_unwrap_or_default,
    reason = "example/teaching code: unwrap/expect/panic read clearly, and the manual `match` on an \
              `Option` is the form the workspace disallowed-methods ledger requires (the `unwrap_or*` \
              family is banned)"
)]
#![forbid(unsafe_code)]

/// The environment variable each PostgreSQL example reads its connection DSN
/// from. Deliberately distinct from an app's `DATABASE_URL`.
pub const DSN_ENV: &str = "BSQL_EXAMPLES_DSN";

/// The PostgreSQL DSN for the examples, from [`DSN_ENV`].
///
/// A loud panic if unset — the PostgreSQL examples need a live server (the
/// SQLite examples need none). Set it, e.g.:
///
/// ```bash
/// export BSQL_EXAMPLES_DSN='postgres://smir-ant@127.0.0.1:5432/postgres'
/// ```
#[must_use]
pub fn dsn() -> String {
    // A `match` rather than `.unwrap_or_else` — the workspace bans the
    // silent-fallback `unwrap_or*` family, and here we want a LOUD panic anyway.
    match std::env::var(DSN_ENV) {
        Ok(value) => value,
        Err(_) => panic!(
            "the PostgreSQL examples need a live server: set the `{DSN_ENV}` \
             environment variable to your DSN, e.g.\n  \
             export {DSN_ENV}='postgres://USER@127.0.0.1:5432/postgres'\n\
             (the SQLite examples — basic_sqlite, multi_backend — need no database)"
        ),
    }
}

/// The migration set baked into the binary at build time by
/// `bsql_build::emit_migrations` (see `build.rs`). Applied by
/// [`ensure_schema_async`] / [`ensure_schema_sync`] and shown off by the
/// `migrations` example. `embed_migrations!()` needs the build step; invoking it
/// without one is a loud compile error, never a silent empty set.
pub const EMBEDDED_MIGRATIONS: &[(&str, &str)] = bsql::embed_migrations!();

/// Apply the shared migration set to a live PostgreSQL over the ASYNC driver,
/// exactly once and in deterministic order (idempotent — a re-run is a no-op via
/// the `_bsql_migrations` ledger). Every async PostgreSQL example calls this at
/// startup so its tables exist.
///
/// # Errors
/// Propagates a [`bsql::pg::MigrationError`] (drift, a failed migration, a lock
/// timeout, …).
pub async fn ensure_schema_async(
    conn: &mut bsql::pg::Connection,
) -> Result<bsql::pg::MigrationReport, bsql::pg::MigrationError> {
    conn.run_migrations(bsql::pg::MigrationSource::embedded(EMBEDDED_MIGRATIONS))
        .await
}

/// The blocking-driver twin of [`ensure_schema_async`].
///
/// # Errors
/// Propagates a [`bsql::pg_sync::MigrationError`].
pub fn ensure_schema_sync(
    conn: &mut bsql::pg_sync::Connection,
) -> Result<bsql::pg_sync::MigrationReport, bsql::pg_sync::MigrationError> {
    conn.run_migrations(bsql::pg_sync::MigrationSource::embedded(EMBEDDED_MIGRATIONS))
}

/// The portable schema the in-memory SQLite examples apply (via
/// `conn.execute_batch_raw(SQLITE_SCHEMA)`). It mirrors the SQLite-portable
/// tables in `migrations/` — the SAME column shapes a `query!` carrier was typed
/// against on PostgreSQL — so a compile-checked query runs unchanged on both.
pub const SQLITE_SCHEMA: &str = "\
    CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL, name TEXT);\
    CREATE TABLE authors (id BIGINT PRIMARY KEY, name TEXT NOT NULL);\
    CREATE TABLE books (id BIGINT PRIMARY KEY, author_id BIGINT NOT NULL, \
                        title TEXT NOT NULL, published_year INTEGER);";

// ── External-type bridge converters ─────────────────────────────────────────
//
// One INFALLIBLE free function per bridged PostgreSQL type. `build.rs` names each
// by its `bsql_examples::…` string path; the macro splices the call into the
// generated record decode. The free-fn form is the orphan-proof seam: a consumer
// canNOT `impl bsql::Cell for chrono::DateTime` (E0117 — both foreign), but a
// free fn compiles for any foreign target — so bsql forces NO dependency.

/// Bridge a PostgreSQL `timestamptz` into `chrono::DateTime<Utc>`.
///
/// Infallible: a real timestamp maps through its exact Unix micros; the
/// `±infinity` sentinels (which have no civil instant) map to the Unix epoch —
/// the consumer's own total-function choice.
#[must_use]
pub fn to_chrono(value: bsql::Timestamptz) -> chrono::DateTime<chrono::Utc> {
    // `to_unix_micros` is `Some` for every real value, `None` only for
    // ±infinity. `from_timestamp_micros(0)` (the epoch) is always `Some`.
    let micros = match value.to_unix_micros() {
        Some(micros) => micros,
        None => 0,
    };
    match chrono::DateTime::from_timestamp_micros(micros) {
        Some(dt) => dt,
        None => chrono::DateTime::from_timestamp_micros(0).expect("the Unix epoch is a valid instant"),
    }
}

/// Bridge a PostgreSQL `uuid` into the real `uuid::Uuid` by copying its 16 raw
/// bytes. Infallible.
#[must_use]
pub fn to_uuid(value: bsql::Uuid) -> uuid::Uuid {
    uuid::Uuid::from_bytes(*value.as_bytes())
}

// ── Generated user types ────────────────────────────────────────────────────
//
// `user_types!()` reads the migration catalog and generates a Rust type for each
// PostgreSQL user-defined type declared in `migrations/`: `enum Mood { Happy,
// Sad, Neutral }` (from the ENUM) and `struct Address { … }` (from the
// COMPOSITE). The DOMAIN generates no type (it IS its base). The `generated_types`
// example imports `bsql_examples::{Mood, Address}` and decodes columns into them.
bsql::user_types!();
