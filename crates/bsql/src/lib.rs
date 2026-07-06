#![forbid(unsafe_code)]

//! # bsql — Multi-backend SQL toolkit
//!
//! Async and sync PostgreSQL + embedded SQLite, built on a
//! sans-IO protocol core. Zero unsafe code in all driver crates.
//!
//! ## Quick start — PostgreSQL (async)
//!
//! ```rust,ignore
//! use bsql::pg::{ConnectConfig, Connection};
//!
//! let config = ConnectConfig::new("127.0.0.1", "myuser")
//!     .database("mydb".into())
//!     .password("secret".into());
//!
//! let mut conn = Connection::connect(&config).await?;
//!
//! // Runtime-SQL query (the typed flagship is `query::<Q>` with `query!`)
//! let result = conn.query_sql("SELECT id, name FROM users").await?;
//! for row in result.iter() {
//!     // Each getter returns `Result<Option<T>, ColumnError>`: `?` propagates a
//!     // classified decode/out-of-range error, the inner `Option` is SQL NULL.
//!     let id: i32 = row.get_i32(0)?.expect("id is NOT NULL");
//!     let name: &str = row.get_str(1)?.expect("name is NOT NULL");
//!     println!("{id}: {name}");
//! }
//!
//! // Parameterized query (SQL injection safe)
//! let row = conn.query_params_one(
//!     "SELECT name FROM users WHERE id = $1",
//!     &(42i32,),
//! ).await?;
//!
//! // Prepared statements (parse once, execute many)
//! let stmt = conn.prepare("INSERT INTO users(name) VALUES ($1)").await?;
//! conn.execute_prepared(&stmt, &("alice",)).await?;
//! conn.execute_prepared(&stmt, &("bob",)).await?;
//! conn.close_statement(stmt).await?;
//!
//! // Transactions (tier-1 safety: closure scope = transaction boundary)
//! conn.transaction(|tx| async {
//!     tx.execute_sql("INSERT INTO log VALUES ('start')").await?;
//!     tx.execute_sql("UPDATE counter SET n = n + 1").await?;
//!     Ok(()) // → COMMIT. Err → ROLLBACK.
//! }).await?;
//! ```
//!
//! ## Quick start — PostgreSQL (sync)
//!
//! ```rust,ignore
//! use bsql::pg_sync::{ConnectConfig, Connection, SslMode};
//!
//! let config = ConnectConfig::new("127.0.0.1", "myuser")
//!     .database("mydb".into())
//!     .ssl_mode(SslMode::Disable);
//!
//! let mut conn = Connection::connect(&config)?;
//! let result = conn.query_sql("SELECT 1 + 1 AS answer")?;
//! assert_eq!(result.get(0).expect("one row").get_i32(0), Ok(Some(2)));
//! conn.close()?;
//! ```
//!
//! ## Quick start — SQLite
//!
//! ```rust,ignore
//! use bsql::sqlite::Connection;
//!
//! let conn = Connection::open_in_memory()?;
//! conn.execute("CREATE TABLE t(v INTEGER)")?;
//! conn.transaction(|tx| {
//!     tx.execute("INSERT INTO t VALUES (42)")?;
//!     Ok(())
//! })?;
//! let row = conn.query_one("SELECT v FROM t")?;
//! assert_eq!(row.get_i64(0), Some(42));
//! ```
//!
//! ## The compile-checked `query!` flagship (feature `macros`)
//!
//! With `features = ["macros"]` and `bsql-build` in `[build-dependencies]`
//! (plus a one-line `build.rs` calling `bsql_build::emit("migrations")`),
//! `query!` types SQL at build time against the schema replayed from the
//! consumer's migration files. A wrong column or type is a compile error.
//!
//! ```rust,ignore
//! // Emits the `UsersById` record + the `UsersByIdQuery` carrier, typed
//! // against `migrations/`. `SELECT nope FROM users` would not compile.
//! bsql::query!(UsersById, "SELECT id, email FROM users WHERE id = $1");
//! ```
//!
//! The carrier implements the re-exported `TypedQuery`; a driver's typed
//! entry points execute it and return a `Rows` of decoded records.
//!
//! ## Architecture
//!
//! ```text
//! bsql-postgres-proto  — sans-IO wire protocol + session engine (no_std + alloc)
//! bsql-postgres-core   — engine materializer + types + config + TLS + Rows (shared)
//! bsql-postgres-async  — tokio thin adapter over the engine
//! bsql-postgres-sync   — std::net thin adapter over the engine
//! bsql-sqlite          — embedded SQLite driver (bundled rusqlite)
//! bsql                 — this umbrella facade + query! re-export
//! ```
//!
//! The compile-checked `query!` toolchain is build-time only:
//! `bsql-build` (a `[build-dependencies]` helper) replays a consumer's
//! migration DDL into a schema catalog, and the `bsql-query-macros`
//! proc-macro types each `query!` against it. Neither enters a consumer's
//! runtime binary.
//!
//! ## Safety guarantees
//!
//! - `#![forbid(unsafe_code)]` on all driver crates
//! - `Row` is `Send + Sync + 'static` (Arc-shared arena, 16 bytes)
//! - NULL is `Option<NonZeroU32>` — compiler enforces handling
//! - `PreparedStatement` consumed by `close_statement()` — no use-after-close
//! - Transactions are closure-scoped — no forgotten commits
//! - Passwords zeroized on drop, redacted in Debug output

#[cfg(feature = "postgres-async")]
pub mod pg {
    //! Async PostgreSQL driver (tokio).
    pub use bsql_postgres_async::*;
}

#[cfg(feature = "postgres-sync")]
pub mod pg_sync {
    //! Sync PostgreSQL driver (std::net).
    pub use bsql_postgres_sync::*;
}

#[cfg(feature = "sqlite")]
pub mod sqlite {
    //! Embedded SQLite driver.
    pub use bsql_sqlite::*;
}

#[cfg(feature = "testkit")]
pub mod testkit {
    //! Deterministic in-memory fake PostgreSQL for testing driver code with no
    //! network. Gated OFF by default so a production build never pulls it.
    pub use bsql_testkit::*;
}

// ════════════════════════════════════════════════════════════════════
// Compile-checked query API (feature `macros`)
// ════════════════════════════════════════════════════════════════════
//
// This is the whole reason a consumer needs ONE crate: `bsql::query!`
// validates SQL against the migration-replayed schema at build time and
// emits typed records + a content-addressed prepared query. The macro's
// expansion names ONLY `::bsql::__rt::` paths, so a consumer depends on
// `bsql` (with `features = ["macros"]`) and nothing else at compile time —
// no hand-wiring of the proc-macro and the sans-IO decode crate.

/// The compile-checked, schema-typed query macro.
///
/// `query!(Name, "<SQL>")` types the SQL against the schema replayed from
/// the consumer's migration DDL (via the catalog `bsql-build` generates in
/// the consumer's `build.rs`) and emits two typed-record types plus their
/// decoders, the const wire artifact, and the [`TypedQuery`] execution
/// bridge. An unknown table/column — or any query that does not type-check
/// — is a `compile_error!`.
///
/// The expansion references only the runtime primitives re-exported here,
/// so a consumer depending on `bsql` with `features = ["macros"]` needs no
/// other dependency to reach the flagship. See the module-level examples
/// for the required one-line `build.rs`.
#[cfg(feature = "macros")]
pub use bsql_query_macros::query;

/// The typed-query execution bridge, the const-checked prepared-query
/// artifact, its build-time fingerprint, and the classified decode error —
/// the user-facing types a `query!`-generated carrier is built from. A
/// driver's typed-query entry points execute a `query!` carrier through
/// these.
#[cfg(feature = "macros")]
pub use bsql_postgres_proto::{DecodeError, PreparedQuery, QueryFingerprint, TypedQuery};

/// Dependency-free bsql-native types a `query!` record field carries for a
/// PostgreSQL `uuid` / `timestamptz` / `timestamp` / `date` / `time` /
/// `interval` / `json` / `jsonb` / `numeric` column — the always-available core
/// that lets `query!` type a real schema without pulling in `uuid` / `chrono` /
/// `time` / `serde_json` / `rust_decimal`. [`Uuid`] round-trips its hyphenated
/// hex text; [`Timestamptz`] exposes an exact Unix-epoch conversion;
/// [`Timestamp`] is the zone-less peer; [`Date`] renders ISO-8601 via a
/// dependency-free Gregorian conversion; [`Time`] is a microsecond time of day;
/// [`Interval`] keeps months / days / microseconds separate; [`Json`] /
/// [`Jsonb`] surface the document's UTF-8 text verbatim; [`Numeric`] is an
/// exact, arbitrary-precision decimal that round-trips its text form. To decode
/// straight into an external crate's type instead, register a build-time
/// external-type bridge — bsql forces no dependency.
#[cfg(feature = "macros")]
pub use bsql_postgres_proto::{
    Date, DateParseError, Interval, Json, Jsonb, Numeric, NumericParseError, Time, TimeParseError,
    Timestamp, Timestamptz, Uuid, UuidParseError,
};

/// Bounded / streaming typed result containers a driver's typed-query
/// entry points return: [`Rows`] holds one query's decoded rows;
/// [`RowsBuilder`] is its prebuffer.
#[cfg(feature = "macros")]
pub use bsql_postgres_core::{Rows, RowsBuilder};

/// Runtime support the `query!` expansion names.
///
/// NOT a stable API and NOT for direct use — every item here exists solely
/// so the code `query!` emits (`::bsql::__rt::...`) resolves through the
/// single `bsql` dependency. The set is exactly the sans-IO decode / wire
/// primitives the macro references: the decode cell + format markers, the
/// raw-row reader, the classified decode error, the fingerprint / prepared
/// / typed-query traits, the OID and query-budget constants, and the
/// `wire_pin!` footprint guard.
#[cfg(feature = "macros")]
#[doc(hidden)]
pub mod __rt {
    pub use bsql_postgres_proto::wire_pin;
    pub use bsql_postgres_proto::{
        BinaryFmt, Cell, ColCellAt, DataRowRef, Date, DecodeError, Interval, Json, Jsonb, Numeric,
        PreparedQuery, QueryFingerprint, Time, Timestamp, Timestamptz, TypedQuery, Uuid, oids,
        prepared, query_budget,
    };
}

// ════════════════════════════════════════════════════════════════════
// Schema-per-test isolation (feature `test-harness`)
// ════════════════════════════════════════════════════════════════════
//
// `#[bsql::test]` over a test taking a single connection runs it in its own
// freshly-created PostgreSQL schema and drops that schema on exit — even on
// panic. Two such tests run in parallel against the same server without
// interfering, because each connection's connect-time `search_path` pins every
// unqualified name to its own schema. It works over BOTH drivers: an `async fn`
// test (`conn: &mut bsql::pg::Connection`) rides the async driver behind a
// per-test tokio runtime, and a plain `fn` test
// (`conn: &mut bsql::pg_sync::Connection`) rides the blocking driver with no
// runtime. Gated OFF by default: a production build never pulls the runtime or
// the harness.

/// Run an integration test in its own isolated PostgreSQL schema — over the
/// async OR the sync driver.
///
/// Applied to an `async fn` taking a single `conn: &mut bsql::pg::Connection`
/// (runs over the async driver) …
///
/// ```rust,ignore
/// #[bsql::test]
/// async fn creates_a_user(conn: &mut bsql::pg::Connection) {
///     conn.execute_sql("CREATE TABLE users (id int)").await.unwrap();
///     // ... assertions, all inside an isolated schema ...
/// }   // schema auto-dropped, even if the test panics
/// ```
///
/// … or to a plain `fn` taking a single `conn: &mut bsql::pg_sync::Connection`
/// (runs over the blocking driver, no runtime):
///
/// ```rust,ignore
/// #[bsql::test]
/// fn creates_a_user(conn: &mut bsql::pg_sync::Connection) {
///     conn.execute_sql("CREATE TABLE users (id int)").unwrap();
/// }   // schema auto-dropped, even if the test panics
/// ```
///
/// The `async`-ness of the function selects the driver; the connection argument
/// type must match (an `async fn` taking a sync connection, or a plain `fn`
/// taking an async connection, is a compile error, never a mis-expansion).
///
/// The harness connects to the server named by the `BSQL_TEST_DSN` environment
/// variable (a *test* variable, deliberately distinct from an application's
/// `DATABASE_URL` — this harness creates and drops schemas), creates a unique
/// schema, hands the body a connection pinned to it, and drops the schema on
/// exit. An unset `BSQL_TEST_DSN` is a loud panic naming the variable, never a
/// silent skip. Other attributes on the function (`#[ignore]`, `#[should_panic]`,
/// …) are forwarded to the generated `#[test]`.
#[cfg(feature = "test-harness")]
pub use bsql_query_macros::test;

/// Runtime support the `#[bsql::test]` expansion names.
///
/// NOT a stable API and NOT for direct use — the `#[bsql::test]` expansion
/// resolves `::bsql::__test_rt::run_schema_isolated_test` (async body) or
/// `::bsql::__test_rt::run_schema_isolated_test_sync` (sync body) through the
/// single `bsql` dependency. Compiled only under the non-default `test-harness`
/// feature.
#[cfg(feature = "test-harness")]
#[doc(hidden)]
pub mod __test_rt {
    pub use crate::test_harness::{
        DSN_ENV, resolve_base_config, run_schema_isolated_test, run_schema_isolated_test_sync,
        schema_exists, schema_exists_sync,
    };
}

#[cfg(feature = "test-harness")]
mod test_harness;
