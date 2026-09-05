#![forbid(unsafe_code)]

//! # bsql — Multi-backend SQL toolkit
//!
//! Async and sync PostgreSQL + embedded SQLite, built on a
//! sans-IO protocol core. Zero unsafe code in all driver crates.
//!
//! ## Quick start — PostgreSQL (async)
//!
//! ```no_run
//! # async fn example() -> Result<(), bsql::pg::DriverError> {
//! use bsql::pg::{ConnectConfig, Connection};
//!
//! let config = ConnectConfig::new("127.0.0.1", "myuser")
//!     .database("mydb")
//!     .password("secret");
//!
//! let mut conn = Connection::connect(&config).await?;
//!
//! // Runtime-SQL query (the typed flagship is `query::<Q>` with `query!`)
//! let result = conn.query_raw("SELECT id, name FROM users").await?;
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
//! conn.transaction(async |tx| {
//!     tx.execute_raw("INSERT INTO log VALUES ('start')").await?;
//!     tx.execute_raw("UPDATE counter SET n = n + 1").await?;
//!     Ok(()) // → COMMIT. Err → ROLLBACK.
//! }).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Quick start — PostgreSQL (sync)
//!
//! ```no_run
//! # fn main() -> Result<(), bsql::pg_sync::DriverError> {
//! use bsql::pg_sync::{ConnectConfig, Connection, SslMode};
//!
//! let config = ConnectConfig::new("127.0.0.1", "myuser")
//!     .database("mydb")
//!     .ssl_mode(SslMode::Disable);
//!
//! let mut conn = Connection::connect(&config)?;
//! let result = conn.query_raw("SELECT 1 + 1 AS answer")?;
//! assert_eq!(result.get(0).expect("one row").get_i32(0), Ok(Some(2)));
//! conn.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Quick start — SQLite
//!
//! ```
//! # fn main() -> Result<(), bsql::sqlite::SqliteError> {
//! use bsql::sqlite::Connection;
//!
//! let mut conn = Connection::open_in_memory()?;
//! conn.execute_raw("CREATE TABLE t(v INTEGER)")?;
//! conn.transaction(|tx| {
//!     tx.execute_raw("INSERT INTO t VALUES (42)")?;
//!     Ok(())
//! })?;
//! // Dynamic (raw-SQL) verbs carry the `_raw` suffix; reads are classified.
//! let row = conn.query_one_raw("SELECT v FROM t")?;
//! assert_eq!(row.get::<i64>(0)?, 42);
//! # Ok(())
//! # }
//!
//! // The compile-checked `query!` flagship runs against SQLite too (feature
//! // `sqlite`): the bare `query::<Q>` / `query_one::<Q>` / … verbs decode into
//! // typed records, verifying each value's storage class at runtime.
//! //   bsql::query!(Val, "SELECT v FROM t");
//! //   let vals = conn.query::<ValQuery>(&[])?;   // TypedRows<ValQuery>
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

// bsql's footprint pins (in the PostgreSQL / SQLite crates below) assert exact
// `size_of` / `align_of` values computed for 64-bit pointers; on a non-64-bit
// target they fail as a wall of confusing `E0080` "FOOTPRINT DRIFT" panics. This
// one honest line replaces that wall. 64-bit is the only supported width
// (i686 / wasm32 / 32-bit ARM are unrequested and unsupported); 64-bit builds are
// unaffected.
#[cfg(not(target_pointer_width = "64"))]
compile_error!("bsql requires a 64-bit target; the footprint pins assume 64-bit pointers");

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
// N+1 detection — ONE cross-backend report type (feature `n1-detect`)
// ════════════════════════════════════════════════════════════════════

// The N+1 query report is now a SINGLE nominal type — `bsql_common::N1Report` —
// that every backend's `conn.n1_report()` returns. Re-exported here as the
// canonical `bsql::N1Report` / `bsql::N1Tracker`, sourced from whichever backend
// is enabled (all are the same type, so the cascade's choice is immaterial). A
// consumer can write ONE `fn handle(r: &bsql::N1Report)` over reports from the
// async PostgreSQL driver, the sync PostgreSQL driver, AND the SQLite driver —
// the "backend-agnostic N+1 handling" the docs promise, now a compiler fact
// rather than two field-identical-but-distinct types.
#[cfg(all(feature = "n1-detect", feature = "postgres-async"))]
pub use bsql_postgres_async::{N1Report, N1Tracker};
#[cfg(all(feature = "n1-detect", feature = "postgres-sync", not(feature = "postgres-async")))]
pub use bsql_postgres_sync::{N1Report, N1Tracker};
#[cfg(all(
    feature = "n1-detect",
    feature = "sqlite",
    not(feature = "postgres-async"),
    not(feature = "postgres-sync")
))]
pub use bsql_sqlite::{N1Report, N1Tracker};

// COMPILE-TIME proof that `N1Report` is now ONE nominal type across every
// backend. Fn-pointer coercions type-check ONLY if all four paths resolve to the
// SAME `bsql_common::N1Report` (fn-pointer argument types are INVARIANT, so the
// signatures must match exactly — not merely field-identical). Before the shared
// leaf crate, `bsql::pg::N1Report` and `bsql::sqlite::N1Report` were two DISTINCT
// nominal types and the coercions below would be `E0308`. Checked on every build
// with every backend AND the detector enabled (`--features n1-detect`), so a
// regression that re-forks the type turns the umbrella's own build red. (A
// `#[test]` here would resolve to the crate's own `#[bsql::test]` attribute, so
// the proof is a `const _` — which is also stronger: it is a compile-time fact,
// not a runtime assertion.)
#[cfg(all(
    feature = "n1-detect",
    feature = "postgres-async",
    feature = "postgres-sync",
    feature = "sqlite"
))]
const _: () = {
    fn the_one_type(_: &sqlite::N1Report) {}
    // Each coercion binds `the_one_type` (typed for the SQLite report) to a
    // fn-pointer typed for another backend's report; it compiles only if the two
    // report types are identical. `the_one_type` is thus USED (no dead code).
    let _async_pg: fn(&pg::N1Report) = the_one_type;
    let _sync_pg: fn(&pg_sync::N1Report) = the_one_type;
    let _umbrella: fn(&N1Report) = the_one_type;
};

// ════════════════════════════════════════════════════════════════════
// Cross-backend error classification (any backend feature)
// ════════════════════════════════════════════════════════════════════

/// A backend-agnostic classification VIEW over a driver error, so a
/// cross-backend consumer can branch on `err.is_unique_violation()` identically
/// whether the error came from PostgreSQL or SQLite.
///
/// Implemented for `bsql::pg::DriverError` (the same type as
/// `bsql::pg_sync::DriverError`) and `bsql::sqlite::SqliteError`. It is
/// ZERO-COST: every accessor is a cheap match layered over each backend's own
/// inherent predicates — no new error variant, no allocation, no conversion. The
/// two backends encode a constraint class very differently — PostgreSQL as a
/// 5-character SQLSTATE, SQLite as a numeric extended result code — and this
/// trait maps the COMMON classes onto one vocabulary. For example
/// `is_unique_violation()` is SQLSTATE `23505` on PostgreSQL and
/// `SQLITE_CONSTRAINT_UNIQUE` **or** `SQLITE_CONSTRAINT_PRIMARYKEY` on SQLite —
/// both, because PostgreSQL's `23505` spans a duplicate UNIQUE index and a
/// duplicate PRIMARY KEY, which SQLite splits into two codes. A class the backend
/// did not report maps to `false` / `None`, never to a wrong classification.
///
/// This is a VIEW, not a replacement: each backend keeps its own richer error
/// (`DriverError` carries the full SQLSTATE + message; `SqliteError` carries the
/// extended code), and this trait exposes only the portable common denominator.
#[cfg(any(feature = "postgres-async", feature = "postgres-sync", feature = "sqlite"))]
pub trait BackendError {
    /// The 5-character PostgreSQL SQLSTATE, if this backend carries one.
    /// `Some` for a PostgreSQL server error; always `None` for SQLite (which
    /// classifies by numeric result code, not SQLSTATE — use the boolean
    /// predicates for a portable class check).
    fn sqlstate(&self) -> Option<&str>;
    /// A UNIQUE / PRIMARY KEY duplicate (PostgreSQL `23505`; SQLite
    /// `SQLITE_CONSTRAINT_UNIQUE` or `SQLITE_CONSTRAINT_PRIMARYKEY`).
    fn is_unique_violation(&self) -> bool;
    /// A NOT NULL violation (PostgreSQL `23502`; SQLite
    /// `SQLITE_CONSTRAINT_NOTNULL`).
    fn is_not_null_violation(&self) -> bool;
    /// A FOREIGN KEY violation (PostgreSQL `23503`; SQLite
    /// `SQLITE_CONSTRAINT_FOREIGNKEY`).
    fn is_foreign_key_violation(&self) -> bool;
    /// A CHECK violation (PostgreSQL `23514`; SQLite `SQLITE_CONSTRAINT_CHECK`).
    fn is_check_violation(&self) -> bool;
    /// A typed `query_one` that matched MORE than one row (both backends'
    /// `TooManyRows`).
    fn is_too_many_rows(&self) -> bool;
    /// A one-row read that expected a row but got NONE (PostgreSQL
    /// `DriverError::NoRows`; SQLite `SqliteError::NoRows`) — lets a generic
    /// consumer treat an empty `fetch_one` identically on both backends.
    fn is_no_rows(&self) -> bool;
    /// The connection / database handle is no longer usable — RECONNECT (or, on
    /// SQLite, reopen) rather than retrying on the same handle. Distinct from a
    /// per-query error the connection survives.
    ///
    /// The concept is honestly DIFFERENT across backends but reads the SAME:
    /// - **PostgreSQL** — a networked connection that DIED mid-operation: a
    ///   dropped socket / EOF / reset, a fatal liveness-deadline (a silently
    ///   vanished peer), or a connection-broken server error (the `08` class,
    ///   `57P01`/`57P02`/`57P03` admin/crash shutdown). Deliberately FALSE for a
    ///   `57014` `query_canceled` (a `statement_timeout` or `CancelToken` cancel
    ///   leaves the connection reusable) and for every ordinary server error.
    ///   Forwards to `DriverError::is_disconnect`.
    /// - **SQLite** — IN-PROCESS, so it never network-disconnects; the analogue is
    ///   a BROKEN HANDLE/FILE (`SQLITE_IOERR` / `SQLITE_CORRUPT` /
    ///   `SQLITE_CANTOPEN` / `SQLITE_NOTADB`), whose recovery is a fresh handle.
    ///   FALSE for a `SQLITE_BUSY` retry, an interrupt, and every constraint /
    ///   type error. Forwards to `SqliteError::is_disconnect`.
    ///
    /// So a cross-backend consumer's reconnect/reopen logic is ONE decision on
    /// both backends.
    fn is_disconnect(&self) -> bool;
}

// Name `DriverError` from whichever PostgreSQL driver is enabled. Both drivers
// re-export the SAME `bsql_postgres_core::DriverError`, so binding one path (async
// preferred, else sync) gives ONE canonical type and ONE impl — matching on both
// paths would be a duplicate-impl error when both drivers are on.
#[cfg(feature = "postgres-async")]
use bsql_postgres_async::DriverError as PgDriverError;
#[cfg(all(feature = "postgres-sync", not(feature = "postgres-async")))]
use bsql_postgres_sync::DriverError as PgDriverError;

#[cfg(any(feature = "postgres-async", feature = "postgres-sync"))]
impl BackendError for PgDriverError {
    fn sqlstate(&self) -> Option<&str> {
        match self {
            PgDriverError::Db(e) => Some(e.code()),
            _ => None,
        }
    }
    fn is_unique_violation(&self) -> bool {
        matches!(self, PgDriverError::Db(e) if e.is_unique_violation())
    }
    fn is_not_null_violation(&self) -> bool {
        matches!(self, PgDriverError::Db(e) if e.is_not_null_violation())
    }
    fn is_foreign_key_violation(&self) -> bool {
        matches!(self, PgDriverError::Db(e) if e.is_foreign_key_violation())
    }
    fn is_check_violation(&self) -> bool {
        matches!(self, PgDriverError::Db(e) if e.is_check_violation())
    }
    fn is_too_many_rows(&self) -> bool {
        matches!(self, PgDriverError::TooManyRows)
    }
    fn is_no_rows(&self) -> bool {
        matches!(self, PgDriverError::NoRows)
    }
    fn is_disconnect(&self) -> bool {
        // Fully-qualified to the inherent method (which classifies the full
        // variant set), so this forwards rather than recursing into the trait.
        PgDriverError::is_disconnect(self)
    }
}

#[cfg(feature = "sqlite")]
impl BackendError for bsql_sqlite::SqliteError {
    fn sqlstate(&self) -> Option<&str> {
        // SQLite classifies by numeric extended result code, not SQLSTATE. The
        // boolean predicates below carry the portable class check.
        None
    }
    fn is_unique_violation(&self) -> bool {
        // Inherent method (preferred over this trait method in path resolution),
        // so this forwards rather than recursing.
        bsql_sqlite::SqliteError::is_unique_violation(self)
    }
    fn is_not_null_violation(&self) -> bool {
        bsql_sqlite::SqliteError::is_not_null_violation(self)
    }
    fn is_foreign_key_violation(&self) -> bool {
        bsql_sqlite::SqliteError::is_foreign_key_violation(self)
    }
    fn is_check_violation(&self) -> bool {
        bsql_sqlite::SqliteError::is_check_violation(self)
    }
    fn is_too_many_rows(&self) -> bool {
        matches!(self, bsql_sqlite::SqliteError::TooManyRows)
    }
    fn is_no_rows(&self) -> bool {
        matches!(self, bsql_sqlite::SqliteError::NoRows)
    }
    fn is_disconnect(&self) -> bool {
        // Inherent method (broken-handle codes: IOERR / CORRUPT / CANTOPEN /
        // NOTADB); fully-qualified so it forwards rather than recursing.
        bsql_sqlite::SqliteError::is_disconnect(self)
    }
}

// ════════════════════════════════════════════════════════════════════
// Write-once cross-backend data access (the two BLOCKING drivers)
// ════════════════════════════════════════════════════════════════════
//
// One generic surface over `pg_sync` + `sqlite`, so a data layer written ONCE
// runs on either. Present whenever a BLOCKING backend is enabled; an
// async-only build has no `SyncBackend` (the async driver is unified
// separately). See the module for the shape and the consumer-signature
// contract.

#[cfg(any(feature = "postgres-sync", feature = "sqlite"))]
mod backend;

/// Write-once cross-backend data access over the two BLOCKING drivers.
///
/// [`SyncBackend`] / [`SyncQueries`] are the generic surface a data layer is
/// written against; [`RunsOn`] bridges a `query!` carrier to a concrete backend.
/// Write the flagship consumer shape as `fn f<B>(conn: &mut B) where B:
/// SyncBackend, SomeQuery: RunsOn<B, Params = .., Owned = ..>` — one `RunsOn<B>`
/// bound per distinct `query!` the function runs, no `dyn`, no HRTB.
#[cfg(any(feature = "postgres-sync", feature = "sqlite"))]
pub use backend::{RunsOn, SyncBackend, SyncQueries};

/// The SQLite transaction-guard adapter handed to a generic
/// [`SyncBackend::transaction`] body. Named only in the public associated type
/// `<sqlite::Connection as SyncBackend>::Tx`; a consumer rarely writes it.
#[cfg(feature = "sqlite")]
pub use backend::SqliteTx;

// ════════════════════════════════════════════════════════════════════
// Migration runner — embed macro (any backend)
// ════════════════════════════════════════════════════════════════════
//
// The migration RUNNER (`conn.run_migrations(..)`) is an always-available
// capability on every driver (it adds NO dependency). It applies a consumer's
// migration set to a live database, exactly once, in deterministic order, with
// a ledger + checksum-drift detection + a concurrency lock. See each backend's
// `run_migrations` / `migration_status` / `dry_run_migrations` verbs and the
// `MigrationSource` type (`bsql::pg::MigrationSource`, `bsql::sqlite::MigrationSource`).

/// Expand to the `&'static [(&'static str, &'static str)]` of `(name, sql)`
/// pairs the consumer's `build.rs` baked with `bsql_build::emit_migrations(..)`.
///
/// Hand the result to `conn.run_migrations(..)` for the EMBEDDED source (no
/// filesystem at run time):
///
/// ```rust,ignore
/// // build.rs:  bsql_build::emit_migrations("migrations")?;
/// const MIGRATIONS: &[(&str, &str)] = bsql::embed_migrations!();
/// let report = conn.run_migrations(MIGRATIONS)?;
/// ```
///
/// It `include!`s the file the `emit_migrations` build step generated (via the
/// `BSQL_EMBEDDED_MIGRATIONS` rustc-env channel). Invoking it WITHOUT that
/// build step is a loud compile error naming the missing `build.rs` call —
/// never a silent empty set.
#[macro_export]
macro_rules! embed_migrations {
    () => {
        include!(env!(
            "BSQL_EMBEDDED_MIGRATIONS",
            "bsql::embed_migrations!() requires a build.rs that calls \
             bsql_build::emit_migrations(\"migrations\")"
        ))
    };
}

/// The compile-time SHA-256 schema fingerprint hex string generated by `bsql-build`.
///
/// If any migration changes a table, column, type, or constraint, this fingerprint
/// changes deterministically, enabling instant detection of schema drift between
/// running application binaries and the database.
#[macro_export]
macro_rules! schema_fingerprint {
    () => {
        env!(
            "BSQL_SCHEMA_FINGERPRINT",
            "bsql::schema_fingerprint!() requires a build.rs that calls \
             bsql_build::emit(\"migrations\") or bsql_build::emit_catalog(\"migrations\")"
        )
    };
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

/// Generate a Rust type for every user-defined PostgreSQL type in the
/// consumer's migrations — `enum Mood { Happy, Sad }` from
/// `CREATE TYPE mood AS ENUM ('happy', 'sad')`, with zero derives and no
/// hand-maintained type name. Invoke once, in a module in scope at your
/// `query!` call sites. See [`macro@user_types`] for the full contract.
#[cfg(feature = "macros")]
pub use bsql_query_macros::user_types;

/// The compile-checked binary COPY-in carrier macro.
///
/// `copy!(Name, "table", (col1, col2, …))` validates the target table, its
/// columns, and their types against the SAME build catalog `query!` reads, and
/// emits an uninhabited `Name` carrier implementing [`TypedCopyIn`]. A driver's
/// `copy_in_typed::<Name>(rows)` then bulk-loads `rows` — each a typed tuple
/// matching the columns' Rust types (a `NOT NULL` column is `T`, a nullable
/// column `Option<T>`) — through the fastest, injection-safe-by-construction
/// PGCOPY *binary* path: no text to mis-escape, and the target identifiers are a
/// compile-time constant. An unknown / duplicate / unsupported column is a
/// `compile_error!`.
#[cfg(feature = "macros")]
pub use bsql_query_macros::copy;

/// The typed-query execution bridge, the const-checked prepared-query
/// artifact, its build-time fingerprint, and the classified decode error —
/// the user-facing types a `query!`-generated carrier is built from. A
/// driver's typed-query entry points execute a `query!` carrier through
/// these.
#[cfg(feature = "macros")]
pub use bsql_postgres_proto::{
    DecodeError, ParamsWriter, PreparedQuery, QueryFingerprint, TypedCopyIn, TypedQuery,
};

/// The runtime contract of a Rust `enum` generated from a `CREATE TYPE ... AS
/// ENUM` migration by [`user_types!`], and the enum bind-parameter wrapper.
///
/// A consumer rarely names these directly — [`user_types!`] emits the
/// `impl PgEnum` and inherent `as_label` / `label` methods, and `query!`
/// decodes/binds enum columns through them — but [`PgEnum`] is a real public
/// contract (be generic over generated enums), and [`EnumLabel`] is the type an
/// enum's `as_label()` returns to bind it as a `query!` parameter.
#[cfg(feature = "macros")]
pub use bsql_postgres_proto::{EnumLabel, PgEnum};

/// The runtime contract of a Rust `struct` generated from a
/// `CREATE TYPE ... AS (...)` COMPOSITE migration by [`user_types!`].
///
/// A consumer rarely names this directly — [`user_types!`] emits the
/// `impl PgComposite` (the row-type binary frame decoder) and `query!` decodes
/// composite columns through it — but [`PgComposite`] is a real public contract
/// (be generic over generated composites, or decode a frame directly with
/// [`PgComposite::decode_row`]).
#[cfg(feature = "macros")]
pub use bsql_postgres_proto::PgComposite;

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

/// The bounded / streaming typed result container a driver's typed-query
/// entry points return: [`Rows`] holds one query's decoded rows.
#[cfg(feature = "macros")]
pub use bsql_postgres_core::{safe_ident, safe_table, Rows, SafeIdent, SafeTable};

/// Heterogeneous atomic pipelining (PostgreSQL) — [`Bound`] wraps a `query!`
/// carrier with its params (build one with [`BindExt::bind`],
/// `UserById::bind((7,))`), and a tuple of `Bound`s (arity `1..=16`) satisfies
/// [`Pipeline`], the batch a driver's `pipeline((...))` verb runs in ONE round trip
/// as ONE implicit transaction (all-or-nothing — see `pg::Connection::pipeline`).
/// Gated on `macros` (like [`Rows`]): the types are usable wherever the `query!`
/// flagship is; a driver feature provides the `Connection` to run a batch on.
#[cfg(feature = "macros")]
pub use bsql_postgres_core::{BindExt, Bound, Pipeline};

// `RowsBuilder` is the INTERNAL prebuffer `Rows` is built from — a decode/collect
// seam, not a consumer API. Re-exported (doc-hidden, like its definition) ONLY so
// the query fixture's offline decode + allocation tests can name it through the
// single `bsql` dependency; a consumer never touches it on the happy path.
#[cfg(feature = "macros")]
#[doc(hidden)]
pub use bsql_postgres_core::RowsBuilder;

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
        BinaryFmt, Cell, ColCellAt, CompositeReader, DataRowRef, Date, DecodeError, EnumLabel,
        Interval, Json, Jsonb, Numeric, ParamsWriter, PgComposite, PgEnum, PreparedQuery,
        QueryFingerprint, Time, Timestamp, Timestamptz, TypedCopyIn, TypedQuery, Uuid, oids,
        prepared, query_budget,
    };
}

/// SQLite typed-runtime support the `query!` expansion names.
///
/// NOT a stable API and NOT for direct use — the SQLite half of a `query!`
/// expansion (`::bsql::__rt_sqlite::...`) resolves through the single `bsql`
/// dependency. Present only when BOTH the `macros` and `sqlite` features are on
/// (the macro emits a `SqliteTypedQuery` impl only then), so a PostgreSQL-only
/// or a driverless-macros build compiles no reference to it. The set is exactly
/// the SQLite typed-decode primitives the macro references: the carrier trait,
/// the row-view seam, the classified error, and the per-field read helpers.
#[cfg(all(feature = "macros", feature = "sqlite"))]
#[doc(hidden)]
pub mod __rt_sqlite {
    pub use bsql_sqlite::{
        read_optional, read_required, ColumnSource, SqliteError, SqliteTypedQuery,
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
///     conn.execute_raw("CREATE TABLE users (id int)").await.unwrap();
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
///     conn.execute_raw("CREATE TABLE users (id int)").unwrap();
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
