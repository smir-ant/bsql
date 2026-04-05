#![forbid(unsafe_code)]

//! # bsql — Safe SQL for Rust
//!
//! **If it compiles, the SQL is correct.**
//!
//! bsql validates every SQL query against a real database at compile time.
//! No runtime, no async — just safe, fast, synchronous SQL.
//!
//! ## Quick Start
//!
//! ```toml
//! [dependencies]
//! bsql = "0.17"
//! ```
//!
//! ```rust,ignore
//! use bsql::Pool;
//!
//! fn main() -> Result<(), bsql::BsqlError> {
//!     let pool = Pool::connect("postgres://user:pass@localhost/mydb")?;
//!
//!     let id = 1i32;
//!     let users = bsql::query!(
//!         "SELECT id, login, active FROM users WHERE id = $id: i32"
//!     ).fetch(&pool)?;
//!
//!     let user = &users[0];
//!     // user.id: i32, user.login: String, user.active: bool
//!     println!("{}: active={}", user.login, user.active);
//!     Ok(())
//! }
//! ```
//!
//! ## Two methods — that's it
//!
//! | Method | Returns | Use |
//! |--------|---------|-----|
//! | `.fetch(&pool)` | `Vec<Row>` | SELECT queries |
//! | `.run(&pool)` | `u64` | INSERT, UPDATE, DELETE |
//!
//! Also: `fetch_one`, `fetch_optional`, `fetch_stream`, `for_each`, `defer` (for transactions).
//!
//! ## Escape hatch
//!
//! For rare cases requiring dynamic SQL (dynamic table names, pivots, DDL):
//!
//! ```rust,ignore
//! let rows = pool.raw_query("SELECT * FROM pg_tables LIMIT 5")?;
//! pool.raw_execute("CREATE INDEX CONCURRENTLY idx ON users (email)")?;
//! ```
//!
//! `raw_query` / `raw_execute` bypass compile-time validation entirely.
//! Use `query!` for everything else.

// Re-export the query! macro and attribute macros
pub use bsql_macros::pg_enum;
pub use bsql_macros::query;
pub use bsql_macros::sort;

// Re-export all runtime types
pub use bsql_core::error::{self, BsqlError, BsqlResult};
// Used by generated code from `bsql::query!`. Not part of the user-facing API.
#[doc(hidden)]
pub use bsql_core::executor::{Executor, OwnedResult};
pub use bsql_core::listener::{Listener, Notification};
pub use bsql_core::pool::{Pool, PoolBuilder, PoolStatus, RawRow};

/// A connection borrowed from the pool via [`Pool::acquire()`].
///
/// Most users should use `Pool` directly (query methods acquire and release
/// connections automatically). `PoolConnection` is for advanced use cases
/// where you need to hold a connection across multiple queries without a
/// transaction.
#[doc(hidden)]
pub use bsql_core::pool::PoolConnection;
pub use bsql_core::stream::QueryStream;
pub use bsql_core::transaction::{IsolationLevel, Transaction};

// SQLite pool, transaction, and streaming
#[cfg(feature = "sqlite")]
pub use bsql_core::{SqlitePool, SqliteStreamingQuery, SqliteTransaction};

// Re-export driver types used by generated code
#[doc(hidden)]
pub use bsql_core::driver;

// Re-export SQLite driver types used by generated code
#[cfg(feature = "sqlite")]
#[doc(hidden)]
pub use bsql_core::driver_sqlite;
