#![forbid(unsafe_code)]

//! # bsql — Safe SQL for Rust
//!
//! **If it compiles, the SQL is correct.**
//!
//! bsql validates every SQL query against a real database at compile time.
//! There is no `query()` function. There is no escape hatch. There is `query!`
//! — validated, typed, checked. If the binary is produced, every SQL query in
//! it is correct.
//!
//! ## Quick Start
//!
//! ```toml
//! [dependencies]
//! bsql = "0.17"
//! ```
//!
//! Set the database URL for compile-time validation:
//! ```bash
//! export BSQL_DATABASE_URL="postgres://user:pass@localhost/mydb"
//! ```
//!
//! Then:
//! ```rust,no_run
//! use bsql::{Pool, BsqlError};
//!
//! fn main() -> Result<(), BsqlError> {
//!     let pool = Pool::connect("postgres://user:pass@localhost/mydb")?;
//!
//!     // Every query is validated against the real database at compile time.
//!     // If this compiles, the SQL is correct — tables, columns, types, all checked.
//!     //
//!     //   let id = 1i32;
//!     //   let users = bsql::query!(
//!     //       "SELECT id, login, active FROM users WHERE id = $id: i32"
//!     //   ).fetch(&pool)?;
//!     //   let user = &users[0];
//!     //
//!     // The result struct has typed fields:
//!     //   user.id: i32, user.login: String, user.active: bool
//!     //   println!("{}: {}", user.id, user.login);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Three methods — that's it
//!
//! | Method | Returns | Use |
//! |--------|---------|-----|
//! | `.fetch(&pool)` | `Vec<Row>` | SELECT queries |
//! | `.run(&pool)` | `u64` | INSERT, UPDATE, DELETE |
//! | `.defer(&tx)` | `()` | Buffer in transaction |
//!
//! Power users: `fetch_one`, `fetch_optional`, `fetch_stream`, `for_each` also available.
//!
//! ## No escape hatch
//!
//! There is no `bsql::query()` function. There is no `raw_sql()`. There is no
//! way to execute unchecked SQL through bsql. If you need unchecked SQL, use
//! a raw PostgreSQL driver directly. bsql will not become the thing it replaces.

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
