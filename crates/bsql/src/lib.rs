#![forbid(unsafe_code)]

//! # bsql — Safe SQL for Rust
//!
//! **If it compiles, the SQL is correct.**
//!
//! bsql is a proc-macro library that validates every SQL query against a real
//! PostgreSQL instance at compile time. There is no `query()` function. There is
//! no escape hatch. There is `query!` — validated, typed, checked. If the binary
//! is produced, every SQL query in it is correct.
//!
//! ## Quick Start
//!
//! ```toml
//! [dependencies]
//! bsql = "0.14"
//! tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
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
//! #[tokio::main]
//! async fn main() -> Result<(), BsqlError> {
//!     let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;
//!
//!     // Every query is validated against the real database at compile time.
//!     // If this compiles, the SQL is correct — tables, columns, types, all checked.
//!     //
//!     //   let id = 1i32;
//!     //   let users = bsql::query!(
//!     //       "SELECT id, login, active FROM users WHERE id = $id: i32"
//!     //   ).fetch(&pool).await?;
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
//! ## No escape hatch
//!
//! There is no `bsql::query()` function. There is no `raw_sql()`. There is no
//! way to execute unchecked SQL through bsql. If you need unchecked SQL, use
//! `tokio-postgres` directly. bsql will not become the thing it replaces.
//!
//! ## Execution methods
//!
//! **Simple API** (recommended):
//!
//! | Method | Returns | Use when |
//! |--------|---------|----------|
//! | `.fetch(&pool)` | `Vec<T>` | SELECT queries |
//! | `.run(&pool)` | `u64` (affected rows) | INSERT/UPDATE/DELETE |
//!
//! **Full API** (power users):
//!
//! | Method | Returns | Use when |
//! |--------|---------|----------|
//! | `.fetch_one(&pool)` | `T` | Exactly one row expected |
//! | `.fetch_all(&pool)` | `Vec<T>` | Same as `.fetch()` |
//! | `.fetch_optional(&pool)` | `Option<T>` | Zero or one row |
//! | `.fetch_stream(&pool)` | `impl Stream<Item = Result<T>>` | Large result sets |
//! | `.execute(&pool)` | `u64` | Same as `.run()` |
//! | `.defer(&tx)` | `()` | Buffer writes in a transaction pipeline |

// Re-export the query! macro and attribute macros
pub use bsql_macros::pg_enum;
pub use bsql_macros::query;
pub use bsql_macros::sort;

// Re-export all runtime types
pub use bsql_core::error::{self, BsqlError, BsqlResult};
pub use bsql_core::executor::{Executor, OwnedResult};
pub use bsql_core::listener::{Listener, Notification};
pub use bsql_core::pool::{Pool, PoolBuilder, PoolConnection, PoolStatus};
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
