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
//! bsql = "0.1"
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
//!     //   let user = bsql::query!(
//!     //       "SELECT id, login, active FROM users WHERE id = $id: i32"
//!     //   ).fetch_one(&pool).await?;
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
//! | Method | Returns | Error if |
//! |--------|---------|----------|
//! | `.fetch_one(&pool)` | `T` | 0 rows, or 2+ rows |
//! | `.fetch_all(&pool)` | `Vec<T>` | never (empty = empty vec) |
//! | `.fetch_optional(&pool)` | `Option<T>` | 2+ rows |
//! | `.fetch_stream(&pool)` | `impl Stream<Item = Result<T>>` | never |
//! | `.execute(&pool)` | `u64` (affected rows) | never |

// Re-export the query! macro and pg_enum attribute macro
pub use bsql_macros::pg_enum;
pub use bsql_macros::query;

// Re-export all runtime types
pub use bsql_core::error::{self, BsqlError, BsqlResult};
pub use bsql_core::executor::Executor;
pub use bsql_core::listener::{Listener, Notification};
pub use bsql_core::pool::{Pool, PoolBuilder, PoolConnection, PoolStatus};
pub use bsql_core::stream::QueryStream;
pub use bsql_core::transaction::Transaction;
pub use bsql_core::types;

// Re-export the postgres_types crate so pg_enum generated code can access it
// via `::bsql_core::pg_types::*` paths.
#[doc(hidden)]
pub use bsql_core::pg_types;

// Re-export Stream trait so users can consume QueryStream without
// adding futures-core as a direct dependency.
pub use futures_core::Stream;
