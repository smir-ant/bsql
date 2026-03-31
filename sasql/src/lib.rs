#![forbid(unsafe_code)]

//! # sasql — Safe SQL for Rust
//!
//! **If it compiles, the SQL is correct.**
//!
//! sasql is a proc-macro library that validates every SQL query against a real
//! PostgreSQL instance at compile time. There is no `query()` function. There is
//! no escape hatch. There is `query!` — validated, typed, checked. If the binary
//! is produced, every SQL query in it is correct.
//!
//! ## Quick Start
//!
//! ```toml
//! [dependencies]
//! sasql = "0.1"
//! tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
//! ```
//!
//! Set the database URL for compile-time validation:
//! ```bash
//! export SASQL_DATABASE_URL="postgres://user:pass@localhost/mydb"
//! ```
//!
//! Then:
//! ```rust,no_run
//! use sasql::Pool;
//!
//! # async fn example() -> Result<(), sasql::SasqlError> {
//! let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;
//!
//! // Every query is validated against the real database at compile time.
//! // If this compiles, the SQL is correct — tables, columns, types, all checked.
//! let user = sasql::query! {
//!     SELECT id, login, active FROM users WHERE id = $id: i32
//! }.fetch_one(&pool).await?;
//!
//! // The result struct has typed fields:
//! // user.id: i32, user.login: String, user.active: bool
//! println!("{}: {}", user.id, user.login);
//! # Ok(())
//! # }
//! ```
//!
//! ## No escape hatch
//!
//! There is no `sasql::query()` function. There is no `raw_sql()`. There is no
//! way to execute unchecked SQL through sasql. If you need unchecked SQL, use
//! `tokio-postgres` directly. sasql will not become the thing it replaces.
//!
//! ## Execution methods
//!
//! | Method | Returns | Error if |
//! |--------|---------|----------|
//! | `.fetch_one(&pool)` | `T` | 0 rows, or 2+ rows |
//! | `.fetch_all(&pool)` | `Vec<T>` | never (empty = empty vec) |
//! | `.fetch_optional(&pool)` | `Option<T>` | 2+ rows |
//! | `.execute(&pool)` | `u64` (affected rows) | never |

// Re-export the query! macro
pub use sasql_macros::query;

// Re-export all runtime types
pub use sasql_core::error::{self, SasqlError, SasqlResult};
pub use sasql_core::executor::Executor;
pub use sasql_core::pool::{Pool, PoolBuilder, PoolConnection, PoolStatus};
pub use sasql_core::types;
