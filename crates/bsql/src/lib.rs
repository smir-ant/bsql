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
//! // Simple query
//! let result = conn.query("SELECT id, name FROM users").await?;
//! for row in &result.rows {
//!     let id: i32 = row.get_i32(0).unwrap();
//!     let name: &str = row.get_str(1).unwrap();
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
//!     tx.execute("INSERT INTO log VALUES ('start')").await?;
//!     tx.execute("UPDATE counter SET n = n + 1").await?;
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
//! let result = conn.query("SELECT 1 + 1 AS answer")?;
//! assert_eq!(result.rows[0].get_i32(0), Some(2));
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
//! ## Architecture
//!
//! ```text
//! bsql-pg-proto        — sans-IO wire protocol state machine (no_std)
//! bsql-postgres-core   — engine materialiser + types + config + SSL (shared)
//! bsql-postgres-async  — tokio thin adapter (~550 LoC)
//! bsql-postgres-sync   — std::net thin adapter (~560 LoC)
//! bsql-sqlite          — embedded SQLite driver (~340 LoC)
//! bsql                 — umbrella re-export crate
//! ```
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
