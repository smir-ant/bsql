//! PostgreSQL wire protocol driver for bsql.
//!
//! `bsql-driver-postgres` is a purpose-built PostgreSQL driver optimized for bsql's
//! architecture: binary protocol only, arena allocation for row data, pipelined
//! extended query protocol, LIFO connection pool with fail-fast semantics.
//!
//! # Design
//!
//! - **Binary protocol only** — numeric types are memcpy, not parsed from ASCII.
//! - **Arena allocation** — all row data from one query shares a single bump allocator.
//! - **Pipelined messages** — Parse+Bind+Execute+Sync in one TCP write.
//! - **Statement cache** — keyed by rapidhash of SQL text. Second query skips Parse.
//! - **LIFO pool** — returns the warmest connection (best PG backend cache locality).
//! - **Fail-fast** — pool exhaustion returns an error immediately, never blocks.
//! - **No unsafe code** — `#![forbid(unsafe_code)]`.
//!
//! # Example
//!
//! ```no_run
//! use bsql_driver_postgres::{Pool, Arena};
//!
//! # async fn example() -> Result<(), bsql_driver_postgres::DriverError> {
//! let pool = Pool::connect("postgres://user:pass@localhost/db").await?;
//! let mut conn = pool.acquire().await?;
//! let mut arena = Arena::new();
//!
//! let hash = bsql_driver_postgres::hash_sql("SELECT $1::int4 + $2::int4 AS sum");
//! let result = conn.query(
//!     "SELECT $1::int4 + $2::int4 AS sum",
//!     hash,
//!     &[&1i32, &2i32],
//!     &mut arena,
//! ).await?;
//!
//! let row = result.row(0, &arena);
//! assert_eq!(row.get_i32(0), Some(3));
//! # Ok(())
//! # }
//! ```
#![forbid(unsafe_code)]
#![deny(clippy::all)]

pub mod arena;
pub mod codec;
pub mod pool;
pub(crate) mod types;

mod auth;
mod conn;
mod proto;
mod stmt_cache;
mod sync_io;
mod sync_conn;
#[cfg(feature = "tls")]
mod tls;
#[cfg(feature = "tls")]
mod tls_sync;

pub use arena::Arena;
pub use codec::Encode;
pub use pool::{Pool, PoolBuilder, PoolGuard, PoolStatus, Transaction};
pub use types::{
    hash_sql, ColumnDesc, Config, Notification, PgDataRow, PrepareResult, QueryResult, Row,
    SimpleRow, SslMode,
};
// Re-export Connection from conn module
pub use conn::Connection;
pub use sync_conn::SyncConnection;

// --- DriverError ---

/// Error type for all bsql-driver-postgres operations.
///
/// Variants cover the four failure modes: I/O, authentication, wire protocol
/// violations, server-reported errors, and pool management.
///
/// # Example
///
/// ```
/// use bsql_driver_postgres::DriverError;
///
/// fn handle_error(err: DriverError) {
///     match err {
///         DriverError::Io(e) => eprintln!("network error: {e}"),
///         DriverError::Auth(msg) => eprintln!("auth failed: {msg}"),
///         DriverError::Protocol(msg) => eprintln!("protocol error: {msg}"),
///         DriverError::Server { code, message, position, .. } => {
///             eprintln!("PG error [{code}]: {message} (pos: {position:?})");
///         }
///         DriverError::Pool(msg) => eprintln!("pool error: {msg}"),
///     }
/// }
/// ```
#[derive(Debug)]
pub enum DriverError {
    /// TCP/TLS I/O failure.
    Io(std::io::Error),
    /// Authentication failure (wrong password, unsupported mechanism, etc.).
    Auth(String),
    /// Wire protocol violation (malformed message, unexpected message type, etc.).
    Protocol(String),
    /// Server-reported error (invalid SQL, constraint violation, etc.).
    Server {
        /// Five-character SQLSTATE code (e.g. "42P01" for undefined table).
        code: Box<str>,
        /// Human-readable error message.
        message: Box<str>,
        /// Optional detail text.
        detail: Option<Box<str>>,
        /// Optional hint text.
        hint: Option<Box<str>>,
        /// Character position in the original query where the error occurred (1-indexed).
        position: Option<u32>,
    },
    /// Connection pool error (exhaustion, misconfiguration).
    Pool(String),
}

impl std::fmt::Display for DriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Auth(msg) => write!(f, "auth error: {msg}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::Server {
                code,
                message,
                detail,
                hint,
                position,
            } => {
                write!(f, "server error [{code}]: {message}")?;
                if let Some(pos) = position {
                    write!(f, " (at position {pos})")?;
                }
                if let Some(d) = detail {
                    write!(f, " DETAIL: {d}")?;
                }
                if let Some(h) = hint {
                    write!(f, " HINT: {h}")?;
                }
                Ok(())
            }
            Self::Pool(msg) => write!(f, "pool error: {msg}"),
        }
    }
}

impl std::error::Error for DriverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for DriverError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn driver_error_display_io() {
        let e = DriverError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        assert!(e.to_string().contains("I/O error"));
        assert!(e.to_string().contains("refused"));
    }

    #[test]
    fn driver_error_display_auth() {
        let e = DriverError::Auth("wrong password".into());
        assert_eq!(e.to_string(), "auth error: wrong password");
    }

    #[test]
    fn driver_error_display_protocol() {
        let e = DriverError::Protocol("unexpected message".into());
        assert_eq!(e.to_string(), "protocol error: unexpected message");
    }

    #[test]
    fn driver_error_display_server() {
        let e = DriverError::Server {
            code: "42P01".into(),
            message: "relation does not exist".into(),
            detail: Some("table was dropped".into()),
            hint: None,
            position: None,
        };
        let s = e.to_string();
        assert!(s.contains("42P01"));
        assert!(s.contains("relation does not exist"));
        assert!(s.contains("table was dropped"));
    }

    #[test]
    fn driver_error_display_server_no_detail() {
        let e = DriverError::Server {
            code: Box::from("23505"),
            message: Box::from("duplicate key"),
            detail: None,
            hint: None,
            position: None,
        };
        assert_eq!(e.to_string(), "server error [23505]: duplicate key");
    }

    #[test]
    fn driver_error_display_server_with_position() {
        let e = DriverError::Server {
            code: Box::from("42601"),
            message: Box::from("syntax error"),
            detail: None,
            hint: None,
            position: Some(8),
        };
        let s = e.to_string();
        assert!(s.contains("(at position 8)"));
    }

    #[test]
    fn driver_error_display_pool() {
        let e = DriverError::Pool("exhausted".into());
        assert_eq!(e.to_string(), "pool error: exhausted");
    }

    #[test]
    fn driver_error_source_io() {
        let inner = std::io::Error::other("test");
        let e = DriverError::Io(inner);
        assert!(std::error::Error::source(&e).is_some());
    }

    #[test]
    fn driver_error_source_non_io() {
        let e = DriverError::Auth("test".into());
        assert!(std::error::Error::source(&e).is_none());
    }

    #[test]
    fn driver_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let e: DriverError = io_err.into();
        assert!(matches!(e, DriverError::Io(_)));
    }

    #[test]
    fn forbid_unsafe_code() {
        // This test exists to document the safety guarantee.
        // The `#![forbid(unsafe_code)]` at the crate root ensures this at compile time.
    }

    // ===============================================================
    // DriverError — extended coverage
    // ===============================================================

    #[test]
    fn driver_error_display_server_all_none() {
        let e = DriverError::Server {
            code: "00000".into(),
            message: "successful completion".into(),
            detail: None,
            hint: None,
            position: None,
        };
        let s = e.to_string();
        assert_eq!(s, "server error [00000]: successful completion");
        // Should NOT contain DETAIL, HINT, or position
        assert!(!s.contains("DETAIL"));
        assert!(!s.contains("HINT"));
        assert!(!s.contains("position"));
    }

    #[test]
    fn driver_error_display_server_detail_only() {
        let e = DriverError::Server {
            code: "23505".into(),
            message: "duplicate key".into(),
            detail: Some("Key (id)=(1) exists.".into()),
            hint: None,
            position: None,
        };
        let s = e.to_string();
        assert!(s.contains("DETAIL: Key (id)=(1) exists."));
        assert!(!s.contains("HINT"));
    }

    #[test]
    fn driver_error_display_server_hint_only() {
        let e = DriverError::Server {
            code: "42601".into(),
            message: "syntax error".into(),
            detail: None,
            hint: Some("check SQL".into()),
            position: None,
        };
        let s = e.to_string();
        assert!(s.contains("HINT: check SQL"));
        assert!(!s.contains("DETAIL"));
    }

    #[test]
    fn driver_error_display_server_position_only() {
        let e = DriverError::Server {
            code: "42601".into(),
            message: "syntax error".into(),
            detail: None,
            hint: None,
            position: Some(15),
        };
        let s = e.to_string();
        assert!(s.contains("(at position 15)"));
    }

    #[test]
    fn driver_error_display_server_all_fields() {
        let e = DriverError::Server {
            code: "42P01".into(),
            message: "relation does not exist".into(),
            detail: Some("table was dropped".into()),
            hint: Some("recreate the table".into()),
            position: Some(42),
        };
        let s = e.to_string();
        assert!(s.contains("[42P01]"));
        assert!(s.contains("relation does not exist"));
        assert!(s.contains("(at position 42)"));
        assert!(s.contains("DETAIL: table was dropped"));
        assert!(s.contains("HINT: recreate the table"));
    }

    #[test]
    fn driver_error_io_preserves_kind() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let e = DriverError::Io(io_err);
        match &e {
            DriverError::Io(inner) => {
                assert_eq!(inner.kind(), std::io::ErrorKind::ConnectionRefused);
            }
            _ => panic!("expected Io variant"),
        }
    }

    #[test]
    fn driver_error_io_timeout() {
        let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "connection timed out");
        let e = DriverError::Io(io_err);
        let s = e.to_string();
        assert!(s.contains("timed out"));
    }

    #[test]
    fn driver_error_io_unexpected_eof() {
        let io_err = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection closed");
        let e: DriverError = io_err.into();
        let s = e.to_string();
        assert!(s.contains("connection closed"));
    }

    #[test]
    fn driver_error_auth_empty() {
        let e = DriverError::Auth(String::new());
        assert_eq!(e.to_string(), "auth error: ");
    }

    #[test]
    fn driver_error_protocol_empty() {
        let e = DriverError::Protocol(String::new());
        assert_eq!(e.to_string(), "protocol error: ");
    }

    #[test]
    fn driver_error_pool_empty() {
        let e = DriverError::Pool(String::new());
        assert_eq!(e.to_string(), "pool error: ");
    }

    #[test]
    fn driver_error_source_protocol_is_none() {
        let e = DriverError::Protocol("test".into());
        assert!(std::error::Error::source(&e).is_none());
    }

    #[test]
    fn driver_error_source_server_is_none() {
        let e = DriverError::Server {
            code: "42601".into(),
            message: "err".into(),
            detail: None,
            hint: None,
            position: None,
        };
        assert!(std::error::Error::source(&e).is_none());
    }

    #[test]
    fn driver_error_source_pool_is_none() {
        let e = DriverError::Pool("test".into());
        assert!(std::error::Error::source(&e).is_none());
    }

    #[test]
    fn driver_error_debug_all_variants() {
        let variants: Vec<DriverError> = vec![
            DriverError::Io(std::io::Error::other("io")),
            DriverError::Auth("auth".into()),
            DriverError::Protocol("proto".into()),
            DriverError::Server {
                code: "00000".into(),
                message: "ok".into(),
                detail: None,
                hint: None,
                position: None,
            },
            DriverError::Pool("pool".into()),
        ];
        for v in &variants {
            let dbg = format!("{v:?}");
            assert!(!dbg.is_empty());
        }
    }
}
