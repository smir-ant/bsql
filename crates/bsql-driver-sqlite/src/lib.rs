//! SQLite driver for bsql — direct FFI, arena allocation, dedicated-thread pool.
//!
//! `bsql-driver-sqlite` is a purpose-built SQLite driver optimized for bsql's
//! architecture: direct FFI to libsqlite3, arena allocation for row data,
//! statement caching with identity-hashed rapidhash keys, and a dedicated-thread
//! pool with WAL mode and reader/writer split.
//!
//! # Design
//!
//! - **Direct FFI** — thin safe wrappers over `libsqlite3-sys`, no ORM overhead.
//! - **Arena allocation** — all row data from one query shares a single bump allocator.
//! - **Statement cache** — keyed by rapidhash of SQL text. Second query skips prepare.
//! - **Dedicated-thread pool** — one writer thread + N reader threads, crossbeam channels.
//! - **WAL mode** — concurrent readers, single writer, no blocking on reads.
//! - **Fail-fast** — `busy_timeout = 0`, pool exhaustion returns error immediately.
//! - **No tokio dependency** — std threads + crossbeam channels. Async wrapping in bsql-core.

#![deny(clippy::all)]

pub mod codec;
pub mod conn;
pub mod ffi;
pub mod pool;

/// SQLite driver error.
///
/// Variants cover the four failure modes: SQLite C API errors, I/O errors,
/// internal logic errors, and pool management errors.
///
/// # Example
///
/// ```
/// use bsql_driver_sqlite::SqliteError;
///
/// fn handle_error(err: SqliteError) {
///     match err {
///         SqliteError::Sqlite { code, message } => {
///             eprintln!("SQLite error [{code}]: {message}");
///         }
///         SqliteError::Io(e) => eprintln!("I/O error: {e}"),
///         SqliteError::Internal(msg) => eprintln!("internal error: {msg}"),
///         SqliteError::Pool(msg) => eprintln!("pool error: {msg}"),
///     }
/// }
/// ```
#[derive(Debug)]
pub enum SqliteError {
    /// SQLite C API returned an error code.
    Sqlite { code: i32, message: String },
    /// I/O error (file not found, permissions, etc).
    Io(std::io::Error),
    /// Internal protocol/logic error.
    Internal(String),
    /// Pool error (closed, exhausted).
    Pool(String),
}

impl std::fmt::Display for SqliteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite { code, message } => write!(f, "SQLite error [{code}]: {message}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
            Self::Pool(msg) => write!(f, "pool error: {msg}"),
        }
    }
}

impl std::error::Error for SqliteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for SqliteError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Display ---

    #[test]
    fn error_display_sqlite() {
        let e = SqliteError::Sqlite {
            code: 1,
            message: "near syntax error".into(),
        };
        assert_eq!(e.to_string(), "SQLite error [1]: near syntax error");
    }

    #[test]
    fn error_display_sqlite_code_zero() {
        let e = SqliteError::Sqlite {
            code: 0,
            message: "not an error".into(),
        };
        assert_eq!(e.to_string(), "SQLite error [0]: not an error");
    }

    #[test]
    fn error_display_sqlite_negative_code() {
        let e = SqliteError::Sqlite {
            code: -1,
            message: "unknown".into(),
        };
        assert_eq!(e.to_string(), "SQLite error [-1]: unknown");
    }

    #[test]
    fn error_display_sqlite_empty_message() {
        let e = SqliteError::Sqlite {
            code: 19,
            message: String::new(),
        };
        assert_eq!(e.to_string(), "SQLite error [19]: ");
    }

    #[test]
    fn error_display_io() {
        let e = SqliteError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        ));
        assert!(e.to_string().contains("I/O error"));
        assert!(e.to_string().contains("file not found"));
    }

    #[test]
    fn error_display_io_permission_denied() {
        let e = SqliteError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "access denied",
        ));
        assert!(e.to_string().contains("I/O error"));
        assert!(e.to_string().contains("access denied"));
    }

    #[test]
    fn error_display_internal() {
        let e = SqliteError::Internal("null in path".into());
        assert_eq!(e.to_string(), "internal error: null in path");
    }

    #[test]
    fn error_display_internal_empty() {
        let e = SqliteError::Internal(String::new());
        assert_eq!(e.to_string(), "internal error: ");
    }

    #[test]
    fn error_display_internal_unicode() {
        let e = SqliteError::Internal("ошибка в пути".into());
        assert_eq!(e.to_string(), "internal error: ошибка в пути");
    }

    #[test]
    fn error_display_pool() {
        let e = SqliteError::Pool("pool is closed".into());
        assert_eq!(e.to_string(), "pool error: pool is closed");
    }

    #[test]
    fn error_display_pool_empty() {
        let e = SqliteError::Pool(String::new());
        assert_eq!(e.to_string(), "pool error: ");
    }

    // --- source() ---

    #[test]
    fn error_source_io() {
        let inner = std::io::Error::new(std::io::ErrorKind::Other, "test");
        let e = SqliteError::Io(inner);
        assert!(std::error::Error::source(&e).is_some());
    }

    #[test]
    fn error_source_sqlite_is_none() {
        let e = SqliteError::Sqlite {
            code: 1,
            message: "err".into(),
        };
        assert!(std::error::Error::source(&e).is_none());
    }

    #[test]
    fn error_source_internal_is_none() {
        let e = SqliteError::Internal("test".into());
        assert!(std::error::Error::source(&e).is_none());
    }

    #[test]
    fn error_source_pool_is_none() {
        let e = SqliteError::Pool("test".into());
        assert!(std::error::Error::source(&e).is_none());
    }

    // --- From<io::Error> ---

    #[test]
    fn error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        let e: SqliteError = io_err.into();
        assert!(matches!(e, SqliteError::Io(_)));
    }

    #[test]
    fn error_from_io_preserves_kind() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let e: SqliteError = io_err.into();
        match e {
            SqliteError::Io(inner) => {
                assert_eq!(inner.kind(), std::io::ErrorKind::PermissionDenied);
            }
            _ => panic!("expected Io variant"),
        }
    }

    #[test]
    fn error_from_io_preserves_message() {
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "custom message");
        let e: SqliteError = io_err.into();
        assert!(e.to_string().contains("custom message"));
    }

    // --- Debug ---

    #[test]
    fn error_debug_sqlite() {
        let e = SqliteError::Sqlite {
            code: 1,
            message: "err".into(),
        };
        let dbg = format!("{e:?}");
        assert!(dbg.contains("Sqlite"));
        assert!(dbg.contains("code: 1"));
    }

    #[test]
    fn error_debug_io() {
        let e = SqliteError::Io(std::io::Error::new(std::io::ErrorKind::Other, "boom"));
        let dbg = format!("{e:?}");
        assert!(dbg.contains("Io"));
    }

    #[test]
    fn error_debug_internal() {
        let e = SqliteError::Internal("bad".into());
        let dbg = format!("{e:?}");
        assert!(dbg.contains("Internal"));
        assert!(dbg.contains("bad"));
    }

    #[test]
    fn error_debug_pool() {
        let e = SqliteError::Pool("exhausted".into());
        let dbg = format!("{e:?}");
        assert!(dbg.contains("Pool"));
        assert!(dbg.contains("exhausted"));
    }
}
