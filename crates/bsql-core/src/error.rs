//! Error types for bsql.
//!
//! [`BsqlError`] is the single error type returned by all bsql operations.
//! It has four variants matching the four failure modes of a database operation:
//! pool, query execution, data decoding, and initial connection.

use std::borrow::Cow;
use std::fmt;

/// The error type for all bsql operations.
///
/// # Variants
///
/// - [`Pool`](BsqlError::Pool) — connection pool exhausted or misconfigured.
/// - [`Query`](BsqlError::Query) — PostgreSQL rejected the query at runtime
///   (triggers, RLS policies, constraint violations).
/// - [`Decode`](BsqlError::Decode) — a column value could not be converted to
///   the expected Rust type.
/// - [`Connect`](BsqlError::Connect) — initial connection to PostgreSQL failed.
#[derive(Debug)]
pub enum BsqlError {
    Pool(PoolError),
    Query(QueryError),
    Decode(DecodeError),
    Connect(ConnectError),
}

/// Connection pool failure.
#[derive(Debug)]
pub struct PoolError {
    pub message: Cow<'static, str>,
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Query execution failure. Contains the PostgreSQL error code when available.
#[derive(Debug)]
pub struct QueryError {
    pub message: Cow<'static, str>,
    /// The five-character SQLSTATE code (e.g. `"23505"` for unique violation).
    pub pg_code: Option<String>,
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Row/column decoding failure.
#[derive(Debug)]
pub struct DecodeError {
    pub column: String,
    pub expected: &'static str,
    pub actual: String,
}

/// Initial connection failure.
#[derive(Debug)]
pub struct ConnectError {
    pub message: Cow<'static, str>,
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Convenience alias used throughout bsql.
pub type BsqlResult<T> = Result<T, BsqlError>;

// --- Display ---

impl fmt::Display for BsqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pool(e) => write!(f, "pool error: {e}"),
            Self::Query(e) => write!(f, "query error: {e}"),
            Self::Decode(e) => write!(f, "decode error: {e}"),
            Self::Connect(e) => write!(f, "connect error: {e}"),
        }
    }
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.pg_code {
            Some(code) => write!(f, "[{code}] {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "column \"{}\": expected {}, got {}",
            self.column, self.expected, self.actual
        )
    }
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for BsqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Pool(e) => e.source(),
            Self::Query(e) => e.source(),
            Self::Decode(_) => None,
            Self::Connect(e) => e.source(),
        }
    }
}

impl std::error::Error for PoolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        boxed_source(&self.source)
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        boxed_source(&self.source)
    }
}

impl std::error::Error for DecodeError {}

impl std::error::Error for ConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        boxed_source(&self.source)
    }
}

fn boxed_source(
    src: &Option<Box<dyn std::error::Error + Send + Sync>>,
) -> Option<&(dyn std::error::Error + 'static)> {
    src.as_ref()
        .map(|e| &**e as &(dyn std::error::Error + 'static))
}

// --- From conversions ---

impl From<bsql_driver::DriverError> for BsqlError {
    fn from(e: bsql_driver::DriverError) -> Self {
        match e {
            bsql_driver::DriverError::Io(io_err) => BsqlError::Connect(ConnectError {
                message: Cow::Owned(io_err.to_string()),
                source: Some(Box::new(io_err)),
            }),
            bsql_driver::DriverError::Auth(msg) => BsqlError::Connect(ConnectError {
                message: Cow::Owned(msg),
                source: None,
            }),
            bsql_driver::DriverError::Protocol(msg) => BsqlError::Query(QueryError {
                message: Cow::Owned(msg),
                pg_code: None,
                source: None,
            }),
            bsql_driver::DriverError::Server {
                code,
                message,
                detail,
                hint,
            } => {
                let mut msg = message.to_string();
                if let Some(d) = &detail {
                    msg.push_str("\n  detail: ");
                    msg.push_str(d);
                }
                if let Some(h) = &hint {
                    msg.push_str("\n  hint: ");
                    msg.push_str(h);
                }
                BsqlError::Query(QueryError {
                    message: Cow::Owned(msg),
                    pg_code: Some(code.to_string()),
                    source: None,
                })
            }
            bsql_driver::DriverError::Pool(msg) => BsqlError::Pool(PoolError {
                message: Cow::Owned(msg),
                source: None,
            }),
        }
    }
}

// --- Constructor helpers ---

impl PoolError {
    pub fn exhausted() -> BsqlError {
        BsqlError::Pool(PoolError {
            message: Cow::Borrowed("pool exhausted: all connections in use"),
            source: None,
        })
    }
}

impl ConnectError {
    pub fn create(msg: impl Into<String>) -> BsqlError {
        BsqlError::Connect(ConnectError {
            message: Cow::Owned(msg.into()),
            source: None,
        })
    }

    pub fn with_source(
        msg: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> BsqlError {
        BsqlError::Connect(ConnectError {
            message: Cow::Owned(msg.into()),
            source: Some(Box::new(source)),
        })
    }
}

impl QueryError {
    pub fn row_count(expected: &str, actual: u64) -> BsqlError {
        BsqlError::Query(QueryError {
            message: Cow::Owned(format!("expected {expected}, got {actual} rows")),
            pg_code: None,
            source: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn pool_error_display() {
        let e = PoolError::exhausted();
        assert_eq!(
            e.to_string(),
            "pool error: pool exhausted: all connections in use"
        );
    }

    #[test]
    fn query_error_with_code_display() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("duplicate key"),
            pg_code: Some("23505".into()),
            source: None,
        });
        assert_eq!(e.to_string(), "query error: [23505] duplicate key");
    }

    #[test]
    fn query_error_without_code_display() {
        let e = QueryError::row_count("exactly 1 row", 0);
        assert_eq!(
            e.to_string(),
            "query error: expected exactly 1 row, got 0 rows"
        );
    }

    #[test]
    fn decode_error_display() {
        let e = BsqlError::Decode(DecodeError {
            column: "age".into(),
            expected: "i32",
            actual: "text".into(),
        });
        assert_eq!(
            e.to_string(),
            "decode error: column \"age\": expected i32, got text"
        );
    }

    #[test]
    fn connect_error_display() {
        let e = ConnectError::create("connection refused");
        assert_eq!(e.to_string(), "connect error: connection refused");
    }

    #[test]
    fn pool_exhausted_uses_borrowed_cow() {
        let e = PoolError::exhausted();
        match e {
            BsqlError::Pool(ref pe) => {
                assert!(
                    matches!(pe.message, Cow::Borrowed(_)),
                    "exhausted() should use Cow::Borrowed for zero-alloc"
                );
            }
            _ => panic!("expected Pool variant"),
        }
    }

    #[test]
    fn connect_error_uses_owned_cow() {
        let e = ConnectError::create("dynamic message");
        match e {
            BsqlError::Connect(ref ce) => {
                assert!(
                    matches!(ce.message, Cow::Owned(_)),
                    "create() with dynamic msg should use Cow::Owned"
                );
            }
            _ => panic!("expected Connect variant"),
        }
    }

    #[test]
    fn query_row_count_uses_owned_cow() {
        let e = QueryError::row_count("exactly 1 row", 5);
        match e {
            BsqlError::Query(ref qe) => {
                assert!(
                    matches!(qe.message, Cow::Owned(_)),
                    "row_count() with formatted msg should use Cow::Owned"
                );
            }
            _ => panic!("expected Query variant"),
        }
    }

    #[test]
    fn pool_error_source_chain() {
        let e = PoolError::exhausted();
        // exhausted() has no source
        assert!(e.source().is_none());
    }

    #[test]
    fn connect_error_with_source_chain() {
        let inner = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let e = ConnectError::with_source("connection failed", inner);
        assert!(e.source().is_some());
    }

    #[test]
    fn server_error_preserves_detail_and_hint() {
        let driver_err = bsql_driver::DriverError::Server {
            code: "23505".into(),
            message: "duplicate key".into(),
            detail: Some("Key (login)=(alice) already exists.".into()),
            hint: Some("Use ON CONFLICT to handle duplicates.".into()),
        };
        let e = BsqlError::from(driver_err);
        let display = e.to_string();
        assert!(
            display.contains("duplicate key"),
            "missing message: {display}"
        );
        assert!(
            display.contains("detail: Key (login)=(alice) already exists."),
            "missing detail: {display}"
        );
        assert!(
            display.contains("hint: Use ON CONFLICT to handle duplicates."),
            "missing hint: {display}"
        );
        // pg_code should be preserved
        match &e {
            BsqlError::Query(qe) => assert_eq!(qe.pg_code.as_deref(), Some("23505")),
            other => panic!("expected Query, got: {other:?}"),
        }
    }

    #[test]
    fn server_error_without_detail_hint() {
        let driver_err = bsql_driver::DriverError::Server {
            code: "42P01".into(),
            message: "relation does not exist".into(),
            detail: None,
            hint: None,
        };
        let e = BsqlError::from(driver_err);
        let display = e.to_string();
        assert!(
            display.contains("relation does not exist"),
            "missing message: {display}"
        );
        assert!(
            !display.contains("detail:"),
            "should not contain detail: {display}"
        );
        assert!(
            !display.contains("hint:"),
            "should not contain hint: {display}"
        );
    }

    #[test]
    fn decode_error_has_no_source() {
        let e = BsqlError::Decode(DecodeError {
            column: "col".into(),
            expected: "i32",
            actual: "text".into(),
        });
        assert!(e.source().is_none());
    }
}
