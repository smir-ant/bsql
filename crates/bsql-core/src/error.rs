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
///
/// # Example
///
/// ```rust,ignore
/// use bsql::{Pool, BsqlError};
///
/// let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;
///
/// // Match on error variants for fine-grained handling
/// let result = bsql::query!("INSERT INTO users (name) VALUES ($n: &str)")
///     .run(&pool).await;
/// match result {
///     Ok(affected) => println!("inserted {affected}"),
///     Err(e) if e.is_unique_violation() => println!("already exists"),
///     Err(e) => return Err(e),
/// }
/// ```
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
    pub pg_code: Option<Box<str>>,
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Row/column decoding failure.
#[derive(Debug)]
pub struct DecodeError {
    pub column: Cow<'static, str>,
    pub expected: &'static str,
    pub actual: Cow<'static, str>,
    /// Optional underlying error that caused the decode failure.
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync>>,
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
            Some(code) => write!(f, "[{}] {}", &**code, self.message),
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
            Self::Decode(e) => e.source(),
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

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        boxed_source(&self.source)
    }
}

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

// --- Query helpers ---

impl BsqlError {
    /// Whether this error is a PostgreSQL query cancellation / statement timeout
    /// (SQLSTATE 57014).
    pub fn is_timeout(&self) -> bool {
        matches!(self, BsqlError::Query(q) if q.pg_code.as_deref() == Some("57014"))
    }

    /// Whether this error is a serialization failure (SQLSTATE 40001).
    ///
    /// When using `SERIALIZABLE` isolation, PostgreSQL may abort a transaction
    /// with this code. The correct response is to retry the entire transaction.
    pub fn is_serialization_failure(&self) -> bool {
        matches!(self, BsqlError::Query(q) if q.pg_code.as_deref() == Some("40001"))
    }

    /// Whether this error is a unique constraint violation (SQLSTATE 23505).
    ///
    /// Common when inserting a row that would duplicate a unique index key.
    /// The error message typically includes which constraint was violated.
    pub fn is_unique_violation(&self) -> bool {
        matches!(self, BsqlError::Query(q) if q.pg_code.as_deref() == Some("23505"))
    }

    /// Whether this error is a foreign key violation (SQLSTATE 23503).
    ///
    /// Raised when an INSERT or UPDATE references a row that does not exist
    /// in the referenced table, or a DELETE would leave dangling references.
    pub fn is_foreign_key_violation(&self) -> bool {
        matches!(self, BsqlError::Query(q) if q.pg_code.as_deref() == Some("23503"))
    }

    /// Whether this error is a NOT NULL violation (SQLSTATE 23502).
    ///
    /// Raised when an INSERT or UPDATE sets a NOT NULL column to NULL.
    pub fn is_not_null_violation(&self) -> bool {
        matches!(self, BsqlError::Query(q) if q.pg_code.as_deref() == Some("23502"))
    }

    /// Whether this error is a check constraint violation (SQLSTATE 23514).
    pub fn is_check_violation(&self) -> bool {
        matches!(self, BsqlError::Query(q) if q.pg_code.as_deref() == Some("23514"))
    }

    /// Whether this error is a deadlock (SQLSTATE 40P01).
    ///
    /// PostgreSQL detected a deadlock between two or more transactions and
    /// chose this one as the victim. The correct response is to retry.
    pub fn is_deadlock(&self) -> bool {
        matches!(self, BsqlError::Query(q) if q.pg_code.as_deref() == Some("40P01"))
    }

    /// The PostgreSQL SQLSTATE code, if this is a query error with a code.
    ///
    /// Returns `None` for non-query errors or query errors without a code
    /// (e.g., I/O errors during query execution).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// match err.pg_code() {
    ///     Some("23505") => println!("unique violation"),
    ///     Some("23503") => println!("foreign key violation"),
    ///     _ => {}
    /// }
    /// ```
    pub fn pg_code(&self) -> Option<&str> {
        match self {
            BsqlError::Query(q) => q.pg_code.as_deref(),
            _ => None,
        }
    }

    /// Convert a `DriverError` that occurred during query execution.
    ///
    /// Unlike the blanket `From<DriverError>` impl (which maps `Io` to `Connect`),
    /// this maps `Io` errors to `Query` — because a network failure mid-query is
    /// a query error, not a connection error.
    pub fn from_driver_query(e: bsql_driver_postgres::DriverError) -> Self {
        match e {
            bsql_driver_postgres::DriverError::Io(io_err) => BsqlError::Query(QueryError {
                message: Cow::Owned(format!("I/O error during query: {io_err}")),
                pg_code: None,
                source: Some(Box::new(io_err)),
            }),
            other => BsqlError::from(other),
        }
    }
}

// --- From conversions ---

impl From<bsql_driver_postgres::DriverError> for BsqlError {
    fn from(e: bsql_driver_postgres::DriverError) -> Self {
        match e {
            bsql_driver_postgres::DriverError::Io(io_err) => BsqlError::Connect(ConnectError {
                message: Cow::Owned(io_err.to_string()),
                source: Some(Box::new(io_err)),
            }),
            bsql_driver_postgres::DriverError::Auth(msg) => BsqlError::Connect(ConnectError {
                message: Cow::Owned(msg),
                source: None,
            }),
            bsql_driver_postgres::DriverError::Protocol(msg) => BsqlError::Query(QueryError {
                message: Cow::Owned(msg),
                pg_code: None,
                source: None,
            }),
            bsql_driver_postgres::DriverError::Server {
                code,
                message,
                detail,
                hint,
                position,
            } => {
                let msg = {
                    let has_extras = position.is_some() || detail.is_some() || hint.is_some();
                    if has_extras {
                        let mut s = String::from(&*message);
                        if let Some(pos) = position {
                            use std::fmt::Write;
                            let _ = write!(s, " (at position {pos})");
                        }
                        if let Some(d) = &detail {
                            s.push_str("\n  detail: ");
                            s.push_str(d);
                        }
                        if let Some(h) = &hint {
                            s.push_str("\n  hint: ");
                            s.push_str(h);
                        }
                        Cow::Owned(s)
                    } else {
                        Cow::Owned(String::from(message))
                    }
                };
                BsqlError::Query(QueryError {
                    message: msg,
                    pg_code: Some(Box::from(std::str::from_utf8(&code).unwrap_or("?????"))),
                    source: None,
                })
            }
            bsql_driver_postgres::DriverError::Pool(msg) => BsqlError::Pool(PoolError {
                message: Cow::Owned(msg),
                source: None,
            }),
        }
    }
}

// --- SQLite error conversion ---

#[cfg(feature = "sqlite")]
impl BsqlError {
    /// Convert a SQLite driver error into a `BsqlError`.
    pub fn from_sqlite(e: bsql_driver_sqlite::SqliteError) -> Self {
        match e {
            bsql_driver_sqlite::SqliteError::Sqlite { code, message } => {
                BsqlError::Query(QueryError {
                    message: Cow::Owned(format!("SQLite error [{code}]: {message}")),
                    pg_code: None,
                    source: None,
                })
            }
            bsql_driver_sqlite::SqliteError::Io(io_err) => BsqlError::Connect(ConnectError {
                message: Cow::Owned(format!("SQLite I/O error: {io_err}")),
                source: Some(Box::new(io_err)),
            }),
            bsql_driver_sqlite::SqliteError::Internal(msg) => BsqlError::Query(QueryError {
                message: Cow::Owned(format!("SQLite internal error: {msg}")),
                pg_code: None,
                source: None,
            }),
            bsql_driver_sqlite::SqliteError::Pool(msg) => BsqlError::Pool(PoolError {
                message: Cow::Owned(format!("SQLite pool error: {msg}")),
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

impl DecodeError {
    /// Create a decode error with an underlying cause.
    pub fn with_source(
        column: impl Into<Cow<'static, str>>,
        expected: &'static str,
        actual: impl Into<Cow<'static, str>>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> BsqlError {
        BsqlError::Decode(DecodeError {
            column: column.into(),
            expected,
            actual: actual.into(),
            source: Some(Box::new(source)),
        })
    }

    /// Create a decode error for a column count mismatch.
    ///
    /// Used by generated code to detect schema drift between compile-time
    /// and runtime (e.g. a column was added/removed after the binary was built).
    pub fn column_count(expected: usize, actual: usize) -> BsqlError {
        BsqlError::Decode(DecodeError {
            column: Cow::Borrowed("*"),
            expected: "matching column count",
            actual: Cow::Owned(format!(
                "expected {} columns but row has {}",
                expected, actual
            )),
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
            pg_code: Some(Box::from("23505")),
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
            column: Cow::Borrowed("age"),
            expected: "i32",
            actual: Cow::Borrowed("text"),
            source: None,
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
        let driver_err = bsql_driver_postgres::DriverError::Server {
            code: *b"23505",
            message: "duplicate key".into(),
            detail: Some("Key (login)=(alice) already exists.".into()),
            hint: Some("Use ON CONFLICT to handle duplicates.".into()),
            position: None,
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
        let driver_err = bsql_driver_postgres::DriverError::Server {
            code: *b"42P01",
            message: "relation does not exist".into(),
            detail: None,
            hint: None,
            position: None,
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
            column: Cow::Borrowed("col"),
            expected: "i32",
            actual: Cow::Borrowed("text"),
            source: None,
        });
        assert!(e.source().is_none());
    }

    #[test]
    fn decode_error_with_source_chain() {
        let inner = std::io::Error::new(std::io::ErrorKind::InvalidData, "bad utf-8");
        let e = DecodeError::with_source("name", "String", "invalid bytes", inner);
        assert!(e.source().is_some());
        match &e {
            BsqlError::Decode(d) => {
                assert_eq!(d.column, "name");
                assert_eq!(d.expected, "String");
            }
            other => panic!("expected Decode, got: {other:?}"),
        }
    }

    #[test]
    fn is_timeout_true_for_57014() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("canceling statement due to statement timeout"),
            pg_code: Some(Box::from("57014")),
            source: None,
        });
        assert!(e.is_timeout());
    }

    #[test]
    fn is_timeout_false_for_other_codes() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("unique violation"),
            pg_code: Some(Box::from("23505")),
            source: None,
        });
        assert!(!e.is_timeout());
    }

    #[test]
    fn is_timeout_false_for_non_query() {
        let e = PoolError::exhausted();
        assert!(!e.is_timeout());
    }

    #[test]
    fn is_serialization_failure_true_for_40001() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("could not serialize access"),
            pg_code: Some(Box::from("40001")),
            source: None,
        });
        assert!(e.is_serialization_failure());
    }

    #[test]
    fn is_serialization_failure_false_for_other_codes() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("timeout"),
            pg_code: Some(Box::from("57014")),
            source: None,
        });
        assert!(!e.is_serialization_failure());
    }

    #[test]
    fn from_driver_query_maps_io_to_query() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broke");
        let e = BsqlError::from_driver_query(bsql_driver_postgres::DriverError::Io(io_err));
        match &e {
            BsqlError::Query(q) => {
                assert!(q.message.contains("I/O error during query"));
                assert!(q.source.is_some());
            }
            other => panic!("expected Query, got: {other:?}"),
        }
    }

    #[test]
    fn from_driver_query_non_io_delegates_to_from() {
        let e =
            BsqlError::from_driver_query(bsql_driver_postgres::DriverError::Pool("test".into()));
        assert!(matches!(e, BsqlError::Pool(_)));
    }

    // --- is_unique_violation ---

    #[test]
    fn is_unique_violation_true_for_23505() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("duplicate key value violates unique constraint"),
            pg_code: Some(Box::from("23505")),
            source: None,
        });
        assert!(e.is_unique_violation());
    }

    #[test]
    fn is_unique_violation_false_for_other_codes() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("timeout"),
            pg_code: Some(Box::from("57014")),
            source: None,
        });
        assert!(!e.is_unique_violation());
    }

    #[test]
    fn is_unique_violation_false_for_non_query() {
        let e = PoolError::exhausted();
        assert!(!e.is_unique_violation());
    }

    // --- is_foreign_key_violation ---

    #[test]
    fn is_foreign_key_violation_true_for_23503() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("insert or update violates foreign key constraint"),
            pg_code: Some(Box::from("23503")),
            source: None,
        });
        assert!(e.is_foreign_key_violation());
    }

    #[test]
    fn is_foreign_key_violation_false_for_other_codes() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("unique"),
            pg_code: Some(Box::from("23505")),
            source: None,
        });
        assert!(!e.is_foreign_key_violation());
    }

    #[test]
    fn is_foreign_key_violation_false_for_non_query() {
        let e = ConnectError::create("down");
        assert!(!e.is_foreign_key_violation());
    }

    // --- is_not_null_violation ---

    #[test]
    fn is_not_null_violation_true_for_23502() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("null value in column \"name\" violates not-null constraint"),
            pg_code: Some(Box::from("23502")),
            source: None,
        });
        assert!(e.is_not_null_violation());
    }

    #[test]
    fn is_not_null_violation_false_for_other_codes() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("unique"),
            pg_code: Some(Box::from("23505")),
            source: None,
        });
        assert!(!e.is_not_null_violation());
    }

    // --- is_check_violation ---

    #[test]
    fn is_check_violation_true_for_23514() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("new row violates check constraint"),
            pg_code: Some(Box::from("23514")),
            source: None,
        });
        assert!(e.is_check_violation());
    }

    #[test]
    fn is_check_violation_false_for_other_codes() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("unique"),
            pg_code: Some(Box::from("23505")),
            source: None,
        });
        assert!(!e.is_check_violation());
    }

    // --- is_deadlock ---

    #[test]
    fn is_deadlock_true_for_40p01() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("deadlock detected"),
            pg_code: Some(Box::from("40P01")),
            source: None,
        });
        assert!(e.is_deadlock());
    }

    #[test]
    fn is_deadlock_false_for_other_codes() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("serialization"),
            pg_code: Some(Box::from("40001")),
            source: None,
        });
        assert!(!e.is_deadlock());
    }

    #[test]
    fn is_deadlock_false_for_non_query() {
        let e = PoolError::exhausted();
        assert!(!e.is_deadlock());
    }

    // --- pg_code ---

    #[test]
    fn pg_code_returns_code_for_query_error() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("duplicate key"),
            pg_code: Some(Box::from("23505")),
            source: None,
        });
        assert_eq!(e.pg_code(), Some("23505"));
    }

    #[test]
    fn pg_code_returns_none_for_query_without_code() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("I/O error"),
            pg_code: None,
            source: None,
        });
        assert_eq!(e.pg_code(), None);
    }

    #[test]
    fn pg_code_returns_none_for_pool_error() {
        let e = PoolError::exhausted();
        assert_eq!(e.pg_code(), None);
    }

    #[test]
    fn pg_code_returns_none_for_connect_error() {
        let e = ConnectError::create("refused");
        assert_eq!(e.pg_code(), None);
    }

    #[test]
    fn pg_code_returns_none_for_decode_error() {
        let e = BsqlError::Decode(DecodeError {
            column: Cow::Borrowed("col"),
            expected: "i32",
            actual: Cow::Borrowed("text"),
            source: None,
        });
        assert_eq!(e.pg_code(), None);
    }

    // --- Server error with position ---

    #[test]
    fn server_error_with_position_display() {
        let driver_err = bsql_driver_postgres::DriverError::Server {
            code: *b"42601",
            message: "syntax error".into(),
            detail: None,
            hint: None,
            position: Some(8),
        };
        let e = BsqlError::from(driver_err);
        let display = e.to_string();
        assert!(
            display.contains("at position 8"),
            "should contain position: {display}"
        );
        assert!(
            display.contains("syntax error"),
            "should contain message: {display}"
        );
    }

    #[test]
    fn server_error_with_all_fields() {
        let driver_err = bsql_driver_postgres::DriverError::Server {
            code: *b"42P01",
            message: "relation does not exist".into(),
            detail: Some("table was dropped".into()),
            hint: Some("recreate the table".into()),
            position: Some(42),
        };
        let e = BsqlError::from(driver_err);
        let display = e.to_string();
        assert!(display.contains("at position 42"));
        assert!(display.contains("detail: table was dropped"));
        assert!(display.contains("hint: recreate the table"));
        assert!(display.contains("relation does not exist"));
    }

    // --- from_driver_query with Server variant delegates to From ---

    #[test]
    fn from_driver_query_server_error_delegates() {
        let e = BsqlError::from_driver_query(bsql_driver_postgres::DriverError::Server {
            code: *b"23505",
            message: "duplicate key".into(),
            detail: None,
            hint: None,
            position: None,
        });
        assert!(matches!(e, BsqlError::Query(_)));
        assert_eq!(e.pg_code(), Some("23505"));
    }

    // --- From<DriverError> for all variants ---

    #[test]
    fn from_driver_io_maps_to_connect() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let e = BsqlError::from(bsql_driver_postgres::DriverError::Io(io_err));
        assert!(matches!(e, BsqlError::Connect(_)));
        assert!(e.source().is_some());
    }

    #[test]
    fn from_driver_auth_maps_to_connect() {
        let e = BsqlError::from(bsql_driver_postgres::DriverError::Auth(
            "wrong password".into(),
        ));
        match &e {
            BsqlError::Connect(ce) => {
                assert!(ce.message.contains("wrong password"));
            }
            other => panic!("expected Connect, got: {other:?}"),
        }
    }

    #[test]
    fn from_driver_protocol_maps_to_query() {
        let e = BsqlError::from(bsql_driver_postgres::DriverError::Protocol(
            "unexpected message".into(),
        ));
        match &e {
            BsqlError::Query(qe) => {
                assert!(qe.message.contains("unexpected message"));
                assert!(qe.pg_code.is_none());
            }
            other => panic!("expected Query, got: {other:?}"),
        }
    }

    // --- SQLite error conversion ---

    #[cfg(feature = "sqlite")]
    mod sqlite_tests {
        use super::*;

        #[test]
        fn from_sqlite_sqlite_error() {
            let e = BsqlError::from_sqlite(bsql_driver_sqlite::SqliteError::Sqlite {
                code: 19,
                message: "UNIQUE constraint failed".into(),
            });
            match &e {
                BsqlError::Query(qe) => {
                    assert!(qe.message.contains("SQLite error [19]"));
                    assert!(qe.message.contains("UNIQUE constraint failed"));
                    assert!(qe.pg_code.is_none());
                }
                other => panic!("expected Query, got: {other:?}"),
            }
        }

        #[test]
        fn from_sqlite_io_error() {
            let io_err =
                std::io::Error::new(std::io::ErrorKind::PermissionDenied, "read-only filesystem");
            let e = BsqlError::from_sqlite(bsql_driver_sqlite::SqliteError::Io(io_err));
            match &e {
                BsqlError::Connect(ce) => {
                    assert!(ce.message.contains("SQLite I/O error"));
                    assert!(ce.message.contains("read-only filesystem"));
                    assert!(ce.source.is_some());
                }
                other => panic!("expected Connect, got: {other:?}"),
            }
        }

        #[test]
        fn from_sqlite_internal_error() {
            let e = BsqlError::from_sqlite(bsql_driver_sqlite::SqliteError::Internal(
                "corrupted database".into(),
            ));
            match &e {
                BsqlError::Query(qe) => {
                    assert!(qe.message.contains("SQLite internal error"));
                    assert!(qe.message.contains("corrupted database"));
                }
                other => panic!("expected Query, got: {other:?}"),
            }
        }

        #[test]
        fn from_sqlite_pool_error() {
            let e = BsqlError::from_sqlite(bsql_driver_sqlite::SqliteError::Pool(
                "no readers available".into(),
            ));
            match &e {
                BsqlError::Pool(pe) => {
                    assert!(pe.message.contains("SQLite pool error"));
                    assert!(pe.message.contains("no readers available"));
                }
                other => panic!("expected Pool, got: {other:?}"),
            }
        }
    }

    // --- Send + Sync assertions ---

    fn _assert_send<T: Send>() {}
    fn _assert_sync<T: Sync>() {}

    #[test]
    fn bsql_error_is_send() {
        _assert_send::<BsqlError>();
    }

    #[test]
    fn bsql_error_is_sync() {
        _assert_sync::<BsqlError>();
    }

    // --- Gap: BsqlError Display for Connect variant includes message ---

    #[test]
    fn bsql_error_display_connect_includes_message() {
        let e = ConnectError::create("tcp connection refused at 127.0.0.1:5432");
        let display = e.to_string();
        assert!(
            display.contains("connect error:"),
            "should start with 'connect error:': {display}"
        );
        assert!(
            display.contains("tcp connection refused at 127.0.0.1:5432"),
            "should include the message: {display}"
        );
    }

    // --- Gap: QueryError Display with pg_code ---

    #[test]
    fn bsql_error_display_query_includes_pg_code() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("relation \"users\" does not exist"),
            pg_code: Some(Box::from("42P01")),
            source: None,
        });
        let display = e.to_string();
        assert!(
            display.contains("42P01"),
            "should include pg_code: {display}"
        );
        assert!(
            display.contains("relation \"users\" does not exist"),
            "should include message: {display}"
        );
    }

    // --- Gap: QueryError Display without pg_code ---

    #[test]
    fn bsql_error_display_query_no_code() {
        let e = BsqlError::Query(QueryError {
            message: Cow::Borrowed("I/O error during query"),
            pg_code: None,
            source: None,
        });
        let display = e.to_string();
        assert!(
            display.contains("I/O error during query"),
            "should include message: {display}"
        );
        assert!(
            !display.contains('['),
            "should not contain brackets without code: {display}"
        );
    }

    // --- Gap: QueryError::row_count produces expected message ---

    #[test]
    fn query_error_row_count_message() {
        let e = QueryError::row_count("exactly 1 row", 5);
        let display = e.to_string();
        assert!(
            display.contains("expected exactly 1 row, got 5 rows"),
            "row_count message: {display}"
        );
    }

    #[test]
    fn query_error_row_count_zero() {
        let e = QueryError::row_count("at least 1 row", 0);
        let display = e.to_string();
        assert!(
            display.contains("expected at least 1 row, got 0 rows"),
            "row_count zero: {display}"
        );
    }

    // --- Gap: DecodeError::with_source preserves all fields ---

    #[test]
    fn decode_error_with_source_preserves_fields() {
        let inner = std::io::Error::new(std::io::ErrorKind::InvalidData, "bad utf-8");
        let e = DecodeError::with_source("email", "String", "bytes", inner);
        let display = e.to_string();
        assert!(
            display.contains("email"),
            "should contain column: {display}"
        );
        assert!(
            display.contains("String"),
            "should contain expected type: {display}"
        );
        assert!(
            display.contains("bytes"),
            "should contain actual type: {display}"
        );
    }

    // --- Gap: PoolError Display ---

    #[test]
    fn pool_error_display_custom_message() {
        let e = BsqlError::Pool(PoolError {
            message: Cow::Owned("all 10 connections in use".to_owned()),
            source: None,
        });
        let display = e.to_string();
        assert!(
            display.contains("pool error: all 10 connections in use"),
            "pool error display: {display}"
        );
    }

    // --- Gap: ConnectError::with_source preserves source ---

    #[test]
    fn connect_error_with_source_display() {
        let inner = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let e = ConnectError::with_source("failed to connect to PG", inner);
        let display = e.to_string();
        assert!(
            display.contains("failed to connect to PG"),
            "should include msg: {display}"
        );
        assert!(e.source().is_some());
    }

    #[test]
    fn decode_error_column_count_mismatch() {
        // DecodeError::column_count returns a BsqlError::Decode
        let e: BsqlError = DecodeError::column_count(3, 2);
        let display = e.to_string();
        assert!(
            display.contains("expected 3 columns but row has 2"),
            "should describe column count mismatch: {display}"
        );
        // Should be a Decode variant
        assert!(matches!(e, BsqlError::Decode(_)));
        // No source
        assert!(e.source().is_none());
    }
}
