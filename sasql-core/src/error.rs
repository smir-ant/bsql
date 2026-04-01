//! Error types for sasql.
//!
//! [`SasqlError`] is the single error type returned by all sasql operations.
//! It has four variants matching the four failure modes of a database operation:
//! pool, query execution, data decoding, and initial connection.

use std::fmt;

/// The error type for all sasql operations.
///
/// # Variants
///
/// - [`Pool`](SasqlError::Pool) — connection pool exhausted or misconfigured.
/// - [`Query`](SasqlError::Query) — PostgreSQL rejected the query at runtime
///   (triggers, RLS policies, constraint violations).
/// - [`Decode`](SasqlError::Decode) — a column value could not be converted to
///   the expected Rust type.
/// - [`Connect`](SasqlError::Connect) — initial connection to PostgreSQL failed.
#[derive(Debug)]
pub enum SasqlError {
    Pool(PoolError),
    Query(QueryError),
    Decode(DecodeError),
    Connect(ConnectError),
}

/// Connection pool failure.
#[derive(Debug)]
pub struct PoolError {
    pub message: String,
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Query execution failure. Contains the PostgreSQL error code when available.
#[derive(Debug)]
pub struct QueryError {
    pub message: String,
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
    pub message: String,
    pub(crate) source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Convenience alias used throughout sasql.
pub type SasqlResult<T> = Result<T, SasqlError>;

// --- Display ---

impl fmt::Display for SasqlError {
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
        if let Some(code) = &self.pg_code {
            write!(f, "[{code}] {}", self.message)
        } else {
            f.write_str(&self.message)
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

impl std::error::Error for SasqlError {
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
        self.source
            .as_ref()
            .map(|e| &**e as &(dyn std::error::Error + 'static))
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| &**e as &(dyn std::error::Error + 'static))
    }
}

impl std::error::Error for DecodeError {}

impl std::error::Error for ConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| &**e as &(dyn std::error::Error + 'static))
    }
}

// --- From conversions ---

impl From<tokio_postgres::Error> for SasqlError {
    fn from(e: tokio_postgres::Error) -> Self {
        let pg_code = e.code().map(|c| c.code().to_owned());
        let message = e.to_string();
        SasqlError::Query(QueryError {
            message,
            pg_code,
            source: Some(Box::new(e)),
        })
    }
}

impl From<deadpool_postgres::PoolError> for SasqlError {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        let message = e.to_string();
        SasqlError::Pool(PoolError {
            message,
            source: Some(Box::new(e)),
        })
    }
}

// --- Constructor helpers ---

impl PoolError {
    pub fn exhausted() -> SasqlError {
        SasqlError::Pool(PoolError {
            message: "pool exhausted: all connections in use".into(),
            source: None,
        })
    }
}

impl ConnectError {
    pub fn create(msg: impl Into<String>) -> SasqlError {
        SasqlError::Connect(ConnectError {
            message: msg.into(),
            source: None,
        })
    }

    pub fn with_source(
        msg: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> SasqlError {
        SasqlError::Connect(ConnectError {
            message: msg.into(),
            source: Some(Box::new(source)),
        })
    }
}

impl QueryError {
    pub fn row_count(expected: &str, actual: u64) -> SasqlError {
        SasqlError::Query(QueryError {
            message: format!("expected {expected}, got {actual} rows"),
            pg_code: None,
            source: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let e = SasqlError::Query(QueryError {
            message: "duplicate key".into(),
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
        let e = SasqlError::Decode(DecodeError {
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
    fn error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync + 'static>() {}
        assert_send_sync::<SasqlError>();
    }

    #[test]
    fn error_implements_std_error() {
        fn assert_std_error<T: std::error::Error>() {}
        assert_std_error::<SasqlError>();
    }

    #[test]
    fn from_tokio_postgres_error() {
        // tokio_postgres::Error is not easily constructable in tests,
        // but we can verify the From impl exists and the type compiles.
        fn _accepts_pg_error(e: tokio_postgres::Error) -> SasqlError {
            e.into()
        }
    }

    #[test]
    fn from_deadpool_error() {
        fn _accepts_pool_error(e: deadpool_postgres::PoolError) -> SasqlError {
            e.into()
        }
    }
}
