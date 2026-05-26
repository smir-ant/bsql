use std::fmt;

/// Structured PostgreSQL error with SQLSTATE code and diagnostics.
#[derive(Debug, Clone)]
pub struct DbError {
    /// SQLSTATE code (e.g., "23505" for unique_violation).
    pub code: String,
    /// Severity (ERROR, FATAL, PANIC, etc.).
    pub severity: String,
    /// Primary human-readable error message.
    pub message: String,
    /// Optional detail string.
    pub detail: Option<String>,
    /// Optional hint string.
    pub hint: Option<String>,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} ({})", self.severity, self.message, self.code)?;
        if let Some(ref d) = self.detail {
            if !d.is_empty() {
                write!(f, "\nDETAIL: {d}")?;
            }
        }
        if let Some(ref h) = self.hint {
            if !h.is_empty() {
                write!(f, "\nHINT: {h}")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for DbError {}

impl DbError {
    /// Check if the SQLSTATE code matches a specific value.
    pub fn is_code(&self, code: &str) -> bool {
        self.code == code
    }

    /// Check if this is a unique_violation (SQLSTATE 23505).
    pub fn is_unique_violation(&self) -> bool {
        self.code == "23505"
    }

    /// Check if this is a foreign_key_violation (SQLSTATE 23503).
    pub fn is_foreign_key_violation(&self) -> bool {
        self.code == "23503"
    }
}

/// Driver-level error combining protocol and I/O failures.
#[derive(Debug)]
pub enum DriverError {
    /// Structured PostgreSQL server error with SQLSTATE code.
    Db(DbError),
    /// Wire protocol error from `bsql-pg-proto`.
    Protocol(bsql_postgres_proto::ProtocolError),
    /// TCP / TLS I/O error.
    Io(std::io::Error),
    /// Connection not in expected state.
    NotReady,
    /// SSL negotiation failed.
    SslRefused,
    /// Query returned no rows (from `query_one` / `query_params_one`).
    NoRows,
    /// Configuration error (invalid user name, database name, etc.).
    Config(&'static str),
}

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Db(e) => write!(f, "{e}"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::NotReady => write!(f, "connection not ready"),
            Self::SslRefused => write!(f, "server refused SSL"),
            Self::NoRows => write!(f, "query returned no rows"),
            Self::Config(msg) => write!(f, "config error: {msg}"),
        }
    }
}

impl std::error::Error for DriverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Db(e) => Some(e),
            Self::Protocol(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::NotReady | Self::SslRefused | Self::NoRows | Self::Config(_) => None,
        }
    }
}

impl From<std::io::Error> for DriverError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<bsql_postgres_proto::ProtocolError> for DriverError {
    fn from(e: bsql_postgres_proto::ProtocolError) -> Self {
        Self::Protocol(e)
    }
}

impl From<DbError> for DriverError {
    fn from(e: DbError) -> Self {
        Self::Db(e)
    }
}
