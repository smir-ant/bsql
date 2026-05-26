use std::fmt;

/// Structured PostgreSQL error with SQLSTATE code.
#[derive(Debug, Clone)]
pub struct DbError {
    pub code: String,
    pub severity: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} ({})", self.severity, self.message, self.code)?;
        if let Some(ref d) = self.detail {
            if !d.is_empty() { write!(f, "\nDETAIL: {d}")?; }
        }
        if let Some(ref h) = self.hint {
            if !h.is_empty() { write!(f, "\nHINT: {h}")?; }
        }
        Ok(())
    }
}

impl std::error::Error for DbError {}

impl DbError {
    pub fn is_code(&self, code: &str) -> bool { self.code == code }
    pub fn is_unique_violation(&self) -> bool { self.code == "23505" }
    pub fn is_foreign_key_violation(&self) -> bool { self.code == "23503" }
}

/// Driver-level error.
#[derive(Debug)]
pub enum DriverError {
    Db(DbError),
    Protocol(bsql_postgres_proto::ProtocolError),
    Io(std::io::Error),
    NotReady,
    SslRefused,
    NoRows,
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
    fn from(e: std::io::Error) -> Self { Self::Io(e) }
}
impl From<bsql_postgres_proto::ProtocolError> for DriverError {
    fn from(e: bsql_postgres_proto::ProtocolError) -> Self { Self::Protocol(e) }
}
impl From<DbError> for DriverError {
    fn from(e: DbError) -> Self { Self::Db(e) }
}
