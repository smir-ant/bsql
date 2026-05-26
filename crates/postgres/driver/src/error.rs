use std::fmt;

/// Driver-level error combining protocol and I/O failures.
#[derive(Debug)]
pub enum DriverError {
    /// Wire protocol error from `bsql-pg-proto`.
    Protocol(bsql_postgres_proto::ProtocolError),
    /// TCP / TLS I/O error.
    Io(std::io::Error),
    /// Connection not in expected state.
    NotReady,
    /// SSL negotiation failed.
    SslRefused,
}

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::NotReady => write!(f, "connection not ready"),
            Self::SslRefused => write!(f, "server refused SSL"),
        }
    }
}

impl std::error::Error for DriverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Protocol(e) => Some(e),
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

impl From<bsql_postgres_proto::ProtocolError> for DriverError {
    fn from(e: bsql_postgres_proto::ProtocolError) -> Self {
        Self::Protocol(e)
    }
}
