#[derive(Debug)]
pub enum SqliteError {
    Open(String),
    Query(String),
    /// SQLite error with preserved error code for programmatic matching.
    Sqlite {
        code: Option<i32>,
        message: String,
    },
    /// A transaction closure failed and the subsequent `ROLLBACK` also failed,
    /// leaving the connection in an indeterminate transactional state. Both the
    /// original cause and the rollback failure are preserved — neither is
    /// silently dropped.
    TransactionRollbackFailed {
        /// The error the user's closure returned (the primary cause).
        original: Box<SqliteError>,
        /// The error the cleanup `ROLLBACK` returned.
        rollback: Box<SqliteError>,
    },
}

// Footprint pin: sized by the widest variant — `Sqlite { code: Option<i32>,
// message: String }` (the boxed TransactionRollbackFailed variant is two words
// of Box, smaller than the String+Option payload). A new wide variant would
// show up here.
crate::footprint_pin!(SqliteError, size = 32, align = 8);

impl SqliteError {
    /// Check if this is a constraint violation (SQLITE_CONSTRAINT = 19).
    pub fn is_constraint_violation(&self) -> bool {
        matches!(self, Self::Sqlite { code: Some(c), .. } if *c == 19)
    }

    /// Check if this is a busy/locked error (SQLITE_BUSY = 5).
    pub fn is_busy(&self) -> bool {
        matches!(self, Self::Sqlite { code: Some(c), .. } if *c == 5)
    }

    /// SQLite error code, if available.
    pub fn code(&self) -> Option<i32> {
        match self {
            Self::Sqlite { code, .. } => *code,
            _ => None,
        }
    }
}

impl core::fmt::Display for SqliteError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Open(msg) => write!(f, "sqlite open: {msg}"),
            Self::Query(msg) => write!(f, "sqlite: {msg}"),
            Self::Sqlite { code: Some(c), message } => write!(f, "sqlite error {c}: {message}"),
            Self::Sqlite { code: None, message } => write!(f, "sqlite: {message}"),
            Self::TransactionRollbackFailed { original, rollback } => write!(
                f,
                "transaction failed ({original}) and ROLLBACK also failed ({rollback})",
            ),
        }
    }
}

impl std::error::Error for SqliteError {}

impl From<rusqlite::Error> for SqliteError {
    fn from(e: rusqlite::Error) -> Self {
        let code = match &e {
            rusqlite::Error::SqliteFailure(err, _) => Some(err.extended_code),
            _ => None,
        };
        Self::Sqlite {
            code,
            message: e.to_string(),
        }
    }
}
