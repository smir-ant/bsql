#[derive(Debug)]
pub enum SqliteError {
    Open(String),
    Query(String),
    /// SQLite error with preserved error code for programmatic matching.
    Sqlite {
        code: Option<i32>,
        message: String,
    },
}

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
