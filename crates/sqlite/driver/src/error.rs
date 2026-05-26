#[derive(Debug)]
pub enum SqliteError {
    Open(String),
    Query(String),
}

impl core::fmt::Display for SqliteError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Open(msg) => write!(f, "sqlite open: {msg}"),
            Self::Query(msg) => write!(f, "sqlite: {msg}"),
        }
    }
}

impl std::error::Error for SqliteError {}

impl From<rusqlite::Error> for SqliteError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Query(e.to_string())
    }
}
