use std::fmt;

/// Structured PostgreSQL error with SQLSTATE code.
#[derive(Debug, Clone)]
pub struct DbError {
    pub code: String,
    /// Server-reported severity. `None` when the server omitted it or it was
    /// unrecognized — never fabricated. (Display falls back to "ERROR" for
    /// presentation only; the stored value stays honest.)
    pub severity: Option<String>,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} ({})", self.severity.as_deref().unwrap_or("ERROR"), self.message, self.code)?;
        if let Some(ref d) = self.detail
            && !d.is_empty() { write!(f, "\nDETAIL: {d}")?; }
        if let Some(ref h) = self.hint
            && !h.is_empty() { write!(f, "\nHINT: {h}")?; }
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
    /// A result row exceeded the 32-bit on-arena bounds (more columns,
    /// offset, or cell length than `u32`/`u16` can address). Never silently
    /// truncated — the row is rejected so no corrupted bytes are surfaced.
    RowTooLarge,
    /// The streaming row collector could not make progress: the protocol
    /// asked for more bytes but none could be supplied (premature server
    /// close mid-stream, or a feed the protocol rejected). Surfacing this
    /// instead of spinning or truncating keeps the result honest.
    StreamStalled,
    /// A `FailReply` was observed but the protocol carried no classified
    /// cause for it. A failure definitely occurred; its detail is absent —
    /// distinct from the connection merely being not-ready.
    UnclassifiedFailure,
    /// A server payload (NOTIFY message, command tag, column name) was not
    /// valid UTF-8, so it could not be decoded losslessly. Returned instead
    /// of substituting Unicode replacement characters.
    NonUtf8Payload,
    /// The requested timeout is so large that adding it to the current clock
    /// instant would overflow. Surfaced instead of panicking.
    TimeoutOverflow,
    /// A row stream produced row data but the column schema needed to size and
    /// interpret each row was absent. Without it every cell would silently read
    /// as 0-column / `None`; the row count and contents cannot be trusted, so
    /// the result is rejected rather than returned hollow.
    RowDescriptionMissing,
    /// A NOTIFY frame was observed but its payload could not be resolved from
    /// the protocol's notification arena. The notification definitely arrived;
    /// dropping it silently would lose an event the caller is waiting on.
    NotificationUnavailable,
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
            Self::RowTooLarge => write!(f, "result row too large to represent (exceeds 32-bit arena bounds)"),
            Self::StreamStalled => write!(f, "row stream stalled: server provided no further data mid-stream"),
            Self::UnclassifiedFailure => write!(f, "server reported a failure with no classified cause"),
            Self::NonUtf8Payload => write!(f, "server payload was not valid UTF-8"),
            Self::TimeoutOverflow => write!(f, "requested timeout overflows the monotonic clock"),
            Self::RowDescriptionMissing => write!(f, "row stream produced rows with no column description; result cannot be decoded"),
            Self::NotificationUnavailable => write!(f, "NOTIFY frame observed but its payload could not be resolved"),
        }
    }
}

impl std::error::Error for DriverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Db(e) => Some(e),
            Self::Protocol(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::NotReady
            | Self::SslRefused
            | Self::NoRows
            | Self::Config(_)
            | Self::RowTooLarge
            | Self::StreamStalled
            | Self::UnclassifiedFailure
            | Self::NonUtf8Payload
            | Self::TimeoutOverflow
            | Self::RowDescriptionMissing
            | Self::NotificationUnavailable => None,
        }
    }
}

impl From<crate::types::RowTooLarge> for DriverError {
    fn from(_: crate::types::RowTooLarge) -> Self {
        Self::RowTooLarge
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
