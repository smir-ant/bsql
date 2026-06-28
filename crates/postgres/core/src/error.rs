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
        // The fallback is a presentation default for the optional `severity`
        // header field, not a data value: PG omits `severity` only on
        // malformed/legacy ErrorResponse frames, and the message + SQLSTATE
        // it labels are always printed in full alongside it. Nothing is
        // hidden — the literal labels a severity-less error as "ERROR".
        #[allow(clippy::disallowed_methods, reason = "presentation default for an absent severity header; the message and SQLSTATE are still printed in full, so no data is dropped")]
        let severity = self.severity.as_deref().unwrap_or("ERROR");
        write!(f, "{}: {} ({})", severity, self.message, self.code)?;
        if let Some(ref d) = self.detail
            && !d.is_empty() { write!(f, "\nDETAIL: {d}")?; }
        if let Some(ref h) = self.hint
            && !h.is_empty() { write!(f, "\nHINT: {h}")?; }
        Ok(())
    }
}

impl std::error::Error for DbError {}

// Footprint pin: five owned String / Option<String> fields (code, severity,
// message, detail, hint). DbError is the structured server-error payload; its
// size is the dominant variant of DriverError, so pinning it documents the
// error path's footprint and catches a field addition.
crate::footprint_pin!(DbError, size = 120, align = 8);

impl DbError {
    pub fn is_code(&self, code: &str) -> bool { self.code == code }
    pub fn is_unique_violation(&self) -> bool { self.code == "23505" }
    pub fn is_foreign_key_violation(&self) -> bool { self.code == "23503" }
}

/// Driver-level error.
#[derive(Debug)]
pub enum DriverError {
    Db(DbError),
    Io(std::io::Error),
    NotReady,
    SslRefused,
    NoRows,
    Config(&'static str),
    /// A result row exceeded the 32-bit on-arena bounds (more columns,
    /// offset, or cell length than `u32`/`u16` can address). Never silently
    /// truncated — the row is rejected so no corrupted bytes are surfaced.
    RowTooLarge,
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
    /// A socket read needed to complete an in-flight command exceeded the
    /// configured read timeout. The protocol is mid-exchange — it had consumed
    /// every buffered byte and was awaiting a server reply that did not arrive
    /// in time. Surfaced as a distinct, classified error rather than a generic
    /// I/O failure (the cause is a deadline, not a broken pipe) and never as a
    /// silent stop (which would truncate the result).
    Timeout,
    /// A synchronous single-poll driver drove an engine future that returned
    /// `Poll::Pending` — the transport was not blocking, violating the
    /// single-poll executor contract. Over a blocking socket this is
    /// structurally impossible; it is classified rather than spun on, deadlocked
    /// on, or unwrapped. The driver-level analog of the engine's
    /// `SpuriousPending` marker.
    SpuriousPending,
    /// A server `DataRow`'s bytes did not match its declared column framing — a
    /// per-column length running past the frame body, or a negative column
    /// count. A well-formed server never sends this; it is rejected loudly
    /// rather than decoded into silently mis-addressed cells.
    RowDecodeFailed,
}

// Footprint pin: a sum type whose size is set by its widest variant,
// Db(DbError). The many fieldless variants (NotReady, NoRows, Timeout, …) cost
// nothing beyond the discriminant; the pin documents that the error enum is no
// wider than its DbError payload and catches a new wide variant.
crate::footprint_pin!(DriverError, size = 120, align = 8);

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Db(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::NotReady => write!(f, "connection not ready"),
            Self::SslRefused => write!(f, "server refused SSL"),
            Self::NoRows => write!(f, "query returned no rows"),
            Self::Config(msg) => write!(f, "config error: {msg}"),
            Self::RowTooLarge => write!(f, "result row too large to represent (exceeds 32-bit arena bounds)"),
            Self::UnclassifiedFailure => write!(f, "server reported a failure with no classified cause"),
            Self::NonUtf8Payload => write!(f, "server payload was not valid UTF-8"),
            Self::TimeoutOverflow => write!(f, "requested timeout overflows the monotonic clock"),
            Self::RowDescriptionMissing => write!(f, "row stream produced rows with no column description; result cannot be decoded"),
            Self::NotificationUnavailable => write!(f, "NOTIFY frame observed but its payload could not be resolved"),
            Self::Timeout => write!(f, "read timed out while awaiting a server reply mid-command"),
            Self::SpuriousPending => write!(f, "single-poll executor: engine future returned Pending over a blocking transport"),
            Self::RowDecodeFailed => write!(f, "server DataRow did not match its declared column framing"),
        }
    }
}

impl std::error::Error for DriverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Db(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::NotReady
            | Self::SslRefused
            | Self::NoRows
            | Self::Config(_)
            | Self::RowTooLarge
            | Self::UnclassifiedFailure
            | Self::NonUtf8Payload
            | Self::TimeoutOverflow
            | Self::RowDescriptionMissing
            | Self::NotificationUnavailable
            | Self::Timeout
            | Self::SpuriousPending
            | Self::RowDecodeFailed => None,
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
impl From<DbError> for DriverError {
    fn from(e: DbError) -> Self { Self::Db(e) }
}
