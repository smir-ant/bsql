use std::fmt;

use bsql_postgres_proto::DecodeError;

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

/// Why reading a typed value from a dynamic [`Row`](crate::Row) column failed.
///
/// A dynamic (`query_sql`) result carries no compile-time schema, so a single
/// column read has several MUTUALLY-EXCLUSIVE outcomes. This type keeps each one
/// distinct — none is ever collapsed into another:
///
/// - the value decoded → `Ok(Some(v))` (not an error);
/// - the column is SQL `NULL` → `Ok(None)` (not an error — a dynamic read is
///   nullable-by-default, since the column's nullability is unknown without a
///   schema);
/// - the column index is past the row's width →
///   [`OutOfRange`](Self::OutOfRange);
/// - a by-name lookup found no such column →
///   [`UnknownColumn`](Self::UnknownColumn);
/// - the bytes did not decode as the requested Rust type →
///   [`Decode`](Self::Decode) (carrying proto's classified [`DecodeError`]) or,
///   for a text floating-point column, [`FloatParse`](Self::FloatParse).
///
/// The retired getters returned `Option<T>` built from `.parse().ok()`, which
/// collapsed NULL, decode-failure, and out-of-range into a single `None` and
/// silently swallowed the parse error. This type exists so that collapse cannot
/// happen: every outcome is a distinct, inspectable value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnError {
    /// The requested column index is `>=` the row's column count.
    OutOfRange {
        /// The requested (zero-based) column index.
        col: usize,
        /// The row's actual column count.
        n_cols: usize,
    },
    /// A by-name lookup found no column with the requested name in the result.
    UnknownColumn,
    /// The column bytes did not decode as the requested Rust type — a parse
    /// error, non-UTF-8, or a truncated body. Carries proto's classified
    /// [`DecodeError`].
    Decode(DecodeError),
    /// A text floating-point column's bytes are valid UTF-8 but did not parse as
    /// the requested floating type. PostgreSQL's binary-uniform typed path never
    /// decodes a float from text, so this text-float classification lives at the
    /// driver layer rather than in proto's `Cell` decode matrix.
    FloatParse,
}

impl fmt::Display for ColumnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfRange { col, n_cols } => {
                write!(f, "column index {col} out of range for a {n_cols}-column row")
            }
            Self::UnknownColumn => f.write_str("no column with the requested name in this result"),
            Self::Decode(e) => write!(f, "column decode failed: {e}"),
            Self::FloatParse => f.write_str("column text is not a valid floating-point number"),
        }
    }
}

impl std::error::Error for ColumnError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Decode(e) => Some(e),
            Self::OutOfRange { .. } | Self::UnknownColumn | Self::FloatParse => None,
        }
    }
}

/// Driver-level error.
#[derive(Debug)]
pub enum DriverError {
    /// A structured server error with SQLSTATE. BOXED: `DbError` is by far the
    /// widest payload (~120 B of owned diagnostic strings), so inlining it would
    /// make EVERY `Result<T, DriverError>` carry 120 B on its error half — paid
    /// on the cold half of every fallible driver return. Boxing moves that
    /// payload behind a pointer: the happy path never allocates, only the cold
    /// error path does, and `DriverError` shrinks from 120 B to 32 B.
    Db(Box<DbError>),
    Io(std::io::Error),
    NotReady,
    /// The server refused SSL while the connection required it
    /// (`SslMode::Require`). Produced only by the TLS SSLRequest probe, so it
    /// exists only under the `tls` feature — with TLS compiled out the probe is
    /// never sent (a `Require`/custom-CA connect is instead a fail-loud
    /// [`DriverError::Config`] before any probe), so this classification can
    /// never occur and is not present.
    #[cfg(feature = "tls")]
    SslRefused,
    NoRows,
    Config(&'static str),
    /// A result row exceeded the 32-bit on-arena bounds (more columns,
    /// offset, or cell length than `u32`/`u16` can address). Never silently
    /// truncated — the row is rejected so no corrupted bytes are surfaced.
    RowTooLarge,
    /// A single `simple_query` / `query_sql` batch contained multiple
    /// statements whose result rows had DIFFERENT column counts. A single
    /// result set has one uniform row shape whose fixed stride addresses every
    /// cell; a batch mixing widths cannot be represented as one result set
    /// without reading cells from the wrong offsets, so it is rejected loudly
    /// rather than returned with silently mis-addressed data. Run the
    /// statements separately (each yields its own result), or ensure the
    /// batch's row-returning statements share a column shape.
    MixedResultWidth,
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
    /// A typed `query!` row failed to decode into its compile-time record shape
    /// (a NULL in a NOT-NULL column, a wrong binary width, a truncated body).
    /// Carries the classified [`DecodeError`]. Decode runs AFTER the verb
    /// settled the connection to a clean idle, so this never harms the
    /// connection — it is a value returned to the caller, never a silent
    /// default.
    Decode(DecodeError),
    /// A typed `query!` result carried a row that exceeded the engine's inline
    /// buffer and was streamed in chunks. The bounded typed decoder needs one
    /// contiguous payload per row, so an oversize row is a classified error
    /// rather than a silently truncated or reassembled record.
    OversizeRow,
    /// A typed `query_one` matched MORE than one row. The exactly-one contract
    /// rejects a multi-row result loudly rather than silently returning the
    /// first row (which would mask a query that is not as selective as the
    /// caller assumed).
    TooManyRows,
    /// A pooled connection could not be acquired within the pool's configured
    /// acquire deadline: every connection was checked out and the pool was at
    /// its `max_size`, so no permit became free in time. Surfaced as a distinct
    /// backpressure signal rather than blocking forever — the caller can shed
    /// load, retry with backoff, or fail fast. Distinct from
    /// [`Timeout`](Self::Timeout) (a read deadline mid-command) and from
    /// [`NotReady`](Self::NotReady) (a specific connection is dead).
    PoolTimeout,
    /// Reading a typed value from a dynamic [`Row`](crate::Row) column failed.
    /// Carries the classified [`ColumnError`] — SQL NULL, out-of-range,
    /// unknown-name, or a decode failure — so a bad dynamic read surfaces as a
    /// distinct, inspectable value and never a silently-swallowed `None`. This is
    /// the dynamic (`query_sql`) counterpart to [`Decode`](Self::Decode), which
    /// classifies the compile-checked `query!` path.
    Column(ColumnError),
    /// A typed notification's payload did not parse into the requested type via
    /// its [`FromStr`](core::str::FromStr) impl. Carries the raw payload string so
    /// the failure is inspectable, never a silently-dropped notification — the
    /// typed-subscription counterpart to a decode failure. The notification was
    /// still removed from the ledger, so it cannot wedge the buffer.
    PayloadParse(String),
}

// Footprint pin: a sum type whose size is set by its widest variant. With the
// dominant `DbError` boxed (`Db(Box<DbError>)` = one pointer), the width is now
// set by the 24-byte payload variants (`Io`/`Decode`/`Column`/`PayloadParse`)
// plus the discriminant: 32 B, down from 120 B. Every `Result<T, DriverError>`
// error half shrinks accordingly. The pin catches a new wide variant that would
// re-inflate the enum.
crate::footprint_pin!(DriverError, size = 32, align = 8);

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Db(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::NotReady => write!(f, "connection not ready"),
            #[cfg(feature = "tls")]
            Self::SslRefused => write!(f, "server refused SSL"),
            Self::NoRows => write!(f, "query returned no rows"),
            Self::Config(msg) => write!(f, "config error: {msg}"),
            Self::RowTooLarge => write!(f, "result row too large to represent (exceeds 32-bit arena bounds)"),
            Self::MixedResultWidth => write!(f, "multi-statement batch mixed result-row widths; a single result set cannot represent statements returning different column counts — run them separately"),
            Self::UnclassifiedFailure => write!(f, "server reported a failure with no classified cause"),
            Self::NonUtf8Payload => write!(f, "server payload was not valid UTF-8"),
            Self::TimeoutOverflow => write!(f, "requested timeout overflows the monotonic clock"),
            Self::RowDescriptionMissing => write!(f, "row stream produced rows with no column description; result cannot be decoded"),
            Self::NotificationUnavailable => write!(f, "NOTIFY frame observed but its payload could not be resolved"),
            Self::Timeout => write!(f, "read timed out while awaiting a server reply mid-command"),
            Self::SpuriousPending => write!(f, "single-poll executor: engine future returned Pending over a blocking transport"),
            Self::RowDecodeFailed => write!(f, "server DataRow did not match its declared column framing"),
            Self::Decode(e) => write!(f, "typed row decode failed: {e}"),
            Self::OversizeRow => write!(f, "typed query result carried an oversize row that exceeds the bounded decoder's contiguous-payload requirement"),
            Self::TooManyRows => write!(f, "query_one matched more than one row"),
            Self::PoolTimeout => write!(f, "timed out acquiring a pooled connection; the pool is exhausted"),
            Self::Column(e) => write!(f, "{e}"),
            Self::PayloadParse(payload) => {
                write!(f, "notification payload did not parse into the requested type: {payload:?}")
            }
        }
    }
}

impl std::error::Error for DriverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            // Deref past the Box so the source is the `DbError`, not the Box —
            // identical to the pre-boxing behaviour.
            Self::Db(e) => Some(e.as_ref()),
            Self::Io(e) => Some(e),
            Self::Decode(e) => Some(e),
            Self::Column(e) => Some(e),
            // TLS-only classification: a separate gated arm so the `|`-chain
            // below is feature-independent (a single alternative cannot itself
            // be `#[cfg]`-gated).
            #[cfg(feature = "tls")]
            Self::SslRefused => None,
            Self::NotReady
            | Self::NoRows
            | Self::Config(_)
            | Self::RowTooLarge
            | Self::MixedResultWidth
            | Self::UnclassifiedFailure
            | Self::NonUtf8Payload
            | Self::TimeoutOverflow
            | Self::RowDescriptionMissing
            | Self::NotificationUnavailable
            | Self::Timeout
            | Self::SpuriousPending
            | Self::RowDecodeFailed
            | Self::OversizeRow
            | Self::TooManyRows
            | Self::PoolTimeout
            | Self::PayloadParse(_) => None,
        }
    }
}

impl From<DecodeError> for DriverError {
    fn from(e: DecodeError) -> Self {
        Self::Decode(e)
    }
}

impl From<ColumnError> for DriverError {
    fn from(e: ColumnError) -> Self {
        Self::Column(e)
    }
}

impl From<crate::types::ArenaSealError> for DriverError {
    fn from(e: crate::types::ArenaSealError) -> Self {
        match e {
            crate::types::ArenaSealError::TooLarge => Self::RowTooLarge,
            crate::types::ArenaSealError::MixedRowWidth => Self::MixedResultWidth,
        }
    }
}

impl From<std::io::Error> for DriverError {
    fn from(e: std::io::Error) -> Self { Self::Io(e) }
}
impl From<DbError> for DriverError {
    fn from(e: DbError) -> Self { Self::Db(Box::new(e)) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_db_error() -> DbError {
        DbError {
            code: "23505".to_string(),
            severity: Some("ERROR".to_string()),
            message: "duplicate key value violates unique constraint".to_string(),
            detail: Some("Key (id)=(1) already exists.".to_string()),
            hint: None,
        }
    }

    /// Boxing `Db` is a LAYOUT change only — the Display / Debug / Error impls
    /// must behave byte-identically to the unboxed variant. In particular
    /// `source()` must yield the `DbError` itself (deref past the Box), so a
    /// caller can still downcast to `DbError` and read the SQLSTATE; a regression
    /// to `Some(e)` (returning the `Box`) would break the downcast.
    #[test]
    fn db_variant_display_and_source_survive_boxing() {
        let db = sample_db_error();
        let expected_display = db.to_string();
        let err = DriverError::from(db); // `From<DbError>` boxes it.

        // Display forwards through the Box to the DbError — identical text.
        assert_eq!(err.to_string(), expected_display);
        // Debug still renders through the boxed payload.
        assert!(format!("{err:?}").contains("Db"));

        // `source()` is the DbError itself (not the Box), so classification is
        // reachable by downcast — the semantics-preservation assertion.
        let src = std::error::Error::source(&err).expect("Db carries a source");
        let dberr = src
            .downcast_ref::<DbError>()
            .expect("source is the DbError, not the Box wrapping it");
        assert!(dberr.is_unique_violation());

        // And the layout win: the enum is the shrunk width, payload one pointer.
        assert_eq!(core::mem::size_of::<DriverError>(), 32);
    }
}
