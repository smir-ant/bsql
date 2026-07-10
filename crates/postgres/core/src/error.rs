//! Error types: the SQLSTATE-classified server error [`DbError`], the
//! driver-level [`DriverError`], and the dynamic-column [`ColumnError`].

use std::fmt;

use bsql_postgres_proto::DecodeError;

/// Structured PostgreSQL error with SQLSTATE code.
#[derive(Debug, Clone)]
pub struct DbError {
    /// The 5-character SQLSTATE code (e.g. `"23505"`); test it with
    /// [`is_code`](Self::is_code).
    pub code: String,
    /// Server-reported severity. `None` when the server omitted it or it was
    /// unrecognized — never fabricated. (Display falls back to "ERROR" for
    /// presentation only; the stored value stays honest.)
    pub severity: Option<String>,
    /// The primary human-readable error message.
    pub message: String,
    /// An optional secondary DETAIL line elaborating the error.
    pub detail: Option<String>,
    /// An optional HINT suggesting how to resolve the error.
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
    /// `true` if the SQLSTATE [`code`](Self::code) equals `code`.
    pub fn is_code(&self, code: &str) -> bool { self.code == code }
    /// `true` if the SQLSTATE is `23505` (`unique_violation`).
    pub fn is_unique_violation(&self) -> bool { self.code == "23505" }
    /// `true` if the SQLSTATE is `23502` (`not_null_violation`).
    pub fn is_not_null_violation(&self) -> bool { self.code == "23502" }
    /// `true` if the SQLSTATE is `23503` (`foreign_key_violation`).
    pub fn is_foreign_key_violation(&self) -> bool { self.code == "23503" }
    /// `true` if the SQLSTATE is `23514` (`check_violation`).
    pub fn is_check_violation(&self) -> bool { self.code == "23514" }
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
///
/// `#[non_exhaustive]`: a consumer matching a `Row::get` failure must carry a
/// wildcard arm, so a future column-error class (added as the decode matrix
/// grows) is an additive, non-breaking change rather than a breaking `match`
/// churn — matching the forward-compat contract the rest of the tree already
/// uses (`DecodeError`, `ProtocolError`, `SqliteError`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColumnError {
    /// The requested column index is `>=` the row's column count.
    ///
    /// Both counts are `u32`: the column count is the arena's `u16` stride, and
    /// the requested index is a diagnostic bounded here (a `usize` past `u32`
    /// is capped — it is trivially out of range for a `u16`-strided row). This
    /// keeps `OutOfRange` an 8-byte payload rather than 16.
    OutOfRange {
        /// The requested (zero-based) column index.
        col: u32,
        /// The row's actual column count.
        n_cols: u32,
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
///
/// `#[non_exhaustive]`: this is the error a consumer actually matches on (it
/// reaches the consumer through every fallible driver return and the
/// `bsql::pg::*` glob), and its variant set has GROWN across the project's life
/// (`PoolTimeout`, `TooManyRows`, `Decode`, `Column`, `PayloadParse`, … were each
/// added later — each a breaking `match` change under an exhaustive enum). The
/// wildcard-arm requirement makes every future classification an additive,
/// non-breaking change, matching the tree's own dominant convention (proto uses
/// `#[non_exhaustive]` 44×; `SqliteError` carries it). Adding it does NOT change
/// the layout, so the 24-byte footprint pin below is unaffected.
#[derive(Debug)]
#[non_exhaustive]
pub enum DriverError {
    /// A structured server error with SQLSTATE. BOXED: `DbError` is by far the
    /// widest payload (~120 B of owned diagnostic strings), so inlining it would
    /// make EVERY `Result<T, DriverError>` carry 120 B on its error half — paid
    /// on the cold half of every fallible driver return. Boxing moves that
    /// payload behind a pointer: the happy path never allocates, only the cold
    /// error path does, and `DriverError` shrinks from 120 B to 32 B (and to 24 B
    /// once the remaining payload variants were narrowed to `<= 16` B — see the
    /// footprint pin below).
    Db(Box<DbError>),
    /// A transport-level socket / I/O failure; the connection is dead.
    Io(std::io::Error),
    /// The connection is not in a state to accept a verb — dead, or its linear
    /// liveness token was already taken by a prior fatal error.
    NotReady,
    /// The server refused SSL while the connection required it
    /// (`SslMode::Require`). Produced only by the TLS SSLRequest probe, so it
    /// exists only under the `tls` feature — with TLS compiled out the probe is
    /// never sent (a `Require`/custom-CA connect is instead a fail-loud
    /// [`DriverError::Config`] before any probe), so this classification can
    /// never occur and is not present.
    #[cfg(feature = "tls")]
    SslRefused,
    /// A query that required at least one row (e.g. `query_one_sql`) matched none.
    NoRows,
    /// A pre-connect configuration / validation error; the `&'static str` names
    /// the specific problem and the fix.
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
    /// still removed from the ledger, so it cannot wedge the buffer. A `Box<str>`
    /// (16 B) rather than a `String` (24 B): the payload is read-only once
    /// captured, so the spare `String` capacity word is dead weight on the error.
    PayloadParse(Box<str>),
}

// Footprint pin: a sum type whose size is set by its widest variant plus the
// discriminant word. With the dominant `DbError` boxed (`Db(Box<DbError>)` = one
// pointer), the widest payloads are the two 16-byte fat pointers `Config(&'static
// str)` and `PayloadParse(Box<str>)`; `Decode(DecodeError)` (12 B, after its
// `TruncatedColumnData` counts narrowed `usize -> u32`) and `Column(ColumnError)`
// (also 12 B, after `OutOfRange` narrowed `usize -> u32`) no longer set the width.
// So 16 B payload + 8 B discriminant = 24 B, down from 32 (and 120 before boxing).
// Every `Result<T, DriverError>` error half shrinks accordingly. The pin catches a
// new wide variant that would re-inflate the enum.
crate::footprint_pin!(DriverError, size = 24, align = 8);

impl DriverError {
    /// `true` if this is a server error whose SQLSTATE says a CACHED prepared
    /// plan is no longer valid on this connection — the signal the dynamic
    /// prepared-statement cache uses to evict and re-warm.
    ///
    /// Two codes qualify:
    /// - `0A000` (`feature_not_supported`) carries PostgreSQL's "cached plan must
    ///   not change result type" — a schema change altered a cached statement's
    ///   result columns, so the stored plan can no longer be executed. In the
    ///   cache's REUSE path this code can arise ONLY from a stale cached plan: a
    ///   genuine unsupported-feature `0A000` would already have failed the query's
    ///   FIRST (fused, unnamed) sighting, so it would never have been cached.
    /// - `26000` (`invalid_sql_statement_name`) — the cached server-side statement
    ///   was dropped out of band (`DEALLOCATE ALL` / `DISCARD ALL` run as SQL),
    ///   so a `Bind` to it now fails.
    ///
    /// On either, the cache reclaims the stale statement and surfaces the error
    /// once; the next sighting re-prepares against the current schema (the
    /// fused→pending→promote re-warm), so a schema change is a loud, self-healing
    /// event, never a silently-stale result.
    #[must_use]
    pub(crate) fn is_stale_prepared_plan(&self) -> bool {
        matches!(self, Self::Db(db) if db.is_code("0A000") || db.is_code("26000"))
    }
}

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Db(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::NotReady => write!(f, "connection not ready"),
            #[cfg(feature = "tls")]
            Self::SslRefused => write!(
                f,
                "server refused SSL while SslMode::Require was set (use SslMode::Prefer to allow a plaintext fallback, or SslMode::Disable to skip SSL)"
            ),
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
        assert_eq!(core::mem::size_of::<DriverError>(), 24);
    }
}
