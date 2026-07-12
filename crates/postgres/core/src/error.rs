//! Error types: the SQLSTATE-classified server error [`DbError`], the
//! driver-level [`DriverError`], and the dynamic-column [`ColumnError`].

use std::fmt;

use bsql_postgres_proto::DecodeError;

/// Build the fixed 5-byte SQLSTATE from wire bytes. A SQLSTATE is invariantly 5
/// ASCII chars (`[0-9A-Z]`); a well-formed server sends exactly that. This
/// narrows any UNTRUSTED input TOTALLY: the first 5 bytes are taken, a short code
/// is space-padded, a longer one truncated, and any non-ASCII byte replaced with
/// `?`, so the result is ALWAYS valid ASCII (hence valid UTF-8) — [`DbError::code`]
/// views it as `&str` infallibly. Never panics (the `decoder_fuzz` gate proves it
/// across arbitrary bytes).
pub(crate) fn sqlstate_bytes(value: &[u8]) -> [u8; 5] {
    let mut code = [b' '; 5];
    for (dst, &src) in code.iter_mut().zip(value.iter()) {
        *dst = if src.is_ascii() { src } else { b'?' };
    }
    code
}

/// Structured PostgreSQL error with SQLSTATE code.
#[derive(Clone)]
pub struct DbError {
    /// The 5-byte SQLSTATE (e.g. `b"23505"`), read as `&str` via
    /// [`code`](Self::code) / tested with [`is_code`](Self::is_code). Stored
    /// INLINE as `[u8; 5]` rather than a heap `String`: a SQLSTATE is invariantly
    /// 5 ASCII chars, so the "exactly 5 bytes" invariant is lifted into the type
    /// and a server error no longer heap-allocates for its code. Private so the
    /// 5-byte / ASCII invariant cannot be violated by a caller (construct via
    /// [`new`](Self::new)). `pub(crate)` so the in-crate materializer sets it
    /// through the same [`sqlstate_bytes`] narrow — never reachable from outside
    /// the crate.
    pub(crate) code: [u8; 5],
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

impl fmt::Debug for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Render `code` as its `&str` view (not the raw `[u8; 5]`), so Debug
        // reads `code: "23505"` exactly as the former `String` field did.
        f.debug_struct("DbError")
            .field("code", &self.code())
            .field("severity", &self.severity)
            .field("message", &self.message)
            .field("detail", &self.detail)
            .field("hint", &self.hint)
            .finish()
    }
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
        write!(f, "{}: {} ({})", severity, self.message, self.code())?;
        if let Some(ref d) = self.detail
            && !d.is_empty() { write!(f, "\nDETAIL: {d}")?; }
        if let Some(ref h) = self.hint
            && !h.is_empty() { write!(f, "\nHINT: {h}")?; }
        Ok(())
    }
}

impl std::error::Error for DbError {}

// Footprint pin: the inline `[u8; 5]` SQLSTATE + four owned String / Option<String>
// fields (severity, message, detail, hint) = 5 + 4·24 = 101, padded to 104 (align
// 8). Down from 120 (the former `code: String` was 24 B); the shrink does NOT reach
// `DriverError` (which boxes `DbError` behind `Db(Box<DbError>)`, so its 24-byte pin
// is untouched). Pinning `DbError` documents the server-error payload footprint and
// catches a field addition.
crate::footprint_pin!(DbError, size = 104, align = 8);

impl DbError {
    /// Assemble a server error from its SQLSTATE and human-readable fields. The
    /// `code` is narrowed to the fixed 5-byte form ([`sqlstate_bytes`]); the sole
    /// constructor, since the `code` field is private.
    #[must_use]
    pub fn new(
        code: &str,
        severity: Option<String>,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
    ) -> Self {
        Self { code: sqlstate_bytes(code.as_bytes()), severity, message, detail, hint }
    }

    /// The 5-character SQLSTATE as `&str` (e.g. `"23505"`). No allocation — a
    /// view of the inline `[u8; 5]`.
    #[must_use]
    #[expect(
        clippy::manual_unwrap_or_default,
        reason = "unwrap_or_default is banned by the silent-fallback ledger; this explicit \
                  match is the sanctioned dead arm for an infallible narrow — `code` is \
                  ASCII-only by construction (`sqlstate_bytes`), so the `Err` view is \
                  unreachable, never a masked failure"
    )]
    pub fn code(&self) -> &str {
        match core::str::from_utf8(&self.code) {
            Ok(s) => s,
            Err(_) => "",
        }
    }

    /// `true` if the SQLSTATE [`code`](Self::code) equals `code`.
    pub fn is_code(&self, code: &str) -> bool { self.code() == code }
    /// `true` if the SQLSTATE is `23505` (`unique_violation`).
    pub fn is_unique_violation(&self) -> bool { self.is_code("23505") }
    /// `true` if the SQLSTATE is `23502` (`not_null_violation`).
    pub fn is_not_null_violation(&self) -> bool { self.is_code("23502") }
    /// `true` if the SQLSTATE is `23503` (`foreign_key_violation`).
    pub fn is_foreign_key_violation(&self) -> bool { self.is_code("23503") }
    /// `true` if the SQLSTATE is `23514` (`check_violation`).
    pub fn is_check_violation(&self) -> bool { self.is_code("23514") }
    /// `true` if the SQLSTATE is `53300` (`too_many_connections`) — the server is
    /// at its connection limit. The signal a connection-pool storm needs to shed
    /// load or back off. Classified from a CONNECT-time `ErrorResponse` exactly as
    /// from an active-phase one (the wire frame is identical), so a pool exhausting
    /// the server's limit no longer collapses to an unclassifiable I/O string.
    pub fn is_too_many_connections(&self) -> bool { self.is_code("53300") }
    /// `true` if the SQLSTATE is `3D000` (`invalid_catalog_name`) — the requested
    /// database does not exist. A connect-time diagnostic formerly collapsed to an
    /// opaque I/O string; now matchable exactly like an active-phase server error.
    pub fn is_invalid_catalog_name(&self) -> bool { self.is_code("3D000") }

    /// `true` if this SQLSTATE says the SERVER connection is broken / being torn
    /// down — so the connection is no longer usable and the fix is to reconnect,
    /// not to change the query. This is the server-error input to
    /// [`DriverError::is_disconnect`].
    ///
    /// Two whole classes qualify, matched by CLASS PREFIX (never an enumeration
    /// that could omit a member):
    /// - the entire **`08`** class (`connection_exception` — `08000`, `08003`
    ///   `connection_does_not_exist`, `08006` `connection_failure`, `08001`,
    ///   `08004`, `08007`, `08P01` `protocol_violation`): every member means the
    ///   connection itself failed.
    /// - the entire **`57P`** operator-intervention TERMINATION/REFUSAL subclass —
    ///   `57P01` `admin_shutdown` (the `pg_terminate_backend` signal), `57P02`
    ///   `crash_shutdown`, `57P03` `cannot_connect_now`, `57P04` `database_dropped`
    ///   (`DROP DATABASE … FORCE` killed the backend), and `57P05`
    ///   `idle_session_timeout` (the server terminated an idle session): every
    ///   member is the server terminating or refusing the backend, and a future
    ///   `57Pxx` termination code is covered too.
    ///
    /// Deliberately EXCLUDES `57014` (`query_canceled`) — it is the `57` class but
    /// NOT the `57P` subclass (a different subcode), so the prefix match does not
    /// sweep it in: a `statement_timeout` abort or a client `CancelToken` leaves
    /// the connection fully usable (it is drained + reusable), so a cancelled query
    /// is NEVER a disconnect. `57000` (`operator_intervention`, the bare class
    /// code) is likewise not `57P` and stays excluded. Every ordinary server error
    /// (a syntax error `42601`, a constraint violation `23505`, an undefined table
    /// `42P01`) also does not close the connection, so it is not a connection error
    /// here.
    #[must_use]
    pub fn is_connection_error(&self) -> bool {
        let code = self.code();
        // The whole `08` (connection exception) class, plus the whole `57P`
        // operator-intervention termination/refusal subclass. BOTH matched by
        // class prefix (consistent — no enumerate-vs-prefix split that could drop a
        // member): `57014` (query_canceled) is the `57` class but not `57P`, so it
        // is NOT swept in.
        code.starts_with("08") || code.starts_with("57P")
    }
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
    /// A pre-connect configuration error whose message is computed at runtime and
    /// so cannot be a `&'static str` — a DSN / environment parse failure naming the
    /// offending value (e.g. `invalid port: 99999`, `unknown DSN parameter: sslmod`).
    ///
    /// The dynamic sibling of [`Config`](Self::Config): a `Box<str>` (16 B, the
    /// same width as the existing widest payloads) rather than a `String` (24 B),
    /// since the message is read-only once built, so the spare capacity word is
    /// dead weight — and so this variant does NOT widen [`DriverError`] past its
    /// pinned 16-byte payload. Kept a SEPARATE variant rather than changing
    /// `Config` to `Cow<'static, str>` (which would be 24 B and re-inflate the
    /// enum, and churn every static `Config("…")` construction site) or to
    /// `Box<str>` (which would force every static message to allocate). Both
    /// classify as configuration errors — [`is_config`](Self::is_config) is `true`
    /// for either, and neither is a disconnect.
    ConfigDynamic(Box<str>),
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
    /// A dynamic (`query_sql` / `query_params`) result declared more columns than
    /// the driver's supported maximum ([`bsql_postgres_proto::MAX_ROW_COLUMNS`] =
    /// 1664, PostgreSQL's own `MaxTupleAttributeNumber` target-list limit). Unlike
    /// a torn-down connection, this is RECOVERABLE: the verb drained the in-flight
    /// result to a clean idle before returning, so the connection stays pooled and
    /// the caller retries with a narrower projection. Names both the offending
    /// `count` and the `max` so the fix is obvious. A conforming server never
    /// exceeds `max` (it errors at 1665 first, surfaced as a server
    /// [`Db`](Self::Db) error), so this classifies a NONCONFORMING peer; the typed
    /// `query!` path cannot reach it (its result columns are compile-capped).
    TooManyColumns {
        /// Column count the server's `RowDescription` declared.
        count: usize,
        /// Maximum supported by the driver.
        max: usize,
    },
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
    /// An explicit `prepare`d statement was executed (`query_prepared` /
    /// `execute_prepared`) with a parameter whose ENCODED type OID disagrees with
    /// the type the SERVER inferred for that `$N` placeholder at prepare time.
    ///
    /// A prepared statement has a FIXED plan: its parameter types are pinned at
    /// `Parse`, so the server CANNOT coerce a differently-typed binary bind
    /// against it — it would read the bytes AS the pinned type (a silent
    /// reinterpretation for two types of the same wire width). The driver
    /// therefore VERIFIES the caller's `<P as ParamsWriter>::OIDS` against the
    /// statement's server-inferred parameter types BEFORE sending the `Bind`, and
    /// rejects a mismatch loudly here — no wire round trip, and the connection is
    /// untouched (fix the parameter type and retry on the SAME connection). This
    /// is STRICTER than the dynamic `query_params` path (whose per-call `Parse`
    /// re-declares the types, so the server applies normal coercion): a fixed plan
    /// admits no coercion, so a strict-equality check is the only sound verification.
    /// An `unspecified` (OID `0`) type on EITHER side — an `EnumLabel` the client
    /// left to inference, or a parameter the server could not infer — is not
    /// verifiable and is passed through (best-effort), never falsely rejected.
    ParamTypeMismatch {
        /// Zero-based index of the offending `$N` parameter (`$1` is index `0`).
        index: usize,
        /// The type OID the server inferred for this parameter at prepare time —
        /// what the fixed plan requires.
        expected: u32,
        /// The type OID the caller's parameter encoded — what was bound.
        found: u32,
    },
    /// An explicit `prepare`d statement was executed with a parameter tuple whose
    /// arity disagrees with the number of `$N` placeholders the prepared statement
    /// declares. Caught client-side before the `Bind` (the server would otherwise
    /// reject the mismatched Bind), so no round trip is wasted and the connection
    /// is untouched — fix the tuple arity and retry.
    ParamCountMismatch {
        /// Number of parameters the prepared statement declares.
        expected: usize,
        /// Number of parameters the caller's tuple supplied.
        found: usize,
    },
}

// Footprint pin: a sum type whose size is set by its widest variant plus the
// discriminant word. With the dominant `DbError` boxed (`Db(Box<DbError>)` = one
// pointer), the widest payloads are the 16-byte fat pointers `Config(&'static
// str)`, `ConfigDynamic(Box<str>)`, and `PayloadParse(Box<str>)` (all 16 B, so
// the dynamic-config carrier does NOT re-inflate the enum);
// `Decode(DecodeError)` (12 B, after its
// `TruncatedColumnData` counts narrowed `usize -> u32`) and `Column(ColumnError)`
// (also 12 B, after `OutOfRange` narrowed `usize -> u32`) no longer set the width.
// So 16 B payload + 8 B discriminant = 24 B, down from 32 (and 120 before boxing).
// Every `Result<T, DriverError>` error half shrinks accordingly. The pin catches a
// new wide variant that would re-inflate the enum.
crate::footprint_pin!(DriverError, size = 24, align = 8);

impl DriverError {
    /// `true` if this error means the CONNECTION is no longer usable — the signal
    /// to RECONNECT (drop this connection / get a fresh pooled one), as opposed to
    /// a per-query error the connection survives (fix the query and retry on the
    /// SAME connection).
    ///
    /// # Intended use
    ///
    /// Branch your reconnect logic on the result of a VERB (a `query_*` / `execute`
    /// on an established connection):
    ///
    /// ```ignore
    /// match conn.query_sql(sql).await {
    ///     Ok(rows) => rows,
    ///     Err(e) if e.is_disconnect() => reconnect_and_retry().await?, // connection dead
    ///     Err(e) => return Err(e),                                     // query at fault
    /// }
    /// ```
    ///
    /// This is the distinction a resilient consumer needs: "the server REJECTED my
    /// query but the connection is fine" (a syntax error, a constraint violation,
    /// a `statement_timeout` cancel — [`is_disconnect`](Self::is_disconnect) is
    /// `false`) vs "the connection DIED mid-operation" (a dropped socket, a
    /// terminated backend — `true`). It is EXACT, not a string-match heuristic:
    /// each classified variant is decided by construction.
    ///
    /// The predicate answers exactly "is this connection UNUSABLE — get a fresh
    /// one?". A CONNECT-phase policy refusal is deliberately NOT a disconnect under
    /// that framing: [`SslRefused`](Self::SslRefused) is a violated TLS-policy
    /// contract, not a broken connection — an identical fresh connect would refuse
    /// the same way, so retrying is pointless (it is `false`). By contrast an
    /// [`Io`](Self::Io) during connect IS unusable and a fresh attempt may succeed,
    /// so it is `true` — consistent with the "unusable → reconnect" reading.
    ///
    /// `true` for:
    /// - [`Io`](Self::Io) — a transport socket failure (an unexpected EOF, a
    ///   connection reset, a broken pipe): the connection is dead.
    /// - [`NotReady`](Self::NotReady) — the connection's linear liveness token was
    ///   already taken by a PRIOR fatal error (a verb after a disconnect), so the
    ///   connection is dead.
    /// - [`Timeout`](Self::Timeout) — a FATAL mid-command read deadline elapsed
    ///   (the pool's dead-peer liveness bound, where a half-open socket's peer
    ///   vanished silently); the connection is torn down. (A notification wait's
    ///   quiet deadline is not this — it never surfaces as an error.)
    /// - [`Db`](Self::Db) whose SQLSTATE is a connection-broken code (the `08`
    ///   class, or `57P01`/`57P02`/`57P03` admin/crash shutdown — see
    ///   [`DbError::is_connection_error`]).
    ///
    /// `false` for every other classified error — a per-query server error
    /// ([`Db`](Self::Db) with an ordinary SQLSTATE, INCLUDING `57014`
    /// `query_canceled` from a `statement_timeout` or a `CancelToken`, which
    /// leaves the connection drained + reusable), [`NoRows`](Self::NoRows),
    /// [`Config`](Self::Config), [`PoolTimeout`](Self::PoolTimeout), a decode /
    /// column error, and the rest — none of which closes the connection.
    #[must_use]
    pub fn is_disconnect(&self) -> bool {
        match self {
            // The transport itself failed, the token was already taken by a prior
            // fatal error, or a fatal mid-command read deadline elapsed — in every
            // case the connection is dead and a verb on it cannot proceed.
            Self::Io(_) | Self::NotReady | Self::Timeout => true,
            // A server error is a disconnect ONLY when its SQLSTATE says the
            // connection is broken (the `08` class / `57P0x` shutdown); a syntax
            // error, a constraint violation, or a `57014` cancel is NOT.
            Self::Db(db) => db.is_connection_error(),
            // A required-SSL refusal aborts the CONNECT (the connection was never
            // established); it is a configuration/handshake fault, not a live
            // connection dying, so reconnecting unchanged would refuse again.
            #[cfg(feature = "tls")]
            Self::SslRefused => false,
            // Every remaining class leaves the connection usable (or never had
            // one): the query is at fault, not the transport. Listed exhaustively
            // (no wildcard) so a new variant forces a classification decision here
            // rather than silently defaulting.
            Self::NoRows
            | Self::Config(_)
            | Self::ConfigDynamic(_)
            | Self::RowTooLarge
            | Self::MixedResultWidth
            | Self::UnclassifiedFailure
            | Self::NonUtf8Payload
            | Self::TimeoutOverflow
            | Self::NotificationUnavailable
            | Self::SpuriousPending
            | Self::RowDecodeFailed
            | Self::Decode(_)
            | Self::TooManyRows
            | Self::TooManyColumns { .. }
            | Self::PoolTimeout
            | Self::Column(_)
            | Self::PayloadParse(_)
            // A client-side parameter-type / arity rejection: caught BEFORE any
            // Bind, so the connection is untouched — fix the parameter and retry.
            | Self::ParamTypeMismatch { .. }
            | Self::ParamCountMismatch { .. } => false,
        }
    }

    /// `true` if this is a pre-connect configuration / validation error —
    /// EITHER the `&'static str` [`Config`](Self::Config) or the runtime-message
    /// [`ConfigDynamic`](Self::ConfigDynamic). One check for "the connection was
    /// misconfigured" so a consumer need not know the two carriers exist (a
    /// static message vs a DSN/env parse failure that names its offending value).
    #[must_use]
    pub fn is_config(&self) -> bool {
        matches!(self, Self::Config(_) | Self::ConfigDynamic(_))
    }

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
            Self::ConfigDynamic(msg) => write!(f, "config error: {msg}"),
            Self::RowTooLarge => write!(f, "result row too large to represent (exceeds 32-bit arena bounds)"),
            Self::MixedResultWidth => write!(f, "multi-statement batch mixed result-row widths; a single result set cannot represent statements returning different column counts — run them separately"),
            Self::UnclassifiedFailure => write!(f, "server reported a failure with no classified cause"),
            Self::NonUtf8Payload => write!(f, "server payload was not valid UTF-8"),
            Self::TimeoutOverflow => write!(f, "requested timeout overflows the monotonic clock"),
            Self::NotificationUnavailable => write!(f, "NOTIFY frame observed but its payload could not be resolved"),
            Self::Timeout => write!(f, "read timed out while awaiting a server reply mid-command"),
            Self::SpuriousPending => write!(f, "single-poll executor: engine future returned Pending over a blocking transport"),
            Self::RowDecodeFailed => write!(f, "server DataRow did not match its declared column framing"),
            Self::Decode(e) => write!(f, "typed row decode failed: {e}"),
            Self::TooManyRows => write!(f, "query_one matched more than one row"),
            Self::TooManyColumns { count, max } => write!(
                f,
                "result-set too wide: {count} columns (max supported {max}); narrow the projection"
            ),
            Self::PoolTimeout => write!(f, "timed out acquiring a pooled connection; the pool is exhausted"),
            Self::Column(e) => write!(f, "{e}"),
            Self::PayloadParse(payload) => {
                write!(f, "notification payload did not parse into the requested type: {payload:?}")
            }
            Self::ParamTypeMismatch { index, expected, found } => write!(
                f,
                "prepared-statement parameter ${} has type OID {expected} but a value of type OID {found} was bound; a prepared statement's parameter types are fixed at prepare time and cannot coerce a differently-typed value",
                index.saturating_add(1)
            ),
            Self::ParamCountMismatch { expected, found } => write!(
                f,
                "prepared statement declares {expected} parameter(s) but {found} were bound"
            ),
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
            | Self::ConfigDynamic(_)
            | Self::RowTooLarge
            | Self::MixedResultWidth
            | Self::UnclassifiedFailure
            | Self::NonUtf8Payload
            | Self::TimeoutOverflow
            | Self::NotificationUnavailable
            | Self::Timeout
            | Self::SpuriousPending
            | Self::RowDecodeFailed
            | Self::TooManyRows
            | Self::TooManyColumns { .. }
            | Self::PoolTimeout
            | Self::PayloadParse(_)
            | Self::ParamTypeMismatch { .. }
            | Self::ParamCountMismatch { .. } => None,
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
        DbError::new(
            "23505",
            Some("ERROR".to_string()),
            "duplicate key value violates unique constraint".to_string(),
            Some("Key (id)=(1) already exists.".to_string()),
            None,
        )
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
    }

    fn db(code: &str) -> DriverError {
        DriverError::from(DbError::new(code, None, "x".to_string(), None, None))
    }

    /// The transport-fatal driver classes mean "the connection is dead —
    /// reconnect": a socket I/O failure, a not-ready connection whose token was
    /// taken by a prior fatal error, and a fatal mid-command read deadline.
    #[test]
    fn is_disconnect_true_for_transport_fatal_classes() {
        assert!(DriverError::Io(std::io::Error::other("server closed the connection")).is_disconnect());
        assert!(DriverError::NotReady.is_disconnect());
        assert!(DriverError::Timeout.is_disconnect());
    }

    /// A server error is a disconnect for a connection-broken SQLSTATE: the whole
    /// `08` (connection exception) class and the whole `57P` operator-intervention
    /// termination/refusal subclass — INCLUDING `57P04` (database_dropped) and
    /// `57P05` (idle_session_timeout), covered by the `57P` class-prefix match.
    #[test]
    fn is_disconnect_true_for_connection_broken_sqlstates() {
        for code in ["08000", "08003", "08006", "08001", "08004", "08007", "08P01",
                     "57P01", "57P02", "57P03", "57P04", "57P05"] {
            assert!(db(code).is_disconnect(), "SQLSTATE {code} must classify as a disconnect");
            // The DbError predicate agrees standalone.
            assert!(
                DbError::new(code, None, String::new(), None, None).is_connection_error(),
                "DbError::is_connection_error must be true for {code}",
            );
        }
    }

    /// A `statement_timeout` / `CancelToken` cancel returns `57014`
    /// (`query_canceled`) but leaves the connection DRAINED + REUSABLE — it is
    /// NEVER a disconnect. This is the load-bearing distinction for the
    /// server-side `statement_timeout` guardrail. The `57P` termination subclass is
    /// matched by prefix, so the `57` class's non-`57P` members must stay excluded.
    #[test]
    fn is_disconnect_false_for_query_canceled() {
        assert!(!db("57014").is_disconnect(), "57014 query_canceled is not a disconnect");
        assert!(!DbError::new("57014", None, String::new(), None, None).is_connection_error());
        // `57000` operator_intervention shares the `57` class but is NOT the `57P`
        // termination subclass — must not be swept in by the prefix match.
        assert!(!db("57000").is_disconnect(), "57000 is not a disconnect");
    }

    /// An ordinary per-query server error (a syntax error, a constraint
    /// violation, an undefined table) is NOT a disconnect: the connection is fine
    /// and the fix is the query, not a reconnect.
    #[test]
    fn is_disconnect_false_for_ordinary_server_errors() {
        for code in ["42601", "23505", "23502", "23503", "23514", "42P01", "53300", "3D000"] {
            assert!(!db(code).is_disconnect(), "SQLSTATE {code} must NOT classify as a disconnect");
        }
    }

    /// Every non-transport, non-server-error driver class leaves the connection
    /// usable (or never had one), so none is a disconnect.
    #[test]
    fn is_disconnect_false_for_non_transport_classes() {
        let usable = [
            DriverError::NoRows,
            DriverError::Config("x"),
            DriverError::ConfigDynamic("invalid port: 99999".into()),
            DriverError::RowTooLarge,
            DriverError::MixedResultWidth,
            DriverError::UnclassifiedFailure,
            DriverError::NonUtf8Payload,
            DriverError::TimeoutOverflow,
            DriverError::NotificationUnavailable,
            DriverError::SpuriousPending,
            DriverError::RowDecodeFailed,
            DriverError::TooManyRows,
            DriverError::TooManyColumns { count: 2000, max: 1664 },
            DriverError::PoolTimeout,
        ];
        for e in &usable {
            assert!(!e.is_disconnect(), "{e:?} must NOT classify as a disconnect");
        }
    }

    /// A runtime-message config error (a DSN / env parse failure) is a classified
    /// `ConfigDynamic`, matchable by a consumer, `is_config()`-true, never a
    /// disconnect, and displays with the same `config error:` prefix as the static
    /// `Config` — so the two carriers read identically.
    #[test]
    fn config_dynamic_is_a_classified_matchable_config_error() {
        let err = DriverError::ConfigDynamic("invalid port: 99999".into());
        assert!(matches!(err, DriverError::ConfigDynamic(_)));
        assert!(err.is_config(), "ConfigDynamic must classify as a config error");
        assert!(DriverError::Config("x").is_config(), "static Config is a config error too");
        assert!(!err.is_disconnect(), "a config error is never a disconnect");
        assert_eq!(err.to_string(), "config error: invalid port: 99999");
        // The static and dynamic carriers share the Display prefix — a consumer
        // reading the message cannot tell (nor should care) which carrier it is.
        assert_eq!(
            DriverError::Config("invalid port: 99999").to_string(),
            err.to_string(),
        );
    }
}
