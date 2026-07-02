//! The observable outcome of replaying one [`crate::Transcript`].
//!
//! Every type here is `std`-only and names NO internal engine type. These
//! values are the invariant across an engine rebuild: a future engine deletes
//! its internal `ProtoState` / `Action` / `Reply` / `Session` internals, but
//! these observable shapes survive unchanged, so the same corpus keeps
//! comparing old-vs-new. All derive `PartialEq + Eq + Debug` so a corpus test
//! body is `assert_eq!(adapter.run(t), t.expect)`.

/// The complete observable result of replaying a transcript.
///
/// Aggregated across every [`crate::Step`] of the transcript: `client_bytes`
/// is the concatenated client→server wire, the notice/notification/parameter
/// lists accumulate in arrival order, `outcome` is the final step's result,
/// and `terminal` is the connection's end state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedRun {
    /// Every byte the client put on the wire, in order: the startup packet,
    /// each request frame, any feed-side bytes the engine emitted (e.g. an
    /// auth response), and a trailing Terminate packet if sent.
    pub client_bytes: Vec<u8>,
    /// The final step's outcome — `Ok` for a completed command (even a
    /// zero-row one), `Err` for a server `ErrorResponse` or a
    /// protocol/transport failure.
    pub outcome: Result<ObservedOk, ObservedErr>,
    /// Server notices (`WARNING`/`NOTICE`/`INFO`/`DEBUG`/`LOG`) surfaced
    /// during any step, in arrival order. A notice the engine silently drops
    /// (e.g. one arriving during the auth handshake) does NOT appear here —
    /// that absence is itself a pinned observable.
    pub notices: Vec<ObservedNotice>,
    /// Every `ParameterStatus` (key, value) the engine surfaced during the run,
    /// in arrival order, decoded raw from the wire — the exact frames the engine
    /// lends, with no known-key projection and no normalization. A duplicate
    /// `ParameterStatus` for one key appears as two entries (the engine surfaces
    /// each frame; it retains no map), so a dropped or reordered frame diverges.
    pub parameter_statuses: Vec<(String, String)>,
    /// Asynchronous `NotificationResponse` (`LISTEN`/`NOTIFY`) events
    /// surfaced during any step, in arrival order.
    pub notifications: Vec<ObservedNotify>,
    /// The backend process ID from the `BackendKeyData` (`'K'`) frame, read
    /// from the public cancel-request surface. `Some(pid)` once a session is
    /// active; `None` when no session was produced (disconnected / failed
    /// handshake). The secret cancel key is intentionally NOT observed — the
    /// engine redacts it and a leaked cancel authenticator is a capability
    /// leak, so only the non-secret PID is part of the observable contract.
    pub backend_pid: Option<i32>,
    /// The connection's final `ReadyForQuery` transaction-status indicator
    /// (`'I'` idle / `'T'` in a transaction block / `'E'` failed transaction),
    /// read after the last step. Collapsed to `Idle` when no session became
    /// active. Distinct from `terminal`: a connection can be `Ready` (reusable)
    /// yet sit in an open or failed transaction.
    pub tx_status: ObservedTxStatus,
    /// The connection's end state after the last step.
    pub terminal: ObservedStatus,
}

/// A successfully completed command's observable result.
///
/// A single command's reply is a SEQUENCE of result sets, one per SQL
/// statement the server delineated (a PG simple-query `Q` frame accepts a
/// `;`-separated batch like `"UPDATE …; INSERT …; SELECT …"` and emits one
/// `CommandComplete` per statement before a single final `ReadyForQuery`).
/// `result_sets` captures that per-statement structure so a mis-delineation
/// (flattening N statements into one, dropping or reordering an intermediate
/// statement's tag) is caught — rather than keeping only the final tag and
/// flattening every statement's rows into one undifferentiated list.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObservedOk {
    /// One entry per SQL statement the server delineated in this command's
    /// reply, in statement order. A single-statement command produces exactly
    /// one entry; a multi-statement batch produces one per statement (each
    /// non-final statement's tag arrives as an intermediate command-complete,
    /// the final statement's via the terminal). A command that completes with
    /// no statement (e.g. `Ping`, a bare `Parse`/`Describe`/`Close`) produces
    /// a single degenerate entry with an empty tag and no rows.
    pub result_sets: Vec<ObservedResultSet>,
    /// COPY-OUT data chunks (PG §55.2.6), in arrival order, each the raw body
    /// of one `CopyData` (`'d'`) frame. Empty for any command that is not a
    /// `COPY … TO STDOUT`. The chunk boundaries are the server's, preserved
    /// verbatim — a re-chunking is an observable change.
    pub copy_out: Vec<Vec<u8>>,
}

/// One SQL statement's observable result within a command's reply.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObservedResultSet {
    /// The server `CommandComplete` tag for this statement (e.g. `"SELECT 1"`,
    /// `"INSERT 0 1"`, `"UPDATE 3"`), or the empty string for a statement that
    /// reports none (an empty query, or a command with no statement-level tag).
    pub command_tag: String,
    /// Column names from this statement's `RowDescription`, or empty for a
    /// statement that describes no columns. For the extended-protocol
    /// bind/execute path the names are not re-surfaced at execute time, so
    /// this is empty there even for a row-bearing statement — a pinned quirk.
    pub column_names: Vec<String>,
    /// Per-column PostgreSQL type OIDs from this statement's `RowDescription`,
    /// positionally aligned with `column_names`. Empty when no description was
    /// observed. Decode is OID-driven, so the OIDs are a first-class
    /// observable, not metadata to discard.
    pub type_oids: Vec<u32>,
    /// Result rows as RAW per-column bytes. `None` is SQL `NULL` (wire
    /// `len = -1`); `Some(bytes)` is the column's wire bytes verbatim —
    /// including `Some(Vec::new())` for an empty but non-NULL value (wire
    /// `len = 0`), which is DISTINCT from `None`. Values are NOT decoded to
    /// typed Rust values: decode policy diverges between engines, so the raw
    /// bytes are the stable observable.
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    /// The server's affected-row count, when the statement reports one
    /// (`INSERT`/`UPDATE`/`DELETE`/`SELECT`/`COPY`/…); `None` for a statement
    /// with no row-count semantics (DDL, transaction control) or none observed.
    pub affected_rows: Option<u64>,
    /// `true` when the server paused this statement at a row cap with
    /// `PortalSuspended` (PG §55.2.7) instead of completing it — the result of
    /// a row-limited `Execute` (`max_rows > 0`). A suspended statement carries
    /// the rows fetched so far and no `CommandComplete` tag; the portal stays
    /// open. `false` for every normally-completed statement.
    pub portal_suspended: bool,
}

/// A failed command's observable result — a server error carries its
/// SQLSTATE, a protocol/transport failure carries a stable classification.
#[allow(
    clippy::large_enum_variant,
    reason = "the Server variant is the dominant error observable and carries the full PG §55.7 diagnostic field set by value; this is a dev-only comparison value constructed once per error fixture and compared by `==`, so the size spread is immaterial — boxing would only obscure the fixture literals with a `Box::new` wrapper"
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservedErr {
    /// The server sent an `ErrorResponse`. The SQLSTATE, severity (when the
    /// server included it), message, and the optional diagnostic fields are
    /// the cross-engine observable.
    ///
    /// Fields the CURRENT engine does not parse (`position`, `schema`,
    /// `table`, `column`, `constraint`) are surfaced as `None` even when the
    /// wire frame carried them — that "the engine drops this field" is itself
    /// a pinned observable, so a future engine that begins surfacing one
    /// diverges here loudly.
    Server {
        /// 5-character SQLSTATE code.
        sqlstate: String,
        /// Server-reported severity, `None` when omitted.
        severity: Option<String>,
        /// Human-readable message text (`M` field).
        message: String,
        /// Optional secondary detail (`D` field), `None` when absent/empty.
        detail: Option<String>,
        /// Optional hint (`H` field), `None` when absent/empty.
        hint: Option<String>,
        /// Optional cursor position (`P` field). The current engine does not
        /// parse it — always `None` (pinned absence).
        position: Option<String>,
        /// Optional schema name (`s` field). Not parsed by the current engine
        /// — always `None` (pinned absence).
        schema: Option<String>,
        /// Optional table name (`t` field). Not parsed by the current engine
        /// — always `None` (pinned absence).
        table: Option<String>,
        /// Optional column name (`c` field). Not parsed by the current engine
        /// — always `None` (pinned absence).
        column: Option<String>,
        /// Optional constraint name (`n` field). Not parsed by the current
        /// engine — always `None` (pinned absence).
        constraint: Option<String>,
    },
    /// A protocol-level or transport-level failure with no server SQLSTATE,
    /// classified to a stable, engine-independent tag.
    Protocol(ProtocolFailureKind),
}

/// Stable classification of a non-server failure. These tags name observable
/// failure CLASSES, not internal error enum variants, so they survive a
/// rebuild.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolFailureKind {
    /// The engine reported a failure for the in-flight command but parked no
    /// classified server cause.
    Unclassified,
    /// The engine asked for more bytes but the scripted transport was
    /// exhausted before the command reached its terminal.
    TransportExhausted,
    /// A row stream could not make progress (premature end mid-stream, or a
    /// feed the engine rejected).
    StreamStalled,
    /// A row stream produced rows with no column description to size them.
    RowDescriptionMissing,
    /// The engine signalled the socket must close (out-of-sync framing).
    SocketClosed,
    /// The connect handshake failed before reaching the ready state.
    HandshakeFailed,
    /// A push (request build) was rejected because the connection was not
    /// ready to accept a command.
    NotReady,
}

/// The `ReadyForQuery` transaction-status indicator (PG §55.7).
///
/// Names the observable transaction state, not an internal engine enum, so it
/// survives a rebuild.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ObservedTxStatus {
    /// `'I'` — idle, no transaction block in progress.
    #[default]
    Idle,
    /// `'T'` — inside an explicit or implicit transaction block.
    InTransaction,
    /// `'E'` — the current transaction failed; commands are rejected until
    /// `ROLLBACK`.
    Failed,
}

/// One server notice surfaced to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedNotice {
    /// Severity label (`WARNING`, `NOTICE`, `INFO`, `DEBUG`, `LOG`).
    pub severity: String,
    /// SQLSTATE code carried by the notice.
    pub sqlstate: String,
    /// Notice message text.
    pub message: String,
}

/// One asynchronous notification (`LISTEN`/`NOTIFY`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedNotify {
    /// PID of the backend that issued the `NOTIFY`.
    pub pid: i32,
    /// Channel name.
    pub channel: String,
    /// Payload bytes (raw — the wire payload need not be UTF-8).
    pub payload: Vec<u8>,
}

/// The connection's terminal state after a transcript.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservedStatus {
    /// Idle and reusable — a command completed, or a server error recovered
    /// to the ready state.
    Ready,
    /// A terminal protocol error; the connection cannot be reused. The kind
    /// is a stable classification, not an internal error variant.
    Errored(TerminalErrorKind),
    /// The client (or engine) requested socket close — a Terminate packet was
    /// sent, or the engine emitted a close-socket signal.
    Closed,
}

/// Stable classification of a terminal `Errored` state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalErrorKind {
    /// A protocol-level violation drove the connection to its error state.
    Protocol,
    /// The startup handshake failed.
    Handshake,
    /// The error class could not be determined from the public surface.
    Unclassified,
}
