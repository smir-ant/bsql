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
    /// The connection's accumulated parameter status set (key, value) read
    /// from the public session-parameter surface after the run, in a fixed
    /// key order. A duplicate `ParameterStatus` for one key collapses to the
    /// engine's retained value — that collapse is the pinned observable.
    pub parameter_statuses: Vec<(String, String)>,
    /// Asynchronous `NotificationResponse` (`LISTEN`/`NOTIFY`) events
    /// surfaced during any step, in arrival order.
    pub notifications: Vec<ObservedNotify>,
    /// The connection's end state after the last step.
    pub terminal: ObservedStatus,
}

/// A successfully completed command's observable result.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObservedOk {
    /// The server `CommandComplete` tag (e.g. `"SELECT 1"`, `"INSERT 0 1"`),
    /// or the empty string for a command that reports none (e.g. `Ping`).
    pub command_tag: String,
    /// Column names from the most recent `RowDescription`, or empty for a
    /// command that describes no columns.
    pub column_names: Vec<String>,
    /// Result rows as RAW per-column bytes. `None` is SQL `NULL`; `Some(bytes)`
    /// is the column's wire bytes verbatim. Values are NOT decoded to typed
    /// Rust values: decode policy diverges between engines, so the raw bytes
    /// are the stable observable.
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    /// The server's affected-row count, when the command reports one
    /// (`INSERT`/`UPDATE`/`DELETE`/`SELECT`/…); `None` for a command with no
    /// row-count semantics (DDL, transaction control) or none observed.
    pub affected_rows: Option<u64>,
}

/// A failed command's observable result — a server error carries its
/// SQLSTATE, a protocol/transport failure carries a stable classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservedErr {
    /// The server sent an `ErrorResponse`. The SQLSTATE, severity (when the
    /// server included it), and message are the cross-engine observable.
    Server {
        /// 5-character SQLSTATE code.
        sqlstate: String,
        /// Server-reported severity, `None` when omitted.
        severity: Option<String>,
        /// Human-readable message text.
        message: String,
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
