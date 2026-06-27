//! The pure-data description of one replay scenario.
//!
//! A [`Transcript`] is the unit the corpus stores: how to reach the starting
//! state ([`Setup`]), a sequence of client request + scripted server reply
//! [`Step`]s, the transport [`ChunkSchedule`] to fragment the server bytes
//! under, and the [`crate::ObservedRun`] the engine is expected to produce. It
//! names no internal engine type — server frames are raw bytes (built from the
//! public vocabulary in [`crate::frames`]) and requests are a closed,
//! observable [`ClientRequest`] vocabulary.

use crate::observed::ObservedRun;

/// One named replay scenario.
#[derive(Debug, Clone)]
pub struct Transcript {
    /// Human-readable scenario name (used in test failure messages).
    pub name: &'static str,
    /// How to reach the state the steps run from.
    pub setup: Setup,
    /// The client requests and their scripted server replies, in order.
    pub steps: Vec<Step>,
    /// How the scripted server bytes are fragmented as the engine reads them.
    pub chunk_schedule: ChunkSchedule,
    /// The observable result the engine is expected to produce.
    pub expect: ObservedRun,
}

/// How a transcript reaches the state its steps run from.
#[derive(Debug, Clone)]
pub enum Setup {
    /// Drive a canonical trust-auth handshake to an active, ready session.
    /// Sends a minimal `AuthenticationOk` + `BackendKeyData` + `ReadyForQuery`
    /// chain (no `ParameterStatus`), so the session's parameter set starts
    /// empty and a transcript's observed parameters reflect only what its own
    /// steps send.
    ActiveViaTrustHandshake,
    /// Drive the connect handshake feeding these scripted server bytes, then
    /// observe the handshake outcome itself (no further client steps run). For
    /// startup/auth-flow transcripts: the server reply is the whole auth
    /// chain. Honours the chunk schedule.
    StartupScript {
        /// The complete scripted server response to the startup packet.
        server_bytes: Vec<u8>,
    },
    /// Drive the connect handshake against a server that supplies no bytes and
    /// closes — observe the disconnected outcome. The steps (if any) do not
    /// run because no session is produced.
    Disconnected,
}

/// A client request paired with the scripted server bytes that answer it.
#[derive(Debug, Clone)]
pub struct Step {
    /// The request the client issues.
    pub request: ClientRequest,
    /// The complete scripted server reply to this request. Empty for requests
    /// that expect no reply (e.g. [`ClientRequest::Terminate`]).
    pub server_reply: Vec<u8>,
}

impl Step {
    /// Convenience constructor.
    #[must_use]
    pub fn new(request: ClientRequest, server_reply: Vec<u8>) -> Self {
        Self { request, server_reply }
    }
}

/// The closed vocabulary of client requests a transcript can issue.
///
/// Each variant maps to one public `Session` push method (or the public
/// Terminate wire literal). Data-driven variants make "adding a fixture =
/// adding a data value"; the prepared-macro variant exercises the
/// `prepared!` binary-param path through a single corpus-local static query.
#[derive(Debug, Clone)]
pub enum ClientRequest {
    /// A simple-query (`Q`) text command.
    SimpleQuery(String),
    /// A liveness `Ping`.
    Ping,
    /// Parse a statement (`Parse` + `Sync`). The generated statement name is
    /// retained for the following describe/bind/close.
    Prepare(String),
    /// Describe the most recently prepared statement (`Describe` + `Sync`).
    /// Completes the prepared statement (param/row description) for a later
    /// bind.
    DescribeStatement,
    /// Bind + Execute + Sync the most recently prepared statement with these
    /// parameters.
    BindExecute(ParamSpec),
    /// Bind + Execute + Sync the most recently prepared statement with these
    /// parameters and a ROW CAP (`Execute.max_rows = max_rows`, PG §55.2.7).
    /// The server may answer with `PortalSuspended` once `max_rows` rows are
    /// produced, leaving the portal open — the row-limited / portal-suspend
    /// path. `max_rows` must be non-zero (zero is the unlimited `BindExecute`
    /// case); a zero cap is reported as a not-ready push failure.
    BindExecuteRowLimited {
        /// The bind parameters.
        params: ParamSpec,
        /// The `Execute.max_rows` cap. Must be > 0.
        max_rows: u32,
    },
    /// Close the most recently prepared statement (`Close` + `Sync`).
    CloseStatement,
    /// Execute the corpus-local `prepared!` demo query (binary params) with a
    /// single `int4` argument. Exercises the macro path's Parse+Bind+Execute
    /// +Sync round trip.
    ExecutePreparedDemo(i32),
    /// Send the Terminate (`X`) wire literal. No server reply; the connection
    /// is closed.
    Terminate,
}

/// Parameters for a bind/execute, as observable data. Each maps to a concrete
/// binary-encoding tuple the public `ParamsWriter` already implements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParamSpec {
    /// No parameters — the unit tuple `()`.
    None,
    /// One `int4` parameter — `(i32,)`.
    I32(i32),
    /// One `text` parameter — `(&str,)`.
    Text(String),
    /// An `int4` then a `text` parameter — `(i32, &str)`.
    I32Text(i32, String),
}

/// How the scripted server bytes are fragmented as the engine reads them.
///
/// The same fixture replays under each schedule; a fixture that survives
/// `OneBytePerRead` and `SplitHeaders` proves partial-frame resumption (a
/// frame header without its body, a length field split across reads). The
/// schedule fragments READS only — the resulting `ObservedRun` is identical
/// across schedules, which is itself an asserted invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkSchedule {
    /// Deliver the entire server reply in one read.
    AllAtOnce,
    /// Deliver one byte per read — maximal fragmentation.
    OneBytePerRead,
    /// Deliver each frame as its 5-byte header, then its body — exercises
    /// header/body boundary resumption.
    SplitHeaders,
}
