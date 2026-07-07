//! An in-memory, deterministic fake PostgreSQL backend behind the engine's
//! [`Transport`] seam.
//!
//! [`FakeTransport`] is a reactive byte pipe: it accumulates the bytes the
//! driver writes, frames the PostgreSQL *frontend* messages out of them
//! (startup, the simple query, and the extended-query
//! `Parse`/`Bind`/`Execute`/`Close`/`Sync`), and queues the matching *backend*
//! reply bytes for the driver to read — all synchronously, in memory, with no
//! socket. So both `query_sql` (simple) and the compile-checked `query!`
//! (extended) run against it unchanged. Because it implements the same
//! [`Transport`] the real socket does,
//! the real sans-IO engine drives it unchanged and parses its bytes exactly as
//! it would a server's; the driver a consumer's test holds is a genuine
//! [`crate::tls::Wire`]-backed connection, not a mock.
//!
//! # Responsibility split
//!
//! This module owns the *frontend framing* — the one piece of server-side
//! protocol knowledge the client-only engine does not have. It does **not**
//! encode replies: the reply bytes are pre-built by the ergonomic testkit
//! layer (via [`wire`]) and handed in as a [`FakeScript`]. So this type stays
//! a pure, allocation-only, `Send` data interpreter with no failure mode of
//! its own ([`Transport::Error`] is [`Infallible`]): every reply it serves is
//! already a wire-correct frame, including the loud [`ErrorResponse`] the
//! caller supplies for an unmatched query or an unsupported protocol message.
//!
//! [`Transport`]: bsql_postgres_proto::engine::Transport
//! [`ErrorResponse`]: wire::error_response

pub mod wire;

use core::convert::Infallible;
use core::future::{ready, Future};
use std::string::String;
use std::vec::Vec;

use bsql_postgres_proto::engine::Transport;

/// The two pre-built replies for one scripted query — one per wire protocol.
///
/// The SAME scripted query answers both `query_sql` (the simple protocol) and
/// the compile-checked `query!` (the extended protocol), but the two protocols
/// demand different reply bytes: the simple path wants a whole
/// `RowDescription` + text `DataRow`s + `CommandComplete` + `ReadyForQuery`;
/// the extended path wants binary `DataRow`s and no `RowDescription`, framed by
/// the per-message acknowledgements the [`FakeTransport`] emits around it. So
/// one [`FakeScript::queries`] entry carries both, and the framer picks by the
/// protocol it observes.
#[derive(Debug, Clone)]
pub struct QueryReply {
    /// The complete simple-query reply: `RowDescription` + text `DataRow`s +
    /// `CommandComplete` + `ReadyForQuery` (or `ErrorResponse` + `ReadyForQuery`
    /// for a scripted error). Served whole for a `'Q'` simple query.
    pub simple: Vec<u8>,
    /// The extended-query Execute PAYLOAD: binary `DataRow`s +
    /// `CommandComplete` (rows), or a bare `ErrorResponse` (a scripted error).
    /// It carries NO acknowledgement frames and NO trailing `ReadyForQuery` —
    /// the framer emits `ParseComplete` / `BindComplete` before it and the
    /// `Sync`'s `ReadyForQuery` after it, so this is exactly the bytes that ride
    /// the `Execute`.
    pub extended: Vec<u8>,
}

/// The pre-built reply bytes a [`FakeTransport`] serves.
///
/// Every field is a complete, wire-correct PostgreSQL server-reply byte stream
/// (built by the ergonomic testkit layer via [`wire`]). Grouping them keeps
/// the [`FakeTransport`] constructor a single argument and marks the data
/// boundary between the two crates: the encoder is testkit's, the framer is
/// core's.
#[derive(Debug, Clone)]
pub struct FakeScript {
    /// The reply to the startup packet: the trust-auth handshake chain
    /// (`AuthenticationOk` + `ParameterStatus` + `BackendKeyData` +
    /// `ReadyForQuery`).
    pub handshake: Vec<u8>,
    /// Scripted queries: the trimmed SQL text mapped to its [`QueryReply`]
    /// (simple + extended). Looked up by exact match after trimming ASCII
    /// whitespace — the extended path matches the `Parse` message's SQL text,
    /// so one script answers both protocols.
    pub queries: Vec<(String, QueryReply)>,
    /// The reply served for a SIMPLE query not present in
    /// [`queries`](Self::queries) — a loud `ErrorResponse` + `ReadyForQuery`.
    pub unmatched_simple: Vec<u8>,
    /// The Execute-payload served for an EXTENDED query not present in
    /// [`queries`](Self::queries) — a bare loud `ErrorResponse` (no trailing
    /// `ReadyForQuery`; the `Sync` supplies that). Staged at `Parse`/`Bind` and
    /// emitted at `Execute`, so an unscripted `query!` is a loud classified
    /// error, never a silent empty result.
    pub unmatched_extended: Vec<u8>,
    /// The bodyless `ParseComplete` (`'1'`) frame — emitted for each `Parse`.
    pub parse_complete: Vec<u8>,
    /// The bodyless `BindComplete` (`'2'`) frame — emitted for each `Bind`.
    pub bind_complete: Vec<u8>,
    /// The bodyless `CloseComplete` (`'3'`) frame — emitted for each `Close`
    /// (the cache-miss `query!` batch leads with one).
    pub close_complete: Vec<u8>,
    /// The `ErrorResponse` frame (WITHOUT a trailing `ReadyForQuery`) served for
    /// a frontend message the fake does not model — a `Describe`/`Flush`, i.e.
    /// the runtime `prepare` path, not the compile-checked `query!` path.
    /// Emitted exactly ONCE per such batch: the framer then discards the rest of
    /// the batch until its terminating `Sync`, which supplies the single
    /// [`ready_for_query`](Self::ready_for_query). This mirrors PostgreSQL's
    /// error-then-skip-to-`Sync` recovery, so a failed op leaves the connection
    /// clean for the next query.
    pub unsupported_error: Vec<u8>,
    /// A standalone `ReadyForQuery(idle)` frame. Served in response to a `Sync`
    /// — the `Sync` that terminates any extended batch (scripted, unmatched, or
    /// unsupported) and a bare liveness `Sync`.
    pub ready_for_query: Vec<u8>,
}

/// A deterministic in-memory fake PostgreSQL backend as a [`Transport`].
///
/// Plug it into a driver via the testkit connect entry; the driver's engine
/// then drives it exactly as it would a real socket. It never blocks (every
/// op resolves immediately) and never errors at the transport level
/// ([`Transport::Error`] is [`Infallible`]): a fake that cannot answer a
/// request answers with a wire-correct `ErrorResponse`, which surfaces to the
/// caller as a classified `DriverError::Db`, never a hang or a silent empty
/// result.
#[derive(Debug)]
pub struct FakeTransport {
    script: FakeScript,
    /// Client bytes received but not yet framed into complete messages.
    inbox: Vec<u8>,
    /// Server bytes queued for the driver to read.
    outbox: Vec<u8>,
    /// Read cursor into [`outbox`](Self::outbox).
    outbox_pos: usize,
    /// The first frontend message is the untagged startup packet.
    expecting_startup: bool,
    /// A `Terminate` was received (or the connection is otherwise done): reads
    /// return EOF.
    closed: bool,
    /// Set after an UNSUPPORTED (`Describe`/`Flush`) message emitted its single
    /// `ErrorResponse`: every subsequent frontend message is discarded (no
    /// reply) until the batch's `Sync` arrives, which emits the one
    /// `ReadyForQuery` and clears this. PostgreSQL's extended-protocol error
    /// recovery, so a failed op does not strand stale reply frames that would
    /// corrupt the next query on a reused connection. The SUPPORTED extended
    /// messages (`Parse`/`Bind`/`Execute`/`Close`) never set it: they each emit
    /// their own acknowledgement in order.
    awaiting_sync: bool,
    /// The extended-query Execute payload staged by the batch's `Parse` (or, on
    /// a cache-hit re-execute, its `Bind`), consumed by the following `Execute`.
    /// `None` outside an extended batch; cleared at `Sync` so it never leaks
    /// into the next batch.
    staged: Option<Vec<u8>>,
    /// Prepared statements seen on this connection: content-addressed statement
    /// name → its extended Execute payload, recorded at `Parse`. A cache-hit
    /// re-execute (bare `Bind` + `Execute`, no `Parse`) resolves its payload
    /// here, so repeating one `query!` on a single connection keeps working.
    prepared: Vec<(String, Vec<u8>)>,
}

/// One framed frontend message, identified without borrowing the inbox (any
/// string range is resolved after the inbox borrow is released).
enum FrontKind {
    /// The untagged startup packet.
    Startup,
    /// A simple query `'Q'`; the SQL text occupies `inbox[start..end]`.
    Query { start: usize, end: usize },
    /// An extended-protocol `Parse` `'P'`; the statement name occupies
    /// `inbox[name_start..name_end]` and the SQL `inbox[sql_start..sql_end]`.
    Parse {
        /// Start of the statement-name bytes in the inbox.
        name_start: usize,
        /// End (exclusive) of the statement-name bytes.
        name_end: usize,
        /// Start of the SQL bytes in the inbox.
        sql_start: usize,
        /// End (exclusive) of the SQL bytes.
        sql_end: usize,
    },
    /// An extended-protocol `Bind` `'B'`; the source statement name occupies
    /// `inbox[name_start..name_end]` (used only on a cache-hit re-execute).
    Bind {
        /// Start of the source statement-name bytes in the inbox.
        name_start: usize,
        /// End (exclusive) of the source statement-name bytes.
        name_end: usize,
    },
    /// An extended-protocol `Execute` `'E'` — emits the staged payload.
    Execute,
    /// An extended-protocol `Close` `'C'` — emits `CloseComplete`.
    Close,
    /// A `Terminate` `'X'`.
    Terminate,
    /// A `Sync` `'S'` — the terminator of an extended-protocol batch (and a
    /// bare liveness sync). Always answered with one `ReadyForQuery`.
    Sync,
    /// A frontend message the fake does not model — a `Describe`/`Flush` (the
    /// runtime `prepare` path). Answered with one `ErrorResponse`, then the
    /// batch is discarded to its `Sync`.
    Unsupported,
}

/// The result of trying to frame one complete frontend message.
enum Framed {
    /// A complete message occupies the leading `consumed` bytes of the inbox.
    Message { consumed: usize, kind: FrontKind },
    /// Not enough bytes are buffered yet.
    Incomplete,
}

impl FakeTransport {
    /// Build a fake backed by pre-encoded reply bytes.
    #[must_use]
    pub fn new(script: FakeScript) -> Self {
        Self {
            script,
            inbox: Vec::new(),
            outbox: Vec::new(),
            outbox_pos: 0,
            expecting_startup: true,
            closed: false,
            awaiting_sync: false,
            staged: None,
            prepared: Vec::new(),
        }
    }

    /// Frame the untagged startup packet: `[len:u32 incl self][protocol][...]`.
    fn frame_startup(inbox: &[u8]) -> Framed {
        let Some(len_bytes) = inbox.get(..4) else {
            return Framed::Incomplete;
        };
        let mut a = [0u8; 4];
        a.copy_from_slice(len_bytes);
        let Ok(total) = usize::try_from(u32::from_be_bytes(a)) else {
            return Framed::Incomplete;
        };
        if total < 4 || inbox.len() < total {
            return Framed::Incomplete;
        }
        Framed::Message {
            consumed: total,
            kind: FrontKind::Startup,
        }
    }

    /// Find the first `NUL` in `inbox[from..limit]`, returning its absolute
    /// index, or `None` if the range holds no `NUL` (or is out of bounds).
    fn find_nul(inbox: &[u8], from: usize, limit: usize) -> Option<usize> {
        let window = inbox.get(from..limit)?;
        window.iter().position(|&b| b == 0).map(|rel| from.saturating_add(rel))
    }

    /// Parse the first two `NUL`-terminated strings of a message body starting
    /// at `start`, bounded by `limit`. Returns each string's `[start, end)`
    /// range (the `end` is the `NUL` index). `None` if fewer than two
    /// `NUL`-terminated strings are present — a malformed message.
    fn two_cstrings(inbox: &[u8], start: usize, limit: usize) -> Option<(usize, usize, usize, usize)> {
        let first_end = Self::find_nul(inbox, start, limit)?;
        let second_start = first_end.saturating_add(1);
        let second_end = Self::find_nul(inbox, second_start, limit)?;
        Some((start, first_end, second_start, second_end))
    }

    /// Frame one tagged frontend message: `[tag:u8][len:u32 incl self][body]`.
    fn frame_tagged(inbox: &[u8]) -> Framed {
        let Some(&tag) = inbox.first() else {
            return Framed::Incomplete;
        };
        let Some(len_bytes) = inbox.get(1..5) else {
            return Framed::Incomplete;
        };
        let mut a = [0u8; 4];
        a.copy_from_slice(len_bytes);
        let Ok(len) = usize::try_from(u32::from_be_bytes(a)) else {
            return Framed::Incomplete;
        };
        let total = len.saturating_add(1); // tag byte is not counted in len
        if len < 4 || inbox.len() < total {
            return Framed::Incomplete;
        }
        // Every tagged body begins at offset 5 (tag + 4-byte length).
        let kind = match tag {
            b'Q' => {
                // Body is inbox[5..total] with a trailing NUL; SQL excludes it.
                let end = total.saturating_sub(1).max(5);
                FrontKind::Query { start: 5, end }
            }
            b'P' => {
                // Parse: stmt_name NUL, sql NUL, then the param-OID section.
                match Self::two_cstrings(inbox, 5, total) {
                    Some((name_start, name_end, sql_start, sql_end)) => FrontKind::Parse {
                        name_start,
                        name_end,
                        sql_start,
                        sql_end,
                    },
                    // A malformed Parse (missing string terminators) cannot be
                    // matched; treat it as an unsupported message (loud), never
                    // a silent match.
                    None => FrontKind::Unsupported,
                }
            }
            b'B' => {
                // Bind: portal NUL, source stmt_name NUL, then params. Only the
                // second string (the statement name) is needed, for a cache-hit
                // re-execute that carries no Parse.
                match Self::two_cstrings(inbox, 5, total) {
                    Some((_portal_start, _portal_end, name_start, name_end)) => {
                        FrontKind::Bind { name_start, name_end }
                    }
                    None => FrontKind::Unsupported,
                }
            }
            b'E' => FrontKind::Execute,
            b'C' => FrontKind::Close,
            b'X' => FrontKind::Terminate,
            b'S' => FrontKind::Sync,
            // `Describe`/`Flush`/anything else — the runtime `prepare` path and
            // other unmodelled extended ops.
            _ => FrontKind::Unsupported,
        };
        Framed::Message { consumed: total, kind }
    }

    /// Look up the SIMPLE-query reply for a trimmed SQL string, or the unmatched
    /// error reply.
    fn simple_reply_for_query(&self, sql: &str) -> Vec<u8> {
        match self.script.queries.iter().find(|(k, _)| k.as_str() == sql) {
            Some((_, reply)) => reply.simple.clone(),
            None => self.script.unmatched_simple.clone(),
        }
    }

    /// Look up the EXTENDED-query Execute payload for a trimmed SQL string, or
    /// the unmatched extended error payload — never a silent empty result.
    fn extended_payload_for_sql(&self, sql: &str) -> Vec<u8> {
        match self.script.queries.iter().find(|(k, _)| k.as_str() == sql) {
            Some((_, reply)) => reply.extended.clone(),
            None => self.script.unmatched_extended.clone(),
        }
    }

    /// Look up the EXTENDED-query Execute payload for a prepared-statement name
    /// recorded on this connection (a cache-hit re-execute), or the unmatched
    /// extended error payload.
    fn extended_payload_for_stmt(&self, name: &str) -> Vec<u8> {
        match self.prepared.iter().find(|(k, _)| k.as_str() == name) {
            Some((_, payload)) => payload.clone(),
            None => self.script.unmatched_extended.clone(),
        }
    }

    /// Frame every complete buffered frontend message and queue its reply.
    fn advance(&mut self) {
        loop {
            if self.closed {
                self.inbox.clear();
                return;
            }
            let framed = if self.expecting_startup {
                Self::frame_startup(&self.inbox)
            } else {
                Self::frame_tagged(&self.inbox)
            };
            let Framed::Message { consumed, kind } = framed else {
                return;
            };
            match kind {
                // A Sync terminates a batch: emit exactly ONE ReadyForQuery and
                // clear the per-batch state. Handled first so it fires even while
                // discarding an unsupported batch — that is precisely the Sync
                // the discard was waiting for. Clears `staged` so an
                // un-Executed payload (a Parse+Sync with no Execute) never leaks
                // into the next batch.
                FrontKind::Sync => {
                    self.awaiting_sync = false;
                    self.staged = None;
                    let reply = self.script.ready_for_query.clone();
                    self.outbox.extend_from_slice(&reply);
                }
                // Discarding an unsupported batch: PostgreSQL skips every message
                // until the Sync (handled above). No reply — so the one
                // ErrorResponse already emitted is the whole batch's response.
                _ if self.awaiting_sync => {}
                FrontKind::Startup => {
                    self.expecting_startup = false;
                    let reply = self.script.handshake.clone();
                    self.outbox.extend_from_slice(&reply);
                }
                FrontKind::Terminate => {
                    self.closed = true;
                }
                // A `Close` (the cache-miss `query!` batch leads with one): emit
                // its acknowledgement in order.
                FrontKind::Close => {
                    let reply = self.script.close_complete.clone();
                    self.outbox.extend_from_slice(&reply);
                }
                // A `Parse`: match its SQL, stage the Execute payload for the
                // following `Execute`, record it under the statement name so a
                // later cache-hit re-execute resolves it, and acknowledge with
                // `ParseComplete`.
                FrontKind::Parse {
                    name_start,
                    name_end,
                    sql_start,
                    sql_end,
                } => {
                    // Resolve the name + SQL to owned strings so the inbox borrow
                    // is released before the payload lookup and the drain below.
                    let name = self
                        .inbox
                        .get(name_start..name_end)
                        .and_then(|b| core::str::from_utf8(b).ok())
                        .map(str::to_owned);
                    let sql = self
                        .inbox
                        .get(sql_start..sql_end)
                        .and_then(|b| core::str::from_utf8(b).ok())
                        .map(|s| s.trim().to_owned());
                    let payload = match &sql {
                        Some(sql) => self.extended_payload_for_sql(sql),
                        // Non-UTF-8 SQL matches nothing: stage the loud unmatched
                        // payload so the Execute is a classified error.
                        None => self.script.unmatched_extended.clone(),
                    };
                    if let Some(name) = name {
                        self.prepared.push((name, payload.clone()));
                    }
                    self.staged = Some(payload);
                    let ack = self.script.parse_complete.clone();
                    self.outbox.extend_from_slice(&ack);
                }
                // A `Bind`: on a cache-hit re-execute (no Parse this batch)
                // resolve the payload from the recorded statement name;
                // otherwise the Parse already staged it. Acknowledge with
                // `BindComplete`.
                FrontKind::Bind { name_start, name_end } => {
                    if self.staged.is_none() {
                        let name = self
                            .inbox
                            .get(name_start..name_end)
                            .and_then(|b| core::str::from_utf8(b).ok())
                            .map(str::to_owned);
                        let payload = match name {
                            Some(name) => self.extended_payload_for_stmt(&name),
                            None => self.script.unmatched_extended.clone(),
                        };
                        self.staged = Some(payload);
                    }
                    let ack = self.script.bind_complete.clone();
                    self.outbox.extend_from_slice(&ack);
                }
                // An `Execute`: emit the staged payload (binary rows, or a
                // scripted / unmatched error). A missing stage cannot happen for
                // a well-formed batch (a Bind always precedes), but if it does,
                // emit the loud unmatched payload — never a silent empty result.
                FrontKind::Execute => {
                    let payload = match self.staged.take() {
                        Some(payload) => payload,
                        None => self.script.unmatched_extended.clone(),
                    };
                    self.outbox.extend_from_slice(&payload);
                }
                FrontKind::Unsupported => {
                    // First message of an unsupported batch (a Describe/Flush —
                    // the runtime `prepare` path): emit ONE ErrorResponse, then
                    // discard the rest of the batch until its Sync. Emitting a
                    // full E+Z per message would strand extra E+Z frames that the
                    // NEXT query on a reused connection would wrongly consume.
                    self.awaiting_sync = true;
                    let reply = self.script.unsupported_error.clone();
                    self.outbox.extend_from_slice(&reply);
                }
                FrontKind::Query { start, end } => {
                    // Resolve the SQL to an owned string so the inbox borrow is
                    // released before the reply lookup and the drain below.
                    let sql = self
                        .inbox
                        .get(start..end)
                        .and_then(|b| core::str::from_utf8(b).ok())
                        .map(|s| s.trim().to_owned());
                    let reply = match sql {
                        Some(sql) => self.simple_reply_for_query(&sql),
                        // Non-UTF-8 SQL matches nothing; a simple query is
                        // self-contained (no Sync follows), so serve the complete
                        // unmatched E+Z reply — never enter the await-Sync state.
                        None => self.script.unmatched_simple.clone(),
                    };
                    self.outbox.extend_from_slice(&reply);
                }
            }
            self.inbox.drain(..consumed);
        }
    }

    /// Serve queued server bytes into `buf`, returning the count. Returns 0
    /// (EOF) when the outbox is drained — for a correct scripted reply the
    /// engine only reads after a write has queued the whole reply, so a 0-read
    /// mid-session means a malformed fake (surfaced loudly as an unexpected
    /// EOF), never a silent stall.
    fn read_bytes(&mut self, buf: &mut [u8]) -> usize {
        let Some(avail) = self.outbox.get(self.outbox_pos..) else {
            return 0;
        };
        let n = avail.len().min(buf.len());
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), avail.get(..n)) {
            dst.copy_from_slice(src);
        }
        self.outbox_pos = self.outbox_pos.saturating_add(n);
        if self.outbox_pos >= self.outbox.len() {
            self.outbox.clear();
            self.outbox_pos = 0;
        }
        n
    }

    /// Accept client bytes, frame any complete messages, and queue replies.
    fn write_bytes(&mut self, buf: &[u8]) -> usize {
        self.inbox.extend_from_slice(buf);
        self.advance();
        buf.len()
    }
}

impl Transport for FakeTransport {
    /// The fake never fails at the transport level: an unanswerable request is
    /// answered with a wire-correct `ErrorResponse`, not a transport error.
    type Error = Infallible;

    #[inline]
    fn is_would_block(err: &Self::Error) -> bool {
        // Uninhabited: no error value exists, so the question is vacuous.
        match *err {}
    }

    #[inline]
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send + 'a {
        let n = self.read_bytes(buf);
        ready(Ok(n))
    }

    #[inline]
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send + 'a {
        let n = self.write_bytes(buf);
        ready(Ok(n))
    }

    #[inline]
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        ready(Ok(()))
    }

    #[inline]
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    //! Frame-and-reply behaviour without an engine: the fake must serve the
    //! handshake for the startup packet, the scripted reply for a matched
    //! query, the unmatched reply for an unknown query, and the unsupported
    //! reply for an extended-protocol message — all from in-memory buffers.

    use super::wire::{self, TX_IDLE};
    use super::{FakeScript, FakeTransport, QueryReply};

    /// A minimal untagged startup packet: `[len][protocol 3.0][terminator]`.
    fn startup_packet() -> Vec<u8> {
        let mut out = vec![0u8; 4];
        out.extend_from_slice(&196608i32.to_be_bytes());
        out.push(0); // empty parameter list terminator
        let len = u32::try_from(out.len()).expect("startup fits u32");
        out[..4].copy_from_slice(&len.to_be_bytes());
        out
    }

    /// A tagged message carrying `body`: `[tag][len incl self + body][body]`.
    fn tagged(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        let len = u32::try_from(body.len() + 4).expect("body fits u32");
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    /// A simple-query `'Q'` message carrying `sql`.
    fn query_message(sql: &str) -> Vec<u8> {
        let mut body = Vec::from(sql.as_bytes());
        body.push(0);
        tagged(b'Q', &body)
    }

    /// A `Parse` `'P'` message: `stmt_name NUL sql NUL n_params(0)`.
    fn parse_message(stmt_name: &str, sql: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(stmt_name.as_bytes());
        body.push(0);
        body.extend_from_slice(sql.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i16.to_be_bytes()); // no parameter types
        tagged(b'P', &body)
    }

    /// A `Bind` `'B'` message binding an empty portal to `stmt_name` (no
    /// params, all-binary results — the shape the `query!` path emits).
    fn bind_message(stmt_name: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.push(0); // empty portal NUL
        body.extend_from_slice(stmt_name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i16.to_be_bytes()); // no param format codes
        body.extend_from_slice(&0i16.to_be_bytes()); // no param values
        body.extend_from_slice(&1i16.to_be_bytes()); // one result format code:
        body.extend_from_slice(&1i16.to_be_bytes()); // binary
        tagged(b'B', &body)
    }

    /// A bare tagged message with an empty body: `[tag][len=4]`.
    fn empty_tagged(tag: u8) -> Vec<u8> {
        tagged(tag, &[])
    }

    /// The two-row `int8` reply used by both protocols: text for simple, binary
    /// for extended (the flagship `query!` decodes the binary form).
    fn sample_reply() -> QueryReply {
        let simple = wire::concat(&[
            wire::row_description(&[("col0".to_owned(), wire::OID_INT8)]).expect("rd"),
            wire::data_row(&[Some(b"1".to_vec())]).expect("dr text"),
            wire::command_complete("SELECT 1").expect("cc"),
            wire::ready_for_query(TX_IDLE).expect("rfq"),
        ]);
        // Extended Execute payload: binary DataRow(s) + CommandComplete, no acks
        // and no trailing ReadyForQuery (the framer wraps those).
        let extended = wire::concat(&[
            wire::data_row(&[Some(wire::binary_int8(1))]).expect("dr binary"),
            wire::command_complete("SELECT 1").expect("cc"),
        ]);
        QueryReply { simple, extended }
    }

    fn sample_script() -> FakeScript {
        let handshake = wire::concat(&[
            wire::auth_ok().expect("auth_ok"),
            wire::backend_key_data(42, 0).expect("bkd"),
            wire::ready_for_query(TX_IDLE).expect("rfq"),
        ]);
        let unmatched_simple = wire::concat(&[
            wire::error_response("ERROR", "XX000", "unmatched").expect("er"),
            wire::ready_for_query(TX_IDLE).expect("rfq"),
        ]);
        FakeScript {
            handshake,
            queries: vec![("SELECT 1".to_owned(), sample_reply())],
            unmatched_simple,
            unmatched_extended: wire::error_response("ERROR", "XX000", "unmatched").expect("er"),
            parse_complete: wire::parse_complete().expect("1"),
            bind_complete: wire::bind_complete().expect("2"),
            close_complete: wire::close_complete().expect("3"),
            unsupported_error: wire::error_response("ERROR", "0A000", "unsupported").expect("er"),
            ready_for_query: wire::ready_for_query(TX_IDLE).expect("rfq"),
        }
    }

    /// Drain everything the fake currently has queued.
    fn drain(ft: &mut FakeTransport) -> Vec<u8> {
        let mut out = Vec::new();
        let mut buf = [0u8; 64];
        loop {
            let n = ft.read_bytes(&mut buf);
            if n == 0 {
                return out;
            }
            out.extend_from_slice(&buf[..n]);
        }
    }

    #[test]
    fn startup_yields_the_handshake() {
        let script = sample_script();
        let expected = script.handshake.clone();
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn matched_query_yields_the_scripted_reply() {
        let script = sample_script();
        let expected = script.queries[0].1.simple.clone();
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        ft.write_bytes(&query_message("SELECT 1"));
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn trimmed_query_still_matches() {
        let script = sample_script();
        let expected = script.queries[0].1.simple.clone();
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        ft.write_bytes(&query_message("  SELECT 1\n"));
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn unmatched_query_yields_the_unmatched_reply() {
        let script = sample_script();
        let expected = script.unmatched_simple.clone();
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        ft.write_bytes(&query_message("SELECT nope"));
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn matched_extended_query_yields_the_binary_reply() {
        // The `query!` cache-miss batch: Close + Parse + Bind + Execute + Sync.
        // The fake acknowledges each message in order, then serves the binary
        // Execute payload, then the Sync's ReadyForQuery — the exact frame
        // sequence the engine's begin_close_parse_bind_execute path expects.
        let script = sample_script();
        let expected = wire::concat(&[
            script.close_complete.clone(),
            script.parse_complete.clone(),
            script.bind_complete.clone(),
            script.queries[0].1.extended.clone(),
            script.ready_for_query.clone(),
        ]);
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        ft.write_bytes(&empty_tagged(b'C')); // Close (of the statement)
        ft.write_bytes(&parse_message("stmt1", "SELECT 1"));
        ft.write_bytes(&bind_message("stmt1"));
        ft.write_bytes(&empty_tagged(b'E')); // Execute
        ft.write_bytes(&empty_tagged(b'S')); // Sync
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn unmatched_extended_query_is_a_loud_error_at_execute() {
        // An unscripted `query!` must be a loud classified error, never a silent
        // empty result: the unmatched ErrorResponse rides the Execute, then the
        // Sync recovers with ReadyForQuery.
        let script = sample_script();
        let expected = wire::concat(&[
            script.close_complete.clone(),
            script.parse_complete.clone(),
            script.bind_complete.clone(),
            script.unmatched_extended.clone(),
            script.ready_for_query.clone(),
        ]);
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        ft.write_bytes(&empty_tagged(b'C'));
        ft.write_bytes(&parse_message("stmtX", "SELECT nope"));
        ft.write_bytes(&bind_message("stmtX"));
        ft.write_bytes(&empty_tagged(b'E'));
        ft.write_bytes(&empty_tagged(b'S'));
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn cache_hit_reexecute_resolves_from_the_recorded_statement() {
        // A repeat of one `query!` on the same connection: the second run is a
        // bare Bind + Execute + Sync (no Parse). The fake resolves the payload
        // from the statement name recorded by the first run's Parse — so
        // repeating a scripted query keeps returning its rows.
        let script = sample_script();
        let extended = script.queries[0].1.extended.clone();
        let bind_complete = script.bind_complete.clone();
        let rfq = script.ready_for_query.clone();
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        // First run (cache miss) records stmt1 -> payload.
        ft.write_bytes(&empty_tagged(b'C'));
        ft.write_bytes(&parse_message("stmt1", "SELECT 1"));
        ft.write_bytes(&bind_message("stmt1"));
        ft.write_bytes(&empty_tagged(b'E'));
        ft.write_bytes(&empty_tagged(b'S'));
        let _ = drain(&mut ft);
        // Second run (cache hit): bare Bind + Execute + Sync.
        ft.write_bytes(&bind_message("stmt1"));
        ft.write_bytes(&empty_tagged(b'E'));
        ft.write_bytes(&empty_tagged(b'S'));
        let expected = wire::concat(&[bind_complete, extended, rfq]);
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn prepare_style_describe_batch_coalesces_to_one_error_then_ready() {
        // The runtime `prepare` path is Parse + Describe + Sync. The Describe is
        // the message the fake does not model, so it acknowledges the Parse,
        // then emits exactly ONE ErrorResponse for the Describe and ONE
        // ReadyForQuery for the Sync — never one per message, or the surplus
        // would strand and corrupt the next query on a reused connection.
        let script = sample_script();
        let expected = wire::concat(&[
            script.parse_complete.clone(),
            script.unsupported_error.clone(),
            script.ready_for_query.clone(),
        ]);
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        ft.write_bytes(&parse_message("stmtP", "SELECT 1"));
        ft.write_bytes(&empty_tagged(b'D')); // Describe — unsupported
        ft.write_bytes(&empty_tagged(b'S')); // Sync
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn a_matched_query_after_a_failed_describe_batch_returns_its_rows() {
        // Connection-reuse invariant: after a failed prepare-style batch (one
        // E+Z past the ParseComplete), the outbox is clean, so a following
        // scripted simple query returns ITS rows — not a stale error frame.
        let script = sample_script();
        let matched = script.queries[0].1.simple.clone();
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        // Failed prepare-style batch.
        ft.write_bytes(&parse_message("stmtP", "SELECT nope"));
        ft.write_bytes(&empty_tagged(b'D'));
        ft.write_bytes(&empty_tagged(b'S'));
        let _ = drain(&mut ft);
        // Reused connection: the scripted query returns its rows.
        ft.write_bytes(&query_message("SELECT 1"));
        assert_eq!(drain(&mut ft), matched);
    }

    #[test]
    fn a_bare_sync_yields_ready_for_query() {
        // A liveness Sync (not preceded by an error) is answered with a single
        // ReadyForQuery, not an error.
        let script = sample_script();
        let expected = script.ready_for_query.clone();
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        ft.write_bytes(&empty_tagged(b'S'));
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn partial_writes_are_reassembled() {
        let script = sample_script();
        let expected = script.handshake.clone();
        let mut ft = FakeTransport::new(script);
        // Feed the startup packet one byte at a time.
        for b in startup_packet() {
            ft.write_bytes(&[b]);
        }
        assert_eq!(drain(&mut ft), expected);
    }
}
