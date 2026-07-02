//! An in-memory, deterministic fake PostgreSQL backend behind the engine's
//! [`Transport`] seam.
//!
//! [`FakeTransport`] is a reactive byte pipe: it accumulates the bytes the
//! driver writes, frames the PostgreSQL *frontend* messages out of them
//! (startup, simple query, terminate), and queues the matching *backend*
//! reply bytes for the driver to read — all synchronously, in memory, with no
//! socket. Because it implements the same [`Transport`] the real socket does,
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
    /// Scripted simple queries: the trimmed SQL text mapped to its complete
    /// reply. Looked up by exact match after trimming ASCII whitespace.
    pub queries: Vec<(String, Vec<u8>)>,
    /// The reply served for a simple query not present in [`queries`](Self::queries)
    /// — a loud `ErrorResponse` + `ReadyForQuery`.
    pub unmatched_reply: Vec<u8>,
    /// The `ErrorResponse` frame (WITHOUT a trailing `ReadyForQuery`) served for
    /// a frontend message the MVP fake does not handle — the extended query
    /// protocol. Emitted exactly ONCE per extended batch: the framer then
    /// discards the rest of the batch until its terminating `Sync`, which
    /// supplies the single [`ready_for_query`](Self::ready_for_query). This
    /// mirrors PostgreSQL's error-then-skip-to-`Sync` recovery, so a failed
    /// extended op leaves the connection clean for the next query.
    pub unsupported_error: Vec<u8>,
    /// A standalone `ReadyForQuery(idle)` frame. Served in response to a `Sync`
    /// — both the `Sync` that terminates a failed extended batch (after
    /// [`unsupported_error`](Self::unsupported_error)) and a bare liveness
    /// `Sync`.
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
    /// Set after an unsupported (extended-protocol) message emitted its single
    /// `ErrorResponse`: every subsequent frontend message is discarded (no
    /// reply) until the batch's `Sync` arrives, which emits the one
    /// `ReadyForQuery` and clears this. PostgreSQL's extended-protocol error
    /// recovery, so a failed extended op does not strand stale reply frames that
    /// would corrupt the next query on a reused connection.
    awaiting_sync: bool,
}

/// One framed frontend message, identified without borrowing the inbox (the
/// SQL range is resolved after the inbox borrow is released).
enum FrontKind {
    /// The untagged startup packet.
    Startup,
    /// A simple query `'Q'`; the SQL text occupies `inbox[start..end]`.
    Query { start: usize, end: usize },
    /// A `Terminate` `'X'`.
    Terminate,
    /// A `Sync` `'S'` — the terminator of an extended-protocol batch (and a
    /// bare liveness sync). Always answered with one `ReadyForQuery`.
    Sync,
    /// Any other frontend tag — the extended query protocol, unsupported here.
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
        }
    }

    /// Frame the untagged startup packet: `[len:u32 incl self][protocol][...]`.
    fn frame_startup(inbox: &[u8]) -> Framed {
        let Some(len_bytes) = inbox.get(..4) else {
            return Framed::Incomplete;
        };
        let mut a = [0u8; 4];
        a.copy_from_slice(len_bytes);
        let total = u32::from_be_bytes(a) as usize;
        if total < 4 || inbox.len() < total {
            return Framed::Incomplete;
        }
        Framed::Message {
            consumed: total,
            kind: FrontKind::Startup,
        }
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
        let len = u32::from_be_bytes(a) as usize;
        let total = len.saturating_add(1); // tag byte is not counted in len
        if len < 4 || inbox.len() < total {
            return Framed::Incomplete;
        }
        let kind = match tag {
            b'Q' => {
                // Body is inbox[5..total] with a trailing NUL; SQL excludes it.
                let end = total.saturating_sub(1).max(5);
                FrontKind::Query { start: 5, end }
            }
            b'X' => FrontKind::Terminate,
            b'S' => FrontKind::Sync,
            _ => FrontKind::Unsupported,
        };
        Framed::Message { consumed: total, kind }
    }

    /// Look up a scripted reply for a trimmed simple-query SQL string.
    fn reply_for_query(&self, sql: &str) -> Vec<u8> {
        match self.script.queries.iter().find(|(k, _)| k.as_str() == sql) {
            Some((_, reply)) => reply.clone(),
            None => self.script.unmatched_reply.clone(),
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
                // clear any failed-batch skip state. Handled first so it fires
                // even while discarding a failed extended batch — that is
                // precisely the Sync the discard was waiting for.
                FrontKind::Sync => {
                    self.awaiting_sync = false;
                    let reply = self.script.ready_for_query.clone();
                    self.outbox.extend_from_slice(&reply);
                }
                // Discarding a failed extended batch: PostgreSQL skips every
                // message until the Sync (handled above). No reply — so the one
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
                FrontKind::Unsupported => {
                    // First message of an unsupported (extended) batch: emit ONE
                    // ErrorResponse, then discard the rest of the batch until its
                    // Sync. A real extended op is Parse+Bind+Execute+Sync (or
                    // Parse+Describe+Sync), so emitting a full E+Z per message
                    // would strand extra E+Z frames that the NEXT query on a
                    // reused connection would wrongly consume.
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
                        Some(sql) => self.reply_for_query(&sql),
                        // Non-UTF-8 SQL matches nothing; a simple query is
                        // self-contained (no Sync follows), so serve the complete
                        // unmatched E+Z reply — never enter the await-Sync state.
                        None => self.script.unmatched_reply.clone(),
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
    use super::{FakeScript, FakeTransport};

    /// A minimal untagged startup packet: `[len][protocol 3.0][terminator]`.
    fn startup_packet() -> Vec<u8> {
        let mut out = vec![0u8; 4];
        out.extend_from_slice(&196608i32.to_be_bytes());
        out.push(0); // empty parameter list terminator
        let len = u32::try_from(out.len()).expect("startup fits u32");
        out[..4].copy_from_slice(&len.to_be_bytes());
        out
    }

    /// A simple-query `'Q'` message carrying `sql`.
    fn query_message(sql: &str) -> Vec<u8> {
        let mut out = vec![b'Q'];
        let body_len = sql.len() + 1 + 4; // sql + NUL + length field
        out.extend_from_slice(&(body_len as u32).to_be_bytes());
        out.extend_from_slice(sql.as_bytes());
        out.push(0);
        out
    }

    /// A bare tagged message with an empty body: `[tag][len=4]`.
    fn empty_tagged(tag: u8) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend_from_slice(&4u32.to_be_bytes());
        out
    }

    fn sample_script() -> FakeScript {
        let handshake = wire::concat(&[
            wire::auth_ok().expect("auth_ok"),
            wire::backend_key_data(42, 0).expect("bkd"),
            wire::ready_for_query(TX_IDLE).expect("rfq"),
        ]);
        let reply = wire::concat(&[
            wire::row_description(&[("col0".to_owned(), wire::OID_INT8)]).expect("rd"),
            wire::data_row(&[Some(b"1".to_vec())]).expect("dr"),
            wire::command_complete("SELECT 1").expect("cc"),
            wire::ready_for_query(TX_IDLE).expect("rfq"),
        ]);
        let unmatched = wire::concat(&[
            wire::error_response("ERROR", "XX000", "unmatched").expect("er"),
            wire::ready_for_query(TX_IDLE).expect("rfq"),
        ]);
        FakeScript {
            handshake,
            queries: vec![("SELECT 1".to_owned(), reply)],
            unmatched_reply: unmatched,
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
        let expected = script.queries[0].1.clone();
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        ft.write_bytes(&query_message("SELECT 1"));
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn trimmed_query_still_matches() {
        let script = sample_script();
        let expected = script.queries[0].1.clone();
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        ft.write_bytes(&query_message("  SELECT 1\n"));
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn unmatched_query_yields_the_unmatched_reply() {
        let script = sample_script();
        let expected = script.unmatched_reply.clone();
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        ft.write_bytes(&query_message("SELECT nope"));
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn extended_batch_coalesces_to_one_error_then_ready() {
        // An extended op is a BATCH: Parse + Describe + Sync. The fake must emit
        // exactly ONE ErrorResponse (for the first message) then ONE
        // ReadyForQuery (for the Sync), never one per message — otherwise the
        // surplus frames strand in the outbox and corrupt the next query.
        let script = sample_script();
        let expected = wire::concat(&[
            script.unsupported_error.clone(),
            script.ready_for_query.clone(),
        ]);
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        ft.write_bytes(&empty_tagged(b'P')); // Parse
        ft.write_bytes(&empty_tagged(b'D')); // Describe
        ft.write_bytes(&empty_tagged(b'S')); // Sync
        assert_eq!(drain(&mut ft), expected);
    }

    #[test]
    fn a_matched_query_after_a_failed_extended_batch_returns_its_rows() {
        // Connection-reuse invariant: after a failed extended batch (one E+Z),
        // the outbox is clean, so a following scripted simple query returns ITS
        // rows — not a stale error frame.
        let script = sample_script();
        let matched = script.queries[0].1.clone();
        let mut ft = FakeTransport::new(script);
        ft.write_bytes(&startup_packet());
        let _ = drain(&mut ft);
        // Failed extended batch.
        ft.write_bytes(&empty_tagged(b'P'));
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
