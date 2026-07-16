#![forbid(unsafe_code)]
//! Witness for the BATCHED close verb (`Engine::close_statements`) — the pool
//! reset's dynamic-cache clear closes N prepared statements in ONE round trip.
//!
//! Drives `close_statements` over a scripted transport that COUNTS `flush` calls
//! and asserts that closing three statements is exactly ONE flush (N `Close`
//! frames + a single `Sync`), not one per statement, and that the drain reaches a
//! clean idle (every `CloseComplete` ack consumed, then the Sync's
//! `ReadyForQuery`).

#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "spec harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bsql_postgres_proto::engine::{poll_once, session, CommandStatus, Never, Outcome, Surface, Transport};
use bsql_postgres_proto::{Credentials, Ident, StmtName};

// ─────────────────────────── frame builders ───────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame body fits u32 length");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn param_status(key: &str, value: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(key.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(b'S', &body)
}

/// AuthenticationOk + a couple of startup `ParameterStatus` + BackendKeyData +
/// ReadyForQuery(Idle) — enough for `connect` to reach the active phase.
fn handshake() -> Vec<u8> {
    let mut out = frame(b'R', &0_i32.to_be_bytes());
    out.extend_from_slice(&param_status("server_version", "16.2"));
    out.extend_from_slice(&param_status("client_encoding", "UTF8"));
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

// ─────────────────────────── flush-counting server ───────────────────────────

/// A scripted server that COUNTS `flush` calls (in a shared atomic) so a test can
/// assert how many round trips a verb costs. Writes are accepted + discarded;
/// every op resolves synchronously (one-poll).
struct FlushCountingServer {
    inbound: Vec<u8>,
    cursor: usize,
    flushes: Arc<AtomicUsize>,
}

impl Transport for FlushCountingServer {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        let n = (self.inbound.len() - self.cursor).min(buf.len());
        let end = self.cursor + n;
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), self.inbound.get(self.cursor..end)) {
            dst.copy_from_slice(src);
        }
        self.cursor = end;
        ready(Ok(n))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(buf.len()))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

// ─────────────────────────── the witness ───────────────────────────

#[test]
fn batched_close_of_many_statements_is_one_flush() {
    let flushes = Arc::new(AtomicUsize::new(0));
    let probe = Arc::clone(&flushes);

    // Reply: handshake, then the batched close's replies — one `CloseComplete`
    // ('3') per statement, then the Sync's ReadyForQuery(Idle).
    let mut inbound = handshake();
    for _ in 0..3 {
        inbound.extend_from_slice(&frame(b'3', &[]));
    }
    inbound.extend_from_slice(&frame(b'Z', b"I"));

    let user = Ident::try_from_str("close").expect("ident");
    let server = FlushCountingServer { inbound, cursor: 0, flushes };

    let flush_delta = session(server, &user, None, &[], Credentials::Trust, |mut engine, live| {
        let live = match poll_once(engine.connect(live)) {
            Ok(Ok(live)) => live,
            other => panic!("handshake must reach active, got {other:?}"),
        };

        // Count flushes charged to the batched close ALONE (post-handshake).
        let before = probe.load(Ordering::Relaxed);
        let n1 = StmtName::try_from_str("_bsql_1").expect("name");
        let n2 = StmtName::try_from_str("_bsql_2").expect("name");
        let n3 = StmtName::try_from_str("_bsql_3").expect("name");
        let names: [&StmtName; 3] = [&n1, &n2, &n3];

        let outcome = poll_once(engine.close_statements(live, &names, |s: Surface<'_>| {
            let _ = core::hint::black_box(s);
            ControlFlow::<Never>::Continue(())
        }));
        let after = probe.load(Ordering::Relaxed);

        match outcome {
            // A clean drain of all three acks + the RFQ ends at Completed.
            Ok(Ok(Outcome { status: CommandStatus::Completed, .. })) => {}
            other => panic!("batched close must complete at idle, got {other:?}"),
        }
        after - before
    })
    .expect("session assembles");

    assert_eq!(
        flush_delta, 1,
        "closing three statements must be ONE flush (N Close + one Sync), not one round trip each"
    );
}
