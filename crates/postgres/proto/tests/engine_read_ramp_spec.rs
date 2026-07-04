//! Read-syscall-count gate for the per-command inbound read sizing.
//!
//! Each command's inbound read lends the active ingest tier's WHOLE remaining
//! spare (the flat read-sizing), so one `socket.read` fills as much as is
//! available in a single syscall. A response that fits the 128-byte inline tier
//! is drained in one read (no heap escape); a response that overflows it drains
//! in about two — one read that fills the inline tier, then one that fills the
//! heap tier — regardless of how large the response is (up to the 4096-byte
//! read-buffer cap). There is no doubling ramp: the read count does not grow
//! with the response size.
//!
//! An allocation counter is blind to this (a read is a syscall, not a heap
//! allocation). This gate makes the count VISIBLE by driving a representative
//! ~4 KB single-row response over a transport that counts `read` calls, then
//! PINNING the count. A regression back to a per-read cap (e.g. a doubling ramp
//! that offers 128, 256, 512, ... instead of the whole spare) would raise this
//! count and fail the pin — a proven RED→GREEN with a witness.
//!
//! # Why bursted delivery
//!
//! The scripted transport delivers in bursts — one for the handshake, one for
//! the query response — and a single `read` never spans two bursts. This models
//! a real request/response socket: the query response is not on the wire until
//! the query has been sent, so the read that drains the handshake cannot
//! pre-buffer any of the query response. A single preloaded blob would let the
//! handshake's greedy first read grab the head of the query response, conflating
//! the handshake and query costs and undercounting the per-command reads. One
//! burst per logical response isolates the true per-command read count.
//!
//! Offline + deterministic: an in-process scripted transport, single-threaded,
//! synchronous (one `poll_once`). The read counter is a plain atomic the
//! transport bumps on each `read`; the gate resets it AFTER the handshake so it
//! isolates the QUERY reads from the connect reads.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "read-count gate harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bsql_postgres_proto::engine::{open_owned, poll_once, Never, Outcome, Surface, Transport};
use bsql_postgres_proto::{Credentials, Ident};

// ─────────────────── read-counting bursted transport ───────────────────

/// A cursor server that counts every `read` call in a shared atomic and
/// delivers its inbound bytes in BURSTS — a single `read` returns bytes only
/// from the current burst, never spanning two, so a burst boundary forces the
/// caller to issue another `read`. This models a real request/response socket:
/// the handshake burst and the query-response burst arrive at different times,
/// so draining the handshake cannot pre-buffer the query response. Writes are
/// discarded; every op resolves synchronously.
struct CountingScript {
    bursts: Vec<Vec<u8>>,
    burst: usize,
    off: usize,
    reads: Arc<AtomicUsize>,
}

impl CountingScript {
    fn new(bursts: Vec<Vec<u8>>, reads: Arc<AtomicUsize>) -> Self {
        Self {
            bursts,
            burst: 0,
            off: 0,
            reads,
        }
    }
}

impl Transport for CountingScript {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        self.reads.fetch_add(1, Ordering::Relaxed);
        // Deliver from the current burst only — never span a boundary. Skip any
        // fully-drained bursts (each skip advances `burst`, so the loop is
        // bounded by `bursts.len()`); once past the last, it is end-of-stream.
        let n = loop {
            match self.bursts.get(self.burst) {
                Some(cur) if self.off < cur.len() => {
                    let avail = cur.len() - self.off;
                    let n = avail.min(buf.len());
                    let end = self.off + n;
                    if let (Some(dst), Some(src)) = (buf.get_mut(..n), cur.get(self.off..end)) {
                        dst.copy_from_slice(src);
                    }
                    self.off = end;
                    break n;
                }
                Some(_) => {
                    self.burst += 1;
                    self.off = 0;
                }
                None => break 0,
            }
        };
        ready(Ok(n))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(buf.len()))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

// ─────────────────────────── frame builders ───────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame body fits u32 length");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn handshake() -> Vec<u8> {
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    let mut out = frame(b'R', &0_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

/// A one-column `text` `RowDescription`.
fn row_description_text(name: &str) -> Vec<u8> {
    let mut body = 1_i16.to_be_bytes().to_vec();
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(&0_i32.to_be_bytes()); // table OID
    body.extend_from_slice(&0_i16.to_be_bytes()); // column attr number
    body.extend_from_slice(&25_i32.to_be_bytes()); // type OID = text
    body.extend_from_slice(&(-1_i16).to_be_bytes()); // type length (varlena)
    body.extend_from_slice(&(-1_i32).to_be_bytes()); // type modifier
    body.extend_from_slice(&0_i16.to_be_bytes()); // format = text
    frame(b'T', &body)
}

/// A one-`text`-column `DataRow` carrying `value`.
fn text_row(value: &[u8]) -> Vec<u8> {
    let mut body = 1_i16.to_be_bytes().to_vec();
    let len = i32::try_from(value.len()).expect("value fits i32");
    body.extend_from_slice(&len.to_be_bytes());
    body.extend_from_slice(value);
    frame(b'D', &body)
}

fn command_tail() -> Vec<u8> {
    let mut cc = b"SELECT 1".to_vec();
    cc.push(0);
    let mut out = frame(b'C', &cc);
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn no_op_sink(surface: Surface<'_>) -> ControlFlow<Never> {
    let _ = core::hint::black_box(surface);
    ControlFlow::Continue(())
}

/// PINNED baseline: `read` calls to drain one ~4 KB single-row response after
/// the handshake, delivered as its own burst. The flat read-sizing lends the
/// active tier's whole remaining spare per read, so the response is drained in
/// exactly **two** reads: the first fills the 128-byte inline tier (128 bytes)
/// and, on overflow, the buffer escapes to the heap tier and the second read
/// drains the remaining ~3.7 KB in one syscall. The count does NOT grow with
/// the response size (any response up to the 4096-byte read-buffer cap is two
/// reads); a regression to a per-read cap — e.g. a doubling ramp offering 128,
/// 256, 512, 1024, 2048 — would take five reads and fail this pin.
const QUERY_READ_COUNT_PIN: usize = 2;

/// The response payload size the pin is calibrated to: one text row whose value
/// is this many bytes, giving a total response of ~4 KB (row frame plus
/// descriptor plus tail) that stays under the 4096-byte read-buffer cap (so it
/// is a normal row, not an oversize-chunked one) yet overflows the 128-byte
/// inline tier (so it exercises the inline-fill + heap-fill two-read path).
const ROW_VALUE_LEN: usize = 3800;

#[test]
fn per_command_read_count_is_pinned() {
    let user = Ident::try_from_str("ramp").expect("valid ident");
    let reads = Arc::new(AtomicUsize::new(0));

    let value = vec![b'x'; ROW_VALUE_LEN];
    // Two bursts: the handshake, then the whole query response. A single read
    // never spans the boundary, so draining the handshake cannot pre-buffer the
    // query response.
    let mut query_response = row_description_text("payload");
    query_response.extend_from_slice(&text_row(&value));
    query_response.extend_from_slice(&command_tail());

    let transport = CountingScript::new(vec![handshake(), query_response], Arc::clone(&reads));
    let (mut engine, live) =
        open_owned(transport, &user, None, &[], Credentials::Trust).expect("session assembles");

    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake must reach active, got {other:?}"),
    };

    // Isolate the QUERY reads: forget the handshake's reads.
    reads.store(0, Ordering::Relaxed);

    let outcome = poll_once(engine.query(live, "SELECT payload FROM t", no_op_sink));
    match outcome {
        Ok(Ok(Outcome { live, .. })) => {
            let _ = live;
        }
        other => panic!("query must complete, got {other:?}"),
    }

    let count = reads.load(Ordering::Relaxed);
    // The whole response was drained by the flat read-sizing: one inline-fill
    // read, then one heap-fill read — not a per-read-capped ramp.
    assert_eq!(
        count, QUERY_READ_COUNT_PIN,
        "per-command read count drifted from its pin ({QUERY_READ_COUNT_PIN}): got {count}. \
         The flat read-sizing lends the whole tier spare per read, so a ~4 KB response is \
         one inline-fill + one heap-fill = two reads; if a change legitimately alters it \
         (e.g. a per-read cap returns), update QUERY_READ_COUNT_PIN with the new number."
    );
}
