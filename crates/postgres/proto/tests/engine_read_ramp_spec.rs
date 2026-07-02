//! Read-syscall-count gate for the per-command inbound read ramp.
//!
//! The active pump starts each command's read `want` at the inline ingest tier
//! (128 bytes) and DOUBLES it only when a read fills the offered slot, capped at
//! the read-buffer size (4096). A single ~4 KB response therefore takes several
//! `read` calls (128, 256, 512, 1024, 2048, ...), not one — the recurring
//! per-command syscall cost the ramp trades against buffer size.
//!
//! An allocation counter is blind to this (a read is a syscall, not a heap
//! allocation). This gate makes the count VISIBLE by driving a representative
//! ~4 KB single-row response over a transport that counts `read` calls, then
//! PINNING the current count. A later slice that flattens the ramp (start wide,
//! or size the initial want to the typical response) lowers this count and
//! updates the pin — a proven RED→GREEN with a witness.
//!
//! Offline + deterministic: an in-process scripted transport, single-threaded,
//! synchronous (one `poll_once`). The read counter is a plain atomic the
//! transport bumps on each `read`; the gate resets it AFTER the handshake so it
//! isolates the QUERY read ramp from the connect reads.

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

// ─────────────────── read-counting scripted transport ───────────────────

/// A cursor server that counts every `read` call in a shared atomic. Writes are
/// discarded; every op resolves synchronously.
struct CountingScript {
    inbound: Vec<u8>,
    cursor: usize,
    reads: Arc<AtomicUsize>,
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
/// the handshake. The active pump's per-command read ramp starts at 128 bytes
/// and doubles on each fully-filled slot (128, 256, 512, 1024, 2048, ...), so a
/// ~4 KB response is drained in several reads — the pinned count exposes exactly
/// that ramp. The current value is **5**: the ramp offers 128, 256, 512, 1024,
/// 2048 (cumulative 3968), which covers this ~3.86 KB response in five reads. A
/// later slice that flattens the ramp (e.g. starts the initial want wide enough
/// to drain a typical response in one or two reads) lowers this pin.
const QUERY_READ_COUNT_PIN: usize = 5;

/// The response payload size the pin is calibrated to: one text row whose value
/// is this many bytes, giving a total response of ~4 KB (row frame plus
/// descriptor plus tail) that stays under the 4096-byte read-buffer cap (so it
/// is a normal row, not an oversize-chunked one).
const ROW_VALUE_LEN: usize = 3800;

#[test]
fn per_command_read_ramp_count_is_pinned() {
    let user = Ident::try_from_str("ramp").expect("valid ident");
    let reads = Arc::new(AtomicUsize::new(0));

    let value = vec![b'x'; ROW_VALUE_LEN];
    let mut inbound = handshake();
    inbound.extend_from_slice(&row_description_text("payload"));
    inbound.extend_from_slice(&text_row(&value));
    inbound.extend_from_slice(&command_tail());

    let transport = CountingScript {
        inbound,
        cursor: 0,
        reads: Arc::clone(&reads),
    };
    let (mut engine, live) =
        open_owned(transport, &user, None, None, Credentials::Trust).expect("session assembles");

    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake must reach active, got {other:?}"),
    };

    // Isolate the QUERY read ramp: forget the handshake's reads.
    reads.store(0, Ordering::Relaxed);

    let outcome = poll_once(engine.query(live, "SELECT payload FROM t", no_op_sink));
    match outcome {
        Ok(Ok(Outcome { live, .. })) => {
            let _ = live;
        }
        other => panic!("query must complete, got {other:?}"),
    }

    let count = reads.load(Ordering::Relaxed);
    // The whole response fit in one buffer refill sequence — the count reflects
    // the doubling ramp, not the number of frames.
    assert_eq!(
        count, QUERY_READ_COUNT_PIN,
        "per-command read count drifted from its pin ({QUERY_READ_COUNT_PIN}): got {count}. \
         This is the 128→doubling ramp over a ~4 KB response; if a change legitimately \
         alters it (e.g. a flatter ramp), update QUERY_READ_COUNT_PIN with the new number."
    );
}
