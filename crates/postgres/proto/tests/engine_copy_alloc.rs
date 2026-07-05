//! Constant-memory allocation gate for the BATCHED COPY IN write path.
//!
//! Installs the workspace counting allocator and brackets the per-row write loop
//! of a COPY IN driven against an in-process scripted transport (no socket, no
//! thread). The batched writer accumulates framed `CopyData` in the send buffer
//! and flushes at a threshold, so the buffer grows ONCE to its BOUNDED
//! steady-state capacity (under `2 * THRESHOLD`) and then never again. The claims
//! PINNED here:
//!
//! **1. Row-count independence at the bounded cap.** After the buffer is warmed
//! to its steady-state capacity (by a warm-up cycle that itself crosses the
//! threshold), streaming N rows allocates NOTHING — and streaming 100x the rows
//! allocates the SAME (zero) amount. If the writer's buffer grew with the row
//! count (never reaching a bound), the larger run would allocate O(rows) more.
//! The buffer is bounded (`reset` reclaims each drained batch, retaining the
//! capacity), so any further row count reuses the warm allocation.
//!
//! **2. Large-chunk passthrough.** A single chunk at or above the threshold is
//! streamed DIRECTLY from the borrowed slice, never copied into the buffer, so
//! streaming a chunk far larger than the warm capacity allocates NOTHING — proof
//! the buffer never absorbs a huge body (were it buffered, the Vec would
//! reallocate to hold it).
//!
//! # One test, one binary, on purpose
//!
//! The counting allocator is process-global and counts every thread. This gate
//! lives in its OWN test binary with a SINGLE `#[test]` fn: `cargo test` runs
//! `#[test]` fns across binaries in separate processes (each with its own
//! allocator instance), and this binary's one fn has no sibling that could
//! allocate inside a measured window.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "alloc-gate harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

use bsql_devgates::CountingAllocator;
use bsql_postgres_proto::engine::{open_owned, poll_once, Never, Outcome, Surface, Transport};
use bsql_postgres_proto::{Credentials, Ident};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

// ─────────────────────────── scripted transport ───────────────────────────

/// Feeds scripted backend bytes on `read`; accepts and discards everything on
/// `write` (a COPY IN client only writes — the server produces nothing until
/// `CopyDone`), so no per-frame allocation is charged to the transport.
struct Script {
    inbound: Vec<u8>,
    cursor: usize,
}

impl Transport for Script {
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

fn param_status(key: &str, value: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(key.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(b'S', &body)
}

/// AuthenticationOk + a run of startup GUC `ParameterStatus` frames +
/// BackendKeyData + ReadyForQuery(Idle).
fn handshake() -> Vec<u8> {
    let mut out = frame(b'R', &0_i32.to_be_bytes());
    for (k, v) in [
        ("client_encoding", "UTF8"),
        ("server_version", "16.2"),
        ("TimeZone", "UTC"),
    ] {
        out.extend_from_slice(&param_status(k, v));
    }
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

/// A `CopyInResponse` ('G'): overall format = 0 (text), 0 per-column formats.
fn copy_in_response() -> Vec<u8> {
    frame(b'G', &[0, 0, 0])
}

/// A COPY-in reply cycle: `CopyInResponse` + `CommandComplete("COPY n")` + RFQ.
fn copy_in_cycle(rows: usize) -> Vec<u8> {
    let mut out = copy_in_response();
    let mut cc = format!("COPY {rows}").into_bytes();
    cc.push(0);
    out.extend_from_slice(&frame(b'C', &cc));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn no_op_sink(surface: Surface<'_>) -> ControlFlow<Never> {
    let _ = core::hint::black_box(surface);
    ControlFlow::Continue(())
}

// ─────────────────────────── the gate ───────────────────────────

#[test]
fn copy_in_write_bounded_buffer_zero_alloc_and_passthrough() {
    let user = Ident::try_from_str("gate").expect("valid ident");

    // The write frame is 5 (CopyData header) + 14 (body) = 19 bytes. WARM streams
    // enough rows to cross the 64 KiB batched-flush threshold several times, so
    // the send buffer reaches its BOUNDED steady-state capacity before any
    // measurement. LARGE repeats the same workload — it must reuse that warm
    // capacity with zero further allocation. BIG_CHUNK is far larger than the warm
    // capacity, so a passthrough (borrowed, unbuffered) stream allocates nothing
    // while a buffered one would reallocate to hold it.
    const WARM: usize = 10_000;
    const SMALL: usize = 100;
    const LARGE: usize = 10_000;
    const BIG_CHUNK_LEN: usize = 512 * 1024;
    let chunk: &[u8] = b"42\tpayload-row\n";
    // Built OUTSIDE every measured window (its one allocation is not charged).
    let big_chunk = vec![b'x'; BIG_CHUNK_LEN];

    // Reply stream: handshake + four COPY cycles (warm, small, large, big-chunk).
    let mut inbound = handshake();
    inbound.extend_from_slice(&copy_in_cycle(WARM)); // warm-up (reaches steady cap)
    inbound.extend_from_slice(&copy_in_cycle(SMALL)); // measured-small
    inbound.extend_from_slice(&copy_in_cycle(LARGE)); // measured-large
    inbound.extend_from_slice(&copy_in_cycle(1)); // measured big-chunk passthrough

    let (mut engine, live) =
        open_owned(Script { inbound, cursor: 0 }, &user, None, &[], Credentials::Trust)
            .expect("session assembles");

    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake must reach active, got {other:?}"),
    };

    // ---- Warm-up cycle (UNTIMED): grows the send buffer to its bounded
    // steady-state capacity (crosses the threshold, so the buffer reaches the
    // same peak the measured cycles will), then a full begin+write+finish. ----
    poll_once(engine.copy_in_begin("COPY t FROM STDIN"))
        .expect("poll")
        .expect("warm begin");
    for _ in 0..WARM {
        poll_once(engine.copy_in_write(chunk))
            .expect("poll")
            .expect("warm write");
    }
    let live = match poll_once(engine.copy_in_finish(live, no_op_sink)) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("warm finish must complete, got {other:?}"),
    };

    // ---- Measured-SMALL: bracket ONLY the SMALL write loop. ----
    poll_once(engine.copy_in_begin("COPY t FROM STDIN"))
        .expect("poll")
        .expect("small begin");
    let before_small = ALLOC.snapshot();
    for _ in 0..SMALL {
        poll_once(engine.copy_in_write(chunk))
            .expect("poll")
            .expect("small write");
    }
    let small_allocs = ALLOC.snapshot().delta(before_small).allocs;
    let live = match poll_once(engine.copy_in_finish(live, no_op_sink)) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("small finish must complete, got {other:?}"),
    };

    // ---- Measured-LARGE: bracket ONLY the LARGE write loop. ----
    poll_once(engine.copy_in_begin("COPY t FROM STDIN"))
        .expect("poll")
        .expect("large begin");
    let before_large = ALLOC.snapshot();
    for _ in 0..LARGE {
        poll_once(engine.copy_in_write(chunk))
            .expect("poll")
            .expect("large write");
    }
    let large_allocs = ALLOC.snapshot().delta(before_large).allocs;
    let live = match poll_once(engine.copy_in_finish(live, no_op_sink)) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("large finish must complete, got {other:?}"),
    };

    // ---- Measured BIG-CHUNK passthrough: bracket ONLY the single large-chunk
    // write. It must allocate NOTHING — the borrowed body is streamed directly,
    // never copied into the (far smaller) send buffer. ----
    poll_once(engine.copy_in_begin("COPY t FROM STDIN"))
        .expect("poll")
        .expect("big begin");
    let before_big = ALLOC.snapshot();
    poll_once(engine.copy_in_write(&big_chunk))
        .expect("poll")
        .expect("big write");
    let big_allocs = ALLOC.snapshot().delta(before_big).allocs;
    match poll_once(engine.copy_in_finish(live, no_op_sink)) {
        Ok(Ok(Outcome { live, .. })) => {
            let _ = live;
        }
        other => panic!("big finish must complete, got {other:?}"),
    }

    // ---- Assertions: bounded buffer (zero-alloc, count-independent) + passthrough. ----
    assert_eq!(
        small_allocs, 0,
        "streaming {SMALL} COPY rows over the warm bounded buffer must not allocate (got {small_allocs})",
    );
    assert_eq!(
        large_allocs, 0,
        "streaming {LARGE} COPY rows over the warm bounded buffer must not allocate (got {large_allocs})",
    );
    assert_eq!(
        small_allocs, large_allocs,
        "COPY IN allocation must be INDEPENDENT of row count — a {LARGE}-row stream \
         allocated {large_allocs} vs a {SMALL}-row stream's {small_allocs}; any growth \
         means the buffer is not bounded",
    );
    assert_eq!(
        big_allocs, 0,
        "a {BIG_CHUNK_LEN}-byte chunk (far larger than the send buffer) must stream \
         DIRECTLY (passthrough) with no allocation (got {big_allocs}); any allocation \
         means it was copied into the buffer, which would reallocate to hold it",
    );
}
