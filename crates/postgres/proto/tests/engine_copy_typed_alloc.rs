//! Constant-memory allocation gate for the TYPED binary COPY IN write path.
//!
//! The peer of `engine_copy_alloc.rs` for `copy_in_write_binary_row`: it streams
//! typed rows (each encoded in place onto the growable send buffer through the
//! shared `ParamsWriter` leaves — the PGCOPY binary `int16 field-count` +
//! `{len, bytes}`/`-1` per field) and brackets ONLY the write loop with the
//! process-global counting allocator. The claim PINNED here:
//!
//! **Row-count independence at the bounded cap.** After the send buffer is warmed
//! to its bounded steady-state capacity (a warm-up cycle that crosses the 64 KiB
//! flush threshold), streaming N typed rows allocates NOTHING — and streaming
//! 100x the rows allocates the SAME (zero) amount. The typed row is encoded
//! DIRECTLY onto the send buffer (no per-row scratch `Vec`), and `reset` reclaims
//! each drained batch retaining the capacity, so any further row count reuses the
//! warm allocation. If the writer buffered per row (or grew with the row count),
//! the larger run would allocate O(rows) more.
//!
//! # One test, one binary, on purpose
//!
//! The counting allocator is process-global. This gate lives in its OWN binary
//! with a SINGLE `#[test]` fn so no sibling allocates inside a measured window
//! (see `engine_copy_alloc.rs`).

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
/// `write` — a COPY IN client only writes until `CopyDone`.
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

fn handshake() -> Vec<u8> {
    let mut out = frame(b'R', &0_i32.to_be_bytes());
    for (k, v) in [("client_encoding", "UTF8"), ("server_version", "16.2")] {
        out.extend_from_slice(&param_status(k, v));
    }
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

/// A `CopyInResponse` ('G'): overall format = 1 (binary), 0 per-column formats.
fn copy_in_response_binary() -> Vec<u8> {
    frame(b'G', &[1, 0, 0])
}

/// A COPY-in reply cycle: `CopyInResponse` + `CommandComplete("COPY n")` + RFQ.
fn copy_in_cycle(rows: usize) -> Vec<u8> {
    let mut out = copy_in_response_binary();
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
fn copy_in_write_binary_row_bounded_buffer_zero_alloc() {
    let user = Ident::try_from_str("gate").expect("valid ident");

    // Each typed row (i64, &str) frames to 'd'(1)+len(4)+count(2)+12+11 ≈ 34 bytes.
    // WARM crosses the 64 KiB threshold many times so the send buffer reaches its
    // bounded steady-state capacity; LARGE repeats the workload and must reuse
    // that warm capacity with zero further allocation.
    const WARM: usize = 10_000;
    const SMALL: usize = 100;
    const LARGE: usize = 10_000;
    // A borrowed &'static str field — no per-row owned String, so the row-source
    // borrow allocates nothing in the measured window.
    let row: (i64, &str) = (42, "payload-row");

    let mut inbound = handshake();
    inbound.extend_from_slice(&copy_in_cycle(WARM));
    inbound.extend_from_slice(&copy_in_cycle(SMALL));
    inbound.extend_from_slice(&copy_in_cycle(LARGE));

    let (mut engine, live) =
        open_owned(Script { inbound, cursor: 0 }, &user, None, &[], Credentials::Trust)
            .expect("session assembles");

    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake must reach active, got {other:?}"),
    };

    // ---- Warm-up (UNTIMED): grow the buffer to its bounded steady-state cap. ----
    poll_once(engine.copy_in_begin(
        "COPY t FROM STDIN WITH (FORMAT binary)",
        |_s| ControlFlow::Continue(()),
    ))
    .expect("poll")
    .expect("warm begin");
    for _ in 0..WARM {
        poll_once(engine.copy_in_write_binary_row(&row))
            .expect("poll")
            .expect("warm write");
    }
    let live = match poll_once(engine.copy_in_finish(live, no_op_sink)) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("warm finish must complete, got {other:?}"),
    };

    // ---- Measured-SMALL: bracket ONLY the SMALL typed-row loop. ----
    poll_once(engine.copy_in_begin(
        "COPY t FROM STDIN WITH (FORMAT binary)",
        |_s| ControlFlow::Continue(()),
    ))
    .expect("poll")
    .expect("small begin");
    let before_small = ALLOC.snapshot();
    for _ in 0..SMALL {
        poll_once(engine.copy_in_write_binary_row(&row))
            .expect("poll")
            .expect("small write");
    }
    let small_allocs = ALLOC.snapshot().delta(before_small).allocs;
    let live = match poll_once(engine.copy_in_finish(live, no_op_sink)) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("small finish must complete, got {other:?}"),
    };

    // ---- Measured-LARGE: bracket ONLY the LARGE typed-row loop. ----
    poll_once(engine.copy_in_begin(
        "COPY t FROM STDIN WITH (FORMAT binary)",
        |_s| ControlFlow::Continue(()),
    ))
    .expect("poll")
    .expect("large begin");
    let before_large = ALLOC.snapshot();
    for _ in 0..LARGE {
        poll_once(engine.copy_in_write_binary_row(&row))
            .expect("poll")
            .expect("large write");
    }
    let large_allocs = ALLOC.snapshot().delta(before_large).allocs;
    match poll_once(engine.copy_in_finish(live, no_op_sink)) {
        Ok(Ok(Outcome { live, .. })) => {
            let _ = live;
        }
        other => panic!("large finish must complete, got {other:?}"),
    }

    // ---- Assertions: bounded buffer, zero-alloc, count-independent. ----
    assert_eq!(
        small_allocs, 0,
        "streaming {SMALL} typed binary-COPY rows over the warm bounded buffer must not allocate (got {small_allocs})",
    );
    assert_eq!(
        large_allocs, 0,
        "streaming {LARGE} typed binary-COPY rows over the warm bounded buffer must not allocate (got {large_allocs})",
    );
    assert_eq!(
        small_allocs, large_allocs,
        "typed binary-COPY allocation must be INDEPENDENT of row count — a {LARGE}-row \
         stream allocated {large_allocs} vs a {SMALL}-row stream's {small_allocs}; any \
         growth means the buffer is not bounded (or a per-row scratch leaked)",
    );
}
