//! Constant-memory allocation gate for the streaming COPY IN write path.
//!
//! Installs the workspace counting allocator and brackets the per-row write loop
//! of a COPY IN driven against an in-process scripted transport (no socket, no
//! thread). The flagship claim PINNED here:
//!
//! **`copy_in_write` is zero-alloc and independent of row count.** Once the send
//! buffer is warm, streaming one `CopyData` frame allocates NOTHING — and, the
//! load-bearing part, streaming 100x the rows allocates the SAME (zero) amount.
//! If the writer accumulated rows instead of streaming them, the larger run would
//! allocate O(rows) more. The send buffer is reset per frame (the drained bytes
//! reclaimed, the capacity retained), so a same-size chunk reuses the warm
//! allocation every time regardless of how many rows stream through.
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
fn copy_in_write_is_zero_alloc_and_independent_of_row_count() {
    let user = Ident::try_from_str("gate").expect("valid ident");

    const SMALL: usize = 100;
    const LARGE: usize = 10_000;
    let chunk: &[u8] = b"42\tpayload-row\n";

    // Reply stream: handshake + three COPY cycles (warm-up, small, large).
    let mut inbound = handshake();
    inbound.extend_from_slice(&copy_in_cycle(8)); // warm-up
    inbound.extend_from_slice(&copy_in_cycle(SMALL)); // measured-small
    inbound.extend_from_slice(&copy_in_cycle(LARGE)); // measured-large

    let (mut engine, live) =
        open_owned(Script { inbound, cursor: 0 }, &user, None, &[], Credentials::Trust)
            .expect("session assembles");

    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake must reach active, got {other:?}"),
    };

    // ---- Warm-up cycle (UNTIMED): grows the send buffer to a chunk's capacity
    // and completes a full begin+write+finish so the measured cycles run warm. ----
    poll_once(engine.copy_in_begin("COPY t FROM STDIN"))
        .expect("poll")
        .expect("warm begin");
    for _ in 0..8 {
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
    match poll_once(engine.copy_in_finish(live, no_op_sink)) {
        Ok(Ok(Outcome { live, .. })) => {
            let _ = live;
        }
        other => panic!("large finish must complete, got {other:?}"),
    }

    // ---- Assertions: the write path is zero-alloc AND count-independent. ----
    assert_eq!(
        small_allocs, 0,
        "streaming {SMALL} COPY rows over warm buffers must not allocate (got {small_allocs})",
    );
    assert_eq!(
        large_allocs, 0,
        "streaming {LARGE} COPY rows over warm buffers must not allocate (got {large_allocs})",
    );
    assert_eq!(
        small_allocs, large_allocs,
        "COPY IN allocation must be INDEPENDENT of row count — a {LARGE}-row stream \
         allocated {large_allocs} vs a {SMALL}-row stream's {small_allocs}; any growth \
         means the writer accumulated rows instead of streaming them",
    );
}
