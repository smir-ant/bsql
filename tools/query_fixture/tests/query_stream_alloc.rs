//! CONSTANT-MEMORY proof for the streaming typed-query path.
//!
//! The engine's breakable verb (`query_params_break`, the primitive both drivers'
//! `query_each` wrap) drives a scripted server delivering N `DataRow`s through the
//! SAME sink shape `query_each` uses: decode each row borrowed (zero-copy) and
//! discard it, accumulating NOTHING. With the workspace counting allocator
//! installed, the allocations charged to the drive are BOUNDED INDEPENDENT of N —
//! the ingest buffer's single heap escape (a `Box<[u8; READ_BUF_CAP]>`) plus the
//! send buffer growing once to hold the request, neither of which scales with the
//! row count. A per-row allocation would make `delta(N_large) > delta(N_small)`;
//! the assertion that they are EQUAL (and small) is the streaming guarantee,
//! proven rather than asserted.
//!
//! Offline + deterministic: an in-process scripted `Transport` (no socket, no
//! thread), driven by the single-poll helper. `query_params_break` is exercised
//! directly — the driver `query_each` adds only the thin
//! `Boundary`-to-`Result` mapping over this primitive, so proving the primitive is
//! O(1) proves `query_each` is O(1).
//!
//! # One test, on purpose
//!
//! The counting allocator is process-global (see `query_alloc.rs`): all
//! measurements live in a single `#[test]` fn run sequentially, so no concurrent
//! test thread can allocate inside a measured window.
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "alloc-proof harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

use bsql_devgates::CountingAllocator;
use bsql_postgres_proto::engine::{poll_once, session, Boundary, Outcome, Surface, Transport};
use bsql_postgres_proto::{Credentials, Ident};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

// An all-fixed-width (two `int8 NOT NULL`) row: the borrowed decode reads
// primitives at const offsets and allocates nothing. `orders.id` / `orders.user_id`
// exist in the fixture's migrations.
bsql::query!(Stream, "SELECT id, user_id FROM orders");

// ─────────────────────────── frame builders ───────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// AuthenticationOk + BackendKeyData + ReadyForQuery — the trust handshake.
fn handshake() -> Vec<u8> {
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    let mut out = frame(b'R', &0_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

/// One well-formed `[int8=id][int8=user_id]` `DataRow`.
fn int8_row(id: i64, user_id: i64) -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    for v in [id, user_id] {
        body.extend_from_slice(&8_i32.to_be_bytes());
        body.extend_from_slice(&v.to_be_bytes());
    }
    frame(b'D', &body)
}

/// A `RowDescription` for the `(int8 id, int8 user_id)` row shape — the reply to
/// the `Describe(portal)` a cache MISS appends, which the typed result-schema
/// guard verifies (OIDs [20, 20]) then discards.
fn row_desc() -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    for name in ["id", "user_id"] {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_i32.to_be_bytes()); // table OID
        body.extend_from_slice(&0_i16.to_be_bytes()); // column attr
        body.extend_from_slice(&20_i32.to_be_bytes()); // type OID (int8)
        body.extend_from_slice(&8_i16.to_be_bytes()); // typlen
        body.extend_from_slice(&(-1_i32).to_be_bytes()); // typmod
        body.extend_from_slice(&0_i16.to_be_bytes()); // format
    }
    frame(b'T', &body)
}

/// The full cache-MISS reply for `query_params_break`: CloseComplete,
/// ParseComplete, BindComplete, RowDescription (for the MISS's Describe), `n`
/// DataRows, CommandComplete, ReadyForQuery.
fn miss_reply(n: usize) -> Vec<u8> {
    let mut out = handshake();
    out.extend_from_slice(&frame(b'3', &[])); // CloseComplete
    out.extend_from_slice(&frame(b'1', &[])); // ParseComplete
    out.extend_from_slice(&frame(b'2', &[])); // BindComplete
    out.extend_from_slice(&row_desc()); // RowDescription (Describe portal)
    for i in 0..n {
        let id = i64::try_from(i).expect("row index fits i64");
        out.extend_from_slice(&int8_row(id, id.wrapping_mul(2)));
    }
    let mut cc = format!("SELECT {n}").into_bytes();
    cc.push(0);
    out.extend_from_slice(&frame(b'C', &cc)); // CommandComplete
    out.extend_from_slice(&frame(b'Z', b"I")); // ReadyForQuery (Idle)
    out
}

// ─────────────────────────── scripted transport ───────────────────────────

/// Static cursor server: `read` drains a fixed reply; writes are accepted and
/// discarded; every op resolves synchronously (one-poll). Constructing it (and
/// its inbound `Vec`) happens OUTSIDE the measured window.
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

/// Stream `n` rows through `query_params_break` with the `query_each`-shaped sink
/// (decode borrowed, discard, count), returning `(rows_seen, allocations charged
/// to the measured drive window)`. The script build + engine assembly + handshake
/// all happen OUTSIDE the measured window; only the streaming drive is bracketed.
fn stream_rows(n: usize) -> (usize, usize) {
    let user = Ident::try_from_str("alloc").expect("ident");
    let inbound = miss_reply(n);
    session(
        Script { inbound, cursor: 0 },
        &user,
        None,
        &[],
        Credentials::Trust,
        |mut engine, live| {
            let live = match poll_once(engine.connect(live)) {
                Ok(Ok(live)) => live,
                other => panic!("handshake must reach active, got {other:?}"),
            };

            let mut rows = 0usize;
            let before = ALLOC.snapshot();
            let outcome = poll_once(engine.query_params_break(
                live,
                &Stream::PREPARED,
                (),
                |s| {
                    if let Surface::Row(body) = s {
                        // The exact per-row work `query_each` does: borrowed decode
                        // (zero-copy, discarded), nothing accumulated.
                        if Stream::decode(body).is_ok() {
                            rows += 1;
                        }
                    }
                    ControlFlow::<()>::Continue(())
                },
            ));
            let after = ALLOC.snapshot();
            let allocs = after.delta(before).allocs;

            match outcome {
                Ok(Ok(Outcome { live, status })) => {
                    assert!(
                        matches!(status, Boundary::Idle),
                        "the full stream reaches a clean Idle, got {status:?}"
                    );
                    let _ = live;
                }
                other => panic!("streaming drive failed: {other:?}"),
            }
            (rows, allocs)
        },
    )
    .expect("session assembles")
}

#[test]
fn streaming_is_constant_memory_independent_of_row_count() {
    // Two row counts an order of magnitude apart. Both saturate the ingest heap
    // tier identically, so their measured allocation deltas are EQUAL — the drive
    // allocates a bounded amount for buffer setup and ZERO per row.
    let (small_rows, small_allocs) = stream_rows(200);
    let (large_rows, large_allocs) = stream_rows(20_000);

    assert_eq!(small_rows, 200, "every small-N row streamed to the sink");
    assert_eq!(large_rows, 20_000, "every large-N row streamed to the sink");

    assert_eq!(
        small_allocs, large_allocs,
        "streaming allocations must be independent of row count \
         (200 rows charged {small_allocs}, 20000 rows charged {large_allocs}) — \
         a difference means the path accumulates per row"
    );
    // And that shared constant is tiny (buffer setup: the ingest heap escape + the
    // send buffer growing once for the request, PLUS — because each drive is a fresh
    // cache MISS — the one-time `RowDescription` parse the typed result-schema guard
    // verifies then DROPS, ~4 allocs the freed-immediately owned oids/names Vecs +
    // 2 name Strings), never O(N). A WARM (cache-HIT) stream sends no `Describe`, so
    // it pays none of the guard's ~4; the load-bearing property is the EQUAL delta
    // above (zero per row).
    assert!(
        large_allocs <= 12,
        "the constant setup cost must be small, got {large_allocs} allocations for 20000 rows"
    );
}
