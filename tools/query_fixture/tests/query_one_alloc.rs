//! ALLOC witness for `query_one`'s decode-DIRECT path: it does 3 FEWER heap
//! allocations than the prebuffer-then-collect "base" path it replaced.
//!
//! `Core::query_one` used to route through the same streaming collector `query`
//! uses — building a `Rows<Q>` prebuffer (its `wire` byte vector + its `slots`
//! span vector, plus a memcpy of the row bytes into `wire`) and then a
//! per-result owned `Vec` — THREE heap allocations to return ONE owned record.
//! It now decodes the single expected row straight off the wire into an
//! `Option<Q::Owned>`, breaking on a second row: ZERO of those three
//! allocations.
//!
//! This drives the engine's breakable verb (`query_params_break`, the primitive
//! `Core::query_one` is built on) over a scripted single-row reply through BOTH
//! sink shapes and compares the allocations charged to each, with the workspace
//! counting allocator installed. The two drives share the same engine assembly,
//! handshake, ingest buffer, and send buffer, so their alloc delta isolates
//! exactly the three buffering allocations the decode-direct path elides:
//!
//! - **base** — a `RowsBuilder` sink (copy each row into `wire` + push a span),
//!   then `finish::<Q>()` + `into_owned()` (the owned `Vec`).
//! - **direct** — decode the first `Surface::Row` into `Option<Q::Owned>`,
//!   break on a second (the `query_one` shape).
//!
//! Both decode the SAME row to the SAME value, so the win is fewer allocations,
//! not less work. Offline + deterministic (an in-process scripted `Transport`).
//!
//! # One test, on purpose
//!
//! The counting allocator is process-global; all measurements live in one
//! `#[test]` run sequentially so no concurrent thread allocates inside a window.
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "alloc-proof harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

use bsql::{Rows, RowsBuilder, TypedQuery};
use bsql_devgates::CountingAllocator;
use bsql_postgres_proto::engine::{poll_once, session, Boundary, Outcome, Surface, Transport};
use bsql_postgres_proto::{Credentials, Ident};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

// Two `int8 NOT NULL` columns: the owned decode reads primitives at const
// offsets and allocates nothing, so the ONLY allocations the paths differ by are
// the prebuffer's three (a text column would add owned-`String` allocs common to
// both and muddy the delta).
bsql::query!(OneAlloc, "SELECT 1::int8 AS a, 2::int8 AS b");

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

/// One well-formed `[int8=a][int8=b]` `DataRow`.
fn int8_row(a: i64, b: i64) -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    for v in [a, b] {
        body.extend_from_slice(&8_i32.to_be_bytes());
        body.extend_from_slice(&v.to_be_bytes());
    }
    frame(b'D', &body)
}

/// The full cache-MISS reply for `query_params_break` delivering exactly `n`
/// rows: CloseComplete, ParseComplete, BindComplete, `n` DataRows,
/// CommandComplete, ReadyForQuery.
fn miss_reply(n: usize) -> Vec<u8> {
    let mut out = handshake();
    out.extend_from_slice(&frame(b'3', &[])); // CloseComplete
    out.extend_from_slice(&frame(b'1', &[])); // ParseComplete
    out.extend_from_slice(&frame(b'2', &[])); // BindComplete
    for i in 0..n {
        let v = i64::try_from(i).expect("row index fits i64");
        out.extend_from_slice(&int8_row(v.wrapping_add(1), v.wrapping_add(2)));
    }
    let mut cc = format!("SELECT {n}").into_bytes();
    cc.push(0);
    out.extend_from_slice(&frame(b'C', &cc)); // CommandComplete
    out.extend_from_slice(&frame(b'Z', b"I")); // ReadyForQuery (Idle)
    out
}

// ─────────────────────────── scripted transport ───────────────────────────

/// Static cursor server: `read` drains a fixed reply; writes are accepted and
/// discarded; every op resolves synchronously (one-poll).
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

// ─────────────────────────── the two drive shapes ──────────────────────────

/// The "base" prebuffer path: collect the single row into a `RowsBuilder`, then
/// `finish` + `into_owned` + take the first — exactly what `query_one` did
/// before. Returns `(allocations charged, the decoded `a` column)`.
fn drive_base() -> (usize, i64) {
    let user = Ident::try_from_str("alloc").expect("ident");
    let inbound = miss_reply(1);
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
            let mut builder = RowsBuilder::new();
            let before = ALLOC.snapshot();
            let outcome = poll_once(engine.query_params_break(
                live,
                &OneAllocQuery::PREPARED,
                (),
                |s| {
                    builder.feed(s);
                    ControlFlow::<()>::Continue(())
                },
            ));
            let live = match outcome {
                Ok(Ok(Outcome { live, status })) => {
                    assert!(matches!(status, Boundary::Idle), "reached Idle, got {status:?}");
                    live
                }
                other => panic!("base drive failed: {other:?}"),
            };
            let rows: Rows<OneAllocQuery> = builder.finish::<OneAllocQuery>();
            let owned = rows
                .into_owned()
                .expect("row decodes")
                .into_iter()
                .next()
                .expect("exactly one row");
            let allocs = ALLOC.snapshot().delta(before).allocs;
            let _ = live;
            (allocs, owned.a)
        },
    )
    .expect("session assembles")
}

/// The decode-DIRECT path `query_one` now uses: decode the first `Surface::Row`
/// straight into `Option<Q::Owned>`, break on a second. Returns `(allocations
/// charged, the decoded `a` column)`.
fn drive_direct() -> (usize, i64) {
    let user = Ident::try_from_str("alloc").expect("ident");
    let inbound = miss_reply(1);
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
            let mut row: Option<<OneAllocQuery as TypedQuery>::Owned> = None;
            let mut seen_first = false;
            let before = ALLOC.snapshot();
            let outcome = poll_once(engine.query_params_break(
                live,
                &OneAllocQuery::PREPARED,
                (),
                |s| match s {
                    Surface::Row(body) => {
                        if seen_first {
                            return ControlFlow::Break(());
                        }
                        seen_first = true;
                        if let Ok(owned) = OneAllocQuery::decode_owned(body) {
                            row = Some(owned);
                        }
                        ControlFlow::Continue(())
                    }
                    _ => ControlFlow::Continue(()),
                },
            ));
            let live = match outcome {
                Ok(Ok(Outcome { live, status })) => {
                    assert!(matches!(status, Boundary::Idle), "reached Idle, got {status:?}");
                    live
                }
                other => panic!("direct drive failed: {other:?}"),
            };
            let allocs = ALLOC.snapshot().delta(before).allocs;
            let _ = live;
            let owned = row.expect("exactly one row decoded");
            (allocs, owned.a)
        },
    )
    .expect("session assembles")
}

#[test]
fn query_one_decode_direct_saves_three_allocations() {
    let (base_allocs, base_a) = drive_base();
    let (direct_allocs, direct_a) = drive_direct();

    // Same row, same value — the win is fewer allocations, not less work.
    assert_eq!(base_a, 1, "base decoded the `a` column");
    assert_eq!(direct_a, 1, "direct decoded the same `a` column");

    // The two drives share the entire engine cost (the same handshake, ingest
    // buffer, send-buffer growth, and statement-cache insert for the same
    // `PREPARED` over the same reply). The ONLY allocation they differ by is the
    // prebuffer's three — the `wire` byte vector, the `slots` span vector, and
    // `into_owned`'s owned `Vec` — which the decode-direct path elides. So their
    // delta is exactly 3, isolating the win independent of that shared cost.
    assert_eq!(
        base_allocs,
        direct_allocs + 3,
        "query_one decode-direct must do exactly 3 fewer allocations than the \
         prebuffer path (base {base_allocs}, direct {direct_allocs}); a change \
         means the prebuffer elision drifted"
    );
}
