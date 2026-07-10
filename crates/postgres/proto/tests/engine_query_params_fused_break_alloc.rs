//! Constant-memory gate for the FUSED-PARAMS dynamic streaming path
//! (`query_each_params`): a warm `query_params_fused_break` drive streams rows
//! through a REUSED per-row buffer with an allocation count INDEPENDENT of the
//! row count.
//!
//! The peer of `engine_query_break_alloc` for the OTHER breakable dynamic verb:
//! `query_break` (simple query) has its gate there; this pins
//! `query_params_fused_break` (the fused extended-protocol
//! `Parse`+`Bind`+`Describe`+`Execute`+`Sync` primitive behind
//! `query_each_params`). Both dynamic streaming verbs are thus pinned, not just
//! one — even though they share the same `pump_active_to_boundary` + reused-buffer
//! sink, a per-verb gate proves the claim rather than transferring it.
//!
//! Structured identically to `engine_query_break_alloc`: it drives the REAL
//! breakable fused verb over an in-process scripted transport, with a sink that
//! REPRODUCES the driver's `stream_dynamic_row` / `fill_row_slots` decode via the
//! public [`DataRowRef`] walker. An 8-row and a 512-row `query_each_params` stream
//! must allocate the SAME fixed count (the per-query schema parse), NOT a per-row
//! cost.
//!
//! # One test, on purpose
//!
//! The counting allocator is process-global and counts every thread, so the
//! measured windows live in a SINGLE `#[test]` fn run sequentially.

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
use bsql_postgres_proto::engine::{open_owned, poll_once, Boundary, Outcome, Surface, Transport};
use bsql_postgres_proto::{Credentials, DataRowRef, Ident};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

// ─────────────────────────── scripted transport ───────────────────────────

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
    out.extend_from_slice(&param_status("server_version", "16.2"));
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

/// A two-`text`-column `RowDescription` (the fused `Describe`(portal) reply).
fn row_description_2text() -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    for name in ["id", "name"] {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_i32.to_be_bytes()); // table OID
        body.extend_from_slice(&0_i16.to_be_bytes()); // column attr
        body.extend_from_slice(&25_i32.to_be_bytes()); // text
        body.extend_from_slice(&(-1_i16).to_be_bytes()); // varlena
        body.extend_from_slice(&(-1_i32).to_be_bytes()); // type modifier
        body.extend_from_slice(&0_i16.to_be_bytes()); // text format
    }
    frame(b'T', &body)
}

fn two_text_row(id: &str, name: &str) -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    for v in [id, name] {
        let len = i32::try_from(v.len()).expect("value fits i32");
        body.extend_from_slice(&len.to_be_bytes());
        body.extend_from_slice(v.as_bytes());
    }
    frame(b'D', &body)
}

/// The fused extended-protocol reply: `ParseComplete` (`'1'`), `BindComplete`
/// (`'2'`), the `Describe`(portal) `RowDescription`, `n_rows` DataRows,
/// CommandComplete, ReadyForQuery.
fn fused_reply(n_rows: usize) -> Vec<u8> {
    let mut out = frame(b'1', &[]); // ParseComplete
    out.extend_from_slice(&frame(b'2', &[])); // BindComplete
    out.extend_from_slice(&row_description_2text());
    for i in 0..n_rows {
        out.extend_from_slice(&two_text_row(&i.to_string(), "row-name"));
    }
    let mut cc = format!("SELECT {n_rows}").into_bytes();
    cc.push(0);
    out.extend_from_slice(&frame(b'C', &cc));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

// ─────────────────────────── reproduced streaming sink ───────────────────────────
//
// Mirrors the driver's `stream_dynamic_row` / `fill_row_slots` (see
// `engine_query_break_alloc` for the rationale): each `DataRow` is parsed into a
// REUSED slot table via the public `DataRowRef` walker — no copy, no per-row
// allocation — then one column is resolved + touched.

type Slot = (usize, Option<usize>);

fn fill(body: &[u8], slots: &mut Vec<Slot>) {
    slots.clear();
    let row = DataRowRef::parse(body).expect("well-formed scripted row");
    let mut off = 2usize;
    for cell in row.columns() {
        let cell = cell.expect("well-formed scripted column");
        let data_off = off + 4;
        match cell {
            None => {
                slots.push((data_off, None));
                off = data_off;
            }
            Some(bytes) => {
                slots.push((data_off, Some(bytes.len())));
                off = data_off + bytes.len();
            }
        }
    }
}

fn touch_col0(body: &[u8], slots: &[Slot]) {
    if let Some(&(off, Some(len))) = slots.first() {
        let end = off + len;
        let cell = body.get(off..end).expect("in-bounds cell");
        core::hint::black_box(cell);
    }
}

// ─────────────────────────── the gate ───────────────────────────

const SMALL_ROWS: usize = 8;
const LARGE_ROWS: usize = 512;

#[test]
fn warm_query_params_fused_break_stream_alloc_is_independent_of_row_count() {
    let user = Ident::try_from_str("stream").expect("valid ident");

    let mut inbound = handshake();
    inbound.extend_from_slice(&fused_reply(LARGE_ROWS)); // warm-up (grows ingest to max)
    inbound.extend_from_slice(&fused_reply(SMALL_ROWS)); // measured small
    inbound.extend_from_slice(&fused_reply(LARGE_ROWS)); // measured large

    let (mut engine, live) =
        open_owned(Script { inbound, cursor: 0 }, &user, None, &[], Credentials::Trust)
            .expect("session assembles");
    let mut live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake must reach active, got {other:?}"),
    };

    // The reused per-row scratch — the gate's stand-in for the driver verb's
    // `slots` buffer, warm across all drives.
    let mut slots: Vec<Slot> = Vec::new();

    // ---- warm-up drive (UNTIMED): grows the ingest buffer to the LARGEST size. ----
    let mut seen = 0usize;
    live = drive_fused_break(&mut engine, live, &mut slots, &mut seen);
    assert_eq!(seen, LARGE_ROWS, "warm-up saw every row");

    // ---- MEASURED small drive. ----
    let mut seen = 0usize;
    let before = ALLOC.snapshot();
    live = drive_fused_break(&mut engine, live, &mut slots, &mut seen);
    let small_allocs = ALLOC.snapshot().delta(before).allocs;
    assert_eq!(seen, SMALL_ROWS, "small drive saw every row");

    // ---- MEASURED large drive (64× the rows). ----
    let mut seen = 0usize;
    let before = ALLOC.snapshot();
    let live = drive_fused_break(&mut engine, live, &mut slots, &mut seen);
    let large_allocs = ALLOC.snapshot().delta(before).allocs;
    let _ = live;
    assert_eq!(seen, LARGE_ROWS, "large drive saw every one of the big result's rows");

    assert_eq!(
        small_allocs, large_allocs,
        "a fused-params stream of {LARGE_ROWS} rows allocated {large_allocs} but {SMALL_ROWS} rows \
         allocated {small_allocs} — a dynamic stream's allocation count must be INDEPENDENT of the \
         row count (the per-row decode reuses the ingest + slot buffers and touches only stack \
         values). A non-zero difference means a per-row allocation crept into the fused path."
    );
    assert!(
        large_allocs < 32,
        "the fixed per-query fused-stream cost drifted to {large_allocs} — expected a small \
         constant (the RowDescription schema parse), never a per-row cost"
    );
}

/// Drive one `query_params_fused_break` (zero params) to its boundary, streaming
/// every row through the reused `slots` buffer, counting the rows seen.
fn drive_fused_break<'b>(
    engine: &mut bsql_postgres_proto::engine::Engine<'b, Script>,
    live: bsql_postgres_proto::engine::Live<'b>,
    slots: &mut Vec<Slot>,
    seen: &mut usize,
) -> bsql_postgres_proto::engine::Live<'b> {
    let sink = |surface: Surface<'_>| -> ControlFlow<Infallible> {
        if let Surface::Row(body) = surface {
            fill(body, slots);
            touch_col0(body, slots);
            *seen += 1;
        }
        ControlFlow::Continue(())
    };
    match poll_once(engine.query_params_fused_break(live, "SELECT id, name FROM t", &(), sink)) {
        Ok(Ok(Outcome { live, status: Boundary::Idle })) => live,
        other => panic!("query_params_fused_break must stream to a clean idle, got {other:?}"),
    }
}
