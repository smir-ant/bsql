//! Constant-memory gate for the DYNAMIC streaming path
//! (`query_each_raw` / `query_each_params`): a warm `query_break` drive that
//! streams rows through a REUSED per-row buffer allocates ZERO — regardless of
//! row count.
//!
//! This is the streaming peer of `engine_query_alloc`'s "warm cache-HIT is
//! zero-alloc": it drives the REAL breakable simple-query engine verb
//! (`query_break`, the primitive behind the driver's `query_each_raw`) over an
//! in-process scripted transport, with a sink that REPRODUCES the driver's
//! `stream_dynamic_row` decode — parsing each `DataRow` into a REUSED slot table
//! via the public [`DataRowRef`] walker (the exact parser the driver's private
//! `fill_row_slots` reuses), exactly as `materialize_alloc` reproduces the
//! driver-private `build_query_result` body.
//!
//! The claim PINNED here: once the ingest buffer and the reused slot buffer are
//! warm, streaming a multi-row result through `query_break` performs ZERO
//! allocations — nothing is accumulated (no per-row `Vec<Row>`, no per-row
//! arena), so a colossal runtime SELECT streams in constant memory. A regression
//! that accumulated per row (or re-allocated the slot buffer instead of clearing
//! it) would turn a warm N-row stream from 0 allocs to N.
//!
//! # One test, on purpose
//!
//! The counting allocator is process-global and counts every thread. `cargo
//! test` runs `#[test]` fns in parallel, so all measured windows live in a
//! SINGLE `#[test]` fn run sequentially.

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

/// A two-`text`-column `RowDescription` (simple-query text results).
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

/// A two-`text`-column `DataRow` (`id`, `name`).
fn two_text_row(id: &str, name: &str) -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    for v in [id, name] {
        let len = i32::try_from(v.len()).expect("value fits i32");
        body.extend_from_slice(&len.to_be_bytes());
        body.extend_from_slice(v.as_bytes());
    }
    frame(b'D', &body)
}

/// A simple-query reply: RowDescription, `n_rows` DataRows, CommandComplete, RFQ.
fn stream_reply(n_rows: usize) -> Vec<u8> {
    let mut out = row_description_2text();
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
// This mirrors the driver's `stream_dynamic_row` / `fill_row_slots`: each
// `DataRow` is parsed into a REUSED slot table (offset + len per column, or NULL)
// via the public `DataRowRef` walker — no copy, no per-row allocation — then one
// column is resolved + touched, exactly the per-row work `BorrowedRow::get` does.

/// A resolved cell: `(offset-into-body, Some(len))` for a value, `None` for NULL.
type Slot = (usize, Option<usize>);

/// Fill `slots` with each column's `(offset, len)` from `body` (cleared first, so
/// a reused buffer allocates nothing per row) — the reproduced `fill_row_slots`.
fn fill(body: &[u8], slots: &mut Vec<Slot>) {
    slots.clear();
    let row = DataRowRef::parse(body).expect("well-formed scripted row");
    let mut off = 2usize; // after the 2-byte column-count header
    for cell in row.columns() {
        let cell = cell.expect("well-formed scripted column");
        let data_off = off + 4; // after the 4-byte length prefix
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

/// Resolve + touch column 0 from `body`/`slots`, exactly the per-row read a
/// caller's `on_row(|row| { row.get::<&str>(0); … })` performs — stack-only, no
/// allocation.
fn touch_col0(body: &[u8], slots: &[Slot]) {
    if let Some(&(off, Some(len))) = slots.first() {
        let end = off + len;
        let cell = body.get(off..end).expect("in-bounds cell");
        core::hint::black_box(cell);
    }
}

// ─────────────────────────── the gate ───────────────────────────

/// A small vs a large stream. Both are driven over an ingest buffer already grown
/// (by a warm-up of the LARGE size) to hold the largest reply, so neither drive
/// pays ingest growth — leaving each drive's allocation count to be the FIXED
/// per-query cost (the RowDescription schema parse: the column-name `String`s +
/// their `Vec`s, once per query, NOT per row). The two counts must be EQUAL: a
/// row-count-independent allocation count is the direct proof that the per-row
/// decode allocates NOTHING (a colossal result streams in constant memory). At
/// ≥1 alloc per row the large drive would allocate ~500 more than the small one.
const SMALL_ROWS: usize = 8;
const LARGE_ROWS: usize = 512;

#[test]
fn warm_query_break_stream_alloc_is_independent_of_row_count() {
    let user = Ident::try_from_str("stream").expect("valid ident");

    let mut inbound = handshake();
    inbound.extend_from_slice(&stream_reply(LARGE_ROWS)); // warm-up (grows ingest to max)
    inbound.extend_from_slice(&stream_reply(SMALL_ROWS)); // measured small
    inbound.extend_from_slice(&stream_reply(LARGE_ROWS)); // measured large

    let (mut engine, live) =
        open_owned(Script { inbound, cursor: 0 }, &user, None, &[], Credentials::Trust)
            .expect("session assembles");
    let mut live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake must reach active, got {other:?}"),
    };

    // The reused per-row scratch — the gate's stand-in for the driver verb's
    // `slots` buffer, DECLARED OUTSIDE the drives so it stays warm across all of
    // them (mirroring how the verb's buffer grows once on the first row then
    // clears + reuses for every later row).
    let mut slots: Vec<Slot> = Vec::new();

    // ---- warm-up drive (UNTIMED): grows the ingest buffer to the LARGEST size. ----
    let mut seen = 0usize;
    live = drive_break(&mut engine, live, &mut slots, &mut seen);
    assert_eq!(seen, LARGE_ROWS, "warm-up saw every row");

    // ---- MEASURED small drive. ----
    let mut seen = 0usize;
    let before = ALLOC.snapshot();
    live = drive_break(&mut engine, live, &mut slots, &mut seen);
    let small_allocs = ALLOC.snapshot().delta(before).allocs;
    assert_eq!(seen, SMALL_ROWS, "small drive saw every row");

    // ---- MEASURED large drive (64× the rows). ----
    let mut seen = 0usize;
    let before = ALLOC.snapshot();
    let live = drive_break(&mut engine, live, &mut slots, &mut seen);
    let large_allocs = ALLOC.snapshot().delta(before).allocs;
    let _ = live;
    assert_eq!(seen, LARGE_ROWS, "large drive saw every one of the big result's rows");

    assert_eq!(
        small_allocs, large_allocs,
        "streaming {LARGE_ROWS} rows allocated {large_allocs} but {SMALL_ROWS} rows \
         allocated {small_allocs} — a dynamic stream's allocation count must be INDEPENDENT \
         of the row count (the per-row decode reuses the ingest + slot buffers and touches \
         only stack values, accumulating nothing). A non-zero difference means a per-row \
         allocation crept in — the constant-memory streaming invariant is broken."
    );
    // And the fixed per-query cost is genuinely O(1) — a handful (the 2-column
    // RowDescription schema parse), never anywhere near the row count.
    assert!(
        large_allocs < 32,
        "the fixed per-query streaming cost drifted to {large_allocs} — expected a small \
         constant (the RowDescription schema parse), never a per-row cost"
    );
}

/// Drive one `query_break` to its boundary, streaming every row through the
/// reused `slots` buffer (parse + touch), counting the rows seen. Returns the
/// restored token.
fn drive_break<'b>(
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
    match poll_once(engine.query_break(live, "SELECT id, name FROM t", sink)) {
        Ok(Ok(Outcome { live, status: Boundary::Idle })) => live,
        other => panic!("query_break must stream to a clean idle, got {other:?}"),
    }
}
