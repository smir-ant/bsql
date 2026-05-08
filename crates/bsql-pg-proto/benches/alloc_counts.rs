//! Allocation-traffic bench — deterministic per-scenario alloc /
//! dealloc / bytes-allocated counters.
//!
//! Companion to `hot_paths.rs` (criterion ns/op) for the "B" layer
//! of the `BENCHMARKING.md` measurement stack. While criterion
//! reports wall-clock timings under statistical sampling,
//! `alloc_counts` reports **structural** properties:
//!
//! - **alloc_count** — number of `GlobalAlloc::alloc` calls per
//!   scenario invocation. Determined by the algorithm + types
//!   in scope, not by scheduler / cache state. Same source +
//!   same scenario → exactly the same number, on every run, on
//!   every machine.
//! - **bytes_allocated** — total bytes requested via
//!   `GlobalAlloc::alloc`. Same determinism. Reveals "did the
//!   refactor actually eliminate the heap pressure, or just
//!   move it around?".
//!
//! # Why a separate bench, not extension to hot_paths.rs
//!
//! Criterion runs the timed body N times per measurement window
//! (default ~100K iterations). An alloc count of "1 per call"
//! would surface as "100K total" — useless without per-iter
//! division (and division gets noisy when N varies). This file
//! drives each scenario **exactly once** per measured outcome,
//! making the count an integer answer to "how many allocs does
//! one push of Ping cost?".
//!
//! # Output format (machine-parsed by `scripts/bench-allocs.sh`)
//!
//! ```text
//! ALLOC_BENCH name=ping_round_trip allocs=0 deallocs=0 bytes=0
//! ALLOC_BENCH name=push_command_ping allocs=0 deallocs=0 bytes=0
//! ALLOC_BENCH name=iter_rows_100 allocs=0 deallocs=0 bytes=0
//! ```
//!
//! One line per scenario. Sentinel `ALLOC_BENCH ` prefix lets the
//! shell script `grep '^ALLOC_BENCH '` for stable extraction even
//! if cargo / rustc emits other text on stdout / stderr.
//!
//! # Tier impact
//!
//! Cross-platform (any `GlobalAlloc`-supporting target — macOS,
//! Linux, Windows, embedded). Deterministic by construction
//! (atomics with `Relaxed` ordering — no thread interleaving
//! since the bench is single-threaded). Cost: per-call atomic
//! increment on every alloc / dealloc on the whole process; the
//! bench process is short-lived and doesn't ship to production,
//! so this is acceptable. Production builds use `#[global_allocator]
//! = System` (the default) — the counting wrapper is local to
//! this bench target only.

#![allow(missing_docs, reason = "bench harness — descriptive fn names + module docstring suffice")]

extern crate alloc;

use bsql_pg_proto::{
    ident::Sql,
    reply_id::{PingKind, QueryKind, ReplyId},
    PgProtocol, PushFailure, WriteBuf,
};
use core::num::NonZeroU64;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;

// ---------------------------------------------------------------
// Counting allocator.
// ---------------------------------------------------------------
//
// Wraps the platform `System` allocator with three monotonic
// counters. We use `Relaxed` ordering because:
//   1. The bench is single-threaded — no inter-thread visibility
//      concerns.
//   2. Even if the bench were multithreaded, we'd only need the
//      counter increments to be atomic w.r.t. each other, not
//      ordered w.r.t. other memory operations.
// `Relaxed` is the cheapest atomic — no fence emitted.

struct CountingAllocator {
    inner: System,
    allocs: AtomicUsize,
    deallocs: AtomicUsize,
    bytes_allocated: AtomicUsize,
}

impl CountingAllocator {
    const fn new() -> Self {
        Self {
            inner: System,
            allocs: AtomicUsize::new(0),
            deallocs: AtomicUsize::new(0),
            bytes_allocated: AtomicUsize::new(0),
        }
    }

    fn snapshot(&self) -> AllocSnapshot {
        AllocSnapshot {
            allocs: self.allocs.load(Ordering::Relaxed),
            deallocs: self.deallocs.load(Ordering::Relaxed),
            bytes_allocated: self.bytes_allocated.load(Ordering::Relaxed),
        }
    }
}

// SAFETY: `CountingAllocator` forwards every alloc / dealloc /
// realloc call to `System` unchanged. The atomic counters are
// pure side effect (Relaxed loads/stores cannot reorder w.r.t.
// the System call in any way that affects allocator semantics).
// The only requirement on `GlobalAlloc` impls is that
// alloc/dealloc honor the Layout — we delegate that to System.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.allocs.fetch_add(1, Ordering::Relaxed);
        self.bytes_allocated
            .fetch_add(layout.size(), Ordering::Relaxed);
        // SAFETY: layout is forwarded unchanged from caller, who
        // is responsible for its validity per GlobalAlloc contract.
        unsafe { self.inner.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.deallocs.fetch_add(1, Ordering::Relaxed);
        // SAFETY: ptr+layout pair forwarded unchanged from caller.
        unsafe { self.inner.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        self.allocs.fetch_add(1, Ordering::Relaxed);
        self.bytes_allocated
            .fetch_add(layout.size(), Ordering::Relaxed);
        // SAFETY: same as alloc — Layout forwarded unchanged.
        unsafe { self.inner.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Realloc counts as 1 alloc + 1 dealloc for our purposes
        // (the moral equivalent at the allocator API). bytes only
        // increases for the delta vs old size.
        self.allocs.fetch_add(1, Ordering::Relaxed);
        self.deallocs.fetch_add(1, Ordering::Relaxed);
        // Saturating in case realloc shrinks (new_size < old) —
        // we never decrement the cumulative bytes counter.
        if new_size > layout.size() {
            self.bytes_allocated
                .fetch_add(new_size - layout.size(), Ordering::Relaxed);
        }
        // SAFETY: ptr+layout+new_size triple forwarded unchanged.
        unsafe { self.inner.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

#[derive(Copy, Clone)]
struct AllocSnapshot {
    allocs: usize,
    deallocs: usize,
    bytes_allocated: usize,
}

impl AllocSnapshot {
    fn delta(self, prior: Self) -> AllocDelta {
        AllocDelta {
            allocs: self.allocs.saturating_sub(prior.allocs),
            deallocs: self.deallocs.saturating_sub(prior.deallocs),
            bytes: self.bytes_allocated.saturating_sub(prior.bytes_allocated),
        }
    }
}

#[derive(Copy, Clone)]
struct AllocDelta {
    allocs: usize,
    deallocs: usize,
    bytes: usize,
}

// ---------------------------------------------------------------
// Scenario harness.
// ---------------------------------------------------------------

/// Runs `scenario` once and prints the alloc-delta line.
///
/// The closure may itself allocate during setup (we count that
/// — that's the point). For "true hot-path only" measurement,
/// callers should hoist setup outside of the snapshot pair (see
/// the `iter_rows_100_pull_only` scenario for the pattern).
fn measure<F: FnOnce()>(name: &str, scenario: F) {
    let before = ALLOCATOR.snapshot();
    scenario();
    let after = ALLOCATOR.snapshot();
    let d = after.delta(before);
    println!(
        "ALLOC_BENCH name={} allocs={} deallocs={} bytes={}",
        name, d.allocs, d.deallocs, d.bytes,
    );
}

// ---------------------------------------------------------------
// Fixture builders (mirror hot_paths.rs).
// ---------------------------------------------------------------

fn rfq_frame() -> [u8; 6] {
    [b'Z', 0x00, 0x00, 0x00, 0x05, b'I']
}

fn data_row_frame(len: u16) -> alloc::vec::Vec<u8> {
    let mut out = alloc::vec::Vec::with_capacity(usize::from(len) + 11);
    out.push(b'D');
    let total: u32 = u32::from(len).saturating_add(10);
    out.extend_from_slice(&total.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&u32::from(len).to_be_bytes());
    out.extend(core::iter::repeat_n(b'x', usize::from(len)));
    out
}

fn build_rowdesc() -> alloc::vec::Vec<u8> {
    let mut out = alloc::vec::Vec::new();
    out.push(b'T');
    let name = b"col\0";
    let body_len = 2 + name.len() + 18;
    let total = 4 + body_len;
    out.extend_from_slice(&(u32::try_from(total).unwrap_or(0)).to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(name);
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&25u32.to_be_bytes());
    out.extend_from_slice(&(-1_i16).to_be_bytes());
    out.extend_from_slice(&(-1_i32).to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out
}

fn reply_id_ping(raw: u64) -> ReplyId<PingKind> {
    ReplyId::from_raw(NonZeroU64::new(raw).unwrap_or(NonZeroU64::MIN))
}

fn bench_push_or_panic<C: bsql_pg_proto::push_command::PushCommand>(
    proto: &mut PgProtocol,
    cmd: C,
    wb: &mut WriteBuf,
) -> Result<(), PushFailure> {
    let status = proto.connection_status();
    let Some(g) = proto.as_ready() else {
        panic!("alloc bench fixture: proto must be Idle for push (status = {status:?})");
    };
    g.push_command(cmd, wb)
}

// ---------------------------------------------------------------
// Scenarios.
// ---------------------------------------------------------------
//
// Each scenario isolates ONE measurable outcome. Setup is
// hoisted OUTSIDE the snapshot when the goal is "hot-path-only
// alloc count"; setup is INSIDE when the scenario name reads
// "...with_setup" and we want the full per-call cost (including
// PgProtocol::new + WriteBuf::new).
//
// The crate's design goal is **zero allocations on the steady-
// state hot path** (`#![no_std]` + `#![forbid(alloc::*)]` is the
// near-future intent; today the lib is no_std but bench fixture
// helpers above use `alloc::vec::Vec` for synthetic frame
// construction). Expected outcome from a clean build: every
// hot-path scenario reports allocs=0 deallocs=0 bytes=0.
//
// Any scenario that prints non-zero is a regression signal:
// either the crate started allocating somewhere new (audit it),
// or the fixture builder leaked an allocation into the snapshot
// (revise the scenario boundaries).

fn scenario_parse_header() {
    use bsql_pg_proto::frame::parse_header;
    let rfq = rfq_frame();
    measure("parse_header", || {
        let result = parse_header(black_box(&rfq));
        black_box(result);
    });
}

fn scenario_ping_round_trip() {
    let rfq = rfq_frame();
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    measure("ping_round_trip", || {
        let push_out = bench_push_or_panic(
            &mut proto,
            bsql_pg_proto::push_command::Ping {
                reply: reply_id_ping(1),
            },
            &mut wb,
        );
        let _ = black_box(push_out);
        let feed_out = proto.feed_bytes(black_box(&rfq), &mut wb);
        black_box(feed_out);
    });
}

fn scenario_push_command_only() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    measure("push_command_ping", || {
        let push_out = bench_push_or_panic(
            &mut proto,
            bsql_pg_proto::push_command::Ping {
                reply: reply_id_ping(1),
            },
            &mut wb,
        );
        let _ = black_box(push_out);
    });
}

fn scenario_iter_rows_100() {
    use bsql_pg_proto::row_stream::StreamItem;
    const N_ROWS: u32 = 100;

    // Setup OUTSIDE the snapshot — we want to measure per-row
    // pull cost, not RowDesc / DataRow Vec construction.
    let rowdesc = build_rowdesc();
    let single_row = data_row_frame(16);
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let push_out = bench_push_or_panic(
        &mut proto,
        bsql_pg_proto::push_command::SimpleQuery {
            sql: Sql::from_str_truncating("SELECT x"),
            reply: ReplyId::<QueryKind>::from_raw(NonZeroU64::MIN),
        },
        &mut wb,
    );
    let _ = black_box(push_out);
    let feed_out = proto.feed_bytes(&rowdesc, &mut wb);
    black_box(feed_out);
    for _ in 0..N_ROWS {
        let append_res = proto.feed_inbound(&single_row);
        assert!(
            append_res.is_ok(),
            "alloc bench setup: feed_inbound must succeed",
        );
    }

    measure("iter_rows_100", || {
        let mut stream = proto.iter_rows(&mut wb);
        let mut rows_seen: u32 = 0;
        loop {
            match stream.next_event() {
                StreamItem::Row { .. } => rows_seen = rows_seen.saturating_add(1),
                StreamItem::NeedMore | StreamItem::CloseSocket | StreamItem::Complete { .. } => {
                    break
                }
                _ => break,
            }
        }
        assert!(
            rows_seen >= N_ROWS,
            "alloc bench: expected {N_ROWS} rows, pulled {rows_seen}",
        );
        black_box(rows_seen);
    });
}

fn scenario_advance_one_frame() {
    use bsql_pg_proto::FeedEvent;
    let rfq = rfq_frame();
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let push_out = bench_push_or_panic(
        &mut proto,
        bsql_pg_proto::push_command::Ping {
            reply: reply_id_ping(1),
        },
        &mut wb,
    );
    let _ = black_box(push_out);
    let append_res = proto.feed_inbound(&rfq);
    assert!(append_res.is_ok(), "alloc bench setup: feed_inbound");

    measure("advance_one_frame", || {
        let event = proto.advance_one_frame(&mut wb);
        match event {
            FeedEvent::Deliver(_, _) | FeedEvent::Idle | FeedEvent::NeedMoreBytes => {}
            other => {
                let _ = black_box(other);
            }
        }
    });
}

// ---------------------------------------------------------------
// Main.
// ---------------------------------------------------------------
//
// Run each scenario once and print one ALLOC_BENCH line per
// scenario. Order is stable so save / compare diffs are
// alignable line-by-line.

fn main() {
    // Header line documents the schema; shell script ignores it.
    println!("# alloc_counts bench — one ALLOC_BENCH line per scenario");
    println!("# format: ALLOC_BENCH name=<name> allocs=<N> deallocs=<N> bytes=<N>");

    scenario_parse_header();
    scenario_push_command_only();
    scenario_ping_round_trip();
    scenario_advance_one_frame();
    scenario_iter_rows_100();

    // Final line gives the bench-runner script a sentinel to
    // confirm the binary ran to completion (vs panicking
    // mid-scenario).
    println!("# alloc_counts bench complete");
}
