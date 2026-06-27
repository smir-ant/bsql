//! Allocation proof for the single-residence `read_slot`/`commit` ingest.
//!
//! Installs the workspace counting allocator as this binary's
//! `#[global_allocator]` and brackets each ingest pattern with snapshots.
//! A counting allocator measures exactly one thing — allocation traffic —
//! so this bench proves only ALLOCATION claims; it deliberately does not
//! claim anything about memsets (a `slice.fill(0)` over already-owned
//! storage allocates nothing and is invisible here). Memset-freedom of the
//! hot path is a separate, real gate: the static source-scan in
//! `engine_ingest_memset_guard`. The claims proved here:
//!
//! 1. **Steady-state is zero-alloc.** A buffer that stays in the inline
//!    tier performs an unbounded number of read_slot/commit/next_event
//!    cycles with zero allocations and zero allocated bytes — the zero-once
//!    construction means there is no per-read allocation.
//! 2. **The heap escape is one-time.** The first read_slot whose wanted
//!    total exceeds the inline tier allocates exactly once (the heap
//!    array); every subsequent cycle in the heap tier is again zero-alloc.
//! 3. **Single residence eliminates the copy-in.** The slice `read_slot`
//!    lends is part of the buffer's own storage, so after `commit` the
//!    unread bytes are read from the very address the socket wrote — there
//!    is no second residence and no copy bridging them. (Contrast: the
//!    `append(bytes)` copy-in path memcpys the caller's bytes into the
//!    buffer, a second residence.)
//!
//! # One test, on purpose
//!
//! The counting allocator is process-global and counts every thread.
//! `cargo test` runs `#[test]` fns in parallel, so all measured windows
//! live in a SINGLE `#[test]` fn run sequentially — no concurrent test
//! thread can allocate inside a measured window. (Other test binaries are
//! separate processes with their own allocator instance.)

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    reason = "test harness — the `cycle` helper uses expect() as the loud failure signal; clippy's allow-expect-in-tests carve-out reaches #[test] fns but not free helper fns"
)]
#![allow(
    clippy::panic,
    reason = "test harness — the `cycle` helper panics on an unexpected event variant as the loud failure signal; the allow-panic-in-tests carve-out does not reach free helper fns"
)]

use bsql_devgates::CountingAllocator;
use bsql_postgres_proto::engine::{Event, IngestBuf};
use std::hint::black_box;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

/// Inline-tier capacity mirror (engine-private). The escape window below
/// asks for more than this to force the inline->heap transition.
const INLINE_CAP: usize = 128;

/// A tiny complete frame: tag `'D'`, big-endian length-inclusive `u32` = 5
/// (4 self + 1 payload), payload `b"x"`. A const array, so building it
/// allocates nothing and cannot pollute a measured window.
const SMALL_FRAME: [u8; 6] = [b'D', 0, 0, 0, 5, b'x'];

/// Run one read_slot/commit/next_event cycle for `SMALL_FRAME`, asking for
/// `want` bytes of slot. Returns nothing observable — the point is the
/// allocation traffic the surrounding snapshot measures.
fn cycle(buf: &mut IngestBuf, want: usize) {
    let slot = buf.read_slot(want).expect("slot");
    let n = SMALL_FRAME.len().min(slot.len());
    slot[..n].copy_from_slice(&SMALL_FRAME[..n]);
    let written = n;
    buf.commit(written).expect("commit");
    match buf.next_event() {
        Event::Row(body) => {
            black_box(body);
        }
        other => panic!("expected Row, got {other:?}"),
    }
}

#[test]
fn ingest_steady_state_is_zero_alloc_escape_is_one_time() {
    // ---- (1) Steady-state inline: zero allocations across many cycles. ----
    let mut inline = IngestBuf::new();
    // One untimed warm-up cycle (there is nothing lazy to initialise, but
    // this makes the measured window's "no first-call alloc" explicit).
    cycle(&mut inline, 32);

    let before = ALLOC.snapshot();
    for _ in 0..10_000u32 {
        cycle(&mut inline, 32);
    }
    let after = ALLOC.snapshot();
    let steady_allocs = after.delta(before).allocs;
    let steady_bytes = after.delta(before).bytes;

    // ---- (2) The heap escape allocates exactly once. ----
    // A fresh buffer; the first read_slot whose wanted total exceeds the
    // inline tier escapes to the heap array (one allocation), then we
    // complete the cycle.
    let mut escaping = IngestBuf::new();
    let before = ALLOC.snapshot();
    {
        let slot = escaping.read_slot(INLINE_CAP + 64).expect("escaping slot");
        let n = SMALL_FRAME.len().min(slot.len());
        slot[..n].copy_from_slice(&SMALL_FRAME[..n]);
        escaping.commit(n).expect("commit");
    }
    let after = ALLOC.snapshot();
    let escape_allocs = after.delta(before).allocs;

    // Consume the frame, then run further heap-tier cycles: zero more
    // allocations — the escape was one-time.
    match escaping.next_event() {
        Event::Row(body) => assert_eq!(body, b"x"),
        other => panic!("expected Row after escape, got {other:?}"),
    }
    let before = ALLOC.snapshot();
    for _ in 0..10_000u32 {
        cycle(&mut escaping, 64);
    }
    let after = ALLOC.snapshot();
    let post_escape_allocs = after.delta(before).allocs;

    // ---- (3) Single residence: write destination == read source. ----
    let mut residence = IngestBuf::new();
    let before = ALLOC.snapshot();
    let slot_ptr = {
        let slot = residence.read_slot(32).expect("slot");
        let p = slot.as_ptr();
        let n = SMALL_FRAME.len().min(slot.len());
        slot[..n].copy_from_slice(&SMALL_FRAME[..n]);
        residence.commit(n).expect("commit");
        p
    };
    let read_ptr = residence.unread().as_ptr();
    let after = ALLOC.snapshot();
    let residence_allocs = after.delta(before).allocs;

    // ---- Assertions. ----
    assert_eq!(
        steady_allocs, 0,
        "steady-state inline ingest must not allocate (got {steady_allocs})"
    );
    assert_eq!(
        steady_bytes, 0,
        "steady-state inline ingest must not allocate any bytes (got {steady_bytes})"
    );
    assert_eq!(
        escape_allocs, 1,
        "the inline->heap escape must allocate exactly once (the heap array, got {escape_allocs})"
    );
    assert_eq!(
        post_escape_allocs, 0,
        "heap-tier steady state after the one-time escape must not allocate (got {post_escape_allocs})"
    );
    assert_eq!(
        residence_allocs, 0,
        "lending a slot + committing must not allocate (got {residence_allocs})"
    );
    assert_eq!(
        slot_ptr, read_ptr,
        "single residence: the committed bytes are read from the very address the socket wrote — no copy-in"
    );
}
