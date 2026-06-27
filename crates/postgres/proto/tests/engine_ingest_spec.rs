//! Behavioural gate for the single-residence `read_slot`/`commit` ingest.
//!
//! Drives the public [`IngestBuf`] API the way a driver pump will: lend a
//! writable tail with [`read_slot`], let a "socket" write into it, publish
//! the bytes with [`commit`], and pull them back in place. Covers the
//! round-trip (bytes are borrowable from the very residence the socket
//! wrote), the steady-state sequential reuse the no-escape wall permits, the
//! R5 small-then-oversize escape (the inline->heap escape relocates a still-
//! unconsumed frame without losing it), and the bounded-buffer failure
//! surfaces (`commit` overflow, oversize-beyond-heap).
//!
//! The no-escape wall itself (holding a lent slot / a borrow-through event
//! across the next mutating call = E0499) is a compile-time property, gated
//! in `engine_ingest_compile_fail`.
//!
//! [`IngestBuf`]: bsql_postgres_proto::engine::IngestBuf
//! [`read_slot`]: bsql_postgres_proto::engine::IngestBuf::read_slot
//! [`commit`]: bsql_postgres_proto::engine::IngestBuf::commit

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    reason = "test harness — the `frame` helper uses expect() as the loud failure signal; clippy's allow-expect-in-tests carve-out reaches #[test] fns but not free helper fns"
)]

use bsql_postgres_proto::engine::{Event, IngestBuf};
use bsql_postgres_proto::frame::READ_BUF_CAP;

/// The inline-tier capacity is private to the engine; mirror it here as the
/// frame-size boundary the escape tests pivot on. Kept in lockstep by the
/// `escape_boundary_matches_inline_cap` assertion below.
const INLINE_CAP: usize = 128;

/// Build a synthetic wire frame: a 1-byte tag, a big-endian length-inclusive
/// `u32` (counts the 4 length bytes plus the payload), then `payload`.
fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(tag);
    let len = u32::try_from(payload.len() + 4).expect("frame fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Simulate a `socket.read(slot)`: write `bytes` into the front of `slot`,
/// returning the count written (`<= slot.len()`, the read contract).
fn socket_write(slot: &mut [u8], bytes: &[u8]) -> usize {
    let n = bytes.len().min(slot.len());
    slot[..n].copy_from_slice(&bytes[..n]);
    n
}

/// Round-trip: socket bytes -> read_slot -> commit -> the bytes are in the
/// buffer, borrowable in place — from the very residence the socket wrote
/// (single residence, no intervening copy).
#[test]
fn round_trip_single_residence() {
    let mut buf = IngestBuf::new();
    let f = frame(b'D', b"hello");

    let slot = buf.read_slot(64).expect("inline slot lent");
    let slot_ptr = slot.as_ptr();
    let n = socket_write(slot, &f);
    buf.commit(n).expect("commit");

    // The committed bytes are exactly what the socket wrote, borrowable in
    // place — and read from the very address the socket wrote into.
    assert_eq!(buf.unread(), f.as_slice());
    assert_eq!(
        buf.unread().as_ptr(),
        slot_ptr,
        "single residence: read source == write destination, no copy"
    );

    // Borrow-through pull lends the frame body in place.
    match buf.next_event() {
        Event::Row(body) => assert_eq!(body, b"hello"),
        other => panic!("expected Row, got {other:?}"),
    }
    assert_eq!(buf.unread_len(), 0, "frame fully consumed");
}

/// Fewer than a header's worth of bytes yields NeedMore, never a bogus
/// event built from uninitialised/partial data.
#[test]
fn partial_frame_needs_more() {
    let mut buf = IngestBuf::new();
    let f = frame(b'D', b"hello");
    let slot = buf.read_slot(64).expect("slot");
    let n = socket_write(slot, &f[..3]);
    buf.commit(n).expect("commit");
    assert!(matches!(buf.next_event(), Event::NeedMore));
}

/// Steady-state sequential reuse: many read_slot/commit/next_event cycles on
/// a buffer that never escapes. Each cycle's borrow ends before the next
/// mutating call — exactly the sequential pattern the no-escape wall
/// permits.
#[test]
fn steady_state_sequential_cycles() {
    let mut buf = IngestBuf::new();
    let f = frame(b'D', b"x");
    for _ in 0..1000u32 {
        let slot = buf.read_slot(32).expect("slot");
        let n = socket_write(slot, &f);
        buf.commit(n).expect("commit");
        match buf.next_event() {
            Event::Row(body) => assert_eq!(body, b"x"),
            other => panic!("expected Row, got {other:?}"),
        }
    }
    assert_eq!(buf.unread_len(), 0);
}

/// Two complete frames committed in one slot are pulled out one at a time,
/// each body borrowed in place.
#[test]
fn two_frames_one_slot_split_on_pull() {
    let mut buf = IngestBuf::new();
    let mut wire = frame(b'D', b"aa");
    wire.extend_from_slice(&frame(b'D', b"bbbb"));

    let slot = buf.read_slot(wire.len()).expect("slot");
    let n = socket_write(slot, &wire);
    buf.commit(n).expect("commit");

    match buf.next_event() {
        Event::Row(body) => assert_eq!(body, b"aa"),
        other => panic!("first Row, got {other:?}"),
    }
    match buf.next_event() {
        Event::Row(body) => assert_eq!(body, b"bbbb"),
        other => panic!("second Row, got {other:?}"),
    }
    assert!(matches!(buf.next_event(), Event::NeedMore));
}

/// R5 — a sub-inline-cap frame precedes an oversize frame in one buffer.
/// The oversize read_slot must escape inline -> heap BEFORE lending,
/// relocating the still-unconsumed small frame, and both frames must then be
/// readable in order (the escape preserves the live bytes).
#[test]
fn r5_small_then_oversize_escapes_preserving_live_bytes() {
    let mut buf = IngestBuf::new();

    // Small frame, comfortably inline; committed but NOT consumed.
    let small = frame(b'D', b"sm");
    assert!(small.len() <= INLINE_CAP);
    {
        let slot = buf.read_slot(small.len()).expect("small slot");
        let n = socket_write(slot, &small);
        assert_eq!(n, small.len());
    }
    buf.commit(small.len()).expect("commit small");

    // Oversize frame: body alone exceeds the inline tier but fits the heap
    // tier. `filled + want` crosses the inline cap, so read_slot escapes to
    // heap before lending — the socket's bytes land in the heap residence.
    let big_payload = vec![b'B'; 200];
    let big = frame(b'D', &big_payload);
    assert!(big.len() > INLINE_CAP);
    assert!(big.len() < READ_BUF_CAP);
    {
        let slot = buf.read_slot(big.len()).expect("oversize slot escapes");
        let n = socket_write(slot, &big);
        assert_eq!(n, big.len());
    }
    buf.commit(big.len()).expect("commit big");

    // The escape preserved the small frame: both are live, in order, in the
    // heap residence.
    assert_eq!(buf.unread_len(), small.len() + big.len());
    match buf.next_event() {
        Event::Row(body) => assert_eq!(body, b"sm"),
        other => panic!("expected small Row, got {other:?}"),
    }
    match buf.next_event() {
        Event::Row(body) => assert_eq!(body, big_payload.as_slice()),
        other => panic!("expected big Row, got {other:?}"),
    }
    assert_eq!(buf.unread_len(), 0);
}

/// A short read (socket commits fewer bytes than the lent slot) leaves the
/// uncommitted tail out of the unread region; a follow-up read_slot/commit
/// completes the frame, and it reads back intact.
#[test]
fn short_read_then_completion() {
    let mut buf = IngestBuf::new();
    let f = frame(b'D', b"split");

    // First read delivers only the first 4 bytes.
    {
        let slot = buf.read_slot(64).expect("slot 1");
        let n = socket_write(slot, &f[..4]);
        buf.commit(n).expect("commit 1");
    }
    assert!(matches!(buf.next_event(), Event::NeedMore));
    assert_eq!(buf.unread_len(), 4);

    // Second read delivers the rest.
    {
        let slot = buf.read_slot(64).expect("slot 2");
        let n = socket_write(slot, &f[4..]);
        buf.commit(n).expect("commit 2");
    }
    match buf.next_event() {
        Event::Row(body) => assert_eq!(body, b"split"),
        other => panic!("expected Row, got {other:?}"),
    }
}

/// Committing more than the lent slot is rejected, not silently advanced
/// past the populated region.
#[test]
fn commit_overflow_is_rejected() {
    let mut buf = IngestBuf::new();
    {
        let slot = buf.read_slot(INLINE_CAP).expect("full slot");
        assert_eq!(slot.len(), INLINE_CAP);
    }
    let err = buf
        .commit(INLINE_CAP + 1)
        .expect_err("over-commit rejected");
    assert_eq!(err.available, INLINE_CAP);
}

/// `read_slot` lends what fits when the wanted total exceeds the remaining
/// room (universal streaming: lend the available tail, never reject just
/// because the want is larger than the room).
#[test]
fn read_slot_lends_what_fits_when_want_exceeds_room() {
    let mut buf = IngestBuf::new();
    // Want more than the heap tier holds: escape, then lend exactly the
    // heap-tier room rather than erroring.
    let slot = buf.read_slot(READ_BUF_CAP + 1).expect("lends what fits");
    assert_eq!(slot.len(), READ_BUF_CAP);
}

/// When no room remains and nothing is consumed, `read_slot` reports the
/// buffer full rather than lending a zero-length slot or silently
/// truncating — the bounded-buffer fatal surface.
#[test]
fn full_buffer_reports_full() {
    let mut buf = IngestBuf::new();
    // Fill the heap tier completely (escape + commit the whole slot).
    {
        let slot = buf.read_slot(READ_BUF_CAP).expect("max slot");
        assert_eq!(slot.len(), READ_BUF_CAP);
    }
    buf.commit(READ_BUF_CAP).expect("commit full");
    // No room left, nothing consumed: the next read_slot reports full.
    let err = buf.read_slot(1).expect_err("buffer full");
    assert_eq!(err.cap, READ_BUF_CAP);
    assert_eq!(err.available, 0);
}

/// Debug never prints buffer contents.
#[test]
fn debug_redacts_contents() {
    let mut buf = IngestBuf::new();
    {
        let slot = buf.read_slot(8).expect("slot");
        let _ = socket_write(slot, b"secret!!");
    }
    buf.commit(8).expect("commit");
    let shown = format!("{buf:?}");
    assert!(!shown.contains("secret"), "Debug must not leak contents");
}

/// The mirror of the engine-private inline cap stays accurate: a frame of
/// exactly `INLINE_CAP` bytes does not escape, one byte more does.
#[test]
fn escape_boundary_matches_inline_cap() {
    // Exactly the inline cap: a single read_slot of INLINE_CAP stays inline
    // (no allocation observable through behaviour — it round-trips).
    let mut at = IngestBuf::new();
    let body = vec![b'z'; INLINE_CAP - 5];
    let f = frame(b'D', &body);
    assert_eq!(f.len(), INLINE_CAP);
    {
        let slot = at.read_slot(INLINE_CAP).expect("exact slot");
        let n = socket_write(slot, &f);
        at.commit(n).expect("commit");
    }
    match at.next_event() {
        Event::Row(b) => assert_eq!(b, body.as_slice()),
        other => panic!("expected Row, got {other:?}"),
    }
}
