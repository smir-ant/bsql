//! Direct unit tests for the bounded-buffer newtypes `ReadBuf` and
//! `WriteBuf`, plus `CappedServerNonce`.
//!
//! These buffers are the load-bearing structural defence against the
//! "buffer overflow / silent truncation" bug class (reforge.md §7.4).
//! Their *structural* invariants (bounded capacity, sealed API surface)
//! are tier-1/tier-2 by construction, but the **observable API
//! contract** — "append returns ReadBufFull on overflow with the exact
//! requested/available sizes", "advance returns AdvancePastEnd", "lazy
//! compact works" — is category (1) spec-conformance per reforge.md
//! §4.11. A one-line drift inside these methods (swap Ok/Err, return
//! 0 instead of available, etc.) would compile but silently break the
//! contract that the wire-layer relies on.
//!
//! The integration tests (`ping_spec.rs`, `startup_spec.rs`) exercise
//! these buffers indirectly through `feed_bytes` and SendBytes actions,
//! but the direct unit tests here pin the API contract without having
//! to construct a full protocol state machine. That matters for
//! classification-correctness of the Err variants — indirect tests
//! would only fail on protocol-level symptoms, not on the buffer's own
//! contract.

#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::mem_forget,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::integer_division
)]
#![deny(unused_must_use, unused_lifetimes)]

use bsql_pg_proto::{
    AdvancePastEnd, READ_BUF_CAP, ReadBuf, ReadBufFull, WriteBuf, WriteBufFull,
    scram::types::{CappedServerNonce, ServerNonceTooLong},
};

// =================================================================
// ReadBuf — category (1) spec-conformance on the bounded-buffer API.
// =================================================================

/// Invariant (spec): a newly-constructed `ReadBuf` is empty and its
/// unread region is the empty slice.
///
/// Pins `ReadBuf::new()`'s initial state. A regression that set
/// `cursor = 1` (instead of 0) would compile; the test catches.
#[test]
fn new_read_buf_is_empty() {
    let buf = ReadBuf::new();
    assert_eq!(buf.unread().len(), 0);
    assert_eq!(buf.unread_len(), 0);
    assert!(buf.unread().is_empty());
}

/// Invariant (spec): `append` adds bytes to the unread region and
/// `unread()` returns them in FIFO order.
#[test]
fn append_and_unread_round_trip() {
    let mut buf = ReadBuf::new();
    let result = buf.append(b"hello");
    assert!(result.is_ok(), "append within capacity must succeed");
    assert_eq!(buf.unread(), b"hello");
    assert_eq!(buf.unread_len(), 5);
}

/// Invariant (spec): `advance(n)` consumes `n` bytes from the head of
/// the unread region; subsequent `unread()` returns the tail.
#[test]
fn advance_consumes_head() {
    let mut buf = ReadBuf::new();
    let append_result = buf.append(b"hello world");
    assert!(append_result.is_ok());

    let advance_result = buf.advance(6);
    assert!(advance_result.is_ok(), "advance within unread must succeed");
    assert_eq!(buf.unread(), b"world");
    assert_eq!(buf.unread_len(), 5);
}

/// Invariant (spec): `advance(0)` is a no-op — cursor doesn't move.
///
/// Pins that `n = 0` falls through the `n > available` check (since
/// `0 <= available` always) and adds 0 via `checked_add`.
#[test]
fn advance_zero_is_noop() {
    let mut buf = ReadBuf::new();
    let append_result = buf.append(b"abc");
    assert!(append_result.is_ok());
    let advance_result = buf.advance(0);
    assert!(advance_result.is_ok());
    assert_eq!(buf.unread(), b"abc");
}

/// Invariant (spec): `advance(n)` where `n > unread_len` returns
/// `AdvancePastEnd { requested, available }` carrying the caller's
/// `n` and the actual unread length. Buffer state is NOT modified.
///
/// Pins the classification boundary and the field-value round-trip.
/// A regression that clamped `requested` to `available` inside the
/// error would hide the caller's bug from diagnostics.
#[test]
fn advance_past_end_is_classified() {
    let mut buf = ReadBuf::new();
    let append_result = buf.append(b"ab");
    assert!(append_result.is_ok());
    let advance_result = buf.advance(5);
    match advance_result {
        Err(AdvancePastEnd {
            requested,
            available,
        }) => {
            assert_eq!(requested, 5);
            assert_eq!(available, 2);
        }
        Ok(()) => panic!("advance(5) on 2-byte buffer must fail"),
    }
    // Buffer state preserved on error.
    assert_eq!(buf.unread(), b"ab");
}

/// Invariant (spec): `append` returning `ReadBufFull` carries the
/// exact attempted length and the actual headroom. Buffer state after
/// a rejected append matches what was in it before (fail-atomic).
///
/// Pins the classification AND the fail-atomic property —
/// `heapless::Vec::extend_from_slice` is all-or-nothing on overflow,
/// but the buffer wrapper has an auto-compact step that must not
/// partially succeed and leak state.
#[test]
fn append_overflow_is_classified_and_fail_atomic() {
    let mut buf = ReadBuf::new();
    // Fill to capacity.
    let chunk = vec![0xABu8; READ_BUF_CAP];
    let append_ok = buf.append(&chunk);
    assert!(append_ok.is_ok(), "filling to cap must succeed");
    assert_eq!(buf.unread_len(), READ_BUF_CAP);

    // Attempt to append one more byte — must fail with classified error.
    let extra = [0xCDu8];
    let result = buf.append(&extra);
    match result {
        Err(ReadBufFull {
            attempted,
            available,
        }) => {
            assert_eq!(attempted, 1);
            assert_eq!(available, 0, "buffer at cap has zero headroom");
        }
        Ok(()) => panic!("append beyond cap must fail"),
    }
    // Fail-atomic: the unread region is unchanged.
    assert_eq!(buf.unread_len(), READ_BUF_CAP);
    assert_eq!(
        buf.unread().last(),
        Some(&0xABu8),
        "pre-overflow tail byte preserved",
    );
}

/// Invariant (spec): `clear` resets the buffer to empty, including
/// the cursor.
///
/// Pins the one-line body `self.inner.clear(); self.cursor = 0;`.
/// A regression that forgot to reset the cursor would leave the
/// buffer in an inconsistent state (empty inner but non-zero cursor
/// → next append's compact would touch OOB).
#[test]
fn clear_resets_both_cursor_and_contents() {
    let mut buf = ReadBuf::new();
    let append_result = buf.append(b"hello");
    assert!(append_result.is_ok());
    let advance_result = buf.advance(3);
    assert!(advance_result.is_ok());
    // cursor = 3, inner.len() = 5, unread = "lo"

    buf.clear();
    assert_eq!(buf.unread_len(), 0);
    assert!(buf.unread().is_empty());
    // Immediately append and verify no stale cursor.
    let append_after_clear = buf.append(b"new");
    assert!(append_after_clear.is_ok());
    assert_eq!(buf.unread(), b"new");
}

/// Invariant (spec, DEF-058 lazy compact): when the tail has room for
/// an append, no memmove is needed; when it doesn't, the compact step
/// reclaims the consumed prefix and the retry succeeds.
///
/// This exercises both paths through the lazy-compact logic. The
/// observable difference between the two paths is a timing one (not
/// directly observable), but the *outcome* — "append succeeds whenever
/// the unread+new fits capacity" — is pinned here.
#[test]
fn lazy_compact_reclaims_consumed_prefix_when_tail_insufficient() {
    // Build a scenario where inner.len() == CAP but unread_len() < CAP
    // (a large consumed prefix). A new append must trigger compact.
    let mut buf = ReadBuf::new();
    let initial = vec![0x11u8; READ_BUF_CAP];
    let ok = buf.append(&initial);
    assert!(ok.is_ok());
    // Consume almost all of it.
    let partial_len = READ_BUF_CAP.saturating_sub(100);
    let adv = buf.advance(partial_len);
    assert!(adv.is_ok());
    // Now: inner.len() = CAP (full), cursor = CAP-100, unread_len = 100.
    assert_eq!(buf.unread_len(), 100);

    // Append 50 more bytes — tail has 0 bytes free (inner is full),
    // but after compact we'd have CAP-100 bytes free in tail.
    let additional = vec![0x22u8; 50];
    let ok = buf.append(&additional);
    assert!(
        ok.is_ok(),
        "append must succeed via lazy-compact when unread+new <= CAP",
    );
    assert_eq!(buf.unread_len(), 150);
    // Verify the content: original tail (100 × 0x11) then new (50 × 0x22).
    let expected_first = buf.unread().first();
    let expected_at_100 = buf.unread().get(100);
    assert_eq!(expected_first, Some(&0x11));
    assert_eq!(expected_at_100, Some(&0x22));
}

// =================================================================
// WriteBuf — category (1) spec-conformance.
// =================================================================

/// Invariant (spec): `push_u8` appends exactly one byte.
#[test]
fn write_buf_push_u8_appends_one_byte() {
    let mut wb = WriteBuf::new();
    let ok = wb.push_u8(0xAB);
    assert!(ok.is_ok());
    assert_eq!(wb.len(), 1);
    assert_eq!(wb.as_bytes(), &[0xAB]);
}

/// Invariant (spec): `push_u32_be` appends four bytes in big-endian
/// order.
///
/// Pins the `.to_be_bytes()` call — a regression to `.to_le_bytes()`
/// would corrupt the wire format (catastrophic for PG). Indirectly
/// tested by `startup_message_wire_format` which pins protocol-
/// version bytes; this direct test catches the same drift without
/// a full StartupMessage.
#[test]
fn write_buf_push_u32_be_is_big_endian() {
    let mut wb = WriteBuf::new();
    let ok = wb.push_u32_be(0x0A0B0C0D);
    assert!(ok.is_ok());
    assert_eq!(wb.as_bytes(), &[0x0A, 0x0B, 0x0C, 0x0D]);
}

/// Invariant (spec): `push_i32_be` — same BE order as u32, sign bit
/// honoured.
#[test]
fn write_buf_push_i32_be_is_big_endian() {
    let mut wb = WriteBuf::new();
    let ok = wb.push_i32_be(-1_i32);
    assert!(ok.is_ok());
    assert_eq!(wb.as_bytes(), &[0xFF, 0xFF, 0xFF, 0xFF]);
}

/// Invariant (spec): `push_nul_terminated` writes bytes + a single
/// NUL. Verifies NUL presence by byte match.
#[test]
fn write_buf_push_nul_terminated_appends_nul() {
    let mut wb = WriteBuf::new();
    let ok = wb.push_nul_terminated(b"user");
    assert!(ok.is_ok());
    assert_eq!(wb.as_bytes(), b"user\0");
}

/// Invariant (spec): `with_length_prefix` writes a 4-byte placeholder,
/// runs the body, then patches the placeholder with the final length
/// (including the 4-byte prefix itself — PG's self-inclusive length).
#[test]
fn write_buf_with_length_prefix_patches_final_length() {
    let mut wb = WriteBuf::new();
    let ok = wb.with_length_prefix(|w| {
        w.push_u8(0xDE)?;
        w.push_u8(0xAD)?;
        Ok(())
    });
    assert!(ok.is_ok());
    // Expected: [0,0,0,6, 0xDE, 0xAD] — total 6 bytes = 4 (prefix) + 2 (body).
    assert_eq!(wb.as_bytes(), &[0, 0, 0, 6, 0xDE, 0xAD]);
}

/// Invariant (spec): writes that would overflow the bounded capacity
/// return `WriteBufFull` and DO NOT partially mutate.
#[test]
fn write_buf_overflow_returns_full() {
    let mut wb = WriteBuf::new();
    // Fill the buffer nearly to capacity.
    let big = vec![0u8; bsql_pg_proto::MAX_OWNED_SEND_LEN];
    let ok = wb.push_bytes(&big);
    assert!(ok.is_ok());

    // One more byte overflows.
    let overflow_result = wb.push_u8(0);
    assert_eq!(overflow_result, Err(WriteBufFull));
    // Contents unchanged.
    assert_eq!(wb.len(), bsql_pg_proto::MAX_OWNED_SEND_LEN);
}

// =================================================================
// CappedServerNonce — category (1) bound-rejection.
// DEF-040 regression guard.
// =================================================================

/// Invariant (spec): `CappedServerNonce::try_from_bytes` with a slice
/// at the capacity bound succeeds; one byte beyond the bound returns
/// `ServerNonceTooLong` with the actual length.
#[test]
fn capped_server_nonce_bound_classification() {
    use bsql_pg_proto::scram::types::MAX_SERVER_NONCE_LEN;

    // At bound: OK.
    let at_bound = vec![0x5Au8; MAX_SERVER_NONCE_LEN];
    let ok = CappedServerNonce::try_from_bytes(&at_bound);
    assert!(ok.is_ok(), "nonce at MAX_SERVER_NONCE_LEN must succeed");

    // One over: error with exact length.
    let over_bound_len = MAX_SERVER_NONCE_LEN.saturating_add(1);
    let over_bound = vec![0x5Au8; over_bound_len];
    let err = CappedServerNonce::try_from_bytes(&over_bound);
    match err {
        Err(ServerNonceTooLong { len }) => {
            assert_eq!(len, over_bound_len);
        }
        Ok(_) => panic!("nonce over MAX_SERVER_NONCE_LEN must fail"),
    }
}
