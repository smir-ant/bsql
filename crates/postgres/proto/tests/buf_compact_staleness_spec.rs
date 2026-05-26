//! Memory-probe verification that `ReadBuf::compact()` zeroizes
//! the abandoned tail in place before truncate. Without the
//! explicit zeroize, bytes physically at positions
//! `[unread_len..pre_compact_len)` would retain pre-compact content
//! even after `truncate(unread_len)` shrinks `inner.len()` to the
//! new compacted length — `heapless::Vec::truncate` only adjusts
//! the length counter for `Copy` types, not the storage.
//!
//! # Scope
//!
//! Verifies the staleness closure for the `compact()` path
//! specifically. Sister test pattern to `scram_zeroize_miri_spec` —
//! read-only pointer probing of memory after the library has
//! logically discarded it.
//!
//! # Why unsafe here is acceptable
//!
//! The crate's `#![forbid(unsafe_code)]` is at `src/lib.rs` and
//! applies to library source only. Integration tests in `tests/`
//! compile as separate crates and may use `unsafe` for black-box
//! verification of library invariants. Same pattern as
//! `tests/scram_zeroize_miri_spec.rs`.
//!
//! The unsafe used here is **read-only**: we never mutate post-
//! compact memory, only observe it to verify the zeroize claim.
//! Miri validates pointer math + that the storage is still live at
//! probe time.
//!
//! # Memory-probe stability
//!
//! Memory-probe via raw pointer runs unconditionally in debug mode
//! (default `cargo test`). Debug builds preserve the writes that
//! the probes observe; release-mode aggressive optimisation can
//! perturb raw-pointer observations but the test harness doesn't
//! build with `--release`. For UB-free verification beyond debug
//! probes:
//!   - `cargo +nightly miri test --test buf_compact_staleness_spec`
//!     — gold-standard verifier (stacked-borrows model).

#![allow(unsafe_code)]

use bsql_postgres_proto::frame::READ_BUF_CAP;
use bsql_postgres_proto::ReadBuf;

/// Read `len` bytes at `ptr` into a Vec. Read-only.
///
/// # Safety
///
/// Caller must ensure:
/// - `ptr` and `ptr + len` are within a single live allocation.
/// - The bytes were previously written (no MaybeUninit reads).
unsafe fn probe_bytes(ptr: *const u8, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        // SAFETY: caller invariants above.
        let byte = unsafe { ptr.add(i).read() };
        out.push(byte);
    }
    out
}

/// Post-compact, the abandoned tail
/// `[unread_len..pre_compact_len)` is physically zeroized.
///
/// A naive shape without the explicit zeroize would leave the tail
/// holding pre-compact content (specifically: the consumed
/// prefix's content + the source side of the `copy_within`, which
/// `copy_within` does NOT clear) — caught by this test.
///
/// **Post-fix behaviour** (verified here): bytes at
/// `[unread_len..pre_compact_len)` are all-zero.
///
/// **Concrete leak vector** the fix closes: a 2 KB SCRAM frame
/// containing server salt + nonce reaches `ReadBuf`; the dispatcher
/// consumes it (cursor advances past it); a small `ReadyForQuery`
/// frame arrives; `append()` triggers `compact()`; the abandoned
/// 2 KB range previously retained password-correlated bytes — now
/// it is scrubbed.
#[test]
fn def204_compact_zeroizes_abandoned_tail() {
    // Magic pattern: a non-zero byte we can later distinguish from
    // the post-fix zeros.
    const MAGIC: u8 = 0xAB;

    let mut buf = ReadBuf::new();

    // Fill the buffer to capacity with the magic pattern. This puts
    // pattern bytes at every physical position [0..READ_BUF_CAP).
    let initial = vec![MAGIC; READ_BUF_CAP];
    let res = buf.append(&initial);
    assert!(res.is_ok(), "initial fill must succeed");

    // Capture a raw pointer to the buffer's backing storage. The
    // `unread()` slice at this point covers all CAP bytes (cursor=0).
    // Same allocation, same lifetime as `buf`.
    let base_ptr: *const u8 = buf.unread().as_ptr();

    // Sanity: pre-advance, all CAP bytes are MAGIC.
    let pre = unsafe { probe_bytes(base_ptr, READ_BUF_CAP) };
    assert!(
        pre.iter().all(|&b| b == MAGIC),
        "pre-condition: all CAP bytes are MAGIC",
    );

    // Advance cursor to leave a small unread tail. This sets up the
    // compact() trigger: tail has 0 bytes free, but compact would
    // free up cursor bytes.
    const UNREAD: usize = 100;
    let advance_by = READ_BUF_CAP.saturating_sub(UNREAD);
    let adv = buf.advance(advance_by);
    assert!(adv.is_ok(), "advance must succeed");
    assert_eq!(buf.unread_len(), UNREAD);

    // Trigger compact via an append that doesn't fit in the empty
    // tail. Post-compact: inner.len() = UNREAD + 1, cursor = 0.
    let trigger = [0xCDu8; 1];
    let res = buf.append(&trigger);
    assert!(res.is_ok(), "append-after-compact must succeed");

    // Post-compact: probe the abandoned tail. Bytes at positions
    // [UNREAD + 1..READ_BUF_CAP) MUST be zero. Without the explicit
    // zeroize, those positions would retain MAGIC (the original
    // pattern at positions [advance_by..READ_BUF_CAP) before the
    // `copy_within` shifted the unread tail to the start).
    //
    // The first `UNREAD` bytes are the relocated unread tail (still
    // MAGIC). The next byte is the post-compact append (`0xCD`).
    // Beyond that, [UNREAD + 1..CAP) is the abandoned region.
    let abandoned_start = UNREAD.saturating_add(1);
    let abandoned_len = READ_BUF_CAP.saturating_sub(abandoned_start);
    let probed = unsafe { probe_bytes(base_ptr.wrapping_add(abandoned_start), abandoned_len) };

    let nonzero_count = probed.iter().filter(|&&b| b != 0).count();
    assert_eq!(
        nonzero_count, 0,
        "abandoned tail [UNREAD+1..CAP) = [{}..{}) must be zero \
         post-compact. Found {} non-zero bytes (first non-zero at offset {}). \
         If this fails, ReadBuf::compact() is leaking pre-compact content \
         beyond the truncated len — secret-correlated bytes from prior frames \
         physically persist in the array.",
        abandoned_start,
        READ_BUF_CAP,
        nonzero_count,
        probed.iter().position(|&b| b != 0).unwrap_or(0),
    );

    // Sanity: the relocated unread tail and the trigger byte are
    // intact (probe via public API is sufficient — compact must not
    // corrupt them).
    let unread = buf.unread();
    assert_eq!(unread.len(), UNREAD + 1);
    let unread_first = unread.first();
    let unread_at_unread = unread.get(UNREAD);
    assert_eq!(
        unread_first,
        Some(&MAGIC),
        "first relocated unread byte must remain MAGIC",
    );
    assert_eq!(
        unread_at_unread,
        Some(&0xCD),
        "trigger append byte must remain at unread tail",
    );
}
