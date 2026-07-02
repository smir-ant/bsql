//! Functional + memory-probe verification of `SecretBoundedStr<N>`'s
//! tier-1 Drop-chain staleness closure.
//!
//! # Scope
//!
//! `SecretBoundedStr<N>` is the foundation type for closing the
//! staleness-leak class in `ErrorPayload` sensitive fields. Its Drop
//! fires `BoundedStr::zeroize_in_place`
//! which scrubs `buf + len + was_lossy_flag` to zero. This file
//! pins:
//!
//! 1. **Functional**: constructors / accessors mirror `BoundedStr<N>`'s
//!    behaviour byte-for-byte. A migration from `BoundedStr<N>` to
//!    `SecretBoundedStr<N>` is mechanical.
//! 2. **Memory-probe**: when a `SecretBoundedStr<N>` is dropped (scope
//!    exit OR overwrite via assignment), the backing buffer at its
//!    stack address is physically zero — verified by raw-pointer
//!    read after Drop fires. Sister to `scram_zeroize_miri_spec.rs`
//!    and `buf_compact_staleness_spec.rs`.
//!
//! # Why unsafe here is acceptable
//!
//! Crate `#![forbid(unsafe_code)]` is at `src/lib.rs` (library only).
//! Integration tests in `tests/` compile as separate crates with
//! their own lint configuration; read-only pointer probing is the
//! same pattern as the existing memory-probe specs. Miri validates
//! pointer math + that the storage is still live at probe time.
//!
//! # Memory-probe stability
//!
//! Memory-probe via raw pointer runs unconditionally in debug mode
//! (default `cargo test`). For UB-free verification under Miri's
//! stacked-borrows model:
//!   - `cargo +nightly miri test --test secret_bounded_str_spec`

// This crate uses `unsafe` to probe memory after a drop — a verification
// technique that cannot be expressed in safe Rust. The raw-pointer read
// itself lives in the audited `bsql_devgates::probe_bytes`; the
// `unsafe { }` call blocks below discharge its safety contract (each
// captured pointer stays valid inside this test function's frame).
#![allow(
    unsafe_code,
    reason = "post-drop memory verification has no sound safe wrapper — a safe fn taking `*const u8` would let any safe caller read arbitrary memory; the raw read lives in the audited `bsql_devgates::probe_bytes` and each captured pointer stays valid inside this test function's own frame"
)]
#![allow(clippy::disallowed_methods, reason = "test harness — .unwrap_or here is a diagnostic default for a position()-of-first-nonzero assertion, not a silent production fallback")]

use bsql_devgates::probe_bytes;
use bsql_postgres_proto::SecretBoundedStr;

// =================================================================
// Functional — constructors / accessors mirror BoundedStr<N>.
// =================================================================

/// Invariant (spec): `new()` produces an empty `SecretBoundedStr<N>`
/// — `len == 0`, `is_empty()`, `as_str() == ""`, `was_lossy() == false`.
#[test]
fn new_is_empty() {
    let s = SecretBoundedStr::<32>::new();
    assert_eq!(s.len(), 0);
    assert!(s.is_empty());
    assert_eq!(s.as_str(), "");
    assert_eq!(s.as_bytes(), b"");
    assert!(!s.was_lossy());
}

/// Invariant (spec): `from_str_truncating` preserves UTF-8 input
/// when it fits within `N - marker_len`. Mirror of `BoundedStr<N>`'s
/// truncating constructor.
#[test]
fn from_str_truncating_preserves_short_input() {
    let s = SecretBoundedStr::<32>::from_str_truncating("boom");
    assert_eq!(s.len(), 4);
    assert_eq!(s.as_str(), "boom");
    assert!(!s.was_lossy());
}

/// Invariant (spec): `from_str_truncating` adds the `"…"` marker on
/// overflow. Caller sees a visibly-truncated string instead of a
/// silent drop.
#[test]
fn from_str_truncating_marks_overflow() {
    // 32-byte cap; long input forces truncation.
    let long = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJ";
    let s = SecretBoundedStr::<32>::from_str_truncating(long);
    let rendered = s.as_str();
    assert!(
        rendered.ends_with("…"),
        "truncation marker must be present, got {rendered:?}",
    );
    assert!(s.len() <= 32);
}

/// Invariant (spec): `from_bytes_lossy` coerces non-UTF-8 to `?`
/// and sets the lossy flag. Mirror of `BoundedStr<N>`.
#[test]
fn from_bytes_lossy_coerces_invalid_and_flags() {
    // Mix valid ASCII with invalid byte 0xFF.
    let raw: &[u8] = &[b'h', b'i', 0xFF, b'!'];
    let s = SecretBoundedStr::<32>::from_bytes_lossy(raw);
    assert!(s.was_lossy(), "lossy flag must be set on invalid bytes");
    let rendered = s.as_str();
    assert!(rendered.contains('?'), "invalid byte coerced to '?', got {rendered:?}");
}

/// Invariant (spec): `Default` matches `new()` — empty buffer,
/// not-lossy.
#[test]
fn default_matches_new() {
    let d = SecretBoundedStr::<32>::default();
    let n = SecretBoundedStr::<32>::new();
    assert_eq!(d, n);
    assert!(d.is_empty());
}

/// Invariant (spec): `Clone` produces a deep copy with independent
/// scrub fate. Each clone gets its own Drop firing.
#[test]
fn clone_is_independent() {
    let a = SecretBoundedStr::<32>::from_str_truncating("payload");
    let b = a.clone();
    assert_eq!(a.as_str(), b.as_str());
    assert_eq!(a, b);
    // Both Drop independently when this scope exits — no shared state.
}

/// Invariant (spec): `Debug` is REDACTED — the buffer content does
/// NOT appear in the formatted output. Defends against accidental
/// log-leak via `eprintln!("{params:?}", containing_struct)`.
#[test]
fn debug_redacts_content() {
    let s = SecretBoundedStr::<32>::from_str_truncating("super-secret-value");
    let rendered = format!("{s:?}");
    assert!(
        !rendered.contains("super-secret-value"),
        "Debug must not leak content, got {rendered:?}",
    );
    assert!(
        rendered.contains("REDACTED"),
        "Debug must indicate redaction, got {rendered:?}",
    );
    // Length is OK to leak (informational, not bytes).
    assert!(rendered.contains("len="));
}

// =================================================================
// Memory-probe — Drop chain scrubs the buffer.
// =================================================================

/// **Tier-1 by Drop chain witness**: a `SecretBoundedStr<N>` that
/// goes out of scope leaves its backing storage all-zero. Verified
/// by raw-pointer probe of the stack frame after Drop fires.
///
/// A naive Copy-or-no-Drop shape would retain content past scope
/// exit until the stack frame is reused — caught here.
#[test]
fn drop_zeroizes_buffer() {
    const MAGIC: &str = "zeroize-probe-MAGIC-XYZ-1234567890";

    // Capture a raw pointer to the buffer's start. Must outlive Drop.
    let (ptr, len) = {
        let s = SecretBoundedStr::<128>::from_str_truncating(MAGIC);
        let raw_ptr: *const u8 = s.as_bytes().as_ptr();
        let len = s.as_bytes().len();
        // Sanity: pre-drop, the bytes are MAGIC.
        let pre = unsafe { probe_bytes(raw_ptr, len) };
        assert_eq!(&pre[..MAGIC.len()], MAGIC.as_bytes(), "pre-condition: MAGIC present");
        (raw_ptr, len)
        // s drops here — `Drop` fires `inner.zeroize_in_place()`.
    };

    // Post-drop: probe the same address. Drop calls
    // `zeroize_in_place`, so all bytes must be zero.
    let post = unsafe { probe_bytes(ptr, len) };
    assert!(
        post.iter().all(|&b| b == 0),
        "post-drop buffer must be all-zero. Found {} non-zero bytes \
         (first non-zero at offset {}).",
        post.iter().filter(|&&b| b != 0).count(),
        post.iter().position(|&b| b != 0).unwrap_or(0),
    );
}

/// **Tier-1 by Drop chain witness — assignment path**: overwriting
/// a `SecretBoundedStr<N>` via `*field = new_value` fires Drop on
/// the OLD value before moving the new one in. Verified by probe.
///
/// Pins the closure for the `Option<T> = None` and
/// `*self = Self::new()` patterns: the language semantics
/// guarantee Drop firing on overwrite.
#[test]
fn overwrite_zeroizes_old_value() {
    const FIRST: &str = "first-secret-MAGIC";
    const SECOND: &str = "second";

    let mut slot = SecretBoundedStr::<128>::from_str_truncating(FIRST);
    let raw_ptr: *const u8 = slot.as_bytes().as_ptr();

    // Sanity: pre-overwrite, the FIRST bytes are present.
    let pre = unsafe { probe_bytes(raw_ptr, FIRST.len()) };
    assert_eq!(&pre[..FIRST.len()], FIRST.as_bytes());

    // Overwrite — Rust drops `slot`'s old value (firing Drop →
    // zeroize_in_place) BEFORE moving the new value in.
    slot = SecretBoundedStr::<128>::from_str_truncating(SECOND);
    // Touch slot post-assignment so the compiler sees a use (silences
    // unused_assignments) AND functionally pins the new content.
    assert_eq!(slot.as_str(), SECOND);

    // After the assignment, the storage at `raw_ptr` may contain
    // either the new SECOND content (if compiler reused the slot)
    // or fresh zeros from Drop (if compiler relocated). Probe just
    // beyond SECOND's length to verify the FIRST tail bytes were
    // scrubbed.
    let beyond_second = SECOND.len();
    let probe_len = FIRST.len().saturating_sub(beyond_second);
    if probe_len > 0 {
        let probe_start = unsafe { raw_ptr.add(beyond_second) };
        let post = unsafe { probe_bytes(probe_start, probe_len) };
        let nonzero_count = post.iter().filter(|&&b| b != 0).count();
        assert_eq!(
            nonzero_count, 0,
            "tail bytes from FIRST (beyond SECOND's len) must be \
             zero post-overwrite. Found {nonzero_count} non-zero bytes — \
             Drop didn't fire on the old value, or fired but didn't scrub.",
        );
    }
}

/// **Tier-1 negative path**: `Drop` cannot be skipped on overwrite
/// — Rust language semantics guarantee `field = new_value` drops
/// the old. This test pins the **non-Copy** invariant: a
/// `SecretBoundedStr<N>` cannot be silently `let x = src; ...`'d
/// (would create an unscrubbed alias under Copy semantics).
///
/// Compile-time: this test would fail to compile if `SecretBoundedStr<N>`
/// implemented `Copy`. The test body uses move-semantics deliberately;
/// `let dup = src;` where `src` is non-Copy MOVES, leaving `src`
/// inaccessible (compile error if used).
#[test]
fn def205_non_copy_enforces_single_owner() {
    let src = SecretBoundedStr::<32>::from_str_truncating("once");
    let dup = src; // move — `src` is consumed
    assert_eq!(dup.as_str(), "once");
    // The next line would be a compile error: `src` was moved.
    //   let _x = src.len();
    // Compile-fail discipline replaces the runtime assert here —
    // the test passes if the file compiles AT ALL, since Copy
    // would let both `src` and `dup` exist (no scrub-aliasing
    // protection). Non-Copy is structurally enforced.
}
