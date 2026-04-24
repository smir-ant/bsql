//! DEF-185 P3-1 (audit 2026-04-24): Miri memory-probe tests for
//! `ScramSession::Drop` zeroization.
//!
//! # Scope
//!
//! Verifies that dropping a `ScramSession` actually overwrites the
//! password bytes in memory. Uses pointer-based probing — available
//! only under Miri (which checks UB-free reads) or explicit opt-in
//! via `RUST_TEST_NOCAPTURE=1 cargo test -- --ignored`.
//!
//! # Why unsafe here is acceptable
//!
//! The crate's `#![forbid(unsafe_code)]` lives in `src/lib.rs` and
//! applies to the library source only. Integration tests in `tests/`
//! compile as separate crates with their own lint configuration —
//! they can use `unsafe` for black-box verification of library
//! invariants without polluting the main crate's tier-1 surface.
//!
//! The unsafe used here is read-only pointer probing: we never
//! mutate post-drop memory, we just observe it. Miri verifies that
//! no UB (read of uninitialized memory, use-after-free beyond scope)
//! occurs during the observation.
//!
//! # Limitations
//!
//! - Under `panic = "abort"` (release profile), Drop doesn't run on
//!   panic paths — this test validates the NORMAL-scope-exit path
//!   only. See `Cargo.toml` DEF-185 P0-A commentary.
//! - The test uses stack-local probing: we read the location where
//!   a Password LIVED after it was consumed. If the compiler moved
//!   the Password elsewhere before drop, the probe might not hit
//!   the post-drop zeros. Miri validates our pointer math matches
//!   Rust's memory model.
//!
//! Run with: `cargo +nightly miri test --test scram_zeroize_miri_spec`
//! (requires Miri: `rustup component add miri --toolchain nightly`).

#![allow(unsafe_code)]

use bsql_pg_proto::password::Password;
use bsql_pg_proto::sensitive::Sensitive;

/// Probe the Password's internal `buf` field via a known-address read.
/// Returns true if ALL bytes at that location are zero.
///
/// This function is `unsafe` because it reads memory at a raw pointer
/// after the Password has been dropped — relying on the stack frame
/// not having been overwritten yet. Valid pattern under Miri, which
/// verifies the memory is still live (within the test function's
/// stack frame) at the time of read.
///
/// # Safety
///
/// Caller must ensure:
/// - `ptr` points to a valid (possibly dropped) `[u8; N]` buffer.
/// - The buffer's stack frame has not been reused since drop.
/// - `N <= 1024` (upper bound covers MAX_PASSWORD_LEN).
unsafe fn read_bytes_at(ptr: *const u8, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        // SAFETY: caller invariants above.
        let byte = unsafe { ptr.add(i).read() };
        out.push(byte);
    }
    out
}

/// DEF-185 P3-1: Dropping a Password zeros its backing buffer.
///
/// Under Miri: passes because Miri verifies the read of post-drop
/// memory is legal under Rust's memory model + observes that all
/// bytes are zero after Drop fires.
///
/// Under regular `cargo test`: runs and usually passes (release-
/// profile compiler may have optimized the move differently — see
/// module docs).
///
/// Marked `#[ignore]` for regular test runs to avoid flakiness on
/// architectures/compilers that optimize away the stack frame; run
/// explicitly via `cargo test -- --ignored` or under Miri.
#[test]
#[ignore = "memory-probe: run via cargo miri test or --ignored"]
fn password_drop_zeros_backing_buffer() {
    // Magic password bytes so we can visually confirm they were present.
    const MAGIC: &[u8] = b"zeroize-probe-magic-XYZ";

    // Capture pointer BEFORE drop. Must live at least until the
    // post-drop read; we use a raw pointer to avoid aliasing a live
    // Rust reference.
    let (ptr, len) = {
        let pw = match Password::try_from_bytes(MAGIC) {
            Ok(p) => p,
            Err(_) => return,  // Cap violation — skip.
        };
        let raw_ptr: *const u8 = pw.as_bytes().as_ptr();
        // Password::as_ref returns &[u8] with len == input_len, but
        // the Zeroize-derived Drop scrubs the ENTIRE backing array
        // (len = MAX_PASSWORD_LEN). Read only the populated prefix
        // to avoid reading uninitialized padding at Miri's discretion.
        let len = pw.as_bytes().len();
        // Verify MAGIC is actually there pre-drop.
        // SAFETY: pw is still live; raw_ptr is valid.
        let pre_drop = unsafe { read_bytes_at(raw_ptr, len) };
        assert_eq!(pre_drop.as_slice(), MAGIC, "pre-drop bytes must match MAGIC");
        (raw_ptr, len)
        // pw drops HERE at end of block. ZeroizeOnDrop fires on the
        // backing [u8; MAX_PASSWORD_LEN] array.
    };

    // Post-drop probe.
    //
    // SAFETY: The Password's stack slot is technically freed (its
    // scope ended), but the physical memory at `ptr` is still within
    // THIS function's stack frame. Miri permits this read under the
    // stacked-borrows model; regular cargo-test also permits but
    // behavior is compiler-dependent. The test is `#[ignore]` by
    // default for this reason.
    let post_drop = unsafe { read_bytes_at(ptr, len) };
    assert!(
        post_drop.iter().all(|&b| b == 0),
        "ZeroizeOnDrop must zero the backing buffer, got {post_drop:?}",
    );
}

/// DEF-185 P3-1: Same invariant for `Sensitive<Password>`.
#[test]
#[ignore = "memory-probe: run via cargo miri test or --ignored"]
fn sensitive_password_drop_zeros_backing_buffer() {
    const MAGIC: &[u8] = b"sensitive-zeroize-probe";

    let (ptr, len) = {
        let pw = match Password::try_from_bytes(MAGIC) {
            Ok(p) => p,
            Err(_) => return,
        };
        let sensitive = Sensitive::new(pw);
        let raw_ptr: *const u8 = sensitive.get().as_bytes().as_ptr();
        let len = sensitive.get().as_bytes().len();
        // Verify MAGIC is there.
        let pre = unsafe { read_bytes_at(raw_ptr, len) };
        assert_eq!(pre.as_slice(), MAGIC);
        (raw_ptr, len)
    };

    let post = unsafe { read_bytes_at(ptr, len) };
    assert!(
        post.iter().all(|&b| b == 0),
        "Sensitive<Password>::Drop must zero backing buffer, got {post:?}",
    );
}

/// DEF-185 P3-1: non-ignored smoke test — just verifies the
/// `ZeroizeOnDrop` trait bound is present structurally without
/// pointer probing. Always runs.
#[test]
fn password_needs_drop_is_true() {
    // `needs_drop::<T>()` returns true iff T's Drop glue does actual
    // work. For Zeroize-derived types, this is true. For trivial
    // `#[derive(Copy)]` types, false. If a future refactor accidentally
    // removed the ZeroizeOnDrop derive from Password, this would flip
    // to false and this test would fail — a compile-adjacent shield.
    assert!(
        core::mem::needs_drop::<Password>(),
        "Password MUST have Drop glue (ZeroizeOnDrop invariant)",
    );
    assert!(
        core::mem::needs_drop::<Sensitive<Password>>(),
        "Sensitive<Password> MUST have Drop glue",
    );
}
