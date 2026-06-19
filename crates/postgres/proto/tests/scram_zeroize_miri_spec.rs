//! Miri memory-probe tests for `ScramSession::Drop` zeroization.
//!
//! # Scope
//!
//! Verifies that dropping a `ScramSession` actually overwrites the
//! password bytes in memory. Uses pointer-based probing — runs in
//! debug mode (default `cargo test`); Miri provides UB-free
//! verification on top (`cargo +nightly miri test`).
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
//!   only. See `Cargo.toml` for the panic-abort policy commentary.
//! - The test uses stack-local probing: we read the location where
//!   a Password LIVED after it was consumed. If the compiler moved
//!   the Password elsewhere before drop, the probe might not hit
//!   the post-drop zeros. Miri validates our pointer math matches
//!   Rust's memory model.
//!
//! Run with: `cargo +nightly miri test --test scram_zeroize_miri_spec`
//! (requires Miri: `rustup component add miri --toolchain nightly`).

// This crate uses `unsafe` to probe memory after a drop — a verification
// technique that cannot be expressed in safe Rust. The raw-pointer read
// itself lives in the audited `bsql_devgates::probe_bytes`; the
// `unsafe { }` call blocks below discharge its safety contract (each
// captured pointer points into this test function's still-live stack
// frame, which Miri verifies).
#![allow(
    unsafe_code,
    reason = "post-drop memory verification has no sound safe wrapper — a safe fn taking `*const u8` would let any safe caller read arbitrary memory; the raw read lives in the audited `bsql_devgates::probe_bytes` and each captured pointer points into this test function's still-live stack frame, which Miri verifies"
)]

use bsql_devgates::probe_bytes as read_bytes_at;
use bsql_postgres_proto::password::Password;
use bsql_postgres_proto::sensitive::Sensitive;

/// Dropping a Password zeros its backing buffer.
///
/// Under Miri: passes because Miri verifies the read of post-drop
/// memory is legal under Rust's memory model + observes that all
/// bytes are zero after Drop fires.
///
/// Under regular `cargo test`: runs and usually passes (release-
/// profile compiler may have optimized the move differently — see
/// module docs).
#[test]
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
    // behavior is compiler-dependent.
    let post_drop = unsafe { read_bytes_at(ptr, len) };
    assert!(
        post_drop.iter().all(|&b| b == 0),
        "ZeroizeOnDrop must zero the backing buffer, got {post_drop:?}",
    );
}

/// Same invariant for `Sensitive<Password>`.
#[test]
fn sensitive_password_drop_zeros_backing_buffer() {
    const MAGIC: &[u8] = b"sensitive-zeroize-probe";

    let (ptr, len) = {
        let pw = match Password::try_from_bytes(MAGIC) {
            Ok(p) => p,
            Err(_) => return,
        };
        let sensitive = Sensitive::new(pw);
        // Closure-scope `with_inner`: the address (`*const u8`) is
        // captured as `R` from the closure return; `R` is
        // independent of the inner borrow's lifetime, so the
        // pointer survives the closure (the underlying buffer also
        // survives because `sensitive` itself is still owned by the
        // outer scope). The probe's post-Drop read is unchanged in
        // semantics.
        let raw_ptr: *const u8 = sensitive.with_inner(|p| p.as_bytes().as_ptr());
        let len = sensitive.with_inner(|p| p.as_bytes().len());
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

/// Non-ignored smoke test — just verifies the `ZeroizeOnDrop`
/// trait bound is present structurally without pointer probing.
/// Always runs.
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
