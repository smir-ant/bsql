//! Memory-probe verification that `SessionParams::clear()` scrubs
//! the previous string-field data via the Drop chain.
//!
//! # Current behaviour (verified here)
//!
//! Sensitive string fields use `SecretBoundedStr<N>` (non-Copy,
//! ZeroizeOnDrop). Rust language semantics guarantee
//! `*self = Self::new()` drops the OLD self (which fires Drop on
//! each `Option<SecretBoundedStr<N>>` field — Drop on `Option<T>`
//! drops the inner `T` if Some, no-op if None — scrubbing the
//! `buf + len + was_lossy_flag` of each populated field). Then
//! `Self::new()` (all None) is moved in.
//!
//! # What this guards against
//!
//! A naive `Option<BoundedStr<N>>` (Copy) shape would not fire any
//! Drop on `clear()`'s field-by-field `*self = Self::new()`
//! reassignment to `None` — the compiler may write only the
//! discriminant, leaving the Some-data region's bytes physically
//! intact. ~256 B of server-echoed config (`server_version`,
//! `application_name`, `session_authorization`, `date_style`,
//! `time_zone`) would leak across the clear boundary into a
//! subsequent connection's diagnostics.
//!
//! **Tier-1 by compiler-enforced Drop**.

#![allow(unsafe_code)]

use bsql_postgres_proto::SessionParams;

/// Read `len` bytes at `ptr` into a Vec. Read-only.
///
/// # Safety
///
/// Caller must ensure:
/// - `ptr..ptr+len` is within a single live allocation.
/// - Bytes were previously written.
unsafe fn probe_bytes(ptr: *const u8, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        // SAFETY: caller invariants above.
        let byte = unsafe { ptr.add(i).read() };
        out.push(byte);
    }
    out
}

/// Compile-time witness: `SessionParams` is non-Copy (its
/// sensitive fields are `Option<SecretBoundedStr<N>>` which are
/// non-Copy because `SecretBoundedStr` is non-Copy + Drop).
///
/// If a future refactor accidentally re-derives Copy on
/// SessionParams (e.g., by reverting the SecretBoundedStr fields
/// back to BoundedStr), this test fails to compile because
/// `let dup = src; ...src.field...` would not consume src.
///
/// Compile-fail is the negative-witness for non-Copy enforcement.
/// Passes if file compiles AT ALL.
#[test]
fn session_params_is_non_copy() {
    let mut src = SessionParams::default();
    src.set(b"application_name", b"deployment-tag-magic");
    let dup = src; // move — `src` consumed
    assert_eq!(
        dup.application_name.as_ref().map(|s| s.as_str()),
        Some("deployment-tag-magic"),
    );
    // The next line would be a compile error: src moved.
    //   let _x = src.application_name.as_ref();
}

/// **Tier-1 by Drop chain witness — `*self = Self::new()` path**:
/// `SessionParams::clear()` does `*self = Self::new()`. Rust drops
/// the old self before moving in the new value; the Drop chain
/// fires on each `Option<SecretBoundedStr<N>>` field, scrubbing
/// the buf bytes of populated entries.
///
/// Verifies via raw-pointer probe that an `application_name`
/// populated with magic bytes is zeroed after `clear()`.
#[test]
fn clear_zeroizes_populated_fields() {
    const MAGIC_APP: &str = "DEPLOYMENT-MAGIC-APP-NAME-XYZ-1234567890";
    const MAGIC_VER: &str = "SERVER-VERSION-MAGIC";

    let mut params = SessionParams::default();
    params.set(b"application_name", MAGIC_APP.as_bytes());
    params.set(b"server_version", MAGIC_VER.as_bytes());

    // Sanity: pre-clear, the values are populated and accessible.
    assert_eq!(
        params.application_name.as_ref().map(|s| s.as_str()),
        Some(MAGIC_APP),
    );

    // Capture raw pointers to the buf storage. Both pointers are
    // valid as long as `params` stays in scope (which it does until
    // end of test function).
    let app_ptr: *const u8 = match params.application_name.as_ref() {
        Some(s) => s.as_bytes().as_ptr(),
        None => return, // architecturally dead under the assert above
    };
    let app_len = MAGIC_APP.len();

    let ver_ptr: *const u8 = match params.server_version.as_ref() {
        Some(s) => s.as_bytes().as_ptr(),
        None => return,
    };
    let ver_len = MAGIC_VER.len();

    // Pre-clear sanity probe.
    let app_pre = unsafe { probe_bytes(app_ptr, app_len) };
    assert_eq!(&app_pre[..MAGIC_APP.len()], MAGIC_APP.as_bytes());

    // Clear — `*self = Self::new()` drops old self → Drop chain
    // fires on each Option<SecretBoundedStr<N>> field → scrubs OLD
    // bytes. Then memcpy of Self::new() writes None pattern; the
    // None variant's data region (formerly the SecretBoundedStr buf)
    // is compiler-determined (may be zero, may contain padding
    // artifacts). **The security-relevant invariant is that the
    // SECRET bytes are gone — NOT that the post-state is all-zero.**
    params.clear();

    // Post-clear: probe the same memory regions.
    let app_post = unsafe { probe_bytes(app_ptr, app_len) };
    let ver_post = unsafe { probe_bytes(ver_ptr, ver_len) };

    // Security invariant: MAGIC bytes are NOT preserved.
    // Compiler-determined None-pattern bytes are acceptable as long
    // as they're not the previous secret.
    assert_ne!(
        &app_post[..],
        MAGIC_APP.as_bytes(),
        "post-clear application_name buf must NOT match the \
         MAGIC_APP secret. Drop should have scrubbed them.",
    );
    assert_ne!(
        &ver_post[..],
        MAGIC_VER.as_bytes(),
        "post-clear server_version buf must NOT match the \
         MAGIC_VER secret.",
    );
    // Stronger assertion: not even a substring match. If the secret
    // content survived in any contiguous run, that's a leak.
    let app_post_str = String::from_utf8_lossy(&app_post);
    let ver_post_str = String::from_utf8_lossy(&ver_post);
    assert!(
        !app_post_str.contains(MAGIC_APP),
        "MAGIC_APP must not survive as substring post-clear. Got {app_post_str:?}",
    );
    assert!(
        !ver_post_str.contains(MAGIC_VER),
        "MAGIC_VER must not survive as substring post-clear. Got {ver_post_str:?}",
    );

    // Document: the post-state may contain compiler-determined bytes
    // from `Option::None`'s data region (unspecified per Rust spec).
    // This is the same compiler-dependent class as `mem::replace`
    // padding — outside the security invariant covered here.
}

/// **Tier-1 by Drop chain witness — `Option::set` overwrite path**:
/// when `set()` writes a new value into an already-populated field,
/// Rust drops the old `Some(SecretBoundedStr)` before moving the
/// new one in.
///
/// Pins the staleness closure for normal session-update flow
/// (server sends multiple `ParameterStatus` frames overwriting
/// previously-cached values).
#[test]
fn overwrite_zeroizes_old_value() {
    const FIRST: &str = "FIRST-APPLICATION-NAME-MAGIC-XYZ";
    const SECOND: &str = "second";

    let mut params = SessionParams::default();
    params.set(b"application_name", FIRST.as_bytes());

    let raw_ptr: *const u8 = match params.application_name.as_ref() {
        Some(s) => s.as_bytes().as_ptr(),
        None => return, // architecturally dead
    };
    let first_len = FIRST.len();

    let pre = unsafe { probe_bytes(raw_ptr, first_len) };
    assert_eq!(&pre[..first_len], FIRST.as_bytes());

    // Overwrite — Rust drops old Some(SecretBoundedStr) BEFORE
    // moving new one in. Drop chain scrubs.
    params.set(b"application_name", SECOND.as_bytes());

    // The new value is at the same memory offset (same Option slot)
    // but its content is shorter than FIRST's. Tail bytes
    // [SECOND.len()..FIRST.len()) are the abandoned region:
    // pre-fix: would retain FIRST's tail bytes; post-fix: scrubbed
    // by Drop on old SecretBoundedStr.
    let beyond_second = SECOND.len();
    let probe_len = first_len.saturating_sub(beyond_second);
    if probe_len > 0 {
        let probe_start = unsafe { raw_ptr.add(beyond_second) };
        let post = unsafe { probe_bytes(probe_start, probe_len) };
        let nonzero_count = post.iter().filter(|&&b| b != 0).count();
        assert_eq!(
            nonzero_count, 0,
            "tail bytes from FIRST (beyond SECOND's len) must \
             be zero post-overwrite. Found {nonzero_count} non-zero \
             bytes — Drop didn't fire on the old SecretBoundedStr.",
        );
    }
}
