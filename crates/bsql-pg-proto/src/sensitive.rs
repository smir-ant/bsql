//! Zero-on-drop wrapper for secret values.
//!
//! [`Sensitive`] wraps a value that must never leak into logs, debug
//! output, or core dumps. On drop the inner bytes are overwritten with
//! zeros via [`zeroize::Zeroize`]. The [`Debug`] impl prints
//! `"<REDACTED>"` unconditionally.
//!
//! Deliberately: no `Copy`, no `Clone`. A secret value has one owner;
//! duplicating it doubles the scrub surface for zero benefit.
//!
//! # When to use vs `zeroize::Zeroizing<T>`
//!
//! `Zeroizing<T>` zeroes on drop but still delegates `Debug` to `T`.
//! `Sensitive<T>` adds the redaction. Use `Sensitive` for anything
//! whose debug representation must never appear (passwords, keys,
//! proofs). Use `Zeroizing` for intermediate buffers whose debug is
//! harmless (e.g. a scratch `[u8; 32]` that is never named in user
//! diagnostics).
//!
//! Per DEF-048: every type containing a `Sensitive<T>` field gets a
//! manual `Debug` that redacts the field, or no `Debug` at all.

use core::fmt;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// A value that is scrubbed on drop and redacted in debug output.
///
/// See [module-level documentation](self) for design rationale.
///
/// `#[repr(transparent)]` (DEF-093) — formal zero-cost ABI layout
/// identical to the inner `T`. `Sensitive<T>` is a compile-time-only
/// wrapper; at runtime the memory is literally a `T`.
#[derive(Zeroize, ZeroizeOnDrop)]
#[repr(transparent)]
pub struct Sensitive<T: Zeroize> {
    inner: T,
}

impl<T: Zeroize> Sensitive<T> {
    /// Wrap a value in a `Sensitive` container.
    #[inline]
    pub const fn new(value: T) -> Self {
        Self { inner: value }
    }

    /// Borrow the inner value.
    ///
    /// The borrow is intentionally short-lived — the caller must not
    /// store the reference beyond the immediate computation.
    #[inline]
    pub const fn get(&self) -> &T {
        &self.inner
    }
}

impl<T: Zeroize> fmt::Debug for Sensitive<T> {
    /// Prints `"<REDACTED>"` unconditionally — never delegates to `T`'s
    /// Debug.
    ///
    /// # Test-pinned invariant
    ///
    /// Pinned by `tests/startup_spec.rs::sensitive_debug_does_not_leak_inner_value`
    /// which asserts the output contains `"REDACTED"` and does NOT
    /// contain the inner value's bytes. A one-line impl drift
    /// (e.g., replacing `write_str("<REDACTED>")` with
    /// `debug_struct("Sensitive").field("inner", &self.inner).finish()`)
    /// compiles silently but fails that test.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<REDACTED>")
    }
}
