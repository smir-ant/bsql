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
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct Sensitive<T: Zeroize> {
    inner: T,
}

impl<T: Zeroize> Sensitive<T> {
    /// Wrap a value in a `Sensitive` container.
    #[inline]
    pub fn new(value: T) -> Self {
        Self { inner: value }
    }

    /// Borrow the inner value.
    ///
    /// The borrow is intentionally short-lived — the caller must not
    /// store the reference beyond the immediate computation.
    #[inline]
    pub fn get(&self) -> &T {
        &self.inner
    }

    /// Mutably borrow the inner value.
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: Zeroize> fmt::Debug for Sensitive<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<REDACTED>")
    }
}
