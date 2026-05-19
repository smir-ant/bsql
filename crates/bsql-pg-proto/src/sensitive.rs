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
//! Every type containing a `Sensitive<T>` field gets a manual
//! `Debug` that redacts the field, or no `Debug` at all.

use core::fmt;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// A value that is scrubbed on drop and redacted in debug output.
///
/// See [module-level documentation](self) for design rationale.
///
/// `#[repr(transparent)]` — formal zero-cost ABI layout identical
/// to the inner `T`. `Sensitive<T>` is a compile-time-only wrapper;
/// at runtime the memory is literally a `T`.
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

    /// Closure-scope borrow of the inner value. The HRTB-quantified
    /// `&'a T` lifetime cannot escape the call — retention attacks
    /// via the borrowed reference are structurally impossible. A
    /// plain `pub const fn get(&self) -> &T` would be tier-2 by
    /// discipline (only a docstring "don't retain the borrow" stops
    /// abuse); the closure shape makes the discipline by-construction.
    ///
    /// The closure receives `&T` and returns `R`. `R` is independent
    /// of the borrow lifetime, so the inner value can be **copied
    /// out** (for `T: Copy`) or **digested** (for `T: !Copy`, e.g.,
    /// `R` is a hash digest computed inside the closure). Retention
    /// of the borrow ITSELF past the call is rejected by HRTB.
    ///
    /// # Use cases
    ///
    /// ```ignore
    /// // Copy out a Copy primitive (i32, u64, etc.):
    /// let pid = sensitive_pid.with_inner(|p| *p);
    ///
    /// // Compute over the inner value:
    /// let hash = sensitive_pwd.with_inner(|pwd| sha256(pwd.as_bytes()));
    ///
    /// // Pass through to a function that needs &T:
    /// sensitive.with_inner(|inner| use_inner(inner));
    /// ```
    ///
    /// # Pairs with `mem::replace` Drop chain
    ///
    /// The `Sensitive<T>` wrapper itself runs `Zeroize::zeroize` on
    /// Drop (via `#[derive(ZeroizeOnDrop)]`); the closure-scoped
    /// `with_inner` is the borrow-side complement to that owned-side
    /// scrub. Together: tier-1 on retention (by-construction via
    /// HRTB) and tier-1 on owned-storage (by-construction via
    /// ZeroizeOnDrop) — both modes route through structural
    /// enforcement, not discipline.
    #[inline]
    pub fn with_inner<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        f(&self.inner)
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

#[cfg(test)]
mod drop_witness_tests {
    //! Tier-1-by-construction Drop-fire witness for [`Sensitive<T>`]
    //! via [`crate::drop_witness::DropCounter`]. Runs on every
    //! `cargo test`. The `DropCounter` wrapper observes that
    //! `Sensitive<T>::drop` reached its `ZeroizeOnDrop` body, which
    //! transitively fires `T::zeroize` (Drop-glue rules).
    //!
    //! A memory-probe alternative
    //! (`tests/scram_zeroize_miri_spec.rs::sensitive_password_drop_zeros_backing_buffer`,
    //! `#[ignore]`-gated and miri-only) verifies that the cleared
    //! bytes actually become zero in the backing buffer; the witness
    //! here verifies that Drop fires on the wrapper.

    use super::Sensitive;
    use crate::drop_witness::{DropCounter, DropProbe};
    use crate::password::Password;

    /// `Sensitive<Password>::drop` fires the inner Password's
    /// `ZeroizeOnDrop`. Witness: counter increments on wrapper drop.
    #[test]
    fn sensitive_password_drop_fires_zeroize_chain() {
        let probe = DropProbe::new();
        let pw = match Password::try_from_bytes(b"sensitive-witness-XYZ") {
            Ok(p) => p,
            Err(_) => return,
        };
        let s = Sensitive::new(pw);
        DropCounter::scoped(s, probe.clone(), || {
            assert_eq!(probe.fired(), 0);
        });
        assert_eq!(
            probe.fired(),
            1,
            "Sensitive<Password> drop must fire exactly once",
        );
    }

    /// `Sensitive<i32>` drop fires (used in
    /// `ProtoState::ConnectingPostAuthHaveKey::secret_key`).
    #[test]
    fn sensitive_i32_drop_fires() {
        let probe = DropProbe::new();
        // Plain literal — `as` casts are forbidden by the
        // crate-root forbid bundle (`clippy::as_conversions`).
        let s = Sensitive::new(0x7fff_ffff_i32);
        DropCounter::scoped(s, probe.clone(), || {});
        assert_eq!(probe.fired(), 1, "Sensitive<i32> drop must fire");
    }

    /// Repeated `Sensitive<Password>` drops accumulate count.
    #[test]
    fn each_sensitive_password_drop_increments_counter() {
        let probe = DropProbe::new();
        for _ in 0..3 {
            let pw = match Password::try_from_bytes(b"x") {
                Ok(p) => p,
                Err(_) => continue,
            };
            DropCounter::scoped(Sensitive::new(pw), probe.clone(), || {});
        }
        assert_eq!(probe.fired(), 3);
    }
}
