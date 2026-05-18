//! DEF-280 Bundle E Phase 1 (2026-05-18) — `trybuild` golden harness
//! for the `Sensitive::get` → `Sensitive::with_inner` closure-scope
//! migration.
//!
//! Pre-Bundle E `Sensitive<T>` had `pub const fn get(&self) -> &T`
//! with a docstring discipline («caller must not store the
//! reference beyond the immediate computation»). Bundle E migrated
//! this to `pub fn with_inner<R>(&self, f: impl FnOnce(&T) -> R)
//! -> R` (tier-1 by-construction — HRTB-scoped borrow cannot
//! escape). This trybuild harness pins that the old method is now
//! absent from the public surface.
//!
//! # Probes
//!
//! - **P-D280E-1** `Sensitive::<T>::get()` is method-absent →
//!   `E0599`. External callers MUST go through `with_inner`.
//!
//! # Regenerating goldens
//!
//! ```sh
//! TRYBUILD=overwrite cargo test --test def280e_compile_fail
//! ```

#![forbid(unsafe_code)]

/// **P-D280E-1** — `Sensitive::<T>::get()` is method-absent.
/// Expected: E0599.
#[test]
fn p_d280e_1_sensitive_get_method_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def280e_compile_fail/p_d280e_1_sensitive_get_method_absent.rs");
}
