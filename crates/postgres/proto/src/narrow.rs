//! Typed numeric-narrowing / widening helpers with single-audit-point
//! encapsulation of the `try_from(...).unwrap_or(SATURATION)` pattern.
//!
//! # Why this module exists
//!
//! Stable Rust requires `try_from` (Result-returning) for narrowing
//! conversions even when the conversion is provably infallible under
//! a target-feature precondition (e.g. `usize::BITS >= 32` const-
//! asserted at the crate root makes `usize::try_from(u32)` infallible
//! on every supported target). The `unwrap_or(saturation_value)`
//! landing pad is the forbid-bundle-compliant syntactic shape
//! (`expect` / `unwrap` / `as` are all banned) but each site repeats
//! the dead-arm fallback.
//!
//! The helpers below collapse N callsite dead-arms into ONE encapsulated
//! audit point per conversion kind. Each helper documents its
//! precondition; the architecturally-dead branch is captured inside
//! the helper, marked `cold_path()`, and contributes a known
//! fail-closed saturation that callers can rely on.
//!
//! # Tier classification
//!
//! - **Widening (e.g. `u32 → usize`)**: tier-1 by const-assert. The
//!   crate root pins `usize::BITS >= 32`; the `try_from` Err arm is
//!   architecturally unreachable on any supported target. The helper
//!   surface keeps the dead `unwrap_or(0)` for forbid-bundle compliance
//!   in ONE place.
//! - **Narrowing under caller precondition (e.g. `usize → u32` where
//!   `v <= u32::MAX` is proved by the caller)**: tier-2 by-discipline
//!   centralised — the precondition lives in the helper's docstring +
//!   caller's call site, dead arm encapsulated.
//! - **Same-bit-width bit-cast (e.g. non-negative `i16 → u16`)**:
//!   tier-1 by-bit-pattern via `to_le_bytes` / `from_le_bytes` round-
//!   trip. No `try_from`, no dead arm — the conversion is structurally
//!   identity for the proved-non-negative case.

/// Widen `u32 → usize`. **Infallible on every supported target.**
///
/// # Precondition
///
/// The crate-root const-assert `usize::BITS >= 32` pins this helper's
/// infallibility. The dead `unwrap_or(0)` landing pad lives here and
/// here only — replacing N call-site dead-arms with one encapsulated
/// audit point.
///
/// # Behaviour on architecturally-dead branch
///
/// Returns `0`. Reachable only on a 16-bit target that bypassed the
/// `usize::BITS >= 32` crate-root assert (impossible under intact
/// build configuration); cold-path-marked.
#[inline]
#[must_use]
pub(crate) fn usize_from_u32(v: u32) -> usize {
    const _: () = assert!(
        usize::BITS >= 32,
        "narrow::usize_from_u32 requires usize::BITS >= 32",
    );
    // `TryFrom` is not yet `const` on stable Rust; the helper is a
    // regular `fn` (inlined). LLVM compiles the dead-arm branch out
    // under the const-asserted precondition. The `unwrap_or(0)`
    // landing pad is the canonical forbid-bundle-compliant shape
    // (`expect` / `unwrap` / `as` are banned); it lives here ONCE
    // as the single audit point — replacing what would otherwise be
    // N call-site dead-arms across the crate.
    usize::try_from(v).unwrap_or(0)
}

/// Narrow `usize → u16` under the **caller-asserted precondition**
/// `v <= u16::MAX` (typically `v <= READ_BUF_CAP ≤ u16::MAX`).
///
/// # Precondition
///
/// Caller must prove `v <= u16::MAX` at the call site. The helper's
/// dead arm fires only on precondition violation.
///
/// # Behaviour on architecturally-dead branch
///
/// Saturates to `u16::MAX`. Saturation is the correct loud-fail-
/// closed signal — downstream consumers either reject MAX as
/// malformed or carry it through as a visible regression.
#[inline]
#[must_use]
pub(crate) fn u16_from_usize_under_u16_bound(v: usize) -> u16 {
    // Same single-audit-point pattern as
    // `u32_from_usize_under_u32_bound`.
    u16::try_from(v).unwrap_or(u16::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usize_from_u32_round_trips() {
        assert_eq!(usize_from_u32(0), 0);
        assert_eq!(usize_from_u32(1), 1);
        assert_eq!(usize_from_u32(u32::MAX), usize::try_from(u32::MAX).unwrap_or(0));
    }

    #[test]
    fn u16_from_usize_under_u16_bound_round_trips() {
        assert_eq!(u16_from_usize_under_u16_bound(0), 0);
        assert_eq!(u16_from_usize_under_u16_bound(1), 1);
        assert_eq!(u16_from_usize_under_u16_bound(usize::from(u16::MAX)), u16::MAX);
    }

    #[test]
    fn u16_from_usize_saturates_on_overflow() {
        // Precondition violation: v > u16::MAX. Saturates loudly.
        let oversize = usize::from(u16::MAX).saturating_add(1);
        assert_eq!(u16_from_usize_under_u16_bound(oversize), u16::MAX);
    }

}
