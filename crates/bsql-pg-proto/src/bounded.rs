//! DEF-195 — bounded unsigned integer with niche-optimised `Option`.
//!
//! [`BoundedU8<MAX>`] holds values in the inclusive range `0..=MAX`.
//! Construction is fallible via [`BoundedU8::try_new`] (rejects values
//! `> MAX`) — tier-2 by-construct.
//!
//! # Memory layout / niche
//!
//! Internally a `NonZeroU8` carrying `value + 1`. Stored 1..=MAX+1
//! corresponds to logical 0..=MAX. The byte 0 is unused — Rust's
//! `NonZeroU8` niche tells the compiler `Option<NonZeroU8>` (and by
//! extension `Option<BoundedU8<MAX>>` since `repr(transparent)`) fits
//! in a single byte: the discriminant bit-pattern is the all-zeros
//! niche. No `as` casts, no `unsafe`, fully stable Rust.
//!
//! # Why offset-by-one over `rustc_layout_scalar_valid_range`?
//!
//! The latter is unstable (and would also need `unsafe`). The
//! offset-by-one trick gives identical memory layout (1 byte) and
//! `Option<BoundedU8<MAX>>` niche at zero — on stable Rust, no
//! `unsafe`, no nightly. Trade-off: `get()` does `wrapping_sub(1)`
//! (one ALU op, branchless, infallible). Negligible.
//!
//! # Use site (DEF-195)
//!
//! [`crate::decode::RowDesc::n_columns`] uses `BoundedU8<32>` (one
//! per `MAX_ROW_COLUMNS`). The struct's first-field `NonZeroU8` niche
//! lets `Option<RowDesc>` shrink from 140 B → 136 B (4 B saved on the
//! single discriminant slot that previously rounded up to alignment).
//! Pre-rolled into [`crate::PgProtocol`] via the `row_desc_slot:
//! Option<RowDesc>` field — saving 4 B per protocol instance.

use core::num::NonZeroU8;

/// Unsigned 8-bit integer constrained to the inclusive range `0..=MAX`.
///
/// Construction goes through [`Self::try_new`] which rejects values
/// `> MAX` — **tier-2 by-construct**: an out-of-range `BoundedU8<MAX>`
/// cannot exist. Type-level `MAX` is a `const` parameter, so the
/// constraint is part of the type signature.
///
/// `Option<BoundedU8<MAX>>` is the same size as `BoundedU8<MAX>` (1 byte)
/// thanks to the underlying `NonZeroU8` niche.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct BoundedU8<const MAX: u8>(NonZeroU8);

impl<const MAX: u8> BoundedU8<MAX> {
    /// The logical value `0` — always valid since `MAX >= 0`.
    /// `NonZeroU8::MIN == 1` corresponds to logical 0 in the
    /// offset-by-one encoding.
    pub const ZERO: Self = Self(NonZeroU8::MIN);

    /// **Tier-1 compile-time construction.** `VAL > MAX` is a build
    /// failure (const-block assertion fires at monomorphisation site).
    ///
    /// Use for compile-time-known values (literals, sentinels, test
    /// fixtures). For runtime values use [`Self::try_new`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bsql_pg_proto::bounded::BoundedU8;
    /// const FIVE: BoundedU8<32> = BoundedU8::<32>::new_const::<5>();
    /// assert_eq!(FIVE.get(), 5);
    /// ```
    ///
    /// Out-of-range values are compile-rejected:
    ///
    /// ```compile_fail
    /// use bsql_pg_proto::bounded::BoundedU8;
    /// // ERROR: VAL exceeds MAX (33 > 32) — build fails at this line.
    /// const TOO_BIG: BoundedU8<32> = BoundedU8::<32>::new_const::<33>();
    /// ```
    #[inline]
    #[must_use]
    pub const fn new_const<const VAL: u8>() -> Self {
        const {
            assert!(
                MAX < u8::MAX,
                "BoundedU8<MAX>: MAX must be < 255 for the NonZeroU8 niche to work",
            );
            assert!(
                VAL <= MAX,
                "BoundedU8::<MAX>::new_const::<VAL>: VAL exceeds MAX (compile-time tier-1 enforced)",
            );
        }
        // VAL <= MAX < 255, so VAL+1 in 1..=MAX+1 ⊆ 1..=255 (non-zero u8).
        match NonZeroU8::new(VAL.wrapping_add(1)) {
            Some(nz) => Self(nz),
            // Dead per the `MAX < u8::MAX` and `VAL <= MAX` const-asserts
            // above. Compiler can't fold without an `unsafe` hint; the
            // sentinel keeps the type-system happy without panicking.
            None => Self(NonZeroU8::MIN),
        }
    }

    /// **Tier-3 runtime construction.** Returns `None` if `value > MAX`.
    ///
    /// Use for runtime-derived values (wire input, parsed bytes, etc).
    /// For compile-time-known values prefer [`Self::new_const`] — its
    /// out-of-range case is a build failure rather than a `None` to
    /// handle at runtime.
    ///
    /// `const fn` so const-context consumers can fold the runtime check
    /// at compile time when the input is itself const.
    #[inline]
    #[must_use]
    pub const fn try_new(value: u8) -> Option<Self> {
        if value > MAX {
            return None;
        }
        // value <= MAX <= 255, so `value + 1` fits u8.
        // `value + 1 >= 1`, so NonZeroU8::new returns Some.
        // `wrapping_add` because the forbid bundle disallows `+`
        // for arithmetic_side_effects; `wrapping` is allowed and
        // correct here since the sum fits u8 by the bound above.
        match NonZeroU8::new(value.wrapping_add(1)) {
            Some(nz) => Some(Self(nz)),
            // Dead branch: `value + 1 >= 1` so `NonZeroU8::new` is `Some`.
            // Compiler can't fold this without an `unsafe` hint; the
            // branch is well-predicted to not-taken in practice.
            None => None,
        }
    }

    /// Logical value in the range `0..=MAX`.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u8 {
        // stored = value + 1; stored >= 1; stored - 1 = value, infallible.
        self.0.get().wrapping_sub(1)
    }
}

/// Tier-1 compile-time construction macro for [`BoundedU8`].
///
/// `bounded_u8!(MAX, VAL)` expands to `BoundedU8::<MAX>::new_const::<VAL>()`.
/// Both `MAX` and `VAL` must be u8 literals; `VAL > MAX` is a build
/// error.
///
/// # Examples
///
/// ```
/// use bsql_pg_proto::bounded_u8;
/// let five: bsql_pg_proto::bounded::BoundedU8<32> = bounded_u8!(32, 5);
/// assert_eq!(five.get(), 5);
/// ```
///
/// ```compile_fail
/// use bsql_pg_proto::bounded_u8;
/// // ERROR: 33 > 32 — caught at compile time.
/// let too_big: bsql_pg_proto::bounded::BoundedU8<32> = bounded_u8!(32, 33);
/// ```
#[macro_export]
macro_rules! bounded_u8 {
    ($max:literal, $val:literal) => {
        $crate::bounded::BoundedU8::<$max>::new_const::<$val>()
    };
}

impl<const MAX: u8> core::fmt::Debug for BoundedU8<MAX> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "BoundedU8<{MAX}>({})", self.get())
    }
}

impl<const MAX: u8> Default for BoundedU8<MAX> {
    /// Default is `0` (logical), regardless of `MAX`.
    #[inline]
    fn default() -> Self {
        Self::ZERO
    }
}

// ─── Tier-1 closure: per-MAX size + niche pins ──────────────────────
//
// Pin the layout invariants in const-asserts so a future stdlib change
// or lint-bundle adjustment that loses the niche becomes a compile
// failure — not a silent layout regression that adds a discriminant
// byte to every `Option<BoundedU8>` in the crate.

const _: () = assert!(
    core::mem::size_of::<BoundedU8<32>>() == 1,
    "BoundedU8<32> must be exactly 1 byte (NonZeroU8 + repr(transparent))",
);
const _: () = assert!(
    core::mem::size_of::<Option<BoundedU8<32>>>() == 1,
    "Option<BoundedU8<32>> must be 1 byte — NonZeroU8 niche absorbs the \
     discriminant. If this fails, repr(transparent) was lost or the \
     stdlib's NonZeroU8 niche layout changed.",
);
const _: () = assert!(
    core::mem::align_of::<BoundedU8<32>>() == 1,
    "BoundedU8<32> alignment must be 1 (u8 alignment).",
);

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper: `try_new(value)` for a value asserted to be ≤ MAX.
    /// `unwrap_or(ZERO)` keeps the forbid-bundle (`expect_used`,
    /// `unwrap_used` banned) clean while letting downstream `assert_eq!`
    /// detect the rare none-case via `value != 0`.
    fn must_bound<const MAX: u8>(value: u8) -> BoundedU8<MAX> {
        match BoundedU8::<MAX>::try_new(value) {
            Some(b) => b,
            None => BoundedU8::<MAX>::ZERO,
        }
    }

    #[test]
    fn try_new_within_range() {
        for value in 0..=32u8 {
            let bounded = BoundedU8::<32>::try_new(value);
            assert!(bounded.is_some(), "value {value} <= 32 must succeed");
            let inner = must_bound::<32>(value);
            assert_eq!(inner.get(), value);
        }
    }

    #[test]
    fn try_new_above_range_rejects() {
        for value in 33..=255u8 {
            let bounded = BoundedU8::<32>::try_new(value);
            assert!(bounded.is_none(), "value {value} > 32 must reject");
        }
    }

    #[test]
    fn zero_const_get_returns_zero() {
        assert_eq!(BoundedU8::<32>::ZERO.get(), 0);
    }

    #[test]
    fn default_is_zero() {
        assert_eq!(BoundedU8::<32>::default().get(), 0);
    }

    #[test]
    fn size_invariants() {
        assert_eq!(core::mem::size_of::<BoundedU8<32>>(), 1);
        assert_eq!(core::mem::size_of::<Option<BoundedU8<32>>>(), 1);
        assert_eq!(core::mem::align_of::<BoundedU8<32>>(), 1);
    }

    #[test]
    fn boundary_values_round_trip() {
        for value in 0..=32u8 {
            let b = must_bound::<32>(value);
            assert_eq!(b.get(), value);
        }
    }

    #[test]
    fn order_relations_match_underlying() {
        let zero = must_bound::<32>(0);
        let small = must_bound::<32>(5);
        let max = must_bound::<32>(32);
        assert!(zero < small);
        assert!(small < max);
        assert_eq!(zero, BoundedU8::<32>::ZERO);
    }

    #[test]
    fn debug_format_carries_max_and_value() {
        let value = must_bound::<32>(7);
        let formatted = std::format!("{value:?}");
        assert!(formatted.contains("32"), "{formatted}");
        assert!(formatted.contains('7'), "{formatted}");
    }

    // ─── Tier-1 compile-time construction (DEF-195 ext) ────────────

    /// Verifies `new_const` builds and round-trips for all valid VAL.
    #[test]
    fn new_const_round_trips_for_all_valid_values() {
        const V0: BoundedU8<32> = BoundedU8::<32>::new_const::<0>();
        const V1: BoundedU8<32> = BoundedU8::<32>::new_const::<1>();
        const V16: BoundedU8<32> = BoundedU8::<32>::new_const::<16>();
        const V31: BoundedU8<32> = BoundedU8::<32>::new_const::<31>();
        const V32: BoundedU8<32> = BoundedU8::<32>::new_const::<32>();
        assert_eq!(V0.get(), 0);
        assert_eq!(V1.get(), 1);
        assert_eq!(V16.get(), 16);
        assert_eq!(V31.get(), 31);
        assert_eq!(V32.get(), 32);
    }

    #[test]
    fn new_const_zero_equals_zero_const() {
        const V0: BoundedU8<32> = BoundedU8::<32>::new_const::<0>();
        assert_eq!(V0, BoundedU8::<32>::ZERO);
    }

    #[test]
    fn macro_expansion_round_trips() {
        let v: BoundedU8<32> = crate::bounded_u8!(32, 5);
        assert_eq!(v.get(), 5);
        let v_max: BoundedU8<32> = crate::bounded_u8!(32, 32);
        assert_eq!(v_max.get(), 32);
    }
}
