//! Bounded unsigned integers with niche-optimised `Option`.
//!
//! Unified module exposing two parallel newtypes:
//!
//! * [`BoundedU8<MAX>`]   — for `MAX ≤ 254`. Storage: 1 byte (`NonZeroU8`).
//! * [`BoundedU16<MAX>`]  — for `MAX ≤ 65534`. Storage: 2 bytes (`NonZeroU16`).
//!
//! Both take `const MAX: usize` so they integrate uniformly with
//! `usize`-based const-generic structs (`PodBytes<N>`, `FixedStr<N, Tag>`,
//! etc.).
//!
//! # Tier surface
//!
//! | Surface | Tier | Mechanism |
//! |---------|------|-----------|
//! | Compile-time const construction | tier-1 | `new_const::<VAL>()` const-block assertion |
//! | Runtime construction | tier-2 by-construct | `try_new(value)` returns `Option<Self>`; the type then carries the `≤ MAX` invariant statically at every use site |
//! | Use-site (`.get()`, ord, eq) | tier-2 | Type-system invariant |
//! | `Option<Self>` discriminant | tier-1 niche | NonZeroU{8,16}::MIN as the all-zeros niche absorbs the discriminant |
//!
//! Bridging trait [`BoundedLen`] lets generic struct fields write
//! length-storage code uniformly:
//!
//! ```text
//! pub struct Container<const N: usize, LenT: BoundedLen<N>> {
//!     buf: [u8; N],
//!     len: LenT,  // either BoundedU8<N> or BoundedU16<N>
//! }
//! ```
//!
//! # Memory layout / niche
//!
//! Internally each type holds a `NonZero{U8,U16}` carrying `value + 1`.
//! Stored 1..=MAX+1 corresponds to logical 0..=MAX. The all-zeros bit
//! pattern is unused — Rust's `NonZero` niche tells the compiler
//! `Option<Self>` (and by extension `Option<Container<.., Self>>`)
//! fits in the same bytes as `Self`: the discriminant bit-pattern is
//! the all-zeros niche. No `as` casts, no `unsafe`, fully stable Rust.
//!
//! # Why offset-by-one over `rustc_layout_scalar_valid_range`?
//!
//! The latter is unstable AND `unsafe`. The offset-by-one trick gives
//! identical memory layout (1 / 2 bytes) and `Option<Self>` niche at
//! zero — on stable Rust, no `unsafe`, no nightly. Trade-off: `get()`
//! does `wrapping_sub(1)` (one ALU op, branchless, infallible).
//! Negligible.

use core::num::{NonZeroU8, NonZeroU16};

// ─── BoundedLen trait — uniform interface for length-storage fields ──

/// Sealed trait bridging [`BoundedU8`] and [`BoundedU16`] for use as
/// generic length-storage parameters in container types.
///
/// `BoundedLen<N>` exposes the runtime API a length field needs:
/// `try_new_usize(usize) → Option<Self>` for fallible construction
/// from a usize, and `get_usize(self) → usize` for accessor widening
/// at use-site (typically slice indexing).
///
/// `MAX` const reflects the bound at compile time so callers (and
/// const-asserts) can inspect it.
///
/// Implemented for `BoundedU8<MAX>` and `BoundedU16<MAX>` via macro;
/// the `sealed` mod ensures no out-of-crate impls so the trait
/// remains a closed sum-type.
///
/// # Build-fail proof
///
/// Naming `<BoundedU8<MAX> as BoundedLen<MAX>>::MAX` for `MAX > 254`
/// is a build error (the `const { assert!(MAX <= 254) }` block in
/// the impl associated const fires at const-eval-time):
///
/// ```compile_fail
/// use bsql_postgres_proto::bounded::{BoundedLen, BoundedU8};
/// const _: usize = <BoundedU8<300> as BoundedLen<300>>::MAX;
/// ```
///
/// Same for `BoundedU16<MAX>` with `MAX > 65_534`:
///
/// ```compile_fail
/// use bsql_postgres_proto::bounded::{BoundedLen, BoundedU16};
/// const _: usize = <BoundedU16<70_000> as BoundedLen<70_000>>::MAX;
/// ```
// Structural diagnostic. The supertrait failure on the sealed
// `Sealed` bound is not actionable from downstream; pointing at the
// two carrier types is.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a `BoundedLen<{N}>` carrier",
    label = "valid carriers are `BoundedU8<{N}>` (when `{N} <= 254`) and `BoundedU16<{N}>` (when `{N} <= 65_534`)",
    note = "`BoundedLen` is sealed — only the two crate-internal carriers qualify; downstream `impl BoundedLen for ...` is forbidden by construction"
)]
pub trait BoundedLen<const N: usize>:
    Sized + Copy + Default + PartialEq + Eq + core::fmt::Debug + sealed::Sealed
{
    /// The compile-time bound `0 ≤ value ≤ MAX`.
    const MAX: usize;

    /// Construct from a runtime `usize` value; `None` if `value > MAX`.
    fn try_new_usize(value: usize) -> Option<Self>;

    /// Logical value as a `usize`.
    fn get_usize(self) -> usize;
}

mod sealed {
    pub trait Sealed {}
}

// ─── BoundedU8<const MAX: usize> ─────────────────────────────────────

/// Unsigned 8-bit-stored integer constrained to the inclusive range
/// `0..=MAX` for `MAX ≤ 254`.
///
/// See module docs for tier surface. `Option<BoundedU8<MAX>>` is the
/// same size as `BoundedU8<MAX>` (1 byte) thanks to the underlying
/// `NonZeroU8` niche.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct BoundedU8<const MAX: usize>(NonZeroU8);

impl<const MAX: usize> sealed::Sealed for BoundedU8<MAX> {}

impl<const MAX: usize> BoundedU8<MAX> {
    // Tier-1 enforcement of `MAX <= 254` lives inside every
    // publicly-reachable monomorph site (`ZERO`, `new_const`,
    // `try_new` — all have inline `const { assert!(MAX <= 254, …) }`
    // blocks). A standalone `_ASSERT_MAX_FITS_NICHE` associated const
    // would only fire when *referenced*, which never happens in
    // practice.

    /// The logical value `0` — always valid since `MAX ≥ 0`.
    /// Stored as `NonZeroU8::MIN == 1` (offset-by-one encoding).
    pub const ZERO: Self = const {
        assert!(
            MAX <= 254,
            "BoundedU8<MAX>: MAX must be ≤ 254 (use BoundedU16 for larger MAX)",
        );
        Self(NonZeroU8::MIN)
    };

    /// **Tier-1 compile-time construction.** `VAL > MAX` is a build
    /// failure (const-block assertion fires at monomorphisation site).
    ///
    /// Use for compile-time-known values (literals, sentinels, test
    /// fixtures). For runtime values use [`Self::try_new`].
    ///
    /// # Examples
    ///
    /// ```
    /// use bsql_postgres_proto::bounded::BoundedU8;
    /// const FIVE: BoundedU8<32> = BoundedU8::<32>::new_const::<5>();
    /// assert_eq!(FIVE.get(), 5);
    /// ```
    ///
    /// Out-of-range `VAL` is compile-rejected:
    ///
    /// ```compile_fail
    /// use bsql_postgres_proto::bounded::BoundedU8;
    /// const TOO_BIG: BoundedU8<32> = BoundedU8::<32>::new_const::<33>();
    /// ```
    #[inline]
    #[must_use]
    pub const fn new_const<const VAL: u8>() -> Self {
        const {
            assert!(MAX <= 254, "BoundedU8<MAX>: MAX must be ≤ 254");
            // `VAL` is `u8` so `VAL` ≤ 255. We compare via the
            // low-byte view of `MAX` (a usize) since `as` casts are
            // forbidden by the workspace clippy bundle. For `MAX ≤ 254`,
            // the low byte equals MAX itself.
            let max_lo: u8 = MAX.to_le_bytes()[0];
            assert!(
                VAL <= max_lo,
                "BoundedU8::<MAX>::new_const::<VAL>: VAL exceeds MAX (compile-time tier-1 enforced)",
            );
        }
        // VAL ≤ MAX ≤ 254, so VAL+1 ∈ 1..=255 (non-zero u8).
        match NonZeroU8::new(VAL.wrapping_add(1)) {
            Some(nz) => Self(nz),
            // Dead per the const-asserts above.
            None => Self(NonZeroU8::MIN),
        }
    }

    /// **Tier-2 runtime construction.** Returns `None` if `value > MAX`.
    ///
    /// Use for runtime-derived values (wire input, parsed bytes). For
    /// compile-time-known values prefer [`Self::new_const`].
    #[inline]
    #[must_use]
    pub fn try_new(value: u8) -> Option<Self> {
        const { assert!(MAX <= 254, "BoundedU8<MAX>: MAX must be ≤ 254"); }
        if usize::from(value) > MAX {
            return None;
        }
        // value ≤ MAX ≤ 254 so value+1 ∈ 1..=255 (non-zero u8).
        Some(Self(NonZeroU8::new(value.wrapping_add(1))?))
    }

    /// Logical value in the range `0..=MAX`.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u8 {
        // stored = value + 1 ≥ 1; stored - 1 = value, infallible.
        self.0.get().wrapping_sub(1)
    }
}

impl<const MAX: usize> BoundedLen<MAX> for BoundedU8<MAX> {
    // When `<BoundedU8<MAX> as BoundedLen<MAX>>::MAX` is named at a
    // callsite, the inline const-block fires at const-eval-time and
    // asserts `MAX <= 254`. A bare `const MAX: usize = MAX` (no
    // assert) would silently return `300` for a hypothetical
    // `BoundedU8<300>`; the assert at the trait-surface naming site
    // matches the discipline of `ZERO` / `new_const` / `try_new`
    // (each carries its own `const { assert!(MAX <= 254, …) }`). The
    // associated `compile_fail` doctest on the `BoundedLen` trait
    // docstring proves the fire.
    const MAX: usize = const {
        assert!(
            MAX <= 254,
            "BoundedU8<MAX>: MAX must be ≤ 254 (NonZeroU8 niche cap). \
             Use BoundedU16 for MAX in 255..=65_534.",
        );
        MAX
    };

    #[inline]
    fn try_new_usize(value: usize) -> Option<Self> {
        u8::try_from(value).ok().and_then(Self::try_new)
    }

    #[inline]
    fn get_usize(self) -> usize {
        usize::from(self.get())
    }
}

impl<const MAX: usize> core::fmt::Debug for BoundedU8<MAX> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "BoundedU8<{MAX}>({})", self.get())
    }
}

impl<const MAX: usize> Default for BoundedU8<MAX> {
    /// Default is `0` (logical), regardless of `MAX`.
    #[inline]
    fn default() -> Self {
        Self::ZERO
    }
}

// ─── BoundedU16<const MAX: usize> ────────────────────────────────────

/// Unsigned 16-bit-stored integer constrained to the inclusive range
/// `0..=MAX` for `MAX ≤ 65534`.
///
/// Parallel design to [`BoundedU8`] for the larger range. Storage:
/// 2 bytes (`NonZeroU16`). `Option<BoundedU16<MAX>>` is the same size
/// as `BoundedU16<MAX>` thanks to the niche.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct BoundedU16<const MAX: usize>(NonZeroU16);

impl<const MAX: usize> sealed::Sealed for BoundedU16<MAX> {}

impl<const MAX: usize> BoundedU16<MAX> {
    // Tier-1 enforcement of `MAX <= 65_534` lives inside every
    // publicly-reachable monomorph site (`ZERO`, `new_const`,
    // `try_new`). See sister comment in the BoundedU8 impl block.

    /// The logical value `0`. Stored as `NonZeroU16::MIN == 1`.
    pub const ZERO: Self = const {
        assert!(MAX <= 65_534, "BoundedU16<MAX>: MAX must be ≤ 65_534");
        Self(NonZeroU16::MIN)
    };

    /// **Tier-1 compile-time construction.** `VAL > MAX` is a build error.
    ///
    /// # Examples
    ///
    /// ```
    /// use bsql_postgres_proto::bounded::BoundedU16;
    /// const FIVE: BoundedU16<2048> = BoundedU16::<2048>::new_const::<5>();
    /// assert_eq!(FIVE.get(), 5);
    /// ```
    ///
    /// ```compile_fail
    /// use bsql_postgres_proto::bounded::BoundedU16;
    /// const TOO_BIG: BoundedU16<2048> = BoundedU16::<2048>::new_const::<2049>();
    /// ```
    #[inline]
    #[must_use]
    pub const fn new_const<const VAL: u16>() -> Self {
        const {
            assert!(MAX <= 65_534, "BoundedU16<MAX>: MAX must be ≤ 65_534");
            // VAL: u16. Compare to MAX (usize) via low 2 bytes.
            // For MAX ≤ 65534, the low 2 bytes equal MAX.
            let max_bytes = MAX.to_le_bytes();
            let max_u16 = u16::from_le_bytes([max_bytes[0], max_bytes[1]]);
            assert!(
                VAL <= max_u16,
                "BoundedU16::<MAX>::new_const::<VAL>: VAL exceeds MAX",
            );
        }
        // VAL ≤ MAX ≤ 65_534, so VAL+1 ∈ 1..=65_535 (non-zero u16).
        match NonZeroU16::new(VAL.wrapping_add(1)) {
            Some(nz) => Self(nz),
            None => Self(NonZeroU16::MIN),
        }
    }

    /// **Tier-2 runtime construction.** Returns `None` if `value > MAX`.
    #[inline]
    #[must_use]
    pub fn try_new(value: u16) -> Option<Self> {
        const { assert!(MAX <= 65_534, "BoundedU16<MAX>: MAX must be ≤ 65_534"); }
        if usize::from(value) > MAX {
            return None;
        }
        Some(Self(NonZeroU16::new(value.wrapping_add(1))?))
    }

    /// Logical value in the range `0..=MAX`.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0.get().wrapping_sub(1)
    }
}

impl<const MAX: usize> BoundedLen<MAX> for BoundedU16<MAX> {
    // Same shape as the BoundedU8 trait impl above — build-fail when
    // `<BoundedU16<MAX> as BoundedLen<MAX>>::MAX` is named for
    // `MAX > 65_534`, paired `compile_fail` doctest on the
    // `BoundedLen` trait docstring.
    const MAX: usize = const {
        assert!(
            MAX <= 65_534,
            "BoundedU16<MAX>: MAX must be ≤ 65_534 (NonZeroU16 niche cap).",
        );
        MAX
    };

    #[inline]
    fn try_new_usize(value: usize) -> Option<Self> {
        u16::try_from(value).ok().and_then(Self::try_new)
    }

    #[inline]
    fn get_usize(self) -> usize {
        usize::from(self.get())
    }
}

impl<const MAX: usize> core::fmt::Debug for BoundedU16<MAX> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "BoundedU16<{MAX}>({})", self.get())
    }
}

impl<const MAX: usize> Default for BoundedU16<MAX> {
    #[inline]
    fn default() -> Self {
        Self::ZERO
    }
}

// ─── Tier-1 closure: per-MAX size + niche pins ──────────────────────

const _: () = assert!(
    core::mem::size_of::<BoundedU8<32>>() == 1,
    "BoundedU8<32> must be exactly 1 byte (NonZeroU8 + repr(transparent))",
);
const _: () = assert!(
    core::mem::size_of::<Option<BoundedU8<32>>>() == 1,
    "Option<BoundedU8<32>> must be 1 byte — NonZeroU8 niche absorbs the discriminant",
);
const _: () = assert!(
    core::mem::align_of::<BoundedU8<32>>() == 1,
    "BoundedU8<32> alignment must be 1 (u8 alignment)",
);
const _: () = assert!(
    core::mem::size_of::<BoundedU16<2048>>() == 2,
    "BoundedU16<2048> must be exactly 2 bytes (NonZeroU16 + repr(transparent))",
);
const _: () = assert!(
    core::mem::size_of::<Option<BoundedU16<2048>>>() == 2,
    "Option<BoundedU16<2048>> must be 2 bytes — NonZeroU16 niche absorbs the discriminant",
);
const _: () = assert!(
    core::mem::align_of::<BoundedU16<2048>>() == 2,
    "BoundedU16<2048> alignment must be 2 (u16 alignment)",
);

// ─── Tier-1 compile-time construction macros ────────────────────────

/// Tier-1 compile-time construction macro for [`BoundedU8`].
///
/// `bounded_u8!(MAX, VAL)` expands to `BoundedU8::<MAX>::new_const::<VAL>()`.
/// `VAL > MAX` is a build error.
///
/// ```
/// use bsql_postgres_proto::bounded_u8;
/// let five: bsql_postgres_proto::bounded::BoundedU8<32> = bounded_u8!(32, 5);
/// assert_eq!(five.get(), 5);
/// ```
///
/// ```compile_fail
/// use bsql_postgres_proto::bounded_u8;
/// let too_big: bsql_postgres_proto::bounded::BoundedU8<32> = bounded_u8!(32, 33);
/// ```
#[macro_export]
macro_rules! bounded_u8 {
    ($max:literal, $val:literal) => {
        $crate::bounded::BoundedU8::<$max>::new_const::<$val>()
    };
}

/// Tier-1 compile-time construction macro for [`BoundedU16`].
///
/// ```
/// use bsql_postgres_proto::bounded_u16;
/// let five: bsql_postgres_proto::bounded::BoundedU16<2048> = bounded_u16!(2048, 5);
/// assert_eq!(five.get(), 5);
/// ```
///
/// ```compile_fail
/// use bsql_postgres_proto::bounded_u16;
/// let too_big: bsql_postgres_proto::bounded::BoundedU16<2048> = bounded_u16!(2048, 2049);
/// ```
#[macro_export]
macro_rules! bounded_u16 {
    ($max:literal, $val:literal) => {
        $crate::bounded::BoundedU16::<$max>::new_const::<$val>()
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn must_bound_u8<const MAX: usize>(value: u8) -> BoundedU8<MAX> {
        match BoundedU8::<MAX>::try_new(value) {
            Some(b) => b,
            None => BoundedU8::<MAX>::ZERO,
        }
    }

    fn must_bound_u16<const MAX: usize>(value: u16) -> BoundedU16<MAX> {
        match BoundedU16::<MAX>::try_new(value) {
            Some(b) => b,
            None => BoundedU16::<MAX>::ZERO,
        }
    }

    // ─── BoundedU8 ─────────────────────────────────────────────────

    #[test]
    fn u8_try_new_within_range() {
        for value in 0..=32u8 {
            let bounded = BoundedU8::<32>::try_new(value);
            assert!(bounded.is_some());
            assert_eq!(must_bound_u8::<32>(value).get(), value);
        }
    }

    #[test]
    fn u8_try_new_above_range_rejects() {
        for value in 33..=255u8 {
            assert!(BoundedU8::<32>::try_new(value).is_none());
        }
    }

    #[test]
    fn u8_zero_const_get_returns_zero() {
        assert_eq!(BoundedU8::<32>::ZERO.get(), 0);
    }

    #[test]
    fn u8_new_const_round_trips() {
        const V0: BoundedU8<32> = BoundedU8::<32>::new_const::<0>();
        const V32: BoundedU8<32> = BoundedU8::<32>::new_const::<32>();
        assert_eq!(V0.get(), 0);
        assert_eq!(V32.get(), 32);
    }

    #[test]
    fn u8_bounded_len_trait() {
        let v = <BoundedU8<32> as BoundedLen<32>>::try_new_usize(5);
        let v = match v {
            Some(b) => b,
            None => BoundedU8::<32>::ZERO,
        };
        assert_eq!(<BoundedU8<32> as BoundedLen<32>>::get_usize(v), 5);
        assert_eq!(<BoundedU8<32> as BoundedLen<32>>::MAX, 32);
        assert!(<BoundedU8<32> as BoundedLen<32>>::try_new_usize(33).is_none());
    }

    #[test]
    fn u8_macro_round_trip() {
        let v: BoundedU8<32> = crate::bounded_u8!(32, 5);
        assert_eq!(v.get(), 5);
    }

    // ─── BoundedU16 ────────────────────────────────────────────────

    #[test]
    fn u16_try_new_within_range() {
        for value in [0u16, 1, 100, 1024, 2048].iter().copied() {
            let bounded = BoundedU16::<2048>::try_new(value);
            assert!(bounded.is_some());
            assert_eq!(must_bound_u16::<2048>(value).get(), value);
        }
    }

    #[test]
    fn u16_try_new_above_range_rejects() {
        for value in [2049u16, 5000, 32_768, 65_534].iter().copied() {
            assert!(BoundedU16::<2048>::try_new(value).is_none());
        }
    }

    #[test]
    fn u16_zero_const_get_returns_zero() {
        assert_eq!(BoundedU16::<2048>::ZERO.get(), 0);
    }

    #[test]
    fn u16_new_const_round_trips() {
        const V0: BoundedU16<2048> = BoundedU16::<2048>::new_const::<0>();
        const V2048: BoundedU16<2048> = BoundedU16::<2048>::new_const::<2048>();
        assert_eq!(V0.get(), 0);
        assert_eq!(V2048.get(), 2048);
    }

    #[test]
    fn u16_bounded_len_trait() {
        let v = <BoundedU16<2048> as BoundedLen<2048>>::try_new_usize(1024);
        let v = match v {
            Some(b) => b,
            None => BoundedU16::<2048>::ZERO,
        };
        assert_eq!(<BoundedU16<2048> as BoundedLen<2048>>::get_usize(v), 1024);
        assert_eq!(<BoundedU16<2048> as BoundedLen<2048>>::MAX, 2048);
        assert!(<BoundedU16<2048> as BoundedLen<2048>>::try_new_usize(2049).is_none());
    }

    #[test]
    fn u16_macro_round_trip() {
        let v: BoundedU16<2048> = crate::bounded_u16!(2048, 5);
        assert_eq!(v.get(), 5);
    }
}
