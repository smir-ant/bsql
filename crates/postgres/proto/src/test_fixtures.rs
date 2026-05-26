//! Shared test-fixture helpers — loud-fail narrowing for fixture-built
//! wire bodies. Test-only module (gated by `#[cfg(test)]` at the crate
//! root); contributes no surface to production builds.
//!
//! # Rationale
//!
//! Crate-wide tests construct PostgreSQL wire frames in `alloc::Vec`
//! buffers and write column counts / payload lengths as fixed-width
//! big-endian integers. The natural narrowing call
//! (`i16::try_from(columns.len()).unwrap_or(0)`) compiles cleanly under
//! the forbid-bundle but introduces a silent test-corruption mode:
//! if a future test mistakenly builds a fixture with `usize` count
//! exceeding `i16::MAX`, the body is written with `count = 0` and the
//! parser-under-test happily decodes an empty row description. The
//! invariant breach is masked.
//!
//! These helpers replace `try_from(...).unwrap_or(fallback)` with an
//! `assert!(n <= LIMIT, "...")` precondition + a same-shape match
//! whose `Err` arm is architecturally dead post-assert. Test invariant
//! breach becomes a loud, `#[track_caller]`-attributed failure instead
//! of a silent fixture corruption.
//!
//! # Forbid-bundle compliance
//!
//! `panic!`, `unwrap()`, `expect()`, `unreachable!()` are all crate-wide
//! `forbid`. The audit-author's original sketch
//! (`unwrap_or_else(|_| panic!(...))`) is therefore not buildable here;
//! the equivalent `assert!(condition, "msg")` form is admitted (the
//! existing `state.rs::nz()` helper uses the same pattern) and provides
//! identical loud-fail semantics — `assert!` expands to `panic!` only
//! under the failure branch, which `clippy::panic` does not flag.
//!
//! The fall-through `unwrap_or(LIMIT)` in each helper is dead under the
//! assert precondition; it survives syntactically so that the
//! function's return type is inhabited (the forbid-bundle bans the
//! shorter `unreachable!()` discharge).

/// `i16::MAX` as `usize` — pinned literal to avoid `as` casts in the
/// bound check. PostgreSQL §55.7 caps RowDescription column count at
/// `i16`; this is the natural test fixture bound.
const I16_MAX_AS_USIZE: usize = 32_767;

/// `i32::MAX` as `usize` — pinned literal for DataRow payload length
/// narrowing. PostgreSQL §55.7 caps DataRow column length at `i32`
/// (with `-1` reserved for SQL NULL).
const I32_MAX_AS_USIZE: usize = 2_147_483_647;

/// Test-fixture narrowing `usize → i16` with loud-fail invariant
/// pinning. Caller passes the natural `usize` count (e.g.
/// `columns.len()`); helper asserts the value fits `i16::MAX` and
/// returns the narrowed result.
///
/// Test invariant breach (count > 32767) panics with a
/// `#[track_caller]`-attributed message; the calling test fails fast
/// with a precise stack location rather than silently writing a
/// corrupt fixture body.
#[track_caller]
pub(crate) fn fixture_i16(n: usize) -> i16 {
    assert!(
        n <= I16_MAX_AS_USIZE,
        "test fixture invariant breach: count {n} > i16::MAX ({I16_MAX_AS_USIZE})",
    );
    // `try_from` is infallible post-assert; the fall-through
    // `unwrap_or(i16::MAX)` is architecturally dead (the assert
    // above fires before this line). The fallback survives
    // syntactically — the forbid-bundle bans `expect`/`unreachable!`
    // discharges that would be shorter but louder.
    i16::try_from(n).unwrap_or(i16::MAX)
}

/// Test-fixture narrowing `usize → i32` with loud-fail invariant
/// pinning. Mirror of [`fixture_i16`] for DataRow per-column payload
/// lengths.
#[track_caller]
pub(crate) fn fixture_i32(n: usize) -> i32 {
    assert!(
        n <= I32_MAX_AS_USIZE,
        "test fixture invariant breach: length {n} > i32::MAX ({I32_MAX_AS_USIZE})",
    );
    i32::try_from(n).unwrap_or(i32::MAX)
}

/// Test-fixture promotion `u64 → NonZeroU64` with loud-fail on zero.
/// Used by reply-id correlator fixtures across the state-machine
/// test suite. Replaces ad-hoc per-module `nz()` helpers that all
/// duplicate the `assert!(n > 0) + NonZeroU64::new(n).unwrap_or(MIN)`
/// idiom; centralising removes the silent `MIN` fallback duplication.
#[track_caller]
pub(crate) fn fixture_nz_u64(n: u64) -> core::num::NonZeroU64 {
    assert!(n > 0, "test fixture invariant breach: nz(0) — use nz(1..) for non-zero correlators");
    // `NonZeroU64::new(n)` is `Some(_)` post-assert (n > 0); the
    // `unwrap_or(MIN)` fall-through is architecturally dead.
    core::num::NonZeroU64::new(n).unwrap_or(core::num::NonZeroU64::MIN)
}

#[cfg(test)]
mod self_tests {
    use super::*;

    #[test]
    fn fixture_i16_within_bound_narrows() {
        assert_eq!(fixture_i16(0), 0);
        assert_eq!(fixture_i16(32_767), 32_767);
    }

    #[test]
    fn fixture_i32_within_bound_narrows() {
        assert_eq!(fixture_i32(0), 0);
        assert_eq!(fixture_i32(2_147_483_647), 2_147_483_647);
    }

    #[test]
    #[should_panic(expected = "fixture invariant breach")]
    fn fixture_i16_overflow_loud_fails() {
        // Discard via `let _ =` — Copy return type (i16) has no
        // `Drop` side effect, so `let_underscore_drop` does not
        // apply; non-`#[must_use]` return so `let_underscore_must_use`
        // does not apply. The `#[should_panic]` harness intercepts
        // the assert panic before any consumer runs.
        let _ = fixture_i16(32_768);
    }

    #[test]
    #[should_panic(expected = "fixture invariant breach")]
    fn fixture_i32_overflow_loud_fails() {
        let _ = fixture_i32(2_147_483_648);
    }

    #[test]
    fn fixture_nz_u64_within_bound_succeeds() {
        assert_eq!(fixture_nz_u64(1).get(), 1);
        assert_eq!(fixture_nz_u64(u64::MAX).get(), u64::MAX);
    }

    #[test]
    #[should_panic(expected = "fixture invariant breach")]
    fn fixture_nz_u64_zero_loud_fails() {
        let _ = fixture_nz_u64(0);
    }
}
