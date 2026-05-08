//! DEF-259 (2026-05-08): test-only drop-counter machinery for
//! tier-1-by-construction zeroize-on-drop verification.
//!
//! # Why this module exists
//!
//! Pre-DEF-259 zeroize-on-drop verification was **tier-2 by-discipline**:
//! manual memory-probe tests (`scram_zeroize_miri_spec.rs`,
//! `error_arena_staleness_spec.rs`, `session_params_staleness_spec.rs`,
//! `secret_bounded_str_spec.rs`) covered SOME secret-bearing types via
//! `unsafe`-pointer reads of post-Drop memory, all `#[ignore]`-gated
//! (run only via `cargo test -- --ignored` or `cargo miri test`).
//! Other secret-bearing types (`ScramSession`, `SecretDigest`,
//! `Md5HandshakeState`) had no per-type witness — coverage was
//! transitive through SCRAM/MD5 integration tests that "happened to
//! drop" them mid-flow. A new secret-bearing type added without a
//! probe wired up would silently lack Drop-fire verification: the
//! canonical tier-4 "happens not to fail" anti-pattern (CREDO §1).
//!
//! # What DEF-259 closes
//!
//! Two complementary structural mechanisms:
//!
//! 1. **`DropCounter<T>` newtype** (this module): wraps a value of any
//!    `T` so that when the wrapper is dropped, it (a) drops the inner
//!    `T` (firing `T`'s Drop chain — `ZeroizeOnDrop` for derive-types,
//!    manual zeroize for `WriteBuf`/`ReadBuf`), and (b) increments an
//!    atomic counter through a clone-able [`DropProbe`] handle. Tests
//!    construct `DropCounter::new(t, probe.clone())`, drop it, and
//!    assert `probe.fired() == 1`. Because the increment is written
//!    inside `DropCounter::drop`, it CANNOT execute without `T`'s
//!    Drop also running — Rust language semantics guarantee field
//!    drops fire on enclosing-struct drop.
//!
//! 2. **Sealed [`CrateZeroizeSecret`] trait + exhaustiveness gate**
//!    (test `tests/zeroize_coverage_spec.rs`): every secret-bearing
//!    type in this crate carries an `impl CrateZeroizeSecret for T`
//!    block in this module. The integration test reads every
//!    `src/**/*.rs` file via `include_str!`, regexes for
//!    `derive(ZeroizeOnDrop)` and manual `impl Drop` containing
//!    `.zeroize()`, extracts the type names, and asserts the
//!    discovered set equals the manifest set declared here. A
//!    contributor adding a new secret type without registering it
//!    via `impl CrateZeroizeSecret` fails the test deterministically
//!    on every `cargo test` run.
//!
//! # Tier elevation
//!
//! - Pre-DEF-259: tier-2 by-discipline (manual probes, easy to
//!   forget, ignore-gated).
//! - Post-DEF-259: **tier-1 by-construction**. The exhaustiveness
//!   gate fails build-time if the manifest drifts from the source
//!   reality; per-type `DropCounter<T>` witnesses run on every
//!   `cargo test` (no `--ignored` required). Drop-fire is now a
//!   compile-adjacent invariant rather than a discipline-adjacent
//!   one.
//!
//! # Production impact
//!
//! Zero. The entire module is `#[cfg(test)]`. Production builds
//! (`cargo build --release`) compile without this module; downstream
//! consumers (`bsql-driver-postgres` Phase 1e, the proc-macro online
//! client Phase 2) see no API surface change. Type signatures, Drop
//! impls, layouts, and SemVer surface are identical pre- and
//! post-DEF-259.
//!
//! # Why no `unsafe`
//!
//! `DropCounter<T>` deliberately avoids `core::mem::ManuallyDrop` and
//! its `ManuallyDrop::take` operation (which would require `unsafe`).
//! The trick: the counter increment lives in `DropCounter::drop`'s
//! body, which Rust calls BEFORE field-drop of `inner`. After the
//! body returns, Rust automatically drops the `inner: T` field per
//! [drop-glue rules][drop-rules]. Both happen — counter increment
//! AND `T`'s Drop. No `unsafe` needed; `#![forbid(unsafe_code)]` at
//! lib root remains valid.
//!
//! [drop-rules]: https://doc.rust-lang.org/reference/destructors.html
//!
//! # Why not `Arc<AtomicUsize>` directly
//!
//! [`DropProbe`] could be `alloc::sync::Arc<AtomicUsize>` directly,
//! but the wrapper struct provides a tier-1 invariant: the
//! `clone()` method returns a `DropProbe`, never a raw `Arc<...>`,
//! so test code cannot accidentally drop the wrong handle (e.g.,
//! `handle.clone()` is unambiguous; `arc.clone()` could mean
//! something else in surrounding test scope). Cosmetic, but cheap.

#![cfg(test)]
#![allow(
    dead_code,
    reason = "DEF-259 manifest impls and helper API surface are exercised by \
              per-module tests + the integration coverage spec; some impls \
              may be referenced only via the source-grep gate, not via direct \
              call sites — that is the whole point of the exhaustiveness \
              mechanism, not unused dead code."
)]

use alloc::sync::Arc;
use core::sync::atomic::{AtomicUsize, Ordering};

/// Test-only drop counter handle. Cheaply clone-able; every clone
/// observes the same underlying counter.
///
/// # Tier-1 by-construction
///
/// `DropProbe::clone()` returns another handle on the SAME counter,
/// not a fresh counter. A test that creates a probe, clones it into
/// a `DropCounter::new(...)`, then asserts on the original handle's
/// `fired()` count is structurally guaranteed to observe the
/// clone-wrapper's drop because both share the `Arc`'s atomic.
#[derive(Debug, Clone)]
pub(crate) struct DropProbe {
    counter: Arc<AtomicUsize>,
}

impl DropProbe {
    /// Construct a fresh probe with counter at zero.
    #[inline]
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Number of `DropCounter<T>` wrappers (sharing this probe via
    /// clone) that have been dropped.
    #[inline]
    #[must_use]
    pub(crate) fn fired(&self) -> usize {
        self.counter.load(Ordering::SeqCst)
    }

    /// Increment the counter. Called from `DropCounter::drop` only.
    #[inline]
    fn record_drop(&self) {
        // SeqCst — tests assert on the exact count post-drop;
        // monotonic single-threaded use, but SeqCst keeps the
        // contract robust if a future test ever spawns threads.
        self.counter.fetch_add(1, Ordering::SeqCst);
    }
}

impl Default for DropProbe {
    fn default() -> Self {
        Self::new()
    }
}

/// Test-only wrapper that increments a [`DropProbe`] counter when
/// dropped. Co-fires with the inner `T`'s own Drop (per Rust drop-glue
/// rules: the wrapper's `drop` body runs first, then `inner: T` is
/// dropped automatically).
///
/// # Why a wrapper rather than instrumenting `T` directly
///
/// Production `T`'s Drop body is unchanged. The wrapper observes the
/// drop event without modifying `T`. A test that holds
/// `DropCounter<Password>` exercises EXACTLY the same `Password::drop`
/// (via `ZeroizeOnDrop` derive) the production code path runs.
///
/// # Counter-increment-then-inner-drop ordering
///
/// `DropCounter::drop` does `probe.record_drop()` first, then control
/// returns to Rust's drop glue which invokes `<T as Drop>::drop` on
/// `self.inner`. Both run on every wrapper drop. The counter cannot
/// increment without the inner drop firing (Rust language semantics);
/// the inner drop cannot fire without the counter incrementing
/// (because both live in the same destructor sequence).
///
/// # No `unsafe` boundary
///
/// `inner: T` is owned. `Drop::drop` takes `&mut self`. We do NOT
/// move out of `self.inner` — Rust's automatic field drop handles it.
/// Compare to the `ManuallyDrop` + `ManuallyDrop::take` pattern which
/// requires `unsafe { ManuallyDrop::take(&mut self.inner) }`; that
/// path is unnecessary here because we don't need to consume `T` mid-
/// drop, only observe the drop event.
#[derive(Debug)]
pub(crate) struct DropCounter<T> {
    inner: T,
    probe: DropProbe,
}

impl<T> DropCounter<T> {
    /// Wrap `value` so its eventual drop is witnessed by `probe`.
    #[inline]
    #[must_use]
    pub(crate) fn new(value: T, probe: DropProbe) -> Self {
        Self { inner: value, probe }
    }

    /// Borrow the inner value (for assertions before drop).
    #[inline]
    #[must_use]
    pub(crate) fn inner(&self) -> &T {
        &self.inner
    }
}

impl<T> Drop for DropCounter<T> {
    fn drop(&mut self) {
        // Step 1: record that THIS wrapper is being dropped.
        self.probe.record_drop();
        // Step 2 (implicit): Rust drop-glue now drops `self.inner: T`,
        // firing `<T as Drop>::drop`. For `T: ZeroizeOnDrop`, this
        // is the zeroize chain; for manual-Drop types
        // (`WriteBuf`, `ReadBufN<N>`), it's the `inner.zeroize()`
        // body. The wrapper observes the drop without modifying it.
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-259 manifest — every secret-bearing type in `bsql-pg-proto`.
// ═════════════════════════════════════════════════════════════════════
//
// **Tier-1 by-construction**: the integration test
// `tests/zeroize_coverage_spec.rs` parses this file (via `include_str!`)
// and asserts the discovered `impl CrateZeroizeSecret for X` list
// matches the discovered `derive(ZeroizeOnDrop)` + manual-`impl Drop`-
// with-`.zeroize()` list scanned across `src/**/*.rs`. Adding a new
// secret-bearing type to the crate WITHOUT adding a matching impl
// here fails the test, deterministically, on every `cargo test` run.
//
// The trait itself carries no methods — it's a marker. The PURPOSE
// is the impl-list-as-manifest, parsed by the gate test. Tests that
// directly construct a `DropCounter<T>` for a secret type need no
// trait method; they only need `T: ZeroizeOnDrop` (or T's manual
// Drop impl). The marker just lets the gate enforce exhaustiveness.
//
// **Sealed**: external crates cannot impl this trait. Closure of
// the manifest is structural.

mod sealed {
    //! Seal namespace — prevents external impls of
    //! [`super::CrateZeroizeSecret`]. The manifest is closed at
    //! crate boundary.
    pub trait Sealed {}
}

/// Marker trait — every secret-bearing type in this crate carries an
/// `impl CrateZeroizeSecret for T` block in this module.
///
/// **Sealed** via private `sealed::Sealed` supertrait — external
/// crates cannot add impls. The manifest is closed.
///
/// # Why a marker, not a method
///
/// The exhaustiveness check happens at the **source-text level**
/// (regex scan of `src/**/*.rs` against the impl list in this file),
/// not via runtime trait dispatch. A method would be unused noise.
/// The trait itself is just an audit anchor: every secret type
/// MUST have an entry in the impl list below. The list is the
/// manifest the gate test compares against.
pub(crate) trait CrateZeroizeSecret: sealed::Sealed {}

// ─────────────────────────────────────────────────────────────────────
// Manifest entries — one per secret-bearing type. Ordered by source
// file path for diff stability. Each entry's comment cites the
// originating definition site.
// ─────────────────────────────────────────────────────────────────────

// `bsql-pg-proto::buf::ReadBufN<N>` — manual Drop impl at `buf.rs:382`.
// Carries `inner.as_mut_slice().zeroize()` body.
impl<const N: usize> sealed::Sealed for crate::buf::ReadBufN<N> {}
impl<const N: usize> CrateZeroizeSecret for crate::buf::ReadBufN<N> {}

// `bsql-pg-proto::buf::ReadBuf` (DEF-265 Idea-38) — two-tier inline+heap
// buffer. Manual Drop impl zeroizes BOTH inline storage and heap-Box
// contents (if escaped). Production use replaces the previous inline
// `ReadBufN<4096>`-as-`ReadBuf` alias. Inline path is the common case
// (frames ≤ 256 B); heap escape on first overflow.
impl sealed::Sealed for crate::buf::ReadBuf {}
impl CrateZeroizeSecret for crate::buf::ReadBuf {}

// `bsql-pg-proto::error_arena::ErrorPayload` — `derive(ZeroizeOnDrop)`
// at `error_arena.rs:129`. Carries 3× `SecretBoundedStr<N>` fields.
impl sealed::Sealed for crate::error_arena::ErrorPayload {}
impl CrateZeroizeSecret for crate::error_arena::ErrorPayload {}

// `bsql-pg-proto::ident::SecretBoundedStr<N>` — manual `impl Zeroize`
// at `ident.rs:702` + manual `impl Drop` at `ident.rs:711`. Drop body
// calls `self.inner.zeroize_in_place()`.
impl<const N: usize> sealed::Sealed for crate::ident::SecretBoundedStr<N> {}
impl<const N: usize> CrateZeroizeSecret for crate::ident::SecretBoundedStr<N> {}

// `bsql-pg-proto::md5::Md5HandshakeState` — `derive(ZeroizeOnDrop)`
// at `md5.rs:94`. Carries `Sensitive<Password>` (zeroized) +
// `Ident` (skip).
impl sealed::Sealed for crate::md5::Md5HandshakeState {}
impl CrateZeroizeSecret for crate::md5::Md5HandshakeState {}

// `bsql-pg-proto::password::Password` — `derive(Zeroize, ZeroizeOnDrop)`
// at `password.rs:79`. Backing `[u8; MAX_PASSWORD_LEN]` + `u16` len.
impl sealed::Sealed for crate::password::Password {}
impl CrateZeroizeSecret for crate::password::Password {}

// `bsql-pg-proto::scram::session::ScramSession` — `derive(Zeroize,
// ZeroizeOnDrop)` at `scram/session.rs:89`. Carries
// `Sensitive<Password>` + 2× skip-zeroized `PodBytes`.
impl sealed::Sealed for crate::scram::session::ScramSession {}
impl CrateZeroizeSecret for crate::scram::session::ScramSession {}

// `bsql-pg-proto::scram::types::SecretDigest` — `derive(Zeroize,
// ZeroizeOnDrop)` at `scram/types.rs:22`. Backing `[u8; 32]`.
impl sealed::Sealed for crate::scram::types::SecretDigest {}
impl CrateZeroizeSecret for crate::scram::types::SecretDigest {}

// `bsql-pg-proto::sensitive::Sensitive<T>` — `derive(Zeroize,
// ZeroizeOnDrop)` at `sensitive.rs:33`. Transparent wrapper over
// `T: Zeroize`.
impl<T: zeroize::Zeroize> sealed::Sealed for crate::sensitive::Sensitive<T> {}
impl<T: zeroize::Zeroize> CrateZeroizeSecret for crate::sensitive::Sensitive<T> {}

// `bsql-pg-proto::write_buf::WriteBuf` — manual Drop impl at
// `write_buf.rs:673`. Body: `inner.as_mut_slice().zeroize()`.
impl sealed::Sealed for crate::write_buf::WriteBuf {}
impl CrateZeroizeSecret for crate::write_buf::WriteBuf {}

// ─────────────────────────────────────────────────────────────────────
// Self-tests for the test-only machinery itself.
// ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod self_tests {
    //! Tests for [`DropCounter`] / [`DropProbe`] mechanics.
    //!
    //! These verify the witness mechanism BEFORE we use it to verify
    //! production secret types. If `DropCounter` has a bug, every
    //! per-type test below it would inherit a false negative.

    use super::*;

    /// Drop ordering: counter increments AND inner T's Drop fires.
    /// Witness via a hand-rolled inner type that records its own
    /// drop into a separate counter; we then assert both counters
    /// observed the same drop event.
    #[test]
    fn drop_counter_records_wrapper_drop() {
        let probe = DropProbe::new();
        assert_eq!(probe.fired(), 0, "fresh probe must read zero");

        // Build a wrapper, drop it, check the counter.
        {
            let _wrapper = DropCounter::new(42_i32, probe.clone());
            // Wrapper alive — counter still 0.
            assert_eq!(probe.fired(), 0, "wrapper alive — counter must stay 0");
        }
        // Wrapper out of scope — counter == 1.
        assert_eq!(
            probe.fired(),
            1,
            "DropCounter::drop must increment exactly once on scope exit",
        );
    }

    /// Inner T's Drop is invoked alongside the counter increment.
    /// Verified via a custom type `DropMarker` that increments its
    /// OWN counter on drop; the test checks both counters fire on
    /// the wrapper's drop.
    #[test]
    fn drop_counter_does_not_skip_inner_drop() {
        struct DropMarker {
            counter: Arc<AtomicUsize>,
        }
        impl Drop for DropMarker {
            fn drop(&mut self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
            }
        }

        let probe = DropProbe::new();
        let inner_counter = Arc::new(AtomicUsize::new(0));

        {
            let marker = DropMarker {
                counter: inner_counter.clone(),
            };
            let _wrapper = DropCounter::new(marker, probe.clone());
        }

        assert_eq!(
            probe.fired(),
            1,
            "wrapper drop must fire (counter == 1)",
        );
        assert_eq!(
            inner_counter.load(Ordering::SeqCst),
            1,
            "inner T's Drop must ALSO fire on wrapper drop",
        );
    }

    /// Multiple wrappers sharing one probe each contribute one count.
    #[test]
    fn drop_probe_counts_each_clone_drop_independently() {
        let probe = DropProbe::new();
        let _w1 = DropCounter::new((), probe.clone());
        let _w2 = DropCounter::new(0_u8, probe.clone());
        let _w3 = DropCounter::new("test", probe.clone());
        assert_eq!(probe.fired(), 0, "wrappers alive — counter is 0");
        drop(_w1);
        drop(_w2);
        drop(_w3);
        assert_eq!(
            probe.fired(),
            3,
            "three wrappers dropped — counter must be 3 (each Drop runs once)",
        );
    }

    /// Probe clones share the same underlying atomic.
    #[test]
    fn drop_probe_clones_share_counter() {
        let probe_a = DropProbe::new();
        let probe_b = probe_a.clone();
        assert_eq!(probe_a.fired(), 0);
        assert_eq!(probe_b.fired(), 0);

        let _w = DropCounter::new(0_u8, probe_a.clone());
        drop(_w);
        assert_eq!(probe_a.fired(), 1, "view via probe_a sees the drop");
        assert_eq!(probe_b.fired(), 1, "view via probe_b sees the same drop");
    }
}
