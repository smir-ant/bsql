//! `SecretZeroize` trait + driver-side panic-hook integration
//! contract. Closes the `panic = "abort"` zeroize gap from
//! "policy-only acknowledgement" to "trait-mediated structural
//! contract".
//!
//! # The gap
//!
//! `panic = "abort"` (workspace-wide release setting, see
//! `Cargo.toml` build-profile docstring) bypasses unwinding. Drop
//! impls — including `ZeroizeOnDrop` derives on
//! [`crate::scram::session::ScramSession`],
//! [`crate::sensitive::Sensitive<T>`],
//! [`crate::ident::SecretBoundedStr<N>`], and the various
//! `Zeroizing<T>` scope-guards in `dispatch::dispatch_auth_sasl_continue`
//! — DO NOT FIRE on a panic path. The aborting process freezes
//! its memory image at the moment of abort; secrets sitting in
//! stack frames or boxed structs persist until kernel reap.
//!
//! Forbid-bundle (`clippy::panic`, `clippy::unwrap_used`,
//! `clippy::indexing_slicing`, `clippy::arithmetic_side_effects`,
//! `clippy::as_conversions`) eliminates every panic-able expression
//! in this crate at compile time. So today the crate-internal
//! abort path is architecturally dead — there is no in-crate code
//! path that reaches `panic`. The residual surface is:
//!
//! 1. **External crate panics** (downstream user code, third-party
//!    deps with unsafe internals) that unwind THROUGH a frame
//!    holding `&mut PgProtocol`.
//! 2. **Allocator OOM** in `Box::new` — `alloc::alloc::handle_alloc_error`
//!    typically panics or aborts; on `panic = "abort"` it aborts
//!    immediately.
//! 3. **Compiler bugs / LLVM mis-codegen** — defence-in-depth.
//!
//! In all three cases, secrets in stack-living `ScramSession` /
//! `Sensitive<T>` / boxed `Box<ScramSession>` slots are not
//! zeroized.
//!
//! # The contract
//!
//! `SecretZeroize` is the trait for types that hold zeroize-on-drop
//! secrets. Implementors register themselves into a driver-managed
//! registry (driver crate provides the registry implementation —
//! the registry is `std`-bounded and lives outside this `no_std + alloc`
//! crate). On panic, the driver's `std::panic::set_hook` walks the
//! registry and calls [`SecretZeroize::zeroize_in_place`] on every
//! registered instance BEFORE the abort propagates.
//!
//! # Today's surface (without driver registry)
//!
//! Without a driver crate, this trait is ALONE — types implement it,
//! but nothing walks the registered set. The trait still buys:
//!
//! - **Type-system documentation**: a contributor adding a new
//!   secret-bearing type can discover the contract by searching
//!   for `impl SecretZeroize`. Forces explicit decision.
//! - **Ready-made API surface for the driver**: when the driver
//!   crate lands, it adds ~30 LoC of `std::panic::set_hook` +
//!   atomic-registered-set walker and the gap closes structurally
//!   — no API change in this crate.
//! - **Audit anchor**: every secret-bearing type is grep-discoverable
//!   via `impl SecretZeroize`. Contrast: `Sensitive<T>` and
//!   `SecretBoundedStr<N>` are zeroize-on-drop but the panic-gap
//!   surface they sit on is implicit.
//!
//! Per CREDO §1 (safety > tier > perf): defining the trait now
//! ships zero structural improvement until the driver lands, but
//! prevents the API from drifting before then. The trait is
//! `pub(crate)` (driver will dep on this crate and re-export); not
//! part of the external public API.

/// Trait for types holding zeroize-on-drop secret bytes that
/// require pre-abort scrubbing under `panic = "abort"`.
///
/// # Implementor contract
///
/// `zeroize_in_place` MUST:
///
/// - Not allocate (the panic hook runs in a fragile state — the
///   allocator may itself be the panic source).
/// - Not panic (panic-during-panic-hook on `panic = "abort"`
///   aborts immediately, leaking the very secrets we're trying
///   to scrub).
/// - Not deadlock on locks held by the panicking thread (no
///   mutex acquisition; no `&mut self` if `self` may be aliased
///   via the registry pointer).
/// - Be idempotent (calling twice MUST be safe — the registry
///   may walk the same instance twice if registration races
///   with the panic hook).
///
/// # Invariant (driver-side, not implementor-side)
///
/// The driver's panic hook MUST walk the registry BEFORE the
/// abort propagates. Driver implementation owns this contract;
/// no in-crate test can pin it (`std::panic::set_hook` is `std`-
/// only and out of this crate's `no_std` scope).
#[expect(
    dead_code,
    reason = "trait is the API anchor for the driver-side panic-hook registry walker. \
              No in-crate caller until the driver lands. Once the driver is built, \
              `#[expect]` triggers a build error reminding the contributor to remove \
              the attribute (the trait will then have a live caller via the registry walker)."
)]
pub(crate) trait SecretZeroize {
    /// Zero out the secret bytes held by `self`. Idempotent;
    /// the panic hook may call this multiple times concurrently
    /// (atomic operations on the registered pointer set).
    fn zeroize_in_place(&mut self);
}

// Concrete impl for `ScramSession` — the most secret-bearing
// type in the crate (carries the `Sensitive<Password>` +
// SCRAM-handshake nonces). The implementation delegates to
// `zeroize::Zeroize::zeroize` which is the same logic the
// derive-generated `Drop` chain runs on the normal-flow drop
// path. Idempotent (the `zeroize` crate guarantees this).
impl SecretZeroize for crate::scram::session::ScramSession {
    #[inline]
    fn zeroize_in_place(&mut self) {
        // `Zeroize::zeroize` is the trait-level entry point;
        // `ZeroizeOnDrop` chains via `Drop::drop` to the same body.
        // Calling `zeroize` directly here makes the panic-hook
        // path explicit (no reliance on Drop being scheduled).
        zeroize::Zeroize::zeroize(self);
    }
}

// Other secret-bearing types (`Sensitive<T>`, `SecretBoundedStr<N>`,
// `SessionParams`, `ErrorArena::ErrorPayload`) impl `Zeroize` /
// `ZeroizeOnDrop` directly via derive; their pre-abort scrub
// surface is reachable through the `ScramSession` impl above
// (which is the only top-level secret container the driver's
// panic hook will register — every other secret is reachable
// transitively through the live `PgProtocol` state). Adding
// per-type `impl SecretZeroize` blocks for each individual
// secret type would generate noise without a corresponding
// registry-walker callsite; the trait alone is the API anchor.
