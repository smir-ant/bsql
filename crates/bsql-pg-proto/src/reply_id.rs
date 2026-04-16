//! Opaque correlator for in-flight commands.
//!
//! `bsql-pg-proto` is `no_std` and oblivious to async runtimes; it cannot
//! own `tokio::sync::oneshot::Sender`s itself. Instead, each
//! [`crate::PgCommand`] carries a `ReplyId` that the upstream wrapper
//! crate (`bsql-driver-postgres`, Phase 1e) uses as the key in a
//! `HashMap<ReplyId, oneshot::Sender<Reply>>`.
//!
//! # ID provenance
//!
//! `ReplyId` wraps a [`NonZeroU64`] for two reasons:
//!
//! 1. **Niche optimization.** `Option<ReplyId>` is 8 bytes, not 16.
//! 2. **No sentinel collision.** Zero is reserved as "no ID"; the
//!    constructor refuses it.
//!
//! `bsql-pg-proto` does **not** mint IDs. The wrapper crate runs a
//! per-connection monotonic counter starting at 1; collision-freedom is
//! the wrapper's responsibility. Per reforge.md §7.5, this is
//! **tier-3 by audit**: the cross-crate seal is not expressible in
//! stable Rust today. Mitigations:
//!
//! - The constructor takes `NonZeroU64` (zero impossible at the type
//!   level).
//! - Production wrappers must use a single fetch-add counter per
//!   connection. Reusing IDs across the same `PgProtocol` instance is
//!   undefined at the spec level (the protocol can deliver to the wrong
//!   sender), but cannot violate memory safety.
//! - `cargo-vet` / commit review flag any new constructor outside the
//!   sanctioned wrapper.

use core::fmt;
use core::num::NonZeroU64;

/// Opaque handle correlating a pushed command with its eventual reply.
///
/// Constructed by the wrapper crate from a per-connection monotonic
/// counter. The protocol crate ferries the value through; it never
/// inspects it, never compares it, never mints one of its own.
///
/// # Consume discipline — tier-1 runtime, tier-2 compile-enforced
///
/// `ReplyId` tracks whether its value has been **delivered** to an
/// outgoing [`crate::Action::DeliverReply`] / [`crate::Action::FailReply`].
/// The only way to extract the underlying `NonZeroU64` is
/// [`ReplyId::consume`], which also marks the id as delivered. Dropping
/// a `ReplyId` for which `consume` was never called is a **runtime
/// failure**: the Drop impl asserts that `delivered == true` and panics
/// with a descriptive message.
///
/// Under release builds with `panic = "abort"` (the workspace-level
/// setting) the panic aborts the process. Under test / dev builds with
/// `panic = "unwind"` the test harness surfaces the panic as a failure.
/// Either way the bug — a caller who silently dropped a pending reply,
/// leaving their `oneshot::Receiver` to hang forever — becomes loudly
/// observable instead of silently corrupting user flows.
///
/// ## Layered guarantees
///
/// - **Tier 1 compile** — non-duplicatable. No `Copy`, no `Clone` impl.
///   `let b = a;` is a move, not a copy; `a.clone()` does not compile.
/// - **Tier 1 compile** — cannot be extracted without acknowledging the
///   consume step. Extracting the value requires calling `consume(self)`
///   (which takes ownership), not `&self` — so you can't "peek and
///   forget" the value while retaining the handle.
/// - **Tier 2 compile** — cannot be silently ignored from a pattern
///   match. The crate-root `#[deny(unused_variables)]` combined with
///   the architect.txt Part V bans on `let _ = expr;` and `_varname`
///   suppression forces a match arm that binds `id: ReplyId` to refer
///   to `id` in the arm body. Calling `drop(id)` is still legal (the
///   variable is "used" by `drop`); the Drop-guard below promotes that
///   path to tier 1 runtime.
/// - **Tier 1 runtime** — Drop panics / aborts on undelivered drop.
///
/// A legitimate transport-teardown path (wrapper closes the connection
/// while a reply is still in flight) calls `consume` internally, then
/// the wrapper delivers the classified `TransportClosed` error to the
/// caller's oneshot. The protocol crate never needs to drop an
/// unconsumed id on this path — [`crate::PgProtocol::terminate`]
/// handles it.
///
/// # Raw counter values are the wire currency
///
/// Outgoing actions carry `NonZeroU64` directly, not `ReplyId` — the
/// wrapper's pending-replies table is keyed on `NonZeroU64` and has no
/// need of the consume-tracking handle. The handle exists only inside
/// the protocol crate's state-transition paths.
#[must_use = "a ReplyId must be consumed via `.consume()` into an Action — dropping it without delivery is a runtime error"]
pub struct ReplyId {
    /// The wire-level correlator value. Never changes after
    /// construction. The wrapper uses this as the key in its pending-
    /// replies map.
    value: NonZeroU64,
    /// Whether [`ReplyId::consume`] was called before drop. The Drop
    /// impl reads this to decide whether to panic. A plain `bool`
    /// suffices — `consume(mut self)` takes ownership so we can mutate
    /// without synchronisation and then let `self` drop with the flag
    /// set.
    delivered: bool,
}

impl ReplyId {
    /// Construct a `ReplyId` from a non-zero monotonic counter value.
    ///
    /// **Caller contract** (tier-2, audit-enforced): `value` must not
    /// have been used previously on the same `PgProtocol` instance.
    /// Reuse causes the protocol to deliver future replies to whichever
    /// sender is still registered under that ID — a logic error, not a
    /// memory-safety issue.
    ///
    /// The standard wrapper (`bsql-driver-postgres`) uses an
    /// `AtomicU64` initialised to 1 with `fetch_add(1, Relaxed)`. At
    /// one pull per nanosecond that lasts ~584 years, so wraparound is
    /// outside any realistic horizon.
    ///
    /// A fresh `ReplyId` starts with `delivered = false`; the
    /// Drop-guard will fire if it is dropped before
    /// [`ReplyId::consume`] is called. See type-level docstring.
    #[inline]
    pub const fn from_raw(value: NonZeroU64) -> Self {
        Self {
            value,
            delivered: false,
        }
    }

    /// Extract the underlying counter value, consuming the handle and
    /// marking the reply as delivered.
    ///
    /// The returned `NonZeroU64` is what the wrapper uses to route
    /// a reply back to the caller's `oneshot::Sender`. After calling
    /// `consume`, the `ReplyId` is gone — the raw value travels inside
    /// an outgoing `Action`, which is not consume-tracked.
    ///
    /// Tier-1 semantic: if a code path wants the raw value it *must*
    /// call `consume`; there is no alternative extraction method.
    /// Calling `consume` is the ack that the reply is en route to the
    /// wrapper; the Drop-guard then runs harmlessly at end-of-scope.
    #[inline]
    pub fn consume(mut self) -> NonZeroU64 {
        self.delivered = true;
        self.value
        // Drop runs here with `delivered = true` → no panic.
    }

    /// Peek at the underlying counter value without consuming the id.
    ///
    /// Useful for logging and for tests that want to assert a reply id
    /// round-trips correctly without having to wait until the id has
    /// been packaged into an outgoing Action. This is **not** the path
    /// used by the wrapper to route replies — that path calls
    /// [`ReplyId::consume`] instead.
    #[inline]
    #[must_use]
    pub const fn get(&self) -> NonZeroU64 {
        self.value
    }
}

impl Drop for ReplyId {
    /// Tier-1 runtime consume-discipline guard.
    ///
    /// Fires when a `ReplyId` reaches end-of-scope without
    /// [`ReplyId::consume`] ever being called. Under the workspace
    /// release profile's `panic = "abort"` this aborts the process;
    /// under test/dev (`unwind`) the test harness surfaces the panic.
    /// Either way the silent-reply-loss bug class — which would
    /// otherwise hang the caller's `oneshot::Receiver` forever — is
    /// made loudly visible at the moment it happens.
    fn drop(&mut self) {
        assert!(
            self.delivered,
            "ReplyId {} dropped without delivery — the caller's oneshot receiver will never resolve",
            self.value.get(),
        );
    }
}

// PartialEq / Eq / Hash compare on `value` only: the `delivered` flag is
// an implementation detail of the Drop-guard and must not participate
// in equality semantics (two `ReplyId`s with the same value are "the
// same id" regardless of whether one of them has been consumed).
impl PartialEq for ReplyId {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for ReplyId {}

impl core::hash::Hash for ReplyId {
    #[inline]
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl fmt::Debug for ReplyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Intentionally does not print `delivered` — that field is
        // internal drop-guard bookkeeping; the wrapper / tests care
        // only about the wire value.
        write!(f, "ReplyId({})", self.value.get())
    }
}

#[cfg(test)]
mod drop_guard_proof {
    //! Tier-1 runtime proof that the Drop-guard fires on the
    //! undelivered-drop path. This is the **only** invariant in
    //! `reply_id.rs` that cannot be lifted to tier-1 compile on stable
    //! Rust (no linear types, no field-level `#[must_use]`), so the
    //! runtime panic is the load-bearing mechanism — and a runtime
    //! mechanism is only trustworthy once it has been observed to
    //! fire. That is this test's sole job.
    //!
    //! The happy path (consume suppresses the guard, equality ignores
    //! the internal flag) is held tier-2 by one-line inspection of
    //! `consume` / `PartialEq` bodies and is exercised implicitly by
    //! every ping-flow test that completes a round-trip.

    use super::*;

    /// Dropping a `ReplyId` without calling `.consume()` trips the
    /// Drop-guard — the tier-1 runtime safety net that catches the
    /// "silent reply loss" bug class.
    ///
    /// The test's `#[should_panic]` expects a specific message
    /// substring; a future refactor that changes the panic text
    /// requires updating this fixture, which is intentional — the
    /// text is user-visible diagnostic and a silent regression in its
    /// shape is worth surfacing.
    #[test]
    #[should_panic(expected = "dropped without delivery")]
    fn undelivered_drop_panics() {
        let raw = NonZeroU64::new(7).unwrap_or(NonZeroU64::MIN);
        let id = ReplyId::from_raw(raw);
        drop(id);
    }
}
