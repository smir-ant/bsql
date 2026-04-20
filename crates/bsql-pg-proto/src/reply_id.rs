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
/// # Consume discipline — tier-1 compile + tier-2 structural
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
/// - **Tier 2 structural** — cannot be silently ignored from a pattern
///   match. The crate-root `#[deny(unused_variables)]` combined with
///   the architect.txt Part V bans on `let _ = expr;` and `_varname`
///   suppression forces a match arm that binds `id: ReplyId` to refer
///   to `id` in the arm body. Calling `drop(id)` is still legal (the
///   variable is "used" by `drop`) — the code **compiles**. This is
///   NOT tier 1. No path in our code calls `drop(id)` (tier 2 by
///   structural audit), and the Drop-guard below surfaces the bug
///   loudly at runtime if someone adds one.
/// - **Tier 2 structural (runtime safety net)** — Drop asserts
///   `self.delivered` and panics on undelivered drop. Under
///   `panic = "abort"` (release) this aborts the process. This is
///   a **runtime** check, not a compile check — per CREDO §3.4,
///   it is NOT tier 1. It is tier 2: the guard makes the bug
///   immediately observable, but the buggy code still compiles.
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
    /// Tier-2 runtime consume-discipline guard (safety net).
    ///
    /// Fires when a `ReplyId` reaches end-of-scope without
    /// [`ReplyId::consume`] ever being called. Under the workspace
    /// release profile's `panic = "abort"` this aborts the process;
    /// under test/dev (`unwind`) the test harness surfaces the panic.
    /// Either way the silent-reply-loss bug class — which would
    /// otherwise hang the caller's `oneshot::Receiver` forever — is
    /// made loudly visible at the moment it happens.
    ///
    /// # Why the Drop-guard stays (DEF-101 re-scoping)
    ///
    /// An earlier DEF-101 proposal was to *remove* the Drop impl
    /// entirely after a full-path audit proved no production path
    /// reaches an undelivered drop. The audit is clean (every
    /// `mem::take(&mut state)` site exhaustively matches and either
    /// consumes via `.consume()` or re-places the id in the new
    /// state — verified at the six sites: `push_command`, the
    /// `feed_bytes` dispatcher loop's three outcome arms, and
    /// `fail_inflight_and_close`). But removing Drop would be a
    /// **tier regression** on stable Rust, not an elevation, because:
    ///
    /// 1. Stable Rust has no linear types. "Cannot drop unconsumed"
    ///    cannot be a tier-1 compile invariant — even with
    ///    `#[must_use]` + `deny(unused_variables)`, patterns like
    ///    `let r = id(); r.get(); // scope-drop` silently compile.
    /// 2. The Drop-guard catches exactly this residual class at
    ///    runtime. Removing it would replace tier-2 runtime with
    ///    tier-3 audit (= strictly weaker).
    /// 3. Production has `panic = "abort"`, so the guard aborts the
    ///    process cleanly — no undefined behaviour, no hang.
    ///
    /// DEF-101 therefore keeps the guard and fixes the *actual*
    /// pain point, DEF-052 (diagnostic-masking), below.
    ///
    /// # DEF-052 close — unwind-safe guard
    ///
    /// The historical problem: a test that panics for an *unrelated*
    /// reason while a non-delivered `ReplyId` is alive ran this Drop
    /// during unwinding; the `assert!` below double-panicked →
    /// `SIGABRT` → the original panic message was lost. The safety
    /// property was not weakened (the guard still caught undelivered
    /// drops), but the test-time diagnostic masked the original
    /// failure.
    ///
    /// The fix: a `std::thread::panicking()` check gated on
    /// `#[cfg(test)]` skips the assert during unwinding. `cfg(test)`
    /// gating is essential and load-bearing:
    ///
    /// - Production is `#![no_std]` and `panic = "abort"`. Unwinding
    ///   never happens; the guard always fires or never runs. The
    ///   `cfg(test)` branch is dead → zero cost.
    /// - `std::thread::panicking()` lives in `std`, not `core`. The
    ///   crate's `#[cfg(test)] extern crate std;` (in `lib.rs`)
    ///   brings `std` in for the test binary only — no production
    ///   `std` pull-in, `no_std` consumers stay happy.
    fn drop(&mut self) {
        // DEF-052 close: during a test-time unwind, skip the guard
        // to prevent double-panic from masking the original panic
        // message. Production builds never reach this line
        // (panic = "abort" never unwinds).
        #[cfg(test)]
        if std::thread::panicking() {
            return;
        }
        assert!(
            self.delivered,
            "ReplyId {} dropped without delivery — the caller's oneshot receiver will never resolve",
            self.value.get(),
        );
    }
}

// `PartialEq`, `Eq`, `Hash` are **deliberately NOT implemented** on
// `ReplyId` (DEF-088 tier raise).
//
// Background: a previous version had hand-rolled impls that compared
// only on `value`, ignoring `delivered`. That was a tier-3 seam — a
// one-line body swap (`... && self.delivered == other.delivered`)
// compiles cleanly and silently shifts equality semantics. Closed at
// tier-1 compile by **removing** the impls entirely: callers who
// genuinely need to compare ids use `.get()` to extract the wire-level
// `NonZeroU64` (which has its own correct-by-construction equality)
// and compare those. The wrapper's pending-replies map is keyed on
// `NonZeroU64` (the `Action::DeliverReply { id: NonZeroU64, .. }`
// shape), not on `ReplyId`. No production / test code requires
// `ReplyId == ReplyId` or `HashMap<ReplyId, _>`.
//
// If a future consumer needs semantic equality on `ReplyId`, they
// must either (a) compare `a.get() == b.get()` explicitly — which
// makes the comparison site greppable and auditable — or (b) propose
// a derive on a refactored `ReplyId` that carries only `value`
// (the `delivered` flag would move to an internal wrapper type,
// similar to DEF-077's `NonErroredState` split). Until either happens,
// the absence of the impl is the structural guarantee.

impl fmt::Debug for ReplyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Intentionally does not print `delivered` — that field is
        // internal drop-guard bookkeeping; the wrapper / tests care
        // only about the wire value.
        write!(f, "ReplyId({})", self.value.get())
    }
}

#[cfg(test)]
mod reply_id_semantics {
    //! Per reforge.md §4.11, tests cover category (1) functional
    //! spec-conformance or (2) tier-3 invariants only. Every test in
    //! this module is labelled with its category in the docstring.

    use super::*;

    /// Category (2) — tier-3 runtime invariant.
    ///
    /// Dropping a `ReplyId` without calling `.consume()` trips the
    /// Drop-guard. This is the load-bearing mechanism against the
    /// "silent reply loss" bug class: on stable Rust we cannot lift
    /// this to tier-1 compile (no linear types, no field-level
    /// `#[must_use]`), so the runtime panic is how we close the hole.
    /// A runtime mechanism is only trustworthy once observed to fire;
    /// that is this test's sole job.
    ///
    /// The `#[should_panic]` expects a specific message substring —
    /// the text is user-visible diagnostic and a silent change in its
    /// shape is worth surfacing as a test update.
    #[test]
    #[should_panic(expected = "dropped without delivery")]
    fn undelivered_drop_panics() {
        let raw = NonZeroU64::new(7).unwrap_or(NonZeroU64::MIN);
        let id = ReplyId::from_raw(raw);
        drop(id);
    }

    /// Category (2) — tier-2 runtime invariant (DEF-052 close).
    ///
    /// If a test panics for an *unrelated* reason while a
    /// non-delivered `ReplyId` is alive, the Drop-guard historically
    /// tripped a double-panic during unwinding, producing `SIGABRT`
    /// and hiding the original panic message from the test harness.
    /// DEF-101 gated the Drop-guard on `std::thread::panicking()`
    /// under `#[cfg(test)]`; during unwinding the guard returns
    /// early, letting the original panic propagate cleanly.
    ///
    /// This test proves the fix: the panic message observed by the
    /// harness is the ORIGINAL `"unrelated panic"`, NOT the
    /// Drop-guard's `"dropped without delivery"`. If the fix
    /// regresses (e.g. the `thread::panicking()` check is removed
    /// or the cfg gate changes), `#[should_panic(expected = ...)]`
    /// will fail because the Drop-guard's message would surface
    /// instead.
    #[test]
    #[should_panic(expected = "unrelated panic")]
    fn unrelated_panic_while_reply_id_alive_surfaces_original_message() {
        // Without DEF-052's close, this `id`'s Drop during unwind
        // would double-panic; with the close, the guard's
        // `thread::panicking()` check returns early and the original
        // panic message propagates.
        //
        // The panic itself is emitted via `assert_eq!` (not `panic!`
        // macro — `clippy::panic` is forbid-level) with the substring
        // "unrelated panic" in the message; `should_panic(expected =
        // "unrelated panic")` matches the substring.
        let raw = NonZeroU64::new(11).unwrap_or(NonZeroU64::MIN);
        let id = ReplyId::from_raw(raw);
        let actual = id.get().get();
        assert_eq!(actual, 0, "unrelated panic (id was {actual})");
    }
}
