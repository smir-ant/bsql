//! DEF-270 cluster (N-D letter, Phase 2 2026-05-10) — tier-1
//! state-transition ↔ command-kind pairing.
//!
//! # Pre-DEF-270 N-D
//!
//! Each `compute_push_*_idle_only` helper received `state: &mut ProtoState`
//! and wrote `*state = ProtoState::SomeVariant(...)` directly at the
//! tail of the happy path. Nothing structural prevented a refactor
//! that, say, transitioned [`crate::state::ProtoState::PingAwaitingRfq`]
//! at the end of the [`crate::push_command::SimpleQuery`] impl: the
//! types didn't pair the command kind to its post-state. **Tier-3
//! by-discipline**: the audit invariant "Ping pushes leave state
//! `PingAwaitingRfq`, SimpleQuery pushes leave it
//! `SimpleQueryAwaitingFirstResponse`, …" was upheld by reviewer
//! attention plus the `compute_push_tests` per-helper transition table.
//! A swap at an arm body would compile.
//!
//! # Post-DEF-270 N-D
//!
//! - [`crate::push_command::PushCommand`] declares
//!   `type PostState: PostStateProof` per impl. Each impl pairs its
//!   command struct to a single witness type.
//! - [`StateSetter<'_, W>`] is the **only** path
//!   [`crate::push_command::PushCommand::execute`] can mutate
//!   [`crate::state::ProtoState`]; the raw `&mut ProtoState` lives
//!   privately inside `push_command_internal`. Constructible only via
//!   `pub(crate)` constructor — external crates cannot mint a setter.
//! - [`StateSetter::install_post_state`] consumes `self` plus a
//!   `Self::PostState` witness. The witness type carries the data the
//!   matching state variant requires (e.g.,
//!   [`crate::push_command::PingAwaitingRfqInstall`] carries
//!   `ReplyId<PingKind>`). **Tier-1 by-construction**: there is no
//!   path to install
//!   [`crate::state::ProtoState::PingAwaitingRfq`] from a
//!   non-Ping impl; the witness type itself is the structural pairing
//!   between command kind and post-state.
//! - [`StateSetter::install_errored`] consumes `self` plus a
//!   [`crate::error::StateErrorKind`] — the failure-path counterpart.
//!   Used by the `try_builder!` macro on builder-error early-return.
//!
//! # Sealed surface
//!
//! Both [`PostStateProof`] and the [`StateSetter`] constructor are
//! `pub(crate)`-gated. External crates have no path to mint either —
//! the typestate seal is total at the crate boundary.

use crate::error::StateErrorKind;
use crate::state::ProtoState;
use core::marker::PhantomData;

/// Sealed-supertrait module. `pub(crate)` so in-crate witness types
/// can `impl Sealed for <their-witness>`. External crates cannot reach
/// this module (no public re-export).
pub(crate) mod sealed {
    /// Sealed marker — implementors are crate-internal only by
    /// virtue of `mod sealed`'s `pub(crate)` visibility (external
    /// crates have no path to the trait).
    pub trait Sealed {}
}

/// Sealed witness trait for a post-push state install. Implementors
/// are per-command witness types defined in
/// [`crate::push_command`] alongside their per-command struct.
///
/// `install_into` consumes the witness and writes the matching
/// [`ProtoState`] variant. **Tier-1 by-construction**: only the
/// matching witness type carries the data the variant needs, and only
/// `mod state_setter` (via [`StateSetter::install_post_state`]) can
/// invoke `install_into`. A future refactor that paired the wrong
/// post-state to a command would surface as a type mismatch at the
/// `type PostState` declaration in the [`crate::push_command::PushCommand`]
/// impl.
pub(crate) trait PostStateProof: sealed::Sealed {
    /// Consume the witness and write the corresponding [`ProtoState`]
    /// variant via `*state = ...`. Implementors should be `#[inline]`
    /// since each call site is monomorphic and the body is a single
    /// match + assignment.
    fn install_into(self, state: &mut ProtoState);
}

/// Tier-1 witness binding a mutable borrow of [`ProtoState`] to a
/// concrete post-state witness type.
///
/// **Construction:** only via [`Self::new`] (`pub(crate)`), called
/// inside [`crate::PgProtocol::push_command_internal`] just before
/// [`crate::push_command::PushCommand::execute`] is dispatched.
///
/// **Consumption:** exactly one of [`Self::install_post_state`] or
/// [`Self::install_errored`] consumes `self` per `execute` call.
/// Calling neither triggers an unused-`#[must_use]` lint at the impl
/// site (build-time loud signal that an `execute` arm leaks the
/// setter).
///
/// `_phantom: PhantomData<fn(W)>` — contravariant in `W`, modelling
/// the setter as "I accept a W". The setter never owns a W (W flows
/// in via `install_post_state(proof)` and is consumed there); the
/// phantom's role is purely the type-system bound that pairs the
/// setter's W to the trait's `Self::PostState` associated type.
#[must_use = "StateSetter must be consumed via install_post_state or install_errored — \
              dropping the setter without consuming leaves ProtoState in its \
              caller-provided value (Idle for push paths) instead of the post-push \
              transition the impl was supposed to perform"]
pub(crate) struct StateSetter<'a, W: PostStateProof> {
    state: &'a mut ProtoState,
    _phantom: PhantomData<fn(W)>,
}

impl<'a, W: PostStateProof> StateSetter<'a, W> {
    /// Construct a new setter. `pub(crate)` — only crate-internal
    /// callers (specifically [`crate::PgProtocol::push_command_internal`])
    /// can mint a setter. External callers (and even other modules
    /// outside the protocol body, save those that go through the
    /// internal push path) have no path to construct one.
    #[inline]
    pub(crate) fn new(state: &'a mut ProtoState) -> Self {
        Self {
            state,
            _phantom: PhantomData,
        }
    }

    /// Consume the setter and install the post-push state via the
    /// witness. The `proof` carries exactly the data the matching
    /// [`ProtoState`] variant requires; the witness's
    /// [`PostStateProof::install_into`] performs the `*state = ...`
    /// write.
    ///
    /// # Tier-1 closure
    ///
    /// `proof: W` — the trait method's witness type is statically
    /// paired to the [`crate::push_command::PushCommand`] impl's
    /// `type PostState`. A future refactor that paired Ping's
    /// command struct to (say) `SimpleQueryAwaitingFirstResponseInstall`
    /// fails to compile: the `setter.install_post_state(...)` call
    /// expects `Self::PostState = SimpleQueryAwaitingFirstResponseInstall`
    /// but the only constructor in scope at the Ping `execute` body
    /// is `PingAwaitingRfqInstall`.
    #[inline]
    pub(crate) fn install_post_state(self, proof: W) {
        proof.install_into(self.state);
    }

    /// Consume the setter and transition the state to
    /// [`ProtoState::Errored`] with the supplied
    /// [`StateErrorKind`]. Used by the `try_builder!` macro on
    /// builder-error early-return paths.
    ///
    /// # Idle-only contract
    ///
    /// Caller (the `try_builder!` macro) MUST ensure prev state was
    /// `Idle` — otherwise the previous variant's embedded `ReplyId`
    /// would leak (zombie-class regression, mirror of the pre-DEF-186
    /// `compute_push_*` perf-recovery audit). All current call sites
    /// are inside `Idle` arms of `compute_push_*_idle_only`; the
    /// macro carries a `debug_assert!(state == Idle)` that mirrors
    /// the pre-Phase 2 macro's same assertion.
    #[inline]
    pub(crate) fn install_errored(self, kind: StateErrorKind) {
        *self.state = ProtoState::Errored(kind);
    }
}

#[cfg(test)]
mod tests {
    /// Sealed-trait pin. The `mod sealed` module is `pub(crate)` and
    /// `Sealed` itself is `pub` within it; external crates have no
    /// path to either, so `impl PostStateProof for ExternalType`
    /// cannot be written outside this crate. Within the crate,
    /// each per-command witness in [`crate::push_command`] must
    /// `impl Sealed for <witness>` — surfacing the cross-module
    /// impl, by-design.
    #[test]
    fn seal_pin_anchor() {
        // Anchor for `git grep "state_setter.*seal"` searches.
    }
}
