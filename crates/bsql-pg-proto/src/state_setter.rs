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
use core::num::NonZeroU64;

/// Sealed-supertrait module. `pub(crate)` so in-crate witness types
/// can `impl Sealed for <their-witness>`. External crates cannot reach
/// this module (no public re-export).
pub(crate) mod sealed {
    /// Sealed marker — implementors are crate-internal only by
    /// virtue of `mod sealed`'s `pub(crate)` visibility (external
    /// crates have no path to the trait).
    pub trait Sealed {}
}

// DEF-280 Bundle F Phase 1 (2026-05-18) — within-crate hostile-witness closure
//
// The `mod install_body_seal` module below is PRIVATE to `mod state_setter`
// (no visibility keyword → private to parent). Combined with `InstallBody`
// having `install_body_seal::InstallBodySealed` as a supertrait, this seals
// the install-body trait against IN-CRATE callers outside this module.
//
// Pre-Bundle-F shape: `PostStateProof: sealed::Sealed { fn install_into(self, &mut ProtoState); }`.
// `sealed::Sealed` is `pub(crate)` (not module-private), so any in-crate module
// could write `impl Sealed for HostileWitness {} impl PostStateProof for
// HostileWitness { fn install_into(self, state) { *state = arbitrary_variant; } }`
// then mint a `StateSetter<HostileWitness>` via the generic `IdleState::into_setter::<W>`
// and call `setter.install_post_state(HostileWitness)`, dispatching to the
// hostile body. Tier-2-by-discipline within-crate.
//
// Post-Bundle-F: install bodies live in `impl InstallBody for *` blocks here in
// `mod state_setter`. `InstallBody` has private supertrait `InstallBodySealed`.
// Any in-crate caller outside state_setter attempting `impl InstallBody for
// HostileWitness` fails E0277 (HostileWitness: InstallBodySealed not satisfied),
// because writing `impl InstallBodySealed for HostileWitness` fails E0603
// (mod install_body_seal is private to state_setter). Tier-1-by-construction
// within-crate.
//
// `PostStateProof` becomes a pure marker (no method). It survives only for the
// `#[diagnostic::on_unimplemented]` UX message and as the publicly-named
// trait that PushCommand impls can satisfy at declaration time. The actual
// installation surface is `InstallBody`, with bound tightened on
// `StateSetter::install_post_state`, `StateSetter::install_errored`,
// `IdleState::into_setter`, and `PushCommand::PostState`.
mod install_body_seal {
    /// Tier-1 closure: this trait + the containing module are
    /// PRIVATE to `mod state_setter`. No in-crate code outside
    /// state_setter can reach the module (E0603 on the path), so
    /// no `impl InstallBodySealed for X` can be written from a sibling
    /// module. Combined with `InstallBody: ... + InstallBodySealed`,
    /// this seals `InstallBody` impls to state_setter only.
    pub trait InstallBodySealed {}
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
//
// DEF-270 N-D follow-up (rust-version 1.78 modernisation):
// structural diagnostic for crate-internal contributors. The
// PostStateProof set is closed at the crate boundary; the
// witness-pairing rule is in module docs but a bare bound failure
// is unactionable. Routes contributors to the matching `*Install`
// witness types in `push_command`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a `PostStateProof` witness for a `PushCommand` post-state install",
    label = "valid witnesses live next to their command in `push_command.rs` (e.g. `PingAwaitingRfqInstall` for `Ping`, `StartupAwaitingAuthRequestInstall` for `Startup`, etc.)",
    note = "`PostStateProof` is sealed crate-internal — each witness corresponds 1:1 to a `ProtoState` variant. To add a new command, define its struct in `push_command.rs` and add the matching `*Install` witness type with `impl PostStateProof` next to it (DEF-270 N-D pattern). The accompanying [`InstallBody`] impl must be added in `state_setter.rs` (DEF-280 Bundle F)."
)]
pub(crate) trait PostStateProof: sealed::Sealed {
    // DEF-280 Bundle F Phase 1: `fn install_into(self, state: &mut ProtoState)`
    // moved to private [`InstallBody`] trait below. `PostStateProof` is now
    // a pure marker — preserved for the `#[diagnostic::on_unimplemented]`
    // UX message and as the publicly-named trait satisfied by `*Install`
    // witnesses in `push_command.rs`. The actual install surface
    // (`fn install(self, &mut ProtoState)`) lives on `InstallBody`, whose
    // private supertrait `install_body_seal::InstallBodySealed` confines
    // impls to state_setter only (tier-1 within-crate by-construction).
}

/// Tier-1 within-crate install-body trait. **PRIVATE supertrait sealed:**
/// `InstallBody: ... + install_body_seal::InstallBodySealed`, and
/// `install_body_seal` is `mod install_body_seal` (no visibility keyword)
/// — private to `mod state_setter`. Any in-crate caller outside
/// state_setter attempting `impl InstallBody for X` fails E0277
/// (`X: InstallBodySealed` not satisfied), because writing
/// `impl InstallBodySealed for X` fails E0603 (mod install_body_seal
/// unreachable from siblings).
///
/// `install` consumes the witness and writes the matching `ProtoState`
/// variant via `*state = ...`. Implementors are `#[inline]` since each
/// call site is monomorphic and the body is a single (match +) assignment.
///
/// ## Bound at consumer surfaces
///
/// Tightened from `PostStateProof` → `InstallBody` on:
/// - [`StateSetter::install_post_state`] (was the attacker-controllable
///   dispatch site pre-Bundle-F)
/// - [`StateSetter::install_errored`] (mirror closure on the failure
///   transition; pre-Bundle-F any in-crate `StateSetter<HostileWitness>`
///   could flip state to `Errored(_)` — limited blast radius but the
///   same zombie-class hazard the audit table at lines 199-211
///   was built to close on the feed-side)
/// - [`IdleState::into_setter`] (declaration boundary closure: minting
///   a `StateSetter<HostileWitness>` itself is now E0277 unless
///   `HostileWitness: InstallBody` — unreachable for any non-state_setter
///   author)
/// - [`crate::push_command::PushCommand::PostState`] associated type bound
///   (full type-level pairing: a future `impl PushCommand for X` with
///   `type PostState = HostileWitness` is rejected at the trait-impl
///   declaration site, not just at the call site)
pub(crate) trait InstallBody: PostStateProof + install_body_seal::InstallBodySealed {
    /// Consume the witness and write the corresponding [`ProtoState`]
    /// variant via `*state = ...`. The body lives in state_setter only —
    /// every `impl InstallBody for *Install { fn install(...) {...} }`
    /// block is in this file. Hostile witnesses cannot supply a body
    /// (sealed supertrait blocks the impl declaration).
    fn install(self, state: &mut ProtoState);
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
pub(crate) struct StateSetter<'a, W: InstallBody> {
    state: &'a mut ProtoState,
    _phantom: PhantomData<fn(W)>,
}

impl<'a, W: InstallBody> StateSetter<'a, W> {
    /// Construct a new setter. **DEF-272 cluster γ (2026-05-10)**:
    /// constructor is `pub(in crate::state_setter)` — only callable
    /// from inside this module. The legitimate path to mint a setter
    /// is [`IdleState::into_setter`], which structurally binds the
    /// `state == Idle` precondition via the typestate's `try_from`
    /// runtime check.
    ///
    /// # Pre-γ (DEF-271 cluster A)
    ///
    /// Constructor was `pub(crate) fn new(state, _proof: IdleStateProof)`.
    /// `IdleStateProof::new()` was itself `pub(crate)` — any in-crate
    /// caller could mint a proof regardless of actual state, then pair
    /// with a non-Idle `&mut state` and trigger the Errored transition
    /// from a non-Idle state (zombie-reply class). Tier-2
    /// by-discipline within-crate; the precondition was a `debug_assert!`
    /// (skipped in release).
    ///
    /// # Post-γ
    ///
    /// Constructor is private to `mod state_setter`. The only path to
    /// a `StateSetter<'a, W>` value is [`IdleState::into_setter`]; the
    /// only path to an [`IdleState<'a>`] is [`IdleState::try_from`],
    /// which performs a runtime `matches!(state, ProtoState::Idle)`
    /// check and returns `None` for non-Idle states. Tier-1
    /// by-construction — pairing a proof with a non-Idle state is
    /// impossible (the typestate IS the state borrow + the Idle proof,
    /// inseparable).
    #[inline]
    pub(in crate::state_setter) fn new(state: &'a mut ProtoState) -> Self {
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
        proof.install(self.state);
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

// ═════════════════════════════════════════════════════════════════════
// DEF-271 cluster A (2026-05-10) — feed-side state setter
//
// Symmetric to push-side [`StateSetter<'_, W>`]. Mutates `ProtoState`
// from feed-side dispatch / RowStream fast-paths via a single typed
// surface that **atomically drains any in-flight reply id during the
// transition to Errored**, returning it to the caller via a
// `#[must_use]` Option. Pre-DEF-271 the feed-side `install_errored_*`
// helpers wrote `*self.state = ProtoState::Errored(...)` directly and
// the in-flight id was peeked separately at an earlier dispatch site —
// a tier-3 dual-source-of-truth audit risk (peek-id vs would-be-drained-
// id could diverge under refactor; the diverged path leaks the user's
// oneshot-receiver as the **zombie-reply class**).
// ═════════════════════════════════════════════════════════════════════

/// Tier-1 witness binding a mutable borrow of [`ProtoState`] to a
/// **feed-side** error transition. Mirror of [`StateSetter`] but for
/// the inbound-frame dispatch and RowStream paths.
///
/// **Construction:** `pub(crate) fn new(&mut ProtoState)`. Production
/// call sites (`PgProtocol::install_errored_*`,
/// `fail_inflight_no_readbuf`) construct the setter, immediately
/// consume it. The setter is `#[must_use]` — forgetting to consume is
/// a build error under crate-wide `deny(unused_must_use)`.
///
/// **Consumption:** [`Self::drain_and_install_errored`] is the sole
/// consumption path. Atomic: extracts the in-flight reply id from the
/// prior state via [`ProtoState::take_inflight_reply_raw_id`] **and**
/// installs `ProtoState::Errored(kind)` in one `mem::replace` —
/// observer (the `!Sync` `PgProtocol`) cannot witness the partial
/// triple. The returned `Option<NonZeroU64>` is `#[must_use]` —
/// dropping it leaks the in-flight reply's correlator (zombie-reply
/// class), and the crate's `deny(unused_must_use)` rejects the leak
/// at build time. **Tier-1 by-construction**: there is no path to
/// transition `ProtoState` to Errored from feed-side without surfacing
/// the prior in-flight reply id to the caller.
///
/// **Idle-only contract intentionally NOT enforced**: feed-side
/// transitions to Errored come from in-flight states (Streaming,
/// AwaitingRfq, Connecting, etc.), the opposite of the push-side
/// `StateSetter` Idle-only contract. The semantics differ; the witness
/// names differ.
#[must_use = "FeedStateSetter must be consumed via drain_and_install_errored — \
              dropping the setter without consuming leaves ProtoState in its \
              caller-provided value (likely an in-flight state) instead of the \
              Errored-with-drained-reply transition the caller was supposed to \
              perform"]
pub(crate) struct FeedStateSetter<'a> {
    state: &'a mut ProtoState,
}

impl<'a> FeedStateSetter<'a> {
    /// Construct a new feed-side setter. **DEF-272 cluster δ (2026-05-10)**:
    /// constructor is `pub(in crate::state_setter)` — only callable
    /// from inside this module. Legitimate construction goes through
    /// the per-call-site free fns below ([`drain_at_replyid_saturation`]
    /// and friends), each of which requires a per-call-site concrete
    /// token type whose mint is gated to a specific leaf submodule in
    /// `mod protocol`.
    #[inline]
    pub(in crate::state_setter) fn new(state: &'a mut ProtoState) -> Self {
        Self { state }
    }

    /// Atomic: drain any in-flight reply id from the prior state, then
    /// install `ProtoState::Errored(kind)` via `mem::replace`. Returns
    /// the drained id (Some if prior state carried an in-flight reply,
    /// None for `Idle` / `DrainRfqAfterError` / `Errored`).
    ///
    /// # Tier-1 closure
    ///
    /// `mem::replace` is the single atomic step: there is no partial
    /// state observable by an external borrower (`PgProtocol` is
    /// `!Sync`; the `&mut self` borrow chain rules out concurrent
    /// observers). Pre-DEF-271 the feed-side install_errored_* helpers
    /// transitioned without draining — the caller used a separately-
    /// peeked id at the dispatch site, a tier-3 dual-source-of-truth
    /// risk. Post-DEF-271 the drain and install are the same step, the
    /// returned id is the **only** id the caller can use, and the
    /// `#[must_use]` lint rejects leaks at build time.
    ///
    /// # Caller contract
    ///
    /// The returned `Option<NonZeroU64>`:
    /// - `Some(id)`: caller MUST emit `StagedAction::FailReply { id, cause }`
    ///   (or `ColEvent::EndQuery::Err { id, cause }` from RowStream).
    ///   Dropping the id leaves the user's oneshot-receiver hanging
    ///   forever — the **zombie-reply class** that the typestate is
    ///   here to prevent.
    /// - `None`: prior state had no in-flight reply (architecturally
    ///   unreachable from production feed-side call sites today, since
    ///   the install_errored_* helpers fire from streaming variants
    ///   that always carry a reply; classified rather than asserted to
    ///   keep tier-1 from collapsing to tier-3 on a future refactor
    ///   that allowed feed-side errors from a non-inflight variant).
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> contains the in-flight reply id (if any) \
                  released by this transition. Caller MUST emit StagedAction::FailReply \
                  { id, cause } or ColEvent::EndQuery::Err { id, cause } — dropping the id \
                  leaves the user's oneshot-receiver hanging forever (zombie-reply class). \
                  This `#[must_use]` + crate-wide `deny(unused_must_use)` rejects the leak \
                  at build time."]
    pub(crate) fn drain_and_install_errored(
        self,
        kind: StateErrorKind,
    ) -> Option<NonZeroU64> {
        let prev = core::mem::replace(self.state, ProtoState::Errored(kind));
        prev.take_inflight_reply_raw_id()
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-272 cluster δ (2026-05-10) — per-call-site token-gated FeedStateSetter constructors
//
// `FeedStateSetter::new` is `pub(in crate::state_setter)`; legitimate
// construction goes through the 4 free fns below, each requiring a
// distinct concrete-type token whose mint is gated to a specific leaf
// submodule in `mod protocol`. Same closure pattern as cluster α/β:
// the token's tuple-struct field is private to its leaf, so `Self(())`
// mints are callable ONLY inside the leaf submodule. Hostile in-crate
// (outside the leaf) attempting to call any `drain_at_*` fn cannot
// supply the required token type — type system rejects.
//
// Pre-δ `FeedStateSetter::new` was `pub(crate)`; any in-crate caller
// could mint a setter and trigger `drain_and_install_errored` to
// transition any state to Errored. Tier-2 by-discipline within-crate.
// Post-δ the call surface is exactly 4 named entry points, each
// gated by its concrete-type token. Tier-1 within-crate.
// ═════════════════════════════════════════════════════════════════════

/// DEF-272 cluster δ leaf entry point: ReplyId saturation transition.
/// Used by `PgProtocol::install_errored_replyid_saturation` (the
/// only legitimate caller; saturation classifier per cluster D).
/// Returns the drained in-flight reply id if any (None for `Idle` /
/// `Errored` / `DrainRfqAfterError` prior states — saturation can
/// fire from any state).
#[inline]
#[must_use = "the returned Option<NonZeroU64> is the in-flight reply id (if any) \
              released by the saturation transition. Caller is `install_errored_replyid_saturation` \
              which has no FailReply emission context (no &mut StagedActions); the caller \
              consumes the value via `match drain(...) { Some(_) | None => {} }`."]
pub(crate) fn drain_at_replyid_saturation(
    state: &mut ProtoState,
    _token: crate::protocol::_replyid_saturation_drain_leaf::ReplyIdSaturationToken,
    kind: StateErrorKind,
) -> Option<NonZeroU64> {
    FeedStateSetter::new(state).drain_and_install_errored(kind)
}

/// DEF-272 cluster δ leaf entry point: read-cursor advance failure
/// transition. Used by `PgProtocol::install_errored_read_cursor_advance`
/// (the only legitimate caller; classified as
/// `CrateBugLocus::ReadCursorAdvance`).
#[inline]
#[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
              by the Errored install. Caller MUST emit ColEvent::EndQuery::Err or equivalent — \
              dropping it leaks the user's oneshot-receiver (zombie-reply class)."]
pub(crate) fn drain_at_read_cursor_advance(
    state: &mut ProtoState,
    _token: crate::protocol::_read_cursor_advance_drain_leaf::ReadCursorAdvanceToken,
    kind: StateErrorKind,
) -> Option<NonZeroU64> {
    FeedStateSetter::new(state).drain_and_install_errored(kind)
}

/// DEF-280 Bundle K (2026-05-18) leaf entry point: partial-mode
/// re-entry detection. Used by
/// `PgProtocol::install_errored_partial_mode_reentry` (the only
/// legitimate caller; classified as
/// `CrateBugLocus::PartialModeReentry`).
#[inline]
#[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
              by the Errored install. Caller MUST emit ColEvent::EndQuery::Err or equivalent — \
              dropping it leaks the user's oneshot-receiver (zombie-reply class)."]
pub(crate) fn drain_at_partial_mode_reentry(
    state: &mut ProtoState,
    _token: crate::protocol::_partial_mode_reentry_drain_leaf::PartialModeReentryToken,
    kind: StateErrorKind,
) -> Option<NonZeroU64> {
    FeedStateSetter::new(state).drain_and_install_errored(kind)
}

/// DEF-280 Bundle K-mirror (2026-05-18) leaf entry point:
/// partial-mode exit-with-bytes-owed detection. Used by
/// `PgProtocol::install_errored_partial_mode_exit_undrained` (the only
/// legitimate caller; classified as
/// `CrateBugLocus::PartialModeExitUndrained`).
#[inline]
#[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
              by the Errored install. Caller MUST emit ColEvent::EndQuery::Err or equivalent — \
              dropping it leaks the user's oneshot-receiver (zombie-reply class)."]
pub(crate) fn drain_at_partial_mode_exit_undrained(
    state: &mut ProtoState,
    _token: crate::protocol::_partial_mode_exit_undrained_drain_leaf::PartialModeExitUndrainedToken,
    kind: StateErrorKind,
) -> Option<NonZeroU64> {
    FeedStateSetter::new(state).drain_and_install_errored(kind)
}

/// DEF-272 cluster δ leaf entry point: malformed-DataRow transition.
/// Used by `PgProtocol::install_errored_malformed_data_row` (the only
/// legitimate caller; classified as
/// `ProtocolError::MalformedDataRow`).
#[inline]
#[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
              by the Errored install. Caller MUST emit ColEvent::EndQuery::Err or equivalent."]
pub(crate) fn drain_at_malformed_data_row(
    state: &mut ProtoState,
    _token: crate::protocol::_malformed_data_row_drain_leaf::MalformedDataRowToken,
    kind: StateErrorKind,
) -> Option<NonZeroU64> {
    FeedStateSetter::new(state).drain_and_install_errored(kind)
}

/// DEF-272 cluster δ leaf entry point: dispatch fail-inflight-no-readbuf
/// transition. Used by `protocol::fail_inflight_no_readbuf` (the only
/// legitimate caller; routes any `ProtocolError` cause that fires
/// during dispatch when a read-buf state is unavailable).
#[inline]
#[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
              by the Errored install. Caller emits FailReply with the cause."]
pub(crate) fn drain_at_fail_inflight_no_readbuf(
    state: &mut ProtoState,
    _token: crate::protocol::_fail_inflight_no_readbuf_drain_leaf::FailInflightNoReadbufToken,
    kind: StateErrorKind,
) -> Option<NonZeroU64> {
    FeedStateSetter::new(state).drain_and_install_errored(kind)
}

/// DEF-248 Sub-A (2026-05-12) leaf entry point: RowStream Drop fired
/// while the stream was mid-frame (column events still pending or
/// partial-frame mode active). Used by
/// [`crate::PgProtocol::install_errored_stream_dropped_mid_stream`]
/// from inside [`crate::row_stream::RowStream::drop`]; classified as
/// [`crate::error::CrateBugLocus::StreamDroppedMidStream`].
///
/// **Caller drop context**: Drop has no `&mut StagedActions` /
/// downstream callbacks, so the drained in-flight reply id is absorbed
/// at the call site (the receiver this id correlates to no longer has
/// a path to be resolved via this RowStream invocation; the next
/// `feed_bytes` / `push_command` will surface
/// `ConnectionAlreadyClosed { prior_kind }` to indicate connection
/// teardown). The `#[must_use]` lint still fires inside the call site
/// for code-review discoverability (a future refactor that gains a
/// FailReply path here should consume the id).
#[inline]
#[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
              by the Errored install. Drop-site caller consumes the value via \
              `match drain(...) { Some(_) | None => {} }` — drop has no FailReply emission \
              context, but the next operation on the connection surfaces \
              ConnectionAlreadyClosed { prior_kind: ClientOrdering } so the user's \
              oneshot is not silently leaked at the wrapper layer."]
pub(crate) fn drain_at_stream_dropped_mid_stream(
    state: &mut ProtoState,
    _token: crate::protocol::_stream_dropped_mid_stream_drain_leaf::StreamDroppedMidStreamToken,
    kind: StateErrorKind,
) -> Option<NonZeroU64> {
    FeedStateSetter::new(state).drain_and_install_errored(kind)
}

// ═════════════════════════════════════════════════════════════════════
// DEF-272 cluster γ (2026-05-10) — IdleState lifetime-bound typestate
//
// Replaces the legacy [`crate::guard::IdleStateProof`] (DEF-198 ext +
// DEF-271 cluster A). The pre-γ proof was a ZST with `pub(crate) const
// fn new()` — anyone in-crate could mint a proof regardless of actual
// state, then pair it with a `&mut ProtoState` for a different state.
// Tier-2 by-discipline within-crate.
//
// Post-γ the typestate IS the state borrow + the Idle proof,
// inseparable. Construction via [`IdleState::try_from`] performs a
// runtime `matches!(state, ProtoState::Idle)` check. The returned
// `Option<Self>` is `None` for non-Idle states. The mut borrow
// captured by [`IdleState`] cannot be paired with a different state
// (lifetime ownership). [`IdleState::into_setter`] is the single
// legitimate path from the typestate to a [`StateSetter`]; the
// setter's `new()` constructor is `pub(in crate::state_setter)`
// (private), so no in-crate code outside this module can mint a
// setter without first proving Idle via the typestate.
//
// Tier-1 within-crate by-construction.
// ═════════════════════════════════════════════════════════════════════

/// Tier-1 within-crate typestate proving `state == ProtoState::Idle`
/// + carrying the matching `&'a mut ProtoState` borrow.
///
/// **Construction:** [`Self::try_from`] performs the runtime Idle
/// check. Returns `None` for non-Idle states. The mut borrow is
/// captured by the typestate; pairing the proof with a different
/// state is impossible by lifetime ownership.
///
/// **Consumption:** [`Self::into_setter`] consumes the typestate and
/// produces a [`StateSetter<'a, W>`]. The W type parameter is supplied
/// by the caller's match-on-command-kind dispatch (e.g.,
/// `idle.into_setter::<PingAwaitingRfqInstall>()` for a Ping).
#[must_use = "IdleState carries the &mut state borrow; dropping it leaves the state unchanged. \
              Consume via into_setter to perform a state-modifying transition."]
pub(crate) struct IdleState<'a> {
    state: &'a mut ProtoState,
}

impl<'a> IdleState<'a> {
    /// Construct an [`IdleState`] from a `&'a mut ProtoState`,
    /// returning `Some(_)` if the state is currently
    /// `ProtoState::Idle` and `None` otherwise. Production callers go
    /// through [`crate::PgProtocol::push_command`] which performs an
    /// upstream `as_ready()` runtime check; this `try_from` re-checks
    /// at the typestate boundary, providing build-time tier-1
    /// enforcement against any in-crate caller pairing a proof with a
    /// non-Idle state.
    #[inline]
    #[must_use]
    pub(crate) fn try_from(state: &'a mut ProtoState) -> Option<Self> {
        matches!(state, ProtoState::Idle).then(|| Self { state })
    }

    /// Consume the typestate and produce a [`StateSetter<'a, W>`].
    /// The setter inherits the mut borrow (the lifetime `'a` is
    /// preserved). This is the SOLE legitimate path to a [`StateSetter`]
    /// — the setter's constructor is `pub(in crate::state_setter)`
    /// (private), so no caller outside this module can construct a
    /// setter without first acquiring an [`IdleState`].
    #[inline]
    pub(crate) fn into_setter<W: InstallBody>(self) -> StateSetter<'a, W> {
        StateSetter::new(self.state)
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-280 Bundle F Phase 1 (2026-05-18) — InstallBody impls
//
// All 7 install bodies live HERE (not in push_command.rs) because
// `mod install_body_seal::InstallBodySealed` is private to state_setter.
// Any in-crate caller outside this module attempting `impl InstallBody
// for *Install` fails E0277 (witness: InstallBodySealed not satisfied),
// because writing `impl InstallBodySealed for witness` fails E0603
// (mod install_body_seal unreachable). Tier-1 within-crate by-construction.
//
// Pre-Bundle-F these bodies lived inside `impl PostStateProof for *Install`
// blocks in push_command.rs, where any in-crate author could mint a
// HostileWitness and supply an arbitrary `install_into` body. The trait
// split moves the install-code authority to state_setter alone.
//
// Each witness type's fields are `pub(crate)`, so state_setter can read
// them. The Cargo cross-module type reference (state_setter → push_command
// for type names; push_command → state_setter for the InstallBody bound on
// PushCommand::PostState) is non-cyclic at the type level (no recursive
// trait bounds) and Rust resolves it normally.
// ═════════════════════════════════════════════════════════════════════

impl install_body_seal::InstallBodySealed for crate::push_command::PingAwaitingRfqInstall {}
impl InstallBody for crate::push_command::PingAwaitingRfqInstall {
    #[inline]
    fn install(self, state: &mut ProtoState) {
        *state = ProtoState::PingAwaitingRfq(self.reply);
    }
}

impl install_body_seal::InstallBodySealed for crate::push_command::StartupPostInstall {}
impl InstallBody for crate::push_command::StartupPostInstall {
    #[inline]
    fn install(self, state: &mut ProtoState) {
        *state = match self {
            crate::push_command::StartupPostInstall::Trust { reply } => {
                ProtoState::ConnectingStartupTrust { reply }
            }
            crate::push_command::StartupPostInstall::Scram { reply, scram } => {
                ProtoState::ConnectingStartupScram { reply, scram }
            }
            crate::push_command::StartupPostInstall::Cleartext { reply, password } => {
                ProtoState::ConnectingStartupCleartext { reply, password }
            }
            crate::push_command::StartupPostInstall::Md5 { reply, handshake } => {
                ProtoState::ConnectingStartupMd5 { reply, handshake }
            }
        };
    }
}

impl install_body_seal::InstallBodySealed for crate::push_command::SimpleQueryAwaitingFirstResponseInstall {}
impl InstallBody for crate::push_command::SimpleQueryAwaitingFirstResponseInstall {
    #[inline]
    fn install(self, state: &mut ProtoState) {
        *state = ProtoState::SimpleQueryAwaitingFirstResponse(self.reply);
    }
}

impl install_body_seal::InstallBodySealed for crate::push_command::ParseAwaitingParseCompleteInstall {}
impl InstallBody for crate::push_command::ParseAwaitingParseCompleteInstall {
    #[inline]
    fn install(self, state: &mut ProtoState) {
        *state = ProtoState::ParseAwaitingParseComplete(self.reply);
    }
}

impl install_body_seal::InstallBodySealed for crate::push_command::DescribeStatementAwaitingParamDescInstall {}
impl InstallBody for crate::push_command::DescribeStatementAwaitingParamDescInstall {
    #[inline]
    fn install(self, state: &mut ProtoState) {
        *state = ProtoState::DescribeStatementAwaitingParamDesc(self.reply);
    }
}

impl install_body_seal::InstallBodySealed for crate::push_command::DescribePortalAwaitingRowDescOrNoDataInstall {}
impl InstallBody for crate::push_command::DescribePortalAwaitingRowDescOrNoDataInstall {
    #[inline]
    fn install(self, state: &mut ProtoState) {
        *state = ProtoState::DescribePortalAwaitingRowDescOrNoData(self.reply);
    }
}

impl install_body_seal::InstallBodySealed for crate::push_command::BindExecutePostInstall {}
impl InstallBody for crate::push_command::BindExecutePostInstall {
    #[inline]
    fn install(self, state: &mut ProtoState) {
        *state = match self {
            crate::push_command::BindExecutePostInstall::Dml { reply } => {
                ProtoState::BindExecuteAwaitingBindCompleteDml(reply)
            }
            crate::push_command::BindExecutePostInstall::Select { reply } => {
                ProtoState::BindExecuteAwaitingBindCompleteSelect { reply }
            }
        };
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

    /// DEF-280 Bundle F Phase 1 (2026-05-18) — InstallBody seal pin
    /// anchor. The `mod install_body_seal` module is PRIVATE to
    /// `mod state_setter` (no visibility keyword). External crates AND
    /// in-crate siblings cannot reach
    /// `state_setter::install_body_seal::InstallBodySealed`, so
    /// `impl InstallBody for HostileWitness` is structurally rejected
    /// (E0277: HostileWitness: InstallBodySealed not satisfied; the
    /// required `impl InstallBodySealed for HostileWitness` fails E0603
    /// at the path). All 7 legitimate `impl InstallBody for *Install`
    /// blocks live in this file (above the `tests` module).
    ///
    /// Negative-bound regression pin: see `push_command::tests::
    /// bundle_f_hostile_witness_install_body_absent` — uses the no-dep
    /// ambiguous-blanket-impl trick (mirror of `lib.rs:535`'s
    /// `assert_not_sync`) to assert at compile time that a HostileWitness
    /// constructed outside state_setter cannot satisfy InstallBody.
    #[test]
    fn install_body_seal_pin_anchor() {
        // Anchor for `git grep "state_setter.*install_body_seal"` searches.
    }
}
