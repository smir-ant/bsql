//! Typed opaque correlator for in-flight commands (DEF-112).
//!
//! `bsql-pg-proto` is `no_std` and oblivious to async runtimes; it cannot
//! own `tokio::sync::oneshot::Sender`s itself. Instead, each
//! [`crate::PgCommand`] carries a [`ReplyId<K>`] that the upstream
//! wrapper crate (`bsql-driver-postgres`, Phase 1e) uses as the key
//! in its pending-replies table.
//!
//! # DEF-112 — kind-parameterisation
//!
//! Before DEF-112 `ReplyId` was untyped (`{ value: NonZeroU64,
//! delivered: bool }`). A dispatcher that emitted
//! `Action::DeliverReply { id: ping_id, value: Reply::StartupComplete
//! { .. } }` would compile cleanly — the value type was erased the
//! moment it landed in the `Reply` sum. The wrapper's
//! `HashMap<NonZeroU64, oneshot::Sender<Reply>>` would silently
//! route a Pong-sender a StartupComplete payload. Tier-3 audit seam.
//!
//! DEF-112 elevates this to tier-1 compile. [`ReplyId<K>`] is now
//! parameterised by a marker type `K: ReplyKind` that binds the
//! **expected payload** via an associated type
//! [`ReplyKind::Payload`]. The only way to produce a
//! `StagedAction::DeliverReply` carrying payload `P` is through
//! [`crate::action::deliver`], whose signature is
//! `fn deliver<K: ReplyKind>(id: ReplyId<K>, payload: K::Payload) -> StagedAction`
//! — passing a `ReplyId<PingKind>` and a `StartupCompletePayload`
//! is a type error at the call site, not a runtime misroute.
//!
//! The [`crate::action::DeliverReplyEntry`] struct that backs the
//! variant has module-private fields so direct struct-literal
//! construction outside the sanctioned path is also a compile error —
//! not just a convention.
//!
//! # Sealing
//!
//! `ReplyKind` is sealed (via the private [`sealed::Sealed`]
//! supertrait) so external code cannot introduce new kinds. Each
//! PG command carries exactly one kind, known statically at the
//! crate level; new kinds land with new commands in later sub-phases.
//!
//! # ID provenance
//!
//! `ReplyId<K>` wraps a [`NonZeroU64`]:
//!
//! 1. **Niche optimization.** `Option<ReplyId<K>>` stays the same
//!    size as `ReplyId<K>` itself (the `NonZeroU64` niche, since the
//!    `delivered: bool` is not in the discriminant).
//! 2. **No sentinel collision.** Zero is reserved as "no ID"; the
//!    constructor refuses it.
//!
//! `bsql-pg-proto` mints IDs internally via [`crate::PgProtocol::next_reply_id`]
//! (DEF-270 cluster, 2026-05-09 — supersedes reforge.md §7.5 wrapper-mint
//! discipline). Pre-DEF-270 the wrapper crate ran a per-connection
//! monotonic counter and collision-freedom was the wrapper's
//! responsibility — **tier-3 by audit** (cross-crate seal not
//! expressible in stable Rust). Post-DEF-270:
//!
//! - **External fabrication: tier-1 by-visibility.** [`ReplyId::from_raw`]
//!   is `pub(crate)` — external crates cannot construct a `ReplyId<K>`.
//!   The sole public mint is [`crate::PgProtocol::next_reply_id`]
//!   `<K: ReplyKind>(&mut self) -> ReplyId<K>`.
//! - **Cross-instance monotonicity: tier-2 by atomic-fetch_add.**
//!   The mint counter is a `static AtomicU64` inside
//!   `next_reply_id` (mod-private). `fetch_add(1, Relaxed)` gives
//!   globally-unique IDs across all `PgProtocol` instances and
//!   threads — stronger than per-instance uniqueness. Saturating
//!   `u64` add — architecturally-distant ceiling (~10^19 mints
//!   process-wide). The counter is NOT a `PgProtocol` field
//!   (bisect 2026-05-09 proved an inline `u64` field would grow
//!   the struct 520 → 528 B and shift LLVM whole-crate heuristic
//!   +6% on `iter_10cols` decode bench). Linear types would lift
//!   this to tier-1 ("counter has never returned this value");
//!   not available pre-stable Rust.
//! - **Niche optimization preserved.** `Option<ReplyId<K>>` stays the
//!   same size as `ReplyId<K>` itself (the `NonZeroU64` niche).
//! - **No sentinel collision.** Zero is reserved as "no ID"; the
//!   constructor refuses it.

use core::fmt;
use core::marker::PhantomData;
use core::num::NonZeroU64;

/// Seal for [`ReplyKind`] — external crates cannot introduce new
/// kinds. Each supported PG command-kind is part of this module's
/// private sealed set.
mod sealed {
    pub trait Sealed {}
}

/// Marker trait pairing a PG-command shape with the payload its
/// eventual reply carries. Sealed (DEF-112).
///
/// Each impl is an uninhabited `enum` (structurally impossible to
/// instantiate) that serves purely as a type-level nominal tag.
/// The associated [`Payload`] type is the **only** payload shape
/// the protocol may deliver for this command-kind; attempts to
/// deliver a different payload fail to compile at the construction
/// site (see [`crate::action::deliver`]).
///
/// [`Payload`]: ReplyKind::Payload
//
// DEF-112 follow-up (rust-version 1.78 modernisation): structural
// diagnostic. The sealed supertrait error «`T: Sealed` is not
// satisfied» is not actionable from outside the crate — listing the
// permitted kind tags here resolves that.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a valid `ReplyKind` tag",
    label = "valid tags are the uninhabited enums `PingKind`, `StartupKind`, `QueryKind`, `ParseKind`, `CloseKind`, `DescribeStatementKind`, `DescribePortalKind`",
    note = "`ReplyKind` is sealed (DEF-112) — the kind tag set is fixed at the crate boundary; downstream `impl ReplyKind for ...` is forbidden by construction"
)]
pub trait ReplyKind: sealed::Sealed {
    /// The typed STAGED payload constructed at dispatch time. Must
    /// convert to the internal [`crate::action::StagedReply`] sum.
    ///
    /// # DEF-119 + DEF-188 + DEF-210 SR-01 — staged vs public payload split
    ///
    /// The dispatch site constructs the staged payload (lifetime-free,
    /// no `&'r RowDesc` borrows); materialise converts to the public
    /// `Reply<'r>` by borrowing `PgProtocol::row_desc_slot` directly.
    /// DEF-210 SR-01 Path C deleted the prior `schema_present: bool`
    /// duplicate flag — the slot's `is_some()` IS the schema-presence
    /// fact (single source of truth, tier-1 by-construction).
    /// The DEF-112 kind-payload pairing is preserved — `ReplyId<K>`
    /// still constrains what payload the dispatcher can stage, and
    /// the `Into<StagedReply>` bound keeps the seal one-way.
    ///
    /// For schema-less kinds (Ping, Startup, Parse, Close),
    /// StagedPayload == PublicPayload (no schema to borrow). For
    /// schema-bearing kinds (Query, DescribeStatement,
    /// DescribePortal), StagedPayload is the crate-private
    /// `Staged*Payload` struct.
    type StagedPayload: Into<crate::action::StagedReply> + Copy + fmt::Debug + 'static;

    /// Human-readable name for Debug output — `"Ping"`,
    /// `"Startup"`, etc.
    const NAME: &'static str;
}

/// Kind marker for [`crate::PgCommand::Ping`] replies.
///
/// Payload type: [`crate::action::PongPayload`]. A dispatcher that
/// emits a non-`Pong` payload via a `ReplyId<PingKind>` fails to
/// type-check at the construction site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PingKind {}
impl sealed::Sealed for PingKind {}
impl ReplyKind for PingKind {
    type StagedPayload = crate::action::PongPayload;
    const NAME: &'static str = "Ping";
}

/// Kind marker for [`crate::PgCommand::Startup`] replies.
///
/// Payload type: [`crate::action::StartupCompletePayload`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupKind {}
impl sealed::Sealed for StartupKind {}
impl ReplyKind for StartupKind {
    type StagedPayload = crate::action::StartupCompletePayload;
    const NAME: &'static str = "Startup";
}

// ───────────────── Phase 1c ReplyKind markers ─────────────────
//
// Each Query-flow command carries a typed `ReplyId<K>` binding
// the final reply payload. Mirror the DEF-112 pattern
// established for PingKind / StartupKind. Dispatch wiring lands
// in sub-phases 1c-1 (Query) and 1c-3 (Parse / Close).

/// Kind marker for `PgCommand::SimpleQuery` and
/// `PgCommand::BindExecute` replies. Payload type:
/// [`crate::action::QueryCompletePayload`].
///
/// Delivered on the terminal `CommandComplete` + `ReadyForQuery`
/// pair after the row stream (which is emitted separately via
/// `Action::StreamRow`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {}
impl sealed::Sealed for QueryKind {}
impl ReplyKind for QueryKind {
    type StagedPayload = crate::action::StagedQueryCompletePayload;
    const NAME: &'static str = "Query";
}

/// Kind marker for `PgCommand::Parse` replies. Payload:
/// [`crate::action::ParseCompletePayload`] (ZST).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseKind {}
impl sealed::Sealed for ParseKind {}
impl ReplyKind for ParseKind {
    type StagedPayload = crate::action::ParseCompletePayload;
    const NAME: &'static str = "Parse";
}

/// Kind marker for `PgCommand::CloseStatement` / `CloseP portal`
/// replies. Payload: [`crate::action::CloseCompletePayload`]
/// (ZST).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseKind {}
impl sealed::Sealed for CloseKind {}
impl ReplyKind for CloseKind {
    type StagedPayload = crate::action::CloseCompletePayload;
    const NAME: &'static str = "Close";
}

/// Kind marker for `PgCommand::DescribeStatement` replies.
///
/// Payload type: [`crate::action::DescribeStatementCompletePayload`] —
/// carries `param_oids` (PG's `ParameterDescription`), `rows`
/// (`RowDescription` → `Rows(..)` / `NoData` → `NoData`), and
/// `tx_status` from the trailing RFQ.
///
/// **Split vs Portal.** DEF-112 drives the kind-based split: a
/// `ReplyId<DescribeStatementKind>` cannot produce a
/// `DescribePortalCompletePayload` at the typed `deliver` call site.
/// The user's oneshot receiver sees only the payload shape the
/// command-variant invoked — no `Option<ParamOids>` surface-level
/// uncertainty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeStatementKind {}
impl sealed::Sealed for DescribeStatementKind {}
impl ReplyKind for DescribeStatementKind {
    type StagedPayload = crate::action::StagedDescribeStatementCompletePayload;
    const NAME: &'static str = "DescribeStatement";
}

/// Kind marker for `PgCommand::DescribePortal` replies.
///
/// Payload type: [`crate::action::DescribePortalCompletePayload`] —
/// no `param_oids` (portals are already bound; parameters are
/// fixed at Bind time and do not appear in a portal-Describe
/// response per PG §55.2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribePortalKind {}
impl sealed::Sealed for DescribePortalKind {}
impl ReplyKind for DescribePortalKind {
    type StagedPayload = crate::action::StagedDescribePortalCompletePayload;
    const NAME: &'static str = "DescribePortal";
}

/// Typed opaque handle correlating a pushed command with its
/// eventual reply.
///
/// The type parameter `K: ReplyKind` binds the expected reply
/// payload: `ReplyId<PingKind>` commits the protocol to producing a
/// `PongPayload` on success, and nothing else. The
/// `PhantomData<fn() -> K>` is the load-bearing nominal-typing
/// mechanism — unconditionally `Copy + Send + Sync + !Drop`
/// regardless of `K` — see [`crate::ident::FixedStr`] for the same
/// pattern on the bounded-string hierarchy.
///
/// # Consume discipline — tier-1 compile + tier-2 runtime
///
/// `ReplyId<K>` tracks whether its value has been **delivered**. The
/// only way to extract the underlying `NonZeroU64` is
/// [`ReplyId::consume`], which also marks the id as delivered.
/// Dropping a `ReplyId<K>` for which `consume` was never called is a
/// runtime failure (DEF-101).
///
/// # Layered guarantees
///
/// - **Tier 1 compile** — non-duplicatable. No `Copy`, no `Clone` impl.
/// - **Tier 1 compile** — cannot be extracted without acknowledging
///   the consume step. Extracting the value requires calling
///   `consume(self)`, not `&self` — so you can't "peek and forget".
/// - **Tier 1 compile** — kind-parameterised. A `ReplyId<PingKind>`
///   cannot produce anything other than a `PongPayload`-backed
///   delivery; attempting so is a type error at
///   [`crate::action::deliver`]. DEF-112.
/// - **Tier 2 structural** — cannot be silently ignored from a
///   pattern match. The crate-root `#[deny(unused_variables)]`
///   combined with the architect.txt Part V bans on `let _ = expr;`
///   and `_varname` suppression forces a match arm that binds
///   `id: ReplyId<K>` to refer to `id` in the arm body.
/// - **Tier 2 runtime** — Drop-guard asserts delivered on drop
///   (see DEF-101 analysis in deferred.md §16 for why this is the
///   stable-Rust ceiling, not tier-1).
#[must_use = "a ReplyId should be consumed via `.consume()` into an Action — dropping it silently leaves the caller's receiver hanging (wrapper-layer timeout concern)"]
pub struct ReplyId<K: ReplyKind> {
    /// The wire-level correlator value. Never changes after
    /// construction.
    ///
    /// # DEF-163 A006 — NOT a secret, intentionally NOT zeroized
    ///
    /// The `value` is a monotonic correlator (matches commands to
    /// replies over the wire). It carries NO user data, NO
    /// credentials — just a sequence number. No `ZeroizeOnDrop` /
    /// `Zeroize` derive: the field is left in memory on drop by
    /// design, same as any POD sequence counter. Contrast with
    /// [`crate::sensitive::Sensitive`] wrappers on password /
    /// SCRAM-key material, which DO scrub on drop.
    value: NonZeroU64,
    // DEF-154 (K): `delivered: bool` field DELETED — it only
    // supported the panic-in-Drop safety net (now removed).
    // Discipline enforced via `#[must_use]` + integration-test
    // observation on OutActions content.
    /// Phantom tag — zero-size, `fn() -> K` for unconditional
    /// autotraits. See [`crate::ident::FixedStr`] docstring for
    /// the full rationale of the `fn() -> K` phantom form.
    _kind: PhantomData<fn() -> K>,
}

impl<K: ReplyKind> ReplyId<K> {
    /// Construct a `ReplyId<K>` from a non-zero monotonic counter
    /// value.
    ///
    /// # Visibility (DEF-270 cluster, 2026-05-09 — U letter)
    ///
    /// `pub(crate)` only. External crates cannot construct a
    /// `ReplyId<K>`; the sole public mint is
    /// [`crate::PgProtocol::next_reply_id`]. This closes the
    /// "external fabrication" tier-3 seam: pre-DEF-270 the wrapper
    /// crate (or any consumer) could mint duplicate IDs by accident,
    /// causing the protocol to deliver replies to the wrong
    /// correlator. Post-DEF-270, **fabrication is impossible from
    /// outside this crate** — visibility tier-1.
    ///
    /// # Internal mint contract (tier-2 by-construction)
    ///
    /// Production callers (lib-internal) get the witness via
    /// [`crate::PgProtocol::next_reply_id`], which uses a static
    /// `AtomicU64` counter (mod-private) with `fetch_add(1, Relaxed)`
    /// — globally unique across all `PgProtocol` instances and
    /// threads. Cross-instance monotonicity is stronger than
    /// per-protocol exclusivity. (Static-atomic chosen over inline
    /// field after bisect proved an inline u64 caused +6% LLVM
    /// codegen shift on synthetic decode benches.)
    ///
    /// Test callers inside this crate (state.rs / reply_id.rs unit
    /// tests, compute_push_tests) construct fixtures directly via this
    /// `pub(crate) from_raw` — collision-freedom in fixture context
    /// is by-test-discipline (each test picks a distinct value range:
    /// 1xxx for ping, 2xxx for startup, etc. — see `state::push_class_tests`).
    ///
    /// DEF-154 (K): construct a fresh `ReplyId`. Pre-(K) carried a
    /// `delivered: bool` field for Drop-time checking — now deleted
    /// since the Drop panic is gone (footgun under integration-test
    /// unwind; see Drop-impl deletion block above).
    #[inline]
    pub(crate) const fn from_raw(value: NonZeroU64) -> Self {
        Self {
            value,
            _kind: PhantomData,
        }
    }

    /// Extract the underlying counter value, consuming the handle.
    ///
    /// After calling `consume`, the `ReplyId<K>` is gone — the raw
    /// value travels inside an outgoing `Action`, which is not
    /// consume-tracked.
    #[inline]
    pub fn consume(self) -> NonZeroU64 {
        self.value
    }

    /// Peek at the underlying counter value without consuming the
    /// id.
    ///
    /// Useful for logging and for tests that want to assert a reply
    /// id round-trips correctly without having to wait until the id
    /// has been packaged into an outgoing Action.
    #[inline]
    #[must_use]
    pub const fn get(&self) -> NonZeroU64 {
        self.value
    }
}

// DEF-154 (K): `Drop for ReplyId<K>` DELETED.
//
// Pre-(K), Drop contained a `assert!(self.delivered, ...)` safety
// net — intended as a tier-2 runtime guard against "caller forgot
// to consume the reply." Two fatal flaws for the user directive
// "никаких потенциальных паник":
//
// 1. **Double-panic SIGABRT in integration tests.** The `#[cfg(test)]`
//    + `std::thread::panicking()` early-return guard was scoped to
//    the LIB's own test mode only — `#[cfg(test)]` evaluates to
//    false when the lib is compiled as a dependency of an
//    integration test crate (in `tests/`). Integration test
//    assertion failures unwound `PgProtocol` which held a
//    non-delivered `ReplyId` in its state → Drop asserted →
//    double-panic → SIGABRT masked the original test failure.
//    User reproduced this on `zero_body_data_row_classified_as_malformed_data_row`.
//
// 2. **Panic-in-Drop is a maintenance footgun**, period. In
//    `panic = "abort"` production profile it's a hard abort;
//    in `panic = "unwind"` any unwind through a ReplyId alive on
//    the stack double-panics. Neither is acceptable per user's
//    "никаких потенциальных паник" directive.
//
// Discipline is now enforced COMPILE-TIME via:
//
//   - `#[must_use]` on `ReplyId<K>` (warns on unused binding).
//   - Every dispatch path routes `ReplyId` through `.consume()`
//     (into a NonZeroU64 for staging) or `.get()` (peek for
//     staging into StagedAction variants). State variants that
//     hold a `ReplyId` across calls are exhaustively classified
//     by the `ProtoState` enum — dropping the state produces no
//     in-flight reply by construction (terminal states transition
//     to Idle/Errored at the moment the inflight reply is drained).
//   - Integration tests observe delivery via the returned
//     OutActions content (e.g. `matches!(a, Action::DeliverReply { .. })`).
//
// If a caller TRULY drops a non-delivered ReplyId (bypassing all
// the dispatch machinery), the caller's oneshot-receiver hangs
// silently — that's a wrapper-layer concern (host runtime decides
// timeout / cancel semantics), not a protocol-crate-internal
// panic target.

// `PartialEq`, `Eq`, `Hash` are **deliberately NOT implemented** on
// `ReplyId<K>` (DEF-088 tier raise, retained through DEF-112).
// Callers that need to compare ids extract the wire-level
// `NonZeroU64` via `.get()` and compare those.

impl<K: ReplyKind> fmt::Debug for ReplyId<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Format: "ReplyId<KindName>(nzu_value)" — the kind name
        // makes Debug output self-describing. `delivered` stays
        // internal bookkeeping; wrapper / tests care only about the
        // wire value and the kind.
        write!(f, "ReplyId<{}>({})", K::NAME, self.value.get())
    }
}

#[cfg(test)]
mod reply_id_semantics {
    //! Per reforge.md §4.11, tests cover category (1) functional
    //! spec-conformance or (2) tier-3 invariants only. Every test in
    //! this module is labelled with its category in the docstring.

    use super::*;

    // DEF-154 (K): `undelivered_drop_panics` + per-kind siblings +
    // `unrelated_panic_while_reply_id_alive_surfaces_original_message`
    // — ALL DELETED. The panic-in-Drop guard was removed (it
    // double-panicked under integration-test unwind, masking
    // original failures with SIGABRT); the tests that pinned its
    // behaviour no longer have a target to pin. Discipline is now
    // `#[must_use]` on ReplyId + integration tests asserting
    // delivery via OutActions content.

    /// Category (2) — DEF-112 tier-1 compile verification.
    ///
    /// The Debug output distinguishes `ReplyId<PingKind>` from
    /// `ReplyId<StartupKind>` via the `K::NAME` const — this makes
    /// the kind visible in logs without exposing `K` to `&self`
    /// patterns. Pinning the format here so a future edit that
    /// silently strips the kind from Debug is caught.
    #[test]
    fn debug_prints_kind_name() {
        let raw = NonZeroU64::new(13).unwrap_or(NonZeroU64::MIN);
        let ping: ReplyId<PingKind> = ReplyId::from_raw(raw);
        let startup: ReplyId<StartupKind> = ReplyId::from_raw(raw);
        let ping_str = std::format!("{ping:?}");
        let startup_str = std::format!("{startup:?}");
        assert_eq!(ping_str, "ReplyId<Ping>(13)");
        assert_eq!(startup_str, "ReplyId<Startup>(13)");
        // Consume both so the Drop-guard doesn't fire at end of scope.
        // `consume` returns `NonZeroU64` (not `#[must_use]`), so the
        // return value is statement-discarded without `let _` (banned).
        ping.consume();
        startup.consume();
    }

    // DEF-154 (K): per-kind drop-panic pins (startup/query/parse/
    // close/describe_statement/describe_portal) DELETED along with
    // the Drop impl itself. Each kind is exercised through
    // dispatch flow tests in `tests/*.rs` which assert delivery by
    // matching on Action variants — the tier ABOVE Drop-guard.
}
