//! Typed opaque correlator for in-flight commands.
//!
//! `bsql-pg-proto` is `no_std` and oblivious to async runtimes; it cannot
//! own `tokio::sync::oneshot::Sender`s itself. Instead, each
//! a command from `push_command` carries a [`ReplyId<K>`] that the upstream
//! wrapper crate uses as the key in its pending-replies table.
//!
//! # Kind-parameterisation
//!
//! [`ReplyId<K>`] is parameterised by a marker type `K: ReplyKind`
//! that binds the **expected payload** via an associated type
//! [`ReplyKind::Payload`]. The only way to produce a
//! `StagedAction::DeliverReply` carrying payload `P` is through
//! [`crate::action::deliver`], whose signature is
//! `fn deliver<K: ReplyKind>(id: ReplyId<K>, payload: K::Payload) -> StagedAction`
//! — passing a `ReplyId<PingKind>` and a `StartupCompletePayload`
//! is a type error at the call site, not a runtime misroute.
//!
//! Without kind-parameterisation, a dispatcher emitting
//! `Action::DeliverReply { id: ping_id, value: Reply::StartupComplete
//! { .. } }` would compile cleanly — the value type erases the moment
//! it lands in the `Reply` sum. The wrapper's `HashMap<NonZeroU64,
//! oneshot::Sender<Reply>>` would silently route a Pong-sender a
//! StartupComplete payload. The kind tag closes that seam tier-1 at
//! compile time.
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
//! `bsql-pg-proto` mints IDs internally via [`crate::PgProtocol::next_reply_id`]:
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
//!   process-wide). The counter is NOT a `PgProtocol` field: an
//!   inline `u64` field grows the struct 520 → 528 B and shifts the
//!   LLVM whole-crate heuristic +6% on the `iter_10cols` decode
//!   bench. Linear types would lift this to tier-1 ("counter has
//!   never returned this value"); not available in stable Rust.
//! - **Niche optimization preserved.** `Option<ReplyId<K>>` stays the
//!   same size as `ReplyId<K>` itself (the `NonZeroU64` niche).
//! - **No sentinel collision.** Zero is reserved as "no ID"; the
//!   constructor refuses it.

use core::fmt;
use core::marker::PhantomData;
use core::num::NonZeroU64;

/// Canonical sentinel raw value for a
/// [`crate::action::PushFailure::id`] field on a `PushFailure` whose
/// `cause` is an [`crate::error::ProtocolError::InternalCrateBug`] and
/// where no real in-flight `ReplyId` is associated.
///
/// # Why a distinct sentinel
///
/// A naive `NonZeroU64::MIN` (= raw value `1`) sentinel is
/// **byte-identical** with the legitimate first `ReplyId` minted by
/// [`crate::PgProtocol::next_reply_id`] (the static atomic counter
/// returns `NonZeroU64::new(1)` on its first call). Monitoring code
/// that distinguishes "CrateBug-classified failure" vs "first-command
/// genuine failure" by inspecting the id alone would false-positive
/// every connection's first command.
///
/// `CRATE_BUG_REPLY_ID_SENTINEL = NonZeroU64::MAX` (raw value
/// `u64::MAX`) cannot collide with legitimate minting except at the
/// saturation edge of the global counter — architecturally distant
/// (~10^19 mints process-wide) AND immediately followed by an
/// Errored-state transition (`install_errored_replyid_saturation`)
/// that drops the connection — so the sentinel can collide with a
/// legitimate id only in a fully-quiescent test fixture that
/// explicitly pre-loads the counter to `u64::MAX − 1`. Production
/// wrappers never observe the collision.
///
/// # Monitoring contract
///
/// Wrappers SHOULD prefer matching on `push_failure.cause` (typed
/// [`crate::error::ProtocolError`] variant) over inspecting
/// `push_failure.id` for classifier disambiguation. This sentinel is
/// the secondary signal, intended for log-line readability and for
/// the very-rare wrapper that lacks a typed-cause inspection path.
pub(crate) const CRATE_BUG_REPLY_ID_SENTINEL: NonZeroU64 = NonZeroU64::MAX;

/// Seal for [`ReplyKind`] — external crates cannot introduce new
/// kinds. Each supported PG command-kind is part of this module's
/// private sealed set.
mod sealed {
    pub trait Sealed {}
}

/// Marker trait pairing a PG-command shape with the payload its
/// eventual reply carries. Sealed.
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
// Structural diagnostic. The sealed supertrait error «`T: Sealed`
// is not satisfied» is not actionable from outside the crate —
// listing the permitted kind tags here resolves that.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a valid `ReplyKind` tag",
    label = "valid tags are the uninhabited enums `PingKind`, `StartupKind`, `QueryKind`, `ParseKind`, `CloseKind`, `DescribeStatementKind`, `DescribePortalKind`",
    note = "`ReplyKind` is sealed — the kind tag set is fixed at the crate boundary; downstream `impl ReplyKind for ...` is forbidden by construction"
)]
pub trait ReplyKind: sealed::Sealed {
    /// The typed STAGED payload constructed at dispatch time. Must
    /// convert to the internal [`crate::action::StagedReply`] sum.
    ///
    /// # Staged vs public payload split
    ///
    /// The dispatch site constructs the staged payload (lifetime-free,
    /// no `&'r RowDesc` borrows); materialise converts to the public
    /// `Reply<'r>` by borrowing `PgProtocol::row_desc_slot` directly.
    /// The slot's `is_some()` IS the schema-presence fact (single
    /// source of truth, tier-1 by-construction) — a duplicate
    /// `schema_present: bool` flag would split that source of truth.
    /// The kind-payload pairing is preserved — `ReplyId<K>` still
    /// constrains what payload the dispatcher can stage, and the
    /// `Into<StagedReply>` bound keeps the seal one-way.
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

/// Kind marker for `push_command::Ping` replies.
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

/// Kind marker for `push_command::Startup` replies.
///
/// Payload type: [`crate::action::StartupCompletePayload`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupKind {}
impl sealed::Sealed for StartupKind {}
impl ReplyKind for StartupKind {
    type StagedPayload = crate::action::StartupCompletePayload;
    const NAME: &'static str = "Startup";
}

// ───────────────── Query-flow ReplyKind markers ─────────────────
//
// Each Query-flow command carries a typed `ReplyId<K>` binding
// the final reply payload. Mirror the kind-marker pattern
// established for PingKind / StartupKind.

/// Kind marker for `push_command::SimpleQuery` and
/// `push_command::BindExecute` replies. Payload type:
/// [`crate::action::QueryCompletePayload`].
///
/// Delivered on the terminal `CommandComplete` + `ReadyForQuery`
/// pair after the row stream (which is emitted separately via
/// the row-streaming `ColEvent` pull API).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {}
impl sealed::Sealed for QueryKind {}
impl ReplyKind for QueryKind {
    type StagedPayload = crate::action::StagedQueryCompletePayload;
    const NAME: &'static str = "Query";
}

/// Kind marker for `push_command::Parse` replies. Payload:
/// [`crate::action::ParseCompletePayload`] (ZST).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseKind {}
impl sealed::Sealed for ParseKind {}
impl ReplyKind for ParseKind {
    type StagedPayload = crate::action::ParseCompletePayload;
    const NAME: &'static str = "Parse";
}

/// Kind marker for `push_command::CloseStatement` / `CloseP portal`
/// replies. Payload: [`crate::action::CloseCompletePayload`]
/// (ZST).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseKind {}
impl sealed::Sealed for CloseKind {}
impl ReplyKind for CloseKind {
    type StagedPayload = crate::action::CloseCompletePayload;
    const NAME: &'static str = "Close";
}

/// Kind marker for `push_command::DescribeStatement` replies.
///
/// Payload type: [`crate::action::DescribeStatementCompletePayload`] —
/// carries `param_oids` (PG's `ParameterDescription`), `rows`
/// (`RowDescription` → `Rows(..)` / `NoData` → `NoData`), and
/// `tx_status` from the trailing RFQ.
///
/// **Split vs Portal.** The kind-based split prevents a
/// `ReplyId<DescribeStatementKind>` from producing a
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

/// Kind marker for `push_command::DescribePortal` replies.
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
/// # Consume discipline — tier-1 compile
///
/// The only way to extract the underlying `NonZeroU64` is
/// [`ReplyId::consume`]. `#[must_use]` on the struct warns when a
/// `ReplyId<K>` is bound and then dropped without being consumed;
/// the dispatch machinery routes every id through `.consume()`
/// (into a NonZeroU64 for staging) or `.get()` (peek for staging
/// into StagedAction variants), so an undropped id at runtime is
/// possible only by deliberate bypass of the dispatcher.
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
///   [`crate::action::deliver`].
/// - **Tier 2 structural** — cannot be silently ignored from a
///   pattern match. The crate-root `#[deny(unused_variables)]`
///   combined with the crate-wide CREDO bans on `let _ = expr;`
///   and `_varname` suppression forces a match arm that binds
///   `id: ReplyId<K>` to refer to `id` in the arm body.
#[must_use = "a ReplyId should be consumed via `.consume()` into an Action — dropping it silently leaves the caller's receiver hanging (wrapper-layer timeout concern)"]
pub struct ReplyId<K: ReplyKind> {
    /// The wire-level correlator value. Never changes after
    /// construction.
    ///
    /// # NOT a secret, intentionally NOT zeroized
    ///
    /// The `value` is a monotonic correlator (matches commands to
    /// replies over the wire). It carries NO user data, NO
    /// credentials — just a sequence number. No `ZeroizeOnDrop` /
    /// `Zeroize` derive: the field is left in memory on drop by
    /// design, same as any POD sequence counter. Contrast with
    /// [`crate::sensitive::Sensitive`] wrappers on password /
    /// SCRAM-key material, which DO scrub on drop.
    value: NonZeroU64,
    // No `delivered: bool` field — discipline is enforced via
    // `#[must_use]` + integration-test observation on OutActions
    // content. A `delivered` flag would only support a panic-in-Drop
    // safety net (footgun under integration-test unwind).
    /// Phantom tag — zero-size, `fn() -> K` for unconditional
    /// autotraits. See [`crate::ident::FixedStr`] docstring for
    /// the full rationale of the `fn() -> K` phantom form.
    _kind: PhantomData<fn() -> K>,
}

impl<K: ReplyKind> ReplyId<K> {
    /// Construct a `ReplyId<K>` from a non-zero monotonic counter
    /// value.
    ///
    /// # Visibility
    ///
    /// `pub(crate)` only. External crates cannot construct a
    /// `ReplyId<K>`; the sole public mint is
    /// [`crate::PgProtocol::next_reply_id`]. This closes the
    /// "external fabrication" tier-3 seam — fabrication is
    /// impossible from outside this crate (visibility tier-1). A
    /// `pub` constructor would let the wrapper crate (or any
    /// consumer) mint duplicate IDs by accident, causing the
    /// protocol to deliver replies to the wrong correlator.
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
    /// Construct a fresh `ReplyId`. No `delivered: bool` field —
    /// discipline is enforced via `#[must_use]` rather than a
    /// Drop-time panic (the panic-in-Drop pattern is a footgun under
    /// integration-test unwind).
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

// ═════════════════════════════════════════════════════════════════════
// Shared NonZeroU64 mint helper for the three PROCESS_REPLY_ID_COUNTER
// mint sites (Disconnected / Connecting / Active phases).
// ═════════════════════════════════════════════════════════════════════

/// Mint a [`NonZeroU64`] from `prev.saturating_add(1)`.
///
/// # Floor invariant (const-asserted)
///
/// `saturating_add(1)` of any `u64` returns a value `≥ 1`:
/// - `u64::MAX.saturating_add(1) == u64::MAX` (saturates at `MAX`)
/// - `0_u64.saturating_add(1) == 1`
///
/// The two `const _: () = assert!(…)` lines below pin both bounds
/// at compile time. The `NonZeroU64::new(raw).unwrap_or(MIN)` arm
/// is therefore architecturally dead (build-time-pinned). A future
/// edit swapping `saturating_add` for `wrapping_add` (which CAN
/// produce `0`) leaves the const-asserts intact but **silently
/// activates the dead arm** — there is no `unsafe`-free way on
/// stable Rust to convert `u64 → NonZeroU64` without a runtime
/// branch. The const-asserts capture the invariant; the dead arm
/// remains as a clippy-`unwrap_used`-compliant landing pad.
///
/// # Mint sites
///
/// - `<DisconnectedPhase>::next_reply_id` — pre-handshake mint
///   (no inner state for a saturation classifier).
/// - `ConnectingInner::next_reply_id` — handshake-window mint
///   (`install_errored_replyid_saturation` fires on `raw_old ==
///   u64::MAX` BEFORE this helper, so the saturation case is
///   classified at the call site).
/// - `ActiveInner::next_reply_id` — active-phase mint (same
///   saturation classifier pattern as Connecting).
///
/// All three sites share the contract via this helper.
#[inline]
pub(crate) fn saturating_inc_to_nonzero(prev: u64) -> NonZeroU64 {
    const _: () = assert!(u64::MAX.saturating_add(1) >= 1);
    const _: () = assert!(0_u64.saturating_add(1) >= 1);
    let raw = prev.saturating_add(1);
    NonZeroU64::new(raw).unwrap_or(NonZeroU64::MIN)
}

// No `Drop` impl on `ReplyId<K>` — a `Drop` containing
// `assert!(self.delivered, ...)` would be a tier-2 runtime guard
// against "caller forgot to consume the reply", but panic-in-Drop is
// a hard footgun: it double-panics under integration-test unwind
// (a `#[cfg(test)]` early-return guard does NOT fire when the lib is
// compiled as a dependency of an integration test crate in `tests/`,
// so the SIGABRT masks the original failure), and under
// `panic = "abort"` production profile it's a hard abort.
//
// Discipline is enforced COMPILE-TIME via:
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
// `ReplyId<K>`. Callers that need to compare ids extract the
// wire-level `NonZeroU64` via `.get()` and compare those.

impl<K: ReplyKind> fmt::Debug for ReplyId<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Format: "ReplyId<KindName>(nzu_value)" — the kind name
        // makes Debug output self-describing. Wrapper / tests care
        // only about the wire value and the kind.
        write!(f, "ReplyId<{}>({})", K::NAME, self.value.get())
    }
}

#[cfg(test)]
mod reply_id_semantics {
    //! Tests cover (1) functional spec-conformance or (2) tier-3
    //! invariants only. Every test in this module is labelled with
    //! its category in the docstring.

    use super::*;

    /// Category (2) — tier-1 compile verification.
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

    /// Category (2) — tier-1 verification: the
    /// `CRATE_BUG_REPLY_ID_SENTINEL` is distinct from the legitimate
    /// first id minted by `next_reply_id`.
    ///
    /// A `NonZeroU64::MIN` (= raw `1`) sentinel would be byte-
    /// identical with the legitimate first id returned by
    /// `next_reply_id` (the static atomic counter starts at 0 and the
    /// first `fetch_add(1) + saturating_add(1)` produces 1) — a
    /// monitoring system distinguishing CrateBug failures from
    /// genuine first-command failures by inspecting `push_failure.id`
    /// alone would false-positive every connection's first command.
    ///
    /// The current sentinel is `NonZeroU64::MAX`. The legitimate
    /// first id is `NonZeroU64::new(1).unwrap()` = `MIN`; the
    /// sentinel is `MAX` — provably distinct.
    #[test]
    fn crate_bug_sentinel_distinct_from_first_mint() {
        // Legitimate first mint by `next_reply_id` cannot be
        // observed directly here (`next_reply_id` is on PgProtocol,
        // and the static counter is process-global), but the FIRST
        // value emitted by an atomic counter starting at 0 with
        // `fetch_add(1, Relaxed)` followed by `saturating_add(1)` is
        // provably `1` = `NonZeroU64::MIN`. Pin the distinctness
        // against that known value.
        let first_mint_shape: NonZeroU64 = NonZeroU64::MIN;
        assert_ne!(
            CRATE_BUG_REPLY_ID_SENTINEL,
            first_mint_shape,
            "CRATE_BUG_REPLY_ID_SENTINEL must be distinct from the \
             legitimate first id (NonZeroU64::MIN) — a colliding \
             sentinel would cause monitoring systems to false-\
             positive every first-command genuine failure as CrateBug",
        );
        // Pin the exact value so a future edit that changes the
        // sentinel surfaces here loudly (and motivates updating the
        // monitoring contract docstring on the const).
        assert_eq!(
            CRATE_BUG_REPLY_ID_SENTINEL,
            NonZeroU64::MAX,
            "CRATE_BUG_REPLY_ID_SENTINEL is canonical at NonZeroU64::MAX \
             — see reply_id.rs docstring",
        );
    }
}
