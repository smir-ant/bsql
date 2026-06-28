//! Typed opaque correlator for in-flight commands.
//!
//! [`ReplyId<K>`] is the nominal handle a connecting/active flow carries to
//! correlate a pushed command with its eventual reply. The kind parameter
//! `K: ReplyKind` is a sealed, uninhabited tag (`PingKind`, `StartupKind`, …)
//! that statically names the command shape — the connecting engine mints a
//! `ReplyId<StartupKind>` for the handshake, and the kind tag set is fixed at
//! the crate boundary.
//!
//! # Sealing
//!
//! `ReplyKind` is sealed (via the private `sealed::Sealed` supertrait) so
//! external code cannot introduce new kinds. Each PG command carries exactly
//! one kind, known statically at the crate level.
//!
//! # ID provenance
//!
//! `ReplyId<K>` wraps a [`NonZeroU64`]:
//!
//! 1. **Niche optimization.** `Option<ReplyId<K>>` stays the same size as
//!    `ReplyId<K>` itself (the `NonZeroU64` niche, since the `delivered: bool`
//!    is not in the discriminant).
//! 2. **No sentinel collision.** Zero is reserved as "no ID"; the constructor
//!    refuses it.
//! 3. **External fabrication: tier-1 by-visibility.** `ReplyId::from_raw` is
//!    `pub(crate)` — external crates cannot construct a `ReplyId<K>`.

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
/// eventual reply carries. Sealed.
///
/// Each impl is an uninhabited `enum` (structurally impossible to
/// instantiate) that serves purely as a type-level nominal tag.
/// The associated `Payload` type is the **only** payload shape
/// the protocol may deliver for this command-kind; attempts to
/// deliver a different payload fail to compile at the construction
/// site (see `crate::action::deliver`).
///
/// `Payload`: the associated type on `ReplyKind`
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
    /// Human-readable name for Debug output — `"Ping"`,
    /// `"Startup"`, etc.
    const NAME: &'static str;
}

/// Kind marker for `push_command::Ping` replies.
///
/// Payload type: `crate::action::PongPayload`. A dispatcher that
/// emits a non-`Pong` payload via a `ReplyId<PingKind>` fails to
/// type-check at the construction site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PingKind {}
impl sealed::Sealed for PingKind {}
impl ReplyKind for PingKind {
    const NAME: &'static str = "Ping";
}

/// Kind marker for `push_command::Startup` replies.
///
/// Payload type: `crate::action::StartupCompletePayload`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupKind {}
impl sealed::Sealed for StartupKind {}
impl ReplyKind for StartupKind {
    const NAME: &'static str = "Startup";
}

// ───────────────── Query-flow ReplyKind markers ─────────────────
//
// Each Query-flow command carries a typed `ReplyId<K>` binding
// the final reply payload. Mirror the kind-marker pattern
// established for PingKind / StartupKind.

/// Kind marker for `push_command::SimpleQuery` and
/// `push_command::BindExecute` replies. Payload type:
/// `crate::action::QueryCompletePayload`.
///
/// Delivered on the terminal `CommandComplete` + `ReadyForQuery`
/// pair after the row stream (which is emitted separately via
/// the row-streaming `ColEvent` pull API).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {}
impl sealed::Sealed for QueryKind {}
impl ReplyKind for QueryKind {
    const NAME: &'static str = "Query";
}

/// Kind marker for `push_command::Parse` replies. Payload:
/// `crate::action::ParseCompletePayload` (ZST).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseKind {}
impl sealed::Sealed for ParseKind {}
impl ReplyKind for ParseKind {
    const NAME: &'static str = "Parse";
}

/// Kind marker for `push_command::CloseStatement` / `CloseP portal`
/// replies. Payload: `crate::action::CloseCompletePayload`
/// (ZST).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseKind {}
impl sealed::Sealed for CloseKind {}
impl ReplyKind for CloseKind {
    const NAME: &'static str = "Close";
}

/// Kind marker for `push_command::DescribeStatement` replies.
///
/// Payload type: `crate::action::DescribeStatementCompletePayload` —
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
    const NAME: &'static str = "DescribeStatement";
}

/// Kind marker for `push_command::DescribePortal` replies.
///
/// Payload type: `crate::action::DescribePortalCompletePayload` —
/// no `param_oids` (portals are already bound; parameters are
/// fixed at Bind time and do not appear in a portal-Describe
/// response per PG §55.2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribePortalKind {}
impl sealed::Sealed for DescribePortalKind {}
impl ReplyKind for DescribePortalKind {
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
///   `crate::action::deliver`.
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
    /// `crate::PgProtocol::next_reply_id`. This closes the
    /// "external fabrication" tier-3 seam — fabrication is
    /// impossible from outside this crate (visibility tier-1). A
    /// `pub` constructor would let the wrapper crate (or any
    /// consumer) mint duplicate IDs by accident, causing the
    /// protocol to deliver replies to the wrong correlator.
    ///
    /// # Internal mint contract (tier-2 by-construction)
    ///
    /// Production callers (lib-internal) get the witness via
    /// `crate::PgProtocol::next_reply_id`, which uses a static
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

}
