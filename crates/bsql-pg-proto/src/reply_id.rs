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
//! `bsql-pg-proto` does **not** mint IDs. The wrapper crate runs a
//! per-connection monotonic counter starting at 1; collision-freedom
//! is the wrapper's responsibility. Per reforge.md §7.5, this is
//! **tier-3 by audit**: the cross-crate seal is not expressible in
//! stable Rust today. Mitigations:
//!
//! - The constructor takes `NonZeroU64` (zero impossible at the type
//!   level).
//! - Production wrappers must use a single fetch-add counter per
//!   connection. Reusing IDs across the same `PgProtocol` instance is
//!   undefined at the spec level (the protocol can deliver to the
//!   wrong sender), but cannot violate memory safety.

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
pub trait ReplyKind: sealed::Sealed {
    /// The typed payload delivered on success. Must convert to the
    /// erased [`crate::action::Reply`] sum for wire-level dispatch.
    type Payload: Into<crate::action::Reply> + Copy + fmt::Debug + 'static;

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
    type Payload = crate::action::PongPayload;
    const NAME: &'static str = "Ping";
}

/// Kind marker for [`crate::PgCommand::Startup`] replies.
///
/// Payload type: [`crate::action::StartupCompletePayload`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupKind {}
impl sealed::Sealed for StartupKind {}
impl ReplyKind for StartupKind {
    type Payload = crate::action::StartupCompletePayload;
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
    type Payload = crate::action::QueryCompletePayload;
    const NAME: &'static str = "Query";
}

/// Kind marker for `PgCommand::Parse` replies. Payload:
/// [`crate::action::ParseCompletePayload`] (ZST).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseKind {}
impl sealed::Sealed for ParseKind {}
impl ReplyKind for ParseKind {
    type Payload = crate::action::ParseCompletePayload;
    const NAME: &'static str = "Parse";
}

/// Kind marker for `PgCommand::CloseStatement` / `CloseP portal`
/// replies. Payload: [`crate::action::CloseCompletePayload`]
/// (ZST).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseKind {}
impl sealed::Sealed for CloseKind {}
impl ReplyKind for CloseKind {
    type Payload = crate::action::CloseCompletePayload;
    const NAME: &'static str = "Close";
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
#[must_use = "a ReplyId must be consumed via `.consume()` into an Action — dropping it without delivery is a runtime error"]
pub struct ReplyId<K: ReplyKind> {
    /// The wire-level correlator value. Never changes after
    /// construction.
    value: NonZeroU64,
    /// Whether [`ReplyId::consume`] was called before drop. The
    /// Drop impl reads this to decide whether to panic.
    delivered: bool,
    /// Phantom tag — zero-size, `fn() -> K` for unconditional
    /// autotraits. See [`crate::ident::FixedStr`] docstring for
    /// the full rationale of the `fn() -> K` phantom form.
    _kind: PhantomData<fn() -> K>,
}

impl<K: ReplyKind> ReplyId<K> {
    /// Construct a `ReplyId<K>` from a non-zero monotonic counter
    /// value.
    ///
    /// **Caller contract** (tier-2, audit-enforced): `value` must
    /// not have been used previously on the same `PgProtocol`
    /// instance. Reuse causes the protocol to deliver future replies
    /// to whichever sender is still registered under that ID — a
    /// logic error, not a memory-safety issue.
    ///
    /// The standard wrapper (`bsql-driver-postgres`) uses an
    /// `AtomicU64` initialised to 1 with `fetch_add(1, Relaxed)`.
    ///
    /// A fresh `ReplyId` starts with `delivered = false`; the
    /// Drop-guard will fire if it is dropped before
    /// [`ReplyId::consume`] is called.
    #[inline]
    pub const fn from_raw(value: NonZeroU64) -> Self {
        Self {
            value,
            delivered: false,
            _kind: PhantomData,
        }
    }

    /// Extract the underlying counter value, consuming the handle
    /// and marking the reply as delivered.
    ///
    /// After calling `consume`, the `ReplyId<K>` is gone — the raw
    /// value travels inside an outgoing `Action`, which is not
    /// consume-tracked.
    #[inline]
    pub fn consume(mut self) -> NonZeroU64 {
        self.delivered = true;
        self.value
        // Drop runs here with `delivered = true` → no panic.
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

impl<K: ReplyKind> Drop for ReplyId<K> {
    /// Tier-2 runtime consume-discipline guard (safety net).
    ///
    /// See the module-level docstring and DEF-101 analysis in
    /// deferred.md §16 for why this is the stable-Rust ceiling on
    /// "cannot drop unconsumed." Removing it would be a tier
    /// regression, not an elevation.
    ///
    /// # DEF-052 close — unwind-safe guard
    ///
    /// During a test-time unwind (test panics for some *unrelated*
    /// reason while a non-delivered ReplyId is alive), the guard
    /// returns early instead of double-panicking and masking the
    /// original failure with `SIGABRT`. Zero cost in production
    /// (`panic = "abort"` never unwinds).
    fn drop(&mut self) {
        #[cfg(test)]
        if std::thread::panicking() {
            return;
        }
        assert!(
            self.delivered,
            "ReplyId<{}>({}) dropped without delivery — the caller's oneshot receiver will never resolve",
            K::NAME,
            self.value.get(),
        );
    }
}

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

    /// Category (2) — tier-2 runtime invariant.
    ///
    /// Dropping a `ReplyId` without calling `.consume()` trips the
    /// Drop-guard. This is the load-bearing mechanism against the
    /// "silent reply loss" bug class.
    #[test]
    #[should_panic(expected = "dropped without delivery")]
    fn undelivered_drop_panics() {
        let raw = NonZeroU64::new(7).unwrap_or(NonZeroU64::MIN);
        let id: ReplyId<PingKind> = ReplyId::from_raw(raw);
        drop(id);
    }

    /// Category (2) — tier-2 runtime invariant (DEF-052 close).
    ///
    /// If a test panics for an *unrelated* reason while a
    /// non-delivered `ReplyId` is alive, the Drop-guard's
    /// `thread::panicking()` check returns early during unwinding,
    /// letting the original panic propagate cleanly.
    #[test]
    #[should_panic(expected = "unrelated panic")]
    fn unrelated_panic_while_reply_id_alive_surfaces_original_message() {
        let raw = NonZeroU64::new(11).unwrap_or(NonZeroU64::MIN);
        let id: ReplyId<PingKind> = ReplyId::from_raw(raw);
        let actual = id.get().get();
        assert_eq!(actual, 0, "unrelated panic (id was {actual})");
    }

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
}
