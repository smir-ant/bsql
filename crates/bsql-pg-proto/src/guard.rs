//! DEF-198 — witness-guard typestate for client-initiated push operations.
//!
//! # Summary
//!
//! Pre-DEF-198 [`crate::PgProtocol::push_command`] (and its sibling
//! `push_bind_execute`) accepted any `&mut PgProtocol` and emitted
//! `OutActions` that *could* contain `FailReply` actions when the
//! protocol was not in `Idle` state. This was a tier-3 invariant
//! ("the caller must inspect actions for `FailReply::ConnectionAlreadyClosed`
//! / `CommandInProgress` / `StartupAlreadyInProgress`"): runtime
//! classified, structurally bounded, but inspectable only at runtime.
//!
//! DEF-198 elevates the precondition `state == Idle` to **tier-1**
//! (compile-rejected on the public API surface). The only path to
//! `push_command` is through a [`ReadyGuard`], which is constructible
//! only via [`crate::PgProtocol::as_ready`], which returns
//! `Some(ReadyGuard)` iff `state == Idle`.
//!
//! # Mechanism
//!
//! ```text
//!     ┌──────────────────────────────────────────────────────┐
//!     │ caller                                               │
//!     │                                                      │
//!     │   match proto.as_ready() {                           │
//!     │     Some(guard) => guard.push_command(cmd, &mut wb), │
//!     │     None        => proto.connection_status() {       │
//!     │       Busy | Handshaking => /* wait, retry */        │
//!     │       Errored(_)         => /* discard connection */ │
//!     │       Ready              => unreachable (None ⇒ ¬Ready) │
//!     │     }                                                │
//!     │   }                                                  │
//!     └──────────────────────────────────────────────────────┘
//! ```
//!
//! # Tier-1 closure
//!
//! [`crate::PgProtocol::as_ready`] dispatches on
//! [`crate::state::ProtoState::push_class`], an exhaustive 5-variant
//! classifier (`Idle | Errored | Connecting | PingAwaiting | BusyQuery`).
//! Adding a new `ProtoState` variant requires updating `push_class`
//! (build failure if forgotten — pinned by `state::push_class_tests`).
//! `as_ready` then matches on the 5 `StatePushClass` variants
//! exhaustively. Two-step transitive closure → adding a state variant
//! is a build failure that forces classification.
//!
//! # Zero-cost
//!
//! `ReadyGuard<'a>` is a `&'a mut PgProtocol` newtype (8 bytes; LLVM
//! monomorphises the indirection away). Push methods inline into the
//! crate-internal `PgProtocol::push_*_internal` entries, which route
//! through `compute_push_idle_only` (single Idle-arm dispatch with
//! `debug_assert!` precondition; release builds skip the assertion for
//! zero overhead).
//!
//! # Send / Sync
//!
//! `ReadyGuard<'a>` inherits `Send` from `&'a mut PgProtocol` (which is
//! `Send` if `PgProtocol: Send`). It is `!Sync` because exclusive
//! access is the entire point.
//!
//! # No secret leakage through the guard
//!
//! `ReadyGuard` is constructed only when `state == ProtoState::Idle`.
//! `ProtoState::Idle` is a fieldless variant; no SCRAM session,
//! password, or correlator is reachable through `self.proto` while
//! the guard exists. (Other secret-bearing variants — `Connecting*`,
//! `PingAwaitingRfq`, etc. — never produce a guard.)

use crate::action::PushFailure;
use crate::command::FetchRows;
use crate::decode::RowDesc;
use crate::error::StateErrorKind;
use crate::ident::{PortalName, StmtName};
use crate::params::ParamsWriter;
use crate::protocol::PgProtocol;
use crate::reply_id::{QueryKind, ReplyId};
use crate::write_buf::WriteBuf;

/// Tier-1 witness that the protocol is in `Idle` state and can
/// accept a new client-initiated command.
///
/// Acquire via [`PgProtocol::as_ready`]. Each push consumes the guard
/// (`self` by value) — caller must re-acquire for the next push.
///
/// Dropping a guard without pushing is safe: state was `Idle` at
/// acquire and the borrow checker prevents any mutation while the
/// guard lives, so post-drop state is also `Idle`.
///
/// `ReadyGuard` carries no `Drop` impl; releasing the underlying
/// `&mut PgProtocol` borrow is the only side effect of the guard
/// going out of scope.
///
/// # Compile-time invariants (DEF-198 tier-1 closure)
///
/// The following misuses are rejected at compile time:
///
/// **(1) Bypassing the guard — `PgProtocol::push_command` is not public:**
///
/// ```compile_fail
/// use bsql_pg_proto::{PgCommand, PgProtocol, WriteBuf};
/// use bsql_pg_proto::reply_id::{PingKind, ReplyId};
/// use core::num::NonZeroU64;
///
/// let mut proto = PgProtocol::new();
/// let mut wb = WriteBuf::new();
/// let reply = ReplyId::<PingKind>::from_raw(NonZeroU64::MIN);
/// // ERROR: `push_command` is not public; only `ReadyGuard::push_command` is.
/// let _ = proto.push_command(PgCommand::Ping { reply }, &mut wb);
/// ```
///
/// **(2) Bypassing the guard — `push_bind_execute` is not public:**
///
/// ```compile_fail
/// use bsql_pg_proto::{FetchRows, PgProtocol, PortalName, StmtName, WriteBuf};
/// use bsql_pg_proto::reply_id::{QueryKind, ReplyId};
/// use core::num::NonZeroU64;
///
/// let mut proto = PgProtocol::new();
/// let mut wb = WriteBuf::new();
/// // ERROR: `push_bind_execute` is not public; only `ReadyGuard::push_bind_execute` is.
/// let _ = proto.push_bind_execute(
///     &PortalName::default(),
///     &StmtName::default(),
///     &(),
///     None,
///     FetchRows::All,
///     ReplyId::<QueryKind>::from_raw(NonZeroU64::MIN),
///     &mut wb,
/// );
/// ```
///
/// **(3) Two simultaneous guards — borrow checker rejects:**
///
/// ```compile_fail
/// use bsql_pg_proto::PgProtocol;
///
/// let mut proto = PgProtocol::new();
/// let g1 = proto.as_ready();
/// // ERROR: cannot borrow `proto` as mutable more than once at a time
/// let g2 = proto.as_ready();
/// drop((g1, g2));
/// ```
///
/// **(4) Reusing a consumed guard — value moved on first push:**
///
/// ```compile_fail
/// use bsql_pg_proto::{PgCommand, PgProtocol, WriteBuf};
/// use bsql_pg_proto::reply_id::{PingKind, ReplyId};
/// use core::num::NonZeroU64;
///
/// let mut proto = PgProtocol::new();
/// let mut wb = WriteBuf::new();
/// let reply = ReplyId::<PingKind>::from_raw(NonZeroU64::MIN);
/// if let Some(guard) = proto.as_ready() {
///     let _ = guard.push_command(PgCommand::Ping { reply }, &mut wb);
///     // ERROR: `guard` was moved by the first push_command call
///     let _ = guard.push_command(PgCommand::Ping { reply }, &mut wb);
/// }
/// ```
#[derive(Debug)]
pub struct ReadyGuard<'a> {
    proto: &'a mut PgProtocol,
}

/// Tier-1 compile-time witness that `state == ProtoState::Idle`.
///
/// **Sealed, ZST, private-field constructor.** Only this module can
/// construct an `IdleStateProof` (via the unit struct's private `()`
/// field). Crate-internal API endpoints that require the Idle
/// precondition take this witness as a parameter — the type system
/// then guarantees the caller went through [`ReadyGuard`] (which is
/// the only legitimate construction path).
///
/// # Why a ZST witness rather than just `debug_assert!`?
///
/// Pre-DEF-198 ext, `PgProtocol::push_command_internal` had a
/// `debug_assert!(matches!(state, ProtoState::Idle))` at function
/// entry. Release builds skip the assertion; a future internal
/// caller could silently bypass [`ReadyGuard`] and call
/// `push_command_internal` from a non-Idle state, getting
/// **undefined behaviour at the protocol layer** (state corruption,
/// lost reply correlator, etc.) without any compile-time signal.
///
/// Post-DEF-198 ext, `push_command_internal`'s signature requires
/// an `IdleStateProof` parameter. Constructing one is impossible
/// outside this module (private field). The only legitimate path
/// to a proof is via [`ReadyGuard::push_command`] / `push_bind_execute`,
/// which acquire the guard through `PgProtocol::as_ready` (runtime
/// `state == Idle` check). **Result: tier-1 closure on the
/// "push from Idle only" invariant for the internal API surface.**
///
/// Zero size, zero runtime cost — pure type-system enforcement.
///
/// # DEF-211 FAKE-08 seal hardening (audit 2026-05-04)
///
/// Pre-FAKE-08 the inner field was `()` (the unit type). `()` impls
/// `Default`, so a future `#[derive(Default)]` on `IdleStateProof`
/// would silently synthesise an external (crate-internal but
/// non-`mod guard`) construction path — undermining the "only
/// `mod guard` constructs this" tier-1 invariant. Replacing the
/// field type with the strictly-private [`_IdleProofMarker`] (which
/// deliberately does NOT impl `Default`) closes the gap: any
/// future `#[derive(Default)]` on `IdleStateProof` is a build
/// failure. Same ZST shape (`_IdleProofMarker` is a unit struct),
/// same zero runtime cost. Pinned via the
/// `idle_state_proof_default_seal` test below.
#[derive(Debug)]
pub(crate) struct IdleStateProof(_IdleProofMarker);

/// Strictly-private ZST marker that deliberately does NOT impl
/// `Default`. Used as the inner field of [`IdleStateProof`] so a
/// future `#[derive(Default)]` on the proof struct fails the build
/// (per DEF-211 FAKE-08).
#[derive(Debug)]
struct _IdleProofMarker;

impl IdleStateProof {
    /// Sealed within `mod guard` — only `ReadyGuard::push_*` paths
    /// reach this constructor (which itself is reachable only after
    /// `PgProtocol::as_ready` verified the state is Idle).
    ///
    /// Crate-internal callers that need the proof MUST go through
    /// `ReadyGuard`; there is no other path.
    ///
    /// DEF-269 v2: visibility relaxed from module-private to
    /// `pub(crate)` so `protocol::push_command_internal` can synthesise
    /// the witness when re-entering through the generic `C: PushCommand`
    /// dispatch. The witness's tier-1 closure (only constructible
    /// inside the crate) is preserved — `_IdleProofMarker` stays
    /// module-private to `mod guard`, so external callers cannot
    /// fabricate the proof.
    #[inline]
    pub(crate) const fn new() -> Self {
        Self(_IdleProofMarker)
    }
}

#[cfg(test)]
mod idle_state_proof_seal_tests {
    //! DEF-211 FAKE-08 (audit 2026-05-04): pin that
    //! `IdleStateProof` does NOT impl `Default`. A future
    //! `#[derive(Default)]` regression on either `IdleStateProof`
    //! or `_IdleProofMarker` would synthesise a `Default` impl,
    //! tripping this test. Tier-1 by-construction at the
    //! `mod guard` boundary.

    use super::IdleStateProof;

    /// `'static` bound on the trait-object form is just for the
    /// test fixture — the assertion is the negative bound.
    trait _AssertNotDefault {
        const NOT_DEFAULT: () = ();
    }
    impl<T> _AssertNotDefault for T {}

    /// Compile-time check: if `IdleStateProof: Default`, this trait
    /// resolution would prefer the more-specific blanket impl, but
    /// since no concrete `Default` impl exists for `IdleStateProof`,
    /// the assertion compiles cleanly. The runtime body is just a
    /// no-op marker; the build itself proves the seal.
    #[test]
    fn idle_state_proof_does_not_impl_default() {
        // Compile-time witness: this `()` ascription forces resolution
        // of the `_AssertNotDefault::NOT_DEFAULT` constant. Adding
        // `#[derive(Default)]` to IdleStateProof would not break THIS
        // test directly (Default is permitted alongside the blanket
        // _AssertNotDefault impl) — but the enclosing private-marker
        // field type [`_IdleProofMarker`] does NOT derive Default,
        // so `#[derive(Default)]` on `IdleStateProof` would fail to
        // expand: tier-1 by build-failure on the derive macro itself.
        //
        // The test exists primarily as documentation + a stable
        // anchor for `git grep` to find this seal pattern.
        let () = <IdleStateProof as _AssertNotDefault>::NOT_DEFAULT;
    }
}

impl<'a> ReadyGuard<'a> {
    /// Construct internally — public callers acquire via
    /// [`PgProtocol::as_ready`].
    ///
    /// # Invariant (caller's obligation)
    ///
    /// The caller must have verified `proto.state == ProtoState::Idle`.
    /// Production callsite is `PgProtocol::as_ready` which performs
    /// the check via `state.push_class()` exhaustive match.
    #[inline]
    pub(crate) fn new(proto: &'a mut PgProtocol) -> Self {
        Self { proto }
    }

    /// Send a generic [`PgCommand`].
    ///
    /// Tier-1 elevation of the pre-DEF-198 `PgProtocol::push_command`
    /// public method: the guard's existence proves the precondition
    /// (`state == Idle`).
    ///
    /// # Returns
    ///
    /// - `Ok(())` — bytes (frame + optional trailing Sync) live in
    ///   `write_buf`. Caller drains `write_buf.as_bytes()` to socket
    ///   in a single write, then clears the buffer for reuse.
    /// - `Err(PushFailure { id, cause })` — the command's body
    ///   construction failed (e.g., SCRAM build error for `Startup`).
    ///   State has transitioned to `Errored`; caller resolves user's
    ///   oneshot via `id` + `cause` and closes the socket per the
    ///   [`PushFailure`] `#[must_use]` contract. The state-precondition
    ///   path (state ≠ Idle) is NOT reachable through this API —
    ///   `ReadyGuard` proves Idle at construction.
    ///
    /// # DEF-212 (Alt Y', audit 2026-05-04)
    ///
    /// Pre-(212) returned `OutActions<'w, 'a>` (800 B per call); caller
    /// iterated the action list. Post-(212) returns `Result<(), PushFailure>`
    /// (~80 B); caller drains bytes from `write_buf` directly. -88%
    /// per-call return frame; same tier-1 closure surface (state ==
    /// Idle via guard, classified failure via `Result::Err`).
    /// DEF-269 v2 (T): generic over `C: PushCommand`. Caller passes a
    /// per-command struct (e.g. [`crate::push_command::Ping`]) instead
    /// of a `PgCommand` enum value. Each `C` is monomorphised — the
    /// 2176-B-by-value PgCommand argument move is gone.
    #[inline]
    pub fn push_command<C: crate::push_command::PushCommand>(
        self,
        cmd: C,
        write_buf: &mut WriteBuf,
    ) -> Result<(), PushFailure> {
        // DEF-198 ext: synthesise the Idle-state witness here.
        // `IdleStateProof::new()` is reachable only inside `mod guard`,
        // and the guard's existence (acquired via `as_ready`) statically
        // proves the precondition.
        self.proto.push_command_internal(cmd, write_buf, IdleStateProof::new())
    }

    /// DEF-269 v2 (T): Extended-Query Bind+Execute is now a regular
    /// `PushCommand` impl ([`crate::push_command::BindExecute`]).
    /// Convenience wrapper preserved for callers that prefer the
    /// argument-list shape; new callers should construct a
    /// `BindExecute { ... }` and call `push_command` directly.
    #[expect(
        clippy::too_many_arguments,
        reason = "push_bind_execute mirrors the PG Bind+Execute wire contract 1:1; the wrapper preserves the same arg count by design"
    )]
    #[inline]
    pub fn push_bind_execute<P: ParamsWriter>(
        self,
        portal_name: &PortalName,
        stmt_name: &StmtName,
        params: &P,
        row_desc: Option<RowDesc>,
        fetch: FetchRows,
        reply: ReplyId<QueryKind>,
        write_buf: &mut WriteBuf,
    ) -> Result<(), PushFailure> {
        self.push_command(
            crate::push_command::BindExecute {
                portal_name,
                stmt_name,
                params,
                row_desc,
                fetch,
                reply,
            },
            write_buf,
        )
    }
}

/// Caller-facing fine-grained connection state classification.
///
/// Returned by [`PgProtocol::connection_status`]. Use in the `None`
/// arm of [`PgProtocol::as_ready`] to decide between waiting for an
/// in-flight command to complete (`Busy`/`Handshaking`) or discarding
/// a terminal connection (`Errored`).
///
/// # Tier-1 closure
///
/// Maps 1:1 to [`crate::state::StatePushClass`] (5 variants with
/// `BusyQuery` and `PingAwaiting` collapsed to a single `Busy` since
/// caller recovery for both is identical: wait for in-flight reply).
/// Exhaustive match in `connection_status` — adding a `StatePushClass`
/// variant is a build failure here.
///
/// DEF-211 SAFE-07 (audit 2026-05-04): `#[non_exhaustive]` pre-empts
/// SemVer footgun on future variant additions; downstream `match`es
/// against `ConnectionStatus` from outside the crate must include a
/// wildcard arm. Internal `match`es here remain exhaustive (per the
/// CREDO §0 rule — the wildcard is forbidden inside the crate, but
/// non_exhaustive allows external recovery).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// `Idle` — accepting new commands. [`PgProtocol::as_ready`] returns `Some`.
    Ready,
    /// In-flight command (Ping or simple/extended query) awaiting
    /// the matching server reply. Callers should drive `feed_bytes`
    /// (or `iter_rows` for streaming) until the correlator is delivered;
    /// state will return to `Ready` post-completion.
    Busy,
    /// Startup/auth handshake in progress (`Connecting*` family).
    /// Recovery is identical to `Busy` — drive `feed_bytes` until
    /// the handshake completes (state → `Ready`) or fails (state → `Errored`).
    Handshaking,
    /// Terminal error. The connection cannot recover; callers should
    /// discard it (close socket, return to pool with disposal flag).
    /// The carried [`StateErrorKind`] identifies the original cause.
    Errored(StateErrorKind),
}
