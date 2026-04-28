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

use crate::action::OutActions;
use crate::command::{FetchRows, PgCommand};
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
#[derive(Debug)]
pub(crate) struct IdleStateProof(());

impl IdleStateProof {
    /// Sealed within `mod guard` — only `ReadyGuard::push_*` paths
    /// reach this constructor (which itself is reachable only after
    /// `PgProtocol::as_ready` verified the state is Idle).
    ///
    /// Crate-internal callers that need the proof MUST go through
    /// `ReadyGuard`; there is no other path.
    #[inline]
    const fn new() -> Self {
        Self(())
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
    /// (`state == Idle`), so the returned `OutActions` is guaranteed
    /// to contain only `SendBytes`/`SendBytesStatic`/`SendBytesRange`
    /// success-path actions (no `FailReply` from a state-mismatch
    /// failure).
    ///
    /// `OutActions` may still contain a single `FailReply` if the
    /// command's *body construction* fails (e.g., SCRAM build error
    /// for `Startup`); see `compute_push_startup` decision table.
    /// That is a separate, tier-3 server-protocol-level error, not
    /// a state-precondition violation.
    #[must_use = "the returned actions carry side-effects that must be executed by the caller's I/O layer"]
    #[inline]
    pub fn push_command<'w>(
        self,
        cmd: PgCommand,
        write_buf: &'w mut WriteBuf,
    ) -> OutActions<'w, 'a> {
        // DEF-198 ext: synthesise the Idle-state witness here.
        // `IdleStateProof::new()` is reachable only inside `mod guard`,
        // and the guard's existence (acquired via `as_ready`) statically
        // proves the precondition.
        self.proto.push_command_internal(cmd, write_buf, IdleStateProof::new())
    }

    /// Extended-Query Bind+Execute pipeline.
    ///
    /// Tier-1 elevation of the pre-DEF-198 `PgProtocol::push_bind_execute`
    /// public method.
    #[expect(
        clippy::too_many_arguments,
        reason = "push_bind_execute mirrors the PG Bind+Execute wire contract 1:1; ReadyGuard wrapper preserves the same arg count by design"
    )]
    #[must_use = "the returned actions carry side-effects that must be executed by the caller's I/O layer"]
    #[inline]
    pub fn push_bind_execute<'w, P: ParamsWriter>(
        self,
        portal_name: &PortalName,
        stmt_name: &StmtName,
        params: &P,
        row_desc: Option<RowDesc>,
        fetch: FetchRows,
        reply: ReplyId<QueryKind>,
        write_buf: &'w mut WriteBuf,
    ) -> OutActions<'w, 'a> {
        self.proto.push_bind_execute_internal(
            portal_name,
            stmt_name,
            params,
            row_desc,
            fetch,
            reply,
            write_buf,
            IdleStateProof::new(),
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
