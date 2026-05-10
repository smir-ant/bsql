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

use crate::action::{OutActions, PushFailure};
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

// DEF-272 cluster γ (2026-05-10): `IdleStateProof` deleted; replaced
// by [`crate::state_setter::IdleState<'a>`] lifetime-bound typestate.
// The pre-γ proof was a ZST with `pub(crate) const fn new()` — anyone
// in-crate could mint regardless of state, then pair with non-Idle
// `&mut state` (zombie-reply class). Post-γ the typestate IS the
// state borrow + the Idle proof, inseparable; pairing-with-different-
// state is impossible by lifetime ownership. See `mod state_setter`
// (cluster γ block) for the typestate.

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
    /// - `Ok(OutActions)` — caller drains the per-chunk action list to
    ///   the socket. The frame is the ordered concatenation of
    ///   `SendBytes(&[u8])` chunks: (1) header range (in `write_buf`),
    ///   (2) borrowed SQL (zero-copy from caller memory —
    ///   `Parse<'a>::sql` / `SimpleQuery<'a>::sql`), (3) trailer range
    ///   (in `write_buf`), (4) `&'static` Sync trailer if applicable.
    ///   Under `writev` / `IoSlice` the chunks collapse to a single
    ///   socket syscall. Commands with no caller-side payload (`Ping`,
    ///   `BindExecute`, etc.) return a single-chunk `OutActions`.
    /// - `Err(PushFailure { id, cause })` — the command's body
    ///   construction failed (e.g., SCRAM build error for `Startup`).
    ///   State has transitioned to `Errored`; caller resolves user's
    ///   oneshot via `id` + `cause` and closes the socket per the
    ///   [`PushFailure`] `#[must_use]` contract. The state-precondition
    ///   path (state ≠ Idle) is NOT reachable through this API —
    ///   `ReadyGuard` proves Idle at construction.
    ///
    /// # DEF-160 Z2 (2026-05-11)
    ///
    /// Pre-DEF-160 returned `Result<(), PushFailure>` (~80 B); caller
    /// drained `write_buf.as_bytes()` after a successful push, with the
    /// SQL copied into `write_buf` (cap-bounded by `MAX_SQL_LEN`). The
    /// fixed cap silently truncated colossal queries — a tier-3 hazard
    /// (DEF-218 colossal-data audit). Post-DEF-160 the SQL is borrowed
    /// end-to-end via `SendBytesBorrowed` and surfaced as
    /// `Action::SendBytes(&[u8])` chunks in `OutActions` — no cap, no
    /// copy, no truncation. `OutActions` retains the pre-DEF-212 fail
    /// classification (`FailReply` arm) via the `Result::Err` short-circuit.
    /// DEF-269 v2 (T): generic over `C: PushCommand`. Caller passes a
    /// per-command struct (e.g. [`crate::push_command::Ping`]) instead
    /// of a `PgCommand` enum value. Each `C` is monomorphised — the
    /// 2176-B-by-value PgCommand argument move is gone.
    #[inline]
    pub fn push_command<'w, C: crate::push_command::PushCommand + 'w>(
        self,
        cmd: C,
        write_buf: &'w mut WriteBuf,
    ) -> Result<OutActions<'w, 'static>, PushFailure> {
        // DEF-272 cluster γ (2026-05-10): the Idle precondition is
        // re-checked inside `push_command_internal` via
        // `IdleState::try_from` (returns `Option<IdleState<'_>>`).
        // ReadyGuard's existence still proves Idle via `as_ready`'s
        // upstream classification; the typestate's runtime check is
        // belt-and-braces (build-time tier-1 closure across in-crate
        // call sites).
        self.proto.push_command_internal(cmd, write_buf)
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
    pub fn push_bind_execute<'w, P: ParamsWriter + 'w>(
        self,
        portal_name: &'w PortalName,
        stmt_name: &'w StmtName,
        params: &'w P,
        row_desc: Option<RowDesc>,
        fetch: FetchRows,
        reply: ReplyId<QueryKind>,
        write_buf: &'w mut WriteBuf,
    ) -> Result<OutActions<'w, 'static>, PushFailure> {
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
