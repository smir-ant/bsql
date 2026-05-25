//! Witness-guard typestate for client-initiated push operations.
//!
//! The push surface (`push_command`, `push_bind_execute`,
//! `execute_prepared`) requires `state == Idle` to be safe. The
//! precondition is elevated to **tier-1** (compile-rejected) via a
//! [`ReadyGuard`] witness type: the only path to push is through a
//! `ReadyGuard`, which is constructible only via
//! [`crate::PgProtocol::as_ready`], which returns `Some(ReadyGuard)`
//! iff `state == Idle`.
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
/// # Compile-time invariants
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
/// match proto.push_command(PgCommand::Ping { reply }, &mut wb) {
///     Ok(_) | Err(_) => {}
/// }
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
/// match proto.push_bind_execute(
///     &PortalName::default(),
///     &StmtName::default(),
///     &(),
///     None,
///     FetchRows::All,
///     ReplyId::<QueryKind>::from_raw(NonZeroU64::MIN),
///     &mut wb,
/// ) {
///     Ok(_) | Err(_) => {}
/// }
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
///     match guard.push_command(PgCommand::Ping { reply }, &mut wb) {
///         Ok(_) | Err(_) => {}
///     }
///     // ERROR: `guard` was moved by the first push_command call
///     match guard.push_command(PgCommand::Ping { reply }, &mut wb) {
///         Ok(_) | Err(_) => {}
///     }
/// }
/// ```
#[derive(Debug)]
pub struct ReadyGuard<'a> {
    proto: &'a mut PgProtocol<crate::protocol::ActivePhase>,
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
    pub(crate) fn new(proto: &'a mut PgProtocol<crate::protocol::ActivePhase>) -> Self {
        Self { proto }
    }

    /// Send a generic a command from `push_command`.
    ///
    /// The guard's existence proves the precondition (`state == Idle`).
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
    /// # Zero-copy SQL
    ///
    /// SQL bodies are borrowed end-to-end via `SendBytesBorrowed` and
    /// surfaced as `Action::SendBytes(&[u8])` chunks in `OutActions` —
    /// no cap, no copy, no truncation. The per-command struct (`C`) is
    /// monomorphised; there is no by-value enum argument move.
    #[inline]
    pub fn push_command<'w, C: crate::push_command::PushCommand + 'w>(
        self,
        cmd: C,
        write_buf: &'w mut WriteBuf,
    ) -> Result<OutActions<'w>, PushFailure> {
        // The Idle precondition is re-checked inside
        // `push_command_internal` via `IdleState::try_from` (returns
        // `Option<IdleState<'_>>`). ReadyGuard's existence already
        // proves Idle via `as_ready`'s upstream classification; the
        // typestate's runtime check is belt-and-braces for in-crate
        // call sites that mint `IdleState` directly without going
        // through `as_ready`.
        self.proto.push_command_internal(cmd, write_buf)
    }

    /// Extended-Query Bind+Execute as an argument-list-shaped wrapper.
    ///
    /// Convenience for callers that prefer the positional-args shape;
    /// new callers should construct
    /// [`crate::push_command::BindExecute`] and call
    /// [`Self::push_command`] directly.
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
    ) -> Result<OutActions<'w>, PushFailure> {
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

    /// Execute a [`crate::prepared::PreparedQuery`] with typed
    /// arguments. The proc-macro emits a `const` of
    /// `PreparedQuery<P, R>`; this helper wraps the (q, args, fetch,
    /// reply) tuple into a [`crate::push_command::BindPrepared`] and
    /// routes through [`Self::push_command`] — the Idle precondition
    /// and typed post-state-install closures apply unchanged.
    ///
    /// Compared to [`Self::push_bind_execute`], the prepared path:
    /// - pays zero CPU on Parse + Bind-prefix header construction
    ///   (the macro baked the bytes at compile time → emitted as
    ///   `SendBytesBorrowed` / `SendBytesStatic` chunks);
    /// - skips the explicit `portal_name`/`stmt_name`/`row_desc`
    ///   arguments — all three are encoded in the prepared query
    ///   (`empty portal` + `q.stmt_name` + synthetic RowDesc built
    ///   from `q.row_oids`);
    /// - emits the static Execute frame (10 bytes,
    ///   `EXECUTE_EMPTY_PORTAL_NO_LIMIT`) instead of computing one.
    ///
    /// # Type parameters
    ///
    /// - `P`: parameter tuple type, sealed via
    ///   [`crate::params::ParamsWriter`].
    /// - `R`: row tuple type, sealed via
    ///   [`crate::prepared::RowDecode`].
    #[inline]
    pub fn execute_prepared<'w, P, R>(
        self,
        q: &'static crate::prepared::PreparedQuery<P, R>,
        args: P,
        fetch: FetchRows,
        reply: ReplyId<QueryKind>,
        write_buf: &'w mut WriteBuf,
    ) -> Result<OutActions<'w>, PushFailure>
    where
        P: ParamsWriter + 'w,
        R: crate::prepared::RowDecode + 'static,
    {
        self.push_command(
            crate::push_command::BindPrepared { q, args, fetch, reply },
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
/// `#[non_exhaustive]` pre-empts a SemVer footgun on future variant
/// additions; downstream `match`es against `ConnectionStatus` from
/// outside the crate must include a wildcard arm. Internal `match`es
/// here remain exhaustive (per the CREDO §0 rule — the wildcard is
/// forbidden inside the crate, but `non_exhaustive` allows external
/// recovery).
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
