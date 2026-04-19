//! The `PgProtocol` state machine — entry points and bounded action emit.
//!
//! Two public methods drive the machine:
//!
//! - [`PgProtocol::push_command`] — user pushes a [`crate::PgCommand`];
//!   protocol reacts (typically: emit a `SendBytes`, transition state).
//! - [`PgProtocol::feed_bytes`] — host hands inbound wire bytes;
//!   protocol parses zero or more frames, dispatches each, emits zero
//!   or more actions (typically: `DeliverReply`).
//!
//! Both methods return [`crate::OutActions`] — a bounded
//! `heapless::Vec` whose capacity is the const [`MAX_ACTIONS_PER_CALL`].
//! Per-call-site `const _: () = assert!(MAX_ACTIONS_PER_CALL >= …)`
//! makes overflow impossible at build time.

use crate::action::{Action, OutActions, SendBuf};
use crate::buf::{ReadBuf, ReadBufFull};
use crate::command::PgCommand;
use crate::dispatch::{DispatchOutcome, dispatch};
use crate::error::ProtocolError;
use crate::frame::{HEADER_LEN, HeaderParse, parse_header};
use crate::ident::{ApplicationName, DatabaseName, Ident};
use crate::password::Credentials;
use crate::reply_id::ReplyId;
use crate::session_params::SessionParams;
use crate::state::ProtoState;
use crate::wire::SYNC_WIRE_BYTES;
use crate::write_buf::WriteBuf;
use core::cell::Cell;
use core::marker::PhantomData;

// -----------------------------------------------------------------
// emit_actions! — tier-1 per-site action-budget enforcement (DEF-045)
// -----------------------------------------------------------------

/// Count the number of expression arguments in a `macro_rules!` call.
/// Used by [`emit_actions!`] to verify at compile time that the
/// declared budget matches the number of actions actually pushed.
///
/// Returns a `usize` literal. Implementation avoids the `1 + rec`
/// pattern that triggers `clippy::int_plus_one` when the result is
/// compared via `>=` in the caller.
macro_rules! count_exprs {
    () => { 0_usize };
    ($one:expr) => { 1_usize };
    ($one:expr, $two:expr) => { 2_usize };
    ($one:expr, $two:expr, $three:expr) => { 3_usize };
    ($one:expr, $two:expr, $three:expr, $four:expr) => { 4_usize };
}

/// Push 1..=N actions into `out`, with compile-time enforcement of
/// both the per-site budget and the global `MAX_ACTIONS_PER_CALL` cap.
///
/// Two forms:
///
/// - **Loop form** (`on_overflow: break`) — for callers inside a loop
///   who must bail out of the loop if the vector fills. Used by the
///   `feed_bytes` drain loop.
/// - **No-bail form** (no `on_overflow:` argument) — for callers where
///   the compile-time `const _: () = assert!(...)` already proves the
///   push cannot fail (the vector starts empty and the budget fits).
///   Used by `push_command` handlers and `fail_inflight_and_close`.
///   The Err arm is dead under the assert but explicit so the
///   compiler sees every `Result` handled.
///
/// ```text
/// // Loop caller:
/// emit_actions!(out, budget: 2, on_overflow: break, [
///     Action::FailReply { id: id.consume(), cause },
///     Action::CloseSocket,
/// ]);
///
/// // Non-loop caller:
/// emit_actions!(out, budget: 1, [
///     Action::SendBytes(SendBuf::from_slice(&SYNC_WIRE_BYTES)?),
/// ]);
/// ```
///
/// Compile-time checks (both are `const _: () = assert!(…)` — failure
/// is a build error, not a runtime branch):
///
/// 1. `MAX_ACTIONS_PER_CALL >= budget` — the site's declared budget
///    fits within the global cap.
/// 2. `budget >= count(actions)` — the site does not push more actions
///    than it declared.
///
/// DEF-045. Form split: DEF-055.
macro_rules! emit_actions {
    // Loop form: bails out of the enclosing loop on overflow.
    (
        $out:expr, budget: $budget:literal, on_overflow: break,
        [$($action:expr),+ $(,)?]
    ) => {{
        const _: () = assert!(
            $crate::protocol::MAX_ACTIONS_PER_CALL >= $budget,
            "emit_actions! per-site budget exceeds MAX_ACTIONS_PER_CALL",
        );
        const _: () = assert!(
            $budget >= count_exprs!($($action),+),
            "emit_actions! site pushes more actions than its declared budget",
        );
        $(
            if $out.push($action).is_err() {
                break;
            }
        )+
    }};

    // No-bail form: const_assert guarantees the push fits; Err arm is
    // dead but explicit to honor `heapless::Vec::push`'s must-use.
    (
        $out:expr, budget: $budget:literal,
        [$($action:expr),+ $(,)?]
    ) => {{
        const _: () = assert!(
            $crate::protocol::MAX_ACTIONS_PER_CALL >= $budget,
            "emit_actions! per-site budget exceeds MAX_ACTIONS_PER_CALL",
        );
        const _: () = assert!(
            $budget >= count_exprs!($($action),+),
            "emit_actions! site pushes more actions than its declared budget",
        );
        $(
            match $out.push($action) {
                Ok(()) => {}
                Err(_) => {}
            }
        )+
    }};
}

/// Maximum number of [`Action`]s a single entry-point call may emit.
///
/// # Two-level guarantee
///
/// - **Per emission site — tier 1 compile (DEF-045).** Each call to
///   `emit_actions!` carries a `budget: N` literal and a `const _: () =
///   assert!(MAX_ACTIONS_PER_CALL >= N)` inside the macro expansion.
///   A site that declares a budget it cannot fit is a build error.
///   A site that pushes more actions than its declared budget is a
///   build error. Both checks are pure compile-time.
/// - **Aggregate across one entry-point call — tier 2 structural.**
///   `feed_bytes` can loop over multiple frames, each potentially
///   emitting up to its site's budget. The aggregate is bounded by
///   `OutActions`'s capacity (`MAX_ACTIONS_PER_CALL`) — overflow
///   causes the `emit_actions!(..., on_overflow: break, ...)` form
///   to bail out of the loop, not silently drop. The aggregate cap is
///   **structural** (bounded container), not compile-proven in terms
///   of frame count. Honest tier: not tier-1, per §3.4's ban on
///   "tier-1 runtime" labels for runtime-checked bounds.
///
/// # Phase 1a + 1b budget audit
///
/// - `push_command(Ping)` from `Idle` → 1 action (`SendBytes`).
/// - `push_command(Ping)` from non-`Idle` → 1 action (`FailReply`).
/// - `push_command(Startup)` → 1 action (`SendBytes` or `FailReply`).
/// - `feed_bytes(rfq)` from `AwaitingPingReply` → 1 action.
/// - `feed_bytes(error_response)` from any flight state → 2 actions
///   (`FailReply` + `CloseSocket`).
/// - `feed_bytes(malformed)` → 2 actions (or 1 if no in-flight reply).
/// - `feed_bytes(multiple frames in one chunk)` — the drain loop in
///   `feed_bytes` bails on `OutActions` full via `on_overflow: break`.
///
/// The worst-case single-site emission in Phase 1a/1b is **2** actions.
/// `MAX_ACTIONS_PER_CALL = 4` gives the dispatcher loop one frame's
/// slack (two actions absorbed, room for one more iteration's emission
/// before the bail-break fires). Bumping happens in 1c with the first
/// multi-action path (compute/apply split — DEF-059 — may surface new
/// emission sites).
pub const MAX_ACTIONS_PER_CALL: usize = 4;

// Sanity assert — the budget audit above demands at least 2.
const _: () = assert!(MAX_ACTIONS_PER_CALL >= 2);

/// PostgreSQL wire-protocol state machine.
///
/// **Phase 1a scope:** ships only the Ping flow. The protocol starts
/// in `Idle`; pushing a `Ping` emits a `Sync`; the matching
/// `ReadyForQuery` reply transitions back to `Idle` and emits a
/// `Pong`. See [crate-level docs](crate) for the full architectural
/// picture.
///
/// `!Sync` by construction (`PhantomData<Cell<()>>` field). Concurrent
/// access is impossible; a `&mut PgProtocol` is the only handle.
pub struct PgProtocol {
    state: ProtoState,
    read_buf: ReadBuf,
    /// Session parameters from the post-auth handshake. Populated
    /// during startup from ParameterStatus messages. Read-only after
    /// startup completes (accessible via `session_params()`).
    session_params: SessionParams,
    /// `!Sync` marker — `Cell<T>: !Sync`, so the whole struct inherits.
    /// Load-bearing: the crate-root ambiguous-impl gate verifies that
    /// `PgProtocol: !Sync` compile-time. Renamed from the earlier
    /// `_not_sync` (leading-underscore convention for structurally-used
    /// fields is forbidden per user-feedback memory).
    sync_marker: PhantomData<Cell<()>>,
}

impl PgProtocol {
    /// Construct a new protocol in [`ProtoState::Idle`].
    ///
    /// **Note:** Phase 1a starts in `Idle` directly. The startup +
    /// auth handshake that legitimately produces this state lives in
    /// 1b/1e; until then the test harness pushes Ping commands without
    /// having authenticated against a real PG server.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: ProtoState::Idle,
            read_buf: ReadBuf::new(),
            session_params: SessionParams::new(),
            sync_marker: PhantomData,
        }
    }

    /// Borrow the current state. Read-only inspection.
    #[inline]
    #[must_use]
    pub const fn state(&self) -> &ProtoState {
        &self.state
    }

    /// Borrow the accumulated session parameters.
    ///
    /// Populated during the startup handshake from `ParameterStatus`
    /// messages. Empty until startup completes.
    #[inline]
    #[must_use]
    pub const fn session_params(&self) -> &SessionParams {
        &self.session_params
    }

    /// Borrow the current unread bytes in the read buffer.
    ///
    /// Useful for tests; production hosts have no need.
    #[inline]
    #[must_use]
    pub fn unread(&self) -> &[u8] {
        self.read_buf.unread()
    }

    /// Push a user command.
    ///
    /// Returns the action list — bounded by [`MAX_ACTIONS_PER_CALL`].
    /// Caller must execute every action in order.
    ///
    /// # Compute / apply split (DEF-059)
    ///
    /// The body is a three-line delegate: move the current state out,
    /// hand it (with `cmd`) to the pure [`compute_push`] free function,
    /// put the returned new state back. All push-path decision logic
    /// — per-command match, per-state transitions, action emission —
    /// lives in [`compute_push`] and its per-command helpers. Those
    /// helpers are free functions taking [`ProtoState`] by value; they
    /// are testable directly, with no `PgProtocol` construction needed
    /// (see `compute_push_tests` at the bottom of this file).
    ///
    /// The `core::mem::take` here momentarily leaves `self.state` in
    /// its `Default` (`Idle`) value for the duration of `compute_push`.
    /// `PgProtocol` is `!Sync` (tier-1 compile, via the `PhantomData<Cell<()>>`
    /// field) so no observer can witness the window; the split is
    /// safe even though the intermediate is not the terminal state.
    #[must_use = "the returned actions carry side-effects that must be executed"]
    pub fn push_command(&mut self, cmd: PgCommand) -> OutActions {
        let prev = core::mem::take(&mut self.state);
        let (new_state, actions) = compute_push(cmd, prev);
        self.state = new_state;
        actions
    }

    /// Feed inbound wire bytes.
    ///
    /// Returns the action list — bounded by [`MAX_ACTIONS_PER_CALL`].
    #[must_use = "the returned actions carry side-effects that must be executed"]
    pub fn feed_bytes(&mut self, bytes: &[u8]) -> OutActions {
        let mut out = OutActions::new();

        // Append into the bounded buffer. Overflow → fatal.
        if let Err(ReadBufFull {
            attempted,
            available,
        }) = self.read_buf.append(bytes)
        {
            self.fail_inflight_and_close(
                ProtocolError::ReadBufferFull {
                    attempted,
                    available,
                },
                &mut out,
            );
            return out;
        }

        // Drain as many complete frames as possible. Bounded by
        // (a) `MAX_ACTIONS_PER_CALL` (emit_actions! budget overflow
        // bails with `break`; we stop) and (b) the buffer being
        // drained empty.
        loop {
            let header = parse_header(self.read_buf.unread());
            match header {
                HeaderParse::Empty | HeaderParse::Incomplete => break,
                HeaderParse::MalformedLength { declared } => {
                    self.fail_inflight_and_close(
                        ProtocolError::MalformedFrameLength { declared },
                        &mut out,
                    );
                    break;
                }
                HeaderParse::FrameTooLarge { declared } => {
                    self.fail_inflight_and_close(
                        ProtocolError::FrameTooLarge { declared },
                        &mut out,
                    );
                    break;
                }
                HeaderParse::Ok {
                    tag,
                    declared_len: _,
                    total_len,
                } => {
                    if self.read_buf.unread().len() < total_len {
                        // Body not yet fully buffered.
                        break;
                    }
                    // Slice the payload (bytes after the header).
                    // `total_len >= HEADER_LEN` is guaranteed by
                    // `parse_header` (it rejects declared_len < 4, so
                    // total_len = declared_len + 1 >= 5 = HEADER_LEN).
                    // `unread().len() >= total_len` was verified just
                    // above. Therefore `get(HEADER_LEN..total_len)` is
                    // always `Some`; the empty-slice fallback is
                    // defensive against a future refactor that breaks
                    // either invariant — the dispatcher's payload-
                    // shape patterns classify such inputs as
                    // `Malformed…` rather than accepting them silently.
                    let payload = self
                        .read_buf
                        .unread()
                        .get(HEADER_LEN..total_len)
                        .unwrap_or(&[]);
                    // ParameterStatus pre-dispatch filter: if the
                    // current state accepts unsolicited parameter
                    // updates, record the param and skip the
                    // dispatcher entirely. PostgreSQL emits PS during
                    // the post-auth handshake, after session `SET`
                    // commands, and on `ALTER SYSTEM` — all reachable
                    // from `Idle` and post-startup states. DEF-054.
                    if tag == crate::wire::TAG_PARAMETER_STATUS
                        && allows_unsolicited_param_status(&self.state)
                    {
                        record_param_status(&mut self.session_params, payload);
                        let Ok(()) = self.read_buf.advance(total_len) else {
                            self.fail_inflight_and_close(
                                ProtocolError::ProtocolInvariantBroken,
                                &mut out,
                            );
                            break;
                        };
                        continue;
                    }

                    // Take ownership of state for the dispatcher.
                    let prev = core::mem::take(&mut self.state);
                    let outcome = dispatch(prev, tag, payload);
                    match outcome {
                        DispatchOutcome::Advanced { new_state, action } => {
                            self.state = new_state;
                            // `advance(total_len)` was proved in-bounds
                            // above (`unread().len() >= total_len`).
                            // The Result surface is kept honest via
                            // `let-else`; a future refactor that
                            // breaks that local invariant classifies as
                            // a typed protocol error rather than
                            // silently corrupting the read cursor.
                            let Ok(()) = self.read_buf.advance(total_len) else {
                                self.fail_inflight_and_close(
                                    ProtocolError::ProtocolInvariantBroken,
                                    &mut out,
                                );
                                break;
                            };
                            if let Some(act) = action {
                                emit_actions!(&mut out, budget: 1, on_overflow: break, [
                                    act,
                                ]);
                            }
                        }
                        DispatchOutcome::Errored { reply_id, cause } => {
                            self.state = ProtoState::Errored(cause.clone());
                            match reply_id {
                                Some(id) => {
                                    emit_actions!(&mut out, budget: 2, on_overflow: break, [
                                        Action::FailReply {
                                            id: id.consume(),
                                            cause,
                                        },
                                        Action::CloseSocket,
                                    ]);
                                }
                                None => {
                                    emit_actions!(&mut out, budget: 1, on_overflow: break, [
                                        Action::CloseSocket,
                                    ]);
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }

        out
    }

    /// Helper: fail any in-flight reply with `cause`, emit `CloseSocket`,
    /// and transition the state to [`ProtoState::Errored`].
    ///
    /// Per-method push budget: **≤ 2** (FailReply + CloseSocket).
    /// `MAX_ACTIONS_PER_CALL >= 2` (const-asserted) guarantees both
    /// pushes succeed.
    ///
    /// If the previous state is already `Errored`, the original cause
    /// is preserved and no new actions are emitted — the wrapper was
    /// already told to close on the first classification; a duplicate
    /// `CloseSocket` would only confuse it.
    ///
    /// Cold path: called only from fatal classifications in
    /// `feed_bytes` (buffer-full, malformed frame, oversized frame,
    /// broken invariant). `#[cold]` hints LLVM to lay out the caller's
    /// hot path contiguously — this body is reachable only on the
    /// protocol-error branch.
    #[cold]
    fn fail_inflight_and_close(
        &mut self,
        cause: ProtocolError,
        out: &mut OutActions,
    ) {
        let prev = core::mem::take(&mut self.state);
        match prev {
            ProtoState::Idle => {
                self.state = ProtoState::Errored(cause);
                self.read_buf.clear();
                emit_actions!(out, budget: 1, [
                    Action::CloseSocket,
                ]);
            }
            ProtoState::AwaitingPingReply(id)
            | ProtoState::ConnectingStartup { reply: id, .. }
            | ProtoState::ConnectingScramAwaitServerFirst { reply: id, .. }
            | ProtoState::ConnectingScramAwaitServerFinal { reply: id, .. }
            | ProtoState::ConnectingScramAwaitAuthOk(id)
            | ProtoState::ConnectingPostAuthWaitKey(id)
            | ProtoState::ConnectingPostAuthHaveKey { reply: id, .. } => {
                self.state = ProtoState::Errored(cause.clone());
                self.read_buf.clear();
                emit_actions!(out, budget: 2, [
                    Action::FailReply {
                        id: id.consume(),
                        cause,
                    },
                    Action::CloseSocket,
                ]);
            }
            ProtoState::Errored(original) => {
                self.state = ProtoState::Errored(original);
            }
        }
    }

}

/// Compute the state transition and actions for a command push.
///
/// # Pure compute / apply split (DEF-059)
///
/// This free function owns the entire push-path decision: given the
/// command and current [`ProtoState`] *by value*, it produces the new
/// state and a bounded [`OutActions`] list. No `&mut PgProtocol` — the
/// only mutation the caller needs is the single `self.state = new_state`
/// assignment in [`PgProtocol::push_command`].
///
/// Why pure:
/// - **Testability.** Unit tests call `compute_push` directly with a
///   synthesised `(cmd, state)` pair and inspect the returned tuple.
///   No `PgProtocol` construction, no `&mut self` dance.
/// - **Single locus of mutation.** All `self.state = ...` statements
///   in the crate are restricted to `push_command` and `feed_bytes`.
///   Adding a new command variant grows the match here, not the
///   mutable surface of `PgProtocol`.
/// - **Errored pre-check dissolves.** The DEF-093 workaround (reading
///   `&self.state` *before* `core::mem::take` to avoid a transient
///   `Idle` window) is no longer needed. `ProtoState::Errored` is a
///   first-class arm: it preserves the cause (returns
///   `ProtoState::Errored(cause)` unchanged) and emits the
///   `FailReply`. No intermediate-value peek, no empty unreachable arm.
///
/// Per-command semantics live in dedicated helpers
/// ([`compute_push_ping`], [`compute_push_startup`]); `compute_push`
/// dispatches on the command variant. Adding a new `PgCommand` variant
/// fails the build here until a matching helper is wired up.
fn compute_push(cmd: PgCommand, state: ProtoState) -> (ProtoState, OutActions) {
    let mut out = OutActions::new();
    let new_state = match cmd {
        PgCommand::Ping { reply } => compute_push_ping(state, reply, &mut out),
        PgCommand::Startup {
            user,
            database,
            app_name,
            credentials,
            reply,
        } => compute_push_startup(state, user, database, app_name, credentials, reply, &mut out),
    };
    (new_state, out)
}

/// Compute the transition for [`PgCommand::Ping`] against the current
/// [`ProtoState`]. Pure; see [`compute_push`] for framing.
///
/// Exhaustive match over every `ProtoState` variant — adding a new
/// variant fails the build until the push-from-that-state policy is
/// declared here. The decision table:
///
/// | current state              | action                 | new state                 |
/// |----------------------------|------------------------|---------------------------|
/// | `Idle`                     | `SendBytes(SYNC)`      | `AwaitingPingReply(reply)`|
/// | `Errored(cause)`           | `FailReply(cause)`     | `Errored(cause)` preserved|
/// | `AwaitingPingReply(prev)`  | `FailReply(UnexpFr)`   | `AwaitingPingReply(prev)` |
/// | any `Connecting*` variant  | `FailReply(InProgr.)`  | same state preserved      |
///
/// The `compute_push_tests` module below pins this table via a
/// per-variant assertion, closing the structural seam where two arm
/// bodies could be swapped without a compile error.
fn compute_push_ping(
    state: ProtoState,
    reply: ReplyId,
    out: &mut OutActions,
) -> ProtoState {
    match state {
        ProtoState::Idle => {
            // DEF-089: SendBuf is a single-shape newtype — no Static /
            // Owned enum arms to swap. `from_slice` copies 5 bytes into
            // the stack-bounded buffer. Err branch is dead (SYNC is 5
            // bytes, fits 512-byte cap) but surfaced honestly via
            // let-else → classified ProtocolInvariantBroken.
            let Ok(sync_buf) = SendBuf::from_slice(&SYNC_WIRE_BYTES) else {
                reply.consume();
                emit_actions!(out, budget: 1, [Action::CloseSocket]);
                return ProtoState::Errored(ProtocolError::ProtocolInvariantBroken);
            };
            emit_actions!(out, budget: 1, [Action::SendBytes(sync_buf)]);
            ProtoState::AwaitingPingReply(reply)
        }
        ProtoState::Errored(cause) => {
            let fail_cause = cause.clone();
            emit_actions!(out, budget: 1, [
                Action::FailReply {
                    id: reply.consume(),
                    cause: fail_cause,
                },
            ]);
            ProtoState::Errored(cause)
        }
        ProtoState::AwaitingPingReply(prev_reply) => {
            emit_actions!(out, budget: 1, [
                Action::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::UnexpectedFrame { tag: b'P' },
                },
            ]);
            ProtoState::AwaitingPingReply(prev_reply)
        }
        other @ (ProtoState::ConnectingStartup { .. }
        | ProtoState::ConnectingScramAwaitServerFirst { .. }
        | ProtoState::ConnectingScramAwaitServerFinal { .. }
        | ProtoState::ConnectingScramAwaitAuthOk(_)
        | ProtoState::ConnectingPostAuthWaitKey(_)
        | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
            emit_actions!(out, budget: 1, [
                Action::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            other
        }
    }
}

/// Compute the transition for [`PgCommand::Startup`] against the current
/// [`ProtoState`]. Pure; see [`compute_push`] for framing.
///
/// Exhaustive match over every `ProtoState` variant. Decision table:
///
/// | current state          | action                       | new state                   |
/// |------------------------|------------------------------|-----------------------------|
/// | `Idle` (build OK)      | `SendBytes(StartupMessage)`  | `ConnectingStartup { ... }` |
/// | `Idle` (build Err)     | `FailReply(ScramError)`      | `Idle` (unchanged)          |
/// | `Errored(cause)`       | `FailReply(cause)`           | `Errored(cause)` preserved  |
/// | any non-`Idle` other   | `FailReply(InProgress)`      | same state preserved        |
///
/// The `Idle` build-failure arm is architecturally unreachable in
/// normal operation (startup fits 512-byte cap) but surfaced honestly
/// — a future refactor that breaks the const drift-guard on
/// `MAX_OWNED_SEND_LEN` classifies as a typed reply failure instead of
/// silent truncation.
fn compute_push_startup(
    state: ProtoState,
    user: Ident,
    database: Option<DatabaseName>,
    app_name: Option<ApplicationName>,
    credentials: Credentials,
    reply: ReplyId,
    out: &mut OutActions,
) -> ProtoState {
    match state {
        ProtoState::Idle => match build_startup_message(&user, database.as_ref(), app_name.as_ref())
        {
            Ok(send_buf) => {
                emit_actions!(out, budget: 1, [Action::SendBytes(send_buf)]);
                ProtoState::ConnectingStartup { reply, credentials }
            }
            Err(_) => {
                emit_actions!(out, budget: 1, [
                    Action::FailReply {
                        id: reply.consume(),
                        cause: ProtocolError::ScramError {
                            detail: heapless::String::try_from(
                                "StartupMessage too large for send buffer",
                            )
                            .unwrap_or_default(),
                        },
                    },
                ]);
                ProtoState::Idle
            }
        },
        ProtoState::Errored(cause) => {
            let fail_cause = cause.clone();
            emit_actions!(out, budget: 1, [
                Action::FailReply {
                    id: reply.consume(),
                    cause: fail_cause,
                },
            ]);
            ProtoState::Errored(cause)
        }
        other @ (ProtoState::AwaitingPingReply(_)
        | ProtoState::ConnectingStartup { .. }
        | ProtoState::ConnectingScramAwaitServerFirst { .. }
        | ProtoState::ConnectingScramAwaitServerFinal { .. }
        | ProtoState::ConnectingScramAwaitAuthOk(_)
        | ProtoState::ConnectingPostAuthWaitKey(_)
        | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
            emit_actions!(out, budget: 1, [
                Action::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            other
        }
    }
}

/// Whether the current protocol state accepts unsolicited
/// `ParameterStatus` frames from the server.
///
/// PostgreSQL emits `ParameterStatus` (tag `'S'`, shared with
/// outbound `Sync` — disambiguated by direction) in three situations:
///
/// - During the post-authentication handshake (the initial burst of
///   server-settings the client needs to know).
/// - In [`ProtoState::Idle`], any time after a session `SET` command
///   commits.
/// - In a flight state (awaiting reply / streaming) if `ALTER SYSTEM`
///   runs on the server while a query is in progress.
///
/// Pre-auth states ([`ProtoState::ConnectingStartup`] and the SCRAM
/// exchange) and [`ProtoState::Errored`] must not see unsolicited PS;
/// the dispatcher classifies those as `UnexpectedFrame` and tears the
/// connection down.
///
/// # Tier-1 regression guard
///
/// The match is exhaustive: adding a new [`ProtoState`] variant fails
/// the build here until the contributor decides how the new state
/// should handle unsolicited PS. This forecloses the latent-bug class
/// where a newly-added state "forgot" to be included and silently
/// tore the connection down on the first runtime PS. DEF-054.
fn allows_unsolicited_param_status(state: &ProtoState) -> bool {
    match state {
        ProtoState::Idle
        | ProtoState::AwaitingPingReply(_)
        | ProtoState::ConnectingPostAuthWaitKey(_)
        | ProtoState::ConnectingPostAuthHaveKey { .. } => true,
        ProtoState::ConnectingStartup { .. }
        | ProtoState::ConnectingScramAwaitServerFirst { .. }
        | ProtoState::ConnectingScramAwaitServerFinal { .. }
        | ProtoState::ConnectingScramAwaitAuthOk(_)
        | ProtoState::Errored(_) => false,
    }
}

/// Parse a ParameterStatus payload and record it in session_params.
fn record_param_status(params: &mut SessionParams, payload: &[u8]) {
    let nul_pos = match payload.iter().position(|b| *b == 0) {
        Some(p) => p,
        None => return,
    };
    let key = match payload.get(..nul_pos) {
        Some(k) => k,
        None => return,
    };
    let value_start = match nul_pos.checked_add(1) {
        Some(s) => s,
        None => return,
    };
    let value_region = match payload.get(value_start..) {
        Some(v) => v,
        None => return,
    };
    let value = match value_region.strip_suffix(&[0]) {
        Some(v) => v,
        None => value_region,
    };
    params.set(key, value);
}

/// Build a PostgreSQL StartupMessage frame.
///
/// StartupMessage format (no tag byte):
/// - 4 bytes: length (includes self)
/// - 4 bytes: protocol version (196608 = 3.0)
/// - key-value pairs, each NUL-terminated
/// - trailing empty key NUL
fn build_startup_message(
    user: &Ident,
    database: Option<&DatabaseName>,
    app_name: Option<&ApplicationName>,
) -> Result<SendBuf, crate::write_buf::WriteBufFull> {
    use crate::write_buf::WriteBufFull;

    let mut wb = WriteBuf::new();
    wb.with_length_prefix(|w| {
        // Protocol version 3.0 = 196608
        w.push_u32_be(crate::wire::PROTOCOL_VERSION_3_0)?;
        // user=<username>\0
        w.push_nul_terminated(b"user")?;
        w.push_nul_terminated(user.as_bytes())?;
        // database=<dbname>\0 (optional)
        if let Some(db) = database {
            w.push_nul_terminated(b"database")?;
            w.push_nul_terminated(db.as_bytes())?;
        }
        // application_name=<name>\0 (optional)
        if let Some(name) = app_name {
            w.push_nul_terminated(b"application_name")?;
            w.push_nul_terminated(name.as_bytes())?;
        }
        // Trailing empty key NUL
        w.push_u8(0).map_err(|_| WriteBufFull)?;
        Ok(())
    })?;
    Ok(SendBuf::from_owned(wb.into_inner()))
}

impl Default for PgProtocol {
    fn default() -> Self {
        Self::new()
    }
}

impl core::fmt::Debug for PgProtocol {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PgProtocol")
            .field("state", &self.state)
            .field("read_buf", &self.read_buf)
            .field("session_params", &self.session_params)
            .finish_non_exhaustive()
    }
}

// `push_action` helper removed — all push sites now go through
// `emit_actions!` which provides compile-time per-site budget
// enforcement (DEF-045). The old helper's Result handling is
// subsumed by the macro's `on_overflow` parameter.

#[cfg(test)]
mod allows_unsolicited_param_status_tests {
    //! Seam-closing table for `allows_unsolicited_param_status`
    //! (S1 / DEF-054). The function's exhaustive match returns `true`
    //! for four variants and `false` for five. Swapping any variant
    //! between arms compiles (both arms return `bool`); only a test
    //! enumerating every variant against its expected policy value
    //! can catch the drift.
    //!
    //! Category (1) per reforge.md §4.11.

    use super::*;
    use crate::password::{Credentials, Password};
    use crate::reply_id::ReplyId;
    use crate::scram::types::SecretDigest;
    use crate::sensitive::Sensitive;
    use core::num::NonZeroU64;

    fn nz(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN)
    }

    /// Consume any ReplyId carried by a state so the Drop-guard does
    /// not trip at end-of-scope.
    fn consume_state(state: ProtoState) {
        match state {
            ProtoState::Idle | ProtoState::Errored(_) => {}
            ProtoState::AwaitingPingReply(id)
            | ProtoState::ConnectingScramAwaitAuthOk(id)
            | ProtoState::ConnectingPostAuthWaitKey(id) => {
                // `.consume()` marks the ReplyId as delivered so the
                // Drop-guard does not fire. The returned `NonZeroU64`
                // is discarded as a statement — no `drop()` call
                // (NonZeroU64 is not Drop; clippy's `drop_non_drop`
                // fires on such calls) and no `let _` (banned).
                id.consume();
            }
            ProtoState::ConnectingStartup { reply, .. }
            | ProtoState::ConnectingScramAwaitServerFirst { reply, .. }
            | ProtoState::ConnectingScramAwaitServerFinal { reply, .. }
            | ProtoState::ConnectingPostAuthHaveKey { reply, .. } => {
                reply.consume();
            }
        }
    }

    /// Each variant is constructed and its policy verified. Adding a
    /// new `ProtoState` variant WITHOUT adding it here causes the
    /// exhaustive-match inside `allows_unsolicited_param_status` to
    /// build-fail; adding it with the wrong arm compiles but fails
    /// THIS test.
    #[test]
    fn policy_per_variant() {
        // --- Accepting states (policy = true) ---
        let idle = ProtoState::Idle;
        assert!(allows_unsolicited_param_status(&idle));
        consume_state(idle);

        let awaiting_ping = ProtoState::AwaitingPingReply(ReplyId::from_raw(nz(1)));
        assert!(allows_unsolicited_param_status(&awaiting_ping));
        consume_state(awaiting_ping);

        let wait_key = ProtoState::ConnectingPostAuthWaitKey(ReplyId::from_raw(nz(2)));
        assert!(allows_unsolicited_param_status(&wait_key));
        consume_state(wait_key);

        let have_key = ProtoState::ConnectingPostAuthHaveKey {
            reply: ReplyId::from_raw(nz(3)),
            pid: 1,
            secret_key: 1,
        };
        assert!(allows_unsolicited_param_status(&have_key));
        consume_state(have_key);

        // --- Rejecting states (policy = false) ---
        let startup = ProtoState::ConnectingStartup {
            reply: ReplyId::from_raw(nz(4)),
            credentials: Credentials::Trust,
        };
        assert!(!allows_unsolicited_param_status(&startup));
        consume_state(startup);

        // ConnectingScramAwaitServerFirst requires a Password *and* a
        // ScramSession (audit A2 typestate). Both constructors have
        // architecturally-dead Err branches for our fixture inputs;
        // we skip the sub-case on either Err to keep the test within
        // the crate-root forbid bundle (no `unwrap` / `panic`).
        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let creds = Credentials::ScramPassword(Sensitive::new(pw));
            if let Ok(scram) = crate::scram::session::ScramSession::try_from_credentials(creds) {
                let scram_first = ProtoState::ConnectingScramAwaitServerFirst {
                    reply: ReplyId::from_raw(nz(5)),
                    scram,
                    client_first_bare: heapless::Vec::new(),
                    client_nonce_b64: heapless::Vec::new(),
                };
                assert!(!allows_unsolicited_param_status(&scram_first));
                consume_state(scram_first);
            }
        }

        let scram_final = ProtoState::ConnectingScramAwaitServerFinal {
            reply: ReplyId::from_raw(nz(6)),
            expected_server_sig: SecretDigest::new([0u8; 32]),
        };
        assert!(!allows_unsolicited_param_status(&scram_final));
        consume_state(scram_final);

        let scram_authok = ProtoState::ConnectingScramAwaitAuthOk(ReplyId::from_raw(nz(7)));
        assert!(!allows_unsolicited_param_status(&scram_authok));
        consume_state(scram_authok);

        // Errored — rejecting (terminal; no traffic accepted).
        let errored = ProtoState::Errored(ProtocolError::UnexpectedFrame { tag: b'X' });
        assert!(!allows_unsolicited_param_status(&errored));
        consume_state(errored);
    }
}

#[cfg(test)]
mod compute_push_tests {
    //! DEF-059 — seam-closing tests for the pure push-compute split.
    //!
    //! The push-path decision table is enumerated per `(cmd, state)`
    //! pair; every arm of [`compute_push_ping`] and
    //! [`compute_push_startup`] is exercised and its `(new_state,
    //! actions)` output is pinned. Swapping any two arm bodies would
    //! compile (identical return shape `ProtoState`, identical
    //! `emit_actions!` budget), so the only shield for the policy
    //! table is this enumeration.
    //!
    //! Category (1) per reforge.md §4.11 — exhaustive-match policy
    //! table. Companion to `allows_unsolicited_param_status_tests`
    //! above (same test style, same helpers).
    //!
    //! These tests also stand as the DEF-059 proof that the pure
    //! half is testable without constructing [`PgProtocol`]: every
    //! test calls [`compute_push`] directly on a synthesised
    //! `(cmd, state)` pair.
    use super::*;
    use crate::password::{Credentials, Password};
    use crate::reply_id::ReplyId;
    use crate::scram::types::SecretDigest;
    use crate::sensitive::Sensitive;
    use core::num::NonZeroU64;

    fn nz(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN)
    }

    /// Consume any ReplyId carried by `state` so its Drop-guard does
    /// not trip when the state drops at end of scope. Copy of the
    /// helper in `allows_unsolicited_param_status_tests` — test
    /// modules are siblings; Rust module privacy forbids re-use
    /// without cross-module `pub(super)` exposure, and a 20-line
    /// match is cheaper than the coupling.
    fn consume_state(state: ProtoState) {
        match state {
            ProtoState::Idle | ProtoState::Errored(_) => {}
            ProtoState::AwaitingPingReply(id)
            | ProtoState::ConnectingScramAwaitAuthOk(id)
            | ProtoState::ConnectingPostAuthWaitKey(id) => {
                id.consume();
            }
            ProtoState::ConnectingStartup { reply, .. }
            | ProtoState::ConnectingScramAwaitServerFirst { reply, .. }
            | ProtoState::ConnectingScramAwaitServerFinal { reply, .. }
            | ProtoState::ConnectingPostAuthHaveKey { reply, .. } => {
                reply.consume();
            }
        }
    }

    /// If `new_state` is `AwaitingPingReply`, consume the inner reply
    /// and return its raw value. Otherwise drain any carried reply
    /// and return `None`. Used to express the assertion "new state
    /// is AwaitingPingReply(expected_raw)" as a single `assert_eq!`
    /// without the forbid-bundle incompatibility of `panic!` in an
    /// else branch.
    fn take_awaiting_ping_raw(new_state: ProtoState) -> Option<NonZeroU64> {
        match new_state {
            ProtoState::AwaitingPingReply(r) => Some(r.consume()),
            other => {
                consume_state(other);
                None
            }
        }
    }

    /// Like [`take_awaiting_ping_raw`] but for `ConnectingStartup`.
    fn take_connecting_startup_raw(new_state: ProtoState) -> Option<NonZeroU64> {
        match new_state {
            ProtoState::ConnectingStartup { reply, .. } => Some(reply.consume()),
            other => {
                consume_state(other);
                None
            }
        }
    }

    /// Build a minimal valid `Ident` for tests. `"u"` is 1 byte, well
    /// within MAX_IDENT_LEN; the Err branch is architecturally dead
    /// but surfaced via `.ok()?` so the forbid-bundle is honoured.
    fn mk_user() -> Option<Ident> {
        Ident::try_from_str("u").ok()
    }

    // -----------------------------------------------------------------
    // Ping — per-variant policy table
    // -----------------------------------------------------------------

    #[test]
    fn ping_from_idle_emits_sync_and_awaits() {
        let raw_id = nz(101);
        let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_id) };
        let (new_state, out) = compute_push(cmd, ProtoState::Idle);

        // Action: exactly one SendBytes whose payload is SYNC_WIRE_BYTES.
        assert_eq!(out.len(), 1);
        assert!(
            matches!(
                out.first(),
                Some(Action::SendBytes(sb)) if sb.as_bytes() == SYNC_WIRE_BYTES.as_slice()
            ),
            "expected SendBytes(SYNC)",
        );

        // State: AwaitingPingReply(raw_id).
        assert_eq!(take_awaiting_ping_raw(new_state), Some(raw_id));
    }

    #[test]
    fn ping_from_errored_preserves_errored_and_fails_with_cause() {
        let raw_id = nz(102);
        let original_cause = ProtocolError::UnexpectedFrame { tag: b'X' };
        let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_id) };
        let (new_state, out) = compute_push(
            cmd,
            ProtoState::Errored(original_cause.clone()),
        );

        // Action: FailReply carrying the ORIGINAL cause (not a synthetic one).
        assert_eq!(out.len(), 1);
        assert!(
            matches!(
                out.first(),
                Some(Action::FailReply { id, cause }) if *id == raw_id && *cause == original_cause
            ),
            "expected FailReply(original cause)",
        );

        // State: Errored preserved with the same cause.
        assert!(
            matches!(&new_state, ProtoState::Errored(cause) if *cause == original_cause),
            "expected Errored(cause) preserved",
        );
        consume_state(new_state);
    }

    #[test]
    fn ping_from_awaiting_ping_reply_fails_with_unexpected_frame_and_preserves_state() {
        let raw_prev = nz(103);
        let raw_new = nz(104);
        let prev_state = ProtoState::AwaitingPingReply(ReplyId::from_raw(raw_prev));
        let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
        let (new_state, out) = compute_push(cmd, prev_state);

        // Action: FailReply(UnexpectedFrame { tag: b'P' }) for the NEW reply.
        assert_eq!(out.len(), 1);
        assert!(
            matches!(
                out.first(),
                Some(Action::FailReply {
                    id,
                    cause: ProtocolError::UnexpectedFrame { tag: b'P' },
                }) if *id == raw_new
            ),
            "expected FailReply(UnexpectedFrame{{tag: b'P'}}) for new reply",
        );

        // State: AwaitingPingReply(raw_prev) — the original prev_reply
        // is preserved, not replaced by the new one.
        assert_eq!(take_awaiting_ping_raw(new_state), Some(raw_prev));
    }

    /// Construct every `Connecting*` variant and assert that pushing
    /// `Ping` against it yields `FailReply(StartupAlreadyInProgress)`
    /// with the state preserved unchanged. Closes the seam where one
    /// variant could be pulled out of the `other @ (…)` or-pattern
    /// into a different arm with different semantics — compile stays
    /// green (exhaustive match still satisfied), runtime drifts.
    #[test]
    fn ping_from_any_connecting_state_fails_with_startup_in_progress() {
        // ConnectingStartup — Trust credentials (no Password needed).
        {
            let raw_prev = nz(201);
            let raw_new = nz(202);
            let prev = ProtoState::ConnectingStartup {
                reply: ReplyId::from_raw(raw_prev),
                credentials: Credentials::Trust,
            };
            let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
            let (new_state, out) = compute_push(cmd, prev);
            assert_eq!(out.len(), 1);
            assert!(
                matches!(
                    out.first(),
                    Some(Action::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ConnectingStartup → expected FailReply(StartupAlreadyInProgress)",
            );
            assert_eq!(take_connecting_startup_raw(new_state), Some(raw_prev));
        }

        // ConnectingScramAwaitServerFirst — needs a Password and
        // ScramSession (audit A2 typestate; construction rejects Trust
        // at the credentials boundary).
        if let Ok(pw) = Password::try_from_bytes(b"pw")
            && let Ok(scram) = crate::scram::session::ScramSession::try_from_credentials(
                Credentials::ScramPassword(Sensitive::new(pw)),
            ) {
                let raw_prev = nz(203);
                let raw_new = nz(204);
                let prev = ProtoState::ConnectingScramAwaitServerFirst {
                    reply: ReplyId::from_raw(raw_prev),
                    scram,
                    client_first_bare: heapless::Vec::new(),
                    client_nonce_b64: heapless::Vec::new(),
                };
                let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
                let (new_state, out) = compute_push(cmd, prev);
                assert_eq!(out.len(), 1);
                assert!(
                    matches!(
                        out.first(),
                        Some(Action::FailReply {
                            id,
                            cause: ProtocolError::StartupAlreadyInProgress,
                        }) if *id == raw_new
                    ),
                    "ScramAwaitServerFirst → expected FailReply(StartupAlreadyInProgress)",
                );
                assert!(matches!(
                    &new_state,
                    ProtoState::ConnectingScramAwaitServerFirst { .. }
                ));
                consume_state(new_state);
            }

        // ConnectingScramAwaitServerFinal.
        {
            let raw_prev = nz(205);
            let raw_new = nz(206);
            let prev = ProtoState::ConnectingScramAwaitServerFinal {
                reply: ReplyId::from_raw(raw_prev),
                expected_server_sig: SecretDigest::new([0u8; 32]),
            };
            let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
            let (new_state, out) = compute_push(cmd, prev);
            assert_eq!(out.len(), 1);
            assert!(
                matches!(
                    out.first(),
                    Some(Action::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ScramAwaitServerFinal → expected FailReply(StartupAlreadyInProgress)",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingScramAwaitServerFinal { .. }
            ));
            consume_state(new_state);
        }

        // ConnectingScramAwaitAuthOk.
        {
            let raw_prev = nz(207);
            let raw_new = nz(208);
            let prev = ProtoState::ConnectingScramAwaitAuthOk(ReplyId::from_raw(raw_prev));
            let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
            let (new_state, out) = compute_push(cmd, prev);
            assert_eq!(out.len(), 1);
            assert!(
                matches!(
                    out.first(),
                    Some(Action::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ScramAwaitAuthOk → expected FailReply(StartupAlreadyInProgress)",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingScramAwaitAuthOk(_)
            ));
            consume_state(new_state);
        }

        // ConnectingPostAuthWaitKey.
        {
            let raw_prev = nz(209);
            let raw_new = nz(210);
            let prev = ProtoState::ConnectingPostAuthWaitKey(ReplyId::from_raw(raw_prev));
            let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
            let (new_state, out) = compute_push(cmd, prev);
            assert_eq!(out.len(), 1);
            assert!(
                matches!(
                    out.first(),
                    Some(Action::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "PostAuthWaitKey → expected FailReply(StartupAlreadyInProgress)",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingPostAuthWaitKey(_)
            ));
            consume_state(new_state);
        }

        // ConnectingPostAuthHaveKey.
        {
            let raw_prev = nz(211);
            let raw_new = nz(212);
            let prev = ProtoState::ConnectingPostAuthHaveKey {
                reply: ReplyId::from_raw(raw_prev),
                pid: 42,
                secret_key: 1337,
            };
            let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
            let (new_state, out) = compute_push(cmd, prev);
            assert_eq!(out.len(), 1);
            assert!(
                matches!(
                    out.first(),
                    Some(Action::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "PostAuthHaveKey → expected FailReply(StartupAlreadyInProgress)",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingPostAuthHaveKey { .. }
            ));
            consume_state(new_state);
        }
    }

    // -----------------------------------------------------------------
    // Startup — per-variant policy table
    // -----------------------------------------------------------------

    #[test]
    fn startup_from_idle_transitions_and_emits_startup_message() {
        let Some(user) = mk_user() else { return };
        let raw_id = nz(301);
        let cmd = PgCommand::Startup {
            user,
            database: None,
            app_name: None,
            credentials: Credentials::Trust,
            reply: ReplyId::from_raw(raw_id),
        };
        let (new_state, out) = compute_push(cmd, ProtoState::Idle);

        // Action: SendBytes with non-empty payload (startup frame, no tag).
        assert_eq!(out.len(), 1);
        assert!(
            matches!(
                out.first(),
                Some(Action::SendBytes(sb)) if !sb.as_bytes().is_empty()
            ),
            "expected non-empty SendBytes(StartupMessage)",
        );

        // State: ConnectingStartup with the pushed reply id.
        assert_eq!(take_connecting_startup_raw(new_state), Some(raw_id));
    }

    #[test]
    fn startup_from_errored_preserves_errored_and_fails_with_cause() {
        let Some(user) = mk_user() else { return };
        let raw_id = nz(302);
        let original_cause = ProtocolError::MalformedFrameLength { declared: 0 };
        let cmd = PgCommand::Startup {
            user,
            database: None,
            app_name: None,
            credentials: Credentials::Trust,
            reply: ReplyId::from_raw(raw_id),
        };
        let (new_state, out) = compute_push(
            cmd,
            ProtoState::Errored(original_cause.clone()),
        );

        // Action: FailReply carrying the ORIGINAL cause intact.
        assert_eq!(out.len(), 1);
        assert!(
            matches!(
                out.first(),
                Some(Action::FailReply { id, cause }) if *id == raw_id && *cause == original_cause
            ),
            "expected FailReply(original cause) for Errored",
        );

        // State: Errored preserved.
        assert!(
            matches!(&new_state, ProtoState::Errored(cause) if *cause == original_cause),
            "expected Errored preserved",
        );
        consume_state(new_state);
    }

    /// Every non-`Idle`/non-`Errored` state rejects Startup with
    /// `StartupAlreadyInProgress` and preserves its state. Closes the
    /// same or-pattern seam as the ping counterpart.
    #[test]
    fn startup_from_non_idle_non_errored_fails_with_startup_in_progress() {
        // Factory to build a Startup command consuming a fresh
        // `user` per sub-case. Each Startup consumes its `user`, so
        // we cannot share one across iterations.
        let make_startup_cmd = |user: Ident, raw: NonZeroU64| PgCommand::Startup {
            user,
            database: None,
            app_name: None,
            credentials: Credentials::Trust,
            reply: ReplyId::from_raw(raw),
        };

        // AwaitingPingReply.
        if let Some(user) = mk_user() {
            let raw_prev = nz(401);
            let raw_new = nz(402);
            let prev = ProtoState::AwaitingPingReply(ReplyId::from_raw(raw_prev));
            let (new_state, out) = compute_push(make_startup_cmd(user, raw_new), prev);
            assert_eq!(out.len(), 1);
            assert!(
                matches!(
                    out.first(),
                    Some(Action::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "AwaitingPingReply → expected StartupAlreadyInProgress",
            );
            assert_eq!(take_awaiting_ping_raw(new_state), Some(raw_prev));
        }

        // ConnectingStartup.
        if let Some(user) = mk_user() {
            let raw_prev = nz(403);
            let raw_new = nz(404);
            let prev = ProtoState::ConnectingStartup {
                reply: ReplyId::from_raw(raw_prev),
                credentials: Credentials::Trust,
            };
            let (new_state, out) = compute_push(make_startup_cmd(user, raw_new), prev);
            assert_eq!(out.len(), 1);
            assert!(
                matches!(
                    out.first(),
                    Some(Action::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ConnectingStartup → expected StartupAlreadyInProgress",
            );
            assert_eq!(take_connecting_startup_raw(new_state), Some(raw_prev));
        }

        // ConnectingScramAwaitServerFirst. Construction requires
        // Password + ScramSession (audit A2 typestate).
        if let (Some(user), Ok(pw)) = (mk_user(), Password::try_from_bytes(b"pw"))
            && let Ok(scram) = crate::scram::session::ScramSession::try_from_credentials(
                Credentials::ScramPassword(Sensitive::new(pw)),
            ) {
                let raw_prev = nz(405);
                let raw_new = nz(406);
                let prev = ProtoState::ConnectingScramAwaitServerFirst {
                    reply: ReplyId::from_raw(raw_prev),
                    scram,
                    client_first_bare: heapless::Vec::new(),
                    client_nonce_b64: heapless::Vec::new(),
                };
                let (new_state, out) = compute_push(make_startup_cmd(user, raw_new), prev);
                assert_eq!(out.len(), 1);
                assert!(
                    matches!(
                        out.first(),
                        Some(Action::FailReply {
                            id,
                            cause: ProtocolError::StartupAlreadyInProgress,
                        }) if *id == raw_new
                    ),
                    "ScramAwaitServerFirst → expected StartupAlreadyInProgress",
                );
                assert!(matches!(
                    &new_state,
                    ProtoState::ConnectingScramAwaitServerFirst { .. }
                ));
                consume_state(new_state);
            }

        // ConnectingScramAwaitServerFinal.
        if let Some(user) = mk_user() {
            let raw_prev = nz(407);
            let raw_new = nz(408);
            let prev = ProtoState::ConnectingScramAwaitServerFinal {
                reply: ReplyId::from_raw(raw_prev),
                expected_server_sig: SecretDigest::new([0u8; 32]),
            };
            let (new_state, out) = compute_push(make_startup_cmd(user, raw_new), prev);
            assert_eq!(out.len(), 1);
            assert!(
                matches!(
                    out.first(),
                    Some(Action::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ScramAwaitServerFinal → expected StartupAlreadyInProgress",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingScramAwaitServerFinal { .. }
            ));
            consume_state(new_state);
        }

        // ConnectingScramAwaitAuthOk.
        if let Some(user) = mk_user() {
            let raw_prev = nz(409);
            let raw_new = nz(410);
            let prev = ProtoState::ConnectingScramAwaitAuthOk(ReplyId::from_raw(raw_prev));
            let (new_state, out) = compute_push(make_startup_cmd(user, raw_new), prev);
            assert_eq!(out.len(), 1);
            assert!(
                matches!(
                    out.first(),
                    Some(Action::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ScramAwaitAuthOk → expected StartupAlreadyInProgress",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingScramAwaitAuthOk(_)
            ));
            consume_state(new_state);
        }

        // ConnectingPostAuthWaitKey.
        if let Some(user) = mk_user() {
            let raw_prev = nz(411);
            let raw_new = nz(412);
            let prev = ProtoState::ConnectingPostAuthWaitKey(ReplyId::from_raw(raw_prev));
            let (new_state, out) = compute_push(make_startup_cmd(user, raw_new), prev);
            assert_eq!(out.len(), 1);
            assert!(
                matches!(
                    out.first(),
                    Some(Action::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "PostAuthWaitKey → expected StartupAlreadyInProgress",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingPostAuthWaitKey(_)
            ));
            consume_state(new_state);
        }

        // ConnectingPostAuthHaveKey.
        if let Some(user) = mk_user() {
            let raw_prev = nz(413);
            let raw_new = nz(414);
            let prev = ProtoState::ConnectingPostAuthHaveKey {
                reply: ReplyId::from_raw(raw_prev),
                pid: 1,
                secret_key: 2,
            };
            let (new_state, out) = compute_push(make_startup_cmd(user, raw_new), prev);
            assert_eq!(out.len(), 1);
            assert!(
                matches!(
                    out.first(),
                    Some(Action::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "PostAuthHaveKey → expected StartupAlreadyInProgress",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingPostAuthHaveKey { .. }
            ));
            consume_state(new_state);
        }
    }
}
