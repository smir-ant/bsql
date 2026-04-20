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

use crate::action::{Action, OutActions, StagedAction, StagedActions};
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
    pub fn push_command<'w>(
        &mut self,
        cmd: PgCommand,
        write_buf: &'w mut WriteBuf,
    ) -> OutActions<'w, 'static> {
        // 1c-1a: push never produces `StreamRow` (rows arrive via
        // server responses, handled in `feed_bytes`). The `'r`
        // lifetime parameter on `OutActions<'w, 'r>` is phantom on
        // this path — unifying it to `'static` gives the caller
        // freedom over what they pair the result with later.
        write_buf.clear();
        let prev = core::mem::take(&mut self.state);
        let (new_state, staged) = compute_push(cmd, prev, write_buf);
        self.state = new_state;
        materialise(staged, write_buf.as_bytes(), &[])
    }

    /// Feed inbound wire bytes.
    ///
    /// Returns the action list — bounded by [`MAX_ACTIONS_PER_CALL`].
    /// DEF-094: caller-owned `write_buf` — see [`push_command`] for
    /// the staged-dispatch architecture.
    ///
    /// 1c-1a: `&'r mut self` — the row slices in `Action::StreamRow`
    /// borrow from `self.read_buf`. The `'r` lifetime propagates
    /// into `OutActions<'w, 'r>`; the borrow checker blocks
    /// subsequent `&mut self` calls (and thus the next `feed_bytes`)
    /// until `OutActions` drops.
    ///
    /// [`push_command`]: Self::push_command
    #[must_use = "the returned actions carry side-effects that must be executed"]
    pub fn feed_bytes<'w, 'r>(
        &'r mut self,
        bytes: &[u8],
        write_buf: &'w mut WriteBuf,
    ) -> OutActions<'w, 'r> {
        write_buf.clear();
        let mut staged = StagedActions::new();

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
                &mut staged,
            );
            return materialise(staged, write_buf.as_bytes(), self.read_buf.unread());
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
                        &mut staged,
                    );
                    break;
                }
                HeaderParse::FrameTooLarge { declared } => {
                    self.fail_inflight_and_close(
                        ProtocolError::FrameTooLarge { declared },
                        &mut staged,
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
                    let payload = self
                        .read_buf
                        .unread()
                        .get(HEADER_LEN..total_len)
                        .unwrap_or(&[]);
                    if tag == crate::wire::TAG_PARAMETER_STATUS
                        && allows_unsolicited_param_status(&self.state)
                    {
                        record_param_status(&mut self.session_params, payload);
                        let Ok(()) = self.read_buf.advance(total_len) else {
                            self.fail_inflight_and_close(
                                ProtocolError::ProtocolInvariantBroken,
                                &mut staged,
                            );
                            break;
                        };
                        continue;
                    }

                    // DEF-062: NoticeResponse pre-dispatch filter.
                    // PG can emit NoticeResponse (tag 'N') in any
                    // state — warnings that do not affect protocol
                    // flow. Silently consume and continue; the
                    // dispatcher never sees them. Future Phase 1c+
                    // can replace the `continue` with an
                    // `Action::EmitNotice(...)` emission when the
                    // wrapper wants visibility.
                    if tag == crate::wire::TAG_NOTICE_RESPONSE {
                        let Ok(()) = self.read_buf.advance(total_len) else {
                            self.fail_inflight_and_close(
                                ProtocolError::ProtocolInvariantBroken,
                                &mut staged,
                            );
                            break;
                        };
                        continue;
                    }

                    let prev = core::mem::take(&mut self.state);
                    let outcome = dispatch(prev, tag, payload, write_buf);
                    match outcome {
                        DispatchOutcome::AdvancedSilent { new_state } => {
                            self.state = new_state;
                            let Ok(()) = self.read_buf.advance(total_len) else {
                                self.fail_inflight_and_close(
                                    ProtocolError::ProtocolInvariantBroken,
                                    &mut staged,
                                );
                                break;
                            };
                        }
                        DispatchOutcome::AdvancedWithAction { new_state, action } => {
                            self.state = new_state;
                            let Ok(()) = self.read_buf.advance(total_len) else {
                                self.fail_inflight_and_close(
                                    ProtocolError::ProtocolInvariantBroken,
                                    &mut staged,
                                );
                                break;
                            };
                            emit_actions!(&mut staged, budget: 1, on_overflow: break, [
                                action,
                            ]);
                        }
                        DispatchOutcome::Errored { reply_id, cause } => {
                            // DEF-061: store the compact 1-byte kind in
                            // state; the full cause goes out in the
                            // `FailReply` below, exactly once.
                            self.state = ProtoState::Errored(cause.kind());
                            match reply_id {
                                // DEF-112: `reply_id` is now
                                // `Option<NonZeroU64>` — already
                                // consumed by the dispatcher.
                                Some(id) => {
                                    emit_actions!(&mut staged, budget: 2, on_overflow: break, [
                                        StagedAction::FailReply { id, cause },
                                        StagedAction::CloseSocket,
                                    ]);
                                }
                                None => {
                                    emit_actions!(&mut staged, budget: 1, on_overflow: break, [
                                        StagedAction::CloseSocket,
                                    ]);
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }

        // 1c-1a: two buffers feed `materialise`. `write_buf`
        // supplies `'w` for `SendBytes` slices; `self.read_buf`
        // supplies `'r` for `StreamRow` slices (not emitted yet
        // in 1c-1a — wired in 1c-1b).
        materialise(staged, write_buf.as_bytes(), self.read_buf.unread())
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
        staged: &mut StagedActions,
    ) {
        // DEF-061 + DEF-094: compute kind before `cause` is moved
        // into the FailReply StagedAction. Stored in state as 1-byte
        // ErrorKind; full cause goes out in the one FailReply emitted
        // below. `staged` is the phase-1 accumulator; entry-point
        // materialises into `OutActions<'buf>`.
        let kind = cause.kind();
        // DEF-117: `core::mem::replace` directly installs the
        // terminal `Errored(kind)` state, eliminating the
        // transient `Idle`-window that `core::mem::take` would
        // create. Consequence: the "Default-on-ProtoState-is-Idle
        // is load-bearing for transient-window safety" invariant
        // becomes architecturally unneeded here. Even if a future
        // refactor grew this function body with `&self` reads
        // between the state swap and the write-back, there is no
        // intermediate `Idle` to misread — the state is already
        // `Errored(kind)` from the first instruction.
        //
        // DEF-112: typed `ReplyId<K>` variants have distinct
        // types; or-pattern `id` bindings are type-incompatible,
        // so per-variant `.consume()` produces the raw
        // `NonZeroU64` individually.
        let prev = core::mem::replace(&mut self.state, ProtoState::Errored(kind));
        let raw_id: Option<core::num::NonZeroU64> = match prev {
            ProtoState::Idle => None,
            ProtoState::AwaitingPingReply(id) => Some(id.consume()),
            ProtoState::ConnectingStartupTrust { reply }
            | ProtoState::ConnectingStartupScram { reply, .. }
            | ProtoState::ConnectingScramAwaitServerFirst { reply, .. }
            | ProtoState::ConnectingScramAwaitServerFinal { reply, .. }
            | ProtoState::ConnectingScramAwaitAuthOk(reply)
            | ProtoState::ConnectingPostAuthWaitKey(reply)
            | ProtoState::ConnectingPostAuthHaveKey { reply, .. } => Some(reply.consume()),
            ProtoState::Errored(original_kind) => {
                // Already Errored — preserve the ORIGINAL kind
                // (the `replace` above wrote the new one; revert).
                // Do not emit another FailReply / CloseSocket; the
                // wrapper already saw them on the first fatal.
                self.state = ProtoState::Errored(original_kind);
                return;
            }
        };
        self.read_buf.clear();
        match raw_id {
            Some(id) => {
                emit_actions!(staged, budget: 2, [
                    StagedAction::FailReply { id, cause },
                    StagedAction::CloseSocket,
                ]);
            }
            None => {
                emit_actions!(staged, budget: 1, [
                    StagedAction::CloseSocket,
                ]);
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
fn compute_push(
    cmd: PgCommand,
    state: ProtoState,
    write_buf: &mut WriteBuf,
) -> (ProtoState, StagedActions) {
    let mut staged = StagedActions::new();
    let new_state = match cmd {
        PgCommand::Ping { reply } => compute_push_ping(state, reply, &mut staged),
        PgCommand::Startup {
            user,
            database,
            app_name,
            credentials,
            reply,
        } => compute_push_startup(
            state,
            user,
            database,
            app_name,
            credentials,
            reply,
            &mut staged,
            write_buf,
        ),
    };
    (new_state, staged)
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
    reply: ReplyId<crate::reply_id::PingKind>,
    staged: &mut StagedActions,
) -> ProtoState {
    match state {
        ProtoState::Idle => {
            // DEF-094: Sync is a compile-time const (5 bytes). Emit
            // `StagedAction::SendBytesStatic(&SYNC_WIRE_BYTES)` so the
            // materialiser passes the static reference through
            // directly — zero write to write_buf, zero copy.
            emit_actions!(staged, budget: 1, [
                StagedAction::SendBytesStatic(&SYNC_WIRE_BYTES),
            ]);
            ProtoState::AwaitingPingReply(reply)
        }
        ProtoState::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
            ProtoState::Errored(prior_kind)
        }
        ProtoState::AwaitingPingReply(prev_reply) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::UnexpectedFrame { tag: b'P' },
                },
            ]);
            ProtoState::AwaitingPingReply(prev_reply)
        }
        other @ (ProtoState::ConnectingStartupTrust { .. }
        | ProtoState::ConnectingStartupScram { .. }
        | ProtoState::ConnectingScramAwaitServerFirst { .. }
        | ProtoState::ConnectingScramAwaitServerFinal { .. }
        | ProtoState::ConnectingScramAwaitAuthOk(_)
        | ProtoState::ConnectingPostAuthWaitKey(_)
        | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
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
#[expect(clippy::too_many_arguments, reason = "compute_push_startup is an internal helper for Pg startup-command dispatch; its arg count matches the `PgCommand::Startup` payload + write_buf + staged accumulator. Splitting into a struct-arg would obscure the pure-compute framing (DEF-059).")]
fn compute_push_startup(
    state: ProtoState,
    user: Ident,
    database: Option<DatabaseName>,
    app_name: Option<ApplicationName>,
    credentials: Credentials,
    reply: ReplyId<crate::reply_id::StartupKind>,
    staged: &mut StagedActions,
    write_buf: &mut WriteBuf,
) -> ProtoState {
    match state {
        ProtoState::Idle => match build_startup_message(
            &user,
            database.as_ref(),
            app_name.as_ref(),
            write_buf,
        ) {
            Ok(range) => {
                emit_actions!(staged, budget: 1, [
                    StagedAction::SendBytesRange(range),
                ]);
                // DEF-097: discriminate Trust vs Scram *here* — the
                // post-push state carries only what its auth method
                // needs. Trust: 24 bytes. Scram: 24 + ScramSession
                // (~1040). The "server sent AUTH_SASL on a Trust
                // connection" case is now a per-variant dispatcher
                // arm instead of a runtime classification.
                match credentials {
                    Credentials::Trust => ProtoState::ConnectingStartupTrust { reply },
                    Credentials::ScramPassword(password) => {
                        ProtoState::ConnectingStartupScram {
                            reply,
                            scram: crate::scram::session::ScramSession::from_password(password),
                        }
                    }
                }
            }
            Err(_) => {
                // Architecturally unreachable; classified as
                // ProtocolInvariantBroken (DEF-060 / DEF-094).
                emit_actions!(staged, budget: 1, [
                    StagedAction::FailReply {
                        id: reply.consume(),
                        cause: ProtocolError::ProtocolInvariantBroken,
                    },
                ]);
                ProtoState::Idle
            }
        },
        ProtoState::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
            ProtoState::Errored(prior_kind)
        }
        other @ (ProtoState::AwaitingPingReply(_)
        | ProtoState::ConnectingStartupTrust { .. }
        | ProtoState::ConnectingStartupScram { .. }
        | ProtoState::ConnectingScramAwaitServerFirst { .. }
        | ProtoState::ConnectingScramAwaitServerFinal { .. }
        | ProtoState::ConnectingScramAwaitAuthOk(_)
        | ProtoState::ConnectingPostAuthWaitKey(_)
        | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
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
        ProtoState::ConnectingStartupTrust { .. }
        | ProtoState::ConnectingStartupScram { .. }
        | ProtoState::ConnectingScramAwaitServerFirst { .. }
        | ProtoState::ConnectingScramAwaitServerFinal { .. }
        | ProtoState::ConnectingScramAwaitAuthOk(_)
        | ProtoState::Errored(_) => false,
    }
}

/// Parse a ParameterStatus payload and record it in session_params.
///
/// Payload format: `key\0value\0`. Compressed with `let-else` to
/// five short lines (DEF-095). `[T]::split_once` with a predicate
/// is still unstable (#112811); the `iter().position` idiom is the
/// stable-library equivalent.
fn record_param_status(params: &mut SessionParams, payload: &[u8]) {
    let Some(nul_pos) = payload.iter().position(|b| *b == 0) else { return; };
    let Some(key) = payload.get(..nul_pos) else { return; };
    let Some(value_start) = nul_pos.checked_add(1) else { return; };
    let Some(value_region) = payload.get(value_start..) else { return; };
    let value = value_region.strip_suffix(b"\0").unwrap_or(value_region);
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
    write_buf: &mut WriteBuf,
) -> Result<crate::action::NonEmptyRange, crate::write_buf::WriteBufFull> {
    use crate::write_buf::WriteBufFull;

    // DEF-094: write in-place into the caller-owned `write_buf`.
    // DEF-100: return a typed `NonEmptyRange` instead of `(start,
    // end)` — non-zero length is a type invariant, materialise's
    // silent-empty fallback closes from tier-3 (audit) to tier-2
    // (type-checked construction).
    let start = write_buf.len();
    write_buf.with_length_prefix(|w| {
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
    // StartupMessage always writes the 4-byte length-prefix minimum,
    // so the range is non-empty by construction. The Option cannot
    // be None unless the writes above all succeeded *and* produced
    // zero bytes — a type-level contradiction given the 4-byte
    // length prefix.
    crate::action::NonEmptyRange::from_write_span(start, write_buf).ok_or(WriteBufFull)
}

/// Phase-2 materialiser: convert the write-phase's
/// [`StagedActions`] into [`OutActions<'w, 'r>`] with references
/// into `write_buf_bytes` (`'w` — range variants, static constants)
/// or `read_buf_bytes` (`'r` — StreamRow variants once 1c-1b wires
/// row emission).
///
/// DEF-094 + 1c-1a lifetime plumbing: `write_buf_bytes` supplies
/// `'w`; `read_buf_bytes` supplies `'r`. The borrow checker
/// refuses any `&mut WriteBuf` re-borrow while the returned
/// `OutActions<'w, 'r>` is alive, and any `&mut self` re-borrow
/// on `PgProtocol` (thus `feed_bytes`) while `'r` is alive.
fn materialise<'w, 'r>(
    staged: StagedActions,
    write_buf_bytes: &'w [u8],
    _read_buf_bytes: &'r [u8],
) -> OutActions<'w, 'r> {
    // `_read_buf_bytes` is unused in 1c-1a — wired in 1c-1b when
    // `StagedAction::StreamRowRange` is introduced. Until then the
    // `'r` parameter on `OutActions` is phantom.
    let mut out = OutActions::new();
    for sa in staged {
        let a: Action<'w, 'r> = match sa {
            StagedAction::SendBytesRange(range) => {
                // DEF-100: `range: NonEmptyRange` was constructor-
                // validated at emission. `apply(write_buf_bytes)`
                // can only be None if the buffer is shorter than
                // emission-time — architecturally the same buffer.
                Action::SendBytes(range.apply(write_buf_bytes).unwrap_or(&[]))
            }
            StagedAction::SendBytesStatic(s) => Action::SendBytes(s),
            // DEF-112: `DeliverReplyEntry` fields are module-private.
            // Access via accessor methods. The entry was constructed
            // by the typed `action::deliver` path — type-payload
            // pairing was enforced there.
            StagedAction::DeliverReply(entry) => Action::DeliverReply {
                id: entry.id(),
                value: entry.value(),
            },
            StagedAction::FailReply { id, cause } => Action::FailReply { id, cause },
            StagedAction::CloseSocket => Action::CloseSocket,
        };
        // `staged` and `out` share `MAX_ACTIONS_PER_CALL` as their
        // bound — staged's length is ≤ out's capacity, so push never
        // fails. `unwrap_or(())` discards the (unreachable) Err
        // branch cleanly: both Ok and Err arms evaluate to `()`,
        // satisfying the `must_use` on `push` without `let _` (banned)
        // or `drop(bool)` (clippy::drop_non_drop).
        out.push(a).unwrap_or(());
    }
    out
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
    use crate::password::Password;
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
        // DEF-112: per-variant extraction. `ReplyId<PingKind>` and
        // `ReplyId<StartupKind>` are distinct types; an or-pattern
        // binding needs all alternatives to share the binding type.
        // Splitting per kind keeps the match exhaustive (compiler
        // will still flag a missing variant on future additions).
        match state {
            ProtoState::Idle | ProtoState::Errored(_) => {}
            ProtoState::AwaitingPingReply(id) => {
                id.consume();
            }
            ProtoState::ConnectingScramAwaitAuthOk(id)
            | ProtoState::ConnectingPostAuthWaitKey(id) => {
                id.consume();
            }
            ProtoState::ConnectingStartupTrust { reply }
            | ProtoState::ConnectingStartupScram { reply, .. }
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
        let startup_trust = ProtoState::ConnectingStartupTrust {
            reply: ReplyId::from_raw(nz(4)),
        };
        assert!(!allows_unsolicited_param_status(&startup_trust));
        consume_state(startup_trust);

        // ConnectingStartupScram — DEF-097 typestate carrying a
        // ScramSession. Constructor is infallible once we have a
        // Password (try_from_bytes is the only Err surface here,
        // architecturally unreachable for the fixture b"pw").
        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let scram = crate::scram::session::ScramSession::from_password(Sensitive::new(pw));
            let startup_scram = ProtoState::ConnectingStartupScram {
                reply: ReplyId::from_raw(nz(4001)),
                scram,
            };
            assert!(!allows_unsolicited_param_status(&startup_scram));
            consume_state(startup_scram);
        }

        // ConnectingScramAwaitServerFirst requires a Password *and* a
        // ScramSession (audit A2 typestate). The Password err branch
        // is architecturally unreachable for the fixture.
        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let scram = crate::scram::session::ScramSession::from_password(Sensitive::new(pw));
            let scram_first = ProtoState::ConnectingScramAwaitServerFirst {
                reply: ReplyId::from_raw(nz(5)),
                scram,
                client_first_bare: crate::ident::PodBytes::new(),
                client_nonce_b64: crate::ident::PodBytes::new(),
            };
            assert!(!allows_unsolicited_param_status(&scram_first));
            consume_state(scram_first);
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
        // DEF-061: Errored carries ErrorKind (1 byte), not the full
        // ProtocolError.
        let errored = ProtoState::Errored(crate::error::ErrorKind::Framing);
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
        // DEF-112: per-kind split (see sibling module's consume_state).
        match state {
            ProtoState::Idle | ProtoState::Errored(_) => {}
            ProtoState::AwaitingPingReply(id) => {
                id.consume();
            }
            ProtoState::ConnectingScramAwaitAuthOk(id)
            | ProtoState::ConnectingPostAuthWaitKey(id) => {
                id.consume();
            }
            ProtoState::ConnectingStartupTrust { reply }
            | ProtoState::ConnectingStartupScram { reply, .. }
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

    /// Like [`take_awaiting_ping_raw`] but for `ConnectingStartup*`
    /// (either Trust or Scram variant — both are valid post-push
    /// states depending on the credentials).
    fn take_connecting_startup_raw(new_state: ProtoState) -> Option<NonZeroU64> {
        match new_state {
            ProtoState::ConnectingStartupTrust { reply }
            | ProtoState::ConnectingStartupScram { reply, .. } => Some(reply.consume()),
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

    /// Test helper — run [`compute_push`] with a fresh [`WriteBuf`] and
    /// return (state, staged). DEF-094 made compute_push take
    /// `&mut WriteBuf`; this wrapper keeps the test-body callsites
    /// terse.
    fn compute_staged(cmd: PgCommand, state: ProtoState) -> (ProtoState, StagedActions) {
        let mut wb = WriteBuf::new();
        compute_push(cmd, state, &mut wb)
    }

    // -----------------------------------------------------------------
    // Ping — per-variant policy table
    // -----------------------------------------------------------------

    #[test]
    fn ping_from_idle_emits_sync_and_awaits() {
        let raw_id = nz(101);
        let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_id) };
        let (new_state, staged) = compute_staged(cmd, ProtoState::Idle);

        // Action: exactly one SendBytes whose payload is SYNC_WIRE_BYTES.
        // DEF-094: Ping from Idle emits the static SYNC const.
        assert_eq!(staged.len(), 1);
        assert!(
            matches!(
                staged.first(),
                Some(StagedAction::SendBytesStatic(s)) if *s == SYNC_WIRE_BYTES.as_slice()
            ),
            "expected SendBytesStatic(SYNC)",
        );

        // State: AwaitingPingReply(raw_id).
        assert_eq!(take_awaiting_ping_raw(new_state), Some(raw_id));
    }

    #[test]
    fn ping_from_errored_preserves_kind_and_fails_with_connection_already_closed() {
        // DEF-061 semantic: on push against Errored, we emit a
        // `ConnectionAlreadyClosed { prior_kind }` — the full original
        // cause was surfaced in the earlier FailReply (when the
        // connection was first torn down). The state retains only the
        // kind (1-byte Copy).
        use crate::error::ErrorKind;
        let raw_id = nz(102);
        let prior_kind = ErrorKind::Framing;
        let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_id) };
        let (new_state, staged) = compute_staged(cmd, ProtoState::Errored(prior_kind));

        // Action: FailReply(ConnectionAlreadyClosed{prior_kind}).
        assert_eq!(staged.len(), 1);
        assert!(
            matches!(
                staged.first(),
                Some(StagedAction::FailReply {
                    id,
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind: pk },
                }) if *id == raw_id && *pk == prior_kind
            ),
            "expected FailReply(ConnectionAlreadyClosed{{prior_kind={prior_kind:?}}})",
        );

        // State: Errored(prior_kind) preserved unchanged.
        assert!(
            matches!(&new_state, ProtoState::Errored(k) if *k == prior_kind),
            "expected Errored(prior_kind) preserved",
        );
        consume_state(new_state);
    }

    #[test]
    fn ping_from_awaiting_ping_reply_fails_with_unexpected_frame_and_preserves_state() {
        let raw_prev = nz(103);
        let raw_new = nz(104);
        let prev_state = ProtoState::AwaitingPingReply(ReplyId::from_raw(raw_prev));
        let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
        let (new_state, staged) = compute_staged(cmd, prev_state);

        // Action: FailReply(UnexpectedFrame { tag: b'P' }) for the NEW reply.
        assert_eq!(staged.len(), 1);
        assert!(
            matches!(
                staged.first(),
                Some(StagedAction::FailReply {
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
        // ConnectingStartupTrust — no credentials payload (DEF-097).
        {
            let raw_prev = nz(201);
            let raw_new = nz(202);
            let prev = ProtoState::ConnectingStartupTrust {
                reply: ReplyId::from_raw(raw_prev),
            };
            let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
            let (new_state, staged) = compute_staged(cmd, prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ConnectingStartup → expected FailReply(StartupAlreadyInProgress)",
            );
            assert_eq!(take_connecting_startup_raw(new_state), Some(raw_prev));
        }

        // ConnectingStartupScram — carries the ScramSession typestate
        // (DEF-097). Constructed via ScramSession::from_password.
        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let scram = crate::scram::session::ScramSession::from_password(Sensitive::new(pw));
            let raw_prev = nz(201_050);
            let raw_new = nz(201_051);
            let prev = ProtoState::ConnectingStartupScram {
                reply: ReplyId::from_raw(raw_prev),
                scram,
            };
            let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
            let (new_state, staged) = compute_staged(cmd, prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ConnectingStartupScram → expected StartupAlreadyInProgress",
            );
            assert_eq!(take_connecting_startup_raw(new_state), Some(raw_prev));
        }

        // ConnectingScramAwaitServerFirst — needs a Password and
        // ScramSession.
        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let scram = crate::scram::session::ScramSession::from_password(Sensitive::new(pw));
            let raw_prev = nz(203);
            let raw_new = nz(204);
            let prev = ProtoState::ConnectingScramAwaitServerFirst {
                reply: ReplyId::from_raw(raw_prev),
                scram,
                client_first_bare: crate::ident::PodBytes::new(),
                client_nonce_b64: crate::ident::PodBytes::new(),
            };
            let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
            let (new_state, staged) = compute_staged(cmd, prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
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
            let (new_state, staged) = compute_staged(cmd, prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
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
            let (new_state, staged) = compute_staged(cmd, prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
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
            let (new_state, staged) = compute_staged(cmd, prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
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
            let (new_state, staged) = compute_staged(cmd, prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
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
        let (new_state, staged) = compute_staged(cmd, ProtoState::Idle);

        // Action: SendBytes with non-empty payload (startup frame, no tag).
        // DEF-094: Startup from Idle writes the message into `wb` via
        // a StagedAction::SendBytesRange(NonEmptyRange). DEF-100:
        // non-empty is a type invariant, so presence of the variant
        // alone is sufficient — no explicit `end > start` check.
        assert_eq!(staged.len(), 1);
        assert!(
            matches!(staged.first(), Some(StagedAction::SendBytesRange(_))),
            "expected SendBytesRange into write_buf",
        );

        // State: ConnectingStartup with the pushed reply id.
        assert_eq!(take_connecting_startup_raw(new_state), Some(raw_id));
    }

    #[test]
    fn startup_from_errored_preserves_kind_and_fails_with_connection_already_closed() {
        // DEF-061 semantic — same shape as
        // `ping_from_errored_preserves_kind_and_fails_with_connection_already_closed`.
        use crate::error::ErrorKind;
        let Some(user) = mk_user() else { return };
        let raw_id = nz(302);
        let prior_kind = ErrorKind::Framing;
        let cmd = PgCommand::Startup {
            user,
            database: None,
            app_name: None,
            credentials: Credentials::Trust,
            reply: ReplyId::from_raw(raw_id),
        };
        let (new_state, staged) = compute_staged(cmd, ProtoState::Errored(prior_kind));

        // Action: FailReply(ConnectionAlreadyClosed{prior_kind}).
        assert_eq!(staged.len(), 1);
        assert!(
            matches!(
                staged.first(),
                Some(StagedAction::FailReply {
                    id,
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind: pk },
                }) if *id == raw_id && *pk == prior_kind
            ),
            "expected FailReply(ConnectionAlreadyClosed{{prior_kind={prior_kind:?}}})",
        );

        // State: Errored(prior_kind) preserved.
        assert!(
            matches!(&new_state, ProtoState::Errored(k) if *k == prior_kind),
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
            let (new_state, staged) = compute_staged(make_startup_cmd(user, raw_new), prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "AwaitingPingReply → expected StartupAlreadyInProgress",
            );
            assert_eq!(take_awaiting_ping_raw(new_state), Some(raw_prev));
        }

        // ConnectingStartupTrust (DEF-097 — the old
        // `ConnectingStartup { credentials }` split).
        if let Some(user) = mk_user() {
            let raw_prev = nz(403);
            let raw_new = nz(404);
            let prev = ProtoState::ConnectingStartupTrust {
                reply: ReplyId::from_raw(raw_prev),
            };
            let (new_state, staged) = compute_staged(make_startup_cmd(user, raw_new), prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ConnectingStartupTrust → expected StartupAlreadyInProgress",
            );
            assert_eq!(take_connecting_startup_raw(new_state), Some(raw_prev));
        }

        // ConnectingStartupScram — the other half of the DEF-097
        // credential split.
        if let (Some(user), Ok(pw)) = (mk_user(), Password::try_from_bytes(b"pw")) {
            let scram = crate::scram::session::ScramSession::from_password(Sensitive::new(pw));
            let raw_prev = nz(405_100);
            let raw_new = nz(405_101);
            let prev = ProtoState::ConnectingStartupScram {
                reply: ReplyId::from_raw(raw_prev),
                scram,
            };
            let (new_state, staged) = compute_staged(make_startup_cmd(user, raw_new), prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ConnectingStartupScram → expected StartupAlreadyInProgress",
            );
            assert_eq!(take_connecting_startup_raw(new_state), Some(raw_prev));
        }

        // ConnectingScramAwaitServerFirst. Construction requires
        // Password + ScramSession (audit A2 typestate).
        if let (Some(user), Ok(pw)) = (mk_user(), Password::try_from_bytes(b"pw")) {
            let scram = crate::scram::session::ScramSession::from_password(Sensitive::new(pw));
            let raw_prev = nz(405);
            let raw_new = nz(406);
            let prev = ProtoState::ConnectingScramAwaitServerFirst {
                reply: ReplyId::from_raw(raw_prev),
                scram,
                client_first_bare: crate::ident::PodBytes::new(),
                client_nonce_b64: crate::ident::PodBytes::new(),
            };
            let (new_state, staged) = compute_staged(make_startup_cmd(user, raw_new), prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
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
            let (new_state, staged) = compute_staged(make_startup_cmd(user, raw_new), prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
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
            let (new_state, staged) = compute_staged(make_startup_cmd(user, raw_new), prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
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
            let (new_state, staged) = compute_staged(make_startup_cmd(user, raw_new), prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
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
            let (new_state, staged) = compute_staged(make_startup_cmd(user, raw_new), prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedAction::FailReply {
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
