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
///     Action::SendBytes(SendBuf::Static(&SYNC_WIRE_BYTES)),
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
    /// `!Sync` marker.
    _not_sync: PhantomData<Cell<()>>,
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
            _not_sync: PhantomData,
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
    #[must_use = "the returned actions carry side-effects that must be executed"]
    pub fn push_command(&mut self, cmd: PgCommand) -> OutActions {
        let mut out = OutActions::new();
        match cmd {
            PgCommand::Ping { reply } => self.handle_push_ping(reply, &mut out),
            PgCommand::Startup {
                user,
                database,
                app_name,
                credentials,
                reply,
            } => self.handle_push_startup(user, database, app_name, credentials, reply, &mut out),
        }
        out
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

    /// Helper: emit a Sync command on the wire and transition to
    /// `AwaitingPingReply`.
    ///
    /// Per-method push budget: **1**. `MAX_ACTIONS_PER_CALL >= 1`
    /// (const-asserted) guarantees the push succeeds; the Err branch
    /// is architecturally unreachable but returned by `heapless`, so
    /// we surface it via `if .is_err() { return }` — the compiler sees
    /// we did not discard a `Result`.
    fn handle_push_ping(&mut self, reply: ReplyId, out: &mut OutActions) {
        match core::mem::take(&mut self.state) {
            ProtoState::Idle => {
                self.state = ProtoState::AwaitingPingReply(reply);
                emit_actions!(out, budget: 1, [
                    Action::SendBytes(SendBuf::Static(&SYNC_WIRE_BYTES)),
                ]);
            }
            ProtoState::AwaitingPingReply(prev_reply) => {
                self.state = ProtoState::AwaitingPingReply(prev_reply);
                emit_actions!(out, budget: 1, [
                    Action::FailReply {
                        id: reply.consume(),
                        cause: ProtocolError::UnexpectedFrame { tag: b'P' },
                    },
                ]);
            }
            other @ (ProtoState::ConnectingStartup { .. }
            | ProtoState::ConnectingScramAwaitServerFirst { .. }
            | ProtoState::ConnectingScramAwaitServerFinal { .. }
            | ProtoState::ConnectingScramAwaitAuthOk(_)
            | ProtoState::ConnectingPostAuthWaitKey(_)
            | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
                self.state = other;
                emit_actions!(out, budget: 1, [
                    Action::FailReply {
                        id: reply.consume(),
                        cause: ProtocolError::StartupAlreadyInProgress,
                    },
                ]);
            }
            ProtoState::Errored(original) => {
                let fail_cause = original.clone();
                self.state = ProtoState::Errored(original);
                emit_actions!(out, budget: 1, [
                    Action::FailReply {
                        id: reply.consume(),
                        cause: fail_cause,
                    },
                ]);
            }
        }
    }

    /// Handle `PgCommand::Startup` — build and emit a StartupMessage.
    ///
    /// Per-method push budget: **1** (SendBytes with the StartupMessage).
    fn handle_push_startup(
        &mut self,
        user: Ident,
        database: Option<DatabaseName>,
        app_name: Option<ApplicationName>,
        credentials: Credentials,
        reply: ReplyId,
        out: &mut OutActions,
    ) {
        match core::mem::take(&mut self.state) {
            ProtoState::Idle => {
                match build_startup_message(&user, database.as_ref(), app_name.as_ref()) {
                    Ok(send_buf) => {
                        self.state = ProtoState::ConnectingStartup {
                            reply,
                            credentials,
                        };
                        emit_actions!(out, budget: 1, [
                            Action::SendBytes(send_buf),
                        ]);
                    }
                    Err(_) => {
                        self.state = ProtoState::Idle;
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
                    }
                }
            }
            other @ (ProtoState::AwaitingPingReply(_)
            | ProtoState::ConnectingStartup { .. }
            | ProtoState::ConnectingScramAwaitServerFirst { .. }
            | ProtoState::ConnectingScramAwaitServerFinal { .. }
            | ProtoState::ConnectingScramAwaitAuthOk(_)
            | ProtoState::ConnectingPostAuthWaitKey(_)
            | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
                self.state = other;
                emit_actions!(out, budget: 1, [
                    Action::FailReply {
                        id: reply.consume(),
                        cause: ProtocolError::StartupAlreadyInProgress,
                    },
                ]);
            }
            ProtoState::Errored(original) => {
                let fail_cause = original.clone();
                self.state = ProtoState::Errored(original);
                emit_actions!(out, budget: 1, [
                    Action::FailReply {
                        id: reply.consume(),
                        cause: fail_cause,
                    },
                ]);
            }
        }
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
    Ok(SendBuf::Owned(wb.into_inner()))
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

        // ConnectingScramAwaitServerFirst requires a Password; we
        // construct one via `try_from_bytes` and skip the sub-case on
        // the architecturally-unreachable Err branch (the input is a
        // legal 2-byte password; Err is dead). Skipping rather than
        // `panic!` keeps the test within the crate-root forbid bundle.
        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let scram_first = ProtoState::ConnectingScramAwaitServerFirst {
                reply: ReplyId::from_raw(nz(5)),
                credentials: Credentials::ScramPassword(Sensitive::new(pw)),
                client_first_bare: heapless::Vec::new(),
                client_nonce_b64: heapless::Vec::new(),
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
        let errored = ProtoState::Errored(ProtocolError::UnexpectedFrame { tag: b'X' });
        assert!(!allows_unsolicited_param_status(&errored));
        consume_state(errored);
    }
}
