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

/// Maximum number of [`Action`]s a single entry-point call may emit.
///
/// **Phase 1a budget audit** (each entry-point bounded above):
///
/// - `push_command(Ping)` from `Idle` → 1 action (`SendBytes`).
/// - `push_command(Ping)` from any non-`Idle` state → not yet
///   reachable (only `AwaitingPingReply` exists, and §54 of the PG
///   protocol does not let us pipeline a second Ping concurrently —
///   the dispatcher refuses with `FailReply` + `CloseSocket`, **2
///   actions**).
/// - `feed_bytes(rfq)` from `AwaitingPingReply` → 1 action
///   (`DeliverReply`).
/// - `feed_bytes(error_response)` from `AwaitingPingReply` → 2
///   actions (`FailReply` + `CloseSocket`).
/// - `feed_bytes(malformed)` from any state → 2 actions
///   (`FailReply` + `CloseSocket`); if no in-flight reply, **1**
///   action (`CloseSocket` only).
/// - `feed_bytes(multiple frames in one chunk)` is a future concern.
///   Phase 1a's only inbound frame is RFQ (6 wire bytes); the read
///   buffer can hold up to `READ_BUF_CAP / 6 ≈ 682` of them, but only
///   one is meaningful at a time. The dispatcher runs one frame per
///   loop iteration; the loop bounds itself on `OutActions::push`
///   returning `Err` (the loop exits when the action vector is full).
///
/// Therefore the Phase 1a worst case is **2** actions per call. We
/// use **4** here to give the dispatcher loop one frame's slack so it
/// can advance and emit on the same call without being forced into a
/// second feed cycle. Bumping happens in 1c with the first multi-action
/// path and is enforced via per-call-site `const _: () = assert!(…)`
/// that lives next to the emission.
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
        // (a) `MAX_ACTIONS_PER_CALL` (push_action returns false when
        // full; we stop) and (b) the buffer being drained empty.
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
                    // If this is a ParameterStatus frame during a
                    // post-auth state, record the parameter before
                    // dispatching. Params live on PgProtocol, not in
                    // the state variant.
                    // If this is a ParameterStatus frame during a
                    // post-auth state, record the parameter. Params
                    // live on PgProtocol, not in the state variant.
                    if tag == crate::wire::TAG_PARAMETER_STATUS
                        && is_post_auth_state(&self.state)
                    {
                        record_param_status(&mut self.session_params, payload);
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
                            if let Some(act) = action
                                && push_action(&mut out, act).is_err()
                            {
                                // Action buffer exhausted before we
                                // could emit. Phase 1a budget (4) is
                                // comfortably above the worst case (2);
                                // this branch is currently dead but the
                                // loop bails cleanly if it ever fires.
                                break;
                            }
                        }
                        DispatchOutcome::Errored { reply_id, cause } => {
                            // State already taken (we called mem::take
                            // above). Do NOT advance the buffer — the
                            // connection is about to be closed. Store
                            // the cause as the root classification so
                            // post-close frames drop silently rather
                            // than misreading `Idle` as "ready".
                            //
                            // Clone: the cause goes both into the state
                            // (for post-terminal diagnostics) AND into
                            // the FailReply action. Error path only —
                            // zero hot-path impact.
                            self.state = ProtoState::Errored(cause.clone());
                            // Push FailReply + CloseSocket if there
                            // was an in-flight reply; otherwise just
                            // CloseSocket. Per-method budget: at most
                            // 2 actions; `MAX_ACTIONS_PER_CALL >= 2`
                            // (asserted const) guarantees push success.
                            // The Err branch is dead but the Result is
                            // surfaced so the compiler knows we saw it.
                            if let Some(id) = reply_id
                                && push_action(
                                    &mut out,
                                    Action::FailReply {
                                        id: id.consume(),
                                        cause,
                                    },
                                )
                                .is_err()
                            {
                                break;
                            }
                            if push_action(&mut out, Action::CloseSocket).is_err() {
                                break;
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
                let Ok(()) = push_action(
                    out,
                    Action::SendBytes(SendBuf::Static(&SYNC_WIRE_BYTES)),
                ) else {
                    return;
                };
            }
            ProtoState::AwaitingPingReply(prev_reply) => {
                self.state = ProtoState::AwaitingPingReply(prev_reply);
                let Ok(()) = push_action(
                    out,
                    Action::FailReply {
                        id: reply.consume(),
                        cause: ProtocolError::UnexpectedFrame { tag: b'P' },
                    },
                ) else {
                    return;
                };
            }
            // Any connecting state: reject the Ping — startup is in progress.
            other @ (ProtoState::ConnectingStartup { .. }
            | ProtoState::ConnectingScramAwaitServerFirst { .. }
            | ProtoState::ConnectingScramAwaitServerFinal { .. }
            | ProtoState::ConnectingScramAwaitAuthOk(_)
            | ProtoState::ConnectingPostAuthWaitKey(_)
            | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
                self.state = other;
                let Ok(()) = push_action(
                    out,
                    Action::FailReply {
                        id: reply.consume(),
                        cause: ProtocolError::StartupAlreadyInProgress,
                    },
                ) else {
                    return;
                };
            }
            ProtoState::Errored(original) => {
                let fail_cause = original.clone();
                self.state = ProtoState::Errored(original);
                let Ok(()) = push_action(
                    out,
                    Action::FailReply {
                        id: reply.consume(),
                        cause: fail_cause,
                    },
                ) else {
                    return;
                };
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
                // Build the StartupMessage frame.
                match build_startup_message(&user, database.as_ref(), app_name.as_ref()) {
                    Ok(send_buf) => {
                        self.state = ProtoState::ConnectingStartup {
                            reply,
                            credentials,
                        };
                        let Ok(()) = push_action(out, Action::SendBytes(send_buf)) else {
                            return;
                        };
                    }
                    Err(_) => {
                        // WriteBuf overflow building StartupMessage.
                        self.state = ProtoState::Idle;
                        let Ok(()) = push_action(
                            out,
                            Action::FailReply {
                                id: reply.consume(),
                                cause: ProtocolError::ScramError {
                                    detail: heapless::String::try_from(
                                        "StartupMessage too large for send buffer",
                                    )
                                    .unwrap_or_default(),
                                },
                            },
                        ) else {
                            return;
                        };
                    }
                }
            }
            // Already connecting or awaiting — reject.
            other @ (ProtoState::AwaitingPingReply(_)
            | ProtoState::ConnectingStartup { .. }
            | ProtoState::ConnectingScramAwaitServerFirst { .. }
            | ProtoState::ConnectingScramAwaitServerFinal { .. }
            | ProtoState::ConnectingScramAwaitAuthOk(_)
            | ProtoState::ConnectingPostAuthWaitKey(_)
            | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
                self.state = other;
                let Ok(()) = push_action(
                    out,
                    Action::FailReply {
                        id: reply.consume(),
                        cause: ProtocolError::StartupAlreadyInProgress,
                    },
                ) else {
                    return;
                };
            }
            ProtoState::Errored(original) => {
                let fail_cause = original.clone();
                self.state = ProtoState::Errored(original);
                let Ok(()) = push_action(
                    out,
                    Action::FailReply {
                        id: reply.consume(),
                        cause: fail_cause,
                    },
                ) else {
                    return;
                };
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
                let Ok(()) = push_action(out, Action::CloseSocket) else {
                    return;
                };
            }
            // All states carrying an in-flight ReplyId.
            ProtoState::AwaitingPingReply(id)
            | ProtoState::ConnectingStartup { reply: id, .. }
            | ProtoState::ConnectingScramAwaitServerFirst { reply: id, .. }
            | ProtoState::ConnectingScramAwaitServerFinal { reply: id, .. }
            | ProtoState::ConnectingScramAwaitAuthOk(id)
            | ProtoState::ConnectingPostAuthWaitKey(id)
            | ProtoState::ConnectingPostAuthHaveKey { reply: id, .. } => {
                self.state = ProtoState::Errored(cause.clone());
                self.read_buf.clear();
                let Ok(()) = push_action(
                    out,
                    Action::FailReply {
                        id: id.consume(),
                        cause,
                    },
                ) else {
                    return;
                };
                let Ok(()) = push_action(out, Action::CloseSocket) else {
                    return;
                };
            }
            ProtoState::Errored(original) => {
                self.state = ProtoState::Errored(original);
            }
        }
    }

}

/// Check if a state is a post-auth connecting state where
/// ParameterStatus should be recorded.
fn is_post_auth_state(state: &ProtoState) -> bool {
    matches!(
        state,
        ProtoState::ConnectingPostAuthWaitKey(_)
            | ProtoState::ConnectingPostAuthHaveKey { .. }
    )
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

/// Push an action into the bounded out-list.
///
/// Returns `Err(action)` if the buffer is full — the caller decides
/// how to surface the budget breach. In Phase 1a all call sites are
/// proved safe by the `MAX_ACTIONS_PER_CALL` const_assert and the
/// per-method audit in the docstring; the Err branch is dead.
#[inline]
#[expect(clippy::result_large_err, reason = "no_alloc: Box unavailable; Action is returned on overflow which is architecturally unreachable")]
fn push_action(out: &mut OutActions, action: Action) -> Result<(), Action> {
    out.push(action)
}
