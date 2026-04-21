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
use crate::dispatch::{
    AbsFrameStart, DispatchOutcome, FrameCoords, FrameTotalLen, PopulatedLen, dispatch,
};
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
/// - `feed_bytes(rfq)` from `PingAwaitingRfq` → 1 action.
/// - `feed_bytes(error_response)` from any flight state → 2 actions
///   (`FailReply` + `CloseSocket`).
/// - `feed_bytes(malformed)` → 2 actions (or 1 if no in-flight reply).
/// - `feed_bytes(multiple frames in one chunk)` — per-iteration
///   budget check (below) gates entry; overflow is architecturally
///   unreachable.
///
/// # 1c-1b bump: 4 → 8
///
/// Row streaming emits one `StreamRow` per `DataRow` frame. A single
/// `feed_bytes` call receiving 7 rows + `CommandComplete` +
/// `ReadyForQuery` produces 7 × `StreamRow` + 1 × `DeliverReply`.
/// Keeping `MAX_ACTIONS_PER_CALL = 4` would force 2+ extra
/// `feed_bytes` calls per batch; 8 covers realistic streaming
/// density with single-digit call counts on typical row sizes.
pub const MAX_ACTIONS_PER_CALL: usize = 8;

/// Worst-case number of actions a single dispatch iteration can
/// emit. Used as the budget-check reserve in [`PgProtocol::feed_bytes`]:
/// a loop iteration enters only if
/// `staged.len() + WORST_CASE_PER_DISPATCH ≤ MAX_ACTIONS_PER_CALL`,
/// so overflow inside the iteration is architecturally unreachable —
/// no partial emission, no silent reply loss (1c-1b DEF-121).
///
/// Current worst case: [`DispatchOutcome::Errored`] with `Some(reply_id)`
/// emits `FailReply + CloseSocket` = 2. Bumping this to 3 would require
/// a new 3-action dispatch outcome.
const WORST_CASE_PER_DISPATCH: usize = 2;

// Sanity asserts — the budget audit above demands at least
// WORST_CASE_PER_DISPATCH; practical batching needs meaningful
// headroom above that.
const _: () = assert!(MAX_ACTIONS_PER_CALL >= WORST_CASE_PER_DISPATCH);
const _: () = assert!(MAX_ACTIONS_PER_CALL >= 4, "practical batching needs ≥4 slots");

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
    ///
    /// F19 removed the former `row_desc: Option<RowDesc>` slot —
    /// schema now lives inline in the `SimpleQueryStreamingRows` /
    /// `SimpleQueryAwaitingRfq` state variants. The slot-vs-state
    /// parallel-representation drift seam is closed structurally.
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

        // F19: no slot-clear needed. Schema lives in state variants
        // (`SimpleQueryStreamingRows` / `SimpleQueryAwaitingRfq`)
        // which are consumed by Z → Idle on query completion. A new
        // `SimpleQuery` push from Idle has no prior schema to carry
        // over; the state variants don't exist outside their query.
        let prev = core::mem::take(&mut self.state);
        let (new_state, staged) = compute_push(cmd, prev, write_buf);
        self.state = new_state;
        materialise(staged, write_buf.as_bytes(), &[])
    }

    /// Extended Query "Bind + Execute + Sync" pipeline — send a
    /// prepared statement's portal with bound parameters and run it.
    ///
    /// Sibling to [`Self::push_command`]; lives on a separate method
    /// because `PgCommand` is a type-unified enum with no generic
    /// parameter surface, whereas Bind/Execute is parameterised over
    /// the caller's parameter tuple type `P: ParamsWriter`.
    ///
    /// # Parameters
    ///
    /// - `portal_name` — the name the server will use to address the
    ///   bound portal. Pass `PortalName::default()` for the
    ///   unnamed-portal convention (most common).
    /// - `stmt_name` — name of a previously-[`push_command`]'d
    ///   [`PgCommand::Parse`] statement. Passing an unparsed stmt
    ///   name → server `ErrorResponse` → `FailReply` (tier-3 server
    ///   check; Phase 2 macro elevates to tier-1 via stmt-cache
    ///   fingerprint).
    /// - `params` — a tuple implementing [`crate::params::ParamsWriter`].
    ///   Arity 0..=16 supported by default impls. The tuple's
    ///   element types must each impl [`crate::decode::EncodeBinary`].
    /// - `row_desc` — pre-provided result-set schema. `Some(desc)`
    ///   for SELECT (caller ran a prior Describe externally or at
    ///   macro-compile time); `None` for DML / RETURNING-less stmts.
    ///   A `None` row_desc + server-emitted DataRow is a tier-2
    ///   shield — the dispatch arm classifies as UnexpectedFrame.
    /// - `fetch` — row-count scope. 1c-3b only accepts
    ///   [`crate::FetchRows::All`] (fetch all rows). The enum's
    ///   `#[non_exhaustive]` leaves room for a `Chunked(NonZeroU32)`
    ///   variant in 1c-6 when chunked-fetch flow lands. F83: using
    ///   an enum instead of `u32` promotes the "must be zero" scope
    ///   guard from tier-3 docs to tier-1 compile.
    /// - `reply` — typed correlator; delivered via
    ///   [`crate::Action::DeliverReply`] as [`crate::Reply::QueryComplete`]
    ///   on success.
    /// - `write_buf` — caller-owned outbound staging buffer (DEF-094).
    ///
    /// # Emitted actions (happy path)
    ///
    /// Three [`crate::Action::SendBytes`] actions: the Bind frame,
    /// the Execute frame, and the 5-byte static `Sync`. The caller
    /// writes all three to the socket in order.
    ///
    /// # Failure modes
    ///
    /// See `compute_push_bind_execute`'s decision table for per-state
    /// policy. Every non-`Idle` entry state emits a classified
    /// `FailReply` and preserves the prior state; the connection
    /// is not torn down.
    #[expect(clippy::too_many_arguments, reason = "push_bind_execute mirrors the PG Bind+Execute wire contract 1:1 — each argument is a distinct wire-protocol input. Splitting into a struct-arg (BindExecuteRequest { ... }) trades arg-count for construction verbosity at every call site, no tier or safety win.")]
    #[must_use = "the returned actions carry side-effects that must be executed"]
    pub fn push_bind_execute<'w, P: crate::params::ParamsWriter>(
        &mut self,
        portal_name: &crate::ident::PortalName,
        stmt_name: &crate::ident::StmtName,
        params: &P,
        row_desc: Option<crate::decode::RowDesc>,
        fetch: crate::command::FetchRows,
        reply: ReplyId<crate::reply_id::QueryKind>,
        write_buf: &'w mut WriteBuf,
    ) -> OutActions<'w, 'static> {
        write_buf.clear();
        let mut staged = StagedActions::new();
        let prev = core::mem::take(&mut self.state);
        let new_state = compute_push_bind_execute(
            prev,
            portal_name,
            stmt_name,
            params,
            row_desc,
            fetch,
            reply,
            &mut staged,
            write_buf,
        );
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
        let staged = StagedActions::new();

        // F66 (pass #6 audit): if the connection is already Errored,
        // drop the incoming bytes and return an empty OutActions.
        // Rationale:
        //   1. The caller already received `CloseSocket` at the
        //      original fail point; the socket is being torn down.
        //   2. The Errored dispatch arm would consume bytes frame-by-
        //      frame with `AdvancedSilent` — wasted CPU on each byte
        //      from a connection that's already dead.
        //   3. An adversarial server flooding a post-close socket
        //      can't force useless parse work: we skip the loop
        //      entirely.
        // Also clear `read_buf` — post-Close bytes are wire-garbage
        // from a dead connection; keeping them wastes memory.
        if matches!(self.state, ProtoState::Errored(_)) {
            self.read_buf.clear();
            return materialise(staged, write_buf.as_bytes(), self.read_buf.populated());
        }

        let mut staged = staged;

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
            return materialise(
                staged,
                write_buf.as_bytes(),
                self.read_buf.populated(),
            );
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
                        // F35: outcome classified but currently both
                        // variants consumed silently — Phase 1d will
                        // add `Action::EmitPsAdvisory` to forward
                        // `MalformedPayload` events to the wrapper
                        // for proxy-interference detection.
                        match record_param_status(&mut self.session_params, payload) {
                            ParamStatusRecordOutcome::Processed
                            | ParamStatusRecordOutcome::MalformedPayload => {
                                // Phase 1c: silently consume both.
                            }
                        }
                        let Ok(()) = self.read_buf.advance(total_len) else {
                            self.fail_inflight_and_close(
                                ProtocolError::ReadCursorAdvanceUnreachable,
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
                                ProtocolError::ReadCursorAdvanceUnreachable,
                                &mut staged,
                            );
                            break;
                        };
                        continue;
                    }

                    // 1c-1b: absolute frame start/payload bounds into
                    // `populated()` are stable across the upcoming
                    // `advance` — the bytes themselves don't move
                    // until the next `append` triggers lazy
                    // compaction, which can't happen while
                    // OutActions is alive. Pass to dispatch so the
                    // DataRow arm can construct an absolute row_range.
                    //
                    // Typed newtypes (`AbsFrameStart`, `FrameTotalLen`,
                    // `PopulatedLen`) — swap two args at the
                    // `FrameCoords::new` call site below = build error.
                    // Derived offsets (payload_start, payload_end)
                    // live inside `FrameCoords` and cannot be
                    // mis-ordered by a caller.
                    let frame_start = AbsFrameStart(self.read_buf.cursor_position());
                    let frame_len = FrameTotalLen(total_len);
                    let populated = PopulatedLen(self.read_buf.populated().len());

                    // DEF-121 budget gate — prevent mid-transition
                    // overflow. A dispatch iteration can emit up to
                    // `WORST_CASE_PER_DISPATCH` (2) actions; if
                    // staged cannot absorb them we break **before**
                    // `core::mem::take(&mut self.state)` and
                    // `dispatch()` — so no reply is consumed, no
                    // state is mutated. The frame stays in the read
                    // buffer for the next `feed_bytes` call.
                    //
                    // Without this gate the `on_overflow: break`
                    // inside `emit_actions!` would fire AFTER the
                    // dispatcher had already consumed the reply
                    // (via `deliver()` or `errored()`) and
                    // transitioned state — dropping the action and
                    // orphaning the caller's oneshot. Tier-4 silent
                    // reply loss → tier-2 structural via the gate.
                    if staged.len().saturating_add(WORST_CASE_PER_DISPATCH)
                        > MAX_ACTIONS_PER_CALL
                    {
                        break;
                    }

                    let prev = core::mem::take(&mut self.state);
                    let outcome = dispatch(
                        prev,
                        tag,
                        payload,
                        write_buf,
                        FrameCoords::new(frame_start, frame_len, populated),
                    );
                    match outcome {
                        DispatchOutcome::AdvancedSilent { new_state } => {
                            self.state = new_state;
                            let Ok(()) = self.read_buf.advance(total_len) else {
                                self.fail_inflight_and_close(
                                    ProtocolError::ReadCursorAdvanceUnreachable,
                                    &mut staged,
                                );
                                break;
                            };
                        }
                        DispatchOutcome::AdvancedWithAction { new_state, action } => {
                            self.state = new_state;
                            let Ok(()) = self.read_buf.advance(total_len) else {
                                self.fail_inflight_and_close(
                                    ProtocolError::ReadCursorAdvanceUnreachable,
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

        // 1c-1b: `populated()` — the full buffer contents including
        // bytes advanced past the cursor during this loop — so that
        // `StreamRowRange` slices resolve correctly. The bytes
        // remain valid until the next `append` triggers lazy
        // compaction, which the borrow checker forbids while
        // `OutActions<'_, 'r>` is alive (`'r = &'r mut self`).
        //
        // F19: `StreamRow` actions now carry `RowDesc` BY VALUE,
        // copied from the StreamingRows state variant at emission.
        // No separate `row_desc` arg to materialise.
        materialise(staged, write_buf.as_bytes(), self.read_buf.populated())
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
        // Already terminal: skip re-fire. The wrapper saw the full
        // cause on the first fatal; a repeat CloseSocket would be
        // duplicative, and any FailReply would consume a non-existent
        // correlator (typestate prevents it — the first transition
        // already drained the `ReplyId`).
        if matches!(self.state, ProtoState::Errored(_)) {
            return;
        }
        // DEF-061 + DEF-094: compute kind before `cause` is moved
        // into the FailReply StagedAction. Stored in state as 1-byte
        // ErrorKind; full cause goes out in the one FailReply emitted
        // below. `staged` is the phase-1 accumulator; entry-point
        // materialises into `OutActions<'buf>`.
        let kind = cause.kind();
        // DEF-117: `core::mem::replace` directly installs the
        // terminal `Errored(kind)` state, eliminating the
        // transient `Idle`-window that `core::mem::take` would
        // create. No intermediate `Idle` for a concurrent &self
        // read (if one were added) to misinterpret as "healthy".
        //
        // DEF-112: typed `ReplyId<K>` are distinct types per phase
        // kind; extraction is centralised in
        // [`ProtoState::inflight_reply_raw_id`] — one exhaustive
        // match in `state.rs`, not duplicated here.
        let prev = core::mem::replace(&mut self.state, ProtoState::Errored(kind));
        let raw_id = prev.inflight_reply_raw_id();
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
        PgCommand::SimpleQuery { sql, reply } => {
            compute_push_simple_query(state, &sql, reply, &mut staged, write_buf)
        }
        PgCommand::Parse {
            stmt_name,
            sql,
            reply,
        } => compute_push_parse(state, &stmt_name, &sql, reply, &mut staged, write_buf),
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
/// | `Idle`                     | `SendBytes(SYNC)`      | `PingAwaitingRfq(reply)`|
/// | `Errored(cause)`           | `FailReply(cause)`     | `Errored(cause)` preserved|
/// | `PingAwaitingRfq(prev)`    | `FailReply(CommandInProgress)` | `PingAwaitingRfq(prev)`  |
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
            ProtoState::PingAwaitingRfq(reply)
        }
        // ERRORED ARM — LOAD-BEARING FOR DIAGNOSTIC CLASSIFICATION.
        //
        // Without an explicit Errored arm here, the `other @ (...)`
        // catch-all below would ALSO match Errored (ProtoState is not
        // #[non_exhaustive] internally; all variants are listed there).
        // State preservation works either way — `other => other` keeps
        // Errored intact. BUT the emitted FailReply cause would be
        // `CommandInProgress` / `StartupAlreadyInProgress` instead of
        // the correct `ConnectionAlreadyClosed { prior_kind }`, which
        // is the only diagnostic that tells the wrapper crate "this
        // connection is already terminal, don't retry".
        //
        // So this arm is tier-3 for diagnostic QUALITY (not tier-2 for
        // state safety). `compute_push_tests::ping_from_errored_preserves_kind...`
        // pins the invariant; the four sibling helpers
        // (compute_push_startup / _simple_query / _parse) have an
        // identical arm for the same reason.
        ProtoState::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
            ProtoState::Errored(prior_kind)
        }
        ProtoState::PingAwaitingRfq(prev_reply) => {
            // Pushing a Ping while another Ping is in flight is a
            // push-path error (not a wire-framing issue), so
            // classify as `CommandInProgress` rather than
            // overloading `UnexpectedFrame` with a synthetic tag
            // byte. Matches the semantics used by
            // `compute_push_simple_query` for the analogous case.
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
            ProtoState::PingAwaitingRfq(prev_reply)
        }
        other @ (ProtoState::ConnectingStartupTrust { .. }
        | ProtoState::ConnectingStartupScram { .. }
        | ProtoState::ConnectingScramAwaitingServerFirst { .. }
        | ProtoState::ConnectingScramAwaitingServerFinal { .. }
        | ProtoState::ConnectingScramAwaitingAuthOk(_)
        | ProtoState::ConnectingPostAuthAwaitingKey(_)
        | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            other
        }
        other @ (ProtoState::SimpleQueryAwaitingFirstResponse(_)
        | ProtoState::SimpleQueryStreamingRows { .. }
        | ProtoState::SimpleQueryAwaitingRfq { .. }
        | ProtoState::DrainRfqAfterError
        | ProtoState::ParseAwaitingParseComplete(_)
        | ProtoState::ParseAwaitingRfq(_)
        | ProtoState::BindExecuteAwaitingBindCompleteDml(_)
        | ProtoState::BindExecuteAwaitingCommandCompleteDml(_)
        | ProtoState::BindExecuteAwaitingRfqDml { .. }
        | ProtoState::BindExecuteAwaitingBindCompleteSelect { .. }
        | ProtoState::BindExecuteAwaitingDataOrCompleteSelect { .. }
        | ProtoState::BindExecuteStreamingRows { .. }
        | ProtoState::BindExecuteAwaitingRfqSelect { .. }) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
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
                // TIER-1 COMPILE DEAD BRANCH. The const assert in
                // `write_buf.rs`:
                //     MAX_OWNED_SEND_LEN >= max_startup_message_size()
                // proves at build time that `build_startup_message`
                // with validated `Ident` + `DatabaseName` +
                // `ApplicationName` inputs cannot overflow the
                // WriteBuf — their total bounded length is
                // accounted for in the size computation. The Err
                // arm is preserved for `match` exhaustiveness only.
                emit_actions!(staged, budget: 1, [
                    StagedAction::FailReply {
                        id: reply.consume(),
                        cause: ProtocolError::OutboundFrameBuildUnreachable {
                            stage: crate::error::FrameBuildStage::Startup,
                        },
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
        other @ (ProtoState::PingAwaitingRfq(_)
        | ProtoState::ConnectingStartupTrust { .. }
        | ProtoState::ConnectingStartupScram { .. }
        | ProtoState::ConnectingScramAwaitingServerFirst { .. }
        | ProtoState::ConnectingScramAwaitingServerFinal { .. }
        | ProtoState::ConnectingScramAwaitingAuthOk(_)
        | ProtoState::ConnectingPostAuthAwaitingKey(_)
        | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            other
        }
        other @ (ProtoState::SimpleQueryAwaitingFirstResponse(_)
        | ProtoState::SimpleQueryStreamingRows { .. }
        | ProtoState::SimpleQueryAwaitingRfq { .. }
        | ProtoState::DrainRfqAfterError
        | ProtoState::ParseAwaitingParseComplete(_)
        | ProtoState::ParseAwaitingRfq(_)
        | ProtoState::BindExecuteAwaitingBindCompleteDml(_)
        | ProtoState::BindExecuteAwaitingCommandCompleteDml(_)
        | ProtoState::BindExecuteAwaitingRfqDml { .. }
        | ProtoState::BindExecuteAwaitingBindCompleteSelect { .. }
        | ProtoState::BindExecuteAwaitingDataOrCompleteSelect { .. }
        | ProtoState::BindExecuteStreamingRows { .. }
        | ProtoState::BindExecuteAwaitingRfqSelect { .. }) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
            other
        }
    }
}

/// Compute the transition for [`PgCommand::SimpleQuery`] against the
/// current [`ProtoState`]. Pure; see [`compute_push`] for framing.
///
/// Decision table:
///
/// | current state                | action                              | new state                            |
/// |------------------------------|-------------------------------------|--------------------------------------|
/// | `Idle` (build OK)            | `SendBytes('Q' frame)`              | `SimpleQueryAwaitingFirstResponse(id)`  |
/// | `Idle` (build Err)           | `FailReply(OutboundFrameBuildUnreachable)`| `Idle` (unchanged)             |
/// | `Errored(kind)`              | `FailReply(ConnectionAlreadyClosed)`| `Errored(kind)` preserved            |
/// | any `Connecting*`            | `FailReply(StartupAlreadyInProgress)`| same state preserved                |
/// | `PingAwaitingRfq(prev)`    | `FailReply(CommandInProgress)`      | same                                 |
/// | any `SimpleQuery*`           | `FailReply(CommandInProgress)`      | same                                 |
fn compute_push_simple_query(
    state: ProtoState,
    sql: &crate::ident::Sql,
    reply: ReplyId<crate::reply_id::QueryKind>,
    staged: &mut StagedActions,
    write_buf: &mut WriteBuf,
) -> ProtoState {
    match state {
        ProtoState::Idle => match build_query_message(sql, write_buf) {
            Ok(range) => {
                emit_actions!(staged, budget: 1, [
                    StagedAction::SendBytesRange(range),
                ]);
                ProtoState::SimpleQueryAwaitingFirstResponse(reply)
            }
            Err(_) => {
                // TIER-1 COMPILE DEAD BRANCH. The const assert in
                // `write_buf.rs`:
                //     MAX_OWNED_SEND_LEN >= max_simple_query_message_size()
                // proves at build time that a `Sql` constructed via
                // `from_str_truncating` (bounded `MAX_SQL_LEN`) cannot
                // overflow the WriteBuf. `build_query_message`'s
                // `Err(WriteBufFull)` therefore cannot fire in
                // production — the arm exists solely to satisfy
                // `match` exhaustiveness under the `clippy::unwrap_used`
                // ban. A future refactor that breaks the size
                // invariant would fail the const assert first.
                emit_actions!(staged, budget: 1, [
                    StagedAction::FailReply {
                        id: reply.consume(),
                        cause: ProtocolError::OutboundFrameBuildUnreachable {
                            stage: crate::error::FrameBuildStage::Query,
                        },
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
        other @ (ProtoState::ConnectingStartupTrust { .. }
        | ProtoState::ConnectingStartupScram { .. }
        | ProtoState::ConnectingScramAwaitingServerFirst { .. }
        | ProtoState::ConnectingScramAwaitingServerFinal { .. }
        | ProtoState::ConnectingScramAwaitingAuthOk(_)
        | ProtoState::ConnectingPostAuthAwaitingKey(_)
        | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            other
        }
        other @ (ProtoState::PingAwaitingRfq(_)
        | ProtoState::SimpleQueryAwaitingFirstResponse(_)
        | ProtoState::SimpleQueryStreamingRows { .. }
        | ProtoState::SimpleQueryAwaitingRfq { .. }
        | ProtoState::DrainRfqAfterError
        | ProtoState::ParseAwaitingParseComplete(_)
        | ProtoState::ParseAwaitingRfq(_)
        | ProtoState::BindExecuteAwaitingBindCompleteDml(_)
        | ProtoState::BindExecuteAwaitingCommandCompleteDml(_)
        | ProtoState::BindExecuteAwaitingRfqDml { .. }
        | ProtoState::BindExecuteAwaitingBindCompleteSelect { .. }
        | ProtoState::BindExecuteAwaitingDataOrCompleteSelect { .. }
        | ProtoState::BindExecuteStreamingRows { .. }
        | ProtoState::BindExecuteAwaitingRfqSelect { .. }) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
            other
        }
    }
}

/// Build a PostgreSQL simple-query frame: `'Q'` + 4-byte length +
/// NUL-terminated SQL.
///
/// PG frame body layout (§55.7 "Simple Query"):
/// - Tag: `'Q'` (1 byte)
/// - Length: u32 BE including itself
/// - Query string: NUL-terminated
fn build_query_message(
    sql: &crate::ident::Sql,
    write_buf: &mut WriteBuf,
) -> Result<crate::action::NonEmptyRange, crate::write_buf::WriteBufFull> {
    use crate::write_buf::WriteBufFull;

    let start = write_buf.len();
    write_buf.push_u8(crate::wire::TAG_QUERY.byte())?;
    write_buf.with_length_prefix(|w| {
        w.push_nul_terminated(sql.as_bytes())
    })?;
    // The 'Q' frame is always ≥ 6 bytes (tag + length + NUL) — the
    // NonEmptyRange constructor succeeds by construction.
    crate::action::NonEmptyRange::from_write_span(start, write_buf).ok_or(WriteBufFull)
}

/// Build a PostgreSQL Extended Query `Parse` frame (PG §55.7).
///
/// Wire layout: tag `'P'`, 4-byte BE length (self-inclusive),
/// NUL-terminated statement name (empty = unnamed statement),
/// NUL-terminated SQL text, then an `i16` BE parameter-type count
/// (always zero in 1c-3a — no parameter-type hints; 1c-3b adds
/// per-parameter OID hints and widens this field).
fn build_parse_message(
    stmt_name: &crate::ident::StmtName,
    sql: &crate::ident::Sql,
    write_buf: &mut WriteBuf,
) -> Result<crate::action::NonEmptyRange, crate::write_buf::WriteBufFull> {
    use crate::write_buf::WriteBufFull;

    let start = write_buf.len();
    write_buf.push_u8(crate::wire::TAG_PARSE.byte())?;
    write_buf.with_length_prefix(|w| {
        w.push_nul_terminated(stmt_name.as_bytes())?;
        w.push_nul_terminated(sql.as_bytes())?;
        // n_param_types = 0; 1c-3b will widen to push actual OIDs here.
        w.push_i16_be(0)
    })?;
    crate::action::NonEmptyRange::from_write_span(start, write_buf).ok_or(WriteBufFull)
}

/// Compute the transition for [`PgCommand::Parse`] against the
/// current [`ProtoState`]. Pure; see [`compute_push`] for framing.
///
/// Happy path emits TWO actions: a `SendBytes(Parse frame)` and
/// a `SendBytes(SYNC)`. The Sync is a `'static` const
/// (`SYNC_WIRE_BYTES`) emitted via `StagedAction::SendBytesStatic`
/// for zero-copy; the Parse frame is written into the caller's
/// `WriteBuf` and referenced via `StagedAction::SendBytesRange`.
///
/// Decision table:
///
/// | current state                | action                              | new state                            |
/// |------------------------------|-------------------------------------|--------------------------------------|
/// | `Idle` (build OK)            | 2× `SendBytes(Parse, SYNC)`         | `ParseAwaitingParseComplete(reply)`  |
/// | `Idle` (build Err)           | `FailReply(OutboundFrameBuildUnreachable)`| `Idle` (unchanged)             |
/// | `Errored(kind)`              | `FailReply(ConnectionAlreadyClosed)`| `Errored(kind)` preserved            |
/// | `Connecting*`                | `FailReply(StartupAlreadyInProgress)`| same                                |
/// | `Awaiting*` / `SimpleQuery*` | `FailReply(CommandInProgress)`      | same                                 |
/// | `Parse*`                     | `FailReply(CommandInProgress)`      | same                                 |
fn compute_push_parse(
    state: ProtoState,
    stmt_name: &crate::ident::StmtName,
    sql: &crate::ident::Sql,
    reply: ReplyId<crate::reply_id::ParseKind>,
    staged: &mut StagedActions,
    write_buf: &mut WriteBuf,
) -> ProtoState {
    match state {
        ProtoState::Idle => match build_parse_message(stmt_name, sql, write_buf) {
            Ok(range) => {
                // Emit Parse frame (range into write_buf) + bundled
                // Sync (static const). Both needed for PG to flush
                // ParseComplete (without Sync the server buffers
                // forever). 2-action site.
                emit_actions!(staged, budget: 2, [
                    StagedAction::SendBytesRange(range),
                    StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
                ]);
                ProtoState::ParseAwaitingParseComplete(reply)
            }
            Err(_) => {
                // TIER-1 COMPILE DEAD BRANCH. The const assert in
                // write_buf.rs proves:
                //   MAX_OWNED_SEND_LEN >= max_parse_message_size()
                // so `build_parse_message` on `StmtName` + `Sql`
                // bounded by their truncating constructors cannot
                // overflow the WriteBuf.
                emit_actions!(staged, budget: 1, [
                    StagedAction::FailReply {
                        id: reply.consume(),
                        cause: ProtocolError::OutboundFrameBuildUnreachable {
                            stage: crate::error::FrameBuildStage::Parse,
                        },
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
        other @ (ProtoState::ConnectingStartupTrust { .. }
        | ProtoState::ConnectingStartupScram { .. }
        | ProtoState::ConnectingScramAwaitingServerFirst { .. }
        | ProtoState::ConnectingScramAwaitingServerFinal { .. }
        | ProtoState::ConnectingScramAwaitingAuthOk(_)
        | ProtoState::ConnectingPostAuthAwaitingKey(_)
        | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            other
        }
        other @ (ProtoState::PingAwaitingRfq(_)
        | ProtoState::SimpleQueryAwaitingFirstResponse(_)
        | ProtoState::SimpleQueryStreamingRows { .. }
        | ProtoState::SimpleQueryAwaitingRfq { .. }
        | ProtoState::DrainRfqAfterError
        | ProtoState::ParseAwaitingParseComplete(_)
        | ProtoState::ParseAwaitingRfq(_)
        | ProtoState::BindExecuteAwaitingBindCompleteDml(_)
        | ProtoState::BindExecuteAwaitingCommandCompleteDml(_)
        | ProtoState::BindExecuteAwaitingRfqDml { .. }
        | ProtoState::BindExecuteAwaitingBindCompleteSelect { .. }
        | ProtoState::BindExecuteAwaitingDataOrCompleteSelect { .. }
        | ProtoState::BindExecuteStreamingRows { .. }
        | ProtoState::BindExecuteAwaitingRfqSelect { .. }) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
            other
        }
    }
}

/// Build a PostgreSQL `Bind` (`'B'`) frame into `write_buf`.
///
/// Wire layout per PG §55.2.2:
///
/// ```text
/// 'B' | len_i32 | portal_name NUL | stmt_name NUL |
///   n_param_formats_i16 | [format_code_i16; N] |
///   n_params_i16 | [len_i32 + bytes; N] |
///   n_result_formats_i16
/// ```
///
/// The length prefix uses PG's "includes itself" convention via
/// [`WriteBuf::with_length_prefix`]. Per-parameter length prefixes
/// use the "excludes self" convention via
/// [`WriteBuf::with_i32_length_prefixed_body`] — see that helper's
/// docs for the rationale on the two different length semantics
/// in the PG wire format.
///
/// Zero-alloc: params are streamed directly from the tuple into
/// `write_buf` via [`crate::params::ParamsWriter::write_params`];
/// no intermediate buffer.
fn build_bind_message<P: crate::params::ParamsWriter>(
    portal_name: &crate::ident::PortalName,
    stmt_name: &crate::ident::StmtName,
    params: &P,
    write_buf: &mut WriteBuf,
) -> Result<crate::action::NonEmptyRange, crate::write_buf::WriteBufFull> {
    use crate::write_buf::WriteBufFull;

    let start = write_buf.len();
    write_buf.push_u8(crate::wire::TAG_BIND.byte())?;
    write_buf.with_length_prefix(|w| {
        w.push_nul_terminated(portal_name.as_bytes())?;
        w.push_nul_terminated(stmt_name.as_bytes())?;
        // n_param_formats = COUNT, followed by COUNT × Binary (wire
        // value 1). The `ParamEncoder` seal guarantees every param
        // ships in binary format — no Text path to dispatch, just
        // write the wire value `1` directly COUNT times. Eliminates
        // the per-element `match format { ... }` of the initial
        // 1c-3b draft — fewer branches in the hot bind path.
        w.push_u16_be(P::COUNT)?;
        for _ in 0..P::COUNT {
            w.push_u16_be(1)?;
        }
        w.push_u16_be(P::COUNT)?;
        params.write_params(w)?;
        // n_result_formats = 0 → server default (all text). 1c-3b
        // does not negotiate per-column result formats; the user
        // dispatches between text and binary decoders via the
        // `ColumnDesc::format_code` in the provided row_desc.
        w.push_u16_be(0)?;
        Ok(())
    })?;
    crate::action::NonEmptyRange::from_write_span(start, write_buf).ok_or(WriteBufFull)
}

/// Build a PostgreSQL `Execute` (`'E'`) frame into `write_buf`.
///
/// Wire layout per PG §55.2.2:
///
/// ```text
/// 'E' | len_i32 | portal_name NUL | max_rows_i32
/// ```
///
/// `max_rows` is derived from the caller's [`crate::FetchRows`] —
/// 1c-3b scope produces `0` (fetch all). F83: the enum narrows the
/// API to only variants the sub-phase supports, turning tier-3 docs
/// into tier-1 compile.
fn build_execute_message(
    portal_name: &crate::ident::PortalName,
    fetch: crate::command::FetchRows,
    write_buf: &mut WriteBuf,
) -> Result<crate::action::NonEmptyRange, crate::write_buf::WriteBufFull> {
    use crate::write_buf::WriteBufFull;

    let start = write_buf.len();
    write_buf.push_u8(crate::wire::TAG_EXECUTE.byte())?;
    write_buf.with_length_prefix(|w| {
        w.push_nul_terminated(portal_name.as_bytes())?;
        w.push_i32_be(fetch.as_wire_i32())
    })?;
    crate::action::NonEmptyRange::from_write_span(start, write_buf).ok_or(WriteBufFull)
}

/// Compute the transition for `push_bind_execute` against the
/// current [`ProtoState`]. Pure helper mirroring the compute_push_*
/// family — wrapped by the `push_bind_execute` method below.
///
/// Happy path emits THREE actions: `SendBytes(Bind frame)` +
/// `SendBytes(Execute frame)` + `SendBytes(SYNC)`. Bind and
/// Execute are written into `write_buf`; Sync is the static const.
///
/// Decision table:
///
/// | current state             | action                                   | new state                                   |
/// |---------------------------|------------------------------------------|---------------------------------------------|
/// | `Idle` (build OK)         | 3× `SendBytes(Bind, Execute, SYNC)`      | `BindExecuteAwaitingBindComplete{Dml,Select}` (depending on row_desc) |
/// | `Idle` (build Err)        | `FailReply(OutboundFrameBuildUnreachable)` | `Idle` (unchanged)                        |
/// | `Errored(kind)`           | `FailReply(ConnectionAlreadyClosed)`     | `Errored(kind)` preserved                   |
/// | any `Connecting*`         | `FailReply(StartupAlreadyInProgress)`    | same state preserved                        |
/// | any in-flight             | `FailReply(CommandInProgress)`           | same state preserved                        |
#[expect(clippy::too_many_arguments, reason = "compute_push_bind_execute is an internal helper; its arg count matches `push_bind_execute`'s parameter surface + the accumulator + write_buf. Splitting into a struct-arg would obscure the pure-compute framing.")]
fn compute_push_bind_execute<P: crate::params::ParamsWriter>(
    state: ProtoState,
    portal_name: &crate::ident::PortalName,
    stmt_name: &crate::ident::StmtName,
    params: &P,
    row_desc: Option<crate::decode::RowDesc>,
    fetch: crate::command::FetchRows,
    reply: ReplyId<crate::reply_id::QueryKind>,
    staged: &mut StagedActions,
    write_buf: &mut WriteBuf,
) -> ProtoState {
    match state {
        ProtoState::Idle => {
            // Build both outbound frames into the same WriteBuf. If
            // Bind succeeds but Execute fails, that's still a
            // build-unreachable path (const-asserted fit) — we
            // surface it the same way as other stages.
            match build_bind_message(portal_name, stmt_name, params, write_buf) {
                Ok(bind_range) => match build_execute_message(portal_name, fetch, write_buf) {
                    Ok(execute_range) => {
                        emit_actions!(staged, budget: 3, [
                            StagedAction::SendBytesRange(bind_range),
                            StagedAction::SendBytesRange(execute_range),
                            StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
                        ]);
                        // Tier-1 structural dispatch: decide the
                        // schema-lessDML vs schema-bearing SELECT
                        // path ONCE, here at push time. Downstream
                        // dispatch arms match on the specific
                        // variant — no runtime `match row_desc`
                        // at the 'D' arm.
                        match row_desc {
                            Some(desc) => {
                                ProtoState::BindExecuteAwaitingBindCompleteSelect {
                                    reply,
                                    row_desc: desc,
                                }
                            }
                            None => ProtoState::BindExecuteAwaitingBindCompleteDml(reply),
                        }
                    }
                    Err(_) => {
                        // TIER-1 COMPILE DEAD BRANCH — const assert
                        // in write_buf.rs proves the Bind+Execute+Sync
                        // bundle fits MAX_OWNED_SEND_LEN.
                        emit_actions!(staged, budget: 1, [
                            StagedAction::FailReply {
                                id: reply.consume(),
                                cause: ProtocolError::OutboundFrameBuildUnreachable {
                                    stage: crate::error::FrameBuildStage::Execute,
                                },
                            },
                        ]);
                        ProtoState::Idle
                    }
                },
                Err(_) => {
                    emit_actions!(staged, budget: 1, [
                        StagedAction::FailReply {
                            id: reply.consume(),
                            cause: ProtocolError::OutboundFrameBuildUnreachable {
                                stage: crate::error::FrameBuildStage::Bind,
                            },
                        },
                    ]);
                    ProtoState::Idle
                }
            }
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
        other @ (ProtoState::ConnectingStartupTrust { .. }
        | ProtoState::ConnectingStartupScram { .. }
        | ProtoState::ConnectingScramAwaitingServerFirst { .. }
        | ProtoState::ConnectingScramAwaitingServerFinal { .. }
        | ProtoState::ConnectingScramAwaitingAuthOk(_)
        | ProtoState::ConnectingPostAuthAwaitingKey(_)
        | ProtoState::ConnectingPostAuthHaveKey { .. }) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            other
        }
        other @ (ProtoState::PingAwaitingRfq(_)
        | ProtoState::SimpleQueryAwaitingFirstResponse(_)
        | ProtoState::SimpleQueryStreamingRows { .. }
        | ProtoState::SimpleQueryAwaitingRfq { .. }
        | ProtoState::DrainRfqAfterError
        | ProtoState::ParseAwaitingParseComplete(_)
        | ProtoState::ParseAwaitingRfq(_)
        | ProtoState::BindExecuteAwaitingBindCompleteDml(_)
        | ProtoState::BindExecuteAwaitingCommandCompleteDml(_)
        | ProtoState::BindExecuteAwaitingRfqDml { .. }
        | ProtoState::BindExecuteAwaitingBindCompleteSelect { .. }
        | ProtoState::BindExecuteAwaitingDataOrCompleteSelect { .. }
        | ProtoState::BindExecuteStreamingRows { .. }
        | ProtoState::BindExecuteAwaitingRfqSelect { .. }) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
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
const fn allows_unsolicited_param_status(state: &ProtoState) -> bool {
    match state {
        ProtoState::Idle
        | ProtoState::PingAwaitingRfq(_)
        | ProtoState::ConnectingPostAuthAwaitingKey(_)
        | ProtoState::ConnectingPostAuthHaveKey { .. }
        | ProtoState::SimpleQueryAwaitingFirstResponse(_)
        | ProtoState::SimpleQueryStreamingRows { .. }
        | ProtoState::SimpleQueryAwaitingRfq { .. }
        | ProtoState::DrainRfqAfterError
        | ProtoState::ParseAwaitingParseComplete(_)
        | ProtoState::ParseAwaitingRfq(_)
        | ProtoState::BindExecuteAwaitingBindCompleteDml(_)
        | ProtoState::BindExecuteAwaitingCommandCompleteDml(_)
        | ProtoState::BindExecuteAwaitingRfqDml { .. }
        | ProtoState::BindExecuteAwaitingBindCompleteSelect { .. }
        | ProtoState::BindExecuteAwaitingDataOrCompleteSelect { .. }
        | ProtoState::BindExecuteStreamingRows { .. }
        | ProtoState::BindExecuteAwaitingRfqSelect { .. } => true,
        ProtoState::ConnectingStartupTrust { .. }
        | ProtoState::ConnectingStartupScram { .. }
        | ProtoState::ConnectingScramAwaitingServerFirst { .. }
        | ProtoState::ConnectingScramAwaitingServerFinal { .. }
        | ProtoState::ConnectingScramAwaitingAuthOk(_)
        | ProtoState::Errored(_) => false,
    }
}

/// Classification of a `record_param_status` call's outcome.
///
/// F35 (2026-04-21): pre-F35 the function returned `()` and
/// silently dropped malformed payloads (missing NUL separator, etc.).
/// Now the outcome is typed so the caller has diagnostic info — and
/// a future Phase 1d wrapper-advisory channel (e.g.,
/// `Action::EmitPsAdvisory`) can forward `MalformedPayload` events
/// to the user for proxy-interference detection. Current caller
/// exhaustive-matches both variants, silently consuming for now,
/// but the compile surface is ready for the upgrade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParamStatusRecordOutcome {
    /// Payload was well-formed (key NUL + value NUL body); routing
    /// to `SessionParams::set` completed. Whether the specific key
    /// was recognised and its typed value stored is
    /// `session_params.set`'s internal concern — still always
    /// reports `Processed` here if the payload parsed.
    Processed,
    /// Payload violated PG's ParameterStatus wire format (missing
    /// NUL separator between key and value, or value region
    /// slice-bounds impossible). PG proper never sends such
    /// payloads; arrival implies a proxy / debugging tool injecting
    /// malformed PS, or a wire-corruption event.
    MalformedPayload,
}

/// Parse a ParameterStatus payload and record it in session_params.
///
/// Payload format: `key\0value\0`. Compressed with `let-else` to
/// five short lines (DEF-095). `[T]::split_once` with a predicate
/// is still unstable (#112811); the `iter().position` idiom is the
/// stable-library equivalent.
#[must_use]
fn record_param_status(
    params: &mut SessionParams,
    payload: &[u8],
) -> ParamStatusRecordOutcome {
    let Some(nul_pos) = payload.iter().position(|b| *b == 0) else {
        return ParamStatusRecordOutcome::MalformedPayload;
    };
    let Some(key) = payload.get(..nul_pos) else {
        return ParamStatusRecordOutcome::MalformedPayload;
    };
    let Some(value_start) = nul_pos.checked_add(1) else {
        return ParamStatusRecordOutcome::MalformedPayload;
    };
    let Some(value_region) = payload.get(value_start..) else {
        return ParamStatusRecordOutcome::MalformedPayload;
    };
    let value = value_region.strip_suffix(b"\0").unwrap_or(value_region);
    params.set(key, value);
    ParamStatusRecordOutcome::Processed
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
    read_buf_bytes: &'r [u8],
) -> OutActions<'w, 'r> {
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
            // 1c-1b: row_range is absolute coordinates into the
            // read buffer's populated region (`ReadBuf::populated`),
            // valid from emission through materialise because
            // the `'r` borrow on OutActions blocks all
            // `&mut self.read_buf` calls on PgProtocol until the
            // caller drops the returned actions.
            //
            // F19: `row_desc` is carried BY VALUE in the staged
            // action (copied from the `StreamingRows { row_desc }`
            // state variant at emission). No external `PgProtocol.row_desc`
            // slot to look up; no `unwrap_or(&EMPTY)` fallback.
            // Schema-state pairing is tier-2 structural — the staged
            // action can't exist without a schema (no constructor path
            // other than `stream_row_or_errored` which receives
            // `RowDesc` from the pattern-matched state).
            StagedAction::StreamRowRange { id, row_range, row_desc } => Action::StreamRow {
                id,
                row_bytes: row_range.apply(read_buf_bytes).unwrap_or(&[]),
                desc: row_desc,
            },
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
            ProtoState::Idle
            | ProtoState::Errored(_)
            | ProtoState::DrainRfqAfterError => {}
            ProtoState::PingAwaitingRfq(id) => {
                id.consume();
            }
            ProtoState::ConnectingScramAwaitingAuthOk(id)
            | ProtoState::ConnectingPostAuthAwaitingKey(id) => {
                id.consume();
            }
            ProtoState::ConnectingStartupTrust { reply }
            | ProtoState::ConnectingStartupScram { reply, .. }
            | ProtoState::ConnectingScramAwaitingServerFirst { reply, .. }
            | ProtoState::ConnectingScramAwaitingServerFinal { reply, .. }
            | ProtoState::ConnectingPostAuthHaveKey { reply, .. } => {
                reply.consume();
            }
            ProtoState::SimpleQueryAwaitingFirstResponse(id) => {
                id.consume();
            }
            ProtoState::BindExecuteAwaitingBindCompleteDml(id)
            | ProtoState::BindExecuteAwaitingCommandCompleteDml(id) => {
                id.consume();
            }
            ProtoState::SimpleQueryStreamingRows { reply, .. }
            | ProtoState::SimpleQueryAwaitingRfq { reply, .. }
            | ProtoState::BindExecuteAwaitingBindCompleteSelect { reply, .. }
            | ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, .. }
            | ProtoState::BindExecuteStreamingRows { reply, .. }
            | ProtoState::BindExecuteAwaitingRfqSelect { reply, .. }
            | ProtoState::BindExecuteAwaitingRfqDml { reply, .. } => {
                reply.consume();
            }
            ProtoState::ParseAwaitingParseComplete(reply)
            | ProtoState::ParseAwaitingRfq(reply) => {
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

        let awaiting_ping = ProtoState::PingAwaitingRfq(ReplyId::from_raw(nz(1)));
        assert!(allows_unsolicited_param_status(&awaiting_ping));
        consume_state(awaiting_ping);

        let awaiting_key = ProtoState::ConnectingPostAuthAwaitingKey(ReplyId::from_raw(nz(2)));
        assert!(allows_unsolicited_param_status(&awaiting_key));
        consume_state(awaiting_key);

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

        // ConnectingScramAwaitingServerFirst requires a Password *and* a
        // ScramSession (audit A2 typestate). The Password err branch
        // is architecturally unreachable for the fixture.
        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let scram = crate::scram::session::ScramSession::from_password(Sensitive::new(pw));
            let scram_first = ProtoState::ConnectingScramAwaitingServerFirst {
                reply: ReplyId::from_raw(nz(5)),
                scram,
                client_first_bare: crate::ident::PodBytes::new(),
                client_nonce_b64: crate::ident::PodBytes::new(),
            };
            assert!(!allows_unsolicited_param_status(&scram_first));
            consume_state(scram_first);
        }

        let scram_final = ProtoState::ConnectingScramAwaitingServerFinal {
            reply: ReplyId::from_raw(nz(6)),
            expected_server_sig: SecretDigest::new([0u8; 32]),
        };
        assert!(!allows_unsolicited_param_status(&scram_final));
        consume_state(scram_final);

        let scram_authok = ProtoState::ConnectingScramAwaitingAuthOk(ReplyId::from_raw(nz(7)));
        assert!(!allows_unsolicited_param_status(&scram_authok));
        consume_state(scram_authok);

        // Errored — rejecting (terminal; no traffic accepted).
        // DEF-061: Errored carries ErrorKind (1 byte), not the full
        // ProtocolError.
        let errored = ProtoState::Errored(crate::error::ErrorKind::Framing);
        assert!(!allows_unsolicited_param_status(&errored));
        consume_state(errored);

        // 1c-1b: simple-query states all accept unsolicited PS
        // (server may emit ParameterStatus mid-query if an
        // `ALTER SYSTEM` fires). Exhaustive enumeration pins the
        // policy row per-variant.
        let q_first = ProtoState::SimpleQueryAwaitingFirstResponse(ReplyId::from_raw(nz(8001)));
        assert!(allows_unsolicited_param_status(&q_first));
        consume_state(q_first);

        let q_rows = ProtoState::SimpleQueryStreamingRows {
            reply: ReplyId::from_raw(nz(8002)),
            row_desc: crate::decode::RowDesc::EMPTY,
        };
        assert!(allows_unsolicited_param_status(&q_rows));
        consume_state(q_rows);

        let q_rfq = ProtoState::SimpleQueryAwaitingRfq {
            reply: ReplyId::from_raw(nz(8003)),
            command_tag: crate::error::BoundedStr::default(),
            row_desc: None,
        };
        assert!(allows_unsolicited_param_status(&q_rfq));
        consume_state(q_rfq);

        let q_drain = ProtoState::DrainRfqAfterError;
        assert!(allows_unsolicited_param_status(&q_drain));
        consume_state(q_drain);
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
            ProtoState::Idle
            | ProtoState::Errored(_)
            | ProtoState::DrainRfqAfterError => {}
            ProtoState::PingAwaitingRfq(id) => {
                id.consume();
            }
            ProtoState::ConnectingScramAwaitingAuthOk(id)
            | ProtoState::ConnectingPostAuthAwaitingKey(id) => {
                id.consume();
            }
            ProtoState::ConnectingStartupTrust { reply }
            | ProtoState::ConnectingStartupScram { reply, .. }
            | ProtoState::ConnectingScramAwaitingServerFirst { reply, .. }
            | ProtoState::ConnectingScramAwaitingServerFinal { reply, .. }
            | ProtoState::ConnectingPostAuthHaveKey { reply, .. } => {
                reply.consume();
            }
            ProtoState::SimpleQueryAwaitingFirstResponse(id) => {
                id.consume();
            }
            ProtoState::BindExecuteAwaitingBindCompleteDml(id)
            | ProtoState::BindExecuteAwaitingCommandCompleteDml(id) => {
                id.consume();
            }
            ProtoState::SimpleQueryStreamingRows { reply, .. }
            | ProtoState::SimpleQueryAwaitingRfq { reply, .. }
            | ProtoState::BindExecuteAwaitingBindCompleteSelect { reply, .. }
            | ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, .. }
            | ProtoState::BindExecuteStreamingRows { reply, .. }
            | ProtoState::BindExecuteAwaitingRfqSelect { reply, .. }
            | ProtoState::BindExecuteAwaitingRfqDml { reply, .. } => {
                reply.consume();
            }
            ProtoState::ParseAwaitingParseComplete(reply)
            | ProtoState::ParseAwaitingRfq(reply) => {
                reply.consume();
            }
        }
    }

    /// If `new_state` is `PingAwaitingRfq`, consume the inner reply
    /// and return its raw value. Otherwise drain any carried reply
    /// and return `None`. Used to express the assertion "new state
    /// is PingAwaitingRfq(expected_raw)" as a single `assert_eq!`
    /// without the forbid-bundle incompatibility of `panic!` in an
    /// else branch.
    fn take_awaiting_ping_raw(new_state: ProtoState) -> Option<NonZeroU64> {
        match new_state {
            ProtoState::PingAwaitingRfq(r) => Some(r.consume()),
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

        // State: PingAwaitingRfq(raw_id).
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
        let prev_state = ProtoState::PingAwaitingRfq(ReplyId::from_raw(raw_prev));
        let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
        let (new_state, staged) = compute_staged(cmd, prev_state);

        // Action: FailReply(CommandInProgress) for the NEW reply.
        // (Previously `UnexpectedFrame { tag: b'P' }` — retyped to
        // `CommandInProgress` during the tier-1 uplift to `InboundTag`
        // on `UnexpectedFrame.tag`; the synthetic `b'P'` byte wasn't
        // a real inbound tag anyway.)
        assert_eq!(staged.len(), 1);
        assert!(
            matches!(
                staged.first(),
                Some(StagedAction::FailReply {
                    id,
                    cause: ProtocolError::CommandInProgress,
                }) if *id == raw_new
            ),
            "expected FailReply(CommandInProgress) for new reply",
        );

        // State: PingAwaitingRfq(raw_prev) — the original prev_reply
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

        // ConnectingScramAwaitingServerFirst — needs a Password and
        // ScramSession.
        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let scram = crate::scram::session::ScramSession::from_password(Sensitive::new(pw));
            let raw_prev = nz(203);
            let raw_new = nz(204);
            let prev = ProtoState::ConnectingScramAwaitingServerFirst {
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
                "ScramAwaitingServerFirst → expected FailReply(StartupAlreadyInProgress)",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingScramAwaitingServerFirst { .. }
            ));
            consume_state(new_state);
        }

        // ConnectingScramAwaitingServerFinal.
        {
            let raw_prev = nz(205);
            let raw_new = nz(206);
            let prev = ProtoState::ConnectingScramAwaitingServerFinal {
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
                "ScramAwaitingServerFinal → expected FailReply(StartupAlreadyInProgress)",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingScramAwaitingServerFinal { .. }
            ));
            consume_state(new_state);
        }

        // ConnectingScramAwaitingAuthOk.
        {
            let raw_prev = nz(207);
            let raw_new = nz(208);
            let prev = ProtoState::ConnectingScramAwaitingAuthOk(ReplyId::from_raw(raw_prev));
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
                "ScramAwaitingAuthOk → expected FailReply(StartupAlreadyInProgress)",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingScramAwaitingAuthOk(_)
            ));
            consume_state(new_state);
        }

        // ConnectingPostAuthAwaitingKey.
        {
            let raw_prev = nz(209);
            let raw_new = nz(210);
            let prev = ProtoState::ConnectingPostAuthAwaitingKey(ReplyId::from_raw(raw_prev));
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
                "PostAuthAwaitingKey → expected FailReply(StartupAlreadyInProgress)",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingPostAuthAwaitingKey(_)
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

        // PingAwaitingRfq.
        if let Some(user) = mk_user() {
            let raw_prev = nz(401);
            let raw_new = nz(402);
            let prev = ProtoState::PingAwaitingRfq(ReplyId::from_raw(raw_prev));
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
                "PingAwaitingRfq → expected StartupAlreadyInProgress",
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

        // ConnectingScramAwaitingServerFirst. Construction requires
        // Password + ScramSession (audit A2 typestate).
        if let (Some(user), Ok(pw)) = (mk_user(), Password::try_from_bytes(b"pw")) {
            let scram = crate::scram::session::ScramSession::from_password(Sensitive::new(pw));
            let raw_prev = nz(405);
            let raw_new = nz(406);
            let prev = ProtoState::ConnectingScramAwaitingServerFirst {
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
                "ScramAwaitingServerFirst → expected StartupAlreadyInProgress",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingScramAwaitingServerFirst { .. }
            ));
            consume_state(new_state);
        }

        // ConnectingScramAwaitingServerFinal.
        if let Some(user) = mk_user() {
            let raw_prev = nz(407);
            let raw_new = nz(408);
            let prev = ProtoState::ConnectingScramAwaitingServerFinal {
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
                "ScramAwaitingServerFinal → expected StartupAlreadyInProgress",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingScramAwaitingServerFinal { .. }
            ));
            consume_state(new_state);
        }

        // ConnectingScramAwaitingAuthOk.
        if let Some(user) = mk_user() {
            let raw_prev = nz(409);
            let raw_new = nz(410);
            let prev = ProtoState::ConnectingScramAwaitingAuthOk(ReplyId::from_raw(raw_prev));
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
                "ScramAwaitingAuthOk → expected StartupAlreadyInProgress",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingScramAwaitingAuthOk(_)
            ));
            consume_state(new_state);
        }

        // ConnectingPostAuthAwaitingKey.
        if let Some(user) = mk_user() {
            let raw_prev = nz(411);
            let raw_new = nz(412);
            let prev = ProtoState::ConnectingPostAuthAwaitingKey(ReplyId::from_raw(raw_prev));
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
                "PostAuthAwaitingKey → expected StartupAlreadyInProgress",
            );
            assert!(matches!(
                &new_state,
                ProtoState::ConnectingPostAuthAwaitingKey(_)
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
