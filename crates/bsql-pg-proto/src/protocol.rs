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
///
/// 1c-5 blocker (audit2 A028): pipelining changes the topology — a
/// single feed_bytes iteration might resolve multiple concurrent
/// inflight replies (e.g., DataRow for query A + CommandComplete
/// for query B). Worst case becomes ≥3; WORST_CASE_PER_DISPATCH and
/// MAX_ACTIONS_PER_CALL both revisit at 1c-5 implementation time
/// per H021 witness-guard session.
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
    /// DEF-119 schema arena — externalised `RowDesc` storage.
    ///
    /// Two-slot slab (see [`crate::schema_arena`] module docs).
    /// State variants / staged actions / staged reply payloads
    /// carry 1-byte `SchemaRef` handles; public `Reply<'r>` and
    /// `Action::StreamRow::desc` resolve refs through the arena at
    /// materialise time.
    ///
    /// Cost: ~528 B on `PgProtocol`, paid once per connection.
    /// Benefit: state drops from ~1224 B → ~300 B;
    /// `Action::StreamRow` drops from ~280 B → ~32 B;
    /// per-row DataRow emission saves ~260 B (hot path on SELECT).
    schema_arena: crate::schema_arena::SchemaSlab,
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
            schema_arena: crate::schema_arena::SchemaSlab::new(),
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
    pub fn push_command<'w, 's>(
        &'s mut self,
        cmd: PgCommand,
        write_buf: &'w mut WriteBuf,
    ) -> OutActions<'w, 's> {
        // 1c-1a: push never produces `StreamRow` (rows arrive via
        // server responses, handled in `feed_bytes`). The `'r`
        // lifetime parameter on `OutActions<'w, 'r>` is phantom on
        // this path — unifying it to `'static` gives the caller
        // freedom over what they pair the result with later.
        write_buf.clear();

        // DEF-172: centralised entry-point arena reclamation.
        self.clear_arena_if_idle_or_errored();
        let prev = core::mem::take(&mut self.state);
        let (new_state, staged) = compute_push(cmd, prev, write_buf);
        self.state = new_state;
        // DEF-154 (C): materialise sees a narrowed reader witness,
        // not the full `&SchemaSlab`. Arena access here is get-only.
        materialise(staged, write_buf.as_bytes(), &[], self.schema_arena.as_reader())
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
    pub fn push_bind_execute<'w, 's, P: crate::params::ParamsWriter>(
        &'s mut self,
        portal_name: &crate::ident::PortalName,
        stmt_name: &crate::ident::StmtName,
        params: &P,
        row_desc: Option<crate::decode::RowDesc>,
        fetch: crate::command::FetchRows,
        reply: ReplyId<crate::reply_id::QueryKind>,
        write_buf: &'w mut WriteBuf,
    ) -> OutActions<'w, 's> {
        write_buf.clear();
        // DEF-172: centralised entry-point arena reclamation.
        self.clear_arena_if_idle_or_errored();
        // DEF-119: if the user supplied a row_desc, allocate it into
        // the arena NOW and thread the resulting SchemaRef into the
        // state machine. The owned RowDesc goes into the arena slab;
        // state + actions carry 1-byte handles. `None` means DML
        // path (no schema) → no alloc.
        let schema_ref = match row_desc {
            Some(desc) => self.schema_arena.alloc(desc),
            None => None,
        };
        let mut staged = StagedActions::new();
        let prev = core::mem::take(&mut self.state);
        let new_state = compute_push_bind_execute(
            prev,
            portal_name,
            stmt_name,
            params,
            schema_ref,
            fetch,
            reply,
            &mut staged,
            write_buf,
        );
        self.state = new_state;
        // DEF-154 (C): reader witness — see push_command for rationale.
        materialise(staged, write_buf.as_bytes(), &[], self.schema_arena.as_reader())
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

        // DEF-119 arena cleanup — any prior OutActions<'_, 'r_prev>
        // has drained (borrow checker enforces it before this
        // `&'r mut self` call). DEF-172: centralised via helper.
        self.clear_arena_if_idle_or_errored();

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
            // DEF-154 (C): reader witness, get-only access.
            return materialise(
                staged,
                write_buf.as_bytes(),
                self.read_buf.populated(),
                self.schema_arena.as_reader(),
            );
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
                self.schema_arena.as_reader(),
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
                HeaderParse::Ok { tag, total_len } => {
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
                            self.fail_read_cursor_advance(&mut staged);
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
                            self.fail_read_cursor_advance(&mut staged);
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
                    // F-018 (pass-#8): private fields, explicit
                    // `::new` constructor at each site. Swap
                    // protection is now structural: a caller who
                    // swapped `AbsFrameStart::new(total_len)` and
                    // `FrameTotalLen::new(cursor_position())` would
                    // compile but the semantic is wrong — the
                    // typed FrameCoords::new argument order is the
                    // only remaining shield (and is tier-1 compile
                    // via distinct types).
                    let frame_start = AbsFrameStart::new(self.read_buf.cursor_position());
                    let frame_len = FrameTotalLen::new(total_len);
                    let populated = PopulatedLen::new(self.read_buf.populated().len());

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
                    // DEF-154 (C): dispatch receives a writer witness,
                    // not the full `&mut SchemaSlab`. Dispatch can
                    // only call `alloc` — `get` / `clear` / `free`
                    // are structurally out of reach. The writer's
                    // mutable borrow of `self.schema_arena` ends at
                    // the last use (the `dispatch` call) under NLL,
                    // so subsequent `self.read_buf.advance` and
                    // `self.replace_state_errored_and_drain` calls
                    // in this iteration see an un-borrowed self.
                    let mut arena_writer = self.schema_arena.as_writer();
                    let outcome = dispatch(
                        prev,
                        tag,
                        payload,
                        write_buf,
                        &mut arena_writer,
                        FrameCoords::new(frame_start, frame_len, populated),
                    );
                    match outcome {
                        DispatchOutcome::AdvancedSilent { new_state } => {
                            self.state = new_state;
                            let Ok(()) = self.read_buf.advance(total_len) else {
                                self.fail_read_cursor_advance(&mut staged);
                                break;
                            };
                        }
                        DispatchOutcome::AdvancedWithAction { new_state, action } => {
                            self.state = new_state;
                            let Ok(()) = self.read_buf.advance(total_len) else {
                                self.fail_read_cursor_advance(&mut staged);
                                break;
                            };
                            emit_actions!(&mut staged, budget: 1, on_overflow: break, [
                                action,
                            ]);
                        }
                        DispatchOutcome::Errored { reply_id, cause } => {
                            // DEF-061 + DEF-142: store the compact
                            // 1-byte `StateErrorKind` in state; the
                            // full cause goes out in the `FailReply`
                            // below, exactly once.
                            //
                            // `try_from_kind` returns None ONLY for
                            // `ErrorKind::AlreadyClosed`, which is
                            // emitted only by
                            // `ProtocolError::ConnectionAlreadyClosed`,
                            // which is only constructed by the
                            // already-Errored push paths — and those
                            // paths never reach `dispatch` (they
                            // short-circuit in `push_command`). The
                            // debug_assert pins this architectural
                            // invariant loudly in tests; release keeps
                            // the INTERNAL_FALLBACK shield per DEF-175.
                            // DEF-175 + DEF-176: `state_kind()` composes
                            // `kind() + try_from_kind`; returns None
                            // only for AlreadyClosed (DEF-142-sealed out
                            // of dispatch paths). debug_assert pins
                            // the dead None branch.
                            let state_kind = cause.state_kind().unwrap_or_else(|| {
                                debug_assert!(
                                    false,
                                    "DEF-175: AlreadyClosed reached dispatch-Errored — \
                                     impossible per DEF-142 seal (ConnectionAlreadyClosed is \
                                     only emitted from push-path on already-Errored state, \
                                     which short-circuits before dispatch).",
                                );
                                crate::error::StateErrorKind::INTERNAL_FALLBACK
                            });
                            // DEF-168 (A001): route through the DEF-149
                            // atomic-terminus helper. Closes the
                            // "state-replace + read_buf.clear" pairing
                            // that the pre-DEF-168 inline assignment
                            // half-applied. Discards the helper's
                            // Option<NonZeroU64> return — reply_id is
                            // already in hand from DispatchOutcome
                            // (dispatcher pre-consumed it).
                            match self.replace_state_errored_and_drain(state_kind) {
                                // Architecturally None here: self.state
                                // was Idle post-dispatcher-mem::take, so
                                // no inflight reply lives in it.
                                Some(_) | None => {}
                            }
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
        // DEF-119: `StreamRow` actions carry a `SchemaRef` handle;
        // materialise resolves to `&'r RowDesc` via `self.schema_arena`.
        // The `'r` lifetime on OutActions ties the arena borrow to the
        // `&'r mut self` parameter — the next `&mut self` call can only
        // run after the caller drops OutActions, at which point
        // feed_bytes entry-cleanup will reclaim the arena slot.
        // DEF-154 (C): reader witness. After the dispatch loop, any
        // ArenaWriter created per-iteration has been dropped, so
        // `self.schema_arena` is freely borrowable for the reader.
        materialise(
            staged,
            write_buf.as_bytes(),
            self.read_buf.populated(),
            self.schema_arena.as_reader(),
        )
    }

    /// DEF-177 — cold helper for the 4 ReadCursorAdvance failure
    /// sites in `feed_bytes`.
    ///
    /// `ReadBuf::advance` returning Err after `parse_header` succeeded
    /// is architecturally dead — the two checks run in the same
    /// iteration with no interleaving mutation. The failure path
    /// classifies as `CrateBugLocus::ReadCursorAdvance`.
    ///
    /// Pre-DEF-177, the 4 sites (around the NoticeResponse /
    /// ParameterStatus / post-dispatch advance calls) open-coded:
    ///     self.fail_inflight_and_close(
    ///         ProtocolError::InternalCrateBug {
    ///             locus: CrateBugLocus::ReadCursorAdvance,
    ///         },
    ///         &mut staged,
    ///     );
    /// ~6 LoC per site + inline IR cost on a hot dispatch loop.
    /// Post-DEF-177 each site is a one-liner cold call. Audit2 A014.
    #[cold]
    #[inline]
    fn fail_read_cursor_advance(&mut self, staged: &mut StagedActions) {
        self.fail_inflight_and_close(
            ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::ReadCursorAdvance,
            },
            staged,
        );
    }

    /// DEF-172 — entry-point arena reclamation.
    ///
    /// If the connection state is `Idle` or `Errored`, clear the
    /// schema arena (reclaims any schemas carried over from the
    /// previous query cycle) and pin the post-clear invariant
    /// (`occupied_count() == 0`) in debug builds.
    ///
    /// Called from all three user-facing entry points: `push_command`,
    /// `push_bind_execute`, `feed_bytes`. Pre-DEF-172, each of the
    /// three sites open-coded the 8-line match+clear+debug_assert
    /// pattern — drift surface (audit2 A007) since adding a fourth
    /// entry point (e.g., a future `push_close_statement` in 1c-3d)
    /// would need to remember the ritual. Post-DEF-172 the discipline
    /// lives in one place.
    ///
    /// DEF-148 / DEF-171: clear() is cheap on the Ping-loop hot case
    /// (2-slot is_some walk, no stores).
    ///
    /// 1c-5 blocker (audit2 A027): pipelining breaks the blanket-
    /// clear model — concurrent inflight queries each hold a live
    /// SchemaRef; clear()ing all slots at an entry point would
    /// invalidate refs that are still legitimately in-flight. At
    /// 1c-5 time this helper becomes per-ref `free()` calls keyed
    /// on which query's borrow is ending. Revisit per H021
    /// witness-guard session.
    #[inline]
    fn clear_arena_if_idle_or_errored(&mut self) {
        if matches!(self.state, ProtoState::Idle | ProtoState::Errored(_)) {
            self.schema_arena.clear();
            debug_assert_eq!(
                self.schema_arena.occupied_count(),
                0,
                "DEF-152: clear() must leave arena empty",
            );
        }
    }

    /// DEF-149 — atomic state-terminus transition.
    ///
    /// Install `ProtoState::Errored(kind)`, extract any in-flight
    /// reply's raw correlator (via the authoritative
    /// [`ProtoState::take_inflight_reply_raw_id`] matcher), and clear
    /// `read_buf`. Returns the extracted raw correlator so the caller
    /// can emit `FailReply { id, cause }`.
    ///
    /// # Why one helper, one caller
    ///
    /// The three operations form ONE conceptual unit at the
    /// state-machine-terminus boundary. A future refactor that
    /// reorders or splits them could produce observable state where:
    /// - state is Errored but `read_buf` still holds post-fatal bytes
    ///   (stale parse-ahead attempts leak CPU), OR
    /// - state is intermediate while `read_buf` is already cleared
    ///   (a concurrent `&self` reader — if one ever lands — sees an
    ///   inconsistent snapshot).
    ///
    /// Naming this triple as a single method makes the atomicity
    /// intent visible to IDE navigation and makes drift mechanically
    /// harder. Currently one caller
    /// ([`Self::fail_inflight_and_close`]); the helper shape is
    /// durable if a new fatal path is added later.
    ///
    /// # Tier
    ///
    /// Tier-3 audit ordering pairing → tier-2 structural (one body,
    /// one ordering, one contract).
    ///
    /// # DEF-117 preservation
    ///
    /// Uses [`core::mem::replace`] (not `take` + assign) to install
    /// the terminal state directly; there is no transient `Idle`
    /// window a concurrent `&self` read could misread as "healthy".
    #[inline]
    fn replace_state_errored_and_drain(
        &mut self,
        kind: crate::error::StateErrorKind,
    ) -> Option<core::num::NonZeroU64> {
        let prev = core::mem::replace(&mut self.state, ProtoState::Errored(kind));
        self.read_buf.clear();
        prev.take_inflight_reply_raw_id()
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
    ///
    /// # 1c-5 blocker (audit2 A030)
    ///
    /// The budget `2` (FailReply + CloseSocket) assumes at most one
    /// inflight reply. Pipelining: a fatal error must fail ALL N
    /// concurrent replies — budget becomes `1 + N`. Helper widens
    /// alongside `take_inflight_reply_raw_id` (see A029 marker in
    /// state.rs). Revisit per H021 witness-guard session.
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
        // DEF-061 + DEF-094 + DEF-142: compute kind before `cause`
        // is moved into the FailReply StagedAction. Stored in state
        // as 1-byte `StateErrorKind`; full cause goes out in the one
        // FailReply emitted below.
        //
        // `StateErrorKind::try_from_kind` returns `None` ONLY for
        // `ErrorKind::AlreadyClosed`. That kind is only produced by
        // `ProtocolError::ConnectionAlreadyClosed`, which in turn
        // is only emitted for already-Errored connections — and the
        // `if matches!(self.state, Errored(_))` early-return above
        // short-circuits BEFORE we reach this point for such cases.
        // So `try_from_kind` is architecturally guaranteed to return
        // `Some` here; the `unwrap_or_else` fallback is dead-safety
        // (falls back to `Internal` as the honest "crate-bug" kind
        // rather than panic — forbid-bundle bans panic).
        // DEF-175 (A012) + DEF-176 (A016): `state_kind()` composes
        // `kind() + try_from_kind`; returns None only for
        // AlreadyClosed (DEF-142-sealed out of this path via the
        // `if matches!(Errored)` early-return above). debug_assert
        // pins the dead None branch loudly in tests.
        let state_kind = cause.state_kind().unwrap_or_else(|| {
            debug_assert!(
                false,
                "DEF-175: AlreadyClosed reached fail_inflight_and_close — \
                 impossible per DEF-142 seal (ConnectionAlreadyClosed is \
                 only emitted from push-path on already-Errored state, \
                 which short-circuits via the `if matches!(Errored)` \
                 early-return above).",
            );
            crate::error::StateErrorKind::INTERNAL_FALLBACK
        });
        // DEF-149: `replace_state_errored_and_drain` centralises the
        // atomic "install Errored(kind) + drain inflight raw_id +
        // clear read_buf" triple. The three operations form one
        // conceptual unit at the state-machine-terminus boundary;
        // a future refactor that reorders them could produce
        // observable state where Errored is set but read_buf still
        // holds post-fatal bytes. The helper pins the ordering.
        let raw_id = self.replace_state_errored_and_drain(state_kind);
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
        PgCommand::DescribeStatement { stmt_name, reply } => {
            compute_push_describe_statement(state, &stmt_name, reply, &mut staged, write_buf)
        }
        PgCommand::DescribePortal { portal_name, reply } => {
            compute_push_describe_portal(state, &portal_name, reply, &mut staged, write_buf)
        }
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
    // DEF-146: classifier dispatch. Pre-DEF-146 this function had 5
    // arms over explicit state variants (with 18-way or-patterns for
    // the tail catch-alls). Post-DEF-146, the enumeration lives once
    // in `ProtoState::push_class`; this match is 5 arms over the
    // classifier's 5 variants — exhaustive, no `_` fallback, tier-1
    // preserved.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-094: Sync is a compile-time const (5 bytes). Emit
            // `StagedAction::SendBytesStatic(&SYNC_WIRE_BYTES)` so the
            // materialiser passes the static reference through
            // directly — zero write to write_buf, zero copy.
            emit_actions!(staged, budget: 1, [
                StagedAction::SendBytesStatic(&SYNC_WIRE_BYTES),
            ]);
            ProtoState::PingAwaitingRfq(reply)
        }
        crate::state::StatePushClass::Errored(prior_kind) => {
            // Preserve the stored cause; emit ConnectionAlreadyClosed
            // so the wrapper sees "connection terminal" rather than
            // a generic in-flight error.
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
            ProtoState::Errored(prior_kind)
        }
        // Ping-specific: a pending Ping ("a command is in flight")
        // classifies as CommandInProgress (not StartupAlreadyInProgress)
        // because this is a push-path error, not a startup-sequence
        // error.
        crate::state::StatePushClass::PingAwaiting
        | crate::state::StatePushClass::BusyQuery => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
            state
        }
        // Any Connecting* variant — startup handshake in progress.
        crate::state::StatePushClass::Connecting => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            state
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
    // DEF-146: single-level classifier dispatch. Startup is the one
    // helper where PingAwaiting groups with Connecting →
    // StartupAlreadyInProgress (not CommandInProgress as in the
    // other 6 helpers). Test:
    // `startup_from_non_idle_non_errored_fails_with_startup_in_progress`
    // (PingAwaitingRfq sub-case) pins this.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-154 (A): infallible builder via WriteReserved.
            let mut reserved = write_buf.reserve();
            let range = build_startup_message(
                &user,
                database.as_ref(),
                app_name.as_ref(),
                &mut reserved,
            );
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
        },
        crate::state::StatePushClass::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
            ProtoState::Errored(prior_kind)
        }
        // Startup-specific: PingAwaiting groups with Connecting
        // (both imply "startup sequence cannot be re-initiated").
        crate::state::StatePushClass::PingAwaiting
        | crate::state::StatePushClass::Connecting => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            state
        }
        crate::state::StatePushClass::BusyQuery => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
            state
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
/// | `Idle` (build OK infallibly) | `SendBytes` + state transition                  | new post-push state            |
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
    // DEF-146: single-level classifier dispatch (standard pattern —
    // Ping + BusyQuery → CommandInProgress, Connecting →
    // StartupAlreadyInProgress).
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-154 (A): capacity witness. Builder is infallible —
            // the pre-DEF-154 `Err(WriteBufFull)` arm is eliminated
            // at the type level.
            let mut reserved = write_buf.reserve();
            let range = build_query_message(sql, &mut reserved);
            emit_actions!(staged, budget: 1, [
                StagedAction::SendBytesRange(range),
            ]);
            ProtoState::SimpleQueryAwaitingFirstResponse(reply)
        },
        crate::state::StatePushClass::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
            ProtoState::Errored(prior_kind)
        }
        crate::state::StatePushClass::Connecting => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            state
        }
        crate::state::StatePushClass::PingAwaiting
        | crate::state::StatePushClass::BusyQuery => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
            state
        }
    }
}

/// DEF-154 (A) — infallible counterpart of
/// [`crate::action::NonEmptyRange::from_write_span`].
///
/// Every PG wire builder in this crate (`build_{query,parse,bind,
/// execute,describe,startup}_message`) emits ≥ 5 bytes (tag +
/// length prefix + body). `from_write_span` therefore always returns
/// `Some` — `debug_assert!` shields the architecturally-dead `None`
/// branch. Single-level fallback to
/// [`crate::action::NonEmptyRange::DEAD_FALLBACK`] keeps the
/// forbid-bundle happy without nested `unwrap_or_else` sprawl.
#[inline]
fn from_write_span_infallible(
    start: usize,
    reserved: &crate::write_buf::WriteReserved<'_>,
) -> crate::action::NonEmptyRange {
    match crate::action::NonEmptyRange::from_write_span(start, reserved.as_write_buf()) {
        Some(r) => r,
        None => {
            debug_assert!(
                false,
                "DEF-154: build_*_message produced zero-length span — every \
                 PG wire frame is ≥ 5 bytes; const-assert on \
                 MAX_OWNED_SEND_LEN or a missing push_*() would cause this.",
            );
            crate::action::NonEmptyRange::DEAD_FALLBACK
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
    reserved: &mut crate::write_buf::WriteReserved<'_>,
) -> crate::action::NonEmptyRange {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_QUERY.byte());
    reserved.with_length_prefix(|w| {
        w.push_nul_terminated(sql.as_bytes());
    });
    // DEF-154 (A): 'Q' frame is ≥ 6 bytes (tag + length + NUL), so
    // `from_write_span` always returns Some. `DEAD_FALLBACK` shields
    // the architecturally-dead zero-span branch.
    from_write_span_infallible(start, reserved)
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
    reserved: &mut crate::write_buf::WriteReserved<'_>,
) -> crate::action::NonEmptyRange {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_PARSE.byte());
    reserved.with_length_prefix(|w| {
        w.push_nul_terminated(stmt_name.as_bytes());
        w.push_nul_terminated(sql.as_bytes());
        // n_param_types = 0; 1c-3b will widen to push actual OIDs here.
        w.push_i16_be(0);
    });
    from_write_span_infallible(start, reserved)
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
/// | `Idle` (build OK infallibly) | `SendBytes` + state transition                  | new post-push state            |
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
    // DEF-146: classifier dispatch. DEF-154 (A): infallible builder.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            let mut reserved = write_buf.reserve();
            let range = build_parse_message(stmt_name, sql, &mut reserved);
            // Emit Parse frame (range into write_buf) + bundled Sync
            // (static const). Both needed for PG to flush ParseComplete
            // (without Sync the server buffers forever).
            emit_actions!(staged, budget: 2, [
                StagedAction::SendBytesRange(range),
                StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
            ]);
            ProtoState::ParseAwaitingParseComplete(reply)
        },
        crate::state::StatePushClass::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
            ProtoState::Errored(prior_kind)
        }
        crate::state::StatePushClass::Connecting => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            state
        }
        crate::state::StatePushClass::PingAwaiting
        | crate::state::StatePushClass::BusyQuery => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
            state
        }
    }
}

// DEF-154 (A) closed pass-#7 F16: `frame_build_unreachable` helper
// + `CrateBugLocus::OutboundFrameBuild { stage }` variant +
// `FrameBuildStage` enum all DELETED. The `build_*_message`
// builders are now infallible via the `WriteReserved` capacity
// witness in `write_buf.rs`; no Err branch exists at call sites,
// no cold helper needed, no locus variant to classify a null case.

/// Build a PostgreSQL Extended Query `Describe` (`'D'`) frame
/// (PG §55.2.2).
///
/// Wire layout: tag `'D'`, 4-byte BE length (self-inclusive),
/// single target byte (`'S'` statement or `'P'` portal via
/// [`crate::wire::DescribeTargetByte`]), NUL-terminated name.
///
/// # Tier-1 target-byte pairing (F12, pass-#7)
///
/// `target` is a typed enum; the wire byte it encodes is pinned
/// by const-asserts in `wire.rs`. The `name: &impl DescribeName`
/// constraint (sealed trait in `ident.rs`) restricts callers to
/// `StmtName` or `PortalName` — passing a raw `&[u8]` is a type
/// error, closing the tier-3 "caller always passes the right
/// typed name" audit seam.
///
/// `#[inline]` because the function is zero-generic monomorphic
/// over `N: DescribeName`, the body is ~10 lines of direct buffer
/// writes, and two call sites (`compute_push_describe_statement` /
/// `..._portal`) invoke it per push — small enough to fold in.
#[inline]
fn build_describe_message<N: crate::ident::DescribeName>(
    target: crate::wire::DescribeTargetByte,
    name: &N,
    reserved: &mut crate::write_buf::WriteReserved<'_>,
) -> crate::action::NonEmptyRange {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_DESCRIBE.byte());
    reserved.with_length_prefix(|w| {
        w.push_u8(target.byte());
        w.push_nul_terminated(name.as_describe_name_bytes());
    });
    from_write_span_infallible(start, reserved)
}

/// Compute the transition for [`PgCommand::DescribeStatement`]
/// against the current [`ProtoState`]. Pure; see [`compute_push`]
/// for framing. 1c-3c.
///
/// Happy path emits TWO actions: `SendBytes(Describe frame)` and
/// `SendBytes(SYNC)`. The Sync is a static const
/// (`SYNC_WIRE_BYTES`) emitted via `StagedAction::SendBytesStatic`
/// for zero-copy; the Describe frame is written into the caller's
/// `WriteBuf`.
///
/// Decision table:
///
/// | current state                | action                              | new state                                   |
/// |------------------------------|-------------------------------------|---------------------------------------------|
/// | `Idle` (build OK)            | 2× `SendBytes(Describe, SYNC)`      | `DescribeStatementAwaitingParamDesc(reply)` |
/// | (build infallible post-DEF-154) | —                                           | —                                    |
/// | `Errored(kind)`              | `FailReply(ConnectionAlreadyClosed)`| `Errored(kind)` preserved                   |
/// | `Connecting*`                | `FailReply(StartupAlreadyInProgress)`| same                                       |
/// | any other in-flight          | `FailReply(CommandInProgress)`      | same                                        |
fn compute_push_describe_statement(
    state: ProtoState,
    stmt_name: &crate::ident::StmtName,
    reply: ReplyId<crate::reply_id::DescribeStatementKind>,
    staged: &mut StagedActions,
    write_buf: &mut WriteBuf,
) -> ProtoState {
    // DEF-146: classifier dispatch (standard pattern).
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-154 (A): infallible builder via WriteReserved.
            let mut reserved = write_buf.reserve();
            let range = build_describe_message(
                crate::wire::DescribeTargetByte::Statement,
                stmt_name,
                &mut reserved,
            );
            // Describe + Sync bundle. Sync is required — without it
            // PG buffers Extended Query responses and never ships
            // back the `'t'` / `'T'` / `'n'` / `'Z'` sequence.
            emit_actions!(staged, budget: 2, [
                StagedAction::SendBytesRange(range),
                StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
            ]);
            ProtoState::DescribeStatementAwaitingParamDesc(reply)
        }
        crate::state::StatePushClass::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
            ProtoState::Errored(prior_kind)
        }
        crate::state::StatePushClass::Connecting => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            state
        }
        crate::state::StatePushClass::PingAwaiting
        | crate::state::StatePushClass::BusyQuery => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
            state
        }
    }
}

/// Compute the transition for [`PgCommand::DescribePortal`] against
/// the current [`ProtoState`]. Pure; see [`compute_push`] for
/// framing. 1c-3c.
///
/// Mirrors [`compute_push_describe_statement`] — differs only in
/// the target byte (`'P'` vs `'S'`) and the initial post-send
/// state (`DescribePortalAwaitingRowDescOrNoData` — no
/// `ParameterDescription` precedes, per PG §55.2.2).
///
/// Same decision table as statement-describe; see that function's
/// docstring.
fn compute_push_describe_portal(
    state: ProtoState,
    portal_name: &crate::ident::PortalName,
    reply: ReplyId<crate::reply_id::DescribePortalKind>,
    staged: &mut StagedActions,
    write_buf: &mut WriteBuf,
) -> ProtoState {
    // DEF-146: classifier dispatch (standard pattern).
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-154 (A): infallible builder via WriteReserved.
            let mut reserved = write_buf.reserve();
            let range = build_describe_message(
                crate::wire::DescribeTargetByte::Portal,
                portal_name,
                &mut reserved,
            );
            emit_actions!(staged, budget: 2, [
                StagedAction::SendBytesRange(range),
                StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
            ]);
            ProtoState::DescribePortalAwaitingRowDescOrNoData(reply)
        }
        crate::state::StatePushClass::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
            ProtoState::Errored(prior_kind)
        }
        crate::state::StatePushClass::Connecting => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            state
        }
        crate::state::StatePushClass::PingAwaiting
        | crate::state::StatePushClass::BusyQuery => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
            state
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
    reserved: &mut crate::write_buf::WriteReserved<'_>,
) -> crate::action::NonEmptyRange {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_BIND.byte());
    reserved.with_length_prefix(|w| {
        w.push_nul_terminated(portal_name.as_bytes());
        w.push_nul_terminated(stmt_name.as_bytes());
        // n_param_formats = COUNT, followed by COUNT × Binary (wire
        // value 1). The `ParamEncoder` seal guarantees every param
        // ships in binary format — no Text path to dispatch, just
        // write the wire value `1` directly COUNT times. Eliminates
        // the per-element `match format { ... }` of the initial
        // 1c-3b draft — fewer branches in the hot bind path.
        w.push_u16_be(P::COUNT);
        for _ in 0..P::COUNT {
            w.push_u16_be(1);
        }
        w.push_u16_be(P::COUNT);
        // DEF-154 (A) escape hatch: ParamsWriter is a pub trait
        // predating the witness pattern; it takes &mut WriteBuf.
        // Use as_write_buf_mut() + debug_assert on the dead Err
        // branch. ParamsWriter impls are const-asserted safe under
        // MAX_OWNED_SEND_LEN; the Err branch is architecturally
        // dead under that invariant.
        match params.write_params(w.as_write_buf_mut()) {
            Ok(()) => {}
            Err(_) => debug_assert!(
                false,
                "DEF-154: params.write_params overflowed — capacity \
                 invariant broken or ParamsWriter impl wrote past \
                 max_bind_message_size bound",
            ),
        }
        // n_result_formats = 0 → server default (all text). 1c-3b
        // does not negotiate per-column result formats; the user
        // dispatches between text and binary decoders via the
        // `ColumnDesc::format_code` in the provided row_desc.
        w.push_u16_be(0);
    });
    from_write_span_infallible(start, reserved)
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
    reserved: &mut crate::write_buf::WriteReserved<'_>,
) -> crate::action::NonEmptyRange {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_EXECUTE.byte());
    reserved.with_length_prefix(|w| {
        w.push_nul_terminated(portal_name.as_bytes());
        w.push_i32_be(fetch.as_wire_i32());
    });
    from_write_span_infallible(start, reserved)
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
/// | (build infallible post-DEF-154) | —                                              | —                                          |
/// | `Errored(kind)`           | `FailReply(ConnectionAlreadyClosed)`     | `Errored(kind)` preserved                   |
/// | any `Connecting*`         | `FailReply(StartupAlreadyInProgress)`    | same state preserved                        |
/// | any in-flight             | `FailReply(CommandInProgress)`           | same state preserved                        |
#[expect(clippy::too_many_arguments, reason = "compute_push_bind_execute is an internal helper; its arg count matches `push_bind_execute`'s parameter surface + the accumulator + write_buf. Splitting into a struct-arg would obscure the pure-compute framing.")]
fn compute_push_bind_execute<P: crate::params::ParamsWriter>(
    state: ProtoState,
    portal_name: &crate::ident::PortalName,
    stmt_name: &crate::ident::StmtName,
    params: &P,
    schema_ref: Option<crate::schema_arena::SchemaRef>,
    fetch: crate::command::FetchRows,
    reply: ReplyId<crate::reply_id::QueryKind>,
    staged: &mut StagedActions,
    write_buf: &mut WriteBuf,
) -> ProtoState {
    // DEF-146: classifier dispatch (standard pattern).
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-154 (A): both Bind and Execute run against the
            // same WriteReserved. MAX_OWNED_SEND_LEN is const-
            // asserted ≥ bind + execute + sync bundle, so both
            // builders are infallible.
            let mut reserved = write_buf.reserve();
            let bind_range = build_bind_message(portal_name, stmt_name, params, &mut reserved);
            let execute_range = build_execute_message(portal_name, fetch, &mut reserved);
            emit_actions!(staged, budget: 3, [
                StagedAction::SendBytesRange(bind_range),
                StagedAction::SendBytesRange(execute_range),
                StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
            ]);
            // Tier-1 structural dispatch: decide the schema-less
            // DML vs schema-bearing SELECT path ONCE, here at push
            // time. Downstream dispatch arms match on the specific
            // variant — no runtime `match row_desc` at the 'D' arm.
            match schema_ref {
                Some(sr) => ProtoState::BindExecuteAwaitingBindCompleteSelect {
                    reply,
                    schema_ref: sr,
                },
                None => ProtoState::BindExecuteAwaitingBindCompleteDml(reply),
            }
        }
        crate::state::StatePushClass::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
            ProtoState::Errored(prior_kind)
        }
        crate::state::StatePushClass::Connecting => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
            state
        }
        crate::state::StatePushClass::PingAwaiting
        | crate::state::StatePushClass::BusyQuery => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
            state
        }
    }
}

// Pass-#7 F6 / DEF-146 closure (2026-04-22):
//
// Pass-#7's F6 audit proposed a `const fn is_busy_in_flight(&ProtoState)
// -> bool` helper to centralise the "busy-state set" — rejected at
// the time because a guarded match arm is not exhaustive (needs a
// `_ =>` fallback, and every forbid-bundle-legal fallback loses the
// "new variant forces decision" property).
//
// DEF-146 landed the correct form: the classifier is an ENUM
// (`StatePushClass`), not a bool. Each compute_push_* matches the
// classifier exhaustively (5 variants, no `_` fallback) → tier-1
// preserved; the variant enumeration lives once in
// `ProtoState::push_class` (7 × duplication → 1 × authoritative).
// Adding a new ProtoState variant fails the build at push_class if
// uncategorised; the 7 compute_push_* helpers flow through
// automatically.

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
        | ProtoState::BindExecuteAwaitingRfqSelect { .. }
        | ProtoState::DescribeStatementAwaitingParamDesc(_)
        | ProtoState::DescribeStatementAwaitingRowDescOrNoData { .. }
        | ProtoState::DescribeStatementAwaitingRfq { .. }
        | ProtoState::DescribePortalAwaitingRowDescOrNoData(_)
        | ProtoState::DescribePortalAwaitingRfq { .. } => true,
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
    reserved: &mut crate::write_buf::WriteReserved<'_>,
) -> crate::action::NonEmptyRange {
    // DEF-094: write in-place into the caller-owned `write_buf`.
    // DEF-100: return a typed `NonEmptyRange` instead of `(start,
    // end)` — non-zero length is a type invariant, materialise's
    // silent-empty fallback closes from tier-3 (audit) to tier-2
    // (type-checked construction).
    // DEF-154 (A): infallible via WriteReserved capacity witness.
    let start = reserved.len();
    reserved.with_length_prefix(|w| {
        // Protocol version 3.0 = 196608
        w.push_u32_be(crate::wire::PROTOCOL_VERSION_3_0);
        // user=<username>\0
        w.push_nul_terminated(b"user");
        w.push_nul_terminated(user.as_bytes());
        // database=<dbname>\0 (optional)
        if let Some(db) = database {
            w.push_nul_terminated(b"database");
            w.push_nul_terminated(db.as_bytes());
        }
        // application_name=<name>\0 (optional)
        if let Some(name) = app_name {
            w.push_nul_terminated(b"application_name");
            w.push_nul_terminated(name.as_bytes());
        }
        // Trailing empty key NUL
        w.push_u8(0);
    });
    from_write_span_infallible(start, reserved)
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
    arena: crate::schema_arena::ArenaReader<'r>,
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
            // DEF-112 + DEF-119: `DeliverReplyEntry` carries a
            // lifetime-free `StagedReply`. Materialise resolves any
            // `SchemaRef` handles into `&'r RowDesc` borrows via the
            // arena, producing the public `Reply<'r>`. The entry was
            // constructed by the typed `action::deliver` path —
            // kind-payload pairing was enforced at dispatch time.
            StagedAction::DeliverReply(entry) => Action::DeliverReply {
                id: entry.id(),
                value: entry.staged().into_public(arena),
            },
            StagedAction::FailReply { id, cause } => Action::FailReply { id, cause },
            // 1c-1b: row_range is absolute coordinates into the
            // read buffer's populated region (`ReadBuf::populated`),
            // valid from emission through materialise because
            // the `'r` borrow on OutActions blocks all
            // `&mut self.read_buf` calls on PgProtocol until the
            // caller drops the returned actions.
            //
            // DEF-119: `schema_ref` is a 2-byte arena handle
            // (post-DEF-148 NonZeroU8 + generation). We resolve via
            // `arena.get(ref)` to a `&'r RowDesc`. The resolution
            // `None` branch is architecturally dead — alloc happens
            // in dispatch at the same moment the state variant is
            // entered that the staged action was constructed from,
            // so the slot is live.
            //
            // DEF-170 (audit2 A010): debug_assert shields the dead
            // None branch loudly in tests. DEF-150 reserved
            // `CrateBugLocus::StaleSchemaRef` for the full structural
            // classification; DEF-154 (buffer-witness) will
            // eliminate the class entirely by making the resolution
            // compile-enforced. Until then: debug-time loud, release
            // falls back to `RowDesc::EMPTY` — forbid-bundle bans
            // `panic!` so this is the tightest non-witness closure.
            StagedAction::StreamRowRange { id, row_range, schema_ref } => {
                let desc_opt = arena.get(schema_ref);
                debug_assert!(
                    desc_opt.is_some(),
                    "DEF-170: stale SchemaRef at materialise (StreamRowRange) — \
                     crate bug; DEF-154 witness-pattern will eliminate \
                     this class structurally.",
                );
                Action::StreamRow {
                    id,
                    row_bytes: row_range.apply(read_buf_bytes).unwrap_or(&[]),
                    desc: desc_opt.unwrap_or(&crate::decode::RowDesc::EMPTY),
                }
            }
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
        // DEF-145: nz(0) is a test bug — a zero raw correlator cannot
        // be minted by a real ReplyId allocator (NonZeroU64 by type).
        // Pre-DEF-145 the `unwrap_or(MIN)` fallback silently coerced
        // `0 → 1`, potentially colliding with a concurrent nz(1).
        // Assert fires loud; the `unwrap_or(MIN)` keeps the forbid-bundle
        // happy (clippy::unwrap_used forbidden) on the assertion-proved
        // dead branch.
        assert!(n > 0, "nz(0) is a test bug — use nz(1..) for non-zero test correlators");
        NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN)
    }

    /// Consume any ReplyId carried by a state so the Drop-guard does
    /// not trip at end-of-scope.
    ///
    /// # Pass-#7 F14: delegate to `ProtoState::take_inflight_reply_raw_id`
    ///
    /// Pre-pass-#7 this was a hand-rolled 20-line exhaustive match
    /// over all ~22 `ProtoState` variants. State.rs has THE
    /// authoritative version (`take_inflight_reply_raw_id`) which
    /// (a) takes `self` by value → consumes the carried `ReplyId<_>`
    /// via its `.consume()` method, (b) returns the raw
    /// `Option<NonZeroU64>` which the test doesn't need.
    ///
    /// Drift surface closed: one exhaustive match in `state.rs`,
    /// zero parallel matches here. Adding a new variant fails the
    /// build in state.rs's authoritative site and automatically
    /// flows through `consume_state` without touching this file.
    ///
    /// The return value is `Option<NonZeroU64>` — `Copy`, no `Drop`
    /// — statement-discarded via `drop()` (explicit no-op, avoids
    /// the forbid-bundle-banned `let _ = ...`). Reading the
    /// `Option::Some(u64)` payload here would add zero value.
    fn consume_state(state: ProtoState) {
        // Side-effect call: `take_inflight_reply_raw_id` consumes the
        // carried `ReplyId<_>` via its internal `.consume()` (marks
        // delivered=true so the Drop-guard doesn't fire). The
        // returned `Option<NonZeroU64>` is Copy / no Drop — bare
        // expression-statement form discards without `let _`
        // (user-banned). Same pattern as `ping.consume();` in
        // reply_id tests.
        match state.take_inflight_reply_raw_id() {
            Some(_) | None => {}
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
        // DEF-061 + DEF-142: Errored carries `StateErrorKind`
        // (1 byte, AlreadyClosed-excluded newtype over ErrorKind).
        let errored = ProtoState::Errored(crate::error::StateErrorKind::from_kind_or_internal(crate::error::ErrorKind::Framing));
        assert!(!allows_unsolicited_param_status(&errored));
        consume_state(errored);

        // 1c-1b: simple-query states all accept unsolicited PS
        // (server may emit ParameterStatus mid-query if an
        // `ALTER SYSTEM` fires). Exhaustive enumeration pins the
        // policy row per-variant.
        let q_first = ProtoState::SimpleQueryAwaitingFirstResponse(ReplyId::from_raw(nz(8001)));
        assert!(allows_unsolicited_param_status(&q_first));
        consume_state(q_first);

        // DEF-119 + DEF-148: state carries `schema_ref: SchemaRef`
        // (2 bytes: NonZeroU8 slot + u8 generation). Test fixture
        // allocates an empty schema into a local arena slab to obtain
        // a valid SchemaRef handle; the handle carries no lifetime.
        //
        // Forbid-bundle note: `panic!`, `.unwrap()`, `.expect()`,
        // `unreachable!()` are all banned. The `assert!(is_some) +
        // unwrap_or(fallback)` idiom fails loudly on precondition
        // break (alloc on a fresh slab MUST succeed by spec); the
        // fallback is architecturally dead but must still produce a
        // well-typed `SchemaRef`. DEF-148 added `SchemaRef::dead_for_test`
        // as the test-only sentinel replacing the pre-DEF-148 `ZERO`
        // (`ZERO` could no longer exist — NonZeroU8 forbids 0).
        let mut arena = crate::schema_arena::SchemaSlab::new();
        let alloc_result = arena.alloc(crate::decode::RowDesc::EMPTY);
        assert!(alloc_result.is_some(), "alloc on fresh slab must succeed");
        let sr = alloc_result.unwrap_or_else(crate::schema_arena::SchemaRef::dead_for_test);
        let q_rows = ProtoState::SimpleQueryStreamingRows {
            reply: ReplyId::from_raw(nz(8002)),
            schema_ref: sr,
        };
        assert!(allows_unsolicited_param_status(&q_rows));
        consume_state(q_rows);

        let q_rfq = ProtoState::SimpleQueryAwaitingRfq {
            reply: ReplyId::from_raw(nz(8003)),
            command_tag: crate::error::BoundedStr::default(),
            schema_ref: None,
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
        // DEF-145: nz(0) is a test bug — a zero raw correlator cannot
        // be minted by a real ReplyId allocator (NonZeroU64 by type).
        // Pre-DEF-145 the `unwrap_or(MIN)` fallback silently coerced
        // `0 → 1`, potentially colliding with a concurrent nz(1).
        // Assert fires loud; the `unwrap_or(MIN)` keeps the forbid-bundle
        // happy (clippy::unwrap_used forbidden) on the assertion-proved
        // dead branch.
        assert!(n > 0, "nz(0) is a test bug — use nz(1..) for non-zero test correlators");
        NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN)
    }

    /// Consume any ReplyId carried by `state` so its Drop-guard does
    /// not trip when the state drops at end of scope.
    ///
    /// # Pass-#7 F14: delegate to `take_inflight_reply_raw_id`
    ///
    /// Pre-pass-#7 this was a hand-rolled 20-line match, documented
    /// as "copy of the helper in `allows_unsolicited_param_status_tests`
    /// — module privacy forbids re-use without cross-module exposure."
    /// After pass-#7, `state.rs` exposes `take_inflight_reply_raw_id` as
    /// `pub(crate)` — both test modules delegate to it, eliminating
    /// the parallel-match drift surface. New variants categorised
    /// once in `state.rs` automatically flow through all test
    /// helpers.
    fn consume_state(state: ProtoState) {
        // Side-effect call: `take_inflight_reply_raw_id` consumes the
        // carried `ReplyId<_>` via its internal `.consume()` (marks
        // delivered=true so the Drop-guard doesn't fire). The
        // returned `Option<NonZeroU64>` is Copy / no Drop — bare
        // expression-statement form discards without `let _`
        // (user-banned). Same pattern as `ping.consume();` in
        // reply_id tests.
        match state.take_inflight_reply_raw_id() {
            Some(_) | None => {}
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
        // DEF-142 (pass-#8): prior_kind is now `StateErrorKind` — the
        // AlreadyClosed-free newtype. Test constructs via helper.
        use crate::error::ErrorKind;
        let raw_id = nz(102);
        let prior_kind_raw = ErrorKind::Framing;
        let prior_kind = crate::error::StateErrorKind::from_kind_or_internal(prior_kind_raw);
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
            "expected FailReply(ConnectionAlreadyClosed{{prior_kind={prior_kind_raw:?}}})",
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
        // DEF-061 + DEF-142 semantic — same shape as
        // `ping_from_errored_preserves_kind_and_fails_with_connection_already_closed`.
        use crate::error::ErrorKind;
        let Some(user) = mk_user() else { return };
        let raw_id = nz(302);
        let prior_kind_raw = ErrorKind::Framing;
        let prior_kind = crate::error::StateErrorKind::from_kind_or_internal(prior_kind_raw);
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
            "expected FailReply(ConnectionAlreadyClosed{{prior_kind={prior_kind_raw:?}}})",
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
