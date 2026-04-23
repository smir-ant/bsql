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

/// Push 1..=N actions into `staged`, with compile-time enforcement
/// of the per-site budget.
///
/// # DEF-154 (Q) P1-6: infallible-only form
///
/// Pre-(Q), `emit_actions!` had two forms: `on_overflow: break`
/// (loop form — bailed the enclosing loop on Err) and no-bail
/// (const-assert-proven fit). The former looked safe (gate
/// reserved `WORST_CASE_PER_DISPATCH` slots before loop entry) but
/// was a silent-loss footgun: if the gate ever drifted, terminal
/// `FailReply + CloseSocket` could be dropped while state had
/// ALREADY transitioned to `Errored` — caller sees state_errored
/// but no Action delivery, orphaned oneshot receiver.
///
/// Post-(Q) only the no-bail infallible form remains. The dispatch
/// gate at feed_bytes reserves `WORST_CASE_PER_DISPATCH = 2` slots
/// before entering any arm, so the Errored arm's 2-action emission
/// always fits the staged cap. `match Ok(()) | Err(_) => {}` is
/// explicit dead-arm handling (no `.unwrap_or(())` silent
/// fallback, no debug_assert panic target).
///
/// ```text
/// emit_actions!(staged, budget: 1, [
///     Action::SendBytes(SendBuf::from_slice(&SYNC_WIRE_BYTES)?),
/// ]);
/// ```
///
/// Compile-time checks (both are `const _: () = assert!(…)`):
///
/// 1. `MAX_STAGED_PER_CALL >= budget` — site's declared budget
///    fits within the staged cap.
/// 2. `budget >= count(actions)` — site does not push more than
///    its declared budget.
///
/// DEF-045. Form split + merge: DEF-055 + DEF-154 (Q).
macro_rules! emit_actions {
    (
        $out:expr, budget: $budget:literal,
        [$($action:expr),+ $(,)?]
    ) => {{
        const _: () = assert!(
            $crate::protocol::MAX_STAGED_PER_CALL >= $budget,
            "emit_actions! per-site budget exceeds MAX_STAGED_PER_CALL",
        );
        const _: () = assert!(
            $budget >= count_exprs!($($action),+),
            "emit_actions! site pushes more actions than its declared budget",
        );
        $(
            match $out.push($action) {
                Ok(()) => {}
                Err(_) => {
                    // Architecturally unreachable: the dispatch gate
                    // at feed_bytes (protocol.rs) reserves
                    // WORST_CASE_PER_DISPATCH slots before arm entry.
                    // Dead-arm explicit, no silent fallback.
                }
            }
        )+
    }};
}

/// DEF-154 (B) Phase B4-W P0-2 + P0-3 fix helper.
///
/// Every `build_*_message` returns `Result<WriteRange,
/// ProtocolError>` post-audit. The Err path is architecturally
/// cold (builder bug / const-drift / user ParamsWriter overflow)
/// but classified — `compute_push_*` handles it via `FailReply +
/// CloseSocket + Errored` state transition.
///
/// This macro centralises that handling. Each `compute_push_*`
/// Idle arm uses `let range = try_builder!(build_X(...), reply,
/// staged);`. On `Err(cause)`: derive `StateErrorKind` via
/// `cause.state_kind()` (DEF-175/176 pattern), emit the FailReply
/// and CloseSocket into `staged`, and early-return
/// `ProtoState::Errored(state_kind)` from the enclosing
/// `compute_push_*` function.
///
/// The macro early-returns, so it must be used in a position
/// where `return ProtoState::Errored(...)` is legal.
macro_rules! try_builder {
    ($result:expr, $reply:expr, $staged:expr) => {
        match $result {
            Ok(r) => r,
            Err(cause) => {
                // DEF-154 (I): state_kind is total — no unwrap_or_else
                // + debug_assert dance. Builders never return
                // AlreadyClosed; the total projection fills any
                // hypothetical AlreadyClosed with Internal honestly.
                let state_kind = cause.state_kind();
                emit_actions!($staged, budget: 2, [
                    StagedAction::FailReply { id: $reply.consume(), cause },
                    StagedAction::CloseSocket,
                ]);
                return ProtoState::Errored(state_kind);
            }
        }
    };
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
///
/// # DEF-154 (L) P0-1 + P0-2(a): staged / output split
///
/// Pre-(L), `MAX_ACTIONS_PER_CALL = 8` governed BOTH the staged
/// (dispatch-side) and output (user-side) capacity. Materialise
/// emits up to 2 actions per staged entry on the stale-SchemaRef
/// fan-out path (`FailReply + CloseSocket`) — a 16-action
/// worst-case that did not fit the 8-slot output container,
/// causing `.unwrap_or(())` to silently drop terminal actions.
///
/// Post-(L): `MAX_STAGED_PER_CALL = 8` bounds dispatch's stage
/// container; `MAX_ACTIONS_PER_CALL = MAX_STAGED_PER_CALL * MAX_FANOUT_PER_STAGED = 16`
/// bounds the output container (compile-asserted below). Worst-case
/// fanout is then ARCHITECTURALLY contained — the silent-drop
/// class is closed at the type/capacity level, not at a runtime
/// shield.
///
/// Also a 2× quick-win for the SELECT-large bottleneck: 15-row
/// streaming density per call (vs 7 pre-(L)) halves feed_bytes
/// round-trips on 1M-row queries. The full pull-based RowStream
/// redesign (P0-2(c)) is the deeper fix.
///
/// # Emission-site vs aggregate
///
/// - **Per emission site — tier 1 compile**: `emit_actions!` asserts
///   budget ≤ `MAX_STAGED_PER_CALL` via `const _: () = assert!(...)`.
/// - **Aggregate output — tier 1 compile (post-(L))**:
///   `const _: () = assert!(MAX_ACTIONS_PER_CALL >= MAX_STAGED_PER_CALL * MAX_FANOUT_PER_STAGED);`
///   guarantees `out.push(a)` during materialise cannot overflow,
///   so the `.unwrap_or(())` pattern is genuinely architecturally
///   dead (not just "believed dead by code review").
pub const MAX_STAGED_PER_CALL: usize = 8;

/// Worst-case output-action fan-out per single staged action.
///
/// - Most staged actions map 1:1 to one `Action`.
/// - `StagedAction::DeliverReply` with a stale `SchemaRef` in its
///   payload (e.g. schema-bearing `QueryComplete`) emits 2
///   (`FailReply { StaleSchemaRef } + CloseSocket`).
///
/// DEF-154 (Y): `StagedAction::StreamRowRange` with stale ref
/// ALSO fanned to 2 actions; post-(Y) the variant is deleted —
/// row-bearing responses flow through `iter_rows` exclusively,
/// where stale-ref is classified inline in the fast-path. The
/// 2-fanout worst case is now driven purely by `DeliverReply`.
///
/// 2 is the documented worst-case; a future staged variant that
/// fans out to 3 actions must bump this const AND
/// `MAX_ACTIONS_PER_CALL` in lockstep (the const-assert below
/// catches a missing bump).
pub const MAX_FANOUT_PER_STAGED: usize = 2;

/// DEF-184 (A15): maximum number of fanout-2 staged entries per
/// dispatch call. Post-DEF-154 (Y) `StreamRowRange` deletion, the
/// only remaining fanout-2 staged variant is `DeliverReply` with
/// stale-ref (architecturally dead — `CrateBugLocus::StaleSchemaRef`).
///
/// # Exhaustive case analysis — why `MAX_FANOUT2_ENTRIES = 1`
///
/// 1. **Which StagedAction variants fan out to 2 actions in
///    materialise?** Grep `push_within_fanout_budget` call sites
///    in `materialise`:
///    - `StagedAction::SendBytesRange` on `None` apply: 1 action
///      (`CloseSocket`) + continue. **Not fanout-2** (only 1
///      action emitted before continue; the continue skips the
///      normal SendBytes emission).
///    - `StagedAction::DeliverReply` on stale-ref `Err(_stale)`:
///      2 actions (`FailReply + CloseSocket`). **FANOUT-2**.
///    - `StagedAction::SendBytesStatic`: 1 action.
///    - `StagedAction::FailReply`: 1 action.
///    - `StagedAction::CloseSocket`: 1 action.
///
///    **Conclusion:** only `DeliverReply` stale-ref is fanout-2.
///
/// 2. **How many `DeliverReply` staged entries can a single
///    dispatch call produce?** Grep `StagedAction::DeliverReply`
///    in dispatch.rs — all construction sites use `action::deliver`
///    which is called from `DispatchOutcome::AdvancedWithAction`.
///    The dispatch loop emits ONE `AdvancedWithAction` per frame,
///    and only terminal frames (RFQ, Z, CommandComplete,
///    Authentication Ok/Final, ParseComplete, CloseComplete,
///    BindComplete, etc.) emit a DeliverReply. For pre-1c-5
///    single-inflight pattern, a single feed_bytes cycle
///    processes one reply — at most ONE DeliverReply per call.
///    **Conclusion:** `MAX_FANOUT2_ENTRIES ≤ 1` pre-pipelining.
///
/// 3. **Can the dispatch loop emit 2+ DeliverReply in one call?**
///    No — state after DeliverReply transitions away from the
///    waiting-for-reply state (back to Idle or an intermediate),
///    blocking a second reply emission in the same feed_bytes
///    iteration. Confirmed via state-machine audit (see
///    `state.rs` transitions).
///
/// 4. **Regression trap — 1c-5 pipelining:** if pipelining adds
///    batched replies (multiple DeliverReply per call), this const
///    MUST bump. The const-assert below catches a stale
///    `MAX_ACTIONS_PER_CALL` when the bump is forgotten.
///
/// # Why not leave `MAX_ACTIONS = STAGED × FANOUT = 16` safely?
///
/// Because stack reservation is proportional: 16 × 312 B = 4992 B
/// vs 9 × 312 B = 2808 B. The -2184 B saving per call × per-
/// connection × QPS is the perf win. Overestimating the cap
/// leaves dead stack space that compiler can't optimise away
/// (heapless::Vec reserves `[MaybeUninit<T>; N]` up front).
const MAX_FANOUT2_ENTRIES_PER_CALL: usize = 1;

/// Output-side action capacity — bounds `OutActions` storage.
///
/// DEF-184 (A15): tightened from
/// `MAX_STAGED * MAX_FANOUT = 8 × 2 = 16` down to
/// `MAX_STAGED + MAX_FANOUT2_ENTRIES × (MAX_FANOUT − 1) =
/// 8 + 1 × 1 = 9`. Reflects the post-(Y) reality that only one
/// staged entry (DeliverReply) can fanout to 2 actions. 7 normal
/// + 1 fanout-2 = 9 outputs maximum.
///
/// **Bench impact:** `OutActions` stack reservation drops from
/// `16 × 312 B = 4992 B` to `9 × 312 B = 2808 B` — 2184 bytes
/// saved per OutActions instance. Combined with DEF-184 A2/B1
/// (ManuallyDrop<heapless::Vec>), init cost stays 0 B regardless
/// of capacity; what shrinks now is the stack FRAME.
pub const MAX_ACTIONS_PER_CALL: usize =
    MAX_STAGED_PER_CALL + MAX_FANOUT2_ENTRIES_PER_CALL * (MAX_FANOUT_PER_STAGED - 1);

// DEF-184 (A15): tight upper bound. Any dispatch/materialise
// path that produces more than 9 actions per call would overflow
// OutActions. Const-assert below verifies:
//   `9 ≥ 8 + 1 × (2 - 1) = 9` — exactly.
// If a future refactor adds a SECOND fanout-2 staged variant (e.g.
// a batched DeliverReply for pipelining in 1c-5), bump
// MAX_FANOUT2_ENTRIES_PER_CALL accordingly.
const _: () = assert!(
    MAX_ACTIONS_PER_CALL >= MAX_STAGED_PER_CALL
        + MAX_FANOUT2_ENTRIES_PER_CALL * (MAX_FANOUT_PER_STAGED - 1),
    "MAX_ACTIONS_PER_CALL (output capacity) must be >= \
     MAX_STAGED_PER_CALL + MAX_FANOUT2_ENTRIES × (MAX_FANOUT − 1). \
     Post-DEF-184 A15: 9 = 8 + 1 × 1. If a second fanout-2 staged \
     variant lands, bump MAX_FANOUT2_ENTRIES_PER_CALL.",
);

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
    /// # Placement rationale (DEF-184 audit-2 item-1)
    ///
    /// The arena lives on `PgProtocol` (not in a separate pool or
    /// thread-through parameter) by design. `SchemaRef` values
    /// embed in `ProtoState` variants (e.g. `StreamingRows { schema_ref }`)
    /// and in staged Action/Reply payloads. Arena + ProtoState share
    /// a single `&mut PgProtocol` borrow in `feed_bytes` /
    /// `push_command`, so the ref-lifetime flow stays within one
    /// scope and the arena cannot be mis-passed against a SchemaRef
    /// minted by a different arena instance.
    ///
    /// SchemaRef staleness is classified independently via
    /// generational counter (tier-2 structural — `Option<&RowDesc>`
    /// returns `None` on gen mismatch). Arena co-location does NOT
    /// supply a tier-1 compile guarantee on its own; it's a
    /// borrow-scope convenience that keeps the tier-2 staleness
    /// check simple. A future out-of-body pool (e.g. per-connection
    /// pool with shared generation space) is architecturally
    /// possible but would require an additional correlation
    /// invariant (tier-2 classifier for "ref minted by a different
    /// pool"), so the placement is load-bearing for the current
    /// single-borrow invariant model.
    ///
    /// Cost: ~528 B on `PgProtocol`, paid once per connection.
    /// Benefit: state drops from ~1224 B → ~300 B;
    /// `Action::StreamRow` drops from ~280 B → ~32 B;
    /// per-row DataRow emission saves ~260 B (hot path on SELECT).
    schema_arena: crate::schema_arena::SchemaSlab,
    /// DEF-184 (A1+A13) error-payload arena — single-slot storage
    /// for `ProtocolError::ServerErrorResponse` bounded strings.
    ///
    /// Pre-(184): inline `BoundedStr<128> + BoundedStr<96> +
    /// BoundedStr<64>` in the `ServerErrorResponse` variant
    /// cascaded through `Action::FailReply.cause: ProtocolError`
    /// → `OutActions = [Action; 9]` → 9 × 312 B = 2808 B stack
    /// frame. `StreamItem::FailReply.cause` similarly 320 B
    /// per-pull return-by-value.
    ///
    /// Post-(184): variant carries `details_ref: ErrorRef` (~2 B);
    /// full payload lives in this arena, resolved by callers via
    /// [`Self::get_server_error`].
    ///
    /// Cost: ~290 B on `PgProtocol` (one `Option<ErrorPayload>`
    /// plus u8 generation plus padding).
    ///
    /// Benefit: `ProtocolError` 312 → ~32 B; `Action` Reply-
    /// bounded (~88 B); `OutActions` 2808 → ~792 B (3.5×);
    /// `StreamItem` 320 → ~80 B (4×).
    ///
    /// # Placement rationale (DEF-184 audit-2 item-1)
    ///
    /// Same single-borrow convenience as `schema_arena` above.
    /// `ErrorRef` (carried by `ProtocolError::ServerErrorResponse`)
    /// resolves via `&self` of `PgProtocol` — arena co-located with
    /// state keeps the ref lifetime within a single borrow scope.
    /// Staleness is tier-3 classified via
    /// [`crate::error_arena::ArenaError::Stale`]; placement is a
    /// load-bearing design decision for the current single-borrow
    /// invariant, NOT a tier-1 compile-enforced guarantee. A future
    /// refactor moving the arena out (e.g. per-worker pool) would
    /// need an additional correlation invariant classifier.
    error_arena: crate::error_arena::ErrorArena,
    /// DEF-184 (A10/B22) SCRAM handshake data — externalised from
    /// `ProtoState` SCRAM variants.
    ///
    /// Pre-(A10): `ScramSession` (512 B) + `client_first_bare`
    /// (128 B) + `client_nonce_b64` (48 B) + `expected_server_sig`
    /// (32 B) lived inline in 4 `ProtoState::ConnectingScram*`
    /// variants. Rust enum sized by max variant → `ProtoState`
    /// dominated at ~712 B by `ConnectingScramAwaitingServerFirst`.
    ///
    /// Post-(A10): heavy SCRAM data moves here; `ProtoState` SCRAM
    /// variants become thin `{ reply: ReplyId<StartupKind> }`
    /// shapes. `ProtoState` shrinks 712 B → **80 B exact** — every
    /// `core::mem::replace(state, Idle)` inside `dispatch()` now
    /// moves 80 B instead of 712 B (**632 B × N_dispatches** saved).
    ///
    /// Correlation invariant (tier-2 structural per CREDO §1):
    /// `state is ConnectingScram*` ⇔ `scram_state is Some(..)`
    /// with matching variant shape. A drift between the pair
    /// classifies as [`crate::error::CrateBugLocus::ScramStateDrift`]
    /// rather than silent take-from-None. See
    /// [`crate::scram_state`] module docs for the full table.
    ///
    /// Cost: ~704 B on `PgProtocol` (dominated by `AwaitingFirst`
    /// variant + `Option` disc). Lives `Some(_)` only during the
    /// 3-4 SCRAM handshake frames; cleared at AuthOk or any
    /// errored transition.
    scram_state: Option<crate::scram_state::ScramHandshakeState>,
    /// DEF-154 (H+V): deferred `read_buf` cursor advance — bytes to
    /// skip at the start of the NEXT `feed_bytes` call before any
    /// new frame parsing.
    ///
    /// Pre-(H), the advance fired in-scope via
    /// `BrandedReadBuf::advance_scope_local` so that staged
    /// `ReadRange<'rb>` (start/len pair + phantom brand) could stay
    /// unaffected by the cursor move. Post-(H), `StreamRowRange`
    /// carries `&'r [u8]` slices of `read_buf.populated()` directly;
    /// Rust's borrow checker blocks any `&mut self.read_buf` call
    /// while OutActions holds those `'r` slices — so advance must
    /// defer until OutActions drops.
    ///
    /// DEF-154 (V) P1-2 (audit-2): typed as `Option<NonZeroU16>`
    /// rather than `u16` with `0 = sentinel`. Niche-packs to same
    /// 2 bytes (NonZeroU16 zero-niche is Option's None discriminant).
    /// `None` IS the "no pending advance" state — previously a
    /// zero-valued u16 was semantically the same but nothing at the
    /// type level prevented a future edit from assigning 0 where a
    /// legit non-zero was expected. Post-(V) the invariant is
    /// tier-1 compile.
    ///
    /// `PgProtocol: !Sync`, so no external observer sees the interim
    /// "not-yet-advanced" cursor state between calls. Value bounded
    /// by `READ_BUF_CAP <= u16::MAX` const-assert.
    pending_advance: Option<core::num::NonZeroU16>,
    /// `!Sync` marker — `Cell<T>: !Sync`, so the whole struct inherits.
    /// Load-bearing: the crate-root ambiguous-impl gate verifies that
    /// `PgProtocol: !Sync` compile-time. Renamed from the earlier
    /// `_not_sync` (leading-underscore convention for structurally-used
    /// fields is forbidden per user-feedback memory).
    sync_marker: PhantomData<Cell<()>>,
}

#[cfg(test)]
impl PgProtocol {
    /// DEF-184 (A10/B22 audit P1-3): test-only forge hook. Lets
    /// lib-internal unit tests construct drift states (e.g.
    /// `ProtoState::ConnectingStartupScram { reply }` paired with
    /// `scram_state = None`) to exercise
    /// [`crate::error::CrateBugLocus::ScramStateDrift`] in dispatch.
    pub(crate) fn test_force_scram_state(
        &mut self,
        scram: Option<crate::scram_state::ScramHandshakeState>,
    ) {
        self.scram_state = scram;
    }

    /// DEF-184 (A10/B22 audit P1-3): test-only forge hook for the
    /// `state` field. See `test_force_scram_state`.
    pub(crate) fn test_force_state(&mut self, new_state: crate::state::ProtoState) {
        self.state = new_state;
    }
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
            error_arena: crate::error_arena::ErrorArena::new(),
            scram_state: None,
            pending_advance: None,
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
    ///
    /// DEF-154 (H): returns the EFFECTIVE unread region — raw
    /// `read_buf.unread()` with the deferred `pending_advance`
    /// prefix skipped. Pre-(H), `feed_bytes` applied the advance
    /// in-scope so `unread()` never showed a not-yet-advanced
    /// state; post-(H), advance defers to the NEXT feed_bytes
    /// call's entry, but external observers (tests, introspection
    /// hosts) still see the dispatched frames as "consumed".
    ///
    /// DEF-154 (P) P0-5: replaced `.unwrap_or(&[])` with an
    /// explicit `split_at_checked` match. Pre-(P) the silent
    /// fallback would have masked a `pending_advance >
    /// unread().len()` invariant break (architecturally dead —
    /// `pending_advance` accumulates validated `total_len` values
    /// per parse_header, and between calls only `append` can
    /// grow unread — but the silent form violated user's "no
    /// silent fallback" directive). Post-(P): the None arm is
    /// tier-2 structural-invariant proof; returning `&[]` is
    /// semantically correct ("cursor past end" = "no bytes left
    /// to observe"), match makes the dead-branch decision
    /// explicit instead of hiding behind `unwrap_or`.
    #[inline]
    #[must_use]
    pub fn unread(&self) -> &[u8] {
        let raw = self.read_buf.unread();
        let skip = match self.pending_advance {
            Some(n) => usize::from(n.get()),
            None => 0,
        };
        // `split_at_checked(n) -> Option<(&[u8], &[u8])>`:
        // None iff n > raw.len(). Architecturally dead (see doc).
        match raw.split_at_checked(skip) {
            Some((_already_advanced, rest)) => rest,
            None => {
                // Invariant-break: cursor past end. Return empty
                // ("no observable bytes"); caller sees same result
                // as a genuinely-drained buffer. Production-safe
                // because `unread()` is a read-only observer —
                // no corruption vector. Documented-dead branch.
                &[]
            }
        }
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
        // this path — unifying it to `'s` here gives the caller
        // freedom over what they pair the result with later.
        write_buf.clear();

        // DEF-172: centralised entry-point arena reclamation.
        self.clear_arena_if_idle_or_errored();

        // DEF-154 (B+H): write-side keeps its brand (`'wb`) for
        // tier-1 `WriteRange::apply`; read side is unbranded (push
        // paths never emit StreamRowRange so the read-buf view is
        // unused by materialise — pass no read slice).
        let state = &mut self.state;
        let schema_arena = &mut self.schema_arena;
        let scram_state = &mut self.scram_state;
        write_buf.with_branded(|mut wb| -> OutActions<'w, 's> {
            let prev = core::mem::take(state);
            let (new_state, staged) = {
                let mut reserved = wb.reserve();
                compute_push(cmd, prev, &mut reserved, scram_state)
            };
            *state = new_state;
            materialise(staged, wb.into_bytes(), schema_arena.as_reader())
        })
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
        // state machine.
        //
        // DEF-183 (P1-A from Senior audit): routed through
        // `as_writer()` witness for consistency with dispatch()'s
        // alloc path. NLL ends the writer borrow at the last use.
        let schema_ref = match row_desc {
            Some(desc) => self.schema_arena.as_writer().alloc(desc),
            None => None,
        };

        // DEF-154 (B+H): write-brand only. Push paths emit no
        // StreamRowRange; materialise doesn't need a read view.
        let state = &mut self.state;
        let schema_arena = &mut self.schema_arena;
        write_buf.with_branded(|mut wb| -> OutActions<'w, 's> {
            let prev = core::mem::take(state);
            let mut staged = StagedActions::new();
            let new_state = {
                let mut reserved = wb.reserve();
                compute_push_bind_execute(
                    prev,
                    portal_name,
                    stmt_name,
                    params,
                    schema_ref,
                    fetch,
                    reply,
                    &mut staged,
                    &mut reserved,
                )
            };
            *state = new_state;
            materialise(staged, wb.into_bytes(), schema_arena.as_reader())
        })
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
        // DEF-184 (B6): `const BOUNDED = false` specialisation —
        // monomorphised body with the per-iter bound check
        // eliminated at compile time. Production hot path no longer
        // pays `if dispatches_this_call >= max_dispatches` every
        // frame (the pre-(184) shape supplied u16::MAX, which LLVM
        // sometimes optimised away but only via inlining — not
        // guaranteed on large functions).
        self.feed_bytes_impl::<false>(bytes, write_buf, 0)
    }

    /// DEF-154 (X) P0-2(c): frame-bounded variant of [`feed_bytes`].
    ///
    /// Processes at most `max_dispatches` actionable dispatches
    /// from the read buffer, then breaks the inner loop. Silent
    /// pre-dispatch skips (`PARAMETER_STATUS`, `NOTICE_RESPONSE`)
    /// do NOT count against the budget — they're transparent noise.
    /// A malformed-length / oversized-frame classification counts
    /// as one dispatch (the terminal `break` already limits it).
    ///
    /// Used by [`RowStream`](crate::RowStream)'s slow path to
    /// process exactly one frame per call (ensures the fast-path
    /// gets control back after a silent `RowDescription`). The
    /// production [`feed_bytes`] entry takes the `BOUNDED=false`
    /// specialisation, eliminating the gate entirely at compile
    /// time.
    #[inline]
    pub(crate) fn feed_bytes_bounded<'w, 'r>(
        &'r mut self,
        bytes: &[u8],
        write_buf: &'w mut WriteBuf,
        max_dispatches: u16,
    ) -> OutActions<'w, 'r> {
        self.feed_bytes_impl::<true>(bytes, write_buf, max_dispatches)
    }

    /// DEF-184 (B6): const-generic dispatch loop body.
    ///
    /// `const BOUNDED: bool` selects between the bounded
    /// (RowStream slow-path, one-frame-per-call) and unbounded
    /// (`feed_bytes` default) monomorphisations. LLVM eliminates
    /// the gate branch in the `BOUNDED = false` specialisation.
    ///
    /// Cost: two monomorphised copies of the loop in the binary
    /// (approximately 1 KB of extra LLVM IR pre-optimisation;
    /// release profile has LTO fat + codegen-units=1 which
    /// further de-duplicates common sub-expressions, so actual
    /// text-segment growth is smaller).
    fn feed_bytes_impl<'w, 'r, const BOUNDED: bool>(
        &'r mut self,
        bytes: &[u8],
        write_buf: &'w mut WriteBuf,
        max_dispatches: u16,
    ) -> OutActions<'w, 'r> {
        write_buf.clear();
        self.clear_arena_if_idle_or_errored();

        // DEF-154 (H): apply any pending cursor advance from the
        // PREVIOUS feed_bytes call. Previously, advance fired
        // in-scope via `BrandedReadBuf::advance_scope_local` so that
        // staged `ReadRange<'rb>` could stay unaffected by the cursor
        // move — a brand-magic requirement. Post-(H), StreamRowRange
        // carries `&'r [u8]` slices directly, so the stage-time
        // borrow blocks any &mut on read_buf until OutActions drops.
        // Deferring advance to the next call's entry breaks the
        // conflict at zero extra state cost: one u16 field. See
        // DEF-149 preservation note on StagedAction::StreamRowRange.
        //
        // Err branch is architecturally dead (pending_advance was
        // computed as `sum(total_len)` from validated parse_header
        // frames against the call-time populated length; between
        // calls, only append occurs, which only grows inner). On
        // actual Err (e.g. a hypothetical regression in cursor math)
        // we classify as InternalCrateBug and fall through to the
        // Errored fast-path below.
        let mut pending_advance_err = false;
        if let Some(n) = self.pending_advance {
            if self.read_buf.advance(usize::from(n.get())).is_err() {
                pending_advance_err = true;
            }
            self.pending_advance = None;
        }

        let is_errored_or_recovering =
            pending_advance_err || matches!(self.state, ProtoState::Errored(_));

        // DEF-154 (H): append BEFORE destructure (append takes
        // `&mut self.read_buf`; destructure holds it as a
        // field-level &mut borrow and would conflict).
        let append_err = if is_errored_or_recovering {
            None
        } else {
            self.read_buf.append(bytes).err()
        };

        // DEF-154 (E) + (H): field-level destructure. Closures cannot
        // see disjoint field borrows through `self`; splitting into
        // separate `&mut` bindings gives each consumer a single-field
        // borrow. `state` + `pending_advance` + `read_buf` are held
        // DISJOINTLY — the main dispatch loop takes a shared view of
        // `populated` from `read_buf` while `state` is separately
        // `&mut` for state transitions and `pending_advance` is
        // separately `&mut` for the post-loop deferred-advance
        // record.
        let state = &mut self.state;
        let read_buf = &mut self.read_buf;
        let session_params = &mut self.session_params;
        let schema_arena = &mut self.schema_arena;
        let error_arena = &mut self.error_arena;
        let scram_state = &mut self.scram_state;
        let pending_advance = &mut self.pending_advance;

        // Fast-path: already-Errored or pending_advance crate-bug
        // recovery. Clear read_buf to reset to consistent state;
        // return empty action list (or crate-bug classified if
        // pending_advance failed).
        if pending_advance_err {
            read_buf.clear();
            return write_buf.with_branded(|wb| -> OutActions<'w, 'r> {
                let mut staged: StagedActions = StagedActions::new();
                fail_inflight_no_readbuf(
                    state,
                    ProtocolError::InternalCrateBug {
                        locus: crate::error::CrateBugLocus::ReadCursorAdvance,
                    },
                    &mut staged,
                );
                materialise(staged, wb.into_bytes(), schema_arena.as_reader())
            });
        }
        if matches!(state, ProtoState::Errored(_)) {
            read_buf.clear();
            return write_buf.with_branded(|wb| -> OutActions<'w, 'r> {
                let staged: StagedActions = StagedActions::new();
                materialise(staged, wb.into_bytes(), schema_arena.as_reader())
            });
        }

        // Fast-path: ReadBufFull. Clear read_buf + stage FailReply +
        // CloseSocket + transition state to Errored.
        if let Some(ReadBufFull {
            attempted,
            available,
        }) = append_err
        {
            read_buf.clear();
            return write_buf.with_branded(|wb| -> OutActions<'w, 'r> {
                let mut staged: StagedActions = StagedActions::new();
                fail_inflight_no_readbuf(
                    state,
                    ProtocolError::ReadBufferFull { attempted, available },
                    &mut staged,
                );
                materialise(staged, wb.into_bytes(), schema_arena.as_reader())
            });
        }

        // Main dispatch. Take shared borrow of populated + cursor
        // (both via immutable reborrow of read_buf's &mut).
        let populated: &'r [u8] = read_buf.populated();
        let cursor: u16 = read_buf.cursor_position_u16();

        write_buf.with_branded(|mut wb| -> OutActions<'w, 'r> {
            let mut staged: StagedActions = StagedActions::new();
            // DEF-154 (G): cursor math stays in u16 end-to-end,
            // bounded by `READ_BUF_CAP <= u16::MAX` const-assert in
            // buf.rs. No silent narrowing anywhere.
            let mut frames_consumed: u16 = 0_u16;
            // DEF-154 (X): dispatch-count budget for RowStream's
            // slow path. `feed_bytes` supplies `u16::MAX`
            // (unbounded); `feed_bytes_bounded` from RowStream
            // supplies `1` so silent-state-transition frames
            // (e.g. `RowDescription`) return control to the
            // fast-path loop after exactly one frame.
            let mut dispatches_this_call: u16 = 0_u16;

            // Dispatch loop block: `reserved` holds `&mut wb.buf`
            // which must release before `wb.into_bytes()`
            // post-loop. NLL ends `reserved`'s borrow at the `}`.
            {
            let mut reserved = wb.reserve();
            loop {
                // DEF-154 (X): frame-budget gate. Transparent-skip
                // frames (ParameterStatus / NoticeResponse) do NOT
                // count — they're noise. Only
                // AdvancedSilent / AdvancedWithAction / Errored
                // increment `dispatches_this_call`.
                //
                // DEF-184 (B6): `const BOUNDED: bool` specialisation
                // — in the `BOUNDED=false` monomorphisation the
                // short-circuit `BOUNDED &&` evaluates at compile
                // time; LLVM eliminates the entire gate. Production
                // `feed_bytes` no longer pays the per-iter check.
                if BOUNDED && dispatches_this_call >= max_dispatches {
                    break;
                }
                // Logical-cursor peek into unread: skip already-
                // dispatched prefix. `frames_consumed` is
                // addend-only; each increment is gated on
                // `after_consumed.len() >= total_len` so the
                // subsequent slice is always in bounds.
                let absolute_start = cursor.saturating_add(frames_consumed);
                let after_consumed = populated
                    .get(usize::from(absolute_start)..)
                    .unwrap_or(&[]);

                let header = parse_header(after_consumed);
                match header {
                    HeaderParse::Empty | HeaderParse::Incomplete => break,
                    HeaderParse::MalformedLength { declared } => {
                        fail_inflight_no_readbuf(
                            state,
                            ProtocolError::MalformedFrameLength { declared },
                            &mut staged,
                        );
                        break;
                    }
                    HeaderParse::FrameTooLarge { declared } => {
                        fail_inflight_no_readbuf(
                            state,
                            ProtocolError::FrameTooLarge { declared },
                            &mut staged,
                        );
                        break;
                    }
                    HeaderParse::Ok { tag, total_len } => {
                        // total_len: u16 (DEF-154 (G)), bounded
                        // `5..=READ_BUF_CAP` by parse_header.
                        let total_len_usize = usize::from(total_len);
                        if after_consumed.len() < total_len_usize {
                            break;
                        }
                        // DEF-182 site 1 (payload extraction):
                        // length-arith invariant — parse_header Ok
                        // ⇒ total_len >= HEADER_LEN; the len-check
                        // above ensures total_len <= after_consumed
                        // .len(). Classified as tier-2 structural
                        // shield (architecturally dead None).
                        let payload_opt =
                            after_consumed.get(HEADER_LEN..total_len_usize);
                        debug_assert!(
                            payload_opt.is_some(),
                            "DEF-182: payload slice .get(HEADER_LEN..total_len) None",
                        );
                        let payload = payload_opt.unwrap_or(&[]);

                        // Pre-dispatch filters.
                        if tag == crate::wire::TAG_PARAMETER_STATUS
                            && allows_unsolicited_param_status(state)
                        {
                            match record_param_status(session_params, payload) {
                                ParamStatusRecordOutcome::Processed
                                | ParamStatusRecordOutcome::MalformedPayload => {}
                            }
                            frames_consumed =
                                frames_consumed.saturating_add(total_len);
                            continue;
                        }
                        if tag == crate::wire::TAG_NOTICE_RESPONSE {
                            frames_consumed =
                                frames_consumed.saturating_add(total_len);
                            continue;
                        }

                        // DEF-154 (L): gate uses `MAX_STAGED_PER_CALL`
                        // (dispatch-side cap) — NOT `MAX_ACTIONS_PER_CALL`
                        // (output-side cap which is 2× larger for
                        // fanout). Pre-(L) both consts were 8 and
                        // aliased; post-(L) they differ and `staged`
                        // overflowing its own `heapless::Vec<_, MAX_STAGED_PER_CALL>`
                        // cap would panic in `emit_actions!`.
                        if staged
                            .len()
                            .saturating_add(WORST_CASE_PER_DISPATCH)
                            > MAX_STAGED_PER_CALL
                        {
                            break;
                        }

                        // DEF-184 (B21/C6): dispatch takes `&mut state`
                        // and writes transitions directly. No
                        // `mem::take` + `*state = new` round-trip —
                        // one mutable borrow, one in-place store.
                        let mut arena_writer = schema_arena.as_writer();
                        let outcome = dispatch(
                            state,
                            tag,
                            payload,
                            &mut reserved,
                            &mut arena_writer,
                            error_arena,
                            scram_state,
                        );
                        match outcome {
                            DispatchOutcome::AdvancedSilent => {
                                // State already written by dispatch arm.
                                frames_consumed =
                                    frames_consumed.saturating_add(total_len);
                                dispatches_this_call =
                                    dispatches_this_call.saturating_add(1);
                            }
                            DispatchOutcome::AdvancedWithAction { action } => {
                                // State already written by dispatch arm.
                                frames_consumed =
                                    frames_consumed.saturating_add(total_len);
                                dispatches_this_call =
                                    dispatches_this_call.saturating_add(1);
                                // DEF-154 (Q) P1-6: use default
                                // infallible emit (budget: 1, no
                                // on_overflow) — the dispatch gate
                                // above reserves WORST_CASE_PER_DISPATCH
                                // = 2 slots before entry, so
                                // staged.len() + 1 ≤ MAX_STAGED_PER_CALL
                                // is guaranteed. Pre-(Q) the
                                // `on_overflow: break` form was
                                // architecturally dead but had a
                                // silent-loss footgun if the gate ever
                                // drifted.
                                emit_actions!(&mut staged, budget: 1, [
                                    action,
                                ]);
                            }
                            DispatchOutcome::Errored { reply_id, cause } => {
                                // State already written to
                                // `ProtoState::Errored(_)` by the
                                // install_errored helper inside dispatch.
                                // DEF-154 (Q) P1-6: terminal
                                // FailReply + CloseSocket MUST reach
                                // the caller (reply promise
                                // resolution, socket teardown signal).
                                // Pre-(Q) the `on_overflow: break`
                                // form could silently drop these if
                                // staged was near-full — state is
                                // Errored but caller sees no
                                // FailReply, no CloseSocket, orphaned
                                // oneshot receiver. Post-(Q) infallible
                                // emit: dispatch gate reserves 2 slots,
                                // push always fits.
                                match reply_id {
                                    Some(id) => {
                                        emit_actions!(&mut staged, budget: 2, [
                                            StagedAction::FailReply { id, cause },
                                            StagedAction::CloseSocket,
                                        ]);
                                    }
                                    None => {
                                        emit_actions!(&mut staged, budget: 1, [
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
            } // end of reserved scope

            // DEF-154 (H) P0-1 preservation: record pending_advance
            // INSTEAD of applying it in-scope. Staged StreamRowRange
            // holds `&'r [u8]` slices of populated — we cannot &mut
            // read_buf while they're alive. Next feed_bytes call
            // applies the advance at entry before any new dispatch.
            //
            // If state transitioned to Errored this call, DON'T
            // record pending_advance — next call's is_errored
            // fast-path will CLEAR the buffer anyway (pending advance
            // becomes moot).
            //
            // DEF-154 (V): `pending_advance: Option<NonZeroU16>` —
            // None means "no pending" (type-enforced, cannot
            // accidentally assign 0). `NonZeroU16::new(frames_consumed)`
            // returns None if frames_consumed == 0, naturally
            // skipping the `frames_consumed > 0` condition.
            if !matches!(state, ProtoState::Errored(_)) {
                *pending_advance = core::num::NonZeroU16::new(frames_consumed);
            }

            materialise(staged, wb.into_bytes(), schema_arena.as_reader())
        })
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
            // DEF-184 (A1+A13): error_arena mirrors schema_arena
            // lifecycle — cleared at entry-point boundaries when
            // state is Idle/Errored. Any outstanding ErrorRef
            // issued from the previous cycle resolves to `None`
            // via generation mismatch post-clear (stale-ref
            // classification per error_arena.rs docs).
            self.error_arena.clear();
        }
    }

    /// DEF-184 (A1+A13): resolve an [`crate::error_arena::ErrorRef`]
    /// handle (carried by `ProtocolError::ServerErrorResponse
    /// .details_ref`) to the full `ErrorPayload` containing the
    /// server's message/detail/hint bounded strings.
    ///
    /// # Return value
    ///
    /// DEF-184 (audit #3 A-06): tier-3 classified `Result`:
    ///
    /// - `Ok(&ErrorPayload)` — ref resolves cleanly; generation
    ///   matches and slot is populated.
    /// - `Err(ArenaError::Stale)` — expected "consumed" signal.
    ///   The arena was cleared (at an entry-point boundary where
    ///   prior state was Idle/Errored) or a subsequent `alloc` bumped
    ///   the generation. The caller held the ref too long.
    /// - `Err(ArenaError::Empty)` — architecturally unreachable
    ///   outside of `unsafe`. `ErrorRef` construction is confined to
    ///   `ErrorArena::alloc` which always populates the slot; a
    ///   generation-match with empty slot indicates a crate bug.
    ///   Classified as Err rather than silently producing a
    ///   default payload (tier-4 banned per CREDO §5).
    ///
    /// # Usage pattern
    ///
    /// The `ErrorRef` is `Copy`, so callers receiving an
    /// `Action::FailReply { cause: ProtocolError::ServerErrorResponse
    /// { details_ref, .. }, .. }` can stash the ref, drop the
    /// `OutActions` (releasing the `&mut PgProtocol` borrow), then
    /// call `proto.get_server_error(r)` on the now-free protocol
    /// reference.
    ///
    /// ```text
    /// // Pattern (simplified):
    /// let err_ref = extract_from_out_actions(...);
    /// drop(out_actions);
    /// match proto.get_server_error(err_ref) {
    ///     Ok(payload) => log::warn!("server error: {}", payload.message.as_str()),
    ///     Err(bsql_pg_proto::ArenaError::Stale) => {
    ///         // Expected if resolution deferred past clear_arena boundary.
    ///     }
    ///     Err(bsql_pg_proto::ArenaError::Empty) => {
    ///         // Architecturally unreachable — crate bug if seen.
    ///     }
    /// }
    /// ```
    #[inline]
    pub fn get_server_error(
        &self,
        r: crate::error_arena::ErrorRef,
    ) -> Result<&crate::error_arena::ErrorPayload, crate::error_arena::ArenaError> {
        self.error_arena.get(r)
    }

    // DEF-184 (A10/B22 audit P1-3): test-only forge hooks live in
    // `pub(crate)` methods below. They're gated by `#[cfg(test)]` at
    // the crate level AND pub(crate) so integration tests in `tests/`
    // cannot see them; only lib-internal unit tests in `#[cfg(test)]
    // mod` blocks within `src/` can drive them. The drift-arm tests
    // live in `src/scram_state.rs` test module.

    /// DEF-184 (audit #3 A-02): Display adapter that resolves a
    /// [`crate::error::ProtocolError`] with `ServerErrorResponse`-arena
    /// strings inline.
    ///
    /// Pre-(A-02) the `Display` impl for
    /// `ProtocolError::ServerErrorResponse` rendered `"[details in
    /// ErrorArena]"` — the cascade-size win (288 B → 8 B) regressed
    /// operator UX because `Display` has no access to the arena.
    /// Post-(A-02) callers with a protocol handle wrap the error:
    ///
    /// ```text
    /// // log the error with full message/detail/hint:
    /// log::error!("{}", proto.display_error(&err));
    /// ```
    ///
    /// The adapter's `Display` impl resolves the ref and prints
    /// `"server error: ERROR (28P01) — <message>; detail: <detail>;
    /// hint: <hint>"`. Arena-miss cases (Stale / Empty) fall through
    /// to `"[arena ref unresolved: <ArenaError>]"` — explicit
    /// diagnostic trail rather than silent empty-string fallback.
    ///
    /// For other `ProtocolError` variants the adapter delegates to
    /// the built-in `Display` impl (no regression).
    #[inline]
    #[must_use]
    pub fn display_error<'a>(
        &'a self,
        err: &'a crate::error::ProtocolError,
    ) -> crate::error_arena::DisplayError<'a> {
        crate::error_arena::DisplayError::new(err, &self.error_arena)
    }

    // ═════════════════════════════════════════════════════════════
    // DEF-154 (X) P0-2(c): RowStream helpers
    // ═════════════════════════════════════════════════════════════
    //
    // Thin crate-internal accessors exposing read_buf / schema_arena
    // operations to the `row_stream` module without opening
    // field-level `pub(crate)` on the field directly. Each is a
    // single-line delegate — no logic added.

    /// DEF-154 (X): append bytes to read_buf; Err on overflow.
    #[inline]
    pub(crate) fn read_buf_append(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        self.read_buf.append(bytes)
    }

    /// DEF-154 (X): shared view of the populated read_buf region.
    #[inline]
    #[must_use]
    pub(crate) fn read_buf_populated(&self) -> &[u8] {
        self.read_buf.populated()
    }

    /// DEF-154 (X): current read cursor (u16 storage).
    #[inline]
    #[must_use]
    pub(crate) fn read_buf_cursor_u16(&self) -> u16 {
        self.read_buf.cursor_position_u16()
    }

    /// DEF-154 (X): advance the read cursor. Err architecturally
    /// dead on RowStream paths (frames gated by `parse_header`
    /// length-check before advance).
    #[inline]
    pub(crate) fn read_buf_advance(
        &mut self,
        n: usize,
    ) -> Result<(), crate::buf::AdvancePastEnd> {
        self.read_buf.advance(n)
    }

    /// DEF-154 (X): `ArenaReader<'_>` for slow-path
    /// `StagedReply::into_public` or direct `.get(schema_ref)`.
    #[inline]
    #[must_use]
    pub(crate) fn schema_arena_reader(&self) -> crate::schema_arena::ArenaReader<'_> {
        self.schema_arena.as_reader()
    }

    /// DEF-154 (X): if current state is a row-streaming variant,
    /// return `(reply_id, schema_ref)`. Otherwise `None`.
    ///
    /// Covers: `SimpleQueryStreamingRows`, `BindExecuteStreamingRows`,
    /// and `BindExecuteAwaitingDataOrCompleteSelect` (BindExecute's
    /// SELECT path can receive DataRow before explicit transition
    /// to StreamingRows).
    #[inline]
    #[must_use]
    pub(crate) fn streaming_reply_id_and_schema(
        &self,
    ) -> Option<(core::num::NonZeroU64, crate::schema_arena::SchemaRef)> {
        match &self.state {
            ProtoState::SimpleQueryStreamingRows { reply, schema_ref } => {
                Some((reply.get(), *schema_ref))
            }
            ProtoState::BindExecuteStreamingRows { reply, schema_ref } => {
                Some((reply.get(), *schema_ref))
            }
            ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, schema_ref } => {
                Some((reply.get(), *schema_ref))
            }
            _ => None,
        }
    }

    /// DEF-154 (X): apply any pending cursor advance from a prior
    /// feed_bytes call. RowStream calls this at `iter_rows()`
    /// construction to mirror `feed_bytes`'s entry semantics.
    ///
    /// DEF-184 (B24): Err was previously silently discarded via
    /// `let _result = ...` — a tier-4 fallback violating CREDO §1.
    /// `pending_advance` is recorded only from validated
    /// `frames_consumed` sums (bounded by parse_header-checked
    /// total_len additions), so Err is architecturally dead. But
    /// "dead by audit" is tier-3 trust; elevating to tier-2
    /// structural via explicit Errored classification closes the
    /// drift surface at zero runtime cost (branch is cold-path
    /// unreachable in practice, but now auditable).
    #[inline]
    pub(crate) fn apply_pending_advance(&mut self) {
        if let Some(n) = self.pending_advance {
            if self.read_buf.advance(usize::from(n.get())).is_err() {
                // Architecturally dead; drift-surface closure via
                // classification. Matches the feed_bytes entry
                // pattern in protocol.rs:~626 (pending_advance_err
                // flag → InternalCrateBug emission).
                let cause = ProtocolError::InternalCrateBug {
                    locus: crate::error::CrateBugLocus::ReadCursorAdvance,
                };
                self.state = ProtoState::Errored(cause.state_kind());
            }
            self.pending_advance = None;
        }
    }

    /// DEF-154 (X): whether the state is currently Errored (for
    /// RowStream fast-path state check).
    #[inline]
    #[must_use]
    pub(crate) fn state_is_errored(&self) -> bool {
        matches!(self.state, ProtoState::Errored(_))
    }

    /// DEF-184 (B25): transition to `Errored(Internal)` for a
    /// dead-branch read_buf advance Err. Used by RowStream's
    /// fast-path when `read_buf_advance(total)` returns Err
    /// — architecturally impossible (total pre-validated) but
    /// tier-2 classification closes the drift surface at zero
    /// runtime cost (branch is cold-path unreachable in practice).
    #[inline]
    pub(crate) fn install_errored_read_cursor_advance(&mut self) {
        let cause = ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::ReadCursorAdvance,
        };
        self.state = ProtoState::Errored(cause.state_kind());
    }

    /// DEF-154 (X): transition to `Errored(Framing)` for a
    /// malformed DataRow (empty body, server-side desync). Used
    /// by RowStream's fast-path when `start == end`.
    #[inline]
    pub(crate) fn install_errored_malformed_data_row(&mut self) {
        let cause = ProtocolError::MalformedDataRow { total_len: 0 };
        let state_kind = cause.state_kind();
        self.state = ProtoState::Errored(state_kind);
    }

    /// DEF-154 (X) P0-2(c): construct a pull-based row stream
    /// over this protocol + a caller-owned write buffer.
    ///
    /// Returns a [`crate::row_stream::RowStream`] that the caller
    /// feeds inbound TCP bytes via `.feed()` and pulls events via
    /// `.next_event()`. Fast-paths the DataRow frame (zero
    /// `OutActions` allocation per row on the SELECT hot path);
    /// slow-path (non-DataRow frames) delegates to `feed_bytes`.
    ///
    /// See `row_stream` module docs for perf rationale + API.
    ///
    /// The returned stream holds `&mut self` + `&mut write_buf`;
    /// both refs are blocked from other uses until the stream
    /// drops.
    #[inline]
    pub fn iter_rows<'p, 'w>(
        &'p mut self,
        write_buf: &'w mut WriteBuf,
    ) -> crate::row_stream::RowStream<'p, 'w> {
        // Entry-point housekeeping mirrors feed_bytes:
        write_buf.clear();
        self.clear_arena_if_idle_or_errored();
        self.apply_pending_advance();
        crate::row_stream::RowStream::new(self, write_buf)
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-154 (E) — field-level free functions for the fail path
// ═════════════════════════════════════════════════════════════════════
//
// `fail_inflight_and_close` + `replace_state_errored_and_drain` +
// `fail_read_cursor_advance` historically took `&mut self` (full
// PgProtocol). DEF-154 (E) wraps `feed_bytes`'s dispatch loop in a
// `self.read_buf.with_branded(|mut rb| { ... })` branded scope —
// inside which `read_buf` is borrowed via `rb` (mut via
// BrandedReadBuf::advance_scope_local / clear_scope_local; shared
// otherwise). `&mut self` calls are incompatible with `rb`'s borrow.
//
// Fix: free-function form taking disjoint field refs
// (`&mut ProtoState`, `&mut ReadBuf`, `&mut StagedActions`).
// Callers can destructure `self` at the dispatch-scope entry and
// thread the disjoint refs down. Instance methods below delegate
// to these for non-branded call sites.

/// DEF-154 (E) — field-level fail helper used inside the branded
/// read scope.
///
/// Takes `&mut ProtoState` + `&mut StagedActions` only — DOES NOT
/// take `&mut ReadBuf`, because inside `self.read_buf.with_branded`
/// the read_buf is held by `rb` and cannot be separately
/// reborrowed. Callers inside the branded scope clear read_buf via
/// `rb.clear_scope_local()` at an appropriate post-mutation point.
///
/// DEF-149 atomic-terminus triple (state install + reply drain +
/// read_buf clear) is preserved by:
/// 1. This fn installs `ProtoState::Errored(kind)` and drains the
///    inflight reply atomically (state-replace is one operation).
/// 2. Caller immediately arranges `rb.clear_scope_local()`
///    (inline or post-loop via a flag).
/// 3. `PgProtocol` is `!Sync` — no concurrent observer can witness
///    the partial triple.
#[cold]
fn fail_inflight_no_readbuf(
    state: &mut ProtoState,
    cause: ProtocolError,
    staged: &mut StagedActions,
) {
    if matches!(state, ProtoState::Errored(_)) {
        return;
    }
    // DEF-154 (I): total state_kind — no unwrap_or_else + debug_assert.
    let state_kind = cause.state_kind();
    let prev = core::mem::replace(state, ProtoState::Errored(state_kind));
    let raw_id = prev.take_inflight_reply_raw_id();
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
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    scram_state: &mut Option<crate::scram_state::ScramHandshakeState>,
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
            reserved,
            scram_state,
        ),
        PgCommand::SimpleQuery { sql, reply } => {
            compute_push_simple_query(state, &sql, reply, &mut staged, reserved)
        }
        PgCommand::Parse {
            stmt_name,
            sql,
            reply,
        } => compute_push_parse(state, &stmt_name, &sql, reply, &mut staged, reserved),
        PgCommand::DescribeStatement { stmt_name, reply } => {
            compute_push_describe_statement(state, &stmt_name, reply, &mut staged, reserved)
        }
        PgCommand::DescribePortal { portal_name, reply } => {
            compute_push_describe_portal(state, &portal_name, reply, &mut staged, reserved)
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
#[expect(clippy::too_many_arguments, reason = "compute_push_startup is an internal helper for Pg startup-command dispatch; its arg count matches the `PgCommand::Startup` payload + write_buf + staged accumulator + scram_state slot. Splitting into a struct-arg would obscure the pure-compute framing (DEF-059).")]
fn compute_push_startup(
    state: ProtoState,
    user: Ident,
    database: Option<DatabaseName>,
    app_name: Option<ApplicationName>,
    credentials: Credentials,
    reply: ReplyId<crate::reply_id::StartupKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    scram_state: &mut Option<crate::scram_state::ScramHandshakeState>,
) -> ProtoState {
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-154 (B) P0-2: builder returns Result; Err →
            // FailReply + CloseSocket + Errored via `try_builder!`.
            let range = try_builder!(
                build_startup_message(
                    &user,
                    database.as_ref(),
                    app_name.as_ref(),
                    reserved,
                ),
                reply,
                staged
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
                    // DEF-184 (A10/B22): heavy SCRAM session moves off
                    // state. Pair: thin state variant + scram_state
                    // holding ScramSession. Set atomically.
                    let session = crate::scram::session::ScramSession::from_password(password);
                    *scram_state = Some(crate::scram_state::ScramHandshakeState::Session(session));
                    ProtoState::ConnectingStartupScram { reply }
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
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> ProtoState {
    // DEF-146: single-level classifier dispatch (standard pattern —
    // Ping + BusyQuery → CommandInProgress, Connecting →
    // StartupAlreadyInProgress).
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-154 (B) P0-2: builder returns Result.
            let range = try_builder!(build_query_message(sql, reserved), reply, staged);
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

// DEF-154 (B) Phase B4: `from_write_span_infallible` deleted.
// Branded builders now use
// [`crate::action::WriteRange::from_write_span`] directly —
// identical shield logic, plus brand-identity binding.

/// Build a PostgreSQL simple-query frame: `'Q'` + 4-byte length +
/// NUL-terminated SQL.
///
/// PG frame body layout (§55.7 "Simple Query"):
/// - Tag: `'Q'` (1 byte)
/// - Length: u32 BE including itself
/// - Query string: NUL-terminated
fn build_query_message(
    sql: &crate::ident::Sql,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_QUERY.byte())?;
    reserved.with_length_prefix(|w| {
        w.push_nul_terminated(sql.as_bytes())?;
        Ok(())
    })?;
    // DEF-154 (B) Phase B4-W P0-2: `from_branded_write_span` returns
    // `Result` post-audit. 'Q' frame is ≥ 6 bytes so Err is
    // architecturally dead; classified upstream as
    // `EmptyWriteRange` if ever triggered.
    crate::action::WriteRange::from_write_span(start, reserved)
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
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_PARSE.byte())?;
    reserved.with_length_prefix(|w| {
        w.push_nul_terminated(stmt_name.as_bytes())?;
        w.push_nul_terminated(sql.as_bytes())?;
        // n_param_types = 0; 1c-3b will widen to push actual OIDs here.
        w.push_i16_be(0)?;
        Ok(())
    })?;
    crate::action::WriteRange::from_write_span(start, reserved)
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
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> ProtoState {
    // DEF-146: classifier dispatch. DEF-154 (B) P0-2: builder returns Result.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            let range = try_builder!(build_parse_message(stmt_name, sql, reserved), reply, staged);
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
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_DESCRIBE.byte())?;
    reserved.with_length_prefix(|w| {
        w.push_u8(target.byte())?;
        w.push_nul_terminated(name.as_describe_name_bytes())?;
        Ok(())
    })?;
    crate::action::WriteRange::from_write_span(start, reserved)
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
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> ProtoState {
    // DEF-146: classifier dispatch (standard pattern).
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-154 (B) P0-2: builder returns Result.
            let range = try_builder!(
                build_describe_message(
                    crate::wire::DescribeTargetByte::Statement,
                    stmt_name,
                    reserved,
                ),
                reply,
                staged
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
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> ProtoState {
    // DEF-146: classifier dispatch (standard pattern).
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-154 (B) P0-2: builder returns Result.
            let range = try_builder!(
                build_describe_message(
                    crate::wire::DescribeTargetByte::Portal,
                    portal_name,
                    reserved,
                ),
                reply,
                staged
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
// DEF-184 (A1+A13): ProtocolError shrunk 312 → ~72 B post-
// ErrorArena externalisation; Err path below 128 B
// result_large_err threshold.
fn build_bind_message<P: crate::params::ParamsWriter>(
    portal_name: &crate::ident::PortalName,
    stmt_name: &crate::ident::StmtName,
    params: &P,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    // Builder fns all return Result post-B4-W P0-2+P0-3 fix.
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_BIND.byte())?;
    // DEF-154 (B4-W + P0-3 fix from architect audit):
    // `params.write_params` can return Err from a user-impl that
    // overflows its advertised budget OR from a drift between
    // MAX_PARAMS_DATA_TOTAL and MAX_OWNED_SEND_LEN.
    //
    // DEF-154 (M): push_* now returns Result<(), WriteBufFull>;
    // ? propagates through the closure's new
    // `-> Result<(), WriteBufFull>` return, and through this
    // builder's `Result<_, ProtocolError>` via `From<WriteBufFull>
    // for ProtocolError` → `BuilderCapacityOverflow`. The
    // params_err out-param handles the OTHER failure (user-impl
    // overflow) which is still classified as
    // `ParamsWriterOverflow`.
    let mut params_err: Option<ProtocolError> = None;
    reserved.with_length_prefix(|w| {
        w.push_nul_terminated(portal_name.as_bytes())?;
        w.push_nul_terminated(stmt_name.as_bytes())?;
        // DEF-184 (A14): compact format-code block.
        //
        // Pre-(184) sent `n_format_codes = P::COUNT` followed by
        // `P::COUNT × u16_be(1)` — for N=16 that's 34 bytes of
        // format codes + 2 bytes of count. Post-(184) uses the PG
        // §55.7 Bind spec's compact form: "The number of parameter
        // format codes can be zero (all default/text), or ONE
        // (specified code applied to all parameters), or equal the
        // actual number of parameters". For N ≥ 1 all params use
        // binary uniformly → send `n_format_codes = 1, [1]` = 4
        // bytes, independent of N. For N = 0 keep
        // `n_format_codes = 0` (text-default, irrelevant with no
        // params) — avoids server-side "1 format code but 0 params"
        // edge case some PG forks might log.
        //
        // Wire-size saving: N=2 → 2 B, N=3 → 4 B, ..., N=16 → 30 B.
        // Loop eliminated entirely (one push_bytes for N ≥ 1).
        if P::COUNT == 0 {
            w.push_u16_be(0)?;
        } else {
            // `[0, 1, 0, 1]` = u16_be(1) + u16_be(1) = n_format_codes=1
            // + format[0]=Binary. Bulk push: LLVM compiles to one
            // 32-bit store on aligned targets.
            w.push_bytes(&[0, 1, 0, 1])?;
        }
        w.push_u16_be(P::COUNT)?;
        // Escape hatch: ParamsWriter takes &mut WriteBuf (pub trait
        // predating the witness pattern). Brand identity preserved
        // by the enclosing BrandedWriteReserved.
        if params.write_params(w.as_write_buf_mut()).is_err() {
            params_err = Some(ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::ParamsWriterOverflow,
            });
        }
        // n_result_formats = 0 → server default (all text). 1c-3b
        // does not negotiate per-column result formats; the user
        // dispatches between text and binary decoders via the
        // `ColumnDesc::format_code` in the provided row_desc.
        w.push_u16_be(0)?;
        Ok(())
    })?;
    if let Some(err) = params_err {
        return Err(err);
    }
    crate::action::WriteRange::from_write_span(start, reserved)
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
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_EXECUTE.byte())?;
    reserved.with_length_prefix(|w| {
        w.push_nul_terminated(portal_name.as_bytes())?;
        w.push_i32_be(fetch.as_wire_i32())?;
        Ok(())
    })?;
    crate::action::WriteRange::from_write_span(start, reserved)
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
#[expect(clippy::too_many_arguments, reason = "compute_push_bind_execute is an internal helper; its arg count matches `push_bind_execute`'s parameter surface + the accumulator + reserved. Splitting into a struct-arg would obscure the pure-compute framing.")]
fn compute_push_bind_execute<P: crate::params::ParamsWriter>(
    state: ProtoState,
    portal_name: &crate::ident::PortalName,
    stmt_name: &crate::ident::StmtName,
    params: &P,
    schema_ref: Option<crate::schema_arena::SchemaRef>,
    fetch: crate::command::FetchRows,
    reply: ReplyId<crate::reply_id::QueryKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> ProtoState {
    // DEF-146: classifier dispatch (standard pattern).
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-154 (B): both Bind and Execute run against the
            // same BrandedWriteReserved. The brand ties both
            // ranges to the buffer they materialise against.
            // `build_execute_message` is infallible (no user-data
            // overflow path); `build_bind_message` returns Result
            // post-B4-W-P0-3 fix — ParamsWriter overflow (user
            // sealed trait) classifies as
            // `CrateBugLocus::ParamsWriterOverflow` and degrades
            // gracefully to FailReply + CloseSocket.
            // DEF-154 (B) P0-2+P0-3: both builders return Result;
            // Err → FailReply + CloseSocket + Errored via
            // `try_builder!`. bind covers ParamsWriterOverflow and
            // EmptyWriteRange; execute covers EmptyWriteRange only.
            let bind_range = try_builder!(
                build_bind_message(portal_name, stmt_name, params, reserved),
                reply,
                staged
            );
            let execute_range = try_builder!(
                build_execute_message(portal_name, fetch, reserved),
                reply,
                staged
            );
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
/// Payload format per PG §55.7: `key\0value\0` — two NUL-terminated
/// C-strings. `[T]::split_once` with a predicate is still unstable
/// (#112811); the `iter().position` idiom is the stable-library
/// equivalent.
///
/// DEF-184 (B17): `#[inline(always)]` — called in the pre-dispatch
/// filter of the main dispatch loop on every ParameterStatus frame;
/// inlining saves a call frame per frame.
///
/// DEF-184 fallback-hygiene catch: pre-(184) the `value_region
/// .strip_suffix(b"\0").unwrap_or(value_region)` silently accepted
/// payload missing the trailing NUL (wire-spec violation per
/// §55.7). CREDO §7 ось 12 — fallback как костыль: silently
/// tolerated malformed input, ParameterStatus with missing
/// trailing NUL recorded the value with potential trailing garbage.
/// Post-(184): explicit `strip_suffix` Result — missing NUL =
/// MalformedPayload, classified not silently absorbed.
#[inline(always)]
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
    let Some(value) = value_region.strip_suffix(b"\0") else {
        return ParamStatusRecordOutcome::MalformedPayload;
    };
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
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    // DEF-094: write in-place into the caller-owned `write_buf`.
    // DEF-100: return a typed non-empty range — length invariant.
    // DEF-154 (A): infallible via capacity witness.
    // DEF-154 (B): branded → `WriteRange` ties the range
    // to the same buffer `reserved` writes into, enabling infallible
    // apply at materialise time.
    let start = reserved.len();
    reserved.with_length_prefix(|w| {
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
        w.push_u8(0)?;
        Ok(())
    })?;
    crate::action::WriteRange::from_write_span(start, reserved)
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
    write_bytes: &'w [u8],
    arena: crate::schema_arena::ArenaReader<'r>,
) -> OutActions<'w, 'r> {
    // DEF-154 (L) P0-1 invariant: `staged.len() ≤ MAX_STAGED_PER_CALL`
    // (heapless::Vec cap); each staged entry fans out to ≤
    // MAX_FANOUT_PER_STAGED actions. `out.push(a)` below is
    // architecturally infallible via the module-level
    // `const _: () = assert!(MAX_ACTIONS_PER_CALL >= MAX_STAGED_PER_CALL
    // * MAX_FANOUT_PER_STAGED)`. The match-Err arms pre-(L) used
    // `.unwrap_or(())` — a silent-drop pattern the user explicitly
    // banned ("тихая эрозия"). Post-(L): explicit match on `push`
    // result with the Err arm a documented dead branch.
    let mut out = OutActions::new();
    for sa in staged {
        // DEF-154 (Y): `StagedAction::StreamRowRange` deleted —
        // DataRow flows via `iter_rows` fast-path (no staging).
        // `DeliverReply` is the only remaining variant with
        // fanout (stale-ref classified crate-bug).
        let a: Action<'w, 'r> = match sa {
            StagedAction::SendBytesRange(range) => {
                // DEF-154 (N) P0-4: `WriteRange::apply` returns
                // `Option<&[u8]>` post-(N) — None is architecturally
                // unreachable under intact brand/bounds invariants
                // (see action.rs::WriteRange::apply doc), but the
                // Option makes the invariant-break explicit and
                // classified HERE via `CloseSocket` emission instead
                // of the pre-(N) silent `unwrap_or(&[])` fallback
                // that shipped a zero-byte SendBytes to the wire.
                match range.apply(write_bytes) {
                    Some(slice) => Action::SendBytes(slice),
                    None => {
                        push_within_fanout_budget(&mut out, Action::CloseSocket);
                        continue;
                    }
                }
            }
            StagedAction::SendBytesStatic(s) => Action::SendBytes(s),
            // DEF-112 + DEF-119: `DeliverReplyEntry` carries a
            // lifetime-free `StagedReply`. Materialise resolves any
            // `SchemaRef` handles into `&'r RowDesc` borrows via the
            // arena, producing the public `Reply<'r>`. The entry was
            // constructed by the typed `action::deliver` path —
            // kind-payload pairing was enforced at dispatch time.
            StagedAction::DeliverReply(entry) => {
                // DEF-154 (J) P0-D: into_public returns
                // Err(StaleSchemaRef) on stale ref — classify as
                // crate-bug FailReply + CloseSocket (2-action
                // fanout, counted in MAX_FANOUT_PER_STAGED).
                let entry_id = entry.id();
                match entry.staged().into_public(arena) {
                    Ok(value) => Action::DeliverReply {
                        id: entry_id,
                        value,
                    },
                    Err(_stale) => {
                        push_within_fanout_budget(
                            &mut out,
                            Action::FailReply {
                                id: entry_id,
                                cause: ProtocolError::InternalCrateBug {
                                    locus: crate::error::CrateBugLocus::StaleSchemaRef,
                                },
                            },
                        );
                        push_within_fanout_budget(&mut out, Action::CloseSocket);
                        continue;
                    }
                }
            }
            StagedAction::FailReply { id, cause } => Action::FailReply { id, cause },
            StagedAction::CloseSocket => Action::CloseSocket,
        };
        push_within_fanout_budget(&mut out, a);
    }
    out
}

/// DEF-154 (L) P0-1 + DEF-184 (B3): push an action with
/// classified dead-arm.
///
/// ## Infallibility proof (post-DEF-184 A15)
///
/// `MAX_ACTIONS_PER_CALL = MAX_STAGED_PER_CALL +
/// MAX_FANOUT2_ENTRIES_PER_CALL × (MAX_FANOUT_PER_STAGED − 1)
/// = 8 + 1 × 1 = 9` (const-asserted at MAX_ACTIONS_PER_CALL).
///
/// Each staged entry contributes ≤ `MAX_FANOUT_PER_STAGED = 2`
/// calls to this helper (1-action variants: 1; DeliverReply
/// stale-ref fanout: 2). With `MAX_FANOUT2_ENTRIES = 1` (at most
/// one DeliverReply per call, per pre-1c-5 single-inflight
/// invariant — see A15 proof), total calls ≤ 9 = out's capacity.
///
/// **Conclusion:** `out.push(a)` is architecturally infallible
/// in the post-A15 capacity regime. The Err arm is dead.
///
/// ## Why not truly tier-1 infallible?
///
/// True type-level infallibility (tier-1) would require either:
/// - `unsafe push_unchecked` in the push impl (forbidden by
///   crate-wide `#![forbid(unsafe_code)]`);
/// - const-generic capacity witness on `OutActions` that proves
///   `len + 1 ≤ MAX` at type level (not expressible in stable
///   Rust without `#![feature(generic_const_exprs)]`).
///
/// We settle for tier-2 structural via const-asserted invariant
/// plus classified dead-arm: the `debug_assert!(false, ...)` in
/// the Err branch fires in dev/test builds, release silently
/// accepts (safe because invariant proof holds structurally).
///
/// ## Why the wrapper vs inline match?
///
/// The function call is `#[inline(always)]` + const-folded in
/// release, so zero runtime overhead. Source-level wrapper
/// centralises the debug-assert pattern across 6 materialise
/// sites, avoiding drift (a future 7th site would inherit the
/// correct dead-arm discipline automatically).
#[inline(always)]
fn push_within_fanout_budget<'w, 'r>(
    out: &mut OutActions<'w, 'r>,
    a: Action<'w, 'r>,
) {
    match out.push(a) {
        Ok(()) => {}
        Err(_architecturally_dead) => {
            // DEF-184 (B3): elevate the pre-(184) silent empty arm
            // to a debug-classified dead-arm sentinel. Dev/test
            // panics LOUDLY if invariant ever breaks (a future
            // refactor drops MAX_FANOUT2_ENTRIES or adds a new
            // fanout-3 staged variant without updating const). In
            // release the silent no-op is the safe fallback — the
            // structural invariant guarantees this is unreachable.
            debug_assert!(
                false,
                "push_within_fanout_budget: OutActions overflow. \
                 Architecturally impossible per const-assert \
                 MAX_ACTIONS_PER_CALL >= MAX_STAGED + MAX_FANOUT2 \
                 × (MAX_FANOUT - 1) = 9 post-DEF-184 A15. \
                 If this fires, either a new fanout-2 StagedAction \
                 variant landed without bumping MAX_FANOUT2_ENTRIES, \
                 or 1c-5 pipelining introduced batched DeliverReply \
                 emissions. Update MAX_FANOUT2_ENTRIES_PER_CALL and \
                 MAX_ACTIONS_PER_CALL in lockstep.",
            );
        }
    }
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
    use crate::reply_id::ReplyId;
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
        // DEF-184 (A10/B22): SCRAM state variants are thin post-refactor.
        // Heavy data (ScramSession, client_first_bare, client_nonce_b64,
        // expected_server_sig) lives on PgProtocol::scram_state now.
        // These fixtures only need the thin state variant for the
        // `allows_unsolicited_param_status` classification test.
        let startup_scram = ProtoState::ConnectingStartupScram {
            reply: ReplyId::from_raw(nz(4001)),
        };
        assert!(!allows_unsolicited_param_status(&startup_scram));
        consume_state(startup_scram);

        let scram_first = ProtoState::ConnectingScramAwaitingServerFirst {
            reply: ReplyId::from_raw(nz(5)),
        };
        assert!(!allows_unsolicited_param_status(&scram_first));
        consume_state(scram_first);

        let scram_final = ProtoState::ConnectingScramAwaitingServerFinal {
            reply: ReplyId::from_raw(nz(6)),
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
    use crate::password::Credentials;
    use crate::reply_id::ReplyId;
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

    /// Test-only observation of a [`StagedAction`] — brand stripped,
    /// range carried as `NonEmptyRange`. Tests compare against this
    /// instead of `StagedAction` directly because `'wb` is
    /// HRTB-fresh per call site (DEF-154 (B)) and cannot be named
    /// outside the branded closure that produced it.
    ///
    /// `ProtocolError` is `Copy + Clone` (error.rs:231) so
    /// `FailReply`'s full cause variant is preserved — tests match
    /// specific causes like
    /// `cause: ProtocolError::ConnectionAlreadyClosed { prior_kind }`.
    ///
    /// Variants covered are those `compute_push` produces.
    /// `StreamRowRange` (only from `feed_bytes` DATA_ROW arm) is
    /// represented as a distinct `StreamRowRangeUnexpected` variant
    /// — if a future refactor ever makes compute_push emit a
    /// StreamRowRange (an architectural bug), tests pattern-matching
    /// on `StagedObs` will SEE a distinct variant instead of a
    /// silent collapse to `CloseSocket` (pre-DEF-154 (P) behaviour
    /// flagged as P0-6 by architect audit).
    // DEF-184 (A1+A13): ProtocolError ~72 B post-ErrorArena; no
    // longer triggers large_enum_variant.
    #[derive(Debug, Clone, Copy)]
    enum StagedObs {
        /// Unit variant — tests discriminate on variant kind, not
        /// range contents. The underlying `NonEmptyRange` is
        /// available via dispatch tests that exercise feed_bytes
        /// separately; compute_push_tests only verify variant
        /// presence.
        SendBytesRange,
        SendBytesStatic(&'static [u8]),
        DeliverReply,
        FailReply {
            id: core::num::NonZeroU64,
            cause: crate::error::ProtocolError,
        },
        CloseSocket,
    }

    impl StagedObs {
        fn from_staged(sa: &StagedAction) -> Self {
            match sa {
                StagedAction::SendBytesRange(_) => Self::SendBytesRange,
                StagedAction::SendBytesStatic(s) => Self::SendBytesStatic(s),
                StagedAction::DeliverReply(_) => Self::DeliverReply,
                StagedAction::FailReply { id, cause } => {
                    Self::FailReply { id: *id, cause: *cause }
                }
                StagedAction::CloseSocket => Self::CloseSocket,
            }
        }
    }

    /// Test helper — run [`compute_push`] inside a branded scope,
    /// observe the staged actions as brand-free [`StagedObs`] items
    /// (returned as a heapless::Vec for asserting `.len()` and
    /// `.first()` / iteration). Returns `(new_state, obs_vec)`.
    fn compute_staged(
        cmd: PgCommand,
        state: ProtoState,
    ) -> (ProtoState, heapless::Vec<StagedObs, MAX_ACTIONS_PER_CALL>) {
        let mut wb = WriteBuf::new();
        let mut scram_state = None;
        wb.with_branded(|mut wb| {
            let mut reserved = wb.reserve();
            let (new_state, staged) = compute_push(cmd, state, &mut reserved, &mut scram_state);
            let mut obs: heapless::Vec<StagedObs, MAX_ACTIONS_PER_CALL> = heapless::Vec::new();
            for a in &staged {
                obs.push(StagedObs::from_staged(a)).unwrap_or_else(|_| {
                    debug_assert!(false, "MAX_ACTIONS_PER_CALL overflow in test");
                });
            }
            (new_state, obs)
        })
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
                Some(StagedObs::SendBytesStatic(s)) if *s == SYNC_WIRE_BYTES.as_slice()
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
                Some(StagedObs::FailReply {
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
                Some(StagedObs::FailReply {
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
                    Some(StagedObs::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ConnectingStartup → expected FailReply(StartupAlreadyInProgress)",
            );
            assert_eq!(take_connecting_startup_raw(new_state), Some(raw_prev));
        }

        // ConnectingStartupScram — post-(A10/B22) the state variant is
        // thin; ScramSession lives on PgProtocol::scram_state. Direct
        // state construction just needs the reply id.
        {
            let raw_prev = nz(201_050);
            let raw_new = nz(201_051);
            let prev = ProtoState::ConnectingStartupScram {
                reply: ReplyId::from_raw(raw_prev),
            };
            let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
            let (new_state, staged) = compute_staged(cmd, prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedObs::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ConnectingStartupScram → expected StartupAlreadyInProgress",
            );
            assert_eq!(take_connecting_startup_raw(new_state), Some(raw_prev));
        }

        // ConnectingScramAwaitingServerFirst — thin state variant post-A10.
        {
            let raw_prev = nz(203);
            let raw_new = nz(204);
            let prev = ProtoState::ConnectingScramAwaitingServerFirst {
                reply: ReplyId::from_raw(raw_prev),
            };
            let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
            let (new_state, staged) = compute_staged(cmd, prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedObs::FailReply {
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
            };
            let cmd = PgCommand::Ping { reply: ReplyId::from_raw(raw_new) };
            let (new_state, staged) = compute_staged(cmd, prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedObs::FailReply {
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
                    Some(StagedObs::FailReply {
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
                    Some(StagedObs::FailReply {
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
                    Some(StagedObs::FailReply {
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
            matches!(staged.first(), Some(StagedObs::SendBytesRange)),
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
                Some(StagedObs::FailReply {
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
                    Some(StagedObs::FailReply {
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
                    Some(StagedObs::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ConnectingStartupTrust → expected StartupAlreadyInProgress",
            );
            assert_eq!(take_connecting_startup_raw(new_state), Some(raw_prev));
        }

        // ConnectingStartupScram — post-(A10/B22) thin variant.
        if let Some(user) = mk_user() {
            let raw_prev = nz(405_100);
            let raw_new = nz(405_101);
            let prev = ProtoState::ConnectingStartupScram {
                reply: ReplyId::from_raw(raw_prev),
            };
            let (new_state, staged) = compute_staged(make_startup_cmd(user, raw_new), prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedObs::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ConnectingStartupScram → expected StartupAlreadyInProgress",
            );
            assert_eq!(take_connecting_startup_raw(new_state), Some(raw_prev));
        }

        // ConnectingScramAwaitingServerFirst — post-(A10/B22) thin variant.
        if let Some(user) = mk_user() {
            let raw_prev = nz(405);
            let raw_new = nz(406);
            let prev = ProtoState::ConnectingScramAwaitingServerFirst {
                reply: ReplyId::from_raw(raw_prev),
            };
            let (new_state, staged) = compute_staged(make_startup_cmd(user, raw_new), prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedObs::FailReply {
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
            };
            let (new_state, staged) = compute_staged(make_startup_cmd(user, raw_new), prev);
            assert_eq!(staged.len(), 1);
            assert!(
                matches!(
                    staged.first(),
                    Some(StagedObs::FailReply {
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
                    Some(StagedObs::FailReply {
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
                    Some(StagedObs::FailReply {
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
                    Some(StagedObs::FailReply {
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

    // ═════════════════════════════════════════════════════════════
    // DEF-154 (B) Phase B4-W P0-3 + P2 — ParamsWriterOverflow
    // classified-Err end-to-end routing test
    // ═════════════════════════════════════════════════════════════

    /// A user-space `ParamsWriter` that always returns
    /// `Err(WriteBufFull)` — simulating a buggy / adversarial impl
    /// whose `write_params` overflows its advertised budget.
    /// Exercises the classified-Err path: `build_bind_message` →
    /// `CrateBugLocus::ParamsWriterOverflow` →
    /// `try_builder!` macro → `FailReply + CloseSocket + Errored`.
    ///
    /// Pre-P0-3 the `Err` was silently discarded with
    /// `debug_assert!(false, …)`, shipping a truncated Bind frame
    /// with miscomputed length prefix in release — tier-4 silent
    /// wire-level corruption. This test pins the classified
    /// routing end-to-end: a failing ParamsWriter MUST produce
    /// `Action::FailReply` + `Action::CloseSocket`, NOT a broken
    /// Bind/Execute/Sync triplet.
    #[test]
    fn bind_execute_params_overflow_routes_to_classified_failreply() {
        use crate::action::Action;
        use crate::error::{CrateBugLocus, ProtocolError};
        use crate::params::OverflowParams;

        let mut proto = PgProtocol::new();
        let mut wb = WriteBuf::new();
        // Drive through a trivial startup so push_bind_execute
        // routes through the Idle-state arm (without a startup the
        // state is pre-handshake and Bind would fail-route as
        // "StartupAlreadyInProgress" — the wrong code path).
        //
        // PgProtocol::new() starts in ProtoState::Idle per
        // tests/bind_execute_spec.rs precedent (bind_execute_spec
        // line 126 calls push_bind_execute immediately after
        // PgProtocol::new() and hits the Idle arm). No handshake
        // drive needed.
        let reply_raw = nz(999);
        let out = proto.push_bind_execute(
            &crate::ident::PortalName::default(),
            &crate::ident::StmtName::default(),
            &OverflowParams,
            None, // No row_desc; DML-style path
            crate::FetchRows::All,
            ReplyId::from_raw(reply_raw),
            &mut wb,
        );

        // Classified Err routing expects exactly TWO actions:
        // FailReply + CloseSocket. Pre-P0-3 this would have been
        // THREE: Bind (truncated) + Execute + Sync.
        assert_eq!(
            out.len(),
            2,
            "ParamsWriter Err must route to FailReply + CloseSocket (2 actions), \
             NOT the 3-action Bind+Execute+Sync bundle. Pre-P0-3 silent \
             corruption would give the latter.",
        );

        // `matches!` with `if` guard — forbid-bundle bans
        // `assert!(false, …)` / `panic!` in tests, so pattern
        // matching is converted to a bool and asserted.
        let matches_expected = matches!(
            out.as_slice(),
            [
                Action::FailReply {
                    id,
                    cause: ProtocolError::InternalCrateBug {
                        locus: CrateBugLocus::ParamsWriterOverflow,
                    },
                },
                Action::CloseSocket,
            ] if *id == reply_raw
        );
        assert!(
            matches_expected,
            "expected [FailReply(reply_raw, ParamsWriterOverflow), CloseSocket]; \
             out = {:?}",
            out.as_slice(),
        );

        // State must have transitioned to Errored — the connection
        // is terminal per the usual InternalCrateBug discipline.
        assert!(
            matches!(proto.state(), ProtoState::Errored(_)),
            "ParamsWriterOverflow triggers terminal Errored state, \
             not a recoverable preserved-state path. Got: {:?}",
            proto.state(),
        );
    }
}
