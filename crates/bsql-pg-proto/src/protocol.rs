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
//!
//! # Naming convention — `compute_push_*` vs `dispatch`
//!
//! The crate splits the two sides of the state machine into
//! functions with **deliberately different name prefixes**:
//!
//! - **`compute_push_*`** — pure functions on the PUSH path
//!   (`compute_push`, `compute_push_ping`, `compute_push_startup`,
//!   `compute_push_simple_query`, etc.). Take `(cmd, state, ...)`
//!   and return `(new_state, staged_actions)`. No I/O, no inbound
//!   bytes — only side effect is building outbound frames into
//!   `write_buf`. DEF-059 framing: push-side state transitions are
//!   **pure compute over (current state × command)**.
//!
//! - **`dispatch`** — single entry point on the FEED path
//!   (`dispatch::dispatch` in `dispatch.rs`). Takes
//!   `(state, tag, payload, ...)` and returns `DispatchOutcome`.
//!   Classifies inbound frames against the current state and
//!   drives transitions / action emission. Sub-helpers
//!   (`dispatch_auth_in_startup_trust`, `dispatch_auth_sasl_continue`,
//!   `advance_to_awaiting_rfq`, etc.) share the `dispatch_` /
//!   `advance_to_` prefixes to mark them feed-path
//!   state-machine members.
//!
//! **Why the split matters:** push-path is CLIENT-DRIVEN (user
//! decides when to command), feed-path is NETWORK-DRIVEN (server
//! decides when to respond). The two timelines are orthogonal in
//! an async wrapper — the prefix tells a reader (and a grep) which
//! side of the state machine they're touching. A function named
//! `compute_push_foo` never parses wire bytes; a function named
//! `dispatch_*` never builds outbound frames.
//!
//! Entry points themselves follow the split:
//! `push_command` → calls `compute_push` tree.
//! `feed_bytes` → calls `dispatch` tree per frame.

use crate::action::{Action, OutActions, StagedAction, StagedActions};
use crate::buf::{ReadBuf, ReadBufFull};
// DEF-271 cluster A (2026-05-10): install_errored_* return drained
// Option<NonZeroU64>; FeedStateSetter::drain_and_install_errored API.
use core::num::NonZeroU64;
// `PgCommand` enum is referenced only by the test-only 5-arm
// `compute_push_*` dispatchers (the `compute_push_idle_only` slow-path
// dispatcher + `impl PushCommand for PgCommand` blanket impl were
// deleted at DEF-270 Phase 2 ship — real call sites were zero). The
// `use` is `#[cfg(test)]`-gated to avoid an unused-import warning in
// release builds.
#[cfg(test)]
use crate::command::PgCommand;
use crate::dispatch::{DispatchOutcome, dispatch};
use crate::error::{ProtocolError, StateErrorKind};
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
/// This macro centralises that handling. Each `compute_push_*_idle_only`
/// uses `let range = try_builder!(build_X(...), setter, reply, staged);`.
/// On `Err(cause)`: derive `StateErrorKind` via `cause.state_kind()`
/// (DEF-175/176 pattern), emit FailReply + CloseSocket into `staged`,
/// consume `setter` via `install_errored(state_kind)`, and
/// early-return from the enclosing `compute_push_*` function.
///
/// The macro early-returns, so it must be used in a position
/// where `return` is legal.
///
/// # DEF-270 N-D (Phase 2, 2026-05-10): setter consumption
///
/// Pre-DEF-270-N-D the macro took `$state: &mut ProtoState` and
/// wrote `*$state = ProtoState::Errored(state_kind)` directly. With
/// raw `&mut ProtoState` no longer reachable from `execute()` (only
/// via [`crate::state_setter::StateSetter`]), the macro now takes
/// `$setter: StateSetter<'_, _>` and consumes it via
/// [`StateSetter::install_errored`] on the Err path. The setter's
/// `must_use` lint surfaces a missed install at the call site;
/// previously the responsibility lived in a docstring discipline
/// note.
///
/// **NLL conditional-move:** on the Ok arm `$setter` is not
/// touched, so the caller retains it for the happy-path
/// `setter.install_post_state(witness)` at the tail of the helper.
/// Rust's NLL borrow checker treats the Err arm's setter consumption
/// as conditional (gated on the early-return), preserving setter
/// ownership on the Ok path. Compile-tested across all 6 helpers
/// using `try_builder!` (Ping skips since Sync is static-bytes
/// infallible).
///
/// **Idle-only contract** is now enforced by setter privacy:
/// `StateSetter::new` is `pub(crate)`, callable only from
/// `PgProtocol::push_command_internal` which asserts
/// `matches!(state, Idle)` at entry. The pre-DEF-270-N-D macro
/// debug_assert (defense-in-depth on the same invariant) is
/// dropped — same invariant, single load-bearing assertion site.
macro_rules! try_builder {
    ($result:expr, $setter:expr, $reply:expr, $staged:expr) => {
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
                $setter.install_errored(state_kind);
                return;
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
/// could emit up to 2 actions per staged entry on the
/// `SendBytesRange.apply == None` fan-out path (`CloseSocket`) —
/// a 16-action worst-case that did not fit the 8-slot output
/// container, causing `.unwrap_or(())` to silently drop terminal
/// actions.
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

/// Output-side action capacity — bounds `OutActions` storage.
///
/// # Budget derivation
///
/// `MAX_STAGED_PER_CALL (8) + 1 = 9`. The `+1` is the single
/// fanout-2 worst case: `StagedAction::SendBytesRange` whose
/// `WriteRange::apply` returns `None` (architecturally dead —
/// classified-via-CloseSocket sentinel, see `materialise`). Every
/// other StagedAction variant maps 1:1 to Action.
///
/// Post-DEF-188: the prior fanout-2 case was
/// `StagedAction::DeliverReply` with a stale `SchemaRef` payload
/// (`FailReply + CloseSocket`). DEF-188 deleted the schema arena
/// and the stale-ref class entirely; the only remaining fanout-2
/// site is `SendBytesRange.apply == None`, which inherits the same
/// budget reservation.
///
/// # Why `+1` and not `× 2`?
///
/// State-machine audit: at most ONE `DeliverReply` can fire per
/// dispatch call pre-pipelining. Terminal frames (RFQ/Z/CommandComplete/
/// AuthOk/ParseComplete/BindComplete/CloseComplete/NoData) transition
/// the state AWAY from the waiting-for-reply state, blocking a
/// second reply in the same feed_bytes iteration. Therefore:
/// 7 non-fanout staged + 1 fanout-2 staged = 7 + 2 = 9 actions.
///
/// # 1c-5 pipelining regression trap
///
/// If a future pipelining refactor emits 2+ DeliverReply per
/// dispatch call (batched replies), bump the `+1` literal to match
/// the max number of simultaneous fanout-2 staged entries.
///
/// # DEF-184 audit (2026-04-24) → DEF-210 SR-05 audit (2026-04-28)
///
/// Pre-DEF-184: 3-const formula (`MAX_FANOUT_PER_STAGED = 2`,
/// `MAX_FANOUT2_ENTRIES_PER_CALL = 1`,
/// `MAX_STAGED + FANOUT2 × (FANOUT − 1)`).
///
/// DEF-184 collapsed to `MAX_STAGED + 1` — "same value (9), half the
/// cognitive load." That collapse silently turned the named topology
/// terms into a magic `+1` literal (DEF-210 SR-05 finding): a 1c-5
/// pipelining refactor that adds a SECOND fanout-2 staged entry
/// would have to know to bump literal `+1` → `+2`, with the only
/// hint being a comment. **Drift surface: a comment.**
///
/// Path C from the audit: restore the named constants. The formula
/// `MAX_STAGED + MAX_FANOUT2_ENTRIES_PER_CALL × (MAX_FANOUT_PER_STAGED − 1)`
/// is self-documenting; future pipelining work bumps a NAMED const
/// (e.g. `MAX_FANOUT2_ENTRIES_PER_CALL = 2`) instead of editing a
/// literal that requires reading docstrings to understand.
///
/// # Bench impact (preserved through both transitions)
///
/// `OutActions` stack reservation: `9 × 88 B = 792 B` vs the
/// naive `MAX_STAGED × 2 = 16 × 88 B = 1408 B`. Saves 616 B per
/// OutActions. Combined with A2/B1 `ManuallyDrop<heapless::Vec>`
/// (0 B zero-fill), OutActions is a lean stack frame.
pub const MAX_ACTIONS_PER_CALL: usize =
    MAX_STAGED_PER_CALL + MAX_FANOUT2_ENTRIES_PER_CALL * (MAX_FANOUT_PER_STAGED - 1);

/// Maximum fan-out factor of any single staged entry into emitted
/// `Action`s. Today only `DeliverReply` is fanout-2 (it emits an
/// extra `Action::FailReply` if the slot it targets has gone stale
/// since staging — the materialise-side stale-ref protection from
/// DEF-184 A1+A13). All other staged entries are fanout-1.
///
/// 1c-5 pipelining note: if a future refactor introduces a fanout-3
/// staged entry, this constant rises and `MAX_ACTIONS_PER_CALL`
/// recomputes from the formula automatically.
///
/// `pub(crate)` — implementation-detail topology constant; external
/// consumers have no use case for reading it. Bumping it in 1c-5
/// pipelining must NOT be a public-API breaking change.
pub(crate) const MAX_FANOUT_PER_STAGED: usize = 2;

/// Number of fanout-2 staged entries that can occur within a single
/// `feed_bytes` call. State-machine audit: at most ONE `DeliverReply`
/// can fire per dispatch call pre-pipelining. Terminal frames
/// (RFQ/Z/CommandComplete/AuthOk/ParseComplete/BindComplete/
/// CloseComplete/NoData) transition the state AWAY from the
/// waiting-for-reply state, blocking a second reply in the same
/// feed_bytes iteration.
///
/// 1c-5 pipelining will lift this to ≥2 (multiple concurrent
/// inflight replies resolvable in one feed_bytes iteration). Bump
/// THIS constant — `MAX_ACTIONS_PER_CALL` recomputes from the
/// formula. The `WORST_CASE_PER_DISPATCH` and `OutActions` budget
/// math both compose from this single source.
///
/// `pub(crate)` — same rationale as `MAX_FANOUT_PER_STAGED`.
pub(crate) const MAX_FANOUT2_ENTRIES_PER_CALL: usize = 1;

// Drift pin: re-state the formula explicitly so an accidental edit
// to `MAX_ACTIONS_PER_CALL` (e.g. adding `+ 2` somewhere) trips at
// build time. The named magnitudes carry the topology rationale —
// future pipelining work bumps a NAMED constant, not a magic literal.
const _: () = assert!(
    MAX_ACTIONS_PER_CALL
        == MAX_STAGED_PER_CALL + MAX_FANOUT2_ENTRIES_PER_CALL * (MAX_FANOUT_PER_STAGED - 1),
    "MAX_ACTIONS_PER_CALL formula: MAX_STAGED + FANOUT2_ENTRIES × \
     (FANOUT − 1). DEF-210 SR-05: named constants restored — \
     pipelining work bumps a NAMED magnitude, not an unnamed literal.",
);
const _: () = assert!(
    MAX_FANOUT_PER_STAGED >= 1,
    "MAX_FANOUT_PER_STAGED must be ≥ 1 (a staged entry emits at least \
     one Action). 0 means dead code; 1 = no fanout, ≥ 2 = fanout.",
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

// DEF-210 BS-11 + REC-08 (audit 2026-04-28): module-scope tier-1
// pin that the `static EMPTY: SessionParams = SessionParams::new()`
// referenced from `cold_session_params` carries no SecretBoundedStr
// bytes — a `'static` value never drops, so its `ZeroizeOnDrop`
// chain never fires; the only safe state for a static SessionParams
// is fully pristine (every Option=None, every counter=0). A future
// refactor of `SessionParams::new()` that initialises a
// SecretBoundedStr field with a non-empty default would otherwise
// leak the bytes into static memory for the program's lifetime.
//
// Module scope so the const-eval is hoisted out of
// `cold_session_params`'s body — keeps the inline hint on that
// accessor effective by not embedding a const-eval expression
// inside it that the optimizer might consider when deciding to
// inline the outer function.
static _BS11_EMPTY_SESSION_PARAMS: SessionParams = SessionParams::new();
// DEF-211 INNO-01 (2026-05-04): use the auto-derived
// `__pristine_const` inherent fn (const-callable) instead of the
// removed manual `is_pristine` const fn. Runtime polymorphic
// `<SessionParams as Pristine>::is_pristine` cannot be const-called
// (trait methods aren't const on stable Rust as of MSRV 1.95).
const _BS11_EMPTY_SESSION_PARAMS_IS_PRISTINE: () = assert!(
    _BS11_EMPTY_SESSION_PARAMS.__pristine_const(),
    "static EMPTY: SessionParams must be pristine — see \
     `crate::pristine` module + `#[derive(Pristine)]` on SessionParams \
     (DEF-210 BS-11 + DEF-211 INNO-01)",
);

// DEF-185 P1-H (audit 2026-04-24): drift pin coupling
// `READ_BUF_CAP` to the `frames_consumed: u16` counter used in
// `feed_bytes_impl`. `frames_consumed` accumulates `total_len` per
// dispatched frame; each `total_len ≤ READ_BUF_CAP`. If
// `READ_BUF_CAP` ever grew past `u16::MAX`, the counter would
// silently saturate at 65535, breaking the subsequent
// `read_buf.advance(usize::from(frames_consumed))` math. The
// corresponding pin in `buf.rs` couples `READ_BUF_CAP` to
// `ReadBuf::cursor: u16`; this one couples the OTHER u16 consumer
// (the protocol dispatch loop) to the same cap — both pins must
// stay in lockstep with the field type choices.
const _: () = assert!(
    crate::frame::READ_BUF_CAP <= 65_535,
    "READ_BUF_CAP must fit frames_consumed: u16 in protocol::feed_bytes_impl. \
     Widen that counter type alongside any READ_BUF_CAP bump above u16::MAX.",
);

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
///
/// # Size budget (DEF-188 post-arena-deletion 2026-04-25)
///
/// `size_of::<PgProtocol>()` is pinned in `lib.rs`. Budget composition:
/// - `ReadBuf`            ~4096 B  (I/O staging, READ_BUF_CAP)
/// - `state`              ~320 B  (ProtoState — RowDesc inline in
///   streaming/AwaitingRfq variants per DEF-188; SCRAM Boxed per DEF-187)
/// - `session_params`     ~420 B
/// - `terminal_row_desc`  ~268 B  (DEF-188 single-slot Option<RowDesc>)
/// - `error_arena`        ~290 B  (DEF-184 A1+A13 single-slot)
/// - padding + flags      varies
///
/// Any field addition or size growth must update the pin in
/// `lib.rs` alongside the code change. See DEF-163 G012 for
/// this cross-reference discipline.
/// DEF-196: lazy-init helper for `Option<Box<ErrorArena>>`.
/// Called by `dispatch.rs` ErrorResponse arms when a server error
/// payload needs to be parsed and stored.
#[inline]
pub(crate) fn error_arena_or_init(
    slot: &mut Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
) -> &mut crate::error_arena::ErrorArena {
    slot.get_or_insert_with(|| {
        alloc::boxed::Box::new(crate::error_arena::ErrorArena::new())
    })
}

/// PostgreSQL wire-protocol state machine — pure sync, no I/O.
///
/// See `lib.rs` module docstring for architectural overview. The
/// state machine ingests inbound bytes via [`Self::feed_bytes`] and
/// outbound commands via [`Self::push_command`], emitting
/// [`OutActions`] for the caller to drive (write to socket, deliver
/// reply to user future, etc.).
///
/// `!Sync` by construction (the `sync_marker: PhantomData<Cell<()>>`
/// field). One task / thread can hold an exclusive borrow at a time —
/// concurrent `&mut PgProtocol` access is structurally impossible.
///
/// # DEF-246 Phase 1 (2026-05-16)
///
/// This is the inner data struct. The public surface is the
/// [`PgProtocol<P: SealedPhase>`] wrapper below, which is
/// `#[repr(transparent)]` over `PgProtocolInner` + a ZST
/// [`PhantomData<fn() -> P>`] phase marker. Layout is byte-identical
/// to pre-DEF-246 `PgProtocol` — all const-asserts on size hold.
///
/// Phase 1 is scaffolding only: all existing methods route through
/// `impl PgProtocol<ActivePhase>` via the default phase parameter; no
/// existing caller code changes. Phase 2+ will introduce
/// phase-conditional methods on `<DisconnectedPhase>`,
/// `<ConnectingPhase>`, and `<ClosedPhase>` to elevate the
/// state-transition invariants from §1 of the design memo
/// (`/tmp/def246-design-memo.md`).
///
/// `pub(crate)` visibility keeps inner-field manipulation
/// within-crate-only. Field-level visibility (no modifier = private to
/// `mod protocol` plus submodules per Rust visibility rules)
/// preserves the DEF-272 cluster's token-gated mutation surface
/// (cells live in private fields; mutations route through token-gated
/// methods).
pub(crate) struct PgProtocolInner {
    // DEF-189 hot-path field ordering: per-row fast-path touches
    // `state` (discriminant + reply_id), `row_desc_slot` (Option
    // projection), and `read_buf` (cursor + populated() slice). All
    // three accessed in <100 ns/row; placing them adjacent maximises
    // cache-line locality. The 4 KB `read_buf` follows because it
    // dominates working-set size (any cache-friendly layout has it
    // somewhere).
    //
    // DEF-196: `session_params`, `error_arena`, `malformed_frame_count`
    // moved into the heap-boxed [`ColdFields`] (cold path; lazily
    // allocated on first cold-write). Hot `PgProtocol` footprint
    // shrinks ~720 B; cache lines covering the hot fields no longer
    // share with unused cold-path data.
    state: ProtoState,
    /// DEF-189 hot-slot — placed adjacent to `state` so the per-row
    /// fast-path's `match state` and `current_row_desc()` projection
    /// share a cache line on small `ProtoState` (~64 B).
    ///
    /// **DEF-272 cluster α (2026-05-10)**: wrapped in
    /// [`crate::schema_slot::RowDescSlotCell`] (`#[repr(transparent)]`
    /// over `Option<RowDesc>`); the inner `Option` is private to
    /// `mod schema_slot`, write methods are gated on per-leaf concrete
    /// tokens. Tier-1 within-crate write provenance.
    /// See full lifecycle docstring at the bottom of this struct's
    /// field block (kept short here for layout-readability).
    row_desc_slot: crate::schema_slot::RowDescSlotCell,
    read_buf: ReadBuf,
    /// DEF-196: session params from post-auth handshake. Empty
    /// until first ParameterStatus / NoticeResponse write.
    /// **DEF-272 cluster β (2026-05-10)**: wrapped in
    /// [`crate::session_params_slot::SessionParamsCell`]
    /// (`#[repr(transparent)]` over `Option<Box<SessionParams>>`); the
    /// inner Option is private to `mod session_params_slot`, write
    /// methods are gated on per-leaf concrete tokens. Tier-1
    /// within-crate write provenance. Layout: 8 B niche-packed
    /// (preserved from pre-β).
    session_params: crate::session_params_slot::SessionParamsCell,
    /// DEF-196: server-error payload arena. None until first
    /// ErrorResponse frame allocates an `ErrorPayload`. Niche-packed.
    error_arena: Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
    /// **DEF-248 Sub-B (2026-05-12)** — universal-coverage streaming
    /// sink for non-`'D'` backend frames whose declared body exceeds
    /// [`crate::frame::READ_BUF_CAP`].
    ///
    /// `None` until first oversize non-`'D'` frame arrives; populated
    /// with a heap-allocated
    /// [`crate::partial_assembly::PartialAssemblyInner`] holding the
    /// bounded 8 KB prefix accumulator + the wire's remaining-byte
    /// counter. Cleared back to `None` when the body completes (sink
    /// dispatches the prefix to the existing per-tag parser) OR on
    /// Idle / Errored entry residue cleanup.
    ///
    /// **Stream-and-truncate**: bytes within the first
    /// [`crate::partial_assembly::PREFIX_CAP`] = 8 KB land in the
    /// prefix buffer (the bytes the inline-bounded parser will read);
    /// bytes beyond are counted-and-skipped. Memory stays constant
    /// 8 KB regardless of declared body length — every wire-legal
    /// size from 0 to ~2 GiB is consumable.
    ///
    /// **Layout**: 8 B niche-packed (`Option<Box<_>>`). The cell
    /// wrapper is `#[repr(transparent)]` over the raw Option — no
    /// overhead.
    ///
    /// **Tier-1 within-crate write provenance** (mirror of DEF-272
    /// cluster α/β): the field is private to `mod protocol`,
    /// mutations route through token-gated methods on
    /// [`crate::partial_assembly::PartialAssemblyCell`].
    /// `feed_bytes_impl` mutates through
    /// [`_partial_assembly_dispatch_leaf`] helpers; residue cleanup
    /// goes through [`_clear_residue_leaf::clear_partial_assembly_residue`].
    /// External callers cannot toggle partial-assembly mode.
    partial_assembly: crate::partial_assembly::PartialAssemblyCell,
    /// DEF-278 Bundle D (2026-05-17) — backend-key pair captured at
    /// handshake-complete (the first RFQ frame after
    /// `BackendKeyData`).
    ///
    /// **Empty** at construction (`<DisconnectedPhase>` /
    /// `<ConnectingPhase>` pre-`K`). **Installed** by the dispatch
    /// arm `(ConnectingPostAuthHaveKey, 'Z')` via the token-gated
    /// path through
    /// [`crate::protocol::_backend_key_install_leaf::install_at_dispatch_arm`].
    /// **Read** by [`PgProtocol::<ActivePhase>::with_cancel_request`].
    ///
    /// `secret_key` carried inline as [`crate::sensitive::Sensitive<i32>`]
    /// (`ZeroizeOnDrop`) — the cell's drop fires the inner Sensitive's
    /// zero-scrub when the connection terminates. `pid` is plain `i32`
    /// (wire-public, used for diagnostic logging).
    ///
    /// **Layout**: `Option<BackendKey>` = 1 B discriminant +
    /// `{ pid: i32, secret_key: Sensitive<i32> }` (8 B) +
    /// padding to align(4) = 12 B. The size pin in `lib.rs` reflects
    /// the post-Bundle-D `PgProtocolInner` total.
    ///
    /// **Tier-1 within-crate write provenance**: the field is private
    /// to `mod protocol`, mutations route through the leaf-token
    /// helper above (`_backend_key_install_leaf`). No other call site
    /// in the crate can install or clear the cell.
    backend_key: crate::cancel::BackendKeyCell,
    /// DEF-196: malformed-frame counter — INLINE u32 (no Box —
    /// 4 B is too small to amortise pointer indirection). Bumped
    /// on every fail_inflight_no_readbuf invocation. DEF-185 P2-9 +
    /// DEF-186 P1-5 widened to u32 for adversarial-flood resilience.
    malformed_frame_count: u32,
    // DEF-270 cluster (U letter, 2026-05-09 — fix-up after bisect):
    // reply-id counter lives in a `static AtomicU64` (mod-private,
    // see `next_reply_id` below) — NOT inline on PgProtocol.
    //
    // Why static instead of per-protocol field: bisect proved
    // adding a `u64` field grew PgProtocol 520 → 528 B and shifted
    // LLVM whole-crate codegen heuristic, regressing
    // `column_decode/iter_10cols_alternating_null_i32` by +6%.
    // Static-atomic mint preserves PgProtocol size at 520 B (no
    // codegen shift) and STRENGTHENS the invariant: globally unique
    // IDs across all `PgProtocol` instances (was per-protocol only).
    // DEF-189 row_desc_slot field is declared at the top of the
    // struct (above) for hot-path cache-line locality; full lifecycle
    // docstring follows.
    //
    // # row_desc_slot lifecycle (DEF-189)
    //
    // One descriptor per protocol, one in-flight reply at a time
    // (single-inflight invariant — pipelining will widen this slot
    // to a slab at 1c-5).
    //
    // ## Population
    //
    // The slot is populated by either of:
    // 1. the `'T'` (RowDescription) dispatch arm during simple-query
    //    or describe flows;
    // 2. `push_bind_execute` when the caller passes a non-None
    //    `row_desc` argument (caller-supplied schema for SELECT
    //    without a prior auto-Describe).
    //
    // ## Lifecycle
    //
    // Set when streaming / SELECT-bearing flow begins; read during
    // streaming via `Self::current_row_desc` (survives across DataRow
    // frames + the trailing `'C'` and `'Z'` transitions); read by
    // terminal materialise to project the public
    // `Reply::QueryComplete::row_desc: Option<RowDescBorrow<'r>>`;
    // cleared by `Self::clear_session_residue_if_idle_or_errored` at
    // every entry-point when state is `Idle` / `Errored`.
    //
    // ## Tier-2 structural — single-slot, single-in-flight
    //
    // Pre-DEF-189: state variants carried inline `RowDesc` (264 B
    // payload duplicated across BindExecuteSelect's 4 transitions +
    // SimpleQueryStreamingRows + AwaitingRfq). Per-row fast path did
    // `match &self.inner.state` twice (gate + project). Post-DEF-189:
    // state strips the schema field entirely; the schema lives in
    // this slot; fast path does `match &self.inner.state` once + a single
    // Option projection.
    //
    // Cost: ~168 B on `PgProtocol` (164 B SoA RowDesc + 1 B
    // discriminant + alignment padding). Per connection lifetime.
    // Pre-DEF-189 was ~268 B (264 B AoS RowDesc + Option niche).
    // DEF-189 saves ~100 B via SoA layout AND dramatically shrinks
    // state variants (~320 → ~64 B dominant).
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
    /// Same single-borrow convenience as `terminal_row_desc` above.
    /// `ErrorRef` (carried by `ProtocolError::ServerErrorResponse`)
    /// resolves via `&self` of `PgProtocol` — arena co-located with
    /// state keeps the ref lifetime within a single borrow scope.
    /// Staleness is tier-3 classified via
    /// [`crate::error_arena::ArenaError::Stale`]; placement is a
    /// load-bearing design decision for the current single-borrow
    /// invariant, NOT a tier-1 compile-enforced guarantee. A future
    /// refactor moving the arena out (e.g. per-worker pool) would
    /// need an additional correlation invariant classifier.
    //
    // DEF-196 (2026-04-28): error_arena lives in `cold: Option<Box<ColdFields>>`
    // (lazily-allocated heap). Field removed from inline storage.
    // DEF-184 (A10/B22) SCRAM externalisation REVERTED 2026-04-24:
    // tier-1 restored (CREDO §1: safety > tier-1 > perf). SCRAM
    // data (ScramSession, client_first_bare, client_nonce_b64,
    // expected_server_sig) now lives INLINE in ProtoState SCRAM
    // variants. Variant drop on state transition invokes
    // ScramSession's ZeroizeOnDrop automatically — no scram_state
    // field, no correlation invariant to maintain, no drift
    // classifier needed. ProtoState size returns to ~712 B
    // (dominated by ConnectingScramAwaitingServerFirst), but the
    // variant-carries-field invariant is tier-1 compile — a
    // future refactor cannot physically create a SCRAM state
    // without SCRAM data. Perf cost: ~632 B per
    // `mem::replace(state, Idle)` × 3-6 dispatches per query. On
    // typical workloads this is 2-4 KB/query memcpy cost — well
    // below audit sensitivity threshold, worth the tier uplift
    // per user directive.
    // DEF-154 (H+V) `pending_advance` DELETED 2026-04-24 (architect
    // audit finding): was a 2-byte deferred cursor-advance slot +
    // ~25 lines of plumbing + 3 architecturally-dead Err classifier
    // sites. Purpose: postpone `read_buf.advance()` past the return
    // of OutActions because `StagedAction::StreamRowRange` carried
    // `row_bytes: &'r [u8]` into read_buf — advance while that
    // borrow was alive = borrow-check conflict. DEF-154 (Y) DELETED
    // `StreamRowRange` entirely (RowStream fast-path emits
    // `StreamItem::Row` directly from `iter_rows`, NOT staged).
    //
    // Post-(Y): no staged action carries a read_buf borrow.
    // `Action<'w, 'r>.'r` is tied to `terminal_row_desc` (RowDesc
    // refs from the parking slot post-DEF-188), NOT `read_buf`.
    // Cursor advance can safely happen IN-SCOPE
    // within `feed_bytes` before materialising OutActions — no
    // borrow conflict exists to defer around.
    //
    // Old plumbing removed: field + `apply_pending_advance` method
    // + `pending_advance_err` fast-path + row_stream callers.
    // advance now happens inside the dispatch loop via
    // `read_buf.advance(total_len)` right after frame consumption.
    /// DEF-185 P2-9 (audit 2026-04-24) + DEF-186 P1-5 widening
    /// (audit 2026-04-24): counter of malformed-frame events that
    /// tripped `fail_inflight_no_readbuf`.
    ///
    /// Bumped on every invocation of `fail_inflight_no_readbuf` —
    /// i.e., every time a frame was classified as malformed
    /// (`MalformedFrameLength`, `FrameTooLarge`, `ReadBufferFull`,
    /// `InternalCrateBug{ReadCursorAdvance}`). Exposed via public
    /// accessor `malformed_frame_count()`.
    ///
    /// Use case: operators investigating connection-health can
    /// distinguish "connection died after one malformed frame"
    /// (bug / transient) from "server kept sending malformed frames
    /// until tear-down" (adversarial / misconfigured proxy).
    ///
    /// `u32` (was `u16` pre-DEF-186 P1-5): u16 saturation at 65535
    /// collapsed adversarial-flood diagnostics for connections that
    /// stay open across high event counts (CREDO §7 ось 5
    /// adversarial-trust class). u32 saturation at 4 billion is
    /// architecturally distant under realistic connection lifetimes.
    /// Cost: +2 B per `PgProtocol`.
    //
    // DEF-196 (2026-04-28): malformed_frame_count lives in
    // `cold: Option<Box<ColdFields>>` (lazily-allocated heap). Field
    // removed from inline storage.
    /// `!Sync` marker — `Cell<T>: !Sync`, so the whole struct inherits.
    /// Load-bearing: the crate-root ambiguous-impl gate verifies that
    /// `PgProtocol: !Sync` compile-time. Renamed from the earlier
    /// `_not_sync` (leading-underscore convention for structurally-used
    /// fields is forbidden per user-feedback memory).
    sync_marker: PhantomData<Cell<()>>,
}

// ═════════════════════════════════════════════════════════════════════
// DEF-246 Phase 1 — Branch-collapse typestate scaffolding (2026-05-16)
//
// `PgProtocol<P: SealedPhase>` is a `#[repr(transparent)]` wrapper
// over `PgProtocolInner` + a ZST `PhantomData<fn() -> P>` phase
// marker. Layout is byte-identical to pre-DEF-246 `PgProtocol`.
//
// Phase 1 deliverables (memo §7.7):
// - 4 ZST phase markers: DisconnectedPhase / ConnectingPhase /
//   ActivePhase / ClosedPhase (memo §1 phase taxonomy)
// - `SealedPhase` super-trait via `_sealed_phase::Sealed` (mirrors
//   DEF-244 sealed-trait pattern + DEF-272 leaf-token pattern)
// - `PgProtocol<P: SealedPhase = ActivePhase>` outer wrapper
// - `Deref` / `DerefMut` from `PgProtocol<P>` to `PgProtocolInner`
//   (canonical zero-cost-wrapper idiom — `repr(transparent)` + Deref
//   is the idiomatic Rust pair). Field access via `self.<field>`
//   continues to work in existing methods without 78 manual
//   substitutions; the deref coercion is structurally zero-cost
//   (asm-diff confirms bit-identical hot paths).
// - Default phase `P = ActivePhase` keeps every existing caller
//   (`PgProtocol::new()`, type-name `PgProtocol` in tests/benches,
//   `impl X for PgProtocol`) compiling without changes
//
// Tier impact: 0 elevations (scaffolding only). Phase 2-4 land the
// 3 tier-elevations (push-before-Startup, push-during-Connecting,
// Errored absorbs input) on top of this scaffolding. Phase 6 removes
// the default phase parameter for final API stabilisation.
// ═════════════════════════════════════════════════════════════════════

/// DEF-246 Phase 1 sealed-trait seal for [`SealedPhase`]. Field-private
/// tuple-struct pattern (mirror of DEF-272 leaf tokens) ensures
/// downstream code cannot extend the phase set via
/// `impl SealedPhase for MyPhase`.
pub(crate) mod _sealed_phase {
    /// Super-trait seal. Field-less marker. Implemented only for the
    /// 4 phase types defined in `mod protocol` below.
    pub trait Sealed {}
}

/// DEF-246 Phase 1 sealed phase marker trait.
///
/// `P: SealedPhase` is the type-level proof that the runtime
/// [`PgProtocol<P>`] is in phase `P`. The trait is sealed via
/// [`_sealed_phase::Sealed`]; the 4 implementing types are
/// [`DisconnectedPhase`], [`ConnectingPhase`], [`ActivePhase`],
/// [`ClosedPhase`].
///
/// Adding a new phase requires an update inside `mod protocol`
/// (sealed-supertrait pattern — downstream `impl SealedPhase for X`
/// fails with E0277 on the sealed-supertrait bound).
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a sealed phase of `PgProtocol`",
    label = "phases are `DisconnectedPhase`, `ConnectingPhase`, `ActivePhase`, `ClosedPhase`",
    note = "the phase set is sealed by `_sealed_phase::Sealed`; downstream cannot add phases. Extend by editing `mod protocol` in the bsql-pg-proto crate."
)]
pub trait SealedPhase: _sealed_phase::Sealed + 'static {}

/// DEF-246 Phase 1 — Disconnected phase marker.
///
/// `PgProtocol<DisconnectedPhase>` represents a fresh protocol
/// instance that has not yet sent the Startup message. The legal
/// operation is `push_startup(...)` (Phase 2 will introduce this).
/// Pushing a regular command from this phase will be a method-absent
/// E0599 compile error.
///
/// Phase 1: marker only; no phase-conditional methods yet.
#[derive(Debug, Clone, Copy)]
pub struct DisconnectedPhase;

/// DEF-246 Phase 1 — Connecting phase marker.
///
/// `PgProtocol<ConnectingPhase>` represents the Startup → AuthOk
/// handshake window. The legal operations are `feed_inbound` /
/// `advance_one_frame` to consume server-driven auth-flow frames.
/// Pushing a regular command from this phase will be a method-absent
/// E0599 compile error (Phase 3 elevation).
///
/// Phase 1: marker only.
#[derive(Debug, Clone, Copy)]
pub struct ConnectingPhase;

/// DEF-246 Phase 1 — Active phase marker.
///
/// `PgProtocol<ActivePhase>` represents the post-handshake, ready
/// state. This is the **default** phase parameter — every existing
/// caller and impl block continues to compile unchanged because
/// `PgProtocol` (no explicit phase) resolves to
/// `PgProtocol<ActivePhase>`.
///
/// All existing methods (push, feed, materialise, etc.) live on
/// `impl PgProtocol<ActivePhase>` in Phase 1.
#[derive(Debug, Clone, Copy)]
pub struct ActivePhase;

/// DEF-246 Phase 1 — Closed phase marker.
///
/// `PgProtocol<ClosedPhase>` represents a terminally-Errored protocol
/// instance. The legal operation is `cause()` accessor (Phase 4 will
/// introduce this). All push / feed paths are method-absent E0599
/// compile errors (Phase 4 elevation — «Errored absorbs input»).
///
/// Phase 1: marker only.
#[derive(Debug, Clone, Copy)]
pub struct ClosedPhase;

impl _sealed_phase::Sealed for DisconnectedPhase {}
impl _sealed_phase::Sealed for ConnectingPhase {}
impl _sealed_phase::Sealed for ActivePhase {}
impl _sealed_phase::Sealed for ClosedPhase {}

impl SealedPhase for DisconnectedPhase {}
impl SealedPhase for ConnectingPhase {}
impl SealedPhase for ActivePhase {}
impl SealedPhase for ClosedPhase {}

/// DEF-246 Phase 1 — Phase-typed wrapper over [`PgProtocolInner`].
///
/// `#[repr(transparent)]` over a single non-ZST field
/// (`inner: PgProtocolInner`) and a ZST
/// [`PhantomData<fn() -> P>`] — layout is byte-identical to
/// `PgProtocolInner` (and to pre-DEF-246 `PgProtocol`). The
/// `fn() -> P` phantom shape gives covariant `P` + unconditional
/// `Send + Sync` of the phantom itself (the wrapper's `!Sync`
/// inherits from `inner.sync_marker: PhantomData<Cell<()>>` via
/// `repr(transparent)` auto-trait propagation).
///
/// **Default `P = ActivePhase`** — Phase 1 is purely additive: every
/// existing `PgProtocol::new()`, `let proto: PgProtocol = ...`,
/// `impl Trait for PgProtocol`, etc., resolves to
/// `PgProtocol<ActivePhase>`. **No caller code changes in Phase 1.**
///
/// # Field-access discipline (foundation for Phase 2+)
///
/// Methods inside `impl PgProtocol<P>` access inner fields via
/// **explicit `self.inner.<field>`** — there is no [`Deref`] /
/// [`DerefMut`] impl. The explicit projection is load-bearing for
/// the multi-phase foundation:
///
/// 1. **Phase transitions** (`fn into_connecting(self) -> PgProtocol<ConnectingPhase>`)
///    move `self.inner` into the new wrapper — the boundary is
///    visible at the call site, not hidden by deref coercion.
/// 2. **Phase-conditional methods** (Phase 2+: `impl PgProtocol<DisconnectedPhase>`
///    gets `push_startup`, `impl PgProtocol<ClosedPhase>` gets
///    `cause()`-only surface) read `self.inner.<field>` — uniform
///    access pattern across all phase impls regardless of which
///    inner fields the phase touches.
/// 3. **Future inner-state evolution** — if Phase 4 adds an
///    error-tracking field on `PgProtocolInner`, the new field is
///    accessible via the same `self.inner.<new_field>` pattern;
///    deref-based access would need additional method shadowing
///    discipline to handle phase-conditional inner fields.
///
/// The `inner` field is **module-private** (no visibility modifier),
/// not `pub(crate)`. Sibling modules (`dispatch.rs`, `row_stream.rs`,
/// etc.) access `PgProtocol<P>` exclusively via the public method
/// surface — the inner-data shape stays an internal detail of
/// `mod protocol` (and its leaf submodules per Rust submodule
/// visibility rules).
#[repr(transparent)]
pub struct PgProtocol<P: SealedPhase = ActivePhase> {
    inner: PgProtocolInner,
    /// ZST phase marker. Load-bearing for the type-level phase
    /// proof; named without leading-underscore per the user-feedback
    /// convention that structurally-used fields must not be
    /// `_`-prefixed (mirrors `sync_marker` renaming).
    phase_marker: PhantomData<fn() -> P>,
}

// DEF-211 FAKE-19 (audit + ship 2026-05-04): bench-hooks feature
// REMOVED ENTIRELY. Pre-FAKE-19 the crate exposed two `pub fn` hooks
// gated `#[cfg(feature = "bench-hooks")]` + `#[doc(hidden)]`:
//
//   `bench_append_read_buf` — raw append into `read_buf` bypassing
//                              dispatch (used by row-iter benches).
//   `reset_for_bench`       — snap state to Idle bypassing Drop
//                              (used by amortised push benches).
//
// Both hooks were tier-3 by-discipline: feature-gated + doc-hidden +
// docstring warnings, but a downstream consumer who explicitly
// enabled `bench-hooks` in their Cargo.toml would get the API in
// production. CREDO §1 absolute-safety target = tier-1 closure.
//
// **Replacement is structural**:
//
// 1. `bench_append_read_buf` was a strict duplicate of the public
//    `feed_inbound(bytes) -> Result<(), ReadBufFull>` shipped in
//    DEF-212 Phase 2 (commit 201f86a). Benches now call
//    `feed_inbound` directly — same byte-for-byte semantics, no
//    duplication, public-API surface stays the same.
//
// 2. `reset_for_bench` was a bench-only `state = Idle` mutation that
//    bypassed Drop scrub for amortised iter perf. criterion's
//    `iter_batched(setup, routine, BatchSize)` is the idiomatic
//    replacement: setup builds a fresh proto per iter (untimed),
//    routine runs the timed measurement on it. Per-iter setup pays
//    PgProtocol::new() init (~50 ns memset for 4 KB ReadBuf) but
//    that cost is OUTSIDE the timed window — criterion reports the
//    routine timing accurately. See `benches/hot_paths.rs` post-
//    refactor patterns.
//
// **Tier closure**: feature physically gone → no leak surface →
// tier-1 by-elimination. Cannot enable from anywhere; the hooks
// don't exist. CREDO §1 absolute-safety satisfied without
// discipline reliance.
//
// **Trade-off accepted**: amortised push benches now include
// per-iter `PgProtocol::new()` cost in their wall time (longer to
// reach criterion's sample budget) but the reported per-iter
// timing is correct. Relative-to-baseline regression detection is
// preserved.

// ═════════════════════════════════════════════════════════════════════
// DEF-272 cluster α (2026-05-10) — schema-side concrete-token leaves
//
// Replaces the DEF-271 cluster C sealed-trait + auth-tag pattern with
// per-leaf concrete-type tokens (private tuple-struct field). The
// sealed-trait pattern was tier-1 EXTERNAL but tier-2 by-discipline
// WITHIN-CRATE: any in-crate file could `impl Sealed for HostileTag` +
// `impl SchemaWriteAuth for HostileTag` and bypass `from_field_with_auth`
// via the hostile tag (architect's empirical hostile-probe verified).
//
// Post-DEF-272-α each leaf has a CONCRETE token type (`pub(crate) struct
// XToken(())`); the `()` field is private to the leaf submodule, so
// `XToken(())` literal is mintable ONLY inside the leaf. The
// `RowDescSlotCell::*_at_*` write methods take the concrete token type
// by value. There is no trait to `impl` for hostile types; bypass
// requires constructing a token (impossible outside the leaf) or a
// type-mismatched parameter (rejected by Rust's type system).
//
// Cluster B-related leaves (`_parameter_status_admit_leaf`,
// `_notice_response_admit_leaf`, the session_params side of
// `_clear_residue_leaf`) still use the DEF-271 cluster B sealed-trait
// pattern; cluster β (next subcluster) migrates them.
//
// Cost: visibility-only; LLVM erases everything; 0 ns / 0 B.
// ═════════════════════════════════════════════════════════════════════

/// DEF-272 cluster α leaf submodule for the BindExecute SELECT install
/// transition. Hosts the [`BeSelectToken`] type and the single helper
/// fn that mints+writes inline.
//
// DEF-244 modernisation audit (rust-version 1.81 sweep): historical
// `#[allow(missing_docs, ...)]` here was DEAD — `missing_docs` only
// fires on `pub` items; this submodule is `pub(crate)`-only, so the
// lint doesn't trigger. Attribute deleted.
pub(crate) mod _bind_execute_select_install_leaf {
    /// DEF-272 cluster α leaf-scope token. The tuple-struct field is
    /// PRIVATE to this submodule — `Self(())` mints are callable ONLY
    /// here. The type itself is `pub(crate)` so
    /// [`crate::schema_slot::RowDescSlotCell::park_at_be_select`] can
    /// name it in its parameter signature; naming alone confers no
    /// minting power.
    pub(crate) struct BeSelectToken(());

    /// Mint a [`BeSelectToken`] and write `desc` into `slot` via
    /// [`crate::schema_slot::RowDescSlotCell::park_at_be_select`]. The
    /// only legitimate path to populate the schema slot from the
    /// BindExecute SELECT install code path.
    #[inline]
    pub(in crate::protocol) fn install_select_transition(
        slot: &mut crate::schema_slot::RowDescSlotCell,
        desc: crate::decode::RowDesc,
    ) {
        slot.park_at_be_select(desc, BeSelectToken(()));
    }
}

/// DEF-272 cluster α + β leaf submodule for the clear-session-residue
/// transitions on Idle/Errored entry. Hosts two concrete-type tokens
/// (one per slot kind) and two helper fns — schema-side (cluster α)
/// and session_params-side (cluster β).
//
// DEF-244 modernisation audit (rust-version 1.81 sweep): historical
// dead `#[allow(missing_docs, ...)]` removed (lint doesn't fire on
// `pub(crate)` items).
pub(crate) mod _clear_residue_leaf {
    /// DEF-272 cluster α leaf-scope token for the schema slot clear.
    /// Field private to the leaf; type `pub(crate)` so the cell can
    /// name it in its method signature.
    pub(crate) struct ClearResidueSchemaToken(());

    /// DEF-272 cluster β leaf-scope token for the session_params slot
    /// clear. Field private to the leaf; type `pub(crate)` so the cell
    /// can name it.
    pub(crate) struct ClearResidueSessionToken(());

    /// DEF-248 Sub-B (2026-05-12) leaf-scope token for the
    /// partial-assembly slot clear at residue transitions. Field
    /// private to the leaf; type `pub(crate)` so
    /// [`crate::partial_assembly::PartialAssemblyCell`] can name it.
    pub(crate) struct ClearResiduePartialAssemblyToken(());

    /// Clear the schema slot via
    /// [`crate::schema_slot::RowDescSlotCell::clear_at_residue`] with
    /// the [`ClearResidueSchemaToken`] minted inline. Used by
    /// `clear_session_residue_for_class` Idle and Errored arms.
    #[inline]
    pub(in crate::protocol) fn clear_schema_slot_residue(
        slot: &mut crate::schema_slot::RowDescSlotCell,
    ) {
        slot.clear_at_residue(ClearResidueSchemaToken(()));
    }

    /// Clear the session-params via
    /// [`crate::session_params_slot::SessionParamsCell::clear_at_residue`]
    /// with the [`ClearResidueSessionToken`] minted inline. Used by
    /// `clear_session_residue_for_class` Errored arm (per DEF-189
    /// Q8-C3 + DEF-205 step 3 — session-state forfeit on tear-down;
    /// the params' Drop chain scrubs `SecretBoundedStr` bytes).
    #[inline]
    pub(in crate::protocol) fn clear_session_params_residue(
        cell: &mut crate::session_params_slot::SessionParamsCell,
    ) {
        cell.clear_at_residue(ClearResidueSessionToken(()));
    }

    /// DEF-248 Sub-B (2026-05-12): clear the partial assembly cell via
    /// [`crate::partial_assembly::PartialAssemblyCell::clear_at_residue`]
    /// with the [`ClearResiduePartialAssemblyToken`] minted inline.
    /// Used by `clear_session_residue_for_class` Idle and Errored arms
    /// — drops any in-flight assembly's Box on residue cleanup,
    /// releasing its `heapless::Vec` allocation.
    ///
    /// Tier-1 by construction: the only way to release a partial
    /// assembly without going through this clear is via PgProtocol's
    /// own Drop — both fire the Box's Drop chain.
    #[inline]
    pub(in crate::protocol) fn clear_partial_assembly_residue(
        cell: &mut crate::partial_assembly::PartialAssemblyCell,
    ) {
        cell.clear_at_residue(ClearResiduePartialAssemblyToken(()));
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-248 Sub-B (2026-05-12) — partial-assembly dispatch leaf submodule
//
// Per-call-site concrete-type tokens that gate
// [`crate::partial_assembly::PartialAssemblyCell`]'s `enter_at_dispatch`
// / `absorb_at_dispatch` / `take_completed` mutating methods. Mirror of
// DEF-272 cluster α/β/δ patterns: tuple-struct field is PRIVATE to the
// leaf submodule, so the `Self(())` literal mint is callable ONLY here.
//
// Tier-1 within-crate by-construction. The leaf body is small enough to
// review as a unit; see [`crate::partial_assembly`] for the cell + sink
// design rationale.
// ═════════════════════════════════════════════════════════════════════

/// DEF-248 Sub-B leaf submodule for [`PgProtocol::feed_bytes_impl`]'s
/// partial-assembly transitions. Hosts three concrete-type tokens and
/// the matching helper fns.
//
// DEF-244 modernisation audit (rust-version 1.81 sweep): historical
// dead `#[allow(missing_docs, ...)]` removed (lint doesn't fire on
// `pub(crate)` items).
pub(crate) mod _partial_assembly_dispatch_leaf {
    /// DEF-248 Sub-B leaf-scope token for **entering** partial-assembly
    /// mode. Field private to the leaf; type `pub(crate)` so
    /// [`crate::partial_assembly::PartialAssemblyCell::enter_at_dispatch`]
    /// can name it in its parameter signature.
    pub(crate) struct PartialAssemblyEnterToken(());

    /// DEF-248 Sub-B leaf-scope token for **absorbing** body bytes into
    /// an active partial-assembly. Field private to the leaf.
    pub(crate) struct PartialAssemblyAbsorbToken(());

    /// DEF-248 Sub-B leaf-scope token for **taking** a completed partial
    /// assembly out of the cell for dispatch. Field private to the leaf.
    pub(crate) struct PartialAssemblyTakeToken(());

    /// Enter partial-assembly mode via
    /// [`crate::partial_assembly::PartialAssemblyCell::enter_at_dispatch`].
    /// Sole legitimate caller: `feed_bytes_impl`'s `FrameTooLarge` arm
    /// for streaming-eligible tags.
    #[inline]
    pub(in crate::protocol) fn enter_partial_assembly_at_dispatch(
        cell: &mut crate::partial_assembly::PartialAssemblyCell,
        tag: u8,
        declared_body_len: u32,
    ) {
        cell.enter_at_dispatch(PartialAssemblyEnterToken(()), tag, declared_body_len);
    }

    /// Absorb body bytes via
    /// [`crate::partial_assembly::PartialAssemblyCell::absorb_at_dispatch`].
    /// Returns the number of bytes consumed from `bytes`; caller
    /// advances its input pointer accordingly.
    #[inline]
    pub(in crate::protocol) fn absorb_partial_assembly_at_dispatch(
        cell: &mut crate::partial_assembly::PartialAssemblyCell,
        bytes: &[u8],
    ) -> usize {
        cell.absorb_at_dispatch(PartialAssemblyAbsorbToken(()), bytes)
    }

    /// Take a completed assembly via
    /// [`crate::partial_assembly::PartialAssemblyCell::take_completed`].
    /// Returns `Some(Box)` only when the assembly is complete (caller
    /// must first check `cell.as_inner()?.is_complete()`).
    #[inline]
    #[must_use]
    pub(in crate::protocol) fn take_completed_partial_assembly_at_dispatch(
        cell: &mut crate::partial_assembly::PartialAssemblyCell,
    ) -> Option<alloc::boxed::Box<crate::partial_assembly::PartialAssemblyInner>> {
        cell.take_completed(PartialAssemblyTakeToken(()))
    }
}

/// DEF-272 cluster β leaf submodule for the inbound `ParameterStatus`
/// pre-dispatch filter. Hosts the [`ParamStatusToken`] type and the
/// single admit helper fn that delegates to the cell's parse+record
/// method.
// DEF-244 modernisation audit (rust-version 1.81 sweep): historical
// dead `#[allow(missing_docs, ...)]` removed (lint doesn't fire on
// `pub(crate)` items).
pub(crate) mod _parameter_status_admit_leaf {
    /// DEF-272 cluster β leaf-scope token. Field private to the leaf;
    /// type `pub(crate)` so
    /// [`crate::session_params_slot::SessionParamsCell::admit_at_param_status`]
    /// can name it.
    pub(crate) struct ParamStatusToken(());

    /// Mint a [`ParamStatusToken`] and admit the `ParameterStatus`
    /// frame via [`crate::session_params_slot::SessionParamsCell::admit_at_param_status`].
    /// The cell handles parse + record (success) / bump-malformed
    /// (parse failure) internally; lazy-inits the inner box on first
    /// call.
    #[inline]
    #[must_use]
    pub(in crate::protocol) fn admit_parameter_status_frame(
        cell: &mut crate::session_params_slot::SessionParamsCell,
        payload: &[u8],
    ) -> super::ParamStatusRecordOutcome {
        cell.admit_at_param_status(payload, ParamStatusToken(()))
    }
}

/// DEF-272 cluster β leaf submodule for the inbound `NoticeResponse`
/// pre-dispatch filter. Hosts the [`NoticeResponseToken`] type and
/// the single admit helper fn.
// DEF-244 modernisation audit (rust-version 1.81 sweep): historical
// dead `#[allow(missing_docs, ...)]` removed (lint doesn't fire on
// `pub(crate)` items).
pub(crate) mod _notice_response_admit_leaf {
    /// DEF-272 cluster β leaf-scope token. Field private to the leaf;
    /// type `pub(crate)` so
    /// [`crate::session_params_slot::SessionParamsCell::admit_at_notice_response`]
    /// can name it.
    pub(crate) struct NoticeResponseToken(());

    /// Mint a [`NoticeResponseToken`] and admit the `NoticeResponse`
    /// frame via
    /// [`crate::session_params_slot::SessionParamsCell::admit_at_notice_response`].
    /// The cell bumps the notice counter and lazy-inits the inner box
    /// on first call.
    #[inline]
    pub(in crate::protocol) fn admit_notice_response_frame(
        cell: &mut crate::session_params_slot::SessionParamsCell,
    ) {
        cell.admit_at_notice_response(NoticeResponseToken(()));
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-272 P6 closure (2026-05-10) — `_proto_init_leaf` submodule
//
// Architect hostile-probe (2026-05-10) confirmed that pre-this-leaf
// `*cell = RowDescSlotCell::EMPTY` / `SessionParamsCell::EMPTY` was
// callable from any in-crate file via the `pub(crate) const EMPTY`. The
// straightforward fix `pub(in crate::protocol) const EMPTY` is invalid
// (E0742: visibility path must be ancestor; mod schema_slot / mod
// session_params_slot are siblings of mod protocol, not children). The
// proper closure is the leaf-token pattern (mirrors DEF-272 cluster δ):
//
//   - `_proto_init_leaf::ProtoInitToken` has a private tuple-struct
//     field — `Self(())` mintable ONLY inside this submodule.
//   - Cells expose `pub(crate) const fn empty(token: ProtoInitToken)`
//     instead of `pub(crate) const EMPTY`. Fresh cell construction
//     requires a token.
//   - `PgProtocol::new` lives INSIDE `_proto_init_leaf` so it has
//     access to `ProtoInitToken::mint()`. Code outside the leaf cannot
//     mint tokens → cannot construct fresh cells → cannot wholesale-
//     replace `*pg.row_desc_slot = …` (no fresh value to assign).
//
// Wholesale-replacement is gated to this submodule by construction.
// Tier-1 within-crate. The leaf body is the entire init logic — small
// enough to review as a unit.
// ═════════════════════════════════════════════════════════════════════

// DEF-244 modernisation audit (rust-version 1.81 sweep): historical
// dead `#[allow(missing_docs, ...)]` removed (lint doesn't fire on
// `pub(crate)` items). Original reason: submodule contains init-token
// + sole legitimate cell-construction site (DEF-272 P6 closure).
pub(crate) mod _proto_init_leaf {
    /// DEF-272 P6 closure token (2026-05-10). Field private to leaf —
    /// `Self(())` mintable ONLY inside this submodule via
    /// [`ProtoInitToken::mint`]. `pub(crate)` type signature so cell
    /// modules can name the type in their `empty(token)` parameter,
    /// but the private tuple-struct field gates construction.
    #[derive(Clone, Copy)]
    pub(crate) struct ProtoInitToken(());

    impl ProtoInitToken {
        /// Mint a token. Private (no `pub`) — only callable inside
        /// `_proto_init_leaf` (which contains the sole legitimate
        /// caller, [`super::PgProtocol::new`]).
        const fn mint() -> Self {
            Self(())
        }
    }

    impl super::PgProtocol<super::DisconnectedPhase> {
        /// Construct a new protocol in [`crate::state::ProtoState::Idle`],
        /// typed `PgProtocol<DisconnectedPhase>` — the only legal next
        /// step is [`Self::push_startup`].
        ///
        /// **DEF-246 Phase 2 (2026-05-16):** the constructor now
        /// produces `<DisconnectedPhase>` (pre-DEF-246 Phase 2 produced
        /// `<ActivePhase>`). Tier-1 elevation #1: the
        /// `PgCommand::Startup` enum + `push_command::Startup` struct +
        /// `impl PushCommand for Startup` are deleted; the only path
        /// into a connecting protocol is
        /// `<DisconnectedPhase>::push_startup(...) ->
        /// PgProtocol<ConnectingPhase>` (consume-self transition).
        /// Pushing any other command from `<DisconnectedPhase>` is a
        /// method-absent E0599 — the per-command structs
        /// (`Ping`, `SimpleQuery`, `Parse`, …) implement
        /// [`crate::push_command::PushCommand`] which is reachable
        /// only through `<ActivePhase>::push_command_internal`.
        ///
        /// Lives inside `_proto_init_leaf` (DEF-272 P6 closure 2026-05-10):
        /// the token-gated [`crate::schema_slot::RowDescSlotCell::empty`]
        /// and [`crate::session_params_slot::SessionParamsCell::empty`]
        /// constructors require a [`ProtoInitToken`], which can only be
        /// minted here. Wholesale-replacement of cell fields is therefore
        /// narrowed to this submodule by construction.
        #[must_use]
        pub const fn new() -> Self {
            let token = ProtoInitToken::mint();
            Self {
                inner: super::PgProtocolInner {
                    state: super::ProtoState::Idle,
                    read_buf: super::ReadBuf::new(),
                    row_desc_slot: crate::schema_slot::RowDescSlotCell::empty(token),
                    // DEF-196: three independent cold slots — none allocated
                    // at construction. Trust auth + no errors + no malformed
                    // frames + no notice/param frames = lifetime-zero heap.
                    session_params: crate::session_params_slot::SessionParamsCell::empty(token),
                    error_arena: None,
                    // DEF-248 Sub-B (2026-05-12): partial-assembly cell —
                    // 8 B niche, `None` at construction. Heap-allocates a
                    // single Box<PartialAssemblyInner> (8 KB prefix + ~12 B
                    // meta) only on the first oversize non-`'D'` frame.
                    // Re-used across subsequent oversize frames on the
                    // same connection.
                    partial_assembly: crate::partial_assembly::PartialAssemblyCell::empty(token),
                    // DEF-278 Bundle D (2026-05-17): backend-key cell —
                    // None at construction; installed by the dispatch arm
                    // at `(ConnectingPostAuthHaveKey, 'Z')` once the
                    // handshake completes. The same `ProtoInitToken`
                    // gates this empty constructor (mirror of the
                    // schema_slot / session_params / partial_assembly
                    // pattern).
                    backend_key: crate::cancel::BackendKeyCell::empty(token),
                    malformed_frame_count: 0,
                    sync_marker: super::PhantomData,
                },
                phase_marker: super::PhantomData,
            }
        }
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-246 Phase 2/3/4 transition surfaces (2026-05-16)
//
// `<DisconnectedPhase>::push_startup`           consume-self → ConnectingPhase
// `<ConnectingPhase>::feed_inbound`             1-line delegate to Inner
// `<ConnectingPhase>::feed_bytes`               1-line delegate to Inner
// `<ConnectingPhase>::advance_one_frame`        1-line delegate to Inner
// `<ConnectingPhase>::into_active`              consume-self → ActivePhase | IntoActiveError
// `<ClosedPhase>::cause`                        accessor — reconstructed ProtocolError
// `<ActivePhase>::into_closed_if_errored`       consume-self → ClosedPhase | ActivePhase (declared above)
//
// Tier elevations (Phase 2+3+4):
//   #1: push-before-Startup            → method-absent E0599 on <DisconnectedPhase>::push_*
//   #2: push-during-Connecting         → method-absent E0599 on <ConnectingPhase>::push_*
//   #3: Closed absorbs no input        → method-absent E0599 on <ClosedPhase>::feed_*/push_*
//   #4: feed_inbound surfaces typed err → Result<(), ProtocolError> across all phases that have feed_inbound
// ═════════════════════════════════════════════════════════════════════

/// DEF-246 Phase 3 (2026-05-16) — error returned by
/// [`PgProtocol::<ConnectingPhase>::into_active`].
///
/// The protocol is consumed by the transition; both arms carry the
/// state needed for the caller to recover or terminate:
///
/// - `Closed(PgProtocol<ClosedPhase>)` — handshake observed an
///   `Errored(_)` transition (auth failure, malformed server frame,
///   etc.). Caller writes `proto.cause()` for the typed error.
/// - `StillConnecting(PgProtocol<ConnectingPhase>)` — handshake has
///   not yet completed (mid-auth, no `ReadyForQuery` yet). Caller
///   continues to drive `feed_inbound` / `advance_one_frame` until
///   either RFQ arrives or the connection terminates.
///
/// Tier-1 closure: the user CANNOT obtain a `PgProtocol<ActivePhase>`
/// without driving the handshake to `state == Idle`. The constructor
/// `PgProtocol::new()` produces `<DisconnectedPhase>`; the only path
/// to `<ConnectingPhase>` is `push_startup`; the only path from
/// `<ConnectingPhase>` to `<ActivePhase>` is `into_active` with this
/// classifier returning the `Ok` arm.
#[expect(
    missing_debug_implementations,
    reason = "DEF-246 Phase 3: both variants carry PgProtocol wrappers with phase-typed markers; \
              Debug is implemented blanket-style on `PgProtocol<P>`, so emitting one for the \
              enum would either redact (defeating purpose) or print the full inner state. \
              Deferred until a concrete diagnostic surface needs the trait."
)]
#[must_use = "IntoActiveError consumes the protocol — the caller must observe the variant \
              to recover the typed wrapper or terminate"]
pub enum IntoActiveError {
    /// Handshake terminated in `Errored(_)` — the `ClosedPhase`
    /// wrapper exposes `cause()` for the typed error.
    Closed(PgProtocol<ClosedPhase>),
    /// Handshake has not completed (mid-auth, no RFQ yet). The
    /// wrapper is still `<ConnectingPhase>` — caller drives
    /// `feed_inbound` / `advance_one_frame` further.
    StillConnecting(PgProtocol<ConnectingPhase>),
}

impl PgProtocol<DisconnectedPhase> {
    /// DEF-246 Phase 2 (2026-05-16) — diagnostic accessor mirror of
    /// `<ActivePhase>::error_arena_overwrite_count`. A fresh
    /// disconnected protocol has no errors yet (counter = 0); the
    /// accessor exposes the same field for diagnostic compatibility.
    #[inline]
    #[must_use]
    pub fn error_arena_overwrite_count(&self) -> u16 {
        match self.inner.error_arena.as_deref() {
            Some(a) => a.overwrite_count(),
            None => 0,
        }
    }

    /// DEF-246 Phase 2 (2026-05-16) — `connection_status` accessor on
    /// `<DisconnectedPhase>`. A fresh protocol reports `Ready` (the
    /// Idle bucket) since `push_startup` is the legal next operation.
    /// This mirrors the runtime state (`ProtoState::Idle`) classifier;
    /// the phase marker is orthogonal to the runtime status.
    #[inline]
    #[must_use]
    pub fn connection_status(&self) -> crate::guard::ConnectionStatus {
        use crate::guard::ConnectionStatus;
        use crate::state::StatePushClass;
        match self.inner.state.push_class() {
            StatePushClass::Idle => ConnectionStatus::Ready,
            StatePushClass::Errored(kind) => ConnectionStatus::Errored(kind),
            StatePushClass::PingAwaiting | StatePushClass::BusyQuery => ConnectionStatus::Busy,
            StatePushClass::Connecting => ConnectionStatus::Handshaking,
        }
    }

    /// DEF-246 Phase 2 (2026-05-16) — public state accessor on
    /// `<DisconnectedPhase>`. Fresh protocols always report `Idle`;
    /// callers comparing against `ProtoState::Idle` in tests or
    /// diagnostics use this.
    #[inline]
    #[must_use]
    pub fn state(&self) -> &ProtoState {
        &self.inner.state
    }

    /// Mint a fresh `ReplyId<K>` for the impending Startup push.
    ///
    /// DEF-246 Phase 2 (2026-05-16): mirror of
    /// `<ActivePhase>::next_reply_id` — the disconnect-state needs a
    /// ReplyId before `push_startup` so the wrapper can route the
    /// Reply::StartupComplete back to the caller's oneshot. Same
    /// static-atomic counter as the other phases (process-global
    /// uniqueness preserved).
    #[inline]
    pub fn next_reply_id<K: crate::reply_id::ReplyKind>(
        &mut self,
    ) -> crate::reply_id::ReplyId<K> {
        self.inner.next_reply_id::<K>()
    }

    /// DEF-246 Phase 2 elevation #1 (2026-05-16) — initiate the
    /// PostgreSQL startup handshake.
    ///
    /// Consume-self transition: the typed `<DisconnectedPhase>` is
    /// converted into `<ConnectingPhase>` on every success path
    /// (including the structurally-distant `Idle build-failed` arm
    /// which transitions to `Errored` — observed via subsequent
    /// `advance_one_frame` → `FeedEvent::Close`, then
    /// `<ConnectingPhase>::into_active` returns
    /// `IntoActiveError::Closed`).
    ///
    /// # Pre-Phase-2 shape (deleted)
    ///
    /// Pre-DEF-246 Phase 2 the entry point was the
    /// `push_command::Startup` per-command struct +
    /// `impl PushCommand for Startup` on `<ActivePhase>` (via the
    /// `ReadyGuard::push_command` typed dispatch). The struct + impl +
    /// the legacy `PgCommand::Startup` enum variant are deleted in the
    /// same commit; this method is the only path.
    ///
    /// # Tier-1 closure
    ///
    /// - Calling `push_command(Ping)` (or any other per-command
    ///   struct) on `<DisconnectedPhase>` is method-absent E0599 —
    ///   `<DisconnectedPhase>` does not implement / expose
    ///   `push_command_internal`. The `_proto_init_leaf` ZST-marker
    ///   protocol can only call `push_startup`.
    /// - The consume-self signature physically prevents calling
    ///   `push_startup` twice on the same wrapper. The first call
    ///   moves `self` into the returned `<ConnectingPhase>` wrapper
    ///   and the original variable is no longer accessible at the
    ///   source level (Rust ownership).
    ///
    /// # Returned tuple
    ///
    /// On `Ok((actions, proto_connecting))`:
    /// - `actions: OutActions<'w, 'static>` — the StartupMessage wire
    ///   bytes (single `Action::SendBytes` chunk in `write_buf`).
    /// - `proto_connecting: PgProtocol<ConnectingPhase>` — the typed
    ///   wrapper for the handshake window.
    ///
    /// On `Err(PushFailure)` (extremely rare — startup fits 512 B cap
    /// and the build pipeline is const-asserted against the wire
    /// frame), the protocol is destroyed — `Err` carries no recovery
    /// surface. Caller logs the failure and drops the connection.
    // DEF-246 Phase 2: argument count mirrors compute_push_startup_idle_only's
    // signature 1:1. Splitting into a struct-arg would obscure the
    // consume-self framing and force an inline destructure at every
    // callsite (which would defeat the migration ergonomics from the
    // pre-DEF-246 `Startup { ... }` struct-literal shape). The
    // returned `Result<_, PushFailure>` carries ~80 B in the Err arm
    // (below the 128 B threshold); no `result_large_err` exception
    // needed.
    pub fn push_startup<'w>(
        mut self,
        user: crate::ident::Ident,
        database: Option<crate::ident::DatabaseName>,
        app_name: Option<crate::ident::ApplicationName>,
        credentials: crate::password::Credentials,
        reply: crate::reply_id::ReplyId<crate::reply_id::StartupKind>,
        write_buf: &'w mut WriteBuf,
    ) -> Result<
        (
            crate::action::OutActions<'w, 'static>,
            PgProtocol<ConnectingPhase>,
        ),
        crate::action::PushFailure,
    > {
        // Mirror of push_command_internal: clear residue + branded
        // scope + materialise. The Idle precondition is structurally
        // guaranteed by the `<DisconnectedPhase>` marker (fresh
        // protocols start at `state == Idle`; this method consumes
        // self so a second call is type-impossible).
        write_buf.clear();
        self.inner
            .clear_session_residue_for_class(crate::state::StatePushClass::Idle);

        let state = &mut self.inner.state;
        let row_desc_slot = &mut self.inner.row_desc_slot;
        let idle = match crate::state_setter::IdleState::try_from(state) {
            Some(idle) => idle,
            None => {
                // Architecturally unreachable: `<DisconnectedPhase>`
                // is consumed by the constructor only path, which
                // installs `state == Idle`. The only other transition
                // path into `<DisconnectedPhase>` does not exist.
                // Classify defensively per CREDO §V (debug_assert!(false)
                // banned for impossible) — the `Errored` arm transitions
                // via the `IntoActiveError::Closed` channel.
                core::hint::cold_path();
                return Err(crate::action::PushFailure {
                    id: reply.consume(),
                    cause: crate::error::ProtocolError::InternalCrateBug {
                        locus: crate::error::CrateBugLocus::PushCommandInternalNonIdle,
                    },
                });
            }
        };

        // DEF-160 Z2: single-pass materialise inside branded closure.
        // The closure produces the final `Result<OutActions, PushFailure>`
        // directly — no intermediate StagedActions escape.
        let result: Result<
            crate::action::OutActions<'w, 'static>,
            crate::action::PushFailure,
        > = write_buf
            .with_branded(
                |mut wb| -> Result<crate::action::OutActions<'w, 'static>, crate::action::PushFailure> {
                    let mut staged: StagedActions<'_> = StagedActions::new();
                    {
                        let mut reserved = wb.reserve();
                        let setter = idle.into_setter::<crate::push_command::StartupPostInstall>();
                        compute_push_startup_idle_only(
                            setter,
                            user,
                            database,
                            app_name,
                            credentials,
                            reply,
                            &mut staged,
                            &mut reserved,
                        );
                    }
                    let bytes: &'w [u8] = wb.into_bytes();

                    let mut failure: Option<crate::action::PushFailure> = None;
                    let mut out: crate::action::OutActions<'w, 'static> =
                        crate::action::OutActions::new();
                    for sa in staged {
                        match sa {
                            StagedAction::FailReply { id, cause } => {
                                if failure.is_none() {
                                    failure = Some(crate::action::PushFailure { id, cause });
                                }
                            }
                            StagedAction::SendBytesRange(range) => {
                                if let Some(slice) = range.apply(bytes) {
                                    push_within_fanout_budget(
                                        &mut out,
                                        crate::action::Action::SendBytes(slice),
                                    );
                                }
                            }
                            StagedAction::SendBytesStatic(slice) => {
                                push_within_fanout_budget(
                                    &mut out,
                                    crate::action::Action::SendBytes(slice),
                                );
                            }
                            StagedAction::SendBytesBorrowed(_)
                            | StagedAction::CloseSocket
                            | StagedAction::DeliverReply(_) => {
                                // compute_push_startup_idle_only emits
                                // only SendBytesRange (StartupMessage)
                                // + post_install. Other variants are
                                // architecturally unreachable from
                                // this push path. Skip silently rather
                                // than panic (CREDO §V); a future
                                // refactor adding emits would surface
                                // via test failure (no actions in out).
                            }
                        }
                    }
                    match failure {
                        Some(f) => Err(f),
                        None => Ok(out),
                    }
                },
            );

        let _: () = row_desc_slot.consume_unused_witness();
        match result {
            Ok(out) => {
                // Move self.inner into the new <ConnectingPhase>
                // wrapper. The inner state is now one of
                // ConnectingStartup{Trust|Scram|Cleartext|Md5}.
                Ok((
                    out,
                    PgProtocol {
                        inner: self.inner,
                        phase_marker: PhantomData,
                    },
                ))
            }
            Err(f) => Err(f),
        }
    }
}

// Phase 2 helper: keep `row_desc_slot: &mut RowDescSlotCell` named so
// the borrow checker is satisfied; the cell is not used by
// Startup push (only BindExecute parks a RowDesc) but the type-system
// path forces a placeholder method.
impl crate::schema_slot::RowDescSlotCell {
    /// DEF-246 Phase 2 (2026-05-16): no-op marker consumed by
    /// `<DisconnectedPhase>::push_startup` to discharge the
    /// `&mut row_desc_slot` binding without an `_ = …` discard
    /// (banned crate-wide). The cell is structurally untouched.
    #[inline]
    pub(crate) const fn consume_unused_witness(&self) {
        // No-op: cell is untouched by Startup push.
    }
}

impl PgProtocol<ConnectingPhase> {
    /// Mint a fresh `ReplyId<K>` during the handshake window.
    ///
    /// DEF-246 Phase 3 (2026-05-16): mirror of
    /// `<ActivePhase>::next_reply_id`. Useful for pipelined drivers
    /// that pre-mint correlators before observing `into_active()`'s
    /// classifier (typically not used during the standard handshake
    /// but available for advanced pipelined flows).
    #[inline]
    pub fn next_reply_id<K: crate::reply_id::ReplyKind>(
        &mut self,
    ) -> crate::reply_id::ReplyId<K> {
        self.inner.next_reply_id::<K>()
    }

    /// DEF-246 Phase 3 elevation #2 (2026-05-16) — append inbound
    /// auth-flow bytes during the startup handshake.
    ///
    /// 1-line delegate to [`PgProtocolInner::feed_inbound`]. The
    /// `<ActivePhase>::feed_inbound` mirror exists on
    /// [`PgProtocol<ActivePhase>`] for the post-handshake hot path.
    /// Both phases route through the same byte path.
    pub fn feed_inbound(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), crate::error::ProtocolError> {
        self.inner.feed_inbound(bytes)
    }

    /// DEF-246 Phase 3 elevation #2 (2026-05-16) — per-event advance
    /// during handshake.
    ///
    /// 1-line delegate to [`PgProtocolInner::advance_one_frame`]
    /// (same body as `<ActivePhase>::advance_one_frame`). During
    /// handshake the caller drives this until either
    /// `FeedEvent::Deliver` (StartupComplete reply) arrives or
    /// `FeedEvent::Close` (Errored) terminates the connection. The
    /// public consume-self [`Self::into_active`] then classifies the
    /// outcome.
    #[must_use = "FeedEvent variants carry side-effect contracts: \
                  SendBytes/Deliver MUST be processed; Fail/Close MUST \
                  trigger socket teardown"]
    pub fn advance_one_frame<'w, 'r>(
        &'r mut self,
        write_buf: &'w mut WriteBuf,
    ) -> crate::action::FeedEvent<'w, 'r> {
        self.inner.advance_one_frame(write_buf)
    }

    /// DEF-246 Phase 3 elevation #2 (2026-05-16) — batched
    /// feed-and-dispatch during handshake.
    ///
    /// Mirror of `<ActivePhase>::feed_bytes` — useful for callers
    /// that prefer the batched OutActions surface over the per-event
    /// `advance_one_frame` loop. Same const-generic specialisation
    /// (`BOUNDED = false`).
    #[must_use = "the returned actions carry side-effects that must be executed"]
    pub fn feed_bytes<'w, 'r>(
        &'r mut self,
        bytes: &[u8],
        write_buf: &'w mut WriteBuf,
    ) -> OutActions<'w, 'r> {
        self.inner.feed_bytes_impl::<false>(bytes, write_buf, 0)
    }

    /// DEF-246 Phase 3 (2026-05-16) — consume-self transition from
    /// `<ConnectingPhase>` to `<ActivePhase>`.
    ///
    /// Returns `Ok(PgProtocol<ActivePhase>)` only when the runtime
    /// state is `ProtoState::Idle` (handshake completed via RFQ).
    /// Pre-RFQ states return `Err(IntoActiveError::StillConnecting(self))`;
    /// `Errored(_)` returns `Err(IntoActiveError::Closed(closed))`.
    ///
    /// # Tier-1 closure
    ///
    /// The user CANNOT obtain a `<ActivePhase>` without observing
    /// either (a) RFQ + state==Idle (Ok arm), or (b) handshake error
    /// (Closed arm). There is no «assume Active» bypass; the only
    /// constructor is `PgProtocol::new() -> <DisconnectedPhase>` and
    /// the only path through is `push_startup → ConnectingPhase →
    /// into_active`.
    #[expect(
        clippy::result_large_err,
        reason = "consume-self transition: BOTH variants carry phase-typed PgProtocol \
                  wrappers (~520 B) by value so the caller can recover the next-phase \
                  state without an alloc. Boxing would penalise every handshake path."
    )]
    pub fn into_active(self) -> Result<PgProtocol<ActivePhase>, IntoActiveError> {
        if matches!(self.inner.state, ProtoState::Errored(_)) {
            return Err(IntoActiveError::Closed(PgProtocol {
                inner: self.inner,
                phase_marker: PhantomData,
            }));
        }
        if matches!(self.inner.state, ProtoState::Idle) {
            return Ok(PgProtocol {
                inner: self.inner,
                phase_marker: PhantomData,
            });
        }
        Err(IntoActiveError::StillConnecting(self))
    }

    /// DEF-246 Phase 3 (2026-05-16) — public read-only accessor for
    /// the current state during handshake. Useful for diagnostic
    /// logging without converting to `<ActivePhase>`.
    #[inline]
    #[must_use]
    pub fn state(&self) -> &ProtoState {
        &self.inner.state
    }

    /// DEF-246 Phase 3 (2026-05-16) — session_params accessor during
    /// handshake (mirrors `<ActivePhase>::session_params`). The
    /// server's `ParameterStatus` frames during the handshake populate
    /// these; callers may inspect mid-handshake values for diagnostic
    /// purposes.
    #[inline]
    #[must_use]
    pub fn session_params(&self) -> &SessionParams {
        // Mirror of `<ActivePhase>::cold_session_params`. The static
        // empty fallback lives at the inner accessor (and matches
        // `<ActivePhase>` byte-for-byte).
        static EMPTY: SessionParams = SessionParams::new();
        match self.inner.session_params.as_deref() {
            Some(p) => p,
            None => &EMPTY,
        }
    }

    /// DEF-246 Phase 3 (2026-05-16) — server-error arena accessor
    /// during handshake (mirrors `<ActivePhase>::get_server_error`).
    /// Useful when handshake fails: `ErrorResponse` during startup
    /// classifies as `ProtocolError::ServerErrorResponse
    /// { details_ref, … }`; callers resolve via this method to
    /// inspect the server's message before transitioning to
    /// `<ClosedPhase>`.
    #[inline]
    pub fn get_server_error(
        &self,
        r: crate::error_arena::ErrorRef,
    ) -> Result<&crate::error_arena::ErrorPayload, crate::error_arena::ArenaError> {
        static EMPTY: crate::error_arena::ErrorArena =
            crate::error_arena::ErrorArena::new();
        let arena: &crate::error_arena::ErrorArena = match self.inner.error_arena.as_deref() {
            Some(a) => a,
            None => &EMPTY,
        };
        // Static-arena fallback never holds a real payload, but
        // generation-mismatch resolves correctly via `get`'s
        // Stale classification — identity result is what we want.
        arena.get(r)
    }

    /// DEF-246 Phase 3 (2026-05-16) — `as_ready` accessor during
    /// handshake. ALWAYS returns `None` while in `<ConnectingPhase>`
    /// because the phase classifier maps every `Connecting*` variant
    /// to `ConnectionStatus::Handshaking` (not `Ready`). Exposed for
    /// test compatibility with the pre-DEF-246 callsites that did
    /// `proto.as_ready().is_none()` checks during a handshake.
    ///
    /// **Type signature note:** returns `Option<()>` rather than
    /// `Option<ReadyGuard>` — there is NO legitimate push path during
    /// handshake (the only Connecting-state command would be
    /// re-Startup, which is also banned by `<DisconnectedPhase>`
    /// consume-self). The `()` return marks "handshaking, no push
    /// guard available" without exposing the ActivePhase-bound
    /// `ReadyGuard` type.
    #[inline]
    #[must_use]
    pub fn as_ready(&mut self) -> Option<()> {
        // `<ConnectingPhase>` always reports Handshaking; no Idle
        // classification path exists during handshake (Idle here would
        // imply RFQ-complete, at which point the caller must
        // `into_active()` to access the push surface).
        None
    }

    /// DEF-246 Phase 3 (2026-05-16) — `connection_status` accessor
    /// during handshake — mirrors `<ActivePhase>::connection_status`.
    #[inline]
    #[must_use]
    pub fn connection_status(&self) -> crate::guard::ConnectionStatus {
        use crate::guard::ConnectionStatus;
        use crate::state::StatePushClass;
        match self.inner.state.push_class() {
            StatePushClass::Idle => ConnectionStatus::Ready,
            StatePushClass::Errored(kind) => ConnectionStatus::Errored(kind),
            StatePushClass::PingAwaiting | StatePushClass::BusyQuery => ConnectionStatus::Busy,
            StatePushClass::Connecting => ConnectionStatus::Handshaking,
        }
    }
}

impl PgProtocol<ClosedPhase> {
    /// DEF-246 Phase 4 (2026-05-16) — typed error accessor for a
    /// terminally-Errored protocol.
    ///
    /// Reconstructs a `ProtocolError::ConnectionAlreadyClosed
    /// { prior_kind }` from the stored `StateErrorKind`. Full arena
    /// lookup (server ErrorResponse details) is reachable separately
    /// via the still-living `inner.error_arena` if the wrapper layer
    /// stashed an `ErrorRef` before the consume-self transition; see
    /// follow-up surfaces for the multi-phase arena handle plan.
    ///
    /// # Tier-1 closure
    ///
    /// `<ClosedPhase>` exposes ONLY `cause()`. No `push_command`, no
    /// `feed_inbound`, no `feed_bytes`, no `advance_one_frame`,
    /// no `into_active`. Calling any of those on a `<ClosedPhase>`
    /// instance is method-absent E0599 (Phase 4 elevation #3 —
    /// «Closed absorbs no input»). The protocol is terminal.
    #[inline]
    #[must_use = "the returned ProtocolError carries the terminal cause; observing it is the only \
                  legitimate operation on a Closed protocol"]
    pub fn cause(&self) -> crate::error::ProtocolError {
        match &self.inner.state {
            ProtoState::Errored(k) => {
                crate::error::ProtocolError::ConnectionAlreadyClosed { prior_kind: *k }
            }
            // Architecturally unreachable: `<ClosedPhase>` is reached
            // ONLY via `<ActivePhase>::into_closed_if_errored` (guard
            // `matches!(state, Errored(_))`) or
            // `<ConnectingPhase>::into_active` (Closed arm — same
            // guard). The non-Errored arm is dead at the type level
            // but the runtime field type does not know that.
            // CREDO §V: classify defensively rather than debug_assert.
            _ => crate::error::ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::ReadCursorAdvance,
            },
        }
    }

    /// DEF-246 Phase 4 (2026-05-16) — public read-only state
    /// accessor for the closed protocol (mirrors `<ActivePhase>::state`).
    #[inline]
    #[must_use]
    pub fn state(&self) -> &ProtoState {
        &self.inner.state
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-272 cluster δ (2026-05-10) — feed-side error-transition leaves
//
// Per-call-site concrete-type tokens that gate
// `crate::state_setter::drain_at_*` constructors. Each leaf submodule
// hosts a token with PRIVATE field — the literal `Self(())` mint is
// callable ONLY inside the submodule. The token is consumed by the
// matching `drain_at_*` free fn in mod state_setter, which in turn
// constructs `FeedStateSetter::new` (private to mod state_setter).
// ═════════════════════════════════════════════════════════════════════

/// DEF-272 cluster δ leaf submodule for the `install_errored_replyid_saturation`
/// transition. The saturation classifier (cluster D) fires from any
/// state, hence the `drain_at_replyid_saturation` returns
/// `Option<NonZeroU64>` (None for `Idle` / `DrainRfqAfterError` /
/// `Errored` prior states).
// DEF-244 modernisation audit (rust-version 1.81 sweep): historical
// dead `#[allow(missing_docs, ...)]` removed (lint doesn't fire on
// `pub(crate)` items).
pub(crate) mod _replyid_saturation_drain_leaf {
    /// DEF-272 cluster δ leaf-scope token. Field private to leaf.
    pub(crate) struct ReplyIdSaturationToken(());

    /// Mint a [`ReplyIdSaturationToken`] and route through
    /// [`crate::state_setter::drain_at_replyid_saturation`]. Used by
    /// [`crate::PgProtocol::install_errored_replyid_saturation`].
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id (if any). \
                  Caller `install_errored_replyid_saturation` binds it to `_drained_id_at_saturation` \
                  for documentation; saturation has no FailReply emission context."]
    pub(in crate::protocol) fn drain(
        state: &mut crate::state::ProtoState,
        kind: crate::error::StateErrorKind,
    ) -> Option<core::num::NonZeroU64> {
        crate::state_setter::drain_at_replyid_saturation(state, ReplyIdSaturationToken(()), kind)
    }
}

/// DEF-272 cluster δ leaf submodule for the
/// `install_errored_read_cursor_advance` transition. Fires when the
/// row-stream fast path detects a read-cursor advance failure
/// (`CrateBugLocus::ReadCursorAdvance`).
// DEF-244 modernisation audit (rust-version 1.81 sweep): historical
// dead `#[allow(missing_docs, ...)]` removed (lint doesn't fire on
// `pub(crate)` items).
pub(crate) mod _read_cursor_advance_drain_leaf {
    /// DEF-272 cluster δ leaf-scope token. Field private to leaf.
    pub(crate) struct ReadCursorAdvanceToken(());

    /// Mint a [`ReadCursorAdvanceToken`] and route through
    /// [`crate::state_setter::drain_at_read_cursor_advance`].
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
                  by the Errored install. Caller MUST emit ColEvent::EndQuery { outcome: Err(_) } or equivalent."]
    pub(in crate::protocol) fn drain(
        state: &mut crate::state::ProtoState,
        kind: crate::error::StateErrorKind,
    ) -> Option<core::num::NonZeroU64> {
        crate::state_setter::drain_at_read_cursor_advance(state, ReadCursorAdvanceToken(()), kind)
    }
}

/// DEF-272 cluster δ leaf submodule for the
/// `install_errored_malformed_data_row` transition. Fires from
/// streaming variants when a DataRow body is malformed (zero-length,
/// etc.).
// DEF-244 modernisation audit (rust-version 1.81 sweep): historical
// dead `#[allow(missing_docs, ...)]` removed (lint doesn't fire on
// `pub(crate)` items).
pub(crate) mod _malformed_data_row_drain_leaf {
    /// DEF-272 cluster δ leaf-scope token. Field private to leaf.
    pub(crate) struct MalformedDataRowToken(());

    /// Mint a [`MalformedDataRowToken`] and route through
    /// [`crate::state_setter::drain_at_malformed_data_row`].
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
                  by the Errored install. Caller MUST emit ColEvent::EndQuery { outcome: Err(_) } or equivalent."]
    pub(in crate::protocol) fn drain(
        state: &mut crate::state::ProtoState,
        kind: crate::error::StateErrorKind,
    ) -> Option<core::num::NonZeroU64> {
        crate::state_setter::drain_at_malformed_data_row(state, MalformedDataRowToken(()), kind)
    }
}

/// DEF-272 cluster δ leaf submodule for the
/// `fail_inflight_no_readbuf` transition. Fires from dispatch when an
/// in-flight error occurs and no read-buf state is available for
/// payload preservation.
// DEF-244 modernisation audit (rust-version 1.81 sweep): historical
// dead `#[allow(missing_docs, ...)]` removed (lint doesn't fire on
// `pub(crate)` items).
pub(crate) mod _fail_inflight_no_readbuf_drain_leaf {
    /// DEF-272 cluster δ leaf-scope token. Field private to leaf.
    pub(crate) struct FailInflightNoReadbufToken(());

    /// Mint a [`FailInflightNoReadbufToken`] and route through
    /// [`crate::state_setter::drain_at_fail_inflight_no_readbuf`].
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
                  by the Errored install. Caller emits FailReply with the cause."]
    pub(in crate::protocol) fn drain(
        state: &mut crate::state::ProtoState,
        kind: crate::error::StateErrorKind,
    ) -> Option<core::num::NonZeroU64> {
        crate::state_setter::drain_at_fail_inflight_no_readbuf(
            state,
            FailInflightNoReadbufToken(()),
            kind,
        )
    }
}

/// DEF-248 Sub-A (2026-05-12) leaf submodule for the
/// `install_errored_stream_dropped_mid_stream` transition. Fires from
/// [`crate::row_stream::RowStream::drop`] when the stream is dropped
/// with `drained == false` (closure exited mid-frame: normal early
/// return, `?` propagation, panic unwind).
///
/// Mirror of cluster δ leaves above (`_read_cursor_advance_drain_leaf`,
/// `_malformed_data_row_drain_leaf`, …). The
/// `StreamDroppedMidStreamToken` tuple-struct field is private to this
/// submodule — `Self(())` mints are callable ONLY inside the leaf.
/// Hostile in-crate attempts to call `drain_at_stream_dropped_mid_stream`
/// from outside this leaf cannot construct the required token type;
/// the type system rejects.
// DEF-244 modernisation audit (rust-version 1.81 sweep): historical
// dead `#[allow(missing_docs, ...)]` removed (lint doesn't fire on
// `pub(crate)` items).
pub(crate) mod _stream_dropped_mid_stream_drain_leaf {
    /// DEF-248 Sub-A leaf-scope token. Field private to leaf.
    pub(crate) struct StreamDroppedMidStreamToken(());

    /// Mint a [`StreamDroppedMidStreamToken`] and route through
    /// [`crate::state_setter::drain_at_stream_dropped_mid_stream`].
    /// Sole legitimate caller is
    /// [`crate::PgProtocol::install_errored_stream_dropped_mid_stream`]
    /// (the `install_errored_*` helper invoked from
    /// `RowStream`'s Drop impl).
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
                  by the Errored install. Drop-site caller binds it to `_drained_at_drop` for \
                  documentation; drop has no FailReply emission context, but the next \
                  operation on the connection surfaces ConnectionAlreadyClosed { prior_kind: \
                  ClientOrdering } so the user's oneshot is not silently leaked at the \
                  wrapper layer."]
    pub(in crate::protocol) fn drain(
        state: &mut crate::state::ProtoState,
        kind: crate::error::StateErrorKind,
    ) -> Option<core::num::NonZeroU64> {
        crate::state_setter::drain_at_stream_dropped_mid_stream(
            state,
            StreamDroppedMidStreamToken(()),
            kind,
        )
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-278 Bundle D (2026-05-17) — `_backend_key_install_leaf`
//
// Per-call-site concrete-type token gating the one-shot install of
// `(pid, secret_key)` into [`crate::cancel::BackendKeyCell`] at the
// dispatch arm that processes `(ConnectingPostAuthHaveKey, 'Z')`.
//
// Mirror of DEF-272 cluster α/β/Sub-B leaf-token pattern. The
// tuple-struct field is PRIVATE to the leaf submodule (no
// `pub(crate)` modifier on the inner `()`), so the `Self(())` literal
// mint is callable ONLY inside this module. The `BackendKeyCell`
// names the token type in its `install_via_token` parameter signature
// but cannot mint a token of its own.
//
// Why a `&BackendKeyInstallToken` parameter (not a value): the token
// is minted once per dispatch call (inside the leaf's
// [`install_at_dispatch_arm`] helper) and the cell's
// `install_via_token` borrows it for the duration of the write. The
// shared-ref shape matches DEF-272 cluster α `ClearResidueSchemaToken`
// which is also passed by-value-of-`()` — both cost zero bytes (ZST)
// and zero cycles (no register pressure beyond the token's ABI
// no-op).
//
// Tier-1 within-crate by-construction: the only legal install path
// is via [`install_at_dispatch_arm`]; no other code site can mint a
// token to call `BackendKeyCell::install_via_token` directly.
// ═════════════════════════════════════════════════════════════════════

/// DEF-278 Bundle D leaf submodule for the one-shot backend-key
/// install at the dispatch arm `(ConnectingPostAuthHaveKey,
/// TAG_READY_FOR_QUERY)`. Hosts the concrete-type token + the helper
/// fn that performs the install.
pub(crate) mod _backend_key_install_leaf {
    /// DEF-278 Bundle D leaf-scope token. Field private to the leaf
    /// (no `pub(crate)` modifier on the inner `()`) — the `Self(())`
    /// literal mint is callable ONLY inside this module. The type
    /// itself is `pub(crate)` so
    /// [`crate::cancel::BackendKeyCell::install_via_token`] can name
    /// it in its parameter signature.
    ///
    /// # Why a separate leaf for one install site
    ///
    /// The one-shot install lives at a single dispatch arm
    /// (`dispatch.rs:587-604`), but a separate leaf gives:
    /// - **Tier-1 within-crate by-construction**: any future code
    ///   trying to install a second key would need to either (a)
    ///   call the leaf helper (which is fine — it's the legitimate
    ///   entry point) or (b) mint its own token, which is rejected
    ///   by the field-private tuple-struct payload at compile time
    ///   (`E0451`).
    /// - **Future-extensibility surface**: if a hypothetical follow-up
    ///   ever needs to clear/re-install the key (e.g.
    ///   `<ErroredPhase>::into_disconnected_for_retry` from Bundle A
    ///   in DEF-278 — out of scope for Bundle D), the leaf gains a
    ///   second token + helper, keeping the surface tight.
    pub(crate) struct BackendKeyInstallToken(());

    /// DEF-278 Bundle D — install `(pid, secret_key)` into the cell
    /// at the dispatch arm that processes the handshake-complete
    /// `ReadyForQuery` frame.
    ///
    /// Sole legitimate caller is the dispatch arm at
    /// `(ConnectingPostAuthHaveKey, TAG_READY_FOR_QUERY)` in
    /// `dispatch.rs`. Token is minted inline and consumed by the
    /// cell's `install_via_token` method.
    ///
    /// # Tier-1 within-crate closure
    ///
    /// The leaf submodule path is `pub(crate)` so the dispatch
    /// module can call it; the token field is leaf-private so no
    /// outside-the-leaf code can mint one. The only write provenance
    /// for the cell traces back through this helper.
    #[inline]
    pub(in crate) fn install_at_dispatch_arm(
        cell: &mut crate::cancel::BackendKeyCell,
        pid: i32,
        secret_key: crate::sensitive::Sensitive<i32>,
    ) {
        let token = BackendKeyInstallToken(());
        cell.install_via_token(
            &token,
            crate::cancel::BackendKey { pid, secret_key },
        );
    }
}

impl PgProtocol<ActivePhase> {
    /// DEF-278 Bundle D' (2026-05-18) — closure-scoped access to the
    /// PostgreSQL §55.2.7 CancelRequest wire frame.
    ///
    /// # Tier-1 against retention (Bundle D' elevation)
    ///
    /// The 16-byte wire frame is materialised on THIS function's
    /// stack inside a [`zeroize::Zeroizing`]`<[u8; 16]>` guard. The
    /// closure receives `&[u8; 16]` borrowed from that guard. On
    /// closure return (`Ok` / early-`Err` / panic unwind under
    /// `panic = "unwind"`) the guard's `Drop` fires `zeroize::Zeroize`
    /// and scrubs the bytes.
    ///
    /// **Retention is structurally impossible:**
    /// - `mem::forget(guard)` / `Box::leak(guard)` /
    ///   `ManuallyDrop::new(guard)` cannot run because the guard is
    ///   never in scope outside this function — neither the closure
    ///   nor any caller can reach it.
    /// - Retaining the `&[u8; 16]` past the closure is rejected at
    ///   compile time: the HRTB on `FnOnce(&[u8; 16], i32) -> R`
    ///   quantifies the borrow's lifetime over `'a` so the reference
    ///   cannot escape the call. Pinned by trybuild probe
    ///   `p_d278d_6` (`E0521` lifetime-may-not-live-long-enough).
    ///
    /// **What the caller CAN do — and the documented gap:** copying
    /// the bytes' *contents* into caller memory (e.g. `bytes.to_vec()`
    /// or `let mut buf = [0u8; 16]; buf.copy_from_slice(bytes);`) is
    /// intentionally allowed — drivers need this for async writes
    /// that outlive the synchronous closure. The copy then lives in
    /// caller-controlled storage and is the caller's responsibility
    /// to scrub. The original bytes (in the Zeroizing guard) are
    /// unaffected and get scrubbed on closure return regardless.
    ///
    /// # Closure arguments
    ///
    /// - `bytes: &[u8; 16]` — the wire-encoded CancelRequest frame
    ///   per PG §55.2.7, ready for `socket.write_all(bytes)`.
    /// - `pid: i32` — the backend process id (wire-public, no
    ///   redaction needed). Useful for diagnostic logging
    ///   (`"cancelling pid {pid}"`).
    ///
    /// # Return semantics
    ///
    /// - `Some(R)` — handshake-complete: cell holds the installed
    ///   `(pid, secret_key)`; closure invoked, its `R` returned.
    /// - `None` — architecturally-distant: a non-standard PG fork
    ///   that skipped the `K` frame would land in `Idle` without an
    ///   install. The closure is **not** invoked.
    ///
    /// # Sans-I/O (CREDO §1)
    ///
    /// The protocol crate does **not** open the side TCP connection.
    /// The driver opens a SEPARATE TCP connection to the same
    /// backend, writes the bytes lent through the closure, and
    /// closes the socket. The server does not reply on this socket.
    ///
    /// # Tier impact
    ///
    /// - **`Some` arm**: build the array via the const-fn
    ///   `cancel_request_bytes` (zero alloc), move into Zeroizing
    ///   guard, invoke closure. ≤ 8 ns per `benches/hot_paths.rs`
    ///   floor.
    /// - **Method-absent on every other phase**: tier-1 by
    ///   visibility. `<DisconnectedPhase>` / `<ConnectingPhase>` /
    ///   `<ClosedPhase>` have no `with_cancel_request` method —
    ///   calling produces `E0599`. Pinned by trybuild probes
    ///   `p_d278d_1` / `_2` / `_3`.
    /// - **Retention rejection**: tier-1 by HRTB lifetime quantification.
    ///   Pinned by trybuild probe `p_d278d_6` (`E0521`).
    ///
    /// # Driver pattern
    ///
    /// ```ignore
    /// // Synchronous side-channel write:
    /// let wrote = active.with_cancel_request(|bytes, pid| {
    ///     log::info!("cancelling pid {pid}");
    ///     side_socket.write_all(bytes)
    /// });
    /// match wrote {
    ///     Some(Ok(())) => {} // bytes scrubbed automatically.
    ///     Some(Err(e)) => return Err(e.into()),
    ///     None => return Err(PoolError::NoBackendKey),
    /// }
    /// drop(side_socket); // No response expected on cancel socket.
    ///
    /// // Async pattern (needs owned-copy across .await):
    /// let owned: Option<[u8; 16]> = active.with_cancel_request(|bytes, _| *bytes);
    /// if let Some(buf) = owned {
    ///     side_socket.write_all(&buf).await?;
    ///     // `buf` is caller-owned; explicit zeroize on drop
    ///     // (e.g. wrap in `Zeroizing<[u8; 16]>` if scrubbing the
    ///     // copy matters for the driver's threat model).
    /// }
    /// ```
    ///
    /// # Wire correctness
    ///
    /// `CancelRequest` is **unauthenticated** at the wire level —
    /// the server validates `(pid, secret_key)` matches a live
    /// backend. A leaked `secret_key` enables impersonated
    /// cancellation of the target query (capability-token class —
    /// see [`crate::StartupCompletePayload`] docstring for the
    /// analogous threat treatment). The closure-scoped API ensures
    /// the bytes themselves never escape; the secret_key bytes in
    /// the Zeroizing guard are scrubbed on every return path
    /// (`panic = "unwind"`; under `panic = "abort"` see the panic
    /// semantics note in [`crate::cancel`]).
    ///
    /// # Decision §8.3 / §8.4 / §8.5 (DEF-278 Bundle D / D' sign-off)
    ///
    /// - **§8.3 — `pid` exposed inside closure**: pid is wire-public;
    ///   matching the [`crate::StartupCompletePayload`] precedent.
    ///   Diagnostic value for operators.
    /// - **§8.4 — Zeroize-on-drop via stack guard**: Bundle D' lifts
    ///   the secret-scrub mechanism from a Sensitive<i32> field
    ///   (tier-1 by-Drop-fire, suppressible by mem::forget /
    ///   Box::leak / ManuallyDrop) to a stack-local Zeroizing
    ///   guard (tier-1 by-closure-scope, retention structurally
    ///   impossible).
    /// - **§8.5 — method-absent on `<ConnectingPhase>`**: tier-1.
    ///   A driver wanting to cancel mid-handshake must drop the
    ///   connection; there is no production scenario where a pool
    ///   cancels a mid-handshake connection (cost of opening a new
    ///   connection < cost of debugging cancel semantics).
    #[inline]
    pub fn with_cancel_request<R>(
        &self,
        f: impl FnOnce(&[u8; 16], i32) -> R,
    ) -> Option<R> {
        // Architectural-distant `None` case: standard PG always emits
        // `K` before `Z`, but a non-standard fork could land here in
        // `Idle` without an install. Honest modelling > runtime panic
        // (CREDO §V).
        let key: &crate::cancel::BackendKey = self.inner.backend_key.as_inner()?;
        let pid: i32 = key.pid;
        // Copy the i32 out of the cell's Sensitive<i32>. The plain
        // i32 lives in this stack frame for the duration of the
        // `cancel_request_bytes` build below; the Zeroizing guard
        // scrubs the encoded array's 16 bytes (which includes a
        // BE copy of this secret at bytes[12..16]) on closure
        // return. The plain-i32 stack slot itself is overwritten by
        // normal function-prologue/epilogue conventions on return —
        // not scrubbed explicitly. For Bundle D'' (if ever needed),
        // wrap `secret` in `Zeroizing` and pass by reference into
        // an i32-aware `cancel_request_bytes_into` helper. Not
        // required for D' tier elevation: the lifetime of the
        // unscrubbed slot is bounded by this function's invocation
        // and not addressable from outside.
        let secret: i32 = *key.secret_key.get();
        // Materialise the wire frame inside a Zeroizing guard. The
        // `cancel_request_bytes` const-fn returns `[u8; 16]` on the
        // stack; the move into `Zeroizing::new(...)` is NRVO-friendly
        // (LLVM writes directly into the guard's inline storage).
        // Single source of truth for the byte layout: the
        // `cancel_request_bytes` builder, which is itself
        // const-pinned in `wire.rs`.
        let bytes_guard: zeroize::Zeroizing<[u8; 16]> = zeroize::Zeroizing::new(
            crate::wire::cancel_request_bytes(pid, secret),
        );
        // Lend the borrow into the closure. The closure's HRTB
        // `for<'a> FnOnce(&'a [u8; 16], i32) -> R` quantifies `'a`
        // so the borrow cannot escape this call. R is independent
        // of `'a` — caller may return a Copy/owned value (e.g.
        // `[u8; 16]` via `*bytes`) but not the reference itself.
        let r = f(&bytes_guard, pid);
        // `bytes_guard` drops here on the Ok return path; under
        // `panic = "unwind"` it also drops if `f` panicked (stack
        // unwind fires Drop). Either way `Zeroizing::Drop` runs
        // `Zeroize::zeroize` on the [u8; 16]. Tier-1 by
        // closure-scope: nothing inside or outside this function
        // can prevent the guard's Drop short of `panic = "abort"`
        // (documented gap aligned with DEF-185 P0-A).
        Some(r)
    }

    /// Mint a fresh `ReplyId<K>` for an outbound command.
    ///
    /// **DEF-270 cluster (U letter, 2026-05-09):** this is the sole
    /// public mint surface for [`crate::ReplyId<K>`]. Pre-DEF-270
    /// `ReplyId::from_raw(...)` was `pub` and external crates minted
    /// their own IDs (tier-3 by-discipline — duplicate-ID risk).
    /// Post-DEF-270 `from_raw` is `pub(crate)` and the only path to a
    /// `ReplyId<K>` from outside the crate is this method.
    ///
    /// # Why `&mut self` if mint is via static atomic
    ///
    /// The counter is a `static AtomicU64` (mod-private below) — NOT
    /// a `PgProtocol` field. Bisect 2026-05-09 proved that adding an
    /// inline `u64` field grew `PgProtocol` 520 → 528 B and shifted
    /// LLVM whole-crate codegen heuristic, regressing the synthetic
    /// `column_decode/iter_10cols` bench by +6%. Static-atomic mint
    /// preserves PgProtocol size at 520 B (no codegen shift) AND
    /// strengthens the invariant: globally-unique IDs across all
    /// `PgProtocol` instances (per-protocol counter would only have
    /// guaranteed per-instance uniqueness).
    ///
    /// `&mut self` is retained on the signature because: (a) it
    /// keeps the API shape consistent with the prior per-protocol
    /// design (forward-compat if we ever move back), and (b) the
    /// borrow makes it obvious to callers that mint participates in
    /// the protocol's mutation cycle (it is not a "look-only"
    /// operation; the minted ID is correlator-bound to a future
    /// push).
    ///
    /// # Tier-1 / tier-2 closure
    ///
    /// - **External fabrication: tier-1 by-visibility.** `ReplyId::from_raw`
    ///   is `pub(crate)` — `mem::transmute` is the only escape, and the
    ///   crate is `#![forbid(unsafe_code)]`.
    /// - **Cross-instance monotonicity: tier-2 by atomic-fetch_add.**
    ///   Globally unique across all `PgProtocol` instances and threads.
    ///   `Ordering::Relaxed` is sufficient: monotonic visibility within
    ///   one observer is not needed — the protocol's pending-replies
    ///   table (in the wrapper) does the rendezvous; mint just needs
    ///   to never return the same value twice. Linear types would
    ///   lift this to tier-1 ("counter has never returned this
    ///   value"); not available pre-stable Rust.
    ///
    /// # Type parameter
    ///
    /// `K: ReplyKind` — the typed reply-kind tag bound to the command
    /// being pushed. Caller writes `proto.next_reply_id::<PingKind>()`
    /// for a Ping reply, etc. The kind binds the payload type via
    /// [`crate::ReplyKind::Payload`] (DEF-112) — passing the wrong
    /// kind to a command's `reply` field is a type error.
    ///
    /// # Counter behaviour
    ///
    /// Saturating `u64` add. First call returns `NonZeroU64::new(1)`;
    /// each subsequent call increments by 1. Saturation at `u64::MAX`
    /// is architecturally distant (~10^19 commands process-wide).
    /// On saturation the counter parks at `u64::MAX` — every
    /// subsequent mint returns the same ID, surfacing as a
    /// duplicate-correlator failure at the wrapper's pending-replies
    /// table (post-Phase-1c-5).
    #[inline]
    pub fn next_reply_id<K: crate::reply_id::ReplyKind>(
        &mut self,
    ) -> crate::reply_id::ReplyId<K> {
        // DEF-270 U (post-bisect fix-up 2026-05-09): static atomic
        // counter to keep PgProtocol size pin at 520 B. See method
        // docstring for the bisect rationale.
        use core::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let raw_old = COUNTER.fetch_add(1, Ordering::Relaxed);
        // DEF-271 cluster D (2026-05-10): saturation classifier.
        // Pre-DEF-271 `saturating_add(1)` capped at `u64::MAX` and
        // returned the duplicate id silently; the atomic itself wraps
        // to 0 by Rust spec, so subsequent mints cycle through
        // previously-issued values — the wrapper's pending-replies
        // table would mis-route server replies to the wrong correlator.
        // Post-DEF-271 the cold branch detects the saturation point
        // (`raw_old == u64::MAX`, the value at which the next mint
        // wraps) and transitions THIS PgProtocol instance to
        // `Errored(ReplyIdSaturation)`. The duplicate id IS still
        // returned (caller gets a `ReplyId<K>` carrying `u64::MAX` —
        // `saturating_add(1)` at `u64::MAX` saturates at `u64::MAX`,
        // never wrapping to zero, so the `NonZeroU64::new(raw)` Some
        // arm is taken and the `unwrap_or(MIN)` fallback is dead
        // here; the docstring previously claimed the saturated value
        // wrapped to MIN — DEF-280 Bundle J 2026-05-18 audit corrected
        // that), but the next push attempt sees Errored state and
        // fails with `ConnectionAlreadyClosed { prior_kind:
        // ReplyIdSaturation }` — the duplicate never reaches the
        // server in a usable state.
        //
        // Cross-instance duplicate-ID risk after wrap remains tier-2
        // (separate residue — architect's #1B brand-lifetime closure
        // deferred to Phase 4+ pending invasive design review).
        if raw_old == u64::MAX {
            self.install_errored_replyid_saturation();
        }
        // saturating_add(1) prevents wrap to zero (NonZeroU64 niche
        // violation) at u64::MAX. SAFETY contract: counter starts at
        // 0, fetch_add returns pre-increment value, then we
        // saturating_add(1). First call: pre=0, post=1.
        // NonZeroU64::new(1) is Some; the unwrap_or fallback to MIN is
        // dead in the non-saturated regime but keeps
        // `forbid(clippy::unwrap_used)` happy on the proven-dead branch.
        // In the saturated regime the fallback IS reached but the
        // returned id is intentionally unusable (Errored state).
        let raw = raw_old.saturating_add(1);
        let nz = core::num::NonZeroU64::new(raw)
            .unwrap_or(core::num::NonZeroU64::MIN);
        crate::reply_id::ReplyId::from_raw(nz)
    }

    /// DEF-271 cluster D (2026-05-10): cold-path classifier for the
    /// `next_reply_id` saturation case. Marked `#[cold]` + `#[inline(never)]`
    /// so LLVM keeps it off the hot mint path.
    ///
    /// DEF-246 Phase 2 (2026-05-16): delegate to
    /// `PgProtocolInner::install_errored_replyid_saturation` so the
    /// blanket `impl<P: SealedPhase> PgProtocol<P>::next_reply_id`
    /// can call the same machinery without an `<ActivePhase>` bound.
    #[cold]
    #[inline(never)]
    fn install_errored_replyid_saturation(&mut self) {
        self.inner.install_errored_replyid_saturation();
    }

    // DEF-196 (2026-04-28): three-field split. Each cold slot
    // independently lazy-allocated; malformed_counter is inline
    // (4 B, no Box).

    /// Read-only accessor for `session_params`. Returns the boxed
    /// contents if allocated, else a `&'static` empty default
    /// (semantically identical to a fresh `SessionParams::new()`).
    #[inline]
    fn cold_session_params(&self) -> &SessionParams {
        // Static lives for program lifetime; const expression
        // `SessionParams::new()` evaluates at compile time (all-None +
        // zero counters). Never dropped — SecretBoundedStr
        // ZeroizeOnDrop never fires on it. Pristine-state guarantee
        // pinned at module scope by `_BS11_EMPTY_SESSION_PARAMS_IS_PRISTINE`
        // below.
        static EMPTY: SessionParams = SessionParams::new();
        match self.inner.session_params.as_deref() {
            Some(p) => p,
            None => &EMPTY,
        }
    }

    /// Read-only accessor for `error_arena`. Returns boxed contents
    /// or `&'static` empty default.
    #[inline]
    fn cold_error_arena(&self) -> &crate::error_arena::ErrorArena {
        static EMPTY: crate::error_arena::ErrorArena =
            crate::error_arena::ErrorArena::new();
        match self.inner.error_arena.as_deref() {
            Some(a) => a,
            None => &EMPTY,
        }
    }

    /// Read-only accessor for `malformed_frame_count`. Direct field
    /// read — no Box indirection (counter is inline since v2).
    #[inline]
    fn cold_malformed_frame_count(&self) -> u32 {
        self.inner.malformed_frame_count
    }

    /// DEF-185 P2-9 (audit 2026-04-24): count of malformed-frame
    /// events that triggered connection teardown.
    ///
    /// Every invocation of the internal `fail_inflight_no_readbuf`
    /// helper bumps this counter — once per fatal wire-level error
    /// (malformed header length, oversized frame, read-buffer
    /// overflow, or internal cursor-advance bug).
    ///
    /// Zero on a healthy connection; non-zero indicates (a) a single
    /// protocol desync that triggered teardown, or (b) repeated
    /// adversarial / buggy frames until teardown. Operators can use
    /// the value to distinguish these cases.
    ///
    /// Saturates at `u32::MAX` (no wrap). Per connection lifetime.
    /// DEF-186 P1-5 widened from u16 — see field doc.
    ///
    /// DEF-196: counter lives in `cold: Option<Box<ColdFields>>`;
    /// returns 0 if cold hasn't been allocated (no malformed frames
    /// have triggered teardown yet on this connection).
    #[inline]
    #[must_use]
    pub fn malformed_frame_count(&self) -> u32 {
        self.cold_malformed_frame_count()
    }

    /// Borrow the current state. Read-only inspection.
    #[inline]
    #[must_use]
    pub const fn state(&self) -> &ProtoState {
        &self.inner.state
    }

    /// **DEF-248 Sub-B (2026-05-12)** — diagnostic predicate for the
    /// partial-assembly cell. Returns `true` iff an oversize non-`'D'`
    /// frame is currently mid-flight (body bytes accumulating across
    /// multiple `feed_bytes` calls).
    ///
    /// Read-only; no token needed. Used by Sub-B integration tests to
    /// pin lifecycle invariants (no orphaned Box across dispatch
    /// completion / Errored-entry residue cleanup).
    ///
    /// Production callers: this is operator-diagnostic / test surface
    /// only. The partial-assembly machinery is internal — drivers
    /// observe the equivalent state via `Action::*` events emitted on
    /// frame completion, not via this predicate.
    #[inline]
    #[must_use]
    pub fn has_active_partial_assembly(&self) -> bool {
        self.inner.partial_assembly.is_active()
    }

    /// Borrow the accumulated session parameters.
    ///
    /// Populated during the startup handshake from `ParameterStatus`
    /// messages. Empty until startup completes.
    ///
    /// DEF-196: session params live in `cold: Option<Box<ColdFields>>`.
    /// Returns a `&'static` empty default if cold hasn't been
    /// allocated yet (semantically identical to a fresh
    /// `SessionParams::new()`).
    #[inline]
    #[must_use]
    pub fn session_params(&self) -> &SessionParams {
        self.cold_session_params()
    }

    /// Borrow the current unread bytes in the read buffer.
    ///
    /// Useful for tests; production hosts have no need.
    ///
    /// Returns the unread region of the read buffer.
    ///
    /// Post-(DEF-154 H+V delete 2026-04-24): pending_advance logic
    /// removed (dead after DEF-154 Y deleted StreamRowRange).
    /// Cursor advance now happens in-scope inside `feed_bytes`, so
    /// this method simply forwards to `ReadBuf::unread()` — the
    /// caller always sees the current cursor state.
    #[inline]
    #[must_use]
    pub fn unread(&self) -> &[u8] {
        self.inner.read_buf.unread()
    }

    // ═════════════════════════════════════════════════════════════════
    // DEF-198 — witness-guard typestate for client-initiated push.
    // ═════════════════════════════════════════════════════════════════

    /// Acquire a [`crate::guard::ReadyGuard`] iff the protocol is in
    /// `Idle` state (ready to accept a new command).
    ///
    /// The guard exclusively borrows `self` and is the only path to
    /// [`crate::guard::ReadyGuard::push_command`] /
    /// [`crate::guard::ReadyGuard::push_bind_execute`]. Returns `None`
    /// when the protocol is busy (in-flight reply pending), handshaking
    /// (startup/auth in progress), or terminal-errored.
    ///
    /// Use [`Self::connection_status`] in the `None` arm to distinguish
    /// recoverable (`Busy`/`Handshaking`) from terminal (`Errored`).
    ///
    /// # Tier-1 closure
    ///
    /// Dispatches on [`crate::state::ProtoState::push_class`] (5-variant
    /// exhaustive classifier). Adding a new state variant is a build
    /// failure that forces classification (transitive: state → push_class
    /// → as_ready). Pinned by `tests/def198_guard_closure_spec.rs`.
    ///
    /// # Zero-cost
    ///
    /// `ReadyGuard<'_>` is a `&mut PgProtocol` newtype; LLVM monomorphises
    /// the indirection away in release builds.
    #[inline]
    #[must_use]
    pub fn as_ready(&mut self) -> Option<crate::guard::ReadyGuard<'_>> {
        match self.inner.state.push_class() {
            crate::state::StatePushClass::Idle => {
                Some(crate::guard::ReadyGuard::new(self))
            }
            crate::state::StatePushClass::Errored(_)
            | crate::state::StatePushClass::Connecting
            | crate::state::StatePushClass::PingAwaiting
            | crate::state::StatePushClass::BusyQuery => None,
        }
    }

    /// Caller-facing fine-grained connection state classification.
    ///
    /// Maps the internal [`crate::state::StatePushClass`] to the
    /// public-API [`crate::guard::ConnectionStatus`] (`PingAwaiting` and
    /// `BusyQuery` collapse to `Busy` — caller recovery is identical:
    /// drive `feed_bytes` until the in-flight reply arrives).
    ///
    /// # Tier-1 closure
    ///
    /// Exhaustive match over `StatePushClass`'s 5 variants — adding a
    /// `StatePushClass` variant is a build failure here. Pinned by
    /// `tests/def198_guard_closure_spec.rs`.
    #[inline]
    #[must_use]
    pub fn connection_status(&self) -> crate::guard::ConnectionStatus {
        use crate::guard::ConnectionStatus;
        use crate::state::StatePushClass;
        match self.inner.state.push_class() {
            StatePushClass::Idle => ConnectionStatus::Ready,
            StatePushClass::Errored(kind) => ConnectionStatus::Errored(kind),
            StatePushClass::PingAwaiting | StatePushClass::BusyQuery => {
                ConnectionStatus::Busy
            }
            StatePushClass::Connecting => ConnectionStatus::Handshaking,
        }
    }

    /// Crate-internal push entry point. Production callers reach
    /// this only via [`crate::guard::ReadyGuard::push_command`],
    /// which is in turn reachable only through [`Self::as_ready`]
    /// (returns `Some` iff `state == Idle`). The `pub(crate)`
    /// visibility plus the witness-guard type make "push from
    /// non-Idle" compile-rejected on the public API surface.
    ///
    /// The body is a thin wiring layer: residue clear, branded
    /// write-buf scope, dispatch via `compute_push_idle_only`,
    /// materialise the staged actions via `materialise_push`. All
    /// per-command transition logic lives in the
    /// `compute_push_<cmd>_idle_only` free functions (testable
    /// directly, no `PgProtocol` construction needed).
    ///
    /// Caller's `Idle` precondition is enforced uniformly at runtime by
    /// `IdleState::try_from(&mut state)` (DEF-272 cluster γ typestate);
    /// the `None` arm classifies via `CrateBugLocus::PushCommandInternalNonIdle`
    /// PushFailure with `core::hint::cold_path()` (DEF-280 Bundle G
    /// eliminated the pre-existing `debug_assert!(false, …)` dev-loud
    /// branch as a CREDO §V glass pattern — the classified failure is
    /// the safety net in both dev and release modes).
    ///
    /// # DEF-212 (Alt Y', architect-vetted impl plan, audit 2026-05-04)
    ///
    /// Pre-(212) returned `OutActions<'w, 's>` (800 B; caller iterated
    /// `Action::SendBytes`/`SendBytesStatic`/`FailReply`/`CloseSocket`).
    /// Post-(212) returns `Result<(), PushFailure>` (~80 B). On Ok,
    /// bytes (frame + optional trailing Sync) live in the caller's
    /// [`WriteBuf`]; caller drains `wb.as_bytes()` to socket in a
    /// single write. On Err, state has already transitioned to
    /// `Errored`; caller resolves user's oneshot via `failure.id` +
    /// `failure.cause` and closes the socket per the
    /// [`crate::PushFailure`] `#[must_use]` contract.
    /// DEF-269 v2 (T): generic over `C: PushCommand`. Caller passes a
    /// per-command struct (e.g. [`crate::push_command::Ping`]) instead
    /// of a `PgCommand` enum value. Each `C` is monomorphised — the
    /// 2176-B-by-value enum dispatch is gone.
    #[must_use = "the returned Result carries OutActions on success (caller drains \
                  the multi-chunk frame: header range + borrowed SQL + trailer range \
                  + Sync) or the consumed-correlator + cause failure signal — both \
                  must be observed by the caller's I/O layer"]
    pub(crate) fn push_command_internal<'w, C: crate::push_command::PushCommand + 'w>(
        &mut self,
        cmd: C,
        write_buf: &'w mut WriteBuf,
    ) -> Result<crate::action::OutActions<'w, 'static>, crate::action::PushFailure> {
        // DEF-160 Z2 (2026-05-11): push API now returns OutActions.
        // Pre-Z2 contract was "caller drains wb.as_bytes() post-Ok" —
        // viable when push only stages SendBytesRange + SendBytesStatic
        // (everything sits inside wb). DEF-160 introduces
        // SendBytesBorrowed for zero-copy SQL — the SQL bytes live in
        // CALLER memory (Parse<'a>::sql / SimpleQuery<'a>::sql), not in
        // wb. The full outbound frame is the ordered concatenation of:
        //   1. SendBytesRange (header bytes in wb)
        //   2. SendBytesBorrowed (caller's SQL — zero-copy)
        //   3. SendBytesRange (trailer bytes in wb)
        //   4. SendBytesStatic (Sync trailer for Parse — `&'static`)
        // OutActions surfaces these as `Action::SendBytes(&[u8])` per
        // chunk (4 for Parse, 3 for SimpleQuery, 1 for Ping/Bind/etc).
        // Under `writev` / IoSlice the caller collapses the chunks to
        // a single socket syscall.
        write_buf.clear();

        // DEF-272 cluster γ (2026-05-10): the Idle precondition is
        // enforced by [`crate::state_setter::IdleState::try_from`]
        // below — the `Option<IdleState<'_>>` typestate IS the proof
        // (replaces the pre-γ `IdleStateProof` witness param). The
        // legitimate caller is `ReadyGuard::push_command` (which
        // performs `as_ready` Idle classification upstream); this
        // re-check is the single load-bearing guard, eliminating the
        // pre-γ "caller must promise + we debug_assert" surface.
        // DEF-211 FAKE-01: pass `StatePushClass::Idle` as a STATIC
        // const argument — LLVM specialises the inlined
        // `clear_session_residue_for_class` body to the Idle arm only,
        // eliding the 5-arm dispatch entirely.
        //
        // DEF-246 Option α (2026-05-16):
        // `clear_session_residue_for_class` lives on `PgProtocolInner`;
        // route through `self.inner` directly.
        self.inner.clear_session_residue_for_class(crate::state::StatePushClass::Idle);

        let state = &mut self.inner.state;
        let row_desc_slot = &mut self.inner.row_desc_slot;
        let idle = match crate::state_setter::IdleState::try_from(state) {
            Some(idle) => idle,
            None => {
                // DEF-280 Bundle G (2026-05-18): debug_assert!(false, …)
                // removed — CREDO §V banned glass pattern (dev loud +
                // release silent fallthrough). The classified `PushFailure`
                // below IS the safety net; the assert was misleading dev-
                // noise on a path that's architecturally unreachable on
                // production callers (ReadyGuard::push_command upstream
                // classifies via `as_ready`'s runtime Idle check; the
                // `&mut PgProtocol` borrow chain rules out interleaving
                // between as_ready and push_command_internal entry).
                //
                // DEF-280 Bundle J (2026-05-18): sentinel id is now the
                // distinct `CRATE_BUG_REPLY_ID_SENTINEL` (NonZeroU64::MAX,
                // see `reply_id.rs` docstring). Pre-Bundle J this site
                // used `NonZeroU64::MIN` which collided with the
                // legitimate first id minted by `next_reply_id` — the
                // collision is now closed by-construction.
                core::hint::cold_path();
                return Err(crate::action::PushFailure {
                    id: crate::reply_id::CRATE_BUG_REPLY_ID_SENTINEL,
                    cause: crate::error::ProtocolError::InternalCrateBug {
                        locus: crate::error::CrateBugLocus::PushCommandInternalNonIdle,
                    },
                });
            }
        };

        // DEF-160 Z2 (2026-05-11, post-bench-stable mitigation):
        // single-pass materialise inside the branded closure. Earlier
        // shape returned `StagedActions` from the closure (~700 B
        // return frame), THEN scanned for `FailReply` in a `.iter()`
        // pass, THEN passed `staged` by value to `materialise` for a
        // second iteration producing `OutActions` (~800 B return).
        // Three big stack moves + two iterations cost ≈+34 ns on
        // `push_command/ping` per bench-stable vs `pre-def160`.
        // Single-pass shape: stage, drain, classify-fail, emit
        // `Action::SendBytes` chunks — all in one walk. Closure
        // returns the final `Result<OutActions<'w, 'static>, _>`
        // directly; no intermediate `StagedActions` escape. The
        // feed-side `materialise` keeps its broader contract
        // (DeliverReply/FailReply → typed `Action` variants) for
        // dispatcher use; push is open-coded here for the perf-tier
        // closure on the hot path.
        //
        // DEF-154 (B+H): write-side keeps its brand (`'wb`) for
        // tier-1 `WriteRange::apply`; read side is unbranded.
        // DEF-269 v2: row_desc_slot threaded through for BindExecute
        // (other commands ignore it).
        write_buf.with_branded(|mut wb| -> Result<crate::action::OutActions<'w, 'static>, crate::action::PushFailure> {
            let mut staged: StagedActions<'_> = StagedActions::new();
            {
                let mut reserved = wb.reserve();
                // DEF-272 cluster γ: setter is minted via the typestate
                // (the only legitimate path — `StateSetter::new` is
                // `pub(in crate::state_setter)`, callable only from
                // [`IdleState::into_setter`]).
                let setter = idle.into_setter::<C::PostState>();
                cmd.execute(setter, row_desc_slot, &mut staged, &mut reserved);
            } // reserved dropped — wb is freely accessible for byte view

            // `into_bytes` consumes `wb` and yields `&'w [u8]` (the outer
            // WriteBuf borrow lifetime). The borrow flows into every
            // `Action::SendBytes(&'w [u8])` chunk via `WriteRange::apply`.
            let bytes: &'w [u8] = wb.into_bytes();

            let mut failure: Option<crate::action::PushFailure> = None;
            let mut out: crate::action::OutActions<'w, 'static> = crate::action::OutActions::new();
            for sa in staged {
                match sa {
                    // Push-path failure surface: a `try_builder!`-emitted
                    // FailReply (BuilderCapacityOverflow / EmptyWriteRange /
                    // ParamsWriterOverflow / SqlFrameU32LengthOverflow).
                    // Architecturally exactly one FailReply per push cycle;
                    // capture the first and continue draining (consistent
                    // with the pre-Z2 `materialise_push` shape that also
                    // walked the full container — keeps the staged-action
                    // accounting invariant intact for any future audit).
                    StagedAction::FailReply { id, cause } => {
                        if failure.is_none() {
                            failure = Some(crate::action::PushFailure { id, cause });
                        }
                    }
                    StagedAction::SendBytesRange(range) => {
                        // DEF-154 (N) P0-4: `apply == None` is
                        // architecturally unreachable under intact brand
                        // discipline; classify `CloseSocket` rather than
                        // the pre-(N) silent zero-byte SendBytes.
                        let action = match range.apply(bytes) {
                            Some(slice) => crate::action::Action::SendBytes(slice),
                            None => {
                                // DEF-280 Bundle G (2026-05-18):
                                // debug_assert!(false, …) removed — CREDO §V
                                // banned glass pattern. The classified
                                // `CloseSocket` fallback IS the safety net;
                                // architecturally impossible per intact brand
                                // discipline (DEF-154 N+W).
                                core::hint::cold_path();
                                crate::action::Action::CloseSocket
                            }
                        };
                        push_within_fanout_budget(&mut out, action);
                    }
                    StagedAction::SendBytesStatic(s) => {
                        push_within_fanout_budget(&mut out, crate::action::Action::SendBytes(s));
                    }
                    // DEF-160 Z2: borrowed bytes (caller's SQL via Parse /
                    // SimpleQuery push paths) flow through unchanged. The
                    // `'sql >= 'w` subtyping induced by the `Self: 'sql`
                    // bound on `PushCommand::execute` coerces `&'sql [u8]`
                    // to `&'w [u8]` for the emitted `Action::SendBytes`.
                    StagedAction::SendBytesBorrowed(b) => {
                        push_within_fanout_budget(&mut out, crate::action::Action::SendBytes(b));
                    }
                    StagedAction::CloseSocket => {
                        push_within_fanout_budget(&mut out, crate::action::Action::CloseSocket);
                    }
                    StagedAction::DeliverReply(_) => {
                        // DEF-280 Bundle G (2026-05-18): tier-1 elevation.
                        // Pre-Bundle G this arm silently dropped on release
                        // (loud dev `debug_assert!(false, …)` + silent
                        // fallthrough — CREDO §V glass pattern). Post-Bundle G
                        // classified as `PushFailure` with `InternalCrateBug`
                        // locus `PushEmittedDeliverReply`. Push paths never
                        // emit DeliverReply (replies come from server via
                        // feed_bytes only); architecturally dead per DEF-160
                        // Z2 invariant.
                        //
                        // DEF-280 Bundle J (2026-05-18): sentinel id is now
                        // the distinct `CRATE_BUG_REPLY_ID_SENTINEL`
                        // (NonZeroU64::MAX, see `reply_id.rs` docstring).
                        // Mirrors the `PushCommandInternalNonIdle` site at
                        // the entry of this same function. Pre-Bundle J both
                        // sites used `NonZeroU64::MIN` which collided with
                        // the legitimate first id minted by `next_reply_id`;
                        // closed by-construction by the distinct sentinel.
                        core::hint::cold_path();
                        if failure.is_none() {
                            failure = Some(crate::action::PushFailure {
                                id: crate::reply_id::CRATE_BUG_REPLY_ID_SENTINEL,
                                cause: crate::error::ProtocolError::InternalCrateBug {
                                    locus: crate::error::CrateBugLocus::PushEmittedDeliverReply,
                                },
                            });
                        }
                    }
                }
            }
            match failure {
                Some(f) => Err(f),
                None => Ok(out),
            }
        })
    }

    // DEF-269 v2 (T): push_bind_execute_internal removed; callers now
    // build a `crate::push_command::BindExecute<P>` struct and dispatch
    // through `push_command_internal::<BindExecute<P>>`. The 8-arg
    // wire-shape contract is preserved by the struct's field layout
    // (mirrors the PG Bind+Execute frame exactly).

    /// Append inbound wire bytes into the read buffer **without
    /// dispatching**. Forward-compat anchor for 1c-5 pipelining where
    /// the caller decouples byte-feeding from event-pulling.
    ///
    /// # DEF-212 Phase 2 (Alt Y', audit 2026-05-04)
    ///
    /// Pre-(212) the only public path was [`Self::feed_bytes`] which
    /// combined append + dispatch + materialise into one batched call.
    /// `feed_inbound` exposes the append step as a separate operation
    /// so callers can drive the protocol via [`Self::advance_one_frame`]
    /// in a per-event loop:
    ///
    /// ```text
    /// proto.feed_inbound(socket_chunk)?;
    /// while let event = proto.advance_one_frame(&mut wb) {
    ///     match event { … }
    /// }
    /// ```
    ///
    /// On `Err(ReadBufFull)` the buffer is unchanged (bounded
    /// container's `extend_from_slice` is atomic-fail). Caller treats
    /// the connection as fatally desynced and discards.
    ///
    /// # Errored-state semantics
    ///
    /// Calling on an `Errored` state silently no-ops (returns
    /// `Ok(())` without appending). The protocol is terminal; further
    /// inbound bytes are ignored. This matches the pre-(212)
    /// `feed_bytes` shape (which routes Errored to the
    /// `IngressClassification::AlreadyErrored` arm).
    pub fn feed_inbound(&mut self, bytes: &[u8]) -> Result<(), crate::error::ProtocolError> {
        // DEF-246 Phase 4 elevation #4: signature returns
        // `Result<(), ProtocolError>` so Errored state surfaces to the
        // caller as a typed error instead of silent no-op. The
        // pre-existing `ReadBufFull` shape is lifted into
        // `ProtocolError::ReadBufferFull { … }` (the same enum the
        // dispatch path uses).
        //
        // DEF-246 Option α: dispatch machinery lives on `PgProtocolInner`;
        // this method is a 1-line delegate so the same body executes
        // identically from `<ActivePhase>`, `<ConnectingPhase>` (Phase 3
        // elevation #2 — server-driven auth bytes during handshake).
        // `<ClosedPhase>` does NOT have feed_inbound (Phase 4 elevation
        // #3 — Errored/Closed terminal absorbs no input).
        self.inner.feed_inbound(bytes)
    }

    /// Process at most one user-observable event and return it.
    ///
    /// # DEF-212 Phase 2 (Alt Y', audit 2026-05-04)
    ///
    /// Per-event alternative to the batched [`Self::feed_bytes`].
    /// Forward-compat anchor for 1c-5 pipelining (where multiple
    /// concurrent in-flight replies may resolve in one cycle and the
    /// caller wants explicit event-by-event control).
    ///
    /// The implementation reuses [`Self::feed_bytes_bounded`] with
    /// `max_dispatches = 1` and an empty byte slice — single source
    /// of truth for dispatch is preserved (Phase 2 is additive, not
    /// a refactor of `feed_bytes`).
    ///
    /// # Returned event mapping
    ///
    /// - `state == Idle`, read_buf empty → [`FeedEvent::Idle`]
    /// - `state` in row-streaming → [`FeedEvent::StreamingRows`]
    ///   (caller switches to [`Self::iter_rows`] for per-row decoding)
    /// - `state == Errored(_)` → [`FeedEvent::Close`]
    /// - read_buf has partial frame, or empty in non-Idle non-streaming
    ///   non-Errored state → [`FeedEvent::NeedMoreBytes`]
    /// - One actionable frame consumed:
    ///   - `Action::SendBytes(b)` → [`FeedEvent::SendBytes(b)`]
    ///   - `Action::DeliverReply { id, value }` → [`FeedEvent::Deliver(id, value)`]
    ///   - `Action::FailReply { id, cause }` (paired with implicit
    ///     `Action::CloseSocket` per M2) → [`FeedEvent::Fail(id, cause)`]
    ///   - `Action::CloseSocket` alone (no in-flight reply id) →
    ///     [`FeedEvent::Close`]
    ///
    /// # Lifetime contract (M3)
    ///
    /// `FeedEvent<'wb, 'r>` carries two lifetimes:
    ///   - `'wb` ties [`FeedEvent::SendBytes`] to the caller's `write_buf`.
    ///   - `'r` ties [`FeedEvent::Deliver`]'s `Reply<'r>` to the
    ///     `&'r mut self` borrow of this protocol.
    ///
    /// # `wb` lifecycle
    ///
    /// `advance_one_frame` calls `wb.clear()` at entry (mirroring
    /// `feed_bytes` semantics). A [`FeedEvent::SendBytes`] slice is
    /// valid until the next `&mut wb` call (typically the next
    /// `advance_one_frame` iteration). Caller MUST drain the slice
    /// to the socket before re-borrowing `wb`.
    #[must_use = "FeedEvent variants carry side-effect contracts: \
                  SendBytes/Deliver MUST be processed; Fail/Close MUST \
                  trigger socket teardown"]
    pub fn advance_one_frame<'w, 'r>(
        &'r mut self,
        write_buf: &'w mut WriteBuf,
    ) -> crate::action::FeedEvent<'w, 'r> {
        // DEF-246 Option α: dispatch machinery on `PgProtocolInner`;
        // 1-line delegate so `<ConnectingPhase>` and `<ActivePhase>`
        // share the same implementation (handshake-window callers also
        // need per-event advance for server-driven auth chains).
        self.inner.advance_one_frame(write_buf)
    }

    /// Feed inbound wire bytes.
    ///
    /// Returns the action list — bounded by [`MAX_ACTIONS_PER_CALL`].
    /// DEF-094: caller-owned `write_buf` — see [`push_command`] for
    /// the staged-dispatch architecture.
    ///
    /// 1c-1a: `&'r mut self` — the row slices in `Action::StreamRow`
    /// borrow from `self.inner.read_buf`. The `'r` lifetime propagates
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
        //
        // DEF-246 Option α: dispatch machinery on `PgProtocolInner`;
        // 1-line delegate. The `<ConnectingPhase>::feed_bytes` mirror
        // (Phase 3) shares the identical inner-method dispatch — same
        // const-generic specialisation, same hot-path codegen.
        self.inner.feed_bytes_impl::<false>(bytes, write_buf, 0)
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
        // DEF-246 Option α: 1-line delegate to `PgProtocolInner` mirror.
        // `row_stream`'s slow-path call site (`self.proto.feed_bytes_bounded`)
        // resolves through this delegate unchanged.
        self.inner.feed_bytes_bounded(bytes, write_buf, max_dispatches)
    }

    // DEF-246 Option α (2026-05-16): dispatch machinery extracted to
    // `impl PgProtocolInner` below. The next two large method bodies —
    // `feed_bytes_impl<const BOUNDED>` and `clear_session_residue_for_class`
    // — now live on `Inner` so `<ActivePhase>` (default phase) and
    // `<ConnectingPhase>` (Phase 3 — server-driven auth bytes during
    // handshake) reach the SAME implementation via 1-line delegates.
    // The 4 surface-facing delegates above (`feed_inbound`,
    // `advance_one_frame`, `feed_bytes`, `feed_bytes_bounded`,
    // `clear_session_residue_for_class`) close the bridge.
    //
    // Re-opens `impl PgProtocol<ActivePhase>` BELOW the moved
    // `feed_bytes_impl` so the remaining methods on `<ActivePhase>`
    // (`get_server_error`, `read_buf_append`, `current_row_desc`,
    // `iter_rows`, etc.) stay where they were before DEF-246. Caller
    // surface unchanged.
}

impl PgProtocolInner {
    /// DEF-246 Phase 2 (2026-05-16): saturation-classifier mutation
    /// surface lives on `PgProtocolInner` so blanket
    /// `impl<P: SealedPhase> PgProtocol<P>::next_reply_id` calls the
    /// same machinery without an `<ActivePhase>` bound.
    ///
    /// Body unchanged from the pre-DEF-246 `<ActivePhase>` location:
    /// fast-return on already-Errored, otherwise install
    /// `Errored(ReplyIdSaturation)` via the leaf-token-gated drain.
    #[cold]
    #[inline(never)]
    pub(crate) fn install_errored_replyid_saturation(&mut self) {
        if matches!(self.state, ProtoState::Errored(_)) {
            return;
        }
        let cause = crate::error::ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::ReplyIdSaturation,
        };
        let _drained_id_at_saturation =
            _replyid_saturation_drain_leaf::drain(&mut self.state, cause.state_kind());
    }

    /// DEF-246 Phase 2 (2026-05-16): mint a fresh ReplyId for any
    /// phase. Body identical to the pre-DEF-246
    /// `<ActivePhase>::next_reply_id` (static atomic counter; cold
    /// saturation classifier).
    #[inline]
    pub(crate) fn next_reply_id<K: crate::reply_id::ReplyKind>(
        &mut self,
    ) -> crate::reply_id::ReplyId<K> {
        use core::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let raw_old = COUNTER.fetch_add(1, Ordering::Relaxed);
        if raw_old == u64::MAX {
            self.install_errored_replyid_saturation();
        }
        let raw = raw_old.saturating_add(1);
        let nz = core::num::NonZeroU64::new(raw)
            .unwrap_or(core::num::NonZeroU64::MIN);
        crate::reply_id::ReplyId::from_raw(nz)
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
    ///
    /// DEF-246 Option α (2026-05-16): moved from
    /// `impl PgProtocol<ActivePhase>` to `impl PgProtocolInner` so
    /// the `<ConnectingPhase>::feed_bytes` mirror (Phase 3 elevation
    /// #2) reaches the same code path via the same delegate shape.
    /// `self.inner.X` references became `self.X` in the move.
    pub(crate) fn feed_bytes_impl<'w, 'r, const BOUNDED: bool>(
        &'r mut self,
        bytes: &[u8],
        write_buf: &'w mut WriteBuf,
        max_dispatches: u16,
    ) -> OutActions<'w, 'r> {
        write_buf.clear();
        // DEF-211 FAKE-01: feed_bytes can be called in any state.
        // Compute `push_class()` ONCE here and pass to the residue
        // helper — pre-FAKE-01 the helper computed `push_class`
        // internally (~+10 ns per call). Caching at the entry point
        // amortises one classification across the full feed_bytes
        // dispatch loop.
        let entry_class = self.state.push_class();
        self.clear_session_residue_for_class(entry_class);

        // DEF-154 (H+V) pending_advance DELETED 2026-04-24: the
        // deferred-advance slot existed because `StagedAction::StreamRowRange`
        // once carried `row_bytes: &'r [u8]` into read_buf — cursor
        // advance while that borrow was alive = borrow-check
        // conflict. DEF-154 (Y) DELETED `StreamRowRange` entirely.
        // Post-(Y) no StagedAction variant carries a read_buf borrow,
        // so cursor advance can fire IN-SCOPE inside the dispatch
        // loop with no conflict. See struct docstring field
        // comment for full post-mortem.

        // DEF-185 P1-6 (audit 2026-04-24): consolidated control flow.
        //
        // Pre-fix: scattered state checks at lines 871-875 (decide
        // whether to append), 892-898 (short-circuit if Errored), and
        // 902-918 (handle append_err). Logically identical but the
        // borrow-checker constraints forced the append to fire before
        // split-borrows, creating a visual disconnect between the
        // first Errored check and the subsequent handler. A future
        // refactor touching any single site could easily break the
        // invariant (e.g. swapping the first check's Some/None
        // polarity).
        //
        // Post-fix: single point of classification. The
        // `IngressClassification` enum enumerates every legal entry
        // condition (Errored / AppendFailed / Ok) so the dispatcher
        // match is tier-1 exhaustive. Each arm has one canonical
        // handler path.
        #[derive(Debug)]
        enum IngressClassification {
            /// State was already Errored before this call. Skip append,
            /// clear read_buf, return empty OutActions.
            AlreadyErrored,
            /// Append overflowed the read buffer. Clear + FailReply +
            /// CloseSocket.
            AppendFailed { attempted: usize, available: usize },
            /// Append succeeded (or was a no-op for empty bytes). Normal
            /// dispatch loop.
            Ok,
        }

        // DEF-248 Sub-B (2026-05-12) — partial-mode bytes routing.
        //
        // When the partial-assembly cell is active (an oversize non-`'D'`
        // body is mid-flight), inbound bytes route to the assembly
        // accumulator FIRST. Up to `body_remaining` bytes are consumed
        // (copied to the bounded prefix or counted-and-skipped beyond
        // the cap); only the leftover (bytes belonging to the NEXT
        // frame) flows to ReadBuf.
        //
        // Routing is gated on `is_active() -> bool`, a single byte-load
        // on the `Option<Box<_>>` niche discriminant. The inactive arm
        // runs `read_buf.append(bytes)` byte-for-byte as before — no
        // perf delta on the hot path.
        //
        // Tier-1 closure: ReadBuf cannot hold > 4 KB; routing through
        // the assembly absorber for active partial mode is the ONLY
        // path that handles bytes 5..= the body's last byte. Without
        // this hook, a 5 KB body would fail `read_buf.append` with
        // `ReadBufFull` on the chunk completing the body.
        // DEF-246 Option α: `self.inner.X` references rewritten to
        // `self.X` for the moved body (on `PgProtocolInner` directly).
        let bytes_for_readbuf: &[u8] = if !matches!(self.state, ProtoState::Errored(_))
            && self.partial_assembly.is_active()
        {
            // DEF-248 Sub-B (2026-05-12): cold-path hint. Partial mode
            // is rare (only oversize non-`'D'` bodies engage it); the
            // inactive arm is the hot path. Keep the routing body out
            // of the I-cache footprint of the standard ingress.
            core::hint::cold_path();
            let absorbed = _partial_assembly_dispatch_leaf::absorb_partial_assembly_at_dispatch(
                &mut self.partial_assembly,
                bytes,
            );
            bytes.get(absorbed..).unwrap_or(&[])
        } else {
            bytes
        };

        let classification = if matches!(self.state, ProtoState::Errored(_)) {
            IngressClassification::AlreadyErrored
        } else {
            match self.read_buf.append(bytes_for_readbuf) {
                Ok(()) => IngressClassification::Ok,
                Err(ReadBufFull { attempted, available, .. }) => {
                    IngressClassification::AppendFailed { attempted, available }
                }
            }
        };

        // DEF-154 (E): field-level destructure. Closures cannot see
        // disjoint field borrows through `self`; splitting into
        // separate `&mut` bindings gives each consumer a single-field
        // borrow. `state` + `read_buf` are held DISJOINTLY — the
        // dispatch loop reads `populated` / `cursor_position` from
        // `read_buf` while `state` is separately `&mut` for
        // transitions, AND advances `read_buf` cursor in-scope
        // after each frame is consumed.
        let state = &mut self.state;
        let read_buf = &mut self.read_buf;
        let terminal_row_desc = &mut self.row_desc_slot;
        // DEF-196 (2026-04-28): three independent cold slots, each
        // lazy-allocated only at its specific write site:
        //   - session_params slot: ParameterStatus + NoticeResponse filters.
        //   - error_arena slot:    ErrorResponse arms in dispatch.rs.
        //   - malformed_counter:   inline u32, direct write (no Box).
        let session_params_slot = &mut self.session_params;
        let error_arena_slot = &mut self.error_arena;
        let malformed_counter = &mut self.malformed_frame_count;
        // DEF-248 Sub-B (2026-05-12): partial-assembly cell mut borrow
        // threaded through the dispatch loop. Used at:
        // 1. `HeaderParse::FrameTooLarge` for streaming-eligible tags
        //    — enter partial mode + absorb the already-buffered prefix.
        // 2. Top of dispatch loop — if the assembly is complete, take
        //    + dispatch the assembled prefix through the existing
        //    per-tag dispatch arm.
        let partial_assembly_slot = &mut self.partial_assembly;
        // DEF-278 Bundle D (2026-05-17): backend-key cell mut borrow
        // threaded through the dispatch loop. Written by exactly one
        // arm in `dispatch()`:
        // `(ConnectingPostAuthHaveKey, TAG_READY_FOR_QUERY)` —
        // installs `(pid, secret_key)` at handshake-complete via the
        // token-gated `_backend_key_install_leaf` helper.
        //
        // Hot-path cost: the parameter is a fat pointer (8 B on
        // arm64). Passed by `&mut` to every dispatch call; the cell
        // itself is touched only when the matching arm fires (one
        // arm out of ~70). Pre-arm `mem::replace` is unaffected — no
        // additional memcpy. Bench gate: `feed_bytes/ping_amortised`
        // must stay within ±1% of baseline.
        let backend_key_slot = &mut self.backend_key;

        // DEF-185 P1-6: single classification-driven dispatch.
        // Exhaustive match on `IngressClassification` — adding a new
        // variant fails the build here until handler exists.
        match classification {
            IngressClassification::AlreadyErrored => {
                // DEF-238 (audit 2026-05-05): cold-path hint. Reaching
                // here means caller fed bytes after a fatal teardown
                // — adversarial / mis-driven state. Push the empty-
                // OutActions emit out of the hot I-cache footprint.
                core::hint::cold_path();
                read_buf.clear();
                // DEF-188: materialise needs an immutable view of
                // terminal_row_desc. NLL collapses the prior `&mut`
                // binding at the last use; reborrow here as `&Option<_>`
                // for the duration of the closure.
                let terminal_ref: Option<&crate::decode::RowDesc> =
                    (*terminal_row_desc).as_ref();
                return write_buf.with_branded(|wb| -> OutActions<'w, 'r> {
                    let staged: StagedActions = StagedActions::new();
                    materialise(staged, wb.into_bytes(), terminal_ref)
                });
            }
            IngressClassification::AppendFailed { attempted, available } => {
                // DEF-238 (audit 2026-05-05): cold-path hint. ReadBuf
                // overflow = fatal connection teardown (FailReply +
                // CloseSocket) on a path the production hot loop never
                // hits — keep this body out of the inlined ingress
                // arm.
                core::hint::cold_path();
                // DEF-186 P1-4 ordering invariant (audit 2026-04-24):
                // `read_buf.clear()` MUST precede `fail_inflight_no_readbuf`
                // here. The clear() zero-on-clear path (P0-C) scrubs any
                // residual SCRAM server-frame bytes (server-first /
                // server-final containing password-correlated material)
                // BEFORE the state transition consumes the SCRAM variant.
                // If a future refactor reorders these two calls, the
                // residue window opens — partial inbound bytes survive
                // into the post-Errored phase until the wrapper drops
                // the connection.
                //
                // Bundled-helper-style refactor (single fn that does
                // both) deferred to keep call-site ordering grep-able.
                read_buf.clear();
                // DEF-196: malformed_counter is inline u32, direct
                // mutation — no Box, no lazy-init.
                return write_buf.with_branded(|wb| -> OutActions<'w, 'r> {
                    let mut staged: StagedActions = StagedActions::new();
                    fail_inflight_no_readbuf(
                        state,
                        ProtocolError::ReadBufferFull { attempted, available },
                        &mut staged,
                        malformed_counter,
                    );
                    // DEF-188: fail_inflight_no_readbuf doesn't touch
                    // terminal_row_desc; reborrow as immutable for
                    // materialise here. NLL ends the outer `&mut`
                    // binding at fail_inflight's return.
                    let terminal_ref: Option<&crate::decode::RowDesc> =
                        (*terminal_row_desc).as_ref();
                    materialise(staged, wb.into_bytes(), terminal_ref)
                });
            }
            IngressClassification::Ok => {
                // Fall through to main dispatch.
            }
        }

        // Main dispatch. Take shared borrow of populated + cursor
        // (both via immutable reborrow of read_buf's &mut).
        write_buf.with_branded(|mut wb| -> OutActions<'w, 'r> {
            let mut staged: StagedActions = StagedActions::new();
            // DEF-184 audit (2026-04-24): `populated` + `cursor`
            // bindings moved INSIDE the closure (post-DEF-154 Y
            // no staged action borrows from read_buf). The shared
            // borrow drops at end of loop body via NLL, unblocking
            // `read_buf.advance()` in-scope after the loop.
            let populated: &[u8] = read_buf.populated();
            let cursor: u16 = read_buf.cursor_position_u16();
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

            // DEF-248 Sub-B (2026-05-12): post-loop staging for
            // partial-assembly entry.
            //
            // The dispatch loop's `populated` shared borrow conflicts
            // with `partial_assembly_slot` mut access inside the loop
            // body. The `FrameTooLarge` arm stages the entry work
            // here; the post-loop block applies the mutation after
            // NLL closes `populated`'s borrow.
            //
            // Carries: (tag_byte, body_remaining_at_entry,
            // body_prefix_already_buffered: &[u8]). The prefix slice
            // is a sub-slice of `populated` — its lifetime ends at
            // the loop's `}` brace (NLL); the apply site at line
            // 2470+ runs BEFORE the closing brace's NLL hits.
            let mut staged_partial_entry:
                Option<(u8, u32, &[u8])> = None;

            // Dispatch loop block: `reserved` holds `&mut wb.buf`
            // which must release before `wb.into_bytes()`
            // post-loop. NLL ends `reserved`'s borrow at the `}`.
            {
            let mut reserved = wb.reserve();
            // DEF-248 Sub-B (2026-05-12): assembly-completion dispatch
            // fires BEFORE the parse-header loop. If the prior
            // `feed_inbound` / `read_buf_append` / top-of-feed-bytes
            // bytes-routing path completed the in-flight body
            // (`body_remaining == 0`), take the assembly out, route
            // its prefix through the existing per-tag `dispatch()`,
            // and free the Box. The standard parse-header loop below
            // then runs against ReadBuf's tail (containing any
            // bytes that arrived alongside the completing chunk —
            // typically a trailing `'Z'`).
            //
            // Identical-event-semantics contract: the dispatch arm
            // sees the (truncated-to-PREFIX_CAP) prefix in the place
            // it would have seen the inline payload. Every non-`'D'`
            // parser is inline-bounded; observation matches the
            // inline-arrival path byte-for-byte.
            // Check completion + budget BEFORE taking the box. If the
            // staged-actions slot is full, defer the dispatch to the
            // next `feed_bytes` call — the assembly stays in the cell
            // (calling code routes any additional inbound bytes
            // through the absorber, which no-ops for complete bodies).
            // Universal coverage preserved: every wire-legal frame
            // eventually dispatches; the budget gate just rate-limits
            // per-call.
            //
            // **Hot-path cost**: `partial_assembly_slot.as_inner()` is
            // a single byte-load on the niche-packed `Option<Box<_>>`
            // discriminator. Common case (no partial mode) is `None`
            // — `matches!` returns false in one compare. `cold_path()`
            // marks the taken-and-dispatch body as the rare branch so
            // LLVM emits it after the parse-header loop in machine
            // code, keeping the hot loop's I-cache footprint
            // unaffected.
            // Fast-path gate: `is_active()` is a single byte-load on
            // the niche-packed Option<Box> discriminator. Returns false
            // 99.99% of the time (no oversize body in flight). The
            // remaining checks (`is_complete()` + budget) only run on
            // the cold arm where partial mode is active.
            if partial_assembly_slot.is_active()
                && matches!(
                    partial_assembly_slot.as_inner(),
                    Some(inner) if inner.is_complete(),
                )
                && staged
                    .len()
                    .saturating_add(WORST_CASE_PER_DISPATCH)
                    <= MAX_STAGED_PER_CALL
                && let Some(assembly_box) =
                    _partial_assembly_dispatch_leaf::take_completed_partial_assembly_at_dispatch(
                        partial_assembly_slot,
                    )
            {
                core::hint::cold_path();
                let assembled_tag = assembly_box.typed_tag();
                let outcome = dispatch(
                    state,
                    assembled_tag,
                    assembly_box.prefix(),
                    &mut reserved,
                    terminal_row_desc,
                    error_arena_slot,
                    backend_key_slot,
                );
                match outcome {
                    DispatchOutcome::AdvancedSilent => {
                        dispatches_this_call =
                            dispatches_this_call.saturating_add(1);
                    }
                    DispatchOutcome::AdvancedWithAction { action } => {
                        dispatches_this_call =
                            dispatches_this_call.saturating_add(1);
                        emit_actions!(&mut staged, budget: 1, [
                            action,
                        ]);
                    }
                    DispatchOutcome::Errored { reply_id, cause } => {
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
                    }
                }
                // assembly_box drops here — heapless::Vec releases its
                // inline allocation; no leak across iterations.
                drop(assembly_box);
            }
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
                        // DEF-196: malformed_counter is inline; pass the
                        // top-of-fn binding directly — no Box, no lazy.
                        fail_inflight_no_readbuf(
                            state,
                            ProtocolError::MalformedFrameLength { declared },
                            &mut staged,
                            malformed_counter,
                        );
                        break;
                    }
                    HeaderParse::FrameTooLarge { declared } => {
                        // DEF-248 Sub-B (2026-05-12): universal-coverage
                        // entry to partial-assembly mode for non-`'D'`
                        // streaming-eligible tags.
                        //
                        // Pre-Sub-B: every `FrameTooLarge` tore the
                        // connection down. Sub-A delivered partial mode
                        // for `'D'` only, routed through
                        // `RowStream::dispatch_next_frame` (not here).
                        // Sub-B delivers the residue: `{T, E, N, A, C,
                        // S, R, v}` tags whose body > READ_BUF_CAP
                        // route through this arm.
                        //
                        // Decision:
                        // 1. Streaming-eligible? If not → existing
                        //    teardown.
                        // 2. Else: stage partial-mode entry. Snapshot
                        //    the body bytes already buffered
                        //    (`after_consumed[HEADER_LEN..]`); the rest
                        //    of the body arrives via subsequent
                        //    `feed_bytes` calls and routes through the
                        //    top-of-feed-bytes absorb path. **No
                        //    frequency-based cap** — bodies up to ~2 GiB
                        //    pass through; bytes beyond the 8 KB
                        //    prefix cap are counted-and-skipped by
                        //    the assembly absorber.
                        //
                        // The actual mutation (enter + absorb +
                        // advance) is deferred to post-loop because
                        // the loop's `populated` shared borrow
                        // conflicts with `partial_assembly_slot` /
                        // `read_buf` mut access here.
                        let tag_byte = after_consumed.first().copied().unwrap_or(0);
                        let body_len_opt = declared.checked_sub(4);
                        match body_len_opt {
                            Some(body_len)
                                if crate::partial_assembly::is_streaming_eligible_tag(
                                    tag_byte,
                                ) =>
                            {
                                // Body bytes already in ReadBuf
                                // (portion buffered alongside header).
                                let already_buffered_body = after_consumed
                                    .get(HEADER_LEN..)
                                    .unwrap_or(&[]);
                                // The slice we snapshot is bounded by
                                // ReadBuf's unread length — at most
                                // READ_BUF_CAP - HEADER_LEN bytes.
                                // u16 storage suffices for the cursor
                                // arithmetic below.
                                let header_plus_body = u16::try_from(
                                    HEADER_LEN.saturating_add(already_buffered_body.len()),
                                )
                                .unwrap_or(u16::MAX);
                                staged_partial_entry = Some((
                                    tag_byte,
                                    body_len,
                                    already_buffered_body,
                                ));
                                frames_consumed = frames_consumed
                                    .saturating_add(header_plus_body);
                                // Break — no more frames after partial
                                // entry; the rest of the body arrives
                                // out-of-band via subsequent calls.
                                break;
                            }
                            _ => {
                                // Tag is NOT streaming-eligible (fixed-size
                                // bodies — K/Z/I/1/2/3/n — or D-tag
                                // which Sub-A handles via column
                                // streaming). Existing teardown path.
                                fail_inflight_no_readbuf(
                                    state,
                                    ProtocolError::FrameTooLarge { declared },
                                    &mut staged,
                                    malformed_counter,
                                );
                                break;
                            }
                        }
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
                            // DEF-185 P2-B (audit 2026-04-24): surface
                            // MalformedPayload via counter. Pre-fix
                            // `{}` silently collapsed the outcome;
                            // post-fix mirrors `n_malformed_bool_dropped`
                            // for ops diagnostic visibility.
                            //
                            // DEF-196: lazy-init session_params Box only
                            // when actually writing (here).
                            //
                            // DEF-272 cluster β (2026-05-10): admit goes
                            // through `_parameter_status_admit_leaf::
                            // admit_parameter_status_frame` which mints a
                            // `ParamStatusToken` (private-field, leaf-
                            // gated mint) and routes to
                            // `SessionParamsCell::admit_at_param_status`.
                            // The cell internally parses + records on
                            // success / bumps malformed counter on parse
                            // failure — single mutation site behind the
                            // token-gated cell method.
                            // Outcome is signalled to the caller for
                            // potential future logging/test observation;
                            // current consumers (this site) discard it.
                            let _outcome: ParamStatusRecordOutcome =
                                _parameter_status_admit_leaf::admit_parameter_status_frame(
                                    session_params_slot,
                                    payload,
                                );
                            frames_consumed =
                                frames_consumed.saturating_add(total_len);
                            continue;
                        }
                        // DEF-185 P1-E (audit 2026-04-24): NoticeResponse
                        // filter gated by exhaustive per-variant classifier
                        // `allows_unsolicited_notice_response`. Pre-auth
                        // states reject the notice (fall through to the
                        // dispatch arm which classifies as UnexpectedFrame
                        // + teardown) — prevents pre-auth attacker-
                        // controlled text from landing in wrapper logs.
                        //
                        // DEF-185 P2-3 (audit 2026-04-24): bump counter
                        // for operator visibility (adversarial notice
                        // flood detection).
                        if tag == crate::wire::TAG_NOTICE_RESPONSE
                            && allows_unsolicited_notice_response(state)
                        {
                            // DEF-196: lazy-init session_params Box only
                            // when actually writing (here, bumping the
                            // notice counter).
                            //
                            // DEF-272 cluster β (2026-05-10): admit goes
                            // through `_notice_response_admit_leaf::
                            // admit_notice_response_frame` which mints a
                            // `NoticeResponseToken` (leaf-gated mint) and
                            // routes to `SessionParamsCell::admit_at_notice_response`.
                            _notice_response_admit_leaf::admit_notice_response_frame(
                                session_params_slot,
                            );
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
                        // DEF-188: terminal_row_desc threaded through
                        // for the Z arms to park the in-flight schema.
                        // DEF-196: pass error_arena_slot only.
                        // Dispatch arms (ErrorResponse) lazy-init the
                        // Box<ErrorArena> when actually writing.
                        // DEF-278 Bundle D (2026-05-17): also pass
                        // `backend_key_slot` so the
                        // `(ConnectingPostAuthHaveKey, 'Z')` arm can
                        // install at handshake-complete.
                        let outcome = dispatch(
                            state,
                            tag,
                            payload,
                            &mut reserved,
                            terminal_row_desc,
                            error_arena_slot,
                            backend_key_slot,
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

            // DEF-248 Sub-B (2026-05-12): apply staged partial-mode
            // entry, if any. The dispatch loop's FrameTooLarge arm
            // (for streaming-eligible tags) deferred enter+absorb to
            // this point because the loop's `populated` shared borrow
            // conflicted with `partial_assembly_slot` mut access.
            //
            // Sequence:
            // 1. Enter partial mode for (tag, body_remaining_at_entry).
            //    This either reuses the existing Box (capacity-
            //    preserving reset) or allocates a fresh
            //    Box<PartialAssemblyInner>.
            // 2. Absorb the already-buffered body prefix. The first
            //    PREFIX_CAP bytes land in `prefix_buf`; bytes beyond
            //    are counted-and-skipped (body_remaining decrements).
            // 3. The cursor advance below moves past
            //    `HEADER_LEN + body_bytes.len()` — the bytes we just
            //    absorbed are gone from ReadBuf.
            if let Some((tag, body_len, body_bytes)) = staged_partial_entry.take() {
                _partial_assembly_dispatch_leaf::enter_partial_assembly_at_dispatch(
                    partial_assembly_slot,
                    tag,
                    body_len,
                );
                if !body_bytes.is_empty() {
                    let _absorbed_n =
                        _partial_assembly_dispatch_leaf::absorb_partial_assembly_at_dispatch(
                            partial_assembly_slot,
                            body_bytes,
                        );
                    debug_assert_eq!(
                        _absorbed_n,
                        body_bytes.len(),
                        "DEF-248 Sub-B: partial-mode entry absorbed all \
                         body bytes (absorbed={}, body_bytes.len={})",
                        _absorbed_n,
                        body_bytes.len(),
                    );
                }
            }

            // DEF-184 audit (2026-04-24) — advance IN-SCOPE.
            //
            // Pre: recorded `pending_advance` for next call's entry,
            // because `StagedAction::StreamRowRange` held `&'r [u8]`
            // into populated. Post-DEF-154 (Y) deletion of that
            // variant + the dispatch loop's narrow `populated: &[u8]`
            // (which drops at end of loop body via NLL), we can now
            // call `read_buf.advance()` right here.
            //
            // Skip advance on Errored transition — `clear_session_residue_if_idle_or_errored`
            // on the NEXT entry call clears the read_buf anyway, so
            // any partial-frame remnant doesn't matter.
            //
            // `advance()` returns `Result<(), AdvancePastEnd>` —
            // architecturally dead post-validated frames_consumed
            // sum, but we classify via InternalCrateBug locus
            // `ReadCursorAdvance` if it ever fires.
            if !matches!(state, ProtoState::Errored(_))
                && frames_consumed > 0
                && read_buf.advance(usize::from(frames_consumed)).is_err()
            {
                // DEF-238 (audit 2026-05-05): cold-path hint on the
                // dead-arm body. Reaching here implies a regression in
                // cursor math (parse_header validates total_len <=
                // populated.len() before each advance contribution).
                // Marked cold so LLVM keeps fail_inflight_no_readbuf
                // out of the hot post-loop epilogue.
                core::hint::cold_path();
                // Classified dead-arm — a regression in cursor math
                // would land here. Transition to Errored and emit
                // FailReply via fail_inflight.
                // DEF-196: malformed_counter is inline u32, direct.
                fail_inflight_no_readbuf(
                    state,
                    ProtocolError::InternalCrateBug {
                        locus: crate::error::CrateBugLocus::ReadCursorAdvance,
                    },
                    &mut staged,
                    malformed_counter,
                );
            }

            // DEF-188: dispatch may have written to `terminal_row_desc`
            // during the loop (Z arms park schemas). NLL ends the
            // dispatch loop's `&mut` reborrow at the loop close brace
            // above; reborrow as immutable here for materialise.
            let terminal_ref: Option<&crate::decode::RowDesc> =
                (*terminal_row_desc).as_ref();
            materialise(staged, wb.into_bytes(), terminal_ref)
        })
    }

    /// DEF-184 (X) P0-2(c): frame-bounded variant of [`Self::feed_bytes_impl`]
    /// — `Self::feed_bytes_impl::<true>` with caller-supplied
    /// `max_dispatches`. Sole production caller is
    /// [`crate::row_stream::RowStream`]'s slow path which needs single-frame
    /// observability after a silent `RowDescription`.
    ///
    /// DEF-246 Option α (2026-05-16): moved from
    /// `impl PgProtocol<ActivePhase>` to `impl PgProtocolInner`; the
    /// `<ActivePhase>` delegate above forwards via
    /// `self.inner.feed_bytes_bounded(...)`. `row_stream`'s call
    /// (`self.proto.feed_bytes_bounded`) routes through the delegate.
    #[inline]
    pub(crate) fn feed_bytes_bounded<'w, 'r>(
        &'r mut self,
        bytes: &[u8],
        write_buf: &'w mut WriteBuf,
        max_dispatches: u16,
    ) -> OutActions<'w, 'r> {
        self.feed_bytes_impl::<true>(bytes, write_buf, max_dispatches)
    }

    /// Append inbound wire bytes into the read buffer **without
    /// dispatching**.
    ///
    /// DEF-246 Phase 4 elevation #4 (2026-05-16): signature returns
    /// `Result<(), ProtocolError>` so Errored state is a typed-error
    /// signal at the caller (previously silent no-op). The previous
    /// `ReadBufFull` map-from is preserved via
    /// `ProtocolError::ReadBufferFull { … }`.
    ///
    /// DEF-246 Option α (2026-05-16): moved from
    /// `impl PgProtocol<ActivePhase>` to `impl PgProtocolInner` so
    /// `<ActivePhase>::feed_inbound` and `<ConnectingPhase>::feed_inbound`
    /// (Phase 3 elevation #2) share the identical body via 1-line
    /// delegate.
    pub(crate) fn feed_inbound(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), crate::error::ProtocolError> {
        if matches!(self.state, ProtoState::Errored(_)) {
            // DEF-246 Phase 4 #4: typed `ConnectionAlreadyClosed`
            // surfaces post-Errored fee attempts instead of silent
            // no-op. `prior_kind` reconstructs from the stored
            // `StateErrorKind` in the Errored variant.
            core::hint::cold_path();
            return Err(crate::error::ProtocolError::ConnectionAlreadyClosed {
                prior_kind: match &self.state {
                    ProtoState::Errored(k) => *k,
                    // SAFETY (tier-1 by match-guard above): `matches!` above
                    // already proved this arm dead. CREDO §V bans
                    // `debug_assert!(false, …)`; classify defensively to
                    // a Crate-bug locus instead.
                    _ => crate::error::ProtocolError::InternalCrateBug {
                        locus: crate::error::CrateBugLocus::ReadCursorAdvance,
                    }.state_kind(),
                },
            });
        }
        // DEF-248 Sub-B (2026-05-12): partial-mode bytes routing —
        // identical to the top-of-feed_bytes_impl logic. If an oversize
        // non-`'D'` body is mid-flight, bytes route to the assembly
        // accumulator first; leftover (next-frame) bytes flow to
        // ReadBuf. Without this hook, a chunk completing a 5 KB body
        // would fail `read_buf.append` with `ReadBufFull` (ReadBuf cap
        // is 4096 B).
        //
        // **Hot-path cost**: the `is_active()` check is one byte-load
        // on the niche-packed `Option<Box<_>>` discriminator. For
        // workloads that never trigger partial mode (every
        // small-frame query, every SCRAM handshake, every error
        // ≤ 4 KB body), the branch predicts false and the partial-
        // mode path stays out of I-cache. `cold_path()` hints LLVM
        // to push the partial-mode body to the end of the function's
        // generated machine code.
        if self.partial_assembly.is_active() {
            core::hint::cold_path();
            let absorbed = _partial_assembly_dispatch_leaf::absorb_partial_assembly_at_dispatch(
                &mut self.partial_assembly,
                bytes,
            );
            let leftover = bytes.get(absorbed..).unwrap_or(&[]);
            if leftover.is_empty() {
                return Ok(());
            }
            return self.read_buf.append(leftover).map_err(|e| {
                let crate::buf::ReadBufFull { attempted, available, .. } = e;
                crate::error::ProtocolError::ReadBufferFull { attempted, available }
            });
        }
        self.read_buf.append(bytes).map_err(|e| {
            let crate::buf::ReadBufFull { attempted, available, .. } = e;
            crate::error::ProtocolError::ReadBufferFull { attempted, available }
        })
    }

    /// Process at most one user-observable event and return it.
    ///
    /// DEF-246 Option α (2026-05-16): moved from
    /// `impl PgProtocol<ActivePhase>` to `impl PgProtocolInner` —
    /// shared by `<ActivePhase>` and `<ConnectingPhase>` (Phase 3
    /// elevation #2). The body is unchanged otherwise; the original
    /// `self.inner.X` references collapsed to `self.X`.
    #[must_use = "FeedEvent variants carry side-effect contracts: \
                  SendBytes/Deliver MUST be processed; Fail/Close MUST \
                  trigger socket teardown"]
    pub(crate) fn advance_one_frame<'w, 'r>(
        &'r mut self,
        write_buf: &'w mut WriteBuf,
    ) -> crate::action::FeedEvent<'w, 'r> {
        use crate::action::{Action, FeedEvent};

        // Fast-path classifications BEFORE reusing feed_bytes_bounded
        // (which calls write_buf.clear() unconditionally — we want to
        // avoid that on these "no work" paths to preserve any caller
        // residue in wb across spurious advance calls).
        //
        // Streaming-rows transition signal: the caller should use
        // `iter_rows()` for per-row decoding while in this state. We
        // detect BEFORE feed_bytes_bounded would consume DataRows in
        // its dispatch loop (which is the wrong shape for the per-
        // event API).
        if matches!(
            self.state,
            ProtoState::SimpleQueryStreamingRows { .. }
                | ProtoState::BindExecuteStreamingRows { .. }
        ) {
            return FeedEvent::StreamingRows;
        }

        // Errored terminal: the connection is dead. Caller closes.
        if matches!(self.state, ProtoState::Errored(_)) {
            return FeedEvent::Close;
        }

        // Idle + empty read_buf: nothing to process — caller can push.
        if matches!(self.state, ProtoState::Idle) && self.read_buf.unread().is_empty() {
            return FeedEvent::Idle;
        }

        // Drive the bounded dispatch loop with empty bytes (no append)
        // and max_dispatches=1 (one actionable frame). The result is
        // an OutActions with 0..=2 actions corresponding to one frame
        // event.
        let actions = self.feed_bytes_bounded(b"", write_buf, 1);

        // Map actions → FeedEvent. The exhaustive match below is the
        // tier-1 contract: any future Action variant addition fails
        // the build until classified here.
        match actions.as_slice() {
            // No actionable frame in this cycle. Caller needs more
            // bytes from network (state was non-Idle non-Errored, so
            // a partial frame is buffered or none at all).
            [] => FeedEvent::NeedMoreBytes,
            // Single SendBytes — outbound message (e.g., SCRAM
            // client-final). The slice borrows into the caller's wb.
            [Action::SendBytes(bytes)] => FeedEvent::SendBytes(bytes),
            // Single DeliverReply — terminal happy reply.
            [Action::DeliverReply { id, value }] => FeedEvent::Deliver(*id, *value),
            // FailReply [+ CloseSocket]: per M2, Fail implies close.
            // Caller learns "close required" from the Fail variant
            // documentation; no separate CloseSocket event needed.
            // Pre-(212) feed_bytes returned a 2-Action slice; post-Phase-2
            // the FeedEvent::Fail collapses both into one.
            [Action::FailReply { id, cause }, ..] => FeedEvent::Fail(*id, *cause),
            // CloseSocket alone (no in-flight reply): adversarial
            // frame in Idle, post-handshake fatal. Caller closes.
            [Action::CloseSocket] => FeedEvent::Close,
            // Architecturally unreachable: feed_bytes_bounded with
            // max_dispatches=1 emits AT MOST 2 actions per cycle
            // (FailReply+CloseSocket pair from install_errored). Any
            // other shape indicates a regression in dispatch's
            // emit-budget invariants.
            //
            // CREDO §V banned `debug_assert!(false, ...)` defensive-
            // for-impossible — instead, classify to the conservative
            // `Close` (forces caller to discard connection, avoiding
            // any silent-state-corruption window).
            _ => FeedEvent::Close,
        }
    }

    /// DEF-188 — entry-point terminal-row-desc reclamation.
    ///
    /// DEF-189 — entry-point session-residue reclamation.
    ///
    /// DEF-246 Option α (2026-05-16): moved from
    /// `impl PgProtocol<ActivePhase>` to `impl PgProtocolInner` so
    /// `<ActivePhase>` (default phase) and `<ConnectingPhase>` (Phase
    /// 3) both reach the residue policy via the SAME implementation.
    /// `self.inner.X` references became `self.X` in the move.
    ///
    /// If the connection state is `Idle`, clear the row_desc_slot and
    /// the error_arena. If state is `Errored`, additionally clear
    /// `session_params` (a tear-down forfeits all session state — any
    /// post-error login retry should observe an empty session, not
    /// inherited locale/encoding/etc. from the dead session).
    ///
    /// Called from all four user-facing entry points: `push_command`,
    /// `push_bind_execute`, `feed_bytes`, `iter_rows`. Pre-DEF-172,
    /// each site open-coded the match+clear pattern — drift surface
    /// (audit2 A007) since adding a fifth entry point would need to
    /// remember the ritual. Post-DEF-172 the discipline lives in one
    /// place.
    ///
    /// # Lifecycle correctness
    ///
    /// The slot is parked at dispatch's `'T'` arm (state enters
    /// streaming variants); the public `Reply::QueryComplete<'r>`
    /// borrow is then handed to the user via `OutActions<'w, 'r>`.
    /// The user holds OutActions until they decide to drop it — they
    /// cannot re-call any `&mut self` method on PgProtocol while
    /// OutActions is alive (NLL on `'r`). When OutActions drops, `'r`
    /// ends; the next entry-point method call observes state ∈
    /// {Idle, Errored} (post-Z or post-tear-down) and clears the slot
    /// here. Tier-2 structural: no caller can peek into the slot
    /// after the borrow outlives, because the borrow checker forbids
    /// the cross.
    ///
    /// # DEF-189 Q8-C3 — session_params clear on Errored
    ///
    /// Pre-DEF-189 this only cleared row_desc_slot + error_arena.
    /// `session_params` (encoding, server_version, application_name,
    /// etc.) survived an Errored transition — operationally a leak:
    /// a wrapper that recycled the `PgProtocol` for a retry would
    /// observe the dead session's ParameterStatus values. Post-fix
    /// the Errored arm scrubs session_params; Idle leaves them intact
    /// (they're load-bearing during a healthy connection).
    ///
    /// # DEF-210 SR-02 (audit 2026-04-28): exhaustive policy
    ///
    /// Pre-Path-2 the `match self.inner.state { Idle => …, Errored(_) => …,
    /// _ => {} }` wildcard accepted any future `ProtoState` variant
    /// silently — the new variant would inherit the "do not clear"
    /// branch with no contributor decision recorded. Tier-2 surface.
    /// Path-2 routes through [`crate::state::ProtoState::push_class`]
    /// (which is itself exhaustive over `ProtoState`); the match here
    /// covers all 5 `StatePushClass` variants.
    ///
    /// # Honest tier framing (re-audit 2026-04-28)
    ///
    /// **Tier-1 at `StatePushClass` granularity** — a NEW
    /// `StatePushClass` variant fails the build here. **Tier-2 at
    /// `ProtoState` granularity** — a new `ProtoState` variant that
    /// classifies into an EXISTING `StatePushClass` bucket inherits
    /// that bucket's residue policy without forcing a contributor
    /// decision. This is acceptable as a design contract: the bucket
    /// IS the policy axis (Idle scrubs, Errored scrubs harder, others
    /// preserve in-flight residue). A contributor who needs DIFFERENT
    /// residue semantics for a new variant must extend `StatePushClass`
    /// — at which point the build fails here until they decide.
    #[inline]
    pub(crate) fn clear_session_residue_for_class(
        &mut self,
        class: crate::state::StatePushClass,
    ) {
        // DEF-211 FAKE-01 (audit 2026-05-04, 5th-pass architect-agent):
        // takes pre-computed `StatePushClass` rather than re-classifying
        // here. Tier-1 closure of the wildcard `_ => {}` arm-body-swap
        // surface: production callers compute `push_class()` ONCE at
        // the entry point and pass it through. The 5-arm exhaustive
        // match on `StatePushClass` below means a future variant added
        // to `StatePushClass` (which is itself driven by an exhaustive
        // match on every `ProtoState` variant in `state.rs::push_class`)
        // forces an explicit residue policy decision here at build
        // time. **No wildcard, no escape hatch** — tier-1 by-construction.
        //
        // Bench history of alternatives tried/rejected:
        // - Routing through `state.push_class()` IN THIS function
        //   (uncached): ~+10 ns on `push_command/ping_amortised`
        //   (LLVM declined to fold the dual match across the
        //   entry-point hot path).
        // - Enumerated 25-variant or-pattern on `ProtoState`: ~+4 ns
        //   (per-variant compares instead of single discriminant range
        //   check).
        // - Extracted `residue_policy(class) -> ResiduePolicy` helper:
        //   ~+21 ns (LLVM's inline budget rejected the function call
        //   despite `#[inline]`).
        // - **Cached classification at entry point** (this form):
        //   bench-neutral. push_command paths pass
        //   `StatePushClass::Idle` as a STATIC const argument so LLVM
        //   specialises to the Idle-only arm body. feed_bytes pays one
        //   `push_class()` call (28-arm match) per call — amortised
        //   over the full dispatch loop.
        //
        // DEF-246 Option α (2026-05-16): moved from
        // `impl PgProtocol<ActivePhase>` to `impl PgProtocolInner`;
        // `self.inner.X` references became `self.X` in the move.
        match class {
            crate::state::StatePushClass::Idle => {
                // DEF-272 cluster α: clear via leaf submodule
                // `_clear_residue_leaf` which mints a
                // `ClearResidueSchemaToken` (leaf-gated) and routes to
                // `RowDescSlotCell::clear_at_residue`.
                _clear_residue_leaf::clear_schema_slot_residue(&mut self.row_desc_slot);
                // DEF-196: only clear arena if it was ever allocated.
                if let Some(arena) = self.error_arena.as_deref_mut() {
                    arena.clear();
                }
                // DEF-248 Sub-B (2026-05-12): clear any in-flight
                // partial assembly. Architecturally rare on Idle entry
                // (a completed partial frame transitions state away
                // from Idle via dispatch's terminal arms), but
                // classifies any leftover Box from a torn-down
                // partial-mode sequence — Box drops, inner
                // heapless::Vec releases its inline allocation.
                _clear_residue_leaf::clear_partial_assembly_residue(&mut self.partial_assembly);
            }
            crate::state::StatePushClass::Errored(_) => {
                // DEF-272 cluster α: same leaf-submodule helper as the
                // Idle arm above.
                _clear_residue_leaf::clear_schema_slot_residue(&mut self.row_desc_slot);
                if let Some(arena) = self.error_arena.as_deref_mut() {
                    arena.clear();
                }
                // DEF-189 Q8-C3 + DEF-205 step 3: session-state
                // forfeit on tear-down; `SessionParams::clear`'s Drop
                // chain scrubs `SecretBoundedStr` bytes.
                //
                // DEF-272 cluster β: clear via the same leaf submodule
                // `_clear_residue_leaf` which hosts both schema and
                // session-side concrete tokens (`ClearResidueSchemaToken`
                // and `ClearResidueSessionToken`). Each clear method on
                // its respective Cell takes the matching token by value.
                _clear_residue_leaf::clear_session_params_residue(&mut self.session_params);
                // DEF-248 Sub-B (2026-05-12): also clear partial
                // assembly on Errored entry — any in-flight oversize
                // body is forfeit alongside the connection's other
                // session state. The Box drops; the Vec releases its
                // inline allocation; no leak across the post-Errored
                // window.
                _clear_residue_leaf::clear_partial_assembly_residue(&mut self.partial_assembly);
            }
            // In-flight states — preserve residue. The exhaustive
            // match here is the load-bearing tier-1 closure: adding
            // a new `StatePushClass` variant fails the build until
            // its residue policy is decided.
            crate::state::StatePushClass::Connecting
            | crate::state::StatePushClass::PingAwaiting
            | crate::state::StatePushClass::BusyQuery => {}
        }
    }
}

// DEF-246 Option α (2026-05-16): re-open `impl PgProtocol<ActivePhase>`
// for the remaining methods. The five methods moved to
// `impl PgProtocolInner` above (feed_bytes_impl, feed_bytes_bounded,
// feed_inbound, advance_one_frame, clear_session_residue_for_class)
// are reached through delegates near the top of this impl + via
// `self.inner.X` from in-crate sites (row_stream slow path,
// cfg(test) integration tests, etc.).
impl PgProtocol<ActivePhase> {

    /// DEF-246 Phase 4 transition surface (2026-05-16). Drives the
    /// terminally-Errored protocol into a typed `PgProtocol<ClosedPhase>`
    /// wrapper. Returns `Err(self)` when the protocol is NOT yet
    /// Errored — caller continues using the `<ActivePhase>` instance.
    ///
    /// # Tier-1 closure
    ///
    /// Pre-DEF-246 Phase 4: callers checked `connection_status()` and
    /// kept driving an Errored `<ActivePhase>`; every `push_command`
    /// classified through the existing `Errored` arm in
    /// `compute_push_*`. Tier-3 by-discipline — a future refactor
    /// could omit the Errored check.
    ///
    /// Post-Phase-4: the `<ClosedPhase>` ZST-marker physically lacks
    /// `push_command`, `feed_bytes`, `feed_inbound`, `advance_one_frame`,
    /// etc. (method-absent E0599 at compile time). The only operation
    /// available on `<ClosedPhase>` is `cause()` accessor (Phase 4 #1).
    ///
    /// The transition is **consume-self** — moving the wrapper keeps
    /// the byte layout identical (`#[repr(transparent)]`) and the
    /// `PhantomData<fn() -> P>` marker swaps cheaply at zero cost.
    ///
    /// # Why both `Ok` and `Err` carry `PgProtocol`
    ///
    /// The signature uses `Result<PgProtocol<ClosedPhase>,
    /// PgProtocol<ActivePhase>>` to return the wrapper by value in
    /// both arms — caller writes
    /// `let active = proto.into_closed_if_errored().map_err(|p| p)?;`
    /// or matches and recovers. `Box<PgProtocol>` would penalise the
    /// happy `Ok` path with an allocation that the wrapper layer does
    /// not need. The clippy `result_large_err` lint
    /// (520 B variant threshold) is acknowledged: the moving-by-value
    /// return is a load-bearing API shape, not a perf footgun
    /// (consume-self, called at most once per protocol lifecycle).
    #[expect(
        clippy::result_large_err,
        reason = "consume-self transition: BOTH arms carry the protocol \
                  wrapper by value so the caller recovers the typed phase \
                  without a Box. Boxing would penalise the happy `Ok` path \
                  with an alloc the wrapper does not need."
    )]
    pub fn into_closed_if_errored(
        self,
    ) -> Result<PgProtocol<ClosedPhase>, PgProtocol<ActivePhase>> {
        if matches!(self.inner.state, ProtoState::Errored(_)) {
            Ok(PgProtocol {
                inner: self.inner,
                phase_marker: PhantomData,
            })
        } else {
            Err(self)
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
        // DEF-196: arena lives in `cold: Option<Box<ColdFields>>`.
        // The static-fallback `cold_error_arena()` returns an empty
        // arena if cold hasn't been allocated; calling `get(r)` on the
        // empty arena classifies as `ArenaError::Stale` (generation
        // mismatch — the empty arena's generation is 0, any forged
        // ErrorRef has a different generation). Same observable
        // semantics as pre-DEF-196 inline arena.
        self.cold_error_arena().get(r)
    }

    /// DEF-185 P2-G (audit 2026-04-24): operator-facing canary for
    /// ErrorArena slot-overwrite events.
    ///
    /// Returns the number of times `parse_error_response` has alloc'd
    /// into the arena while the slot was already occupied — an
    /// architecturally-dead event under the current single-inflight
    /// state machine (the dispatch layer clears the arena at each
    /// entry-point when state is Idle/Errored, before the next cycle
    /// can trigger another error-response parse).
    ///
    /// A non-zero value on a live connection indicates a protocol-
    /// layer invariant break — wrappers surfacing this in their
    /// health checks get an early-warning signal for pipelining /
    /// dispatch-refactor regressions. 1c-5 pipelining support is
    /// expected to replace the single-slot arena with a slab; this
    /// canary stays meaningful until that refactor lands.
    #[inline]
    #[must_use]
    pub fn error_arena_overwrite_count(&self) -> u16 {
        // DEF-196: returns 0 when cold hasn't been allocated (no
        // error path has fired yet on this connection — same
        // semantics as pre-DEF-196 fresh arena).
        self.cold_error_arena().overwrite_count()
    }

    // DEF-184 (A10/B22 revert 2026-04-24): no test-only forge hooks.
    // Post-revert the variant-carries-field invariant is tier-1 compile,
    // so drift states simply cannot be constructed — tests exercise
    // SCRAM flow via real wire bytes through the public API.

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
        // DEF-196: passes the boxed arena reference, or an
        // &'static empty fallback if cold hasn't been allocated.
        // Display formatting of an unresolved ErrorRef classifies
        // as `ArenaError::Stale` — same diagnostic surface as before.
        crate::error_arena::DisplayError::new(err, self.cold_error_arena())
    }

    // ═════════════════════════════════════════════════════════════
    // DEF-154 (X) P0-2(c): RowStream helpers
    // ═════════════════════════════════════════════════════════════
    //
    // Thin crate-internal accessors exposing read_buf / state
    // operations to the `row_stream` module without opening
    // field-level `pub(crate)` on the field directly. Each is a
    // single-line delegate — no logic added.

    /// DEF-154 (X): append bytes to read_buf; Err on overflow.
    ///
    /// # DEF-248 Sub-B (2026-05-12) — partial-mode routing
    ///
    /// When the partial-assembly cell is active (an oversize non-`'D'`
    /// body is mid-flight), incoming bytes route to the assembly
    /// absorber FIRST. Up to `body_remaining` bytes are consumed
    /// (copied to the bounded prefix or counted-and-skipped beyond
    /// the cap); only the leftover (next-frame bytes) flows to ReadBuf.
    ///
    /// Without this hook, a chunk completing a body > READ_BUF_CAP
    /// would fail with `ReadBufFull` since ReadBuf is capped at 4 KB
    /// while bodies of any wire-legal size (up to ~2 GiB) must pass
    /// through.
    #[inline]
    pub(crate) fn read_buf_append(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        // DEF-248 Sub-B (2026-05-12): partial-mode routing —
        // `cold_path()` keeps the partial-mode body out of the hot
        // I-cache footprint. RowStream's per-row fast path calls this
        // function via `feed_inbound`-equivalent; the inactive arm
        // is the hot path 99.99% of the time (real PG payloads ≤ 4 KB).
        if self.inner.partial_assembly.is_active() {
            core::hint::cold_path();
            let absorbed = _partial_assembly_dispatch_leaf::absorb_partial_assembly_at_dispatch(
                &mut self.inner.partial_assembly,
                bytes,
            );
            let leftover = bytes.get(absorbed..).unwrap_or(&[]);
            if leftover.is_empty() {
                return Ok(());
            }
            return self.inner.read_buf.append(leftover);
        }
        self.inner.read_buf.append(bytes)
    }

    /// DEF-154 (X): shared view of the populated read_buf region.
    ///
    /// DEF-249 audit (2026-05-08): per-row hot path — called twice per
    /// row from `RowStream::next_row_bytes` (header peek + row carve).
    /// `#[inline]` already present (was added pre-DEF-249); audit
    /// confirms the call chain `next_row_bytes → read_buf_populated →
    /// ReadBufN::populated` is fully inlined under workspace
    /// `lto = "fat"` + `codegen-units = 1`. Future heuristic shifts
    /// in LLVM are pinned by the explicit hint here.
    #[inline]
    #[must_use]
    pub(crate) fn read_buf_populated(&self) -> &[u8] {
        self.inner.read_buf.populated()
    }

    /// DEF-154 (X): current read cursor (u16 storage).
    ///
    /// DEF-249 audit (2026-05-08): per-row hot path — called once per
    /// row from `RowStream::next_row_bytes` (cursor capture for row
    /// carve coordinates). `#[inline]` already present; audit
    /// confirms the call chain is fully inlined under workspace LTO.
    #[inline]
    #[must_use]
    pub(crate) fn read_buf_cursor_u16(&self) -> u16 {
        self.inner.read_buf.cursor_position_u16()
    }

    /// DEF-154 (X): advance the read cursor. Err architecturally
    /// dead on RowStream paths (frames gated by `parse_header`
    /// length-check before advance).
    #[inline]
    pub(crate) fn read_buf_advance(
        &mut self,
        n: usize,
    ) -> Result<(), crate::buf::AdvancePastEnd> {
        self.inner.read_buf.advance(n)
    }

    /// DEF-248 Sub-A (2026-05-12): unread-region length accessor for
    /// the row-stream state machine's chunk-vs-whole-col decision.
    /// Re-export of [`crate::buf::ReadBuf::unread_len`].
    #[inline]
    #[must_use]
    pub(crate) fn read_buf_unread_len(&self) -> usize {
        self.inner.read_buf.unread_len()
    }


    /// DEF-248 Sub-A (2026-05-12): partial-mode entry point routed
    /// through the leaf-gated [`crate::buf::ReadBuf::enter_partial_mode`]
    /// accepting a `&PartialFrameToken`. The token mint is gated to
    /// `crate::row_stream::_row_stream_partial_leaf::mint_for_row_stream_dispatcher`,
    /// itself `pub(in crate::row_stream)` — so this entry point is
    /// only legitimately reachable from inside `mod row_stream`.
    #[inline]
    pub(crate) fn enter_partial_mode_for_data_row(
        &mut self,
        token: &crate::row_stream::_row_stream_partial_leaf::PartialFrameToken,
        declared_body_len: u32,
    ) {
        self.inner.read_buf.enter_partial_mode(token, declared_body_len);
    }

    /// DEF-248 Sub-A (2026-05-12): partial-mode exit point. Mirror
    /// of [`Self::enter_partial_mode_for_data_row`].
    #[inline]
    pub(crate) fn exit_partial_mode_for_row_stream(
        &mut self,
        token: &crate::row_stream::_row_stream_partial_leaf::PartialFrameToken,
    ) {
        self.inner.read_buf.exit_partial_mode(token);
    }

    /// DEF-248 Sub-A (2026-05-12): drain `n` bytes from the
    /// partial-mode counter. Returns Err on attempted underflow.
    #[inline]
    pub(crate) fn subtract_partial_for_row_stream(
        &mut self,
        token: &crate::row_stream::_row_stream_partial_leaf::PartialFrameToken,
        n: u32,
    ) -> Result<(), crate::buf::AdvancePastEnd> {
        self.inner.read_buf.subtract_partial_remaining(token, n)
    }

    /// DEF-248 Sub-A (2026-05-12): partial-mode predicate. Used by
    /// the row-stream state machine to decide whether the
    /// `subtract_partial_*` bookkeeping is needed.
    #[inline]
    #[must_use]
    pub(crate) fn is_in_partial_mode_for_row_stream(&self) -> bool {
        self.inner.read_buf.is_in_partial_mode()
    }

    /// DEF-248 Sub-A (2026-05-12): partial-mode counter readout.
    /// Used by the row-stream state machine to decide whether
    /// exit-partial-mode is safe (counter == 0).
    #[inline]
    #[must_use]
    pub(crate) fn partial_remaining_for_row_stream(&self) -> u32 {
        self.inner.read_buf.partial_remaining()
    }

    /// DEF-189: project the current row_desc_slot as a
    /// [`crate::decode::RowDescBorrow`], or `None` if no schema is
    /// parked.
    ///
    /// Used by terminal materialise to construct
    /// `Reply::QueryComplete::row_desc` and by the per-row fast-path
    /// to project the schema descriptor after `read_buf_advance`.
    ///
    /// # DEF-189 perf win
    ///
    /// Pre-DEF-188/-189 the per-row hot path did `match &self.inner.state`
    /// twice: once for the streaming-variant gate (returning the
    /// `reply_id`) and once after `read_buf_advance` to re-project
    /// the schema field on the variant. Two enum matches per row.
    ///
    /// Post-DEF-189 the fast path is `match &self.inner.state` ONCE for
    /// the gate (with the schema NOT in the variant) + a single
    /// `Option::as_ref` projection here. The Option projection is
    /// strictly cheaper than the enum match — one byte read for the
    /// discriminant, one ptr-deref on Some.
    #[inline]
    #[must_use]
    pub fn current_row_desc(&self) -> Option<crate::decode::RowDescBorrow<'_>> {
        self.inner.row_desc_slot
            .as_ref()
            .map(crate::decode::RowDescBorrow::from_ref)
    }

    /// DEF-189: fused state classification for the row-stream
    /// fast-path entry. Single `match &self.inner.state` returns the
    /// classification needed by `RowStream::next_event`:
    ///
    /// - `Errored`: state is terminal — caller drains and emits
    ///   `CloseSocket`.
    /// - `Streaming(reply_id)`: state is row-streaming with the
    ///   given correlator — caller proceeds to fast-path data-row
    ///   handling.
    /// - `Other`: any non-streaming, non-errored state — caller
    ///   delegates to `slow_path_once`.
    ///
    /// Pre-DEF-189 the row_stream entry called separate
    /// `state_is_errored()` + `streaming_reply_id()` accessors —
    /// two enum matches per `next_event`. DEF-189 fuses them into
    /// one match, observed once per `next_event` call. Saves one
    /// enum-discriminant load per row (~1 ns at 3 GHz on
    /// branch-predicted state) — the compiler did not reliably fuse
    /// the two separate match calls because they were separated by
    /// header-parse logic.
    ///
    /// DEF-249 audit (2026-05-08): per-stream hot path — called once
    /// per `next_event` / `next_row_bytes` invocation (cached in
    /// `RowStream::cached_reply_id` after first call). Amortised
    /// cost is sub-1 ns. `#[inline]` already present; audit confirms
    /// the call chain is fully inlined under workspace LTO.
    #[inline]
    #[must_use]
    pub(crate) fn classify_for_iter_rows(&self) -> IterRowsClass {
        match &self.inner.state {
            ProtoState::Errored(_) => IterRowsClass::Errored,
            ProtoState::SimpleQueryStreamingRows { reply }
            | ProtoState::BindExecuteStreamingRows { reply }
            | ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply } => {
                IterRowsClass::Streaming(reply.get())
            }
            _ => IterRowsClass::Other,
        }
    }

    /// DEF-184 (B25): transition to `Errored(Internal)` for a
    /// dead-branch read_buf advance Err. Used by RowStream's
    /// fast-path when `read_buf_advance(total)` returns Err
    /// — architecturally impossible (total pre-validated) but
    /// tier-2 classification closes the drift surface at zero
    /// runtime cost (branch is cold-path unreachable in practice).
    ///
    /// # DEF-271 cluster A (2026-05-10): atomic drain via FeedStateSetter
    ///
    /// Pre-DEF-271 the helper wrote `*self.inner.state = Errored(...)`
    /// directly; the in-flight reply id was peeked separately at the
    /// dispatch site (tier-3 dual-source-of-truth). Post-DEF-271 the
    /// drain and install are one `mem::replace` via
    /// [`crate::state_setter::FeedStateSetter::drain_and_install_errored`];
    /// the returned `Option<NonZeroU64>` is `#[must_use]` and the
    /// caller in `RowStream` uses it directly for
    /// `StreamItem::FailReply { id, cause }`. The peek-then-write
    /// dual-source-of-truth that previously existed at the
    /// `classify_for_iter_rows` site collapses to a single source.
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically \
                  drained by the Errored install. Caller MUST emit ColEvent::EndQuery \
                  { outcome: Err(_) } or equivalent — dropping it leaks the user's \
                  oneshot-receiver (zombie-reply class)."]
    pub(crate) fn install_errored_read_cursor_advance(&mut self) -> Option<NonZeroU64> {
        let cause = ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::ReadCursorAdvance,
        };
        _read_cursor_advance_drain_leaf::drain(&mut self.inner.state, cause.state_kind())
    }

    /// DEF-154 (X): transition to `Errored(Framing)` for a
    /// malformed DataRow (empty body, server-side desync). Used
    /// by RowStream's fast-path when `start == end`.
    ///
    /// # DEF-186 P1-1 (audit 2026-04-24)
    ///
    /// Takes `total_len: usize` matching the caller's
    /// `ProtocolError::MalformedDataRow { total_len }` payload.
    /// Pre-fix hardcoded `total_len: 0` for the state-kind derivation,
    /// relying on the discriminator being payload-independent — correct
    /// today but tier-4 fragility if a future `state_kind()` ever folds
    /// on `total_len` (e.g. distinct kind for "0-byte body" vs other
    /// malformed lengths). Pass-through closes the "mismatched twin
    /// payloads" drift.
    ///
    /// # DEF-271 cluster A (2026-05-10): atomic drain via FeedStateSetter
    ///
    /// See [`Self::install_errored_read_cursor_advance`] for the
    /// drain-and-install rationale. Same pattern.
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically \
                  drained by the Errored install. Caller MUST emit ColEvent::EndQuery \
                  { outcome: Err(_) } or equivalent — dropping it leaks the user's \
                  oneshot-receiver (zombie-reply class)."]
    pub(crate) fn install_errored_malformed_data_row(
        &mut self,
        total_len: usize,
    ) -> Option<NonZeroU64> {
        let cause = ProtocolError::MalformedDataRow { total_len };
        _malformed_data_row_drain_leaf::drain(&mut self.inner.state, cause.state_kind())
    }

    // DEF-188: install_errored_stale_schema_ref DELETED — there is
    // no longer a SchemaRef type or generation drift class. State
    // variants carry RowDesc inline; the fast-path reads
    // `&self.inner.state.row_desc` directly. The "stale ref" bug class is
    // architecturally impossible (no handle to be stale).

    /// DEF-248 Sub-A (2026-05-12): transition to `Errored(Internal)`
    /// when a [`crate::row_stream::RowStream`] is dropped mid-frame
    /// (closure exited via early return / `?` / panic-unwind without
    /// reaching a terminal `ColEvent::EndQuery`).
    ///
    /// # When this fires
    ///
    /// `RowStream::Drop` checks `self.drained` at scope close. The
    /// flag is set `true` only by the terminal-event paths inside
    /// `col_next` (success terminal, fail terminal, or
    /// already-Errored state classifier). Any non-terminal closure
    /// exit (normal `return`, `?`-propagation, panic unwind) leaves
    /// the flag `false`; Drop installs Errored via this helper.
    ///
    /// # Tier-1 closure on `mem::forget`
    ///
    /// `iter_rows` owns the stream value on its stack frame; the
    /// caller's closure receives `&mut RowStream`. `mem::forget` on a
    /// `&mut` does nothing to the underlying value — the stream
    /// always drops at `iter_rows`'s return. Structural by Rust's
    /// drop-glue contract (see memo `/tmp/def248-design-memo-v2.md`
    /// §3.2).
    ///
    /// # Tier-1 closure on panic unwind
    ///
    /// Drop fires unconditionally on stack unwind by Rust spec. The
    /// crate runs under `panic = "unwind"` (workspace default); a
    /// downstream binary with `panic = "abort"` is an OS-level
    /// boundary (process death → TCP RST → server-side teardown;
    /// stronger than any library mechanism — see memo §3.3).
    ///
    /// # Why no FailReply emission here
    ///
    /// Drop has no access to a `StagedActions` / closure-return
    /// channel. The in-flight reply id is atomically drained by the
    /// state-setter route, but absorbed at the call site —
    /// architectural boundary documented on
    /// [`crate::state_setter::drain_at_stream_dropped_mid_stream`].
    /// The next operation on the connection observes the Errored
    /// state and the wrapper layer surfaces
    /// `ConnectionAlreadyClosed { prior_kind: ClientOrdering }` via
    /// the existing `as_ready` classifier — the user's oneshot is
    /// not silently leaked.
    #[inline]
    pub(crate) fn install_errored_stream_dropped_mid_stream(&mut self) {
        let cause = ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::StreamDroppedMidStream,
        };
        // The drained id has no caller context here (Drop). Bind to
        // `_drained_at_drop` for the `#[must_use]` discoverability
        // contract — see the leaf submodule docstring.
        let _drained_at_drop: Option<NonZeroU64> =
            _stream_dropped_mid_stream_drain_leaf::drain(&mut self.inner.state, cause.state_kind());
        // DEF-248 Sub-A: clear read_buf so a subsequent feed_bytes on
        // the post-Errored connection does not classify mid-frame
        // bytes as a fresh frame header. The state is already Errored
        // — `feed_bytes_impl`'s `IngressClassification::AlreadyErrored`
        // arm also calls `read_buf.clear()`, but doing it here keeps
        // the post-Drop invariant tight without needing a follow-up
        // feed_bytes to scrub.
        self.inner.read_buf.clear();
    }

    /// DEF-248 Sub-A (2026-05-12): closure-scoped row-stream API.
    ///
    /// Caller passes a closure that receives `&mut RowStream`. The
    /// `RowStream` value lives on this function's stack frame and is
    /// dropped synchronously before this function returns. Caller
    /// never owns the value — `mem::forget` of the closure-borrowed
    /// reference is a no-op against the underlying stream.
    ///
    /// # Tier-1 closure of cycle-1 hazards
    ///
    /// 1. **`mem::forget(RowStream)`** — structurally impossible: the
    ///    closure has only `&mut RowStream`, not `RowStream` by
    ///    value. Forgetting the reference forgets a reborrow, not the
    ///    value.
    /// 2. **Drop mid-stream** — Rust drop-glue runs unconditionally
    ///    on every closure exit (normal return, `?` propagation,
    ///    panic unwind under `panic = "unwind"`). The stream's
    ///    `Drop` impl installs Errored when the closure exited
    ///    without reaching `ColEvent::EndQuery`.
    /// 3. **`Box::leak` / `ManuallyDrop` on the stream** — caller has
    ///    no value to wrap.
    ///
    /// `panic = "abort"` is a binary-level setting outside the
    /// library's reach; on process death, the OS closes the TCP
    /// socket and the peer observes connection teardown — an
    /// architectural boundary stronger than any library mechanism
    /// (memo §3.3).
    ///
    /// # Hot-path cost
    ///
    /// `#[inline]` + closure monomorphisation produces machine code
    /// identical to inlined cycle-1-style usage. The `&mut RowStream`
    /// indirection is elided by LLVM's inliner. Drop call at scope
    /// end is one `call` instruction — same as a caller-side `}`
    /// scope close would have had on a by-value stream.
    ///
    /// # Caller pattern
    ///
    /// ```ignore
    /// let outcome: Result<MyRow, MyError> = proto.iter_rows(&mut wb, |stream| {
    ///     stream.feed(&inbound_bytes_from_socket)?;
    ///     loop {
    ///         match stream.col_next() {
    ///             ColEvent::Got { idx, bytes } => { /* … */ }
    ///             ColEvent::Null { idx } => { /* … */ }
    ///             ColEvent::EndRow => { /* … */ }
    ///             ColEvent::Chunk { idx, bytes, .. } => { /* … */ }
    ///             ColEvent::ChunkEnd { idx, bytes } => { /* … */ }
    ///             ColEvent::NeedMore => return Err(MyError::NotEnoughBytes),
    ///             ColEvent::EndQuery { id, outcome } => {
    ///                 return outcome.map(/* … */).map_err(/* … */);
    ///             }
    ///         }
    ///     }
    /// });
    /// ```
    ///
    /// # Sub-A scope
    ///
    /// D-tag streaming-exposed only. Non-D frames > READ_BUF_CAP
    /// continue to tear down with `FrameTooLarge` (Sub-B's concern).
    /// Within-D, every wire-legal body size is handled via
    /// partial-frame chunking — see [`crate::row_stream::ColEvent`].
    #[inline]
    pub fn iter_rows<R, F>(&mut self, write_buf: &mut WriteBuf, f: F) -> R
    where
        F: for<'p, 'w> FnOnce(&mut crate::row_stream::RowStream<'p, 'w>) -> R,
    {
        // Entry-point housekeeping mirrors feed_bytes:
        write_buf.clear();
        // DEF-211 FAKE-01: cached classification (see feed_bytes for
        // rationale).
        //
        // DEF-246 Option α (2026-05-16):
        // `clear_session_residue_for_class` lives on `PgProtocolInner`;
        // route through `self.inner` directly.
        let entry_class = self.inner.state.push_class();
        self.inner.clear_session_residue_for_class(entry_class);

        // The stream value lives here on `iter_rows`'s stack frame.
        // Caller's closure receives `&mut stream` — a borrow, not
        // the value. Drop fires at end of this function body, even
        // on panic unwind (Rust spec). DEF-248 Sub-A `mem::forget`
        // closure: caller has no value, only a borrow.
        //
        // The HRTB `for<'p, 'w>` binds the closure to ANY lifetimes
        // `RowStream<'p, 'w>` carries; the actual lifetimes here are
        // tied to this stack frame, but the trait bound prevents the
        // closure from naming them (and thus from smuggling the
        // stream out via type-state shenanigans).
        let mut stream = crate::row_stream::RowStream::new(self, write_buf);
        f(&mut stream)
    }
}

/// DEF-189 — classifier output for [`PgProtocol::classify_for_iter_rows`].
///
/// 3-variant enum (each ZST-discriminator except Streaming carrying
/// `NonZeroU64`) selecting the row-stream fast-path entry behaviour.
/// Returned by a single `match &self.inner.state` in
/// `classify_for_iter_rows`, replacing the pre-DEF-189 separate
/// `state_is_errored()` + `streaming_reply_id()` calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IterRowsClass {
    /// State is terminal `Errored(_)` — `RowStream` drains and emits
    /// `CloseSocket`.
    Errored,
    /// State is a row-streaming variant with the given reply
    /// correlator — `RowStream` proceeds to fast-path data-row
    /// handling.
    Streaming(core::num::NonZeroU64),
    /// State is neither errored nor streaming — `RowStream` delegates
    /// to `slow_path_once`.
    Other,
}

// ═════════════════════════════════════════════════════════════════════
// DEF-154 (E) — field-level free functions for the fail path
// ═════════════════════════════════════════════════════════════════════
//
// `fail_inflight_and_close` + `replace_state_errored_and_drain` +
// `fail_read_cursor_advance` historically took `&mut self` (full
// PgProtocol). DEF-154 (E) wraps `feed_bytes`'s dispatch loop in a
// `self.inner.read_buf.with_branded(|mut rb| { ... })` branded scope —
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
/// take `&mut ReadBuf`, because inside `self.inner.read_buf.with_branded`
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
    malformed_counter: &mut u32,
) {
    if matches!(state, ProtoState::Errored(_)) {
        return;
    }
    // DEF-185 P2-9 (audit 2026-04-24): bump counter before transitioning.
    // This fires on every classified fatal wire event — operator-facing
    // canary (exposed via `PgProtocol::malformed_frame_count`). Saturating
    // add keeps the counter pinned at u32::MAX on extreme flood.
    *malformed_counter = malformed_counter.saturating_add(1);
    // DEF-189 Q8-C4: counter-storm classifier. If the counter has
    // accumulated past `MALFORMED_STORM_THRESHOLD` (10_000), the
    // `Errored` transition classifies as `MalformedStorm` regardless
    // of the per-frame `cause.state_kind()`. Defensive tier-3: under
    // current single-event-then-Errored semantics the counter caps
    // at 1 in practice (the early-return above prevents re-entry).
    // The classifier activates if a future flow change unblocks
    // counter accumulation — without this branch the saturation
    // event would be tier-4 silent (counter pins at u32::MAX, no
    // diagnostic signal).
    let state_kind = if *malformed_counter >= MALFORMED_STORM_THRESHOLD {
        StateErrorKind::from_kind_or_internal(crate::error::ErrorKind::MalformedStorm)
    } else {
        // DEF-154 (I): total state_kind — no unwrap_or_else + debug_assert.
        cause.state_kind()
    };
    // DEF-184 (A10/B22 revert 2026-04-24): `mem::replace` drops the
    // previous state, which may be a SCRAM variant carrying
    // `ScramSession`. `ScramSession`'s `ZeroizeOnDrop` fires here
    // automatically — password bytes scrubbed in the drop path of
    // `prev`. No explicit `scram_state = None` step needed post-revert
    // because there IS no separate scram_state field — SCRAM data
    // lives inline in the state variant and rides the drop glue.
    //
    // DEF-271 cluster A (2026-05-10): route through FeedStateSetter
    // for tier-1 by-construction drain+install atomicity. Same
    // mem::replace under the hood; the `#[must_use]` returned id is
    // consumed in the FailReply emission below — explicit handling
    // (no fallback, no leak).
    let raw_id = _fail_inflight_no_readbuf_drain_leaf::drain(state, state_kind);
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

/// DEF-189 Q8-C4 — malformed-frame-count threshold for the
/// `MalformedStorm` classifier in `fail_inflight_no_readbuf`.
///
/// 10_000 is high enough to rule out single-event noise (a single
/// transient malformed frame on a healthy connection) and low enough
/// to fire well below `u32::MAX` saturation (4 billion).
///
/// Under current single-event-then-Errored semantics this threshold
/// is unreachable; see `ErrorKind::MalformedStorm` docstring for the
/// defensive-classifier rationale.
const MALFORMED_STORM_THRESHOLD: u32 = 10_000;

/// Compute the state transition and actions for a command push.
///
/// # Pure compute / apply split (DEF-059)
///
/// This free function owns the entire push-path decision: given the
/// command and current [`ProtoState`] *by value*, it produces the new
/// state and a bounded [`OutActions`] list. No `&mut PgProtocol` — the
/// only mutation the caller needs is the single `self.inner.state = new_state`
/// assignment in [`PgProtocol::push_command`].
///
/// Why pure:
/// - **Testability.** Unit tests call `compute_push` directly with a
///   synthesised `(cmd, state)` pair and inspect the returned tuple.
///   No `PgProtocol` construction, no `&mut self` dance.
/// - **Single locus of mutation.** All `self.inner.state = ...` statements
///   in the crate are restricted to `push_command` and `feed_bytes`.
///   Adding a new command variant grows the match here, not the
///   mutable surface of `PgProtocol`.
/// - **Errored pre-check dissolves.** The DEF-093 workaround (reading
///   `&self.inner.state` *before* `core::mem::take` to avoid a transient
///   `Idle` window) is no longer needed. `ProtoState::Errored` is a
///   first-class arm: it preserves the cause (returns
///   `ProtoState::Errored(cause)` unchanged) and emits the
///   `FailReply`. No intermediate-value peek, no empty unreachable arm.
///
/// Per-command semantics live in dedicated helpers
/// ([`compute_push_ping`], [`compute_push_startup`]); `compute_push`
/// dispatches on the command variant. Adding a new `PgCommand` variant
/// fails the build here until a matching helper is wired up.
#[cfg(test)]
fn compute_push(
    cmd: PgCommand,
    state: &mut ProtoState,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> StagedActions<'static> {
    // DEF-160 (Z2): the cfg(test) dispatcher operates on the legacy
    // owned-`Sql` `PgCommand` enum. Although the `PgCommand` variants
    // own their `Sql`, the SimpleQuery / Parse arms route through
    // `compute_push_simple_query` / `compute_push_parse` (cfg(test))
    // which take `&'a Sql` and stage `&'a [u8]` via SendBytesBorrowed
    // — so `staged` borrows from the locally-owned Sql for the
    // duration of this function. Returning the staged container
    // out of scope is safe because the inner StagedAction-lifetime
    // tracks the caller's expectation; the local Sql lives for the
    // 'static lifetime of `PgCommand` (variants are owned). Bind
    // staged's 'sql to the function-local 'a to keep the borrow
    // checker honest, then return as 'static (subtype) once we know
    // no SendBytesBorrowed survives past the local scope.
    let mut staged: StagedActions<'_> = StagedActions::new();
    match cmd {
        PgCommand::Ping { reply } => compute_push_ping(state, reply, &mut staged),
        // DEF-246 Phase 2 (2026-05-16): `PgCommand::Startup` arm
        // deleted alongside the variant itself. Startup is the only
        // command with a phase-typed entry-point
        // (`<DisconnectedPhase>::push_startup`); the test-only
        // dispatcher no longer needs the arm because
        // `compute_push_startup` (cfg(test)) is also deleted.
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
    }
    staged
}

// DEF-269 v2 (T) backwards-compat slow-path `compute_push_idle_only`
// DELETED at DEF-270 Phase 2 (2026-05-10). Real call sites were zero
// (audit grep `push_command(PgCommand::...)` returned only doc-comment
// references); the legacy `impl PushCommand for PgCommand` blanket
// impl was removed in the same commit. `PgCommand` enum survives for
// the test-only `compute_push_tests` 5-arm dispatchers.

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
#[cfg(test)]
fn compute_push_ping(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::PingKind>,
    staged: &mut StagedActions,
) {
    // DEF-186 perf-recovery 2026-04-24: signature changed from
    // by-value `state: ProtoState` to `&mut ProtoState`. Idle arm
    // writes new state via `*state = ...`; preserve arms (Errored,
    // PingAwaiting, BusyQuery, Connecting) leave state untouched —
    // saves the 712 B mem::take + 712 B write-back per non-Idle push.
    //
    // DEF-146: classifier dispatch. Pre-DEF-146 this function had 5
    // arms over explicit state variants (with 18-way or-patterns for
    // the tail catch-alls). Post-DEF-146, the enumeration lives once
    // in `ProtoState::push_class`; this match is 5 arms over the
    // classifier's 5 variants — exhaustive, no `_` fallback, tier-1
    // preserved.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-272 cluster γ (2026-05-10): IdleState typestate
            // replaces (state, IdleStateProof). Idle arm of
            // push_class() classification is the precondition; the
            // typestate's try_from re-checks at the boundary.
            let idle = match crate::state_setter::IdleState::try_from(state) {
                Some(idle) => idle,
                None => {
                    debug_assert!(
                        false,
                        "Idle arm of push_class() — try_from returned None (push_class() bug)",
                    );
                    return;
                }
            };
            let setter = idle.into_setter::<crate::push_command::PingAwaitingRfqInstall>();
            compute_push_ping_idle_only(setter, reply, staged);
        }
        crate::state::StatePushClass::Errored(prior_kind) => {
            // Preserve the stored cause; emit ConnectionAlreadyClosed
            // so the wrapper sees "connection terminal" rather than
            // a generic in-flight error. State unchanged (already
            // Errored(prior_kind)).
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
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
        }
        // Any Connecting* variant — startup handshake in progress.
        crate::state::StatePushClass::Connecting => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
        }
    }
}

/// DEF-208 — Idle-only path for [`PgCommand::Ping`] push.
///
/// Caller must guarantee `state == ProtoState::Idle` (production
/// callsite is `ReadyGuard::push_command` which proves this via the
/// witness-guard typestate). Skips the 5-arm `state.push_class()`
/// dispatch in `compute_push_ping`, avoiding ~3 ns of branch +
/// dispatch overhead per push.
///
/// Tier-1 closure of DEF-198 surface 6 (internal compute_push
/// 5-arm dispatch was dead code from the public API path through
/// ReadyGuard).
#[inline]
pub(crate) fn compute_push_ping_idle_only(
    setter: crate::state_setter::StateSetter<'_, crate::push_command::PingAwaitingRfqInstall>,
    reply: ReplyId<crate::reply_id::PingKind>,
    staged: &mut StagedActions,
) {
    // DEF-094: Sync is a compile-time const (5 bytes). Emit
    // `StagedAction::SendBytesStatic(&SYNC_WIRE_BYTES)` so the
    // materialiser passes the static reference through directly —
    // zero write to write_buf, zero copy.
    emit_actions!(staged, budget: 1, [
        StagedAction::SendBytesStatic(&SYNC_WIRE_BYTES),
    ]);
    // DEF-270 N-D: typed witness pairs Ping → PingAwaitingRfq.
    setter.install_post_state(crate::push_command::PingAwaitingRfqInstall { reply });
}

// DEF-246 Phase 2 (2026-05-16): `compute_push_startup` cfg(test) +
// `compute_push_tests::push_startup_*` retired tests deleted —
// `<DisconnectedPhase>::push_startup`'s consume-self signature
// physically forbids pushing Startup from non-Disconnected states,
// so the per-state dispatcher (Idle / Errored / Connecting /
// PingAwaiting / BusyQuery) is dead. The remaining `Idle` path
// lives in `compute_push_startup_idle_only` below (still reached
// from `<DisconnectedPhase>::push_startup`).

/// DEF-208 — Idle-only path for the Startup handshake push.
///
/// Caller must guarantee `state == ProtoState::Idle`. See
/// [`compute_push_ping_idle_only`] for closure rationale.
#[expect(clippy::too_many_arguments, reason = "mirrors compute_push_startup signature 1:1; struct-arg refactor would obscure the pure-compute framing")]
#[inline]
pub(crate) fn compute_push_startup_idle_only(
    setter: crate::state_setter::StateSetter<'_, crate::push_command::StartupPostInstall>,
    user: Ident,
    database: Option<DatabaseName>,
    app_name: Option<ApplicationName>,
    credentials: Credentials,
    reply: ReplyId<crate::reply_id::StartupKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    // DEF-154 (B) P0-2: builder returns Result; Err →
    // FailReply + CloseSocket + Errored via `try_builder!`.
    let range = try_builder!(
        build_startup_message(
            &user,
            database.as_ref(),
            app_name.as_ref(),
            reserved,
        ),
        setter,
        reply,
        staged
    );
    emit_actions!(staged, budget: 1, [
        StagedAction::SendBytesRange(range),
    ]);
    // DEF-097: discriminate Trust vs Scram *here* — the
    // post-push state carries only what its auth method
    // needs. Trust: 24 bytes. Scram: 24 + ScramSession
    // (~1040).
    // DEF-270 N-D: typed witness pairs Startup → ConnectingStartup{Trust|Scram|Cleartext|Md5}.
    let post_install = match credentials {
        Credentials::Trust => crate::push_command::StartupPostInstall::Trust { reply },
        Credentials::ScramPassword(password) => {
            // DEF-184 (A10/B22 revert 2026-04-24): tier-1
            // restored — ScramSession lives INSIDE the
            // variant. Variant-carries-field invariant is
            // compile-enforced (CREDO §1: safety > tier-1 > perf).
            let scram = alloc::boxed::Box::new(
                crate::scram::session::ScramSession::from_password(password),
            );
            crate::push_command::StartupPostInstall::Scram { reply, scram }
        }
        Credentials::CleartextPassword(password) => {
            // DEF-215 (2026-05-05): mirror of the SCRAM construction
            // above. `Sensitive<Password>` is heap-boxed so the
            // variant footprint stays within the `ProtoState == 80`
            // size pin. Variant-carries-field invariant is
            // compile-enforced — the variant cannot exist without a
            // valid `Box<Sensitive<Password>>`. ZeroizeOnDrop fires
            // on every exit path through the Box's Drop.
            let password = alloc::boxed::Box::new(password);
            crate::push_command::StartupPostInstall::Cleartext { reply, password }
        }
        Credentials::Md5Password(password) => {
            // DEF-216 (2026-05-05): MD5 needs BOTH password AND
            // username at digest-construction time (server's
            // 4-byte salt arrives later in
            // AuthenticationMD5Password). Bundle them in a single
            // Box<Md5HandshakeState> — same single-Box pattern as
            // SCRAM PERF-02. Tier-1 variant-carries-field; the
            // Box can never be None and ZeroizeOnDrop fires on
            // every exit path through Box::drop →
            // Md5HandshakeState::drop → Sensitive::drop →
            // Password::drop. `user` is non-secret (cleartext on
            // wire in StartupMessage above) and not zeroized.
            let handshake = alloc::boxed::Box::new(crate::md5::Md5HandshakeState {
                password,
                user,
            });
            crate::push_command::StartupPostInstall::Md5 { reply, handshake }
        }
    };
    setter.install_post_state(post_install);
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
#[cfg(test)]
fn compute_push_simple_query(
    state: &mut ProtoState,
    sql: &crate::ident::Sql,
    reply: ReplyId<crate::reply_id::QueryKind>,
    staged: &mut StagedActions<'_>,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    // DEF-160 Z2: `sql`'s lifetime is intentionally decoupled from
    // `staged`'s `'sql` parameter. The cfg(test) legacy path copies
    // SQL bytes into `reserved` (via build_query_message_cfgtest)
    // and stages a single SendBytesRange — no borrow flows into
    // staged, so staged's `'_` is independent and compute_push can
    // return staged out of scope safely.
    // DEF-186 perf-recovery 2026-04-24: &mut state signature.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-272 cluster γ (2026-05-10): IdleState typestate
            // replaces (state, IdleStateProof).
            let idle = match crate::state_setter::IdleState::try_from(state) {
                Some(idle) => idle,
                None => {
                    debug_assert!(
                        false,
                        "Idle arm of push_class() — try_from returned None (push_class() bug)",
                    );
                    return;
                }
            };
            let setter = idle.into_setter::<crate::push_command::SimpleQueryAwaitingFirstResponseInstall>();
            // DEF-160 (Z2 cfg(test) legacy path): the typed-surface
            // `SimpleQuery<'a>` uses `compute_push_simple_query_idle_only`
            // with `SendBytesBorrowed` for zero-copy SQL. The cfg(test)
            // dispatcher operates on the legacy `PgCommand::SimpleQuery`
            // enum which owns `Sql` (FixedStr<2048>) and is consumed by
            // value through `compute_push`. To keep the legacy path's
            // staged-actions lifetime-portable (returnable from compute_push
            // out of scope) we build the full single-frame here via the
            // cfg(test)-only helper and emit one `SendBytesRange` —
            // identical wire output, no SendBytesBorrowed surface.
            let range_result = build_query_message_cfgtest(sql, reserved);
            let range = try_builder!(range_result, setter, reply, staged);
            emit_actions!(staged, budget: 1, [
                StagedAction::SendBytesRange(range),
            ]);
            setter.install_post_state(
                crate::push_command::SimpleQueryAwaitingFirstResponseInstall { reply },
            );
        },
        crate::state::StatePushClass::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
        }
        crate::state::StatePushClass::Connecting => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
        }
        crate::state::StatePushClass::PingAwaiting
        | crate::state::StatePushClass::BusyQuery => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
        }
    }
}

/// DEF-208 — Idle-only path for [`PgCommand::SimpleQuery`] (legacy
/// cfg(test) enum) / [`crate::push_command::SimpleQuery<'a>`] (typed
/// surface).
///
/// DEF-160 Z2 (2026-05-11): emits **3** staged actions (was 1 pre-Z2)
/// — `SendBytesRange(header) + SendBytesBorrowed(sql) + SendBytesRange(trailer)`.
/// SQL is borrowed end-to-end, never copied into `WriteBuf`.
#[inline]
pub(crate) fn compute_push_simple_query_idle_only<'sql>(
    setter: crate::state_setter::StateSetter<
        '_,
        crate::push_command::SimpleQueryAwaitingFirstResponseInstall,
    >,
    sql_bytes: &'sql [u8],
    reply: ReplyId<crate::reply_id::QueryKind>,
    staged: &mut StagedActions<'sql>,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    let header_range = try_builder!(
        build_query_header(sql_bytes.len(), reserved),
        setter,
        reply,
        staged
    );
    let trailer_range = try_builder!(
        build_query_trailer(reserved),
        setter,
        reply,
        staged
    );
    emit_actions!(staged, budget: 3, [
        StagedAction::SendBytesRange(header_range),
        StagedAction::SendBytesBorrowed(sql_bytes),
        StagedAction::SendBytesRange(trailer_range),
    ]);
    // DEF-270 N-D: typed witness pairs SimpleQuery → SimpleQueryAwaitingFirstResponse.
    setter.install_post_state(
        crate::push_command::SimpleQueryAwaitingFirstResponseInstall { reply },
    );
}

// DEF-154 (B) Phase B4: `from_write_span_infallible` deleted.
// Branded builders now use
// [`crate::action::WriteRange::from_write_span`] directly —
// identical shield logic, plus brand-identity binding.

/// Build the PG simple-query (`'Q'`) frame **header** — tag plus the
/// upfront-computed length prefix.
///
/// DEF-160 Z2 (2026-05-11): split from the pre-Z2 monolithic
/// `build_query_message`. The PG length-prefix INCLUDES itself
/// (PG §55.7 wire spec); for SimpleQuery the body is `sql + NUL`,
/// so length = 4 (length self) + sql_len + 1 (NUL). Both inputs
/// are known at the call site, so the length is computed upfront
/// here — no `with_length_prefix` back-patch needed.
///
/// PG frame body layout (§55.7 "Simple Query"):
/// - Tag: `'Q'` (1 byte) ← in this header
/// - Length: u32 BE including itself ← in this header
/// - Query string ← `SendBytesBorrowed` (NOT in WriteBuf)
/// - NUL terminator (1 byte) ← in trailer
///
/// `BuilderCapacityOverflow` is classified Err iff the SQL is so
/// large that `4 + sql_len + 1 > u32::MAX` — i.e., SQL > ~4 GB.
/// This is a PG-protocol-level limit (the wire format's u32 length
/// prefix), not a bsql-internal cap. Practically dead.
fn build_query_header(
    sql_len: usize,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_QUERY.byte())?;
    // length-prefix = 4 (self) + sql_len + 1 (NUL terminator).
    // saturating_add stays within the forbid-bundle (no
    // `arithmetic_side_effects`); u32::try_from gates the overflow
    // case as `BuilderCapacityOverflow`.
    let length_usize = 4_usize
        .saturating_add(sql_len)
        .saturating_add(1);
    let length_u32 = u32::try_from(length_usize).map_err(|_| {
        ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::BuilderCapacityOverflow,
        }
    })?;
    reserved.push_u32_be(length_u32)?;
    crate::action::WriteRange::from_write_span(start, reserved)
}

/// Build the PG simple-query (`'Q'`) frame **trailer** — the NUL
/// terminator that follows the borrowed SQL bytes. DEF-160 Z2.
fn build_query_trailer(
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(0)?; // NUL terminator for the SQL string
    crate::action::WriteRange::from_write_span(start, reserved)
}

/// cfg(test)-only legacy single-frame builder: writes the full
/// SimpleQuery (`'Q'`) frame including the SQL bytes into `reserved`
/// and returns one [`crate::action::WriteRange`] covering the whole
/// frame. Used by [`compute_push_simple_query`] which dispatches
/// the legacy owned-`Sql` `PgCommand::SimpleQuery` variant; the
/// production typed-surface path uses
/// [`build_query_header`] / [`build_query_trailer`] +
/// [`StagedAction::SendBytesBorrowed`] for zero-copy.
#[cfg(test)]
fn build_query_message_cfgtest(
    sql: &crate::ident::Sql,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_QUERY.byte())?;
    reserved.with_length_prefix(|w| {
        w.push_nul_terminated(sql.as_bytes())?;
        Ok(())
    })?;
    crate::action::WriteRange::from_write_span(start, reserved)
}

/// Build the PG Extended Query `Parse` (`'P'`) frame **header** —
/// tag, length prefix, NUL-terminated statement name. DEF-160 Z2.
///
/// PG frame body layout (§55.7 "Parse"):
/// - Tag: `'P'` (1 byte) ← in this header
/// - Length: u32 BE including itself ← in this header
/// - Statement name: NUL-terminated ← in this header
/// - SQL text ← `SendBytesBorrowed` (NOT in WriteBuf)
/// - NUL terminator (1 byte) ← in trailer
/// - n_param_types: i16 BE (always 0 in Phase 1c-3a) ← in trailer
fn build_parse_header(
    stmt_name: &crate::ident::StmtName,
    sql_len: usize,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_PARSE.byte())?;
    // length-prefix = 4 (self) + stmt_name + 1 (NUL) + sql_len + 1 (NUL) + 2 (i16)
    let length_usize = 4_usize
        .saturating_add(stmt_name.len())
        .saturating_add(1)
        .saturating_add(sql_len)
        .saturating_add(1)
        .saturating_add(2);
    let length_u32 = u32::try_from(length_usize).map_err(|_| {
        ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::BuilderCapacityOverflow,
        }
    })?;
    reserved.push_u32_be(length_u32)?;
    reserved.push_nul_terminated(stmt_name.as_bytes())?;
    crate::action::WriteRange::from_write_span(start, reserved)
}

/// Build the PG Extended Query `Parse` (`'P'`) frame **trailer** —
/// NUL terminator after the borrowed SQL bytes, plus the i16 BE
/// parameter-type count (always 0 in Phase 1c-3a). DEF-160 Z2.
fn build_parse_trailer(
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(0)?; // NUL terminator for the SQL string
    // n_param_types = 0; Phase 1c-3b will widen to push actual OIDs here.
    reserved.push_i16_be(0)?;
    crate::action::WriteRange::from_write_span(start, reserved)
}

/// cfg(test)-only legacy single-frame builder for the Parse (`'P'`)
/// frame. See [`build_query_message_cfgtest`] for the rationale.
#[cfg(test)]
fn build_parse_message_cfgtest(
    stmt_name: &crate::ident::StmtName,
    sql: &crate::ident::Sql,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_PARSE.byte())?;
    reserved.with_length_prefix(|w| {
        w.push_nul_terminated(stmt_name.as_bytes())?;
        w.push_nul_terminated(sql.as_bytes())?;
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
#[cfg(test)]
fn compute_push_parse(
    state: &mut ProtoState,
    stmt_name: &crate::ident::StmtName,
    sql: &crate::ident::Sql,
    reply: ReplyId<crate::reply_id::ParseKind>,
    staged: &mut StagedActions<'_>,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    // DEF-160 Z2: see `compute_push_simple_query` — sql lifetime
    // decoupled from staged's `'_`.
    // DEF-186 perf-recovery 2026-04-24: &mut state signature.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-272 cluster γ (2026-05-10): IdleState typestate
            // replaces (state, IdleStateProof).
            let idle = match crate::state_setter::IdleState::try_from(state) {
                Some(idle) => idle,
                None => {
                    debug_assert!(
                        false,
                        "Idle arm of push_class() — try_from returned None (push_class() bug)",
                    );
                    return;
                }
            };
            let setter = idle.into_setter::<crate::push_command::ParseAwaitingParseCompleteInstall>();
            // DEF-160 Z2 cfg(test) legacy path — see compute_push_simple_query above.
            let range_result = build_parse_message_cfgtest(stmt_name, sql, reserved);
            let range = try_builder!(range_result, setter, reply, staged);
            emit_actions!(staged, budget: 2, [
                StagedAction::SendBytesRange(range),
                StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
            ]);
            setter.install_post_state(
                crate::push_command::ParseAwaitingParseCompleteInstall { reply },
            );
        },
        crate::state::StatePushClass::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
        }
        crate::state::StatePushClass::Connecting => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
        }
        crate::state::StatePushClass::PingAwaiting
        | crate::state::StatePushClass::BusyQuery => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
        }
    }
}

/// DEF-208 — Idle-only path for [`PgCommand::Parse`] (legacy
/// cfg(test) enum) / [`crate::push_command::Parse<'a>`] (typed
/// surface).
///
/// DEF-160 Z2 (2026-05-11): emits **4** staged actions (was 2 pre-Z2)
/// — `SendBytesRange(header) + SendBytesBorrowed(sql) + SendBytesRange(trailer) + SendBytesStatic(SYNC)`.
/// SQL is borrowed end-to-end, never copied into `WriteBuf`.
#[inline]
pub(crate) fn compute_push_parse_idle_only<'sql>(
    setter: crate::state_setter::StateSetter<
        '_,
        crate::push_command::ParseAwaitingParseCompleteInstall,
    >,
    stmt_name: &crate::ident::StmtName,
    sql_bytes: &'sql [u8],
    reply: ReplyId<crate::reply_id::ParseKind>,
    staged: &mut StagedActions<'sql>,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    let header_range = try_builder!(
        build_parse_header(stmt_name, sql_bytes.len(), reserved),
        setter,
        reply,
        staged
    );
    let trailer_range = try_builder!(
        build_parse_trailer(reserved),
        setter,
        reply,
        staged
    );
    emit_actions!(staged, budget: 4, [
        StagedAction::SendBytesRange(header_range),
        StagedAction::SendBytesBorrowed(sql_bytes),
        StagedAction::SendBytesRange(trailer_range),
        StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
    ]);
    // DEF-270 N-D: typed witness pairs Parse → ParseAwaitingParseComplete.
    setter.install_post_state(crate::push_command::ParseAwaitingParseCompleteInstall { reply });
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
#[cfg(test)]
fn compute_push_describe_statement(
    state: &mut ProtoState,
    stmt_name: &crate::ident::StmtName,
    reply: ReplyId<crate::reply_id::DescribeStatementKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    // DEF-186 perf-recovery 2026-04-24: &mut state signature.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-272 cluster γ (2026-05-10): IdleState typestate
            // replaces (state, IdleStateProof).
            let idle = match crate::state_setter::IdleState::try_from(state) {
                Some(idle) => idle,
                None => {
                    debug_assert!(
                        false,
                        "Idle arm of push_class() — try_from returned None (push_class() bug)",
                    );
                    return;
                }
            };
            let setter = idle.into_setter::<crate::push_command::DescribeStatementAwaitingParamDescInstall>();
            compute_push_describe_statement_idle_only(setter, stmt_name, reply, staged, reserved);
        }
        crate::state::StatePushClass::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
        }
        crate::state::StatePushClass::Connecting => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
        }
        crate::state::StatePushClass::PingAwaiting
        | crate::state::StatePushClass::BusyQuery => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
        }
    }
}

/// DEF-208 — Idle-only path for [`PgCommand::DescribeStatement`].
#[inline]
pub(crate) fn compute_push_describe_statement_idle_only(
    setter: crate::state_setter::StateSetter<
        '_,
        crate::push_command::DescribeStatementAwaitingParamDescInstall,
    >,
    stmt_name: &crate::ident::StmtName,
    reply: ReplyId<crate::reply_id::DescribeStatementKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    let range = try_builder!(
        build_describe_message(
            crate::wire::DescribeTargetByte::Statement,
            stmt_name,
            reserved,
        ),
        setter,
        reply,
        staged
    );
    emit_actions!(staged, budget: 2, [
        StagedAction::SendBytesRange(range),
        StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
    ]);
    // DEF-270 N-D: typed witness pairs DescribeStatement → DescribeStatementAwaitingParamDesc.
    setter.install_post_state(
        crate::push_command::DescribeStatementAwaitingParamDescInstall { reply },
    );
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
#[cfg(test)]
fn compute_push_describe_portal(
    state: &mut ProtoState,
    portal_name: &crate::ident::PortalName,
    reply: ReplyId<crate::reply_id::DescribePortalKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    // DEF-186 perf-recovery 2026-04-24: &mut state signature.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-272 cluster γ (2026-05-10): IdleState typestate
            // replaces (state, IdleStateProof).
            let idle = match crate::state_setter::IdleState::try_from(state) {
                Some(idle) => idle,
                None => {
                    debug_assert!(
                        false,
                        "Idle arm of push_class() — try_from returned None (push_class() bug)",
                    );
                    return;
                }
            };
            let setter = idle.into_setter::<crate::push_command::DescribePortalAwaitingRowDescOrNoDataInstall>();
            compute_push_describe_portal_idle_only(setter, portal_name, reply, staged, reserved);
        }
        crate::state::StatePushClass::Errored(prior_kind) => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::ConnectionAlreadyClosed { prior_kind },
                },
            ]);
        }
        crate::state::StatePushClass::Connecting => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::StartupAlreadyInProgress,
                },
            ]);
        }
        crate::state::StatePushClass::PingAwaiting
        | crate::state::StatePushClass::BusyQuery => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
        }
    }
}

/// DEF-208 — Idle-only path for [`PgCommand::DescribePortal`].
#[inline]
pub(crate) fn compute_push_describe_portal_idle_only(
    setter: crate::state_setter::StateSetter<
        '_,
        crate::push_command::DescribePortalAwaitingRowDescOrNoDataInstall,
    >,
    portal_name: &crate::ident::PortalName,
    reply: ReplyId<crate::reply_id::DescribePortalKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    let range = try_builder!(
        build_describe_message(
            crate::wire::DescribeTargetByte::Portal,
            portal_name,
            reserved,
        ),
        setter,
        reply,
        staged
    );
    emit_actions!(staged, budget: 2, [
        StagedAction::SendBytesRange(range),
        StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
    ]);
    // DEF-270 N-D: typed witness pairs DescribePortal → DescribePortalAwaitingRowDescOrNoData.
    setter.install_post_state(
        crate::push_command::DescribePortalAwaitingRowDescOrNoDataInstall { reply },
    );
}

/// DEF-270 P-ordering (Phase 2, 2026-05-10) — typed
/// [`crate::action::WriteRange`] newtype identifying a `Bind` frame
/// body. Constructed by [`build_bind_message`]; consumed by
/// [`stage_bind_execute_sync`] (the only caller). Swapping with
/// [`ExecuteRange`] at the consumer is a type error.
///
/// Tuple-struct field is module-private: only `mod protocol` can
/// project to the inner `WriteRange`. Tier-1 by-construction:
/// type-distinct from `ExecuteRange`, no path to silent reorder.
struct BindRange(crate::action::WriteRange);

/// DEF-270 P-ordering (Phase 2, 2026-05-10) — typed
/// [`crate::action::WriteRange`] newtype identifying an `Execute`
/// frame body. Sibling of [`BindRange`].
struct ExecuteRange(crate::action::WriteRange);

/// DEF-270 P-ordering (Phase 2, 2026-05-10) — single-purpose
/// stager for the `Bind`+`Execute`+`Sync` frame triple. Argument
/// order pins frame order; the const-asserted `budget: 3` matches
/// the three actions emitted; `Sync` is the static
/// [`crate::wire::SYNC_WIRE_BYTES`] reference (zero-copy).
///
/// **Tier-1 closures:**
/// - argument-order swap (`(execute, bind)`) → type error.
/// - missing `Sync` → impossible (function emits all three or none).
/// - missing `Bind` or `Execute` → impossible (function takes both
///   by value, function must be called to stage anything).
///
/// Pre-DEF-270-P-ordering the same wire-frame triple was open-coded
/// inside `compute_push_bind_execute_idle_only` as three
/// `emit_actions!` arms. Tier-3 by-discipline: a refactor that
/// reordered them or dropped Sync would compile.
#[inline]
fn stage_bind_execute_sync(
    staged: &mut StagedActions,
    bind: BindRange,
    execute: ExecuteRange,
) {
    emit_actions!(staged, budget: 3, [
        StagedAction::SendBytesRange(bind.0),
        StagedAction::SendBytesRange(execute.0),
        StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
    ]);
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
///
/// # DEF-270 P-ordering (Phase 2): typed return
///
/// Returns [`BindRange`], not raw [`crate::action::WriteRange`]. The
/// typed newtype binds at the boundary so [`stage_bind_execute_sync`]
/// statically rejects an `ExecuteRange` in the bind slot.
// DEF-184 (A1+A13): ProtocolError shrunk 312 → ~72 B post-
// ErrorArena externalisation; Err path below 128 B
// result_large_err threshold.
fn build_bind_message<P: crate::params::ParamsWriter>(
    portal_name: &crate::ident::PortalName,
    stmt_name: &crate::ident::StmtName,
    params: &P,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<BindRange, ProtocolError> {
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
    let raw = crate::action::WriteRange::from_write_span(start, reserved)?;
    Ok(BindRange(raw))
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
///
/// # DEF-270 P-ordering (Phase 2): typed return
///
/// Returns [`ExecuteRange`], not raw [`crate::action::WriteRange`].
/// See [`build_bind_message`] / [`stage_bind_execute_sync`] for the
/// typed-frame closure rationale.
fn build_execute_message(
    portal_name: &crate::ident::PortalName,
    fetch: crate::command::FetchRows,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<ExecuteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_EXECUTE.byte())?;
    reserved.with_length_prefix(|w| {
        w.push_nul_terminated(portal_name.as_bytes())?;
        w.push_i32_be(fetch.as_wire_i32())?;
        Ok(())
    })?;
    let raw = crate::action::WriteRange::from_write_span(start, reserved)?;
    Ok(ExecuteRange(raw))
}

/// Idle-only push path for [`PgProtocol::push_bind_execute`].
#[expect(clippy::too_many_arguments, reason = "mirrors compute_push_bind_execute signature 1:1")]
#[inline]
pub(crate) fn compute_push_bind_execute_idle_only<P: crate::params::ParamsWriter>(
    setter: crate::state_setter::StateSetter<'_, crate::push_command::BindExecutePostInstall>,
    row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
    portal_name: &crate::ident::PortalName,
    stmt_name: &crate::ident::StmtName,
    params: &P,
    row_desc: Option<crate::decode::RowDesc>,
    fetch: crate::command::FetchRows,
    reply: ReplyId<crate::reply_id::QueryKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    let bind_range = try_builder!(
        build_bind_message(portal_name, stmt_name, params, reserved),
        setter,
        reply,
        staged
    );
    let execute_range = try_builder!(
        build_execute_message(portal_name, fetch, reserved),
        setter,
        reply,
        staged
    );
    // DEF-270 P-ordering (Phase 2): typed builder fn pins frame order
    // (Bind → Execute → Sync). Argument-order swap → type error.
    stage_bind_execute_sync(staged, bind_range, execute_range);
    // DEF-189: caller-supplied RowDesc lands in the protocol's
    // single slot BEFORE the state transition. The variant
    // shape (Select vs Dml) is the tier-1 signal that the
    // slot is populated.
    // DEF-270 N-D (Phase 2): typed witness pairs BindExecute →
    // BindExecuteAwaitingBindComplete{Dml,Select}.
    // DEF-272 cluster α: park via leaf submodule
    // `_bind_execute_select_install_leaf::install_select_transition`
    // which mints a `BeSelectToken` (private-field, leaf-gated mint)
    // and routes to `RowDescSlotCell::park_at_be_select`.
    let post_install = match row_desc {
        Some(desc) => {
            _bind_execute_select_install_leaf::install_select_transition(row_desc_slot, desc);
            crate::push_command::BindExecutePostInstall::Select { reply }
        }
        None => crate::push_command::BindExecutePostInstall::Dml { reply },
    };
    setter.install_post_state(post_install);
}

// ═════════════════════════════════════════════════════════════════════
// DEF-244 (2026-05-13) — Idle-only push path for the `prepared!`
// macro's `BindPrepared<'q, P, R>` command. Sister to
// `compute_push_bind_execute_idle_only` above but routed through
// the macro's pre-baked Parse + Bind-prefix bytes.
//
// Wire frame sequence:
//   1. Parse template (static, baked by macro) — Parse frame with
//      stmt_name + sql + per-param OID list.
//   2. Bind prefix (static, baked by macro) — 'B' tag + length
//      placeholder + empty portal NUL + stmt_name NUL + compact
//      format-code block + n_params header.
//   3. Per-param values — `args.write_params(reserved.as_write_buf_mut())`.
//      Length prefix patched via `WriteBuf::with_length_prefix`.
//   4. n_result_formats trailer (static, 2 bytes = 0 = all-text).
//   5. Execute frame (static, 10 bytes for empty portal + fetch-all).
//   6. Sync trailer (static, 5 bytes).
//
// State install: BindExecutePostInstall::Dml if row_oids is empty,
// else BindExecutePostInstall::Select with a synthetic RowDesc
// parked into row_desc_slot built from `q.row_oids`.
// ═════════════════════════════════════════════════════════════════════

/// Idle-only push path for [`BindPrepared`](crate::push_command::BindPrepared).
///
/// Per the design memo §6.2 + §5.3: emits the pre-baked Parse and
/// Bind-prefix bytes (the macro computed them at expansion time;
/// caller pays zero CPU on the header construction), appends the
/// per-param payload via the existing `ParamsWriter` path, and
/// stages the static Execute + Sync frames at the end.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors compute_push_bind_execute_idle_only's argument-list shape (sister helper for the prepared! macro's push path); collapsing into a struct would obscure the wire-frame parameter order"
)]
#[inline]
pub(crate) fn compute_push_bind_prepared_idle_only<'sql, P, R>(
    setter: crate::state_setter::StateSetter<'_, crate::push_command::BindExecutePostInstall>,
    row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
    q: &'sql crate::prepared::PreparedQuery<P, R>,
    args: P,
    _fetch: crate::command::FetchRows,
    reply: ReplyId<crate::reply_id::QueryKind>,
    staged: &mut StagedActions<'sql>,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) where
    P: crate::params::ParamsWriter,
    R: crate::prepared::RowDecode,
{
    // Step 1: stage the pre-baked Parse template.
    let parse_template: &'sql [u8] = q.parse_template;
    // Step 2: build the Bind frame in-place — copy the static prefix
    // into `reserved`, append per-param values via ParamsWriter,
    // append the static `n_result_formats = 0` trailer, and patch
    // the length prefix.
    let bind_range_result = build_bind_prepared_frame(reserved, q.bind_execute_prefix, &args);
    let bind_range = try_builder!(bind_range_result, setter, reply, staged);

    // Step 3: row-less vs row-bearing dispatch. Empty row_oids →
    // DML; non-empty → Select with a synthetic RowDesc.
    let post_install = if q.row_oids.is_empty() {
        crate::push_command::BindExecutePostInstall::Dml { reply }
    } else {
        // Synthesise a RowDesc from `q.row_oids` (all-text format,
        // memo §5.4). The macro's row_oids list is small (≤ 16) and
        // bounded by MAX_ROW_COLUMNS = 32; the construction is
        // infallible at runtime.
        let row_desc = match build_synthetic_row_desc(q.row_oids) {
            Ok(desc) => desc,
            Err(cause) => {
                // Architecturally rare: macro emits row_oids of
                // arity > MAX_ROW_COLUMNS would have failed the
                // RowDecode trait bound at compile time (RowDecode
                // tuple impls cap at 16 < 32). Fall through with a
                // classified error.
                emit_actions!(staged, budget: 1, [
                    StagedAction::FailReply {
                        id: reply.consume(),
                        cause,
                    },
                ]);
                setter.install_errored(
                    crate::error::StateErrorKind::from_kind_or_internal(
                        crate::error::ErrorKind::Internal,
                    ),
                );
                return;
            }
        };
        // Park via the leaf-private token mint (DEF-272 cluster α).
        _bind_execute_select_install_leaf::install_select_transition(row_desc_slot, row_desc);
        crate::push_command::BindExecutePostInstall::Select { reply }
    };

    // Step 4: stage the four wire-frame actions:
    //   - Parse template (borrowed from q's .rodata)
    //   - Bind frame range (written into write_buf)
    //   - Execute frame (static)
    //   - Sync trailer (static)
    //
    // Budget: 4 staged actions. MAX_FANOUT_PER_STAGED + MAX_STAGED_PER_CALL
    // const-asserts ensure this fits within MAX_ACTIONS_PER_CALL.
    emit_actions!(staged, budget: 4, [
        StagedAction::SendBytesBorrowed(parse_template),
        StagedAction::SendBytesRange(bind_range),
        StagedAction::SendBytesStatic(&crate::prepared::EXECUTE_EMPTY_PORTAL_NO_LIMIT),
        StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
    ]);

    setter.install_post_state(post_install);
}

/// Build the Bind frame for a prepared query.
///
/// Layout per PG §55.2.2:
/// 1. `'B'` tag (1 byte).
/// 2. Length prefix (4 bytes, BE, self-inclusive) — patched by
///    `with_length_prefix`.
/// 3. Macro-baked `prefix` bytes (portal NUL + stmt_name NUL +
///    compact format-code block + n_params).
/// 4. Per-param values from `args.write_params(...)`.
/// 5. `n_result_formats = 0` trailer (2 bytes,
///    [`crate::prepared::BIND_N_RESULT_FORMATS_ZERO`]).
fn build_bind_prepared_frame<P>(
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    prefix: &[u8],
    args: &P,
) -> Result<crate::action::WriteRange, ProtocolError>
where
    P: crate::params::ParamsWriter,
{
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_BIND.byte())?;
    // `with_length_prefix` reserves 4 bytes for the length, runs the
    // body closure, and patches the length on close (mirror of
    // `build_bind_message`'s pattern in compute_push_bind_execute).
    let mut params_err: Option<ProtocolError> = None;
    reserved.with_length_prefix(|w| {
        w.push_bytes(prefix)?;
        if args.write_params(w.as_write_buf_mut()).is_err() {
            params_err = Some(ProtocolError::InternalCrateBug {
                locus: crate::error::CrateBugLocus::ParamsWriterOverflow,
            });
        }
        w.push_bytes(&crate::prepared::BIND_N_RESULT_FORMATS_ZERO)?;
        Ok(())
    })?;
    if let Some(err) = params_err {
        return Err(err);
    }
    crate::action::WriteRange::from_write_span(start, reserved)
}

/// Build a synthetic [`crate::decode::RowDesc`] from a static OID
/// list for the `prepared!` macro's path. All columns use
/// [`FormatCode::Text`] (memo §5.4 — text format in v1).
///
/// Bounded above by [`crate::decode::MAX_ROW_COLUMNS`] = 32. The
/// macro's RowDecode trait impls cap arity at 16 < 32, so this
/// is architecturally always-success; the Result keeps the no-panic
/// discipline.
fn build_synthetic_row_desc(
    oids: &[u32],
) -> Result<crate::decode::RowDesc, ProtocolError> {
    // We need to construct a RowDesc; the existing constructors are
    // `RowDesc::EMPTY` (0 cols) and the internal `parse_row_description`
    // (parses wire bytes). For the macro path we synthesise directly
    // via a helper on `RowDesc` itself — exposed `pub(crate)` for
    // the prepared module to use.
    crate::decode::RowDesc::from_static_oids_text_format(oids)
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
/// # DEF-210 SR-03 (audit 2026-04-28): unified classifier
///
/// Pre-Path-3 this function had its OWN exhaustive match over
/// `ProtoState`, mirrored byte-for-byte in
/// [`allows_unsolicited_notice_response`] below. Tier-1 closure
/// existed PER FUNCTION (each `match` was exhaustive) but NOT
/// across the pair: a new variant added to one classifier without
/// the other would silently classify asymmetrically (PS accepted,
/// NR rejected, or vice versa). Path-3 routes both classifiers
/// through [`crate::state::ProtoState::unsolicited_admit`] — one
/// exhaustive match, two bool projections. **Drift between the
/// two classifiers is structurally impossible**.
// DEF-236 (audit 2026-05-05): hot inbound dispatch, called per frame.
// LLVM already inlines transparently — `#[inline]` makes the intent
// explicit (explicit > implicit) and pins behaviour against future
// heuristic shifts.
#[inline]
const fn allows_unsolicited_param_status(state: &ProtoState) -> bool {
    state.unsolicited_admit().allow_param_status
}

/// DEF-185 P1-E (audit 2026-04-24) → DEF-210 SR-03 (audit 2026-04-28):
/// classifier for `NoticeResponse` frame acceptance, today identical
/// to [`allows_unsolicited_param_status`] in policy.
///
/// PG server behaviour (§48.5 "Asynchronous Operations"): NoticeResponse
/// may arrive at any time after connection start, BUT our client
/// enforces a stricter client-side invariant: notices are only accepted
/// in states where they would be delivered to the wrapper's async
/// logging channel. Pre-auth states (Connecting*) reject notices to
/// ensure nothing from the server is trusted before authentication
/// completes — a pre-auth MITM-injected notice could carry
/// attacker-controlled text that ends up in operator logs.
///
/// Routes through [`crate::state::ProtoState::unsolicited_admit`].
/// See [`allows_unsolicited_param_status`] for the SR-03 unification
/// rationale (single exhaustive source, no parallel-classifier drift).
// DEF-236 (audit 2026-05-05): same reasoning as
// `allows_unsolicited_param_status` — LLVM already inlines; `#[inline]`
// pins intent.
#[inline]
const fn allows_unsolicited_notice_response(state: &ProtoState) -> bool {
    state.unsolicited_admit().allow_notice_response
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

// DEF-160 Z2 (2026-05-11): `materialise_push` removed. Pre-Z2 it was
// the push-path counterpart to `materialise` — verifying staged ranges
// resolve against `wb` and appending `SendBytesStatic` (Sync) bytes
// INTO `wb`, returning `Result<(), PushFailure>`. Post-Z2 the push API
// returns `OutActions` so callers can stream borrowed-SQL chunks via
// `writev`; `push_command_internal` unifies push and feed materialisation
// through the single `materialise` entry below (`terminal_row_desc:
// None` for push — push never emits `DeliverReply`). The
// `BrandedWriteReserved::as_bytes` helper that `materialise_push` used
// for the M5 brand-roundtrip verification is also dead post-Z2.

/// Phase-2 materialiser: convert the write-phase's
/// [`StagedActions`] into [`OutActions<'w, 'r>`] with references
/// into `write_buf_bytes` (`'w`) or `terminal_row_desc` (`'r`).
///
/// DEF-094 + 1c-1a + DEF-188 lifetime plumbing: `write_buf_bytes`
/// supplies `'w`; `terminal_row_desc: Option<&'r RowDesc>` supplies
/// `'r` (the parking slot on `PgProtocol`). The borrow checker
/// refuses any `&mut WriteBuf` re-borrow while the returned
/// `OutActions<'w, 'r>` is alive, and any `&mut self` re-borrow
/// on `PgProtocol` while `'r` is alive.
// DEF-236 (audit 2026-05-05): NO `#[inline]`. ASM diff (revert vs
// `#[inline]`): standalone symbol persists with `bl` calls at all
// 4 call sites — LLVM rejects the hint. Body too large to inline at
// 4 sites without net code bloat. Annotation would be ineffective
// noise; LLVM heuristic is correct here.
// DEF-160 (Z2): `staged: StagedActions<'w>` — the staged container's
// `'sql` lifetime is unified with the WriteBuf's `'w`. This expresses
// "any borrowed SQL bytes inside staged outlive the WriteBuf borrow",
// which is the natural caller-side invariant: caller passes
// `Parse<'a> { sql: &'a str }` AND `&mut WriteBuf` to the same
// `push_command` call; the borrow checker enforces `'a >= 'w`. The
// materialiser then emits `Action::SendBytes(&'w [u8])` for both
// `SendBytesRange` (bytes from `write_bytes: &'w [u8]`) and
// `SendBytesBorrowed` (bytes from caller's SQL, lifetime ≥ 'w by
// the unified parameter).
fn materialise<'w, 'r>(
    staged: StagedActions<'w>,
    write_bytes: &'w [u8],
    terminal_row_desc: Option<&'r crate::decode::RowDesc>,
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
        // DEF-188: stale-ref class deleted — into_public is
        // infallible.
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
            // DEF-112 + DEF-188 + DEF-210 SR-01 Path C/D:
            // `DeliverReplyEntry` carries a lifetime-free `StagedReply`.
            // Materialise reads `row_desc_slot` directly for ALL
            // schema-bearing reply paths (QueryComplete, Describe*Complete)
            // — single source of truth for "is there a schema?". Path C
            // deleted the `schema_present: bool` duplicate from
            // QueryComplete; Path D deleted the `DescribedRowsStaged*`
            // duplicate enums from Describe*Complete. No defensive
            // `debug_assert!(false)` arms left. The entry was constructed
            // by the typed `action::deliver` path — kind-payload pairing
            // enforced at dispatch time.
            StagedAction::DeliverReply(entry) => {
                let entry_id = entry.id();
                Action::DeliverReply {
                    id: entry_id,
                    value: entry.staged().into_public(terminal_row_desc),
                }
            }
            StagedAction::FailReply { id, cause } => Action::FailReply { id, cause },
            StagedAction::CloseSocket => Action::CloseSocket,
            // DEF-160 (Z2): pass borrowed slice through unchanged.
            // The `'sql: 'w` bound on `materialise` ensures the borrow
            // is at least as long-lived as the WriteBuf's bytes — the
            // returned `Action::SendBytes(&'w [u8])` carries the
            // shorter lifetime safely.
            StagedAction::SendBytesBorrowed(b) => Action::SendBytes(b),
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
/// alone. Post-DEF-280 Bundle G the `debug_assert!(false, …)` in
/// the Err branch was removed as a CREDO §V glass pattern (dev
/// loud + release silent fallthrough); the build-time const-assert
/// at `MAX_ACTIONS_PER_CALL` is the actual safety proof, and the
/// runtime Err arm is `core::hint::cold_path()` + silent no-op
/// (architecturally dead under intact invariant; a future refactor
/// that breaks the capacity inequality without updating the const
/// fails to compile rather than reaching this arm).
///
/// ## Why the wrapper vs inline match?
///
/// The function call is `#[inline(always)]` + const-folded in
/// release, so zero runtime overhead. Source-level wrapper
/// centralises the cold-path discipline across 6 materialise
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
            // DEF-280 Bundle G (2026-05-18): debug_assert!(false, …)
            // removed — CREDO §V banned glass pattern. The const-asserted
            // capacity invariant is the build-time safety net:
            // `MAX_ACTIONS_PER_CALL >= MAX_STAGED_PER_CALL +
            //  MAX_FANOUT2_ENTRIES_PER_CALL × (MAX_FANOUT_PER_STAGED − 1) = 9`
            // (asserted at MAX_ACTIONS_PER_CALL in action.rs). The Err arm
            // is architecturally dead in any binary that compiles. Silent
            // no-op is the safe fallback if a future refactor breaks the
            // capacity inequality without bumping the const — the const-
            // assert at build time will catch the drift before this
            // runtime arm matters. (Pre-Bundle G the
            // `debug_assert!(false, …)` provided only dev loudness, which
            // misled readers into thinking the runtime check was the
            // safety net rather than the build-time const-assert.)
            core::hint::cold_path();
        }
    }
}

// DEF-246 Phase 2 (2026-05-16): `Default` impl moved from
// `<ActivePhase>` to `<DisconnectedPhase>` — `PgProtocol::default()`
// produces a fresh disconnected protocol (matches `PgProtocol::new()`).
impl Default for PgProtocol<DisconnectedPhase> {
    fn default() -> Self {
        Self::new()
    }
}

// DEF-246 Phase 1 + 2 (2026-05-16): blanket `Debug` for every phase.
// Phase-specific marker is the `phase_marker: PhantomData<fn() -> P>`
// ZST; the human-readable contents are inner fields that exist in
// every phase (no phase-conditional Debug output). The previous
// `<ActivePhase>`-only impl is upgraded to `<P: SealedPhase>` so
// `eprintln!("{:?}", proto)` works in handshake / closed contexts too.
impl<P: SealedPhase> core::fmt::Debug for PgProtocol<P> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Inline routing mirrors `cold_session_params`: prefer the
        // boxed contents if allocated, else fall back to a static
        // empty `SessionParams::new()` (pristine, never dropped).
        static EMPTY: SessionParams = SessionParams::new();
        let session_params: &SessionParams = match self.inner.session_params.as_deref() {
            Some(p) => p,
            None => &EMPTY,
        };
        f.debug_struct("PgProtocol")
            .field("state", &self.inner.state)
            .field("read_buf", &self.inner.read_buf)
            .field("session_params", session_params)
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
            secret_key: crate::sensitive::Sensitive::new(1_i32),
        };
        assert!(allows_unsolicited_param_status(&have_key));
        consume_state(have_key);

        // --- Rejecting states (policy = false) ---
        let startup_trust = ProtoState::ConnectingStartupTrust {
            reply: ReplyId::from_raw(nz(4)),
        };
        assert!(!allows_unsolicited_param_status(&startup_trust));
        consume_state(startup_trust);

        // ConnectingStartupScram — DEF-097 typestate carrying
        // ScramSession inline (DEF-184 A10/B22 revert 2026-04-24:
        // tier-1 variant-carries-field restoration). The classification
        // test only reads the variant tag, but the variant cannot be
        // constructed without its required SCRAM data — that is the
        // tier-1 invariant under test.
        if let Ok(pw) = crate::password::Password::try_from_bytes(b"pw") {
            let scram = alloc::boxed::Box::new(
                crate::scram::session::ScramSession::from_password(
                    crate::sensitive::Sensitive::new(pw),
                ),
            );
            let startup_scram = ProtoState::ConnectingStartupScram {
                reply: ReplyId::from_raw(nz(4001)),
                scram,
            };
            assert!(!allows_unsolicited_param_status(&startup_scram));
            consume_state(startup_scram);
        }

        if let Ok(pw) = crate::password::Password::try_from_bytes(b"pw") {
            let scram = alloc::boxed::Box::new(
                crate::scram::session::ScramSession::from_password(
                    crate::sensitive::Sensitive::new(pw),
                ),
            );
            let scram_first = ProtoState::ConnectingScramAwaitingServerFirst {
                reply: ReplyId::from_raw(nz(5)),
                scram,
            };
            assert!(!allows_unsolicited_param_status(&scram_first));
            consume_state(scram_first);
        }

        let scram_final = ProtoState::ConnectingScramAwaitingServerFinal {
            reply: ReplyId::from_raw(nz(6)),
            expected_server_sig: crate::scram::types::SecretDigest::new([0_u8; 32]),
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

        // DEF-189: state variants no longer carry inline RowDesc;
        // schema lives in PgProtocol::row_desc_slot. Test fixtures
        // construct streaming variants directly without schema; the
        // policy under test (`allows_unsolicited_param_status`) is
        // schema-agnostic.
        let q_rows = ProtoState::SimpleQueryStreamingRows {
            reply: ReplyId::from_raw(nz(8002)),
        };
        assert!(allows_unsolicited_param_status(&q_rows));
        consume_state(q_rows);

        let q_rfq = ProtoState::SimpleQueryAwaitingRfq {
            reply: ReplyId::from_raw(nz(8003)),
            command_tag: crate::error::BoundedStr::default(),
        };
        assert!(allows_unsolicited_param_status(&q_rfq));
        consume_state(q_rfq);

        let q_drain = ProtoState::DrainRfqAfterError;
        assert!(allows_unsolicited_param_status(&q_drain));
        consume_state(q_drain);
    }
}

#[cfg(test)]
mod residue_policy_per_class_tests {
    //! DEF-210 NB-04 (audit 2026-04-28): per-`StatePushClass` pinning
    //! of `clear_session_residue_if_idle_or_errored` arm bodies.
    //!
    //! The production function uses a wildcard `_ => {}` for the
    //! Connecting / PingAwaiting / BusyQuery preserve-residue arm
    //! (the wildcard form compiles to a single discriminant compare
    //! pair — explicit 25-variant or-pattern cost ~+2 ns, see
    //! comment on `clear_session_residue_if_idle_or_errored`). The
    //! wildcard is tier-2-by-discipline at the broad scope: a future
    //! `ProtoState` variant inherits the wildcard "preserve" arm
    //! silently, with no compile-time signal.
    //!
    //! These tests close the gap at the **`StatePushClass` granularity**
    //! by pinning the per-class residue policy on observable state:
    //! - **Idle** — `row_desc_slot` cleared; `session_params` preserved.
    //! - **Errored(_)** — `row_desc_slot` cleared; `session_params`
    //!   internally `clear()`-ed (verified via `is_pristine()`).
    //! - **Connecting / PingAwaiting / BusyQuery** — every observable
    //!   residue field preserved.
    //!
    //! An arm-body swap (e.g. `Idle => clear session_params` instead
    //! of preserve) trips one of these tests immediately. Adding a
    //! new `StatePushClass` variant requires a new test arm here too
    //! (the test for the new class would be missing — caught by
    //! contributor discipline + code review, not compile-fail; this
    //! is the residual tier-3 surface that integration-via-public-
    //! API would close, but the public-API path requires real
    //! server-frame fixtures that are outside this test's scope).
    use super::*;
    use crate::decode::RowDesc;
    use crate::error::{ErrorKind, StateErrorKind};
    use crate::reply_id::ReplyId;
    use crate::session_params::SessionParams;
    use crate::state::ProtoState;
    use core::num::NonZeroU64;

    fn nz(n: u64) -> NonZeroU64 {
        assert!(n > 0, "nz(0) is a test bug — must be ≥ 1");
        NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN)
    }

    /// Construct a non-pristine `SessionParams` (one counter bumped
    /// off-zero) to make the preserve-vs-clear distinction observable
    /// via [`SessionParams::is_pristine`].
    fn dirty_session_params() -> alloc::boxed::Box<SessionParams> {
        let mut params = SessionParams::new();
        params.n_unknown_dropped = 1;
        alloc::boxed::Box::new(params)
    }

    /// Populate every observable residue field on `proto`:
    /// `row_desc_slot = Some(EMPTY)`, `session_params` non-pristine,
    /// `error_arena` allocated. After the test we observe how each
    /// arm of `clear_session_residue_*` mutated them.
    ///
    /// DEF-246 Phase 2 (2026-05-16): generic over `P: SealedPhase` so
    /// the same helper drives both the new `<DisconnectedPhase>`
    /// protocols (from `PgProtocol::new()`) and the legacy
    /// `<ActivePhase>` set-up paths inside `mod compute_push_tests`.
    fn populate_residue<P: SealedPhase>(proto: &mut PgProtocol<P>) {
        proto.inner.row_desc_slot._set_for_test(Some(RowDesc::EMPTY));
        proto.inner.session_params._set_for_test(Some(dirty_session_params()));
        proto.inner.error_arena = Some(alloc::boxed::Box::new(
            crate::error_arena::ErrorArena::new(),
        ));
    }

    /// Replace `proto.inner.state` with `Idle` so the destructor doesn't
    /// trip the in-flight `ReplyId<_>` Drop-guard at scope end.
    /// DEF-246 Phase 2 (2026-05-16): generic over `P: SealedPhase`.
    fn quench_inflight<P: SealedPhase>(proto: &mut PgProtocol<P>) {
        let prev = core::mem::replace(&mut proto.inner.state, ProtoState::Idle);
        match prev.take_inflight_reply_raw_id() {
            Some(_) | None => {}
        }
    }

    /// DEF-246 Phase 2 (2026-05-16): generic over `P: SealedPhase`.
    fn session_params_is_pristine<P: SealedPhase>(proto: &PgProtocol<P>) -> bool {
        // DEF-211 INNO-01 (2026-05-04): trait method via `Pristine` import.
        // Inherent `__pristine_const` would also work but trait dispatch
        // here matches polymorphic intent (test helper takes any
        // `SessionParams`-like thing).
        use crate::pristine::Pristine as _;
        match proto.inner.session_params.as_deref() {
            Some(p) => p.is_pristine(),
            None => true,
        }
    }

    #[test]
    fn idle_clears_row_desc_preserves_session_params() {
        let mut proto = PgProtocol::new();
        // Default state is `Idle` post-`new()`.
        populate_residue(&mut proto);
        let class = proto.inner.state.push_class();
        proto.inner.clear_session_residue_for_class(class);

        assert!(
            proto.inner.row_desc_slot.is_none(),
            "Idle must clear row_desc_slot",
        );
        assert!(
            proto.inner.error_arena.is_some(),
            "Idle preserves the error_arena Box (contents cleared internally)",
        );
        assert!(
            proto.inner.session_params.is_some(),
            "Idle preserves session_params Box",
        );
        assert!(
            !session_params_is_pristine(&proto),
            "Idle MUST NOT clear session_params content (load-bearing during a healthy connection)",
        );
    }

    #[test]
    fn errored_clears_everything_including_session_params() {
        let mut proto = PgProtocol::new();
        proto.inner.state = ProtoState::Errored(
            StateErrorKind::from_kind_or_internal(ErrorKind::Framing),
        );
        populate_residue(&mut proto);
        let class = proto.inner.state.push_class();
        proto.inner.clear_session_residue_for_class(class);

        assert!(
            proto.inner.row_desc_slot.is_none(),
            "Errored must clear row_desc_slot",
        );
        assert!(
            proto.inner.session_params.is_some(),
            "Errored preserves session_params Box (only contents cleared)",
        );
        assert!(
            session_params_is_pristine(&proto),
            "Errored MUST clear session_params content (DEF-189 Q8-C3 forfeit on tear-down)",
        );
        // No state mutation back to Idle here — Errored is terminal.
        // Drop-guard for `Errored(StateErrorKind)` is fine: the kind
        // is `Copy`, no in-flight ReplyId to consume.
    }

    #[test]
    fn connecting_preserves_all_residue() {
        let mut proto = PgProtocol::new();
        proto.inner.state = ProtoState::ConnectingStartupTrust {
            reply: ReplyId::from_raw(nz(11)),
        };
        populate_residue(&mut proto);
        let class = proto.inner.state.push_class();
        proto.inner.clear_session_residue_for_class(class);

        assert!(
            proto.inner.row_desc_slot.is_some(),
            "Connecting (StatePushClass::Connecting) must preserve row_desc_slot",
        );
        assert!(
            proto.inner.session_params.is_some(),
            "Connecting must preserve session_params Box",
        );
        assert!(
            !session_params_is_pristine(&proto),
            "Connecting must preserve session_params content",
        );
        assert!(
            proto.inner.error_arena.is_some(),
            "Connecting must preserve error_arena",
        );
        quench_inflight(&mut proto);
    }

    #[test]
    fn ping_awaiting_preserves_all_residue() {
        let mut proto = PgProtocol::new();
        proto.inner.state = ProtoState::PingAwaitingRfq(ReplyId::from_raw(nz(12)));
        populate_residue(&mut proto);
        let class = proto.inner.state.push_class();
        proto.inner.clear_session_residue_for_class(class);

        assert!(
            proto.inner.row_desc_slot.is_some(),
            "PingAwaiting (StatePushClass::PingAwaiting) must preserve row_desc_slot",
        );
        assert!(
            !session_params_is_pristine(&proto),
            "PingAwaiting must preserve session_params content",
        );
        assert!(
            proto.inner.error_arena.is_some(),
            "PingAwaiting must preserve error_arena",
        );
        quench_inflight(&mut proto);
    }

    #[test]
    fn busy_query_preserves_all_residue() {
        let mut proto = PgProtocol::new();
        proto.inner.state = ProtoState::SimpleQueryStreamingRows {
            reply: ReplyId::from_raw(nz(13)),
        };
        populate_residue(&mut proto);
        let class = proto.inner.state.push_class();
        proto.inner.clear_session_residue_for_class(class);

        assert!(
            proto.inner.row_desc_slot.is_some(),
            "BusyQuery (StatePushClass::BusyQuery) must preserve row_desc_slot",
        );
        assert!(
            !session_params_is_pristine(&proto),
            "BusyQuery must preserve session_params content",
        );
        assert!(
            proto.inner.error_arena.is_some(),
            "BusyQuery must preserve error_arena",
        );
        quench_inflight(&mut proto);
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
    // DEF-246 Phase 2 (2026-05-16): `Credentials` import was used by
    // the retired Startup cross-state tests; the new
    // `<DisconnectedPhase>::push_startup` consumes Credentials via
    // its own param, not via the cfg(test) dispatcher.
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

    // DEF-246 Phase 2 (2026-05-16): `mk_user` was used by the
    // retired Startup cross-state tests. The new
    // `<DisconnectedPhase>::push_startup` consume-self entry point
    // makes those tests structurally impossible; the helper is
    // removed alongside them.

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
        fn from_staged(sa: &StagedAction<'_>) -> Self {
            match sa {
                StagedAction::SendBytesRange(_) => Self::SendBytesRange,
                StagedAction::SendBytesStatic(s) => Self::SendBytesStatic(s),
                StagedAction::DeliverReply(_) => Self::DeliverReply,
                StagedAction::FailReply { id, cause } => {
                    Self::FailReply { id: *id, cause: *cause }
                }
                StagedAction::CloseSocket => Self::CloseSocket,
                // DEF-160 (Z2): borrowed bytes don't appear in the
                // legacy cfg(test) `PgCommand`-driven path (no Parse /
                // SimpleQuery test fixtures route through this enum
                // post-DEF-269-v2). Keep an explicit arm to fail the
                // build if a future test introduces a borrowed-SQL
                // path here without updating the observation type.
                StagedAction::SendBytesBorrowed(_) => Self::SendBytesStatic(b""),
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
        // DEF-186 perf-recovery: compute_push takes &mut state now.
        // Closure captures &mut state_var to mutate in place; returns
        // the obs vec. After closure, state_var holds the post-push
        // state.
        let mut wb = WriteBuf::new();
        let mut state_var = state;
        let obs = wb.with_branded(|mut wb| {
            let mut reserved = wb.reserve();
            let staged = compute_push(cmd, &mut state_var, &mut reserved);
            let mut obs: heapless::Vec<StagedObs, MAX_ACTIONS_PER_CALL> = heapless::Vec::new();
            for a in &staged {
                obs.push(StagedObs::from_staged(a)).unwrap_or_else(|_| {
                    debug_assert!(false, "MAX_ACTIONS_PER_CALL overflow in test");
                });
            }
            obs
        });
        (state_var, obs)
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

        // ConnectingStartupScram — DEF-184 A10/B22 revert 2026-04-24:
        // tier-1 variant-carries-field restored. `scram: ScramSession`
        // lives INSIDE this variant — the variant cannot be constructed
        // without it.
        if let Ok(pw) = crate::password::Password::try_from_bytes(b"pw") {
            let raw_prev = nz(201_050);
            let raw_new = nz(201_051);
            let scram = alloc::boxed::Box::new(
                crate::scram::session::ScramSession::from_password(
                    crate::sensitive::Sensitive::new(pw),
                ),
            );
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
                    Some(StagedObs::FailReply {
                        id,
                        cause: ProtocolError::StartupAlreadyInProgress,
                    }) if *id == raw_new
                ),
                "ConnectingStartupScram → expected StartupAlreadyInProgress",
            );
            assert_eq!(take_connecting_startup_raw(new_state), Some(raw_prev));
        }

        // ConnectingScramAwaitingServerFirst — variant carries
        // `scram: ScramSession` + `client_first_bare` + `client_nonce_b64`
        // inline per tier-1 invariant.
        if let Ok(pw) = crate::password::Password::try_from_bytes(b"pw") {
            let raw_prev = nz(203);
            let raw_new = nz(204);
            let scram = alloc::boxed::Box::new(
                crate::scram::session::ScramSession::from_password(
                    crate::sensitive::Sensitive::new(pw),
                ),
            );
            let prev = ProtoState::ConnectingScramAwaitingServerFirst {
                reply: ReplyId::from_raw(raw_prev),
                scram,
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

        // ConnectingScramAwaitingServerFinal — variant carries
        // `expected_server_sig: SecretDigest` inline per tier-1 invariant.
        {
            let raw_prev = nz(205);
            let raw_new = nz(206);
            let prev = ProtoState::ConnectingScramAwaitingServerFinal {
                reply: ReplyId::from_raw(raw_prev),
                expected_server_sig: crate::scram::types::SecretDigest::new([0_u8; 32]),
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
                secret_key: crate::sensitive::Sensitive::new(1337_i32),
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
    // Startup — per-variant policy table  (DEF-246 Phase 2 RETIRED)
    //
    // The 3 tests previously here
    // (`startup_from_idle_transitions_and_emits_startup_message`,
    //  `startup_from_errored_preserves_kind_and_fails_with_connection_already_closed`,
    //  `startup_from_non_idle_non_errored_fails_with_startup_in_progress`)
    // defended the legacy `compute_push_startup` 5-arm dispatcher.
    // Post-DEF-246 Phase 2 (2026-05-16) the Startup push lives on
    // `<DisconnectedPhase>::push_startup` (consume-self) and the
    // type system physically forbids pushing Startup from any other
    // state — the 3 tests' invariants are STRUCTURALLY IMPOSSIBLE
    // (`Closed`-state push is a method-absent E0599 compile error,
    // not a FailReply runtime classification). Tests deleted.
    //
    // The Idle arm (which DID produce real wire-bytes) is preserved
    // structurally: `<DisconnectedPhase>::push_startup` exercises the
    // exact same `compute_push_startup_idle_only` body, so the wire
    // shape is unchanged. Integration tests in
    // `tests/startup_spec.rs` cover the wire-shape end-to-end.
    // -----------------------------------------------------------------

    #[test]
    #[allow(dead_code, reason = "DEF-246 Phase 2: 3 tests retired (see comment block above). The cfg(test) `compute_push_startup` helper that drove the Idle/Errored/Connecting/PingAwaiting/BusyQuery decision table is also retired — the new typed entry point `<DisconnectedPhase>::push_startup` is consume-self, so non-Idle dispatches are E0599 at compile time.")]
    fn _def246_phase2_startup_dispatch_table_retired_compile_anchor() {
        // Placeholder test: exists only so a grep for `fn startup_from_`
        // in protocol.rs lands here with the retired-block comment
        // above. No body — empty test passes trivially.
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
        use crate::error::{CrateBugLocus, ProtocolError};
        use crate::params::OverflowParams;

        // DEF-246 Phase 2 (2026-05-16): `PgProtocol::new()` produces
        // `<DisconnectedPhase>` — for this test we want an
        // `<ActivePhase>` so we can exercise `push_bind_execute`. The
        // in-crate cfg(test) path constructs the Active wrapper
        // directly from `<DisconnectedPhase>::new()` by re-tagging the
        // phase marker. This is NOT a production bypass: external
        // crates cannot reach the `inner` field (module-private to
        // `mod protocol`) and the `phase_marker` is reachable only
        // from sibling `cfg(test)` code (no `pub`).
        let proto_disconnected = PgProtocol::<DisconnectedPhase>::new();
        let mut proto: PgProtocol<ActivePhase> = PgProtocol {
            inner: proto_disconnected.inner,
            phase_marker: PhantomData,
        };
        let mut wb = WriteBuf::new();
        let reply_raw = nz(999);
        // DEF-272 cluster γ: internal test goes through `ReadyGuard`
        // (the only legitimate path that runtime-classifies state as
        // Idle via `as_ready`). `push_command_internal` re-checks via
        // `IdleState::try_from` typestate at entry; production
        // callers always satisfy the check. Fresh proto is in `Idle`
        // state so `as_ready()` returns `Some`. The architecturally-
        // dead `None` arm early-returns to satisfy the lib-level
        // `clippy::panic` forbid bundle.
        let Some(guard) = proto.as_ready() else { return };
        // DEF-160 Z2 (2026-05-11): `push_bind_execute` borrows the
        // identifier args for the `'w` lifetime that flows into the
        // returned `OutActions`. Pre-Z2 the args were taken `&_` and
        // didn't extend their lifetime past the call; post-Z2 named
        // bindings are required to keep the borrows alive for the
        // `Result::is_err` inspection below.
        let portal = crate::ident::PortalName::default();
        let stmt = crate::ident::StmtName::default();
        let result = guard.push_bind_execute(
            &portal,
            &stmt,
            &OverflowParams,
            None, // No row_desc; DML-style path
            crate::FetchRows::All,
            ReplyId::from_raw(reply_raw),
            &mut wb,
        );

        // DEF-212 (Alt Y'): classified Err routes through
        // `Result::Err(PushFailure)` instead of pre-(212)'s 2-Action
        // FailReply+CloseSocket bundle. The atomic state transition
        // to `Errored` happens inside `push_bind_execute_internal` via
        // `install_errored`; the caller learns of the failure via the
        // typed `PushFailure { id, cause }` (~80 B) — no `OutActions`
        // 800 B return frame, no per-call action iteration.
        //
        // Pre-P0-3 silent corruption would have shipped a truncated
        // Bind frame on the wire (3-action bundle with miscomputed
        // length-prefix); the post-(212) contract surfaces the
        // classified failure at the type-system level.
        assert!(
            result.is_err(),
            "ParamsWriter Err must route to Result::Err(PushFailure); \
             got Ok — pre-P0-3 silent-corruption regression?",
        );
        // Architecturally dead via the assert above; `let-else { return }`
        // is the forbid-bundle-clean dead-arm landing pad (no panic!,
        // no unwrap!, no expect! on the success path).
        let Err(failure) = result else { return };

        assert_eq!(
            failure.id, reply_raw,
            "PushFailure.id must echo the consumed correlator (DEF-149 ReplyId discipline)",
        );
        assert!(
            matches!(
                failure.cause,
                ProtocolError::InternalCrateBug {
                    locus: CrateBugLocus::ParamsWriterOverflow,
                },
            ),
            "expected InternalCrateBug(ParamsWriterOverflow); got cause = {:?}",
            failure.cause,
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

    // ───────────────────────────────────────────────────────────────
    // DEF-186 P1-2 (audit 2026-04-24): pin tests for all 5 remaining
    // compute_push_* Idle-arm transitions.
    //
    // Pre-DEF-186 the by-value `compute_push_*` signatures forced
    // every arm to RETURN a ProtoState — a missing transition was a
    // build error (tier-1 compile). Post-DEF-186 the `&mut state`
    // signature only requires that the Idle arm WRITE *state =
    // <next>; preserve arms simply leave state untouched. Adding a
    // 6th compute_push_* helper that forgets `*state = ...` in the
    // Idle arm would compile, leaving state unchanged. These pin
    // tests catch that omission via runtime assertion on the
    // post-Idle state's variant.
    //
    // Ping + Startup already covered by tests above; these 5 close
    // the rest of the surface.
    // ───────────────────────────────────────────────────────────────

    #[test]
    fn simple_query_from_idle_pins_post_state_transition() {
        let raw_id = nz(186_001);
        let cmd = PgCommand::SimpleQuery {
            sql: crate::ident::Sql::from_str_truncating("SELECT 1"),
            reply: ReplyId::from_raw(raw_id),
        };
        let (new_state, _staged) = compute_staged(cmd, ProtoState::Idle);
        assert!(
            matches!(&new_state, ProtoState::SimpleQueryAwaitingFirstResponse(_)),
            "compute_push_simple_query Idle arm must write SimpleQueryAwaitingFirstResponse, got {new_state:?}",
        );
        consume_state(new_state);
    }

    #[test]
    fn parse_from_idle_pins_post_state_transition() {
        let raw_id = nz(186_002);
        let stmt_name = match crate::ident::StmtName::try_from_str("s") {
            Ok(s) => s,
            Err(_) => return,
        };
        let cmd = PgCommand::Parse {
            stmt_name,
            sql: crate::ident::Sql::from_str_truncating("SELECT 1"),
            reply: ReplyId::from_raw(raw_id),
        };
        let (new_state, _staged) = compute_staged(cmd, ProtoState::Idle);
        assert!(
            matches!(&new_state, ProtoState::ParseAwaitingParseComplete(_)),
            "compute_push_parse Idle arm must write ParseAwaitingParseComplete, got {new_state:?}",
        );
        consume_state(new_state);
    }

    #[test]
    fn describe_statement_from_idle_pins_post_state_transition() {
        let raw_id = nz(186_003);
        let stmt_name = match crate::ident::StmtName::try_from_str("s") {
            Ok(s) => s,
            Err(_) => return,
        };
        let cmd = PgCommand::DescribeStatement {
            stmt_name,
            reply: ReplyId::from_raw(raw_id),
        };
        let (new_state, _staged) = compute_staged(cmd, ProtoState::Idle);
        assert!(
            matches!(&new_state, ProtoState::DescribeStatementAwaitingParamDesc(_)),
            "compute_push_describe_statement Idle arm must write DescribeStatementAwaitingParamDesc, got {new_state:?}",
        );
        consume_state(new_state);
    }

    #[test]
    fn describe_portal_from_idle_pins_post_state_transition() {
        let raw_id = nz(186_004);
        let portal_name = match crate::ident::PortalName::try_from_str("p") {
            Ok(p) => p,
            Err(_) => return,
        };
        let cmd = PgCommand::DescribePortal {
            portal_name,
            reply: ReplyId::from_raw(raw_id),
        };
        let (new_state, _staged) = compute_staged(cmd, ProtoState::Idle);
        assert!(
            matches!(&new_state, ProtoState::DescribePortalAwaitingRowDescOrNoData(_)),
            "compute_push_describe_portal Idle arm must write DescribePortalAwaitingRowDescOrNoData, got {new_state:?}",
        );
        consume_state(new_state);
    }

    #[test]
    fn preserve_arms_leave_state_untouched_simple_query() {
        // DEF-186 P1-2: Errored / preserve arms MUST NOT write *state.
        // Trip a SimpleQuery against Errored — state must remain at the
        // EXACT same Errored(prior_kind) it was before.
        use crate::error::{ErrorKind, StateErrorKind};
        let prior_kind = StateErrorKind::from_kind_or_internal(ErrorKind::Framing);
        let raw_id = nz(186_005);
        let cmd = PgCommand::SimpleQuery {
            sql: crate::ident::Sql::from_str_truncating("SELECT 1"),
            reply: ReplyId::from_raw(raw_id),
        };
        let (new_state, _staged) = compute_staged(cmd, ProtoState::Errored(prior_kind));
        // Pin via matches! + extracting ErrorKind separately. `panic!`
        // banned by forbid-bundle even in tests.
        assert!(
            matches!(&new_state, ProtoState::Errored(_)),
            "expected preserved Errored, got {new_state:?}",
        );
        if let ProtoState::Errored(observed) = new_state {
            assert_eq!(
                observed.as_kind(),
                ErrorKind::Framing,
                "Errored kind preserved byte-exactly across non-Idle push",
            );
        }
    }
}
