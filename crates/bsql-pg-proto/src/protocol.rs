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
pub struct PgProtocol {
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
    // `match &self.state` twice (gate + project). Post-DEF-189:
    // state strips the schema field entirely; the schema lives in
    // this slot; fast path does `match &self.state` once + a single
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
#[allow(missing_docs, reason = "submodule contains a single-purpose leaf helper; module-level docs above the submodule explain the design")]
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
#[allow(missing_docs, reason = "submodule contains single-purpose leaf helpers; module-level docs above the submodule explain the design")]
pub(crate) mod _clear_residue_leaf {
    /// DEF-272 cluster α leaf-scope token for the schema slot clear.
    /// Field private to the leaf; type `pub(crate)` so the cell can
    /// name it in its method signature.
    pub(crate) struct ClearResidueSchemaToken(());

    /// DEF-272 cluster β leaf-scope token for the session_params slot
    /// clear. Field private to the leaf; type `pub(crate)` so the cell
    /// can name it.
    pub(crate) struct ClearResidueSessionToken(());

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
}

/// DEF-272 cluster β leaf submodule for the inbound `ParameterStatus`
/// pre-dispatch filter. Hosts the [`ParamStatusToken`] type and the
/// single admit helper fn that delegates to the cell's parse+record
/// method.
#[allow(missing_docs, reason = "submodule contains a single-purpose leaf helper; module-level docs above the submodule explain the design")]
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
#[allow(missing_docs, reason = "submodule contains a single-purpose leaf helper; module-level docs above the submodule explain the design")]
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
            row_desc_slot: crate::schema_slot::RowDescSlotCell::EMPTY,
            // DEF-196: three independent cold slots — none allocated
            // at construction. Trust auth + no errors + no malformed
            // frames + no notice/param frames = lifetime-zero heap.
            session_params: crate::session_params_slot::SessionParamsCell::EMPTY,
            error_arena: None,
            malformed_frame_count: 0,
            sync_marker: PhantomData,
        }
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
        // returned (caller gets a `ReplyId<K>` carrying `u64::MAX`
        // wrapped to NonZeroU64::MIN via the saturating_add fallback),
        // but the next push attempt sees Errored state and fails with
        // `ConnectionAlreadyClosed { prior_kind: ReplyIdSaturation }`
        // — the duplicate never reaches the server in a usable state.
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
    /// The transition is `Idle → Errored(ReplyIdSaturation)` (or no-op
    /// if state is already Errored — saturation classifier doesn't
    /// override the original cause). The drained inflight reply id (if
    /// any) is dropped silently — saturation has no FailReply emission
    /// context (no `&mut StagedActions` accessible from `next_reply_id`).
    /// The signal reaches the user via the next push attempt
    /// classifying as `ConnectionAlreadyClosed { prior_kind: ReplyIdSaturation }`.
    #[cold]
    #[inline(never)]
    fn install_errored_replyid_saturation(&mut self) {
        if matches!(self.state, ProtoState::Errored(_)) {
            return;
        }
        let cause = ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::ReplyIdSaturation,
        };
        // Route through cluster A's FeedStateSetter for tier-1
        // single-mutation-surface. The drained id (if any inflight)
        // is bound to an underscore-prefixed variable — saturation
        // has no FailReply emission context (no &mut StagedActions
        // accessible from next_reply_id). Option<NonZeroU64> is
        // `Copy`, so the binding is a structural no-op; the
        // `_drained_*` name documents the discard intent + dodges
        // both `unused_variables` (underscore prefix) and
        // `dropping_copy_types` (no `core::mem::drop` call).
        // Operator-visible signal arrives on the next push as
        // ConnectionAlreadyClosed { prior_kind: ReplyIdSaturation }.
        let _drained_id_at_saturation =
            crate::state_setter::FeedStateSetter::new(&mut self.state)
                .drain_and_install_errored(cause.state_kind());
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
        match self.session_params.as_deref() {
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
        match self.error_arena.as_deref() {
            Some(a) => a,
            None => &EMPTY,
        }
    }

    /// Read-only accessor for `malformed_frame_count`. Direct field
    /// read — no Box indirection (counter is inline since v2).
    #[inline]
    fn cold_malformed_frame_count(&self) -> u32 {
        self.malformed_frame_count
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
        &self.state
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
        self.read_buf.unread()
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
        match self.state.push_class() {
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
        match self.state.push_class() {
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
    /// `debug_assert!(matches!(self.state, ProtoState::Idle))` pins
    /// the caller's invariant in development builds; release builds
    /// skip the assertion for zero overhead.
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
    #[must_use = "the returned Result carries the bytes-in-wb success signal \
                  or the consumed-correlator + cause failure signal; both must \
                  be observed by the caller's I/O layer"]
    pub(crate) fn push_command_internal<C: crate::push_command::PushCommand>(
        &mut self,
        cmd: C,
        write_buf: &mut WriteBuf,
        // DEF-198 ext: tier-1 compile-time witness that state is Idle.
        // Constructible only inside `mod guard`; reachable only via
        // `ReadyGuard::push_command` which acquired the guard through
        // `PgProtocol::as_ready`'s runtime classification.
        _proof: crate::guard::IdleStateProof,
    ) -> Result<(), crate::action::PushFailure> {
        // DEF-212: bytes-only push contract — bytes live in caller's wb
        // post-Ok (drained via `wb.as_bytes()`); no per-call action
        // allocation (~800 B → ~80 B return frame).
        write_buf.clear();

        // DEF-188: centralised entry-point terminal-row-desc reclamation.
        // DEF-211 FAKE-01: the `IdleStateProof` witness above guarantees
        // `state == Idle`, so pass `StatePushClass::Idle` as a STATIC
        // const argument — LLVM specialises the inlined
        // `clear_session_residue_for_class` body to the Idle arm only,
        // eliding the 5-arm dispatch entirely.
        self.clear_session_residue_for_class(crate::state::StatePushClass::Idle);

        // DEF-154 (B+H): write-side keeps its brand (`'wb`) for
        // tier-1 `WriteRange::apply`; read side is unbranded.
        // DEF-208: caller is `ReadyGuard::push_command`, which proves
        // `state == Idle` via the witness-guard typestate (DEF-198).
        // DEF-269 v2: row_desc_slot threaded through for BindExecute
        // (other commands ignore it).
        debug_assert!(
            matches!(self.state, ProtoState::Idle),
            "push_command_internal: caller (ReadyGuard) must guarantee Idle state",
        );
        let state = &mut self.state;
        let row_desc_slot = &mut self.row_desc_slot;
        // DEF-212: reserved kept alive across PushCommand::execute and
        // materialise_push (the latter appends static SYNC bytes).
        // DEF-270 N-D (Phase 2, 2026-05-10): construct typed
        // `StateSetter<'_, C::PostState>` here — this is the only
        // call site in the crate that mints a setter via
        // `pub(crate) StateSetter::new`. The raw `&mut ProtoState`
        // never escapes this function. `execute()` consumes the
        // setter via `install_post_state` (happy path) or
        // `install_errored` (try_builder! Err path).
        write_buf.with_branded(|mut wb| -> Result<(), crate::action::PushFailure> {
            let mut reserved = wb.reserve();
            let mut staged = StagedActions::new();
            // DEF-271 cluster A (2026-05-10): StateSetter::new now takes
            // an IdleStateProof witness, structurally binding the
            // `state == Idle` precondition at the mint site. The
            // `debug_assert!(matches!(self.state, ProtoState::Idle))`
            // above proves the precondition; we mint two proofs (one
            // for the setter, one for execute()'s _proof param) — both
            // land at the same load-bearing assertion.
            let setter = crate::state_setter::StateSetter::<C::PostState>::new(
                state,
                crate::guard::IdleStateProof::new(),
            );
            cmd.execute(
                setter,
                row_desc_slot,
                &mut staged,
                &mut reserved,
                crate::guard::IdleStateProof::new(),
            );
            materialise_push(staged, &mut reserved)
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
    pub fn feed_inbound(&mut self, bytes: &[u8]) -> Result<(), crate::buf::ReadBufFull> {
        if matches!(self.state, ProtoState::Errored(_)) {
            // Silent no-op — terminal state, further bytes irrelevant.
            // Caller learns of Errored via `connection_status()` /
            // `advance_one_frame()` returning `FeedEvent::Close`.
            return Ok(());
        }
        self.read_buf.append(bytes)
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

        let classification = if matches!(self.state, ProtoState::Errored(_)) {
            IngressClassification::AlreadyErrored
        } else {
            match self.read_buf.append(bytes) {
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
                        fail_inflight_no_readbuf(
                            state,
                            ProtocolError::FrameTooLarge { declared },
                            &mut staged,
                            malformed_counter,
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
                            // DEF-185 P2-B (audit 2026-04-24): surface
                            // MalformedPayload via counter. Pre-fix
                            // `{}` silently collapsed the outcome;
                            // post-fix mirrors `n_malformed_bool_dropped`
                            // for ops diagnostic visibility.
                            //
                            // DEF-196: lazy-init session_params Box only
                            // when actually writing (here).
                            //
                            // DEF-271 cluster B (2026-05-10): the
                            // post-record MalformedPayload bump now lives
                            // INSIDE record_param_status_with_slot — the
                            // helper takes a SessionParamsSlot witness
                            // (gated on AtParameterStatusFrame auth tag)
                            // and consumes it via record() OR
                            // bump_malformed_param_status() depending on
                            // parse outcome. Consolidates the two-step
                            // "parse → caller-bumps" into a single
                            // mutation site behind the witness.
                            //
                            // DEF-271 cluster C (2026-05-10): full mint+use
                            // moved into the leaf submodule
                            // `_parameter_status_admit_leaf::admit_parameter_status_frame`.
                            // The auth tag's struct literal `AtParameterStatusFrame(())`
                            // is constructible only inside that submodule
                            // (private field). External call sites in mod
                            // protocol invoke the leaf helper.
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
                            // DEF-271 cluster B (2026-05-10): write
                            // gated through SessionParamsSlot witness.
                            //
                            // DEF-271 cluster C (2026-05-10): full
                            // mint+use moved into the leaf submodule
                            // `_notice_response_admit_leaf::admit_notice_response_frame`.
                            // The auth tag literal is private to that
                            // submodule.
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
                        let outcome = dispatch(
                            state,
                            tag,
                            payload,
                            &mut reserved,
                            terminal_row_desc,
                            error_arena_slot,
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
    /// DEF-188 — entry-point terminal-row-desc reclamation.
    ///
    /// DEF-189 — entry-point session-residue reclamation.
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
    /// Pre-Path-2 the `match self.state { Idle => …, Errored(_) => …,
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
    fn clear_session_residue_for_class(
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
        match class {
            crate::state::StatePushClass::Idle => {
                // DEF-270 R-rephrased: clear via SchemaParkedSlot witness.
                // DEF-271 cluster C (2026-05-10): mint+use moved into
                // the leaf submodule `_clear_residue_leaf`.
                _clear_residue_leaf::clear_schema_slot_residue(&mut self.row_desc_slot);
                // DEF-196: only clear arena if it was ever allocated.
                if let Some(arena) = self.error_arena.as_deref_mut() {
                    arena.clear();
                }
            }
            crate::state::StatePushClass::Errored(_) => {
                // DEF-270 R-rephrased + DEF-271 cluster C: same leaf
                // submodule helper as the Idle arm.
                _clear_residue_leaf::clear_schema_slot_residue(&mut self.row_desc_slot);
                if let Some(arena) = self.error_arena.as_deref_mut() {
                    arena.clear();
                }
                // DEF-189 Q8-C3 + DEF-205 step 3: session-state
                // forfeit on tear-down; `SessionParams::clear`'s Drop
                // chain scrubs `SecretBoundedStr` bytes.
                //
                // DEF-271 cluster B (2026-05-10): write gated through
                // SessionParamsSlot witness. AtClearSessionResidue
                // implements both SchemaWriteAuth and
                // SessionParamsWriteAuth — the same residue-cleanup
                // site clears both slots; one tag, two sealed-trait
                // impls (architect's #9 finding).
                //
                // DEF-271 cluster C (2026-05-10): mint+use for the
                // session-params clear moved into the leaf helper too.
                _clear_residue_leaf::clear_session_params_residue(&mut self.session_params);
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
    #[inline]
    pub(crate) fn read_buf_append(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        self.read_buf.append(bytes)
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
        self.read_buf.populated()
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
    /// Pre-DEF-188/-189 the per-row hot path did `match &self.state`
    /// twice: once for the streaming-variant gate (returning the
    /// `reply_id`) and once after `read_buf_advance` to re-project
    /// the schema field on the variant. Two enum matches per row.
    ///
    /// Post-DEF-189 the fast path is `match &self.state` ONCE for
    /// the gate (with the schema NOT in the variant) + a single
    /// `Option::as_ref` projection here. The Option projection is
    /// strictly cheaper than the enum match — one byte read for the
    /// discriminant, one ptr-deref on Some.
    #[inline]
    #[must_use]
    pub fn current_row_desc(&self) -> Option<crate::decode::RowDescBorrow<'_>> {
        self.row_desc_slot
            .as_ref()
            .map(crate::decode::RowDescBorrow::from_ref)
    }

    /// DEF-189: fused state classification for the row-stream
    /// fast-path entry. Single `match &self.state` returns the
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
        match &self.state {
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
    /// Pre-DEF-271 the helper wrote `*self.state = Errored(...)`
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
                  drained by the Errored install. Caller MUST emit StreamItem::FailReply \
                  or equivalent — dropping it leaks the user's oneshot-receiver \
                  (zombie-reply class)."]
    pub(crate) fn install_errored_read_cursor_advance(&mut self) -> Option<NonZeroU64> {
        let cause = ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::ReadCursorAdvance,
        };
        crate::state_setter::FeedStateSetter::new(&mut self.state)
            .drain_and_install_errored(cause.state_kind())
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
                  drained by the Errored install. Caller MUST emit StreamItem::FailReply \
                  or equivalent — dropping it leaks the user's oneshot-receiver \
                  (zombie-reply class)."]
    pub(crate) fn install_errored_malformed_data_row(
        &mut self,
        total_len: usize,
    ) -> Option<NonZeroU64> {
        let cause = ProtocolError::MalformedDataRow { total_len };
        crate::state_setter::FeedStateSetter::new(&mut self.state)
            .drain_and_install_errored(cause.state_kind())
    }

    // DEF-188: install_errored_stale_schema_ref DELETED — there is
    // no longer a SchemaRef type or generation drift class. State
    // variants carry RowDesc inline; the fast-path reads
    // `&self.state.row_desc` directly. The "stale ref" bug class is
    // architecturally impossible (no handle to be stale).

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
        // DEF-211 FAKE-01: cached classification (see feed_bytes for
        // rationale).
        let entry_class = self.state.push_class();
        self.clear_session_residue_for_class(entry_class);
        // DEF-184 audit (2026-04-24): `apply_pending_advance`
        // DELETED — the deferred mechanism is gone (post-DEF-154 Y
        // StreamRowRange delete, cursor advance happens in-scope
        // inside feed_bytes_impl). Nothing to catch up.
        crate::row_stream::RowStream::new(self, write_buf)
    }
}

/// DEF-189 — classifier output for [`PgProtocol::classify_for_iter_rows`].
///
/// 3-variant enum (each ZST-discriminator except Streaming carrying
/// `NonZeroU64`) selecting the row-stream fast-path entry behaviour.
/// Returned by a single `match &self.state` in
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
    let raw_id = crate::state_setter::FeedStateSetter::new(state)
        .drain_and_install_errored(state_kind);
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
#[cfg(test)]
fn compute_push(
    cmd: PgCommand,
    state: &mut ProtoState,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> StagedActions {
    let mut staged = StagedActions::new();
    match cmd {
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
            // DEF-271 cluster A: StateSetter::new requires IdleStateProof.
            // The Idle arm of push_class() is the precondition justification.
            let setter = crate::state_setter::StateSetter::<
                crate::push_command::PingAwaitingRfqInstall,
            >::new(state, crate::guard::IdleStateProof::new());
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
#[cfg(test)]
#[expect(clippy::too_many_arguments, reason = "compute_push_startup is an internal helper for Pg startup-command dispatch; its arg count matches the `PgCommand::Startup` payload + write_buf + staged accumulator. Splitting into a struct-arg would obscure the pure-compute framing (DEF-059).")]
fn compute_push_startup(
    state: &mut ProtoState,
    user: Ident,
    database: Option<DatabaseName>,
    app_name: Option<ApplicationName>,
    credentials: Credentials,
    reply: ReplyId<crate::reply_id::StartupKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    // DEF-186 perf-recovery 2026-04-24: signature changed to
    // `&mut ProtoState` with `()` return. Idle arm writes new state
    // via `*state = ...`; preserve arms (Errored / Connecting /
    // PingAwaiting / BusyQuery) leave state untouched.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-271 cluster A: StateSetter::new requires IdleStateProof.
            let setter = crate::state_setter::StateSetter::<
                crate::push_command::StartupPostInstall,
            >::new(state, crate::guard::IdleStateProof::new());
            compute_push_startup_idle_only(
                setter, user, database, app_name, credentials, reply, staged, reserved,
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
        }
        crate::state::StatePushClass::BusyQuery => {
            emit_actions!(staged, budget: 1, [
                StagedAction::FailReply {
                    id: reply.consume(),
                    cause: ProtocolError::CommandInProgress,
                },
            ]);
        }
    }
}

/// DEF-208 — Idle-only path for [`PgCommand::Startup`] push.
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
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    // DEF-186 perf-recovery 2026-04-24: &mut state signature.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-271 cluster A: StateSetter::new requires IdleStateProof.
            let setter = crate::state_setter::StateSetter::<
                crate::push_command::SimpleQueryAwaitingFirstResponseInstall,
            >::new(state, crate::guard::IdleStateProof::new());
            compute_push_simple_query_idle_only(setter, sql, reply, staged, reserved);
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

/// DEF-208 — Idle-only path for [`PgCommand::SimpleQuery`].
#[inline]
pub(crate) fn compute_push_simple_query_idle_only(
    setter: crate::state_setter::StateSetter<
        '_,
        crate::push_command::SimpleQueryAwaitingFirstResponseInstall,
    >,
    sql: &crate::ident::Sql,
    reply: ReplyId<crate::reply_id::QueryKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    let range = try_builder!(build_query_message(sql, reserved), setter, reply, staged);
    emit_actions!(staged, budget: 1, [
        StagedAction::SendBytesRange(range),
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
#[cfg(test)]
fn compute_push_parse(
    state: &mut ProtoState,
    stmt_name: &crate::ident::StmtName,
    sql: &crate::ident::Sql,
    reply: ReplyId<crate::reply_id::ParseKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    // DEF-186 perf-recovery 2026-04-24: &mut state signature.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // DEF-271 cluster A: StateSetter::new requires IdleStateProof.
            let setter = crate::state_setter::StateSetter::<
                crate::push_command::ParseAwaitingParseCompleteInstall,
            >::new(state, crate::guard::IdleStateProof::new());
            compute_push_parse_idle_only(setter, stmt_name, sql, reply, staged, reserved);
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

/// DEF-208 — Idle-only path for [`PgCommand::Parse`].
#[inline]
pub(crate) fn compute_push_parse_idle_only(
    setter: crate::state_setter::StateSetter<
        '_,
        crate::push_command::ParseAwaitingParseCompleteInstall,
    >,
    stmt_name: &crate::ident::StmtName,
    sql: &crate::ident::Sql,
    reply: ReplyId<crate::reply_id::ParseKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    let range = try_builder!(
        build_parse_message(stmt_name, sql, reserved),
        setter,
        reply,
        staged
    );
    emit_actions!(staged, budget: 2, [
        StagedAction::SendBytesRange(range),
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
            // DEF-271 cluster A: StateSetter::new requires IdleStateProof.
            let setter = crate::state_setter::StateSetter::<
                crate::push_command::DescribeStatementAwaitingParamDescInstall,
            >::new(state, crate::guard::IdleStateProof::new());
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
            // DEF-271 cluster A: StateSetter::new requires IdleStateProof.
            let setter = crate::state_setter::StateSetter::<
                crate::push_command::DescribePortalAwaitingRowDescOrNoDataInstall,
            >::new(state, crate::guard::IdleStateProof::new());
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
    // DEF-270 R-rephrased (Phase 1): park via SchemaParkedSlot witness.
    // DEF-270 N-D (Phase 2): typed witness pairs BindExecute → BindExecuteAwaitingBindComplete{Dml,Select}.
    // DEF-271 cluster C (Phase 3): mint+park moved into the leaf
    // submodule `_bind_execute_select_install_leaf::install_select_transition`.
    // The auth tag's struct literal is private to that submodule.
    let post_install = match row_desc {
        Some(desc) => {
            _bind_execute_select_install_leaf::install_select_transition(row_desc_slot, desc);
            crate::push_command::BindExecutePostInstall::Select { reply }
        }
        None => crate::push_command::BindExecutePostInstall::Dml { reply },
    };
    setter.install_post_state(post_install);
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

/// DEF-212 (Alt Y', architect-vetted impl plan, audit 2026-05-04):
/// push-path materialiser. Convert [`StagedActions`] into
/// `Result<(), PushFailure>` while appending any [`StagedAction::SendBytesStatic`]
/// bytes into the caller's `BrandedWriteReserved` (e.g., trailing
/// `SYNC_WIRE_BYTES` post-Bind+Execute / post-Parse / post-Describe).
///
/// # Why a separate materialiser from [`materialise`]
///
/// [`materialise`] is FEED-side: it converts staged actions into
/// `OutActions<'w, 'r>` which the caller iterates to drive I/O.
/// [`materialise_push`] is PUSH-side: bytes already live in the
/// caller's `WriteBuf` (compute_push_*_idle_only wrote them via the
/// branded reserved); the caller drains `wb.as_bytes()` post-Ok to
/// get the full outbound frame stream — the concatenation of all
/// ranges with appended Sync. No `OutActions` allocation per push
/// call — the per-call return frame shrinks 800 → ~80 B (DEF-212
/// headline).
///
/// # Per-StagedAction semantics
///
/// - [`StagedAction::SendBytesRange`] — bytes already in
///   `reserved.as_bytes()[range.start..range.start+range.len]` from
///   the builder. M5 verification: `range.apply(reserved.as_bytes())`
///   must resolve cleanly (`Some(_)`); the slice is unused (caller
///   drains the entire `wb.as_bytes()` post-Ok), the call exists
///   solely to detect brand/bounds invariant breaks. `apply == None`
///   is architecturally unreachable per DEF-154 (N+W) brand
///   discipline; classified `debug_assert!(false)` per architect M5.
///
/// - [`StagedAction::SendBytesStatic`] — append the static bytes
///   (e.g., `SYNC_WIRE_BYTES` post-Bind+Execute) to `reserved` via
///   `push_bytes`. Capacity is proven by const-asserts in
///   `write_buf.rs`:
///   - Bind+Execute+Sync (line 208-217 — pre-DEF-212)
///   - Describe+Sync (line 247-251 — pre-DEF-212)
///   - **Parse+Sync (DEF-212 M1, audit 2026-05-04)** — sibling pin,
///     closes a pre-(212) tier-4 "happens to fit" gap to tier-1.
///   - Ping=Sync alone (5 B trivially fits 2176 B empty wb).
///
///   `push_bytes` Err arm is architecturally-dead per the const-
///   assert chain; classified `debug_assert!(false)` for dev-time
///   loud signal, silent in release (the failure mode is "wire
///   frame truncated → server detects malformed → server errors";
///   not memory-unsafe).
///
/// - [`StagedAction::DeliverReply`] — UNREACHABLE on push paths.
///   Push commands transition state to a "waiting for server reply"
///   variant; the actual reply (`Pong`, `QueryComplete`, `ParseComplete`,
///   `Describe*Complete`) is delivered later from a feed_bytes call
///   processing the corresponding server frame. Any `DeliverReply`
///   in push staged would indicate a compute_push refactor regression
///   (or pipelining work without DEF-212 update). Classified
///   `debug_assert!(false)` per architect M5.
///
/// - [`StagedAction::FailReply { id, cause }`] — the builder failed
///   (`EmptyWriteRange`, `BuilderCapacityOverflow`,
///   `ParamsWriterOverflow`) OR the public push API was reached
///   without `state == Idle` (architecturally-dead via DEF-198
///   ReadyGuard + IdleStateProof witness; classified upstream by
///   `compute_push` non-Idle arms). Captured for `Result::Err` arm.
///   Architecturally exactly one `FailReply` per push cycle (single
///   builder error per command); the `if failure.is_none()` guard is
///   defensive against future pipelining refactors that batch pushes.
///
/// - [`StagedAction::CloseSocket`] — paired with `FailReply` on push
///   paths (`install_errored` emits both atomically). State has
///   ALREADY transitioned to `Errored` via `install_errored` from
///   inside the compute_push body. Caller learns to close the
///   socket via the [`crate::PushFailure`] `#[must_use]` contract;
///   no explicit signal needed in `materialise_push`.
///
/// # `BrandedWriteReserved` lifetime
///
/// The reserved is borrowed mutably for the whole materialise call.
/// The caller (`push_command_internal`) holds the reserved across
/// `compute_push_idle_only` (which writes the main frame) AND
/// `materialise_push` (which appends Sync). Sequential mutable
/// borrows; brand stays sealed inside the parent `with_branded`
/// closure.
///
/// # Tier classification
///
/// - Push success: tier-2 structural (bytes-in-wb contract).
/// - Push failure: tier-1 by `Result::Err` arm exhaustive match.
/// - Dead arm classification: tier-2 structural (debug_assert in
///   dev/test, architectural-impossibility-by-const-assert in release).
// DEF-236 (audit 2026-05-05): single call site (push_command_internal).
// ASM diff (revert vs `#[inline]`): standalone symbol disappears,
// body folds into caller's tail. LLVM accepts hint at this size +
// site-count combination — codegen evidence supports the annotation.
#[inline]
fn materialise_push(
    staged: StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<(), crate::action::PushFailure> {
    let mut failure: Option<crate::action::PushFailure> = None;
    for sa in staged {
        match sa {
            StagedAction::SendBytesRange(range) => {
                // M5 verification: apply must resolve cleanly. Slice unused
                // (caller drains entire wb.as_bytes() post-Ok).
                match range.apply(reserved.as_bytes()) {
                    Some(_slice) => {
                        // OK; bytes are in wb at the verified range.
                    }
                    None => {
                        // DEF-238 (audit 2026-05-05): cold hint on the
                        // architecturally-dead arm. The brand-discipline
                        // construction guarantees this branch is
                        // unreachable under intact forbid_unsafe.
                        core::hint::cold_path();
                        debug_assert!(
                            false,
                            "DEF-212 M5: SendBytesRange.apply == None — \
                             architecturally impossible per intact brand \
                             discipline (DEF-154 N+W). Compiler bug or \
                             memory corruption.",
                        );
                        // Release: silent no-op. Reaching this branch under
                        // intact forbid_unsafe + brand discipline implies
                        // external memory corruption; the wire frame may be
                        // partially truncated (server detects malformed,
                        // errors out — not memory-unsafe at protocol layer).
                    }
                }
            }
            StagedAction::SendBytesStatic(s) => {
                // Append to wb. Const-asserts in write_buf.rs prove capacity
                // (Bind+Execute+Sync line 208; Describe+Sync line 247;
                // Parse+Sync DEF-212 M1; Ping=Sync trivially).
                match reserved.push_bytes(s) {
                    Ok(()) => {}
                    Err(_) => {
                        // DEF-238 (audit 2026-05-05): cold hint —
                        // architecturally-dead per the const-assert chain
                        // in write_buf.rs (max_*_message_size sums).
                        core::hint::cold_path();
                        debug_assert!(
                            false,
                            "DEF-212 M5: SendBytesStatic append overflowed wb — \
                             const-assert chain in write_buf.rs violated. If a \
                             new push command emits Sync, ship a sibling \
                             const-assert in the same commit.",
                        );
                        // Release: silent. Architecturally-dead per the
                        // const-assert chain.
                    }
                }
            }
            StagedAction::DeliverReply(_) => {
                // DEF-238 (audit 2026-05-05): cold hint. Push paths
                // never emit DeliverReply (replies come from server
                // via feed_bytes, not push). Architecturally dead.
                core::hint::cold_path();
                debug_assert!(
                    false,
                    "DEF-212 M5: push paths must NEVER emit DeliverReply — \
                     replies come from server via feed_bytes only. Reaching \
                     this branch indicates a compute_push refactor regression \
                     (or pipelining work without DEF-212 update).",
                );
            }
            StagedAction::FailReply { id, cause } => {
                // DEF-238 (audit 2026-05-05): cold hint. Builder
                // failures (EmptyWriteRange / BuilderCapacityOverflow /
                // ParamsWriterOverflow) are rare classified-Err paths;
                // happy path (no FailReply staged) dominates production.
                core::hint::cold_path();
                // Capture for Err arm. Architecturally exactly one FailReply
                // per push cycle (single builder error per command).
                if failure.is_none() {
                    failure = Some(crate::action::PushFailure { id, cause });
                }
            }
            StagedAction::CloseSocket => {
                // Paired with FailReply on push paths; state already Errored.
                // Caller closes socket per PushFailure #[must_use] contract.
            }
        }
    }
    match failure {
        None => Ok(()),
        Some(f) => Err(f),
    }
}

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
fn materialise<'w, 'r>(
    staged: StagedActions,
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
            .field("session_params", self.cold_session_params())
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
    fn populate_residue(proto: &mut PgProtocol) {
        proto.row_desc_slot._set_for_test(Some(RowDesc::EMPTY));
        proto.session_params._set_for_test(Some(dirty_session_params()));
        proto.error_arena = Some(alloc::boxed::Box::new(
            crate::error_arena::ErrorArena::new(),
        ));
    }

    /// Replace `proto.state` with `Idle` so the destructor doesn't
    /// trip the in-flight `ReplyId<_>` Drop-guard at scope end.
    fn quench_inflight(proto: &mut PgProtocol) {
        let prev = core::mem::replace(&mut proto.state, ProtoState::Idle);
        match prev.take_inflight_reply_raw_id() {
            Some(_) | None => {}
        }
    }

    fn session_params_is_pristine(proto: &PgProtocol) -> bool {
        // DEF-211 INNO-01 (2026-05-04): trait method via `Pristine` import.
        // Inherent `__pristine_const` would also work but trait dispatch
        // here matches polymorphic intent (test helper takes any
        // `SessionParams`-like thing).
        use crate::pristine::Pristine as _;
        match proto.session_params.as_deref() {
            Some(p) => p.is_pristine(),
            None => true,
        }
    }

    #[test]
    fn idle_clears_row_desc_preserves_session_params() {
        let mut proto = PgProtocol::new();
        // Default state is `Idle` post-`new()`.
        populate_residue(&mut proto);
        proto.clear_session_residue_for_class(proto.state.push_class());

        assert!(
            proto.row_desc_slot.is_none(),
            "Idle must clear row_desc_slot",
        );
        assert!(
            proto.error_arena.is_some(),
            "Idle preserves the error_arena Box (contents cleared internally)",
        );
        assert!(
            proto.session_params.is_some(),
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
        proto.state = ProtoState::Errored(
            StateErrorKind::from_kind_or_internal(ErrorKind::Framing),
        );
        populate_residue(&mut proto);
        proto.clear_session_residue_for_class(proto.state.push_class());

        assert!(
            proto.row_desc_slot.is_none(),
            "Errored must clear row_desc_slot",
        );
        assert!(
            proto.session_params.is_some(),
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
        proto.state = ProtoState::ConnectingStartupTrust {
            reply: ReplyId::from_raw(nz(11)),
        };
        populate_residue(&mut proto);
        proto.clear_session_residue_for_class(proto.state.push_class());

        assert!(
            proto.row_desc_slot.is_some(),
            "Connecting (StatePushClass::Connecting) must preserve row_desc_slot",
        );
        assert!(
            proto.session_params.is_some(),
            "Connecting must preserve session_params Box",
        );
        assert!(
            !session_params_is_pristine(&proto),
            "Connecting must preserve session_params content",
        );
        assert!(
            proto.error_arena.is_some(),
            "Connecting must preserve error_arena",
        );
        quench_inflight(&mut proto);
    }

    #[test]
    fn ping_awaiting_preserves_all_residue() {
        let mut proto = PgProtocol::new();
        proto.state = ProtoState::PingAwaitingRfq(ReplyId::from_raw(nz(12)));
        populate_residue(&mut proto);
        proto.clear_session_residue_for_class(proto.state.push_class());

        assert!(
            proto.row_desc_slot.is_some(),
            "PingAwaiting (StatePushClass::PingAwaiting) must preserve row_desc_slot",
        );
        assert!(
            !session_params_is_pristine(&proto),
            "PingAwaiting must preserve session_params content",
        );
        assert!(
            proto.error_arena.is_some(),
            "PingAwaiting must preserve error_arena",
        );
        quench_inflight(&mut proto);
    }

    #[test]
    fn busy_query_preserves_all_residue() {
        let mut proto = PgProtocol::new();
        proto.state = ProtoState::SimpleQueryStreamingRows {
            reply: ReplyId::from_raw(nz(13)),
        };
        populate_residue(&mut proto);
        proto.clear_session_residue_for_class(proto.state.push_class());

        assert!(
            proto.row_desc_slot.is_some(),
            "BusyQuery (StatePushClass::BusyQuery) must preserve row_desc_slot",
        );
        assert!(
            !session_params_is_pristine(&proto),
            "BusyQuery must preserve session_params content",
        );
        assert!(
            proto.error_arena.is_some(),
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

        // ConnectingStartupScram — DEF-184 A10/B22 revert 2026-04-24:
        // variant carries `scram: ScramSession` inline per tier-1
        // invariant. Construction requires SCRAM data.
        if let Some(user) = mk_user()
            && let Ok(pw) = crate::password::Password::try_from_bytes(b"pw")
        {
            let raw_prev = nz(405_100);
            let raw_new = nz(405_101);
            let scram = alloc::boxed::Box::new(
                crate::scram::session::ScramSession::from_password(
                    crate::sensitive::Sensitive::new(pw),
                ),
            );
            let prev = ProtoState::ConnectingStartupScram {
                reply: ReplyId::from_raw(raw_prev),
                scram,
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

        // ConnectingScramAwaitingServerFirst — tier-1 variant-carries-field.
        if let Some(user) = mk_user()
            && let Ok(pw) = crate::password::Password::try_from_bytes(b"pw")
        {
            let raw_prev = nz(405);
            let raw_new = nz(406);
            let scram = alloc::boxed::Box::new(
                crate::scram::session::ScramSession::from_password(
                    crate::sensitive::Sensitive::new(pw),
                ),
            );
            let prev = ProtoState::ConnectingScramAwaitingServerFirst {
                reply: ReplyId::from_raw(raw_prev),
                scram,
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

        // ConnectingScramAwaitingServerFinal — `expected_server_sig` inline.
        if let Some(user) = mk_user() {
            let raw_prev = nz(407);
            let raw_new = nz(408);
            let prev = ProtoState::ConnectingScramAwaitingServerFinal {
                reply: ReplyId::from_raw(raw_prev),
                expected_server_sig: crate::scram::types::SecretDigest::new([0_u8; 32]),
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
                secret_key: crate::sensitive::Sensitive::new(2_i32),
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
        // DEF-198 ext: internal test now goes through `ReadyGuard`
        // since `push_bind_execute_internal`'s signature requires the
        // sealed `IdleStateProof` witness (constructible only inside
        // `mod guard`). Production-equivalent path; fresh proto is
        // in `Idle` state so `as_ready()` returns `Some`. The
        // architecturally-dead `None` arm early-returns to satisfy
        // the lib-level `clippy::panic` forbid bundle.
        let Some(guard) = proto.as_ready() else { return };
        let result = guard.push_bind_execute(
            &crate::ident::PortalName::default(),
            &crate::ident::StmtName::default(),
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
