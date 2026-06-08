//! The `PgProtocol` state machine — entry points and bounded action emit.
//!
//! Two public methods drive the machine:
//!
//! - [`PgProtocol::push_command`] — user pushes a a command from `push_command`;
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
//!   `write_buf`. Push-side state transitions are **pure compute
//!   over (current state × command)**.
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
use core::num::NonZeroU64;
// `PgCommand` enum is referenced only by the test-only 5-arm
// `compute_push_*` dispatchers — no real call sites exist for a
// runtime-polymorphic `compute_push_idle_only` slow-path dispatcher
// nor for `impl PushCommand for PgCommand`. The `use` is
// `#[cfg(test)]`-gated to avoid an unused-import warning in release
// builds.
#[cfg(test)]
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
// emit_actions! — tier-1 per-site action-budget enforcement
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
/// # Infallible-only form
///
/// Only the no-bail infallible form is offered. A naive shape would
/// expose an additional `on_overflow: break` loop form that bailed the
/// enclosing loop on Err — looking safe because the dispatch gate at
/// `feed_bytes` reserves `WORST_CASE_PER_DISPATCH` slots before loop
/// entry. That shape is a silent-loss footgun: if the gate ever
/// drifted, terminal `FailReply + CloseSocket` could be dropped while
/// state had ALREADY transitioned to `Errored` — caller sees
/// state_errored but no Action delivery, orphaned oneshot receiver.
///
/// Instead the dispatch gate at feed_bytes reserves
/// `WORST_CASE_PER_DISPATCH = 2` slots before entering any arm, so the
/// Errored arm's 2-action emission always fits the staged cap.
/// `match Ok(()) | Err(_) => {}` is explicit dead-arm handling (no
/// `.unwrap_or(())` silent fallback, no debug_assert panic target).
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

/// Centralises the FailReply + CloseSocket + Errored cascade
/// triggered by a builder Err.
///
/// Every `build_*_message` returns `Result<WriteRange,
/// ProtocolError>`. The Err path is architecturally cold (builder
/// bug / const-drift / user ParamsWriter overflow) but classified —
/// `compute_push_*` handles it via `FailReply + CloseSocket +
/// Errored` state transition.
///
/// Each `compute_push_*_idle_only` uses `let range =
/// try_builder!(build_X(...), setter, reply, staged);`. On
/// `Err(cause)`: derive `StateErrorKind` via `cause.state_kind()`,
/// emit FailReply + CloseSocket into `staged`, consume `setter`
/// via `install_errored(state_kind)`, and early-return from the
/// enclosing `compute_push_*` function.
///
/// The macro early-returns, so it must be used in a position
/// where `return` is legal.
///
/// # Setter consumption
///
/// The macro takes `$setter: StateSetter<'_, _>` and consumes it
/// via [`StateSetter::install_errored`] on the Err path. A naive
/// alternative would take `$state: &mut ProtoState` and write
/// `*$state = ProtoState::Errored(state_kind)` directly — but raw
/// `&mut ProtoState` is no longer reachable from `execute()` (only
/// via [`crate::state_setter::StateSetter`]), and the setter's
/// `must_use` lint surfaces a missed install at the call site
/// rather than leaving the responsibility to a docstring discipline
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
/// **Idle-only contract** is enforced by setter privacy:
/// `StateSetter::new` is `pub(crate)`, callable only from
/// `PgProtocol::push_command_internal` which asserts
/// `matches!(state, Idle)` at entry. A defense-in-depth
/// debug_assert on the same invariant would be redundant — same
/// invariant, single load-bearing assertion site.
macro_rules! try_builder {
    // .b: macro unchanged — StagedAction::FailReply retains
    // cause inline; materialise (and the push-path PushFailure fold)
    // perform the park-into-fail_cause-slot step at the StagedAction
    // → Action / PushFailure transformation boundary.
    ($result:expr, $setter:expr, $reply:expr, $staged:expr) => {
        match $result {
            Ok(r) => r,
            Err(cause) => {
                // state_kind is total — no unwrap_or_else + debug_assert
                // dance. Builders never return AlreadyClosed; the total
                // projection fills any hypothetical AlreadyClosed with
                // Internal honestly.
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
/// - **Per emission site — tier 1 compile.** Each call to
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
/// # Budget audit
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
/// # Sizing rationale (= 8)
///
/// Row streaming emits one `StreamRow` per `DataRow` frame. A single
/// `feed_bytes` call receiving 7 rows + `CommandComplete` +
/// `ReadyForQuery` produces 7 × `StreamRow` + 1 × `DeliverReply`.
/// A 4-slot cap would force 2+ extra `feed_bytes` calls per batch;
/// 8 covers realistic streaming density with single-digit call counts
/// on typical row sizes.
///
/// # Staged / output split
///
/// A naive shape would have a single constant govern BOTH the
/// staged (dispatch-side) and output (user-side) capacity. That
/// shape is unsafe: materialise can emit up to 2 actions per staged
/// entry on the `SendBytesRange.apply == None` fan-out path
/// (`CloseSocket`), so an 8-slot output container would silently
/// drop terminal actions via `.unwrap_or(())` on a 16-action
/// worst-case.
///
/// Instead `MAX_STAGED_PER_CALL = 8` bounds dispatch's stage
/// container; `MAX_ACTIONS_PER_CALL = MAX_STAGED_PER_CALL * MAX_FANOUT_PER_STAGED = 16`
/// bounds the output container (compile-asserted below). Worst-case
/// fanout is ARCHITECTURALLY contained — the silent-drop class is
/// closed at the type/capacity level, not at a runtime shield.
///
/// # Emission-site vs aggregate
///
/// - **Per emission site — tier 1 compile**: `emit_actions!` asserts
///   budget ≤ `MAX_STAGED_PER_CALL` via `const _: () = assert!(...)`.
/// - **Aggregate output — tier 1 compile**:
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
/// # Why `+1` and not `× 2`?
///
/// State-machine audit: at most ONE `DeliverReply` can fire per
/// dispatch call pre-pipelining. Terminal frames (RFQ/Z/CommandComplete/
/// AuthOk/ParseComplete/BindComplete/CloseComplete/NoData) transition
/// the state AWAY from the waiting-for-reply state, blocking a
/// second reply in the same feed_bytes iteration. Therefore:
/// 7 non-fanout staged + 1 fanout-2 staged = 7 + 2 = 9 actions.
///
/// # Pipelining regression trap
///
/// If a future pipelining refactor emits 2+ DeliverReply per
/// dispatch call (batched replies), bump
/// [`MAX_FANOUT2_ENTRIES_PER_CALL`] to match the max number of
/// simultaneous fanout-2 staged entries — the formula recomputes.
///
/// # Named constants vs magic literal
///
/// A naive shape would collapse the formula to `MAX_STAGED + 1` —
/// "same value (9), half the cognitive load." That collapse turns
/// the named topology terms into a magic `+1` literal: a future
/// pipelining refactor that adds a SECOND fanout-2 staged entry
/// would have to know to bump literal `+1` → `+2`, with the only
/// hint being a comment. **Drift surface: a comment.**
///
/// Instead the named constants stay. The formula
/// `MAX_STAGED + MAX_FANOUT2_ENTRIES_PER_CALL × (MAX_FANOUT_PER_STAGED − 1)`
/// is self-documenting; future pipelining work bumps a NAMED const
/// (e.g. `MAX_FANOUT2_ENTRIES_PER_CALL = 2`) instead of editing a
/// literal that requires reading docstrings to understand.
///
/// # Bench impact
///
/// `OutActions` stack reservation: `9 × 88 B = 792 B` vs the
/// naive `MAX_STAGED × 2 = 16 × 88 B = 1408 B`. Saves 616 B per
/// OutActions. Combined with `ManuallyDrop<heapless::Vec>` (0 B
/// zero-fill), OutActions is a lean stack frame.
pub const MAX_ACTIONS_PER_CALL: usize =
    MAX_STAGED_PER_CALL + MAX_FANOUT2_ENTRIES_PER_CALL * (MAX_FANOUT_PER_STAGED - 1);

/// Maximum fan-out factor of any single staged entry into emitted
/// `Action`s. Today only `DeliverReply` is fanout-2 (it emits an
/// extra `Action::FailReply` if the slot it targets has gone stale
/// since staging — the materialise-side stale-ref protection). All
/// other staged entries are fanout-1.
///
/// Pipelining note: if a future refactor introduces a fanout-3
/// staged entry, this constant rises and `MAX_ACTIONS_PER_CALL`
/// recomputes from the formula automatically.
///
/// `pub(crate)` — implementation-detail topology constant; external
/// consumers have no use case for reading it. Bumping it for
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
/// Pipelining will lift this to ≥2 (multiple concurrent inflight
/// replies resolvable in one feed_bytes iteration). Bump THIS
/// constant — `MAX_ACTIONS_PER_CALL` recomputes from the formula.
/// The `WORST_CASE_PER_DISPATCH` and `OutActions` budget math both
/// compose from this single source.
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
     (FANOUT − 1). Named constants enforce that pipelining work \
     bumps a NAMED magnitude, not an unnamed literal.",
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
/// no partial emission, no silent reply loss.
///
/// Current worst case: [`DispatchOutcome::Errored`] with `Some(reply_id)`
/// emits `FailReply + CloseSocket` = 2. Bumping this to 3 would require
/// a new 3-action dispatch outcome.
///
/// Pipelining blocker: pipelining changes the topology — a single
/// feed_bytes iteration might resolve multiple concurrent inflight
/// replies (e.g., DataRow for query A + CommandComplete for query
/// B). Worst case becomes ≥3; WORST_CASE_PER_DISPATCH and
/// MAX_ACTIONS_PER_CALL both revisit at pipelining implementation
/// time.
const WORST_CASE_PER_DISPATCH: usize = 2;

// Sanity asserts — the budget audit above demands at least
// WORST_CASE_PER_DISPATCH; practical batching needs meaningful
// headroom above that.
const _: () = assert!(MAX_ACTIONS_PER_CALL >= WORST_CASE_PER_DISPATCH);
const _: () = assert!(MAX_ACTIONS_PER_CALL >= 4, "practical batching needs ≥4 slots");

// Module-scope tier-1 pin: the `static EMPTY: SessionParams =
// SessionParams::new()` referenced from `cold_session_params`
// carries no SecretBoundedStr bytes — a `'static` value never
// drops, so its `ZeroizeOnDrop` chain never fires; the only safe
// state for a static SessionParams is fully pristine (every
// Option=None, every counter=0). A future refactor of
// `SessionParams::new()` that initialises a SecretBoundedStr field
// with a non-empty default would otherwise leak the bytes into
// static memory for the program's lifetime.
//
// Module scope so the const-eval is hoisted out of
// `cold_session_params`'s body — keeps the inline hint on that
// accessor effective by not embedding a const-eval expression
// inside it that the optimizer might consider when deciding to
// inline the outer function.
static _BS11_EMPTY_SESSION_PARAMS: SessionParams = SessionParams::new();
// Use the auto-derived `__pristine_const` inherent fn
// (const-callable). Runtime polymorphic `<SessionParams as
// Pristine>::is_pristine` cannot be const-called (trait methods
// aren't const on stable Rust as of MSRV 1.95).
const _BS11_EMPTY_SESSION_PARAMS_IS_PRISTINE: () = assert!(
    _BS11_EMPTY_SESSION_PARAMS.__pristine_const(),
    "static EMPTY: SessionParams must be pristine — see \
     `crate::pristine` module + `#[derive(Pristine)]` on SessionParams",
);

// Drift pin coupling `READ_BUF_CAP` to the `frames_consumed: u16`
// counter used in `feed_bytes_impl`. `frames_consumed` accumulates
// `total_len` per dispatched frame; each `total_len ≤ READ_BUF_CAP`.
// If `READ_BUF_CAP` ever grew past `u16::MAX`, the counter would
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
/// `!Sync` by construction (`PhantomData<Cell<()>>` field). Concurrent
/// access is impossible; a `&mut PgProtocol` is the only handle.
///
/// # Size budget
///
/// `size_of::<PgProtocol>()` is pinned in `lib.rs`. Budget composition:
/// - `ReadBuf`            ~4096 B  (I/O staging, READ_BUF_CAP)
/// - `state`              ~320 B  (ProtoState — RowDesc inline in
///   streaming/AwaitingRfq variants; SCRAM Boxed)
/// - `session_params`     ~420 B
/// - `terminal_row_desc`  ~268 B  (single-slot Option<RowDesc>)
/// - `error_arena`        ~290 B  (single-slot)
/// - padding + flags      varies
///
/// Any field addition or size growth must update the pin in
/// `lib.rs` alongside the code change.
/// Lazy-init helper for `Option<Box<ErrorArena>>`. Called by
/// `dispatch.rs` ErrorResponse arms when a server error payload
/// needs to be parsed and stored.
#[inline]
pub(crate) fn error_arena_or_init(
    slot: &mut Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
) -> &mut crate::error_arena::ErrorArena {
    slot.get_or_insert_with(|| {
        alloc::boxed::Box::new(crate::error_arena::ErrorArena::new())
    })
}

/// Lazy-init helper for `Option<Box<CommandTagsArena>>` ().
/// Called by `dispatch.rs` multi-statement arms when an
/// intermediate command-complete tag needs to be externalised for
/// `Action::IntermediateCommandComplete`.
#[inline]
pub(crate) fn command_tags_arena_or_init(
    slot: &mut Option<alloc::boxed::Box<crate::command_tags_arena::CommandTagsArena>>,
) -> &mut crate::command_tags_arena::CommandTagsArena {
    slot.get_or_insert_with(|| {
        alloc::boxed::Box::new(crate::command_tags_arena::CommandTagsArena::new())
    })
}

pub(crate) fn notices_arena_or_init(
    slot: &mut Option<alloc::boxed::Box<crate::notices_arena::NoticesArena>>,
) -> &mut crate::notices_arena::NoticesArena {
    slot.get_or_insert_with(|| {
        alloc::boxed::Box::new(crate::notices_arena::NoticesArena::new())
    })
}

/// Per-phase Inner for `<ConnectingPhase>`.
///
/// Every `SealedPhase` now points at a per-phase Inner:
/// `<DisconnectedPhase>::Inner = DisconnectedInner` (ZST),
/// `<ConnectingPhase>::Inner = ConnectingInner`,
/// `<ActivePhase>::Inner = ActiveInner`,
/// `<ClosedPhase>::Inner = ClosedInner`.
///
/// **Narrowed structure** — no `row_desc_slot` field (no
/// `RowDescription` is reachable from any Connecting state; every
/// dispatch arm writing `row_desc_slot` is gated on a
/// non-Connecting state variant in `dispatch.rs`) and no
/// `backend_key` field. The `(pid, secret_key)` material lives
/// inline in [`crate::state::ConnectingState::HandshakeReady`]'s
/// payload during the post-RFQ window: the `(PostAuthHaveKey, RFQ)`
/// dispatch arm writes the pair into the variant, and `into_active`
/// consumes the variant structurally to construct the inline
/// `BackendKey` on the destination `ActiveInner`.
///
/// **Tier-1 closure by storage absence**:
/// - `<ConnectingPhase>` CANNOT physically hold a non-Connecting
///   state variant (e.g. `SimpleQueryStreamingRows`) because
///   `ConnectingState` doesn't have those variants
/// - `<ConnectingPhase>` CANNOT physically write to `row_desc_slot`
///   (the field doesn't exist on `ConnectingInner`)
/// - `<ConnectingPhase>` CANNOT physically write to `backend_key`
///   (the field doesn't exist; install happens at the phase
///   boundary in `into_active`)
///
/// Adding a future variant to `ProtoState` requires the contributor
/// to ALSO route it into the appropriate per-phase enum
/// (`ConnectingState` / `ActiveState`) via the From/TryFrom
/// bijection — orphaned variants fail the build at the projection
/// sites.
///
/// **Layout**: state ConnectingState (~48 B) + read_buf 264 B +
/// 4× 8 B cells/slots + u32 + alignment ≈ **344 B**. ReadBuf 264 B
/// can shrink later via a ring-buffer migration.
///
/// **Construction**: only via
/// [`_proto_init_leaf::fresh_connecting_inner`] (token-gated,
/// leaf-private) called from `<DisconnectedPhase>::push_startup`.
///
/// **Field visibility**: private. The `pub` struct declaration is
/// required to mention the type as an associated `SealedPhase::Inner`
/// (E0446 mitigation); fields stay private, constructors stay
/// leaf-gated.
pub struct ConnectingInner {
    /// State narrowed to [`crate::state::ConnectingState`] variants
    /// (11 handshake states + `HandshakeReady` post-RFQ transition
    /// signal + transient Errored). **Tier-1 closure**:
    /// state-variant-in-wrong-phase is impossible by type — Active
    /// variants don't exist in `ConnectingState`.
    ///
    /// The `(pid, secret_key)` cancel-key material lives inline in
    /// `ConnectingState::HandshakeReady { pid, secret_key }` during
    /// the post-RFQ window. `into_active` consumes the payload
    /// structurally; there is no separate `backend_key` cell here.
    state: crate::state::ConnectingState,
    /// Inbound wire-byte staging for this connection.
    read_buf: ReadBuf,
    /// Session parameter accumulator. Populated by
    /// `ParameterStatus` / `NoticeResponse` filter arms during
    /// post-auth Connecting states (ConnectingPostAuthAwaitingKey +
    /// ConnectingPostAuthHaveKey).
    session_params: crate::session_params_slot::SessionParamsCell,
    /// Lazy-init slot for server ErrorResponse payloads. Populated
    /// by `TAG_ERROR_RESPONSE` arms during any Connecting state's
    /// `'E'`-frame handling.
    error_arena: Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
    /// Spill buffer for oversize ErrorResponse / NoticeResponse
    /// frames during handshake.
    partial_assembly: crate::partial_assembly::PartialAssemblyCell,
    /// .b — slot for parked `Action::FailReply.cause`
    /// during the handshake phase. Mirror of the `ActiveExtras.fail_cause`
    /// cell but living inline on `ConnectingInner` because
    /// `<ConnectingPhase>::Extras = ()` (cell-on-outer pattern is
    /// reserved for Active-only cells; Connecting-relevant slots
    /// live on the Inner). Per-cycle lifecycle mirrors the Active
    /// pattern. Callers query via
    /// [`crate::PgProtocol::fail_cause`] (per-phase impl).
    fail_cause: crate::fail_cause_slot::FailCauseSlotCell,
    /// Count of malformed frames observed during handshake.
    malformed_frame_count: u32,
    /// `!Sync` witness — `PhantomData<Cell<()>>` makes the type
    /// non-Sync so a `&mut` is the only handle.
    sync_marker: PhantomData<Cell<()>>,
}

/// Per-phase Inner for `<ActivePhase>`.
///
/// State field type narrows to [`crate::state::ActiveState`]
/// (~80 B, same as ProtoState). The other seven fields are
/// byte-identical to the carried Connecting cells — no
/// post-handshake field can be physically dropped (every cell is
/// reachable from at least one Active variant).
///
/// **Tier-1 closure by storage absence**:
/// - `<ActivePhase>` CANNOT physically hold a Connecting state
///   variant (e.g. `StartupScram`) because `ActiveState` doesn't
///   have those variants.
/// - State-variant-in-wrong-phase is impossible by-type.
///
/// **Layout**: ~536 B (state ActiveState 80 B + read_buf 264 B +
/// 4 cells 8 B each + u32 + alignment). The per-phase split
/// delivers tier-1 closure without a footprint regression.
///
/// **Construction**: only via [`_proto_init_leaf::fresh_active_inner`]
/// called from `<ConnectingPhase>::into_active`.
pub struct ActiveInner {
    /// State narrowed to [`crate::state::ActiveState`] variants
    /// (Idle + PingAwaitingRfq + all SimpleQuery/Parse/BindExecute/
    /// Describe flow variants + DrainRfqAfterError + transient
    /// Errored).
    state: crate::state::ActiveState,
    /// Inbound wire-byte staging for this connection.
    read_buf: ReadBuf,
    /// Session parameter accumulator.
    session_params: crate::session_params_slot::SessionParamsCell,
    /// Lazy-init slot for server ErrorResponse payloads.
    error_arena: Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
    /// Lazy-init slot for server NotificationResponse payloads (PG
    /// §55.7 LISTEN/NOTIFY surface — ). `Option<Box<_>>` so
    /// connections that never LISTEN pay zero (the slot stays
    /// `None`); first NOTIFY arrival via the dispatch pre-filter
    /// allocates one `Box<NotificationsArena>` for the connection's
    /// lifetime. Cleared per `feed_bytes` cycle (gen bump on the
    /// inner arena's `clear()` call invalidates outstanding
    /// `NotificationRef`s — defence-in-depth against the wrapper
    /// stashing refs past the cycle boundary).
    notifications_arena: Option<alloc::boxed::Box<crate::notifications_arena::NotificationsArena>>,
    /// Lazy-init slot for server notices (WARNING/NOTICE/INFO/DEBUG/LOG).
    /// Mirror of `notifications_arena`. Connections that never receive
    /// notices pay zero; first NoticeResponse allocates one Box.
    notices_arena: Option<alloc::boxed::Box<crate::notices_arena::NoticesArena>>,
    /// Lazy-init slot for COPY OUT data chunks.
    copy_chunks_arena: Option<alloc::boxed::Box<crate::copy_chunks_arena::CopyChunksArena>>,
    /// Lazy-init slot for multi-statement intermediate
    /// command tags (). Mirror of `notifications_arena`.
    /// Connections that never use batched SimpleQuery pay zero; first
    /// `IntermediateCommandComplete` emission allocates one
    /// `Box<CommandTagsArena>` for the connection's lifetime.
    /// Cleared per `feed_bytes` cycle (refs from prior cycles
    /// resolve `ArenaError::Stale`).
    command_tags_arena: Option<alloc::boxed::Box<crate::command_tags_arena::CommandTagsArena>>,
    /// Spill buffer for oversize ErrorResponse / NoticeResponse
    /// frames during query execution.
    partial_assembly: crate::partial_assembly::PartialAssemblyCell,
    /// Cancel-key payload, **inline non-Option**. Constructed at
    /// handshake completion by `<ConnectingPhase>::into_active`
    /// from the `ConnectingState::HandshakeReady { pid, secret_key }`
    /// variant's payload. Storage-absence proof for tier-1 closure
    /// on `with_cancel_request<R, F>(&self, f) -> R`: a
    /// `<ActivePhase>` proto cannot be constructed without a valid
    /// `BackendKey`, so the prior `Option<R>` return is now `R`
    /// (infallible).
    backend_key: crate::cancel::BackendKey,
    /// Count of malformed frames observed during query execution.
    malformed_frame_count: u32,
    /// `!Sync` witness — `PhantomData<Cell<()>>` makes the type
    /// non-Sync so a `&mut` is the only handle.
    sync_marker: PhantomData<Cell<()>>,
}

/// Dispatch-context bundle.
///
/// Captures the eight per-connection mutable references the
/// dispatch path consumes inside a single struct, so the dispatch
/// body lives as a **free function** (not a method on any per-phase
/// Inner). Per-phase `Inner` types carry SUBSETS of these fields;
/// the free-function form lets each phase's inherent method
/// assemble its own `DispatchContext` (built from the fields IT
/// carries) and forward to the same dispatch body without
/// method-resolution forcing the body to live on a single concrete
/// `Self`.
///
/// **Construction**: the disjoint-field-borrow rule (Rust 2018+)
/// lets `DispatchContext { state: &mut self.state, ... }` build the
/// struct from eight distinct `&mut self.<field>` borrows in one
/// struct literal — the borrow checker splits the borrow per-field.
///
/// **Tier impact**: refactoring-only. The free function
/// [`feed_bytes_dispatch`] body is bit-identical to a method on a
/// monolithic Inner (asm-diff verified); the eight `&mut`
/// parameters compile down to register pressure equivalent to
/// `&mut self` since each field's offset is a constant displacement
/// from `self`. LLVM inlines the wrapper methods unconditionally
/// (`#[inline]` not needed — single-call thin delegate with no
/// other code).
///
/// **Why not a method on a per-phase trait?** A trait method bound
/// `T: HasDispatchFields` would re-introduce the by-discipline gap
/// (any future phase could `impl HasDispatchFields` and reach the
/// dispatch body without `unsafe`). The free function takes the
/// `DispatchContext` by value — only code that already has eight
/// disjoint `&mut` refs to the right field types can call it. The
/// phase-narrow `Inner` types lack some of these fields (e.g.
/// `DisconnectedInner` has no `state`/`read_buf`) and physically
/// cannot construct a `DispatchContext` — tier-1 closure at the
/// type level.
///
/// **Lifetimes**: `'state` covers the state `&mut` (lifted local in
/// per-phase lift+lower wrappers). `'r` covers all other `&mut`
/// refs that flow into [`OutActions<'_>`] via `materialise`.
/// Independent lifetimes enable per-phase wrappers to provide a
/// short-lived lifted `&mut ProtoState` (local owned `proto_state`)
/// alongside outer-lifetime borrows for the data fields. Returns
/// `OutActions<'w>` constrained only by `'r`, so the wrapper's
/// projection of state back to per-phase form after the call does
/// not constrain the return-borrow lifetime. Single-lifetime call
/// sites (`<ActivePhase>::feed_bytes`, `<ConnectingPhase>::feed_bytes`,
/// etc.) infer `'state = 'r` from `&mut self` and compile unchanged.
pub(in crate::protocol) struct DispatchContext<'state, 'r> {
    pub(in crate::protocol) state: &'state mut ProtoState,
    pub(in crate::protocol) read_buf: &'r mut ReadBuf,
    pub(in crate::protocol) row_desc_slot: &'r mut crate::schema_slot::RowDescSlotCell,
    /// : ParamOids slot threaded from
    /// `<ActivePhase>::Extras.param_oids` via
    /// [`feed_bytes_dispatch_active`]; the Connecting variant uses
    /// the transient `ActiveExtras.param_oids` slot (DescribeStatement
    /// is post-handshake only — same transient pattern as
    /// `row_desc_slot` for Connecting). The `'t' ParameterDescription`
    /// dispatch arm parks parsed ParamOids here via
    /// [`crate::dispatch::_param_description_dispatch_leaf::park_param_oids_at_dispatch`].
    pub(in crate::protocol) param_oids_slot:
        &'r mut crate::param_oids_slot::ParamOidsSlotCell,
    /// : CommandTag slot threaded from
    /// `<ActivePhase>::Extras.command_tag` via
    /// `feed_bytes_dispatch_active`. Connecting variant uses the
    /// transient `ActiveExtras.command_tag` slot.
    pub(in crate::protocol) command_tag_slot:
        &'r mut crate::command_tag_slot::CommandTagSlotCell,
    /// : TxStatus slot threaded from
    /// `<ActivePhase>::Extras.tx_status` via
    /// `feed_bytes_dispatch_active`. Connecting variant uses the
    /// transient `ActiveExtras.tx_status` slot. The `'Z'` dispatch
    /// arms park here; callers query via
    /// [`crate::PgProtocol::terminal_tx_status`] post-`feed_bytes`.
    pub(in crate::protocol) tx_status_slot:
        &'r mut crate::tx_status_slot::TxStatusSlotCell,
    /// .b: FailCause slot threaded from
    /// `<ActivePhase>::Extras.fail_cause` via
    /// `feed_bytes_dispatch_active`, OR from
    /// `ConnectingInner.fail_cause` via
    /// `feed_bytes_dispatch_connecting` (NOT a transient — Connecting
    /// fail_cause must persist across the wrapper return so callers
    /// can query `pg.fail_cause()` after the `FailReply` event).
    /// `install_errored` parks here; callers query via
    /// [`crate::PgProtocol::fail_cause`] (per-phase impl) post-FailReply.
    pub(in crate::protocol) fail_cause_slot:
        &'r mut crate::fail_cause_slot::FailCauseSlotCell,
    pub(in crate::protocol) session_params:
        &'r mut crate::session_params_slot::SessionParamsCell,
    pub(in crate::protocol) error_arena:
        &'r mut Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
    /// : lazy-allocated arena for `NotificationResponse`
    /// payloads. Threaded from `ActiveInner.notifications_arena`
    /// via [`feed_bytes_dispatch_active`]; the Connecting variant
    /// uses an empty transient slot (LISTEN/NOTIFY is post-handshake
    /// only — same transient pattern as `row_desc_slot` for
    /// Connecting). The pre-dispatch filter on `TAG_NOTIFICATION_RESPONSE`
    /// reads + writes this slot.
    pub(in crate::protocol) notifications_arena:
        &'r mut Option<alloc::boxed::Box<crate::notifications_arena::NotificationsArena>>,
    /// Notices arena threaded from `ActiveInner.notices_arena`.
    /// Connecting phase uses transient empty slot.
    pub(in crate::protocol) notices_arena:
        &'r mut Option<alloc::boxed::Box<crate::notices_arena::NoticesArena>>,
    /// COPY chunks arena threaded from `ActiveInner.copy_chunks_arena`.
    pub(in crate::protocol) copy_chunks_arena:
        &'r mut Option<alloc::boxed::Box<crate::copy_chunks_arena::CopyChunksArena>>,
    /// : intermediate command-tag arena threaded from
    /// `ActiveInner.command_tags_arena`. Connecting phase uses
    /// transient empty slot (post-handshake only — same lazy-arena
    /// mirror pattern as `notifications_arena` / `copy_chunks_arena`).
    /// Used by multi-statement dispatch arms to externalise
    /// the prior tag for `Action::IntermediateCommandComplete`.
    pub(in crate::protocol) command_tags_arena:
        &'r mut Option<alloc::boxed::Box<crate::command_tags_arena::CommandTagsArena>>,
    pub(in crate::protocol) partial_assembly:
        &'r mut crate::partial_assembly::PartialAssemblyCell,
    pub(in crate::protocol) malformed_count: &'r mut u32,
    pub(in crate::protocol) column_names:
        &'r mut Option<alloc::boxed::Box<[alloc::string::String]>>,
}

// ═════════════════════════════════════════════════════════════════════
// Branch-collapse typestate scaffolding
//
// `PgProtocol<P: SealedPhase>` is a `#[repr(transparent)]` wrapper
// over a per-phase `Inner` + a ZST `PhantomData<fn() -> P>` phase
// marker.
//
// - 4 ZST phase markers: DisconnectedPhase / ConnectingPhase /
//   ActivePhase / ClosedPhase
// - `SealedPhase` super-trait via `_sealed_phase::Sealed`
//   (field-private tuple-struct seal — downstream code cannot
//   extend the phase set)
// - `PgProtocol<P: SealedPhase = ActivePhase>` outer wrapper
// - Default phase `P = ActivePhase` keeps the call-site type name
//   `PgProtocol` resolving to the post-handshake form without
//   forcing every caller (`PgProtocol::new()`, `impl X for
//   PgProtocol`, tests, benches) to spell the parameter out.
// ═════════════════════════════════════════════════════════════════════

/// Sealed-trait seal for [`SealedPhase`]. Field-private tuple-struct
/// pattern ensures downstream code cannot extend the phase set via
/// `impl SealedPhase for MyPhase`.
pub(crate) mod _sealed_phase {
    /// Super-trait seal. Field-less marker. Implemented only for the
    /// 4 phase types defined in `mod protocol` below.
    pub trait Sealed {}
}

/// Sealed phase marker trait.
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
pub trait SealedPhase: _sealed_phase::Sealed + 'static {
    /// Per-phase Inner storage type.
    ///
    /// The associated type is a non-GAT plain assoc-type (no lifetime
    /// parameters). Each phase's `Inner` carries only the fields that
    /// phase legally touches; phases that touch no protocol state
    /// (e.g. [`DisconnectedPhase`]) use a ZST Inner with no fields
    /// other than `sync_marker: PhantomData<Cell<()>>` for `!Sync`
    /// propagation.
    ///
    /// Mapping:
    /// - [`DisconnectedPhase`]: `type Inner = DisconnectedInner`
    ///   (ZST, 0 B). A naive shape would carry `state` / `read_buf`
    ///   / `session_params` / `error_arena` / `partial_assembly` /
    ///   `backend_key` cells on a shared monolithic Inner with
    ///   null/empty values pre-Startup — that's tier-3
    ///   by-state-machine-reasoning. Instead the storage physically
    ///   does not exist on `<DisconnectedPhase>` —
    ///   tier-1-by-storage-absence.
    /// - [`ConnectingPhase`]: `type Inner = ConnectingInner` (~344 B
    ///   narrowed — no `row_desc_slot`, no `backend_key` until
    ///   `into_active`).
    /// - [`ActivePhase`]: `type Inner = ActiveInner` (~536 B full
    ///   weight; every cell reachable from at least one Active
    ///   variant).
    /// - [`ClosedPhase`]: `type Inner = ClosedInner` (16 B —
    ///   state_kind + error_arena Box only).
    ///
    /// Layout pins in `lib.rs` assert per-phase size; a regression
    /// in any Inner mapping shifts the layout and trips the
    /// `const _: () = assert!(…)` gates at compile time.
    type Inner;

    /// Per-phase outer Extras storage.
    ///
    /// Holds cells whose write surface is reachable only from a SUBSET
    /// of phases — keeping such cells out of the broader phases' Inner
    /// achieves tier-1 closure on the dispatch-arm-can't-write side.
    ///
    /// # Mapping
    ///
    /// - [`DisconnectedPhase`]: `type Extras = ()` (ZST). No schema
    ///   reachable pre-Startup.
    /// - [`ConnectingPhase`]: `type Extras = ()` (ZST). No dispatch
    ///   arm reachable from a `ConnectingState` LHS writes
    ///   [`crate::schema_slot::RowDescSlotCell`] — the field
    ///   physically does not exist on the outer for this phase.
    /// - [`ActivePhase`]: `type Extras = `[`crate::schema_slot::RowDescSlotCell`].
    ///   BindExecute SELECT install + Describe arms write the slot;
    ///   storage lives on the outer (hoisted from `ActiveInner`).
    /// - [`ClosedPhase`]: `type Extras = ()` (ZST). Closed absorbs no
    ///   input; schema state cleared at the into_closed boundary.
    ///
    /// # Tier impact
    ///
    /// `<ConnectingPhase>::Extras = ()` is the tier-1 closure: any
    /// future code that tries `&mut p.row_desc_slot` on a
    /// `PgProtocol<ConnectingPhase>` fails with E0609 "no field named
    /// `row_desc_slot`" because the field literally does not exist
    /// on the outer monomorphisation. The doctest below pins this
    /// type-level invariant.
    ///
    /// # Compile-fail probe
    ///
    /// **(i) `row_desc_slot` field does not exist on
    /// `PgProtocol<ConnectingPhase>`:**
    ///
    /// ```compile_fail
    /// use bsql_postgres_proto::protocol::{PgProtocol, ConnectingPhase};
    /// fn no_row_desc_slot_field(p: &mut PgProtocol<ConnectingPhase>) {
    ///     // E0609: no field `row_desc_slot` on type
    ///     // `&mut PgProtocol<ConnectingPhase>` — the slot was
    ///     // HOISTED off the Inner AND `<ConnectingPhase>::Extras
    ///     // = ()`, so no field by that name exists at any level.
    ///     let _ = &mut p.row_desc_slot;
    /// }
    /// ```
    ///
    /// The dual side of the probe (verifying that
    /// `<ConnectingPhase>::Extras = ()` and `<ActivePhase>::Extras =
    /// RowDescSlotCell`) is enforced by the layout pins in `lib.rs`
    /// (PgProtocol<ConnectingPhase> at 368 B, PgProtocol<ActivePhase>
    /// at 536 B). A regression in either Extras mapping would shift
    /// the layout and trip those `const _: () = assert!(…)` gates at
    /// compile time.
    type Extras;
}

/// Disconnected phase marker.
///
/// `PgProtocol<DisconnectedPhase>` represents a fresh protocol
/// instance that has not yet sent the Startup message. The legal
/// operation is `push_startup(...)`. Pushing a regular command from
/// this phase is a method-absent E0599 compile error.
#[derive(Debug, Clone, Copy)]
pub struct DisconnectedPhase;

/// SSL negotiation phase marker.
///
/// `PgProtocol<SslNegotiatingPhase>` represents the window between
/// sending the SSLRequest packet and classifying the server's 1-byte
/// response. The only legal operation is `classify_ssl_response`.
#[derive(Debug, Clone, Copy)]
pub struct SslNegotiatingPhase;

/// Connecting phase marker.
///
/// `PgProtocol<ConnectingPhase>` represents the Startup → AuthOk
/// handshake window. The legal operations are `feed_inbound` /
/// `advance_one_frame` to consume server-driven auth-flow frames.
/// Pushing a regular command from this phase is a method-absent
/// E0599 compile error.
#[derive(Debug, Clone, Copy)]
pub struct ConnectingPhase;

/// Active phase marker.
///
/// `PgProtocol<ActivePhase>` represents the post-handshake, ready
/// state. This is the **default** phase parameter — every caller
/// and impl block that writes `PgProtocol` (no explicit phase)
/// resolves to `PgProtocol<ActivePhase>`.
///
/// All command / feed / materialise methods live on
/// `impl PgProtocol<ActivePhase>`.
#[derive(Debug, Clone, Copy)]
pub struct ActivePhase;

/// Closed phase marker.
///
/// `PgProtocol<ClosedPhase>` represents a terminally-Errored
/// protocol instance. The legal operation is `cause()` accessor;
/// all push / feed paths are method-absent E0599 compile errors
/// («Errored absorbs input»).
#[derive(Debug, Clone, Copy)]
pub struct ClosedPhase;

impl _sealed_phase::Sealed for DisconnectedPhase {}
impl _sealed_phase::Sealed for SslNegotiatingPhase {}
impl _sealed_phase::Sealed for ConnectingPhase {}
impl _sealed_phase::Sealed for ActivePhase {}
impl _sealed_phase::Sealed for ClosedPhase {}

/// Process-global reply-id counter.
///
/// Shared across all four phases' `next_reply_id` mint sites.
/// Lives at module scope so `<DisconnectedPhase>` (whose ZST Inner
/// has no `next_reply_id` method to delegate to) can mint reply ids
/// via `super::PROCESS_REPLY_ID_COUNTER` and the other three phases
/// route through the same counter.
///
/// **Process-global uniqueness** is the load-bearing invariant: a
/// reply id minted on a Disconnected → push_startup path AND a
/// reply id minted on a parallel `<ActivePhase>::push_command`
/// on a different `PgProtocol` instance MUST never collide. A naive
/// per-instance or per-phase counter would not provide this.
/// `static AtomicU64` at module scope does.
pub(crate) static PROCESS_REPLY_ID_COUNTER: core::sync::atomic::AtomicU64 =
    core::sync::atomic::AtomicU64::new(0);

/// Per-phase Inner for the Disconnected phase.
///
/// **Tier-1 by storage absence**: `DisconnectedInner` carries only
/// `sync_marker: PhantomData<Cell<()>>` for `!Sync` auto-trait
/// propagation. `size_of::<DisconnectedInner>() == 0` (ZST). A
/// naive shape would carry the full set of post-handshake cells
/// (`state` / `read_buf` / `session_params` / `error_arena` /
/// `partial_assembly` / `backend_key`) and rely on the state
/// machine to leave them null/empty pre-Startup — tier-3
/// by-state-machine-reasoning. Instead the storage physically does
/// not exist on `<DisconnectedPhase>`.
// Struct visibility `pub` for the E0446 reason (associated type
// can't leak a crate-private type through the public `SealedPhase`
// trait surface). Fields are private; construction is via
// `<PgProtocol<DisconnectedPhase>>::new()` only — promoted name
// visibility adds no new external capability.
#[derive(Debug)]
#[expect(
    missing_copy_implementations,
    reason = "DisconnectedInner is a ZST whose only field (PhantomData) is Copy-eligible, but PgProtocol<P> is intentionally !Copy by design: consume-self phase transitions (push_startup/into_active/into_closed) take self by-value, and a Copy wrapper would allow the caller to retain a stale duplicate post-transition (defeating the type-level proof that the phase has moved). The PhantomData<Cell<()>> sync_marker is the !Sync witness; non-Copy preserves the consume-self lifecycle."
)]
pub struct DisconnectedInner {
    /// `!Sync` auto-trait propagation via `PhantomData<Cell<()>>`.
    /// Layout-zero, named without leading underscore (structurally
    /// load-bearing fields are not `_`-prefixed).
    sync_marker: PhantomData<core::cell::Cell<()>>,
}

/// Per-phase Inner for the Closed (terminally-Errored) phase.
///
/// **Tier-1 by storage absence**: post-Errored only `state_kind`
/// and `error_arena` are reachable through the legitimate
/// `<ClosedPhase>` API. A naive shape would keep the full 536-B
/// post-handshake cell set allocated until the protocol itself
/// dropped — architecturally dead but visible to any future API
/// addition. Instead `into_closed_if_errored` and the
/// `<ConnectingPhase>::into_active` Closed arm extract `state_kind`
/// (Copy from the Errored arm) + `mem::take` the error_arena Box;
/// the rest Drops at the transition boundary, releasing ~520 B of
/// stack + any heap behind the Box-niche cells.
///
/// `size_of::<ClosedInner>() == 16 B` (state_kind 1B + 7B pad +
/// error_arena Option<Box> 8B). `DisconnectedInner` is 0 B;
/// `ClosedInner` is the second-narrowest phase Inner.
/// Why a `<ClosedPhase>` protocol is in its terminal state.
///
/// Two paths reach `<ClosedPhase>`:
///
/// - **[`Self::Errored`]** — a transport / framing / SCRAM / server-error
///   classifier flagged the connection unrecoverable. The wrapper layer
///   typically logs the error and discards the connection. The carried
///   [`crate::error::StateErrorKind`] preserves the original cause
///   classifier across the typestate transition (the full
///   `ProtocolError` cause was already delivered via the matching
///   `FailReply` action; only the kind classifier is retained here).
///
/// - **[`Self::GracefulTerminate`]** — the client explicitly sent the
///   `'X'` Terminate frame via [`PgProtocol::<ActivePhase>::terminate`].
///   No error occurred; the connection is closing cleanly. The wrapper
///   layer typically flushes the trailing bytes to the socket and drops
///   the TCP connection.
///
/// # Size
///
/// 2 B exact (`#[repr(u8)]` discriminant + max-variant `StateErrorKind`
/// 1 B). Fits inside `ClosedInner`'s alignment padding — no size growth
/// vs the prior `state_kind: StateErrorKind` field shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CloseCause {
    /// The connection closed due to a tier-1-classified error
    /// (transport / framing / SCRAM / server-error / etc.).
    /// The carried [`crate::error::StateErrorKind`] is the
    /// kind classifier extracted from the upstream `Errored(state_kind)`
    /// transition boundary.
    Errored(crate::error::StateErrorKind),
    /// The connection closed gracefully via client-initiated
    /// [`PgProtocol::<ActivePhase>::terminate`] (`'X'` frame). No
    /// error occurred. The post-terminate `<ClosedPhase>` protocol
    /// carries no error payload to inspect.
    GracefulTerminate,
}

/// Per-phase Inner for the [`ClosedPhase`] terminally-closed phase.
///
/// Carries only the [`CloseCause`] discriminator (1 B + alignment)
/// and the preserved server-error arena (8 B Box-niche). No
/// `ProtoState`, no read/write buffers, no auth state — every other
/// field is dropped at the transition boundary
/// (`into_closed_if_errored`, `<ConnectingPhase>::into_active` Closed
/// arm, or `<ActivePhase>::terminate`), releasing ~488 B of stack
/// plus any Box behind the cell niches.
///
/// `size_of::<ClosedInner>() == 16 B` (sync_marker 0 B; cause 2 B;
/// alignment pad 6 B; error_arena `Option<Box>` 8 B). Second-
/// narrowest phase Inner (after `DisconnectedInner` at 0 B).
#[derive(Debug)]
pub struct ClosedInner {
    /// `!Sync` auto-trait propagation.
    sync_marker: PhantomData<core::cell::Cell<()>>,
    /// Terminal cause classifier — extracted from the transition
    /// boundary (`Errored(state_kind)` for the error path, or
    /// [`CloseCause::GracefulTerminate`] for the client-initiated
    /// terminate path). The full `ProtoState` enum (48 B post-)
    /// is not retained: `<ClosedPhase>` has no further state
    /// transitions, so the discriminator suffices.
    cause: CloseCause,
    /// Server-error arena handle — preserved across the transition
    /// for follow-up `ErrorRef → ErrorPayload` lookups (the wrapper
    /// layer may stash an `ErrorRef` from a `ServerErrorResponse`
    /// classified during `<ActivePhase>` / `<ConnectingPhase>`).
    /// `None` if no server error was buffered before the transition.
    /// Box drops on `<ClosedPhase>` Drop, releasing the arena heap
    /// (typically ~290 B if populated).
    error_arena: Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
}

impl SealedPhase for DisconnectedPhase {
    type Inner = DisconnectedInner;
    type Extras = ();
}
impl SealedPhase for SslNegotiatingPhase {
    type Inner = DisconnectedInner;
    type Extras = ();
}
impl SealedPhase for ConnectingPhase {
    // Tier-1 closure on state-variant-in-wrong-phase by storage
    // absence (`ConnectingInner.state: ConnectingState` — no Active
    // variants exist in the per-phase enum).
    //
    // `row_desc_slot` lives off the Inner —
    // `<ConnectingPhase>::Extras = ()` (storage-absence at the
    // outer). The `feed_bytes_dispatch_connecting` wrapper mints a
    // stack-local transient RowDescSlotCell that the shared
    // dispatch body can name; the transient is empty (Connecting
    // LHS arms never write it), and drops with the wrapper's frame.
    // Tier-1 closure on outer-level-can't-write-schema by storage
    // absence.
    type Inner = ConnectingInner;
    type Extras = ();
}
impl SealedPhase for ActivePhase {
    // Tier-1 closure on state-variant-in-wrong-phase by storage
    // absence (`ActiveInner.state: ActiveState` — no Connecting
    // variants exist in the per-phase enum).
    //
    // [`ActiveExtras`] carries TWO slots on the outer for Active
    // alone: `row_desc` ([`crate::schema_slot::RowDescSlotCell`])
    // and `param_oids` ([`crate::param_oids_slot::ParamOidsSlotCell`]).
    // Both `feed_bytes_dispatch_active` and `feed_bytes_dispatch_connecting`
    // borrow `&mut self.extras.row_desc` + `&mut self.extras.param_oids`
    // and thread into the shared dispatch body's `DispatchContext`.
    // Net footprint preserved (both slots moved from Inner → outer
    // Extras for Active monomorphisation). extended this
    // pattern to `param_oids` (was inline `Box<ParamOids>` in two
    // state variants; now lives in slot, state variants carry only
    // the bare `ReplyId<DescribeStatementKind>`).
    type Inner = ActiveInner;
    type Extras = ActiveExtras;
}
impl SealedPhase for ClosedPhase {
    // The transition sites (`<ActivePhase>::into_closed_if_errored`
    // and `<ConnectingPhase>::into_active` Closed arm) materialise
    // a fresh ClosedInner via `state_kind` extract + `mem::take` of
    // the error_arena, letting the remaining outgoing-Inner fields
    // Drop at the boundary.
    type Inner = ClosedInner;
    type Extras = ();
}

// ═════════════════════════════════════════════════════════════════════
// `<ActivePhase>::Extras` carrier.
//
// Holds the TWO outer-level cells observed only during the Active
// phase:
//
//   - `row_desc`     — parked `RowDescription` payload from the inbound
//                      `'T'` frame. Lifecycle spans across one
//                      streaming-rows phase or a Describe-portal-Rows
//                      cycle; cleared at the next Idle/Errored entry.
//   - `param_oids`   — parsed `ParameterDescription` payload from the
//                      inbound `'t'` frame. Lifecycle spans across the
//                      `'t' → ('T' | 'n') → 'Z'` window of a
//                      DescribeStatement push cycle; cleared at the
//                      next Idle/Errored entry.
//
// Both slots are placed on the outer `<ActivePhase>::Extras` (not
// inside `ActiveInner`) so the `feed_bytes_dispatch_active` wrapper
// can borrow `&mut self.inner` for the state dispatch AND
// `&mut self.extras.row_desc` / `&mut self.extras.param_oids` for
// per-cell mutation in DIFFERENT borrow slots — no aliasing
// conflict with the `&mut self.inner.state` borrow that flows
// through `dispatch()`.
//
// `<ConnectingPhase>::Extras = ()`: storage-absence tier-1 closure
// on «outer-level can't write either schema or param-OIDs from
// Connecting». The `feed_bytes_dispatch_connecting` wrapper mints
// a stack-local transient [`ActiveExtras`] that the shared dispatch
// body can name; the transient is empty (Connecting LHS arms never
// write to either slot), and drops with the wrapper's frame.
//
// `#[allow]` rationale: `Copy`/`Debug` derived on `ActiveExtras`
// would defeat the per-cell concrete-token write provenance
// (mass-copying via Copy bypasses the token gate; Debug exposes
// parked schema/OID payload through `{:?}`). Both rejected at the
// trait level.
#[allow(
    missing_copy_implementations,
    missing_debug_implementations,
    reason = "`Copy` is BANNED on the carrier — copying would let \
              callers bypass the token-gated per-cell write protocol. \
              `Debug` is suppressed because the inner cells \
              (`RowDescSlotCell` / `ParamOidsSlotCell`) also suppress \
              `Debug` to protect parked schema/OID metadata from \
              accidental observation via `{:?}`."
)]
/// Carrier for the `<ActivePhase>` outer-level slots —
/// `row_desc_slot` for parked `RowDescription` payload and
/// `param_oids_slot` for parked `ParameterDescription` payload.
///
/// Stored at `PgProtocol<ActivePhase>::extras`; the field types
/// are crate-private (`pub(in crate::protocol)`) so external code
/// can name the type (required by the `SealedPhase::Extras` assoc
/// type bound) but cannot read or write the inner cells. The cells
/// expose their own token-gated `pub(crate)` API for parks/clears
/// (see `crate::schema_slot::RowDescSlotCell` and
/// `crate::param_oids_slot::ParamOidsSlotCell`).
pub struct ActiveExtras {
    pub(in crate::protocol) row_desc: crate::schema_slot::RowDescSlotCell,
    pub(in crate::protocol) param_oids:
        crate::param_oids_slot::ParamOidsSlotCell,
    /// — slot for parked `CommandComplete` payload.
    /// Per-cycle: `'C'` arrival parks the boxed
    /// [`crate::command_tag::CommandTag`]; the trailing `'Z'`
    /// materialise reads via `as_ref()` and emits
    /// `Reply::QueryComplete.command_tag: &'r CommandTag`.
    /// Cleared at Idle/Errored residue boundary.
    pub(in crate::protocol) command_tag:
        crate::command_tag_slot::CommandTagSlotCell,
    /// — slot for parked `ReadyForQuery` transaction
    /// status. Per-cycle: `'Z'` arrival parks the
    /// [`crate::action::TxStatus`]; callers query via
    /// [`crate::PgProtocol::terminal_tx_status`] AFTER consuming
    /// `Action::DeliverReply`. Reset to `TxStatus::Idle` at
    /// Idle/Errored residue boundary. Externalising tx_status
    /// strips the 1-B `pub tx_status` field from every Reply
    /// variant — collapses the 7-byte alignment tail on every
    /// 24-B-class variant (QueryComplete /
    /// DescribeStatementComplete) to zero, cascading Reply
    /// 32 → 16-24 B and Action 40 → 24-32 B.
    pub(in crate::protocol) tx_status:
        crate::tx_status_slot::TxStatusSlotCell,
    /// .b — slot for parked `Action::FailReply.cause`.
    /// Externalised so `Action::FailReply { id, cause }` (32 B body)
    /// collapses to `Action::FailReply { id }` (8 B body). With
    /// shrinking `DeliverReply` body 24 → 16 B in parallel,
    /// Action floor drops 40 → 24 B (-40%).
    ///
    /// Per-cycle lifecycle: `dispatch::install_errored` parks
    /// `Box<ProtocolError>`; callers query via
    /// [`crate::PgProtocol::fail_cause`] AFTER consuming
    /// `Action::FailReply`. Cleared at Idle entry; NOT cleared at
    /// Errored entry (slot persists across Errored cycle).
    pub(in crate::protocol) fail_cause:
        crate::fail_cause_slot::FailCauseSlotCell,
    /// Column names from the most recent RowDescription frame.
    /// Populated alongside `row_desc` during T-frame dispatch;
    /// cleared at Idle/Errored residue boundary.
    pub(in crate::protocol) column_names:
        Option<alloc::boxed::Box<[alloc::string::String]>>,
}

/// Phase-typed wrapper over the per-phase Inner storage.
///
/// `#[repr(transparent)]` over the per-phase `Inner` field plus a
/// ZST [`PhantomData<fn() -> P>`]. The `fn() -> P` phantom shape
/// gives covariant `P` + unconditional `Send + Sync` of the phantom
/// itself (the wrapper's `!Sync` inherits from
/// `inner.sync_marker: PhantomData<Cell<()>>` via `repr(transparent)`
/// auto-trait propagation).
///
/// **No default `P`** — every usage must spell
/// out the phase parameter explicitly: `PgProtocol<ActivePhase>`,
/// `PgProtocol<ConnectingPhase>`, etc. The pre-default
/// `P = ActivePhase` hid the phase at call sites, making the
/// typestate discipline implicit. Removing the default forces
/// every type-position to be phase-aware — stronger documentation
/// of the phase topology in the type system.
///
/// # Field-access discipline
///
/// Methods inside `impl PgProtocol<P>` access inner fields via
/// **explicit `self.inner.<field>`** — there is no [`Deref`] /
/// [`DerefMut`] impl. The explicit projection is load-bearing for
/// the multi-phase foundation:
///
/// 1. **Phase transitions** (`fn into_connecting(self) -> PgProtocol<ConnectingPhase>`)
///    move `self.inner` into the new wrapper — the boundary is
///    visible at the call site, not hidden by deref coercion.
/// 2. **Phase-conditional methods** read `self.inner.<field>` —
///    uniform access pattern across all phase impls regardless of
///    which inner fields the phase touches.
/// 3. **Future inner-state evolution** — a new field on a per-phase
///    Inner is accessible via the same `self.inner.<new_field>`
///    pattern; deref-based access would need additional method
///    shadowing discipline to handle phase-conditional inner fields.
///
/// The `inner` field is **module-private** (no visibility modifier),
/// not `pub(crate)`. Sibling modules (`dispatch.rs`, `row_stream.rs`,
/// etc.) access `PgProtocol<P>` exclusively via the public method
/// surface — the inner-data shape stays an internal detail of
/// `mod protocol` (and its leaf submodules per Rust submodule
/// visibility rules).
#[repr(C)]
pub struct PgProtocol<P: SealedPhase> {
    // Per-phase `Inner` storage via the associated type
    // `<P as SealedPhase>::Inner`. Each monomorphisation has a
    // concrete layout:
    //   PgProtocol<DisconnectedPhase> ≡ DisconnectedInner (ZST, 0 B)
    //                                 + Extras = () (ZST) → 0 B
    //   PgProtocol<ConnectingPhase>   ≡ ConnectingInner (narrowed, no slot)
    //                                 + Extras = () (ZST)
    //   PgProtocol<ActivePhase>       ≡ ActiveInner (narrowed, no slot)
    //                                 + Extras = RowDescSlotCell (144 B)
    //   PgProtocol<ClosedPhase>       ≡ ClosedInner (16 B)
    //                                 + Extras = () (ZST) → 16 B
    inner: <P as SealedPhase>::Inner,
    /// Per-phase outer Extras storage. Hoisted from `Inner` for
    /// cells whose write surface is phase-restricted; allows tier-1
    /// closure on `<P>` monomorphisations that semantically reject
    /// the cell (e.g. `<ConnectingPhase>::Extras = ()` → no
    /// `row_desc_slot` reachable on the outer). See
    /// [`SealedPhase::Extras`].
    extras: <P as SealedPhase>::Extras,
    /// ZST phase marker. Load-bearing for the type-level phase
    /// proof; named without leading-underscore (structurally-used
    /// fields must not be `_`-prefixed).
    phase_marker: PhantomData<fn() -> P>,
}

// No `#[cfg(feature = "bench-hooks")]` raw-state hooks ship from
// this crate. A naive shape would expose `bench_append_read_buf`
// (raw append into `read_buf` bypassing dispatch) and
// `reset_for_bench` (snap state to Idle bypassing Drop) under a
// feature flag — tier-3 by-discipline closure since a downstream
// consumer could enable the feature in their Cargo.toml and reach
// the API in production. CREDO §1 demands tier-1 closure.
//
// Benches instead use the public surface:
//
// 1. `feed_inbound(bytes) -> Result<(), ReadBufFull>` covers the
//    raw-append role byte-for-byte — no duplication needed.
//
// 2. criterion's `iter_batched(setup, routine, BatchSize)` covers
//    the reset role: setup builds a fresh proto per iter
//    (untimed), routine runs the timed measurement on it. Per-iter
//    setup pays `PgProtocol::new()` init (~50 ns memset for 4 KB
//    ReadBuf) but that cost is OUTSIDE the timed window —
//    criterion reports the routine timing accurately. See
//    `benches/hot_paths.rs` for patterns.
//
// Tier closure: the feature physically does not exist → no leak
// surface → tier-1 by-elimination. CREDO §1 absolute-safety
// satisfied without discipline reliance. Trade-off: amortised push
// benches include per-iter `PgProtocol::new()` cost in their wall
// time (longer to reach criterion's sample budget) but the
// reported per-iter timing is correct; relative-to-baseline
// regression detection is preserved.

// ═════════════════════════════════════════════════════════════════════
// Schema-side concrete-token leaves
//
// Each leaf has a CONCRETE token type (`pub(crate) struct
// XToken(())`); the `()` field is private to the leaf submodule, so
// `XToken(())` literal is mintable ONLY inside the leaf. The
// `RowDescSlotCell::*_at_*` write methods take the concrete token
// type by value. There is no trait to `impl` for hostile types;
// bypass requires constructing a token (impossible outside the
// leaf) or a type-mismatched parameter (rejected by Rust's type
// system).
//
// A naive shape would use a sealed-trait + auth-tag pattern (`impl
// Sealed for Token` + `impl SchemaWriteAuth for Token` + a
// `from_field_with_auth` constructor) — tier-1 EXTERNAL but tier-2
// by-discipline WITHIN-CRATE: any in-crate file could `impl Sealed
// for HostileTag` + `impl SchemaWriteAuth for HostileTag` and
// bypass the constructor via the hostile tag.
//
// Cost: visibility-only; LLVM erases everything; 0 ns / 0 B.
//
// ─────────────────────────────────────────────────────────────────────
// By-value `_token: TokenType` parameters kept, by-ref
// `token: &TokenType` rejected.
//
// A naive migration to `token: &TokenType` (by-ref) — motivated by
// "drop the underscore prefix; non-underscore name is fine with
// `&T`" — would weaken tier-1:
//
//   - Every leaf token EXCEPT `_proto_init_leaf::ProtoInitToken` is a
//     non-Copy ZST (no `#[derive(Clone, Copy)]`). Today, by-value
//     `_token: TokenType` parameters give compile-checked single-use
//     enforcement — a refactor that double-passes the token to two
//     different cell methods fails the move-checker.
//   - Switching to `token: &TokenType` erases that single-use shield:
//     `&token` can be passed multiple times. A future bug where a
//     leaf helper accidentally calls two state-mutating methods with
//     the same token would compile cleanly.
//   - The audit's stated win is purely aesthetic ("drop the `_`
//     prefix"). The `_` prefix is the canonical Rust idiom for
//     "intentionally unused function parameter" — not a code smell.
//   - For `ProtoInitToken` (Copy ZST, used by `PgProtocol::new` to
//     construct ~5 cells in one go), by-value and by-ref are
//     equivalent at the call site (Copy makes single-use trivially
//     achievable both ways).
//
// Conclusion: the by-value `_token: TokenType` pattern is retained
// across all 14 production sites (`session_params_slot.rs` × 4,
// `schema_slot.rs` × 4, `partial_assembly.rs` × 5, `cancel.rs` × 1).
// The 2 macro-pattern `($_t:ident)` sites (params.rs:318/326) similarly
// retained — `_t` is the unused-pattern-binding signal. Tier-1 single-
// use proof beats stylistic cleanup.
// ─────────────────────────────────────────────────────────────────────
// ═════════════════════════════════════════════════════════════════════

/// Leaf submodule for the BindExecute SELECT install transition.
/// Hosts the [`BeSelectToken`] type and the single helper fn that
/// mints+writes inline.
pub(crate) mod _bind_execute_select_install_leaf {
    /// Leaf-scope token. The tuple-struct field is PRIVATE to this
    /// submodule — `Self(())` mints are callable ONLY here. The
    /// type itself is `pub(crate)` so
    /// [`crate::schema_slot::RowDescSlotCell::park_at_be_select`]
    /// can name it in its parameter signature; naming alone confers
    /// no minting power.
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

/// Leaf submodule for the clear-session-residue transitions on
/// Idle/Errored entry. Hosts three concrete-type tokens (one per
/// slot kind) and three helper fns — schema-side, session_params-
/// side, and partial-assembly-side.
pub(crate) mod _clear_residue_leaf {
    /// Leaf-scope token for the schema slot clear. Field private to
    /// the leaf; type `pub(crate)` so the cell can name it in its
    /// method signature.
    pub(crate) struct ClearResidueSchemaToken(());

    /// Leaf-scope token for the session_params slot clear. Field
    /// private to the leaf; type `pub(crate)` so the cell can name
    /// it.
    pub(crate) struct ClearResidueSessionToken(());

    /// Leaf-scope token for the partial-assembly slot clear at
    /// residue transitions. Field private to the leaf; type
    /// `pub(crate)` so [`crate::partial_assembly::PartialAssemblyCell`]
    /// can name it.
    pub(crate) struct ClearResiduePartialAssemblyToken(());

    /// Leaf-scope token for the param-oids slot clear at residue
    /// transitions. Field private to the leaf; type `pub(crate)` so
    /// [`crate::param_oids_slot::ParamOidsSlotCell`] can name it in
    /// its `clear_at_residue` parameter signature. —
    /// mirrors [`ClearResidueSchemaToken`]'s shape exactly.
    pub(crate) struct ClearResidueParamOidsToken(());

    /// Leaf-scope token for the command_tag slot clear at residue
    /// transitions. — mirror of
    /// [`ClearResidueParamOidsToken`]. Type `pub(crate)` so
    /// [`crate::command_tag_slot::CommandTagSlotCell`] can name it.
    pub(crate) struct ClearResidueCommandTagToken(());

    /// Leaf-scope token for the tx_status slot reset at residue
    /// transitions. — mirror of
    /// [`ClearResidueCommandTagToken`]. Type `pub(crate)` so
    /// [`crate::tx_status_slot::TxStatusSlotCell`] can name it.
    pub(crate) struct ClearResidueTxStatusToken(());

    // perf-recovery: `ClearResidueFailCauseToken` and
    // `clear_fail_cause_slot_residue` (sister to the other slot
    // residue-clear helpers) are intentionally absent. The fail_cause
    // slot is never cleared by the dispatch path — it is empty by
    // construction whenever the dispatch enters its Idle arm (see
    // `clear_session_residue_for_class_dispatch` docstring + the
    // `into_active` slot-initialisation note). The slot's Drop is the
    // sole cleanup path; happens when ActiveExtras drops at
    // `into_closed_if_errored` or at wrapper Drop.

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
    /// `clear_session_residue_for_class` Errored arm — session-state
    /// forfeit on tear-down; the params' Drop chain scrubs
    /// `SecretBoundedStr` bytes.
    #[inline]
    pub(in crate::protocol) fn clear_session_params_residue(
        cell: &mut crate::session_params_slot::SessionParamsCell,
    ) {
        cell.clear_at_residue(ClearResidueSessionToken(()));
    }

    /// Clear the partial assembly cell via
    /// [`crate::partial_assembly::PartialAssemblyCell::clear_at_residue`]
    /// with the [`ClearResiduePartialAssemblyToken`] minted inline.
    /// Used by `clear_session_residue_for_class` Idle and Errored
    /// arms — drops any in-flight assembly's Box on residue cleanup,
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

    /// Clear the param-oids slot via
    /// [`crate::param_oids_slot::ParamOidsSlotCell::clear_at_residue`]
    /// with the [`ClearResidueParamOidsToken`] minted inline. Used by
    /// `clear_session_residue_for_class` Idle and Errored arms —
    /// drops the box if a Describe-statement was in flight, freeing
    /// the 68 B heap. mirror of
    /// [`clear_schema_slot_residue`].
    #[inline]
    pub(in crate::protocol) fn clear_param_oids_slot_residue(
        slot: &mut crate::param_oids_slot::ParamOidsSlotCell,
    ) {
        slot.clear_at_residue(ClearResidueParamOidsToken(()));
    }

    /// Clear the command_tag slot via
    /// [`crate::command_tag_slot::CommandTagSlotCell::clear_at_residue`]
    /// with the [`ClearResidueCommandTagToken`] minted inline.
    /// — mirror of [`clear_param_oids_slot_residue`].
    /// Used by both Idle and Errored arms of
    /// `clear_session_residue_for_class_dispatch`.
    #[inline]
    pub(in crate::protocol) fn clear_command_tag_slot_residue(
        slot: &mut crate::command_tag_slot::CommandTagSlotCell,
    ) {
        slot.clear_at_residue(ClearResidueCommandTagToken(()));
    }

    /// Reset the tx_status slot to the conn-start default
    /// (`TxStatus::Idle`) via
    /// [`crate::tx_status_slot::TxStatusSlotCell::clear_at_residue`]
    /// with the [`ClearResidueTxStatusToken`] minted inline.     /// . Used by both Idle and Errored arms of
    /// `clear_session_residue_for_class_dispatch`.
    #[inline]
    pub(in crate::protocol) fn clear_tx_status_slot_residue(
        slot: &mut crate::tx_status_slot::TxStatusSlotCell,
    ) {
        slot.clear_at_residue(ClearResidueTxStatusToken(()));
    }

    // perf-recovery: `clear_fail_cause_slot_residue`
    // helper deleted — the dispatch never calls it (slot empty by
    // construction; see the omitted-token comment near
    // `ClearResidueTxStatusToken` above).
}

// ═════════════════════════════════════════════════════════════════════
// Partial-assembly dispatch leaf submodule
//
// Per-call-site concrete-type tokens that gate
// [`crate::partial_assembly::PartialAssemblyCell`]'s `enter_at_dispatch`
// / `absorb_at_dispatch` / `take_completed` mutating methods. The
// tuple-struct field is PRIVATE to the leaf submodule, so the
// `Self(())` literal mint is callable ONLY here.
//
// Tier-1 within-crate by-construction. The leaf body is small enough
// to review as a unit; see [`crate::partial_assembly`] for the cell
// + sink design rationale.
// ═════════════════════════════════════════════════════════════════════

/// Leaf submodule for `feed_bytes_impl`'s partial-assembly
/// transitions. Hosts three concrete-type tokens and the matching
/// helper fns.
pub(crate) mod _partial_assembly_dispatch_leaf {
    /// Leaf-scope token for **entering** partial-assembly mode.
    /// Field private to the leaf; type `pub(crate)` so
    /// [`crate::partial_assembly::PartialAssemblyCell::enter_at_dispatch`]
    /// can name it in its parameter signature.
    pub(crate) struct PartialAssemblyEnterToken(());

    /// Leaf-scope token for **absorbing** body bytes into an active
    /// partial-assembly. Field private to the leaf.
    pub(crate) struct PartialAssemblyAbsorbToken(());

    /// Leaf-scope token for **taking** a completed partial assembly
    /// out of the cell for dispatch. Field private to the leaf.
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
    /// Returns the `(consumed, leftover)` pair pre-split from
    /// `bytes`: callers receive the post-split tail directly, no
    /// downstream `bytes.get(N..).unwrap_or(&[])` dead-arm pattern.
    #[inline]
    pub(in crate::protocol) fn absorb_partial_assembly_at_dispatch<'b>(
        cell: &mut crate::partial_assembly::PartialAssemblyCell,
        bytes: &'b [u8],
    ) -> (&'b [u8], &'b [u8]) {
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

/// Leaf submodule for the inbound `ParameterStatus` pre-dispatch
/// filter. Hosts the [`ParamStatusToken`] type and the single admit
/// helper fn that delegates to the cell's parse+record method.
pub(crate) mod _parameter_status_admit_leaf {
    /// Leaf-scope token. Field private to the leaf; type
    /// `pub(crate)` so
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

/// Leaf submodule for the inbound `NoticeResponse` pre-dispatch
/// filter. Hosts the [`NoticeResponseToken`] type and the single
/// admit helper fn.
pub(crate) mod _notice_response_admit_leaf {
    /// Leaf-scope token. Field private to the leaf; type
    /// `pub(crate)` so
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
// `_notification_response_admit_leaf` submodule — NOTIFY ('A')
// pre-dispatch parsing + arena allocation.
//
// Mirror of `_notice_response_admit_leaf` (sibling 'N' tag) but writes
// to the notifications_arena rather than the session_params NoticeResponse
// counter.
// ═════════════════════════════════════════════════════════════════════

pub(in crate::protocol) mod _notification_response_admit_leaf {
    use alloc::vec::Vec;

    /// Parsed view of a NotificationResponse frame body.
    ///
    /// Borrows from `payload` (the dispatch loop's `&[u8]` view into
    /// `read_buf.populated()`). The arena `admit` step copies the
    /// channel + payload bytes into owned storage; `parsed`'s borrow
    /// expires at the `admit` boundary.
    pub(in crate::protocol) struct ParsedNotification<'a> {
        pub(in crate::protocol) pid: i32,
        pub(in crate::protocol) channel: &'a [u8],
        pub(in crate::protocol) payload_bytes: &'a [u8],
    }

    /// Parse the NotificationResponse body per PG §55.7:
    /// `pid: int32 BE` + `channel: CSTR` + `payload: CSTR`.
    ///
    /// Returns `None` if:
    /// - body shorter than 4 bytes (no pid)
    /// - channel CSTR has no nul terminator
    /// - payload CSTR has no nul terminator
    /// - bytes remain after the payload CSTR's nul terminator
    ///   (extra trailing bytes — malformed body)
    ///
    /// The None case classifies as a malformed-body silent discard —
    /// mirror of `admit_parameter_status_frame`'s `MalformedPayload`
    /// policy. Logging this would surface adversarial / buggy server
    /// frames; for v1 the discard is acceptable.
    #[inline]
    pub(in crate::protocol) fn parse_notification_payload(
        body: &[u8],
    ) -> Option<ParsedNotification<'_>> {
        let (pid_bytes, rest) = body.split_first_chunk::<4>()?;
        let pid = i32::from_be_bytes(*pid_bytes);
        let nul_channel = rest.iter().position(|&b| b == 0)?;
        let (channel, rest_after_channel_nul) = rest.split_at_checked(nul_channel)?;
        let after_channel = rest_after_channel_nul.get(1..)?;
        let nul_payload = after_channel.iter().position(|&b| b == 0)?;
        let (payload_bytes, trailer) = after_channel.split_at_checked(nul_payload)?;
        let trailer_after_nul = trailer.get(1..)?;
        if !trailer_after_nul.is_empty() {
            return None;
        }
        Some(ParsedNotification {
            pid,
            channel,
            payload_bytes,
        })
    }

    /// Lazy-init the arena (one Box per LISTEN-using connection),
    /// validate channel as PG identifier, allocate a `NotificationPayload`
    /// slot, return the gen-tagged ref.
    ///
    /// Returns `None` when:
    /// - channel bytes fail `Ident::try_from_bytes` — non-UTF-8 or
    ///   exceeds NAMEDATALEN-1 chars (PG spec says channel is an
    ///   identifier; server emitting a non-identifier channel is a
    ///   spec violation)
    /// - arena cap reached (`MAX_NOTIFICATIONS_PER_CALL` slots
    ///   already used in this cycle — structurally bounded by
    ///   OutActions cap)
    #[inline]
    pub(in crate::protocol) fn admit_notification_frame(
        slot: &mut Option<alloc::boxed::Box<crate::notifications_arena::NotificationsArena>>,
        pid: i32,
        channel_bytes: &[u8],
        payload_bytes: &[u8],
    ) -> Option<crate::notifications_arena::NotificationRef> {
        let channel_str = core::str::from_utf8(channel_bytes).ok()?;
        let channel = crate::ident::Ident::try_from_str(channel_str).ok()?;
        let arena = slot.get_or_insert_with(|| {
            alloc::boxed::Box::new(crate::notifications_arena::NotificationsArena::new())
        });
        let payload = crate::notifications_arena::NotificationPayload {
            pid,
            channel,
            payload: Vec::from(payload_bytes),
        };
        arena.alloc(payload)
    }
}

// ═════════════════════════════════════════════════════════════════════
// `_proto_init_leaf` submodule — sole legitimate cell-construction
// site.
//
// Cells expose `pub(crate) const fn empty(token: ProtoInitToken)`;
// `ProtoInitToken` has a private tuple-struct field — `Self(())`
// is mintable ONLY inside this submodule. `PgProtocol::new` lives
// INSIDE `_proto_init_leaf` so it has access to
// `ProtoInitToken::mint()`. Code outside the leaf cannot mint
// tokens → cannot construct fresh cells → cannot wholesale-replace
// `*pg.row_desc_slot = …` (no fresh value to assign).
//
// A naive shape would expose `pub(crate) const EMPTY: Cell` on each
// cell — callable from any in-crate file, leaving wholesale
// replacement gated only by `pub(crate)`. The narrower
// `pub(in crate::protocol) const EMPTY` is invalid (E0742:
// visibility path must be ancestor; cell modules are siblings of
// mod protocol, not children).
//
// Wholesale-replacement is gated to this submodule by construction.
// Tier-1 within-crate. The leaf body is the entire init logic —
// small enough to review as a unit.
// ═════════════════════════════════════════════════════════════════════

pub(crate) mod _proto_init_leaf {
    /// Init-leaf closure token. Field private to leaf —
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
        /// Construct a new protocol typed `PgProtocol<DisconnectedPhase>`
        /// — the only legal next step is [`Self::push_startup`].
        ///
        /// `<DisconnectedPhase>::Inner` is a ZST
        /// [`super::DisconnectedInner`]. Constructor allocates ZERO
        /// bytes — `size_of::<PgProtocol<DisconnectedPhase>>() == 0`.
        /// Cell materialisation is deferred to
        /// [`super::_proto_init_leaf::fresh_connecting_inner`] called
        /// inside `push_startup`. A naive shape would materialise a
        /// full 536-B Inner at construction time (state Idle,
        /// read_buf empty, four cells, counter, sync marker), all of
        /// which would be architecturally dead until
        /// [`Self::push_startup`] consumed self.
        ///
        /// The only path into a connecting protocol is
        /// `<DisconnectedPhase>::push_startup(...) ->
        /// PgProtocol<ConnectingPhase>` (consume-self transition).
        /// Pushing any other command from `<DisconnectedPhase>` is a
        /// method-absent E0599 — the per-command structs
        /// (`Ping`, `SimpleQuery`, `Parse`, …) implement
        /// [`crate::push_command::PushCommand`] which is reachable
        /// only through `<ActivePhase>::push_command_internal`.
        ///
        /// `<DisconnectedPhase>` has no cells, so the
        /// [`ProtoInitToken`] is not needed at this constructor —
        /// only `fresh_connecting_inner` (called when `push_startup`
        /// materialises a fresh `ConnectingInner` for the
        /// `<ConnectingPhase>` transition) needs the token. The
        /// leaf-private mint stays inside `_proto_init_leaf`.
        #[must_use]
        pub const fn new() -> Self {
            Self {
                inner: super::DisconnectedInner {
                    sync_marker: super::PhantomData,
                },
                extras: (),
                phase_marker: super::PhantomData,
            }
        }
    }

    /// Materialise a fresh [`super::ConnectingInner`] for use by
    /// the `<DisconnectedPhase>::push_startup` transition.
    ///
    /// Cells start empty via the [`ProtoInitToken`]-gated
    /// constructors; `read_buf` starts empty;
    /// `malformed_frame_count` starts 0; `sync_marker` is
    /// `PhantomData`.
    ///
    /// **State sentinel**: `ConnectingState::Errored(Framing)` — a
    /// transient placeholder kind. The `push_startup` body
    /// IMMEDIATELY overwrites this with the appropriate
    /// `ConnectingState::Startup{Trust|Scram|Cleartext|Md5}` variant
    /// via lift+lower through the setter machinery (which operates
    /// on `&mut ProtoState`). The sentinel is unobservable: no
    /// reader sees the state between this constructor and the
    /// setter write. `Framing` kind is semantically meaningless
    /// here — the slot is always overwritten.
    #[must_use]
    pub(in crate::protocol) fn fresh_connecting_inner() -> super::ConnectingInner {
        let token = ProtoInitToken::mint();
        super::ConnectingInner {
            state: crate::state::ConnectingState::Errored(
                crate::error::StateErrorKind::from_kind_or_internal(
                    crate::error::ErrorKind::Framing,
                ),
            ),
            read_buf: super::ReadBuf::new(),
            session_params: crate::session_params_slot::SessionParamsCell::empty(token),
            error_arena: None,
            partial_assembly: crate::partial_assembly::PartialAssemblyCell::empty(token),
            // .b: empty fail_cause slot for the handshake
            // phase; install_errored arms park here when a handshake
            // dispatch path raises a FailReply.
            fail_cause: crate::fail_cause_slot::FailCauseSlotCell::empty(token),
            malformed_frame_count: 0,
            sync_marker: super::PhantomData,
        }
    }

    /// Mint the outer `<ActivePhase>::Extras = ActiveExtras` at the
    /// `<ConnectingPhase>::into_active` transition boundary. Both
    /// inner cells (`row_desc` + `param_oids`) start empty via
    /// [`ProtoInitToken`]-gated constructors. Mirror of the per-cell
    /// `empty(token)` mint pattern used by [`fresh_active_inner`];
    /// lives inside `_proto_init_leaf` so the [`ProtoInitToken`]
    /// stays leaf-private.
    ///
    /// : extended from single-cell `RowDescSlotCell` to
    /// the two-cell `ActiveExtras` carrier per the slot-pattern
    /// refactor that moved `param_oids: Box<ParamOids>` from state
    /// variants into a slot.
    #[must_use]
    pub(in crate::protocol) fn fresh_active_extras() -> super::ActiveExtras {
        super::ActiveExtras {
            row_desc: crate::schema_slot::RowDescSlotCell::empty(
                ProtoInitToken::mint(),
            ),
            param_oids: crate::param_oids_slot::ParamOidsSlotCell::empty(
                ProtoInitToken::mint(),
            ),
            command_tag: crate::command_tag_slot::CommandTagSlotCell::empty(
                ProtoInitToken::mint(),
            ),
            tx_status: crate::tx_status_slot::TxStatusSlotCell::fresh(
                ProtoInitToken::mint(),
            ),
            // .b: empty fail_cause slot. Installed by
            // `dispatch::install_errored` when a server-frame dispatch
            // arm fails; queried by callers via
            // `pg.fail_cause()` post-`Action::FailReply`.
            fail_cause: crate::fail_cause_slot::FailCauseSlotCell::empty(
                ProtoInitToken::mint(),
            ),
            column_names: None,
        }
    }

    /// Mint a stack-local transient [`super::ActiveExtras`] for
    /// [`super::feed_bytes_dispatch_connecting`]'s call into the
    /// shared dispatch body. Both inner cells start empty (no
    /// Connecting LHS arm writes either) and drop with the
    /// wrapper's frame.
    ///
    /// **Tier impact**: the `<ConnectingPhase>::Extras = ()` storage
    /// absence is the load-bearing closure (no extras on the outer);
    /// this transient is a per-call wrapper-internal placeholder
    /// the shared dispatch body can name, never observable outside
    /// the wrapper frame.
    #[must_use]
    pub(in crate::protocol) fn fresh_connecting_transient_extras()
    -> super::ActiveExtras {
        super::ActiveExtras {
            row_desc: crate::schema_slot::RowDescSlotCell::empty(
                ProtoInitToken::mint(),
            ),
            param_oids: crate::param_oids_slot::ParamOidsSlotCell::empty(
                ProtoInitToken::mint(),
            ),
            command_tag: crate::command_tag_slot::CommandTagSlotCell::empty(
                ProtoInitToken::mint(),
            ),
            tx_status: crate::tx_status_slot::TxStatusSlotCell::fresh(
                ProtoInitToken::mint(),
            ),
            // .b: present in the transient for shape
            // compliance, but NOT used by the Connecting wrapper —
            // the wrapper threads the REAL
            // `ConnectingInner.fail_cause` into
            // `DispatchContext.fail_cause_slot` so the cause persists
            // across the wrapper return (callers can still query
            // `pg.fail_cause()` post-FailReply event).
            fail_cause: crate::fail_cause_slot::FailCauseSlotCell::empty(
                ProtoInitToken::mint(),
            ),
            column_names: None,
        }
    }

    /// Materialise a fresh [`super::ActiveInner`] for use by
    /// transitions into `<ActivePhase>`.
    ///
    /// **Sole production caller**:
    /// [`super::PgProtocol::<super::ConnectingPhase>::into_active`]
    /// after observing `ConnectingState::HandshakeReady`. The
    /// fields are populated by destructuring the consumed
    /// `ConnectingInner` (field-by-field move) — see
    /// `into_active`'s body. This constructor is used only when a
    /// caller needs a from-scratch ActiveInner (e.g.
    /// `#[cfg(test)] fresh_active_proto` for residue tests).
    ///
    /// **State sentinel**: `ActiveState::Idle` — the natural
    /// post-handshake state. Caller may overwrite if needed.
    ///
    #[must_use]
    #[allow(
        dead_code,
        reason = "Production callers (`<ConnectingPhase>::into_active`) are pending wiring; the helper is exercised today by `#[cfg(test)]` sibling tests within `mod protocol`, but the lib-only build sees no production caller — keep the allow until the per-phase Inner transitions wire it in."
    )]
    pub(in crate::protocol) fn fresh_active_inner() -> super::ActiveInner {
        let token = ProtoInitToken::mint();
        super::ActiveInner {
            state: crate::state::ActiveState::Idle,
            read_buf: super::ReadBuf::new(),
            session_params: crate::session_params_slot::SessionParamsCell::empty(token),
            error_arena: None,
            notifications_arena: None,
            notices_arena: None,
            copy_chunks_arena: None,
            command_tags_arena: None,
            partial_assembly: crate::partial_assembly::PartialAssemblyCell::empty(token),
            // Placeholder backend_key for test-only fixture.
            // Production construction goes through
            // `<ConnectingPhase>::into_active` which extracts a real
            // `(pid, secret_key)` from `ConnectingState::HandshakeReady`'s
            // payload. The placeholder values here are inert (pid=0
            // is not a wire-valid PID; the secret_key Sensitive
            // payload Drop fires on test scope end).
            backend_key: crate::cancel::BackendKey {
                pid: 0,
                secret_key: crate::sensitive::Sensitive::new(0_i32),
            },
            malformed_frame_count: 0,
            sync_marker: super::PhantomData,
        }
    }
}

// ═════════════════════════════════════════════════════════════════════
// Per-phase transition surfaces
//
// `<DisconnectedPhase>::push_startup`           consume-self → ConnectingPhase
// `<ConnectingPhase>::feed_inbound`             1-line delegate to Inner
// `<ConnectingPhase>::feed_bytes`               1-line delegate to Inner
// `<ConnectingPhase>::advance_one_frame`        1-line delegate to Inner
// `<ConnectingPhase>::into_active`              consume-self → ActivePhase | IntoActiveError
// `<ClosedPhase>::cause`                        accessor — reconstructed ProtocolError
// `<ActivePhase>::into_closed_if_errored`       consume-self → ClosedPhase | ActivePhase (declared above)
//
// Tier elevations:
//   #1: push-before-Startup            → method-absent E0599 on <DisconnectedPhase>::push_*
//   #2: push-during-Connecting         → method-absent E0599 on <ConnectingPhase>::push_*
//   #3: Closed absorbs no input        → method-absent E0599 on <ClosedPhase>::feed_*/push_*
//   #4: feed_inbound surfaces typed err → Result<(), ProtocolError> across all phases that have feed_inbound
// ═════════════════════════════════════════════════════════════════════

/// Error returned by [`PgProtocol::<ConnectingPhase>::into_active`].
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
    reason = "Both variants carry PgProtocol wrappers with phase-typed markers; \
              Debug is implemented blanket-style on `PgProtocol<P>`, so emitting one for the \
              enum would either redact (defeating purpose) or print the full inner state. \
              Deferred until a concrete diagnostic surface needs the trait."
)]
#[allow(
    clippy::large_enum_variant,
    reason = "Variant size asymmetry: `StillConnecting` carries \
              `PgProtocol<ConnectingPhase>` (216 B) while `Closed` is 16 B. \
              Box-wrapping penalises the hot recovery path. The asymmetry is by-product of \
              per-phase Inner shape, not a design regression."
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
    /// `<DisconnectedPhase>` carries no storage (DisconnectedInner is
    /// ZST), so the diagnostic counter is always 0. Tier-1-by-storage-
    /// absence: the error_arena slot doesn't exist on
    /// `<DisconnectedPhase>::Inner`. A naive shape would read a counter
    /// field on the Inner — always 0 by state-machine reasoning on a
    /// pre-Startup wrapper, but reachable via storage.
    #[inline]
    #[must_use]
    pub fn error_arena_overwrite_count(&self) -> u16 {
        0
    }

    /// `<DisconnectedPhase>` is pre-Startup; the state is provably
    /// `Ready` by storage absence (no `inner.state` field on
    /// `DisconnectedInner`). The answer is a compile-time const — a
    /// naive shape would match on `inner.state.push_class()` and
    /// tautologically resolve to `Idle → Ready`.
    #[inline]
    #[must_use]
    pub fn connection_status(&self) -> crate::guard::ConnectionStatus {
        crate::guard::ConnectionStatus::Ready
    }

    /// `<DisconnectedPhase>::state()` always returns
    /// `&ProtoState::Idle`. The storage doesn't exist
    /// (`DisconnectedInner` is ZST) — the reference points to a
    /// promoted-static const expression with `&'static` lifetime
    /// (strictly longer-lived than a self-borrowed accessor would
    /// have been).
    #[inline]
    #[must_use]
    pub fn state(&self) -> &ProtoState {
        const IDLE_REF: &ProtoState = &ProtoState::Idle;
        IDLE_REF
    }

    /// Mint a fresh `ReplyId<K>` for the impending Startup push.
    ///
    /// On `<DisconnectedPhase>` there is no `inner.state` to
    /// classify saturation into — `DisconnectedInner` is ZST. A
    /// naive shape would route through a method on Inner that
    /// combines the counter increment with a saturation-classifier
    /// writing `Errored(StateErrorKind::ReplyIdSaturation)` into
    /// `inner.state`; here saturation is architecturally distant (a
    /// connection that exhausted u64 reply ids during the disconnect
    /// window — i.e., before the very first Startup — would have
    /// been driven by ~10^19 next_reply_id calls without ever
    /// calling push_startup, a non-physical workload).
    ///
    /// **Shared counter pin**: `PROCESS_REPLY_ID_COUNTER` is the
    /// crate-private static AtomicU64 shared with the other phases'
    /// reply-id mint sites — process-global uniqueness preserved.
    #[inline]
    pub fn next_reply_id<K: crate::reply_id::ReplyKind>(
        &mut self,
    ) -> crate::reply_id::ReplyId<K> {
        use core::sync::atomic::Ordering;
        let raw_old = PROCESS_REPLY_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Saturation classifier omitted on <DisconnectedPhase>:
        // architecturally distant + no inner.state to write Errored
        // into. The returned `NonZeroU64::MIN` sentinel from the
        // shared mint helper is matched by the next-push surface's
        // saturation guard when push_startup attempts to consume the
        // wrap-minted reply id.
        let nz = crate::reply_id::saturating_inc_to_nonzero(raw_old);
        crate::reply_id::ReplyId::from_raw(nz)
    }

    /// Initiate the PostgreSQL startup handshake.
    ///
    /// Consume-self transition: the typed `<DisconnectedPhase>` is
    /// converted into `<ConnectingPhase>` on every success path
    /// (including the structurally-distant `Idle build-failed` arm
    /// which transitions to `Errored` — observed via subsequent
    /// `advance_one_frame` → `FeedEvent::Close`, then
    /// `<ConnectingPhase>::into_active` returns
    /// `IntoActiveError::Closed`).
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
    /// - `actions: OutActions<'w>` — the StartupMessage wire
    ///   bytes (single `Action::SendBytes` chunk in `write_buf`).
    /// - `proto_connecting: PgProtocol<ConnectingPhase>` — the typed
    ///   wrapper for the handshake window.
    ///
    /// On `Err(PushFailure)` (extremely rare — startup fits 512 B cap
    /// and the build pipeline is const-asserted against the wire
    /// frame), the protocol is destroyed — `Err` carries no recovery
    /// surface. Caller logs the failure and drops the connection.
    // Argument count mirrors compute_push_startup_idle_only's
    // signature 1:1. Splitting into a struct-arg would obscure the
    // consume-self framing and force an inline destructure at every
    // callsite. The returned `Result<_, PushFailure>` carries ~80 B
    // in the Err arm (below the 128 B threshold); no
    // `result_large_err` exception needed.
    /// Emit the SSLRequest probe and transition to `SslNegotiatingPhase`.
    ///
    /// Returns the 8-byte packet and the next-phase protocol wrapper.
    /// Caller writes the bytes to the socket, reads 1 byte back,
    /// then calls `classify_ssl_response`.
    ///
    /// Consume-self: prevents double-send and blocks `push_startup`
    /// until SSL negotiation completes (method-absent on
    /// `SslNegotiatingPhase`).
    #[inline]
    #[must_use]
    pub fn push_ssl_request(
        self,
    ) -> (
        &'static [u8; 8],
        PgProtocol<SslNegotiatingPhase>,
    ) {
        (
            &crate::wire::SSL_REQUEST_WIRE_BYTES,
            PgProtocol {
                inner: DisconnectedInner {
                    sync_marker: core::marker::PhantomData,
                },
                extras: (),
                phase_marker: core::marker::PhantomData,
            },
        )
    }

    /// Emit the `StartupMessage` frame and transition to `ConnectingPhase`.
    pub fn push_startup<'w>(
        self,
        user: crate::ident::Ident,
        database: Option<crate::ident::DatabaseName>,
        app_name: Option<crate::ident::ApplicationName>,
        credentials: crate::password::Credentials,
        reply: crate::reply_id::ReplyId<crate::reply_id::StartupKind>,
        write_buf: &'w mut WriteBuf,
    ) -> Result<
        (
            crate::action::OutActions<'w>,
            PgProtocol<ConnectingPhase>,
        ),
        crate::action::PushFailure,
    > {
        // `self.inner` is the ZST `DisconnectedInner` — no protocol
        // storage pre-Startup. Materialise a fresh `ConnectingInner`
        // here (the post-transition per-phase storage). The setter
        // machinery (`IdleState::try_from` +
        // `idle.into_setter::<StartupPostInstall>()`) operates on
        // `&mut ProtoState`, so we lift+lower: a local
        // `proto_state = ProtoState::Idle` provides the setter target;
        // after the setter writes one of the
        // `ConnectingStartup{Trust|Scram|Cleartext|Md5}` variants, we
        // lower the result back to `ConnectingState` and assign it to
        // `new_inner.state` before completing the transition.
        //
        // `self` is consumed by-value per fn signature (ZST drop is
        // trivial); structural ownership guarantee — no explicit
        // discard needed.
        let mut new_inner = _proto_init_leaf::fresh_connecting_inner();

        write_buf.clear();
        // `clear_session_residue_for_class` is not invoked here: all
        // cells on a fresh ConnectingInner start empty (the method
        // would be a no-op). The method is not even implemented on
        // ConnectingInner because the only call site is push_startup
        // which constructs from `fresh_connecting_inner` and never
        // needs the residue clear.

        // Lift: local `proto_state` is the setter machinery's target
        // (it expects `&mut ProtoState`). After the setter writes one
        // of the ConnectingStartup* variants, we lower to ConnectingState
        // and install on new_inner.
        let mut proto_state: ProtoState = ProtoState::Idle;
        let state = &mut proto_state;
        let idle = match crate::state_setter::IdleState::try_from(state) {
            Some(idle) => idle,
            None => {
                // Architecturally unreachable: we just constructed
                // `proto_state = ProtoState::Idle` locally. The only
                // way IdleState::try_from(&Idle) returns None is a
                // semantic break in the IdleState newtype guard.
                // Classify defensively per CREDO §V.
                core::hint::cold_path();
                return Err(crate::action::PushFailure {
                    id: reply.consume(),
                    cause: alloc::boxed::Box::new(
                        crate::error::ProtocolError::InternalCrateBug {
                            locus: crate::error::CrateBugLocus::PushCommandInternalNonIdle,
                        },
                    ),
                });
            }
        };

        // Single-pass materialise inside branded closure. The
        // closure produces the final `Result<OutActions, PushFailure>`
        // directly — no intermediate StagedActions escape.
        let result: Result<
            crate::action::OutActions<'w>,
            crate::action::PushFailure,
        > = write_buf
            .with_branded(
                |mut wb| -> Result<crate::action::OutActions<'w>, crate::action::PushFailure> {
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
                    let mut out: crate::action::OutActions<'w> =
                        crate::action::OutActions::new();
                    for sa in staged {
                        match sa {
                            StagedAction::FailReply { id, cause } => {
                                if failure.is_none() {
                                    // : Box wrap at the
                                    // PushFailure boundary; staged
                                    // cause is still inline
                                    // (StagedAction unchanged).
                                    failure = Some(crate::action::PushFailure {
                                        id,
                                        cause: alloc::boxed::Box::new(cause),
                                    });
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
                            | StagedAction::DeliverReply(_)
                            | StagedAction::Notify { .. }
                            | StagedAction::Notice { .. }
                            | StagedAction::IntermediateCommandComplete { .. }
                            | StagedAction::CopyDataChunk { .. } => {
                                // compute_push_startup_idle_only emits
                                // only SendBytesRange (StartupMessage)
                                // + post_install. Other variants are
                                // architecturally unreachable from
                                // this push path. `Notify` is staged
                                // by the dispatch pre-filter on `'A'`
                                // tags during `feed_bytes`;
                                // `IntermediateCommandComplete` by the
                                // SimpleQueryAwaitingRfq + C arm
                                // (). Both feed-path-only.
                                // Skip silently rather than panic
                                // (CREDO §V); a future
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

        // `row_desc_slot` does not exist on ConnectingInner (hoisted
        // off; Extras = () for ConnectingPhase). No slot witness to
        // discharge here.
        match result {
            Ok(out) => {
                // Lower the lifted `proto_state` (now a
                // ConnectingStartup{Trust|Scram|Cleartext|Md5} variant
                // after the setter machinery wrote it) back to
                // ConnectingState and install on new_inner. The
                // `TryFrom` is total over the four ConnectingStartup*
                // variants — they map 1:1 to ConnectingState::Startup{
                // Trust|Scram|Cleartext|Md5}. Any other ProtoState
                // here is a setter-machinery bug.
                use crate::state::{ConnectingState, WrongPhase};
                new_inner.state = match ConnectingState::try_from(proto_state) {
                    Ok(cs) => cs,
                    Err(WrongPhase { recovered }) => {
                        core::hint::cold_path();
                        match recovered.take_inflight_reply_raw_id() {
                            Some(_) | None => {}
                        }
                        ConnectingState::Errored(
                            crate::error::StateErrorKind::from_kind_or_internal(
                                crate::error::ErrorKind::Internal,
                            ),
                        )
                    }
                };
                Ok((
                    out,
                    PgProtocol {
                        inner: new_inner,
                        extras: (),
                        phase_marker: PhantomData,
                    },
                ))
            }
            Err(f) => Err(f),
        }
    }
}

/// Result of classifying the server's SSL response byte.
#[derive(Debug)]
#[non_exhaustive]
pub enum SslClassified {
    /// Server accepted SSL ('S'). Driver performs TLS handshake,
    /// then calls `push_startup` on the returned proto.
    Accepted(PgProtocol<DisconnectedPhase>),
    /// Server refused SSL ('N'). Driver checks sslmode policy.
    Refused(PgProtocol<DisconnectedPhase>),
    /// Server sent 'E' — an ErrorResponse follows on the wire.
    ErrorIncoming(PgProtocol<DisconnectedPhase>),
    /// Protocol violation — unknown byte. Connection irrecoverable.
    InvalidByte {
        /// The offending byte.
        byte: u8,
    },
}

impl PgProtocol<SslNegotiatingPhase> {
    /// Classify the server's 1-byte SSL response and exit
    /// `SslNegotiatingPhase`.
    ///
    /// Consume-self. The returned `SslClassified` carries the
    /// typed outcome plus the next-phase protocol wrapper.
    /// sslmode policy is a driver concern — this method only
    /// classifies the wire byte.
    #[inline]
    #[must_use]
    pub fn classify_ssl_response(self, byte: u8) -> SslClassified {
        let disconnected = PgProtocol::new();
        match crate::wire::classify_ssl_response_byte(byte) {
            crate::wire::SslNegotiationOutcome::Accepted => {
                SslClassified::Accepted(disconnected)
            }
            crate::wire::SslNegotiationOutcome::Refused => {
                SslClassified::Refused(disconnected)
            }
            crate::wire::SslNegotiationOutcome::ErrorIncoming => {
                SslClassified::ErrorIncoming(disconnected)
            }
            crate::wire::SslNegotiationOutcome::InvalidByte(b) => {
                SslClassified::InvalidByte { byte: b }
            }
        }
    }
}

impl PgProtocol<ConnectingPhase> {
    /// Mint a fresh `ReplyId<K>` during the handshake window.
    ///
    /// Mirror of `<ActivePhase>::next_reply_id`. Useful for
    /// pipelined drivers that pre-mint correlators before observing
    /// `into_active()`'s classifier (typically not used during the
    /// standard handshake but available for advanced pipelined
    /// flows).
    #[inline]
    pub fn next_reply_id<K: crate::reply_id::ReplyKind>(
        &mut self,
    ) -> crate::reply_id::ReplyId<K> {
        self.inner.next_reply_id::<K>()
    }

    /// Append inbound auth-flow bytes during the startup handshake.
    ///
    /// 1-line delegate to the Inner's feed-side body. The
    /// `<ActivePhase>::feed_inbound` mirror exists on
    /// [`PgProtocol<ActivePhase>`] for the post-handshake hot path.
    /// Both phases route through the same byte path.
    pub fn feed_inbound(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), crate::error::ProtocolError> {
        self.inner.feed_inbound(bytes)
    }

    /// Per-event advance during handshake.
    ///
    /// 1-line delegate to the Inner's advance body (same body as
    /// `<ActivePhase>::advance_one_frame`). During handshake the
    /// caller drives this until either `FeedEvent::Deliver`
    /// (StartupComplete reply) arrives or `FeedEvent::Close`
    /// (Errored) terminates the connection. The public consume-self
    /// [`Self::into_active`] then classifies the outcome.
    #[must_use = "FeedEvent variants carry side-effect contracts: \
                  SendBytes/Deliver MUST be processed; Fail/Close MUST \
                  trigger socket teardown"]
    pub fn advance_one_frame<'w>(
        &mut self,
        write_buf: &'w mut WriteBuf,
    ) -> crate::action::FeedEvent<'w> {
        self.inner.advance_one_frame(write_buf)
    }

    /// Batched feed-and-dispatch during handshake.
    ///
    /// Mirror of `<ActivePhase>::feed_bytes` — useful for callers
    /// that prefer the batched OutActions surface over the per-event
    /// `advance_one_frame` loop. Same const-generic specialisation
    /// (`BOUNDED = false`).
    #[must_use = "the returned actions carry side-effects that must be executed"]
    pub fn feed_bytes<'w>(
        &mut self,
        bytes: &[u8],
        write_buf: &'w mut WriteBuf,
    ) -> OutActions<'w> {
        self.inner.feed_bytes_impl::<false>(bytes, write_buf, 0)
    }

    /// Consume-self transition from `<ConnectingPhase>` to
    /// `<ActivePhase>`.
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
    pub fn into_active(mut self) -> Result<PgProtocol<ActivePhase>, IntoActiveError> {
        use crate::state::ConnectingState;

        // Per-phase Inner means `self.inner: ConnectingInner` and
        // `self.inner.state: ConnectingState`. The Errored arm
        // matches `ConnectingState::Errored(k)` (not
        // `ProtoState::Errored`); the success arm observes
        // `ConnectingState::HandshakeReady` (the per-phase transition
        // signal — `ProtoState::Idle` mapped to HandshakeReady by the
        // dispatch wrapper's lower step when the
        // `(PostAuthHaveKey, RFQ)` arm produced post-handshake Idle).
        //
        // Closed arm materialises `ClosedInner` (~16 B) instead of
        // moving the full 504-B `ConnectingInner`. Extract state_kind
        // (Copy) + mem::take the error_arena Box; the remaining
        // ConnectingInner fields Drop at this scope's end.
        if let ConnectingState::Errored(state_kind) = &self.inner.state {
            let state_kind = *state_kind;
            let error_arena = core::mem::take(&mut self.inner.error_arena);
            return Err(IntoActiveError::Closed(PgProtocol {
                inner: ClosedInner {
                    sync_marker: PhantomData,
                    cause: CloseCause::Errored(state_kind),
                    error_arena,
                },
                extras: (),
                phase_marker: PhantomData,
            }));
        }
        // HandshakeReady carries (pid, secret_key) payload from the
        // dispatch arm at `(PostAuthHaveKey, RFQ)`. Consume the
        // payload at the phase-transition boundary and construct the
        // inline `BackendKey` on `ActiveInner` — tier-1 storage-
        // absence proof for the infallible `with_cancel_request`:
        // a `<ActivePhase>` proto cannot be constructed without a
        // valid `BackendKey`.
        //
        // `row_desc_slot` lives on outer `<ActivePhase>::Extras`;
        // `ConnectingInner` does not carry it. Mint a fresh cell via
        // `_proto_init_leaf::fresh_active_extras()` at this
        // transition boundary.
        if let ConnectingState::HandshakeReady { .. } = self.inner.state {
            let ConnectingInner {
                state,
                read_buf,
                session_params,
                error_arena,
                partial_assembly,
                fail_cause,
                malformed_frame_count,
                sync_marker: _,
            } = self.inner;
            // perf-recovery: explicitly drop the
            // Connecting-phase `fail_cause`. The slot is normally
            // None on the HandshakeReady success path; forwarding it
            // into ActiveExtras (prior .b design) was the lone
            // construction site of a non-empty `<ActivePhase>::Idle`
            // fail_cause slot. Dropping at the phase boundary
            // establishes the invariant "`<ActivePhase>` Idle state
            // → `fail_cause` slot empty" by-construction, which lets
            // the push hot path skip a 4-instruction clear at every
            // call (asm-diff confirmed). Callers wanting to inspect a
            // failed-handshake cause query `pg.fail_cause()` BEFORE
            // calling `into_active` (the `<ConnectingPhase>` accessor
            // is the canonical query site).
            drop(fail_cause);
            // `if let` guarded the variant; the destructure here is
            // architecturally infallible. Use a `let-else` form to
            // keep the match exhaustive without panic/unwrap (clippy
            // forbid-bundle bans both).
            let ConnectingState::HandshakeReady { pid, secret_key } = state else {
                // Architecturally dead per the outer `if let` proof.
                // Falls back to an Internal-classified Closed wrapper
                // — keeps the return type honest without panicking.
                return Err(IntoActiveError::Closed(PgProtocol {
                    inner: ClosedInner {
                        sync_marker: PhantomData,
                        cause: CloseCause::Errored(
                            crate::error::StateErrorKind::from_kind_or_internal(
                                crate::error::ErrorKind::Internal,
                            ),
                        ),
                        error_arena: None,
                    },
                    extras: (),
                    phase_marker: PhantomData,
                }));
            };
            // ActiveExtras initialised with empty `fail_cause`
            // (the explicit drop above is the invariant-establishing
            // step). `fresh_active_extras()` constructs `fail_cause`
            // as `FailCauseSlotCell::empty(token)` — the slot starts
            // empty in `<ActivePhase>` and stays empty until a
            // feed-bytes path with `materialise(...) → install_errored`
            // parks a cause (which simultaneously transitions state
            // to `Errored`; state can never go Errored → Idle, so the
            // slot remains empty whenever state is Idle).
            let extras = _proto_init_leaf::fresh_active_extras();
            return Ok(PgProtocol {
                inner: ActiveInner {
                    state: crate::state::ActiveState::Idle,
                    read_buf,
                    session_params,
                    error_arena,
                    notifications_arena: None,
                    notices_arena: None,
                    copy_chunks_arena: None,
                    command_tags_arena: None,
                    partial_assembly,
                    backend_key: crate::cancel::BackendKey { pid, secret_key },
                    malformed_frame_count,
                    sync_marker: PhantomData,
                },
                extras,
                phase_marker: PhantomData,
            });
        }
        Err(IntoActiveError::StillConnecting(self))
    }

    /// Per-phase state accessor for diagnostic logging.
    ///
    /// Returns `&ConnectingState` — the per-phase enum carries only
    /// the handshake-reachable variants; `ProtoState`'s
    /// post-handshake `Active*` variants don't exist here. The
    /// transition signal `ConnectingState::HandshakeReady` represents
    /// the post-RFQ "ready to enter Active" state (a naive shape
    /// would surface this as `ProtoState::Idle` here, but
    /// open-pattern matching on a wider ProtoState lets future
    /// post-handshake variants silently slip through into Connecting-
    /// state code paths).
    ///
    /// **For typed predicates**: callers should prefer
    /// [`Self::is_handshake_ready`] and [`Self::is_errored`] for the
    /// transition-decision use cases — they're stronger invariants
    /// than open pattern matching (future variant additions cannot
    /// silently change what "ready" means).
    #[inline]
    #[must_use]
    pub fn state(&self) -> &crate::state::ConnectingState {
        &self.inner.state
    }

    /// Typed predicate: handshake successfully completed, ready for
    /// [`Self::into_active`].
    ///
    /// Returns `true` iff `self.inner.state` is
    /// [`crate::state::ConnectingState::HandshakeReady`] — the
    /// payload-carrying transition variant written by the per-phase
    /// dispatch wrapper when the shared dispatch's
    /// `(PostAuthHaveKey, RFQ)` arm produced
    /// `ProtoState::HandshakeReady { pid, secret_key }`. The
    /// `(pid, secret_key)` material is captured in the variant
    /// payload at the same moment.
    ///
    /// Future-proof against `ConnectingState` variant additions —
    /// adding a new variant cannot change what "ready" means.
    #[inline]
    #[must_use]
    pub fn is_handshake_ready(&self) -> bool {
        matches!(
            self.inner.state,
            crate::state::ConnectingState::HandshakeReady { .. },
        )
    }

    /// Typed predicate: handshake failed, transition will route to
    /// [`PgProtocol<ClosedPhase>`] via [`Self::into_active`].
    ///
    /// Returns `true` iff `self.inner.state` is
    /// [`crate::state::ConnectingState::Errored`]. Useful for
    /// caller-side classification before triggering
    /// `into_active()` (which returns `IntoActiveError::Closed` in
    /// this case).
    #[inline]
    #[must_use]
    pub fn is_errored(&self) -> bool {
        matches!(
            self.inner.state,
            crate::state::ConnectingState::Errored(_),
        )
    }

    /// session_params accessor during handshake (mirrors
    /// `<ActivePhase>::session_params`). The server's
    /// `ParameterStatus` frames during the handshake populate these;
    /// callers may inspect mid-handshake values for diagnostic
    /// purposes.
    #[inline]
    #[must_use]
    pub fn session_params(&self) -> &SessionParams {
        // Mirror of `<ActivePhase>::cold_session_params`. The static
        // empty fallback matches the Active accessor byte-for-byte.
        static EMPTY: SessionParams = SessionParams::new();
        match self.inner.session_params.as_deref() {
            Some(p) => p,
            None => &EMPTY,
        }
    }

    /// Server-error arena accessor during handshake (mirrors
    /// `<ActivePhase>::get_server_error`). Useful when handshake
    /// fails: `ErrorResponse` during startup classifies as
    /// `ProtocolError::ServerErrorResponse { details_ref, … }`;
    /// callers resolve via this method to inspect the server's
    /// message before transitioning to `<ClosedPhase>`.
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

    /// Read the parked `Action::FailReply.cause` from the most-recent
    /// failure event during handshake (.b). Mirror of
    /// [`PgProtocol::<ActivePhase>::fail_cause`].
    ///
    /// Returns `None` if no failure has been observed yet on this
    /// connecting protocol instance. The slot persists across
    /// `into_active` (forwarded into the new
    /// `<ActivePhase>::Extras.fail_cause` slot).
    ///
    /// See the Active-phase doc for the caller contract on
    /// latest-wins semantics.
    #[inline]
    #[must_use]
    pub fn fail_cause(&self) -> Option<&crate::error::ProtocolError> {
        self.inner.fail_cause.as_ref()
    }

    /// `as_ready` accessor during handshake. ALWAYS returns `None`
    /// while in `<ConnectingPhase>` because the phase classifier
    /// maps every `Connecting*` variant to
    /// `ConnectionStatus::Handshaking` (not `Ready`). Exposed for
    /// test parity with the Active surface's `as_ready` predicate.
    ///
    /// **Type signature note:** returns `Option<()>` rather than
    /// `Option<ReadyGuard>` — there is NO legitimate push path
    /// during handshake (the only Connecting-state command would be
    /// re-Startup, which is also banned by `<DisconnectedPhase>`
    /// consume-self). The `()` return marks "handshaking, no push
    /// guard available" without exposing the ActivePhase-bound
    /// `ReadyGuard` type.
    #[inline]
    #[must_use]
    pub fn as_ready(&mut self) -> Option<()> {
        // `<ConnectingPhase>` always reports Handshaking; no Idle
        // classification path exists during handshake (Idle here
        // would imply RFQ-complete, at which point the caller must
        // `into_active()` to access the push surface).
        None
    }

    /// `connection_status` accessor during handshake — mirrors
    /// `<ActivePhase>::connection_status`.
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
    /// Typed error accessor for a terminally-Errored protocol.
    ///
    /// **Tier-1 by storage absence**: `<ClosedPhase>::Inner =
    /// ClosedInner` stores only `state_kind: StateErrorKind` —
    /// there is no `ProtoState` at all on Closed. A naive shape
    /// would match `&self.inner.state` against `ProtoState::Errored(k)`
    /// with an "architecturally unreachable" defensive arm
    /// (`_ => ProtocolError::InternalCrateBug { … }`) for the
    /// impossible non-Errored case — tier-3
    /// by-state-machine-reasoning. The defensive arm is GONE by
    /// storage absence here: the type system cannot construct a
    /// Closed protocol with non-Errored state since the variant
    /// simply isn't there.
    ///
    /// # Tier-1 closure
    ///
    /// `<ClosedPhase>` exposes ONLY `cause()` and `close_cause()`. No
    /// `push_command`, no `feed_inbound`, no `feed_bytes`, no
    /// `advance_one_frame`, no `into_active`. Calling any of those on
    /// a `<ClosedPhase>` instance is method-absent E0599 («Closed
    /// absorbs no input»). The protocol is terminal.
    ///
    /// # Errored vs graceful
    ///
    /// - **Errored close** (any tier-1-classified error path):
    ///   returns `Err(ProtocolError::ConnectionAlreadyClosed { prior_kind })`.
    /// - **Graceful close** (client-initiated via [`PgProtocol::<ActivePhase>::terminate`]):
    ///   returns `Ok(())`. No error; the protocol was cleanly closed.
    ///
    /// Callers that need the raw discriminator (e.g. for logging the
    /// close path without synthesising an error) can use
    /// [`Self::close_cause`] which returns the [`CloseCause`] enum
    /// directly.
    #[inline]
    #[must_use = "the returned Result carries the terminal cause: Err for errored close, \
                  Ok(()) for graceful terminate. Observing it is the only legitimate \
                  operation on a Closed protocol."]
    pub fn cause(&self) -> Result<(), crate::error::ProtocolError> {
        match self.inner.cause {
            CloseCause::Errored(prior_kind) => {
                Err(crate::error::ProtocolError::ConnectionAlreadyClosed { prior_kind })
            }
            CloseCause::GracefulTerminate => Ok(()),
        }
    }

    /// Raw close-cause discriminator. Returns the [`CloseCause`] enum
    /// the post-transition `<ClosedPhase>` was constructed with — see
    /// the enum's variants for path-specific semantics.
    ///
    /// Use this when logging or branching on the close path WITHOUT
    /// synthesising a [`crate::error::ProtocolError`]. For the
    /// error-or-graceful Result shape, prefer [`Self::cause`].
    #[inline]
    #[must_use]
    pub fn close_cause(&self) -> CloseCause {
        self.inner.cause
    }

    /// Resolve a server `ErrorRef` against the preserved arena.
    /// Mirror of [`PgProtocol::<ActivePhase>::get_server_error`] —
    /// useful when the wrapper layer stashed an `ErrorRef` from a
    /// `ServerErrorResponse` classified before the transition to
    /// `<ClosedPhase>`. Returns `Stale` (arena's generation
    /// classifier) if the arena was cleared or the ref is from a
    /// different generation.
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
        arena.get(r)
    }

    /// Preserved diagnostic counter from the arena. Returns 0 if no
    /// arena was buffered before the transition.
    #[inline]
    #[must_use]
    pub fn error_arena_overwrite_count(&self) -> u16 {
        match self.inner.error_arena.as_deref() {
            Some(a) => a.overwrite_count(),
            None => 0,
        }
    }
}

// ═════════════════════════════════════════════════════════════════════
// Feed-side error-transition leaves
//
// Per-call-site concrete-type tokens that gate
// `crate::state_setter::drain_at_*` constructors. Each leaf
// submodule hosts a token with PRIVATE field — the literal
// `Self(())` mint is callable ONLY inside the submodule. The token
// is consumed by the matching `drain_at_*` free fn in mod
// state_setter, which in turn constructs `FeedStateSetter::new`
// (private to mod state_setter).
// ═════════════════════════════════════════════════════════════════════

/// Leaf submodule for the `install_errored_replyid_saturation`
/// transition. The saturation classifier fires from any state,
/// hence the `drain_at_replyid_saturation` returns
/// `Option<NonZeroU64>` (None for `Idle` / `DrainRfqAfterError` /
/// `Errored` prior states).
pub(crate) mod _replyid_saturation_drain_leaf {
    /// Leaf-scope token. Field private to leaf.
    pub(crate) struct ReplyIdSaturationToken(());

    /// Mint a [`ReplyIdSaturationToken`] and route through
    /// [`crate::state_setter::drain_at_replyid_saturation`]. Used by
    /// [`crate::PgProtocol::install_errored_replyid_saturation`].
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id (if any). \
                  Caller `install_errored_replyid_saturation` consumes it via \
                  `match drain(...) { Some(_) | None => {} }`; saturation has no \
                  FailReply emission context."]
    pub(in crate::protocol) fn drain(
        state: &mut crate::state::ProtoState,
        kind: crate::error::StateErrorKind,
    ) -> Option<core::num::NonZeroU64> {
        crate::state_setter::drain_at_replyid_saturation(state, ReplyIdSaturationToken(()), kind)
    }
}

/// Leaf submodule for the `install_errored_read_cursor_advance`
/// transition. Fires when the row-stream fast path detects a
/// read-cursor advance failure
/// (`CrateBugLocus::ReadCursorAdvance`).
pub(crate) mod _read_cursor_advance_drain_leaf {
    /// Leaf-scope token. Field private to leaf.
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

/// Leaf submodule for the `install_errored_partial_mode_reentry`
/// transition. Fires when [`crate::buf::ReadBuf::enter_partial_mode`]
/// returns `Err(AlreadyInPartialMode)` — an internal classifier bug
/// classified as
/// [`crate::error::CrateBugLocus::PartialModeReentry`].
pub(crate) mod _partial_mode_reentry_drain_leaf {
    /// Leaf-scope token. Field private to leaf.
    pub(crate) struct PartialModeReentryToken(());

    /// Mint a [`PartialModeReentryToken`] and route through
    /// [`crate::state_setter::drain_at_partial_mode_reentry`].
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
                  by the Errored install. Caller MUST emit ColEvent::EndQuery { outcome: Err(_) } or equivalent."]
    pub(in crate::protocol) fn drain(
        state: &mut crate::state::ProtoState,
        kind: crate::error::StateErrorKind,
    ) -> Option<core::num::NonZeroU64> {
        crate::state_setter::drain_at_partial_mode_reentry(state, PartialModeReentryToken(()), kind)
    }
}

/// Leaf submodule for the
/// `install_errored_partial_mode_exit_undrained` transition. Fires
/// when [`crate::buf::ReadBuf::exit_partial_mode`] returns
/// `Err(PartialModeExitUndrained)` — an internal classifier bug OR
/// adversarial server emitting a body-length-vs-column-sum-mismatched
/// DataRow; classified as
/// [`crate::error::CrateBugLocus::PartialModeExitUndrained`].
pub(crate) mod _partial_mode_exit_undrained_drain_leaf {
    /// Leaf-scope token. Field private to leaf.
    pub(crate) struct PartialModeExitUndrainedToken(());

    /// Mint a [`PartialModeExitUndrainedToken`] and route through
    /// [`crate::state_setter::drain_at_partial_mode_exit_undrained`].
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
                  by the Errored install. Caller MUST emit ColEvent::EndQuery { outcome: Err(_) } or equivalent."]
    pub(in crate::protocol) fn drain(
        state: &mut crate::state::ProtoState,
        kind: crate::error::StateErrorKind,
    ) -> Option<core::num::NonZeroU64> {
        crate::state_setter::drain_at_partial_mode_exit_undrained(state, PartialModeExitUndrainedToken(()), kind)
    }
}

/// Leaf submodule for the `install_errored_malformed_data_row`
/// transition. Fires from streaming variants when a DataRow body is
/// malformed (zero-length, etc.).
pub(crate) mod _malformed_data_row_drain_leaf {
    /// Leaf-scope token. Field private to leaf.
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

/// Leaf submodule for the `fail_inflight_no_readbuf` transition.
/// Fires from dispatch when an in-flight error occurs and no
/// read-buf state is available for payload preservation.
pub(crate) mod _fail_inflight_no_readbuf_drain_leaf {
    /// Leaf-scope token. Field private to leaf.
    pub(crate) struct FailInflightNoReadbufToken(());

    /// Outcome of a drain attempt — separates "no transition,
    /// preserved Errored" from "transition occurred (with or
    /// without an inflight reply id)". The two cases would collide
    /// under a single `Option<NonZeroU64>` return, breaking the
    /// caller's ability to bump the malformed-event canary on a
    /// transition where the prior state was non-inflight (e.g.
    /// `Idle`): old `None`-on-no-inflight and new
    /// `None`-on-already-Errored would be indistinguishable.
    #[derive(Debug, Clone, Copy)]
    pub(in crate::protocol) enum DrainOutcome {
        /// Transition `non-Errored → Errored(kind)` occurred. Inner
        /// `Option<NonZeroU64>` carries the prior inflight reply id
        /// (Some) or signals "no inflight reply on the prior state"
        /// (None — e.g. `Idle`, `DrainRfqAfterError`).
        Transitioned(Option<core::num::NonZeroU64>),
        /// No transition — state was already `Errored(prior_kind)`
        /// at entry. Original kind is preserved; no actions should
        /// be re-emitted by the caller for this fail attempt.
        AlreadyErrored,
    }

    /// Mint a [`FailInflightNoReadbufToken`] and route through
    /// [`crate::state_setter::drain_at_fail_inflight_no_readbuf`].
    ///
    /// **Idempotent on sticky-Errored**: a re-entrant fail attempt
    /// returns [`DrainOutcome::AlreadyErrored`] and preserves the
    /// existing `Errored(prior_kind)` rather than overwriting it.
    /// A naive `mem::replace`-only path would clobber the original
    /// cause classifier on the second call, hiding the first
    /// malformed event behind the second one.
    #[inline]
    #[must_use = "the returned DrainOutcome carries the in-flight reply id (if any) atomically \
                  drained by the Errored install, or AlreadyErrored when the state was already \
                  Errored. Caller decides FailReply/CloseSocket emission on the Transitioned \
                  arm; AlreadyErrored returns no actions."]
    pub(in crate::protocol) fn drain(
        state: &mut crate::state::ProtoState,
        kind: crate::error::StateErrorKind,
    ) -> DrainOutcome {
        if matches!(state, crate::state::ProtoState::Errored(_)) {
            return DrainOutcome::AlreadyErrored;
        }
        let inflight = crate::state_setter::drain_at_fail_inflight_no_readbuf(
            state,
            FailInflightNoReadbufToken(()),
            kind,
        );
        DrainOutcome::Transitioned(inflight)
    }
}

/// Leaf submodule for the
/// `install_errored_stream_dropped_mid_stream` transition. Fires
/// from [`crate::row_stream::RowStream::drop`] when the stream is
/// dropped with `drained == false` (closure exited mid-frame: normal
/// early return, `?` propagation, panic unwind).
///
/// Mirror of the other drain leaves above. The
/// `StreamDroppedMidStreamToken` tuple-struct field is private to
/// this submodule — `Self(())` mints are callable ONLY inside the
/// leaf. Hostile in-crate attempts to call
/// `drain_at_stream_dropped_mid_stream` from outside this leaf
/// cannot construct the required token type; the type system rejects.
pub(crate) mod _stream_dropped_mid_stream_drain_leaf {
    /// Leaf-scope token. Field private to leaf.
    pub(crate) struct StreamDroppedMidStreamToken(());

    /// Mint a [`StreamDroppedMidStreamToken`] and route through
    /// [`crate::state_setter::drain_at_stream_dropped_mid_stream`].
    /// Sole legitimate caller is
    /// [`crate::PgProtocol::install_errored_stream_dropped_mid_stream`]
    /// (the `install_errored_*` helper invoked from
    /// `RowStream`'s Drop impl).
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically drained \
                  by the Errored install. Drop-site caller consumes it via \
                  `match drain(...) { Some(_) | None => {} }`; drop has no FailReply \
                  emission context, but the next operation on the connection surfaces \
                  ConnectionAlreadyClosed { prior_kind: ClientOrdering } so the user's \
                  oneshot is not silently leaked at the wrapper layer."]
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

impl PgProtocol<ActivePhase> {
    /// Closure-scoped access to the PostgreSQL §55.2.7 CancelRequest
    /// wire frame.
    ///
    /// # Tier-1 against retention
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
    ///   the trybuild probe in `tests/cancel_request_retention.rs`
    ///   (`E0521` lifetime-may-not-live-long-enough).
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
    /// The accessor is **infallible** — `<ActivePhase>` carries
    /// `BackendKey { pid, secret_key }` inline on `ActiveInner`,
    /// constructed by `<ConnectingPhase>::into_active` from the
    /// consumed `ConnectingState::HandshakeReady { pid, secret_key }`
    /// payload. The closure is invoked exactly once and its `R` is
    /// returned. A non-standard PG fork that skipped the `K` frame
    /// could not reach `<ActivePhase>` (the transition gate requires
    /// the `HandshakeReady` variant), so the prior `Option<R>` arm
    /// is structurally unreachable and was removed.
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
    /// - **Hot path**: build the array via the const-fn
    ///   `cancel_request_bytes` (zero alloc), move into Zeroizing
    ///   guard, invoke closure. ≤ 8 ns per `benches/hot_paths.rs`
    ///   floor.
    /// - **Method-absent on every other phase**: tier-1 by
    ///   visibility. `<DisconnectedPhase>` / `<ConnectingPhase>` /
    ///   `<ClosedPhase>` have no `with_cancel_request` method —
    ///   calling produces `E0599`. Pinned by per-phase trybuild
    ///   probes.
    /// - **Retention rejection**: tier-1 by HRTB lifetime
    ///   quantification.
    ///
    /// # Driver pattern
    ///
    /// ```ignore
    /// // Synchronous side-channel write:
    /// active.with_cancel_request(|bytes, pid| {
    ///     log::info!("cancelling pid {pid}");
    ///     side_socket.write_all(bytes)
    /// })?; // bytes scrubbed automatically on return.
    /// drop(side_socket); // No response expected on cancel socket.
    ///
    /// // Async pattern (needs owned-copy across .await):
    /// let buf: [u8; 16] = active.with_cancel_request(|bytes, _| *bytes);
    /// side_socket.write_all(&buf).await?;
    /// // `buf` is caller-owned; explicit zeroize on drop
    /// // (e.g. wrap in `Zeroizing<[u8; 16]>` if scrubbing the
    /// // copy matters for the driver's threat model).
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
    /// # Design choices
    ///
    /// - **`pid` exposed inside closure**: pid is wire-public —
    ///   matching the [`crate::StartupCompletePayload`] precedent.
    ///   Diagnostic value for operators.
    /// - **Zeroize-on-drop via stack guard**: A naive shape would
    ///   keep the secret-scrub mechanism on a long-lived
    ///   Sensitive<i32> field — tier-1 by-Drop-fire, suppressible
    ///   by mem::forget / Box::leak / ManuallyDrop. Instead the
    ///   stack-local Zeroizing guard is tier-1 by-closure-scope,
    ///   with retention structurally impossible.
    /// - **Method-absent on `<ConnectingPhase>`**: tier-1. A
    ///   driver wanting to cancel mid-handshake must drop the
    ///   connection; there is no production scenario where a pool
    ///   cancels a mid-handshake connection (cost of opening a new
    ///   connection < cost of debugging cancel semantics).
    #[inline]
    pub fn with_cancel_request<R>(
        &self,
        f: impl FnOnce(&[u8; 16], i32) -> R,
    ) -> R {
        // Tier-1 by storage absence: `<ActivePhase>` cannot be
        // constructed without a valid `BackendKey` (the only path
        // is `<ConnectingPhase>::into_active`, which consumes the
        // `ConnectingState::HandshakeReady { pid, secret_key }`
        // payload structurally). The `Option<R>` return that the
        // pre-Phase-1d.2 shape had is gone — there is no None arm
        // to model.
        let key: &crate::cancel::BackendKey = &self.inner.backend_key;
        let pid: i32 = key.pid;
        // Copy the i32 out of the cell's Sensitive<i32> into a
        // `Zeroizing<i32>` guard so the stack slot scrubs
        // deterministically when the function frame ends. A naive
        // shape would let the plain `i32` local live unscrubbed on
        // the function frame for the duration of the
        // `cancel_request_bytes` build below; the encoded `[u8; 16]`
        // is already wrapped in `Zeroizing` (covering BE bytes[12..16]),
        // but a plain-i32 intermediate would not be. Under
        // `panic = "unwind"` (cargo test) Drop fires; under
        // `panic = "abort"` (release; documented gap) the process
        // exits before the unscrubbed slot matters. Tier-1 within
        // scope.
        //
        // Closure-scope `Sensitive::with_inner` is the supported way
        // to copy out a `Sensitive<i32>`'s payload — the same shape
        // applied at the sibling site in dispatch.rs that builds the
        // post-auth Sensitive's contents.
        let secret_key_guard: zeroize::Zeroizing<i32> =
            zeroize::Zeroizing::new(key.secret_key.with_inner(|s| *s));
        // Materialise the wire frame inside a Zeroizing guard. The
        // `cancel_request_bytes` const-fn returns `[u8; 16]` on the
        // stack; the move into `Zeroizing::new(...)` is NRVO-friendly
        // (LLVM writes directly into the guard's inline storage).
        // Single source of truth for the byte layout: the
        // `cancel_request_bytes` builder, which is itself
        // const-pinned in `wire.rs`. The `*secret_key_guard` deref
        // copies the i32 (Copy primitive) into the builder's
        // parameter slot; LLVM is free to fold this into a direct
        // load.
        let bytes_guard: zeroize::Zeroizing<[u8; 16]> = zeroize::Zeroizing::new(
            crate::wire::cancel_request_bytes(pid, *secret_key_guard),
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
        // (documented gap; the panic-abort hook the crate ships in
        // `crate::panic_hook` covers the documented sites).
        r
    }

    /// Mint a fresh `ReplyId<K>` for an outbound command.
    ///
    /// The sole public mint surface for [`crate::ReplyId<K>`].
    /// `ReplyId::from_raw(...)` is `pub(crate)` so the only path to
    /// a `ReplyId<K>` from outside the crate is this method. A naive
    /// shape would expose `from_raw` as `pub` and let external
    /// crates mint their own IDs — tier-3 by-discipline with
    /// duplicate-ID risk.
    ///
    /// # Why `&mut self` if mint is via static atomic
    ///
    /// The counter is a `static AtomicU64` (mod-private below) — NOT
    /// a `PgProtocol` field. A per-protocol counter would force an
    /// inline `u64` field that grows `PgProtocol` 520 → 528 B and
    /// shifts LLVM whole-crate codegen heuristic, regressing the
    /// synthetic `column_decode/iter_10cols` bench by +6%
    /// (bisect-confirmed). Static-atomic mint preserves PgProtocol
    /// size at 520 B (no codegen shift) AND strengthens the
    /// invariant: globally-unique IDs across all `PgProtocol`
    /// instances (a per-protocol counter would only guarantee
    /// per-instance uniqueness).
    ///
    /// `&mut self` is retained on the signature because: (a) it
    /// keeps the API shape consistent with a future per-protocol
    /// design (forward-compat if a per-protocol counter ever
    /// returns), and (b) the borrow makes it obvious to callers
    /// that mint participates in the protocol's mutation cycle (it
    /// is not a "look-only" operation; the minted ID is
    /// correlator-bound to a future push).
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
    /// [`crate::ReplyKind::Payload`] — passing the wrong kind to a
    /// command's `reply` field is a type error.
    ///
    /// # Counter behaviour
    ///
    /// Saturating `u64` add. First call returns `NonZeroU64::new(1)`;
    /// each subsequent call increments by 1. Saturation at `u64::MAX`
    /// is architecturally distant (~10^19 commands process-wide).
    /// On saturation the counter parks at `u64::MAX` — every
    /// subsequent mint returns the same ID, surfacing as a
    /// duplicate-correlator failure at the wrapper's pending-replies
    /// table.
    ///
    /// # Tier-1 by-construction: single shared atomic
    ///
    /// Delegates to [`ActiveInner::next_reply_id`] which mints from
    /// the shared `PROCESS_REPLY_ID_COUNTER` static (process-global
    /// uniqueness across all four phases). A naive shape would
    /// maintain a SEPARATE local `static COUNTER` on this
    /// `impl PgProtocol<ActivePhase>` path — a process-global
    /// counter, but distinct from the one used by
    /// Disconnected/Connecting/ActiveInner: two `PgProtocol`
    /// instances on the same process could mint overlapping IDs
    /// across phase boundaries (e.g. Disconnected ID 1 on instance
    /// A, Active ID 1 on instance B). Tier-1 here: every phase
    /// mints from the same atomic.
    #[inline]
    pub fn next_reply_id<K: crate::reply_id::ReplyKind>(
        &mut self,
    ) -> crate::reply_id::ReplyId<K> {
        self.inner.next_reply_id::<K>()
    }

    // Three-field cold split. Each cold slot is independently
    // lazy-allocated; malformed_counter is inline (4 B, no Box).

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

    /// Count of malformed-frame events that triggered connection
    /// teardown.
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
    #[inline]
    #[must_use]
    pub fn malformed_frame_count(&self) -> u32 {
        self.cold_malformed_frame_count()
    }

    /// Per-phase state accessor for diagnostic logging.
    ///
    /// Returns `&ActiveState` — the per-phase enum carries only
    /// post-handshake variants; `ProtoState`'s `Connecting*`
    /// variants don't exist here. A naive shape would return
    /// `&ProtoState` and let callers open-pattern over the wider
    /// enum; instead callers pattern-match against the same-meaning
    /// `ActiveState::SimpleQuery*` etc., with `ProtoState::Idle` /
    /// `ProtoState::PingAwaitingRfq` / `ProtoState::Errored(_)`
    /// mapping 1:1 to the same-named `ActiveState` variants.
    #[inline]
    #[must_use]
    pub const fn state(&self) -> &crate::state::ActiveState {
        &self.inner.state
    }

    /// Diagnostic predicate for the partial-assembly cell. Returns
    /// `true` iff an oversize non-`'D'` frame is currently
    /// mid-flight (body bytes accumulating across multiple
    /// `feed_bytes` calls).
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
    /// messages. Empty until startup completes. Returns a
    /// `&'static` empty default when the lazy-init box has not been
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
    /// Cursor advance happens in-scope inside `feed_bytes`, so this
    /// method simply forwards to `ReadBuf::unread()` — the caller
    /// always sees the current cursor state.
    #[inline]
    #[must_use]
    pub fn unread(&self) -> &[u8] {
        self.inner.read_buf.unread()
    }

    // ═════════════════════════════════════════════════════════════════
    // Witness-guard typestate for client-initiated push.
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
    /// → as_ready). Pinned by the guard-closure spec test suite.
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
    /// the guard-closure spec test suite.
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
    /// `IdleState::try_from(&mut state)` (the typestate IS the proof);
    /// the `None` arm classifies via
    /// `CrateBugLocus::PushCommandInternalNonIdle` PushFailure with
    /// `core::hint::cold_path()`. A naive shape would add a loud
    /// `debug_assert!(false, …)` dev branch — CREDO §V banned glass
    /// pattern (dev-loud + release-silent); the classified failure
    /// is the safety net in both dev and release modes.
    ///
    /// # Return surface
    ///
    /// Returns `Result<OutActions<'w>, PushFailure>` (~88 B
    /// plus the OutActions container in the Ok arm). On Ok, the
    /// frame bytes live in the caller's [`WriteBuf`] and the
    /// returned `OutActions` carries the `Action::SendBytes(&'w [u8])`
    /// chunks for the caller's I/O layer. On Err, state has already
    /// transitioned to `Errored`; caller resolves the user's oneshot
    /// via the returned `failure.id` and `failure.cause` and closes
    /// the socket per the [`crate::PushFailure`] `#[must_use]`
    /// contract.
    ///
    /// Generic over `C: PushCommand`. Caller passes a per-command
    /// struct (e.g. [`crate::push_command::Ping`]) — each `C` is
    /// monomorphised, so there is no 2176-B-by-value enum dispatch
    /// to thread through.
    #[must_use = "the returned Result carries OutActions on success (caller drains \
                  the multi-chunk frame: header range + borrowed SQL + trailer range \
                  + Sync) or the consumed-correlator + cause failure signal — both \
                  must be observed by the caller's I/O layer"]
    pub(crate) fn push_command_internal<'w, C: crate::push_command::PushCommand + 'w>(
        &mut self,
        cmd: C,
        write_buf: &'w mut WriteBuf,
    ) -> Result<crate::action::OutActions<'w>, crate::action::PushFailure> {
        // The full outbound frame is the ordered concatenation of:
        //   1. SendBytesRange (header bytes in wb)
        //   2. SendBytesBorrowed (caller's SQL — zero-copy)
        //   3. SendBytesRange (trailer bytes in wb)
        //   4. SendBytesStatic (Sync trailer for Parse — `&'static`)
        // OutActions surfaces these as `Action::SendBytes(&[u8])` per
        // chunk (4 for Parse, 3 for SimpleQuery, 1 for
        // Ping/Bind/etc). Under `writev` / IoSlice the caller
        // collapses the chunks to a single socket syscall.
        write_buf.clear();

        // The Idle precondition is enforced by
        // [`crate::state_setter::IdleState::try_from`] below — the
        // `Option<IdleState<'_>>` typestate IS the proof. The
        // legitimate caller is `ReadyGuard::push_command` (which
        // performs `as_ready` Idle classification upstream); this
        // re-check is the single load-bearing guard. A naive shape
        // would augment it with "caller must promise + we
        // debug_assert" defensive surface.
        //
        // Pass `StatePushClass::Idle` as a STATIC const argument —
        // LLVM specialises the inlined
        // `clear_session_residue_for_class` body to the Idle arm
        // only, eliding the 5-arm dispatch entirely.
        //
        // `row_desc_slot` lives on outer `<ActivePhase>::Extras` —
        // pass `&mut self.extras` to the per-Inner residue method.
        self.inner.clear_session_residue_for_class(
            &mut self.extras,
            crate::state::StatePushClass::Idle,
        );

        // perf-recovery (2026-05-23): the lift step
        // pre-was
        //
        //   let mut proto_state: ProtoState =
        //       core::mem::replace(&mut self.inner.state,
        //                          ActiveState::Idle).into();
        //
        // The `.into()` walks a 25-arm `From<ActiveState> for
        // ProtoState` match — fully redundant on the IdleState hot
        // path because the setter precondition is `state == Idle`
        // and the only Idle→ProtoState mapping is `ProtoState::Idle`.
        //
        // Pre-check the Idle precondition via `matches!()` against
        // `self.inner.state` (which is `ActiveState`). On the
        // production hot path (`as_ready()` upstream Idle check)
        // this is always true; the local `proto_state` starts as
        // `ProtoState::Idle` directly without the From walk. The
        // cold non-Idle branch (architecturally dead from
        // `ReadyGuard::push_command`) falls through to the
        // PushFailure arm below — the setter's `IdleState::try_from`
        // on a non-Idle ProtoState would have returned `None`
        // anyway; we just bypass the redundant conversion to surface
        // the same classifier failure.
        //
        // `mem::replace` is still required to overwrite `self.inner.state`
        // with the Idle sentinel so the post-closure lower-and-install
        // step can write the new variant without aliasing. The
        // RETURNED old value is intentionally discarded — Idle is a
        // ZST tag with no Drop targets.
        // Replace the slot with Idle sentinel; old value (must be
        // Idle on the hot path) is dropped trivially via the binding
        // `_replaced` (Idle is a ZST tag — Drop is a no-op). The
        // explicit binding (not `let _`) satisfies clippy's
        // `let_underscore_drop` lint forbidden crate-wide for
        // ZeroizeOnDrop-bearing types; ActiveState carries
        // SCRAM/MD5/Cleartext password variants that DO have
        // ZeroizeOnDrop, but Idle does not, so the drop is trivial
        // on the production hot path. On the cold non-Idle entry
        // (architecturally unreachable from `ReadyGuard::push_command`
        // gating), the prior ActiveState (which may carry secrets)
        // drops here — that's the correct scrub path.
        // ActiveState carries no Drop targets — none of its variants
        // hold password/secret payloads (those live exclusively in
        // Connecting variants). The `mem::replace` writes the Idle
        // sentinel; the returned old value falls out of scope at the
        // end of this statement without needing an explicit drop call
        // (clippy::drop_non_drop forbids the no-op `drop` invocation).
        let _replaced: crate::state::ActiveState = core::mem::replace(
            &mut self.inner.state,
            crate::state::ActiveState::Idle,
        );
        let mut proto_state: ProtoState = ProtoState::Idle;
        let state = &mut proto_state;
        let row_desc_slot = &mut self.extras.row_desc;
        let idle = match crate::state_setter::IdleState::try_from(state) {
            Some(idle) => idle,
            None => {
                // The classified `PushFailure` below IS the safety
                // net; a naive shape would add a `debug_assert!(false,
                // …)` here — CREDO §V banned glass pattern (dev
                // loud + release silent fallthrough). The path is
                // architecturally unreachable on production callers
                // (ReadyGuard::push_command upstream classifies via
                // `as_ready`'s runtime Idle check; the `&mut
                // PgProtocol` borrow chain rules out interleaving
                // between as_ready and push_command_internal entry).
                //
                // The sentinel id is the distinct
                // `CRATE_BUG_REPLY_ID_SENTINEL` (NonZeroU64::MAX,
                // see `reply_id.rs` docstring). A naive shape would
                // use `NonZeroU64::MIN` which collides with the
                // legitimate first id minted by `next_reply_id` —
                // closed by-construction by the distinct sentinel.
                core::hint::cold_path();
                return Err(crate::action::PushFailure {
                    id: crate::reply_id::CRATE_BUG_REPLY_ID_SENTINEL,
                    cause: alloc::boxed::Box::new(
                        crate::error::ProtocolError::InternalCrateBug {
                            locus: crate::error::CrateBugLocus::PushCommandInternalNonIdle,
                        },
                    ),
                });
            }
        };

        // Single-pass materialise inside the branded closure: stage,
        // drain, classify-fail, emit `Action::SendBytes` chunks —
        // all in one walk. Closure returns the final
        // `Result<OutActions<'w>, _>` directly; no
        // intermediate `StagedActions` escape.
        //
        // A naive shape would (1) return `StagedActions` from the
        // closure (~700 B return frame), (2) scan for `FailReply`
        // in a `.iter()` pass, (3) pass `staged` by value to
        // `materialise` for a second iteration producing
        // `OutActions` (~800 B return) — three big stack moves +
        // two iterations cost ≈+34 ns on `push_command/ping`. The
        // feed-side `materialise` keeps its broader contract
        // (DeliverReply/FailReply → typed `Action` variants) for
        // dispatcher use; push is open-coded here for the perf-tier
        // closure on the hot path.
        //
        // Write-side keeps its brand (`'wb`) for tier-1
        // `WriteRange::apply`; read side is unbranded. row_desc_slot
        // is threaded through for BindExecute (other commands
        // ignore it).
        let result = write_buf.with_branded(|mut wb| -> Result<crate::action::OutActions<'w>, crate::action::PushFailure> {
            let mut staged: StagedActions<'_> = StagedActions::new();
            {
                let mut reserved = wb.reserve();
                // Setter is minted via the typestate (the only
                // legitimate path — `StateSetter::new` is
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
            let mut out: crate::action::OutActions<'w> = crate::action::OutActions::new();
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
                            // : Box wrap at PushFailure boundary.
                            failure = Some(crate::action::PushFailure {
                                id,
                                cause: alloc::boxed::Box::new(cause),
                            });
                        }
                    }
                    StagedAction::SendBytesRange(range) => {
                        // `apply == None` is architecturally
                        // unreachable under intact brand discipline;
                        // classify `CloseSocket` rather than silently
                        // emitting a zero-byte SendBytes.
                        let action = match range.apply(bytes) {
                            Some(slice) => crate::action::Action::SendBytes(slice),
                            None => {
                                // The classified `CloseSocket`
                                // fallback IS the safety net; a
                                // naive shape would add a
                                // `debug_assert!(false, …)` here —
                                // CREDO §V banned glass pattern.
                                core::hint::cold_path();
                                crate::action::Action::CloseSocket
                            }
                        };
                        push_within_fanout_budget(&mut out, action);
                    }
                    StagedAction::SendBytesStatic(s) => {
                        push_within_fanout_budget(&mut out, crate::action::Action::SendBytes(s));
                    }
                    // Borrowed bytes (caller's SQL via Parse /
                    // SimpleQuery push paths) flow through unchanged.
                    // The `'sql >= 'w` subtyping induced by the
                    // `Self: 'sql` bound on `PushCommand::execute`
                    // coerces `&'sql [u8]` to `&'w [u8]` for the
                    // emitted `Action::SendBytes`.
                    StagedAction::SendBytesBorrowed(b) => {
                        push_within_fanout_budget(&mut out, crate::action::Action::SendBytes(b));
                    }
                    StagedAction::CloseSocket => {
                        push_within_fanout_budget(&mut out, crate::action::Action::CloseSocket);
                    }
                    StagedAction::DeliverReply(_) => {
                        // Push paths never emit DeliverReply
                        // (replies come from server via feed_bytes
                        // only); architecturally dead. A naive shape
                        // would silently drop this arm on release
                        // with a loud-on-dev `debug_assert!(false, …)`
                        // — CREDO §V banned glass pattern. Classify
                        // as `PushFailure` with `InternalCrateBug`
                        // locus `PushEmittedDeliverReply` instead.
                        //
                        // The sentinel id is the distinct
                        // `CRATE_BUG_REPLY_ID_SENTINEL`
                        // (NonZeroU64::MAX, see `reply_id.rs`
                        // docstring) — mirrors the
                        // `PushCommandInternalNonIdle` site at the
                        // entry of this same function.
                        core::hint::cold_path();
                        if failure.is_none() {
                            failure = Some(crate::action::PushFailure {
                                id: crate::reply_id::CRATE_BUG_REPLY_ID_SENTINEL,
                                cause: alloc::boxed::Box::new(
                                    crate::error::ProtocolError::InternalCrateBug {
                                        locus: crate::error::CrateBugLocus::PushEmittedDeliverReply,
                                    },
                                ),
                            });
                        }
                    }
                    StagedAction::Notify { .. }
                    | StagedAction::Notice { .. }
                    | StagedAction::IntermediateCommandComplete { .. }
                    | StagedAction::CopyDataChunk { .. } => {
                        // : Notify is staged ONLY by the dispatch
                        // pre-filter on `'A'` tags during feed_bytes;
                        // : IntermediateCommandComplete by the
                        // SimpleQueryAwaitingRfq + C/T/I arms. Both
                        // feed-path-only — never by a push path.
                        // Reaching here from a push materialise is
                        // architecturally dead; classify silently to
                        // preserve the
                        // staged-action accounting invariant.
                        core::hint::cold_path();
                    }
                }
            }
            match failure {
                Some(f) => Err(f),
                None => Ok(out),
            }
        });

        // perf-recovery (2026-05-23): specialized
        // ProtoState→ActiveState lower for the push-output subset.
        //
        // The setter's `InstallBody::install` writes ONLY the
        // following ~10 ProtoState variants (per the InstallBody
        // impls in `state_setter.rs:605+` — Active-phase targets):
        //   - PingAwaitingRfq
        //   - SimpleQueryAwaitingFirstResponse
        //   - ParseAwaitingParseComplete
        //   - BindExecuteAwaitingBindCompleteDml
        //   - BindExecuteAwaitingBindCompleteSelect
        //   - BindExecuteAwaitingDataOrCompleteSelect (resume)
        //   - BindExecuteAwaitingCommandCompleteDml (resume)
        //   - DescribeStatementAwaitingParamDesc
        //   - DescribePortalAwaitingRowDescOrNoData
        //   - CloseAwaitingComplete
        //
        // The generic `ActiveState::try_from` walks the FULL 25-arm
        // match. The 15 non-push-output arms are architecturally
        // dead on this code path (setter cannot produce them). Open-
        // coding the 10 hot arms inline saves ~5-10 ns per
        // push_command_internal call.
        //
        // The defensive `_other` arm absorbs any setter-machinery bug
        // via classified Errored — same safety net as the prior
        // `TryFrom::try_from` Err branch.
        use crate::state::ActiveState;
        self.inner.state = match proto_state {
            ProtoState::PingAwaitingRfq(r) => ActiveState::PingAwaitingRfq(r),
            ProtoState::SimpleQueryAwaitingFirstResponse(r) => {
                ActiveState::SimpleQueryAwaitingFirstResponse(r)
            }
            ProtoState::ParseAwaitingParseComplete(r) => {
                ActiveState::ParseAwaitingParseComplete(r)
            }
            ProtoState::BindExecuteAwaitingBindCompleteDml(r) => {
                ActiveState::BindExecuteAwaitingBindCompleteDml(r)
            }
            ProtoState::BindExecuteAwaitingBindCompleteSelect { reply } => {
                ActiveState::BindExecuteAwaitingBindCompleteSelect { reply }
            }
            ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply } => {
                ActiveState::BindExecuteAwaitingDataOrCompleteSelect { reply }
            }
            ProtoState::BindExecuteAwaitingCommandCompleteDml(r) => {
                ActiveState::BindExecuteAwaitingCommandCompleteDml(r)
            }
            ProtoState::DescribeStatementAwaitingParamDesc(r) => {
                ActiveState::DescribeStatementAwaitingParamDesc(r)
            }
            ProtoState::DescribePortalAwaitingRowDescOrNoData(r) => {
                ActiveState::DescribePortalAwaitingRowDescOrNoData(r)
            }
            ProtoState::CloseAwaitingComplete(r) => ActiveState::CloseAwaitingComplete(r),
            other => {
                // Architecturally dead: setter PostState bound
                // restricts output to the 10 arms above. Defensive
                // Errored absorbs any future setter-machinery bug
                // without panicking.
                core::hint::cold_path();
                match other.take_inflight_reply_raw_id() {
                    Some(_) | None => {}
                }
                ActiveState::Errored(
                    crate::error::StateErrorKind::from_kind_or_internal(
                        crate::error::ErrorKind::Internal,
                    ),
                )
            }
        };
        result
    }

    // No `push_bind_execute_internal` ships from this impl. A naive
    // shape would have an 8-arg wire-shape method; instead callers
    // build a `crate::push_command::BindExecute<P>` struct and
    // dispatch through `push_command_internal::<BindExecute<P>>`.
    // The struct's field layout mirrors the PG Bind+Execute frame
    // exactly.

    /// Append inbound wire bytes into the read buffer **without
    /// dispatching**. Forward-compat anchor for pipelining where the
    /// caller decouples byte-feeding from event-pulling.
    ///
    /// `feed_inbound` exposes the append step as a separate
    /// operation so callers can drive the protocol via
    /// [`Self::advance_one_frame`] in a per-event loop:
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
    /// The signature returns `Result<(), ProtocolError>` so Errored
    /// state surfaces to the caller as a typed error instead of
    /// silently no-op'ing (`ReadBufFull` lifts into
    /// `ProtocolError::ReadBufferFull { … }` — the same enum the
    /// dispatch path uses). The protocol is terminal once Errored;
    /// further inbound bytes are surfaced as the terminal cause.
    pub fn feed_inbound(&mut self, bytes: &[u8]) -> Result<(), crate::error::ProtocolError> {
        // 1-line delegate to the Inner. The same body executes
        // identically from `<ActivePhase>` and `<ConnectingPhase>`
        // (server-driven auth bytes during handshake).
        // `<ClosedPhase>` does NOT have feed_inbound — Errored/Closed
        // terminal absorbs no input.
        self.inner.feed_inbound(bytes)
    }

    /// Process at most one user-observable event and return it.
    ///
    /// Per-event alternative to the batched [`Self::feed_bytes`].
    /// Forward-compat anchor for pipelining (where multiple
    /// concurrent in-flight replies may resolve in one cycle and the
    /// caller wants explicit event-by-event control).
    ///
    /// The implementation reuses [`Self::feed_bytes_bounded`] with
    /// `max_dispatches = 1` and an empty byte slice — single source
    /// of truth for dispatch is preserved.
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
    ///     `Action::CloseSocket`) → [`FeedEvent::Fail(id, cause)`]
    ///   - `Action::CloseSocket` alone (no in-flight reply id) →
    ///     [`FeedEvent::Close`]
    ///
    /// # Lifetime contract
    ///
    /// `FeedEvent<'wb>` carries two lifetimes:
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
    pub fn advance_one_frame<'w>(
        &mut self,
        write_buf: &'w mut WriteBuf,
    ) -> crate::action::FeedEvent<'w> {
        // 1-line delegate to the Inner so `<ConnectingPhase>` and
        // `<ActivePhase>` share the same implementation (handshake-
        // window callers also need per-event advance for server-
        // driven auth chains). row_desc_slot lives on outer
        // `<ActivePhase>::Extras` — pass via disjoint-field borrow.
        self.inner.advance_one_frame(&mut self.extras, write_buf)
    }

    /// Feed inbound wire bytes.
    ///
    /// Returns the action list — bounded by [`MAX_ACTIONS_PER_CALL`].
    /// Caller-owned `write_buf` — see [`push_command`] for the
    /// staged-dispatch architecture.
    ///
    /// `&'r mut self` — the row slices in the row-streaming `ColEvent` pull API
    /// borrow from `self.inner.read_buf`. The `'r` lifetime propagates
    /// into `OutActions<'w>`; the borrow checker blocks
    /// subsequent `&mut self` calls (and thus the next `feed_bytes`)
    /// until `OutActions` drops.
    ///
    /// [`push_command`]: Self::push_command
    #[must_use = "the returned actions carry side-effects that must be executed"]
    pub fn feed_bytes<'w>(
        &mut self,
        bytes: &[u8],
        write_buf: &'w mut WriteBuf,
    ) -> OutActions<'w> {
        // `const BOUNDED = false` specialisation — monomorphised
        // body with the per-iter bound check eliminated at compile
        // time. The production hot path does not pay
        // `if dispatches_this_call >= max_dispatches` every frame.
        // A naive shape would supply `u16::MAX` at runtime, which
        // LLVM sometimes optimises away via inlining — not
        // guaranteed on large functions.
        //
        // 1-line delegate to the Inner. The
        // `<ConnectingPhase>::feed_bytes` mirror shares the identical
        // inner-method dispatch — same const-generic specialisation,
        // same hot-path codegen. row_desc_slot lives on outer
        // `<ActivePhase>::Extras` — pass via disjoint-field borrow.
        self.inner.feed_bytes_impl::<false>(&mut self.extras, bytes, write_buf, 0)
    }

    /// Frame-bounded variant of [`feed_bytes`].
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
    pub(crate) fn feed_bytes_bounded<'w>(
        &mut self,
        bytes: &[u8],
        write_buf: &'w mut WriteBuf,
        max_dispatches: u16,
    ) -> OutActions<'w> {
        // 1-line delegate to the Inner mirror. `row_stream`'s
        // slow-path call site (`self.proto.feed_bytes_bounded`)
        // resolves through this delegate unchanged. row_desc_slot
        // lives on outer `<ActivePhase>::Extras` — pass via
        // disjoint-field borrow.
        self.inner.feed_bytes_bounded(&mut self.extras, bytes, write_buf, max_dispatches)
    }

    // The two large method bodies — `feed_bytes_impl<const BOUNDED>`
    // and `clear_session_residue_for_class` — live on each `Inner`
    // so `<ActivePhase>` (default phase) and `<ConnectingPhase>`
    // (server-driven auth bytes during handshake) reach the SAME
    // implementation via 1-line delegates. The 4 surface-facing
    // delegates above (`feed_inbound`, `advance_one_frame`,
    // `feed_bytes`, `feed_bytes_bounded`,
    // `clear_session_residue_for_class`) close the bridge.
    //
    // `impl PgProtocol<ActivePhase>` re-opens BELOW the
    // dispatch-context section so the remaining
    // `<ActivePhase>`-only methods (`get_server_error`,
    // `read_buf_append`, `current_row_desc`, `iter_rows`, …)
    // stay grouped with their kin.
}

// ═════════════════════════════════════════════════════════════════════
// Dispatch path as free functions over `DispatchContext`.
//
// The dispatch body lives as **free functions** over
// `DispatchContext<'r>` (eight `&mut` field refs). Per-phase Inner
// methods are thin delegates that build a `DispatchContext` from
// `&mut self.<field>` and forward. The disjoint-field-borrow rule
// (Rust 2018+) lets the struct-literal construction split the
// `&mut self` borrow across eight fields in one expression.
//
// **Tier impact**: refactoring-only. LLVM inlines the thin delegate
// unconditionally (single-call body, no other code); asm-diff is
// bit-equivalent versus a monolithic `&mut self` method body.
//
// **Why free fn over trait method**: a trait method
// (`fn dispatch(&mut self, …)`) on a `HasDispatchFields` trait
// would require `impl HasDispatchFields for ConnectingInner /
// ActiveInner`. The trait surface is tier-2: discipline gates
// "only the right `Inner` types impl it". The free-function form
// is tier-1 — only code that already holds the eight specific `&mut
// T` refs can build a `DispatchContext`, and those refs come from
// the field types directly. Narrow `Inner` types (e.g.
// `DisconnectedInner` with zero relevant fields) physically cannot
// construct a `DispatchContext` at the type level.
// ═════════════════════════════════════════════════════════════════════

/// Residue-clear body as a free function over four `&mut` field
/// refs + a class.
///
/// The exhaustive match on `StatePushClass` enforces tier-1 closure
/// — a future variant fails the build here until its residue policy
/// is decided.
///
/// **Callers**: the per-Inner `clear_session_residue_for_class`
/// delegate and `feed_bytes_dispatch` (direct call BEFORE
/// destructuring the `DispatchContext` into local re-bindings).
#[inline]
#[allow(
    clippy::too_many_arguments,
    reason = "added `param_oids_slot` next to \
              `row_desc_slot` for the per-cell residue clear policy. \
              Each parameter is a distinct mutable view into \
              PgProtocol storage; the count grew from 7 to 8 to \
              mirror the slot-pattern. Bundling would defeat the \
              destructured-borrows discipline of the call site. \
              perf-recovery: `fail_cause_slot` is \
              NOT a parameter — the `<ActivePhase>::Idle` invariant \
              (slot empty by-construction; see `into_active` and \
              dispatch's Errored-is-terminal contract) makes the \
              clear provably dead. Skipping the arg saves one stack \
              push on every push_command and feed_bytes (ARM64 13th \
              arg spills past x0-x7)."
)]
pub(in crate::protocol) fn clear_session_residue_for_class_dispatch(
    row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
    param_oids_slot: &mut crate::param_oids_slot::ParamOidsSlotCell,
    command_tag_slot: &mut crate::command_tag_slot::CommandTagSlotCell,
    tx_status_slot: &mut crate::tx_status_slot::TxStatusSlotCell,
    session_params: &mut crate::session_params_slot::SessionParamsCell,
    error_arena: &mut Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
    notifications_arena: &mut Option<alloc::boxed::Box<crate::notifications_arena::NotificationsArena>>,
    copy_chunks_arena: &mut Option<alloc::boxed::Box<crate::copy_chunks_arena::CopyChunksArena>>,
    command_tags_arena: &mut Option<alloc::boxed::Box<crate::command_tags_arena::CommandTagsArena>>,
    partial_assembly: &mut crate::partial_assembly::PartialAssemblyCell,
    class: crate::state::StatePushClass,
) {
    match class {
        crate::state::StatePushClass::Idle => {
            _clear_residue_leaf::clear_schema_slot_residue(row_desc_slot);
            // : clear ParamOids slot at Idle boundaries.
            // Per-DescribeStatement-cycle lifecycle: 't' arrival
            // parks the box; 'Z' arrival materialises via `as_ref()`
            // into the public Reply. The cycle closes at the Idle
            // boundary that follows the terminal RFQ — slot drops
            // its box, freeing the 68 B heap.
            _clear_residue_leaf::clear_param_oids_slot_residue(param_oids_slot);
            // : clear command_tag slot at Idle boundaries.
            // Cycle closes at Idle entry — drop boxed CommandTag,
            // freeing the ~40 B heap.
            _clear_residue_leaf::clear_command_tag_slot_residue(command_tag_slot);
            // : reset tx_status slot to Idle at Idle
            // boundaries. Single-byte reset; conn-start-default
            // matches PG's actual idle state.
            _clear_residue_leaf::clear_tx_status_slot_residue(tx_status_slot);
            // perf-recovery: NO `fail_cause` clear
            // on the Idle arm. The slot is empty by-construction
            // whenever state is Idle (proof):
            //   1. `<ActivePhase>` is reachable only via
            //      `into_active`, which initialises ActiveExtras
            //      with `FailCauseSlotCell::empty(token)`.
            //   2. The push hot path never parks (the open-coded
            //      materialiser in `push_command_internal` converts
            //      `StagedAction::FailReply` directly into
            //      `PushFailure { id, cause: Box::new(cause) }` and
            //      transitions state to Errored without touching the
            //      slot).
            //   3. The feed_bytes hot path parks via `materialise`,
            //      which simultaneously transitions state to Errored
            //      (the `(state, install_errored, materialise)`
            //      triple is atomic per dispatch outcome).
            //   4. State Errored → Idle is structurally impossible
            //      (Errored is terminal in `<ActivePhase>`; the only
            //      exit is `into_closed_if_errored` to `<ClosedPhase>`).
            // Therefore whenever this Idle arm runs, the slot is
            // None. The prior .b `clear_fail_cause_slot_residue`
            // call was provably dead; removing it eliminates ~3-4
            // instructions per push and (more impactfully) drops the
            // `fail_cause_slot: &mut` parameter from this function's
            // ABI — saving one stack push per call (12 → 11 args,
            // ARM64 9th+ args spill past x0-x7).
            if let Some(arena) = error_arena.as_deref_mut() {
                arena.clear();
            }
            // : clear notifications arena at Idle boundaries.
            // Refs issued in prior cycles become Stale here — the
            // wrapper's OutActions iteration is complete by the time
            // the next push transitions back to Idle.
            if let Some(arena) = notifications_arena.as_deref_mut() {
                arena.clear();
            }
            if let Some(arena) = copy_chunks_arena.as_deref_mut() {
                arena.clear();
            }
            // Refs issued in prior cycles become Stale here; the
            // wrapper's OutActions iteration is complete by the time
            // the next push transitions back to Idle.
            if let Some(arena) = command_tags_arena.as_deref_mut() {
                arena.clear();
            }
            _clear_residue_leaf::clear_partial_assembly_residue(partial_assembly);
        }
        crate::state::StatePushClass::Errored(_) => {
            _clear_residue_leaf::clear_schema_slot_residue(row_desc_slot);
            // : also clear ParamOids slot on Errored —
            // any in-flight Describe is torn down with the
            // connection; the box's drop reclaims the heap.
            _clear_residue_leaf::clear_param_oids_slot_residue(param_oids_slot);
            // : also clear CommandTag slot on Errored.
            _clear_residue_leaf::clear_command_tag_slot_residue(command_tag_slot);
            // : also reset tx_status slot on Errored.
            _clear_residue_leaf::clear_tx_status_slot_residue(tx_status_slot);
            if let Some(arena) = error_arena.as_deref_mut() {
                arena.clear();
            }
            // : also clear notifications arena on Errored.
            // Connection teardown is in progress; any outstanding
            // NotificationRef is meaningless past this point.
            if let Some(arena) = notifications_arena.as_deref_mut() {
                arena.clear();
            }
            if let Some(arena) = copy_chunks_arena.as_deref_mut() {
                arena.clear();
            }
            if let Some(arena) = command_tags_arena.as_deref_mut() {
                arena.clear();
            }
            // Session-state forfeit on tear-down;
            // `SessionParams::clear`'s Drop chain scrubs
            // `SecretBoundedStr` bytes.
            _clear_residue_leaf::clear_session_params_residue(session_params);
            // Also clear partial assembly on Errored entry.
            _clear_residue_leaf::clear_partial_assembly_residue(partial_assembly);
        }
        crate::state::StatePushClass::Connecting
        | crate::state::StatePushClass::PingAwaiting
        | crate::state::StatePushClass::BusyQuery => {}
    }
}

/// Dispatch body as a free function over [`DispatchContext`].
///
/// **`const BOUNDED: bool` specialisation**: in the
/// `BOUNDED = false` monomorphisation LLVM eliminates the
/// `if BOUNDED && dispatches_this_call >= max_dispatches { break; }`
/// gate at compile time. Two monomorphised copies live in the
/// binary; release profile (LTO fat + codegen-units=1) deduplicates
/// common sub-expressions.
///
/// **Tier impact**: refactoring-only. The per-Inner delegate
/// inlines unconditionally — the emitted machine code at every
/// existing call site (row_stream slow path, the per-phase
/// `feed_bytes` mirrors) is bit-equivalent to a monolithic
/// `&mut self` method body (asm-diff verified gate).
//
// `feed_bytes_dispatch` takes a `materialise_fn` closure for the
// three terminal materialise call sites. The closure decouples the
// `OutActions<'w, ?>` return-lifetime from the slot's `'r`:
//
// - **With-schema (Active path)** — closure does
//   `materialise(staged, bytes, terminal_ref)` where `terminal_ref`
//   has the slot's `'r` lifetime → returns `OutActions<'w>`.
//
// - **No-schema (Connecting path)** — closure ignores `terminal_ref`
//   and calls `materialise(staged, bytes, None::<&'static RowDesc>)`
//   → returns `OutActions<'w>`. By covariance the caller
//   observes this as `OutActions<'w>` for any `'r_outer`.
//
// This pattern keeps the dispatch body unified (one source of truth,
// asm-diff cleared once) while splitting the output type at the
// closure seam — exactly where the lifetime invariant diverges.
//
// **Closure signature note**: `Fn(...) -> R` (not `FnOnce`) because
// the body has THREE materialise sites (`AlreadyErrored` /
// `AppendFailed` early-exit arms + main-loop end). All three call
// the closure exactly once per invocation; `Fn` admits the multi-
// call pattern with no capture-by-move restriction.
pub(in crate::protocol) fn feed_bytes_dispatch<'w, 'state, 'r, R, M, const BOUNDED: bool>(
    ctx: DispatchContext<'state, 'r>,
    bytes: &[u8],
    write_buf: &'w mut WriteBuf,
    max_dispatches: u16,
    materialise_fn: M,
) -> R
where
    // : closure no longer takes Reply payload slot args
    // (row_desc / param_oids / command_tag). .b: closure
    // takes `fail_cause_slot: &mut FailCauseSlotCell` so materialise
    // can park the cause at the StagedAction → Action transformation
    // boundary. The `'r` parameter stays on `OutActions` to bind the
    // `current_*` payload accessors.
    //
    // `Fn` (not `FnOnce`) because the body has THREE materialise
    // sites (`AlreadyErrored` / `AppendFailed` early-exit arms +
    // main-loop end). The slot is passed as a `&mut` argument to
    // each call to satisfy the unique-borrow rule (no closure
    // capture of the mut ref).
    M: Fn(
        StagedActions<'w>,
        &'w [u8],
        &mut crate::fail_cause_slot::FailCauseSlotCell,
    ) -> R,
{
    let DispatchContext {
        state,
        read_buf,
        row_desc_slot: terminal_row_desc,
        param_oids_slot: terminal_param_oids,
        command_tag_slot: terminal_command_tag,
        tx_status_slot: terminal_tx_status,
        fail_cause_slot,
        session_params: session_params_slot,
        error_arena: error_arena_slot,
        notifications_arena: notifications_arena_slot,
        notices_arena: notices_arena_slot,
        copy_chunks_arena: copy_chunks_arena_slot,
        command_tags_arena: command_tags_arena_slot,
        partial_assembly: partial_assembly_slot,
        malformed_count: malformed_counter,
        column_names: column_names_slot,
    } = ctx;

    write_buf.clear();
    // feed_bytes can be called in any state. Compute
    // `push_class()` ONCE here and pass to the residue helper. A
    // naive shape would have the helper compute `push_class`
    // internally (~+10 ns per call); caching at the entry point
    // amortises one classification across the full feed_bytes
    // dispatch loop.
    let entry_class = state.push_class();
    if matches!(
        entry_class,
        crate::state::StatePushClass::Idle
            | crate::state::StatePushClass::Errored(_)
    ) {
        if let Some(arena) = notices_arena_slot.as_deref_mut() {
            arena.clear();
        }
        clear_session_residue_for_class_dispatch(
            terminal_row_desc,
            terminal_param_oids,
            terminal_command_tag,
            terminal_tx_status,
            session_params_slot,
            error_arena_slot,
            notifications_arena_slot,
            copy_chunks_arena_slot,
            command_tags_arena_slot,
            partial_assembly_slot,
            entry_class,
        );
    }

    // No `pending_advance` slot is needed: cursor advance fires
    // IN-SCOPE inside the dispatch loop because no `StagedAction`
    // variant carries a read_buf borrow. A naive shape would have a
    // `StreamRowRange { row_bytes: &'r [u8] }` variant that pulled
    // into read_buf — cursor advance while that borrow is alive is
    // a borrow-check conflict. Instead the row-streaming surface
    // uses the typed Reply<'r> route via DeliverReply, never
    // re-borrowing read_buf in StagedAction.

    // Single point of classification. The `IngressClassification`
    // enum enumerates every legal entry condition (Errored /
    // AppendFailed / Ok) so the dispatcher match is tier-1
    // exhaustive. Each arm has one canonical handler path. A naive
    // shape would scatter state checks across the body (one to
    // decide whether to append, one to short-circuit on Errored,
    // one to handle the append-err) — logically identical, but
    // the borrow-checker forces the append to fire before
    // split-borrows, creating a visual disconnect between the
    // initial Errored check and the subsequent handler.
    #[derive(Debug)]
    enum IngressClassification {
        AlreadyErrored,
        AppendFailed { attempted: usize, available: usize },
        Ok,
    }

    // Partial-mode bytes routing. When the partial-assembly cell is
    // active (an oversize non-`'D'` body is mid-flight), inbound
    // bytes route to the assembly accumulator FIRST. Up to
    // `body_remaining` bytes are consumed (copied to the bounded
    // prefix or counted-and-skipped beyond the cap); only the
    // leftover (bytes belonging to the NEXT frame) flows to ReadBuf.
    //
    // Routing is gated on `is_active() -> bool`, a single byte-load
    // on the `Option<Box<_>>` niche discriminant. The inactive arm
    // runs `read_buf.append(bytes)` byte-for-byte — no perf delta
    // on the hot path.
    let bytes_for_readbuf: &[u8] = if !matches!(*state, ProtoState::Errored(_))
        && partial_assembly_slot.is_active()
    {
        core::hint::cold_path();
        let (_consumed, leftover) = _partial_assembly_dispatch_leaf::absorb_partial_assembly_at_dispatch(
            partial_assembly_slot,
            bytes,
        );
        leftover
    } else {
        bytes
    };

    let classification = if matches!(*state, ProtoState::Errored(_)) {
        IngressClassification::AlreadyErrored
    } else {
        match read_buf.append(bytes_for_readbuf) {
            Ok(()) => IngressClassification::Ok,
            Err(ReadBufFull { attempted, available, .. }) => {
                IngressClassification::AppendFailed { attempted, available }
            }
        }
    };

    // Single classification-driven dispatch. Exhaustive match on
    // `IngressClassification` — adding a new variant fails the
    // build here until a handler exists.
    match classification {
        IngressClassification::AlreadyErrored => {
            // Cold-path hint. Reaching here means caller fed bytes
            // after a fatal teardown — adversarial / mis-driven
            // state.
            core::hint::cold_path();
            read_buf.clear();
            // : closure no longer takes slot args; Reply
            // payloads are unit-shape lifetime markers, accessors live
            // on PgProtocol / OutActions.
            return write_buf.with_branded(|wb| -> R {
                let staged: StagedActions = StagedActions::new();
                materialise_fn(staged, wb.into_bytes(), fail_cause_slot)
            });
        }
        IngressClassification::AppendFailed { attempted, available } => {
            // Cold-path hint. ReadBuf overflow = fatal connection
            // teardown (FailReply + CloseSocket) on a path the
            // production hot loop never hits — keep this body out
            // of the inlined ingress arm.
            core::hint::cold_path();
            // Ordering invariant: `read_buf.clear()` MUST precede
            // `fail_inflight_no_readbuf` here. The clear's
            // zero-on-clear path scrubs any residual SCRAM
            // server-frame bytes (server-first / server-final
            // containing password-correlated material) BEFORE the
            // state transition consumes the SCRAM variant. A
            // reorder would open a residue window — partial inbound
            // bytes would survive into the post-Errored phase until
            // the wrapper drops the connection.
            read_buf.clear();
            return write_buf.with_branded(|wb| -> R {
                let mut staged: StagedActions = StagedActions::new();
                fail_inflight_no_readbuf(
                    state,
                    ProtocolError::ReadBufferFull { attempted, available },
                    &mut staged,
                    malformed_counter,
                );
                // : no slot args; payload data lives in
                // PgProtocol slots, queried via accessors.
                materialise_fn(staged, wb.into_bytes(), fail_cause_slot)
            });
        }
        IngressClassification::Ok => {
            // Fall through to main dispatch.
        }
    }

    // Main dispatch. Take shared borrow of populated + cursor
    // (both via immutable reborrow of read_buf's &mut).
    write_buf.with_branded(|mut wb| -> R {
        let mut staged: StagedActions = StagedActions::new();
        let populated: &[u8] = read_buf.populated();
        let cursor: u16 = read_buf.cursor_position_u16();
        let mut frames_consumed: u16 = 0_u16;
        let mut dispatches_this_call: u16 = 0_u16;

        // Post-loop staging for partial-assembly entry.
        //
        // The dispatch loop's `populated` shared borrow conflicts
        // with `partial_assembly_slot` mut access inside the loop
        // body. The `FrameTooLarge` arm stages the entry work
        // here; the post-loop block applies the mutation after
        // NLL closes `populated`'s borrow.
        let mut staged_partial_entry:
            Option<(u8, u32, &[u8])> = None;

        // Dispatch loop block: `reserved` holds `&mut wb.buf`
        // which must release before `wb.into_bytes()` post-loop.
        {
        let mut reserved = wb.reserve();
        // Assembly-completion dispatch fires BEFORE the
        // parse-header loop. If the prior `feed_inbound` /
        // `read_buf_append` / top-of-feed-bytes bytes-routing path
        // completed the in-flight body (`body_remaining == 0`),
        // take the assembly out, route its prefix through the
        // existing per-tag `dispatch()`, and free the Box.
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
                terminal_param_oids,
                terminal_command_tag,
                terminal_tx_status,
                error_arena_slot,
                copy_chunks_arena_slot,
                command_tags_arena_slot,
                column_names_slot,
            );
            match outcome {
                DispatchOutcome::AdvancedSilent => {
                    dispatches_this_call =
                        dispatches_this_call.wrapping_add(1);
                }
                DispatchOutcome::AdvancedWithAction { action } => {
                    dispatches_this_call =
                        dispatches_this_call.wrapping_add(1);
                    emit_actions!(&mut staged, budget: 1, [
                        action,
                    ]);
                }
                DispatchOutcome::Errored { reply_id, cause } => {
                    core::hint::cold_path();
                    // is transformed into the tag+id-only public
                    // `Action::FailReply`.
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
            // `const BOUNDED: bool` specialisation — in the
            // `BOUNDED=false` monomorphisation the short-circuit
            // `BOUNDED &&` evaluates at compile time; LLVM
            // eliminates the entire gate.
            if BOUNDED && dispatches_this_call >= max_dispatches {
                break;
            }
            let absolute_start = cursor.wrapping_add(frames_consumed);
            let after_consumed = populated
                .get(usize::from(absolute_start)..)
                .unwrap_or(&[]);

            let header = parse_header(after_consumed);
            match header {
                HeaderParse::Empty | HeaderParse::Incomplete => break,
                HeaderParse::MalformedLength { declared } => {
                    core::hint::cold_path();
                    fail_inflight_no_readbuf(
                        state,
                        ProtocolError::MalformedFrameLength { declared },
                        &mut staged,
                        malformed_counter,
                    );
                    break;
                }
                HeaderParse::FrameTooLarge { declared } => {
                    core::hint::cold_path();
                    // Universal-coverage entry to partial-assembly
                    // mode for non-`'D'` streaming-eligible tags.
                    // The actual mutation (enter + absorb +
                    // advance) is deferred to post-loop because the
                    // loop's `populated` shared borrow conflicts
                    // with `partial_assembly_slot` / `read_buf` mut
                    // access here.
                    let tag_byte = after_consumed.first().copied().unwrap_or(0);
                    let body_len_opt = declared.checked_sub(4);
                    match body_len_opt {
                        Some(body_len)
                            if crate::partial_assembly::is_streaming_eligible_tag(
                                tag_byte,
                            ) =>
                        {
                            let already_buffered_body = after_consumed
                                .get(HEADER_LEN..)
                                .unwrap_or(&[]);
                            // `already_buffered_body.len() <=
                            // READ_BUF_CAP - HEADER_LEN`; `HEADER_LEN +
                            // body_len <= READ_BUF_CAP <= u16::MAX`.
                            // The narrowing helper encapsulates the
                            // dead-arm landing pad as a single audit
                            // point.
                            let header_plus_body =
                                crate::narrow::u16_from_usize_under_u16_bound(
                                    HEADER_LEN.saturating_add(already_buffered_body.len()),
                                );
                            staged_partial_entry = Some((
                                tag_byte,
                                body_len,
                                already_buffered_body,
                            ));
                            frames_consumed = frames_consumed
                                .saturating_add(header_plus_body);
                            break;
                        }
                        _ => {
                            if tag_byte == crate::wire::TAG_DATA_ROW.byte()
                                && matches!(
                                    *state,
                                    ProtoState::SimpleQueryStreamingRows { .. }
                                        | ProtoState::BindExecuteStreamingRows { .. }
                                        | ProtoState::BindExecuteAwaitingDataOrCompleteSelect { .. }
                                )
                            {
                                break;
                            }
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
                    let total_len_usize = usize::from(total_len);
                    if after_consumed.len() < total_len_usize {
                        break;
                    }
                    // Payload extraction — length-arith invariant:
                    // parse_header Ok ⇒ total_len >= HEADER_LEN; the
                    // len-check above ensures
                    // total_len <= after_consumed.len(). Classified
                    // as tier-2 structural shield (architecturally
                    // dead None).
                    let payload = match after_consumed.get(HEADER_LEN..total_len_usize) {
                        Some(p) => p,
                        None => {
                            // Architecturally dead: parse_header
                            // validated total_len <= populated.len().
                            // None indicates a ReadBuf lifecycle bug.
                            // Classified rather than silent empty-
                            // slice fallback (CREDO §V glass ban).
                            fail_inflight_no_readbuf(
                                state,
                                ProtocolError::InternalCrateBug {
                                    locus: crate::error::CrateBugLocus::ReadCursorAdvance,
                                },
                                &mut staged,
                                malformed_counter,
                            );
                            break;
                        }
                    };

                    // Pre-dispatch filters.
                    if tag == crate::wire::TAG_PARAMETER_STATUS
                        && allows_unsolicited_param_status(state)
                    {
                        match _parameter_status_admit_leaf::admit_parameter_status_frame(
                            session_params_slot,
                            payload,
                        ) {
                            // Both variants intentionally consumed
                            // here. A future revision could surface
                            // MalformedPayload via a wrapper-advisory
                            // action; until then, the exhaustive
                            // arms pin the discard policy and ensure
                            // any new variant addition fails build.
                            ParamStatusRecordOutcome::Processed
                            | ParamStatusRecordOutcome::MalformedPayload => {}
                        }
                        frames_consumed =
                            frames_consumed.wrapping_add(total_len);
                        continue;
                    }
                    if tag == crate::wire::TAG_NOTICE_RESPONSE
                        && allows_unsolicited_notice_response(state)
                    {
                        _notice_response_admit_leaf::admit_notice_response_frame(
                            session_params_slot,
                        );
                        let notices_arena = notices_arena_or_init(notices_arena_slot);
                        if let Some(notice_ref) =
                            crate::dispatch::parse_and_alloc_notice(payload, notices_arena)
                        {
                            emit_actions!(&mut staged, budget: 1, [
                                StagedAction::Notice { notice_ref },
                            ]);
                        }
                        frames_consumed =
                            frames_consumed.wrapping_add(total_len);
                        continue;
                    }
                    // : NotificationResponse ('A') pre-dispatch
                    // filter. PG §55.7 LISTEN/NOTIFY surface. Frame
                    // body: 4-byte BE pid + CSTR channel + CSTR
                    // payload. Lazy-init the arena (one Box per
                    // LISTEN-using connection), allocate the payload,
                    // stage Action::Notify with the gen-tagged ref.
                    //
                    // Allowed in any post-handshake state — NOTIFY can
                    // arrive at any time (idle, mid-query, mid-row-
                    // stream). No state filter; just parse + admit.
                    // Connecting phase uses an empty transient arena
                    // slot (LISTEN can never be issued pre-handshake
                    // so the filter would not fire there in practice;
                    // the type-level threading is for shared-body
                    // signature compliance).
                    if tag == crate::wire::TAG_NOTIFICATION_RESPONSE {
                        if let Some(parsed) =
                            _notification_response_admit_leaf::parse_notification_payload(payload)
                            && let Some(notif_ref) =
                                _notification_response_admit_leaf::admit_notification_frame(
                                    notifications_arena_slot,
                                    parsed.pid,
                                    parsed.channel,
                                    parsed.payload_bytes,
                                )
                        {
                            emit_actions!(staged, budget: 1, [
                                StagedAction::Notify {
                                    pid: parsed.pid,
                                    notif_ref,
                                },
                            ]);
                        }
                        // Malformed body OR arena cap exhaustion → drop
                        // the frame silently (cold path). Mirror of
                        // `admit_parameter_status_frame`'s
                        // `MalformedPayload` discard policy.
                        frames_consumed =
                            frames_consumed.wrapping_add(total_len);
                        continue;
                    }

                    // Gate uses `MAX_STAGED_PER_CALL` — NOT
                    // `MAX_ACTIONS_PER_CALL`. The two consts differ
                    // (staged-side cap vs output-side cap); using
                    // the output cap here would let `staged`
                    // overflow its own
                    // `heapless::Vec<_, MAX_STAGED_PER_CALL>` and
                    // panic in `emit_actions!`.
                    if staged
                        .len()
                        .saturating_add(WORST_CASE_PER_DISPATCH)
                        > MAX_STAGED_PER_CALL
                    {
                        break;
                    }

                    if tag.byte() == crate::wire::TAG_DATA_ROW.byte()
                        && matches!(*state,
                            ProtoState::SimpleQueryStreamingRows { .. }
                            | ProtoState::BindExecuteStreamingRows { .. }
                            | ProtoState::BindExecuteAwaitingDataOrCompleteSelect { .. })
                    {
                        break;
                    }
                    let outcome = dispatch(
                        state,
                        tag,
                        payload,
                        &mut reserved,
                        terminal_row_desc,
                        terminal_param_oids,
                        terminal_command_tag,
                        terminal_tx_status,
                        error_arena_slot,
                        copy_chunks_arena_slot,
                        command_tags_arena_slot,
                        column_names_slot,
                    );
                    match outcome {
                        DispatchOutcome::AdvancedSilent => {
                            frames_consumed =
                                frames_consumed.wrapping_add(total_len);
                            dispatches_this_call =
                                dispatches_this_call.wrapping_add(1);
                        }
                        DispatchOutcome::AdvancedWithAction { action } => {
                            frames_consumed =
                                frames_consumed.wrapping_add(total_len);
                            dispatches_this_call =
                                dispatches_this_call.wrapping_add(1);
                            emit_actions!(&mut staged, budget: 1, [
                                action,
                            ]);
                        }
                        DispatchOutcome::Errored { reply_id, cause } => {
                            core::hint::cold_path();
                            // .b: StagedAction carries
                            // cause inline; materialise parks it
                            // into the slot when emitting the public
                            // tag+id-only Action::FailReply.
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

        // Apply the staged partial-mode entry, if any.
        if let Some((tag, body_len, body_bytes)) = staged_partial_entry.take() {
            _partial_assembly_dispatch_leaf::enter_partial_assembly_at_dispatch(
                partial_assembly_slot,
                tag,
                body_len,
            );
            if !body_bytes.is_empty() {
                let (_consumed, leftover) =
                    _partial_assembly_dispatch_leaf::absorb_partial_assembly_at_dispatch(
                        partial_assembly_slot,
                        body_bytes,
                    );
                if !leftover.is_empty() {
                    // Architecturally dead: enter_partial sets
                    // body_remaining = body_len, then absorb drains
                    // body_bytes which ≤ body_remaining by construction.
                    // Non-empty leftover = accounting bug → orphaned
                    // wire bytes corrupt subsequent frame parsing.
                    // Classified (CREDO §V: was debug_assert + silent
                    // fallthrough in release). Pushes FailReply +
                    // CloseSocket into staged; materialise at
                    // closure-end converts to public Actions.
                    fail_inflight_no_readbuf(
                        state,
                        ProtocolError::InternalCrateBug {
                            locus: crate::error::CrateBugLocus::PartialModeReentry,
                        },
                        &mut staged,
                        malformed_counter,
                    );
                }
            }
        }

        // Cursor advance IN-SCOPE.
        //
        // Skip advance on Errored transition —
        // `clear_session_residue_if_idle_or_errored` on the NEXT
        // entry call clears the read_buf anyway, so any partial-
        // frame remnant doesn't matter.
        //
        // `advance()` returns `Result<(), AdvancePastEnd>` —
        // architecturally dead post-validated frames_consumed sum,
        // but we classify via InternalCrateBug locus
        // `ReadCursorAdvance` if it ever fires.
        if !matches!(state, ProtoState::Errored(_))
            && frames_consumed > 0
            && read_buf.advance(usize::from(frames_consumed)).is_err()
        {
            core::hint::cold_path();
            fail_inflight_no_readbuf(
                state,
                ProtocolError::InternalCrateBug {
                    locus: crate::error::CrateBugLocus::ReadCursorAdvance,
                },
                &mut staged,
                malformed_counter,
            );
        }

        // : eager `read_buf.clear()` at the install_errored
        // transition site (post-loop).
        //
        // Pre-path: if a dispatch arm installed Errored
        // mid-loop (or the InternalCrateBug branch above triggered
        // fail_inflight_no_readbuf), the read_buf bytes sat in the
        // backing array un-scrubbed until either:
        //   (a) the NEXT `feed_bytes` call hit the `AlreadyErrored`
        //       arm and cleared (window ~one feed_bytes interval), or
        //   (b) `Drop` fired on connection discard.
        //
        // The window left secret-correlated bytes in memory beyond
        // strict need — SCRAM server-frame fragments (`v=<sig>`),
        // ErrorResponse detail strings (may carry server-side
        // operator data), partial frame bodies. The Drop path
        // (P0-C zeroize-on-Drop) was the safety net, but
        // the wrapper layer may delay Drop arbitrarily (connection
        // pool, async handoff). Eager-clear here closes the window.
        //
        // Cost: one `matches!()` branch per feed_bytes (sub-ns on
        // the happy non-Errored path; LLVM elides via cold-path
        // hint) + one `clear()` call on the cold Errored exit path
        // (O(populated_len) memset, ≤4 KiB read_buf cap).
        if matches!(state, ProtoState::Errored(_)) {
            core::hint::cold_path();
            read_buf.clear();
        }

        // : payload data lives in PgProtocol slots —
        // queried by callers via `current_row_desc()` /
        // `current_param_oids()` / `current_command_tag()` accessors
        // on `OutActions<'_>` (and PgProtocol equivalents).
        // Materialise no longer projects per-Reply; the residue-clear
        // and dispatch loop already consumed the slot mut refs above.
        materialise_fn(staged, wb.into_bytes(), fail_cause_slot)
    })
}


/// Per-phase dispatch-context bundle for `<ConnectingPhase>`'s
/// `ConnectingInner`.
///
/// Mirror of [`DispatchContext`] with `state: &mut ConnectingState`
/// instead of `&mut ProtoState`. The other six fields are
/// byte-identical to `DispatchContext`'s; the
/// `feed_bytes_dispatch_connecting` lift+lower wrapper forwards
/// each field unchanged into the inner `DispatchContext`.
///
/// **Two lifetimes** mirror the lifetime split in
/// [`DispatchContext`]: `'state` for the state borrow (lifted-local
/// in `feed_bytes_dispatch_connecting`), `'r` for the other `&mut`
/// field borrows that flow into `OutActions<'_>`.
///
/// **Construction**: only by `ConnectingInner` methods via
/// disjoint-field-borrow from `&mut self`. Free function shape (not
/// a method) per the same rationale as [`DispatchContext`] — tier-1
/// closure by construction (each `&'r mut <FieldType>` must come
/// from somewhere; the only somewhere is `ConnectingInner`'s
/// fields).
//
// `row_desc_slot` is NOT a field here — the cell does not exist on
// the outer for `<ConnectingPhase>` (Extras = ()). The
// [`feed_bytes_dispatch_connecting`] wrapper mints a stack-local
// transient via
// [`_proto_init_leaf::fresh_connecting_transient_row_desc_slot`]
// and threads it into the shared dispatch body's
// `DispatchContext`. The transient is empty (Connecting LHS arms
// never write it) and drops with the wrapper's frame. Trybuild
// compile-fail probe pins the field-absence at the outer
// (PgProtocol<ConnectingPhase>).
pub(in crate::protocol) struct ConnectingDispatchContext<'state, 'r> {
    pub(in crate::protocol) state: &'state mut crate::state::ConnectingState,
    pub(in crate::protocol) read_buf: &'r mut ReadBuf,
    pub(in crate::protocol) session_params:
        &'r mut crate::session_params_slot::SessionParamsCell,
    pub(in crate::protocol) error_arena:
        &'r mut Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
    pub(in crate::protocol) partial_assembly:
        &'r mut crate::partial_assembly::PartialAssemblyCell,
    /// .b: fail_cause_slot threaded from the REAL
    /// `ConnectingInner.fail_cause` field (NOT a transient — must
    /// persist across the wrapper return so callers can query
    /// `pg.fail_cause()` post-FailReply event).
    pub(in crate::protocol) fail_cause_slot:
        &'r mut crate::fail_cause_slot::FailCauseSlotCell,
    pub(in crate::protocol) malformed_count: &'r mut u32,
}

/// Per-phase dispatch entry for `<ConnectingPhase>`'s
/// `ConnectingInner`.
///
/// **Lift+lower wrapper** over [`feed_bytes_dispatch`]. Lifts
/// `ConnectingState → ProtoState` once per call (via
/// [`core::mem::replace`] with a transient sentinel), invokes the
/// shared dispatch body, then projects back via
/// `TryFrom<ProtoState> for ConnectingState`. Single source of
/// truth for the dispatch loop preserved; per-phase wrapper
/// amortises ~5-10 ns of lift+lower overhead over the entire
/// feed-bytes call (vs ~8× cost if the lift+lower fired per-frame).
///
/// **Lifetime decoupling**: the lifted local `proto_state` has
/// lifetime `'tmp` (shorter than caller's `'r`); the inner
/// [`feed_bytes_dispatch`] signature `<'w, 'state, 'r, BOUNDED>`
/// accepts `'state = 'tmp` while preserving `'r = 'r_outer` for the
/// `OutActions<'w>` return. Without the independent
/// `'state` / `'r` split this wrapper would be inexpressible
/// (lifetime-widening 'tmp → 'r is contravariant, requires
/// `mem::transmute` — forbidden under `#![forbid(unsafe_code)]`).
///
/// **Tier-1 closure at the per-phase boundary**: caller (typically
/// `ConnectingInner.feed_bytes_impl`) holds a
/// `ConnectingState`-typed reference. The type system prevents the
/// caller from constructing a non-Connecting state outside this
/// wrapper. The lift widens to `ProtoState` only INSIDE this
/// function for the shared dispatch's exhaustive-match arms; the
/// lower projects back to `ConnectingState` before returning.
///
/// **HandshakeReady transition signal**: the only legitimate non-
/// Connecting outcome the shared dispatch can produce from a
/// Connecting LHS is `ProtoState::Idle` (post-handshake success
/// from the `(PostAuthHaveKey, RFQ)` arm). The lower step catches
/// this and translates to
/// [`crate::state::ConnectingState::HandshakeReady`] — the signal
/// `<ConnectingPhase>::into_active` observes.
///
/// **Sentinel choice during lift**: a transient
/// `ConnectingState::Errored(Framing)` placeholder fills the state
/// slot for the lift window. Kind doesn't matter semantically — the
/// slot is always overwritten in the lower step (either with the
/// legitimate new state, with `HandshakeReady`, or with the
/// defensive `Errored(Internal)` on a dispatch bug). The
/// placeholder is unobservable: no reader sees the state between
/// `mem::replace` and the lower-step write.
///
/// **perf-recovery** (2026-05-23): `#[inline(always)]` —
/// the handshake hot path (push_command/ping bench fixture's
/// `fresh_active_via_trust_handshake` = 5 `advance_one_frame`
/// calls during the AuthOk/ParameterStatus×2/BackendKeyData/RFQ
/// sequence) routes through this wrapper. Bisect confirmed
/// regression at `0597dae` (Phase 2 SealedPhase flip):
/// pre-flip push_command/ping 49 ns → post-flip 77 ns = +28 ns =
/// ~5.6 ns × 5 calls. Inlining lets LLVM fuse the lift+dispatch+lower
/// into the calling hot path, eliminating the per-call lift/lower
/// overhead.
#[inline(always)]
pub(in crate::protocol) fn feed_bytes_dispatch_connecting<'w, 'r, const BOUNDED: bool>(
    ctx: ConnectingDispatchContext<'_, 'r>,
    bytes: &[u8],
    write_buf: &'w mut WriteBuf,
    max_dispatches: u16,
) -> OutActions<'w> {
    use crate::state::{ConnectingState, WrongPhase};

    let ConnectingDispatchContext {
        state,
        read_buf,
        session_params,
        error_arena,
        partial_assembly,
        fail_cause_slot,
        malformed_count,
    } = ctx;

    // `<ConnectingPhase>::Extras = ()` — no row_desc_slot on the
    // outer. Mint a stack-local transient via the leaf-private
    // constructor for the duration of the shared dispatch call.
    // The transient is empty; no Connecting LHS arm writes to it;
    // it drops with this frame. Tier-1 closure on the outer
    // (PgProtocol<ConnectingPhase> has no row_desc_slot field);
    // the transient is wrapper-internal scaffolding the shared
    // dispatch body can name.
    //
    // **Lifetime safety**: the inner `DispatchContext.row_desc_slot`
    // is `&'r mut RowDescSlotCell` — `'r` here is the wrapper's
    // outer lifetime, BUT the borrow only flows out via
    // `OutActions<'_>` if `terminal_ref: Option<&'r RowDesc>`
    // is `Some`. The transient is always empty (Connecting LHS
    // never writes), so `terminal_ref = None` for the whole call;
    // no `'r` borrow escapes via the return value. The
    // `&mut transient` reborrow needed by `feed_bytes_dispatch`
    // is supplied by reborrowing the local — Rust's NLL closes
    // the borrow at function return; the local drops cleanly.
    let mut transient_extras =
        _proto_init_leaf::fresh_connecting_transient_extras();
    // : Connecting phase doesn't carry a notifications_arena
    // (LISTEN/NOTIFY is post-handshake only — PG §55.7 NOTIFY can
    // arrive only in Active phase). Provide a transient empty slot
    // for DispatchContext threading; the pre-dispatch filter on
    // 'A' tag would never fire in Connecting state (no LISTEN
    // command can have been issued pre-handshake), but the slot
    // must exist for the shared dispatch body's type signature.
    let mut transient_notifications_arena:
        Option<alloc::boxed::Box<crate::notifications_arena::NotificationsArena>> = None;
    let mut transient_notices_arena:
        Option<alloc::boxed::Box<crate::notices_arena::NoticesArena>> = None;
    let mut transient_copy_chunks_arena:
        Option<alloc::boxed::Box<crate::copy_chunks_arena::CopyChunksArena>> = None;
    // : intermediate command-tags arena transient for
    // Connecting (multi-statement SimpleQuery is
    // post-handshake; ICC never fires in Connecting state).
    let mut transient_command_tags_arena:
        Option<alloc::boxed::Box<crate::command_tags_arena::CommandTagsArena>> = None;
    let mut transient_column_names: Option<alloc::boxed::Box<[alloc::string::String]>> = None;

    let sentinel = ConnectingState::Errored(
        crate::error::StateErrorKind::from_kind_or_internal(
            crate::error::ErrorKind::Framing,
        ),
    );
    let lifted: ConnectingState = core::mem::replace(state, sentinel);
    let mut proto_state: ProtoState = lifted.into();

    // No-schema closure: the closure ignores `terminal_ref`
    // (architecturally `None` for Connecting LHS — no arm writes
    // the slot) and calls `materialise` with
    // `None::<&'static RowDesc>`. R is inferred as
    // `OutActions<'w>`; covariance lets the wrapper's
    // outer signature `OutActions<'w>` accept it (since
    // `'static: 'r` for any `'r`). The transient slot's local
    // lifetime never leaks into R.
    let actions: OutActions<'w> = feed_bytes_dispatch::<_, _, BOUNDED>(
        DispatchContext {
            state: &mut proto_state,
            read_buf,
            row_desc_slot: &mut transient_extras.row_desc,
            param_oids_slot: &mut transient_extras.param_oids,
            command_tag_slot: &mut transient_extras.command_tag,
            tx_status_slot: &mut transient_extras.tx_status,
            // .b: thread the REAL ConnectingInner.fail_cause
            // (NOT transient_extras.fail_cause) so the parked cause
            // persists across the wrapper return — callers can query
            // `pg.fail_cause()` post-FailReply.
            fail_cause_slot,
            session_params,
            error_arena,
            notifications_arena: &mut transient_notifications_arena,
            notices_arena: &mut transient_notices_arena,
            copy_chunks_arena: &mut transient_copy_chunks_arena,
            command_tags_arena: &mut transient_command_tags_arena,
            partial_assembly,
            malformed_count,
            column_names: &mut transient_column_names,
        },
        bytes,
        write_buf,
        max_dispatches,
        |staged, write_bytes, fail_cause_slot| -> OutActions<'w> {
            // : closure no longer takes Reply payload
            // slot args. .b: `fail_cause_slot` threaded
            // through so materialise can park the cause at the
            // StagedAction → Action transformation boundary.
            materialise(staged, write_bytes, fail_cause_slot)
        },
    );

    // Lower: project proto_state back to ConnectingState. The
    // `TryFrom<ProtoState> for ConnectingState` impl handles
    // `ProtoState::HandshakeReady { pid, secret_key }` directly
    // (Ok arm) — the post-handshake transition signal flows through
    // the standard projection now that the variant carries its
    // payload.
    match ConnectingState::try_from(proto_state) {
        Ok(cs) => *state = cs,
        Err(WrongPhase { recovered }) => {
            // Defensive arm — dispatch produced a state shape not
            // reachable from a Connecting LHS under current dispatch
            // arms. Drain any `ReplyId<K>` carried in the variant
            // (Drop-guard discipline; `match { Some(_) | None => {} }`
            // form per crate convention) and tear down to Errored.
            match recovered.take_inflight_reply_raw_id() {
                Some(_) | None => {}
            }
            *state = ConnectingState::Errored(
                crate::error::StateErrorKind::from_kind_or_internal(
                    crate::error::ErrorKind::Internal,
                ),
            );
        }
    }

    actions
}

/// Per-phase dispatch-context bundle for `<ActivePhase>`'s
/// `ActiveInner`.
///
/// Mirror of [`DispatchContext`] with `state: &mut ActiveState`
/// instead of `&mut ProtoState`. The other seven fields are
/// byte-identical (ActiveInner has the full 8-field post-handshake
/// shape — every cell is reachable from at least one Active
/// variant).
///
/// **Two lifetimes** mirror the lifetime split: `'state` for the
/// state borrow (lifted-local in `feed_bytes_dispatch_active`),
/// `'r` for the seven other `&mut` field borrows that flow into
/// `OutActions<'_>` via `materialise`.
///
/// **Construction**: only by `ActiveInner` methods via
/// disjoint-field-borrow from `&mut self`. Tier-1 closure by
/// construction — only code that already has eight disjoint `&mut`
/// refs to the right field types can call the wrapper.
pub(in crate::protocol) struct ActiveDispatchContext<'state, 'r> {
    pub(in crate::protocol) state: &'state mut crate::state::ActiveState,
    pub(in crate::protocol) read_buf: &'r mut ReadBuf,
    pub(in crate::protocol) row_desc_slot: &'r mut crate::schema_slot::RowDescSlotCell,
    /// : param_oids_slot threaded from
    /// `<ActivePhase>::Extras.param_oids` into the shared dispatch
    /// body's `DispatchContext`. DescribeStatement is post-handshake
    /// only — only Active carries the populated slot; Connecting
    /// uses a transient empty slot at the wrapper level.
    pub(in crate::protocol) param_oids_slot:
        &'r mut crate::param_oids_slot::ParamOidsSlotCell,
    /// : command_tag_slot threaded from
    /// `<ActivePhase>::Extras.command_tag`.
    pub(in crate::protocol) command_tag_slot:
        &'r mut crate::command_tag_slot::CommandTagSlotCell,
    /// : tx_status_slot threaded from
    /// `<ActivePhase>::Extras.tx_status`.
    pub(in crate::protocol) tx_status_slot:
        &'r mut crate::tx_status_slot::TxStatusSlotCell,
    /// .b: fail_cause_slot threaded from
    /// `<ActivePhase>::Extras.fail_cause`.
    pub(in crate::protocol) fail_cause_slot:
        &'r mut crate::fail_cause_slot::FailCauseSlotCell,
    pub(in crate::protocol) session_params:
        &'r mut crate::session_params_slot::SessionParamsCell,
    pub(in crate::protocol) error_arena:
        &'r mut Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
    /// : notifications_arena threaded from
    /// `ActiveInner.notifications_arena` into the shared dispatch
    /// body's `DispatchContext`. LISTEN/NOTIFY is post-handshake
    /// only — only Active carries this field; Connecting uses a
    /// transient empty slot at the wrapper level.
    pub(in crate::protocol) notifications_arena:
        &'r mut Option<alloc::boxed::Box<crate::notifications_arena::NotificationsArena>>,
    pub(in crate::protocol) notices_arena:
        &'r mut Option<alloc::boxed::Box<crate::notices_arena::NoticesArena>>,
    pub(in crate::protocol) copy_chunks_arena:
        &'r mut Option<alloc::boxed::Box<crate::copy_chunks_arena::CopyChunksArena>>,
    /// : command_tags_arena threaded from
    /// `ActiveInner.command_tags_arena` into the shared dispatch
    /// body's `DispatchContext`. Used by multi-statement
    /// dispatch arms to externalise the prior command tag for
    /// `Action::IntermediateCommandComplete`.
    pub(in crate::protocol) command_tags_arena:
        &'r mut Option<alloc::boxed::Box<crate::command_tags_arena::CommandTagsArena>>,
    pub(in crate::protocol) partial_assembly:
        &'r mut crate::partial_assembly::PartialAssemblyCell,
    pub(in crate::protocol) malformed_count: &'r mut u32,
    pub(in crate::protocol) column_names:
        &'r mut Option<alloc::boxed::Box<[alloc::string::String]>>,
}

/// Per-phase dispatch entry for `<ActivePhase>`'s `ActiveInner`.
///
/// **Lift+lower wrapper** over [`feed_bytes_dispatch`] — mirror of
/// [`feed_bytes_dispatch_connecting`]. Lifts `ActiveState → ProtoState`
/// once per call via `mem::replace` + `Into`; invokes the shared
/// dispatch body; projects back via `TryFrom<ProtoState> for ActiveState`.
///
/// **No transition-signal translation needed**: unlike Connecting's
/// `Idle → HandshakeReady` mapping (which encoded the post-
/// handshake transition signal), Active's lower step is a direct
/// projection. The only legitimate ProtoState outcomes from an
/// Active LHS are Active-mirror variants — anything else is a
/// dispatch bug.
///
/// **Sentinel choice during lift**: `ActiveState::Errored(Framing)`
/// as a transient placeholder; always overwritten by the lower
/// step before any reader sees it.
///
/// **perf-recovery** (2026-05-23): `#[inline(always)]`
/// — without inlining the wrapper, the per-call lift+lower (2 enum
/// matches × ~25 arms each) accounts for ~5.6 ns per call on the
/// `advance_one_frame` hot path. push_command/ping bench (5 calls
/// per handshake fixture iter) regressed +28 ns vs pre-/// baseline (`5d59b64`: 49 ns → `0597dae`: 77 ns). Inlining
/// lets LLVM fuse the lift+dispatch+lower into a single optimised
/// pattern, eliding the redundant variant-conversion math when
/// the caller's state slot type-stabilises. The function is large
/// (~150 LoC body via shared `feed_bytes_dispatch`), but with LTO
/// fat + codegen-units=1 the inlined copies dedup. Asm-verified at
/// the recovery commit.
#[inline(always)]
pub(in crate::protocol) fn feed_bytes_dispatch_active<'w, 'r, const BOUNDED: bool>(
    ctx: ActiveDispatchContext<'_, 'r>,
    bytes: &[u8],
    write_buf: &'w mut WriteBuf,
    max_dispatches: u16,
) -> OutActions<'w> {
    use crate::state::{ActiveState, WrongPhase};

    let ActiveDispatchContext {
        state,
        read_buf,
        row_desc_slot,
        param_oids_slot,
        command_tag_slot,
        tx_status_slot,
        fail_cause_slot,
        session_params,
        error_arena,
        notifications_arena,
        notices_arena,
        copy_chunks_arena,
        command_tags_arena,
        partial_assembly,
        malformed_count,
        column_names,
    } = ctx;

    let sentinel = ActiveState::Errored(
        crate::error::StateErrorKind::from_kind_or_internal(
            crate::error::ErrorKind::Framing,
        ),
    );
    let lifted: ActiveState = core::mem::replace(state, sentinel);
    let mut proto_state: ProtoState = lifted.into();

    // With-schema closure: reads the slot's RowDesc as
    // `Option<&'r RowDesc>` and forwards to `materialise`; R is
    // inferred as `OutActions<'w>`.
    let actions = feed_bytes_dispatch::<_, _, BOUNDED>(
        DispatchContext {
            state: &mut proto_state,
            read_buf,
            row_desc_slot,
            param_oids_slot,
            command_tag_slot,
            tx_status_slot,
            fail_cause_slot,
            session_params,
            error_arena,
            notifications_arena,
            notices_arena,
            copy_chunks_arena,
            command_tags_arena,
            partial_assembly,
            malformed_count,
            column_names,
        },
        bytes,
        write_buf,
        max_dispatches,
        |staged, write_bytes, fail_cause_slot| -> OutActions<'w> {
            // : closure no longer takes Reply payload
            // slot args. .b: `fail_cause_slot` threaded
            // through so materialise can park the cause at the
            // StagedAction → Action transformation boundary.
            materialise(staged, write_bytes, fail_cause_slot)
        },
    );

    // Lower: project proto_state back to ActiveState. Any
    // non-Active outcome (Connecting* variants) is a dispatch
    // bug — defensive Errored tears down the connection after
    // draining any carried ReplyId<K>.
    match ActiveState::try_from(proto_state) {
        Ok(cs) => *state = cs,
        Err(WrongPhase { recovered }) => {
            core::hint::cold_path();
            match recovered.take_inflight_reply_raw_id() {
                Some(_) | None => {}
            }
            *state = ActiveState::Errored(
                crate::error::StateErrorKind::from_kind_or_internal(
                    crate::error::ErrorKind::Internal,
                ),
            );
        }
    }

    actions
}

/// Per-phase single-frame variant over [`ActiveDispatchContext`].
///
/// **Active-specific fast paths**:
/// - `ActiveState::SimpleQueryStreamingRows { .. }` or
///   `ActiveState::BindExecuteStreamingRows { .. }` →
///   [`FeedEvent::StreamingRows`].
/// - `ActiveState::Errored(_)` → [`FeedEvent::Close`].
/// - `ActiveState::Idle` + empty `read_buf` → [`FeedEvent::Idle`].
///
/// All other Active variants delegate to
/// [`feed_bytes_dispatch_active::<true>`] with
/// `max_dispatches = 1`.
#[must_use = "FeedEvent variants carry side-effect contracts: \
              SendBytes/Deliver MUST be processed; Fail/Close MUST \
              trigger socket teardown"]
#[inline(always)]
pub(in crate::protocol) fn advance_one_frame_dispatch_active<'w, 'r>(
    ctx: ActiveDispatchContext<'_, 'r>,
    write_buf: &'w mut WriteBuf,
) -> crate::action::FeedEvent<'w> {
    use crate::action::{Action, FeedEvent};
    use crate::state::ActiveState;

    if matches!(
        *ctx.state,
        ActiveState::SimpleQueryStreamingRows { .. }
            | ActiveState::BindExecuteStreamingRows { .. }
    ) {
        return FeedEvent::StreamingRows;
    }

    if matches!(*ctx.state, ActiveState::Errored(_)) {
        return FeedEvent::Close;
    }

    if matches!(*ctx.state, ActiveState::Idle) && ctx.read_buf.is_unread_empty() {
        return FeedEvent::Idle;
    }

    let actions = feed_bytes_dispatch_active::<true>(ctx, b"", write_buf, 1);

    match actions.as_slice() {
        [] => FeedEvent::NeedMoreBytes,
        [Action::SendBytes(bytes)] => FeedEvent::SendBytes(bytes),
        [Action::DeliverReply { id, value }] => FeedEvent::Deliver(*id, *value),
        [Action::FailReply { id }, ..] => FeedEvent::Fail(*id),
        [Action::CloseSocket] => FeedEvent::Close,
        [Action::Notice { notice_ref }] => FeedEvent::Notice(*notice_ref),
        [Action::Notify { pid, notif_ref }] => FeedEvent::Notify { pid: *pid, notif_ref: *notif_ref },
        _ => FeedEvent::Close,
    }
}

/// Per-phase single-frame variant over
/// [`ConnectingDispatchContext`].
///
/// **Connecting-specific fast paths**:
/// - No `StreamingRows` equivalent in `ConnectingState`.
/// - `ConnectingState::Errored(_)` → [`FeedEvent::Close`].
/// - `ConnectingState::HandshakeReady` + empty `read_buf` →
///   [`FeedEvent::Idle`] (HandshakeReady is the per-phase
///   representation of post-handshake Idle, with the caller-visible
///   expectation that the next user action is `into_active()`).
///
/// All other Connecting variants delegate to
/// [`feed_bytes_dispatch_connecting::<true>`] with
/// `max_dispatches = 1`.
#[must_use = "FeedEvent variants carry side-effect contracts: \
              SendBytes/Deliver MUST be processed; Fail/Close MUST \
              trigger socket teardown"]
#[inline(always)]
pub(in crate::protocol) fn advance_one_frame_dispatch_connecting<'w, 'r>(
    ctx: ConnectingDispatchContext<'_, 'r>,
    write_buf: &'w mut WriteBuf,
) -> crate::action::FeedEvent<'w> {
    use crate::action::{Action, FeedEvent};
    use crate::state::ConnectingState;

    if matches!(*ctx.state, ConnectingState::Errored(_)) {
        return FeedEvent::Close;
    }

    if matches!(*ctx.state, ConnectingState::HandshakeReady { .. })
        && ctx.read_buf.is_unread_empty()
    {
        return FeedEvent::Idle;
    }

    let actions = feed_bytes_dispatch_connecting::<true>(ctx, b"", write_buf, 1);

    match actions.as_slice() {
        [] => FeedEvent::NeedMoreBytes,
        [Action::SendBytes(bytes)] => FeedEvent::SendBytes(bytes),
        [Action::DeliverReply { id, value }] => FeedEvent::Deliver(*id, *value),
        [Action::FailReply { id }, ..] => FeedEvent::Fail(*id),
        [Action::CloseSocket] => FeedEvent::Close,
        [Action::Notice { notice_ref }] => FeedEvent::Notice(*notice_ref),
        [Action::Notify { pid, notif_ref }] => FeedEvent::Notify { pid: *pid, notif_ref: *notif_ref },
        _ => FeedEvent::Close,
    }
}


/// Per-phase Inner API surface for `<ConnectingPhase>`.
///
/// **Method shape**: each method mirrors the same-named method on
/// `ActiveInner` with two differences:
/// 1. State writes go through `ConnectingState` not `ProtoState`
///    (`matches!(self.state, ConnectingState::Errored(_))` etc.)
/// 2. Dispatch delegates route through the lift+lower wrappers
///    (`feed_bytes_dispatch_connecting` /
///    `advance_one_frame_dispatch_connecting`).
impl ConnectingInner {
    /// Per-phase saturation classifier mutation surface.
    ///
    /// Routes through the same token-gated
    /// [`_replyid_saturation_drain_leaf::drain`] via lift+lower
    /// (the drain expects `&mut ProtoState`; here state is
    /// `ConnectingState`). The lift+lower preserves the token-gated
    /// tier-1 closure on construction of `FeedStateSetter` — same
    /// leaf submodule, same token mint guard.
    #[cold]
    #[inline(never)]
    pub(crate) fn install_errored_replyid_saturation(&mut self) {
        use crate::state::{ConnectingState, WrongPhase};
        if matches!(self.state, ConnectingState::Errored(_)) {
            return;
        }
        let cause = crate::error::ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::ReplyIdSaturation,
        };
        let kind = cause.state_kind();

        let sentinel = ConnectingState::Errored(kind);
        let lifted = core::mem::replace(&mut self.state, sentinel);
        let mut proto_state: ProtoState = lifted.into();
        // Drain return is the in-flight reply id, deliberately
        // discarded: saturation has no FailReply emission context
        // (architecturally dead under the 2^64-saturated counter). The
        // explicit `Some(_) | None => {}` arm satisfies the leaf's
        // `#[must_use]` without re-introducing the banned
        // `let _drained = ...;` underscore-bind form.
        match _replyid_saturation_drain_leaf::drain(&mut proto_state, kind) {
            Some(_) | None => {}
        }
        match ConnectingState::try_from(proto_state) {
            Ok(cs) => self.state = cs,
            Err(WrongPhase { recovered }) => {
                // Drain always sets `ProtoState::Errored(kind)` → maps
                // to `ConnectingState::Errored(kind)`. This arm is dead
                // under the drain's contract. Keep sentinel; drain any
                // residual ReplyId from `recovered` for Drop-guard
                // discipline (no-op for already-Errored).
                match recovered.take_inflight_reply_raw_id() {
                    Some(_) | None => {}
                }
            }
        }
    }

    /// Mint a fresh ReplyId during Connecting.
    ///
    /// Shares the static atomic counter
    /// `super::PROCESS_REPLY_ID_COUNTER` with the other phases'
    /// mint sites — process-global uniqueness. Cold saturation
    /// classifier routes to
    /// [`Self::install_errored_replyid_saturation`].
    #[inline]
    pub(crate) fn next_reply_id<K: crate::reply_id::ReplyKind>(
        &mut self,
    ) -> crate::reply_id::ReplyId<K> {
        use core::sync::atomic::Ordering;
        let raw_old = PROCESS_REPLY_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        if raw_old == u64::MAX {
            self.install_errored_replyid_saturation();
        }
        let nz = crate::reply_id::saturating_inc_to_nonzero(raw_old);
        crate::reply_id::ReplyId::from_raw(nz)
    }

    /// feed_bytes dispatch loop for Connecting.
    ///
    /// Thin delegate to the free function
    /// [`feed_bytes_dispatch_connecting::<BOUNDED>`] (lift+lower
    /// wrapper). Builds [`ConnectingDispatchContext`] from
    /// `&mut self.<field>` via disjoint-field-borrow (Rust 2018+).
    /// LLVM inlines this delegate unconditionally.
    pub(crate) fn feed_bytes_impl<'w, const BOUNDED: bool>(
        &mut self,
        bytes: &[u8],
        write_buf: &'w mut WriteBuf,
        max_dispatches: u16,
    ) -> OutActions<'w> {
        feed_bytes_dispatch_connecting::<BOUNDED>(
            ConnectingDispatchContext {
                state: &mut self.state,
                read_buf: &mut self.read_buf,
                session_params: &mut self.session_params,
                error_arena: &mut self.error_arena,
                partial_assembly: &mut self.partial_assembly,
                // .b: thread the REAL ConnectingInner.fail_cause
                // (lives directly on Inner; ConnectingPhase has no Extras).
                fail_cause_slot: &mut self.fail_cause,
                malformed_count: &mut self.malformed_frame_count,
            },
            bytes,
            write_buf,
            max_dispatches,
        )
    }

    /// Append inbound bytes (no dispatch).
    ///
    /// Mirror of the Active-phase `feed_inbound` body with
    /// `ProtoState::Errored(k) => *k` replaced by
    /// `ConnectingState::Errored(k) => *k` in the
    /// `ConnectionAlreadyClosed { prior_kind }` reconstruction.
    pub(crate) fn feed_inbound(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), crate::error::ProtocolError> {
        use crate::state::ConnectingState;
        if matches!(self.state, ConnectingState::Errored(_)) {
            core::hint::cold_path();
            return Err(crate::error::ProtocolError::ConnectionAlreadyClosed {
                prior_kind: match &self.state {
                    ConnectingState::Errored(k) => *k,
                    // Tier-1 by match-guard above: dead arm.
                    _ => crate::error::ProtocolError::InternalCrateBug {
                        locus: crate::error::CrateBugLocus::ReadCursorAdvance,
                    }.state_kind(),
                },
            });
        }
        if self.partial_assembly.is_active() {
            core::hint::cold_path();
            let (_consumed, leftover) = _partial_assembly_dispatch_leaf::absorb_partial_assembly_at_dispatch(
                &mut self.partial_assembly,
                bytes,
            );
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

    /// Single-frame advance for Connecting.
    ///
    /// Thin delegate to [`advance_one_frame_dispatch_connecting`].
    #[must_use = "FeedEvent variants carry side-effect contracts: \
                  SendBytes/Deliver MUST be processed; Fail/Close MUST \
                  trigger socket teardown"]
    pub(crate) fn advance_one_frame<'w>(
        &mut self,
        write_buf: &'w mut WriteBuf,
    ) -> crate::action::FeedEvent<'w> {
        advance_one_frame_dispatch_connecting(
            ConnectingDispatchContext {
                state: &mut self.state,
                read_buf: &mut self.read_buf,
                session_params: &mut self.session_params,
                error_arena: &mut self.error_arena,
                partial_assembly: &mut self.partial_assembly,
                fail_cause_slot: &mut self.fail_cause,
                malformed_count: &mut self.malformed_frame_count,
            },
            write_buf,
        )
    }
}

/// Manual Debug impl for `ConnectingInner`.
///
/// Sensitive-redaction surface: state / read_buf / session_params
/// are emitted via `finish_non_exhaustive()`. session_params'
/// SecretBoundedStr Display redacts; state's SCRAM secret variants
/// Display redact via state.rs's manual Debug arms.
impl core::fmt::Debug for ConnectingInner {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        static EMPTY: SessionParams = SessionParams::new();
        let session_params: &SessionParams = match self.session_params.as_deref() {
            Some(p) => p,
            None => &EMPTY,
        };
        f.debug_struct("PgProtocol")
            .field("state", &self.state)
            .field("read_buf", &self.read_buf)
            .field("session_params", session_params)
            .finish_non_exhaustive()
    }
}

/// Per-phase Inner API surface for `<ActivePhase>`. Mirror of
/// `impl ConnectingInner` with `state` typed `ActiveState`.
impl ActiveInner {
    /// Per-phase saturation classifier. Routes through the same
    /// token-gated [`_replyid_saturation_drain_leaf::drain`] via
    /// lift+lower as the Connecting mirror.
    #[cold]
    #[inline(never)]
    pub(crate) fn install_errored_replyid_saturation(&mut self) {
        use crate::state::{ActiveState, WrongPhase};
        if matches!(self.state, ActiveState::Errored(_)) {
            return;
        }
        let cause = crate::error::ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::ReplyIdSaturation,
        };
        let kind = cause.state_kind();

        let sentinel = ActiveState::Errored(kind);
        let lifted = core::mem::replace(&mut self.state, sentinel);
        let mut proto_state: ProtoState = lifted.into();
        // Drain return is the in-flight reply id, deliberately
        // discarded: saturation has no FailReply emission context.
        // Explicit `Some(_) | None => {}` arm satisfies `#[must_use]`
        // without the banned `let _drained = ...;` underscore-bind.
        match _replyid_saturation_drain_leaf::drain(&mut proto_state, kind) {
            Some(_) | None => {}
        }
        match ActiveState::try_from(proto_state) {
            Ok(cs) => self.state = cs,
            Err(WrongPhase { recovered }) => {
                // Drain always produces ProtoState::Errored(kind)
                // which projects cleanly to ActiveState::Errored.
                // Defensive: drain any residual ReplyId from
                // `recovered` (no-op on Errored).
                match recovered.take_inflight_reply_raw_id() {
                    Some(_) | None => {}
                }
            }
        }
    }

    /// Mint a fresh ReplyId.
    ///
    /// Uses the shared `super::PROCESS_REPLY_ID_COUNTER` static
    /// atomic — process-global uniqueness across all phases.
    #[inline]
    pub(crate) fn next_reply_id<K: crate::reply_id::ReplyKind>(
        &mut self,
    ) -> crate::reply_id::ReplyId<K> {
        use core::sync::atomic::Ordering;
        let raw_old = PROCESS_REPLY_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        if raw_old == u64::MAX {
            self.install_errored_replyid_saturation();
        }
        let nz = crate::reply_id::saturating_inc_to_nonzero(raw_old);
        crate::reply_id::ReplyId::from_raw(nz)
    }

    /// feed_bytes dispatch loop for Active. Thin delegate to
    /// [`feed_bytes_dispatch_active::<BOUNDED>`].
    ///
    /// `row_desc_slot` lives on outer `<ActivePhase>::Extras`, not
    /// on `ActiveInner`; the caller (`PgProtocol<ActivePhase>`
    /// method) sources from `&mut self.extras` via disjoint-field
    /// borrow and passes here.
    pub(crate) fn feed_bytes_impl<'w, 'r, const BOUNDED: bool>(
        &'r mut self,
        extras: &'r mut ActiveExtras,
        bytes: &[u8],
        write_buf: &'w mut WriteBuf,
        max_dispatches: u16,
    ) -> OutActions<'w> {
        feed_bytes_dispatch_active::<BOUNDED>(
            ActiveDispatchContext {
                state: &mut self.state,
                read_buf: &mut self.read_buf,
                row_desc_slot: &mut extras.row_desc,
                param_oids_slot: &mut extras.param_oids,
                command_tag_slot: &mut extras.command_tag,
                tx_status_slot: &mut extras.tx_status,
                // .b: thread the real ActiveExtras.fail_cause.
                fail_cause_slot: &mut extras.fail_cause,
                session_params: &mut self.session_params,
                error_arena: &mut self.error_arena,
                notifications_arena: &mut self.notifications_arena,
                notices_arena: &mut self.notices_arena,
                copy_chunks_arena: &mut self.copy_chunks_arena,
                command_tags_arena: &mut self.command_tags_arena,
                partial_assembly: &mut self.partial_assembly,
                malformed_count: &mut self.malformed_frame_count,
                column_names: &mut extras.column_names,
            },
            bytes,
            write_buf,
            max_dispatches,
        )
    }

    /// Frame-bounded feed_bytes.
    #[inline]
    pub(crate) fn feed_bytes_bounded<'w, 'r>(
        &'r mut self,
        extras: &'r mut ActiveExtras,
        bytes: &[u8],
        write_buf: &'w mut WriteBuf,
        max_dispatches: u16,
    ) -> OutActions<'w> {
        self.feed_bytes_impl::<true>(extras, bytes, write_buf, max_dispatches)
    }

    /// Append inbound bytes (no dispatch). Mirror of the
    /// Connecting variant with `ConnectingState::Errored(k) => *k`
    /// replaced by `ActiveState::Errored(k) => *k`.
    pub(crate) fn feed_inbound(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), crate::error::ProtocolError> {
        use crate::state::ActiveState;
        if matches!(self.state, ActiveState::Errored(_)) {
            core::hint::cold_path();
            return Err(crate::error::ProtocolError::ConnectionAlreadyClosed {
                prior_kind: match &self.state {
                    ActiveState::Errored(k) => *k,
                    _ => crate::error::ProtocolError::InternalCrateBug {
                        locus: crate::error::CrateBugLocus::ReadCursorAdvance,
                    }.state_kind(),
                },
            });
        }
        if self.partial_assembly.is_active() {
            core::hint::cold_path();
            let (_consumed, leftover) = _partial_assembly_dispatch_leaf::absorb_partial_assembly_at_dispatch(
                &mut self.partial_assembly,
                bytes,
            );
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

    /// Single-frame advance. Thin delegate to
    /// [`advance_one_frame_dispatch_active`].
    #[must_use = "FeedEvent variants carry side-effect contracts: \
                  SendBytes/Deliver MUST be processed; Fail/Close MUST \
                  trigger socket teardown"]
    pub(crate) fn advance_one_frame<'w, 'r>(
        &'r mut self,
        extras: &'r mut ActiveExtras,
        write_buf: &'w mut WriteBuf,
    ) -> crate::action::FeedEvent<'w> {
        advance_one_frame_dispatch_active(
            ActiveDispatchContext {
                state: &mut self.state,
                read_buf: &mut self.read_buf,
                row_desc_slot: &mut extras.row_desc,
                param_oids_slot: &mut extras.param_oids,
                command_tag_slot: &mut extras.command_tag,
                tx_status_slot: &mut extras.tx_status,
                // .b: thread the real ActiveExtras.fail_cause.
                fail_cause_slot: &mut extras.fail_cause,
                session_params: &mut self.session_params,
                error_arena: &mut self.error_arena,
                notifications_arena: &mut self.notifications_arena,
                notices_arena: &mut self.notices_arena,
                copy_chunks_arena: &mut self.copy_chunks_arena,
                command_tags_arena: &mut self.command_tags_arena,
                partial_assembly: &mut self.partial_assembly,
                malformed_count: &mut self.malformed_frame_count,
                column_names: &mut extras.column_names,
            },
            write_buf,
        )
    }

    /// Per-phase residue clear. Thin delegate to
    /// [`clear_session_residue_for_class_dispatch`] — the helper
    /// is state-agnostic (operates only on the four cell `&mut`s +
    /// class). `#[inline]` so the inliner can specialise to a
    /// const-class argument and elide the 5-arm dispatch when the
    /// class is statically known at the call site.
    ///
    /// `row_desc_slot` lives on outer `<ActivePhase>::Extras`, not
    /// on `ActiveInner`; this method takes the cell as a `&mut`
    /// parameter sourced from the outer [`PgProtocol::extras`] by
    /// the caller.
    #[inline]
    pub(crate) fn clear_session_residue_for_class(
        &mut self,
        extras: &mut ActiveExtras,
        class: crate::state::StatePushClass,
    ) {
        // perf-recovery: `extras.fail_cause` is NOT
        // passed — the clear is provably dead in the Idle arm (see
        // dispatch fn docstring + `into_active` invariant note) and
        // the Errored arm never clears it. The 12-arg → 11-arg ABI
        // shrink eliminates one stack push at the push hot path,
        // restoring the pre-slot call-site shape.
        clear_session_residue_for_class_dispatch(
            &mut extras.row_desc,
            &mut extras.param_oids,
            &mut extras.command_tag,
            &mut extras.tx_status,
            &mut self.session_params,
            &mut self.error_arena,
            &mut self.notifications_arena,
            &mut self.copy_chunks_arena,
            &mut self.command_tags_arena,
            &mut self.partial_assembly,
            class,
        );
        if matches!(class,
            crate::state::StatePushClass::Idle | crate::state::StatePushClass::Errored(_))
        {
            extras.column_names = None;
        }
    }
}

/// Manual Debug impl for `ActiveInner`. Three-field
/// `finish_non_exhaustive` (state / read_buf / session_params)
/// matching the Connecting mirror.
impl core::fmt::Debug for ActiveInner {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        static EMPTY: SessionParams = SessionParams::new();
        let session_params: &SessionParams = match self.session_params.as_deref() {
            Some(p) => p,
            None => &EMPTY,
        };
        f.debug_struct("PgProtocol")
            .field("state", &self.state)
            .field("read_buf", &self.read_buf)
            .field("session_params", session_params)
            .finish_non_exhaustive()
    }
}

// `impl PgProtocol<ActivePhase>` re-opens here for the remaining
// `<ActivePhase>`-only methods. The five methods that live on
// `ActiveInner` above (feed_bytes_impl, feed_bytes_bounded,
// feed_inbound, advance_one_frame, clear_session_residue_for_class)
// are reached through delegates earlier in this file + via
// `self.inner.X` from in-crate sites (row_stream slow path,
// cfg(test) integration tests, etc.).
impl PgProtocol<ActivePhase> {

    /// Drive a terminally-Errored protocol into a typed
    /// `PgProtocol<ClosedPhase>` wrapper. Returns `Err(self)` when
    /// the protocol is NOT yet Errored — caller continues using
    /// the `<ActivePhase>` instance.
    ///
    /// # Tier-1 closure
    ///
    /// The `<ClosedPhase>` ZST-marker physically lacks
    /// `push_command`, `feed_bytes`, `feed_inbound`,
    /// `advance_one_frame`, etc. (method-absent E0599 at compile
    /// time). The only operation available on `<ClosedPhase>` is
    /// the `cause()` accessor. A naive shape would let callers
    /// keep driving an Errored `<ActivePhase>` and rely on every
    /// `push_command`'s Errored-arm classification —
    /// tier-3-by-discipline, because a future refactor could omit
    /// the check.
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
        mut self,
    ) -> Result<PgProtocol<ClosedPhase>, PgProtocol<ActivePhase>> {
        // Ok arm materialises `ClosedInner` (~16 B) instead of
        // moving the full 504-B `ActiveInner`. Same
        // extract-and-drop shape as `<ConnectingPhase>::into_active`'s
        // Closed arm: state_kind is Copy from `&state`; error_arena
        // is mem::take'd; the remaining ActiveInner Drops at scope
        // end, releasing ~488 B of stack + any heap behind the
        // Box-niche cells.
        if let crate::state::ActiveState::Errored(state_kind) = &self.inner.state {
            let state_kind = *state_kind;
            let error_arena = core::mem::take(&mut self.inner.error_arena);
            Ok(PgProtocol {
                inner: ClosedInner {
                    sync_marker: PhantomData,
                    cause: CloseCause::Errored(state_kind),
                    error_arena,
                },
                extras: (),
                phase_marker: PhantomData,
            })
        } else {
            Err(self)
        }
    }

    /// Push a graceful Terminate (`'X'`) frame and consume self into
    /// [`PgProtocol<ClosedPhase>`] with cause
    /// [`CloseCause::GracefulTerminate`].
    ///
    /// # PG semantics
    ///
    /// The Terminate frame (PG §55.7) is a 5-byte client-initiated
    /// graceful close: `[b'X', 0, 0, 0, 4]`. After sending Terminate,
    /// the server completes any in-flight query, then closes the
    /// connection. The client is expected to:
    ///
    /// 1. Flush the trailing bytes (returned by this method) to the
    ///    socket.
    /// 2. Drop the [`PgProtocol<ClosedPhase>`] (releasing any preserved
    ///    `error_arena` heap) — or hold it briefly to inspect
    ///    [`PgProtocol<ClosedPhase>::cause`] / [`close_cause`] for
    ///    diagnostic logging.
    /// 3. Close the TCP connection.
    ///
    /// [`close_cause`]: PgProtocol<ClosedPhase>::close_cause
    ///
    /// # Callable from any [`ActiveState`]
    ///
    /// PG accepts `Terminate` at any time in the protocol lifecycle
    /// (Idle, mid-query, even during an in-flight Sync). This method
    /// mirrors that — `self` is consumed regardless of `state`. Any
    /// in-flight [`crate::reply_id::ReplyId`] inside the consumed state
    /// drops cleanly (the [`#[must_use]`] lint is a HINT, not a
    /// runtime check; the matching `oneshot::Sender` on the wrapper
    /// layer drops along with the rest of the connection).
    ///
    /// [`ActiveState`]: crate::state::ActiveState
    ///
    /// # Returned bytes lifetime
    ///
    /// The `&'w [u8]` slice borrows from `wb` for the lifetime `'w`.
    /// The returned [`PgProtocol<ClosedPhase>`] does NOT borrow `wb` —
    /// it owns its own [`ClosedInner`]. Callers can drain the bytes to
    /// the socket and then drop the `wb` borrow; the
    /// `PgProtocol<ClosedPhase>` survives independently.
    ///
    /// # Failure modes
    ///
    /// - [`crate::write_buf::WriteBufFull`] if `wb` cannot fit 5 more
    ///   bytes. `self` is consumed regardless (the protocol intent is
    ///   "close this thing"); the caller has the [`WriteBufFull`]
    ///   error and is expected to drop the socket.
    ///
    /// # Tier-1 closure on post-terminate API
    ///
    /// The returned [`PgProtocol<ClosedPhase>`] is method-absent for
    /// every send/receive operation (`push_command`, `feed_inbound`,
    /// `feed_bytes`, `advance_one_frame`, `into_active`). Calling any
    /// of those is E0599 («Closed absorbs no input»). The only
    /// available operations are [`PgProtocol<ClosedPhase>::cause`],
    /// [`PgProtocol<ClosedPhase>::close_cause`],
    /// [`PgProtocol<ClosedPhase>::get_server_error`], and
    /// [`PgProtocol<ClosedPhase>::error_arena_overwrite_count`].
    #[expect(
        clippy::needless_lifetimes,
        reason = "explicit `'w` documents that the returned `&[u8]` borrows from `wb`, \
                  while the tuple's `PgProtocol<ClosedPhase>` is owned (no borrow). \
                  Eliding makes the signature reader guess which output borrows from `wb`."
    )]
    pub fn terminate<'w>(
        mut self,
        wb: &'w mut crate::write_buf::WriteBuf,
    ) -> Result<(&'w [u8], PgProtocol<ClosedPhase>), crate::write_buf::WriteBufFull> {
        let start = wb.len();
        wb.push_bytes(&crate::wire::TERMINATE_WIRE_BYTES)?;
        let end = wb.len();
        // Detach the wb-borrow before the consume-self assembly. After
        // `wb.push_bytes` returns the `&mut WriteBuf` is no longer in
        // active use; NLL releases the implicit `&mut` here, letting us
        // take an immutable `&wb[start..end]` slice that lives for `'w`.
        let bytes = match wb.as_bytes().get(start..end) {
            Some(s) => s,
            // Architecturally dead: push_bytes succeeded.
            None => &[],
        };
        let error_arena = core::mem::take(&mut self.inner.error_arena);
        let closed = PgProtocol {
            inner: ClosedInner {
                sync_marker: PhantomData,
                cause: CloseCause::GracefulTerminate,
                error_arena,
            },
            extras: (),
            phase_marker: PhantomData,
        };
        Ok((bytes, closed))
    }

    /// Resolve an [`crate::error_arena::ErrorRef`] handle (carried
    /// by `ProtocolError::ServerErrorResponse.details_ref`) to the
    /// full `ErrorPayload` containing the server's
    /// message/detail/hint bounded strings.
    ///
    /// # Return value
    ///
    /// Tier-3 classified `Result`:
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
    ///     Err(bsql_postgres_proto::ArenaError::Stale) => {
    ///         // Expected if resolution deferred past clear_arena boundary.
    ///     }
    ///     Err(bsql_postgres_proto::ArenaError::Empty) => {
    ///         // Architecturally unreachable — crate bug if seen.
    ///     }
    /// }
    /// ```
    #[inline]
    pub fn get_server_error(
        &self,
        r: crate::error_arena::ErrorRef,
    ) -> Result<&crate::error_arena::ErrorPayload, crate::error_arena::ArenaError> {
        // The static-fallback `cold_error_arena()` returns an empty
        // arena if the lazy-init slot has not been allocated;
        // calling `get(r)` on the empty arena classifies as
        // `ArenaError::Stale` (generation mismatch — the empty
        // arena's generation is 0, any forged ErrorRef has a
        // different generation). Same observable semantics as a
        // freshly-init'd arena.
        self.cold_error_arena().get(r)
    }

    /// Operator-facing canary for ErrorArena slot-overwrite events.
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
    /// dispatch-refactor regressions. Pipelining support is
    /// expected to replace the single-slot arena with a slab; this
    /// canary stays meaningful until that refactor lands.
    #[inline]
    #[must_use]
    pub fn error_arena_overwrite_count(&self) -> u16 {
        // Returns 0 when the lazy-init slot has not been allocated
        // (no error path has fired yet on this connection — same
        // observable semantics as a freshly-init'd arena).
        self.cold_error_arena().overwrite_count()
    }

    // No test-only forge hooks ship. The variant-carries-field
    // invariant is tier-1 compile, so drift states simply cannot
    // be constructed — tests exercise the SCRAM flow via real wire
    // bytes through the public API. A naive shape would expose
    // `forge_*` helpers under `#[cfg(test)]` to short-circuit the
    // handshake; under the variant-carries-field rule those would
    // not even compile.

    /// Display adapter that resolves a
    /// [`crate::error::ProtocolError`] with
    /// `ServerErrorResponse`-arena strings inline.
    ///
    /// A naive shape would render
    /// `ProtocolError::ServerErrorResponse` as `"[details in
    /// ErrorArena]"` — the cascade-size win (288 B → 8 B) regresses
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
        // Passes the boxed arena reference, or an &'static empty
        // fallback if the lazy-init slot has not been allocated.
        // Display formatting of an unresolved ErrorRef classifies
        // as `ArenaError::Stale` — same diagnostic surface as a
        // populated arena's stale-ref path.
        crate::error_arena::DisplayError::new(err, self.cold_error_arena())
    }

    /// Resolve a [`crate::Action::Notify`]'s gen-tagged ref to its
    /// payload (PG §55.7 LISTEN/NOTIFY surface — ).
    ///
    /// Returns:
    /// - `Ok(&NotificationPayload)` — ref resolves cleanly within
    ///   the current OutActions iteration cycle.
    /// - `Err(ArenaError::Stale)` — ref was issued in a prior cycle
    ///   (gen mismatch via the per-cycle `clear()` bump). Expected
    ///   when the wrapper stashes refs past their cycle boundary.
    /// - `Err(ArenaError::Empty)` — slot index out of bounds.
    ///   Architecturally unreachable: `alloc` pushes to `slots` before
    ///   issuing the ref. Classified explicitly per ArenaError
    ///   discipline.
    ///
    /// The wrapper pattern: iterate the `OutActions` returned by
    /// `feed_bytes`, and for each `Action::Notify { pid, notif_ref }`
    /// call `proto.get_notification(notif_ref)` to read the payload
    /// (channel + payload bytes). Copy what you need before the next
    /// `feed_bytes` cycle clears the arena.
    ///
    /// # Empty-arena fallback
    ///
    /// If the slot has never been allocated (`None` because no NOTIFY
    /// has arrived this connection's lifetime), the resolution against
    /// a static empty `NotificationsArena` returns `Err(Stale)` for
    /// any ref the caller might construct (impossible in practice —
    /// no ref can be issued without an alloc, and alloc lazy-inits
    /// the slot).
    #[inline]
    pub fn get_notification(
        &self,
        r: crate::notifications_arena::NotificationRef,
    ) -> Result<
        &crate::notifications_arena::NotificationPayload,
        crate::error_arena::ArenaError,
    > {
        static EMPTY_ARENA: crate::notifications_arena::NotificationsArena =
            crate::notifications_arena::NotificationsArena::new();
        let arena = match self.inner.notifications_arena.as_deref() {
            Some(a) => a,
            None => &EMPTY_ARENA,
        };
        arena.get(r)
    }

    /// Resolve a `NoticeRef` to the notice payload.
    ///
    /// Returns the server-sent notice text (severity, code, message,
    /// detail, hint). Refs are valid only within the `feed_bytes`
    /// cycle that produced them — after the next call, stale refs
    /// return `Err(ArenaError::Stale)`.
    #[inline]
    pub fn get_notice(
        &self,
        r: crate::notices_arena::NoticeRef,
    ) -> Result<
        &crate::notices_arena::NoticePayload,
        crate::error_arena::ArenaError,
    > {
        static EMPTY_ARENA: crate::notices_arena::NoticesArena =
            crate::notices_arena::NoticesArena::new();
        let arena = match self.inner.notices_arena.as_deref() {
            Some(a) => a,
            None => &EMPTY_ARENA,
        };
        arena.get(r)
    }

    /// Push a `CopyData` ('d') frame to the server during an
    /// active COPY IN cycle (Phase 4, PG §55.2.6).
    ///
    /// Writes the `'d' + len + bytes` frame to `wb` and returns the
    /// staged byte slice. State stays in
    /// `SimpleQueryCopyInActive` — only the server's
    /// `CommandComplete` (or `ErrorResponse`) response advances
    /// state. Caller may invoke this repeatedly to stream bulk
    /// data across multiple sub-buffer-sized chunks.
    ///
    /// # Errors
    ///
    /// - `CopyPushError::NotInCopyInState` — current state is not
    ///   `CopyInActive`. The push is rejected without writing.
    /// - `CopyPushError::FrameTooLarge` — `bytes.len() + 4 >
    ///   i32::MAX as usize`. PG's wire length field is `i32 BE`;
    ///   bodies exceeding 2 GiB cannot be framed.
    /// - `CopyPushError::WriteBufFull` — `wb` lacks capacity for
    ///   the framed bytes.
    pub fn push_copy_data<'w>(
        &mut self,
        bytes: &[u8],
        wb: &'w mut crate::write_buf::WriteBuf,
    ) -> Result<&'w [u8], crate::action::CopyPushError> {
        if !matches!(
            self.inner.state,
            crate::state::ActiveState::SimpleQueryCopyInActive(_)
        ) {
            return Err(crate::action::CopyPushError::NotInCopyInState);
        }
        let body_len_usize = bytes.len().saturating_add(4);
        let body_len = u32::try_from(body_len_usize)
            .map_err(|_| crate::action::CopyPushError::FrameTooLarge)?;
        // PG wire length field is signed i32 — values > i32::MAX
        // wrap negative on the server side. Reject via the
        // u32-from-i32-LE-bytes round-trip (avoids `as` cast which
        // is banned crate-wide).
        const I32_MAX_AS_U32: u32 = u32::from_le_bytes(i32::MAX.to_le_bytes());
        if body_len > I32_MAX_AS_U32 {
            return Err(crate::action::CopyPushError::FrameTooLarge);
        }
        let start = wb.len();
        wb.push_bytes(&[crate::wire::TAG_COPY_DATA_OUTBOUND.byte()])
            .map_err(crate::action::CopyPushError::WriteBufFull)?;
        wb.push_bytes(&body_len.to_be_bytes())
            .map_err(crate::action::CopyPushError::WriteBufFull)?;
        wb.push_bytes(bytes)
            .map_err(crate::action::CopyPushError::WriteBufFull)?;
        let end = wb.len();
        match wb.as_bytes().get(start..end) {
            Some(s) => Ok(s),
            // Architecturally dead: push_bytes succeeded, so
            // start..end is within bounds. Classified rather
            // than silent empty-slice (CREDO §V).
            None => Ok(&[]),
        }
    }

    /// Push a `CopyDone` ('c') frame to the server .
    ///
    /// Signals clean end-of-data from the client side during COPY
    /// IN. Server responds with `CommandComplete` (carrying the row
    /// count tag) followed by `ReadyForQuery`. State stays in
    /// `CopyInActive` until the server's `CommandComplete` arrives.
    pub fn push_copy_done<'w>(
        &mut self,
        wb: &'w mut crate::write_buf::WriteBuf,
    ) -> Result<&'w [u8], crate::action::CopyPushError> {
        if !matches!(
            self.inner.state,
            crate::state::ActiveState::SimpleQueryCopyInActive(_)
        ) {
            return Err(crate::action::CopyPushError::NotInCopyInState);
        }
        // CopyDone has empty body; length = 4 (self-inclusive).
        let frame: [u8; 5] = [crate::wire::TAG_COPY_DONE_OUTBOUND.byte(), 0, 0, 0, 4];
        let start = wb.len();
        wb.push_bytes(&frame)
            .map_err(crate::action::CopyPushError::WriteBufFull)?;
        let end = wb.len();
        match wb.as_bytes().get(start..end) {
            Some(s) => Ok(s),
            // Architecturally dead: push_bytes succeeded, so
            // start..end is within bounds. Classified rather
            // than silent empty-slice (CREDO §V).
            None => Ok(&[]),
        }
    }

    /// Push a `CopyFail` ('f') frame to the server with the given
    /// error message .
    ///
    /// Aborts an in-progress COPY IN cycle from the client side.
    /// `error` is sent as a CSTR (NUL-terminated). Server responds
    /// with `ErrorResponse` (carrying the abort reason classified
    /// as a server error) followed by `ReadyForQuery`. State stays
    /// in `CopyInActive` until the server's error arrives.
    pub fn push_copy_fail<'w>(
        &mut self,
        error: &str,
        wb: &'w mut crate::write_buf::WriteBuf,
    ) -> Result<&'w [u8], crate::action::CopyPushError> {
        if !matches!(
            self.inner.state,
            crate::state::ActiveState::SimpleQueryCopyInActive(_)
        ) {
            return Err(crate::action::CopyPushError::NotInCopyInState);
        }
        let error_bytes = error.as_bytes();
        if error_bytes.contains(&0) {
            // Reject embedded NUL — would corrupt CSTR framing.
            return Err(crate::action::CopyPushError::EmbeddedNul);
        }
        // Body = error CSTR (bytes + NUL terminator).
        // Total wire = tag (1) + len (4) + body.
        let body_len_usize = error_bytes.len().saturating_add(1).saturating_add(4);
        let body_len = u32::try_from(body_len_usize)
            .map_err(|_| crate::action::CopyPushError::FrameTooLarge)?;
        // PG wire length field is signed i32 — values > i32::MAX
        // wrap negative on the server side. Reject via the
        // u32-from-i32-LE-bytes round-trip (avoids `as` cast which
        // is banned crate-wide).
        const I32_MAX_AS_U32: u32 = u32::from_le_bytes(i32::MAX.to_le_bytes());
        if body_len > I32_MAX_AS_U32 {
            return Err(crate::action::CopyPushError::FrameTooLarge);
        }
        let start = wb.len();
        wb.push_bytes(&[crate::wire::TAG_COPY_FAIL_OUTBOUND.byte()])
            .map_err(crate::action::CopyPushError::WriteBufFull)?;
        wb.push_bytes(&body_len.to_be_bytes())
            .map_err(crate::action::CopyPushError::WriteBufFull)?;
        wb.push_bytes(error_bytes)
            .map_err(crate::action::CopyPushError::WriteBufFull)?;
        wb.push_bytes(&[0])
            .map_err(crate::action::CopyPushError::WriteBufFull)?;
        let end = wb.len();
        match wb.as_bytes().get(start..end) {
            Some(s) => Ok(s),
            // Architecturally dead: push_bytes succeeded, so
            // start..end is within bounds. Classified rather
            // than silent empty-slice (CREDO §V).
            None => Ok(&[]),
        }
    }

    /// Resolve a [`crate::Action::CopyDataChunk`]'s gen-tagged ref
    /// to its bytes (Phase 3, PG §55.2.6 COPY OUT).
    ///
    /// Same lifetime contract as [`Self::get_notification`]: refs
    /// are valid within the OutActions iteration cycle only.
    #[inline]
    pub fn get_copy_chunk(
        &self,
        r: crate::copy_chunks_arena::CopyChunkRef,
    ) -> Result<
        &crate::copy_chunks_arena::CopyChunkPayload,
        crate::error_arena::ArenaError,
    > {
        static EMPTY_ARENA: crate::copy_chunks_arena::CopyChunksArena =
            crate::copy_chunks_arena::CopyChunksArena::new();
        let arena = match self.inner.copy_chunks_arena.as_deref() {
            Some(a) => a,
            None => &EMPTY_ARENA,
        };
        arena.get(r)
    }

    /// Resolve a [`crate::Action::IntermediateCommandComplete`]'s
    /// gen-tagged ref to its `CommandTag` (,     /// multi-statement SimpleQuery surface).
    ///
    /// Same lifetime contract as [`Self::get_notification`] /
    /// [`Self::get_copy_chunk`]: refs are valid within the
    /// OutActions iteration cycle only. The arena clears at the
    /// next `feed_bytes` entry; refs held past that boundary
    /// resolve [`crate::error_arena::ArenaError::Stale`].
    #[inline]
    pub fn get_command_tag(
        &self,
        r: crate::command_tags_arena::CommandTagRef,
    ) -> Result<&crate::command_tag::CommandTag, crate::error_arena::ArenaError> {
        static EMPTY_ARENA: crate::command_tags_arena::CommandTagsArena =
            crate::command_tags_arena::CommandTagsArena::new();
        let arena = match self.inner.command_tags_arena.as_deref() {
            Some(a) => a,
            None => &EMPTY_ARENA,
        };
        arena.get(r)
    }

    // ═════════════════════════════════════════════════════════════
    // RowStream helpers
    // ═════════════════════════════════════════════════════════════
    //
    // Thin crate-internal accessors exposing read_buf / state
    // operations to the `row_stream` module without opening
    // field-level `pub(crate)` on the field directly. Each is a
    // single-line delegate — no logic added.

    /// Append bytes to read_buf; Err on overflow.
    ///
    /// # Partial-mode routing
    ///
    /// When the partial-assembly cell is active (an oversize non-`'D'`
    /// body is mid-flight), incoming bytes route to the assembly
    /// absorber FIRST. Up to `body_remaining` bytes are consumed
    /// (copied to the bounded prefix or counted-and-skipped beyond
    /// the cap); only the leftover (next-frame bytes) flows to
    /// ReadBuf.
    ///
    /// Without this hook, a chunk completing a body > READ_BUF_CAP
    /// would fail with `ReadBufFull` since ReadBuf is capped at 4 KB
    /// while bodies of any wire-legal size (up to ~2 GiB) must pass
    /// through.
    #[inline]
    pub(crate) fn read_buf_append(&mut self, bytes: &[u8]) -> Result<(), ReadBufFull> {
        // Partial-mode routing — `cold_path()` keeps the partial-
        // mode body out of the hot I-cache footprint. RowStream's
        // per-row fast path calls this function via the
        // `feed_inbound`-equivalent route; the inactive arm is the
        // hot path 99.99% of the time (real PG payloads ≤ 4 KB).
        if self.inner.partial_assembly.is_active() {
            core::hint::cold_path();
            let (_consumed, leftover) = _partial_assembly_dispatch_leaf::absorb_partial_assembly_at_dispatch(
                &mut self.inner.partial_assembly,
                bytes,
            );
            if leftover.is_empty() {
                return Ok(());
            }
            return self.inner.read_buf.append(leftover);
        }
        self.inner.read_buf.append(bytes)
    }

    /// Shared view of the populated read_buf region.
    ///
    /// Per-row hot path — called twice per row from
    /// `RowStream::next_row_bytes` (header peek + row carve). The
    /// `#[inline]` hint pins the inlined call chain
    /// `next_row_bytes → read_buf_populated → ReadBufN::populated`
    /// against future heuristic shifts in LLVM (today fully inlined
    /// under workspace `lto = "fat"` + `codegen-units = 1`).
    #[inline]
    #[must_use]
    pub(crate) fn read_buf_populated(&self) -> &[u8] {
        self.inner.read_buf.populated()
    }

    /// Current read cursor (u16 storage).
    ///
    /// Per-row hot path — called once per row from
    /// `RowStream::next_row_bytes` (cursor capture for row carve
    /// coordinates). `#[inline]` pins the fully-inlined call chain
    /// against future heuristic shifts in LLVM.
    #[inline]
    #[must_use]
    pub(crate) fn read_buf_cursor_u16(&self) -> u16 {
        self.inner.read_buf.cursor_position_u16()
    }

    /// Advance the read cursor. Err architecturally dead on
    /// RowStream paths (frames gated by `parse_header` length-check
    /// before advance).
    #[inline]
    pub(crate) fn read_buf_advance(
        &mut self,
        n: usize,
    ) -> Result<(), crate::buf::AdvancePastEnd> {
        self.inner.read_buf.advance(n)
    }

    /// Unread-region length accessor for the row-stream state
    /// machine's chunk-vs-whole-col decision. Re-export of
    /// [`crate::buf::ReadBuf::unread_len`].
    #[inline]
    #[must_use]
    pub(crate) fn read_buf_unread_len(&self) -> usize {
        self.inner.read_buf.unread_len()
    }

    /// Maximum bytes that can be fed via `RowStream::feed` without
    /// overflow. Equal to `READ_BUF_CAP - unread_bytes` (post-compaction).
    #[inline]
    #[must_use]
    pub fn feed_capacity(&self) -> usize {
        crate::frame::READ_BUF_CAP.saturating_sub(self.inner.read_buf.unread_len())
    }


    /// Partial-mode entry point routed through the leaf-gated
    /// [`crate::buf::ReadBuf::enter_partial_mode`] accepting a
    /// `&PartialFrameToken`. The token mint is gated to
    /// `crate::row_stream::_row_stream_partial_leaf::mint_for_row_stream_dispatcher`,
    /// itself `pub(in crate::row_stream)` — so this entry point is
    /// only legitimately reachable from inside `mod row_stream`.
    ///
    /// Propagates `Err(AlreadyInPartialMode)` from the inner call;
    /// callers route Err through
    /// [`Self::install_errored_partial_mode_reentry`] +
    /// `ColEvent::EndQuery::Err` (classifier-bug protocol). A naive
    /// shape would return `()` and treat re-entry as a silent
    /// overwrite in release (debug-asserted in dev) —
    /// dev-loud/release-silent CREDO §V glass pattern.
    #[inline]
    pub(crate) fn enter_partial_mode_for_data_row(
        &mut self,
        token: &crate::row_stream::_row_stream_partial_leaf::PartialFrameToken,
        declared_body_len: u32,
    ) -> Result<(), crate::buf::AlreadyInPartialMode> {
        self.inner.read_buf.enter_partial_mode(token, declared_body_len)
    }

    /// Partial-mode exit point. Mirror of
    /// [`Self::enter_partial_mode_for_data_row`].
    ///
    /// Propagates `Err(PartialModeExitUndrained)` from the inner
    /// call; callers route Err through
    /// [`Self::install_errored_partial_mode_exit_undrained`] +
    /// `ColEvent::EndQuery::Err` (classifier-bug protocol). Single
    /// source of truth: the function enforces the
    /// `partial_remaining == 0` precondition. A naive shape would
    /// return `()` and require callers to pre-check the counter
    /// upstream — tier-2 by-discipline with a silent-reset path in
    /// release that is wire-desync-class.
    #[inline]
    pub(crate) fn exit_partial_mode_for_row_stream(
        &mut self,
        token: &crate::row_stream::_row_stream_partial_leaf::PartialFrameToken,
    ) -> Result<(), crate::buf::PartialModeExitUndrained> {
        self.inner.read_buf.exit_partial_mode(token)
    }

    /// Drain `n` bytes from the partial-mode counter. Returns Err
    /// on attempted underflow.
    #[inline]
    pub(crate) fn subtract_partial_for_row_stream(
        &mut self,
        token: &crate::row_stream::_row_stream_partial_leaf::PartialFrameToken,
        n: u32,
    ) -> Result<(), crate::buf::AdvancePastEnd> {
        self.inner.read_buf.subtract_partial_remaining(token, n)
    }

    /// Partial-mode predicate. Used by the row-stream state machine
    /// to decide whether the `subtract_partial_*` bookkeeping is
    /// needed.
    #[inline]
    #[must_use]
    pub(crate) fn is_in_partial_mode_for_row_stream(&self) -> bool {
        self.inner.read_buf.is_in_partial_mode()
    }

    // No `partial_remaining_for_row_stream` accessor. The
    // precondition `partial_remaining == 0` is enforced INSIDE
    // [`Self::exit_partial_mode_for_row_stream`] via typed Err
    // return — single source of truth. A naive shape would expose
    // a counter accessor and require callers to pre-check it
    // before calling exit — tier-2 discipline gap.
    //
    // The underlying `ReadBuf::partial_remaining()` accessor is
    // preserved because the row_stream partial-mode spec tests
    // assert the counter value directly on a `ReadBuf` fixture
    // (load-bearing use via the field-accessor path).

    /// Project the current row_desc_slot as a
    /// [`crate::decode::RowDescBorrow`], or `None` if no schema is
    /// parked.
    ///
    /// Used by terminal materialise to construct
    /// `Reply::QueryComplete::row_desc` and by the per-row fast-path
    /// to project the schema descriptor after `read_buf_advance`.
    ///
    /// # Perf rationale
    ///
    /// The per-row hot path runs `match &self.inner.state` ONCE for
    /// the streaming-variant gate (with the schema NOT in the
    /// variant) plus a single `Option::as_ref` projection here. A
    /// naive shape would keep the RowDesc inline in the streaming-
    /// state variant and require a SECOND enum match per row to
    /// re-project the schema after `read_buf_advance`. The Option
    /// projection is strictly cheaper than the second enum match
    /// — one byte read for the discriminant, one ptr-deref on Some.
    /// Column names from the most recent RowDescription frame.
    #[inline]
    #[must_use]
    pub fn current_column_names(&self) -> Option<&[alloc::string::String]> {
        self.extras.column_names.as_deref()
    }

    /// Read the parked `RowDesc` from the protocol's row_desc_slot.
    #[inline]
    #[must_use]
    pub fn current_row_desc(&self) -> Option<crate::decode::RowDescBorrow<'_>> {
        self.extras
            .row_desc
            .as_ref()
            .map(crate::decode::RowDescBorrow::from_ref)
    }

    /// Read the parked terminal `CommandTag` from the protocol's
    /// command_tag_slot (/). Returns `None` if no
    /// `'C'` (CommandComplete) frame has been observed since the
    /// last residue clear. Callers query this AFTER consuming
    /// `Action::DeliverReply { Reply::QueryComplete(..) }` from
    /// `OutActions`.
    ///
    /// moved `command_tag` off the inline Reply
    /// variant. The accessor mirrors `current_row_desc`'s contract.
    #[inline]
    #[must_use]
    pub fn current_command_tag(&self) -> Option<&crate::command_tag::CommandTag> {
        self.extras.command_tag.as_ref()
    }

    /// Read the parked terminal `ParamOids` from the protocol's
    /// param_oids_slot (/). Returns `None` if no
    /// `'t'` (ParameterDescription) frame has been observed since
    /// the last residue clear. Callers query this AFTER consuming
    /// `Action::DeliverReply { Reply::DescribeStatementComplete(..) }`
    /// from `OutActions`.
    ///
    /// moved `param_oids` off the inline
    /// `DescribeStatementCompletePayload`. The accessor mirrors
    /// `current_row_desc`'s contract.
    #[inline]
    #[must_use]
    pub fn current_param_oids(&self) -> Option<&crate::action::ParamOids> {
        self.extras.param_oids.as_ref()
    }

    /// Compute the typed `DescribedRows<'_>` view from the parked
    /// row_desc_slot (). `Rows(..)` if a `'T'`
    /// (RowDescription) frame was observed since the last residue
    /// clear; `NoData` if `'n'` (NoData) was observed.
    ///
    /// Slot population is the single source of truth for schema
    /// presence: dispatch arms park RowDesc on `'T'` arrival;
    /// `'n'` arms skip the park. The accessor materialises the
    /// `DescribedRows` sum from the slot state, mirroring the
    /// dispatch-time discrimination.
    ///
    /// Callers query this AFTER consuming `Action::DeliverReply
    /// { Reply::Describe*Complete(..) }` from `OutActions`.
    #[inline]
    #[must_use]
    pub fn current_described_rows(&self) -> crate::action::DescribedRows<'_> {
        crate::action::describe_rows_from_slot(self.extras.row_desc.as_ref())
    }

    /// Read the terminal `ReadyForQuery` transaction-status byte
    /// parked at the most-recent `'Z'` arrival ().
    ///
    /// Externalisation of the byte from every `Reply<'r>` variant
    /// payload removed it from inline pattern destructure; callers
    /// query this accessor AFTER consuming `Action::DeliverReply`
    /// from `OutActions`. Default value is
    /// [`crate::action::TxStatus::Idle`] — the conn-start state for
    /// a freshly handshaked Active connection. Reset to `Idle` at
    /// every Idle/Errored residue boundary.
    ///
    /// # Lifetime contract
    ///
    /// Unlike `get_notification` / `get_copy_chunk` /
    /// `get_command_tag` (gen-tagged arena handles that require
    /// resolution within the current OutActions cycle), this is a
    /// plain `Copy` accessor — the parked value is valid until the
    /// next residue clear OR the next `'Z'` arrival overwrites it.
    /// Callers may stash it freely.
    #[inline]
    #[must_use]
    pub fn terminal_tx_status(&self) -> crate::action::TxStatus {
        self.extras.tx_status.get()
    }

    /// Read the parked `Action::FailReply.cause` from the most-recent
    /// failure event (.b).
    ///
    /// Returns `None` if no failure has been observed yet on this
    /// protocol instance (slot empty post-init or post-Idle residue
    /// clear).
    ///
    /// # Caller contract
    ///
    /// Query IMMEDIATELY after consuming `Action::FailReply` /
    /// `FeedEvent::Fail` from the action surface. A subsequent
    /// `install_errored` (e.g. `ConnectionAlreadyClosed` raised when
    /// the caller pushes a command on an already-Errored protocol)
    /// overwrites the slot via latest-wins semantics. Deferring the
    /// query past a subsequent push loses the original cause.
    ///
    /// # Lifetime contract
    ///
    /// Returns `Option<&ProtocolError>` — the borrow is tied to
    /// `&self`. `ProtocolError: Copy`, so callers typically `.copied()`
    /// to detach.
    #[inline]
    #[must_use]
    pub fn fail_cause(&self) -> Option<&crate::error::ProtocolError> {
        self.extras.fail_cause.as_ref()
    }

    /// Fused state classification for the row-stream fast-path
    /// entry. Single `match &self.inner.state` returns the
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
    /// A naive shape would expose separate `state_is_errored()` +
    /// `streaming_reply_id()` accessors — two enum matches per
    /// `next_event`. Fusing them into one match (observed once per
    /// `next_event` call) saves an enum-discriminant load per row
    /// (~1 ns at 3 GHz on branch-predicted state); the compiler
    /// does not reliably fuse two separate match calls because
    /// they are separated by header-parse logic.
    ///
    /// Per-stream hot path — called once per `next_event` /
    /// `next_row_bytes` invocation (cached in
    /// `RowStream::cached_reply_id` after first call). Amortised
    /// cost is sub-1 ns. `#[inline]` pins the fully-inlined call
    /// chain against future LLVM heuristic shifts.
    #[inline]
    #[must_use]
    pub(crate) fn classify_for_iter_rows(&self) -> IterRowsClass {
        use crate::state::ActiveState;
        match &self.inner.state {
            ActiveState::Errored(_) => IterRowsClass::Errored,
            ActiveState::SimpleQueryStreamingRows { reply }
            | ActiveState::BindExecuteStreamingRows { reply }
            | ActiveState::BindExecuteAwaitingDataOrCompleteSelect { reply } => {
                IterRowsClass::Streaming(reply.get())
            }
            _ => IterRowsClass::Other,
        }
    }

    /// Transition to `Errored(Internal)` for a dead-branch read_buf
    /// advance Err. Used by RowStream's fast-path when
    /// `read_buf_advance(total)` returns Err — architecturally
    /// impossible (total pre-validated) but tier-2 classification
    /// closes the drift surface at zero runtime cost (branch is
    /// cold-path unreachable in practice).
    ///
    /// # Atomic drain via FeedStateSetter
    ///
    /// The drain and install fire as one `mem::replace` via
    /// [`crate::state_setter::FeedStateSetter::drain_and_install_errored`];
    /// the returned `Option<NonZeroU64>` is `#[must_use]` and the
    /// caller in `RowStream` uses it directly for
    /// `StreamItem::FailReply { id, cause }`. A naive shape would
    /// write `*self.inner.state = Errored(...)` directly and peek
    /// the in-flight reply id separately at the dispatch site —
    /// tier-3 dual-source-of-truth.
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically \
                  drained by the Errored install. Caller MUST emit ColEvent::EndQuery \
                  { outcome: Err(_) } or equivalent — dropping it leaks the user's \
                  oneshot-receiver (zombie-reply class)."]
    pub(crate) fn install_errored_read_cursor_advance(&mut self) -> Option<NonZeroU64> {
        let cause = ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::ReadCursorAdvance,
        };
        self.drain_via_leaf(cause.state_kind(), _read_cursor_advance_drain_leaf::drain)
    }

    /// Per-phase lift+lower wrapper for the drain-and-install
    /// pattern.
    ///
    /// Five `install_errored_*` sites (RowStream cold paths + Drop)
    /// need to drain the in-flight reply id and atomically install
    /// `Errored(kind)`. The drain leaves expect `&mut ProtoState`;
    /// this helper provides the per-phase
    /// `ActiveState → ProtoState` lift, calls the supplied
    /// `drain_fn` (which mints its own per-call-site token
    /// internally — tier-1 closure on `FeedStateSetter::new`
    /// preserved), then lowers the result.
    ///
    /// All 5 drain leaves produce `ProtoState::Errored(kind)` on
    /// success; the `ActiveState::try_from` lower-step projects this
    /// cleanly to `ActiveState::Errored(kind)`. The `WrongPhase` arm
    /// is architecturally impossible (drain always produces Errored)
    /// and falls through to the sentinel `Errored(kind)` already
    /// installed by the `mem::replace`.
    #[inline]
    fn drain_via_leaf<F>(
        &mut self,
        kind: crate::error::StateErrorKind,
        drain_fn: F,
    ) -> Option<NonZeroU64>
    where
        F: FnOnce(&mut ProtoState, crate::error::StateErrorKind) -> Option<NonZeroU64>,
    {
        use crate::state::{ActiveState, WrongPhase};
        let sentinel = ActiveState::Errored(kind);
        let lifted = core::mem::replace(&mut self.inner.state, sentinel);
        let mut proto_state: ProtoState = lifted.into();
        let drained = drain_fn(&mut proto_state, kind);
        self.inner.state = match ActiveState::try_from(proto_state) {
            Ok(s) => s,
            Err(WrongPhase { recovered }) => {
                core::hint::cold_path();
                match recovered.take_inflight_reply_raw_id() {
                    Some(_) | None => {}
                }
                ActiveState::Errored(kind)
            }
        };
        drained
    }

    /// Transition to
    /// `Errored(InternalCrateBug { locus: PartialModeReentry })`
    /// when [`Self::enter_partial_mode_for_data_row`] returns Err.
    /// Routes the drain through the leaf shape mirroring
    /// `install_errored_read_cursor_advance`; same atomic
    /// drain-and-install discipline.
    ///
    /// Architecturally dead under intact callers — the streaming
    /// dispatcher in row_stream.rs's begin_partial_data_row
    /// guarantees `exit_partial_mode` runs before any re-entry
    /// attempt — but the typed-Err + classified install is the
    /// by-construction shield against future dispatch-loop drift.
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically \
                  drained by the Errored install. Caller MUST emit ColEvent::EndQuery \
                  { outcome: Err(_) } or equivalent — dropping it leaks the user's \
                  oneshot-receiver (zombie-reply class)."]
    pub(crate) fn install_errored_partial_mode_reentry(&mut self) -> Option<NonZeroU64> {
        let cause = ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::PartialModeReentry,
        };
        self.drain_via_leaf(cause.state_kind(), _partial_mode_reentry_drain_leaf::drain)
    }

    /// Transition to
    /// `Errored(InternalCrateBug { locus: PartialModeExitUndrained })`
    /// when [`Self::exit_partial_mode_for_row_stream`] returns Err.
    /// Routes the drain through the leaf shape mirroring
    /// [`Self::install_errored_partial_mode_reentry`]; same atomic
    /// drain-and-install discipline.
    ///
    /// Two paths reach this: (a) internal classifier bug in the
    /// dispatch loop (architecturally dead under intact callers); (b)
    /// adversarial server emitting a malformed DataRow whose
    /// per-column-length sum disagrees with the frame-header body
    /// length, leaving body bytes uncounted at end-of-row. Either
    /// way, classification + Errored transition + reply-id drain
    /// + ColEvent::EndQuery::Err.
    #[inline]
    #[must_use = "the returned Option<NonZeroU64> is the in-flight reply id atomically \
                  drained by the Errored install. Caller MUST emit ColEvent::EndQuery \
                  { outcome: Err(_) } or equivalent — dropping it leaks the user's \
                  oneshot-receiver (zombie-reply class)."]
    pub(crate) fn install_errored_partial_mode_exit_undrained(&mut self) -> Option<NonZeroU64> {
        let cause = ProtocolError::InternalCrateBug {
            locus: crate::error::CrateBugLocus::PartialModeExitUndrained,
        };
        self.drain_via_leaf(
            cause.state_kind(),
            _partial_mode_exit_undrained_drain_leaf::drain,
        )
    }

    /// Transition to `Errored(Framing)` for a malformed DataRow
    /// (empty body, server-side desync). Used by RowStream's
    /// fast-path when `start == end`.
    ///
    /// Takes `total_len: usize` matching the caller's
    /// `ProtocolError::MalformedDataRow { total_len }` payload —
    /// single source of truth for the discriminator. A naive shape
    /// would hardcode `total_len: 0` for the state-kind derivation
    /// (correct today since the discriminator is payload-
    /// independent, but tier-4 fragility if a future `state_kind()`
    /// ever folds on `total_len`). Pass-through closes the
    /// "mismatched twin payloads" drift.
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
        self.drain_via_leaf(cause.state_kind(), _malformed_data_row_drain_leaf::drain)
    }

    // No `install_errored_stale_schema_ref`. There is no SchemaRef
    // type or generation drift class. State variants carry RowDesc
    // inline; the fast-path reads `&self.inner.state.row_desc`
    // directly. The "stale ref" bug class is architecturally
    // impossible (no handle to be stale).

    /// Transition to `Errored(Internal)` when a
    /// [`crate::row_stream::RowStream`] is dropped mid-frame
    /// (closure exited via early return / `?` / panic-unwind
    /// without reaching a terminal `ColEvent::EndQuery`).
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
    /// drop-glue contract.
    ///
    /// # Tier-1 closure on panic unwind
    ///
    /// Drop fires unconditionally on stack unwind by Rust spec. The
    /// crate runs under `panic = "unwind"` (workspace default); a
    /// downstream binary with `panic = "abort"` is an OS-level
    /// boundary (process death → TCP RST → server-side teardown —
    /// stronger than any library mechanism can offer).
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
        // Drop-path drain: the in-flight reply id has no caller
        // context here (the RowStream is being dropped without a
        // FailReply emission target). Explicit `Some(_) | None => {}`
        // arm consumes the `#[must_use]` return without re-introducing
        // the banned `let _drained = ...;` underscore-bind. See the
        // leaf submodule docstring for the must_use rationale.
        match self.drain_via_leaf(
            cause.state_kind(),
            _stream_dropped_mid_stream_drain_leaf::drain,
        ) {
            Some(_) | None => {}
        }
        // Clear read_buf so a subsequent feed_bytes on the
        // post-Errored connection does not classify mid-frame bytes
        // as a fresh frame header. The state is already Errored —
        // `feed_bytes_impl`'s
        // `IngressClassification::AlreadyErrored` arm also calls
        // `read_buf.clear()`, but doing it here keeps the post-Drop
        // invariant tight without needing a follow-up feed_bytes
        // to scrub.
        self.inner.read_buf.clear();
    }

    /// Closure-scoped row-stream API.
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
    /// architectural boundary stronger than any library mechanism.
    ///
    /// # Hot-path cost
    ///
    /// `#[inline]` + closure monomorphisation produces machine code
    /// identical to a by-value stream returned to the caller. The
    /// `&mut RowStream` indirection is elided by LLVM's inliner.
    /// Drop call at scope end is one `call` instruction — same as
    /// a caller-side `}` scope close would have had on a by-value
    /// stream.
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
    /// # Scope
    ///
    /// D-tag streaming-exposed. Within-D, every wire-legal body
    /// size is handled via partial-frame chunking — see
    /// [`crate::row_stream::ColEvent`]. Non-D frames > READ_BUF_CAP
    /// route through the partial-assembly cell separately (the
    /// dispatch loop's FrameTooLarge arm for streaming-eligible
    /// tags).
    #[inline]
    pub fn iter_rows<R, F>(&mut self, write_buf: &mut WriteBuf, f: F) -> R
    where
        F: for<'p, 'w> FnOnce(&mut crate::row_stream::RowStream<'p, 'w>) -> R,
    {
        // Entry-point housekeeping mirrors feed_bytes: cache the
        // push class once before residue clear so the inliner can
        // specialise the residue-helper body when entry_class is
        // statically known at the call site. row_desc_slot lives on
        // outer `<ActivePhase>::Extras` — pass via disjoint-field
        // borrow.
        write_buf.clear();
        let entry_class = self.inner.state.push_class();
        self.inner.clear_session_residue_for_class(&mut self.extras, entry_class);

        // The stream value lives here on `iter_rows`'s stack frame.
        // Caller's closure receives `&mut stream` — a borrow, not
        // the value. Drop fires at end of this function body, even
        // on panic unwind (Rust spec). `mem::forget` of the closure-
        // borrowed reference is a no-op against the underlying value.
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

/// Classifier output for [`PgProtocol::classify_for_iter_rows`].
///
/// 3-variant enum (each ZST-discriminator except Streaming carrying
/// `NonZeroU64`) selecting the row-stream fast-path entry behaviour.
/// Returned by a single `match &self.inner.state` so the row-stream
/// entry does ONE enum match per `next_event` — a naive shape would
/// expose separate `state_is_errored()` + `streaming_reply_id()`
/// accessors and the compiler does not reliably fuse the two
/// matches.
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
// Field-level free functions for the fail path
// ═════════════════════════════════════════════════════════════════════
//
// `feed_bytes`'s dispatch loop runs inside a
// `self.inner.read_buf.with_branded(|mut rb| { ... })` branded scope.
// Inside that scope `read_buf` is borrowed via `rb` (mut via
// `BrandedReadBuf::advance_scope_local` / `clear_scope_local`; shared
// otherwise). A naive `&mut self` fail helper would conflict with
// `rb`'s borrow at the type level.
//
// Free-function form below takes disjoint field refs
// (`&mut ProtoState`, `&mut StagedActions`, `&mut u32`). Callers
// destructure `self` at the dispatch-scope entry and thread the
// disjoint refs down. Instance methods below delegate to these for
// non-branded call sites.

/// Field-level fail helper used inside the branded read scope.
///
/// Takes `&mut ProtoState` + `&mut StagedActions` only — DOES NOT
/// take `&mut ReadBuf`, because inside `self.inner.read_buf.with_branded`
/// the read_buf is held by `rb` and cannot be separately
/// reborrowed. Callers inside the branded scope clear read_buf via
/// `rb.clear_scope_local()` at an appropriate post-mutation point.
///
/// The atomic-terminus triple (state install + reply drain +
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
    // Drain is idempotent on sticky-Errored: `Some(raw_id)` on the
    // first transition out of any non-Errored variant, `None` when
    // state is already Errored (original kind preserved). Counter
    // increment and `FailReply` emission ride the `Some`-arm —
    // physically gated by the state transition itself.
    //
    // # Tier-1 by-construction: counter capped at 1
    //
    // Each `PgProtocol` instance witnesses at most one
    // non-Errored → Errored transition in its lifetime
    // (`drain_and_install_errored` writes `Errored(kind)` only
    // when prior was non-Errored, per the leaf-helper's
    // `matches!(state, Errored(_))` short-circuit above). Counter
    // bump lives inside the `Some`-arm here, so it fires exactly
    // once per instance. A naive flow that bumped the counter
    // unconditionally and routed `MalformedStorm` via a
    // `>= 10_000` threshold would document the cap "by discipline
    // + early-return"; the fused-increment form pins the cap "by
    // sticky-Errored drain semantic".
    //
    // # `mem::replace` SCRAM zeroization
    //
    // `drain_and_install_errored`'s underlying `mem::replace`
    // drops the previous state, which may be a SCRAM variant
    // carrying `ScramSession`. `ScramSession`'s `ZeroizeOnDrop`
    // fires automatically — password bytes scrubbed in the drop
    // path of the replaced state. No explicit
    // `scram_state = None` step needed because there is no
    // separate scram_state field — SCRAM data lives inline in
    // the state variant and rides the drop glue.
    use _fail_inflight_no_readbuf_drain_leaf::DrainOutcome;
    match _fail_inflight_no_readbuf_drain_leaf::drain(state, cause.state_kind()) {
        DrainOutcome::Transitioned(inflight) => {
            // Real transition occurred: bump the canary regardless
            // of whether prior state carried an inflight reply.
            *malformed_counter = malformed_counter.saturating_add(1);
            // .b: StagedAction::FailReply carries cause
            // inline; materialise parks it into the slot at the
            // StagedAction → Action transformation boundary.
            match inflight {
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
        DrainOutcome::AlreadyErrored => {
            // No transition — state preserved as `Errored(prior_kind)`.
            // Canary not bumped (this re-entry doesn't represent a
            // fresh malformed event). No actions emitted: the
            // original `FailReply` + `CloseSocket` were already
            // emitted on the first call that transitioned the state.
        }
    }
}

/// Compute the state transition and actions for a command push.
///
/// # Pure compute / apply split
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
/// - **Errored as a first-class arm.** `ProtoState::Errored` is a
///   first-class arm: it preserves the cause (returns
///   `ProtoState::Errored(cause)` unchanged) and emits the
///   `FailReply`. A naive shape would peek `&self.inner.state`
///   *before* `core::mem::take` to avoid a transient `Idle` window;
///   passing state by value here makes the peek unnecessary.
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
    // The cfg(test) dispatcher operates on the owned-`Sql`
    // `PgCommand` enum. Although `PgCommand` variants own their
    // `Sql`, the SimpleQuery / Parse arms route through
    // `compute_push_simple_query` / `compute_push_parse` (cfg(test))
    // which take `&'a Sql` and stage `&'a [u8]` via
    // SendBytesBorrowed — so `staged` borrows from the locally-owned
    // Sql for the duration of this function. Returning the staged
    // container out of scope is safe because the local Sql lives for
    // the 'static lifetime of `PgCommand` (variants are owned).
    // Bind staged's 'sql to the function-local 'a to keep the
    // borrow checker honest, then return as 'static (subtype) once
    // we know no SendBytesBorrowed survives past the local scope.
    let mut staged: StagedActions<'_> = StagedActions::new();
    match cmd {
        PgCommand::Ping { reply } => compute_push_ping(state, reply, &mut staged),
        // No PgCommand::Startup arm. Startup is the only command
        // with a phase-typed entry-point
        // (`<DisconnectedPhase>::push_startup`); the test-only
        // dispatcher does not need the arm because there is no
        // `compute_push_startup` cfg(test) variant.
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

// No backwards-compat slow-path `compute_push_idle_only` ships.
// A naive shape would expose `impl PushCommand for PgCommand` as a
// blanket impl over the runtime-polymorphic enum — but the per-
// command structs are the sole production entry point; `PgCommand`
// survives only for the test-only `compute_push_tests` 5-arm
// dispatchers.

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
    // `&mut ProtoState` signature: the Idle arm writes new state
    // via `*state = ...`; preserve arms (Errored, PingAwaiting,
    // BusyQuery, Connecting) leave state untouched — saves the
    // 712 B mem::take + 712 B write-back per non-Idle push that a
    // by-value `state: ProtoState` signature would force.
    //
    // Classifier dispatch over `ProtoState::push_class`: 5 arms over
    // the classifier's 5 variants — exhaustive, no `_` fallback,
    // tier-1 preserved. A naive shape would match explicit state
    // variants directly with 18-way or-patterns for the tail
    // catch-alls; the classifier centralises the enumeration.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // IdleState typestate is the precondition; the
            // typestate's try_from re-checks at the boundary.
            let idle = match crate::state_setter::IdleState::try_from(state) {
                Some(idle) => idle,
                None => {
                    // session audit: glass pattern
                    // (debug_assert + return) replaced with
                    // classified error. See CrateBugLocus::
                    // PushClassIdleMismatch.
                    *state = ProtoState::Errored(
                        crate::error::StateErrorKind::from_kind_or_internal(
                            crate::error::ErrorKind::Internal,
                        ),
                    );
                    emit_actions!(staged, budget: 2, [
                        StagedAction::FailReply {
                            id: reply.consume(),
                            cause: ProtocolError::InternalCrateBug {
                                locus: crate::error::CrateBugLocus::PushClassIdleMismatch,
                            },
                        },
                        StagedAction::CloseSocket,
                    ]);
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

/// Idle-only path for the Ping push.
///
/// Caller must guarantee `state == ProtoState::Idle` (production
/// callsite is `ReadyGuard::push_command` which proves this via the
/// witness-guard typestate). Skips the 5-arm `state.push_class()`
/// dispatch in `compute_push_ping`, avoiding ~3 ns of branch +
/// dispatch overhead per push.
///
/// Tier-1 closure: the public API path through `ReadyGuard` only
/// reaches the Idle classification; the 5-arm dispatch surface
/// stays on the cfg(test) `compute_push_ping` for the
/// per-classification spec tests.
#[inline]
pub(crate) fn compute_push_ping_idle_only(
    setter: crate::state_setter::StateSetter<'_, crate::push_command::PingAwaitingRfqInstall>,
    reply: ReplyId<crate::reply_id::PingKind>,
    staged: &mut StagedActions,
) {
    // Sync is a compile-time const (5 bytes). Emit
    // `StagedAction::SendBytesStatic(&SYNC_WIRE_BYTES)` so the
    // materialiser passes the static reference through directly —
    // zero write to write_buf, zero copy.
    emit_actions!(staged, budget: 1, [
        StagedAction::SendBytesStatic(&SYNC_WIRE_BYTES),
    ]);
    // Typed witness pairs Ping → PingAwaitingRfq.
    setter.install_post_state(crate::push_command::PingAwaitingRfqInstall { reply });
}

// No `compute_push_startup` cfg(test) dispatcher.
// `<DisconnectedPhase>::push_startup`'s consume-self signature
// physically forbids pushing Startup from non-Disconnected states,
// so a per-state dispatcher (Idle / Errored / Connecting /
// PingAwaiting / BusyQuery) would be dead. The `Idle` path lives
// in `compute_push_startup_idle_only` below (reached from
// `<DisconnectedPhase>::push_startup`).

/// Idle-only path for the Startup handshake push.
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
    // Builder returns Result; Err → FailReply + CloseSocket +
    // Errored via `try_builder!`.
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
    // Discriminate auth method *here* — the post-push state
    // carries only what its auth method needs. Trust: 24 bytes.
    // SCRAM: 24 + ScramSession (~1040). Typed witness pairs Startup
    // → ConnectingStartup{Trust|Scram|Cleartext|Md5}.
    let post_install = match credentials {
        Credentials::Trust => crate::push_command::StartupPostInstall::Trust { reply },
        Credentials::ScramPassword(password) => {
            // Tier-1 variant-carries-field: ScramSession lives
            // INSIDE the variant; the variant cannot exist without
            // a valid `Box<ScramSession>`. ZeroizeOnDrop fires on
            // every exit path through the Box's Drop.
            let scram = alloc::boxed::Box::new(
                crate::scram::session::ScramSession::from_password(password),
            );
            crate::push_command::StartupPostInstall::Scram { reply, scram }
        }
        Credentials::CleartextPassword(password) => {
            // Mirror of the SCRAM construction above.
            // `Sensitive<Password>` is heap-boxed so the variant
            // footprint stays within the `ProtoState == 80` size
            // pin. Variant-carries-field invariant is
            // compile-enforced — the variant cannot exist without a
            // valid `Box<Sensitive<Password>>`. ZeroizeOnDrop fires
            // on every exit path through the Box's Drop.
            let password = alloc::boxed::Box::new(password);
            crate::push_command::StartupPostInstall::Cleartext { reply, password }
        }
        Credentials::Md5Password(password) => {
            // MD5 needs BOTH password AND username at
            // digest-construction time (server's 4-byte salt
            // arrives later in AuthenticationMD5Password). Bundle
            // them in a single Box<Md5HandshakeState> — same
            // single-Box pattern as SCRAM. Tier-1
            // variant-carries-field; the Box can never be None and
            // ZeroizeOnDrop fires on every exit path through
            // Box::drop → Md5HandshakeState::drop →
            // Sensitive::drop → Password::drop. `user` is
            // non-secret (cleartext on wire in StartupMessage
            // above) and not zeroized.
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
    // `sql`'s lifetime is intentionally decoupled from `staged`'s
    // `'sql` parameter. The cfg(test) legacy path copies SQL bytes
    // into `reserved` (via build_query_message_cfgtest) and stages
    // a single SendBytesRange — no borrow flows into staged, so
    // staged's `'_` is independent and compute_push can return
    // staged out of scope safely. `&mut ProtoState` signature: see
    // `compute_push_ping` rationale.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // IdleState typestate is the precondition.
            let idle = match crate::state_setter::IdleState::try_from(state) {
                Some(idle) => idle,
                None => {
                    // session audit: glass pattern
                    // (debug_assert + return) replaced with
                    // classified error. See CrateBugLocus::
                    // PushClassIdleMismatch.
                    *state = ProtoState::Errored(
                        crate::error::StateErrorKind::from_kind_or_internal(
                            crate::error::ErrorKind::Internal,
                        ),
                    );
                    emit_actions!(staged, budget: 2, [
                        StagedAction::FailReply {
                            id: reply.consume(),
                            cause: ProtocolError::InternalCrateBug {
                                locus: crate::error::CrateBugLocus::PushClassIdleMismatch,
                            },
                        },
                        StagedAction::CloseSocket,
                    ]);
                    return;
                }
            };
            let setter = idle.into_setter::<crate::push_command::SimpleQueryAwaitingFirstResponseInstall>();
            // cfg(test) legacy path: the typed-surface
            // `SimpleQuery<'a>` uses
            // `compute_push_simple_query_idle_only` with
            // `SendBytesBorrowed` for zero-copy SQL. The cfg(test)
            // dispatcher operates on the owned-`Sql`
            // `PgCommand::SimpleQuery` enum which is consumed
            // by-value through `compute_push`. To keep the legacy
            // path's staged-actions lifetime-portable (returnable
            // from compute_push out of scope) the full single-frame
            // is built here via the cfg(test)-only helper and one
            // `SendBytesRange` is emitted — identical wire output,
            // no SendBytesBorrowed surface.
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

/// Idle-only path for [`crate::push_command::SimpleQuery<'a>`] (typed
/// surface) and the cfg(test) [`PgCommand::SimpleQuery`] enum.
///
/// Emits **3** staged actions:
/// `SendBytesRange(header) + SendBytesBorrowed(sql) + SendBytesRange(trailer)`.
/// SQL is borrowed end-to-end, never copied into `WriteBuf`. A
/// naive shape would copy SQL bytes into `reserved` and emit one
/// `SendBytesRange` covering the whole frame; for large SQL strings
/// that adds a memcpy on every push.
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
    // Typed witness pairs SimpleQuery → SimpleQueryAwaitingFirstResponse.
    setter.install_post_state(
        crate::push_command::SimpleQueryAwaitingFirstResponseInstall { reply },
    );
}

// No `from_write_span_infallible` helper. Branded builders use
// [`crate::action::WriteRange::from_write_span`] directly —
// identical shield logic, plus brand-identity binding.

/// Build the PG simple-query (`'Q'`) frame **header** — tag plus the
/// upfront-computed length prefix.
///
/// The PG length-prefix INCLUDES itself (PG §55.7 wire spec); for
/// SimpleQuery the body is `sql + NUL`, so
/// length = 4 (length self) + sql_len + 1 (NUL). Both inputs are
/// known at the call site, so the length is computed upfront here —
/// no `with_length_prefix` back-patch needed.
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
/// terminator that follows the borrowed SQL bytes.
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
/// tag, length prefix, NUL-terminated statement name.
///
/// PG frame body layout (§55.7 "Parse"):
/// - Tag: `'P'` (1 byte) ← in this header
/// - Length: u32 BE including itself ← in this header
/// - Statement name: NUL-terminated ← in this header
/// - SQL text ← `SendBytesBorrowed` (NOT in WriteBuf)
/// - NUL terminator (1 byte) ← in trailer
/// - n_param_types: i16 BE (always 0 in current scope) ← in trailer
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
/// parameter-type count (always 0 in current scope).
fn build_parse_trailer(
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(0)?; // NUL terminator for the SQL string
    // n_param_types = 0; a future Parse-with-OIDs variant would
    // widen this to push actual OIDs.
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
    // `sql` lifetime decoupled from staged's `'_` (see
    // `compute_push_simple_query`). `&mut ProtoState` signature
    // matches the other compute_push_* arms.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // IdleState typestate is the precondition.
            let idle = match crate::state_setter::IdleState::try_from(state) {
                Some(idle) => idle,
                None => {
                    // session audit: glass pattern
                    // (debug_assert + return) replaced with
                    // classified error. See CrateBugLocus::
                    // PushClassIdleMismatch.
                    *state = ProtoState::Errored(
                        crate::error::StateErrorKind::from_kind_or_internal(
                            crate::error::ErrorKind::Internal,
                        ),
                    );
                    emit_actions!(staged, budget: 2, [
                        StagedAction::FailReply {
                            id: reply.consume(),
                            cause: ProtocolError::InternalCrateBug {
                                locus: crate::error::CrateBugLocus::PushClassIdleMismatch,
                            },
                        },
                        StagedAction::CloseSocket,
                    ]);
                    return;
                }
            };
            let setter = idle.into_setter::<crate::push_command::ParseAwaitingParseCompleteInstall>();
            // cfg(test) legacy path — see compute_push_simple_query above.
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

/// Idle-only path for [`crate::push_command::Parse<'a>`] (typed
/// surface) and the cfg(test) [`PgCommand::Parse`] enum.
///
/// Emits **4** staged actions:
/// `SendBytesRange(header) + SendBytesBorrowed(sql) + SendBytesRange(trailer) + SendBytesStatic(SYNC)`.
/// SQL is borrowed end-to-end, never copied into `WriteBuf`. A
/// naive shape would copy SQL bytes into `reserved` and emit one
/// `SendBytesRange` + the static Sync trailer.
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
    // Typed witness pairs Parse → ParseAwaitingParseComplete.
    setter.install_post_state(crate::push_command::ParseAwaitingParseCompleteInstall { reply });
}

// No `frame_build_unreachable` helper or
// `CrateBugLocus::OutboundFrameBuild { stage }` variant.  The
// `build_*_message` builders use the `WriteReserved` capacity
// witness in `write_buf.rs` — calls return Result where the Err
// arm is reachable (BuilderCapacityOverflow on >4 GB inputs); the
// dispatch via `try_builder!` covers it without a cold helper.

/// Build a PostgreSQL Extended Query `Describe` (`'D'`) frame
/// (PG §55.2.2).
///
/// Wire layout: tag `'D'`, 4-byte BE length (self-inclusive),
/// single target byte (`'S'` statement or `'P'` portal via
/// [`crate::wire::DescribeTargetByte`]), NUL-terminated name.
///
/// # Tier-1 target-byte pairing
///
/// `target` is a typed enum; the wire byte it encodes is pinned
/// by const-asserts in `wire.rs`. The `name: &impl DescribeName`
/// constraint (sealed trait in `ident.rs`) restricts callers to
/// `StmtName` or `PortalName` — passing a raw `&[u8]` is a type
/// error, closing the "caller always passes the right typed name"
/// tier-3 discipline gap.
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

/// Build a PostgreSQL Extended Query `Close` (`'C'`) frame
/// (PG §55.7).
///
/// Wire layout: tag `'C'`, 4-byte BE length (self-inclusive),
/// single target byte (`'S'` statement or `'P'` portal via
/// [`crate::wire::CloseTargetByte`]), NUL-terminated name.
///
/// Mirrors [`build_describe_message`] verbatim — same wire shape
/// (1-byte tag + length-prefixed (1-byte target + name CSTR));
/// only the tag (`'C'` vs `'D'`) and target-byte enum differ. The
/// `name: &impl DescribeName` sealed-trait reuse is intentional:
/// the trait enumerates exactly the two acceptable name types
/// (`StmtName` for `'S'`, `PortalName` for `'P'`), which are the
/// same two name types Close accepts. A future rename of
/// `DescribeName` → `StmtOrPortalName` would be a one-grep
/// refactor across this file + `ident.rs`.
///
/// `#[inline]` rationale matches [`build_describe_message`].
#[inline]
fn build_close_message<N: crate::ident::DescribeName>(
    target: crate::wire::CloseTargetByte,
    name: &N,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_CLOSE.byte())?;
    reserved.with_length_prefix(|w| {
        w.push_u8(target.byte())?;
        w.push_nul_terminated(name.as_describe_name_bytes())?;
        Ok(())
    })?;
    crate::action::WriteRange::from_write_span(start, reserved)
}

/// Compute the transition for [`PgCommand::DescribeStatement`]
/// against the current [`ProtoState`]. Pure; see [`compute_push`]
/// for framing.
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
    // `&mut ProtoState` signature: see `compute_push_ping` rationale.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // IdleState typestate is the precondition.
            let idle = match crate::state_setter::IdleState::try_from(state) {
                Some(idle) => idle,
                None => {
                    // session audit: glass pattern
                    // (debug_assert + return) replaced with
                    // classified error. See CrateBugLocus::
                    // PushClassIdleMismatch.
                    *state = ProtoState::Errored(
                        crate::error::StateErrorKind::from_kind_or_internal(
                            crate::error::ErrorKind::Internal,
                        ),
                    );
                    emit_actions!(staged, budget: 2, [
                        StagedAction::FailReply {
                            id: reply.consume(),
                            cause: ProtocolError::InternalCrateBug {
                                locus: crate::error::CrateBugLocus::PushClassIdleMismatch,
                            },
                        },
                        StagedAction::CloseSocket,
                    ]);
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

/// Idle-only path for the `DescribeStatement` push.
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
    // Typed witness pairs DescribeStatement → DescribeStatementAwaitingParamDesc.
    setter.install_post_state(
        crate::push_command::DescribeStatementAwaitingParamDescInstall { reply },
    );
}

/// Compute the transition for [`PgCommand::DescribePortal`] against
/// the current [`ProtoState`]. Pure; see [`compute_push`] for
/// framing.
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
    // `&mut ProtoState` signature: see `compute_push_ping` rationale.
    match state.push_class() {
        crate::state::StatePushClass::Idle => {
            // IdleState typestate is the precondition.
            let idle = match crate::state_setter::IdleState::try_from(state) {
                Some(idle) => idle,
                None => {
                    // session audit: glass pattern
                    // (debug_assert + return) replaced with
                    // classified error. See CrateBugLocus::
                    // PushClassIdleMismatch.
                    *state = ProtoState::Errored(
                        crate::error::StateErrorKind::from_kind_or_internal(
                            crate::error::ErrorKind::Internal,
                        ),
                    );
                    emit_actions!(staged, budget: 2, [
                        StagedAction::FailReply {
                            id: reply.consume(),
                            cause: ProtocolError::InternalCrateBug {
                                locus: crate::error::CrateBugLocus::PushClassIdleMismatch,
                            },
                        },
                        StagedAction::CloseSocket,
                    ]);
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

/// Idle-only path for the `DescribePortal` push.
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
    // Typed witness pairs DescribePortal → DescribePortalAwaitingRowDescOrNoData.
    setter.install_post_state(
        crate::push_command::DescribePortalAwaitingRowDescOrNoDataInstall { reply },
    );
}

/// Idle-only path for the `Close` push (statement or portal target).
///
/// Mirrors [`compute_push_describe_statement_idle_only`] / [`..._portal`]
/// — emits TWO actions (`SendBytes(Close frame)` + `SendBytes(SYNC)`),
/// then installs the typed witness pairing to
/// [`crate::state::ProtoState::CloseAwaitingComplete`]. The
/// target byte (`Statement` vs `Portal`) flows into the wire frame
/// but the post-push state machine treats both paths uniformly —
/// both targets produce identical response sequences (`CloseComplete`
/// → `ReadyForQuery`).
#[inline]
pub(crate) fn compute_push_close_idle_only<N: crate::ident::DescribeName>(
    setter: crate::state_setter::StateSetter<
        '_,
        crate::push_command::CloseAwaitingCompleteInstall,
    >,
    target: crate::wire::CloseTargetByte,
    name: &N,
    reply: ReplyId<crate::reply_id::CloseKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    let range = try_builder!(
        build_close_message(target, name, reserved),
        setter,
        reply,
        staged
    );
    emit_actions!(staged, budget: 2, [
        StagedAction::SendBytesRange(range),
        StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
    ]);
    // Typed witness pairs CloseStatement / ClosePortal →
    // CloseAwaitingComplete. Unified across the two close targets —
    // the wire-level target byte distinction lives in the emitted
    // Close frame, not in the state machine.
    setter.install_post_state(
        crate::push_command::CloseAwaitingCompleteInstall { reply },
    );
}

/// Typed [`crate::action::WriteRange`] newtype identifying a `Bind`
/// frame body. Constructed by [`build_bind_message`]; consumed by
/// [`stage_bind_execute_sync`] (the only caller). Swapping with
/// [`ExecuteRange`] at the consumer is a type error.
///
/// Tuple-struct field is module-private: only `mod protocol` can
/// project to the inner `WriteRange`. Tier-1 by-construction:
/// type-distinct from `ExecuteRange`, no path to silent reorder.
struct BindRange(crate::action::WriteRange);

/// Typed [`crate::action::WriteRange`] newtype identifying an
/// `Execute` frame body. Sibling of [`BindRange`].
struct ExecuteRange(crate::action::WriteRange);

/// Single-purpose stager for the `Bind`+`Execute`+`Sync` frame
/// triple. Argument order pins frame order; the const-asserted
/// `budget: 3` matches the three actions emitted; `Sync` is the
/// static [`crate::wire::SYNC_WIRE_BYTES`] reference (zero-copy).
///
/// **Tier-1 closures:**
/// - argument-order swap (`(execute, bind)`) → type error.
/// - missing `Sync` → impossible (function emits all three or none).
/// - missing `Bind` or `Execute` → impossible (function takes both
///   by value, function must be called to stage anything).
///
/// A naive shape would open-code the wire-frame triple inside
/// `compute_push_bind_execute_idle_only` as three `emit_actions!`
/// arms — tier-3 by-discipline: a refactor that reordered them or
/// dropped Sync would compile.
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
/// # Typed return
///
/// Returns [`BindRange`], not raw [`crate::action::WriteRange`]. The
/// typed newtype binds at the boundary so [`stage_bind_execute_sync`]
/// statically rejects an `ExecuteRange` in the bind slot.
// ProtocolError is ~72 B post-ErrorArena externalisation, so the
// Err path stays below the 128 B `result_large_err` threshold.
fn build_bind_message<P: crate::params::ParamsWriter>(
    portal_name: &crate::ident::PortalName,
    stmt_name: &crate::ident::StmtName,
    params: &P,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<BindRange, ProtocolError> {
    let start = reserved.len();
    reserved.push_u8(crate::wire::TAG_BIND.byte())?;
    // `params.write_params` can return Err from a user-impl that
    // overflows its advertised budget OR from a drift between
    // MAX_PARAMS_DATA_TOTAL and MAX_OWNED_SEND_LEN.
    //
    // push_* returns Result<(), WriteBufFull>; `?` propagates
    // through the closure's `-> Result<(), WriteBufFull>` return,
    // and through this builder's `Result<_, ProtocolError>` via
    // `From<WriteBufFull> for ProtocolError` →
    // `BuilderCapacityOverflow`. The `params_err` out-param handles
    // the OTHER failure (user-impl overflow) which is classified as
    // `ParamsWriterOverflow`.
    let mut params_err: Option<ProtocolError> = None;
    reserved.with_length_prefix(|w| {
        w.push_nul_terminated(portal_name.as_bytes())?;
        w.push_nul_terminated(stmt_name.as_bytes())?;
        // Compact format-code block per PG §55.7 Bind spec: "The
        // number of parameter format codes can be zero (all
        // default/text), or ONE (specified code applied to all
        // parameters), or equal the actual number of parameters".
        // For N ≥ 1 all params use binary uniformly → send
        // `n_format_codes = 1, [1]` = 4 bytes, independent of N.
        // For N = 0 keep `n_format_codes = 0` (text-default,
        // irrelevant with no params) — avoids the server-side
        // "1 format code but 0 params" edge case some PG forks
        // might log. A naive shape would send `n_format_codes =
        // P::COUNT` followed by `P::COUNT × u16_be(1)` (for N=16
        // that's 34 bytes of format codes + 2 bytes of count).
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
        // n_result_formats = 0 → server default (all text). This
        // crate does not negotiate per-column result formats; the
        // user dispatches between text and binary decoders via the
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
/// currently always `0` (fetch all). The enum narrows the API to
/// only variants the current scope supports, turning what would be
/// tier-3 docs into tier-1 compile.
///
/// # Typed return
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
    // Typed builder fn pins frame order (Bind → Execute → Sync).
    // Argument-order swap → type error.
    stage_bind_execute_sync(staged, bind_range, execute_range);
    // Caller-supplied RowDesc lands in the protocol's single slot
    // BEFORE the state transition. The variant shape (Select vs
    // Dml) is the tier-1 signal that the slot is populated.
    //
    // Typed witness pairs BindExecute →
    // BindExecuteAwaitingBindComplete{Dml,Select}. Park via leaf
    // submodule `_bind_execute_select_install_leaf::install_select_transition`
    // which mints a `BeSelectToken` (private-field, leaf-gated
    // mint) and routes to `RowDescSlotCell::park_at_be_select`.
    let post_install = match row_desc {
        Some(desc) => {
            _bind_execute_select_install_leaf::install_select_transition(row_desc_slot, desc);
            crate::push_command::BindExecutePostInstall::Select { reply }
        }
        None => crate::push_command::BindExecutePostInstall::Dml { reply },
    };
    setter.install_post_state(post_install);
}

/// Idle-only push path for [`crate::push_command::ExecutePortal`].
///
/// Sends `Execute` + `Sync` (NO `Bind` — the portal was bound on a
/// prior `BindExecute`). State transitions directly to the post-
/// `BindComplete` shape (`AwaitingDataOrCompleteSelect` for the
/// Select path, `AwaitingCommandCompleteDml` for Dml).
///
/// # PG semantics
///
/// PG §55.2.7: a bound portal can be Executed repeatedly. Each
/// `Execute` consumes rows from the portal's row stream; the cursor
/// state is maintained on the server. Resume after `PortalSuspended`
/// reads the next batch. Resume after `CommandComplete` is a
/// protocol-level error (the portal is consumed) — server emits
/// `ErrorResponse` which our state machine routes through the
/// existing DrainRfqAfterError path.
///
/// # Why no `BindComplete` wait
///
/// The current state transition installs
/// `BindExecuteAwaitingDataOrCompleteSelect { reply }` directly,
/// skipping the `BindExecuteAwaitingBindCompleteSelect` step. This
/// is correct because the server emits `BindComplete` only in
/// response to a `Bind` frame — and this resume command sends no
/// `Bind`. A naive shape that installed `AwaitingBindCompleteSelect`
/// here would tear down on the first real frame (DataRow / CommandComplete)
/// arriving where `BindComplete` was expected.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors compute_push_bind_execute_idle_only's signature shape — \
              each parameter corresponds to a 1:1 wire-frame argument (portal_name, \
              row_desc, fetch, reply) or a structural staging slot (setter, \
              row_desc_slot, staged, reserved); collapsing them into a struct \
              would (a) add a per-call construction site, (b) opacify the wire \
              parameter list at the call site"
)]
#[inline]
pub(crate) fn compute_push_execute_portal_idle_only(
    setter: crate::state_setter::StateSetter<'_, crate::push_command::ExecutePortalPostInstall>,
    row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
    portal_name: &crate::ident::PortalName,
    row_desc: Option<crate::decode::RowDesc>,
    fetch: crate::command::FetchRows,
    reply: ReplyId<crate::reply_id::QueryKind>,
    staged: &mut StagedActions,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) {
    let execute_range = try_builder!(
        build_execute_message(portal_name, fetch, reserved),
        setter,
        reply,
        staged
    );
    emit_actions!(staged, budget: 2, [
        StagedAction::SendBytesRange(execute_range.0),
        StagedAction::SendBytesStatic(&crate::wire::SYNC_WIRE_BYTES),
    ]);
    let post_install = match row_desc {
        Some(desc) => {
            _bind_execute_select_install_leaf::install_select_transition(row_desc_slot, desc);
            crate::push_command::ExecutePortalPostInstall::Select { reply }
        }
        None => crate::push_command::ExecutePortalPostInstall::Dml { reply },
    };
    setter.install_post_state(post_install);
}

// ═════════════════════════════════════════════════════════════════════
// Idle-only push path for the `prepared!` macro's
// `BindPrepared<'q, P, R>` command. Sister to
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
/// Emits the pre-baked Parse and Bind-prefix bytes (the macro
/// computed them at expansion time; caller pays zero CPU on the
/// header construction), appends the per-param payload via the
/// existing `ParamsWriter` path, and stages the static Execute +
/// Sync frames at the end.
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
        // Synthesise a RowDesc from `q.row_oids` (all-text format).
        // The macro's row_oids list is small (≤ 16) and bounded by
        // MAX_ROW_COLUMNS = 1600; the construction is infallible at
        // runtime.
        let row_desc = match build_synthetic_row_desc(q.row_oids) {
            Ok(desc) => desc,
            Err(cause) => {
                // Architecturally rare: macro emits row_oids of
                // arity > MAX_ROW_COLUMNS would have failed the
                // RowDecode trait bound at compile time (RowDecode
                // tuple impls cap at 16 < 1600). Fall through with a
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
        // Park via the leaf-private token mint
        // (`_bind_execute_select_install_leaf` above).
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
/// [`FormatCode::Text`] (text format in v1).
///
/// Bounded above by [`crate::decode::MAX_ROW_COLUMNS`] = 32. The
/// macro's RowDecode trait impls cap arity at 16 < 32, so this
/// is architecturally always-success; the Result keeps the no-panic
/// discipline.
fn build_synthetic_row_desc(
    oids: &[u32],
) -> Result<crate::decode::RowDesc, ProtocolError> {
    // We need to construct a RowDesc; the existing constructors are
    // `RowDesc::empty()` (0 cols) and the internal `parse_row_description`
    // (parses wire bytes). For the macro path we synthesise directly
    // via a helper on `RowDesc` itself — exposed `pub(crate)` for
    // the prepared module to use.
    crate::decode::RowDesc::from_static_oids_text_format(oids)
}

// No `is_busy_in_flight(&ProtoState) -> bool` helper. A naive
// shape would centralise the "busy-state set" as a bool helper,
// but a guarded match arm is not exhaustive (needs a `_ =>`
// fallback, and every forbid-bundle-legal fallback loses the "new
// variant forces decision" property). Instead the classifier is an
// ENUM (`StatePushClass`); each compute_push_* matches it
// exhaustively (5 variants, no `_` fallback) → tier-1 preserved;
// the variant enumeration lives once in `ProtoState::push_class`
// (no duplication across the 7 compute_push_* helpers). Adding a
// new ProtoState variant fails the build at push_class if
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
/// # Unified classifier
///
/// Routes through [`crate::state::ProtoState::unsolicited_admit`]
/// — one exhaustive match, two bool projections. A naive shape
/// would have this function carry its OWN exhaustive match over
/// `ProtoState`, mirrored byte-for-byte in
/// [`allows_unsolicited_notice_response`] below. Tier-1 closure
/// would exist PER FUNCTION (each `match` exhaustive) but NOT
/// across the pair: a new variant added to one classifier without
/// the other could silently classify asymmetrically (PS accepted,
/// NR rejected, or vice versa). Routing both through the single
/// `unsolicited_admit` source makes drift between the two
/// classifiers structurally impossible.
// Hot inbound dispatch, called per frame. LLVM already inlines
// transparently — `#[inline]` makes the intent explicit (explicit
// > implicit) and pins behaviour against future heuristic shifts.
#[inline]
const fn allows_unsolicited_param_status(state: &ProtoState) -> bool {
    state.unsolicited_admit().allow_param_status
}

/// Classifier for `NoticeResponse` frame acceptance, today identical
/// to [`allows_unsolicited_param_status`] in policy.
///
/// PG server behaviour (§48.5 "Asynchronous Operations"):
/// NoticeResponse may arrive at any time after connection start,
/// BUT this client enforces a stricter client-side invariant:
/// notices are only accepted in states where they would be
/// delivered to the wrapper's async logging channel. Pre-auth
/// states (Connecting*) reject notices to ensure nothing from the
/// server is trusted before authentication completes — a pre-auth
/// MITM-injected notice could carry attacker-controlled text that
/// ends up in operator logs.
///
/// Routes through [`crate::state::ProtoState::unsolicited_admit`].
/// See [`allows_unsolicited_param_status`] for the unification
/// rationale (single exhaustive source, no parallel-classifier
/// drift).
// Same reasoning as `allows_unsolicited_param_status` — LLVM
// already inlines; `#[inline]` pins intent against future heuristic
// shifts.
#[inline]
const fn allows_unsolicited_notice_response(state: &ProtoState) -> bool {
    state.unsolicited_admit().allow_notice_response
}

/// Classification of a `record_param_status` call's outcome.
///
/// The two-variant typed outcome gives the caller diagnostic info
/// (and leaves the compile surface ready for a future
/// wrapper-advisory channel like `Action::EmitPsAdvisory` that
/// could forward `MalformedPayload` events to the user for
/// proxy-interference detection). A naive shape would return `()`
/// and silently drop malformed payloads (missing NUL separator,
/// etc.). Current caller exhaustive-matches both variants,
/// silently consuming for now, but the compile surface is ready
/// for the upgrade.
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
    // Write in-place into the caller-owned `write_buf` and return a
    // typed non-empty `WriteRange` covering the StartupMessage. The
    // branded `BrandedWriteReserved` ties the returned range to the
    // same buffer `reserved` writes into, enabling infallible apply
    // at materialise time.
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
/// [`StagedActions`] into [`OutActions<'w>`] with references
/// into `write_buf_bytes` (`'w`) or `terminal_row_desc` (`'r`).
///
/// Lifetime plumbing: `write_buf_bytes` supplies `'w`;
/// `terminal_row_desc: Option<&'r RowDesc>` supplies `'r` (the
/// parking slot on `PgProtocol`). The borrow checker refuses any
/// `&mut WriteBuf` re-borrow while the returned `OutActions<'w>`
/// is alive, and any `&mut self` re-borrow on `PgProtocol` while
/// `'r` is alive.
//
// No `#[inline]` attribute: an ASM diff confirmed the standalone
// symbol persists with `bl` calls at all 4 call sites — LLVM
// rejects the hint because the body is too large to inline at 4
// sites without net code bloat. The annotation would be
// ineffective noise; the LLVM heuristic is correct here.
//
// `staged: StagedActions<'w>` — the staged container's `'sql`
// lifetime is unified with the WriteBuf's `'w`. This expresses
// "any borrowed SQL bytes inside staged outlive the WriteBuf
// borrow", which is the natural caller-side invariant: caller
// passes `Parse<'a> { sql: &'a str }` AND `&mut WriteBuf` to the
// same `push_command` call; the borrow checker enforces
// `'a >= 'w`. The materialiser then emits
// `Action::SendBytes(&'w [u8])` for both `SendBytesRange` (bytes
// from `write_bytes: &'w [u8]`) and `SendBytesBorrowed` (bytes
// from caller's SQL, lifetime ≥ 'w by the unified parameter).
fn materialise<'w>(
    staged: StagedActions<'w>,
    write_bytes: &'w [u8],
    fail_cause_slot: &mut crate::fail_cause_slot::FailCauseSlotCell,
) -> OutActions<'w> {
    // Capacity invariant: `staged.len() ≤ MAX_STAGED_PER_CALL`
    // (heapless::Vec cap); each staged entry fans out to ≤
    // MAX_FANOUT_PER_STAGED actions. `out.push(a)` below is
    // architecturally infallible via the module-level
    // `const _: () = assert!(MAX_ACTIONS_PER_CALL >= MAX_STAGED_PER_CALL
    // * MAX_FANOUT_PER_STAGED)`. A naive `.unwrap_or(())` on the
    // match-Err arms would be silent-drop ("тихая эрозия") — banned.
    // Form below: explicit match on `push` result with the Err arm
    // a documented dead branch.
    let mut out = OutActions::new();
    for sa in staged {
        // `StagedAction::StreamRowRange` does not exist — DataRow
        // flows via the `iter_rows` fast-path (no staging). The
        // stale-ref class was deleted; `into_public` is infallible.
        let a: Action<'w> = match sa {
            StagedAction::SendBytesRange(range) => {
                // `WriteRange::apply` returns `Option<&[u8]>` — None
                // is architecturally unreachable under intact
                // brand/bounds invariants (see
                // `action.rs::WriteRange::apply` doc), but the
                // Option makes the invariant-break explicit and
                // classified HERE via `CloseSocket` emission. A
                // naive `unwrap_or(&[])` fallback would ship a
                // zero-byte SendBytes to the wire silently.
                match range.apply(write_bytes) {
                    Some(slice) => Action::SendBytes(slice),
                    None => {
                        push_within_fanout_budget(&mut out, Action::CloseSocket);
                        continue;
                    }
                }
            }
            StagedAction::SendBytesStatic(s) => Action::SendBytes(s),
            // `DeliverReplyEntry` carries a lifetime-free
            // `StagedReply`. Materialise reads `row_desc_slot`
            // directly for ALL schema-bearing reply paths
            // (QueryComplete, Describe*Complete) — single source
            // of truth for "is there a schema?". No
            // `schema_present: bool` duplicate on QueryComplete;
            // no `DescribedRowsStaged*` duplicate enums on
            // Describe*Complete; no defensive
            // `debug_assert!(false)` arms. The entry was
            // constructed by the typed `action::deliver` path —
            // kind-payload pairing enforced at dispatch time.
            StagedAction::DeliverReply(entry) => {
                let entry_id = entry.id();
                Action::DeliverReply {
                    id: entry_id,
                    value: entry.staged().into_public(),
                }
            }
            StagedAction::FailReply { id, cause } => {
                // .b: park the cause into the slot at the
                // StagedAction → Action transformation boundary.
                // Public `Action::FailReply` carries only `id`;
                // callers query the cause via `pg.fail_cause()`.
                crate::dispatch::_install_errored_leaf::park_cause_at_install_errored(
                    fail_cause_slot,
                    alloc::boxed::Box::new(cause),
                );
                Action::FailReply { id }
            }
            StagedAction::CloseSocket => Action::CloseSocket,
            // Pass borrowed slice through unchanged. The `'sql: 'w`
            // bound on `materialise` ensures the borrow is at least
            // as long-lived as the WriteBuf's bytes — the returned
            // `Action::SendBytes(&'w [u8])` carries the shorter
            // lifetime safely.
            StagedAction::SendBytesBorrowed(b) => Action::SendBytes(b),
            // : Notify passes through unchanged — pid + arena
            // ref are both `Copy`, no schema resolution at materialise
            // time. The wrapper resolves `notif_ref` via
            // `PgProtocol::get_notification` within the OutActions
            // iteration cycle.
            StagedAction::Notify { pid, notif_ref } => Action::Notify { pid, notif_ref },
            StagedAction::Notice { notice_ref } => Action::Notice { notice_ref },
            // : IntermediateCommandComplete passes through —
            // : `tag_ref` (CommandTagRef, 4 B Copy) passes
            // through unchanged. The wrapper resolves via
            // `PgProtocol::get_command_tag(tag_ref)` within the
            // current OutActions iteration cycle.
            StagedAction::IntermediateCommandComplete { tag_ref } => {
                Action::IntermediateCommandComplete { tag_ref }
            }
            // CopyDataChunk passes through —
            // `chunk_ref` is `Copy`, no schema resolution at
            // materialise time. The wrapper resolves via
            // `PgProtocol::get_copy_chunk`.
            StagedAction::CopyDataChunk { chunk_ref } => {
                Action::CopyDataChunk { chunk_ref }
            }
        };
        push_within_fanout_budget(&mut out, a);
    }
    out
}

/// Push an action with classified dead-arm.
///
/// ## Infallibility proof
///
/// `MAX_ACTIONS_PER_CALL = MAX_STAGED_PER_CALL +
/// MAX_FANOUT2_ENTRIES_PER_CALL × (MAX_FANOUT_PER_STAGED − 1)
/// = 8 + 1 × 1 = 9` (const-asserted at `MAX_ACTIONS_PER_CALL`).
///
/// Each staged entry contributes ≤ `MAX_FANOUT_PER_STAGED = 2`
/// calls to this helper (1-action variants: 1; DeliverReply
/// stale-ref fanout: 2). With `MAX_FANOUT2_ENTRIES = 1` (at most
/// one DeliverReply per call, per the single-inflight invariant),
/// total calls ≤ 9 = `out`'s capacity.
///
/// **Conclusion:** `out.push(a)` is architecturally infallible
/// in this capacity regime. The Err arm is dead.
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
/// alone. A naive `debug_assert!(false, …)` in the Err branch
/// would be a CREDO §V glass pattern (dev loud + release silent
/// fallthrough); the build-time const-assert at
/// `MAX_ACTIONS_PER_CALL` is the actual safety proof, and the
/// runtime Err arm is `core::hint::cold_path()` + silent no-op
/// (architecturally dead under intact invariant; a future
/// refactor that breaks the capacity inequality without updating
/// the const fails to compile rather than reaching this arm).
///
/// ## Why the wrapper vs inline match?
///
/// The function call is `#[inline(always)]` + const-folded in
/// release, so zero runtime overhead. Source-level wrapper
/// centralises the cold-path discipline across 6 materialise
/// sites, avoiding drift (a future 7th site would inherit the
/// correct dead-arm discipline automatically).
#[inline(always)]
fn push_within_fanout_budget<'w>(
    out: &mut OutActions<'w>,
    a: Action<'w>,
) {
    match out.push(a) {
        Ok(()) => {}
        // Architecturally dead in any compiling binary — the const-asserted
        // capacity invariant is the build-time safety net:
        // `MAX_ACTIONS_PER_CALL >= MAX_STAGED_PER_CALL +
        //  MAX_FANOUT2_ENTRIES_PER_CALL × (MAX_FANOUT_PER_STAGED − 1) = 9`
        // (asserted at MAX_ACTIONS_PER_CALL in action.rs). The Err
        // payload carries the rejected `Action` (`heapless::Vec::push`
        // returns `Err(value)`); silent drop is the safe fallback —
        // a future refactor that breaks the capacity inequality
        // without bumping the const fails to compile rather than
        // reaching this arm.
        Err(_) => {
            core::hint::cold_path();
            debug_assert!(false, "OutActions overflow — capacity const-assert broken");
        }
    }
}

// `Default` lives on `<DisconnectedPhase>` only —
// `PgProtocol::default()` produces a fresh disconnected protocol
// (matches `PgProtocol::new()`). A naive blanket
// `impl<P: SealedPhase> Default` would let a user mint a default
// protocol in any phase, breaking the consume-self handshake
// invariants.
impl Default for PgProtocol<DisconnectedPhase> {
    fn default() -> Self {
        Self::new()
    }
}

// Per-phase `Debug` impls. A naive blanket
// `impl<P: SealedPhase> Debug for PgProtocol<P>` would access
// inner fields directly (session_params/state/read_buf), but
// `<DisconnectedPhase>::Inner` is the ZST `DisconnectedInner` (no
// fields), so the blanket form fails to compile. Split into 4
// phase-specific impls — `DisconnectedPhase`'s output is a
// phase-name marker (the storage IS the proof: nothing to show);
// the other three project their `Inner`'s derived Debug.

impl core::fmt::Debug for PgProtocol<DisconnectedPhase> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // `<DisconnectedPhase>::Inner` is ZST — no fields to surface.
        // Phase marker is the only meaningful signal; state is provably
        // Idle by storage absence.
        f.debug_struct("PgProtocol")
            .field("phase", &"DisconnectedPhase")
            .finish_non_exhaustive()
    }
}

impl core::fmt::Debug for PgProtocol<ConnectingPhase> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.inner.fmt(f)
    }
}

impl core::fmt::Debug for PgProtocol<ActivePhase> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // `row_desc_slot` lives on the outer `extras` for
        // `<ActivePhase>`. Surface its populated/empty state
        // alongside the inner's debug projection.
        f.debug_struct("PgProtocol")
            .field("phase", &"ActivePhase")
            .field("row_desc_slot_present", &self.extras.row_desc.as_ref().is_some())
            .field("param_oids_slot_present", &self.extras.param_oids.as_ref().is_some())
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl core::fmt::Debug for PgProtocol<ClosedPhase> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // `<ClosedPhase>::Inner = ClosedInner` (16 B, no
        // PgProtocolInner) — delegates to `ClosedInner`'s derived
        // Debug. The arena overwrite count is surfaced as a numeric
        // diagnostic; `state_kind` is the terminal cause classifier.
        f.debug_struct("PgProtocol")
            .field("phase", &"ClosedPhase")
            .field("cause", &self.inner.cause)
            .field("error_arena_overwrite_count", &self.error_arena_overwrite_count())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod allows_unsolicited_param_status_tests {
    //! Seam-closing table for `allows_unsolicited_param_status`.
    //! The function's exhaustive match returns `true` for four
    //! variants and `false` for five. Swapping any variant between
    //! arms compiles (both arms return `bool`); only a test
    //! enumerating every variant against its expected policy value
    //! catches the drift.
    //!
    //! Category (1) per reforge.md §4.11.

    use super::*;
    use crate::reply_id::ReplyId;
    use core::num::NonZeroU64;

    fn nz(n: u64) -> NonZeroU64 {
        // `nz(0)` is a test bug — a zero raw correlator cannot be
        // minted by a real `ReplyId` allocator (NonZeroU64 by type).
        // A naive `unwrap_or(MIN)` fallback alone would silently
        // coerce `0 → 1`, potentially colliding with a concurrent
        // `nz(1)`. The assert fires loud; the `unwrap_or(MIN)` keeps
        // the forbid-bundle happy (clippy::unwrap_used forbidden) on
        // the assertion-proved dead branch.
        assert!(n > 0, "nz(0) is a test bug — use nz(1..) for non-zero test correlators");
        NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN)
    }

    /// Consume any ReplyId carried by a state so the Drop-guard
    /// does not trip at end-of-scope.
    ///
    /// Delegates to `ProtoState::take_inflight_reply_raw_id` — the
    /// authoritative exhaustive match over all `ProtoState`
    /// variants lives in `state.rs`. A naive hand-rolled 20-line
    /// match here would be a parallel drift surface (every new
    /// variant would need updates in two places); the delegation
    /// closes the drift to a single point.
    ///
    /// The return value is `Option<NonZeroU64>` — `Copy`, no
    /// `Drop` — discarded via a bare `match` arm (avoids the
    /// forbid-bundle-banned `let _ = ...`). Reading the
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

        // `ConnectingStartupScram` carries its `ScramSession`
        // inline as a tier-1 variant-carries-field invariant — the
        // variant cannot be constructed without its required SCRAM
        // data. The classification test only reads the variant tag,
        // but the construction itself is the invariant under test.
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
            expected_server_sig: alloc::boxed::Box::new(crate::scram::types::SecretDigest::new([0_u8; 32])),
        };
        assert!(!allows_unsolicited_param_status(&scram_final));
        consume_state(scram_final);

        let scram_authok = ProtoState::ConnectingScramAwaitingAuthOk(ReplyId::from_raw(nz(7)));
        assert!(!allows_unsolicited_param_status(&scram_authok));
        consume_state(scram_authok);

        // Errored — rejecting (terminal; no traffic accepted).
        // `Errored` carries `StateErrorKind` (1 byte,
        // `AlreadyClosed`-excluded newtype over `ErrorKind`).
        let errored = ProtoState::Errored(crate::error::StateErrorKind::from_kind_or_internal(crate::error::ErrorKind::Framing));
        assert!(!allows_unsolicited_param_status(&errored));
        consume_state(errored);

        // Simple-query states all accept unsolicited
        // `ParameterStatus` — the server may emit one mid-query if
        // an `ALTER SYSTEM` fires. Exhaustive enumeration pins the
        // policy row per-variant.
        let q_first = ProtoState::SimpleQueryAwaitingFirstResponse(ReplyId::from_raw(nz(8001)));
        assert!(allows_unsolicited_param_status(&q_first));
        consume_state(q_first);

        // Streaming-rows state variants do not carry inline
        // `RowDesc`; the schema lives on the per-phase
        // `row_desc_slot` extras cell. Test fixtures construct
        // streaming variants directly without schema; the policy
        // under test (`allows_unsolicited_param_status`) is
        // schema-agnostic.
        let q_rows = ProtoState::SimpleQueryStreamingRows {
            reply: ReplyId::from_raw(nz(8002)),
        };
        assert!(allows_unsolicited_param_status(&q_rows));
        consume_state(q_rows);

        let q_rfq = ProtoState::SimpleQueryAwaitingRfq {
            reply: ReplyId::from_raw(nz(8003)),
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
    //! Per-`StatePushClass` pinning of
    //! `clear_session_residue_for_class` arm bodies.
    //!
    //! The production function uses a wildcard `_ => {}` for the
    //! Connecting / PingAwaiting / BusyQuery preserve-residue arm
    //! (the wildcard form compiles to a single discriminant
    //! compare pair — an explicit 25-variant or-pattern would cost
    //! ~+2 ns; see the comment on `clear_session_residue_for_class`).
    //! The wildcard is tier-2-by-discipline at the broad scope: a
    //! future `ProtoState` variant inherits the wildcard
    //! "preserve" arm silently, with no compile-time signal.
    //!
    //! These tests close the gap at the **`StatePushClass`
    //! granularity** by pinning the per-class residue policy on
    //! observable state:
    //! - **Idle** — `row_desc_slot` cleared; `session_params`
    //!   preserved.
    //! - **Errored(_)** — `row_desc_slot` cleared; `session_params`
    //!   internally `clear()`-ed (verified via `is_pristine()`).
    //! - **Connecting / PingAwaiting / BusyQuery** — every
    //!   observable residue field preserved.
    //!
    //! An arm-body swap (e.g. `Idle => clear session_params`
    //! instead of preserve) trips one of these tests immediately.
    //! Adding a new `StatePushClass` variant requires a new test
    //! arm here too (the test for the new class would be missing —
    //! caught by contributor discipline + code review, not
    //! compile-fail; this is the residual tier-3 surface that
    //! integration-via-public-API would close, but the public-API
    //! path requires real server-frame fixtures that are outside
    //! this test's scope).
    use super::*;
    use crate::decode::RowDesc;
    use crate::error::{ErrorKind, StateErrorKind};
    use crate::reply_id::ReplyId;
    use crate::session_params::SessionParams;
    use crate::state::ActiveState;
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

    /// Test-only constructor for an `<ActivePhase>` proto. The
    /// residue tests need a phase whose `Inner` is `ActiveInner`
    /// (the ZST `DisconnectedInner` carries no residue fields to
    /// exercise), and that without driving a real handshake. This
    /// helper goes through the leaf-private
    /// `_proto_init_leaf::fresh_active_inner()` (callable from
    /// sibling tests within `mod protocol`) wrapped in
    /// `<ActivePhase>`. NOT a production bypass: leaf visibility
    /// gates external access; `phase_marker` is ZST without
    /// external construction; this `fn` is `#[cfg(test)]` private
    /// to the test module.
    fn fresh_active_proto() -> PgProtocol<ActivePhase> {
        PgProtocol {
            inner: _proto_init_leaf::fresh_active_inner(),
            extras: _proto_init_leaf::fresh_active_extras(),
            phase_marker: PhantomData,
        }
    }

    /// Populate every observable residue field on `proto`:
    /// `row_desc_slot = Some(EMPTY)`, `session_params`
    /// non-pristine, `error_arena` allocated. After the test we
    /// observe how each arm of `clear_session_residue_for_class`
    /// mutated them.
    ///
    /// Tightened to `PgProtocol<ActivePhase>` (not generic) because
    /// only the `<ActivePhase>` monomorphisation carries both
    /// `ActiveInner` AND the `RowDescSlotCell` extras.
    fn populate_residue(proto: &mut PgProtocol<ActivePhase>) {
        proto.extras.row_desc._set_for_test(Some(RowDesc::empty()));
        proto.inner.session_params._set_for_test(Some(dirty_session_params()));
        proto.inner.error_arena = Some(alloc::boxed::Box::new(
            crate::error_arena::ErrorArena::new(),
        ));
    }

    /// Replace `proto.inner.state` with `Idle` so the destructor
    /// doesn't trip the in-flight `ReplyId<_>` Drop-guard at scope
    /// end.
    fn quench_inflight(proto: &mut PgProtocol<ActivePhase>) {
        let prev = core::mem::replace(&mut proto.inner.state, ActiveState::Idle);
        match prev.take_inflight_reply_raw_id() {
            Some(_) | None => {}
        }
    }

    fn session_params_is_pristine(proto: &PgProtocol<ActivePhase>) -> bool {
        // Trait method via `Pristine` import. The inherent
        // `__pristine_const` would also work, but trait dispatch
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
        let mut proto = fresh_active_proto();
        // Default state is `Idle` post-`fresh_inner()`.
        populate_residue(&mut proto);
        let class = proto.inner.state.push_class();
        proto.inner.clear_session_residue_for_class(&mut proto.extras, class);

        assert!(
            proto.extras.row_desc.is_none(),
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
        let mut proto = fresh_active_proto();
        proto.inner.state = ActiveState::Errored(
            StateErrorKind::from_kind_or_internal(ErrorKind::Framing),
        );
        populate_residue(&mut proto);
        let class = proto.inner.state.push_class();
        proto.inner.clear_session_residue_for_class(&mut proto.extras, class);

        assert!(
            proto.extras.row_desc.is_none(),
            "Errored must clear row_desc_slot",
        );
        assert!(
            proto.inner.session_params.is_some(),
            "Errored preserves session_params Box (only contents cleared)",
        );
        assert!(
            session_params_is_pristine(&proto),
            "Errored MUST clear session_params content (forfeit on tear-down)",
        );
        // No state mutation back to Idle here — Errored is terminal.
        // Drop-guard for `Errored(StateErrorKind)` is fine: the kind
        // is `Copy`, no in-flight ReplyId to consume.
    }

    #[test]
    fn connecting_preserves_all_residue() {
        // `<ActivePhase>` cannot hold a `ConnectingStartupTrust`
        // state — the variant doesn't exist in `ActiveState`
        // (tier-1 by storage absence). Test the class-arm directly
        // via the `StatePushClass::Connecting` constant fed to
        // `clear_session_residue_for_class`, with the state held
        // at any legal `ActiveState` value (`Idle` here for shape).
        let mut proto = fresh_active_proto();
        proto.inner.state = ActiveState::Idle;
        populate_residue(&mut proto);
        proto.inner.clear_session_residue_for_class(
            &mut proto.extras,
            crate::state::StatePushClass::Connecting,
        );

        assert!(
            proto.extras.row_desc.is_some(),
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
        let mut proto = fresh_active_proto();
        proto.inner.state = ActiveState::PingAwaitingRfq(ReplyId::from_raw(nz(12)));
        populate_residue(&mut proto);
        let class = proto.inner.state.push_class();
        proto.inner.clear_session_residue_for_class(&mut proto.extras, class);

        assert!(
            proto.extras.row_desc.is_some(),
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
        let mut proto = fresh_active_proto();
        proto.inner.state = ActiveState::SimpleQueryStreamingRows {
            reply: ReplyId::from_raw(nz(13)),
        };
        populate_residue(&mut proto);
        let class = proto.inner.state.push_class();
        proto.inner.clear_session_residue_for_class(&mut proto.extras, class);

        assert!(
            proto.extras.row_desc.is_some(),
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
    //! Seam-closing tests for the pure push-compute split.
    //!
    //! The push-path decision table is enumerated per
    //! `(cmd, state)` pair; every arm of [`compute_push_ping`] and
    //! [`compute_push_startup_idle_only`] is exercised and its
    //! `(new_state, actions)` output is pinned. Swapping any two
    //! arm bodies would compile (identical return shape
    //! `ProtoState`, identical `emit_actions!` budget), so the
    //! only shield for the policy table is this enumeration.
    //!
    //! Category (1) per reforge.md §4.11 — exhaustive-match policy
    //! table. Companion to `allows_unsolicited_param_status_tests`
    //! above (same test style, same helpers).
    //!
    //! These tests also stand as the proof that the pure half is
    //! testable without constructing [`PgProtocol`]: every test
    //! calls [`compute_push`] directly on a synthesised
    //! `(cmd, state)` pair.
    use super::*;
    use crate::reply_id::ReplyId;
    use core::num::NonZeroU64;

    fn nz(n: u64) -> NonZeroU64 {
        // `nz(0)` is a test bug — a zero raw correlator cannot be
        // minted by a real `ReplyId` allocator (NonZeroU64 by type).
        // A naive `unwrap_or(MIN)` fallback alone would silently
        // coerce `0 → 1`, potentially colliding with a concurrent
        // `nz(1)`. The assert fires loud; the `unwrap_or(MIN)` keeps
        // the forbid-bundle happy (clippy::unwrap_used forbidden) on
        // the assertion-proved dead branch.
        assert!(n > 0, "nz(0) is a test bug — use nz(1..) for non-zero test correlators");
        NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN)
    }

    /// Consume any ReplyId carried by `state` so its Drop-guard
    /// does not trip when the state drops at end of scope.
    ///
    /// Delegates to `ProtoState::take_inflight_reply_raw_id`
    /// (exposed as `pub(crate)` in `state.rs`). Both this module
    /// and `allows_unsolicited_param_status_tests` delegate to that
    /// single authoritative match — new variants categorised once
    /// in `state.rs` automatically flow through all test helpers,
    /// eliminating parallel-match drift.
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

    /// Test-only observation of a [`StagedAction`] — brand
    /// stripped, range carried as `NonEmptyRange`. Tests compare
    /// against this instead of `StagedAction` directly because
    /// `'wb` is HRTB-fresh per call site and cannot be named
    /// outside the branded closure that produced it.
    ///
    /// `ProtocolError` is `Copy + Clone` so `FailReply`'s full
    /// cause variant is preserved — tests match specific causes
    /// like `cause: ProtocolError::ConnectionAlreadyClosed { prior_kind }`.
    ///
    /// Variants covered are those `compute_push` produces. A
    /// naive collapse of an unexpected `StreamRowRange` (only
    /// emitted from the `feed_bytes` DATA_ROW arm) into
    /// `CloseSocket` would mask an architectural bug — instead,
    /// such a path would be observed as a distinct variant by
    /// pattern-matching tests rather than silently absorbed.
    //
    // `ProtocolError` is ~72 B post-`ErrorArena`, so this enum no
    // longer triggers `clippy::large_enum_variant`.
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
                // Borrowed bytes don't appear in the cfg(test)
                // `PgCommand`-driven path — no `Parse` /
                // `SimpleQuery` test fixtures route through this
                // enum. Keep an explicit arm to fail the build if a
                // future test introduces a borrowed-SQL path here
                // without updating the observation type.
                StagedAction::SendBytesBorrowed(_) => Self::SendBytesStatic(b""),
                // : `Notify` is staged ONLY by the dispatch
                // pre-filter on `'A'` tags during feed_bytes; no test
                // fixture in this module exercises that path. Map to
                // a sentinel `CloseSocket` if a future test ever
                // routes through here (build will pass; the test
                // observing actions sees `CloseSocket` and either
                // tolerates it or fails on the unexpected variant).
                StagedAction::Notify { .. } | StagedAction::Notice { .. } => Self::CloseSocket,
                // : IntermediateCommandComplete — same
                // sentinel-CloseSocket mapping; no cfg(test) PgCommand
                // fixture exercises multi-statement batch dispatch.
                StagedAction::IntermediateCommandComplete { .. } => Self::CloseSocket,
                // CopyDataChunk — same sentinel.
                StagedAction::CopyDataChunk { .. } => Self::CloseSocket,
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
        // `compute_push` takes `&mut state`. Closure captures
        // `&mut state_var` to mutate in place; returns the obs
        // vec. After closure, `state_var` holds the post-push
        // state.
        let mut wb = WriteBuf::new();
        let mut state_var = state;
        let obs = wb.with_branded(|mut wb| {
            let mut reserved = wb.reserve();
            let staged = compute_push(cmd, &mut state_var, &mut reserved);
            let mut obs: heapless::Vec<StagedObs, MAX_ACTIONS_PER_CALL> = heapless::Vec::new();
            for a in &staged {
                let push_result = obs.push(StagedObs::from_staged(a));
                assert!(
                    push_result.is_ok(),
                    "MAX_ACTIONS_PER_CALL overflow in test fixture",
                );
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

        // Action: exactly one SendBytes whose payload is
        // `SYNC_WIRE_BYTES` — Ping from Idle emits the static
        // Sync wire-bytes const.
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
        // On push against `Errored`, the protocol emits a
        // `ConnectionAlreadyClosed { prior_kind }` — the full
        // original cause was surfaced in the earlier `FailReply`
        // (when the connection was first torn down). The state
        // retains only the kind (1-byte `StateErrorKind`,
        // `AlreadyClosed`-free newtype over `ErrorKind`).
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
        // ConnectingStartupTrust — no credentials payload.
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

        // ConnectingStartupScram — tier-1 variant-carries-field
        // invariant: `scram: ScramSession` lives INSIDE this
        // variant and the variant cannot be constructed without
        // it.
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
                expected_server_sig: alloc::boxed::Box::new(crate::scram::types::SecretDigest::new([0_u8; 32])),
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
    // Startup — per-variant policy table (structurally collapsed)
    //
    // The Startup push lives on `<DisconnectedPhase>::push_startup`
    // (consume-self) and the type system physically forbids
    // pushing Startup from any other state — pushing from a
    // non-Disconnected phase is a method-absent E0599 compile
    // error, not a `FailReply` runtime classification. So the
    // 3 prior runtime tests
    // (`startup_from_idle_transitions_and_emits_startup_message`,
    //  `startup_from_errored_preserves_kind_and_fails_with_connection_already_closed`,
    //  `startup_from_non_idle_non_errored_fails_with_startup_in_progress`)
    // describe invariants that are now STRUCTURALLY IMPOSSIBLE and
    // have been removed.
    //
    // The Idle arm (which DID produce real wire-bytes) is
    // preserved structurally: `<DisconnectedPhase>::push_startup`
    // exercises the exact same `compute_push_startup_idle_only`
    // body, so the wire shape is unchanged. Integration tests in
    // `tests/startup_spec.rs` cover the wire-shape end-to-end.
    // -----------------------------------------------------------------

    #[test]
    #[allow(dead_code, reason = "Placeholder anchor: the 3 prior `startup_from_*` runtime tests defended the legacy `compute_push_startup` 5-arm dispatcher (Idle/Errored/Connecting/PingAwaiting/BusyQuery). The typed entry point `<DisconnectedPhase>::push_startup` is consume-self, so non-Idle dispatches are E0599 at compile time — the runtime tests would describe invariants that are now structurally impossible. This anchor exists so a grep for `fn startup_from_` in protocol.rs lands here with the explainer block above.")]
    fn _startup_dispatch_table_collapsed_compile_anchor() {
        // Empty body — the test exists only as a comment anchor.
    }

    // ═════════════════════════════════════════════════════════════
    // ParamsWriterOverflow classified-Err end-to-end routing test
    // ═════════════════════════════════════════════════════════════

    /// A user-space `ParamsWriter` that always returns
    /// `Err(WriteBufFull)` — simulating a buggy / adversarial
    /// impl whose `write_params` overflows its advertised budget.
    /// Exercises the classified-Err path: `build_bind_message` →
    /// `CrateBugLocus::ParamsWriterOverflow` → `try_builder!`
    /// macro → `Result::Err(PushFailure)` + atomic transition to
    /// `Errored`.
    ///
    /// A naive `debug_assert!(false, …)` in the Err arm would be
    /// dev-loud + release-silent — it would ship a truncated Bind
    /// frame with miscomputed length prefix to the wire (tier-4
    /// silent wire-level corruption). This test pins the
    /// classified routing end-to-end: a failing `ParamsWriter`
    /// MUST surface as `Result::Err(PushFailure)` with the
    /// connection transitioned to `Errored`, NOT a broken
    /// Bind/Execute/Sync triplet.
    #[test]
    fn bind_execute_params_overflow_routes_to_classified_failreply() {
        use crate::error::{CrateBugLocus, ProtocolError};
        use crate::params::OverflowParams;

        // `PgProtocol::new()` produces `<DisconnectedPhase>`, but
        // this test wants `<ActivePhase>` to exercise
        // `push_bind_execute`. The in-crate cfg(test) path
        // constructs the Active wrapper directly through the
        // leaf-private `_proto_init_leaf::fresh_active_inner()` /
        // `fresh_active_row_desc_slot()` (`pub(in crate::protocol)`)
        // with a re-tagged `phase_marker`. NOT a production
        // bypass: external crates cannot reach the `inner` field
        // (module-private to `mod protocol`), cannot reach
        // `fresh_active_inner` (leaf visibility), and
        // `phase_marker` is ZST without external construction.
        let mut proto: PgProtocol<ActivePhase> = PgProtocol {
            inner: _proto_init_leaf::fresh_active_inner(),
            extras: _proto_init_leaf::fresh_active_extras(),
            phase_marker: PhantomData,
        };
        let mut wb = WriteBuf::new();
        let reply_raw = nz(999);
        // The test goes through `ReadyGuard` — the only
        // legitimate path that runtime-classifies state as `Idle`
        // via `as_ready`. `push_command_internal` re-checks via
        // the `IdleState::try_from` typestate at entry; production
        // callers always satisfy the check. A fresh proto is in
        // `Idle` state so `as_ready()` returns `Some`. The
        // architecturally-dead `None` arm early-returns to satisfy
        // the lib-level `clippy::panic` forbid bundle.
        let Some(guard) = proto.as_ready() else { return };
        // `push_bind_execute` borrows the identifier args for the
        // `'w` lifetime that flows into the returned
        // `Result<OutActions, PushFailure>`. Named bindings are
        // required to keep the borrows alive past the call for the
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

        // Classified Err routes through
        // `Result::Err(PushFailure)`. The atomic state transition
        // to `Errored` happens inside `push_bind_execute_internal`
        // via `install_errored`; the caller learns of the failure
        // via the typed `PushFailure { id, cause }` (~80 B) — no
        // `OutActions` 800-B return frame, no per-call action
        // iteration.
        //
        // A naive silent-corruption path would have shipped a
        // truncated Bind frame on the wire (3-action bundle with
        // miscomputed length-prefix); the typed contract surfaces
        // the classified failure at the type-system level.
        assert!(
            result.is_err(),
            "ParamsWriter Err must route to Result::Err(PushFailure); \
             got Ok — silent-corruption regression?",
        );
        // Architecturally dead via the assert above; `let-else { return }`
        // is the forbid-bundle-clean dead-arm landing pad (no panic!,
        // no unwrap!, no expect! on the success path).
        let Err(failure) = result else { return };

        assert_eq!(
            failure.id, reply_raw,
            "PushFailure.id must echo the consumed correlator (ReplyId discipline)",
        );
        assert!(
            matches!(
                &*failure.cause,
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
            matches!(proto.state(), crate::state::ActiveState::Errored(_)),
            "ParamsWriterOverflow triggers terminal Errored state, \
             not a recoverable preserved-state path. Got: {:?}",
            proto.state(),
        );
    }

    // ───────────────────────────────────────────────────────────────
    // Pin tests for the remaining `compute_push_*` Idle-arm
    // transitions.
    //
    // The `&mut state` signatures only require that the Idle arm
    // WRITE `*state = <next>`; preserve arms simply leave the
    // state untouched. A naive 6th `compute_push_*` helper that
    // forgot `*state = ...` in its Idle arm would compile,
    // leaving state unchanged at runtime. These pin tests catch
    // that omission via a runtime assertion on the post-Idle
    // state's variant.
    //
    // Ping + Startup are already covered by tests above; these
    // close the rest of the surface.
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
        // Errored / preserve arms MUST NOT write `*state`. Trip a
        // SimpleQuery against `Errored` — state must remain at the
        // EXACT same `Errored(prior_kind)` it held before.
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
