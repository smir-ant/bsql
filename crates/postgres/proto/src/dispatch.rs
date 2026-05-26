//! `(prev_state, frame_tag) → outcome` matcher.
//!
//! The dispatcher is the **single** place the protocol decides what to
//! do with a freshly-parsed frame given the current state. The match is
//! exhaustive over `(state, tag)` pairs; adding a new state or tag is a
//! build error until it is wired into the matcher.
//!
//! # Payload contract — tier-1 via slice patterns
//!
//! The caller has already parsed the header (5 bytes: tag + 4-byte BE
//! length) and verified that the full frame is buffered. It passes the
//! dispatcher the **payload** — the bytes *after* the header, of length
//! `total_len - 5`. Every arm that needs to inspect bytes uses a slice
//! pattern (`[b0]`, `[b0, b1, ..]`, etc.) so the compiler enforces the
//! length / presence check.
//!
//! # Tag-LUT path rejected — DO NOT retry without new measurement
//!
//! A tempting refactor is to replace `match (prev, tag: InboundTag)`
//! with a compact `InboundTagClass` enum (17 dense variants + classify
//! step) under the hypothesis "dense-discriminant jump table beats
//! sparse-ASCII-byte switch". This was implemented, measured, and
//! rejected (commit `1a762ca`, reverted).
//!
//! Measured result (criterion baseline, aarch64-apple-darwin): all 4
//! hot-path benches regressed (+2.6% to +8.2%, p<0.05). Modern LLVM
//! already lowers the byte switch into a compact cmp-and-branch chain
//! that CSEs across arms; the extra classify() call +
//! `InboundTagClass::Unknown` catch-all branch add indirection LLVM
//! cannot fold out. Hypothesis falsified.
//!
//! If you are tempted to re-open this: first produce a NEW criterion
//! measurement refuting the rejection result (different machine,
//! different LLVM, or architectural change in the dispatch loop) —
//! the rejection is gated on measurement, not on opinion.

use crate::action::StagedAction;
use crate::error::ProtocolError;
use crate::reply_id::ReplyId;
use crate::state::ProtoState;

// ═════════════════════════════════════════════════════════════════════
// Schema-side concrete-token leaf
//
// The leaf hosts a CONCRETE `TDispatchToken` type with a private
// tuple-struct field; the literal `Self(())` mint is callable
// ONLY inside this submodule. The cell's `park_at_t_dispatch`
// method takes `TDispatchToken` by value — there is no trait to
// `impl` for hostile types, no sealed-supertrait to route around.
// Three call sites (simple-query, describe-statement,
// describe-portal 'T' arms) all invoke the leaf helper.
//
// A naive `impl SchemaWriteAuth for AtRowDescriptionDispatch`
// sealed-trait shape would be tier-1 EXTERNAL but
// tier-2-by-discipline WITHIN-CRATE: any in-crate file could
// `impl Sealed + SchemaWriteAuth for HostileTag` and bypass.
//
// See `mod protocol` for the parallel schema-slot leaves and the
// session_params leaves.
// ═════════════════════════════════════════════════════════════════════

/// Leaf submodule for the inbound `'T'` (RowDescription) frame
/// dispatch. Hosts the [`TDispatchToken`] type and the single park
/// helper fn.
pub(crate) mod _row_description_dispatch_leaf {
    /// Leaf-scope token. The tuple-struct field is PRIVATE to this
    /// submodule — `Self(())` mints are callable ONLY here. The type
    /// itself is `pub(crate)` so
    /// [`crate::schema_slot::RowDescSlotCell::park_at_t_dispatch`] can
    /// name it in its parameter signature.
    pub(crate) struct TDispatchToken(());

    /// Mint a [`TDispatchToken`] and write `row_desc` into `slot` via
    /// [`crate::schema_slot::RowDescSlotCell::park_at_t_dispatch`].
    /// Used by all three 'T' arm dispatch transitions (simple-query,
    /// describe-statement, describe-portal).
    #[inline]
    pub(in crate::dispatch) fn park_row_description_at_dispatch(
        slot: &mut crate::schema_slot::RowDescSlotCell,
        row_desc: crate::decode::RowDesc,
    ) {
        slot.park_at_t_dispatch(row_desc, TDispatchToken(()));
    }
}

/// Leaf submodule for the inbound `'C'` (CommandComplete) frame
/// dispatch. — hosts [`CommandCompleteDispatchToken`]
/// and the single park helper fn. Mirror of
/// [`_row_description_dispatch_leaf`] /
/// [`_param_description_dispatch_leaf`].
pub(crate) mod _command_complete_dispatch_leaf {
    /// Leaf-scope token. The tuple-struct field is PRIVATE to this
    /// submodule. The type itself is `pub(crate)` so
    /// [`crate::command_tag_slot::CommandTagSlotCell::park_at_command_complete_dispatch`]
    /// can name it.
    pub(crate) struct CommandCompleteDispatchToken(());

    /// Mint a [`CommandCompleteDispatchToken`] and park the boxed
    /// command tag in the slot. Sole call sites: dispatch C arms
    /// in SimpleQueryAwaitingRfq / BindExecuteAwaitingRfq{Dml,Select}.
    #[inline]
    pub(in crate::dispatch) fn park_command_tag_at_dispatch(
        slot: &mut crate::command_tag_slot::CommandTagSlotCell,
        tag: alloc::boxed::Box<crate::command_tag::CommandTag>,
    ) {
        slot.park_at_command_complete_dispatch(
            tag,
            CommandCompleteDispatchToken(()),
        );
    }
}

/// Leaf submodule for the inbound `'t'` (ParameterDescription) frame
/// dispatch. Hosts the [`ParamDescDispatchToken`] type and the single
/// park helper fn. mirror of
/// [`_row_description_dispatch_leaf`].
///
/// Sole call site:
/// `(ProtoState::DescribeStatementAwaitingParamDesc, TAG_PARAMETER_DESCRIPTION)`
/// dispatch arm. Parses the wire bytes into a boxed
/// [`crate::decode::ParamOids`] then hands the Box to the slot via
/// [`crate::param_oids_slot::ParamOidsSlotCell::park_at_param_desc_dispatch`].
///
/// State variant transitions to
/// [`ProtoState::DescribeStatementAwaitingRowDescOrNoData`] AFTER
/// the park; the variant carries only the bare `ReplyId` post-/// (no more `param_oids: Box<ParamOids>` field — the slot owns the
/// box).
pub(crate) mod _param_description_dispatch_leaf {
    /// Leaf-scope token. The tuple-struct field is PRIVATE to this
    /// submodule — `Self(())` mints are callable ONLY here. The type
    /// itself is `pub(crate)` so
    /// [`crate::param_oids_slot::ParamOidsSlotCell::park_at_param_desc_dispatch`]
    /// can name it in its parameter signature.
    pub(crate) struct ParamDescDispatchToken(());

    /// Mint a [`ParamDescDispatchToken`] and write `param_oids` into
    /// `slot` via
    /// [`crate::param_oids_slot::ParamOidsSlotCell::park_at_param_desc_dispatch`].
    /// Sole call site: `'t'` arm dispatch transition for
    /// `ProtoState::DescribeStatementAwaitingParamDesc`.
    ///
    /// The `param_oids` is passed by Box (allocated once at parse-time
    /// inside this arm's parser call) — slot owns the heap.
    #[inline]
    pub(in crate::dispatch) fn park_param_oids_at_dispatch(
        slot: &mut crate::param_oids_slot::ParamOidsSlotCell,
        param_oids: crate::action::ParamOids,
    ) {
        slot.park_at_param_desc_dispatch(param_oids, ParamDescDispatchToken(()));
    }
}

/// Leaf submodule for the inbound `'Z'` (ReadyForQuery) frame
/// dispatch. Hosts the [`RfqDispatchToken`] type and the park helper
/// fn. mirror of [`_command_complete_dispatch_leaf`].
///
/// Sole call sites: every `'Z'`-handling dispatch arm
/// (`PingAwaitingRfq`, `SimpleQueryAwaitingRfq`,
/// `BindExecuteAwaitingRfq*`, `DescribeStatement*`, `DescribePortal*`,
/// etc.). Each arm parses the wire byte into a [`crate::action::TxStatus`]
/// via [`parse_rfq_payload`], then parks via this helper.
pub(crate) mod _rfq_dispatch_leaf {
    /// Leaf-scope token for the `'Z'` arm parking call. The
    /// tuple-struct field is PRIVATE to this submodule — `Self(())`
    /// mints are callable ONLY here. Type `pub(crate)` so
    /// [`crate::tx_status_slot::TxStatusSlotCell::park_at_rfq_dispatch`]
    /// can name it in its parameter signature.
    pub(crate) struct RfqDispatchToken(());

    /// Mint a [`RfqDispatchToken`] and write `tx_status` into `slot`
    /// via
    /// [`crate::tx_status_slot::TxStatusSlotCell::park_at_rfq_dispatch`].
    /// Sole call sites: every `'Z'`-handling dispatch arm.
    #[inline]
    pub(in crate::dispatch) fn park_tx_status_at_dispatch(
        slot: &mut crate::tx_status_slot::TxStatusSlotCell,
        tx_status: crate::action::TxStatus,
    ) {
        slot.park_at_rfq_dispatch(tx_status, RfqDispatchToken(()));
    }
}

/// Leaf submodule for the `install_errored` cause-park transition.
/// .b. Hosts the [`InstallErroredToken`] type and the
/// [`park_cause_at_install_errored`] helper. The token's tuple-struct
/// field is PRIVATE to this submodule, so `Self(())` mints are
/// callable ONLY here. Tier-1 within-crate write provenance.
pub(crate) mod _install_errored_leaf {
    /// Leaf-scope token for the `install_errored` cause-park call.
    /// Field private to this submodule; type `pub(crate)` so
    /// [`crate::fail_cause_slot::FailCauseSlotCell::park_at_install_errored`]
    /// can name it in its parameter signature. Naming alone confers
    /// no minting power.
    pub(crate) struct InstallErroredToken(());

    /// Mint an [`InstallErroredToken`] and park `cause` into `slot`
    /// via
    /// [`crate::fail_cause_slot::FailCauseSlotCell::park_at_install_errored`].
    /// Sole call sites: [`super::install_errored`] (dispatch path),
    /// `compute_push_*` error-classification arms, `try_builder!`
    /// macro, `feed_bytes_dispatch` Errored arm, push-path startup
    /// FailReply fold (PushFailure construction).
    ///
    /// **Latest-wins**: subsequent park overwrites the prior cause.
    /// Caller contract: query `pg.fail_cause()` IMMEDIATELY on the
    /// first FailReply event.
    #[inline]
    pub(crate) fn park_cause_at_install_errored(
        slot: &mut crate::fail_cause_slot::FailCauseSlotCell,
        cause: alloc::boxed::Box<crate::error::ProtocolError>,
    ) {
        slot.park_at_install_errored(cause, InstallErroredToken(()));
    }
}

use crate::wire::{
    SCRAM_SHA_256_MECHANISM, TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_BIND_COMPLETE,
    TAG_CLOSE_COMPLETE, TAG_COMMAND_COMPLETE, TAG_COPY_DATA, TAG_COPY_DONE, TAG_COPY_IN_RESPONSE,
    TAG_COPY_OUT_RESPONSE, TAG_EMPTY_QUERY_RESPONSE, TAG_ERROR_RESPONSE,
    TAG_NEGOTIATE_PROTOCOL_VERSION, TAG_NO_DATA, TAG_PARAMETER_DESCRIPTION, TAG_PARSE_COMPLETE,
    TAG_PORTAL_SUSPENDED, TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};

/// What to do after dispatching a single frame.
///
/// Three variants to keep the "emit zero actions" and "emit one
/// action" cases structurally distinct.
///
/// # By-ref state, no `new_state` payload
///
/// Dispatch takes `state: &mut ProtoState` and writes the transition
/// directly. `DispatchOutcome` carries the **side-effect** signal
/// only:
/// - `AdvancedSilent` — no payload (~1 B discriminant).
/// - `AdvancedWithAction { action }` — 88 B `StagedAction`.
/// - `Errored { reply_id, cause }` — 80 B classified failure.
///
/// Size: 88 B exact (pin in lib.rs).
///
/// A naive `new_state: ProtoState` payload (712 B each) inside the
/// Advanced variants would produce a by-value `DispatchOutcome` of
/// ~800 B that the caller would move into its `match` arm then
/// write back into `*state`. LLVM does NOT optimise that round-trip
/// because `ProtoState` contains an opaque password buffer
/// (non-trivial move semantics).
///
/// # Real-world win sizing
///
/// `dispatch()` runs **per frame**, not per row — and `DataRow`
/// bypasses `dispatch()` entirely via the
/// `row_stream::fast_path_data_row` fast-path. Typical query
/// routes through dispatch ~3-6 times (Parse/Bind complete,
/// RowDescription, CommandComplete, RFQ). So the saving is ~712 B
/// × 3-6 per query, not 712 MB per million rows.
///
/// The material benefit is **async future frame reduction**: every
/// `feed_bytes` suspension point in a downstream async wrapper
/// carries the 800 → 88 B delta, which snowballs through the
/// `async fn` state-machine nesting. That is the real per-QPS
/// win — smaller suspended futures, better L1 residency on
/// poll reawakening.
//
// `StagedAction<'static>` — the dispatch path (server→client
// frames) produces no `SendBytesBorrowed` actions; only push paths
// (Parse / SimpleQuery) borrow SQL bytes from the caller.
// Hard-pinning to `'static` here keeps the dispatch fn signatures
// lifetime-free. If a future server-driven path needs to borrow
// (e.g., streaming COPY data references), promote this to `<'sql>`
// then.
#[derive(Debug)]
pub(crate) enum DispatchOutcome {
    /// Frame consumed; transition already written to caller's
    /// state slot. No action emitted. Used by ParameterStatus,
    /// BackendKeyData, AuthenticationOk, SASLFinal — frames that
    /// advance state without user-visible side effects.
    AdvancedSilent,
    /// Frame consumed; transition already written to caller's
    /// state slot. One staged action emitted.
    ///
    /// `StagedAction` is range-based — the entry-point materialises
    /// into a ref-bound `Action<'buf>` after the write phase
    /// releases. `StagedAction<'static>` here because the dispatch
    /// path never produces `SendBytesBorrowed`.
    AdvancedWithAction {
        /// The single side-effect to push.
        action: StagedAction<'static>,
    },
    /// Frame rejected; connection irrecoverable. Caller tears down.
    /// State has already been set to `ProtoState::Errored(kind)` by
    /// the dispatch helper [`install_errored`] (the caller observes
    /// the terminal state without needing a second write).
    ///
    /// # Pre-consumed reply_id
    ///
    /// `reply_id` is `Option<NonZeroU64>` (already-consumed raw
    /// value), not `Option<ReplyId<K>>`. Dispatchers are
    /// parameterised per-command-kind; the Errored path is kind-
    /// agnostic (the downstream action is a
    /// `FailReply { id: NonZeroU64, cause }` that carries no
    /// payload). Pre-consuming at each dispatcher's Errored
    /// construction site keeps the `DispatchOutcome` kind-free and
    /// avoids forcing it to be generic over K.
    Errored {
        reply_id: Option<core::num::NonZeroU64>,
        cause: ProtocolError,
    },
}

/// `#[cold] #[inline]` helper centralising every
/// `DispatchOutcome::Errored` construction **plus** the
/// `*state = ProtoState::Errored(...)` install.
///
/// A naive shape where the helper only returns `DispatchOutcome`
/// and the caller writes `*state = ProtoState::Errored(kind)` in
/// the outer match arm would leave the seam open: an arm could
/// forget to set Errored while returning `DispatchOutcome::Errored`.
/// Installing the terminal state inside the helper keeps the seam
/// tight.
///
/// The `#[cold]` marker tells LLVM to push the Errored-path basic
/// block out of the hot-path I-cache footprint; `#[inline]` keeps
/// the call-site free of an actual function call (the helper body
/// folds into the caller).
///
/// `reply_id` is `Option<NonZeroU64>` (already-consumed raw value).
#[cold]
#[inline]
fn install_errored(
    state: &mut ProtoState,
    reply_id: Option<core::num::NonZeroU64>,
    cause: ProtocolError,
) -> DispatchOutcome {
    // .b: `install_errored` does NOT park the cause into
    // the slot — that happens at materialise time when the
    // `StagedAction::FailReply { id, cause }` is transformed into the
    // public `Action::FailReply { id }`. Keeping cause inline through
    // the staged surface avoids threading `fail_cause_slot` through
    // every `compute_push_*` signature.
    //
    // `install_errored`'s sole responsibility: write the Errored
    // state transition. The Caller-Drains-Cause discipline ensures
    // dispatch arms always pair `install_errored` with downstream
    // `StagedAction::FailReply` emission carrying the same cause.
    *state = ProtoState::Errored(cause.state_kind());
    DispatchOutcome::Errored { reply_id, cause }
}

/// Zero-body-payload validator for PG §55.7 frames that carry no
/// data (`EmptyQueryResponse`, `ParseComplete`, `BindComplete`,
/// `NoData`, `CloseComplete`, etc.). Returns `Ok(())` iff
/// `payload.is_empty()`; otherwise classifies as
/// [`ProtocolError::UnexpectedFrameBody`] with the wire tag +
/// observed body length.
///
/// Unifies the `match payload { [] => Ok(()), other => Err(...) }`
/// pattern shared by 6 dispatch arms. An "`EmptyBody` ZST" witness
/// alternative was considered and rejected as cosmetic — no caller
/// takes a typed witness as a parameter; the helper-function shape
/// carries identical bundle-compliance with a smaller surface.
///
/// `#[inline(always)]` — verified ASM-neutral on `feed_bytes` hot
/// path (0 codegen delta); locks the inlining guarantee against
/// future opt-level shifts.
#[inline(always)]
fn validate_empty_body(
    payload: &[u8],
    tag: crate::wire::InboundTag,
) -> Result<(), ProtocolError> {
    if payload.is_empty() {
        Ok(())
    } else {
        core::hint::cold_path();
        Err(ProtocolError::UnexpectedFrameBody {
            tag,
            payload_len: payload.len(),
        })
    }
}

// No `install_internal_bug` helper currently exists. Re-introduce
// inline if a future arm needs the `install_errored`-shaped helper
// for an `InternalCrateBug` cause.

/// Dispatch a single frame.
///
/// `write_buf` is the caller-owned outbound staging buffer;
/// dispatchers that produce [`StagedAction::SendBytesRange`] write
/// into it and record the range. The caller (feed_bytes) is
/// responsible for clearing `write_buf` at the start of each
/// entry-point call and materialising the ranges into `&'buf [u8]`
/// slices after the write-phase mutable borrow completes.
///
/// # `terminal_row_desc: &mut Option<RowDesc>`
///
/// The terminal-RowDesc slot on `PgProtocol` is threaded mutably.
/// Z arms (terminal `ReadyForQuery` transitions to `Idle`) park
/// the in-flight schema into this slot — schema-bearing variants'
/// `row_desc: RowDesc` field is moved out of the consumed state
/// variant and written into the slot — so the materialise-time
/// borrow has a stable address that outlives the state transition
/// (state moves to `Idle`, but the slot persists until the next
/// entry-point's clear).
///
/// A naive 2-slot `SchemaArena` with generation tracking would
/// thread `arena: &mut ArenaWriter<'_>` through dispatch and
/// require a `StaleSchemaRef` classified-error path (handle +
/// generation drift). The slot shape eliminates that class
/// entirely and shrinks the per-row hot-path by removing the dual
/// arena lookup.
///
/// # State by `&mut`
///
/// Dispatch takes `state: &mut ProtoState`, snaps the previous
/// state via `core::mem::replace(state, ProtoState::Idle)` for
/// pattern matching, then each arm writes `*state = new_state`
/// directly — one store, no round trip. A naive
/// `dispatch(prev: ProtoState) -> DispatchOutcome::Advanced* {
/// new_state, ... }` shape would pay two 712 B memcpies per
/// dispatch iteration — on 1M-row SELECT workloads, ~1.4 GB stack
/// traffic purely for state round-tripping (and LLVM does NOT
/// optimise it because `ProtoState` carries a non-trivial-move
/// password buffer).
///
/// Invariant: EVERY match arm must either (a) assign `*state =
/// new_state` before returning `AdvancedSilent`/`AdvancedWithAction`,
/// or (b) delegate to [`install_errored`] which installs
/// `ProtoState::Errored(...)`. Forgetting to assign leaves state
/// at the placeholder `Idle` — a silent regression class the
/// compiler cannot catch. Mitigation: arm-body coverage tests
/// across all transitions (the existing test suite exercises every
/// `(state, tag)` pair reachable in the state machine).
#[allow(
    clippy::too_many_arguments,
    reason = "Slot threading across the per-frame dispatch boundary \
              requires one `&mut` per outer-level cell + ancillary \
              arena slots. Each parameter is a distinct mutable view \
              into PgProtocol storage; bundling into a single \
              `DispatchContext`-like struct would force the dispatch \
              body to destructure that struct on every call (the \
              outer `DispatchContext` already does this once — \
              re-bundling here would double the indirection). \
              added `param_oids_slot` next to \
              `row_desc_slot` to mirror the per-cell slot-pattern; \
              the count grew from 7 to 8."
)]
pub(crate) fn dispatch(
    state: &mut ProtoState,
    tag: crate::wire::InboundTag,
    payload: &[u8],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
    // : ParamOids slot. Only the `'t'`
    // (ParameterDescription) arm in the
    // `DescribeStatementAwaitingParamDesc` state writes here. All
    // other arms ignore it. The slot's box is read via `as_ref()`
    // by `materialise` at the trailing `'Z'` arm and projected into
    // the public `Reply::DescribeStatementComplete.param_oids:
    // &'r ParamOids`. Cycle close (Idle/Errored entry) drops the
    // box via `clear_at_residue`.
    param_oids_slot: &mut crate::param_oids_slot::ParamOidsSlotCell,
    // : CommandTag slot. Written by `'C'`
    // (CommandComplete) arms in SimpleQueryAwaitingRfq /
    // BindExecuteAwaitingRfq{Dml,Select} and by the
    // EmptyQueryResponse transition into SimpleQueryAwaitingRfq.
    // Materialise reads via `as_ref()` at the trailing `'Z'` arm
    // and projects into `Reply::QueryComplete.command_tag:
    // &'r CommandTag`.
    command_tag_slot: &mut crate::command_tag_slot::CommandTagSlotCell,
    // : TxStatus slot. Written by every `'Z'`
    // (ReadyForQuery) dispatch arm; the parked value is read by
    // callers via `PgProtocol::terminal_tx_status` post-`feed_bytes`.
    // Materialise does NOT read it — Reply payloads no longer carry
    // tx_status; the slot is the single source of truth.
    tx_status_slot: &mut crate::tx_status_slot::TxStatusSlotCell,
    // `&mut Option<Box<ErrorArena>>` slot for the dispatch path's
    // only cold-write target. Most dispatch arms don't write
    // error_arena; the few that do (ErrorResponse arms) lazy-init
    // via `error_arena_or_init(error_arena_slot)` inline, allocating
    // exactly once on the first server error per connection. Frames
    // that don't reach an ErrorResponse arm pay zero allocation
    // cost.
    error_arena_slot: &mut Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
    // copy_chunks_arena slot. Lazy-init via
    // `get_or_insert_with` on first CopyData arrival; most arms
    // ignore it.
    copy_chunks_arena_slot: &mut Option<alloc::boxed::Box<crate::copy_chunks_arena::CopyChunksArena>>,
    // : command_tags_arena slot. Lazy-init via
    // `get_or_insert_with` on first IntermediateCommandComplete
    // emission (multi-statement batch). Only the three
    // SimpleQueryAwaitingRfq arms below touch it.
    command_tags_arena_slot: &mut Option<alloc::boxed::Box<crate::command_tags_arena::CommandTagsArena>>,
    column_names_slot: &mut Option<alloc::boxed::Box<[alloc::string::String]>>,
) -> DispatchOutcome {
    // Snap owned prev for pattern matching; state slot holds the
    // explicit `ProtoState::Idle` placeholder during the match.
    // Every match arm below MUST `*state = <transition>` before
    // returning a non-Errored outcome; `install_errored` handles
    // Errored transitions.
    //
    // Use `mem::replace` (not `mem::take`) to make the placeholder
    // explicit — `mem::take` silently relies on the `Default` impl
    // returning `Idle`, and a future `Default` change could swap
    // placeholder semantics under us.
    let prev = core::mem::replace(state, ProtoState::Idle);
    let outcome = match (prev, tag) {
        // =============================================================
        // Ping flow
        // =============================================================
        (ProtoState::PingAwaitingRfq(id), TAG_READY_FOR_QUERY) => {
            // `id: ReplyId<PingKind>` — the typed `deliver` helper
            // binds the payload to `PongPayload` at compile time.
            // Attempting to deliver any other payload type here is
            // a type error.
            //
            // Tier-1 tx_status validation via the centralised
            // `parse_rfq_payload`: users never receive a `TxStatus`
            // outside `{Idle, InTransaction, Failed}`; any other
            // byte is a wire violation classified as
            // `MalformedReadyForQuery` with the correct
            // `payload_len`.
            match parse_rfq_payload(payload) {
                Ok(tx_status) => {
                    _rfq_dispatch_leaf::park_tx_status_at_dispatch(
                        tx_status_slot,
                        tx_status,
                    );
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            id,
                            crate::action::PongPayload,
                        ),
                    }
                }
                Err(payload_len) => install_errored(state, Some(id.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
            }
        }
        (ProtoState::PingAwaitingRfq(id), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(id.consume()), cause)
        }
        (ProtoState::PingAwaitingRfq(id), other) => install_errored(state, Some(id.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingStartupTrust — awaiting AuthenticationOk
        // (Trust connections cannot accept AUTH_SASL — that case is
        // a per-variant dispatcher arm, not a runtime
        // classification.)
        // =============================================================
        (ProtoState::ConnectingStartupTrust { reply }, TAG_AUTHENTICATION) => {
            dispatch_auth_in_startup_trust(state, reply, payload)
        }
        (ProtoState::ConnectingStartupTrust { reply }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingStartupTrust { reply }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnsupportedProtocolOption)
        }
        (ProtoState::ConnectingStartupTrust { reply }, other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingStartupScram — awaiting AuthenticationSASL
        // Mirror of the Trust arm. A Scram connection receiving
        // AUTH_OK in this state is classified as
        // `UnsupportedAuthMethod` — the server accepted without
        // challenge while the user supplied a password, a PG policy
        // mismatch worth surfacing.
        // =============================================================
        (ProtoState::ConnectingStartupScram { reply, scram }, TAG_AUTHENTICATION) => {
            // `scram: ScramSession` is destructured DIRECTLY from
            // the variant — variant-carries-field is tier-1 compile
            // (CREDO §1). No drift classifier needed: the variant
            // cannot exist without its SCRAM session.
            dispatch_auth_in_startup_scram(state, reply, scram, payload, reserved)
        }
        (ProtoState::ConnectingStartupScram { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingStartupScram { reply, .. }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnsupportedProtocolOption)
        }
        (ProtoState::ConnectingStartupScram { reply, .. }, other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingStartupCleartext — awaiting
        // AuthenticationCleartextPassword. Mirror of the Trust +
        // SCRAM startup arms. A Cleartext connection only accepts
        // AUTH_CLEARTEXT_PASSWORD (sub-code 3); AUTH_OK without
        // challenge would be a server-side policy mismatch (server
        // accepted nothing despite the user supplying a password —
        // surfaced as `UnsupportedAuthMethod`).
        // =============================================================
        (ProtoState::ConnectingStartupCleartext { reply, password }, TAG_AUTHENTICATION) => {
            // Variant-carries-field: the variant cannot exist without
            // a `Box<Sensitive<Password>>`. Destructure here moves the
            // Box into the per-variant dispatcher, which builds the
            // `PasswordMessage` frame and drops the Box at scope end
            // (Drop chain scrubs the password bytes).
            dispatch_auth_in_startup_cleartext(state, reply, password, payload, reserved)
        }
        (ProtoState::ConnectingStartupCleartext { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingStartupCleartext { reply, .. }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnsupportedProtocolOption)
        }
        (ProtoState::ConnectingStartupCleartext { reply, .. }, other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingCleartextAwaitingAuthOk — PasswordMessage sent;
        // awaiting AuthenticationOk.
        // =============================================================
        (ProtoState::ConnectingCleartextAwaitingAuthOk(reply), TAG_AUTHENTICATION) => {
            dispatch_auth_ok_after_cleartext(state, reply, payload)
        }
        (ProtoState::ConnectingCleartextAwaitingAuthOk(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingCleartextAwaitingAuthOk(reply), other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingStartupMd5 — awaiting AuthenticationMD5Password.
        // Mirror of cleartext + SCRAM startup arms. Only sub-code 5
        // (with a valid 4-byte salt) progresses; any other code is
        // `UnsupportedAuthMethod` (downgrade rejection — security
        // mirror of SCRAM dispatcher).
        // =============================================================
        (ProtoState::ConnectingStartupMd5 { reply, handshake }, TAG_AUTHENTICATION) => {
            // Variant-carries-field: variant cannot exist without
            // `Box<Md5HandshakeState>`. Move into per-variant
            // dispatcher; the Box drops at function-return through
            // the ZeroizeOnDrop chain (Box::drop → md5 state drop →
            // Sensitive::drop → Password::drop).
            dispatch_auth_in_startup_md5(state, reply, handshake, payload, reserved)
        }
        (ProtoState::ConnectingStartupMd5 { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingStartupMd5 { reply, .. }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnsupportedProtocolOption)
        }
        (ProtoState::ConnectingStartupMd5 { reply, .. }, other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingMd5AwaitingAuthOk — PasswordMessage sent;
        // awaiting AuthenticationOk.
        // =============================================================
        (ProtoState::ConnectingMd5AwaitingAuthOk(reply), TAG_AUTHENTICATION) => {
            dispatch_auth_ok_after_md5(state, reply, payload)
        }
        (ProtoState::ConnectingMd5AwaitingAuthOk(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingMd5AwaitingAuthOk(reply), other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // SCRAM: awaiting server-first-message
        // =============================================================
        (
            ProtoState::ConnectingScramAwaitingServerFirst { reply, scram },
            TAG_AUTHENTICATION,
        ) => {
            // Heavy SCRAM fields destructured inline from the
            // variant — tier-1 invariant (CREDO §1: variant-carries-
            // field). No drift path.
            //
            // The SCRAM handshake state is one `Box<ScramSession>`
            // carrying password + `client_first_bare` +
            // `client_nonce_b64` inline. The Box is moved here by
            // the destructure (no allocator op);
            // `dispatch_auth_sasl_continue` borrows `&scram` for
            // HMAC composition + reads `scram.client_first_bare` /
            // `scram.client_nonce_b64` through the same borrow. The
            // Box drops at the end of this arm scope (1 free),
            // firing `ScramSession::Drop` with `ZeroizeOnDrop` scrub
            // of the password.
            dispatch_auth_sasl_continue(
                state,
                reply,
                &scram,
                payload,
                reserved,
            )
        }
        (ProtoState::ConnectingScramAwaitingServerFirst { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingScramAwaitingServerFirst { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // SCRAM: awaiting server-final-message
        // =============================================================
        (
            ProtoState::ConnectingScramAwaitingServerFinal {
                reply,
                expected_server_sig,
            },
            TAG_AUTHENTICATION,
        ) => {
            // `expected_server_sig` destructured inline — tier-1
            // variant-carries-field.
            dispatch_auth_sasl_final(state, reply, *expected_server_sig, payload, error_arena_slot)
        }
        (ProtoState::ConnectingScramAwaitingServerFinal { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingScramAwaitingServerFinal { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // SCRAM: awaiting AuthenticationOk after server sig verified
        // =============================================================
        (ProtoState::ConnectingScramAwaitingAuthOk(reply), TAG_AUTHENTICATION) => {
            dispatch_auth_ok_after_scram(state, reply, payload)
        }
        (ProtoState::ConnectingScramAwaitingAuthOk(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingScramAwaitingAuthOk(reply), other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // Post-auth: waiting for BackendKeyData
        //
        // `ParameterStatus` (tag 'S') is filtered pre-dispatch in
        // `feed_bytes` via `allows_unsolicited_param_status`; the
        // dispatcher never sees it for these states.
        // =============================================================
        (ProtoState::ConnectingPostAuthAwaitingKey(reply), TAG_BACKEND_KEY_DATA) => {
            match parse_backend_key_data(payload) {
                Ok((pid, secret_key)) => {
                    // Wrap the secret_key in Sensitive so the
                    // variant's storage scrubs on drop (state
                    // transition).
                    *state = ProtoState::ConnectingPostAuthHaveKey {
                        reply,
                        pid,
                        secret_key: crate::sensitive::Sensitive::new(secret_key),
                    };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::ConnectingPostAuthAwaitingKey(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingPostAuthAwaitingKey(reply), other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // Post-auth: have BackendKeyData, waiting for ReadyForQuery
        //
        // `ParameterStatus` (tag 'S') is filtered pre-dispatch in
        // `feed_bytes` via `allows_unsolicited_param_status`; the
        // dispatcher never sees it for these states.
        // =============================================================
        (
            ProtoState::ConnectingPostAuthHaveKey {
                reply,
                pid,
                secret_key,
            },
            TAG_READY_FOR_QUERY,
        ) => {
            // `reply: ReplyId<StartupKind>` — typed `deliver`
            // forces a `StartupCompletePayload` payload.
            //
            // Extract the inner `i32` from `Sensitive<i32>` via
            // `.with_inner(...)` (closure-scope access). The
            // Sensitive wrapper drops at end of arm scope, scrubbing
            // the source slot. The plain `i32` in
            // `StartupCompletePayload` then flows through the staged
            // pipeline; that payload's manual `Debug` impl redacts
            // the field.
            //
            // Persist `(pid, secret_key)` directly into the
            // payload of the post-RFQ transition variant
            // `ProtoState::HandshakeReady { pid, secret_key }`.
            // Re-wraps the secret in a fresh `Sensitive<i32>`
            // (the source variant's `Sensitive<i32>` drops + scrubs
            // at arm scope exit; the new wrapper inside
            // `HandshakeReady` rides the state variant's drop glue
            // forward to `BackendKey.secret_key` on
            // `<ConnectingPhase>::into_active`). Two scrub sites,
            // two `ZeroizeOnDrop` chains — defense-in-depth.
            //
            // The install runs inside the success arm only — on
            // parse-error the variant stays at PostAuthHaveKey-or-
            // Errored and the connection tears down via
            // `install_errored`.
            match parse_rfq_payload(payload) {
                Ok(tx_status) => {
                    // Wrap the local `i32` extraction in
                    // `Zeroizing<i32>` so the stack slot scrubs
                    // deterministically when
                    // the arm scope ends. A naive plain-`i32` local
                    // would live unscrubbed on the dispatch arm's
                    // function frame: the source `Sensitive<i32>`
                    // (variant payload) scrubs at arm-scope exit and
                    // the cell-installed `Sensitive<i32>` scrubs at
                    // connection teardown, but the **stack-resident
                    // intermediate** between the two scrub points
                    // would be uncovered. Under `panic = "unwind"`
                    // (cargo test) Drop runs even on early-return;
                    // under `panic = "abort"` (release; documented
                    // gap in Cargo.toml) the process exits before the
                    // unscrubbed slot matters. Tier-1 within scope:
                    // the secret can no longer leak via a future
                    // cold-path borrow / leak / coredump-of-stack-
                    // frame.
                    //
                    // `Zeroizing<i32>` defers to `<i32 as Zeroize>`
                    // (the zeroize crate's blanket impl for primitive
                    // ints writes `0`). `Deref` exposes the inner
                    // `i32` for the two consumers below without
                    // cloning. The `Sensitive::with_inner(...)`
                    // closure dereferences the inner `i32` (which is
                    // `Copy`) and returns a copy; the `&i32` borrow
                    // is HRTB-scoped and cannot escape.
                    let secret_key_inner: zeroize::Zeroizing<i32> =
                        zeroize::Zeroizing::new(secret_key.with_inner(|s| *s));
                    // Write the post-handshake transition signal as
                    // a payload-carrying `ProtoState::HandshakeReady`
                    // variant. The per-phase Connecting wrapper's
                    // lower-step projects this into
                    // `ConnectingState::HandshakeReady { pid,
                    // secret_key }`, which `into_active` then
                    // consumes structurally to construct the inline
                    // `BackendKey` on `ActiveInner` (tier-1 closure
                    // by storage absence on `with_cancel_request`).
                    //
                    // Sensitive<i32> ownership chain: the
                    // `secret_key_inner` Zeroizing<i32> guard scrubs
                    // the stack slot at end of arm scope; the new
                    // `Sensitive::new(...)` wraps a fresh i32 copy
                    // and rides the state variant's drop glue
                    // forward to `BackendKey.secret_key` at
                    // `into_active` time.
                    _rfq_dispatch_leaf::park_tx_status_at_dispatch(
                        tx_status_slot,
                        tx_status,
                    );
                    *state = ProtoState::HandshakeReady {
                        pid,
                        secret_key: crate::sensitive::Sensitive::new(*secret_key_inner),
                    };
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            reply,
                            crate::action::StartupCompletePayload {
                                pid,
                                secret_key: *secret_key_inner,
                                // exception: tx_status
                                // kept inline ONLY on StartupComplete
                                // because Connecting phase has no
                                // persistent slot. Other Reply variants
                                // strip the field; callers query via
                                // PgProtocol::terminal_tx_status post-
                                // into_active().
                                tx_status,
                            },
                        ),
                    }
                }
                Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
            }
        }
        (ProtoState::ConnectingPostAuthHaveKey { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingPostAuthHaveKey { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Simple Query flow
        //
        // State transitions per PG §55.2.3:
        //   Q sent → AwaitingFirstResponse
        //     T (RowDescription)   → StreamingRows
        //     C (CommandComplete)  → AwaitingRfq(command_tag)
        //     I (EmptyQueryResp)   → AwaitingRfq(empty tag)
        //     E (ErrorResponse)    → DrainRfqAfterError (FailReply emitted)
        //   StreamingRows
        //     D (DataRow)          → emit StreamRow; stay
        //     C                    → AwaitingRfq(command_tag)
        //     E                    → DrainRfqAfterError (FailReply emitted)
        //   AwaitingRfq
        //     Z                    → DeliverReply QueryComplete; Idle
        //   DrainRfqAfterError
        //     Z                    → silent; Idle (reply already sent)
        //
        // Errors within simple-query states: query-level `E` is
        // recoverable (connection survives); framing-level `E`
        // during the handshake states would tear down, but those
        // states are never entered from simple-query flow.
        // =============================================================

        // AwaitingFirstResponse: T / C / I / E — any other tag is desync
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_ROW_DESCRIPTION) => {
            match crate::decode::parse_row_description(payload) {
                Ok(row_desc) => {
                    _row_description_dispatch_leaf::park_row_description_at_dispatch(
                        row_desc_slot,
                        row_desc,
                    );
                    *column_names_slot = Some(crate::decode::parse_column_names(payload).into_boxed_slice());
                    *state = ProtoState::SimpleQueryStreamingRows { reply };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_COMMAND_COMPLETE) => {
            // DML path: no RowDescription frame fired, so
            // row_desc_slot remained `None` since the last Idle
            // entry — materialise emits Reply with `row_desc = None`.
            advance_to_awaiting_rfq(state, reply, payload, command_tag_slot)
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_EMPTY_QUERY_RESPONSE) => {
            // PG §55.7 EmptyQueryResponse has a zero-body payload.
            // Routed through `validate_empty_body` for uniform
            // classification across all zero-body frames.
            match validate_empty_body(payload, TAG_EMPTY_QUERY_RESPONSE) {
                Ok(()) => {
                    // : SimpleQueryAwaitingRfq no longer
                    // carries inline command_tag. Empty-query path:
                    // park CommandTag::EMPTY into the slot so the
                    // terminal `'Z'` materialise emits an empty tag
                    // in Reply::QueryComplete.
                    _command_complete_dispatch_leaf::park_command_tag_at_dispatch(
                        command_tag_slot,
                        alloc::boxed::Box::new(
                            crate::command_tag::CommandTag::EMPTY,
                        ),
                    );
                    *state = ProtoState::SimpleQueryAwaitingRfq { reply };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        // COPY entry points: 'H' = CopyOutResponse,
        // 'G' = CopyInResponse. Parse + validate the header (format
        // byte 0/1, n_cols ≤ MAX_ROW_COLUMNS, per-col formats agree),
        // transition into the appropriate COPY state.
        // Note: header data (format, n_cols) is parsed-and-
        // discarded for now — will route it into Action::Copy*
        // surface when the data-emission path lands.
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_COPY_OUT_RESPONSE) => {
            match crate::decode::parse_copy_response_header(payload) {
                Ok(_header) => {
                    *state = ProtoState::SimpleQueryCopyOutStreaming(reply);
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_COPY_IN_RESPONSE) => {
            match crate::decode::parse_copy_response_header(payload) {
                Ok(_header) => {
                    *state = ProtoState::SimpleQueryCopyInActive(reply);
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // COPY OUT state-machine arms.
        //
        // CopyOutStreaming: server streams `CopyData` ('d') frames,
        // terminates with `CopyDone` ('c'). ErrorResponse drains
        // through standard recoverable path. stays silent
        // on CopyData (no Action emission yet); will surface
        // the bytes via `Action::CopyDataChunk` or pull API.
        (ProtoState::SimpleQueryCopyOutStreaming(reply), TAG_COPY_DATA) => {
            // lazy-init the chunks arena, allocate
            // a slot for these bytes, emit Action::CopyDataChunk
            // carrying the gen-tagged ref. Caller resolves via
            // PgProtocol::get_copy_chunk within the OutActions cycle.
            // Arena cap exhaustion (rare; bounded by OutActions cap)
            // → silent drop (cold path; mirror of NotificationsArena
            // overflow behaviour).
            let arena = copy_chunks_arena_slot.get_or_insert_with(|| {
                alloc::boxed::Box::new(crate::copy_chunks_arena::CopyChunksArena::new())
            });
            let payload_bytes = crate::copy_chunks_arena::CopyChunkPayload {
                bytes: alloc::vec::Vec::from(payload),
            };
            match arena.alloc(payload_bytes) {
                Some(chunk_ref) => {
                    *state = ProtoState::SimpleQueryCopyOutStreaming(reply);
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::StagedAction::CopyDataChunk { chunk_ref },
                    }
                }
                None => {
                    // Arena exhausted (rare; per-cycle cap).
                    core::hint::cold_path();
                    *state = ProtoState::SimpleQueryCopyOutStreaming(reply);
                    DispatchOutcome::AdvancedSilent
                }
            }
        }
        (ProtoState::SimpleQueryCopyOutStreaming(reply), TAG_COPY_DONE) => {
            match validate_empty_body(payload, TAG_COPY_DONE) {
                Ok(()) => {
                    *state = ProtoState::SimpleQueryCopyOutAwaitingCC(reply);
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::SimpleQueryCopyOutStreaming(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::SimpleQueryCopyOutStreaming(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // CopyOutAwaitingCC: server sends `CommandComplete` carrying
        // the row count (e.g. `"COPY 1000"`), transitions into the
        // standard `SimpleQueryAwaitingRfq` tail state.
        (ProtoState::SimpleQueryCopyOutAwaitingCC(reply), TAG_COMMAND_COMPLETE) => {
            advance_to_awaiting_rfq(state, reply, payload, command_tag_slot)
        }
        (ProtoState::SimpleQueryCopyOutAwaitingCC(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::SimpleQueryCopyOutAwaitingCC(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // COPY IN state-machine arms.
        //
        // CopyInActive: server has acknowledged the COPY IN request
        // (via `CopyInResponse`); client now pushes `CopyData` /
        // `CopyDone` / `CopyFail` frames via the () push API.
        // Server transitions to `CommandComplete` once it observes
        // the client's `CopyDone`. State stays in `CopyInActive`
        // through the entire client push phase.
        (ProtoState::SimpleQueryCopyInActive(reply), TAG_COMMAND_COMPLETE) => {
            advance_to_awaiting_rfq(state, reply, payload, command_tag_slot)
        }
        (ProtoState::SimpleQueryCopyInActive(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::SimpleQueryCopyInActive(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // StreamingRows: C / E — any other tag (including DataRow
        // via `feed_bytes`) is a desync. DataRow is handled
        // exclusively by `iter_rows` fast-path; reaching dispatch
        // with TAG_DATA_ROW in a row-streaming state means the
        // caller used the `feed_bytes` API for a row-bearing
        // response (API misuse). The catch-all arm below
        // classifies as `UnexpectedFrame { tag: DataRow }` →
        // FailReply + CloseSocket.
        (ProtoState::SimpleQueryStreamingRows { reply }, TAG_COMMAND_COMPLETE) => {
            // SELECT path terminates: schema lives in row_desc_slot
            // (parked by the 'T' arm earlier in this query).
            // AwaitingRfq → Z → Idle. Materialise reads the slot
            // directly — no flag to set.
            advance_to_awaiting_rfq(state, reply, payload, command_tag_slot)
        }
        (ProtoState::SimpleQueryStreamingRows { reply, .. }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::SimpleQueryStreamingRows { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // AwaitingRfq: Z is the only legal frame
        // : command_tag lives in slot. State variant
        // carries only `reply`. Materialise reads slot at Z arm.
        (ProtoState::SimpleQueryAwaitingRfq { reply }, TAG_READY_FOR_QUERY) => {
            match parse_rfq_payload(payload) {
                Ok(tx_status) => {
                    _rfq_dispatch_leaf::park_tx_status_at_dispatch(
                        tx_status_slot,
                        tx_status,
                    );
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            reply,
                            crate::action::StagedQueryCompletePayload::Completed,
                        ),
                    }
                }
                Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
            }
        }
        // multi-statement batch: PG SimpleQuery accepts
        // `;`-separated batches like `"BEGIN; UPDATE; UPDATE; COMMIT;"`.
        // Server emits one CommandComplete per statement followed
        // by a single final RFQ. Each non-final CommandComplete /
        // RowDescription / EmptyQueryResponse emits
        // `Action::IntermediateCommandComplete` carrying the PRIOR
        // tag, and the state cycles back into the next statement's
        // response pattern. The final RFQ arm above produces the
        // standard `Reply::QueryComplete` carrying the LAST tag from
        // the slot.
        //
        // : slot holds the LATEST parked tag. Intermediate
        // emit captures the PRIOR tag (from slot) BEFORE overwriting
        // — by-value copy via slot.as_ref() + Copy. Then parse new
        // tag → park (overwrites slot). State stays in AwaitingRfq.
        (ProtoState::SimpleQueryAwaitingRfq { reply }, TAG_COMMAND_COMPLETE) => {
            // Snapshot prior tag from slot (by-value, Copy).
            let prior_tag = command_tag_slot
                .as_ref()
                .copied()
                .unwrap_or(crate::command_tag::CommandTag::EMPTY);
            // Parse new tag — classified on malformed (missing NUL /
            // embedded NUL) per pre-contract.
            match crate::command_tag::parse_command_tag_bytes(payload) {
                Ok(new_tag) => {
                    // : alloc prior tag into arena;
                    // overflow (arch-dead, OutActions cap is 9 = same
                    // as arena cap) installs InternalCrateBug rather
                    // than silently dropping the action.
                    let prior_tag_ref = crate::protocol::command_tags_arena_or_init(
                        command_tags_arena_slot,
                    )
                    .alloc(prior_tag);
                    let Some(prior_tag_ref) = prior_tag_ref else {
                        return install_errored(
                            state,
                            Some(reply.consume()),
                            ProtocolError::InternalCrateBug {
                                locus: crate::error::CrateBugLocus::CommandTagsArenaOverflow,
                            },
                        );
                    };
                    _command_complete_dispatch_leaf::park_command_tag_at_dispatch(
                        command_tag_slot,
                        alloc::boxed::Box::new(new_tag),
                    );
                    *state = ProtoState::SimpleQueryAwaitingRfq { reply };
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::StagedAction::IntermediateCommandComplete {
                            tag_ref: prior_tag_ref,
                        },
                    }
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::SimpleQueryAwaitingRfq { reply }, TAG_ROW_DESCRIPTION) => {
            // Next statement is a SELECT. Snapshot prior tag, parse
            // RowDesc + park into row_desc_slot, transition to
            // StreamingRows. Slot keeps prior tag for now; SELECT's
            // own C will overwrite later.
            let prior_tag = command_tag_slot
                .as_ref()
                .copied()
                .unwrap_or(crate::command_tag::CommandTag::EMPTY);
            match crate::decode::parse_row_description(payload) {
                Ok(row_desc) => {
                    // : alloc prior tag into arena.
                    let prior_tag_ref = crate::protocol::command_tags_arena_or_init(
                        command_tags_arena_slot,
                    )
                    .alloc(prior_tag);
                    let Some(prior_tag_ref) = prior_tag_ref else {
                        return install_errored(
                            state,
                            Some(reply.consume()),
                            ProtocolError::InternalCrateBug {
                                locus: crate::error::CrateBugLocus::CommandTagsArenaOverflow,
                            },
                        );
                    };
                    _row_description_dispatch_leaf::park_row_description_at_dispatch(
                        row_desc_slot,
                        row_desc,
                    );
                    *column_names_slot = Some(crate::decode::parse_column_names(payload).into_boxed_slice());
                    *state = ProtoState::SimpleQueryStreamingRows { reply };
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::StagedAction::IntermediateCommandComplete {
                            tag_ref: prior_tag_ref,
                        },
                    }
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::SimpleQueryAwaitingRfq { reply }, TAG_EMPTY_QUERY_RESPONSE) => {
            // Next statement is empty (e.g., `;;`). Snapshot prior
            // tag, park CommandTag::EMPTY (overwrites slot), emit
            // IntermediateCommandComplete for prior tag.
            let prior_tag = command_tag_slot
                .as_ref()
                .copied()
                .unwrap_or(crate::command_tag::CommandTag::EMPTY);
            match validate_empty_body(payload, TAG_EMPTY_QUERY_RESPONSE) {
                Ok(()) => {
                    // : alloc prior tag into arena.
                    let prior_tag_ref = crate::protocol::command_tags_arena_or_init(
                        command_tags_arena_slot,
                    )
                    .alloc(prior_tag);
                    let Some(prior_tag_ref) = prior_tag_ref else {
                        return install_errored(
                            state,
                            Some(reply.consume()),
                            ProtocolError::InternalCrateBug {
                                locus: crate::error::CrateBugLocus::CommandTagsArenaOverflow,
                            },
                        );
                    };
                    _command_complete_dispatch_leaf::park_command_tag_at_dispatch(
                        command_tag_slot,
                        alloc::boxed::Box::new(crate::command_tag::CommandTag::EMPTY),
                    );
                    *state = ProtoState::SimpleQueryAwaitingRfq { reply };
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::StagedAction::IntermediateCommandComplete {
                            tag_ref: prior_tag_ref,
                        },
                    }
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::SimpleQueryAwaitingRfq { reply, .. }, TAG_ERROR_RESPONSE) => {
            // Mid-batch error — drain to RFQ via standard recoverable
            // path. Intermediate tags up to this point are observable
            // via the stream; the final reply is FailReply, NOT
            // QueryComplete with the last successful tag.
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::SimpleQueryAwaitingRfq { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // DrainRfqAfterError: consume Z → Idle, with full tx_status
        // validation. **Tier-3 classified**: malformed RFQ payload on
        // this arm routes to `MalformedReadyForQuery` via
        // `parse_rfq_payload`'s Err arm — same uniform classifier as
        // every other RFQ arm (`PingAwaitingRfq`, `ParseAwaitingRfq`,
        // `SimpleQueryAwaitingRfq`, etc.).
        (ProtoState::DrainRfqAfterError, TAG_READY_FOR_QUERY) => {
            // tx_status is unused on the drain path — we're returning
            // to Idle after a query-level error; the pre-error query's
            // consumer already received FailReply via `advance_to_drain_after_error`.
            // Nobody consumes the drain's tx_status. Pattern-bind `_`
            // in Ok arm to validate-and-discard (not a `let _` — that
            // form is user-banned per no-underscore-vars feedback).
            match parse_rfq_payload(payload) {
                Ok(_) => {
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedSilent
                }
                Err(payload_len) => install_errored(
                    state,
                    None,
                    ProtocolError::MalformedReadyForQuery { payload_len },
                ),
            }
        }
        (ProtoState::DrainRfqAfterError, other) => {
            install_errored(state, None, ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Extended Query — Parse flow
        //
        // Parse + Sync sequence:
        //   Client sends: P frame + S frame (bundled in push_command)
        //   Server responds: '1' (ParseComplete) + 'Z' (ReadyForQuery)
        //                or: 'E' (ErrorResponse) + 'Z' (recoverable)
        //
        // State lifecycle:
        //   Idle → ParseAwaitingParseComplete(reply) → ParseAwaitingRfq(reply) → Idle
        //                                            ↘ (on E)
        //                                              DrainRfqAfterError → Idle
        // =============================================================

        (ProtoState::ParseAwaitingParseComplete(reply), TAG_PARSE_COMPLETE) => {
            // PG §55.7 ParseComplete body must be empty.
            match validate_empty_body(payload, TAG_PARSE_COMPLETE) {
                Ok(()) => {
                    *state = ProtoState::ParseAwaitingRfq(reply);
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::ParseAwaitingParseComplete(reply), TAG_ERROR_RESPONSE) => {
            // Recoverable parse error — PG spec sends Z after E, so
            // drain it silently and return to Idle (reusing the
            // `DrainRfqAfterError` variant — both drain
            // the same trailing RFQ pattern).
            let cause = parse_error_response(payload, crate::protocol::error_arena_or_init(error_arena_slot)).into_protocol_error();
            *state = ProtoState::DrainRfqAfterError;
            DispatchOutcome::AdvancedWithAction {
                action: StagedAction::FailReply {
                    id: reply.consume(),
                    cause,
                },
            }
        }
        (ProtoState::ParseAwaitingParseComplete(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        (ProtoState::ParseAwaitingRfq(reply), TAG_READY_FOR_QUERY) => {
            match parse_rfq_payload(payload) {
                Ok(tx_status) => {
                    _rfq_dispatch_leaf::park_tx_status_at_dispatch(
                        tx_status_slot,
                        tx_status,
                    );
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            reply,
                            crate::action::ParseCompletePayload,
                        ),
                    }
                }
                Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
            }
        }
        (ProtoState::ParseAwaitingRfq(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Extended Query — Bind + Execute flow
        //
        // Flow splits at `push_bind_execute` call time by whether the
        // caller provided a `row_desc`:
        //
        // • SCHEMA-LESS (DML) path — 3 states, 'D' is UnexpectedFrame
        //   at every stage since there's no schema to decode rows with:
        //     AwaitingBindCompleteDml → '2' → AwaitingCommandCompleteDml
        //     AwaitingCommandCompleteDml → 'C' → AwaitingRfqDml
        //     AwaitingRfqDml → 'Z' → Idle + DeliverReply(row_desc=None)
        //
        // • SCHEMA-BEARING (SELECT) path — 4 states, 'D' streams rows:
        //     AwaitingBindCompleteSelect → '2' → AwaitingDataOrCompleteSelect
        //     AwaitingDataOrCompleteSelect → 'D' → StreamingRows (+emit)
        //                                  → 'C' → AwaitingRfqSelect
        //     StreamingRows → 'D' → StreamingRows (+emit)
        //                   → 'C' → AwaitingRfqSelect
        //     AwaitingRfqSelect → 'Z' → Idle + DeliverReply(row_desc=Some)
        //
        // Shared-across-paths:
        //   'E' (ErrorResponse) at ANY state → drain-after-error
        //                                      (query-level recoverable)
        //   's' (PortalSuspended) → UnexpectedFrame (chunked fetch
        //                           is a planned follow-up)
        //
        // Tier uplift via the split: the "can we stream rows?"
        // decision is resolved at the VARIANT level (tier-1 structural
        // dispatch). A naive single-set-of-states shape would need a
        // runtime `match row_desc: Option<_>` at the 'D' arm.
        // =============================================================

        // ─── DML path ───

        (ProtoState::BindExecuteAwaitingBindCompleteDml(reply), TAG_BIND_COMPLETE) => {
            // PG §55.7 BindComplete body must be empty.
            match validate_empty_body(payload, TAG_BIND_COMPLETE) {
                Ok(()) => {
                    *state = ProtoState::BindExecuteAwaitingCommandCompleteDml(reply);
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        // The prepared! macro path bundles Parse + Bind + Execute +
        // Sync; server replies with `1` (ParseComplete) before `2`
        // (BindComplete). Accept `1` as a silent transition in this
        // state (state stays the same; ParseComplete carries no
        // payload data we'd track separately). The post-condition
        // still requires `2` to advance.
        //
        // **State restore**: the dispatch entry `mem::replace`'d state with
        // Idle; the AdvancedSilent path MUST write back the original
        // variant or the protocol would silently transition to Idle
        // (and the next BindComplete frame would be UnexpectedFrame).
        (ProtoState::BindExecuteAwaitingBindCompleteDml(reply), TAG_PARSE_COMPLETE) => {
            if payload.is_empty() {
                *state = ProtoState::BindExecuteAwaitingBindCompleteDml(reply);
                DispatchOutcome::AdvancedSilent
            } else {
                let payload_len = payload.len();
                install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_PARSE_COMPLETE,
                        payload_len,
                    },
                )
            }
        }
        (ProtoState::BindExecuteAwaitingBindCompleteDml(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::BindExecuteAwaitingBindCompleteDml(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        (ProtoState::BindExecuteAwaitingCommandCompleteDml(reply), TAG_COMMAND_COMPLETE) => {
            // : parse + park; classified on malformed.
            match crate::command_tag::parse_command_tag_bytes(payload) {
                Ok(parsed) => {
                    _command_complete_dispatch_leaf::park_command_tag_at_dispatch(
                        command_tag_slot,
                        alloc::boxed::Box::new(parsed),
                    );
                    *state = ProtoState::BindExecuteAwaitingRfqDml { reply };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::BindExecuteAwaitingCommandCompleteDml(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::BindExecuteAwaitingCommandCompleteDml(reply), TAG_PORTAL_SUSPENDED) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_PORTAL_SUSPENDED })
        }
        (ProtoState::BindExecuteAwaitingCommandCompleteDml(reply), other) => {
            // Includes 'D' (DataRow) — structurally no schema here,
            // server emitting rows is a wire violation.
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        (
            ProtoState::BindExecuteAwaitingRfqDml { reply },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                _rfq_dispatch_leaf::park_tx_status_at_dispatch(
                    tx_status_slot,
                    tx_status,
                );
                // : command_tag in slot. Materialise reads
                // the slot at the Z arm → Reply::QueryComplete with
                // `command_tag: &'r CommandTag`. :
                // tx_status now also in slot (parked above).
                *state = ProtoState::Idle;
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedQueryCompletePayload::Completed,
                    ),
                }
            }
            Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
        },
        (ProtoState::BindExecuteAwaitingRfqDml { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // ─── SELECT path ───

        (
            ProtoState::BindExecuteAwaitingBindCompleteSelect { reply },
            TAG_BIND_COMPLETE,
        ) => {
            // PG §55.7 BindComplete body must be empty.
            match validate_empty_body(payload, TAG_BIND_COMPLETE) {
                Ok(()) => {
                    *state =
                        ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        // The prepared! macro path mirrors the DML arm above: the
        // server may emit `1` (ParseComplete) before `2`
        // (BindComplete) when Parse + Bind + Execute + Sync bundle
        // in one push. State name stays; the state semantically
        // represents "awaiting BindComplete for the in-flight Bind,
        // optionally preceded by ParseComplete". Same state-restore
        // discipline as the DML arm (the `mem::replace` dance at
        // entry forces the write-back).
        (ProtoState::BindExecuteAwaitingBindCompleteSelect { reply }, TAG_PARSE_COMPLETE) => {
            if payload.is_empty() {
                *state = ProtoState::BindExecuteAwaitingBindCompleteSelect { reply };
                DispatchOutcome::AdvancedSilent
            } else {
                let payload_len = payload.len();
                install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_PARSE_COMPLETE,
                        payload_len,
                    },
                )
            }
        }
        (ProtoState::BindExecuteAwaitingBindCompleteSelect { reply, .. }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::BindExecuteAwaitingBindCompleteSelect { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // BindExecuteAwaitingDataOrCompleteSelect: DataRow via
        // `feed_bytes` classifies as UnexpectedFrame in the
        // catch-all arm below (see SimpleQueryStreamingRows for
        // the full rationale).
        (
            ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply },
            TAG_COMMAND_COMPLETE,
        ) => advance_to_bindexecute_awaiting_rfq_select(state, reply, payload, command_tag_slot),
        (ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, .. }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        // PortalSuspended before any DataRow is valid PG §55.2.7 —
        // server may emit PortalSuspended immediately if the cap is
        // smaller than the row count actually produced (e.g., 0-row
        // portal with `FetchRows::Chunked(N)`). Body must be empty;
        // transition mirrors the StreamingRows arm below.
        (
            ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply },
            TAG_PORTAL_SUSPENDED,
        ) => {
            match validate_empty_body(payload, TAG_PORTAL_SUSPENDED) {
                Ok(()) => {
                    *state = ProtoState::BindExecuteAwaitingRfqAfterSuspended { reply };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // BindExecuteStreamingRows: DataRow via `feed_bytes`
        // classifies as UnexpectedFrame in the catch-all arm
        // below (see SimpleQueryStreamingRows for the full
        // rationale).
        (ProtoState::BindExecuteStreamingRows { reply }, TAG_COMMAND_COMPLETE) => {
            advance_to_bindexecute_awaiting_rfq_select(state, reply, payload, command_tag_slot)
        }
        (ProtoState::BindExecuteStreamingRows { reply, .. }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        // PortalSuspended ('s') — alternative terminal frame to
        // CommandComplete when `FetchRows::Chunked(N)` hit the row
        // cap before the portal exhausted (PG §55.2.7). Body must be
        // empty. Transition to AwaitingRfqAfterSuspended; trailing
        // RFQ delivers `Reply::QuerySuspended`.
        (ProtoState::BindExecuteStreamingRows { reply }, TAG_PORTAL_SUSPENDED) => {
            match validate_empty_body(payload, TAG_PORTAL_SUSPENDED) {
                Ok(()) => {
                    *state = ProtoState::BindExecuteAwaitingRfqAfterSuspended { reply };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::BindExecuteStreamingRows { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // BindExecuteAwaitingRfqAfterSuspended terminal arms.
        (
            ProtoState::BindExecuteAwaitingRfqAfterSuspended { reply },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                _rfq_dispatch_leaf::park_tx_status_at_dispatch(
                    tx_status_slot,
                    tx_status,
                );
                *state = ProtoState::Idle;
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedQueryCompletePayload::Suspended,
                    ),
                }
            }
            Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
        },
        (ProtoState::BindExecuteAwaitingRfqAfterSuspended { reply }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::BindExecuteAwaitingRfqAfterSuspended { reply }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        (
            ProtoState::BindExecuteAwaitingRfqSelect { reply },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                _rfq_dispatch_leaf::park_tx_status_at_dispatch(
                    tx_status_slot,
                    tx_status,
                );
                // : command_tag in slot; row_desc in
                // row_desc_slot. : tx_status in slot.
                // Materialise reads command_tag + row_desc slots at Z;
                // callers query tx_status via terminal_tx_status.
                *state = ProtoState::Idle;
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedQueryCompletePayload::Completed,
                    ),
                }
            }
            Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
        },
        (ProtoState::BindExecuteAwaitingRfqSelect { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Extended Query — Describe flow
        //
        // Statement-target response sequence (PG §55.2.2):
        //   't' (ParameterDescription) → 'T' (RowDescription) OR
        //   'n' (NoData) → 'Z' (ReadyForQuery).
        // Portal-target response sequence:
        //   'T' or 'n' → 'Z'. NO 't' — portals are bound-state
        //   handles, parameters fixed at Bind time.
        //
        // Either target, 'E' at any point → recoverable (connection
        // survives, same pattern as SimpleQuery/Parse query-level
        // error per PG §55.2.3).
        //
        // State transitions encode the per-target topology:
        // statement-describe walks 3 states, portal-describe walks 2.
        // A `'T'` arrival in `DescribeStatementAwaitingParamDesc` is
        // UnexpectedFrame — the server has violated the documented
        // sequence. Tier-1 structural dispatch.
        // =============================================================

        // ─── Statement-describe path ───

        // Stage 1: awaiting ParameterDescription.
        //
        // (slot-pattern): parsed `ParamOids` is heap-
        // boxed once here and PARKED IN THE SLOT (not inline in the
        // post-state variant). The post-state variant carries only
        // the bare `ReplyId<DescribeStatementKind>`. Slot
        // `as_ref()` projects to `&'r ParamOids` at the trailing
        // `'Z'` materialise, which produces the public Reply with
        // `param_oids: &'r ParamOids` (rather than the prior
        // inlined owned `ParamOids`). Slot cleared at the next
        // Idle/Errored residue clear — net: 1 alloc per Describe,
        // 1 free per Describe.
        (ProtoState::DescribeStatementAwaitingParamDesc(reply), TAG_PARAMETER_DESCRIPTION) => {
            match crate::decode::parse_parameter_description(payload) {
                Ok(param_oids) => {
                    _param_description_dispatch_leaf::park_param_oids_at_dispatch(
                        param_oids_slot,
                        param_oids,
                    );
                    *state = ProtoState::DescribeStatementAwaitingRowDescOrNoData {
                        reply,
                    };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::DescribeStatementAwaitingParamDesc(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::DescribeStatementAwaitingParamDesc(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // Stage 2: awaiting RowDescription or NoData.
        //
        // : ParamOids no longer lives in the state variant
        // — slot holds the box from the preceding `'t'` arm.
        // Transition is a pure state-discriminant flip (no Box move).
        (
            ProtoState::DescribeStatementAwaitingRowDescOrNoData { reply },
            TAG_ROW_DESCRIPTION,
        ) => match crate::decode::parse_row_description(payload) {
            Ok(row_desc) => {
                _row_description_dispatch_leaf::park_row_description_at_dispatch(
                    row_desc_slot,
                    row_desc,
                );
                *column_names_slot = Some(crate::decode::parse_column_names(payload).into_boxed_slice());
                *state = ProtoState::DescribeStatementAwaitingRfq { reply };
                DispatchOutcome::AdvancedSilent
            }
            Err(cause) => install_errored(state, Some(reply.consume()), cause),
        },
        (
            ProtoState::DescribeStatementAwaitingRowDescOrNoData { reply },
            TAG_NO_DATA,
        ) => {
            // PG §55.7 NoData body must be empty. row_desc_slot stays
            // `None` (no 'T' fired); materialise reads the slot at Z
            // and emits Reply::DescribeStatementComplete with `NoData`.
            match validate_empty_body(payload, TAG_NO_DATA) {
                Ok(()) => {
                    *state = ProtoState::DescribeStatementAwaitingRfq { reply };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (
            ProtoState::DescribeStatementAwaitingRowDescOrNoData { reply, .. },
            TAG_ERROR_RESPONSE,
        ) => advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot)),
        (ProtoState::DescribeStatementAwaitingRowDescOrNoData { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // Stage 3: awaiting ReadyForQuery — deliver the terminal
        // reply. Both `param_oids` AND schema (if any) live in slots
        // (`<ActivePhase>::Extras.param_oids` and `.row_desc`
        // respectively, both populated at the earlier dispatch arms);
        // materialise reads both slots directly via `as_ref()` and
        // emits `Reply::DescribeStatementComplete` with `param_oids:
        // &'r ParamOids` borrowed from the slot.
        //
        // : state variant carries no payload beyond
        // `ReplyId<K>` — the staged variant emits NO `param_oids`
        // payload either (consumer reads via the borrow at the
        // Reply level). Slot drops the box at the next Idle/Errored
        // residue clear (one-shot per Describe flow, paired with
        // the `Box::new` at the `'t'` arrival).
        (
            ProtoState::DescribeStatementAwaitingRfq { reply },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                _rfq_dispatch_leaf::park_tx_status_at_dispatch(
                    tx_status_slot,
                    tx_status,
                );
                *state = ProtoState::Idle;
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedDescribeStatementCompletePayload,
                    ),
                }
            }
            Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
        },
        (ProtoState::DescribeStatementAwaitingRfq { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // ─── Portal-describe path ───

        // Stage 1: awaiting RowDescription or NoData (no ParamDesc).
        (ProtoState::DescribePortalAwaitingRowDescOrNoData(reply), TAG_ROW_DESCRIPTION) => {
            // Parsed schema lands in PgProtocol::row_desc_slot —
            // single source of truth.
            match crate::decode::parse_row_description(payload) {
                Ok(row_desc) => {
                    _row_description_dispatch_leaf::park_row_description_at_dispatch(
                        row_desc_slot,
                        row_desc,
                    );
                    *column_names_slot = Some(crate::decode::parse_column_names(payload).into_boxed_slice());
                    *state = ProtoState::DescribePortalAwaitingRfq { reply };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::DescribePortalAwaitingRowDescOrNoData(reply), TAG_NO_DATA) => {
            // PG §55.7 NoData body must be empty. row_desc_slot stays
            // None (no 'T' fired); materialise reads slot at Z and
            // emits NoData.
            match validate_empty_body(payload, TAG_NO_DATA) {
                Ok(()) => {
                    *state = ProtoState::DescribePortalAwaitingRfq { reply };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::DescribePortalAwaitingRowDescOrNoData(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::DescribePortalAwaitingRowDescOrNoData(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // Stage 2: awaiting ReadyForQuery — deliver portal reply.
        // Schema (if any) lives in row_desc_slot; materialise reads
        // it at the materialise pass.
        (
            ProtoState::DescribePortalAwaitingRfq { reply },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                _rfq_dispatch_leaf::park_tx_status_at_dispatch(
                    tx_status_slot,
                    tx_status,
                );
                *state = ProtoState::Idle;
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedDescribePortalCompletePayload,
                    ),
                }
            }
            Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
        },
        (ProtoState::DescribePortalAwaitingRfq { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Close flow (PG §55.7)
        //
        // Both `CloseStatement` and `ClosePortal` produce the SAME
        // response sequence on the wire:
        //   '3' (CloseComplete) → 'Z' (ReadyForQuery)
        //
        // The push-side target byte ('S'/'P') is consumed inside the
        // Close frame on its way out; the dispatch-side treats both
        // paths uniformly because both yield identical reply payload
        // shapes (`CloseCompletePayload`, ZST).
        //
        // PG accepts Close on a non-existent name (NOT an error — see
        // PG §55.7), so the happy path is the only common case.
        // ErrorResponse during Close is non-standard but spec-conforming:
        // emit FailReply + transition to DrainRfqAfterError. Connection
        // survives (query-level recoverable, mirroring Parse / Describe).
        // =============================================================

        // Stage 1: awaiting CloseComplete ('3').
        (ProtoState::CloseAwaitingComplete(reply), TAG_CLOSE_COMPLETE) => {
            // PG §55.7 CloseComplete body must be empty. Reject any
            // payload that doesn't match the empty-body invariant.
            match validate_empty_body(payload, TAG_CLOSE_COMPLETE) {
                Ok(()) => {
                    *state = ProtoState::CloseAwaitingRfq(reply);
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::CloseAwaitingComplete(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, crate::protocol::error_arena_or_init(error_arena_slot))
        }
        (ProtoState::CloseAwaitingComplete(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // Stage 2: awaiting ReadyForQuery — deliver the terminal
        // CloseComplete reply. No payload data — `CloseCompletePayload`
        // is ZST; only the correlator is meaningful.
        (ProtoState::CloseAwaitingRfq(reply), TAG_READY_FOR_QUERY) => {
            match parse_rfq_payload(payload) {
                Ok(_tx_status) => {
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            reply,
                            crate::action::CloseCompletePayload,
                        ),
                    }
                }
                Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
            }
        }
        (ProtoState::CloseAwaitingRfq(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Idle — unsolicited frames are out-of-spec
        // =============================================================
        (ProtoState::Idle, other) => install_errored(state, None, ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // HandshakeReady — transition-signal write target
        //
        // Architecturally dead at dispatch entry. The
        // `(PostAuthHaveKey, RFQ)` arm above WRITES this variant
        // as the post-handshake transition signal; the per-phase
        // Connecting wrapper's lower-step then projects it to
        // `ConnectingState::HandshakeReady { pid, secret_key }`
        // and `into_active` consumes the payload structurally.
        // Dispatch never re-enters with this variant under intact
        // invariants — but the exhaustive match needs an arm to
        // build.
        // =============================================================
        (ProtoState::HandshakeReady { pid, secret_key }, _) => {
            // Preserve the variant verbatim — drop the payload only
            // at the legitimate consumer (`into_active`). A defensive
            // install_errored here would scrub the cancel-key
            // material before the wrapper had a chance to surface it.
            *state = ProtoState::HandshakeReady { pid, secret_key };
            DispatchOutcome::AdvancedSilent
        }

        // =============================================================
        // Errored — terminal sink
        //
        // Architecturally dead under current flow. `feed_bytes`
        // short-circuits on `ProtoState::Errored(_)` via the
        // is-errored-or-recovering fast-path check (see
        // `protocol.rs` — `is_errored_or_recovering` → clear
        // read_buf + return empty OutActions, dispatch is NEVER
        // entered). Arm retained for exhaustive `(ProtoState, tag)`
        // coverage — a future API that bypasses the early-return
        // would still land here classified, not UB. Missing arm
        // would be a compile error by match exhaustiveness.
        // =============================================================
        (ProtoState::Errored(original), _) => {
            *state = ProtoState::Errored(original);
            DispatchOutcome::AdvancedSilent
        }
    };

    // No separate `scram_state` cleanup needed: the
    // `mem::replace(state, ProtoState::Idle)` above consumed the
    // SCRAM variant by value, and if the arm ended in Errored the
    // variant is already dropped — `ScramSession::Drop`
    // (`ZeroizeOnDrop`) scrubbed password bytes inside the match.
    // Variant-carries-field (CREDO §1) makes this automatic: there
    // is no separate slot that could linger past the state
    // transition.
    outcome
}

// -----------------------------------------------------------------
// Helper: parse Authentication sub-code from payload
// -----------------------------------------------------------------

/// Extract + classify the 4-byte BE auth sub-code from an `'R'`
/// payload. Returns a typed [`crate::wire::AuthSubCode`] (only the 4
/// PG-defined values) plus the rest of the payload; classifies
/// unknown codes as `UnsupportedAuthMethod` at parse time.
///
/// Tier-1 uplift: downstream `dispatch_auth_*` handlers now match
/// on an enum with 4 variants — adding a new PG auth sub-code
/// forces every handler to decide how to treat it. Previously each
/// handler's `_ =>` arm swallowed both "unknown" and "known-but-wrong"
/// codes silently.
fn auth_sub_code(payload: &[u8]) -> Result<(crate::wire::AuthSubCode, &[u8]), ProtocolError> {
    match payload {
        [a, b, c, d, rest @ ..] => {
            let raw = u32::from_be_bytes([*a, *b, *c, *d]);
            // `try_from_u32` returns `Result<Self, u32>` (not
            // `Option<Self>`) — forward the rejected raw u32 via
            // `map_err`, no separate `.ok_or(..raw)` layer needed.
            let code = crate::wire::AuthSubCode::try_from_u32(raw).map_err(|unknown| {
                // `unknown ≠ 0` structurally — AUTH_OK = 0 is
                // matched to `Ok(AuthSubCode::Ok)` by try_from_u32
                // above, so `Err(0)` is architecturally impossible.
                // `AuthSubCodeClass::Unknown(NonZeroU32)` carries
                // the tier-1 type-level proof; the dead None arm is
                // classified as the `AuthSubCodeZeroInErr` crate-bug
                // locus.
                match core::num::NonZeroU32::new(unknown) {
                    Some(nz) => ProtocolError::UnsupportedAuthMethod {
                        sub_code: crate::error::AuthSubCodeClass::Unknown(nz),
                    },
                    None => ProtocolError::InternalCrateBug {
                        locus: crate::error::CrateBugLocus::AuthSubCodeZeroInErr,
                    },
                }
            })?;
            Ok((code, rest))
        }
        _ => Err(ProtocolError::MalformedAuthentication {
            payload_len: payload.len(),
        }),
    }
}

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingStartupTrust`].
///
/// Only `AUTH_OK` is acceptable here: the user provided no password,
/// so a SCRAM challenge from the server is classified as
/// `UnsupportedAuthMethod` (the server's pg_hba.conf disagrees with
/// the client's no-password configuration). The dispatcher match
/// cannot reach this arm with SCRAM payloads already buffered,
/// because the Scram variant of the state has its own handler —
/// per-variant dispatchers carry the tier-1 compile guarantee.
fn dispatch_auth_in_startup_trust(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    payload: &[u8],
) -> DispatchOutcome {
    let (code, _rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    match code {
        crate::wire::AuthSubCode::Ok => {
            *state = ProtoState::ConnectingPostAuthAwaitingKey(reply);
            DispatchOutcome::AdvancedSilent
        }
        // Any non-Ok auth code means the server expects an auth
        // method this Trust connection is not configured for:
        // `CleartextPassword` / `Md5Password` — Trust client
        // carries no password. `Sasl` / `SaslContinue` /
        // `SaslFinal` — Trust client never requested SCRAM.
        // Tier-1 exhaustive — a future new `AuthSubCode` variant
        // forces this match to be updated.
        other @ (crate::wire::AuthSubCode::CleartextPassword
            | crate::wire::AuthSubCode::Md5Password
            | crate::wire::AuthSubCode::Sasl
            | crate::wire::AuthSubCode::SaslContinue
            | crate::wire::AuthSubCode::SaslFinal) => install_errored(state,
            Some(reply.consume()),
            ProtocolError::UnsupportedAuthMethod { sub_code: crate::error::AuthSubCodeClass::KnownButWrong(other) },
        ),
    }
}

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingStartupScram`].
///
/// Only `AUTH_SASL` is acceptable here; the server taking `AUTH_OK`
/// without asking for the password is an auth-method mismatch on
/// the server side (client expected SCRAM).
fn dispatch_auth_in_startup_scram(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    mut scram: alloc::boxed::Box<crate::scram::session::ScramSession>,
    payload: &[u8],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    match code {
        crate::wire::AuthSubCode::Sasl => {
            if !mechanism_list_contains_scram(rest) {
                return install_errored(state, Some(reply.consume()), ProtocolError::scram_no_text(crate::scram::wire::ScramFailureClass::NoSupportedMechanism));
            }

            // Build client-first-message and SASLInitialResponse.
            // Write directly into the caller-owned `write_buf` and
            // record the range; materialise at the entry-point
            // boundary after the mutable write phase releases.
            // `build_sasl_initial_response` populates
            // `scram.client_first_bare` + `scram.client_nonce_b64`
            // IN PLACE through `&mut Box<ScramSession>`. The same
            // `Box<ScramSession>` allocation is reused across the
            // StartupScram → ServerFirst transition (zero allocator
            // ops). Per-handshake total: 1 alloc (StartupScram
            // construction) + 1 free (ServerFinal drop), zero
            // transitions in between — the literal "one heap alloc
            // per SCRAM connection" invariant.
            match build_sasl_initial_response(&mut scram, reserved) {
                Ok(range) => {
                    *state = ProtoState::ConnectingScramAwaitingServerFirst {
                        reply,
                        scram,
                    };
                    DispatchOutcome::AdvancedWithAction {
                        action: StagedAction::SendBytesRange(range),
                    }
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        // Any non-`Sasl` auth code in this state is an auth-method
        // mismatch: the SCRAM client expected the server to offer
        // SASL mechanisms but got a different code instead.
        // `CleartextPassword` / `Md5Password`: a SCRAM client
        // refuses to downgrade to a weaker password-auth method
        // even if it carries credentials (security: prevents
        // server-side downgrade attacks). `Ok` here means the
        // server accepted nothing without asking — also a mismatch
        // from the client's POV. Tier-1 exhaustive — a future new
        // `AuthSubCode` variant forces this match to be updated.
        other @ (crate::wire::AuthSubCode::Ok
            | crate::wire::AuthSubCode::CleartextPassword
            | crate::wire::AuthSubCode::Md5Password
            | crate::wire::AuthSubCode::SaslContinue
            | crate::wire::AuthSubCode::SaslFinal) => install_errored(state,
            Some(reply.consume()),
            ProtocolError::UnsupportedAuthMethod { sub_code: crate::error::AuthSubCodeClass::KnownButWrong(other) },
        ),
    }
}

/// Check if the SASL mechanism list contains SCRAM-SHA-256.
///
/// Happy-path fast-check — most servers announce
/// `"SCRAM-SHA-256\0\0"` as the only mechanism (or first), so a
/// single `starts_with` covers the common case in one compare
/// instead of a full NUL-split loop. Falls through to the generic
/// scan for the multi-mechanism case (rare — PG typically sends
/// only SCRAM-SHA-256, sometimes with `SCRAM-SHA-256-PLUS`).
#[inline]
fn mechanism_list_contains_scram(data: &[u8]) -> bool {
    // Fast path: first mechanism is SCRAM-SHA-256 followed by
    // the NUL separator. Covers ≥95% of real-world servers.
    if let Some(rest) = data.strip_prefix(SCRAM_SHA_256_MECHANISM)
        && let Some(&0) = rest.first()
    {
        return true;
    }
    // Slow path: walk NUL-separated names. e.g. server offering
    // ["SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"] lands here.
    for name in data.split(|b| *b == 0) {
        if name == SCRAM_SHA_256_MECHANISM {
            return true;
        }
    }
    false
}

/// Build the SASLInitialResponse frame for SCRAM-SHA-256.
///
/// # Pattern: compile-time capability proof
///
/// Takes `_: &ScramSession` — an anonymous-parameter typed reference
/// that is **never dereferenced**. The function does not use the
/// password inside the `ScramSession`; the password is consumed only
/// later in `dispatch_auth_sasl_continue`. So why require the
/// parameter at all?
///
/// The parameter is a **capability proof**. To construct a
/// `ScramSession` the caller must have discriminated away
/// `Credentials::Trust` (see `compute_push_startup` routing — Trust
/// lands in `ConnectingStartupTrust` which never reaches this
/// function; Scram lands in `ConnectingStartupScram { scram, .. }`
/// which does). Accepting `&ScramSession` as an argument forces the
/// caller to have that evidence at hand; passing no argument or the
/// wrong type is a compile error.
///
/// The anonymous `_` binding is intentional — the parameter shape is
/// load-bearing (the type `&ScramSession`), the binding is not (we
/// never read the value). The crate's "no `_var` prefixed discards"
/// rule applies to `let _prefix = expr;` bindings, not to
/// anonymous-parameter match discards (which are standard idiomatic
/// Rust for capability-proof parameters — see
/// e.g. `std::marker::Unpin` witness patterns).
///
/// # Trust-vs-Scram separation
///
/// The `Credentials`-vs-`ScramPassword` split happens exactly once
/// at `ScramSession::from_password`; this function cannot be reached
/// from a Trust-credentials push path because the state variant it
/// destructures from (`ConnectingStartupScram { scram, .. }`) carries
/// a `ScramSession`, not a `Credentials`.
///
/// [`ScramSession`]: crate::scram::session::ScramSession
///
/// Writes `client_first_bare` and `client_nonce_b64` directly into
/// the caller's `&mut ScramSession` (single source of truth for
/// handshake state — see `ScramSession` struct docstring). Returns
/// only the [`WriteRange`] for the wire bytes to send.
fn build_sasl_initial_response(
    scram: &mut crate::scram::session::ScramSession,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    use crate::scram::wire;

    // PG convention — the SCRAM `n=<user>` field is hard-coded
    // empty because the real user name travelled in the
    // StartupMessage's `user` parameter. PG's SCRAM implementation
    // explicitly ignores the SASL-level user (see PG
    // src/backend/libpq/auth-scram.c — the `user` from client-first
    // is never consulted; authentication is bound to the startup
    // user).
    //
    // RFC 5802 §5.1 allows an empty `saslname` per RFC 4013 SASLprep.
    // PG-specific clients set `n=` verbatim; non-PG SCRAM servers
    // (MongoDB, Kafka) REQUIRE a non-empty `n=` and this code path
    // would fail against those. `bsql-pg-proto` is PG-only by design;
    // interop with other SCRAM servers is out of scope.
    //
    // If a future phase adds non-PG SCRAM server support, plumb the
    // user identifier through as a `Sensitive<...>` argument here.
    let user_bytes: &[u8] = b"";

    let client_nonce_vec = wire::generate_client_nonce().map_err(ProtocolError::from_scram_no_text)?;

    let client_first_bare_vec =
        wire::build_client_first_bare(user_bytes, &client_nonce_vec).map_err(ProtocolError::from_scram_no_text)?;

    let client_first_msg =
        wire::build_client_first_message(user_bytes, &client_nonce_vec).map_err(ProtocolError::from_scram_no_text)?;

    // SCRAM auth is a cold handshake path. The scram::wire builders
    // hand back owned heapless::Vec; the bytes are pushed into the
    // branded reserved via `as_write_buf_mut()`. The brand is
    // preserved by the enclosing
    // `reserved: &mut BrandedWriteReserved<'wb>` —
    // `as_write_buf_mut` returns `&mut WriteBuf` without a brand,
    // and the pushed range is wrapped via
    // `WriteRange::from_write_span` below against the same reserved.
    let start = reserved.len();
    let buf = reserved.as_write_buf_mut();
    buf.push_u8(crate::wire::TAG_SASL_RESPONSE.byte())
        .map_err(|_| ProtocolError::scram_no_text(crate::scram::wire::ScramFailureClass::BufferOverflow))?;
    buf.with_length_prefix(|w| {
        w.push_bytes(SCRAM_SHA_256_MECHANISM)
            .map_err(|_| crate::write_buf::WriteBufFull)?;
        w.push_u8(0).map_err(|_| crate::write_buf::WriteBufFull)?;
        let body_len =
            i32::try_from(client_first_msg.len()).map_err(|_| crate::write_buf::WriteBufFull)?;
        w.push_i32_be(body_len)
            .map_err(|_| crate::write_buf::WriteBufFull)?;
        w.push_bytes(&client_first_msg)
            .map_err(|_| crate::write_buf::WriteBufFull)?;
        Ok(())
    })
    .map_err(|_| ProtocolError::scram_no_text(crate::scram::wire::ScramFailureClass::BufferOverflow))?;

    // Populate the SCRAM session's client_first_bare +
    // client_nonce_b64 fields IN PLACE — a naive shape that returned
    // them by value would force a re-Box and break the
    // single-allocation invariant. The caller's
    // `Box<ScramSession>` is reused across the StartupScram →
    // ServerFirst transition with zero allocator ops.
    scram.client_first_bare = crate::ident::PodBytes::try_from_slice(&client_first_bare_vec)
        .map_err(|_| ProtocolError::scram_no_text(crate::scram::wire::ScramFailureClass::BufferOverflow))?;
    scram.client_nonce_b64 = crate::ident::PodBytes::try_from_slice(&client_nonce_vec)
        .map_err(|_| ProtocolError::scram_no_text(crate::scram::wire::ScramFailureClass::BufferOverflow))?;
    // `from_write_span` returns `Result`; `?` propagates up
    // through the function's own Result return type. Err here
    // classifies as `EmptyWriteRange` — dead under intact SCRAM
    // invariants.
    crate::action::WriteRange::from_write_span(start, reserved)
}

/// Dispatch AuthenticationSASLContinue (server-first-message).
///
/// Takes a [`ScramSession`] by reference — the
/// `Trust`-vs-`ScramPassword` discrimination was consumed at
/// [`ScramSession::try_from_credentials`] in the parent dispatch
/// call; this function cannot be reached with `Trust` credentials
/// because the state variant it destructures from
/// ([`ProtoState::ConnectingScramAwaitingServerFirst`]) carries
/// `ScramSession`, not `Credentials`.
///
/// [`ScramSession`]: crate::scram::session::ScramSession
/// [`ScramSession::try_from_credentials`]: crate::scram::session::ScramSession::try_from_credentials
//
// The three SCRAM-handshake fields are consolidated inside one
// `Box<ScramSession>` carried by
// `ConnectingScramAwaitingServerFirst`. The caller destructures
// the variant and passes `scram` BY REFERENCE (helper only reads
// `scram.with_password_bytes(...)` once for HMAC composition; no
// memcpy of ~520 B onto this helper's stack frame, no re-Box). Net
// per SCRAM handshake: 0 allocs + 1 Box-free (the consolidated
// `Box<ScramSession>`).
fn dispatch_auth_sasl_continue(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    scram: &crate::scram::session::ScramSession,
    payload: &[u8],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::SaslContinue) {
        return install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION });
    }

    // `rest` is the server-first-message body.
    let server_first =
        match crate::scram::wire::parse_server_first(rest, scram.client_nonce_b64.as_slice()) {
            Ok(sf) => sf,
            Err(e) => {
                return install_errored(state, Some(reply.consume()), ProtocolError::from_scram_no_text(e));
            }
        };

    // Build client-final-without-proof. (Order-of-bindings note:
    // this lands above the password computation because the
    // closure-scoped `with_password_bytes` borrows `scram` and
    // forces all `scram.*` reads outside its scope.)
    let client_final_without_proof =
        match crate::scram::wire::build_client_final_without_proof(
            server_first.server_nonce.as_bytes(),
        ) {
            Ok(v) => v,
            Err(e) => {
                return install_errored(state, Some(reply.consume()), ProtocolError::from_scram_no_text(e));
            }
        };

    // Compute proof and expected server signature.
    //
    // AuthMessage = client-first-bare + "," + server-first + "," +
    // client-final-without-proof. The three components are passed
    // separately — `compute_client_proof` feeds them incrementally
    // into `HMAC::update()`, with zero intermediate buffer. No
    // staging buffer → no silent-truncation class → tier-1 by
    // construction. `compute_client_proof` returns `Result` on the
    // architecturally-dead `HmacKeyRejected` path. On Err (supply-
    // chain compromise of RustCrypto's HMAC, etc.), the handshake
    // tears down with a typed diagnostic — no continuation with
    // zero-filled bytes. Fail-closed.
    //
    // The closure-scoped `ScramSession::with_password_bytes`
    // HRTB-binds the `&[u8]` to the closure body — the
    // `compute_client_proof` call — and prevents it from escaping.
    // Password bytes physically die at closure return; only the
    // resulting `(proof, expected_server_sig)` tuple survives. A
    // naive borrow-and-return shape would invite the discipline-
    // by-docstring «callers must not cache it past the call
    // boundary» — banned.
    let client_first_bare = scram.client_first_bare.as_slice();
    let proof_result = scram.with_password_bytes(|password_bytes| {
        crate::scram::crypto::compute_client_proof(
            password_bytes,
            &server_first.salt,
            server_first.iterations,
            client_first_bare,
            rest,
            &client_final_without_proof,
        )
    });
    let (proof, expected_server_sig) = match proof_result {
        Ok(v) => v,
        Err(e) => return install_errored(state, Some(reply.consume()), ProtocolError::from_scram_no_text(e)),
    };

    // Base64-encode proof.
    //
    // Stack buffer holds base64
    // ClientProof — password-correlated via `SHA-256(ClientKey) =
    // StoredKey`, and `proof = ClientKey XOR HMAC(StoredKey, AuthMessage)`.
    // A core-dump attacker with the base64-decoded proof + AuthMessage
    // can derive StoredKey and replay the handshake. Wrap in
    // `zeroize::Zeroizing` so the 64-byte buffer scrubs on scope exit.
    let mut proof_b64_buf: zeroize::Zeroizing<[u8; 64]> = zeroize::Zeroizing::new([0_u8; 64]);
    let proof_b64_len =
        match crate::scram::wire::base64_encode_to_buf(proof.as_ref(), proof_b64_buf.as_mut()) {
            Ok(n) => n,
            Err(_) => {
                return install_errored(state, Some(reply.consume()), ProtocolError::scram_no_text(crate::scram::wire::ScramFailureClass::BufferOverflow))
            }
        };
    let proof_b64 = match proof_b64_buf.get(..proof_b64_len) {
        Some(s) => s,
        None => {
            return install_errored(state, Some(reply.consume()), ProtocolError::scram_no_text(crate::scram::wire::ScramFailureClass::BufferOverflow))
        }
    };

    // Build client-final-message.
    //
    // `client_final_msg` contains the embedded `p=<proof_b64>`
    // payload — password-correlated via the same StoredKey algebra
    // as `proof_b64_buf`. The `heapless::Vec` it lives in does NOT
    // implement `Zeroize` (upstream crate), so the `Zeroizing` wrap
    // is unavailable here. Instead, after the value has been copied
    // into the write buffer (push_bytes call below), the heapless::
    // Vec's backing bytes are zeroized in-place via
    // `Zeroize::zeroize()` on the mut slice (slice impl exists
    // upstream). Done just before the Vec drops at function scope
    // end.
    let mut client_final_msg = match crate::scram::wire::build_client_final_message(
        server_first.server_nonce.as_bytes(),
        proof_b64,
    ) {
        Ok(v) => v,
        Err(e) => {
            return install_errored(state, Some(reply.consume()), ProtocolError::from_scram_no_text(e));
        }
    };

    // Build SASLResponse frame via the branded reserved using the
    // `as_write_buf_mut()` escape hatch for the Result-returning
    // push path; `WriteRange::from_write_span` wraps the span at
    // the end.
    let start = reserved.len();
    {
        let buf = reserved.as_write_buf_mut();
        if buf.push_u8(crate::wire::TAG_SASL_RESPONSE.byte()).is_err()
            || buf
                .with_length_prefix(|w| w.push_bytes(&client_final_msg))
                .is_err()
        {
            // Scrub client_final_msg before early-return
            // classification. The buffer's bytes may contain partial
            // `p=<proof_b64>` payload depending on when the push
            // failed.
            use zeroize::Zeroize;
            client_final_msg.as_mut_slice().zeroize();
            return install_errored(state, Some(reply.consume()), ProtocolError::scram_no_text(crate::scram::wire::ScramFailureClass::BufferOverflow));
        }
    }
    // Scrub the password-correlated client_final_msg contents now
    // that they have been copied into the write buffer. The write
    // buffer itself is zeroed on `WriteBuf::clear()` separately.
    // Without this step the 384-byte heapless::Vec backing array
    // would hold `p=<proof_b64>` until this function's stack frame
    // is overwritten by a subsequent call.
    use zeroize::Zeroize;
    client_final_msg.as_mut_slice().zeroize();
    // `from_write_span` returns `Result`. Err is architecturally
    // dead here — the SASL_RESPONSE frame body always has the
    // 1-byte tag + 4-byte length prefix + the client-final-message
    // which is non-empty by SCRAM protocol. Classified as
    // `EmptyWriteRange` if triggered.
    let range = match crate::action::WriteRange::from_write_span(start, reserved) {
        Ok(r) => r,
        Err(cause) => return install_errored(state, Some(reply.consume()), cause),
    };

    // `expected_server_sig` moves INTO the variant — tier-1
    // compile (CREDO §1: variant-carries-field). The `scram`
    // ScramSession consumed here (moved into this function) is
    // NOT needed by the next state; its drop here fires
    // `ZeroizeOnDrop` — password material scrubbed exactly when
    // the handshake no longer needs it.
    *state = ProtoState::ConnectingScramAwaitingServerFinal {
        reply,
        expected_server_sig: alloc::boxed::Box::new(expected_server_sig),
    };
    DispatchOutcome::AdvancedWithAction {
        action: StagedAction::SendBytesRange(range),
    }
}

/// Dispatch AuthenticationSASLFinal (server-final-message).
fn dispatch_auth_sasl_final(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    expected_server_sig: crate::scram::types::SecretDigest,
    payload: &[u8],
    error_arena_slot: &mut Option<alloc::boxed::Box<crate::error_arena::ErrorArena>>,
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::SaslFinal) {
        return install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION });
    }

    // Parse server-final-message.
    //
    // The `Err(e)` arm is the ONLY SCRAM dispatch site that may
    // observe `ScramError::ServerScramError { message }` — the
    // server's `e=<text>` field per RFC 5802 §5.1. Route through
    // the arena-aware helper so the text is preserved via
    // `ErrorPayload::Scram` instead of dropped.
    let received_sig = match crate::scram::wire::parse_server_final(rest) {
        Ok(sig) => sig,
        Err(e) => {
            let cause = crate::error_arena::scram_error_to_protocol_error(e, error_arena_slot);
            return install_errored(state, Some(reply.consume()), cause);
        }
    };

    // Constant-time comparison.
    if !bool::from(expected_server_sig.ct_eq(&received_sig)) {
        return install_errored(state, Some(reply.consume()), ProtocolError::scram_no_text(crate::scram::wire::ScramFailureClass::SignatureMismatch));
    }

    // Signature matches. Await AuthenticationOk.
    *state = ProtoState::ConnectingScramAwaitingAuthOk(reply);
    DispatchOutcome::AdvancedSilent
}

/// Dispatch AuthenticationOk after SCRAM verification.
fn dispatch_auth_ok_after_scram(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    payload: &[u8],
) -> DispatchOutcome {
    // AuthOk has no trailing data; destructure with anonymous `_`
    // pattern (pattern-discard, not a `_`-prefixed identifier —
    // allowed by the `no underscore vars` discipline).
    let code = match auth_sub_code(payload) {
        Ok((code, _)) => code,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::Ok) {
        return install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION });
    }

    *state = ProtoState::ConnectingPostAuthAwaitingKey(reply);
    DispatchOutcome::AdvancedSilent
}

// -----------------------------------------------------------------
// Cleartext-password handshake
// -----------------------------------------------------------------

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingStartupCleartext`].
///
/// Only `AUTH_CLEARTEXT_PASSWORD` (sub-code 3) is acceptable here:
/// any other code means the server expects an auth method this
/// connection is not configured for. The match is tier-1 exhaustive
/// — adding a new `AuthSubCode` variant forces this dispatcher to
/// classify it explicitly.
///
/// # Drop chain
///
/// `password: Box<Sensitive<Password>>` is moved in by value. On the
/// happy path the password bytes are written into the
/// `PasswordMessage` frame (cleartext on the wire — TLS gate is the
/// driver-wrapper's responsibility) and the Box drops at function
/// return, scrubbing the in-memory copy via `ZeroizeOnDrop`. On any
/// error path the Box drops at function return through the same
/// chain.
fn dispatch_auth_in_startup_cleartext(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    password: alloc::boxed::Box<crate::sensitive::Sensitive<crate::password::Password>>,
    payload: &[u8],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> DispatchOutcome {
    let (code, _rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    match code {
        crate::wire::AuthSubCode::CleartextPassword => {
            // Build the PasswordMessage frame inline against the
            // branded write reservation, then transition to the
            // AuthOk-awaiting state. The Box drops at function
            // return after this arm, scrubbing the password bytes.
            match build_password_message(&password, reserved) {
                Ok(range) => {
                    *state = ProtoState::ConnectingCleartextAwaitingAuthOk(reply);
                    DispatchOutcome::AdvancedWithAction {
                        action: StagedAction::SendBytesRange(range),
                    }
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        // Any other auth code in this state is an auth-method
        // mismatch: the server expected something other than
        // cleartext-password. Tier-1 exhaustive.
        other @ (crate::wire::AuthSubCode::Ok
            | crate::wire::AuthSubCode::Md5Password
            | crate::wire::AuthSubCode::Sasl
            | crate::wire::AuthSubCode::SaslContinue
            | crate::wire::AuthSubCode::SaslFinal) => install_errored(state,
            Some(reply.consume()),
            ProtocolError::UnsupportedAuthMethod { sub_code: crate::error::AuthSubCodeClass::KnownButWrong(other) },
        ),
    }
}

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingCleartextAwaitingAuthOk`].
///
/// Mirror of [`dispatch_auth_ok_after_scram`]: only `AUTH_OK` is
/// acceptable; any other code or any other frame-tag is an
/// `UnexpectedFrame` protocol error.
fn dispatch_auth_ok_after_cleartext(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    payload: &[u8],
) -> DispatchOutcome {
    let code = match auth_sub_code(payload) {
        Ok((code, _)) => code,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::Ok) {
        return install_errored(
            state,
            Some(reply.consume()),
            ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION },
        );
    }

    *state = ProtoState::ConnectingPostAuthAwaitingKey(reply);
    DispatchOutcome::AdvancedSilent
}

/// Build the `PasswordMessage` frame for cleartext-password auth.
///
/// PG protocol §55.7: the frame is `'p'` (`TAG_SASL_RESPONSE` —
/// the byte is shared between SASL response and generic password
/// messages, disambiguated by context) + BE u32 length-field
/// (length includes itself + body) + password bytes + NUL
/// terminator.
///
/// The length-prefix wrapper handles the BE u32 framing; the
/// closure pushes password bytes followed by the trailing NUL.
///
/// # Tier-1 architectural-impossibility of `WriteBufFull`
///
/// The `Err(WriteBufFull)` arm propagated through `?` is
/// **architecturally unreachable** per the const-assert
/// `MAX_OWNED_SEND_LEN >= max_password_message_size()` in
/// `write_buf.rs`. The error path is preserved as defence in depth
/// (and to keep the function signature uniform with sibling
/// builders that DO have legitimate runtime overflow paths), but
/// any actual `Err` here would indicate a const-assert drift —
/// itself a build error. Routed through
/// `InternalCrateBug { BuilderCapacityOverflow }` via the existing
/// `From<WriteBufFull> for ProtocolError` impl on the unlikely
/// path; that classification gives forensic visibility if a future
/// contributor disables the const-assert.
fn build_password_message(
    password: &crate::sensitive::Sensitive<crate::password::Password>,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    let start = reserved.len();
    let buf = reserved.as_write_buf_mut();
    buf.push_u8(crate::wire::TAG_SASL_RESPONSE.byte())?;
    // Closure-scope `Sensitive::with_inner` HRTB-binds the
    // `&Password` borrow to the inner closure; the `as_bytes()`
    // slice borrows from `&Password` and cannot escape past the
    // closure either (transitively HRTB-bounded). A naive
    // `password.get().as_bytes()` chain would invite the borrow to
    // live past the call boundary.
    buf.with_length_prefix(|w| {
        password.with_inner(|pwd| w.push_bytes(pwd.as_bytes()))?;
        // PG requires NUL-terminated password in the PasswordMessage
        // body. The length-prefix above includes the NUL byte.
        w.push_u8(0)?;
        Ok(())
    })?;

    crate::action::WriteRange::from_write_span(start, reserved)
}

// -----------------------------------------------------------------
// MD5-password handshake
// -----------------------------------------------------------------

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingStartupMd5`].
///
/// Only `AUTH_MD5_PASSWORD` (sub-code 5) carrying a valid 4-byte
/// salt is acceptable here. Any other code → `UnsupportedAuthMethod`.
/// Wrong-length salt → `MalformedAuthentication`. Tier-1 exhaustive
/// — adding a new `AuthSubCode` variant forces this dispatcher to
/// classify it explicitly.
///
/// # Drop chain
///
/// `handshake: Box<Md5HandshakeState>` is moved in by value. The
/// Box drops at function return on every path (success or error)
/// through `Box::drop → Md5HandshakeState::drop → Sensitive::drop
/// → Password::drop`, scrubbing the in-memory password copy via
/// `ZeroizeOnDrop`. The MD5 digest computation in
/// [`crate::md5::compute_response_body`] additionally wraps every
/// password-derived intermediate buffer in `Zeroizing<>` so
/// nothing leaks even transiently.
fn dispatch_auth_in_startup_md5(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    handshake: alloc::boxed::Box<crate::md5::Md5HandshakeState>,
    payload: &[u8],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    match code {
        crate::wire::AuthSubCode::Md5Password => {
            // Salt must be EXACTLY 4 bytes per PG §55.4. Anything
            // else is a malformed Authentication frame — classify
            // tier-3, transition to Errored. `payload_len` reports
            // the FULL payload byte count (sub-code 4 B + salt
            // bytes) for forensic visibility; a well-formed
            // MD5 auth frame has payload_len == 8.
            let salt: [u8; 4] = match <[u8; 4]>::try_from(rest) {
                Ok(s) => s,
                Err(_) => {
                    return install_errored(
                        state,
                        Some(reply.consume()),
                        ProtocolError::MalformedAuthentication {
                            payload_len: payload.len(),
                        },
                    );
                }
            };

            // Build the PasswordMessage frame inline against the
            // branded write reservation. The handshake Box drops
            // at function return (after this arm) — Drop chain
            // scrubs password.
            match build_md5_password_message(&handshake, salt, reserved) {
                Ok(range) => {
                    *state = ProtoState::ConnectingMd5AwaitingAuthOk(reply);
                    DispatchOutcome::AdvancedWithAction {
                        action: StagedAction::SendBytesRange(range),
                    }
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        // Any other auth code in this state is an auth-method
        // mismatch. Tier-1 exhaustive — symmetric with the
        // cleartext + SCRAM dispatchers.
        other @ (crate::wire::AuthSubCode::Ok
            | crate::wire::AuthSubCode::CleartextPassword
            | crate::wire::AuthSubCode::Sasl
            | crate::wire::AuthSubCode::SaslContinue
            | crate::wire::AuthSubCode::SaslFinal) => install_errored(state,
            Some(reply.consume()),
            ProtocolError::UnsupportedAuthMethod { sub_code: crate::error::AuthSubCodeClass::KnownButWrong(other) },
        ),
    }
}

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingMd5AwaitingAuthOk`]. Mirror of
/// [`dispatch_auth_ok_after_cleartext`] /
/// [`dispatch_auth_ok_after_scram`].
fn dispatch_auth_ok_after_md5(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    payload: &[u8],
) -> DispatchOutcome {
    let code = match auth_sub_code(payload) {
        Ok((code, _)) => code,
        Err(cause) => {
            return install_errored(state, Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::Ok) {
        return install_errored(
            state,
            Some(reply.consume()),
            ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION },
        );
    }

    *state = ProtoState::ConnectingPostAuthAwaitingKey(reply);
    DispatchOutcome::AdvancedSilent
}

/// Build the `PasswordMessage` frame for MD5-password auth.
///
/// Wire shape: `'p' (TAG_SASL_RESPONSE)` + BE u32 length + 35-byte
/// MD5 response body (`"md5" + 32 hex chars`) + NUL terminator.
///
/// The MD5 digest is computed by [`crate::md5::compute_response_body`]
/// which wraps every password-derived intermediate in `Zeroizing<>`.
///
/// # Tier-1 architectural-impossibility of `WriteBufFull`
///
/// Same as [`build_password_message`]: the const-assert
/// `MAX_OWNED_SEND_LEN >= max_password_message_size()` in
/// `write_buf.rs` makes the error path architecturally
/// unreachable. The MD5 frame is fixed-size at 41 bytes total —
/// trivially under the 2176 B `MAX_OWNED_SEND_LEN` budget.
fn build_md5_password_message(
    handshake: &crate::md5::Md5HandshakeState,
    salt: [u8; 4],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<crate::action::WriteRange, ProtocolError> {
    // Compute the 35-byte response body. `compute_response_body`
    // returns an owned `Zeroizing<[u8; 35]>` — tier-1 type-level
    // array signature so the caller cannot accidentally pass a
    // wrong-size buffer or a buffer that wouldn't be fully
    // overwritten. The returned array scrubs on drop at fn return.
    //
    // Closure-scope `Sensitive::with_inner` HRTB-binds the
    // `&Password` borrow to the closure body —
    // `compute_response_body` reads it for the MD5 digest
    // computation, the digest result (35-byte Zeroizing array) is
    // the value returned from the closure (R is independent of the
    // password borrow lifetime).
    let user_bytes = handshake.user.as_bytes();
    let body = handshake.password.with_inner(|pwd| {
        crate::md5::compute_response_body(pwd, user_bytes, salt)
    });

    let start = reserved.len();
    let buf = reserved.as_write_buf_mut();
    buf.push_u8(crate::wire::TAG_SASL_RESPONSE.byte())?;
    buf.with_length_prefix(|w| {
        w.push_bytes(body.as_slice())?;
        // PG's PasswordMessage body must be NUL-terminated (cleartext
        // and MD5 share this contract — the body is treated as a
        // cstring at the server).
        w.push_u8(0)?;
        Ok(())
    })?;

    crate::action::WriteRange::from_write_span(start, reserved)
}

// -----------------------------------------------------------------
// Helper: parse ErrorResponse typed fields
// -----------------------------------------------------------------

/// Dedicated parser return type for [`parse_error_response`].
///
/// `parse_error_response` returns this struct; callers wanting the
/// public `ProtocolError` use [`Self::into_protocol_error`]. The
/// struct is crate-private — exposing it on the public API would
/// create a second shape for the same data. A naive shape that
/// returned `ProtocolError` directly would couple parser output to
/// the public error shape: the three fields of
/// `ServerErrorResponse` would be packed/unpacked twice (once at
/// the `ProtocolError::ServerErrorResponse { .. }` construction
/// site, once at the `let ProtocolError::ServerErrorResponse { ..
/// } = err else { ... }` deconstruction in `parse_and_resolve` and
/// any future introspection site), and a variant-rename refactor
/// would ripple into every caller.
///
/// # Layout
///
/// `Severity` 1 B + `SqlStateCode` 5 B + `ErrorRef` 8 B = 14 B
/// (+ alignment padding → 16 B on 4-byte-aligned targets). Identical
/// budget to the `ProtocolError::ServerErrorResponse` payload slice,
/// so no cascade size impact.
#[derive(Debug, Clone, Copy)]
#[must_use = "ParsedServerError holds an ErrorRef into the caller's ErrorArena; \
              convert via .into_protocol_error() or resolve the ref via \
              PgProtocol::get_server_error before drop. Silent drop loses the \
              only handle to server message/detail/hint strings."]
pub(crate) struct ParsedServerError {
    /// Severity classification.
    ///
    /// `None` indicates a non-conformant peer (missing S+V fields).
    /// `Some(Severity::Unknown)` indicates an unrecognised severity
    /// string. See [`ProtocolError::ServerErrorResponse::severity`]
    /// for the full classification.
    pub(crate) severity: Option<crate::error::Severity>,
    pub(crate) code: crate::error::SqlStateCode,
    pub(crate) details_ref: crate::error_arena::ErrorRef,
}

impl ParsedServerError {
    /// Wrap into the public `ProtocolError` shape.
    #[inline]
    pub(crate) fn into_protocol_error(self) -> ProtocolError {
        ProtocolError::ServerErrorResponse {
            severity: self.severity,
            code: self.code,
            details_ref: self.details_ref,
        }
    }
}

/// Parse an ErrorResponse payload into a classified error.
///
/// -residue audit follow-up (Sub-B): the per-field
/// inline cap (`MAX_ERROR_FIELDS`) and the largest per-field byte
/// budget (`MAX_ERROR_RESPONSE_FIELD_BYTES`) are promoted to
/// crate-visible consts so [`crate::partial_assembly::PREFIX_CAP`]'s
/// lower-bound assertion can derive its floor from these source-of-
/// truth values instead of repeating a hand-derived `5 * 1024`
/// literal. Drift between parser caps and PREFIX_CAP would risk
/// observational-inequivalence in the stream-and-truncate
/// universal-coverage path.
pub(crate) const MAX_ERROR_FIELDS: usize = 32;

/// Largest per-field typed-output byte budget across the
/// `parse_error_response` arms. Drives the [`crate::partial_assembly::PREFIX_CAP`]
/// lower-bound assertion. The 128-byte ceiling is the `message`
/// field's [`crate::error::SecretBoundedStr<128>`] cap; detail/hint
/// use smaller caps (`<96>`/`<64>`).
pub(crate) const MAX_ERROR_RESPONSE_FIELD_BYTES: usize = 128;

/// Per-field framing overhead in the wire body: 1 byte field-code
/// tag + 1 byte NUL terminator. Each field contributes
/// `MAX_ERROR_RESPONSE_FIELD_BYTES + ERROR_FIELD_FRAMING_OVERHEAD`
/// bytes worst-case in the inline-bounded parser-read region.
pub(crate) const ERROR_FIELD_FRAMING_OVERHEAD: usize = 2;

/// PG ErrorResponse body: series of typed fields, each = type-byte +
/// NUL-terminated string. Terminated by a bare NUL (0x00). We extract
/// 'S' (severity), 'C' (code), 'M' (message), 'D' (detail), 'H' (hint).
///
/// Cold path: called only when the server emits an `ErrorResponse`
/// frame (`'E'` tag). The `#[cold]` attribute tells LLVM to keep the
/// body out of hot-path inlining scope.
///
/// Returns [`ParsedServerError`] (a shape-stable struct). Callers
/// that want the public error shape use `.into_protocol_error()`;
/// callers that want to inspect the parsed fields (e.g. test
/// helper, future diagnostic layers) access the struct fields
/// directly.
#[cold]
fn parse_error_response(
    payload: &[u8],
    error_arena: &mut crate::error_arena::ErrorArena,
) -> ParsedServerError {
    use crate::error::{Severity, SqlStateCode};
    use crate::ident::SecretBoundedStr;
    // Typed fields. Severity → enum (1 byte); code → SqlStateCode
    // ([u8;5]); message/detail/hint → `SecretBoundedStr<N>`
    // (non-Copy, ZeroizeOnDrop) with explicit truncation marker
    // (no `.unwrap_or_default()` silent-truncation).
    //
    // Severity is `Option<Severity>` so an absent S/V field
    // collapses cleanly. A naive `severity_set: bool` +
    // `severity = Severity::Unknown` pair would force tier-3
    // audit-by-discipline (the bool flip has to stay in sync with
    // the enum assignment); `Option` makes desync impossible. Niche-
    // packed: `Severity::Unknown = 0` as `#[repr(u8)]` means
    // `Option<Severity>` stays 1 byte (same as `Severity` alone).
    let mut severity: Option<Severity> = None;
    let mut code = SqlStateCode::from_bytes(b"");
    let mut message: SecretBoundedStr<128> = SecretBoundedStr::default();
    let mut detail: SecretBoundedStr<96> = SecretBoundedStr::default();
    let mut hint: SecretBoundedStr<64> = SecretBoundedStr::default();

    // Bounded-iteration DoS shield. PG's documented ErrorResponse
    // field set has ~18 tags total (S, V, C, M, D, H, P, p, q, W,
    // s, t, c, n, F, L, R, plus future). A legitimate server sends
    // each at most once. Cap at 32 — 2× headroom for any future
    // addition. Beyond the cap, parsing stops and whatever fields
    // have already been extracted are used.
    //
    // Without this cap the loop is still bounded by
    // `payload.len() ≤ MAX_FRAME_LEN_FIELD ≈ 4 KB` (pos advances
    // monotonically, `payload.get(pos)` returns `None` at
    // end-of-payload), so a 4 KB pathological frame could produce
    // ~1300 tight iterations. The cap keeps the work bounded to
    // O(field_count) regardless of frame size. Tier-2 structural —
    // the invariant is enforced by the `for _ in 0..N` bound, not
    // an audit of `pos` math.
    //
    // **Module-level const reference**: `MAX_ERROR_FIELDS` is the
    // module-level `pub(crate)` const declared above; it is the
    // source-of-truth value that `partial_assembly::PREFIX_CAP`'s
    // lower-bound assertion derives its floor from. The local
    // alias here keeps the existing arm body's `MAX_ERROR_FIELDS`
    // unqualified references working with no body changes.
    const MAX_ERROR_FIELDS: usize = self::MAX_ERROR_FIELDS;

    // Drift pin: typed arms below extract structured fields into
    // named locals (severity / code / message / detail / hint). If
    // a contributor adds a new typed arm without raising the cap,
    // an adversarial server can flood the leading 32 fields with
    // noise and push a typed field out of range — silently lost
    // diagnostic. The assert below catches that class:
    // `MAX_ERROR_FIELDS` must hold at least 2× the typed-arm count
    // (every typed field plus an equal-sized noise prefix).
    // Updating the typed-arm list below requires updating this
    // slice in lockstep — the slice is the source of truth for
    // "how many typed extractors exist" and is referenced in the
    // assert; manual lockstep with the arms is tier-3 by-
    // discipline (a full tier-1 lift would require reflective arm
    // counting, which Rust does not expose).
    const KNOWN_TYPED_ERROR_FIELD_TAGS: &[u8] = b"SVCMDH";
    const _: () = assert!(
        MAX_ERROR_FIELDS >= KNOWN_TYPED_ERROR_FIELD_TAGS.len() * 2,
        "MAX_ERROR_FIELDS cap must be ≥ 2 × count of typed arms — \
         otherwise an adversarial flood can truncate a typed field \
         out of the parsed prefix.",
    );

    let mut pos: usize = 0;
    for _ in 0..MAX_ERROR_FIELDS {
        let field_type = match payload.get(pos) {
            Some(0) | None => break, // Terminator or end of payload.
            Some(b) => *b,
        };
        pos = match pos.checked_add(1) {
            Some(p) => p,
            None => break,
        };

        // Find NUL terminator for this field's value.
        //
        // `iter().position(|&b| b == 0)` on the slice tail is
        // LLVM-vectorisable (SIMD chunk-compare for u8 slices ≥ 8
        // bytes). A naive byte-by-byte
        // `while let Some(b) = payload.get(pos)` loop with per-
        // iter `checked_add(1)` would be O(N) with one compare +
        // one bounds-check + one add per byte; the iterator scan
        // LLVM lowers to SIMD for long fields (error messages can
        // be up to 128 bytes) — ~3× faster on the server-error
        // parsing path that fires on every ServerErrorResponse.
        let start = pos;
        // Single match on the natural failure mode: `split_at_checked`
        // returns None iff `start > payload.len()`. The surrounding
        // loop maintains `pos <= payload.len()` via `checked_add(1)`
        // + `break` on overflow; the None arm is architecturally dead
        // but classified via `cold_path` for the future-drift safety
        // net. Replaces the prior `if start > len { &[] } else
        // { payload.get(start..).unwrap_or(&[]) }` form whose two
        // dead-arm branches each carried a separate fallback.
        let tail: &[u8] = match payload.split_at_checked(start) {
            Some((_head, tail)) => tail,
            None => {
                core::hint::cold_path();
                &[]
            }
        };
        let value_bytes;
        match tail.iter().position(|&b| b == 0) {
            Some(n) => {
                // `n` is an index from `iter().position()`, so `n <=
                // tail.len()`; `split_at_checked(n)` always succeeds
                // unless `n > tail.len()` (architecturally impossible
                // post-position). The None arm is cold-hinted dead
                // code under the bundle's no-unwrap_or policy.
                value_bytes = match tail.split_at_checked(n) {
                    Some((head, _tail)) => head,
                    None => {
                        core::hint::cold_path();
                        &[]
                    }
                };
                // Advance past value + NUL. `start + n + 1 ≤
                // payload.len()` by construction (n is an index
                // into tail = payload[start..]), so checked_add
                // cannot fail unless start+n+1 > usize::MAX —
                // architecturally impossible for a ≤ 4 KB PG
                // frame. Classified dead-arm via saturating_add.
                pos = start.saturating_add(n).saturating_add(1);
            }
            None => {
                // No NUL terminator in remainder — wire-spec
                // violation but tolerate by using rest-of-payload
                // as the value (forward-compat). Exit loop next
                // iter via `pos > payload.len()` peek.
                value_bytes = tail;
                pos = payload.len();
            }
        }

        match field_type {
            // `S` (localised) takes precedence; `V` (non-localised,
            // PG 9.6+) fills in if `S` didn't. `severity.is_none()`
            // guard expresses "first-wins" precedence directly.
            b'S' | b'V' if severity.is_none() => {
                severity = Some(Severity::from_bytes(value_bytes));
            }
            b'C' => {
                code = SqlStateCode::from_bytes(value_bytes);
            }
            // F22: text fields come in as bytes. PG encodes them in
            // `client_encoding` which MAY be non-UTF-8 on legacy
            // servers; the lossy path preserves the ASCII subset
            // and visibly marks non-ASCII bytes with `?`. Previously
            // `from_utf8(..).unwrap_or("")` silently dropped the entire
            // field on any single invalid byte — tier-3 diagnostic loss.
            //
            // Funnel the wire bytes through the `LossyText` typed
            // witness before committing to bounded storage. The
            // type name surfaces the lossy contract at the call
            // site (no longer hidden inside
            // `from_bytes_lossy(value_bytes)`); `raw_bytes()` on
            // the `LossyText` instance is the escape hatch for
            // forensic byte-fidelity callers that may want
            // pre-coercion access in a future codepath.
            b'M' => {
                message = crate::ident::LossyText::from_bytes_lossy(value_bytes)
                    .to_secret_bounded::<128>();
            }
            b'D' => {
                detail = crate::ident::LossyText::from_bytes_lossy(value_bytes)
                    .to_secret_bounded::<96>();
            }
            b'H' => {
                hint = crate::ident::LossyText::from_bytes_lossy(value_bytes)
                    .to_secret_bounded::<64>();
            }
            _ => {} // Unknown field type — skip.
        }
    }

    // Allocate the bounded strings into the caller-supplied error
    // arena; return the small `ParsedServerError` (16 B) with the
    // `ErrorRef` handle.
    let details_ref = error_arena.alloc(crate::error_arena::ErrorPayload::ServerError {
        message,
        detail,
        hint,
    });
    ParsedServerError {
        // Preserve the absence as `None` instead of collapsing to
        // `Some(Severity::Unknown)`. The public API consumer (via
        // `ProtocolError::ServerErrorResponse`) can distinguish
        // "server didn't send S/V" (None) from "server sent an
        // unrecognised severity string" (Some(Severity::Unknown)).
        severity,
        code,
        details_ref,
    }
}

/// Parse a `NoticeResponse` payload into a [`crate::notices_arena::NoticePayload`]
/// and allocate it into the notices arena.
///
/// Same wire format as ErrorResponse. Uses `BoundedStr` (not Secret)
/// because notices carry operator-informational text, not credentials.
#[cold]
pub(crate) fn parse_and_alloc_notice(
    payload: &[u8],
    notices_arena: &mut crate::notices_arena::NoticesArena,
) -> Option<crate::notices_arena::NoticeRef> {
    use crate::error::SqlStateCode;
    use crate::ident::BoundedStr;

    let mut severity: BoundedStr<32> = BoundedStr::default();
    let mut code = SqlStateCode::from_bytes(b"");
    let mut message: BoundedStr<128> = BoundedStr::default();
    let mut detail: BoundedStr<96> = BoundedStr::default();
    let mut hint: BoundedStr<64> = BoundedStr::default();

    const MAX_FIELDS: usize = self::MAX_ERROR_FIELDS;
    let mut pos: usize = 0;
    for _ in 0..MAX_FIELDS {
        let field_type = match payload.get(pos) {
            Some(0) | None => break,
            Some(b) => *b,
        };
        pos = match pos.checked_add(1) {
            Some(p) => p,
            None => break,
        };
        let tail: &[u8] = match payload.split_at_checked(pos) {
            Some((_head, tail)) => tail,
            None => {
                core::hint::cold_path();
                &[]
            }
        };
        let value_bytes;
        match tail.iter().position(|&b| b == 0) {
            Some(n) => {
                value_bytes = match tail.split_at_checked(n) {
                    Some((head, _)) => head,
                    None => {
                        core::hint::cold_path();
                        &[]
                    }
                };
                pos = pos.saturating_add(n).saturating_add(1);
            }
            None => {
                value_bytes = tail;
                pos = payload.len();
            }
        }

        match field_type {
            b'S' | b'V' => {
                if severity.is_empty() {
                    severity = crate::ident::LossyText::from_bytes_lossy(value_bytes)
                        .to_bounded::<32>();
                }
            }
            b'C' => {
                code = SqlStateCode::from_bytes(value_bytes);
            }
            b'M' => {
                message = crate::ident::LossyText::from_bytes_lossy(value_bytes)
                    .to_bounded::<128>();
            }
            b'D' => {
                detail = crate::ident::LossyText::from_bytes_lossy(value_bytes)
                    .to_bounded::<96>();
            }
            b'H' => {
                hint = crate::ident::LossyText::from_bytes_lossy(value_bytes)
                    .to_bounded::<64>();
            }
            _ => {}
        }
    }

    notices_arena.alloc(crate::notices_arena::NoticePayload {
        severity,
        code,
        message,
        detail,
        hint,
    })
}

// -----------------------------------------------------------------
// Helper: parse BackendKeyData
// -----------------------------------------------------------------

/// Shared body for the `C` arm in both
/// `AwaitingFirstResponse` and `StreamingRows` states. Transitions
/// to `AwaitingRfq { reply, command_tag }` on a well-formed tag and
/// classifies a missing-NUL / framing error as `Errored`.
///
/// Centralises the "`CommandComplete` → AwaitingRfq" invariant in
/// one place — an arm-body edit at only one of the two call sites
/// would diverge silently; the helper makes the transition atomic.
///
/// Schema presence is observed via
/// `PgProtocol::row_desc_slot.is_some()` at materialise — no
/// synchronised flag. A naive shape would carry a
/// `schema_present: bool` stamped into the variant; the two call
/// sites (DML CommandComplete and SELECT StreamingRows
/// CommandComplete) only differ in the slot's prior population (DML
/// never fired a 'T' arm, so the slot is None; SELECT did, so it is
/// Some).
fn advance_to_awaiting_rfq(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::QueryKind>,
    payload: &[u8],
    command_tag_slot: &mut crate::command_tag_slot::CommandTagSlotCell,
) -> DispatchOutcome {
    // : typed parser + slot park. Classified on malformed
    // (missing NUL / embedded NUL) per pre-contract.
    match crate::command_tag::parse_command_tag_bytes(payload) {
        Ok(parsed) => {
            _command_complete_dispatch_leaf::park_command_tag_at_dispatch(
                command_tag_slot,
                alloc::boxed::Box::new(parsed),
            );
            *state = ProtoState::SimpleQueryAwaitingRfq { reply };
            DispatchOutcome::AdvancedSilent
        }
        Err(cause) => install_errored(state, Some(reply.consume()), cause),
    }
}

/// `CommandComplete` on the schema-bearing (SELECT) path →
/// `BindExecuteAwaitingRfqSelect`. The variant carries no
/// `row_desc` field; the schema lives in
/// `PgProtocol::row_desc_slot` (parked at push time by
/// `push_bind_execute`). The variant name `Select` is the tier-1
/// signal that the slot is populated.
///
/// The DML path's 'C' transition is inlined directly in the
/// dispatch arm (one call-site only) and doesn't need a helper.
fn advance_to_bindexecute_awaiting_rfq_select(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::QueryKind>,
    payload: &[u8],
    command_tag_slot: &mut crate::command_tag_slot::CommandTagSlotCell,
) -> DispatchOutcome {
    // : parse + park via slot pattern; classified on malformed.
    match crate::command_tag::parse_command_tag_bytes(payload) {
        Ok(parsed) => {
            _command_complete_dispatch_leaf::park_command_tag_at_dispatch(
                command_tag_slot,
                alloc::boxed::Box::new(parsed),
            );
            *state = ProtoState::BindExecuteAwaitingRfqSelect { reply };
            DispatchOutcome::AdvancedSilent
        }
        Err(cause) => install_errored(state, Some(reply.consume()), cause),
    }
}

/// Shared body for the `E` arm across multiple flows. Emits
/// `FailReply` (NO `CloseSocket` — query-level errors are
/// connection-survivable per PG §55.2.3) and transitions to
/// `DrainRfqAfterError` so the trailing `Z` returns the state to
/// `Idle`.
///
/// # Signature rationale — pre-consume at call site
///
/// The signature takes `raw_id: NonZeroU64` — the caller pre-
/// consumes the typed `ReplyId<K>`. A naive `<K: ReplyKind>`
/// generic taking `ReplyId<K>` by value would force
/// monomorphisation once per kind; since the body only uses
/// `reply.consume() -> NonZeroU64` (K-oblivious), every call site
/// would emit an identical 3-instruction basic block. The
/// pre-consumed shape lets LLVM emit one function body for all
/// kinds and mirrors the `errored(Some(reply.consume()), …)`
/// pattern elsewhere in this module.
///
/// # `#[cold] #[inline]`
///
/// Error drain is a cold branch — typical dispatch iterations
/// complete without encountering `ErrorResponse`. `#[cold]` pushes
/// this body out of the hot-match I-cache footprint; `#[inline]`
/// allows LLVM to fold the function into each call site when
/// register pressure permits. Same treatment as `install_errored`
/// above.
#[cold]
#[inline]
fn advance_to_drain_after_error(
    state: &mut ProtoState,
    raw_id: core::num::NonZeroU64,
    payload: &[u8],
    error_arena: &mut crate::error_arena::ErrorArena,
) -> DispatchOutcome {
    let cause = parse_error_response(payload, error_arena).into_protocol_error();
    *state = ProtoState::DrainRfqAfterError;
    DispatchOutcome::AdvancedWithAction {
        action: StagedAction::FailReply {
            id: raw_id,
            cause,
        },
    }
}

/// Parse a `ReadyForQuery` payload (body of the `'Z'` frame) into a
/// typed [`crate::action::TxStatus`].
///
/// PG §55.7 `ReadyForQuery` carries exactly one byte: `'I'`, `'T'`,
/// or `'E'`. Any other shape is a wire violation.
///
/// # Narrow return type
///
/// Returns `Result<TxStatus, usize>` — Err carries the offending
/// `payload_len` as a bare `usize`. Callers wrap via
/// `.map_err(|payload_len| ProtocolError::MalformedReadyForQuery { payload_len })`.
/// A naive `Result<TxStatus, ProtocolError>` shape would force
/// every dispatch arm to reserve ~304 B of stack for the return
/// slot (dominated by `ProtocolError::ServerErrorResponse`); the
/// narrow shape shrinks the slot to 16 B (usize + discriminant +
/// padding) across 10+ dispatch call sites.
///
/// # Single-point classifier
///
/// A naive shape would inline `match payload { [b] => ..., other
/// => ... }` at every `*AwaitingRfq` state (4+ parallel sites);
/// centralising here closes drift if a future change alters the
/// `TxStatus` variant set.
#[inline]
fn parse_rfq_payload(
    payload: &[u8],
) -> Result<crate::action::TxStatus, usize> {
    match payload {
        // `[tx_byte]` pattern proves payload_len == 1 structurally.
        // `try_from_byte` returns `Result<Self, u8>`; the rejected
        // byte is dropped here and only the length-1 classification
        // is forwarded. Diagnostic-wise the rejected byte is not
        // currently surfaced upstream; if `MalformedReadyForQuery`
        // gains a `byte` field, this `map_err` flips to pass it
        // through.
        [tx_byte] => crate::action::TxStatus::try_from_byte(*tx_byte).map_err(|_| 1usize),
        other => Err(other.len()),
    }
}

// : `parse_command_tag` (returning `BoundedStr<32>`)
// removed. The typed [`crate::command_tag::parse_command_tag_bytes`]
// in `mod command_tag` replaces it — produces a typed
// [`crate::command_tag::CommandTag`] enum with known commands
// (Insert/Update/Select/Delete/Fetch/Move/Copy) carrying parsed
// u64 row counts and `Other(BoundedStr<32>)` freeform fallback.

/// Parse BackendKeyData payload: 8 bytes = pid(i32 BE) + secret_key(i32 BE).
///
/// Cold path: called once per connection at end of startup handshake.
/// Not on any per-frame or per-query hot path.
#[cold]
fn parse_backend_key_data(payload: &[u8]) -> Result<(i32, i32), ProtocolError> {
    match payload {
        [a, b, c, d, e, f, g, h] => {
            let pid = i32::from_be_bytes([*a, *b, *c, *d]);
            let secret_key = i32::from_be_bytes([*e, *f, *g, *h]);
            Ok((pid, secret_key))
        }
        other => Err(ProtocolError::MalformedBackendKeyData {
            payload_len: other.len(),
        }),
    }
}

#[cfg(test)]
mod parse_error_response_tests {
    //! Seam-closing tests for `parse_error_response` (S4 / B1 from the
    //! 2026-04-18 second-pass audit).
    //!
    //! The function has nine field-type arms (`b'S'`, `b'V'`, `b'C'`,
    //! `b'M'`, `b'D'`, `b'H'`, unknown) mapping to five
    //! `ServerErrorResponse` fields. Swapping any two arms compiles
    //! cleanly — the lint-level type checker cannot see that `b'M'`
    //! should land in `message` and `b'D'` should land in `detail`.
    //! Tests below set each field explicitly and assert the full
    //! `ProtocolError` value for byte-exact mapping.
    //!
    //! Also covers pathological inputs:
    //! - Empty payload (just the terminator).
    //! - Field type with no NUL-terminated value (unterminated).
    //! - Duplicate severity (S / V) — first wins (`if severity.is_empty()`).
    //!
    //! Category (1) per reforge.md §4.11. Uses `assert_eq!` on the
    //! full `ProtocolError` variant because `panic!` is crate-root
    //! forbidden even in unit tests.

    extern crate alloc;

    use super::*;

    /// Build a well-formed ErrorResponse body: a sequence of
    /// `type_byte + NUL-terminated-value` entries, followed by a
    /// single trailing `\0` terminator.
    fn build_error_body(fields: &[(u8, &[u8])]) -> alloc::vec::Vec<u8> {
        let mut body = alloc::vec::Vec::new();
        for (type_byte, value) in fields {
            body.push(*type_byte);
            body.extend_from_slice(value);
            body.push(0);
        }
        body.push(0); // Terminator.
        body
    }

    /// Test-fixture tuple — (Option<Severity>, SqlStateCode,
    /// ErrorPayload). Tests build expected tuples and compare
    /// against parsed actual via `parse_and_resolve`. Severity is
    /// `Option<_>` to disambiguate "server didn't send" (None) from
    /// "server sent unknown" (Some(Severity::Unknown)).
    type ExpectedErr = (
        Option<crate::error::Severity>,
        crate::error::SqlStateCode,
        crate::error_arena::ErrorPayload,
    );

    fn mk_err(
        severity: &str,
        code: &str,
        message: &str,
        detail: &str,
        hint: &str,
    ) -> ExpectedErr {
        use crate::error::{Severity, SqlStateCode};
        use crate::ident::SecretBoundedStr;
        // Convention: an empty severity string means "no S/V field
        // in the wire payload" — maps to `None`. A non-empty string
        // maps to `Some(Severity::from_bytes(...))` which classifies
        // known severities and falls back to `Severity::Unknown`
        // for unrecognised strings.
        let parsed_severity = if severity.is_empty() {
            None
        } else {
            Some(Severity::from_bytes(severity.as_bytes()))
        };
        (
            parsed_severity,
            SqlStateCode::from_bytes(code.as_bytes()),
            crate::error_arena::ErrorPayload::ServerError {
                message: SecretBoundedStr::<128>::from_str_truncating(message),
                detail: SecretBoundedStr::<96>::from_str_truncating(detail),
                hint: SecretBoundedStr::<64>::from_str_truncating(hint),
            },
        )
    }

    /// Parse `body` using a fresh arena, resolve the `ErrorRef`
    /// into a full payload, and return the comparable tuple.
    ///
    /// The `arena.get(details_ref)` Err arm is architecturally
    /// unreachable by construction (`parse_error_response` always
    /// allocates into the fresh arena; the returned ref's
    /// generation matches; no intervening clear or realloc can
    /// fire here). A naive `.unwrap_or_default()` would form the
    /// banned silent-fallback pattern; instead the Result is
    /// asserted-ok and matched exhaustively, with the dead arm
    /// inlined as an empty-payload sentinel.
    fn parse_and_resolve(body: &[u8]) -> ExpectedErr {
        let mut arena = crate::error_arena::ErrorArena::new();
        let parsed = parse_error_response(body, &mut arena);
        let r = arena.get(parsed.details_ref);
        // Forbid-bundle compliance: `assert!(is_ok, ...) + match
        // { Ok | Err(_) => fallback }` — the assert fires loudly
        // if the invariant (parse always populates the fresh arena,
        // no intervening clear) breaks; the structural match
        // consumes `r.cloned()` exhaustively so the
        // architecturally-dead Err arm doesn't trip the bundle's
        // `unwrap_used` ban. The Err arm's fallback is inlined as
        // empty `SecretBoundedStr` fields.
        assert!(
            r.is_ok(),
            "parse_error_response + arena.get Err {r:?} — architecturally unreachable \
             (parse always allocates into the fresh arena, no intervening clear)",
        );
        let payload = match r.cloned() {
            Ok(p) => p,
            Err(_) => crate::error_arena::ErrorPayload::ServerError {
                message: crate::ident::SecretBoundedStr::<128>::new(),
                detail: crate::ident::SecretBoundedStr::<96>::new(),
                hint: crate::ident::SecretBoundedStr::<64>::new(),
            },
        };
        (parsed.severity, parsed.code, payload)
    }

    /// Invariant (spec): each known field type routes to its dedicated
    /// `ServerErrorResponse` field. A one-arm swap in `parse_error_response`
    /// compiles silently; this table catches it via full-value equality.
    #[test]
    fn field_type_routes_to_correct_output_field() {
        let body = build_error_body(&[
            (b'S', b"FATAL"),
            (b'C', b"28P01"),
            (b'M', b"authentication failed"),
            (b'D', b"user does not exist"),
            (b'H', b"check pg_hba.conf"),
        ]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err(
            "FATAL",
            "28P01",
            "authentication failed",
            "user does not exist",
            "check pg_hba.conf",
        );
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): `b'V'` is an alternate for `b'S'` (non-localised
    /// severity). When only `V` arrives, it populates `severity`.
    #[test]
    fn severity_v_used_when_s_absent() {
        let body = build_error_body(&[(b'V', b"ERROR")]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("ERROR", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): first severity (`S` or `V`) wins — the
    /// `if severity.is_empty()` guard in the S/V arms blocks overwrite.
    #[test]
    fn severity_s_wins_over_later_v() {
        let body = build_error_body(&[(b'S', b"FATAL"), (b'V', b"ERROR")]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("FATAL", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): unknown field types are silently dropped;
    /// other fields still parse.
    #[test]
    fn unknown_field_types_are_silently_skipped() {
        let body = build_error_body(&[
            (b'Z', b"irrelevant"),
            (b'M', b"real message"),
            (b'Q', b"also irrelevant"),
        ]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "", "real message", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): empty payload (just the terminator NUL)
    /// yields an all-empty `ServerErrorResponse`, not a parse failure
    /// or panic.
    #[test]
    fn empty_payload_yields_empty_fields() {
        let body: alloc::vec::Vec<u8> = alloc::vec![0];
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): a field-type byte with no NUL-terminated
    /// value (adversarial input: payload ends mid-field) does not
    /// loop forever and does not panic. Pins that the
    /// `while let Some(b) = payload.get(pos)` inner loop terminates
    /// on end-of-buffer. A regression using unchecked indexing would
    /// loop / panic; this test verifies graceful termination.
    #[test]
    fn unterminated_final_field_does_not_panic() {
        // [S, 'A', 'B'] — no NUL after 'B', no terminator.
        let body: alloc::vec::Vec<u8> = alloc::vec![b'S', b'A', b'B'];
        let actual = parse_and_resolve(&body);
        // Whatever the parser recovered: must be a bounded string and
        // must not panic. Exact value of severity is an implementation
        // detail (parser reads to EOF as the value).
        let expected = mk_err("AB", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): duplicate non-severity fields — second
    /// overwrites first. Pins that the unguarded assignments for
    /// `C`, `M`, `D`, `H` arms hold. A regression that added `if
    /// .is_empty()` guards would silently change the semantics
    /// (first-wins vs last-wins).
    #[test]
    fn duplicate_code_second_wins() {
        let body = build_error_body(&[(b'C', b"FIRST"), (b'C', b"SECOND")]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "SECOND", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Non-UTF-8 bytes in M/D/H fields are coerced to `?`
    /// placeholders, preserving ASCII content. A naive
    /// `from_utf8(value).unwrap_or("")` shape would lose the entire
    /// field on any single invalid byte; a message like
    /// `b"Ung\xFCltige Eingabe"` (Latin-1 "Ungültige Eingabe")
    /// would become `""` — full diagnostic loss. The lossy decoder
    /// preserves the ASCII subset: `b"Ung\xFCltige Eingabe"` →
    /// `"Ung?ltige Eingabe"`.
    #[test]
    fn non_utf8_message_preserves_ascii_subset() {
        // Latin-1 "Ungültige Eingabe" — the \xFC is ü in Latin-1,
        // invalid as a standalone UTF-8 byte.
        let body = build_error_body(&[(b'M', b"Ung\xFCltige Eingabe")]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "", "Ung?ltige Eingabe", "", "");
        assert_eq!(actual, expected);
    }

    /// Valid UTF-8 multibyte sequences pass through unchanged
    /// (fast path).
    #[test]
    fn valid_utf8_message_preserved_verbatim() {
        // Proper UTF-8 "Ungültige Eingabe".
        let body = build_error_body(&[(b'M', "Ungültige Eingabe".as_bytes())]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "", "Ungültige Eingabe", "", "");
        assert_eq!(actual, expected);
    }

    /// Slow-path control + high-bit bytes in an invalid-UTF-8
    /// payload all coerce to `?`. Ensures that binary junk in a
    /// field doesn't produce an empty message.
    ///
    /// Note: the `0xFF` byte triggers slow-path (invalid UTF-8). In
    /// slow path, every byte that isn't ASCII printable or
    /// `\t`/`\n`/`\r` coerces — including the otherwise-valid-UTF-8
    /// control bytes `0x01` / `0x02`. (Fast path, separately tested,
    /// preserves valid UTF-8 verbatim including control bytes.)
    #[test]
    fn non_utf8_control_bytes_coerced_to_question_mark() {
        // Mix: ASCII + 0x01 (valid UTF-8 SOH) + 0xFF (invalid UTF-8
        // high-bit lead) + 0x02 (SOT). The 0xFF forces slow path.
        let body = build_error_body(&[(b'M', b"A\x01B\xFFC\x02")]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "", "A?B?C?", "", "");
        assert_eq!(actual, expected);
    }
}
