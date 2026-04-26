//! `(prev_state, frame_tag) → outcome` matcher.
//!
//! The dispatcher is the **single** place the protocol decides what to
//! do with a freshly-parsed frame given the current state. The match is
//! exhaustive over `(state, tag)` pairs the Phase 1b flows can encounter;
//! adding a new state or tag is a build error until it is wired into
//! the matcher.
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
//! # DEF-184 A7 (2026-04-24) — tag LUT path rejected, DO NOT retry
//!
//! A tempting refactor is to replace `match (prev, tag: InboundTag)`
//! with a compact `InboundTagClass` enum (17 dense variants + classify
//! step) under the hypothesis "dense-discriminant jump table beats
//! sparse-ASCII-byte switch". **This was implemented, measured, and
//! rejected** — commit `1a762ca` (reverted).
//!
//! Measured result (criterion against `def184-complete` baseline,
//! aarch64-apple-darwin): all 4 hot-path benches regressed
//! (+2.6% to +8.2%, p<0.05). Modern LLVM already lowers the byte
//! switch into a compact cmp-and-branch chain that CSEs across arms;
//! the extra classify() call + `InboundTagClass::Unknown` catch-all
//! branch add indirection LLVM cannot fold out. Hypothesis falsified.
//!
//! If you are tempted to reopen A7: first produce a NEW criterion
//! measurement refuting the 2026-04-24 result (different machine,
//! different LLVM, or architectural change in the dispatch loop).
//! See `reforge.md §4.12` (measurement-gated perf) and
//! `deferred.md §B` (measurement-rejected items) before touching.

use crate::action::StagedAction;
use crate::error::ProtocolError;
use crate::reply_id::ReplyId;
use crate::state::ProtoState;
use crate::wire::{
    SCRAM_SHA_256_MECHANISM, TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_BIND_COMPLETE,
    TAG_COMMAND_COMPLETE, TAG_EMPTY_QUERY_RESPONSE, TAG_ERROR_RESPONSE,
    TAG_NEGOTIATE_PROTOCOL_VERSION, TAG_NO_DATA, TAG_PARAMETER_DESCRIPTION, TAG_PARSE_COMPLETE,
    TAG_PORTAL_SUSPENDED, TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};
// DEF-154 (B) Phase B4: `WriteBuf` import no longer needed here —
// dispatch takes `&mut BrandedWriteReserved<'_>` post-migration.

/// What to do after dispatching a single frame.
///
/// Three variants to keep the "emit zero actions" and "emit one
/// action" cases structurally distinct (audit round 2 A4).
///
/// # DEF-184 (B21/C6) — by-ref state, no `new_state` payload
///
/// Pre-(B21/C6) the Advanced variants carried `new_state: ProtoState`
/// (712 B each). Every successful dispatch produced a by-value
/// DispatchOutcome of ~800 B that the caller moved into its `match`
/// arm then wrote back into `*state`. LLVM does NOT optimise the
/// round-trip because `ProtoState` contains an opaque password
/// buffer (non-trivial move semantics).
///
/// Post-(B21/C6) dispatch takes `state: &mut ProtoState` and writes
/// the transition directly. DispatchOutcome shrinks to the
/// **side-effect** signal only:
/// - `AdvancedSilent` — no payload (~1 B discriminant).
/// - `AdvancedWithAction { action }` — 88 B `StagedAction`.
/// - `Errored { reply_id, cause }` — 80 B classified failure.
///
/// Size: 88 B exact (vs ~800 B). Pin in lib.rs.
///
/// # Real-world win sizing (honest scope — architect audit 2026-04-24)
///
/// `dispatch()` runs **per frame**, not per row — and `DataRow`
/// bypasses `dispatch()` entirely via the
/// `row_stream::fast_path_data_row` fast-path. Typical query
/// routes through dispatch ~3-6 times (Parse/Bind complete,
/// RowDescription, CommandComplete, RFQ). So the true saving is
/// ~712 B × 3-6 per query, not 712 MB per million rows.
///
/// The material benefit is **async future frame reduction**: every
/// `feed_bytes` suspension point in a downstream async wrapper
/// carries the 800 → 88 B delta, which snowballs through the
/// `async fn` state-machine nesting. That is the real per-QPS
/// win — smaller suspended futures, better L1 residency on
/// poll reawakening.
// DEF-184 (A1+A13): DispatchOutcome's Errored.cause ProtocolError
// shrunk 312 → ~72 B via ErrorArena externalisation.
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
    /// DEF-094: `StagedAction` is range-based — the entry-point
    /// materialises into a ref-bound `Action<'buf>` after the write
    /// phase releases.
    ///
    /// DEF-154 (Y): `'r` deleted post-StreamRowRange removal —
    /// `StagedAction` no longer carries read-buf references (the
    /// only lifetime'd field was `StreamRowRange::row_bytes`).
    AdvancedWithAction {
        /// The single side-effect to push.
        action: StagedAction,
    },
    /// Frame rejected; connection irrecoverable. Caller tears down.
    /// State has already been set to `ProtoState::Errored(kind)` by
    /// the dispatch helper [`install_errored`] (the caller observes
    /// the terminal state without needing a second write).
    ///
    /// # Pre-consumed reply_id (DEF-112)
    ///
    /// `reply_id` is `Option<NonZeroU64>` (already-consumed raw
    /// value), not `Option<ReplyId<K>>`. Rationale: dispatchers
    /// are parameterised per-command-kind after DEF-112, and the
    /// Errored path is kind-agnostic (the downstream action is a
    /// `FailReply { id: NonZeroU64, cause }` that carries no
    /// payload). Pre-consuming at each dispatcher's Errored
    /// construction site keeps the DispatchOutcome kind-free and
    /// avoids forcing DispatchOutcome to be generic over K.
    Errored {
        reply_id: Option<core::num::NonZeroU64>,
        cause: ProtocolError,
    },
}

/// DEF-103 + DEF-184 (B21/C6): `#[cold] #[inline]` helper centralising
/// every `DispatchOutcome::Errored` construction **plus** the
/// `*state = ProtoState::Errored(...)` install.
///
/// Pre-(B21/C6) this was `errored(reply_id, cause) -> DispatchOutcome`
/// and the caller wrote `*state = ProtoState::Errored(kind)` in the
/// outer match arm on the Errored variant. Post-(B21/C6) state is
/// a `&mut` parameter to `dispatch()`; installing the terminal state
/// inside the helper keeps the seam tight (no arm can forget to set
/// Errored while returning `DispatchOutcome::Errored`).
///
/// The `#[cold]` marker tells LLVM to push the Errored-path basic
/// block out of the hot-path I-cache footprint; `#[inline]` keeps
/// the call-site free of an actual function call (the helper body
/// folds into the caller).
///
/// `reply_id` is `Option<NonZeroU64>` (already-consumed raw
/// value) per DEF-112's pre-consume convention.
#[cold]
#[inline]
fn install_errored(
    state: &mut ProtoState,
    reply_id: Option<core::num::NonZeroU64>,
    cause: ProtocolError,
) -> DispatchOutcome {
    *state = ProtoState::Errored(cause.state_kind());
    DispatchOutcome::Errored { reply_id, cause }
}

// DEF-177 + DEF-184 (B21/C6) `install_internal_bug` DELETED 2026-04-25
// (DEF-188 cascade): the helper's only callers were the three
// `SchemaArenaAllocFull` arms in dispatch (one per RowDescription
// destination) plus a row-range-construction site that no longer
// exists. With DEF-188's arena deletion, no dispatch arm currently
// classifies into `InternalCrateBug`. The helper became dead code
// — re-introduce inline if a future arm needs the same shape.

/// Dispatch a single frame.
///
/// `write_buf` is the caller-owned outbound staging buffer (DEF-094);
/// dispatchers that produce [`StagedAction::SendBytesRange`] write
/// into it and record the range. The caller (feed_bytes) is
/// responsible for clearing `write_buf` at the start of each
/// entry-point call and materialising the ranges into `&'buf [u8]`
/// slices after the write-phase mutable borrow completes.
///
/// # DEF-188 — `terminal_row_desc: &mut Option<RowDesc>`
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
/// Pre-DEF-188 the equivalent slot was a 2-slot `SchemaArena` with
/// generation tracking; the dispatch path took
/// `arena: &mut ArenaWriter<'_>`. Deleting the arena removes the
/// `StaleSchemaRef` class entirely (no handle, no generation
/// drift) and shrinks the per-row hot-path by removing the dual
/// arena lookup.
///
/// # DEF-184 (B21/C6) — state by `&mut`
///
/// Pre-(B21/C6) dispatch took `prev: ProtoState` by value (712 B
/// memcpy per call) and returned `DispatchOutcome::Advanced* {
/// new_state: ProtoState, ... }` (another 712 B on the stack
/// return). Caller wrote the transition via
/// `*caller_state = outcome.new_state`. That's two 712 B memcpies
/// per dispatch iteration — on 1M-row SELECT workloads, ~1.4 GB
/// stack traffic purely for state round-tripping.
///
/// Post-(B21/C6) dispatch takes `state: &mut ProtoState`, snaps
/// the previous state via `core::mem::take(state)` for pattern
/// matching (state reset to `Default` == `Idle` during the match),
/// then each arm writes `*state = new_state` directly — one
/// store, no round trip. DispatchOutcome shrinks to the
/// **side-effect** signal only (see `DispatchOutcome` docs).
///
/// Invariant: EVERY match arm must either (a) assign `*state =
/// new_state` before returning `AdvancedSilent`/`AdvancedWithAction`,
/// or (b) delegate to [`install_errored`] / [`install_internal_bug`]
/// which install `ProtoState::Errored(...)`. Forgetting to assign
/// leaves state at the default `Idle` — a silent regression class
/// the compiler cannot catch. Mitigation: arm-body coverage tests
/// across all transitions (existing 230-test suite exercises every
/// `(state, tag)` pair reachable in the state machine).
pub(crate) fn dispatch(
    state: &mut ProtoState,
    tag: crate::wire::InboundTag,
    payload: &[u8],
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    terminal_row_desc: &mut Option<crate::decode::RowDesc>,
    error_arena: &mut crate::error_arena::ErrorArena,
) -> DispatchOutcome {
    // DEF-184 (B21/C6): snap owned prev for pattern matching; state
    // slot holds the explicit `ProtoState::Idle` placeholder during
    // the match. Every match arm below MUST `*state = <transition>`
    // before returning a non-Errored outcome; the install_errored /
    // install_internal_bug helpers handle Errored transitions.
    //
    // Use `mem::replace` (not `mem::take`) to make the placeholder
    // explicit — `mem::take` silently relies on the `Default` impl
    // returning `Idle`, and a future `Default` change could swap
    // placeholder semantics under us.
    let prev = core::mem::replace(state, ProtoState::Idle);
    let outcome = match (prev, tag) {
        // =============================================================
        // Ping flow (Phase 1a, carried forward)
        // =============================================================
        (ProtoState::PingAwaitingRfq(id), TAG_READY_FOR_QUERY) => {
            // DEF-112: `id: ReplyId<PingKind>` — the typed
            // `deliver` helper binds the payload to `PongPayload`
            // at compile time. Attempting to deliver any other
            // payload type here is a type error.
            //
            // Tier-1 tx_status validation via the centralised
            // `parse_rfq_payload` (F13): users never receive a
            // `TxStatus` outside `{Idle, InTransaction, Failed}`;
            // any other byte is a wire violation classified as
            // `MalformedReadyForQuery` with the correct `payload_len`.
            match parse_rfq_payload(payload) {
                Ok(tx_status) => {
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            id,
                            crate::action::PongPayload { tx_status },
                        ),
                    }
                }
                Err(payload_len) => install_errored(state, Some(id.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
            }
        }
        (ProtoState::PingAwaitingRfq(id), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, error_arena).into_protocol_error();
            install_errored(state, Some(id.consume()), cause)
        }
        (ProtoState::PingAwaitingRfq(id), other) => install_errored(state, Some(id.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingStartupTrust — awaiting AuthenticationOk
        // (DEF-097: Trust connections cannot accept AUTH_SASL — that
        // case is a per-variant dispatcher arm now, not a runtime
        // classification.)
        // =============================================================
        (ProtoState::ConnectingStartupTrust { reply }, TAG_AUTHENTICATION) => {
            dispatch_auth_in_startup_trust(state, reply, payload)
        }
        (ProtoState::ConnectingStartupTrust { reply }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, error_arena).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingStartupTrust { reply }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnsupportedProtocolOption)
        }
        (ProtoState::ConnectingStartupTrust { reply }, other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingStartupScram — awaiting AuthenticationSASL
        // (DEF-097: mirror of the Trust arm. A Scram connection
        // receiving AUTH_OK in this state is classified as
        // `UnsupportedAuthMethod` — the server accepted without
        // challenge while the user supplied a password, a PG policy
        // mismatch worth surfacing.)
        // =============================================================
        (ProtoState::ConnectingStartupScram { reply, scram }, TAG_AUTHENTICATION) => {
            // DEF-184 (A10/B22 revert 2026-04-24): `scram: ScramSession`
            // is destructured DIRECTLY from the variant — variant-
            // carries-field is tier-1 compile (CREDO §1). No drift
            // classifier needed: the variant cannot exist without its
            // SCRAM session.
            dispatch_auth_in_startup_scram(state, reply, scram, payload, reserved)
        }
        (ProtoState::ConnectingStartupScram { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, error_arena).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingStartupScram { reply, .. }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnsupportedProtocolOption)
        }
        (ProtoState::ConnectingStartupScram { reply, .. }, other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // SCRAM: awaiting server-first-message
        // =============================================================
        (
            ProtoState::ConnectingScramAwaitingServerFirst {
                reply,
                scram,
                client_first_bare,
                client_nonce_b64,
            },
            TAG_AUTHENTICATION,
        ) => {
            // DEF-184 (A10/B22 revert 2026-04-24): heavy SCRAM fields
            // destructured inline from the variant — tier-1 invariant
            // (CREDO §1: variant-carries-field). No drift path.
            //
            // DEF-187 (architect 2026-04-26): client_first_bare and
            // client_nonce_b64 are Box<PodBytes<N>> in the variant for
            // variant-size compaction; deref-move (`*box`) extracts
            // the PodBytes by value for the dispatch helper which takes
            // PodBytes by value (clippy::boxed_local would fire if we
            // kept Box in the helper signature). `scram` stays Box since
            // ScramSession is large enough that by-value would memcpy
            // 520 B onto the helper's stack frame.
            dispatch_auth_sasl_continue(
                state,
                reply,
                scram,
                *client_first_bare,
                *client_nonce_b64,
                payload,
                reserved,
            )
        }
        (ProtoState::ConnectingScramAwaitingServerFirst { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, error_arena).into_protocol_error();
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
            // DEF-184 (A10/B22 revert 2026-04-24): `expected_server_sig`
            // destructured inline — tier-1 variant-carries-field.
            dispatch_auth_sasl_final(state, reply, expected_server_sig, payload)
        }
        (ProtoState::ConnectingScramAwaitingServerFinal { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, error_arena).into_protocol_error();
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
            let cause = parse_error_response(payload, error_arena).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingScramAwaitingAuthOk(reply), other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // Post-auth: waiting for BackendKeyData
        //
        // `ParameterStatus` (tag 'S') is filtered pre-dispatch in
        // `feed_bytes` via `allows_unsolicited_param_status`; the
        // dispatcher never sees it for these states. DEF-054.
        // =============================================================
        (ProtoState::ConnectingPostAuthAwaitingKey(reply), TAG_BACKEND_KEY_DATA) => {
            match parse_backend_key_data(payload) {
                Ok((pid, secret_key)) => {
                    *state = ProtoState::ConnectingPostAuthHaveKey {
                        reply,
                        pid,
                        secret_key,
                    };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::ConnectingPostAuthAwaitingKey(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, error_arena).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingPostAuthAwaitingKey(reply), other) => install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // Post-auth: have BackendKeyData, waiting for ReadyForQuery
        //
        // `ParameterStatus` (tag 'S') is filtered pre-dispatch in
        // `feed_bytes` via `allows_unsolicited_param_status`; the
        // dispatcher never sees it for these states. DEF-054.
        // =============================================================
        (
            ProtoState::ConnectingPostAuthHaveKey {
                reply,
                pid,
                secret_key,
            },
            TAG_READY_FOR_QUERY,
        ) => {
            // DEF-112: `reply: ReplyId<StartupKind>` — typed
            // `deliver` forces a `StartupCompletePayload` payload.
            match parse_rfq_payload(payload) {
                Ok(tx_status) => {
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            reply,
                            crate::action::StartupCompletePayload {
                                pid,
                                secret_key,
                                tx_status,
                            },
                        ),
                    }
                }
                Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
            }
        }
        (ProtoState::ConnectingPostAuthHaveKey { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload, error_arena).into_protocol_error();
            install_errored(state, Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingPostAuthHaveKey { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Phase 1c-1b: Simple Query flow
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
            // F19 + DEF-188: parsed schema lands inline in the
            // StreamingRows variant. No arena, no handle.
            match crate::decode::parse_row_description(payload) {
                Ok(row_desc) => {
                    *state = ProtoState::SimpleQueryStreamingRows { reply, row_desc };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_COMMAND_COMPLETE) => {
            // DML path: no RowDescription → AwaitingRfq with row_desc=None.
            advance_to_awaiting_rfq(state, reply, payload, None)
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_EMPTY_QUERY_RESPONSE) => {
            // DEF-185 P0-F (audit 2026-04-24): PG §55.7 specifies
            // EmptyQueryResponse has a zero-byte body. Enforce tier-2
            // structural — non-empty body classifies as
            // UnexpectedFrameBody. Pre-fix: payload was ignored entirely.
            match payload {
                [] => {
                    *state = ProtoState::SimpleQueryAwaitingRfq {
                        reply,
                        command_tag: crate::error::BoundedStr::default(),
                        row_desc: None,
                    };
                    DispatchOutcome::AdvancedSilent
                }
                other => install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_EMPTY_QUERY_RESPONSE,
                        payload_len: other.len(),
                    },
                ),
            }
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, error_arena)
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), other) => {
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
        (ProtoState::SimpleQueryStreamingRows { reply, row_desc }, TAG_COMMAND_COMPLETE) => {
            // SELECT path terminates: preserve schema into AwaitingRfq
            // so the trailing RFQ's QueryComplete can park it into
            // terminal_row_desc.
            advance_to_awaiting_rfq(state, reply, payload, Some(row_desc))
        }
        (ProtoState::SimpleQueryStreamingRows { reply, .. }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, error_arena)
        }
        (ProtoState::SimpleQueryStreamingRows { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // AwaitingRfq: Z is the only legal frame
        (ProtoState::SimpleQueryAwaitingRfq { reply, command_tag, row_desc }, TAG_READY_FOR_QUERY) => {
            // DEF-188: park the inline RowDesc (if any) into
            // PgProtocol::terminal_row_desc so the public
            // QueryComplete reply's Option<&'r RowDesc> borrow
            // finds a stable address that outlives the state's
            // transition to Idle. The slot persists until the next
            // entry-point's clear (mirrors pre-188 arena clear
            // discipline, simpler — single slot, no generation).
            match parse_rfq_payload(payload) {
                Ok(tx_status) => {
                    *state = ProtoState::Idle;
                    let schema_present = row_desc.is_some();
                    *terminal_row_desc = row_desc;
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            reply,
                            crate::action::StagedQueryCompletePayload {
                                command_tag,
                                tx_status,
                                schema_present,
                            },
                        ),
                    }
                }
                Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
            }
        }
        (ProtoState::SimpleQueryAwaitingRfq { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // DrainRfqAfterError: consume Z → Idle, with full tx_status validation.
        //
        // DEF-185 P1-G (audit 2026-04-24): delegate to parse_rfq_payload
        // for uniform validation. Pre-fix: accepted any 1-byte payload
        // via `[_]` slice-pattern without validating the byte is one of
        // `{I, T, E}`. Every OTHER RFQ arm (`PingAwaitingRfq`,
        // `ParseAwaitingRfq`, `SimpleQueryAwaitingRfq`, etc.) used
        // `parse_rfq_payload`; this one was an asymmetry — `drained`
        // still succeeded but a malformed byte got silently accepted.
        // Semantically low-stakes (we reach Idle either way), but tier-4
        // uniformity drift nonetheless. Post-fix: consistent classification.
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
        // Phase 1c-3a: Extended Query — Parse flow
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
            // DEF-185 P0-F: ParseComplete body must be empty per
            // PG §55.7.
            match payload {
                [] => {
                    *state = ProtoState::ParseAwaitingRfq(reply);
                    DispatchOutcome::AdvancedSilent
                }
                other => install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_PARSE_COMPLETE,
                        payload_len: other.len(),
                    },
                ),
            }
        }
        (ProtoState::ParseAwaitingParseComplete(reply), TAG_ERROR_RESPONSE) => {
            // Recoverable parse error — PG spec sends Z after E, so
            // drain it silently and return to Idle (reusing the
            // `DrainRfqAfterError` variant — both drain
            // the same trailing RFQ pattern).
            let cause = parse_error_response(payload, error_arena).into_protocol_error();
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
                    *state = ProtoState::Idle;
                    DispatchOutcome::AdvancedWithAction {
                        action: crate::action::deliver(
                            reply,
                            crate::action::ParseCompletePayload { tx_status },
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
        // Phase 1c-3b: Extended Query — Bind + Execute flow
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
        //   's' (PortalSuspended) → UnexpectedFrame (1c-3b scope;
        //                           1c-6 lifts for chunked fetch)
        //
        // Tier uplift vs pre-split: the "can we stream rows?"
        // decision is resolved at the VARIANT level (tier-1 structural
        // dispatch) instead of via a runtime `match row_desc: Option<_>`
        // at the 'D' arm.
        // =============================================================

        // ─── DML path ───

        (ProtoState::BindExecuteAwaitingBindCompleteDml(reply), TAG_BIND_COMPLETE) => {
            // DEF-185 P0-F: BindComplete body must be empty per
            // PG §55.7.
            match payload {
                [] => {
                    *state = ProtoState::BindExecuteAwaitingCommandCompleteDml(reply);
                    DispatchOutcome::AdvancedSilent
                }
                other => install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_BIND_COMPLETE,
                        payload_len: other.len(),
                    },
                ),
            }
        }
        (ProtoState::BindExecuteAwaitingBindCompleteDml(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, error_arena)
        }
        (ProtoState::BindExecuteAwaitingBindCompleteDml(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        (ProtoState::BindExecuteAwaitingCommandCompleteDml(reply), TAG_COMMAND_COMPLETE) => {
            match parse_command_tag(payload) {
                Ok(command_tag) => {
                    *state = ProtoState::BindExecuteAwaitingRfqDml { reply, command_tag };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::BindExecuteAwaitingCommandCompleteDml(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, error_arena)
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
            ProtoState::BindExecuteAwaitingRfqDml { reply, command_tag },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                // DML path: no schema to park (terminal_row_desc
                // stays at its prior cleared state). schema_present
                // = false — public QueryComplete.row_desc = None.
                *state = ProtoState::Idle;
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedQueryCompletePayload {
                            command_tag,
                            tx_status,
                            schema_present: false,
                        },
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
            ProtoState::BindExecuteAwaitingBindCompleteSelect { reply, row_desc },
            TAG_BIND_COMPLETE,
        ) => {
            // DEF-185 P0-F: BindComplete body must be empty per
            // PG §55.7.
            match payload {
                [] => {
                    *state =
                        ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, row_desc };
                    DispatchOutcome::AdvancedSilent
                }
                other => install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_BIND_COMPLETE,
                        payload_len: other.len(),
                    },
                ),
            }
        }
        (ProtoState::BindExecuteAwaitingBindCompleteSelect { reply, .. }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, error_arena)
        }
        (ProtoState::BindExecuteAwaitingBindCompleteSelect { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // BindExecuteAwaitingDataOrCompleteSelect: DataRow via
        // `feed_bytes` classifies as UnexpectedFrame in the
        // catch-all arm below (see SimpleQueryStreamingRows for
        // the full rationale).
        (
            ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, row_desc },
            TAG_COMMAND_COMPLETE,
        ) => advance_to_bindexecute_awaiting_rfq_select(state, reply, payload, row_desc),
        (ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, .. }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, error_arena)
        }
        (
            ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, .. },
            TAG_PORTAL_SUSPENDED,
        ) => install_errored(state, 
            Some(reply.consume()),
            ProtocolError::UnexpectedFrame { tag: TAG_PORTAL_SUSPENDED },
        ),
        (ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // BindExecuteStreamingRows: DataRow via `feed_bytes`
        // classifies as UnexpectedFrame in the catch-all arm
        // below (see SimpleQueryStreamingRows for the full
        // rationale).
        (ProtoState::BindExecuteStreamingRows { reply, row_desc }, TAG_COMMAND_COMPLETE) => {
            advance_to_bindexecute_awaiting_rfq_select(state, reply, payload, row_desc)
        }
        (ProtoState::BindExecuteStreamingRows { reply, .. }, TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, error_arena)
        }
        (ProtoState::BindExecuteStreamingRows { reply, .. }, TAG_PORTAL_SUSPENDED) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_PORTAL_SUSPENDED })
        }
        (ProtoState::BindExecuteStreamingRows { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        (
            ProtoState::BindExecuteAwaitingRfqSelect { reply, command_tag, row_desc },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                // DEF-188: park the SELECT path's inline RowDesc
                // into terminal_row_desc so the public
                // QueryComplete reply's borrow is valid through
                // materialise (state goes to Idle here, but slot
                // persists until next entry-point clear).
                *state = ProtoState::Idle;
                *terminal_row_desc = Some(row_desc);
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedQueryCompletePayload {
                            command_tag,
                            tx_status,
                            schema_present: true,
                        },
                    ),
                }
            }
            Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
        },
        (ProtoState::BindExecuteAwaitingRfqSelect { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Phase 1c-3c: Extended Query — Describe flow
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
        (ProtoState::DescribeStatementAwaitingParamDesc(reply), TAG_PARAMETER_DESCRIPTION) => {
            match crate::decode::parse_parameter_description(payload) {
                Ok(param_oids) => {
                    *state = ProtoState::DescribeStatementAwaitingRowDescOrNoData {
                        reply,
                        param_oids,
                    };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::DescribeStatementAwaitingParamDesc(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, error_arena)
        }
        (ProtoState::DescribeStatementAwaitingParamDesc(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // Stage 2: awaiting RowDescription or NoData.
        (
            ProtoState::DescribeStatementAwaitingRowDescOrNoData { reply, param_oids },
            TAG_ROW_DESCRIPTION,
        ) => match crate::decode::parse_row_description(payload) {
            Ok(row_desc) => {
                // DEF-188: parsed schema lands inline in the
                // DescribedRowsStaged::Rows variant (no arena).
                *state = ProtoState::DescribeStatementAwaitingRfq {
                    reply,
                    param_oids,
                    rows: crate::state::DescribedRowsStaged::Rows(row_desc),
                };
                DispatchOutcome::AdvancedSilent
            }
            Err(cause) => install_errored(state, Some(reply.consume()), cause),
        },
        (
            ProtoState::DescribeStatementAwaitingRowDescOrNoData { reply, param_oids },
            TAG_NO_DATA,
        ) => {
            // DEF-185 P0-F: NoData body must be empty per PG §55.7.
            match payload {
                [] => {
                    *state = ProtoState::DescribeStatementAwaitingRfq {
                        reply,
                        param_oids,
                        rows: crate::state::DescribedRowsStaged::NoData,
                    };
                    DispatchOutcome::AdvancedSilent
                }
                other => install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_NO_DATA,
                        payload_len: other.len(),
                    },
                ),
            }
        }
        (
            ProtoState::DescribeStatementAwaitingRowDescOrNoData { reply, .. },
            TAG_ERROR_RESPONSE,
        ) => advance_to_drain_after_error(state, reply.consume(), payload, error_arena),
        (ProtoState::DescribeStatementAwaitingRowDescOrNoData { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // Stage 3: awaiting ReadyForQuery — deliver the terminal
        // reply carrying the accumulated param_oids + rows.
        (
            ProtoState::DescribeStatementAwaitingRfq {
                reply,
                param_oids,
                rows,
            },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                // DEF-188: park inline RowDesc (if any) into
                // terminal_row_desc and stage the slim discriminator.
                let staged_rows = stage_described_rows(rows, terminal_row_desc);
                *state = ProtoState::Idle;
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedDescribeStatementCompletePayload {
                            param_oids,
                            rows: staged_rows,
                            tx_status,
                        },
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
            // DEF-188: parsed schema lands inline in DescribedRowsStaged::Rows.
            match crate::decode::parse_row_description(payload) {
                Ok(row_desc) => {
                    *state = ProtoState::DescribePortalAwaitingRfq {
                        reply,
                        rows: crate::state::DescribedRowsStaged::Rows(row_desc),
                    };
                    DispatchOutcome::AdvancedSilent
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        (ProtoState::DescribePortalAwaitingRowDescOrNoData(reply), TAG_NO_DATA) => {
            // DEF-185 P0-F: NoData body must be empty per PG §55.7.
            match payload {
                [] => {
                    *state = ProtoState::DescribePortalAwaitingRfq {
                        reply,
                        rows: crate::state::DescribedRowsStaged::NoData,
                    };
                    DispatchOutcome::AdvancedSilent
                }
                other => install_errored(
                    state,
                    Some(reply.consume()),
                    ProtocolError::UnexpectedFrameBody {
                        tag: TAG_NO_DATA,
                        payload_len: other.len(),
                    },
                ),
            }
        }
        (ProtoState::DescribePortalAwaitingRowDescOrNoData(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(state, reply.consume(), payload, error_arena)
        }
        (ProtoState::DescribePortalAwaitingRowDescOrNoData(reply), other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // Stage 2: awaiting ReadyForQuery — deliver portal reply.
        (
            ProtoState::DescribePortalAwaitingRfq { reply, rows },
            TAG_READY_FOR_QUERY,
        ) => match parse_rfq_payload(payload) {
            Ok(tx_status) => {
                let staged_rows = stage_described_rows(rows, terminal_row_desc);
                *state = ProtoState::Idle;
                DispatchOutcome::AdvancedWithAction {
                    action: crate::action::deliver(
                        reply,
                        crate::action::StagedDescribePortalCompletePayload {
                            rows: staged_rows,
                            tx_status,
                        },
                    ),
                }
            }
            Err(payload_len) => install_errored(state, Some(reply.consume()), ProtocolError::MalformedReadyForQuery { payload_len }),
        },
        (ProtoState::DescribePortalAwaitingRfq { reply, .. }, other) => {
            install_errored(state, Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Idle — unsolicited frames are out-of-spec
        // =============================================================
        (ProtoState::Idle, other) => install_errored(state, None, ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // Errored — terminal sink (Phase 1a pattern carried forward)
        //
        // DEF-163 A012: architecturally dead under current flow.
        // `feed_bytes` short-circuits on `ProtoState::Errored(_)`
        // via the is-errored-or-recovering fast-path check (see
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

    // DEF-184 (A10/B22 revert 2026-04-24): no scram_state cleanup needed.
    // `mem::replace(state, ProtoState::Idle)` above consumed the SCRAM
    // variant by value; if the arm ended in Errored the variant is
    // already dropped — `ScramSession::Drop` impl (`ZeroizeOnDrop`)
    // scrubbed password bytes inside the match. Variant-carries-field
    // invariant (CREDO §1) makes this automatic: there is no separate
    // slot that could linger past the state transition.
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
            // F-046 (pass-#8): `try_from_u32` returns `Result<Self, u32>`
            // (not `Option<Self>`) — forward the rejected raw u32 via
            // `map_err`, no separate `.ok_or(..raw)` layer needed.
            let code = crate::wire::AuthSubCode::try_from_u32(raw).map_err(|unknown| {
                // DEF-184 (B9): `unknown ≠ 0` structurally — AUTH_OK
                // = 0 is matched to Ok(AuthSubCode::Ok) by
                // try_from_u32 above, so `Err(0)` is architecturally
                // impossible. `AuthSubCodeClass::Unknown(NonZeroU32)`
                // carries the tier-1 type-level proof; dead None
                // arm classified as AuthSubCodeZeroInErr crate-bug.
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
/// [`ProtoState::ConnectingStartupTrust`]. DEF-097.
///
/// Only `AUTH_OK` is acceptable here: the user provided no password,
/// so a SCRAM challenge from the server is classified as
/// `UnsupportedAuthMethod` (the server's pg_hba.conf disagrees with
/// the client's no-password configuration). The dispatcher match
/// cannot reach this arm with SCRAM payloads already buffered,
/// because the Scram variant of the state has its own handler —
/// this separation is the tier-1 compile guarantee DEF-097 buys.
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
        // `Sasl` / `SaslContinue` / `SaslFinal`: a Trust connection
        // never requested SCRAM, so any SASL message means the server
        // expects an auth method we are not configured for.
        // Tier-1 exhaustive — a future new `AuthSubCode` variant
        // forces this match to be updated.
        other @ (crate::wire::AuthSubCode::Sasl
            | crate::wire::AuthSubCode::SaslContinue
            | crate::wire::AuthSubCode::SaslFinal) => install_errored(state,
            Some(reply.consume()),
            ProtocolError::UnsupportedAuthMethod { sub_code: crate::error::AuthSubCodeClass::KnownButWrong(other) },
        ),
    }
}

/// Dispatch an Authentication message while in
/// [`ProtoState::ConnectingStartupScram`]. DEF-097.
///
/// Only `AUTH_SASL` is acceptable here; the server taking `AUTH_OK`
/// without asking for the password is an auth-method mismatch on
/// the server side (client expected SCRAM).
fn dispatch_auth_in_startup_scram(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    scram: alloc::boxed::Box<crate::scram::session::ScramSession>,
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
                return install_errored(state, Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::NoSupportedMechanism));
            }

            // Build client-first-message and SASLInitialResponse.
            // DEF-094: write directly into the caller-owned `write_buf`
            // and record the range; materialise at the entry-point
            // boundary after the mutable write phase releases.
            match build_sasl_initial_response(&scram, reserved) {
                Ok((range, client_first_bare, client_nonce_b64)) => {
                    // DEF-184 (A10/B22 revert) + DEF-187 box transition:
                    // tier-1 variant-carries-field; SCRAM bytes heap-
                    // boxed for 24 B variant footprint vs 720 B inline.
                    *state = ProtoState::ConnectingScramAwaitingServerFirst {
                        reply,
                        scram,
                        client_first_bare: alloc::boxed::Box::new(client_first_bare),
                        client_nonce_b64: alloc::boxed::Box::new(client_nonce_b64),
                    };
                    DispatchOutcome::AdvancedWithAction {
                        action: StagedAction::SendBytesRange(range),
                    }
                }
                Err(cause) => install_errored(state, Some(reply.consume()), cause),
            }
        }
        other @ (crate::wire::AuthSubCode::Ok
            | crate::wire::AuthSubCode::SaslContinue
            | crate::wire::AuthSubCode::SaslFinal) => install_errored(state,
            Some(reply.consume()),
            ProtocolError::UnsupportedAuthMethod { sub_code: crate::error::AuthSubCodeClass::KnownButWrong(other) },
        ),
    }
}

/// Check if the SASL mechanism list contains SCRAM-SHA-256.
///
/// DEF-184 (B12): happy-path fast-check — most servers announce
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
/// # Audit A2 + DEF-097
///
/// The `Credentials`-vs-`ScramPassword` split happens exactly once at
/// `ScramSession::from_password`; this function cannot be reached
/// from a Trust-credentials push path because the state variant it
/// destructures from (`ConnectingStartupScram { scram, .. }`) carries
/// a `ScramSession`, not a `Credentials`.
///
/// [`ScramSession`]: crate::scram::session::ScramSession
fn build_sasl_initial_response(
    _: &crate::scram::session::ScramSession,
    reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
) -> Result<
    (
        // DEF-154 (B): typed branded range — brand ties to `reserved`
        // scope. Apply at materialise time is infallible.
        crate::action::WriteRange,
        crate::ident::PodBytes<{ crate::scram::wire::MAX_CLIENT_FIRST_BARE_LEN }>,
        crate::ident::PodBytes<{ crate::scram::wire::MAX_CLIENT_NONCE_B64_LEN }>,
    ),
    ProtocolError,
> {
    use crate::scram::wire;

    // DEF-185 P2-F (audit 2026-04-24): PG convention — the SCRAM
    // `n=<user>` field is hard-coded empty because the real user
    // name travelled in the StartupMessage's `user` parameter. PG's
    // SCRAM implementation explicitly ignores the SASL-level user
    // (see PG src/backend/libpq/auth-scram.c — the `user` from
    // client-first is never consulted; authentication is bound to
    // the startup user).
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

    let client_nonce_vec = wire::generate_client_nonce().map_err(ProtocolError::Scram)?;

    let client_first_bare_vec =
        wire::build_client_first_bare(user_bytes, &client_nonce_vec).map_err(ProtocolError::Scram)?;

    let client_first_msg =
        wire::build_client_first_message(user_bytes, &client_nonce_vec).map_err(ProtocolError::Scram)?;

    // DEF-154 (B) escape hatch: SCRAM auth is a cold handshake path
    // that predates the branded push_* helpers. The scram::wire
    // builders hand back owned heapless::Vec; we push those bytes
    // into the branded reserved via `as_write_buf_mut()`. The brand
    // is preserved by the enclosing `reserved: &mut BrandedWriteReserved<'wb>` —
    // `as_write_buf_mut` returns &mut WriteBuf without a brand, and
    // the pushed range is wrapped via `WriteRange::from_write_span`
    // below against the same reserved.
    let start = reserved.len();
    let buf = reserved.as_write_buf_mut();
    buf.push_u8(crate::wire::TAG_SASL_RESPONSE.byte())
        .map_err(|_| ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;
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
    .map_err(|_| ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;

    let client_first_bare = crate::ident::PodBytes::try_from_slice(&client_first_bare_vec)
        .map_err(|_| ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;
    let client_nonce_b64 = crate::ident::PodBytes::try_from_slice(&client_nonce_vec)
        .map_err(|_| ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;
    // DEF-154 (B) P0-2: `from_branded_write_span` returns Result;
    // `?` propagates up through the function's own Result return
    // type. Err here classifies as `EmptyWriteRange` — dead under
    // intact SCRAM invariants.
    let range = crate::action::WriteRange::from_write_span(start, reserved)?;
    Ok((range, client_first_bare, client_nonce_b64))
}

/// Dispatch AuthenticationSASLContinue (server-first-message).
///
/// Takes a [`ScramSession`] by value — the `Trust`-vs-`ScramPassword`
/// discrimination was consumed at
/// [`ScramSession::try_from_credentials`] in the parent dispatch
/// call; this function cannot be reached with `Trust` credentials
/// because the state variant it destructures from
/// ([`ProtoState::ConnectingScramAwaitingServerFirst`]) carries
/// `ScramSession`, not `Credentials`. Audit A2.
///
/// [`ScramSession`]: crate::scram::session::ScramSession
/// [`ScramSession::try_from_credentials`]: crate::scram::session::ScramSession::try_from_credentials
// DEF-184 (A10/B22 revert 2026-04-24): 7-arg function — within
// clippy::too_many_arguments default threshold. No `#[expect]` needed.
//
// DEF-187 (architect 2026-04-26): SCRAM data is heap-boxed inside
// the variant for variant-size compaction. The dispatch function takes
// boxed values (scram still as Box because ScramSession is large
// 520 B; passing by value would memcpy onto stack frame, same cost
// either way but Box keeps the type explicit). client_first_bare /
// client_nonce_b64 take PodBytes by value (caller derefs the Box at
// the variant destructure). Clippy::boxed_local doesn't fire on the
// owned-Box for ScramSession since the Box is consumed (passed into
// password_bytes() ref methods).
fn dispatch_auth_sasl_continue(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::StartupKind>,
    scram: alloc::boxed::Box<crate::scram::session::ScramSession>,
    client_first_bare: crate::ident::PodBytes<{ crate::scram::wire::MAX_CLIENT_FIRST_BARE_LEN }>,
    client_nonce_b64: crate::ident::PodBytes<{ crate::scram::wire::MAX_CLIENT_NONCE_B64_LEN }>,
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
        match crate::scram::wire::parse_server_first(rest, client_nonce_b64.as_slice()) {
            Ok(sf) => sf,
            Err(e) => {
                return install_errored(state, Some(reply.consume()), ProtocolError::Scram(e));
            }
        };

    // Password bytes come from the typed SCRAM session — no
    // Trust-vs-ScramPassword discrimination here (audit A2).
    let password_bytes = scram.password_bytes();

    // Build client-final-without-proof.
    let client_final_without_proof =
        match crate::scram::wire::build_client_final_without_proof(
            server_first.server_nonce.as_bytes(),
        ) {
            Ok(v) => v,
            Err(e) => {
                return install_errored(state, Some(reply.consume()), ProtocolError::Scram(e));
            }
        };

    // Compute proof and expected server signature.
    //
    // AuthMessage = client-first-bare + "," + server-first + "," +
    // client-final-without-proof. The three components are passed
    // separately — compute_client_proof feeds them incrementally into
    // HMAC::update(), with zero intermediate buffer. No staging
    // buffer → no silent-truncation class → tier-1 by construction.
    // F54: `compute_client_proof` returns Result on architecturally-
    // dead `HmacKeyRejected` path. On Err (supply-chain compromise of
    // RustCrypto's HMAC, etc.), tear down the handshake with a typed
    // diagnostic — don't continue with zero-filled bytes. Fail-closed.
    let (proof, expected_server_sig) = match crate::scram::crypto::compute_client_proof(
        password_bytes,
        &server_first.salt,
        server_first.iterations,
        client_first_bare.as_slice(),
        rest,
        &client_final_without_proof,
    ) {
        Ok(v) => v,
        Err(e) => return install_errored(state, Some(reply.consume()), ProtocolError::Scram(e)),
    };

    // Base64-encode proof.
    //
    // DEF-185 P1-A (audit 2026-04-24): stack buffer holds base64
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
                return install_errored(state, Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))
            }
        };
    let proof_b64 = match proof_b64_buf.get(..proof_b64_len) {
        Some(s) => s,
        None => {
            return install_errored(state, Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))
        }
    };

    // Build client-final-message.
    //
    // DEF-185 P1-A (audit 2026-04-24): `client_final_msg` contains the
    // embedded `p=<proof_b64>` payload — password-correlated via the
    // same StoredKey algebra as proof_b64_buf. The `heapless::Vec` it
    // lives in does NOT implement `Zeroize` (upstream crate), so we
    // cannot wrap in `Zeroizing`. Instead, after the value has been
    // copied into the write buffer (push_bytes call below), zeroize
    // the heapless::Vec's backing bytes in-place via
    // `Zeroize::zeroize()` on the mut slice (slice impl exists
    // upstream). Done just before the Vec drops at function scope end.
    let mut client_final_msg = match crate::scram::wire::build_client_final_message(
        server_first.server_nonce.as_bytes(),
        proof_b64,
    ) {
        Ok(v) => v,
        Err(e) => {
            return install_errored(state, Some(reply.consume()), ProtocolError::Scram(e));
        }
    };

    // DEF-154 (B): build SASLResponse frame via the branded reserved.
    // Escape hatch as_write_buf_mut() for the pre-DEF-154 (B)
    // Result-returning push path; WriteRange wraps the span
    // at the end via `from_branded_write_span`.
    let start = reserved.len();
    {
        let buf = reserved.as_write_buf_mut();
        if buf.push_u8(crate::wire::TAG_SASL_RESPONSE.byte()).is_err()
            || buf
                .with_length_prefix(|w| w.push_bytes(&client_final_msg))
                .is_err()
        {
            // DEF-185 P1-A: scrub client_final_msg before early-return
            // classification. The buffer's bytes may contain partial
            // `p=<proof_b64>` payload depending on when the push failed.
            use zeroize::Zeroize;
            client_final_msg.as_mut_slice().zeroize();
            return install_errored(state, Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow));
        }
    }
    // DEF-185 P1-A: scrub the password-correlated client_final_msg
    // contents now that it has been copied into the write buffer.
    // The write buffer itself is zeroed on `WriteBuf::clear()` per
    // P0-B separately. Without this step the 384-byte heapless::Vec
    // backing array would hold `p=<proof_b64>` until this function's
    // stack frame gets overwritten by a subsequent call.
    use zeroize::Zeroize;
    client_final_msg.as_mut_slice().zeroize();
    // DEF-154 (B) P0-2: `from_branded_write_span` returns Result.
    // Err is architecturally dead here — the SASL_RESPONSE frame
    // body always has the 1-byte tag + 4-byte length prefix + the
    // client-final-message which is non-empty by SCRAM protocol.
    // Classified as `EmptyWriteRange` if triggered.
    let range = match crate::action::WriteRange::from_write_span(start, reserved) {
        Ok(r) => r,
        Err(cause) => return install_errored(state, Some(reply.consume()), cause),
    };

    // DEF-184 (A10/B22 revert 2026-04-24): `expected_server_sig` moves
    // INTO the variant — tier-1 compile (CREDO §1: variant-carries-field).
    // The `scram` ScramSession consumed here (moved into this function)
    // is NOT needed by the next state; its drop here fires
    // `ZeroizeOnDrop` — password material scrubbed exactly when the
    // handshake no longer needs it.
    *state = ProtoState::ConnectingScramAwaitingServerFinal {
        reply,
        expected_server_sig,
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
    let received_sig = match crate::scram::wire::parse_server_final(rest) {
        Ok(sig) => sig,
        Err(e) => {
            return install_errored(state, Some(reply.consume()), ProtocolError::Scram(e));
        }
    };

    // Constant-time comparison (DEF-039).
    if !bool::from(expected_server_sig.ct_eq(&received_sig)) {
        return install_errored(state, Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::SignatureMismatch));
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
// Helper: parse ErrorResponse typed fields
// -----------------------------------------------------------------

/// DEF-184 (audit #3 A-11): dedicated parser return type.
///
/// Pre-audit, [`parse_error_response`] returned `ProtocolError` —
/// coupling parser output to the public error shape. The three
/// fields of `ServerErrorResponse` were packed/unpacked twice (once
/// at the `ProtocolError::ServerErrorResponse { .. }` construction
/// in the parser, once at the `let ProtocolError::ServerErrorResponse
/// { .. } = err else { ... }` deconstruction in `parse_and_resolve`
/// and any future introspection site). A variant-rename refactor on
/// `ProtocolError` would ripple into the parser body and every
/// caller unless the parser outputs a shape-stable struct.
///
/// Post-audit: parser returns `ParsedServerError` (struct with
/// public fields); callers wanting the public `ProtocolError` use
/// [`Self::into_protocol_error`]. The struct is crate-private —
/// exposing it on the public API would create a second shape for
/// the same data.
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
    pub(crate) severity: crate::error::Severity,
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
/// PG ErrorResponse body: series of typed fields, each = type-byte +
/// NUL-terminated string. Terminated by a bare NUL (0x00). We extract
/// 'S' (severity), 'C' (code), 'M' (message), 'D' (detail), 'H' (hint).
///
/// Cold path: called only when the server emits an `ErrorResponse`
/// frame (`'E'` tag). The `#[cold]` attribute tells LLVM to keep the
/// body out of hot-path inlining scope.
///
/// DEF-184 (audit #3 A-11): returns [`ParsedServerError`] struct
/// instead of `ProtocolError`. Callers that want the public error
/// shape use `.into_protocol_error()`; callers that want to inspect
/// the parsed fields (e.g. test helper, future diagnostic layers)
/// access the struct fields directly.
#[cold]
fn parse_error_response(
    payload: &[u8],
    error_arena: &mut crate::error_arena::ErrorArena,
) -> ParsedServerError {
    use crate::error::{BoundedStr, Severity, SqlStateCode};
    // DEF-060 part 2: typed fields. Severity → enum (1 byte);
    // code → SqlStateCode ([u8;5]); message/detail/hint →
    // BoundedStr<N> with explicit truncation marker (no
    // `.unwrap_or_default()` silent-truncation).
    //
    // Architect audit #3 (2026-04-21): `severity_set: bool` +
    // `severity = Severity::Unknown` pair collapsed into
    // `Option<Severity>`. Tier-3 audit (the bool flip had to stay
    // in sync with the enum assignment) → tier-1 compile (the
    // `Option` discriminator and the `Some(Severity)` payload are
    // the same value; impossible to desync). Niche-packed:
    // `Severity::Unknown = 0` as `#[repr(u8)]` means `Option<Severity>`
    // stays 1 byte (same as the prior `Severity` alone).
    let mut severity: Option<Severity> = None;
    let mut code = SqlStateCode::from_bytes(b"");
    let mut message: BoundedStr<128> = BoundedStr::default();
    let mut detail: BoundedStr<96> = BoundedStr::default();
    let mut hint: BoundedStr<64> = BoundedStr::default();

    // DEF-064: bounded-iteration DoS shield. PG's documented
    // ErrorResponse field set has ~18 tags total (S, V, C, M, D,
    // H, P, p, q, W, s, t, c, n, F, L, R, plus future). A
    // legitimate server sends each at most once. Cap at 32 — 2×
    // headroom for any future addition. Beyond the cap, we stop
    // parsing and use whatever fields we've already extracted.
    //
    // Without this cap the loop is still bounded by
    // `payload.len() ≤ MAX_FRAME_LEN_FIELD ≈ 4KB` (pos advances
    // monotonically, `payload.get(pos)` returns `None` at
    // end-of-payload), so a 4 KB pathological frame could
    // produce ~1300 tight iterations. The cap keeps the work
    // bounded to O(field_count) regardless of frame size. Tier-2
    // structural — the invariant is enforced by the `for _ in
    // 0..N` bound, not an audit of `pos` math.
    const MAX_ERROR_FIELDS: usize = 32;

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
        // DEF-184 (B4): `iter().position(|&b| b == 0)` on the slice
        // tail is LLVM-vectorisable (SIMD chunk-compare for u8
        // slices ≥ 8 bytes). Pre-(184) was a byte-by-byte `while
        // let Some(b) = payload.get(pos)` loop with per-iter
        // `checked_add(1)` — O(N) with one compare + one bounds-
        // check + one add per byte. Post-(184): single iterator
        // scan that LLVM lowers to SIMD for long fields (error
        // messages can be up to 128 bytes). ~3× on server-error
        // parsing hot path — fired on every ServerErrorResponse.
        let start = pos;
        let tail = payload.get(start..).unwrap_or(&[]);
        let value_bytes;
        match tail.iter().position(|&b| b == 0) {
            Some(n) => {
                value_bytes = tail.get(..n).unwrap_or(&[]);
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
                // as the value (DEF-064 forward-compat). Exit
                // loop next iter via `pos > payload.len()` peek.
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
            // servers; `from_bytes_lossy` preserves the ASCII subset
            // and visibly marks non-ASCII bytes with `?`. Previously
            // `from_utf8(..).unwrap_or("")` silently dropped the entire
            // field on any single invalid byte — tier-3 diagnostic loss.
            b'M' => {
                message = BoundedStr::from_bytes_lossy(value_bytes);
            }
            b'D' => {
                detail = BoundedStr::from_bytes_lossy(value_bytes);
            }
            b'H' => {
                hint = BoundedStr::from_bytes_lossy(value_bytes);
            }
            _ => {} // Unknown field type — skip.
        }
    }

    // DEF-184 (A1+A13): allocate the bounded strings into the
    // caller-supplied error arena; return the small ParsedServerError
    // (16 B) with the ErrorRef handle.
    let details_ref = error_arena.alloc(crate::error_arena::ErrorPayload {
        message,
        detail,
        hint,
    });
    ParsedServerError {
        // No S or V field in payload → `Severity::Unknown` fallback
        // (public API preserves the pre-uplift shape).
        severity: severity.unwrap_or(Severity::Unknown),
        code,
        details_ref,
    }
}

// -----------------------------------------------------------------
// Helper: parse BackendKeyData
// -----------------------------------------------------------------

/// 1c-1b helper: shared body for the `C` arm in both
/// `AwaitingFirstResponse` and `StreamingRows` states. Transition to
/// `AwaitingRfq { reply, command_tag }` on well-formed tag; classify
/// missing-NUL / framing error as `Errored`.
///
/// Centralises the "`CommandComplete` → AwaitingRfq" invariant in one
/// place — an arm-body edit in only one of the two call sites
/// would diverge silently; the helper makes the transition atomic.
fn advance_to_awaiting_rfq(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::QueryKind>,
    payload: &[u8],
    row_desc: Option<crate::decode::RowDesc>,
) -> DispatchOutcome {
    match parse_command_tag(payload) {
        Ok(command_tag) => {
            *state = ProtoState::SimpleQueryAwaitingRfq {
                reply,
                command_tag,
                row_desc,
            };
            DispatchOutcome::AdvancedSilent
        }
        Err(cause) => install_errored(state, Some(reply.consume()), cause),
    }
}

/// 1c-3b helper: `CommandComplete` on the schema-bearing (SELECT)
/// path → `BindExecuteAwaitingRfqSelect`. The schema (`RowDesc`) is
/// mandatory by the target variant's shape — caller pattern-matched
/// it from `AwaitingDataOrCompleteSelect` or `StreamingRows`.
///
/// The DML path's 'C' transition is inlined directly in the dispatch
/// arm (one call-site only) and doesn't need a helper.
fn advance_to_bindexecute_awaiting_rfq_select(
    state: &mut ProtoState,
    reply: ReplyId<crate::reply_id::QueryKind>,
    payload: &[u8],
    row_desc: crate::decode::RowDesc,
) -> DispatchOutcome {
    match parse_command_tag(payload) {
        Ok(command_tag) => {
            *state = ProtoState::BindExecuteAwaitingRfqSelect {
                reply,
                command_tag,
                row_desc,
            };
            DispatchOutcome::AdvancedSilent
        }
        Err(cause) => install_errored(state, Some(reply.consume()), cause),
    }
}

/// DEF-188: park the inline `RowDesc` from a state-side
/// [`crate::state::DescribedRowsStaged`] into the protocol's
/// `terminal_row_desc` slot, returning the action-side
/// [`crate::action::DescribedRowsStagedSlim`] discriminator.
///
/// Used by both DescribeStatement and DescribePortal Z arms — the
/// only caller-visible difference is the staged payload type
/// (`Staged{Statement,Portal}CompletePayload`), which both wrap
/// the same `DescribedRowsStagedSlim` shape.
///
/// # Why this helper
///
/// The state-side `Rows(RowDesc)` carries 264 B inline; the
/// action-side `Rows` is ZST (the schema lives in `terminal_row_desc`
/// post-park). Centralising the conversion + park in one helper
/// makes the "Rows + park atomic" invariant tier-2 structural —
/// a future arm-body edit cannot stage `Rows` without parking
/// without breaking compilation here. Both Z arms call this once;
/// no drift between them.
#[inline]
fn stage_described_rows(
    rows: crate::state::DescribedRowsStaged,
    terminal_row_desc: &mut Option<crate::decode::RowDesc>,
) -> crate::action::DescribedRowsStagedSlim {
    match rows {
        crate::state::DescribedRowsStaged::Rows(desc) => {
            *terminal_row_desc = Some(desc);
            crate::action::DescribedRowsStagedSlim::Rows
        }
        crate::state::DescribedRowsStaged::NoData => {
            crate::action::DescribedRowsStagedSlim::NoData
        }
    }
}

/// 1c-1b helper (pass-#7 audit, 2026-04-21): shared body for the
/// `E` arm across multiple flows. Emit `FailReply` (NO `CloseSocket`
/// — query-level errors are connection-survivable per PG §55.2.3)
/// and transition to `DrainRfqAfterError` so the trailing `Z`
/// returns the state to `Idle`.
///
/// # Signature rationale — pre-consume at call site (F1)
///
/// Pre-pass-#7 this was generic `<K: ReplyKind>` taking `ReplyId<K>`
/// by value, forcing monomorphisation once per kind. Since the body
/// only uses `reply.consume() -> NonZeroU64` (`K`-oblivious), every
/// call site emitted an identical 3-instruction basic block. After
/// pass-#7 the signature takes `raw_id: NonZeroU64` — the caller
/// pre-consumes the typed `ReplyId<K>`, exactly mirroring the pattern
/// `errored(Some(reply.consume()), …)` elsewhere in this module.
/// LLVM now emits one function body for all kinds.
///
/// # `#[cold] #[inline]` (F2)
///
/// Error drain is a cold branch — typical dispatch iterations
/// complete without encountering `ErrorResponse`. `#[cold]` pushes
/// this body out of the hot-match I-cache footprint; `#[inline]`
/// allows LLVM to fold the function into each call site when
/// register pressure permits. Same treatment as `errored()` above.
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
/// # F-048 (pass-#8) — narrow return type
///
/// Returns `Result<TxStatus, usize>` — Err carries the offending
/// `payload_len` as a bare `usize`. Callers wrap via
/// `.map_err(|payload_len| ProtocolError::MalformedReadyForQuery { payload_len })`.
///
/// Prior shape `Result<TxStatus, ProtocolError>` forced every dispatch
/// arm to reserve ~304 B of stack for the return slot (dominated by
/// ProtocolError::ServerErrorResponse). Narrowing to `Result<_, usize>`
/// shrinks the slot to 16 B (usize + discriminant + padding). 10+
/// dispatch call sites pay for this return.
///
/// # F-013 consolidation
///
/// Centralised pass-#7 audit F13 — prior to F13 there were 4+ parallel
/// `match payload { [b] => ..., other => ... }` patterns across every
/// `*AwaitingRfq` state. Single-point classifier closes drift between
/// handlers if a future change alters the `TxStatus` variant set.
#[cold]
#[inline]
fn parse_rfq_payload(
    payload: &[u8],
) -> Result<crate::action::TxStatus, usize> {
    match payload {
        // `[tx_byte]` pattern proves payload_len == 1 structurally.
        // F-009: `try_from_byte` returns `Result<Self, u8>`; we drop
        // the rejected byte and forward just the length-1 classification.
        // Diagnostic-wise the rejected byte is not currently surfaced
        // further upstream; if `MalformedReadyForQuery` gains a `byte`
        // field in the future, this map_err flips to pass it through.
        [tx_byte] => crate::action::TxStatus::try_from_byte(*tx_byte).map_err(|_| 1usize),
        other => Err(other.len()),
    }
}

/// Parse `CommandComplete` payload into a bounded command tag.
///
/// PG §55.7 CommandComplete body: a single NUL-terminated ASCII
/// string — e.g. `"SELECT 5\0"`, `"INSERT 0 3\0"`, `"UPDATE 7\0"`.
/// A missing NUL terminator is treated as a framing error
/// ([`ProtocolError::MalformedCommandComplete`]); the bytes-before-NUL
/// are truncating-fitted into a [`crate::error::BoundedStr<32>`].
///
/// Capacity 32 bytes handles PG's documented tag shapes with
/// headroom: the longest standard tag,
/// `"INSERT <oid:10-digit> <n:10-digit>\0"`, is ~23 bytes.
/// Overflow appends `"…"` rather than silently truncating (DEF-060
/// pattern — BoundedStr's `from_str_truncating`).
///
/// Cold path: called once per completed command. Not on any
/// per-row hot path.
#[cold]
fn parse_command_tag(payload: &[u8]) -> Result<crate::error::BoundedStr<32>, ProtocolError> {
    use crate::error::BoundedStr;
    // Strip the trailing NUL terminator. Missing NUL → framing error.
    let Some(body) = payload.strip_suffix(b"\0") else {
        return Err(ProtocolError::MalformedCommandComplete {
            payload_len: payload.len(),
        });
    };
    // DEF-185 P2-7 (audit 2026-04-24): embedded NUL validation.
    //
    // Pre-fix: PG CommandComplete body `SELECT\x00 5\x00` — strip_suffix
    // removed the trailing NUL, leaving `SELECT\x00 5` with embedded
    // NUL that `from_bytes_lossy` passed through verbatim (UTF-8
    // validator accepts NUL as a valid codepoint). User saw
    // `command_tag` with embedded NUL — weird but not exploitable.
    //
    // Post-fix: reject bodies containing embedded NUL as
    // `MalformedCommandComplete`. PG's CommandComplete is NUL-terminated
    // per §55.7 with the NUL strictly at the END; an interior NUL is a
    // wire violation.
    if body.contains(&0u8) {
        return Err(ProtocolError::MalformedCommandComplete {
            payload_len: payload.len(),
        });
    }
    // F-045 (pass-#8): use `from_bytes_lossy` — preserves ASCII subset,
    // coerces non-ASCII / invalid UTF-8 to `?`. Prior
    // `core::str::from_utf8(body).unwrap_or("")` silently dropped the
    // entire tag on any single invalid byte (tier-4 silent-pass). Now a
    // buggy proxy corrupting a tag byte leaves the rest readable and
    // marks corruption with `?` in the Debug/Display output.
    //
    // Mirrors F22's treatment of ErrorResponse fields (message / detail
    // / hint) — CommandComplete was missed in that pass; F-045 closes
    // the class.
    Ok(BoundedStr::from_bytes_lossy(body))
}

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

    /// DEF-184 (A1+A13): post-refactor test-fixture tuple —
    /// (Severity, SqlStateCode, ErrorPayload). Tests build expected
    /// tuples and compare against parsed actual via `parse_and_resolve`.
    type ExpectedErr = (
        crate::error::Severity,
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
        use crate::error::{BoundedStr, Severity, SqlStateCode};
        (
            Severity::from_bytes(severity.as_bytes()),
            SqlStateCode::from_bytes(code.as_bytes()),
            crate::error_arena::ErrorPayload {
                message: BoundedStr::<128>::from_str_truncating(message),
                detail: BoundedStr::<96>::from_str_truncating(detail),
                hint: BoundedStr::<64>::from_str_truncating(hint),
            },
        )
    }

    /// DEF-184 (A1+A13): parse body using a fresh arena, resolve the
    /// ErrorRef into full payload, and return the comparable tuple.
    ///
    /// DEF-184 (audit #3 A-03): pre-audit used
    /// `arena.get(details_ref).copied().unwrap_or_default()` — the
    /// silent-fallback pattern banned per user feedback. Post-audit
    /// the Result-returning `get()` is matched explicitly; the
    /// Err branch panics with a locus (architecturally unreachable
    /// by construction — `parse_error_response` always allocates
    /// into the fresh arena; the returned ref's generation matches;
    /// no intervening clear or realloc can fire here).
    ///
    /// DEF-184 (audit #3 A-11): pre-audit also had a defensive
    /// `else { return (...) }` fallback for the non-ServerErrorResponse
    /// branch, because the parser returned `ProtocolError`. Post-audit
    /// `parse_error_response` returns the dedicated `ParsedServerError`
    /// struct with unambiguous fields; the match-pattern + fallback is
    /// replaced with direct field access.
    fn parse_and_resolve(body: &[u8]) -> ExpectedErr {
        let mut arena = crate::error_arena::ErrorArena::new();
        let parsed = parse_error_response(body, &mut arena);
        let r = arena.get(parsed.details_ref);
        // Forbid-bundle compliance (mirror of error_arena::tests::must_alloc):
        // `assert!(is_ok) + .copied().unwrap_or(dead_for_test())` — assert
        // fires loudly if the invariant (parse always populates arena) breaks;
        // the dead_for_test fallback satisfies the no-panic bundle without
        // the tier-4 silent `unwrap_or_default` pattern banned per CREDO §5.
        assert!(
            r.is_ok(),
            "parse_error_response + arena.get Err {r:?} — architecturally unreachable \
             (parse always allocates into the fresh arena, no intervening clear)",
        );
        let payload = r.copied().unwrap_or(crate::error_arena::ErrorPayload::dead_for_test());
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

    /// F22 regression: non-UTF-8 bytes in M/D/H fields are coerced to
    /// `?` placeholders, preserving ASCII content, NOT silently
    /// collapsed to empty.
    ///
    /// Before F22: `from_utf8(value).unwrap_or("")` → whole field lost
    /// on any single invalid byte. A message like
    /// `b"Ung\xFCltige Eingabe"` (Latin-1 "Ungültige Eingabe") would
    /// become `""` — full diagnostic loss.
    ///
    /// After F22: ASCII subset preserved, non-ASCII byte → `?`.
    /// `b"Ung\xFCltige Eingabe"` → `"Ung?ltige Eingabe"` — user still
    /// sees 94% of the original message.
    #[test]
    fn non_utf8_message_preserves_ascii_subset() {
        // Latin-1 "Ungültige Eingabe" — the \xFC is ü in Latin-1,
        // invalid as a standalone UTF-8 byte.
        let body = build_error_body(&[(b'M', b"Ung\xFCltige Eingabe")]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "", "Ung?ltige Eingabe", "", "");
        assert_eq!(actual, expected);
    }

    /// F22 regression: valid UTF-8 multibyte sequences pass through
    /// unchanged (fast path).
    #[test]
    fn valid_utf8_message_preserved_verbatim() {
        // Proper UTF-8 "Ungültige Eingabe".
        let body = build_error_body(&[(b'M', "Ungültige Eingabe".as_bytes())]);
        let actual = parse_and_resolve(&body);
        let expected = mk_err("", "", "Ungültige Eingabe", "", "");
        assert_eq!(actual, expected);
    }

    /// F22 regression: slow-path control + high-bit bytes in an
    /// invalid-UTF-8 payload all coerce to `?`. Ensures that binary
    /// junk in a field doesn't produce an empty message.
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
