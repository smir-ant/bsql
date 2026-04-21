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

use crate::action::StagedAction;
use crate::error::ProtocolError;
use crate::reply_id::ReplyId;
use crate::state::ProtoState;
use crate::wire::{
    SCRAM_SHA_256_MECHANISM, TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_COMMAND_COMPLETE,
    TAG_DATA_ROW, TAG_EMPTY_QUERY_RESPONSE, TAG_ERROR_RESPONSE,
    TAG_NEGOTIATE_PROTOCOL_VERSION, TAG_PARSE_COMPLETE, TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};
use crate::write_buf::WriteBuf;

// ════════════════════════════════════════════════════════════════════
// Typed constructor arguments for `FrameCoords` — each offset has
// its own nominal type, so `FrameCoords::new` cannot receive swapped
// arguments at a call site. Tier-1 compile on "don't confuse frame
// start with total length with populated length".
// ════════════════════════════════════════════════════════════════════

/// Absolute position where a frame begins in
/// [`crate::buf::ReadBuf::populated`]. Equals the read cursor at the
/// moment `parse_header` consumed the frame's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct AbsFrameStart(pub usize);

/// Total wire bytes the frame occupies — tag (1) + length-prefix
/// (4) + body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct FrameTotalLen(pub usize);

/// Current `populated` length of the caller's `ReadBuf`. Serves as
/// the `bounds` argument for [`crate::action::NonEmptyRange::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct PopulatedLen(pub usize);

/// Absolute byte-coordinates of the frame being dispatched, resolved
/// against [`crate::buf::ReadBuf::populated`]. 1c-1b.
///
/// The dispatcher uses these to construct
/// [`StagedAction::StreamRowRange`] for `DataRow` frames — the
/// `row_range` must survive the post-dispatch `ReadBuf::advance` call,
/// which only moves the cursor (the bytes themselves stay in place
/// until the next `append` triggers lazy compaction).
///
/// # Tier-1 construction
///
/// Internal fields are private; the only constructor is
/// [`FrameCoords::new`] which takes three distinct newtypes
/// ([`AbsFrameStart`], [`FrameTotalLen`], [`PopulatedLen`]). Swapping
/// any two arguments at the call site is a compile error. `payload_start`
/// and `payload_end` are derived from `frame_start + HEADER_LEN` and
/// `frame_start + total_len` internally — no opportunity for a caller
/// to pass a wrong offset into either slot.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FrameCoords {
    frame_start: usize,
    total_len: usize,
    populated_len: usize,
}

impl FrameCoords {
    /// Typed constructor. Swap any two arguments → build error.
    #[inline]
    #[must_use]
    pub(crate) const fn new(
        frame_start: AbsFrameStart,
        total_len: FrameTotalLen,
        populated_len: PopulatedLen,
    ) -> Self {
        Self {
            frame_start: frame_start.0,
            total_len: total_len.0,
            populated_len: populated_len.0,
        }
    }

    /// Absolute offset where the frame payload begins — right after
    /// the 5-byte header. Derived from `frame_start + HEADER_LEN`
    /// (no direct field access, so no swap hazard with `payload_end`).
    #[inline]
    #[must_use]
    pub(crate) const fn payload_start(&self) -> usize {
        self.frame_start.saturating_add(crate::frame::HEADER_LEN)
    }

    /// Absolute offset one-past-last of the frame payload. Derived
    /// from `frame_start + total_len`.
    #[inline]
    #[must_use]
    pub(crate) const fn payload_end(&self) -> usize {
        self.frame_start.saturating_add(self.total_len)
    }

    /// Bounds argument for [`crate::action::NonEmptyRange::new`].
    #[inline]
    #[must_use]
    pub(crate) const fn populated_len(&self) -> usize {
        self.populated_len
    }
}

/// What to do after dispatching a single frame.
///
/// Three variants to keep the "emit zero actions" and "emit one
/// action" cases structurally distinct (audit round 2 A4). The
/// earlier two-variant form used `Advanced { action: Option<Action> }`
/// and an arm-body drift that flipped `Some(act)` into `None`
/// compiled silently — now such a drift is a compile error
/// (`AdvancedWithAction` requires the field, `AdvancedSilent` does not).
#[derive(Debug)]
#[expect(clippy::large_enum_variant, reason = "no_alloc: Box unavailable; DispatchOutcome is a one-shot return, not stored. FailReply.cause (ProtocolError ~280 bytes) dominates.")]
pub(crate) enum DispatchOutcome {
    /// Frame consumed; transition to `new_state`. No action emitted.
    /// Used by ParameterStatus, BackendKeyData, AuthenticationOk,
    /// SASLFinal — frames that advance state without user-visible
    /// side effects.
    AdvancedSilent {
        /// The state to transition to.
        new_state: ProtoState,
    },
    /// Frame consumed; transition to `new_state` **and** emit one
    /// staged action. DEF-094: `StagedAction` is range-based — the
    /// entry-point materialises into a ref-bound `Action<'buf>`
    /// after the write phase releases.
    AdvancedWithAction {
        /// The state to transition to.
        new_state: ProtoState,
        /// The single side-effect to push — range-based, ref-free.
        action: StagedAction,
    },
    /// Frame rejected; connection irrecoverable. Caller tears down.
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

/// DEF-103: `#[cold] #[inline]` helper centralising every
/// `DispatchOutcome::Errored` construction. The `#[cold]` marker
/// tells LLVM to push the Errored-path basic block out of the
/// hot-path I-cache footprint; `#[inline]` keeps the call-site
/// free of an actual function call (the helper body folds into
/// the caller). Net: every Errored site in this module shares
/// the cold-hint treatment through a single canonical function.
///
/// `reply_id` is `Option<NonZeroU64>` (already-consumed raw
/// value) per DEF-112's pre-consume convention.
#[cold]
#[inline]
fn errored(reply_id: Option<core::num::NonZeroU64>, cause: ProtocolError) -> DispatchOutcome {
    DispatchOutcome::Errored { reply_id, cause }
}


/// Dispatch a single frame.
///
/// `write_buf` is the caller-owned outbound staging buffer (DEF-094);
/// dispatchers that produce [`StagedAction::SendBytesRange`] write
/// into it and record the range. The caller (feed_bytes) is
/// responsible for clearing `write_buf` at the start of each
/// entry-point call and materialising the ranges into `&'buf [u8]`
/// slices after the write-phase mutable borrow completes.
pub(crate) fn dispatch(
    prev: ProtoState,
    tag: crate::wire::InboundTag,
    payload: &[u8],
    write_buf: &mut WriteBuf,
    coords: FrameCoords,
    row_desc_slot: &mut Option<crate::decode::RowDesc>,
) -> DispatchOutcome {
    match (prev, tag) {
        // =============================================================
        // Ping flow (Phase 1a, carried forward)
        // =============================================================
        (ProtoState::PingAwaitingRfq(id), TAG_READY_FOR_QUERY) => match payload {
            // DEF-112: `id: ReplyId<PingKind>` — the typed
            // `deliver` helper binds the payload to `PongPayload`
            // at compile time. Attempting to deliver any other
            // payload type here is a type error.
            //
            // Tier-1 tx_status validation — users never receive a
            // `TxStatus` outside `{Idle, InTransaction, Failed}`;
            // any other byte is a wire violation classified as
            // `MalformedReadyForQuery`.
            [tx_byte] => match crate::action::TxStatus::try_from_byte(*tx_byte) {
                Some(tx_status) => DispatchOutcome::AdvancedWithAction {
                    new_state: ProtoState::Idle,
                    action: crate::action::deliver(
                        id,
                        crate::action::PongPayload { tx_status },
                    ),
                },
                None => errored(
                    Some(id.consume()),
                    ProtocolError::MalformedReadyForQuery { payload_len: 1 },
                ),
            },
            other => errored(Some(id.consume()), ProtocolError::MalformedReadyForQuery {
                    payload_len: other.len(),
                }),
        },
        (ProtoState::PingAwaitingRfq(id), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            errored(Some(id.consume()), cause)
        }
        (ProtoState::PingAwaitingRfq(id), other) => errored(Some(id.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingStartupTrust — awaiting AuthenticationOk
        // (DEF-097: Trust connections cannot accept AUTH_SASL — that
        // case is a per-variant dispatcher arm now, not a runtime
        // classification.)
        // =============================================================
        (ProtoState::ConnectingStartupTrust { reply }, TAG_AUTHENTICATION) => {
            dispatch_auth_in_startup_trust(reply, payload)
        }
        (ProtoState::ConnectingStartupTrust { reply }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            errored(Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingStartupTrust { reply }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            errored(Some(reply.consume()), ProtocolError::UnsupportedProtocolOption)
        }
        (ProtoState::ConnectingStartupTrust { reply }, other) => errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // ConnectingStartupScram — awaiting AuthenticationSASL
        // (DEF-097: mirror of the Trust arm. A Scram connection
        // receiving AUTH_OK in this state is classified as
        // `UnsupportedAuthMethod` — the server accepted without
        // challenge while the user supplied a password, a PG policy
        // mismatch worth surfacing.)
        // =============================================================
        (ProtoState::ConnectingStartupScram { reply, scram }, TAG_AUTHENTICATION) => {
            dispatch_auth_in_startup_scram(reply, scram, payload, write_buf)
        }
        (ProtoState::ConnectingStartupScram { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            errored(Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingStartupScram { reply, .. }, TAG_NEGOTIATE_PROTOCOL_VERSION) => {
            errored(Some(reply.consume()), ProtocolError::UnsupportedProtocolOption)
        }
        (ProtoState::ConnectingStartupScram { reply, .. }, other) => errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

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
            dispatch_auth_sasl_continue(reply, scram, client_first_bare, client_nonce_b64, payload, write_buf)
        }
        (ProtoState::ConnectingScramAwaitingServerFirst { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            errored(Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingScramAwaitingServerFirst { reply, .. }, other) => {
            errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
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
        ) => dispatch_auth_sasl_final(reply, expected_server_sig, payload),
        (ProtoState::ConnectingScramAwaitingServerFinal { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            errored(Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingScramAwaitingServerFinal { reply, .. }, other) => {
            errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // SCRAM: awaiting AuthenticationOk after server sig verified
        // =============================================================
        (ProtoState::ConnectingScramAwaitingAuthOk(reply), TAG_AUTHENTICATION) => {
            dispatch_auth_ok_after_scram(reply, payload)
        }
        (ProtoState::ConnectingScramAwaitingAuthOk(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            errored(Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingScramAwaitingAuthOk(reply), other) => errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // Post-auth: waiting for BackendKeyData
        //
        // `ParameterStatus` (tag 'S') is filtered pre-dispatch in
        // `feed_bytes` via `allows_unsolicited_param_status`; the
        // dispatcher never sees it for these states. DEF-054.
        // =============================================================
        (ProtoState::ConnectingPostAuthAwaitingKey(reply), TAG_BACKEND_KEY_DATA) => {
            match parse_backend_key_data(payload) {
                Ok((pid, secret_key)) => DispatchOutcome::AdvancedSilent {
                    new_state: ProtoState::ConnectingPostAuthHaveKey {
                        reply,
                        pid,
                        secret_key,
                    },
                },
                Err(cause) => errored(Some(reply.consume()), cause),
            }
        }
        (ProtoState::ConnectingPostAuthAwaitingKey(reply), TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            errored(Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingPostAuthAwaitingKey(reply), other) => errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other }),

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
        ) => match payload {
            // DEF-112: `reply: ReplyId<StartupKind>` — typed
            // `deliver` forces a `StartupCompletePayload` payload.
            [tx_byte] => match crate::action::TxStatus::try_from_byte(*tx_byte) {
                Some(tx_status) => DispatchOutcome::AdvancedWithAction {
                    new_state: ProtoState::Idle,
                    action: crate::action::deliver(
                        reply,
                        crate::action::StartupCompletePayload {
                            pid,
                            secret_key,
                            tx_status,
                        },
                    ),
                },
                None => errored(
                    Some(reply.consume()),
                    ProtocolError::MalformedReadyForQuery { payload_len: 1 },
                ),
            },
            other => errored(Some(reply.consume()), ProtocolError::MalformedReadyForQuery {
                    payload_len: other.len(),
                }),
        },
        (ProtoState::ConnectingPostAuthHaveKey { reply, .. }, TAG_ERROR_RESPONSE) => {
            let cause = parse_error_response(payload);
            errored(Some(reply.consume()), cause)
        }
        (ProtoState::ConnectingPostAuthHaveKey { reply, .. }, other) => {
            errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
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
            // 1c-2a: parse the schema and stash it in PgProtocol's
            // row_desc slot before transitioning to StreamingRows.
            // Subsequent DataRow frames' StreamRowRange emissions
            // will borrow from this slot at materialise time.
            match crate::decode::parse_row_description(payload) {
                Ok(desc) => {
                    *row_desc_slot = Some(desc);
                    DispatchOutcome::AdvancedSilent {
                        new_state: ProtoState::SimpleQueryStreamingRows(reply),
                    }
                }
                Err(cause) => errored(Some(reply.consume()), cause),
            }
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_COMMAND_COMPLETE) => {
            advance_to_awaiting_rfq(reply, payload)
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_EMPTY_QUERY_RESPONSE) => {
            DispatchOutcome::AdvancedSilent {
                new_state: ProtoState::SimpleQueryAwaitingRfq {
                    reply,
                    command_tag: crate::error::BoundedStr::default(),
                },
            }
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(reply, payload)
        }
        (ProtoState::SimpleQueryAwaitingFirstResponse(reply), other) => {
            errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // StreamingRows: D / C / E — any other tag is desync
        (ProtoState::SimpleQueryStreamingRows(reply), TAG_DATA_ROW) => {
            stream_row_or_errored(reply, coords)
        }
        (ProtoState::SimpleQueryStreamingRows(reply), TAG_COMMAND_COMPLETE) => {
            advance_to_awaiting_rfq(reply, payload)
        }
        (ProtoState::SimpleQueryStreamingRows(reply), TAG_ERROR_RESPONSE) => {
            advance_to_drain_after_error(reply, payload)
        }
        (ProtoState::SimpleQueryStreamingRows(reply), other) => {
            errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // AwaitingRfq: Z is the only legal frame
        (ProtoState::SimpleQueryAwaitingRfq { reply, command_tag }, TAG_READY_FOR_QUERY) => {
            match payload {
                [tx_byte] => match crate::action::TxStatus::try_from_byte(*tx_byte) {
                    Some(tx_status) => {
                        // 1c-2a: copy the schema out of the protocol's
                        // `row_desc` slot into the terminal reply. The
                        // slot is NOT cleared here — any `StreamRowRange`
                        // staged earlier in this same `feed_bytes` call
                        // still borrows it through materialise; the slot
                        // is cleared at the next `push_command(SimpleQuery)`.
                        let row_desc = *row_desc_slot;
                        DispatchOutcome::AdvancedWithAction {
                            new_state: ProtoState::Idle,
                            action: crate::action::deliver(
                                reply,
                                crate::action::QueryCompletePayload {
                                    command_tag,
                                    tx_status,
                                    row_desc,
                                },
                            ),
                        }
                    }
                    None => errored(
                        Some(reply.consume()),
                        ProtocolError::MalformedReadyForQuery { payload_len: 1 },
                    ),
                },
                other => errored(
                    Some(reply.consume()),
                    ProtocolError::MalformedReadyForQuery {
                        payload_len: other.len(),
                    },
                ),
            }
        }
        (ProtoState::SimpleQueryAwaitingRfq { reply, .. }, other) => {
            errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // DrainRfqAfterError: silent consume of Z → Idle
        (ProtoState::DrainRfqAfterError, TAG_READY_FOR_QUERY) => {
            match payload {
                [_] => DispatchOutcome::AdvancedSilent {
                    new_state: ProtoState::Idle,
                },
                other => errored(
                    None,
                    ProtocolError::MalformedReadyForQuery {
                        payload_len: other.len(),
                    },
                ),
            }
        }
        (ProtoState::DrainRfqAfterError, other) => {
            errored(None, ProtocolError::UnexpectedFrame { tag: other })
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
            DispatchOutcome::AdvancedSilent {
                new_state: ProtoState::ParseAwaitingRfq(reply),
            }
        }
        (ProtoState::ParseAwaitingParseComplete(reply), TAG_ERROR_RESPONSE) => {
            // Recoverable parse error — PG spec sends Z after E, so
            // drain it silently and return to Idle (reusing the
            // `DrainRfqAfterError` variant — both drain
            // the same trailing RFQ pattern).
            let cause = parse_error_response(payload);
            DispatchOutcome::AdvancedWithAction {
                new_state: ProtoState::DrainRfqAfterError,
                action: StagedAction::FailReply {
                    id: reply.consume(),
                    cause,
                },
            }
        }
        (ProtoState::ParseAwaitingParseComplete(reply), other) => {
            errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        (ProtoState::ParseAwaitingRfq(reply), TAG_READY_FOR_QUERY) => match payload {
            [tx_byte] => match crate::action::TxStatus::try_from_byte(*tx_byte) {
                Some(tx_status) => DispatchOutcome::AdvancedWithAction {
                    new_state: ProtoState::Idle,
                    action: crate::action::deliver(
                        reply,
                        crate::action::ParseCompletePayload { tx_status },
                    ),
                },
                None => errored(
                    Some(reply.consume()),
                    ProtocolError::MalformedReadyForQuery { payload_len: 1 },
                ),
            },
            other => errored(
                Some(reply.consume()),
                ProtocolError::MalformedReadyForQuery {
                    payload_len: other.len(),
                },
            ),
        },
        (ProtoState::ParseAwaitingRfq(reply), other) => {
            errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: other })
        }

        // =============================================================
        // Idle — unsolicited frames are out-of-spec
        // =============================================================
        (ProtoState::Idle, other) => errored(None, ProtocolError::UnexpectedFrame { tag: other }),

        // =============================================================
        // Errored — terminal sink (Phase 1a pattern carried forward)
        // =============================================================
        (ProtoState::Errored(original), _) => DispatchOutcome::AdvancedSilent {
            new_state: ProtoState::Errored(original),
        },
    }
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
#[expect(clippy::result_large_err, reason = "no_alloc: Box unavailable; error path only")]
fn auth_sub_code(payload: &[u8]) -> Result<(crate::wire::AuthSubCode, &[u8]), ProtocolError> {
    match payload {
        [a, b, c, d, rest @ ..] => {
            let raw = u32::from_be_bytes([*a, *b, *c, *d]);
            let code = crate::wire::AuthSubCode::try_from_u32(raw)
                .ok_or(ProtocolError::UnsupportedAuthMethod {
                    sub_code: crate::error::AuthSubCodeClass::Unknown(raw),
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
fn dispatch_auth_in_startup_trust(reply: ReplyId<crate::reply_id::StartupKind>, payload: &[u8]) -> DispatchOutcome {
    let (code, _rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return errored(Some(reply.consume()), cause)
        }
    };

    match code {
        crate::wire::AuthSubCode::Ok => DispatchOutcome::AdvancedSilent {
            new_state: ProtoState::ConnectingPostAuthAwaitingKey(reply),
        },
        // `Sasl` / `SaslContinue` / `SaslFinal`: a Trust connection
        // never requested SCRAM, so any SASL message means the server
        // expects an auth method we are not configured for.
        // Tier-1 exhaustive — a future new `AuthSubCode` variant
        // forces this match to be updated.
        other @ (crate::wire::AuthSubCode::Sasl
            | crate::wire::AuthSubCode::SaslContinue
            | crate::wire::AuthSubCode::SaslFinal) => errored(
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
    reply: ReplyId<crate::reply_id::StartupKind>,
    scram: crate::scram::session::ScramSession,
    payload: &[u8],
    write_buf: &mut WriteBuf,
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return errored(Some(reply.consume()), cause)
        }
    };

    match code {
        crate::wire::AuthSubCode::Sasl => {
            if !mechanism_list_contains_scram(rest) {
                return errored(Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::NoSupportedMechanism));
            }

            // Build client-first-message and SASLInitialResponse.
            // DEF-094: write directly into the caller-owned `write_buf`
            // and record the range; materialise at the entry-point
            // boundary after the mutable write phase releases.
            match build_sasl_initial_response(&scram, write_buf) {
                Ok((range, client_first_bare, client_nonce_b64)) => {
                    DispatchOutcome::AdvancedWithAction {
                        new_state: ProtoState::ConnectingScramAwaitingServerFirst {
                            reply,
                            scram,
                            client_first_bare,
                            client_nonce_b64,
                        },
                        action: StagedAction::SendBytesRange(range),
                    }
                }
                Err(cause) => errored(Some(reply.consume()), cause),
            }
        }
        other @ (crate::wire::AuthSubCode::Ok
            | crate::wire::AuthSubCode::SaslContinue
            | crate::wire::AuthSubCode::SaslFinal) => errored(
            Some(reply.consume()),
            ProtocolError::UnsupportedAuthMethod { sub_code: crate::error::AuthSubCodeClass::KnownButWrong(other) },
        ),
    }
}

/// Check if the SASL mechanism list contains SCRAM-SHA-256.
fn mechanism_list_contains_scram(data: &[u8]) -> bool {
    // Mechanism names are NUL-separated, with an extra NUL terminator.
    // e.g.: b"SCRAM-SHA-256\0\0"
    for name in data.split(|b| *b == 0) {
        if name == SCRAM_SHA_256_MECHANISM {
            return true;
        }
    }
    false
}

/// Build the SASLInitialResponse frame for SCRAM-SHA-256.
///
/// Takes a [`ScramSession`] by shared reference. The parameter is
/// not dereferenced — the password is not needed until the
/// SASL-continue step — but the signature makes the call-site
/// typestate explicit: calling this function without constructing a
/// `ScramSession` first is a compile error. The
/// `Credentials`-vs-`ScramPassword` split happens exactly once at
/// [`ScramSession::try_from_credentials`] (audit A2). The `_` in
/// `_: &ScramSession` uses Rust's anonymous-parameter syntax — the
/// parameter shape is load-bearing, its binding is not.
///
/// [`ScramSession`]: crate::scram::session::ScramSession
/// [`ScramSession::try_from_credentials`]: crate::scram::session::ScramSession::try_from_credentials
#[expect(clippy::result_large_err, reason = "no_alloc: Box unavailable; error path only")]
fn build_sasl_initial_response(
    _: &crate::scram::session::ScramSession,
    write_buf: &mut WriteBuf,
) -> Result<
    (
        // DEF-100: typed non-empty range into write_buf (replaces
        // raw `(start, end): (usize, usize)`). Non-zero length is
        // a type invariant; silent-empty fallback in `materialise`
        // closes from tier-3 audit to tier-2 structural.
        crate::action::NonEmptyRange,
        // DEF-099: POD state-bound buffers instead of heapless::Vec.
        // Copy-capable, Drop-free — no `Vec::drop` propagation into
        // ProtoState.
        crate::ident::PodBytes<{ crate::scram::wire::MAX_CLIENT_FIRST_BARE_LEN }>,
        crate::ident::PodBytes<{ crate::scram::wire::MAX_CLIENT_NONCE_B64_LEN }>,
    ),
    ProtocolError,
> {
    use crate::scram::wire;

    // SCRAM client-first uses the username in "n=<user>".
    // PostgreSQL already has the username from the StartupMessage;
    // an empty "n=" field is accepted per RFC 5802.
    let user_bytes: &[u8] = b"";

    let client_nonce_vec = wire::generate_client_nonce().map_err(ProtocolError::Scram)?;

    let client_first_bare_vec =
        wire::build_client_first_bare(user_bytes, &client_nonce_vec).map_err(ProtocolError::Scram)?;

    let client_first_msg =
        wire::build_client_first_message(user_bytes, &client_nonce_vec).map_err(ProtocolError::Scram)?;

    // Build SASLInitialResponse frame in-place in the caller-owned
    // `write_buf`. DEF-094: the entry-point materialises the
    // `(start, end)` range into a `&'buf [u8]` ref after the write
    // phase releases — zero-copy SendBytes.
    let start = write_buf.len();
    write_buf
        .push_u8(crate::wire::TAG_SASL_RESPONSE.byte())
        .map_err(|_| ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;
    write_buf
        .with_length_prefix(|w| {
            // Mechanism name NUL-terminated.
            w.push_bytes(SCRAM_SHA_256_MECHANISM)
                .map_err(|_| crate::write_buf::WriteBufFull)?;
            w.push_u8(0).map_err(|_| crate::write_buf::WriteBufFull)?;
            // Body length as i32.
            let body_len =
                i32::try_from(client_first_msg.len()).map_err(|_| crate::write_buf::WriteBufFull)?;
            w.push_i32_be(body_len)
                .map_err(|_| crate::write_buf::WriteBufFull)?;
            // Body = client-first-message.
            w.push_bytes(&client_first_msg)
                .map_err(|_| crate::write_buf::WriteBufFull)?;
            Ok(())
        })
        .map_err(|_| ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;
    let end = write_buf.len();

    // DEF-099: convert heapless::Vec output of the scram::wire
    // builders into POD PodBytes for state storage. One extra copy
    // on the cold SCRAM handshake path; structural win is that the
    // state variant becomes `Vec::drop`-free.
    let client_first_bare = crate::ident::PodBytes::try_from_slice(&client_first_bare_vec)
        .map_err(|_| ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;
    let client_nonce_b64 = crate::ident::PodBytes::try_from_slice(&client_nonce_vec)
        .map_err(|_| ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;
    // DEF-100: typed NonEmptyRange. The SASLInitialResponse frame
    // always writes ≥1 byte (the tag byte), so the `from_write_span`
    // None-branch is architecturally unreachable.
    let range = crate::action::NonEmptyRange::new(start, end, write_buf.len())
        .ok_or(ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))?;
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
fn dispatch_auth_sasl_continue(
    reply: ReplyId<crate::reply_id::StartupKind>,
    scram: crate::scram::session::ScramSession,
    client_first_bare: crate::ident::PodBytes<{ crate::scram::wire::MAX_CLIENT_FIRST_BARE_LEN }>,
    client_nonce_b64: crate::ident::PodBytes<{ crate::scram::wire::MAX_CLIENT_NONCE_B64_LEN }>,
    payload: &[u8],
    write_buf: &mut WriteBuf,
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return errored(Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::SaslContinue) {
        return errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION });
    }

    // `rest` is the server-first-message body.
    let server_first =
        match crate::scram::wire::parse_server_first(rest, client_nonce_b64.as_slice()) {
            Ok(sf) => sf,
            Err(e) => {
                return errored(Some(reply.consume()), ProtocolError::Scram(e));
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
                return errored(Some(reply.consume()), ProtocolError::Scram(e));
            }
        };

    // Compute proof and expected server signature.
    //
    // AuthMessage = client-first-bare + "," + server-first + "," +
    // client-final-without-proof. The three components are passed
    // separately — compute_client_proof feeds them incrementally into
    // HMAC::update(), with zero intermediate buffer. No staging
    // buffer → no silent-truncation class → tier-1 by construction.
    let (proof, expected_server_sig) = crate::scram::crypto::compute_client_proof(
        password_bytes,
        &server_first.salt,
        server_first.iterations,
        client_first_bare.as_slice(),
        rest,
        &client_final_without_proof,
    );

    // Base64-encode proof.
    let mut proof_b64_buf = [0u8; 64];
    let proof_b64_len =
        match crate::scram::wire::base64_encode_to_buf(proof.as_ref(), &mut proof_b64_buf) {
            Ok(n) => n,
            Err(_) => {
                return errored(Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))
            }
        };
    let proof_b64 = match proof_b64_buf.get(..proof_b64_len) {
        Some(s) => s,
        None => {
            return errored(Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow))
        }
    };

    // Build client-final-message.
    let client_final_msg = match crate::scram::wire::build_client_final_message(
        server_first.server_nonce.as_bytes(),
        proof_b64,
    ) {
        Ok(v) => v,
        Err(e) => {
            return errored(Some(reply.consume()), ProtocolError::Scram(e));
        }
    };

    // Build SASLResponse frame directly in the caller-owned
    // `write_buf`. DEF-094 — materialises to `&'buf [u8]` at the
    // entry-point boundary (zero-copy SendBytes).
    let start = write_buf.len();
    if write_buf.push_u8(crate::wire::TAG_SASL_RESPONSE.byte()).is_err()
        || write_buf
            .with_length_prefix(|w| w.push_bytes(&client_final_msg))
            .is_err()
    {
        return errored(Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow));
    }
    // DEF-100: typed NonEmptyRange. The SASLResponse frame always
    // includes the 1-byte tag, so `from_write_span` cannot yield
    // None under a successful write path.
    let Some(range) = crate::action::NonEmptyRange::from_write_span(start, write_buf) else {
        return errored(Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::BufferOverflow));
    };

    DispatchOutcome::AdvancedWithAction {
        new_state: ProtoState::ConnectingScramAwaitingServerFinal {
            reply,
            expected_server_sig,
        },
        action: StagedAction::SendBytesRange(range),
    }
}

/// Dispatch AuthenticationSASLFinal (server-final-message).
fn dispatch_auth_sasl_final(
    reply: ReplyId<crate::reply_id::StartupKind>,
    expected_server_sig: crate::scram::types::SecretDigest,
    payload: &[u8],
) -> DispatchOutcome {
    let (code, rest) = match auth_sub_code(payload) {
        Ok(pair) => pair,
        Err(cause) => {
            return errored(Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::SaslFinal) {
        return errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION });
    }

    // Parse server-final-message.
    let received_sig = match crate::scram::wire::parse_server_final(rest) {
        Ok(sig) => sig,
        Err(e) => {
            return errored(Some(reply.consume()), ProtocolError::Scram(e));
        }
    };

    // Constant-time comparison (DEF-039).
    if !bool::from(expected_server_sig.ct_eq(&received_sig)) {
        return errored(Some(reply.consume()), ProtocolError::Scram(crate::scram::wire::ScramError::SignatureMismatch));
    }

    // Signature matches. Await AuthenticationOk.
    DispatchOutcome::AdvancedSilent {
        new_state: ProtoState::ConnectingScramAwaitingAuthOk(reply),
    }
}

/// Dispatch AuthenticationOk after SCRAM verification.
fn dispatch_auth_ok_after_scram(reply: ReplyId<crate::reply_id::StartupKind>, payload: &[u8]) -> DispatchOutcome {
    // AuthOk has no trailing data; destructure with anonymous `_`
    // pattern (pattern-discard, not a `_`-prefixed identifier —
    // allowed by the `no underscore vars` discipline).
    let code = match auth_sub_code(payload) {
        Ok((code, _)) => code,
        Err(cause) => {
            return errored(Some(reply.consume()), cause)
        }
    };

    if !matches!(code, crate::wire::AuthSubCode::Ok) {
        return errored(Some(reply.consume()), ProtocolError::UnexpectedFrame { tag: TAG_AUTHENTICATION });
    }

    DispatchOutcome::AdvancedSilent {
        new_state: ProtoState::ConnectingPostAuthAwaitingKey(reply),
    }
}

// -----------------------------------------------------------------
// Helper: parse ErrorResponse typed fields
// -----------------------------------------------------------------

/// Parse an ErrorResponse payload into a classified error.
///
/// PG ErrorResponse body: series of typed fields, each = type-byte +
/// NUL-terminated string. Terminated by a bare NUL (0x00). We extract
/// 'S' (severity), 'C' (code), 'M' (message), 'D' (detail), 'H' (hint).
///
/// Cold path: called only when the server emits an `ErrorResponse`
/// frame (`'E'` tag). The `#[cold]` attribute tells LLVM to keep the
/// body out of hot-path inlining scope.
#[cold]
fn parse_error_response(payload: &[u8]) -> ProtocolError {
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
        let start = pos;
        while let Some(b) = payload.get(pos) {
            if *b == 0 {
                break;
            }
            pos = match pos.checked_add(1) {
                Some(p) => p,
                None => break,
            };
        }
        let value_bytes = payload.get(start..pos).unwrap_or(&[]);
        let value_str = core::str::from_utf8(value_bytes).unwrap_or("");

        // Skip past the NUL terminator.
        pos = match pos.checked_add(1) {
            Some(p) => p,
            None => break,
        };

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
            b'M' => {
                message = BoundedStr::from_str_truncating(value_str);
            }
            b'D' => {
                detail = BoundedStr::from_str_truncating(value_str);
            }
            b'H' => {
                hint = BoundedStr::from_str_truncating(value_str);
            }
            _ => {} // Unknown field type — skip.
        }
    }

    ProtocolError::ServerErrorResponse {
        // No S or V field in payload → `Severity::Unknown` fallback
        // (public API preserves the pre-uplift shape).
        severity: severity.unwrap_or(Severity::Unknown),
        code,
        message,
        detail,
        hint,
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
    reply: ReplyId<crate::reply_id::QueryKind>,
    payload: &[u8],
) -> DispatchOutcome {
    match parse_command_tag(payload) {
        Ok(command_tag) => DispatchOutcome::AdvancedSilent {
            new_state: ProtoState::SimpleQueryAwaitingRfq {
                reply,
                command_tag,
            },
        },
        Err(cause) => errored(Some(reply.consume()), cause),
    }
}

/// 1c-1b helper: shared body for the `E` arm in both
/// `AwaitingFirstResponse` and `StreamingRows` states. Emit
/// `FailReply` (NO `CloseSocket` — query-level errors are
/// connection-survivable per PG §55.2.3) and transition to
/// `DrainRfqAfterError` so the trailing `Z` returns the state to
/// `Idle`.
///
/// Centralises the "query-level E → recoverable" invariant. The
/// contrast with fatal error paths (which return
/// `DispatchOutcome::Errored { .. }` → forced `CloseSocket`) is
/// the load-bearing distinction test 5
/// (`query_error_emits_fail_reply_and_connection_survives`) pins.
fn advance_to_drain_after_error(
    reply: ReplyId<crate::reply_id::QueryKind>,
    payload: &[u8],
) -> DispatchOutcome {
    let cause = parse_error_response(payload);
    DispatchOutcome::AdvancedWithAction {
        new_state: ProtoState::DrainRfqAfterError,
        action: StagedAction::FailReply {
            id: reply.consume(),
            cause,
        },
    }
}

/// 1c-1b helper: build a `StreamRowRange` for a `DataRow` frame, or
/// classify as `ProtocolInvariantBroken` on a malformed empty body.
///
/// `reply.get()` — not `.consume()` — rows are in-progress signals;
/// the `ReplyId` commits on the terminal `CommandComplete` →
/// `ReadyForQuery` pair. The `NonEmptyRange` constructor's `None`
/// branch is architecturally unreachable when `parse_header` has
/// already validated frame bounds, but surfacing it as a classified
/// error (vs a panic) preserves the crate-root panic ban.
fn stream_row_or_errored(
    reply: ReplyId<crate::reply_id::QueryKind>,
    coords: FrameCoords,
) -> DispatchOutcome {
    match crate::action::NonEmptyRange::new(
        coords.payload_start(),
        coords.payload_end(),
        coords.populated_len(),
    ) {
        Some(row_range) => {
            let id = reply.get();
            DispatchOutcome::AdvancedWithAction {
                new_state: ProtoState::SimpleQueryStreamingRows(reply),
                action: StagedAction::StreamRowRange { id, row_range },
            }
        }
        None => errored(Some(reply.consume()), ProtocolError::ProtocolInvariantBroken),
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
#[expect(clippy::result_large_err, reason = "no_alloc: Box unavailable; error path only")]
fn parse_command_tag(payload: &[u8]) -> Result<crate::error::BoundedStr<32>, ProtocolError> {
    use crate::error::BoundedStr;
    // Strip the trailing NUL terminator. Missing NUL → framing error.
    let Some(body) = payload.strip_suffix(b"\0") else {
        return Err(ProtocolError::MalformedCommandComplete {
            payload_len: payload.len(),
        });
    };
    let s = core::str::from_utf8(body).unwrap_or("");
    Ok(BoundedStr::from_str_truncating(s))
}

/// Parse BackendKeyData payload: 8 bytes = pid(i32 BE) + secret_key(i32 BE).
///
/// Cold path: called once per connection at end of startup handshake.
/// Not on any per-frame or per-query hot path.
#[cold]
#[expect(clippy::result_large_err, reason = "no_alloc: Box unavailable; error path only")]
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

    fn mk_err(
        severity: &str,
        code: &str,
        message: &str,
        detail: &str,
        hint: &str,
    ) -> ProtocolError {
        use crate::error::{BoundedStr, Severity, SqlStateCode};
        ProtocolError::ServerErrorResponse {
            severity: Severity::from_bytes(severity.as_bytes()),
            code: SqlStateCode::from_bytes(code.as_bytes()),
            message: BoundedStr::from_str_truncating(message),
            detail: BoundedStr::from_str_truncating(detail),
            hint: BoundedStr::from_str_truncating(hint),
        }
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
        let actual = parse_error_response(&body);
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
        let actual = parse_error_response(&body);
        let expected = mk_err("ERROR", "", "", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): first severity (`S` or `V`) wins — the
    /// `if severity.is_empty()` guard in the S/V arms blocks overwrite.
    #[test]
    fn severity_s_wins_over_later_v() {
        let body = build_error_body(&[(b'S', b"FATAL"), (b'V', b"ERROR")]);
        let actual = parse_error_response(&body);
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
        let actual = parse_error_response(&body);
        let expected = mk_err("", "", "real message", "", "");
        assert_eq!(actual, expected);
    }

    /// Invariant (spec): empty payload (just the terminator NUL)
    /// yields an all-empty `ServerErrorResponse`, not a parse failure
    /// or panic.
    #[test]
    fn empty_payload_yields_empty_fields() {
        let body: alloc::vec::Vec<u8> = alloc::vec![0];
        let actual = parse_error_response(&body);
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
        let actual = parse_error_response(&body);
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
        let actual = parse_error_response(&body);
        let expected = mk_err("", "SECOND", "", "", "");
        assert_eq!(actual, expected);
    }
}
