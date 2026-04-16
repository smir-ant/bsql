//! `(prev_state, frame_tag) → outcome` matcher.
//!
//! The dispatcher is the **single** place the protocol decides what to
//! do with a freshly-parsed frame given the current state. The match is
//! exhaustive over `(state, tag)` pairs the Phase 1a flow can encounter;
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
//! length / presence check. There is no `slice.get(i)` `Option` dance
//! in here and no "unreachable but classify" branch: any payload shape
//! the pattern does not match falls through to a typed
//! `ProtocolError::Malformed…` classification.
//!
//! # Outcomes
//!
//! - [`DispatchOutcome::Advanced`] — frame consumed cleanly. The caller
//!   replaces state with `new_state`, pushes `action` (if `Some`), and
//!   advances the read buffer by `total_len` (which the caller already
//!   holds — no reason to echo it back through this enum).
//! - [`DispatchOutcome::Errored`] — protocol violation. Caller must
//!   tear the connection down. The state has already been moved out by
//!   the caller (it called `mem::take`); this outcome surfaces the
//!   in-flight reply id (if any) so the caller can FailReply it.

use crate::action::{Action, Reply};
use crate::error::ProtocolError;
use crate::reply_id::ReplyId;
use crate::state::ProtoState;
use crate::wire::{TAG_ERROR_RESPONSE, TAG_READY_FOR_QUERY};

/// What to do after dispatching a single frame.
#[derive(Debug)]
pub(crate) enum DispatchOutcome {
    /// Frame consumed; transition to `new_state`. Caller advances the
    /// read buffer by the `total_len` it already holds from the
    /// preceding `parse_header` and pushes `action` if present.
    Advanced {
        new_state: ProtoState,
        action: Option<Action>,
    },
    /// Frame rejected; connection irrecoverable. Caller must tear the
    /// transport down (FailReply if `reply_id` Some, then CloseSocket).
    Errored {
        reply_id: Option<ReplyId>,
        cause: ProtocolError,
    },
}

/// Dispatch a single frame.
///
/// - `prev` was just `mem::take`'d from `PgProtocol::state`; it is
///   now owned by the dispatcher.
/// - `tag` is the first byte of the frame.
/// - `payload` is the body after the 5-byte `(tag + length)` header —
///   its length equals `total_len - 5`, which the caller computed and
///   verified before invoking us.
pub(crate) fn dispatch(prev: ProtoState, tag: u8, payload: &[u8]) -> DispatchOutcome {
    match (prev, tag) {
        // Successful Ping reply: RFQ in `AwaitingPingReply`.
        //
        // RFQ's payload is spec'd at exactly 1 byte (the tx-status —
        // one of `I`, `T`, `E`). The slice pattern `[tx_status]`
        // matches only when that length holds; any other shape falls
        // through to `MalformedReadyForQuery` carrying the observed
        // length for diagnostic.
        (ProtoState::AwaitingPingReply(id), TAG_READY_FOR_QUERY) => match payload {
            [tx_status] => DispatchOutcome::Advanced {
                new_state: ProtoState::Idle,
                action: Some(Action::DeliverReply {
                    id,
                    value: Reply::Pong {
                        tx_status: *tx_status,
                    },
                }),
            },
            other => DispatchOutcome::Errored {
                reply_id: Some(id),
                cause: ProtocolError::MalformedReadyForQuery {
                    payload_len: other.len(),
                },
            },
        },
        // Server emitted ErrorResponse where we expected RFQ.
        (ProtoState::AwaitingPingReply(id), TAG_ERROR_RESPONSE) => DispatchOutcome::Errored {
            reply_id: Some(id),
            cause: ProtocolError::ServerError,
        },
        // Anything else in `AwaitingPingReply` is out-of-spec for the
        // Ping flow. Phase 1a does not understand auth/notice/etc.
        // mid-Ping; the connection is desynced.
        (ProtoState::AwaitingPingReply(id), other) => DispatchOutcome::Errored {
            reply_id: Some(id),
            cause: ProtocolError::UnexpectedFrame { tag: other },
        },
        // Any inbound frame in `Idle` is unsolicited — we never asked
        // for it. Out-of-spec.
        (ProtoState::Idle, other) => DispatchOutcome::Errored {
            reply_id: None,
            cause: ProtocolError::UnexpectedFrame { tag: other },
        },
        // Already classified as terminal: the wrapper has been told to
        // close the socket (the earlier `fail_inflight_and_close` or
        // `DispatchOutcome::Errored` emitted `CloseSocket`). A frame
        // arriving here is either a genuine post-close flush on the
        // wire or a wrapper that hasn't dropped us yet. Either way the
        // protocol stays passive: no new actions, no change of state,
        // and the caller advances past the frame's bytes so the read
        // buffer does not fill up in the meantime. The stored cause
        // stays the *original* classification — not overwritten by
        // subsequent noise.
        (ProtoState::Errored(original), _) => DispatchOutcome::Advanced {
            new_state: ProtoState::Errored(original),
            action: None,
        },
    }
}
