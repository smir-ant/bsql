//! `(prev_state, frame_tag) → outcome` matcher.
//!
//! The dispatcher is the **single** place the protocol decides what to
//! do with a freshly-parsed frame given the current state. The match is
//! exhaustive over `(state, tag)` pairs the Phase 1a flow can encounter;
//! adding a new state or tag is a build error until it is wired into
//! the matcher.
//!
//! Two outcomes:
//!
//! - [`DispatchOutcome::Advanced`] — frame consumed cleanly. Caller
//!   advances the read buffer by `by` bytes, replaces state with
//!   `new_state`, and pushes `action` (if `Some`).
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
    /// Frame consumed; transition to `new_state`. Caller must advance
    /// the read buffer by exactly `by` bytes (= the frame's
    /// `total_len`) and push `action` if present.
    Advanced {
        new_state: ProtoState,
        by: usize,
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
/// `prev` was just `mem::take`'d from `PgProtocol::state`; it is now
/// owned. `tag` and `total_len` come from a successful `parse_header`;
/// `unread` is the buffer slice starting with the frame's tag (the
/// caller has already verified `unread.len() >= total_len`).
pub(crate) fn dispatch(
    prev: ProtoState,
    tag: u8,
    unread: &[u8],
    total_len: usize,
) -> DispatchOutcome {
    match (prev, tag) {
        // Successful Ping reply: RFQ in `AwaitingPingReply`.
        (ProtoState::AwaitingPingReply(id), TAG_READY_FOR_QUERY) => {
            // RFQ payload must be exactly 1 byte (the tx-status).
            // total_len = 1 (tag) + 4 (len) + payload_len = 5 + payload_len.
            // total_len = 6 ⇒ payload = 1 (legal). Anything else is
            // out-of-spec.
            //
            // checked_sub: total_len is at least 5 (smallest legal
            // header). If somehow it were below 5 we'd never have
            // gotten here (parse_header would have returned
            // MalformedLength).
            let payload_len = total_len.saturating_sub(5);
            if payload_len != 1 {
                return DispatchOutcome::Errored {
                    reply_id: Some(id),
                    cause: ProtocolError::MalformedReadyForQuery { payload_len },
                };
            }
            // Read the single payload byte. `unread.get(5)` is in-
            // bounds because the caller verified `unread.len() >= total_len`
            // and `total_len == 6`.
            let tx_status = match unread.get(5) {
                Some(b) => *b,
                None => {
                    // Unreachable in practice; classify rather than panic.
                    return DispatchOutcome::Errored {
                        reply_id: Some(id),
                        cause: ProtocolError::MalformedReadyForQuery { payload_len },
                    };
                }
            };
            DispatchOutcome::Advanced {
                new_state: ProtoState::Idle,
                by: total_len,
                action: Some(Action::DeliverReply {
                    id,
                    value: Reply::Pong { tx_status },
                }),
            }
        }
        // Server emitted ErrorResponse where we expected RFQ.
        (ProtoState::AwaitingPingReply(id), TAG_ERROR_RESPONSE) => {
            DispatchOutcome::Errored {
                reply_id: Some(id),
                cause: ProtocolError::ServerError,
            }
        }
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
    }
}
