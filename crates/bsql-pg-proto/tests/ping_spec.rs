//! Phase 1a — end-to-end Ping flow + bad-path coverage.
//!
//! Every test here names the invariant it defends. Per architect.txt
//! Part III, a test exists *only* for:
//!
//! - **(A) Spec conformance** — the observable API behaviour on legal
//!   input matches the PostgreSQL wire spec.
//! - **(B) Tier-3 invariants** — properties the compiler / architecture
//!   cannot verify (parsers on arbitrary bytes, concurrent interleavings).
//! - **(C) Compile-time invariant docs** — `compile_fail` doctests.
//!
//! Tests covering tier-1 or tier-2 invariants have no place here.
//!
//! This file is pure-sync — no tokio, no async. It pushes commands and
//! feeds bytes into the state machine directly and pattern-matches on
//! the returned [`OutActions`]. That is exactly what the async wrapper
//! (`bsql-driver-postgres`, Phase 1e) does internally; covering it
//! here without a runtime keeps the testbed maximally minimal.

#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::mem_forget,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::integer_division
)]
// Tests *must* be able to fail fast on unexpected match arms — that is
// the whole point of a test. `panic!` / `unreachable!` in test code
// models "this branch is a spec violation by the code under test";
// allowing them is how we surface failure. Note production code carries
// the full forbid bundle — no exceptions there.
#![deny(unused_must_use, unused_lifetimes)]

use bsql_pg_proto::{
    Action, PgCommand, PgProtocol, ProtoState, ProtocolError, Reply, ReplyId, SendBuf,
    wire::{SYNC_WIRE_BYTES, TAG_ERROR_RESPONSE, TAG_READY_FOR_QUERY},
};
use core::num::NonZeroU64;

/// Build a legal `ReadyForQuery` frame: tag `'Z'`, length 5 (self + 1
/// payload byte), one byte of tx-status.
fn rfq_frame(tx_status: u8) -> [u8; 6] {
    // Length field value is 5 (4 bytes of length + 1 payload byte).
    [TAG_READY_FOR_QUERY, 0, 0, 0, 5, tx_status]
}

/// Build an `ErrorResponse` frame with a minimal payload. The frame
/// parser does not inspect the payload; we use `b'\0'` as a single
/// terminator byte so the length is the minimum 5.
fn error_frame() -> [u8; 6] {
    [TAG_ERROR_RESPONSE, 0, 0, 0, 5, b'\0']
}

/// Non-zero correlator value — the raw counter the wrapper would mint.
///
/// Tests keep the raw `NonZeroU64` on the side and compare against it
/// via [`ReplyId::get`]; the `ReplyId` itself is move-only by design
/// (non-`Copy`, non-`Clone` — see [`ReplyId`] docstring), so a test
/// cannot hold a reference to it *and* pass it into a command at the
/// same time.
fn raw(value: u64) -> NonZeroU64 {
    // Tests pass 1..= never 0; fall-through is defensive only.
    NonZeroU64::new(value).unwrap_or(NonZeroU64::MIN)
}

/// A distinguishable `ReplyId` for a single-command test, minted from a
/// raw counter value. Consumes the raw so the caller also remembers the
/// value on the side if they need to assert the round-trip.
fn id(value: NonZeroU64) -> ReplyId {
    ReplyId::from_raw(value)
}

/// Assert the state is `AwaitingPingReply` carrying the given raw value.
///
/// Does **not** construct a temporary `ReplyId` for comparison —
/// constructing one just to feed PartialEq would be an undelivered
/// drop and trip the consume-discipline Drop-guard at end-of-expression.
/// Instead we pattern-match and extract the inner `value` directly.
#[track_caller]
fn expect_awaiting_ping_reply(state: &ProtoState, expected: NonZeroU64) {
    match state {
        ProtoState::AwaitingPingReply(id) => assert_eq!(
            id.get(),
            expected,
            "state is AwaitingPingReply but carrier id does not match",
        ),
        other => panic!("expected AwaitingPingReply({expected}), got {other:?}"),
    }
}

/// Drain an in-flight ping reply via synthetic `ReadyForQuery`.
///
/// Required at the end of any test that leaves the state in
/// `AwaitingPingReply` — because the `ReplyId` inside that variant
/// would otherwise be dropped without delivery when the protocol
/// goes out of scope, tripping its tier-2 structural Drop-guard and
/// aborting the test process.
///
/// This is not ceremony; it is the architectural reality that production
/// code must also respect. Every in-flight `ReplyId` must be consumed,
/// either by a genuine RFQ (this helper) or by
/// [`crate::PgProtocol::terminate`] (not shipped yet; lands with the
/// async wrapper in 1e).
#[track_caller]
fn drain_pending_ping(proto: &mut PgProtocol) {
    let out = proto.feed_bytes(&rfq_frame(b'I'));
    assert_eq!(
        out.len(),
        1,
        "drain: RFQ must emit exactly one Action (DeliverReply)",
    );
    assert!(
        matches!(out.as_slice(), [Action::DeliverReply { .. }]),
        "drain: expected DeliverReply, got {out:?}",
    );
}

/// Push a Ping command and assert the single expected emission
/// (`SendBytes(Static(Sync))`). Used to set up tests whose interesting
/// behaviour is on the response half of the round-trip.
///
/// Using this helper instead of `let _ = proto.push_command(...)`
/// verifies the setup is well-formed on every call site — any
/// regression in the push path is surfaced at the top of the test,
/// not masked.
fn ping_setup(proto: &mut PgProtocol, reply: ReplyId) {
    let out = proto.push_command(PgCommand::Ping { reply });
    assert_eq!(out.len(), 1, "Ping setup: push emits exactly 1 action");
    assert!(
        matches!(out.as_slice(), [Action::SendBytes(SendBuf::Static(_))]),
        "Ping setup: must emit SendBytes(Static), got {out:?}",
    );
}

// ------------------------------------------------------------------
// (A) Spec conformance — legal input → correct protocol output.
// ------------------------------------------------------------------

/// Invariant (spec): pushing a Ping from `Idle` emits exactly one
/// action — `SendBytes(SendBuf::Static(SYNC_WIRE_BYTES))`. The state
/// transitions to `AwaitingPingReply`.
///
/// This corresponds to reforge.md §13 / §19's wire-layer contract:
/// a Ping maps 1:1 to a `Sync` frame on the wire.
#[test]
fn ping_from_idle_emits_sync_bytes() {
    let mut proto = PgProtocol::new();
    assert!(matches!(proto.state(), ProtoState::Idle));

    let ping_raw = raw(1);
    let out = proto.push_command(PgCommand::Ping { reply: id(ping_raw) });

    assert_eq!(out.len(), 1, "Phase 1a budget: push_command emits exactly 1 action");
    match out.as_slice() {
        [Action::SendBytes(SendBuf::Static(bytes))] => {
            assert_eq!(
                *bytes, &SYNC_WIRE_BYTES,
                "must send the const Sync wire bytes, not a rebuilt copy",
            );
        }
        _ => panic!("unexpected action shape: {out:?}"),
    }
    expect_awaiting_ping_reply(proto.state(), ping_raw);

    // Tier-2 structural consume-discipline: drain the in-flight reply
    // before the protocol drops. See [`drain_pending_ping`].
    drain_pending_ping(&mut proto);
}

/// Invariant (spec): feeding a complete `ReadyForQuery` frame while
/// awaiting a ping reply emits `DeliverReply { value: Pong { tx_status } }`,
/// carries the correct status byte, returns state to `Idle`, and leaves
/// no bytes in the read buffer.
#[test]
fn rfq_delivers_pong_and_returns_to_idle() {
    let mut proto = PgProtocol::new();
    let ping_raw = raw(42);
    ping_setup(&mut proto, id(ping_raw));

    let out = proto.feed_bytes(&rfq_frame(b'I'));

    assert_eq!(out.len(), 1, "Phase 1a budget: feed_bytes(RFQ) emits exactly 1 action");
    match out.as_slice() {
        [Action::DeliverReply { id: delivered_id, value }] => {
            assert_eq!(
                delivered_id,
                &ping_raw,
                "reply correlator round-trips unchanged",
            );
            match value {
                Reply::Pong { tx_status } => assert_eq!(
                    *tx_status, b'I',
                    "Pong must surface the RFQ payload byte (tx-status) unchanged",
                ),
                other => panic!("only Reply::Pong defined in Phase 1a; got {other:?}"),
            }
        }
        other => panic!("unexpected action shape: {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Idle));
    assert_eq!(proto.unread().len(), 0, "frame fully consumed");
}

/// Invariant (spec): a frame arriving byte-by-byte (partial feeds) is
/// buffered until complete. Each partial feed emits zero actions; the
/// final feed emits the delivery.
///
/// This exercises `feed_bytes`'s loop bailout on `HeaderParse::Incomplete`
/// — the parser must never act on a half-read frame.
#[test]
fn partial_rfq_feeds_are_buffered_until_complete() {
    let mut proto = PgProtocol::new();
    ping_setup(&mut proto, id(raw(7)));
    let frame = rfq_frame(b'I');

    // Feed the header one byte at a time, then the payload.
    for (i, byte) in frame.iter().enumerate().take(5) {
        let out = proto.feed_bytes(core::slice::from_ref(byte));
        assert_eq!(
            out.len(),
            0,
            "no actions until frame is complete (after feeding byte {i})",
        );
        assert!(
            matches!(proto.state(), ProtoState::AwaitingPingReply(_)),
            "state stays in AwaitingPingReply while buffering",
        );
    }
    // Final byte completes the frame. `frame: [u8; 6]` — slice [5..]
    // is always `[frame[5]]`, compile-time known.
    let last_slice: &[u8] = match frame.last() {
        Some(b) => core::slice::from_ref(b),
        None => panic!("frame has 6 bytes; .last() must be Some"),
    };
    let out = proto.feed_bytes(last_slice);
    assert_eq!(out.len(), 1);
    assert!(matches!(out.as_slice(), [Action::DeliverReply { .. }]));
    assert!(matches!(proto.state(), ProtoState::Idle));
}

// ------------------------------------------------------------------
// (A) Bad-path coverage — user-feedback: "100% coverage of all
//     failure modes, not just happy paths."
// ------------------------------------------------------------------

/// Invariant (spec): an unsolicited `ReadyForQuery` arriving in
/// `Idle` (we never sent anything) is out-of-spec. The protocol
/// classifies it as `UnexpectedFrame` and closes the socket. No
/// `DeliverReply` — there is no in-flight ReplyId to deliver to.
#[test]
fn rfq_in_idle_is_unexpected_frame() {
    let mut proto = PgProtocol::new();
    let out = proto.feed_bytes(&rfq_frame(b'I'));

    // Expect exactly one action: CloseSocket. No FailReply because
    // nothing was in-flight.
    assert_eq!(out.len(), 1, "unexpected frame with no in-flight reply emits CloseSocket only");
    assert!(matches!(out.as_slice(), [Action::CloseSocket]));
}

/// Invariant (spec): `ErrorResponse` arriving while awaiting a Ping
/// reply is classified as `ServerError`. Both FailReply (to notify the
/// caller) and CloseSocket (the connection is desynced) are emitted.
#[test]
fn error_response_fails_the_in_flight_ping() {
    let mut proto = PgProtocol::new();
    let ping_raw = raw(5);
    ping_setup(&mut proto, id(ping_raw));

    let out = proto.feed_bytes(&error_frame());

    assert_eq!(
        out.len(),
        2,
        "Phase 1a budget: server-error during ping → FailReply + CloseSocket",
    );
    match out.as_slice() {
        [
            Action::FailReply { id: failed_id, cause },
            Action::CloseSocket,
        ] => {
            assert_eq!(failed_id, &ping_raw);
            assert!(
                matches!(cause, ProtocolError::ServerErrorResponse { .. }),
                "expected ServerErrorResponse, got {cause:?}",
            );
        }
        _ => panic!("unexpected action sequence: {out:?}"),
    }
}

/// Invariant (spec): a frame with a malformed length-field (< 4) is
/// classified and tears the connection down.
#[test]
fn malformed_length_fails_and_closes() {
    let mut proto = PgProtocol::new();
    let ping_raw = raw(9);
    ping_setup(&mut proto, id(ping_raw));

    // Tag 'Z', length field = 3 (illegal: min is 4).
    let frame = [TAG_READY_FOR_QUERY, 0, 0, 0, 3, b'I'];
    let out = proto.feed_bytes(&frame);

    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [
            Action::FailReply { id: failed_id, cause },
            Action::CloseSocket,
        ] => {
            assert_eq!(failed_id, &ping_raw);
            assert!(matches!(
                cause,
                ProtocolError::MalformedFrameLength { declared: 3 },
            ));
        }
        _ => panic!("unexpected action sequence: {out:?}"),
    }
}

/// Invariant (spec): a frame declaring a length that exceeds
/// `MAX_FRAME_LEN_FIELD` (structural DoS guard) is rejected without
/// the body ever being buffered. `FrameTooLarge` is emitted and the
/// connection is closed.
///
/// This defends reforge.md §53's "Frame length amplification DoS =
/// STRUCTURALLY UNREACHABLE": the cap is checked before any allocation
/// is made toward the body.
#[test]
fn frame_too_large_is_rejected_pre_buffer() {
    let mut proto = PgProtocol::new();
    let ping_raw = raw(11);
    ping_setup(&mut proto, id(ping_raw));

    // Tag 'Z', length field = u32::MAX (obviously > MAX_FRAME_LEN_FIELD).
    // Only the 5-byte header is fed; the body is never sent.
    let frame = [TAG_READY_FOR_QUERY, 0xFF, 0xFF, 0xFF, 0xFF];
    let out = proto.feed_bytes(&frame);

    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [
            Action::FailReply { id: failed_id, cause },
            Action::CloseSocket,
        ] => {
            assert_eq!(failed_id, &ping_raw);
            assert!(matches!(
                cause,
                ProtocolError::FrameTooLarge { declared: 0xFFFF_FFFF },
            ));
        }
        _ => panic!("unexpected action sequence: {out:?}"),
    }
}

/// Invariant (spec): a second Ping pushed while one is already in
/// flight is refused without disturbing the first. The new command's
/// reply gets `FailReply(UnexpectedFrame)`; the original in-flight
/// Ping continues to wait for its RFQ.
#[test]
fn pipelined_ping_is_refused_without_disturbing_first() {
    let mut proto = PgProtocol::new();
    let first_raw = raw(1);
    let second_raw = raw(2);
    ping_setup(&mut proto, id(first_raw));

    let out = proto.push_command(PgCommand::Ping { reply: id(second_raw) });

    assert_eq!(out.len(), 1);
    match out.as_slice() {
        [Action::FailReply { id: failed_id, cause: _ }] => {
            assert_eq!(
                failed_id,
                &second_raw,
                "second Ping's reply id fails, not the first",
            );
        }
        _ => panic!("unexpected action sequence: {out:?}"),
    }
    expect_awaiting_ping_reply(proto.state(), first_raw);

    // Drain the still-pending first-ping reply so its ReplyId is
    // consumed before the protocol drops. See [`drain_pending_ping`].
    drain_pending_ping(&mut proto);
}

/// Build a `ReadyForQuery` frame with a custom payload length. Payload
/// bytes are all `b'X'` — only the length matters for the
/// malformed-length classification, not the byte values.
fn build_rfq_frame_with_payload_len(payload_len: usize) -> Vec<u8> {
    // declared = self-inclusive length = 4 (length-field) + payload_len.
    let declared_usize = payload_len.saturating_add(4);
    let declared = u32::try_from(declared_usize).unwrap_or(u32::MAX);
    let len_bytes = declared.to_be_bytes();
    let mut frame = Vec::with_capacity(5_usize.saturating_add(payload_len));
    frame.push(TAG_READY_FOR_QUERY);
    frame.extend_from_slice(&len_bytes);
    frame.extend(std::iter::repeat_n(b'X', payload_len));
    frame
}

/// Invariant (spec): an RFQ whose payload is not exactly 1 byte is
/// rejected with a `MalformedReadyForQuery` classification carrying
/// the **observed** `payload_len` — the PG spec demands exactly one
/// transaction-status byte.
///
/// Sweeps payload lengths 0, 2, 3, 10. This pins (a) the
/// `[tx_status]` slice pattern in the dispatcher rejecting *any*
/// non-single-element shape (not just empty), (b) the classification
/// variant carrying the actual length for wrapper diagnostics — not a
/// placeholder.
///
/// Closes seam class §4.11.1 / 4 (arm-body access beyond pattern):
/// without multi-byte coverage a future edit replacing `[tx_status] =>`
/// with `[tx_status, ..] =>` (accepting any non-empty prefix) would
/// compile and silently accept bogus RFQ frames. Any input in this
/// sweep catches that regression.
#[test]
fn rfq_with_non_single_byte_payload_is_rejected() {
    for payload_len in [0_usize, 2, 3, 10] {
        let mut proto = PgProtocol::new();
        let ping_raw = raw(100);
        ping_setup(&mut proto, id(ping_raw));

        let frame = build_rfq_frame_with_payload_len(payload_len);
        let out = proto.feed_bytes(&frame);

        assert_eq!(
            out.len(),
            2,
            "payload_len={payload_len}: expected FailReply + CloseSocket",
        );
        match out.as_slice() {
            [
                Action::FailReply { id: failed_id, cause },
                Action::CloseSocket,
            ] => {
                assert_eq!(failed_id, &ping_raw);
                assert!(
                    matches!(
                        cause,
                        ProtocolError::MalformedReadyForQuery { payload_len: actual }
                            if *actual == payload_len,
                    ),
                    "payload_len={payload_len}: unexpected cause {cause:?}",
                );
            }
            other => panic!("payload_len={payload_len}: unexpected actions {other:?}"),
        }
    }
}

/// Invariant (spec): once `ProtoState::Errored(cause)` is entered, the
/// state machine stays terminal — subsequent `feed_bytes` calls drop
/// the incoming frames silently (zero actions emitted) and do **not**
/// overwrite the original cause.
///
/// This pins the dispatcher's `(ProtoState::Errored(original), _) =>
/// Advanced { new_state: Errored(original), action: None }` arm in
/// `dispatch.rs`. Without this test, a future edit could replace
/// `action: None` with `Some(Action::CloseSocket)` (or worse, a
/// DeliverReply with a spoofed id) and the regression would compile
/// with no downstream indication.
///
/// Closes seam classes §4.11.1 / 2 (arm return swap) and 11 (action-
/// ordering / action-presence assumption).
#[test]
fn errored_state_is_terminal_and_drops_subsequent_frames() {
    let mut proto = PgProtocol::new();
    let ping_raw = raw(100);
    ping_setup(&mut proto, id(ping_raw));

    // Drive into Errored via an ErrorResponse — `ServerError` cause.
    let err_out = proto.feed_bytes(&error_frame());
    assert_eq!(err_out.len(), 2, "entering Errored emits FailReply + CloseSocket");
    assert!(matches!(
        proto.state(),
        ProtoState::Errored(ProtocolError::ServerErrorResponse { .. }),
    ));

    // First post-terminal frame: a well-formed RFQ. Expect zero actions
    // (the terminal sink silently drops it) and the original cause
    // preserved.
    let post_out_1 = proto.feed_bytes(&rfq_frame(b'I'));
    assert_eq!(
        post_out_1.len(),
        0,
        "post-terminal RFQ must emit zero actions, got {post_out_1:?}",
    );
    assert!(matches!(
        proto.state(),
        ProtoState::Errored(ProtocolError::ServerErrorResponse { .. }),
    ));

    // Second post-terminal frame: an ErrorResponse that would *normally*
    // classify as a separate ServerError. The original cause must still
    // win — the terminal sink does not overwrite.
    let post_out_2 = proto.feed_bytes(&error_frame());
    assert_eq!(
        post_out_2.len(),
        0,
        "post-terminal ErrorResponse must emit zero actions, got {post_out_2:?}",
    );
    assert!(matches!(
        proto.state(),
        ProtoState::Errored(ProtocolError::ServerErrorResponse { .. }),
    ));
}

/// Invariant (spec): `push_command` on a protocol that has already
/// reached `Errored` fails the new command with the **stored** cause
/// — no new wire actions, no state transition, and the caller's
/// `oneshot` is never left hanging.
///
/// Pins the `handle_push_ping`'s `ProtoState::Errored(original) =>
/// FailReply` arm. A future edit that (a) drops the arm (compile
/// error via exhaustive match — good) or (b) replaces the cause with
/// something else (e.g. `UnexpectedFrame { tag: b'P' }`) would
/// silently shadow the root cause the wrapper is trying to diagnose.
#[test]
fn push_command_on_errored_state_fails_with_stored_cause() {
    let mut proto = PgProtocol::new();
    let first_raw = raw(50);
    ping_setup(&mut proto, id(first_raw));

    // Drive into Errored via ErrorResponse.
    let err_out = proto.feed_bytes(&error_frame());
    assert_eq!(err_out.len(), 2);

    // Push a new Ping while Errored. The command's reply correlator
    // must fail with the stored cause (ServerError), not with a fresh
    // UnexpectedFrame classification.
    let second_raw = raw(51);
    let out = proto.push_command(PgCommand::Ping { reply: id(second_raw) });

    assert_eq!(
        out.len(),
        1,
        "post-terminal push_command emits exactly one FailReply, got {out:?}",
    );
    match out.as_slice() {
        [Action::FailReply { id: failed_id, cause }] => {
            assert_eq!(failed_id, &second_raw, "fail correlates to the new command");
            assert!(
                matches!(cause, ProtocolError::ServerErrorResponse { .. }),
                "cause must be the STORED terminal cause (ServerErrorResponse), got {cause:?}",
            );
        }
        other => panic!("unexpected action shape: {other:?}"),
    }

    // State unchanged — original cause preserved.
    assert!(matches!(
        proto.state(),
        ProtoState::Errored(ProtocolError::ServerErrorResponse { .. }),
    ));
}
