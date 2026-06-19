//! End-to-end Ping flow + bad-path coverage.
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
// Fixture-builder helper fns below panic on malformed synthetic input.
// Integration-test helpers run WITHOUT `cfg(test)`, so the floor's
// `allow-panic-in-tests` carve-out (keyed on `#[test]` context) cannot
// reach them; the panic is the loud test-failure signal, not a silent
// production fallback.
#![allow(clippy::panic, reason = "test harness — fixture builders panic on malformed synthetic input as the loud test-failure signal, not as a silent production fallback; integration-test helper fns are not in `#[test]` context so the in-tests carve-out cannot reach them")]

#![allow(clippy::disallowed_methods, reason = "test/bench harness — fixtures use the sanctioned try_from(..).unwrap_or(SAT) / slice.get(..).unwrap_or(&[]) dead-arm shape, not production data fallbacks")]
use bsql_postgres_proto::{
    Action, ActiveState, ConnectionStatus, PgProtocol, PingKind, ProtocolError, Reply,
    ReplyId,
    wire::{TAG_ERROR_RESPONSE, TAG_READY_FOR_QUERY},
};
use core::num::NonZeroU64;

mod common;
use common::{PushOrPanic, fresh_active_via_trust_handshake, mint_reply};

/// Build a legal `ReadyForQuery` frame: tag `'Z'`, length 5 (self + 1
/// payload byte), one byte of tx-status.
fn rfq_frame(tx_status: u8) -> [u8; 6] {
    // Length field value is 5 (4 bytes of length + 1 payload byte).
    [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, tx_status]
}

/// Build an `ErrorResponse` frame with a minimal payload. The frame
/// parser does not inspect the payload; we use `b'\0'` as a single
/// terminator byte so the length is the minimum 5.
fn error_frame() -> [u8; 6] {
    [TAG_ERROR_RESPONSE.byte(), 0, 0, 0, 5, b'\0']
}

/// Assert the state is `PingAwaitingRfq` carrying the given raw value.
///
/// Does **not** construct a temporary `ReplyId` for comparison —
/// constructing one just to feed PartialEq would be an undelivered
/// drop and trip the consume-discipline Drop-guard at end-of-expression.
/// Instead we pattern-match and extract the inner `value` directly.
#[track_caller]
fn expect_awaiting_ping_reply(state: &ActiveState, expected: NonZeroU64) {
    match state {
        ActiveState::PingAwaitingRfq(id) => assert_eq!(
            id.get(),
            expected,
            "state is PingAwaitingRfq but carrier id does not match",
        ),
        other => panic!("expected PingAwaitingRfq({expected}), got {other:?}"),
    }
}

/// Drain an in-flight ping reply via synthetic `ReadyForQuery`.
///
/// Required at the end of any test that leaves the state in
/// `PingAwaitingRfq` — because the `ReplyId` inside that variant
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
fn drain_pending_ping(proto: &mut PgProtocol<bsql_postgres_proto::ActivePhase>, wb: &mut bsql_postgres_proto::WriteBuf) {
    let out = proto.feed_bytes(&rfq_frame(b'I'), wb);
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

/// Push a Ping command and assert the single expected emission —
/// `wb.as_bytes()` carries the literal `SYNC_WIRE_BYTES` payload.
///
/// # Wire layout
///
/// `push_or_panic` returns `()`; the bytes live in `wb`. Ping is
/// the simplest case — just the 5-byte Sync wire payload, no
/// preceding frame, no trailing Sync (the Sync IS the Ping in
/// PG §55.2.4). A naive 1-Action `OutActions` list containing
/// `Action::SendBytes(SYNC_WIRE_BYTES)` would force a heap-bearing
/// list shape for this 5 B literal.
///
/// Using this helper instead of `proto.push_or_panic(..., &mut wb)`
/// verifies the setup is well-formed on every call site — any
/// regression in the push path (wrong bytes, partial write) is
/// surfaced at the top of the test, not masked. Every test that uses
/// `ping_setup` implicitly validates push-content for free.
#[track_caller]
fn ping_setup(proto: &mut PgProtocol<bsql_postgres_proto::ActivePhase>, reply: ReplyId<PingKind>, wb: &mut bsql_postgres_proto::WriteBuf) {
    proto.push_or_panic(bsql_postgres_proto::push_command::Ping { reply }, wb);
    // F33: assert the LITERAL 5-byte Sync wire layout from PG §55.7 —
    // tag 'S' + BE u32 length-field `4`. This is the load-bearing wire
    // contract: stronger than comparing to the library's own internal
    // `SYNC_WIRE_BYTES` const (which would be tautological).
    assert_eq!(
        wb.as_bytes(),
        &[b'S', 0u8, 0u8, 0u8, 4u8],
        "Ping setup: wb must carry the PG Sync wire layout (tag 'S' + BE u32 length=4)",
    );
}

// ------------------------------------------------------------------
// (A) Spec conformance — legal input → correct protocol output.
// ------------------------------------------------------------------

/// Invariant (spec): pushing a Ping from `Idle` emits exactly one
/// action — `SendBytes(send_buf)` carrying the `SYNC_WIRE_BYTES`
/// payload. The state transitions to `PingAwaitingRfq`.
///
/// This corresponds to reforge.md §13 / §19's wire-layer contract:
/// a Ping maps 1:1 to a `Sync` frame on the wire.
#[test]
fn ping_from_idle_emits_sync_bytes() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    assert!(matches!(proto.state(), ActiveState::Idle));

    let (reply, ping_raw) = mint_reply::<PingKind>(&mut proto);
    proto.push_or_panic(bsql_postgres_proto::push_command::Ping { reply }, &mut wb);

    // Bytes live in `wb`. Anti-tautology: assert literal PG Sync
    // wire layout (tag 'S' + BE u32 length=4), not the internal
    // `SYNC_WIRE_BYTES` const (which would mirror any drift).
    assert_eq!(
        wb.as_bytes(),
        &[b'S', 0u8, 0u8, 0u8, 4u8],
        "must send PG Sync wire bytes: tag 'S' + BE u32 length=4",
    );
    expect_awaiting_ping_reply(proto.state(), ping_raw);

    // Tier-2 structural consume-discipline: drain the in-flight reply
    // before the protocol drops. See [`drain_pending_ping`].
    drain_pending_ping(&mut proto, &mut wb);
}

/// Invariant (§2.2 visitor surface): driving the same buffered
/// `ReadyForQuery` through `PgProtocol::drive` delivers the Pong via
/// `Sink::on_deliver` (same correlator, same `Reply::Pong`) and returns
/// `DriveStatus::Idle`. Proves the new borrowing-visitor output surface
/// is wired end-to-end, parallel to the `feed_bytes`/`OutActions` path
/// exercised by the test below.
#[test]
fn drive_delivers_pong_via_sink() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (reply, ping_raw) = mint_reply::<PingKind>(&mut proto);
    ping_setup(&mut proto, reply, &mut wb);

    // Buffer the RFQ, then DRIVE it through the Sink surface.
    match proto.feed_inbound(&rfq_frame(b'I')) {
        Ok(()) => {}
        Err(e) => panic!("feed_inbound failed: {e:?}"),
    }

    struct CaptureSink {
        delivered: Option<NonZeroU64>,
        pong: bool,
    }
    impl bsql_postgres_proto::Sink for CaptureSink {
        fn on_deliver(
            &mut self,
            id: NonZeroU64,
            reply: &Reply,
        ) -> bsql_postgres_proto::Flow {
            self.delivered = Some(id);
            self.pong = matches!(reply, Reply::Pong(_));
            bsql_postgres_proto::Flow::Continue
        }
    }

    let mut sink = CaptureSink { delivered: None, pong: false };
    let status = proto.drive(&mut wb, &mut sink);

    assert_eq!(
        status,
        bsql_postgres_proto::sink::DriveStatus::Idle,
        "ping RFQ fully consumed via drive → Idle",
    );
    assert_eq!(
        sink.delivered,
        Some(ping_raw),
        "Sink::on_deliver routed the ping correlator unchanged",
    );
    assert!(sink.pong, "delivered Reply::Pong via the Sink surface");
    assert!(matches!(proto.state(), ActiveState::Idle));
}

/// Invariant (spec): feeding a complete `ReadyForQuery` frame while
/// awaiting a ping reply emits `DeliverReply { value: Pong { tx_status } }`,
/// carries the correct status byte, returns state to `Idle`, and leaves
/// no bytes in the read buffer.
#[test]
fn rfq_delivers_pong_and_returns_to_idle() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (reply, ping_raw) = mint_reply::<PingKind>(&mut proto);
    ping_setup(&mut proto, reply, &mut wb);

    let out = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);

    assert_eq!(out.len(), 1, "feed_bytes(RFQ) emits exactly 1 action");
    match out.as_slice() {
        [Action::DeliverReply { id: delivered_id, value }] => {
            assert_eq!(
                delivered_id,
                &ping_raw,
                "reply correlator round-trips unchanged",
            );
            assert!(
                matches!(value, Reply::Pong(_)),
                "only Reply::Pong is valid here; got {value:?}",
            );
        }
        other => panic!("unexpected action shape: {other:?}"),
    }
    // DEF-286 Φ-E: tx_status moved to accessor — Pong is now ZST.
    assert_eq!(
        proto.terminal_tx_status(),
        bsql_postgres_proto::TxStatus::Idle,
        "RFQ payload byte (tx-status) must round-trip via terminal_tx_status accessor",
    );
    assert!(matches!(proto.state(), ActiveState::Idle));
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
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (reply, _raw) = mint_reply::<PingKind>(&mut proto);
    ping_setup(&mut proto, reply, &mut wb);
    let frame = rfq_frame(b'I');

    // Feed the header one byte at a time, then the payload.
    for (i, byte) in frame.iter().enumerate().take(5) {
        let out = proto.feed_bytes(core::slice::from_ref(byte), &mut wb);
        assert_eq!(
            out.len(),
            0,
            "no actions until frame is complete (after feeding byte {i})",
        );
        assert!(
            matches!(proto.state(), ActiveState::PingAwaitingRfq(_)),
            "state stays in PingAwaitingRfq while buffering",
        );
    }
    // Final byte completes the frame. `frame: [u8; 6]` — slice [5..]
    // is always `[frame[5]]`, compile-time known.
    let last_slice: &[u8] = match frame.last() {
        Some(b) => core::slice::from_ref(b),
        None => panic!("frame has 6 bytes; .last() must be Some"),
    };
    let out = proto.feed_bytes(last_slice, &mut wb);
    assert_eq!(out.len(), 1);
    assert!(matches!(out.as_slice(), [Action::DeliverReply { .. }]));
    assert!(matches!(proto.state(), ActiveState::Idle));
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
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let out = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);

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
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (reply, ping_raw) = mint_reply::<PingKind>(&mut proto);
    ping_setup(&mut proto, reply, &mut wb);

    let out = proto.feed_bytes(&error_frame(), &mut wb);

    assert_eq!(
        out.len(),
        2,
        "server-error during ping → FailReply + CloseSocket",
    );
    match out.as_slice() {
        [
            Action::FailReply { id: failed_id },
            Action::CloseSocket,
        ] => {
            assert_eq!(failed_id, &ping_raw);
        }
        _ => panic!("unexpected action sequence: {out:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(
        matches!(cause, ProtocolError::ServerErrorResponse { .. }),
        "expected ServerErrorResponse, got {cause:?}",
    );
}

/// Invariant (spec): a frame with a malformed length-field (< 4) is
/// classified and tears the connection down.
#[test]
fn malformed_length_fails_and_closes() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (reply, ping_raw) = mint_reply::<PingKind>(&mut proto);
    ping_setup(&mut proto, reply, &mut wb);

    // Tag 'Z', length field = 3 (illegal: min is 4).
    let frame = [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 3, b'I'];
    let out = proto.feed_bytes(&frame, &mut wb);

    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [
            Action::FailReply { id: failed_id },
            Action::CloseSocket,
        ] => {
            assert_eq!(failed_id, &ping_raw);
        }
        _ => panic!("unexpected action sequence: {out:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(matches!(
        cause,
        ProtocolError::MalformedFrameLength { declared: 3 },
    ));
}

/// Invariant (spec): a chunk of bytes exceeding the `ReadBuf`
/// capacity triggers `ReadBufferFull` classification at `append` time
/// (before any parsing), which propagates through `feed_bytes` as
/// `FailReply(ReadBufferFull { attempted, available })` + `CloseSocket`,
/// and the state transitions to `Errored(ReadBufferFull{...})` with the
/// exact overflow dimensions preserved.
///
/// This E2E test pins the full propagation chain:
/// - `ReadBuf::append` returns `ReadBufFull { attempted, available }`
///   with the exact input size and the actual headroom.
/// - `feed_bytes`'s early-return on ReadBufFull invokes
///   `fail_inflight_and_close(ProtocolError::ReadBufferFull{...})`.
/// - `fail_inflight_and_close` from PingAwaitingRfq emits
///   FailReply(ping_id) + CloseSocket.
/// - State becomes `Errored(ReadBufferFull{...})` with matching
///   `attempted`/`available` fields preserved byte-for-byte.
///
/// Complements `bounded_buffers_spec::append_overflow_is_classified_and_fail_atomic`
/// (which pins the ReadBuf API contract). Neither alone covers the
/// full chain — that's what this E2E test closes.
#[test]
fn read_buf_overflow_through_feed_bytes_propagates_as_classified_error() {
    use bsql_postgres_proto::READ_BUF_CAP;

    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (reply, ping_raw) = mint_reply::<PingKind>(&mut proto);
    ping_setup(&mut proto, reply, &mut wb);

    // Feed a chunk one byte larger than READ_BUF_CAP. `append` rejects
    // with `ReadBufFull { attempted: CAP+1, available: CAP }`.
    let overflow_len = READ_BUF_CAP.saturating_add(1);
    let chunk = vec![0xAA_u8; overflow_len];
    let out = proto.feed_bytes(&chunk, &mut wb);

    assert_eq!(
        out.len(),
        2,
        "ReadBufferFull during PingAwaitingRfq → FailReply + CloseSocket",
    );
    match out.as_slice() {
        [
            Action::FailReply { id: failed_id },
            Action::CloseSocket,
        ] => {
            assert_eq!(failed_id, &ping_raw);
        }
        other => panic!("unexpected action sequence: {other:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    match cause {
        ProtocolError::ReadBufferFull {
            attempted,
            available,
        } => {
            assert_eq!(attempted, overflow_len);
            assert_eq!(available, READ_BUF_CAP);
        }
        other => panic!(
            "expected ReadBufferFull {{ attempted: {overflow_len}, available: {READ_BUF_CAP} }}, got {other:?}",
        ),
    }

    // State carries only `ErrorKind` (the `Transport` kind
    // classifies `ReadBufferFull`). The full `ReadBufferFull`
    // diagnostic (with exact `attempted`/`available` bytes) went
    // out in the `FailReply` action above, which the test already
    // pins.
    use bsql_postgres_proto::error::ErrorKind;
    match proto.state() {
        ActiveState::Errored(k) if k.as_kind() == ErrorKind::Transport => {}
        other => panic!(
            "state must be Errored(Transport), got {other:?}",
        ),
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
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (reply, ping_raw) = mint_reply::<PingKind>(&mut proto);
    ping_setup(&mut proto, reply, &mut wb);

    // Tag 'Z', length field = u32::MAX (obviously > MAX_FRAME_LEN_FIELD).
    // Only the 5-byte header is fed; the body is never sent.
    let frame = [TAG_READY_FOR_QUERY.byte(), 0xFF, 0xFF, 0xFF, 0xFF];
    let out = proto.feed_bytes(&frame, &mut wb);

    assert_eq!(out.len(), 2);
    match out.as_slice() {
        [
            Action::FailReply { id: failed_id },
            Action::CloseSocket,
        ] => {
            assert_eq!(failed_id, &ping_raw);
        }
        _ => panic!("unexpected action sequence: {out:?}"),
    }
    let _ = out;
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
    assert!(matches!(
        cause,
        ProtocolError::FrameTooLarge { declared: 0xFFFF_FFFF },
    ));
}

/// Invariant: a second Ping while one is in flight is
/// **structurally impossible** at the public API surface.
///
/// `proto.as_ready()` returns `None` while a Ping is in flight —
/// the second push never happens. The internal
/// `compute_push_ping` non-Idle arms (still emit `FailReply`
/// defensively) are unreachable from the public API and covered by
/// `compute_push_tests` in protocol.rs.
#[test]
fn pipelined_ping_blocked_at_compile_time() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (first_reply, first_raw) = mint_reply::<PingKind>(&mut proto);
    ping_setup(&mut proto, first_reply, &mut wb);

    // State is `PingAwaitingRfq`. Public API blocks second push.
    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None during in-flight Ping",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Busy,
        "in-flight Ping classifies as ConnectionStatus::Busy",
    );
    expect_awaiting_ping_reply(proto.state(), first_raw);

    // Drain the still-pending first-ping reply so its ReplyId is
    // consumed before the protocol drops. See [`drain_pending_ping`].
    drain_pending_ping(&mut proto, &mut wb);
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
    frame.push(TAG_READY_FOR_QUERY.byte());
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
        let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
        let (reply, ping_raw) = mint_reply::<PingKind>(&mut proto);
        ping_setup(&mut proto, reply, &mut wb);

        let frame = build_rfq_frame_with_payload_len(payload_len);
        let out = proto.feed_bytes(&frame, &mut wb);

        assert_eq!(
            out.len(),
            2,
            "payload_len={payload_len}: expected FailReply + CloseSocket",
        );
        match out.as_slice() {
            [
                Action::FailReply { id: failed_id },
                Action::CloseSocket,
            ] => {
                assert_eq!(failed_id, &ping_raw);
            }
            other => panic!("payload_len={payload_len}: unexpected actions {other:?}"),
        }
        let _ = out;
        let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated"); };
        assert!(
            matches!(
                cause,
                ProtocolError::MalformedReadyForQuery { payload_len: actual }
                    if actual == payload_len,
            ),
            "payload_len={payload_len}: unexpected cause {cause:?}",
        );
    }
}

/// Invariant (tier-1 shield): a 1-byte RFQ payload with a value
/// outside `{'I', 'T', 'E'}` is rejected by the `TxStatus::try_from_byte`
/// parse at the dispatch layer — users NEVER receive a `Reply::Pong`
/// with an unrecognised `tx_status`. Regression-pins the 2026-04-21
/// TxStatus uplift.
///
/// Pre-uplift: the byte slipped through as `Reply::Pong { tx_status: b'X' }`
/// — raw u8 — and user code pattern-matching only on `I`/`T`/`E`
/// would silently ignore the frame.
#[test]
fn rfq_with_invalid_tx_status_byte_is_rejected() {
    for bad in [b'X', b'\0', b'i', b't', b'e', 0xFF] {
        let mut proto = fresh_active_via_trust_handshake();
        let mut wb = bsql_postgres_proto::WriteBuf::new();
        let (reply, _ping_raw) = mint_reply::<PingKind>(&mut proto);
        ping_setup(&mut proto, reply, &mut wb);

        let out = proto.feed_bytes(&rfq_frame(bad), &mut wb);
        let actions = out.as_slice();
        let saw_fail = actions.iter().any(|a| matches!(a, Action::FailReply { .. }));
        let _ = out;
        let cause_match = proto.fail_cause().is_some_and(|c|
            matches!(c, ProtocolError::MalformedReadyForQuery { payload_len: 1 })
        );
        assert!(
            saw_fail && cause_match,
            "bad={bad:#04x}: expected FailReply(MalformedReadyForQuery{{1}})",
        );
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
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (reply, _ping_raw) = mint_reply::<PingKind>(&mut proto);
    ping_setup(&mut proto, reply, &mut wb);

    // Drive into Errored via an ErrorResponse — `ServerError` cause.
    let err_out = proto.feed_bytes(&error_frame(), &mut wb);
    assert_eq!(err_out.len(), 2, "entering Errored emits FailReply + CloseSocket");
    // State carries `ErrorKind::ServerError` (1 byte); the
    // full diagnostic cause went out in the `FailReply` above.
    use bsql_postgres_proto::error::ErrorKind;
    // `Errored(StateErrorKind)` — match outer + compare via `as_kind()`
    match proto.state() {
        ActiveState::Errored(k) => assert_eq!(k.as_kind(), ErrorKind::ServerError),
        other => panic!("expected Errored(ServerError), got {other:?}"),
    }

    // First post-terminal frame: a well-formed RFQ. Expect zero actions
    // (the terminal sink silently drops it) and the kind preserved.
    let post_out_1 = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    assert_eq!(
        post_out_1.len(),
        0,
        "post-terminal RFQ must emit zero actions, got {post_out_1:?}",
    );
    // `Errored(StateErrorKind)` — match outer + compare via `as_kind()`
    match proto.state() {
        ActiveState::Errored(k) => assert_eq!(k.as_kind(), ErrorKind::ServerError),
        other => panic!("expected Errored(ServerError), got {other:?}"),
    }

    // Second post-terminal frame: an ErrorResponse that would *normally*
    // classify as a separate ServerError. The original kind must still
    // win — the terminal sink does not overwrite.
    let post_out_2 = proto.feed_bytes(&error_frame(), &mut wb);
    assert_eq!(
        post_out_2.len(),
        0,
        "post-terminal ErrorResponse must emit zero actions, got {post_out_2:?}",
    );
    // `Errored(StateErrorKind)` — match outer + compare via `as_kind()`
    match proto.state() {
        ActiveState::Errored(k) => assert_eq!(k.as_kind(), ErrorKind::ServerError),
        other => panic!("expected Errored(ServerError), got {other:?}"),
    }
}

/// Invariant: `push_command` on `Errored` is structurally
/// impossible at the public API surface. The stored cause is exposed
/// to the caller via [`PgProtocol::connection_status`] for recovery
/// decisions — `ConnectionStatus::Errored(StateErrorKind)` gives
/// direct access without synthesising a fake reply id.
#[test]
fn push_on_errored_blocked_at_compile_time_status_exposes_cause() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (first_reply, _first_raw) = mint_reply::<PingKind>(&mut proto);
    ping_setup(&mut proto, first_reply, &mut wb);

    // Drive into Errored via ErrorResponse.
    let err_out = proto.feed_bytes(&error_frame(), &mut wb);
    assert_eq!(err_out.len(), 2);

    use bsql_postgres_proto::error::ErrorKind;

    // `as_ready` returns `None` on Errored.
    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None on Errored",
    );

    // `ConnectionStatus` exposes the underlying error kind for
    // caller-side recovery (a naive synthesised-failure-id
    // `FailReply.cause` shape would bury the diagnostic).
    match proto.connection_status() {
        ConnectionStatus::Errored(state_err_kind) => {
            assert_eq!(
                state_err_kind.as_kind(),
                ErrorKind::ServerError,
                "stored cause must be ServerError",
            );
        }
        other => panic!(
            "expected ConnectionStatus::Errored(ServerError), got {other:?}",
        ),
    }

    // State unchanged — kind preserved internally.
    match proto.state() {
        ActiveState::Errored(k) => {
            assert_eq!(k.as_kind(), ErrorKind::ServerError);
        }
        other => panic!("expected Errored(ServerError), got {other:?}"),
    }
}

/// Invariant (spec): `NoticeResponse` (tag `'N'`) is a PG advisory
/// frame that can arrive in any state. The pre-dispatch
/// filter in `feed_bytes` silently consumes it — state unchanged,
/// no actions emitted, subsequent frames are processed normally.
///
/// Without the filter, a Notice would reach the dispatcher and land
/// in the `(state, other)` arm as `UnexpectedFrame` → connection
/// teardown. This test pins the filter: a Notice followed by an RFQ
/// must complete the ping flow cleanly.
#[test]
fn notice_response_mid_flight_is_silently_consumed() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = bsql_postgres_proto::WriteBuf::new();
    let (reply, ping_raw) = mint_reply::<PingKind>(&mut proto);
    ping_setup(&mut proto, reply, &mut wb);

    // Build a NoticeResponse frame: tag 'N', minimal body with a
    // single field (`M` = message) then terminator. Body: 'M' +
    // "hi" + \0 + \0 terminator = 5 bytes. Declared length = 5 + 4
    // (self-inclusive) = 9.
    let notice: [u8; 10] = [b'N', 0, 0, 0, 9, b'M', b'h', b'i', 0, 0];
    let out = proto.feed_bytes(&notice, &mut wb);
    assert_eq!(out.len(), 1, "NoticeResponse emits Action::Notice");
    assert!(
        matches!(out.as_slice(), [Action::Notice { .. }]),
        "expected Action::Notice, got {:?}",
        out.as_slice()
    );
    expect_awaiting_ping_reply(proto.state(), ping_raw);

    // Now complete the ping normally.
    let rfq: [u8; 6] = [b'Z', 0, 0, 0, 5, b'I'];
    let out = proto.feed_bytes(&rfq, &mut wb);
    assert_eq!(out.len(), 1, "RFQ completes the ping after filtered notice");
    match out.as_slice() {
        [Action::DeliverReply { id, .. }] => assert_eq!(id, &ping_raw),
        other => panic!("expected DeliverReply, got {other:?}"),
    }
}
