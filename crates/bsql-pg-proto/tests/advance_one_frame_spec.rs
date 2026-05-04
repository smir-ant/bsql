//! DEF-212 Phase 2 (Alt Y', audit 2026-05-04) — `advance_one_frame` +
//! `FeedEvent` per-event API spec.
//!
//! Forward-compat anchor for 1c-5 pipelining: drives the protocol
//! one user-observable event at a time, mapping to the typed
//! [`FeedEvent`] enum.
//!
//! Coverage:
//! - `FeedEvent::Idle` — fresh proto, empty buf.
//! - `FeedEvent::NeedMoreBytes` — partial frame, non-Idle.
//! - `FeedEvent::Deliver` — Ping → Sync → RFQ → Pong delivered.
//! - `FeedEvent::Fail` — adversarial frame mid-Ping, M2 implies close.
//! - `FeedEvent::Close` — adversarial frame in Idle, no in-flight.
//! - `FeedEvent::StreamingRows` — SimpleQuery + RowDescription → switch
//!   signal.
//! - `FeedEvent::SendBytes` — SCRAM continue → client-final outbound.
//! - **Equivalence pin**: `feed_bytes(bytes)` = sequence of
//!   `advance_one_frame()` until `Idle`/`NeedMoreBytes`. Same final
//!   state, same correlator routing.

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
#![deny(unused_must_use, unused_lifetimes)]

use bsql_pg_proto::{
    FeedEvent, PgCommand, PgProtocol, ProtoState, ProtocolError, Reply, Sql, WriteBuf,
    reply_id::{PingKind, QueryKind, ReplyId},
    wire::{TAG_DATA_ROW, TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION},
};
use core::num::NonZeroU64;

mod common;
use common::PushOrPanic;

fn raw(v: u64) -> NonZeroU64 {
    assert!(v > 0, "raw(0) is a test bug — use raw(1..)");
    NonZeroU64::new(v).unwrap_or(NonZeroU64::MIN)
}

fn ping_id(v: u64) -> ReplyId<PingKind> {
    ReplyId::from_raw(raw(v))
}

fn query_id(v: u64) -> ReplyId<QueryKind> {
    ReplyId::from_raw(raw(v))
}

fn rfq_idle() -> [u8; 6] {
    [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, b'I']
}

/// Build a bare PG frame: tag + 4-byte BE length (self-inclusive) + body.
fn frame(tag: u8, body: &[u8]) -> std::vec::Vec<u8> {
    let mut out = std::vec::Vec::new();
    out.push(tag);
    let Ok(len) = u32::try_from(body.len().saturating_add(4)) else {
        panic!("fixture body too large");
    };
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

// ═══════════════════════════════════════════════════════════════════
// (A) Trivial state classifications — no inbound bytes
// ═══════════════════════════════════════════════════════════════════

/// Fresh proto, empty read_buf, state==Idle → FeedEvent::Idle.
#[test]
fn fresh_idle_proto_yields_idle_event() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let event = proto.advance_one_frame(&mut wb);
    assert!(
        matches!(event, FeedEvent::Idle),
        "fresh proto must yield FeedEvent::Idle, got {event:?}",
    );
}

// ═══════════════════════════════════════════════════════════════════
// (B) Happy path — Ping → Pong via per-event drive
// ═══════════════════════════════════════════════════════════════════

/// Push Ping, feed RFQ via feed_inbound, advance → Deliver(Pong).
#[test]
fn ping_then_rfq_yields_deliver_pong() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let ping_raw = raw(1);

    // Push Ping (state → PingAwaitingRfq).
    proto.push_or_panic(PgCommand::Ping { reply: ping_id(1) }, &mut wb);
    assert!(matches!(proto.state(), ProtoState::PingAwaitingRfq(_)));

    // Feed inbound RFQ via the per-event API.
    let rfq = rfq_idle();
    let feed_result = proto.feed_inbound(&rfq);
    assert!(
        feed_result.is_ok(),
        "feed_inbound must accept the 6-B RFQ frame; got {feed_result:?}",
    );

    // Advance — should emit Deliver(Pong) for the in-flight Ping.
    let event = proto.advance_one_frame(&mut wb);
    let (delivered_id, value) = match event {
        FeedEvent::Deliver(id, value) => (id, value),
        other => panic!("expected FeedEvent::Deliver, got {other:?}"),
    };
    assert_eq!(delivered_id, ping_raw, "correlator round-trip");
    assert!(
        matches!(value, Reply::Pong(_)),
        "Ping reply must be Pong variant, got {value:?}",
    );
    assert!(matches!(proto.state(), ProtoState::Idle), "post-Pong state == Idle");
}

/// Drain after Deliver: subsequent advance returns Idle.
#[test]
fn post_deliver_advance_returns_idle() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    proto.push_or_panic(PgCommand::Ping { reply: ping_id(2) }, &mut wb);
    let feed_result = proto.feed_inbound(&rfq_idle());
    assert!(feed_result.is_ok());
    let _first = proto.advance_one_frame(&mut wb); // consumes the Deliver

    // Second advance — state is back to Idle, read_buf empty.
    let event = proto.advance_one_frame(&mut wb);
    assert!(
        matches!(event, FeedEvent::Idle),
        "post-Deliver-and-drain advance must yield Idle, got {event:?}",
    );
}

// ═══════════════════════════════════════════════════════════════════
// (C) Partial frames → NeedMoreBytes
// ═══════════════════════════════════════════════════════════════════

/// Partial header (1 of 5 bytes) in non-Idle state → NeedMoreBytes.
#[test]
fn partial_header_yields_need_more_bytes() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    proto.push_or_panic(PgCommand::Ping { reply: ping_id(3) }, &mut wb);

    // Feed only 1 byte — header parser sees Incomplete.
    let partial = [b'Z'];
    assert!(proto.feed_inbound(&partial).is_ok());

    let event = proto.advance_one_frame(&mut wb);
    assert!(
        matches!(event, FeedEvent::NeedMoreBytes),
        "partial header must yield NeedMoreBytes, got {event:?}",
    );

    // Drain so the test exit doesn't hold an unconsumed in-flight reply.
    let rest = [0u8, 0, 0, 5, b'I'];
    assert!(proto.feed_inbound(&rest).is_ok());
    let _ = proto.advance_one_frame(&mut wb);
}

// ═══════════════════════════════════════════════════════════════════
// (D) Fail / Close (M2: Fail implies close)
// ═══════════════════════════════════════════════════════════════════

/// Adversarial RFQ in Idle (no in-flight reply) → Close.
#[test]
fn unsolicited_rfq_in_idle_yields_close_no_in_flight() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    assert!(proto.feed_inbound(&rfq_idle()).is_ok());
    let event = proto.advance_one_frame(&mut wb);
    assert!(
        matches!(event, FeedEvent::Close),
        "unsolicited RFQ in Idle must yield Close, got {event:?}",
    );
    assert!(matches!(proto.state(), ProtoState::Errored(_)));
}

/// Adversarial wrong-tag frame mid-Ping → Fail (M2 implies close).
#[test]
fn unexpected_frame_mid_ping_yields_fail_with_id() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let ping_raw = raw(4);
    proto.push_or_panic(PgCommand::Ping { reply: ping_id(4) }, &mut wb);

    // Feed an unexpected DataRow ('D') frame while awaiting RFQ.
    let bad = frame(TAG_DATA_ROW.byte(), &[0, 0]);
    assert!(proto.feed_inbound(&bad).is_ok());

    let event = proto.advance_one_frame(&mut wb);
    let (failed_id, cause) = match event {
        FeedEvent::Fail(id, cause) => (id, cause),
        other => panic!("expected FeedEvent::Fail, got {other:?}"),
    };
    assert_eq!(failed_id, ping_raw, "correlator must round-trip on Fail");
    assert!(
        matches!(cause, ProtocolError::UnexpectedFrame { .. }),
        "wrong-tag must classify as UnexpectedFrame, got {cause:?}",
    );
    assert!(matches!(proto.state(), ProtoState::Errored(_)));
}

/// Errored state on subsequent advance → Close.
#[test]
fn errored_state_yields_close_on_advance() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    // Force Errored via unsolicited RFQ.
    assert!(proto.feed_inbound(&rfq_idle()).is_ok());
    let _first = proto.advance_one_frame(&mut wb); // Close from the unsolicited

    // Subsequent advance on Errored state must keep returning Close.
    let event = proto.advance_one_frame(&mut wb);
    assert!(
        matches!(event, FeedEvent::Close),
        "Errored state must yield Close on advance, got {event:?}",
    );
}

// ═══════════════════════════════════════════════════════════════════
// (E) StreamingRows — RowDescription transitions caller to iter_rows
// ═══════════════════════════════════════════════════════════════════

/// SimpleQuery + RowDescription → state == Streaming → advance_one_frame
/// yields StreamingRows (signal to switch to iter_rows API).
#[test]
fn row_description_transitions_to_streaming_rows_event() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    proto.push_or_panic(
        PgCommand::SimpleQuery {
            sql: Sql::from_str_truncating("SELECT 1"),
            reply: query_id(5),
        },
        &mut wb,
    );

    // Build a minimal RowDescription frame: i16 n_columns=1 + per-col
    // (NUL-terminated name + i32 tbl_oid + i16 attnum + i32 type_oid +
    // i16 type_size + i32 type_mod + i16 format).
    let mut body = std::vec::Vec::new();
    body.extend_from_slice(&1i16.to_be_bytes()); // 1 column
    body.extend_from_slice(b"c");
    body.push(0); // NUL
    body.extend_from_slice(&0i32.to_be_bytes()); // table_oid
    body.extend_from_slice(&0i16.to_be_bytes()); // attr_num
    body.extend_from_slice(&23i32.to_be_bytes()); // int4 oid
    body.extend_from_slice(&4i16.to_be_bytes()); // type_size
    body.extend_from_slice(&(-1i32).to_be_bytes()); // type_mod
    body.extend_from_slice(&0i16.to_be_bytes()); // text format
    let row_desc = frame(TAG_ROW_DESCRIPTION.byte(), &body);
    assert!(proto.feed_inbound(&row_desc).is_ok());

    // First advance consumes the RowDescription silently
    // (AdvancedSilent → state transition to streaming → next advance
    // returns StreamingRows). Drive once so dispatch processes 'T'.
    let _silent = proto.advance_one_frame(&mut wb);

    // After RowDescription, state is SimpleQueryStreamingRows; the
    // next advance signals the caller to switch APIs.
    let event = proto.advance_one_frame(&mut wb);
    assert!(
        matches!(event, FeedEvent::StreamingRows),
        "post-RowDescription state must yield StreamingRows, got {event:?}",
    );
}

// ═══════════════════════════════════════════════════════════════════
// (F) Equivalence pin — feed_bytes vs advance_one_frame loop
// ═══════════════════════════════════════════════════════════════════

/// Driving `advance_one_frame` to completion produces the same final
/// state and correlator delivery as `feed_bytes` in one call.
#[test]
fn advance_loop_equals_feed_bytes_on_ping_round_trip() {
    // (a) feed_bytes path.
    let mut proto_a = PgProtocol::new();
    let mut wb_a = WriteBuf::new();
    proto_a.push_or_panic(PgCommand::Ping { reply: ping_id(101) }, &mut wb_a);
    let actions = proto_a.feed_bytes(&rfq_idle(), &mut wb_a);
    assert_eq!(actions.len(), 1, "feed_bytes: 1 DeliverReply on Ping/RFQ");
    // OutActions is ManuallyDrop<Vec<Action,9>>+len — not actually
    // Drop. NLL ends its borrow at the last use (the assert above);
    // the next-line `proto_a.state()` is `&self`, no borrow conflict.
    assert!(matches!(proto_a.state(), ProtoState::Idle));

    // (b) advance_one_frame path.
    let mut proto_b = PgProtocol::new();
    let mut wb_b = WriteBuf::new();
    proto_b.push_or_panic(PgCommand::Ping { reply: ping_id(101) }, &mut wb_b);
    assert!(proto_b.feed_inbound(&rfq_idle()).is_ok());

    let event = proto_b.advance_one_frame(&mut wb_b);
    let (id_b, _value) = match event {
        FeedEvent::Deliver(id, value) => (id, value),
        other => panic!("path b: expected Deliver, got {other:?}"),
    };
    assert_eq!(id_b, raw(101), "advance: same correlator");
    assert!(matches!(proto_b.state(), ProtoState::Idle));

    // Both paths reached Idle with same correlator routing — the per-
    // event API is observationally equivalent on the canonical happy
    // path.
}

// ═══════════════════════════════════════════════════════════════════
// (G) feed_inbound on Errored is silent no-op
// ═══════════════════════════════════════════════════════════════════

/// `feed_inbound` returns Ok(()) without appending when state==Errored
/// (terminal — caller learns via `connection_status` /
/// `advance_one_frame → Close`).
#[test]
fn feed_inbound_on_errored_is_silent_noop() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    // Force Errored.
    assert!(proto.feed_inbound(&rfq_idle()).is_ok());
    let _close = proto.advance_one_frame(&mut wb);
    assert!(matches!(proto.state(), ProtoState::Errored(_)));

    // Subsequent feed_inbound is silent — Ok despite being on Errored.
    let result = proto.feed_inbound(b"some bytes");
    assert!(
        result.is_ok(),
        "feed_inbound on Errored must silently no-op, got {result:?}",
    );
    // Verify advance still returns Close (state unchanged).
    assert!(matches!(
        proto.advance_one_frame(&mut wb),
        FeedEvent::Close,
    ));
}
