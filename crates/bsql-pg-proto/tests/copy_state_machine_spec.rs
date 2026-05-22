//! DEF-219 Phase 2 — COPY state machine tests.
//!
//! Verifies the state transitions for COPY OUT and COPY IN sub-protocols
//! WITHOUT the Phase 3 data-emission surface. Tests assert that the
//! proto's `state()` advances correctly when COPY frames are fed.
//!
//! Phase 2 stays SILENT on `CopyData` (no Action emission); the bytes
//! are consumed but not surfaced. Phase 3 will add
//! `Action::CopyDataChunk` and the integration tests for the data
//! surface.

#![forbid(unsafe_code)]

use bsql_pg_proto::{
    ActiveState, Reply, WriteBuf,
    push_command::SimpleQuery,
    wire::{
        TAG_COMMAND_COMPLETE, TAG_COPY_DATA, TAG_COPY_DONE, TAG_COPY_IN_RESPONSE,
        TAG_COPY_OUT_RESPONSE, TAG_READY_FOR_QUERY,
    },
};

mod common;
use common::{PushOrPanic, fresh_active_via_trust_handshake, mint_reply};

fn copy_response_frame(tag: u8, format: u8, n_cols: u16) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.push(format);
    body.extend_from_slice(&(i16::try_from(n_cols).unwrap_or(0)).to_be_bytes());
    let code_as_i16 = i16::from(format);
    for _ in 0..n_cols {
        body.extend_from_slice(&code_as_i16.to_be_bytes());
    }
    let Ok(body_len) = u32::try_from(body.len().saturating_add(4)) else {
        return std::vec::Vec::new();
    };
    let mut frame = std::vec::Vec::new();
    frame.push(tag);
    frame.extend_from_slice(&body_len.to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn copy_data_frame(payload: &[u8]) -> std::vec::Vec<u8> {
    let Ok(body_len) = u32::try_from(payload.len().saturating_add(4)) else {
        return std::vec::Vec::new();
    };
    let mut frame = std::vec::Vec::new();
    frame.push(TAG_COPY_DATA.byte());
    frame.extend_from_slice(&body_len.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn copy_done_frame() -> [u8; 5] {
    [TAG_COPY_DONE.byte(), 0, 0, 0, 4]
}

fn command_complete_frame(tag: &[u8]) -> std::vec::Vec<u8> {
    let mut body = tag.to_vec();
    body.push(0);
    let Ok(body_len) = u32::try_from(body.len().saturating_add(4)) else {
        return std::vec::Vec::new();
    };
    let mut frame = std::vec::Vec::new();
    frame.push(TAG_COMMAND_COMPLETE.byte());
    frame.extend_from_slice(&body_len.to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn rfq_frame(tx_status: u8) -> [u8; 6] {
    [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, tx_status]
}

#[test]
fn copy_out_full_cycle_state_transitions() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_pg_proto::QueryKind>(&mut proto);

    proto.push_or_panic(
        SimpleQuery {
            sql: "COPY users TO STDOUT",
            reply,
        },
        &mut wb,
    );

    // Step 1: server sends CopyOutResponse — state transitions to
    // SimpleQueryCopyOutStreaming.
    let frame_h = copy_response_frame(TAG_COPY_OUT_RESPONSE.byte(), 0, 2);
    let _actions = proto.feed_bytes(&frame_h, &mut wb);
    assert!(
        matches!(proto.state(), ActiveState::SimpleQueryCopyOutStreaming(_)),
        "post-H: expected CopyOutStreaming, got {:?}",
        proto.state()
    );

    // Step 2: 2× CopyData frames — state stays in CopyOutStreaming.
    let frame_d = copy_data_frame(b"row1\trow1col2\n");
    let _ = proto.feed_bytes(&frame_d, &mut wb);
    let _ = proto.feed_bytes(&frame_d, &mut wb);
    assert!(
        matches!(proto.state(), ActiveState::SimpleQueryCopyOutStreaming(_)),
        "post-D×2: still CopyOutStreaming"
    );

    // Step 3: server sends CopyDone — transition to CopyOutAwaitingCC.
    let _ = proto.feed_bytes(&copy_done_frame(), &mut wb);
    assert!(
        matches!(proto.state(), ActiveState::SimpleQueryCopyOutAwaitingCC(_)),
        "post-c: expected CopyOutAwaitingCC, got {:?}",
        proto.state()
    );

    // Step 4: CommandComplete + RFQ — transition to AwaitingRfq → Idle.
    let mut tail = std::vec::Vec::new();
    tail.extend(command_complete_frame(b"COPY 2"));
    tail.extend(rfq_frame(b'I'));
    let actions = proto.feed_bytes(&tail, &mut wb);
    let slice = actions.as_slice();
    // Final action is DeliverReply with command_tag "COPY 2".
    let last = slice.last().expect("at least one action");
    let bsql_pg_proto::Action::DeliverReply {
        value: Reply::QueryComplete(p),
        ..
    } = last
    else {
        panic!("expected final DeliverReply(QueryComplete); got {:?}", last);
    };
    assert_eq!(p.command_tag.as_str(), "COPY 2");
    assert!(matches!(proto.state(), ActiveState::Idle));
}

#[test]
fn copy_in_full_cycle_state_transitions() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_pg_proto::QueryKind>(&mut proto);

    proto.push_or_panic(
        SimpleQuery {
            sql: "COPY users FROM STDIN",
            reply,
        },
        &mut wb,
    );

    // Server sends CopyInResponse — state transitions to CopyInActive.
    let frame_g = copy_response_frame(TAG_COPY_IN_RESPONSE.byte(), 0, 2);
    let _ = proto.feed_bytes(&frame_g, &mut wb);
    assert!(
        matches!(proto.state(), ActiveState::SimpleQueryCopyInActive(_)),
        "post-G: expected CopyInActive, got {:?}",
        proto.state()
    );

    // Phase 4 will land the push API for client→server CopyData.
    // For Phase 2 we skip directly to the server's CommandComplete +
    // RFQ (simulating that client did the push + server accepted).
    let mut tail = std::vec::Vec::new();
    tail.extend(command_complete_frame(b"COPY 5"));
    tail.extend(rfq_frame(b'I'));
    let actions = proto.feed_bytes(&tail, &mut wb);
    let slice = actions.as_slice();
    let last = slice.last().expect("at least one action");
    let bsql_pg_proto::Action::DeliverReply {
        value: Reply::QueryComplete(p),
        ..
    } = last
    else {
        panic!("expected final DeliverReply(QueryComplete); got {:?}", last);
    };
    assert_eq!(p.command_tag.as_str(), "COPY 5");
    assert!(matches!(proto.state(), ActiveState::Idle));
}

#[test]
fn copy_out_rejects_malformed_response_header() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_pg_proto::QueryKind>(&mut proto);

    proto.push_or_panic(
        SimpleQuery {
            sql: "COPY users TO STDOUT",
            reply,
        },
        &mut wb,
    );

    // Build a malformed CopyOutResponse: format byte = 2 (invalid;
    // only 0 and 1 are allowed per PG §55.2.6).
    let frame_malformed = copy_response_frame(TAG_COPY_OUT_RESPONSE.byte(), 2, 0);
    let _ = proto.feed_bytes(&frame_malformed, &mut wb);

    // State should be Errored (Framing-class).
    assert!(
        matches!(proto.state(), ActiveState::Errored(_)),
        "malformed copy response must error; got {:?}",
        proto.state()
    );
}
