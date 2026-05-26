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

use bsql_postgres_proto::{
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

    let (reply, _raw) = mint_reply::<bsql_postgres_proto::QueryKind>(&mut proto);

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
    let Some(last) = slice.last() else { panic!("at least one action"); };
    let bsql_postgres_proto::Action::DeliverReply {
        value: Reply::QueryComplete(_),
        ..
    } = last
    else {
        panic!("expected final DeliverReply(QueryComplete); got {:?}", last);
    };
    let _ = actions;
    let Some(command_tag) = proto.current_command_tag() else { panic!("command_tag slot populated"); };
    assert_eq!(format!("{}", command_tag), "COPY 2");
    assert!(matches!(proto.state(), ActiveState::Idle));
}

#[test]
fn copy_in_full_cycle_state_transitions() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_postgres_proto::QueryKind>(&mut proto);

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
    let Some(last) = slice.last() else { panic!("at least one action"); };
    let bsql_postgres_proto::Action::DeliverReply {
        value: Reply::QueryComplete(_),
        ..
    } = last
    else {
        panic!("expected final DeliverReply(QueryComplete); got {:?}", last);
    };
    let _ = actions;
    let Some(command_tag) = proto.current_command_tag() else { panic!("command_tag slot populated"); };
    assert_eq!(format!("{}", command_tag), "COPY 5");
    assert!(matches!(proto.state(), ActiveState::Idle));
}

#[test]
fn copy_out_data_chunks_surface_via_action() {
    // DEF-219 Phase 3: CopyData frames must produce
    // Action::CopyDataChunk entries in OutActions; bytes resolvable
    // via PgProtocol::get_copy_chunk within the same cycle.
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_postgres_proto::QueryKind>(&mut proto);

    proto.push_or_panic(
        SimpleQuery {
            sql: "COPY users TO STDOUT",
            reply,
        },
        &mut wb,
    );

    // Push CopyOutResponse first (separately) so the chunks flow
    // arrives in a clean OutActions cycle.
    let frame_h = copy_response_frame(TAG_COPY_OUT_RESPONSE.byte(), 0, 2);
    let _ = proto.feed_bytes(&frame_h, &mut wb);

    // Now feed 3 CopyData frames in one call — expect 3
    // Action::CopyDataChunk entries.
    let mut bytes = std::vec::Vec::new();
    bytes.extend(copy_data_frame(b"row_a\tval_a\n"));
    bytes.extend(copy_data_frame(b"row_b\tval_b\n"));
    bytes.extend(copy_data_frame(b"row_c\tval_c\n"));

    let refs: std::vec::Vec<_> = {
        let actions = proto.feed_bytes(&bytes, &mut wb);
        let slice = actions.as_slice();
        assert_eq!(slice.len(), 3, "expected 3 CopyDataChunk actions; got {:?}", slice);
        let mut out = std::vec::Vec::new();
        for action in slice.iter() {
            if let bsql_postgres_proto::Action::CopyDataChunk { chunk_ref } = action {
                out.push(*chunk_ref);
            }
        }
        out
    };
    assert_eq!(refs.len(), 3);

    // Resolve each ref — but note: get_copy_chunk MUST be called
    // BEFORE the next feed_bytes (gen-bumped on cycle boundary).
    // Phase 3 docstring: refs valid within same OutActions cycle.
    // We resolve here, before any further feed_bytes calls.
    let expected: [&[u8]; 3] = [b"row_a\tval_a\n", b"row_b\tval_b\n", b"row_c\tval_c\n"];
    for (idx, r) in refs.iter().enumerate() {
        let res = proto.get_copy_chunk(*r);
        assert!(res.is_ok(), "ref {idx} must resolve in same cycle");
        let Ok(payload) = res else { return };
        assert_eq!(payload.bytes.as_slice(), expected[idx]);
    }
}

#[test]
fn copy_in_full_push_cycle() {
    // DEF-219 Phase 4: full COPY IN client-push cycle. push_copy_data
    // ×3, then push_copy_done, then verify server-response-side
    // transitions to Idle on CommandComplete + RFQ.
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_postgres_proto::QueryKind>(&mut proto);

    proto.push_or_panic(
        SimpleQuery {
            sql: "COPY users FROM STDIN",
            reply,
        },
        &mut wb,
    );

    // Server: CopyInResponse → state = CopyInActive.
    let frame_g = copy_response_frame(TAG_COPY_IN_RESPONSE.byte(), 0, 3);
    let _ = proto.feed_bytes(&frame_g, &mut wb);
    assert!(matches!(proto.state(), ActiveState::SimpleQueryCopyInActive(_)));

    // Push 3× CopyData frames. Use a separate WriteBuf for push
    // staging to keep frame slices live for inspection. (In a real
    // app the bytes are flushed to the socket between calls.)
    let mut push_wb = WriteBuf::new();
    let bytes_pushed_1 = {
        let res = proto.push_copy_data(b"row1\tval1\n", &mut push_wb);
        assert!(res.is_ok(), "push_copy_data must succeed in CopyInActive");
        let Ok(slice) = res else { return };
        slice.len()
    };
    assert!(bytes_pushed_1 > 0, "push wrote bytes");
    // State stays CopyInActive (no client-side transition).
    assert!(matches!(proto.state(), ActiveState::SimpleQueryCopyInActive(_)));

    let _ = proto.push_copy_data(b"row2\tval2\n", &mut push_wb);
    let _ = proto.push_copy_data(b"row3\tval3\n", &mut push_wb);
    assert!(matches!(proto.state(), ActiveState::SimpleQueryCopyInActive(_)));

    // CopyDone — still CopyInActive until server's CommandComplete.
    let done_res = proto.push_copy_done(&mut push_wb);
    assert!(done_res.is_ok());
    assert!(matches!(proto.state(), ActiveState::SimpleQueryCopyInActive(_)));

    // Server: CommandComplete + RFQ → state = Idle.
    let mut tail = std::vec::Vec::new();
    tail.extend(command_complete_frame(b"COPY 3"));
    tail.extend(rfq_frame(b'I'));
    let actions = proto.feed_bytes(&tail, &mut wb);
    let slice = actions.as_slice();
    let Some(last) = slice.last() else { panic!("at least one action"); };
    let bsql_postgres_proto::Action::DeliverReply {
        value: Reply::QueryComplete(_),
        ..
    } = last
    else {
        panic!("expected DeliverReply(QueryComplete); got {:?}", last);
    };
    let _ = actions;
    let Some(command_tag) = proto.current_command_tag() else { panic!("command_tag slot populated"); };
    assert_eq!(format!("{}", command_tag), "COPY 3");
    assert!(matches!(proto.state(), ActiveState::Idle));
}

#[test]
fn push_copy_data_outside_copy_in_state_errors() {
    // Calling push_copy_data when state is Idle (not CopyInActive)
    // must return CopyPushError::NotInCopyInState without writing.
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let pre_len = wb.len();
    let res = proto.push_copy_data(b"data", &mut wb);
    assert!(
        matches!(res, Err(bsql_postgres_proto::CopyPushError::NotInCopyInState)),
        "expected NotInCopyInState; got {:?}",
        res
    );
    assert_eq!(
        wb.len(),
        pre_len,
        "WriteBuf must NOT be written on rejected push"
    );
}

#[test]
fn push_copy_fail_rejects_embedded_nul() {
    // Set up CopyInActive state.
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _raw) = mint_reply::<bsql_postgres_proto::QueryKind>(&mut proto);
    proto.push_or_panic(
        SimpleQuery {
            sql: "COPY users FROM STDIN",
            reply,
        },
        &mut wb,
    );
    let frame_g = copy_response_frame(TAG_COPY_IN_RESPONSE.byte(), 0, 1);
    let _ = proto.feed_bytes(&frame_g, &mut wb);

    let mut push_wb = WriteBuf::new();
    let res = proto.push_copy_fail("err\0msg", &mut push_wb);
    assert!(
        matches!(res, Err(bsql_postgres_proto::CopyPushError::EmbeddedNul)),
        "expected EmbeddedNul; got {:?}",
        res
    );
}

#[test]
fn copy_out_rejects_malformed_response_header() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_postgres_proto::QueryKind>(&mut proto);

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

// ═════════════════════════════════════════════════════════════════
// COPY IN push-method error-arm coverage (DEF-286 session audit #6)
// ═════════════════════════════════════════════════════════════════

/// push_copy_data on Idle state (not CopyInActive) → NotInCopyInState.
#[test]
fn push_copy_data_not_in_copy_state_rejects() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let result = proto.push_copy_data(b"hello", &mut wb);
    assert!(
        matches!(result, Err(bsql_postgres_proto::CopyPushError::NotInCopyInState)),
        "push_copy_data on Idle must return NotInCopyInState, got {result:?}",
    );
}

/// push_copy_done on Idle state → NotInCopyInState.
#[test]
fn push_copy_done_not_in_copy_state_rejects() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let result = proto.push_copy_done(&mut wb);
    assert!(
        matches!(result, Err(bsql_postgres_proto::CopyPushError::NotInCopyInState)),
        "push_copy_done on Idle must return NotInCopyInState, got {result:?}",
    );
}

/// push_copy_fail on Idle state → NotInCopyInState.
#[test]
fn push_copy_fail_not_in_copy_state_rejects() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let result = proto.push_copy_fail("abort reason", &mut wb);
    assert!(
        matches!(result, Err(bsql_postgres_proto::CopyPushError::NotInCopyInState)),
        "push_copy_fail on Idle must return NotInCopyInState, got {result:?}",
    );
}

/// push_copy_fail with embedded NUL byte → EmbeddedNul.
#[test]
fn push_copy_fail_embedded_nul_rejects() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    // Enter CopyInActive state via server CopyInResponse.
    let (reply, _raw) = mint_reply::<bsql_postgres_proto::QueryKind>(&mut proto);
    proto.push_or_panic(
        SimpleQuery {
            sql: "COPY users FROM STDIN",
            reply,
        },
        &mut wb,
    );
    let gin = copy_response_frame(TAG_COPY_IN_RESPONSE.byte(), 0, 1);
    let _ = proto.feed_bytes(&gin, &mut wb);
    assert!(
        matches!(proto.state(), ActiveState::SimpleQueryCopyInActive(_)),
        "pre-condition: must be CopyInActive",
    );

    let result = proto.push_copy_fail("error\0embedded", &mut wb);
    assert!(
        matches!(result, Err(bsql_postgres_proto::CopyPushError::EmbeddedNul)),
        "push_copy_fail with NUL byte must return EmbeddedNul, got {result:?}",
    );
}

/// push_copy_done happy path — frame bytes in WriteBuf.
#[test]
fn push_copy_done_happy_path() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_postgres_proto::QueryKind>(&mut proto);
    proto.push_or_panic(
        SimpleQuery {
            sql: "COPY users FROM STDIN",
            reply,
        },
        &mut wb,
    );
    let gin = copy_response_frame(TAG_COPY_IN_RESPONSE.byte(), 0, 1);
    let _ = proto.feed_bytes(&gin, &mut wb);
    assert!(matches!(proto.state(), ActiveState::SimpleQueryCopyInActive(_)));

    let result = proto.push_copy_done(&mut wb);
    assert!(result.is_ok(), "push_copy_done must succeed in CopyInActive, got {result:?}");
    if let Ok(frame_bytes) = result {
        // CopyDone frame: tag 'c' + length 4 = 5 bytes total.
        assert_eq!(frame_bytes.len(), 5, "CopyDone frame must be 5 bytes");
    }
}

/// push_copy_fail happy path — frame includes NUL-terminated error message.
#[test]
fn push_copy_fail_happy_path() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_postgres_proto::QueryKind>(&mut proto);
    proto.push_or_panic(
        SimpleQuery {
            sql: "COPY users FROM STDIN",
            reply,
        },
        &mut wb,
    );
    let gin = copy_response_frame(TAG_COPY_IN_RESPONSE.byte(), 0, 1);
    let _ = proto.feed_bytes(&gin, &mut wb);
    assert!(matches!(proto.state(), ActiveState::SimpleQueryCopyInActive(_)));

    let result = proto.push_copy_fail("client abort", &mut wb);
    assert!(result.is_ok(), "push_copy_fail must succeed in CopyInActive, got {result:?}");
    if let Ok(frame_bytes) = result {
        // CopyFail frame: tag 'f' + length(4) + "client abort" + NUL
        // = 1 + 4 + 12 + 1 = 18 bytes total.
        assert_eq!(frame_bytes.len(), 18, "CopyFail frame length mismatch");
    }
}
