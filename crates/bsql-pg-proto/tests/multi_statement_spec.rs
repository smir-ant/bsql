//! DEF-226 multi-statement SimpleQuery batch tests.
//!
//! PG `Q` frame accepts `;`-separated batches like
//! `"BEGIN; UPDATE; UPDATE; COMMIT;"`. The server emits one
//! `CommandComplete` per statement followed by a single final
//! `ReadyForQuery`. Pre-DEF-226 the second CommandComplete arriving in
//! `SimpleQueryAwaitingRfq` hit `UnexpectedFrame` teardown; post-DEF-226
//! each non-final CommandComplete / RowDescription / EmptyQueryResponse
//! emits `Action::IntermediateCommandComplete` carrying the PRIOR
//! statement's tag.

#![forbid(unsafe_code)]

use bsql_pg_proto::{
    Action, Reply, WriteBuf,
    push_command::SimpleQuery,
    wire::{
        TAG_COMMAND_COMPLETE, TAG_EMPTY_QUERY_RESPONSE, TAG_READY_FOR_QUERY,
    },
};

mod common;
use common::{PushOrPanic, fresh_active_via_trust_handshake, mint_reply};

fn command_complete_frame(tag: &[u8]) -> std::vec::Vec<u8> {
    let mut body = tag.to_vec();
    body.push(0); // CSTR terminator
    let Ok(body_len) = u32::try_from(body.len().saturating_add(4)) else {
        return std::vec::Vec::new();
    };
    let mut frame = std::vec::Vec::new();
    frame.push(TAG_COMMAND_COMPLETE.byte());
    frame.extend_from_slice(&body_len.to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn empty_query_response_frame() -> [u8; 5] {
    [TAG_EMPTY_QUERY_RESPONSE.byte(), 0, 0, 0, 4]
}

fn rfq_frame(tx_status: u8) -> [u8; 6] {
    [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, tx_status]
}

#[test]
fn batch_two_dml_statements_surfaces_intermediate() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_pg_proto::QueryKind>(&mut proto);

    // Push a multi-statement query. The SQL bytes themselves are
    // opaque to the protocol (server interprets); we exercise the
    // RESPONSE-side state machine.
    proto.push_or_panic(
        SimpleQuery {
            sql: "BEGIN; COMMIT;",
            reply,
        },
        &mut wb,
    );

    // Server response: CommandComplete("BEGIN") +
    // CommandComplete("COMMIT") + RFQ.
    let mut server_bytes = std::vec::Vec::new();
    server_bytes.extend(command_complete_frame(b"BEGIN"));
    server_bytes.extend(command_complete_frame(b"COMMIT"));
    server_bytes.extend(rfq_frame(b'I'));

    let actions = proto.feed_bytes(&server_bytes, &mut wb);
    let slice = actions.as_slice();

    // Expect: IntermediateCommandComplete { "BEGIN" } + DeliverReply { "COMMIT" }
    assert_eq!(
        slice.len(),
        2,
        "expected 2 actions (intermediate + final); got {} = {:?}",
        slice.len(),
        slice
    );

    // DEF-286 Φ-D: ICC carries `tag_ref` (CommandTagRef, 4 B Copy).
    // Snapshot ref before dropping `actions` so `proto.get_command_tag`
    // can reborrow.
    let icc0_ref = match &slice[0] {
        Action::IntermediateCommandComplete { tag_ref } => *tag_ref,
        other => panic!("slot 0: expected IntermediateCommandComplete, got {other:?}"),
    };
    match &slice[1] {
        Action::DeliverReply {
            value: Reply::QueryComplete(_),
            ..
        } => {}
        other => panic!("slot 1: expected DeliverReply(QueryComplete), got {other:?}"),
    }
    // `actions` is `ManuallyDrop<heapless::Vec<_>>` — NLL releases
    // the &mut proto borrow at the last `&slice[…]` use above.
    let Some(command_tag) = proto.current_command_tag() else { panic!("command_tag slot populated"); };
    assert_eq!(format!("{}", command_tag), "COMMIT");
    let icc0_tag = proto
        .get_command_tag(icc0_ref)
        .unwrap_or_else(|e| panic!("ICC[0] resolve: {e:?}"));
    assert_eq!(format!("{}", icc0_tag), "BEGIN");
}

#[test]
fn batch_three_statements_surfaces_two_intermediates() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_pg_proto::QueryKind>(&mut proto);

    proto.push_or_panic(
        SimpleQuery {
            sql: "BEGIN; UPDATE t SET x=1; COMMIT;",
            reply,
        },
        &mut wb,
    );

    let mut server_bytes = std::vec::Vec::new();
    server_bytes.extend(command_complete_frame(b"BEGIN"));
    server_bytes.extend(command_complete_frame(b"UPDATE 1"));
    server_bytes.extend(command_complete_frame(b"COMMIT"));
    server_bytes.extend(rfq_frame(b'I'));

    let actions = proto.feed_bytes(&server_bytes, &mut wb);
    let slice = actions.as_slice();

    // Expect: 2 IntermediateCommandComplete + 1 DeliverReply.
    assert_eq!(
        slice.len(),
        3,
        "expected 3 actions (2 intermediates + final); got {:?}",
        slice
    );

    // DEF-286 Φ-D: snapshot tag_refs before dropping `actions`.
    let icc0_ref = match &slice[0] {
        Action::IntermediateCommandComplete { tag_ref } => *tag_ref,
        other => panic!("slot 0: expected Intermediate, got {other:?}"),
    };
    let icc1_ref = match &slice[1] {
        Action::IntermediateCommandComplete { tag_ref } => *tag_ref,
        other => panic!("slot 1: expected Intermediate, got {other:?}"),
    };
    let Action::DeliverReply {
        value: Reply::QueryComplete(_),
        ..
    } = &slice[2]
    else {
        panic!("slot 2: expected DeliverReply, got {:?}", slice[2]);
    };
    // NLL releases the &mut proto borrow after the last `slice` use
    // above; the subsequent accessor calls reborrow.
    let Some(command_tag) = proto.current_command_tag() else { panic!("command_tag slot populated"); };
    assert_eq!(format!("{}", command_tag), "COMMIT");
    let icc0_tag = proto.get_command_tag(icc0_ref).unwrap_or_else(|e| panic!("ICC[0]: {e:?}"));
    assert_eq!(format!("{}", icc0_tag), "BEGIN");
    let icc1_tag = proto.get_command_tag(icc1_ref).unwrap_or_else(|e| panic!("ICC[1]: {e:?}"));
    assert_eq!(format!("{}", icc1_tag), "UPDATE 1");
}

#[test]
fn batch_with_empty_query_response_in_middle() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_pg_proto::QueryKind>(&mut proto);

    // Batch with empty middle statement: `"BEGIN;;COMMIT;"` parses
    // as BEGIN + empty + COMMIT. Server emits CommandComplete("BEGIN")
    // + EmptyQueryResponse + CommandComplete("COMMIT") + RFQ.
    proto.push_or_panic(
        SimpleQuery {
            sql: "BEGIN;;COMMIT;",
            reply,
        },
        &mut wb,
    );

    let mut server_bytes = std::vec::Vec::new();
    server_bytes.extend(command_complete_frame(b"BEGIN"));
    server_bytes.extend(empty_query_response_frame());
    server_bytes.extend(command_complete_frame(b"COMMIT"));
    server_bytes.extend(rfq_frame(b'I'));

    let actions = proto.feed_bytes(&server_bytes, &mut wb);
    let slice = actions.as_slice();

    // Expect: Intermediate("BEGIN") + Intermediate("") + DeliverReply("COMMIT")
    assert_eq!(
        slice.len(),
        3,
        "expected 3 actions; got {:?}",
        slice
    );

    let icc0_ref = match &slice[0] {
        Action::IntermediateCommandComplete { tag_ref } => *tag_ref,
        other => panic!("slot 0: expected Intermediate, got {other:?}"),
    };
    let icc1_ref = match &slice[1] {
        Action::IntermediateCommandComplete { tag_ref } => *tag_ref,
        other => panic!("slot 1: expected Intermediate (empty), got {other:?}"),
    };
    let Action::DeliverReply {
        value: Reply::QueryComplete(_),
        ..
    } = &slice[2]
    else {
        panic!("slot 2: expected DeliverReply, got {:?}", slice[2]);
    };
    // NLL: borrow ends after last `slice` use; reborrow proto for accessor.
    let Some(command_tag) = proto.current_command_tag() else { panic!("command_tag slot populated"); };
    assert_eq!(format!("{}", command_tag), "COMMIT");
    let icc0_tag = proto.get_command_tag(icc0_ref).unwrap_or_else(|e| panic!("ICC[0]: {e:?}"));
    assert_eq!(format!("{}", icc0_tag), "BEGIN");
    let icc1_tag = proto.get_command_tag(icc1_ref).unwrap_or_else(|e| panic!("ICC[1]: {e:?}"));
    assert_eq!(format!("{}", icc1_tag), "", "empty query response yields empty tag");
}

#[test]
fn single_statement_batch_no_intermediates() {
    // Regression check: a single-statement Q frame must NOT emit any
    // IntermediateCommandComplete. The DEF-226 arms only fire when a
    // SECOND CommandComplete / RowDescription / EmptyQueryResponse
    // arrives BEFORE RFQ.
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _raw) = mint_reply::<bsql_pg_proto::QueryKind>(&mut proto);

    proto.push_or_panic(
        SimpleQuery {
            sql: "SELECT 1",
            reply,
        },
        &mut wb,
    );

    // Single-statement response: CommandComplete("SELECT 1") + RFQ.
    // (No RowDescription / DataRow because we're not actually
    // running SELECT; the server's response to the empty Q just for
    // state-machine tests is just `C "SELECT 1" + Z`.)
    let mut server_bytes = std::vec::Vec::new();
    server_bytes.extend(command_complete_frame(b"SELECT 1"));
    server_bytes.extend(rfq_frame(b'I'));

    let actions = proto.feed_bytes(&server_bytes, &mut wb);
    let slice = actions.as_slice();

    // Expect: ONE DeliverReply only — no intermediate.
    assert_eq!(slice.len(), 1, "single-statement must produce 1 action; got {:?}", slice);
    let Action::DeliverReply {
        value: Reply::QueryComplete(_),
        ..
    } = &slice[0]
    else {
        panic!("slot 0: expected DeliverReply, got {:?}", slice[0]);
    };
    drop(actions);
    let Some(command_tag) = proto.current_command_tag() else { panic!("command_tag slot populated"); };
    assert_eq!(format!("{}", command_tag), "SELECT 1");
}
