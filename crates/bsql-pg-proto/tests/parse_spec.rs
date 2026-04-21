//! Phase 1c-3a — Extended Query `Parse` command end-to-end.
//!
//! Covers the minimum-viable extended-query slice: `Parse + Sync`
//! out, `ParseComplete + ReadyForQuery` in, `Reply::ParseComplete`
//! delivered. Error recovery and push-state policy paths.

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
    Action, ParseKind, PgCommand, PgProtocol, ProtoState, ProtocolError, Reply, ReplyId, Sql,
    StmtName, WriteBuf,
    wire::{SYNC_WIRE_BYTES, TAG_ERROR_RESPONSE, TAG_PARSE, TAG_PARSE_COMPLETE, TAG_READY_FOR_QUERY},
};
use core::num::NonZeroU64;

fn raw(v: u64) -> NonZeroU64 {
    NonZeroU64::new(v).unwrap_or(NonZeroU64::MIN)
}

fn id(v: NonZeroU64) -> ReplyId<ParseKind> {
    ReplyId::from_raw(v)
}

fn sql(s: &str) -> Sql {
    Sql::from_str_truncating(s)
}

fn stmt_unnamed() -> StmtName {
    StmtName::default()
}

/// Build a bare frame: tag + 4-byte BE length (self-inclusive) + body.
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

fn parse_complete_frame() -> [u8; 5] {
    // tag '1', length 4 (empty body)
    [TAG_PARSE_COMPLETE.byte(), 0, 0, 0, 4]
}

fn rfq_frame(tx_status: u8) -> [u8; 6] {
    [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, tx_status]
}

fn error_response_frame(message: &[u8]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR");
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message);
    body.push(0);
    body.push(0); // terminator
    frame(TAG_ERROR_RESPONSE.byte(), &body)
}

/// Push a Parse + assert two SendBytes actions emitted (P frame + Sync).
/// Returns the P-frame bytes for wire-layout inspection.
#[track_caller]
fn parse_setup(
    proto: &mut PgProtocol,
    stmt_name: StmtName,
    sql_text: &str,
    reply: ReplyId<ParseKind>,
    wb: &mut WriteBuf,
) -> std::vec::Vec<u8> {
    let out = proto.push_command(
        PgCommand::Parse {
            stmt_name,
            sql: sql(sql_text),
            reply,
        },
        wb,
    );
    assert_eq!(out.len(), 2, "Parse emits 2 actions: P frame + Sync");
    match out.as_slice() {
        [Action::SendBytes(p_frame), Action::SendBytes(sync_frame)] => {
            assert_eq!(
                p_frame.first(),
                Some(&TAG_PARSE.byte()),
                "first action must be 'P' frame",
            );
            assert_eq!(
                *sync_frame, &SYNC_WIRE_BYTES,
                "second action must be the static SYNC const",
            );
            p_frame.to_vec()
        }
        other => panic!("expected 2 SendBytes actions, got {other:?}"),
    }
}

// ==================================================================
// (A) Spec conformance
// ==================================================================

/// Invariant (spec): Parse success path delivers
/// `Reply::ParseComplete` on the terminal RFQ.
#[test]
fn parse_success_end_to_end() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let reply_raw = raw(100);
    parse_setup(&mut proto, stmt_unnamed(), "SELECT 1", id(reply_raw), &mut wb);

    assert!(matches!(
        proto.state(),
        ProtoState::ParseAwaitingParseComplete(_),
    ));

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&parse_complete_frame());
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    assert_eq!(out.len(), 1, "ParseComplete + Z emits exactly one DeliverReply");
    match out.as_slice() {
        [Action::DeliverReply {
            id: delivered_id,
            value: Reply::ParseComplete(p),
        }] => {
            assert_eq!(*delivered_id, reply_raw, "correlator round-trips");
            assert_eq!(p.tx_status, bsql_pg_proto::TxStatus::Idle);
        }
        other => panic!("expected DeliverReply(ParseComplete), got {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Idle));
}

/// Invariant (spec): Parse error path — server sends ErrorResponse
/// followed by ReadyForQuery. Client emits `FailReply` and drains
/// the RFQ back to Idle without closing the socket (PG guarantees
/// `Z` after `E`).
#[test]
fn parse_error_is_recoverable() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let reply_raw = raw(200);
    parse_setup(&mut proto, stmt_unnamed(), "SELECT 1/0", id(reply_raw), &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&error_response_frame(b"division by zero"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    let actions = out.as_slice();
    assert_eq!(actions.len(), 1, "E emits FailReply; Z drained silently");
    match actions.first() {
        Some(Action::FailReply { id: failed_id, cause }) => {
            assert_eq!(*failed_id, reply_raw);
            assert!(
                matches!(cause, ProtocolError::ServerErrorResponse { .. }),
                "FailReply.cause must be ServerErrorResponse, got {cause:?}",
            );
        }
        other => panic!("expected FailReply, got {other:?}"),
    }
    // Critical: no CloseSocket — connection survives.
    for a in actions {
        assert!(
            !matches!(a, Action::CloseSocket),
            "parse-error must not close socket: {a:?}",
        );
    }
    assert!(
        matches!(proto.state(), ProtoState::Idle),
        "state returns to Idle after drain; got {:?}", proto.state(),
    );
}

/// Invariant (spec): named prepared statements carry their name on
/// the wire. Verify the `P` frame layout: tag + length + stmt_name
/// + NUL + SQL + NUL + i16(0).
#[test]
fn parse_frame_wire_format_with_named_statement() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let reply_raw = raw(300);
    let Ok(name) = StmtName::try_from_str("my_stmt") else {
        panic!("fixture: valid stmt name");
    };
    let p_bytes = parse_setup(&mut proto, name, "SELECT 1", id(reply_raw), &mut wb);

    // Layout check:
    //   byte 0: 'P'
    //   bytes 1..=4: BE u32 length = 4 + 7(name) + 1 + 8(sql) + 1 + 2 = 23
    //   bytes 5..=11: "my_stmt"
    //   byte 12: NUL
    //   bytes 13..=20: "SELECT 1"
    //   byte 21: NUL
    //   bytes 22..=23: i16(0) = [0, 0]
    let expected_len_field = 4u32 + 7 + 1 + 8 + 1 + 2;
    assert_eq!(p_bytes.first(), Some(&TAG_PARSE.byte()));
    assert_eq!(
        p_bytes.get(1..5),
        Some(&expected_len_field.to_be_bytes()[..]),
    );
    assert_eq!(p_bytes.get(5..12), Some(&b"my_stmt"[..]));
    assert_eq!(p_bytes.get(12), Some(&0u8));
    assert_eq!(p_bytes.get(13..21), Some(&b"SELECT 1"[..]));
    assert_eq!(p_bytes.get(21), Some(&0u8));
    assert_eq!(p_bytes.get(22..24), Some(&[0u8, 0u8][..]));

    // Drain the parse so the ReplyId doesn't trip the Drop-guard.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&parse_complete_frame());
    drain.extend_from_slice(&rfq_frame(b'I'));
    let drain_out = proto.feed_bytes(&drain, &mut wb);
    assert!(matches!(drain_out.as_slice(), [Action::DeliverReply { .. }]));
}

// ==================================================================
// (B) Push-state policy
// ==================================================================

/// Invariant: pushing a second Parse while one is in flight returns
/// `FailReply(CommandInProgress)` for the new id; the in-flight
/// state is preserved.
#[test]
fn parse_while_parse_in_flight_fails_with_command_in_progress() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let first_raw = raw(400);
    parse_setup(&mut proto, stmt_unnamed(), "SELECT 1", id(first_raw), &mut wb);

    let second_raw = raw(401);
    let out = proto.push_command(
        PgCommand::Parse {
            stmt_name: stmt_unnamed(),
            sql: sql("SELECT 2"),
            reply: id(second_raw),
        },
        &mut wb,
    );
    assert_eq!(out.len(), 1);
    match out.as_slice() {
        [Action::FailReply {
            id: failed_id,
            cause: ProtocolError::CommandInProgress,
        }] => {
            assert_eq!(*failed_id, second_raw, "FailReply targets NEW reply");
        }
        other => panic!("expected FailReply(CommandInProgress), got {other:?}"),
    }
    assert!(matches!(
        proto.state(),
        ProtoState::ParseAwaitingParseComplete(_),
    ));

    // Drain the first parse.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&parse_complete_frame());
    drain.extend_from_slice(&rfq_frame(b'I'));
    let drain_out = proto.feed_bytes(&drain, &mut wb);
    assert!(matches!(drain_out.as_slice(), [Action::DeliverReply { .. }]));
}

/// Invariant: Parse pushed onto an Errored connection fails with
/// `ConnectionAlreadyClosed`.
#[test]
fn parse_on_errored_fails_with_connection_already_closed() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();

    // Force Errored via an unsolicited Z in Idle.
    let unsolicited = frame(TAG_READY_FOR_QUERY.byte(), b"I");
    let out = proto.feed_bytes(&unsolicited, &mut wb);
    assert!(out.as_slice().iter().any(|a| matches!(a, Action::CloseSocket)));
    assert!(matches!(proto.state(), ProtoState::Errored(_)));

    let reply_raw = raw(500);
    let out = proto.push_command(
        PgCommand::Parse {
            stmt_name: stmt_unnamed(),
            sql: sql("SELECT 1"),
            reply: id(reply_raw),
        },
        &mut wb,
    );
    match out.as_slice() {
        [Action::FailReply {
            id: failed_id,
            cause: ProtocolError::ConnectionAlreadyClosed { .. },
        }] => {
            assert_eq!(*failed_id, reply_raw);
        }
        other => panic!("expected FailReply(ConnectionAlreadyClosed), got {other:?}"),
    }
}

/// Invariant: unexpected frame (e.g., DataRow) during
/// `ParseAwaitingParseComplete` tears the connection down — parse
/// flow has a narrow legal sequence, stray frames are framing
/// desync.
#[test]
fn parse_unexpected_frame_tears_down() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let reply_raw = raw(600);
    parse_setup(&mut proto, stmt_unnamed(), "SELECT 1", id(reply_raw), &mut wb);

    // DataRow ('D') in ParseAwaitingParseComplete is out-of-spec.
    let bad = frame(b'D', &[0, 0]);
    let out = proto.feed_bytes(&bad, &mut wb);
    let actions = out.as_slice();
    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::FailReply {
                cause: ProtocolError::UnexpectedFrame { .. },
                ..
            },
        )),
        "expected FailReply(UnexpectedFrame), got {actions:?}",
    );
    assert!(actions.iter().any(|a| matches!(a, Action::CloseSocket)));
    assert!(matches!(proto.state(), ProtoState::Errored(_)));
}
