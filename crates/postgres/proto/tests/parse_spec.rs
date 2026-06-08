//! Extended Query `Parse` command end-to-end.
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

use bsql_postgres_proto::{
    Action, ActiveState, ConnectionStatus, ParseKind, PgProtocol, ProtocolError, Reply,
    ReplyId, StmtName, WriteBuf,
    wire::{TAG_ERROR_RESPONSE, TAG_PARSE, TAG_PARSE_COMPLETE, TAG_READY_FOR_QUERY},
};

mod common;
use common::{PushOrPanic, fresh_active_via_trust_handshake, mint_reply};

// `push_command::Parse.sql` is `&'a str`; fixtures pass `&str`
// literals directly. The legacy `cfg(test)` `PgCommand::Parse` enum
// still owns `Sql` but lives only inside the lib's own unit tests.

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

/// Push a Parse + assert wire layout in `wb.as_bytes()`.
/// Returns the P-frame bytes for wire-layout inspection.
///
/// # Wire layout
///
/// `push_or_panic` returns `()`; bytes live in the caller's `wb` as
/// a single concatenation of the P frame followed by the trailing
/// Sync wire bytes — the production I/O path drains them in one
/// socket write. A naive `OutActions` list of
/// `[Action::SendBytes(p), Action::SendBytes(sync)]` (800 B per
/// call) would cost a per-action heap surface that the
/// single-concatenation shape erases.
///
/// Wire-layout assertion preserved exactly (tail = literal `[b'S', 0,
/// 0, 0, 4]` per PG §55.2.4 — tag 'S' + BE u32 length=4 self-inclusive,
/// zero body). F33 anti-tautology stance unchanged: the 5-byte literal
/// here, not a reference to the internal `SYNC_WIRE_BYTES` const.
#[track_caller]
fn parse_setup(
    proto: &mut PgProtocol<bsql_postgres_proto::ActivePhase>,
    stmt_name: StmtName,
    sql_text: &str,
    reply: ReplyId<ParseKind>,
    wb: &mut WriteBuf,
) -> std::vec::Vec<u8> {
    proto.push_or_panic(
        bsql_postgres_proto::push_command::Parse::new(stmt_name, sql_text, reply),
        wb,
    );
    let bytes = wb.as_bytes();
    let total_len = bytes.len();
    assert!(
        total_len >= 5,
        "Parse push must emit at least the trailing Sync (5 B); got {total_len} B",
    );
    let split = total_len.saturating_sub(5);
    let Some((p_frame, sync_frame)) = bytes.split_at_checked(split) else {
        panic!("wb split unreachable post-assert(total_len >= 5): split={split} total={total_len}");
    };
    assert_eq!(
        sync_frame, &[b'S', 0u8, 0u8, 0u8, 4u8],
        "tail must be the PG Sync wire bytes (tag 'S' + BE u32 length=4)",
    );
    assert_eq!(
        p_frame.first(),
        Some(&TAG_PARSE.byte()),
        "head must start with the 'P' Parse tag",
    );
    p_frame.to_vec()
}

// ==================================================================
// (A) Spec conformance
// ==================================================================

/// Invariant (spec): Parse success path delivers
/// `Reply::ParseComplete` on the terminal RFQ.
#[test]
fn parse_success_end_to_end() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, reply_raw) = mint_reply::<ParseKind>(&mut proto);
    parse_setup(&mut proto, stmt_unnamed(), "SELECT 1", reply, &mut wb);

    assert!(matches!(
        proto.state(),
        ActiveState::ParseAwaitingParseComplete(_),
    ));

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&parse_complete_frame());
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    assert_eq!(out.len(), 1, "ParseComplete + Z emits exactly one DeliverReply");
    match out.as_slice() {
        [Action::DeliverReply {
            id: delivered_id,
            value: Reply::ParseComplete(_p),
        }] => {
            assert_eq!(*delivered_id, reply_raw, "correlator round-trips");
        }
        other => panic!("expected DeliverReply(ParseComplete), got {other:?}"),
    }
    // DEF-286 Φ-E: tx_status moved off Reply payloads to slot accessor.
    assert_eq!(proto.terminal_tx_status(), bsql_postgres_proto::TxStatus::Idle);
    assert!(matches!(proto.state(), ActiveState::Idle));
}

/// Invariant (spec): Parse error path — server sends ErrorResponse
/// followed by ReadyForQuery. Client emits `FailReply` and drains
/// the RFQ back to Idle without closing the socket (PG guarantees
/// `Z` after `E`).
#[test]
fn parse_error_is_recoverable() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, reply_raw) = mint_reply::<ParseKind>(&mut proto);
    parse_setup(&mut proto, stmt_unnamed(), "SELECT 1/0", reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&error_response_frame(b"division by zero"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    let actions = out.as_slice();
    assert_eq!(actions.len(), 1, "E emits FailReply; Z drained silently");
    match actions.first() {
        Some(Action::FailReply { id: failed_id }) => {
            assert_eq!(*failed_id, reply_raw);
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
    let _ = out;
    // DEF-286 Φ-I.b: cause via slot.
    let Some(cause) = proto.fail_cause().copied() else { panic!("fail_cause slot must be populated post-FailReply"); };
    assert!(
        matches!(cause, ProtocolError::ServerErrorResponse { .. }),
        "FailReply.cause must be ServerErrorResponse, got {cause:?}",
    );
    assert!(
        matches!(proto.state(), ActiveState::Idle),
        "state returns to Idle after drain; got {:?}", proto.state(),
    );
}

/// Invariant (spec): named prepared statements carry their name on
/// the wire. Verify the `P` frame layout: tag + length + stmt_name
/// + NUL + SQL + NUL + i16(0).
#[test]
fn parse_frame_wire_format_with_named_statement() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<ParseKind>(&mut proto);
    let Ok(name) = StmtName::try_from_str("my_stmt") else {
        panic!("fixture: valid stmt name");
    };
    let p_bytes = parse_setup(&mut proto, name, "SELECT 1", reply, &mut wb);

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

/// Invariant: pushing a second Parse while one is in flight is
/// **structurally impossible** at the public API surface.
///
/// The public surface routes through [`PgProtocol::as_ready`], which
/// returns `None` during the in-flight wait — the second push never
/// happens. The internal `compute_push_parse` non-Idle arm (which
/// still emits `FailReply(CommandInProgress)` defensively) is
/// exercised by the in-file `compute_push_tests` module; this
/// integration test verifies only the public API contract.
#[test]
fn parse_while_parse_in_flight_blocked_at_compile_time() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (first_reply, _first_raw) = mint_reply::<ParseKind>(&mut proto);
    parse_setup(&mut proto, stmt_unnamed(), "SELECT 1", first_reply, &mut wb);

    // State is `ParseAwaitingParseComplete`. The public API
    // (`as_ready`) returns `None` — caller cannot acquire a guard,
    // therefore cannot push a second Parse.
    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None during in-flight Parse",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Busy,
        "in-flight Parse must classify as ConnectionStatus::Busy",
    );

    // First Parse correlator still pending (state preserved).
    assert!(matches!(
        proto.state(),
        ActiveState::ParseAwaitingParseComplete(_),
    ));

    // Drain the first parse so its ReplyId is consumed before drop.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&parse_complete_frame());
    drain.extend_from_slice(&rfq_frame(b'I'));
    let drain_out = proto.feed_bytes(&drain, &mut wb);
    assert!(matches!(drain_out.as_slice(), [Action::DeliverReply { .. }]));
}

/// Invariant: Parse on an `Errored` connection is structurally
/// impossible at the public API surface.
///
/// `as_ready` returns `None` and `connection_status` returns
/// `ConnectionStatus::Errored(kind)` — caller has structured access
/// to the underlying `StateErrorKind` for recovery decisions
/// (typically: discard the connection, return to pool with disposal).
#[test]
fn parse_on_errored_blocked_at_compile_time() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    // Force Errored via an unsolicited Z in Idle.
    let unsolicited = frame(TAG_READY_FOR_QUERY.byte(), b"I");
    let out = proto.feed_bytes(&unsolicited, &mut wb);
    assert!(out.as_slice().iter().any(|a| matches!(a, Action::CloseSocket)));
    assert!(matches!(proto.state(), ActiveState::Errored(_)));

    // as_ready returns None on Errored.
    assert!(
        proto.as_ready().is_none(),
        "as_ready must return None on Errored state",
    );

    // connection_status exposes the underlying error kind for
    // structured caller-side recovery (a naive `FailReply.cause`
    // shape on a synthesised reply would bury the diagnostic).
    match proto.connection_status() {
        ConnectionStatus::Errored(_kind) => {
            // Caller can inspect _kind to decide recovery policy
            // (discard vs retry). Test value: structured access to
            // the tier-3 classified state error, exposed as a
            // public-API enum variant.
        }
        other => panic!(
            "expected ConnectionStatus::Errored(_) on Errored state, got {other:?}",
        ),
    }
}

/// Invariant: unexpected frame (e.g., DataRow) during
/// `ParseAwaitingParseComplete` tears the connection down — parse
/// flow has a narrow legal sequence, stray frames are framing
/// desync.
#[test]
fn parse_unexpected_frame_tears_down() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _reply_raw) = mint_reply::<ParseKind>(&mut proto);
    parse_setup(&mut proto, stmt_unnamed(), "SELECT 1", reply, &mut wb);

    // DataRow ('D') in ParseAwaitingParseComplete is out-of-spec.
    let bad = frame(b'D', &[0, 0]);
    let out = proto.feed_bytes(&bad, &mut wb);
    let actions = out.as_slice();
    let saw_fail = actions.iter().any(|a| matches!(a, Action::FailReply { .. }));
    let saw_close = actions.iter().any(|a| matches!(a, Action::CloseSocket));
    let _ = out;
    let cause_match = proto.fail_cause().is_some_and(|c| matches!(c, ProtocolError::UnexpectedFrame { .. }));
    assert!(saw_fail && cause_match, "expected FailReply(UnexpectedFrame)");
    assert!(saw_close, "expected CloseSocket");
    assert!(matches!(proto.state(), ActiveState::Errored(_)));
}
