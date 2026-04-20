//! Phase 1c-1b — SimpleQuery flow end-to-end + bad-path coverage.
//!
//! Every test here names the invariant it defends. Tests cover:
//!
//! - **(A) Spec conformance** — SELECT / DML / empty-query / error
//!   response sequences from PG §55.2.3 produce the documented
//!   `Action` sequence.
//! - **(B) Tier-3 invariants** — push-state policy table, bad-path
//!   framing, row-stream correlator stability.
//!
//! The protocol is driven synchronously: tests push a `SimpleQuery`,
//! feed synthesised response bytes, and pattern-match on the returned
//! [`OutActions`]. Same mechanism as the async wrapper
//! (`bsql-driver-postgres`, Phase 1e), without a runtime.

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
    Action, PgCommand, PgProtocol, ProtoState, ProtocolError, QueryKind, Reply, ReplyId, Sql,
    WriteBuf,
    wire::{
        TAG_COMMAND_COMPLETE, TAG_DATA_ROW, TAG_EMPTY_QUERY_RESPONSE, TAG_ERROR_RESPONSE,
        TAG_QUERY, TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
    },
};
use core::num::NonZeroU64;

// ------------------------------------------------------------------
// Frame builders — pure functions, no protocol state. Each builder
// names the PG message shape it produces so the call sites read like
// the wire trace they emulate.
// ------------------------------------------------------------------

/// Build a `ReadyForQuery` frame: `'Z'` + len=5 + 1-byte tx-status.
fn rfq_frame(tx_status: u8) -> [u8; 6] {
    [TAG_READY_FOR_QUERY, 0, 0, 0, 5, tx_status]
}

/// Build a PG frame around `body`: tag byte + 4-byte big-endian
/// length (includes itself, excludes tag) + body.
fn frame(tag: u8, body: &[u8]) -> std::vec::Vec<u8> {
    let mut out = std::vec::Vec::new();
    out.push(tag);
    // length includes the 4 bytes of length field itself
    let Ok(len) = u32::try_from(body.len().saturating_add(4)) else {
        panic!("test fixture body too large for u32 length field");
    };
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// Build a minimal `RowDescription` frame for `n` columns. The
/// dispatcher treats the body as opaque — only the tag matters for
/// state-machine transitions — so a minimal body with the documented
/// i16-column-count header plus per-column sentinel stubs suffices.
fn row_description_frame(n_columns: u16) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.extend_from_slice(&n_columns.to_be_bytes());
    // Each column = name(NUL-term) + oid(i32) + attnum(i16) + type_oid(i32)
    //             + type_size(i16) + type_mod(i32) + format(i16).
    for i in 0..n_columns {
        body.extend_from_slice(b"c");
        body.push(0); // NUL-terminate name "c"
        body.extend_from_slice(&0i32.to_be_bytes());
        body.extend_from_slice(&i.to_be_bytes());
        body.extend_from_slice(&25i32.to_be_bytes()); // text oid
        body.extend_from_slice(&(-1i16).to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes()); // text format
    }
    frame(TAG_ROW_DESCRIPTION, &body)
}

/// Build a `DataRow` frame carrying a single text column with the
/// given `value` bytes. Body = column-count(i16=1) + len(i32) + value.
fn data_row_frame(value: &[u8]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.extend_from_slice(&1i16.to_be_bytes());
    let Ok(vlen) = i32::try_from(value.len()) else {
        panic!("test fixture data_row value too large");
    };
    body.extend_from_slice(&vlen.to_be_bytes());
    body.extend_from_slice(value);
    frame(TAG_DATA_ROW, &body)
}

/// Build a `CommandComplete` frame carrying a NUL-terminated tag
/// string, e.g. `"SELECT 5\0"`.
fn command_complete_frame(tag: &[u8]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::from(tag);
    body.push(0);
    frame(TAG_COMMAND_COMPLETE, &body)
}

/// Build an `EmptyQueryResponse` frame (no body).
fn empty_query_response_frame() -> std::vec::Vec<u8> {
    frame(TAG_EMPTY_QUERY_RESPONSE, &[])
}

/// Build a minimal `ErrorResponse` frame with a severity + message +
/// NUL terminator — enough for `parse_error_response` to classify.
fn error_response_frame(message: &[u8]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR");
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message);
    body.push(0);
    body.push(0); // terminator
    frame(TAG_ERROR_RESPONSE, &body)
}

// ------------------------------------------------------------------
// Correlator / ReplyId helpers.
// ------------------------------------------------------------------

fn raw(v: u64) -> NonZeroU64 {
    NonZeroU64::new(v).unwrap_or(NonZeroU64::MIN)
}

fn id(v: NonZeroU64) -> ReplyId<QueryKind> {
    ReplyId::from_raw(v)
}

/// Construct a `Sql` value from a `&str` test fixture.
///
/// Uses the truncating constructor `FixedStr::from_str_truncating`
/// (generic over `Truncating`-tagged types, of which `SqlTag` is
/// one). Source ≤ `MAX_SQL_LEN` fits verbatim; overflow gets a
/// trailing `"…"` — both paths are exact-byte round-trip.
fn sql(s: &str) -> Sql {
    Sql::from_str_truncating(s)
}

/// Push a SimpleQuery with the given SQL and correlator; assert the
/// single `SendBytes` action carries a `'Q'`-prefixed frame and
/// return the outbound bytes for further assertions.
#[track_caller]
fn simple_query_setup(
    proto: &mut PgProtocol,
    reply: ReplyId<QueryKind>,
    wb: &mut WriteBuf,
) -> std::vec::Vec<u8> {
    let out = proto.push_command(
        PgCommand::SimpleQuery {
            sql: sql("SELECT 1"),
            reply,
        },
        wb,
    );
    assert_eq!(out.len(), 1, "SimpleQuery push must emit exactly 1 action");
    match out.as_slice() {
        [Action::SendBytes(send_buf)] => {
            assert!(!send_buf.is_empty(), "SendBytes payload must be non-empty");
            assert_eq!(
                send_buf.first(),
                Some(&TAG_QUERY),
                "first byte of outbound must be `'Q'` (simple-query tag)",
            );
            send_buf.to_vec()
        }
        other => panic!("expected a single Action::SendBytes, got {other:?}"),
    }
}

// ==================================================================
// (A) Spec conformance tests
// ==================================================================

/// Invariant (spec): SELECT returning 0 rows produces
/// RowDescription → CommandComplete → ReadyForQuery, and the
/// protocol delivers `QueryComplete { command_tag, tx_status }` at
/// the terminal Z.
#[test]
fn select_zero_rows_end_to_end() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(100);
    let _sent = simple_query_setup(&mut proto, id(q_raw), &mut wb);

    // After push: state should be SimpleQueryAwaitFirstResponse.
    assert!(matches!(
        proto.state(),
        ProtoState::SimpleQueryAwaitFirstResponse(_),
    ));

    // Feed: T (0 cols) + C ("SELECT 0\0") + Z('I').
    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(0));
    bytes.extend_from_slice(&command_complete_frame(b"SELECT 0"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    assert_eq!(out.len(), 1, "0-row SELECT emits exactly DeliverReply on terminal Z");
    match out.as_slice() {
        [Action::DeliverReply { id: delivered_id, value }] => {
            assert_eq!(*delivered_id, q_raw, "correlator round-trips");
            match value {
                Reply::QueryComplete { command_tag, tx_status } => {
                    assert_eq!(command_tag.as_str(), "SELECT 0");
                    assert_eq!(*tx_status, b'I');
                }
                other => panic!("expected QueryComplete, got {other:?}"),
            }
        }
        other => panic!("expected DeliverReply, got {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Idle));
}

/// Invariant (spec): SELECT returning N rows emits one
/// `Action::StreamRow` per DataRow, carrying the row's body bytes,
/// followed by `DeliverReply(QueryComplete{..})` on terminal Z. The
/// StreamRow correlator matches the in-flight reply id; the
/// `ReplyId` is NOT consumed until the terminal Z.
#[test]
fn select_multiple_rows_stream_then_deliver() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(101);
    simple_query_setup(&mut proto, id(q_raw), &mut wb);

    // Build row values; 3 rows fits within MAX_ACTIONS_PER_CALL=4
    // (3 StreamRow + 1 DeliverReply = 4 actions).
    let row_values: [&[u8]; 3] = [b"alpha", b"beta", b"gamma"];
    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    for v in &row_values {
        bytes.extend_from_slice(&data_row_frame(v));
    }
    bytes.extend_from_slice(&command_complete_frame(b"SELECT 3"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    let actions = out.as_slice();
    assert_eq!(actions.len(), 4, "3 rows + 1 DeliverReply = 4 actions");

    // First three actions = StreamRow with matching id and row bytes.
    for (i, expected_value) in row_values.iter().enumerate() {
        match actions.get(i) {
            Some(Action::StreamRow { id, row_bytes }) => {
                assert_eq!(*id, q_raw, "StreamRow id must match in-flight reply");
                // Body = column-count (2 bytes BE) + len (4 bytes BE) + value.
                assert_eq!(
                    row_bytes.get(..2),
                    Some(&1i16.to_be_bytes()[..]),
                    "col count = 1",
                );
                let Ok(vlen) = i32::try_from(expected_value.len()) else { unreachable!() };
                assert_eq!(
                    row_bytes.get(2..6),
                    Some(&vlen.to_be_bytes()[..]),
                    "col len matches",
                );
                assert_eq!(
                    row_bytes.get(6..),
                    Some(*expected_value),
                    "row bytes round-trip verbatim",
                );
            }
            other => panic!("expected StreamRow at index {i}, got {other:?}"),
        }
    }

    // Fourth = DeliverReply QueryComplete.
    match actions.get(3) {
        Some(Action::DeliverReply { id: delivered_id, value: Reply::QueryComplete { command_tag, tx_status } }) => {
            assert_eq!(*delivered_id, q_raw);
            assert_eq!(command_tag.as_str(), "SELECT 3");
            assert_eq!(*tx_status, b'I');
        }
        other => panic!("expected DeliverReply(QueryComplete), got {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Idle));
}

/// Invariant (spec): a DML statement (no rows) yields
/// CommandComplete → ReadyForQuery directly, with no intermediate
/// RowDescription / DataRow. DeliverReply carries the PG-provided
/// tag (`"INSERT 0 3"`, `"UPDATE 7"`, …).
#[test]
fn dml_no_rows_end_to_end() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(102);
    simple_query_setup(&mut proto, id(q_raw), &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&command_complete_frame(b"INSERT 0 3"));
    bytes.extend_from_slice(&rfq_frame(b'T')); // inside a transaction

    let out = proto.feed_bytes(&bytes, &mut wb);
    assert_eq!(out.len(), 1, "DML emits exactly DeliverReply");
    match out.as_slice() {
        [Action::DeliverReply { id: delivered_id, value: Reply::QueryComplete { command_tag, tx_status } }] => {
            assert_eq!(*delivered_id, q_raw);
            assert_eq!(command_tag.as_str(), "INSERT 0 3");
            assert_eq!(*tx_status, b'T');
        }
        other => panic!("expected DeliverReply(QueryComplete(INSERT 0 3)), got {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Idle));
}

/// Invariant (spec): submitting an empty / whitespace-only SQL
/// yields EmptyQueryResponse → ReadyForQuery. DeliverReply carries
/// an empty `command_tag`.
#[test]
fn empty_query_yields_empty_tag() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(103);
    simple_query_setup(&mut proto, id(q_raw), &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&empty_query_response_frame());
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    assert_eq!(out.len(), 1);
    match out.as_slice() {
        [Action::DeliverReply { value: Reply::QueryComplete { command_tag, .. }, .. }] => {
            assert_eq!(
                command_tag.as_str(),
                "",
                "EmptyQueryResponse surfaces as empty command tag",
            );
        }
        other => panic!("expected DeliverReply with empty command_tag, got {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Idle));
}

/// Invariant (spec): a query-level error (E → Z) emits FailReply
/// and leaves the connection open (state returns to Idle after Z,
/// no `Action::CloseSocket`). PG §55.2.3 guarantees Z follows E on
/// query-level errors; the connection must survive.
#[test]
fn query_error_emits_fail_reply_and_connection_survives() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(104);
    simple_query_setup(&mut proto, id(q_raw), &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&error_response_frame(b"syntax error at or near EOF"));
    bytes.extend_from_slice(&rfq_frame(b'E')); // failed-transaction status

    let out = proto.feed_bytes(&bytes, &mut wb);
    let actions = out.as_slice();
    assert_eq!(actions.len(), 1, "E emits FailReply; trailing Z drained silently");
    match actions.first() {
        Some(Action::FailReply { id: failed_id, cause }) => {
            assert_eq!(*failed_id, q_raw);
            assert!(
                matches!(cause, ProtocolError::ServerErrorResponse { .. }),
                "FailReply cause must be ServerErrorResponse, got {cause:?}",
            );
        }
        other => panic!("expected FailReply, got {other:?}"),
    }
    // Critical: NO CloseSocket. Connection survives query-level errors.
    for a in actions {
        assert!(
            !matches!(a, Action::CloseSocket),
            "query-level error must not close the socket: {a:?}",
        );
    }
    assert!(
        matches!(proto.state(), ProtoState::Idle),
        "state returns to Idle after drain Z; got {:?}",
        proto.state(),
    );
}

/// Invariant (spec): error-in-stream variant — E arrives AFTER
/// some rows have streamed. Rows still emit; FailReply replaces
/// DeliverReply; Z drains; connection returns to Idle.
#[test]
fn error_after_some_rows_emits_stream_then_fail() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(105);
    simple_query_setup(&mut proto, id(q_raw), &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    bytes.extend_from_slice(&data_row_frame(b"partial"));
    bytes.extend_from_slice(&error_response_frame(b"division by zero"));
    bytes.extend_from_slice(&rfq_frame(b'E'));

    let out = proto.feed_bytes(&bytes, &mut wb);
    let actions = out.as_slice();
    assert_eq!(actions.len(), 2, "1 StreamRow + 1 FailReply = 2 actions");
    assert!(matches!(actions.first(), Some(Action::StreamRow { .. })));
    assert!(matches!(
        actions.get(1),
        Some(Action::FailReply {
            cause: ProtocolError::ServerErrorResponse { .. },
            ..
        }),
    ));
    assert!(matches!(proto.state(), ProtoState::Idle));
}

// ==================================================================
// (B) Tier-3 invariants — bad paths + push-state policy
// ==================================================================

/// Invariant: pushing SimpleQuery while another simple-query is in
/// flight yields FailReply(CommandInProgress) for the new push, and
/// the original in-flight state is preserved.
#[test]
fn simple_query_while_in_flight_yields_command_in_progress() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let first_raw = raw(110);
    simple_query_setup(&mut proto, id(first_raw), &mut wb);

    // Push a second SimpleQuery while the first is still waiting.
    let second_raw = raw(111);
    let out = proto.push_command(
        PgCommand::SimpleQuery {
            sql: sql("SELECT 2"),
            reply: id(second_raw),
        },
        &mut wb,
    );
    assert_eq!(out.len(), 1);
    match out.as_slice() {
        [Action::FailReply { id: failed_id, cause: ProtocolError::CommandInProgress }] => {
            assert_eq!(*failed_id, second_raw, "FailReply targets the NEW reply, not the in-flight one");
        }
        other => panic!("expected FailReply(CommandInProgress), got {other:?}"),
    }
    // Original state preserved.
    assert!(matches!(
        proto.state(),
        ProtoState::SimpleQueryAwaitFirstResponse(_),
    ));

    // Drain the first query so the protocol doesn't drop with an
    // in-flight ReplyId.
    let drain_bytes = {
        let mut v = std::vec::Vec::new();
        v.extend_from_slice(&command_complete_frame(b"SELECT 0"));
        v.extend_from_slice(&rfq_frame(b'I'));
        v
    };
    let out = proto.feed_bytes(&drain_bytes, &mut wb);
    assert!(matches!(
        out.as_slice(),
        [Action::DeliverReply { .. }],
    ));
}

/// Invariant: pushing SimpleQuery after the connection was torn
/// down (Errored state) yields FailReply(ConnectionAlreadyClosed).
#[test]
fn simple_query_on_errored_state_fails() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();

    // Force an Errored state: feed an unexpected frame from Idle.
    let unexpected = frame(b'Z', b"I"); // Z in Idle is unsolicited
    let out = proto.feed_bytes(&unexpected, &mut wb);
    assert!(
        out.as_slice().iter().any(|a| matches!(a, Action::CloseSocket)),
        "Errored transition must emit CloseSocket",
    );
    assert!(matches!(proto.state(), ProtoState::Errored(_)));

    // Now push SimpleQuery.
    let q_raw = raw(120);
    let out = proto.push_command(
        PgCommand::SimpleQuery {
            sql: sql("SELECT 1"),
            reply: id(q_raw),
        },
        &mut wb,
    );
    assert_eq!(out.len(), 1);
    match out.as_slice() {
        [Action::FailReply { id: failed_id, cause: ProtocolError::ConnectionAlreadyClosed { .. } }] => {
            assert_eq!(*failed_id, q_raw);
        }
        other => panic!("expected FailReply(ConnectionAlreadyClosed), got {other:?}"),
    }
}

/// Invariant: a `CommandComplete` with no NUL terminator is
/// classified as `MalformedCommandComplete` and tears the
/// connection down — no silent recovery of a wire-framing desync.
#[test]
fn malformed_command_complete_no_nul_terminator_tears_down() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(130);
    simple_query_setup(&mut proto, id(q_raw), &mut wb);

    // Body without NUL terminator.
    let bad = frame(TAG_COMMAND_COMPLETE, b"SELECT 1");
    let out = proto.feed_bytes(&bad, &mut wb);
    let actions = out.as_slice();
    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::FailReply { cause: ProtocolError::MalformedCommandComplete { .. }, .. }
        )),
        "expected FailReply(MalformedCommandComplete), got {actions:?}",
    );
    assert!(
        actions.iter().any(|a| matches!(a, Action::CloseSocket)),
        "malformed wire framing must close the socket: {actions:?}",
    );
    assert!(matches!(proto.state(), ProtoState::Errored(_)));
}

/// Invariant: a `ReadyForQuery` arriving BEFORE any C (i.e. in
/// `SimpleQueryAwaitFirstResponse` or `SimpleQueryStreamingRows`)
/// is classified as UnexpectedFrame — desync.
#[test]
fn unexpected_rfq_during_await_first_response_tears_down() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(140);
    simple_query_setup(&mut proto, id(q_raw), &mut wb);

    let out = proto.feed_bytes(&rfq_frame(b'I'), &mut wb);
    let actions = out.as_slice();
    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::FailReply {
                cause: ProtocolError::UnexpectedFrame { tag: TAG_READY_FOR_QUERY },
                ..
            }
        )),
        "expected FailReply(UnexpectedFrame{{Z}}), got {actions:?}",
    );
    assert!(actions.iter().any(|a| matches!(a, Action::CloseSocket)));
    assert!(matches!(proto.state(), ProtoState::Errored(_)));
}

/// Invariant: rows arriving across multiple feed_bytes calls stream
/// correctly — the `StreamingRows` state persists across boundaries,
/// each feed_bytes call emits the rows it observed, the terminal Z
/// delivers the reply.
#[test]
fn rows_across_multiple_feed_bytes_calls() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(150);
    simple_query_setup(&mut proto, id(q_raw), &mut wb);

    // Batch 1: T + two D frames.
    let mut batch1 = std::vec::Vec::new();
    batch1.extend_from_slice(&row_description_frame(1));
    batch1.extend_from_slice(&data_row_frame(b"r1"));
    batch1.extend_from_slice(&data_row_frame(b"r2"));
    {
        let out1 = proto.feed_bytes(&batch1, &mut wb);
        let actions1 = out1.as_slice();
        assert_eq!(actions1.len(), 2, "two rows in batch 1");
        assert!(actions1.iter().all(|a| matches!(a, Action::StreamRow { .. })));
        // OutActions is Copy-POD; the scope close releases the
        // `'r` borrow — no explicit `drop()` needed (clippy warns
        // that dropping Copy types is a no-op).
    }
    assert!(matches!(proto.state(), ProtoState::SimpleQueryStreamingRows(_)));

    // Batch 2: one more D + C + Z.
    let mut batch2 = std::vec::Vec::new();
    batch2.extend_from_slice(&data_row_frame(b"r3"));
    batch2.extend_from_slice(&command_complete_frame(b"SELECT 3"));
    batch2.extend_from_slice(&rfq_frame(b'I'));
    let out2 = proto.feed_bytes(&batch2, &mut wb);
    let actions2 = out2.as_slice();
    assert_eq!(actions2.len(), 2, "one row + DeliverReply");
    assert!(matches!(actions2.first(), Some(Action::StreamRow { .. })));
    assert!(matches!(
        actions2.get(1),
        Some(Action::DeliverReply { value: Reply::QueryComplete { .. }, .. }),
    ));
    assert!(matches!(proto.state(), ProtoState::Idle));
}

/// Invariant (DEF-121 backpressure gate): if an inbound chunk
/// contains more actionable frames than `OutActions` can hold, the
/// `feed_bytes` loop must break **before** consuming any reply /
/// mutating state — leaving the overflowing frames in the read
/// buffer for the next `feed_bytes` call. Without the gate, the
/// in-macro `on_overflow: break` fires AFTER `deliver()` /
/// `errored()` have already consumed the reply, silently dropping
/// the action and orphaning the caller's oneshot.
///
/// Construction: push SimpleQuery, feed 10 DataRow frames + C + Z
/// in ONE chunk. `MAX_ACTIONS_PER_CALL=8`, `WORST_CASE_PER_DISPATCH=2`
/// — so the first call emits ≤ 7 StreamRows and leaves the rest in
/// the buffer. The second call (empty bytes) drains the remainder
/// and the terminal DeliverReply.
#[test]
fn overflow_backpressure_preserves_delivery_across_calls() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(200);
    simple_query_setup(&mut proto, id(q_raw), &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    let row_count = 10usize;
    for i in 0..row_count {
        let Ok(digit) = u8::try_from(i) else { unreachable!() };
        let payload = [b'r', digit.saturating_add(b'0')];
        bytes.extend_from_slice(&data_row_frame(&payload));
    }
    bytes.extend_from_slice(&command_complete_frame(b"SELECT 10"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    // First call: gate caps emission under MAX_ACTIONS_PER_CALL.
    let mut total_rows = 0usize;
    let mut saw_deliver = false;
    let first = proto.feed_bytes(&bytes, &mut wb);
    assert!(
        first.len() <= 8,
        "first call must not exceed MAX_ACTIONS_PER_CALL=8",
    );
    for a in first.as_slice() {
        match a {
            Action::StreamRow { id: sid, .. } => {
                assert_eq!(*sid, q_raw);
                total_rows = total_rows.saturating_add(1);
            }
            Action::DeliverReply { .. } => saw_deliver = true,
            other => panic!("unexpected action in first call: {other:?}"),
        }
    }
    // State is still a simple-query state (not consumed to Idle).
    let still_streaming = matches!(
        proto.state(),
        ProtoState::SimpleQueryStreamingRows(_)
            | ProtoState::SimpleQueryAwaitRfq { .. }
            | ProtoState::SimpleQueryAwaitFirstResponse(_),
    );
    assert!(
        still_streaming,
        "state must stay in simple-query flow — reply not consumed mid-overflow",
    );

    // Subsequent calls drain the remaining frames.
    while !saw_deliver {
        let out = proto.feed_bytes(&[], &mut wb);
        assert!(out.len() <= 8);
        for a in out.as_slice() {
            match a {
                Action::StreamRow { id: sid, .. } => {
                    assert_eq!(*sid, q_raw);
                    total_rows = total_rows.saturating_add(1);
                }
                Action::DeliverReply { .. } => saw_deliver = true,
                other => panic!("unexpected action while draining: {other:?}"),
            }
        }
    }
    assert_eq!(total_rows, row_count, "every row delivered exactly once");
    assert!(matches!(proto.state(), ProtoState::Idle));
}

/// Invariant: the outbound `Q` frame layout is tag + BE-length +
/// NUL-terminated SQL. Drift-pin on the wire builder:
///
/// - byte 0: `'Q'`
/// - bytes 1..=4: BE u32 length = 4 (self) + len(sql) + 1 (NUL)
/// - bytes 5..5+len(sql): SQL text
/// - byte 5+len(sql): NUL terminator
#[test]
fn query_frame_wire_format() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(160);
    let sent = simple_query_setup(&mut proto, id(q_raw), &mut wb);

    // `simple_query_setup` uses `sql("SELECT 1")` — 8 bytes + NUL.
    let expected_sql = b"SELECT 1";
    let expected_len_field = 4u32.saturating_add(u32::try_from(expected_sql.len()).unwrap_or(0)).saturating_add(1);
    let expected_total = 1 + 4 + expected_sql.len() + 1; // tag + length + sql + NUL

    assert_eq!(sent.first(), Some(&TAG_QUERY), "tag = 'Q'");
    assert_eq!(
        sent.get(1..5),
        Some(&expected_len_field.to_be_bytes()[..]),
        "length field = 4 (self) + len(sql) + 1 (NUL)",
    );
    assert_eq!(
        sent.get(5..5 + expected_sql.len()),
        Some(&expected_sql[..]),
        "SQL text copied verbatim",
    );
    assert_eq!(
        sent.get(5 + expected_sql.len()),
        Some(&0u8),
        "trailing NUL terminator",
    );
    assert_eq!(sent.len(), expected_total, "total frame size");

    // Drain so ReplyId doesn't trip the Drop-guard.
    let mut drain = std::vec::Vec::new();
    drain.extend_from_slice(&command_complete_frame(b"SELECT 1"));
    drain.extend_from_slice(&rfq_frame(b'I'));
    let out = proto.feed_bytes(&drain, &mut wb);
    assert!(matches!(out.as_slice(), [Action::DeliverReply { .. }]));
}
