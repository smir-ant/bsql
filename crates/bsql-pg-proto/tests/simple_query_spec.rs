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
    Action, ConnectionStatus, PgProtocol, ProtoState, ProtocolError, QueryKind, Reply,
    ReplyId, Sql, WriteBuf,
    wire::{
        TAG_COMMAND_COMPLETE, TAG_DATA_ROW, TAG_EMPTY_QUERY_RESPONSE, TAG_ERROR_RESPONSE,
        TAG_QUERY, TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
    },
};
use core::num::NonZeroU64;

mod common;
use common::PushOrPanic;

// ------------------------------------------------------------------
// Frame builders — pure functions, no protocol state. Each builder
// names the PG message shape it produces so the call sites read like
// the wire trace they emulate.
// ------------------------------------------------------------------

/// Build a `ReadyForQuery` frame: `'Z'` + len=5 + 1-byte tx-status.
fn rfq_frame(tx_status: u8) -> [u8; 6] {
    [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, tx_status]
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
    frame(TAG_ROW_DESCRIPTION.byte(), &body)
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
    frame(TAG_DATA_ROW.byte(), &body)
}

/// Build a `CommandComplete` frame carrying a NUL-terminated tag
/// string, e.g. `"SELECT 5\0"`.
fn command_complete_frame(tag: &[u8]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::from(tag);
    body.push(0);
    frame(TAG_COMMAND_COMPLETE.byte(), &body)
}

/// Build an `EmptyQueryResponse` frame (no body).
fn empty_query_response_frame() -> std::vec::Vec<u8> {
    frame(TAG_EMPTY_QUERY_RESPONSE.byte(), &[])
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
    frame(TAG_ERROR_RESPONSE.byte(), &body)
}

// ------------------------------------------------------------------
// Correlator / ReplyId helpers.
// ------------------------------------------------------------------

fn raw(v: u64) -> NonZeroU64 {
    // DEF-145: raw(0) is a test bug; assert fires loud.
    assert!(v > 0, "raw(0) is a test bug — use raw(1..) for non-zero test correlators");
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
/// outbound bytes start with the `'Q'` tag and return them for
/// further assertions.
///
/// DEF-212 (Alt Y'): post-Phase-1a `push_or_panic` returns `()`;
/// bytes live in `wb`. SimpleQuery emits a single 'Q' frame (no
/// trailing Sync — Q is self-syncing per PG §55.2.4).
#[track_caller]
fn simple_query_setup(
    proto: &mut PgProtocol,
    reply: ReplyId<QueryKind>,
    wb: &mut WriteBuf,
) -> std::vec::Vec<u8> {
    proto.push_or_panic(
        bsql_pg_proto::push_command::SimpleQuery {
            sql: sql("SELECT 1"),
            reply,
        },
        wb,
    );
    let bytes = wb.as_bytes();
    assert!(!bytes.is_empty(), "SimpleQuery push must emit a Q frame; wb is empty");
    assert_eq!(
        bytes.first(),
        Some(&TAG_QUERY.byte()),
        "first byte of outbound must be `'Q'` (simple-query tag)",
    );
    bytes.to_vec()
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
    simple_query_setup(&mut proto, id(q_raw), &mut wb);

    // After push: state should be SimpleQueryAwaitingFirstResponse.
    assert!(matches!(
        proto.state(),
        ProtoState::SimpleQueryAwaitingFirstResponse(_),
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
                Reply::QueryComplete(p) => {
                    assert_eq!(p.command_tag.as_str(), "SELECT 0");
                    assert_eq!(p.tx_status, bsql_pg_proto::TxStatus::Idle);
                    // 1c-2a: 0-row SELECT delivers schema via Reply
                    // (no StreamRow to carry it).
                    assert!(
                        matches!(p.row_desc, Some(desc) if desc.is_empty()),
                        "0-row SELECT: row_desc must be Some(empty-desc), got {:?}", p.row_desc,
                    );
                }
                other => panic!("expected QueryComplete, got {other:?}"),
            }
        }
        other => panic!("expected DeliverReply, got {other:?}"),
    }
    assert!(matches!(proto.state(), ProtoState::Idle));
}

// DEF-154 (Y): `select_multiple_rows_stream_then_deliver` DELETED —
// row-bearing SELECT is covered end-to-end by
// `row_stream_spec::multi_row_select_end_to_end`. Post-(Y),
// `Action::StreamRow` is deleted + DataRow via `feed_bytes` is
// classified as `UnexpectedFrame`; feed_bytes is the control-path
// API (no row streaming). Use `iter_rows` for row-bearing
// responses.

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
        [Action::DeliverReply {
            id: delivered_id,
            value: Reply::QueryComplete(p),
        }] => {
            assert_eq!(*delivered_id, q_raw);
            assert_eq!(p.command_tag.as_str(), "INSERT 0 3");
            assert_eq!(p.tx_status, bsql_pg_proto::TxStatus::InTransaction);
            // 1c-2a: DML never received a 'T' frame — row_desc is None.
            // Critical invariant: push_command clears prior SELECT's
            // row_desc, so a DML following a SELECT gets None here,
            // not stale schema.
            assert!(
                p.row_desc.is_none(),
                "DML receives no RowDescription → row_desc must be None",
            );
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
        [Action::DeliverReply { value: Reply::QueryComplete(p), .. }] => {
            assert_eq!(
                p.command_tag.as_str(),
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

// DEF-154 (Y): `error_after_some_rows_emits_stream_then_fail`
// DELETED — migrated to `row_stream_spec::
// rows_before_mid_stream_error_are_preserved`, which tests the
// same invariant (server ErrorResponse after partial rows → rows
// still emit, FailReply replaces DeliverReply) on the `iter_rows`
// API (the only API supporting row streaming post-(Y)).

// ==================================================================
// (B) Tier-3 invariants — bad paths + push-state policy
// ==================================================================

/// DEF-198 invariant: SimpleQuery while another in flight is
/// structurally blocked at the public API. The original in-flight
/// state is preserved (caller must drive `feed_bytes` to drain).
#[test]
fn def198_simple_query_while_in_flight_blocked_at_compile_time() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let first_raw = raw(110);
    simple_query_setup(&mut proto, id(first_raw), &mut wb);

    assert!(
        proto.as_ready().is_none(),
        "DEF-198: as_ready must return None during in-flight SimpleQuery",
    );
    assert_eq!(
        proto.connection_status(),
        ConnectionStatus::Busy,
        "in-flight SimpleQuery classifies as ConnectionStatus::Busy",
    );
    // Original state preserved.
    assert!(matches!(
        proto.state(),
        ProtoState::SimpleQueryAwaitingFirstResponse(_),
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

/// DEF-198 invariant: SimpleQuery on Errored is structurally blocked.
/// `ConnectionStatus::Errored(kind)` exposes the underlying cause.
#[test]
fn def198_simple_query_on_errored_blocked_at_compile_time() {
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

    assert!(
        proto.as_ready().is_none(),
        "DEF-198: as_ready must return None on Errored",
    );
    match proto.connection_status() {
        ConnectionStatus::Errored(_kind) => {}
        other => panic!("expected ConnectionStatus::Errored(_), got {other:?}"),
    }
}

// DEF-154 (Y): `data_row_then_malformed_command_complete_preserves_row_bytes`
// DELETED — migrated to `row_stream_spec::
// rows_preserved_when_command_complete_malformed`.
//
// DEF-154 (Y): `zero_body_data_row_classified_as_malformed_data_row`
// DELETED — covered by `row_stream_spec::
// fast_path_empty_data_row_body_is_malformed` (same classification
// — `MalformedDataRow` — via `iter_rows` fast-path).

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
    let bad = frame(TAG_COMMAND_COMPLETE.byte(), b"SELECT 1");
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
/// `SimpleQueryAwaitingFirstResponse` or `SimpleQueryStreamingRows`)
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
            },
        )),
        "expected FailReply(UnexpectedFrame{{Z}}), got {actions:?}",
    );
    assert!(actions.iter().any(|a| matches!(a, Action::CloseSocket)));
    assert!(matches!(proto.state(), ProtoState::Errored(_)));
}

// DEF-154 (Y): `rows_across_multiple_feed_bytes_calls` DELETED —
// migrated to `row_stream_spec::rows_across_multiple_feed_calls`
// (iter_rows equivalent: `feed()` split across calls).
//
// DEF-154 (Y): `overflow_backpressure_preserves_delivery_across_calls`
// DELETED — obsolete post-(Y). The test pinned the behaviour of
// `MAX_STAGED_PER_CALL`-bounded backpressure on the `feed_bytes`
// row path; `iter_rows` pulls one event per `next_event` call,
// so there's no output-buffer overflow to backpressure against.
// Row throughput is now bounded only by the caller's loop rate.
//
// DEF-154 (Y): `stream_row_bytes_decode_via_data_row_ref` DELETED —
// migrated to `row_stream_spec::row_bytes_decode_via_data_row_ref`.
//
// DEF-154 (Y): `end_to_end_decode_typed_row` DELETED —
// migrated to `row_stream_spec::end_to_end_decode_typed_row`.

/// Invariant (1c-2a): a DML query following a SELECT must receive
/// `Reply::QueryComplete { row_desc: None }` — NOT the prior
/// SELECT's schema. This pins the `push_command` clear at line
/// `self.row_desc = None` (in `bsql_pg_proto::push_command::SimpleQuery` branch).
///
/// Without the clear, `feed_bytes` on the DML path would `copy` the
/// stale row_desc from `PgProtocol.row_desc` into the Reply, leaking
/// query 1's schema into query 2's result.
/// F19 regression (was "clears row_desc slot" pre-F19): after a
/// SELECT Q1 that terminates via the StreamingRows → AwaitingRfq →
/// Idle transitions, the schema lives ONLY inside those state
/// variants. By Idle time (Q1 done), the schema is gone
/// architecturally — there's no slot to clear. A following DML Q2
/// starts fresh at Idle → AwaitingFirstResponse → AwaitingRfq
/// (row_desc=None, no `T` frame arrived) → Idle. Its QueryComplete
/// payload carries `row_desc: None` by construction, not by discipline.
#[test]
fn dml_after_select_clears_row_desc() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();

    // Query 1: SELECT with 1 TEXT column. Schema lives in the
    // SimpleQueryStreamingRows { row_desc } state for the duration
    // of the stream; transitions to AwaitingRfq { row_desc: Some(..) }
    // on `C`; consumed on `Z` into DeliverReply(QueryComplete).
    // Post-Q1 state = Idle (no residual schema anywhere).
    //
    // DEF-154 (Y): row-bearing Q1 drives through `iter_rows`
    // (the sole row-streaming API post-(Y)). Drain the stream to
    // the terminal Complete event; the inner `flush_pending`
    // mechanism consumes the trailing Z so state returns to Idle.
    let q1_raw = raw(300);
    simple_query_setup(&mut proto, id(q1_raw), &mut wb);
    let mut q1_bytes = std::vec::Vec::new();
    q1_bytes.extend_from_slice(&row_description_frame(1));
    q1_bytes.extend_from_slice(&data_row_frame(b"hello"));
    q1_bytes.extend_from_slice(&command_complete_frame(b"SELECT 1"));
    q1_bytes.extend_from_slice(&rfq_frame(b'I'));
    {
        let mut stream = proto.iter_rows(&mut wb);
        if let Err(err) = stream.feed(&q1_bytes) {
            panic!("feed Q1 fits: {err:?}");
        }
        let mut saw_complete = false;
        for _ in 0..16 {
            match stream.next_event() {
                bsql_pg_proto::StreamItem::Complete { .. } => {
                    saw_complete = true;
                    break;
                }
                bsql_pg_proto::StreamItem::Row { .. }
                | bsql_pg_proto::StreamItem::NeedMore => continue,
                other => panic!("unexpected event on Q1: {other:?}"),
            }
        }
        assert!(saw_complete, "query 1 must deliver");
        // Drain the trailing Z via one more next_event so the
        // RowStream's flush_pending returns state to Idle before
        // drop.
        let _ = stream.next_event();
    }
    assert!(matches!(proto.state(), ProtoState::Idle), "Q1 post-drain state must be Idle, got {:?}", proto.state());

    // Query 2: DML path. No `T` frame → AwaitingRfq never gets a
    // schema in its row_desc field. QueryComplete carries None.
    let q2_raw = raw(301);
    simple_query_setup(&mut proto, id(q2_raw), &mut wb);
    let mut q2_bytes = std::vec::Vec::new();
    q2_bytes.extend_from_slice(&command_complete_frame(b"DELETE 3"));
    q2_bytes.extend_from_slice(&rfq_frame(b'I'));
    let out = proto.feed_bytes(&q2_bytes, &mut wb);
    match out.as_slice() {
        [Action::DeliverReply {
            value: Reply::QueryComplete(p),
            ..
        }] => {
            assert!(
                p.row_desc.is_none(),
                "DML following SELECT must NOT inherit prior schema; got {:?}", p.row_desc,
            );
        }
        other => panic!("expected single DeliverReply for DML, got {other:?}"),
    }
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

    assert_eq!(sent.first(), Some(&TAG_QUERY.byte()), "tag = 'Q'");
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
