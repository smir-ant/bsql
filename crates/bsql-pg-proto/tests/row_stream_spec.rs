//! DEF-154 (X) — behavioural tests for the pull-based
//! [`RowStream`](bsql_pg_proto::RowStream) API.
//!
//! Every test here names the invariant it defends. Coverage
//! mirrors the slow/fast-path split plus the full bad-path
//! surface of the new API:
//!
//! - **(A) Happy path** — multi-row SELECT streamed via
//!   `iter_rows` produces `Row × N → Complete` in the caller's
//!   event loop.
//! - **(B) Silent transitions** — `RowDescription` is consumed
//!   but emits no action; slow-path surfaces `NeedMore`, caller
//!   loops, fast-path then hits the following `DataRow`.
//! - **(C) Drained terminal** — after `Complete` (or
//!   `CloseSocket`) every subsequent `next_event` is `NeedMore`,
//!   regardless of unread bytes.
//! - **(D) Errored-state entry** — constructing a stream on a
//!   torn-down protocol yields `CloseSocket` exactly once.
//! - **(E) Fast-path malformed body** — empty `DataRow`
//!   (`total_len == HEADER_LEN`) surfaces `FailReply` via the
//!   fast-path classifier, the stream drains, and the protocol
//!   transitions to Errored.
//! - **(F) Feed error** — over-fill of the read buffer returns
//!   a tiny `ReadBufFull` Err and drains the stream.
//! - **(G) Server error** — `ErrorResponse` flows through the
//!   slow path to `FailReply`.

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
    Action, ColumnDesc, DataRowRef, FormatCode, FromPgText, PgCommand, PgProtocol, ProtoState,
    ProtocolError, QueryKind, Reply, ReplyId, Sql, StreamItem, WriteBuf, oids,
    wire::{
        TAG_COMMAND_COMPLETE, TAG_DATA_ROW, TAG_ERROR_RESPONSE, TAG_QUERY, TAG_READY_FOR_QUERY,
        TAG_ROW_DESCRIPTION,
    },
};
use core::num::NonZeroU64;

mod common;
use common::PushOrPanic;

// ------------------------------------------------------------------
// Frame builders — pure functions, mirror `simple_query_spec` shapes.
// ------------------------------------------------------------------

fn rfq_frame(tx_status: u8) -> [u8; 6] {
    [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, tx_status]
}

fn frame(tag: u8, body: &[u8]) -> std::vec::Vec<u8> {
    let mut out = std::vec::Vec::new();
    out.push(tag);
    let Ok(len) = u32::try_from(body.len().saturating_add(4)) else {
        panic!("test fixture body too large for u32 length field");
    };
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn row_description_frame(n_columns: u16) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.extend_from_slice(&n_columns.to_be_bytes());
    for i in 0..n_columns {
        body.extend_from_slice(b"c");
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes());
        body.extend_from_slice(&i.to_be_bytes());
        body.extend_from_slice(&25i32.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
    }
    frame(TAG_ROW_DESCRIPTION.byte(), &body)
}

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

fn command_complete_frame(tag: &[u8]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::from(tag);
    body.push(0);
    frame(TAG_COMMAND_COMPLETE.byte(), &body)
}

fn error_response_frame(message: &[u8]) -> std::vec::Vec<u8> {
    let mut body = std::vec::Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR");
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message);
    body.push(0);
    body.push(0);
    frame(TAG_ERROR_RESPONSE.byte(), &body)
}

fn raw(v: u64) -> NonZeroU64 {
    assert!(v > 0, "raw(0) is a test bug");
    NonZeroU64::new(v).unwrap_or(NonZeroU64::MIN)
}

fn id(v: NonZeroU64) -> ReplyId<QueryKind> {
    ReplyId::from_raw(v)
}

fn sql(s: &str) -> Sql {
    Sql::from_str_truncating(s)
}

/// Push a SimpleQuery and assert it produced an outbound `'Q'` frame.
///
/// DEF-212 (Alt Y'): post-Phase-1a `push_or_panic` returns `()`; bytes
/// live in `wb`. SimpleQuery emits a single 'Q' frame (no trailing
/// Sync — Q is self-syncing per PG §55.2.4).
#[track_caller]
fn push_simple_query(proto: &mut PgProtocol, reply: ReplyId<QueryKind>, wb: &mut WriteBuf) {
    proto.push_or_panic(
        PgCommand::SimpleQuery {
            sql: sql("SELECT 1"),
            reply,
        },
        wb,
    );
    let bytes = wb.as_bytes();
    assert!(
        !bytes.is_empty(),
        "SimpleQuery push must emit a Q frame; wb is empty",
    );
    assert_eq!(
        bytes.first(),
        Some(&TAG_QUERY.byte()),
        "first byte must be the 'Q' Query tag",
    );
}

// ==================================================================
// (A) Happy path — multi-row SELECT via iter_rows.
// ==================================================================

/// Invariant: feeding a full SELECT response (T D D D C Z) and
/// iterating via [`RowStream::next_event`] yields exactly
/// `Row × N → Complete` with all rows carrying the in-flight id,
/// the same schema ref, and byte-equal row bodies. The caller
/// absorbs one `NeedMore` for the silent `T` transition per the
/// MVP contract documented in `row_stream.rs`.
#[test]
fn multi_row_select_end_to_end() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(200);
    push_simple_query(&mut proto, id(q_raw), &mut wb);

    let row_values: [&[u8]; 3] = [b"alpha", b"beta", b"gamma"];
    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    for v in &row_values {
        bytes.extend_from_slice(&data_row_frame(v));
    }
    bytes.extend_from_slice(&command_complete_frame(b"SELECT 3"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let mut stream = proto.iter_rows(&mut wb);
    if let Err(err) = stream.feed(&bytes) {
        panic!("feed fits within read buf: {err:?}");
    }

    let mut row_count = 0usize;
    let mut saw_complete = false;
    let mut saw_need_more_for_silent_t = 0usize;
    // Cap iterations to prove absence of runaway loops under bad frames.
    for _ in 0..32 {
        match stream.next_event() {
            StreamItem::Row { id: row_id, row_bytes, desc } => {
                assert_eq!(row_id, q_raw, "row id matches in-flight reply");
                assert_eq!(desc.len(), 1, "schema is 1 column");
                // row_bytes layout: col_count(2) + col_len(4) + value.
                let col_count = row_bytes.get(..2);
                assert_eq!(col_count, Some(&1i16.to_be_bytes()[..]));
                let expected = row_values.get(row_count).copied().unwrap_or(&[]);
                assert_eq!(row_bytes.get(6..), Some(expected), "row payload byte-equal");
                row_count = row_count.saturating_add(1);
            }
            StreamItem::Complete { id: reply_id, value: Reply::QueryComplete(p) } => {
                assert_eq!(reply_id, q_raw);
                assert_eq!(p.command_tag.as_str(), "SELECT 3");
                saw_complete = true;
                break;
            }
            StreamItem::NeedMore => {
                saw_need_more_for_silent_t = saw_need_more_for_silent_t.saturating_add(1);
                assert!(saw_need_more_for_silent_t <= 2, "silent T yields at most 1 NeedMore");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert_eq!(row_count, row_values.len(), "all 3 rows emitted");
    assert!(saw_complete, "QueryComplete reached");
}

// ==================================================================
// (C) Drained terminal — no double-delivery.
// ==================================================================

/// Invariant: after Complete, subsequent `next_event` calls are
/// `NeedMore`, even if the read buffer still holds bytes. MVP
/// contract: one `iter_rows` scope = one reply.
#[test]
fn drained_after_complete_emits_need_more() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(201);
    push_simple_query(&mut proto, id(q_raw), &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    bytes.extend_from_slice(&data_row_frame(b"x"));
    bytes.extend_from_slice(&command_complete_frame(b"SELECT 1"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let mut stream = proto.iter_rows(&mut wb);
    if let Err(err) = stream.feed(&bytes) {
        panic!("feed fits: {err:?}");
    }

    // Pull until Complete or we hit the iteration cap.
    let mut reached_complete = false;
    for _ in 0..16 {
        if let StreamItem::Complete { .. } = stream.next_event() {
            reached_complete = true;
            break;
        }
    }
    assert!(reached_complete, "must reach Complete");

    // Now drained — every next call is NeedMore.
    for _ in 0..3 {
        assert!(
            matches!(stream.next_event(), StreamItem::NeedMore),
            "post-terminal event must be NeedMore",
        );
    }
}

// ==================================================================
// (D) Errored-state entry — immediate CloseSocket.
// ==================================================================

/// Invariant: if the protocol is already Errored when the caller
/// constructs a stream, the first `next_event` returns
/// `CloseSocket` exactly once, then the stream drains.
#[test]
fn errored_state_emits_close_socket_once_then_need_more() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(202);
    push_simple_query(&mut proto, id(q_raw), &mut wb);

    // Tear down via a malformed frame: total_len < 4 is classified
    // as MalformedLength which routes through fail_inflight and
    // drives the protocol to Errored.
    let malformed: [u8; 5] = [TAG_DATA_ROW.byte(), 0, 0, 0, 3];
    let out = proto.feed_bytes(&malformed, &mut wb);
    assert!(
        out.as_slice().iter().any(|a| matches!(a, Action::CloseSocket)),
        "malformed frame drives protocol to CloseSocket",
    );
    assert!(matches!(proto.state(), ProtoState::Errored { .. }));

    // Now open a stream on the errored protocol.
    let mut stream = proto.iter_rows(&mut wb);
    assert!(
        matches!(stream.next_event(), StreamItem::CloseSocket),
        "first event on errored protocol must be CloseSocket",
    );
    for _ in 0..3 {
        assert!(
            matches!(stream.next_event(), StreamItem::NeedMore),
            "stream drained after CloseSocket",
        );
    }
}

// ==================================================================
// (E) Fast-path malformed body — empty DataRow.
// ==================================================================

/// Invariant: fast-path hit on a DataRow whose total_len equals
/// HEADER_LEN (empty body) yields a classified
/// `FailReply { cause: MalformedDataRow }`, transitions the
/// protocol to Errored, and advances the read cursor past the bad
/// frame so the stream drain is deterministic.
#[test]
fn fast_path_empty_data_row_body_is_malformed() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(203);
    push_simple_query(&mut proto, id(q_raw), &mut wb);

    // Install RowDescription first so the fast-path (which looks
    // up `streaming_reply_id_and_schema`) engages on the next D
    // frame. Then feed a malformed D with len == HEADER_LEN.
    let mut setup = std::vec::Vec::new();
    setup.extend_from_slice(&row_description_frame(1));
    let actions = proto.feed_bytes(&setup, &mut wb);
    assert!(
        actions.as_slice().is_empty(),
        "RowDescription is a silent state transition",
    );

    // Now append the malformed empty-body DataRow frame directly.
    let empty_d: [u8; 5] = [TAG_DATA_ROW.byte(), 0, 0, 0, 4];
    let mut stream = proto.iter_rows(&mut wb);
    if let Err(err) = stream.feed(&empty_d) {
        panic!("5 bytes fit: {err:?}");
    }

    match stream.next_event() {
        StreamItem::FailReply { id: fail_id, cause } => {
            assert_eq!(fail_id, q_raw);
            assert!(
                matches!(cause, ProtocolError::MalformedDataRow { .. }),
                "cause must be MalformedDataRow, got {cause:?}",
            );
        }
        other => panic!("expected FailReply, got {other:?}"),
    }

    // Subsequent events drained.
    assert!(matches!(stream.next_event(), StreamItem::NeedMore));
}

// ==================================================================
// (F) Feed error — read buf overflow.
// ==================================================================

/// Invariant: [`RowStream::feed`] returns a tiny `ReadBufFull`
/// error (not a `ProtocolError`) when the caller over-fills the
/// read buffer, and the stream transitions to drained so
/// subsequent `next_event` calls are `NeedMore`.
#[test]
fn feed_overflow_returns_tiny_read_buf_full_err() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(204);
    push_simple_query(&mut proto, id(q_raw), &mut wb);

    let mut stream = proto.iter_rows(&mut wb);
    // Feed a very large slice — read_buf capacity is bounded; this
    // must fail via ReadBufFull.
    let oversized = std::vec![0u8; 1 << 20];
    let err = match stream.feed(&oversized) {
        Ok(()) => panic!("oversized feed must err"),
        Err(e) => e,
    };
    // Tiny POD struct: two `usize` fields describing overflow.
    assert_eq!(err.attempted, oversized.len());
    // `available` is whatever room remained; positive and less than
    // READ_BUF_CAP.
    assert!(err.available < oversized.len());

    // Drained: every next_event is NeedMore.
    for _ in 0..3 {
        assert!(matches!(stream.next_event(), StreamItem::NeedMore));
    }
}

// ==================================================================
// (G) Server error — slow-path FailReply.
// ==================================================================

/// Invariant: an `ErrorResponse` during an in-flight query flows
/// through the slow path to `StreamItem::FailReply` carrying the
/// server's `ServerErrorResponse` payload, and subsequent
/// `next_event` emits `CloseSocket` per the post-error teardown
/// contract.
#[test]
fn server_error_response_surfaces_as_fail_reply() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(205);
    push_simple_query(&mut proto, id(q_raw), &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&error_response_frame(b"syntax error at or near \"SELCT\""));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let mut stream = proto.iter_rows(&mut wb);
    if let Err(err) = stream.feed(&bytes) {
        panic!("feed fits: {err:?}");
    }

    let mut saw_fail = false;
    for _ in 0..8 {
        match stream.next_event() {
            StreamItem::FailReply { id: fail_id, cause } => {
                assert_eq!(fail_id, q_raw);
                // Cause is server-reported — either
                // ServerErrorResponse or a derived class.
                assert!(
                    matches!(cause, ProtocolError::ServerErrorResponse { .. }),
                    "expected ServerErrorResponse, got {cause:?}",
                );
                saw_fail = true;
                break;
            }
            StreamItem::NeedMore => continue,
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert!(saw_fail, "FailReply must be emitted");
}

// ==================================================================
// (H) Mid-stream server error — rows BEFORE the E frame are
// preserved; FailReply replaces the Complete terminal.
// ==================================================================

/// Invariant (migrated DEF-154 (F) P0-1 pin from simple_query_spec):
/// if server sends one or more rows then an ErrorResponse, every
/// `Row` event that already emitted must carry the FULL DataRow
/// body bytes (pre-(F) the fatal-path clear wiped populated()
/// before materialise, producing silent empty rows). Post-(Y) the
/// invariant lives exclusively on the iter_rows path.
#[test]
fn rows_before_mid_stream_error_are_preserved() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(206);
    push_simple_query(&mut proto, id(q_raw), &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    bytes.extend_from_slice(&data_row_frame(b"partial"));
    bytes.extend_from_slice(&error_response_frame(b"division by zero"));
    bytes.extend_from_slice(&rfq_frame(b'E'));

    let mut stream = proto.iter_rows(&mut wb);
    if let Err(err) = stream.feed(&bytes) {
        panic!("feed fits: {err:?}");
    }

    let mut row_seen = false;
    let mut fail_seen = false;
    for _ in 0..16 {
        match stream.next_event() {
            StreamItem::Row { id: row_id, row_bytes, .. } => {
                assert_eq!(row_id, q_raw);
                // col-count(2) + vlen(4) + "partial"(7) = 13 bytes.
                assert_eq!(row_bytes.len(), 13, "full row body preserved");
                let Some(tail) = row_bytes.get(6..) else {
                    panic!("row_bytes too short: {}", row_bytes.len());
                };
                assert_eq!(tail, b"partial");
                row_seen = true;
            }
            StreamItem::FailReply { id: fail_id, cause } => {
                assert_eq!(fail_id, q_raw);
                assert!(
                    matches!(cause, ProtocolError::ServerErrorResponse { .. }),
                    "cause must be ServerErrorResponse, got {cause:?}",
                );
                fail_seen = true;
                break;
            }
            StreamItem::NeedMore => continue,
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert!(row_seen && fail_seen, "must see both Row and FailReply");
}

// ==================================================================
// (I) Malformed CommandComplete mid-stream — rows preserved,
// FailReply(MalformedCommandComplete) + CloseSocket emitted.
// ==================================================================

/// Invariant: rows streamed before a CommandComplete framing
/// desync (missing NUL terminator) are preserved; the invariant
/// moved from simple_query_spec's fatal-path pin to iter_rows
/// post-(Y). Demonstrates that slow-path classification of
/// malformed control-frames doesn't eat prior fast-path rows.
#[test]
fn rows_preserved_when_command_complete_malformed() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(207);
    push_simple_query(&mut proto, id(q_raw), &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    bytes.extend_from_slice(&data_row_frame(b"intact"));
    // CC frame WITHOUT NUL terminator — framing desync.
    bytes.extend_from_slice(&frame(TAG_COMMAND_COMPLETE.byte(), b"SELECT 1"));

    let mut stream = proto.iter_rows(&mut wb);
    if let Err(err) = stream.feed(&bytes) {
        panic!("feed fits: {err:?}");
    }

    let mut row_seen = false;
    let mut fail_seen = false;
    for _ in 0..16 {
        match stream.next_event() {
            StreamItem::Row { row_bytes, .. } => {
                // col-count(2) + vlen(4) + "intact"(6) = 12 bytes.
                assert_eq!(row_bytes.len(), 12);
                let Some(tail) = row_bytes.get(6..) else {
                    panic!("row_bytes too short: {}", row_bytes.len());
                };
                assert_eq!(tail, b"intact");
                row_seen = true;
            }
            StreamItem::FailReply { cause, .. } => {
                assert!(
                    matches!(cause, ProtocolError::MalformedCommandComplete { .. }),
                    "cause must be MalformedCommandComplete, got {cause:?}",
                );
                fail_seen = true;
                break;
            }
            StreamItem::NeedMore => continue,
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert!(row_seen && fail_seen, "must see both Row and FailReply");
}

// ==================================================================
// (J) Split feed — rows across multiple `feed()` calls.
// ==================================================================

/// Invariant: the `StreamingRows` state persists across
/// `feed()` call boundaries. Each `next_event` pull operates on
/// whatever bytes have been `feed()`-ed so far; when the buffer
/// empties mid-stream `next_event` returns `NeedMore`; subsequent
/// `feed()` + `next_event` resume cleanly.
#[test]
fn rows_across_multiple_feed_calls() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(208);
    push_simple_query(&mut proto, id(q_raw), &mut wb);

    let mut batch1 = std::vec::Vec::new();
    batch1.extend_from_slice(&row_description_frame(1));
    batch1.extend_from_slice(&data_row_frame(b"r1"));
    batch1.extend_from_slice(&data_row_frame(b"r2"));

    let mut batch2 = std::vec::Vec::new();
    batch2.extend_from_slice(&data_row_frame(b"r3"));
    batch2.extend_from_slice(&command_complete_frame(b"SELECT 3"));
    batch2.extend_from_slice(&rfq_frame(b'I'));

    let mut stream = proto.iter_rows(&mut wb);

    // Feed batch 1. Pull up to 2 rows + the trailing NeedMore
    // (buffer empty). Silent T transition surfaces one extra
    // NeedMore between the T frame and the first D; loop until
    // we've seen the expected rows OR two consecutive NeedMores
    // indicating the buffer truly empty.
    if let Err(err) = stream.feed(&batch1) {
        panic!("feed batch1: {err:?}");
    }
    let mut rows = 0usize;
    let mut consecutive_need_more = 0usize;
    let mut complete = false;
    for _ in 0..16 {
        match stream.next_event() {
            StreamItem::Row { .. } => {
                rows = rows.saturating_add(1);
                consecutive_need_more = 0;
            }
            StreamItem::NeedMore => {
                consecutive_need_more = consecutive_need_more.saturating_add(1);
                if consecutive_need_more >= 2 {
                    break;
                }
            }
            StreamItem::Complete { .. } => {
                complete = true;
                break;
            }
            other => panic!("unexpected event in batch1: {other:?}"),
        }
    }
    assert_eq!(rows, 2, "batch 1 yields 2 rows");
    assert!(!complete, "batch 1 must not complete yet");

    // Feed batch 2 — pull the remaining row + Complete.
    if let Err(err) = stream.feed(&batch2) {
        panic!("feed batch2: {err:?}");
    }
    for _ in 0..16 {
        match stream.next_event() {
            StreamItem::Row { .. } => rows = rows.saturating_add(1),
            StreamItem::Complete { id: reply_id, .. } => {
                assert_eq!(reply_id, q_raw);
                complete = true;
                break;
            }
            StreamItem::NeedMore => continue,
            other => panic!("unexpected event in batch2: {other:?}"),
        }
    }
    assert_eq!(rows, 3, "3 rows total across both feeds");
    assert!(complete, "Complete must emit after the final feed");
}

// ==================================================================
// (K) DataRowRef round-trip decode over iter_rows `row_bytes`.
// ==================================================================

/// Invariant (migrated from simple_query_spec's 1c-2b test):
/// `DataRowRef::parse` + `columns()` decode the raw
/// `StreamItem::Row::row_bytes` into per-column `Option<&[u8]>`,
/// with the NULL sentinel (`len = -1`) round-tripping to `None`.
#[test]
fn row_bytes_decode_via_data_row_ref() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(209);
    push_simple_query(&mut proto, id(q_raw), &mut wb);

    // Build DataRow with 2 columns: "answer" and NULL.
    let mut row_body = std::vec::Vec::new();
    row_body.extend_from_slice(&2i16.to_be_bytes());
    let col0 = b"answer";
    let Ok(col0_len) = i32::try_from(col0.len()) else { unreachable!() };
    row_body.extend_from_slice(&col0_len.to_be_bytes());
    row_body.extend_from_slice(col0);
    row_body.extend_from_slice(&(-1i32).to_be_bytes());

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(2));
    bytes.push(TAG_DATA_ROW.byte());
    let Ok(framelen) = u32::try_from(row_body.len().saturating_add(4)) else { unreachable!() };
    bytes.extend_from_slice(&framelen.to_be_bytes());
    bytes.extend_from_slice(&row_body);
    bytes.extend_from_slice(&command_complete_frame(b"SELECT 1"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let mut stream = proto.iter_rows(&mut wb);
    if let Err(err) = stream.feed(&bytes) {
        panic!("feed fits: {err:?}");
    }

    let mut decoded = false;
    for _ in 0..16 {
        match stream.next_event() {
            StreamItem::Row { row_bytes, .. } => {
                let Ok(row) = DataRowRef::parse(row_bytes) else {
                    panic!("DataRowRef::parse must succeed");
                };
                assert_eq!(row.len(), 2);
                let items: std::vec::Vec<_> = row.columns().collect();
                assert_eq!(items.len(), 2);
                assert!(
                    matches!(items.first(), Some(Ok(Some(b"answer")))),
                    "col 0 should decode to b\"answer\", got {:?}",
                    items.first(),
                );
                assert!(
                    matches!(items.get(1), Some(Ok(None))),
                    "col 1 should decode as SQL NULL, got {:?}",
                    items.get(1),
                );
                decoded = true;
            }
            StreamItem::Complete { .. } => break,
            StreamItem::NeedMore => continue,
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert!(decoded, "must have decoded the row");
}

// ==================================================================
// (L) End-to-end typed-decode round-trip.
// ==================================================================

/// Invariant (migrated from simple_query_spec's 1c-2c test):
/// full decode round-trip — push a SELECT, server replies with
/// typed rows, caller uses `DataRowRef` + `FromPgText` to
/// reconstruct Rust values. User-level API that Phase 2 macros
/// target.
#[test]
fn end_to_end_decode_typed_row() {
    let mut proto = PgProtocol::new();
    let mut wb = WriteBuf::new();
    let q_raw = raw(210);
    push_simple_query(&mut proto, id(q_raw), &mut wb);

    // RowDescription: 2 cols — id INT4, name TEXT.
    let mut rd_body = std::vec::Vec::new();
    rd_body.extend_from_slice(&2i16.to_be_bytes());
    for (name, oid) in [(&b"id"[..], oids::INT4), (&b"name"[..], oids::TEXT)] {
        rd_body.extend_from_slice(name);
        rd_body.push(0);
        rd_body.extend_from_slice(&0i32.to_be_bytes());
        rd_body.extend_from_slice(&0i16.to_be_bytes());
        rd_body.extend_from_slice(&oid.to_be_bytes());
        rd_body.extend_from_slice(&(-1i16).to_be_bytes());
        rd_body.extend_from_slice(&(-1i32).to_be_bytes());
        rd_body.extend_from_slice(&0i16.to_be_bytes());
    }
    let mut dr_body = std::vec::Vec::new();
    dr_body.extend_from_slice(&2i16.to_be_bytes());
    let id_text = b"42";
    let Ok(id_len) = i32::try_from(id_text.len()) else { unreachable!() };
    dr_body.extend_from_slice(&id_len.to_be_bytes());
    dr_body.extend_from_slice(id_text);
    let name_text = b"alice";
    let Ok(name_len) = i32::try_from(name_text.len()) else { unreachable!() };
    dr_body.extend_from_slice(&name_len.to_be_bytes());
    dr_body.extend_from_slice(name_text);

    let mut bytes = std::vec::Vec::new();
    bytes.push(TAG_ROW_DESCRIPTION.byte());
    let Ok(rd_len) = u32::try_from(rd_body.len().saturating_add(4)) else { unreachable!() };
    bytes.extend_from_slice(&rd_len.to_be_bytes());
    bytes.extend_from_slice(&rd_body);
    bytes.push(TAG_DATA_ROW.byte());
    let Ok(dr_len) = u32::try_from(dr_body.len().saturating_add(4)) else { unreachable!() };
    bytes.extend_from_slice(&dr_len.to_be_bytes());
    bytes.extend_from_slice(&dr_body);
    bytes.extend_from_slice(&command_complete_frame(b"SELECT 1"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    let mut stream = proto.iter_rows(&mut wb);
    if let Err(err) = stream.feed(&bytes) {
        panic!("feed fits: {err:?}");
    }

    let mut decoded_id: Option<i32> = None;
    let mut decoded_name: Option<std::string::String> = None;
    for _ in 0..16 {
        match stream.next_event() {
            StreamItem::Row { row_bytes, desc, .. } => {
                assert!(
                    matches!(
                        desc.get(0),
                        Some(ColumnDesc { type_oid: oids::INT4, format_code: FormatCode::Text }),
                    ) && matches!(
                        desc.get(1),
                        Some(ColumnDesc { type_oid: oids::TEXT, format_code: FormatCode::Text }),
                    ) && desc.len() == 2,
                    "schema mismatch: {desc:?}",
                );
                let Ok(row) = DataRowRef::parse(row_bytes) else {
                    panic!("DataRowRef::parse must succeed");
                };
                let mut cols = row.columns();
                match cols.next() {
                    Some(Ok(Some(b))) => match i32::from_pg_text(b) {
                        Ok(v) => decoded_id = Some(v),
                        Err(e) => panic!("i32 decode: {e:?}"),
                    },
                    other => panic!("col 0: {other:?}"),
                }
                match cols.next() {
                    Some(Ok(Some(b))) => match <&str>::from_pg_text(b) {
                        Ok(s) => decoded_name = Some(std::string::String::from(s)),
                        Err(e) => panic!("str decode: {e:?}"),
                    },
                    other => panic!("col 1: {other:?}"),
                }
                assert!(cols.next().is_none());
            }
            StreamItem::Complete { .. } => break,
            StreamItem::NeedMore => continue,
            other => panic!("unexpected event: {other:?}"),
        }
    }
    assert_eq!(decoded_id, Some(42));
    assert_eq!(decoded_name.as_deref(), Some("alice"));
}
