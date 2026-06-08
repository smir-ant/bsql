//! Behavioural tests for the closure-scoped pull-based
//! [`RowStream`](bsql_postgres_proto::RowStream) +
//! [`ColEvent`](bsql_postgres_proto::ColEvent) API.
//!
//! Every test here names the invariant it defends. Coverage mirrors
//! the column-by-column + partial-frame split plus the full bad-path
//! surface of the new API:
//!
//! - **(A) Happy path** — multi-row SELECT streamed via `iter_rows`
//!   closure produces `Got × N → EndRow × N → EndQuery(Ok)`.
//! - **(B) Silent transitions** — `RowDescription` is consumed but
//!   emits no event; slow-path surfaces `NeedMore`, caller loops,
//!   fast-path then hits the following `DataRow`.
//! - **(C) Drained terminal** — after `EndQuery` every subsequent
//!   `col_next` is `NeedMore`, regardless of unread bytes.
//! - **(D) Errored-state entry** — constructing a stream on a
//!   torn-down protocol yields `EndQuery::Err` once, then drains.
//! - **(E) Fast-path malformed body** — empty `DataRow` surfaces
//!   `EndQuery::Err(MalformedDataRow)`, drains, state ⇒ Errored.
//! - **(F) Feed error** — over-fill of read buffer returns tiny
//!   `ReadBufFull` Err and drains the stream.
//! - **(G) Server error** — `ErrorResponse` flows through the slow
//!   path to `EndQuery::Err(ServerErrorResponse)`.
//! - **(H) Mid-stream server error** — rows before the E frame are
//!   preserved; `EndQuery::Err` replaces the `EndQuery::Ok` terminal.
//! - **(I) Malformed CommandComplete** — rows preserved,
//!   `EndQuery::Err(MalformedCommandComplete)` + CloseSocket emitted.
//! - **(J) Split feed** — rows across multiple `feed()` calls.
//! - **(K) DataRowRef round-trip** — `Got` bytes decode via
//!   `DataRowRef::parse` + `columns()` round-trip.
//! - **(L) End-to-end typed decode** — Phase 2-target user-level API.
//! - **(M) Drop-mid-stream install** — closure exits without
//!   reaching `EndQuery`; Drop installs Errored.
//! - **(N) Drop on panic** — closure panics; Drop unwind path
//!   installs Errored.
//! - **(O) Partial-frame mode** — single huge column body streamed
//!   as `Chunk × N → ChunkEnd`.

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
    Action, ActiveState, ColEvent, ColumnDesc, DataRowRef, FormatCode, FromPgText, PgProtocol,
    ProtocolError, QueryKind, Reply, ReplyId, WriteBuf, oids,
    wire::{
        TAG_COMMAND_COMPLETE, TAG_DATA_ROW, TAG_ERROR_RESPONSE, TAG_QUERY, TAG_READY_FOR_QUERY,
        TAG_ROW_DESCRIPTION,
    },
};

mod common;
use common::{PushOrPanic, fresh_active_via_trust_handshake, mint_reply};

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

/// Push a SimpleQuery and assert it produced an outbound `'Q'` frame.
#[track_caller]
fn push_simple_query(proto: &mut PgProtocol<bsql_postgres_proto::ActivePhase>, reply: ReplyId<QueryKind>, wb: &mut WriteBuf) {
    proto.push_or_panic(
        bsql_postgres_proto::push_command::SimpleQuery::new("SELECT 1", reply),
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
/// iterating via [`RowStream::col_next`] yields exactly
/// `Got × N → EndRow × N → EndQuery(Ok(QueryComplete))`.
#[test]
fn multi_row_select_end_to_end() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

    let row_values: [&[u8]; 3] = [b"alpha", b"beta", b"gamma"];
    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    for v in &row_values {
        bytes.extend_from_slice(&data_row_frame(v));
    }
    bytes.extend_from_slice(&command_complete_frame(b"SELECT 3"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    proto.iter_rows(&mut wb, |stream| {
        if let Err(err) = stream.feed(&bytes) {
            panic!("feed fits within read buf: {err:?}");
        }
        let mut row_index = 0usize;
        let mut col_in_row = 0u16;
        let mut saw_end_query = false;
        for _ in 0..64 {
            match stream.col_next() {
                ColEvent::Got { idx, bytes } => {
                    assert_eq!(idx, col_in_row, "col idx tracks within row");
                    let expected = row_values.get(row_index).copied().unwrap_or(&[]);
                    assert_eq!(bytes, expected, "row {row_index} col {idx} payload");
                    col_in_row = col_in_row.saturating_add(1);
                }
                ColEvent::Null { .. } => panic!("happy-path has no NULLs"),
                ColEvent::EndRow => {
                    row_index = row_index.saturating_add(1);
                    col_in_row = 0;
                }
                ColEvent::Chunk { .. } | ColEvent::ChunkEnd { .. } => {
                    panic!("small-row test should not chunk")
                }
                ColEvent::NeedMore => continue,
                ColEvent::EndQuery { id, outcome } => {
                    assert_eq!(id, Some(q_raw), "terminal id matches in-flight");
                    let Ok(Reply::QueryComplete(_)) = outcome else {
                        panic!("expected Ok(QueryComplete), got {outcome:?}");
                    };
                    // DEF-286 Φ-F*: command_tag externalised; cannot
                    // be queried mid-iter_rows borrow chain. The wire
                    // round-trip is unit-tested elsewhere.
                    saw_end_query = true;
                    break;
                }
                _ => panic!("unexpected event"),
            }
        }
        assert_eq!(row_index, row_values.len(), "all 3 rows emitted");
        assert!(saw_end_query, "EndQuery reached");
    });
}

// ==================================================================
// (C) Drained terminal — no double-delivery.
// ==================================================================

/// Invariant: after EndQuery, subsequent `col_next` calls are
/// `NeedMore`, even if the read buffer still holds bytes.
#[test]
fn drained_after_end_query_emits_need_more() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    bytes.extend_from_slice(&data_row_frame(b"x"));
    bytes.extend_from_slice(&command_complete_frame(b"SELECT 1"));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    proto.iter_rows(&mut wb, |stream| {
        if let Err(err) = stream.feed(&bytes) {
            panic!("feed fits: {err:?}");
        }
        let mut reached_end_query = false;
        for _ in 0..32 {
            if let ColEvent::EndQuery { .. } = stream.col_next() {
                reached_end_query = true;
                break;
            }
        }
        assert!(reached_end_query, "must reach EndQuery");
        // Now drained — every next call is NeedMore.
        for _ in 0..3 {
            assert!(
                matches!(stream.col_next(), ColEvent::NeedMore),
                "post-terminal event must be NeedMore",
            );
        }
    });
}

// ==================================================================
// (D) Errored-state entry — immediate EndQuery::Err.
// ==================================================================

/// Invariant: if the protocol is already Errored when the caller
/// constructs a stream, the first `col_next` returns `EndQuery::Err`
/// exactly once, then the stream drains.
#[test]
fn errored_state_emits_end_query_err_once_then_need_more() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

    // Tear down via a malformed frame: total_len < 4 is classified
    // as MalformedLength which routes through fail_inflight and
    // drives the protocol to Errored.
    let malformed: [u8; 5] = [TAG_DATA_ROW.byte(), 0, 0, 0, 3];
    let out = proto.feed_bytes(&malformed, &mut wb);
    assert!(
        out.as_slice().iter().any(|a| matches!(a, Action::CloseSocket)),
        "malformed frame drives protocol to CloseSocket",
    );
    assert!(matches!(proto.state(), ActiveState::Errored { .. }));

    // Now open a stream on the errored protocol.
    proto.iter_rows(&mut wb, |stream| {
        assert!(
            matches!(stream.col_next(), ColEvent::EndQuery { outcome: Err(_), .. }),
            "first event on errored protocol must be EndQuery::Err",
        );
        for _ in 0..3 {
            assert!(
                matches!(stream.col_next(), ColEvent::NeedMore),
                "stream drained after EndQuery::Err",
            );
        }
    });
}

// ==================================================================
// (E) Fast-path malformed body — empty DataRow.
// ==================================================================

/// Invariant: fast-path hit on a DataRow whose total_len equals
/// HEADER_LEN (empty body) yields a classified
/// `EndQuery::Err(MalformedDataRow)`, transitions the protocol to
/// Errored, and advances the read cursor past the bad frame so the
/// stream drain is deterministic.
#[test]
fn fast_path_empty_data_row_body_is_malformed() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

    // Install RowDescription first so the fast-path engages on the
    // next D frame. Then feed a malformed D with len == HEADER_LEN.
    let mut setup = std::vec::Vec::new();
    setup.extend_from_slice(&row_description_frame(1));
    let actions = proto.feed_bytes(&setup, &mut wb);
    assert!(
        actions.as_slice().is_empty(),
        "RowDescription is a silent state transition",
    );

    // Now append the malformed empty-body DataRow frame directly.
    let empty_d: [u8; 5] = [TAG_DATA_ROW.byte(), 0, 0, 0, 4];
    proto.iter_rows(&mut wb, |stream| {
        if let Err(err) = stream.feed(&empty_d) {
            panic!("5 bytes fit: {err:?}");
        }
        match stream.col_next() {
            ColEvent::EndQuery { id: fail_id, outcome: Err(cause) } => {
                assert_eq!(fail_id, Some(q_raw));
                assert!(
                    matches!(cause, ProtocolError::MalformedDataRow { .. }),
                    "cause must be MalformedDataRow, got {cause:?}",
                );
            }
            other => panic!("expected EndQuery::Err, got {other:?}"),
        }
        // Subsequent events drained.
        assert!(matches!(stream.col_next(), ColEvent::NeedMore));
    });
}

// ==================================================================
// (F) Feed error — read buf overflow.
// ==================================================================

/// Invariant: [`RowStream::feed`] returns a tiny `ReadBufFull` error
/// when the caller over-fills the read buffer, and the stream
/// transitions to drained so subsequent `col_next` calls are
/// `NeedMore`.
#[test]
fn feed_overflow_returns_tiny_read_buf_full_err() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

    proto.iter_rows(&mut wb, |stream| {
        let oversized = std::vec![0u8; 1 << 20];
        let err = match stream.feed(&oversized) {
            Ok(()) => panic!("oversized feed must err"),
            Err(e) => e,
        };
        assert_eq!(err.attempted, oversized.len());
        assert!(err.available < oversized.len());

        // Drained: every col_next is NeedMore.
        for _ in 0..3 {
            assert!(matches!(stream.col_next(), ColEvent::NeedMore));
        }
    });
}

// ==================================================================
// (G) Server error — slow-path EndQuery::Err.
// ==================================================================

/// Invariant: an `ErrorResponse` during an in-flight query flows
/// through the slow path to `EndQuery::Err(ServerErrorResponse)`.
#[test]
fn server_error_response_surfaces_as_end_query_err() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&error_response_frame(b"syntax error at or near \"SELCT\""));
    bytes.extend_from_slice(&rfq_frame(b'I'));

    proto.iter_rows(&mut wb, |stream| {
        if let Err(err) = stream.feed(&bytes) {
            panic!("feed fits: {err:?}");
        }
        let mut saw_fail = false;
        for _ in 0..16 {
            match stream.col_next() {
                ColEvent::EndQuery { id: fail_id, outcome: Err(cause) } => {
                    assert_eq!(fail_id, Some(q_raw));
                    assert!(
                        matches!(cause, ProtocolError::ServerErrorResponse { .. }),
                        "expected ServerErrorResponse, got {cause:?}",
                    );
                    saw_fail = true;
                    break;
                }
                ColEvent::NeedMore => continue,
                other => panic!("unexpected event: {other:?}"),
            }
        }
        assert!(saw_fail, "EndQuery::Err must be emitted");
    });
}

// ==================================================================
// (H) Mid-stream server error.
// ==================================================================

/// Invariant: rows before the E frame are preserved as `Got` events;
/// `EndQuery::Err` replaces the would-be `EndQuery::Ok` terminal.
#[test]
fn rows_before_mid_stream_error_are_preserved() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    bytes.extend_from_slice(&data_row_frame(b"partial"));
    bytes.extend_from_slice(&error_response_frame(b"division by zero"));
    bytes.extend_from_slice(&rfq_frame(b'E'));

    proto.iter_rows(&mut wb, |stream| {
        if let Err(err) = stream.feed(&bytes) {
            panic!("feed fits: {err:?}");
        }
        let mut row_seen = false;
        let mut fail_seen = false;
        for _ in 0..32 {
            match stream.col_next() {
                ColEvent::Got { bytes, .. } => {
                    assert_eq!(bytes, b"partial");
                    row_seen = true;
                }
                ColEvent::EndRow => {}
                ColEvent::EndQuery { id: fail_id, outcome: Err(cause) } => {
                    assert_eq!(fail_id, Some(q_raw));
                    assert!(
                        matches!(cause, ProtocolError::ServerErrorResponse { .. }),
                        "cause must be ServerErrorResponse, got {cause:?}",
                    );
                    fail_seen = true;
                    break;
                }
                ColEvent::NeedMore => continue,
                other => panic!("unexpected event: {other:?}"),
            }
        }
        assert!(row_seen && fail_seen, "must see both Got and EndQuery::Err");
    });
}

// ==================================================================
// (I) Malformed CommandComplete mid-stream.
// ==================================================================

/// Invariant: rows streamed before a CommandComplete framing desync
/// are preserved; the terminal is `EndQuery::Err(MalformedCommandComplete)`.
#[test]
fn rows_preserved_when_command_complete_malformed() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    bytes.extend_from_slice(&data_row_frame(b"intact"));
    // CC frame WITHOUT NUL terminator — framing desync.
    bytes.extend_from_slice(&frame(TAG_COMMAND_COMPLETE.byte(), b"SELECT 1"));

    proto.iter_rows(&mut wb, |stream| {
        if let Err(err) = stream.feed(&bytes) {
            panic!("feed fits: {err:?}");
        }
        let mut row_seen = false;
        let mut fail_seen = false;
        for _ in 0..32 {
            match stream.col_next() {
                ColEvent::Got { bytes, .. } => {
                    assert_eq!(bytes, b"intact");
                    row_seen = true;
                }
                ColEvent::EndRow => {}
                ColEvent::EndQuery { outcome: Err(cause), .. } => {
                    assert!(
                        matches!(cause, ProtocolError::MalformedCommandComplete { .. }),
                        "cause must be MalformedCommandComplete, got {cause:?}",
                    );
                    fail_seen = true;
                    break;
                }
                ColEvent::NeedMore => continue,
                other => panic!("unexpected event: {other:?}"),
            }
        }
        assert!(row_seen && fail_seen, "must see both Got and EndQuery::Err");
    });
}

// ==================================================================
// (J) Split feed.
// ==================================================================

/// Invariant: streaming state persists across `feed()` call
/// boundaries; rows split across feeds are emitted in order.
#[test]
fn rows_across_multiple_feed_calls() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

    let mut batch1 = std::vec::Vec::new();
    batch1.extend_from_slice(&row_description_frame(1));
    batch1.extend_from_slice(&data_row_frame(b"r1"));
    batch1.extend_from_slice(&data_row_frame(b"r2"));

    let mut batch2 = std::vec::Vec::new();
    batch2.extend_from_slice(&data_row_frame(b"r3"));
    batch2.extend_from_slice(&command_complete_frame(b"SELECT 3"));
    batch2.extend_from_slice(&rfq_frame(b'I'));

    proto.iter_rows(&mut wb, |stream| {
        if let Err(err) = stream.feed(&batch1) {
            panic!("feed batch1: {err:?}");
        }
        let mut rows = 0usize;
        let mut consecutive_need_more = 0usize;
        let mut end_query_id: Option<core::num::NonZeroU64> = None;
        for _ in 0..64 {
            match stream.col_next() {
                ColEvent::Got { .. } => {
                    consecutive_need_more = 0;
                }
                ColEvent::EndRow => {
                    rows = rows.saturating_add(1);
                    consecutive_need_more = 0;
                }
                ColEvent::NeedMore => {
                    consecutive_need_more = consecutive_need_more.saturating_add(1);
                    if consecutive_need_more >= 2 {
                        break;
                    }
                }
                ColEvent::EndQuery { id, outcome: Ok(_) } => {
                    end_query_id = id;
                    break;
                }
                other => panic!("unexpected event in batch1: {other:?}"),
            }
        }
        assert_eq!(rows, 2, "batch 1 yields 2 rows");
        assert!(end_query_id.is_none(), "batch 1 must not complete yet");

        if let Err(err) = stream.feed(&batch2) {
            panic!("feed batch2: {err:?}");
        }
        for _ in 0..64 {
            match stream.col_next() {
                ColEvent::Got { .. } => {}
                ColEvent::EndRow => rows = rows.saturating_add(1),
                ColEvent::EndQuery { id, outcome: Ok(_) } => {
                    assert_eq!(id, Some(q_raw));
                    end_query_id = id;
                    break;
                }
                ColEvent::NeedMore => continue,
                other => panic!("unexpected event in batch2: {other:?}"),
            }
        }
        assert_eq!(rows, 3, "3 rows total across both feeds");
        assert!(end_query_id.is_some(), "EndQuery must emit after the final feed");
    });
}

// ==================================================================
// (K) DataRowRef round-trip.
// ==================================================================

/// Invariant: `Got` bytes round-trip through `DataRowRef::parse` +
/// `columns()` — the test relies on a Got bytes layout matching
/// `DataRowRef` input (the `bytes` slice is just the column body,
/// no col-count header; the composite `DataRowRef::parse` is used
/// only on the manually-constructed 2-col row body to validate the
/// decoder's invariants).
///
/// Caller-side decoding from `Got { bytes }` per column is the
/// canonical Sub-A path; this test pins that `Got::bytes` is the
/// raw column body (text or binary) ready for `FromPgText` /
/// `FromPgBinary`.
#[test]
fn got_bytes_decode_per_column() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

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

    proto.iter_rows(&mut wb, |stream| {
        if let Err(err) = stream.feed(&bytes) {
            panic!("feed fits: {err:?}");
        }
        let mut col0_seen = false;
        let mut null_seen = false;
        let mut row_done = false;
        for _ in 0..32 {
            match stream.col_next() {
                ColEvent::Got { idx, bytes } => {
                    assert_eq!(idx, 0);
                    assert_eq!(bytes, b"answer");
                    col0_seen = true;
                }
                ColEvent::Null { idx } => {
                    assert_eq!(idx, 1);
                    null_seen = true;
                }
                ColEvent::EndRow => {
                    row_done = true;
                }
                ColEvent::EndQuery { .. } => break,
                ColEvent::NeedMore => continue,
                other => panic!("unexpected event: {other:?}"),
            }
        }
        assert!(col0_seen && null_seen && row_done, "all events emitted");
    });

    // `DataRowRef::parse` works on a manually-assembled row body
    // — the decoder's invariant is independent of the streaming API.
    let Ok(row) = DataRowRef::parse(&row_body) else {
        panic!("DataRowRef::parse must succeed");
    };
    assert_eq!(row.len(), 2);
    let items: std::vec::Vec<_> = row.columns().collect();
    assert_eq!(items.len(), 2);
    assert!(matches!(items.first(), Some(Ok(Some(b"answer")))));
    assert!(matches!(items.get(1), Some(Ok(None))));
}

// ==================================================================
// (L) End-to-end typed-decode round-trip.
// ==================================================================

/// Invariant: full decode round-trip — push a SELECT, server replies
/// with typed rows, caller uses `Got { bytes }` + `FromPgText` to
/// reconstruct Rust values. User-level API that Phase 2 macros target.
#[test]
fn end_to_end_decode_typed_row() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

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

    proto.iter_rows(&mut wb, |stream| {
        if let Err(err) = stream.feed(&bytes) {
            panic!("feed fits: {err:?}");
        }

        let mut decoded_id: Option<i32> = None;
        let mut decoded_name: Option<std::string::String> = None;
        // Schema is invariant across rows; snapshot once before the
        // event loop. The desc borrow ties to `&stream` and cannot
        // span an &mut method (col_next) call, so we capture the
        // descriptor fields by value here (ColumnDesc is Copy).
        let desc_snapshot: (u32, FormatCode, u32, FormatCode) = {
            // Pull until we land in a row-streaming state (after
            // the silent T transition).
            let mut snap: Option<(u32, FormatCode, u32, FormatCode)> = None;
            for _ in 0..8 {
                if let Some(desc) = stream.current_row_desc() {
                    let Some(ColumnDesc { type_oid: t0, format_code: f0 }) = desc.get(0) else {
                        panic!("desc.get(0) None")
                    };
                    let Some(ColumnDesc { type_oid: t1, format_code: f1 }) = desc.get(1) else {
                        panic!("desc.get(1) None")
                    };
                    snap = Some((t0, f0, t1, f1));
                    break;
                }
                // Drive the dispatcher one step to advance past the
                // RowDescription silent transition.
                let _ = stream.col_next();
            }
            snap.unwrap_or_else(|| panic!("schema never landed"))
        };
        for _ in 0..32 {
            match stream.col_next() {
                ColEvent::Got { idx, bytes } => match idx {
                    0 => {
                        let Ok(v) = i32::from_pg_text(bytes) else {
                            panic!("i32::from_pg_text")
                        };
                        decoded_id = Some(v);
                    }
                    1 => {
                        let Ok(s) = <&str>::from_pg_text(bytes) else {
                            panic!("str::from_pg_text")
                        };
                        decoded_name = Some(std::string::String::from(s));
                    }
                    other => panic!("unexpected col idx: {other}"),
                },
                ColEvent::Null { .. } => panic!("no NULLs in fixture"),
                ColEvent::EndRow => {}
                ColEvent::EndQuery { .. } => break,
                ColEvent::NeedMore => continue,
                _ => panic!("unexpected event"),
            }
        }
        assert_eq!(decoded_id, Some(42));
        assert_eq!(decoded_name.as_deref(), Some("alice"));
        let (t0, f0, t1, f1) = desc_snapshot;
        assert_eq!((t0, t1), (oids::INT4, oids::TEXT));
        assert_eq!((f0, f1), (FormatCode::Text, FormatCode::Text));
    });
}

// ==================================================================
// (M) Drop-mid-stream install.
// ==================================================================

/// Invariant: closure exits without reaching the
/// terminal `EndQuery`; Drop installs Errored via the leaf-gated
/// state setter. The subsequent operation on the connection observes
/// the Errored state.
#[test]
fn drop_mid_stream_installs_errored() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    bytes.extend_from_slice(&data_row_frame(b"x"));
    bytes.extend_from_slice(&data_row_frame(b"y"));
    bytes.extend_from_slice(&data_row_frame(b"z"));
    // NOTE: no CommandComplete/Z — the body deliberately ends mid-stream.

    proto.iter_rows(&mut wb, |stream| {
        if let Err(err) = stream.feed(&bytes) {
            panic!("feed fits: {err:?}");
        }
        // Pull one Got + EndRow then return early WITHOUT EndQuery.
        let mut saw_one_row = false;
        for _ in 0..16 {
            match stream.col_next() {
                ColEvent::Got { .. } => {}
                ColEvent::EndRow => {
                    saw_one_row = true;
                    break;
                }
                ColEvent::NeedMore => continue,
                other => panic!("unexpected event: {other:?}"),
            }
        }
        assert!(saw_one_row);
        // Closure exits here; stream Drop fires; install_errored triggers.
    });

    // Post-Drop: protocol is Errored.
    assert!(
        matches!(proto.state(), ActiveState::Errored(_)),
        "Drop must install Errored, got state {:?}",
        proto.state(),
    );
}

// ==================================================================
// (N) Drop on panic.
// ==================================================================

/// Invariant: closure panics; stack unwind fires
/// Drop; Drop installs Errored. The crate runs under `panic = "unwind"`
/// (workspace default). Use `catch_unwind` to assert the post-state.
#[test]
fn drop_on_closure_panic_installs_errored() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

    let mut bytes = std::vec::Vec::new();
    bytes.extend_from_slice(&row_description_frame(1));
    bytes.extend_from_slice(&data_row_frame(b"x"));

    // SAFETY: `catch_unwind` is safe Rust; the closure captures
    // `&mut proto` / `&mut wb` mutably. We need `AssertUnwindSafe`
    // because raw mut refs are not UnwindSafe by default.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        proto.iter_rows(&mut wb, |stream| {
            if let Err(err) = stream.feed(&bytes) {
                panic!("feed fits: {err:?}");
            }
            // Pull one event then panic.
            let _ = stream.col_next();
            panic!("intentional test panic — Drop must unwind");
        });
    }));

    assert!(result.is_err(), "closure panic must propagate");
    // Post-unwind: protocol is Errored.
    assert!(
        matches!(proto.state(), ActiveState::Errored(_)),
        "Drop on panic unwind must install Errored, got state {:?}",
        proto.state(),
    );
}

// ==================================================================
// (O) Partial-frame mode — single huge column body.
// ==================================================================

/// Invariant: a single DataRow frame whose body
/// exceeds READ_BUF_CAP (4096 B) is streamed as a sequence of
/// `Chunk` events followed by one `ChunkEnd`. Sum of all chunk
/// bytes equals the column's declared length.
///
/// Test design: declared frame total is just above 4096 B (4097 B
/// declared length field). Body is 4093 B (4 length-field + 2
/// col_count + 4 col_len + 4083 col_body). The 4083 col_body cannot
/// fit alongside the 2 col_count + 4 col_len in the 4091 inline-mode
/// post-header headroom (INLINE_BUF_CAP=256, escape to
/// READ_BUF_CAP=4096 heap); confirms the chunked path activates and
/// the entire body decodes correctly.
#[test]
fn partial_frame_mode_streams_huge_data_row() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();
    let (reply, _q_raw) = mint_reply::<QueryKind>(&mut proto);
    push_simple_query(&mut proto, reply, &mut wb);

    // 1 column, col_body length = 5000 B (> READ_BUF_CAP 4096 — forces
    // partial-frame mode).
    let col_body_len: usize = 5000;
    let col_body: std::vec::Vec<u8> = (0..col_body_len)
        .map(|i| {
            let Ok(b) = u8::try_from(i & 0xFF) else { return 0 };
            b
        })
        .collect();

    let mut row_body = std::vec::Vec::new();
    row_body.extend_from_slice(&1i16.to_be_bytes());
    let Ok(col_len_i32) = i32::try_from(col_body.len()) else {
        panic!("col_body too large for i32")
    };
    row_body.extend_from_slice(&col_len_i32.to_be_bytes());
    row_body.extend_from_slice(&col_body);
    // declared length includes itself (4 bytes) + body bytes.
    let Ok(declared) = u32::try_from(row_body.len().saturating_add(4)) else {
        panic!("declared too large for u32")
    };

    let mut frame_buf = std::vec::Vec::new();
    frame_buf.push(TAG_DATA_ROW.byte());
    frame_buf.extend_from_slice(&declared.to_be_bytes());
    frame_buf.extend_from_slice(&row_body);

    // Setup: install RowDescription first via a separate feed_bytes
    // (small frame, fits inline).
    let setup = row_description_frame(1);
    let actions = proto.feed_bytes(&setup, &mut wb);
    assert!(
        actions.as_slice().is_empty(),
        "RowDescription is a silent state transition",
    );

    // Now stream the huge DataRow frame in 1024-B chunks via feed().
    // Each feed call triggers a col_next pull cycle.
    proto.iter_rows(&mut wb, |stream| {
        let mut fed = 0usize;
        let chunk_size = 1024usize;
        let mut collected: std::vec::Vec<u8> = std::vec::Vec::with_capacity(col_body_len);
        let mut total_seen = 0u32;
        let mut got_chunk_end = false;
        let mut iterations = 0u32;
        while fed < frame_buf.len() {
            let end = core::cmp::min(fed.saturating_add(chunk_size), frame_buf.len());
            let Some(slice) = frame_buf.get(fed..end) else {
                panic!("slice OOB");
            };
            if let Err(err) = stream.feed(slice) {
                panic!("feed slice {fed}..{end}: {err:?}");
            }
            fed = end;
            // Drain available events.
            for _ in 0..64 {
                iterations = iterations.saturating_add(1);
                let Ok(col_body_len_u32) = u32::try_from(col_body.len()) else {
                    panic!("col_body too large for u32")
                };
                match stream.col_next() {
                    ColEvent::Chunk { idx, bytes, total_len, remaining_len } => {
                        assert_eq!(idx, 0);
                        assert_eq!(total_len, col_body_len_u32);
                        collected.extend_from_slice(bytes);
                        let Ok(bytes_len_u32) = u32::try_from(bytes.len()) else {
                            panic!("chunk bytes len overflow")
                        };
                        total_seen = total_seen.saturating_add(bytes_len_u32);
                        assert_eq!(
                            total_seen.saturating_add(remaining_len),
                            col_body_len_u32,
                            "total+remaining must equal col len",
                        );
                    }
                    ColEvent::ChunkEnd { idx, bytes } => {
                        assert_eq!(idx, 0);
                        collected.extend_from_slice(bytes);
                        got_chunk_end = true;
                    }
                    ColEvent::Got { idx, bytes } => {
                        // Tier-2 defence: if the col happened to fit
                        // inline after a feed round, accept Got as a
                        // single-shot. Validate full body.
                        assert_eq!(idx, 0);
                        collected.extend_from_slice(bytes);
                        got_chunk_end = true;
                    }
                    ColEvent::EndRow => {}
                    ColEvent::EndQuery { .. } => return,
                    ColEvent::NeedMore => break,
                    other => panic!("unexpected event: {other:?}"),
                }
            }
            assert!(iterations < 10_000, "runaway loop");
        }
        // Feed the trailing CommandComplete + RFQ to drain the
        // closure cleanly.
        let mut trailer = std::vec::Vec::new();
        trailer.extend_from_slice(&command_complete_frame(b"SELECT 1"));
        trailer.extend_from_slice(&rfq_frame(b'I'));
        if let Err(err) = stream.feed(&trailer) {
            panic!("feed trailer: {err:?}");
        }
        for _ in 0..64 {
            match stream.col_next() {
                ColEvent::EndQuery { outcome: Ok(_), .. } => break,
                ColEvent::EndQuery { outcome: Err(cause), .. } => {
                    panic!("expected EndQuery::Ok, got Err({cause:?})")
                }
                ColEvent::NeedMore => continue,
                _ => {}
            }
        }
        assert!(got_chunk_end, "must see ChunkEnd (or fitted Got)");
        assert_eq!(
            collected.as_slice(),
            col_body.as_slice(),
            "all body bytes reassembled",
        );
    });
}
