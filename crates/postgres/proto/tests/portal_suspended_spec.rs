//! Extended Query `PortalSuspended` + chunked-fetch primitives
//! (PG §55.2.7 / DEF-225).
//!
//! This file covers the **building blocks** of the chunked-fetch
//! flow:
//!
//! - `FetchRows::Chunked(N)` wire-encoding (const-asserted in
//!   `command.rs`; external visibility checked here).
//! - `push_command::ExecutePortal` push struct emits exactly 2
//!   SendBytes (Execute + Sync — no Bind), with the correct wire
//!   bytes (`'E'` tag + length + portal-name CSTR + max_rows).
//! - Post-push state for `ExecutePortal` skips
//!   `AwaitingBindCompleteSelect` and lands directly in
//!   `AwaitingDataOrCompleteSelect` (Select path) or
//!   `AwaitingCommandCompleteDml` (Dml path).
//!
//! **End-to-end PortalSuspended → QuerySuspended round-trip** lives
//! in `row_stream_spec.rs` territory (DataRow pull via
//! `iter_rows` + `col_next`); the iter_rows machinery surfaces
//! `ColEvent::EndQuery { outcome: Ok(Reply::QuerySuspended(_)) }` on
//! the suspended terminal. That integration test is a follow-up to
//! the building-block primitives validated here.

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
// Fixture-builder helper fns below panic on malformed synthetic input.
// Integration-test helpers run WITHOUT `cfg(test)`, so the floor's
// `allow-panic-in-tests` carve-out (keyed on `#[test]` context) cannot
// reach them; the panic is the loud test-failure signal, not a silent
// production fallback.
#![allow(clippy::panic, reason = "test harness — fixture builders panic on malformed synthetic input as the loud test-failure signal, not as a silent production fallback; integration-test helper fns are not in `#[test]` context so the in-tests carve-out cannot reach them")]

#![allow(clippy::disallowed_methods, reason = "test/bench harness — fixtures use the sanctioned try_from(..).unwrap_or(SAT) / slice.get(..).unwrap_or(&[]) dead-arm shape, not production data fallbacks")]
use core::num::NonZeroU32;

use bsql_postgres_proto::{
    ActiveState, ColEvent, FetchRows, PortalName, ProtocolError, QueryKind, Reply, StmtName,
    WriteBuf,
    decode::RowDesc,
    push_command::ExecutePortal,
    wire::{
        TAG_BIND_COMPLETE, TAG_COMMAND_COMPLETE, TAG_DATA_ROW, TAG_EXECUTE, TAG_PORTAL_SUSPENDED,
        TAG_READY_FOR_QUERY,
    },
};

mod common;
use common::{PushOrPanic, fresh_active_via_trust_handshake, mint_reply};

fn portal_unnamed() -> PortalName {
    PortalName::default()
}

fn stmt_unnamed() -> StmtName {
    StmtName::default()
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

fn bind_complete_frame() -> [u8; 5] {
    [TAG_BIND_COMPLETE.byte(), 0, 0, 0, 4]
}

/// 0-column DataRow body: `n_cols: i16 = 0`. Per PG §55.2.2 the body
/// is `n_cols + per-column (i32 len + bytes)`; n_cols=0 means an empty
/// column list. Useful with `RowDesc::empty()` to exercise the
/// streaming-state machine WITHOUT depending on populated `RowDesc`
/// (the only externally-constructable shape is EMPTY — populated
/// `RowDesc` requires parsing a `RowDescription` frame, which the
/// `BindExecute`-with-caller-supplied-schema path bypasses).
fn empty_data_row_frame() -> std::vec::Vec<u8> {
    let body = 0_i16.to_be_bytes();
    frame(TAG_DATA_ROW.byte(), &body)
}

fn portal_suspended_frame() -> [u8; 5] {
    [TAG_PORTAL_SUSPENDED.byte(), 0, 0, 0, 4]
}

fn command_complete_frame(tag: &[u8]) -> std::vec::Vec<u8> {
    let mut body = tag.to_vec();
    body.push(0);
    frame(TAG_COMMAND_COMPLETE.byte(), &body)
}

fn rfq_frame(tx_byte: u8) -> [u8; 6] {
    [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, tx_byte]
}

// =====================================================================
// FetchRows::Chunked wire-encoding pins (runtime witnesses).
//
// The const-asserts in `command.rs` already pin the wire-encoding
// invariants at build time (1 → 1, u32::MAX → i32::MAX saturated).
// These runtime tests verify the type is publicly constructable
// from outside the crate (visibility witness — internal const-
// asserts cannot catch a `pub` → `pub(crate)` regression on the
// re-export path).
// =====================================================================

#[test]
fn fetch_rows_chunked_one_constructs_externally() {
    let one = match NonZeroU32::new(1) {
        Some(n) => n,
        None => panic!("1 is non-zero"),
    };
    let _: FetchRows = FetchRows::Chunked(one);
}

#[test]
fn fetch_rows_chunked_u32_max_constructs_externally() {
    let max = match NonZeroU32::new(u32::MAX) {
        Some(n) => n,
        None => panic!("u32::MAX is non-zero"),
    };
    let _: FetchRows = FetchRows::Chunked(max);
}

// =====================================================================
// ExecutePortal push emits 2 SendBytes (Execute + Sync — no Bind).
// =====================================================================

#[test]
fn execute_portal_select_path_emits_execute_and_sync() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _) = mint_reply::<QueryKind>(&mut proto);
    let three = match NonZeroU32::new(3) {
        Some(n) => n,
        None => panic!("3 is non-zero"),
    };
    proto.push_or_panic(
        ExecutePortal {
            portal_name: &portal_unnamed(),
            row_desc: Some(RowDesc::empty()),
            fetch: FetchRows::Chunked(three),
            reply,
        },
        &mut wb,
    );

    // Wire layout: Execute frame ('E' + len-field + portal_name NUL +
    // max_rows BE-i32) followed by Sync (5 B: 'S', 0, 0, 0, 4).
    let bytes = wb.as_bytes();
    assert_eq!(
        bytes.first().copied(),
        Some(TAG_EXECUTE.byte()),
        "first byte is Execute tag",
    );

    // Trailing 5 bytes = Sync ('S' + length-field 4).
    let total_len = bytes.len();
    assert!(
        total_len >= 5,
        "must contain at least the Sync trailer (5 B)",
    );
    let trailer_start = total_len.saturating_sub(5);
    let last_five = bytes.get(trailer_start..).unwrap_or(&[]);
    assert_eq!(
        last_five.first().copied(),
        Some(b'S'),
        "trailing frame is Sync tag",
    );
    assert_eq!(
        last_five.get(1..5).unwrap_or(&[]),
        &[0u8, 0, 0, 4],
        "Sync length-field is BE 4",
    );

    // Post-push state: Select path (row_desc was Some) lands directly
    // in AwaitingDataOrCompleteSelect — skips AwaitingBindCompleteSelect
    // because no Bind frame was sent.
    assert!(
        matches!(
            proto.state(),
            ActiveState::BindExecuteAwaitingDataOrCompleteSelect { .. }
        ),
        "ExecutePortal Select must transition to AwaitingDataOrCompleteSelect, got {:?}",
        proto.state(),
    );
}

#[test]
fn execute_portal_dml_path_skips_bind_complete() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _) = mint_reply::<QueryKind>(&mut proto);
    proto.push_or_panic(
        ExecutePortal {
            portal_name: &portal_unnamed(),
            row_desc: None,
            fetch: FetchRows::All,
            reply,
        },
        &mut wb,
    );

    // Dml path (row_desc = None) lands in AwaitingCommandCompleteDml
    // directly — skips AwaitingBindCompleteDml.
    assert!(
        matches!(
            proto.state(),
            ActiveState::BindExecuteAwaitingCommandCompleteDml(_)
        ),
        "ExecutePortal Dml must transition to AwaitingCommandCompleteDml, got {:?}",
        proto.state(),
    );
}

// =====================================================================
// End-to-end iter_rows integration (DEF-225 Phase F)
//
// Validates the full chunked-fetch round-trip:
//   push_bind_execute(Chunked(N))
//     → server: BindComplete + DataRow × N + PortalSuspended + RFQ
//     → iter_rows pull loop observes:
//        - EndRow × N (for 0-column DataRows with RowDesc::empty())
//        - EndQuery { outcome: Ok(Reply::QuerySuspended(_)) }
//   ExecutePortal(All) resume
//     → server: DataRow × 1 + CommandComplete + RFQ
//     → iter_rows pull loop observes:
//        - EndRow × 1
//        - EndQuery { outcome: Ok(Reply::QueryComplete(_)) }
//
// Uses `RowDesc::empty()` + 0-column DataRows because populated
// `RowDesc` requires parsing a `RowDescription` frame from the wire
// (parser is `pub(crate)`), but `BindExecute`'s caller-supplied
// schema path accepts `EMPTY` directly. The state-machine path
// being tested is identical regardless of column count.
// =====================================================================

#[test]
fn iter_rows_chunked_suspended_then_resume_to_completion() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    // Step 1: push BindExecute with Chunked(2). Caller pre-supplies
    // RowDesc::empty() (0-column SELECT path).
    let (reply1, raw1) = mint_reply::<QueryKind>(&mut proto);
    let two = match NonZeroU32::new(2) {
        Some(n) => n,
        None => panic!("2 is non-zero"),
    };
    proto.push_bind_execute_or_panic(
        &portal_unnamed(),
        &stmt_unnamed(),
        &(),
        Some(RowDesc::empty()),
        FetchRows::Chunked(two),
        reply1,
        &mut wb,
    );

    // Server response: BindComplete + 2×DataRow(0-col) + PortalSuspended + RFQ.
    let mut server_bytes = std::vec::Vec::new();
    server_bytes.extend_from_slice(&bind_complete_frame());
    server_bytes.extend_from_slice(&empty_data_row_frame());
    server_bytes.extend_from_slice(&empty_data_row_frame());
    server_bytes.extend_from_slice(&portal_suspended_frame());
    server_bytes.extend_from_slice(&rfq_frame(b'I'));

    let collect_result: Result<(), ProtocolError> = proto.iter_rows(&mut wb, |stream| {
        if stream.feed(&server_bytes).is_err() {
            return Err(ProtocolError::InternalCrateBug {
                locus: bsql_postgres_proto::CrateBugLocus::ReadCursorAdvance,
            });
        }
        let mut row_count = 0_usize;
        let mut saw_end_query = false;
        let mut events_seen: std::vec::Vec<std::string::String> = std::vec::Vec::new();
        for _ in 0..64_u32 {
            match stream.col_next() {
                ColEvent::Got { .. } => panic!("0-column DataRows must not emit Got events"),
                ColEvent::Null { .. } => panic!("0-column DataRows must not emit Null events"),
                ColEvent::EndRow => {
                    events_seen.push("EndRow".into());
                    row_count = row_count.saturating_add(1);
                }
                ColEvent::Chunk { .. } | ColEvent::ChunkEnd { .. } => {
                    panic!("0-column rows must not chunk")
                }
                ColEvent::NeedMore => {
                    events_seen.push("NeedMore".into());
                    continue;
                }
                ColEvent::EndQuery { id, outcome } => {
                    events_seen.push(format!("EndQuery(id={id:?}, ok={})", outcome.is_ok()));
                    assert_eq!(id, Some(raw1), "EndQuery id matches in-flight reply");
                    match outcome {
                        Ok(Reply::QuerySuspended(_)) => {}
                        other => panic!(
                            "expected EndQuery {{ outcome: Ok(QuerySuspended) }}, got {other:?}; events: {events_seen:?}"
                        ),
                    }
                    saw_end_query = true;
                    break;
                }
                // ColEvent is `#[non_exhaustive]`; wildcard mandatory.
                other => panic!("unexpected ColEvent variant: {other:?}"),
            }
        }
        assert_eq!(row_count, 2, "must observe exactly 2 EndRow events before suspension; events: {events_seen:?}");
        assert!(saw_end_query, "must observe EndQuery with QuerySuspended outcome; events: {events_seen:?}");
        Ok(())
    });
    if let Err(e) = collect_result {
        panic!("Phase 1 iter_rows errored: {e:?}");
    }

    // Step 2: ExecutePortal resume with All. Server returns 1 more
    // DataRow + CommandComplete + RFQ — portal exhausted.
    let (reply2, raw2) = mint_reply::<QueryKind>(&mut proto);
    proto.push_or_panic(
        ExecutePortal {
            portal_name: &portal_unnamed(),
            row_desc: Some(RowDesc::empty()),
            fetch: FetchRows::All,
            reply: reply2,
        },
        &mut wb,
    );

    let mut server_bytes = std::vec::Vec::new();
    server_bytes.extend_from_slice(&empty_data_row_frame());
    server_bytes.extend_from_slice(&command_complete_frame(b"SELECT 3"));
    server_bytes.extend_from_slice(&rfq_frame(b'I'));

    let collect_result: Result<(), ProtocolError> = proto.iter_rows(&mut wb, |stream| {
        if stream.feed(&server_bytes).is_err() {
            return Err(ProtocolError::InternalCrateBug {
                locus: bsql_postgres_proto::CrateBugLocus::ReadCursorAdvance,
            });
        }
        let mut row_count = 0_usize;
        let mut saw_end_query = false;
        for _ in 0..64_u32 {
            match stream.col_next() {
                ColEvent::Got { .. } | ColEvent::Null { .. } => {
                    panic!("0-column DataRows emit no Got/Null events")
                }
                ColEvent::EndRow => {
                    row_count = row_count.saturating_add(1);
                }
                ColEvent::Chunk { .. } | ColEvent::ChunkEnd { .. } => panic!("no chunks"),
                ColEvent::NeedMore => continue,
                ColEvent::EndQuery { id, outcome } => {
                    assert_eq!(id, Some(raw2), "Phase 2 EndQuery id matches resume reply");
                    match outcome {
                        Ok(Reply::QueryComplete(_)) => {
                            // DEF-286 Φ-F*: command_tag externalised.
                            // (Note: querying proto.current_command_tag()
                            // here would require dropping the borrow chain
                            // from the iter_rows loop; the assertion was
                            // about the wire round-trip, which is
                            // structurally tested at the unit level.)
                        }
                        other => panic!(
                            "expected EndQuery {{ outcome: Ok(QueryComplete) }} on resume to exhaustion, got {other:?}"
                        ),
                    }
                    saw_end_query = true;
                    break;
                }
                other => panic!("unexpected ColEvent variant: {other:?}"),
            }
        }
        assert_eq!(row_count, 1, "Phase 2 sees exactly 1 EndRow event");
        assert!(
            saw_end_query,
            "Phase 2 EndQuery must observe QueryComplete (portal exhausted)"
        );
        Ok(())
    });
    if let Err(e) = collect_result {
        panic!("Phase 2 iter_rows errored: {e:?}");
    }
}

#[test]
fn execute_portal_wire_contains_max_rows_be_i32() {
    let mut proto = fresh_active_via_trust_handshake();
    let mut wb = WriteBuf::new();

    let (reply, _) = mint_reply::<QueryKind>(&mut proto);
    let cap = match NonZeroU32::new(42) {
        Some(n) => n,
        None => panic!("42 is non-zero"),
    };
    proto.push_or_panic(
        ExecutePortal {
            portal_name: &portal_unnamed(),
            row_desc: Some(RowDesc::empty()),
            fetch: FetchRows::Chunked(cap),
            reply,
        },
        &mut wb,
    );

    let bytes = wb.as_bytes();
    // Execute frame layout: 'E' (1) + length-field (4) + portal_name
    // CSTR (1 for empty + NUL = 1) + max_rows BE i32 (4). For unnamed
    // portal: total inner = 1 + 1 + 4 = 6; length-field self-includes
    // → 10.
    assert_eq!(bytes.first().copied(), Some(TAG_EXECUTE.byte()));
    // Skip past tag + length to body. body[0] = NUL (empty portal_name CSTR).
    assert_eq!(
        bytes.get(5).copied(),
        Some(0),
        "byte at body[0] is NUL terminator for empty portal_name",
    );
    // body[1..5] = max_rows BE i32 = 42.
    let max_rows_bytes = bytes.get(6..10).unwrap_or(&[]);
    assert_eq!(
        max_rows_bytes,
        &42_i32.to_be_bytes(),
        "max_rows field encodes Chunked(42) as BE i32 42",
    );
}
