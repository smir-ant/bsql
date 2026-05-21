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

use core::num::NonZeroU32;

use bsql_pg_proto::{
    ActiveState, FetchRows, PortalName, QueryKind, WriteBuf,
    decode::RowDesc,
    push_command::ExecutePortal,
    wire::TAG_EXECUTE,
};

mod common;
use common::{PushOrPanic, fresh_active_via_trust_handshake, mint_reply};

fn portal_unnamed() -> PortalName {
    PortalName::default()
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
            row_desc: Some(RowDesc::EMPTY),
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
            row_desc: Some(RowDesc::EMPTY),
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
