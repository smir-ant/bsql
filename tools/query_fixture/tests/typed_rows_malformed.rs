//! OFFLINE proof of the tier-1 no-swallow Err-item path.
//!
//! A correctly-validated `query!` cannot honestly produce a live decode error
//! (the build-checked record type matches what PG sends), so the no-swallow
//! guarantee is proven here with hand-built byte fixtures instead: a malformed
//! `DataRow` payload fed through the SAME `RowsBuilder` prebuffer the drivers use
//! yields a per-row `Err(DecodeError)` from `Rows::iter()` — WITHOUT a panic, and
//! WITHOUT poisoning the well-formed rows around it. No live server.
//!
//! This is the decode-side half of the structural no-swallow argument: the
//! collection sink is infallible (it only copies bytes), and decoding — which CAN
//! fail — runs lazily over the owned prebuffer, so a malformed row is a value
//! returned to the caller, never a swallowed error and never a panic.
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "offline test harness — panic/expect surface failures loudly; not production fallbacks"
)]

use bsql_postgres_core::RowsBuilder;
use bsql_postgres_proto::engine::Surface;
use bsql_postgres_proto::DecodeError;

// Two `int8 NOT NULL` columns -> the all-fixed-width record `Mal { id, user_id }`
// (both `i64`). `orders.id` / `orders.user_id` exist in the fixture's migrations.
bsql::query!(Mal, "SELECT id, user_id FROM orders");

/// A well-formed 2x`int8` `DataRow` body: `[count=2][len=8][i64=42][len=8][i64=7]`.
const GOOD: &[u8] = &[
    0x00, 0x02, // 2 columns
    0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 42, // col0 i64 = 42
    0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 7, // col1 i64 = 7
];

/// Malformed: col1's length prefix declares 8 bytes but only 2 follow, so the
/// decode classifies a `DecodeError` rather than mis-reading past the body.
const BAD: &[u8] = &[
    0x00, 0x02, // 2 columns
    0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 42, // col0 i64 = 42 (well-formed)
    0, 0, 0, 8, 0, 7, // col1: declares len 8, only 2 bytes present
];

#[test]
fn malformed_row_is_a_per_row_err_item_not_a_panic() {
    // Feed the prebuffer GOOD, BAD, GOOD through the exact sink shape the drivers
    // use (an infallible byte copy).
    let mut builder = RowsBuilder::new();
    builder.feed(Surface::Row(GOOD));
    builder.feed(Surface::Row(BAD));
    builder.feed(Surface::Row(GOOD));
    let rows = builder.finish::<MalQuery>();
    assert_eq!(rows.len(), 3, "three rows buffered");

    // Decoding lazily over the prebuffer: the malformed row is an `Err` ITEM, the
    // well-formed rows around it still decode `Ok`. Collecting (rather than
    // short-circuiting) proves the Err does not stop or poison iteration.
    let items: Vec<Result<Mal, DecodeError>> = rows.iter().collect();
    match items.as_slice() {
        [Ok(first), Err(_), Ok(third)] => {
            assert_eq!(first.id, 42);
            assert_eq!(first.user_id, 7);
            assert_eq!(third.id, 42);
            assert_eq!(third.user_id, 7);
        }
        other => panic!("expected [Ok, Err(DecodeError), Ok], got {other:?}"),
    }
}

#[test]
fn into_owned_surfaces_the_malformed_row_as_err_not_partial() {
    // `into_owned` fails the WHOLE call on the first malformed row rather than
    // returning a silently-truncated partial vector.
    let mut builder = RowsBuilder::new();
    builder.feed(Surface::Row(GOOD));
    builder.feed(Surface::Row(BAD));
    let rows = builder.finish::<MalQuery>();
    assert!(
        rows.into_owned().is_err(),
        "a malformed row makes into_owned fail closed, not return a partial Vec"
    );
}
