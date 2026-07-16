//! `query!` end-to-end: typed-record twins decoded from canned `DataRow`
//! payload byte fixtures.
//!
//! Each `query!` below is typed AT COMPILE TIME against the catalog that
//! `build.rs` -> `bsql-build` replays from `migrations/`. The macro emits
//! a borrowed record (`Name`, with `&'q str` text cells) and an owned twin
//! (`NameOwned`, with `String`), plus a `decode` fn over a raw `DataRow`
//! payload — the wire bytes AFTER the 5-byte frame header, beginning with
//! the 2-byte column-count.
//!
//! The fixtures here are hand-built `DataRow` bodies (no live server), so
//! the whole test is structurally offline. They exercise:
//!   * the vectorized all-fixed-width path (`OrderKey`),
//!   * the per-cell NULL / text path (`OrderRow`, `UserNames`),
//!   * `NULL -> Option` on a nullable column,
//!   * a classified `NullInNonNullColumn` when a NULL lands in a NOT-NULL
//!     column, on BOTH the borrowed and owned decode paths, and on both
//!     the per-cell path and the all-fixed path's fallback.

use bsql_postgres_proto::DecodeError;

// All-fixed-width, all-NOT-NULL row: both columns are `int8` (the PK and
// a NOT NULL FK), so the borrowed record carries no lifetime and the
// decode emits the vectorized fast path.
bsql::query!(OrderKey, "SELECT id, user_id FROM orders");

// Mixed: `id` is NOT NULL `int8`, `total` is nullable `int4`, `status` is
// nullable `text`. The text column makes the borrowed record carry `<'q>`
// and forces the per-cell decode path.
bsql::query!(OrderRow, "SELECT id, total, status FROM orders");

// NOT-NULL text: `id` `int8`, `email` NOT NULL `text`. The borrowed
// `email` aliases the input bytes (zero-copy).
bsql::query!(UserNames, "SELECT id, email FROM users");

// ── canned DataRow payloads ────────────────────────────────────────────

/// `OrderKey` row: id = 42, user_id = 7. All fixed-width, no NULL.
const ORDER_KEY_ROW: &[u8] = &[
    0x00, 0x02, // 2 columns
    0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 42, // col0 i64 = 42
    0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 7, // col1 i64 = 7
];

/// `OrderKey` row with the NOT-NULL `id` sent as SQL NULL (len = -1). The
/// NULL shortens the body below the fast path's exact-length expectation,
/// so it falls through to the per-cell path, which classifies it.
const ORDER_KEY_ID_NULL: &[u8] = &[
    0x00, 0x02, // 2 columns
    0xFF, 0xFF, 0xFF, 0xFF, // col0 (id) = NULL
    0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 7, // col1 i64 = 7
];

/// `OrderRow` row: id = 100, total = NULL, status = "open".
const ORDER_ROW_TOTAL_NULL: &[u8] = &[
    0x00, 0x03, // 3 columns
    0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 100, // id = 100
    0xFF, 0xFF, 0xFF, 0xFF, // total = NULL
    0x00, 0x00, 0x00, 0x04, b'o', b'p', b'e', b'n', // status = "open"
];

/// `OrderRow` row: id = 100, total = 55, status = "paid".
const ORDER_ROW_FULL: &[u8] = &[
    0x00, 0x03, // 3 columns
    0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 100, // id = 100
    0x00, 0x00, 0x00, 0x04, 0, 0, 0, 55, // total = 55
    0x00, 0x00, 0x00, 0x04, b'p', b'a', b'i', b'd', // status = "paid"
];

/// `OrderRow` row with the NOT-NULL `id` sent as SQL NULL — the per-cell
/// path must classify this as `NullInNonNullColumn`.
const ORDER_ROW_ID_NULL: &[u8] = &[
    0x00, 0x03, // 3 columns
    0xFF, 0xFF, 0xFF, 0xFF, // id = NULL (NOT NULL column!)
    0x00, 0x00, 0x00, 0x04, 0, 0, 0, 55, // total = 55
    0x00, 0x00, 0x00, 0x04, b'p', b'a', b'i', b'd', // status = "paid"
];

/// `UserNames` row: id = 1, email = "a@b.co".
const USER_NAMES_ROW: &[u8] = &[
    0x00, 0x02, // 2 columns
    0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 1, // id = 1
    0x00, 0x00, 0x00, 0x06, b'a', b'@', b'b', b'.', b'c', b'o', // email
];

// ── tests ──────────────────────────────────────────────────────────────

#[test]
fn all_fixed_fast_path_borrowed_and_owned() {
    // Borrowed decode (the vectorized fast path) yields the typed record.
    let borrowed = OrderKey::decode(ORDER_KEY_ROW).expect("fast-path decode");
    assert_eq!(borrowed, OrderKey { id: 42, user_id: 7 });
    // Owned twin decodes identically (no text -> structurally identical).
    let owned = OrderKeyOwned::decode(ORDER_KEY_ROW).expect("owned fast-path decode");
    assert_eq!(owned, OrderKeyOwned { id: 42, user_id: 7 });
}

#[test]
fn nullable_column_becomes_option() {
    // A NULL in the nullable `total` column decodes to `None`; the
    // present `status` decodes to `Some`.
    let borrowed = OrderRow::decode(ORDER_ROW_TOTAL_NULL).expect("decode");
    assert_eq!(borrowed.id, 100);
    assert_eq!(borrowed.total, None);
    assert_eq!(borrowed.status, Some("open"));

    let owned = OrderRowOwned::decode(ORDER_ROW_TOTAL_NULL).expect("decode owned");
    assert_eq!(owned.id, 100);
    assert_eq!(owned.total, None);
    assert_eq!(owned.status, Some("open".to_string()));
}

#[test]
fn present_nullable_values_decode() {
    let borrowed = OrderRow::decode(ORDER_ROW_FULL).expect("decode");
    assert_eq!(borrowed.total, Some(55));
    assert_eq!(borrowed.status, Some("paid"));

    let owned = OrderRowOwned::decode(ORDER_ROW_FULL).expect("decode owned");
    assert_eq!(owned.total, Some(55));
    assert_eq!(owned.status, Some("paid".to_string()));
}

#[test]
fn null_in_not_null_column_is_tier3_per_cell_path() {
    // A NULL in the NOT-NULL `id` column on the per-cell path is a
    // classified error on BOTH the borrowed and owned decoders — never a
    // silent default or panic.
    let borrowed = OrderRow::decode(ORDER_ROW_ID_NULL);
    assert!(matches!(borrowed, Err(DecodeError::NullInNonNullColumn)));

    let owned = OrderRowOwned::decode(ORDER_ROW_ID_NULL);
    assert!(matches!(owned, Err(DecodeError::NullInNonNullColumn)));
}

#[test]
fn null_in_not_null_column_is_tier3_fast_path_fallback() {
    // The same classification holds when the NULL arrives in an
    // all-fixed-width query: the NULL shortens the row, the fast path
    // defers to the per-cell path, and the per-cell path classifies it.
    let borrowed = OrderKey::decode(ORDER_KEY_ID_NULL);
    assert!(matches!(borrowed, Err(DecodeError::NullInNonNullColumn)));

    let owned = OrderKeyOwned::decode(ORDER_KEY_ID_NULL);
    assert!(matches!(owned, Err(DecodeError::NullInNonNullColumn)));
}

#[test]
fn not_null_text_borrows_zero_copy() {
    let borrowed = UserNames::decode(USER_NAMES_ROW).expect("decode");
    assert_eq!(borrowed.id, 1);
    assert_eq!(borrowed.email, "a@b.co");

    let owned = UserNamesOwned::decode(USER_NAMES_ROW).expect("decode owned");
    assert_eq!(owned.email, "a@b.co".to_string());
}

#[test]
fn malformed_count_header_fails_closed() {
    // A body too short to hold the 2-byte count header is a classified
    // error, not a panic.
    let truncated: &[u8] = &[0x00];
    assert!(matches!(
        OrderKey::decode(truncated),
        Err(DecodeError::TruncatedRow)
    ));
}

// ── footprint pins (64-bit) ─────────────────────────────────────────────
//
// The emitted records pin their layout at build time. Pinned only on
// 64-bit targets, where `&str` (16 B) and `String` (24 B) sizes are
// stable; a 32-bit target's smaller pointers are not asserted.
#[cfg(target_pointer_width = "64")]
const _: () = {
    use core::mem::size_of;
    // All-fixed twins: two `i64` = 16 B.
    assert!(size_of::<OrderKey>() == 16);
    assert!(size_of::<OrderKeyOwned>() == 16);
    // Borrowed mixed: i64(8) + Option<i32>(8) + Option<&str>(16) = 32 B.
    assert!(size_of::<OrderRow<'static>>() == 32);
    // Owned mixed: i64(8) + Option<i32>(8) + Option<String>(24) = 40 B.
    assert!(size_of::<OrderRowOwned>() == 40);
    // Borrowed text: i64(8) + &str(16) = 24 B.
    assert!(size_of::<UserNames<'static>>() == 24);
    // Owned text: i64(8) + String(24) = 32 B.
    assert!(size_of::<UserNamesOwned>() == 32);
};
