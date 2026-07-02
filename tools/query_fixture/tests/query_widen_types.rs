//! `query!` type-widening: the dep-free bsql-native types (`uuid`,
//! `timestamptz`, `timestamp`) as `query!` record fields, decoded from
//! hand-built `DataRow` payloads (no live server).
//!
//! Each `query!` below types AT COMPILE TIME against the `events` table in
//! `migrations/` — the mere fact these compile is the proof that a column of
//! `uuid` / `timestamptz` / `timestamp` is no longer a `compile_error!`. The
//! decode assertions prove the wire bytes materialise into the correct
//! bsql-native values (16 raw bytes -> `Uuid`; an `i64` micro count ->
//! `Timestamptz` / `Timestamp`), and the NULL paths prove nullable columns
//! become `Option<T>` while a NULL in a NOT-NULL column is classified.

use bsql::{Json, Jsonb, Timestamp, Timestamptz, Uuid};
use bsql_postgres_proto::DecodeError;

// NOT-NULL `uuid` PK + NOT-NULL `timestamptz` + nullable `timestamp` +
// nullable `uuid`. Every column is a bsql-native type: `id` and
// `occurred_at` are the all-fixed-width fast path candidates when NOT NULL,
// but the two nullable columns force the per-cell path for the whole row.
bsql::query!(
    Event,
    "SELECT id, occurred_at, recorded_at, prev_id FROM events"
);

// A pure all-fixed-width, all-NOT-NULL row (uuid + timestamptz) — exercises
// the vectorized const-offset fast path with the widened types.
bsql::query!(EventKey, "SELECT id, occurred_at FROM events");

// The literal-cast path (no table) — proves a `::uuid` / `::timestamptz`
// cast types identically to a catalog column.
bsql::query!(
    CastRow,
    "SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid AS u, \
     '2000-01-01 00:00:01+00'::timestamptz AS t"
);

// A `jsonb` NOT NULL column + a nullable `json` column -> `Jsonb` /
// `Option<Json>`. Both are variable-width, so the whole row decodes on the
// per-cell path.
bsql::query!(Doc, "SELECT payload, meta FROM events");

// ── canned DataRow payloads ───────────────────────────────────────────────

const UUID_BYTES: [u8; 16] = [
    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
];
const PREV_UUID_BYTES: [u8; 16] = [
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
];

/// `EventKey` fast-path row: id = UUID_BYTES, occurred_at = 1_000_000 µs
/// (2000-01-01 00:00:01 UTC). All fixed-width, no NULL.
const EVENT_KEY_ROW: &[u8] = &[
    0x00, 0x02, // 2 columns
    0x00, 0x00, 0x00, 0x10, // col0 uuid, len = 16
    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x08, // col1 timestamptz, len = 8
    0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x42, 0x40, // i64 = 1_000_000
];

/// `Event` row: id, occurred_at present; recorded_at = NULL; prev_id present.
const EVENT_ROW_MIXED: &[u8] = &[
    0x00, 0x04, // 4 columns
    0x00, 0x00, 0x00, 0x10, // id uuid
    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x08, // occurred_at timestamptz = 1_000_000
    0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x42, 0x40,
    0xFF, 0xFF, 0xFF, 0xFF, // recorded_at = NULL
    0x00, 0x00, 0x00, 0x10, // prev_id uuid
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
];

/// `Event` row with the NOT-NULL `id` (uuid) sent as SQL NULL — the per-cell
/// path must classify this rather than default it.
const EVENT_ROW_ID_NULL: &[u8] = &[
    0x00, 0x04, // 4 columns
    0xFF, 0xFF, 0xFF, 0xFF, // id = NULL (NOT NULL column!)
    0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x42, 0x40, // occurred_at
    0xFF, 0xFF, 0xFF, 0xFF, // recorded_at = NULL
    0xFF, 0xFF, 0xFF, 0xFF, // prev_id = NULL
];

/// `EventKey` row with a wrong-width uuid (15 bytes) — the fast path's
/// length check fails, defers to the per-cell path, which classifies the
/// binary length mismatch.
const EVENT_KEY_BAD_UUID_LEN: &[u8] = &[
    0x00, 0x02, // 2 columns
    0x00, 0x00, 0x00, 0x0F, // col0 uuid, len = 15 (WRONG)
    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
    0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x42, 0x40, // occurred_at
];

// ── tests ─────────────────────────────────────────────────────────────────

#[test]
fn uuid_and_timestamptz_fast_path() {
    let borrowed = EventKey::decode(EVENT_KEY_ROW).expect("fast-path decode");
    assert_eq!(borrowed.id, Uuid::from_bytes(UUID_BYTES));
    assert_eq!(borrowed.occurred_at, Timestamptz::from_micros(1_000_000));
    // The owned twin is structurally identical (no borrowing field).
    let owned = EventKeyOwned::decode(EVENT_KEY_ROW).expect("owned fast-path decode");
    assert_eq!(owned.id, Uuid::from_bytes(UUID_BYTES));
    // The timestamptz's exact instant: 2000-01-01 00:00:01 UTC.
    assert_eq!(owned.occurred_at.to_unix_micros(), Some(946_684_801_000_000));
}

#[test]
fn uuid_round_trips_its_hex_form() {
    let borrowed = EventKey::decode(EVENT_KEY_ROW).expect("decode");
    assert_eq!(borrowed.id.to_string(), "550e8400-e29b-41d4-a716-446655440000");
}

#[test]
fn nullable_semantic_columns_become_option() {
    let row = Event::decode(EVENT_ROW_MIXED).expect("decode");
    assert_eq!(row.id, Uuid::from_bytes(UUID_BYTES));
    assert_eq!(row.occurred_at, Timestamptz::from_micros(1_000_000));
    // recorded_at (nullable timestamp) is NULL -> None.
    assert_eq!(row.recorded_at, None::<Timestamp>);
    // prev_id (nullable uuid) is present -> Some.
    assert_eq!(row.prev_id, Some(Uuid::from_bytes(PREV_UUID_BYTES)));
}

#[test]
fn null_in_not_null_uuid_is_classified() {
    let borrowed = Event::decode(EVENT_ROW_ID_NULL);
    assert!(matches!(borrowed, Err(DecodeError::NullInNonNullColumn)));
    let owned = EventOwned::decode(EVENT_ROW_ID_NULL);
    assert!(matches!(owned, Err(DecodeError::NullInNonNullColumn)));
}

#[test]
fn wrong_uuid_width_is_classified_not_truncated() {
    let decoded = EventKey::decode(EVENT_KEY_BAD_UUID_LEN);
    assert!(
        matches!(
            decoded,
            Err(DecodeError::BinaryLengthMismatch { expected_len: 16, actual_len: 15 })
        ),
        "a 15-byte uuid must be a classified length mismatch, got {decoded:?}"
    );
}

#[test]
fn literal_casts_type_the_widened_types() {
    // The `CastRow` query compiling at all proves `::uuid` / `::timestamptz`
    // casts type to the bsql-native types. Decode a matching payload.
    let row: &[u8] = &[
        0x00, 0x02, // 2 columns
        0x00, 0x00, 0x00, 0x10, // u uuid
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00, //
        0x00, 0x00, 0x00, 0x08, // t timestamptz = 1_000_000
        0x00, 0x00, 0x00, 0x00, 0x00, 0x0f, 0x42, 0x40,
    ];
    let decoded = CastRow::decode(row).expect("decode cast row");
    assert_eq!(decoded.u, Uuid::from_bytes(UUID_BYTES));
    assert_eq!(decoded.t.to_unix_micros(), Some(946_684_801_000_000));
}

// ── json / jsonb ────────────────────────────────────────────────────────

/// `Doc` row: payload jsonb = `{"k":1}` (version byte 1 + 7 text bytes),
/// meta json = `[1,2]`.
const DOC_ROW: &[u8] = &[
    0x00, 0x02, // 2 columns
    0x00, 0x00, 0x00, 0x08, // payload jsonb, len = 8 (version + 7 text)
    0x01, b'{', b'"', b'k', b'"', b':', b'1', b'}', // version 1 + {"k":1}
    0x00, 0x00, 0x00, 0x05, // meta json, len = 5
    b'[', b'1', b',', b'2', b']', // [1,2]
];

/// `Doc` row: payload present, meta = NULL.
const DOC_ROW_META_NULL: &[u8] = &[
    0x00, 0x02, // 2 columns
    0x00, 0x00, 0x00, 0x08, //
    0x01, b'{', b'"', b'k', b'"', b':', b'1', b'}', //
    0xFF, 0xFF, 0xFF, 0xFF, // meta = NULL
];

/// `Doc` row with a jsonb version byte of 2 (invalid) — must be classified,
/// never silently decoded.
const DOC_ROW_BAD_VERSION: &[u8] = &[
    0x00, 0x02, // 2 columns
    0x00, 0x00, 0x00, 0x08, //
    0x02, b'{', b'"', b'k', b'"', b':', b'1', b'}', // version 2 (WRONG)
    0x00, 0x00, 0x00, 0x05, b'[', b'1', b',', b'2', b']',
];

#[test]
fn jsonb_and_json_columns_decode_text() {
    let row = Doc::decode(DOC_ROW).expect("decode");
    assert_eq!(row.payload.as_str(), r#"{"k":1}"#);
    assert_eq!(row.meta, Some(Json::new(String::from("[1,2]"))));
    // The owned twin is identical (json/jsonb always own their text).
    let owned = DocOwned::decode(DOC_ROW).expect("decode owned");
    assert_eq!(owned.payload, Jsonb::new(String::from(r#"{"k":1}"#)));
}

#[test]
fn nullable_json_column_is_option() {
    let row = Doc::decode(DOC_ROW_META_NULL).expect("decode");
    assert_eq!(row.payload.as_str(), r#"{"k":1}"#);
    assert_eq!(row.meta, None::<Json>);
}

#[test]
fn jsonb_bad_version_byte_is_classified() {
    let decoded = Doc::decode(DOC_ROW_BAD_VERSION);
    assert!(
        matches!(decoded, Err(DecodeError::JsonbHeaderInvalid { version: Some(2) })),
        "a jsonb version byte != 1 must be classified, got {decoded:?}"
    );
}

// ── footprint pins (64-bit) ────────────────────────────────────────────────
#[cfg(target_pointer_width = "64")]
const _: () = {
    use core::mem::size_of;
    // uuid(16) + timestamptz(8) = 24, all by-value, no lifetime.
    assert!(size_of::<EventKey>() == 24);
    assert!(size_of::<EventKeyOwned>() == 24);
};
