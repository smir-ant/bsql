//! `query!` 1-D array result columns: `int4[]` / `text[]` / `uuid[]` decode to
//! `Vec<Option<T>>`, and a NULLABLE array column to `Option<Vec<Option<T>>>`,
//! from hand-built `DataRow` payloads (no live server).
//!
//! Each `query!` below types AT COMPILE TIME against the `array_rows` table in
//! `migrations/` — the mere fact these compile is the proof that a column of
//! `int4[]` / `text[]` / `uuid[]` is no longer a `compile_error!`. The
//! `_field_types` function is a compile-time assertion that each record field
//! has exactly the expected `Vec<Option<T>>` / `Option<Vec<Option<T>>>` shape.
//! The decode assertions prove the array wire bytes materialise, including a
//! NULL element (an honest `None` inside the `Vec`) and a NULL WHOLE array (a
//! `None` for the nullable column).
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "offline test fixture — expect surfaces a malformed hand-built fixture loudly; not a production fallback"
)]

use bsql::Uuid;
use bsql_postgres_proto::DecodeError;

// A SELECT over every array column: `ints` (`int4[]` NOT NULL), `labels`
// (`text[]` NOT NULL), `ids` (`uuid[]` NOT NULL), and `tags` (a NULLABLE
// `text[]`). No column borrows the input (arrays are self-owning), so the
// borrowed record carries no lifetime.
bsql::query!(
    ArrayRow,
    "SELECT id, ints, labels, ids, tags FROM array_rows"
);

// A literal `int4[]` cast (no table) — proves the cast path types to the same
// array type as a catalog column.
bsql::query!(CastArray, "SELECT '{1,2,3}'::int4[] AS xs");

/// Compile-time assertions that each record field has the exact expected type
/// — a 1-D array is `Vec<Option<T>>` (element NULL intrinsic), a nullable
/// array column adds the outer `Option`. This function never runs; it only has
/// to type-check.
#[allow(dead_code, reason = "compile-time field-type assertion; never called")]
fn _field_types(r: ArrayRow) {
    let _id: i32 = r.id;
    let _ints: Vec<Option<i32>> = r.ints;
    let _labels: Vec<Option<String>> = r.labels;
    let _ids: Vec<Option<Uuid>> = r.ids;
    let _tags: Option<Vec<Option<String>>> = r.tags;
}

// ── array wire builders ───────────────────────────────────────────────────

/// Build a 1-D PG binary array body: header + per-element `(len, body)`
/// (`None` element -> `-1`).
fn array_body(elem_oid: u32, elems: &[Option<&[u8]>]) -> Vec<u8> {
    let mut out = Vec::new();
    let has_null = if elems.iter().any(Option::is_none) { 1i32 } else { 0 };
    out.extend_from_slice(&1i32.to_be_bytes()); // ndim = 1
    out.extend_from_slice(&has_null.to_be_bytes());
    out.extend_from_slice(&elem_oid.to_be_bytes());
    out.extend_from_slice(&i32::try_from(elems.len()).expect("len").to_be_bytes()); // dim_len
    out.extend_from_slice(&1i32.to_be_bytes()); // lower bound
    for elem in elems {
        match elem {
            Some(body) => {
                out.extend_from_slice(&i32::try_from(body.len()).expect("elen").to_be_bytes());
                out.extend_from_slice(body);
            }
            None => out.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    out
}

/// Build a `DataRow` body: 2-byte column count + per-column `(len, body)`
/// (`None` = a NULL column -> `-1`).
fn data_row(cols: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&i16::try_from(cols.len()).expect("cols").to_be_bytes());
    for col in cols {
        match col {
            Some(body) => {
                out.extend_from_slice(&i32::try_from(body.len()).expect("clen").to_be_bytes());
                out.extend_from_slice(body);
            }
            None => out.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    out
}

const UUID_A: [u8; 16] = [
    0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
];
const UUID_B: [u8; 16] = [
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
];

// ── tests ─────────────────────────────────────────────────────────────────

#[test]
fn array_row_decodes_with_null_element_and_null_whole_array() {
    let ten = 10i32.to_be_bytes();
    let thirty = 30i32.to_be_bytes();
    let ints = array_body(23, &[Some(&ten), None, Some(&thirty)]); // {10, NULL, 30}
    let labels = array_body(25, &[Some(b"a"), None, Some(b"c")]); // {"a", NULL, "c"}
    let ids = array_body(2950, &[Some(&UUID_A), Some(&UUID_B)]); // {uuid_a, uuid_b}
    let row = data_row(&[
        Some(1i32.to_be_bytes().to_vec()), // id
        Some(ints),
        Some(labels),
        Some(ids),
        None, // tags: whole array is SQL NULL
    ]);

    let r = ArrayRow::decode(&row).expect("array row decodes");
    assert_eq!(r.id, 1);
    // int4[] with a NULL middle element -> honest `None`.
    assert_eq!(r.ints, vec![Some(10), None, Some(30)]);
    // text[] with a NULL middle element -> owned `String`s.
    assert_eq!(
        r.labels,
        vec![Some(String::from("a")), None, Some(String::from("c"))]
    );
    // uuid[] -> owned `Uuid` values.
    assert_eq!(r.ids, vec![Some(Uuid::from_bytes(UUID_A)), Some(Uuid::from_bytes(UUID_B))]);
    // The NULLABLE `tags` column is a NULL whole array -> `None`.
    assert_eq!(r.tags, None);

    // The owned twin is structurally identical (arrays are self-owning).
    let owned = ArrayRow::decode(&row).expect("owned decodes");
    assert_eq!(owned.ints, vec![Some(10), None, Some(30)]);
    assert_eq!(owned.tags, None);
}

#[test]
fn nullable_array_present_is_some() {
    // The same row but with `tags` present -> `Some(vec![...])`.
    let ints = array_body(23, &[Some(&1i32.to_be_bytes())]);
    let labels = array_body(25, &[Some(b"x")]);
    let ids = array_body(2950, &[Some(&UUID_A)]);
    let tags = array_body(25, &[Some(b"hot"), None]);
    let row = data_row(&[
        Some(2i32.to_be_bytes().to_vec()),
        Some(ints),
        Some(labels),
        Some(ids),
        Some(tags),
    ]);
    let r = ArrayRow::decode(&row).expect("decodes");
    assert_eq!(r.tags, Some(vec![Some(String::from("hot")), None]));
}

#[test]
fn empty_array_decodes_to_empty_vec() {
    // ndim = 0 is PG's empty array.
    let mut empty_ints = Vec::new();
    empty_ints.extend_from_slice(&0i32.to_be_bytes()); // ndim = 0
    empty_ints.extend_from_slice(&0i32.to_be_bytes()); // flags
    empty_ints.extend_from_slice(&23u32.to_be_bytes()); // elem oid
    let row = data_row(&[
        Some(3i32.to_be_bytes().to_vec()),
        Some(empty_ints),
        Some(array_body(25, &[])),
        Some(array_body(2950, &[])),
        None,
    ]);
    let r = ArrayRow::decode(&row).expect("decodes");
    assert!(r.ints.is_empty());
    assert!(r.labels.is_empty());
}

#[test]
fn multidim_array_element_is_classified() {
    // An `ints` column whose header claims ndim = 2 is a classified error, not
    // a silently-flattened array.
    let mut multi = Vec::new();
    multi.extend_from_slice(&2i32.to_be_bytes()); // ndim = 2
    multi.extend_from_slice(&0i32.to_be_bytes());
    multi.extend_from_slice(&23u32.to_be_bytes());
    let row = data_row(&[
        Some(4i32.to_be_bytes().to_vec()),
        Some(multi),
        Some(array_body(25, &[])),
        Some(array_body(2950, &[])),
        None,
    ]);
    let decoded = ArrayRow::decode(&row);
    assert!(
        matches!(decoded, Err(DecodeError::ArrayMultiDim { ndim: 2 })),
        "a 2-D array element must be classified, got {decoded:?}"
    );
}

#[test]
fn wrong_element_oid_is_classified() {
    // An `ints` (int4[]) column whose header declares a text element OID is a
    // classified mismatch, never reinterpreted.
    let bad = array_body(25, &[Some(b"x")]); // header says text (25)
    let row = data_row(&[
        Some(5i32.to_be_bytes().to_vec()),
        Some(bad),
        Some(array_body(25, &[])),
        Some(array_body(2950, &[])),
        None,
    ]);
    let decoded = ArrayRow::decode(&row);
    assert!(
        matches!(
            decoded,
            Err(DecodeError::ArrayElemOidMismatch { expected: 23, found: 25 })
        ),
        "an int4[] header declaring a text element must be classified, got {decoded:?}"
    );
}

#[test]
fn literal_int4_array_cast_decodes() {
    // `CastArray` compiling proves `'{...}'::int4[]` types to `Vec<Option<i32>>`.
    let one = 1i32.to_be_bytes();
    let two = 2i32.to_be_bytes();
    let three = 3i32.to_be_bytes();
    let body = array_body(23, &[Some(&one), Some(&two), Some(&three)]);
    let row = data_row(&[Some(body)]);
    let r = CastArray::decode(&row).expect("decodes");
    assert_eq!(r.xs, vec![Some(1), Some(2), Some(3)]);
}
