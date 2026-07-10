//! OFFLINE proof of the generated user-COMPOSITE codegen — no PostgreSQL.
//!
//! `bsql::user_types!()` turns the `0017_composites.sql` migration
//! (`CREATE TYPE addr AS (street text, zip int4)`, plus a nested `region` and an
//! enum-bearing `tagged`) into real Rust `struct`s with a `PgComposite` row-type
//! binary frame decoder. This exercises the GENERATED decoder directly, with NO
//! live server — the codegen itself is the unit under test:
//!
//!   * every field is `Option<T>` (a composite attribute is nullable on the wire),
//!   * `decode_row` walks the exact `record_send` frame captured from a real
//!     server (`int32 nfields`, then per field `{uint32 oid, int32 len, bytes}`),
//!   * a NULL field (`len = -1`) decodes to `None`,
//!   * an ARITY mismatch / a TRUNCATED frame is a classified `DecodeError`, never
//!     a panic or a partial record,
//!   * a NESTED composite field recurses into its own `decode_row`,
//!   * an ENUM field recurses into the label reshape (unknown label classified).
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "test harness — expect/unwrap surface failures loudly"
)]

use bsql::{DecodeError, PgComposite as _};

bsql::user_types!();

/// Build a composite (row-type) binary frame from `(oid, Option<body>)` fields —
/// the exact `record_send` wire form. The field OID is never validated on decode
/// (dynamic), so an arbitrary but realistic OID is fine.
fn frame(fields: &[(u32, Option<&[u8]>)]) -> Vec<u8> {
    let mut out = Vec::new();
    let n = i32::try_from(fields.len()).unwrap();
    out.extend_from_slice(&n.to_be_bytes());
    for (oid, body) in fields {
        out.extend_from_slice(&oid.to_be_bytes());
        match body {
            None => out.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(b) => {
                out.extend_from_slice(&i32::try_from(b.len()).unwrap().to_be_bytes());
                out.extend_from_slice(b);
            }
        }
    }
    out
}

// PG binary OIDs used in the crafted frames (values are informational — decode
// does not check them).
const OID_TEXT: u32 = 25;
const OID_INT4: u32 = 23;
const OID_RECORD: u32 = 2249; // pseudo-type `record`; the field OID is ignored

fn addr_frame(street: Option<&str>, zip: Option<i32>) -> Vec<u8> {
    let zip_bytes = zip.map(i32::to_be_bytes);
    frame(&[
        (OID_TEXT, street.map(str::as_bytes)),
        (OID_INT4, zip_bytes.as_ref().map(|b| &b[..])),
    ])
}

#[test]
fn addr_decode_row_reads_present_fields() {
    let a = Addr::decode_row(&addr_frame(Some("main st"), Some(5))).expect("decode addr");
    assert_eq!(a.street.as_deref(), Some("main st"));
    assert_eq!(a.zip, Some(5));
}

#[test]
fn addr_null_field_decodes_to_none() {
    // A NULL street field (`len = -1`) is `None`; the present int stays `Some`.
    let a = Addr::decode_row(&addr_frame(None, Some(5))).expect("decode addr");
    assert_eq!(a.street, None, "a NULL composite field decodes to None");
    assert_eq!(a.zip, Some(5));

    let b = Addr::decode_row(&addr_frame(Some("x"), None)).expect("decode addr");
    assert_eq!(b.street.as_deref(), Some("x"));
    assert_eq!(b.zip, None);
}

#[test]
fn addr_arity_mismatch_is_classified() {
    // The migration declares `addr` with 2 fields; a 3-field frame (an
    // out-of-band `ADD ATTRIBUTE` on the live type) is a classified mismatch.
    let three = frame(&[
        (OID_TEXT, Some(b"main st")),
        (OID_INT4, Some(&5i32.to_be_bytes())),
        (OID_TEXT, Some(b"US")),
    ]);
    match Addr::decode_row(&three) {
        Err(DecodeError::CompositeArityMismatch { expected: 2, found: 3 }) => {}
        other => panic!("expected a classified CompositeArityMismatch, got {other:?}"),
    }
}

#[test]
fn addr_truncated_frame_is_classified() {
    // A frame that declares 2 fields but is cut off mid-header.
    let mut bad = Vec::new();
    bad.extend_from_slice(&2i32.to_be_bytes());
    bad.extend_from_slice(&OID_TEXT.to_be_bytes());
    bad.extend_from_slice(&[0x00, 0x00]); // partial length
    match Addr::decode_row(&bad) {
        Err(DecodeError::CompositeTruncated) => {}
        other => panic!("expected a classified CompositeTruncated, got {other:?}"),
    }
    // Trailing surplus past the last field is also classified.
    let mut surplus = addr_frame(Some("x"), Some(1));
    surplus.extend_from_slice(&[0xDE, 0xAD]);
    assert!(matches!(
        Addr::decode_row(&surplus),
        Err(DecodeError::CompositeTruncated)
    ));
}

#[test]
fn region_nested_composite_recurses() {
    // `region (name text, seat addr)` — the `seat` field's bytes are a full
    // `addr` frame, decoded by recursing into `Addr::decode_row`.
    let seat = addr_frame(Some("elm st"), Some(7));
    let r = Region::decode_row(&frame(&[
        (OID_TEXT, Some(b"west")),
        (OID_RECORD, Some(&seat)),
    ]))
    .expect("decode region");
    assert_eq!(r.name.as_deref(), Some("west"));
    let seat = r.seat.expect("seat present");
    assert_eq!(seat.street.as_deref(), Some("elm st"));
    assert_eq!(seat.zip, Some(7));

    // A NULL nested-composite field decodes to `None` (not a recurse into an
    // empty frame).
    let r2 = Region::decode_row(&frame(&[(OID_TEXT, Some(b"east")), (OID_RECORD, None)]))
        .expect("decode region");
    assert_eq!(r2.seat, None);
}

#[test]
fn tagged_enum_field_recurses_into_the_label_reshape() {
    // `tagged (label text, feeling mood)` — the `feeling` field is a `mood` enum
    // label, decoded by recursing into `Mood::from_wire_label`.
    let t = Tagged::decode_row(&frame(&[
        (OID_TEXT, Some(b"note")),
        (OID_TEXT, Some(b"happy")), // an enum travels as its label text
    ]))
    .expect("decode tagged");
    assert_eq!(t.label.as_deref(), Some("note"));
    assert_eq!(t.feeling, Some(Mood::Happy));

    // An unknown enum label in a composite field is classified, never a panic.
    let bad = frame(&[(OID_TEXT, Some(b"note")), (OID_TEXT, Some(b"ecstatic"))]);
    match Tagged::decode_row(&bad) {
        Err(DecodeError::UnknownEnumLabel) => {}
        other => panic!("expected a classified UnknownEnumLabel, got {other:?}"),
    }

    // A NULL enum field decodes to None.
    let t2 = Tagged::decode_row(&frame(&[(OID_TEXT, Some(b"note")), (OID_TEXT, None)]))
        .expect("decode tagged");
    assert_eq!(t2.feeling, None);
}

#[test]
fn field_names_colliding_with_generated_locals_still_decode() {
    // `collide (__reader int4, __frame text, __bytes int4, ok int4)` — every
    // attribute name collides with an internal `decode_row` local. Mixed-site
    // hygiene isolates the reader / frame; the `__bytes` match binding is
    // inner-scoped. If any collision shadowed a generated local, this would fail
    // to compile OR mis-decode; it compiles and round-trips exactly.
    let seven = 7i32.to_be_bytes();
    let nine = 9i32.to_be_bytes();
    let eleven = 11i32.to_be_bytes();
    let c = Collide::decode_row(&frame(&[
        (OID_INT4, Some(&seven)),  // __reader
        (OID_TEXT, Some(b"road")), // __frame
        (OID_INT4, Some(&nine)),   // __bytes
        (OID_INT4, Some(&eleven)), // ok
    ]))
    .expect("decode collide");
    assert_eq!(c.__reader, Some(7), "the `__reader` field decodes (reader local not shadowed)");
    assert_eq!(c.__frame.as_deref(), Some("road"), "the `__frame` field decodes");
    assert_eq!(c.__bytes, Some(9), "the `__bytes` field decodes");
    assert_eq!(c.ok, Some(11), "a field AFTER the collisions still decodes");

    // A NULL in a colliding field is still `None` (the reader keeps stepping).
    let c2 = Collide::decode_row(&frame(&[
        (OID_INT4, None),          // __reader NULL
        (OID_TEXT, Some(b"x")),    // __frame
        (OID_INT4, None),          // __bytes NULL
        (OID_INT4, Some(&eleven)), // ok
    ]))
    .expect("decode collide with nulls");
    assert_eq!(c2.__reader, None);
    assert_eq!(c2.__frame.as_deref(), Some("x"));
    assert_eq!(c2.__bytes, None);
    assert_eq!(c2.ok, Some(11));
}
