// The `encode` helper below is not a `#[test]` fn, so the floor's
// `allow-expect-in-tests` carve-out (keyed on `#[test]` context) does not
// reach it; its `expect` is the loud encode-failure signal a fixture wants
// (an over-budget buffer would be a test bug), not a production fallback.
#![allow(
    clippy::expect_used,
    reason = "fixture helper — expect() surfaces an encode failure loudly; not a `#[test]` fn so the in-tests carve-out cannot reach it, and there is no production path"
)]

//! Corpus of the `col = ANY($N)` in-list Bind-frame bytes — the wire
//! encoding of a SINGLE array parameter, produced by the sole format
//! authority [`ParamsWriter`] (binary-uniform, like every other param).
//!
//! These pin the PostgreSQL one-dimensional binary array layout the macro
//! relies on when it lowers `col = ANY($N)` to a single array bind. The
//! expected bytes are hand-derived from PG `array_send`
//! (`src/backend/utils/adt/arrayfuncs.c`):
//!
//! ```text
//! per-param outer length prefix: i32_be (the array body's byte length)
//!   ndim:        i32_be = 1
//!   has_null:    i32_be = 0
//!   element_oid: i32_be (the scalar element type OID)
//!   dim_len:     i32_be = N
//!   lower_bound: i32_be = 1
//!   per element: { len_i32_be, body }
//! ```
//!
//! Both halves of the contract are checked: the array bytes themselves AND
//! that the `query!`-baked param OID for the same column matches the
//! element's array OID — so the bytes on the wire and the type declared in
//! the Parse frame cannot disagree.

use bsql_postgres_proto::oids;
use bsql_postgres_proto::params::ParamsWriter;
use bsql_postgres_proto::WriteBuf;

// The macro that lowers `id = ANY($1)` to a single `int8[]` array param.
bsql::query!(AnyInts, "SELECT id FROM orders WHERE id = ANY($1)");

/// Encode one parameter tuple's Bind param-value block to bytes.
fn encode<P: ParamsWriter>(params: &P) -> Vec<u8> {
    let mut buf = WriteBuf::new();
    params
        .write_params(&mut buf)
        .expect("array param fits the send buffer");
    buf.as_bytes().to_vec()
}

#[test]
fn int8_array_three_elements_bind_bytes() {
    // One param: int8[] = {10, 20, 30}.
    let bytes = encode(&(&[10i64, 20i64, 30i64][..],));
    assert_eq!(
        bytes,
        vec![
            // outer per-param length prefix = 56 bytes of array body
            0x00, 0x00, 0x00, 0x38, //
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x00, // has_null = 0
            0x00, 0x00, 0x00, 0x14, // element_oid = 20 (int8)
            0x00, 0x00, 0x00, 0x03, // dim length = 3
            0x00, 0x00, 0x00, 0x01, // lower bound = 1
            0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 10, // elem 10
            0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 20, // elem 20
            0x00, 0x00, 0x00, 0x08, 0, 0, 0, 0, 0, 0, 0, 30, // elem 30
        ],
    );
}

#[test]
fn int4_array_two_elements_bind_bytes() {
    let bytes = encode(&(&[7i32, 9i32][..],));
    assert_eq!(
        bytes,
        vec![
            0x00, 0x00, 0x00, 0x24, // outer length = 36
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x00, // has_null = 0
            0x00, 0x00, 0x00, 0x17, // element_oid = 23 (int4)
            0x00, 0x00, 0x00, 0x02, // dim length = 2
            0x00, 0x00, 0x00, 0x01, // lower bound = 1
            0x00, 0x00, 0x00, 0x04, 0, 0, 0, 7, // elem 7
            0x00, 0x00, 0x00, 0x04, 0, 0, 0, 9, // elem 9
        ],
    );
}

#[test]
fn text_array_bind_bytes() {
    let bytes = encode(&(&["hi", "yo"][..],));
    assert_eq!(
        bytes,
        vec![
            0x00, 0x00, 0x00, 0x20, // outer length = 32 (header 20 + 2*(4+2))
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x00, // has_null = 0
            0x00, 0x00, 0x00, 0x19, // element_oid = 25 (text)
            0x00, 0x00, 0x00, 0x02, // dim length = 2
            0x00, 0x00, 0x00, 0x01, // lower bound = 1
            0x00, 0x00, 0x00, 0x02, b'h', b'i', // elem "hi"
            0x00, 0x00, 0x00, 0x02, b'y', b'o', // elem "yo"
        ],
    );
}

#[test]
fn empty_int8_array_bind_bytes() {
    // An empty in-list is still a well-formed zero-length array (the macro
    // never collapses it to a different SQL form).
    let empty: &[i64] = &[];
    let bytes = encode(&(empty,));
    // PG's canonical empty array: zero dimensions (matches
    // `array_send('{}'::int8[])` exactly).
    assert_eq!(
        bytes,
        vec![
            0x00, 0x00, 0x00, 0x0C, // outer length = 12 (3 i32 words)
            0x00, 0x00, 0x00, 0x00, // ndim = 0
            0x00, 0x00, 0x00, 0x00, // has_null = 0
            0x00, 0x00, 0x00, 0x14, // element_oid = 20 (int8)
        ],
    );
}

#[test]
fn baked_param_oid_matches_array_wire() {
    // The `query!`-baked param OID for `id = ANY($1)` is the int8[] array
    // OID — the SAME OID the array body declares as its element type's
    // array, so the Parse-frame type and the Bind bytes cannot drift.
    assert_eq!(AnyIntsQuery::PREPARED.param_oids(), &[oids::INT8_ARRAY]);
    assert_eq!(oids::INT8_ARRAY, 1016);
}
