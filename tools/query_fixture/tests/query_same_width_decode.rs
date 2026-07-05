//! WITNESS for the closed silent mis-decode class (glass→compiler 3.1).
//!
//! Two projected columns of the SAME wire width (4 bytes) but DIFFERENT
//! declared type — `oid` (`u32`, OID 26) and `int4` (`i32`, OID 23) — decoded
//! from ONE canned `DataRow` whose two cells carry the SAME bit pattern
//! (`0xFFFFFFFF`). The witness: the `u32` column decodes to `4294967295` and
//! the `i32` column to `-1` — each honors its DECLARED type, never a
//! width-based conflation.
//!
//! This is the proof the unification closed the drift: after it, the record
//! decode routes through the SAME marker (`u32` / `i32`) whose
//! `ColCellAt::OID` the const validator pins into `ROW_OIDS`. Before it,
//! `cell_marker` (the decode map) was a THIRD parallel map, unchecked against
//! the OID-validated `tuple_marker`; a same-width divergence there would have
//! silently turned this wire `-1` into `4294967295` (or vice versa). Because
//! decoder and wire OID are now one source, a same-width mismatch is either an
//! `error[E0080]` at the validator or an `error[E0308]` at the record — never
//! a silent mis-decode. The compile-side half of the witness is the
//! `query_wire_row_oid_drift` trybuild golden (a same-width `u32`-vs-`int4`
//! ROW-OID drift is `error[E0080]`).
//!
//! Structurally offline — the `DataRow` body is hand-built (no live server).

// `as_oid` is `oid` -> `u32` (OID 26); `as_int` is `int4` -> `i32` (OID 23).
// Both are non-null literal casts, so both columns are `NOT NULL` and
// fixed-width -> the borrowed record carries no lifetime and the decode emits
// the vectorized const-offset fast path (which now decodes each column through
// `<marker as ColCellAt<'_>>::decode_at`, the OID-validated marker).
bsql::query!(SameWidth, "SELECT 1::oid AS as_oid, 1::int4 AS as_int");

/// One `DataRow` body: two 4-byte columns, BOTH the bit pattern `0xFFFFFFFF`.
/// As `u32` that is `4294967295`; as `i32` that is `-1`.
const SAME_WIDTH_ROW: &[u8] = &[
    0x00, 0x02, // 2 columns
    0x00, 0x00, 0x00, 0x04, 0xFF, 0xFF, 0xFF, 0xFF, // col0 (oid / u32)
    0x00, 0x00, 0x00, 0x04, 0xFF, 0xFF, 0xFF, 0xFF, // col1 (int4 / i32)
];

#[test]
fn same_width_columns_decode_per_declared_type() {
    let borrowed = SameWidth::decode(SAME_WIDTH_ROW).expect("row decodes");
    // Same 4 bytes, DIFFERENT decoded value — each column honors its own
    // OID-validated marker, not a width-based decode.
    assert_eq!(borrowed.as_oid, u32::MAX); // 4294967295
    assert_eq!(borrowed.as_int, -1_i32);

    // The owned twin decodes identically (both columns are self-owning value
    // types, so the owned/borrowed twins carry the same fields).
    let owned = SameWidthOwned::decode(SAME_WIDTH_ROW).expect("owned row decodes");
    assert_eq!(owned.as_oid, u32::MAX);
    assert_eq!(owned.as_int, -1_i32);
}
