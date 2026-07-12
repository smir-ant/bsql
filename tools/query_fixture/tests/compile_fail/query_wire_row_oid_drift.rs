//! WITNESS (compile-side) for the closed silent mis-decode class: a SAME-WIDTH
//! row-OID drift is now UNREPRESENTABLE — a hard `error[E0308]`.
//!
//! The row OID list is no longer a `new_prepared_query` argument: it is SOURCED
//! from `<Row as RowDecode>::OIDS`, whose every entry is `<marker as
//! ColCellAt>::OID` — the SAME marker the record decode routes through. So the
//! row OID and the decoder are ONE source and cannot drift; there is no
//! `row_oids` slice to lie in, and (unlike the param OID) the row OID has no
//! independent wire representation to cross-check. The property "the row
//! decodes as its DECLARED type" is therefore STRUCTURAL, not a caught
//! const-assert.
//!
//! What remains expressible is the type identity itself: `(u32,)` is the `oid`
//! type (OID 26) and `(i32,)` is `int4` (OID 23) — the SAME 4-byte wire width,
//! DIFFERENT types. `PreparedQuery` is INVARIANT in `Row`, so a `(u32,)`-row
//! prepared query cannot stand in for an `(i32,)`-row one: a same-width row-OID
//! confusion is a TYPE error. The runtime half — that two same-width columns
//! decode to their DECLARED values, not a width-based conflation — is
//! `tests/query_same_width_decode.rs`.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::PreparedQuery;

fn takes_int4_row(_q: PreparedQuery<(), (i32,)>) {}

fn hand_off(q: PreparedQuery<(), (u32,)>) {
    // `(u32,)` (`oid` = 26) is not `(i32,)` (`int4` = 23) even at the same
    // 4-byte width: a same-width row-OID confusion is a TYPE error.
    takes_int4_row(q);
}

fn main() {
    let _ = hand_off;
}
