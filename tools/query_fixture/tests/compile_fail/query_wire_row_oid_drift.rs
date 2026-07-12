//! WITNESS (type-distinctness) for the closed silent mis-decode class: the PG
//! types `oid` and `int4` — the SAME 4-byte wire width — map to DISTINCT Rust
//! types (`u32` vs `i32`), so a `PreparedQuery` over one row type is not
//! substitutable for the other (`error[E0308]`).
//!
//! HONEST SCOPE — what this does and does NOT prove. The row OID list is now
//! SOURCED from `<Row as RowDecode>::OIDS`, whose every entry is `<marker as
//! ColCellAt>::OID` — the SAME marker the record decode routes through — so the
//! row OID and the decoder are ONE source and cannot drift (and, unlike the param
//! OID, the row OID has no independent wire representation to cross-check). That
//! makes "the row decodes as its DECLARED type" STRUCTURAL. This fixture
//! witnesses the type-level FOUNDATION beneath it: two same-width PG types are
//! DISTINCT Rust types, so a `(u32,)`-row query cannot be passed where an
//! `(i32,)`-row one is required. The `error[E0308]` below fires from
//! CONCRETE-TYPE DISTINCTNESS (`(u32,)` and `(i32,)` are different
//! instantiations), NOT from variance and NOT from any OID check — this fixture
//! never calls `new_prepared_query`, so it would stay green even if the
//! OID-sourcing regressed.
//!
//! The runtime half — that two same-width columns actually DECODE to their
//! declared values, not a width-based conflation — is
//! `tests/query_same_width_decode.rs`. There is no compile-time row-OID
//! cross-check to witness (the row OID has no independent wire representation);
//! the param-side wire pin is the E0080 `query_wire_schema_pin_drift` /
//! `query_hostile_fingerprint` goldens.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::PreparedQuery;

fn takes_int4_row(_q: PreparedQuery<(), (i32,)>) {}

fn hand_off(q: PreparedQuery<(), (u32,)>) {
    // `(u32,)` (`oid` = 26) is a different type from `(i32,)` (`int4` = 23) even
    // at the same 4-byte width: the two prepared-query instantiations are not
    // interchangeable.
    takes_int4_row(q);
}

fn main() {
    let _ = hand_off;
}
