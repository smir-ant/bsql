//! WITNESS (compile-side) for the closed silent mis-decode class
//! (glass→compiler 3.1): a SAME-WIDTH row-OID drift is `error[E0080]`.
//!
//! The declared `Row = (u32,)` — its decoder is `oid` (OID 26). The supplied
//! `row_oids` is `[23]` (`int4`), which is the SAME 4-byte wire width but a
//! DIFFERENT type. Because the record decode and the wire OID now derive from
//! the ONE row-tuple marker (`<u32 as ColCellAt>::OID` is what the validator
//! pins `row_oids` against, and the decode routes through that same `u32`
//! marker), a same-width divergence like this can never be silent: it is a
//! const-evaluation failure at the validating constructor, with no unchecked
//! twin. This is the compile-time half of the witness; the runtime half —
//! that two same-width columns decode to their DECLARED values, not a
//! width-based conflation — is `tests/query_same_width_decode.rs`.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::PreparedQuery;
use bsql_postgres_proto::prepared::new_prepared_query;

const Q: PreparedQuery<(), (u32,)> = new_prepared_query::<(), (u32,)>(
    "SELECT 1::oid",
    "bsql_q_row_drift",
    &[],
    // Same width as `u32` (4 bytes) but the WRONG type: `int4` = 23, while the
    // `(u32,)` decoder's `ColCellAt::OID` is `oid` = 26.
    &[23],
    &[],
    &[0, 0],
);

fn main() {
    let _ = &Q;
}
