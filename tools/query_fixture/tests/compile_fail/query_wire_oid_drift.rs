//! WITNESS: a parameter OID that disagrees with the declared type is now
//! UNREPRESENTABLE, strictly stronger than the former const cross-check.
//!
//! `new_prepared_query` no longer accepts a `param_oids` slice: the param OID
//! list is SOURCED from `<Params as ParamsWriter>::OIDS`, so there is no
//! separate array to lie in. A "wrong param OID" is therefore a WRONG PARAM
//! TYPE — and `PreparedQuery` is INVARIANT in `Params`, so a same-width type
//! confusion is a hard `error[E0308]`, not a value the validator has to catch.
//!
//! Here `(u32,)` is the `oid` type (OID 26) and `(i32,)` is `int4` (OID 23):
//! the SAME 4-byte wire width, DIFFERENT types. Before the OID lists were
//! sourced from the tuple, this pair was distinguished only by the
//! `oids_equal(param_oids, P::OIDS)` const assert; now it is distinguished by
//! the TYPE itself — a `(u32,)`-param prepared query cannot stand in for an
//! `(i32,)`-param one. (The runtime half — that two same-width params encode
//! to their DECLARED type — rides the same single-source `ParamsWriter::OIDS`.)

extern crate bsql_postgres_proto;

use bsql_postgres_proto::PreparedQuery;

fn takes_int4_param(_q: PreparedQuery<(i32,), ()>) {}

fn hand_off(q: PreparedQuery<(u32,), ()>) {
    // `(u32,)` (`oid` = 26) is not `(i32,)` (`int4` = 23) even at the same
    // 4-byte width: a same-width param-OID confusion is a TYPE error.
    takes_int4_param(q);
}

fn main() {
    let _ = hand_off;
}
