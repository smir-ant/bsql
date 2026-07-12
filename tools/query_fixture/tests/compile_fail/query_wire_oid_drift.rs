//! WITNESS (type-distinctness): the PG types `oid` and `int4` — the SAME 4-byte
//! wire width — map to DISTINCT Rust types (`u32` vs `i32`), so a `PreparedQuery`
//! over one is not substitutable for the other (`error[E0308]`).
//!
//! HONEST SCOPE — what this does and does NOT prove. Since the OID lists are now
//! SOURCED from the parameter tuple type (`<Params as ParamsWriter>::OIDS`, in
//! `new_prepared_query`), there is no separate `param_oids` array a caller can
//! lie in. This fixture therefore witnesses the type-level FOUNDATION that makes
//! that sourcing safe: two same-width PG types are DISTINCT Rust types, so the
//! type is a faithful OID discriminator and a `(u32,)`-param query cannot be
//! passed where an `(i32,)`-param one is required. The `error[E0308]` below fires
//! from CONCRETE-TYPE DISTINCTNESS (`(u32,)` and `(i32,)` are different
//! instantiations), NOT from variance and NOT from the OID machinery — this
//! fixture never calls `new_prepared_query`, so it would stay green even if the
//! OID-sourcing itself regressed.
//!
//! The actual "a wrong OID is CAUGHT" property lives in the E0080 wire-template
//! goldens: `query_wire_schema_pin_drift` (the direct constructor cross-checks
//! the pre-baked `Parse` bytes against `<P as ParamsWriter>::OIDS`) and
//! `query_hostile_fingerprint` (the same check through the `run` boundary). The
//! runtime half — that two same-width params encode to their DECLARED OID — is
//! asserted in `tests/query_wire.rs` (`q.param_oids()`).

extern crate bsql_postgres_proto;

use bsql_postgres_proto::PreparedQuery;

fn takes_int4_param(_q: PreparedQuery<(i32,), ()>) {}

fn hand_off(q: PreparedQuery<(u32,), ()>) {
    // `(u32,)` (`oid` = 26) is a different type from `(i32,)` (`int4` = 23) even
    // at the same 4-byte width: the two prepared-query instantiations are not
    // interchangeable.
    takes_int4_param(q);
}

fn main() {
    let _ = hand_off;
}
