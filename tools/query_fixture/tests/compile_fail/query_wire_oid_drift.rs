//! The validating constructor rejects a parameter OID list that drifts
//! from the declared parameter tuple — `error[E0080]` at the `const`
//! binding, never a silently-wrong artifact.
//!
//! Declared `Params = (i32,)`, whose OID is `int4` (23). The supplied
//! `param_oids` is `[99]`, which matches no declared type. There is no
//! unchecked twin constructor, so this cannot be smuggled past the
//! check.

extern crate bsql_postgres_proto;

use bsql_postgres_proto::prepared::new_prepared_query;
use bsql_postgres_proto::PreparedQuery;

const Q: PreparedQuery<(i32,), ()> = new_prepared_query::<(i32,), ()>(
    "SELECT $1::int4",
    "bsql_q_drift",
    &[99],
    &[],
    &[],
    &[0, 0],
);

fn main() {
    let _ = &Q;
}
