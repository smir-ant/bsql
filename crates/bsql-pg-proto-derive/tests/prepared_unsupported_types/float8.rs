//! Pin the `float8` (PG DOUBLE PRECISION) rejection at
//! macro-expand. Same runtime dependency as `float4`. **Delete this
//! file in the same commit that adds `f64` runtime support.**

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, PreparedQuery};

const Q: PreparedQuery<(f64,), ()> = prepared!(
    "SELECT $1::float8"
);

fn main() {
    let _ = Q;
}
