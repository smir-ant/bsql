//! Pin the `numeric` (arbitrary-precision DECIMAL)
//! rejection at macro-expand. Awaits runtime decoder/encoder support: numeric requires a
//! `bigdecimal` / `rust_decimal` dependency or a hand-rolled
//! decimal type. **Delete this file in the same commit that adds
//! numeric runtime support.**

extern crate bsql_postgres_proto;

use bsql_postgres_proto::{prepared, PreparedQuery};

// Placeholder Rust type — actual mapping TBD by the runtime decoder/encoder design.
const Q: PreparedQuery<(i64,), ()> = prepared!(
    "SELECT $1::numeric"
);

fn main() {
    let _ = Q;
}
