//! DEF-244 — pin the `numeric` (arbitrary-precision DECIMAL)
//! rejection at macro-expand. Tracks DEF-228: numeric requires a
//! `bigdecimal` / `rust_decimal` dependency or a hand-rolled
//! decimal type. **Delete this file in the same commit that adds
//! numeric runtime support.**

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, PreparedQuery};

// Placeholder Rust type — actual mapping TBD by DEF-228.
const Q: PreparedQuery<(i64,), ()> = prepared!(
    "SELECT $1::numeric"
);

fn main() {
    let _ = Q;
}
