//! DEF-244 — pin the `timestamp` rejection at macro-expand. Tracks
//! DEF-228: timestamp decoding requires picking a chrono/time crate
//! and committing to the wire format (text vs binary microseconds-
//! since-epoch). **Delete this file in the same commit that adds
//! timestamp runtime support.**

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, PreparedQuery};

// Placeholder Rust type — actual mapping TBD by DEF-228.
const Q: PreparedQuery<(i64,), ()> = prepared!(
    "SELECT $1::timestamp"
);

fn main() {
    let _ = Q;
}
