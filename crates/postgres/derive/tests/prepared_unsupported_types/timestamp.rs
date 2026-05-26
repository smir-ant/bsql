//! Pin the `timestamp` rejection at macro-expand. Tracks
//! Runtime gap: timestamp decoding requires picking a chrono/time crate
//! and committing to the wire format (text vs binary microseconds-
//! since-epoch). **Delete this file in the same commit that adds
//! timestamp runtime support.**

extern crate bsql_postgres_proto;

use bsql_postgres_proto::{prepared, PreparedQuery};

// Placeholder Rust type — actual mapping TBD by the runtime decoder/encoder design.
const Q: PreparedQuery<(i64,), ()> = prepared!(
    "SELECT $1::timestamp"
);

fn main() {
    let _ = Q;
}
