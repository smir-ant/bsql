//! DEF-244 — pin the `uuid` rejection at macro-expand. Tracks
//! DEF-228: UUID requires picking a uuid crate (uuid or
//! standard-library nothing yet) for the Rust mapping. **Delete
//! this file in the same commit that adds UUID runtime support.**

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, PreparedQuery};

// Placeholder Rust type — actual mapping TBD by DEF-228.
const Q: PreparedQuery<(&'static str,), ()> = prepared!(
    "SELECT $1::uuid"
);

fn main() {
    let _ = Q;
}
