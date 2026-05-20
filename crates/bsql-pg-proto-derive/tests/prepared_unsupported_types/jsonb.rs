//! Pin the `jsonb` rejection at macro-expand. Tracks
//! Runtime gap: jsonb binary-format starts with a 1-byte version
//! header (`0x01`); text format is the JSON text bytes. Picking a
//! sonic-rs / simd-json / serde_json mapping is a separate design
//! decision. **Delete this file in the same commit that adds jsonb
//! runtime support.**

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, PreparedQuery};

// Placeholder Rust type — actual mapping TBD by the runtime decoder/encoder design.
const Q: PreparedQuery<(&'static str,), ()> = prepared!(
    "SELECT $1::jsonb"
);

fn main() {
    let _ = Q;
}
