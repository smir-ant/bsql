//! Pin the `float4` (PG REAL) rejection at macro-expand.
//! Awaits runtime decoder/encoder support: float decoding requires special-care text-format
//! (IEEE 754 special-value strings: `NaN`, `Infinity`, `-Infinity`,
//! plus locale-insensitive decimal). When `DecodeFormat<TextFmt>`
//! for `f32` lands, the macro grows the `float4` entry and **this
//! file must be deleted in the same commit**.

extern crate bsql_pg_proto;

use bsql_pg_proto::{prepared, PreparedQuery};

const Q: PreparedQuery<(f32,), ()> = prepared!(
    "SELECT $1::float4"
);

fn main() {
    let _ = Q;
}
