// FOOTPRINT-DRIFT PROBE (size) — a `wire_pin!` with the wrong pinned
// `size` is an E0080 const-eval build failure, fired at type-check time
// for a type that is never instantiated.

#![allow(dead_code)]

use bsql_postgres_proto::wire_pin;

#[repr(C)]
struct Drifted {
    a: u32,
    b: u32,
    c: u32,
} // actual size = 12 B

wire_pin!(Drifted, size = 8, align = 4); // pinned size 8 → E0080

fn main() {}
