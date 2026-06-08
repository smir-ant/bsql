// FOOTPRINT-DRIFT PROBE (align) — a `wire_pin!` with the wrong pinned
// `align` is an E0080 const-eval build failure, EVEN WHEN THE SIZE IS
// PRESERVED. This is the dimension a bare `size_of` anchor cannot see.

#![allow(dead_code)]

use bsql_postgres_proto::wire_pin;

#[repr(C, align(8))]
struct AlignDrifted {
    a: u32,
    b: u32,
} // size = 8 B (unchanged), align = 8

wire_pin!(AlignDrifted, size = 8, align = 4); // pinned align 4 → E0080

fn main() {}
