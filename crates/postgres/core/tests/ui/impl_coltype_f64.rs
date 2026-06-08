// CLOSED COLUMN-TYPE SET (directly): `impl ColType for f64` is E0117 —
// orphan rule, `f64` is a foreign primitive. A seventh column type is
// impossible from a downstream crate.

use bsql_postgres_core::col::ColType;

impl ColType for f64 {
    const OID: u32 = 0;
    type Value<'a> = f64;
}

fn main() {}
