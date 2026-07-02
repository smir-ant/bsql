// `Row.x` is `f32` (the `float4` cast), NOT `f64`: the widened type system
// keeps the wrong-type wall. Returning the `f32` field where an `f64` is
// expected is an E0308 type mismatch against the typed record — proving that
// widening `{f32, f64, bytea}` did NOT weaken the compile-time type safety
// (an `f32` column is never silently an `f64`, and vice versa).
bsql::query!(Row, "SELECT 2.5::float4 AS x");

fn take(r: Row) -> f64 {
    r.x
}

fn main() {}
