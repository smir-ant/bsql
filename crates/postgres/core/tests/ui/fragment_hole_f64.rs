// NON-BINDABLE HOLE = E0277. A `{}` hole accepts exactly i16, i32, i64,
// u32, bool, &str, String — `f64` does not implement `IntoBound`, so it
// cannot be bound. There is no raw-text interpolation path.

use bsql_postgres_core::fragment;

fn main() {
    let _ = fragment!("x = {}", 3.14f64);
}
