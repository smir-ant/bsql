// NO Fragment::from_str. There is no runtime-string -> SQL skeleton path:
// a `Fragment` is constructed only via the `fragment!` macro (literal
// skeleton). `Fragment::from_str(&runtime)` is E0599 (no such function).

use bsql_postgres_core::Fragment;

fn main() {
    let r = String::from("DROP TABLE users");
    let _ = Fragment::from_str(&r);
}
