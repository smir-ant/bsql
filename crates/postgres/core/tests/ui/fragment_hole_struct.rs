// NON-BINDABLE HOLE = E0277. A foreign/local struct does not implement
// `IntoBound`, so it cannot enter a `{}` hole.

use bsql_postgres_core::fragment;

struct Evil;

fn main() {
    let _ = fragment!("x = {}", Evil);
}
