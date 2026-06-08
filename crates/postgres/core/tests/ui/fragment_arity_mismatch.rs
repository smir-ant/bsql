// THE MACRO ARITY WALL: every `{}` hole consumes exactly one positional
// argument. A skeleton with two holes but one argument is a compile error
// (no silently-dropped or silently-reused bind).

use bsql_postgres_core::fragment;

fn main() {
    let _ = fragment!("a = {} AND b = {}", 1i32);
}
