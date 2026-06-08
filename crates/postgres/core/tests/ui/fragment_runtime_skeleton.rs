// THE MACRO WALL: the `fragment!` skeleton must be a string LITERAL. A
// runtime `String` (or any non-literal first argument) is a compile error
// — there is no runtime-string -> SQL skeleton path even through the
// macro front-end.

use bsql_postgres_core::fragment;

fn main() {
    let runtime = String::from("DROP TABLE users WHERE id = {}");
    let _ = fragment!(runtime, 1i32);
}
