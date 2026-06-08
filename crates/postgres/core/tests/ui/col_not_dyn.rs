// THE RUNTIME CARRIER MUST BE AN ENUM. `Col: Copy` forces `Self: Sized`,
// so `Col` is not dyn-compatible and `&dyn Col` is E0038. `DynCol` is the
// only legal runtime carrier — not a stylistic choice.

use bsql_postgres_core::col::Col;

bsql_postgres_core::columns! { t => [ id: i32 ] }

fn take_dyn(_c: &dyn Col) {}

fn main() {
    take_dyn(&t::id);
}
