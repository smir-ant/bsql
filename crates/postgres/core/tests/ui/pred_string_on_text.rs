// TYPED GUARD = E0308. A `Text` column's value type is `&str` (a borrowed
// view), not an owned `String` — binding a `String` is a compile error
// (rustc suggests borrowing).

use bsql_postgres_core::fragment::ColPredicate;

bsql_postgres_core::columns! {
    users => [ id: i32, name: Text, age: i16, active: bool ]
}

fn main() {
    let _ = users::name.eq(String::from("a"));
}
