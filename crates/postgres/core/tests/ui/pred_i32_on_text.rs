// TYPED GUARD = E0308. `users::name` is a `Text` column whose value type is
// `&str`; binding an `i32` is a compile error.

use bsql_postgres_core::fragment::ColPredicate;

bsql_postgres_core::columns! {
    users => [ id: i32, name: Text, age: i16, active: bool ]
}

fn main() {
    let _ = users::name.eq(5i32);
}
