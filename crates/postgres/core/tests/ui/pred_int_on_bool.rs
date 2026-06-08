// TYPED GUARD = E0308. `users::active` is a `bool` column; binding an `i32`
// is a compile error.

use bsql_postgres_core::fragment::ColPredicate;

bsql_postgres_core::columns! {
    users => [ id: i32, name: Text, age: i16, active: bool ]
}

fn main() {
    let _ = users::active.eq(1i32);
}
