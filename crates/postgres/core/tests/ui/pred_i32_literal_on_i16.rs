// NO SILENT WIDENING = E0308. `users::age` is an `i16` column; an `i32`
// literal is a compile error (rustc suggests changing the literal type) —
// the guard never silently widens a numeric value.

use bsql_postgres_core::fragment::ColPredicate;

bsql_postgres_core::columns! {
    users => [ id: i32, name: Text, age: i16, active: bool ]
}

fn main() {
    let _ = users::age.gt(18i32);
}
