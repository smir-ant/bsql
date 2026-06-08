// NO SILENT WIDENING = E0308 (binding form). Even an `i32` *variable*
// (not a literal) is rejected against an `i16` column — rustc suggests an
// explicit fallible `try_into`, never a silent narrowing/widening.

use bsql_postgres_core::fragment::ColPredicate;

bsql_postgres_core::columns! {
    users => [ id: i32, name: Text, age: i16, active: bool ]
}

fn main() {
    let x: i32 = 18;
    let _ = users::age.gt(x);
}
