// OUT-OF-RANGE LITERAL. An unsuffixed literal that infers to the column's
// value type (`i16`) but does not fit is rejected by `overflowing_literals`
// (deny-by-default), so a value that cannot fit the column type fails the
// build.

use bsql_postgres_core::fragment::ColPredicate;

bsql_postgres_core::columns! {
    users => [ id: i32, name: Text, age: i16, active: bool ]
}

fn main() {
    let _ = users::age.gt(40000);
}
