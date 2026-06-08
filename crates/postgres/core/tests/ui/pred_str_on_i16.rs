// THE MAKE-OR-BREAK GUARD = E0308. `users::age` is an `i16` column; binding
// a `&str` value is a compile error. A wrong-typed predicate value cannot
// unify with `<Self::Ty as ColType>::Value<'v>` (here `i16`).

use bsql_postgres_core::fragment::ColPredicate;

bsql_postgres_core::columns! {
    users => [ id: i32, name: Text, age: i16, active: bool ]
}

fn main() {
    let _ = users::age.gt("oops");
}
