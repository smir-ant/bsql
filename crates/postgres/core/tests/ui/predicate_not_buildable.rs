// A PREDICATE IS NOT A STATEMENT = E0599. A `Predicate` (a boolean
// expression) cannot be `build()`-assembled on its own — only a `Fragment`
// can. A predicate must be handed to `Fragment::and_where`.

use bsql_postgres_core::fragment::ColPredicate;

bsql_postgres_core::columns! {
    users => [ id: i32, name: Text, age: i16, active: bool ]
}

fn main() {
    let _ = users::id.eq(1i32).build();
}
