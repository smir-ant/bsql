// THE DROPPED PRECEDENCE FOOTGUN = E0599. There is intentionally no
// `or_where` on the builder: a flat `.and_where(B).or_where(C)` would emit
// `WHERE A AND B OR C`, which SQL parses as `(A AND B) OR C`, not the
// call-order grouping. `OR` is expressed only via `Predicate::or`, which
// self-parenthesises.

use bsql_postgres_core::fragment::{Chunk, ColPredicate, Fragment};

bsql_postgres_core::columns! {
    users => [ id: i32, name: Text, age: i16, active: bool ]
}

fn main() {
    let f = Fragment::__from_chunks(vec![Chunk::Rodata("SELECT 1")]);
    let _ = f.or_where(users::id.eq(1i32));
}
