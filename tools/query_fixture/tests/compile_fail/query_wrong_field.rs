// A field that the typed record does NOT have is a normal E0609, on top of
// the schema-typed record `query!` emits. `Row` has only `id`.
bsql_query_macros::query!(Row, "SELECT id FROM users");

fn take(r: Row) -> i64 {
    r.nope
}

fn main() {}
