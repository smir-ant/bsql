// `Row.id` is `i64` (the `int8` PK); returning it where a `bool` is
// expected is an E0308 type mismatch against the typed record.
bsql_query_macros::query!(Row, "SELECT id FROM users");

fn take(r: Row) -> bool {
    r.id
}

fn main() {}
