// Not a valid SQL statement — rejected at parse time, before any catalog
// or SQLite lookup.
fn main() {
    bsql_query_macros::query!(Row, "SELECT id FROM");
}
