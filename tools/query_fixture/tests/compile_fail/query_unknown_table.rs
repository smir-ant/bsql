// No migration defines a `widgets` table — the inference engine's error
// is surfaced as a compile_error at the SQL literal.
fn main() {
    bsql_query_macros::query!(Row, "SELECT id FROM widgets");
}
