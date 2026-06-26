// `users` exists but has no `nope` column — the inference engine's error
// is surfaced as a compile_error at the SQL literal.
fn main() {
    bsql_query_macros::query!(Row, "SELECT nope FROM users");
}
