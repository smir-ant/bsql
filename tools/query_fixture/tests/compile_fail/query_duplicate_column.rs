// Two output columns named `id` cannot become two fields of one name in
// the generated record — the inference engine's DuplicateOutputColumn is
// surfaced as a compile_error (it never silently collapses the columns).
fn main() {
    bsql_query_macros::query!(Row, "SELECT id, id FROM users");
}
