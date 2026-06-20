// `users` exists but has no `nope` column — must be a compile_error.
fn main() {
    bsql_query_macros::schema_check!(users.nope);
}
