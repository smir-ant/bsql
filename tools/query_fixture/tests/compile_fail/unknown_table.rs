// No migration defines a `widgets` table — must be a compile_error.
fn main() {
    bsql_query_macros::schema_check!(widgets.id);
}
