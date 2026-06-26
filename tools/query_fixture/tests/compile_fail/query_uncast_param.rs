// A bare `$1` in the projection has no inferable type — it must carry an
// explicit cast. The inference engine's ParamCastRequired is surfaced as a
// compile_error.
fn main() {
    bsql_query_macros::query!(Row, "SELECT $1 FROM users");
}
