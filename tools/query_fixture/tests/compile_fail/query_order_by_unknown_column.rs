// A runtime ORDER BY allow-set whose option names a column that does not
// exist on the relation is a build error — every ordering is
// inference-validated against the migration-replayed schema, so an
// ordering "outside the allow-set" of real columns cannot compile.
fn main() {
    bsql_query_macros::query!(Row, "SELECT id FROM orders ORDER BY { nonexistent ASC }");
}
