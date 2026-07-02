// The runtime ORDER BY selector is a CLOSED set: the generated enum has
// ONLY the declared `(column, direction)` variants. Naming an ordering
// outside the allow-set (`TotalDesc`, never declared) is `error[E0599]` —
// the ordering is unrepresentable, so there is no runtime SQL-building or
// injection surface.
bsql::query!(Sorted, "SELECT id FROM orders ORDER BY { id ASC | id DESC }");

fn main() {
    let _ = SortedOrderBy::TotalDesc.prepared();
}
