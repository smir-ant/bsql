// `vaccount_summary` is a VIEW that projects only `id` and `balance` — NOT
// `label`, though the base table `vaccount` has it (`0022_views.sql`). A
// `query!` naming a column the view does not expose is a loud `UnknownColumn`,
// which is exactly the drift guarantee: if a later `CREATE OR REPLACE VIEW`
// drops a column, code naming it stops compiling.
fn main() {
    bsql::query!(Row, "SELECT label FROM vaccount_summary");
}
