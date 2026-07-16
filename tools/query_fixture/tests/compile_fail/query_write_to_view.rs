// A `query!` write (INSERT/UPDATE/DELETE ... RETURNING) targeting a VIEW is a
// loud `WriteToView` compile error: a view is generally not writable, so
// accepting the write at build time would be a build-passes / run-fails gap the
// compile-time guarantee exists to close. `vaccount_summary` is a view over the
// base table `vaccount` (`0022_views.sql`); the fix is to write the base table.
fn main() {
    bsql::query!(
        Row,
        "INSERT INTO vaccount_summary (id, balance) VALUES ($1, $2) RETURNING id"
    );
}
