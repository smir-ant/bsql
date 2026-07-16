// Every valid dynamic form compiles: a toggled `OPTIONAL(...)` filter, a
// `= ANY($N)` array in-list, and a runtime `ORDER BY` allow-set. (The
// behavioural assertions live in `tests/query_dynamics.rs`; this pins that
// the macro EXPANSION type-checks.)
bsql::query!(OptUser, "SELECT id, email FROM users WHERE OPTIONAL(id = $1)");
bsql::query!(AnyOrders, "SELECT id FROM orders WHERE id = ANY($1)");
bsql::query!(
    Sorted,
    "SELECT id, total FROM orders WHERE user_id = $1 ORDER BY { id ASC | total DESC }"
);

fn main() {
    // Touch the generated artifacts so the expansion is fully exercised.
    let _ = OptUser::PREPARED.param_oids();
    let _ = AnyOrders::PREPARED.param_oids();
    let _ = SortedOrderBy::IdAsc.prepared().stmt_name();
}
