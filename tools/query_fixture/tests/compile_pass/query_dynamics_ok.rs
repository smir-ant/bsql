// Every valid dynamic form compiles: a toggled `OPTIONAL(...)` filter, a
// `= ANY($N)` array in-list, and a runtime `ORDER BY` allow-set. (The
// behavioural assertions live in `tests/query_dynamics.rs`; this pins that
// the macro EXPANSION type-checks.)
bsql_query_macros::query!(OptUser, "SELECT id, email FROM users WHERE OPTIONAL(id = $1)");
bsql_query_macros::query!(AnyOrders, "SELECT id FROM orders WHERE id = ANY($1)");
bsql_query_macros::query!(
    Sorted,
    "SELECT id, total FROM orders WHERE user_id = $1 ORDER BY { id ASC | total DESC }"
);

fn main() {
    // Touch the generated artifacts so the expansion is fully exercised.
    let _ = OptUserQuery::PREPARED.param_oids();
    let _ = AnyOrdersQuery::PREPARED.param_oids();
    let _ = SortedOrderBy::IdAsc.prepared().stmt_name();
}
