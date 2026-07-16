// After the one-name collapse, a PLAIN `query!(Foo, "…")` makes the record `Foo`
// ITSELF the runnable carrier, so `conn.query::<Foo>()` is CORRECT (proven in
// `compile_pass/query_one_name_ok.rs`) — the former "record vs `FooQuery` carrier"
// footgun is now unrepresentable for a plain query.
//
// The residual misuse the `#[diagnostic::on_unimplemented]` on `TypedQuery` still
// guards is a runtime `ORDER BY { ... }` query: its RECORD is NOT a carrier,
// because one record cannot carry N orderings' distinct prepared plans. Each
// ordering is a separate `Foo...Query` carrier, picked via the `FooOrderBy`
// selector — turbofishing the bare record `Foo` is the mistake, and the
// diagnostic names the fix.
use bsql_postgres_sync::Connection;

bsql::query!(SortedUsers, "SELECT id FROM users ORDER BY { id ASC | id DESC }");

fn run(conn: &mut Connection) {
    // Mistake: `SortedUsers` is the record; a runtime-ORDER-BY query is run
    // per-ordering via its `SortedUsers...Query` carriers / the
    // `SortedUsersOrderBy` selector, not by turbofishing the record.
    let _ = conn.query::<SortedUsers>(());
}

fn main() {}
