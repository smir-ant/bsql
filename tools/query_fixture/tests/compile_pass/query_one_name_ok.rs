// The one-name collapse (GREEN peer of `compile_fail/query_not_a_carrier.rs`):
// a plain `query!(User, "…")` makes the record `User` ITSELF the runnable
// carrier, so `conn.query::<User>(params)` compiles — there is no separate
// `UserQuery` marker to name, and the former "record vs carrier" footgun is
// gone. `query_one` / `query_opt` return the OWNED record `User` (`Send +
// 'static`); the eager `query` returns `Rows<User>` whose `iter()` lends the
// zero-copy borrowed VIEW `UserRef<'q>`, and `into_owned()` yields `Vec<User>`.
use bsql_postgres_sync::Connection;

bsql::query!(User, "SELECT id, email FROM users WHERE id = $1");

fn _run(conn: &mut Connection) {
    // The record IS the carrier — the one-name turbofish (eager `Rows<User>`).
    let _rows = conn.query::<User>((7,));
    // `query_one` / `query_opt` return the OWNED record `User` itself.
    let _one: Result<User, _> = conn.query_one::<User>((7,));
    let _opt: Result<Option<User>, _> = conn.query_opt::<User>((7,));
}

fn main() {}
