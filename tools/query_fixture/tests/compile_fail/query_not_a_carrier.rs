// The single most common `query!` misuse: passing the generated RECORD type
// (`User`) where a runnable CARRIER (`UserQuery`) is required. `query!(User, ..)`
// emits `User` — a decoded-row struct that holds values but CANNOT be run — and
// `UserQuery`, the carrier the driver's `query` / `query_one` / `query_opt` verbs
// take. Passing the record is a `TypedQuery` unsatisfied-bound error; the
// `#[diagnostic::on_unimplemented]` on `TypedQuery` names the exact fix (use the
// `…Query` carrier) instead of a raw trait-bound wall.
use bsql_postgres_sync::Connection;

bsql::query!(User, "SELECT id FROM users");

fn run(conn: &mut Connection) {
    // Mistake: `User` is the record, not the carrier `UserQuery`.
    let _ = conn.query::<User>(());
}

fn main() {}
