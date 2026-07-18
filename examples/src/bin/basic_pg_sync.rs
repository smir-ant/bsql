//! # basic_pg_sync — the same CRUD against PostgreSQL, blocking driver
//!
//! Identical to `basic_pg_async` with the `.await`s removed — the point is to
//! SHOW the async/sync symmetry. bsql's async and sync PostgreSQL drivers are
//! generated from ONE transport-generic engine, so the verb surface is the same;
//! only `.await` vs blocking differs. Use the blocking driver (`bsql::pg_sync`)
//! from a plain `fn main`, a thread pool, or a synchronous codebase.
//!
//! Features/verbs: `bsql::pg_sync::{ConnectConfig, Connection}`, `query!`, the
//! typed `query` / `query_one` verbs, the dynamic `execute_params` verb.
//!
//! Backend: PostgreSQL — needs a live server.
//! ```bash
//! export BSQL_EXAMPLES_DSN='postgres://USER@127.0.0.1:5432/postgres'
//! cargo run -p bsql-examples --bin basic_pg_sync
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::manual_unwrap_or,
    clippy::manual_unwrap_or_default,
    reason = "example/teaching code: unwrap/expect/panic read clearly, and the manual match on an Option is the form the workspace disallowed-methods ledger requires (the unwrap_or family is banned)"
)]
#![forbid(unsafe_code)]

use bsql::pg_sync::{ConnectConfig, Connection};

// The SAME `query!` carriers again — one schema, one query surface, three
// backends (async PG, sync PG, SQLite).
bsql::query!(AllUsers, "SELECT id, email, name FROM users ORDER BY id");
bsql::query!(UserById, "SELECT id, email, name FROM users WHERE id = $1");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ConnectConfig::from_dsn(&bsql_examples::dsn())?;
    // No `.await` — this blocks the calling thread until the connection is ready.
    let mut conn = Connection::connect(&config)?;

    bsql_examples::ensure_schema_sync(&mut conn)?;

    conn.execute_params(
        "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        &(1i64, "alice@example.com", Some("Alice")),
    )?;
    conn.execute_params(
        "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        &(2i64, "bob@example.com", Option::<&str>::None),
    )?;

    println!("all users:");
    let users = conn.query::<AllUsers>(())?;
    for row in users.iter() {
        let user = row?;
        let name = match user.name {
            Some(display) => display,
            None => "(no name)",
        };
        println!("  #{}: {} — {name}", user.id, user.email);
    }

    let alice = conn.query_one::<UserById>((1i64,))?;
    println!("looked up #{}: {}", alice.id, alice.email);

    conn.close()?;
    Ok(())
}
