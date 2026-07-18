//! # basic_pg_async — the same CRUD against PostgreSQL, async (tokio)
//!
//! The PostgreSQL-async twin of `basic_sqlite`: connect, ensure the schema
//! exists (by running the migrations — idempotent), insert, and read back
//! through the compile-checked `query!` flagship. Compare with `basic_pg_sync`
//! to see the async/sync symmetry (identical code minus `.await`).
//!
//! Features/verbs: `bsql::pg::{ConnectConfig, Connection}`, `query!`, the typed
//! `query` / `query_one` verbs, the dynamic `execute_params` verb, the migration
//! runner (via `bsql_examples::ensure_schema_async`).
//!
//! Backend: PostgreSQL — needs a live server. Point `BSQL_EXAMPLES_DSN` at it:
//! ```bash
//! export BSQL_EXAMPLES_DSN='postgres://USER@127.0.0.1:5432/postgres'
//! cargo run -p bsql-examples --bin basic_pg_async
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

use bsql::pg::{ConnectConfig, Connection};

// The SAME `query!` carriers as `basic_sqlite` — typed against the same catalog.
bsql::query!(AllUsers, "SELECT id, email, name FROM users ORDER BY id");
bsql::query!(UserById, "SELECT id, email, name FROM users WHERE id = $1");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `from_dsn` parses a libpq-style URL and fails LOUD on a bad one. The DSN
    // comes from the environment (a clear panic if `BSQL_EXAMPLES_DSN` is unset).
    let config = ConnectConfig::from_dsn(&bsql_examples::dsn())?;
    let mut conn = Connection::connect(&config).await?;

    // Ensure our tables exist by APPLYING the migrations. The runner records each
    // in a `_bsql_migrations` ledger, so this is idempotent — a re-run is a no-op.
    // (See the `migrations` example for the runner in depth.)
    bsql_examples::ensure_schema_async(&mut conn).await?;

    // Insert two users with the DYNAMIC parameterized verb. `ON CONFLICT DO
    // NOTHING` keeps the example idempotent across re-runs. Parameters bind in
    // one uniform BINARY format (no injection surface, no text/binary drift).
    conn.execute_params(
        "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        &(1i64, "alice@example.com", Some("Alice")),
    )
    .await?;
    conn.execute_params(
        "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
        &(2i64, "bob@example.com", Option::<&str>::None), // NULL name
    )
    .await?;

    // Read them back through the TYPED flagship — byte-identical shape to the
    // SQLite example: `id: i64`, `email: &str`, `name: Option<&str>`.
    println!("all users:");
    let users = conn.query::<AllUsers>(()).await?;
    for row in users.iter() {
        let user = row?;
        let name = match user.name {
            Some(display) => display,
            None => "(no name)",
        };
        println!("  #{}: {} — {name}", user.id, user.email);
    }

    // A single-row lookup by key.
    let alice = conn.query_one::<UserById>((1i64,)).await?;
    println!("looked up #{}: {}", alice.id, alice.email);

    // Send a protocol `Terminate` so the server sees a clean disconnect.
    conn.close().await?;
    Ok(())
}
