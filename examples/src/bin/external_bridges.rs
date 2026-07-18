//! # external_bridges — decode columns into external crate types
//!
//! By default a `uuid` / `timestamptz` column decodes into bsql's own
//! dependency-free `bsql::Uuid` / `bsql::Timestamptz`. A build-time BRIDGE makes
//! `query!` decode them directly into the REAL `uuid::Uuid` /
//! `chrono::DateTime<Utc>` instead — with bsql depending on and forcing NOTHING.
//! The bridge target type and an INFALLIBLE converter free function travel to the
//! macro as STRINGS (see `build.rs` + the converters in `src/lib.rs`), so
//! `bsql-build` / the proc-macro gain no dependency. The free-fn form is the
//! orphan-proof seam: you cannot `impl bsql::Cell for chrono::DateTime` (both
//! foreign — E0117), but a free fn compiles for any target.
//!
//! Features/verbs: `Catalog::…bridge(pg_type, target, converter).emit_catalog()`
//! (in `build.rs`), `query!` decoding bridged columns.
//!
//! Backend: PostgreSQL — `uuid` / `timestamptz` are PostgreSQL types. Uses a
//! session TEMP shadow of `events`.
//! ```bash
//! export BSQL_EXAMPLES_DSN='postgres://USER@127.0.0.1:5432/postgres'
//! cargo run -p bsql-examples --bin external_bridges
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "example/teaching code: unwrap/expect/panic surface failure loudly and keep the code readable"
)]
#![forbid(unsafe_code)]

use bsql::pg::{ConnectConfig, Connection};

// `id` decodes into `uuid::Uuid` and `occurred_at` into `chrono::DateTime<Utc>`
// — the REAL external crates, via the bridges registered in `build.rs`.
bsql::query!(RecentEvents, "SELECT id, name, occurred_at FROM events ORDER BY occurred_at DESC");
bsql::query!(EventByName, "SELECT id, name, occurred_at FROM events WHERE name = $1");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::connect(&ConnectConfig::from_dsn(&bsql_examples::dsn())?).await?;

    // A TEMP shadow of `events` (uuid PK + timestamptz), seeded with literals.
    conn.execute_raw(
        "CREATE TEMP TABLE events (id UUID PRIMARY KEY, name TEXT NOT NULL, \
         occurred_at TIMESTAMPTZ NOT NULL)",
    )
    .await?;
    conn.execute_raw(
        "INSERT INTO events (id, name, occurred_at) VALUES \
         ('550e8400-e29b-41d4-a716-446655440000', 'launch', '2026-01-15T10:30:00Z'), \
         ('7c9e6679-7425-40de-944b-e07fc1f90ae7', 'update', '2026-02-01T14:00:00Z')",
    )
    .await?;

    println!("recent events:");
    for row in conn.query::<RecentEvents>(()).await?.iter() {
        let event = row?;
        // `event.id` is a `uuid::Uuid`; `event.occurred_at` is a
        // `chrono::DateTime<Utc>` — decoded straight into those crates' types.
        let id: uuid::Uuid = event.id;
        let at: chrono::DateTime<chrono::Utc> = event.occurred_at;
        println!("  {} — {} @ {}", id, event.name, at.to_rfc3339());
    }

    // A by-name lookup, returning the owned record with the bridged types.
    let launch = conn.query_one::<EventByName>(("launch",)).await?;
    println!(
        "\nthe launch event {} happened at {}",
        launch.id,
        launch.occurred_at.to_rfc3339()
    );

    conn.close().await?;
    Ok(())
}
