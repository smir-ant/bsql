//! # multi_backend — one compile-checked query, two backends
//!
//! The cross-backend headline: the SAME `query!` carrier, typed ONCE against the
//! migration catalog, RUNS on both SQLite and PostgreSQL and decodes into the
//! SAME typed record. Write a data layer once; run it on either backend. This is
//! the capability that sets bsql apart — the compile-checked query is
//! backend-independent, and each backend verifies the result its own way (PG by
//! wire OID, SQLite by storage class).
//!
//! Features/verbs: one `query!` carrier run through `bsql::sqlite::Connection`
//! AND `bsql::pg::Connection`, both via the identical typed `query` verb.
//!
//! Backend: SQLite (always) + PostgreSQL (if `BSQL_EXAMPLES_DSN` is set).
//! ```bash
//! # SQLite half runs with no database; the PostgreSQL half needs a server:
//! export BSQL_EXAMPLES_DSN='postgres://USER@127.0.0.1:5432/postgres'
//! cargo run -p bsql-examples --bin multi_backend
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

// ONE carrier, typed once. `users` uses only SQLite-portable column types, so
// the macro emits BOTH a PostgreSQL executor and a `SqliteTypedQuery` impl — the
// same `AllUsers` record decodes on either backend. (A carrier touching a
// PostgreSQL-only type — an enum, uuid, timestamptz — would simply not implement
// `SqliteTypedQuery`, so running it on SQLite is a located compile error, never a
// silent mis-run.)
bsql::query!(AllUsers, "SELECT id, email, name FROM users ORDER BY id");

/// Print every user from whichever backend the rows came from. Generic over the
/// row ITEM, not the backend — the decoded record fields are identical.
fn describe<'a>(who: &str, id: i64, email: &'a str, name: Option<&'a str>) {
    let name = match name {
        Some(display) => display,
        None => "(no name)",
    };
    println!("  [{who}] #{id}: {email} — {name}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Backend 1: SQLite, in-memory, no server ──────────────────────────────
    {
        use bsql::sqlite::{Connection, ValueRef};
        let conn = Connection::open_in_memory()?;
        conn.execute_batch_raw(bsql_examples::SQLITE_SCHEMA)?;
        conn.execute_params(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
            &[ValueRef::Integer(1), ValueRef::Text(b"alice@example.com"), ValueRef::Text(b"Alice")],
        )?;

        println!("from SQLite:");
        // The IDENTICAL typed verb + record as the PostgreSQL half below.
        for row in conn.query::<AllUsers>(())?.iter() {
            let user = row?;
            describe("sqlite", user.id, user.email, user.name);
        }
    }

    // ── Backend 2: PostgreSQL, if a DSN is configured ────────────────────────
    match std::env::var(bsql_examples::DSN_ENV) {
        Ok(dsn) => {
            use bsql::pg::{ConnectConfig, Connection};
            let mut conn = Connection::connect(&ConnectConfig::from_dsn(&dsn)?).await?;
            bsql_examples::ensure_schema_async(&mut conn).await?;
            conn.execute_params(
                "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
                &(1i64, "alice@example.com", Some("Alice")),
            )
            .await?;

            println!("from PostgreSQL:");
            // The SAME `AllUsers` carrier, the SAME `.query::<AllUsers>()` verb,
            // the SAME record fields — only the backend differs.
            for row in conn.query::<AllUsers>(()).await?.iter() {
                let user = row?;
                describe("postgres", user.id, user.email, user.name);
            }
            conn.close().await?;
        }
        Err(_) => {
            println!(
                "(skipping the PostgreSQL half — set {} to run it)",
                bsql_examples::DSN_ENV
            );
        }
    }

    Ok(())
}
