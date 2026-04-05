//! Batch INSERT with transaction pipelining.
//!
//! Demonstrates `.defer()` — the fastest way to insert many rows.
//! All INSERTs are buffered and sent in one network round-trip on commit.
//!
//! ## Setup
//!
//! ```sql
//! CREATE TABLE users (
//!     id    SERIAL PRIMARY KEY,
//!     name  TEXT NOT NULL,
//!     email TEXT NOT NULL
//! );
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb
//! cargo run --bin pg_batch_insert
//! ```

use bsql::{BsqlError, Pool};

fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb")?;

    // Sample data — imagine hundreds or thousands of rows.
    let users = vec![
        ("alice", "alice@example.com"),
        ("bob", "bob@example.com"),
        ("charlie", "charlie@example.com"),
    ];

    // ---------------------------------------------------------------
    // Batch INSERT with .defer() — one round-trip for N inserts
    // ---------------------------------------------------------------
    let tx = pool.begin()?;

    // .defer() buffers each INSERT — no network I/O yet.
    for (name, email) in &users {
        bsql::query!("INSERT INTO users (name, email) VALUES ($name: &str, $email: &str)")
            .defer(&tx)?;
    }

    // commit() sends ALL buffered INSERTs in one pipeline round-trip.
    // 3 inserts = 1 round-trip, not 3.
    tx.commit()?;

    println!("Inserted {} users in one round-trip", users.len());

    // ---------------------------------------------------------------
    // Verify the inserts
    // ---------------------------------------------------------------
    let rows = bsql::query!("SELECT id, name, email FROM users")
        .fetch(&pool)?;

    for row in &rows {
        println!("  id={}, name={}, email={}", row.id, row.name, row.email);
    }

    println!("Total users: {}", rows.len());

    Ok(())
}
