//! Streaming large result sets from PostgreSQL with bsql.
//!
//! Demonstrates: fetch_stream for row-by-row processing without loading
//! the entire result set into memory.
//!
//! Uses PostgreSQL's portal / extended query protocol to stream rows
//! in batches. Each row is decoded and processed before the next batch
//! is fetched, keeping memory usage constant regardless of result size.
//!
//! Requires a PostgreSQL instance with:
//!   CREATE TABLE events (id SERIAL PRIMARY KEY, kind TEXT NOT NULL, payload TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
//!
//! Run:
//!   BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --bin pg_streaming

use bsql::{BsqlError, Pool};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    // --- Stream rows one by one ---
    // fetch_stream returns a Stream<Item = Result<Row>>. Rows are fetched
    // in batches from PostgreSQL, but yielded one at a time.
    // Memory usage stays constant even for millions of rows.
    let mut stream = bsql::query!(
        "SELECT id, kind, payload FROM events ORDER BY id"
    )
    .fetch_stream(&pool)
    .await?;

    let mut count = 0u64;
    while let Some(event) = stream.next().await {
        let event = event?;
        count += 1;

        // Process each row without accumulating them.
        if count <= 5 {
            println!(
                "Event {}: kind={}, payload={:?}",
                event.id,
                event.kind,
                event.payload
            );
        }
    }
    println!("Streamed {count} total events.");

    // --- Streaming with a filter ---
    // Combine streaming with parameterized queries.
    let kind = "user_signup";
    let mut stream = bsql::query!(
        "SELECT id, kind, payload FROM events WHERE kind = $kind: &str ORDER BY id"
    )
    .fetch_stream(&pool)
    .await?;

    let mut signup_count = 0u64;
    while let Some(event) = stream.next().await {
        let event = event?;
        signup_count += 1;

        // Example: write each row to a file, send to a queue, etc.
        // The point is that you never hold more than one row in memory.
        if signup_count <= 3 {
            println!("Signup event {}: {:?}", event.id, event.payload);
        }
    }
    println!("Found {signup_count} signup events.");

    Ok(())
}
