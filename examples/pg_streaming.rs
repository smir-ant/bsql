//! Streaming large result sets from PostgreSQL.
//!
//! Demonstrates `fetch_stream` for row-by-row processing without loading
//! the entire result set into memory.
//!
//! Uses PostgreSQL's portal / extended query protocol to stream rows in
//! batches of 64. Each row is decoded and processed before the next batch
//! is fetched. Memory usage stays constant regardless of result set size.
//!
//! Use `.fetch()` for normal queries. Use `.fetch_stream()` when you have
//! millions of rows and cannot afford to hold them all in memory at once.
//!
//! ## Setup
//!
//! ```sql
//! CREATE TABLE events (
//!     id         SERIAL PRIMARY KEY,
//!     kind       TEXT NOT NULL,
//!     payload    TEXT,
//!     created_at TIMESTAMPTZ NOT NULL DEFAULT now()
//! );
//! -- Insert some test data:
//! INSERT INTO events (kind, payload)
//!     SELECT 'user_signup', 'user_' || i
//!     FROM generate_series(1, 10000) AS i;
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb
//! cargo run --bin pg_streaming
//! ```

use bsql::{BsqlError, Pool};

fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb")?;

    // ---------------------------------------------------------------
    // Stream all rows — constant memory regardless of table size
    // ---------------------------------------------------------------
    // fetch_stream returns a Stream that yields rows one at a time.
    // Use advance() to step forward, then next_row() to access the row.
    let mut stream = bsql::query!(
        "SELECT id, kind, payload FROM events ORDER BY id"
    )
    .fetch_stream(&pool)?;

    let mut count = 0u64;
    while stream.advance()? {
        let event = stream.next_row()?;
        count += 1;

        // Process each row without accumulating. In a real app, you
        // might write to a file, send to a queue, or update a cache.
        if count <= 5 {
            println!(
                "Event {}: kind={}, payload={:?}",
                event.id, event.kind, event.payload
            );
        }
    }
    println!("Streamed {count} total events.");

    // ---------------------------------------------------------------
    // Streaming with filters — same parameterized query syntax
    // ---------------------------------------------------------------
    let kind = "user_signup";
    let mut stream = bsql::query!(
        "SELECT id, kind, payload FROM events WHERE kind = $kind: &str ORDER BY id"
    )
    .fetch_stream(&pool)?;

    let mut signup_count = 0u64;
    while stream.advance()? {
        let event = stream.next_row()?;
        signup_count += 1;

        if signup_count <= 3 {
            println!("Signup event {}: {:?}", event.id, event.payload);
        }
    }
    println!("Found {signup_count} signup events.");

    Ok(())
}
