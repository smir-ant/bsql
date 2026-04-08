//! Singleflight — deduplicate identical concurrent queries.
//!
//! When 100 requests hit the same endpoint simultaneously and each runs
//! the same query with the same parameters, bsql executes it once and
//! shares the result. The other 99 requests wait on a condvar (not poll)
//! and receive an `Arc`-shared copy of the result.
//!
//! ## How it works
//!
//! bsql tracks in-flight queries by a key derived from:
//!   - The SQL text hash
//!   - The encoded parameter bytes
//!
//! Same query + same parameters = same key. When a second request arrives
//! with a matching key, it becomes a "follower" and waits for the "leader"
//! to complete. The leader executes the query and broadcasts the result.
//!
//! ## Key details
//!
//! - Only **read queries** are coalesced. Writes always execute independently.
//! - Different parameter values = different keys = no coalescing.
//! - Coalescing is transparent — the API is identical with or without it.
//! - If the leader panics, followers are woken and retry independently.
//!
//! ## Setup
//!
//! ```sql
//! CREATE TABLE config (
//!     key   TEXT PRIMARY KEY,
//!     value TEXT NOT NULL
//! );
//! INSERT INTO config (key, value) VALUES ('theme', 'dark');
//! INSERT INTO config (key, value) VALUES ('locale', 'en-US');
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb
//! cargo run --bin pg_singleflight
//! ```

use bsql::{BsqlError, Pool};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    let pool = Arc::new(
        Pool::connect("postgres://user:pass@localhost/mydb").await?
    );

    // ---------------------------------------------------------------
    // Simulate 100 concurrent requests for the same config value
    // ---------------------------------------------------------------
    // In production, this happens naturally when a popular endpoint
    // queries the same data under load. Without singleflight, all 100
    // requests would each execute the query — 100 round-trips.
    // With singleflight, only ONE query hits PostgreSQL.
    let mut handles = vec![];
    for i in 0..100 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let config = bsql::query!(
                "SELECT value FROM config WHERE key = 'theme'"
            )
            .fetch_one(&*pool).await
            .expect("query failed");

            // All 100 tasks get the same result.
            (i, config.value.to_string())
        }));
    }

    // Collect results — all 100 should have the same value.
    let mut results = Vec::new();
    for h in handles {
        let (i, value) = h.await.unwrap();
        results.push((i, value));
    }

    // Verify all results are identical.
    let first_value = &results[0].1;
    let all_same = results.iter().all(|(_, v)| v == first_value);
    println!(
        "100 concurrent requests, all got '{}': {}",
        first_value,
        if all_same { "YES" } else { "NO (bug!)" }
    );

    // ---------------------------------------------------------------
    // Different parameters = no coalescing
    // ---------------------------------------------------------------
    // These two queries have different parameter values, so they
    // execute independently — no coalescing across different keys.
    let key1 = "theme";
    let key2 = "locale";
    let (theme, locale) = tokio::join!(
        async {
            bsql::query!("SELECT value FROM config WHERE key = $key1: &str")
                .fetch_one(&*pool).await
        },
        async {
            bsql::query!("SELECT value FROM config WHERE key = $key2: &str")
                .fetch_one(&*pool).await
        }
    );
    println!("theme={}, locale={}", theme?.value, locale?.value);

    Ok(())
}
