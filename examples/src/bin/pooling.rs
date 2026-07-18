//! # pooling — a connection pool, diagnostics, and reconnect-vs-retry
//!
//! What a real service uses: a connection POOL with structured DIAGNOSTICS (a
//! dep-free callback — forward it to your logging/metrics stack), a bounded
//! connection lifecycle (`max_lifetime` / `idle_timeout`), and the load-bearing
//! reconnect pattern built on `DriverError::is_disconnect()` — which draws the
//! EXACT line between "the connection DIED mid-query, get a FRESH one" and "the
//! server rejected the query but the connection is FINE, surface it".
//!
//! Features/verbs: `Pool::builder(..).on_diagnostic(..).slow_query_threshold(..)
//! .max_lifetime(..).idle_timeout(..).build()`, `pool.get()`, `DriverError::
//! is_disconnect()`, `pool.stats()`, `pool.close()`.
//!
//! Backend: PostgreSQL — needs a live server.
//! ```bash
//! export BSQL_EXAMPLES_DSN='postgres://USER@127.0.0.1:5432/postgres'
//! cargo run -p bsql-examples --bin pooling
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "example/teaching code: unwrap/expect/panic surface failure loudly and keep the code readable"
)]
#![forbid(unsafe_code)]

use core::time::Duration;

use bsql::pg::{ConnectConfig, DiagEvent, DriverError, Pool};

bsql::query!(UserByEmail, "SELECT id, name FROM users WHERE email = $1");

/// Look up a user, RETRYING ONCE on a dead connection. This is the reconnect
/// pattern: `is_disconnect()` is `true` only when the connection itself died
/// (a dropped socket, a vanished peer), so we drop it and retry on a fresh
/// checkout; a genuine per-query error propagates unchanged.
async fn find_user(pool: &Pool, email: &str) -> Result<Option<(i64, Option<String>)>, DriverError> {
    let mut pooled = pool.get().await?;
    let first = pooled.conn_mut()?.query_opt::<UserByEmail>((email,)).await;
    let found = match first {
        Ok(found) => found,
        Err(err) if err.is_disconnect() => {
            // Connection died mid-query -> drop it (the pool evicts it) and retry.
            drop(pooled);
            let mut fresh = pool.get().await?;
            fresh.conn_mut()?.query_opt::<UserByEmail>((email,)).await?
        }
        // A per-query error the connection SURVIVED -> surface it, no retry.
        Err(err) => return Err(err),
    };
    Ok(found.map(|user| (user.id, user.name)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ConnectConfig::from_dsn(&bsql_examples::dsn())?;

    // Build the pool. Diagnostics are ONE dep-free callback (`DiagEvent` is
    // `#[non_exhaustive]`, so the catch-all arm is required); the lifecycle
    // knobs default to `None` (disabled) and are shown here for completeness.
    let pool = Pool::builder(config, 8)
        .on_diagnostic(|event: &DiagEvent<'_>| match event {
            DiagEvent::SlowQuery { sql, elapsed } => eprintln!("[bsql] slow {elapsed:?}: {sql}"),
            DiagEvent::PoolAcquireTimeout { waited } => {
                eprintln!("[bsql] pool exhausted; waited {waited:?}");
            }
            other => eprintln!("[bsql] {other:?}"),
        })
        .slow_query_threshold(Duration::from_millis(100))
        .max_lifetime(Some(Duration::from_secs(1800))) // rotate connections > 30 min old
        .idle_timeout(Some(Duration::from_secs(300))) // shed connections idle > 5 min
        .build();

    // Set up the schema + one row on a checked-out connection.
    {
        let mut pooled = pool.get().await?;
        let conn = pooled.conn_mut()?;
        bsql_examples::ensure_schema_async(conn).await?;
        conn.execute_params(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING",
            &(1i64, "alice@example.com", Some("Alice")),
        )
        .await?;
    }

    // A read through the pool, with the reconnect-vs-retry pattern.
    match find_user(&pool, "alice@example.com").await? {
        Some((id, name)) => println!("found user #{id}: {name:?}"),
        None => println!("no such user"),
    }

    // A cheap operational gauge for a metrics scrape.
    let stats = pool.stats();
    println!(
        "pool stats: idle={} max={} acquire_timeouts={} evicted={}",
        stats.idle, stats.max_size, stats.acquire_timeouts, stats.connections_evicted
    );

    // Graceful shutdown: a clean protocol `Terminate` to every idle backend
    // (bounded, best-effort) instead of dropping sockets bare.
    pool.close().await;
    Ok(())
}
