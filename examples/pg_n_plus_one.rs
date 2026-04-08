//! Automatic N+1 query detection.
//!
//! Enable the `detect-n-plus-one` feature to catch N+1 patterns at runtime.
//! When the same query fires more than N times sequentially on a single
//! connection checkout, bsql logs a warning via the `log` crate.
//!
//! ## Cargo.toml
//!
//! ```toml
//! [dependencies]
//! bsql = { version = "0.22", features = ["detect-n-plus-one"] }
//! ```
//!
//! ## How it works
//!
//! Each connection checkout tracks the last query hash. When the same hash
//! fires more times than the threshold (default: 10), bsql emits:
//!
//!   `[WARN bsql] potential N+1: sql_hash=0x... repeated 11 times`
//!
//! The detection resets when a different query runs or the connection is
//! returned to the pool. Zero overhead when the feature is disabled.
//!
//! ## Setup
//!
//! ```sql
//! CREATE TABLE users (
//!     id    SERIAL PRIMARY KEY,
//!     login TEXT NOT NULL
//! );
//! CREATE TABLE orders (
//!     id      SERIAL PRIMARY KEY,
//!     user_id INT NOT NULL REFERENCES users(id),
//!     amount  INT NOT NULL
//! );
//! INSERT INTO users (login) VALUES ('alice'), ('bob'), ('charlie'),
//!     ('dave'), ('eve'), ('frank'), ('grace'), ('heidi'), ('ivan'), ('judy');
//! INSERT INTO orders (user_id, amount)
//!     SELECT u.id, (random() * 10000)::int FROM users u, generate_series(1, 3);
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb
//! RUST_LOG=warn cargo run --bin pg_n_plus_one
//! ```

use bsql::{BsqlError, Pool};

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    // Initialize logging so you can see the N+1 warnings.
    // In production, use env_logger, tracing-subscriber, or your preferred logger.
    // Here we just print a note — the warning goes through the `log` crate.
    eprintln!("(Enable RUST_LOG=warn to see N+1 detection warnings)\n");

    // Configure the pool with a custom N+1 threshold.
    // Default threshold is 10. Setting it to 5 means a warning fires
    // after the 5th consecutive identical query on a single checkout.
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    // ---------------------------------------------------------------
    // BAD: Classic N+1 pattern — one query per user in a loop
    // ---------------------------------------------------------------
    // This fetches all users, then runs a separate query for each user's
    // orders. With 10 users, that's 1 + 10 = 11 queries.
    // After the threshold, bsql logs: "potential N+1 detected"
    let users = bsql::query!("SELECT id, login FROM users ORDER BY id LIMIT 10")
        .fetch(&pool).await?;

    println!("Fetching orders per user (N+1 pattern):");
    for user in &users {
        // This query fires once per user — classic N+1.
        // bsql detects the repeated sql_hash and warns.
        let user_id = user.id;
        let orders = bsql::query!(
            "SELECT id, amount FROM orders WHERE user_id = $user_id: i32"
        )
        .fetch(&pool).await?;
        println!("  {}: {} orders", user.login, orders.len());
    }

    // ---------------------------------------------------------------
    // GOOD: Use a JOIN instead — one query for all data
    // ---------------------------------------------------------------
    // A single query replaces the entire loop. No N+1.
    println!("\nFetching with JOIN (no N+1):");
    let joined = bsql::query!(
        "SELECT u.login, COUNT(o.id) AS order_count
         FROM users u
         LEFT JOIN orders o ON u.id = o.user_id
         GROUP BY u.login
         ORDER BY u.login"
    )
    .fetch(&pool).await?;

    for row in &joined {
        println!("  {}: {} orders", row.login, row.order_count);
    }

    Ok(())
}
