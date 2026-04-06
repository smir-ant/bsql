//! Real-time LISTEN/NOTIFY with PostgreSQL.
//!
//! Demonstrates:
//!   - `Listener::connect()` for a dedicated notification connection
//!   - `listener.listen()` to subscribe to channels
//!   - `listener.recv()` to receive notifications (blocks until one arrives)
//!   - `listener.notify()` to send notifications
//!
//! LISTEN/NOTIFY is useful for cache invalidation, job queues, and real-time
//! updates. The listener uses a dedicated connection (not from the pool)
//! because subscriptions are tied to the PostgreSQL backend process.
//!
//! On connection loss, the listener automatically reconnects with exponential
//! backoff and re-subscribes to all channels.
//!
//! ## Setup
//!
//! No tables needed. LISTEN/NOTIFY operates on channels, not tables.
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb
//! cargo run --bin pg_listener
//! ```

use bsql::{BsqlError, Listener, Pool};

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    let url = "postgres://user:pass@localhost/mydb";

    // ---------------------------------------------------------------
    // Set up a listener on a dedicated connection
    // ---------------------------------------------------------------
    let mut listener = Listener::connect(url).await?;

    // Subscribe to one or more channels.
    listener.listen("cache_invalidation").await?;
    listener.listen("job_complete").await?;
    println!("Listening on: cache_invalidation, job_complete");

    // ---------------------------------------------------------------
    // Send notifications from a background task
    // ---------------------------------------------------------------
    // In production, notifications come from other processes or DB triggers.
    // Here we use a pool with raw_execute to send NOTIFY from a spawned task.
    // (Listener is !Sync due to mpsc::Receiver, so we use Pool for the sender.)
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let pool = Pool::connect("postgres://user:pass@localhost/mydb")
            .await
            .expect("connect for notify");

        pool.raw_execute("NOTIFY cache_invalidation, 'users:42'")
            .await
            .expect("notify");
        pool.raw_execute("NOTIFY job_complete, '{\"job_id\": 7, \"status\": \"ok\"}'")
            .await
            .expect("notify");
        println!("Sent 2 notifications.");

        // Signal the listener to stop (for this example only).
        pool.raw_execute("NOTIFY cache_invalidation, 'STOP'")
            .await
            .expect("notify stop");
    });

    // ---------------------------------------------------------------
    // Receive notifications — recv() blocks until one arrives
    // ---------------------------------------------------------------
    loop {
        let notification = listener.recv().await?;
        println!(
            "Received on '{}': {}",
            notification.channel(),
            notification.payload()
        );

        // Exit condition for this example.
        if notification.payload() == "STOP" {
            println!("Stop signal received, exiting.");
            break;
        }
    }

    // Clean up all subscriptions.
    listener.unlisten_all().await?;

    Ok(())
}
