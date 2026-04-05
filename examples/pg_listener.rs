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

use bsql::{BsqlError, Listener};

fn main() -> Result<(), BsqlError> {
    let url = "postgres://user:pass@localhost/mydb";

    // ---------------------------------------------------------------
    // Set up a listener on a dedicated connection
    // ---------------------------------------------------------------
    let mut listener = Listener::connect(url)?;

    // Subscribe to one or more channels.
    listener.listen("cache_invalidation")?;
    listener.listen("job_complete")?;
    println!("Listening on: cache_invalidation, job_complete");

    // ---------------------------------------------------------------
    // Send notifications from a background thread
    // ---------------------------------------------------------------
    // In production, notifications come from other processes or DB triggers.
    // Here we spawn a thread to demonstrate the full round-trip.
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Each Listener::connect() opens a separate connection.
        let notifier = Listener::connect("postgres://user:pass@localhost/mydb")
            .expect("connect for notify");

        // notify() sends a payload on a channel.
        notifier
            .notify("cache_invalidation", "users:42")
            .expect("notify");
        notifier
            .notify("job_complete", r#"{"job_id": 7, "status": "ok"}"#)
            .expect("notify");
        println!("Sent 2 notifications.");

        // Signal the listener to stop (for this example only).
        notifier
            .notify("cache_invalidation", "STOP")
            .expect("notify stop");
    });

    // ---------------------------------------------------------------
    // Receive notifications — recv() blocks until one arrives
    // ---------------------------------------------------------------
    loop {
        let notification = listener.recv()?;
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
    listener.unlisten_all()?;

    Ok(())
}
