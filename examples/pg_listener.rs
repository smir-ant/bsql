//! PostgreSQL LISTEN/NOTIFY with bsql.
//!
//! Demonstrates: real-time notifications using PostgreSQL's LISTEN/NOTIFY.
//! One task sends notifications, another listens. Useful for cache
//! invalidation, job queues, real-time updates.
//!
//! Requires a running PostgreSQL instance. No tables needed --
//! LISTEN/NOTIFY operates on channels, not tables.
//!
//! Run:
//!   BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --bin pg_listener

use bsql::{BsqlError, Listener, Pool};

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    let url = "postgres://user:pass@localhost/mydb";

    // --- Set up a listener ---
    // Listener uses a dedicated connection (not from the pool) because
    // it holds the connection open for the lifetime of the subscription.
    let mut listener = Listener::connect(url).await?;

    // Subscribe to one or more channels.
    listener.listen("cache_invalidation").await?;
    listener.listen("job_complete").await?;
    println!("Listening on: cache_invalidation, job_complete");

    // --- Send notifications from another connection ---
    // In a real application, these would come from other processes or
    // from triggers. Here we use a pool connection to demonstrate.
    let pool = Pool::connect(url).await?;

    // Spawn a task that sends notifications after a short delay.
    let notify_pool = pool;
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // notify() sends a payload on a channel. Any listener subscribed
        // to that channel receives it.
        let notify_listener = Listener::connect("postgres://user:pass@localhost/mydb")
            .await
            .expect("connect for notify");
        notify_listener
            .notify("cache_invalidation", "users:42")
            .await
            .expect("notify");
        notify_listener
            .notify("job_complete", r#"{"job_id": 7, "status": "ok"}"#)
            .await
            .expect("notify");

        println!("Sent 2 notifications.");

        // Signal the listener to stop.
        notify_listener
            .notify("cache_invalidation", "STOP")
            .await
            .expect("notify stop");
    });

    // --- Receive notifications ---
    // recv() blocks until a notification arrives. In production, run
    // this in a loop on a dedicated task.
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

    // Clean up subscriptions.
    listener.unlisten_all().await?;

    Ok(())
}
