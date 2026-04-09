//! Async-specific integration tests.
//!
//! Verifies that the async API (Pool::connect().await, tokio::spawn concurrency,
//! transaction commit/rollback .await, fetch_stream, and Listener) works
//! correctly under a tokio runtime.
//!
//! Requires a running PostgreSQL with the test schema.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::{Listener, Pool};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const DB_URL: &str = "postgres://bsql:bsql@localhost/bsql_test";

async fn pool() -> Pool {
    Pool::connect(DB_URL)
        .await
        .expect("Failed to connect to test database. Is PostgreSQL running?")
}

/// Generate a unique channel name to prevent cross-test interference.
fn unique_channel(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "async_{}_{}",
        prefix,
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

// ---------------------------------------------------------------------------
// T-1: Pool::connect().await works
// ---------------------------------------------------------------------------

#[tokio::test]
async fn async_pool_connect() {
    let pool = Pool::connect(DB_URL).await;
    assert!(pool.is_ok(), "Pool::connect().await should succeed");

    let pool = pool.unwrap();
    let status = pool.status();
    assert!(status.max_size > 0, "pool should have positive max_size");

    // Verify the pool is usable by running a query.
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.id, 1);
    assert_eq!(user.login, "alice");
}

// ---------------------------------------------------------------------------
// T-2: Concurrent queries via tokio::spawn — no data corruption
// ---------------------------------------------------------------------------

#[tokio::test]
async fn async_concurrent_queries() {
    let pool = Arc::new(pool().await);

    let mut handles = Vec::new();

    // Spawn 5 tasks, each doing 10 queries concurrently via the same pool.
    for task_id in 0..5u32 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            for query_idx in 0..10u32 {
                let id = 1i32;
                let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
                    .fetch_one(pool.as_ref())
                    .await;

                match user {
                    Ok(user) => {
                        assert_eq!(user.id, 1, "task {task_id} query {query_idx}: id mismatch");
                        assert_eq!(
                            user.login, "alice",
                            "task {task_id} query {query_idx}: login mismatch"
                        );
                    }
                    Err(bsql::BsqlError::Pool(_)) => {
                        // Pool exhaustion is acceptable under high concurrency
                        // with a small default pool. Skip this iteration.
                    }
                    Err(e) => panic!("task {task_id} query {query_idx}: unexpected error: {e}"),
                }
            }
            task_id
        }));
    }

    // All 5 tasks must complete without panicking.
    for handle in handles {
        let task_id = handle.await.expect("task panicked");
        assert!(task_id < 5);
    }

    // After all concurrent queries, the pool should still be usable.
    let id = 2i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(pool.as_ref())
        .await
        .unwrap();
    assert_eq!(user.id, 2);
    assert_eq!(user.login, "bob");
}

// ---------------------------------------------------------------------------
// T-3: Transaction commit with .await — data persists
// ---------------------------------------------------------------------------

#[tokio::test]
async fn async_transaction_commit_await() {
    let pool = pool().await;

    // Begin a transaction.
    let mut tx = pool.begin().await.unwrap();

    // Insert a row via defer (the recommended async pattern).
    let title = "async_commit_test";
    let uid = 1i32;
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&mut tx)
    .await
    .unwrap();
    let ticket_id = ticket.id;

    // Commit the transaction.
    tx.commit().await.unwrap();

    // Verify data persisted outside the transaction.
    let found = bsql::query!("SELECT id, title FROM tickets WHERE id = $ticket_id: i32")
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(
        found.is_some(),
        "committed row should persist after tx.commit().await"
    );
    assert_eq!(found.unwrap().title, "async_commit_test");

    // Clean up.
    bsql::query!("DELETE FROM tickets WHERE id = $ticket_id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// T-4: Transaction rollback with .await — data NOT persisted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn async_transaction_rollback_await() {
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();

    let title = "async_rollback_test";
    let uid = 1i32;
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&mut tx)
    .await
    .unwrap();
    let ticket_id = ticket.id;

    // Rollback the transaction.
    tx.rollback().await.unwrap();

    // Verify data did NOT persist.
    let found = bsql::query!("SELECT id FROM tickets WHERE id = $ticket_id: i32")
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(
        found.is_none(),
        "rolled-back row should NOT persist after tx.rollback().await"
    );
}

// ---------------------------------------------------------------------------
// T-5: fetch_stream with .await loop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn async_fetch_stream() {
    let pool = pool().await;

    let mut stream = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_stream(&pool)
        .await
        .unwrap();

    let mut rows = Vec::new();
    while let Some(user) = stream.next().await.unwrap() {
        rows.push((user.id, user.login.clone()));
    }

    assert!(
        rows.len() >= 2,
        "expected at least 2 users, got {}",
        rows.len()
    );
    assert_eq!(rows[0].1, "alice");
    assert_eq!(rows[1].1, "bob");
}

// ---------------------------------------------------------------------------
// T-6: Listener connect/listen/notify/recv all with .await
// ---------------------------------------------------------------------------

#[tokio::test]
async fn async_listener_recv() {
    let ch = unique_channel("listener_test");

    // Connect the listener.
    let mut listener = Listener::connect(DB_URL).await.unwrap();

    // Subscribe to a channel.
    listener.listen(&ch).await.unwrap();

    // Send a notification.
    listener.notify(&ch, "async_hello").await.unwrap();

    // Receive the notification.
    let notif = listener.recv().await.unwrap();
    assert_eq!(notif.channel(), ch);
    assert_eq!(notif.payload(), "async_hello");
}
