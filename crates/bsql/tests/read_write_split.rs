//! Integration tests: read/write splitting.
//!
//! With bsql-driver, read/write splitting is not yet implemented at the driver
//! level. These tests verify that all queries work on a primary-only pool.
//!
//! Requires a running PostgreSQL with the test schema.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::Pool;

async fn pool() -> Pool {
    Pool::connect("postgres://bsql:bsql@localhost/bsql_test")
        .await
        .expect("Failed to connect to test database. Is PostgreSQL running?")
}

/// SELECT queries work on a primary-only pool.
#[tokio::test]
async fn select_works_without_replicas() {
    let pool = pool().await;

    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(users.len() >= 2);
}

/// INSERT works on a primary-only pool.
#[tokio::test]
async fn insert_uses_primary() {
    let pool = pool().await;
    let title = "rw-split-test";
    let uid = 1i32;
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(ticket.id > 0);

    // Clean up
    let ticket_id = ticket.id;
    bsql::query!("DELETE FROM tickets WHERE id = $ticket_id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

/// UPDATE works on primary.
#[tokio::test]
async fn update_uses_primary() {
    let pool = pool().await;
    let desc = "rw-split-update";
    let id = 1i32;
    let affected = bsql::query!("UPDATE tickets SET description = $desc: &str WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(affected, 1);
}

/// DELETE works on primary.
#[tokio::test]
async fn delete_uses_primary() {
    let pool = pool().await;
    let title = "rw-split-delete";
    let uid = 1i32;
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let ticket_id = ticket.id;
    let affected = bsql::query!("DELETE FROM tickets WHERE id = $ticket_id: i32")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(affected, 1);
}

/// Transaction queries work.
#[tokio::test]
async fn transaction_uses_primary() {
    let pool = pool().await;

    let txn = pool.begin().await.unwrap();

    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&txn)
        .await
        .unwrap();
    assert!(users.len() >= 2);

    txn.rollback().await.unwrap();
}

/// PoolConnection queries work.
#[tokio::test]
async fn pool_connection_uses_primary() {
    let pool = pool().await;
    let conn = pool.acquire().await.unwrap();

    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&conn)
        .await
        .unwrap();
    assert!(users.len() >= 2);
}

/// Pool status reflects connection counts.
#[tokio::test]
async fn pool_status_reports_metrics() {
    let pool = pool().await;
    let status = pool.status();
    assert!(status.max_size > 0, "max_size should be positive");
}
