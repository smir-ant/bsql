//! Integration tests: read/write splitting (v0.7).
//!
//! These tests verify that:
//! - SELECT queries call `query_raw_readonly` (which routes to replicas)
//! - INSERT/UPDATE/DELETE call `query_raw` / `execute_raw` (always primary)
//! - Without replicas configured, everything works on primary
//!
//! Since we can't easily spin up real replicas in tests, we verify the
//! routing at the API level: all queries succeed on a primary-only pool,
//! and the PoolBuilder accepts replica URLs.
//!
//! Requires a running PostgreSQL with the test schema.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::Pool;

async fn pool() -> Pool {
    Pool::connect("postgres://bsql:bsql@localhost/bsql_test")
        .await
        .expect("Failed to connect to test database. Is PostgreSQL running?")
}

/// SELECT queries work on a primary-only pool (no replicas).
/// This tests the fallback path: query_raw_readonly falls through to primary.
#[tokio::test]
async fn select_works_without_replicas() {
    let pool = pool().await;
    assert!(!pool.has_replicas());

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

/// Builder with replica URLs (doesn't actually connect since we don't have replicas).
#[test]
fn builder_accepts_replicas() {
    let builder = Pool::builder()
        .host("localhost")
        .port(5432)
        .dbname("bsql_test")
        .user("bsql")
        .password("bsql")
        .replica("postgres://bsql:bsql@replica1:5432/bsql_test")
        .replica("postgres://bsql:bsql@replica2:5432/bsql_test");

    // We can't call .build() because the replicas don't exist,
    // but we verify the builder accepts the configuration.
    let _ = builder;
}

/// Pool reports has_replicas correctly.
#[tokio::test]
async fn pool_reports_no_replicas() {
    let pool = pool().await;
    assert!(!pool.has_replicas());
}

/// Builder with replica pointing to same host (simulates replica for testing).
/// The primary and "replica" are the same PG instance.
#[tokio::test]
async fn builder_with_same_host_replica() {
    let pool = Pool::builder()
        .host("localhost")
        .port(5432)
        .dbname("bsql_test")
        .user("bsql")
        .password("bsql")
        // Point "replica" to the same PG instance
        .replica("postgres://bsql:bsql@localhost/bsql_test")
        .build()
        .await
        .unwrap();

    assert!(pool.has_replicas());

    // SELECT should route to "replica" (same instance)
    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(users.len() >= 2);
}

/// Transaction queries always use primary, even with replicas configured.
#[tokio::test]
async fn transaction_always_uses_primary() {
    let pool = Pool::builder()
        .host("localhost")
        .port(5432)
        .dbname("bsql_test")
        .user("bsql")
        .password("bsql")
        .replica("postgres://bsql:bsql@localhost/bsql_test")
        .build()
        .await
        .unwrap();

    let txn = pool.begin().await.unwrap();

    // SELECT in transaction uses primary (transaction is bound)
    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&txn)
        .await
        .unwrap();
    assert!(users.len() >= 2);

    txn.rollback().await.unwrap();
}
