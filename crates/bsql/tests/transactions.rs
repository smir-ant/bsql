//! Integration tests for v0.5: transactions.
//!
//! Requires a running PostgreSQL with the test schema.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::{BsqlError, Pool};

async fn pool() -> Pool {
    Pool::connect("postgres://bsql:bsql@localhost/bsql_test")
        .await
        .expect("Failed to connect to test database. Is PostgreSQL running?")
}

// ---------------------------------------------------------------------------
// commit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_commit_persists() {
    let pool = pool().await;

    let title = "tx_commit_test";
    let uid = 1i32;

    // Insert inside a transaction, then commit.
    let tx = pool.begin().await.unwrap();
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&tx)
    .await
    .unwrap();
    let ticket_id = ticket.id;
    tx.commit().await.unwrap();

    // Verify the row exists outside the transaction.
    let found = bsql::query!("SELECT id FROM tickets WHERE id = $ticket_id: i32")
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(found.is_some(), "committed row should persist");

    // Clean up.
    bsql::query!("DELETE FROM tickets WHERE id = $ticket_id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// explicit rollback
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_rollback_discards() {
    let pool = pool().await;

    let title = "tx_rollback_test";
    let uid = 1i32;

    let tx = pool.begin().await.unwrap();
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&tx)
    .await
    .unwrap();
    let ticket_id = ticket.id;
    tx.rollback().await.unwrap();

    // Verify the row does NOT exist.
    let found = bsql::query!("SELECT id FROM tickets WHERE id = $ticket_id: i32")
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(found.is_none(), "rolled-back row should not persist");
}

// ---------------------------------------------------------------------------
// drop without commit (implicit discard)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_drop_without_commit_discards() {
    let pool = pool().await;
    let ticket_id: i32;

    {
        let title = "tx_drop_test";
        let uid = 1i32;

        let tx = pool.begin().await.unwrap();
        let ticket = bsql::query!(
            "INSERT INTO tickets (title, status, created_by_user_id)
             VALUES ($title: &str, 'new', $uid: i32)
             RETURNING id"
        )
        .fetch_one(&tx)
        .await
        .unwrap();
        ticket_id = ticket.id;
        // tx dropped here — connection discarded, insert not committed
    }

    // Verify the row does NOT exist.
    let found = bsql::query!("SELECT id FROM tickets WHERE id = $ticket_id: i32")
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(found.is_none(), "dropped-tx row should not persist");
}

// ---------------------------------------------------------------------------
// multiple queries in one transaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_multiple_queries() {
    let pool = pool().await;

    let tx = pool.begin().await.unwrap();

    // Insert two tickets in the same transaction.
    let title1 = "tx_multi_1";
    let title2 = "tx_multi_2";
    let uid = 1i32;

    let t1 = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title1: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&tx)
    .await
    .unwrap();

    let t2 = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title2: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&tx)
    .await
    .unwrap();

    tx.commit().await.unwrap();

    // Both rows should exist.
    let id1 = t1.id;
    let id2 = t2.id;
    let found1 = bsql::query!("SELECT id FROM tickets WHERE id = $id1: i32")
        .fetch_optional(&pool)
        .await
        .unwrap();
    let found2 = bsql::query!("SELECT id FROM tickets WHERE id = $id2: i32")
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(found1.is_some());
    assert!(found2.is_some());

    // Clean up.
    bsql::query!("DELETE FROM tickets WHERE id = $id1: i32")
        .execute(&pool)
        .await
        .unwrap();
    bsql::query!("DELETE FROM tickets WHERE id = $id2: i32")
        .execute(&pool)
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// query error inside transaction rolls back
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_error_rolls_back() {
    let pool = pool().await;

    let tx = pool.begin().await.unwrap();

    // Insert a valid row.
    let title = "tx_error_test";
    let uid = 1i32;
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&tx)
    .await
    .unwrap();
    let ticket_id = ticket.id;

    // Now cause an error: reference a non-existent user (FK violation).
    let bad_title = "tx_error_fk";
    let bad_uid = 999999i32;
    let result = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($bad_title: &str, 'new', $bad_uid: i32)
         RETURNING id"
    )
    .fetch_one(&tx)
    .await;
    assert!(result.is_err());

    // After a PG error, the transaction is in an aborted state.
    // Drop the tx (cannot commit an aborted transaction).
    drop(tx);

    // The first insert should NOT have persisted.
    let found = bsql::query!("SELECT id FROM tickets WHERE id = $ticket_id: i32")
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(found.is_none(), "error in tx should roll back all changes");
}

// ---------------------------------------------------------------------------
// read-your-writes inside transaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_read_your_writes() {
    let pool = pool().await;

    let tx = pool.begin().await.unwrap();

    let title = "tx_read_write_test";
    let uid = 1i32;
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&tx)
    .await
    .unwrap();
    let ticket_id = ticket.id;

    // Read the row back within the same transaction.
    let found = bsql::query!("SELECT id, title FROM tickets WHERE id = $ticket_id: i32")
        .fetch_one(&tx)
        .await
        .unwrap();
    assert_eq!(found.id, ticket_id);
    assert_eq!(found.title, "tx_read_write_test");

    tx.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// begin on pool_exhausted errors immediately (fail-fast)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn begin_on_exhausted_pool_fails_fast() {
    // Create a pool with exactly 1 connection.
    let pool = Pool::builder()
        .url("postgres://bsql:bsql@localhost/bsql_test")
        .max_size(1)
        .build()
        .await
        .unwrap();

    // Hold the one connection via a transaction.
    let _tx = pool.begin().await.unwrap();

    // Second begin() should fail immediately.
    let result = pool.begin().await;
    assert!(result.is_err());
    match result.unwrap_err() {
        BsqlError::Pool(_) => {} // expected
        other => panic!("expected Pool error, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// independent transactions get independent connections
// ---------------------------------------------------------------------------

#[tokio::test]
async fn independent_transactions_are_isolated() {
    let pool = pool().await;

    let tx1 = pool.begin().await.unwrap();
    let tx2 = pool.begin().await.unwrap();

    // Insert in tx1 only.
    let title = "tx_isolated_test";
    let uid = 1i32;
    bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)"
    )
    .execute(&tx1)
    .await
    .unwrap();

    // tx2 should NOT see the uncommitted row (default READ COMMITTED isolation).
    let search = "tx_isolated_test";
    let seen = bsql::query!("SELECT id FROM tickets WHERE title = $search: &str")
        .fetch_all(&tx2)
        .await
        .unwrap();
    assert!(seen.is_empty(), "tx2 should not see tx1's uncommitted row");

    tx1.rollback().await.unwrap();
    tx2.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// lazy BEGIN — transaction without queries never sends BEGIN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_commit_without_queries_is_noop() {
    // Create a pool with exactly 1 connection to prove the connection
    // returns cleanly (if BEGIN were sent without COMMIT, the connection
    // would be dirty and the pool slot lost).
    let pool = Pool::builder()
        .url("postgres://bsql:bsql@localhost/bsql_test")
        .max_size(1)
        .build()
        .await
        .unwrap();

    // Begin and immediately commit — no queries executed.
    // Lazy BEGIN means no BEGIN/COMMIT round-trips sent.
    let tx = pool.begin().await.unwrap();
    tx.commit().await.unwrap();

    // The single connection should be back in the pool, usable.
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.id, 1);
}

#[tokio::test]
async fn transaction_rollback_without_queries_is_noop() {
    let pool = Pool::builder()
        .url("postgres://bsql:bsql@localhost/bsql_test")
        .max_size(1)
        .build()
        .await
        .unwrap();

    let tx = pool.begin().await.unwrap();
    tx.rollback().await.unwrap();

    // Connection should be clean and returned to pool.
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.id, 1);
}

#[tokio::test]
async fn transaction_drop_without_queries_returns_connection_clean() {
    let pool = Pool::builder()
        .url("postgres://bsql:bsql@localhost/bsql_test")
        .max_size(1)
        .build()
        .await
        .unwrap();

    {
        let _tx = pool.begin().await.unwrap();
        // Drop without any queries — BEGIN was never sent.
        // Connection should return to pool CLEAN (not discarded).
    }

    // If the connection was discarded (Object::take), the pool would need
    // to create a new one. With max_size=1, a second acquire proves the
    // connection is still in the pool.
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.id, 1, "connection should be clean and reusable");
}

#[tokio::test]
async fn transaction_lazy_begin_first_query_triggers_begin() {
    let pool = pool().await;
    let tx = pool.begin().await.unwrap();

    // First query inside tx triggers lazy BEGIN, then runs the query.
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&tx)
        .await
        .unwrap();
    assert_eq!(user.id, 1);
    assert_eq!(user.login, "alice");

    tx.commit().await.unwrap();
}

// ---------------------------------------------------------------------------
// transaction debug format
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_debug_format() {
    let pool = pool().await;
    let tx = pool.begin().await.unwrap();

    let debug = format!("{:?}", tx);
    assert!(debug.contains("Transaction"), "debug: {debug}");

    tx.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// execute inside transaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_execute_returns_affected_rows() {
    let pool = pool().await;
    let tx = pool.begin().await.unwrap();

    let desc = "tx_execute_test";
    let id = 1i32;
    let affected = bsql::query!("UPDATE tickets SET description = $desc: &str WHERE id = $id: i32")
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(affected, 1);

    tx.rollback().await.unwrap();
}
