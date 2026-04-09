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
    let mut tx = pool.begin().await.unwrap();
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&mut tx)
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

    let mut tx = pool.begin().await.unwrap();
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&mut tx)
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

        let mut tx = pool.begin().await.unwrap();
        let ticket = bsql::query!(
            "INSERT INTO tickets (title, status, created_by_user_id)
             VALUES ($title: &str, 'new', $uid: i32)
             RETURNING id"
        )
        .fetch_one(&mut tx)
        .await
        .unwrap();
        ticket_id = ticket.id;
        // tx dropped here -- connection discarded, insert not committed
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

    let mut tx = pool.begin().await.unwrap();

    // Insert two tickets in the same transaction.
    let title1 = "tx_multi_1";
    let title2 = "tx_multi_2";
    let uid = 1i32;

    let t1 = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title1: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&mut tx)
    .await
    .unwrap();

    let t2 = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title2: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&mut tx)
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

    let mut tx = pool.begin().await.unwrap();

    // Insert a valid row.
    let title = "tx_error_test";
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

    // Now cause an error: reference a non-existent user (FK violation).
    let bad_title = "tx_error_fk";
    let bad_uid = 999999i32;
    let result = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($bad_title: &str, 'new', $bad_uid: i32)
         RETURNING id"
    )
    .fetch_one(&mut tx)
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

    let mut tx = pool.begin().await.unwrap();

    let title = "tx_read_write_test";
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

    // Read the row back within the same transaction.
    let found = bsql::query!("SELECT id, title FROM tickets WHERE id = $ticket_id: i32")
        .fetch_one(&mut tx)
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

    let mut tx1 = pool.begin().await.unwrap();
    let mut tx2 = pool.begin().await.unwrap();

    // Insert in tx1 only.
    let title = "tx_isolated_test";
    let uid = 1i32;
    bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)"
    )
    .execute(&mut tx1)
    .await
    .unwrap();

    // tx2 should NOT see the uncommitted row (default READ COMMITTED isolation).
    let search = "tx_isolated_test";
    let seen = bsql::query!("SELECT id FROM tickets WHERE title = $search: &str")
        .fetch_all(&mut tx2)
        .await
        .unwrap();
    assert!(seen.is_empty(), "tx2 should not see tx1's uncommitted row");

    tx1.rollback().await.unwrap();
    tx2.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// lazy BEGIN -- transaction without queries never sends BEGIN
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

    // Begin and immediately commit -- no queries executed.
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
        // Drop without any queries -- BEGIN was never sent.
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
    let mut tx = pool.begin().await.unwrap();

    // First query inside tx triggers lazy BEGIN, then runs the query.
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&mut tx)
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
    let mut tx = pool.begin().await.unwrap();

    let desc = "tx_execute_test";
    let id = 1i32;
    let affected = bsql::query!("UPDATE tickets SET description = $desc: &str WHERE id = $id: i32")
        .execute(&mut tx)
        .await
        .unwrap();
    assert_eq!(affected, 1);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn transaction_execute_insert_and_delete() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();

    let title = "tx_insert_delete";
    let uid = 1i32;
    let inserted = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)"
    )
    .execute(&mut tx)
    .await
    .unwrap();
    assert_eq!(inserted, 1);

    let title2 = "tx_insert_delete";
    let deleted = bsql::query!("DELETE FROM tickets WHERE title = $title2: &str")
        .execute(&mut tx)
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn transaction_execute_returns_zero_for_no_match() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();

    let id = 999999i32;
    let affected = bsql::query!("UPDATE tickets SET description = 'x' WHERE id = $id: i32")
        .execute(&mut tx)
        .await
        .unwrap();
    assert_eq!(affected, 0);

    tx.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// deferred pipeline (defer_execute / flush_deferred / auto-flush)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_defer_execute_commit() {
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();

    let title = "defer_commit_bsql";
    let uid = 1i32;
    let sql = "INSERT INTO tickets (title, status, created_by_user_id) VALUES ($1, 'new', $2)";
    let hash = bsql_driver_postgres::hash_sql(sql);
    let params: &[&(dyn bsql_driver_postgres::Encode + Sync)] = &[&title, &uid];

    tx.defer_execute(sql, hash, params).await.unwrap();
    tx.defer_execute(sql, hash, params).await.unwrap();
    assert_eq!(tx.deferred_count(), 2);

    tx.commit().await.unwrap();

    // Verify rows were inserted (use existing cached query by id pattern)
    let search = "defer_commit_bsql";
    let rows = bsql::query!("SELECT id FROM tickets WHERE title = $search: &str")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    // Clean up -- delete by each id
    for row in &rows {
        let id = row.id;
        bsql::query!("DELETE FROM tickets WHERE id = $id: i32")
            .execute(&pool)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn transaction_defer_execute_flush_returns_counts() {
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();

    let title = "defer_flush_bsql";
    let uid = 1i32;
    let sql = "INSERT INTO tickets (title, status, created_by_user_id) VALUES ($1, 'new', $2)";
    let hash = bsql_driver_postgres::hash_sql(sql);
    let params: &[&(dyn bsql_driver_postgres::Encode + Sync)] = &[&title, &uid];

    tx.defer_execute(sql, hash, params).await.unwrap();
    tx.defer_execute(sql, hash, params).await.unwrap();

    let results = tx.flush_deferred().await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], 1);
    assert_eq!(results[1], 1);
    assert_eq!(tx.deferred_count(), 0);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn transaction_defer_execute_auto_flushes_before_read() {
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();

    let title = "defer_autoflush_bsql";
    let uid = 1i32;
    let sql = "INSERT INTO tickets (title, status, created_by_user_id) VALUES ($1, 'new', $2)";
    let hash = bsql_driver_postgres::hash_sql(sql);
    let params: &[&(dyn bsql_driver_postgres::Encode + Sync)] = &[&title, &uid];

    tx.defer_execute(sql, hash, params).await.unwrap();
    assert_eq!(tx.deferred_count(), 1);

    // SELECT triggers auto-flush, so we can read-your-writes
    let search = "defer_autoflush_bsql";
    let rows = bsql::query!("SELECT id FROM tickets WHERE title = $search: &str")
        .fetch_all(&mut tx)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(tx.deferred_count(), 0);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn transaction_defer_execute_rollback_discards() {
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();

    let title = "defer_rollback_bsql";
    let uid = 1i32;
    let sql = "INSERT INTO tickets (title, status, created_by_user_id) VALUES ($1, 'new', $2)";
    let hash = bsql_driver_postgres::hash_sql(sql);
    let params: &[&(dyn bsql_driver_postgres::Encode + Sync)] = &[&title, &uid];

    tx.defer_execute(sql, hash, params).await.unwrap();
    tx.rollback().await.unwrap();

    // Nothing should have been inserted
    let search = "defer_rollback_bsql";
    let found = bsql::query!("SELECT id FROM tickets WHERE title = $search: &str")
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(found.is_none());
}

#[tokio::test]
async fn transaction_defer_execute_empty_flush_is_noop() {
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();
    let results = tx.flush_deferred().await.unwrap();
    assert!(results.is_empty());
    assert_eq!(tx.deferred_count(), 0);
    tx.commit().await.unwrap();
}

// ---------------------------------------------------------------------------
// savepoints
// ---------------------------------------------------------------------------

#[tokio::test]
async fn savepoint_and_release() {
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();

    tx.savepoint("sp1").await.unwrap();

    let title = "sp_release_test";
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

    tx.release_savepoint("sp1").await.unwrap();
    tx.commit().await.unwrap();

    // Row should exist after commit because the savepoint was released (kept).
    let found = bsql::query!("SELECT id FROM tickets WHERE id = $ticket_id: i32")
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(
        found.is_some(),
        "released savepoint row should persist after commit"
    );

    // Clean up.
    bsql::query!("DELETE FROM tickets WHERE id = $ticket_id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn savepoint_and_rollback_to() {
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();

    tx.savepoint("sp_rb").await.unwrap();

    let title = "sp_rollback_test";
    let uid = 1i32;
    bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)"
    )
    .execute(&mut tx)
    .await
    .unwrap();

    // Rollback to savepoint — the insert should be undone.
    tx.rollback_to("sp_rb").await.unwrap();

    // Verify the row does NOT exist within the transaction.
    let search = "sp_rollback_test";
    let found = bsql::query!("SELECT id FROM tickets WHERE title = $search: &str")
        .fetch_all(&mut tx)
        .await
        .unwrap();
    assert!(
        found.is_empty(),
        "rolled-back savepoint insert should not be visible"
    );

    tx.commit().await.unwrap();

    // Also verify outside the transaction.
    let found = bsql::query!("SELECT id FROM tickets WHERE title = $search: &str")
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(
        found.is_none(),
        "rolled-back savepoint row should not persist"
    );
}

#[tokio::test]
async fn nested_savepoints() {
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();

    // Savepoint A: insert row A.
    tx.savepoint("sp_a").await.unwrap();
    let title_a = "nested_sp_a";
    let uid = 1i32;
    bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title_a: &str, 'new', $uid: i32)"
    )
    .execute(&mut tx)
    .await
    .unwrap();

    // Savepoint B: insert row B.
    tx.savepoint("sp_b").await.unwrap();
    let title_b = "nested_sp_b";
    bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title_b: &str, 'new', $uid: i32)"
    )
    .execute(&mut tx)
    .await
    .unwrap();

    // Rollback to B — only B's insert is undone.
    tx.rollback_to("sp_b").await.unwrap();

    // A's insert should still be visible.
    let search_a = "nested_sp_a";
    let found_a = bsql::query!("SELECT id FROM tickets WHERE title = $search_a: &str")
        .fetch_all(&mut tx)
        .await
        .unwrap();
    assert_eq!(
        found_a.len(),
        1,
        "savepoint A insert should survive rollback to B"
    );

    // B's insert should be gone.
    let search_b = "nested_sp_b";
    let found_b = bsql::query!("SELECT id FROM tickets WHERE title = $search_b: &str")
        .fetch_all(&mut tx)
        .await
        .unwrap();
    assert!(
        found_b.is_empty(),
        "savepoint B insert should be rolled back"
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn rollback_to_nonexistent_savepoint_errors() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();

    // Force the transaction to be active (lazy BEGIN) with a harmless query.
    let id = 1i32;
    let _ = bsql::query!("SELECT id FROM users WHERE id = $id: i32")
        .fetch_one(&mut tx)
        .await
        .unwrap();

    let result = tx.rollback_to("never_created_sp").await;
    assert!(
        result.is_err(),
        "rollback to nonexistent savepoint should error"
    );

    // Transaction is now in aborted state; drop it.
    drop(tx);
}

#[tokio::test]
async fn savepoint_invalid_name_rejected() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();

    // Empty name.
    let result = tx.savepoint("").await;
    assert!(result.is_err(), "empty savepoint name should be rejected");

    // Special characters.
    let result = tx.savepoint("sp;drop").await;
    assert!(
        result.is_err(),
        "savepoint name with special chars should be rejected"
    );

    let result = tx.savepoint("sp name").await;
    assert!(
        result.is_err(),
        "savepoint name with space should be rejected"
    );

    tx.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// isolation levels
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_isolation_serializable() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();

    tx.set_isolation(bsql::IsolationLevel::Serializable)
        .await
        .unwrap();

    // Run a query to prove the transaction works with SERIALIZABLE.
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&mut tx)
        .await
        .unwrap();
    assert_eq!(user.id, 1);

    tx.commit().await.unwrap();
}

#[tokio::test]
async fn set_isolation_read_committed() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();

    tx.set_isolation(bsql::IsolationLevel::ReadCommitted)
        .await
        .unwrap();

    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&mut tx)
        .await
        .unwrap();
    assert_eq!(user.id, 1);

    tx.commit().await.unwrap();
}

// ---------------------------------------------------------------------------
// deferred pipeline edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flush_empty_deferred_queue() {
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();

    // Force the transaction active with a read.
    let id = 1i32;
    let _ = bsql::query!("SELECT id FROM users WHERE id = $id: i32")
        .fetch_one(&mut tx)
        .await
        .unwrap();

    // Flush with nothing deferred — should be a no-op.
    let results = tx.flush_deferred().await.unwrap();
    assert!(results.is_empty());
    assert_eq!(tx.deferred_count(), 0);

    tx.commit().await.unwrap();
}

#[tokio::test]
async fn multiple_flush_calls() {
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();

    let title = "multi_flush_test";
    let uid = 1i32;
    let sql = "INSERT INTO tickets (title, status, created_by_user_id) VALUES ($1, 'new', $2)";
    let hash = bsql_driver_postgres::hash_sql(sql);
    let params: &[&(dyn bsql_driver_postgres::Encode + Sync)] = &[&title, &uid];

    // First batch: defer 3, flush.
    tx.defer_execute(sql, hash, params).await.unwrap();
    tx.defer_execute(sql, hash, params).await.unwrap();
    tx.defer_execute(sql, hash, params).await.unwrap();
    assert_eq!(tx.deferred_count(), 3);

    let results1 = tx.flush_deferred().await.unwrap();
    assert_eq!(results1.len(), 3);
    assert_eq!(tx.deferred_count(), 0);

    // Second batch: defer 3 more, flush.
    tx.defer_execute(sql, hash, params).await.unwrap();
    tx.defer_execute(sql, hash, params).await.unwrap();
    tx.defer_execute(sql, hash, params).await.unwrap();
    assert_eq!(tx.deferred_count(), 3);

    let results2 = tx.flush_deferred().await.unwrap();
    assert_eq!(results2.len(), 3);
    assert_eq!(tx.deferred_count(), 0);

    // All 6 rows should exist.
    let search = "multi_flush_test";
    let rows = bsql::query!("SELECT id FROM tickets WHERE title = $search: &str")
        .fetch_all(&mut tx)
        .await
        .unwrap();
    assert_eq!(rows.len(), 6);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn deferred_count_tracks_correctly() {
    let pool = pool().await;

    let mut tx = pool.begin().await.unwrap();

    let title = "defer_count_test";
    let uid = 1i32;
    let sql = "INSERT INTO tickets (title, status, created_by_user_id) VALUES ($1, 'new', $2)";
    let hash = bsql_driver_postgres::hash_sql(sql);
    let params: &[&(dyn bsql_driver_postgres::Encode + Sync)] = &[&title, &uid];

    assert_eq!(tx.deferred_count(), 0);

    for expected in 1..=5 {
        tx.defer_execute(sql, hash, params).await.unwrap();
        assert_eq!(tx.deferred_count(), expected);
    }

    tx.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// transaction error recovery — aborted state
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transaction_error_aborts_state() {
    // After a PG error inside a transaction, the tx is in aborted state.
    // All subsequent queries should fail until rollback.
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();

    // This should succeed
    let uid = 1i32;
    bsql::query!("SELECT id FROM users WHERE id = $uid: i32")
        .fetch_one(&mut tx)
        .await
        .unwrap();

    // This should fail (FK violation: user 999999 doesn't exist)
    let bad_title = "tx_aborted_state_test";
    let bad_uid = 999999i32;
    let result = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($bad_title: &str, 'new', $bad_uid: i32)
         RETURNING id"
    )
    .fetch_one(&mut tx)
    .await;
    assert!(result.is_err());

    // After error, further queries should also fail (aborted state)
    let uid2 = 1i32;
    let result2 = bsql::query!("SELECT id FROM users WHERE id = $uid2: i32")
        .fetch_one(&mut tx)
        .await;
    // PG returns "current transaction is aborted"
    assert!(result2.is_err());

    tx.rollback().await.unwrap();
}
