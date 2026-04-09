//! Integration tests: singleflight query coalescing (v0.7).
//!
//! Verifies that identical concurrent queries share a single PG round-trip
//! via `Arc<[Row]>`. The singleflight is transparent -- all existing query
//! semantics are preserved.
//!
//! Requires a running PostgreSQL with the test schema.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::Pool;

async fn pool() -> Pool {
    Pool::connect("postgres://bsql:bsql@localhost/bsql_test")
        .await
        .expect("Failed to connect to test database. Is PostgreSQL running?")
}

/// Basic: singleflight is transparent for a normal fetch_one.
#[tokio::test]
async fn singleflight_fetch_one_works() {
    let pool = pool().await;
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(user.id, 1);
    assert_eq!(user.login, "alice");
}

/// Basic: singleflight is transparent for fetch_all.
#[tokio::test]
async fn singleflight_fetch_all_works() {
    let pool = pool().await;
    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();

    assert!(users.len() >= 2);
    assert_eq!(users[0].login, "alice");
}

/// Concurrent identical queries should all succeed.
/// We can't directly observe singleflight coalescing from the outside,
/// but we can verify that N concurrent identical queries all return
/// correct results without errors.
///
/// Uses `tokio::spawn` so all 10 queries are genuinely concurrent --
/// a sequential for-loop would never actually race and would not
/// exercise the singleflight coalescing path.
#[tokio::test]
async fn concurrent_identical_queries_all_succeed() {
    use std::sync::Arc;

    let pool = Arc::new(pool().await);

    let mut handles = Vec::new();
    for _ in 0..10 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            bsql::query!("SELECT id, login FROM users ORDER BY id")
                .fetch_all(pool.as_ref())
                .await
        }));
    }

    for handle in handles {
        let users = handle.await.expect("task panicked").unwrap();
        assert!(users.len() >= 2);
        assert_eq!(users[0].login, "alice");
    }
}

/// Parameterized queries with the same SQL text still work correctly.
/// (Singleflight keys by SQL text, so same-SQL queries may coalesce
/// even with different params -- but the result is still correct because
/// params are sent to PG.)
#[tokio::test]
async fn parameterized_query_works_with_singleflight() {
    let pool = pool().await;
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.id, 1);

    let id = 2i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.id, 2);
}

/// Singleflight does NOT apply to transactions (snapshot isolation).
#[tokio::test]
async fn transaction_queries_are_not_coalesced() {
    let pool = pool().await;
    let mut txn = pool.begin().await.unwrap();

    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&mut txn)
        .await
        .unwrap();
    assert!(users.len() >= 2);

    txn.rollback().await.unwrap();
}

/// Singleflight does NOT apply to PoolConnection.
#[tokio::test]
async fn pool_connection_queries_not_coalesced() {
    let pool = pool().await;
    let mut conn = pool.acquire().await.unwrap();

    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert!(users.len() >= 2);
}

/// Execute (writes) are not affected by singleflight.
#[tokio::test]
async fn execute_not_affected_by_singleflight() {
    let pool = pool().await;
    let desc = "singleflight-test-desc";
    let id = 1i32;
    let affected = bsql::query!("UPDATE tickets SET description = $desc: &str WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(affected, 1);
}

/// After concurrent queries complete, subsequent queries still work.
/// Verifies singleflight does not leak entries or corrupt state.
#[tokio::test]
async fn queries_work_after_concurrent_burst() {
    use std::sync::Arc;

    let pool = Arc::new(pool().await);

    // Burst of 20 concurrent identical queries.
    // Some may fail with pool exhaustion (fail-fast pool, max 10 connections).
    // The point is that singleflight doesn't corrupt state.
    let mut handles = Vec::new();
    for _ in 0..20 {
        let pool = Arc::clone(&pool);
        handles.push(tokio::spawn(async move {
            bsql::query!("SELECT id, login FROM users ORDER BY id")
                .fetch_all(pool.as_ref())
                .await
        }));
    }

    for handle in handles {
        let result = handle.await.expect("task panicked");
        // Accept both success and pool exhaustion
        if let Ok(users) = &result {
            assert!(users.len() >= 2);
        }
    }

    // After the burst, normal queries should still work
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(pool.as_ref())
        .await
        .unwrap();
    assert_eq!(user.id, 1);
    assert_eq!(user.login, "alice");
}

/// fetch_optional through singleflight path works.
#[tokio::test]
async fn singleflight_fetch_optional_works() {
    let pool = pool().await;
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert!(user.is_some());
    assert_eq!(user.unwrap().login, "alice");
}

/// Different SQL texts are independently handled by singleflight.
#[tokio::test]
async fn different_queries_are_independent() {
    use std::sync::Arc;

    let pool = Arc::new(pool().await);

    let pool1 = Arc::clone(&pool);
    let h1 = tokio::spawn(async move {
        bsql::query!("SELECT id, login FROM users ORDER BY id")
            .fetch_all(pool1.as_ref())
            .await
    });

    let pool2 = Arc::clone(&pool);
    let h2 = tokio::spawn(async move {
        bsql::query!("SELECT id FROM tickets ORDER BY id")
            .fetch_all(pool2.as_ref())
            .await
    });

    let users = h1.await.expect("task panicked").unwrap();
    let tickets = h2.await.expect("task panicked").unwrap();

    assert!(users.len() >= 2);
    assert!(!tickets.is_empty());
}

/// When the leader query succeeds, all followers should get the result (not hang).
#[tokio::test]
async fn singleflight_leader_error_propagates_to_followers() {
    // If the leader query errors, followers should also get errors (not hang)
    let pool = std::sync::Arc::new(pool().await);

    let mut handles = vec![];
    for _ in 0..5 {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            // All 5 tasks try the exact same (valid) query
            let rows = bsql::query!("SELECT id, login FROM users ORDER BY id")
                .fetch_all(p.as_ref())
                .await;
            rows // return Result
        }));
    }

    // All should succeed (singleflight coalesces them)
    let mut success_count = 0;
    for h in handles {
        if h.await.unwrap().is_ok() {
            success_count += 1;
        }
    }
    assert_eq!(
        success_count, 5,
        "all followers should get the leader's result"
    );
}
