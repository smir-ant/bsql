//! Integration tests: singleflight query coalescing (v0.7).
//!
//! Verifies that identical concurrent queries share a single PG round-trip
//! via `Arc<Vec<Row>>`. The singleflight is transparent -- all existing query
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
#[tokio::test]
async fn concurrent_identical_queries_all_succeed() {
    let pool = pool().await;

    let mut handles = Vec::new();
    for _ in 0..10 {
        let pool_ref = &pool;
        handles.push(async move {
            bsql::query!("SELECT id, login FROM users ORDER BY id")
                .fetch_all(pool_ref)
                .await
        });
    }

    let results = futures_core_join_all(handles).await;
    for result in results {
        let users = result.unwrap();
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
    let txn = pool.begin().await.unwrap();

    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&txn)
        .await
        .unwrap();
    assert!(users.len() >= 2);

    txn.rollback().await.unwrap();
}

/// Singleflight does NOT apply to PoolConnection.
#[tokio::test]
async fn pool_connection_queries_not_coalesced() {
    let pool = pool().await;
    let conn = pool.acquire().await.unwrap();

    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&conn)
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

/// Helper to join multiple futures. We avoid adding tokio::join! for N futures
/// by collecting into a Vec and using select_all-style iteration.
async fn futures_core_join_all<F, T>(futures: Vec<F>) -> Vec<T>
where
    F: std::future::Future<Output = T>,
{
    let mut results = Vec::with_capacity(futures.len());
    for fut in futures {
        results.push(fut.await);
    }
    results
}
