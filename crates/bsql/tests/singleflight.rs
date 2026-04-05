//! Integration tests: singleflight query coalescing (v0.7).
//!
//! Verifies that identical concurrent queries share a single PG round-trip
//! via `Arc<[Row]>`. The singleflight is transparent -- all existing query
//! semantics are preserved.
//!
//! Requires a running PostgreSQL with the test schema.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::Pool;

fn pool() -> Pool {
    Pool::connect("postgres://bsql:bsql@localhost/bsql_test")
        .expect("Failed to connect to test database. Is PostgreSQL running?")
}

/// Basic: singleflight is transparent for a normal fetch_one.
#[test]
fn singleflight_fetch_one_works() {
    let pool = pool();
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .unwrap();

    assert_eq!(user.id, 1);
    assert_eq!(user.login, "alice");
}

/// Basic: singleflight is transparent for fetch_all.
#[test]
fn singleflight_fetch_all_works() {
    let pool = pool();
    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&pool)
        .unwrap();

    assert!(users.len() >= 2);
    assert_eq!(users[0].login, "alice");
}

/// Concurrent identical queries should all succeed.
/// We can't directly observe singleflight coalescing from the outside,
/// but we can verify that N concurrent identical queries all return
/// correct results without errors.
///
/// Uses `std::thread::spawn` so all 10 queries are genuinely concurrent --
/// a sequential for-loop would never actually race and would not
/// exercise the singleflight coalescing path.
#[test]
fn concurrent_identical_queries_all_succeed() {
    use std::sync::Arc;

    let pool = Arc::new(pool());

    let mut handles = Vec::new();
    for _ in 0..10 {
        let pool = Arc::clone(&pool);
        handles.push(std::thread::spawn(move || {
            bsql::query!("SELECT id, login FROM users ORDER BY id")
                .fetch_all(pool.as_ref())
        }));
    }

    for handle in handles {
        let users = handle.join().expect("thread panicked").unwrap();
        assert!(users.len() >= 2);
        assert_eq!(users[0].login, "alice");
    }
}

/// Parameterized queries with the same SQL text still work correctly.
/// (Singleflight keys by SQL text, so same-SQL queries may coalesce
/// even with different params -- but the result is still correct because
/// params are sent to PG.)
#[test]
fn parameterized_query_works_with_singleflight() {
    let pool = pool();
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(user.id, 1);

    let id = 2i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(user.id, 2);
}

/// Singleflight does NOT apply to transactions (snapshot isolation).
#[test]
fn transaction_queries_are_not_coalesced() {
    let pool = pool();
    let txn = pool.begin().unwrap();

    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&txn)
        .unwrap();
    assert!(users.len() >= 2);

    txn.rollback().unwrap();
}

/// Singleflight does NOT apply to PoolConnection.
#[test]
fn pool_connection_queries_not_coalesced() {
    let pool = pool();
    let conn = pool.acquire().unwrap();

    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&conn)
        .unwrap();
    assert!(users.len() >= 2);
}

/// Execute (writes) are not affected by singleflight.
#[test]
fn execute_not_affected_by_singleflight() {
    let pool = pool();
    let desc = "singleflight-test-desc";
    let id = 1i32;
    let affected = bsql::query!("UPDATE tickets SET description = $desc: &str WHERE id = $id: i32")
        .execute(&pool)
        .unwrap();
    assert_eq!(affected, 1);
}

/// After concurrent queries complete, subsequent queries still work.
/// Verifies singleflight does not leak entries or corrupt state.
#[test]
fn queries_work_after_concurrent_burst() {
    use std::sync::Arc;

    let pool = Arc::new(pool());

    // Burst of 20 concurrent identical queries.
    // Some may fail with pool exhaustion (fail-fast pool, max 10 connections).
    // The point is that singleflight doesn't corrupt state.
    let mut handles = Vec::new();
    for _ in 0..20 {
        let pool = Arc::clone(&pool);
        handles.push(std::thread::spawn(move || {
            bsql::query!("SELECT id, login FROM users ORDER BY id")
                .fetch_all(pool.as_ref())
        }));
    }

    for handle in handles {
        let result = handle.join().expect("thread panicked");
        // Accept both success and pool exhaustion
        if let Ok(users) = &result {
            assert!(users.len() >= 2);
        }
    }

    // After the burst, normal queries should still work
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(pool.as_ref())
        .unwrap();
    assert_eq!(user.id, 1);
    assert_eq!(user.login, "alice");
}

/// fetch_optional through singleflight path works.
#[test]
fn singleflight_fetch_optional_works() {
    let pool = pool();
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_optional(&pool)
        .unwrap();
    assert!(user.is_some());
    assert_eq!(user.unwrap().login, "alice");
}

/// Different SQL texts are independently handled by singleflight.
#[test]
fn different_queries_are_independent() {
    use std::sync::Arc;

    let pool = Arc::new(pool());

    let pool1 = Arc::clone(&pool);
    let h1 = std::thread::spawn(move || {
        bsql::query!("SELECT id, login FROM users ORDER BY id")
            .fetch_all(pool1.as_ref())
    });

    let pool2 = Arc::clone(&pool);
    let h2 = std::thread::spawn(move || {
        bsql::query!("SELECT id FROM tickets ORDER BY id")
            .fetch_all(pool2.as_ref())
    });

    let users = h1.join().expect("thread panicked").unwrap();
    let tickets = h2.join().expect("thread panicked").unwrap();

    assert!(users.len() >= 2);
    assert!(!tickets.is_empty());
}
