//! SQLite integration tests for bsql.
//!
//! Tests the SQLite pool, transactions, error handling, and isolation.

#![cfg(feature = "sqlite-bundled")]

use bsql::SqlitePool;

fn setup_db() -> SqlitePool {
    let pool = SqlitePool::open(":memory:").unwrap();
    pool.simple_exec(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            score INTEGER
        )",
    )
    .unwrap();
    pool.simple_exec("INSERT INTO users (name, email, score) VALUES ('alice', 'a@test.com', 42)")
        .unwrap();
    pool.simple_exec("INSERT INTO users (name, email, score) VALUES ('bob', NULL, NULL)")
        .unwrap();
    pool
}

#[test]
fn sqlite_open_memory() {
    let _pool = SqlitePool::open(":memory:").unwrap();
}

#[test]
fn sqlite_simple_exec_create_and_insert() {
    let pool = setup_db();
    // Verify data exists via fetch_all_direct
    let hash = bsql::driver::hash_sql("SELECT COUNT(*) FROM users");
    let counts = pool
        .fetch_all_direct("SELECT COUNT(*) FROM users", hash, &[], true, |stmt| {
            Ok(stmt.column_int64(0))
        })
        .unwrap();
    assert_eq!(counts, vec![2]);
}

#[test]
fn sqlite_nullable_column_returns_none() {
    let pool = setup_db();
    let hash = bsql::driver::hash_sql("SELECT email FROM users ORDER BY id");
    let emails = pool
        .fetch_all_direct(
            "SELECT email FROM users ORDER BY id",
            hash,
            &[],
            true,
            |stmt| Ok(stmt.column_text(0).map(|s| s.to_owned())),
        )
        .unwrap();
    assert_eq!(emails.len(), 2);
    assert!(emails[0].is_some()); // alice has email
    assert!(emails[1].is_none()); // bob has NULL email
}

#[test]
fn sqlite_in_memory_isolation() {
    let pool1 = SqlitePool::open(":memory:").unwrap();
    let pool2 = SqlitePool::open(":memory:").unwrap();

    pool1
        .simple_exec("CREATE TABLE isolated (id INTEGER)")
        .unwrap();

    // pool2 should NOT see pool1's table
    let result = pool2.simple_exec("INSERT INTO isolated VALUES (1)");
    assert!(result.is_err(), "in-memory DBs should be isolated");
}

#[test]
fn sqlite_transaction_commit() {
    let pool = setup_db();
    pool.simple_exec("BEGIN").unwrap();
    pool.simple_exec("INSERT INTO users (name) VALUES ('charlie')")
        .unwrap();
    pool.simple_exec("COMMIT").unwrap();

    let hash = bsql::driver::hash_sql("SELECT COUNT(*) FROM users");
    let counts = pool
        .fetch_all_direct("SELECT COUNT(*) FROM users", hash, &[], true, |stmt| {
            Ok(stmt.column_int64(0))
        })
        .unwrap();
    assert_eq!(counts, vec![3]);
}

#[test]
fn sqlite_transaction_rollback() {
    let pool = setup_db();
    pool.simple_exec("BEGIN").unwrap();
    pool.simple_exec("INSERT INTO users (name) VALUES ('dave')")
        .unwrap();
    pool.simple_exec("ROLLBACK").unwrap();

    let hash = bsql::driver::hash_sql("SELECT COUNT(*) FROM users");
    let counts = pool
        .fetch_all_direct("SELECT COUNT(*) FROM users", hash, &[], true, |stmt| {
            Ok(stmt.column_int64(0))
        })
        .unwrap();
    assert_eq!(counts, vec![2]);
}

#[test]
fn sqlite_error_bad_sql() {
    let pool = setup_db();
    let result = pool.simple_exec("NOT VALID SQL");
    assert!(result.is_err());
}

#[test]
fn sqlite_error_nonexistent_table() {
    let pool = setup_db();
    let hash = bsql::driver::hash_sql("SELECT * FROM nonexistent");
    let result = pool.fetch_all_direct("SELECT * FROM nonexistent", hash, &[], true, |stmt| {
        Ok(stmt.column_int64(0))
    });
    assert!(result.is_err());
}

#[test]
fn sqlite_multiple_readers() {
    // SqlitePool supports multiple concurrent readers
    let pool = setup_db();
    let hash = bsql::driver::hash_sql("SELECT COUNT(*) FROM users");

    // Multiple reads should not block
    for _ in 0..10 {
        let counts = pool
            .fetch_all_direct("SELECT COUNT(*) FROM users", hash, &[], true, |stmt| {
                Ok(stmt.column_int64(0))
            })
            .unwrap();
        assert_eq!(counts, vec![2]);
    }
}

#[test]
fn sqlite_open_nonexistent_readonly_fails() {
    // Opening a nonexistent path for read should fail gracefully
    let result = SqlitePool::open("/nonexistent/path/to/db.sqlite");
    assert!(result.is_err());
}
