//! SQLite integration tests for bsql.
//!
//! Tests the SQLite pool, transactions, error handling, and isolation.

#![cfg(feature = "sqlite-bundled")]

use bsql::SqlitePool;

fn setup_db() -> SqlitePool {
    let pool = SqlitePool::open(":memory:").unwrap();
    pool.raw_execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            score INTEGER
        )",
    )
    .unwrap();
    pool.raw_execute("INSERT INTO users (name, email, score) VALUES ('alice', 'a@test.com', 42)")
        .unwrap();
    pool.raw_execute("INSERT INTO users (name, email, score) VALUES ('bob', NULL, NULL)")
        .unwrap();
    pool
}

#[test]
fn sqlite_open_memory() {
    let _pool = SqlitePool::open(":memory:").unwrap();
}

#[test]
fn sqlite_raw_execute_create_and_insert() {
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
        .raw_execute("CREATE TABLE isolated (id INTEGER)")
        .unwrap();

    // pool2 should NOT see pool1's table
    let result = pool2.raw_execute("INSERT INTO isolated VALUES (1)");
    assert!(result.is_err(), "in-memory DBs should be isolated");
}

#[test]
fn sqlite_transaction_commit() {
    let pool = setup_db();
    pool.raw_execute("BEGIN").unwrap();
    pool.raw_execute("INSERT INTO users (name) VALUES ('charlie')")
        .unwrap();
    pool.raw_execute("COMMIT").unwrap();

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
    pool.raw_execute("BEGIN").unwrap();
    pool.raw_execute("INSERT INTO users (name) VALUES ('dave')")
        .unwrap();
    pool.raw_execute("ROLLBACK").unwrap();

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
    let result = pool.raw_execute("NOT VALID SQL");
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

#[test]
fn sqlite_concurrent_writes_wal() {
    // WAL mode allows concurrent readers + 1 writer
    let pool = setup_db();
    pool.raw_execute("PRAGMA journal_mode=WAL").unwrap();

    // Sequential writes should work
    for i in 0..10 {
        let sql = format!("INSERT INTO users (name) VALUES ('wal_test_{i}')");
        pool.raw_execute(&sql).unwrap();
    }

    let hash = bsql::driver::hash_sql("SELECT COUNT(*) FROM users");
    let counts = pool
        .fetch_all_direct("SELECT COUNT(*) FROM users", hash, &[], true, |stmt| {
            Ok(stmt.column_int64(0))
        })
        .unwrap();
    assert_eq!(counts, vec![12]); // 2 seed + 10 new
}

#[test]
fn sqlite_execute_batch_multiple_statements() {
    let pool = setup_db();
    pool.raw_execute("INSERT INTO users (name) VALUES ('batch1')")
        .unwrap();
    pool.raw_execute("INSERT INTO users (name) VALUES ('batch2')")
        .unwrap();
    pool.raw_execute("INSERT INTO users (name) VALUES ('batch3')")
        .unwrap();

    let hash = bsql::driver::hash_sql("SELECT COUNT(*) FROM users");
    let counts = pool
        .fetch_all_direct("SELECT COUNT(*) FROM users", hash, &[], true, |stmt| {
            Ok(stmt.column_int64(0))
        })
        .unwrap();
    assert_eq!(counts, vec![5]); // 2 seed + 3 batch
}

#[test]
fn sqlite_large_insert_batch() {
    let pool = setup_db();
    for i in 0..500 {
        let sql = format!("INSERT INTO users (name) VALUES ('large_{i}')");
        pool.raw_execute(&sql).unwrap();
    }
    let hash = bsql::driver::hash_sql("SELECT COUNT(*) FROM users");
    let counts = pool
        .fetch_all_direct("SELECT COUNT(*) FROM users", hash, &[], true, |stmt| {
            Ok(stmt.column_int64(0))
        })
        .unwrap();
    assert_eq!(counts, vec![502]); // 2 seed + 500
}
