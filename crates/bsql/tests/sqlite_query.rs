//! SQLite integration tests using bsql::query! macro.
//!
//! Requires:
//!   BSQL_DATABASE_URL=sqlite:///tmp/bsql_test.db (compile-time + runtime)
//!   Run tests/sqlite_setup.sh first to create the test database.
//!
//! Run with:
//!   BSQL_DATABASE_URL=sqlite:///tmp/bsql_test.db cargo test -p bsql --test sqlite_query --features sqlite-bundled

#![cfg(feature = "sqlite-bundled")]

use bsql::SqlitePool;

fn pool() -> SqlitePool {
    SqlitePool::open("/tmp/bsql_test.db").unwrap()
}

// ---------------------------------------------------------------------------
// Basic CRUD
// ---------------------------------------------------------------------------

#[test]
fn sqlite_fetch_all() {
    let pool = pool();
    let rows = bsql::query!("SELECT id, name FROM users ORDER BY id")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "alice");
    assert_eq!(rows[1].name, "bob");
}

#[test]
fn sqlite_fetch_one() {
    let pool = pool();
    let id = 1i64;
    let user = bsql::query!("SELECT id, name FROM users WHERE id = $id: i64")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(user.name, "alice");
}

#[test]
fn sqlite_fetch_optional_found() {
    let pool = pool();
    let id = 1i64;
    let user = bsql::query!("SELECT id, name FROM users WHERE id = $id: i64")
        .fetch_optional(&pool)
        .unwrap();
    assert!(user.is_some());
    assert_eq!(user.unwrap().name, "alice");
}

#[test]
fn sqlite_fetch_optional_not_found() {
    let pool = pool();
    let id = 999i64;
    let user = bsql::query!("SELECT id, name FROM users WHERE id = $id: i64")
        .fetch_optional(&pool)
        .unwrap();
    assert!(user.is_none());
}

#[test]
fn sqlite_execute() {
    let pool = pool();
    let name = "temp_user";
    let affected = bsql::query!("INSERT INTO users (name) VALUES ($name: &str)")
        .execute(&pool)
        .unwrap();
    assert_eq!(affected, 1);

    // Clean up
    bsql::query!("DELETE FROM users WHERE name = $name: &str")
        .execute(&pool)
        .unwrap();
}

// ---------------------------------------------------------------------------
// Nullable columns
// ---------------------------------------------------------------------------

#[test]
fn sqlite_nullable_column() {
    let pool = pool();
    let id = 2i64; // bob has NULL email
    let user = bsql::query!("SELECT id, name, email FROM users WHERE id = $id: i64")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(user.name, "bob");
    assert!(user.email.is_none());
}

#[test]
fn sqlite_not_null_column() {
    let pool = pool();
    let id = 1i64; // alice has email
    let user = bsql::query!("SELECT id, name, email FROM users WHERE id = $id: i64")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(user.email, Some("a@test.com".to_owned()));
}

// ---------------------------------------------------------------------------
// String auto-deref (String variable → &str param)
// ---------------------------------------------------------------------------

#[test]
fn sqlite_string_auto_deref() {
    let pool = pool();
    let name: String = "alice".to_owned();
    let user = bsql::query!("SELECT id, name FROM users WHERE name = $name: &str")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(user.name, "alice");
}

// ---------------------------------------------------------------------------
// Empty result
// ---------------------------------------------------------------------------

#[test]
fn sqlite_fetch_all_empty() {
    let pool = pool();
    let name = "nonexistent_user_xyz";
    let rows = bsql::query!("SELECT id, name FROM users WHERE name = $name: &str")
        .fetch_all(&pool)
        .unwrap();
    assert!(rows.is_empty());
}

#[test]
fn sqlite_fetch_one_empty_errors() {
    let pool = pool();
    let id = 999i64;
    let result = bsql::query!("SELECT id, name FROM users WHERE id = $id: i64").fetch_one(&pool);
    assert!(result.is_err());
}
