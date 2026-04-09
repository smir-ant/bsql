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

// ===========================================================================
// Execute edge cases
// ===========================================================================

#[test]
fn sqlite_execute_affected_zero() {
    let pool = pool();
    let id = 999i64;
    let affected = bsql::query!("UPDATE users SET score = 99 WHERE id = $id: i64")
        .execute(&pool)
        .unwrap();
    assert_eq!(affected, 0);
}

#[test]
fn sqlite_execute_affected_multiple() {
    let pool = pool();
    // Insert 3 temp rows
    for i in 100..103i64 {
        let name = format!("temp_{i}");
        bsql::query!("INSERT INTO users (name) VALUES ($name: &str)")
            .execute(&pool)
            .unwrap();
    }
    let affected = bsql::query!("DELETE FROM users WHERE name LIKE 'temp_%'")
        .execute(&pool)
        .unwrap();
    assert_eq!(affected, 3);
}

// ===========================================================================
// Parameters
// ===========================================================================

#[test]
fn sqlite_option_param_none() {
    let pool = pool();
    let id = 1i64;
    let desc: Option<String> = None;
    bsql::query!("UPDATE items SET description = $desc: Option<String> WHERE id = $id: i64")
        .execute(&pool)
        .unwrap();
    let item = bsql::query!("SELECT description FROM items WHERE id = $id: i64")
        .fetch_one(&pool)
        .unwrap();
    assert!(item.description.is_none());
    // Restore
    let desc: Option<String> = None; // was already NULL for item 1
    bsql::query!("UPDATE items SET description = $desc: Option<String> WHERE id = $id: i64")
        .execute(&pool)
        .unwrap();
}

#[test]
fn sqlite_option_param_some() {
    let pool = pool();
    let id = 1i64;
    let desc: Option<String> = Some("new_desc".to_owned());
    bsql::query!("UPDATE items SET description = $desc: Option<String> WHERE id = $id: i64")
        .execute(&pool)
        .unwrap();
    let item = bsql::query!("SELECT description FROM items WHERE id = $id: i64")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(item.description, Some("new_desc".to_owned()));
    // Restore to NULL
    let desc: Option<String> = None;
    bsql::query!("UPDATE items SET description = $desc: Option<String> WHERE id = $id: i64")
        .execute(&pool)
        .unwrap();
}

// ===========================================================================
// SQL Constructs
// ===========================================================================

#[test]
fn sqlite_join() {
    let pool = pool();
    let rows = bsql::query!(
        "SELECT i.title, u.name FROM items i JOIN users u ON u.id = i.owner_id ORDER BY i.id"
    )
    .fetch_all(&pool)
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "alice");
}

#[test]
fn sqlite_left_join() {
    let pool = pool();
    // LEFT JOIN: users without items get NULL title
    let rows = bsql::query!(
        "SELECT u.name, i.title FROM users u LEFT JOIN items i ON u.id = i.owner_id ORDER BY u.id"
    )
    .fetch_all(&pool)
    .unwrap();
    assert!(rows.len() >= 2);
    // title is Option<String> due to LEFT JOIN
}

#[test]
fn sqlite_subquery_in() {
    let pool = pool();
    let rows = bsql::query!("SELECT name FROM users WHERE id IN (SELECT owner_id FROM items)")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn sqlite_group_by_count() {
    let pool = pool();
    let rows = bsql::query!("SELECT owner_id, COUNT(*) AS cnt FROM items GROUP BY owner_id")
        .fetch_all(&pool)
        .unwrap();
    assert!(!rows.is_empty());
    // cnt is Option<i64> in SQLite
}

#[test]
fn sqlite_order_by_limit_offset() {
    let pool = pool();
    let rows = bsql::query!("SELECT name FROM users ORDER BY id LIMIT 1 OFFSET 1")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "bob");
}

#[test]
fn sqlite_like_with_param() {
    let pool = pool();
    let pattern = "%ali%";
    let rows = bsql::query!("SELECT name FROM users WHERE name LIKE $pattern: &str")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "alice");
}

#[test]
fn sqlite_is_null() {
    let pool = pool();
    let rows = bsql::query!("SELECT name FROM users WHERE email IS NULL")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "bob");
}

#[test]
fn sqlite_is_not_null() {
    let pool = pool();
    let rows = bsql::query!("SELECT name FROM users WHERE email IS NOT NULL")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "alice");
}

#[test]
fn sqlite_union_all() {
    let pool = pool();
    let rows =
        bsql::query!("SELECT name AS val FROM users UNION ALL SELECT title AS val FROM items")
            .fetch_all(&pool)
            .unwrap();
    assert_eq!(rows.len(), 4); // 2 users + 2 items
}

#[test]
fn sqlite_cte() {
    let pool = pool();
    let rows = bsql::query!(
        "WITH active AS (SELECT id, name FROM users WHERE active = 1)
         SELECT name FROM active ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn sqlite_between() {
    let pool = pool();
    let low = 1i64;
    let high = 2i64;
    let rows = bsql::query!(
        "SELECT name FROM users WHERE id BETWEEN $low: i64 AND $high: i64 ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn sqlite_coalesce() {
    let pool = pool();
    let rows = bsql::query!("SELECT COALESCE(email, 'N/A') AS email FROM users ORDER BY id")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(rows.len(), 2);
    // COALESCE in SQLite returns Option<String>
}

#[test]
fn sqlite_case_when() {
    let pool = pool();
    let rows = bsql::query!(
        "SELECT CASE WHEN active = 1 THEN 'yes' ELSE 'no' END AS status FROM users ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();
    assert_eq!(rows.len(), 2);
    // status is Option<String> in SQLite
}

// ===========================================================================
// Nullability
// ===========================================================================

#[test]
fn sqlite_null_vs_empty_string() {
    let pool = pool();
    let id = 1i64;
    // Set description to empty string (not NULL)
    let desc: Option<String> = Some(String::new());
    bsql::query!("UPDATE items SET description = $desc: Option<String> WHERE id = $id: i64")
        .execute(&pool)
        .unwrap();
    let item = bsql::query!("SELECT description FROM items WHERE id = $id: i64")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(item.description, Some(String::new())); // empty string, not None
                                                       // Restore to NULL
    let desc: Option<String> = None;
    bsql::query!("UPDATE items SET description = $desc: Option<String> WHERE id = $id: i64")
        .execute(&pool)
        .unwrap();
}

// ===========================================================================
// Error handling
// ===========================================================================

#[test]
fn sqlite_unique_constraint_error() {
    let pool = pool();
    // Insert alice again — id=1 already exists (INTEGER PRIMARY KEY)
    let result =
        bsql::query!("INSERT INTO users (id, name) VALUES (1, 'duplicate')").execute(&pool);
    assert!(result.is_err());
}

// ===========================================================================
// for_each
// ===========================================================================

#[test]
fn sqlite_for_each_iterates() {
    let pool = pool();
    let mut count = 0u32;
    bsql::query!("SELECT id, name FROM users ORDER BY id")
        .for_each(&pool, |_row| {
            count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 2);
}

#[test]
fn sqlite_for_each_empty() {
    let pool = pool();
    let mut count = 0u32;
    let name = "nonexistent";
    bsql::query!("SELECT id FROM users WHERE name = $name: &str")
        .for_each(&pool, |_row| {
            count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 0);
}

// ===========================================================================
// Unicode
// ===========================================================================

#[test]
fn sqlite_unicode_roundtrip() {
    let pool = pool();
    let name = "Тест 🎉 中文";
    bsql::query!("INSERT INTO users (name) VALUES ($name: &str)")
        .execute(&pool)
        .unwrap();
    let row = bsql::query!("SELECT name FROM users WHERE name = $name: &str")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(row.name, "Тест 🎉 中文");
    // Cleanup
    bsql::query!("DELETE FROM users WHERE name = $name: &str")
        .execute(&pool)
        .unwrap();
}
