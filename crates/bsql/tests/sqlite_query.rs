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

// ===========================================================================
// Query execution edge cases
// ===========================================================================

#[test]
fn sqlite_fetch_one_multiple_rows_errors() {
    let pool = pool();
    // users has 2 rows — fetch_one should error
    let result = bsql::query!("SELECT id, name FROM users").fetch_one(&pool);
    assert!(result.is_err());
}

#[test]
fn sqlite_fetch_optional_multiple_rows_returns_first() {
    let pool = pool();
    // SQLite fetch_optional uses LIMIT internally — returns first row, no error
    let result = bsql::query!("SELECT id, name FROM users ORDER BY id").fetch_optional(&pool);
    assert!(result.is_ok());
    let row = result.unwrap().unwrap();
    assert_eq!(row.name, "alice");
}

#[test]
fn sqlite_for_each_map_collects() {
    let pool = pool();
    let names: Vec<String> = bsql::query!("SELECT name FROM users ORDER BY id")
        .for_each_map(&pool, |row| row.name.to_owned())
        .unwrap();
    assert_eq!(names, vec!["alice", "bob"]);
}

// ===========================================================================
// Parameters — more edge cases
// ===========================================================================

#[test]
fn sqlite_same_param_twice() {
    let pool = pool();
    let name = "alice";
    let rows =
        bsql::query!("SELECT id FROM users WHERE name = $name: &str OR email LIKE $name: &str")
            .fetch_all(&pool)
            .unwrap();
    // alice matches name='alice', email doesn't match 'alice' exactly
    assert!(!rows.is_empty());
}

// ===========================================================================
// SQL constructs — remaining gaps
// ===========================================================================

#[test]
fn sqlite_self_join() {
    let pool = pool();
    let rows = bsql::query!(
        "SELECT a.name AS name_a, b.name AS name_b FROM users a JOIN users b ON a.id != b.id"
    )
    .fetch_all(&pool)
    .unwrap();
    assert_eq!(rows.len(), 2); // alice-bob and bob-alice
}

#[test]
fn sqlite_exists_subquery() {
    let pool = pool();
    let rows = bsql::query!(
        "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM items WHERE owner_id = users.id)"
    )
    .fetch_all(&pool)
    .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn sqlite_group_by_having() {
    let pool = pool();
    let rows = bsql::query!(
        "SELECT owner_id, COUNT(*) AS cnt FROM items GROUP BY owner_id HAVING COUNT(*) >= 1"
    )
    .fetch_all(&pool)
    .unwrap();
    assert!(!rows.is_empty());
}

#[test]
fn sqlite_count_distinct() {
    let pool = pool();
    let row = bsql::query!("SELECT COUNT(DISTINCT owner_id) AS cnt FROM items")
        .fetch_one(&pool)
        .unwrap();
    // cnt is Option<i64> in SQLite
    assert!(row.cnt.is_some());
}

#[test]
fn sqlite_intersect() {
    let pool = pool();
    let rows = bsql::query!(
        "SELECT id FROM users WHERE active = 1
         INTERSECT
         SELECT owner_id AS id FROM items"
    )
    .fetch_all(&pool)
    .unwrap();
    assert!(!rows.is_empty());
}

#[test]
fn sqlite_except() {
    let pool = pool();
    let rows = bsql::query!(
        "SELECT id FROM users
         EXCEPT
         SELECT owner_id AS id FROM items"
    )
    .fetch_all(&pool)
    .unwrap();
    // Both users own items
    assert!(rows.is_empty());
}

#[test]
fn sqlite_recursive_cte() {
    let pool = pool();
    let rows = bsql::query!(
        "WITH RECURSIVE nums AS (
            SELECT 1 AS n
            UNION ALL
            SELECT n + 1 FROM nums WHERE n < 5
        )
        SELECT n FROM nums ORDER BY n"
    )
    .fetch_all(&pool)
    .unwrap();
    assert_eq!(rows.len(), 5);
}

#[test]
fn sqlite_on_conflict() {
    let pool = pool();
    // Insert with existing id=1 — ON CONFLICT ignore
    let affected = bsql::query!("INSERT OR IGNORE INTO users (id, name) VALUES (1, 'duplicate')")
        .execute(&pool)
        .unwrap();
    assert_eq!(affected, 0);
}

#[test]
fn sqlite_arithmetic() {
    let pool = pool();
    let row = bsql::query!("SELECT 1 + 2 AS result")
        .fetch_one(&pool)
        .unwrap();
    // result is Option<String> in SQLite (expression columns)
    assert_eq!(row.result, Some("3".to_owned()));
}

#[test]
fn sqlite_string_concat() {
    let pool = pool();
    let row = bsql::query!("SELECT name || '@test' AS combined FROM users WHERE id = 1")
        .fetch_one(&pool)
        .unwrap();
    // combined is Option<String> in SQLite
    assert_eq!(row.combined, Some("alice@test".to_owned()));
}

#[test]
fn sqlite_comments_in_sql() {
    let pool = pool();
    let rows = bsql::query!(
        "SELECT id, name -- line comment
         FROM users
         /* block comment */
         ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();
    assert_eq!(rows.len(), 2);
}

// ===========================================================================
// Error handling
// ===========================================================================

#[test]
fn sqlite_fk_violation() {
    let pool = pool();
    // Enable FK enforcement
    pool.raw_execute("PRAGMA foreign_keys = ON").unwrap();
    // owner_id 999 doesn't exist
    let result =
        bsql::query!("INSERT INTO items (title, owner_id) VALUES ('fk_test', 999)").execute(&pool);
    assert!(result.is_err());
    pool.raw_execute("PRAGMA foreign_keys = OFF").unwrap();
}

#[test]
fn sqlite_fetch_one_error_message_clear() {
    let pool = pool();
    let id = 999i64;
    let result = bsql::query!("SELECT name FROM users WHERE id = $id: i64").fetch_one(&pool);
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("1 row") || msg.contains("exactly"),
        "error should mention row count: {msg}"
    );
}

// ===========================================================================
// Edge cases
// ===========================================================================

#[test]
fn sqlite_boundary_i64_max() {
    let pool = pool();
    let big = i64::MAX;
    bsql::query!("INSERT INTO users (name, score) VALUES ('big', $big: i64)")
        .execute(&pool)
        .unwrap();
    let row = bsql::query!("SELECT score FROM users WHERE name = 'big'")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(row.score, Some(i64::MAX));
    bsql::query!("DELETE FROM users WHERE name = 'big'")
        .execute(&pool)
        .unwrap();
}

#[test]
fn sqlite_concurrent_reads() {
    let pool = pool();
    // Multiple sequential reads should not deadlock
    for _ in 0..20 {
        let rows = bsql::query!("SELECT id FROM users")
            .fetch_all(&pool)
            .unwrap();
        assert_eq!(rows.len(), 2);
    }
}

// ===========================================================================
// Stress tests
// ===========================================================================

#[test]
#[ignore] // stress: ~2 seconds
fn sqlite_stress_insert_delete_100() {
    let pool = pool();
    for i in 0..100 {
        let name = format!("stress_{i}");
        bsql::query!("INSERT INTO users (name) VALUES ($name: &str)")
            .execute(&pool)
            .unwrap();
    }
    let affected = bsql::query!("DELETE FROM users WHERE name LIKE 'stress_%'")
        .execute(&pool)
        .unwrap();
    assert_eq!(affected, 100);
}

#[test]
#[ignore] // stress: for_each on many rows
fn sqlite_stress_for_each_100() {
    let pool = pool();
    for i in 0..100 {
        let name = format!("fe_stress_{i}");
        bsql::query!("INSERT INTO users (name) VALUES ($name: &str)")
            .execute(&pool)
            .unwrap();
    }
    let mut count = 0u32;
    bsql::query!("SELECT id, name FROM users WHERE name LIKE 'fe_stress_%'")
        .for_each(&pool, |_row| {
            count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 100);
    bsql::query!("DELETE FROM users WHERE name LIKE 'fe_stress_%'")
        .execute(&pool)
        .unwrap();
}

// ===========================================================================
// Parity with PG shared scenarios
// ===========================================================================

#[test]
fn sqlite_select_multiple_types() {
    let pool = pool();
    let id = 1i64;
    let user = bsql::query!("SELECT id, name, email, score, active FROM users WHERE id = $id: i64")
        .fetch_one(&pool)
        .unwrap();
    // Verify all types are accessible
    let _id: Option<i64> = user.id;
    let _name: String = user.name;
    let _email: Option<String> = user.email;
    let _score: Option<i64> = user.score;
    let _active: i64 = user.active;
}

#[test]
fn sqlite_select_star() {
    let pool = pool();
    let rows = bsql::query!("SELECT * FROM users ORDER BY id")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn sqlite_select_count_expression() {
    let pool = pool();
    let row = bsql::query!("SELECT COUNT(*) AS cnt FROM users")
        .fetch_one(&pool)
        .unwrap();
    // COUNT in SQLite is Option<i64>
    assert!(row.cnt.is_some());
}

#[test]
fn sqlite_for_each_closure_error_stops() {
    let pool = pool();
    let mut count = 0u32;
    let result = bsql::query!("SELECT id, name FROM users ORDER BY id").for_each(&pool, |_row| {
        count += 1;
        if count >= 1 {
            return Err(bsql::error::QueryError::row_count("test stop", 0));
        }
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(count, 1, "should stop after first row");
}

#[test]
fn sqlite_multiple_fetch_all_same_pool() {
    let pool = pool();
    let users = bsql::query!("SELECT id, name FROM users ORDER BY id")
        .fetch_all(&pool)
        .unwrap();
    let items = bsql::query!("SELECT id, title FROM items ORDER BY id")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(items.len(), 2);
}

#[test]
fn sqlite_fetch_all_then_fetch_one() {
    let pool = pool();
    let users = bsql::query!("SELECT id, name FROM users ORDER BY id")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(users.len(), 2);
    let id = 1i64;
    let user = bsql::query!("SELECT id, name FROM users WHERE id = $id: i64")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(user.name, "alice");
}

#[test]
fn sqlite_insert_verify_delete() {
    let pool = pool();
    let name = "roundtrip_test";
    bsql::query!("INSERT INTO users (name) VALUES ($name: &str)")
        .execute(&pool)
        .unwrap();
    let row = bsql::query!("SELECT name FROM users WHERE name = $name: &str")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(row.name, "roundtrip_test");
    bsql::query!("DELETE FROM users WHERE name = $name: &str")
        .execute(&pool)
        .unwrap();
    let rows = bsql::query!("SELECT name FROM users WHERE name = $name: &str")
        .fetch_all(&pool)
        .unwrap();
    assert!(rows.is_empty());
}

#[test]
fn sqlite_empty_string_param() {
    let pool = pool();
    let id = 1i64;
    let desc = Some("".to_owned());
    bsql::query!("UPDATE items SET description = $desc: Option<String> WHERE id = $id: i64")
        .execute(&pool)
        .unwrap();
    let item = bsql::query!("SELECT description FROM items WHERE id = $id: i64")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(item.description, Some(String::new()));
    // Restore
    let desc: Option<String> = None;
    bsql::query!("UPDATE items SET description = $desc: Option<String> WHERE id = $id: i64")
        .execute(&pool)
        .unwrap();
}

#[test]
fn sqlite_error_display_format() {
    let pool = pool();
    let id = 999i64;
    let err = bsql::query!("SELECT name FROM users WHERE id = $id: i64")
        .fetch_one(&pool)
        .unwrap_err();
    let msg = err.to_string();
    assert!(!msg.is_empty(), "error display should not be empty");
}

// ===========================================================================
// query_as!
// ===========================================================================

#[derive(Debug)]
#[allow(dead_code)]
struct UserRow {
    id: Option<i64>,
    name: String,
    email: Option<String>,
}

#[test]
fn sqlite_query_as_fetch_one() {
    let pool = pool();
    let id = 1i64;
    let user = bsql::query_as!(
        UserRow,
        "SELECT id, name, email FROM users WHERE id = $id: i64"
    )
    .fetch_one(&pool)
    .unwrap();
    assert_eq!(user.name, "alice");
    assert_eq!(user.email, Some("a@test.com".to_owned()));
}

#[test]
fn sqlite_query_as_fetch_all() {
    let pool = pool();
    let rows = bsql::query_as!(UserRow, "SELECT id, name, email FROM users ORDER BY id")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "alice");
    assert_eq!(rows[1].name, "bob");
}

#[test]
fn sqlite_query_as_fetch_optional_found() {
    let pool = pool();
    let id = 1i64;
    let user = bsql::query_as!(
        UserRow,
        "SELECT id, name, email FROM users WHERE id = $id: i64"
    )
    .fetch_optional(&pool)
    .unwrap();
    assert!(user.is_some());
    assert_eq!(user.unwrap().name, "alice");
}

#[test]
fn sqlite_query_as_fetch_optional_not_found() {
    let pool = pool();
    let id = 999i64;
    let user = bsql::query_as!(
        UserRow,
        "SELECT id, name, email FROM users WHERE id = $id: i64"
    )
    .fetch_optional(&pool)
    .unwrap();
    assert!(user.is_none());
}

#[test]
fn sqlite_query_as_nullable_field() {
    let pool = pool();
    let id = 2i64; // bob has NULL email
    let user = bsql::query_as!(
        UserRow,
        "SELECT id, name, email FROM users WHERE id = $id: i64"
    )
    .fetch_one(&pool)
    .unwrap();
    assert_eq!(user.name, "bob");
    assert!(user.email.is_none());
}

// ===========================================================================
// SQL constructs — remaining gaps
// ===========================================================================

#[test]
fn sqlite_subquery_in_from() {
    let pool = pool();
    let rows = bsql::query!("SELECT sub.name FROM (SELECT id, name FROM users ORDER BY id) sub")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn sqlite_window_function() {
    let pool = pool();
    let rows =
        bsql::query!("SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users ORDER BY id")
            .fetch_all(&pool)
            .unwrap();
    assert_eq!(rows.len(), 2);
    // rn is Option<i64> in SQLite (no NOT NULL inference for window functions)
}

#[test]
fn sqlite_union_deduplicated() {
    let pool = pool();
    // UNION (not ALL) removes duplicates
    let rows = bsql::query!("SELECT name FROM users UNION SELECT name FROM users")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(rows.len(), 2); // deduplicated
}

#[test]
fn sqlite_multiple_ctes() {
    let pool = pool();
    let rows = bsql::query!(
        "WITH u AS (SELECT id, name FROM users WHERE active = 1),
              i AS (SELECT owner_id, COUNT(*) AS cnt FROM items GROUP BY owner_id)
         SELECT u.name FROM u JOIN i ON u.id = i.owner_id ORDER BY u.name"
    )
    .fetch_all(&pool)
    .unwrap();
    assert!(!rows.is_empty());
}

#[test]
fn sqlite_multiple_joins() {
    let pool = pool();
    // Join users with items
    let rows = bsql::query!(
        "SELECT i.title, u.name
         FROM items i
         JOIN users u ON u.id = i.owner_id
         ORDER BY i.id"
    )
    .fetch_all(&pool)
    .unwrap();
    assert_eq!(rows.len(), 2);
}
