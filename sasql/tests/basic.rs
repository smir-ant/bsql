//! Integration tests: basic SELECT, INSERT, UPDATE, DELETE.
//!
//! Requires a running PostgreSQL with the test schema.
//! Set SASQL_DATABASE_URL=postgres://sasql:sasql@localhost/sasql_test

use sasql::{Pool, SasqlError};

async fn pool() -> Pool {
    Pool::connect("postgres://sasql:sasql@localhost/sasql_test")
        .await
        .expect("Failed to connect to test database. Is PostgreSQL running?")
}

#[tokio::test]
async fn select_fetch_one() {
    let pool = pool().await;
    let id = 1i32;
    let user = sasql::query!(
        "SELECT id, login, first_name, last_name FROM users WHERE id = $id: i32"
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(user.id, 1);
    assert_eq!(user.login, "alice");
    assert_eq!(user.first_name, "Alice");
    assert_eq!(user.last_name, "Smith");
}

#[tokio::test]
async fn select_fetch_all() {
    let pool = pool().await;
    let users = sasql::query!(
        "SELECT id, login FROM users WHERE active = true ORDER BY id"
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(users.len(), 2);
    assert_eq!(users[0].login, "alice");
    assert_eq!(users[1].login, "bob");
}

#[tokio::test]
async fn select_fetch_optional_found() {
    let pool = pool().await;
    let login = "alice";
    let user = sasql::query!(
        "SELECT id, login FROM users WHERE login = $login: &str"
    )
    .fetch_optional(&pool)
    .await
    .unwrap();

    assert!(user.is_some());
    assert_eq!(user.unwrap().login, "alice");
}

#[tokio::test]
async fn select_fetch_optional_not_found() {
    let pool = pool().await;
    let login = "nonexistent";
    let user = sasql::query!(
        "SELECT id, login FROM users WHERE login = $login: &str"
    )
    .fetch_optional(&pool)
    .await
    .unwrap();

    assert!(user.is_none());
}

#[tokio::test]
async fn select_nullable_column() {
    let pool = pool().await;
    let id = 1i32;
    let user = sasql::query!(
        "SELECT id, middle_name FROM users WHERE id = $id: i32"
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(user.id, 1);
    assert!(user.middle_name.is_none());
}

#[tokio::test]
async fn insert_returning() {
    let pool = pool().await;
    let title = "Test ticket";
    let uid = 1i32;
    let ticket = sasql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert!(ticket.id > 0);
}

#[tokio::test]
async fn update_execute() {
    let pool = pool().await;
    let desc = "Updated description";
    let id = 1i32;
    let affected = sasql::query!(
        "UPDATE tickets SET description = $desc: &str WHERE id = $id: i32"
    )
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(affected, 1);
}

#[tokio::test]
async fn delete_execute() {
    let pool = pool().await;
    let title = "To be deleted";
    let uid = 1i32;
    let ticket = sasql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let ticket_id = ticket.id;
    let affected = sasql::query!(
        "DELETE FROM tickets WHERE id = $ticket_id: i32"
    )
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(affected, 1);
}

#[tokio::test]
async fn fetch_one_zero_rows_errors() {
    let pool = pool().await;
    let id = 999999i32;
    let result = sasql::query!(
        "SELECT id, login FROM users WHERE id = $id: i32"
    )
    .fetch_one(&pool)
    .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        SasqlError::Query(e) => {
            assert!(e.message.contains("exactly 1 row"), "unexpected: {}", e.message);
        }
        other => panic!("expected Query error, got: {other:?}"),
    }
}

#[tokio::test]
async fn select_multiple_types() {
    let pool = pool().await;
    let id = 1i32;
    let user = sasql::query!(
        "SELECT id, login, active, score, rating, balance
         FROM users WHERE id = $id: i32"
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(user.id, 1i32);
    assert_eq!(user.login, "alice");
    assert!(user.active);
    assert_eq!(user.score, 42i16);
    assert!((user.rating - 4.5f32).abs() < f32::EPSILON);
    assert!((user.balance - 100.50f64).abs() < f64::EPSILON);
}

#[tokio::test]
async fn select_count_expression() {
    // COUNT(*) is a computed column — should be i64, nullable by default
    let pool = pool().await;
    let result = sasql::query!(
        "SELECT COUNT(*) as cnt FROM users"
    ).fetch_one(&pool).await.unwrap();
    // COUNT(*) never returns NULL (returns 0 for empty sets)
    // but our system defaults computed columns to nullable → Option<i64>
    assert!(result.cnt.is_some());
    assert!(result.cnt.unwrap() >= 2);
}

#[tokio::test]
async fn select_with_join_and_aliases() {
    let pool = pool().await;
    let id = 1i32;
    let result = sasql::query!(
        "SELECT t.id as ticket_id, t.title, u.login as creator
         FROM tickets t
         JOIN users u ON u.id = t.created_by_user_id
         WHERE t.id = $id: i32"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(result.ticket_id, 1);
    assert_eq!(result.title, "Fix login bug");
    assert_eq!(result.creator, "alice");
}

#[tokio::test]
async fn select_with_cte() {
    let pool = pool().await;
    let results = sasql::query!(
        "WITH active_users AS (
            SELECT id, login FROM users WHERE active = true
        )
        SELECT id, login FROM active_users ORDER BY id"
    ).fetch_all(&pool).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].login, "alice");
}

#[tokio::test]
async fn fetch_all_empty_result() {
    let pool = pool().await;
    let login = "absolutely_nobody_has_this_login";
    let results = sasql::query!(
        "SELECT id, login FROM users WHERE login = $login: &str"
    ).fetch_all(&pool).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn select_expression_arithmetic() {
    let pool = pool().await;
    let result = sasql::query!(
        "SELECT 1 + 1 as sum_val"
    ).fetch_one(&pool).await.unwrap();
    // Computed expression → nullable by default
    assert_eq!(result.sum_val, Some(2i32));
}

#[tokio::test]
async fn insert_on_conflict_do_nothing() {
    let pool = pool().await;
    // alice already exists — ON CONFLICT DO NOTHING returns 0 affected
    let login = "alice";
    let first_name = "Alice";
    let last_name = "Smith";
    let email = "alice@example.com";
    let affected = sasql::query!(
        "INSERT INTO users (login, first_name, last_name, email)
         VALUES ($login: &str, $first_name: &str, $last_name: &str, $email: &str)
         ON CONFLICT (login) DO NOTHING"
    ).execute(&pool).await.unwrap();
    assert_eq!(affected, 0);
}

#[tokio::test]
async fn delete_returning() {
    let pool = pool().await;
    let title = "Ticket for RETURNING test";
    let uid = 1i32;
    let ticket = sasql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    ).fetch_one(&pool).await.unwrap();

    let id = ticket.id;
    let deleted = sasql::query!(
        "DELETE FROM tickets WHERE id = $id: i32 RETURNING id, title"
    ).fetch_all(&pool).await.unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].id, id);
}

#[tokio::test]
async fn param_reuse_in_real_query() {
    let pool = pool().await;
    let id = 1i32;
    let user = sasql::query!(
        "SELECT id, login FROM users WHERE id = $id: i32 AND id = $id: i32"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(user.id, 1);
}

#[tokio::test]
async fn fetch_optional_multiple_rows_errors() {
    let pool = pool().await;
    // users table has 2+ rows with active=true — fetch_optional must error
    let result = sasql::query!(
        "SELECT id, login FROM users WHERE active = true"
    ).fetch_optional(&pool).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        SasqlError::Query(e) => {
            assert!(e.message.contains("0 or 1 rows"), "unexpected: {}", e.message);
        }
        other => panic!("expected Query error, got: {other:?}"),
    }
}

#[tokio::test]
async fn bytea_column_round_trip() {
    let pool = pool().await;
    let avatar: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
    let id = 1i32;
    // Set avatar
    sasql::query!(
        "UPDATE users SET avatar = $avatar: &[u8] WHERE id = $id: i32"
    ).execute(&pool).await.unwrap();

    // Read it back
    let user = sasql::query!(
        "SELECT id, avatar FROM users WHERE id = $id: i32"
    ).fetch_one(&pool).await.unwrap();

    assert_eq!(user.id, 1);
    assert_eq!(user.avatar.as_deref(), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
}

#[tokio::test]
async fn array_column_type() {
    let pool = pool().await;
    let id = 1i32;
    let user = sasql::query!(
        "SELECT id, tag_ids FROM users WHERE id = $id: i32"
    ).fetch_one(&pool).await.unwrap();

    assert_eq!(user.id, 1);
    assert!(user.tag_ids.is_empty()); // default '{}'
}
