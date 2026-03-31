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
    let status = "resolved";
    let id = 1i32;
    let affected = sasql::query!(
        "UPDATE tickets SET status = $status: &str WHERE id = $id: i32"
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
