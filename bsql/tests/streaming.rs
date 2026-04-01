//! Integration tests: fetch_stream — streaming query results.
//!
//! Requires a running PostgreSQL with the test schema.
//! Set BSQL_DATABASE_URL=postgres://sasql:sasql@localhost/sasql_test

use bsql::Pool;
use tokio_stream::StreamExt;

async fn pool() -> Pool {
    Pool::connect("postgres://sasql:sasql@localhost/sasql_test")
        .await
        .expect("Failed to connect to test database. Is PostgreSQL running?")
}

#[tokio::test]
async fn stream_multiple_rows() {
    let pool = pool().await;
    let mut stream = bsql::query!("SELECT id, login FROM users WHERE active = true ORDER BY id")
        .fetch_stream(&pool)
        .await
        .unwrap();

    let mut rows = Vec::new();
    while let Some(row) = stream.next().await {
        rows.push(row.unwrap());
    }

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].login, "alice");
    assert_eq!(rows[1].login, "bob");
}

#[tokio::test]
async fn stream_single_row() {
    let pool = pool().await;
    let id = 1i32;
    let mut stream = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_stream(&pool)
        .await
        .unwrap();

    let row = stream.next().await.unwrap().unwrap();
    assert_eq!(row.id, 1);
    assert_eq!(row.login, "alice");

    // No more rows
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn stream_zero_rows() {
    let pool = pool().await;
    let login = "nonexistent_user_for_stream_test";
    let mut stream = bsql::query!("SELECT id, login FROM users WHERE login = $login: &str")
        .fetch_stream(&pool)
        .await
        .unwrap();

    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn stream_with_nullable_column() {
    let pool = pool().await;
    let mut stream = bsql::query!("SELECT id, middle_name FROM users ORDER BY id")
        .fetch_stream(&pool)
        .await
        .unwrap();

    let row = stream.next().await.unwrap().unwrap();
    assert_eq!(row.id, 1);
    assert!(row.middle_name.is_none());
}

#[tokio::test]
async fn stream_connection_returns_to_pool_on_drop() {
    let pool = pool().await;
    let before = pool.status();

    {
        let mut stream = bsql::query!("SELECT id, login FROM users ORDER BY id")
            .fetch_stream(&pool)
            .await
            .unwrap();

        // Consume one row then drop the stream
        let _row = stream.next().await.unwrap().unwrap();
        // stream drops here
    }

    // Connection should be returned to the pool
    let after = pool.status();
    assert!(
        after.available >= before.available,
        "connection not returned: before={}, after={}",
        before.available,
        after.available,
    );
}

#[tokio::test]
async fn stream_collect_all() {
    let pool = pool().await;
    let stream = bsql::query!("SELECT id, login FROM users WHERE active = true ORDER BY id")
        .fetch_stream(&pool)
        .await
        .unwrap();

    let rows: Vec<_> = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].login, "alice");
    assert_eq!(rows[1].login, "bob");
}

#[tokio::test]
async fn stream_with_params_and_join() {
    let pool = pool().await;
    let id = 1i32;
    let mut stream = bsql::query!(
        "SELECT t.id as ticket_id, t.title, u.login as creator
         FROM tickets t
         JOIN users u ON u.id = t.created_by_user_id
         WHERE t.id = $id: i32"
    )
    .fetch_stream(&pool)
    .await
    .unwrap();

    let row = stream.next().await.unwrap().unwrap();
    assert_eq!(row.ticket_id, 1);
    assert_eq!(row.title, "Fix login bug");
    assert_eq!(row.creator, "alice");
    assert!(stream.next().await.is_none());
}
