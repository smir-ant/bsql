//! Integration tests: basic SELECT, INSERT, UPDATE, DELETE.
//!
//! Requires a running PostgreSQL with the test schema.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::{BsqlError, Pool};

async fn pool() -> Pool {
    Pool::connect("postgres://bsql:bsql@localhost/bsql_test")
        .await
        .expect("Failed to connect to test database. Is PostgreSQL running?")
}

#[tokio::test]
async fn select_fetch_one() {
    let pool = pool().await;
    let id = 1i32;
    let user =
        bsql::query!("SELECT id, login, first_name, last_name FROM users WHERE id = $id: i32")
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
    let users = bsql::query!("SELECT id, login FROM users WHERE active = true ORDER BY id")
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
    let user = bsql::query!("SELECT id, login FROM users WHERE login = $login: &str")
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
    let user = bsql::query!("SELECT id, login FROM users WHERE login = $login: &str")
        .fetch_optional(&pool)
        .await
        .unwrap();

    assert!(user.is_none());
}

#[tokio::test]
async fn select_nullable_column() {
    let pool = pool().await;
    let id = 1i32;
    let user = bsql::query!("SELECT id, middle_name FROM users WHERE id = $id: i32")
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
    let ticket = bsql::query!(
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
    let affected = bsql::query!("UPDATE tickets SET description = $desc: &str WHERE id = $id: i32")
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
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let ticket_id = ticket.id;
    let affected = bsql::query!("DELETE FROM tickets WHERE id = $ticket_id: i32")
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(affected, 1);
}

#[tokio::test]
async fn fetch_one_zero_rows_errors() {
    let pool = pool().await;
    let id = 999999i32;
    let result = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        BsqlError::Query(e) => {
            assert!(
                e.message.contains("exactly 1 row"),
                "unexpected: {}",
                e.message
            );
        }
        other => panic!("expected Query error, got: {other:?}"),
    }
}

#[tokio::test]
async fn select_multiple_types() {
    let pool = pool().await;
    let id = 1i32;
    let user = bsql::query!(
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
    let result = bsql::query!("SELECT COUNT(*) as cnt FROM users")
        .fetch_one(&pool)
        .await
        .unwrap();
    // COUNT(*) never returns NULL (returns 0 for empty sets)
    // but our system defaults computed columns to nullable → Option<i64>
    assert!(result.cnt.is_some());
    assert!(result.cnt.unwrap() >= 2);
}

#[tokio::test]
async fn select_with_join_and_aliases() {
    let pool = pool().await;
    let id = 1i32;
    let result = bsql::query!(
        "SELECT t.id as ticket_id, t.title, u.login as creator
         FROM tickets t
         JOIN users u ON u.id = t.created_by_user_id
         WHERE t.id = $id: i32"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(result.ticket_id, 1);
    assert_eq!(result.title, "Fix login bug");
    assert_eq!(result.creator, "alice");
}

#[tokio::test]
async fn select_with_cte() {
    let pool = pool().await;
    let results = bsql::query!(
        "WITH active_users AS (
            SELECT id, login FROM users WHERE active = true
        )
        SELECT id, login FROM active_users ORDER BY id"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].login, "alice");
}

#[tokio::test]
async fn fetch_all_empty_result() {
    let pool = pool().await;
    let login = "absolutely_nobody_has_this_login";
    let results = bsql::query!("SELECT id, login FROM users WHERE login = $login: &str")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn select_expression_arithmetic() {
    let pool = pool().await;
    let result = bsql::query!("SELECT 1 + 1 as sum_val")
        .fetch_one(&pool)
        .await
        .unwrap();
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
    let affected = bsql::query!(
        "INSERT INTO users (login, first_name, last_name, email)
         VALUES ($login: &str, $first_name: &str, $last_name: &str, $email: &str)
         ON CONFLICT (login) DO NOTHING"
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(affected, 0);
}

#[tokio::test]
async fn delete_returning() {
    let pool = pool().await;
    let title = "Ticket for RETURNING test";
    let uid = 1i32;
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let id = ticket.id;
    let deleted = bsql::query!("DELETE FROM tickets WHERE id = $id: i32 RETURNING id, title")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].id, id);
}

#[tokio::test]
async fn param_reuse_in_real_query() {
    let pool = pool().await;
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32 AND id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.id, 1);
}

#[tokio::test]
async fn fetch_optional_multiple_rows_errors() {
    let pool = pool().await;
    // users table has 2+ rows with active=true — fetch_optional must error
    let result = bsql::query!("SELECT id, login FROM users WHERE active = true")
        .fetch_optional(&pool)
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        BsqlError::Query(e) => {
            assert!(
                e.message.contains("0 or 1 rows"),
                "unexpected: {}",
                e.message
            );
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
    bsql::query!("UPDATE users SET avatar = $avatar: &[u8] WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();

    // Read it back
    let user = bsql::query!("SELECT id, avatar FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(user.id, 1);
    assert_eq!(user.avatar.as_deref(), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
}

#[tokio::test]
async fn array_column_type() {
    let pool = pool().await;
    let id = 1i32;
    let user = bsql::query!("SELECT id, tag_ids FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(user.id, 1);
    assert!(user.tag_ids.is_empty()); // default '{}'
}

#[tokio::test]
async fn connect_invalid_url() {
    let result = Pool::connect("not_a_url").await;
    assert!(result.is_err(), "invalid URL should fail");
}

#[tokio::test]
async fn select_star() {
    let pool = pool().await;
    let id = 1i32;
    let user = bsql::query!("SELECT * FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.id, 1);
}

#[tokio::test]
async fn pool_debug_format() {
    let pool = pool().await;
    let debug = format!("{:?}", pool);
    assert!(debug.contains("Pool"), "debug: {debug}");
    assert!(debug.contains("status"), "debug: {debug}");
}

#[tokio::test]
async fn pool_builder_url_method() {
    let pool = Pool::builder()
        .url("postgres://bsql:bsql@localhost/bsql_test")
        .build()
        .await
        .unwrap();

    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(users.len() >= 2);
}

// ---------------------------------------------------------------------------
// additional coverage: error variant matching
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetch_one_multiple_rows_errors() {
    let pool = pool().await;
    let result = bsql::query!("SELECT id, login FROM users WHERE active = true")
        .fetch_one(&pool)
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        BsqlError::Query(e) => {
            assert!(
                e.message.contains("exactly 1 row"),
                "unexpected: {}",
                e.message
            );
        }
        other => panic!("expected Query error, got: {other:?}"),
    }
}

#[tokio::test]
async fn pool_builder_max_size_and_status() {
    let pool = Pool::builder()
        .url("postgres://bsql:bsql@localhost/bsql_test")
        .max_size(4)
        .build()
        .await
        .unwrap();

    let status = pool.status();
    assert_eq!(status.max_size, 4);
}

#[tokio::test]
async fn pool_acquire_and_use() {
    let pool = pool().await;
    let conn = pool.acquire().await.unwrap();

    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&conn)
        .await
        .unwrap();
    assert_eq!(user.id, 1);
}

#[tokio::test]
async fn pool_builder_bad_url_errors() {
    let result = Pool::builder().url("not_a_url").build().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn execute_returns_zero_for_no_match() {
    let pool = pool().await;
    let id = 999999i32;
    let affected = bsql::query!("UPDATE tickets SET description = 'x' WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(affected, 0);
}

#[tokio::test]
async fn error_display_format() {
    let pool = pool().await;
    let id = 999999i32;
    let err = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap_err();

    let display = format!("{err}");
    assert!(
        display.contains("query error"),
        "BsqlError Display should start with variant name: {display}"
    );

    // std::error::Error trait is implemented
    let _: &dyn std::error::Error = &err;
}

// --- warmup ---

#[tokio::test]
async fn warmup_prepares_statements() {
    let pool = pool().await;
    pool.set_warmup_sqls(&["SELECT id, login FROM users WHERE id = $1::int4"]);
    // Acquire forces warmup on the new connection — the statement is prepared
    // via Parse+Describe+Sync (no Bind+Execute). Subsequent queries using the
    // same SQL skip the Parse round-trip because the statement is already cached.
    let conn = pool.acquire().await.unwrap();
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&conn)
        .await
        .unwrap();
    assert_eq!(user.id, 1);
    assert_eq!(user.login, "alice");
}

// ---------------------------------------------------------------------------
// T-1: basic streaming integration test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetch_stream_basic() {
    let pool = pool().await;
    let mut stream = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_stream(&pool)
        .await
        .unwrap();

    let mut count = 0;
    while let Some(user) = stream.next().await.unwrap() {
        count += 1;
        assert!(!user.login.is_empty());
    }
    assert!(count >= 2, "expected at least 2 users, got {count}");
}

// ---------------------------------------------------------------------------
// T-4: streaming with bind parameters
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetch_stream_with_bind_params() {
    let pool = pool().await;
    let active = true;
    let mut stream =
        bsql::query!("SELECT id, login FROM users WHERE active = $active: bool ORDER BY id")
            .fetch_stream(&pool)
            .await
            .unwrap();

    let mut count = 0;
    while let Some(user) = stream.next().await.unwrap() {
        count += 1;
        assert!(!user.login.is_empty());
    }
    assert!(count >= 2, "expected at least 2 active users, got {count}");
}

// ---------------------------------------------------------------------------
// T-10: streaming drop mid-iteration (connection should not be returned
//       to pool in broken state)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetch_stream_drop_mid_iteration() {
    let pool = pool().await;

    // Open a stream and drop it after reading only the first row.
    {
        let mut stream = bsql::query!("SELECT id, login FROM users ORDER BY id")
            .fetch_stream(&pool)
            .await
            .unwrap();

        let first = stream.next().await.unwrap();
        assert!(first.is_some(), "should have at least one row");
        // stream dropped here — connection discarded (not returned to pool)
    }

    // The pool should still be usable: a new connection can be acquired and
    // queries succeed without protocol-state corruption.
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.id, 1);
}

// ---------------------------------------------------------------------------
// T-10b: streaming fully consumed leaves pool healthy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetch_stream_fully_consumed() {
    let pool = pool().await;

    {
        let mut stream = bsql::query!("SELECT id, login FROM users ORDER BY id")
            .fetch_stream(&pool)
            .await
            .unwrap();

        while let Some(_user) = stream.next().await.unwrap() {}
        // stream dropped after full consumption — connection returned to pool
    }

    // Pool should work fine after a fully consumed stream.
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.id, 1);
}
