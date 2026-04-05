//! Integration tests: basic SELECT, INSERT, UPDATE, DELETE.
//!
//! Requires a running PostgreSQL with the test schema.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::{BsqlError, Pool};

fn pool() -> Pool {
    Pool::connect("postgres://bsql:bsql@localhost/bsql_test")
        .expect("Failed to connect to test database. Is PostgreSQL running?")
}

#[test]
fn select_fetch_one() {
    let pool = pool();
    let id = 1i32;
    let user =
        bsql::query!("SELECT id, login, first_name, last_name FROM users WHERE id = $id: i32")
            .fetch_one(&pool)
            .unwrap();

    assert_eq!(user.id, 1);
    assert_eq!(user.login, "alice");
    assert_eq!(user.first_name, "Alice");
    assert_eq!(user.last_name, "Smith");
}

#[test]
fn select_fetch_all() {
    let pool = pool();
    let users = bsql::query!("SELECT id, login FROM users WHERE active = true ORDER BY id")
        .fetch_all(&pool)
        .unwrap();

    assert_eq!(users.len(), 2);
    assert_eq!(users[0].login, "alice");
    assert_eq!(users[1].login, "bob");
}

#[test]
fn select_fetch_optional_found() {
    let pool = pool();
    let login = "alice";
    let user = bsql::query!("SELECT id, login FROM users WHERE login = $login: &str")
        .fetch_optional(&pool)
        .unwrap();

    assert!(user.is_some());
    assert_eq!(user.unwrap().login, "alice");
}

#[test]
fn select_fetch_optional_not_found() {
    let pool = pool();
    let login = "nonexistent";
    let user = bsql::query!("SELECT id, login FROM users WHERE login = $login: &str")
        .fetch_optional(&pool)
        .unwrap();

    assert!(user.is_none());
}

#[test]
fn select_nullable_column() {
    let pool = pool();
    let id = 1i32;
    let user = bsql::query!("SELECT id, middle_name FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .unwrap();

    assert_eq!(user.id, 1);
    assert!(user.middle_name.is_none());
}

#[test]
fn insert_returning() {
    let pool = pool();
    let title = "Test ticket";
    let uid = 1i32;
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&pool)
    .unwrap();

    assert!(ticket.id > 0);
}

#[test]
fn update_execute() {
    let pool = pool();
    let desc = "Updated description";
    let id = 1i32;
    let affected = bsql::query!("UPDATE tickets SET description = $desc: &str WHERE id = $id: i32")
        .execute(&pool)
        .unwrap();

    assert_eq!(affected, 1);
}

#[test]
fn delete_execute() {
    let pool = pool();
    let title = "To be deleted";
    let uid = 1i32;
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&pool)
    .unwrap();

    let ticket_id = ticket.id;
    let affected = bsql::query!("DELETE FROM tickets WHERE id = $ticket_id: i32")
        .execute(&pool)
        .unwrap();

    assert_eq!(affected, 1);
}

#[test]
fn fetch_one_zero_rows_errors() {
    let pool = pool();
    let id = 999999i32;
    let result = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool);

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

#[test]
fn select_multiple_types() {
    let pool = pool();
    let id = 1i32;
    let user = bsql::query!(
        "SELECT id, login, active, score, rating, balance
         FROM users WHERE id = $id: i32"
    )
    .fetch_one(&pool)
    .unwrap();

    assert_eq!(user.id, 1i32);
    assert_eq!(user.login, "alice");
    assert!(user.active);
    assert_eq!(user.score, 42i16);
    assert!((user.rating - 4.5f32).abs() < f32::EPSILON);
    assert!((user.balance - 100.50f64).abs() < f64::EPSILON);
}

#[test]
fn select_count_expression() {
    // COUNT(*) is a computed column -- should be i64, nullable by default
    let pool = pool();
    let result = bsql::query!("SELECT COUNT(*) as cnt FROM users")
        .fetch_one(&pool)
        .unwrap();
    // COUNT(*) never returns NULL (returns 0 for empty sets)
    // but our system defaults computed columns to nullable -> Option<i64>
    assert!(result.cnt.is_some());
    assert!(result.cnt.unwrap() >= 2);
}

#[test]
fn select_with_join_and_aliases() {
    let pool = pool();
    let id = 1i32;
    let result = bsql::query!(
        "SELECT t.id as ticket_id, t.title, u.login as creator
         FROM tickets t
         JOIN users u ON u.id = t.created_by_user_id
         WHERE t.id = $id: i32"
    )
    .fetch_one(&pool)
    .unwrap();
    assert_eq!(result.ticket_id, 1);
    assert_eq!(result.title, "Fix login bug");
    assert_eq!(result.creator, "alice");
}

#[test]
fn select_with_cte() {
    let pool = pool();
    let results = bsql::query!(
        "WITH active_users AS (
            SELECT id, login FROM users WHERE active = true
        )
        SELECT id, login FROM active_users ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].login, "alice");
}

#[test]
fn fetch_all_empty_result() {
    let pool = pool();
    let login = "absolutely_nobody_has_this_login";
    let results = bsql::query!("SELECT id, login FROM users WHERE login = $login: &str")
        .fetch_all(&pool)
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn select_expression_arithmetic() {
    let pool = pool();
    let result = bsql::query!("SELECT 1 + 1 as sum_val")
        .fetch_one(&pool)
        .unwrap();
    // Computed expression -> nullable by default
    assert_eq!(result.sum_val, Some(2i32));
}

#[test]
fn insert_on_conflict_do_nothing() {
    let pool = pool();
    // alice already exists -- ON CONFLICT DO NOTHING returns 0 affected
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
    .unwrap();
    assert_eq!(affected, 0);
}

#[test]
fn delete_returning() {
    let pool = pool();
    let title = "Ticket for RETURNING test";
    let uid = 1i32;
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&pool)
    .unwrap();

    let id = ticket.id;
    let deleted = bsql::query!("DELETE FROM tickets WHERE id = $id: i32 RETURNING id, title")
        .fetch_all(&pool)
        .unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].id, id);
}

#[test]
fn param_reuse_in_real_query() {
    let pool = pool();
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32 AND id = $id: i32")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(user.id, 1);
}

#[test]
fn fetch_optional_multiple_rows_errors() {
    let pool = pool();
    // users table has 2+ rows with active=true -- fetch_optional must error
    let result = bsql::query!("SELECT id, login FROM users WHERE active = true")
        .fetch_optional(&pool);

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

#[test]
fn bytea_column_round_trip() {
    let pool = pool();
    let avatar: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
    let id = 1i32;
    // Set avatar
    bsql::query!("UPDATE users SET avatar = $avatar: &[u8] WHERE id = $id: i32")
        .execute(&pool)
        .unwrap();

    // Read it back
    let user = bsql::query!("SELECT id, avatar FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .unwrap();

    assert_eq!(user.id, 1);
    assert_eq!(user.avatar.as_deref(), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
}

#[test]
fn array_column_type() {
    let pool = pool();
    let id = 1i32;
    let user = bsql::query!("SELECT id, tag_ids FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .unwrap();

    assert_eq!(user.id, 1);
    assert!(user.tag_ids.is_empty()); // default '{}'
}

#[test]
fn connect_invalid_url() {
    let result = Pool::connect("not_a_url");
    assert!(result.is_err(), "invalid URL should fail");
}

#[test]
fn select_star() {
    let pool = pool();
    let id = 1i32;
    let user = bsql::query!("SELECT * FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(user.id, 1);
}

#[test]
fn pool_debug_format() {
    let pool = pool();
    let debug = format!("{:?}", pool);
    assert!(debug.contains("Pool"), "debug: {debug}");
    assert!(debug.contains("status"), "debug: {debug}");
}

#[test]
fn pool_builder_url_method() {
    let pool = Pool::builder()
        .url("postgres://bsql:bsql@localhost/bsql_test")
        .build()
        .unwrap();

    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&pool)
        .unwrap();
    assert!(users.len() >= 2);
}

// ---------------------------------------------------------------------------
// additional coverage: error variant matching
// ---------------------------------------------------------------------------

#[test]
fn fetch_one_multiple_rows_errors() {
    let pool = pool();
    let result = bsql::query!("SELECT id, login FROM users WHERE active = true")
        .fetch_one(&pool);

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

#[test]
fn pool_builder_max_size_and_status() {
    let pool = Pool::builder()
        .url("postgres://bsql:bsql@localhost/bsql_test")
        .max_size(4)
        .build()
        .unwrap();

    let status = pool.status();
    assert_eq!(status.max_size, 4);
}

#[test]
fn pool_acquire_and_use() {
    let pool = pool();
    let conn = pool.acquire().unwrap();

    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&conn)
        .unwrap();
    assert_eq!(user.id, 1);
}

#[test]
fn pool_builder_bad_url_errors() {
    let result = Pool::builder().url("not_a_url").build();
    assert!(result.is_err());
}

#[test]
fn execute_returns_zero_for_no_match() {
    let pool = pool();
    let id = 999999i32;
    let affected = bsql::query!("UPDATE tickets SET description = 'x' WHERE id = $id: i32")
        .execute(&pool)
        .unwrap();
    assert_eq!(affected, 0);
}

#[test]
fn error_display_format() {
    let pool = pool();
    let id = 999999i32;
    let err = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
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

#[test]
fn warmup_prepares_statements() {
    let pool = pool();
    pool.set_warmup_sqls(&["SELECT id, login FROM users WHERE id = $1::int4"]);
    // Acquire forces warmup on the new connection -- the statement is prepared
    // via Parse+Describe+Sync (no Bind+Execute). Subsequent queries using the
    // same SQL skip the Parse round-trip because the statement is already cached.
    let conn = pool.acquire().unwrap();
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&conn)
        .unwrap();
    assert_eq!(user.id, 1);
    assert_eq!(user.login, "alice");
}

// ---------------------------------------------------------------------------
// T-1: basic streaming integration test
// ---------------------------------------------------------------------------

#[test]
fn fetch_stream_basic() {
    let pool = pool();
    let mut stream = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_stream(&pool)
        .unwrap();

    let mut count = 0;
    while let Some(user) = stream.next().unwrap() {
        count += 1;
        assert!(!user.login.is_empty());
    }
    assert!(count >= 2, "expected at least 2 users, got {count}");
}

// ---------------------------------------------------------------------------
// T-4: streaming with bind parameters
// ---------------------------------------------------------------------------

#[test]
fn fetch_stream_with_bind_params() {
    let pool = pool();
    let active = true;
    let mut stream =
        bsql::query!("SELECT id, login FROM users WHERE active = $active: bool ORDER BY id")
            .fetch_stream(&pool)
            .unwrap();

    let mut count = 0;
    while let Some(user) = stream.next().unwrap() {
        count += 1;
        assert!(!user.login.is_empty());
    }
    assert!(count >= 2, "expected at least 2 active users, got {count}");
}

// ---------------------------------------------------------------------------
// T-10: streaming drop mid-iteration (connection should not be returned
//       to pool in broken state)
// ---------------------------------------------------------------------------

#[test]
fn fetch_stream_drop_mid_iteration() {
    let pool = pool();

    // Open a stream and drop it after reading only the first row.
    {
        let mut stream = bsql::query!("SELECT id, login FROM users ORDER BY id")
            .fetch_stream(&pool)
            .unwrap();

        let first = stream.next().unwrap();
        assert!(first.is_some(), "should have at least one row");
        // stream dropped here -- connection discarded (not returned to pool)
    }

    // The pool should still be usable: a new connection can be acquired and
    // queries succeed without protocol-state corruption.
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(user.id, 1);
}

// ---------------------------------------------------------------------------
// T-10b: streaming fully consumed leaves pool healthy
// ---------------------------------------------------------------------------

#[test]
fn fetch_stream_fully_consumed() {
    let pool = pool();

    {
        let mut stream = bsql::query!("SELECT id, login FROM users ORDER BY id")
            .fetch_stream(&pool)
            .unwrap();

        while let Some(_user) = stream.next().unwrap() {}
        // stream dropped after full consumption -- connection returned to pool
    }

    // Pool should work fine after a fully consumed stream.
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .unwrap();
    assert_eq!(user.id, 1);
}
