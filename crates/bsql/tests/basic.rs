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
    // COUNT(*) is a computed column -- should be i64, nullable by default
    let pool = pool().await;
    let result = bsql::query!("SELECT COUNT(*) as cnt FROM users")
        .fetch_one(&pool)
        .await
        .unwrap();
    // COUNT(*) is correctly inferred as NOT NULL — returns i64, not Option<i64>
    assert!(result.cnt >= 2);
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
    // Computed expression -> nullable by default
    assert_eq!(result.sum_val, Some(2i32));
}

#[tokio::test]
async fn insert_on_conflict_do_nothing() {
    let pool = pool().await;
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
    // users table has 2+ rows with active=true -- fetch_optional must error
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
    assert_eq!(user.avatar, Some(vec![0xDE, 0xAD, 0xBE, 0xEF]));
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
    let mut conn = pool.acquire().await.unwrap();

    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(user.id, 1);
}

#[tokio::test]
async fn pool_acquire_execute() {
    let pool = pool().await;
    let mut conn = pool.acquire().await.unwrap();

    let desc = "via conn";
    let id = 1i32;
    let affected = bsql::query!("UPDATE tickets SET description = $desc: &str WHERE id = $id: i32")
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(affected, 1);
}

#[tokio::test]
async fn conn_execute_insert_and_delete() {
    let pool = pool().await;
    let mut conn = pool.acquire().await.unwrap();

    let title = "conn_insert_test";
    let uid = 1i32;
    let inserted = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ($title: &str, 'new', $uid: i32)"
    )
    .execute(&mut conn)
    .await
    .unwrap();
    assert_eq!(inserted, 1);

    let title2 = "conn_insert_test";
    let deleted = bsql::query!("DELETE FROM tickets WHERE title = $title2: &str")
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(deleted, 1);
}

#[tokio::test]
async fn conn_execute_returns_zero_for_no_match() {
    let pool = pool().await;
    let mut conn = pool.acquire().await.unwrap();

    let id = 999999i32;
    let affected = bsql::query!("UPDATE tickets SET description = 'x' WHERE id = $id: i32")
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(affected, 0);
}

// ---------------------------------------------------------------------------
// .execute() — affected > 1
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_update_multiple_rows() {
    let pool = pool().await;

    // Insert 3 tickets with a unique marker title
    let title = "batch_multi_test";
    let uid = 1i32;
    for _ in 0..3 {
        bsql::query!(
            "INSERT INTO tickets (title, status, created_by_user_id)
             VALUES ($title: &str, 'new', $uid: i32)"
        )
        .execute(&pool)
        .await
        .unwrap();
    }

    // UPDATE all three at once
    let title2 = "batch_multi_test";
    let affected =
        bsql::query!("UPDATE tickets SET description = 'batched' WHERE title = $title2: &str")
            .execute(&pool)
            .await
            .unwrap();
    assert_eq!(affected, 3);

    // DELETE all three at once
    let title3 = "batch_multi_test";
    let deleted = bsql::query!("DELETE FROM tickets WHERE title = $title3: &str")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(deleted, 3);
}

// ---------------------------------------------------------------------------
// .execute() — constraint violations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_unique_constraint_violation() {
    let pool = pool().await;

    // "alice" already exists in seed data — UNIQUE on users.login
    let login = "alice";
    let result = bsql::query!(
        "INSERT INTO users (login, first_name, last_name, email)
         VALUES ($login: &str, 'Dup', 'User', 'dup@example.com')"
    )
    .execute(&pool)
    .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.is_unique_violation(),
        "expected unique violation, got: {err:?}"
    );
}

#[tokio::test]
async fn execute_foreign_key_violation() {
    let pool = pool().await;

    // user_id 999999 does not exist — FK on tickets.created_by_user_id
    let bad_uid = 999999i32;
    let result = bsql::query!(
        "INSERT INTO tickets (title, status, created_by_user_id)
         VALUES ('fk_test', 'new', $bad_uid: i32)"
    )
    .execute(&pool)
    .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.is_foreign_key_violation(),
        "expected FK violation, got: {err:?}"
    );
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
    pool.set_warmup_sqls(["SELECT id, login FROM users WHERE id = $1::int4"]);
    // Acquire forces warmup on the new connection -- the statement is prepared
    // via Parse+Describe+Sync (no Bind+Execute). Subsequent queries using the
    // same SQL skip the Parse round-trip because the statement is already cached.
    let mut conn = pool.acquire().await.unwrap();
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&mut conn)
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
        // stream dropped here -- connection discarded (not returned to pool)
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
        // stream dropped after full consumption -- connection returned to pool
    }

    // Pool should work fine after a fully consumed stream.
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.id, 1);
}

// ---------------------------------------------------------------------------
// raw_query / raw_execute
// ---------------------------------------------------------------------------

#[tokio::test]
async fn raw_query_returns_rows() {
    let pool = pool().await;
    let rows = pool.raw_query("SELECT 1 AS n").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some("1"));
}

#[tokio::test]
async fn raw_execute_creates_table() {
    let pool = pool().await;

    // Create a temp table via raw_execute.
    pool.raw_execute("CREATE TEMP TABLE _raw_exec_test (val int)")
        .await
        .unwrap();

    // Verify the table exists by inserting and querying.
    pool.raw_execute("INSERT INTO _raw_exec_test VALUES (42)")
        .await
        .unwrap();
    let rows = pool
        .raw_query("SELECT val FROM _raw_exec_test")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some("42"));
}

#[tokio::test]
async fn raw_query_empty_result() {
    let pool = pool().await;
    let rows = pool.raw_query("SELECT 1 WHERE false").await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn raw_query_syntax_error() {
    let pool = pool().await;
    let result = pool.raw_query("SELECTTTT").await;
    assert!(result.is_err(), "syntax error should return Err");
}

// ---------------------------------------------------------------------------
// QueryStream low-level tests (advance + next_row pattern)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetch_stream_iterate_all() {
    let pool = pool().await;
    let sql = "SELECT generate_series(1, 200)::int4 AS n";
    let h = bsql::driver::hash_sql(sql);
    let params: &[&(dyn bsql::driver::Encode + Sync)] = &[];

    let mut stream = pool.query_stream(sql, h, params).await.unwrap();
    let mut count = 0usize;
    while stream.advance().await.unwrap() {
        let row = stream.next_row().unwrap();
        let n = row.get_i32(0).unwrap();
        count += 1;
        assert_eq!(n, count as i32);
    }
    assert_eq!(count, 200);
}

#[tokio::test]
async fn fetch_stream_empty_result() {
    let pool = pool().await;
    let sql = "SELECT 1::int4 AS n WHERE false";
    let h = bsql::driver::hash_sql(sql);
    let params: &[&(dyn bsql::driver::Encode + Sync)] = &[];

    let mut stream = pool.query_stream(sql, h, params).await.unwrap();

    // advance() should immediately return false for an empty result
    let has_rows = stream.advance().await.unwrap();
    assert!(!has_rows, "empty result stream should have no rows");

    // next_row should return None
    assert!(stream.next_row().is_none());
}

#[tokio::test]
async fn fetch_stream_columns() {
    let pool = pool().await;
    let sql = "SELECT 1::int4 AS id, 'hello'::text AS name, true::bool AS active";
    let h = bsql::driver::hash_sql(sql);
    let params: &[&(dyn bsql::driver::Encode + Sync)] = &[];

    let stream = pool.query_stream(sql, h, params).await.unwrap();

    let columns = stream.columns();
    assert_eq!(columns.len(), 3);
    assert_eq!(&*columns[0].name, "id");
    assert_eq!(columns[0].type_oid, 23); // int4
    assert_eq!(&*columns[1].name, "name");
    assert_eq!(columns[1].type_oid, 25); // text
    assert_eq!(&*columns[2].name, "active");
    assert_eq!(columns[2].type_oid, 16); // bool
}

#[tokio::test]
async fn fetch_stream_single_row() {
    let pool = pool().await;
    let sql = "SELECT 42::int4 AS n";
    let h = bsql::driver::hash_sql(sql);
    let params: &[&(dyn bsql::driver::Encode + Sync)] = &[];

    let mut stream = pool.query_stream(sql, h, params).await.unwrap();
    assert!(stream.advance().await.unwrap());
    let row = stream.next_row().unwrap();
    assert_eq!(row.get_i32(0), Some(42));

    // No more rows
    assert!(!stream.advance().await.unwrap());
}

// ---------------------------------------------------------------------------
// LEFT JOIN nullability — right-side columns must be Option<T>
// ---------------------------------------------------------------------------

#[tokio::test]
async fn left_join_right_side_is_nullable() {
    let pool = pool().await;
    let uid = 1i32;

    // tickets LEFT JOIN users: users columns should be Option even though
    // users.login is NOT NULL in the table definition, because LEFT JOIN
    // can produce NULL when no matching row exists.
    let rows = bsql::query!(
        "SELECT t.id, u.login
         FROM tickets t
         LEFT JOIN users u ON u.id = $uid: i32 AND u.id = -1
         WHERE t.id = 1"
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    // u.id = -1 guarantees no match → u.login should be None.
    // bsql detects LEFT JOIN and forces all table-backed columns to Option<T>.
    assert!(!rows.is_empty());
    assert!(
        rows[0].login.is_none(),
        "LEFT JOIN with no match should produce NULL login"
    );
}

#[tokio::test]
async fn cast_on_not_null_column_is_not_null() {
    let pool = pool().await;
    let id = 1i32;

    // tickets.title is NOT NULL. title::text should also be NOT NULL.
    let ticket = bsql::query!(
        "SELECT id, title, title::text AS title_text
         FROM tickets WHERE id = $id: i32"
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // title_text should be String, not Option<String>
    assert!(!ticket.title_text.is_empty());
    assert_eq!(ticket.title, ticket.title_text);
}

// ---------------------------------------------------------------------------
// JSONB auto-cast — &str transparently cast to jsonb
// ---------------------------------------------------------------------------

#[tokio::test]
async fn jsonb_insert_and_select() {
    let pool = pool().await;

    // INSERT &str into jsonb column — bsql auto-casts to ::jsonb
    let data = r#"{"key": "value", "num": 42}"#;
    let row = bsql::query!("INSERT INTO test_jsonb (data) VALUES ($data: &str) RETURNING id")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(row.id > 0);

    // SELECT jsonb column — returns String
    let id = row.id;
    let row = bsql::query!("SELECT data FROM test_jsonb WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(row.data.contains("key"));

    // Clean up
    bsql::query!("DELETE FROM test_jsonb WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn jsonb_nullable_column() {
    let pool = pool().await;

    // meta is JSONB nullable — insert without it
    let data = r#"{"test": true}"#;
    let row = bsql::query!("INSERT INTO test_jsonb (data) VALUES ($data: &str) RETURNING id, meta")
        .fetch_one(&pool)
        .await
        .unwrap();

    // meta should be None (Option<String>)
    assert!(row.meta.is_none());

    let id = row.id;
    bsql::query!("DELETE FROM test_jsonb WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn jsonb_invalid_json_returns_error() {
    let pool = pool().await;

    // Invalid JSON should produce PG error, not panic
    let data = "not valid json {{{";
    let result = bsql::query!("INSERT INTO test_jsonb (data) VALUES ($data: &str) RETURNING id")
        .fetch_one(&pool)
        .await;

    assert!(result.is_err(), "invalid JSON should fail");
}

// ---------------------------------------------------------------------------
// Array params — unnest and ANY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn array_param_with_any() {
    let pool = pool().await;

    let ids = vec![1i32, 2];
    let rows =
        bsql::query!("SELECT id, login FROM users WHERE id = ANY($ids: Vec<i32>) ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].login, "alice");
    assert_eq!(rows[1].login, "bob");
}

// ---------------------------------------------------------------------------
// JSON (not JSONB) — same transparent auto-cast, same tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_insert_and_select() {
    let pool = pool().await;

    let notes = r#"{"note": "hello"}"#;
    let data = r#"{"x": 1}"#;
    let row = bsql::query!(
        "INSERT INTO test_jsonb (data, notes) VALUES ($data: &str, $notes: &str) RETURNING id"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(row.id > 0);

    let id = row.id;
    let row = bsql::query!("SELECT notes FROM test_jsonb WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    // notes is JSON (nullable) → Option<String>
    assert!(row.notes.is_some());
    assert!(row.notes.unwrap().contains("hello"));

    bsql::query!("DELETE FROM test_jsonb WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn json_null_column() {
    let pool = pool().await;

    let data = r#"{"x": 1}"#;
    let row =
        bsql::query!("INSERT INTO test_jsonb (data) VALUES ($data: &str) RETURNING id, notes")
            .fetch_one(&pool)
            .await
            .unwrap();

    assert!(row.notes.is_none());

    let id = row.id;
    bsql::query!("DELETE FROM test_jsonb WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn json_invalid_returns_error() {
    let pool = pool().await;

    let data = r#"{"valid": true}"#;
    let notes = "not json!!!";
    let result = bsql::query!(
        "INSERT INTO test_jsonb (data, notes) VALUES ($data: &str, $notes: &str) RETURNING id"
    )
    .fetch_one(&pool)
    .await;

    assert!(result.is_err(), "invalid JSON in json column should fail");
}

// ---------------------------------------------------------------------------
// Auto-deref: String → &str, Vec<T> → &[T]
// ---------------------------------------------------------------------------

#[tokio::test]
async fn string_variable_accepted_as_str_param() {
    let pool = pool().await;
    // `login` is String, param declared as &str — auto-deref should work
    let login: String = "alice".to_owned();
    let user = bsql::query!("SELECT id, login FROM users WHERE login = $login: &str")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.login, "alice");
}

#[tokio::test]
async fn vec_variable_accepted_as_slice_param() {
    let pool = pool().await;
    // `ids` is Vec<i32>, param declared as &[i32] — auto-deref should work
    let ids: Vec<i32> = vec![1, 2];
    let rows = bsql::query!("SELECT id, login FROM users WHERE id = ANY($ids: &[i32]) ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
}

// ---------------------------------------------------------------------------
// Option<T> nullable parameters
// ---------------------------------------------------------------------------

#[tokio::test]
async fn option_param_none_inserts_null() {
    let pool = pool().await;
    let data = r#"{"test": true}"#;
    let meta: Option<&str> = None;
    let row = bsql::query!(
        "INSERT INTO test_jsonb (data, meta) VALUES ($data: &str, $meta: Option<&str>) RETURNING id, meta"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(row.meta.is_none(), "None should insert NULL");

    let id = row.id;
    bsql::query!("DELETE FROM test_jsonb WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn option_param_some_inserts_value() {
    let pool = pool().await;
    let data = r#"{"test": true}"#;
    let meta: Option<&str> = Some(r#"{"source": "test"}"#);
    let row = bsql::query!(
        "INSERT INTO test_jsonb (data, meta) VALUES ($data: &str, $meta: Option<&str>) RETURNING id, meta"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(row.meta.is_some(), "Some should insert value");
    assert!(row.meta.unwrap().contains("source"));

    let id = row.id;
    bsql::query!("DELETE FROM test_jsonb WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn option_i32_param_none_and_some() {
    let pool = pool().await;
    // description is nullable TEXT — test with Option<&str>
    let desc: Option<&str> = None;
    let id = 1i32;
    let affected =
        bsql::query!("UPDATE tickets SET description = $desc: Option<&str> WHERE id = $id: i32")
            .execute(&pool)
            .await
            .unwrap();
    assert_eq!(affected, 1);

    // Verify NULL was set
    let ticket = bsql::query!("SELECT description FROM tickets WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(ticket.description.is_none());

    // Now set a value
    let desc: Option<&str> = Some("restored");
    bsql::query!("UPDATE tickets SET description = $desc: Option<&str> WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();

    let ticket = bsql::query!("SELECT description FROM tickets WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(ticket.description.as_deref(), Some("restored"));
}

// ---------------------------------------------------------------------------
// raw_query_params — dynamic SQL with parameter binding
// ---------------------------------------------------------------------------

#[tokio::test]
async fn raw_query_params_basic() {
    let pool = pool().await;
    let rows = pool
        .raw_query_params(
            "SELECT id, login FROM users WHERE id = $1 ORDER BY id",
            &[&1i32 as &(dyn bsql::driver::Encode + Sync)],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(1), Some("alice"));
}

#[tokio::test]
async fn raw_query_params_multiple() {
    let pool = pool().await;
    let rows = pool
        .raw_query_params(
            "SELECT id FROM users WHERE id = ANY($1) ORDER BY id",
            &[&vec![1i32, 2] as &(dyn bsql::driver::Encode + Sync)],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn raw_query_params_no_params() {
    let pool = pool().await;
    let rows = pool.raw_query_params("SELECT 1 AS n", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some("1"));
}

// ---------------------------------------------------------------------------
// &[String] as parameter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn slice_of_string_as_param() {
    let pool = pool().await;
    let logins: Vec<String> = vec!["alice".to_owned(), "bob".to_owned()];
    let rows = bsql::query!(
        "SELECT id, login FROM users WHERE login = ANY($logins: &[String]) ORDER BY id"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].login, "alice");
    assert_eq!(rows[1].login, "bob");
}

// ---------------------------------------------------------------------------
// Option<i32> on integer nullable column
// ---------------------------------------------------------------------------

#[tokio::test]
async fn option_i32_none_sets_null_on_integer_column() {
    let pool = pool().await;
    let dept: Option<i32> = None;
    let id = 1i32;
    bsql::query!("UPDATE tickets SET department_id = $dept: Option<i32> WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();

    let ticket = bsql::query!("SELECT department_id FROM tickets WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(ticket.department_id.is_none());
}

#[tokio::test]
async fn option_i32_some_sets_value_on_integer_column() {
    let pool = pool().await;
    let dept: Option<i32> = Some(42);
    let id = 1i32;
    bsql::query!("UPDATE tickets SET department_id = $dept: Option<i32> WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();

    let ticket = bsql::query!("SELECT department_id FROM tickets WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(ticket.department_id, Some(42));

    // Clean up — reset to NULL
    let dept: Option<i32> = None;
    bsql::query!("UPDATE tickets SET department_id = $dept: Option<i32> WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// raw_query_params edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn raw_query_params_insert_and_delete() {
    let pool = pool().await;
    pool.raw_query_params(
        "INSERT INTO test_jsonb (data) VALUES ($1::jsonb)",
        &[&r#"{"raw": true}"# as &(dyn bsql::driver::Encode + Sync)],
    )
    .await
    .unwrap();

    let rows = pool
        .raw_query_params(
            "SELECT data FROM test_jsonb WHERE data->>'raw' = $1",
            &[&"true" as &(dyn bsql::driver::Encode + Sync)],
        )
        .await
        .unwrap();
    assert!(!rows.is_empty());

    pool.raw_query_params(
        "DELETE FROM test_jsonb WHERE data->>'raw' = $1",
        &[&"true" as &(dyn bsql::driver::Encode + Sync)],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn raw_query_params_invalid_sql_returns_error() {
    let pool = pool().await;
    let result = pool
        .raw_query_params(
            "SELECT FROM nonexistent_xyz WHERE id = $1",
            &[&1i32 as &(dyn bsql::driver::Encode + Sync)],
        )
        .await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// raw_execute: SET commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn raw_execute_set_search_path() {
    let pool = pool().await;
    // SET is a session-level command, not a query. raw_execute handles it.
    pool.raw_execute("SET search_path TO public").await.unwrap();
}

// ---------------------------------------------------------------------------
// raw_query_params: NULL parameter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn raw_query_params_with_null_param() {
    let pool = pool().await;
    // Pass NULL as a parameter — the driver should send -1 length, PG should
    // handle it as NULL. Use Option<i32> = None via the Encode trait.
    let null_id: Option<i32> = None;
    let rows = pool
        .raw_query_params(
            "SELECT $1::int4 IS NULL AS is_null",
            &[&null_id as &(dyn bsql::driver::Encode + Sync)],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some("t")); // PG returns "t" for true in text protocol
}

// ---------------------------------------------------------------------------
// raw_execute: full schema isolation pattern (the test-infrastructure pattern)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn raw_execute_schema_isolation_lifecycle() {
    let pool = pool().await;
    let schema = format!("bsql_test_iso_{}", std::process::id());

    // 1. Create isolated schema
    pool.raw_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .unwrap();

    // 2. Set search path to the new schema
    pool.raw_execute(&format!("SET search_path TO \"{schema}\", public"))
        .await
        .unwrap();

    // 3. Create a table inside the isolated schema
    pool.raw_execute(&format!(
        "CREATE TABLE \"{schema}\".iso_test (id int PRIMARY KEY, val text)"
    ))
    .await
    .unwrap();

    // 4. Insert data through the schema
    pool.raw_execute(&format!(
        "INSERT INTO \"{schema}\".iso_test (id, val) VALUES (1, 'hello')"
    ))
    .await
    .unwrap();

    // 5. Query back — verify the table is accessible
    let rows = pool
        .raw_query(&format!(
            "SELECT val FROM \"{schema}\".iso_test WHERE id = 1"
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some("hello"));

    // 6. Drop schema cascade — clean up everything in one shot
    pool.raw_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .unwrap();

    // 7. Verify schema is gone — querying it should fail
    let result = pool
        .raw_query(&format!("SELECT 1 FROM \"{schema}\".iso_test"))
        .await;
    assert!(
        result.is_err(),
        "schema should be dropped, query should fail"
    );

    // Reset search path to default
    pool.raw_execute("SET search_path TO public").await.unwrap();
}

// ---------------------------------------------------------------------------
// &[String] edge case: empty array
// ---------------------------------------------------------------------------

#[tokio::test]
async fn empty_string_array_param() {
    let pool = pool().await;
    let logins: Vec<String> = vec![];
    let rows = bsql::query!(
        "SELECT id, login FROM users WHERE login = ANY($logins: &[String]) ORDER BY id"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(rows.is_empty(), "empty array should match no rows");
}

// ---------------------------------------------------------------------------
// query_as! with nullable columns — struct must use Option<T>
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct TicketRow {
    id: i32,
    description: Option<String>, // nullable column → Option
}

#[tokio::test]
async fn query_as_with_nullable_column() {
    let pool = pool().await;
    let id = 1i32;
    let ticket = bsql::query_as!(
        TicketRow,
        "SELECT id, description FROM tickets WHERE id = $id: i32"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(ticket.id, 1);
    // description may or may not be set
    let _ = ticket.description;
}

#[tokio::test]
async fn query_as_fetch_all_with_nullable() {
    let pool = pool().await;
    let rows = bsql::query_as!(
        TicketRow,
        "SELECT id, description FROM tickets ORDER BY id LIMIT 2"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
}

// ---------------------------------------------------------------------------
// QueryError construction from user code
// ---------------------------------------------------------------------------

#[test]
fn query_error_constructable_from_user_code() {
    use std::borrow::Cow;
    let err = BsqlError::Query(bsql::error::QueryError {
        message: Cow::Borrowed("test error"),
        pg_code: Some(Box::from("23505")),
        source: None,
    });
    assert!(err.is_unique_violation());
    assert!(err.to_string().contains("test error"));
}

#[test]
fn query_error_with_source() {
    use std::borrow::Cow;
    let io_err = std::io::Error::other("underlying");
    let err = BsqlError::Query(bsql::error::QueryError {
        message: Cow::Borrowed("wrapper"),
        pg_code: None,
        source: Some(Box::new(io_err)),
    });
    assert!(!err.is_unique_violation());
}

// ---------------------------------------------------------------------------
// for_each — zero-alloc iteration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn for_each_iterates_all_rows() {
    let pool = pool().await;
    let mut count = 0u32;
    bsql::query!("SELECT id, login FROM users ORDER BY id")
        .for_each(&pool, |row| {
            count += 1;
            assert!(!row.login.is_empty());
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn for_each_map_collects_results() {
    let pool = pool().await;
    let logins: Vec<String> = bsql::query!("SELECT login FROM users ORDER BY id")
        .for_each_map(&pool, |row| row.login.to_owned())
        .await
        .unwrap();
    assert_eq!(logins, vec!["alice", "bob"]);
}

#[tokio::test]
async fn for_each_empty_result() {
    let pool = pool().await;
    let mut count = 0u32;
    let name = "nonexistent_xyz";
    bsql::query!("SELECT id FROM users WHERE login = $name: &str")
        .for_each(&pool, |_row| {
            count += 1;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(count, 0);
}

// ---------------------------------------------------------------------------
// window functions — NOT NULL inference
// ---------------------------------------------------------------------------

#[tokio::test]
async fn window_function_row_number() {
    let pool = pool().await;
    let rows = bsql::query!(
        "SELECT id, login, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users ORDER BY id"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    // rn should be i64, NOT Option<i64> — ROW_NUMBER is always NOT NULL
    assert_eq!(rows[0].rn, 1i64);
    assert_eq!(rows[1].rn, 2i64);
}

// ---------------------------------------------------------------------------
// GROUP BY + aggregates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn group_by_with_count() {
    let pool = pool().await;
    let rows =
        bsql::query!("SELECT active, COUNT(*) AS cnt FROM users GROUP BY active ORDER BY active")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(!rows.is_empty());
    // cnt: i64 (COUNT is NOT NULL)
    assert!(rows[0].cnt > 0);
}

#[tokio::test]
async fn aggregate_sum_is_nullable() {
    let pool = pool().await;
    // SUM on empty group returns NULL — must be Option
    let row = bsql::query!("SELECT SUM(score) AS total FROM users WHERE login = 'nonexistent'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(row.total.is_none());
}

// ---------------------------------------------------------------------------
// subquery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn subquery_in_from() {
    let pool = pool().await;
    let rows =
        bsql::query!("SELECT sub.id, sub.login FROM (SELECT id, login FROM users ORDER BY id) sub")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(rows.len(), 2);
}

// ---------------------------------------------------------------------------
// UNION
// ---------------------------------------------------------------------------

#[tokio::test]
async fn union_all_query() {
    let pool = pool().await;
    let rows = bsql::query!(
        "SELECT login AS name FROM users WHERE id = 1
         UNION ALL
         SELECT title AS name FROM tickets WHERE id = 1"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
}

// ---------------------------------------------------------------------------
// extreme values — empty string vs NULL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn extreme_value_empty_string() {
    let pool = pool().await;
    let desc: Option<&str> = Some("");
    let id = 1i32;
    bsql::query!("UPDATE tickets SET description = $desc: Option<&str> WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
    let ticket = bsql::query!("SELECT description FROM tickets WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    // Empty string is NOT NULL — should be Some("")
    assert_eq!(ticket.description, Some(String::new()));
    // Restore
    let desc: Option<&str> = None;
    bsql::query!("UPDATE tickets SET description = $desc: Option<&str> WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Encoding edge cases — boundary values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn boundary_i16_max() {
    let pool = pool().await;
    let score = i16::MAX;
    let affected = bsql::query!("UPDATE users SET score = $score: i16 WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(affected, 1);
    let user = bsql::query!("SELECT score FROM users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.score, i16::MAX);
    // Restore
    let score = 42i16;
    bsql::query!("UPDATE users SET score = $score: i16 WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn boundary_f64_nan() {
    let pool = pool().await;
    let rating = f64::NAN;
    bsql::query!("UPDATE users SET balance = $rating: f64 WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
    let user = bsql::query!("SELECT balance FROM users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(user.balance.is_nan());
    // Restore
    let rating = 100.50f64;
    bsql::query!("UPDATE users SET balance = $rating: f64 WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn boundary_f64_infinity() {
    let pool = pool().await;
    let val = f64::INFINITY;
    bsql::query!("UPDATE users SET balance = $val: f64 WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
    let user = bsql::query!("SELECT balance FROM users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(user.balance.is_infinite() && user.balance.is_sign_positive());
    // Restore
    let val = 100.50f64;
    bsql::query!("UPDATE users SET balance = $val: f64 WHERE id = 1")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn unicode_text_roundtrip() {
    let pool = pool().await;
    let title = "unicode_test_row";
    let uid = 1i32;
    let row = bsql::query!(
        "INSERT INTO tickets (title, description, status, created_by_user_id)
         VALUES ($title: &str, 'Привет мир 🎉 中文 العربية', 'new', $uid: i32)
         RETURNING id, description"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        row.description.as_deref(),
        Some("Привет мир 🎉 中文 العربية")
    );
    let id = row.id;
    bsql::query!("DELETE FROM tickets WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn empty_bytea_not_null() {
    let pool = pool().await;
    let avatar: Option<&[u8]> = Some(&[]);
    let id = 1i32;
    bsql::query!("UPDATE users SET avatar = $avatar: Option<&[u8]> WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
    let user = bsql::query!("SELECT avatar FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    // Empty bytea is Some(vec![]), not None
    assert_eq!(user.avatar, Some(vec![]));
    // Restore
    let avatar: Option<&[u8]> = None;
    bsql::query!("UPDATE users SET avatar = $avatar: Option<&[u8]> WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Security tests — credential leak prevention
// ---------------------------------------------------------------------------

#[test]
fn password_not_in_config_debug() {
    let config =
        bsql::driver::Config::from_url("postgres://user:supersecret@localhost/db").unwrap();
    let debug = format!("{:?}", config);
    assert!(
        !debug.contains("supersecret"),
        "password must not appear in Debug: {debug}"
    );
}

#[tokio::test]
async fn password_not_in_connection_error() {
    // Pool creation is lazy — the error surfaces on acquire/query, not connect.
    let pool = bsql::PgPool::connect("postgres://user:supersecret@127.0.0.1:1/db")
        .await
        .unwrap();
    let id = 1i32;
    let result = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await;
    // Connection to port 1 should fail
    assert!(result.is_err(), "connection to port 1 should fail");
    let msg = format!("{}", result.unwrap_err());
    assert!(!msg.contains("supersecret"), "password in error: {msg}");
}

// ---------------------------------------------------------------------------
// Additional SQL construct tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_like_with_param() {
    let pool = pool().await;
    let pattern = "%ali%";
    let rows = bsql::query!("SELECT id, login FROM users WHERE login LIKE $pattern: &str")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].login, "alice");
}

#[tokio::test]
async fn sql_between_with_params() {
    let pool = pool().await;
    let low = 1i32;
    let high = 2i32;
    let rows =
        bsql::query!("SELECT id FROM users WHERE id BETWEEN $low: i32 AND $high: i32 ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn sql_is_null_in_where() {
    let pool = pool().await;
    let rows = bsql::query!("SELECT id FROM tickets WHERE description IS NULL")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn sql_is_not_null_in_where() {
    let pool = pool().await;
    let rows = bsql::query!("SELECT id FROM users WHERE middle_name IS NOT NULL")
        .fetch_all(&pool)
        .await
        .unwrap();
    // All seed users have NULL middle_name
    assert!(rows.is_empty());
}

#[tokio::test]
async fn sql_coalesce_in_select() {
    let pool = pool().await;
    let rows =
        bsql::query!("SELECT id, COALESCE(middle_name, 'N/A') AS middle FROM users ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].middle, "N/A");
}

#[tokio::test]
async fn sql_case_when_in_select() {
    let pool = pool().await;
    let rows = bsql::query!(
        "SELECT id, CASE WHEN active THEN 'yes' ELSE 'no' END AS status FROM users ORDER BY id"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].status, "yes");
}

// ---------------------------------------------------------------------------
// SQL construct coverage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_right_join() {
    let pool = pool().await;
    // RIGHT JOIN: left side nullable
    let rows = bsql::query!(
        "SELECT u.id, t.title FROM tickets t RIGHT JOIN users u ON u.id = t.created_by_user_id ORDER BY u.id"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(rows.len() >= 2);
}

#[tokio::test]
async fn sql_cross_join() {
    let pool = pool().await;
    let rows = bsql::query!(
        "SELECT u.login, t.title FROM users u CROSS JOIN tickets t ORDER BY u.id, t.id LIMIT 4"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn sql_self_join() {
    let pool = pool().await;
    let rows = bsql::query!(
        "SELECT a.login AS login_a, b.login AS login_b FROM users a JOIN users b ON a.id != b.id"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2); // alice-bob and bob-alice
}

#[tokio::test]
async fn sql_multiple_joins() {
    let pool = pool().await;
    let rows = bsql::query!(
        "SELECT t.id, t.title, u.login
         FROM tickets t
         JOIN users u ON u.id = t.created_by_user_id
         ORDER BY t.id LIMIT 2"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn sql_exists_subquery() {
    let pool = pool().await;
    let rows = bsql::query!(
        "SELECT id, login FROM users WHERE EXISTS (SELECT 1 FROM tickets WHERE created_by_user_id = users.id)"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(rows.len() >= 2);
}

#[tokio::test]
async fn sql_in_subquery() {
    let pool = pool().await;
    let rows = bsql::query!(
        "SELECT id, login FROM users WHERE id IN (SELECT created_by_user_id FROM tickets)"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(rows.len() >= 2);
}

#[tokio::test]
async fn sql_group_by_having() {
    let pool = pool().await;
    let rows = bsql::query!(
        "SELECT created_by_user_id, COUNT(*) AS cnt FROM tickets GROUP BY created_by_user_id HAVING COUNT(*) >= 1"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn sql_count_distinct() {
    let pool = pool().await;
    let row =
        bsql::query!("SELECT COUNT(DISTINCT created_by_user_id) AS unique_creators FROM tickets")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(row.unique_creators >= 2);
}

#[tokio::test]
async fn sql_offset() {
    let pool = pool().await;
    let rows = bsql::query!("SELECT id, login FROM users ORDER BY id LIMIT 1 OFFSET 1")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].login, "bob");
}

#[tokio::test]
async fn sql_string_concatenation() {
    let pool = pool().await;
    let row =
        bsql::query!("SELECT first_name || ' ' || last_name AS full_name FROM users WHERE id = 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    // PG marks concatenation expressions as nullable (no column-level NOT NULL info)
    assert!(row.full_name.is_some());
    assert_eq!(row.full_name.unwrap(), "Alice Smith");
}

#[tokio::test]
async fn sql_arithmetic_expression() {
    let pool = pool().await;
    let row = bsql::query!("SELECT id, score * 2 AS double_score FROM users WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.id, 1);
}

#[tokio::test]
async fn sql_insert_on_conflict_do_nothing() {
    let pool = pool().await;
    // alice already exists — no params variant
    let affected = bsql::query!(
        "INSERT INTO users (login, first_name, last_name, email) VALUES ('alice', 'A', 'A', 'a@a.com') ON CONFLICT (login) DO NOTHING"
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(affected, 0);
}

#[tokio::test]
async fn sql_with_comments() {
    let pool = pool().await;
    let rows = bsql::query!(
        "SELECT id, login -- this is a comment
         FROM users
         /* multi-line
            comment */
         ORDER BY id"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
}

// ---------------------------------------------------------------------------
// Query execution edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn same_param_used_twice() {
    let pool = pool().await;
    let name = "alice";
    let rows =
        bsql::query!("SELECT id FROM users WHERE login = $name: &str OR first_name = $name: &str")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(!rows.is_empty());
}

// ---------------------------------------------------------------------------
// Feature interactions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dynamic_query_in_transaction() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let dept: Option<i32> = Some(999);
    let affected = bsql::query!(
        "UPDATE tickets SET description = 'dyn_tx'
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]"
    )
    .execute(&mut tx)
    .await
    .unwrap();
    assert_eq!(affected, 0); // no tickets in dept 999
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn fetch_one_then_execute_same_connection() {
    let pool = pool().await;
    let mut conn = pool.acquire().await.unwrap();

    // Read then write on same PoolConnection
    let id = 1i32;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(user.login, "alice");

    let desc = "conn_reuse_test";
    bsql::query!("UPDATE tickets SET description = $desc: &str WHERE id = $id: i32")
        .execute(&mut conn)
        .await
        .unwrap();

    // Restore
    let desc2: Option<&str> = None;
    bsql::query!("UPDATE tickets SET description = $desc2: Option<&str> WHERE id = $id: i32")
        .execute(&mut conn)
        .await
        .unwrap();
}

#[tokio::test]
async fn multiple_fetch_all_same_pool() {
    let pool = pool().await;
    // Two fetch_all calls in sequence — pool should handle connection reuse
    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    let tickets = bsql::query!("SELECT id, title FROM tickets ORDER BY id LIMIT 2")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(tickets.len(), 2);
}

#[tokio::test]
async fn option_param_in_insert_returning() {
    let pool = pool().await;
    let title = "opt_returning";
    let desc: Option<&str> = None;
    let uid = 1i32;
    let row = bsql::query!(
        "INSERT INTO tickets (title, description, status, created_by_user_id)
         VALUES ($title: &str, $desc: Option<&str>, 'new', $uid: i32)
         RETURNING id, description"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(row.description.is_none());

    let id = row.id;
    bsql::query!("DELETE FROM tickets WHERE id = $id: i32")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn fetch_all_then_fetch_one_interleaved() {
    let pool = pool().await;
    let users = bsql::query!("SELECT id, login FROM users ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(users.len(), 2);

    let id = users[0].id;
    let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.login, users[0].login);
}

#[tokio::test]
async fn execute_multiple_tables_same_tx() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();

    let desc = "multi_table_tx";
    let id = 1i32;
    bsql::query!("UPDATE tickets SET description = $desc: &str WHERE id = $id: i32")
        .execute(&mut tx)
        .await
        .unwrap();
    bsql::query!("UPDATE users SET score = 99 WHERE id = $id: i32")
        .execute(&mut tx)
        .await
        .unwrap();

    tx.rollback().await.unwrap();
}

// ---------------------------------------------------------------------------
// Concurrency (non-stress, quick)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_fetch_all_tokio_spawn() {
    let pool = pool().await;
    let pool = std::sync::Arc::new(pool);

    let mut handles = vec![];
    for _ in 0..5 {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let rows = bsql::query!("SELECT id, login FROM users ORDER BY id")
                .fetch_all(p.as_ref())
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn concurrent_execute_different_rows() {
    let pool = pool().await;
    let pool = std::sync::Arc::new(pool);

    // Insert temp rows
    for i in 0..5i32 {
        let title = format!("conc_exec_{i}");
        let uid = 1i32;
        bsql::query!(
            "INSERT INTO tickets (title, status, created_by_user_id) VALUES ($title: &str, 'new', $uid: i32)"
        )
        .execute(pool.as_ref())
        .await
        .unwrap();
    }

    // Concurrently delete each
    let mut handles = vec![];
    for i in 0..5i32 {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let title = format!("conc_exec_{i}");
            bsql::query!("DELETE FROM tickets WHERE title = $title: &str")
                .execute(p.as_ref())
                .await
                .unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// Remaining SQL constructs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_intersect() {
    let pool = pool().await;
    // Users who created tickets AND are active
    let rows = bsql::query!(
        "SELECT id FROM users WHERE active = true
         INTERSECT
         SELECT created_by_user_id AS id FROM tickets"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn sql_except() {
    let pool = pool().await;
    // All user IDs EXCEPT those who created tickets (should be empty with seed data)
    let rows = bsql::query!(
        "SELECT id FROM users
         EXCEPT
         SELECT created_by_user_id AS id FROM tickets"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    // Both seed users created tickets
    assert!(rows.is_empty());
}

#[tokio::test]
async fn sql_window_partition_by() {
    let pool = pool().await;
    let rows = bsql::query!(
        "SELECT id, created_by_user_id,
                ROW_NUMBER() OVER (PARTITION BY created_by_user_id ORDER BY id) AS rn
         FROM tickets ORDER BY id LIMIT 4"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(!rows.is_empty());
    // rn should be i64 (NOT NULL)
    assert!(rows[0].rn >= 1);
}

#[tokio::test]
async fn sql_multiple_ctes() {
    let pool = pool().await;
    let rows = bsql::query!(
        "WITH active_users AS (SELECT id, login FROM users WHERE active = true),
              user_tickets AS (SELECT created_by_user_id, COUNT(*) AS cnt FROM tickets GROUP BY created_by_user_id)
         SELECT au.login, ut.cnt
         FROM active_users au
         JOIN user_tickets ut ON au.id = ut.created_by_user_id
         ORDER BY au.login"
    ).fetch_all(&pool).await.unwrap();
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn sql_in_list_with_params() {
    let pool = pool().await;
    let id1 = 1i32;
    let id2 = 2i32;
    let rows =
        bsql::query!("SELECT id, login FROM users WHERE id IN ($id1: i32, $id2: i32) ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn sql_insert_on_conflict_do_update() {
    let pool = pool().await;
    // Insert alice — she already exists, so DO UPDATE changes email
    let affected = bsql::query!(
        "INSERT INTO users (login, first_name, last_name, email)
         VALUES ('alice', 'Alice', 'Smith', 'updated@example.com')
         ON CONFLICT (login) DO UPDATE SET email = EXCLUDED.email"
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(affected, 1);

    // Verify update
    let id = 1i32;
    let user = bsql::query!("SELECT email FROM users WHERE id = $id: i32")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(user.email, "updated@example.com");

    // Restore
    bsql::query!("UPDATE users SET email = 'alice@example.com' WHERE login = 'alice'")
        .execute(&pool)
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Recursive CTE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_recursive_cte() {
    let pool = pool().await;
    // Generate a series 1..5 using recursive CTE
    let rows = bsql::query!(
        "WITH RECURSIVE nums AS (
            SELECT 1 AS n
            UNION ALL
            SELECT n + 1 FROM nums WHERE n < 5
        )
        SELECT n FROM nums ORDER BY n"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0].n, 1);
    assert_eq!(rows[4].n, 5);
}

// ===========================================================================
// STRESS TESTS — run with: cargo test -- --ignored
// ===========================================================================

#[tokio::test]
#[ignore] // stress: inserts 10K rows, ~2-3 seconds
async fn stress_fetch_all_10k_rows() {
    let pool = pool().await;

    // Insert 10K rows
    for i in 0..10_000i32 {
        let title = format!("stress_row_{i}");
        let uid = 1i32;
        bsql::query!(
            "INSERT INTO tickets (title, status, created_by_user_id) VALUES ($title: &str, 'new', $uid: i32)"
        ).execute(&pool).await.unwrap();
    }

    // fetch_all should return all 10K + seed rows
    let search = "stress_row_%";
    let rows =
        bsql::query!("SELECT id, title FROM tickets WHERE title LIKE $search: &str ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(rows.len(), 10_000);

    // Cleanup
    bsql::query!("DELETE FROM tickets WHERE title LIKE $search: &str")
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore] // stress: for_each on 10K rows, verify constant-ish memory
async fn stress_for_each_10k_rows() {
    let insert_pool = pool().await;
    let query_pool = Pool::connect("postgres://bsql:bsql@localhost/bsql_test")
        .await
        .unwrap();

    // Insert 10K rows
    for i in 0..10_000i32 {
        let title = format!("fe_stress_{i}");
        let uid = 1i32;
        bsql::query!(
            "INSERT INTO tickets (title, status, created_by_user_id) VALUES ($title: &str, 'new', $uid: i32)"
        ).execute(&insert_pool).await.unwrap();
    }

    // for_each uses sync connections internally while execute uses async
    // connections — use a separate pool so we get fresh sync connection slots.
    let mut count = 0u64;
    let search = "fe_stress_%";
    bsql::query!("SELECT id, title FROM tickets WHERE title LIKE $search: &str")
        .for_each(&query_pool, |_row| {
            count += 1;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(count, 10_000);

    // Cleanup
    bsql::query!("DELETE FROM tickets WHERE title LIKE $search: &str")
        .execute(&query_pool)
        .await
        .unwrap();
}

// --- Option<T> type_oid for None (keyset pagination pattern) ---

#[tokio::test]
async fn option_param_is_null_or_pattern() {
    let pool = pool().await;

    // First page: seek_id is None → "$1 IS NULL" is true → returns all rows
    let seek_id: Option<i32> = None;
    let rows = bsql::query!(
        "SELECT id, login FROM users WHERE $seek_id: Option<i32> IS NULL OR id > $seek_id: Option<i32> ORDER BY id LIMIT 2"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(!rows.is_empty(), "should return rows when seek_id is None");
    let first_page_last = rows.last().unwrap().id;

    // Second page: seek_id is Some → "$1 IS NULL" is false → filters by id > seek_id
    let seek_id: Option<i32> = Some(first_page_last);
    let rows2 = bsql::query!(
        "SELECT id, login FROM users WHERE $seek_id: Option<i32> IS NULL OR id > $seek_id: Option<i32> ORDER BY id LIMIT 2"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    for row in &rows2 {
        assert!(
            row.id > first_page_last,
            "second page ids must be > seek_id"
        );
    }
}

// ---------------------------------------------------------------------------
// Binary COPY — bulk insert via PG binary protocol
// ---------------------------------------------------------------------------

#[tokio::test]
async fn copy_in_binary_basic() {
    let pool = pool().await;

    // Create temp table
    pool.raw_execute("CREATE TEMP TABLE _copy_bin_test (id int4 NOT NULL, score int4 NOT NULL)")
        .await
        .unwrap();

    // Bulk insert 3 rows via binary COPY
    let rows: Vec<Vec<&(dyn bsql::driver::Encode + Sync)>> = vec![
        vec![&1i32 as &(dyn bsql::driver::Encode + Sync), &100i32],
        vec![&2i32, &200i32],
        vec![&3i32, &300i32],
    ];
    let row_refs: Vec<&[&(dyn bsql::driver::Encode + Sync)]> =
        rows.iter().map(|r| r.as_slice()).collect();

    let count = pool
        .copy_in_binary("_copy_bin_test", &["id", "score"], &row_refs)
        .await
        .unwrap();
    assert_eq!(count, 3, "should insert 3 rows");

    // Verify data
    let result = pool
        .raw_query("SELECT id, score FROM _copy_bin_test ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].get(0), Some("1"));
    assert_eq!(result[0].get(1), Some("100"));
    assert_eq!(result[1].get(0), Some("2"));
    assert_eq!(result[1].get(1), Some("200"));
    assert_eq!(result[2].get(0), Some("3"));
    assert_eq!(result[2].get(1), Some("300"));
}

#[tokio::test]
async fn copy_in_binary_empty() {
    let pool = pool().await;

    pool.raw_execute("CREATE TEMP TABLE _copy_bin_empty (id int4 NOT NULL)")
        .await
        .unwrap();

    let rows: Vec<&[&(dyn bsql::driver::Encode + Sync)]> = vec![];
    let count = pool
        .copy_in_binary("_copy_bin_empty", &["id"], &rows)
        .await
        .unwrap();
    assert_eq!(count, 0, "empty COPY should insert 0 rows");
}

#[tokio::test]
async fn copy_in_binary_with_text() {
    let pool = pool().await;

    pool.raw_execute("CREATE TEMP TABLE _copy_bin_text (id int4 NOT NULL, name text NOT NULL)")
        .await
        .unwrap();

    let name1 = "alice";
    let name2 = "bob";
    let rows: Vec<Vec<&(dyn bsql::driver::Encode + Sync)>> = vec![
        vec![&1i32 as &(dyn bsql::driver::Encode + Sync), &name1],
        vec![&2i32, &name2],
    ];
    let row_refs: Vec<&[&(dyn bsql::driver::Encode + Sync)]> =
        rows.iter().map(|r| r.as_slice()).collect();

    let count = pool
        .copy_in_binary("_copy_bin_text", &["id", "name"], &row_refs)
        .await
        .unwrap();
    assert_eq!(count, 2);

    let result = pool
        .raw_query("SELECT name FROM _copy_bin_text ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result[0].get(0), Some("alice"));
    assert_eq!(result[1].get(0), Some("bob"));
}

#[tokio::test]
async fn copy_in_binary_with_nulls() {
    let pool = pool().await;

    pool.raw_execute("CREATE TEMP TABLE _copy_bin_nulls (id int4 NOT NULL, note text)")
        .await
        .unwrap();

    let note1: Option<&str> = Some("hello");
    let note2: Option<&str> = None; // NULL
    let rows: Vec<Vec<&(dyn bsql::driver::Encode + Sync)>> = vec![
        vec![&1i32 as &(dyn bsql::driver::Encode + Sync), &note1],
        vec![&2i32, &note2], // second row has NULL note
    ];
    let row_refs: Vec<&[&(dyn bsql::driver::Encode + Sync)]> =
        rows.iter().map(|r| r.as_slice()).collect();

    let count = pool
        .copy_in_binary("_copy_bin_nulls", &["id", "note"], &row_refs)
        .await
        .unwrap();
    assert_eq!(count, 2);

    let result = pool
        .raw_query("SELECT id, note FROM _copy_bin_nulls ORDER BY id")
        .await
        .unwrap();
    assert_eq!(result[0].get(1), Some("hello"));
    assert_eq!(result[1].get(1), None); // NULL
}

#[tokio::test]
async fn copy_in_binary_multiple_types() {
    let pool = pool().await;

    pool.raw_execute(
        "CREATE TEMP TABLE _copy_bin_types (a bool NOT NULL, b int8 NOT NULL, c float8 NOT NULL)",
    )
    .await
    .unwrap();

    let rows: Vec<Vec<&(dyn bsql::driver::Encode + Sync)>> = vec![
        vec![
            &true as &(dyn bsql::driver::Encode + Sync),
            &42i64,
            &3.14f64,
        ],
        vec![&false, &(-1i64), &0.0f64],
    ];
    let row_refs: Vec<&[&(dyn bsql::driver::Encode + Sync)]> =
        rows.iter().map(|r| r.as_slice()).collect();

    let count = pool
        .copy_in_binary("_copy_bin_types", &["a", "b", "c"], &row_refs)
        .await
        .unwrap();
    assert_eq!(count, 2);

    let result = pool
        .raw_query("SELECT a, b, c FROM _copy_bin_types ORDER BY b")
        .await
        .unwrap();
    assert_eq!(result[0].get(0), Some("f")); // false
    assert_eq!(result[0].get(1), Some("-1"));
    assert_eq!(result[1].get(0), Some("t")); // true
    assert_eq!(result[1].get(1), Some("42"));
}

#[tokio::test]
async fn copy_in_binary_column_count_mismatch() {
    let pool = pool().await;

    pool.raw_execute("CREATE TEMP TABLE _copy_bin_mismatch (id int4 NOT NULL, name text NOT NULL)")
        .await
        .unwrap();

    // Row has 1 column but table expects 2
    let rows: Vec<Vec<&(dyn bsql::driver::Encode + Sync)>> =
        vec![vec![&1i32 as &(dyn bsql::driver::Encode + Sync)]];
    let row_refs: Vec<&[&(dyn bsql::driver::Encode + Sync)]> =
        rows.iter().map(|r| r.as_slice()).collect();

    let result = pool
        .copy_in_binary("_copy_bin_mismatch", &["id", "name"], &row_refs)
        .await;
    assert!(
        result.is_err(),
        "column count mismatch should produce an error"
    );
}

#[tokio::test]
async fn copy_in_binary_large_batch() {
    let pool = pool().await;

    pool.raw_execute("CREATE TEMP TABLE _copy_bin_large (id int4 NOT NULL, val int4 NOT NULL)")
        .await
        .unwrap();

    // 10,000 rows — exceeds 64KB buffer, tests flush mid-stream
    let data: Vec<(i32, i32)> = (0..10_000).map(|i| (i, i * 10)).collect();
    let rows: Vec<Vec<&(dyn bsql::driver::Encode + Sync)>> = data
        .iter()
        .map(|(id, val)| vec![id as &(dyn bsql::driver::Encode + Sync), val as &_])
        .collect();
    let row_refs: Vec<&[&(dyn bsql::driver::Encode + Sync)]> =
        rows.iter().map(|r| r.as_slice()).collect();

    let count = pool
        .copy_in_binary("_copy_bin_large", &["id", "val"], &row_refs)
        .await
        .unwrap();
    assert_eq!(count, 10_000);

    // Spot check
    let result = pool
        .raw_query("SELECT COUNT(*) FROM _copy_bin_large")
        .await
        .unwrap();
    assert_eq!(result[0].get(0), Some("10000"));
}

// ---------------------------------------------------------------------------
// for_each — fixed-offset decode optimisation paths
// ---------------------------------------------------------------------------

#[tokio::test]
async fn for_each_all_fixed_columns() {
    // All columns are fixed-size NOT NULL → compile-time column offset path.
    let pool = pool().await;
    let mut count = 0u32;
    bsql::query!("SELECT id, active, score, balance FROM users ORDER BY id")
        .for_each(&pool, |row| {
            count += 1;
            match count {
                1 => {
                    assert_eq!(row.id, 1);
                    assert_eq!(row.active, true);
                    assert_eq!(row.score, 42i16);
                    assert!((row.balance - 100.50).abs() < f64::EPSILON);
                }
                2 => {
                    assert_eq!(row.id, 2);
                    assert_eq!(row.active, true);
                    assert_eq!(row.score, 7i16);
                    assert!((row.balance - 0.0).abs() < f64::EPSILON);
                }
                _ => panic!("unexpected row count {count}"),
            }
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn for_each_mixed_fixed_and_variable() {
    // id (i32 NOT NULL) gets fixed-offset decode; login (text NOT NULL) and
    // active (bool NOT NULL) fall back to sequential decode.
    let pool = pool().await;
    let mut count = 0u32;
    bsql::query!("SELECT id, login, active FROM users ORDER BY id")
        .for_each(&pool, |row| {
            count += 1;
            match count {
                1 => {
                    assert_eq!(row.id, 1);
                    assert_eq!(row.login, "alice");
                    assert_eq!(row.active, true);
                }
                2 => {
                    assert_eq!(row.id, 2);
                    assert_eq!(row.login, "bob");
                    assert_eq!(row.active, true);
                }
                _ => panic!("unexpected row count {count}"),
            }
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn for_each_no_fixed_prefix_nullable_first() {
    // First column is nullable text → no fixed-offset optimisation applies,
    // all columns decoded sequentially.
    let pool = pool().await;
    let mut count = 0u32;
    bsql::query!("SELECT middle_name, id FROM users ORDER BY id")
        .for_each(&pool, |row| {
            count += 1;
            match count {
                1 => {
                    assert!(row.middle_name.is_none());
                    assert_eq!(row.id, 1);
                }
                2 => {
                    assert!(row.middle_name.is_none());
                    assert_eq!(row.id, 2);
                }
                _ => panic!("unexpected row count {count}"),
            }
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn large_text_column_exceeds_stream_buffer() {
    // INSERT a 20KB text value, then SELECT it back via for_each.
    // Verifies the reduced 16KB stream_buf handles oversized messages.
    //
    // for_each uses sync connections while fetch_one/execute use async, so we
    // need a separate pool for the for_each call (same pattern as stress test).
    let insert_pool = pool().await;
    let query_pool = Pool::connect("postgres://bsql:bsql@localhost/bsql_test")
        .await
        .unwrap();

    let large_desc = "x".repeat(20_000); // 20KB text
    let title = "large_text_test";
    let uid = 1i32;

    // Insert a ticket with the large description.
    let ticket = bsql::query!(
        "INSERT INTO tickets (title, description, status, created_by_user_id)
         VALUES ($title: &str, $large_desc: &str, 'new', $uid: i32)
         RETURNING id"
    )
    .fetch_one(&insert_pool)
    .await
    .unwrap();

    let ticket_id = ticket.id;

    // Read it back through for_each to exercise the streaming path.
    let mut found = false;
    bsql::query!("SELECT id, description FROM tickets WHERE id = $ticket_id: i32")
        .for_each(&query_pool, |row| {
            assert_eq!(row.id, ticket_id);
            let desc = row.description.expect("description should not be NULL");
            assert_eq!(desc.len(), 20_000, "full 20KB text must round-trip");
            assert!(desc.chars().all(|c| c == 'x'));
            found = true;
            Ok(())
        })
        .await
        .unwrap();
    assert!(found, "for_each must visit the inserted row");

    // Clean up.
    bsql::query!("DELETE FROM tickets WHERE id = $ticket_id: i32")
        .execute(&insert_pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn for_each_int_array_column() {
    // tag_ids is INTEGER[] NOT NULL on the users table.
    // for_each should decode it as Vec<i32> (borrowed path).
    let pool = pool().await;
    let mut count = 0u32;
    bsql::query!("SELECT id, tag_ids FROM users ORDER BY id")
        .for_each(&pool, |row| {
            count += 1;
            // Both seeded users have the default empty array.
            assert_eq!(row.id, count as i32);
            assert!(row.tag_ids.is_empty(), "default tag_ids should be empty");
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(count, 2);
}
