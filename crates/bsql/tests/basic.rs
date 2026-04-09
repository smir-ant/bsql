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
