//! Integration tests for v0.3: dynamic queries (optional clauses).
//!
//! Requires a running PostgreSQL with the test schema.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::Pool;

async fn pool() -> Pool {
    Pool::connect("postgres://bsql:bsql@localhost/bsql_test")
        .await
        .expect("Failed to connect to test database. Is PostgreSQL running?")
}

// --- Single optional clause ---

#[tokio::test]
async fn one_optional_clause_some() {
    let pool = pool().await;
    let dept: Option<i32> = Some(1);
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch(&pool)
    .await
    .unwrap();

    // With dept=Some(1), only tickets in department 1 should be returned.
    // Our seed data doesn't have department_id set, so this should return 0.
    assert!(
        results.is_empty(),
        "no seed tickets have department_id=1: got {} rows",
        results.len()
    );
}

#[tokio::test]
async fn one_optional_clause_none() {
    let pool = pool().await;
    let dept: Option<i32> = None;
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch(&pool)
    .await
    .unwrap();

    // With dept=None, the clause is excluded -- returns all non-deleted tickets.
    assert!(
        results.len() >= 2,
        "expected at least 2 tickets, got {}",
        results.len()
    );
}

// --- Two optional clauses: all 4 combinations ---

#[tokio::test]
async fn two_optional_clauses_none_none() {
    let pool = pool().await;
    let dept: Option<i32> = None;
    let assignee: Option<i32> = None;
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         [AND assignee_id = $assignee: Option<i32>]
         ORDER BY id"
    )
    .fetch(&pool)
    .await
    .unwrap();

    assert!(
        results.len() >= 2,
        "both None -- should return all tickets, got {}",
        results.len()
    );
}

#[tokio::test]
async fn two_optional_clauses_some_none() {
    let pool = pool().await;
    let dept: Option<i32> = Some(999);
    let assignee: Option<i32> = None;
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         [AND assignee_id = $assignee: Option<i32>]
         ORDER BY id"
    )
    .fetch(&pool)
    .await
    .unwrap();

    assert!(
        results.is_empty(),
        "dept=999 -- should return 0 tickets, got {}",
        results.len()
    );
}

#[tokio::test]
async fn two_optional_clauses_none_some() {
    let pool = pool().await;
    let dept: Option<i32> = None;
    let assignee: Option<i32> = Some(999);
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         [AND assignee_id = $assignee: Option<i32>]
         ORDER BY id"
    )
    .fetch(&pool)
    .await
    .unwrap();

    assert!(
        results.is_empty(),
        "assignee=999 -- should return 0 tickets, got {}",
        results.len()
    );
}

#[tokio::test]
async fn two_optional_clauses_some_some() {
    let pool = pool().await;
    let dept: Option<i32> = Some(999);
    let assignee: Option<i32> = Some(999);
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         [AND assignee_id = $assignee: Option<i32>]
         ORDER BY id"
    )
    .fetch(&pool)
    .await
    .unwrap();

    assert!(
        results.is_empty(),
        "both=999 -- should return 0 tickets, got {}",
        results.len()
    );
}

// --- Optional clause with base required params ---

#[tokio::test]
async fn optional_clause_with_base_params() {
    let pool = pool().await;
    let uid = 1i32;
    let dept: Option<i32> = None;
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE created_by_user_id = $uid: i32
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch(&pool)
    .await
    .unwrap();

    // uid=1 (alice) has tickets. dept=None means no department filter.
    assert!(!results.is_empty(), "alice should have tickets, got 0");
}

#[tokio::test]
async fn optional_clause_with_base_params_filtered() {
    let pool = pool().await;
    let uid = 1i32;
    let dept: Option<i32> = Some(999);
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE created_by_user_id = $uid: i32
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch(&pool)
    .await
    .unwrap();

    // uid=1 (alice) has tickets but none in dept 999
    assert!(
        results.is_empty(),
        "alice has no tickets in dept 999, got {}",
        results.len()
    );
}

// --- fetch_one and fetch_optional with optional clauses ---

#[tokio::test]
async fn optional_clause_fetch_optional_found() {
    let pool = pool().await;
    let login = "alice";
    let middle: Option<&str> = None;
    let result = bsql::query!(
        "SELECT id, login FROM users
         WHERE login = $login: &str
         [AND middle_name = $middle: Option<&str>]"
    )
    .fetch_optional(&pool)
    .await
    .unwrap();

    assert!(result.is_some());
    assert_eq!(result.unwrap().login, "alice");
}

#[tokio::test]
async fn optional_clause_fetch_optional_not_found() {
    let pool = pool().await;
    let login = "alice";
    let middle: Option<&str> = Some("NonexistentMiddle");
    let result = bsql::query!(
        "SELECT id, login FROM users
         WHERE login = $login: &str
         [AND middle_name = $middle: Option<&str>]"
    )
    .fetch_optional(&pool)
    .await
    .unwrap();

    // alice has no middle name (NULL), so middle_name = 'NonexistentMiddle' won't match
    assert!(result.is_none());
}

// --- execute with optional clause ---

#[tokio::test]
async fn optional_clause_execute() {
    let pool = pool().await;
    let dept: Option<i32> = Some(999);

    // UPDATE with optional clause -- should affect 0 rows (no tickets in dept 999)
    let affected = bsql::query!(
        "UPDATE tickets SET description = 'test'
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]"
    )
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(affected, 0);
}

#[tokio::test]
async fn optional_clause_execute_via_transaction() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();

    let dept: Option<i32> = Some(999);
    let affected = bsql::query!(
        "UPDATE tickets SET description = 'tx_dyn'
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]"
    )
    .execute(&mut tx)
    .await
    .unwrap();
    assert_eq!(affected, 0);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn optional_clause_execute_via_conn() {
    let pool = pool().await;
    let mut conn = pool.acquire().await.unwrap();

    let dept: Option<i32> = Some(999);
    let affected = bsql::query!(
        "UPDATE tickets SET description = 'conn_dyn'
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]"
    )
    .execute(&mut conn)
    .await
    .unwrap();
    assert_eq!(affected, 0);
}

// --- Three optional clauses ---

#[tokio::test]
async fn three_optional_clauses() {
    let pool = pool().await;
    let dept: Option<i32> = None;
    let assignee: Option<i32> = None;
    let creator: Option<i32> = Some(1);
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         [AND assignee_id = $assignee: Option<i32>]
         [AND created_by_user_id = $creator: Option<i32>]
         ORDER BY id"
    )
    .fetch(&pool)
    .await
    .unwrap();

    // Only creator=1 (alice) filter active, dept and assignee excluded
    assert!(!results.is_empty(), "alice has tickets");
}

// --- Optional clause with ILIKE pattern ---

#[tokio::test]
async fn optional_clause_ilike_pattern() {
    let pool = pool().await;
    let search: Option<String> = Some("login".to_owned());
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND title ILIKE '%' || $search: Option<String> || '%']
         ORDER BY id"
    )
    .fetch(&pool)
    .await
    .unwrap();

    // "Fix login bug" should match
    assert!(
        results.iter().any(|r| r.title.contains("login")),
        "should find 'Fix login bug': {results:?}"
    );
}

#[tokio::test]
async fn optional_clause_ilike_pattern_none() {
    let pool = pool().await;
    let search: Option<String> = None;
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND title ILIKE '%' || $search: Option<String> || '%']
         ORDER BY id"
    )
    .fetch(&pool)
    .await
    .unwrap();

    // No search filter -- returns all non-deleted tickets
    assert!(results.len() >= 2);
}

// --- T-2: Streaming + dynamic queries ---

#[tokio::test]
async fn stream_with_optional_clause_none() {
    let pool = pool().await;
    let dept: Option<i32> = None;
    let mut stream = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch_stream(&pool)
    .await
    .unwrap();

    let mut count = 0;
    while let Some(ticket) = stream.next().await.unwrap() {
        count += 1;
        assert!(!ticket.title.is_empty());
    }
    // dept=None -- all non-deleted tickets
    assert!(count >= 2, "expected at least 2 tickets, got {count}");
}

#[tokio::test]
async fn stream_with_optional_clause_some() {
    let pool = pool().await;
    let dept: Option<i32> = Some(999);
    let mut stream = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch_stream(&pool)
    .await
    .unwrap();

    let mut count = 0;
    while let Some(_ticket) = stream.next().await.unwrap() {
        count += 1;
    }
    // dept=999 -- no tickets
    assert_eq!(count, 0, "no tickets in dept 999, got {count}");
}

#[tokio::test]
async fn stream_with_optional_clause_and_base_params() {
    let pool = pool().await;
    let uid = 1i32;
    let dept: Option<i32> = None;
    let mut stream = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE created_by_user_id = $uid: i32
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch_stream(&pool)
    .await
    .unwrap();

    let mut count = 0;
    while let Some(_ticket) = stream.next().await.unwrap() {
        count += 1;
    }
    assert!(count >= 1, "alice should have tickets, got {count}");
}
