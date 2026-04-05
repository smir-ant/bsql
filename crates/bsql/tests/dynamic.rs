//! Integration tests for v0.3: dynamic queries (optional clauses).
//!
//! Requires a running PostgreSQL with the test schema.
//! Set BSQL_DATABASE_URL=postgres://bsql:bsql@localhost/bsql_test

use bsql::Pool;

fn pool() -> Pool {
    Pool::connect("postgres://bsql:bsql@localhost/bsql_test")
        .expect("Failed to connect to test database. Is PostgreSQL running?")
}

// --- Single optional clause ---

#[test]
fn one_optional_clause_some() {
    let pool = pool();
    let dept: Option<i32> = Some(1);
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();

    // With dept=Some(1), only tickets in department 1 should be returned.
    // Our seed data doesn't have department_id set, so this should return 0.
    assert!(
        results.is_empty(),
        "no seed tickets have department_id=1: got {} rows",
        results.len()
    );
}

#[test]
fn one_optional_clause_none() {
    let pool = pool();
    let dept: Option<i32> = None;
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();

    // With dept=None, the clause is excluded -- returns all non-deleted tickets.
    assert!(
        results.len() >= 2,
        "expected at least 2 tickets, got {}",
        results.len()
    );
}

// --- Two optional clauses: all 4 combinations ---

#[test]
fn two_optional_clauses_none_none() {
    let pool = pool();
    let dept: Option<i32> = None;
    let assignee: Option<i32> = None;
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         [AND assignee_id = $assignee: Option<i32>]
         ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();

    assert!(
        results.len() >= 2,
        "both None -- should return all tickets, got {}",
        results.len()
    );
}

#[test]
fn two_optional_clauses_some_none() {
    let pool = pool();
    let dept: Option<i32> = Some(999);
    let assignee: Option<i32> = None;
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         [AND assignee_id = $assignee: Option<i32>]
         ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();

    assert!(
        results.is_empty(),
        "dept=999 -- should return 0 tickets, got {}",
        results.len()
    );
}

#[test]
fn two_optional_clauses_none_some() {
    let pool = pool();
    let dept: Option<i32> = None;
    let assignee: Option<i32> = Some(999);
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         [AND assignee_id = $assignee: Option<i32>]
         ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();

    assert!(
        results.is_empty(),
        "assignee=999 -- should return 0 tickets, got {}",
        results.len()
    );
}

#[test]
fn two_optional_clauses_some_some() {
    let pool = pool();
    let dept: Option<i32> = Some(999);
    let assignee: Option<i32> = Some(999);
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         [AND assignee_id = $assignee: Option<i32>]
         ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();

    assert!(
        results.is_empty(),
        "both=999 -- should return 0 tickets, got {}",
        results.len()
    );
}

// --- Optional clause with base required params ---

#[test]
fn optional_clause_with_base_params() {
    let pool = pool();
    let uid = 1i32;
    let dept: Option<i32> = None;
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE created_by_user_id = $uid: i32
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();

    // uid=1 (alice) has tickets. dept=None means no department filter.
    assert!(!results.is_empty(), "alice should have tickets, got 0");
}

#[test]
fn optional_clause_with_base_params_filtered() {
    let pool = pool();
    let uid = 1i32;
    let dept: Option<i32> = Some(999);
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE created_by_user_id = $uid: i32
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();

    // uid=1 (alice) has tickets but none in dept 999
    assert!(
        results.is_empty(),
        "alice has no tickets in dept 999, got {}",
        results.len()
    );
}

// --- fetch_one and fetch_optional with optional clauses ---

#[test]
fn optional_clause_fetch_optional_found() {
    let pool = pool();
    let login = "alice";
    let middle: Option<&str> = None;
    let result = bsql::query!(
        "SELECT id, login FROM users
         WHERE login = $login: &str
         [AND middle_name = $middle: Option<&str>]"
    )
    .fetch_optional(&pool)
    .unwrap();

    assert!(result.is_some());
    assert_eq!(result.unwrap().login, "alice");
}

#[test]
fn optional_clause_fetch_optional_not_found() {
    let pool = pool();
    let login = "alice";
    let middle: Option<&str> = Some("NonexistentMiddle");
    let result = bsql::query!(
        "SELECT id, login FROM users
         WHERE login = $login: &str
         [AND middle_name = $middle: Option<&str>]"
    )
    .fetch_optional(&pool)
    .unwrap();

    // alice has no middle name (NULL), so middle_name = 'NonexistentMiddle' won't match
    assert!(result.is_none());
}

// --- execute with optional clause ---

#[test]
fn optional_clause_execute() {
    let pool = pool();
    let dept: Option<i32> = Some(999);

    // UPDATE with optional clause -- should affect 0 rows (no tickets in dept 999)
    let affected = bsql::query!(
        "UPDATE tickets SET description = 'test'
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]"
    )
    .execute(&pool)
    .unwrap();

    assert_eq!(affected, 0);
}

// --- Three optional clauses ---

#[test]
fn three_optional_clauses() {
    let pool = pool();
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
    .fetch_all(&pool)
    .unwrap();

    // Only creator=1 (alice) filter active, dept and assignee excluded
    assert!(!results.is_empty(), "alice has tickets");
}

// --- Optional clause with ILIKE pattern ---

#[test]
fn optional_clause_ilike_pattern() {
    let pool = pool();
    let search: Option<String> = Some("login".to_owned());
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND title ILIKE '%' || $search: Option<String> || '%']
         ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();

    // "Fix login bug" should match
    assert!(
        results.iter().any(|r| r.title.contains("login")),
        "should find 'Fix login bug': {results:?}"
    );
}

#[test]
fn optional_clause_ilike_pattern_none() {
    let pool = pool();
    let search: Option<String> = None;
    let results = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND title ILIKE '%' || $search: Option<String> || '%']
         ORDER BY id"
    )
    .fetch_all(&pool)
    .unwrap();

    // No search filter -- returns all non-deleted tickets
    assert!(results.len() >= 2);
}

// --- T-2: Streaming + dynamic queries ---

#[test]
fn stream_with_optional_clause_none() {
    let pool = pool();
    let dept: Option<i32> = None;
    let mut stream = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch_stream(&pool)
    .unwrap();

    let mut count = 0;
    while let Some(ticket) = stream.next().unwrap() {
        count += 1;
        assert!(!ticket.title.is_empty());
    }
    // dept=None -- all non-deleted tickets
    assert!(count >= 2, "expected at least 2 tickets, got {count}");
}

#[test]
fn stream_with_optional_clause_some() {
    let pool = pool();
    let dept: Option<i32> = Some(999);
    let mut stream = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch_stream(&pool)
    .unwrap();

    let mut count = 0;
    while let Some(_ticket) = stream.next().unwrap() {
        count += 1;
    }
    // dept=999 -- no tickets
    assert_eq!(count, 0, "no tickets in dept 999, got {count}");
}

#[test]
fn stream_with_optional_clause_and_base_params() {
    let pool = pool();
    let uid = 1i32;
    let dept: Option<i32> = None;
    let mut stream = bsql::query!(
        "SELECT id, title FROM tickets
         WHERE created_by_user_id = $uid: i32
         [AND department_id = $dept: Option<i32>]
         ORDER BY id"
    )
    .fetch_stream(&pool)
    .unwrap();

    let mut count = 0;
    while let Some(_ticket) = stream.next().unwrap() {
        count += 1;
    }
    assert!(count >= 1, "alice should have tickets, got {count}");
}
