//! # schema_per_test — `#[bsql::test]` gives each test its own schema
//!
//! `#[bsql::test]` runs each test in its OWN freshly-created PostgreSQL schema
//! and drops it on exit — even if the test panics. So tests running in parallel
//! (cargo's default) never interfere, even when they use the SAME table names.
//! The isolation rides the connect-time `search_path`, which survives a pool's
//! `RESET ALL`. An `async fn` test runs over the async driver; a plain `fn` test
//! over the blocking driver — the connection argument type selects which.
//!
//! These are integration tests (in `examples/tests/`), gated behind the
//! `test-harness` feature (enabled by default for this example crate). They are
//! `#[ignore]` — they need a live PostgreSQL named by `BSQL_TEST_DSN` (a
//! test-specific variable, since the harness CREATES and DROPS schemas):
//!
//! ```bash
//! BSQL_TEST_DSN='postgres://USER@127.0.0.1:5432/postgres' \
//!   cargo test -p bsql-examples --test schema_per_test -- --ignored
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code: a failed unwrap/assert IS the loud test-failure signal"
)]
#![forbid(unsafe_code)]

// ── Two ASYNC tests, the SAME table name, DISTINCT rows ──────────────────────
// If they shared a schema, one would see the other's row (or the CREATE would
// collide). Each asserting it sees ONLY its own single row is the isolation proof.

#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
async fn alpha_sees_only_its_own_row(conn: &mut bsql::pg::Connection) {
    conn.execute_raw("CREATE TABLE widget (id int)").await.unwrap();
    conn.execute_raw("INSERT INTO widget VALUES (111)").await.unwrap();

    let only = conn.query_one_raw("SELECT id FROM widget").await.unwrap();
    assert_eq!(only.get_i32(0), Ok(Some(111)), "sees only its own row");

    let count = conn.query_one_raw("SELECT count(*) FROM widget").await.unwrap();
    assert_eq!(count.get_i64(0), Ok(Some(1)), "isolation: exactly one row visible");
}

#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
async fn beta_sees_only_its_own_row(conn: &mut bsql::pg::Connection) {
    // SAME table name as `alpha`, but a different isolated schema.
    conn.execute_raw("CREATE TABLE widget (id int)").await.unwrap();
    conn.execute_raw("INSERT INTO widget VALUES (222)").await.unwrap();

    let only = conn.query_one_raw("SELECT id FROM widget").await.unwrap();
    assert_eq!(only.get_i32(0), Ok(Some(222)), "sees only its own row");

    let count = conn.query_one_raw("SELECT count(*) FROM widget").await.unwrap();
    assert_eq!(count.get_i64(0), Ok(Some(1)), "isolation: exactly one row visible");
}

// ── A SYNC test in the same file: a plain `fn` selects the blocking driver ───
#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
fn sync_test_is_also_isolated(conn: &mut bsql::pg_sync::Connection) {
    conn.execute_raw("CREATE TABLE widget (id int)").unwrap();
    conn.execute_raw("INSERT INTO widget VALUES (333)").unwrap();

    let only = conn.query_one_raw("SELECT id FROM widget").unwrap();
    assert_eq!(only.get_i32(0), Ok(Some(333)), "sees only its own row");
}
