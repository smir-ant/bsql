#![forbid(unsafe_code)]

//! Live witnesses for `#[bsql::test]` (need a real PostgreSQL at `BSQL_TEST_DSN`;
//! all `#[ignore]`, run with `--ignored`).
//!
//! Run with, e.g.:
//! ```text
//! BSQL_TEST_DSN=postgres://smir-ant@localhost/postgres \
//!   cargo test -p bsql-test-harness-fixture -- --ignored
//! ```

use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use bsql::pg::Connection;

// ─────────────────────────────────────────────────────────────────────
// Parallel isolation: two tests, the SAME table name, DISTINCT rows.
// Cargo runs them in parallel by default; if they shared a schema, one
// would see the other's row (or the CREATE would collide). Each asserting
// it sees ONLY its own single row is the isolation proof.
// ─────────────────────────────────────────────────────────────────────

#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
async fn isolation_a_sees_only_its_own_row(conn: &mut Connection) {
    conn.execute_sql("CREATE TABLE shared_name (v int)").await.unwrap();
    conn.execute_sql("INSERT INTO shared_name (v) VALUES (111)").await.unwrap();

    let only = conn.query_one_sql("SELECT v FROM shared_name").await.unwrap();
    assert_eq!(only.get_i32(0), Ok(Some(111)), "must see only its own row");

    let count = conn.query_one_sql("SELECT count(*) FROM shared_name").await.unwrap();
    assert_eq!(count.get_i64(0), Ok(Some(1)), "isolation: exactly one row visible");

    let schema = conn.query_one_sql("SELECT current_schema()::text").await.unwrap();
    let name = schema.get_str(0).unwrap().unwrap();
    assert!(name.starts_with("bsql_t_"), "runs in a harness schema, got {name:?}");
}

#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
async fn isolation_b_sees_only_its_own_row(conn: &mut Connection) {
    conn.execute_sql("CREATE TABLE shared_name (v int)").await.unwrap();
    conn.execute_sql("INSERT INTO shared_name (v) VALUES (222)").await.unwrap();

    let only = conn.query_one_sql("SELECT v FROM shared_name").await.unwrap();
    assert_eq!(only.get_i32(0), Ok(Some(222)), "must see only its own row");

    let count = conn.query_one_sql("SELECT count(*) FROM shared_name").await.unwrap();
    assert_eq!(count.get_i64(0), Ok(Some(1)), "isolation: exactly one row visible");
}

// ─────────────────────────────────────────────────────────────────────
// Teardown proof. These drive the harness directly (rather than via the
// attribute) so the test controls what happens AFTER teardown: it captures
// the isolated schema's name from inside the body, then — once the harness
// has returned and dropped it — asserts (via information_schema) it is gone.
// ─────────────────────────────────────────────────────────────────────

/// A body that records its own schema name into `sink` and (optionally) panics.
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "test-support helper: it is a free fn, not a #[test] fn, so the \
              in-tests carve-out for the panic-class floor lints does not reach \
              it; an unwrap/panic here IS the loud test-failure signal"
)]
async fn capture_schema_then(
    conn: &mut Connection,
    sink: Arc<Mutex<Option<String>>>,
    then_panic: bool,
) {
    let row = conn.query_one_sql("SELECT current_schema()::text").await.unwrap();
    let schema = row.get_str(0).unwrap().unwrap().to_string();
    conn.execute_sql("CREATE TABLE probe (v int)").await.unwrap();
    match sink.lock() {
        Ok(mut guard) => *guard = Some(schema),
        Err(_) => panic!("schema-capture mutex was poisoned"),
    }
    if then_panic {
        panic!("intentional body panic (teardown-on-panic witness)");
    }
}

#[test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
fn teardown_drops_schema_on_success() {
    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&captured);

    bsql::__test_rt::run_schema_isolated_test("passing_body", async move |conn: &mut Connection| {
        capture_schema_then(conn, sink, false).await;
    });

    let schema = captured.lock().unwrap().take().expect("the body must capture its schema");
    assert!(
        !bsql::__test_rt::schema_exists(&schema),
        "schema {schema} must be dropped after a passing test",
    );
}

#[test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
fn teardown_drops_schema_even_on_panic() {
    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&captured);

    // The harness catches the body panic, drops the schema, then re-raises the
    // panic — so it propagates out here and we catch it to keep testing.
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        bsql::__test_rt::run_schema_isolated_test(
            "deliberately_panicking_body",
            async move |conn: &mut Connection| {
                capture_schema_then(conn, sink, true).await;
            },
        );
    }));
    assert!(result.is_err(), "the body panic must propagate out of the harness");

    let schema = captured.lock().unwrap().take().expect("the body must capture its schema");
    assert!(
        !bsql::__test_rt::schema_exists(&schema),
        "schema {schema} must be dropped despite the test panicking",
    );
}
