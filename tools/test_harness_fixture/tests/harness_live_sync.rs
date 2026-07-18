#![forbid(unsafe_code)]

//! Live witnesses for the SYNC `#[bsql::test]` (a plain `fn` over the blocking
//! driver). The exact twin of `harness_live.rs`: same isolation, same
//! teardown-on-success and teardown-on-panic guarantees, with no runtime and no
//! `.await`. Needs a real PostgreSQL at `BSQL_TEST_DSN`; all `#[ignore]`, run
//! with `--ignored`.
//!
//! Run with, e.g.:
//! ```text
//! BSQL_TEST_DSN=postgres://smir-ant@localhost/postgres \
//!   cargo test -p bsql-test-harness-fixture -- --ignored
//! ```

use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use bsql::pg_sync::Connection;

// ─────────────────────────────────────────────────────────────────────
// Parallel isolation: two SYNC tests, the SAME table name, DISTINCT rows.
// Cargo runs them in parallel by default; if they shared a schema, one
// would see the other's row (or the CREATE would collide). Each asserting
// it sees ONLY its own single row is the isolation proof.
// ─────────────────────────────────────────────────────────────────────

#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
fn sync_isolation_a_sees_only_its_own_row(conn: &mut Connection) {
    conn.execute_raw("CREATE TABLE shared_name (v int)").unwrap();
    conn.execute_raw("INSERT INTO shared_name (v) VALUES (111)").unwrap();

    let only = conn.query_one_raw("SELECT v FROM shared_name").unwrap();
    assert_eq!(only.get_i32(0), Ok(Some(111)), "must see only its own row");

    let count = conn.query_one_raw("SELECT count(*) FROM shared_name").unwrap();
    assert_eq!(count.get_i64(0), Ok(Some(1)), "isolation: exactly one row visible");

    let schema = conn.query_one_raw("SELECT current_schema()::text").unwrap();
    let name = schema.get_str(0).unwrap().unwrap();
    assert!(name.starts_with("bsql_t_"), "runs in a harness schema, got {name:?}");
}

#[bsql::test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
fn sync_isolation_b_sees_only_its_own_row(conn: &mut Connection) {
    conn.execute_raw("CREATE TABLE shared_name (v int)").unwrap();
    conn.execute_raw("INSERT INTO shared_name (v) VALUES (222)").unwrap();

    let only = conn.query_one_raw("SELECT v FROM shared_name").unwrap();
    assert_eq!(only.get_i32(0), Ok(Some(222)), "must see only its own row");

    let count = conn.query_one_raw("SELECT count(*) FROM shared_name").unwrap();
    assert_eq!(count.get_i64(0), Ok(Some(1)), "isolation: exactly one row visible");
}

// ─────────────────────────────────────────────────────────────────────
// Teardown proof (sync). These drive the sync harness directly (rather than
// via the attribute) so the test controls what happens AFTER teardown: it
// captures the isolated schema's name from inside the body, then — once the
// harness has returned and dropped it — asserts (via information_schema) it is
// gone. The probe uses the sync existence check, so the whole witness is sync.
// ─────────────────────────────────────────────────────────────────────

/// A body that records its own schema name into `sink` and (optionally) panics.
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "test-support helper: it is a free fn, not a #[test] fn, so the \
              in-tests carve-out for the panic-class floor lints does not reach \
              it; an unwrap/panic here IS the loud test-failure signal"
)]
fn capture_schema_then(conn: &mut Connection, sink: Arc<Mutex<Option<String>>>, then_panic: bool) {
    let row = conn.query_one_raw("SELECT current_schema()::text").unwrap();
    let schema = row.get_str(0).unwrap().unwrap().to_string();
    conn.execute_raw("CREATE TABLE probe (v int)").unwrap();
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
fn sync_teardown_drops_schema_on_success() {
    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&captured);

    bsql::__test_rt::run_schema_isolated_test_sync("passing_sync_body", move |conn: &mut Connection| {
        capture_schema_then(conn, sink, false);
    });

    let schema = captured.lock().unwrap().take().expect("the body must capture its schema");
    assert!(
        !bsql::__test_rt::schema_exists_sync(&schema),
        "schema {schema} must be dropped after a passing sync test",
    );
}

#[test]
#[ignore = "live: needs PostgreSQL at BSQL_TEST_DSN"]
fn sync_teardown_drops_schema_even_on_panic() {
    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&captured);

    // The harness catches the body panic, drops the schema, then re-raises the
    // panic — so it propagates out here and we catch it to keep testing.
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        bsql::__test_rt::run_schema_isolated_test_sync(
            "deliberately_panicking_sync_body",
            move |conn: &mut Connection| {
                capture_schema_then(conn, sink, true);
            },
        );
    }));
    assert!(result.is_err(), "the body panic must propagate out of the sync harness");

    let schema = captured.lock().unwrap().take().expect("the body must capture its schema");
    assert!(
        !bsql::__test_rt::schema_exists_sync(&schema),
        "schema {schema} must be dropped despite the sync test panicking",
    );
}
