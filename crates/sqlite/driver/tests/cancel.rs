#![forbid(unsafe_code)]
//! Query-cancellation witness for the SQLite driver.
//!
//! An in-flight compute-bound query interrupted from ANOTHER thread via a
//! `SqliteCancelToken` returns the classified `SqliteError::Interrupted`, and the
//! connection stays reusable. This is the SQLite twin of the PostgreSQL
//! `cancel_token()` witness — the two read the same (`conn.cancel_token()` +
//! `token.cancel()`), one cross-backend mental model.

use bsql_sqlite::{Connection, SqliteError};

/// WITNESS: a never-terminating recursive CTE, interrupted ~100 ms in from
/// another thread, classifies as `SqliteError::Interrupted` and leaves the
/// connection reusable.
#[test]
fn interrupt_stops_an_inflight_query() {
    let conn = Connection::open_in_memory().expect("open");
    // The token is obtained BEFORE the long query and borrows nothing from `conn`.
    let token = conn.cancel_token();
    // From another thread, interrupt ~100 ms in — the query below never
    // terminates on its own, so it is certainly still running.
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(100));
        token.cancel();
    });
    let start = std::time::Instant::now();
    // An infinite recursive CTE: `count(*)` over it never completes, so the first
    // step runs until interrupted (bounded memory — count is a scalar).
    let outcome = conn.query_raw(
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c",
    );
    let elapsed = start.elapsed();
    canceller.join().expect("cancel thread join");
    match outcome {
        Err(SqliteError::Interrupted) => {}
        Err(other) => panic!("an interrupted query must be SqliteError::Interrupted, got {other:?}"),
        Ok(_) => panic!("the never-terminating query must be interrupted, not complete"),
    }
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "the interrupt must return promptly, took {elapsed:?}",
    );
    // The interrupt aborted the STATEMENT, not the connection: it is reusable.
    let row = conn
        .query_one_raw("SELECT 42")
        .expect("connection reusable after interrupt");
    assert_eq!(row.get::<i64>(0).expect("read i64"), 42);
}

/// A one-shot query with a token that is NEVER canceled completes normally — the
/// mere existence of a token does not interrupt anything (no false positive).
#[test]
fn an_uncanceled_query_completes_normally() {
    let conn = Connection::open_in_memory().expect("open");
    let _token = conn.cancel_token();
    let row = conn.query_one_raw("SELECT 7").expect("query completes");
    assert_eq!(row.get::<i64>(0).expect("read i64"), 7);
}

/// A cancel with no query running is harmless — it interrupts the next step if
/// one starts, else is a no-op; the connection keeps working either way.
#[test]
fn cancel_with_no_query_running_is_harmless() {
    let conn = Connection::open_in_memory().expect("open");
    let token = conn.cancel_token();
    token.cancel(); // nothing is running
    // A quick query afterward may or may not catch the pending interrupt flag;
    // either way the connection stays usable (retry on the benign interrupt).
    let value = match conn.query_one_raw("SELECT 1") {
        Ok(row) => row.get::<i64>(0).expect("read i64"),
        Err(SqliteError::Interrupted) => conn
            .query_one_raw("SELECT 1")
            .expect("reusable after a stray interrupt")
            .get::<i64>(0)
            .expect("read i64"),
        Err(other) => panic!("unexpected error: {other:?}"),
    };
    assert_eq!(value, 1);
}
