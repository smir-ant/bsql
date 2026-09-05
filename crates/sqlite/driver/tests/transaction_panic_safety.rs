#![forbid(unsafe_code)]
//! Witness for the `transaction()` PANIC-SAFETY fix (C5).
//!
//! `Connection::transaction` issues a deferred `BEGIN`, runs the closure, and
//! terminates with `COMMIT`/`ROLLBACK` off the closure's `Result`. If the closure
//! PANICS it yields no `Result`, so — before the fix — the explicit terminator was
//! bypassed and the eagerly-issued `BEGIN` was left OPEN on the reused in-process
//! handle (SQLite has no pool `reset` to launder a stranded transaction). The fix
//! rebinds the terminate obligation to SCOPE DESTRUCTION via a hand-rolled RAII
//! rollback guard whose `Drop` fires a best-effort `ROLLBACK` when the closure
//! unwinds.
//!
//! This proves three properties:
//! 1. PANIC ROLLS BACK + CONNECTION IS REUSABLE: after a panicking `transaction`,
//!    a FRESH `transaction` (and a bare statement) succeeds on the SAME connection
//!    — proving no stranded open `BEGIN` — and the panicked write was NOT persisted.
//! 2. NORMAL COMMIT still commits (the disarm → explicit-COMMIT path is unchanged).
//! 3. NORMAL ERROR still rolls back AND returns its exact classified `SqliteError`
//!    (the disarm → explicit-ROLLBACK path is unchanged — the guard never
//!    double-terminates or re-wraps the error).

use std::panic::{catch_unwind, AssertUnwindSafe};

use bsql_sqlite::{Connection, SqliteError};

#[expect(
    clippy::expect_used,
    reason = "test helper (not a `#[test]` fn, so outside the `allow-expect-in-tests` \
              carve-out) — a failed count query is a loud test failure, not a production fallback"
)]
fn count(conn: &Connection, table: &str) -> i64 {
    conn.query_one_raw(&format!("SELECT count(*) FROM {table}"))
        .expect("count query")
        .get::<i64>(0)
        .expect("count is an integer")
}

/// (1) A panic inside the closure rolls the transaction back, leaves the
/// connection at a CLEAN boundary (reusable), and persists nothing.
#[test]
fn a_panicking_closure_rolls_back_and_leaves_the_connection_reusable() {
    let mut conn = Connection::open_in_memory().expect("open in-memory");
    conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create t");

    // The closure inserts a row (inside the open BEGIN) then PANICS. The panic
    // propagates out of `transaction` — caught here so the test process survives.
    // `AssertUnwindSafe`: `&mut Connection` wraps rusqlite's interior mutability and is
    // not `RefUnwindSafe`; the assertion is sound because we OBSERVE the connection
    // only through its public verbs afterward (we never read logically-torn state).
    let caught = catch_unwind(AssertUnwindSafe(|| {
        conn.transaction(|tx| -> Result<(), SqliteError> {
            tx.execute_raw("INSERT INTO t (id) VALUES (1)")?;
            panic!("deliberate panic inside the transaction closure");
        })
    }));
    assert!(
        caught.is_err(),
        "the panic must propagate out of transaction(), not be swallowed"
    );

    // The panicked INSERT was rolled back — the row is NOT there.
    assert_eq!(
        count(&conn, "t"),
        0,
        "the panic-path ROLLBACK must have undone the INSERT (no stranded write)"
    );

    // The connection is at a CLEAN boundary: a FRESH transaction BEGINs + COMMITs
    // (before the fix the stranded open BEGIN failed this with "cannot start a
    // transaction within a transaction").
    conn.transaction(|tx| {
        tx.execute_raw("INSERT INTO t (id) VALUES (2)")?;
        Ok(())
    })
    .expect("a fresh transaction must BEGIN + COMMIT cleanly after the panic");

    // And a bare dynamic statement still works — the handle is fully reusable.
    let affected = conn
        .execute_raw("INSERT INTO t (id) VALUES (3)")
        .expect("a bare statement after the panic");
    assert_eq!(affected, 1);
    assert_eq!(
        count(&conn, "t"),
        2,
        "only the two post-panic writes committed; the panicked write did not"
    );
}

/// (2) A normal `Ok`-returning closure still COMMITs — the disarm → explicit-COMMIT
/// path is byte-for-byte the prior behaviour.
#[test]
fn a_normal_commit_still_commits() {
    let mut conn = Connection::open_in_memory().expect("open in-memory");
    conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create t");

    let out: i32 = conn
        .transaction(|tx| {
            tx.execute_raw("INSERT INTO t (id) VALUES (1)")?;
            tx.execute_raw("INSERT INTO t (id) VALUES (2)")?;
            Ok(7)
        })
        .expect("a normal transaction commits and returns its value");
    assert_eq!(out, 7, "the closure's return value is passed through on commit");
    assert_eq!(count(&conn, "t"), 2, "both inserts committed");
}

/// (3) A normal `Err`-returning closure still ROLLs BACK and surfaces its EXACT
/// classified error — the disarm → explicit-ROLLBACK path is unchanged, and the
/// guard never double-rolls-back nor re-wraps the error.
#[test]
fn a_normal_error_still_rolls_back_with_its_exact_classified_error() {
    let mut conn = Connection::open_in_memory().expect("open in-memory");
    conn.execute_raw("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create t");

    // The closure inserts a row, then returns a REAL classified SQLite error (a
    // duplicate PRIMARY KEY constraint violation). The transaction must roll back.
    let err = conn
        .transaction(|tx| {
            tx.execute_raw("INSERT INTO t (id) VALUES (1)")?;
            // Second insert with the same PK: a constraint violation the driver
            // classifies. `?` returns it as the closure's Err.
            tx.execute_raw("INSERT INTO t (id) VALUES (1)")?;
            Ok(())
        })
        .expect_err("a duplicate-PK insert must surface a classified error");

    assert!(
        err.is_constraint_violation(),
        "the closure's error must survive the ROLLBACK unchanged (classified): {err:?}"
    );
    assert!(
        !matches!(err, SqliteError::TransactionRollbackFailed { .. }),
        "a successful ROLLBACK must NOT wrap the error (which would declassify it): {err:?}"
    );

    assert_eq!(
        count(&conn, "t"),
        0,
        "the whole transaction rolled back — the first INSERT did not persist either"
    );

    // Recovery: the connection is at a clean boundary after the rollback.
    conn.transaction(|tx| {
        tx.execute_raw("INSERT INTO t (id) VALUES (9)")?;
        Ok(())
    })
    .expect("a follow-up transaction commits cleanly after the rollback");
    assert_eq!(count(&conn, "t"), 1);
}
