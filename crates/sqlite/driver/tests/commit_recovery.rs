#![forbid(unsafe_code)]
//! Witness for the `transaction()` COMMIT-failure RECOVERY fix (audit-9 item 2).
//!
//! A deferred foreign-key constraint is checked at COMMIT: the INSERT inside the
//! closure succeeds (deferred), then COMMIT fails the check and SQLite leaves the
//! transaction OPEN on the reused handle — the SAME "COMMIT fails, tx left open"
//! shape a BUSY on the RESERVED→EXCLUSIVE upgrade produces in a rollback-journal
//! mode, but deterministic and in-memory (no lock timing, no second connection).
//!
//! It proves the fix's two properties:
//! 1. RECOVERY: after the failed `transaction()`, a fresh `transaction()` BEGINs
//!    cleanly — the connection is at a clean boundary. Before the fix, the tx
//!    stayed open and the follow-up `BEGIN` failed ("cannot start a transaction
//!    within a transaction").
//! 2. CLASSIFICATION PRESERVED: the returned error still classifies as a
//!    constraint violation, proving it was NOT wrapped in
//!    `TransactionRollbackFailed` (whose `primary_code()` is `None`, which would
//!    declassify EVERY code predicate — including the `is_busy()` retry signal —
//!    to `false`). The BUSY-at-COMMIT case is preserved by the IDENTICAL mechanism
//!    (the error stays `SqliteError::Sqlite { code }`).

use bsql_sqlite::{Connection, SqliteError};

#[test]
fn a_commit_failure_rolls_back_and_preserves_the_classified_error() {
    let conn = Connection::open_in_memory().expect("open in-memory");
    conn.execute_raw("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .expect("create parent");
    conn.execute_raw(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER \
         REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
    )
    .expect("create child");

    // The INSERT is deferred, so it succeeds inside the closure; COMMIT then fails
    // the deferred FK check (parent 999 does not exist) and leaves the tx open.
    let err = conn
        .transaction(|tx| {
            tx.execute_raw("INSERT INTO child (id, parent_id) VALUES (1, 999)")?;
            Ok(())
        })
        .expect_err("a deferred-FK COMMIT must fail");

    // (2) The error stays the classified constraint failure, NOT a declassifying
    //     `TransactionRollbackFailed`.
    assert!(
        err.is_constraint_violation(),
        "the COMMIT error must stay classified (not TransactionRollbackFailed): {err:?}"
    );
    assert!(
        err.is_foreign_key_violation(),
        "the specific FK subtype must survive too: {err:?}"
    );
    assert!(
        !matches!(err, SqliteError::TransactionRollbackFailed { .. }),
        "the COMMIT error must not be wrapped (which would zero every code predicate): {err:?}"
    );

    // (1) RECOVERY: the connection is at a clean boundary, so a fresh transaction
    //     BEGINs and COMMITs cleanly (before the fix, the stale open tx failed this
    //     with "cannot start a transaction within a transaction").
    conn.transaction(|tx| {
        tx.execute_raw("INSERT INTO parent (id) VALUES (1)")?;
        Ok(())
    })
    .expect("a follow-up transaction must BEGIN + COMMIT cleanly after the rolled-back COMMIT failure");

    // And a bare dynamic statement still works — the connection is fully reusable.
    let affected = conn
        .execute_raw("INSERT INTO parent (id) VALUES (2)")
        .expect("a bare statement after recovery");
    assert_eq!(affected, 1, "the recovery transaction and the follow-up write both applied");
}
