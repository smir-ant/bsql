//! Compile-fail golden PINNING the transaction-guard tier-1 atomicity guarantee.
//!
//! The blocking `transaction` hands its closure a borrowing [`Transaction`] guard
//! that exposes ONLY the data / bulk / session verbs, NOT the six transaction /
//! connection LIFECYCLE verbs. Hand-driving the transaction boundary from inside
//! the body — `tx.begin()` / `tx.commit()` / `tx.rollback()` / a nested
//! `tx.transaction(..)` (PostgreSQL flattens it silently) / `tx.close()` /
//! `tx.reset_session()` — is therefore a method-not-found compile error (E0599),
//! the SAME tier as `PreparedStatement`-after-close, not a silent runtime
//! atomicity break.
//!
//! Without this golden the guarantee is UNPINNED: a future refactor that re-added
//! a lifecycle verb to the guard, or reverted the closure argument to the whole
//! `&mut Connection`, would silently downgrade tier-1 → tier-4 with every other
//! test still green. This test freezes the E0599s verbatim.
//!
//! [`Transaction`]: bsql_postgres_sync::Transaction
//!
//! Regenerate the golden after an intentional diagnostic change:
//! ```sh
//! TRYBUILD=overwrite cargo test -p bsql-postgres-sync --test tx_guard_compile_fail
//! ```
//! Then review the `.stderr` diff.

#![forbid(unsafe_code)]

/// The compile-time atomicity wall: every lifecycle verb on the guard is E0599.
#[test]
fn transaction_guard_forbids_lifecycle_verbs() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/tx_guard_ui/forbids_lifecycle_verbs.rs");
}
