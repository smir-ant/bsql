//! Compile-time wall for the transaction guard: the closure a
//! `Connection::transaction` receives is handed a `&Transaction`, which exposes
//! ONLY the data verbs. Every transaction-lifecycle call on it — a nested
//! `transaction`, a manual `begin` / `commit` / `rollback`, a `close` — is a
//! method that does NOT exist on `Transaction`, so each is an E0599 compile
//! error. The old `&Connection` argument compiled all of these and failed (or
//! desynced) only at runtime; this fixture pins that they now fail to compile.

use bsql_sqlite::Connection;

fn main() {
    let conn = Connection::open_in_memory().expect("open");
    let _ = conn.transaction(|tx| {
        // Nested transaction: the classic "cannot start a transaction within a
        // transaction" runtime error is now unrepresentable.
        tx.transaction(|_inner| Ok(()))?;
        // Manual lifecycle verbs the guard deliberately does not expose.
        tx.begin()?;
        tx.commit()?;
        tx.rollback()?;
        tx.close()?;
        Ok(())
    });
}
