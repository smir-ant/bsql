//! # transactions — closure-scoped, commit-on-Ok / rollback-on-Err
//!
//! `conn.transaction(|tx| …)` makes the closure body THE transaction: return
//! `Ok` and it COMMITs, return `Err` (or an early `?`) and it ROLLs BACK. The
//! guard `tx` exposes only the data verbs — `tx.commit()` / `tx.begin()` /
//! `tx.rollback()` do NOT exist, so a forgotten or double commit is a COMPILE
//! error (E0599), not a runtime bug. Atomicity is a compile-time guarantee.
//!
//! Features/verbs: `conn.transaction(|tx| …)`, the guard's data verbs.
//!
//! Backend: SQLite — needs NO database (the closure guard works identically on
//! PostgreSQL: `conn.transaction(async |tx| …).await`).
//! ```bash
//! cargo run -p bsql-examples --bin transactions
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "example/teaching code: unwrap/expect/panic surface failure loudly and keep the code readable"
)]
#![forbid(unsafe_code)]

use bsql::sqlite::{Connection, SqliteError};

fn count_users(conn: &Connection) -> i64 {
    conn.query_one_raw("SELECT count(*) FROM users")
        .expect("count")
        .get::<i64>(0)
        .expect("decode")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch_raw(bsql_examples::SQLITE_SCHEMA)?;

    // ── A transaction that COMMITS: both inserts land atomically ─────────────
    conn.transaction(|tx| {
        tx.execute_raw("INSERT INTO users (id, email, name) VALUES (1, 'alice@x', 'Alice')")?;
        tx.execute_raw("INSERT INTO users (id, email, name) VALUES (2, 'bob@x', NULL)")?;
        // NOTE: `tx.commit()` is intentionally NOT available — returning `Ok`
        // IS the commit. Calling it would be a compile error (E0599).
        Ok(())
    })?;
    println!("after a committed transaction: {} users", count_users(&conn));

    // ── A transaction that ROLLS BACK: the second insert violates the PRIMARY
    //    KEY, so the `?` propagates the error and the WHOLE transaction is
    //    rolled back — the first insert is undone too. ─────────────────────────
    let result: Result<(), SqliteError> = conn.transaction(|tx| {
        tx.execute_raw("INSERT INTO users (id, email, name) VALUES (3, 'carol@x', NULL)")?;
        // Duplicate id 3 -> constraint violation -> `?` returns Err -> ROLLBACK.
        tx.execute_raw("INSERT INTO users (id, email, name) VALUES (3, 'dup@x', NULL)")?;
        Ok(())
    });
    match result {
        Ok(()) => println!("(unexpected) the failing transaction committed"),
        Err(err) => println!("transaction rolled back on error: {err}"),
    }
    // The rolled-back transaction left NO trace — still just the two committed users.
    println!("after the rolled-back transaction: {} users", count_users(&conn));

    Ok(())
}
