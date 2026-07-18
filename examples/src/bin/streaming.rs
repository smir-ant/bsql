//! # streaming — constant-memory reads over a colossal result
//!
//! An eager `query` materializes the WHOLE result. When a result is huge (or
//! unbounded), stream it instead: `query_each` hands you one row at a time and
//! accumulates NOTHING, so memory stays O(1) in the row count. Each row is lent
//! as a zero-copy borrowed view. The closure returns `ControlFlow`: `Continue`
//! keeps going, `Break(payload)` stops early (and the connection is drained back
//! to a clean, reusable state).
//!
//! Features/verbs: dynamic `query_each_raw` (constant memory over a million-row
//! sequence), typed `query_each::<Q>`, and `ControlFlow` early-break.
//!
//! Backend: SQLite — needs NO database (uses an in-memory recursive CTE to
//! generate a million rows without storing them).
//! ```bash
//! cargo run -p bsql-examples --bin streaming
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "example/teaching code: unwrap/expect/panic surface failure loudly and keep the code readable"
)]
#![forbid(unsafe_code)]

use core::ops::ControlFlow;

use bsql::sqlite::{Connection, ValueRef};

// A typed carrier over the `books` table, for the typed streaming half.
bsql::query!(AllBooks, "SELECT id, author_id, title FROM books ORDER BY id");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch_raw(bsql_examples::SQLITE_SCHEMA)?;

    // ── Dynamic streaming: sum 1..=1_000_000 without ever storing a row ──────
    // A recursive CTE generates a million rows; `query_each_raw` steps them one
    // at a time. An eager `query_raw` here would build a million-row result.
    const N: i64 = 1_000_000;
    let sql = format!(
        "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < {N}) \
         SELECT n FROM seq"
    );
    let mut sum: i64 = 0;
    let mut rows_seen: i64 = 0;
    conn.query_each_raw(&sql, |row| {
        // `row.get::<i64>(0)` reads the column in its true storage class; a
        // mismatch or a NULL would be a classified error, never a silent coercion.
        let n = row.get::<i64>(0).expect("n decodes");
        sum += n;
        rows_seen += 1;
        ControlFlow::<()>::Continue(())
    })?;
    println!("streamed {rows_seen} rows; sum(1..={N}) = {sum}");

    // ── Early break: stop as soon as the running sum passes a threshold ──────
    let stopped_at = conn.query_each_raw(&sql, {
        let mut running = 0i64;
        move |row| {
            running += row.get::<i64>(0).expect("n decodes");
            if running > 1_000 {
                ControlFlow::Break(running) // the payload rides `Ok(Some(..))`
            } else {
                ControlFlow::Continue(())
            }
        }
    })?;
    println!("early-broke once the running sum passed 1000: {stopped_at:?}");

    // ── Typed streaming over a real table ────────────────────────────────────
    for (id, title) in [(1i64, "A"), (2, "B"), (3, "C")] {
        conn.execute_params(
            "INSERT INTO books (id, author_id, title, published_year) VALUES ($1, 1, $2, NULL)",
            &[ValueRef::Integer(id), ValueRef::Text(title.as_bytes())],
        )?;
    }
    let mut titles = Vec::new();
    conn.query_each::<AllBooks, _, ()>((), |book| {
        // `book` is the borrowed typed record — zero-copy, decoded on access.
        titles.push(book.title.to_string());
        ControlFlow::Continue(())
    })?;
    println!("typed stream saw titles: {titles:?}");

    Ok(())
}
