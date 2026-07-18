//! # n1_detection — catch the classic N+1 query anti-pattern
//!
//! The N+1 anti-pattern: one query to fetch a list, then ONE MORE query per row
//! (here: fetch each author's books in a loop). bsql's diagnostics-only detector
//! flags the SAME query executed repeatedly from the SAME source line past a
//! threshold (25), reporting the offending SQL / file / line / count via
//! `conn.n1_report()`. It NEVER alters a result — a false positive is at most a
//! spurious report — and is zero-cost when the feature is off. This example runs
//! the anti-pattern, prints the report, then shows the fixed one-query version.
//!
//! Features/verbs: `conn.n1_report()` (feature `n1-detect`), the typed `query`
//! verb. Uses SQLite in-memory so it runs ANYWHERE.
//!
//! Backend: SQLite — needs NO database. Requires the `n1-detect` feature:
//! ```bash
//! cargo run -p bsql-examples --features n1-detect --bin n1_detection
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "example/teaching code: unwrap/expect/panic surface failure loudly and keep the code readable"
)]
#![forbid(unsafe_code)]

// When the feature is OFF, `conn.n1_report()` does not exist, so the bin can only
// print how to enable it (it still compiles either way).
#[cfg(not(feature = "n1-detect"))]
fn main() {
    eprintln!("this example needs the `n1-detect` feature — run it with:");
    eprintln!("  cargo run -p bsql-examples --features n1-detect --bin n1_detection");
}

#[cfg(feature = "n1-detect")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    demo::run()
}

#[cfg(feature = "n1-detect")]
mod demo {
    use bsql::sqlite::{Connection, ValueRef};

    // The per-author query the N+1 loop runs (once per author).
    bsql::query!(BooksByAuthor, "SELECT id, title FROM books WHERE author_id = $1 ORDER BY id");
    // The FIX: fetch every book in ONE query, then group in Rust.
    bsql::query!(AllBooks, "SELECT id, author_id, title FROM books ORDER BY author_id, id");

    const AUTHORS: i64 = 30; // > the detector's threshold of 25

    fn seeded() -> Connection {
        let conn = Connection::open_in_memory().expect("open");
        conn.execute_batch_raw(bsql_examples::SQLITE_SCHEMA).expect("schema");
        for a in 1..=AUTHORS {
            conn.execute_params(
                "INSERT INTO authors (id, name) VALUES ($1, $2)",
                &[ValueRef::Integer(a), ValueRef::Text(b"author")],
            )
            .expect("author");
            conn.execute_params(
                "INSERT INTO books (id, author_id, title, published_year) VALUES ($1, $2, $3, NULL)",
                &[ValueRef::Integer(a), ValueRef::Integer(a), ValueRef::Text(b"a book")],
            )
            .expect("book");
        }
        conn
    }

    pub fn run() -> Result<(), Box<dyn std::error::Error>> {
        // ── The N+1 anti-pattern: a query PER author, all from ONE line ───────
        let conn = seeded();
        let mut books_seen = 0usize;
        for author_id in 1..=AUTHORS {
            // This single source line executes AUTHORS times -> the N+1 site.
            let books = conn.query::<BooksByAuthor>((author_id,))?;
            for book in books.iter() {
                book?; // diagnostics-only: every result is still correct
                books_seen += 1;
            }
        }
        println!("N+1 version: fetched {books_seen} books via {AUTHORS} separate queries");

        // The detector flagged it: one site, with the SQL / file / line / count.
        let report = conn.n1_report();
        println!("n1_report() flagged {} site(s):", report.len());
        for finding in &report {
            println!(
                "  {}x  {}:{}  ->  {}",
                finding.count, finding.file, finding.line, finding.sql
            );
        }

        // ── The FIX: one query, group in Rust ────────────────────────────────
        let conn2 = seeded();
        let all = conn2.query::<AllBooks>(())?; // ONE query, not N
        let grouped = all.iter().filter_map(Result::ok).count();
        println!("\nfixed version: fetched {grouped} books via ONE query");
        let report2 = conn2.n1_report();
        println!("n1_report() flagged {} site(s) (expected 0)", report2.len());

        Ok(())
    }
}
