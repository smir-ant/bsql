//! # joins_aggregates — JOIN, GROUP BY, and a subquery, all compile-checked
//!
//! bsql infers column types AND NULLABILITY through joins and aggregates at
//! build time — a capability that leans on bsql parsing your migration set. The
//! headline: a `LEFT JOIN` column that is `NOT NULL` in its base table becomes
//! `Option<_>` in the record, because the join can null-extend it. Get that
//! wrong (read it as non-`Option`) and it is a COMPILE error, not a runtime
//! `UnexpectedNull`.
//!
//! Features/verbs: `query!` over a JOIN + GROUP BY + a correlated subquery;
//! nullability inference on `LEFT JOIN` columns; the typed `query` verb.
//!
//! Backend: PostgreSQL — needs a live server. Uses session TEMP tables (seeded
//! fresh each run), so it is idempotent and parallel-safe.
//! ```bash
//! export BSQL_EXAMPLES_DSN='postgres://USER@127.0.0.1:5432/postgres'
//! cargo run -p bsql-examples --bin joins_aggregates
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::manual_unwrap_or,
    clippy::manual_unwrap_or_default,
    reason = "example/teaching code: unwrap/expect/panic read clearly, and the manual match on an Option is the form the workspace disallowed-methods ledger requires (the unwrap_or family is banned)"
)]
#![forbid(unsafe_code)]

use bsql::pg::{ConnectConfig, Connection};

// GROUP BY + aggregate: each author with their book count. `count(b.id)` is
// `i64` (bigint) and NON-null (count is never NULL). `a.name` is NOT NULL.
bsql::query!(
    BookCountsByAuthor,
    "SELECT a.name AS author, count(b.id) AS books \
     FROM authors a LEFT JOIN books b ON b.author_id = a.id \
     GROUP BY a.id, a.name ORDER BY a.name"
);

// LEFT JOIN nullability: `a.name` is NOT NULL in `authors`, but a book whose
// `author_id` matches no author null-extends it — so through the LEFT JOIN the
// inferred field is `Option<String>`. This is the nullability headline.
bsql::query!(
    BooksWithMaybeAuthor,
    "SELECT b.title, a.name AS author \
     FROM books b LEFT JOIN authors a ON a.id = b.author_id \
     ORDER BY b.id"
);

// A correlated subquery: authors with at least `$1` books.
bsql::query!(
    ProlificAuthors,
    "SELECT name FROM authors \
     WHERE id IN (SELECT author_id FROM books GROUP BY author_id HAVING count(*) >= $1::int8) \
     ORDER BY name"
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::connect(&ConnectConfig::from_dsn(&bsql_examples::dsn())?).await?;

    // Fresh TEMP shadows of `authors` / `books`, seeded for the demo.
    conn.execute_raw("CREATE TEMP TABLE authors (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .await?;
    conn.execute_raw(
        "CREATE TEMP TABLE books (id BIGINT PRIMARY KEY, author_id BIGINT NOT NULL, \
         title TEXT NOT NULL, published_year INTEGER)",
    )
    .await?;
    conn.execute_raw(
        "INSERT INTO authors (id, name) VALUES \
         (1, 'Le Guin'), (2, 'Butler'), (3, 'Unpublished Yet')",
    )
    .await?;
    conn.execute_raw(
        "INSERT INTO books (id, author_id, title, published_year) VALUES \
         (10, 1, 'A Wizard of Earthsea', 1968), \
         (11, 1, 'The Dispossessed', 1974), \
         (12, 2, 'Kindred', 1979), \
         (13, 99, 'Orphaned Book (no such author)', NULL)",
    )
    .await?;

    // Aggregate: author 3 has zero books (the LEFT JOIN keeps them with count 0).
    println!("book counts by author:");
    for row in conn.query::<BookCountsByAuthor>(()).await?.iter() {
        let row = row?;
        println!("  {} — {} book(s)", row.author, row.books);
    }

    // LEFT JOIN nullability: book 13's author is NULL (no matching author).
    println!("\nbooks with (maybe) an author:");
    for row in conn.query::<BooksWithMaybeAuthor>(()).await?.iter() {
        let row = row?;
        let author = match row.author {
            Some(name) => name,
            None => "(unknown author)", // this is why `author` is Option<String>
        };
        println!("  {} — {author}", row.title);
    }

    // Subquery: authors with >= 2 books.
    println!("\nauthors with >= 2 books:");
    for row in conn.query::<ProlificAuthors>((2i64,)).await?.iter() {
        println!("  {}", row?.name);
    }

    conn.close().await?;
    Ok(())
}
