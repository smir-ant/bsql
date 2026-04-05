//! Dynamic queries with optional WHERE clauses.
//!
//! Demonstrates how bsql handles optional filters without string concatenation.
//! Wrap any clause in `[...]` and give the parameter an `Option<T>` type.
//! When the value is `None`, the clause is omitted entirely.
//!
//! bsql expands every combination at compile time and validates each one
//! against the real database. 2 optional clauses = 4 SQL variants, all checked.
//!
//! Also shows sort enums: compile-time-validated ORDER BY from a Rust enum.
//!
//! ## Setup
//!
//! ```sql
//! CREATE TABLE tickets (
//!     id            SERIAL PRIMARY KEY,
//!     title         TEXT NOT NULL,
//!     department_id INT,
//!     assignee_id   INT,
//!     priority      INT NOT NULL DEFAULT 0,
//!     created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
//!     deleted_at    TIMESTAMPTZ
//! );
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb
//! cargo run --bin pg_dynamic
//! ```

use bsql::{BsqlError, Pool};

// Each variant maps to a SQL ORDER BY expression.
// The macro validates every variant's SQL at compile time.
#[bsql::sort]
enum TicketSort {
    #[sql("created_at ASC")]
    Newest,
    #[sql("created_at DESC")]
    Oldest,
    #[sql("priority DESC, created_at ASC")]
    Priority,
}

fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb")?;

    // ---------------------------------------------------------------
    // Optional WHERE clauses
    // ---------------------------------------------------------------
    // When dept is Some(3), the query includes "AND department_id = 3".
    // When dept is None, that clause is omitted entirely.
    // No string concatenation. No runtime SQL construction.
    let dept: Option<i32> = Some(3);
    let assignee: Option<i32> = None;

    let tickets = bsql::query!(
        "SELECT id, title, priority FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         [AND assignee_id = $assignee: Option<i32>]
         ORDER BY created_at DESC
         LIMIT 50"
    )
    .fetch(&pool)?;

    // With dept=Some(3), assignee=None, bsql runs:
    //   SELECT ... WHERE deleted_at IS NULL AND department_id = $1 ...
    println!("Found {} tickets for department 3:", tickets.len());
    for t in &tickets {
        println!("  [{}] {} (priority={})", t.id, t.title, t.priority);
    }

    // ---------------------------------------------------------------
    // Sort enum — compile-time validated ORDER BY
    // ---------------------------------------------------------------
    let sort = TicketSort::Priority;
    let limit = 20i64;

    let sorted = bsql::query!(
        "SELECT id, title, priority FROM tickets
         WHERE deleted_at IS NULL
         ORDER BY $[sort: TicketSort]
         LIMIT $limit: i64"
    )
    .fetch(&pool)?;

    println!("\nTop {} tickets by priority:", limit);
    for t in &sorted {
        println!("  [{}] {} (priority={})", t.id, t.title, t.priority);
    }

    // ---------------------------------------------------------------
    // Combining optional clauses + sort + pagination
    // ---------------------------------------------------------------
    // Real-world pattern: API endpoint with optional filters, sort, and paging.
    let dept: Option<i32> = None;
    let min_priority: Option<i32> = Some(5);
    let sort = TicketSort::Newest;
    let limit = 10i64;
    let offset = 0i64;

    let page = bsql::query!(
        "SELECT id, title, priority FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i32>]
         [AND priority >= $min_priority: Option<i32>]
         ORDER BY $[sort: TicketSort]
         LIMIT $limit: i64 OFFSET $offset: i64"
    )
    .fetch(&pool)?;

    println!("\nPage of high-priority tickets: {}", page.len());

    Ok(())
}
