//! Dynamic queries with optional WHERE clauses in bsql.
//!
//! Demonstrates: Optional clauses [AND ...], sort enums, pagination.
//!
//! bsql expands optional clauses at compile time into every combination,
//! validating each against the real database. No string concatenation,
//! no runtime SQL construction.
//!
//! Requires a PostgreSQL instance with a `tickets` table:
//!   CREATE TABLE tickets (
//!     id SERIAL PRIMARY KEY,
//!     title TEXT NOT NULL,
//!     department_id INT,
//!     assignee_id INT,
//!     priority INT NOT NULL DEFAULT 0,
//!     created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
//!     deleted_at TIMESTAMPTZ
//!   );
//!
//! Run:
//!   BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --bin pg_dynamic

use bsql::{BsqlError, Pool};

// Define a sort enum. Each variant maps to a SQL ORDER BY expression.
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

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    // --- Optional WHERE clauses ---
    // Wrap clauses in [...]. When the Option parameter is None, the clause
    // is omitted entirely. When Some, it's included.
    //
    // bsql generates every combination at compile time:
    //   (None, None)   -> SELECT ... WHERE deleted_at IS NULL
    //   (Some, None)   -> SELECT ... WHERE deleted_at IS NULL AND department_id = $1
    //   (None, Some)   -> SELECT ... WHERE deleted_at IS NULL AND assignee_id = $1
    //   (Some, Some)   -> SELECT ... WHERE deleted_at IS NULL AND department_id = $1 AND assignee_id = $2
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
    .fetch_all(&pool)
    .await?;

    println!("Found {} tickets for department 3:", tickets.len());
    for t in &tickets {
        println!("  [{}] {} (priority={})", t.id, t.title, t.priority);
    }

    // --- Sort enum ---
    // The sort parameter controls ORDER BY without any string manipulation.
    let sort = TicketSort::Priority;
    let limit = 20i64;
    let sorted = bsql::query!(
        "SELECT id, title, priority FROM tickets
         WHERE deleted_at IS NULL
         ORDER BY $[sort: TicketSort]
         LIMIT $limit: i64"
    )
    .fetch_all(&pool)
    .await?;

    println!("\nTop {} tickets by priority:", limit);
    for t in &sorted {
        println!("  [{}] {} (priority={})", t.id, t.title, t.priority);
    }

    // --- Combining optional clauses + sort + pagination ---
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
    .fetch_all(&pool)
    .await?;

    println!("\nPage of high-priority tickets: {}", page.len());

    Ok(())
}
