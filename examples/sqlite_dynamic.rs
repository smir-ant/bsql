//! Dynamic queries with optional WHERE clauses in SQLite.
//!
//! Same syntax as PostgreSQL. Wrap clauses in `[...]` with `Option<T>` params.
//! bsql generates every combination at compile time and validates each.
//!
//! Key difference from PostgreSQL: SQLite uses `i64` for integer types.
//!
//! ## Setup
//!
//! ```sh
//! sqlite3 myapp.db "CREATE TABLE tickets (
//!     id            INTEGER PRIMARY KEY,
//!     title         TEXT NOT NULL,
//!     department_id INTEGER,
//!     assignee_id   INTEGER,
//!     priority      INTEGER NOT NULL DEFAULT 0,
//!     created_at    TEXT NOT NULL DEFAULT (datetime('now')),
//!     deleted_at    TEXT
//! );"
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=sqlite:./myapp.db
//! cargo run --bin sqlite_dynamic
//! ```

use bsql::{BsqlError, SqlitePool};

// Sort enum works identically for SQLite and PostgreSQL.
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
    let pool = SqlitePool::open("./myapp.db")?;

    // ---------------------------------------------------------------
    // Optional WHERE clauses — same syntax as PostgreSQL
    // ---------------------------------------------------------------
    // dept=Some(3) includes the clause; assignee=None omits it.
    let dept: Option<i64> = Some(3);
    let assignee: Option<i64> = None;

    let tickets = bsql::query!(
        "SELECT id, title, priority FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i64>]
         [AND assignee_id = $assignee: Option<i64>]
         ORDER BY created_at DESC
         LIMIT 50"
    )
    .fetch(&pool)?;

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
    let dept: Option<i64> = None;
    let min_priority: Option<i64> = Some(5);
    let sort = TicketSort::Newest;
    let limit = 10i64;
    let offset = 0i64;

    let page = bsql::query!(
        "SELECT id, title, priority FROM tickets
         WHERE deleted_at IS NULL
         [AND department_id = $dept: Option<i64>]
         [AND priority >= $min_priority: Option<i64>]
         ORDER BY $[sort: TicketSort]
         LIMIT $limit: i64 OFFSET $offset: i64"
    )
    .fetch(&pool)?;

    println!("\nPage of high-priority tickets: {}", page.len());

    Ok(())
}
