//! Sort enums — type-safe ORDER BY.
//!
//! Define allowed sort orders as a Rust enum. Each variant maps to
//! a SQL ORDER BY fragment. Validated at compile time — column typos
//! are compile errors, not runtime bugs discovered in production.
//!
//! The `#[bsql::sort]` attribute generates:
//!   - `Debug, Clone, Copy, PartialEq, Eq, Hash` derives
//!   - A `sql(&self) -> &'static str` method returning the SQL fragment
//!   - `Display` impl that formats as the SQL fragment
//!
//! At compile time, bsql validates every variant's SQL fragment against
//! the real database. If `"priority DECS"` is a typo, it won't compile.
//!
//! ## Setup
//!
//! ```sql
//! CREATE TABLE tickets (
//!     id         SERIAL PRIMARY KEY,
//!     title      TEXT NOT NULL,
//!     priority   INT NOT NULL DEFAULT 0,
//!     created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
//!     deleted_at TIMESTAMPTZ
//! );
//! INSERT INTO tickets (title, priority)
//!     VALUES ('Fix login bug', 8), ('Add dark mode', 3), ('Upgrade deps', 5);
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb
//! cargo run --bin pg_sort_enum
//! ```

use bsql::{BsqlError, Pool};

// Each variant maps to a SQL ORDER BY expression.
// The #[sql("...")] attribute is the actual SQL that gets spliced into the query.
// bsql validates each fragment at compile time via PREPARE.
#[bsql::sort]
pub enum TicketSort {
    // Most recent first
    #[sql("created_at DESC")]
    Newest,

    // Oldest first
    #[sql("created_at ASC")]
    Oldest,

    // High priority first, then newest within same priority
    #[sql("priority DESC, created_at DESC")]
    HighPriority,

    // Low priority first (useful for backlog views)
    #[sql("priority ASC, created_at ASC")]
    LowPriority,
}

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    // ---------------------------------------------------------------
    // Basic sort enum usage
    // ---------------------------------------------------------------
    // The sort variant is type-safe and exhaustive. If you add a new
    // variant, `match` arms will force you to handle it everywhere.
    let sort = TicketSort::Newest;
    let tickets = bsql::query!(
        "SELECT id, title, priority FROM tickets
         WHERE deleted_at IS NULL
         ORDER BY $[sort: TicketSort]
         LIMIT 20"
    )
    .fetch(&pool).await?;

    println!("Tickets sorted by {:?}:", sort);
    for t in &tickets {
        println!("  #{}: {} (priority={})", t.id, t.title, t.priority);
    }

    // ---------------------------------------------------------------
    // Different sort — same query, different ORDER BY
    // ---------------------------------------------------------------
    let sort = TicketSort::HighPriority;
    let tickets = bsql::query!(
        "SELECT id, title, priority FROM tickets
         WHERE deleted_at IS NULL
         ORDER BY $[sort: TicketSort]
         LIMIT 20"
    )
    .fetch(&pool).await?;

    println!("\nTickets sorted by {:?}:", sort);
    for t in &tickets {
        println!("  #{}: {} (priority={})", t.id, t.title, t.priority);
    }

    // ---------------------------------------------------------------
    // Real-world: sort from user input (e.g., API query parameter)
    // ---------------------------------------------------------------
    // In a web handler, you'd parse the sort parameter from the request.
    // The enum ensures only valid sort orders are accepted.
    let sort_param = "high_priority"; // imagine this comes from ?sort=high_priority
    let sort = match sort_param {
        "newest" => TicketSort::Newest,
        "oldest" => TicketSort::Oldest,
        "high_priority" => TicketSort::HighPriority,
        "low_priority" => TicketSort::LowPriority,
        _ => TicketSort::Newest, // safe default
    };

    let limit = 10i64;
    let tickets = bsql::query!(
        "SELECT id, title, priority FROM tickets
         WHERE deleted_at IS NULL
         ORDER BY $[sort: TicketSort]
         LIMIT $limit: i64"
    )
    .fetch(&pool).await?;

    println!("\nAPI response ({sort_param}, limit={limit}):");
    for t in &tickets {
        println!("  #{}: {} (priority={})", t.id, t.title, t.priority);
    }

    Ok(())
}
