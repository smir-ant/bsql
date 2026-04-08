//! SQLite with `query_as!` — same compile-time safety as PostgreSQL.
//!
//! `query_as!` maps SELECT results into your own structs. Field names
//! must match column names, and types are verified at compile time
//! against the real SQLite database file.
//!
//! Key difference from PostgreSQL: SQLite uses `i64` for INTEGER PRIMARY KEY
//! (ROWID alias), where PostgreSQL SERIAL uses `i32`.
//!
//! ## Setup
//!
//! ```sh
//! sqlite3 myapp.db <<'SQL'
//! CREATE TABLE todos (
//!     id    INTEGER PRIMARY KEY,
//!     title TEXT NOT NULL,
//!     done  INTEGER NOT NULL DEFAULT 0
//! );
//! INSERT INTO todos (title, done) VALUES ('Write examples', 1);
//! INSERT INTO todos (title, done) VALUES ('Review PR', 0);
//! INSERT INTO todos (title, done) VALUES ('Deploy release', 0);
//! SQL
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=sqlite:./myapp.db
//! cargo run --bin sqlite_query_as
//! ```

use bsql::{BsqlError, SqlitePool};

// SQLite booleans are stored as INTEGER (0 or 1).
// bsql maps them to bool automatically.
#[derive(Debug)]
struct Todo {
    id: i64,       // SQLite INTEGER PRIMARY KEY is i64
    title: String,
    done: bool,    // SQLite INTEGER 0/1 maps to bool
}

// Aggregate result struct.
#[derive(Debug)]
struct TodoStats {
    total: i64,
    completed: i64,
}

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    let pool = SqlitePool::open("./myapp.db").await?;

    // ---------------------------------------------------------------
    // Fetch all todos into Vec<Todo>
    // ---------------------------------------------------------------
    let todos = bsql::query_as!(Todo,
        "SELECT id, title, done FROM todos ORDER BY id"
    )
    .fetch(&pool)?;

    println!("All todos:");
    for t in &todos {
        let mark = if t.done { "[x]" } else { "[ ]" };
        println!("  {} #{}: {}", mark, t.id, t.title);
    }

    // ---------------------------------------------------------------
    // Fetch incomplete todos only
    // ---------------------------------------------------------------
    let done = false;
    let pending = bsql::query_as!(Todo,
        "SELECT id, title, done FROM todos WHERE done = $done: bool ORDER BY id"
    )
    .fetch(&pool)?;

    println!("\nPending ({} items):", pending.len());
    for t in &pending {
        println!("  #{}: {}", t.id, t.title);
    }

    // ---------------------------------------------------------------
    // Aggregate query mapped to TodoStats
    // ---------------------------------------------------------------
    let stats = bsql::query_as!(TodoStats,
        "SELECT COUNT(*) AS total,
                SUM(CASE WHEN done THEN 1 ELSE 0 END) AS completed
         FROM todos"
    )
    .fetch_one(&pool)?;

    println!("\nStats: {}/{} completed", stats.completed, stats.total);

    Ok(())
}
