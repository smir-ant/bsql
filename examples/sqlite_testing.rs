//! # SQLite testing with #[bsql::test]
//!
//! Each test gets its own temporary SQLite database file.
//! Fixtures are applied, the test runs, then the file is deleted.
//! Zero shared state between tests — full parallelism.
//!
//! ## How it works
//!
//! The macro detects `SqlitePool` as the parameter type and automatically:
//! 1. Creates a unique temp file (`/tmp/bsql_test_{pid}_{counter}.db`)
//! 2. Opens a SqlitePool pointing to it
//! 3. Applies fixture SQL files (embedded at compile time)
//! 4. Passes the pool to your test function
//! 5. Deletes the file on cleanup (including WAL and SHM)
//!
//! ## Fixtures
//!
//! Put SQL files in `fixtures/` or `tests/fixtures/`:
//!
//! ```sql
//! -- fixtures/sqlite_schema.sql
//! CREATE TABLE todos (
//!     id INTEGER PRIMARY KEY,
//!     title TEXT NOT NULL,
//!     done INTEGER NOT NULL DEFAULT 0
//! );
//!
//! -- fixtures/sqlite_seed.sql
//! INSERT INTO todos (title) VALUES ('Buy milk'), ('Write docs');
//! ```
//!
//! ## Run
//!
//! ```bash
//! cargo test --features sqlite
//! ```

fn main() {
    println!("This example documents #[bsql::test] for SQLite.");
    println!("The actual tests are in the #[cfg(test)] module below.");
    println!();
    println!("Key differences from PostgreSQL:");
    println!("  - SQLite tests are sync (fn, not async fn)");
    println!("  - Isolation via temp file, not schema");
    println!("  - No BSQL_DATABASE_URL needed");
    println!("  - WAL/SHM files cleaned up automatically");
}

// The test module shows real usage patterns.
// Run with: cargo test --example sqlite_testing --features sqlite
#[cfg(test)]
mod tests {
    // Example test patterns (these won't compile without fixtures,
    // but they show the correct API):
    //
    // #[bsql::test(fixtures("sqlite_schema", "sqlite_seed"))]
    // fn test_todo_count(pool: bsql::SqlitePool) {
    //     let result = bsql::query!("SELECT COUNT(*) AS cnt FROM todos")
    //         .fetch_one(&pool).unwrap();
    //     assert_eq!(result.cnt, 2);
    // }
    //
    // #[bsql::test(fixtures("sqlite_schema"))]
    // fn test_empty_table(pool: bsql::SqlitePool) {
    //     let result = bsql::query!("SELECT COUNT(*) AS cnt FROM todos")
    //         .fetch_one(&pool).unwrap();
    //     assert_eq!(result.cnt, 0);
    // }
    //
    // #[bsql::test]
    // fn test_create_own_table(pool: bsql::SqlitePool) {
    //     pool.raw_execute("CREATE TABLE t (id INTEGER)").unwrap();
    //     // Table exists only in this test's temp DB
    // }
}
