//! Testing with `#[bsql::test]` — schema-isolated database tests.
//!
//! Each test gets its own PostgreSQL schema (~2ms setup).
//! Fixtures are SQL files applied before the test runs.
//! Cleanup happens automatically — even on panic.
//!
//! ## How it works
//!
//! 1. Before the test: creates a unique schema (`bsql_test_{uuid}`)
//! 2. Sets `search_path` to the new schema
//! 3. Applies fixture SQL files in order
//! 4. Passes a connected `Pool` to the test function
//! 5. After the test: drops the schema (even on panic)
//!
//! Tests run in parallel — each in a separate schema, no shared state,
//! no flaky tests from interleaved data.
//!
//! ## Setup
//!
//! 1. Create fixture files under `fixtures/` or `tests/fixtures/`:
//!
//! ```
//! fixtures/
//!   schema.sql    — CREATE TABLE statements
//!   seed.sql      — INSERT test data
//! ```
//!
//! 2. Set `BSQL_DATABASE_URL` environment variable
//!
//! 3. Write tests with `#[bsql::test(fixtures("schema", "seed"))]`
//!
//! ## Example fixtures
//!
//! `fixtures/schema.sql`:
//! ```sql
//! CREATE TABLE users (
//!     id    SERIAL PRIMARY KEY,
//!     name  TEXT NOT NULL,
//!     email TEXT
//! );
//! ```
//!
//! `fixtures/seed.sql`:
//! ```sql
//! INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com');
//! INSERT INTO users (name, email) VALUES ('Bob', 'bob@test.com');
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb
//! cargo test
//! ```
//!
//! ## Note
//!
//! This file is a documentation example — it demonstrates the patterns
//! but cannot be executed as a binary. Copy these patterns into your
//! `tests/` directory to use them.

// This example shows the test patterns but is structured as a binary
// for documentation purposes. The actual #[bsql::test] attribute only
// works inside `#[cfg(test)]` modules or test files.

fn main() {
    println!("This is a documentation example for #[bsql::test].");
    println!("Copy these patterns into your tests/ directory.");
    println!();
    println!("Example test:");
    println!();
    println!("  #[bsql::test(fixtures(\"schema\", \"seed\"))]");
    println!("  async fn test_user_exists(pool: bsql::Pool) {{");
    println!("      let id = 1i32;");
    println!("      let user = bsql::query!(\"SELECT name FROM users WHERE id = $id: i32\")");
    println!("          .fetch_one(&pool).await.unwrap();");
    println!("      assert_eq!(user.name, \"Alice\");");
    println!("  }}");
    println!();
    println!("Key points:");
    println!("  - Each test gets its own schema (~2ms setup)");
    println!("  - Tests run in parallel safely");
    println!("  - Fixtures are applied in order");
    println!("  - Cleanup is automatic, even on panic");
}

// Below are the test patterns for reference.
// In a real project, these would be in tests/*.rs files.

#[cfg(test)]
mod tests {
    use bsql::Pool;

    /// Each test runs in its own schema — no shared state, no flaky tests.
    /// The `pool` parameter is a connected Pool scoped to the isolated schema.
    #[bsql::test(fixtures("schema", "seed"))]
    async fn test_user_exists(pool: Pool) {
        let id = 1i32;
        let user = bsql::query!("SELECT name FROM users WHERE id = $id: i32")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(user.name, "Alice");
    }

    /// Tests run in parallel — each in a separate schema.
    /// No locks, no serialization, no cleanup between tests.
    #[bsql::test(fixtures("schema", "seed"))]
    async fn test_user_count(pool: Pool) {
        let result = bsql::query!("SELECT COUNT(*) AS cnt FROM users")
            .fetch_one(&pool).await.unwrap();
        // COUNT(*) returns i64, not Option<i64> — smart NULL inference
        assert_eq!(result.cnt, 2i64);
    }

    /// Test without fixtures — starts with an empty schema.
    /// Useful when the test creates its own tables.
    #[bsql::test]
    async fn test_create_table(pool: Pool) {
        pool.raw_execute("CREATE TABLE temp (id INT)").await.unwrap();
        bsql::query!("INSERT INTO temp (id) VALUES (1)")
            .run(&pool).await.unwrap();

        let rows = bsql::query!("SELECT id FROM temp")
            .fetch(&pool).await.unwrap();
        assert_eq!(rows.len(), 1);
    }

    /// Mutations in one test are invisible to other tests.
    /// This test deletes all users, but other tests still see the seed data.
    #[bsql::test(fixtures("schema", "seed"))]
    async fn test_delete_is_isolated(pool: Pool) {
        bsql::query!("DELETE FROM users")
            .run(&pool).await.unwrap();

        let result = bsql::query!("SELECT COUNT(*) AS cnt FROM users")
            .fetch_one(&pool).await.unwrap();
        assert_eq!(result.cnt, 0i64);
    }
}
