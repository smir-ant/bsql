//! Basic PostgreSQL CRUD operations with bsql.
//!
//! Demonstrates the three core methods:
//!   - `.fetch(&pool)` for SELECT (returns Vec<Row>)
//!   - `.run(&pool)` for INSERT/UPDATE/DELETE (returns affected row count)
//!   - `.pop()` on fetch results for single-row lookups
//!
//! Every query is validated at compile time against your real database.
//! If it compiles, the SQL is correct.
//!
//! ## Setup
//!
//! ```sql
//! CREATE TABLE users (
//!     id    SERIAL PRIMARY KEY,
//!     name  TEXT NOT NULL,
//!     email TEXT NOT NULL
//! );
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb
//! cargo run --bin pg_basic
//! ```

use bsql::{BsqlError, Pool};

fn main() -> Result<(), BsqlError> {
    // Pool::connect() opens a connection pool to PostgreSQL.
    // The URL here is for runtime; compile-time validation uses BSQL_DATABASE_URL.
    let pool = Pool::connect("postgres://user:pass@localhost/mydb")?;

    // ---------------------------------------------------------------
    // INSERT — .run() returns the number of affected rows (u64)
    // ---------------------------------------------------------------
    let name = "alice";
    let email = "alice@example.com";
    let affected = bsql::query!(
        "INSERT INTO users (name, email) VALUES ($name: &str, $email: &str)"
    )
    .run(&pool)?;
    println!("Inserted {affected} row(s)");

    // ---------------------------------------------------------------
    // SELECT all — .fetch() returns Vec<Row>
    // ---------------------------------------------------------------
    // Each row is a generated struct with typed fields matching the columns.
    // user.id: i32, user.name: String, user.email: String
    let users = bsql::query!("SELECT id, name, email FROM users")
        .fetch(&pool)?;

    for user in &users {
        println!("id={}, name={}, email={}", user.id, user.name, user.email);
    }

    // ---------------------------------------------------------------
    // SELECT one — .fetch() + .pop() for single-row lookups
    // ---------------------------------------------------------------
    // Use LIMIT 1 in SQL, then .pop() to get Option<Row>.
    let id = 1i32;
    let user = bsql::query!(
        "SELECT id, name, email FROM users WHERE id = $id: i32 LIMIT 1"
    )
    .fetch(&pool)?
    .pop(); // Option<Row> — None if no match

    if let Some(user) = user {
        println!("Found: {} <{}>", user.name, user.email);
    }

    // ---------------------------------------------------------------
    // UPDATE — .run() returns how many rows were changed
    // ---------------------------------------------------------------
    let new_email = "alice@newdomain.com";
    let updated = bsql::query!(
        "UPDATE users SET email = $new_email: &str WHERE id = $id: i32"
    )
    .run(&pool)?;
    println!("Updated {updated} row(s)");

    // ---------------------------------------------------------------
    // DELETE — .run() returns how many rows were removed
    // ---------------------------------------------------------------
    let deleted = bsql::query!("DELETE FROM users WHERE id = $id: i32")
        .run(&pool)?;
    println!("Deleted {deleted} row(s)");

    Ok(())
}
