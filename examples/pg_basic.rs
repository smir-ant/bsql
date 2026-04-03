//! Basic PostgreSQL operations with bsql.
//!
//! Demonstrates: Pool::connect, fetch, fetch_optional, run.
//!
//! Requires a running PostgreSQL instance with a `users` table:
//!   CREATE TABLE users (id SERIAL PRIMARY KEY, login TEXT NOT NULL, active BOOLEAN NOT NULL DEFAULT true);
//!
//! Run:
//!   BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --bin pg_basic

use bsql::{BsqlError, Pool};

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    // Connect to PostgreSQL. The URL here is for runtime; compile-time
    // validation uses the BSQL_DATABASE_URL environment variable.
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    // --- INSERT a new user ---
    let login = "alice";
    let _affected = bsql::query!(
        "INSERT INTO users (login) VALUES ($login: &str)"
    )
    .run(&pool) // also available: .execute(&pool)
    .await?;
    println!("Inserted user '{login}'");

    // --- SELECT one row ---
    // Use fetch + LIMIT 1 in SQL, then index into the result.
    // For power users: .fetch_one(&pool) errors if not exactly 1 row.
    let id = 1i32;
    let users = bsql::query!(
        "SELECT id, login, active FROM users WHERE id = $id: i32"
    )
    .fetch(&pool) // also available: .fetch_all(&pool)
    .await?;
    let user = &users[0];
    println!("User: {} (id={}, active={})", user.login, user.id, user.active);

    // --- SELECT optional ---
    // fetch_optional returns None if no rows match.
    let lookup_id = 9999i32;
    let optional_user = bsql::query!(
        "SELECT id, login FROM users WHERE id = $lookup_id: i32"
    )
    .fetch_optional(&pool)
    .await?;
    match optional_user {
        Some(u) => println!("Found: {}", u.login),
        None => println!("No user with id={lookup_id}"),
    }

    // --- SELECT all rows ---
    let users = bsql::query!("SELECT id, login FROM users")
        .fetch(&pool) // also available: .fetch_all(&pool)
        .await?;
    println!("Total users: {}", users.len());
    for u in &users {
        println!("  id={}, login={}", u.id, u.login);
    }

    // --- UPDATE ---
    let target_id = 1i32;
    let new_login = "alice_updated";
    let updated = bsql::query!(
        "UPDATE users SET login = $new_login: &str WHERE id = $target_id: i32"
    )
    .run(&pool) // also available: .execute(&pool)
    .await?;
    println!("Updated {updated} row(s)");

    // --- DELETE ---
    let delete_id = 1i32;
    let deleted = bsql::query!(
        "DELETE FROM users WHERE id = $delete_id: i32"
    )
    .run(&pool) // also available: .execute(&pool)
    .await?;
    println!("Deleted {deleted} row(s)");

    Ok(())
}
