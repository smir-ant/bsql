//! Basic SQLite operations with bsql.
//!
//! Demonstrates: SqlitePool::open, fetch, fetch_optional, run.
//!
//! bsql validates SQLite queries at compile time against the real database file,
//! just like it does for PostgreSQL. Same query! macro, same guarantees.
//!
//! Requires the `sqlite` feature and a SQLite database file:
//!   sqlite3 myapp.db "CREATE TABLE users (id INTEGER PRIMARY KEY, login TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1);"
//!
//! Run:
//!   BSQL_DATABASE_URL=sqlite:./myapp.db cargo run --bin sqlite_basic

use bsql::{BsqlError, SqlitePool};

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    // Open a SQLite pool. The path is relative to the working directory.
    // bsql automatically configures WAL mode, mmap, and page cache.
    let pool = SqlitePool::open("./myapp.db")?; // also available: SqlitePool::connect("./myapp.db")

    // --- INSERT ---
    let login = "alice";
    let _affected = bsql::query!(
        "INSERT INTO users (login) VALUES ($login: &str)"
    )
    .run(&pool) // also available: .execute(&pool)
    .await?;
    println!("Inserted user '{login}'");

    // --- SELECT one row ---
    // SQLite uses i64 for INTEGER PRIMARY KEY (ROWID alias).
    // Use fetch + index. For power users: .fetch_one(&pool) errors if not exactly 1 row.
    let id = 1i64;
    let users = bsql::query!(
        "SELECT id, login, active FROM users WHERE id = $id: i64"
    )
    .fetch(&pool) // also available: .fetch_all(&pool)
    .await?;
    let user = &users[0];
    println!("User: {} (id={}, active={})", user.login, user.id, user.active);

    // --- SELECT optional ---
    let lookup_id = 9999i64;
    let optional_user = bsql::query!(
        "SELECT id, login FROM users WHERE id = $lookup_id: i64"
    )
    .fetch_optional(&pool)
    .await?;
    match optional_user {
        Some(u) => println!("Found: {}", u.login),
        None => println!("No user with id={lookup_id}"),
    }

    // --- SELECT all ---
    let users = bsql::query!("SELECT id, login FROM users")
        .fetch(&pool) // also available: .fetch_all(&pool)
        .await?;
    println!("Total users: {}", users.len());
    for u in &users {
        println!("  id={}, login={}", u.id, u.login);
    }

    // --- UPDATE ---
    let target_id = 1i64;
    let new_login = "alice_updated";
    let updated = bsql::query!(
        "UPDATE users SET login = $new_login: &str WHERE id = $target_id: i64"
    )
    .run(&pool) // also available: .execute(&pool)
    .await?;
    println!("Updated {updated} row(s)");

    // --- DELETE ---
    let delete_id = 1i64;
    let deleted = bsql::query!(
        "DELETE FROM users WHERE id = $delete_id: i64"
    )
    .run(&pool) // also available: .execute(&pool)
    .await?;
    println!("Deleted {deleted} row(s)");

    Ok(())
}
