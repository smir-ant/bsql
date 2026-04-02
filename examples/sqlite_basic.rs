//! Basic SQLite operations with bsql.
//!
//! Demonstrates: SqlitePool::connect, fetch_one, fetch_all, fetch_optional, execute.
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
    // Connect to SQLite. The path is relative to the working directory.
    // bsql automatically configures WAL mode, mmap, and page cache.
    let pool = SqlitePool::connect("./myapp.db")?;

    // --- INSERT ---
    let login = "alice";
    let _affected = bsql::query!(
        "INSERT INTO users (login) VALUES ($login: &str)"
    )
    .execute(&pool)
    .await?;
    println!("Inserted user '{login}'");

    // --- SELECT one row ---
    // SQLite uses i64 for INTEGER PRIMARY KEY (ROWID alias).
    let id = 1i64;
    let user = bsql::query!(
        "SELECT id, login, active FROM users WHERE id = $id: i64"
    )
    .fetch_one(&pool)
    .await?;
    println!("User: {} (id={}, active={})", user.login, user.id, user.active);

    // --- SELECT optional ---
    let maybe_id = 9999i64;
    let maybe_user = bsql::query!(
        "SELECT id, login FROM users WHERE id = $maybe_id: i64"
    )
    .fetch_optional(&pool)
    .await?;
    match maybe_user {
        Some(u) => println!("Found: {}", u.login),
        None => println!("No user with id={maybe_id}"),
    }

    // --- SELECT all ---
    let users = bsql::query!("SELECT id, login FROM users")
        .fetch_all(&pool)
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
    .execute(&pool)
    .await?;
    println!("Updated {updated} row(s)");

    // --- DELETE ---
    let delete_id = 1i64;
    let deleted = bsql::query!(
        "DELETE FROM users WHERE id = $delete_id: i64"
    )
    .execute(&pool)
    .await?;
    println!("Deleted {deleted} row(s)");

    Ok(())
}
