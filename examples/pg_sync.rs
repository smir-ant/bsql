//! Sync mode example — no tokio, no async, maximum performance.
//!
//! Use this when:
//! - CLI tools, batch jobs, ETL pipelines
//! - Maximum single-query latency matters
//! - You don't need concurrent connections
//!
//! Cargo.toml:
//!   bsql = { version = "0.22", default-features = false, features = ["sync"] }
//!
//! Run: cargo run --example pg_sync

use bsql::{Pool, BsqlError};

fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb")?;

    // Simple query
    let id = 1i32;
    let user = bsql::query!("SELECT id, login, first_name FROM users WHERE id = $id: i32")
        .fetch_one(&pool)?;
    println!("User: {} ({})", user.first_name, user.login);

    // Multiple rows
    let users = bsql::query!("SELECT id, login FROM users WHERE active = true ORDER BY id LIMIT 10")
        .fetch(&pool)?;
    for r in &users {
        println!("  {} — {}", r.id, r.login);
    }

    // INSERT
    let login = "newuser";
    let first_name = "New";
    let last_name = "User";
    let email = "new@example.com";
    bsql::query!(
        "INSERT INTO users (login, first_name, last_name, email) VALUES ($login: &str, $first_name: &str, $last_name: &str, $email: &str)"
    ).execute(&pool)?;
    println!("Inserted user: {login}");

    // Transaction
    let mut tx = pool.begin()?;
    bsql::query!("DELETE FROM users WHERE login = $login: &str")
        .defer(&mut tx)?;
    tx.commit()?;
    println!("Deleted user: {login}");

    Ok(())
}
