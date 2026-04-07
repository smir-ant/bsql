//! Sync mode example — no tokio, no async, maximum performance.
//!
//! Use this when:
//! - CLI tools, batch jobs, ETL pipelines
//! - Maximum single-query latency matters
//! - You don't need concurrent connections
//!
//! Cargo.toml:
//!   bsql = { version = "0.19", default-features = false, features = ["sync"] }
//!
//! Run: cargo run --example pg_sync

use bsql::{Pool, BsqlError};

fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb")?;

    // Simple query
    let id = 1i32;
    let user = bsql::query!("SELECT id, login, first_name FROM users WHERE id = $id: i32")
        .fetch_one(&pool)?;
    let r = user.get()?;
    println!("User: {} ({})", r.first_name, r.login);

    // Multiple rows
    let users = bsql::query!("SELECT id, login FROM users WHERE active = true ORDER BY id LIMIT 10")
        .fetch(&pool)?;
    for row in users.iter() {
        let r = row?;
        println!("  {} — {}", r.id, r.login);
    }

    // INSERT
    let login = "newuser";
    let first_name = "New";
    let last_name = "User";
    let email = "new@example.com";
    bsql::query!(
        "INSERT INTO users (login, first_name, last_name, email) VALUES ($login: &str, $first_name: &str, $last_name: &str, $email: &str)"
    ).run(&pool)?;
    println!("Inserted user: {login}");

    // Transaction
    let tx = pool.begin()?;
    bsql::query!("DELETE FROM users WHERE login = $login: &str")
        .defer(&tx)?;
    tx.commit()?;
    println!("Deleted user: {login}");

    Ok(())
}
