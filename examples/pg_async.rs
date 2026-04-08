//! Async mode example — tokio runtime, non-blocking TCP I/O.
//!
//! Use this when:
//! - Web servers (axum, actix-web, warp)
//! - High concurrency (100+ concurrent database queries)
//! - Remote PostgreSQL over TCP (not localhost UDS)
//!
//! Cargo.toml:
//!   bsql = { version = "0.19", features = ["async"] }
//!   tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
//!
//! Run: cargo run --example pg_async

use bsql::{Pool, BsqlError};

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    // Simple query — .await on every database operation
    let id = 1i32;
    let user = bsql::query!("SELECT id, login, first_name FROM users WHERE id = $id: i32")
        .fetch_one(&pool).await?;
    let r = user.get()?;
    println!("User: {} ({})", r.first_name, r.login);

    // Multiple rows
    let users = bsql::query!("SELECT id, login FROM users WHERE active = true ORDER BY id LIMIT 10")
        .fetch(&pool).await?;
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
    ).run(&pool).await?;
    println!("Inserted user: {login}");

    // Transaction
    let mut tx = pool.begin().await?;
    bsql::query!("DELETE FROM users WHERE login = $login: &str")
        .defer(&mut tx).await?;
    tx.commit().await?;
    println!("Deleted user: {login}");

    // Concurrent queries — the real power of async
    let handles: Vec<_> = (1..=5).map(|i| {
        let pool = pool.clone();
        tokio::spawn(async move {
            let id = i as i32;
            let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
                .fetch_one(&pool).await.ok();
            (id, user)
        })
    }).collect();

    for handle in handles {
        let (id, result) = handle.await.unwrap();
        match result {
            Some(user) => {
                let r = user.get().unwrap();
                println!("Concurrent fetch {id}: {}", r.login);
            }
            None => println!("Concurrent fetch {id}: not found"),
        }
    }

    Ok(())
}
