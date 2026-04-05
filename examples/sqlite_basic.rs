//! Basic SQLite CRUD operations with bsql.
//!
//! Same API as PostgreSQL: `.fetch()`, `.run()`, `.pop()`.
//! bsql validates SQLite queries at compile time against the real database
//! file, just like it does for PostgreSQL.
//!
//! Key difference: SQLite uses `i64` for INTEGER PRIMARY KEY (ROWID alias),
//! where PostgreSQL SERIAL uses `i32`.
//!
//! ## Setup
//!
//! ```sh
//! sqlite3 myapp.db "CREATE TABLE users (
//!     id    INTEGER PRIMARY KEY,
//!     name  TEXT NOT NULL,
//!     email TEXT NOT NULL
//! );"
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=sqlite:./myapp.db
//! cargo run --bin sqlite_basic
//! ```

use bsql::{BsqlError, SqlitePool};

fn main() -> Result<(), BsqlError> {
    // SqlitePool::open() opens a pool with 1 writer + 4 reader connections.
    // WAL mode, mmap, and page cache are configured automatically.
    let pool = SqlitePool::open("./myapp.db")?;

    // ---------------------------------------------------------------
    // INSERT — .run() returns affected row count, same as PostgreSQL
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
    let users = bsql::query!("SELECT id, name, email FROM users")
        .fetch(&pool)?;

    for user in &users {
        println!("id={}, name={}, email={}", user.id, user.name, user.email);
    }

    // ---------------------------------------------------------------
    // SELECT one — same .pop() pattern as PostgreSQL
    // ---------------------------------------------------------------
    // Note: SQLite INTEGER PRIMARY KEY is i64, not i32.
    let id = 1i64;
    let user = bsql::query!(
        "SELECT id, name, email FROM users WHERE id = $id: i64 LIMIT 1"
    )
    .fetch(&pool)?
    .pop();

    if let Some(user) = user {
        println!("Found: {} <{}>", user.name, user.email);
    }

    // ---------------------------------------------------------------
    // UPDATE
    // ---------------------------------------------------------------
    let new_email = "alice@newdomain.com";
    let updated = bsql::query!(
        "UPDATE users SET email = $new_email: &str WHERE id = $id: i64"
    )
    .run(&pool)?;
    println!("Updated {updated} row(s)");

    // ---------------------------------------------------------------
    // DELETE
    // ---------------------------------------------------------------
    let deleted = bsql::query!("DELETE FROM users WHERE id = $id: i64")
        .run(&pool)?;
    println!("Deleted {deleted} row(s)");

    Ok(())
}
