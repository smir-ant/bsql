//! Mapping query results to your own structs with `query_as!`.
//!
//! `query_as!` validates field names and types at compile time.
//! If your struct has a field that doesn't match a SELECT column,
//! or the types are incompatible — it won't compile.
//!
//! Unlike `query!` which generates an anonymous struct, `query_as!` maps
//! results directly into your named struct via struct literal construction.
//! No runtime reflection, no derive macros — just rustc verifying types.
//!
//! ## Setup
//!
//! ```sql
//! CREATE TABLE users (
//!     id     SERIAL PRIMARY KEY,
//!     login  TEXT NOT NULL,
//!     email  TEXT NOT NULL,
//!     active BOOL NOT NULL DEFAULT true
//! );
//! CREATE TABLE orders (
//!     id      SERIAL PRIMARY KEY,
//!     user_id INT NOT NULL REFERENCES users(id),
//!     total   INT NOT NULL
//! );
//! INSERT INTO users (login, email) VALUES ('alice', 'alice@example.com');
//! INSERT INTO users (login, email, active) VALUES ('bob', 'bob@example.com', false);
//! INSERT INTO orders (user_id, total) VALUES (1, 4200), (1, 1500);
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb
//! cargo run --bin pg_query_as
//! ```

use bsql::{BsqlError, Pool};

// Your struct — no derive macros needed for bsql.
// Field names must match SELECT column names exactly.
// Field types must be compatible with the PostgreSQL column types.
#[derive(Debug)]
struct User {
    id: i32,
    login: String,
    email: String,
    active: bool,
}

// Aggregates work too. COUNT(*) returns i64 in PostgreSQL.
// Because COUNT(*) never returns NULL (even on empty tables), bsql
// infers i64 — not Option<i64>. This is smart NULL inference at work.
#[derive(Debug)]
struct UserOrderSummary {
    login: String,
    order_count: i64,
}

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    // ---------------------------------------------------------------
    // Basic: map SELECT columns directly to struct fields
    // ---------------------------------------------------------------
    // At compile time, bsql checks that User has fields id, login, email,
    // active — and that each field's type matches the column type.
    let id = 1i32;
    let user = bsql::query_as!(User,
        "SELECT id, login, email, active FROM users WHERE id = $id: i32"
    )
    .fetch_one(&pool).await?;
    println!("User: {:?}", user);

    // ---------------------------------------------------------------
    // Fetch all rows into a Vec<User>
    // ---------------------------------------------------------------
    let users = bsql::query_as!(User,
        "SELECT id, login, email, active FROM users ORDER BY id"
    )
    .fetch(&pool).await?;

    for u in &users {
        println!("  {} ({}) — active={}", u.login, u.email, u.active);
    }

    // ---------------------------------------------------------------
    // JOIN + aggregate — COUNT(*) is i64, not Option<i64>
    // ---------------------------------------------------------------
    // Smart NULL inference: COUNT(*) can never be NULL, so bsql maps
    // it to i64 directly. No unwrap() needed.
    let summaries = bsql::query_as!(UserOrderSummary,
        "SELECT u.login, COUNT(o.id) AS order_count
         FROM users u LEFT JOIN orders o ON u.id = o.user_id
         GROUP BY u.login
         ORDER BY order_count DESC"
    )
    .fetch(&pool).await?;

    for s in &summaries {
        println!("{}: {} orders", s.login, s.order_count);
    }

    // ---------------------------------------------------------------
    // fetch_optional — returns Option<User>
    // ---------------------------------------------------------------
    let id = 999i32;
    let maybe_user = bsql::query_as!(User,
        "SELECT id, login, email, active FROM users WHERE id = $id: i32"
    )
    .fetch_optional(&pool).await?;

    match maybe_user {
        Some(u) => println!("Found: {:?}", u),
        None => println!("No user with id={id}"),
    }

    Ok(())
}
