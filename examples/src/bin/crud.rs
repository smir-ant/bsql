//! # crud — INSERT / UPDATE / DELETE / SELECT, all compile-checked
//!
//! The four data-manipulation shapes through the typed flagship: INSERT with
//! `RETURNING` (read the generated/echoed row back via `query_one`), UPDATE via
//! the typed `execute` verb (affected-row COUNT), SELECT via `query_one` /
//! `query_opt`, and DELETE via `execute`. A write carrier must be a SELECT or a
//! `… RETURNING` (the macro rejects a bare non-returning write), so every write
//! stays row-typed.
//!
//! Features/verbs: `query!`, typed `query_one` / `query_opt` / `execute`.
//!
//! Backend: PostgreSQL — needs a live server. Uses a session TEMP table so it is
//! idempotent and parallel-safe (never touches your real `orders` table).
//! ```bash
//! export BSQL_EXAMPLES_DSN='postgres://USER@127.0.0.1:5432/postgres'
//! cargo run -p bsql-examples --bin crud
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "example/teaching code: unwrap/expect/panic surface failure loudly and keep the code readable"
)]
#![forbid(unsafe_code)]

use bsql::pg::{ConnectConfig, Connection};

// INSERT ... RETURNING: a write that reads a row back. `query_one` decodes the
// RETURNING row into the owned record `{ id: i64, status: String }`.
bsql::query!(
    InsertOrder,
    "INSERT INTO orders (id, user_id, total, status) VALUES ($1, $2, $3, $4) RETURNING id, status"
);
// UPDATE ... RETURNING: a valid write carrier. `execute` runs it and returns the
// AFFECTED-ROW count (the RETURNING rows are read and discarded).
bsql::query!(SetStatus, "UPDATE orders SET status = $2 WHERE id = $1 RETURNING id");
// A plain SELECT for read-back.
bsql::query!(OrderById, "SELECT id, user_id, total, status FROM orders WHERE id = $1");
// DELETE ... RETURNING: `execute` returns how many rows were removed.
bsql::query!(DeleteOrder, "DELETE FROM orders WHERE id = $1 RETURNING id");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::connect(&ConnectConfig::from_dsn(&bsql_examples::dsn())?).await?;

    // A session-private TEMP table SHADOWS the migration `orders` table: the
    // `query!` carriers were validated against `orders`' columns, and a TEMP
    // table with the same columns resolves first in the search path — so this
    // example is idempotent and never pollutes your real table.
    conn.execute_raw(
        "CREATE TEMP TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, \
         total INTEGER, status TEXT NOT NULL DEFAULT 'pending')",
    )
    .await?;

    // CREATE: insert and read the RETURNING row back. In a compile-checked
    // `VALUES` insert, a nullable column's parameter infers as its non-null base
    // type (`total` -> `i32`); to store SQL NULL, omit the column or use the
    // dynamic `execute_params` verb.
    let created = conn
        .query_one::<InsertOrder>((1i64, 42i64, 500i32, "pending"))
        .await?;
    println!("inserted order #{} with status {:?}", created.id, created.status);

    // READ: a single-row lookup. `total` decodes as `Option<i32>` (nullable).
    let order = conn.query_one::<OrderById>((1i64,)).await?;
    println!(
        "read order #{}: user={} total={:?} status={:?}",
        order.id, order.user_id, order.total, order.status
    );

    // UPDATE: `execute` returns the affected-row count.
    let updated = conn.execute::<SetStatus>((1i64, "shipped")).await?;
    println!("updated {updated} row(s) -> status 'shipped'");
    let after = conn.query_one::<OrderById>((1i64,)).await?;
    println!("status is now {:?}", after.status);

    // DELETE: `execute` again — count of rows removed.
    let deleted = conn.execute::<DeleteOrder>((1i64,)).await?;
    println!("deleted {deleted} row(s)");

    // The row is gone: `query_opt` is at-most-one, so an absent row is `Ok(None)`
    // (not an error) — the right shape for a "maybe present" lookup.
    match conn.query_opt::<OrderById>((1i64,)).await? {
        Some(_) => println!("order #1 still present (unexpected)"),
        None => println!("order #1 is gone (query_opt returned None)"),
    }

    conn.close().await?;
    Ok(())
}
