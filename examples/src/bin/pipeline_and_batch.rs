//! # pipeline_and_batch — atomic multi-command verbs
//!
//! Three ways to run many commands ATOMICALLY (all-or-nothing, one implicit
//! transaction) in ~one round trip:
//!   * `pipeline((Q0::bind(p), Q1::bind(p), …))` — a HETEROGENEOUS fixed tuple of
//!     different `query!` carriers; returns the typed tuple `(Rows<Q0>, …)`.
//!   * `execute_batch::<Q>(iter)` — ONE write carrier against N runtime parameter
//!     sets; returns `Vec<u64>` per-command affected counts.
//!   * `query_batch::<Q>(iter)` — the typed-RETURNING peer; KEEPS each command's
//!     rows, returning `Vec<Rows<Q>>` (e.g. the N generated keys, typed).
//!
//! All are all-or-nothing: the whole batch commits and returns every result, or
//! it errors (`DriverError::BatchFailed { index }`) and returns ZERO.
//!
//! Features/verbs: `bsql::BindExt::bind`, `conn.pipeline`, `conn.execute_batch`,
//! `conn.query_batch`.
//!
//! Backend: PostgreSQL — needs a live server. Uses a session TEMP shadow.
//! ```bash
//! export BSQL_EXAMPLES_DSN='postgres://USER@127.0.0.1:5432/postgres'
//! cargo run -p bsql-examples --bin pipeline_and_batch
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "example/teaching code: unwrap/expect/panic surface failure loudly and keep the code readable"
)]
#![forbid(unsafe_code)]

// `BindExt` provides `Q::bind(params)`, wrapping a carrier with its params for a
// `pipeline` tuple.
use bsql::BindExt;
use bsql::pg::{ConnectConfig, Connection};

bsql::query!(Lit1, "SELECT 1::int4 AS n");
bsql::query!(LitHi, "SELECT 'hi'::text AS s");
// A write carrier (INSERT ... RETURNING id): usable by all three verbs.
bsql::query!(
    InsertOrder,
    "INSERT INTO orders (id, user_id, total, status) VALUES ($1, $2, $3, $4) RETURNING id"
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::connect(&ConnectConfig::from_dsn(&bsql_examples::dsn())?).await?;
    conn.execute_raw(
        "CREATE TEMP TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, \
         total INTEGER, status TEXT NOT NULL DEFAULT 'pending')",
    )
    .await?;

    // ── pipeline: a heterogeneous batch in ~ONE round trip ───────────────────
    // Two reads + one write, decoded against EACH carrier's own compile-time
    // shape. The insert commits atomically with the whole batch.
    // (`total` is a NULLABLE column, so a compile-checked `VALUES` insert types
    // its parameter as `Option<i32>`: `Some(x)` inserts x, `None` inserts SQL
    // NULL. The NOT NULL columns `id` / `user_id` / `status` keep their base
    // types `i64` / `i64` / `&str`.)
    let (one, hi, inserted) = conn
        .pipeline((
            Lit1::bind(()),
            LitHi::bind(()),
            InsertOrder::bind((1i64, 42i64, Some(500i32), "pending")),
        ))
        .await?;
    println!(
        "pipeline -> n={}, s={:?}, inserted id={}",
        one.iter().next().expect("row")?.n,
        hi.iter().next().expect("row")?.s,
        inserted.iter().next().expect("row")?.id,
    );

    // ── execute_batch: ONE carrier, N parameter sets, per-command counts ─────
    // `total` is nullable, so its param is `Option<i32>`: the second row binds
    // `None` to store SQL NULL. The turbofish names only the carrier
    // (`::<InsertOrder>`); the iterator type is inferred from the argument.
    let counts = conn
        .execute_batch::<InsertOrder>([
            (2i64, 42i64, Some(20i32), "pending"),
            (3i64, 42i64, None, "pending"),
        ])
        .await?;
    println!("execute_batch -> affected counts {counts:?}"); // [1, 1]

    // ── query_batch: like execute_batch, but KEEP the RETURNING rows ─────────
    let grouped = conn
        .query_batch::<InsertOrder>([
            (4i64, 42i64, Some(40i32), "pending"),
            (5i64, 42i64, Some(50i32), "pending"),
        ])
        .await?;
    let ids: Vec<i64> = grouped
        .iter()
        .filter_map(|rows| rows.iter().next())
        .filter_map(Result::ok)
        .map(|row| row.id)
        .collect();
    println!("query_batch -> generated ids {ids:?}"); // [4, 5]

    conn.close().await?;
    Ok(())
}
