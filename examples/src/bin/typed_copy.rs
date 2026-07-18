//! # typed_copy — safe, fast bulk loading with `copy!` + `copy_in_typed`
//!
//! The raw `copy_in` takes `&[u8]` — YOU hand-format COPY text with correct
//! escaping, and a mis-escaped tab or newline silently corrupts a row (the
//! classic COPY footgun). `copy!(Name, "table", (cols))` validates the target
//! table + columns + types against the migration catalog and emits a carrier
//! whose `copy_in_typed::<Name>(rows)` streams each row as PGCOPY *binary* — no
//! text to mis-escape (a tab / newline / quote rides the binary field verbatim),
//! the identifiers are a compile-time constant, and it is FASTER (no text
//! parse/format). Wrong column type or arity is a compile error.
//!
//! Features/verbs: `copy!`, `conn.copy_in_typed::<Q>(rows)`.
//!
//! Backend: PostgreSQL — needs a live server. Uses a session TEMP shadow of
//! `metrics`.
//! ```bash
//! export BSQL_EXAMPLES_DSN='postgres://USER@127.0.0.1:5432/postgres'
//! cargo run -p bsql-examples --bin typed_copy
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "example/teaching code: unwrap/expect/panic surface failure loudly and keep the code readable"
)]
#![forbid(unsafe_code)]

use bsql::pg::{ConnectConfig, Connection};

// The typed COPY carrier: the columns (and their order) are validated against the
// `metrics` table. The row tuple is `(i64, &str, Option<&str>, Option<i32>)` — a
// NOT NULL column is `T`, a nullable one is `Option<T>`.
bsql::copy!(MetricsCopy, "metrics", (id, label, note, amount));
bsql::query!(AllMetrics, "SELECT id, label, note, amount FROM metrics ORDER BY id");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::connect(&ConnectConfig::from_dsn(&bsql_examples::dsn())?).await?;

    // TEMP shadow so the demo is idempotent and never touches your real table.
    conn.execute_raw(
        "CREATE TEMP TABLE metrics (id BIGINT PRIMARY KEY, label TEXT NOT NULL, \
         note TEXT, amount INTEGER)",
    )
    .await?;

    // A label with every byte that would corrupt a TEXT COPY row — binary COPY
    // carries it VERBATIM, no escaping needed.
    let hostile = "tab\there\nnewline\"quote\\backslash";
    let rows: Vec<(i64, &str, Option<&str>, Option<i32>)> = vec![
        (1, hostile, Some(hostile), Some(100)),
        (2, "plain-label", None, None), // NULL note + NULL amount
        (3, "third", Some("a note"), Some(-5)),
    ];

    // ONE call streams every row as binary, in constant memory (batched flushes).
    let loaded = conn.copy_in_typed::<MetricsCopy, _>(rows).await?;
    println!("bulk-loaded {loaded} rows via typed binary COPY");

    // Read them back — the hostile bytes round-tripped exactly.
    for row in conn.query::<AllMetrics>(()).await?.iter() {
        let metric = row?;
        println!(
            "  #{} label={:?} note={:?} amount={:?}",
            metric.id, metric.label, metric.note, metric.amount
        );
    }

    conn.close().await?;
    Ok(())
}
