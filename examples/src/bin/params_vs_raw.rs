//! # params_vs_raw — the three input modes, and when to use each
//!
//! bsql gives you THREE ways to run SQL. Pick by how much is known at build time:
//!
//!   1. **Typed `query!` (COMPILE-CHECKED)** — SQL is a string literal validated
//!      against your schema at build time; params bind as a typed tuple. Use this
//!      by default: an unknown column / wrong type / forgotten nullability is a
//!      COMPILE error. Injection-proof (params are bound, never concatenated).
//!
//!   2. **`query_params` (DYNAMIC SQL + BOUND PARAMS)** — the SQL text is a
//!      runtime `&str` (e.g. assembled from user choices), but VALUES still bind
//!      as parameters — so it is STILL injection-safe. Use it when the query
//!      shape is not known until run time.
//!
//!   3. **`query_raw` / `execute_raw` (RAW SQL TEXT, NO PARAMS)** — the whole
//!      statement is a string. The escape hatch, mainly for DDL (`CREATE TABLE`)
//!      or admin statements. NEVER build one by concatenating untrusted input —
//!      that is the SQL-injection footgun. The `_raw` suffix marks it as such.
//!
//! Features/verbs: typed `query_one`, dynamic `query_params` / `query_params_one`
//! / `execute_params`, raw `execute_batch_raw` / `query_raw`.
//!
//! Backend: SQLite — needs NO database (the distinction is identical on PostgreSQL).
//! ```bash
//! cargo run -p bsql-examples --bin params_vs_raw
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "example/teaching code: unwrap/expect/panic surface failure loudly and keep the code readable"
)]
#![forbid(unsafe_code)]

use bsql::sqlite::{Connection, ValueRef};

// MODE 1: the compile-checked flagship. `$1` binds a typed parameter.
bsql::query!(UserByName, "SELECT id, email, name FROM users WHERE name = $1");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;

    // MODE 3 (raw, no params): DDL is static SQL text — the right place for `_raw`.
    conn.execute_batch_raw(bsql_examples::SQLITE_SCHEMA)?;

    // MODE 2 (dynamic SQL + bound params): the VALUES are parameters, so this is
    // injection-safe even though the SQL is a runtime string.
    conn.execute_params(
        "INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
        &[ValueRef::Integer(1), ValueRef::Text(b"alice@example.com"), ValueRef::Text(b"Alice")],
    )?;

    // MODE 1 (typed, compile-checked): the SAME lookup, but the SQL was validated
    // at build time and the record is typed. Prefer this whenever the query is
    // known at compile time.
    let alice = conn.query_one::<UserByName>(("Alice",))?;
    println!("[typed]   found #{}: {}", alice.id, alice.email);

    // ── The injection distinction, made vivid ────────────────────────────────
    // A classic attack string. Bound as a PARAMETER (mode 2), it is just DATA —
    // stored verbatim as a name, harmless. Concatenated into raw SQL (mode 3), it
    // would DROP the table. bsql's parameterized verbs never concatenate.
    let hostile = "Robert'); DROP TABLE users; --";
    conn.execute_params(
        "INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
        &[ValueRef::Integer(2), ValueRef::Text(b"bobby@example.com"), ValueRef::Text(hostile.as_bytes())],
    )?;
    // The table is intact, and the "name" is stored as the literal attack string.
    let bobby = conn.query_params_one(
        "SELECT id, name FROM users WHERE id = $1",
        &[ValueRef::Integer(2)],
    )?;
    println!(
        "[params]  the attack string was stored as harmless data: {:?}",
        bobby.get::<String>(1)?
    );

    // MODE 3 read: a static admin query via `query_raw` (a dynamic Row result).
    let total = conn.query_raw("SELECT count(*) FROM users")?;
    let count = total.get(0).expect("one row").get::<i64>(0)?;
    println!("[raw]     users table survived; it has {count} rows");

    Ok(())
}
