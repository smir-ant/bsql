//! # basic_sqlite — the zero-setup starting point
//!
//! Demonstrates the embedded SQLite backend end-to-end IN-PROCESS (no server,
//! runs ANYWHERE): open a database, create a table, insert rows, and read them
//! back through the compile-checked `query!` flagship.
//!
//! Features/verbs: `bsql::sqlite::Connection`, `query!`, the typed `query` /
//! `query_one` verbs, the dynamic `execute_batch_raw` / `execute_params` verbs.
//!
//! Backend: SQLite (in-memory) — needs NO database.
//!
//! Run:
//! ```bash
//! cargo run -p bsql-examples --bin basic_sqlite
//! ```
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::manual_unwrap_or,
    clippy::manual_unwrap_or_default,
    reason = "example/teaching code: unwrap/expect/panic read clearly, and the manual match on an Option is the form the workspace disallowed-methods ledger requires (the unwrap_or family is banned)"
)]
#![forbid(unsafe_code)]

use bsql::sqlite::{Connection, ValueRef};

// A COMPILE-CHECKED query. `query!(Name, "SQL")` types the SQL at BUILD time
// against the schema `build.rs` replayed from `migrations/`. `SELECT nope FROM
// users` — or reading `name` as a non-`Option` when the column is nullable —
// would be a `compile_error!`, never a runtime surprise. The macro emits an
// owned record `AllUsers` that is ITSELF the runnable carrier.
bsql::query!(AllUsers, "SELECT id, email, name FROM users ORDER BY id");

// A parameterized compile-checked query: `$1` is the `id` (an `i64`). The bound
// parameter is a typed tuple, checked against the query at compile time.
bsql::query!(UserById, "SELECT id, email, name FROM users WHERE id = $1");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open an in-memory database. `open("path.db")` would open a file instead.
    let conn = Connection::open_in_memory()?;

    // Apply the schema. `execute_batch_raw` runs a multi-statement SQL script —
    // a DYNAMIC (raw-SQL, unchecked) verb; the `_raw` suffix marks the escape
    // hatch. Here it creates the `users` / `authors` / `books` tables.
    conn.execute_batch_raw(bsql_examples::SQLITE_SCHEMA)?;

    // Insert rows with the DYNAMIC parameterized verb. SQLite values bind as
    // `ValueRef`s in their TRUE storage class (`Integer` / `Text` / `Null`), so
    // there is no injection surface and NULL is a first-class value.
    let seed: &[(i64, &str, Option<&str>)] = &[
        (1, "alice@example.com", Some("Alice")),
        (2, "bob@example.com", None), // Bob has no display name -> SQL NULL
    ];
    for &(id, email, name) in seed {
        conn.execute_params(
            "INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
            &[
                ValueRef::Integer(id),
                ValueRef::Text(email.as_bytes()),
                match name {
                    Some(text) => ValueRef::Text(text.as_bytes()),
                    None => ValueRef::Null,
                },
            ],
        )?;
    }

    // Read them back through the TYPED flagship. `conn.query::<AllUsers>(())`
    // returns `TypedRows` — each row decoded into the macro-generated record with
    // fields `id: i64`, `email: &str`, `name: Option<&str>` (nullability honored).
    println!("all users:");
    let users = conn.query::<AllUsers>(())?;
    for row in users.iter() {
        let user = row?; // a per-row classified decode error would surface via `?`
        // `name` is `Option<&str>` — a real NULL is `None`. (We avoid the banned
        // silent-fallback `unwrap_or`; a `match` makes the default explicit.)
        let name = match user.name {
            Some(display) => display,
            None => "(no name)",
        };
        println!("  #{}: {} — {name}", user.id, user.email);
    }

    // A single-row lookup by key. `query_one` returns the OWNED record (or a
    // classified error if the result is not exactly one row).
    let alice = conn.query_one::<UserById>((1i64,))?;
    println!("looked up #{}: {}", alice.id, alice.email);

    Ok(())
}
