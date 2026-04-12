//! Keyset pagination ("seek pagination") with bsql on SQLite.
//!
//! This is the SQLite counterpart to `pg_keyset_pagination.rs` — the same
//! pattern, the same `query!()` syntax, the same `$seek: Option<i64>`
//! declaration. SQLite and PostgreSQL both understand the
//! `$seek IS NULL OR id > $seek` trick natively; bsql hides the
//! protocol-level differences (PostgreSQL needs the parameter's OID even
//! when the value is NULL; SQLite binds NULL without any type hint) so
//! your code stays identical.
//!
//! SQLite integer columns are `i64` by default — `INTEGER` in SQLite
//! is a 64-bit integer, unlike PostgreSQL where `int4` and `int8` are
//! distinct. That's the only substantive change from the PG version.
//!
//! ## Setup
//!
//! The example uses an in-memory database and creates the table on
//! startup — no external setup needed. Just run it.
//!
//! ## Run
//!
//! ```sh
//! cargo run --bin sqlite_keyset_pagination
//! ```

use bsql::{BsqlError, SqlitePool};

fn main() -> Result<(), BsqlError> {
    let pool = SqlitePool::open(":memory:")?;

    // Schema + seed data. DDL and multi-row inserts go through
    // `simple_exec` — runtime SQL, no compile-time validation,
    // which is the right fit for one-shot setup statements.
    pool.simple_exec(
        "CREATE TABLE users (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            login TEXT NOT NULL
        ) STRICT",
    )?;

    pool.simple_exec(
        "INSERT INTO users (login) VALUES
            ('alice'), ('bob'), ('carol'), ('dave'), ('eve'),
            ('frank'), ('grace'), ('heidi'), ('ivan'), ('judy')",
    )?;

    let page_size = 3i64;

    // First page: seek is None — the WHERE clause degenerates to "true".
    let mut seek: Option<i64> = None;
    let mut page_number = 1;

    loop {
        let rows = bsql::query!(
            "SELECT id, login FROM users
             WHERE $seek: Option<i64> IS NULL OR id > $seek: Option<i64>
             ORDER BY id
             LIMIT $page_size: i64"
        )
        .fetch_all(&pool)?;

        if rows.is_empty() {
            break;
        }

        println!("--- page {page_number} ---");
        for row in &rows {
            println!("  id={} login={}", row.id, row.login);
        }

        seek = Some(rows.last().unwrap().id);
        page_number += 1;

        if (rows.len() as i64) < page_size {
            break;
        }
    }

    Ok(())
}
