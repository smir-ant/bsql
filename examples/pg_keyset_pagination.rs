//! Keyset pagination ("seek pagination") with bsql.
//!
//! Keyset pagination is the correct way to paginate large result sets at
//! scale: instead of `OFFSET N LIMIT M` (which re-scans every previous
//! page on every request), you pass the last row's sort key from the
//! previous page and ask for rows strictly after it.
//!
//! The trick: the **first** page has no previous key. So you need one
//! query that handles both cases — "no key, give me the first page" and
//! "here's the last key, give me the next page" — without string
//! concatenation and without two separate `query!()` sites.
//!
//! The SQL pattern that does this in one query:
//!
//! ```sql
//! SELECT id, login FROM users
//! WHERE $seek IS NULL OR id > $seek
//! ORDER BY id
//! LIMIT $limit
//! ```
//!
//! When `$seek` is NULL, the left side of the `OR` is true and every row
//! passes. When `$seek` is a concrete value, the left side is false and
//! the right side (`id > $seek`) filters correctly.
//!
//! bsql handles this with `Option<T>`: `None` for the first page, `Some(id)`
//! for subsequent pages. The parameter type is declared as
//! `$seek: Option<i32>` and bsql sends the correct PostgreSQL OID (int4)
//! to the server even when the value is `None` — so PG can resolve the
//! `$seek IS NULL OR id > $seek` expression without guessing at the type.
//!
//! This pattern works identically in PostgreSQL and SQLite — see
//! `examples/sqlite_keyset_pagination.rs` for the SQLite version.
//!
//! ## Setup
//!
//! ```sql
//! CREATE TABLE users (
//!     id    SERIAL PRIMARY KEY,
//!     login TEXT NOT NULL
//! );
//!
//! INSERT INTO users (login) VALUES
//!     ('alice'), ('bob'), ('carol'), ('dave'), ('eve'),
//!     ('frank'), ('grace'), ('heidi'), ('ivan'), ('judy');
//! ```
//!
//! ## Run
//!
//! ```sh
//! export BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb
//! cargo run --bin pg_keyset_pagination
//! ```

use bsql::{BsqlError, Pool};

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect(
        &std::env::var("BSQL_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://bsql:bsql@localhost/bsql_test".into()),
    )
    .await?;

    let page_size = 3i64;

    // First page: seek is None — the WHERE clause degenerates to "true".
    let mut seek: Option<i32> = None;
    let mut page_number = 1;

    loop {
        let rows = bsql::query!(
            "SELECT id, login FROM users
             WHERE $seek: Option<i32> IS NULL OR id > $seek: Option<i32>
             ORDER BY id
             LIMIT $page_size: i64"
        )
        .fetch_all(&pool)
        .await?;

        if rows.is_empty() {
            break;
        }

        println!("--- page {page_number} ---");
        for row in &rows {
            println!("  id={} login={}", row.id, row.login);
        }

        // Advance the cursor: the next page starts strictly after the
        // highest id we just fetched. When the page comes back short
        // of `page_size`, we know there are no more rows and exit.
        seek = Some(rows.last().unwrap().id);
        page_number += 1;

        if (rows.len() as i64) < page_size {
            break;
        }
    }

    Ok(())
}
