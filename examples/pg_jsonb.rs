//! Working with JSONB columns and array parameters in bsql.
//!
//! Demonstrates how bsql handles JSONB transparently:
//!   - Pass `&str` with valid JSON — no wrapper types needed
//!   - bsql auto-detects the JSONB column and handles conversion
//!   - PostgreSQL validates the JSON server-side
//!
//! ## How it works
//!
//! When you write `$data: &str` and the target column is JSONB,
//! bsql detects the type mismatch at compile time (text ≠ jsonb) and
//! automatically adds a `::jsonb` cast to the SQL. PostgreSQL then
//! parses the JSON string and stores it in native JSONB format.
//!
//! Other libraries (sqlx, diesel) require a wrapper type like `Json<T>`
//! or `serde_json::Value`. bsql uses PostgreSQL's native type casting
//! instead — simpler API, same safety, same performance.
//!
//! If the string is not valid JSON, PostgreSQL returns a clear error
//! (not a panic). This is the same behavior as writing raw SQL.
//!
//! ## Setup
//!
//! ```sql
//! CREATE TABLE events (
//!     id       SERIAL PRIMARY KEY,
//!     kind     TEXT NOT NULL,
//!     payload  JSONB NOT NULL,
//!     metadata JSONB
//! );
//! ```
//!
//! ## Run
//!
//! ```bash
//! BSQL_DATABASE_URL=postgres://user:pass@localhost/mydb cargo run --example pg_jsonb
//! ```

use bsql::{BsqlError, Pool};

#[tokio::main]
async fn main() -> Result<(), BsqlError> {
    let pool = Pool::connect("postgres://user:pass@localhost/mydb").await?;

    // ---------------------------------------------------------------
    // INSERT with JSONB — just pass a &str, no wrapper needed
    // ---------------------------------------------------------------
    let kind = "user.created";
    let payload = r#"{"user_id": 42, "name": "Alice"}"#;
    let metadata = r#"{"source": "api", "version": 2}"#;

    let event = bsql::query!(
        "INSERT INTO events (kind, payload, metadata)
         VALUES ($kind: &str, $payload: &str, $metadata: &str)
         RETURNING id"
    )
    .fetch_one(&pool).await?;
    println!("Created event {}", event.id);

    // ---------------------------------------------------------------
    // SELECT JSONB — returns String (the JSON text)
    // ---------------------------------------------------------------
    let id = event.id;
    let event = bsql::query!(
        "SELECT id, kind, payload, metadata FROM events WHERE id = $id: i32"
    )
    .fetch_one(&pool).await?;
    println!("Event {}: kind={}, payload={}", event.id, event.kind, event.payload);

    // ---------------------------------------------------------------
    // JSONB operators in WHERE — works naturally
    // ---------------------------------------------------------------
    let events = bsql::query!(
        "SELECT id, kind FROM events
         WHERE payload->>'user_id' = '42'
         ORDER BY id"
    )
    .fetch_all(&pool).await?;
    println!("Found {} events for user 42", events.len());

    // ---------------------------------------------------------------
    // UPDATE JSONB field with jsonb_set
    // ---------------------------------------------------------------
    let new_name = r#""Bob""#; // JSON string value (with quotes)
    let affected = bsql::query!(
        "UPDATE events
         SET payload = jsonb_set(payload, '{name}', $new_name: &str)
         WHERE id = $id: i32"
    )
    .execute(&pool).await?;
    println!("Updated {affected} event(s)");

    // ---------------------------------------------------------------
    // NULL-able JSONB — metadata is Option<String>
    // ---------------------------------------------------------------
    let affected = bsql::query!(
        "INSERT INTO events (kind, payload) VALUES ('test', '{}')"
    )
    .execute(&pool).await?;
    println!("Inserted {affected} event without metadata (NULL)");

    // ---------------------------------------------------------------
    // Array parameters with unnest — bsql sends the correct array OID
    // ---------------------------------------------------------------
    // unnest() expands an array into rows. bsql automatically sends
    // the correct PG array OID (e.g. text[] = 1009) so PG can resolve
    // the function overload. No explicit casts needed.
    let tags = vec!["rust".to_owned(), "sql".to_owned(), "postgres".to_owned()];
    let rows = bsql::query!(
        "SELECT t AS tag FROM unnest($tags: Vec<String>) AS t ORDER BY t"
    )
    .fetch_all(&pool).await?;
    for row in &rows {
        println!("Tag: {}", row.tag);
    }

    // Array parameter in WHERE IN — same mechanism
    let ids = vec![1i32, 2, 3];
    let users = bsql::query!(
        "SELECT id, login FROM users WHERE id = ANY($ids: Vec<i32>) ORDER BY id"
    )
    .fetch_all(&pool).await?;
    println!("Found {} users by ID array", users.len());

    Ok(())
}
