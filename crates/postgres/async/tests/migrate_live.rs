#![forbid(unsafe_code)]
//! Live witness for the migration RUNNER over the ASYNC driver.
//!
//! The runner LOGIC is shared with the sync driver via the transport-generic
//! `Core<S>` (and is exhaustively witnessed there — see
//! `bsql-postgres-sync`'s `migrate_live`); this proves the ASYNC driver's OWN
//! `run_migrations` wrapper — the non-blocking try-lock acquire poll with
//! `tokio::time::sleep` backoff, and the apply + release around it — end to end.
//!
//! Run with: `cargo test -p bsql-postgres-async --test migrate_live -- --ignored`
#![allow(
    clippy::expect_used,
    clippy::unwrap_in_result,
    reason = "live test harness — the schema fixture expects a live PG and panics loudly on failure; not a production fallback path"
)]

use bsql_postgres_async::{ConnectConfig, Connection, MigrationSource};

fn config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_async::SslMode::Disable)
}

#[tokio::test]
#[ignore = "requires local PG"]
async fn async_runner_applies_then_is_idempotent() {
    let mut conn = Connection::connect(&config()).await.expect("connect");
    let schema = format!("bsql_migtest_async_{}", std::process::id());
    conn.simple_query(&format!("CREATE SCHEMA {schema}")).await.expect("create schema");
    conn.simple_query(&format!("SET search_path TO {schema}")).await.expect("search_path");

    let set = [
        ("0001_users.sql", "CREATE TABLE users (id int PRIMARY KEY, name text)"),
        ("0002_email.sql", "ALTER TABLE users ADD COLUMN email text"),
    ];

    // First run applies both, in order, via the async try-lock acquire path.
    let report = conn.run_migrations(MigrationSource::embedded(&set)).await.expect("run");
    assert_eq!(report.applied, vec!["0001_users.sql", "0002_email.sql"]);
    assert_eq!(report.already_applied, 0);

    // The migrations really ran.
    conn.execute_sql("INSERT INTO users (id, name, email) VALUES (1, 'a', 'a@x')")
        .await
        .expect("table + column exist");

    // Re-run: no-op (exactly-once).
    let again = conn.run_migrations(MigrationSource::embedded(&set)).await.expect("rerun");
    assert!(again.applied.is_empty());
    assert_eq!(again.already_applied, 2);

    // Status + dry-run over the async driver.
    let status = conn.migration_status(MigrationSource::embedded(&set)).await.expect("status");
    assert_eq!(status.applied.len(), 2);
    assert!(status.pending.is_empty());

    conn.simple_query(&format!("DROP SCHEMA {schema} CASCADE")).await.expect("drop schema");
}
