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

use bsql_postgres_async::{ConnectConfig, Connection, MigrationError, MigrationSource};

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
    conn.execute_raw("INSERT INTO users (id, name, email) VALUES (1, 'a', 'a@x')")
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

/// WITNESS (C1f — migration progress): running a set emits a
/// `MigrationApplying` then `MigrationApplied` for each migration, in order —
/// so a long deploy is visible, not silent between start and the final report.
#[tokio::test]
#[ignore = "requires local PG"]
async fn async_runner_emits_progress_events() {
    use std::sync::{Arc, Mutex};

    use bsql_postgres_async::{DiagEvent, Diagnostics};

    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let events_in = Arc::clone(&events);
    let diag = Diagnostics::new().on_event(move |ev: &DiagEvent<'_>| match ev {
        DiagEvent::MigrationApplying { name } => {
            events_in.lock().expect("lock").push(format!("applying:{name}"));
        }
        DiagEvent::MigrationApplied { name } => {
            events_in.lock().expect("lock").push(format!("applied:{name}"));
        }
        _ => {}
    });

    let mut conn = Connection::connect_with(&config(), &diag).await.expect("connect_with");
    let schema = format!("bsql_migdiag_async_{}", std::process::id());
    conn.simple_query(&format!("CREATE SCHEMA {schema}")).await.expect("create schema");
    conn.simple_query(&format!("SET search_path TO {schema}")).await.expect("search_path");

    let set = [
        ("0001_a.sql", "CREATE TABLE t (id int)"),
        ("0002_b.sql", "ALTER TABLE t ADD COLUMN v int"),
    ];
    conn.run_migrations(MigrationSource::embedded(&set)).await.expect("run");

    let got = events.lock().expect("lock").clone();
    assert_eq!(
        got,
        vec![
            "applying:0001_a.sql",
            "applied:0001_a.sql",
            "applying:0002_b.sql",
            "applied:0002_b.sql",
        ],
        "each migration emits applying then applied, in order",
    );

    conn.simple_query(&format!("DROP SCHEMA {schema} CASCADE")).await.expect("drop schema");
}

/// WITNESS (runtime backstop, async twin of the sync test): a migration with a
/// top-level COMMIT commits the runner's own BEGIN mid-way — a broken
/// per-migration boundary. PG's own trailing COMMIT is a silent no-op-warning, so
/// only the native RFQ transaction-status backstop catches it: a classified
/// `TransactionBoundaryBroken` naming the migration; the runner STOPS. Plus a
/// no-false-positive half: a normal set still applies clean.
#[tokio::test]
#[ignore = "requires local PG"]
async fn a_top_level_commit_in_a_migration_is_a_boundary_broken_error() {
    let mut conn = Connection::connect(&config()).await.expect("connect");
    let schema = format!("bsql_migcommit_async_{}", std::process::id());
    conn.simple_query(&format!("CREATE SCHEMA {schema}")).await.expect("create schema");
    conn.simple_query(&format!("SET search_path TO {schema}")).await.expect("search_path");

    let set = [
        ("0001_t.sql", "CREATE TABLE t (a int)"),
        ("0002_commit.sql", "CREATE TABLE mid (a int);\nCOMMIT;"),
        ("0003_after.sql", "CREATE TABLE after_marker (a int)"),
    ];
    let err = conn
        .run_migrations(MigrationSource::embedded(&set))
        .await
        .expect_err("a top-level COMMIT must be a boundary-broken error");
    assert!(
        matches!(&err, MigrationError::TransactionBoundaryBroken { migration } if migration == "0002_commit.sql"),
        "expected TransactionBoundaryBroken naming 0002, got {err:?}"
    );

    // 0003 never ran; the connection is reusable (drained to a clean idle).
    let result = conn
        .query_raw("SELECT to_regclass('after_marker')::text")
        .await
        .expect("connection reusable after the boundary break");
    let row = result.get(0).expect("one row");
    assert!(
        row.get_str(0).expect("text").is_none(),
        "0003 must not run after the boundary break"
    );

    // NO FALSE POSITIVE: a normal set still applies clean under the backstop.
    conn.simple_query(&format!("DROP SCHEMA {schema} CASCADE")).await.expect("drop schema");
    let schema2 = format!("bsql_migclean_async_{}", std::process::id());
    conn.simple_query(&format!("CREATE SCHEMA {schema2}")).await.expect("create schema");
    conn.simple_query(&format!("SET search_path TO {schema2}")).await.expect("search_path");
    let normal = [
        ("0001_a.sql", "CREATE TABLE a (x int)"),
        ("0002_b.sql", "ALTER TABLE a ADD COLUMN y int"),
    ];
    let report = conn
        .run_migrations(MigrationSource::embedded(&normal))
        .await
        .expect("a normal migration set applies clean under the backstop");
    assert_eq!(report.applied.len(), 2, "no false positive: a normal set applies");
    conn.simple_query(&format!("DROP SCHEMA {schema2} CASCADE")).await.expect("drop schema");
}
