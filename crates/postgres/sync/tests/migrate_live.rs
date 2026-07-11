#![forbid(unsafe_code)]
//! Live witnesses for the PostgreSQL migration RUNNER (blocking driver).
//!
//! Each test runs inside its OWN freshly-created schema (via `search_path`), so
//! the unqualified `_bsql_migrations` ledger and the migration tables land in
//! that schema and are dropped with it — parallel tests never interfere.
//!
//! Proves: fresh-DB apply in order + ledger correct, idempotent re-run
//! (exactly-once), checksum-drift classified, a failing migration rolls back +
//! stops + names itself, status / dry-run, the `-- bsql:no-transaction` path
//! (`CREATE INDEX CONCURRENTLY`) AND its fail-loud counterpart (the same
//! statement WITHOUT the marker is rejected, never a silent atomicity break),
//! and the advisory-lock concurrency (two runners, one applies).
//!
//! Run with: `cargo test -p bsql-postgres-sync --test migrate_live -- --ignored`
// The connect fixture helper is not a `#[test]` fn, so the in-tests carve-out
// does not reach its `expect`; it is the loud connect-failure signal a test
// wants, not a production data fallback.
#![allow(
    clippy::expect_used,
    clippy::unwrap_in_result,
    reason = "live test harness — the schema fixture expects a live PG and panics loudly on failure; not a production fallback path"
)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};

use bsql_postgres_sync::{
    ConnectConfig, Connection, DriftKind, MigrationError, MigrationSource, MigrationSourceError,
};

fn config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(bsql_postgres_sync::SslMode::Disable)
}

static SEQ: AtomicU32 = AtomicU32::new(0);

/// A unique, injection-safe schema name for one test.
fn unique_schema() -> String {
    format!(
        "bsql_migtest_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// Connect and enter a fresh isolated schema; returns `(conn, schema_name)`. The
/// caller drops the schema with [`drop_schema`] at the end.
fn conn_in_fresh_schema() -> (Connection, String) {
    let mut conn = Connection::connect(&config()).expect("connect to local PG");
    let schema = unique_schema();
    conn.simple_query(&format!("CREATE SCHEMA {schema}")).expect("create schema");
    conn.simple_query(&format!("SET search_path TO {schema}")).expect("set search_path");
    (conn, schema)
}

fn drop_schema(conn: &mut Connection, schema: &str) {
    conn.simple_query(&format!("DROP SCHEMA {schema} CASCADE"))
        .expect("drop schema");
}

const M1: (&str, &str) = ("0001_users.sql", "CREATE TABLE users (id int PRIMARY KEY, name text)");
const M2: (&str, &str) = ("0002_email.sql", "ALTER TABLE users ADD COLUMN email text");
const M3: (&str, &str) = ("0003_posts.sql", "CREATE TABLE posts (id int PRIMARY KEY, user_id int)");

fn three() -> [(&'static str, &'static str); 3] {
    [M1, M2, M3]
}

#[test]
#[ignore = "requires local PG"]
fn fresh_schema_applies_all_in_order_and_records_them() {
    let (mut conn, schema) = conn_in_fresh_schema();
    let set = three();

    let report = conn.run_migrations(MigrationSource::embedded(&set)).expect("run");
    assert_eq!(report.applied, vec!["0001_users.sql", "0002_email.sql", "0003_posts.sql"]);
    assert_eq!(report.already_applied, 0);

    let status = conn.migration_status(MigrationSource::embedded(&set)).expect("status");
    let names: Vec<&str> = status.applied.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(names, vec!["0001_users.sql", "0002_email.sql", "0003_posts.sql"]);
    assert!(status.pending.is_empty());
    // The migrations really ran (the columns/tables exist).
    conn.execute_sql("INSERT INTO users (id, name, email) VALUES (1, 'a', 'a@x')").expect("users ok");
    conn.execute_sql("INSERT INTO posts (id, user_id) VALUES (1, 1)").expect("posts ok");

    drop_schema(&mut conn, &schema);
}

#[test]
#[ignore = "requires local PG"]
fn rerun_is_idempotent() {
    let (mut conn, schema) = conn_in_fresh_schema();
    let set = three();
    conn.run_migrations(MigrationSource::embedded(&set)).expect("first");
    let again = conn.run_migrations(MigrationSource::embedded(&set)).expect("second");
    assert!(again.applied.is_empty());
    assert_eq!(again.already_applied, 3);
    drop_schema(&mut conn, &schema);
}

#[test]
#[ignore = "requires local PG"]
fn editing_an_applied_migration_is_checksum_drift() {
    let (mut conn, schema) = conn_in_fresh_schema();
    conn.run_migrations(MigrationSource::embedded(&three())).expect("run");

    let edited = [M1, ("0002_email.sql", "ALTER TABLE users ADD COLUMN email varchar(50)"), M3];
    let err = conn
        .run_migrations(MigrationSource::embedded(&edited))
        .expect_err("edited applied migration is drift");
    assert!(matches!(
        err,
        MigrationError::Drift { migration, kind: DriftKind::ChecksumMismatch { .. } } if migration == "0002_email.sql"
    ));
    drop_schema(&mut conn, &schema);
}

#[test]
#[ignore = "requires local PG"]
fn a_failing_migration_rolls_back_and_stops_naming_it() {
    let (mut conn, schema) = conn_in_fresh_schema();
    let broken = [
        M1,
        ("0002_broken.sql", "ALTER TABLE users ADD COLUMN name text"), // duplicate column -> server error
        ("0003_after.sql", "CREATE TABLE after_marker (id int)"),
    ];
    let err = conn
        .run_migrations(MigrationSource::embedded(&broken))
        .expect_err("broken migration must fail");
    assert!(matches!(
        err,
        MigrationError::MigrationFailed { migration, .. } if migration == "0002_broken.sql"
    ));

    // 0001 committed, 0003 never ran, 0002 rolled back — the ledger has ONLY 0001.
    let status = conn.migration_status(MigrationSource::embedded(&broken)).expect("status");
    let applied: Vec<&str> = status.applied.iter().map(|a| a.name.as_str()).collect();
    assert_eq!(applied, vec!["0001_users.sql"]);
    let after = conn
        .query_sql("SELECT to_regclass('after_marker') IS NOT NULL AS exists")
        .expect("q");
    assert_eq!(after.get(0).expect("row").get_bool(0).expect("bool"), Some(false), "0003 must not have run");
    // The connection is still usable (0002's transaction rolled back cleanly).
    conn.ping().expect("connection reusable after a failed migration");

    drop_schema(&mut conn, &schema);
}

#[test]
#[ignore = "requires local PG"]
fn status_and_dry_run_report_pending_without_applying() {
    let (mut conn, schema) = conn_in_fresh_schema();
    let set = three();
    let status = conn.migration_status(MigrationSource::embedded(&set)).expect("status");
    assert!(status.applied.is_empty());
    assert_eq!(status.pending, vec!["0001_users.sql", "0002_email.sql", "0003_posts.sql"]);
    let would = conn.dry_run_migrations(MigrationSource::embedded(&set)).expect("dry run");
    assert_eq!(would, vec!["0001_users.sql", "0002_email.sql", "0003_posts.sql"]);
    // Dry run applied nothing — nothing is recorded.
    let still = conn.migration_status(MigrationSource::embedded(&set)).expect("status again");
    assert!(still.applied.is_empty());
    drop_schema(&mut conn, &schema);
}

#[test]
#[ignore = "requires local PG"]
fn non_transactional_marker_runs_create_index_concurrently() {
    let (mut conn, schema) = conn_in_fresh_schema();
    let set = [
        ("0001_t.sql", "CREATE TABLE t (a int)"),
        // CREATE INDEX CONCURRENTLY cannot run inside a transaction — the marker
        // makes the runner apply it OUTSIDE one.
        ("0002_idx.sql", "-- bsql:no-transaction\nCREATE INDEX CONCURRENTLY idx_t_a ON t (a)"),
    ];
    let report = conn.run_migrations(MigrationSource::embedded(&set)).expect("run");
    assert_eq!(report.applied, vec!["0001_t.sql", "0002_idx.sql"]);
    // Idempotent re-run.
    assert!(conn.run_migrations(MigrationSource::embedded(&set)).expect("rerun").applied.is_empty());
    drop_schema(&mut conn, &schema);
}

#[test]
#[ignore = "requires local PG"]
fn create_index_concurrently_without_the_marker_fails_loud() {
    // WITHOUT the marker the runner wraps it in BEGIN; PG rejects it loudly —
    // the runner does NOT silently break atomicity. Fail-loud, named.
    let (mut conn, schema) = conn_in_fresh_schema();
    let set = [
        ("0001_t.sql", "CREATE TABLE t (a int)"),
        ("0002_idx.sql", "CREATE INDEX CONCURRENTLY idx_t_a ON t (a)"),
    ];
    let err = conn
        .run_migrations(MigrationSource::embedded(&set))
        .expect_err("CONCURRENTLY in a txn must fail loudly");
    assert!(matches!(
        err,
        MigrationError::MigrationFailed { migration, .. } if migration == "0002_idx.sql"
    ));
    drop_schema(&mut conn, &schema);
}

#[test]
#[ignore = "requires local PG"]
fn duplicate_named_migration_is_loud_not_a_silent_skip() {
    // PG now fails loud at the pre-flight duplicate check (before the ledger PK
    // would), giving the SAME classified `Source(DuplicateName)` as SQLite — one
    // cross-backend error for a duplicate-named source.
    let (mut conn, schema) = conn_in_fresh_schema();
    let dup = [
        ("0001.sql", "CREATE TABLE a (x int)"),
        ("0001.sql", "CREATE TABLE b (x int)"),
    ];
    let err = conn
        .run_migrations(MigrationSource::embedded(&dup))
        .expect_err("a duplicate name must be loud");
    assert!(matches!(
        err,
        MigrationError::Source(MigrationSourceError::DuplicateName { name }) if name == "0001.sql"
    ));
    // Nothing applied.
    let a = conn
        .query_sql("SELECT to_regclass('a') IS NULL AS absent")
        .expect("q");
    assert_eq!(a.get(0).expect("row").get_bool(0).expect("bool"), Some(true));
    drop_schema(&mut conn, &schema);
}

#[test]
#[ignore = "requires local PG"]
fn two_concurrent_runners_apply_exactly_once_via_advisory_lock() {
    // Both runners target the SAME schema; the session advisory lock serializes
    // them, so between them each migration runs EXACTLY once (no double-apply).
    let (mut setup, schema) = conn_in_fresh_schema();

    let set: [(&'static str, &'static str); 4] = [
        ("0001.sql", "CREATE TABLE a (id int PRIMARY KEY)"),
        ("0002.sql", "CREATE TABLE b (id int PRIMARY KEY)"),
        ("0003.sql", "CREATE TABLE c (id int PRIMARY KEY)"),
        ("0004.sql", "CREATE TABLE d (id int PRIMARY KEY)"),
    ];

    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();
    for _ in 0..2u8 {
        let sch = schema.clone();
        let gate = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let mut conn = Connection::connect(&config()).expect("connect");
            conn.simple_query(&format!("SET search_path TO {sch}")).expect("search_path");
            gate.wait();
            conn.run_migrations(MigrationSource::embedded(&set))
        }));
    }
    let mut total = 0usize;
    for h in handles {
        total += h.join().expect("join").expect("runner ok").applied.len();
    }
    assert_eq!(total, 4, "advisory lock -> each migration applied exactly once");

    let status = setup.migration_status(MigrationSource::embedded(&set)).expect("status");
    assert_eq!(status.applied.len(), 4);
    assert!(status.pending.is_empty());

    drop_schema(&mut setup, &schema);
}

/// WITNESS (C1f — migration progress, blocking twin): running a set emits a
/// `MigrationApplying` then `MigrationApplied` for each migration, in order.
#[test]
#[ignore = "requires local PG"]
fn sync_runner_emits_progress_events() {
    use std::sync::{Arc, Mutex};

    use bsql_postgres_sync::{DiagEvent, Diagnostics};

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

    let mut conn = Connection::connect_with(&config(), &diag).expect("connect_with");
    let schema = unique_schema();
    conn.simple_query(&format!("CREATE SCHEMA {schema}")).expect("create schema");
    conn.simple_query(&format!("SET search_path TO {schema}")).expect("set search_path");

    conn.run_migrations(MigrationSource::embedded(&[M1, M2])).expect("run");

    let got = events.lock().expect("lock").clone();
    assert_eq!(
        got,
        vec![
            "applying:0001_users.sql",
            "applied:0001_users.sql",
            "applying:0002_email.sql",
            "applied:0002_email.sql",
        ],
        "each migration emits applying then applied, in order",
    );

    drop_schema(&mut conn, &schema);
}
