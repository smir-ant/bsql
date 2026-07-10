//! End-to-end witness for the EMBEDDED migration runner in a REAL consumer:
//! `build.rs` calls `bsql_build::emit_migrations("runner_migrations")`, which
//! bakes the set (via `include_str!`) and runs the destructive-ack gate on it;
//! `bsql::embed_migrations!()` includes it; the driver's `run_migrations`
//! applies it — no filesystem at run time.
//!
//! The offline test proves the build.rs -> embed -> `&[(name, sql)]` chain (and
//! that the acked destructive migration baked without failing the build). The
//! `--ignored` test proves the embedded set APPLIES to a live database.
//!
//! Run the live half with:
//! `cargo test -p bsql-query-fixture --test embed_migrations_live -- --ignored`
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    reason = "live test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

/// The migration set baked by the fixture's `build.rs` (from
/// `runner_migrations/`). This line alone proves the whole build-time chain:
/// `emit_migrations` walked + ack-gated + baked the set, and
/// `embed_migrations!()` `include!`d it — a compile error here would mean the
/// build step was missing.
const MIGRATIONS: &[(&str, &str)] = bsql::embed_migrations!();

/// OFFLINE WITNESS: the baked set has the expected names, in order, with the SQL
/// present (baked via `include_str!`) — including the ACKED destructive
/// migration (its presence proves emit_migrations accepted an acknowledged
/// `DROP TABLE`, since an unacked one would have failed the fixture build).
#[test]
fn embedded_set_is_baked_with_names_and_sql() {
    let names: Vec<&str> = MIGRATIONS.iter().map(|&(n, _)| n).collect();
    assert_eq!(
        names,
        vec!["0001_create.sql", "0002_add_col.sql", "0003_drop_scratch.sql"],
        "the baked set, in lexicographic-by-name order"
    );

    // The SQL rode `include_str!` — the real file bytes are present.
    let create = MIGRATIONS.iter().find(|&&(n, _)| n == "0001_create.sql").expect("0001");
    assert!(create.1.contains("CREATE TABLE app_items"), "0001 SQL baked");

    // The destructive migration is baked WITH its acknowledgement (the fixture
    // built, so emit_migrations accepted it).
    let drop = MIGRATIONS.iter().find(|&&(n, _)| n == "0003_drop_scratch.sql").expect("0003");
    assert!(drop.1.contains("DROP TABLE app_scratch"), "0003 destructive SQL baked");
    assert!(drop.1.contains("bsql:ack-destructive"), "0003 carries its ack marker");
}

/// LIVE WITNESS: the embedded set applies to a real database in an isolated
/// schema — the include_str! embed -> `run_migrations` chain end to end — and a
/// re-run is a no-op (exactly-once).
#[test]
#[ignore = "requires local PG"]
fn embedded_set_applies_live_and_is_idempotent() {
    use bsql_postgres_sync::{ConnectConfig, Connection, MigrationSource, SslMode};

    let cfg = ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable);
    let mut conn = Connection::connect(&cfg).expect("connect");
    let schema = format!("bsql_embed_{}", std::process::id());
    conn.simple_query(&format!("CREATE SCHEMA {schema}")).expect("create schema");
    conn.simple_query(&format!("SET search_path TO {schema}")).expect("search_path");

    // Apply the EMBEDDED set (no filesystem access — the SQL is baked in).
    let report = conn.run_migrations(MigrationSource::embedded(MIGRATIONS)).expect("run");
    assert_eq!(
        report.applied,
        vec!["0001_create.sql", "0002_add_col.sql", "0003_drop_scratch.sql"]
    );

    // The migrations really ran: app_items has the added `qty` column, and the
    // acked-destructive migration dropped app_scratch.
    conn.execute_sql("INSERT INTO app_items (id, label, qty) VALUES (1, 'x', 5)")
        .expect("app_items with qty exists");
    let scratch = conn
        .query_sql("SELECT to_regclass('app_scratch') IS NULL AS dropped")
        .expect("q");
    assert_eq!(
        scratch.get(0).expect("row").get_bool(0).expect("bool"),
        Some(true),
        "app_scratch was created then dropped by the acked-destructive migration"
    );

    // Re-run: no-op (exactly-once).
    let again = conn.run_migrations(MigrationSource::embedded(MIGRATIONS)).expect("rerun");
    assert!(again.applied.is_empty());
    assert_eq!(again.already_applied, 3);

    conn.simple_query(&format!("DROP SCHEMA {schema} CASCADE")).expect("drop schema");
}
