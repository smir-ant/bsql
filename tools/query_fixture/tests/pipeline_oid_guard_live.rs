//! LIVE proof of the PER-COMMAND typed result-schema OID guard on the PIPELINE
//! path (both drivers).
//!
//! A `pipeline((...))` decodes each command into ITS OWN `Rows<Qi>`, so a
//! cache-MISS command whose runtime result columns diverged from its carrier's
//! migration schema would silently mis-decode — the SAME class the single-query
//! guard closes, now closed on the pipeline too. On a MISS, each pipeline command
//! appends a `Describe(portal)`, and the receive multiplexer verifies each runtime
//! `RowDescription` column OID against that command's compile-time schema. A
//! divergence is a classified `DriverError::BatchColumnOidMismatch` naming the
//! failing COMMAND (via `batch_failed_index()`) plus the reused
//! `DecodeError::ColumnOidMismatch` (column / expected / found) — never a silent
//! value; the connection drains to a clean idle and is reusable.
//!
//! HONEST NOTE: unlike a mid-batch SERVER error (which rolls back), the mismatch is
//! a client decode rejection AFTER the server processed — and, for this implicit-tx
//! batch, COMMITTED — the transaction; the client returns the classified drift
//! instead of decoding. Fail-loud beats a silent wrong value: the caller learns the
//! SCHEMA drifted (fix it), not a transient error to blind-retry.
//!
//! PARALLEL-SAFE: every test creates its OWN connection and a PER-CONNECTION
//! `CREATE TEMP TABLE ...` shadowing a migration table (`oidguard` / `members`). A
//! TEMP table is session-local, so tests never interfere — run WITHOUT
//! `--test-threads=1`.
//!
//! Run with:
//!   cargo test -p bsql-query-fixture --test pipeline_oid_guard_live -- --ignored
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    clippy::panic,
    reason = "live test harness — expect/unwrap/panic surface failures loudly; not production fallbacks"
)]

use bsql::BindExt;

// Carriers over migration tables. `oidguard(tag text, vc varchar, bp bpchar,
// n int4)`; `members(id int, a age NOT NULL, h handle)` where `age` is a DOMAIN
// over int (a user-defined type whose RowDescription OID is dynamic, >= 16384).
bsql::query!(PgTag, "SELECT tag FROM oidguard"); // text (row-OID marker 25)
bsql::query!(PgN, "SELECT n FROM oidguard"); // int4 (23)
bsql::query!(PgVc, "SELECT vc FROM oidguard"); // varchar -> String (native OID 1043)
bsql::query!(PgBp, "SELECT bp FROM oidguard"); // bpchar  -> String (native OID 1042)
bsql::query!(PgAge, "SELECT a FROM members WHERE id = $1"); // domain age -> i32 (OID >= 16384)

// PostgreSQL built-in OIDs the guard reports for the drift.
const OID_TEXT: u32 = 25;
const OID_INT4: u32 = 23;

/// Per-test SCHEMA DDL for the user-type (domain) no-false-positive case: a
/// distinct schema on the search_path holding the `age` domain + a `members`
/// table, so a `SELECT a FROM members` reports the domain's DYNAMIC OID
/// (`>= 16384`) the guard SKIPS. Parallel-safe (distinct schema per test), matching
/// `query_domain_live` — the migration types are build-time only, so a live test
/// must create them itself.
fn ut_setup(schema: &str) -> String {
    format!(
        "SET client_min_messages = warning; \
         DROP SCHEMA IF EXISTS {schema} CASCADE; \
         CREATE SCHEMA {schema}; \
         SET search_path = {schema}; \
         CREATE DOMAIN age AS int CHECK (VALUE >= 0); \
         CREATE TABLE members (id int PRIMARY KEY, a age NOT NULL)"
    )
}

mod sync_driver {
    use super::{BindExt, OID_INT4, OID_TEXT, PgAge, PgBp, PgN, PgTag, PgVc};
    use bsql::DecodeError;
    use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    /// A per-connection TEMP shadow of `oidguard` with `tag` retyped to `int4`
    /// (the DRIFT) and one seeded row.
    fn shadow_drift(c: &mut Connection) {
        c.simple_query("CREATE TEMP TABLE oidguard (tag int4 NOT NULL, vc varchar NOT NULL, bp bpchar NOT NULL, n int4 NOT NULL)")
            .expect("create temp drift");
        c.simple_query("INSERT INTO oidguard (tag, vc, bp, n) VALUES (1094795585, 'vv', 'bb', 42)")
            .expect("insert drift");
    }

    /// A per-connection TEMP shadow of `oidguard` matching the migration types.
    fn shadow_match(c: &mut Connection) {
        c.simple_query("CREATE TEMP TABLE oidguard (tag text NOT NULL, vc varchar NOT NULL, bp bpchar NOT NULL, n int4 NOT NULL)")
            .expect("create temp match");
        c.simple_query("INSERT INTO oidguard (tag, vc, bp, n) VALUES ('hello', 'vv', 'bb', 42)")
            .expect("insert match");
    }

    #[test]
    #[ignore = "requires local PG"]
    fn a_drifted_pipeline_command_is_a_classified_batch_column_oid_mismatch() {
        let mut c = Connection::connect(&config()).expect("connect");
        shadow_drift(&mut c);
        // Command #0 (`n` int4) matches; command #1 (`tag`, typed text, is int4 in
        // the shadow) drifts. The guard fires on command #1's RowDescription.
        match c.pipeline((PgN::bind(()), PgTag::bind(()))) {
            Err(e @ DriverError::BatchColumnOidMismatch { .. }) => {
                assert_eq!(
                    e.batch_failed_index(),
                    Some(1),
                    "the drift is attributed to command #1",
                );
                assert!(!e.is_disconnect(), "a schema drift is not a disconnect");
                match &e {
                    DriverError::BatchColumnOidMismatch {
                        command,
                        source: DecodeError::ColumnOidMismatch { index, expected, found },
                    } => {
                        assert_eq!(*command, 1);
                        assert_eq!(*index, 0, "the drifting column is result column 0");
                        assert_eq!(*expected, OID_TEXT, "migration typed `tag` as text (25)");
                        assert_eq!(*found, OID_INT4, "the live TEMP column is int4 (23)");
                    }
                    other => panic!("expected a ColumnOidMismatch source, got {other:?}"),
                }
            }
            other => panic!("expected BatchColumnOidMismatch, got {other:?}"),
        }
        // The connection drained to a clean idle — it is REUSABLE.
        c.simple_query("SELECT 1").expect("connection recovers after the guard fires");
        c.close().expect("close");
    }

    #[test]
    #[ignore = "requires local PG"]
    fn a_matching_shadow_pipeline_decodes_correctly() {
        let mut c = Connection::connect(&config()).expect("connect");
        shadow_match(&mut c);
        let (n, tag) = c
            .pipeline((PgN::bind(()), PgTag::bind(())))
            .expect("matching pipeline runs");
        assert_eq!(n.iter().next().expect("row").expect("decode").n, 42);
        assert_eq!(tag.iter().next().expect("row").expect("decode").tag, "hello");
        c.close().expect("close");
    }

    #[test]
    #[ignore = "requires local PG"]
    fn varchar_and_bpchar_columns_in_a_pipeline_are_not_falsely_rejected() {
        let mut c = Connection::connect(&config()).expect("connect");
        shadow_match(&mut c);
        // `vc` (varchar, native OID 1043) and `bp` (bpchar, 1042) both marker-type to
        // the `text` (25) class — one wire decode, so NEITHER is a false mismatch.
        let (vc, bp) = c
            .pipeline((PgVc::bind(()), PgBp::bind(())))
            .expect("varchar/bpchar pipeline runs");
        assert_eq!(vc.iter().next().expect("row").expect("decode").vc, "vv");
        let bp_val = bp.iter().next().expect("row").expect("decode").bp;
        assert!(bp_val.starts_with("bb"), "bpchar decoded (got {bp_val:?})");
        c.close().expect("close");
    }

    #[test]
    #[ignore = "requires local PG"]
    fn a_user_type_column_in_a_pipeline_is_not_falsely_rejected() {
        // A domain column reports its OWN dynamic OID (>= 16384), which the guard
        // SKIPS (the user-type boundary) — decoded transparently as its base int4.
        // A per-test SCHEMA (matching `query_domain_live`) keeps it parallel-safe:
        // the `age` domain + `members` table live in a distinct schema on the
        // search_path, dropped at the end (the migration types are build-time only).
        let schema = "bsql_pl_oidguard_ut_sync";
        let mut c = Connection::connect(&config()).expect("connect");
        c.simple_query(&super::ut_setup(schema)).expect("setup schema");
        c.simple_query("INSERT INTO members (id, a) VALUES (1, 30)")
            .expect("insert member");
        let (age,) = c
            .pipeline((PgAge::bind((1,)),))
            .expect("domain-column pipeline runs");
        assert_eq!(age.iter().next().expect("row").expect("decode").a, 30);
        c.simple_query(&format!("DROP SCHEMA {schema} CASCADE"))
            .expect("drop schema");
        c.close().expect("close");
    }
}

mod async_driver {
    use super::{BindExt, OID_INT4, OID_TEXT, PgAge, PgBp, PgN, PgTag, PgVc};
    use bsql::DecodeError;
    use bsql_postgres_async::{ConnectConfig, Connection, DriverError, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    async fn shadow_drift(c: &mut Connection) {
        c.simple_query("CREATE TEMP TABLE oidguard (tag int4 NOT NULL, vc varchar NOT NULL, bp bpchar NOT NULL, n int4 NOT NULL)")
            .await
            .expect("create temp drift");
        c.simple_query("INSERT INTO oidguard (tag, vc, bp, n) VALUES (1094795585, 'vv', 'bb', 42)")
            .await
            .expect("insert drift");
    }

    async fn shadow_match(c: &mut Connection) {
        c.simple_query("CREATE TEMP TABLE oidguard (tag text NOT NULL, vc varchar NOT NULL, bp bpchar NOT NULL, n int4 NOT NULL)")
            .await
            .expect("create temp match");
        c.simple_query("INSERT INTO oidguard (tag, vc, bp, n) VALUES ('hello', 'vv', 'bb', 42)")
            .await
            .expect("insert match");
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn a_drifted_pipeline_command_is_a_classified_batch_column_oid_mismatch() {
        let mut c = Connection::connect(&config()).await.expect("connect");
        shadow_drift(&mut c).await;
        match c.pipeline((PgN::bind(()), PgTag::bind(()))).await {
            Err(e @ DriverError::BatchColumnOidMismatch { .. }) => {
                assert_eq!(e.batch_failed_index(), Some(1), "drift attributed to command #1");
                assert!(!e.is_disconnect(), "a schema drift is not a disconnect");
                match &e {
                    DriverError::BatchColumnOidMismatch {
                        command,
                        source: DecodeError::ColumnOidMismatch { index, expected, found },
                    } => {
                        assert_eq!(*command, 1);
                        assert_eq!(*index, 0);
                        assert_eq!(*expected, OID_TEXT);
                        assert_eq!(*found, OID_INT4);
                    }
                    other => panic!("expected a ColumnOidMismatch source, got {other:?}"),
                }
            }
            other => panic!("expected BatchColumnOidMismatch, got {other:?}"),
        }
        c.simple_query("SELECT 1")
            .await
            .expect("connection recovers after the guard fires");
        c.close().await.expect("close");
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn a_matching_shadow_pipeline_decodes_correctly() {
        let mut c = Connection::connect(&config()).await.expect("connect");
        shadow_match(&mut c).await;
        let (n, tag) = c
            .pipeline((PgN::bind(()), PgTag::bind(())))
            .await
            .expect("matching pipeline runs");
        assert_eq!(n.iter().next().expect("row").expect("decode").n, 42);
        assert_eq!(tag.iter().next().expect("row").expect("decode").tag, "hello");
        c.close().await.expect("close");
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn varchar_and_bpchar_columns_in_a_pipeline_are_not_falsely_rejected() {
        let mut c = Connection::connect(&config()).await.expect("connect");
        shadow_match(&mut c).await;
        let (vc, bp) = c
            .pipeline((PgVc::bind(()), PgBp::bind(())))
            .await
            .expect("varchar/bpchar pipeline runs");
        assert_eq!(vc.iter().next().expect("row").expect("decode").vc, "vv");
        let bp_val = bp.iter().next().expect("row").expect("decode").bp;
        assert!(bp_val.starts_with("bb"), "bpchar decoded (got {bp_val:?})");
        c.close().await.expect("close");
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn a_user_type_column_in_a_pipeline_is_not_falsely_rejected() {
        let schema = "bsql_pl_oidguard_ut_async";
        let mut c = Connection::connect(&config()).await.expect("connect");
        c.simple_query(&super::ut_setup(schema))
            .await
            .expect("setup schema");
        c.simple_query("INSERT INTO members (id, a) VALUES (1, 30)")
            .await
            .expect("insert member");
        let (age,) = c
            .pipeline((PgAge::bind((1,)),))
            .await
            .expect("domain-column pipeline runs");
        assert_eq!(age.iter().next().expect("row").expect("decode").a, 30);
        c.simple_query(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .expect("drop schema");
        c.close().await.expect("close");
    }
}
