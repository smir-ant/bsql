//! LIVE proof of the TYPED result-schema OID guard (both drivers).
//!
//! The typed `query!` decode is POSITIONAL / const-offset and, on a fresh Parse
//! (a cache MISS), the driver now appends a `Describe(portal)` and VERIFIES each
//! runtime `RowDescription` column OID against the carrier's compile-time
//! expected OID (the migration schema). A runtime column whose type DIVERGED from
//! the migration — an out-of-band `ALTER COLUMN TYPE`, or a `CREATE TEMP TABLE`
//! shadowing the migration table with a different column type — would otherwise
//! silently mis-decode (a `text` decoder reading 4 `int4` bytes yields a
//! plausible-but-wrong `"AAAA"`). This proves against a real PostgreSQL that the
//! mismatch is a CLASSIFIED `DecodeError::ColumnOidMismatch`, never a silent wrong
//! value — while the NORMAL and TEXT-FAMILY (`varchar`/`bpchar`) cases decode
//! correctly (no false positive).
//!
//! PARALLEL-SAFE: every test creates its OWN connection and a PER-CONNECTION
//! `CREATE TEMP TABLE oidguard (...)` shadowing the `0020_oidguard.sql` migration
//! table. A TEMP table is session-local, so tests never interfere — run WITHOUT
//! `--test-threads=1`.
//!
//! Run with:
//!   cargo test -p bsql-query-fixture --test query_oid_guard_live -- --ignored
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    reason = "live test harness — expect/unwrap/panic surface failures loudly; not production fallbacks"
)]

use core::ops::ControlFlow;

// `tag` is `text` (row-OID marker 25); `vc` is `varchar` and `bp` is `bpchar` —
// both type to Rust `String`/`&str` (marker OID 25) but PostgreSQL reports them
// as distinct native OIDs (1043 / 1042), which the guard's text-family class
// accepts (identical wire decode).
bsql::query!(OgTag, "SELECT tag FROM oidguard");
bsql::query!(OgVc, "SELECT vc FROM oidguard");
bsql::query!(OgBp, "SELECT bp FROM oidguard");

// PostgreSQL's built-in OIDs (< FirstNormalObjectId): the guard's expected/found.
const OID_TEXT: u32 = 25;
const OID_INT4: u32 = 23;

mod sync_driver {
    use super::{ControlFlow, OgBp, OgTag, OgVc, OID_INT4, OID_TEXT};
    use bsql::DecodeError;
    use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[test]
    #[ignore = "requires local PG"]
    fn shadow_of_a_different_type_is_a_classified_mismatch() {
        let mut c = Connection::connect(&config()).expect("connect");
        // A TEMP table shadowing the migration `oidguard(tag text)` with an `int4`
        // `tag` — the query resolves to it (pg_temp is searched first), so its
        // RowDescription reports int4, not text.
        c.simple_query("CREATE TEMP TABLE oidguard (tag int4 NOT NULL)")
            .expect("create temp");
        c.simple_query("INSERT INTO oidguard (tag) VALUES (1094795585)")
            .expect("insert");

        // (1) query (collect): the guard drains the result before any row is
        // decoded, so it is a classified error, not an empty `Rows`.
        match c.query::<OgTag>(()) {
            Err(DriverError::Decode(DecodeError::ColumnOidMismatch { index, expected, found })) => {
                assert_eq!(index, 0, "the diverging column is result column 0");
                assert_eq!(expected, OID_TEXT, "the migration typed `tag` as text (25)");
                assert_eq!(found, OID_INT4, "the live TEMP column is int4 (23)");
            }
            other => panic!("query(): expected ColumnOidMismatch, got {other:?}"),
        }
        // (2) query_one: same classified error, NOT a mis-decoded "AAAA" row.
        match c.query_one::<OgTag>(()) {
            Err(DriverError::Decode(DecodeError::ColumnOidMismatch { found, .. })) => {
                assert_eq!(found, OID_INT4);
            }
            other => panic!("query_one(): expected ColumnOidMismatch, got {other:?}"),
        }
        // (3) query_each: the drain swallows the row, so `on_row` NEVER sees a
        // garbage record — the error dominates.
        let mut seen = 0usize;
        let each = c.query_each::<OgTag, _, _>((), |_rec| {
            seen += 1;
            ControlFlow::<()>::Continue(())
        });
        match each {
            Err(DriverError::Decode(DecodeError::ColumnOidMismatch { found, .. })) => {
                assert_eq!(found, OID_INT4);
                assert_eq!(seen, 0, "query_each must yield NO garbage row before the mismatch");
            }
            other => panic!("query_each(): expected ColumnOidMismatch, got {other:?}"),
        }
        // The connection RECOVERED (the guard drained to a clean idle): a follow-up
        // query on the SAME connection works.
        let tag = c.simple_query("SELECT 1").expect("reuse after mismatch");
        assert_eq!(tag, "SELECT 1");
    }

    #[test]
    #[ignore = "requires local PG"]
    fn matching_text_shadow_decodes_correctly() {
        let mut c = Connection::connect(&config()).expect("connect");
        c.simple_query("CREATE TEMP TABLE oidguard (tag text NOT NULL)")
            .expect("create temp");
        c.simple_query("INSERT INTO oidguard (tag) VALUES ('hello')")
            .expect("insert");
        let row = c.query_one::<OgTag>(()).expect("matching shadow decodes");
        assert_eq!(row.tag, "hello", "a matching-typed shadow decodes the real value");
    }

    #[test]
    #[ignore = "requires local PG"]
    fn varchar_column_is_not_falsely_rejected() {
        let mut c = Connection::connect(&config()).expect("connect");
        c.simple_query("CREATE TEMP TABLE oidguard (vc varchar NOT NULL)")
            .expect("create temp");
        c.simple_query("INSERT INTO oidguard (vc) VALUES ('world')")
            .expect("insert");
        match c.query_one::<OgVc>(()) {
            Ok(row) => assert_eq!(row.vc, "world", "varchar decodes as text (family compat)"),
            Err(e) => panic!("varchar column falsely rejected: {e:?}"),
        }
    }

    #[test]
    #[ignore = "requires local PG"]
    fn bpchar_column_is_not_falsely_rejected() {
        let mut c = Connection::connect(&config()).expect("connect");
        c.simple_query("CREATE TEMP TABLE oidguard (bp char(8) NOT NULL)")
            .expect("create temp");
        c.simple_query("INSERT INTO oidguard (bp) VALUES ('bb')")
            .expect("insert");
        match c.query_one::<OgBp>(()) {
            // char(8) blank-pads to 8; the family class accepts bpchar as text.
            Ok(row) => assert_eq!(row.bp.trim_end(), "bb", "bpchar decodes as text (family compat)"),
            Err(e) => panic!("bpchar column falsely rejected: {e:?}"),
        }
    }

    /// TYPED cache `0A000` self-heal + drift classification (the RESULT-TYPE-change
    /// peer of the `26000` vanished-statement self-heal in `query_live_sync`).
    /// After a recorded typed statement, an out-of-band `ALTER COLUMN TYPE` changes
    /// the result type, so the next HIT reuse (bare `Bind`, no `Describe`) fails
    /// PostgreSQL's `0A000` "cached plan must not change result type". The driver
    /// SELF-HEALS on the forced MISS path, which sends a `Describe` and re-arms the
    /// RESULT-OID guard:
    ///   - a text-family shift (`text` -> `varchar`) STILL matches `OgTag`'s marker,
    ///     so the guard passes and the CORRECT rows return (transparent self-heal);
    ///   - a DIFFERING type (`text` -> `int4`) becomes a classified
    ///     `ColumnOidMismatch` (the right error) — never a raw `0A000`.
    ///
    /// A STRICT improvement over the old typed fail-loud on `0A000`.
    #[test]
    #[ignore = "requires local PG"]
    fn result_type_change_self_heals_or_classifies() {
        // Case 1: a text-family shift (text -> varchar) still matches OgTag (text 25).
        let mut c = Connection::connect(&config()).expect("connect");
        c.simple_query("CREATE TEMP TABLE oidguard (tag text NOT NULL)")
            .expect("temp shadow");
        c.simple_query("INSERT INTO oidguard (tag) VALUES ('hello')")
            .expect("insert");
        // Record the statement (MISS at Idle -> recorded, HIT-eligible next).
        assert_eq!(c.query_one::<OgTag>(()).expect("first records").tag, "hello");
        // Change the result type: the next HIT reuse fails 0A000.
        c.simple_query("ALTER TABLE oidguard ALTER COLUMN tag TYPE varchar")
            .expect("alter text -> varchar");
        // HIT -> 0A000 -> self-heal MISS -> Describe (varchar 1043) -> text-family
        // compat passes -> the correct rows (transparent; no 0A000 surfaces).
        let healed = c
            .query_one::<OgTag>(())
            .expect("0A000 self-heals to correct rows on a text-family match");
        assert_eq!(healed.tag, "hello", "the self-healed query returns the correct value");
        assert!(c.is_healthy(), "connection healthy after the self-heal");
        c.close().expect("close");

        // Case 2: a DIFFERING type (text -> int4) is a classified ColumnOidMismatch.
        let mut c = Connection::connect(&config()).expect("connect");
        c.simple_query("CREATE TEMP TABLE oidguard (tag text NOT NULL)")
            .expect("temp shadow");
        c.simple_query("INSERT INTO oidguard (tag) VALUES ('42')")
            .expect("insert");
        assert_eq!(c.query_one::<OgTag>(()).expect("first records").tag, "42");
        c.simple_query("ALTER TABLE oidguard ALTER COLUMN tag TYPE int4 USING tag::int4")
            .expect("alter text -> int4");
        // HIT -> 0A000 -> self-heal MISS -> Describe (int4 23) -> guard classifies.
        match c.query_one::<OgTag>(()) {
            Err(DriverError::Decode(DecodeError::ColumnOidMismatch { index, expected, found })) => {
                assert_eq!(index, 0, "the diverging column is result column 0");
                assert_eq!(expected, OID_TEXT, "OgTag expects text (25)");
                assert_eq!(found, OID_INT4, "the ALTERed column is int4 (23)");
            }
            other => panic!("a differing result type must self-heal to a classified ColumnOidMismatch, got {other:?}"),
        }
        assert!(c.is_healthy(), "connection recovers after the classified drift");
        c.close().expect("close");
    }
}

mod async_driver {
    use super::{ControlFlow, OgBp, OgTag, OgVc, OID_INT4, OID_TEXT};
    use bsql::DecodeError;
    use bsql_postgres_async::{ConnectConfig, Connection, DriverError, SslMode};

    fn config() -> ConnectConfig {
        ConnectConfig::new("127.0.0.1", "smir-ant")
            .database("postgres".to_string())
            .ssl_mode(SslMode::Disable)
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn shadow_of_a_different_type_is_a_classified_mismatch() {
        let mut c = Connection::connect(&config()).await.expect("connect");
        c.simple_query("CREATE TEMP TABLE oidguard (tag int4 NOT NULL)")
            .await
            .expect("create temp");
        c.simple_query("INSERT INTO oidguard (tag) VALUES (1094795585)")
            .await
            .expect("insert");

        match c.query::<OgTag>(()).await {
            Err(DriverError::Decode(DecodeError::ColumnOidMismatch { index, expected, found })) => {
                assert_eq!(index, 0);
                assert_eq!(expected, OID_TEXT, "the migration typed `tag` as text (25)");
                assert_eq!(found, OID_INT4, "the live TEMP column is int4 (23)");
            }
            other => panic!("query(): expected ColumnOidMismatch, got {other:?}"),
        }
        match c.query_one::<OgTag>(()).await {
            Err(DriverError::Decode(DecodeError::ColumnOidMismatch { found, .. })) => {
                assert_eq!(found, OID_INT4);
            }
            other => panic!("query_one(): expected ColumnOidMismatch, got {other:?}"),
        }
        let mut seen = 0usize;
        let each = c
            .query_each::<OgTag, _, _>((), |_rec| {
                seen += 1;
                ControlFlow::<()>::Continue(())
            })
            .await;
        match each {
            Err(DriverError::Decode(DecodeError::ColumnOidMismatch { found, .. })) => {
                assert_eq!(found, OID_INT4);
                assert_eq!(seen, 0, "query_each must yield NO garbage row before the mismatch");
            }
            other => panic!("query_each(): expected ColumnOidMismatch, got {other:?}"),
        }
        let tag = c.simple_query("SELECT 1").await.expect("reuse after mismatch");
        assert_eq!(tag, "SELECT 1");
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn matching_text_shadow_decodes_correctly() {
        let mut c = Connection::connect(&config()).await.expect("connect");
        c.simple_query("CREATE TEMP TABLE oidguard (tag text NOT NULL)")
            .await
            .expect("create temp");
        c.simple_query("INSERT INTO oidguard (tag) VALUES ('hello')")
            .await
            .expect("insert");
        let row = c.query_one::<OgTag>(()).await.expect("matching shadow decodes");
        assert_eq!(row.tag, "hello");
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn varchar_column_is_not_falsely_rejected() {
        let mut c = Connection::connect(&config()).await.expect("connect");
        c.simple_query("CREATE TEMP TABLE oidguard (vc varchar NOT NULL)")
            .await
            .expect("create temp");
        c.simple_query("INSERT INTO oidguard (vc) VALUES ('world')")
            .await
            .expect("insert");
        match c.query_one::<OgVc>(()).await {
            Ok(row) => assert_eq!(row.vc, "world", "varchar decodes as text (family compat)"),
            Err(e) => panic!("varchar column falsely rejected: {e:?}"),
        }
    }

    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn bpchar_column_is_not_falsely_rejected() {
        let mut c = Connection::connect(&config()).await.expect("connect");
        c.simple_query("CREATE TEMP TABLE oidguard (bp char(8) NOT NULL)")
            .await
            .expect("create temp");
        c.simple_query("INSERT INTO oidguard (bp) VALUES ('bb')")
            .await
            .expect("insert");
        match c.query_one::<OgBp>(()).await {
            Ok(row) => assert_eq!(row.bp.trim_end(), "bb", "bpchar decodes as text (family compat)"),
            Err(e) => panic!("bpchar column falsely rejected: {e:?}"),
        }
    }

    /// The async twin of the sync `result_type_change_self_heals_or_classifies`: a
    /// `0A000` cached-plan result-type change self-heals to the correct rows on a
    /// text-family match, and to a classified `ColumnOidMismatch` on a differing
    /// type — never a raw `0A000`.
    #[tokio::test]
    #[ignore = "requires local PG"]
    async fn result_type_change_self_heals_or_classifies() {
        // Case 1: a text-family shift (text -> varchar) still matches OgTag (text 25).
        let mut c = Connection::connect(&config()).await.expect("connect");
        c.simple_query("CREATE TEMP TABLE oidguard (tag text NOT NULL)")
            .await
            .expect("temp shadow");
        c.simple_query("INSERT INTO oidguard (tag) VALUES ('hello')")
            .await
            .expect("insert");
        assert_eq!(c.query_one::<OgTag>(()).await.expect("first records").tag, "hello");
        c.simple_query("ALTER TABLE oidguard ALTER COLUMN tag TYPE varchar")
            .await
            .expect("alter text -> varchar");
        let healed = c
            .query_one::<OgTag>(())
            .await
            .expect("0A000 self-heals to correct rows on a text-family match");
        assert_eq!(healed.tag, "hello", "the self-healed query returns the correct value");
        assert!(c.is_healthy(), "connection healthy after the self-heal");
        c.close().await.expect("close");

        // Case 2: a DIFFERING type (text -> int4) is a classified ColumnOidMismatch.
        let mut c = Connection::connect(&config()).await.expect("connect");
        c.simple_query("CREATE TEMP TABLE oidguard (tag text NOT NULL)")
            .await
            .expect("temp shadow");
        c.simple_query("INSERT INTO oidguard (tag) VALUES ('42')")
            .await
            .expect("insert");
        assert_eq!(c.query_one::<OgTag>(()).await.expect("first records").tag, "42");
        c.simple_query("ALTER TABLE oidguard ALTER COLUMN tag TYPE int4 USING tag::int4")
            .await
            .expect("alter text -> int4");
        match c.query_one::<OgTag>(()).await {
            Err(DriverError::Decode(DecodeError::ColumnOidMismatch { index, expected, found })) => {
                assert_eq!(index, 0, "the diverging column is result column 0");
                assert_eq!(expected, OID_TEXT, "OgTag expects text (25)");
                assert_eq!(found, OID_INT4, "the ALTERed column is int4 (23)");
            }
            other => panic!("a differing result type must self-heal to a classified ColumnOidMismatch, got {other:?}"),
        }
        assert!(c.is_healthy(), "connection recovers after the classified drift");
        c.close().await.expect("close");
    }
}
