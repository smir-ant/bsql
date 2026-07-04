//! The moat's TEXT-path contract: the SIMPLE-query (`query_sql`) reply is either
//! byte-faithful to what a real PostgreSQL server sends, or a LOUD classified
//! error — never plausible-but-wrong text a consumer could bake into a green
//! `get_str` assertion. The testkit exists to prove genuine end-to-end
//! behaviour, not a mock, so serving bytes a real server never sends would be
//! the exact mock-divergence it eliminates.
//!
//! `timestamptz` / `timestamp` (binary-only bsql types, no ISO text form) and
//! `float4` / `float8` (Rust's `Display` diverges from PostgreSQL's
//! `float ::text` for large / small magnitudes and `±Infinity`) fail closed on
//! the `query_sql` path. The `query!` (binary) path over the SAME script stays
//! byte-exact — proven in `tools/query_fixture/tests/query_fake.rs`.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

use bsql_postgres_async::DriverError;
use bsql_postgres_proto::{Timestamp, Timestamptz, Uuid};
use bsql_testkit::{rows, FakePostgres, ScriptedRows};

/// Script one column, connect (which MUST succeed — the fail-close is scoped to
/// the simple-query reply, so the extended `query!` reply stays intact), run
/// `query_sql`, and assert it is a loud classified `DriverError::Db` naming the
/// faithful route (`query!`) and the offending type. RED before this slice: the
/// unfaithful cell silently rendered a plausible-but-wrong string and
/// `query_sql` returned `Ok`, so `expect_err` would panic.
async fn assert_query_sql_fails_closed(sql: &str, script: ScriptedRows, type_name: &str) {
    let mut fake = FakePostgres::new();
    fake.on(sql).returns(script);

    // build_script does NOT fail the whole script — connect succeeds.
    let mut conn = fake
        .connect()
        .await
        .expect("connect succeeds — the fail-close is scoped to the simple reply");

    let err = conn
        .query_sql(sql)
        .await
        .expect_err("query_sql over an unfaithful-text type must be loud, not fake text");
    assert!(matches!(err, DriverError::Db(_)), "classified DbError, got: {err:?}");
    let msg = format!("{err}");
    assert!(msg.contains("query!"), "names the faithful route `query!`: {msg}");
    assert!(msg.contains(type_name), "names the offending type `{type_name}`: {msg}");
}

#[tokio::test]
async fn query_sql_over_timestamptz_fails_closed() {
    assert_query_sql_fails_closed(
        "SELECT occurred_at FROM t",
        rows![[Timestamptz::from_micros(1_000_000)]],
        "timestamptz",
    )
    .await;
}

#[tokio::test]
async fn query_sql_over_timestamp_fails_closed() {
    assert_query_sql_fails_closed(
        "SELECT recorded_at FROM t",
        rows![[Timestamp::from_micros(2_000_000)]],
        "timestamp",
    )
    .await;
}

#[tokio::test]
async fn query_sql_over_float8_fails_closed() {
    assert_query_sql_fails_closed("SELECT ratio FROM t", rows![[1.5_f64]], "float8").await;
}

#[tokio::test]
async fn query_sql_over_float4_fails_closed() {
    assert_query_sql_fails_closed("SELECT ratio FROM t", rows![[2.5_f32]], "float4").await;
}

/// The sync twin: the same fail-close over the blocking driver.
#[test]
fn query_sql_over_timestamptz_fails_closed_sync() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT occurred_at FROM t")
        .returns(rows![[Timestamptz::from_micros(1_000_000)]]);
    let mut conn = fake.connect_sync().expect("connect (sync) succeeds");
    let err = conn
        .query_sql("SELECT occurred_at FROM t")
        .expect_err("query_sql (sync) over a timestamptz must be loud, not fake text");
    assert!(matches!(err, DriverError::Db(_)), "classified DbError, got: {err:?}");
    let msg = format!("{err}");
    assert!(msg.contains("query!"), "names the faithful route: {msg}");
    assert!(msg.contains("timestamptz"), "names the offending type: {msg}");
}

/// The positive control: a FAITHFUL new type (uuid — its `Display` IS PG's
/// `uuid ::text`) still flows through `query_sql`, so the fail-close is scoped
/// to the unfaithful types, not a blanket break of the new vocabulary.
#[tokio::test]
async fn query_sql_over_a_faithful_uuid_still_works() {
    let raw = [
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00,
    ];
    let mut fake = FakePostgres::new();
    fake.on("SELECT id FROM t").returns(rows![[Uuid::from_bytes(raw)]]);
    let mut conn = fake.connect().await.expect("connect");
    let result = conn.query_sql("SELECT id FROM t").await.expect("query_sql over a uuid works");
    assert_eq!(result.rows.len(), 1);
    // The dynamic Row's text form is exactly PostgreSQL's `uuid ::text`.
    assert_eq!(
        result.rows[0].get_str(0),
        Ok(Some("550e8400-e29b-41d4-a716-446655440000"))
    );
}
