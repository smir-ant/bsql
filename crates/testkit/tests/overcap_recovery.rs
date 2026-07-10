//! A too-wide dynamic result RECOVERS the connection — the documented
//! `TooManyColumns` contract, proven end-to-end over the real driver + engine.
//!
//! A conforming PostgreSQL never emits a `RowDescription` wider than its own
//! `MaxTupleAttributeNumber` (it errors at 1665 before producing a result), so
//! the client-side over-cap path is only reachable from a NONCONFORMING peer —
//! exactly what a scripted fake models deterministically, with no network and no
//! server that would refuse the query first. The fake sends the full result
//! (`RowDescription` + `DataRow` + `CommandComplete` + `ReadyForQuery`) the
//! client must drain to recover.
//!
//! Both drivers run the SAME script: the too-wide query is a classified,
//! recoverable [`DriverError::TooManyColumns`] naming the exact limit, and a
//! follow-up query on the SAME connection succeeds — proving the connection was
//! drained to a clean idle, not torn down (the `alive_after=false → true` fix).

use bsql_postgres_proto::MAX_ROW_COLUMNS;
use bsql_testkit::{rows, FakePostgres, FakeValue, ScriptedRows};

/// The over-cap query and its scripted reply: one row exactly ONE column wider
/// than the driver's supported maximum, so it over-caps regardless of the cap's
/// value (a robust `MAX_ROW_COLUMNS + 1`, not a hard-coded width).
fn over_cap_reply() -> ScriptedRows {
    let over = MAX_ROW_COLUMNS.saturating_add(1);
    // The cell values are irrelevant — the driver rejects at the RowDescription
    // and swallows the DataRow bytes during recovery — so a uniform Int4 keeps
    // the fixture trivial. The row is itself wider than the ingest buffer, so its
    // DataRow exercises the oversize-Skip drain path too.
    ScriptedRows::from_rows(vec![vec![FakeValue::Int4(0); over]])
}

/// Assert an over-cap error names the exact limit.
fn assert_over_cap(count: usize, max: usize) {
    assert_eq!(count, MAX_ROW_COLUMNS.saturating_add(1), "over-cap count");
    assert_eq!(max, MAX_ROW_COLUMNS, "the reported cap is the driver's max");
}

#[tokio::test]
async fn overcap_result_is_classified_and_recovers_async() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT wide").returns(over_cap_reply());
    fake.on("SELECT 1").returns(rows![[1_i64]]);

    let mut conn = fake.connect().await.expect("connect");

    // The too-wide result is a CLASSIFIED, RECOVERABLE error naming the limit —
    // NOT a torn-down connection (the pre-fix `Io("connection torn down")`).
    let err = conn
        .query_sql("SELECT wide")
        .await
        .expect_err("an over-cap result must be a classified error");
    match err {
        bsql_postgres_async::DriverError::TooManyColumns { count, max } => assert_over_cap(count, max),
        other => panic!("expected TooManyColumns, got {other:?}"),
    }

    // The connection RECOVERED: a follow-up query on the SAME connection succeeds
    // (alive_after = true). Before the fix this panicked — the connection was dead.
    let ok = conn
        .query_sql("SELECT 1")
        .await
        .expect("connection recovered to idle after the over-cap");
    assert_eq!(ok.get(0).expect("row 0").get_i64(0), Ok(Some(1)));
}

#[test]
fn overcap_result_is_classified_and_recovers_sync() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT wide").returns(over_cap_reply());
    fake.on("SELECT 1").returns(rows![[1_i64]]);

    let mut conn = fake.connect_sync().expect("connect");

    let err = conn
        .query_sql("SELECT wide")
        .expect_err("an over-cap result must be a classified error");
    match err {
        bsql_postgres_sync::DriverError::TooManyColumns { count, max } => assert_over_cap(count, max),
        other => panic!("expected TooManyColumns, got {other:?}"),
    }

    let ok = conn
        .query_sql("SELECT 1")
        .expect("connection recovered to idle after the over-cap");
    assert_eq!(ok.get(0).expect("row 0").get_i64(0), Ok(Some(1)));
}
