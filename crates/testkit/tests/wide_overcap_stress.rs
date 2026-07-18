//! GATE (audit-8, offline): a nonconforming too-wide `RowDescription` tears down
//! and RECOVERS the connection every time — under SUSTAINED load, on BOTH drivers,
//! up to the widest wire column count possible (`i16::MAX`).
//!
//! This gate locks the audit-5 teardown fix under REPETITION: it drains a wide
//! result in a tight loop and asserts the connection recovers on EVERY iteration
//! — the property a happy-path test can never observe, and the direct regression
//! net for a teardown that leaks or poisons the connection only after the Nth
//! over-cap. It also SUBSUMES the former `overcap_recovery` test (now deleted as
//! a strict subset): its tight-boundary case (`MAX_ROW_COLUMNS + 1` columns — the
//! SMALLEST width that over-caps) is ported below as
//! `minimal_overcap_at_cap_plus_one_recovers_{async,sync}`, so every width and
//! assertion that test carried lives here.
//!
//! A conforming PostgreSQL errors at `1665` before producing a result, so the
//! client-side over-cap path is only reachable from a NONCONFORMING peer — which
//! the in-memory [`FakePostgres`] scripts deterministically, no network, no live
//! server. So this runs in the ROUTINE `cargo test --workspace` flow, not the
//! `--ignored` live tier.
//!
//! Two widths, both `> MAX_ROW_COLUMNS`:
//! - `3000` columns × `300` rows, drained `40×` on ONE connection (the drain
//!   loop is exercised `40 × 300 = 12 000` times);
//! - a single `32767` (`i16::MAX`) column result — the WIDEST count the wire's
//!   `int16` field can carry — proving the teardown holds at the boundary.
//!
//! Each iteration asserts (a) the wide query is a classified
//! `TooManyColumns { count, max = MAX_ROW_COLUMNS }` naming the exact limit, and
//! (b) a follow-up `SELECT 1` on the SAME connection succeeds — proving the
//! connection drained to a clean idle, not torn down.

use bsql_postgres_proto::MAX_ROW_COLUMNS;
use bsql_testkit::{rows, FakePostgres, FakeValue, ScriptedRows};

/// The widest wire column count possible: the `RowDescription`/`DataRow` field
/// count is a signed `int16`, so `i16::MAX` columns is the ceiling a
/// nonconforming server can put on the wire.
const I16_MAX_COLUMNS: usize = 32_767; // = i16::MAX

/// The moderate over-cap width used for the sustained drain loop (comfortably
/// past `MAX_ROW_COLUMNS = 1664`, wide enough to make each `RowDescription`
/// substantial without an outsized fixture).
const WIDE: usize = 3000;

/// Rows scripted for the sustained-load drain: `nrows` rows, each `width`
/// columns of a uniform `int4`. The cell values are irrelevant — the driver
/// rejects at the `RowDescription` and SKIPS the `DataRow` bytes during the
/// over-cap drain — so a uniform `Int4(0)` keeps the fixture trivial while the
/// row COUNT stresses the drain loop.
fn wide_reply(width: usize, nrows: usize) -> ScriptedRows {
    ScriptedRows::from_rows(vec![vec![FakeValue::Int4(0); width]; nrows])
}

/// Assert an over-cap error names the exact width and the driver's cap.
fn assert_over_cap(count: usize, max: usize, expected_width: usize) {
    assert_eq!(count, expected_width, "over-cap count is the scripted width");
    assert_eq!(max, MAX_ROW_COLUMNS, "the reported cap is the driver's max");
    assert!(
        expected_width > MAX_ROW_COLUMNS,
        "the scripted width must exceed the cap to over-cap",
    );
}

/// The number of drain iterations — enough that a teardown leaking or poisoning
/// the connection after the first over-cap would be caught.
const ITERS: usize = 40;

/// The sustained wide-drain loop over ONE async connection.
#[tokio::test]
async fn wide_overcap_drains_and_recovers_under_load_async() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT wide").returns(wide_reply(WIDE, 300));
    fake.on("SELECT 1").returns(rows![[1_i64]]);

    let mut conn = fake.connect().await.expect("connect");

    for i in 0..ITERS {
        let err = conn
            .query_raw("SELECT wide")
            .await
            .expect_err("a too-wide result must be a classified error");
        match err {
            bsql_postgres_async::DriverError::TooManyColumns { count, max } => {
                assert_over_cap(count, max, WIDE);
            }
            other => panic!("iter {i}: expected TooManyColumns, got {other:?}"),
        }

        // RECOVERED: a follow-up query on the SAME connection succeeds every
        // iteration — the connection drained to a clean idle, not torn down.
        let ok = match conn.query_raw("SELECT 1").await {
            Ok(r) => r,
            Err(e) => panic!("iter {i}: connection must recover after over-cap: {e:?}"),
        };
        assert_eq!(ok.get(0).expect("row 0").get_i64(0), Ok(Some(1)));
    }
}

/// The sync twin of the sustained wide-drain loop.
#[test]
fn wide_overcap_drains_and_recovers_under_load_sync() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT wide").returns(wide_reply(WIDE, 300));
    fake.on("SELECT 1").returns(rows![[1_i64]]);

    let mut conn = fake.connect_sync().expect("connect");

    for i in 0..ITERS {
        let err = conn
            .query_raw("SELECT wide")
            .expect_err("a too-wide result must be a classified error");
        match err {
            bsql_postgres_sync::DriverError::TooManyColumns { count, max } => {
                assert_over_cap(count, max, WIDE);
            }
            other => panic!("iter {i}: expected TooManyColumns, got {other:?}"),
        }

        let ok = match conn.query_raw("SELECT 1") {
            Ok(r) => r,
            Err(e) => panic!("iter {i}: connection must recover after over-cap: {e:?}"),
        };
        assert_eq!(ok.get(0).expect("row 0").get_i64(0), Ok(Some(1)));
    }
}

/// The widest wire column count (`i16::MAX = 32767`) over-caps and recovers —
/// async. A single row is enough; the point is the boundary width, not row count.
#[tokio::test]
async fn wide_overcap_at_i16_max_columns_recovers_async() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT widest").returns(wide_reply(I16_MAX_COLUMNS, 1));
    fake.on("SELECT 1").returns(rows![[1_i64]]);

    let mut conn = fake.connect().await.expect("connect");

    let err = conn
        .query_raw("SELECT widest")
        .await
        .expect_err("an i16::MAX-column result must be a classified error");
    match err {
        bsql_postgres_async::DriverError::TooManyColumns { count, max } => {
            assert_over_cap(count, max, I16_MAX_COLUMNS);
        }
        other => panic!("expected TooManyColumns at i16::MAX width, got {other:?}"),
    }

    let ok = conn
        .query_raw("SELECT 1")
        .await
        .expect("connection recovered after the widest-possible over-cap");
    assert_eq!(ok.get(0).expect("row 0").get_i64(0), Ok(Some(1)));
}

/// The widest wire column count (`i16::MAX = 32767`) over-caps and recovers —
/// sync.
#[test]
fn wide_overcap_at_i16_max_columns_recovers_sync() {
    let mut fake = FakePostgres::new();
    fake.on("SELECT widest").returns(wide_reply(I16_MAX_COLUMNS, 1));
    fake.on("SELECT 1").returns(rows![[1_i64]]);

    let mut conn = fake.connect_sync().expect("connect");

    let err = conn
        .query_raw("SELECT widest")
        .expect_err("an i16::MAX-column result must be a classified error");
    match err {
        bsql_postgres_sync::DriverError::TooManyColumns { count, max } => {
            assert_over_cap(count, max, I16_MAX_COLUMNS);
        }
        other => panic!("expected TooManyColumns at i16::MAX width, got {other:?}"),
    }

    let ok = conn
        .query_raw("SELECT 1")
        .expect("connection recovered after the widest-possible over-cap");
    assert_eq!(ok.get(0).expect("row 0").get_i64(0), Ok(Some(1)));
}

/// The TIGHT boundary — exactly `MAX_ROW_COLUMNS + 1` columns, the SMALLEST
/// width that over-caps (one column past the driver's cap) — is a classified,
/// recoverable `TooManyColumns` on ONE async connection. Ported verbatim from the
/// deleted `overcap_recovery` test so its minimal-over-cap width is not lost; a
/// single row is enough (the point is the boundary width, not row count). The row
/// is itself wider than the ingest buffer, so its `DataRow` exercises the
/// oversize-Skip drain path too.
#[tokio::test]
async fn minimal_overcap_at_cap_plus_one_recovers_async() {
    let over = MAX_ROW_COLUMNS.saturating_add(1);
    let mut fake = FakePostgres::new();
    fake.on("SELECT wide").returns(wide_reply(over, 1));
    fake.on("SELECT 1").returns(rows![[1_i64]]);

    let mut conn = fake.connect().await.expect("connect");

    let err = conn
        .query_raw("SELECT wide")
        .await
        .expect_err("a cap+1-column result must be a classified error");
    match err {
        bsql_postgres_async::DriverError::TooManyColumns { count, max } => {
            assert_over_cap(count, max, over);
        }
        other => panic!("expected TooManyColumns at cap+1 width, got {other:?}"),
    }

    let ok = conn
        .query_raw("SELECT 1")
        .await
        .expect("connection recovered after the minimal over-cap");
    assert_eq!(ok.get(0).expect("row 0").get_i64(0), Ok(Some(1)));
}

/// The tight-boundary (`MAX_ROW_COLUMNS + 1`) case — sync twin of
/// [`minimal_overcap_at_cap_plus_one_recovers_async`].
#[test]
fn minimal_overcap_at_cap_plus_one_recovers_sync() {
    let over = MAX_ROW_COLUMNS.saturating_add(1);
    let mut fake = FakePostgres::new();
    fake.on("SELECT wide").returns(wide_reply(over, 1));
    fake.on("SELECT 1").returns(rows![[1_i64]]);

    let mut conn = fake.connect_sync().expect("connect");

    let err = conn
        .query_raw("SELECT wide")
        .expect_err("a cap+1-column result must be a classified error");
    match err {
        bsql_postgres_sync::DriverError::TooManyColumns { count, max } => {
            assert_over_cap(count, max, over);
        }
        other => panic!("expected TooManyColumns at cap+1 width, got {other:?}"),
    }

    let ok = conn
        .query_raw("SELECT 1")
        .expect("connection recovered after the minimal over-cap");
    assert_eq!(ok.get(0).expect("row 0").get_i64(0), Ok(Some(1)));
}
