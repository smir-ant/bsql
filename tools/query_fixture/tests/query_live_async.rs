//! LIVE `query!` round-trip over the ASYNC (tokio) driver — the async half of
//! the S18 gate.
//!
//! Mirrors `query_live_sync.rs` over the tokio driver: same literal-`SELECT`
//! queries, same end-to-end pipeline (`query!` -> `TypedQuery` -> `query_params`
//! -> `Rows<Q>` prebuffer -> typed decode), only `.await`ed. See that file for
//! the design notes (no schema setup; one carrier per round-trip).
//!
//! Run with: `cargo test -p bsql-query-fixture --test query_live_async -- --ignored`
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    reason = "live test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

use bsql_postgres_async::{ConnectConfig, Connection, DriverError, SslMode};

bsql_query_macros::query!(One, "SELECT 1::int4 AS n");
bsql_query_macros::query!(Seven, "SELECT 7::int4 AS n");
bsql_query_macros::query!(Hi, "SELECT 'hello'::text AS s");
bsql_query_macros::query!(Nums, "SELECT n FROM (VALUES (10::int4), (20), (30)) AS t(n)");
bsql_query_macros::query!(Many, "SELECT n FROM (VALUES (1::int4), (2)) AS t(n)");
bsql_query_macros::query!(Echo, "SELECT $1::int4 AS n");
bsql_query_macros::query!(EchoS, "SELECT $1::text AS s");
bsql_query_macros::query!(WithNull, "SELECT NULL::int4 AS n");
bsql_query_macros::query!(MaybeNum, "SELECT n FROM (VALUES (7::int4), (NULL)) AS t(n)");
bsql_query_macros::query!(RepeatLit, "SELECT 100::int4 AS n");

fn async_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// (a) The minimal no-schema case + the owned `query_one_typed`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_literal_select_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let rows = c.query_typed::<OneQuery>(()).await.expect("query_typed One");
    assert_eq!(rows.len(), 1);
    let rec = rows
        .iter()
        .next()
        .expect("one row")
        .expect("row decodes");
    assert_eq!(rec.n, 1);

    let owned = c
        .query_one_typed::<SevenQuery>(())
        .await
        .expect("query_one_typed Seven");
    assert_eq!(owned.n, 7);

    c.close().await.expect("close");
}

/// (b) TEXT column borrows the prebuffer.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_text_column_borrows_zero_copy() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let rows = c.query_typed::<HiQuery>(()).await.expect("query_typed Hi");
    let rec = rows
        .iter()
        .next()
        .expect("one row")
        .expect("row decodes");
    assert_eq!(rec.s, "hello");

    c.close().await.expect("close");
}

/// (c) Multi-row: iter() yields all, into_owned() gives the owned Vec.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_multi_row_iter_and_into_owned() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let rows = c.query_typed::<NumsQuery>(()).await.expect("query_typed Nums");
    assert_eq!(rows.len(), 3);
    let via_iter: Vec<i32> = rows.iter().map(|r| r.expect("decodes").n).collect();
    assert_eq!(via_iter, vec![10, 20, 30]);
    let owned = rows.into_owned().expect("into_owned");
    assert_eq!(owned.iter().map(|o| o.n).collect::<Vec<_>>(), vec![10, 20, 30]);

    c.close().await.expect("close");
}

/// `query_one_typed` rejects a multi-row result loudly.
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_one_typed_rejects_many_rows() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let many = c.query_one_typed::<ManyQuery>(()).await;
    assert!(
        matches!(many, Err(DriverError::TooManyRows)),
        "two rows must be TooManyRows, got {many:?}"
    );
    assert!(c.is_healthy());
    c.close().await.expect("close");
}

/// Int + text params round-trip through the binary-bind path.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_params_int_and_text_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let n = c
        .query_one_typed::<EchoQuery>((42,))
        .await
        .expect("query_one_typed Echo(42)");
    assert_eq!(n.n, 42);

    let s = c
        .query_one_typed::<EchoSQuery>(("hi",))
        .await
        .expect("query_one_typed EchoS");
    assert_eq!(s.s, "hi");

    c.close().await.expect("close");
}

/// A nullable column decodes `NULL -> None` and a present value -> `Some`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_nullable_column_decodes_none_and_some() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let only_null = c
        .query_one_typed::<WithNullQuery>(())
        .await
        .expect("query_one_typed WithNull");
    assert_eq!(only_null.n, None);

    let rows = c.query_typed::<MaybeNumQuery>(()).await.expect("query_typed MaybeNum");
    let vals: Vec<Option<i32>> = rows.iter().map(|r| r.expect("decodes").n).collect();
    assert_eq!(vals, vec![Some(7), None]);

    c.close().await.expect("close");
}

/// The repeat-call limitation: running the SAME carrier twice on one connection
/// fails LOUD with a duplicate_prepared_statement error (42P05), connection
/// stays healthy.
#[tokio::test]
#[ignore = "requires local PG"]
async fn repeat_same_carrier_fails_loud_and_stays_healthy() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let first = c.query_typed::<RepeatLitQuery>(()).await.expect("first call");
    assert_eq!(first.len(), 1);

    let second = c.query_typed::<RepeatLitQuery>(()).await;
    match second {
        Err(DriverError::Db(ref db)) => assert!(
            db.is_code("42P05"),
            "expected 42P05, got SQLSTATE {}",
            db.code
        ),
        other => panic!("expected a duplicate-prepared-statement Db error, got {other:?}"),
    }

    assert!(c.is_healthy());
    let after = c.query_one_typed::<SevenQuery>(()).await.expect("fresh carrier works");
    assert_eq!(after.n, 7);

    c.close().await.expect("close");
}
