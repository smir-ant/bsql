//! LIVE `query!` round-trip over the SYNC driver — the named S18 gate.
//!
//! Proves the WHOLE typed pipeline end-to-end against a real PostgreSQL:
//! `query!` (schema-validated at build) -> `TypedQuery` -> `query_params`
//! (Parse+Bind+Execute+Sync over the const wire artifact) -> the `Rows<Q>`
//! prebuffer -> typed decode back to the macro's records.
//!
//! Every query is a LITERAL `SELECT` needing no table, so it validates trivially
//! against the migration catalog and needs no live schema setup. Each distinct
//! `query!` carrier is executed AT MOST ONCE per connection — the macro path
//! Parses a named, content-addressed statement on every call, and re-Parsing the
//! same name in one session is a server error, so distinct round-trips use
//! distinct carriers (distinct SQL -> distinct content address).
//!
//! Run with: `cargo test -p bsql-query-fixture --test query_live_sync -- --ignored`
#![forbid(unsafe_code)]
// Live integration harness: `.expect(..)` here is the loud test-failure signal
// (it panics, surfacing the failure), not a silent production fallback.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    reason = "live test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

// One column, fixed-width, NOT NULL -> the borrowed record carries no lifetime
// (`One { n: i32 }`) and decodes through the vectorized fast path.
bsql_query_macros::query!(One, "SELECT 1::int4 AS n");
// A distinct literal so `query_one_typed` can run on the SAME connection without
// re-Parsing `One`'s content-addressed statement name.
bsql_query_macros::query!(Seven, "SELECT 7::int4 AS n");
// One TEXT column, NOT NULL -> the borrowed record carries `<'q>` and `s` aliases
// the prebuffer (`Hi<'q> { s: &'q str }`).
bsql_query_macros::query!(Hi, "SELECT 'hello'::text AS s");
// Multi-row via a `VALUES` derived table (no real table needed). The `int4` cast
// on the first row types the column; `n` is NOT NULL.
bsql_query_macros::query!(Nums, "SELECT n FROM (VALUES (10::int4), (20), (30)) AS t(n)");
// Zero rows (a literal SELECT filtered out) -> `query_one_typed` must classify
// `NoRows`.
bsql_query_macros::query!(NoneRow, "SELECT 1::int4 AS n WHERE false");
// Multi-row again (distinct SQL) -> `query_one_typed` must classify `TooManyRows`.
bsql_query_macros::query!(Many, "SELECT n FROM (VALUES (1::int4), (2)) AS t(n)");
// An INT param -> exercises the `(i32,)` binary-bind path end-to-end.
bsql_query_macros::query!(Echo, "SELECT $1::int4 AS n");
// A TEXT param -> exercises the `&str` binary-bind path end-to-end.
bsql_query_macros::query!(EchoS, "SELECT $1::text AS s");
// A NULL cast -> the inference engine types it nullable, so the record field is
// `Option<i32>`; it must decode to `None`.
bsql_query_macros::query!(WithNull, "SELECT NULL::int4 AS n");
// A `VALUES` column with a NULL row -> nullable `Option<i32>`, carrying BOTH a
// present value (`Some`) and a NULL (`None`) in one result.
bsql_query_macros::query!(MaybeNum, "SELECT n FROM (VALUES (7::int4), (NULL)) AS t(n)");
// A distinct literal for the repeat-call limitation probe.
bsql_query_macros::query!(RepeatLit, "SELECT 100::int4 AS n");

fn sync_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// (a) The minimal no-schema case: `SELECT 1::int4 AS n` round-trips to a typed
/// record `One { n: 1 }`, and `query_one_typed` yields the owned twin. Proves the
/// whole pipeline with zero schema setup.
#[test]
#[ignore = "requires local PG"]
fn typed_literal_select_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let rows = c.query_typed::<OneQuery>(()).expect("query_typed One");
    assert_eq!(rows.len(), 1, "exactly one row");
    let rec = rows
        .iter()
        .next()
        .expect("one row present")
        .expect("row decodes");
    assert_eq!(rec.n, 1, "SELECT 1::int4 must decode to n == 1");

    // `query_one_typed` returns the OWNED twin (outlives the buffer). Distinct
    // carrier (`Seven`) so its statement name does not collide with `One`.
    let owned = c.query_one_typed::<SevenQuery>(()).expect("query_one_typed Seven");
    assert_eq!(owned.n, 7, "SELECT 7::int4 must decode to n == 7");

    c.close().expect("close");
}

/// (b) A TEXT column proves the borrowed record aliases the prebuffer (zero-copy
/// `&str`).
#[test]
#[ignore = "requires local PG"]
fn typed_text_column_borrows_zero_copy() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let rows = c.query_typed::<HiQuery>(()).expect("query_typed Hi");
    assert_eq!(rows.len(), 1);
    let rec = rows
        .iter()
        .next()
        .expect("one row")
        .expect("row decodes");
    assert_eq!(rec.s, "hello", "text column borrows 'hello' from the prebuffer");

    c.close().expect("close");
}

/// (c) A multi-row result: `iter()` yields every row, and `into_owned()` gives a
/// `Vec` of owned twins (on the SAME single round-trip — iter borrows the buffer,
/// then into_owned consumes it).
#[test]
#[ignore = "requires local PG"]
fn typed_multi_row_iter_and_into_owned() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let rows = c.query_typed::<NumsQuery>(()).expect("query_typed Nums");
    assert_eq!(rows.len(), 3, "three VALUES rows");

    // iter() yields all three, in order.
    let via_iter: Vec<i32> = rows
        .iter()
        .map(|r| r.expect("row decodes").n)
        .collect();
    assert_eq!(via_iter, vec![10, 20, 30]);

    // into_owned() (consumes `rows` after the iter borrows are dropped) yields the
    // owned twins.
    let owned = rows.into_owned().expect("into_owned");
    assert_eq!(owned.len(), 3);
    assert_eq!(owned.iter().map(|o| o.n).collect::<Vec<_>>(), vec![10, 20, 30]);

    c.close().expect("close");
}

/// `query_one_typed` is EXACTLY-one: zero rows -> `NoRows`, more than one ->
/// `TooManyRows` (never a silently-taken first row).
#[test]
#[ignore = "requires local PG"]
fn query_one_typed_classifies_zero_and_many() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let none = c.query_one_typed::<NoneRowQuery>(());
    assert!(
        matches!(none, Err(DriverError::NoRows)),
        "zero rows must be NoRows, got {none:?}"
    );

    let many = c.query_one_typed::<ManyQuery>(());
    assert!(
        matches!(many, Err(DriverError::TooManyRows)),
        "two rows must be TooManyRows, got {many:?}"
    );

    // The connection survives both classified errors (they are decode-side, not
    // connection faults): a follow-up typed query still works.
    assert!(c.is_healthy(), "connection stays healthy after classified errors");
    c.close().expect("close");
}

/// A real parameter round-trips through the `(i32,)` / `(&str,)` binary-bind
/// path: the value is encoded as a Bind param and echoed back as a typed record.
#[test]
#[ignore = "requires local PG"]
fn typed_params_int_and_text_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let n = c
        .query_one_typed::<EchoQuery>((42,))
        .expect("query_one_typed Echo(42)");
    assert_eq!(n.n, 42, "int4 param 42 must round-trip");

    // A `&'static str` literal binds through the text-param path.
    let s = c
        .query_one_typed::<EchoSQuery>(("hi",))
        .expect("query_one_typed EchoS(\"hi\")");
    assert_eq!(s.s, "hi", "text param \"hi\" must round-trip");

    c.close().expect("close");
}

/// A nullable column decodes `NULL -> None` and a present value -> `Some` on the
/// SAME `Option<T>` record field — proving nullable decode end-to-end.
#[test]
#[ignore = "requires local PG"]
fn typed_nullable_column_decodes_none_and_some() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // `NULL::int4` is nullable -> the field is `Option<i32>`; the value is None.
    let only_null = c
        .query_one_typed::<WithNullQuery>(())
        .expect("query_one_typed WithNull");
    assert_eq!(only_null.n, None, "NULL::int4 must decode to None");

    // A VALUES column with a NULL row carries Some(7) then None.
    let rows = c.query_typed::<MaybeNumQuery>(()).expect("query_typed MaybeNum");
    let vals: Vec<Option<i32>> = rows.iter().map(|r| r.expect("decodes").n).collect();
    assert_eq!(vals, vec![Some(7), None], "Some(value) then None on an Option column");

    c.close().expect("close");
}

/// The repeat-call limitation, documented on `query_typed`/`query_one_typed`:
/// running the SAME `query!` carrier twice on ONE connection re-Parses its
/// content-addressed prepared statement, so the second call fails LOUD with a
/// `duplicate_prepared_statement` server error (SQLSTATE 42P05) — AND the
/// connection stays healthy + pooled (the error is recoverable, not fatal).
#[test]
#[ignore = "requires local PG"]
fn repeat_same_carrier_fails_loud_and_stays_healthy() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // First call Parses + runs fine.
    let first = c.query_typed::<RepeatLitQuery>(()).expect("first call succeeds");
    assert_eq!(first.len(), 1);

    // Second call re-Parses the same statement name -> 42P05.
    let second = c.query_typed::<RepeatLitQuery>(());
    match second {
        Err(DriverError::Db(ref db)) => assert!(
            db.is_code("42P05"),
            "expected duplicate_prepared_statement (42P05), got SQLSTATE {}",
            db.code
        ),
        other => panic!("expected a duplicate-prepared-statement Db error, got {other:?}"),
    }

    // The connection survives the recoverable error: a DIFFERENT carrier still
    // runs on the same connection.
    assert!(c.is_healthy(), "connection stays healthy after the 42P05");
    let after = c.query_one_typed::<SevenQuery>(()).expect("a fresh carrier still works");
    assert_eq!(after.n, 7);

    c.close().expect("close");
}
