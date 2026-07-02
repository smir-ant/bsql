//! LIVE `query!` round-trip over the ASYNC (tokio) driver — the async half of
//! the S18 gate.
//!
//! Mirrors `query_live_sync.rs` over the tokio driver: same literal-`SELECT`
//! queries, same end-to-end pipeline (`query!` -> `TypedQuery` -> `query_params`
//! -> `Rows<Q>` prebuffer -> typed decode), only `.await`ed. See that file for
//! the design notes (no schema setup; the per-connection statement cache makes a
//! carrier reusable on one connection).
//!
//! Run with: `cargo test -p bsql-query-fixture --test query_live_async -- --ignored`
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    reason = "live test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

use core::ops::ControlFlow;

use bsql_postgres_async::{ConnectConfig, Connection, DriverError, Pool, SslMode};

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
bsql_query_macros::query!(TxLit, "SELECT 11::int4 AS n");
bsql_query_macros::query!(MultiTxLit, "SELECT 22::int4 AS n");
bsql_query_macros::query!(HealLit, "SELECT 33::int4 AS n");
bsql_query_macros::query!(
    StreamAll,
    "SELECT n FROM (VALUES (1::int4), (2), (3), (4), (5)) AS t(n)"
);
bsql_query_macros::query!(
    StreamParam,
    "SELECT n FROM (VALUES (1::int4), (2), (3), (4), (5)) AS t(n) WHERE n <= $1::int4"
);

// ── widened types: float4 / float8 / bytea ──────────────────────────────
bsql_query_macros::query!(Fl, "SELECT 1.5::float8 AS x, 2.5::float4 AS y");
bsql_query_macros::query!(NullFloat, "SELECT NULL::float8 AS x");
bsql_query_macros::query!(Bytes, r"SELECT '\xDEADBEEF'::bytea AS b");
bsql_query_macros::query!(EchoF, "SELECT $1::float8 AS x");
bsql_query_macros::query!(EchoB, "SELECT $1::bytea AS b");
bsql_query_macros::query!(
    Mixed,
    r"SELECT 7::int4 AS i, 2.5::float4 AS f, 8.5::float8 AS g, '\x0102'::bytea AS b"
);

// ── widened ARRAY params: `col = ANY($1)` — see the sync file for the wire
//    rationale (the `array_send` bytes must reach PG byte-correct).
bsql_query_macros::query!(
    FloatAny,
    "SELECT x FROM (VALUES (1.5::float8), (2.5::float8), (3.5::float8)) t(x) WHERE x = ANY($1)"
);
bsql_query_macros::query!(
    Float4Any,
    "SELECT x FROM (VALUES (1.5::float4), (2.5::float4), (3.5::float4)) t(x) WHERE x = ANY($1)"
);
bsql_query_macros::query!(
    IntAny,
    "SELECT n FROM (VALUES (10::int8), (20::int8), (30::int8)) t(n) WHERE n = ANY($1)"
);
bsql_query_macros::query!(
    ByteaAny,
    r"SELECT b FROM (VALUES ('\x01'::bytea), ('\x02'::bytea), ('\x03'::bytea)) t(b) WHERE b = ANY($1)"
);

fn async_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// (a) The minimal no-schema case + the owned `query_one`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_literal_select_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let rows = c.query::<OneQuery>(()).await.expect("query One");
    assert_eq!(rows.len(), 1);
    let rec = rows
        .iter()
        .next()
        .expect("one row")
        .expect("row decodes");
    assert_eq!(rec.n, 1);

    let owned = c
        .query_one::<SevenQuery>(())
        .await
        .expect("query_one Seven");
    assert_eq!(owned.n, 7);

    c.close().await.expect("close");
}

/// (b) TEXT column borrows the prebuffer.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_text_column_borrows_zero_copy() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let rows = c.query::<HiQuery>(()).await.expect("query Hi");
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

    let rows = c.query::<NumsQuery>(()).await.expect("query Nums");
    assert_eq!(rows.len(), 3);
    let via_iter: Vec<i32> = rows.iter().map(|r| r.expect("decodes").n).collect();
    assert_eq!(via_iter, vec![10, 20, 30]);
    let owned = rows.into_owned().expect("into_owned");
    assert_eq!(owned.iter().map(|o| o.n).collect::<Vec<_>>(), vec![10, 20, 30]);

    c.close().await.expect("close");
}

/// `query_one` rejects a multi-row result loudly.
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_one_rejects_many_rows() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let many = c.query_one::<ManyQuery>(()).await;
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
        .query_one::<EchoQuery>((42,))
        .await
        .expect("query_one Echo(42)");
    assert_eq!(n.n, 42);

    let s = c
        .query_one::<EchoSQuery>(("hi",))
        .await
        .expect("query_one EchoS");
    assert_eq!(s.s, "hi");

    c.close().await.expect("close");
}

/// A nullable column decodes `NULL -> None` and a present value -> `Some`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_nullable_column_decodes_none_and_some() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let only_null = c
        .query_one::<WithNullQuery>(())
        .await
        .expect("query_one WithNull");
    assert_eq!(only_null.n, None);

    let rows = c.query::<MaybeNumQuery>(()).await.expect("query MaybeNum");
    let vals: Vec<Option<i32>> = rows.iter().map(|r| r.expect("decodes").n).collect();
    assert_eq!(vals, vec![Some(7), None]);

    c.close().await.expect("close");
}

/// The 42P05-is-gone proof: the SAME carrier looped on one connection succeeds
/// every time (before the statement cache, call #2 was a 42P05).
#[tokio::test]
#[ignore = "requires local PG"]
async fn same_carrier_loops_without_42p05() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    for i in 0..100 {
        let r = c
            .query::<RepeatLitQuery>(())
            .await
            .unwrap_or_else(|e| panic!("iteration {i} must succeed, got {e:?}"));
        assert_eq!(r.len(), 1, "iteration {i}: one row");
        let rec = r.iter().next().expect("row").expect("decodes");
        assert_eq!(rec.n, 100, "iteration {i}: n == 100");
    }
    assert!(c.is_healthy(), "connection healthy after 100 reuse runs");
    c.close().await.expect("close");
}

/// PLAN-REUSE OBSERVABLE: after several runs of one carrier, the server holds
/// EXACTLY ONE prepared statement (the Parse happened once). A fresh connection
/// starts with zero (handshake + `SHOW` are simple queries).
#[tokio::test]
#[ignore = "requires local PG"]
async fn plan_is_parsed_once_and_persists() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    for _ in 0..5 {
        assert_eq!(c.query::<RepeatLitQuery>(()).await.expect("run").len(), 1);
    }
    let result = c
        .query_sql("SELECT count(*)::int4 FROM pg_prepared_statements")
        .await
        .expect("count prepared statements");
    let count = result
        .rows
        .first()
        .expect("count row")
        .get_i32(0)
        .expect("count value");
    assert_eq!(count, 1, "the query! must be Parsed exactly once and persist");
    c.close().await.expect("close");
}

/// THE RESET-vs-STATEMENT-CACHE CONSISTENCY PROOF: `max_size = 1` forces every
/// checkout to reuse the SAME physical connection. The pool RESETS a reused
/// connection on acquire, but the reset is TARGETED — it keeps prepared
/// statements — so the per-connection statement cache stays consistent with the
/// server's prepared-statement set. Looping checkout -> reset -> same query! ->
/// return must stay green (no 42P05 duplicate-prepare, no Bind-to-missing) on one
/// stable backend pid, and the server must hold the statement EXACTLY ONCE the
/// whole time (parsed once, survives every reset).
#[tokio::test]
#[ignore = "requires local PG"]
async fn pooled_connection_reset_keeps_parsed_plan() {
    let pool = Pool::new(async_config(), 1).await.expect("pool");
    let mut pid: Option<i32> = None;
    for i in 0..20 {
        let mut c = pool.get().await.unwrap_or_else(|e| panic!("checkout {i}: {e:?}"));
        let this_pid = c.conn().expect("live").backend_pid();
        match pid {
            None => pid = Some(this_pid),
            Some(p) => {
                assert_eq!(p, this_pid, "checkout {i} must reuse the one physical connection")
            }
        }
        // Same carrier every checkout: after the on-acquire reset, this must still
        // reuse the server-side plan (no re-Parse -> no 42P05).
        let r = c
            .conn_mut()
            .expect("live")
            .query::<RepeatLitQuery>(())
            .await
            .unwrap_or_else(|e| panic!("checkout {i}: {e:?}"));
        assert_eq!(r.len(), 1, "checkout {i}: one row");
        // The targeted reset KEEPS statements: the server holds it exactly once,
        // never zero (dropped) and never more than one (re-Parsed under a new name).
        let count = c
            .conn_mut()
            .expect("live")
            .query_sql("SELECT count(*)::int4 FROM pg_prepared_statements")
            .await
            .unwrap_or_else(|e| panic!("checkout {i} count: {e:?}"))
            .rows
            .first()
            .expect("count row")
            .get_i32(0)
            .expect("count value");
        assert_eq!(count, 1, "checkout {i}: statement kept across reset (parsed once)");
        // `c` drops here -> returned to the pool dirty; the NEXT checkout resets it
        // (keeping the statement), and this loop proves the cache stays consistent.
    }
}

/// THE ROLLBACK-BRANCH + CACHE PROOF: a connection returned mid-transaction
/// (tx_status != Idle) makes the on-acquire reset take its ROLLBACK-prefixed
/// branch. That branch must abort the open transaction AND keep the cached
/// prepared statement, so a reused `query!` on the reacquired (same physical)
/// connection still succeeds (no 42P05) and the server holds it exactly once.
#[tokio::test]
#[ignore = "requires local PG"]
async fn pooled_reset_rolls_back_open_tx_and_keeps_plan() {
    let pool = Pool::new(async_config(), 1).await.expect("pool");
    let pid = {
        let mut c = pool.get().await.expect("get1");
        let conn = c.conn_mut().expect("live1");
        let pid = conn.backend_pid();
        // Cache the statement durably (autocommit), THEN open a transaction and
        // leave it open, so the connection is returned with tx_status = 'T'.
        assert_eq!(conn.query::<RepeatLitQuery>(()).await.expect("first use caches").len(), 1);
        conn.begin().await.expect("begin (leaves the tx open)");
        pid
    }; // dropped mid-transaction -> returned to the pool with an OPEN transaction
    // Reacquire the SAME physical connection: the on-acquire reset takes its
    // ROLLBACK-prefixed branch (tx_status != Idle), aborting the open tx while
    // KEEPING the cached statement.
    let mut c = pool.get().await.expect("get2");
    let conn = c.conn_mut().expect("live2");
    assert_eq!(conn.backend_pid(), pid, "max_size=1 must reuse the same physical connection");
    // Reuse the cached statement: must succeed (the ROLLBACK-reset kept it, no 42P05).
    assert_eq!(
        conn.query::<RepeatLitQuery>(()).await.expect("reuse after rollback-reset").len(),
        1
    );
    // The server still holds it exactly once (kept across the ROLLBACK-prefixed reset).
    let count = conn
        .query_sql("SELECT count(*)::int4 FROM pg_prepared_statements")
        .await
        .expect("count")
        .rows
        .first()
        .expect("count row")
        .get_i32(0)
        .expect("count value");
    assert_eq!(count, 1, "the ROLLBACK-prefixed reset kept the cached plan (parsed once)");
}

/// 42P05-GONE (transactional): a `query!` first used inside a committed
/// transaction, then again at Idle, SUCCEEDS (was a 42P05 before Close-before-Parse).
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_inside_committed_transaction_then_idle_succeeds() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let n = c
        .transaction(async |tx| Ok(tx.query::<TxLitQuery>(()).await?.len()))
        .await
        .expect("transaction commits");
    assert_eq!(n, 1);
    let again = c
        .query::<TxLitQuery>(())
        .await
        .expect("same carrier after commit must succeed (was 42P05)");
    assert_eq!(again.len(), 1);
    c.close().await.expect("close");
}

/// 42P05-GONE (across transactions): the SAME `query!` in MANY sequential
/// transactions all SUCCEED (was 42P05 on transaction #2).
#[tokio::test]
#[ignore = "requires local PG"]
async fn same_carrier_across_many_transactions_succeeds() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    for i in 0..20 {
        let n = c
            .transaction(async |tx| Ok(tx.query::<MultiTxLitQuery>(()).await?.len()))
            .await
            .unwrap_or_else(|e| panic!("transaction {i} must commit, got {e:?}"));
        assert_eq!(n, 1, "transaction {i}: one row");
    }
    c.close().await.expect("close");
}

/// DISCARD ALL self-heal: a dropped recorded statement makes the next reuse error
/// ONCE (loud, connection healthy); the call after that re-creates it and succeeds.
#[tokio::test]
#[ignore = "requires local PG"]
async fn discard_all_then_reuse_errors_once_then_self_heals() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    assert_eq!(c.query::<HealLitQuery>(()).await.expect("first use records").len(), 1);
    c.execute_sql("DISCARD ALL").await.expect("discard all");
    let poisoned = c.query::<HealLitQuery>(()).await;
    assert!(
        matches!(poisoned, Err(DriverError::Db(_))),
        "reuse over a dropped statement must be a loud Db error, got {poisoned:?}"
    );
    assert!(c.is_healthy(), "connection stays healthy (recoverable error)");
    let healed = c
        .query::<HealLitQuery>(())
        .await
        .expect("self-heal: the next use re-creates the statement and succeeds");
    assert_eq!(healed.len(), 1);
    c.close().await.expect("close");
}

/// STREAMING: `query_each` hands every row to the closure and returns `Ok(None)`
/// on a fully-streamed result.
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_each_streams_all_returns_none() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let mut collected = Vec::new();
    let done = c
        .query_each::<StreamAllQuery, _, _>((), |rec| {
            collected.push(rec.n);
            ControlFlow::<()>::Continue(())
        })
        .await
        .expect("query_each streams");
    assert_eq!(done, None, "a fully-streamed result returns Ok(None)");
    assert_eq!(collected, vec![1, 2, 3, 4, 5], "every row, in order");
    c.close().await.expect("close");
}

/// EARLY BREAK + DRAIN RECLAIM: break at row 3 of 5 -> `Ok(Some(..))`, the
/// connection is drained to a clean idle and a follow-up query on the SAME
/// connection succeeds.
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_each_break_early_drains_and_reuses() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let mut collected = Vec::new();
    let stopped = c
        .query_each::<StreamAllQuery, _, _>((), |rec| {
            collected.push(rec.n);
            if rec.n == 3 {
                ControlFlow::Break(rec.n)
            } else {
                ControlFlow::Continue(())
            }
        })
        .await
        .expect("query_each with an early break");
    assert_eq!(stopped, Some(3), "the break payload rides Ok(Some(..))");
    assert_eq!(collected, vec![1, 2, 3], "rows up to and including the break");
    assert!(
        c.is_healthy(),
        "an early break leaves the connection healthy (drained to a clean idle)"
    );
    let owned = c
        .query_one::<OneQuery>(())
        .await
        .expect("follow-up query on the reused connection");
    assert_eq!(owned.n, 1, "the reused connection returns correct data");
    c.close().await.expect("close");
}

/// TRANSACTION + REPEAT: `query_each` inside a transaction, repeated — the
/// statement cache's Close-before-Parse makes every run succeed (no 42P05).
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_each_inside_transaction_and_repeated() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    for i in 0..5 {
        let sum = c
            .transaction(async |tx| {
                let mut total = 0i64;
                tx.query_each::<StreamAllQuery, _, _>((), |rec| {
                    total += i64::from(rec.n);
                    ControlFlow::<()>::Continue(())
                })
                .await?;
                Ok(total)
            })
            .await
            .unwrap_or_else(|e| panic!("transaction {i} must commit, got {e:?}"));
        assert_eq!(sum, 15, "transaction {i}: 1+2+3+4+5");
    }
    assert!(c.is_healthy(), "connection healthy after repeated in-tx streams");
    c.close().await.expect("close");
}

/// PARAM: `query_each` binds `$1` (the cap) and streams the filtered rows.
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_each_with_param_streams_filtered() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let mut collected = Vec::new();
    let done = c
        .query_each::<StreamParamQuery, _, _>((3,), |rec| {
            collected.push(rec.n);
            ControlFlow::<()>::Continue(())
        })
        .await
        .expect("query_each with a bound param");
    assert_eq!(done, None, "fully streamed");
    assert_eq!(collected, vec![1, 2, 3], "only rows where n <= $1 (=3)");
    c.close().await.expect("close");
}

/// WIDENING (float4/float8): two fixed-width float columns round-trip exactly.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_float_columns_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let owned = c.query_one::<FlQuery>(()).await.expect("query_one Fl");
    assert_eq!(owned.x, 1.5_f64, "float8 1.5 exact");
    assert_eq!(owned.y, 2.5_f32, "float4 2.5 exact");
    c.close().await.expect("close");
}

/// WIDENING (nullable float): `NULL::float8` -> `Option<f64>` -> None.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_nullable_float_decodes_none() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let owned = c
        .query_one::<NullFloatQuery>(())
        .await
        .expect("query_one NullFloat");
    assert_eq!(owned.x, None, "NULL::float8 -> None");
    c.close().await.expect("close");
}

/// WIDENING (bytea): borrowed `&[u8]` and owned `Vec<u8>` both decode the bytes.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_bytea_column_borrowed_and_owned() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let rows = c.query::<BytesQuery>(()).await.expect("query Bytes");
    let rec = rows.iter().next().expect("one row").expect("row decodes");
    assert_eq!(rec.b, &[0xDE, 0xAD, 0xBE, 0xEF], "borrowed &[u8]");

    let owned = c.query_one::<BytesQuery>(()).await.expect("query_one Bytes");
    assert_eq!(owned.b, vec![0xDE, 0xAD, 0xBE, 0xEF], "owned Vec<u8>");
    c.close().await.expect("close");
}

/// WIDENING (params): a `float8` and a `&[u8]` bind through `ParamsWriter`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_float_and_bytea_params_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let f = c
        .query_one::<EchoFQuery>((1.25_f64,))
        .await
        .expect("query_one EchoF(1.25)");
    assert_eq!(f.x, 1.25_f64, "float8 param 1.25");

    let b = c
        .query_one::<EchoBQuery>((&[1u8, 2, 3][..],))
        .await
        .expect("query_one EchoB([1,2,3])");
    assert_eq!(b.b, vec![1u8, 2, 3], "bytea param [1,2,3]");
    c.close().await.expect("close");
}

/// WIDENING (mixed row): int + float4 + float8 + bytea on the per-cell path.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_mixed_fixed_and_variable_row() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let rows = c.query::<MixedQuery>(()).await.expect("query Mixed");
    let rec = rows.iter().next().expect("one row").expect("row decodes");
    assert_eq!(rec.i, 7);
    assert_eq!(rec.f, 2.5_f32);
    assert_eq!(rec.g, 8.5_f64);
    assert_eq!(rec.b, &[0x01, 0x02]);
    c.close().await.expect("close");
}

/// WIDENING ARRAY (float8[]): `x = ANY($1)` binds a `&[f64]` array over the
/// wire; sorted before the exact compare (VALUES+ANY order is unspecified).
#[tokio::test]
#[ignore = "requires local PG"]
async fn float8_array_any_bind_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let rows = c
        .query::<FloatAnyQuery>((&[1.5_f64, 3.5][..],))
        .await
        .expect("query FloatAny");
    let mut got: Vec<f64> = rows.iter().map(|r| r.expect("row decodes").x).collect();
    got.sort_by(f64::total_cmp);
    assert_eq!(got, vec![1.5_f64, 3.5], "float8[] ANY($1)");
    c.close().await.expect("close");
}

/// WIDENING ARRAY (float4[]): distinct element OID (700) + 4-byte width.
#[tokio::test]
#[ignore = "requires local PG"]
async fn float4_array_any_bind_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let rows = c
        .query::<Float4AnyQuery>((&[2.5_f32, 3.5][..],))
        .await
        .expect("query Float4Any");
    let mut got: Vec<f32> = rows.iter().map(|r| r.expect("row decodes").x).collect();
    got.sort_by(f32::total_cmp);
    assert_eq!(got, vec![2.5_f32, 3.5], "float4[] ANY($1)");
    c.close().await.expect("close");
}

/// WIDENING ARRAY (int8[]): the previously offline-only `col = ANY($1)` pattern.
#[tokio::test]
#[ignore = "requires local PG"]
async fn int8_array_any_bind_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let rows = c
        .query::<IntAnyQuery>((&[10i64, 30][..],))
        .await
        .expect("query IntAny");
    let mut got: Vec<i64> = rows.iter().map(|r| r.expect("row decodes").n).collect();
    got.sort_unstable();
    assert_eq!(got, vec![10i64, 30], "int8[] ANY($1)");
    c.close().await.expect("close");
}

/// WIDENING ARRAY (bytea[]): the variable-length element shape (`&[&[u8]]`).
#[tokio::test]
#[ignore = "requires local PG"]
async fn bytea_array_any_bind_round_trip() {
    const BYTEA_ARG: &[&[u8]] = &[b"\x01", b"\x03"];
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let rows = c
        .query::<ByteaAnyQuery>((BYTEA_ARG,))
        .await
        .expect("query ByteaAny");
    let mut got: Vec<Vec<u8>> = rows.iter().map(|r| r.expect("row decodes").b.to_vec()).collect();
    got.sort_unstable();
    assert_eq!(got, vec![vec![0x01u8], vec![0x03u8]], "bytea[] ANY($1)");
    c.close().await.expect("close");
}
