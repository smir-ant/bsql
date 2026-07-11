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
use core::str::FromStr as _;

use bsql::{Date, Interval, Jsonb, Numeric, Time, Timestamptz, Uuid};
use bsql_postgres_async::{ConnectConfig, Connection, DriverError, Pool, SslMode};

bsql::query!(One, "SELECT 1::int4 AS n");
// A genuinely SLOW typed query (the server sleeps 0.2s) — the witness that
// slow-query detection covers the compile-checked FLAGSHIP, not only dynamic SQL.
bsql::query!(
    SlowSleep,
    "SELECT n FROM (VALUES (1::int4)) AS t(n) WHERE pg_sleep(0.2) IS NOT NULL"
);
bsql::query!(Seven, "SELECT 7::int4 AS n");
bsql::query!(Hi, "SELECT 'hello'::text AS s");
bsql::query!(Nums, "SELECT n FROM (VALUES (10::int4), (20), (30)) AS t(n)");
bsql::query!(Many, "SELECT n FROM (VALUES (1::int4), (2)) AS t(n)");
bsql::query!(NoneRow, "SELECT 1::int4 AS n WHERE false");
bsql::query!(Echo, "SELECT $1::int4 AS n");
bsql::query!(EchoS, "SELECT $1::text AS s");
bsql::query!(WithNull, "SELECT NULL::int4 AS n");
bsql::query!(MaybeNum, "SELECT n FROM (VALUES (7::int4), (NULL)) AS t(n)");
bsql::query!(RepeatLit, "SELECT 100::int4 AS n");
bsql::query!(TxLit, "SELECT 11::int4 AS n");
bsql::query!(MultiTxLit, "SELECT 22::int4 AS n");
bsql::query!(HealLit, "SELECT 33::int4 AS n");
bsql::query!(
    StreamAll,
    "SELECT n FROM (VALUES (1::int4), (2), (3), (4), (5)) AS t(n)"
);
bsql::query!(
    StreamParam,
    "SELECT n FROM (VALUES (1::int4), (2), (3), (4), (5)) AS t(n) WHERE n <= $1::int4"
);

// ── widened types: float4 / float8 / bytea ──────────────────────────────
bsql::query!(Fl, "SELECT 1.5::float8 AS x, 2.5::float4 AS y");
bsql::query!(NullFloat, "SELECT NULL::float8 AS x");
bsql::query!(Bytes, r"SELECT '\xDEADBEEF'::bytea AS b");
bsql::query!(EchoF, "SELECT $1::float8 AS x");
bsql::query!(EchoB, "SELECT $1::bytea AS b");
bsql::query!(
    Mixed,
    r"SELECT 7::int4 AS i, 2.5::float4 AS f, 8.5::float8 AS g, '\x0102'::bytea AS b"
);

// ── widened ARRAY params: `col = ANY($1)` — see the sync file for the wire
//    rationale (the `array_send` bytes must reach PG byte-correct).
bsql::query!(
    FloatAny,
    "SELECT x FROM (VALUES (1.5::float8), (2.5::float8), (3.5::float8)) t(x) WHERE x = ANY($1)"
);
bsql::query!(
    Float4Any,
    "SELECT x FROM (VALUES (1.5::float4), (2.5::float4), (3.5::float4)) t(x) WHERE x = ANY($1)"
);
bsql::query!(
    IntAny,
    "SELECT n FROM (VALUES (10::int8), (20::int8), (30::int8)) t(n) WHERE n = ANY($1)"
);
bsql::query!(
    ByteaAny,
    r"SELECT b FROM (VALUES ('\x01'::bytea), ('\x02'::bytea), ('\x03'::bytea)) t(b) WHERE b = ANY($1)"
);

// ── BIG-PARAMETER witness (flagship typed path): a big bind streams onto the
//    growable send buffer instead of failing with `FrameTooLong`. The RESULT is
//    a single int (not the echoed blob) so the reply row stays well under the
//    inbound row buffer — the send-side cap (B1) is what this exercises.
bsql::query!(BigByteaLen, "SELECT length($1::bytea)::int4 AS n");

// ── widened bsql-native types: uuid / timestamptz / timestamp ─────────────
bsql::query!(
    UuidLit,
    "SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid AS u"
);
bsql::query!(TsLit, "SELECT '2000-01-01 00:00:01+00'::timestamptz AS t");
bsql::query!(TsNaiveLit, "SELECT '2000-01-01 00:00:02'::timestamp AS t");
bsql::query!(EchoUuid, "SELECT $1::uuid AS u");
bsql::query!(EchoTs, "SELECT $1::timestamptz AS t");
bsql::query!(JsonLit, "SELECT '{\"k\":1}'::json AS j");
bsql::query!(JsonbLit, "SELECT '[1,2,3]'::jsonb AS j");

// ── 1-D array result columns (see the sync twin) ─────────────────────────
bsql::query!(IntArrayLit, "SELECT ARRAY[10, NULL, 30]::int4[] AS xs");
bsql::query!(TextArrayLit, "SELECT ARRAY['a', NULL, 'c']::text[] AS xs");
bsql::query!(NullArrayLit, "SELECT NULL::int4[] AS xs");
bsql::query!(EmptyArrayLit, "SELECT ARRAY[]::int4[] AS xs");

// ── int2 (i16) + bool: the last two of the 18 scalar types without a
//    decode-VALUE witness through the `ColCellAt::decode_at` seam. The scalar
//    row is all-fixed-not-null (the const-offset FAST path decodes each column
//    through `<marker as ColCellAt<'_>>::decode_at`); the arrays exercise
//    `int2[]` / `bool[]` on the per-cell path (with a NULL element each).
bsql::query!(SmallBool, "SELECT 1::int2 AS a, true AS b");
bsql::query!(
    SmallBoolArrays,
    "SELECT ARRAY[1, NULL, 2]::int2[] AS c, ARRAY[true, NULL, false]::bool[] AS d"
);

// ── exact numeric / decimal (see the sync twin) ──────────────────────────
bsql::query!(EchoNum, "SELECT $1::numeric AS n");
bsql::query!(EchoNumText, "SELECT $1::numeric::text AS t");
bsql::query!(NumArrayLit, "SELECT '{1.5,NULL,100}'::numeric[] AS xs");

// ── temporal family: date / time / interval (see the sync twin) ──────────
bsql::query!(EchoDate, "SELECT $1::date AS d");
bsql::query!(EchoDateText, "SELECT $1::date::text AS t");
bsql::query!(EchoTime, "SELECT $1::time AS x");
bsql::query!(EchoTimeText, "SELECT $1::time::text AS t");
bsql::query!(EchoInterval, "SELECT $1::interval AS i");
bsql::query!(EchoIntervalText, "SELECT $1::interval::text AS t");
bsql::query!(DateArrayLit, "SELECT '{2000-01-01,NULL,2000-02-29}'::date[] AS xs");
bsql::query!(TimeArrayLit, "SELECT '{00:00:00,NULL,23:59:59.999999}'::time[] AS xs");
bsql::query!(
    IntervalArrayLit,
    r#"SELECT '{"1 day",NULL,"-1 day"}'::interval[] AS xs"#
);
bsql::query!(
    DateAny,
    "SELECT d FROM (VALUES ('2000-01-01'::date), ('2000-02-29'::date), ('9999-12-31'::date)) t(d) WHERE d = ANY($1)"
);

// A USING-merged column drawn from an OUTER-JOIN-promoted (nullable) side. `bk`
// is NOT NULL on every base table, yet the merged key CAN be NULL (an `oj_a`
// row with no matching `oj_b` null-extends `oj_b.bk`, which the second LEFT JOIN
// preserves). The soundness fix infers `bk` as `Option<i32>`; before it, this
// field was a non-Option `i32` and a real NULL crashed the decode. `ORDER BY`
// pins the row order so the (None, Some) pair is deterministic.
bsql::query!(
    OuterUsingNull,
    "SELECT bk FROM oj_a LEFT JOIN oj_b ON oj_a.j = oj_b.j \
     LEFT JOIN oj_c USING (bk) ORDER BY oj_a.j"
);

// ── INBOUND OVERSIZE-ROW witness (flagship typed path): a RESULT row WIDER than
//    the engine's inline read buffer (READ_BUF_CAP = 4096) streams from PG as
//    `RowChunk` pieces the typed `Rows` builder / `query_one` / `query_each` now
//    REASSEMBLE and decode identically to an inline row — at base each was a hard
//    `DriverError::OversizeRow`. Payloads are generated with repeat()/decode() to
//    exceed 4 KiB; the function-derived columns type nullable (`Option<_>`).
bsql::query!(OvBigText, "SELECT repeat('x', 5000)::text AS s");
bsql::query!(OvBigJsonb, r#"SELECT ('"' || repeat('z', 6000) || '"')::jsonb AS j"#);
bsql::query!(OvBigBytea, "SELECT decode(repeat('cd', 5000), 'hex')::bytea AS b");
bsql::query!(
    OvWideCols,
    "SELECT repeat('a', 450)::text AS c1, repeat('b', 450)::text AS c2, \
     repeat('c', 450)::text AS c3, repeat('d', 450)::text AS c4, \
     repeat('e', 450)::text AS c5, repeat('f', 450)::text AS c6, \
     repeat('g', 450)::text AS c7, repeat('h', 450)::text AS c8, \
     repeat('i', 450)::text AS c9, repeat('j', 450)::text AS c10, \
     repeat('k', 450)::text AS c11, repeat('l', 450)::text AS c12"
);
// A table-backed carrier for the multi-row cases: `body` is TEXT NOT NULL, so it
// decodes NON-nullable (`&str` borrowed / `String` owned) and `k` pins the order.
bsql::query!(OvRows, "SELECT body FROM ov_rows ORDER BY k");

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

/// `query_opt` is AT-MOST-one: zero rows -> `Ok(None)`, exactly one ->
/// `Ok(Some(record))`, more than one -> `TooManyRows` (same precedence as
/// `query_one`, only the zero-row outcome differs).
#[tokio::test]
#[ignore = "requires local PG"]
async fn query_opt_classifies_zero_one_and_many() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    // Zero rows -> Ok(None) (NOT NoRows — the whole point of the opt shape).
    let none = c.query_opt::<NoneRowQuery>(()).await.expect("query_opt runs");
    assert!(none.is_none(), "zero rows must be Ok(None), got {none:?}");

    // Exactly one row -> Ok(Some(owned record)).
    let one = c.query_opt::<OneQuery>(()).await.expect("query_opt runs");
    assert_eq!(one.expect("one row present").n, 1, "the single row decodes");

    // Two rows -> TooManyRows (loud, same as query_one — never a silent first row).
    let many = c.query_opt::<ManyQuery>(()).await;
    assert!(
        matches!(many, Err(DriverError::TooManyRows)),
        "two rows must be TooManyRows, got {many:?}"
    );

    assert!(c.is_healthy(), "connection stays healthy after classified errors");
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
        .get(0)
        .expect("count row")
        .get_i32(0).expect("count decodes").expect("count value");
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
    let pool = Pool::new(async_config(), 1);
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
            .get(0)
            .expect("count row")
            .get_i32(0).expect("count decodes").expect("count value");
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
    let pool = Pool::new(async_config(), 1);
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
        .get(0)
        .expect("count row")
        .get_i32(0).expect("count decodes").expect("count value");
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

/// BIG-PARAMETER STREAMING (async): a Bind whose encoded parameters far exceed
/// the old ~2 KiB bounded-frame cap now round-trips over REAL PG — the fix for
/// the B1 capability gap. Each of a ~4 KiB `bytea`, a ~5 KiB `jsonb`, and a
/// 500-element `int4[]` was a `FrameTooLong` before the Bind streamed onto the
/// growable send buffer (see `frames::bind_stream_twin` for the offline
/// RED→GREEN unit proof: the SAME builder `Err`s on the bounded `WriteBuf`).
#[tokio::test]
#[ignore = "requires local PG"]
async fn big_params_stream_past_the_old_bind_cap() {
    // Each parameter below encodes to > 4 KiB, FAR past the old ~2 KiB
    // bounded-`WriteBuf` Bind cap — before the streaming fix each was a
    // `FrameTooLong` on the wire. The exact round-trip is proven SERVER-SIDE (a
    // single-`bool` reply comparing the bound value against a server-constructed
    // reference), so the reply row stays tiny and this isolates the SEND-side
    // Bind (the reassembling dynamic path would also handle a big reply, but the
    // send cap is what B1 lifts).
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    // (a) ~4 KiB bytea (a small binary blob / image shape).
    let big_bytea = vec![0xABu8; 4096];
    let r = c
        .query_params_one(
            "SELECT ($1::bytea = decode(repeat('ab', 4096), 'hex')) AS eq",
            &(big_bytea.as_slice(),),
        )
        .await
        .expect("query_params_one 4 KiB bytea — was FrameTooLong before streaming");
    assert_eq!(r.get_bool(0), Ok(Some(true)), "4 KiB bytea param arrived byte-for-byte");

    // (b) ~5 KiB jsonb (a JSON string scalar, an owned param).
    let big_json = format!("\"{}\"", "x".repeat(5000));
    let r = c
        .query_params_one(
            "SELECT ($1::jsonb = ('\"' || repeat('x', 5000) || '\"')::jsonb) AS eq",
            &(Jsonb::new(big_json),),
        )
        .await
        .expect("query_params_one ~5 KiB jsonb — was FrameTooLong before streaming");
    assert_eq!(r.get_bool(0), Ok(Some(true)), "~5 KiB jsonb param arrived");

    // (c) 500-element int4[] — the array wire is ~4 KiB.
    let big_arr = vec![7i32; 500];
    let r = c
        .query_params_one(
            "SELECT ($1::int4[] = array_fill(7, ARRAY[500])) AS eq",
            &(big_arr.as_slice(),),
        )
        .await
        .expect("query_params_one 500-elem int4[] — was FrameTooLong before streaming");
    assert_eq!(r.get_bool(0), Ok(Some(true)), "500-element int4[] param arrived");

    // (d) FLAGSHIP typed `query!` path also streams a big Bind: a 4 KiB bytea
    //     param, returning its length (a tiny reply row).
    const BIG_BYTEA: &[u8] = &[0xCDu8; 4096];
    let n = c
        .query_one::<BigByteaLenQuery>((BIG_BYTEA,))
        .await
        .expect("query_one BigByteaLen(4 KiB) — was FrameTooLong before streaming");
    assert_eq!(n.n, Some(4096), "typed query! binds a 4 KiB bytea param");

    c.close().await.expect("close");
}

/// WIDENING (uuid): a `uuid` column decodes to its canonical hex form.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_uuid_column_round_trips() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let row = c.query_one::<UuidLitQuery>(()).await.expect("query_one UuidLit");
    assert_eq!(row.u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    c.close().await.expect("close");
}

/// WIDENING (timestamptz / timestamp): decode + exact epoch conversion.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_timestamp_columns_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let tz = c.query_one::<TsLitQuery>(()).await.expect("query_one TsLit");
    assert_eq!(tz.t.to_unix_micros(), Some(946_684_801_000_000));
    let naive = c.query_one::<TsNaiveLitQuery>(()).await.expect("query_one TsNaiveLit");
    assert_eq!(naive.t.as_micros(), 2_000_000);
    c.close().await.expect("close");
}

/// WIDENING (params): a `bsql::Uuid` and a `bsql::Timestamptz` bind and echo.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_uuid_and_timestamptz_params_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let u = Uuid::from_bytes([
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00,
    ]);
    let echoed = c.query_one::<EchoUuidQuery>((u,)).await.expect("query_one EchoUuid");
    assert_eq!(echoed.u, u);

    let ts = Timestamptz::from_micros(1_000_000);
    let echoed_ts = c.query_one::<EchoTsQuery>((ts,)).await.expect("query_one EchoTs");
    assert_eq!(echoed_ts.t, ts);
    assert_eq!(echoed_ts.t.to_unix_micros(), Some(946_684_801_000_000));
    c.close().await.expect("close");
}

/// WIDENING (json / jsonb): text surfaced verbatim (json) / past the version
/// byte (jsonb), plus a jsonb param round-trip.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_json_and_jsonb_columns_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let j = c.query_one::<JsonLitQuery>(()).await.expect("query_one JsonLit");
    assert_eq!(j.j.as_str(), r#"{"k":1}"#);
    let jb = c.query_one::<JsonbLitQuery>(()).await.expect("query_one JsonbLit");
    assert_eq!(jb.j.as_str(), "[1, 2, 3]");
    c.close().await.expect("close");
}

/// PRECISION BATTERY (numeric, async twin): the exact decimal battery binds a
/// `FromStr`-constructed `bsql::Numeric` param, round-trips through REAL PG, and
/// decodes to the exact string == `Display` == PG's `$1::numeric::text` oracle.
/// See the sync twin for the full rationale (a wrong digit is silent-wrong
/// money; values past `i128` prove arbitrary precision).
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_numeric_precision_battery() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    for s in [
        "0",
        "1",
        "-1",
        "0.1",
        "0.0001",
        "3.14159265358979323846",
        "1.500",
        "100.00",
        "123456789012345678901234567890123456789012345678901234567890",
        "-99999999999999999999999999999999999999999999.000001",
        "NaN",
    ] {
        let n = Numeric::from_str(s).expect("battery value parses");
        let echoed = c
            .query_one::<EchoNumQuery>((n.clone(),))
            .await
            .expect("echo numeric");
        let oracle = c
            .query_one::<EchoNumTextQuery>((n.clone(),))
            .await
            .expect("pg ::text oracle");
        assert_eq!(echoed.n.to_string(), s, "decode Display == expected for `{s}`");
        assert_eq!(
            echoed.n.to_string(),
            oracle.t,
            "decode Display == PG ::text for `{s}`",
        );
        assert_eq!(echoed.n, n, "decoded value equals the bound value for `{s}`");
    }
    c.close().await.expect("close");
}

/// PRECISION BATTERY (specials, async twin): `±Infinity` round-trip; a pre-14
/// server's loud `DbError` is a skip, never a false pass.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_numeric_infinity_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    for (value, text) in [
        (Numeric::infinity(), "Infinity"),
        (Numeric::neg_infinity(), "-Infinity"),
    ] {
        match c.query_one::<EchoNumQuery>((value.clone(),)).await {
            Ok(echoed) => {
                assert_eq!(echoed.n, value, "{text} round-trips exactly");
                assert_eq!(echoed.n.to_string(), text);
                let oracle = c
                    .query_one::<EchoNumTextQuery>((value.clone(),))
                    .await
                    .expect("pg ::text oracle");
                assert_eq!(oracle.t, text, "PG ::text renders {text}");
            }
            Err(DriverError::Db(_)) => {
                c = Connection::connect(&async_config())
                    .await
                    .expect("reconnect after skip");
            }
            Err(other) => panic!("unexpected error binding {text}: {other:?}"),
        }
    }
    c.close().await.expect("close");
}

/// ARRAYS (numeric, async twin): a real `numeric[]` with a NULL middle element
/// decodes to `Vec<Option<bsql::Numeric>>` with exact values.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_numeric_array_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let row = c
        .query_one::<NumArrayLitQuery>(())
        .await
        .expect("query_one NumArrayLit");
    let rendered: Vec<Option<String>> = row
        .xs
        .iter()
        .map(|e| e.as_ref().map(ToString::to_string))
        .collect();
    assert_eq!(
        rendered,
        vec![Some("1.5".to_string()), None, Some("100".to_string())],
        "numeric[] decodes exact values with a NULL element",
    );
    c.close().await.expect("close");
}

/// ARRAYS (async twin): a real `int4[]` / `text[]` with a NULL element decode
/// to `Vec<Option<T>>`; a NULL whole array to `None`; an empty array to an
/// empty `Vec`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_array_columns_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let ints = c.query_one::<IntArrayLitQuery>(()).await.expect("query_one IntArrayLit");
    assert_eq!(ints.xs, Some(vec![Some(10), None, Some(30)]));

    let labels = c.query_one::<TextArrayLitQuery>(()).await.expect("query_one TextArrayLit");
    assert_eq!(
        labels.xs,
        Some(vec![Some(String::from("a")), None, Some(String::from("c"))])
    );

    let none = c.query_one::<NullArrayLitQuery>(()).await.expect("query_one NullArrayLit");
    assert_eq!(none.xs, None);

    let empty = c.query_one::<EmptyArrayLitQuery>(()).await.expect("query_one EmptyArrayLit");
    assert_eq!(empty.xs, Some(Vec::<Option<i32>>::new()));

    c.close().await.expect("close");
}

/// int2 (`i16`) + `bool` — the last two of the 18 scalar types, decoded through
/// the unified `ColCellAt::decode_at` seam. The scalar pair is all-fixed-not-null
/// (const-offset FAST path); the arrays cover `int2[]` / `bool[]` (per-cell path,
/// each with a NULL element). Each column decodes to its DECLARED Rust type.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_int2_and_bool_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    // FAST path: two fixed-width, NOT-NULL columns (`int2` = 2 B, `bool` = 1 B).
    let sb = c.query_one::<SmallBoolQuery>(()).await.expect("query_one SmallBool");
    assert_eq!(sb.a, 1_i16);
    assert!(sb.b);

    // Per-cell path: `int2[]` and `bool[]`, each with an honest `None` element.
    let arr = c
        .query_one::<SmallBoolArraysQuery>(())
        .await
        .expect("query_one SmallBoolArrays");
    assert_eq!(arr.c, Some(vec![Some(1_i16), None, Some(2_i16)]));
    assert_eq!(arr.d, Some(vec![Some(true), None, Some(false)]));

    c.close().await.expect("close");
}

/// PRECISION BATTERY (date, async twin): calendar days — epoch, leap day, the
/// day before the epoch, year 1 AD, a far-future date, plus the ±infinity
/// sentinels — each round-trip bit-exact against the `$1::date::text` oracle.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_date_precision_battery() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    for s in ["2000-01-01", "2000-02-29", "1999-12-31", "0001-01-01", "9999-12-31"] {
        let d = Date::from_str(s).expect("date parses");
        let echoed = c.query_one::<EchoDateQuery>((d,)).await.expect("echo date");
        let oracle = c.query_one::<EchoDateTextQuery>((d,)).await.expect("oracle");
        assert_eq!(echoed.d.to_string(), s, "decode Display == expected for `{s}`");
        assert_eq!(echoed.d.to_string(), oracle.t, "decode Display == PG ::text for `{s}`");
        assert_eq!(echoed.d, d, "decoded value equals the bound value for `{s}`");
    }
    for (value, text) in [(Date::infinity(), "infinity"), (Date::neg_infinity(), "-infinity")] {
        let echoed = c.query_one::<EchoDateQuery>((value,)).await.expect("echo date inf");
        let oracle = c.query_one::<EchoDateTextQuery>((value,)).await.expect("oracle");
        assert_eq!(echoed.d, value, "{text} round-trips exactly");
        assert_eq!(echoed.d.to_string(), text);
        assert_eq!(oracle.t, text, "PG ::text renders {text}");
    }
    c.close().await.expect("close");
}

/// PRECISION BATTERY (time, async twin).
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_time_precision_battery() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    for s in ["00:00:00", "12:34:56.789012", "23:59:59.999999", "01:02:03"] {
        let t = Time::from_str(s).expect("time parses");
        let echoed = c.query_one::<EchoTimeQuery>((t,)).await.expect("echo time");
        let oracle = c.query_one::<EchoTimeTextQuery>((t,)).await.expect("oracle");
        assert_eq!(echoed.x.to_string(), s, "decode Display == expected for `{s}`");
        assert_eq!(echoed.x.to_string(), oracle.t, "decode Display == PG ::text for `{s}`");
        assert_eq!(echoed.x, t, "decoded value equals the bound value for `{s}`");
    }
    c.close().await.expect("close");
}

/// PRECISION BATTERY (interval, async twin): the three separate fields
/// round-trip bit-exact and render PostgreSQL's own `$1::interval::text`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_interval_precision_battery() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    for (value, text) in [
        (Interval::new(0, 0, 0), "00:00:00"),
        (Interval::new(14, 3, 14_706_000_000), "1 year 2 mons 3 days 04:05:06"),
        (Interval::new(0, -1, 0), "-1 days"),
        (Interval::new(1200, 0, 0), "100 years"),
        (Interval::new(0, 0, 3_723_000_000), "01:02:03"),
        (Interval::new(0, 1, 90_000_000_000), "1 day 25:00:00"),
        (Interval::new(0, -2, -11_045_678_000), "-2 days -03:04:05.678"),
        // Mixed-sign: a positive field after a negative one takes a `+` prefix
        // (PostgreSQL's `is_before` state); the oracle catches any divergence.
        (Interval::new(-1, 2, 0), "-1 mons +2 days"),
        (Interval::new(-13, 5, 0), "-1 years -1 mons +5 days"),
        (Interval::new(1, -2, 0), "1 mon -2 days"),
        (Interval::new(-1, -2, 10_800_000_000), "-1 mons -2 days +03:00:00"),
    ] {
        let echoed = c.query_one::<EchoIntervalQuery>((value,)).await.expect("echo interval");
        let oracle = c.query_one::<EchoIntervalTextQuery>((value,)).await.expect("oracle");
        assert_eq!(echoed.i.to_string(), text, "decode Display == expected for `{text}`");
        assert_eq!(echoed.i.to_string(), oracle.t, "decode Display == PG ::text for `{text}`");
        assert_eq!(echoed.i, value, "decoded fields equal the bound fields for `{text}`");
    }
    c.close().await.expect("close");
}

/// ARRAYS (temporal, async twin): `date[]` / `time[]` / `interval[]` with a
/// NULL middle element decode to `Vec<Option<T>>`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_temporal_arrays_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let dates = c.query_one::<DateArrayLitQuery>(()).await.expect("date[]");
    let d: Vec<Option<String>> =
        dates.xs.iter().map(|e| e.as_ref().map(ToString::to_string)).collect();
    assert_eq!(d, vec![Some("2000-01-01".to_string()), None, Some("2000-02-29".to_string())]);

    let times = c.query_one::<TimeArrayLitQuery>(()).await.expect("time[]");
    let t: Vec<Option<String>> =
        times.xs.iter().map(|e| e.as_ref().map(ToString::to_string)).collect();
    assert_eq!(t, vec![Some("00:00:00".to_string()), None, Some("23:59:59.999999".to_string())]);

    let spans = c.query_one::<IntervalArrayLitQuery>(()).await.expect("interval[]");
    let i: Vec<Option<String>> =
        spans.xs.iter().map(|e| e.as_ref().map(ToString::to_string)).collect();
    assert_eq!(i, vec![Some("1 day".to_string()), None, Some("-1 days".to_string())]);

    c.close().await.expect("close");
}

/// ARRAY ENCODE (date[], async twin): `d = ANY($1)` sends a `date[]` frame that
/// PostgreSQL must accept byte-correct.
#[tokio::test]
#[ignore = "requires local PG"]
async fn date_array_any_bind_round_trip() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    const WANTED: [Date; 2] = [Date::from_days(0), Date::from_days(2_921_939)];
    let rows = c.query::<DateAnyQuery>((&WANTED[..],)).await.expect("query DateAny");
    let mut got: Vec<i32> = rows.iter().map(|r| r.expect("row decodes").d.to_days()).collect();
    got.sort_unstable();
    assert_eq!(got, vec![0, 2_921_939], "date[] ANY($1) returns the matching rows");
    c.close().await.expect("close");
}

/// SOUNDNESS witness (async): a `USING`-merged column drawn from an
/// outer-join-promoted side decodes a REAL NULL into `None` without a decode
/// error. The `query!` above types `bk` as `Option<i32>` only because the
/// nullability fix propagates the LEFT JOIN's promotion through the merge; the
/// pre-fix inference typed it `i32`, and the first row's genuine NULL would then
/// fail `Rows` decode (`NullInNonNullColumn`) on a perfectly valid query.
#[tokio::test]
#[ignore = "requires local PG"]
async fn merged_outer_join_null_round_trips_as_none() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    // Fresh tables (drop any residue from an aborted prior run, dependents last).
    // `execute_sql` returns an affected-row count (no `#[must_use]` row handle).
    c.execute_sql("DROP TABLE IF EXISTS oj_c, oj_b, oj_a").await.expect("drop");
    c.execute_sql("CREATE TABLE oj_a (j INTEGER NOT NULL, x INTEGER)").await.expect("a");
    c.execute_sql("CREATE TABLE oj_b (j INTEGER NOT NULL, bk INTEGER NOT NULL, y INTEGER)")
        .await
        .expect("b");
    c.execute_sql("CREATE TABLE oj_c (bk INTEGER NOT NULL, z INTEGER)").await.expect("c");
    // j=1: no matching oj_b -> merged bk is NULL. j=2: fully matched -> bk = 42.
    c.execute_sql("INSERT INTO oj_a (j, x) VALUES (1, 100), (2, 200)").await.expect("ins a");
    c.execute_sql("INSERT INTO oj_b (j, bk, y) VALUES (2, 42, 7)").await.expect("ins b");
    c.execute_sql("INSERT INTO oj_c (bk, z) VALUES (42, 9)").await.expect("ins c");

    let rows = c.query::<OuterUsingNullQuery>(()).await.expect("query OuterUsingNull");
    let got: Vec<Option<i32>> = rows.iter().map(|r| r.expect("row decodes").bk).collect();
    assert_eq!(
        got,
        vec![None, Some(42)],
        "the outer-join×USING merged key round-trips its real NULL as None",
    );

    c.execute_sql("DROP TABLE oj_c, oj_b, oj_a").await.expect("cleanup");
    c.close().await.expect("close");
}

/// INBOUND OVERSIZE (async): a > 4 KiB TEXT result row streams from PG as
/// `RowChunk` pieces the typed path now REASSEMBLES — `query` (both `iter` and
/// `into_owned`) and `query_one` decode the exact 5000-byte value. At base each
/// of these was a hard `DriverError::OversizeRow`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn oversize_typed_text_row_reassembles() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    // (a) query -> Rows: one oversize row, borrowed then owned decode agree. The
    // borrowed record aliases `rows`, so it is scoped closed before `into_owned`
    // (the documented E0505 escape wall).
    let rows = c.query::<OvBigTextQuery>(()).await.expect("query OvBigText");
    assert_eq!(rows.len(), 1, "the oversize result is one row");
    {
        let rec = rows.iter().next().expect("one row").expect("row decodes");
        let borrowed = rec.s.expect("text present");
        assert_eq!(borrowed.len(), 5000, "borrowed > 4 KiB text reassembled contiguous");
        assert!(borrowed.bytes().all(|b| b == b'x'), "every byte survived reassembly");
    }
    let owned = rows.into_owned().expect("into_owned");
    let s_owned = owned[0].s.as_deref().expect("owned text present");
    assert_eq!(s_owned.len(), 5000, "owned reassembled text length");
    assert!(s_owned.bytes().all(|b| b == b'x'), "owned bytes intact");

    // (b) query_one on the SAME oversize row (single-row direct-decode path).
    let one = c.query_one::<OvBigTextQuery>(()).await.expect("query_one OvBigText");
    let s = one.s.expect("query_one text present");
    assert_eq!(s.len(), 5000, "query_one reassembles the oversize row");
    assert!(s.bytes().all(|b| b == b'x'));

    c.close().await.expect("close");
}

/// INBOUND OVERSIZE (async): a > 4 KiB JSONB and a > 4 KiB BYTEA column each
/// reassemble and round-trip their exact value through `query_one`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn oversize_typed_jsonb_and_bytea_reassemble() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");

    let jb = c.query_one::<OvBigJsonbQuery>(()).await.expect("query_one OvBigJsonb");
    let j = jb.j.expect("jsonb present");
    // PG renders the jsonb string scalar back WITH quotes: `"zzz…z"` (6000 z's).
    assert_eq!(j.as_str().len(), 6002, "> 4 KiB jsonb reassembled");
    assert!(j.as_str().starts_with('"') && j.as_str().ends_with('"'));
    assert!(j.as_str()[1..6001].bytes().all(|b| b == b'z'), "jsonb payload intact");

    let bt = c.query_one::<OvBigByteaQuery>(()).await.expect("query_one OvBigBytea");
    let bytes = bt.b.expect("bytea present");
    assert_eq!(bytes.len(), 5000, "> 4 KiB bytea reassembled");
    assert!(bytes.iter().all(|&x| x == 0xCD), "every bytea byte survived reassembly");

    c.close().await.expect("close");
}

/// INBOUND OVERSIZE (async): a row made oversize by MANY columns (none itself
/// over 4 KiB) reassembles — the width comes from column count, not one fat cell.
#[tokio::test]
#[ignore = "requires local PG"]
async fn oversize_typed_wide_many_columns_reassembles() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    let r = c.query_one::<OvWideColsQuery>(()).await.expect("query_one OvWideCols");
    // 12 columns × 450 bytes = 5400 payload bytes > READ_BUF_CAP, no single
    // column over 4 KiB — the reassembly path is reached by total width.
    for (field, ch) in [
        (r.c1, b'a'), (r.c2, b'b'), (r.c3, b'c'), (r.c4, b'd'),
        (r.c5, b'e'), (r.c6, b'f'), (r.c7, b'g'), (r.c8, b'h'),
        (r.c9, b'i'), (r.c10, b'j'), (r.c11, b'k'), (r.c12, b'l'),
    ] {
        let s = field.expect("wide column present");
        assert_eq!(s.len(), 450, "each column intact after reassembly");
        assert!(s.bytes().all(|b| b == ch), "each column's bytes unshuffled");
    }
    c.close().await.expect("close");
}

/// INBOUND OVERSIZE (async): the MULTI-ROW cases over a real table — an oversize
/// row FOLLOWED by a small one (the reassembly buffer must RESET), MULTIPLE
/// oversize rows in one result, `query_each` streaming a reassembled oversize
/// row, and `query_one` TOO-MANY over both orders (an oversize FIRST row + a
/// small second → the Row-arm break; a small FIRST row + an oversize second →
/// the RowChunk-arm break MID-oversize-frame), each proving the connection
/// drains to a clean idle and stays healthy. Single test (serial) so the shared
/// `ov_rows` table cannot race a parallel sibling.
#[tokio::test]
#[ignore = "requires local PG"]
async fn oversize_typed_multirow_reassembly_over_table() {
    let mut c = Connection::connect(&async_config()).await.expect("connect");
    c.execute_sql("DROP TABLE IF EXISTS ov_rows").await.expect("drop residue");
    c.execute_sql("CREATE TABLE ov_rows (k INTEGER NOT NULL, body TEXT NOT NULL)")
        .await
        .expect("create ov_rows");

    // Scenario A — oversize (k=1) THEN small (k=2): the reassembly accumulator
    // must reset so the small row is not corrupted by the prior oversize row.
    c.execute_sql("INSERT INTO ov_rows (k, body) VALUES (1, repeat('x', 5000)), (2, 'small')")
        .await
        .expect("insert oversize-then-small");
    let rows = c.query::<OvRowsQuery>(()).await.expect("query OvRows (A)");
    let lens: Vec<usize> = rows.iter().map(|r| r.expect("decodes").body.len()).collect();
    assert_eq!(lens, vec![5000, 5], "oversize row then small row; buffer reset");
    let owned = rows.into_owned().expect("into_owned (A)");
    assert!(owned[0].body.bytes().all(|b| b == b'x'), "oversize row bytes intact");
    assert_eq!(owned[1].body, "small", "the small row after an oversize row is clean");

    // `query_each` streams the same result: the oversize row reassembles into a
    // reused scratch buffer, then the small row streams normally.
    let mut streamed: Vec<usize> = Vec::new();
    c.query_each::<OvRowsQuery, _, ()>((), |rec| {
        streamed.push(rec.body.len());
        ControlFlow::Continue(())
    })
    .await
    .expect("query_each OvRows (A)");
    assert_eq!(streamed, vec![5000, 5], "query_each reassembles the oversize row and resets");

    // Scenario B — MULTIPLE oversize rows in one result; the accumulator resets
    // between them and each decodes to its own exact width.
    c.execute_sql("TRUNCATE ov_rows").await.expect("truncate");
    c.execute_sql("INSERT INTO ov_rows (k, body) VALUES (1, repeat('x', 5000)), (2, repeat('y', 6000))")
        .await
        .expect("insert two oversize");
    let owned = c
        .query::<OvRowsQuery>(())
        .await
        .expect("query OvRows (B)")
        .into_owned()
        .expect("into_owned (B)");
    assert_eq!(owned.len(), 2, "two oversize rows");
    assert_eq!(owned[0].body.len(), 5000);
    assert!(owned[0].body.bytes().all(|b| b == b'x'), "first oversize row intact");
    assert_eq!(owned[1].body.len(), 6000);
    assert!(owned[1].body.bytes().all(|b| b == b'y'), "second oversize row intact");

    // Scenario C — query_one TOO-MANY, oversize FIRST then small: the oversize
    // row reassembles + counts (seen_first set at RowChunkEnd), then the small
    // second row trips the Row-arm break. TooManyRows must dominate, and the
    // connection must drain from that whole-row boundary to a clean idle.
    c.execute_sql("TRUNCATE ov_rows").await.expect("truncate C");
    c.execute_sql("INSERT INTO ov_rows (k, body) VALUES (1, repeat('x', 5000)), (2, 'small')")
        .await
        .expect("insert oversize-then-small (C)");
    let too_many = c.query_one::<OvRowsQuery>(()).await;
    assert!(
        matches!(too_many, Err(DriverError::TooManyRows)),
        "oversize first + small second must be TooManyRows, got {too_many:?}",
    );
    assert_eq!(
        c.query_one::<OneQuery>(()).await.expect("probe after C drain").n,
        1,
        "connection drained healthy after the oversize-first too-many break",
    );

    // Scenario D — query_one TOO-MANY, small FIRST then oversize: the small row
    // sets seen_first (Row arm), then the second row's FIRST RowChunk trips the
    // RowChunk-arm break MID-oversize-frame (the otherwise-unwitnessed branch).
    // The mid-frame drain must still reach a clean idle.
    c.execute_sql("TRUNCATE ov_rows").await.expect("truncate D");
    c.execute_sql("INSERT INTO ov_rows (k, body) VALUES (1, 'small'), (2, repeat('x', 5000))")
        .await
        .expect("insert small-then-oversize (D)");
    let too_many = c.query_one::<OvRowsQuery>(()).await;
    assert!(
        matches!(too_many, Err(DriverError::TooManyRows)),
        "small first + oversize second must be TooManyRows, got {too_many:?}",
    );
    assert_eq!(
        c.query_one::<OneQuery>(()).await.expect("probe after D drain").n,
        1,
        "connection drained healthy after the mid-oversize-frame too-many break",
    );

    c.execute_sql("DROP TABLE ov_rows").await.expect("cleanup");
    c.close().await.expect("close");
}

/// WITNESS (review MAJOR 3 — the compile-checked FLAGSHIP is covered by
/// slow-query detection): a slow typed `query!(…)` emits `DiagEvent::SlowQuery`
/// carrying the typed query's SQL text (never the params — no PII). The gap the
/// review flagged: the guard was installed only on the dynamic verbs.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_slow_query_emits_slow_query() {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bsql_postgres_async::{DiagEvent, Diagnostics};

    let slow: Arc<Mutex<Vec<(String, Duration)>>> = Arc::new(Mutex::new(Vec::new()));
    let slow_in = Arc::clone(&slow);
    let diag = Diagnostics::new()
        .slow_query_threshold(Duration::from_millis(50))
        .on_event(move |ev: &DiagEvent<'_>| {
            if let DiagEvent::SlowQuery { sql, elapsed } = ev {
                slow_in.lock().expect("lock").push(((*sql).to_string(), *elapsed));
            }
        });
    let mut c = Connection::connect_with(&async_config(), &diag)
        .await
        .expect("connect_with");

    let rows = c.query::<SlowSleepQuery>(()).await.expect("slow typed query");
    assert_eq!(rows.len(), 1);

    let got = slow.lock().expect("lock").clone();
    assert_eq!(got.len(), 1, "the slow typed query! emitted SlowQuery once, got {got:?}");
    assert!(got[0].0.contains("pg_sleep"), "the event carries the typed SQL, got {:?}", got[0].0);
    assert!(got[0].1 >= Duration::from_millis(50), "elapsed >= threshold, got {:?}", got[0].1);
}
