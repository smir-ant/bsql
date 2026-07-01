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

/// POOL REUSE + invalidation correctness: `max_size = 1` forces every checkout to
/// reuse the SAME physical connection (the pool does NOT reset on return, so the
/// statement cache persists with it). Looping checkout -> same query! -> return
/// stays green on one stable backend pid — no stale-cache Bind-to-missing.
#[tokio::test]
#[ignore = "requires local PG"]
async fn pooled_connection_reuses_parsed_plan() {
    let pool = Pool::new(async_config(), 1).await.expect("pool");
    let mut pid: Option<i32> = None;
    for i in 0..20 {
        let mut c = pool.get().await.unwrap_or_else(|e| panic!("checkout {i}: {e:?}"));
        let this_pid = c.backend_pid();
        match pid {
            None => pid = Some(this_pid),
            Some(p) => {
                assert_eq!(p, this_pid, "checkout {i} must reuse the one physical connection")
            }
        }
        let r = c
            .query::<RepeatLitQuery>(())
            .await
            .unwrap_or_else(|e| panic!("checkout {i}: {e:?}"));
        assert_eq!(r.len(), 1, "checkout {i}: one row");
        // `c` drops here -> returned to the pool (no reset), cache persists.
    }
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
