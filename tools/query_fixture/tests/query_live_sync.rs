//! LIVE `query!` round-trip over the SYNC driver — the named S18 gate.
//!
//! Proves the WHOLE typed pipeline end-to-end against a real PostgreSQL:
//! `query!` (schema-validated at build) -> `TypedQuery` -> `query_params`
//! (Parse+Bind+Execute+Sync over the const wire artifact) -> the `Rows<Q>`
//! prebuffer -> typed decode back to the macro's records.
//!
//! Every query is a LITERAL `SELECT` needing no table, so it validates trivially
//! against the migration catalog and needs no live schema setup. A connection
//! Parses each content-addressed statement ONCE and reuses the server-side plan
//! on every later call (the per-connection statement cache), so the same carrier
//! can run any number of times on one connection — proven below by a loop, a
//! `pg_prepared_statements` count, and a pooled-connection reuse loop.
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

use core::ops::ControlFlow;

use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, Pool, SslMode};

// One column, fixed-width, NOT NULL -> the borrowed record carries no lifetime
// (`One { n: i32 }`) and decodes through the vectorized fast path.
bsql_query_macros::query!(One, "SELECT 1::int4 AS n");
// A distinct literal (distinct SQL -> distinct content address) — a second shape
// for `query_one`.
bsql_query_macros::query!(Seven, "SELECT 7::int4 AS n");
// One TEXT column, NOT NULL -> the borrowed record carries `<'q>` and `s` aliases
// the prebuffer (`Hi<'q> { s: &'q str }`).
bsql_query_macros::query!(Hi, "SELECT 'hello'::text AS s");
// Multi-row via a `VALUES` derived table (no real table needed). The `int4` cast
// on the first row types the column; `n` is NOT NULL.
bsql_query_macros::query!(Nums, "SELECT n FROM (VALUES (10::int4), (20), (30)) AS t(n)");
// Zero rows (a literal SELECT filtered out) -> `query_one` must classify
// `NoRows`.
bsql_query_macros::query!(NoneRow, "SELECT 1::int4 AS n WHERE false");
// Multi-row again (distinct SQL) -> `query_one` must classify `TooManyRows`.
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
// A distinct literal for the repeat / plan-reuse probes.
bsql_query_macros::query!(RepeatLit, "SELECT 100::int4 AS n");
// Distinct literals for the transactional 42P05-gone probes.
bsql_query_macros::query!(TxLit, "SELECT 11::int4 AS n");
bsql_query_macros::query!(MultiTxLit, "SELECT 22::int4 AS n");
bsql_query_macros::query!(HealLit, "SELECT 33::int4 AS n");
// A five-row VALUES stream for the `query_each` streaming / early-break probes.
bsql_query_macros::query!(
    StreamAll,
    "SELECT n FROM (VALUES (1::int4), (2), (3), (4), (5)) AS t(n)"
);
// A distinct five-row stream for the transaction probe (distinct SQL -> distinct
// content-addressed statement, so it does not collide with StreamAll).
bsql_query_macros::query!(
    StreamTx,
    "SELECT n FROM (VALUES (10::int4), (20), (30), (40), (50)) AS t(n)"
);
// A param-filtered stream: `$1` caps the rows returned, exercising `query_each`
// with a bound parameter.
bsql_query_macros::query!(
    StreamParam,
    "SELECT n FROM (VALUES (1::int4), (2), (3), (4), (5)) AS t(n) WHERE n <= $1::int4"
);

// ── widened types: float4 / float8 / bytea ──────────────────────────────
// Two fixed-width floats, NOT NULL -> the const-offset fast path; `1.5`/`2.5`
// are exact in IEEE-754 so `==` is an exact comparison.
bsql_query_macros::query!(Fl, "SELECT 1.5::float8 AS x, 2.5::float4 AS y");
// A NULL float -> the record field is `Option<f64>`, decoding to `None`.
bsql_query_macros::query!(NullFloat, "SELECT NULL::float8 AS x");
// A `bytea` literal -> borrowed `&'q [u8]` (aliases the prebuffer) / owned
// `Vec<u8>` (copies), mirroring `text`.
bsql_query_macros::query!(Bytes, r"SELECT '\xDEADBEEF'::bytea AS b");
// A float8 param -> the `(f64,)` binary-bind path end-to-end.
bsql_query_macros::query!(EchoF, "SELECT $1::float8 AS x");
// A bytea param -> the `(&[u8],)` binary-bind path end-to-end.
bsql_query_macros::query!(EchoB, "SELECT $1::bytea AS b");
// A mixed row: fixed int + fixed floats + variable bytea -> the presence of a
// variable column disables the all-fixed fast path, so the WHOLE row decodes on
// the per-cell path (proving the fast/per-cell split handles the mix).
bsql_query_macros::query!(
    Mixed,
    r"SELECT 7::int4 AS i, 2.5::float4 AS f, 8.5::float8 AS g, '\x0102'::bytea AS b"
);

// ── widened ARRAY params: `col = ANY($1)` sends a one-dimensional array over
//    the wire, so PG must accept the `array_send` bytes (element-OID header +
//    per-element length-prefix) that `encode_array_1d` writes. VALUES-derived
//    so no migration is needed; the element type differs per query, exercising
//    a distinct array element OID + element width each time.
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

fn sync_config() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// (a) The minimal no-schema case: `SELECT 1::int4 AS n` round-trips to a typed
/// record `One { n: 1 }`, and `query_one` yields the owned twin. Proves the
/// whole pipeline with zero schema setup.
#[test]
#[ignore = "requires local PG"]
fn typed_literal_select_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let rows = c.query::<OneQuery>(()).expect("query One");
    assert_eq!(rows.len(), 1, "exactly one row");
    let rec = rows
        .iter()
        .next()
        .expect("one row present")
        .expect("row decodes");
    assert_eq!(rec.n, 1, "SELECT 1::int4 must decode to n == 1");

    // `query_one` returns the OWNED twin (outlives the buffer). Distinct
    // carrier (`Seven`) so its statement name does not collide with `One`.
    let owned = c.query_one::<SevenQuery>(()).expect("query_one Seven");
    assert_eq!(owned.n, 7, "SELECT 7::int4 must decode to n == 7");

    c.close().expect("close");
}

/// (b) A TEXT column proves the borrowed record aliases the prebuffer (zero-copy
/// `&str`).
#[test]
#[ignore = "requires local PG"]
fn typed_text_column_borrows_zero_copy() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let rows = c.query::<HiQuery>(()).expect("query Hi");
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

    let rows = c.query::<NumsQuery>(()).expect("query Nums");
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

/// `query_one` is EXACTLY-one: zero rows -> `NoRows`, more than one ->
/// `TooManyRows` (never a silently-taken first row).
#[test]
#[ignore = "requires local PG"]
fn query_one_classifies_zero_and_many() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let none = c.query_one::<NoneRowQuery>(());
    assert!(
        matches!(none, Err(DriverError::NoRows)),
        "zero rows must be NoRows, got {none:?}"
    );

    let many = c.query_one::<ManyQuery>(());
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
        .query_one::<EchoQuery>((42,))
        .expect("query_one Echo(42)");
    assert_eq!(n.n, 42, "int4 param 42 must round-trip");

    // A `&'static str` literal binds through the text-param path.
    let s = c
        .query_one::<EchoSQuery>(("hi",))
        .expect("query_one EchoS(\"hi\")");
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
        .query_one::<WithNullQuery>(())
        .expect("query_one WithNull");
    assert_eq!(only_null.n, None, "NULL::int4 must decode to None");

    // A VALUES column with a NULL row carries Some(7) then None.
    let rows = c.query::<MaybeNumQuery>(()).expect("query MaybeNum");
    let vals: Vec<Option<i32>> = rows.iter().map(|r| r.expect("decodes").n).collect();
    assert_eq!(vals, vec![Some(7), None], "Some(value) then None on an Option column");

    c.close().expect("close");
}

/// The 42P05-is-gone proof: the SAME `query!` carrier run in a loop on ONE
/// connection SUCCEEDS every time. Before the per-connection statement cache this
/// failed on call #2 with `duplicate_prepared_statement` (42P05); now the Parse
/// happens once and calls 2..N reuse the server-side plan.
#[test]
#[ignore = "requires local PG"]
fn same_carrier_loops_without_42p05() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    for i in 0..100 {
        let r = c
            .query::<RepeatLitQuery>(())
            .unwrap_or_else(|e| panic!("iteration {i} must succeed, got {e:?}"));
        assert_eq!(r.len(), 1, "iteration {i}: one row");
        let rec = r.iter().next().expect("row").expect("decodes");
        assert_eq!(rec.n, 100, "iteration {i}: n == 100");
    }
    assert!(c.is_healthy(), "connection healthy after 100 reuse runs");
    c.close().expect("close");
}

/// PLAN-REUSE OBSERVABLE: after running the same carrier several times on one
/// connection, the server holds EXACTLY ONE prepared statement — proof the Parse
/// happened once. A fresh connection starts with zero prepared statements (the
/// connect handshake + `SHOW server_version` use only simple queries), so the
/// single entry is this carrier's content-addressed statement.
#[test]
#[ignore = "requires local PG"]
fn plan_is_parsed_once_and_persists() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    for _ in 0..5 {
        assert_eq!(c.query::<RepeatLitQuery>(()).expect("run").len(), 1);
    }
    let result = c
        .query_sql("SELECT count(*)::int4 FROM pg_prepared_statements")
        .expect("count prepared statements");
    let count = result
        .rows
        .first()
        .expect("count row")
        .get_i32(0)
        .expect("count value");
    assert_eq!(
        count, 1,
        "the query! must be Parsed exactly once and persist for the session"
    );
    c.close().expect("close");
}

/// THE RESET-vs-STATEMENT-CACHE CONSISTENCY PROOF: with `max_size = 1` every
/// checkout reuses the SAME physical connection. The pool RESETS a reused
/// connection on acquire, but the reset is TARGETED — it keeps prepared
/// statements — so the per-connection statement cache stays consistent with the
/// server's prepared-statement set. Looping checkout -> reset -> same query! ->
/// return must stay green (no 42P05 duplicate-prepare, no Bind-to-missing) on one
/// stable backend pid, and the server must hold the statement EXACTLY ONCE the
/// whole time (parsed once, survives every reset).
#[test]
#[ignore = "requires local PG"]
fn pooled_connection_reset_keeps_parsed_plan() {
    let pool = Pool::new(sync_config(), 1);
    let mut pid: Option<i32> = None;
    for i in 0..20 {
        let mut c = pool.get().unwrap_or_else(|e| panic!("checkout {i}: {e:?}"));
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
            .unwrap_or_else(|e| panic!("checkout {i}: {e:?}"));
        assert_eq!(r.len(), 1, "checkout {i}: one row");
        // The targeted reset KEEPS statements: the server holds it exactly once,
        // never zero (dropped) and never more than one (re-Parsed under a new name).
        let count = c
            .conn_mut()
            .expect("live")
            .query_sql("SELECT count(*)::int4 FROM pg_prepared_statements")
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
#[test]
#[ignore = "requires local PG"]
fn pooled_reset_rolls_back_open_tx_and_keeps_plan() {
    let pool = Pool::new(sync_config(), 1);
    let pid = {
        let mut c = pool.get().expect("get1");
        let conn = c.conn_mut().expect("live1");
        let pid = conn.backend_pid();
        // Cache the statement durably (autocommit), THEN open a transaction and
        // leave it open, so the connection is returned with tx_status = 'T'.
        assert_eq!(conn.query::<RepeatLitQuery>(()).expect("first use caches").len(), 1);
        conn.begin().expect("begin (leaves the tx open)");
        pid
    }; // dropped mid-transaction -> returned to the pool with an OPEN transaction
    // Reacquire the SAME physical connection: the on-acquire reset takes its
    // ROLLBACK-prefixed branch (tx_status != Idle), aborting the open tx while
    // KEEPING the cached statement.
    let mut c = pool.get().expect("get2");
    let conn = c.conn_mut().expect("live2");
    assert_eq!(conn.backend_pid(), pid, "max_size=1 must reuse the same physical connection");
    // Reuse the cached statement: must succeed (the ROLLBACK-reset kept it, no 42P05).
    assert_eq!(
        conn.query::<RepeatLitQuery>(()).expect("reuse after rollback-reset").len(),
        1
    );
    // The server still holds it exactly once (kept across the ROLLBACK-prefixed reset).
    let count = conn
        .query_sql("SELECT count(*)::int4 FROM pg_prepared_statements")
        .expect("count")
        .rows
        .first()
        .expect("count row")
        .get_i32(0)
        .expect("count value");
    assert_eq!(count, 1, "the ROLLBACK-prefixed reset kept the cached plan (parsed once)");
}

/// 42P05-GONE (transactional): a `query!` first used INSIDE a committed
/// transaction, then used again at Idle, SUCCEEDS. Before Close-before-Parse this
/// was a duplicate-prepared-statement error (the in-tx Parse committed, then the
/// idle reuse re-Parsed the still-present name).
#[test]
#[ignore = "requires local PG"]
fn query_inside_committed_transaction_then_idle_succeeds() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let n = c
        .transaction(|tx| Ok(tx.query::<TxLitQuery>(())?.len()))
        .expect("transaction commits");
    assert_eq!(n, 1, "the in-transaction query! runs");
    // The SAME carrier at Idle after the commit — Close-before-Parse re-creates
    // the (still-present) statement, so this succeeds instead of a 42P05.
    let again = c
        .query::<TxLitQuery>(())
        .expect("same carrier after commit must succeed (was 42P05)");
    assert_eq!(again.len(), 1);
    c.close().expect("close");
}

/// 42P05-GONE (across transactions): the SAME `query!` used inside MANY sequential
/// transactions all SUCCEED. Before the fix, transaction #2 failed with 42P05.
#[test]
#[ignore = "requires local PG"]
fn same_carrier_across_many_transactions_succeeds() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    for i in 0..20 {
        let n = c
            .transaction(|tx| Ok(tx.query::<MultiTxLitQuery>(())?.len()))
            .unwrap_or_else(|e| panic!("transaction {i} must commit, got {e:?}"));
        assert_eq!(n, 1, "transaction {i}: one row");
    }
    c.close().expect("close");
}

/// STREAMING: `query_each` hands every row to the closure and returns `Ok(None)`
/// when the result is fully streamed (exhausted). Constant memory: nothing is
/// accumulated by the driver — the test itself collects, but the connection path
/// buffers no rows.
#[test]
#[ignore = "requires local PG"]
fn query_each_streams_all_returns_none() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let mut collected = Vec::new();
    let done = c
        .query_each::<StreamAllQuery, _, _>((), |rec| {
            collected.push(rec.n);
            ControlFlow::<()>::Continue(())
        })
        .expect("query_each streams");
    assert_eq!(done, None, "a fully-streamed result returns Ok(None)");
    assert_eq!(collected, vec![1, 2, 3, 4, 5], "every row, in order");
    c.close().expect("close");
}

/// EARLY BREAK + DRAIN RECLAIM: `on_row` returns `Break` at row 3 of 5. The call
/// returns `Ok(Some(..))`, the connection is DRAINED back to a clean idle (stays
/// healthy), and a FOLLOW-UP query on the SAME connection succeeds — proving the
/// drain left the connection reusable.
#[test]
#[ignore = "requires local PG"]
fn query_each_break_early_drains_and_reuses() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
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
        .expect("query_each with an early break");
    assert_eq!(stopped, Some(3), "the break payload rides Ok(Some(..))");
    assert_eq!(collected, vec![1, 2, 3], "rows up to and including the break");
    assert!(
        c.is_healthy(),
        "an early break leaves the connection healthy (drained to a clean idle)"
    );
    // The drain left the connection clean: a follow-up typed query works.
    let owned = c
        .query_one::<OneQuery>(())
        .expect("follow-up query on the reused connection");
    assert_eq!(owned.n, 1, "the reused connection returns correct data");
    c.close().expect("close");
}

/// TRANSACTION + REPEAT: `query_each` runs INSIDE a transaction and is repeated
/// (the same carrier, several transactions) — the statement cache's
/// Close-before-Parse makes every run succeed with no `duplicate_prepared_statement`
/// (42P05).
#[test]
#[ignore = "requires local PG"]
fn query_each_inside_transaction_and_repeated() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    for i in 0..5 {
        let sum = c
            .transaction(|tx| {
                let mut total = 0i64;
                tx.query_each::<StreamTxQuery, _, _>((), |rec| {
                    total += i64::from(rec.n);
                    ControlFlow::<()>::Continue(())
                })?;
                Ok(total)
            })
            .unwrap_or_else(|e| panic!("transaction {i} must commit, got {e:?}"));
        assert_eq!(sum, 150, "transaction {i}: 10+20+30+40+50");
    }
    assert!(c.is_healthy(), "connection healthy after repeated in-tx streams");
    c.close().expect("close");
}

/// PARAM: `query_each` binds `$1` (the cap) and streams the filtered rows.
#[test]
#[ignore = "requires local PG"]
fn query_each_with_param_streams_filtered() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let mut collected = Vec::new();
    let done = c
        .query_each::<StreamParamQuery, _, _>((3,), |rec| {
            collected.push(rec.n);
            ControlFlow::<()>::Continue(())
        })
        .expect("query_each with a bound param");
    assert_eq!(done, None, "fully streamed");
    assert_eq!(collected, vec![1, 2, 3], "only rows where n <= $1 (=3)");
    c.close().expect("close");
}

/// WIDENING (float4/float8): two fixed-width float columns round-trip through the
/// const-offset fast path; the exactly-representable literals compare with `==`.
#[test]
#[ignore = "requires local PG"]
fn typed_float_columns_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let owned = c.query_one::<FlQuery>(()).expect("query_one Fl");
    assert_eq!(owned.x, 1.5_f64, "float8 1.5 must round-trip exactly");
    assert_eq!(owned.y, 2.5_f32, "float4 2.5 must round-trip exactly");
    c.close().expect("close");
}

/// WIDENING (nullable float): `NULL::float8` types `Option<f64>` and decodes None.
#[test]
#[ignore = "requires local PG"]
fn typed_nullable_float_decodes_none() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let owned = c
        .query_one::<NullFloatQuery>(())
        .expect("query_one NullFloat");
    assert_eq!(owned.x, None, "NULL::float8 must decode to None");
    c.close().expect("close");
}

/// WIDENING (bytea): a `bytea` column decodes to the exact bytes both borrowed
/// (`&[u8]`, zero-copy from the prebuffer) and owned (`Vec<u8>`, copied).
#[test]
#[ignore = "requires local PG"]
fn typed_bytea_column_borrowed_and_owned() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let rows = c.query::<BytesQuery>(()).expect("query Bytes");
    let rec = rows.iter().next().expect("one row").expect("row decodes");
    assert_eq!(
        rec.b,
        &[0xDE, 0xAD, 0xBE, 0xEF],
        "borrowed &[u8] aliases the 4 payload bytes"
    );

    let owned = c.query_one::<BytesQuery>(()).expect("query_one Bytes");
    assert_eq!(
        owned.b,
        vec![0xDE, 0xAD, 0xBE, 0xEF],
        "owned Vec<u8> copies the 4 bytes"
    );
    c.close().expect("close");
}

/// WIDENING (params): a `float8` value and a `&[u8]` value bind through the
/// binary `ParamsWriter` path and echo back exactly.
#[test]
#[ignore = "requires local PG"]
fn typed_float_and_bytea_params_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let f = c
        .query_one::<EchoFQuery>((1.25_f64,))
        .expect("query_one EchoF(1.25)");
    assert_eq!(f.x, 1.25_f64, "float8 param 1.25 must round-trip");

    let b = c
        .query_one::<EchoBQuery>((&[1u8, 2, 3][..],))
        .expect("query_one EchoB([1,2,3])");
    assert_eq!(b.b, vec![1u8, 2, 3], "bytea param [1,2,3] must round-trip");
    c.close().expect("close");
}

/// WIDENING (mixed row): int + float4 + float8 + bytea in one row decode
/// correctly on the per-cell path (a variable column present).
#[test]
#[ignore = "requires local PG"]
fn typed_mixed_fixed_and_variable_row() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let rows = c.query::<MixedQuery>(()).expect("query Mixed");
    let rec = rows.iter().next().expect("one row").expect("row decodes");
    assert_eq!(rec.i, 7, "int column");
    assert_eq!(rec.f, 2.5_f32, "float4 column");
    assert_eq!(rec.g, 8.5_f64, "float8 column");
    assert_eq!(rec.b, &[0x01, 0x02], "bytea column");
    c.close().expect("close");
}

/// WIDENING ARRAY (float8[]): `x = ANY($1)` binds a `&[f64]` — the primary
/// proof that the `float8[]` `array_send` bytes reach real PG byte-correct (not
/// just offline-composed). VALUES+ANY row order is unspecified, so the result is
/// sorted before the exact compare.
#[test]
#[ignore = "requires local PG"]
fn float8_array_any_bind_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let rows = c
        .query::<FloatAnyQuery>((&[1.5_f64, 3.5][..],))
        .expect("query FloatAny");
    let mut got: Vec<f64> = rows.iter().map(|r| r.expect("row decodes").x).collect();
    got.sort_by(f64::total_cmp);
    assert_eq!(got, vec![1.5_f64, 3.5], "float8[] ANY($1) returns the matching rows");
    c.close().expect("close");
}

/// WIDENING ARRAY (float4[]): a distinct element OID (700) and 4-byte element
/// width from `float8[]` — a separate live proof of the `float4[]` header.
#[test]
#[ignore = "requires local PG"]
fn float4_array_any_bind_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let rows = c
        .query::<Float4AnyQuery>((&[2.5_f32, 3.5][..],))
        .expect("query Float4Any");
    let mut got: Vec<f32> = rows.iter().map(|r| r.expect("row decodes").x).collect();
    got.sort_by(f32::total_cmp);
    assert_eq!(got, vec![2.5_f32, 3.5], "float4[] ANY($1) returns the matching rows");
    c.close().expect("close");
}

/// WIDENING ARRAY (int8[]): the `col = ANY($1)` pattern was OFFLINE-only until
/// now (byte-golden `query_any_bind`); this exercises it live.
#[test]
#[ignore = "requires local PG"]
fn int8_array_any_bind_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let rows = c
        .query::<IntAnyQuery>((&[10i64, 30][..],))
        .expect("query IntAny");
    let mut got: Vec<i64> = rows.iter().map(|r| r.expect("row decodes").n).collect();
    got.sort_unstable();
    assert_eq!(got, vec![10i64, 30], "int8[] ANY($1) returns the matching rows");
    c.close().expect("close");
}

/// WIDENING ARRAY (bytea[]): the variable-length element shape (`&[&[u8]]`) —
/// each element is length-prefixed, unlike the fixed-width float/int arrays.
#[test]
#[ignore = "requires local PG"]
fn bytea_array_any_bind_round_trip() {
    const BYTEA_ARG: &[&[u8]] = &[b"\x01", b"\x03"];
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let rows = c
        .query::<ByteaAnyQuery>((BYTEA_ARG,))
        .expect("query ByteaAny");
    let mut got: Vec<Vec<u8>> = rows.iter().map(|r| r.expect("row decodes").b.to_vec()).collect();
    got.sort_unstable();
    assert_eq!(
        got,
        vec![vec![0x01u8], vec![0x03u8]],
        "bytea[] ANY($1) returns the matching rows"
    );
    c.close().expect("close");
}

/// DISCARD ALL self-heal: after a recorded statement is dropped out of band, the
/// next reuse errors ONCE (loud, classified) and the connection stays healthy;
/// the call after that re-creates the statement (cache MISS -> Close+Parse) and
/// succeeds — a self-heal, never a persistent poison.
#[test]
#[ignore = "requires local PG"]
fn discard_all_then_reuse_errors_once_then_self_heals() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    // Record the statement (autocommit MISS completes at Idle -> recorded).
    assert_eq!(c.query::<HealLitQuery>(()).expect("first use records").len(), 1);
    // Drop ALL prepared statements out of band (a non-row command).
    c.execute_sql("DISCARD ALL").expect("discard all");
    // The next reuse hits the now-missing statement: ONE loud classified error.
    let poisoned = c.query::<HealLitQuery>(());
    assert!(
        matches!(poisoned, Err(DriverError::Db(_))),
        "reuse over a dropped statement must be a loud Db error, got {poisoned:?}"
    );
    assert!(c.is_healthy(), "connection stays healthy (recoverable error)");
    // The call AFTER that is a MISS (the name was evicted) -> re-created -> works.
    let healed = c
        .query::<HealLitQuery>(())
        .expect("self-heal: the next use re-creates the statement and succeeds");
    assert_eq!(healed.len(), 1);
    c.close().expect("close");
}
