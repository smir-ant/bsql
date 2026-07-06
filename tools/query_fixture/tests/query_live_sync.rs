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
use core::str::FromStr as _;

use bsql::{Date, Interval, Jsonb, Numeric, Time, Timestamptz, Uuid};
use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, Pool, SslMode};

// One column, fixed-width, NOT NULL -> the borrowed record carries no lifetime
// (`One { n: i32 }`) and decodes through the vectorized fast path.
bsql::query!(One, "SELECT 1::int4 AS n");
// A distinct literal (distinct SQL -> distinct content address) — a second shape
// for `query_one`.
bsql::query!(Seven, "SELECT 7::int4 AS n");
// One TEXT column, NOT NULL -> the borrowed record carries `<'q>` and `s` aliases
// the prebuffer (`Hi<'q> { s: &'q str }`).
bsql::query!(Hi, "SELECT 'hello'::text AS s");
// Multi-row via a `VALUES` derived table (no real table needed). The `int4` cast
// on the first row types the column; `n` is NOT NULL.
bsql::query!(Nums, "SELECT n FROM (VALUES (10::int4), (20), (30)) AS t(n)");
// Zero rows (a literal SELECT filtered out) -> `query_one` must classify
// `NoRows`.
bsql::query!(NoneRow, "SELECT 1::int4 AS n WHERE false");
// Multi-row again (distinct SQL) -> `query_one` must classify `TooManyRows`.
bsql::query!(Many, "SELECT n FROM (VALUES (1::int4), (2)) AS t(n)");
// An INT param -> exercises the `(i32,)` binary-bind path end-to-end.
bsql::query!(Echo, "SELECT $1::int4 AS n");
// A TEXT param -> exercises the `&str` binary-bind path end-to-end.
bsql::query!(EchoS, "SELECT $1::text AS s");
// A NULL cast -> the inference engine types it nullable, so the record field is
// `Option<i32>`; it must decode to `None`.
bsql::query!(WithNull, "SELECT NULL::int4 AS n");
// A `VALUES` column with a NULL row -> nullable `Option<i32>`, carrying BOTH a
// present value (`Some`) and a NULL (`None`) in one result.
bsql::query!(MaybeNum, "SELECT n FROM (VALUES (7::int4), (NULL)) AS t(n)");
// A distinct literal for the repeat / plan-reuse probes.
bsql::query!(RepeatLit, "SELECT 100::int4 AS n");
// Distinct literals for the transactional 42P05-gone probes.
bsql::query!(TxLit, "SELECT 11::int4 AS n");
bsql::query!(MultiTxLit, "SELECT 22::int4 AS n");
bsql::query!(HealLit, "SELECT 33::int4 AS n");
// A five-row VALUES stream for the `query_each` streaming / early-break probes.
bsql::query!(
    StreamAll,
    "SELECT n FROM (VALUES (1::int4), (2), (3), (4), (5)) AS t(n)"
);
// A distinct five-row stream for the transaction probe (distinct SQL -> distinct
// content-addressed statement, so it does not collide with StreamAll).
bsql::query!(
    StreamTx,
    "SELECT n FROM (VALUES (10::int4), (20), (30), (40), (50)) AS t(n)"
);
// A param-filtered stream: `$1` caps the rows returned, exercising `query_each`
// with a bound parameter.
bsql::query!(
    StreamParam,
    "SELECT n FROM (VALUES (1::int4), (2), (3), (4), (5)) AS t(n) WHERE n <= $1::int4"
);

// ── widened types: float4 / float8 / bytea ──────────────────────────────
// Two fixed-width floats, NOT NULL -> the const-offset fast path; `1.5`/`2.5`
// are exact in IEEE-754 so `==` is an exact comparison.
bsql::query!(Fl, "SELECT 1.5::float8 AS x, 2.5::float4 AS y");
// A NULL float -> the record field is `Option<f64>`, decoding to `None`.
bsql::query!(NullFloat, "SELECT NULL::float8 AS x");
// A `bytea` literal -> borrowed `&'q [u8]` (aliases the prebuffer) / owned
// `Vec<u8>` (copies), mirroring `text`.
bsql::query!(Bytes, r"SELECT '\xDEADBEEF'::bytea AS b");
// A float8 param -> the `(f64,)` binary-bind path end-to-end.
bsql::query!(EchoF, "SELECT $1::float8 AS x");
// A bytea param -> the `(&[u8],)` binary-bind path end-to-end.
bsql::query!(EchoB, "SELECT $1::bytea AS b");
// A mixed row: fixed int + fixed floats + variable bytea -> the presence of a
// variable column disables the all-fixed fast path, so the WHOLE row decodes on
// the per-cell path (proving the fast/per-cell split handles the mix).
bsql::query!(
    Mixed,
    r"SELECT 7::int4 AS i, 2.5::float4 AS f, 8.5::float8 AS g, '\x0102'::bytea AS b"
);

// ── widened ARRAY params: `col = ANY($1)` sends a one-dimensional array over
//    the wire, so PG must accept the `array_send` bytes (element-OID header +
//    per-element length-prefix) that `encode_array_1d` writes. VALUES-derived
//    so no migration is needed; the element type differs per query, exercising
//    a distinct array element OID + element width each time.
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

// ── BIG-PARAMETER witness (flagship typed path; see the async twin). The reply
//    is a single int, so the reply row stays small — this isolates the SEND-side
//    Bind cap (B1).
bsql::query!(BigByteaLen, "SELECT length($1::bytea)::int4 AS n");

// ── widened bsql-native types: uuid / timestamptz / timestamp ─────────────
// Literal casts (no schema): each proves PG's binary wire for that type
// materialises into the dep-free bsql-native record field.
bsql::query!(
    UuidLit,
    "SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid AS u"
);
bsql::query!(TsLit, "SELECT '2000-01-01 00:00:01+00'::timestamptz AS t");
bsql::query!(TsNaiveLit, "SELECT '2000-01-01 00:00:02'::timestamp AS t");
// Param round-trips: a `bsql::Uuid` / `bsql::Timestamptz` binds through the
// binary `ParamsWriter` path and echoes back.
bsql::query!(EchoUuid, "SELECT $1::uuid AS u");
bsql::query!(EchoTs, "SELECT $1::timestamptz AS t");
// json / jsonb literal columns — `jsonb` proves the leading version byte is
// stripped byte-correct. (A json/jsonb PARAM is out of scope this slice: the
// `TypedQuery::Params: Copy` bound rejects a `String`-backed param tuple; a
// by-reference param path is a deferred follow-up.)
bsql::query!(JsonLit, "SELECT '{\"k\":1}'::json AS j");
bsql::query!(JsonbLit, "SELECT '[1,2,3]'::jsonb AS j");

// ── 1-D array result columns: `int4[]` / `text[]` decode to
//    `Vec<Option<T>>` (with an honest NULL element), and a NULL WHOLE array
//    (a NULL cast, typed nullable) to `Option<Vec<Option<T>>> == None`.
//    Literal casts, so no migration / live schema is needed.
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

// ── exact numeric / decimal: a FromStr-constructed `bsql::Numeric` binds as a
//    param, and `$1::numeric::text` is PG's own text rendering (the oracle).
bsql::query!(EchoNum, "SELECT $1::numeric AS n");
bsql::query!(EchoNumText, "SELECT $1::numeric::text AS t");
bsql::query!(NumArrayLit, "SELECT '{1.5,NULL,100}'::numeric[] AS xs");

// ── temporal family: date / time / interval bind as params and echo back, and
//    `$1::TYPE::text` is PG's own text rendering (the oracle). Array RESULT
//    columns are literal casts; the array ENCODE path rides `= ANY($1)`.
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

// See the async twin: a USING-merged column drawn from an outer-join-promoted
// (nullable) side. `bk` is NOT NULL on every base table, yet the merged key CAN
// be NULL, so the soundness fix types it `Option<i32>` (pre-fix: a non-Option
// `i32` a real NULL would crash on decode).
bsql::query!(
    OuterUsingNull,
    "SELECT bk FROM oj_a LEFT JOIN oj_b ON oj_a.j = oj_b.j \
     LEFT JOIN oj_c USING (bk) ORDER BY oj_a.j"
);

// ── INBOUND OVERSIZE-ROW witness (see the async twin for the full rationale): a
//    RESULT row WIDER than the engine's inline read buffer (READ_BUF_CAP = 4096)
//    streams from PG as `RowChunk` pieces the typed path now REASSEMBLES and
//    decodes identically to an inline row — at base each was a hard
//    `DriverError::OversizeRow`. Function-derived columns type nullable.
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
// `body` is TEXT NOT NULL, so it decodes NON-nullable; `k` pins the order.
bsql::query!(OvRows, "SELECT body FROM ov_rows ORDER BY k");

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

/// `query_opt` is AT-MOST-one: zero rows -> `Ok(None)`, exactly one ->
/// `Ok(Some(record))`, more than one -> `TooManyRows` (same precedence as
/// `query_one`, only the zero-row outcome differs).
#[test]
#[ignore = "requires local PG"]
fn query_opt_classifies_zero_one_and_many() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // Zero rows -> Ok(None) (NOT NoRows — the whole point of the opt shape).
    let none = c.query_opt::<NoneRowQuery>(()).expect("query_opt runs");
    assert!(none.is_none(), "zero rows must be Ok(None), got {none:?}");

    // Exactly one row -> Ok(Some(owned record)).
    let one = c.query_opt::<OneQuery>(()).expect("query_opt runs");
    assert_eq!(one.expect("one row present").n, 1, "the single row decodes");

    // Two rows -> TooManyRows (loud, same as query_one — never a silent first row).
    let many = c.query_opt::<ManyQuery>(());
    assert!(
        matches!(many, Err(DriverError::TooManyRows)),
        "two rows must be TooManyRows, got {many:?}"
    );

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
/// connect handshake issues no query — `server_version` is captured from the
/// handshake `ParameterStatus`, not fetched), so the single entry is this
/// carrier's content-addressed statement.
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
        .get(0)
        .expect("count row")
        .get_i32(0).expect("count decodes").expect("count value");
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
        .get(0)
        .expect("count row")
        .get_i32(0).expect("count decodes").expect("count value");
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

/// BIG-PARAMETER STREAMING (sync twin): a Bind whose encoded parameters far
/// exceed the old ~2 KiB bounded-frame cap now round-trips over REAL PG (the B1
/// fix). A ~4 KiB `bytea`, a ~5 KiB `jsonb`, and a 500-element `int4[]` — each a
/// `FrameTooLong` before the Bind streamed onto the growable send buffer.
#[test]
#[ignore = "requires local PG"]
fn big_params_stream_past_the_old_bind_cap() {
    // Each parameter encodes to > 4 KiB, far past the old ~2 KiB bounded-`WriteBuf`
    // Bind cap (a `FrameTooLong` before streaming). Round-trip proven SERVER-SIDE
    // (a single-`bool` reply comparing the bound value to a server reference), so
    // the reply row stays tiny and this isolates the SEND-side Bind (B1).
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // (a) ~4 KiB bytea.
    let big_bytea = vec![0xABu8; 4096];
    let r = c
        .query_params_one(
            "SELECT ($1::bytea = decode(repeat('ab', 4096), 'hex')) AS eq",
            &(big_bytea.as_slice(),),
        )
        .expect("query_params_one 4 KiB bytea — was FrameTooLong before streaming");
    assert_eq!(r.get_bool(0), Ok(Some(true)), "4 KiB bytea param arrived byte-for-byte");

    // (b) ~5 KiB jsonb (a JSON string scalar).
    let big_json = format!("\"{}\"", "x".repeat(5000));
    let r = c
        .query_params_one(
            "SELECT ($1::jsonb = ('\"' || repeat('x', 5000) || '\"')::jsonb) AS eq",
            &(Jsonb::new(big_json),),
        )
        .expect("query_params_one ~5 KiB jsonb — was FrameTooLong before streaming");
    assert_eq!(r.get_bool(0), Ok(Some(true)), "~5 KiB jsonb param arrived");

    // (c) 500-element int4[] — the array wire is ~4 KiB.
    let big_arr = vec![7i32; 500];
    let r = c
        .query_params_one(
            "SELECT ($1::int4[] = array_fill(7, ARRAY[500])) AS eq",
            &(big_arr.as_slice(),),
        )
        .expect("query_params_one 500-elem int4[] — was FrameTooLong before streaming");
    assert_eq!(r.get_bool(0), Ok(Some(true)), "500-element int4[] param arrived");

    // (d) FLAGSHIP typed `query!` path also streams a big Bind: a 4 KiB bytea
    //     param, returning its length (a tiny reply row).
    const BIG_BYTEA: &[u8] = &[0xCDu8; 4096];
    let n = c
        .query_one::<BigByteaLenQuery>((BIG_BYTEA,))
        .expect("query_one BigByteaLen(4 KiB) — was FrameTooLong before streaming");
    assert_eq!(n.n, Some(4096), "typed query! binds a 4 KiB bytea param");

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

/// WIDENING (uuid): a `uuid` column decodes to the exact 16 bytes and
/// round-trips its canonical hyphenated hex form.
#[test]
#[ignore = "requires local PG"]
fn typed_uuid_column_round_trips() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let row = c.query_one::<UuidLitQuery>(()).expect("query_one UuidLit");
    assert_eq!(
        row.u.to_string(),
        "550e8400-e29b-41d4-a716-446655440000",
        "uuid decodes to its canonical hex form"
    );
    c.close().expect("close");
}

/// WIDENING (timestamptz / timestamp): the `i64` micro count decodes to the
/// bsql-native timestamp; `to_unix_micros` matches the known UTC instant.
#[test]
#[ignore = "requires local PG"]
fn typed_timestamp_columns_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    // 2000-01-01 00:00:01 UTC = 1 s after the PG epoch = Unix 946_684_801 s.
    let tz = c.query_one::<TsLitQuery>(()).expect("query_one TsLit");
    assert_eq!(
        tz.t.to_unix_micros(),
        Some(946_684_801_000_000),
        "timestamptz decodes to the exact UTC instant"
    );
    // Naive `timestamp` 2000-01-01 00:00:02 = 2 s after the epoch, zone-less.
    let naive = c.query_one::<TsNaiveLitQuery>(()).expect("query_one TsNaiveLit");
    assert_eq!(naive.t.as_micros(), 2_000_000, "naive timestamp is raw micros");
    c.close().expect("close");
}

/// WIDENING (params): a `bsql::Uuid` and a `bsql::Timestamptz` bind through
/// the binary `ParamsWriter` path and echo back exactly.
#[test]
#[ignore = "requires local PG"]
fn typed_uuid_and_timestamptz_params_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let u = Uuid::from_bytes([
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00,
    ]);
    let echoed = c.query_one::<EchoUuidQuery>((u,)).expect("query_one EchoUuid");
    assert_eq!(echoed.u, u, "uuid param round-trips");

    let ts = Timestamptz::from_micros(1_000_000);
    let echoed_ts = c.query_one::<EchoTsQuery>((ts,)).expect("query_one EchoTs");
    assert_eq!(echoed_ts.t, ts, "timestamptz param round-trips");
    assert_eq!(echoed_ts.t.to_unix_micros(), Some(946_684_801_000_000));
    c.close().expect("close");
}

/// WIDENING (json / jsonb): a `json` column surfaces its text verbatim; a
/// `jsonb` column decodes past the version byte. PG may re-serialise jsonb
/// with normalised spacing, so the jsonb assertions check the parsed content
/// rather than exact bytes.
#[test]
#[ignore = "requires local PG"]
fn typed_json_and_jsonb_columns_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let j = c.query_one::<JsonLitQuery>(()).expect("query_one JsonLit");
    assert_eq!(j.j.as_str(), r#"{"k":1}"#, "json text is surfaced verbatim");

    let jb = c.query_one::<JsonbLitQuery>(()).expect("query_one JsonbLit");
    // jsonb round-trips through PG's canonical spacing: `[1, 2, 3]`.
    assert_eq!(jb.j.as_str(), "[1, 2, 3]", "jsonb decodes past the version byte");
    c.close().expect("close");
}

/// PRECISION BATTERY (numeric): a WIDE range of exact decimal values each bind
/// as a `FromStr`-constructed `bsql::Numeric` param, round-trip through REAL
/// PostgreSQL, and decode back to the EXACT decimal string == the value's own
/// `Display` == PostgreSQL's own `$1::numeric::text` rendering (the oracle).
///
/// This is the load-bearing precision proof: a single wrong digit in encode or
/// decode is silently-wrong money. The `== s` assertion pins my ENCODE (my
/// bytes must mean `s` to PG); the `== oracle` assertion pins my DECODE against
/// PG's text; together they close the round-trip. Values past the `i128` range
/// prove genuine arbitrary precision.
#[test]
#[ignore = "requires local PG"]
fn typed_numeric_precision_battery() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    for s in [
        "0",
        "1",
        "-1",
        "0.1",
        "0.0001",
        "3.14159265358979323846",
        "1.500",
        "100.00",
        // > i128 (60 digits) and a negative large high-scale value — genuine
        // arbitrary precision, unrepresentable in any fixed-width mantissa.
        "123456789012345678901234567890123456789012345678901234567890",
        "-99999999999999999999999999999999999999999999.000001",
        "NaN",
    ] {
        let n = Numeric::from_str(s).expect("battery value parses");
        let echoed = c.query_one::<EchoNumQuery>((n.clone(),)).expect("echo numeric");
        let oracle = c
            .query_one::<EchoNumTextQuery>((n.clone(),))
            .expect("pg ::text oracle");
        assert_eq!(echoed.n.to_string(), s, "decode Display == expected for `{s}`");
        assert_eq!(
            echoed.n.to_string(),
            oracle.t,
            "decode Display == PG ::text for `{s}`",
        );
        assert_eq!(echoed.n, n, "decoded value equals the bound value for `{s}`");
    }
    c.close().expect("close");
}

/// PRECISION BATTERY (specials): `±Infinity` bind and round-trip exactly. The
/// infinities are PostgreSQL 14+; on an older server the bind is a loud
/// `DbError`, which this test treats as a skip (never a false pass) rather than
/// asserting an unsupported feature.
#[test]
#[ignore = "requires local PG"]
fn typed_numeric_infinity_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    for (value, text) in [
        (Numeric::infinity(), "Infinity"),
        (Numeric::neg_infinity(), "-Infinity"),
    ] {
        match c.query_one::<EchoNumQuery>((value.clone(),)) {
            Ok(echoed) => {
                assert_eq!(echoed.n, value, "{text} round-trips exactly");
                assert_eq!(echoed.n.to_string(), text);
                let oracle = c
                    .query_one::<EchoNumTextQuery>((value.clone(),))
                    .expect("pg ::text oracle");
                assert_eq!(oracle.t, text, "PG ::text renders {text}");
            }
            // Pre-14 PostgreSQL rejects numeric infinity — a loud DbError, not a
            // silent miss. Skip rather than fail on an unsupported server.
            Err(DriverError::Db(_)) => {
                c = Connection::connect(&sync_config()).expect("reconnect after skip");
            }
            Err(other) => panic!("unexpected error binding {text}: {other:?}"),
        }
    }
    c.close().expect("close");
}

/// ARRAYS (numeric): a real `numeric[]` with a NULL middle element decodes to
/// `Vec<Option<bsql::Numeric>>` with exact values and an honest `None`. The
/// server sends its own `array_send` bytes, so this proves the numeric-array
/// wire decode end-to-end against PostgreSQL.
#[test]
#[ignore = "requires local PG"]
fn typed_numeric_array_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let row = c.query_one::<NumArrayLitQuery>(()).expect("query_one NumArrayLit");
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
    c.close().expect("close");
}

/// ARRAYS: a real `int4[]` and `text[]` (each with a NULL middle element)
/// decode to `Vec<Option<T>>` with an honest `None` element; a NULL WHOLE
/// array decodes to `None`; an empty array to an empty `Vec`. The server sends
/// its real `array_send` bytes, so this proves the wire decode end-to-end
/// against PostgreSQL.
///
/// The literal `ARRAY[...]::T[]` cast is inferred NULLABLE (the conservative,
/// over-nullable direction — never silently non-null), so each column is
/// `Option<Vec<Option<T>>>`; the NOT-NULL `Vec<Option<T>>` whole-array shape is
/// covered by the `array_rows` catalog columns in `query_arrays`.
#[test]
#[ignore = "requires local PG"]
fn typed_array_columns_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // int4[] with a NULL element -> the `Vec` carries an honest `None`.
    let ints = c.query_one::<IntArrayLitQuery>(()).expect("query_one IntArrayLit");
    assert_eq!(ints.xs, Some(vec![Some(10), None, Some(30)]));

    // text[] with a NULL element -> owned `String`s with a `None`.
    let labels = c.query_one::<TextArrayLitQuery>(()).expect("query_one TextArrayLit");
    assert_eq!(
        labels.xs,
        Some(vec![Some(String::from("a")), None, Some(String::from("c"))])
    );

    // A NULL WHOLE array -> None.
    let none = c.query_one::<NullArrayLitQuery>(()).expect("query_one NullArrayLit");
    assert_eq!(none.xs, None);

    // An empty array (PG ndim = 0) -> an empty `Vec`.
    let empty = c.query_one::<EmptyArrayLitQuery>(()).expect("query_one EmptyArrayLit");
    assert_eq!(empty.xs, Some(Vec::<Option<i32>>::new()));

    c.close().expect("close");
}

/// int2 (`i16`) + `bool` — the last two of the 18 scalar types, decoded through
/// the unified `ColCellAt::decode_at` seam. The scalar pair is all-fixed-not-null
/// (const-offset FAST path); the arrays cover `int2[]` / `bool[]` (per-cell path,
/// each with a NULL element). Each column decodes to its DECLARED Rust type.
#[test]
#[ignore = "requires local PG"]
fn typed_int2_and_bool_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // FAST path: two fixed-width, NOT-NULL columns (`int2` = 2 B, `bool` = 1 B).
    let sb = c.query_one::<SmallBoolQuery>(()).expect("query_one SmallBool");
    assert_eq!(sb.a, 1_i16);
    assert!(sb.b);

    // Per-cell path: `int2[]` and `bool[]`, each with an honest `None` element.
    let arr = c
        .query_one::<SmallBoolArraysQuery>(())
        .expect("query_one SmallBoolArrays");
    assert_eq!(arr.c, Some(vec![Some(1_i16), None, Some(2_i16)]));
    assert_eq!(arr.d, Some(vec![Some(true), None, Some(false)]));

    c.close().expect("close");
}

/// PRECISION BATTERY (date): a range of calendar days — the epoch, a leap day,
/// the day before the epoch, year 1 AD, a far-future date — each bind as a
/// `FromStr`-constructed `bsql::Date` param, round-trip through REAL PostgreSQL,
/// and decode back to the EXACT ISO text == the value's own `Display` ==
/// PostgreSQL's own `$1::date::text` rendering (the oracle). A single wrong day
/// in the Gregorian conversion is a wrong calendar date, so this is the
/// load-bearing correctness proof — the `== s` assertion pins my ENCODE, the
/// `== oracle` assertion pins my DECODE.
#[test]
#[ignore = "requires local PG"]
fn typed_date_precision_battery() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    for s in ["2000-01-01", "2000-02-29", "1999-12-31", "0001-01-01", "9999-12-31"] {
        let d = Date::from_str(s).expect("date parses");
        let echoed = c.query_one::<EchoDateQuery>((d,)).expect("echo date");
        let oracle = c.query_one::<EchoDateTextQuery>((d,)).expect("pg ::text oracle");
        assert_eq!(echoed.d.to_string(), s, "decode Display == expected for `{s}`");
        assert_eq!(echoed.d.to_string(), oracle.t, "decode Display == PG ::text for `{s}`");
        assert_eq!(echoed.d, d, "decoded value equals the bound value for `{s}`");
    }
    // The ±infinity sentinels bind and round-trip exactly (date infinity is not
    // version-gated, unlike numeric).
    for (value, text) in [(Date::infinity(), "infinity"), (Date::neg_infinity(), "-infinity")] {
        let echoed = c.query_one::<EchoDateQuery>((value,)).expect("echo date infinity");
        let oracle = c.query_one::<EchoDateTextQuery>((value,)).expect("oracle");
        assert_eq!(echoed.d, value, "{text} round-trips exactly");
        assert_eq!(echoed.d.to_string(), text);
        assert_eq!(oracle.t, text, "PG ::text renders {text}");
    }
    c.close().expect("close");
}

/// PRECISION BATTERY (time): midnight, a mid-day microsecond value, and the
/// last microsecond of the day each round-trip bit-exact against the
/// `$1::time::text` oracle.
#[test]
#[ignore = "requires local PG"]
fn typed_time_precision_battery() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    for s in ["00:00:00", "12:34:56.789012", "23:59:59.999999", "01:02:03"] {
        let t = Time::from_str(s).expect("time parses");
        let echoed = c.query_one::<EchoTimeQuery>((t,)).expect("echo time");
        let oracle = c.query_one::<EchoTimeTextQuery>((t,)).expect("pg ::text oracle");
        assert_eq!(echoed.x.to_string(), s, "decode Display == expected for `{s}`");
        assert_eq!(echoed.x.to_string(), oracle.t, "decode Display == PG ::text for `{s}`");
        assert_eq!(echoed.x, t, "decoded value equals the bound value for `{s}`");
    }
    c.close().expect("close");
}

/// PRECISION BATTERY (interval): the three fields (months / days / micros) are
/// kept separate, so each value binds as an `Interval::new(..)`, round-trips
/// through REAL PostgreSQL bit-exact, and its `Display` reproduces PostgreSQL's
/// own `$1::interval::text` (the oracle) — the year/month split, per-field
/// signs, plural forms, and the time-part rule all match.
#[test]
#[ignore = "requires local PG"]
fn typed_interval_precision_battery() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
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
        let echoed = c.query_one::<EchoIntervalQuery>((value,)).expect("echo interval");
        let oracle = c.query_one::<EchoIntervalTextQuery>((value,)).expect("pg ::text oracle");
        assert_eq!(echoed.i.to_string(), text, "decode Display == expected for `{text}`");
        assert_eq!(echoed.i.to_string(), oracle.t, "decode Display == PG ::text for `{text}`");
        assert_eq!(echoed.i, value, "decoded fields equal the bound fields for `{text}`");
    }
    c.close().expect("close");
}

/// ARRAYS (temporal): a real `date[]` / `time[]` / `interval[]` (each with a
/// NULL middle element) decodes to `Vec<Option<T>>` with exact values and an
/// honest `None`. The server sends its own `array_send` bytes, so this proves
/// the temporal-array wire decode end-to-end against PostgreSQL.
#[test]
#[ignore = "requires local PG"]
fn typed_temporal_arrays_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let dates = c.query_one::<DateArrayLitQuery>(()).expect("date[]");
    let d: Vec<Option<String>> =
        dates.xs.iter().map(|e| e.as_ref().map(ToString::to_string)).collect();
    assert_eq!(
        d,
        vec![Some("2000-01-01".to_string()), None, Some("2000-02-29".to_string())]
    );

    let times = c.query_one::<TimeArrayLitQuery>(()).expect("time[]");
    let t: Vec<Option<String>> =
        times.xs.iter().map(|e| e.as_ref().map(ToString::to_string)).collect();
    assert_eq!(
        t,
        vec![Some("00:00:00".to_string()), None, Some("23:59:59.999999".to_string())]
    );

    let spans = c.query_one::<IntervalArrayLitQuery>(()).expect("interval[]");
    let i: Vec<Option<String>> =
        spans.xs.iter().map(|e| e.as_ref().map(ToString::to_string)).collect();
    assert_eq!(
        i,
        vec![Some("1 day".to_string()), None, Some("-1 days".to_string())]
    );

    c.close().expect("close");
}

/// ARRAY ENCODE (date[]): `d = ANY($1)` sends a `date[]` `array_send` frame
/// (element-OID header + per-element bodies) that `encode_array_1d` writes, so
/// PostgreSQL must accept the bytes byte-correct. VALUES+ANY row order is
/// unspecified, so the result is sorted before the compare.
#[test]
#[ignore = "requires local PG"]
fn date_array_any_bind_round_trip() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    // The array param's associated type is `&'static [Date]`; a `const` array of
    // `Date::from_days` (a const fn) is `'static`. Day 0 = 2000-01-01,
    // day 2_921_939 = 9999-12-31.
    const WANTED: [Date; 2] = [Date::from_days(0), Date::from_days(2_921_939)];
    let rows = c.query::<DateAnyQuery>((&WANTED[..],)).expect("query DateAny");
    let mut got: Vec<i32> = rows.iter().map(|r| r.expect("row decodes").d.to_days()).collect();
    got.sort_unstable();
    assert_eq!(got, vec![0, 2_921_939], "date[] ANY($1) returns the matching rows");
    c.close().expect("close");
}

/// SOUNDNESS witness (sync twin of `merged_outer_join_null_round_trips_as_none`):
/// a `USING`-merged column drawn from an outer-join-promoted side decodes a REAL
/// NULL into `None` without a decode error, over the blocking driver. Pre-fix the
/// field was typed `i32` and the first row's genuine NULL failed `Rows` decode.
#[test]
#[ignore = "requires local PG"]
fn merged_outer_join_null_round_trips_as_none() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // `execute_sql` returns an affected-row count (no `#[must_use]` row handle).
    c.execute_sql("DROP TABLE IF EXISTS oj_c, oj_b, oj_a").expect("drop");
    c.execute_sql("CREATE TABLE oj_a (j INTEGER NOT NULL, x INTEGER)").expect("a");
    c.execute_sql("CREATE TABLE oj_b (j INTEGER NOT NULL, bk INTEGER NOT NULL, y INTEGER)")
        .expect("b");
    c.execute_sql("CREATE TABLE oj_c (bk INTEGER NOT NULL, z INTEGER)").expect("c");
    c.execute_sql("INSERT INTO oj_a (j, x) VALUES (1, 100), (2, 200)").expect("ins a");
    c.execute_sql("INSERT INTO oj_b (j, bk, y) VALUES (2, 42, 7)").expect("ins b");
    c.execute_sql("INSERT INTO oj_c (bk, z) VALUES (42, 9)").expect("ins c");

    let rows = c.query::<OuterUsingNullQuery>(()).expect("query OuterUsingNull");
    let got: Vec<Option<i32>> = rows.iter().map(|r| r.expect("row decodes").bk).collect();
    assert_eq!(
        got,
        vec![None, Some(42)],
        "the outer-join×USING merged key round-trips its real NULL as None",
    );

    c.execute_sql("DROP TABLE oj_c, oj_b, oj_a").expect("cleanup");
    c.close().expect("close");
}

/// INBOUND OVERSIZE (sync): a > 4 KiB TEXT result row streams as `RowChunk`
/// pieces the typed path now REASSEMBLES — `query` (`iter` + `into_owned`) and
/// `query_one` decode the exact 5000-byte value. At base each was a hard
/// `DriverError::OversizeRow`.
#[test]
#[ignore = "requires local PG"]
fn oversize_typed_text_row_reassembles() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    // The borrowed record aliases `rows`, so it is scoped closed before
    // `into_owned` (the documented E0505 escape wall).
    let rows = c.query::<OvBigTextQuery>(()).expect("query OvBigText");
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

    let one = c.query_one::<OvBigTextQuery>(()).expect("query_one OvBigText");
    let s = one.s.expect("query_one text present");
    assert_eq!(s.len(), 5000, "query_one reassembles the oversize row");
    assert!(s.bytes().all(|b| b == b'x'));

    c.close().expect("close");
}

/// INBOUND OVERSIZE (sync): a > 4 KiB JSONB and a > 4 KiB BYTEA column each
/// reassemble and round-trip their exact value through `query_one`.
#[test]
#[ignore = "requires local PG"]
fn oversize_typed_jsonb_and_bytea_reassemble() {
    let mut c = Connection::connect(&sync_config()).expect("connect");

    let jb = c.query_one::<OvBigJsonbQuery>(()).expect("query_one OvBigJsonb");
    let j = jb.j.expect("jsonb present");
    assert_eq!(j.as_str().len(), 6002, "> 4 KiB jsonb reassembled");
    assert!(j.as_str().starts_with('"') && j.as_str().ends_with('"'));
    assert!(j.as_str()[1..6001].bytes().all(|b| b == b'z'), "jsonb payload intact");

    let bt = c.query_one::<OvBigByteaQuery>(()).expect("query_one OvBigBytea");
    let bytes = bt.b.expect("bytea present");
    assert_eq!(bytes.len(), 5000, "> 4 KiB bytea reassembled");
    assert!(bytes.iter().all(|&x| x == 0xCD), "every bytea byte survived reassembly");

    c.close().expect("close");
}

/// INBOUND OVERSIZE (sync): a row made oversize by MANY columns (none itself
/// over 4 KiB) reassembles — the width comes from column count, not one fat cell.
#[test]
#[ignore = "requires local PG"]
fn oversize_typed_wide_many_columns_reassembles() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    let r = c.query_one::<OvWideColsQuery>(()).expect("query_one OvWideCols");
    for (field, ch) in [
        (r.c1, b'a'), (r.c2, b'b'), (r.c3, b'c'), (r.c4, b'd'),
        (r.c5, b'e'), (r.c6, b'f'), (r.c7, b'g'), (r.c8, b'h'),
        (r.c9, b'i'), (r.c10, b'j'), (r.c11, b'k'), (r.c12, b'l'),
    ] {
        let s = field.expect("wide column present");
        assert_eq!(s.len(), 450, "each column intact after reassembly");
        assert!(s.bytes().all(|b| b == ch), "each column's bytes unshuffled");
    }
    c.close().expect("close");
}

/// INBOUND OVERSIZE (sync): the MULTI-ROW cases over a real table — an oversize
/// row FOLLOWED by a small one (buffer must RESET), MULTIPLE oversize rows,
/// `query_each` streaming a reassembled oversize row, and `query_one` TOO-MANY
/// over both orders (oversize FIRST + small → Row-arm break; small FIRST +
/// oversize → RowChunk-arm break MID-oversize-frame), each proving the
/// connection drains to a clean idle and stays healthy. Single test (serial) so
/// the shared `ov_rows` table cannot race a parallel sibling.
#[test]
#[ignore = "requires local PG"]
fn oversize_typed_multirow_reassembly_over_table() {
    let mut c = Connection::connect(&sync_config()).expect("connect");
    c.execute_sql("DROP TABLE IF EXISTS ov_rows").expect("drop residue");
    c.execute_sql("CREATE TABLE ov_rows (k INTEGER NOT NULL, body TEXT NOT NULL)")
        .expect("create ov_rows");

    // Scenario A — oversize (k=1) THEN small (k=2): the accumulator must reset.
    c.execute_sql("INSERT INTO ov_rows (k, body) VALUES (1, repeat('x', 5000)), (2, 'small')")
        .expect("insert oversize-then-small");
    let rows = c.query::<OvRowsQuery>(()).expect("query OvRows (A)");
    let lens: Vec<usize> = rows.iter().map(|r| r.expect("decodes").body.len()).collect();
    assert_eq!(lens, vec![5000, 5], "oversize row then small row; buffer reset");
    let owned = rows.into_owned().expect("into_owned (A)");
    assert!(owned[0].body.bytes().all(|b| b == b'x'), "oversize row bytes intact");
    assert_eq!(owned[1].body, "small", "the small row after an oversize row is clean");

    let mut streamed: Vec<usize> = Vec::new();
    c.query_each::<OvRowsQuery, _, ()>((), |rec| {
        streamed.push(rec.body.len());
        ControlFlow::Continue(())
    })
    .expect("query_each OvRows (A)");
    assert_eq!(streamed, vec![5000, 5], "query_each reassembles the oversize row and resets");

    // Scenario B — MULTIPLE oversize rows; the accumulator resets between them.
    c.execute_sql("TRUNCATE ov_rows").expect("truncate");
    c.execute_sql("INSERT INTO ov_rows (k, body) VALUES (1, repeat('x', 5000)), (2, repeat('y', 6000))")
        .expect("insert two oversize");
    let owned = c
        .query::<OvRowsQuery>(())
        .expect("query OvRows (B)")
        .into_owned()
        .expect("into_owned (B)");
    assert_eq!(owned.len(), 2, "two oversize rows");
    assert_eq!(owned[0].body.len(), 5000);
    assert!(owned[0].body.bytes().all(|b| b == b'x'), "first oversize row intact");
    assert_eq!(owned[1].body.len(), 6000);
    assert!(owned[1].body.bytes().all(|b| b == b'y'), "second oversize row intact");

    // Scenario C — query_one TOO-MANY, oversize FIRST then small: the oversize
    // row reassembles + counts, then the small second row trips the Row-arm
    // break. TooManyRows dominates; the connection drains healthy.
    c.execute_sql("TRUNCATE ov_rows").expect("truncate C");
    c.execute_sql("INSERT INTO ov_rows (k, body) VALUES (1, repeat('x', 5000)), (2, 'small')")
        .expect("insert oversize-then-small (C)");
    let too_many = c.query_one::<OvRowsQuery>(());
    assert!(
        matches!(too_many, Err(DriverError::TooManyRows)),
        "oversize first + small second must be TooManyRows, got {too_many:?}",
    );
    assert_eq!(
        c.query_one::<OneQuery>(()).expect("probe after C drain").n,
        1,
        "connection drained healthy after the oversize-first too-many break",
    );

    // Scenario D — query_one TOO-MANY, small FIRST then oversize: the second
    // row's FIRST RowChunk trips the RowChunk-arm break MID-oversize-frame (the
    // otherwise-unwitnessed branch); the mid-frame drain must reach a clean idle.
    c.execute_sql("TRUNCATE ov_rows").expect("truncate D");
    c.execute_sql("INSERT INTO ov_rows (k, body) VALUES (1, 'small'), (2, repeat('x', 5000))")
        .expect("insert small-then-oversize (D)");
    let too_many = c.query_one::<OvRowsQuery>(());
    assert!(
        matches!(too_many, Err(DriverError::TooManyRows)),
        "small first + oversize second must be TooManyRows, got {too_many:?}",
    );
    assert_eq!(
        c.query_one::<OneQuery>(()).expect("probe after D drain").n,
        1,
        "connection drained healthy after the mid-oversize-frame too-many break",
    );

    c.execute_sql("DROP TABLE ov_rows").expect("cleanup");
    c.close().expect("close");
}
