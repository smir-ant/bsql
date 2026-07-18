//! LIVE homogeneous atomic bulk-write batch over the SYNC (blocking) driver — the
//! twin of `execute_batch_live_async.rs`. Same witnesses; async/sync parity is a
//! COMPILER guarantee (one `Core::execute_batch`), so this pins that the blocking
//! `poll_once` shim drives the windowed multi-await batch identically.
//!
//! Run: `cargo test -p bsql-query-fixture --test execute_batch_live_sync -- --ignored`
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    clippy::panic,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    reason = "live test harness — expect/unwrap/panic surface failures loudly; not production fallbacks"
)]

use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

bsql::query!(
    EbsIns,
    "INSERT INTO eb_rows (id, balance) VALUES ($1, $2) RETURNING id"
);
bsql::query!(
    EbsUpd,
    "UPDATE eb_rows SET balance = balance + $2::int8 WHERE id = $1 RETURNING id"
);
bsql::query!(EbsSeven, "SELECT 7::int4 AS n");
bsql::query!(
    EbsDeferIns,
    "INSERT INTO eb_uniq (id, tag) VALUES ($1, $2) RETURNING id"
);

fn cfg() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

fn fresh(c: &mut Connection) {
    // A per-connection TEMP TABLE (session-private) shadows the migration's permanent
    // `eb_rows` so the tests, each on its OWN connection, never interfere under cargo's
    // default parallelism (no shared permanent table is dropped or truncated).
    c.execute_raw("CREATE TEMP TABLE eb_rows (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)")
        .expect("create temp");
}

fn row_count(c: &mut Connection) -> i64 {
    c.query_one_raw("SELECT count(*)::int8 AS n FROM eb_rows")
        .expect("count")
        .get_i64(0)
        .expect("decode")
        .unwrap_or(-1)
}

/// (a) N updates → N correct affected counts, every write applied.
#[test]
#[ignore = "requires local PG"]
fn n_updates_return_correct_counts_and_all_apply() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    fresh(&mut c);
    c.execute_batch::<EbsIns, _>((1..=5).map(|i| (i, 0_i64)))
        .expect("seed");
    let counts = c
        .execute_batch::<EbsUpd, _>(vec![(1_i64, 10_i64), (2, 20), (3, 30), (99, 40)])
        .expect("update batch");
    assert_eq!(counts, vec![1, 1, 1, 0]);
    let sum = c
        .query_one_raw("SELECT coalesce(sum(balance),0)::int8 AS s FROM eb_rows")
        .expect("sum")
        .get_i64(0)
        .expect("decode")
        .unwrap_or(-1);
    assert_eq!(sum, 60);
    c.close().expect("close");
}

/// (b) A mid-batch duplicate-PK failure applies NOTHING.
#[test]
#[ignore = "requires local PG"]
fn mid_batch_failure_applies_nothing() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    fresh(&mut c);
    let result = c.execute_batch::<EbsIns, _>(vec![
        (10_i64, 1_i64),
        (11, 1),
        (12, 1),
        (11, 1),
        (13, 1),
    ]);
    match result {
        Err(DriverError::BatchFailed { index, ref source }) => {
            assert_eq!(index, 3);
            assert_eq!(source.code(), "23505");
        }
        other => panic!("expected BatchFailed {{ index: 3 }}, got {other:?}"),
    }
    assert_eq!(result.expect_err("failed").batch_failed_index(), Some(3));
    assert_eq!(row_count(&mut c), 0, "mid-batch failure applied NOTHING");
    assert_eq!(c.query_one::<EbsSeven>(()).expect("reuse").n, 7);
    c.close().expect("close");
}

/// (c) COMMIT-TIME deferred failure → Db(23505), `batch_failed_index` None.
#[test]
#[ignore = "requires local PG"]
fn commit_time_deferred_failure_is_db_none_index() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    // Session-private TEMP TABLE (shadows the migration's permanent `eb_uniq`) so
    // parallel tests never interfere.
    c.execute_raw(
        "CREATE TEMP TABLE eb_uniq (id INTEGER PRIMARY KEY, tag INTEGER NOT NULL, \
         CONSTRAINT eb_uniq_tag_uniq UNIQUE (tag) DEFERRABLE INITIALLY DEFERRED)",
    )
    .expect("create temp");
    let result = c.execute_batch::<EbsDeferIns, _>(vec![(1_i32, 77_i32), (2, 77)]);
    match result {
        Err(DriverError::Db(ref e)) => assert_eq!(e.code(), "23505"),
        other => panic!("expected Db(23505), got {other:?}"),
    }
    assert_eq!(result.expect_err("failed").batch_failed_index(), None);
    c.close().expect("close");
}

/// (d) NO auto-rollback: an IGNORED batch error inside a guard makes the next verb
/// fail loudly (25P02).
#[test]
#[ignore = "requires local PG"]
fn ignored_in_guard_batch_error_does_not_autocommit() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    fresh(&mut c);
    let code = c
        .transaction(|tx| {
            drop(tx.execute_batch::<EbsIns, _>(vec![(1_i64, 1_i64), (1, 1)]));
            let d = tx.execute_batch::<EbsIns, _>(vec![(2_i64, 2_i64)]);
            Ok(match &d {
                Err(DriverError::Db(e)) => e.code().to_string(),
                Err(DriverError::BatchFailed { source, .. }) => source.code().to_string(),
                other => panic!("expected a loud 25P02, got {other:?}"),
            })
        })
        .expect("the guard resolves (COMMIT of an aborted tx rolls back cleanly)");
    assert_eq!(code, "25P02", "loud abort, never a silent autocommit");
    assert_eq!(row_count(&mut c), 0);
    c.close().expect("close");
}

/// (e) N == 0 (no wire I/O) and N == 1 (one command).
#[test]
#[ignore = "requires local PG"]
fn zero_and_one() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    fresh(&mut c);
    assert_eq!(
        c.execute_batch::<EbsIns, _>(Vec::<(i64, i64)>::new())
            .expect("N=0"),
        Vec::<u64>::new(),
    );
    assert_eq!(
        c.execute_batch::<EbsIns, _>(vec![(1_i64, 5_i64)]).expect("N=1"),
        vec![1],
    );
    assert_eq!(row_count(&mut c), 1);
    c.close().expect("close");
}

/// (f) LARGE N crossing MANY send windows: constant memory, deadlock-free.
#[test]
#[ignore = "requires local PG"]
fn large_n_windowed_is_correct_and_deadlock_free() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    fresh(&mut c);
    const N: i64 = 20_000;
    let counts = c
        .execute_batch::<EbsIns, _>((0..N).map(|i| (i, i * 10)))
        .expect("large batch");
    assert_eq!(counts.len(), N as usize);
    assert!(counts.iter().all(|&r| r == 1));
    assert_eq!(row_count(&mut c), N);
    // Large mid-batch failure still rolls the whole thing back. The TEMP `eb_rows`
    // already exists on this connection, so CLEAR it (not re-create).
    c.execute_raw("TRUNCATE eb_rows").expect("truncate");
    let mut sets: Vec<(i64, i64)> = (0..N).map(|i| (i, 1)).collect();
    sets.push((5_000, 1));
    let result = c.execute_batch::<EbsIns, _>(sets);
    assert!(matches!(result, Err(DriverError::BatchFailed { index, .. }) if index == N as usize));
    assert_eq!(row_count(&mut c), 0, "large mid-batch failure applied NOTHING");
    c.close().expect("close");
}

/// (Part B) The typed FLAGSHIP `execute::<Q>(params)` — SYMMETRIC with `query`,
/// derived from the carrier and the N=1 sibling of `execute_batch::<Q>`. An INSERT
/// and an UPDATE return the correct affected count (sync twin).
#[test]
#[ignore = "requires local PG"]
fn typed_execute_returns_the_affected_count() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    fresh(&mut c);
    let inserted = c.execute::<EbsIns>((1_i64, 100_i64)).expect("insert");
    assert_eq!(inserted, 1, "one INSERT affected 1 row");
    let updated = c.execute::<EbsUpd>((1_i64, 5_i64)).expect("update");
    assert_eq!(updated, 1, "the UPDATE affected 1 row");
    let missed = c.execute::<EbsUpd>((999_i64, 5_i64)).expect("update-absent");
    assert_eq!(missed, 0, "an absent id affects 0 rows");
    let balance = c
        .query_one_raw("SELECT coalesce(sum(balance),0)::int8 AS s FROM eb_rows")
        .expect("sum")
        .get_i64(0)
        .expect("decode")
        .unwrap_or(-1);
    assert_eq!(balance, 105, "INSERT + UPDATE applied");
    c.close().expect("close");
}
