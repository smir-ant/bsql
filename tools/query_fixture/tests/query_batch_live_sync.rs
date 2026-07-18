//! LIVE homogeneous atomic bulk-QUERY batch over the SYNC (blocking) driver — the
//! twin of `query_batch_live_async.rs`. The cancel witness uses a `std::thread` (the
//! sync `query_batch` blocks the calling thread). Same witnesses (a)-(g).
//!
//! Run: `cargo test -p bsql-query-fixture --test query_batch_live_sync -- --ignored`
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

use std::thread;
use std::time::{Duration, Instant};

use bsql::DecodeError;
use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

bsql::query!(
    QbsIns,
    "INSERT INTO qb_rows (id, label) VALUES ($1, $2::text) RETURNING id, label"
);
bsql::query!(
    QbsSel,
    "SELECT id, label FROM qb_rows WHERE id <= $1 ORDER BY id"
);
bsql::query!(QbsSeven, "SELECT 7::int4 AS n");
bsql::query!(QbsTag, "SELECT tag FROM oidguard");
bsql::query!(QbsSleep, "SELECT pg_sleep(3)::text AS s");

const OID_TEXT: u32 = 25;
const OID_INT4: u32 = 23;

fn cfg() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

fn fresh(c: &mut Connection) {
    c.execute_raw("CREATE TEMP TABLE qb_rows (id BIGINT PRIMARY KEY, label TEXT NOT NULL)")
        .expect("create temp");
}

fn row_count(c: &mut Connection) -> i64 {
    c.query_one_raw("SELECT count(*)::int8 AS n FROM qb_rows")
        .expect("count")
        .get_i64(0)
        .expect("decode")
        .unwrap_or(-1)
}

/// (a) N INSERT...RETURNING → N grouped `Rows<Q>` with the correct DECODED values.
#[test]
#[ignore = "requires local PG"]
fn n_inserts_return_grouped_decoded_rows_and_all_apply() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    fresh(&mut c);
    let labels = ["alpha", "beta", "gamma", "delta"];
    let grouped = c
        .query_batch::<QbsIns>(labels.iter().enumerate().map(|(i, l)| ((i as i64) + 1, *l)))
        .expect("insert batch");
    assert_eq!(grouped.len(), labels.len(), "one Rows<Q> per command");
    for (i, rows) in grouped.iter().enumerate() {
        let rec = rows.iter().next().expect("one returning row").expect("decode");
        assert_eq!(rec.id, (i as i64) + 1, "returned id");
        assert_eq!(rec.label, labels[i], "returned label DECODED, not just counted");
        assert_eq!(rows.len(), 1);
    }
    assert_eq!(row_count(&mut c), labels.len() as i64, "all applied");
    c.close().expect("close");
}

/// (b) GROUPING: a multi-row-per-command SELECT batch keeps each command's rows in
/// its OWN `Rows<Q>`.
#[test]
#[ignore = "requires local PG"]
fn grouping_is_preserved_per_command() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    fresh(&mut c);
    c.query_batch::<QbsIns>((1..=5).map(|i| (i, "x"))).expect("seed 5");
    let grouped = c
        .query_batch::<QbsSel>(vec![(1_i64,), (3,), (5,)])
        .expect("select batch");
    assert_eq!(grouped.len(), 3, "one Rows<Q> per command");
    assert_eq!(grouped[0].len(), 1, "id <= 1 → 1 row");
    assert_eq!(grouped[1].len(), 3, "id <= 3 → 3 rows");
    assert_eq!(grouped[2].len(), 5, "id <= 5 → 5 rows");
    let ids2: Vec<i64> = grouped[1].iter().map(|r| r.expect("decode").id).collect();
    assert_eq!(ids2, vec![1, 2, 3], "command #1's own rows");
    c.close().expect("close");
}

/// (c) THE AIRTIGHT PROOF (small N): a mid-batch failure returns ZERO grouped results
/// and applies NOTHING.
#[test]
#[ignore = "requires local PG"]
fn mid_batch_failure_returns_zero_results_and_applies_nothing() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    fresh(&mut c);
    let result = c.query_batch::<QbsIns>(vec![
        (10_i64, "a"),
        (11, "b"),
        (12, "c"),
        (11, "d"), // duplicate PK → 23505 at command #3
        (13, "e"),
    ]);
    match result {
        Err(DriverError::BatchFailed { index, ref source }) => {
            assert_eq!(index, 3);
            assert_eq!(source.code(), "23505", "duplicate key");
        }
        other => panic!("expected BatchFailed {{ index: 3 }}, got {other:?}"),
    }
    let err = result.expect_err("failed");
    assert_eq!(err.batch_failed_index(), Some(3));
    assert!(!err.is_disconnect());
    assert_eq!(row_count(&mut c), 0, "a mid-batch failure applied NOTHING");
    assert_eq!(c.query_one::<QbsSeven>(()).expect("reuse").n, 7);
    c.close().expect("close");
}

/// (d) N == 0 (no wire I/O) and N == 1 (equals a single `query`).
#[test]
#[ignore = "requires local PG"]
fn zero_and_one() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    fresh(&mut c);
    let empty = c
        .query_batch::<QbsIns>(Vec::<(i64, &str)>::new())
        .expect("N=0");
    assert!(empty.is_empty(), "N=0 → empty Vec, no I/O");
    let one = c.query_batch::<QbsIns>(vec![(1_i64, "solo")]).expect("N=1");
    assert_eq!(one.len(), 1);
    let rec = one[0].iter().next().expect("row").expect("decode");
    assert_eq!((rec.id, rec.label), (1, "solo"));
    assert_eq!(row_count(&mut c), 1);
    c.close().expect("close");
}

/// (e) LARGE N crossing MANY send windows: constant memory, deadlock-free; + a LARGE
/// mid-batch failure rolls the WHOLE thing back.
#[test]
#[ignore = "requires local PG"]
fn large_n_windowed_is_correct_and_deadlock_free() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    fresh(&mut c);
    const N: i64 = 20_000;
    let grouped = c
        .query_batch::<QbsIns>((0..N).map(|i| (i, "bulk")))
        .expect("large batch");
    assert_eq!(grouped.len(), N as usize, "one grouped result per command");
    for (i, rows) in grouped.iter().enumerate() {
        assert_eq!(rows.iter().next().expect("row").expect("decode").id, i as i64);
    }
    assert_eq!(row_count(&mut c), N, "all N rows persisted");
    c.execute_raw("TRUNCATE qb_rows").expect("truncate");
    let mut sets: Vec<(i64, &str)> = (0..N).map(|i| (i, "x")).collect();
    sets.push((5_000, "dup"));
    let result = c.query_batch::<QbsIns>(sets);
    assert!(matches!(result, Err(DriverError::BatchFailed { index, .. }) if index == N as usize));
    assert_eq!(row_count(&mut c), 0, "large mid-batch failure applied NOTHING");
    c.close().expect("close");
}

/// (f) OID-GUARD DRIFT → classified `BatchColumnOidMismatch { command: 0 }`, recovers.
#[test]
#[ignore = "requires local PG"]
fn oid_guard_drift_is_a_classified_batch_column_oid_mismatch() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE oidguard (tag int4 NOT NULL, vc varchar NOT NULL, bp bpchar NOT NULL, n int4 NOT NULL)")
        .expect("create drift shadow");
    c.execute_raw("INSERT INTO oidguard (tag, vc, bp, n) VALUES (1094795585, 'v', 'b', 1)")
        .expect("seed drift");
    match c.query_batch::<QbsTag>(vec![(), ()]) {
        Err(e @ DriverError::BatchColumnOidMismatch { .. }) => {
            assert_eq!(e.batch_failed_index(), Some(0), "verified ONCE on command 0");
            assert!(!e.is_disconnect());
            match &e {
                DriverError::BatchColumnOidMismatch {
                    command,
                    source: DecodeError::ColumnOidMismatch { index, expected, found },
                } => {
                    assert_eq!(*command, 0);
                    assert_eq!(*index, 0);
                    assert_eq!(*expected, OID_TEXT);
                    assert_eq!(*found, OID_INT4);
                }
                other => panic!("expected a ColumnOidMismatch source, got {other:?}"),
            }
        }
        other => panic!("expected BatchColumnOidMismatch, got {other:?}"),
    }
    assert_eq!(c.query_one::<QbsSeven>(()).expect("recovers").n, 7);
    c.close().expect("close");
}

/// (f2) OID-guard NO false positive: a matching-typed shadow decodes correctly.
#[test]
#[ignore = "requires local PG"]
fn oid_guard_matching_shadow_decodes_correctly() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    c.execute_raw("CREATE TEMP TABLE oidguard (tag text NOT NULL, vc varchar NOT NULL, bp bpchar NOT NULL, n int4 NOT NULL)")
        .expect("create match shadow");
    c.execute_raw("INSERT INTO oidguard (tag, vc, bp, n) VALUES ('hello', 'v', 'b', 1)")
        .expect("seed match");
    let grouped = c.query_batch::<QbsTag>(vec![(), ()]).expect("matching batch runs");
    assert_eq!(grouped.len(), 2);
    for rows in &grouped {
        assert_eq!(rows.iter().next().expect("row").expect("decode").tag, "hello");
    }
    c.close().expect("close");
}

/// (g) CANCEL mid-batch (from another thread) → 57014, connection recovers.
#[test]
#[ignore = "requires local PG"]
fn cancel_mid_batch_is_57014_and_connection_recovers() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    let token = c.cancel_token();
    let canceller = thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        drop(token.cancel());
    });
    let started = Instant::now();
    let result = c.query_batch::<QbsSleep>(vec![(), ()]);
    let elapsed = started.elapsed();
    drop(canceller.join());
    assert!(elapsed < Duration::from_secs(2), "cancel bounded the batch ({elapsed:?})");
    match result {
        Err(DriverError::BatchFailed { source, .. }) => {
            assert_eq!(source.code(), "57014", "query_canceled");
        }
        other => panic!("expected a 57014 BatchFailed, got {other:?}"),
    }
    assert!(c.is_healthy(), "a cancel is NOT a disconnect — connection reusable");
    assert_eq!(c.query_one::<QbsSeven>(()).expect("reuse after cancel").n, 7);
    c.close().expect("close");
}
