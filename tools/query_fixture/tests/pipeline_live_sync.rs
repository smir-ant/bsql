//! LIVE heterogeneous atomic pipeline over the SYNC (blocking) driver — the sync
//! twin of `pipeline_live_async.rs`. Same six witnesses (a heterogeneous batch, the
//! all-or-nothing rollback proof, cancel, transport death, in-transaction, explicit
//! BEGIN recovery), driven with blocking verbs; the concurrent cancel / terminate
//! use a `std::thread` (the sync `pipeline` blocks the calling thread).
//!
//! Run: `cargo test -p bsql-query-fixture --test pipeline_live_sync -- --ignored`
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    clippy::panic,
    reason = "live test harness — expect/unwrap/panic surface failures loudly; not production fallbacks"
)]

use std::thread;
use std::time::{Duration, Instant};

use bsql::BindExt;
use bsql_postgres_sync::{ConnectConfig, Connection, DriverError, SslMode};

bsql::query!(PlOneS, "SELECT 1::int4 AS n");
bsql::query!(PlHiS, "SELECT 'hello'::text AS s");
bsql::query!(PlSevenS, "SELECT 7::int4 AS n");
bsql::query!(
    PlInsAccountS,
    "INSERT INTO accounts (id, balance) VALUES ($1, $2) RETURNING id"
);
bsql::query!(PlSelAccountS, "SELECT id FROM accounts WHERE id = $1");
bsql::query!(PlSleepS, "SELECT 1::int4 AS n WHERE pg_sleep(3) IS NOT NULL");

fn cfg() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

fn prepare(c: &mut Connection, lo: i64, hi: i64) {
    c.execute_sql("CREATE TABLE IF NOT EXISTS accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)")
        .expect("create accounts");
    c.execute_sql(&format!("DELETE FROM accounts WHERE id BETWEEN {lo} AND {hi}"))
        .expect("clear id range");
}

fn account_exists(c: &mut Connection, id: i64) -> bool {
    !c.query::<PlSelAccountSQuery>((id,)).expect("select account").is_empty()
}

/// (a) heterogeneous read + read + write in one batch, all correct + committed.
#[test]
#[ignore = "requires local PG"]
fn heterogeneous_read_read_write_all_correct_and_committed() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    let id = 9_000_001i64;
    prepare(&mut c, 9_000_000, 9_000_099);

    let (one, hi, ins) = c
        .pipeline((
            PlOneSQuery::bind(()),
            PlHiSQuery::bind(()),
            PlInsAccountSQuery::bind((id, 500)),
        ))
        .expect("pipeline runs");
    assert_eq!(one.iter().next().expect("row").expect("decode").n, 1);
    assert_eq!(hi.iter().next().expect("row").expect("decode").s, "hello");
    assert_eq!(ins.iter().next().expect("row").expect("decode").id, id);
    assert!(account_exists(&mut c, id), "the insert committed");
    c.close().expect("close");
}

/// (b) THE AIRTIGHT PROOF: mid-batch duplicate-key → BatchFailed(index 1), and
/// command #0's write is ROLLED BACK (zero rows).
#[test]
#[ignore = "requires local PG"]
fn mid_batch_failure_rolls_back_the_whole_batch() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    let id = 9_100_001i64;
    prepare(&mut c, 9_100_000, 9_100_099);

    let result = c.pipeline((
        PlInsAccountSQuery::bind((id, 100)),
        PlInsAccountSQuery::bind((id, 200)),
    ));
    match result {
        Err(DriverError::BatchFailed { index, source }) => {
            assert_eq!(index, 1, "the SECOND command (index 1) failed");
            assert!(source.code().starts_with("23"), "23xxx, got {source:?}");
        }
        other => panic!("expected BatchFailed at index 1, got {other:?}"),
    }
    assert!(
        !account_exists(&mut c, id),
        "command #0's write MUST be rolled back — zero rows after a mid-batch failure",
    );
    assert!(c.is_healthy(), "connection healthy after a batch failure");
    assert_eq!(c.query_one::<PlSevenSQuery>(()).expect("reuse works").n, 7);
    c.close().expect("close");
}

/// The classified index accessor names the failing command.
#[test]
#[ignore = "requires local PG"]
fn batch_failed_index_accessor_names_the_command() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    let id = 9_150_001i64;
    prepare(&mut c, 9_150_000, 9_150_099);
    let err = c
        .pipeline((
            PlOneSQuery::bind(()),
            PlInsAccountSQuery::bind((id, 1)),
            PlInsAccountSQuery::bind((id, 2)),
        ))
        .expect_err("batch fails");
    assert_eq!(err.batch_failed_index(), Some(2), "the third command failed");
    assert!(!account_exists(&mut c, id), "all rolled back");
    c.close().expect("close");
}

/// (c) Cancel mid-batch (from another thread) → 57014, connection recovers.
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
    let result = c.pipeline((PlSleepSQuery::bind(()), PlOneSQuery::bind(())));
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
    assert_eq!(c.query_one::<PlSevenSQuery>(()).expect("reuse after cancel").n, 7);
    c.close().expect("close");
}

/// (d) Transport death mid-batch (backend terminated from another thread) → a
/// classified disconnect, never a torn success, bounded (no hang).
#[test]
#[ignore = "requires local PG"]
fn transport_death_mid_batch_is_a_classified_disconnect() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    let pid = c.backend_pid();
    let killer = thread::spawn(move || {
        let mut k = Connection::connect(&cfg()).expect("killer connect");
        thread::sleep(Duration::from_millis(300));
        drop(k.execute_sql(&format!("SELECT pg_terminate_backend({pid})")));
        drop(k.close());
    });

    let started = Instant::now();
    let result = c.pipeline((PlSleepSQuery::bind(()), PlOneSQuery::bind(())));
    let elapsed = started.elapsed();
    drop(killer.join());

    assert!(elapsed < Duration::from_secs(5), "bounded, not a hang ({elapsed:?})");
    let err = result.expect_err("a terminated backend fails the batch");
    assert!(err.is_disconnect(), "a terminated backend mid-batch is a disconnect, got {err:?}");
    assert!(!c.is_healthy(), "the connection is dead after a mid-batch termination");
    drop(c);
}

/// (e) A batch inside `conn.transaction(|tx| …)` commits.
#[test]
#[ignore = "requires local PG"]
fn pipeline_inside_a_transaction_guard_commits() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    let id = 9_200_001i64;
    prepare(&mut c, 9_200_000, 9_200_099);

    let (a, b) = c
        .transaction(|tx| {
            let (one, ins) = tx.pipeline((PlOneSQuery::bind(()), PlInsAccountSQuery::bind((id, 42))))?;
            Ok((
                one.iter().next().expect("row").expect("decode").n,
                ins.iter().next().expect("row").expect("decode").id,
            ))
        })
        .expect("transaction commits");
    assert_eq!(a, 1);
    assert_eq!(b, id);
    assert!(account_exists(&mut c, id), "the in-tx batch committed");
    c.close().expect("close");
}

/// (f) An explicit `BEGIN` around a failing batch: `pipeline` is CONSISTENT with a
/// normal failed verb — it leaves the explicit transaction ABORTED (`'E'`), NOT
/// auto-rolled-back. A follow-up verb is a loud `25P02`; an explicit `rollback()`
/// restores clean + reusable and the writes are gone.
#[test]
#[ignore = "requires local PG"]
fn explicit_begin_then_failing_batch_leaves_aborted_tx_until_rollback() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    let id = 9_300_001i64;
    prepare(&mut c, 9_300_000, 9_300_099);

    c.execute_sql("BEGIN").expect("open explicit tx");
    let result = c.pipeline((
        PlInsAccountSQuery::bind((id, 1)),
        PlInsAccountSQuery::bind((id, 2)),
    ));
    assert!(
        matches!(result, Err(DriverError::BatchFailed { index: 1, .. })),
        "the batch fails at index 1, got {result:?}",
    );
    // Left ABORTED: a follow-up verb is a LOUD 25P02, not a silent autocommit.
    match c.query_one::<PlSevenSQuery>(()) {
        Err(DriverError::Db(e)) => assert_eq!(
            e.code(), "25P02",
            "an in-aborted-tx verb must be a loud 25P02, never a silent autocommit; got {e:?}",
        ),
        other => panic!("expected a loud 25P02, got {other:?}"),
    }
    assert!(c.is_healthy(), "the connection is alive (25P02 is recoverable)");
    c.rollback().expect("rollback restores clean state");
    assert_eq!(c.query_one::<PlSevenSQuery>(()).expect("clean + reusable after rollback").n, 7);
    assert!(!account_exists(&mut c, id), "the failed batch's writes are rolled back");
    c.close().expect("close");
}

/// THE BLIND-ZONE REGRESSION (sync): inside `conn.transaction(|tx| …)`, ignoring a
/// failing `tx.pipeline` and issuing another verb yields a LOUD `25P02`, NOT a
/// silent autocommit — and the guard rolls the WHOLE scope back.
#[test]
#[ignore = "requires local PG"]
fn ignored_in_guard_pipeline_error_does_not_autocommit_later_verbs() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    let a_id = 9_400_001i64;
    let p_id = 9_400_002i64;
    let d_id = 9_400_003i64;
    prepare(&mut c, 9_400_000, 9_400_099);

    let d_is_25p02 = c
        .transaction(|tx| {
            let _a = tx.query::<PlInsAccountSQuery>((a_id, 1))?;
            drop(tx.pipeline((
                PlInsAccountSQuery::bind((p_id, 1)),
                PlInsAccountSQuery::bind((p_id, 2)),
            )));
            let d = tx.query::<PlInsAccountSQuery>((d_id, 1));
            Ok(matches!(&d, Err(DriverError::Db(e)) if e.code() == "25P02"))
        })
        .expect("the guard resolves (COMMIT of an aborted tx rolls back cleanly)");

    assert!(
        d_is_25p02,
        "a verb after an ignored in-guard pipeline error MUST be a loud 25P02 — never a silent autocommit",
    );
    assert!(!account_exists(&mut c, a_id), "A's write rolled back with the whole tx");
    assert!(!account_exists(&mut c, p_id), "B's write rolled back");
    assert!(!account_exists(&mut c, d_id), "D never committed (25P02) and did not autocommit");
    c.close().expect("close");
}
