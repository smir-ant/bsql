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
bsql::query!(
    PlDeferInsS,
    "INSERT INTO pl_deferred (id, tag) VALUES ($1, $2) RETURNING id"
);
// Carriers for the WINDOWED deadlock-free witnesses (0021_pl_bulk.sql) — the sync
// twins of `pipeline_live_async`'s: an EARLY command returning a LARGE (~4 MiB)
// result, paired with LATER commands carrying LARGE `text` params.
bsql::query!(PlBigResultS, "SELECT repeat('x', 4000000)::text AS s");
bsql::query!(
    PlBulkInsS,
    "INSERT INTO pl_bulk (id, payload) VALUES ($1, $2) RETURNING id"
);

fn cfg() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

fn prepare(c: &mut Connection, lo: i64, hi: i64) {
    c.execute_raw("CREATE TABLE IF NOT EXISTS accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)")
        .expect("create accounts");
    c.execute_raw(&format!("DELETE FROM accounts WHERE id BETWEEN {lo} AND {hi}"))
        .expect("clear id range");
}

fn account_exists(c: &mut Connection, id: i64) -> bool {
    !c.query::<PlSelAccountS>((id,)).expect("select account").is_empty()
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
            PlOneS::bind(()),
            PlHiS::bind(()),
            PlInsAccountS::bind((id, 500)),
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
        PlInsAccountS::bind((id, 100)),
        PlInsAccountS::bind((id, 200)),
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
    assert_eq!(c.query_one::<PlSevenS>(()).expect("reuse works").n, 7);
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
            PlOneS::bind(()),
            PlInsAccountS::bind((id, 1)),
            PlInsAccountS::bind((id, 2)),
        ))
        .expect_err("batch fails");
    assert_eq!(err.batch_failed_index(), Some(2), "the third command failed");
    assert!(!account_exists(&mut c, id), "all rolled back");
    c.close().expect("close");
}

/// COMMIT-TIME failure (regression for `failed_index == arity`): both commands
/// succeed at Execute, then the implicit COMMIT fails a `DEFERRABLE INITIALLY
/// DEFERRED UNIQUE` — a batch-level `Db(23505)`, `batch_failed_index()` is `None`,
/// NEVER `BatchFailed { index: 2 }`; zero rows persisted (all-or-nothing).
#[test]
#[ignore = "requires local PG"]
fn commit_time_deferred_constraint_failure_is_honest_not_out_of_range_index() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    c.execute_raw("DROP TABLE IF EXISTS pl_deferred").expect("drop");
    c.execute_raw(
        "CREATE TABLE pl_deferred (id INTEGER PRIMARY KEY, tag INTEGER NOT NULL, \
         CONSTRAINT pl_deferred_tag_uniq UNIQUE (tag) DEFERRABLE INITIALLY DEFERRED)",
    )
    .expect("create pl_deferred");

    let result = c.pipeline((PlDeferInsS::bind((1, 77)), PlDeferInsS::bind((2, 77))));

    match result {
        Err(DriverError::Db(ref e)) => {
            assert_eq!(e.code(), "23505", "the deferred UNIQUE fired at commit: {e:?}");
        }
        Err(DriverError::BatchFailed { index, .. }) => panic!(
            "a commit-time failure must NOT be BatchFailed (it named a nonexistent command #{index})",
        ),
        other => panic!("expected a commit-time Db(23505), got {other:?}"),
    }
    let err = result.expect_err("the batch failed at commit");
    assert_eq!(
        err.batch_failed_index(),
        None,
        "a commit-time failure is attributable to no command → batch_failed_index() is None",
    );
    assert!(!err.is_disconnect(), "a 23505 is a per-query error, not a disconnect");

    let count = c
        .query_one_raw("SELECT count(*)::int8 AS n FROM pl_deferred")
        .expect("count");
    assert_eq!(
        count.get_i64(0).expect("decode").unwrap_or(-1),
        0,
        "the commit-time failure rolled the whole batch back — zero rows persisted",
    );
    assert_eq!(c.query_one::<PlSevenS>(()).expect("reuse after commit failure").n, 7);
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
    let result = c.pipeline((PlSleepS::bind(()), PlOneS::bind(())));
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
    assert_eq!(c.query_one::<PlSevenS>(()).expect("reuse after cancel").n, 7);
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
        drop(k.execute_raw(&format!("SELECT pg_terminate_backend({pid})")));
        drop(k.close());
    });

    let started = Instant::now();
    let result = c.pipeline((PlSleepS::bind(()), PlOneS::bind(())));
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
            let (one, ins) = tx.pipeline((PlOneS::bind(()), PlInsAccountS::bind((id, 42))))?;
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

    c.execute_raw("BEGIN").expect("open explicit tx");
    let result = c.pipeline((
        PlInsAccountS::bind((id, 1)),
        PlInsAccountS::bind((id, 2)),
    ));
    assert!(
        matches!(result, Err(DriverError::BatchFailed { index: 1, .. })),
        "the batch fails at index 1, got {result:?}",
    );
    // Left ABORTED: a follow-up verb is a LOUD 25P02, not a silent autocommit.
    match c.query_one::<PlSevenS>(()) {
        Err(DriverError::Db(e)) => assert_eq!(
            e.code(), "25P02",
            "an in-aborted-tx verb must be a loud 25P02, never a silent autocommit; got {e:?}",
        ),
        other => panic!("expected a loud 25P02, got {other:?}"),
    }
    assert!(c.is_healthy(), "the connection is alive (25P02 is recoverable)");
    c.rollback().expect("rollback restores clean state");
    assert_eq!(c.query_one::<PlSevenS>(()).expect("clean + reusable after rollback").n, 7);
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
            let _a = tx.query::<PlInsAccountS>((a_id, 1))?;
            drop(tx.pipeline((
                PlInsAccountS::bind((p_id, 1)),
                PlInsAccountS::bind((p_id, 2)),
            )));
            let d = tx.query::<PlInsAccountS>((d_id, 1));
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

/// Ensure `pl_bulk` exists and this test's id range is clear.
fn prepare_bulk(c: &mut Connection, lo: i64, hi: i64) {
    // Tolerate the `CREATE TABLE IF NOT EXISTS` concurrent-creation race (a `23505` on
    // the `pg_type` unique index / `42P07`) between the parallel windowed tests — see
    // the async twin for the full note.
    if let Err(e) =
        c.execute_raw("CREATE TABLE IF NOT EXISTS pl_bulk (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)")
    {
        let raced = matches!(&e, DriverError::Db(db) if db.code() == "23505" || db.code() == "42P07");
        assert!(raced, "create pl_bulk failed for a non-race reason: {e:?}");
    }
    c.execute_raw(&format!("DELETE FROM pl_bulk WHERE id BETWEEN {lo} AND {hi}"))
        .expect("clear id range");
}

fn bulk_count(c: &mut Connection, lo: i64, hi: i64) -> i64 {
    c.query_one_raw(&format!(
        "SELECT count(*)::int8 AS n FROM pl_bulk WHERE id BETWEEN {lo} AND {hi}"
    ))
    .expect("count")
    .get_i64(0)
    .expect("decode")
    .unwrap_or(-1)
}

/// THE WINDOWED DEADLOCK-FREE WITNESS (sync twin). Same batch shape as the async
/// witness — an EARLY ~4 MiB result + SIX 512 KiB `text` params. The blocking
/// `pipeline` runs in a WORKER thread joined with `recv_timeout`, so a
/// stage-all-then-flush regression (which DEADLOCKS this shape) fails LOUDLY at the
/// timeout instead of hanging the test forever. Completion with correct results is
/// the deadlock-free + bounded-memory proof.
#[test]
#[ignore = "requires local PG"]
fn windowed_large_result_plus_large_params_does_not_deadlock() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    let base = 9_800_000i64;
    prepare_bulk(&mut c, base, base + 999);
    let payload = "a".repeat(512 * 1024); // 512 KiB per command → ~3 MiB tail

    let (tx, rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let out = c.pipeline((
            PlBigResultS::bind(()),
            PlBulkInsS::bind((base + 1, payload.as_str())),
            PlBulkInsS::bind((base + 2, payload.as_str())),
            PlBulkInsS::bind((base + 3, payload.as_str())),
            PlBulkInsS::bind((base + 4, payload.as_str())),
            PlBulkInsS::bind((base + 5, payload.as_str())),
            PlBulkInsS::bind((base + 6, payload.as_str())),
        ));
        // Extract owned primitives INSIDE the worker (the borrowed records alias the
        // per-command `Rows`), then hand the connection + data back to the test.
        let extracted = out.map(|(big, r1, r2, r3, r4, r5, r6)| {
            let big_len = big
                .iter()
                .next()
                .expect("row")
                .expect("decode")
                .s
                .expect("non-null result")
                .len();
            let ids: Vec<i64> = [r1, r2, r3, r4, r5, r6]
                .iter()
                .map(|r| r.iter().next().expect("row").expect("decode").id)
                .collect();
            (big_len, ids)
        });
        let _ = tx.send((c, extracted));
    });

    let (mut c, extracted) = rx.recv_timeout(Duration::from_secs(60)).expect(
        "pipeline completed within 60s — a stage-all-then-flush regression would DEADLOCK here",
    );
    worker.join().expect("worker thread");
    let (big_len, ids) = extracted.expect("pipeline runs");
    assert_eq!(big_len, 4_000_000, "the ~4 MiB early result decoded whole");
    assert_eq!(
        ids,
        vec![base + 1, base + 2, base + 3, base + 4, base + 5, base + 6],
        "every windowed write returned its id, in order",
    );
    assert_eq!(bulk_count(&mut c, base + 1, base + 6), 6, "all six writes committed");
    c.close().expect("close");
}

/// ALL-OR-NOTHING at LARGE payload across MANY windows (sync twin). Eight 256 KiB
/// `text` params then a NINTH duplicating id #0 → `23505`; the whole windowed batch
/// ROLLS BACK: `BatchFailed { index: 8 }` and ZERO rows persisted.
#[test]
#[ignore = "requires local PG"]
fn windowed_all_or_nothing_rollback_at_large_payload() {
    let mut c = Connection::connect(&cfg()).expect("connect");
    let base = 9_900_000i64;
    prepare_bulk(&mut c, base, base + 999);
    let payload = "b".repeat(256 * 1024); // 256 KiB per command → ~2 MiB tail

    let (tx, rx) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let out = c.pipeline((
            PlBulkInsS::bind((base + 1, payload.as_str())),
            PlBulkInsS::bind((base + 2, payload.as_str())),
            PlBulkInsS::bind((base + 3, payload.as_str())),
            PlBulkInsS::bind((base + 4, payload.as_str())),
            PlBulkInsS::bind((base + 5, payload.as_str())),
            PlBulkInsS::bind((base + 6, payload.as_str())),
            PlBulkInsS::bind((base + 7, payload.as_str())),
            PlBulkInsS::bind((base + 8, payload.as_str())),
            PlBulkInsS::bind((base + 1, payload.as_str())), // DUPLICATE id → 23505
        ));
        let classified: Result<(usize, String), String> = match out {
            Err(DriverError::BatchFailed { index, source }) => Ok((index, source.code().to_string())),
            Ok(_) => Err("expected BatchFailed, got Ok".to_string()),
            Err(other) => Err(format!("expected BatchFailed, got {other:?}")),
        };
        let _ = tx.send((c, classified));
    });

    let (mut c, classified) = rx
        .recv_timeout(Duration::from_secs(60))
        .expect("pipeline completed within 60s (no deadlock)");
    worker.join().expect("worker thread");
    let (index, code) = classified.expect("batch failed as a BatchFailed");
    assert_eq!(index, 8, "the NINTH command (index 8) hit the duplicate key");
    assert!(code.starts_with("23"), "a constraint violation, got {code}");
    // THE PROOF: the whole implicit transaction rolled back — zero rows persisted.
    assert_eq!(
        bulk_count(&mut c, base + 1, base + 8),
        0,
        "a mid-batch failure rolled back every windowed write — zero rows persisted",
    );
    assert!(c.is_healthy(), "connection stays healthy after a windowed batch failure");
    assert_eq!(c.query_one::<PlSevenS>(()).expect("reuse").n, 7);
    c.close().expect("close");
}
