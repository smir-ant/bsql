//! LIVE homogeneous atomic bulk-write batch over the ASYNC (tokio) driver.
//!
//! `conn.execute_batch::<Q>(params_iter)` runs ONE compile-checked `query!` write
//! carrier against N runtime parameter sets, Parse-once, ONE trailing `Sync` (one
//! implicit transaction), returning `Vec<u64>` affected counts. End-to-end witnesses
//! against REAL PostgreSQL:
//!
//! - (a) N updates → N correct affected counts AND every write applied;
//! - (b) THE AIRTIGHT PROOF: a mid-batch failure applies NOTHING (zero rows persisted)
//!   and returns `BatchFailed` naming the failing index;
//! - (c) COMMIT-TIME deferred-constraint failure → `Db` (23505), `batch_failed_index`
//!   `None` (inherited from the pipeline core), zero rows persisted;
//! - (d) NO auto-rollback: an IGNORED batch error inside a guard makes the next verb
//!   fail loudly (`25P02`), never a silent autocommit;
//! - (e) N == 0 (no wire I/O) and N == 1 (equals a single `execute`);
//! - (f) LARGE N crossing MANY send windows → constant memory, every write applied,
//!   deadlock-free (the naive single-flush design would hang at this N).
//!
//! Run: `cargo test -p bsql-query-fixture --test execute_batch_live_async -- --ignored`
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

use bsql_postgres_async::{ConnectConfig, Connection, DriverError, SslMode};

// A typed `query!` carrier requires a ROW SHAPE (SELECT or `… RETURNING`), exactly
// like the serial `execute::<Q>`; the affected count rides the CommandComplete tag,
// and the RETURNING rows are read-and-ignored by `execute_batch`.
bsql::query!(
    EbIns,
    "INSERT INTO eb_rows (id, balance) VALUES ($1, $2) RETURNING id"
);
bsql::query!(
    EbUpd,
    "UPDATE eb_rows SET balance = balance + $2::int8 WHERE id = $1 RETURNING id"
);
bsql::query!(EbSeven, "SELECT 7::int4 AS n");
bsql::query!(
    EbDeferIns,
    "INSERT INTO eb_uniq (id, tag) VALUES ($1, $2) RETURNING id"
);

fn cfg() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// Fresh empty `eb_rows` table (PK id, NOT NULL balance). A per-connection TEMP
/// TABLE (session-private) shadows the migration's permanent `eb_rows` — same
/// columns, which is what the `query!` carrier was validated against — so the six
/// tests, EACH on its OWN connection, never interfere under cargo's default
/// parallelism (no shared permanent table is dropped or truncated).
async fn fresh(c: &mut Connection) {
    c.execute_raw("CREATE TEMP TABLE eb_rows (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)")
        .await
        .expect("create temp");
}

async fn row_count(c: &mut Connection) -> i64 {
    c.query_one_raw("SELECT count(*)::int8 AS n FROM eb_rows")
        .await
        .expect("count")
        .get_i64(0)
        .expect("decode")
        .unwrap_or(-1)
}

async fn balance_sum(c: &mut Connection) -> i64 {
    c.query_one_raw("SELECT coalesce(sum(balance),0)::int8 AS s FROM eb_rows")
        .await
        .expect("sum")
        .get_i64(0)
        .expect("decode")
        .unwrap_or(-1)
}

/// (a) N updates → N correct affected counts, every write applied.
#[tokio::test]
#[ignore = "requires local PG"]
async fn n_updates_return_correct_counts_and_all_apply() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    fresh(&mut c).await;
    // Seed ids 1..=5 with balance 0.
    c.execute_batch::<EbIns>((1..=5).map(|i| (i, 0_i64)))
        .await
        .expect("seed");
    // Update ids 1,2,3 (exist → 1 each) and id 99 (absent → 0).
    let counts = c
        .execute_batch::<EbUpd>(vec![(1_i64, 10_i64), (2, 20), (3, 30), (99, 40)])
        .await
        .expect("update batch");
    assert_eq!(counts, vec![1, 1, 1, 0], "per-command affected counts");
    // Balances applied.
    let sum = balance_sum(&mut c).await;
    assert_eq!(sum, 60, "10+20+30 applied");
    c.close().await.expect("close");
}

/// (b) THE AIRTIGHT PROOF: a mid-batch duplicate-PK failure applies NOTHING.
#[tokio::test]
#[ignore = "requires local PG"]
async fn mid_batch_failure_applies_nothing() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    fresh(&mut c).await;
    // Batch: ids 10,11,12,11(dup!),13 — command #3 violates the PK.
    let result = c
        .execute_batch::<EbIns>(vec![
            (10_i64, 1_i64),
            (11, 1),
            (12, 1),
            (11, 1), // duplicate PK → 23505 at command #3
            (13, 1),
        ])
        .await;
    match result {
        Err(DriverError::BatchFailed { index, ref source }) => {
            assert_eq!(index, 3, "the 4th command (index 3) failed");
            assert_eq!(source.code(), "23505", "duplicate key");
        }
        other => panic!("expected BatchFailed {{ index: 3 }}, got {other:?}"),
    }
    let err = result.expect_err("failed");
    assert_eq!(err.batch_failed_index(), Some(3));
    assert!(!err.is_disconnect(), "a per-command error is not a disconnect");
    // ALL-OR-NOTHING: the whole implicit transaction rolled back — ZERO rows.
    assert_eq!(row_count(&mut c).await, 0, "a mid-batch failure applied NOTHING");
    // The connection survived.
    assert_eq!(c.query_one::<EbSeven>(()).await.expect("reuse").n, 7);
    c.close().await.expect("close");
}

/// (c) COMMIT-TIME deferred-constraint failure → Db(23505), `batch_failed_index`
/// None (inherited commit-time-None fix), zero rows persisted.
#[tokio::test]
#[ignore = "requires local PG"]
async fn commit_time_deferred_failure_is_db_none_index() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    // Session-private TEMP TABLE (shadows the migration's permanent `eb_uniq`) so
    // parallel tests never interfere.
    c.execute_raw(
        "CREATE TEMP TABLE eb_uniq (id INTEGER PRIMARY KEY, tag INTEGER NOT NULL, \
         CONSTRAINT eb_uniq_tag_uniq UNIQUE (tag) DEFERRABLE INITIALLY DEFERRED)",
    )
    .await
    .expect("create temp");
    // Distinct ids, SAME tag → both Execute OK, the implicit COMMIT at the trailing
    // Sync fires the deferred UNIQUE (23505 attributable to no single command).
    let result = c
        .execute_batch::<EbDeferIns>(vec![(1_i32, 77_i32), (2, 77)])
        .await;
    match result {
        Err(DriverError::Db(ref e)) => assert_eq!(e.code(), "23505"),
        Err(DriverError::BatchFailed { index, .. }) => {
            panic!("commit-time failure must NOT be BatchFailed (named nonexistent #{index})")
        }
        other => panic!("expected Db(23505), got {other:?}"),
    }
    assert_eq!(
        result.expect_err("failed").batch_failed_index(),
        None,
        "commit-time → batch_failed_index None",
    );
    let n = c
        .query_one_raw("SELECT count(*)::int8 AS n FROM eb_uniq")
        .await
        .expect("count")
        .get_i64(0)
        .expect("decode")
        .unwrap_or(-1);
    assert_eq!(n, 0, "commit-time failure rolled the whole batch back");
    c.close().await.expect("close");
}

/// (d) NO auto-rollback: an IGNORED batch error inside a guard makes the next verb
/// fail loudly (25P02), never a silent autocommit.
#[tokio::test]
#[ignore = "requires local PG"]
async fn ignored_in_guard_batch_error_does_not_autocommit() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    fresh(&mut c).await;
    let outcome: Result<(), DriverError> = c
        .transaction(async |tx| {
            // A mid-batch failure inside the guard.
            let _ignored = tx
                .execute_batch::<EbIns>(vec![(1_i64, 1_i64), (1, 1)])
                .await; // duplicate PK → BatchFailed, IGNORED
            // The next verb must fail loudly with 25P02 (aborted transaction), never
            // run in silent autocommit.
            tx.execute_batch::<EbIns>(vec![(2_i64, 2_i64)]).await?;
            Ok(())
        })
        .await;
    // The next verb fails LOUDLY with 25P02 (aborted transaction), never a silent
    // autocommit. Because that verb is itself a batch, the 25P02 rides its command
    // #0 as `BatchFailed { index: 0, source: 25P02 }` (a batch's first command hits
    // the aborted transaction) — either wrapper is the loud aborted-tx signal.
    let code = match outcome {
        Err(DriverError::Db(ref e)) => e.code().to_string(),
        Err(DriverError::BatchFailed { ref source, .. }) => source.code().to_string(),
        other => panic!("expected a loud 25P02, got {other:?}"),
    };
    assert_eq!(code, "25P02", "the next in-guard verb fails loudly, never a silent autocommit");
    // Nothing committed.
    assert_eq!(row_count(&mut c).await, 0);
    c.close().await.expect("close");
}

/// (e) N == 0 (no wire I/O — the connection is untouched) and N == 1 (one command).
#[tokio::test]
#[ignore = "requires local PG"]
async fn zero_and_one() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    fresh(&mut c).await;
    let empty = c
        .execute_batch::<EbIns>(Vec::<(i64, i64)>::new())
        .await
        .expect("N=0");
    assert_eq!(empty, Vec::<u64>::new(), "N=0 → empty, no I/O");
    let one = c
        .execute_batch::<EbIns>(vec![(1_i64, 5_i64)])
        .await
        .expect("N=1");
    assert_eq!(one, vec![1], "N=1 → one count");
    assert_eq!(row_count(&mut c).await, 1);
    c.close().await.expect("close");
}

/// (f) LARGE N crossing MANY send windows: constant memory, every write applied,
/// deadlock-free. At N=20_000 the naive single-flush-drain-at-end design would
/// DEADLOCK (server response backlog + client send backlog both fill); the windowed
/// Flush+drain does not.
#[tokio::test]
#[ignore = "requires local PG"]
async fn large_n_windowed_is_correct_and_deadlock_free() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    fresh(&mut c).await;
    const N: i64 = 20_000;
    let counts = c
        .execute_batch::<EbIns>((0..N).map(|i| (i, i * 10)))
        .await
        .expect("large batch");
    assert_eq!(counts.len(), N as usize, "one count per command");
    assert!(counts.iter().all(|&r| r == 1), "each INSERT affected 1 row");
    // Every row applied, in one atomic transaction.
    assert_eq!(row_count(&mut c).await, N, "all N rows persisted");
    let sum = balance_sum(&mut c).await;
    // Expected = Σ (i*10) for i in 0..N — computed by iteration (no integer division).
    let expected: i64 = (0..N).map(|i| i * 10).sum();
    assert_eq!(sum, expected, "sum of the balances");
    // A mid-batch failure at large N still rolls the WHOLE thing back. The TEMP
    // `eb_rows` already exists on this connection, so CLEAR it (not re-create).
    c.execute_raw("TRUNCATE eb_rows").await.expect("truncate");
    let mut sets: Vec<(i64, i64)> = (0..N).map(|i| (i, 1)).collect();
    sets.push((5_000, 1)); // duplicate PK late in the run
    let result = c.execute_batch::<EbIns>(sets).await;
    assert!(matches!(result, Err(DriverError::BatchFailed { index, .. }) if index == N as usize));
    assert_eq!(row_count(&mut c).await, 0, "large mid-batch failure applied NOTHING");
    c.close().await.expect("close");
}

/// (Part B) The typed FLAGSHIP `execute::<Q>(params)` — SYMMETRIC with `query`,
/// derived from the carrier (no hand-passed `&Q::PREPARED`) and the N=1 sibling of
/// `execute_batch::<Q>`. An INSERT and an UPDATE return the correct affected count.
#[tokio::test]
#[ignore = "requires local PG"]
async fn typed_execute_returns_the_affected_count() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    fresh(&mut c).await;
    // INSERT one row → 1 affected (RETURNING rows are read-and-ignored).
    let inserted = c.execute::<EbIns>((1_i64, 100_i64)).await.expect("insert");
    assert_eq!(inserted, 1, "one INSERT affected 1 row");
    // UPDATE the existing row → 1; an absent id → 0.
    let updated = c.execute::<EbUpd>((1_i64, 5_i64)).await.expect("update");
    assert_eq!(updated, 1, "the UPDATE affected 1 row");
    let missed = c.execute::<EbUpd>((999_i64, 5_i64)).await.expect("update-absent");
    assert_eq!(missed, 0, "an absent id affects 0 rows");
    // The writes landed: balance 100 + 5 = 105.
    assert_eq!(balance_sum(&mut c).await, 105, "INSERT + UPDATE applied");
    c.close().await.expect("close");
}
