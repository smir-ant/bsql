//! LIVE homogeneous atomic bulk-QUERY batch over the ASYNC (tokio) driver.
//!
//! `conn.query_batch::<Q>(params_iter)` runs ONE compile-checked `query!` carrier
//! against N runtime parameter sets, Parse-once, ONE trailing `Sync` (one implicit
//! transaction), returning a GROUPED `Vec<Rows<Q>>` — one typed result per command,
//! KEEPING each command's RETURNING rows (the typed-RETURNING peer of
//! `execute_batch`, which discards them). End-to-end witnesses against REAL
//! PostgreSQL:
//!
//! - (a) N `INSERT ... RETURNING` → N grouped `Rows<Q>` with the correct DECODED
//!   RETURNING values AND every write applied;
//! - (b) GROUPING: a multi-row-per-command SELECT batch keeps each command's rows in
//!   ITS OWN `Rows<Q>` (a flattened result would lose this) — the reason the return
//!   type is `Vec<Rows<Q>>`, not one `Rows<Q>`;
//! - (c) THE AIRTIGHT PROOF: a mid-batch failure returns ZERO grouped results and
//!   applies NOTHING (zero rows persisted), `BatchFailed` naming the failing index —
//!   at SMALL and LARGE N;
//! - (d) N == 0 (no wire I/O) and N == 1 (equals a single `query`);
//! - (e) LARGE N crossing MANY send windows → constant memory, every result grouped
//!   + applied, deadlock-free (the naive single-flush design would hang at this N);
//! - (f) OID-GUARD DRIFT: a runtime result column diverging from the migration
//!   schema is a classified `BatchColumnOidMismatch` (verified ONCE on command 0),
//!   connection recovers;
//! - (g) CANCEL mid-batch → classified `57014`, connection recovers.
//!
//! Run: `cargo test -p bsql-query-fixture --test query_batch_live_async -- --ignored`
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

use std::time::{Duration, Instant};

use bsql::DecodeError;
use bsql_postgres_async::{ConnectConfig, Connection, DriverError, SslMode};

// Typed carriers over `qb_rows (id bigint pk, label text not null)`. An
// `INSERT ... RETURNING` carrier keeps its RETURNING rows on the query_batch path.
bsql::query!(
    QbIns,
    "INSERT INTO qb_rows (id, label) VALUES ($1, $2::text) RETURNING id, label"
);
// A pure SELECT whose row COUNT varies with the bound `$1` — the grouping witness.
bsql::query!(
    QbSel,
    "SELECT id, label FROM qb_rows WHERE id <= $1 ORDER BY id"
);
bsql::query!(QbSeven, "SELECT 7::int4 AS n");
// Over the shared `oidguard(tag text, …)` migration table — a TEMP shadow retyping
// `tag` to int4 is the drift the result-OID guard catches.
bsql::query!(QbTag, "SELECT tag FROM oidguard");
// A carrier that sleeps 3s — the cancel witness (command 0 in flight when the token
// fires).
bsql::query!(QbSleep, "SELECT pg_sleep(3)::text AS s");

const OID_TEXT: u32 = 25;
const OID_INT4: u32 = 23;

fn cfg() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// Fresh empty per-connection TEMP `qb_rows` (session-private) shadowing the
/// migration's permanent table — same columns the carrier was validated against —
/// so each test on its OWN connection never interferes under cargo parallelism.
async fn fresh(c: &mut Connection) {
    c.execute_raw("CREATE TEMP TABLE qb_rows (id BIGINT PRIMARY KEY, label TEXT NOT NULL)")
        .await
        .expect("create temp");
}

async fn row_count(c: &mut Connection) -> i64 {
    c.query_one_raw("SELECT count(*)::int8 AS n FROM qb_rows")
        .await
        .expect("count")
        .get_i64(0)
        .expect("decode")
        .unwrap_or(-1)
}

/// (a) N INSERT...RETURNING → N grouped `Rows<Q>` with the correct DECODED values.
#[tokio::test]
#[ignore = "requires local PG"]
async fn n_inserts_return_grouped_decoded_rows_and_all_apply() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    fresh(&mut c).await;
    let labels = ["alpha", "beta", "gamma", "delta"];
    let grouped = c
        .query_batch::<QbIns>(
            labels.iter().enumerate().map(|(i, l)| ((i as i64) + 1, *l)),
        )
        .await
        .expect("insert batch");
    // One grouped Rows<Q> per command, in order, each with its single RETURNING row.
    assert_eq!(grouped.len(), labels.len(), "one Rows<Q> per command");
    for (i, rows) in grouped.iter().enumerate() {
        let rec = rows.iter().next().expect("one returning row").expect("decode");
        assert_eq!(rec.id, (i as i64) + 1, "returned id");
        assert_eq!(rec.label, labels[i], "returned label DECODED, not just counted");
        assert_eq!(rows.len(), 1, "each INSERT RETURNING yields exactly one row");
    }
    // Every write applied in one atomic transaction.
    assert_eq!(row_count(&mut c).await, labels.len() as i64, "all applied");
    c.close().await.expect("close");
}

/// (b) GROUPING: a multi-row-per-command SELECT batch keeps each command's rows in
/// ITS OWN `Rows<Q>`. A flattened single `Rows<Q>` would make "which rows belong to
/// which param set" unrecoverable — the reason the return type is `Vec<Rows<Q>>`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn grouping_is_preserved_per_command() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    fresh(&mut c).await;
    c.query_batch::<QbIns>((1..=5).map(|i| (i, "x")))
        .await
        .expect("seed 5");
    // Three SELECT commands with $1 = 1, 3, 5 → row counts 1, 3, 5 respectively.
    let grouped = c
        .query_batch::<QbSel>(vec![(1_i64,), (3,), (5,)])
        .await
        .expect("select batch");
    assert_eq!(grouped.len(), 3, "one Rows<Q> per command");
    assert_eq!(grouped[0].len(), 1, "id <= 1 → 1 row (grouping intact)");
    assert_eq!(grouped[1].len(), 3, "id <= 3 → 3 rows (grouping intact)");
    assert_eq!(grouped[2].len(), 5, "id <= 5 → 5 rows (grouping intact)");
    // Each group's rows are the right ids (proves no cross-command bleed).
    let ids2: Vec<i64> = grouped[1].iter().map(|r| r.expect("decode").id).collect();
    assert_eq!(ids2, vec![1, 2, 3], "command #1's own rows");
    c.close().await.expect("close");
}

/// (c) THE AIRTIGHT PROOF (small N): a mid-batch duplicate-PK failure returns ZERO
/// grouped results and applies NOTHING; `BatchFailed` names the failing index.
#[tokio::test]
#[ignore = "requires local PG"]
async fn mid_batch_failure_returns_zero_results_and_applies_nothing() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    fresh(&mut c).await;
    // ids 10,11,12,11(dup!),13 — command #3 violates the PK.
    let result = c
        .query_batch::<QbIns>(vec![
            (10_i64, "a"),
            (11, "b"),
            (12, "c"),
            (11, "d"), // duplicate PK → 23505 at command #3
            (13, "e"),
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
    // ALL-OR-NOTHING: the whole implicit transaction rolled back — ZERO rows, and
    // ZERO grouped results were built (the error carries none).
    assert_eq!(row_count(&mut c).await, 0, "a mid-batch failure applied NOTHING");
    // The connection survived + is reusable.
    assert_eq!(c.query_one::<QbSeven>(()).await.expect("reuse").n, 7);
    c.close().await.expect("close");
}

/// (d) N == 0 (no wire I/O — the connection is untouched) and N == 1 (one command,
/// equals a single `query`).
#[tokio::test]
#[ignore = "requires local PG"]
async fn zero_and_one() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    fresh(&mut c).await;
    let empty = c
        .query_batch::<QbIns>(Vec::<(i64, &str)>::new())
        .await
        .expect("N=0");
    assert!(empty.is_empty(), "N=0 → empty Vec, no I/O");
    let one = c
        .query_batch::<QbIns>(vec![(1_i64, "solo")])
        .await
        .expect("N=1");
    assert_eq!(one.len(), 1, "N=1 → one grouped result");
    let rec = one[0].iter().next().expect("row").expect("decode");
    assert_eq!((rec.id, rec.label), (1, "solo"));
    assert_eq!(row_count(&mut c).await, 1);
    c.close().await.expect("close");
}

/// (e) LARGE N crossing MANY send windows: constant memory, every result grouped +
/// applied, deadlock-free. At N=20_000 the naive single-flush-drain-at-end design
/// would DEADLOCK (server response backlog + client send backlog both fill); the
/// windowed Flush+drain does not. Also proves a LARGE mid-batch failure rolls the
/// WHOLE thing back with ZERO results.
#[tokio::test]
#[ignore = "requires local PG"]
async fn large_n_windowed_is_correct_and_deadlock_free() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    fresh(&mut c).await;
    const N: i64 = 20_000;
    let grouped = c
        .query_batch::<QbIns>((0..N).map(|i| (i, "bulk")))
        .await
        .expect("large batch");
    assert_eq!(grouped.len(), N as usize, "one grouped result per command");
    // Every command returned its single row, decoded, with the right id.
    for (i, rows) in grouped.iter().enumerate() {
        let rec = rows.iter().next().expect("row").expect("decode");
        assert_eq!(rec.id, i as i64, "returned id at command {i}");
    }
    assert_eq!(row_count(&mut c).await, N, "all N rows persisted");
    // A LARGE mid-batch failure still rolls the WHOLE thing back. The TEMP `qb_rows`
    // already exists on this connection, so CLEAR it (not re-create).
    c.execute_raw("TRUNCATE qb_rows").await.expect("truncate");
    let mut sets: Vec<(i64, &str)> = (0..N).map(|i| (i, "x")).collect();
    sets.push((5_000, "dup")); // duplicate PK late in the run
    let result = c.query_batch::<QbIns>(sets).await;
    assert!(matches!(result, Err(DriverError::BatchFailed { index, .. }) if index == N as usize));
    assert_eq!(row_count(&mut c).await, 0, "large mid-batch failure applied NOTHING");
    c.close().await.expect("close");
}

/// (f) OID-GUARD DRIFT: a per-connection TEMP shadow of `oidguard` retypes `tag`
/// (typed `text`) to `int4`, so `query_batch::<QbTag>` decoding it would silently
/// mis-decode. The guard — verified ONCE on command 0 (homogeneity: all N reuse the
/// one plan) — fires a classified `BatchColumnOidMismatch { command: 0 }`, connection
/// recovers. A matching shadow decodes correctly (no false positive).
#[tokio::test]
#[ignore = "requires local PG"]
async fn oid_guard_drift_is_a_classified_batch_column_oid_mismatch() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    // Drift shadow: `tag` retyped to int4.
    c.execute_raw("CREATE TEMP TABLE oidguard (tag int4 NOT NULL, vc varchar NOT NULL, bp bpchar NOT NULL, n int4 NOT NULL)")
        .await
        .expect("create drift shadow");
    c.execute_raw("INSERT INTO oidguard (tag, vc, bp, n) VALUES (1094795585, 'v', 'b', 1)")
        .await
        .expect("seed drift");
    // A homogeneous batch of the SAME carrier — the guard fires on command 0's MISS
    // Describe, before ANY row decodes.
    match c.query_batch::<QbTag>(vec![(), ()]).await {
        Err(e @ DriverError::BatchColumnOidMismatch { .. }) => {
            assert_eq!(e.batch_failed_index(), Some(0), "verified ONCE on command 0");
            assert!(!e.is_disconnect(), "a schema drift is not a disconnect");
            match &e {
                DriverError::BatchColumnOidMismatch {
                    command,
                    source: DecodeError::ColumnOidMismatch { index, expected, found },
                } => {
                    assert_eq!(*command, 0);
                    assert_eq!(*index, 0, "the drifting column is result column 0");
                    assert_eq!(*expected, OID_TEXT, "migration typed `tag` as text (25)");
                    assert_eq!(*found, OID_INT4, "the live TEMP column is int4 (23)");
                }
                other => panic!("expected a ColumnOidMismatch source, got {other:?}"),
            }
        }
        other => panic!("expected BatchColumnOidMismatch, got {other:?}"),
    }
    // The connection drained to a clean idle — it is REUSABLE.
    assert_eq!(c.query_one::<QbSeven>(()).await.expect("recovers").n, 7);
    c.close().await.expect("close");
}

/// (f2) OID-guard NO false positive: a matching-typed shadow decodes correctly.
#[tokio::test]
#[ignore = "requires local PG"]
async fn oid_guard_matching_shadow_decodes_correctly() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    c.execute_raw("CREATE TEMP TABLE oidguard (tag text NOT NULL, vc varchar NOT NULL, bp bpchar NOT NULL, n int4 NOT NULL)")
        .await
        .expect("create match shadow");
    c.execute_raw("INSERT INTO oidguard (tag, vc, bp, n) VALUES ('hello', 'v', 'b', 1)")
        .await
        .expect("seed match");
    let grouped = c.query_batch::<QbTag>(vec![(), ()]).await.expect("matching batch runs");
    assert_eq!(grouped.len(), 2);
    for rows in &grouped {
        assert_eq!(rows.iter().next().expect("row").expect("decode").tag, "hello");
    }
    c.close().await.expect("close");
}

/// (g) CANCEL mid-batch: a `cancel_token` obtained BEFORE the batch is moved to a
/// task that cancels the in-flight (sleeping) command → classified `57014`, and the
/// connection is left drained + reusable.
#[tokio::test]
#[ignore = "requires local PG"]
async fn cancel_mid_batch_is_57014_and_connection_recovers() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    let token = c.cancel_token();
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(token.cancel().await);
    });
    // Command #0 sleeps 3s; the cancel fires at ~300ms.
    let started = Instant::now();
    let result = c.query_batch::<QbSleep>(vec![(), ()]).await;
    let elapsed = started.elapsed();
    drop(canceller.await);
    assert!(elapsed < Duration::from_secs(2), "cancel bounded the batch, took {elapsed:?}");
    match result {
        Err(DriverError::BatchFailed { source, .. }) => {
            assert_eq!(source.code(), "57014", "query_canceled");
        }
        other => panic!("expected a 57014 BatchFailed, got {other:?}"),
    }
    // A cancel is NOT a disconnect — the connection is reusable.
    assert_eq!(c.query_one::<QbSeven>(()).await.expect("reuse after cancel").n, 7);
    c.close().await.expect("close");
}
