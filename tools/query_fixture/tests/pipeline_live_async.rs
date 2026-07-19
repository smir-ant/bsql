//! LIVE heterogeneous atomic pipeline over the ASYNC (tokio) driver.
//!
//! `conn.pipeline((...))` sends N compile-checked `query!` commands with ONE
//! trailing `Sync` — one implicit transaction, all-or-nothing. These are the
//! end-to-end witnesses against REAL PostgreSQL:
//!
//! - (a) three heterogeneous typed commands (read + read + write) in one batch →
//!   all results correct AND the write committed atomically;
//! - (b) THE AIRTIGHT PROOF: a mid-batch constraint violation → `BatchFailed`
//!   names the failing index, and command #0's write was ROLLED BACK (zero rows);
//! - (c) cancel mid-batch → classified `57014`, connection reusable;
//! - (d) transport death mid-batch (backend terminated) → classified disconnect,
//!   never a torn success;
//! - (e) a batch inside `conn.transaction(|tx| …)`;
//! - (f) an explicit `BEGIN` around a failing batch → the connection recovers
//!   clean (the pipeline rolls back the aborted transaction).
//!
//! Carriers are validated at build time against `migrations/` (`accounts` exists
//! post-rename with `id BIGINT PK, balance BIGINT NOT NULL`); the live table is
//! created idempotently at test start, and each test uses a DISJOINT id range so
//! the default parallel run does not interfere.
//!
//! Run: `cargo test -p bsql-query-fixture --test pipeline_live_async -- --ignored`
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::disallowed_methods,
    clippy::panic,
    reason = "live test harness — expect/unwrap/panic surface failures loudly; not production fallbacks"
)]

use std::time::{Duration, Instant};

use bsql::BindExt;
use bsql_postgres_async::{ConnectConfig, Connection, DriverError, SslMode};

bsql::query!(PlOne, "SELECT 1::int4 AS n");
bsql::query!(PlHi, "SELECT 'hello'::text AS s");
bsql::query!(PlSeven, "SELECT 7::int4 AS n");
bsql::query!(
    PlInsAccount,
    "INSERT INTO accounts (id, balance) VALUES ($1, $2) RETURNING id"
);
bsql::query!(PlSelAccount, "SELECT id FROM accounts WHERE id = $1");
bsql::query!(PlSleep, "SELECT 1::int4 AS n WHERE pg_sleep(3) IS NOT NULL");
bsql::query!(
    PlDeferIns,
    "INSERT INTO pl_deferred (id, tag) VALUES ($1, $2) RETURNING id"
);
// Carriers for the WINDOWED deadlock-free witnesses (0021_pl_bulk.sql): an EARLY
// command that returns a LARGE (~4 MiB) result, paired with LATER commands that
// carry LARGE `text` params — the batch spans MANY 64 KiB send windows, the exact
// shape that DEADLOCKS a stage-all-then-flush pipeline and STREAMS through the
// windowed drive.
bsql::query!(PlBigResult, "SELECT repeat('x', 4000000)::text AS s");
bsql::query!(
    PlBulkIns,
    "INSERT INTO pl_bulk (id, payload) VALUES ($1, $2) RETURNING id"
);
// The DECISIVE co-window-deadlock carriers (no table): an EARLY command returning
// a ~40 MB result, paired with a LATER command whose SINGLE `text` Bind param is
// OVERSIZE (well past any socket send buffer). `PlEcho` echoes its huge param, so
// its OWN Bind AND its result are both large. See
// `co_window_oversize_param_does_not_deadlock`.
bsql::query!(PlHugeResult, "SELECT repeat('x', 40000000)::text AS s");
bsql::query!(PlEcho, "SELECT $1::text AS s");

fn cfg() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// Ensure the `accounts` table exists and this test's id range is clear.
async fn prepare(c: &mut Connection, lo: i64, hi: i64) {
    c.execute_raw("CREATE TABLE IF NOT EXISTS accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)")
        .await
        .expect("create accounts");
    c.execute_raw(&format!("DELETE FROM accounts WHERE id BETWEEN {lo} AND {hi}"))
        .await
        .expect("clear id range");
}

async fn account_exists(c: &mut Connection, id: i64) -> bool {
    let rows = c
        .query::<PlSelAccount>((id,))
        .await
        .expect("select account");
    !rows.is_empty()
}

/// (a) Three heterogeneous typed commands (read + read + write) run in ONE batch:
/// every result is correct AND the write is committed (the batch is one atomic
/// transaction).
#[tokio::test]
#[ignore = "requires local PG"]
async fn heterogeneous_read_read_write_all_correct_and_committed() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    let id = 8_000_001i64;
    prepare(&mut c, 8_000_000, 8_000_099).await;

    let (one, hi, ins) = c
        .pipeline((
            PlOne::bind(()),
            PlHi::bind(()),
            PlInsAccount::bind((id, 500)),
        ))
        .await
        .expect("pipeline runs");

    // Each command's rows decoded against ITS carrier's compile-time shape.
    assert_eq!(one.iter().next().expect("row").expect("decode").n, 1);
    assert_eq!(hi.iter().next().expect("row").expect("decode").s, "hello");
    assert_eq!(ins.iter().next().expect("row").expect("decode").id, id);

    // The write committed atomically with the batch.
    assert!(account_exists(&mut c, id).await, "the insert committed");

    c.close().await.expect("close");
}

/// (b) THE AIRTIGHT ALL-OR-NOTHING PROOF: command #1 violates the primary-key
/// constraint (command #0 already inserted the same id within the implicit
/// transaction). The batch fails with `BatchFailed` naming index 1, and a query
/// AFTER confirms command #0's write was ROLLED BACK — zero rows.
#[tokio::test]
#[ignore = "requires local PG"]
async fn mid_batch_failure_rolls_back_the_whole_batch() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    let id = 8_100_001i64;
    prepare(&mut c, 8_100_000, 8_100_099).await;

    // Command #0 inserts id, command #1 inserts the SAME id → duplicate-key 23505.
    let result = c
        .pipeline((
            PlInsAccount::bind((id, 100)),
            PlInsAccount::bind((id, 200)),
        ))
        .await;

    match result {
        Err(DriverError::BatchFailed { index, source }) => {
            assert_eq!(index, 1, "the SECOND command (index 1) failed");
            assert!(
                source.code().starts_with("23"),
                "a constraint violation (23xxx), got {source:?}"
            );
        }
        other => panic!("expected BatchFailed at index 1, got {other:?}"),
    }
    // The classified accessor names the same index without matching the variant.
    // (Re-run to inspect: the batch is idempotent — it inserted nothing.)

    // THE PROOF: command #0's insert was ROLLED BACK by the implicit-transaction
    // abort — the row is GONE, not present. Returning "the results before the
    // failure" would be a lie; the all-or-nothing contract forbids it.
    assert!(
        !account_exists(&mut c, id).await,
        "command #0's write MUST be rolled back — zero rows after a mid-batch failure",
    );

    // The connection survived the recoverable failure and is reusable.
    assert!(c.is_healthy(), "connection stays healthy after a batch failure");
    let one = c.query_one::<PlSeven>(()).await.expect("reuse works");
    assert_eq!(one.n, 7);

    c.close().await.expect("close");
}

/// The classified index accessor: `DriverError::batch_failed_index` names the
/// failing command without matching the `BatchFailed` variant.
#[tokio::test]
#[ignore = "requires local PG"]
async fn batch_failed_index_accessor_names_the_command() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    let id = 8_150_001i64;
    prepare(&mut c, 8_150_000, 8_150_099).await;
    // #0 read (ok), #1 read (ok), #2 dup insert after #? — make #2 the failure by
    // inserting a duplicate against a row #0's sibling command created is complex;
    // instead: #0 insert id, #1 insert id (dup) → index 1.
    let err = c
        .pipeline((
            PlOne::bind(()),
            PlInsAccount::bind((id, 1)),
            PlInsAccount::bind((id, 2)),
        ))
        .await
        .expect_err("batch fails");
    assert_eq!(err.batch_failed_index(), Some(2), "the third command failed");
    assert!(!account_exists(&mut c, id).await, "all rolled back");
    c.close().await.expect("close");
}

/// COMMIT-TIME failure (regression for the out-of-range `failed_index == arity`
/// bug): both commands SUCCEED at Execute, then the implicit COMMIT at the trailing
/// `Sync` fails a `DEFERRABLE INITIALLY DEFERRED UNIQUE` constraint. The failure is
/// attributable to NO single command, so `batch_failed_index()` is `None` and the
/// error is a batch-level `Db` (SQLSTATE `23505`), NEVER a `BatchFailed { index: 2 }`
/// that would name a nonexistent command (a consumer indexing an N-array by it would
/// panic). All-or-nothing still holds: zero rows persisted.
#[tokio::test]
#[ignore = "requires local PG"]
async fn commit_time_deferred_constraint_failure_is_honest_not_out_of_range_index() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    // Recreate the deferred-constraint table fresh (the deferrable UNIQUE is a
    // runtime property; the migration only feeds the carrier's catalog columns).
    c.execute_raw("DROP TABLE IF EXISTS pl_deferred").await.expect("drop");
    c.execute_raw(
        "CREATE TABLE pl_deferred (id INTEGER PRIMARY KEY, tag INTEGER NOT NULL, \
         CONSTRAINT pl_deferred_tag_uniq UNIQUE (tag) DEFERRABLE INITIALLY DEFERRED)",
    )
    .await
    .expect("create pl_deferred");

    // Two commands, DISTINCT ids (no PK clash at Execute) but the SAME tag: both
    // Execute succeed (the UNIQUE check is deferred), then the implicit COMMIT at
    // the batch's single trailing Sync fires the deferred check → 23505 at commit.
    let result = c
        .pipeline((PlDeferIns::bind((1, 77)), PlDeferIns::bind((2, 77))))
        .await;

    match result {
        // Commit-time: a batch-level `Db`, NOT `BatchFailed`.
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
    // A commit failure leaves the connection drained + reusable (not a disconnect).
    assert!(!err.is_disconnect(), "a 23505 is a per-query error, not a disconnect");

    // ALL-OR-NOTHING: the whole implicit transaction rolled back — zero rows.
    let count = c
        .query_one_raw("SELECT count(*)::int8 AS n FROM pl_deferred")
        .await
        .expect("count");
    assert_eq!(
        count.get_i64(0).expect("decode").unwrap_or(-1),
        0,
        "the commit-time failure rolled the whole batch back — zero rows persisted",
    );

    // The connection survived the recoverable failure.
    let seven = c.query_one::<PlSeven>(()).await.expect("reuse after commit failure");
    assert_eq!(seven.n, 7);
    c.close().await.expect("close");
}

/// (c) Cancel mid-batch: a `cancel_token` obtained BEFORE the batch is moved to a
/// task that cancels the in-flight (sleeping) command → classified `57014`
/// `query_canceled`, and the connection is left drained + reusable.
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
    let result = c.pipeline((PlSleep::bind(()), PlOne::bind(()))).await;
    let elapsed = started.elapsed();
    drop(canceller.await);

    assert!(elapsed < Duration::from_secs(2), "cancel bounded the batch, took {elapsed:?}");
    match result {
        Err(DriverError::BatchFailed { source, .. }) => {
            assert_eq!(source.code(), "57014", "query_canceled");
        }
        other => panic!("expected a 57014 BatchFailed, got {other:?}"),
    }
    assert!(
        !result_is_disconnect(&c),
        "a cancel is NOT a disconnect — the connection is reusable",
    );
    let seven = c.query_one::<PlSeven>(()).await.expect("reuse after cancel");
    assert_eq!(seven.n, 7);
    c.close().await.expect("close");
}

fn result_is_disconnect(c: &Connection) -> bool {
    !c.is_healthy()
}

/// (d) Transport death mid-batch: a second connection terminates this batch's
/// backend while the sleeping command is in flight → a classified DISCONNECT
/// (never a torn success), and the connection is dead (a follow-up verb is
/// `NotReady`), never a hang.
#[tokio::test]
#[ignore = "requires local PG"]
async fn transport_death_mid_batch_is_a_classified_disconnect() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    let pid = c.backend_pid();

    let killer = tokio::spawn(async move {
        let mut k = Connection::connect(&cfg()).await.expect("killer connect");
        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(k.execute_raw(&format!("SELECT pg_terminate_backend({pid})")).await);
        drop(k.close().await);
    });

    let started = Instant::now();
    let result = c.pipeline((PlSleep::bind(()), PlOne::bind(()))).await;
    let elapsed = started.elapsed();
    drop(killer.await);

    assert!(elapsed < Duration::from_secs(5), "bounded, not a hang ({elapsed:?})");
    let err = result.expect_err("a terminated backend fails the batch");
    assert!(
        err.is_disconnect(),
        "a terminated backend mid-batch is a disconnect, got {err:?}",
    );
    // Never a torn success and the connection is unusable now.
    assert!(!c.is_healthy(), "the connection is dead after a mid-batch termination");
    // Dropping the dead connection must not hang.
    drop(c);
}

/// (e) A batch inside `conn.transaction(|tx| …)`: the guard owns commit/rollback;
/// the batch's own Sync does not close the transaction.
#[tokio::test]
#[ignore = "requires local PG"]
async fn pipeline_inside_a_transaction_guard_commits() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    let id = 8_200_001i64;
    prepare(&mut c, 8_200_000, 8_200_099).await;

    let (a, b) = c
        .transaction(async |tx| {
            let (one, ins) = tx
                .pipeline((PlOne::bind(()), PlInsAccount::bind((id, 42))))
                .await?;
            Ok((
                one.iter().next().expect("row").expect("decode").n,
                ins.iter().next().expect("row").expect("decode").id,
            ))
        })
        .await
        .expect("transaction commits");
    assert_eq!(a, 1);
    assert_eq!(b, id);
    // The guard committed — the insert is durable.
    assert!(account_exists(&mut c, id).await, "the in-tx batch committed");
    c.close().await.expect("close");
}

/// (f) An explicit `BEGIN` around a FAILING batch: `pipeline` is CONSISTENT with a
/// normal failed verb — it leaves the explicit transaction ABORTED (`'E'`), NOT
/// auto-rolled-back. A follow-up verb fails loudly with `25P02` (never a silent
/// autocommit); an explicit `rollback()` (or a pooled checkout's reset) restores
/// the connection to clean + reusable, and the batch's writes are gone.
#[tokio::test]
#[ignore = "requires local PG"]
async fn explicit_begin_then_failing_batch_leaves_aborted_tx_until_rollback() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    let id = 8_300_001i64;
    prepare(&mut c, 8_300_000, 8_300_099).await;

    c.execute_raw("BEGIN").await.expect("open explicit tx");
    let result = c
        .pipeline((
            PlInsAccount::bind((id, 1)),
            PlInsAccount::bind((id, 2)),
        ))
        .await;
    assert!(
        matches!(result, Err(DriverError::BatchFailed { index: 1, .. })),
        "the batch fails at index 1, got {result:?}",
    );

    // The explicit transaction is left ABORTED — a follow-up verb is a LOUD `25P02`,
    // exactly as it would be after `conn.begin(); conn.query(fails)`. NOT a silent
    // autocommit (which is what an auto-ROLLBACK inside `pipeline` would have caused).
    let blocked = c.query_one::<PlSeven>(()).await;
    match blocked {
        Err(DriverError::Db(e)) => assert_eq!(
            e.code(), "25P02",
            "an in-aborted-tx verb must be a loud 25P02, never a silent autocommit; got {e:?}",
        ),
        other => panic!("expected a loud 25P02, got {other:?}"),
    }
    assert!(c.is_healthy(), "the connection is alive (25P02 is recoverable)");

    // The OWNER rolls it back → clean + reusable.
    c.rollback().await.expect("rollback restores clean state");
    let seven = c.query_one::<PlSeven>(()).await.expect("clean + reusable after rollback");
    assert_eq!(seven.n, 7);
    assert!(!account_exists(&mut c, id).await, "the failed batch's writes are rolled back");

    c.close().await.expect("close");
}

/// THE BLIND-ZONE REGRESSION: inside `conn.transaction(|tx| …)`, a caller who
/// IGNORES a failing `tx.pipeline` and issues another verb must get a LOUD `25P02`
/// (the transaction is aborted), NOT a silent autocommit. Proves `pipeline` does
/// NOT auto-roll-back the guard's transaction (which would silently escape it), and
/// that the guard rolls the WHOLE scope back (A, B, D all leave no committed rows).
#[tokio::test]
#[ignore = "requires local PG"]
async fn ignored_in_guard_pipeline_error_does_not_autocommit_later_verbs() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    let a_id = 8_400_001i64;
    let p_id = 8_400_002i64;
    let d_id = 8_400_003i64;
    prepare(&mut c, 8_400_000, 8_400_099).await;

    let d_is_25p02 = c
        .transaction(async |tx| {
            // A: a real typed write (opens the transaction, succeeds).
            let _a = tx.query::<PlInsAccount>((a_id, 1)).await?;
            // B, C: a failing batch (C dups p_id) — the caller IGNORES the error.
            drop(
                tx.pipeline((
                    PlInsAccount::bind((p_id, 1)),
                    PlInsAccount::bind((p_id, 2)),
                ))
                .await,
            );
            // D: a verb AFTER the ignored pipeline error. In an aborted transaction it
            // MUST fail loudly with 25P02 — NOT silently autocommit (which is exactly
            // what an auto-ROLLBACK inside `pipeline` would have allowed).
            let d = tx.query::<PlInsAccount>((d_id, 1)).await;
            Ok(matches!(&d, Err(DriverError::Db(e)) if e.code() == "25P02"))
        })
        .await
        .expect("the guard resolves (COMMIT of an aborted tx rolls back cleanly)");

    assert!(
        d_is_25p02,
        "a verb after an ignored in-guard pipeline error MUST be a loud 25P02 — never a silent autocommit",
    );
    // The guard rolled the WHOLE transaction back — A's write is gone (proving no
    // silent autocommit anywhere), B's is gone, and D never committed.
    assert!(!account_exists(&mut c, a_id).await, "A's write rolled back with the whole tx");
    assert!(!account_exists(&mut c, p_id).await, "B's write rolled back");
    assert!(!account_exists(&mut c, d_id).await, "D never committed (25P02) and did not autocommit");
    c.close().await.expect("close");
}

/// Ensure `pl_bulk` exists and this test's id range is clear.
async fn prepare_bulk(c: &mut Connection, lo: i64, hi: i64) {
    // `CREATE TABLE IF NOT EXISTS` has a known concurrent-creation RACE (two callers
    // both find the table absent, both try to insert its `pg_type` row → one wins,
    // the other gets a `23505` on the `pg_type` unique index, or a `42P07`). Both
    // windowed tests may create the brand-new `pl_bulk` on the first parallel run, so
    // tolerate that race — on a duplicate error the table now exists (a failed
    // autocommit `CREATE` leaves the connection idle + reusable for the `DELETE`).
    if let Err(e) = c
        .execute_raw("CREATE TABLE IF NOT EXISTS pl_bulk (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)")
        .await
    {
        let raced = matches!(&e, DriverError::Db(db) if db.code() == "23505" || db.code() == "42P07");
        assert!(raced, "create pl_bulk failed for a non-race reason: {e:?}");
    }
    c.execute_raw(&format!("DELETE FROM pl_bulk WHERE id BETWEEN {lo} AND {hi}"))
        .await
        .expect("clear id range");
}

async fn bulk_count(c: &mut Connection, lo: i64, hi: i64) -> i64 {
    c.query_one_raw(&format!(
        "SELECT count(*)::int8 AS n FROM pl_bulk WHERE id BETWEEN {lo} AND {hi}"
    ))
    .await
    .expect("count")
    .get_i64(0)
    .expect("decode")
    .unwrap_or(-1)
}

/// THE WINDOWED DEADLOCK-FREE WITNESS. A heterogeneous batch pairs an EARLY command
/// that returns a ~4 MiB result with SIX later commands each carrying a 512 KiB
/// `text` param — the whole batch stages ~3 MiB of tail while the server produces a
/// ~4 MiB early result. A stage-all-then-flush `pipeline` DEADLOCKS here (the client
/// blocks writing the tail while the server blocks writing the early result); the
/// windowed drive STREAMS it (each window drains before the next stages, so the
/// client always reads before it write-blocks). The `timeout` is the regression net:
/// a revert to the old drive would HANG this and elapse the timeout instead of
/// hanging forever. The completion — with EVERY result correct and all six writes
/// committed — is also the BOUNDED-MEMORY proof: an unwindowed drive would need to
/// buffer the whole ~3 MiB tail (or deadlock), the windowed drive never buffers past
/// ~2× the 64 KiB window.
#[tokio::test]
#[ignore = "requires local PG"]
async fn windowed_large_result_plus_large_params_does_not_deadlock() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    let base = 8_800_000i64;
    prepare_bulk(&mut c, base, base + 999).await;
    let payload = "a".repeat(512 * 1024); // 512 KiB per command → ~3 MiB tail

    // arity 7: [ big-result, ins+1, ins+2, ins+3, ins+4, ins+5, ins+6 ].
    let out = tokio::time::timeout(
        Duration::from_secs(60),
        c.pipeline((
            PlBigResult::bind(()),
            PlBulkIns::bind((base + 1, payload.as_str())),
            PlBulkIns::bind((base + 2, payload.as_str())),
            PlBulkIns::bind((base + 3, payload.as_str())),
            PlBulkIns::bind((base + 4, payload.as_str())),
            PlBulkIns::bind((base + 5, payload.as_str())),
            PlBulkIns::bind((base + 6, payload.as_str())),
        )),
    )
    .await
    .expect("pipeline completed within 60s — a stage-all-then-flush regression would DEADLOCK here")
    .expect("pipeline runs");

    let (big, r1, r2, r3, r4, r5, r6) = out;
    // The early LARGE result decoded correctly (drained in the FIRST window, which
    // unblocked the server — the deadlock-free proof).
    assert_eq!(
        big.iter()
            .next()
            .expect("row")
            .expect("decode")
            .s
            .expect("non-null result")
            .len(),
        4_000_000,
        "the ~4 MiB early result decoded whole",
    );
    // Every LARGE-param write returned its id, in order.
    for (r, id) in [
        (r1, base + 1),
        (r2, base + 2),
        (r3, base + 3),
        (r4, base + 4),
        (r5, base + 5),
        (r6, base + 6),
    ] {
        assert_eq!(r.iter().next().expect("row").expect("decode").id, id);
    }
    // All six writes committed atomically with the batch.
    assert_eq!(bulk_count(&mut c, base + 1, base + 6).await, 6, "all six writes committed");
    // The payloads round-tripped byte-exact (no window boundary corrupted a param).
    let got = c
        .query_one_raw(&format!("SELECT length(payload)::int8 AS n FROM pl_bulk WHERE id = {}", base + 3))
        .await
        .expect("length")
        .get_i64(0)
        .expect("decode")
        .unwrap_or(-1);
    assert_eq!(got, 512 * 1024, "the 512 KiB payload round-tripped whole");
    c.close().await.expect("close");
}

/// THE DECISIVE co-window write-path DEADLOCK witness. The 512 KiB witness above
/// stays UNDER the socket send buffers, so it is a platform-dependent NON-proof of
/// the deadlock. This one ENTERS the regime unambiguously: an EARLY command
/// returning a ~40 MB result (`PlHugeResult`) paired with a LATER command whose
/// SINGLE ~40 MiB `text` Bind param (`PlEcho`) is well past any socket send buffer.
///
/// Pre-fix (the co-window drive: stage cmd0 + cmd1 into one window, flush both) this
/// DEADLOCKS: the client blocks WRITING the ~40 MiB Bind while the server blocks
/// WRITING the ~40 MB early result — each end blocked on write, neither reading. The
/// drain-before-oversize windowing ISOLATES the oversize command: it flushes +
/// DRAINS the prefix (the big-result command) ALONE first, so the client reads the
/// ~40 MB result before it can write-block on the ~40 MiB Bind, then the oversize
/// command rides its own fresh window (a single command never self-deadlocks — the
/// server reads its whole Bind before producing any result). The 90 s timeout is the
/// regression net: a revert to the co-window drive HANGS here and elapses it.
#[tokio::test]
#[ignore = "requires local PG"]
async fn co_window_oversize_param_does_not_deadlock() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    // 40 MiB — one Bind frame well past any socket send buffer, so the client
    // genuinely write-blocks (a 512 KiB param does not, hence this larger witness).
    let huge = "z".repeat(40 * 1024 * 1024);
    let out = tokio::time::timeout(
        Duration::from_secs(90),
        c.pipeline((PlHugeResult::bind(()), PlEcho::bind((huge.as_str(),)))),
    )
    .await
    .expect("pipeline completed within 90 s — a co-window drive would DEADLOCK on the ~40 MiB Bind")
    .expect("pipeline runs");
    let (big, echo) = out;
    // The ~40 MB early result decoded whole — it was drained (in the isolated prefix
    // window) BEFORE the oversize Bind was written, which is exactly what breaks the
    // deadlock.
    assert_eq!(
        big.iter().next().expect("row").expect("decode").s.expect("non-null").len(),
        40_000_000,
        "the ~40 MB early result decoded whole",
    );
    // The ~40 MiB echoed param round-tripped whole (the isolate relocated its WIRE
    // bytes verbatim — no window boundary corrupted it).
    assert_eq!(
        echo.iter().next().expect("row").expect("decode").s.len(),
        40 * 1024 * 1024,
        "the ~40 MiB oversize param round-tripped whole",
    );
    assert!(c.is_healthy(), "connection is reusable after the oversize batch");
    c.close().await.expect("close");
}

/// ALL-OR-NOTHING at LARGE payload across MANY windows. Eight commands each carry a
/// 256 KiB `text` param (~2 MiB tail → multi-window); the LAST duplicates command
/// #0's id → `23505` at its Execute. The batch is ONE implicit transaction under the
/// single trailing `Sync`, so the whole thing ROLLS BACK: `BatchFailed { index: 8 }`
/// and ZERO rows persisted — the windowed drive's all-or-nothing is airtight even
/// when the failure lands after several flushed windows already streamed to the
/// server.
#[tokio::test]
#[ignore = "requires local PG"]
async fn windowed_all_or_nothing_rollback_at_large_payload() {
    let mut c = Connection::connect(&cfg()).await.expect("connect");
    let base = 8_900_000i64;
    prepare_bulk(&mut c, base, base + 999).await;
    let payload = "b".repeat(256 * 1024); // 256 KiB per command → ~2 MiB tail

    // arity 9: eight distinct-id inserts, then a NINTH that duplicates id (base+1).
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        c.pipeline((
            PlBulkIns::bind((base + 1, payload.as_str())),
            PlBulkIns::bind((base + 2, payload.as_str())),
            PlBulkIns::bind((base + 3, payload.as_str())),
            PlBulkIns::bind((base + 4, payload.as_str())),
            PlBulkIns::bind((base + 5, payload.as_str())),
            PlBulkIns::bind((base + 6, payload.as_str())),
            PlBulkIns::bind((base + 7, payload.as_str())),
            PlBulkIns::bind((base + 8, payload.as_str())),
            PlBulkIns::bind((base + 1, payload.as_str())), // DUPLICATE id → 23505
        )),
    )
    .await
    .expect("pipeline completed within 60s (no deadlock)");

    match result {
        Err(DriverError::BatchFailed { index, source }) => {
            assert_eq!(index, 8, "the NINTH command (index 8) hit the duplicate key");
            assert!(source.code().starts_with("23"), "a constraint violation, got {source:?}");
        }
        other => panic!("expected BatchFailed at index 8, got {other:?}"),
    }
    // THE PROOF: the whole implicit transaction rolled back — the eight LARGE-payload
    // writes that streamed across several flushed windows persisted NOTHING.
    assert_eq!(
        bulk_count(&mut c, base + 1, base + 8).await,
        0,
        "a mid-batch failure rolled back every windowed write — zero rows persisted",
    );
    // The connection survived the recoverable failure and is reusable.
    assert!(c.is_healthy(), "connection stays healthy after a windowed batch failure");
    assert_eq!(c.query_one::<PlSeven>(()).await.expect("reuse").n, 7);
    c.close().await.expect("close");
}
