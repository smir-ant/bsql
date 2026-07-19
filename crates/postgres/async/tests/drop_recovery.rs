#![forbid(unsafe_code)]
//! GATE (C1 fix, --ignored live): a connection whose IN-FLIGHT query future is
//! DROPPED — the single most common async cancellation pattern,
//! `tokio::time::timeout(dur, conn.query(..))` or `select!` losing its race —
//! transparently RECOVERS on next use instead of being permanently bricked to
//! `NotReady`, AND the abandoned server-side query is cancelled (no zombie holding
//! locks until `statement_timeout`).
//!
//! Pre-fix, dropping the future between a verb's `take_live` and its token restore
//! left `Core::live == None` forever (a DIRECT connection unusable; a POOLED one
//! evicted), and no `CancelRequest` was ever sent (the backend kept running the
//! query). These tests are the RED->GREEN witnesses.
//!
//! RED-proof: neuter the recovery (make `Core::begin_command`'s `DIRTY_DRAIN` arm
//! `return Err(DriverError::NotReady)` instead of recovering) and
//! `dropping_an_inflight_query_future_recovers_the_connection` goes red at the
//! post-drop `SELECT 1` (which then classifies as a disconnect, not `Ok`).
//!
//! Needs a local PG (trust auth, plaintext loopback), so `#[ignore]`.

use core::ops::ControlFlow;
use std::time::{Duration, Instant};

use bsql_postgres_async::{ConnectConfig, Connection, DriverError, Pool, SslMode};

/// A direct plaintext config to local PG.
fn direct() -> ConnectConfig {
    ConnectConfig::new("127.0.0.1", "smir-ant")
        .database("postgres".to_string())
        .ssl_mode(SslMode::Disable)
}

/// THE CORE PROOF: a `pg_sleep` query dropped under a `tokio::time::timeout` loss
/// leaves the connection recoverable, and the very NEXT verb on the SAME
/// connection succeeds (the connection was NOT bricked). Bounded — the recovery
/// best-effort cancels the abandoned sleep so the drain is quick.
#[tokio::test]
#[ignore = "requires local PG"]
async fn dropping_an_inflight_query_future_recovers_the_connection() {
    let mut conn = Connection::connect(&direct()).await.expect("connect");
    assert!(conn.is_healthy());

    // Start a slow query and DROP its future when the 50 ms timeout fires — the
    // query is still running server-side, the `Live` token is gone, and the
    // verb-scoped `CancelScope`'s Drop set the connection's `dirty` marker.
    let timed = tokio::time::timeout(
        Duration::from_millis(50),
        conn.query_one_raw("SELECT pg_sleep(2)"),
    )
    .await;
    assert!(timed.is_err(), "the sleep must still be running when its future is dropped");

    // THE ASSERTION: the next verb transparently recovers the connection and
    // SUCCEEDS. Pre-fix this returned `DriverError::NotReady` forever.
    let start = Instant::now();
    let row = conn
        .query_one_raw("SELECT 1::int4")
        .await
        .expect("the connection must transparently recover after a dropped-future");
    assert_eq!(row.get_i32(0), Ok(Some(1)));
    // Recovery is bounded: the best-effort cancel stops the abandoned 2 s sleep,
    // so recovery completes in well under the sleep's remaining time.
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(1500),
        "recovery must be bounded + fast (cancel stopped the sleep), took {elapsed:?}",
    );

    // Fully healthy for continued use.
    assert!(conn.is_healthy(), "the recovered connection is healthy");
    let again = conn.query_one_raw("SELECT 7::int4").await.expect("still healthy");
    assert_eq!(again.get_i32(0), Ok(Some(7)));
    conn.close().await.expect("close");
}

/// A dropped TRANSACTION future recovers to a GENUINELY clean idle (status `I`),
/// not merely to an RFQ still inside the leftover transaction. The fused `BEGIN`
/// was sent and the drop skips the guard's async rollback (async `Drop` can't
/// `.await`), so recovery must ROLL BACK the leftover transaction — otherwise the
/// connection is "recovered" while stuck in `T`/`E`, and every subsequent verb
/// returns `Db(25P02 "current transaction is aborted…")` forever (a regression:
/// pre-fix a dropped-future connection was a clean `NotReady`/reconnect).
///
/// RED-proof: remove the `tx_status`-gated `ROLLBACK` from `recover_drain` and the
/// `SELECT 55` below returns `25P02` instead of `Ok`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn dropping_an_inflight_transaction_future_recovers_to_a_clean_idle() {
    let mut conn = Connection::connect(&direct()).await.expect("connect");

    // Start a transaction whose body runs a long sleep, then DROP its future when
    // the 50 ms timeout fires — the `BEGIN` was sent, the body is mid-`pg_sleep`,
    // and the guard's rollback is skipped.
    let timed = tokio::time::timeout(
        Duration::from_millis(50),
        conn.transaction(async |tx| {
            drop(tx.query_one_raw("SELECT pg_sleep(30)").await?);
            Ok::<(), DriverError>(())
        }),
    )
    .await;
    assert!(
        timed.is_err(),
        "the transaction body must still be running when its future is dropped",
    );

    // THE ASSERTION: the next plain verb SUCCEEDS — recovery rolled the leftover
    // transaction back to a clean idle. Without the rollback this is a permanent
    // `25P02` with `is_disconnect() == false`.
    let row = conn
        .query_one_raw("SELECT 55::int4")
        .await
        .expect("the next verb must succeed on a connection recovered to a clean idle (not 25P02)");
    assert_eq!(row.get_i32(0), Ok(Some(55)));
    assert!(conn.is_healthy(), "the connection is at a genuine clean idle");

    // A subsequent NEW transaction also works (no leftover-tx interference).
    let n = conn
        .transaction(async |tx| {
            let r = tx.query_one_raw("SELECT 7::int4").await?;
            Ok::<Option<i32>, DriverError>(r.get_i32(0).expect("decode i32"))
        })
        .await
        .expect("a fresh transaction runs cleanly after recovery");
    assert_eq!(n, Some(7));
    conn.close().await.expect("close");
}

/// The SERVER-SIDE half: after a dropped `pg_sleep` future is recovered, the
/// abandoned query is GONE — the connection's backend is no longer running the
/// sleep (the recovery sent a `CancelRequest`). Checked from a SECOND connection
/// via `pg_stat_activity`.
#[tokio::test]
#[ignore = "requires local PG"]
async fn the_abandoned_backend_query_is_cancelled_by_recovery() {
    let mut conn = Connection::connect(&direct()).await.expect("connect");
    let pid = conn.backend_pid();
    assert!(pid > 0);

    // Drop a long sleep's future mid-flight.
    let timed = tokio::time::timeout(
        Duration::from_millis(50),
        conn.query_one_raw("SELECT pg_sleep(30)"),
    )
    .await;
    assert!(timed.is_err(), "the 30 s sleep must still be running when dropped");

    // Trigger recovery (best-effort cancel + drain) via the next verb.
    let row = conn.query_one_raw("SELECT 1::int4").await.expect("recovers");
    assert_eq!(row.get_i32(0), Ok(Some(1)));

    // From a SECOND connection, the backend must NOT be running the sleep anymore
    // (it was cancelled + the connection re-idled). It is either idle or running
    // this connection's own subsequent query — never the abandoned `pg_sleep(30)`.
    let mut observer = Connection::connect(&direct()).await.expect("observer connect");
    let obs = observer
        .query_one_raw(&format!(
            "SELECT coalesce((SELECT query FROM pg_stat_activity WHERE pid = {pid}), 'gone')",
        ))
        .await
        .expect("observe backend state");
    let running: Option<&str> = obs.get_str(0).expect("query text");
    assert!(
        !running.is_some_and(|q| q.contains("pg_sleep(30)")),
        "the abandoned backend must not still be running pg_sleep(30), saw: {running:?}",
    );
    conn.close().await.expect("close");
    observer.close().await.expect("close observer");
}

/// A dropped future that loses its `timeout` race — with the recovering next verb
/// ITSELF also under a (generous) timeout — never hangs and always succeeds: the
/// recovery drain is bounded (the cancel stops the abandoned query).
#[tokio::test]
#[ignore = "requires local PG"]
async fn recovery_of_a_dropped_future_is_never_a_hang() {
    let mut conn = Connection::connect(&direct()).await.expect("connect");
    let timed = tokio::time::timeout(
        Duration::from_millis(50),
        conn.query_one_raw("SELECT pg_sleep(10)"),
    )
    .await;
    assert!(timed.is_err());

    // The recovering verb under a generous outer bound — must resolve, never hang.
    let recovered = tokio::time::timeout(
        Duration::from_secs(8),
        conn.query_one_raw("SELECT 5::int4"),
    )
    .await
    .expect("recovery must not hang")
    .expect("recovery must succeed");
    assert_eq!(recovered.get_i32(0), Ok(Some(5)));
    conn.close().await.expect("close");
}

/// The POOLED variant: a pooled connection whose checked-out verb future is
/// dropped is, on return + re-checkout, HEALTHY — recovered or cleanly
/// evicted-and-replaced, never bricked. The pool never hands out a `NotReady`
/// connection.
#[tokio::test]
#[ignore = "requires local PG"]
async fn a_pooled_connection_survives_a_dropped_future() {
    let pool = Pool::new(direct(), 2);

    {
        let mut c = pool.get().await.expect("check out a pooled connection");
        // Drop a slow query's future mid-flight on the checked-out connection.
        let timed = tokio::time::timeout(
            Duration::from_millis(50),
            c.conn_mut().expect("borrow").query_one_raw("SELECT pg_sleep(2)"),
        )
        .await;
        assert!(timed.is_err(), "the sleep must still be running when dropped");
        // `c` is dropped here → returns toward the pool (dirty → evicted on return,
        // or recovered at the next checkout's reset — either way never handed out
        // bricked).
    }

    // Re-checkout: the pool yields a HEALTHY connection (fresh replacement, or the
    // same one recovered by the checkout reset), bounded, and it works.
    let start = Instant::now();
    let mut c2 = pool.get().await.expect("re-checkout must yield a healthy connection");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "re-checkout must be bounded, took {:?}",
        start.elapsed(),
    );
    let row = c2
        .conn_mut()
        .expect("borrow")
        .query_one_raw("SELECT 4::int4")
        .await
        .expect("pooled connection works");
    assert_eq!(row.get_i32(0), Ok(Some(4)));
    // And a plain SELECT streams fine (no leaked dirty state).
    let mut seen = 0i32;
    c2.conn_mut()
        .expect("borrow")
        .query_each_raw::<_, ()>("SELECT generate_series(1, 3)", |_r| {
            seen += 1;
            ControlFlow::Continue(())
        })
        .await
        .expect("stream on the recovered/replaced connection");
    assert_eq!(seen, 3);
    drop(c2);
    pool.close().await;
}

/// A dropped future followed by a REPEATED recovery-then-drop cycle leaves no
/// leak: the connection recovers each time and stays usable.
#[tokio::test]
#[ignore = "requires local PG"]
async fn repeated_drop_then_recover_does_not_leak() {
    let mut conn = Connection::connect(&direct()).await.expect("connect");
    for i in 0..5i32 {
        let timed = tokio::time::timeout(
            Duration::from_millis(40),
            conn.query_one_raw("SELECT pg_sleep(1)"),
        )
        .await;
        assert!(timed.is_err(), "iteration {i}: the sleep must still be running when dropped");
        let row = match conn.query_one_raw("SELECT 1::int4").await {
            Ok(r) => r,
            Err(e) => panic!("iteration {i}: must recover, got {e:?}"),
        };
        assert_eq!(row.get_i32(0), Ok(Some(1)));
    }
    assert!(conn.is_healthy());
    conn.close().await.expect("close");
}
