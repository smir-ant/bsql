//! The no-drop notification ledger, proven at the driver level with NO socket.
//!
//! A `NOTIFY` scripted to arrive DURING a query (the interleaving the real
//! backend does when a notification is pending while a command runs) must be
//! CAPTURED by the driver, not dropped. Before the ledger, the result collectors
//! folded `Surface::Notify` into a no-op arm, so a notification interleaved with a
//! query was silently lost — `buffered_notifications()` would be 0 and a later
//! `recv_notification` would never see it. These tests drive a REAL `Connection`
//! (async and sync) over the in-memory fake and assert the interleaved
//! notification is buffered and drained — the RED→GREEN witness for the fix.

use std::time::Duration;

use bsql_postgres_core::DriverError;
use bsql_testkit::{rows, FakePostgres};

/// The exact session-reset command the drivers issue when the connection is idle
/// (the fake reports an idle transaction status). Scripting it lets the offline
/// test exercise `reset_session`, which clears the ledger.
const RESET_SQL: &str = "SET SESSION AUTHORIZATION DEFAULT; RESET ALL; CLOSE ALL; \
     UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES";

// ── The no-drop witness: a NOTIFY interleaved with a query is captured ────────

#[tokio::test]
async fn notify_during_query_is_captured_async() -> Result<(), Box<dyn std::error::Error>> {
    let mut fake = FakePostgres::new();
    // A NOTIFY arrives DURING the query's reply. Before the ledger this was
    // dropped by the result collector; now it is captured.
    fake.on("SELECT id FROM users")
        .notifying(4242, "orders", "shipped")
        .returns(rows![[1_i64], [2_i64]]);

    let mut conn = fake.connect().await?;
    let result = conn.query_sql("SELECT id FROM users").await?;
    // The query itself still returns its rows, unaffected.
    assert_eq!(result.len(), 2);

    // The smoking gun: the interleaved notification was captured DURING the query
    // (this is the exact frame the old no-op arm dropped).
    assert_eq!(conn.buffered_notifications(), 1, "the interleaved NOTIFY was captured, not dropped");
    assert_eq!(conn.notifications_received(), 1);
    assert_eq!(conn.notifications_shed(), 0, "nothing shed");

    // recv_notification drains the ledger FIRST — returns the already-arrived
    // notification with no socket wait (a ZERO timeout would would-block if it
    // touched the socket).
    let n = conn
        .recv_notification(Duration::ZERO)
        .await?
        .expect("the buffered notification is delivered");
    assert_eq!((n.pid, n.channel.as_str(), n.payload.as_str()), (4242, "orders", "shipped"));
    assert_eq!(conn.buffered_notifications(), 0, "the ledger is now drained");
    Ok(())
}

#[test]
fn notify_during_query_is_captured_sync() -> Result<(), Box<dyn std::error::Error>> {
    let mut fake = FakePostgres::new();
    fake.on("SELECT id FROM users")
        .notifying(4242, "orders", "shipped")
        .returns(rows![[1_i64], [2_i64]]);

    let mut conn = fake.connect_sync()?;
    let result = conn.query_sql("SELECT id FROM users")?;
    assert_eq!(result.len(), 2);

    assert_eq!(conn.buffered_notifications(), 1, "the interleaved NOTIFY was captured, not dropped");
    assert_eq!(conn.notifications_received(), 1);

    let n = conn
        .recv_notification(Duration::ZERO)?
        .expect("the buffered notification is delivered");
    assert_eq!((n.pid, n.channel.as_str(), n.payload.as_str()), (4242, "orders", "shipped"));
    assert_eq!(conn.buffered_notifications(), 0, "the ledger is now drained");
    Ok(())
}

#[tokio::test]
async fn multiple_interleaved_notifications_drain_in_order_async(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut fake = FakePostgres::new();
    fake.on("SELECT 1")
        .notifying(1, "c", "first")
        .notifying(1, "c", "second")
        .notifying(1, "c", "third")
        .returns(rows![[1_i64]]);

    let mut conn = fake.connect().await?;
    let _ = conn.query_sql("SELECT 1").await?;

    assert_eq!(conn.buffered_notifications(), 3, "all three interleaved NOTIFYs captured");
    assert_eq!(conn.notifications_received(), 3);

    // Front-first drain order matches arrival order.
    for expected in ["first", "second", "third"] {
        let n = conn
            .recv_notification(Duration::ZERO)
            .await?
            .expect("a buffered notification");
        assert_eq!(n.payload, expected);
    }
    // The ledger is fully drained (that recv reaches the socket for MORE data is
    // the LIVE path; the in-memory fake cannot model a would-block empty read, so
    // "recv returns None on an empty ledger" is asserted in the live tests).
    assert_eq!(conn.buffered_notifications(), 0, "all captured notifications drained");
    Ok(())
}

// ── The reset-clears witness: a pooled connection does not leak notifications ──

#[tokio::test]
async fn reset_session_clears_pending_notifications_async(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut fake = FakePostgres::new();
    fake.on("SELECT 1")
        .notifying(9, "prior_user_channel", "leaked?")
        .returns(rows![[1_i64]]);
    fake.on(RESET_SQL).returns(rows![[1_i64]]);

    let mut conn = fake.connect().await?;
    let _ = conn.query_sql("SELECT 1").await?;
    assert_eq!(conn.buffered_notifications(), 1, "a prior user's NOTIFY is buffered");

    // A pooled connection is reset before the next user gets it. The ledger being
    // empty afterward proves the next user's `recv_notification` finds nothing to
    // deliver from the prior user (the LIVE test then confirms that recv returns
    // None over a real socket).
    conn.reset_session().await?;
    assert_eq!(conn.buffered_notifications(), 0, "reset cleared the prior user's notifications");
    assert_eq!(conn.notifications_received(), 1, "the lifetime counter is preserved across reset");
    Ok(())
}

#[test]
fn reset_session_clears_pending_notifications_sync() -> Result<(), Box<dyn std::error::Error>> {
    let mut fake = FakePostgres::new();
    fake.on("SELECT 1")
        .notifying(9, "prior_user_channel", "leaked?")
        .returns(rows![[1_i64]]);
    fake.on(RESET_SQL).returns(rows![[1_i64]]);

    let mut conn = fake.connect_sync()?;
    let _ = conn.query_sql("SELECT 1")?;
    assert_eq!(conn.buffered_notifications(), 1, "a prior user's NOTIFY is buffered");

    conn.reset_session()?;
    assert_eq!(conn.buffered_notifications(), 0, "reset cleared the prior user's notifications");
    assert_eq!(conn.notifications_received(), 1, "the lifetime counter is preserved across reset");
    Ok(())
}

// ── The typed subscription layer: FromStr payloads, classified parse failures ─

/// A consumer's own payload type, parsed from the notification text — proves the
/// typed layer is dep-free (any `FromStr`, including a domain enum).
#[derive(Debug, PartialEq, Eq)]
enum Job {
    Start,
    Stop,
}

impl std::str::FromStr for Job {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "start" => Ok(Self::Start),
            "stop" => Ok(Self::Stop),
            _ => Err(()),
        }
    }
}

#[tokio::test]
async fn recv_notification_as_parses_a_typed_payload() -> Result<(), Box<dyn std::error::Error>> {
    let mut fake = FakePostgres::new();
    fake.on("SELECT 1")
        .notifying(7, "jobs", "start")
        .returns(rows![[1_i64]]);

    let mut conn = fake.connect().await?;
    let _ = conn.query_sql("SELECT 1").await?;

    let n = conn
        .recv_notification_as::<Job>(Duration::ZERO)
        .await?
        .expect("a typed notification");
    assert_eq!(n.payload, Job::Start);
    assert_eq!(n.channel, "jobs");
    assert_eq!(n.pid, 7);
    Ok(())
}

#[tokio::test]
async fn recv_notification_as_classifies_a_parse_failure_not_a_silent_drop(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut fake = FakePostgres::new();
    fake.on("SELECT 1")
        .notifying(7, "counter", "not-a-number")
        .returns(rows![[1_i64]]);

    let mut conn = fake.connect().await?;
    let _ = conn.query_sql("SELECT 1").await?;

    // A payload that does not parse into the requested type is a LOUD classified
    // error carrying the raw payload — never a silent drop or a defaulted value.
    let err = conn
        .recv_notification_as::<i64>(Duration::ZERO)
        .await
        .expect_err("a non-numeric payload must not parse into i64");
    match err {
        DriverError::PayloadParse(raw) => assert_eq!(&*raw, "not-a-number"),
        other => panic!("expected a classified PayloadParse, got {other:?}"),
    }
    // The notification was removed from the ledger (it cannot wedge the buffer).
    assert_eq!(conn.buffered_notifications(), 0);
    Ok(())
}

#[test]
fn recv_notification_as_parses_a_typed_payload_sync() -> Result<(), Box<dyn std::error::Error>> {
    let mut fake = FakePostgres::new();
    fake.on("SELECT 1")
        .notifying(7, "jobs", "42")
        .returns(rows![[1_i64]]);

    let mut conn = fake.connect_sync()?;
    let _ = conn.query_sql("SELECT 1")?;

    let n = conn
        .recv_notification_as::<i64>(Duration::ZERO)?
        .expect("a typed notification");
    assert_eq!(n.payload, 42_i64);
    Ok(())
}
