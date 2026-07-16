//! The drain-path no-drop witness: a `NOTIFY` riding the DRAINED REMAINDER of a
//! streaming `query_each` (after an early break) must be CAPTURED, not silently
//! dropped.
//!
//! When `query_each` stops early, the connection is left dirty and reclaimed by
//! draining its remaining reply frames to a clean idle. That reclaim reads real
//! wire bytes — including any asynchronous `NotificationResponse` the backend
//! interleaved in the tail — so its sink must capture notifications exactly like
//! every other verb. The testkit fake splices the scripted `NOTIFY` AFTER the
//! rows, so an early break leaves it squarely in the drained remainder.
//!
//! RED (before the drain-sink fix): the `drain` verb pumped with a noop sink, so
//! a `NOTIFY` in the tail was silently dropped — `buffered_notifications() == 0`.
//! GREEN: `drain` threads the capture adapter, so it is buffered (`== 1`), then
//! drains typed through `recv_notification`.
#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "test harness — expect/unwrap surface failures loudly; not production fallbacks"
)]

use core::ops::ControlFlow;
use std::time::Duration;

use bsql_testkit::{rows, FakePostgres};

// `users` (from migrations/): id BIGINT (i64), name TEXT NOT NULL (&str).
bsql::query!(StreamUsers, "SELECT id, name FROM users");

/// Script three rows with a `NOTIFY` interleaved AFTER them, so a `query_each`
/// that breaks on the first row leaves the notification in the drained remainder.
fn fake_with_tail_notification() -> FakePostgres {
    let mut fake = FakePostgres::new();
    fake.on("SELECT id, name FROM users")
        .notifying(99, "drain_ch", "from-the-tail")
        .returns(rows![[1_i64, "alice"], [2_i64, "bob"], [3_i64, "carol"]]);
    fake
}

#[tokio::test]
async fn notification_in_drained_tail_is_captured_async() {
    let mut conn = fake_with_tail_notification()
        .connect()
        .await
        .expect("connect over the fake");

    // Break after the FIRST row: rows 2..3 + the NOTIFY + CommandComplete are the
    // drained remainder the reclaim reads.
    let mut seen = 0usize;
    let out = conn
        .query_each::<StreamUsersQuery, _, _>((), |_row| {
            seen += 1;
            ControlFlow::<()>::Break(())
        })
        .await
        .expect("stream + drain succeeds");
    assert_eq!(out, Some(()), "the early break returns Ok(Some(()))");
    assert_eq!(seen, 1, "broke after the first row");

    // THE WITNESS: the NOTIFY in the drained remainder was captured, not dropped.
    assert_eq!(
        conn.buffered_notifications(),
        1,
        "a NOTIFY riding the drained remainder is captured by the drain sink"
    );
    let n = conn
        .recv_notification(Duration::ZERO)
        .await
        .expect("recv")
        .expect("the buffered notification drains from the ledger");
    assert_eq!(n.payload, "from-the-tail");
    assert_eq!(n.channel, "drain_ch");
    assert_eq!(n.pid, 99);
}

#[test]
fn notification_in_drained_tail_is_captured_sync() {
    let mut conn = fake_with_tail_notification()
        .connect_sync()
        .expect("connect over the fake");

    let mut seen = 0usize;
    let out = conn
        .query_each::<StreamUsersQuery, _, _>((), |_row| {
            seen += 1;
            ControlFlow::<()>::Break(())
        })
        .expect("stream + drain succeeds");
    assert_eq!(out, Some(()), "the early break returns Ok(Some(()))");
    assert_eq!(seen, 1, "broke after the first row");

    assert_eq!(
        conn.buffered_notifications(),
        1,
        "a NOTIFY riding the drained remainder is captured by the drain sink"
    );
    let n = conn
        .recv_notification(Duration::ZERO)
        .expect("recv")
        .expect("the buffered notification drains from the ledger");
    assert_eq!(n.payload, "from-the-tail");
    assert_eq!(n.channel, "drain_ch");
    assert_eq!(n.pid, 99);
}
