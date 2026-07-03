//! The per-connection notification ledger and the sink adapter that fills it.
//!
//! An asynchronous `NotificationResponse` (`LISTEN`/`NOTIFY`) can ride the
//! response stream of ANY command — the PostgreSQL backend interleaves pending
//! notifications with a query's own frames — as well as arrive between commands.
//! The sans-IO engine surfaces every one as a [`Surface::Notify`], one at a time,
//! to the driver's sink closure. Historically the result collectors folded
//! `Surface::Notify` into a no-op arm, so a notification arriving DURING a query
//! was silently lost; only a dedicated wait caught one.
//!
//! [`NotificationLedger`] closes that hole: it is a per-connection buffer that
//! captures EVERY surfaced notification, so `recv_notification` can drain an
//! already-arrived notification without another round trip. [`capture_notify`]
//! is the one shared sink adapter every verb wraps its sink with, so the capture
//! rule lives in exactly one place (no per-verb drift).
//!
//! # No silent loss, bounded memory
//!
//! An unbounded buffer is a memory-DoS on a connection that `LISTEN`s but never
//! drains under a notify flood. The ledger is instead a **bounded ring**: on
//! overflow it drops the OLDEST buffered notification and increments a LOUD,
//! monotonic [`shed`](NotificationLedger::shed) counter — the loss is surfaced,
//! never silent. A monotonic [`received`](NotificationLedger::received) counter
//! (every notification ever captured, shed or not) lets a consumer detect any
//! gap. So the ledger never *silently* loses a notification: it either buffers it
//! or records a visible shed count.
//!
//! # Lazy, fail-loud decode
//!
//! The ledger stores each notification's RAW wire body (owned, so it outlives
//! the transient read buffer it was surfaced from) and parses it only at drain,
//! via [`parse_notification`](crate::materialize::parse_notification). A
//! structurally malformed or non-UTF-8 notification therefore surfaces as a
//! classified [`DriverError`] to the consumer that drains it — never swallowed,
//! and never charged to an unrelated query that merely happened to be running
//! when the frame arrived.

use std::collections::VecDeque;
use std::ops::ControlFlow;

use bsql_postgres_proto::engine::Surface;

use crate::error::DriverError;
use crate::materialize::parse_notification;
use crate::types::Notification;

/// A bounded, counted, per-connection buffer of asynchronous notifications.
///
/// Every [`Surface::Notify`] a verb surfaces is captured here (see
/// [`capture_notify`]); [`recv_notification`] drains it front-first, so an
/// already-arrived notification returns with no round trip. The buffer is a
/// bounded ring — see the [module docs](crate::notify) for the no-silent-loss
/// and lazy-decode guarantees.
///
/// [`recv_notification`]: https://docs.rs/bsql-postgres-async
#[derive(Debug)]
pub struct NotificationLedger {
    /// The captured notification bodies (raw wire bytes, parsed at drain),
    /// oldest at the front.
    buf: VecDeque<Vec<u8>>,
    /// The ring's maximum length. At capture, exceeding it drops the oldest and
    /// bumps [`shed`](Self::shed).
    capacity: usize,
    /// Every notification ever captured (buffered or later shed) — monotonic.
    received: u64,
    /// Every notification shed on overflow — monotonic, LOUD (a non-zero value
    /// means notifications were lost to the bound).
    shed: u64,
}

// Footprint pin: VecDeque (4 words: head + len + ptr + cap) + usize capacity +
// two u64 counters = 56 bytes. A new field, or a wider buffer element type,
// shows up here as an E0080 at `cargo check`.
crate::footprint_pin!(NotificationLedger, size = 56, align = 8);

impl NotificationLedger {
    /// The default ring capacity: the most notifications buffered before the
    /// bound sheds the oldest. Generous for any reasonable drain cadence while
    /// bounding worst-case memory (a PostgreSQL notify payload is capped at
    /// 8000 bytes, so the ceiling is on the order of a few megabytes even when
    /// full — a hard bound, versus an unbounded buffer's unbounded growth).
    pub const DEFAULT_CAPACITY: usize = 1024;

    /// A fresh, empty ledger with the [`DEFAULT_CAPACITY`](Self::DEFAULT_CAPACITY).
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY)
    }

    /// A fresh, empty ledger with a chosen ring capacity (clamped to at least 1,
    /// since a zero-length ring could hold nothing and would shed every capture).
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: VecDeque::new(),
            capacity: capacity.max(1),
            received: 0,
            shed: 0,
        }
    }

    /// Capture one notification's raw wire body.
    ///
    /// Always records it: on a full ring the OLDEST buffered notification is
    /// dropped and [`shed`](Self::shed) is bumped (a LOUD, visible loss — never a
    /// silent drop), then the new body is pushed at the back. [`received`](Self::received)
    /// counts every capture regardless.
    pub fn capture(&mut self, body: &[u8]) {
        self.received = self.received.saturating_add(1);
        if self.buf.len() >= self.capacity {
            // Drop the oldest to make room; the loss is recorded loudly.
            let _shed = self.buf.pop_front();
            self.shed = self.shed.saturating_add(1);
        }
        self.buf.push_back(body.to_vec());
    }

    /// Drain and parse the oldest buffered notification, if any.
    ///
    /// Returns `None` when the ledger is empty. Otherwise pops the front body and
    /// parses it: `Some(Ok(n))` on success, or `Some(Err(e))` for a malformed /
    /// non-UTF-8 body — the parse failure is surfaced to the drainer, classified,
    /// never swallowed. A malformed front entry is still POPPED, so it cannot wedge
    /// the ledger: the next drain sees the following notification.
    pub fn drain_one(&mut self) -> Option<Result<Notification, DriverError>> {
        let body = self.buf.pop_front()?;
        Some(parse_notification(&body))
    }

    /// Discard every buffered notification.
    ///
    /// The monotonic [`received`](Self::received) / [`shed`](Self::shed) counters
    /// are deliberately NOT reset — they are a lifetime audit trail. This is the
    /// hook a connection's session-reset calls so a pooled connection never
    /// delivers a prior user's notifications to the next user.
    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// The number of notifications currently buffered (not yet drained or shed).
    #[must_use]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the ledger currently holds no buffered notifications.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The ring capacity — the buffered count at which the next capture sheds.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Every notification ever captured on this connection (buffered or later
    /// shed) — monotonic. A consumer can compare successive reads to detect a gap.
    #[must_use]
    pub fn received(&self) -> u64 {
        self.received
    }

    /// Every notification shed on overflow — monotonic. Non-zero means the bound
    /// dropped notifications; the loss is LOUD, not silent.
    #[must_use]
    pub fn shed(&self) -> u64 {
        self.shed
    }
}

impl Default for NotificationLedger {
    fn default() -> Self {
        Self::new()
    }
}

/// A notification whose payload has been parsed into an application type `T`.
///
/// The typed counterpart of [`Notification`]: the driver's
/// `recv_notification_as::<T>` parses the raw payload string into `T` via its
/// [`FromStr`](core::str::FromStr) impl, so a subscriber consumes a decoded value
/// directly. A parse failure is a classified
/// [`DriverError::PayloadParse`](crate::DriverError::PayloadParse), never a
/// silently-dropped notification. Dep-free: any `T: FromStr` (a std scalar, or a
/// consumer's own enum) works.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedNotification<T> {
    /// The `LISTEN` channel the notification arrived on.
    pub channel: String,
    /// The payload, parsed into `T`.
    pub payload: T,
    /// The backend PID of the notifying session.
    pub pid: i32,
}

/// Wrap a verb's surface sink so every [`Surface::Notify`] is captured into
/// `ledger` and every other surface flows to `inner` unchanged.
///
/// This is the ONE place the notification-capture rule lives; every driver verb
/// wraps its own sink with it, so a notification arriving on any command's
/// response stream is buffered rather than dropped. A captured notification does
/// NOT stop the pump — the wrapper returns [`ControlFlow::Continue`] for it, so a
/// query runs to completion and merely deposits any interleaved notification in
/// the ledger as a side effect.
///
/// The returned closure is higher-ranked over the surface lifetime (`inner` sees
/// `Surface<'e>` for any `'e`) and generic over the sink's break payload `B`, so
/// it fits both the command verbs (break payload `Never`) and the notification
/// wait (break payload `()`). The captured body is copied into the ledger inside
/// the call, so it never outlives the borrow.
pub fn capture_notify<'l, B>(
    ledger: &'l mut NotificationLedger,
    mut inner: impl FnMut(Surface<'_>) -> ControlFlow<B> + 'l,
) -> impl FnMut(Surface<'_>) -> ControlFlow<B> + 'l {
    move |surface: Surface<'_>| {
        if let Surface::Notify(body) = surface {
            ledger.capture(body);
            ControlFlow::Continue(())
        } else {
            inner(surface)
        }
    }
}

#[cfg(test)]
mod tests {
    //! The ledger's invariants without a driver: bounded capture, drop-oldest
    //! shed accounting, front-first drain, lazy classified decode, and that the
    //! capture adapter routes `Notify` to the ledger while passing every other
    //! surface through untouched.

    use std::ops::ControlFlow;

    use bsql_postgres_proto::engine::Surface;

    use super::{capture_notify, NotificationLedger};
    use crate::error::DriverError;

    /// A well-formed `NotificationResponse` body: `[i32 pid][channel\0][payload\0]`.
    fn body(pid: i32, channel: &str, payload: &str) -> Vec<u8> {
        let mut b = pid.to_be_bytes().to_vec();
        b.extend_from_slice(channel.as_bytes());
        b.push(0);
        b.extend_from_slice(payload.as_bytes());
        b.push(0);
        b
    }

    #[test]
    fn capture_then_drain_front_first_parses() {
        let mut led = NotificationLedger::new();
        led.capture(&body(1, "a", "first"));
        led.capture(&body(2, "b", "second"));
        assert_eq!(led.len(), 2);
        assert_eq!(led.received(), 2);
        assert_eq!(led.shed(), 0);

        let n1 = match led.drain_one() {
            Some(Ok(n)) => n,
            other => panic!("expected first notification, got {other:?}"),
        };
        assert_eq!((n1.pid, n1.channel.as_str(), n1.payload.as_str()), (1, "a", "first"));
        let n2 = match led.drain_one() {
            Some(Ok(n)) => n,
            other => panic!("expected second notification, got {other:?}"),
        };
        assert_eq!((n2.pid, n2.channel.as_str(), n2.payload.as_str()), (2, "b", "second"));
        assert!(led.drain_one().is_none(), "ledger drained empty");
        assert!(led.is_empty());
    }

    #[test]
    fn overflow_drops_oldest_and_sheds_loudly() {
        let mut led = NotificationLedger::with_capacity(2);
        assert_eq!(led.capacity(), 2);
        led.capture(&body(1, "c", "oldest"));
        led.capture(&body(2, "c", "middle"));
        led.capture(&body(3, "c", "newest")); // evicts "oldest"

        assert_eq!(led.len(), 2, "ring stays bounded at capacity");
        assert_eq!(led.received(), 3, "received counts every capture");
        assert_eq!(led.shed(), 1, "the eviction is a LOUD shed, not a silent drop");

        // The survivors are the two NEWEST, in order.
        let first = match led.drain_one() {
            Some(Ok(n)) => n.payload,
            other => panic!("expected middle, got {other:?}"),
        };
        assert_eq!(first, "middle");
        let second = match led.drain_one() {
            Some(Ok(n)) => n.payload,
            other => panic!("expected newest, got {other:?}"),
        };
        assert_eq!(second, "newest");
    }

    #[test]
    fn zero_capacity_is_clamped_to_one() {
        let mut led = NotificationLedger::with_capacity(0);
        assert_eq!(led.capacity(), 1, "a zero-length ring is useless; clamp to 1");
        led.capture(&body(1, "c", "x"));
        assert_eq!(led.len(), 1);
    }

    #[test]
    fn malformed_front_is_classified_and_popped_not_wedged() {
        let mut led = NotificationLedger::new();
        // A truncated body (fewer than the 4 pid bytes) is structurally malformed.
        led.capture(&[0, 0]);
        led.capture(&body(7, "c", "good"));

        match led.drain_one() {
            Some(Err(DriverError::NotificationUnavailable)) => {}
            other => panic!("expected a classified malformed error, got {other:?}"),
        }
        // The malformed entry was popped, so the following good one drains next —
        // a bad frame does not wedge the ledger.
        match led.drain_one() {
            Some(Ok(n)) => assert_eq!(n.payload, "good"),
            other => panic!("expected the good notification next, got {other:?}"),
        }
    }

    #[test]
    fn clear_discards_buffer_but_keeps_counters() {
        let mut led = NotificationLedger::new();
        led.capture(&body(1, "c", "x"));
        led.capture(&body(2, "c", "y"));
        led.clear();
        assert!(led.is_empty(), "clear discards buffered notifications");
        assert_eq!(led.received(), 2, "clear keeps the lifetime audit counters");
    }

    #[test]
    fn capture_notify_routes_notify_and_passes_others_through() {
        let mut led = NotificationLedger::new();
        let mut rows: Vec<Vec<u8>> = Vec::new();
        {
            let mut sink = capture_notify::<()>(&mut led, |s| {
                if let Surface::Row(r) = s {
                    rows.push(r.to_vec());
                }
                ControlFlow::Continue(())
            });
            // A notify is captured (Continue, never breaks the pump); a row flows
            // to the inner sink.
            assert!(matches!(sink(Surface::Notify(&body(5, "c", "z"))), ControlFlow::Continue(())));
            assert!(matches!(sink(Surface::Row(b"rowbytes")), ControlFlow::Continue(())));
        }
        assert_eq!(led.len(), 1, "notify was captured by the adapter");
        assert_eq!(rows, vec![b"rowbytes".to_vec()], "the row passed through to inner");
    }
}
