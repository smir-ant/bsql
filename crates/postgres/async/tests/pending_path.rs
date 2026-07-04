#![forbid(unsafe_code)]

//! Offline de-risk tests for the async cutover's genuinely-new paths, run under
//! a real tokio runtime over mock transports (no socket, no live PG — fully
//! deterministic):
//!
//! 1. The engine pump correctly SUSPENDS on a transport `Pending` and resumes on
//!    wake — the wakeup path the always-ready scripted transports never exercise.
//!    Every read here returns `Pending` on its first poll (re-waking) then the
//!    bytes, so connect and a follow-up verb each drive a suspend/resume cycle.
//! 2. A would-block read during `recv_notification` is the QUIET outcome: the
//!    linear token rides back in `Ok` and a follow-up verb works — the
//!    deadline-in-read contract that must NOT strand the token.
//!
//! The real `TokioSocket` read-deadline mechanism (arm/disarm + `timeout_at`
//! over a real socket/reactor) is unit-tested in the driver's `transport`
//! module; the full stack against live PG is `sq_live`.

use core::convert::Infallible;
use core::future::Future;
use core::ops::ControlFlow;
use core::pin::Pin;
use core::task::{Context, Poll};

use bsql_postgres_core::{materialize, DriverError, Notification};
use bsql_postgres_proto::engine::{self, CommandStatus, NotifyStatus, Surface, Transport};
use bsql_postgres_proto::{Credentials, Ident};

// ── Wire fixtures (clean Result-threading: no unwrap in these helpers) ──────

/// Build a tagged, length-prefixed wire frame. The fixtures are a handful of
/// bytes, so the `u32` length always fits; a `?` propagates the impossible
/// overflow rather than fabricating a length.
fn frame(tag: u8, body: &[u8]) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let len = u32::try_from(body.len() + 4)?;
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(tag);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

/// The Trust handshake reply: `AuthenticationOk` + `BackendKeyData` + a clean
/// `ReadyForQuery`.
fn trust_handshake() -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut key = 1234_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&5678_i32.to_be_bytes());
    let mut reply = Vec::new();
    reply.extend_from_slice(&frame(b'R', &0_i32.to_be_bytes())?); // AuthenticationOk
    reply.extend_from_slice(&frame(b'K', &key)?); // BackendKeyData
    reply.extend_from_slice(&frame(b'Z', b"I")?); // ReadyForQuery (Idle)
    Ok(reply)
}

/// A bare `ReadyForQuery` — the reply to a `Sync` (ping).
fn ready_for_query() -> Result<Vec<u8>, std::num::TryFromIntError> {
    frame(b'Z', b"I")
}

/// A `NotificationResponse` ('A') frame: `[i32 pid][channel CString][payload CString]`.
fn notification_frame(
    pid: i32,
    channel: &str,
    payload: &str,
) -> Result<Vec<u8>, std::num::TryFromIntError> {
    let mut body = pid.to_be_bytes().to_vec();
    body.extend_from_slice(channel.as_bytes());
    body.push(0);
    body.extend_from_slice(payload.as_bytes());
    body.push(0);
    frame(b'A', &body)
}

/// Drain up to `buf.len()` bytes from `src` at `*cursor`, advancing it.
fn drain(src: &[u8], cursor: &mut usize, buf: &mut [u8]) -> usize {
    let remaining = src.len().saturating_sub(*cursor);
    let n = remaining.min(buf.len());
    let end = cursor.saturating_add(n);
    if let (Some(dst), Some(s)) = (buf.get_mut(..n), src.get(*cursor..end)) {
        dst.copy_from_slice(s);
    }
    *cursor = end;
    n
}

// ── Test 1: the engine pump survives a Pending read ─────────────────────────

/// A scripted server whose every `read` returns `Pending` on its first poll
/// (after re-waking the task) and the bytes on the next, so each read drives the
/// engine pump through one suspend/resume cycle.
struct PendingEachRead {
    inbound: Vec<u8>,
    cursor: usize,
}

impl Transport for PendingEachRead {
    type Error = Infallible;

    fn is_would_block(err: &Infallible) -> bool {
        match *err {}
    }

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ReadOnce {
            server: self,
            buf,
            polled_once: false,
        }
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        core::future::ready(Ok(buf.len()))
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        core::future::ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        core::future::ready(Ok(()))
    }
}

/// One read that yields `Pending` on its first poll (re-waking immediately) then
/// delivers bytes — the suspend/resume cycle a real socket produces.
struct ReadOnce<'a> {
    server: &'a mut PendingEachRead,
    buf: &'a mut [u8],
    polled_once: bool,
}

impl Future for ReadOnce<'_> {
    type Output = Result<usize, Infallible>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // `ReadOnce` holds only references + a bool, so it is `Unpin`; `get_mut`
        // needs no `unsafe`.
        let this = self.get_mut();
        if !this.polled_once {
            this.polled_once = true;
            // Re-wake so the runtime polls again — the suspend/resume cycle.
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let n = drain(&this.server.inbound, &mut this.server.cursor, this.buf);
        Poll::Ready(Ok(n))
    }
}

#[tokio::test]
async fn a_verb_completes_across_a_pending_read() {
    let user = Ident::try_from_str("test").expect("valid ident");
    let mut inbound = trust_handshake().expect("handshake fixture");
    inbound.extend_from_slice(&ready_for_query().expect("rfq fixture")); // ping reply
    let server = PendingEachRead { inbound, cursor: 0 };

    let (mut engine, live) =
        engine::open_owned(server, &user, None, &[], Credentials::Trust).expect("open");
    // connect drives the connecting pump over the Pending-returning transport.
    let live = engine.connect(live).await.expect("connect across pending");
    // A follow-up active verb drives the active pump, also across a Pending read.
    let outcome = engine
        .ping(live, |_s: Surface<'_>| ControlFlow::Continue(()))
        .await
        .expect("ping across pending");
    assert_eq!(outcome.status, CommandStatus::Completed);
}

// ── Test 2: a would-block read during recv_notification is Quiet ────────────

/// A scripted server that serves the handshake, then returns ONE would-block
/// read (standing in for the notification deadline), then serves a follow-up
/// `ReadyForQuery`.
struct WouldBlockOnce {
    before: Vec<u8>,
    before_cursor: usize,
    wb_pending: bool,
    after: Vec<u8>,
    after_cursor: usize,
}

impl WouldBlockOnce {
    fn read_now(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        if self.before_cursor < self.before.len() {
            return Ok(drain(&self.before, &mut self.before_cursor, buf));
        }
        if self.wb_pending {
            self.wb_pending = false;
            return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock));
        }
        if self.after_cursor < self.after.len() {
            return Ok(drain(&self.after, &mut self.after_cursor, buf));
        }
        // Nothing scripted left: a would-block keeps the connection alive without
        // a spurious `Ok(0)` EOF.
        Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
    }
}

impl Transport for WouldBlockOnce {
    type Error = std::io::Error;

    fn is_would_block(err: &std::io::Error) -> bool {
        matches!(
            err.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        )
    }

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, std::io::Error>> + Send + 'a {
        core::future::ready(self.read_now(buf))
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, std::io::Error>> + Send + 'a {
        core::future::ready(Ok(buf.len()))
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), std::io::Error>> + Send + 'a {
        core::future::ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), std::io::Error>> + Send + 'a {
        core::future::ready(Ok(()))
    }
}

#[tokio::test]
async fn recv_notification_quiet_on_would_block_keeps_the_token() {
    let user = Ident::try_from_str("test").expect("valid ident");
    let server = WouldBlockOnce {
        before: trust_handshake().expect("handshake fixture"),
        before_cursor: 0,
        wb_pending: true,
        after: ready_for_query().expect("rfq fixture"),
        after_cursor: 0,
    };
    let (mut engine, live) =
        engine::open_owned(server, &user, None, &[], Credentials::Trust).expect("open");
    let live = engine.connect(live).await.expect("connect");

    // The read returns would-block (the deadline): the verb reports Quiet and the
    // token rides back alive — not a fatal error, and not a stranded token.
    let outcome = engine
        .recv_notification(live, |_s: Surface<'_>| ControlFlow::Continue(()))
        .await
        .expect("a would-block notification wait is not fatal");
    assert_eq!(outcome.status, NotifyStatus::Quiet);

    // The surviving token drives a follow-up verb to completion.
    let outcome = engine
        .ping(outcome.live, |_s: Surface<'_>| ControlFlow::Continue(()))
        .await
        .expect("follow-up ping after a quiet wait");
    assert_eq!(outcome.status, CommandStatus::Completed);
}

// ── Test 3: a delivered notification is Received and parses correctly ────────

#[tokio::test]
async fn recv_notification_received_parses_the_payload_and_keeps_the_token() {
    let user = Ident::try_from_str("test").expect("valid ident");
    let mut inbound = trust_handshake().expect("handshake fixture");
    inbound.extend_from_slice(&notification_frame(4242, "bsql_ch", "hello").expect("notif frame"));
    inbound.extend_from_slice(&ready_for_query().expect("rfq fixture")); // follow-up ping reply
    let server = PendingEachRead { inbound, cursor: 0 };

    let (mut engine, live) =
        engine::open_owned(server, &user, None, &[], Credentials::Trust).expect("open");
    let live = engine.connect(live).await.expect("connect");

    // Mirror the driver's capture: a Notify surfaces the raw body, which
    // `parse_notification` decodes — the Received-arm path the Quiet test misses.
    let mut captured: Option<Result<Notification, DriverError>> = None;
    let outcome = engine
        .recv_notification(live, |s| {
            if let Surface::Notify(body) = s {
                captured = Some(materialize::parse_notification(body));
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        })
        .await
        .expect("recv_notification with a delivered notification");
    assert_eq!(outcome.status, NotifyStatus::Received);
    let notification = captured
        .expect("a Notify was surfaced")
        .expect("the notification payload parses");
    assert_eq!(notification.channel, "bsql_ch");
    assert_eq!(notification.payload, "hello");
    assert_eq!(notification.pid, 4242);

    // The token rode back in Ok — the connection is alive and a follow-up works.
    let outcome = engine
        .ping(outcome.live, |_s: Surface<'_>| ControlFlow::Continue(()))
        .await
        .expect("follow-up ping after a received notification");
    assert_eq!(outcome.status, CommandStatus::Completed);
}
