//! The tokio async socket behind the engine's [`Transport`] seam.
//!
//! [`TokioSocket`] wraps a `tokio::net::TcpStream`. UNLIKE the blocking driver's
//! socket — whose ops resolve on the first poll — every op here returns a REAL
//! `Pending` until the socket is ready, so the engine's pump future genuinely
//! suspends and is woken by tokio's reactor. That wakeup path is what the async
//! driver exists to drive.
//!
//! The plaintext-or-TLS multiplexer the engine is monomorphic over —
//! [`Wire`](bsql_postgres_core::tls::Wire) — lives in `bsql-postgres-core`,
//! shared with the blocking driver. This module supplies only the tokio socket
//! arm it wraps.
//!
//! # The read deadline (how `recv_notification` bounds a wait without stranding
//! the linear token)
//!
//! A notification wait must time out, but the engine owns the socket (it was
//! moved into the engine, possibly inside a TLS layer), so the driver cannot
//! reach it directly — and wrapping the `recv_notification` *verb future* in a
//! `tokio::time::timeout` is forbidden: dropping that future on a timeout strands
//! the linear liveness token (there is no re-mint). Instead the deadline lives
//! INSIDE the read. [`TokioSocket`] and the driver share one [`ReadDeadline`]
//! cell (via `Arc`); the driver arms an absolute deadline before
//! `recv_notification` and disarms it after. When armed, [`TokioSocket::read`]
//! bounds the socket read with `tokio::time::timeout_at`; an elapsed deadline
//! surfaces as a `TimedOut` read error, which [`is_would_block`](TokioSocket)
//! classifies as a read deadline (not a broken connection), so the engine reports
//! the quiet outcome and the token rides back alive. The verb future is never
//! dropped mid-flight, so the token is never stranded.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;

use bsql_postgres_proto::engine::Transport;

/// A read deadline shared between the driver and the [`TokioSocket`] the engine
/// owns.
///
/// The socket the engine drives was moved into the engine (and may be nested
/// inside a TLS layer), so the driver no longer has a direct handle to it. Both
/// the driver and the socket hold an `Arc` to the SAME `ReadDeadline`, so the
/// driver can arm a per-read deadline on a socket it cannot otherwise reach.
///
/// `armed` is the hot-path gate: a relaxed-acquire bool load is effectively free,
/// so a read on the steady-state query path (no deadline) pays only that load and
/// never locks the mutex. Only [`recv_notification`] arms a deadline, and only
/// then is the `at` mutex consulted.
///
/// [`recv_notification`]: crate::Connection::recv_notification
pub(crate) struct ReadDeadline {
    /// Hot-path gate: when `false`, reads take the deadline-free fast path.
    armed: AtomicBool,
    /// The armed absolute deadline. Read only when `armed` is observed `true`.
    at: Mutex<Option<Instant>>,
}

impl ReadDeadline {
    /// A disarmed deadline.
    pub(crate) fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            at: Mutex::new(None),
        }
    }

    /// Arm an absolute read deadline. Sets the instant BEFORE flipping the gate
    /// (release), so a reader that observes `armed` (acquire) also observes the
    /// instant.
    pub(crate) fn arm(&self, at: Instant) {
        *self.lock() = Some(at);
        self.armed.store(true, Ordering::Release);
    }

    /// Disarm: subsequent reads take the deadline-free fast path. The stored
    /// instant is irrelevant once the gate is down, so it is left as-is.
    pub(crate) fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
    }

    /// The armed deadline, or `None` if disarmed (the hot path).
    fn current(&self) -> Option<Instant> {
        if !self.armed.load(Ordering::Acquire) {
            return None;
        }
        *self.lock()
    }

    /// Lock the instant cell, recovering a poisoned guard.
    ///
    /// Poison recovery is not a data fallback: a poisoned lock means a thread
    /// panicked while holding it, which cannot happen here (no panic point exists
    /// under this lock, and the workspace builds with `panic = "abort"`). The
    /// recovery reclaims the guarded `Option<Instant>` so the cell keeps working.
    #[allow(
        clippy::disallowed_methods,
        reason = "mutex poison recovery — reclaims the guard after a panic (no panic point exists under this lock, and release builds abort on panic); not a silent data fallback"
    )]
    fn lock(&self) -> MutexGuard<'_, Option<Instant>> {
        self.at.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// A `tokio::net::TcpStream` presented through the engine's [`Transport`] seam.
///
/// Each op is genuinely asynchronous: `read`/`write` return `Pending` until the
/// socket is ready, so the engine's pump future suspends and is woken by tokio's
/// reactor — the wakeup path the always-ready scripted transports never exercise.
/// `write` performs exactly one `poll_write` attempt (the seam's one-attempt
/// contract — looping is the engine's job), `flush` drives tokio's writer flush
/// (a no-op for a bare `TcpStream`), and `shutdown` closes the write half so the
/// peer sees a clean FIN.
pub(crate) struct TokioSocket {
    stream: TcpStream,
    deadline: Arc<ReadDeadline>,
}

impl TokioSocket {
    /// Wrap an already-connected `TcpStream`, sharing the driver's read-deadline
    /// cell.
    pub(crate) fn new(stream: TcpStream, deadline: Arc<ReadDeadline>) -> Self {
        Self { stream, deadline }
    }
}

impl Transport for TokioSocket {
    type Error = io::Error;

    #[inline]
    fn is_would_block(err: &io::Error) -> bool {
        // A read deadline surfaces as `TimedOut` (armed deadline elapsed) or, for
        // a non-blocking socket, `WouldBlock`; both mean "no data within the
        // deadline", not a broken connection. Every other `io::ErrorKind` is a
        // genuine failure.
        matches!(
            err.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
        )
    }

    #[inline]
    async fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> Result<usize, io::Error> {
        match self.deadline.current() {
            // Hot path: no deadline armed — a plain async read that yields real
            // `Pending` until the socket is readable.
            None => self.stream.read(buf).await,
            // A deadline is armed (a notification wait): bound this read by the
            // absolute deadline. The deadline lives IN the read, not in a timeout
            // wrapping the verb future, so a fired deadline returns a `TimedOut`
            // error the engine classifies as a quiet outcome (the linear token
            // rides back) — it never drops the verb future and strands the token.
            Some(at) => match tokio::time::timeout_at(at, self.stream.read(buf)).await {
                Ok(result) => result,
                Err(_elapsed) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "read deadline elapsed",
                )),
            },
        }
    }

    #[inline]
    async fn write<'a>(&'a mut self, buf: &'a [u8]) -> Result<usize, io::Error> {
        // One write attempt, mirroring a single `poll_write` — the engine owns
        // the drain loop, so this never internally retries (NOT `write_all`).
        self.stream.write(buf).await
    }

    #[inline]
    async fn flush(&mut self) -> Result<(), io::Error> {
        // A bare `TcpStream` holds no userspace write buffer, so tokio's flush is
        // `Ok(())`; calling it keeps the seam's drain contract honest if the
        // inner writer ever buffers.
        self.stream.flush().await
    }

    #[inline]
    async fn shutdown(&mut self) -> Result<(), io::Error> {
        // Close the write half so the peer sees a clean FIN.
        self.stream.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    //! The read-deadline mechanism over a REAL socket pair and tokio reactor —
    //! the genuinely-new async bit the mock-transport offline tests cannot reach
    //! (they have no `timeout_at`). An armed deadline must make a read over a
    //! silent peer return `TimedOut` (classified as a would-block read, not a
    //! broken connection), and a disarmed socket must read normally again — so a
    //! notification wait times out from inside the read without breaking the
    //! connection.

    use std::sync::Arc;
    use std::time::Duration;

    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::Instant;

    use bsql_postgres_proto::engine::Transport;

    use super::{ReadDeadline, TokioSocket};

    #[tokio::test]
    async fn read_honours_an_armed_deadline_then_reads_after_disarm() {
        // A real connected socket pair on loopback; the server end stays silent
        // for the deadline window, then sends a byte.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let client = TcpStream::connect(addr).await.expect("connect");
        let (mut server, _peer) = listener.accept().await.expect("accept");

        let deadline = Arc::new(ReadDeadline::new());
        let mut socket = TokioSocket::new(client, Arc::clone(&deadline));

        // Arm a short deadline; the silent peer never sends, so the read must time
        // out (not hang, not error fatally).
        deadline.arm(Instant::now() + Duration::from_millis(120));
        let mut buf = [0u8; 16];
        match socket.read(&mut buf).await {
            Err(e) => assert!(
                TokioSocket::is_would_block(&e),
                "an elapsed deadline must classify as a would-block read, got {e:?}",
            ),
            Ok(n) => panic!("expected a timed-out read, got Ok({n})"),
        }

        // Disarm and let the peer speak: the same socket now reads normally,
        // proving the deadline never broke the connection.
        deadline.disarm();
        server.write_all(b"x").await.expect("server write");
        server.flush().await.expect("server flush");
        let n = socket.read(&mut buf).await.expect("read after disarm");
        assert_eq!(n, 1);
        assert_eq!(buf.first().copied(), Some(b'x'));
    }
}
