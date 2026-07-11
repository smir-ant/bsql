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
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
// `tokio::net::UnixStream` exists only on unix targets. A unix-socket host on a
// non-unix target is rejected at connect (see the driver's `Endpoint::Unix` arm),
// so no non-unix `Unix` arm is needed here.
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::time::Instant;

use bsql_postgres_proto::engine::Transport;

/// A tokio async stream that is EITHER a TCP socket or a unix-domain socket.
///
/// A local connection over an absolute-path host is a `tokio::net::UnixStream`;
/// every other host is a `tokio::net::TcpStream`. Rather than making the whole
/// `Connection` (a concrete `Core<TokioSocket>` after the engine collapse)
/// generic over the socket — a generic ripple through every re-export, the pool,
/// and the static assertions — the duality lives in this enum ONE level down.
/// `Connection` and the engine stay monomorphic over the single [`TokioSocket`]
/// type; the enum forwards its `AsyncRead`/`AsyncWrite` poll to the active arm
/// with one branch, a branch the reactor + syscall cost dwarfs (near-zero-cost).
/// A boxed `dyn` transport would instead add a vtable indirection per poll, and
/// `Transport`'s async-fn RPITIT is not even dyn-compatible; the enum is the
/// zero-cost shape.
///
/// Both tokio streams are `Unpin`, so the `AsyncRead`/`AsyncWrite` forwards use a
/// safe `Pin::new` re-pin of the active arm (no unsafe pin projection) — the
/// crate stays `#![forbid(unsafe_code)]`. Presenting the enum through the tokio
/// I/O traits keeps [`TokioSocket`]'s read-deadline logic byte-identical: it
/// still calls `self.stream.read(buf).await` on a single `AsyncRead + Unpin`
/// value, unaware whether the fd underneath is TCP or unix.
pub enum Sock {
    /// A TCP socket (the default, non-path host).
    Tcp(TcpStream),
    /// A unix-domain socket (an absolute-path host). Unix targets only.
    #[cfg(unix)]
    Unix(UnixStream),
}

impl AsyncRead for Sock {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // `get_mut` needs `Self: Unpin` (both arms are), so no unsafe projection.
        match self.get_mut() {
            Sock::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            Sock::Unix(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Sock {
    #[inline]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Sock::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(unix)]
            Sock::Unix(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Sock::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            Sock::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }

    #[inline]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Sock::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(unix)]
            Sock::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

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

    /// Arm an absolute read deadline and return an RAII guard that DISARMS on
    /// drop — whether the guarded scope ends by a normal return OR by the future
    /// being dropped (an outer `tokio::time::timeout` / `select!` losing the race,
    /// a cancelled task). This makes "no stale deadline survives a dropped verb
    /// future" a STRUCTURAL guarantee (the compiler's `Drop`), not caller
    /// discipline: a direct caller who wraps a deadline-armed verb in an outer
    /// timeout and loses the race can no longer strand an armed deadline that
    /// would fire a spurious `TimedOut` on the reused connection's NEXT verb.
    ///
    /// The happy path is byte-identical to a manual `arm` then `disarm`: on a
    /// normal return the guard's `Drop` does exactly the one atomic store the
    /// manual `disarm` did (drop the guard explicitly to place that store).
    pub(crate) fn arm_scoped(&self, at: Instant) -> DisarmOnDrop<'_> {
        self.arm(at);
        DisarmOnDrop { deadline: self }
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

/// An RAII guard, minted by [`ReadDeadline::arm_scoped`], that DISARMS its
/// [`ReadDeadline`] when it drops — on a normal return, an unwind, OR a
/// dropped/cancelled future. It carries only a borrow (no owned state), so it is
/// zero happy-path cost: its `Drop` performs the same single atomic store a
/// manual `disarm` would. Its whole purpose is that `Drop`, so it is `#[must_use]`
/// (dropping it immediately would disarm before the guarded read even runs).
#[must_use = "the guard disarms the read deadline on drop; binding it to `_` would disarm immediately"]
pub(crate) struct DisarmOnDrop<'a> {
    deadline: &'a ReadDeadline,
}

impl Drop for DisarmOnDrop<'_> {
    fn drop(&mut self) {
        self.deadline.disarm();
    }
}

/// A tokio [`Sock`] (TCP or unix) presented through the engine's [`Transport`]
/// seam.
///
/// Each op is genuinely asynchronous: `read`/`write` return `Pending` until the
/// socket is ready, so the engine's pump future suspends and is woken by tokio's
/// reactor — the wakeup path the always-ready scripted transports never exercise.
/// `write` performs exactly one `poll_write` attempt (the seam's one-attempt
/// contract — looping is the engine's job), `flush` drives tokio's writer flush
/// (a no-op for a bare stream socket), and `shutdown` closes the write half so
/// the peer sees a clean FIN.
pub(crate) struct TokioSocket {
    stream: Sock,
    deadline: Arc<ReadDeadline>,
}

// Footprint contract for the socket duality — pinned RELATIVELY, not absolutely.
//
// A tokio `TcpStream`/`UnixStream`'s size is a feature-unification-dependent
// INTERNAL detail (net-only resolves ~16 B; net+rt, as a `cargo test` build
// unifies via dev-deps, ~40 B), so an absolute `size_of` pin would be brittle
// and could even disagree between `cargo check` and `cargo test` of this very
// crate. These relative assertions instead capture exactly what OUR change
// costs, and hold regardless of tokio's absolute layout:
//   1. both arms are the same fd-backed size (no arm is secretly larger);
//   2. `Sock` adds only a single (pointer-sized) discriminant over one stream —
//      the whole cost of carrying TCP-or-unix;
//   3. `TokioSocket` adds only the shared read-deadline `Arc` pointer over `Sock`
//      (no other per-connection state crept in).
// (The blocking driver's socket rides std types with a stable layout, so it
// pins its ABSOLUTE 8/4 footprint; the asymmetry is deliberate.)
const _: () = {
    use core::mem::size_of;
    // Property 1 compares the two arms and so is unix-only (no `UnixStream` type
    // exists elsewhere). Properties 2 and 3 hold regardless: on a non-unix target
    // `Sock` is the single-variant `Tcp` (size == `TcpStream`, still within the
    // bound), and `TokioSocket` still adds only the deadline `Arc`.
    #[cfg(unix)]
    assert!(size_of::<TcpStream>() == size_of::<UnixStream>());
    assert!(size_of::<Sock>() <= size_of::<TcpStream>() + size_of::<usize>());
    assert!(size_of::<TokioSocket>() == size_of::<Sock>() + size_of::<Arc<ReadDeadline>>());
};

impl TokioSocket {
    /// Wrap an already-connected [`Sock`] (a TCP or unix stream), sharing the
    /// driver's read-deadline cell.
    pub(crate) fn new(stream: Sock, deadline: Arc<ReadDeadline>) -> Self {
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

    use super::{ReadDeadline, Sock, TokioSocket};

    #[tokio::test]
    async fn read_honours_an_armed_deadline_then_reads_after_disarm() {
        // A real connected socket pair on loopback; the server end stays silent
        // for the deadline window, then sends a byte.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let client = TcpStream::connect(addr).await.expect("connect");
        let (mut server, _peer) = listener.accept().await.expect("accept");

        let deadline = Arc::new(ReadDeadline::new());
        let mut socket = TokioSocket::new(Sock::Tcp(client), Arc::clone(&deadline));

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
