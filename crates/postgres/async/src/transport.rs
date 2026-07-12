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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

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
/// `armed` is the hot-path gate for the absolute deadline: a relaxed-acquire bool
/// load is effectively free, so a read that arms no absolute deadline pays only
/// that load and never locks the mutex. Only [`recv_notification`] /
/// [`reset_session`](crate::Connection::reset_session) arm the absolute deadline,
/// and only then is the `at` mutex consulted.
///
/// `steady_window_ms` is the always-on, MUTABLE per-read inactivity bound for the
/// steady-state query path (the client-liveness window): the milliseconds a plain
/// query read may sit silent before it elapses, or the sentinel [`NO_WINDOW`]
/// (`u64::MAX`) for the historical UNBOUNDED read. It starts at the connect-time
/// window (`Some` when the consumer configured a server-side `statement_timeout`,
/// see [`ConnectConfig::client_liveness_window`]) and is RE-DERIVED when the driver
/// observes a runtime `SET statement_timeout` and SUPPRESSED for the migration
/// runner (a relaxed `AtomicU64` — one store between verbs, one relaxed load per
/// read). A read consults it only after finding no absolute deadline armed; the
/// absolute deadline (a bounded reset/notification round-trip) always takes
/// PRIORITY, so those verbs keep their exact semantics.
///
/// `connect_window_ms` is the IMMUTABLE connect-time baseline (same encoding), the
/// value a `RESET statement_timeout` / `RESET ALL` / pool `reset_session` restores.
///
/// [`recv_notification`]: crate::Connection::recv_notification
/// [`ConnectConfig::client_liveness_window`]: bsql_postgres_core::ConnectConfig::client_liveness_window
pub(crate) struct ReadDeadline {
    /// Hot-path gate: when `false`, reads arm no ABSOLUTE deadline (they may
    /// still take the steady-window path — see `steady_window_ms`).
    armed: AtomicBool,
    /// The armed absolute deadline. Read only when `armed` is observed `true`.
    at: Mutex<Option<Instant>>,
    /// The mutable steady-state window in ms ([`NO_WINDOW`] = unbounded).
    steady_window_ms: AtomicU64,
    /// The immutable connect-time baseline in ms ([`NO_WINDOW`] = unbounded).
    connect_window_ms: u64,
    /// The immutable `connect_timeout` (seconds) — the network-round-trip margin a
    /// runtime `SET statement_timeout` re-derivation adds to the raised budget
    /// ([`window_after_statement`](bsql_postgres_core::window_after_statement)).
    /// Stored here so [`observe_statement_timeout`](Self::observe_statement_timeout)
    /// is self-contained: BOTH the connection verbs and the transaction-guard verbs
    /// (which hold only this shared cell, not the driver's config) re-derive the
    /// window through the ONE shared authority with no extra plumbing.
    connect_timeout_secs: u64,
}

/// The `steady_window_ms` / `connect_window_ms` sentinel for "no client window"
/// (the historical unbounded steady read). `u64::MAX` ms is ~5.8×10^8 years, far
/// beyond any real budget (PG's ceiling is `i32::MAX` ms), so it can never be a
/// real window value.
const NO_WINDOW: u64 = u64::MAX;

/// Encode an optional window `Duration` as its `steady_window_ms` value. A real
/// window is always whole ms well below the sentinel (PG's ceiling + connect
/// budget), so `min(NO_WINDOW - 1)` only guards against an absurd input, never a
/// real one.
fn window_to_ms(window: Option<Duration>) -> u64 {
    match window {
        None => NO_WINDOW,
        Some(d) => match u64::try_from(d.as_millis()) {
            Ok(ms) => ms.min(NO_WINDOW.saturating_sub(1)),
            Err(_) => NO_WINDOW,
        },
    }
}

/// Decode a `steady_window_ms` / `connect_window_ms` value back to an optional
/// window `Duration` ([`NO_WINDOW`] → `None`).
fn ms_to_window(ms: u64) -> Option<Duration> {
    if ms == NO_WINDOW {
        None
    } else {
        Some(Duration::from_millis(ms))
    }
}

/// The effective bound for one socket read, resolved by [`ReadDeadline::read_bound`].
enum ReadBound {
    /// No bound: the deadline-free fast path (an unbounded async read).
    None,
    /// An ABSOLUTE deadline (a bounded reset / notification round-trip).
    At(Instant),
    /// A per-read inactivity WINDOW (the steady-state liveness bound).
    Within(Duration),
}

impl ReadDeadline {
    /// A deadline with no absolute arming and the given connect-time steady
    /// window (`None` for the historical unbounded steady read) plus the
    /// `connect_timeout` (seconds) a runtime re-derivation adds as its
    /// network-round-trip margin. The connect window is BOTH the current window and
    /// the immutable baseline a later `RESET` / pool reset restores.
    pub(crate) fn new(steady_window: Option<Duration>, connect_timeout_secs: u64) -> Self {
        let ms = window_to_ms(steady_window);
        Self {
            armed: AtomicBool::new(false),
            at: Mutex::new(None),
            steady_window_ms: AtomicU64::new(ms),
            connect_window_ms: ms,
            connect_timeout_secs,
        }
    }

    /// The connect-time baseline window (what a `RESET` / pool reset restores).
    pub(crate) fn connect_window(&self) -> Option<Duration> {
        ms_to_window(self.connect_window_ms)
    }

    /// Re-derive the steady window from an EXECUTED statement's observed effect on
    /// the server's `statement_timeout` — so the window is never left STALE below a
    /// budget a runtime `SET`/`set_config` raised (which would falsely cut a query
    /// the server now allows). Routes through the ONE shared authority
    /// [`window_after_statement`](bsql_postgres_core::window_after_statement), so
    /// every observing verb — a connection verb OR a transaction-guard verb, which
    /// holds only this shared cell — re-derives identically. Only the caller's
    /// success gates it (a failed `SET` changed nothing on the server); a
    /// `WindowAction::Unchanged` leaves the window untouched (the common path).
    pub(crate) fn observe_statement_timeout(&self, sql: &str) {
        match bsql_postgres_core::window_after_statement(
            sql,
            self.connect_timeout_secs,
            self.connect_window(),
        ) {
            bsql_postgres_core::WindowAction::Unchanged => {}
            bsql_postgres_core::WindowAction::Set(window) => self.set_steady_window(window),
        }
    }

    /// Re-derive the steady window to `window` (a runtime `SET statement_timeout`
    /// observed, or a `RESET` back to the connect baseline). One relaxed store,
    /// read by the socket on its next read — no lock (the window is advisory, and
    /// a verb and its own socket read never run concurrently on one connection).
    pub(crate) fn set_steady_window(&self, window: Option<Duration>) {
        self.steady_window_ms
            .store(window_to_ms(window), Ordering::Relaxed);
    }

    /// SUPPRESS the steady window for a trusted long operation (the migration
    /// runner), returning an RAII guard that RESTORES the pre-suppression value on
    /// drop (normal return OR a dropped future). While suppressed, a steady read is
    /// UNBOUNDED — a migration's own server-side `statement_timeout` governs it, so
    /// bsql never client-cuts its own trusted long op (a `CREATE INDEX
    /// CONCURRENTLY` behind a `SET statement_timeout = 0`).
    pub(crate) fn suppress_scoped(&self) -> RestoreWindowOnDrop<'_> {
        let saved = self.steady_window_ms.swap(NO_WINDOW, Ordering::Relaxed);
        RestoreWindowOnDrop { deadline: self, saved }
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

    /// The effective bound for the next read: the armed ABSOLUTE deadline if one
    /// is armed (a bounded reset / notification round-trip — highest priority),
    /// otherwise the steady-state inactivity WINDOW, otherwise none (the hot
    /// path). A read consults the `at` mutex only when the `armed` gate is `true`.
    fn read_bound(&self) -> ReadBound {
        if self.armed.load(Ordering::Acquire)
            && let Some(at) = *self.lock()
        {
            return ReadBound::At(at);
        }
        match ms_to_window(self.steady_window_ms.load(Ordering::Relaxed)) {
            Some(w) => ReadBound::Within(w),
            None => ReadBound::None,
        }
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

/// An RAII guard, minted by [`ReadDeadline::suppress_scoped`], that RESTORES the
/// steady window it swapped out when it drops (a normal return, an unwind, OR a
/// dropped future). It carries the saved encoded window, so a suppressed
/// migration run always re-arms whatever window was in effect before it — the
/// window can never be left suppressed by an early return or a cancelled future.
#[must_use = "the guard restores the steady window on drop; binding it to `_` would restore immediately"]
pub(crate) struct RestoreWindowOnDrop<'a> {
    deadline: &'a ReadDeadline,
    saved: u64,
}

impl Drop for RestoreWindowOnDrop<'_> {
    fn drop(&mut self) {
        self.deadline
            .steady_window_ms
            .store(self.saved, Ordering::Relaxed);
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
        match self.deadline.read_bound() {
            // Hot path: no bound — a plain async read that yields real `Pending`
            // until the socket is readable.
            ReadBound::None => self.stream.read(buf).await,
            // An ABSOLUTE deadline is armed (a notification wait / reset): bound
            // this read by it. The deadline lives IN the read, not in a timeout
            // wrapping the verb future, so a fired deadline returns a `TimedOut`
            // error the engine classifies (a quiet outcome for `recv_notification`,
            // a fatal one for `reset_session` / a query) — it never drops the verb
            // future and strands the linear token.
            ReadBound::At(at) => match tokio::time::timeout_at(at, self.stream.read(buf)).await {
                Ok(result) => result,
                Err(_elapsed) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "read deadline elapsed",
                )),
            },
            // A steady-state inactivity WINDOW (a configured `statement_timeout`
            // derived the client-liveness bound): bound THIS read by the relative
            // window, re-armed per read — so it fires only on total silence longer
            // than the window (a black-holed peer), never on a legitimately slow
            // stream whose bytes keep the window fresh. A fired window is `TimedOut`,
            // which the query pump treats as a FATAL transport error (no quiet arm
            // there) → a classified `DriverError::Timeout`, and the connection is
            // dropped — never the `tcp_retries2` / never-detected-black-hole hang.
            ReadBound::Within(window) => {
                match tokio::time::timeout(window, self.stream.read(buf)).await {
                    Ok(result) => result,
                    Err(_elapsed) => Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "read liveness window elapsed",
                    )),
                }
            }
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

    use std::sync::atomic::Ordering;
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

        let deadline = Arc::new(ReadDeadline::new(None, 0));
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

    #[tokio::test]
    async fn steady_window_bounds_a_read_without_an_armed_deadline() {
        // A steady-state inactivity window is armed via the immutable field (no
        // `arm`), so a read over a silent peer with NO absolute deadline still
        // times out — the black-hole bound. A read that receives data returns it
        // (the window is a ceiling, not a delay).
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let client = TcpStream::connect(addr).await.expect("connect");
        let (mut server, _peer) = listener.accept().await.expect("accept");

        let deadline = Arc::new(ReadDeadline::new(Some(Duration::from_millis(120)), 0));
        let mut socket = TokioSocket::new(Sock::Tcp(client), Arc::clone(&deadline));

        // No `arm`: only the immutable steady window is in effect. A silent peer
        // must make the read elapse into a `TimedOut` (a fatal transport error on
        // the query path — never the historical unbounded hang).
        let mut buf = [0u8; 16];
        match socket.read(&mut buf).await {
            Err(e) => assert!(
                TokioSocket::is_would_block(&e),
                "an elapsed steady window must surface as a timed-out read, got {e:?}",
            ),
            Ok(n) => panic!("expected a timed-out read, got Ok({n})"),
        }

        // A peer that speaks inside the window returns its bytes — the window
        // never breaks a live connection.
        server.write_all(b"y").await.expect("server write");
        server.flush().await.expect("server flush");
        let n = socket.read(&mut buf).await.expect("read within window");
        assert_eq!(n, 1);
        assert_eq!(buf.first().copied(), Some(b'y'));
    }

    #[test]
    fn observe_statement_timeout_re_derives_the_steady_window() {
        // The shared observation primitive BOTH the connection verbs and the
        // transaction-guard verbs route through: a runtime `SET`/`RESET`/`set_config`
        // re-derives the steady window; an unrelated statement leaves it untouched.
        // Connect-time budget 300 ms + connect_timeout 2 s = 2300 ms baseline.
        let d = ReadDeadline::new(Some(Duration::from_millis(2300)), 2);
        let steady = || super::ms_to_window(d.steady_window_ms.load(Ordering::Relaxed));
        assert_eq!(steady(), Some(Duration::from_millis(2300)), "connect baseline");

        // A runtime SET RAISES the budget to 30 s → window = 30 s + 2 s = 32 s.
        d.observe_statement_timeout("SET statement_timeout = '30s'");
        assert_eq!(steady(), Some(Duration::from_millis(32_000)));

        // An unrelated statement leaves the (raised) window exactly as it is.
        d.observe_statement_timeout("SELECT 1");
        assert_eq!(steady(), Some(Duration::from_millis(32_000)));

        // A `set_config` of statement_timeout cannot be pinned → DISARM (unbounded).
        d.observe_statement_timeout("SELECT set_config('statement_timeout','5min',false)");
        assert_eq!(steady(), None, "set_config disarms the window (fail-safe)");

        // A RESET restores the connect-time baseline (not the disarmed None).
        d.observe_statement_timeout("RESET statement_timeout");
        assert_eq!(steady(), Some(Duration::from_millis(2300)));
    }
}
