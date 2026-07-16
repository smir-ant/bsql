//! The blocking I/O socket behind the engine's [`Transport`] seam.
//!
//! [`SyncSocket`] wraps a [`SyncSock`] — a TCP-or-unix std stream — and presents
//! it through the engine's [`Transport`] quartet: every op is a single blocking
//! std op, so each future resolves on its FIRST poll (never `Pending`) and the
//! engine's `poll_once` single-poll executor drives the whole sans-IO engine over
//! it with no async runtime.
//!
//! # Carrying TCP and unix behind ONE concrete socket type
//!
//! A local connection over an absolute-path host is a unix-domain socket
//! (`std::os::unix::net::UnixStream`); every other host is a `std::net::TcpStream`.
//! Rather than making the whole `Connection` (a concrete `Core<SyncSocket>` after
//! the engine collapse) generic over the socket — a generic ripple through every
//! re-export, the pool, and the static assertions — the duality lives in a single
//! [`SyncSock`] enum ONE level down. `Connection` and the engine stay monomorphic
//! over the single `SyncSocket` type; the enum dispatches TCP-vs-unix with one
//! branch inside each leaf syscall op, a branch the kernel read/write cost dwarfs
//! (near-zero-cost). A boxed `dyn` transport would instead add a vtable
//! indirection per syscall, and `Transport`'s async-fn RPITIT is not even
//! dyn-compatible; the enum is the zero-cost shape.
//!
//! The plaintext-or-TLS multiplexer the engine is monomorphic over —
//! [`Wire`](bsql_postgres_core::tls::Wire) — lives in `bsql-postgres-core`,
//! shared with the async driver so the multiplexer exists once. This module
//! supplies only the blocking socket arm it wraps.

use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
// The unix-domain-socket arm exists only on unix targets — `std::os::unix` is not
// present elsewhere. A unix-socket host on a non-unix target is rejected at connect
// with a classified `DriverError::Config` (never a silent TCP fallback), so nothing
// below needs a non-unix `Unix` arm.
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

// Used only by the two `#[cfg(unix)]` footprint pins below (they capture the unix
// fd layout); on a non-unix target there is no socket duality to pin.
#[cfg(unix)]
use bsql_postgres_core::footprint_pin;
use bsql_postgres_proto::engine::Transport;

/// Process-wide monotonic epoch for the connect-phase deadline.
///
/// A [`std::time::Instant`] cannot live in an atomic, so the shared connect
/// deadline is stored as nanoseconds elapsed from this fixed epoch (captured
/// once, on first use). Only DIFFERENCES from it are ever taken, so the absolute
/// value is irrelevant — it exists solely to turn two `Instant`s into a
/// comparable `u64`.
static CONNECT_EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// The smallest `SO_RCVTIMEO` the connect-phase re-arm sets while budget remains.
///
/// `std`'s `set_read_timeout(Some(Duration::ZERO))` ERRORS ("cannot set a 0
/// duration timeout"), and a sub-`SO_RCVTIMEO`-granularity value can round to 0
/// (which DISABLES the timeout — the very hang this guards against), so a
/// remaining budget below this floor is armed AT this floor (an overshoot of at
/// most 1 ms, negligible against `connect_timeout`). A budget that has actually
/// reached zero short-circuits (never re-armed to a floor).
const MIN_CONNECT_READ_BUDGET: Duration = Duration::from_millis(1);

/// The classified read error a spent connect-phase budget yields. `TimedOut` is
/// lifted to [`DriverError::Timeout`](bsql_postgres_core::DriverError::Timeout) —
/// the SAME class a per-read `SO_RCVTIMEO` elapse produces, so the two are one
/// classification.
const CONNECT_DEADLINE_ELAPSED: &str =
    "connect aggregate deadline (connect_timeout) elapsed during the startup/auth handshake";

/// A connect-phase AGGREGATE wall-clock deadline, shared (by `Arc`) between the
/// blocking driver and the engine-owned [`SyncSocket`].
///
/// # Why the socket enforces it
///
/// The blocking driver drives the WHOLE startup/auth handshake inside a single
/// `poll_once` (every leaf read blocks, so the handshake future resolves in one
/// poll), so the driver CANNOT interpose a wall-clock check between the engine's
/// individual reads. Without an aggregate bound, a hostile or broken server that
/// DRIPS endless state-non-advancing frames (`NoticeResponse` / `ParameterStatus`)
/// each within the per-read `SO_RCVTIMEO` window pins the connecting thread
/// forever — a few such connects exhaust a blocking pool. This handle lets the
/// socket's own [`read`](Transport::read) enforce the budget: the driver arms it
/// with `connect_timeout` before the handshake and DISARMS it the instant the
/// handshake completes. It is the sync analogue of the async driver's
/// `tokio::time::timeout(connect_timeout, connect_inner)`, which bounds the whole
/// connect in one place.
///
/// # Zero steady-state cost
///
/// A disarmed deadline is a single relaxed `AtomicU64` load per steady read —
/// value `0`, taken branch skipped, no `Instant::now`, no re-arm — so a
/// connection's steady read path is byte-for-byte the historical one. Only the
/// connect phase (armed) pays the clock read + the re-arm.
///
/// Stored as nanoseconds from [`CONNECT_EPOCH`]; `0` means disarmed.
#[derive(Clone, Debug)]
pub struct ConnectDeadline(Arc<AtomicU64>);

/// What the connect-phase deadline dictates for the read about to happen.
enum ConnectReadPhase {
    /// Disarmed (steady state, or handshake complete): read normally.
    Disarmed,
    /// The aggregate budget is spent: fail the read NOW (never re-arm to zero).
    Expired,
    /// Budget remains: re-arm `SO_RCVTIMEO` to this (floored) value first.
    Remaining(Duration),
}

impl ConnectDeadline {
    /// Arm the deadline at `now + budget`, captured as nanoseconds from the
    /// process epoch. A budget so large it would overflow the epoch offset is
    /// clamped to the maximum (effectively unbounded within `u64` nanoseconds ≈
    /// 584 years), never to `0` (which would read as disarmed).
    #[must_use]
    pub fn armed(budget: Duration) -> Self {
        // Explicit `match` (not `unwrap_or`) — the workspace bans the
        // silent-fallback `unwrap_or*` family; here the saturation to `u64::MAX`
        // is a deliberate, documented overflow clamp, never a swallowed error.
        let deadline_nanos = match Instant::now().checked_add(budget) {
            Some(deadline) => {
                let nanos = deadline.saturating_duration_since(*CONNECT_EPOCH).as_nanos();
                match u64::try_from(nanos) {
                    // `.max(1)` reserves `0` for disarmed.
                    Ok(n) => n.max(1),
                    Err(_) => u64::MAX,
                }
            }
            None => u64::MAX,
        };
        Self(Arc::new(AtomicU64::new(deadline_nanos)))
    }

    /// Disarm: subsequent reads take the [`ConnectReadPhase::Disarmed`] fast path.
    /// Called by the driver the instant the handshake completes.
    pub fn disarm(&self) {
        self.0.store(0, Ordering::Relaxed);
    }

    /// The read-time decision.
    ///
    /// `Relaxed` is sufficient: the arm-before-handshake and disarm-after-handshake
    /// stores are ordered w.r.t. the socket's reads by the single-threaded connect
    /// drive and the `Send` hand-off that publishes the connection to any later
    /// thread, and the flag value is the only shared datum.
    fn phase(&self) -> ConnectReadPhase {
        let deadline = self.0.load(Ordering::Relaxed);
        if deadline == 0 {
            return ConnectReadPhase::Disarmed;
        }
        let now_nanos = Instant::now().saturating_duration_since(*CONNECT_EPOCH).as_nanos();
        // Compare in `u128` (the `u64` deadline widens losslessly) so no `u128`→`u64`
        // clamp on `now` is needed — avoiding both the banned `unwrap_or*` family and
        // its `manual_unwrap_or` match shape.
        match u128::from(deadline).checked_sub(now_nanos) {
            None | Some(0) => ConnectReadPhase::Expired,
            Some(remaining) => match u64::try_from(remaining) {
                // `remaining <= deadline` (a `u64`), so it always fits `u64`.
                Ok(nanos) => ConnectReadPhase::Remaining(
                    Duration::from_nanos(nanos).max(MIN_CONNECT_READ_BUDGET),
                ),
                // Structurally unreachable; the SAFE fallback is Expired (a false
                // deadline-spent, never a false Remaining that could hang).
                Err(_) => ConnectReadPhase::Expired,
            },
        }
    }
}

/// A blocking std stream that is EITHER a TCP socket or a unix-domain socket.
///
/// The two arms carry identical capability — both `std::net::TcpStream` and
/// `std::os::unix::net::UnixStream` are file-descriptor handles offering the
/// same `Read`/`Write`, `set_{read,write}_timeout`, `try_clone`, and `shutdown`
/// — so this enum forwards each to the active arm. It is used for BOTH the
/// engine-owned data socket (wrapped in [`SyncSocket`]) and the connection's
/// `try_clone`d control handle (which arms read/write timeouts on a fd the engine
/// otherwise owns), so the TCP/unix duality is expressed exactly once.
///
/// `TCP_NODELAY` is deliberately NOT a method here: it is meaningless on
/// `AF_UNIX` and is set on the raw `TcpStream` before it is wrapped, so no unix
/// arm ever needs to skip it.
pub enum SyncSock {
    /// A TCP socket (the default, non-path host).
    Tcp(TcpStream),
    /// A unix-domain socket (an absolute-path host). Unix targets only.
    #[cfg(unix)]
    Unix(UnixStream),
}

// A `TcpStream`/`UnixStream` is a 4-byte fd handle; the two-arm enum is that plus
// a discriminant, rounded to 8. The pin makes the +4 B (over a bare `TcpStream`)
// the socket duality costs a visible, reviewed number rather than a silent drift.
// Unix-only: it captures the unix fd layout (a 4-byte `RawFd`, align 4); a non-unix
// `TcpStream` wraps a platform handle of a different size/align, so the pin does not
// apply there (and there is no unix arm to cost).
#[cfg(unix)]
footprint_pin!(SyncSock, size = 8, align = 4);

impl SyncSock {
    /// Set the blocking read timeout on the underlying fd (`None` = block
    /// indefinitely). Used to bound the connect-phase handshake and each
    /// `recv_notification` wait, then disarm.
    #[inline]
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            SyncSock::Tcp(s) => s.set_read_timeout(dur),
            #[cfg(unix)]
            SyncSock::Unix(s) => s.set_read_timeout(dur),
        }
    }

    /// Set the blocking write timeout on the underlying fd (`None` = block
    /// indefinitely).
    #[inline]
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            SyncSock::Tcp(s) => s.set_write_timeout(dur),
            #[cfg(unix)]
            SyncSock::Unix(s) => s.set_write_timeout(dur),
        }
    }

    /// Duplicate the fd into a second handle sharing the same kernel socket — so
    /// a timeout armed on the clone applies to the engine's own reads and writes.
    #[inline]
    pub fn try_clone(&self) -> io::Result<SyncSock> {
        match self {
            SyncSock::Tcp(s) => s.try_clone().map(SyncSock::Tcp),
            #[cfg(unix)]
            SyncSock::Unix(s) => s.try_clone().map(SyncSock::Unix),
        }
    }

    /// Whether this is a unix-domain socket.
    ///
    /// Always `false` on a non-unix target — no `Unix` arm exists there and a
    /// unix-socket host is rejected before a socket is ever built — so a caller
    /// gating the TLS-only steps on socket kind stays portable.
    #[inline]
    pub fn is_unix(&self) -> bool {
        #[cfg(unix)]
        {
            matches!(self, SyncSock::Unix(_))
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

    /// Shut the write half so the peer sees a clean FIN.
    #[inline]
    fn shutdown_write(&self) -> io::Result<()> {
        match self {
            SyncSock::Tcp(s) => s.shutdown(Shutdown::Write),
            #[cfg(unix)]
            SyncSock::Unix(s) => s.shutdown(Shutdown::Write),
        }
    }
}

impl Read for SyncSock {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            SyncSock::Tcp(s) => s.read(buf),
            #[cfg(unix)]
            SyncSock::Unix(s) => s.read(buf),
        }
    }
}

impl Write for SyncSock {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            SyncSock::Tcp(s) => s.write(buf),
            #[cfg(unix)]
            SyncSock::Unix(s) => s.write(buf),
        }
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        match self {
            SyncSock::Tcp(s) => s.flush(),
            #[cfg(unix)]
            SyncSock::Unix(s) => s.flush(),
        }
    }
}

/// A blocking [`SyncSock`] (TCP or unix) presented through the engine's
/// [`Transport`] seam.
///
/// Every op is a single blocking std call evaluated eagerly, then handed back as
/// an already-resolved [`core::future::Ready`] — so the seam's futures never
/// return `Pending` and `poll_once` completes them in one poll. `write` performs
/// exactly one `write` syscall (the seam's one-attempt contract — looping is the
/// engine's job), `flush` is a no-op (a stream socket has no userspace buffer),
/// and `shutdown` closes the write half so the peer sees a clean FIN.
pub struct SyncSocket {
    stream: SyncSock,
    /// The connect-phase aggregate deadline (see [`ConnectDeadline`]). Disarmed
    /// (a single relaxed load) in steady state; armed only across the handshake,
    /// where the blocking driver cannot check a wall clock between the engine's
    /// individual reads.
    connect_deadline: ConnectDeadline,
}

// `SyncSocket` = its inner `SyncSock` (8 B, align 4) + the shared connect-phase
// deadline handle (`Arc<AtomicU64>`, one pointer, 8 B, align 8): 16 B, align 8.
// The +8 B (over a bare `SyncSock`) buys the connect-liveness aggregate bound — a
// hostile frame drip cannot pin the connecting thread forever (the MAJOR blind
// zone), the sync analogue of the async driver's whole-connect `tokio::time::timeout`
// — at one relaxed atomic load per steady read (disarmed → the branch is skipped).
// Pinned so the wrapper cannot silently grow further. Unix-only, for the same
// reason as the `SyncSock` pin above (the std socket layout it captures is the
// unix fd's).
#[cfg(unix)]
footprint_pin!(SyncSocket, size = 16, align = 8);

impl SyncSocket {
    /// Wrap an already-connected [`SyncSock`], carrying the connect-phase
    /// [`ConnectDeadline`] the driver arms across the handshake and disarms after.
    #[must_use]
    pub fn new(stream: SyncSock, connect_deadline: ConnectDeadline) -> Self {
        Self { stream, connect_deadline }
    }
}

impl Transport for SyncSocket {
    type Error = io::Error;

    #[inline]
    fn is_would_block(err: &io::Error) -> bool {
        // A blocking socket with `SO_RCVTIMEO`/`SO_SNDTIMEO` set surfaces a read
        // deadline as `WouldBlock` (BSD `EAGAIN`/`EWOULDBLOCK`) or `TimedOut`
        // (`ETIMEDOUT`); both mean "no data within the deadline", not a broken
        // connection. Every other `io::ErrorKind` is a genuine failure.
        matches!(
            err.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
        )
    }

    #[inline]
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl core::future::Future<Output = Result<usize, io::Error>> + Send + 'a {
        // Connect-phase aggregate-deadline enforcement (disarmed = the historical
        // plain read below). The blocking driver cannot check a wall clock between
        // the engine's handshake reads (they all run inside one `poll_once`), so
        // the socket enforces the budget here: re-arm `SO_RCVTIMEO` to the
        // remaining budget so a per-read block cannot overshoot the deadline (the
        // drip case), and fail NOW once the budget is spent so an endless-frame
        // flood cannot loop forever (the busy-flood case). Both surface as the
        // classified `TimedOut`, lifted to `DriverError::Timeout`.
        match self.connect_deadline.phase() {
            ConnectReadPhase::Disarmed => {}
            ConnectReadPhase::Expired => {
                return core::future::ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    CONNECT_DEADLINE_ELAPSED,
                )));
            }
            ConnectReadPhase::Remaining(budget) => {
                if let Err(e) = self.stream.set_read_timeout(Some(budget)) {
                    return core::future::ready(Err(e));
                }
            }
        }
        // The blocking read happens here, eagerly; the future merely carries its
        // already-resolved result, so it is `Ready` on the first poll.
        core::future::ready(Read::read(&mut self.stream, buf))
    }

    #[inline]
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl core::future::Future<Output = Result<usize, io::Error>> + Send + 'a {
        // One write attempt, mirroring a single `poll_write` — the engine owns
        // the drain loop, so this never internally retries.
        core::future::ready(Write::write(&mut self.stream, buf))
    }

    #[inline]
    fn flush<'a>(
        &'a mut self,
    ) -> impl core::future::Future<Output = Result<(), io::Error>> + Send + 'a {
        // A plaintext stream socket holds no userspace buffer, so `flush` has
        // nothing to drain — `Write::flush` is `Ok(())`.
        core::future::ready(Write::flush(&mut self.stream))
    }

    #[inline]
    fn shutdown<'a>(
        &'a mut self,
    ) -> impl core::future::Future<Output = Result<(), io::Error>> + Send + 'a {
        core::future::ready(self.stream.shutdown_write())
    }
}

#[cfg(test)]
mod connect_deadline_tests {
    //! Offline unit coverage for the connect-phase aggregate deadline the socket's
    //! own `read` enforces (see the `--test connect_handshake_deadline` regression
    //! witness for the end-to-end drip proof over a real loopback server).

    use super::{ConnectDeadline, ConnectReadPhase, MIN_CONNECT_READ_BUDGET};
    use std::time::Duration;

    #[test]
    fn armed_far_future_leaves_almost_the_whole_budget_and_re_arms() {
        let d = ConnectDeadline::armed(Duration::from_secs(3600));
        let ConnectReadPhase::Remaining(budget) = d.phase() else {
            panic!("a generous-budget deadline must be Remaining, not Expired/Disarmed");
        };
        assert!(budget >= MIN_CONNECT_READ_BUDGET, "never below the positive floor");
        assert!(budget <= Duration::from_secs(3600), "never above the armed budget");
        assert!(budget > Duration::from_secs(3599), "almost the whole budget remains");
    }

    #[test]
    fn a_spent_budget_is_expired_never_re_armed_to_zero() {
        // `now + 0` is already in the past by the time `phase()` reads the clock,
        // so the read must short-circuit rather than re-arm `SO_RCVTIMEO` to zero
        // (which `std` rejects).
        let d = ConnectDeadline::armed(Duration::ZERO);
        assert!(matches!(d.phase(), ConnectReadPhase::Expired));
    }

    #[test]
    fn disarm_takes_the_plain_read_fast_path() {
        let d = ConnectDeadline::armed(Duration::from_secs(3600));
        d.disarm();
        assert!(matches!(d.phase(), ConnectReadPhase::Disarmed));
    }

    #[test]
    fn the_driver_and_socket_share_one_handle_so_disarm_is_observed() {
        // The driver disarms via ITS clone the instant the handshake completes;
        // the engine-owned socket's clone must observe it (else steady reads would
        // keep enforcing a stale, shrinking budget — the catastrophic bug).
        let driver = ConnectDeadline::armed(Duration::from_secs(3600));
        let socket = driver.clone();
        assert!(matches!(socket.phase(), ConnectReadPhase::Remaining(_)));
        driver.disarm();
        assert!(matches!(socket.phase(), ConnectReadPhase::Disarmed));
    }
}
