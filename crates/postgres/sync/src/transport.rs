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
use std::time::Duration;

// Used only by the two `#[cfg(unix)]` footprint pins below (they capture the unix
// fd layout); on a non-unix target there is no socket duality to pin.
#[cfg(unix)]
use bsql_postgres_core::footprint_pin;
use bsql_postgres_proto::engine::Transport;

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
}

// The wrapper is exactly its inner `SyncSock` — the same 8 B — with no added
// state. Pinned so the wrapper cannot silently grow past the socket it carries.
// Unix-only, for the same reason as the `SyncSock` pin above (the std socket
// layout it captures is the unix fd's).
#[cfg(unix)]
footprint_pin!(SyncSocket, size = 8, align = 4);

impl SyncSocket {
    /// Wrap an already-connected [`SyncSock`] (a TCP or unix stream).
    #[must_use]
    pub fn new(stream: SyncSock) -> Self {
        Self { stream }
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
