//! The blocking I/O socket behind the engine's [`Transport`] seam.
//!
//! [`SyncSocket`] wraps a `std::net::TcpStream`: every `Transport` op is a
//! single blocking std op, so each future resolves on its FIRST poll (never
//! `Pending`) and the engine's `poll_once` single-poll executor drives the whole
//! sans-IO engine over it with no async runtime.
//!
//! The plaintext-or-TLS multiplexer the engine is monomorphic over —
//! [`Wire`](bsql_postgres_core::tls::Wire) — lives in `bsql-postgres-core`,
//! shared with the async driver so the multiplexer exists once. This module
//! supplies only the blocking socket arm it wraps.

use std::io::{self, Read, Write};
use std::net::TcpStream;

use bsql_postgres_proto::engine::Transport;

/// A blocking `std::net::TcpStream` presented through the engine's
/// [`Transport`] seam.
///
/// Every op is a single blocking std call evaluated eagerly, then handed back as
/// an already-resolved [`core::future::Ready`] — so the seam's futures never
/// return `Pending` and `poll_once` completes them in one poll. `write` performs
/// exactly one `write` syscall (the seam's one-attempt contract — looping is the
/// engine's job), `flush` is a no-op (a TCP socket has no userspace buffer), and
/// `shutdown` closes the write half so the peer sees a clean FIN.
pub struct SyncSocket {
    stream: TcpStream,
}

impl SyncSocket {
    /// Wrap an already-connected `TcpStream`.
    #[must_use]
    pub fn new(stream: TcpStream) -> Self {
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
        // A plaintext TCP socket holds no userspace buffer, so `flush` has
        // nothing to drain — `TcpStream`'s `Write::flush` is `Ok(())`.
        core::future::ready(Write::flush(&mut self.stream))
    }

    #[inline]
    fn shutdown<'a>(
        &'a mut self,
    ) -> impl core::future::Future<Output = Result<(), io::Error>> + Send + 'a {
        core::future::ready(self.stream.shutdown(std::net::Shutdown::Write))
    }
}
