//! The blocking I/O transport layer behind the engine's [`Transport`] seam.
//!
//! [`SyncSocket`] wraps a `std::net::TcpStream`: every `Transport` op is a
//! single blocking std op, so each future resolves on its FIRST poll (never
//! `Pending`) and the engine's `poll_once` single-poll executor drives the whole
//! sans-IO engine over it with no async runtime.
//!
//! [`Wire`] keeps the engine MONOMORPHIC over one transport type whether the
//! connection is plaintext or TLS: it is an `enum { Plain, Tls }` that itself
//! implements [`Transport`], forwarding each op to the active arm — the role the
//! old `Stream` enum played, now behind the seam. The error union is
//! [`TlsError<S::Error>`] for both arms: the TLS arm's error already is that
//! type, and a plaintext socket error rides [`TlsError::Socket`]. Reusing the
//! TLS error union (rather than minting a third `enum WireError`) avoids the
//! double-wrapping a bespoke union would create — `TlsError` already nests the
//! inner socket error in its `Socket` variant — and inherits its
//! `Send`-when-inner-is-`Send` property, which keeps the verb futures `Send`.

use std::io::{self, Read, Write};
use std::net::TcpStream;

use bsql_postgres_core::tls::{TlsError, TlsTransport};
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

/// A plaintext-or-TLS transport that itself implements [`Transport`], so the
/// engine stays monomorphic over a single `Wire<SyncSocket>` type.
pub enum Wire<S: Transport> {
    /// Plaintext socket.
    Plain(S),
    /// TLS over the socket (rustls::unbuffered, driven by `poll_once`).
    ///
    /// Boxed: the TLS state (rustls connection + record buffers) dwarfs a bare
    /// socket, so boxing the rare TLS arm keeps `Wire` — and the `Engine` that
    /// embeds it — small for the plaintext common case. The deref is per
    /// syscall, never per row.
    Tls(Box<TlsTransport<S>>),
}

impl<S: Transport> Transport for Wire<S> {
    /// The arm-uniform error union: a plaintext socket error rides
    /// [`TlsError::Socket`]; the TLS arm's error already is this type.
    type Error = TlsError<S::Error>;

    // The forwarding arms are `async fn` (which satisfies the trait's RPITIT
    // `+ Send` bound — the compiler checks the future is `Send`); the explicit
    // `<'a>` matches the trait's single-lifetime signature (self and buf share
    // it). The active arm's inner future is awaited in place; the plaintext
    // arm's error is lifted onto the shared union.
    #[inline]
    async fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> Result<usize, Self::Error> {
        match self {
            Wire::Plain(s) => s.read(buf).await.map_err(TlsError::Socket),
            Wire::Tls(t) => t.read(buf).await,
        }
    }

    #[inline]
    async fn write<'a>(&'a mut self, buf: &'a [u8]) -> Result<usize, Self::Error> {
        match self {
            Wire::Plain(s) => s.write(buf).await.map_err(TlsError::Socket),
            Wire::Tls(t) => t.write(buf).await,
        }
    }

    #[inline]
    async fn flush(&mut self) -> Result<(), Self::Error> {
        match self {
            Wire::Plain(s) => s.flush().await.map_err(TlsError::Socket),
            Wire::Tls(t) => t.flush().await,
        }
    }

    #[inline]
    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        match self {
            Wire::Plain(s) => s.shutdown().await.map_err(TlsError::Socket),
            Wire::Tls(t) => t.shutdown().await,
        }
    }
}
