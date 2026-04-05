//! Unified synchronous I/O stream for PostgreSQL connections.
//!
//! `Stream` abstracts over TCP, Unix domain socket, and TLS transports using
//! blocking `std::io::Read` / `Write`. This replaces the previous tokio-based
//! async `Stream` enum that required an async runtime.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use crate::DriverError;

/// The underlying transport — plain TCP, TLS-wrapped TCP, or Unix domain socket.
///
/// All variants implement blocking `Read` + `Write`. The enum dispatches I/O
/// to the appropriate stream type with zero overhead (single match per call).
pub(crate) enum Stream {
    /// Plain TCP connection.
    Tcp(TcpStream),

    /// Unix domain socket connection (macOS, Linux).
    #[cfg(unix)]
    Unix(UnixStream),

    /// TLS-encrypted TCP connection via rustls.
    #[cfg(feature = "tls")]
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Read for Stream {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.read(buf),
            #[cfg(unix)]
            Stream::Unix(s) => s.read(buf),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Tcp(s) => s.write(buf),
            #[cfg(unix)]
            Stream::Unix(s) => s.write(buf),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.write(buf),
        }
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.flush(),
            #[cfg(unix)]
            Stream::Unix(s) => s.flush(),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.flush(),
        }
    }
}

impl Stream {
    /// Set the read timeout on the underlying socket.
    ///
    /// Used by the Listener to poll for notifications with a timeout,
    /// and for connection health checks. `None` means block indefinitely.
    pub(crate) fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_read_timeout(dur),
            #[cfg(unix)]
            Stream::Unix(s) => s.set_read_timeout(dur),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.sock.set_read_timeout(dur),
        }
    }

    /// Set the write timeout on the underlying socket.
    #[allow(dead_code)] // used by future phases
    pub(crate) fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            Stream::Tcp(s) => s.set_write_timeout(dur),
            #[cfg(unix)]
            Stream::Unix(s) => s.set_write_timeout(dur),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.sock.set_write_timeout(dur),
        }
    }

    /// Set TCP_NODELAY on the underlying socket (TCP/TLS only).
    ///
    /// No-op for Unix domain sockets (Nagle doesn't apply to UDS).
    #[allow(dead_code)] // used when tls feature is enabled
    pub(crate) fn set_nodelay(&self) -> Result<(), DriverError> {
        match self {
            Stream::Tcp(s) => s.set_nodelay(true).map_err(DriverError::Io),
            #[cfg(unix)]
            Stream::Unix(_) => Ok(()),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.sock.set_nodelay(true).map_err(DriverError::Io),
        }
    }

    /// Set TCP keepalive to detect dead connections (TCP/TLS only).
    ///
    /// Sends a probe after 60s of idle, retries every 15s. Without keepalive,
    /// a half-open connection (server crash, firewall timeout) hangs forever.
    /// No-op for Unix domain sockets.
    pub(crate) fn set_keepalive(&self) -> Result<(), DriverError> {
        match self {
            Stream::Tcp(s) => set_tcp_keepalive(s),
            #[cfg(unix)]
            Stream::Unix(_) => Ok(()),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => set_tcp_keepalive(&s.sock),
        }
    }
}

/// Configure TCP keepalive on a raw TCP socket.
fn set_tcp_keepalive(tcp: &TcpStream) -> Result<(), DriverError> {
    let sock = socket2::SockRef::from(tcp);
    let ka = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(60))
        .with_interval(Duration::from_secs(15));
    sock.set_tcp_keepalive(&ka).map_err(DriverError::Io)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_tcp_read_write_traits() {
        // Verify Stream implements Read + Write at compile time.
        fn assert_read_write<T: Read + Write>() {}
        assert_read_write::<Stream>();
    }
}
