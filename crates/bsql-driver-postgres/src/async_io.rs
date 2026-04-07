//! Async I/O stream for TCP connections via tokio.
//!
//! `AsyncStream` wraps `tokio::net::TcpStream` (plain TCP) or
//! `tokio_rustls::client::TlsStream<TcpStream>` (TLS-encrypted TCP).
//! All I/O is non-blocking and requires a tokio runtime.
//!
//! This is the async counterpart to `sync_io::Stream`. Unix domain sockets
//! are not supported here — they use the sync `Connection` path exclusively.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::DriverError;

/// Async transport — plain TCP or TLS-encrypted TCP.
pub(crate) enum AsyncStream {
    /// Plain TCP connection.
    Tcp(TcpStream),

    /// TLS-encrypted TCP connection via tokio-rustls.
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncStream {
    /// Read up to `buf.len()` bytes. Returns the number of bytes read.
    /// Returns 0 on EOF.
    #[inline]
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        match self {
            Self::Tcp(s) => s.read(buf).await,
            #[cfg(feature = "tls")]
            Self::Tls(s) => s.read(buf).await,
        }
    }

    /// Write all bytes from `buf` to the stream.
    #[inline]
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        match self {
            Self::Tcp(s) => s.write_all(buf).await,
            #[cfg(feature = "tls")]
            Self::Tls(s) => s.write_all(buf).await,
        }
    }

    /// Flush the stream, ensuring all buffered data is sent.
    #[allow(dead_code)] // used by future phases (COPY protocol, streaming)
    #[inline]
    pub async fn flush(&mut self) -> Result<(), std::io::Error> {
        match self {
            Self::Tcp(s) => s.flush().await,
            #[cfg(feature = "tls")]
            Self::Tls(s) => s.flush().await,
        }
    }

    /// Set TCP_NODELAY on the underlying socket.
    #[allow(dead_code)] // used by async_conn and future phases
    pub fn set_nodelay(&self, nodelay: bool) -> Result<(), DriverError> {
        match self {
            Self::Tcp(s) => s.set_nodelay(nodelay).map_err(DriverError::Io),
            #[cfg(feature = "tls")]
            Self::Tls(s) => {
                let (tcp, _) = s.get_ref();
                tcp.set_nodelay(nodelay).map_err(DriverError::Io)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn async_stream_enum_variants_exist() {
        // Compile-time check that the enum and its variants are well-formed.
        fn _assert_send<T: Send>() {}
        _assert_send::<AsyncStream>();
    }
}
