//! Out-of-band query cancellation for the blocking driver: [`CancelToken`].

use std::io;
use std::time::Duration;

use bsql_postgres_core::cancel::{CancelKey, Redial};
use bsql_postgres_core::driver::{lift_tls_error, WireError};
use bsql_postgres_core::tls::Wire;
use bsql_postgres_core::DriverError;
use bsql_postgres_proto::engine::{self, SpuriousPending, Transport};

use crate::connection::Connection;
use crate::transport::{ConnectDeadline, SyncSocket};

/// A detached capability to REQUEST cancellation of the query in flight on the
/// blocking [`Connection`] it was minted from.
///
/// The blocking twin of the async driver's cancel token: minted by
/// [`Connection::cancel_token`], `Send + Sync + 'static`, borrowing NOTHING. The
/// canonical use is to obtain it before a long blocking query and hand it to
/// ANOTHER thread that calls [`cancel`](Self::cancel) mid-query:
///
/// ```no_run
/// # fn demo(conn: &mut bsql_postgres_sync::Connection) -> Result<(), bsql_postgres_sync::DriverError> {
/// let token = conn.cancel_token();            // obtained BEFORE the long query
/// let canceller = std::thread::spawn(move || {
///     std::thread::sleep(std::time::Duration::from_millis(300));
///     let _ = token.cancel();                  // from another thread, mid-query
/// });
/// let _ = conn.query_sql("SELECT pg_sleep(5)"); // returns ~early, canceled
/// canceller.join().ok();
/// # Ok(())
/// # }
/// ```
///
/// # Best-effort, not a guarantee
///
/// A PostgreSQL cancel MUST travel on a SECOND connection, because the connection
/// running the query is blocked server-side; [`cancel`](Self::cancel) opens a
/// throwaway socket to the same endpoint (re-running the `SSLRequest` probe and
/// honoring the original `SslMode` / custom CA roots — a cancel to a TLS-required
/// server negotiates TLS), writes the 16-byte `CancelRequest`, and closes it.
/// Per PG §55.4 this REQUESTS cancellation: the server may honor it (the query
/// returns SQLSTATE `57014` `query_canceled`), or the cancel may arrive too late
/// and be a harmless no-op. The token is tier-1 (unforgeable, typed,
/// `Send + Sync`); the network EFFECT is best-effort.
#[derive(Debug)]
pub struct CancelToken {
    key: CancelKey,
    redial: Redial,
}

impl CancelToken {
    /// Compose a token from a cancel key and a redial snapshot. The driver-facing
    /// seam (`#[doc(hidden)]`), called by [`Connection::cancel_token`].
    #[doc(hidden)]
    #[must_use]
    pub fn new(key: CancelKey, redial: Redial) -> Self {
        Self { key, redial }
    }

    /// The backend process id the cancel targets — the non-secret half of the
    /// cancel key.
    #[must_use]
    pub fn backend_pid(&self) -> i32 {
        self.key.backend_pid()
    }

    /// REQUEST cancellation of the in-flight query by sending a `CancelRequest`
    /// on a fresh throwaway connection to the same server (blocking).
    ///
    /// Best-effort by spec (§55.4) — see [`CancelToken`]. Honors the original
    /// connection's TLS decision via the driver's shared dial + wire builder. The
    /// WHOLE dial is bounded by the original connect-timeout budget: the TCP
    /// connect via `TcpStream::connect_timeout` (in the shared `dial_socket`), and
    /// the `SSLRequest` probe + packet write via the socket read/write timeouts
    /// armed here — so a black-holed cancel socket fails fast rather than stalling
    /// the calling thread.
    ///
    /// # Errors
    ///
    /// A classified [`DriverError`] if the throwaway socket cannot be opened
    /// (connect / TLS / config) or the packet cannot be written — never a panic.
    /// A successful `Ok(())` means the packet was DELIVERED to the server, not
    /// that the query stopped (that is the server's best-effort decision).
    pub fn cancel(&self) -> Result<(), DriverError> {
        let config = self.redial.rebuild_config();
        // Drive the driver's OWN dial + wire builder so the cancel socket runs the
        // exact SSLRequest probe + TLS handshake the connection did — one
        // authority, no drift.
        let (sock, ssl_mode) = Connection::dial_socket(&config)?;
        let budget = Duration::from_secs(self.redial.connect_timeout_secs());
        sock.set_read_timeout(Some(budget))?;
        sock.set_write_timeout(Some(budget))?;
        // A detached, throwaway cancel dial carries no diagnostics sink — an SSL
        // downgrade here keeps the historical stderr warning, never a wired event.
        let diagnostics = bsql_postgres_core::Diagnostics::default();
        // Bound the cancel wire's SSL-probe + TLS handshake reads by the SAME
        // aggregate budget (a hostile server cannot drip a TLS handshake to stall
        // the cancelling thread). No disarm: the wire is thrown away after the
        // 16-byte CancelRequest write, and `write` never consults the deadline.
        let connect_deadline = ConnectDeadline::armed(budget);
        let mut wire =
            Connection::build_wire(sock, &config, ssl_mode, &diagnostics, &connect_deadline)?;
        send_cancel_packet(&mut wire, &self.key.request_bytes())
    }
}

/// Flatten a single-poll drive of a wire I/O op: over the blocking socket it
/// resolves on the first poll, so a `Pending` is the classified
/// [`DriverError::SpuriousPending`], and a wire error is lifted like every other
/// blocking-driver I/O failure.
fn poll_sync<T>(polled: Result<Result<T, WireError>, SpuriousPending>) -> Result<T, DriverError> {
    match polled {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(lift_tls_error(e)),
        Err(SpuriousPending) => Err(DriverError::SpuriousPending),
    }
}

/// Write the 16-byte `CancelRequest` through the (plaintext or TLS) wire, then
/// flush and shut the write side down for an orderly close. Each single-attempt
/// [`Transport::write`] is driven with one `poll_once`; the send-cursor loop
/// mirrors the engine's own discipline.
fn send_cancel_packet(
    wire: &mut Wire<SyncSocket>,
    packet: &[u8; 16],
) -> Result<(), DriverError> {
    let mut sent: usize = 0;
    while sent < packet.len() {
        let remaining = packet
            .get(sent..)
            .ok_or(DriverError::Config("cancel packet cursor out of bounds"))?;
        let n = poll_sync(engine::poll_once(wire.write(remaining)))?;
        if n == 0 {
            return Err(DriverError::Io(io::Error::from(io::ErrorKind::WriteZero)));
        }
        sent = sent
            .checked_add(n)
            .ok_or(DriverError::Config("cancel send cursor overflow"))?;
    }
    poll_sync(engine::poll_once(wire.flush()))?;
    poll_sync(engine::poll_once(wire.shutdown()))?;
    Ok(())
}
