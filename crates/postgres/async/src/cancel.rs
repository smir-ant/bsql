//! Out-of-band query cancellation for the async driver: [`CancelToken`].

use core::future::Future;
use core::pin::Pin;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use bsql_postgres_core::cancel::{CancelKey, Redial};
use bsql_postgres_core::driver::{lift_tls_error, RecoveryCancel};
use bsql_postgres_core::DriverError;
use bsql_postgres_proto::engine::Transport;

use crate::connection::Connection;
use crate::transport::ReadDeadline;

/// A detached capability to REQUEST cancellation of the query in flight on the
/// [`Connection`] it was minted from.
///
/// Minted by [`Connection::cancel_token`], it composes the connection's
/// unforgeable [`CancelKey`] (the `(backend_pid, secret_key)` authenticator) with
/// a credential-free [`Redial`] (how to re-open a throwaway socket to the same
/// server). It is `Send + Sync + 'static` and borrows NOTHING from the
/// connection, so the canonical use is:
///
/// ```no_run
/// # async fn demo(conn: &mut bsql_postgres_async::Connection) -> Result<(), bsql_postgres_async::DriverError> {
/// let token = conn.cancel_token();            // obtained BEFORE the long query
/// let handle = tokio::spawn(async move {
///     tokio::time::sleep(std::time::Duration::from_secs(1)).await;
///     let _ = token.cancel().await;            // from another task, mid-query
/// });
/// let _ = conn.query_raw("SELECT pg_sleep(5)").await;  // returns ~early, canceled
/// handle.await.ok();
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
/// and be a harmless no-op. The token itself is tier-1 (unforgeable, typed,
/// `Send + Sync`); the network EFFECT is best-effort.
///
/// Dropping the query future on the ORIGINAL connection only frees the CLIENT-side
/// wait — the backend keeps running (holding locks) until it finishes or a
/// `CancelToken` reaches the server. That is exactly the hole this closes.
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
    /// cancel key (useful for logging which backend a cancel was aimed at).
    #[must_use]
    pub fn backend_pid(&self) -> i32 {
        self.key.backend_pid()
    }

    /// REQUEST cancellation of the in-flight query by sending a `CancelRequest`
    /// on a fresh throwaway connection to the same server.
    ///
    /// Best-effort by spec (§55.4) — see [`CancelToken`]. Honors the original
    /// connection's TLS decision: over a TLS-required endpoint the cancel socket
    /// re-runs the `SSLRequest` probe and completes the handshake before writing
    /// the packet. The whole dial is bounded by the original connect-timeout
    /// budget, so a black-hole cancel socket fails fast rather than hanging.
    ///
    /// # Errors
    ///
    /// A classified [`DriverError`] if the throwaway socket cannot be opened
    /// (connect / TLS / config) or the packet cannot be written — never a panic.
    /// A successful `Ok(())` means the packet was DELIVERED to the server, not
    /// that the query stopped (that is the server's best-effort decision).
    pub async fn cancel(&self) -> Result<(), DriverError> {
        let budget = Duration::from_secs(self.redial.connect_timeout_secs());
        match tokio::time::timeout(budget, self.cancel_inner()).await {
            Ok(result) => result,
            Err(_elapsed) => Err(DriverError::Timeout),
        }
    }

    /// The unbounded cancel dial, run UNDER the timeout budget by
    /// [`cancel`](Self::cancel).
    async fn cancel_inner(&self) -> Result<(), DriverError> {
        // Rebuild a minimal (credential-free) config and drive the driver's OWN
        // wire-builder, so the cancel socket runs the exact SSLRequest probe +
        // TLS handshake the connection did — one authority, no drift.
        let config = self.redial.rebuild_config();
        // The cancel never reads a reply and runs no steady-state queries, so a
        // fresh disarmed deadline with no steady window suffices (it never observes
        // a runtime `SET`, so its `connect_timeout` margin is immaterial).
        let deadline = Arc::new(ReadDeadline::new(None, config.connect_timeout_secs));
        // A detached, throwaway cancel dial carries no diagnostics sink — an SSL
        // downgrade here (should the cancel endpoint's TLS posture differ) keeps
        // the historical stderr warning, never a wired event.
        let diagnostics = bsql_postgres_core::Diagnostics::default();
        let mut wire = Connection::connect_wire(&config, &deadline, &diagnostics).await?;
        send_cancel_packet(&mut wire, &self.key.request_bytes()).await
    }
}

/// The async driver's DROPPED-FUTURE recovery cancel hook: dials a throwaway
/// socket and sends the `CancelRequest` so a query abandoned by a dropped verb
/// future stops FAST, letting the next use's recovery drain complete quickly
/// instead of waiting for the zombie's natural end.
///
/// Installed on every [`Core`](bsql_postgres_core::driver::Core) the async driver
/// mints (via [`Core::set_recovery_cancel`](bsql_postgres_core::driver::Core::set_recovery_cancel)).
/// The blocking driver installs NONE — its verbs cannot be dropped mid-command, so
/// it never reaches a drain-recovery. Holds only a credential-free [`Redial`]; the
/// unforgeable cancel packet is built by the recovering connection and passed in.
#[derive(Debug)]
pub(crate) struct RecoveryCancelDial {
    redial: Redial,
}

impl RecoveryCancelDial {
    #[must_use]
    pub(crate) fn new(redial: Redial) -> Self {
        Self { redial }
    }
}

impl RecoveryCancel for RecoveryCancelDial {
    fn cancel<'a>(&'a self, packet: [u8; 16]) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let budget = Duration::from_secs(self.redial.connect_timeout_secs());
            // Best-effort: swallow a dial/timeout/write failure — a cancel that
            // cannot be delivered is a documented no-op (§55.4), NEVER a recovery
            // failure (the drain still reaches idle once the server's own
            // `statement_timeout` fires or the query finishes). `drop` — not the
            // banned `let _ =` — discards the bounded result.
            drop(tokio::time::timeout(budget, recovery_dial_and_send(&self.redial, packet)).await);
        })
    }
}

/// Rebuild a credential-free config, dial a throwaway wire (honoring the original
/// TLS decision), and send `packet` — the recovery peer of
/// [`CancelToken::cancel_inner`], factored so both share ONE dial + send authority.
async fn recovery_dial_and_send(redial: &Redial, packet: [u8; 16]) -> Result<(), DriverError> {
    let config = redial.rebuild_config();
    // A throwaway dial: a fresh disarmed deadline with no steady window (it never
    // reads a reply nor observes a runtime `SET`), exactly as `CancelToken`.
    let deadline = Arc::new(ReadDeadline::new(None, config.connect_timeout_secs));
    let diagnostics = bsql_postgres_core::Diagnostics::default();
    let mut wire = Connection::connect_wire(&config, &deadline, &diagnostics).await?;
    send_cancel_packet(&mut wire, &packet).await
}

/// Write the 16-byte `CancelRequest` through the (plaintext or TLS) wire, then
/// flush and shut the write side down for an orderly close (`close_notify` on
/// TLS). The engine's send-cursor discipline — loop the single-attempt
/// [`Transport::write`] until the whole packet is committed — applies here too.
async fn send_cancel_packet(
    wire: &mut bsql_postgres_core::tls::Wire<crate::transport::TokioSocket>,
    packet: &[u8; 16],
) -> Result<(), DriverError> {
    let mut sent: usize = 0;
    while sent < packet.len() {
        let remaining = packet
            .get(sent..)
            .ok_or(DriverError::Config("cancel packet cursor out of bounds"))?;
        let n = wire.write(remaining).await.map_err(lift_tls_error)?;
        if n == 0 {
            return Err(DriverError::Io(io::Error::from(io::ErrorKind::WriteZero)));
        }
        sent = sent
            .checked_add(n)
            .ok_or(DriverError::Config("cancel send cursor overflow"))?;
    }
    wire.flush().await.map_err(lift_tls_error)?;
    wire.shutdown().await.map_err(lift_tls_error)?;
    Ok(())
}
