//! The transport-agnostic building blocks of driver-level query cancellation.
//!
//! A PostgreSQL cancel is OUT-OF-BAND: it MUST travel on a SECOND connection,
//! because the connection running the query is blocked server-side (PG §55.4).
//! So a cancel needs two things a live connection cannot lend directly to
//! another task without aliasing its in-flight `&mut` future:
//!
//! - the [`CancelKey`] — the `(backend_pid, secret_key)` authenticator the
//!   server minted for this backend at handshake, which authorises cancelling
//!   ITS queries and nothing else; and
//! - the [`Redial`] — enough of the original [`ConnectConfig`] to open a fresh
//!   throwaway socket to the SAME endpoint with the SAME TLS decision.
//!
//! Both are DETACHED owned values (`Send + Sync + 'static`, borrowing nothing),
//! so a driver's `CancelToken` composes them into a capability object that can
//! be held or moved to another task while the query is in flight. The per-driver
//! `CancelToken` (async / blocking) owns the I/O half — dialing the socket and
//! writing the [`cancel_request_bytes`](bsql_postgres_proto::cancel_request_bytes)
//! packet — because that is the one piece that genuinely differs between the two
//! transports.
//!
//! # Best-effort, by construction and by spec
//!
//! The [`CancelKey`] is unforgeable (the secret is minted only at connect and
//! never leaves a [`Sensitive`] except to be BE-encoded into the wire packet)
//! and typed, so it is a tier-1 authenticator. The NETWORK EFFECT of sending it
//! is BEST-EFFORT: PG §55.4 makes no guarantee the query stops (the cancel may
//! arrive after the query finished, or between statements). A driver documents
//! `cancel()` as *requesting* cancellation, never *guaranteeing* it.

use std::sync::Arc;

use bsql_postgres_proto::{cancel_request_bytes, Sensitive};

use crate::config::{ConnectConfig, SslMode};

/// The unforgeable cancel-key authenticator for one backend: its `backend_pid`
/// plus the `BackendKeyData` secret, both captured at handshake completion.
///
/// Produced only by [`Core::cancel_key`](crate::Core::cancel_key), which reads
/// the pid and clones the secret out of the engine — the secret is minted by the
/// server at connect and never surfaced any other way, so a `CancelKey` cannot
/// be forged for a backend the process never opened. It authorises cancelling
/// exactly that backend's queries; a stale key (its query already finished) is a
/// server-side no-op, and a duplicate cancel is two harmless packets.
///
/// `Send + Sync + 'static` and borrows nothing, so it can be moved into another
/// task while the owning connection's query future is in flight (no `&mut`
/// aliasing with that future — the whole point of the out-of-band design).
/// The secret stays in a [`Sensitive`] end-to-end (redacted in `Debug`, zeroed
/// on drop); it is materialised only transiently inside
/// [`request_bytes`](Self::request_bytes), never stored or logged in the clear.
pub struct CancelKey {
    backend_pid: i32,
    secret: Sensitive<i32>,
}

impl core::fmt::Debug for CancelKey {
    /// Prints the public pid and redacts the secret (via `Sensitive`'s own
    /// redacting `Debug`), so a `CancelKey` in a log line never leaks the cancel
    /// authenticator.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CancelKey")
            .field("backend_pid", &self.backend_pid)
            .field("secret", &self.secret)
            .finish()
    }
}

// Footprint pin: two `i32`s (the pid + the `#[repr(transparent)]`
// `Sensitive<i32>` secret), no discriminant — 8 bytes, 4-byte aligned. A widened
// key (a boxed secret, an added field) trips this.
crate::footprint_pin!(CancelKey, size = 8, align = 4);

impl CancelKey {
    /// Assemble a key from the pid and the secret cloned out of the engine. The
    /// driver-facing construction seam (`#[doc(hidden)]`), called by
    /// [`Core::cancel_key`](crate::Core::cancel_key).
    #[doc(hidden)]
    #[must_use]
    pub fn new(backend_pid: i32, secret: Sensitive<i32>) -> Self {
        Self { backend_pid, secret }
    }

    /// The backend process id — the non-secret half of the key, safe to surface.
    #[inline]
    #[must_use]
    pub fn backend_pid(&self) -> i32 {
        self.backend_pid
    }

    /// Materialise the 16-byte `CancelRequest` wire packet for this backend.
    ///
    /// The secret is read out of its [`Sensitive`] ONLY here and ONLY to be
    /// BE-encoded into the packet by the tier-1 `const fn`
    /// [`cancel_request_bytes`](bsql_postgres_proto::cancel_request_bytes); it is
    /// never copied elsewhere. A driver writes the returned bytes to a throwaway
    /// socket and closes it.
    #[inline]
    #[must_use]
    pub fn request_bytes(&self) -> [u8; 16] {
        self.secret
            .with_inner(|secret| cancel_request_bytes(self.backend_pid, *secret))
    }
}

/// The subset of a [`ConnectConfig`] needed to re-open a THROWAWAY socket to the
/// same server with the same TLS decision — the "how to redial" half of a cancel
/// token.
///
/// It deliberately carries NO credentials: a cancel authenticates with the
/// [`CancelKey`], never with the user's password, so a leaked `Redial` grants no
/// login capability — only the endpoint + TLS posture, which are not secret.
/// `Clone` (an `Arc`-refcount bump for the host + PEM, never a deep copy) and
/// `Send + Sync + 'static`.
///
/// The raw `ssl_mode` (an `Option<SslMode>`, `None` = defaulted) is preserved,
/// NOT the resolved mode, so [`rebuild_config`](Self::rebuild_config) reproduces
/// the ORIGINAL connection's SSL decision exactly — the same threat-scoped
/// default resolution AND the same explicit/defaulted error classification —
/// with zero drift from the live connect path.
#[derive(Clone, Debug)]
pub struct Redial {
    host: Arc<str>,
    port: u16,
    ssl_mode: Option<SslMode>,
    ca_roots: Option<Arc<[u8]>>,
    connect_timeout_secs: u64,
}

// Footprint pin: an `Arc<str>` (16) + `Option<Arc<[u8]>>` (16, niche-packed) +
// `u64` timeout (8) + `u16` port + the niche-packed `Option<SslMode>` byte
// (padded to the 8-byte alignment) = 48 bytes. A new redial field lands here as
// a reviewed drift.
crate::footprint_pin!(Redial, size = 48, align = 8);

impl Redial {
    /// Snapshot the redial-relevant fields of `config`. The driver-facing seam
    /// (`#[doc(hidden)]`), captured once at connect and stored on the
    /// `Connection` so `cancel_token()` can mint tokens on demand.
    #[doc(hidden)]
    #[must_use]
    pub fn from_config(config: &ConnectConfig, encrypted: bool) -> Self {
        let mode = if encrypted {
            Some(SslMode::Require)
        } else {
            config.ssl_mode_raw()
        };
        Self {
            host: Arc::from(config.host.as_str()),
            port: config.port,
            ssl_mode: mode,
            ca_roots: config.ca_roots_arc(),
            connect_timeout_secs: config.connect_timeout_secs,
        }
    }

    /// Rebuild a minimal [`ConnectConfig`] faithful to the original for the
    /// cancel dial — same host, port, SSL mode (preserving its explicit/defaulted
    /// state), custom CA roots, and connect-timeout budget; NO user or password
    /// (a cancel never authenticates). The driver drives its normal wire-builder
    /// over this config, so the cancel socket runs the SAME `SSLRequest` probe +
    /// TLS handshake the connection did — one authority, no drift.
    #[doc(hidden)]
    #[must_use]
    pub fn rebuild_config(&self) -> ConnectConfig {
        let mut config = ConnectConfig::new(&*self.host, "")
            .port(self.port)
            .connect_timeout(self.connect_timeout_secs);
        // Preserve explicit-ness: set the mode ONLY when the original was
        // explicit, so a defaulted mode re-resolves to the same threat-scoped
        // default (deterministic for the same endpoint) rather than being frozen
        // into an explicit one (which would change the SSL-refused error class).
        if let Some(mode) = self.ssl_mode {
            config = config.ssl_mode(mode);
        }
        if let Some(pem) = &self.ca_roots {
            config = config.with_ca_roots(pem);
        }
        config
    }

    /// The connect-timeout budget (seconds) to bound the whole cancel dial, so a
    /// black-hole cancel socket fails fast instead of hanging the caller.
    #[inline]
    #[must_use]
    pub fn connect_timeout_secs(&self) -> u64 {
        self.connect_timeout_secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_bytes_matches_the_wire_builder() {
        // The key's packet must be byte-identical to the tier-1 wire builder for
        // the same pid + secret — the key is only a Sensitive carrier around it.
        let pid: i32 = 0x1234_5678;
        let secret: i32 = -559_038_737; // 0xDEAD_BEEF as an i32 bit pattern
        let key = CancelKey::new(pid, Sensitive::new(secret));
        assert_eq!(key.request_bytes(), cancel_request_bytes(pid, secret));
        assert_eq!(key.backend_pid(), pid);
    }

    #[test]
    fn debug_redacts_the_secret() {
        let key = CancelKey::new(42, Sensitive::new(0x7fff_ffff));
        let shown = format!("{key:?}");
        assert!(shown.contains("backend_pid: 42"), "pid is public: {shown}");
        assert!(shown.contains("REDACTED"), "secret must be redacted: {shown}");
        assert!(
            !shown.contains("2147483647") && !shown.contains("7fffffff"),
            "the secret value must not appear: {shown}",
        );
    }

    #[test]
    fn rebuild_preserves_a_defaulted_ssl_mode_as_defaulted() {
        // A defaulted (unset) SSL mode must stay defaulted through a round-trip,
        // so the cancel dial re-resolves the SAME threat-scoped default and keeps
        // the same SSL-refused error classification.
        let config = ConnectConfig::new("db.example.com", "alice").port(6432);
        assert!(!config.ssl_mode_is_explicit());
        let redial = Redial::from_config(&config, false);
        let rebuilt = redial.rebuild_config();
        assert_eq!(rebuilt.host, "db.example.com");
        assert_eq!(rebuilt.port, 6432);
        assert!(
            !rebuilt.ssl_mode_is_explicit(),
            "a defaulted mode must round-trip as defaulted, not frozen explicit",
        );
        // No credentials ride the redial.
        assert_eq!(rebuilt.user, "");
        assert!(rebuilt.password_str().is_none());
    }

    #[test]
    fn rebuild_preserves_an_explicit_ssl_mode() {
        let config = ConnectConfig::new("db.example.com", "alice").ssl_mode(SslMode::Require);
        let redial = Redial::from_config(&config, false);
        let rebuilt = redial.rebuild_config();
        assert!(rebuilt.ssl_mode_is_explicit());
        assert_eq!(
            rebuilt.resolve_ssl_mode(&crate::resolve_endpoint(&rebuilt.host, rebuilt.port)),
            SslMode::Require,
        );
    }

    #[test]
    fn rebuild_enforces_require_when_parent_was_encrypted() {
        let config = ConnectConfig::new("localhost", "alice").ssl_mode(SslMode::Prefer);
        let redial = Redial::from_config(&config, true);
        let rebuilt = redial.rebuild_config();
        assert!(rebuilt.ssl_mode_is_explicit());
        assert_eq!(rebuilt.ssl_mode_raw(), Some(SslMode::Require));
    }

    fn assert_send_sync_static<T: Send + Sync + 'static>() {}

    #[test]
    fn cancel_pieces_are_detached_capabilities() {
        // The whole point: both halves are Send + Sync + 'static so a driver's
        // CancelToken built from them can move to another task while the owning
        // connection's query future is in flight.
        assert_send_sync_static::<CancelKey>();
        assert_send_sync_static::<Redial>();
    }
}
