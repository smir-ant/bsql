//! The blocking PostgreSQL connection: a thin blocking adapter over the shared
//! [`Core`] driver engine.
//!
//! A [`Connection`] owns a [`Core<SyncSocket>`] — the transport-generic engine
//! that defines every non-I/O verb ONCE (shared verbatim with the async driver) —
//! plus the blocking-socket control handle. Every delegated verb drives the
//! corresponding `Core` verb with a SINGLE [`engine::poll_once`]: over a blocking
//! transport every leaf op resolves on its FIRST poll (never `Pending`), so the
//! whole composite verb future — synchronous prologue, the awaited engine call,
//! and synchronous epilogue — runs to completion in that one poll. [`drive_sync`]
//! then collapses the executor-invariant [`SpuriousPending`] (a `Pending` where a
//! blocking op must not suspend) into a classified [`DriverError::SpuriousPending`].
//!
//! The liveness token, health-bit semantics, and recoverable-error model all live
//! in `Core` (see its docs); this module supplies only the blocking-specific
//! connect (socket read/write timeouts + a `try_clone`d control handle), the
//! notification read-timeout arming, and the `FnOnce` `transaction` /
//! `copy_in_with`. Dropping a [`Connection`] closes the socket fd (an abrupt FIN
//! PostgreSQL treats like a `Terminate`); a graceful protocol-level `Terminate` is
//! sent only by an explicit [`Connection::close`], never on `Drop` — an implicit
//! drop must never risk a blocking write (see the `Connection` `Drop` note below).
//!
//! # Footprint regime
//!
//! The stable public *types* this driver re-exports carry their `size_of` pins in
//! `bsql-postgres-core`. The sync driver has no futures of its own — its verbs are
//! blocking calls whose working set lives on the caller's stack — so there is no
//! unnameable-future footprint surface here.

use core::ops::ControlFlow;
use core::str::FromStr;
// `std::io` and the blocking `Read`/`Write` traits are named only by the
// `tls`-gated SSLRequest probe + TLS-config path in `build_wire` (and
// `lift_probe_io`); with `tls` off no probe is sent and none is used.
#[cfg(feature = "tls")]
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
// Unix targets only — `std::os::unix` is absent elsewhere; a unix-socket host on a
// non-unix target is rejected at connect (see the `Endpoint::Unix` dial arm below).
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::time::Duration;

use bsql_postgres_core::driver::{lift_conn_fail, lift_engine_error, Core, WireError};
// The TLS handshake lifters + the SSLRequest probe + the rustls transport are
// reached only from the `tls`-gated arm of `build_wire`; with `tls` off the probe
// is compiled out and none of these are named. `WireError` (`= TlsError<io>`)
// stays: it is the plaintext-or-TLS wire error either way.
#[cfg(feature = "tls")]
use bsql_postgres_core::driver::{lift_ca_roots_error, lift_tls_error};
use bsql_postgres_core::tls::Wire;
#[cfg(feature = "tls")]
use bsql_postgres_core::tls::{self, TlsTransport};
use bsql_postgres_core::{
    resolve_endpoint, validate_startup_params, BorrowedRow, ConnectConfig, Diagnostics, DriverError,
    Endpoint, MigrationError, MigrationReport, MigrationSource, MigrationStatus, Notification,
    QueryResult, Redial, Row, Rows, SslMode, TypedNotification,
};
// Referenced only by the non-unix `Endpoint::Unix` reject arm in `dial_socket`.
#[cfg(not(unix))]
use bsql_postgres_core::UNIX_SOCKET_UNSUPPORTED;
use bsql_postgres_proto::engine::{self, EngineError, SpuriousPending};
use bsql_postgres_proto::params::ParamsWriter;
use bsql_postgres_proto::{
    Credentials, DatabaseName, Ident, PreparedQuery, RowDecode, TypedCopyIn, TypedQuery,
};
// `saslprep_password` SASLpreps (RFC 4013) the config password and builds the
// zeroize-on-drop `Password`; `Sensitive` wraps it into a
// `Credentials::ScramPassword`; `resolve_channel_binding` computes its channel
// binding from the built wire. All SCRAM-only.
#[cfg(feature = "scram")]
use bsql_postgres_core::{resolve_channel_binding, saslprep_password};
#[cfg(feature = "scram")]
use bsql_postgres_proto::Sensitive;

use crate::transport::{SyncSock, SyncSocket};

/// The prepared-statement handle (defined once in `bsql-postgres-core`, shared by
/// both drivers). Re-exported so `bsql_postgres_sync::PreparedStatement` resolves.
pub use bsql_postgres_core::PreparedStatement;

/// The plaintext-or-TLS transport the engine is monomorphic over.
type SyncWire = Wire<SyncSocket>;
/// Result of a single-poll drive before the [`SpuriousPending`] collapse: the
/// verb's own `Result<T, DriverError>` (Core already lifted the engine error),
/// wrapped in the `poll_once` verdict.
type Driven<T> = Result<Result<T, DriverError>, SpuriousPending>;
/// Result of a single-poll drive of an engine call whose error is still an
/// [`EngineError`] (the connect-phase drives that predate a `Core`).
type Polled<T> = Result<Result<T, EngineError<WireError>>, SpuriousPending>;

/// Collapse a single-poll drive of a `Core` verb: the verb resolved to its own
/// `Result` in one poll, or the transport spuriously suspended (a blocking op that
/// must not `Pending`ed — classified, never spun on or dropped).
#[inline]
fn drive_sync<T>(driven: Driven<T>) -> Result<T, DriverError> {
    match driven {
        Ok(result) => result,
        Err(SpuriousPending) => Err(DriverError::SpuriousPending),
    }
}

/// Collapse a single-poll drive of a migration-runner verb, whose error surface
/// is [`MigrationError`] (not `DriverError`). A spurious `Pending` over the
/// blocking socket is classified, never spun on.
#[inline]
fn drive_migration<T>(
    driven: Result<Result<T, MigrationError>, SpuriousPending>,
) -> Result<T, MigrationError> {
    match driven {
        Ok(result) => result,
        Err(SpuriousPending) => Err(DriverError::SpuriousPending.into()),
    }
}

/// Flatten a single-poll drive of a raw engine call to the driver error surface
/// (the connect / close path, which drives an engine call directly rather than a
/// `Core` verb, so it lifts the [`EngineError`] here).
fn flatten_poll<T>(polled: Polled<T>) -> Result<T, DriverError> {
    match polled {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(lift_engine_error(e)),
        Err(SpuriousPending) => Err(DriverError::SpuriousPending),
    }
}

/// Open a TCP connection to `addr` ("host:port") with the SYN bounded by
/// `timeout`.
///
/// [`std::net::TcpStream::connect`] has NO connect timeout — a black-holed host
/// stalls the calling thread until the OS SYN timeout (~1-2 min). This resolves
/// the address and tries each candidate with
/// [`TcpStream::connect_timeout`](std::net::TcpStream::connect_timeout), so the
/// connect fails within the caller's connect-timeout budget, while preserving
/// `TcpStream::connect`'s try-each-resolved-address behaviour (important for a
/// dual-stack host). DNS resolution itself is the OS's and unbounded — as with
/// the async driver's `tokio::net::TcpStream::connect`, only the SYN is budgeted.
fn connect_tcp_within(addr: &str, timeout: Duration) -> std::io::Result<TcpStream> {
    let mut last_err: Option<std::io::Error> = None;
    for socket_addr in addr.to_socket_addrs()? {
        match TcpStream::connect_timeout(&socket_addr, timeout) {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }
    match last_err {
        Some(e) => Err(e),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "no socket addresses resolved for the host",
        )),
    }
}

/// TCP keepalive idle time — the kernel starts sending keepalive probes after
/// this long with no traffic on an IDLE connection.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(60);
/// TCP keepalive probe interval — the gap between successive keepalive probes.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Enable TCP keepalive on a connected TCP stream (idle [`KEEPALIVE_IDLE`],
/// interval [`KEEPALIVE_INTERVAL`]) so a silently-vanished peer on an IDLE
/// connection is eventually detected by the kernel — libpq enables keepalives by
/// default, and this matches it. TCP-only (a unix socket has no keepalive), so
/// the caller invokes this only on the TCP dial arm. The blocking twin of the
/// async driver's `set_tcp_keepalive`.
///
/// Uses `socket2`'s SAFE borrowed-fd API ([`socket2::SockRef::from`] +
/// `set_tcp_keepalive`), so this crate stays `#![forbid(unsafe_code)]` — the
/// `unsafe` fd handling lives inside `socket2`, never here. `socket2` is already
/// in the workspace graph (via the async driver's tokio), so this adds no new
/// crate.
fn set_tcp_keepalive(stream: &TcpStream) -> std::io::Result<()> {
    let params = socket2::TcpKeepalive::new()
        .with_time(KEEPALIVE_IDLE)
        .with_interval(KEEPALIVE_INTERVAL);
    socket2::SockRef::from(stream).set_tcp_keepalive(&params)
}

/// A lending `COPY … FROM STDIN` writer, handed to the closure of
/// [`copy_in_with`](Connection::copy_in_with).
///
/// Borrows the connection's [`Core`] for the copy's duration and streams each
/// row/chunk as one `CopyData` frame. Frames are BATCHED — accumulated in a
/// bounded send buffer and flushed only when it crosses a threshold (or at
/// finish) — so a megarow load costs far fewer socket writes than there are
/// rows, while the buffer stays bounded (CONSTANT memory, never O(rows); one
/// reused scratch buffer for [`write_row`](Self::write_row)'s trailing newline).
/// A chunk at or above the threshold streams directly, never buffered.
///
/// The writer never closes the copy itself: [`copy_in_with`](Connection::copy_in_with)
/// owns the terminal step (`CopyDone` on `Ok`, `CopyFail` on `Err`).
///
/// No `Debug`: it borrows the connection's engine (a live socket / TLS session).
pub struct CopyInWriter<'e> {
    core: &'e mut Core<SyncSocket>,
    /// Reused across [`write_row`](Self::write_row) calls so appending the row
    /// separator costs no per-row allocation.
    scratch: Vec<u8>,
}

impl CopyInWriter<'_> {
    /// Stream one `CopyData` frame with `chunk` as its verbatim body. Zero-copy:
    /// the bytes are queued directly (a large chunk is streamed straight to the
    /// socket, never buffered) and the flush is batched (see [`CopyInWriter`]).
    /// For text `COPY`, `chunk` is raw copy-format bytes — the caller controls row
    /// boundaries and framing.
    ///
    /// # Errors
    ///
    /// A classified [`DriverError`] on a transport fault (the connection is then
    /// dead) or a [`SpuriousPending`] over a blocking socket; never a panic.
    pub fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), DriverError> {
        drive_sync(engine::poll_once(self.core.copy_in_write(chunk)))
    }

    /// Stream one text-`COPY` row: `row`'s bytes followed by a `\n` separator, as
    /// one `CopyData` frame. A convenience over [`write_chunk`](Self::write_chunk)
    /// that reuses an internal scratch buffer for the newline (no per-row
    /// allocation).
    ///
    /// # Errors
    ///
    /// As [`write_chunk`](Self::write_chunk).
    pub fn write_row(&mut self, row: &[u8]) -> Result<(), DriverError> {
        self.scratch.clear();
        self.scratch.extend_from_slice(row);
        self.scratch.push(b'\n');
        // Disjoint field borrows: `&mut self.core` and `&self.scratch`.
        drive_sync(engine::poll_once(self.core.copy_in_write(&self.scratch)))
    }
}

/// A blocking PostgreSQL connection over the shared sans-IO engine.
pub struct Connection {
    /// The transport-generic driver engine: engine + liveness token + session
    /// facts + notification ledger (+ the N+1 tracker under `n1-detect`). Every
    /// non-I/O verb is defined on it once and shared with the async driver.
    core: Core<SyncSocket>,
    /// The credential-free endpoint snapshot for a cancel dial, captured from the
    /// [`ConnectConfig`] at connect. A [`cancel_token`](Self::cancel_token)
    /// combines it with the [`Core`]'s cancel key into a detached
    /// [`CancelToken`](crate::CancelToken); it carries no password, so it grants
    /// only the redial endpoint + TLS posture, never login.
    redial: Redial,
    /// A `try_clone` of the underlying socket (TCP or unix), used to arm socket
    /// read/write timeouts on a fd the engine otherwise owns: a dup'd handle
    /// shares the same kernel socket, so a timeout set here applies to the
    /// engine's own reads and writes. [`connect`](Self::connect) bounds the
    /// connect + handshake with `connect_timeout` then disarms it (steady-state
    /// I/O blocks indefinitely, matching the async driver);
    /// [`recv_notification`](Self::recv_notification) arms a bounded read deadline
    /// for its own poll and restores the disarmed state on exit. `None` for an
    /// in-memory testkit connection, which never blocks — the arming is then a
    /// no-op.
    socket_ctl: Option<SyncSock>,
}

impl Connection {
    /// Open a connection: TCP or unix-socket connect, optional TLS negotiation,
    /// then the startup/auth handshake through the engine.
    ///
    /// The transport is chosen by libpq's rule: an ABSOLUTE-PATH host (begins
    /// with `/`) selects a unix-domain socket at `<host>/.s.PGSQL.<port>`; every
    /// other host is TCP. A unix socket connects PLAINTEXT (TLS is pointless on a
    /// local kernel socket, and PostgreSQL does not offer it there), so
    /// [`SslMode::Require`] over a unix host is a fail-loud
    /// [`DriverError::Config`] rather than a silently-broken TLS contract.
    ///
    /// # Errors
    ///
    /// A classified [`DriverError`] for any pre-connect validation, transport,
    /// TLS, or handshake failure — never a panic.
    pub fn connect(config: &ConnectConfig) -> Result<Self, DriverError> {
        // Diagnostics off: no sink, so an SSL downgrade keeps the historical
        // stderr warning and nothing is installed on the connection.
        Self::connect_with(config, &Diagnostics::default())
    }

    /// Open a connection and install the structured-diagnostics configuration on
    /// it, so operational events (a server `NOTICE`, a slow query, an SSL
    /// downgrade at connect, …) surface through `diagnostics`' sink. The blocking
    /// twin of the async `Connection::connect_with`.
    ///
    /// Diagnostics is NOT a [`ConnectConfig`] field (the config footprint is
    /// untouched) — it rides the connection, and the sink is threaded into the
    /// connect sequence so a connect-time SSL `Prefer`→plaintext downgrade routes
    /// through it, not only steady-state events. A pool installs the same
    /// configuration on every connection it mints via
    /// [`Pool::builder`](crate::Pool::builder).
    ///
    /// # Errors
    ///
    /// The same classified [`DriverError`] set as [`connect`](Self::connect).
    pub fn connect_with(
        config: &ConnectConfig,
        diagnostics: &Diagnostics,
    ) -> Result<Self, DriverError> {
        let (sock, ssl_mode) = Self::dial_socket(config)?;
        // `connect_timeout` bounds ONLY the connect phase — the SSL `SSLRequest`
        // probe (TCP only) and the startup/auth handshake — armed as the socket
        // read+write timeout here and DISARMED once the handshake completes
        // (below): steady-state reads/writes then block indefinitely, matching the
        // async driver, so a slow query can never turn a healthy connection into a
        // fatal timeout.
        let connect_timeout = Duration::from_secs(config.connect_timeout_secs);
        sock.set_read_timeout(Some(connect_timeout))?;
        sock.set_write_timeout(Some(connect_timeout))?;
        // The dup'd control handle shares the kernel socket. Taken before the
        // socket is moved into the wire / TLS layer.
        let socket_ctl = sock.try_clone()?;

        let wire = Self::build_wire(sock, config, ssl_mode, diagnostics)?;
        // Snapshot the encryption state from the built wire BEFORE it is moved
        // into the engine.
        let encrypted = wire.is_encrypted();

        let user = Ident::try_from_str(&config.user)
            .map_err(|_| DriverError::Config("invalid user name"))?;
        let database = match &config.database {
            Some(d) => Some(
                DatabaseName::try_from_str(d)
                    .map_err(|_| DriverError::Config("invalid database name"))?,
            ),
            None => None,
        };
        let credentials = match config.password_str() {
            // Password auth is SCRAM-SHA-256 only; with the `scram` feature off
            // there is no client mechanism to satisfy it, so a supplied password
            // is a FAIL-LOUD config error at connect — never a silent Trust
            // attempt (which the server would reject anyway) or a panic.
            #[cfg(feature = "scram")]
            Some(pw) => {
                // RFC 5802 SCRAM mandates RFC 4013 SASLprep of the password
                // before PBKDF2 — a non-breaking space / soft hyphen /
                // NFKC-normalisable codepoint set through psql/libpq is stored
                // by the server in its SASLprep form, so the raw bytes would
                // never match. Normalise here (a prohibited codepoint is a
                // classified `DriverError::Config`) so proto sees the RFC form.
                let password = saslprep_password(pw)?;
                // Resolve SCRAM channel binding from the negotiated transport +
                // the consumer's policy: over TLS this hashes the server's
                // certificate into the `tls-server-end-point` binding data, so the
                // engine can select SCRAM-SHA-256-PLUS. `channel_binding=require`
                // over a plaintext channel fails closed here.
                let channel_binding = resolve_channel_binding(
                    encrypted,
                    wire.peer_end_entity_cert(),
                    config.channel_binding_mode(),
                )?;
                Credentials::ScramPassword(Sensitive::new(password), channel_binding)
            }
            #[cfg(not(feature = "scram"))]
            Some(_) => {
                return Err(DriverError::Config(
                    "password authentication needs the `scram` feature \
                     (SCRAM-SHA-256 support is not compiled in) — enable `scram`, \
                     or use a trust/peer-authenticated connection",
                ));
            }
            None => Credentials::Trust,
        };
        let startup_params = validate_startup_params(config)?;

        let (mut engine, live) =
            engine::open_owned(wire, &user, database.as_ref(), &startup_params, credentials)
                .map_err(lift_conn_fail)?;
        let live = flatten_poll(engine::poll_once(engine.connect(live)))?;
        // Handshake complete: disarm the connect-phase deadline so steady-state
        // reads and writes block indefinitely (async-parity). The only remaining
        // deadline is the bounded one `recv_notification` arms for its own wait.
        socket_ctl.set_read_timeout(None)?;
        socket_ctl.set_write_timeout(None)?;
        let backend_pid = engine.backend_pid().map_err(|_| DriverError::NotReady)?;
        // Capture the SECRET half of the cancel key alongside the pid, so a later
        // `cancel_token()` can build an out-of-band `CancelRequest`. Read out of
        // the engine's `Sensitive` here and re-wrapped inside `Core::new`.
        let secret_key = engine.with_secret_key(|s| s).map_err(|_| DriverError::NotReady)?;
        // The engine captured `server_version` from the startup `ParameterStatus`
        // reports during the handshake, so it is read here for free. `None` if the
        // server sent no such report (honest absence, not a fabricated value).
        let server_version = engine
            .server_version()
            .map_err(|_| DriverError::NotReady)?
            .map(str::to_owned);

        let mut this = Self {
            core: Core::new(engine, live, encrypted, server_version, backend_pid, secret_key),
            redial: Redial::from_config(config),
            socket_ctl: Some(socket_ctl),
        };
        // Install the full configuration (sink + slow-query threshold) for
        // steady-state events; the connect-time SSL-downgrade event already
        // routed through the sink threaded into `build_wire` above.
        this.set_diagnostics(diagnostics.clone());
        Ok(this)
    }

    /// Install (or replace) the structured-diagnostics configuration on this
    /// connection: the [`DiagSink`](bsql_postgres_core::DiagSink) callback plus the
    /// slow-query threshold. Passing [`Diagnostics::default`] turns diagnostics off.
    pub fn set_diagnostics(&mut self, diagnostics: Diagnostics) {
        self.core.set_diagnostics(diagnostics);
    }

    /// Open a connection over an in-memory
    /// [`FakeTransport`](bsql_postgres_core::testkit::FakeTransport) instead of a
    /// socket — the testkit entry point, the sync twin of the async `connect_fake`.
    ///
    /// It drives the real startup/auth handshake and every subsequent verb through
    /// the SAME engine the TCP path uses (single-poll: the fake never blocks), so
    /// the returned `Connection` is a genuine connection backed by the fake's
    /// scripted replies with no network. `socket_ctl` is `None`: there is no socket
    /// to arm a read timeout on, and the fake never blocks.
    ///
    /// # Errors
    ///
    /// A classified [`DriverError`] if the fake's handshake bytes are not a clean
    /// trust-auth chain the engine accepts — never a panic.
    #[cfg(feature = "testkit")]
    pub fn connect_fake(
        fake: bsql_postgres_core::testkit::FakeTransport,
    ) -> Result<Self, DriverError> {
        let wire: SyncWire = Wire::Fake(Box::new(fake));
        // The in-memory fake is plaintext by construction — no socket, no TLS.
        let encrypted = wire.is_encrypted();
        let user = Ident::try_from_str("bsql_testkit")
            .map_err(|_| DriverError::Config("invalid testkit user name"))?;
        let (mut engine, live) = engine::open_owned(wire, &user, None, &[], Credentials::Trust)
            .map_err(lift_conn_fail)?;
        let live = flatten_poll(engine::poll_once(engine.connect(live)))?;
        let backend_pid = engine.backend_pid().map_err(|_| DriverError::NotReady)?;
        let secret_key = engine.with_secret_key(|s| s).map_err(|_| DriverError::NotReady)?;
        let server_version = engine
            .server_version()
            .map_err(|_| DriverError::NotReady)?
            .map(str::to_owned);
        Ok(Self {
            core: Core::new(engine, live, encrypted, server_version, backend_pid, secret_key),
            // The in-memory fake has no network endpoint; a nominal loopback
            // redial satisfies the field (a testkit connection is never canceled).
            redial: Redial::from_config(&ConnectConfig::new("localhost", "")),
            socket_ctl: None,
        })
    }

    /// Resolve the endpoint, reject the fail-loud unix + `SslMode::Require`
    /// combination, and DIAL the chosen socket (TCP with Nagle disabled, or unix)
    /// — returning the connected socket plus the resolved SSL mode. The single
    /// dial authority shared by [`connect`](Self::connect) and the cancel dial, so
    /// the two cannot drift on endpoint resolution or the unix-TLS rule.
    ///
    /// Timeout arming and the `try_clone` control handle are the CALLER's job (the
    /// two callers want different follow-ups), so this returns the bare socket.
    pub(crate) fn dial_socket(config: &ConnectConfig) -> Result<(SyncSock, SslMode), DriverError> {
        let endpoint = resolve_endpoint(&config.host, config.port);
        // Resolve the effective SSL mode ONCE against the endpoint (the
        // threat-scoped default: LOCAL → Prefer, REMOTE → Require; an explicit
        // mode wins). Thread it down so nothing below re-reads the raw config —
        // one resolution point, no drift between the two drivers.
        let ssl_mode = config.resolve_ssl_mode(&endpoint);
        // Fail LOUD: TLS cannot be required over a socket that will never do it.
        // Rejected before the connect syscall — a wasted dial would tell us
        // nothing, and this is a pre-connect configuration fault. (A defaulted
        // unix endpoint resolves to Prefer, so this fires only for an EXPLICIT
        // `SslMode::Require`.) Unix-only: on a non-unix target a unix endpoint can
        // never be dialed at all, so the platform rejection below subsumes this
        // (and takes precedence — the more fundamental fault wins).
        #[cfg(unix)]
        if endpoint.is_unix() && ssl_mode == SslMode::Require {
            return Err(DriverError::Config(
                "SslMode::Require cannot be honored over a unix-domain socket \
                 (TLS is not available on a local socket)",
            ));
        }
        let sock = match endpoint {
            Endpoint::Tcp(addr) => {
                // Bound the TCP connect (the SYN) by the connect-timeout budget so
                // a black-holed host fails within the budget instead of the OS SYN
                // timeout (~1-2 min) — the sync twin of the async driver wrapping
                // the whole connect in `tokio::time::timeout`. Applies to BOTH the
                // main connect and the cancel dial, since both route through here.
                let budget = Duration::from_secs(config.connect_timeout_secs);
                let tcp = connect_tcp_within(&addr, budget)?;
                // Disable Nagle on the data socket for the connection's whole life
                // — Nagle + delayed-ACK can add ~40ms stalls to small writes and
                // COPY-in streaming; one setsockopt with zero per-op cost. Nagle
                // is a TCP concept — `AF_UNIX` has no such buffering, so the unix
                // arm skips it (it is meaningless, not an error).
                tcp.set_nodelay(true)?;
                // Enable TCP keepalive so a silently-vanished peer on an IDLE
                // connection is detected by the kernel (libpq enables keepalives
                // by default; this matches it). TCP-only — a unix socket has no
                // keepalive concept, so the unix arm skips it.
                set_tcp_keepalive(&tcp)?;
                SyncSock::Tcp(tcp)
            }
            #[cfg(unix)]
            Endpoint::Unix(path) => SyncSock::Unix(UnixStream::connect(&path)?),
            // No unix-domain socket on a non-unix target: fail loud + classified,
            // never a silent TCP fallback or a panic. The classification lives in
            // `resolve_endpoint` (portable); only the dial is platform-specific.
            #[cfg(not(unix))]
            Endpoint::Unix(_path) => return Err(DriverError::Config(UNIX_SOCKET_UNSUPPORTED)),
        };
        Ok((sock, ssl_mode))
    }

    /// Build the plaintext or TLS wire, performing the PG `SSLRequest` negotiation
    /// on the raw socket when SSL is wanted.
    ///
    /// A unix-domain socket is ALWAYS plaintext: TLS over a local kernel socket is
    /// pointless, PostgreSQL does not offer it there, and `SslMode::Require` +
    /// unix was already rejected by [`dial_socket`](Self::dial_socket). So the
    /// `SSLRequest` probe runs only for a TCP socket with SSL wanted; `Prefer`
    /// over unix is plaintext with no probe and no downgrade warning (nothing was
    /// downgraded — TLS was never applicable to a local socket).
    pub(crate) fn build_wire(
        sock: SyncSock,
        config: &ConnectConfig,
        ssl_mode: SslMode,
        // Threaded to `classify_ssl_response` so a `Prefer`→plaintext downgrade
        // routes through the diagnostics sink. TLS-only: with `tls` off there is
        // no probe and no downgrade to report, so the parameter is unused there.
        #[cfg_attr(
            not(feature = "tls"),
            expect(
                unused_variables,
                reason = "the SSL-downgrade signal is TLS-only; with tls off there is no SSLRequest probe and no downgrade to surface"
            )
        )]
        diagnostics: &Diagnostics,
    ) -> Result<SyncWire, DriverError> {
        if sock.is_unix() || ssl_mode == SslMode::Disable {
            return Ok(Wire::Plain(SyncSocket::new(sock)));
        }
        // A TCP socket with `ssl_mode` == `Prefer` or `Require` here.
        //
        // With `tls` OFF the client cannot negotiate TLS at all: `Require` or a
        // custom CA is a FAIL-LOUD `DriverError::Config` at connect (never a
        // silent plaintext connect the consumer believes is encrypted), and
        // `Prefer` connects plaintext with the SSLRequest probe compiled out —
        // `is_encrypted()` is then always `false`.
        #[cfg(not(feature = "tls"))]
        {
            if ssl_mode == SslMode::Require {
                return Err(DriverError::Config(
                    "TLS support is not compiled in (the `tls` feature is off); \
                     SslMode::Require cannot be honored — enable the `tls` feature, \
                     or use SslMode::Prefer/Disable for a plaintext connection",
                ));
            }
            if config.ca_roots_pem().is_some() {
                return Err(DriverError::Config(
                    "TLS support is not compiled in (the `tls` feature is off); \
                     custom CA roots (with_ca_roots / sslrootcert / PGSSLROOTCERT) \
                     cannot be used — enable the `tls` feature",
                ));
            }
            Ok(Wire::Plain(SyncSocket::new(sock)))
        }
        #[cfg(feature = "tls")]
        {
            let ssl_bytes = bsql_postgres_core::ssl::ssl_request_bytes();
            let mut tcp = sock;
            // The armed connect-phase `connect_timeout` firing on a server silent on
            // the `SSLRequest` probe byte is a connect-phase TIMEOUT, classified as
            // such via `lift_probe_io`: the SAME class the async driver and the
            // post-probe TLS handshake surface, so the two drivers agree. A bare `?`
            // would instead map it to the generic `DriverError::Io(TimedOut)`. Every
            // other io error keeps its real class.
            Write::write_all(&mut tcp, ssl_bytes).map_err(lift_probe_io)?;
            let mut response = [0u8; 1];
            Read::read_exact(&mut tcp, &mut response).map_err(lift_probe_io)?;
            match bsql_postgres_core::ssl::classify_ssl_response(
                response[0],
                config,
                ssl_mode,
                diagnostics,
            )? {
                bsql_postgres_core::ssl::SslProbe::Accepted { server_name } => {
                    // Use the provider-explicit ring config; custom CA roots build a
                    // config verified against EXACTLY those roots, otherwise the shared
                    // default-roots config. A bad/empty custom PEM is a classified
                    // `Config` error — fail-closed, never a fallback.
                    let cfg = match config.ca_roots_pem() {
                        Some(pem) => {
                            tls::client_config_with_ca_roots(pem).map_err(lift_ca_roots_error)?
                        }
                        None => tls::shared_client_config().map_err(|e| {
                            DriverError::Io(io::Error::other(format!("TLS config: {e}")))
                        })?,
                    };
                    let socket = SyncSocket::new(tcp);
                    let tls =
                        match engine::poll_once(TlsTransport::connect(socket, cfg, server_name)) {
                            Ok(Ok(transport)) => transport,
                            Ok(Err(e)) => return Err(lift_tls_error(e)),
                            Err(SpuriousPending) => return Err(DriverError::SpuriousPending),
                        };
                    Ok(Wire::Tls(Box::new(tls)))
                }
                bsql_postgres_core::ssl::SslProbe::PlainTcp => {
                    Ok(Wire::Plain(SyncSocket::new(tcp)))
                }
            }
        }
    }

    /// Mint a detached [`CancelToken`](crate::CancelToken) for this connection's
    /// in-flight (or next) query.
    ///
    /// The token is `Send + Sync + 'static` and borrows NOTHING from this
    /// connection, so it can be obtained BEFORE a long blocking query and moved to
    /// another THREAD that calls [`cancel`](crate::CancelToken::cancel) while the
    /// query is still running. It is unforgeable (the cancel key's secret is
    /// minted only at connect).
    ///
    /// PostgreSQL cancellation is OUT-OF-BAND (a second connection) and
    /// BEST-EFFORT by spec (§55.4): `cancel()` REQUESTS cancellation; it does not
    /// guarantee the query stops. See [`CancelToken`](crate::CancelToken).
    #[must_use]
    pub fn cancel_token(&self) -> crate::CancelToken {
        crate::CancelToken::new(self.core.cancel_key(), self.redial.clone())
    }

    // ── Delegated runtime-SQL verbs (one `poll_once` drive each) ────────────

    /// Round-trip a `Sync` to confirm the connection is live.
    pub fn ping(&mut self) -> Result<(), DriverError> {
        drive_sync(engine::poll_once(self.core.ping()))
    }

    /// Issue a simple query, returning the command tag string.
    pub fn simple_query(&mut self, sql: &str) -> Result<String, DriverError> {
        drive_sync(engine::poll_once(self.core.simple_query(sql)))
    }

    /// Execute a non-row runtime-SQL command, returning the affected-row count.
    /// The compile-checked counterpart is [`execute`](Self::execute).
    pub fn execute_sql(&mut self, sql: &str) -> Result<u64, DriverError> {
        drive_sync(engine::poll_once(self.core.execute_sql(sql)))
    }

    /// Run a row-returning runtime-SQL query (text result columns). The
    /// compile-checked, typed counterpart is [`query`](Self::query).
    pub fn query_sql(&mut self, sql: &str) -> Result<QueryResult, DriverError> {
        drive_sync(engine::poll_once(self.core.query_sql(sql)))
    }

    /// Run a runtime-SQL query returning the first row, or [`DriverError::NoRows`].
    pub fn query_one_sql(&mut self, sql: &str) -> Result<Row, DriverError> {
        drive_sync(engine::poll_once(self.core.query_one_sql(sql)))
    }

    /// Run a runtime-SQL query returning the first row if any (typed peer:
    /// [`query_opt`](Self::query_opt)).
    pub fn query_opt_sql(&mut self, sql: &str) -> Result<Option<Row>, DriverError> {
        drive_sync(engine::poll_once(self.core.query_opt_sql(sql)))
    }

    /// Stream a runtime raw-SQL query's rows one at a time to `on_row` in CONSTANT
    /// memory — the dynamic (untyped) streaming peer of [`query_sql`](Self::query_sql),
    /// and the PostgreSQL peer of the SQLite driver's `query_each_sql` (so a
    /// dynamic stream reads the SAME on both backends).
    ///
    /// Each row is handed to `on_row` as a zero-copy [`BorrowedRow`] as it arrives,
    /// accumulating NOTHING — a colossal runtime SELECT streams without growing
    /// memory (the escape from eager `query_sql`). `on_row` returns [`ControlFlow`]:
    /// [`Continue`](ControlFlow::Continue) to keep streaming, or
    /// [`Break(e)`](ControlFlow::Break) to stop early; the borrowed row cannot
    /// escape the closure (`for<'r>`). Reads are POSITIONAL (the result's column
    /// names arrive only after every row).
    ///
    /// # Returns
    ///
    /// - `Ok(None)` — streamed to completion.
    /// - `Ok(Some(e))` — `on_row` broke early; the connection was drained back to a
    ///   clean idle and stays healthy + pooled.
    /// - `Err(DriverError::Decode(..))` — a row body was malformed (LOUD, never
    ///   swallowed); `Err(DriverError::Db(..))` — a server error mid-stream; either
    ///   leaves the connection drained + healthy. Other `Err` — a fatal fault; the
    ///   connection is dead.
    ///
    /// A [`Break`](ControlFlow::Break) of a colossal result still reads and discards
    /// the remaining rows to reach the reusable idle boundary — O(remaining rows).
    /// An oversize row is reassembled into a reused scratch buffer and streamed like
    /// an inline one (constant memory, no cap).
    pub fn query_each_sql<F, E>(&mut self, sql: &str, on_row: F) -> Result<Option<E>, DriverError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        drive_sync(engine::poll_once(self.core.query_each_sql(sql, on_row)))
    }

    /// Stream a runtime parameterised query's rows one at a time to `on_row` in
    /// CONSTANT memory — the dynamic streaming peer of
    /// [`query_params`](Self::query_params), and the PostgreSQL peer of the SQLite
    /// driver's `query_each_params`. See [`query_each_sql`](Self::query_each_sql)
    /// for the full contract; the params are borrowed all the way to the engine.
    pub fn query_each_params<P: ParamsWriter, F, E>(
        &mut self,
        sql: &str,
        params: &P,
        on_row: F,
    ) -> Result<Option<E>, DriverError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        drive_sync(engine::poll_once(self.core.query_each_params(sql, params, on_row)))
    }

    /// Prepare a statement: `Parse` + `Describe` + `Sync`, recovering the result
    /// schema for later `Bind`+`Execute`.
    pub fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, DriverError> {
        drive_sync(engine::poll_once(self.core.prepare(sql)))
    }

    /// Execute a prepared statement returning rows. The params are borrowed all the
    /// way to the engine, so a non-`Copy` owned param binds by reference.
    pub fn query_prepared<P: ParamsWriter>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        drive_sync(engine::poll_once(self.core.query_prepared(stmt, params)))
    }

    /// Execute a prepared statement for its side effect, returning the affected
    /// count.
    pub fn execute_prepared<P: ParamsWriter>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<u64, DriverError> {
        drive_sync(engine::poll_once(self.core.execute_prepared(stmt, params)))
    }

    /// Prepare, query, and close a runtime SQL statement with params. The three
    /// engine round trips run in ONE `poll_once` drive (each resolves on its first
    /// poll over the blocking transport).
    pub fn query_params<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        drive_sync(engine::poll_once(self.core.query_params(sql, params)))
    }

    /// Like [`query_params`](Self::query_params), returning the first row.
    pub fn query_params_one<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<Row, DriverError> {
        drive_sync(engine::poll_once(self.core.query_params_one(sql, params)))
    }

    /// Like [`query_params`](Self::query_params), returning the first row if any.
    pub fn query_params_opt<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<Option<Row>, DriverError> {
        drive_sync(engine::poll_once(self.core.query_params_opt(sql, params)))
    }

    /// Prepare, execute, and close a runtime SQL statement with params.
    pub fn execute_params<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<u64, DriverError> {
        drive_sync(engine::poll_once(self.core.execute_params(sql, params)))
    }

    /// Close a prepared statement, consuming it (use-after-close is a move error).
    pub fn close_statement(&mut self, stmt: PreparedStatement) -> Result<(), DriverError> {
        drive_sync(engine::poll_once(self.core.close_statement(stmt)))
    }

    // ── Compile-checked typed verbs (the `query!` flagship) ─────────────────
    //
    // `#[track_caller]` works on a plain blocking `fn`, so — unlike the async
    // driver's `fn -> impl Future` shape — each is a direct blocking call. Under
    // `n1-detect` the wrapper is `#[track_caller]` and passes the USER's captured
    // call site to the shared `Core` verb as a cfg-gated argument; when off the
    // argument is cfg-removed and there is no `#[track_caller]` ABI cost.

    /// Execute a compile-checked `query!` for its side effect, returning the
    /// affected-row count (binary-uniform params). Parses the content-addressed
    /// statement once per connection, then reuses the server-side plan. The
    /// runtime-SQL escape hatch is [`execute_sql`](Self::execute_sql).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn execute<P, R>(
        &mut self,
        q: &'static PreparedQuery<P, R>,
        params: P,
    ) -> Result<u64, DriverError>
    where
        P: ParamsWriter + 'static,
        R: RowDecode + 'static,
    {
        drive_sync(engine::poll_once(self.core.execute(
            q,
            params,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )))
    }

    /// Run a compile-checked `query!` and collect its TYPED rows — the flagship
    /// parameterised query.
    ///
    /// `Q` is a `query!`-generated carrier; the returned [`Rows<Q>`] decodes lazily
    /// into the macro's typed records — borrowed (zero-copy text) via
    /// [`Rows::iter`], or owned via [`Rows::into_owned`]. SQL is validated against
    /// the schema at build time, params are bound in binary. An oversize row
    /// (wider than the engine's inline read buffer) is reassembled into the
    /// prebuffer and decodes identically to an inline one — no size cap. The
    /// statement is Parsed once per connection and the server-side plan reused
    /// thereafter. The runtime-SQL escape hatch is [`query_sql`](Self::query_sql).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query<'p, Q: TypedQuery>(&mut self, params: Q::Params<'p>) -> Result<Rows<Q>, DriverError> {
        drive_sync(engine::poll_once(self.core.query::<Q>(
            params,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )))
    }

    /// Run a compile-checked `query!` expecting EXACTLY one row, returning the
    /// owned record. Zero rows is [`DriverError::NoRows`]; more than one is
    /// [`DriverError::TooManyRows`] (loud, never a silently-taken first row).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_one<'p, Q: TypedQuery>(&mut self, params: Q::Params<'p>) -> Result<Q::Owned, DriverError> {
        drive_sync(engine::poll_once(self.core.query_one::<Q>(
            params,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )))
    }

    /// Run a compile-checked `query!` expecting AT MOST one row, returning the
    /// owned record if present or `None` if absent — the by-key maybe-absent shape.
    /// Zero rows is `Ok(None)`; more than one is [`DriverError::TooManyRows`]
    /// (loud, never a silently-taken first row). The zero-or-one peer of
    /// [`query_one`](Self::query_one); the runtime-SQL escape hatch is
    /// [`query_opt_sql`](Self::query_opt_sql).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_opt<'p, Q: TypedQuery>(
        &mut self,
        params: Q::Params<'p>,
    ) -> Result<Option<Q::Owned>, DriverError> {
        drive_sync(engine::poll_once(self.core.query_opt::<Q>(
            params,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )))
    }

    /// Stream a compile-checked `query!`'s rows one at a time to `on_row` in
    /// CONSTANT memory — the streaming peer of [`query`](Self::query).
    ///
    /// Each `DataRow` decodes as it arrives (borrowed, zero-copy) and is handed to
    /// `on_row`, accumulating NOTHING. `on_row` returns [`ControlFlow`]:
    /// [`Continue`](ControlFlow::Continue) to keep streaming, or
    /// [`Break(e)`](ControlFlow::Break) to stop early. The borrowed record CANNOT
    /// escape the closure (the `for<'q>` bound is the escape wall).
    ///
    /// # Returns
    ///
    /// - `Ok(None)` — streamed to completion.
    /// - `Ok(Some(e))` — `on_row` broke early; drained back to idle, stays healthy.
    /// - `Err(DriverError::Decode(..))` — a row failed to decode into its
    ///   compile-time shape; drained, stays healthy — LOUD, never swallowed.
    /// - `Err(DriverError::Db(..))` — a server error mid-stream; drained, healthy.
    /// - other `Err` — a fatal transport/protocol fault; the connection is dead.
    ///
    /// An oversize row (wider than the inline read buffer) is reassembled into a
    /// reused scratch buffer and streamed to `on_row` exactly like an inline one
    /// — constant memory (bounded by the widest oversize row), no size cap.
    ///
    /// A [`Break`](ControlFlow::Break) of a colossal result still reads the
    /// remaining rows to reach the clean idle boundary — O(remaining rows).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_each<'p, Q, F, E>(
        &mut self,
        params: Q::Params<'p>,
        on_row: F,
    ) -> Result<Option<E>, DriverError>
    where
        Q: TypedQuery,
        F: for<'q> FnMut(Q::Record<'q>) -> ControlFlow<E>,
    {
        drive_sync(engine::poll_once(self.core.query_each::<Q, F, E>(
            params,
            on_row,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )))
    }

    // ── Migration runner ─────────────────────────────────────────────────────

    /// Apply every pending migration from `source` to the database, exactly
    /// once, in deterministic order — the runtime migration RUNNER. See
    /// [`bsql_postgres_core::migrate`] for the ledger / atomicity /
    /// checksum-drift / advisory-lock guarantees.
    pub fn run_migrations<'a>(
        &'a mut self,
        source: impl Into<MigrationSource<'a>>,
    ) -> Result<MigrationReport, MigrationError> {
        use bsql_postgres_core::migrate;
        let source = source.into();
        // Acquire the migration lock by NON-BLOCKING poll with backoff (see the
        // async driver): a waiter holds no long-lived transaction, so a
        // `CREATE INDEX CONCURRENTLY` migration cannot deadlock against it.
        let start = std::time::Instant::now();
        let mut backoff = migrate::LOCK_POLL_INITIAL;
        loop {
            let got = drive_sync(engine::poll_once(self.core.try_acquire_migration_lock()))
                .map_err(MigrationError::from)?;
            if got {
                break;
            }
            let elapsed = start.elapsed();
            if elapsed >= migrate::LOCK_ACQUIRE_TIMEOUT {
                return Err(MigrationError::LockTimeout);
            }
            // Surface the wait so a serialized deploy is not mistaken for a hang.
            self.core
                .diagnostics()
                .emit(&bsql_postgres_core::DiagEvent::MigrationLockWaiting { elapsed });
            std::thread::sleep(backoff);
            backoff = migrate::next_backoff(backoff);
        }
        // Lock held — apply, then ALWAYS release (best effort).
        let result = drive_migration(engine::poll_once(self.core.apply_pending_locked(source)));
        match drive_sync(engine::poll_once(self.core.release_migration_lock())) {
            Ok(()) | Err(_) => {}
        }
        result
    }

    /// A read-only snapshot of applied vs pending migrations (no lock, no
    /// apply).
    pub fn migration_status<'a>(
        &'a mut self,
        source: impl Into<MigrationSource<'a>>,
    ) -> Result<MigrationStatus, MigrationError> {
        drive_migration(engine::poll_once(self.core.migration_status(source.into())))
    }

    /// Report which migrations WOULD be applied (running the same drift checks
    /// as [`run_migrations`](Self::run_migrations)) without applying anything.
    pub fn dry_run_migrations<'a>(
        &'a mut self,
        source: impl Into<MigrationSource<'a>>,
    ) -> Result<Vec<String>, MigrationError> {
        drive_migration(engine::poll_once(self.core.dry_run_migrations(source.into())))
    }

    // ── Transaction / session boundary primitives ───────────────────────────

    /// `BEGIN` a transaction.
    pub fn begin(&mut self) -> Result<(), DriverError> {
        drive_sync(engine::poll_once(self.core.begin()))
    }

    /// `COMMIT` the current transaction.
    pub fn commit(&mut self) -> Result<(), DriverError> {
        drive_sync(engine::poll_once(self.core.commit()))
    }

    /// `ROLLBACK` the current transaction.
    pub fn rollback(&mut self) -> Result<(), DriverError> {
        drive_sync(engine::poll_once(self.core.rollback()))
    }

    /// Run `f` inside a transaction: `COMMIT` on `Ok`, best-effort `ROLLBACK` on
    /// `Err`.
    ///
    /// Tier-1 safety: `f` is handed a borrowing [`Transaction`] guard — NOT the
    /// whole `Connection`. The guard exposes ONLY the data verbs (query / execute /
    /// the typed `query!` verbs), so `tx.begin()` / `tx.commit()` / `tx.rollback()`
    /// / a nested `tx.transaction(..)` / `tx.close()` inside the body do not exist
    /// (a method-not-found compile error, E0599). Transaction atomicity is thus a
    /// COMPILE-TIME guarantee: hand-driving the transaction lifecycle from the body
    /// — or nesting a helper that opens its own transaction (PostgreSQL has no
    /// nested transactions, so the inner `COMMIT` would silently flatten the outer's
    /// atomic scope) — is impossible by construction. The `transaction` wrapper
    /// alone owns the terminating `COMMIT` / `ROLLBACK`; on a body error the
    /// caller's error dominates and a best-effort `ROLLBACK` is issued (its outcome
    /// rides the liveness token — a failed `ROLLBACK` leaves the connection dead,
    /// which [`is_healthy`](Self::is_healthy) reports so a pool evicts it).
    ///
    /// The `BEGIN` is DEFERRED and PIPELINED with the first statement the body
    /// issues: it rides that statement's flush (one round trip carries both), so a
    /// one-statement transaction costs the pipelined round trips, not a separate
    /// `BEGIN` round trip plus the statement's. The `BEGIN` is armed INSIDE that
    /// first verb, never out-of-band at entry — so if the body PANICS before
    /// issuing any verb (the panic caught upstream, the bare connection reused),
    /// NOTHING is staged: the connection is left clean, never carrying a stranded
    /// `BEGIN` a later verb would silently fuse. An EMPTY body (no statement) is
    /// therefore a true no-op: it opens nothing and costs zero round trips — no
    /// `COMMIT` / `ROLLBACK` is issued. A fused `BEGIN` that errors surfaces as the
    /// transaction's failure (it cannot be swallowed by the first statement).
    pub fn transaction<R>(
        &mut self,
        f: impl FnOnce(&mut Transaction<'_>) -> Result<R, DriverError>,
    ) -> Result<R, DriverError> {
        // The BEGIN is NOT armed out-of-band here. The guard arms it inside the
        // first verb, so a body that panics before issuing any verb leaves nothing
        // staged. The guard borrows `self.core` for the body's scope ONLY; the
        // block ends that borrow (and reads back whether a verb opened the
        // transaction) so the terminating COMMIT/ROLLBACK can re-borrow `self.core`.
        let (outcome, opened) = {
            let mut tx = Transaction { core: &mut self.core, begin_armed: false };
            let outcome = f(&mut tx);
            (outcome, tx.begin_armed)
        };
        // Terminate ONLY if a verb actually opened the transaction. An empty body
        // armed no BEGIN, so there is nothing to commit or roll back — and the
        // terminator can never carry a fused BEGIN into the next verb.
        let result = match outcome {
            Ok(value) => {
                if opened {
                    self.simple_query("COMMIT")?;
                }
                Ok(value)
            }
            Err(e) => {
                // Best-effort rollback; the outcome rides the liveness token, so it
                // is explicitly discarded. The caller's error `e` dominates.
                if opened {
                    drop(self.simple_query("ROLLBACK"));
                }
                Err(e)
            }
        };
        // The transaction scope closes a logical operation: forget the N+1 recency
        // window (a no-op with the feature off).
        self.core.n1_reset();
        result
    }

    /// Subscribe to a `LISTEN` channel. The channel name is validated as an
    /// unquoted identifier BEFORE interpolation — an injection-shaped name is a
    /// classified [`DriverError::Config`], never spliced into SQL.
    pub fn listen(&mut self, channel: &str) -> Result<(), DriverError> {
        drive_sync(engine::poll_once(self.core.listen(channel)))
    }

    /// Unsubscribe from a `LISTEN` channel (validated as [`listen`](Self::listen)).
    pub fn unlisten(&mut self, channel: &str) -> Result<(), DriverError> {
        drive_sync(engine::poll_once(self.core.unlisten(channel)))
    }

    /// Reset all BLEEDABLE session state so this connection can be safely reused by
    /// a different logical user, WITHOUT dropping prepared statements.
    ///
    /// Runs `DISCARD ALL` MINUS `DEALLOCATE ALL` / `DISCARD PLANS` (session GUCs
    /// incl. `search_path`, role, cursors, `LISTEN`s, advisory locks, temp tables,
    /// sequence caches) in one round trip — prefixed with `ROLLBACK` only when
    /// inside a transaction — then clears the notification ledger. Prepared
    /// statements (content-addressed plans safe to share) are KEPT so the
    /// server-side plan reuse survives a pool checkout with no cache invalidation.
    ///
    /// # Liveness bound (why a pool checkout can never hang on a dead peer)
    ///
    /// The reset is a round-trip liveness probe run on every pool checkout, so a
    /// pooled connection whose peer VANISHED silently (a NAT idle-drop, a cable
    /// pull, an AZ partition — a half-open socket where no FIN/RST ever arrives)
    /// must not block the checkout for the kernel's `tcp_retries2` budget
    /// (~15 min). This reset therefore arms a bounded socket read+write timeout —
    /// the SAME control handle [`recv_notification`](Self::recv_notification) and
    /// [`connect`](Self::connect) arm — bounded by the connection's own
    /// `connect_timeout` (a reset is a mini handshake, so it earns the handshake's
    /// budget; no separate knob, so the
    /// [`ConnectConfig`](bsql_postgres_core::ConnectConfig) footprint is untouched).
    /// On a vanished peer the read (or a write into a full send buffer) ELAPSES
    /// into a fatal transport error — the reset's pump has no would-block quiet arm
    /// (that arm is unique to the notification wait) — so the token is dropped,
    /// this returns classified, and a pool EVICTS the connection and hands out a
    /// fresh one (or a classified acquire-timeout if the whole budget is spent)
    /// instead of hanging. A healthy reset completes in microseconds, far inside
    /// the budget, so the deadline never fires on the happy path (no added round
    /// trip, only a pair of `setsockopt`s bracketing the existing round trip).
    ///
    /// # Errors
    ///
    /// Any transport / server error is returned classified; a pool evicts a
    /// connection whose reset failed rather than handing out a still-dirty one.
    pub fn reset_session(&mut self) -> Result<(), DriverError> {
        // Arm the bounded read+write timeout BEFORE the inner verb takes the token,
        // so a failed `set_*_timeout` syscall returns Err with the token still live
        // — never stranding it and bricking a connection nothing touched on the
        // wire. No socket (testkit) → nothing to arm; the wait is vacuous. The
        // WHOLE reset sequence (the RESET simple-query plus the batched
        // dynamic-statement Close) runs under this per-read/write ceiling.
        let budget = Duration::from_secs(self.redial.connect_timeout_secs());
        if let Some(ctl) = &self.socket_ctl {
            ctl.set_read_timeout(Some(budget)).map_err(DriverError::Io)?;
            ctl.set_write_timeout(Some(budget)).map_err(DriverError::Io)?;
        }
        let result = drive_sync(engine::poll_once(self.core.reset_session()));
        // Restore the disarmed (block-forever) steady state in EVERY arm BEFORE
        // returning, so the caller's real verbs block indefinitely again (the
        // steady-state I/O contract), exactly as `recv_notification` restores.
        let restore = match &self.socket_ctl {
            Some(ctl) => ctl
                .set_read_timeout(None)
                .and_then(|()| ctl.set_write_timeout(None)),
            None => Ok(()),
        };
        match result {
            Ok(()) => {
                restore.map_err(DriverError::Io)?;
                Ok(())
            }
            Err(e) => {
                restore.map_err(DriverError::Io)?;
                Err(e)
            }
        }
    }

    // ── Notifications (read-timeout arming is blocking-specific; stays here) ─

    /// Wait up to `timeout` for the next asynchronous notification.
    ///
    /// Drains the per-connection notification ledger FIRST (a notification that
    /// already arrived returns immediately with NO round trip). Only when the
    /// ledger is empty does this wait on the socket, bounded by setting the socket
    /// read timeout on the control handle; a read-timeout on the engine's reads
    /// surfaces inside the engine as the quiet outcome — the token rides back in
    /// `Ok`, so the connection stays alive.
    ///
    /// # Errors
    ///
    /// A malformed or non-UTF-8 buffered notification surfaces here as a classified
    /// [`DriverError`] (it is still removed from the ledger) — never a silent drop.
    pub fn recv_notification(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<Notification>, DriverError> {
        // An already-arrived notification returns without touching the socket.
        if let Some(buffered) = self.core.drain_one_notification() {
            return buffered.map(Some);
        }
        // Arm the fallible read timeout BEFORE the shared inner verb takes the
        // token, so a failed `set_read_timeout` syscall returns Err with the token
        // still live — never stranding it and bricking a connection nothing touched
        // on the wire. No socket (testkit) → no timeout to arm; the wait is vacuous.
        if let Some(ctl) = &self.socket_ctl {
            ctl.set_read_timeout(Some(timeout)).map_err(DriverError::Io)?;
        }
        let received = drive_sync(engine::poll_once(self.core.recv_notification_inner()));
        // Disarm the bounded read deadline so subsequent verbs block indefinitely
        // again (the steady-state I/O contract). Restored in EVERY arm BEFORE the
        // buffered notification is drained, so a disarm failure leaves the
        // notification buffered (recoverable next call), never lost.
        let restore = match &self.socket_ctl {
            Some(ctl) => ctl.set_read_timeout(None),
            None => Ok(()),
        };
        match received {
            Ok(got) => {
                restore.map_err(DriverError::Io)?;
                if got {
                    self.core.take_expected_notification()
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                restore.map_err(DriverError::Io)?;
                Err(e)
            }
        }
    }

    /// The count of asynchronous notifications currently buffered in the ledger
    /// and awaiting [`recv_notification`](Self::recv_notification).
    #[must_use]
    pub fn buffered_notifications(&self) -> usize {
        self.core.buffered_notifications()
    }

    /// The total number of asynchronous notifications ever captured on this
    /// connection (buffered, drained, or shed) — monotonic.
    #[must_use]
    pub fn notifications_received(&self) -> u64 {
        self.core.notifications_received()
    }

    /// The number of asynchronous notifications SHED because the bounded ledger was
    /// full — monotonic. Non-zero means notifications were lost to the bound; the
    /// loss is LOUD (visible here), never a silent drop.
    #[must_use]
    pub fn notifications_shed(&self) -> u64 {
        self.core.notifications_shed()
    }

    /// Wait up to `timeout` for the next notification, parsing its payload into an
    /// application type `T` — the typed peer of
    /// [`recv_notification`](Self::recv_notification).
    ///
    /// # Errors
    ///
    /// [`DriverError::PayloadParse`] (carrying the raw payload) if the payload does
    /// not parse into `T` — a LOUD classified error, never a silently-dropped or
    /// defaulted notification. The notification is still removed from the ledger.
    pub fn recv_notification_as<T: FromStr>(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<TypedNotification<T>>, DriverError> {
        match self.recv_notification(timeout)? {
            Some(n) => match n.payload.parse::<T>() {
                Ok(payload) => Ok(Some(TypedNotification {
                    channel: n.channel,
                    payload,
                    pid: n.pid,
                })),
                // The payload string moves into the classified error as a
                // read-only `Box<str>` (no spare capacity retained).
                Err(_) => Err(DriverError::PayloadParse(n.payload.into_boxed_str())),
            },
            None => Ok(None),
        }
    }

    // ── COPY (`FnOnce` `copy_in_with` stays here; `copy_out` delegates) ──────

    /// `COPY <table> FROM STDIN`, bulk-loading `rows_data` in CONSTANT memory — the
    /// ergonomic batch form of [`copy_in_with`](Self::copy_in_with).
    ///
    /// Each item is streamed as one text-`COPY` row through the lending writer, so
    /// the rows are NOT pre-collected. `table` is validated as an identifier BEFORE
    /// interpolation.
    ///
    /// # Errors
    ///
    /// A row rejected by the server is a classified [`DriverError::Db`], and the
    /// connection RECOVERS to a clean idle (it stays pooled). A transport fault is
    /// fatal.
    pub fn copy_in(
        &mut self,
        table: &str,
        rows_data: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<u64, DriverError> {
        self.copy_in_with(table, |w| {
            for row in rows_data {
                w.write_row(row.as_ref().as_bytes())?;
            }
            Ok(())
        })
    }

    /// `COPY <table> FROM STDIN` with a scoped streaming writer: run `f` against a
    /// [`CopyInWriter`], then finish (`CopyDone`) if it returns `Ok` or abort
    /// (`CopyFail`) if it returns `Err`. The CONSTANT-MEMORY, cancellation-safe
    /// bulk-load primitive.
    ///
    /// `f` may interleave arbitrary work between rows, and `write_chunk` /
    /// `write_row` each frame one `CopyData`; the frames are batched into a bounded
    /// send buffer and flushed at a threshold (or at finish), so any row count
    /// loads in bounded memory with far fewer socket writes than rows.
    ///
    /// # Cancellation and recovery
    ///
    /// The terminal step is owned here, not by the writer's `Drop`:
    ///
    /// - `f` returns `Ok(())` → `CopyDone`; the server's `COPY n` count is returned
    ///   and the connection stays clean and pooled.
    /// - `f` returns `Err(e)` → `CopyFail`; the server tears the COPY down and the
    ///   verb drains the recovering `ReadyForQuery`, so the connection is
    ///   RECOVERABLE. The caller's `e` is returned.
    ///
    /// `table` is validated as an identifier (see [`copy_in`](Self::copy_in)).
    ///
    /// # Errors
    ///
    /// A server rejection at `CopyDone` is a recoverable [`DriverError::Db`]; `f`'s
    /// own error is returned verbatim; a transport fault is fatal.
    pub fn copy_in_with<F>(&mut self, table: &str, f: F) -> Result<u64, DriverError>
    where
        F: FnOnce(&mut CopyInWriter<'_>) -> Result<(), DriverError>,
    {
        // The table splice is validated ONCE, in `Core::copy_in_begin_table`
        // (the single COPY-in splice site shared by both drivers): an
        // injection-shaped table is a classified `DriverError::Config` there,
        // never assembled into SQL. On a fault the token is dropped by `Core` —
        // the connection is dead.
        let live = drive_sync(engine::poll_once(self.core.copy_in_begin_table(table)))?;
        let body = {
            let mut writer = CopyInWriter {
                core: &mut self.core,
                scratch: Vec::new(),
            };
            f(&mut writer)
            // `writer` is dropped here, releasing the `&mut self.core` borrow.
        };
        match body {
            // `copy_in_finish` restores the token on either status and maps a server
            // rejection to `DriverError::Db` with the connection kept pooled.
            Ok(()) => drive_sync(engine::poll_once(self.core.copy_in_finish(live))),
            Err(e) => {
                // The caller abandoned the copy: `CopyFail` reclaims the connection
                // (the abort's `ServerErrored` is expected, not a fault); a transport
                // fault leaves it dead. The caller's `e` dominates either way, so the
                // best-effort abort's single-poll verdict is deliberately consumed and
                // discarded — the token was restored inside `copy_in_abort` on success,
                // and a spurious pending on a best-effort abort is unrecoverable.
                match engine::poll_once(self.core.copy_in_abort(live)) {
                    Ok(()) | Err(SpuriousPending) => {}
                }
                Err(e)
            }
        }
    }

    /// `COPY … FROM STDIN` in PGCOPY BINARY, bulk-loading `rows` into the
    /// compile-checked target of a [`copy!`](bsql_postgres_core::TypedCopyIn)
    /// carrier `Q`, in CONSTANT memory — the TYPED, injection-safe-by-construction
    /// peer of [`copy_in`](Self::copy_in).
    ///
    /// Each item is a typed row tuple `Q::Row<'q>`: a `NOT NULL` column is `T`, a
    /// nullable column `Option<T>` (pass `None` for a SQL NULL), a `text` /
    /// `bytea` column borrows the caller's data. The target table, column list,
    /// and per-column types were pinned at COMPILE time by `copy!` against the
    /// migration catalog, so a wrong-typed or wrong-arity row is a compile error —
    /// and there is no COPY text to mis-escape (an embedded tab / newline / quote
    /// rides the binary field verbatim). Rows are NOT pre-collected; a megarow
    /// load streams in bounded memory. The blocking peer of the async
    /// [`Connection::copy_in_typed`](crate::Connection::copy_in_typed) (they share
    /// the one `Core::copy_in_typed`, driven to completion in a single poll here).
    ///
    /// # Errors
    ///
    /// A row the server rejects at `CopyDone` is a classified [`DriverError::Db`],
    /// and the connection RECOVERS to a clean idle (it stays pooled). A transport
    /// fault is fatal.
    pub fn copy_in_typed<'q, Q, I>(&mut self, rows: I) -> Result<u64, DriverError>
    where
        Q: TypedCopyIn,
        I: IntoIterator<Item = Q::Row<'q>>,
    {
        drive_sync(engine::poll_once(self.core.copy_in_typed::<Q, I>(rows)))
    }

    /// `COPY <table> TO STDOUT`, streaming each row to `on_chunk` in CONSTANT
    /// memory — the bulk-unload peer of [`copy_in`](Self::copy_in).
    ///
    /// Each server `CopyData` frame is handed to `on_chunk` as a borrowed slice into
    /// the transient ingest buffer; nothing is accumulated. The borrowed chunk
    /// CANNOT escape the closure (the `for<'q>` bound is the escape wall).
    /// `on_chunk` returns [`ControlFlow`]. `table` is validated as an identifier.
    ///
    /// # Returns
    ///
    /// - `Ok(None)` — streamed to completion; clean and pooled.
    /// - `Ok(Some(e))` — `on_chunk` broke early; drained back to idle, stays healthy.
    /// - `Err(DriverError::Db(..))` — a server error mid-unload; drained, healthy.
    /// - other `Err` — a fatal transport/protocol fault; the connection is dead.
    ///
    /// A [`Break`](ControlFlow::Break) still reads the remaining `CopyData` to reach
    /// the clean idle — O(remaining rows).
    pub fn copy_out<F, E>(&mut self, table: &str, on_chunk: F) -> Result<Option<E>, DriverError>
    where
        F: for<'q> FnMut(&'q [u8]) -> ControlFlow<E>,
    {
        drive_sync(engine::poll_once(self.core.copy_out(table, on_chunk)))
    }

    // ── Lifecycle + accessors ───────────────────────────────────────────────

    /// Whether the connection is at a clean boundary and reusable.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.core.is_healthy()
    }

    /// The server version reported at connect, if recovered.
    #[must_use]
    pub fn server_version(&self) -> Option<&str> {
        self.core.server_version()
    }

    /// The backend process id from `BackendKeyData`.
    #[must_use]
    pub fn backend_pid(&self) -> i32 {
        self.core.backend_pid()
    }

    /// Whether this connection's traffic is TLS-encrypted.
    ///
    /// `true` iff the TLS handshake completed. `false` for a plaintext connection,
    /// INCLUDING a `SslMode::Prefer` connection the server downgraded to plain TCP
    /// (the downgrade also emits a stderr warning). A consumer over an untrusted
    /// network can ASSERT this after connect to reject a silent downgrade:
    ///
    /// ```no_run
    /// # fn check(conn: &bsql_postgres_sync::Connection) -> Result<(), &'static str> {
    /// if !conn.is_encrypted() {
    ///     return Err("refusing to proceed on a plaintext connection");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Captured at connect and never changes: PostgreSQL negotiates TLS once,
    /// before the startup packet, and never up- or down-grades mid-session.
    #[must_use]
    pub fn is_encrypted(&self) -> bool {
        self.core.is_encrypted()
    }

    /// The N+1 anti-patterns detected on this connection so far — one entry per
    /// `(query, source line)` site that ran past the detector's threshold within a
    /// single logical operation. Present ONLY under the `n1-detect` feature; purely
    /// diagnostic — enabling detection cannot change what any query returns.
    #[cfg(feature = "n1-detect")]
    #[must_use]
    pub fn n1_report(&self) -> &[bsql_postgres_core::N1Report] {
        self.core.n1_report()
    }

    /// Gracefully close the connection (`Terminate` + shutdown). Idempotent.
    pub fn close(&mut self) -> Result<(), DriverError> {
        drive_sync(engine::poll_once(self.core.close()))
    }

    /// BEST-EFFORT graceful close for a pooled connection the pool is DISCARDING
    /// (a [`Pool::close`](crate::Pool::close) drain, or a `max_lifetime` /
    /// `idle_timeout` reap): send a protocol `Terminate` so the server sees a
    /// CLEAN disconnect — not the "unexpected EOF on client connection" its log
    /// records for a bare socket drop — then let the socket close when `self`
    /// drops. The blocking twin of the async `close_graceful`.
    ///
    /// BOUNDED by the connection's own `connect_timeout`: it arms the SAME
    /// `SO_RCVTIMEO`/`SO_SNDTIMEO` control-handle ceiling [`reset_session`](Self::reset_session)
    /// arms, so a `Terminate` write into a full send buffer on a black-hole peer
    /// ELAPSES instead of hanging for the kernel's `tcp_retries2` budget (~15 min).
    /// The timeout is NOT restored — the connection is dropped next — so no restore
    /// error can surface. No `ConnectConfig` knob is added.
    ///
    /// Best-effort: any outcome is DISCARDED (the socket closes on drop
    /// regardless), so the pool's drain continues past a single dead peer.
    pub(crate) fn close_graceful(&mut self) {
        let budget = Duration::from_secs(self.redial.connect_timeout_secs());
        if let Some(ctl) = &self.socket_ctl {
            // Best-effort arm: a `setsockopt` on a live fd does not fail in
            // practice, and this connection is being discarded — a failed arm at
            // worst leaves the (rare) full-buffer write unbounded, so it is
            // discarded rather than propagated (there is no `Result` channel here).
            drop(ctl.set_read_timeout(Some(budget)));
            drop(ctl.set_write_timeout(Some(budget)));
        }
        drop(self.close());
    }
}

/// A borrowing transaction guard, handed to the closure of
/// [`transaction`](Connection::transaction).
///
/// The guard forbids EXACTLY the six transaction / connection LIFECYCLE verbs —
/// `begin`, `commit`, `rollback`, a nested `transaction`, `close`,
/// `reset_session` — so hand-driving the transaction boundary from inside the
/// body (or nesting a helper that opens its own transaction, which PostgreSQL
/// flattens with no diagnostic) is a compile error (E0599), not a silent runtime
/// atomicity break. That is the WHOLE point: the atomicity invariant is enforced
/// by the type, and the `transaction` wrapper alone owns the terminating `COMMIT`
/// / `ROLLBACK`.
///
/// EVERY other verb the body legitimately uses remains available: the runtime-SQL
/// family (`query_sql` / `execute_sql` / `query_params*` / prepared statements),
/// the compile-checked typed `query!` family (`query` / `query_one` /
/// `query_opt` / `query_each` / `execute`), bulk [`COPY`](Self::copy_in_with) in
/// and out, and `LISTEN` / `UNLISTEN`. COPY in particular is legal (and atomic)
/// inside a transaction — it completes its own COPY sub-protocol before returning
/// and never touches the transaction boundary, so atomic bulk-load-with-rollback
/// works exactly as it did when the closure received the whole `Connection`. The
/// sole non-lifecycle verb NOT offered is [`recv_notification`](Connection::recv_notification):
/// it needs the driver's connection-level read-timeout state the guard does not
/// hold, and a backend cannot receive notifications mid-transaction anyway.
///
/// Borrows the connection's [`Core`] for the closure scope, so it holds no
/// object past the call. Every verb drives the same shared `Core` verb the
/// [`Connection`] method drives (one [`engine::poll_once`] each) — the guard adds
/// no layer, and under `n1-detect` it records the USER's call site (not a
/// guard-internal line) via `#[track_caller]`.
///
/// No `Debug`: it borrows the connection's engine (a live socket / TLS session).
pub struct Transaction<'t> {
    core: &'t mut Core<SyncSocket>,
    /// `true` once the deferred `BEGIN` has been armed by the first verb. Armed
    /// exactly once, and ONLY from within a verb (never out-of-band at
    /// `transaction()` entry) — which is what makes a body that panics before any
    /// verb leave nothing staged. Read by the combinator after the body to decide
    /// whether a terminating `COMMIT` / `ROLLBACK` is owed.
    begin_armed: bool,
}

impl Transaction<'_> {
    /// Arm the deferred `BEGIN` once, immediately BEFORE the first verb runs.
    ///
    /// The first verb the body issues opens the transaction — its flush fuses the
    /// `BEGIN` (the 1-RTT win). A blocking verb runs to completion the instant it
    /// is called (there is no unpolled-future gap the async driver must guard
    /// against), so arming here — synchronously, immediately before the verb's own
    /// `take_live` — shares one critical section with it: a body that panics
    /// before issuing any verb never reaches this, so nothing is staged. This is
    /// the sync twin of the async guard's poll-time `armed` wrapper.
    #[inline]
    fn arm_begin(&mut self) {
        if !self.begin_armed {
            self.core.defer_begin();
            self.begin_armed = true;
        }
    }

    // ── Delegated runtime-SQL verbs (data only; one `poll_once` drive each) ──

    /// Round-trip a `Sync` to confirm the connection is live.
    pub fn ping(&mut self) -> Result<(), DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.ping()))
    }

    /// Issue a simple query, returning the command tag string.
    pub fn simple_query(&mut self, sql: &str) -> Result<String, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.simple_query(sql)))
    }

    /// Execute a non-row runtime-SQL command, returning the affected-row count.
    pub fn execute_sql(&mut self, sql: &str) -> Result<u64, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.execute_sql(sql)))
    }

    /// Run a row-returning runtime-SQL query (text result columns).
    pub fn query_sql(&mut self, sql: &str) -> Result<QueryResult, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query_sql(sql)))
    }

    /// Run a runtime-SQL query returning the first row, or [`DriverError::NoRows`].
    pub fn query_one_sql(&mut self, sql: &str) -> Result<Row, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query_one_sql(sql)))
    }

    /// Run a runtime-SQL query returning the first row if any (typed peer:
    /// [`query_opt`](Self::query_opt)).
    pub fn query_opt_sql(&mut self, sql: &str) -> Result<Option<Row>, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query_opt_sql(sql)))
    }

    /// Stream a runtime raw-SQL query's rows in CONSTANT memory, inside the
    /// transaction. See [`Connection::query_each_sql`] for the full contract.
    pub fn query_each_sql<F, E>(&mut self, sql: &str, on_row: F) -> Result<Option<E>, DriverError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query_each_sql(sql, on_row)))
    }

    /// Stream a runtime parameterised query's rows in CONSTANT memory, inside the
    /// transaction. See [`Connection::query_each_params`] for the full contract.
    pub fn query_each_params<P: ParamsWriter, F, E>(
        &mut self,
        sql: &str,
        params: &P,
        on_row: F,
    ) -> Result<Option<E>, DriverError>
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E>,
    {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query_each_params(sql, params, on_row)))
    }

    /// Prepare a statement: `Parse` + `Describe` + `Sync`.
    pub fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.prepare(sql)))
    }

    /// Execute a prepared statement returning rows.
    pub fn query_prepared<P: ParamsWriter>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query_prepared(stmt, params)))
    }

    /// Execute a prepared statement for its side effect, returning the affected
    /// count.
    pub fn execute_prepared<P: ParamsWriter>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<u64, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.execute_prepared(stmt, params)))
    }

    /// Prepare, query, and close a runtime SQL statement with params.
    pub fn query_params<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query_params(sql, params)))
    }

    /// Like [`query_params`](Self::query_params), returning the first row.
    pub fn query_params_one<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<Row, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query_params_one(sql, params)))
    }

    /// Like [`query_params`](Self::query_params), returning the first row if any.
    pub fn query_params_opt<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<Option<Row>, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query_params_opt(sql, params)))
    }

    /// Prepare, execute, and close a runtime SQL statement with params.
    pub fn execute_params<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<u64, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.execute_params(sql, params)))
    }

    /// Close a prepared statement, consuming it (use-after-close is a move error).
    pub fn close_statement(&mut self, stmt: PreparedStatement) -> Result<(), DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.close_statement(stmt)))
    }

    // ── Compile-checked typed verbs (the `query!` flagship) ─────────────────
    //
    // `#[track_caller]` on a plain blocking `fn` captures the USER's call site and
    // threads it to the shared `Core` verb (identical to the `Connection` methods).

    /// Execute a compile-checked `query!` for its side effect, returning the
    /// affected-row count (binary-uniform params).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn execute<P, R>(
        &mut self,
        q: &'static PreparedQuery<P, R>,
        params: P,
    ) -> Result<u64, DriverError>
    where
        P: ParamsWriter + 'static,
        R: RowDecode + 'static,
    {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.execute(
            q,
            params,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )))
    }

    /// Run a compile-checked `query!` and collect its TYPED rows — the flagship
    /// parameterised query.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query<'p, Q: TypedQuery>(&mut self, params: Q::Params<'p>) -> Result<Rows<Q>, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query::<Q>(
            params,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )))
    }

    /// Run a compile-checked `query!` expecting EXACTLY one row, returning the
    /// owned record. Zero rows is [`DriverError::NoRows`]; more than one is
    /// [`DriverError::TooManyRows`].
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_one<'p, Q: TypedQuery>(&mut self, params: Q::Params<'p>) -> Result<Q::Owned, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query_one::<Q>(
            params,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )))
    }

    /// Run a compile-checked `query!` expecting AT MOST one row, returning the
    /// owned record if present or `None` if absent. Zero rows is `Ok(None)`; more
    /// than one is [`DriverError::TooManyRows`]. The zero-or-one peer of
    /// [`query_one`](Self::query_one).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_opt<'p, Q: TypedQuery>(
        &mut self,
        params: Q::Params<'p>,
    ) -> Result<Option<Q::Owned>, DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query_opt::<Q>(
            params,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )))
    }

    /// Stream a compile-checked `query!`'s rows one at a time to `on_row` in
    /// CONSTANT memory — the streaming peer of [`query`](Self::query). See
    /// [`Connection::query_each`] for the full contract.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_each<'p, Q, F, E>(
        &mut self,
        params: Q::Params<'p>,
        on_row: F,
    ) -> Result<Option<E>, DriverError>
    where
        Q: TypedQuery,
        F: for<'q> FnMut(Q::Record<'q>) -> ControlFlow<E>,
    {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.query_each::<Q, F, E>(
            params,
            on_row,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )))
    }

    // ── COPY (bulk load / unload — legal + atomic inside a transaction) ─────
    //
    // COPY completes its own COPY sub-protocol before returning and touches NO
    // transaction boundary, so `COPY … FROM STDIN` inside a `transaction` body is
    // an atomic bulk load that commits/rolls back with the surrounding scope. The
    // deferred BEGIN even fuses into a COPY that is the transaction's first
    // statement (`Core::copy_in_begin`). These mirror the `Connection` methods.

    /// `COPY <table> FROM STDIN`, bulk-loading `rows_data` in CONSTANT memory —
    /// the ergonomic batch form of [`copy_in_with`](Self::copy_in_with).
    pub fn copy_in(
        &mut self,
        table: &str,
        rows_data: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<u64, DriverError> {
        self.copy_in_with(table, |w| {
            for row in rows_data {
                w.write_row(row.as_ref().as_bytes())?;
            }
            Ok(())
        })
    }

    /// `COPY <table> FROM STDIN` with a scoped streaming writer — the
    /// CONSTANT-MEMORY, cancellation-safe bulk-load primitive. See
    /// [`Connection::copy_in_with`] for the full cancellation / recovery contract;
    /// the orchestration here is identical (the guard just borrows the same
    /// [`Core`] the `Connection` method does).
    pub fn copy_in_with<F>(&mut self, table: &str, f: F) -> Result<u64, DriverError>
    where
        F: FnOnce(&mut CopyInWriter<'_>) -> Result<(), DriverError>,
    {
        // Arm the deferred BEGIN before the first COPY step, so a COPY that is the
        // transaction's first statement still fuses the BEGIN — and a body that
        // panicked before reaching here armed nothing.
        self.arm_begin();
        let live = drive_sync(engine::poll_once(self.core.copy_in_begin_table(table)))?;
        let body = {
            let mut writer = CopyInWriter {
                core: &mut *self.core,
                scratch: Vec::new(),
            };
            f(&mut writer)
        };
        match body {
            Ok(()) => drive_sync(engine::poll_once(self.core.copy_in_finish(live))),
            Err(e) => {
                // Best-effort abort; the caller's `e` dominates (see the
                // `Connection::copy_in_with` rationale for the discarded verdict).
                match engine::poll_once(self.core.copy_in_abort(live)) {
                    Ok(()) | Err(SpuriousPending) => {}
                }
                Err(e)
            }
        }
    }

    /// `COPY … FROM STDIN` in PGCOPY BINARY into a [`copy!`](bsql_postgres_core::TypedCopyIn)
    /// carrier `Q`'s target, in CONSTANT memory. See
    /// [`Connection::copy_in_typed`](crate::Connection::copy_in_typed) for the full
    /// typed contract; the deferred `BEGIN` fuses into this COPY when it is the
    /// transaction's FIRST statement.
    pub fn copy_in_typed<'q, Q, I>(&mut self, rows: I) -> Result<u64, DriverError>
    where
        Q: TypedCopyIn,
        I: IntoIterator<Item = Q::Row<'q>>,
    {
        // Arm the deferred BEGIN before the first COPY step, so a COPY that is the
        // transaction's first statement still fuses the BEGIN. The whole typed
        // COPY is one Core verb, driven to completion in a single poll.
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.copy_in_typed::<Q, I>(rows)))
    }

    /// `COPY <table> TO STDOUT`, streaming each row to `on_chunk` in CONSTANT
    /// memory. See [`Connection::copy_out`] for the full contract.
    pub fn copy_out<F, E>(&mut self, table: &str, on_chunk: F) -> Result<Option<E>, DriverError>
    where
        F: for<'q> FnMut(&'q [u8]) -> ControlFlow<E>,
    {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.copy_out(table, on_chunk)))
    }

    // ── Session subscription (LISTEN/UNLISTEN — atomicity-neutral) ──────────

    /// Subscribe to a `LISTEN` channel (validated; see [`Connection::listen`]).
    pub fn listen(&mut self, channel: &str) -> Result<(), DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.listen(channel)))
    }

    /// Unsubscribe from a `LISTEN` channel (see [`Connection::unlisten`]).
    pub fn unlisten(&mut self, channel: &str) -> Result<(), DriverError> {
        self.arm_begin();
        drive_sync(engine::poll_once(self.core.unlisten(channel)))
    }
}

// `Connection` deliberately has NO `Drop`: dropping it simply closes the socket
// fd, and PostgreSQL treats the resulting abrupt FIN identically to a graceful
// `Terminate` ('X') for a connection at idle — either way the backend tears down
// its session. A `Drop` that wrote `Terminate` would be a BLOCKING write on a
// socket whose write deadline is `None`; if the local send buffer were full and
// the peer had stopped ACKing (a half-open socket), that write could block
// unboundedly inside `drop` — a hazard Rust's `Drop` cannot signal or bound. A
// caller who wants the graceful protocol-level goodbye calls
// [`Connection::close`] explicitly (where blocking is the caller's choice); an
// implicit drop must never block. This matches the async driver, whose
// `Connection` likewise has no `Drop` — so the two drivers close a connection
// identically.

/// Classify an [`io::Error`] from the connect-phase `SSLRequest` probe — the raw
/// `write_all` / `read_exact` on the bare socket BEFORE the wire is built.
///
/// A read/write deadline (the armed connect-phase `connect_timeout` firing on a
/// server that never answers the probe) surfaces as [`WouldBlock`]/[`TimedOut`]
/// and is a connect-phase timeout, mapped to [`DriverError::Timeout`] — the SAME
/// class the async driver's connect budget and the post-probe TLS handshake use,
/// so the two drivers agree. Every OTHER io error keeps its real class; only the
/// timeout is remapped.
///
/// [`WouldBlock`]: io::ErrorKind::WouldBlock
/// [`TimedOut`]: io::ErrorKind::TimedOut
///
/// Reached only from the `tls`-gated SSLRequest probe in `build_wire`; with `tls`
/// off no probe is sent, so this helper is not compiled.
#[cfg(feature = "tls")]
fn lift_probe_io(e: io::Error) -> DriverError {
    match e.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => DriverError::Timeout,
        _ => DriverError::Io(e),
    }
}

#[cfg(test)]
mod keepalive_tests {
    //! C3 WITNESS (offline): [`set_tcp_keepalive`] turns `SO_KEEPALIVE` ON for a
    //! connected TCP stream — the blocking twin of the async driver's witness. A
    //! real loopback socket pair (no PG); the getter reads the option back off the
    //! fd via `socket2`.

    use super::set_tcp_keepalive;
    use std::net::{TcpListener, TcpStream};

    #[test]
    fn set_tcp_keepalive_enables_so_keepalive() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let client = TcpStream::connect(addr).expect("connect");
        let (_server, _peer) = listener.accept().expect("accept");

        assert!(
            !socket2::SockRef::from(&client)
                .keepalive()
                .expect("read keepalive before"),
            "a fresh TCP socket must have SO_KEEPALIVE off",
        );
        set_tcp_keepalive(&client).expect("enable keepalive");
        assert!(
            socket2::SockRef::from(&client)
                .keepalive()
                .expect("read keepalive after"),
            "set_tcp_keepalive must turn SO_KEEPALIVE on",
        );
    }
}
