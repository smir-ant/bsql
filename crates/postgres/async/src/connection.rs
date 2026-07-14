//! The tokio async PostgreSQL connection: a thin async adapter over the shared
//! [`Core`] driver engine.
//!
//! A [`Connection`] owns a [`Core<TokioSocket>`] — the transport-generic engine
//! that defines every non-I/O verb ONCE (shared verbatim with the blocking
//! driver) — plus the async-specific [`ReadDeadline`] cell. Every delegated verb
//! `.await`s the corresponding `Core` verb over the tokio socket: the pump future
//! suspends on a real `Pending` until the socket is ready and is woken by tokio's
//! reactor. The liveness token, health-bit semantics, and recoverable-error model
//! all live in `Core` (see its docs); this module supplies only what is genuinely
//! async — the connect sequence and its timeout budget, the notification read
//! deadline, and the async-closure `transaction` / `copy_in_with`.
//!
//! # Footprint regime
//!
//! The stable public *types* this driver re-exports (`Row`, `DriverError`,
//! `ConnectConfig`, `Notification`, …) carry their `size_of`/`align_of` pins in
//! `bsql-postgres-core`, where they are defined. The driver's own hot-path
//! futures (the state machine each delegating verb lowers to) are thin — a
//! `&mut Core` reborrow plus the `Core` verb's future — and are not pinned; the
//! working set is the engine's already-pinned buffers.

use core::future::Future;
use core::ops::ControlFlow;
use core::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

// `std::io` and the tokio read/write extension traits are named only by the
// `tls`-gated SSLRequest probe + TLS-config path in `build_tcp_wire`; with `tls`
// off no probe is sent and neither is used.
#[cfg(feature = "tls")]
use std::io;
#[cfg(feature = "tls")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
// Unix targets only — `tokio::net::UnixStream` is absent elsewhere; a unix-socket
// host on a non-unix target is rejected at connect (see the `Endpoint::Unix` arm).
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::time::Instant;

use bsql_postgres_core::driver::{lift_conn_fail, lift_engine_error, Core};
// The TLS handshake lifters + the SSLRequest probe + the rustls transport are
// reached only from the `tls`-gated arm of `build_tcp_wire`; with `tls` off the
// probe is compiled out and none of these are named.
#[cfg(feature = "tls")]
use bsql_postgres_core::driver::{lift_ca_roots_error, lift_tls_error};
#[cfg(feature = "tls")]
use bsql_postgres_core::ssl::SslProbe;
use bsql_postgres_core::tls::Wire;
#[cfg(feature = "tls")]
use bsql_postgres_core::tls::{self, TlsTransport};
use bsql_postgres_core::{
    resolve_endpoint, validate_startup_params, BorrowedRow, ConnectConfig, Diagnostics, DriverError,
    Endpoint, MigrationError, MigrationReport, MigrationSource, MigrationStatus, Notification,
    Pipeline, QueryResult, Redial, Row, Rows, SslMode, TypedNotification,
};
// Referenced only by the non-unix `Endpoint::Unix` reject arm in `build_wire`.
#[cfg(not(unix))]
use bsql_postgres_core::UNIX_SOCKET_UNSUPPORTED;
use bsql_postgres_proto::engine;
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

use crate::transport::{ReadDeadline, Sock, TokioSocket};

/// The prepared-statement handle (defined once in `bsql-postgres-core`, shared by
/// both drivers). Re-exported so `bsql_postgres_async::PreparedStatement` still
/// resolves.
pub use bsql_postgres_core::PreparedStatement;

/// The plaintext-or-TLS transport the engine is monomorphic over.
type AsyncWire = Wire<TokioSocket>;

/// TCP keepalive idle time — the kernel starts sending keepalive probes after
/// this long with no traffic on an IDLE connection.
const KEEPALIVE_IDLE: Duration = Duration::from_secs(60);
/// TCP keepalive probe interval — the gap between successive keepalive probes
/// once probing has begun.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Enable TCP keepalive on a connected TCP stream (idle [`KEEPALIVE_IDLE`],
/// interval [`KEEPALIVE_INTERVAL`]) so a silently-vanished peer on an IDLE
/// connection is eventually detected by the kernel — libpq enables keepalives by
/// default, and this matches it. TCP-only (a unix socket has no keepalive), so
/// the caller invokes this only on the TCP dial arm.
///
/// Uses `socket2`'s SAFE borrowed-fd API (`socket2::SockRef::from` +
/// `set_tcp_keepalive`), so this crate stays `#![forbid(unsafe_code)]` — the
/// `unsafe` fd handling lives inside `socket2`, never here. `socket2` is already
/// in the dependency graph (via tokio), so this adds no new crate.
fn set_tcp_keepalive(stream: &TcpStream) -> std::io::Result<()> {
    let params = socket2::TcpKeepalive::new()
        .with_time(KEEPALIVE_IDLE)
        .with_interval(KEEPALIVE_INTERVAL);
    socket2::SockRef::from(stream).set_tcp_keepalive(&params)
}

/// A lending `COPY … FROM STDIN` writer, handed to the closure of
/// [`copy_in_with`](Connection::copy_in_with).
///
/// Borrows the connection's [`Core`] for the copy's duration (so no other verb
/// can run concurrently), and streams each row/chunk as one `CopyData` frame.
/// Frames are BATCHED — accumulated in a bounded send buffer and flushed only
/// when it crosses a threshold (or at finish) — so a megarow load costs far
/// fewer socket writes than there are rows, while the buffer stays bounded
/// (CONSTANT memory, never O(rows); one reused scratch buffer for
/// [`write_row`](Self::write_row)'s trailing newline). A chunk at or above the
/// threshold streams directly, never buffered.
///
/// The writer never closes the copy itself: [`copy_in_with`](Connection::copy_in_with)
/// owns the terminal step (`CopyDone` on `Ok`, `CopyFail` on `Err`), so
/// cancellation is correct by construction.
///
/// No `Debug`: it borrows the connection's engine (a live socket / TLS session),
/// which is not `Debug`.
pub struct CopyInWriter<'e> {
    core: &'e mut Core<TokioSocket>,
    /// Reused across [`write_row`](Self::write_row) calls so appending the row
    /// separator costs no per-row allocation — the whole point of a streaming
    /// bulk load.
    scratch: Vec<u8>,
}

impl CopyInWriter<'_> {
    /// Stream one `CopyData` frame with `chunk` as its verbatim body. Zero-copy:
    /// the bytes are queued directly, no reframing (a large chunk is streamed
    /// straight to the socket, never buffered) and the flush is batched (see
    /// [`CopyInWriter`]). For text `COPY`, `chunk` is raw copy-format bytes — the
    /// caller controls row boundaries and framing.
    ///
    /// # Errors
    ///
    /// A classified [`DriverError`] on a transport fault (the connection is then
    /// dead); never a panic.
    pub async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), DriverError> {
        self.core.copy_in_write(chunk).await
    }

    /// Stream one text-`COPY` row: `row`'s bytes followed by a `\n` separator, as
    /// one `CopyData` frame. A convenience over [`write_chunk`](Self::write_chunk)
    /// that reuses an internal scratch buffer for the newline (no per-row
    /// allocation).
    ///
    /// # Errors
    ///
    /// As [`write_chunk`](Self::write_chunk).
    pub async fn write_row(&mut self, row: &[u8]) -> Result<(), DriverError> {
        self.scratch.clear();
        self.scratch.extend_from_slice(row);
        self.scratch.push(b'\n');
        // Disjoint field borrows: `&mut self.core` and `&self.scratch`.
        self.core.copy_in_write(&self.scratch).await
    }
}

/// A tokio async PostgreSQL connection over the shared sans-IO engine.
///
/// # Graceful close
///
/// Call [`close`](Self::close) to issue the PostgreSQL `Terminate` and shut the
/// write side down. `Drop` cannot `.await`, so a dropped (un-`close`d) connection
/// relies on the OS socket close rather than a graceful `Terminate` — the
/// standard async-Rust limitation. Pooled connections are returned, not dropped,
/// so the graceful path is the common one.
pub struct Connection {
    /// The transport-generic driver engine: engine + liveness token + session
    /// facts + notification ledger (+ the N+1 tracker under `n1-detect`). Every
    /// non-I/O verb is defined on it once and shared with the blocking driver.
    core: Core<TokioSocket>,
    /// The credential-free endpoint snapshot for a cancel dial, captured from the
    /// [`ConnectConfig`] at connect. A [`cancel_token`](Self::cancel_token)
    /// combines it with the [`Core`]'s cancel key into a detached
    /// [`CancelToken`](crate::CancelToken); it carries no password, so it grants
    /// only the redial endpoint + TLS posture, never login.
    redial: Redial,
    /// Shared with the [`TokioSocket`] the engine owns: the driver arms an
    /// absolute read deadline here before [`recv_notification`](Self::recv_notification)
    /// and disarms it after, so a notification wait times out from inside the read
    /// (never by dropping the verb future, which would strand the linear token).
    read_deadline: Arc<ReadDeadline>,
}

impl Connection {
    /// Open a connection: TCP or unix-socket connect, optional TLS negotiation,
    /// then the startup/auth handshake through the engine — the WHOLE sequence
    /// bounded by `connect_timeout`, measured from the start.
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
    /// TLS, or handshake failure — never a panic. A connect that does not complete
    /// within `connect_timeout` is [`DriverError::Timeout`], so a server that
    /// accepts the connection but never answers the startup packet fails fast
    /// rather than hanging forever.
    pub async fn connect(config: &ConnectConfig) -> Result<Self, DriverError> {
        // Diagnostics off: no sink, so an SSL downgrade keeps the historical
        // stderr warning and nothing is installed on the connection.
        Self::connect_with(config, &Diagnostics::default()).await
    }

    /// Open a connection and install the structured-diagnostics configuration on
    /// it, so operational events (a server `NOTICE`, a slow query, an SSL
    /// downgrade at connect, …) surface through `diagnostics`' sink.
    ///
    /// This is how a standalone connection (outside a pool) opts into
    /// diagnostics; a pool installs the same configuration on every connection it
    /// mints via [`Pool::builder`](crate::Pool::builder). Diagnostics is NOT a
    /// [`ConnectConfig`] field (the config footprint is untouched) — it rides the
    /// connection, so different connections over the same config can carry
    /// different sinks. The sink is threaded into the connect sequence itself, so
    /// a connect-time event (an SSL `Prefer`→plaintext downgrade) routes through
    /// it — not only steady-state events after connect.
    ///
    /// # Errors
    ///
    /// The same classified [`DriverError`] set as [`connect`](Self::connect).
    pub async fn connect_with(
        config: &ConnectConfig,
        diagnostics: &Diagnostics,
    ) -> Result<Self, DriverError> {
        // Bound the ENTIRE connect sequence under ONE `connect_timeout` budget,
        // measured from the start. On elapse tokio drops the in-flight future;
        // nothing is stranded, since no `Connection` (and no reusable liveness
        // token) exists yet.
        let budget = Duration::from_secs(config.connect_timeout_secs);
        let mut conn = match tokio::time::timeout(
            budget,
            Self::connect_inner(config, diagnostics),
        )
        .await
        {
            Ok(result) => result?,
            // The same class the blocking driver surfaces for a connect-phase
            // (handshake) timeout, so the two drivers agree.
            Err(_elapsed) => return Err(DriverError::Timeout),
        };
        // Install the full configuration (sink + slow-query threshold) for
        // steady-state events. The connect-time SSL-downgrade event already
        // routed through the sink threaded into `connect_inner` above.
        conn.set_diagnostics(diagnostics.clone());
        Ok(conn)
    }

    /// Install (or replace) the structured-diagnostics configuration on this
    /// connection: the [`DiagSink`](bsql_postgres_core::DiagSink) callback plus the
    /// slow-query threshold. Passing [`Diagnostics::default`] turns diagnostics off.
    pub fn set_diagnostics(&mut self, diagnostics: Diagnostics) {
        self.core.set_diagnostics(diagnostics);
    }

    /// The connect sequence proper — run UNDER the `connect_timeout` budget by
    /// [`connect_with`](Self::connect_with). `diagnostics` is threaded to the wire
    /// build so a connect-time SSL downgrade routes through its sink.
    async fn connect_inner(
        config: &ConnectConfig,
        diagnostics: &Diagnostics,
    ) -> Result<Self, DriverError> {
        // The read-deadline cell shared with the socket the engine will own. It
        // carries the steady-state client-liveness window derived from the
        // configured `statement_timeout` (`None` when unset), so a black-holed
        // in-flight query is bounded WITHOUT cutting a query the server allows.
        let read_deadline = Arc::new(ReadDeadline::new(
            config.client_liveness_window(),
            config.connect_timeout_secs,
        ));
        // Dial the chosen transport (TCP or unix) and build the wire. No
        // dial-only timeout: the caller's single outer budget bounds the whole
        // sequence, so a black-hole dial elapses into `DriverError::Timeout`
        // exactly like a silent handshake.
        let wire = Self::connect_wire(config, &read_deadline, diagnostics).await?;
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
        let live = engine.connect(live).await.map_err(lift_engine_error)?;
        let backend_pid = engine.backend_pid().map_err(|_| DriverError::NotReady)?;
        // Capture the SECRET half of the cancel key alongside the pid, so a later
        // `cancel_token()` can build an out-of-band `CancelRequest`. Read out of
        // the engine's `Sensitive` here and re-wrapped inside `Core::new`.
        let secret_key = engine.with_secret_key(|s| s).map_err(|_| DriverError::NotReady)?;
        // The engine captured `server_version` from the startup `ParameterStatus`
        // reports during the handshake, so it is read here for free — no
        // `SHOW server_version` round-trip. `None` if the server sent no such
        // report (honest absence, not a fabricated value).
        let server_version = engine
            .server_version()
            .map_err(|_| DriverError::NotReady)?
            .map(str::to_owned);

        Ok(Self {
            core: Core::new(engine, live, encrypted, server_version, backend_pid, secret_key),
            redial: Redial::from_config(config),
            read_deadline,
        })
    }

    /// Dial the transport chosen by [`resolve_endpoint`] and build the
    /// plaintext-or-TLS wire.
    ///
    /// A TCP endpoint disables Nagle and runs the SSL negotiation
    /// ([`build_tcp_wire`](Self::build_tcp_wire)); a unix-socket endpoint connects
    /// PLAINTEXT (no `TCP_NODELAY` — meaningless on `AF_UNIX`; no `SSLRequest`
    /// probe — TLS is not applicable to a local socket). `SslMode::Require` over a
    /// unix host is a fail-loud [`DriverError::Config`], never a silent plaintext
    /// downgrade.
    pub(crate) async fn connect_wire(
        config: &ConnectConfig,
        deadline: &Arc<ReadDeadline>,
        diagnostics: &Diagnostics,
    ) -> Result<AsyncWire, DriverError> {
        let endpoint = resolve_endpoint(&config.host, config.port);
        // Resolve the effective SSL mode ONCE against the endpoint (the
        // threat-scoped default: LOCAL → Prefer, REMOTE → Require; an explicit
        // mode wins). Thread it down so nothing below re-reads the raw config —
        // one resolution point, no drift.
        let ssl_mode = config.resolve_ssl_mode(&endpoint);
        match endpoint {
            Endpoint::Tcp(addr) => {
                let tcp = TcpStream::connect(&addr).await?;
                // Disable Nagle on the data socket for the connection's whole life
                // — Nagle + delayed-ACK can add ~40ms stalls to small writes and
                // COPY-in streaming; one setsockopt with zero per-op cost.
                tcp.set_nodelay(true)?;
                // Enable TCP keepalive so a silently-vanished peer on an IDLE
                // connection is detected by the kernel (libpq enables keepalives by
                // default; this matches it). TCP-only — a unix socket has no
                // keepalive concept.
                set_tcp_keepalive(&tcp)?;
                Self::build_tcp_wire(tcp, config, ssl_mode, deadline, diagnostics).await
            }
            #[cfg(unix)]
            Endpoint::Unix(path) => {
                // Fail LOUD: TLS cannot be required over a socket that will never
                // do it. A local kernel socket is trusted by filesystem
                // permissions, not TLS, and PostgreSQL does not offer TLS there.
                // (A defaulted unix endpoint resolves to Prefer, so this fires
                // only for an EXPLICIT `SslMode::Require`.)
                if ssl_mode == SslMode::Require {
                    return Err(DriverError::Config(
                        "SslMode::Require cannot be honored over a unix-domain socket \
                         (TLS is not available on a local socket)",
                    ));
                }
                // `Prefer` over unix is plaintext with no probe and no downgrade
                // warning — nothing was downgraded; TLS was never applicable.
                let unix = UnixStream::connect(&path).await?;
                Ok(Wire::Plain(TokioSocket::new(
                    Sock::Unix(unix),
                    Arc::clone(deadline),
                )))
            }
            // No unix-domain socket on a non-unix target: fail loud + classified,
            // never a silent TCP fallback or a panic. The classification lives in
            // `resolve_endpoint` (portable); only the dial is platform-specific.
            #[cfg(not(unix))]
            Endpoint::Unix(_path) => Err(DriverError::Config(UNIX_SOCKET_UNSUPPORTED)),
        }
    }

    /// Build the plaintext or TLS wire over an already-connected TCP socket,
    /// performing the PG `SSLRequest` negotiation on the raw socket when SSL is
    /// wanted.
    async fn build_tcp_wire(
        tcp: TcpStream,
        config: &ConnectConfig,
        ssl_mode: SslMode,
        deadline: &Arc<ReadDeadline>,
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
    ) -> Result<AsyncWire, DriverError> {
        if ssl_mode == SslMode::Disable {
            return Ok(Wire::Plain(TokioSocket::new(
                Sock::Tcp(tcp),
                Arc::clone(deadline),
            )));
        }
        // `ssl_mode` is `Prefer` or `Require` here.
        //
        // With `tls` OFF the client cannot negotiate TLS at all: `Require` or a
        // custom CA is a FAIL-LOUD `DriverError::Config` at connect (never a
        // silent plaintext connect the consumer believes is encrypted), and
        // `Prefer` connects plaintext with the SSLRequest probe compiled out
        // (nothing to negotiate) — `is_encrypted()` is then always `false`.
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
            Ok(Wire::Plain(TokioSocket::new(
                Sock::Tcp(tcp),
                Arc::clone(deadline),
            )))
        }
        #[cfg(feature = "tls")]
        {
            let ssl_bytes = bsql_postgres_core::ssl::ssl_request_bytes();
            let mut tcp = tcp;
            tcp.write_all(ssl_bytes).await?;
            let mut response = [0u8; 1];
            tcp.read_exact(&mut response).await?;
            match bsql_postgres_core::ssl::classify_ssl_response(
                response[0],
                config,
                ssl_mode,
                diagnostics,
            )? {
                SslProbe::Accepted { server_name } => {
                    // Use the provider-explicit ring config (the workspace pins
                    // rustls to ring only). Custom CA roots use a SHARED config
                    // verified against EXACTLY those roots — shared per root set so
                    // reconnections resume the TLS session; otherwise the shared
                    // default-roots config. A bad/empty custom PEM is a classified
                    // `Config` error — fail-closed.
                    let cfg = match config.ca_roots_pem() {
                        Some(pem) => {
                            tls::shared_client_config_with_ca_roots(pem)
                                .map_err(lift_ca_roots_error)?
                        }
                        None => tls::shared_client_config().map_err(|e| {
                            DriverError::Io(io::Error::other(format!("TLS config: {e}")))
                        })?,
                    };
                    let socket = TokioSocket::new(Sock::Tcp(tcp), Arc::clone(deadline));
                    let tls = TlsTransport::connect(socket, cfg, server_name)
                        .await
                        .map_err(lift_tls_error)?;
                    Ok(Wire::Tls(Box::new(tls)))
                }
                SslProbe::PlainTcp => Ok(Wire::Plain(TokioSocket::new(
                    Sock::Tcp(tcp),
                    Arc::clone(deadline),
                ))),
            }
        }
    }

    /// Open a connection over an in-memory
    /// [`FakeTransport`](bsql_postgres_core::testkit::FakeTransport) instead of a
    /// socket — the testkit entry point.
    ///
    /// It drives the real startup/auth handshake and every subsequent verb
    /// through the SAME engine the TCP path uses, so the returned `Connection` is
    /// a genuine connection backed by the fake's scripted replies with no network.
    ///
    /// # Errors
    ///
    /// A classified [`DriverError`] if the fake's handshake bytes are not a clean
    /// trust-auth chain the engine accepts — never a panic.
    #[cfg(feature = "testkit")]
    pub async fn connect_fake(
        fake: bsql_postgres_core::testkit::FakeTransport,
    ) -> Result<Self, DriverError> {
        let wire: AsyncWire = Wire::Fake(Box::new(fake));
        // The in-memory fake is plaintext by construction — no socket, no TLS.
        let encrypted = wire.is_encrypted();
        // The fake never blocks, so it ignores the read deadline; a fresh disarmed
        // cell with no steady window (and a nominal connect_timeout) satisfies the
        // struct invariant.
        let read_deadline = Arc::new(ReadDeadline::new(None, 0));
        let user = Ident::try_from_str("bsql_testkit")
            .map_err(|_| DriverError::Config("invalid testkit user name"))?;
        let (mut engine, live) = engine::open_owned(wire, &user, None, &[], Credentials::Trust)
            .map_err(lift_conn_fail)?;
        let live = engine.connect(live).await.map_err(lift_engine_error)?;
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
            read_deadline,
        })
    }

    /// Mint a detached [`CancelToken`](crate::CancelToken) for this connection's
    /// in-flight (or next) query.
    ///
    /// The token is `Send + Sync + 'static` and borrows NOTHING from this
    /// connection, so it can be obtained BEFORE a long query and moved to another
    /// task that calls [`cancel`](crate::CancelToken::cancel) while the query is
    /// still running — with no `&mut` aliasing against the in-flight verb future.
    /// It is unforgeable (the cancel key's secret is minted only at connect).
    ///
    /// PostgreSQL cancellation is OUT-OF-BAND (a second connection) and
    /// BEST-EFFORT by spec (§55.4): `cancel()` REQUESTS cancellation; it does not
    /// guarantee the query stops. See [`CancelToken`](crate::CancelToken).
    #[must_use]
    pub fn cancel_token(&self) -> crate::CancelToken {
        crate::CancelToken::new(self.core.cancel_key(), self.redial.clone())
    }

    // ── Delegated runtime-SQL verbs ─────────────────────────────────────────
    //
    // The non-SQL verbs (`ping`) and the prepared-EXECUTE / typed `query!` verbs
    // RETURN the shared `Core` verb's future directly (a `fn -> impl Future`
    // forwarder, no wrapping `async` block) so there is no extra forwarder
    // state-machine layer. The DYNAMIC runtime-SQL verbs (raw text + params +
    // streaming + `prepare`) instead route through [`observed`](Self::observed),
    // which awaits the `Core` verb and then RE-DERIVES the client-liveness window
    // from the executed SQL's effect on `statement_timeout` — so a runtime
    // `SET`/`RESET`/`set_config` can never leave the window stale below a raised
    // budget (a false cut). The bare RPIT leaks the future's `Send` (which the pool
    // relies on) in both shapes, so no `+ Send` is added and `.await` call sites are
    // unaffected. Observation is NOT on the per-row hot path — it fires once per
    // verb, after the round trip completes.

    /// Run a DYNAMIC runtime-SQL verb, then OBSERVE its effect on the server's
    /// `statement_timeout` so the client-liveness window is re-derived — never left
    /// stale below a runtime-raised budget (which would falsely cut a query the
    /// server now allows, the ABSOLUTE mandate). The ONE chokepoint EVERY dynamic
    /// runtime-SQL verb routes through, so none can silently skip observation; the
    /// build-time-constant typed `query!` verbs and the prepared-EXECUTE verbs (a
    /// fixed plan; the SQL was already seen at [`prepare`](Self::prepare)) carry no
    /// runtime GUC change and are deliberately NOT routed here.
    ///
    /// Observes only on SUCCESS (a failed `SET` changed nothing on the server) and
    /// via the shared [`ReadDeadline::observe_statement_timeout`](crate::transport)
    /// primitive — the same one the transaction guard uses, so a `SET` inside a
    /// `transaction` closure re-derives identically.
    fn observed<'a, T, F, Fut>(
        &'a mut self,
        sql: &'a str,
        run: F,
    ) -> impl Future<Output = Result<T, DriverError>> + 'a
    where
        F: FnOnce(&'a mut Core<TokioSocket>) -> Fut + 'a,
        Fut: Future<Output = Result<T, DriverError>> + 'a,
        T: 'a,
    {
        // Disjoint field borrows: the verb runs on `core`, the observation applies
        // to the shared `read_deadline` cell — never the same field, so the split
        // is a compiler fact.
        let core = &mut self.core;
        let read_deadline = &self.read_deadline;
        async move {
            let result = run(core).await;
            if result.is_ok() {
                read_deadline.observe_statement_timeout(sql);
            }
            result
        }
    }

    /// Round-trip a `Sync` to confirm the connection is live.
    pub fn ping(&mut self) -> impl Future<Output = Result<(), DriverError>> + '_ {
        self.core.ping()
    }

    /// Issue a simple query, returning the command tag string.
    ///
    /// A successful top-level `SET`/`RESET`/`set_config` of `statement_timeout` here
    /// RE-DERIVES the client-liveness window (see [`observed`](Self::observed)) so it
    /// is never left stale below a runtime-raised server budget. The returned future
    /// is `Send` (the RPIT leaks it), so the pool is unaffected.
    pub fn simple_query<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<String, DriverError>> + 'a {
        self.observed(sql, move |c| c.simple_query(sql))
    }

    /// Execute a non-row runtime-SQL command, returning the affected-row count.
    /// The compile-checked counterpart is [`execute`](Self::execute).
    ///
    /// A successful top-level `SET`/`RESET`/`set_config` of `statement_timeout` here
    /// RE-DERIVES the client-liveness window (see [`observed`](Self::observed)).
    pub fn execute_sql<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<u64, DriverError>> + 'a {
        self.observed(sql, move |c| c.execute_sql(sql))
    }

    /// Run a row-returning runtime-SQL query (text result columns). The
    /// compile-checked, typed counterpart is [`query`](Self::query).
    ///
    /// A successful top-level `SET`/`RESET`/`set_config` of `statement_timeout` here
    /// RE-DERIVES the client-liveness window (see [`observed`](Self::observed)).
    pub fn query_sql<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<QueryResult, DriverError>> + 'a {
        self.observed(sql, move |c| c.query_sql(sql))
    }

    /// Run a runtime-SQL query returning the first row, or [`DriverError::NoRows`].
    /// The compile-checked counterpart is [`query_one`](Self::query_one). A
    /// `set_config('statement_timeout', …)` here RE-DERIVES the client-liveness
    /// window (see [`observed`](Self::observed)).
    pub fn query_one_sql<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<Row, DriverError>> + 'a {
        self.observed(sql, move |c| c.query_one_sql(sql))
    }

    /// Run a runtime-SQL query returning the first row if any. The compile-checked
    /// typed counterpart is [`query_opt`](Self::query_opt). A
    /// `set_config('statement_timeout', …)` here RE-DERIVES the client-liveness
    /// window (see [`observed`](Self::observed)).
    pub fn query_opt_sql<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<Option<Row>, DriverError>> + 'a {
        self.observed(sql, move |c| c.query_opt_sql(sql))
    }

    /// Stream a runtime raw-SQL query's rows one at a time to `on_row` in CONSTANT
    /// memory — the dynamic (untyped) streaming peer of [`query_sql`](Self::query_sql),
    /// and the PostgreSQL peer of the SQLite driver's `query_each_sql` (so a
    /// dynamic stream reads the SAME on both backends).
    ///
    /// Each row is handed to `on_row` as a zero-copy [`BorrowedRow`] as it arrives,
    /// accumulating NOTHING — a colossal runtime SELECT streams without growing
    /// memory (the escape from eager `query_sql`, which materialises the whole
    /// result). `on_row` returns [`ControlFlow`]: [`Continue`](ControlFlow::Continue)
    /// to keep streaming, or [`Break(e)`](ControlFlow::Break) to stop early. The
    /// borrowed row CANNOT escape the closure (the `for<'r>` bound is the escape
    /// wall) — a value that must outlive it is decoded to an owned type inside.
    ///
    /// # Returns
    ///
    /// - `Ok(None)` — streamed to completion.
    /// - `Ok(Some(e))` — `on_row` broke early; the connection was drained back to a
    ///   clean idle and stays healthy + pooled.
    /// - `Err(DriverError::Decode(..))` — a row body was malformed; the connection
    ///   was drained and stays healthy — LOUD, never swallowed.
    /// - `Err(DriverError::Db(..))` — the server reported an error mid-stream; the
    ///   connection was drained and stays healthy.
    /// - other `Err` — a fatal transport/protocol fault; the connection is dead.
    ///
    /// Reads are POSITIONAL (`row.get::<i32>(0)` / `row.get_str(1)` / …); the
    /// result's column names arrive on the wire only after every row, so by-name
    /// reads live on the eager [`QueryResult::row`] path, not the streaming one.
    /// An oversize row (wider than the inline read buffer) is reassembled into a
    /// reused scratch buffer and streamed exactly like an inline one — constant
    /// memory, no size cap.
    ///
    /// # Early-abort cost
    ///
    /// A [`Break`](ControlFlow::Break) of a colossal result still READS (and
    /// discards) the remaining rows to reach the clean idle boundary that makes the
    /// connection reusable — O(remaining rows).
    pub fn query_each_sql<'a, F, E>(
        &'a mut self,
        sql: &'a str,
        on_row: F,
    ) -> impl Future<Output = Result<Option<E>, DriverError>> + 'a
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E> + 'a,
        E: 'a,
    {
        self.observed(sql, move |c| c.query_each_sql(sql, on_row))
    }

    /// Stream a runtime parameterised query's rows one at a time to `on_row` in
    /// CONSTANT memory — the dynamic streaming peer of
    /// [`query_params`](Self::query_params), and the PostgreSQL peer of the SQLite
    /// driver's `query_each_params`. See [`query_each_sql`](Self::query_each_sql)
    /// for the full contract; the params are borrowed all the way to the engine.
    pub fn query_each_params<'a, P: ParamsWriter, F, E>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
        on_row: F,
    ) -> impl Future<Output = Result<Option<E>, DriverError>> + 'a
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E> + 'a,
        E: 'a,
    {
        self.observed(sql, move |c| c.query_each_params(sql, params, on_row))
    }

    /// Prepare a statement: `Parse` + `Describe` + `Sync`, recovering the result
    /// schema for later `Bind`+`Execute`.
    ///
    /// Deliberately NOT routed through [`observed`](Self::observed): `Parse` does not
    /// EXECUTE the statement, so a prepared `SET statement_timeout = …` changes the
    /// server budget by NOTHING — re-deriving the window from it would tighten the
    /// window below the actual budget (a false cut). A `set_config` smuggled into a
    /// prepared plan and run via [`query_prepared`](Self::query_prepared) is the
    /// documented theoretical floor; the SQL-level `PREPARE … AS … set_config` +
    /// `EXECUTE` path stays fail-safe because both ride the observed
    /// [`execute_sql`](Self::execute_sql).
    pub fn prepare<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<PreparedStatement, DriverError>> + 'a {
        self.core.prepare(sql)
    }

    /// Execute a prepared statement returning rows. The params are borrowed all
    /// the way to the engine, so a non-`Copy` owned param binds by reference.
    pub fn query_prepared<'a, P: ParamsWriter>(
        &'a mut self,
        stmt: &'a PreparedStatement,
        params: &'a P,
    ) -> impl Future<Output = Result<QueryResult, DriverError>> + 'a {
        self.core.query_prepared(stmt, params)
    }

    /// Execute a prepared statement for its side effect, returning the affected
    /// count.
    pub fn execute_prepared<'a, P: ParamsWriter>(
        &'a mut self,
        stmt: &'a PreparedStatement,
        params: &'a P,
    ) -> impl Future<Output = Result<u64, DriverError>> + 'a {
        self.core.execute_prepared(stmt, params)
    }

    /// Prepare, query, and close a runtime SQL statement with params. A
    /// `set_config('statement_timeout', …)` in the SQL text RE-DERIVES the
    /// client-liveness window (see [`observed`](Self::observed)).
    pub fn query_params<'a, P: ParamsWriter>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
    ) -> impl Future<Output = Result<QueryResult, DriverError>> + 'a {
        self.observed(sql, move |c| c.query_params(sql, params))
    }

    /// Like [`query_params`](Self::query_params), returning the first row.
    pub fn query_params_one<'a, P: ParamsWriter>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
    ) -> impl Future<Output = Result<Row, DriverError>> + 'a {
        self.observed(sql, move |c| c.query_params_one(sql, params))
    }

    /// Like [`query_params`](Self::query_params), returning the first row if any.
    pub fn query_params_opt<'a, P: ParamsWriter>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
    ) -> impl Future<Output = Result<Option<Row>, DriverError>> + 'a {
        self.observed(sql, move |c| c.query_params_opt(sql, params))
    }

    /// Prepare, execute, and close a runtime SQL statement with params. A
    /// `set_config('statement_timeout', …)` in the SQL text RE-DERIVES the
    /// client-liveness window (see [`observed`](Self::observed)).
    pub fn execute_params<'a, P: ParamsWriter>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
    ) -> impl Future<Output = Result<u64, DriverError>> + 'a {
        self.observed(sql, move |c| c.execute_params(sql, params))
    }

    /// Close a prepared statement, consuming it (use-after-close is a move error).
    pub fn close_statement(
        &mut self,
        stmt: PreparedStatement,
    ) -> impl Future<Output = Result<(), DriverError>> + '_ {
        self.core.close_statement(stmt)
    }

    // ── Compile-checked typed verbs (the `query!` flagship) ─────────────────
    //
    // Each is a `fn -> impl Future` that FORWARDS the shared `Core` verb's future
    // directly (no wrapping async block — so `clippy::manual_async_fn` never fires
    // and there is no extra state-machine layer). Under `n1-detect` the wrapper is
    // `#[track_caller]` and captures the USER's call site synchronously, passing it
    // to the `Core` verb; when the feature is off the caller argument is
    // cfg-removed and the wrapper carries no `#[track_caller]` ABI cost. The bare
    // RPIT return LEAKS the concrete future's `Send` (the lib.rs static assertion
    // pins it), so no explicit `+ Send` is added and no `!Send` `on_row` closure is
    // constrained.

    /// Execute a compile-checked `query!` for its side effect, returning the
    /// affected-row count (binary-uniform params). Parses the content-addressed
    /// statement once per connection, then reuses the server-side plan. The
    /// runtime-SQL escape hatch is [`execute_sql`](Self::execute_sql).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn execute<'a, P, R>(
        &'a mut self,
        q: &'static PreparedQuery<P, R>,
        params: P,
    ) -> impl core::future::Future<Output = Result<u64, DriverError>> + 'a
    where
        P: ParamsWriter + 'static,
        R: RowDecode + 'static,
    {
        self.core.execute(
            q,
            params,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )
    }

    /// Run a compile-checked `query!` and collect its TYPED rows — the flagship
    /// parameterised query.
    ///
    /// `Q` is a `query!`-generated carrier; the returned [`Rows<Q>`] decodes
    /// lazily into the macro's typed records — borrowed (zero-copy text) via
    /// [`Rows::iter`], or owned via [`Rows::into_owned`]. SQL is validated against
    /// the schema at build time, params are bound in binary. An oversize row
    /// (wider than the engine's inline read buffer) is reassembled into the
    /// prebuffer and decodes identically to an inline one — no size cap. The
    /// statement is Parsed once per connection and the server-side plan reused
    /// thereafter (safe across pool checkouts and transactions). The runtime-SQL
    /// escape hatch is [`query_sql`](Self::query_sql).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query<'a, Q: TypedQuery + 'a>(
        &'a mut self,
        params: Q::Params<'a>,
    ) -> impl core::future::Future<Output = Result<Rows<Q>, DriverError>> + 'a
    where
        Q::Params<'a>: 'a,
    {
        self.core.query::<Q>(
            params,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )
    }

    /// Run a compile-checked `query!` expecting EXACTLY one row, returning the
    /// owned record. Zero rows is [`DriverError::NoRows`]; more than one is
    /// [`DriverError::TooManyRows`] (loud, never a silently-taken first row).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_one<'a, Q: TypedQuery + 'a>(
        &'a mut self,
        params: Q::Params<'a>,
    ) -> impl core::future::Future<Output = Result<Q::Owned, DriverError>> + 'a
    where
        Q::Params<'a>: 'a,
    {
        self.core.query_one::<Q>(
            params,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )
    }

    /// Run a compile-checked `query!` expecting AT MOST one row, returning the
    /// owned record if present or `None` if absent — the by-key maybe-absent shape.
    /// Zero rows is `Ok(None)`; more than one is [`DriverError::TooManyRows`]
    /// (loud, never a silently-taken first row). The zero-or-one peer of
    /// [`query_one`](Self::query_one); the runtime-SQL escape hatch is
    /// [`query_opt_sql`](Self::query_opt_sql).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_opt<'a, Q: TypedQuery + 'a>(
        &'a mut self,
        params: Q::Params<'a>,
    ) -> impl core::future::Future<Output = Result<Option<Q::Owned>, DriverError>> + 'a
    where
        Q::Params<'a>: 'a,
    {
        self.core.query_opt::<Q>(
            params,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )
    }

    /// Stream a compile-checked `query!`'s rows one at a time to `on_row` in
    /// CONSTANT memory — the streaming peer of [`query`](Self::query).
    ///
    /// Each `DataRow` decodes as it arrives (borrowed, zero-copy) and is handed to
    /// `on_row`, accumulating NOTHING — a colossal result streams without growing
    /// memory. `on_row` returns [`ControlFlow`]: [`Continue`](ControlFlow::Continue)
    /// to keep streaming, or [`Break(e)`](ControlFlow::Break) to stop early. The
    /// borrowed record CANNOT escape the closure (the `for<'q>` bound is the
    /// escape wall).
    ///
    /// # Returns
    ///
    /// - `Ok(None)` — streamed to completion.
    /// - `Ok(Some(e))` — `on_row` broke early; the connection was drained back to a
    ///   clean idle and stays healthy.
    /// - `Err(DriverError::Decode(..))` — a row failed to decode into its
    ///   compile-time shape; the connection was drained and stays healthy — LOUD,
    ///   never swallowed nor defaulted.
    /// - `Err(DriverError::Db(..))` — the server reported an error mid-stream; the
    ///   connection was drained and stays healthy.
    /// - other `Err` — a fatal transport/protocol fault; the connection is dead.
    ///
    /// An oversize row (wider than the inline read buffer) is reassembled into a
    /// reused scratch buffer and streamed to `on_row` exactly like an inline one
    /// — constant memory (bounded by the widest oversize row), no size cap.
    ///
    /// # Early-abort cost
    ///
    /// A [`Break`](ControlFlow::Break) of a colossal result still READS (and
    /// discards) the remaining rows to reach the clean idle boundary that makes
    /// the connection reusable — O(remaining rows).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_each<'a, Q, F, E>(
        &'a mut self,
        params: Q::Params<'a>,
        on_row: F,
    ) -> impl core::future::Future<Output = Result<Option<E>, DriverError>> + 'a
    where
        Q: TypedQuery + 'a,
        Q::Params<'a>: 'a,
        E: 'a,
        F: for<'q> FnMut(Q::Record<'q>) -> ControlFlow<E> + 'a,
    {
        self.core.query_each::<Q, F, E>(
            params,
            on_row,
            #[cfg(feature = "n1-detect")]
            core::panic::Location::caller(),
        )
    }

    /// Run a HETEROGENEOUS ATOMIC pipeline — N compile-checked `query!` commands
    /// (each a [`Bound`](bsql_postgres_core::Bound) carrier +
    /// params) sent in ONE round trip as ONE implicit transaction, returning a
    /// tuple of one [`Rows<Qi>`] per command.
    ///
    /// ```ignore
    /// let (users, logs, count) = conn.pipeline((
    ///     UserById::bind((7,)),
    ///     RecentLogs::bind((&since,)),
    ///     CountAll::bind(()),
    /// )).await?;
    /// ```
    ///
    /// # Airtight all-or-nothing
    ///
    /// The whole batch commits and returns every result, or it errors and returns
    /// ZERO — a mid-batch failure ROLLS BACK the commands before it (they form one
    /// implicit transaction), so the results before a failure are NEVER returned. A
    /// failure is [`DriverError::BatchFailed`] naming the failing command's
    /// zero-based index ([`DriverError::batch_failed_index`]). The connection is left
    /// EXACTLY as any failed verb leaves it — a common implicit-tx batch is
    /// immediately clean; a batch inside an explicit transaction leaves it aborted
    /// (`'E'`) for its owner (a guard, a pooled reset, or the caller) to roll back,
    /// so the next in-transaction verb fails loudly (`25P02`), never a silent
    /// autocommit. Saves `N - 1` round trips versus `N` sequential `query` calls. See
    /// [`Core::pipeline`](bsql_postgres_core::Core::pipeline) for the full contract.
    pub fn pipeline<'a, B>(
        &'a mut self,
        batch: B,
    ) -> impl core::future::Future<Output = Result<B::Output, DriverError>> + 'a
    where
        B: Pipeline<'a> + 'a,
    {
        self.core.pipeline(batch)
    }

    /// Run a HOMOGENEOUS ATOMIC bulk write — ONE compile-checked `query!` write
    /// carrier `Q` (an `UPDATE`/`DELETE`/`INSERT`) against N runtime parameter sets,
    /// `Parse`d ONCE and re-bound per set, in ~ONE round trip, returning each
    /// command's affected-row count. The batch peer of [`execute`](Self::execute) and
    /// the homogeneous sibling of [`pipeline`](Self::pipeline).
    ///
    /// # Airtight all-or-nothing
    ///
    /// The N commands ride ONE trailing `Sync` (a single implicit transaction): the
    /// whole batch commits and returns every count, or it errors and returns ZERO. A
    /// mid-batch failure is [`DriverError::BatchFailed`] naming the failing command's
    /// zero-based index; a COMMIT-TIME failure (a deferred constraint) is
    /// [`DriverError::Db`] with [`batch_failed_index`](DriverError::batch_failed_index)
    /// `None`. Like every verb it does NOT auto-rollback — a failure inside an
    /// explicit transaction leaves it aborted (`'E'`) for its owner, so a next
    /// in-guard verb fails loudly (`25P02`), never a silent autocommit. Constant send
    /// memory regardless of N (the windowed batcher). `N == 0` does no wire I/O; `N ==
    /// 1` equals a single [`execute`](Self::execute). See
    /// [`Core::execute_batch`](bsql_postgres_core::Core::execute_batch) for the full
    /// contract.
    pub fn execute_batch<'a, Q, I>(
        &'a mut self,
        params: I,
    ) -> impl core::future::Future<Output = Result<Vec<u64>, DriverError>> + 'a
    where
        Q: TypedQuery + 'a,
        I: IntoIterator<Item = Q::Params<'a>> + 'a,
    {
        self.core.execute_batch::<Q, I>(params)
    }

    // ── Transaction / session boundary primitives ───────────────────────────

    /// Apply every pending migration from `source` to the database, exactly
    /// once, in deterministic order — the runtime migration RUNNER. See
    /// [`bsql_postgres_core::migrate`] for the ledger / atomicity /
    /// checksum-drift / advisory-lock guarantees.
    ///
    /// The client-liveness window is SUPPRESSED for the whole run: a migration is
    /// a TRUSTED bsql-controlled long operation (a `CREATE INDEX CONCURRENTLY`
    /// behind a `SET statement_timeout = 0`), governed by its own server-side
    /// budget, so bsql must never client-cut its own migration. The suppression is
    /// restored on return OR a dropped future (RAII).
    pub async fn run_migrations<'a>(
        &'a mut self,
        source: impl Into<MigrationSource<'a>>,
    ) -> Result<MigrationReport, MigrationError> {
        use bsql_postgres_core::migrate;
        // Suppress the client-liveness window for the trusted long op. A cloned
        // `Arc` so the guard borrows a local, leaving `self.core` free below.
        let deadline = std::sync::Arc::clone(&self.read_deadline);
        let _window_suppressed = deadline.suppress_scoped();
        let source = source.into();
        // Acquire the migration lock by NON-BLOCKING poll with backoff: a waiter
        // holds no long-lived transaction, so a `CREATE INDEX CONCURRENTLY`
        // migration in the lock-holder cannot deadlock against a waiter's vxid.
        let start = std::time::Instant::now();
        let mut backoff = migrate::LOCK_POLL_INITIAL;
        loop {
            if self.core.try_acquire_migration_lock().await.map_err(MigrationError::from)? {
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
            tokio::time::sleep(backoff).await;
            backoff = migrate::next_backoff(backoff);
        }
        // Lock held — apply, then ALWAYS release (best effort: on a healthy
        // connection this succeeds; a dead one auto-releases at session end).
        let result = self.core.apply_pending_locked(source).await;
        match self.core.release_migration_lock().await {
            Ok(()) | Err(_) => {}
        }
        result
    }

    /// A read-only snapshot of applied vs pending migrations (no lock, no
    /// apply).
    pub fn migration_status<'a>(
        &'a mut self,
        source: impl Into<MigrationSource<'a>>,
    ) -> impl Future<Output = Result<MigrationStatus, MigrationError>> + 'a {
        self.core.migration_status(source.into())
    }

    /// Report which migrations WOULD be applied (running the same drift checks
    /// as [`run_migrations`](Self::run_migrations)) without applying anything.
    pub fn dry_run_migrations<'a>(
        &'a mut self,
        source: impl Into<MigrationSource<'a>>,
    ) -> impl Future<Output = Result<Vec<String>, MigrationError>> + 'a {
        self.core.dry_run_migrations(source.into())
    }

    /// `BEGIN` a transaction.
    pub fn begin(&mut self) -> impl Future<Output = Result<(), DriverError>> + '_ {
        self.core.begin()
    }

    /// `COMMIT` the current transaction.
    pub fn commit(&mut self) -> impl Future<Output = Result<(), DriverError>> + '_ {
        self.core.commit()
    }

    /// `ROLLBACK` the current transaction.
    pub fn rollback(&mut self) -> impl Future<Output = Result<(), DriverError>> + '_ {
        self.core.rollback()
    }

    /// Run `f` inside a transaction: `COMMIT` on `Ok`, best-effort `ROLLBACK` on
    /// `Err`, KEEPING the connection on a recoverable error.
    ///
    /// `f` is an async closure handed a borrowing [`Transaction`] guard — NOT the
    /// whole `Connection`. The guard exposes ONLY the data verbs (query / execute /
    /// the typed `query!` verbs), so `tx.begin()` / `tx.commit()` / `tx.rollback()`
    /// / a nested `tx.transaction(..)` / `tx.close()` inside the body do not exist
    /// (a method-not-found compile error, E0599). This makes transaction atomicity
    /// a COMPILE-TIME guarantee: the classic footgun where a body hand-drives the
    /// transaction lifecycle — or a composed helper opens its own inner
    /// `transaction` (PostgreSQL has no nested transactions, so the inner `COMMIT`
    /// would silently flatten the outer's atomic scope) — is now impossible by
    /// construction, the same tier as [`copy_in_with`](Self::copy_in_with)'s
    /// exclusive-borrow writer. The guard holds no object to leak — its boundary is
    /// the call scope. There is no `Drop`-based async guard (`Drop` cannot
    /// `.await`): the async closure form is the cancellation-correct shape — if the
    /// returned future is dropped mid-body, no `COMMIT` runs and the server rolls
    /// back when the socket later closes.
    ///
    /// The `BEGIN` is DEFERRED and PIPELINED with the first statement the body
    /// issues: it rides that statement's flush (one round trip carries both), so a
    /// one-statement transaction costs the pipelined round trips, not a separate
    /// `BEGIN` round trip plus the statement's. The `BEGIN` is armed INSIDE that
    /// first verb (within the `take_live` window it opens), never out-of-band at
    /// entry — so if the returned future is dropped BEFORE any verb runs (its
    /// first suspending await is non-bsql — a `sleep`, a lock, an external fetch),
    /// NOTHING is staged: the connection is left clean, never carrying a stranded
    /// `BEGIN` a later verb on a reused (bare) connection would silently fuse. An
    /// EMPTY body (no statement) is therefore a true no-op: it opens nothing and
    /// costs zero round trips — no `COMMIT` / `ROLLBACK` is issued. A fused `BEGIN`
    /// that errors surfaces as the transaction's failure (it cannot be swallowed by
    /// the first statement).
    pub async fn transaction<R, F>(&mut self, f: F) -> Result<R, DriverError>
    where
        F: AsyncFnOnce(&mut Transaction<'_>) -> Result<R, DriverError>,
    {
        // The BEGIN is NOT armed out-of-band here. The guard arms it inside the
        // first verb (poll-time, within that verb's take_live window), so a
        // transaction future dropped before any verb runs leaves nothing staged.
        // The guard borrows `self.core` for the body's scope ONLY; the block ends
        // that borrow (and reads back whether a verb opened the transaction) so the
        // terminating COMMIT/ROLLBACK below can re-borrow `self.core`.
        let (outcome, opened) = {
            let mut tx = Transaction {
                core: &mut self.core,
                read_deadline: &self.read_deadline,
                begin_armed: false,
            };
            let outcome = f(&mut tx).await;
            (outcome, tx.begin_armed)
        };
        // Terminate ONLY if a verb actually opened the transaction. An empty body
        // armed no BEGIN, so there is nothing to commit or roll back — and the
        // terminator can never carry a fused BEGIN into the next verb.
        let result = match outcome {
            Ok(value) => {
                if opened {
                    self.core.simple_query("COMMIT").await?;
                }
                Ok(value)
            }
            Err(e) => {
                // Best-effort rollback; the outcome rides the liveness token, so it
                // is explicitly discarded. The caller's error `e` dominates.
                if opened {
                    drop(self.core.simple_query("ROLLBACK").await);
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
    pub fn listen<'a>(
        &'a mut self,
        channel: &'a str,
    ) -> impl Future<Output = Result<(), DriverError>> + 'a {
        self.core.listen(channel)
    }

    /// Unsubscribe from a `LISTEN` channel (validated as [`listen`](Self::listen)).
    pub fn unlisten<'a>(
        &'a mut self,
        channel: &'a str,
    ) -> impl Future<Output = Result<(), DriverError>> + 'a {
        self.core.unlisten(channel)
    }

    /// Reset all BLEEDABLE session state so this connection can be safely reused
    /// by a different logical user, WITHOUT dropping prepared statements.
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
    /// (~15 min). This reset therefore arms an absolute read deadline — the SAME
    /// [`ReadDeadline`](crate::transport) primitive
    /// [`recv_notification`](Self::recv_notification) uses, proven token-safe —
    /// bounded by the connection's own `connect_timeout` (a reset is a mini
    /// handshake, so it earns the handshake's wall-clock budget; no separate knob,
    /// so the [`ConnectConfig`](bsql_postgres_core::ConnectConfig) footprint is
    /// untouched). On a vanished peer the read ELAPSES into a fatal transport
    /// error — the reset's pump has no would-block quiet arm (that arm is unique to
    /// the notification wait), so an elapsed deadline is NOT a quiet alive outcome
    /// here but a dead-connection one — so the token is dropped, this returns
    /// classified, and a pool EVICTS the connection and hands out a fresh one (or a
    /// classified acquire-timeout if the whole budget is spent) instead of hanging.
    /// A healthy reset completes in microseconds, far inside the budget, so the
    /// deadline never fires on the happy path (no added round trip, only an
    /// arm/disarm atomic store bracketing the existing round trip).
    ///
    /// # Errors
    ///
    /// Any transport / server error is returned classified; a pool evicts a
    /// connection whose reset failed rather than handing out a still-dirty one.
    /// [`DriverError::TimeoutOverflow`] if `connect_timeout` is so large that the
    /// absolute deadline overflows the clock (the token is untaken, connection
    /// still alive).
    pub async fn reset_session(&mut self) -> Result<(), DriverError> {
        self.reset_session_bounded().await
    }

    /// The POOL-checkout reset: behaviourally IDENTICAL to
    /// [`reset_session`](Self::reset_session) (both drop the dynamic
    /// prepared-statement cache, so a pooled connection behaves exactly like a fresh
    /// one for the next logical user). A distinct `pub(crate)` entry so the pool has
    /// a named checkout hook; bounded identically.
    pub(crate) async fn pool_reset_session(&mut self) -> Result<(), DriverError> {
        self.reset_session_bounded().await
    }

    /// The shared, bounded reset wrapper: arms the connect-budget liveness deadline
    /// around the reset (so a vanished pooled peer ELAPSES into a classified error,
    /// never a `tcp_retries2` hang) and restores the client-liveness window to the
    /// connect baseline afterward.
    async fn reset_session_bounded(&mut self) -> Result<(), DriverError> {
        // Bound the WHOLE reset sequence (the RESET simple-query plus the batched
        // dynamic-statement Close) under ONE absolute deadline = the connection's
        // connect budget. Compute it with `checked_add` BEFORE arming / taking the
        // token, so an overflow returns Err with the connection untouched.
        let budget = Duration::from_secs(self.redial.connect_timeout_secs());
        let deadline = Instant::now()
            .checked_add(budget)
            .ok_or(DriverError::TimeoutOverflow)?;
        // The RAII guard disarms on drop, so the deadline cannot survive this
        // future being dropped mid-`.await` (an outer timeout / cancelled task) —
        // a structural guarantee, not caller discipline. On the normal path the
        // explicit `drop(guard)` performs the same single atomic-store disarm a
        // manual one would, before the caller's real verbs read deadline-free.
        let guard = self.read_deadline.arm_scoped(deadline);
        let result = self.core.reset_session().await;
        drop(guard);
        // The reset ran `RESET ALL`, restoring `statement_timeout` to its
        // connect-time value — so the client-liveness window returns to the
        // connect baseline. A runtime `SET` made on THIS checkout cannot leak its
        // (possibly larger/disarmed) window to the next checkout of a pooled
        // connection.
        self.read_deadline
            .set_steady_window(self.read_deadline.connect_window());
        result
    }

    // ── Notifications (deadline arming is async-specific; stays here) ────────

    /// Wait up to `timeout` for the next asynchronous notification.
    ///
    /// Drains the per-connection notification ledger FIRST (a notification that
    /// already arrived returns immediately with NO round trip). Only when the
    /// ledger is empty does this wait on the socket. Returns `Ok(None)` if the
    /// deadline passes with no notification (the connection stays alive). The wait
    /// is bounded by arming an absolute read deadline on the socket the engine
    /// owns (shared via the connection's read-deadline cell); a deadline elapsed
    /// mid-read surfaces inside the engine as the quiet outcome — the token rides
    /// back in `Ok`. The deadline lives in the read, NOT in a `timeout` wrapping
    /// this future, so a timed-out wait never drops the verb future and strands the
    /// linear token.
    ///
    /// # Errors
    ///
    /// A malformed or non-UTF-8 buffered notification surfaces here as a classified
    /// [`DriverError`] (it is still removed from the ledger) — never a silent drop.
    pub async fn recv_notification(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<Notification>, DriverError> {
        // An already-arrived notification returns without touching the socket.
        if let Some(buffered) = self.core.drain_one_notification() {
            return buffered.map(Some);
        }
        // Validate the fallible input (a near-MAX timeout would overflow
        // `Instant + Duration`) BEFORE the shared inner verb takes the token — so a
        // `TimeoutOverflow` returns Err with the connection still alive.
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(DriverError::TimeoutOverflow)?;
        // The RAII guard disarms on drop, so a stale deadline cannot survive this
        // future being dropped mid-`.await` (an outer `tokio::time::timeout` /
        // `select!` losing the race) and fire a spurious `TimedOut` on the reused
        // connection's next verb — a structural guarantee, not caller discipline.
        let guard = self.read_deadline.arm_scoped(deadline);
        let received = self.core.recv_notification_inner().await;
        // Disarm before draining, so a later verb's reads are deadline-free. The
        // explicit `drop(guard)` is the same single infallible atomic store the
        // manual `disarm` was (no restore error to thread, unlike the blocking
        // driver's socket-timeout restore).
        drop(guard);
        if received? {
            self.core.take_expected_notification()
        } else {
            Ok(None)
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

    /// The number of asynchronous notifications SHED because the bounded ledger
    /// was full — monotonic. Non-zero means notifications were lost to the bound;
    /// the loss is LOUD (visible here), never a silent drop.
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
    pub async fn recv_notification_as<T: FromStr>(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<TypedNotification<T>>, DriverError> {
        match self.recv_notification(timeout).await? {
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

    // ── COPY (async-closure `copy_in_with` stays here; `copy_out` delegates) ─

    /// `COPY <table> FROM STDIN`, bulk-loading `rows_data` in CONSTANT memory —
    /// the ergonomic batch form of [`copy_in_with`](Self::copy_in_with).
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
    pub async fn copy_in(
        &mut self,
        table: &str,
        rows_data: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<u64, DriverError> {
        self.copy_in_with(table, async |w| {
            for row in rows_data {
                w.write_row(row.as_ref().as_bytes()).await?;
            }
            Ok(())
        })
        .await
    }

    /// `COPY <table> FROM STDIN` with a scoped streaming writer: run `f` against a
    /// [`CopyInWriter`], then finish (`CopyDone`) if it returns `Ok` or abort
    /// (`CopyFail`) if it returns `Err`. The CONSTANT-MEMORY, cancellation-safe
    /// bulk-load primitive.
    ///
    /// `f` may interleave arbitrary async work between rows, and `write_chunk` /
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
    /// - the returned future is dropped mid-`f` → no terminal frame is sent; the
    ///   connection is left dead and the server tears the COPY down when the socket
    ///   later closes. Use `Err` to abort recoverably.
    ///
    /// `table` is validated as an identifier (see [`copy_in`](Self::copy_in)).
    ///
    /// # Errors
    ///
    /// A server rejection at `CopyDone` is a recoverable [`DriverError::Db`]; `f`'s
    /// own error is returned verbatim; a transport fault is fatal.
    pub async fn copy_in_with<F>(&mut self, table: &str, f: F) -> Result<u64, DriverError>
    where
        F: AsyncFnOnce(&mut CopyInWriter<'_>) -> Result<(), DriverError>,
    {
        // The table splice is validated ONCE, in `Core::copy_in_begin_table`
        // (the single COPY-in splice site shared by both drivers): an
        // injection-shaped table is a classified `DriverError::Config` there,
        // never assembled into SQL. On a fault the token is dropped by `Core` —
        // the connection is dead.
        let live = self.core.copy_in_begin_table(table).await?;
        let body = {
            let mut writer = CopyInWriter {
                core: &mut self.core,
                scratch: Vec::new(),
            };
            f(&mut writer).await
            // `writer` is dropped here, releasing the `&mut self.core` borrow so
            // the terminal step below can re-borrow it.
        };
        match body {
            // `copy_in_finish` restores the token on either status and maps a
            // server rejection to `DriverError::Db` with the connection kept pooled.
            Ok(()) => self.core.copy_in_finish(live).await,
            Err(e) => {
                // The caller abandoned the copy: `CopyFail` reclaims the connection
                // (the abort's `ServerErrored` is expected, not a fault); a
                // transport fault leaves it dead. The caller's `e` dominates either
                // way.
                self.core.copy_in_abort(live).await;
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
    /// `bytea` column borrows the caller's data (`&'q str` / `&'q [u8]`). The
    /// target table, column list, and per-column types were pinned at COMPILE
    /// time by `copy!` against the migration catalog, so a wrong-typed or
    /// wrong-arity row is a compile error — and there is no COPY text to
    /// mis-escape (an embedded tab / newline / quote rides the binary field
    /// verbatim), the footgun the raw [`copy_in`](Self::copy_in) leaves the
    /// caller. Rows are NOT pre-collected; a megarow load streams in bounded
    /// memory through the 64 KiB batcher.
    ///
    /// # Errors
    ///
    /// A row the server rejects at `CopyDone` (a constraint / type violation) is a
    /// classified [`DriverError::Db`], and the connection RECOVERS to a clean idle
    /// (it stays pooled). A transport fault is fatal.
    pub async fn copy_in_typed<'q, Q, I>(&mut self, rows: I) -> Result<u64, DriverError>
    where
        Q: TypedCopyIn,
        I: IntoIterator<Item = Q::Row<'q>>,
    {
        self.core.copy_in_typed::<Q, I>(rows).await
    }

    /// `COPY <table> TO STDOUT`, streaming each row to `on_chunk` in CONSTANT
    /// memory — the bulk-unload peer of [`copy_in`](Self::copy_in).
    ///
    /// Each server `CopyData` frame is handed to `on_chunk` as a borrowed slice
    /// into the transient ingest buffer; nothing is accumulated. The borrowed
    /// chunk CANNOT escape the closure (the `for<'q>` bound is the escape wall).
    /// `on_chunk` returns [`ControlFlow`]. `table` is validated as an identifier.
    ///
    /// # Returns
    ///
    /// - `Ok(None)` — streamed to completion; clean and pooled.
    /// - `Ok(Some(e))` — `on_chunk` broke early; drained back to idle, stays healthy.
    /// - `Err(DriverError::Db(..))` — a server error mid-unload; drained, stays healthy.
    /// - other `Err` — a fatal transport/protocol fault; the connection is dead.
    ///
    /// A [`Break`](ControlFlow::Break) still reads the remaining `CopyData` to
    /// reach the clean idle — O(remaining rows).
    pub fn copy_out<'a, F, E>(
        &'a mut self,
        table: &'a str,
        on_chunk: F,
    ) -> impl Future<Output = Result<Option<E>, DriverError>> + 'a
    where
        F: for<'q> FnMut(&'q [u8]) -> ControlFlow<E> + 'a,
        E: 'a,
    {
        self.core.copy_out(table, on_chunk)
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
    /// # fn check(conn: &bsql_postgres_async::Connection) -> Result<(), &'static str> {
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
    pub fn close(&mut self) -> impl Future<Output = Result<(), DriverError>> + '_ {
        self.core.close()
    }

    /// BEST-EFFORT graceful close for a pooled connection the pool is DISCARDING
    /// (a [`Pool::close`](crate::Pool::close) drain, or a `max_lifetime` /
    /// `idle_timeout` reap): send a protocol `Terminate` so the server sees a
    /// CLEAN disconnect — not the "unexpected EOF on client connection" its log
    /// records for a bare socket drop (an RST/FIN with no `Terminate`) — then let
    /// the socket close when `self` drops.
    ///
    /// BOUNDED by the connection's own `connect_timeout`: the `Terminate` write
    /// into a full send buffer on a black-hole peer (a half-open socket) would
    /// otherwise block for the kernel's `tcp_retries2` budget (~15 min). Wrapping
    /// the whole close in an outer [`tokio::time::timeout`] is SAFE here — unlike
    /// [`reset_session`](Self::reset_session) / [`recv_notification`](Self::recv_notification),
    /// where dropping the verb future would strand the linear liveness token —
    /// precisely BECAUSE the connection is being discarded: `close` consumes the
    /// token (`live.take()`) and `self` is dropped next, so a dropped-mid-flight
    /// close future strands nothing. No `ConnectConfig` knob is added — the budget
    /// is the existing `connect_timeout`.
    ///
    /// Best-effort: any outcome — a completed `Terminate`, a server/transport
    /// error, or an elapsed budget — is DISCARDED (the socket closes on drop
    /// regardless), so the pool's drain continues past a single dead peer.
    pub(crate) async fn close_graceful(&mut self) {
        let budget = Duration::from_secs(self.redial.connect_timeout_secs());
        // Discard the nested outcome (timeout OR verb error): the graceful close
        // is fire-and-forget and there is no token to strand.
        drop(tokio::time::timeout(budget, self.close()).await);
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
/// it needs the driver's connection-level read-deadline state the guard does not
/// hold, and a backend cannot receive notifications mid-transaction anyway.
///
/// Borrows the connection's [`Core`] for the closure scope, so it holds no
/// object past the call and adds no `Drop` terminator: cancellation correctness
/// is exactly as [`transaction`](Connection::transaction) documents. Every verb
/// forwards DIRECTLY to the same shared `Core` verb the [`Connection`] method
/// calls — the guard adds no state-machine layer, and under `n1-detect` it
/// records the USER's call site (not a guard-internal line) via `#[track_caller]`.
///
/// No `Debug`: it borrows the connection's engine (a live socket / TLS session),
/// which is not `Debug` — the same reason [`CopyInWriter`] carries none.
pub struct Transaction<'t> {
    core: &'t mut Core<TokioSocket>,
    /// The connection's shared client-liveness window cell, so a `SET`/`RESET`/
    /// `set_config` of `statement_timeout` issued INSIDE the transaction re-derives
    /// the window through the SAME authority the connection-level verbs use (closes
    /// the tx-guard observation gap — a transaction is a common place to bound a
    /// long operation via `SET statement_timeout`). Borrowed for the body's scope;
    /// the guard adds no owned state and no `Drop`.
    read_deadline: &'t Arc<ReadDeadline>,
    /// `true` once the deferred `BEGIN` has been armed by the first verb. Armed
    /// exactly once, and ONLY from within a verb's poll (never out-of-band at
    /// `transaction()` entry) — which is what makes a transaction future dropped
    /// before any verb runs leave nothing staged. Read by the combinator after
    /// the body to decide whether a terminating `COMMIT` / `ROLLBACK` is owed.
    begin_armed: bool,
}

impl Transaction<'_> {
    /// Poll-time arm-once wrapper: arm the deferred `BEGIN` INSIDE the returned
    /// future, at the SAME poll the built verb takes the liveness token.
    ///
    /// The first verb the body issues opens the transaction — its flush fuses
    /// the `BEGIN` (the 1-RTT win). Because the arming (`defer_begin`) and the
    /// core verb's `take_live` share ONE synchronous critical section with no
    /// `.await` between them, a transaction future dropped before any verb is
    /// polled arms NOTHING: the strand hazard is unreachable by construction, not
    /// merely unlikely. Every data verb routes through here, so whichever verb
    /// runs first opens the transaction — exactly as the (deleted) out-of-band
    /// arming did, minus the blind zone between `transaction()` entry and the
    /// first verb.
    fn armed<'a, F, Fut>(&'a mut self, make: F) -> impl Future<Output = Fut::Output> + 'a
    where
        F: FnOnce(&'a mut Core<TokioSocket>) -> Fut + 'a,
        Fut: Future + 'a,
    {
        let core = &mut *self.core;
        let armed = &mut self.begin_armed;
        async move {
            if !*armed {
                core.defer_begin();
                *armed = true;
            }
            make(core).await
        }
    }

    /// [`armed`](Self::armed) PLUS the client-liveness-window observation the
    /// connection-level [`Connection::observed`] applies — the SQL-carrying verbs
    /// route through here so a `SET`/`RESET`/`set_config` of `statement_timeout`
    /// issued INSIDE the transaction re-derives the window (never left stale below a
    /// budget the transaction raised: the tx-guard observation gap, closed). Uses
    /// the connection's shared [`ReadDeadline`] cell (the guard holds only a borrow
    /// of it) via the SAME [`ReadDeadline::observe_statement_timeout`](crate::transport)
    /// authority, so a `SET` inside vs outside a transaction re-derives identically.
    /// Observes only on SUCCESS.
    fn observed<'a, T, F, Fut>(
        &'a mut self,
        sql: &'a str,
        make: F,
    ) -> impl Future<Output = Result<T, DriverError>> + 'a
    where
        F: FnOnce(&'a mut Core<TokioSocket>) -> Fut + 'a,
        Fut: Future<Output = Result<T, DriverError>> + 'a,
        T: 'a,
    {
        let core = &mut *self.core;
        let armed = &mut self.begin_armed;
        let read_deadline = self.read_deadline;
        async move {
            if !*armed {
                core.defer_begin();
                *armed = true;
            }
            let result = make(core).await;
            if result.is_ok() {
                read_deadline.observe_statement_timeout(sql);
            }
            result
        }
    }

    // ── Delegated runtime-SQL verbs (data only) ─────────────────────────────
    //
    // The non-SQL `ping` routes through `armed` (poll-time arm-once); the DYNAMIC
    // runtime-SQL verbs route through `observed` (arm-once PLUS window observation),
    // so the first verb the body issues opens the transaction and a `SET
    // statement_timeout` inside the transaction re-derives the window. Every verb
    // otherwise drives the same shared `Core` verb the `Connection` method drives —
    // no extra state layer beyond the window observation.

    /// Round-trip a `Sync` to confirm the connection is live.
    pub fn ping(&mut self) -> impl Future<Output = Result<(), DriverError>> + '_ {
        self.armed(|c| c.ping())
    }

    /// Issue a simple query, returning the command tag string. A `SET`/`RESET`/
    /// `set_config` of `statement_timeout` here re-derives the client-liveness
    /// window (the tx-guard peer of [`Connection::simple_query`]).
    pub fn simple_query<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<String, DriverError>> + 'a {
        self.observed(sql, move |c| c.simple_query(sql))
    }

    /// Execute a non-row runtime-SQL command, returning the affected-row count. A
    /// `SET`/`RESET`/`set_config` of `statement_timeout` here re-derives the
    /// client-liveness window.
    pub fn execute_sql<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<u64, DriverError>> + 'a {
        self.observed(sql, move |c| c.execute_sql(sql))
    }

    /// Run a row-returning runtime-SQL query (text result columns). A `set_config`
    /// of `statement_timeout` here re-derives the client-liveness window.
    pub fn query_sql<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<QueryResult, DriverError>> + 'a {
        self.observed(sql, move |c| c.query_sql(sql))
    }

    /// Run a runtime-SQL query returning the first row, or [`DriverError::NoRows`].
    pub fn query_one_sql<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<Row, DriverError>> + 'a {
        self.observed(sql, move |c| c.query_one_sql(sql))
    }

    /// Run a runtime-SQL query returning the first row if any (typed peer:
    /// [`query_opt`](Self::query_opt)).
    pub fn query_opt_sql<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<Option<Row>, DriverError>> + 'a {
        self.observed(sql, move |c| c.query_opt_sql(sql))
    }

    /// Stream a runtime raw-SQL query's rows in CONSTANT memory, inside the
    /// transaction. See [`Connection::query_each_sql`] for the full contract.
    pub fn query_each_sql<'a, F, E>(
        &'a mut self,
        sql: &'a str,
        on_row: F,
    ) -> impl Future<Output = Result<Option<E>, DriverError>> + 'a
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E> + 'a,
        E: 'a,
    {
        self.observed(sql, move |c| c.query_each_sql(sql, on_row))
    }

    /// Stream a runtime parameterised query's rows in CONSTANT memory, inside the
    /// transaction. See [`Connection::query_each_params`] for the full contract.
    pub fn query_each_params<'a, P: ParamsWriter, F, E>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
        on_row: F,
    ) -> impl Future<Output = Result<Option<E>, DriverError>> + 'a
    where
        F: for<'r> FnMut(BorrowedRow<'r>) -> ControlFlow<E> + 'a,
        E: 'a,
    {
        self.observed(sql, move |c| c.query_each_params(sql, params, on_row))
    }

    /// Prepare a statement: `Parse` + `Describe` + `Sync`.
    pub fn prepare<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<PreparedStatement, DriverError>> + 'a {
        self.armed(move |c| c.prepare(sql))
    }

    /// Execute a prepared statement returning rows.
    pub fn query_prepared<'a, P: ParamsWriter>(
        &'a mut self,
        stmt: &'a PreparedStatement,
        params: &'a P,
    ) -> impl Future<Output = Result<QueryResult, DriverError>> + 'a {
        self.armed(move |c| c.query_prepared(stmt, params))
    }

    /// Execute a prepared statement for its side effect, returning the affected
    /// count.
    pub fn execute_prepared<'a, P: ParamsWriter>(
        &'a mut self,
        stmt: &'a PreparedStatement,
        params: &'a P,
    ) -> impl Future<Output = Result<u64, DriverError>> + 'a {
        self.armed(move |c| c.execute_prepared(stmt, params))
    }

    /// Prepare, query, and close a runtime SQL statement with params.
    pub fn query_params<'a, P: ParamsWriter>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
    ) -> impl Future<Output = Result<QueryResult, DriverError>> + 'a {
        self.observed(sql, move |c| c.query_params(sql, params))
    }

    /// Like [`query_params`](Self::query_params), returning the first row.
    pub fn query_params_one<'a, P: ParamsWriter>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
    ) -> impl Future<Output = Result<Row, DriverError>> + 'a {
        self.observed(sql, move |c| c.query_params_one(sql, params))
    }

    /// Like [`query_params`](Self::query_params), returning the first row if any.
    pub fn query_params_opt<'a, P: ParamsWriter>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
    ) -> impl Future<Output = Result<Option<Row>, DriverError>> + 'a {
        self.observed(sql, move |c| c.query_params_opt(sql, params))
    }

    /// Prepare, execute, and close a runtime SQL statement with params.
    pub fn execute_params<'a, P: ParamsWriter>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
    ) -> impl Future<Output = Result<u64, DriverError>> + 'a {
        self.observed(sql, move |c| c.execute_params(sql, params))
    }

    /// Close a prepared statement, consuming it (use-after-close is a move error).
    pub fn close_statement(
        &mut self,
        stmt: PreparedStatement,
    ) -> impl Future<Output = Result<(), DriverError>> + '_ {
        self.armed(move |c| c.close_statement(stmt))
    }

    // ── Compile-checked typed verbs (the `query!` flagship) ─────────────────
    //
    // Same `fn -> impl Future` + `#[track_caller]` shape as the `Connection`
    // methods, so under `n1-detect` the USER's call site is captured (not the
    // guard's forwarder line) and threaded to the shared `Core` verb.

    /// Execute a compile-checked `query!` for its side effect, returning the
    /// affected-row count (binary-uniform params).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn execute<'a, P, R>(
        &'a mut self,
        q: &'static PreparedQuery<P, R>,
        params: P,
    ) -> impl Future<Output = Result<u64, DriverError>> + 'a
    where
        P: ParamsWriter + 'static,
        R: RowDecode + 'static,
    {
        #[cfg(feature = "n1-detect")]
        let loc = core::panic::Location::caller();
        self.armed(move |c| {
            c.execute(
                q,
                params,
                #[cfg(feature = "n1-detect")]
                loc,
            )
        })
    }

    /// Run a compile-checked `query!` and collect its TYPED rows — the flagship
    /// parameterised query.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query<'a, Q: TypedQuery + 'a>(
        &'a mut self,
        params: Q::Params<'a>,
    ) -> impl Future<Output = Result<Rows<Q>, DriverError>> + 'a
    where
        Q::Params<'a>: 'a,
    {
        #[cfg(feature = "n1-detect")]
        let loc = core::panic::Location::caller();
        self.armed(move |c| {
            c.query::<Q>(
                params,
                #[cfg(feature = "n1-detect")]
                loc,
            )
        })
    }

    /// Run a compile-checked `query!` expecting EXACTLY one row, returning the
    /// owned record. Zero rows is [`DriverError::NoRows`]; more than one is
    /// [`DriverError::TooManyRows`].
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_one<'a, Q: TypedQuery + 'a>(
        &'a mut self,
        params: Q::Params<'a>,
    ) -> impl Future<Output = Result<Q::Owned, DriverError>> + 'a
    where
        Q::Params<'a>: 'a,
    {
        #[cfg(feature = "n1-detect")]
        let loc = core::panic::Location::caller();
        self.armed(move |c| {
            c.query_one::<Q>(
                params,
                #[cfg(feature = "n1-detect")]
                loc,
            )
        })
    }

    /// Run a compile-checked `query!` expecting AT MOST one row, returning the
    /// owned record if present or `None` if absent. Zero rows is `Ok(None)`; more
    /// than one is [`DriverError::TooManyRows`]. The zero-or-one peer of
    /// [`query_one`](Self::query_one).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_opt<'a, Q: TypedQuery + 'a>(
        &'a mut self,
        params: Q::Params<'a>,
    ) -> impl Future<Output = Result<Option<Q::Owned>, DriverError>> + 'a
    where
        Q::Params<'a>: 'a,
    {
        #[cfg(feature = "n1-detect")]
        let loc = core::panic::Location::caller();
        self.armed(move |c| {
            c.query_opt::<Q>(
                params,
                #[cfg(feature = "n1-detect")]
                loc,
            )
        })
    }

    /// Stream a compile-checked `query!`'s rows one at a time to `on_row` in
    /// CONSTANT memory — the streaming peer of [`query`](Self::query). See
    /// [`Connection::query_each`] for the full contract.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_each<'a, Q, F, E>(
        &'a mut self,
        params: Q::Params<'a>,
        on_row: F,
    ) -> impl Future<Output = Result<Option<E>, DriverError>> + 'a
    where
        Q: TypedQuery + 'a,
        Q::Params<'a>: 'a,
        E: 'a,
        F: for<'q> FnMut(Q::Record<'q>) -> ControlFlow<E> + 'a,
    {
        #[cfg(feature = "n1-detect")]
        let loc = core::panic::Location::caller();
        self.armed(move |c| {
            c.query_each::<Q, F, E>(
                params,
                on_row,
                #[cfg(feature = "n1-detect")]
                loc,
            )
        })
    }

    /// Run a HETEROGENEOUS ATOMIC pipeline inside this transaction — the guard peer
    /// of [`Connection::pipeline`].
    ///
    /// The pipeline's OWN `Sync` does NOT close this explicit transaction (the guard
    /// owns commit/rollback), so a batch here composes with the surrounding scope: a
    /// mid-batch failure rolls back the batch's commands AND leaves the transaction
    /// aborted, so the closure returns the classified [`DriverError::BatchFailed`]
    /// and the guard rolls the whole transaction back. When the pipeline is the
    /// FIRST statement in the body it fuses the deferred `BEGIN` (one round trip).
    pub fn pipeline<'a, B>(
        &'a mut self,
        batch: B,
    ) -> impl Future<Output = Result<B::Output, DriverError>> + 'a
    where
        B: Pipeline<'a> + 'a,
    {
        self.armed(move |c| c.pipeline(batch))
    }

    /// Run a HOMOGENEOUS ATOMIC bulk write inside this transaction — the guard peer
    /// of [`Connection::execute_batch`].
    ///
    /// The batch's OWN `Sync` does NOT close this explicit transaction (the guard
    /// owns commit/rollback), so it composes with the surrounding scope: a mid-batch
    /// failure rolls back the batch's commands AND leaves the transaction aborted, so
    /// the closure returns the classified [`DriverError::BatchFailed`] and the guard
    /// rolls the whole transaction back. When the batch is the FIRST statement in the
    /// body it fuses the deferred `BEGIN` (one round trip).
    pub fn execute_batch<'a, Q, I>(
        &'a mut self,
        params: I,
    ) -> impl Future<Output = Result<Vec<u64>, DriverError>> + 'a
    where
        Q: TypedQuery + 'a,
        I: IntoIterator<Item = Q::Params<'a>> + 'a,
    {
        self.armed(move |c| c.execute_batch::<Q, I>(params))
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
    pub async fn copy_in(
        &mut self,
        table: &str,
        rows_data: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<u64, DriverError> {
        self.copy_in_with(table, async |w| {
            for row in rows_data {
                w.write_row(row.as_ref().as_bytes()).await?;
            }
            Ok(())
        })
        .await
    }

    /// `COPY <table> FROM STDIN` with a scoped streaming writer — the
    /// CONSTANT-MEMORY, cancellation-safe bulk-load primitive. See
    /// [`Connection::copy_in_with`] for the full cancellation / recovery contract;
    /// the orchestration here is identical (the guard just borrows the same
    /// [`Core`] the `Connection` method does).
    pub async fn copy_in_with<F>(&mut self, table: &str, f: F) -> Result<u64, DriverError>
    where
        F: AsyncFnOnce(&mut CopyInWriter<'_>) -> Result<(), DriverError>,
    {
        // Arm the deferred BEGIN at the poll of this first COPY step, so a COPY
        // that is the transaction's first statement still fuses the BEGIN — and a
        // transaction future dropped before this point armed nothing.
        let live = self.armed(|c| c.copy_in_begin_table(table)).await?;
        let body = {
            let mut writer = CopyInWriter {
                core: &mut *self.core,
                scratch: Vec::new(),
            };
            f(&mut writer).await
        };
        match body {
            Ok(()) => self.core.copy_in_finish(live).await,
            Err(e) => {
                self.core.copy_in_abort(live).await;
                Err(e)
            }
        }
    }

    /// `COPY … FROM STDIN` in PGCOPY BINARY into a [`copy!`](bsql_postgres_core::TypedCopyIn)
    /// carrier `Q`'s target, in CONSTANT memory. See
    /// [`Connection::copy_in_typed`] for the full typed contract; the
    /// orchestration here is identical (the guard borrows the same [`Core`]), and
    /// the deferred `BEGIN` even fuses into this COPY when it is the transaction's
    /// FIRST statement.
    pub async fn copy_in_typed<'q, Q, I>(&mut self, rows: I) -> Result<u64, DriverError>
    where
        Q: TypedCopyIn,
        I: IntoIterator<Item = Q::Row<'q>>,
    {
        // Arm the deferred BEGIN at the poll of this first COPY step (the whole
        // typed-COPY future is one Core verb, and its `copy_in_begin` stages the
        // armed prelude), so a COPY that is the transaction's first statement
        // still fuses the BEGIN — and a transaction future dropped before this
        // point armed nothing.
        self.armed(|c| c.copy_in_typed::<Q, I>(rows)).await
    }

    /// `COPY <table> TO STDOUT`, streaming each row to `on_chunk` in CONSTANT
    /// memory. See [`Connection::copy_out`] for the full contract.
    pub fn copy_out<'a, F, E>(
        &'a mut self,
        table: &'a str,
        on_chunk: F,
    ) -> impl Future<Output = Result<Option<E>, DriverError>> + 'a
    where
        F: for<'q> FnMut(&'q [u8]) -> ControlFlow<E> + 'a,
        E: 'a,
    {
        self.armed(move |c| c.copy_out(table, on_chunk))
    }

    // ── Session subscription (LISTEN/UNLISTEN — atomicity-neutral) ──────────

    /// Subscribe to a `LISTEN` channel (validated; see [`Connection::listen`]).
    pub fn listen<'a>(
        &'a mut self,
        channel: &'a str,
    ) -> impl Future<Output = Result<(), DriverError>> + 'a {
        self.armed(move |c| c.listen(channel))
    }

    /// Unsubscribe from a `LISTEN` channel (see [`Connection::unlisten`]).
    pub fn unlisten<'a>(
        &'a mut self,
        channel: &'a str,
    ) -> impl Future<Output = Result<(), DriverError>> + 'a {
        self.armed(move |c| c.unlisten(channel))
    }
}

#[cfg(test)]
mod keepalive_tests {
    //! C3 WITNESS (offline): [`set_tcp_keepalive`] turns `SO_KEEPALIVE` ON for a
    //! connected TCP stream — the dead-peer-detection posture every TCP connection
    //! gets by default. A real loopback socket pair (no PG), so the getter reads
    //! the option straight back off the fd via `socket2`.

    use super::set_tcp_keepalive;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn set_tcp_keepalive_enables_so_keepalive() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let client = TcpStream::connect(addr).await.expect("connect");
        let (_server, _peer) = listener.accept().await.expect("accept");

        // A fresh TCP socket has keepalive OFF by default.
        assert!(
            !socket2::SockRef::from(&client)
                .keepalive()
                .expect("read keepalive before"),
            "a fresh TCP socket must have SO_KEEPALIVE off",
        );
        // After our helper it is ON — the connect path applies exactly this.
        set_tcp_keepalive(&client).expect("enable keepalive");
        assert!(
            socket2::SockRef::from(&client)
                .keepalive()
                .expect("read keepalive after"),
            "set_tcp_keepalive must turn SO_KEEPALIVE on",
        );
    }
}
