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
//! notification read-timeout arming, the `FnOnce` `transaction` / `copy_in_with`,
//! and the best-effort `Drop`-time `Terminate`.
//!
//! # Footprint regime
//!
//! The stable public *types* this driver re-exports carry their `size_of` pins in
//! `bsql-postgres-core`. The sync driver has no futures of its own — its verbs are
//! blocking calls whose working set lives on the caller's stack — so there is no
//! `future_pin!` surface here.

use core::ops::ControlFlow;
use core::str::FromStr;
// `std::io` and the blocking `Read`/`Write` traits are named only by the
// `tls`-gated SSLRequest probe + TLS-config path in `build_wire` (and
// `lift_probe_io`); with `tls` off no probe is sent and none is used.
#[cfg(feature = "tls")]
use std::io::{self, Read, Write};
use std::net::TcpStream;
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
    resolve_endpoint, validate_startup_params, ConnectConfig, DriverError, Endpoint, Notification,
    QueryResult, Row, Rows, SslMode, TypedNotification,
};
use bsql_postgres_proto::engine::{self, EngineError, SpuriousPending};
use bsql_postgres_proto::params::ParamsWriter;
use bsql_postgres_proto::{
    Credentials, DatabaseName, Ident, Password, PreparedQuery, RowDecode, Sensitive, TypedQuery,
};

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
        let endpoint = resolve_endpoint(&config.host, config.port);
        // Fail LOUD: TLS cannot be required over a socket that will never do it.
        // Rejected before the connect syscall — a wasted dial would tell us
        // nothing, and this is a pre-connect configuration fault.
        if endpoint.is_unix() && config.ssl_mode == SslMode::Require {
            return Err(DriverError::Config(
                "SslMode::Require cannot be honored over a unix-domain socket \
                 (TLS is not available on a local socket)",
            ));
        }
        let sock = match endpoint {
            Endpoint::Tcp(addr) => {
                let tcp = TcpStream::connect(&addr)?;
                // Disable Nagle on the data socket for the connection's whole life
                // — Nagle + delayed-ACK can add ~40ms stalls to small writes and
                // COPY-in streaming; one setsockopt with zero per-op cost. Nagle
                // is a TCP concept — `AF_UNIX` has no such buffering, so the unix
                // arm skips it (it is meaningless, not an error).
                tcp.set_nodelay(true)?;
                SyncSock::Tcp(tcp)
            }
            Endpoint::Unix(path) => SyncSock::Unix(UnixStream::connect(&path)?),
        };
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

        let wire = Self::build_wire(sock, config)?;
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
            Some(pw) => {
                let password = Password::try_from_str(pw)
                    .map_err(|_| DriverError::Config("invalid password"))?;
                Credentials::ScramPassword(Sensitive::new(password))
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
        // The engine captured `server_version` from the startup `ParameterStatus`
        // reports during the handshake, so it is read here for free. `None` if the
        // server sent no such report (honest absence, not a fabricated value).
        let server_version = engine
            .server_version()
            .map_err(|_| DriverError::NotReady)?
            .map(str::to_owned);

        Ok(Self {
            core: Core::new(engine, live, encrypted, server_version, backend_pid),
            socket_ctl: Some(socket_ctl),
        })
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
        let server_version = engine
            .server_version()
            .map_err(|_| DriverError::NotReady)?
            .map(str::to_owned);
        Ok(Self {
            core: Core::new(engine, live, encrypted, server_version, backend_pid),
            socket_ctl: None,
        })
    }

    /// Build the plaintext or TLS wire, performing the PG `SSLRequest` negotiation
    /// on the raw socket when SSL is wanted.
    ///
    /// A unix-domain socket is ALWAYS plaintext: TLS over a local kernel socket is
    /// pointless, PostgreSQL does not offer it there, and `SslMode::Require` +
    /// unix was already rejected by [`connect`](Self::connect). So the
    /// `SSLRequest` probe runs only for a TCP socket with SSL wanted; `Prefer`
    /// over unix is plaintext with no probe and no downgrade warning (nothing was
    /// downgraded — TLS was never applicable to a local socket).
    fn build_wire(sock: SyncSock, config: &ConnectConfig) -> Result<SyncWire, DriverError> {
        if matches!(sock, SyncSock::Unix(_)) || config.ssl_mode == SslMode::Disable {
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
            if config.ssl_mode == SslMode::Require {
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
            match bsql_postgres_core::ssl::classify_ssl_response(response[0], config)? {
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

    /// Run a runtime-SQL query returning the first row if any.
    pub fn query_opt(&mut self, sql: &str) -> Result<Option<Row>, DriverError> {
        drive_sync(engine::poll_once(self.core.query_opt(sql)))
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
    /// the schema at build time, params are bound in binary. An oversize row is a
    /// classified [`DriverError::OversizeRow`], never a silent truncation. The
    /// statement is Parsed once per connection and the server-side plan reused
    /// thereafter. The runtime-SQL escape hatch is [`query_sql`](Self::query_sql).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query<Q: TypedQuery>(&mut self, params: Q::Params) -> Result<Rows<Q>, DriverError> {
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
    pub fn query_one<Q: TypedQuery>(&mut self, params: Q::Params) -> Result<Q::Owned, DriverError> {
        drive_sync(engine::poll_once(self.core.query_one::<Q>(
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
    /// - `Err(DriverError::OversizeRow)` — a row exceeded the inline buffer; rows
    ///   before it were delivered, then the classified error (never a truncation).
    /// - other `Err` — a fatal transport/protocol fault; the connection is dead.
    ///
    /// A [`Break`](ControlFlow::Break) of a colossal result still reads the
    /// remaining rows to reach the clean idle boundary — O(remaining rows).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_each<Q, F, E>(
        &mut self,
        params: Q::Params,
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
    /// Tier-1 safety: the transaction boundary is the closure scope, so there is no
    /// object to leak. On a body error the caller's error dominates and a
    /// best-effort `ROLLBACK` is issued; its outcome is already encoded in the
    /// liveness token (a failed `ROLLBACK` leaves the connection dead, which
    /// [`is_healthy`](Self::is_healthy) reports so a pool evicts it).
    pub fn transaction<R>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<R, DriverError>,
    ) -> Result<R, DriverError> {
        self.simple_query("BEGIN")?;
        let result = match f(self) {
            Ok(value) => {
                self.simple_query("COMMIT")?;
                Ok(value)
            }
            Err(e) => {
                // Best-effort rollback; the outcome rides the liveness token, so it
                // is explicitly discarded. The caller's error `e` dominates.
                drop(self.simple_query("ROLLBACK"));
                Err(e)
            }
        };
        // Either terminator closes a logical operation: forget the N+1 recency
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
    /// # Errors
    ///
    /// Any transport / server error is returned classified; a pool evicts a
    /// connection whose reset failed rather than handing out a still-dirty one.
    pub fn reset_session(&mut self) -> Result<(), DriverError> {
        drive_sync(engine::poll_once(self.core.reset_session()))
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
                // The payload string moves into the classified error.
                Err(_) => Err(DriverError::PayloadParse(n.payload)),
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
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Best-effort graceful terminate over the blocking socket (resolves in one
        // poll). Drop cannot propagate the outcome and a closed socket is fine, so
        // it is explicitly discarded. `close` is idempotent — a no-op if the token
        // was already taken (a graceful `close` call, or a fatal verb).
        drop(engine::poll_once(self.core.close()));
    }
}

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
