//! The blocking PostgreSQL connection, driven through the sans-IO engine.
//!
//! A [`Connection`] owns an [`Engine`] over a [`Wire<SyncSocket>`] plus the
//! linear liveness token the engine's verbs thread. Every public method takes
//! the token, drives one verb over the blocking transport with `poll_once`
//! (single-poll: a blocking op resolves on the first poll), and returns it on a
//! clean boundary — so at-most-one-command-in-flight is a move-checked invariant.
//!
//! # Token lifecycle and recovery (the health bit)
//!
//! `self.live` is the health bit: `Some` = the connection is at a clean boundary
//! and reusable, `None` = a verb failed fatally and the connection is dead. The
//! engine's tier-1 error model decides the bit: a verb returns its linear `Live`
//! token inside `Ok(Outcome { live, status })` whenever the connection is ALIVE
//! — including on a *recoverable* server error (a query-level `ErrorResponse`),
//! which the verb drains to a clean idle itself and reports as
//! [`CommandStatus::ServerErrored`]. So [`settle`] ALWAYS restores `self.live`
//! from an `Ok` outcome (no separate token reclaim), then maps a `ServerErrored`
//! status to `Err(DriverError::Db)` while keeping the connection pooled. Only a
//! FATAL `Err(EngineError)` (transport/protocol/EOF) — or a `SpuriousPending` —
//! leaves `self.live` `None`.
//!
//! [`settle`]: Connection::settle

use core::ops::ControlFlow;
use core::str::FromStr;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use bsql_postgres_core::materialize::{self, ResultCollector};
use bsql_postgres_core::sql_ident;
use bsql_postgres_core::tls::{self, CaRootsError, TlsError, TlsTransport, Wire};
use bsql_postgres_core::{
    capture_notify, validate_startup_params, ConnectConfig, DbError, DbErrorSink, DriverError,
    NotificationLedger, Notification, QueryResult, Row, Rows, RowsBuilder, SslMode,
    TypedNotification,
};
use bsql_postgres_proto::engine::{
    self, Boundary, CommandStatus, ConnFail, Engine, EngineError, Live, NotifyStatus, Outcome,
    PreparedStatement as WireStatement, SpuriousPending, Surface,
};
use bsql_postgres_proto::params::ParamsWriter;
use bsql_postgres_proto::{
    Credentials, DatabaseName, DecodeError, Ident, Password, PreparedQuery, RowDecode, Sensitive,
    StmtName, TxStatus, TypedQuery,
};

use crate::transport::SyncSocket;

/// The plaintext-or-TLS transport the engine is monomorphic over.
type SyncWire = Wire<SyncSocket>;
/// The arm-uniform transport error: a plaintext socket error rides
/// [`TlsError::Socket`]; the TLS arm's error already is this type.
type WireError = TlsError<io::Error>;
/// The owned, poolable engine handle (branded `'static`).
type SyncEngine = Engine<'static, SyncWire>;
/// Result of a single-poll verb drive, before classification.
type Polled<T> = Result<Result<T, EngineError<WireError>>, SpuriousPending>;

/// Why the streaming [`query_each`](Connection::query_each) sink stopped the pump
/// early — the break payload it hands to the engine's breakable verb.
///
/// Two DISTINCT constructors keep a per-row typed-decode failure and a
/// caller-requested stop impossible to conflate: the pump boundary's `Stopped`
/// payload alone says which happened, so the driver never has to cross-reference a
/// side channel to know why the stream ended. Only ever constructed on the cold
/// break path (a stack value), never on the per-row hot path.
enum Stop<E> {
    /// A row's bytes did not match the query's compile-time record shape.
    Decode(DecodeError),
    /// The caller's `on_row` returned [`ControlFlow::Break`], carrying its payload.
    User(E),
}

/// A prepared statement handle.
///
/// Carries the engine's wire-level statement handle (statement name + recovered
/// result OIDs) plus the column names captured at prepare time — the extended
/// execute reply does not re-send them, so a prepared query's `QueryResult`
/// draws its names from here. Move-only: [`close_statement`] consumes it by
/// value, so a use after close is a compile error (E0382), not a runtime
/// use-after-close.
///
/// [`close_statement`]: Connection::close_statement
#[derive(Debug)]
pub struct PreparedStatement {
    inner: WireStatement,
    column_names: Arc<[String]>,
}

/// A lending `COPY … FROM STDIN` writer, handed to the closure of
/// [`copy_in_with`](Connection::copy_in_with).
///
/// Borrows the connection's engine for the copy's duration and streams each
/// row/chunk as one `CopyData` frame flushed straight to the socket — nothing is
/// buffered, so a bulk load of millions of rows runs in CONSTANT memory (0 heap
/// growth per row; one reused scratch buffer for [`write_row`](Self::write_row)'s
/// trailing newline).
///
/// The writer never closes the copy itself: [`copy_in_with`](Connection::copy_in_with)
/// owns the terminal step, sending `CopyDone` when the closure returns `Ok` and
/// `CopyFail` when it returns `Err`, so an early error recovers the connection
/// rather than stranding it in copy mode.
///
/// No `Debug`: it borrows the connection's engine (a live socket / TLS session),
/// which is not `Debug` — the same reason [`Connection`] carries none.
pub struct CopyInWriter<'e> {
    engine: &'e mut SyncEngine,
    /// Reused across [`write_row`](Self::write_row) calls so appending the row
    /// separator costs no per-row allocation.
    scratch: Vec<u8>,
}

impl CopyInWriter<'_> {
    /// Stream one `CopyData` frame with `chunk` as its verbatim body, flushed to
    /// the socket. Zero-copy: the bytes are queued directly. For text `COPY`,
    /// `chunk` is raw copy-format bytes — the caller controls row boundaries and
    /// framing.
    ///
    /// # Errors
    ///
    /// A classified [`DriverError`] on a transport fault (the connection is then
    /// dead) or a [`SpuriousPending`] over a blocking socket; never a panic.
    pub fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), DriverError> {
        match engine::poll_once(self.engine.copy_in_write(chunk)) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(other)) => Err(lift_engine_error(other)),
            Err(SpuriousPending) => Err(DriverError::SpuriousPending),
        }
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
        match engine::poll_once(self.engine.copy_in_write(&self.scratch)) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(other)) => Err(lift_engine_error(other)),
            Err(SpuriousPending) => Err(DriverError::SpuriousPending),
        }
    }
}

/// Non-secret session facts captured at connect time.
struct SessionParams {
    server_version: Option<String>,
    backend_pid: i32,
}

/// A blocking PostgreSQL connection over the sans-IO engine.
pub struct Connection {
    engine: SyncEngine,
    /// The liveness token, or `None` when the connection is dead. The health bit.
    live: Option<Live<'static>>,
    /// Whether the underlying wire is TLS-encrypted, captured at connect from the
    /// built [`Wire`] arm. Immutable for the connection's life (PostgreSQL
    /// negotiates TLS once, before startup, and never up/downgrades mid-session).
    /// Read via [`is_encrypted`](Self::is_encrypted) so a consumer over an
    /// untrusted network can ASSERT it — catching a silent `SslMode::Prefer`
    /// downgrade that left the connection on plain TCP.
    encrypted: bool,
    /// A `try_clone` of the underlying socket, used to arm socket read/write
    /// timeouts on a fd the engine otherwise owns: the engine owns the I/O
    /// socket, but a dup'd handle shares the same kernel socket, so a timeout set
    /// here applies to the engine's own reads and writes. Two callers arm it, and
    /// both leave it DISARMED on exit: [`connect`](Self::connect) bounds the
    /// TCP-connect + startup/auth handshake with `connect_timeout`, then disarms
    /// it so steady-state I/O blocks indefinitely (matching the async driver — a
    /// slow query must never trip a deadline and kill a healthy connection); and
    /// [`recv_notification`](Self::recv_notification) arms a bounded read deadline
    /// for its own poll and restores the disarmed state on exit. `None` for an
    /// in-memory testkit connection, which has no socket and never blocks — the
    /// arming is then a no-op.
    socket_ctl: Option<TcpStream>,
    params: SessionParams,
    stmt_counter: u32,
    /// The bounded, counted no-drop buffer of asynchronous notifications. Every
    /// verb's sink is wrapped with [`capture_notify`] so a `NOTIFY` arriving on
    /// any command's response stream is buffered here rather than dropped;
    /// [`recv_notification`](Self::recv_notification) drains it front-first, and
    /// [`reset_session`](Self::reset_session) clears it so a pooled connection
    /// never delivers a prior user's notifications to the next.
    notifications: NotificationLedger,
    /// The diagnostics-only N+1 query detector. Present ONLY under the
    /// `n1-detect` feature — a default build has no such field, so the flagship
    /// typed verbs stay byte-identical and the connection footprint is
    /// unchanged. Each typed verb feeds it its `(sql, call-site)` pair; a
    /// logical-operation boundary (commit/rollback, session reset) clears its
    /// recency window. Read via [`n1_report`](Self::n1_report).
    #[cfg(feature = "n1-detect")]
    n1_tracker: bsql_postgres_core::N1Tracker,
}

impl Connection {
    /// Open a connection: TCP connect, optional TLS negotiation, then the
    /// startup/auth handshake through the engine.
    ///
    /// # Errors
    ///
    /// A classified [`DriverError`] for any pre-connect validation, transport,
    /// TLS, or handshake failure — never a panic.
    pub fn connect(config: &ConnectConfig) -> Result<Self, DriverError> {
        let addr = format!("{}:{}", config.host, config.port);
        let tcp = TcpStream::connect(&addr)?;
        // Disable Nagle on the data socket for the connection's whole life. Set
        // once here, TCP_NODELAY rides the SAME kernel socket through the
        // `SSLRequest` probe, the TLS wrap, and the `try_clone` control handle —
        // so the actual data socket carries it post-probe, post-clone. Nagle +
        // delayed-ACK can add ~40ms stalls to small writes and COPY-in streaming;
        // this is one setsockopt with zero per-op cost.
        tcp.set_nodelay(true)?;
        // `connect_timeout` bounds ONLY the connect phase — the TCP connect
        // (above), the TLS `SSLRequest` probe, and the startup/auth handshake —
        // so a dead or silent server at connect still fails fast. It is armed as
        // the socket read+write timeout here and DISARMED once the handshake
        // completes (below): steady-state reads/writes then block indefinitely,
        // matching the async driver, so a slow query (a long OLAP scan, a lock
        // wait) can never turn a healthy connection into a fatal timeout.
        let connect_timeout = Duration::from_secs(config.connect_timeout_secs);
        tcp.set_read_timeout(Some(connect_timeout))?;
        tcp.set_write_timeout(Some(connect_timeout))?;
        // The dup'd control handle shares the kernel socket, so a timeout set on
        // it applies to the engine's reads/writes. Taken before the socket is
        // moved into the wire / TLS layer.
        let socket_ctl = tcp.try_clone()?;

        let wire = Self::build_wire(tcp, config)?;
        // Snapshot the encryption state from the built wire BEFORE it is moved
        // into the engine — `Wire::is_encrypted` is the single source of truth.
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
        // reads and writes block indefinitely (async-parity). A slow query must
        // not become a fatal timeout on a healthy connection; the only remaining
        // deadline is the bounded one `recv_notification` arms for its own wait,
        // which it restores to disarmed on exit.
        socket_ctl.set_read_timeout(None)?;
        socket_ctl.set_write_timeout(None)?;
        let backend_pid = engine.backend_pid().map_err(|_| DriverError::NotReady)?;
        // The engine captured `server_version` from the startup `ParameterStatus`
        // reports during the handshake, so it is read here for free — no
        // `SHOW server_version` round-trip. `None` if the server sent no such
        // report (honest absence, not a fabricated value).
        let server_version = engine
            .server_version()
            .map_err(|_| DriverError::NotReady)?
            .map(str::to_owned);

        Ok(Self {
            engine,
            live: Some(live),
            encrypted,
            socket_ctl: Some(socket_ctl),
            params: SessionParams {
                server_version,
                backend_pid,
            },
            stmt_counter: 0,
            notifications: NotificationLedger::new(),
            #[cfg(feature = "n1-detect")]
            n1_tracker: bsql_postgres_core::N1Tracker::new(),
        })
    }

    /// Open a connection over an in-memory
    /// [`FakeTransport`](bsql_postgres_core::testkit::FakeTransport) instead of a
    /// socket — the testkit entry point, the sync twin of the async
    /// `connect_fake`.
    ///
    /// It drives the real startup/auth handshake and every subsequent verb
    /// through the SAME engine the TCP path uses (single-poll: the fake never
    /// blocks, so `poll_once` resolves on the first poll), so the returned
    /// `Connection` is a genuine connection — same methods, same decode — backed
    /// by the fake's scripted replies with no network. There is no SSL
    /// negotiation (the fake is plaintext by construction), so `connect_fake`
    /// skips the socket build entirely and plugs the fake straight into the
    /// `Wire::Fake` arm. `socket_ctl` is `None`: there is no socket to arm a read
    /// timeout on, and the fake never blocks.
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
            engine,
            live: Some(live),
            encrypted,
            socket_ctl: None,
            params: SessionParams {
                server_version,
                backend_pid,
            },
            stmt_counter: 0,
            notifications: NotificationLedger::new(),
            #[cfg(feature = "n1-detect")]
            n1_tracker: bsql_postgres_core::N1Tracker::new(),
        })
    }

    /// Build the plaintext or TLS wire, performing the PG `SSLRequest`
    /// negotiation on the raw socket when SSL is wanted.
    fn build_wire(tcp: TcpStream, config: &ConnectConfig) -> Result<SyncWire, DriverError> {
        if config.ssl_mode == SslMode::Disable {
            return Ok(Wire::Plain(SyncSocket::new(tcp)));
        }
        let ssl_bytes = bsql_postgres_core::ssl::ssl_request_bytes();
        let mut tcp = tcp;
        // A read/write deadline here — the armed connect-phase `connect_timeout`
        // firing on a server silent on the `SSLRequest` probe byte — is a
        // connect-phase TIMEOUT, classified as such via `lift_probe_io`: the SAME
        // class the async driver and the post-probe TLS handshake (`lift_tls_error`)
        // surface, so the two drivers agree. A bare `?` here would instead map it
        // through `From<io::Error>` to the generic `DriverError::Io(TimedOut)` —
        // the cross-driver divergence this closes. Every other io error keeps its
        // real class.
        Write::write_all(&mut tcp, ssl_bytes).map_err(lift_probe_io)?;
        let mut response = [0u8; 1];
        Read::read_exact(&mut tcp, &mut response).map_err(lift_probe_io)?;
        match bsql_postgres_core::ssl::classify_ssl_response(response[0], config)? {
            bsql_postgres_core::ssl::SslProbe::Accepted { server_name } => {
                // Use the provider-explicit ring config: under the ring-only
                // crypto pin a bare `ClientConfig::builder()` installs no default
                // provider and would fault at the handshake. Custom CA roots (an
                // internal CA via `with_ca_roots` / `sslrootcert`) build a
                // dedicated config verified against EXACTLY those roots; otherwise
                // the shared default-roots config. A bad/empty custom PEM is a
                // classified `Config` error — fail-closed, never a fallback to the
                // default roots or to plaintext. The server name comes from the probe.
                let cfg = match config.ca_roots_pem() {
                    Some(pem) => tls::client_config_with_ca_roots(pem).map_err(lift_ca_roots_error)?,
                    None => tls::shared_client_config().map_err(|e| {
                        DriverError::Io(io::Error::other(format!("TLS config: {e}")))
                    })?,
                };
                let socket = SyncSocket::new(tcp);
                let tls = match engine::poll_once(TlsTransport::connect(socket, cfg, server_name)) {
                    Ok(Ok(transport)) => transport,
                    Ok(Err(e)) => return Err(lift_tls_error(e)),
                    Err(SpuriousPending) => return Err(DriverError::SpuriousPending),
                };
                Ok(Wire::Tls(Box::new(tls)))
            }
            bsql_postgres_core::ssl::SslProbe::PlainTcp => Ok(Wire::Plain(SyncSocket::new(tcp))),
        }
    }

    /// Take the liveness token, or classify a dead connection.
    fn take_live(&mut self) -> Result<Live<'static>, DriverError> {
        self.live.take().ok_or(DriverError::NotReady)
    }

    /// Classify a command verb's single-poll [`Outcome`] and manage the token.
    ///
    /// An `Ok` outcome ALWAYS restores the token — the connection is alive
    /// whether the command completed or recovered from a server error (the verb
    /// already drained the recovering `ReadyForQuery`). A
    /// [`CommandStatus::ServerErrored`] then surfaces the parsed [`DbError`] the
    /// collector captured from the raw `ErrorResponse`, while the connection
    /// stays pooled. A fatal `Err` (transport/protocol/EOF) or a
    /// `SpuriousPending` leaves the token gone (`self.live == None`), so
    /// [`is_healthy`](Self::is_healthy) reports the connection dead — no separate
    /// token-reclaim step exists.
    fn settle(
        &mut self,
        polled: Polled<Outcome<'static, CommandStatus>>,
        collector: &mut impl DbErrorSink,
    ) -> Result<(), DriverError> {
        match polled {
            Ok(Ok(Outcome { live, status })) => {
                // The connection is alive on either status — restore the token.
                self.live = Some(live);
                match status {
                    CommandStatus::Completed => Ok(()),
                    CommandStatus::ServerErrored => {
                        // The pump surfaced the raw `ErrorResponse` to the sink
                        // before the failure boundary, so the collector holds the
                        // parsed cause; the connection stays pooled.
                        match collector.take_db_error() {
                            Some(db) => Err(DriverError::Db(Box::new(db))),
                            None => Err(DriverError::UnclassifiedFailure),
                        }
                    }
                }
            }
            // Fatal: the verb consumed the token and the connection is dead.
            // `self.live` was taken before the verb and is not restored.
            Ok(Err(other)) => Err(lift_engine_error(other)),
            Err(SpuriousPending) => Err(DriverError::SpuriousPending),
        }
    }

    /// Generate a fresh, unique prepared-statement name.
    fn next_stmt_name(&mut self) -> Result<StmtName, DriverError> {
        let id = self.stmt_counter;
        self.stmt_counter = self.stmt_counter.wrapping_add(1);
        StmtName::try_from_str(&format!("_bsql_{id}"))
            .map_err(|_| DriverError::Config("generated statement name invalid"))
    }


    /// Build a [`QueryResult`] from a finished collector, optionally overriding
    /// the column names (the prepared path supplies the names captured at
    /// prepare time, since the execute reply re-sends none).
    fn build_query_result(
        collector: ResultCollector,
        names_override: Option<Arc<[String]>>,
    ) -> Result<QueryResult, DriverError> {
        let collected = collector.finish()?;
        // An empty result set has 0 columns by definition; a non-empty one
        // exposes its width via row 0.
        let column_count = match collected.rows.first() {
            Some(row) => row.len(),
            None => 0,
        };
        let column_names = match names_override {
            Some(names) => names,
            None => Arc::from(collected.column_names.into_boxed_slice()),
        };
        Ok(QueryResult {
            rows: collected.rows,
            command_tag: collected.command_tag,
            column_count,
            column_names,
        })
    }

    // ── Public API ────────────────────────────────────────────────────────

    /// Round-trip a `Sync` to confirm the connection is live.
    pub fn ping(&mut self) -> Result<(), DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.ping(live, capture_notify(&mut self.notifications, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        })));
        self.settle(polled, &mut collector)
    }

    /// Issue a simple query, returning the command tag string.
    pub fn simple_query(&mut self, sql: &str) -> Result<String, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.simple_query(live, sql, capture_notify(&mut self.notifications, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        })));
        self.settle(polled, &mut collector)?;
        Ok(collector.command_tag().to_string())
    }

    /// Execute a non-row runtime-SQL command, returning the affected-row count.
    /// The compile-checked counterpart is [`execute`](Self::execute).
    pub fn execute_sql(&mut self, sql: &str) -> Result<u64, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.execute(live, sql, capture_notify(&mut self.notifications, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        })));
        self.settle(polled, &mut collector)?;
        Ok(collector.affected())
    }

    /// Run a row-returning runtime-SQL query (text result columns). The
    /// compile-checked, typed counterpart is [`query`](Self::query).
    pub fn query_sql(&mut self, sql: &str) -> Result<QueryResult, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.query(live, sql, capture_notify(&mut self.notifications, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        })));
        self.settle(polled, &mut collector)?;
        Self::build_query_result(collector, None)
    }

    /// Run a runtime-SQL query returning the first row, or [`DriverError::NoRows`].
    /// The compile-checked counterpart is [`query_one`](Self::query_one).
    pub fn query_one_sql(&mut self, sql: &str) -> Result<Row, DriverError> {
        self.query_sql(sql)?.rows.into_iter().next().ok_or(DriverError::NoRows)
    }

    /// Run a runtime-SQL query returning the first row if any.
    pub fn query_opt(&mut self, sql: &str) -> Result<Option<Row>, DriverError> {
        Ok(self.query_sql(sql)?.rows.into_iter().next())
    }

    /// Prepare a statement: `Parse` + `Describe` + `Sync`, recovering the result
    /// schema for later `Bind`+`Execute`.
    pub fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, DriverError> {
        let stmt_name = self.next_stmt_name()?;
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.prepare(live, &stmt_name, sql, capture_notify(&mut self.notifications, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        })));
        self.settle(polled, &mut collector)?;
        let result_oids = collector.oids().to_vec();
        let column_names: Arc<[String]> =
            Arc::from(collector.column_names().to_vec().into_boxed_slice());
        Ok(PreparedStatement {
            inner: WireStatement::new(stmt_name, result_oids),
            column_names,
        })
    }

    /// Execute a prepared statement returning rows.
    ///
    /// The params are borrowed all the way to the engine (which serialises them
    /// through `ParamsWriter::write_params` by reference), so a non-`Copy`
    /// owned param — a `Numeric`, a `Json` / `Jsonb`, a `String` — binds here
    /// exactly as it does through the compile-checked `query!` path.
    pub fn query_prepared<P: ParamsWriter>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.query_prepared(live, &stmt.inner, params, capture_notify(&mut self.notifications, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        })));
        self.settle(polled, &mut collector)?;
        Self::build_query_result(collector, Some(stmt.column_names.clone()))
    }

    /// Execute a prepared statement for its side effect, returning the affected
    /// count. Params are borrowed to the engine (see
    /// [`query_prepared`](Self::query_prepared)), so a non-`Copy` owned param
    /// binds here too.
    pub fn execute_prepared<P: ParamsWriter>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<u64, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.execute_prepared(live, &stmt.inner, params, capture_notify(&mut self.notifications, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        })));
        self.settle(polled, &mut collector)?;
        Ok(collector.affected())
    }

    /// Execute a compile-checked `query!` query for its side effect,
    /// returning the affected-row count (binary-uniform params).
    ///
    /// The flagship typed `execute`: Parses the content-addressed statement once
    /// per connection, then reuses the server-side plan (a bare Bind + Execute)
    /// on repeats. The runtime-SQL escape hatch is [`execute_sql`](Self::execute_sql).
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
        // Diagnostics-only N+1 record at the CALL site; compiled out when off.
        #[cfg(feature = "n1-detect")]
        self.n1_record(q.sql(), core::panic::Location::caller());
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.query_params(live, q, params, capture_notify(&mut self.notifications, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        })));
        self.settle(polled, &mut collector)?;
        Ok(collector.affected())
    }

    /// Run a compile-checked `query!` and collect its TYPED rows — the flagship
    /// parameterised query.
    ///
    /// `Q` is a `query!`-generated carrier (e.g. `FooQuery`); the returned
    /// [`Rows<Q>`] decodes lazily into the macro's typed records — borrowed
    /// (zero-copy text) via [`Rows::iter`], or owned via [`Rows::into_owned`].
    /// SQL is validated against the schema at build time, params are bound in
    /// binary, rows decode into the macro's records. The runtime-SQL escape
    /// hatch is [`query_sql`](Self::query_sql).
    ///
    /// The Parse + Bind + Execute + Sync runs over the macro's const wire
    /// artifact ([`Q::PREPARED`](TypedQuery::PREPARED)). The sink that collects
    /// rows into the prebuffer is INFALLIBLE (it copies bytes, never failing in
    /// a way the engine sink could report), and the connection is settled to a
    /// clean idle and repooled BEFORE any row is decoded — so a per-row decode
    /// failure is a [`Rows::iter`] item / [`Rows::into_owned`] error, never a
    /// connection fault. An oversize (chunk-streamed) row is a classified
    /// [`DriverError::OversizeRow`] (the bounded decoder needs one contiguous
    /// payload), never a silent truncation.
    ///
    /// # Server-side plan reuse
    ///
    /// Repeating the same `query!` on one connection always works — including a
    /// reused pooled connection, inside a transaction, and across transactions:
    /// correctness never depends on the cache. A first (or otherwise uncached) use
    /// safely (re)creates the prepared statement, and once it is durable
    /// (committed / autocommit) later uses reuse the server-side plan with a bare
    /// Bind + Execute. There is no duplicate-prepared-statement footgun.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query<Q: TypedQuery>(&mut self, params: Q::Params) -> Result<Rows<Q>, DriverError> {
        // Diagnostics-only N+1 record at the CALL site (`#[track_caller]` makes
        // `Location::caller()` report the caller). Compiled out when off — the
        // method is then byte-identical to a bare delegation.
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::PREPARED.sql(), core::panic::Location::caller());
        self.query_collect::<Q>(params)
    }

    /// The verb body shared by [`query`](Self::query) and the engine of
    /// [`query_one`](Self::query_one). Collects a typed result into a
    /// [`Rows<Q>`] prebuffer; classifies an oversize row loudly.
    fn query_collect<Q: TypedQuery>(&mut self, params: Q::Params) -> Result<Rows<Q>, DriverError> {
        let live = self.take_live()?;
        let mut builder = RowsBuilder::new();
        let polled = engine::poll_once(self.engine.query_params(live, &Q::PREPARED, params, capture_notify(&mut self.notifications, |s| {
            builder.feed(s);
            ControlFlow::Continue(())
        })));
        self.settle(polled, &mut builder)?;
        // The connection is now idle + pooled. An oversize row cannot be decoded
        // contiguously by the bounded path; classify it loudly.
        if builder.saw_oversize() {
            return Err(DriverError::OversizeRow);
        }
        Ok(builder.finish::<Q>())
    }

    /// Run a compile-checked `query!` expecting EXACTLY one row, returning the
    /// owned record.
    ///
    /// Returns the `'static` owned twin so the row outlives the result buffer.
    /// Zero rows is [`DriverError::NoRows`]; more than one is
    /// [`DriverError::TooManyRows`] (loud, never a silently-taken first row). The
    /// typed counterpart to the runtime-SQL
    /// [`query_one_sql`](Self::query_one_sql); shares [`query`](Self::query)'s
    /// server-side plan reuse.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_one<Q: TypedQuery>(
        &mut self,
        params: Q::Params,
    ) -> Result<Q::Owned, DriverError> {
        // Record at THIS call site, then reuse the shared body directly (NOT the
        // public `query`) so the N+1 count is attributed once, here, rather than
        // double-counted through an inner verb. Compiled out when off.
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::PREPARED.sql(), core::panic::Location::caller());
        let rows = self.query_collect::<Q>(params)?;
        match rows.len() {
            0 => Err(DriverError::NoRows),
            1 => rows
                .into_owned()?
                .into_iter()
                .next()
                .ok_or(DriverError::NoRows),
            _ => Err(DriverError::TooManyRows),
        }
    }

    /// Stream a compile-checked `query!`'s rows one at a time to `on_row` in
    /// CONSTANT memory — the streaming peer of [`query`](Self::query).
    ///
    /// Where [`query`](Self::query) buffers the whole result into a [`Rows<Q>`],
    /// this decodes each `DataRow` as it arrives (borrowed, zero-copy: a text
    /// column is `&str` into the transient ingest buffer) and hands the record to
    /// `on_row`, accumulating NOTHING — so a colossal result streams without
    /// growing memory (0 heap allocations per row, and none per result beyond the
    /// connection's already-allocated buffers).
    ///
    /// `on_row` returns [`ControlFlow`]: [`Continue`](ControlFlow::Continue) to
    /// keep streaming, or [`Break(e)`](ControlFlow::Break) to stop early. The
    /// borrowed record CANNOT escape the closure — the `for<'q>` bound is the
    /// compiler-enforced escape wall (an attempt to stash a record in an outer
    /// collection is a borrow error).
    ///
    /// # Returns
    ///
    /// - `Ok(None)` — the result was streamed to completion (every row seen).
    /// - `Ok(Some(e))` — `on_row` returned [`Break(e)`](ControlFlow::Break); the
    ///   stream stopped early and the connection was drained back to a clean idle,
    ///   so it stays healthy and reusable.
    /// - `Err(DriverError::Decode(..))` — a row failed to decode into its
    ///   compile-time record shape; the stream stopped, the connection was drained,
    ///   and it stays healthy — a decode failure is LOUD, never swallowed nor
    ///   defaulted, and never harms the connection.
    /// - `Err(DriverError::Db(..))` — the server reported an error mid-stream (a
    ///   runtime fault a build-time schema check cannot catch); the connection was
    ///   drained and stays healthy.
    /// - `Err(DriverError::OversizeRow)` — a row exceeded the engine's inline
    ///   buffer and streamed in chunks the bounded decoder cannot reassemble; rows
    ///   before it were still delivered to `on_row`, then the classified error is
    ///   returned (never a silent truncation).
    /// - other `Err` — a fatal transport/protocol fault; the connection is dead.
    ///
    /// # Early-abort cost
    ///
    /// A [`Break`](ControlFlow::Break) of a colossal result still READS (and
    /// discards) the remaining rows to reach the clean idle boundary that makes the
    /// connection reusable — O(remaining rows). A true constant-time early abort
    /// (a server-side row-limited portal) is a distinct, deferred capability.
    ///
    /// Shares [`query`](Self::query)'s server-side plan reuse: the statement is
    /// Parsed once per connection and reused thereafter.
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_each<Q, F, E>(
        &mut self,
        params: Q::Params,
        mut on_row: F,
    ) -> Result<Option<E>, DriverError>
    where
        Q: TypedQuery,
        F: for<'q> FnMut(Q::Record<'q>) -> ControlFlow<E>,
    {
        // Diagnostics-only N+1 record at the CALL site; compiled out when off.
        #[cfg(feature = "n1-detect")]
        self.n1_record(Q::PREPARED.sql(), core::panic::Location::caller());
        let live = self.take_live()?;
        // Captured across the streaming sink; read after the verb settles.
        let mut db_error: Option<DbError> = None;
        let mut oversize = false;
        let polled = engine::poll_once(self.engine.query_params_break(
            live,
            &Q::PREPARED,
            params,
            capture_notify(&mut self.notifications, |surface| match surface {
                Surface::Row(body) => match Q::decode_borrowed(body) {
                    // The record borrows the transient ingest buffer; `on_row`
                    // consumes it in-scope (the `for<'q>` wall forbids escape).
                    Ok(rec) => match on_row(rec) {
                        ControlFlow::Continue(()) => ControlFlow::Continue(()),
                        ControlFlow::Break(e) => ControlFlow::Break(Stop::User(e)),
                    },
                    // A decode failure is LOUD: stop the pump, never Continue past
                    // it and never substitute a default.
                    Err(de) => ControlFlow::Break(Stop::Decode(de)),
                },
                // Capture the server error's cause, then let the pump reach its
                // `Failed` boundary so the connection can be drained to idle.
                Surface::Fail(body) => {
                    db_error = Some(materialize::parse_error_response(body));
                    ControlFlow::Continue(())
                }
                // An oversize row streams as chunks the bounded typed decoder
                // cannot reassemble; flag it for a classified `OversizeRow` after
                // the stream ends — never reassemble, never truncate.
                Surface::RowChunk(_) | Surface::RowChunkEnd => {
                    oversize = true;
                    ControlFlow::Continue(())
                }
                // COPY / delivery / other async frames are not stream rows (a
                // NOTIFY is captured into the ledger by the wrapper above this
                // match, so it never reaches here to be dropped).
                _ => ControlFlow::Continue(()),
            }),
        ));

        // The token rides `Ok` on any ALIVE boundary; a fatal is `Err`.
        let (live, boundary) = match polled {
            Ok(Ok(Outcome { live, status })) => (live, status),
            Ok(Err(other)) => return Err(lift_engine_error(other)),
            Err(SpuriousPending) => return Err(DriverError::SpuriousPending),
        };
        match boundary {
            Boundary::Idle => {
                // Streamed to completion at a clean idle — no drain needed.
                self.live = Some(live);
                if oversize {
                    return Err(DriverError::OversizeRow);
                }
                Ok(None)
            }
            Boundary::Failed => {
                // Server error mid-stream: drain the recovering `ReadyForQuery`,
                // then surface the parsed cause. Connection stays alive + pooled.
                self.drain_to_idle(live)?;
                match db_error {
                    Some(db) => Err(DriverError::Db(Box::new(db))),
                    None => Err(DriverError::UnclassifiedFailure),
                }
            }
            Boundary::Stopped(Stop::User(e)) => {
                // Caller broke early: drain to reclaim, then report the stop value.
                self.drain_to_idle(live)?;
                if oversize {
                    return Err(DriverError::OversizeRow);
                }
                Ok(Some(e))
            }
            Boundary::Stopped(Stop::Decode(de)) => {
                // A per-row decode failure broke the stream: drain to reclaim, then
                // surface the loud classified decode error.
                self.drain_to_idle(live)?;
                Err(DriverError::Decode(de))
            }
            // `query_params_break` maps Closed/Suspended to a fatal `Err`, so they
            // never ride an `Ok` outcome; `Boundary` is `#[non_exhaustive]`, so
            // this classified arm also covers any future boundary. The token is
            // dropped (not restored), so the connection is left dead + evictable.
            _ => Err(DriverError::Io(io::Error::other(
                "unexpected protocol boundary from a streaming query",
            ))),
        }
    }

    /// Drain a connection left DIRTY by an early stop of a streaming query to a
    /// clean idle boundary, restoring the token. Sends nothing (the request was
    /// already flushed). A fatal transport/protocol fault (or a `SpuriousPending`)
    /// during the drain kills the connection (the token is consumed, `self.live`
    /// stays `None`), never swallowed.
    fn drain_to_idle(&mut self, live: Live<'static>) -> Result<(), DriverError> {
        // Thread the capture adapter through the reclaim: a NOTIFY riding the
        // drained remainder (a colossal result's tail on an early break, or the
        // recovery window after a server error) is buffered — or shed-counted at
        // overflow — never silently dropped. The reclaim reads real wire bytes
        // exactly like any other verb, so its sink captures notifications too.
        let polled = engine::poll_once(self.engine.drain(
            live,
            capture_notify(&mut self.notifications, |_s: Surface<'_>| ControlFlow::Continue(())),
        ));
        match polled {
            // The drain reached a clean idle — its own status is irrelevant (even a
            // second recoverable server error means the connection is back at idle
            // and reusable), so only the token matters. Restore it.
            Ok(Ok(Outcome { live, .. })) => {
                self.live = Some(live);
                Ok(())
            }
            Ok(Err(other)) => Err(lift_engine_error(other)),
            Err(SpuriousPending) => Err(DriverError::SpuriousPending),
        }
    }

    /// Prepare, query, and close a runtime SQL statement with params.
    pub fn query_params<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        let stmt = self.prepare(sql)?;
        let result = self.query_prepared(&stmt, params);
        // Always attempt the CLOSE so the statement is released. The primary op
        // error dominates: if `result` is Err, `result?` returns it and the
        // CLOSE Result is dropped; a CLOSE failure surfaces only when the primary
        // op SUCCEEDED.
        let close = self.close_statement(stmt);
        let result = result?;
        close?;
        Ok(result)
    }

    /// Like [`query_params`](Self::query_params), returning the first row.
    pub fn query_params_one<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<Row, DriverError> {
        self.query_params(sql, params)?
            .rows
            .into_iter()
            .next()
            .ok_or(DriverError::NoRows)
    }

    /// Like [`query_params`](Self::query_params), returning the first row if any.
    pub fn query_params_opt<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<Option<Row>, DriverError> {
        Ok(self.query_params(sql, params)?.rows.into_iter().next())
    }

    /// Prepare, execute, and close a runtime SQL statement with params.
    pub fn execute_params<P: ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<u64, DriverError> {
        let stmt = self.prepare(sql)?;
        let result = self.execute_prepared(&stmt, params);
        let close = self.close_statement(stmt);
        let count = result?;
        close?;
        Ok(count)
    }

    /// Close a prepared statement, consuming it (use-after-close is a move
    /// error).
    pub fn close_statement(&mut self, stmt: PreparedStatement) -> Result<(), DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.close_statement(live, stmt.inner, capture_notify(&mut self.notifications, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        })));
        self.settle(polled, &mut collector)
    }

    /// Feed one typed-verb execution to the N+1 detector (diagnostics-only).
    /// Records nothing that steers control flow — the query result is computed
    /// independently, so a spurious record can never change behaviour.
    #[cfg(feature = "n1-detect")]
    fn n1_record(&mut self, sql: &'static str, caller: &'static core::panic::Location<'static>) {
        self.n1_tracker.record(sql, caller);
    }

    /// The N+1 anti-patterns detected on this connection so far — one entry per
    /// `(query, source line)` site that ran more times than the detector's
    /// threshold within a single logical operation.
    ///
    /// Present ONLY under the `n1-detect` feature. Purely diagnostic: the driver
    /// builds this ledger as a side effect of running the typed verbs
    /// ([`query`](Self::query), [`query_one`](Self::query_one),
    /// [`query_each`](Self::query_each), [`execute`](Self::execute)) and never
    /// acts on it, so enabling detection cannot change what any query returns.
    /// The recency window is cleared at each logical-operation boundary
    /// (commit/rollback, [`reset_session`](Self::reset_session)) so repetition
    /// ACROSS operations is forgiven; the returned ledger itself persists for the
    /// connection's life.
    #[cfg(feature = "n1-detect")]
    #[must_use]
    pub fn n1_report(&self) -> &[bsql_postgres_core::N1Report] {
        self.n1_tracker.report()
    }

    /// `BEGIN` a transaction.
    pub fn begin(&mut self) -> Result<(), DriverError> {
        self.simple_query("BEGIN")?;
        Ok(())
    }

    /// `COMMIT` the current transaction.
    pub fn commit(&mut self) -> Result<(), DriverError> {
        self.simple_query("COMMIT")?;
        // A committed transaction is a logical-operation boundary: forget the
        // N+1 recency window so a query repeated in the NEXT operation is not
        // conflated with this one. Diagnostics-only.
        #[cfg(feature = "n1-detect")]
        self.n1_tracker.reset();
        Ok(())
    }

    /// `ROLLBACK` the current transaction.
    pub fn rollback(&mut self) -> Result<(), DriverError> {
        self.simple_query("ROLLBACK")?;
        #[cfg(feature = "n1-detect")]
        self.n1_tracker.reset();
        Ok(())
    }

    /// Run `f` inside a transaction: `COMMIT` on `Ok`, `ROLLBACK` on `Err`.
    ///
    /// Tier-1 safety: the transaction boundary is the closure scope, so there is
    /// no object to leak. On a body error the caller's error dominates and a
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
                // Best-effort rollback; the outcome rides the liveness token, so
                // it is explicitly discarded. The caller's error `e` dominates.
                drop(self.simple_query("ROLLBACK"));
                Err(e)
            }
        };
        // Either terminator closes a logical operation: forget the N+1 recency
        // window so the next operation starts fresh. Diagnostics-only.
        #[cfg(feature = "n1-detect")]
        self.n1_tracker.reset();
        result
    }

    /// Subscribe to a `LISTEN` channel.
    ///
    /// The channel name is interpolated into `LISTEN <channel>`, so it is
    /// validated as an unquoted identifier BEFORE interpolation — an
    /// injection-shaped name is a classified [`DriverError::Config`], never
    /// spliced into SQL.
    pub fn listen(&mut self, channel: &str) -> Result<(), DriverError> {
        sql_ident::validate_identifier(channel)?;
        let channel = Ident::try_from_str(channel)
            .map_err(|_| DriverError::Config("invalid channel name"))?;
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.listen(live, &channel, capture_notify(&mut self.notifications, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        })));
        self.settle(polled, &mut collector)
    }

    /// Unsubscribe from a `LISTEN` channel.
    ///
    /// The channel name is interpolated into `UNLISTEN <channel>`, so it is
    /// validated as an unquoted identifier BEFORE interpolation — an
    /// injection-shaped name is a classified [`DriverError::Config`], never
    /// spliced into SQL.
    pub fn unlisten(&mut self, channel: &str) -> Result<(), DriverError> {
        sql_ident::validate_identifier(channel)?;
        let channel = Ident::try_from_str(channel)
            .map_err(|_| DriverError::Config("invalid channel name"))?;
        self.simple_query(&format!("UNLISTEN {}", channel.as_str()))?;
        Ok(())
    }

    /// Reset all BLEEDABLE session state so this connection can be safely reused
    /// by a different logical user, WITHOUT dropping prepared statements.
    ///
    /// Clears, in one simple-query round trip:
    /// - session GUCs incl. `search_path` (`RESET ALL`),
    /// - the session authorization / role (`SET SESSION AUTHORIZATION DEFAULT`),
    /// - open cursors / portals (`CLOSE ALL`),
    /// - `LISTEN` subscriptions (`UNLISTEN *`),
    /// - held advisory locks (`pg_advisory_unlock_all`),
    /// - temporary tables (`DISCARD TEMP`),
    /// - cached sequence state (`DISCARD SEQUENCES`).
    ///
    /// A leading `ROLLBACK` is issued ONLY when the connection is inside a
    /// transaction — decided from the transaction status cached on the last
    /// `ReadyForQuery`, so it costs no extra round trip and emits no
    /// "no transaction in progress" notice on the common idle path — so a
    /// connection returned mid-transaction is aborted rather than leaking an
    /// open or failed transaction (locks, uncommitted rows) to the next user.
    ///
    /// # Prepared statements are deliberately KEPT
    ///
    /// The command set is `DISCARD ALL` MINUS `DEALLOCATE ALL` and
    /// `DISCARD PLANS`. Prepared statements are content-addressed query plans,
    /// safe to share across logical users; keeping them preserves the
    /// server-side plan reuse across pool checkouts (a repeat of the same typed
    /// query on a reused connection stays a bare Bind + Execute, never a
    /// re-Parse — and never a `42P05` duplicate-prepared-statement error). Because
    /// no statement is dropped, the per-connection statement cache stays
    /// consistent with the server's prepared-statement set with NO cache
    /// invalidation — this method does NOT clear it. (A hypothetical
    /// `DISCARD ALL` reset WOULD drop the server's statements and so would
    /// require the cache be cleared in lockstep to avoid a later Bind to a
    /// missing statement.)
    ///
    /// # Errors
    ///
    /// Any transport / server error is returned classified. The connection pool
    /// evicts a connection whose reset failed rather than handing out an
    /// un-reset (still-dirty) one.
    pub fn reset_session(&mut self) -> Result<(), DriverError> {
        // The targeted reset: DISCARD ALL minus DEALLOCATE ALL / DISCARD PLANS,
        // so prepared statements AND their cached plans survive.
        const RESET: &str = "SET SESSION AUTHORIZATION DEFAULT; RESET ALL; CLOSE ALL; \
             UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES";
        // Prefixed with ROLLBACK for the in-transaction case (RESET ALL etc. would
        // otherwise run inside — or be rejected by — the open/failed transaction).
        const RESET_WITH_ROLLBACK: &str = "ROLLBACK; SET SESSION AUTHORIZATION DEFAULT; RESET ALL; \
             CLOSE ALL; UNLISTEN *; SELECT pg_advisory_unlock_all(); DISCARD TEMP; DISCARD SEQUENCES";
        let sql = if matches!(self.engine.tx_status(), Ok(TxStatus::Idle)) {
            RESET
        } else {
            RESET_WITH_ROLLBACK
        };
        self.simple_query(sql)?;
        // Clear the notification ledger AFTER the reset round trip: `UNLISTEN *`
        // (above) stops new notifications, and this discards every notification
        // captured before or during the reset — so a pooled connection never
        // delivers a prior user's notifications to the next. Done last so a
        // notification that rode the reset's own response stream is cleared too.
        self.notifications.clear();
        // A pool session reset is the strongest logical-operation boundary: the
        // connection is about to serve a new logical user, so forget the N+1
        // recency window (the accumulated report ledger persists). Diagnostics-only.
        #[cfg(feature = "n1-detect")]
        self.n1_tracker.reset();
        Ok(())
    }

    /// Wait up to `timeout` for the next asynchronous notification.
    ///
    /// Drains the per-connection notification ledger FIRST: a notification that
    /// already arrived — captured by any earlier verb whose response stream it
    /// rode, or by a prior wait — returns immediately with NO round trip. Only
    /// when the ledger is empty does this wait on the socket.
    ///
    /// Returns `Ok(None)` if the deadline passes with no notification (the
    /// connection stays alive). The wait is bounded by setting the socket read
    /// timeout on the control handle; a read-timeout on the engine's reads then
    /// surfaces inside the engine (via [`Transport::is_would_block`]) as the
    /// [`NotifyStatus::Quiet`] outcome — the token rides back in `Ok`, so the
    /// connection stays alive with no separate reclaim.
    ///
    /// # Errors
    ///
    /// A malformed or non-UTF-8 buffered notification surfaces here as a
    /// classified [`DriverError`] (it is still removed from the ledger, so it
    /// cannot wedge the buffer) — never a silent drop.
    ///
    /// [`Transport::is_would_block`]: bsql_postgres_proto::engine::Transport::is_would_block
    pub fn recv_notification(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<Notification>, DriverError> {
        // An already-arrived notification returns without touching the socket.
        if let Some(buffered) = self.notifications.drain_one() {
            return buffered.map(Some);
        }
        // Arm the fallible read timeout BEFORE taking the linear token, so a
        // failed `set_read_timeout` syscall (e.g. the OS rejects a zero
        // Duration) returns Err with the token still in `self.live` — never
        // stranding it and bricking a connection nothing touched on the wire.
        // Mirrors the validate-fallible-input-before-take_live discipline of
        // every other verb.
        // No socket (an in-memory testkit connection) → no timeout to arm; the
        // fake never blocks, so the wait is vacuous.
        if let Some(ctl) = &self.socket_ctl {
            ctl.set_read_timeout(Some(timeout)).map_err(DriverError::Io)?;
        }
        let live = self.take_live()?;
        let ledger = &mut self.notifications;
        let polled = engine::poll_once(self.engine.recv_notification(live, |s| {
            if let Surface::Notify(body) = s {
                // Capture into the ledger (the same buffer every verb feeds),
                // then stop the pump — the notification is what we waited for.
                ledger.capture(body);
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }));
        // Disarm the bounded read deadline before classifying, so subsequent
        // verbs block indefinitely again (the steady-state I/O contract) — the
        // notification wait's deadline is scoped to this call alone, and a
        // notification result is not lost to a disarm failure. A socketless
        // testkit connection has nothing to restore.
        let restore = match &self.socket_ctl {
            Some(ctl) => ctl.set_read_timeout(None),
            None => Ok(()),
        };
        match polled {
            Ok(Ok(Outcome { live, status })) => {
                // Alive on either status — the would-block deadline is the Quiet
                // outcome, handled inside the engine, so the token rides back.
                self.live = Some(live);
                restore.map_err(DriverError::Io)?;
                match status {
                    // `Received` means the sink broke on a `Notify`, so the ledger
                    // holds it; drain it front-first. An empty ledger here is a
                    // classified inconsistency, never a silent `None`.
                    NotifyStatus::Received => match self.notifications.drain_one() {
                        Some(res) => res.map(Some),
                        None => Err(DriverError::UnclassifiedFailure),
                    },
                    NotifyStatus::Quiet => Ok(None),
                }
            }
            Ok(Err(other)) => {
                restore.map_err(DriverError::Io)?;
                Err(lift_engine_error(other))
            }
            Err(SpuriousPending) => {
                restore.map_err(DriverError::Io)?;
                Err(DriverError::SpuriousPending)
            }
        }
    }

    /// The count of asynchronous notifications currently buffered in the ledger
    /// and awaiting [`recv_notification`](Self::recv_notification).
    #[must_use]
    pub fn buffered_notifications(&self) -> usize {
        self.notifications.len()
    }

    /// The total number of asynchronous notifications ever captured on this
    /// connection (buffered, drained, or shed) — monotonic. Compare successive
    /// reads to detect a gap.
    #[must_use]
    pub fn notifications_received(&self) -> u64 {
        self.notifications.received()
    }

    /// The number of asynchronous notifications SHED because the bounded ledger
    /// was full — monotonic. Non-zero means notifications were lost to the bound;
    /// the loss is LOUD (visible here), never a silent drop.
    #[must_use]
    pub fn notifications_shed(&self) -> u64 {
        self.notifications.shed()
    }

    /// Wait up to `timeout` for the next notification, parsing its payload into an
    /// application type `T` — the typed peer of
    /// [`recv_notification`](Self::recv_notification).
    ///
    /// A subscriber `LISTEN`s to a channel, then reads decoded values: the raw
    /// payload string is parsed via `T`'s [`FromStr`](core::str::FromStr). Any
    /// `T: FromStr` works (a std scalar, or a consumer's own enum) — dep-free, no
    /// serialization framework. Use the untyped
    /// [`recv_notification`](Self::recv_notification) when the payload is a plain
    /// string. This is channel-agnostic: it types whatever notification arrives,
    /// so a subscriber that wants a single type per channel `LISTEN`s to one
    /// channel per connection.
    ///
    /// # Errors
    ///
    /// [`DriverError::PayloadParse`] (carrying the raw payload) if the payload does
    /// not parse into `T` — a LOUD classified error, never a silently-dropped or
    /// defaulted notification. The notification is still removed from the ledger.
    /// Other errors as [`recv_notification`](Self::recv_notification).
    pub fn recv_notification_as<T: FromStr>(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<TypedNotification<T>>, DriverError> {
        match self.recv_notification(timeout)? {
            Some(n) => match n.payload.parse::<T>() {
                Ok(payload) => Ok(Some(TypedNotification { channel: n.channel, payload, pid: n.pid })),
                // The payload string moves into the classified error — the failure
                // is inspectable, never swallowed.
                Err(_) => Err(DriverError::PayloadParse(n.payload)),
            },
            None => Ok(None),
        }
    }

    /// `COPY <table> FROM STDIN`, bulk-loading `rows_data` in CONSTANT memory —
    /// the ergonomic batch form of [`copy_in_with`](Self::copy_in_with).
    ///
    /// Each item is streamed as one text-`COPY` row (its bytes + a `\n`) through
    /// the lending writer, so the rows are NOT pre-collected: a lazy iterator of
    /// millions of rows loads without materialising them all. Returns the
    /// server's affected-row count (`COPY n`).
    ///
    /// `COPY` has no parameterized form for the target table, so `table` is
    /// interpolated into the SQL. It is validated as an unquoted identifier
    /// (optionally `schema.table`) BEFORE interpolation — an injection-shaped
    /// string is a classified [`DriverError::Config`], never spliced into SQL.
    ///
    /// # Errors
    ///
    /// A row rejected by the server (a bad value, a constraint violation) is a
    /// classified [`DriverError::Db`], and the connection RECOVERS to a clean
    /// idle (it stays pooled). A transport fault is fatal.
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
    /// `f` may interleave arbitrary work between rows and `write_chunk` /
    /// `write_row` each stream one `CopyData` frame flushed to the socket —
    /// nothing accumulates, so any row count loads in bounded memory.
    ///
    /// # Cancellation and recovery
    ///
    /// The terminal step is owned here, not by the writer's `Drop`:
    ///
    /// - `f` returns `Ok(())` → `CopyDone`; the server's `COPY n` count is
    ///   returned and the connection stays clean and pooled.
    /// - `f` returns `Err(e)` (the caller abandoned the copy mid-stream) →
    ///   `CopyFail`; the server tears the COPY down and the verb drains the
    ///   recovering `ReadyForQuery`, so the connection is RECOVERABLE (a
    ///   subsequent query works). The caller's `e` is returned.
    ///
    /// `table` is validated as an identifier (see [`copy_in`](Self::copy_in)).
    ///
    /// # Errors
    ///
    /// A server rejection at `CopyDone` (a bad row / constraint) is a recoverable
    /// [`DriverError::Db`]; `f`'s own error is returned verbatim; a transport
    /// fault is fatal.
    pub fn copy_in_with<F>(&mut self, table: &str, f: F) -> Result<u64, DriverError>
    where
        F: FnOnce(&mut CopyInWriter<'_>) -> Result<(), DriverError>,
    {
        sql_ident::validate_table(table)?;
        let sql = format!("COPY {table} FROM STDIN");
        let live = self.take_live()?;
        // A transport fault while issuing the COPY command is fatal: the token is
        // dropped (not restored), so the connection is dead — never swallowed.
        match engine::poll_once(self.engine.copy_in_begin(&sql)) {
            Ok(Ok(())) => {}
            Ok(Err(other)) => return Err(lift_engine_error(other)),
            Err(SpuriousPending) => return Err(DriverError::SpuriousPending),
        }
        let body = {
            let mut writer = CopyInWriter {
                engine: &mut self.engine,
                scratch: Vec::new(),
            };
            f(&mut writer)
            // `writer` is dropped here, releasing the `&mut self.engine` borrow.
        };
        match body {
            Ok(()) => {
                let mut collector = ResultCollector::new();
                let polled = engine::poll_once(self.engine.copy_in_finish(live, capture_notify(&mut self.notifications, |s| {
                    collector.feed(s);
                    ControlFlow::Continue(())
                })));
                // `settle` restores the token on either status and maps a server
                // rejection (a bad row surfaced at `CopyDone`) to `DriverError::Db`
                // with the connection kept pooled.
                self.settle(polled, &mut collector)?;
                Ok(collector.affected())
            }
            Err(e) => {
                // The caller abandoned the copy: send `CopyFail` to reclaim the
                // connection. The server ALWAYS answers `CopyFail` with an
                // `ErrorResponse` + `ReadyForQuery`, so the abort's `ServerErrored`
                // status is EXPECTED, not a fault — the token is restored and the
                // caller's `e` dominates. A transport fault during the abort means
                // the socket is truly broken; the token stays gone (connection
                // dead) and `e` still dominates.
                if let Ok(Ok(Outcome { live, .. })) =
                    engine::poll_once(self.engine.copy_in_abort(
                        live,
                        b"client aborted COPY",
                        capture_notify(&mut self.notifications, |_s: Surface<'_>| ControlFlow::Continue(())),
                    ))
                {
                    self.live = Some(live);
                }
                Err(e)
            }
        }
    }

    /// `COPY <table> TO STDOUT`, streaming each row to `on_chunk` in CONSTANT
    /// memory — the bulk-unload peer of [`copy_in`](Self::copy_in).
    ///
    /// Each server `CopyData` frame (one text-`COPY` row, `\n`-terminated) is
    /// handed to `on_chunk` as a borrowed slice into the transient ingest buffer;
    /// nothing is accumulated, so a colossal unload streams without growing
    /// memory. The borrowed chunk CANNOT escape the closure (the `for<'q>` on the
    /// bound is the compiler-enforced escape wall).
    ///
    /// `on_chunk` returns [`ControlFlow`]: [`Continue`](ControlFlow::Continue) to
    /// keep streaming, or [`Break(e)`](ControlFlow::Break) to stop early.
    ///
    /// `table` is validated as an identifier (see [`copy_in`](Self::copy_in)).
    ///
    /// # Returns
    ///
    /// - `Ok(None)` — the unload streamed to completion; the connection is clean
    ///   and pooled.
    /// - `Ok(Some(e))` — `on_chunk` returned [`Break(e)`](ControlFlow::Break); the
    ///   stream stopped early and the connection was drained back to a clean idle,
    ///   so it stays healthy and reusable.
    /// - `Err(DriverError::Db(..))` — the server reported an error mid-unload; the
    ///   connection was drained and stays healthy.
    /// - other `Err` — a fatal transport/protocol fault; the connection is dead.
    ///
    /// # Early-abort cost
    ///
    /// A [`Break`](ControlFlow::Break) still READS (and discards) the remaining
    /// `CopyData` to reach the clean idle that makes the connection reusable —
    /// O(remaining rows).
    pub fn copy_out<F, E>(
        &mut self,
        table: &str,
        mut on_chunk: F,
    ) -> Result<Option<E>, DriverError>
    where
        F: for<'q> FnMut(&'q [u8]) -> ControlFlow<E>,
    {
        sql_ident::validate_table(table)?;
        let sql = format!("COPY {table} TO STDOUT");
        let live = self.take_live()?;
        // Captured across the streaming sink; read after the verb settles.
        let mut db_error: Option<DbError> = None;
        let polled = engine::poll_once(self.engine.copy_out(live, &sql, capture_notify(&mut self.notifications, |surface| match surface {
            // The chunk borrows the transient ingest buffer; `on_chunk` consumes
            // it in-scope (the `for<'q>` wall forbids escape).
            Surface::CopyData(body) => on_chunk(body),
            // Capture the server error's cause, then let the pump reach its
            // `Failed` boundary so the connection can be drained to idle.
            Surface::Fail(body) => {
                db_error = Some(materialize::parse_error_response(body));
                ControlFlow::Continue(())
            }
            // `CopyDone`, the `COPY n` `Deliver`, and other async frames are not
            // unload rows (a NOTIFY is captured into the ledger by the wrapper
            // above this match, so it never reaches here to be dropped).
            _ => ControlFlow::Continue(()),
        })));

        // The token rides `Ok` on any ALIVE boundary; a fatal is `Err`.
        let (live, boundary) = match polled {
            Ok(Ok(Outcome { live, status })) => (live, status),
            Ok(Err(other)) => return Err(lift_engine_error(other)),
            Err(SpuriousPending) => return Err(DriverError::SpuriousPending),
        };
        match boundary {
            Boundary::Idle => {
                self.live = Some(live);
                Ok(None)
            }
            Boundary::Failed => {
                self.drain_to_idle(live)?;
                match db_error {
                    Some(db) => Err(DriverError::Db(Box::new(db))),
                    None => Err(DriverError::UnclassifiedFailure),
                }
            }
            Boundary::Stopped(e) => {
                self.drain_to_idle(live)?;
                Ok(Some(e))
            }
            // `copy_out` maps Closed/Suspended to a fatal `Err`, so they never ride
            // an `Ok` outcome; `Boundary` is `#[non_exhaustive]`, so this classified
            // arm also covers any future boundary. The token is dropped, so the
            // connection is left dead + evictable.
            _ => Err(DriverError::Io(io::Error::other(
                "unexpected protocol boundary from a streaming COPY OUT",
            ))),
        }
    }

    /// Whether the connection is at a clean boundary and reusable.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.live.is_some()
    }

    /// The server version reported at connect, if recovered.
    #[must_use]
    pub fn server_version(&self) -> Option<&str> {
        self.params.server_version.as_deref()
    }

    /// The backend process id from `BackendKeyData`.
    #[must_use]
    pub fn backend_pid(&self) -> i32 {
        self.params.backend_pid
    }

    /// Whether this connection's traffic is TLS-encrypted.
    ///
    /// `true` iff the TLS handshake completed — `SslMode::Require`, or
    /// `SslMode::Prefer` where the server accepted SSL. `false` for a plaintext
    /// connection, INCLUDING a `SslMode::Prefer` connection the server
    /// downgraded to plain TCP (the downgrade also emits a stderr warning). A
    /// consumer over an untrusted network can ASSERT this after connect to
    /// reject a silent downgrade:
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
        self.encrypted
    }

    /// Gracefully close the connection (`Terminate` + shutdown). Idempotent.
    pub fn close(&mut self) -> Result<(), DriverError> {
        match self.live.take() {
            Some(live) => flatten_poll(engine::poll_once(self.engine.terminate(live))),
            None => Ok(()),
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(live) = self.live.take() {
            // Best-effort graceful terminate over the blocking socket (resolves
            // in one poll). Drop cannot propagate the outcome and a closed socket
            // is fine, so it is explicitly discarded.
            drop(engine::poll_once(self.engine.terminate(live)));
        }
    }
}

/// Flatten a single-poll outcome to the driver error surface (the fatal-only
/// path: connect and close, where there is no recoverable-error retry).
fn flatten_poll<T>(polled: Polled<T>) -> Result<T, DriverError> {
    match polled {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(lift_engine_error(e)),
        Err(SpuriousPending) => Err(DriverError::SpuriousPending),
    }
}

/// Lift a FATAL [`EngineError`] over the wire transport to a classified
/// [`DriverError`]. A recoverable server error never reaches here — the verb
/// returns it as [`CommandStatus::ServerErrored`] inside `Ok`, which
/// [`settle`](Connection::settle) maps to `DriverError::Db` — so the
/// `ServerError` arm is the fatal drain-during-recovery case alone.
fn lift_engine_error(e: EngineError<WireError>) -> DriverError {
    match e {
        EngineError::Transport(t) => lift_tls_error(t),
        EngineError::Handshake(cf) => lift_conn_fail(cf),
        EngineError::WrongPhase(_) => DriverError::NotReady,
        EngineError::UnexpectedEof => {
            DriverError::Io(io::Error::other("server closed the connection"))
        }
        EngineError::ServerError => DriverError::UnclassifiedFailure,
        EngineError::ProtocolViolation => {
            DriverError::Io(io::Error::other("protocol violation; connection torn down"))
        }
        // `EngineError` is `#[non_exhaustive]`; the remaining framing/flush
        // faults (WriteZero / SendOverrun / IngestFull / IngestCommitOverflow /
        // UnexpectedSuspend / RowCount / FrameTooLong / SpuriousPending) surface
        // as classified I/O carrying the engine's own detail.
        other => DriverError::Io(io::Error::other(format!("engine error: {other:?}"))),
    }
}

/// Classify an [`io::Error`] from the connect-phase `SSLRequest` probe — the raw
/// `write_all` / `read_exact` on the bare socket BEFORE the wire is built.
///
/// A read/write deadline (the armed connect-phase `connect_timeout` firing on a
/// server that never answers the probe) surfaces as [`WouldBlock`]/[`TimedOut`]
/// and is a connect-phase timeout, mapped to [`DriverError::Timeout`] — the SAME
/// class the async driver's connect budget and the post-probe TLS handshake
/// ([`lift_tls_error`]) use, so the two drivers agree for a connect-phase
/// timeout. Every OTHER io error (e.g. a connection reset) keeps its real class
/// as [`DriverError::Io`]; only the timeout is remapped.
///
/// [`WouldBlock`]: io::ErrorKind::WouldBlock
/// [`TimedOut`]: io::ErrorKind::TimedOut
fn lift_probe_io(e: io::Error) -> DriverError {
    match e.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => DriverError::Timeout,
        _ => DriverError::Io(e),
    }
}

/// Lift a wire transport error to a [`DriverError`].
fn lift_tls_error(e: WireError) -> DriverError {
    match e {
        TlsError::Socket(io) => match io.kind() {
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => DriverError::Timeout,
            _ => DriverError::Io(io),
        },
        // Preserve the TLS error verbatim as the source of a classified I/O error.
        other => DriverError::Io(io::Error::other(other)),
    }
}

/// Lift a custom-CA-roots build failure to a classified [`DriverError::Config`]
/// (the crate's pre-connect-validation class) — fail-closed: a bad or empty CA
/// PEM aborts the connect, never a silent fallback to the default roots.
fn lift_ca_roots_error(e: CaRootsError) -> DriverError {
    match e {
        CaRootsError::NoCertificates => {
            DriverError::Config("custom CA roots (with_ca_roots/sslrootcert) contained no certificate")
        }
        CaRootsError::MalformedPem(_) => {
            DriverError::Config("custom CA roots PEM is malformed")
        }
        CaRootsError::InvalidCertificate(_) => {
            DriverError::Config("a custom CA certificate is not a valid trust anchor")
        }
        CaRootsError::ProtocolVersions(_) => {
            DriverError::Config("TLS provider advertised no usable protocol versions")
        }
        // `CaRootsError` is `#[non_exhaustive]`; a future rejection class must
        // still fail CLOSED (a classified Config error), never a silent fallback.
        _ => DriverError::Config("custom CA roots could not be used"),
    }
}

/// Lift a handshake failure to a [`DriverError`].
fn lift_conn_fail(cf: ConnFail) -> DriverError {
    match cf {
        ConnFail::UnsupportedAuthMethod => {
            DriverError::Config("server requested an unsupported authentication method")
        }
        ConnFail::ServerError => {
            DriverError::Io(io::Error::other("server returned an error during startup"))
        }
        // `ConnFail` is `#[non_exhaustive]`; the malformed-frame / SCRAM /
        // overflow causes surface as I/O carrying the classified detail.
        other => DriverError::Io(io::Error::other(format!("handshake failed: {other:?}"))),
    }
}
