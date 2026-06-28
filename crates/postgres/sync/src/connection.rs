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
//! and reusable, `None` = a verb failed fatally and the connection is dead. A
//! *recoverable* server error (a query-level `ErrorResponse`) is NOT fatal: the
//! verb consumes the token but the connection survives, so [`settle`] drains the
//! recovering `ReadyForQuery` and reclaims a fresh token via
//! [`Engine::recover`], keeping `self.live` `Some` while returning the parsed
//! [`DbError`]. A transport/protocol/EOF failure leaves `self.live` `None`.
//!
//! [`settle`]: Connection::settle

use core::ops::ControlFlow;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use bsql_postgres_core::materialize::{self, ResultCollector};
use bsql_postgres_core::tls::{self, TlsError, TlsTransport};
use bsql_postgres_core::{
    ConnectConfig, DriverError, Notification, QueryResult, Row, SslMode,
};
use bsql_postgres_proto::engine::{
    self, ConnFail, Engine, EngineError, Live, NoObserver, PreparedStatement as WireStatement,
    SpuriousPending, Surface,
};
use bsql_postgres_proto::params::ParamsWriter;
use bsql_postgres_proto::{
    Credentials, DatabaseName, Ident, Password, PreparedQuery, RowDecode, Sensitive, StmtName,
};

use crate::transport::{SyncSocket, Wire};

/// The plaintext-or-TLS transport the engine is monomorphic over.
type SyncWire = Wire<SyncSocket>;
/// The arm-uniform transport error: a plaintext socket error rides
/// [`TlsError::Socket`]; the TLS arm's error already is this type.
type WireError = TlsError<io::Error>;
/// The owned, poolable engine handle (branded `'static`).
type SyncEngine = Engine<'static, SyncWire, NoObserver>;
/// Result of a single-poll verb drive, before classification.
type Polled<T> = Result<Result<T, EngineError<WireError>>, SpuriousPending>;

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
    /// A `try_clone` of the underlying socket, used ONLY to set the read timeout
    /// for [`recv_notification`](Self::recv_notification): the engine owns the
    /// I/O socket, but a dup'd handle shares the same kernel socket, so a
    /// timeout set here applies to the engine's reads too.
    socket_ctl: TcpStream,
    /// The steady-state read timeout, restored after a notification wait.
    read_timeout: Duration,
    params: SessionParams,
    stmt_counter: u32,
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
        let read_timeout = Duration::from_secs(config.connect_timeout_secs);
        tcp.set_read_timeout(Some(read_timeout))?;
        tcp.set_write_timeout(Some(read_timeout))?;
        // The dup'd control handle shares the kernel socket, so a timeout set on
        // it applies to the engine's reads. Taken before the socket is moved into
        // the wire / TLS layer.
        let socket_ctl = tcp.try_clone()?;

        let wire = Self::build_wire(tcp, config)?;

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

        let (mut engine, live) =
            engine::open_owned(wire, &user, database.as_ref(), None, credentials)
                .map_err(lift_conn_fail)?;
        let live = flatten_poll(engine::poll_once(engine.connect(live)))?;
        let backend_pid = engine.backend_pid().map_err(|_| DriverError::NotReady)?;

        let mut conn = Self {
            engine,
            live: Some(live),
            socket_ctl,
            read_timeout,
            params: SessionParams {
                server_version: None,
                backend_pid,
            },
            stmt_counter: 0,
        };
        // The new engine drops the startup `ParameterStatus` frames (the connect
        // pump surfaces them to nothing), so `server_version` is recovered with a
        // one-round-trip `SHOW` rather than carried from the handshake.
        conn.params.server_version = conn.fetch_server_version()?;
        Ok(conn)
    }

    /// Build the plaintext or TLS wire, performing the PG `SSLRequest`
    /// negotiation on the raw socket when SSL is wanted.
    fn build_wire(tcp: TcpStream, config: &ConnectConfig) -> Result<SyncWire, DriverError> {
        if config.ssl_mode == SslMode::Disable {
            return Ok(Wire::Plain(SyncSocket::new(tcp)));
        }
        let (ssl_bytes, ssl_proto) = bsql_postgres_core::ssl::ssl_request_bytes();
        let mut tcp = tcp;
        Write::write_all(&mut tcp, ssl_bytes)?;
        let mut response = [0u8; 1];
        Read::read_exact(&mut tcp, &mut response)?;
        match bsql_postgres_core::ssl::classify_ssl_response(ssl_proto, response[0], config)? {
            bsql_postgres_core::ssl::SslProbe::Accepted { server_name, .. } => {
                // Use the provider-explicit ring config, NOT the probe's bare
                // `ClientConfig::builder()` tls_config: under the ring-only crypto
                // pin the bare builder installs no default provider and would
                // fault at the handshake. The server name comes from the probe;
                // the config comes from `shared_client_config`.
                let cfg = tls::shared_client_config()
                    .map_err(|e| DriverError::Io(io::Error::other(format!("TLS config: {e}"))))?;
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

    /// Classify a verb's single-poll outcome and manage the token.
    ///
    /// On success the token is restored. On a recoverable server error the
    /// connection is drained to its recovering boundary and a fresh token
    /// reclaimed (so the connection stays usable), returning the parsed
    /// [`DbError`]. On a fatal error the token stays gone (`self.live == None`),
    /// so [`is_healthy`](Self::is_healthy) reports the connection dead.
    fn settle(&mut self, polled: Polled<Live<'static>>, collector: &mut ResultCollector) -> Result<(), DriverError> {
        match polled {
            Ok(Ok(live)) => {
                self.live = Some(live);
                Ok(())
            }
            Ok(Err(EngineError::ServerError)) => {
                // The pump surfaced the raw `ErrorResponse` to the sink before the
                // failure boundary, so the collector holds the parsed cause.
                let err = match collector.take_db_error() {
                    Some(db) => DriverError::Db(db),
                    None => DriverError::UnclassifiedFailure,
                };
                self.reclaim_token();
                Err(err)
            }
            Ok(Err(other)) => Err(lift_engine_error(other)),
            Err(SpuriousPending) => Err(DriverError::SpuriousPending),
        }
    }

    /// Reclaim the token after a recoverable failure: drain the recovering
    /// `ReadyForQuery` (or mint immediately when already clean-idle). On a
    /// recovery failure the connection stays dead (`self.live` stays `None`).
    fn reclaim_token(&mut self) {
        // On a recovery failure `self.live` stays `None` — the correct dead-state
        // outcome, encoded in the health bit rather than swallowed.
        if let Ok(Ok(live)) = engine::poll_once(self.engine.recover()) {
            self.live = Some(live);
        }
    }

    /// Generate a fresh, unique prepared-statement name.
    fn next_stmt_name(&mut self) -> Result<StmtName, DriverError> {
        let id = self.stmt_counter;
        self.stmt_counter = self.stmt_counter.wrapping_add(1);
        StmtName::try_from_str(&format!("_bsql_{id}"))
            .map_err(|_| DriverError::Config("generated statement name invalid"))
    }

    /// Recover the server version with a one-round-trip `SHOW server_version`.
    fn fetch_server_version(&mut self) -> Result<Option<String>, DriverError> {
        let result = self.query("SHOW server_version")?;
        Ok(match result.rows.first() {
            Some(row) => row.get_str(0).map(String::from),
            None => None,
        })
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
        let polled = engine::poll_once(self.engine.ping(live, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        }));
        self.settle(polled, &mut collector)
    }

    /// Issue a simple query, returning the command tag string.
    pub fn simple_query(&mut self, sql: &str) -> Result<String, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.simple_query(live, sql, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        }));
        self.settle(polled, &mut collector)?;
        Ok(collector.command_tag().to_string())
    }

    /// Execute a non-row command, returning the affected-row count.
    pub fn execute(&mut self, sql: &str) -> Result<u64, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.execute(live, sql, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        }));
        self.settle(polled, &mut collector)?;
        Ok(collector.affected())
    }

    /// Run a row-returning query (text result columns).
    pub fn query(&mut self, sql: &str) -> Result<QueryResult, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.query(live, sql, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        }));
        self.settle(polled, &mut collector)?;
        Self::build_query_result(collector, None)
    }

    /// Run a query returning the first row, or [`DriverError::NoRows`].
    pub fn query_one(&mut self, sql: &str) -> Result<Row, DriverError> {
        self.query(sql)?.rows.into_iter().next().ok_or(DriverError::NoRows)
    }

    /// Run a query returning the first row if any.
    pub fn query_opt(&mut self, sql: &str) -> Result<Option<Row>, DriverError> {
        Ok(self.query(sql)?.rows.into_iter().next())
    }

    /// Prepare a statement: `Parse` + `Describe` + `Sync`, recovering the result
    /// schema for later `Bind`+`Execute`.
    pub fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, DriverError> {
        let stmt_name = self.next_stmt_name()?;
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.prepare(live, &stmt_name, sql, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        }));
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
    /// The `Copy` bound is vacuous: every `ParamsWriter` is a tuple of `Copy`
    /// scalars / `&str` / slices, so this excludes no caller; it lets the
    /// borrowed params be passed to the engine by value with no clone.
    pub fn query_prepared<P: ParamsWriter + Copy>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.query_prepared(live, &stmt.inner, *params, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        }));
        self.settle(polled, &mut collector)?;
        Self::build_query_result(collector, Some(stmt.column_names.clone()))
    }

    /// Execute a prepared statement for its side effect, returning the affected
    /// count. See [`query_prepared`](Self::query_prepared) on the `Copy` bound.
    pub fn execute_prepared<P: ParamsWriter + Copy>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<u64, DriverError> {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.execute_prepared(live, &stmt.inner, *params, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        }));
        self.settle(polled, &mut collector)?;
        Ok(collector.affected())
    }

    /// Execute a `prepared!` macro query in one Parse+Bind+Execute+Sync round
    /// trip (binary-uniform params and results), returning the affected count.
    pub fn execute_prepared_macro<P, R>(
        &mut self,
        q: &'static PreparedQuery<P, R>,
        params: P,
    ) -> Result<u64, DriverError>
    where
        P: ParamsWriter + 'static,
        R: RowDecode + 'static,
    {
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.query_params(live, q, params, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        }));
        self.settle(polled, &mut collector)?;
        Ok(collector.affected())
    }

    /// Prepare, query, and close a runtime SQL statement with params.
    pub fn query_params<P: ParamsWriter + Copy>(
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
    pub fn query_params_one<P: ParamsWriter + Copy>(
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
    pub fn query_params_opt<P: ParamsWriter + Copy>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<Option<Row>, DriverError> {
        Ok(self.query_params(sql, params)?.rows.into_iter().next())
    }

    /// Prepare, execute, and close a runtime SQL statement with params.
    pub fn execute_params<P: ParamsWriter + Copy>(
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
        let polled = engine::poll_once(self.engine.close_statement(live, stmt.inner, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        }));
        self.settle(polled, &mut collector)
    }

    /// `BEGIN` a transaction.
    pub fn begin(&mut self) -> Result<(), DriverError> {
        self.simple_query("BEGIN")?;
        Ok(())
    }

    /// `COMMIT` the current transaction.
    pub fn commit(&mut self) -> Result<(), DriverError> {
        self.simple_query("COMMIT")?;
        Ok(())
    }

    /// `ROLLBACK` the current transaction.
    pub fn rollback(&mut self) -> Result<(), DriverError> {
        self.simple_query("ROLLBACK")?;
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
        match f(self) {
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
        }
    }

    /// Subscribe to a `LISTEN` channel (the name is validated as an identifier,
    /// so it cannot inject SQL).
    pub fn listen(&mut self, channel: &str) -> Result<(), DriverError> {
        let channel = Ident::try_from_str(channel)
            .map_err(|_| DriverError::Config("invalid channel name"))?;
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let polled = engine::poll_once(self.engine.listen(live, &channel, |s| {
            collector.feed(s);
            ControlFlow::Continue(())
        }));
        self.settle(polled, &mut collector)
    }

    /// Unsubscribe from a `LISTEN` channel (validated, no injection).
    pub fn unlisten(&mut self, channel: &str) -> Result<(), DriverError> {
        let channel = Ident::try_from_str(channel)
            .map_err(|_| DriverError::Config("invalid channel name"))?;
        self.simple_query(&format!("UNLISTEN {}", channel.as_str()))?;
        Ok(())
    }

    /// Wait up to `timeout` for the next asynchronous notification.
    ///
    /// Returns `Ok(None)` if the deadline passes with no notification (the
    /// connection stays alive). The wait is bounded by setting the socket read
    /// timeout on the control handle; a read-timeout on the engine's reads then
    /// surfaces as a transport `WouldBlock`/`TimedOut`, classified here as "no
    /// notification" and the token reclaimed.
    pub fn recv_notification(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<Notification>, DriverError> {
        let live = self.take_live()?;
        self.socket_ctl
            .set_read_timeout(Some(timeout))
            .map_err(DriverError::Io)?;
        let mut captured: Option<Result<Notification, DriverError>> = None;
        let polled = engine::poll_once(self.engine.recv_notification(live, |s| {
            if let Surface::Notify(body) = s {
                captured = Some(materialize::parse_notification(body));
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }));
        // Restore the steady-state timeout for subsequent verbs before
        // classifying, so a notification result is not lost to a restore failure.
        let restore = self.socket_ctl.set_read_timeout(Some(self.read_timeout));
        match polled {
            Ok(Ok(live)) => {
                self.live = Some(live);
                restore.map_err(DriverError::Io)?;
                match captured {
                    Some(Ok(notification)) => Ok(Some(notification)),
                    Some(Err(e)) => Err(e),
                    None => Ok(None),
                }
            }
            Ok(Err(EngineError::Transport(e))) if is_timeout(&e) => {
                // No notification within the deadline; the read timed out before
                // consuming anything, so the connection is alive — reclaim the
                // token (a clean-idle reclaim mints without further I/O).
                self.reclaim_token();
                restore.map_err(DriverError::Io)?;
                Ok(None)
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

    /// `COPY <table> FROM STDIN`, streaming each row as a `CopyData` chunk.
    pub fn copy_in(
        &mut self,
        table: &str,
        rows_data: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<u64, DriverError> {
        let sql = format!("COPY {table} FROM STDIN");
        // Materialise each row + a trailing newline into an owned store the data
        // closure yields slices from: the engine's `data` callback borrows for
        // one fixed lifetime, so the rows must outlive the call. One allocation
        // per row (matching the prior driver); the engine flushes each chunk, so
        // the send buffer stays bounded.
        let store: Vec<Vec<u8>> = rows_data
            .into_iter()
            .map(|row| {
                let mut bytes = row.as_ref().as_bytes().to_vec();
                bytes.push(b'\n');
                bytes
            })
            .collect();
        let live = self.take_live()?;
        let mut collector = ResultCollector::new();
        let mut rows = store.iter();
        let polled = engine::poll_once(self.engine.copy_in(
            live,
            &sql,
            || rows.next().map(Vec::as_slice),
            |s| {
                collector.feed(s);
                ControlFlow::Continue(())
            },
        ));
        self.settle(polled, &mut collector)?;
        Ok(collector.affected())
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

/// Is `e` a read-timeout (a deadline, not a broken connection)?
fn is_timeout(e: &WireError) -> bool {
    matches!(
        e,
        TlsError::Socket(io)
            if matches!(io.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)
    )
}

/// Lift an [`EngineError`] over the wire transport to a classified
/// [`DriverError`]. The recoverable `ServerError` is handled by [`settle`] and
/// is the dead arm here.
///
/// [`settle`]: Connection::settle
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
