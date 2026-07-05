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
use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::Instant;

use bsql_postgres_core::driver::{
    lift_ca_roots_error, lift_conn_fail, lift_engine_error, lift_tls_error, Core,
};
use bsql_postgres_core::sql_ident;
use bsql_postgres_core::ssl::SslProbe;
use bsql_postgres_core::tls::{self, TlsTransport, Wire};
use bsql_postgres_core::{
    resolve_endpoint, validate_startup_params, ConnectConfig, DriverError, Endpoint, Notification,
    QueryResult, Row, Rows, SslMode, TypedNotification,
};
use bsql_postgres_proto::engine;
use bsql_postgres_proto::params::ParamsWriter;
use bsql_postgres_proto::{
    Credentials, DatabaseName, Ident, Password, PreparedQuery, RowDecode, Sensitive, TypedQuery,
};

use crate::transport::{ReadDeadline, Sock, TokioSocket};

/// The prepared-statement handle (defined once in `bsql-postgres-core`, shared by
/// both drivers). Re-exported so `bsql_postgres_async::PreparedStatement` still
/// resolves.
pub use bsql_postgres_core::PreparedStatement;

/// The plaintext-or-TLS transport the engine is monomorphic over.
type AsyncWire = Wire<TokioSocket>;

/// A lending `COPY … FROM STDIN` writer, handed to the closure of
/// [`copy_in_with`](Connection::copy_in_with).
///
/// Borrows the connection's [`Core`] for the copy's duration (so no other verb
/// can run concurrently), and streams each row/chunk as one `CopyData` frame
/// flushed straight to the socket — nothing is buffered, so a bulk load of
/// millions of rows runs in CONSTANT memory (0 heap growth per row; one reused
/// scratch buffer for [`write_row`](Self::write_row)'s trailing newline).
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
    /// Stream one `CopyData` frame with `chunk` as its verbatim body, flushed to
    /// the socket. Zero-copy: the bytes are queued directly (no reframing). For
    /// text `COPY`, `chunk` is raw copy-format bytes — the caller controls row
    /// boundaries and framing.
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
        // Bound the ENTIRE connect sequence under ONE `connect_timeout` budget,
        // measured from the start. On elapse tokio drops the in-flight future;
        // nothing is stranded, since no `Connection` (and no reusable liveness
        // token) exists yet.
        let budget = Duration::from_secs(config.connect_timeout_secs);
        match tokio::time::timeout(budget, Self::connect_inner(config)).await {
            Ok(result) => result,
            // The same class the blocking driver surfaces for a connect-phase
            // (handshake) timeout, so the two drivers agree.
            Err(_elapsed) => Err(DriverError::Timeout),
        }
    }

    /// The connect sequence proper — run UNDER the `connect_timeout` budget by
    /// [`connect`](Self::connect).
    async fn connect_inner(config: &ConnectConfig) -> Result<Self, DriverError> {
        // The read-deadline cell shared with the socket the engine will own.
        let read_deadline = Arc::new(ReadDeadline::new());
        // Dial the chosen transport (TCP or unix) and build the wire. No
        // dial-only timeout: the caller's single outer budget bounds the whole
        // sequence, so a black-hole dial elapses into `DriverError::Timeout`
        // exactly like a silent handshake.
        let wire = Self::connect_wire(config, &read_deadline).await?;
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
        let live = engine.connect(live).await.map_err(lift_engine_error)?;
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
            core: Core::new(engine, live, encrypted, server_version, backend_pid),
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
    async fn connect_wire(
        config: &ConnectConfig,
        deadline: &Arc<ReadDeadline>,
    ) -> Result<AsyncWire, DriverError> {
        match resolve_endpoint(&config.host, config.port) {
            Endpoint::Tcp(addr) => {
                let tcp = TcpStream::connect(&addr).await?;
                // Disable Nagle on the data socket for the connection's whole life
                // — Nagle + delayed-ACK can add ~40ms stalls to small writes and
                // COPY-in streaming; one setsockopt with zero per-op cost.
                tcp.set_nodelay(true)?;
                Self::build_tcp_wire(tcp, config, deadline).await
            }
            Endpoint::Unix(path) => {
                // Fail LOUD: TLS cannot be required over a socket that will never
                // do it. A local kernel socket is trusted by filesystem
                // permissions, not TLS, and PostgreSQL does not offer TLS there.
                if config.ssl_mode == SslMode::Require {
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
        }
    }

    /// Build the plaintext or TLS wire over an already-connected TCP socket,
    /// performing the PG `SSLRequest` negotiation on the raw socket when SSL is
    /// wanted.
    async fn build_tcp_wire(
        tcp: TcpStream,
        config: &ConnectConfig,
        deadline: &Arc<ReadDeadline>,
    ) -> Result<AsyncWire, DriverError> {
        if config.ssl_mode == SslMode::Disable {
            return Ok(Wire::Plain(TokioSocket::new(
                Sock::Tcp(tcp),
                Arc::clone(deadline),
            )));
        }
        let ssl_bytes = bsql_postgres_core::ssl::ssl_request_bytes();
        let mut tcp = tcp;
        tcp.write_all(ssl_bytes).await?;
        let mut response = [0u8; 1];
        tcp.read_exact(&mut response).await?;
        match bsql_postgres_core::ssl::classify_ssl_response(response[0], config)? {
            SslProbe::Accepted { server_name } => {
                // Use the provider-explicit ring config (the workspace pins rustls
                // to ring only). Custom CA roots build a config verified against
                // EXACTLY those roots; otherwise the shared default-roots config. A
                // bad/empty custom PEM is a classified `Config` error — fail-closed.
                let cfg = match config.ca_roots_pem() {
                    Some(pem) => {
                        tls::client_config_with_ca_roots(pem).map_err(lift_ca_roots_error)?
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
        // cell satisfies the struct invariant.
        let read_deadline = Arc::new(ReadDeadline::new());
        let user = Ident::try_from_str("bsql_testkit")
            .map_err(|_| DriverError::Config("invalid testkit user name"))?;
        let (mut engine, live) = engine::open_owned(wire, &user, None, &[], Credentials::Trust)
            .map_err(lift_conn_fail)?;
        let live = engine.connect(live).await.map_err(lift_engine_error)?;
        let backend_pid = engine.backend_pid().map_err(|_| DriverError::NotReady)?;
        let server_version = engine
            .server_version()
            .map_err(|_| DriverError::NotReady)?
            .map(str::to_owned);
        Ok(Self {
            core: Core::new(engine, live, encrypted, server_version, backend_pid),
            read_deadline,
        })
    }

    // ── Delegated runtime-SQL verbs ─────────────────────────────────────────
    //
    // Each RETURNS the shared `Core` verb's future directly (a `fn -> impl Future`
    // forwarder, no wrapping `async` block) so there is no extra forwarder
    // state-machine layer — the awaited future is `Core`'s own, monomorphised over
    // `TokioSocket`, byte-for-byte the work the driver did inline before. The bare
    // RPIT leaks the future's `Send` (which the pool relies on), so no `+ Send` is
    // added. `.await` call sites are unaffected.

    /// Round-trip a `Sync` to confirm the connection is live.
    pub fn ping(&mut self) -> impl Future<Output = Result<(), DriverError>> + '_ {
        self.core.ping()
    }

    /// Issue a simple query, returning the command tag string.
    pub fn simple_query<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<String, DriverError>> + 'a {
        self.core.simple_query(sql)
    }

    /// Execute a non-row runtime-SQL command, returning the affected-row count.
    /// The compile-checked counterpart is [`execute`](Self::execute).
    pub fn execute_sql<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<u64, DriverError>> + 'a {
        self.core.execute_sql(sql)
    }

    /// Run a row-returning runtime-SQL query (text result columns). The
    /// compile-checked, typed counterpart is [`query`](Self::query).
    pub fn query_sql<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<QueryResult, DriverError>> + 'a {
        self.core.query_sql(sql)
    }

    /// Run a runtime-SQL query returning the first row, or [`DriverError::NoRows`].
    /// The compile-checked counterpart is [`query_one`](Self::query_one).
    pub fn query_one_sql<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<Row, DriverError>> + 'a {
        self.core.query_one_sql(sql)
    }

    /// Run a runtime-SQL query returning the first row if any.
    pub fn query_opt<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> impl Future<Output = Result<Option<Row>, DriverError>> + 'a {
        self.core.query_opt(sql)
    }

    /// Prepare a statement: `Parse` + `Describe` + `Sync`, recovering the result
    /// schema for later `Bind`+`Execute`.
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

    /// Prepare, query, and close a runtime SQL statement with params.
    pub fn query_params<'a, P: ParamsWriter>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
    ) -> impl Future<Output = Result<QueryResult, DriverError>> + 'a {
        self.core.query_params(sql, params)
    }

    /// Like [`query_params`](Self::query_params), returning the first row.
    pub fn query_params_one<'a, P: ParamsWriter>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
    ) -> impl Future<Output = Result<Row, DriverError>> + 'a {
        self.core.query_params_one(sql, params)
    }

    /// Like [`query_params`](Self::query_params), returning the first row if any.
    pub fn query_params_opt<'a, P: ParamsWriter>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
    ) -> impl Future<Output = Result<Option<Row>, DriverError>> + 'a {
        self.core.query_params_opt(sql, params)
    }

    /// Prepare, execute, and close a runtime SQL statement with params.
    pub fn execute_params<'a, P: ParamsWriter>(
        &'a mut self,
        sql: &'a str,
        params: &'a P,
    ) -> impl Future<Output = Result<u64, DriverError>> + 'a {
        self.core.execute_params(sql, params)
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
    /// the schema at build time, params are bound in binary. An oversize row is a
    /// classified [`DriverError::OversizeRow`], never a silent truncation. The
    /// statement is Parsed once per connection and the server-side plan reused
    /// thereafter (safe across pool checkouts and transactions). The runtime-SQL
    /// escape hatch is [`query_sql`](Self::query_sql).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query<'a, Q: TypedQuery + 'a>(
        &'a mut self,
        params: Q::Params,
    ) -> impl core::future::Future<Output = Result<Rows<Q>, DriverError>> + 'a
    where
        Q::Params: 'a,
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
        params: Q::Params,
    ) -> impl core::future::Future<Output = Result<Q::Owned, DriverError>> + 'a
    where
        Q::Params: 'a,
    {
        self.core.query_one::<Q>(
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
    /// - `Err(DriverError::OversizeRow)` — a row exceeded the inline buffer and
    ///   streamed in chunks the bounded decoder cannot reassemble; rows before it
    ///   were delivered, then the classified error (never a silent truncation).
    /// - other `Err` — a fatal transport/protocol fault; the connection is dead.
    ///
    /// # Early-abort cost
    ///
    /// A [`Break`](ControlFlow::Break) of a colossal result still READS (and
    /// discards) the remaining rows to reach the clean idle boundary that makes
    /// the connection reusable — O(remaining rows).
    #[cfg_attr(feature = "n1-detect", track_caller)]
    pub fn query_each<'a, Q, F, E>(
        &'a mut self,
        params: Q::Params,
        on_row: F,
    ) -> impl core::future::Future<Output = Result<Option<E>, DriverError>> + 'a
    where
        Q: TypedQuery + 'a,
        Q::Params: 'a,
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

    // ── Transaction / session boundary primitives ───────────────────────────

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
    /// `f` is an async closure borrowing `&mut Self`, so the body can run any
    /// sequence of the connection's async verbs and the transaction holds no
    /// object to leak — the boundary is the call scope. There is no `Drop`-based
    /// async guard (`Drop` cannot `.await`): the async closure form is the
    /// cancellation-correct shape — if the returned future is dropped mid-body, no
    /// `COMMIT` runs and the server rolls back when the socket later closes.
    pub async fn transaction<R, F>(&mut self, f: F) -> Result<R, DriverError>
    where
        F: AsyncFnOnce(&mut Self) -> Result<R, DriverError>,
    {
        self.core.simple_query("BEGIN").await?;
        let result = match f(self).await {
            Ok(value) => {
                self.core.simple_query("COMMIT").await?;
                Ok(value)
            }
            Err(e) => {
                // Best-effort rollback; the outcome rides the liveness token, so it
                // is explicitly discarded. The caller's error `e` dominates.
                drop(self.core.simple_query("ROLLBACK").await);
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
    /// # Errors
    ///
    /// Any transport / server error is returned classified; a pool evicts a
    /// connection whose reset failed rather than handing out a still-dirty one.
    pub fn reset_session(&mut self) -> impl Future<Output = Result<(), DriverError>> + '_ {
        self.core.reset_session()
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
        self.read_deadline.arm(deadline);
        let received = self.core.recv_notification_inner().await;
        // Disarm before draining, so a later verb's reads are deadline-free. The
        // disarm is infallible (an atomic store), so — unlike the blocking driver's
        // socket-timeout restore — there is no restore error to thread.
        self.read_deadline.disarm();
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
                // The payload string moves into the classified error.
                Err(_) => Err(DriverError::PayloadParse(n.payload)),
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
    /// `write_row` each stream one `CopyData` frame flushed to the socket — nothing
    /// accumulates, so any row count loads in bounded memory.
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
        sql_ident::validate_table(table)?;
        let sql = format!("COPY {table} FROM STDIN");
        // Take the token and issue the COPY command. On a fault the token is
        // dropped by `Core` — the connection is dead.
        let live = self.core.copy_in_begin(&sql).await?;
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
}
