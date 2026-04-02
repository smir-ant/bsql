//! PostgreSQL connection — startup, authentication, statement cache, query execution.
//!
//! `Connection` owns a TCP (or TLS) stream and implements the extended query protocol
//! with pipelining. Statements are cached by rapidhash of the SQL text. On first use,
//! Parse+Describe+Bind+Execute+Sync are pipelined in one TCP write. On subsequent uses,
//! only Bind+Execute+Sync are sent.

use std::collections::HashMap;
use std::sync::Arc;

use rapidhash::quality::RapidHasher;
use std::hash::Hasher;

use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::DriverError;
use crate::arena::Arena;
use crate::auth;
use crate::codec::Encode;
use crate::proto::{self, BackendMessage};

#[cfg(feature = "tls")]
use crate::tls;

// --- Stream abstraction ---

/// The underlying stream type — either plain TCP or TLS.
enum Stream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl Stream {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.write_all(buf).await,
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.write_all(buf).await,
        }
    }

    #[cfg(feature = "tls")]
    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush().await,
            Stream::Tls(s) => s.flush().await,
        }
    }
}

/// Wrapper to implement AsyncRead for Stream.
struct StreamReader<'a>(&'a mut Stream);

impl AsyncRead for StreamReader<'_> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self.0 {
            Stream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

// --- Config ---

/// Connection configuration parsed from a URL.
///
/// Format: `postgres://user:password@host:port/database`
#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub ssl: SslMode,
    /// PG-side statement timeout in seconds. Default: 30. 0 = no timeout.
    ///
    /// After connecting, the driver sends `SET statement_timeout = '{N}s'`.
    /// If a query exceeds this duration, PostgreSQL kills it and returns an error.
    pub statement_timeout_secs: u32,
}

/// SSL/TLS connection mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    /// Never use TLS.
    Disable,
    /// Try TLS, fall back to plain if server says 'N'.
    Prefer,
    /// Require TLS, fail if server says 'N'.
    Require,
}

impl Config {
    /// Parse a PostgreSQL connection URL.
    ///
    /// Format: `postgres://user:password@host:port/database?sslmode=prefer`
    pub fn from_url(url: &str) -> Result<Self, DriverError> {
        let url = url
            .strip_prefix("postgres://")
            .or_else(|| url.strip_prefix("postgresql://"))
            .ok_or_else(|| DriverError::Protocol("URL must start with postgres://".into()))?;

        // Split user:password@host:port/database
        let (userinfo, rest) = url
            .split_once('@')
            .ok_or_else(|| DriverError::Protocol("missing @ in connection URL".into()))?;

        let (user, password) = userinfo.split_once(':').unwrap_or((userinfo, ""));

        // Split host:port/database?params
        let (hostport, rest) = rest.split_once('/').unwrap_or((rest, ""));
        let (database, params) = rest.split_once('?').unwrap_or((rest, ""));

        let (host, port) = if let Some((h, p)) = hostport.split_once(':') {
            let port = p
                .parse::<u16>()
                .map_err(|_| DriverError::Protocol(format!("invalid port: {p}")))?;
            (h.to_owned(), port)
        } else {
            (hostport.to_owned(), 5432)
        };

        let mut ssl = SslMode::Prefer;
        let mut statement_timeout_secs: u32 = 30;
        for param in params.split('&') {
            if let Some(val) = param.strip_prefix("sslmode=") {
                ssl = match val {
                    "disable" => SslMode::Disable,
                    "prefer" => SslMode::Prefer,
                    "require" => SslMode::Require,
                    _ => SslMode::Prefer,
                };
            } else if let Some(val) = param.strip_prefix("statement_timeout=") {
                statement_timeout_secs = val.parse::<u32>().unwrap_or(30);
            }
        }

        let config = Config {
            host: url_decode(&host),
            port,
            user: url_decode(user),
            password: url_decode(password),
            database: if database.is_empty() {
                url_decode(user)
            } else {
                url_decode(database)
            },
            ssl,
            statement_timeout_secs,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validate configuration fields before attempting a connection.
    ///
    /// Called automatically by `from_url()`. Call manually if constructing
    /// a `Config` by hand.
    pub fn validate(&self) -> Result<(), DriverError> {
        if self.host.is_empty() {
            return Err(DriverError::Protocol("host cannot be empty".into()));
        }
        if self.user.is_empty() {
            return Err(DriverError::Protocol("user cannot be empty".into()));
        }
        if self.database.is_empty() {
            return Err(DriverError::Protocol("database cannot be empty".into()));
        }
        Ok(())
    }
}

/// Minimal percent-decoding for connection URL components.
fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.as_bytes().iter();
    while let Some(&b) = chars.next() {
        if b == b'%' {
            let hi = chars.next().copied().unwrap_or(0);
            let lo = chars.next().copied().unwrap_or(0);
            let val = hex_val(hi) * 16 + hex_val(lo);
            result.push(val as char);
        } else {
            result.push(b as char);
        }
    }
    result
}

fn hex_val(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

/// Owned action from a startup message, avoiding borrow conflicts with `self.read_buf`.
enum StartupAction {
    AuthOk,
    AuthCleartext,
    AuthMd5([u8; 4]),
    AuthSasl(Vec<u8>),
    ParameterStatus(Box<str>, Box<str>),
    BackendKeyData(i32, i32),
    ReadyForQuery(u8),
    Error(String),
    Notice,
}

// --- Statement cache ---

/// Cached information about a prepared statement.
///
/// The statement name is a 64-bit rapidhash formatted as `"s_{hash:016x}"`.
/// With 2^64 possible values, collision probability is negligible for realistic
/// workloads (e.g., ~1 in 10^13 for 10,000 distinct queries). A collision would
/// cause a protocol error from PostgreSQL (parameter mismatch), not silent
/// data corruption. If you have an adversarial workload that could craft
/// collisions, consider a verified cache keyed on the full SQL text.
struct StmtInfo {
    /// Statement name: `"s_{hash:016x}"`
    name: Box<str>,
    /// Column metadata from RowDescription.
    columns: Arc<[ColumnDesc]>,
}

/// Description of a result column.
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    /// Column name from the query.
    pub name: Box<str>,
    /// PostgreSQL type OID.
    pub type_oid: u32,
    /// Type size in bytes (-1 for variable-length).
    pub type_size: i16,
}

// --- Connection ---

/// A PostgreSQL connection with statement cache and inline message processing.
///
/// Connections are not `Send` — they must be used on one task at a time. The pool
/// handles concurrent access by lending connections to individual tasks.
pub struct Connection {
    stream: Stream,
    /// Message payload buffer (re-used per message).
    read_buf: Vec<u8>,
    /// Buffered read: raw bytes from the TCP stream. We read 64KB chunks and
    /// parse messages from this buffer, issuing a new read only when exhausted.
    stream_buf: Vec<u8>,
    /// How many valid bytes are in `stream_buf[stream_buf_pos..]`.
    stream_buf_pos: usize,
    /// One past the last valid byte in `stream_buf`.
    stream_buf_end: usize,
    write_buf: Vec<u8>,
    stmts: HashMap<u64, StmtInfo>,
    params: HashMap<Box<str>, Box<str>>,
    pid: i32,
    secret: i32,
    tx_status: u8,
}

impl Connection {
    /// Connect to PostgreSQL and complete the startup/auth handshake.
    pub async fn connect(config: &Config) -> Result<Self, DriverError> {
        config.validate()?;

        let addr = format!("{}:{}", config.host, config.port);
        let tcp = TcpStream::connect(&addr).await.map_err(DriverError::Io)?;

        // Set TCP_NODELAY to avoid Nagle delay on pipelined messages
        tcp.set_nodelay(true).map_err(DriverError::Io)?;

        let stream = match config.ssl {
            SslMode::Disable => Stream::Plain(tcp),
            #[cfg(feature = "tls")]
            SslMode::Prefer | SslMode::Require => {
                match tls::try_upgrade(tcp, &config.host, config.ssl == SslMode::Require).await {
                    Ok(tls_stream) => Stream::Tls(tls_stream),
                    Err(e) if config.ssl == SslMode::Require => return Err(e),
                    Err(_) => {
                        // Prefer mode: TLS failed, reconnect plain
                        let tcp = TcpStream::connect(&addr).await.map_err(DriverError::Io)?;
                        tcp.set_nodelay(true).map_err(DriverError::Io)?;
                        Stream::Plain(tcp)
                    }
                }
            }
            #[cfg(not(feature = "tls"))]
            SslMode::Require => {
                return Err(DriverError::Protocol(
                    "TLS required but bsql-driver compiled without 'tls' feature".into(),
                ));
            }
            #[cfg(not(feature = "tls"))]
            SslMode::Prefer => Stream::Plain(tcp),
        };

        let mut conn = Self {
            stream,
            read_buf: Vec::with_capacity(8192),
            stream_buf: vec![0u8; 32768],
            stream_buf_pos: 0,
            stream_buf_end: 0,
            write_buf: Vec::with_capacity(4096),
            stmts: HashMap::new(),
            params: HashMap::new(),
            pid: 0,
            secret: 0,
            tx_status: b'I',
        };

        conn.startup(config).await?;

        if config.statement_timeout_secs > 0 {
            conn.simple_query(&format!(
                "SET statement_timeout = '{}s'",
                config.statement_timeout_secs
            ))
            .await?;
        }

        Ok(conn)
    }

    /// Perform the startup handshake: StartupMessage -> auth -> parameter status -> ReadyForQuery.
    ///
    /// Uses a two-phase read approach: first read the message type + copy needed
    /// data out of the borrow, then act on it. This avoids holding a borrow on
    /// `self.read_buf` while calling other `&mut self` methods.
    async fn startup(&mut self, config: &Config) -> Result<(), DriverError> {
        // Send StartupMessage
        self.write_buf.clear();
        proto::write_startup(&mut self.write_buf, &config.user, &config.database);
        self.flush_write().await?;

        // Process auth and startup messages
        loop {
            let action = self.read_startup_action().await?;
            match action {
                StartupAction::AuthOk => {}
                StartupAction::AuthCleartext => {
                    self.write_buf.clear();
                    let mut pw = config.password.as_bytes().to_vec();
                    pw.push(0);
                    proto::write_password(&mut self.write_buf, &pw);
                    self.flush_write().await?;
                }
                StartupAction::AuthMd5(salt) => {
                    self.write_buf.clear();
                    let hash = auth::md5_password(&config.user, &config.password, &salt);
                    proto::write_password(&mut self.write_buf, &hash);
                    self.flush_write().await?;
                }
                StartupAction::AuthSasl(mechanisms_data) => {
                    self.handle_scram(config, &mechanisms_data).await?;
                }
                StartupAction::ParameterStatus(name, value) => {
                    self.params.insert(name, value);
                }
                StartupAction::BackendKeyData(pid, secret) => {
                    self.pid = pid;
                    self.secret = secret;
                }
                StartupAction::ReadyForQuery(status) => {
                    self.tx_status = status;
                    return Ok(());
                }
                StartupAction::Error(msg) => {
                    return Err(DriverError::Auth(msg));
                }
                StartupAction::Notice => {}
            }
        }
    }

    /// Read one startup message, parse it, copy needed data, and return an owned action.
    ///
    /// This method reads the raw message into `self.read_buf`, parses it, extracts
    /// all needed data into owned types, and drops the borrow before returning.
    async fn read_startup_action(&mut self) -> Result<StartupAction, DriverError> {
        let (msg_type, _) = self.read_message_buffered().await?;
        self.read_startup_message_from_type(msg_type)
    }

    fn read_startup_message_from_type(&self, msg_type: u8) -> Result<StartupAction, DriverError> {
        let payload = &self.read_buf;
        let msg = proto::parse_backend_message(msg_type, payload)?;
        match msg {
            BackendMessage::AuthOk => Ok(StartupAction::AuthOk),
            BackendMessage::AuthCleartext => Ok(StartupAction::AuthCleartext),
            BackendMessage::AuthMd5 { salt } => Ok(StartupAction::AuthMd5(salt)),
            BackendMessage::AuthSasl { mechanisms } => {
                Ok(StartupAction::AuthSasl(mechanisms.to_vec()))
            }
            BackendMessage::ParameterStatus { name, value } => {
                Ok(StartupAction::ParameterStatus(name.into(), value.into()))
            }
            BackendMessage::BackendKeyData { pid, secret } => {
                Ok(StartupAction::BackendKeyData(pid, secret))
            }
            BackendMessage::ReadyForQuery { status } => Ok(StartupAction::ReadyForQuery(status)),
            BackendMessage::ErrorResponse { data } => {
                let fields = proto::parse_error_response(data);
                Ok(StartupAction::Error(fields.to_string()))
            }
            BackendMessage::NoticeResponse { .. } => Ok(StartupAction::Notice),
            other => Err(DriverError::Protocol(format!(
                "unexpected message during startup: {other:?}"
            ))),
        }
    }

    /// Handle SCRAM-SHA-256 authentication exchange.
    async fn handle_scram(
        &mut self,
        config: &Config,
        mechanisms_data: &[u8],
    ) -> Result<(), DriverError> {
        let mechs = auth::parse_sasl_mechanisms(mechanisms_data);
        if !mechs.contains(&"SCRAM-SHA-256") {
            return Err(DriverError::Auth(format!(
                "server requires unsupported SASL mechanism(s): {mechs:?}"
            )));
        }

        let mut scram = auth::ScramClient::new(&config.user, &config.password)?;

        // Send SASLInitialResponse
        let client_first = scram.client_first_message();
        self.write_buf.clear();
        proto::write_sasl_initial(&mut self.write_buf, "SCRAM-SHA-256", &client_first);
        self.flush_write().await?;

        // Read SASLContinue — read message, extract data, drop borrow
        let (msg_type, _) = self.read_message_buffered().await?;
        let server_first = {
            let msg = proto::parse_backend_message(msg_type, &self.read_buf)?;
            match msg {
                BackendMessage::AuthSaslContinue { data } => data.to_vec(),
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    return Err(DriverError::Auth(fields.to_string()));
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "expected AuthSaslContinue, got: {other:?}"
                    )));
                }
            }
        };

        scram.process_server_first(&server_first)?;

        // Send SASLResponse (client-final)
        let client_final = scram.client_final_message()?;
        self.write_buf.clear();
        proto::write_sasl_response(&mut self.write_buf, &client_final);
        self.flush_write().await?;

        // Read SASLFinal — read message, extract data, drop borrow
        let (msg_type, _) = self.read_message_buffered().await?;
        {
            let msg = proto::parse_backend_message(msg_type, &self.read_buf)?;
            match msg {
                BackendMessage::AuthSaslFinal { data } => {
                    // Copy server final data to verify after the borrow ends
                    let data_owned = data.to_vec();
                    scram.verify_server_final(&data_owned)?;
                }
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    return Err(DriverError::Auth(fields.to_string()));
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "expected AuthSaslFinal, got: {other:?}"
                    )));
                }
            }
        }

        // AuthOk should follow
        let (msg_type, _) = self.read_message_buffered().await?;
        let msg = proto::parse_backend_message(msg_type, &self.read_buf)?;
        match msg {
            BackendMessage::AuthOk => Ok(()),
            BackendMessage::ErrorResponse { data } => {
                let fields = proto::parse_error_response(data);
                Err(DriverError::Auth(fields.to_string()))
            }
            other => Err(DriverError::Protocol(format!(
                "expected AuthOk after SCRAM, got: {other:?}"
            ))),
        }
    }

    // --- Query execution ---

    /// Prepare a statement without executing it (Parse+Describe+Sync only).
    ///
    /// Used by connection warmup to pre-cache statements without executing them.
    /// If the statement is already cached, this is a no-op.
    pub async fn prepare_only(&mut self, sql: &str, sql_hash: u64) -> Result<(), DriverError> {
        if self.stmts.contains_key(&sql_hash) {
            return Ok(());
        }
        let name = format!("s_{sql_hash:016x}").into_boxed_str();
        self.write_buf.clear();
        proto::write_parse(&mut self.write_buf, &name, sql, &[]);
        proto::write_describe(&mut self.write_buf, b'S', &name);
        proto::write_sync(&mut self.write_buf);
        self.flush_write().await?;

        // Read ParseComplete
        self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))
            .await?;

        // Read ParameterDescription + RowDescription/NoData via existing helper
        let columns = self.read_column_description().await?;

        // ReadyForQuery
        self.expect_ready().await?;

        // Cache the statement
        self.stmts.insert(sql_hash, StmtInfo { name, columns });
        Ok(())
    }

    /// Begin a streaming query using the PG extended query protocol with
    /// `Execute(max_rows=chunk_size)`.
    ///
    /// Returns column metadata and puts the connection into streaming mode.
    /// The caller must repeatedly call `streaming_next_chunk()` until it returns
    /// `Ok(false)` (all rows consumed) before issuing any other query on this
    /// connection.
    ///
    /// Uses the unnamed portal `""` which stays open between Execute calls
    /// as long as Sync is NOT sent. We use Flush (not Sync) to force PG to
    /// send buffered output without destroying the portal. Sync is only sent
    /// after CommandComplete to cleanly end the query cycle.
    pub async fn query_streaming_start(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        chunk_size: i32,
    ) -> Result<(Arc<[ColumnDesc]>, bool), DriverError> {
        let cached = self.stmts.contains_key(&sql_hash);

        self.write_buf.clear();

        let new_name = if !cached {
            let name = format!("s_{sql_hash:016x}").into_boxed_str();
            let param_oids: Vec<u32> = params.iter().map(|p| p.type_oid()).collect();
            proto::write_parse(&mut self.write_buf, &name, sql, &param_oids);
            proto::write_describe(&mut self.write_buf, b'S', &name);
            proto::write_bind_params(&mut self.write_buf, "", &name, params);
            Some(name)
        } else {
            let name = &*self.stmts[&sql_hash].name;
            proto::write_bind_params(&mut self.write_buf, "", name, params);
            None
        };

        proto::write_execute(&mut self.write_buf, "", chunk_size);
        // Use Flush (not Sync!) to keep the portal alive between chunks.
        proto::write_flush(&mut self.write_buf);
        self.flush_write().await?;

        // Read responses for Parse+Describe if needed
        let columns = if let Some(stmt_name) = new_name {
            self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))
                .await?;
            let columns = self.read_column_description().await?;
            self.stmts.insert(
                sql_hash,
                StmtInfo {
                    name: stmt_name,
                    columns: columns.clone(),
                },
            );
            columns
        } else {
            self.stmts[&sql_hash].columns.clone()
        };

        // BindComplete
        self.expect_message(|m| matches!(m, BackendMessage::BindComplete))
            .await?;

        Ok((columns, false))
    }

    /// Read the next chunk of rows from an in-progress streaming query.
    ///
    /// Returns `Ok(true)` if more rows are available (PortalSuspended),
    /// `Ok(false)` when all rows have been consumed (CommandComplete).
    ///
    /// After CommandComplete, this method sends Sync and reads ReadyForQuery,
    /// returning the connection to a clean protocol state.
    pub async fn streaming_next_chunk(
        &mut self,
        arena: &mut Arena,
        all_col_offsets: &mut Vec<(usize, i32)>,
    ) -> Result<bool, DriverError> {
        all_col_offsets.clear();

        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::DataRow { data } => {
                    parse_data_row_flat(data, arena, all_col_offsets)?;
                }
                BackendMessage::PortalSuspended => {
                    // More rows available. The portal stays open because we
                    // used Flush (not Sync). The caller will call
                    // streaming_send_execute() to request the next chunk.
                    return Ok(true);
                }
                BackendMessage::CommandComplete { .. } => {
                    // All rows consumed. Send Sync to end the query cycle
                    // and read ReadyForQuery to restore clean state.
                    self.write_buf.clear();
                    proto::write_sync(&mut self.write_buf);
                    self.flush_write().await?;
                    self.expect_ready().await?;
                    self.shrink_buffers();
                    return Ok(false);
                }
                BackendMessage::EmptyQuery => {
                    self.write_buf.clear();
                    proto::write_sync(&mut self.write_buf);
                    self.flush_write().await?;
                    self.expect_ready().await?;
                    return Ok(false);
                }
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    // Send Sync to reset and drain to ReadyForQuery
                    self.write_buf.clear();
                    proto::write_sync(&mut self.write_buf);
                    self.flush_write().await?;
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                BackendMessage::NoticeResponse { .. }
                | BackendMessage::NotificationResponse { .. } => {}
                other => {
                    return Err(DriverError::Protocol(format!(
                        "unexpected message during streaming: {other:?}"
                    )));
                }
            }
        }
    }

    /// Send Execute+Flush for the next chunk of a streaming query.
    ///
    /// Must be called before `streaming_next_chunk()` on the 2nd and
    /// subsequent chunks (the first chunk's Execute is sent by
    /// `query_streaming_start`).
    ///
    /// Uses Flush (not Sync) to keep the unnamed portal alive.
    pub async fn streaming_send_execute(&mut self, chunk_size: i32) -> Result<(), DriverError> {
        self.write_buf.clear();
        proto::write_execute(&mut self.write_buf, "", chunk_size);
        proto::write_flush(&mut self.write_buf);
        self.flush_write().await
    }

    /// Execute a prepared query and return rows in arena-allocated storage.
    ///
    /// If the statement is not yet cached, Parse+Describe+Bind+Execute+Sync are
    /// pipelined in a single TCP write. On cache hit, only Bind+Execute+Sync are sent.
    pub async fn query(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        arena: &mut Arena,
    ) -> Result<QueryResult, DriverError> {
        let cached = self.stmts.contains_key(&sql_hash);

        // Build the pipelined message
        self.write_buf.clear();

        // Compute statement name once, reuse for write + cache insert.
        let new_name = if !cached {
            let name = format!("s_{sql_hash:016x}").into_boxed_str();
            let param_oids: Vec<u32> = params.iter().map(|p| p.type_oid()).collect();
            proto::write_parse(&mut self.write_buf, &name, sql, &param_oids);
            proto::write_describe(&mut self.write_buf, b'S', &name);
            proto::write_bind_params(&mut self.write_buf, "", &name, params);
            Some(name)
        } else {
            let name = &*self.stmts[&sql_hash].name;
            proto::write_bind_params(&mut self.write_buf, "", name, params);
            None
        };

        proto::write_execute(&mut self.write_buf, "", 0);
        proto::write_sync(&mut self.write_buf);
        self.flush_write().await?;

        // Read responses
        let columns = if let Some(stmt_name) = new_name {
            self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))
                .await?;

            let columns = self.read_column_description().await?;

            self.stmts.insert(
                sql_hash,
                StmtInfo {
                    name: stmt_name,
                    columns: columns.clone(),
                },
            );
            columns
        } else {
            self.stmts[&sql_hash].columns.clone()
        };

        // BindComplete
        self.expect_message(|m| matches!(m, BackendMessage::BindComplete))
            .await?;

        // Read DataRow messages and CommandComplete.
        // Flat column offsets: all rows' columns are stored contiguously in
        // `all_col_offsets`. Row N starts at index `N * num_cols`.
        let num_cols = columns.len();
        let mut all_col_offsets: Vec<(usize, i32)> = Vec::with_capacity(num_cols * 64);
        let mut affected_rows: u64 = 0;

        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::DataRow { data } => {
                    parse_data_row_flat(data, arena, &mut all_col_offsets)?;
                }
                BackendMessage::CommandComplete { tag } => {
                    affected_rows = proto::parse_command_tag(tag);
                    break;
                }
                BackendMessage::EmptyQuery => {
                    break;
                }
                BackendMessage::NoticeResponse { .. }
                | BackendMessage::NotificationResponse { .. } => {
                    // Async messages can arrive mid-query — skip them
                }
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "unexpected message during query: {other:?}"
                    )));
                }
            }
        }

        // ReadyForQuery
        self.expect_ready().await?;
        self.shrink_buffers();

        Ok(QueryResult {
            all_col_offsets,
            num_cols,
            columns,
            affected_rows,
        })
    }

    /// Read RowDescription / NoData after ParseComplete+Describe, handling
    /// ParameterDescription that precedes RowDescription for Describe Statement.
    async fn read_column_description(&mut self) -> Result<Arc<[ColumnDesc]>, DriverError> {
        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::RowDescription { data } => {
                    let col_infos = proto::parse_row_description(data)?;
                    return Ok(col_infos
                        .into_iter()
                        .map(|c| ColumnDesc {
                            name: c.name,
                            type_oid: c.type_oid,
                            type_size: c.type_size,
                        })
                        .collect::<Vec<_>>()
                        .into());
                }
                BackendMessage::ParameterDescription { .. } => {
                    // ParameterDescription precedes RowDescription — continue reading
                }
                BackendMessage::NoData => return Ok(Arc::from(Vec::new())),
                BackendMessage::NoticeResponse { .. }
                | BackendMessage::NotificationResponse { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "expected RowDescription/NoData after Parse, got: {other:?}"
                    )));
                }
            }
        }
    }

    /// Execute a query without result rows (INSERT/UPDATE/DELETE).
    ///
    /// Skips DataRow parsing entirely — only reads until CommandComplete.
    /// Does not allocate an Arena.
    pub async fn execute(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<u64, DriverError> {
        let cached = self.stmts.contains_key(&sql_hash);

        // Build the pipelined message
        self.write_buf.clear();

        let new_name = if !cached {
            let name = format!("s_{sql_hash:016x}").into_boxed_str();
            let param_oids: Vec<u32> = params.iter().map(|p| p.type_oid()).collect();
            proto::write_parse(&mut self.write_buf, &name, sql, &param_oids);
            proto::write_describe(&mut self.write_buf, b'S', &name);
            proto::write_bind_params(&mut self.write_buf, "", &name, params);
            Some(name)
        } else {
            let name = &*self.stmts[&sql_hash].name;
            proto::write_bind_params(&mut self.write_buf, "", name, params);
            None
        };
        proto::write_execute(&mut self.write_buf, "", 0);
        proto::write_sync(&mut self.write_buf);
        self.flush_write().await?;

        // Read responses
        if let Some(stmt_name) = new_name {
            self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))
                .await?;
            let columns = self.read_column_description().await?;
            self.stmts.insert(
                sql_hash,
                StmtInfo {
                    name: stmt_name,
                    columns,
                },
            );
        }

        self.expect_message(|m| matches!(m, BackendMessage::BindComplete))
            .await?;

        // Skip DataRow messages, read until CommandComplete
        let mut affected_rows: u64 = 0;
        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::DataRow { .. } => {
                    // execute() discards row data — no arena allocation
                }
                BackendMessage::CommandComplete { tag } => {
                    affected_rows = proto::parse_command_tag(tag);
                    break;
                }
                BackendMessage::EmptyQuery => break,
                BackendMessage::NoticeResponse { .. }
                | BackendMessage::NotificationResponse { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "unexpected message during execute: {other:?}"
                    )));
                }
            }
        }

        self.expect_ready().await?;
        self.shrink_buffers();
        Ok(affected_rows)
    }

    /// Simple query protocol — for non-prepared SQL (BEGIN, COMMIT, SET, etc.).
    ///
    /// Does not use the extended query protocol. Cannot have parameters.
    pub async fn simple_query(&mut self, sql: &str) -> Result<(), DriverError> {
        self.write_buf.clear();
        proto::write_simple_query(&mut self.write_buf, sql);
        self.flush_write().await?;

        // Read until ReadyForQuery
        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::ReadyForQuery { status } => {
                    self.tx_status = status;
                    return Ok(());
                }
                BackendMessage::CommandComplete { .. }
                | BackendMessage::RowDescription { .. }
                | BackendMessage::DataRow { .. }
                | BackendMessage::EmptyQuery
                | BackendMessage::NoticeResponse { .. }
                | BackendMessage::NotificationResponse { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                _ => {}
            }
        }
    }

    /// Block until a NotificationResponse arrives on this connection.
    ///
    /// Reads raw messages from the stream and skips everything except
    /// `NotificationResponse`. Returns the `(channel, payload)` pair.
    /// Used by the listener's background task to receive LISTEN/NOTIFY events.
    ///
    /// This method never returns `Ok` for non-notification messages -- it loops
    /// internally, discarding `ParameterStatus`, `NoticeResponse`, etc.
    pub async fn wait_for_notification(&mut self) -> Result<(String, String), DriverError> {
        loop {
            let (msg_type, _payload_len) = self.read_message_buffered().await?;
            let msg = proto::parse_backend_message(msg_type, &self.read_buf)?;
            match msg {
                BackendMessage::NotificationResponse {
                    channel, payload, ..
                } => {
                    return Ok((channel.to_owned(), payload.to_owned()));
                }
                BackendMessage::ParameterStatus { .. } | BackendMessage::NoticeResponse { .. } => {
                    continue;
                }
                _ => continue,
            }
        }
    }

    /// Send Terminate and close the connection.
    pub async fn close(mut self) -> Result<(), DriverError> {
        self.write_buf.clear();
        proto::write_terminate(&mut self.write_buf);
        // Best-effort flush — ignore errors since we're closing
        let _ = self.flush_write().await;
        Ok(())
    }

    /// Whether the connection is in an idle transaction state.
    pub fn is_idle(&self) -> bool {
        self.tx_status == b'I'
    }

    /// Whether the connection is in a transaction.
    pub fn is_in_transaction(&self) -> bool {
        self.tx_status == b'T'
    }

    /// Whether the connection is in a failed transaction.
    pub fn is_in_failed_transaction(&self) -> bool {
        self.tx_status == b'E'
    }

    /// Get a server parameter value (set during startup or via SET).
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.params.get(name).map(|s| &**s)
    }

    /// Backend process ID (for cancel requests).
    pub fn pid(&self) -> i32 {
        self.pid
    }

    // --- Internal helpers ---

    /// Reclaim memory if buffers grew beyond normal thresholds.
    ///
    /// Called after query()/execute() to prevent a single large result from
    /// permanently bloating the connection's buffers.
    fn shrink_buffers(&mut self) {
        if self.read_buf.capacity() > 64 * 1024 {
            self.read_buf = Vec::with_capacity(8192);
        }
        if self.write_buf.capacity() > 16 * 1024 {
            self.write_buf = Vec::with_capacity(4096);
        }
    }

    /// Read one backend message. The returned message borrows from `self.read_buf`.
    ///
    /// We need to use an index-based approach because the message borrows from
    /// `read_buf`, and we can't return a reference to it while `self` is borrowed.
    async fn read_one_message(&mut self) -> Result<BackendMessage<'_>, DriverError> {
        let (msg_type, _payload_len) = self.read_message_buffered().await?;
        proto::parse_backend_message(msg_type, &self.read_buf)
    }

    /// Read messages until we find one matching `pred`, erroring on ErrorResponse.
    ///
    /// On error, drains to ReadyForQuery so the connection remains usable.
    /// Skips NotificationResponse, NoticeResponse, and ParameterStatus — all
    /// of which PostgreSQL can send asynchronously at any time.
    async fn expect_message(
        &mut self,
        pred: impl Fn(&BackendMessage<'_>) -> bool,
    ) -> Result<(), DriverError> {
        loop {
            let msg = self.read_one_message().await?;
            if pred(&msg) {
                return Ok(());
            }
            match msg {
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                BackendMessage::NoticeResponse { .. }
                | BackendMessage::ParameterStatus { .. }
                | BackendMessage::NotificationResponse { .. } => {
                    // Asynchronous messages — skip them
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "unexpected message while waiting for expected type: {other:?}"
                    )));
                }
            }
        }
    }

    /// Read until ReadyForQuery. Skips NotificationResponse and other async messages.
    async fn expect_ready(&mut self) -> Result<(), DriverError> {
        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::ReadyForQuery { status } => {
                    self.tx_status = status;
                    return Ok(());
                }
                BackendMessage::NoticeResponse { .. }
                | BackendMessage::ParameterStatus { .. }
                | BackendMessage::NotificationResponse { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    // Continue draining until ReadyForQuery
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                _ => {}
            }
        }
    }

    /// Drain messages until ReadyForQuery (used after an error).
    /// Skips all intermediate messages including NotificationResponse.
    async fn drain_to_ready(&mut self) -> Result<(), DriverError> {
        loop {
            let msg = self.read_one_message().await?;
            if let BackendMessage::ReadyForQuery { status } = msg {
                self.tx_status = status;
                return Ok(());
            }
        }
    }

    /// Convert parsed ErrorFields into a DriverError::Server.
    fn make_server_error(&self, fields: proto::ErrorFields) -> DriverError {
        DriverError::Server {
            code: fields.code.into_boxed_str(),
            message: fields.message.into_boxed_str(),
            detail: fields.detail.map(String::into_boxed_str),
            hint: fields.hint.map(String::into_boxed_str),
        }
    }

    /// Flush the write buffer to the stream.
    ///
    /// For plain TCP with TCP_NODELAY, the OS sends data immediately on write_all,
    /// so explicit flush() is a no-op but still costs a syscall. We skip it.
    /// TLS streams buffer internally and require an explicit flush.
    async fn flush_write(&mut self) -> Result<(), DriverError> {
        self.stream
            .write_all(&self.write_buf)
            .await
            .map_err(DriverError::Io)?;
        match &self.stream {
            Stream::Plain(_) => {
                // TCP_NODELAY is set — write_all already pushes bytes to the wire.
                // No need for an extra flush syscall.
            }
            #[cfg(feature = "tls")]
            Stream::Tls(_) => {
                self.stream.flush().await.map_err(DriverError::Io)?;
            }
        }
        Ok(())
    }

    /// Read one complete backend message using the internal buffer.
    ///
    /// Returns `(msg_type, payload_len)`. The payload is stored in `self.read_buf`.
    async fn read_message_buffered(&mut self) -> Result<(u8, usize), DriverError> {
        // Read 5-byte header: type(1) + length(4)
        let mut header = [0u8; 5];
        buffered_read_exact(
            &mut self.stream,
            &mut self.stream_buf,
            &mut self.stream_buf_pos,
            &mut self.stream_buf_end,
            &mut header,
        )
        .await?;

        let msg_type = header[0];
        let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);

        if len < 4 {
            return Err(DriverError::Protocol(format!(
                "invalid message length {len} for type '{}'",
                msg_type as char
            )));
        }

        const MAX_MESSAGE_LEN: i32 = 128 * 1024 * 1024;
        if len > MAX_MESSAGE_LEN {
            return Err(DriverError::Protocol(format!(
                "message length {len} exceeds maximum ({MAX_MESSAGE_LEN}) for type '{}'",
                msg_type as char
            )));
        }

        let payload_len = (len - 4) as usize;
        self.read_buf.clear();
        self.read_buf.resize(payload_len, 0);
        if payload_len > 0 {
            buffered_read_exact(
                &mut self.stream,
                &mut self.stream_buf,
                &mut self.stream_buf_pos,
                &mut self.stream_buf_end,
                &mut self.read_buf[..payload_len],
            )
            .await?;
        }

        Ok((msg_type, payload_len))
    }
}

/// Read exactly `out.len()` bytes using a persistent read buffer.
///
/// This is a free function to avoid double-mutable-borrow issues when the caller
/// also needs to write into `self.read_buf`.
async fn buffered_read_exact(
    stream: &mut Stream,
    buf: &mut [u8],
    pos: &mut usize,
    end: &mut usize,
    out: &mut [u8],
) -> Result<(), DriverError> {
    let mut filled = 0;
    while filled < out.len() {
        let avail = *end - *pos;
        if avail > 0 {
            let take = avail.min(out.len() - filled);
            out[filled..filled + take].copy_from_slice(&buf[*pos..*pos + take]);
            *pos += take;
            filled += take;
        } else {
            // Buffer exhausted — refill from the stream
            *pos = 0;
            let n = {
                let mut reader = StreamReader(stream);
                use tokio::io::AsyncReadExt;
                reader.read(buf).await.map_err(DriverError::Io)?
            };
            if n == 0 {
                return Err(DriverError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                )));
            }
            *end = n;
        }
    }
    Ok(())
}

// --- QueryResult ---

/// Result of a query execution. Owns the row offset metadata.
///
/// Uses flat column offset storage: all rows' `(arena_offset, length)` pairs
/// are stored contiguously in `all_col_offsets`. Row N starts at index
/// `N * num_cols`. No separate `row_starts` Vec needed.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), bsql_driver::DriverError> {
/// # let mut conn: bsql_driver::Connection = todo!();
/// # let mut arena = bsql_driver::Arena::new();
/// let result = conn.query("SELECT 1 as n", 0, &[], &mut arena).await?;
/// for i in 0..result.len() {
///     let row = result.row(i, &arena);
///     // Access columns by index
/// }
/// # Ok(())
/// # }
/// ```
pub struct QueryResult {
    /// All rows' column (arena_offset, length) pairs, contiguous.
    /// length = -1 means NULL.
    all_col_offsets: Vec<(usize, i32)>,
    /// Number of columns per row.
    num_cols: usize,
    columns: Arc<[ColumnDesc]>,
    affected_rows: u64,
}

impl QueryResult {
    /// Construct a `QueryResult` from its constituent parts.
    ///
    /// Used by `bsql-core`'s streaming layer to assemble per-chunk results.
    pub fn from_parts(
        all_col_offsets: Vec<(usize, i32)>,
        num_cols: usize,
        columns: Arc<[ColumnDesc]>,
        affected_rows: u64,
    ) -> Self {
        Self {
            all_col_offsets,
            num_cols,
            columns,
            affected_rows,
        }
    }

    /// Number of rows in the result.
    pub fn len(&self) -> usize {
        if self.num_cols == 0 {
            return 0;
        }
        self.all_col_offsets.len() / self.num_cols
    }

    /// Whether the result set is empty.
    pub fn is_empty(&self) -> bool {
        self.all_col_offsets.is_empty()
    }

    /// Number of affected rows (for INSERT/UPDATE/DELETE).
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    /// Column descriptors.
    pub fn columns(&self) -> &[ColumnDesc] {
        &self.columns
    }

    /// Get a row by index. The returned `Row` borrows from the arena.
    pub fn row<'a>(&'a self, idx: usize, arena: &'a Arena) -> Row<'a> {
        let start = idx * self.num_cols;
        let end = start + self.num_cols;
        Row {
            arena,
            col_offsets: &self.all_col_offsets[start..end],
            columns: &self.columns,
        }
    }

    /// Iterate over rows.
    pub fn rows<'a>(&'a self, arena: &'a Arena) -> impl Iterator<Item = Row<'a>> {
        let num_cols = self.num_cols;
        let columns = &self.columns;
        self.all_col_offsets
            .chunks(num_cols.max(1))
            .map(move |chunk| Row {
                arena,
                col_offsets: chunk,
                columns,
            })
    }
}

// --- Row ---

/// A view into a single result row, borrowing data from the arena.
///
/// Column values are accessed by index. NULL values return `None`.
/// Decode errors (protocol violations from a malicious server) are treated
/// as `None` rather than panicking — a compliant PostgreSQL server always
/// sends correctly-sized data for the declared type.
pub struct Row<'a> {
    arena: &'a Arena,
    col_offsets: &'a [(usize, i32)],
    columns: &'a [ColumnDesc],
}

impl<'a> Row<'a> {
    /// Get the raw bytes for a column, or `None` if NULL.
    pub fn get_raw(&self, idx: usize) -> Option<&'a [u8]> {
        let (offset, len) = self.col_offsets[idx];
        if len < 0 {
            None
        } else {
            Some(self.arena.get(offset, len as usize))
        }
    }

    /// Whether a column is NULL.
    pub fn is_null(&self, idx: usize) -> bool {
        self.col_offsets[idx].1 < 0
    }

    /// Number of columns.
    pub fn column_count(&self) -> usize {
        self.col_offsets.len()
    }

    /// Get a boolean column value. Returns `None` on NULL or decode error.
    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_bool(data).ok())
    }

    /// Get an i16 column value. Returns `None` on NULL or decode error.
    pub fn get_i16(&self, idx: usize) -> Option<i16> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_i16(data).ok())
    }

    /// Get an i32 column value. Returns `None` on NULL or decode error.
    pub fn get_i32(&self, idx: usize) -> Option<i32> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_i32(data).ok())
    }

    /// Get an i64 column value. Returns `None` on NULL or decode error.
    pub fn get_i64(&self, idx: usize) -> Option<i64> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_i64(data).ok())
    }

    /// Get an f32 column value. Returns `None` on NULL or decode error.
    pub fn get_f32(&self, idx: usize) -> Option<f32> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_f32(data).ok())
    }

    /// Get an f64 column value. Returns `None` on NULL or decode error.
    pub fn get_f64(&self, idx: usize) -> Option<f64> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_f64(data).ok())
    }

    /// Get a string column value. Returns `None` on NULL or decode error.
    pub fn get_str(&self, idx: usize) -> Option<&'a str> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_str(data).ok())
    }

    /// Get a byte slice column value.
    pub fn get_bytes(&self, idx: usize) -> Option<&'a [u8]> {
        self.get_raw(idx)
    }

    /// Get the column name by index.
    pub fn column_name(&self, idx: usize) -> &str {
        &self.columns[idx].name
    }

    /// Get the column type OID by index.
    pub fn column_type_oid(&self, idx: usize) -> u32 {
        self.columns[idx].type_oid
    }
}

// --- DataRow parsing ---

/// Parse a DataRow message into the flat column offset storage.
///
/// Appends `(arena_offset, length)` pairs for each column to `out`.
/// `length = -1` indicates NULL.
///
/// DataRow format: `[num_columns: i16] ([col_len: i32] [col_data: col_len bytes])...`
fn parse_data_row_flat(
    data: &[u8],
    arena: &mut Arena,
    out: &mut Vec<(usize, i32)>,
) -> Result<(), DriverError> {
    if data.len() < 2 {
        return Err(DriverError::Protocol("DataRow too short".into()));
    }

    let num_cols = i16::from_be_bytes([data[0], data[1]]) as usize;
    out.reserve(num_cols);
    let mut pos = 2;

    for _ in 0..num_cols {
        if pos + 4 > data.len() {
            return Err(DriverError::Protocol("DataRow truncated".into()));
        }

        let col_len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;

        if col_len < 0 {
            // NULL
            out.push((0, -1));
        } else {
            let len = col_len as usize;
            if pos + len > data.len() {
                return Err(DriverError::Protocol(
                    "DataRow column data truncated".into(),
                ));
            }

            let offset = arena.alloc_copy(&data[pos..pos + len]);
            out.push((offset, col_len));
            pos += len;
        }
    }

    Ok(())
}

/// Compute a rapidhash of a SQL string.
pub fn hash_sql(sql: &str) -> u64 {
    let mut hasher = RapidHasher::default();
    hasher.write(sql.as_bytes());
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_parse_full_url() {
        let cfg = Config::from_url("postgres://user:pass@localhost:5432/mydb").unwrap();
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.password, "pass");
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.database, "mydb");
    }

    #[test]
    fn config_parse_default_port() {
        let cfg = Config::from_url("postgres://user:pass@localhost/mydb").unwrap();
        assert_eq!(cfg.port, 5432);
    }

    #[test]
    fn config_parse_no_password() {
        let cfg = Config::from_url("postgres://user@localhost/mydb").unwrap();
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.password, "");
    }

    #[test]
    fn config_parse_empty_database() {
        let cfg = Config::from_url("postgres://user:pass@localhost").unwrap();
        // database defaults to user
        assert_eq!(cfg.database, "user");
    }

    #[test]
    fn config_parse_sslmode() {
        let cfg = Config::from_url("postgres://user:pass@localhost/db?sslmode=require").unwrap();
        assert_eq!(cfg.ssl, SslMode::Require);
    }

    #[test]
    fn config_parse_percent_encoding() {
        let cfg = Config::from_url("postgres://user%40domain:p%40ss@localhost/db").unwrap();
        assert_eq!(cfg.user, "user@domain");
        assert_eq!(cfg.password, "p@ss");
    }

    #[test]
    fn config_rejects_bad_scheme() {
        let result = Config::from_url("mysql://user:pass@localhost/db");
        assert!(result.is_err());
    }

    #[test]
    fn hash_sql_deterministic() {
        let h1 = hash_sql("SELECT 1");
        let h2 = hash_sql("SELECT 1");
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_sql_different_queries() {
        let h1 = hash_sql("SELECT 1");
        let h2 = hash_sql("SELECT 2");
        assert_ne!(h1, h2);
    }

    #[test]
    fn data_row_parsing() {
        let mut arena = Arena::new();
        let mut out = Vec::new();

        // Build a DataRow with 2 columns: i32(42) and NULL
        let mut data = Vec::new();
        data.extend_from_slice(&2i16.to_be_bytes()); // 2 columns

        // Column 1: i32 = 42
        data.extend_from_slice(&4i32.to_be_bytes()); // length = 4
        data.extend_from_slice(&42i32.to_be_bytes()); // value

        // Column 2: NULL
        data.extend_from_slice(&(-1i32).to_be_bytes()); // length = -1

        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 2);

        // First column should have length 4
        assert_eq!(out[0].1, 4);

        // Second column should be NULL
        assert_eq!(out[1].1, -1);
    }

    #[test]
    fn data_row_empty() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let data = 0i16.to_be_bytes();
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn query_result_empty() {
        let result = QueryResult {
            all_col_offsets: vec![],
            num_cols: 0,
            columns: Arc::from(Vec::new()),
            affected_rows: 0,
        };
        assert!(result.is_empty());
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn url_decode_works() {
        assert_eq!(url_decode("hello%20world"), "hello world");
        assert_eq!(url_decode("no%20escape"), "no escape");
        assert_eq!(url_decode("plain"), "plain");
        assert_eq!(url_decode("a%40b"), "a@b");
    }
}
