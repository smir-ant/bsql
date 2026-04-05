//! PostgreSQL connection — startup, authentication, statement cache, query execution.
//!
//! `Connection` owns a TCP, TLS, or Unix domain socket stream and implements the
//! extended query protocol with pipelining. Statements are cached by rapidhash of the
//! SQL text. On first use, Parse+Describe+Bind+Execute+Sync are pipelined in one write.
//! On subsequent uses, only Bind+Execute+Sync are sent.
//!
//! # Unix domain sockets
//!
//! When `Config::host` starts with `/`, the driver connects via Unix domain socket
//! at `{host}/.s.PGSQL.{port}` (libpq convention). Use `?host=/tmp` in the connection
//! URL to enable UDS. This avoids TCP overhead for localhost connections.

use std::sync::Arc;

use crate::stmt_cache::{build_bind_template, make_stmt_name, StmtCache, StmtInfo};

use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::DriverError;
use crate::arena::Arena;
use crate::auth;
use crate::codec::Encode;
use crate::proto::{self, BackendMessage};
use crate::types::{parse_data_row_flat, SimpleRow, StartupAction};
pub use crate::types::{
    ColumnDesc, Config, Notification, PgDataRow, PrepareResult, QueryResult, SslMode,
};

#[cfg(feature = "tls")]
use crate::tls;

// --- Stream abstraction ---

/// The underlying stream type — plain TCP, TLS, or Unix domain socket.
enum Stream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

impl Stream {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.write_all(buf).await,
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.write_all(buf).await,
            #[cfg(unix)]
            Stream::Unix(s) => s.write_all(buf).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush().await,
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.flush().await,
            #[cfg(unix)]
            Stream::Unix(s) => s.flush().await,
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
            Stream::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_read(cx, buf),
            #[cfg(unix)]
            Stream::Unix(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
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
    stmts: StmtCache,
    params: Vec<(Box<str>, Box<str>)>,
    pid: i32,
    secret: i32,
    tx_status: u8,
    /// Timestamp of the last successful query completion. Used by the pool
    /// to detect stale connections and discard them instead of returning
    /// a potentially dead TCP socket.
    last_used: std::time::Instant,
    /// Whether a streaming query is in progress. When true, the
    /// connection is in an indeterminate protocol state (portal open, no
    /// ReadyForQuery) and cannot be reused. PoolGuard::drop checks this flag.
    streaming_active: bool,
    /// Timestamp of connection creation. Used by pool max_lifetime.
    created_at: std::time::Instant,
    /// Notifications received during query processing. Buffered here
    /// instead of dropped; call `drain_notifications()` to retrieve.
    pending_notifications: Vec<Notification>,
    /// Maximum number of cached prepared statements. When the cache exceeds
    /// this size, the least recently used statement is evicted (Close sent to PG).
    /// Default: 256.
    max_stmt_cache_size: usize,
    /// Monotonic counter for LRU eviction — incremented on each cache access.
    /// Replaces `Instant::now()` to avoid syscall overhead (~20-40ns on macOS).
    query_counter: u64,
}

impl Connection {
    /// Connect to PostgreSQL and complete the startup/auth handshake.
    ///
    /// When `config.host` starts with `/` (Unix domain socket directory),
    /// connects via `UnixStream` at `{host}/.s.PGSQL.{port}` instead of TCP.
    /// TCP_NODELAY and keepalive are skipped for UDS since they are TCP-only.
    pub async fn connect(config: &Config) -> Result<Self, DriverError> {
        // Config::from_url() already validates. Manual Config construction
        // should call validate() explicitly before passing to connect().

        #[cfg(unix)]
        if config.host_is_uds() {
            let path = config.uds_path();
            let unix = tokio::net::UnixStream::connect(&path)
                .await
                .map_err(DriverError::Io)?;
            let stream = Stream::Unix(unix);
            return Self::finish_connect(stream, config).await;
        }

        let addr = format!("{}:{}", config.host, config.port);
        let tcp = TcpStream::connect(&addr).await.map_err(DriverError::Io)?;

        // Set TCP_NODELAY to avoid Nagle delay on pipelined messages
        tcp.set_nodelay(true).map_err(DriverError::Io)?;

        // Without keepalive, a half-open connection (server crashed, firewall
        // timeout) can hang forever on read.
        Self::set_keepalive(&tcp)?;

        let stream = match config.ssl {
            SslMode::Disable => Stream::Plain(tcp),
            #[cfg(feature = "tls")]
            SslMode::Prefer | SslMode::Require => {
                match tls::try_upgrade(tcp, &config.host, config.ssl == SslMode::Require).await {
                    Ok(tls_stream) => Stream::Tls(Box::new(tls_stream)),
                    Err(e) if config.ssl == SslMode::Require => return Err(e),
                    Err(_) => {
                        // Prefer mode: TLS failed, reconnect plain
                        let tcp = TcpStream::connect(&addr).await.map_err(DriverError::Io)?;
                        tcp.set_nodelay(true).map_err(DriverError::Io)?;
                        Self::set_keepalive(&tcp)?;
                        Stream::Plain(tcp)
                    }
                }
            }
            #[cfg(not(feature = "tls"))]
            SslMode::Require => {
                return Err(DriverError::Protocol(
                    "TLS required but bsql-driver-postgres compiled without 'tls' feature".into(),
                ));
            }
            #[cfg(not(feature = "tls"))]
            SslMode::Prefer => Stream::Plain(tcp),
        };

        Self::finish_connect(stream, config).await
    }

    /// Shared connection setup: build the `Connection`, run startup handshake,
    /// validate server params, and set statement timeout. Called by both the
    /// TCP and UDS paths in [`connect`].
    async fn finish_connect(stream: Stream, config: &Config) -> Result<Self, DriverError> {
        let mut conn = Self {
            stream,
            read_buf: Vec::with_capacity(8192),

            stream_buf: vec![0u8; 65536],
            stream_buf_pos: 0,
            stream_buf_end: 0,
            write_buf: Vec::with_capacity(4096),
            stmts: StmtCache::default(),
            params: Vec::new(),
            pid: 0,
            secret: 0,
            tx_status: b'I',
            last_used: std::time::Instant::now(),
            streaming_active: false,
            created_at: std::time::Instant::now(),
            pending_notifications: Vec::new(),
            max_stmt_cache_size: 256,
            query_counter: 0,
        };

        conn.startup(config).await?;

        // Validate critical server parameters received during startup.
        conn.validate_server_params()?;

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
                    // Linear scan on ~10 entries is faster than HashMap
                    if let Some(entry) = self.params.iter_mut().find(|(k, _)| *k == name) {
                        entry.1 = value;
                    } else {
                        self.params.push((name, value));
                    }
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
        let name = make_stmt_name(sql_hash);
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

        // Cache the statement (with LRU eviction if needed)
        self.query_counter += 1;
        self.cache_stmt(
            sql_hash,
            StmtInfo {
                name,
                columns,
                last_used: self.query_counter,
                bind_template: None,
            },
        );
        Ok(())
    }

    /// Prepare a statement and return full column + parameter metadata.
    ///
    /// Sends Parse + Describe(Statement) + Sync, then reads:
    /// - ParseComplete
    /// - ParameterDescription (param type OIDs)
    /// - RowDescription or NoData (column metadata)
    /// - ReadyForQuery
    ///
    /// Unlike `prepare_only`, this always sends Parse (no cache check) and
    /// uses the unnamed statement `""` so it does not pollute the statement
    /// cache. This is designed for compile-time SQL validation in the proc
    /// macro, where we need column + param metadata but never execute.
    pub async fn prepare_describe(&mut self, sql: &str) -> Result<PrepareResult, DriverError> {
        self.write_buf.clear();
        // Use unnamed statement "" — PG replaces it on every Parse,
        // so there is no cache pollution.
        proto::write_parse(&mut self.write_buf, "", sql, &[]);
        proto::write_describe(&mut self.write_buf, b'S', "");
        proto::write_sync(&mut self.write_buf);
        self.flush_write().await?;

        // Read ParseComplete
        self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))
            .await?;

        // Read ParameterDescription + RowDescription/NoData
        let mut param_oids: Vec<u32> = Vec::new();
        let columns;
        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::ParameterDescription { data } => {
                    param_oids = proto::parse_parameter_description(data)?;
                }
                BackendMessage::RowDescription { data } => {
                    columns = proto::parse_row_description(data)?;
                    break;
                }
                BackendMessage::NoData => {
                    columns = Vec::new();
                    break;
                }
                BackendMessage::NoticeResponse { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "expected ParameterDescription/RowDescription/NoData, got: {other:?}"
                    )));
                }
            }
        }

        // ReadyForQuery
        self.expect_ready().await?;

        Ok(PrepareResult {
            columns,
            param_oids,
        })
    }

    /// Execute a simple (text protocol) query and return all result rows.
    ///
    /// Each row is a `Vec<Option<String>>` — NULL values are `None`, text
    /// values are `Some(String)`. This uses the simple query protocol which
    /// always returns text-format results.
    ///
    /// Designed for compile-time schema introspection queries in the proc
    /// macro (e.g. `pg_attribute`, `information_schema`). Not intended for
    /// high-performance runtime use.
    pub async fn simple_query_rows(&mut self, sql: &str) -> Result<Vec<SimpleRow>, DriverError> {
        self.write_buf.clear();
        proto::write_simple_query(&mut self.write_buf, sql);
        self.flush_write().await?;

        let mut rows: Vec<SimpleRow> = Vec::new();
        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::ReadyForQuery { status } => {
                    self.tx_status = status;
                    return Ok(rows);
                }
                BackendMessage::DataRow { data } => {
                    rows.push(proto::parse_simple_data_row(data)?);
                }
                BackendMessage::RowDescription { .. }
                | BackendMessage::CommandComplete { .. }
                | BackendMessage::EmptyQuery
                | BackendMessage::NoticeResponse { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                BackendMessage::ParameterStatus { .. } => {}
                other => {
                    return Err(DriverError::Protocol(format!(
                        "unexpected message during simple_query_rows: {other:?}"
                    )));
                }
            }
        }
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
        self.write_buf.clear();

        // Single hash lookup via get_mut — avoids contains_key + index double-lookup.
        let columns = if let Some(info) = self.stmts.get_mut(&sql_hash) {
            // Cache hit: try bind template, fall back to write_bind_params.
            self.query_counter += 1;
            info.last_used = self.query_counter;

            let can_use_template = info
                .bind_template
                .as_ref()
                .is_some_and(|t| t.param_slots.len() == params.len());

            if can_use_template {
                let tmpl = info.bind_template.as_ref().unwrap();
                // Copy only the Bind portion (not EXECUTE_SYNC) — streaming
                // needs Execute+Flush instead.
                self.write_buf
                    .extend_from_slice(&tmpl.bytes[..tmpl.bind_end]);

                let mut template_ok = true;
                for (i, param) in params.iter().enumerate() {
                    let (data_offset, old_len) = tmpl.param_slots[i];
                    if param.is_null() {
                        let len_offset = data_offset - 4;
                        self.write_buf[len_offset..len_offset + 4]
                            .copy_from_slice(&(-1i32).to_be_bytes());
                    } else if old_len >= 0 {
                        let end = data_offset + old_len as usize;
                        if !param.encode_at(&mut self.write_buf[data_offset..end]) {
                            template_ok = false;
                            break;
                        }
                    } else {
                        template_ok = false;
                        break;
                    }
                }

                if !template_ok {
                    self.write_buf.clear();
                    proto::write_bind_params(&mut self.write_buf, "", &info.name, params);
                    info.bind_template = None;
                }
            } else {
                proto::write_bind_params(&mut self.write_buf, "", &info.name, params);
            }

            let cols = info.columns.clone();

            if info.bind_template.is_none() && !self.write_buf.is_empty() {
                info.bind_template = build_bind_template(&self.write_buf, params.len());
            }

            proto::write_execute(&mut self.write_buf, "", chunk_size);
            // Use Flush (not Sync!) to keep the portal alive between chunks.
            proto::write_flush(&mut self.write_buf);
            self.flush_write().await?;

            cols
        } else {
            // Cache miss: Parse+Describe+Bind+Execute+Flush
            let name = make_stmt_name(sql_hash);
            let param_oids: smallvec::SmallVec<[u32; 8]> =
                params.iter().map(|p| p.type_oid()).collect();
            proto::write_parse(&mut self.write_buf, &name, sql, &param_oids);
            proto::write_describe(&mut self.write_buf, b'S', &name);
            proto::write_bind_params(&mut self.write_buf, "", &name, params);

            proto::write_execute(&mut self.write_buf, "", chunk_size);
            proto::write_flush(&mut self.write_buf);
            self.flush_write().await?;

            self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))
                .await?;
            let columns = self.read_column_description().await?;
            self.query_counter += 1;
            self.cache_stmt(
                sql_hash,
                StmtInfo {
                    name,
                    columns: columns.clone(),
                    last_used: self.query_counter,
                    bind_template: None,
                },
            );
            columns
        };

        // BindComplete
        self.expect_message(|m| matches!(m, BackendMessage::BindComplete))
            .await?;

        self.streaming_active = true;

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

                    self.streaming_active = false;
                    return Ok(false);
                }
                BackendMessage::EmptyQuery => {
                    self.write_buf.clear();
                    proto::write_sync(&mut self.write_buf);
                    self.flush_write().await?;
                    self.expect_ready().await?;

                    self.streaming_active = false;
                    return Ok(false);
                }
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    // Send Sync to reset and drain to ReadyForQuery
                    self.write_buf.clear();
                    proto::write_sync(&mut self.write_buf);
                    self.flush_write().await?;
                    self.drain_to_ready().await?;

                    self.streaming_active = false;
                    return Err(self.make_server_error(fields));
                }
                BackendMessage::NoticeResponse { .. } => {}
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

    /// Common pipeline setup — builds Parse+Describe+Bind+Execute+Sync (or
    /// Bind+Execute+Sync on cache hit), sends to wire, reads ParseComplete+Describe
    /// responses if needed, reads BindComplete. Returns column metadata.
    ///
    /// When `need_columns` is false (e.g. `for_each_raw`, `execute`), the Arc
    /// clone of column metadata is skipped — saving an atomic increment on the
    /// hot path.
    ///
    /// When `skip_bind_complete` is true, the BindComplete message is NOT
    /// consumed here — the caller reads it inline from stream_buf (e.g.
    /// `for_each_raw` which already has a zero-copy stream_buf reader).
    async fn send_pipeline(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        need_columns: bool,
        skip_bind_complete: bool,
    ) -> Result<Option<Arc<[ColumnDesc]>>, DriverError> {
        debug_assert_eq!(
            hash_sql(sql),
            sql_hash,
            "sql_hash mismatch: caller-provided hash does not match hash_sql(sql)"
        );

        if params.len() > i16::MAX as usize {
            return Err(DriverError::Protocol(format!(
                "parameter count {} exceeds maximum {} for PG wire protocol",
                params.len(),
                i16::MAX
            )));
        }

        self.write_buf.clear();

        // Single hash lookup — get_mut avoids the contains_key + index double-lookup.
        let columns = if let Some(info) = self.stmts.get_mut(&sql_hash) {
            // Cache hit: try bind template for fast path, fall back to write_bind_params.
            self.query_counter += 1;
            info.last_used = self.query_counter;

            let can_use_template = info
                .bind_template
                .as_ref()
                .is_some_and(|t| t.param_slots.len() == params.len());

            // Tracks whether write_buf already contains EXECUTE_SYNC (from template).
            let mut has_exec_sync = false;

            if can_use_template {
                // Fast path: copy template (includes EXECUTE_SYNC) and patch params
                // directly via encode_at — no scratch buffer, no double-copy.
                let tmpl = info.bind_template.as_ref().unwrap();
                self.write_buf.extend_from_slice(&tmpl.bytes);

                let mut template_ok = true;
                for (i, param) in params.iter().enumerate() {
                    let (data_offset, old_len) = tmpl.param_slots[i];
                    if param.is_null() {
                        let len_offset = data_offset - 4;
                        self.write_buf[len_offset..len_offset + 4]
                            .copy_from_slice(&(-1i32).to_be_bytes());
                    } else if old_len >= 0 {
                        let end = data_offset + old_len as usize;
                        if !param.encode_at(&mut self.write_buf[data_offset..end]) {
                            template_ok = false;
                            break;
                        }
                    } else {
                        // Template had NULL here but now non-NULL — rebuild.
                        template_ok = false;
                        break;
                    }
                }

                if template_ok {
                    has_exec_sync = true; // Template includes EXECUTE_SYNC.
                } else {
                    self.write_buf.clear();
                    proto::write_bind_params(&mut self.write_buf, "", &info.name, params);
                    info.bind_template = None;
                }
            } else {
                proto::write_bind_params(&mut self.write_buf, "", &info.name, params);
            }

            // Clone Arc only when caller needs columns (query path).
            // for_each_raw / execute skip this atomic increment.
            let cols = if need_columns {
                Some(info.columns.clone())
            } else {
                None
            };

            // Snapshot bind template on first use or after invalidation.
            // build_bind_template appends EXECUTE_SYNC to the template bytes.
            if info.bind_template.is_none() && !self.write_buf.is_empty() {
                info.bind_template = build_bind_template(&self.write_buf, params.len());
            }

            if !has_exec_sync {
                self.write_buf.extend_from_slice(proto::EXECUTE_SYNC);
            }
            self.flush_write().await?;

            cols
        } else {
            // Cache miss: Parse+Describe+Bind+Execute+Sync
            let name = make_stmt_name(sql_hash);
            let param_oids: smallvec::SmallVec<[u32; 8]> =
                params.iter().map(|p| p.type_oid()).collect();
            proto::write_parse(&mut self.write_buf, &name, sql, &param_oids);
            proto::write_describe(&mut self.write_buf, b'S', &name);
            proto::write_bind_params(&mut self.write_buf, "", &name, params);

            self.write_buf.extend_from_slice(proto::EXECUTE_SYNC);
            self.flush_write().await?;

            self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))
                .await?;
            let columns = self.read_column_description().await?;
            self.query_counter += 1;
            self.cache_stmt(
                sql_hash,
                StmtInfo {
                    name,
                    columns: columns.clone(),
                    last_used: self.query_counter,
                    bind_template: None,
                },
            );
            if need_columns { Some(columns) } else { None }
        };

        // BindComplete — skip when caller handles it inline (for_each_raw).
        if !skip_bind_complete {
            self.expect_message(|m| matches!(m, BackendMessage::BindComplete))
                .await?;
        }

        Ok(columns)
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
        let columns = self
            .send_pipeline(sql, sql_hash, params, true, false)
            .await?
            .expect("send_pipeline(need_columns=true) must return Some");

        // Read DataRow messages and CommandComplete.
        // Flat column offsets: all rows' columns are stored contiguously in
        // `all_col_offsets`. Row N starts at index `N * num_cols`.

        // is just num_cols; for fetch_all we grow dynamically. The previous
        // `num_cols * 64` over-allocates for single-row queries.
        let num_cols = columns.len();
        // .max(1) prevents zero-capacity allocation when num_cols is 0 (e.g., INSERT/UPDATE/DELETE
        // with no RETURNING clause), ensuring Vec has a reasonable initial capacity.
        let mut all_col_offsets: Vec<(usize, i32)> = Vec::with_capacity(num_cols.max(1) * 8);
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
                BackendMessage::NoticeResponse { .. } => {
                    // Async messages can arrive mid-query — skip them
                }
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);

                    self.maybe_invalidate_stmt_cache(&fields, sql_hash);
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
                    let cols = proto::parse_row_description(data)?;
                    return Ok(cols.into());
                }
                BackendMessage::ParameterDescription { .. } => {
                    // ParameterDescription precedes RowDescription — continue reading
                }
                BackendMessage::NoData => return Ok(Arc::from(Vec::new())),
                BackendMessage::NoticeResponse { .. } => {}
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
        let _ = self
            .send_pipeline(sql, sql_hash, params, false, false)
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
                BackendMessage::NoticeResponse { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);

                    self.maybe_invalidate_stmt_cache(&fields, sql_hash);
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

    /// Execute the same prepared statement N times with different parameters
    /// in a single pipeline round-trip.
    ///
    /// Sends all N Bind+Execute messages followed by one Sync. PostgreSQL
    /// processes them in order and returns N BindComplete+CommandComplete
    /// responses followed by one ReadyForQuery.
    ///
    /// This is a real optimization for bulk operations: N inserts in a
    /// transaction become 1 round-trip instead of N round-trips.
    ///
    /// The statement must already be cached (call `execute` at least once first,
    /// or use `prepare_describe`). If not cached, it will be prepared inline
    /// for the first entry, then the rest use the cached version.
    ///
    /// Returns the number of affected rows for each parameter set.
    pub async fn execute_pipeline(
        &mut self,
        sql: &str,
        sql_hash: u64,
        param_sets: &[&[&(dyn Encode + Sync)]],
    ) -> Result<Vec<u64>, DriverError> {
        if param_sets.is_empty() {
            return Ok(Vec::new());
        }

        debug_assert_eq!(
            hash_sql(sql),
            sql_hash,
            "sql_hash mismatch: caller-provided hash does not match hash_sql(sql)"
        );

        self.write_buf.clear();

        // Ensure statement is prepared. If not cached, prepare it first with
        // a standalone Parse+Describe+Sync pipeline.
        if !self.stmts.contains_key(&sql_hash) {
            let name = make_stmt_name(sql_hash);
            let first_params = param_sets[0];
            if first_params.len() > i16::MAX as usize {
                return Err(DriverError::Protocol(format!(
                    "parameter count {} exceeds maximum {}",
                    first_params.len(),
                    i16::MAX
                )));
            }
            let param_oids: smallvec::SmallVec<[u32; 8]> =
                first_params.iter().map(|p| p.type_oid()).collect();
            proto::write_parse(&mut self.write_buf, &name, sql, &param_oids);
            proto::write_describe(&mut self.write_buf, b'S', &name);
            proto::write_sync(&mut self.write_buf);
            self.flush_write().await?;

            self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))
                .await?;
            let columns = self.read_column_description().await?;
            self.expect_ready().await?;

            self.query_counter += 1;
            self.cache_stmt(
                sql_hash,
                StmtInfo {
                    name,
                    columns,
                    last_used: self.query_counter,
                    bind_template: None,
                },
            );

            self.write_buf.clear();
        }

        // Build N x (Bind + Execute) + 1 x Sync
        let stmt_name = self
            .stmts
            .get(&sql_hash)
            .expect("BUG: stmt just cached but not found")
            .name
            .clone();
        let count = param_sets.len();

        for params in param_sets {
            if params.len() > i16::MAX as usize {
                return Err(DriverError::Protocol(format!(
                    "parameter count {} exceeds maximum {}",
                    params.len(),
                    i16::MAX
                )));
            }
            proto::write_bind_params(&mut self.write_buf, "", &stmt_name, params);
            self.write_buf.extend_from_slice(proto::EXECUTE_ONLY);
        }

        // One Sync at the end
        self.write_buf.extend_from_slice(proto::SYNC_ONLY);
        self.flush_write().await?;

        // Read N x (BindComplete + CommandComplete) + ReadyForQuery
        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            self.expect_message(|m| matches!(m, BackendMessage::BindComplete))
                .await?;

            // Read until CommandComplete, skipping DataRow/EmptyQuery/Notice
            let mut affected_rows: u64 = 0;
            loop {
                let msg = self.read_one_message().await?;
                match msg {
                    BackendMessage::DataRow { .. } => {}
                    BackendMessage::CommandComplete { tag } => {
                        affected_rows = proto::parse_command_tag(tag);
                        break;
                    }
                    BackendMessage::EmptyQuery => break,
                    BackendMessage::NoticeResponse { .. } => {}
                    BackendMessage::ErrorResponse { data } => {
                        let fields = proto::parse_error_response(data);
                        self.maybe_invalidate_stmt_cache(&fields, sql_hash);
                        self.drain_to_ready().await?;
                        return Err(self.make_server_error(fields));
                    }
                    other => {
                        return Err(DriverError::Protocol(format!(
                            "unexpected message during execute_pipeline: {other:?}"
                        )));
                    }
                }
            }
            results.push(affected_rows);
        }

        self.expect_ready().await?;
        self.shrink_buffers();
        Ok(results)
    }

    /// Ensure a statement is prepared and cached, doing a round-trip if needed.
    ///
    /// Returns the cached statement name. If the statement is already cached,
    /// this is a no-op (hash lookup only). Otherwise, sends Parse+Describe+Sync
    /// and waits for the response.
    ///
    /// Used by deferred pipeline execution to separate the prepare step
    /// (which requires I/O) from the Bind+Execute buffering step (which doesn't).
    pub(crate) async fn ensure_stmt_prepared(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<Box<str>, DriverError> {
        if let Some(info) = self.stmts.get(&sql_hash) {
            return Ok(info.name.clone());
        }

        // Cache miss: Parse+Describe+Sync round-trip
        let name = make_stmt_name(sql_hash);
        if params.len() > i16::MAX as usize {
            return Err(DriverError::Protocol(format!(
                "parameter count {} exceeds maximum {}",
                params.len(),
                i16::MAX
            )));
        }
        let param_oids: smallvec::SmallVec<[u32; 8]> =
            params.iter().map(|p| p.type_oid()).collect();

        self.write_buf.clear();
        proto::write_parse(&mut self.write_buf, &name, sql, &param_oids);
        proto::write_describe(&mut self.write_buf, b'S', &name);
        proto::write_sync(&mut self.write_buf);
        self.flush_write().await?;

        self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))
            .await?;
        let columns = self.read_column_description().await?;
        self.expect_ready().await?;

        self.query_counter += 1;
        let stmt_name = name.clone();
        self.cache_stmt(
            sql_hash,
            StmtInfo {
                name,
                columns,
                last_used: self.query_counter,
                bind_template: None,
            },
        );

        Ok(stmt_name)
    }

    /// Write Bind+Execute message bytes for a prepared statement into an
    /// external buffer. Does NOT send anything on the wire.
    ///
    /// The statement must already be prepared (call `ensure_stmt_prepared` first).
    /// Panics in debug mode if the statement is not cached.
    pub(crate) fn write_deferred_bind_execute(
        &self,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        buf: &mut Vec<u8>,
    ) {
        let stmt_name = &self
            .stmts
            .get(&sql_hash)
            .expect("BUG: stmt just cached but not found")
            .name;
        proto::write_bind_params(buf, "", stmt_name, params);
        buf.extend_from_slice(proto::EXECUTE_ONLY);
    }

    /// Flush a buffer of deferred Bind+Execute messages as a single pipeline.
    ///
    /// Appends Sync to the buffer, writes everything in one TCP write, then
    /// reads `count` x (BindComplete + CommandComplete) + ReadyForQuery.
    /// Returns the affected row count for each deferred operation.
    pub(crate) async fn flush_deferred_pipeline(
        &mut self,
        buf: &mut Vec<u8>,
        count: usize,
    ) -> Result<Vec<u64>, DriverError> {
        if count == 0 {
            buf.clear();
            return Ok(Vec::new());
        }

        buf.extend_from_slice(proto::SYNC_ONLY);

        // Write the entire buffer in one TCP write
        self.stream.write_all(buf).await.map_err(DriverError::Io)?;
        self.stream.flush().await.map_err(DriverError::Io)?;
        buf.clear();

        // Read count x (BindComplete + CommandComplete) + ReadyForQuery
        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            self.expect_message(|m| matches!(m, BackendMessage::BindComplete))
                .await?;

            let mut affected_rows: u64 = 0;
            loop {
                let msg = self.read_one_message().await?;
                match msg {
                    BackendMessage::DataRow { .. } => {}
                    BackendMessage::CommandComplete { tag } => {
                        affected_rows = proto::parse_command_tag(tag);
                        break;
                    }
                    BackendMessage::EmptyQuery => break,
                    BackendMessage::NoticeResponse { .. } => {}
                    BackendMessage::ErrorResponse { data } => {
                        let fields = proto::parse_error_response(data);
                        self.drain_to_ready().await?;
                        return Err(self.make_server_error(fields));
                    }
                    other => {
                        return Err(DriverError::Protocol(format!(
                            "unexpected message during flush_deferred_pipeline: {other:?}"
                        )));
                    }
                }
            }
            results.push(affected_rows);
        }

        self.expect_ready().await?;
        self.shrink_buffers();
        Ok(results)
    }

    /// Process each row directly from the wire buffer via a closure.
    ///
    /// Zero arena allocation — the closure receives a [`PgDataRow`] that reads
    /// columns directly from the DataRow message bytes in the read buffer.
    /// Column offsets are pre-scanned once per row into a stack-allocated SmallVec.
    ///
    /// This is the fastest path for row-by-row processing: no arena, no Vec of
    /// offsets, no materialization of the entire result set.
    pub async fn for_each<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        mut f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(PgDataRow<'_>) -> Result<(), DriverError>,
    {
        let _ = self
            .send_pipeline(sql, sql_hash, params, false, false)
            .await?;

        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::DataRow { data } => {
                    let row = PgDataRow::new(data)?;
                    f(row)?;
                }
                BackendMessage::CommandComplete { .. } => break,
                BackendMessage::EmptyQuery => break,
                BackendMessage::NoticeResponse { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.maybe_invalidate_stmt_cache(&fields, sql_hash);
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "unexpected message during for_each: {other:?}"
                    )));
                }
            }
        }

        self.expect_ready().await?;
        self.shrink_buffers();
        Ok(())
    }

    /// Process each DataRow as raw bytes — no `PgDataRow`, no SmallVec, no
    /// pre-scanning of column offsets.
    ///
    /// The closure receives the raw DataRow message payload (starting with the
    /// `i16` column count). Generated code decodes columns sequentially inline,
    /// advancing a position cursor through the bytes.
    ///
    /// This is faster than `for_each` because it eliminates the SmallVec
    /// construction (~20-30ns per row) and the per-column method call overhead.
    ///
    /// Optimization: DataRow messages that fit entirely within `stream_buf` are
    /// parsed directly from the buffer (zero-copy — no memcpy into `read_buf`).
    /// Messages that span the buffer boundary fall back to `read_message_buffered`.
    pub async fn for_each_raw<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        mut f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(&[u8]) -> Result<(), DriverError>,
    {
        let _ = self
            .send_pipeline(sql, sql_hash, params, false, true)
            .await?;

        // Read BindComplete inline from stream_buf — avoids the full
        // expect_message -> read_one_message -> read_message_buffered path.
        // BindComplete is always exactly 5 bytes: type='2'(1) + len=4(4).
        loop {
            let avail = self.stream_buf_end - self.stream_buf_pos;
            if avail >= 5 {
                let bc_type = self.stream_buf[self.stream_buf_pos];
                match bc_type {
                    b'2' => {
                        // BindComplete — skip the 5-byte message.
                        self.stream_buf_pos += 5;
                        break;
                    }
                    b'E' => {
                        // ErrorResponse — fall back to full message reader.
                        let msg = self.read_one_message().await?;
                        if let BackendMessage::ErrorResponse { data } = msg {
                            let fields = proto::parse_error_response(data);
                            self.drain_to_ready().await?;
                            return Err(self.make_server_error(fields));
                        }
                    }
                    b'N' | b'S' => {
                        // NoticeResponse or ParameterStatus — parse length,
                        // skip, and continue looking for BindComplete.
                        let raw_len = i32::from_be_bytes([
                            self.stream_buf[self.stream_buf_pos + 1],
                            self.stream_buf[self.stream_buf_pos + 2],
                            self.stream_buf[self.stream_buf_pos + 3],
                            self.stream_buf[self.stream_buf_pos + 4],
                        ]);
                        let total = 1 + raw_len as usize;
                        if avail >= total {
                            self.stream_buf_pos += total;
                            continue;
                        }
                        // Async message spans buffer boundary — fall back.
                        self.expect_message(|m| matches!(m, BackendMessage::BindComplete))
                            .await?;
                        break;
                    }
                    _ => {
                        // Unexpected type — fall back to full reader for
                        // proper error handling.
                        self.expect_message(|m| matches!(m, BackendMessage::BindComplete))
                            .await?;
                        break;
                    }
                }
            } else {
                // Not enough data in stream_buf — compact and refill.
                let remaining = self.stream_buf_end - self.stream_buf_pos;
                if remaining > 0 && self.stream_buf_pos > 0 {
                    self.stream_buf
                        .copy_within(self.stream_buf_pos..self.stream_buf_end, 0);
                }
                self.stream_buf_pos = 0;
                self.stream_buf_end = remaining;

                let n = {
                    let mut reader = StreamReader(&mut self.stream);
                    use tokio::io::AsyncReadExt;
                    reader
                        .read(&mut self.stream_buf[remaining..])
                        .await
                        .map_err(DriverError::Io)?
                };
                if n == 0 {
                    return Err(DriverError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "connection closed",
                    )));
                }
                self.stream_buf_end = remaining + n;
            }
        }

        // Bulk DataRow loop: parse messages directly from stream_buf when possible.
        'outer: loop {
            // Inner loop: process all complete messages already in stream_buf.
            loop {
                let avail = self.stream_buf_end - self.stream_buf_pos;
                if avail < 5 {
                    break; // need more data from TCP
                }

                let msg_type = self.stream_buf[self.stream_buf_pos];
                let raw_len = i32::from_be_bytes([
                    self.stream_buf[self.stream_buf_pos + 1],
                    self.stream_buf[self.stream_buf_pos + 2],
                    self.stream_buf[self.stream_buf_pos + 3],
                    self.stream_buf[self.stream_buf_pos + 4],
                ]);

                if raw_len < 4 {
                    return Err(DriverError::Protocol(format!(
                        "invalid message length {raw_len} for type '{}'",
                        msg_type as char
                    )));
                }

                let payload_len = (raw_len - 4) as usize;
                let total_msg_len = 5 + payload_len; // type(1) + length(4) + payload

                if avail < total_msg_len {
                    // Message doesn't fit in available buffer data.
                    if total_msg_len > self.stream_buf.len() {
                        // Message is larger than entire stream_buf — fall back to
                        // read_message_buffered which handles arbitrary sizes.
                        let msg = self.read_one_message().await?;
                        match msg {
                            BackendMessage::DataRow { data } => {
                                f(data)?;
                                continue;
                            }
                            BackendMessage::CommandComplete { .. } | BackendMessage::EmptyQuery => {
                                break 'outer;
                            }
                            BackendMessage::ErrorResponse { data } => {
                                let fields = proto::parse_error_response(data);
                                self.maybe_invalidate_stmt_cache(&fields, sql_hash);
                                self.drain_to_ready().await?;
                                return Err(self.make_server_error(fields));
                            }
                            BackendMessage::NoticeResponse { .. } => continue,
                            other => {
                                return Err(DriverError::Protocol(format!(
                                    "unexpected message during for_each_raw: {other:?}"
                                )));
                            }
                        }
                    }
                    // Partial message in buffer — compact and refill below.
                    break;
                }

                // Full message is available in stream_buf — zero-copy path.
                let payload_start = self.stream_buf_pos + 5;
                let payload_end = payload_start + payload_len;

                // Happy path first: DataRow is ~99.9% of messages during
                // bulk streaming. Single predicted branch.
                if msg_type == b'D' {
                    // DataRow — ZERO COPY from stream_buf.
                    // Safety: payload_start..payload_end is within stream_buf bounds
                    // (checked by `avail < total_msg_len` above).
                    f(&self.stream_buf[payload_start..payload_end])?;
                } else if msg_type == b'C' || msg_type == b'I' {
                    // CommandComplete / EmptyQuery — done.
                    self.stream_buf_pos += total_msg_len;
                    break 'outer;
                } else {
                    self.handle_non_datarow_async(msg_type, payload_start, payload_end, sql_hash)
                        .await?;
                }

                self.stream_buf_pos += total_msg_len;
            }

            // Compact: move unprocessed bytes to front of buffer.
            let remaining = self.stream_buf_end - self.stream_buf_pos;
            if remaining > 0 && self.stream_buf_pos > 0 {
                self.stream_buf
                    .copy_within(self.stream_buf_pos..self.stream_buf_end, 0);
            }
            self.stream_buf_pos = 0;
            self.stream_buf_end = remaining;

            // Read more from TCP.
            let n = {
                let mut reader = StreamReader(&mut self.stream);
                use tokio::io::AsyncReadExt;
                reader
                    .read(&mut self.stream_buf[remaining..])
                    .await
                    .map_err(DriverError::Io)?
            };
            if n == 0 {
                return Err(DriverError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                )));
            }
            self.stream_buf_end = remaining + n;
        }

        // Read ReadyForQuery.
        self.expect_ready().await?;
        self.shrink_buffers();
        Ok(())
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
                | BackendMessage::NoticeResponse { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }

                // ParameterStatus can arrive asynchronously during any query.
                BackendMessage::ParameterStatus { .. } => {}

                // Startup messages should not appear post-startup, but if
                // the stream buffer contains leftover data, skip them safely.
                BackendMessage::AuthOk
                | BackendMessage::AuthSaslFinal { .. }
                | BackendMessage::AuthSaslContinue { .. }
                | BackendMessage::AuthSasl { .. }
                | BackendMessage::AuthMd5 { .. }
                | BackendMessage::AuthCleartext
                | BackendMessage::BackendKeyData { .. } => {}

                other => {
                    return Err(DriverError::Protocol(format!(
                        "unexpected message during simple_query: {other:?}"
                    )));
                }
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

    /// Record that the connection was just used. Called after successful
    /// query completion so the pool can detect stale connections.
    pub fn touch(&mut self) {
        self.last_used = std::time::Instant::now();
    }

    /// How long since this connection last completed a query.
    pub fn idle_duration(&self) -> std::time::Duration {
        self.last_used.elapsed()
    }

    /// Get a server parameter value (set during startup or via SET).
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(k, _)| &**k == name)
            .map(|(_, v)| &**v)
    }

    /// All server parameters received during startup.
    pub fn server_params(&self) -> &[(Box<str>, Box<str>)] {
        &self.params
    }

    /// Validate critical server parameters after startup.
    ///
    /// Checks:
    /// - `server_encoding` must be UTF-8 (or UTF8). Our SIMD UTF-8 validation
    ///   and text decoding assume UTF-8 encoding.
    /// - `integer_datetimes` must be "on". Our timestamp/date codecs assume
    ///   integer-format timestamps (microseconds since 2000-01-01). If "off",
    ///   PG uses float-format timestamps and our decode is wrong.
    fn validate_server_params(&self) -> Result<(), DriverError> {
        // Check server_encoding — must be UTF-8
        if let Some(encoding) = self.parameter("server_encoding") {
            let normalized = encoding.to_uppercase();
            if normalized != "UTF8" && normalized != "UTF-8" {
                return Err(DriverError::Protocol(format!(
                    "server_encoding is '{encoding}', but bsql requires UTF-8. \
                     Set server encoding to UTF-8 in postgresql.conf or \
                     use CREATE DATABASE ... ENCODING 'UTF8'."
                )));
            }
        }

        // Check client_encoding — must be UTF-8
        if let Some(encoding) = self.parameter("client_encoding") {
            let normalized = encoding.to_uppercase();
            if normalized != "UTF8" && normalized != "UTF-8" {
                return Err(DriverError::Protocol(format!(
                    "client_encoding is '{encoding}', but bsql requires UTF-8. \
                     Check your connection or database configuration."
                )));
            }
        }

        // Check integer_datetimes — MUST be "on"
        if let Some(idt) = self.parameter("integer_datetimes") {
            if idt != "on" {
                return Err(DriverError::Protocol(format!(
                    "integer_datetimes is '{idt}', but bsql requires 'on'. \
                     Our timestamp codec assumes integer-format timestamps \
                     (microseconds since 2000-01-01). Float-format timestamps \
                     would produce incorrect decode results."
                )));
            }
        }

        Ok(())
    }

    /// Backend process ID (for cancel requests).
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Backend secret key (for cancel requests).
    pub fn secret_key(&self) -> i32 {
        self.secret
    }

    /// Cancel the currently running query on this connection.
    ///
    /// Opens a NEW TCP connection to the same host:port and sends a
    /// CancelRequest message (16 bytes: length=16, code=80877102, pid, secret).
    /// The cancel connection is closed immediately after sending.
    ///
    /// The `config` is needed to get the host:port for the new TCP connection.
    pub async fn cancel(&self, config: &Config) -> Result<(), DriverError> {
        let addr = format!("{}:{}", config.host, config.port);
        let mut tcp = TcpStream::connect(&addr).await.map_err(DriverError::Io)?;
        let mut buf = Vec::with_capacity(16);
        proto::write_cancel_request(&mut buf, self.pid, self.secret);
        tcp.write_all(&buf).await.map_err(DriverError::Io)?;
        tcp.flush().await.map_err(DriverError::Io)?;
        // Close immediately — PG expects no further data
        drop(tcp);
        Ok(())
    }

    /// Whether a streaming query is in progress.
    pub fn is_streaming(&self) -> bool {
        self.streaming_active
    }

    /// Drain all buffered notifications received during query processing.
    ///
    /// Returns the pending notifications and clears the buffer.
    /// Notifications arrive asynchronously from PG (via LISTEN/NOTIFY)
    /// and are buffered during normal query execution instead of being dropped.
    pub fn drain_notifications(&mut self) -> Vec<Notification> {
        std::mem::take(&mut self.pending_notifications)
    }

    /// Number of pending notifications in the buffer.
    pub fn pending_notification_count(&self) -> usize {
        self.pending_notifications.len()
    }

    /// Set the maximum number of cached prepared statements.
    ///
    /// When the cache exceeds this size, the least recently used statement
    /// is evicted and a Close message is sent to PG to free server memory.
    /// Default: 256.
    pub fn set_max_stmt_cache_size(&mut self, size: usize) {
        self.max_stmt_cache_size = size;
    }

    /// Number of currently cached prepared statements.
    pub fn stmt_cache_len(&self) -> usize {
        self.stmts.len()
    }

    /// Set TCP keepalive on a socket to detect dead connections.
    fn set_keepalive(tcp: &TcpStream) -> Result<(), DriverError> {
        let sock = socket2::SockRef::from(tcp);
        let ka = socket2::TcpKeepalive::new()
            .with_time(std::time::Duration::from_secs(60))
            .with_interval(std::time::Duration::from_secs(15));
        sock.set_tcp_keepalive(&ka).map_err(DriverError::Io)?;
        Ok(())
    }

    /// When this connection was created.
    pub fn created_at(&self) -> std::time::Instant {
        self.created_at
    }

    // --- Internal helpers ---

    /// Insert a statement into the cache, evicting the LRU entry if full.
    ///
    /// When the cache exceeds `max_stmt_cache_size`, the least recently used
    /// statement is evicted. A Close(Statement) message is queued to free
    /// server-side memory. The Close is sent lazily on the next flush.
    ///
    /// 256 entries = negligible linear scan cost (~1us worst case).
    fn cache_stmt(&mut self, sql_hash: u64, info: StmtInfo) {
        // Evict LRU if cache is full
        if self.stmts.len() >= self.max_stmt_cache_size && !self.stmts.contains_key(&sql_hash) {
            if let Some((_lru_hash, evicted)) = self.stmts.evict_lru() {
                // Queue Close(Statement) to free server-side memory.
                // This will be sent on the next write+flush.
                proto::write_close(&mut self.write_buf, b'S', &evicted.name);
            }
        }
        self.stmts.insert(sql_hash, info);
    }

    /// Buffer a notification received during query processing.
    fn buffer_notification(&mut self, pid: i32, channel: &str, payload: &str) {
        // Cap at 1024 buffered notifications to prevent unbounded memory growth
        if self.pending_notifications.len() < 1024 {
            self.pending_notifications.push(Notification {
                pid,
                channel: channel.to_owned(),
                payload: payload.to_owned(),
            });
        }
    }

    /// Reclaim memory if buffers grew beyond normal thresholds.
    ///
    /// Called after query()/execute() to prevent a single large result from
    /// permanently bloating the connection's buffers.
    fn shrink_buffers(&mut self) {
        // Only check every 64 queries — the capacity comparisons are cheap
        // but the shrink itself (realloc) is not. Most queries never trigger
        // the threshold, so this saves ~2-5ns of branch overhead per query.
        if self.query_counter & 63 != 0 {
            return;
        }
        if self.read_buf.capacity() > 64 * 1024 {
            self.read_buf.clear();
            self.read_buf.shrink_to(8192);
        }
        if self.write_buf.capacity() > 16 * 1024 {
            self.write_buf.clear();
            self.write_buf.shrink_to(8192);
        }
    }

    /// Read one backend message. The returned message borrows from `self.read_buf`.
    ///
    /// When a NotificationResponse is received, it is automatically buffered
    /// in `self.pending_notifications` and the next message is read instead.
    /// This means callers never see NotificationResponse from this method.
    async fn read_one_message(&mut self) -> Result<BackendMessage<'_>, DriverError> {
        loop {
            let (msg_type, _payload_len) = self.read_message_buffered().await?;
            // Check for NotificationResponse before parsing into BackendMessage,
            // because we need to extract owned data while we have exclusive access.
            if msg_type == b'A' {
                let msg = proto::parse_backend_message(msg_type, &self.read_buf)?;
                if let BackendMessage::NotificationResponse {
                    pid,
                    channel,
                    payload,
                } = msg
                {
                    // Extract owned data before releasing the borrow on self.read_buf.
                    let pid_owned = pid;
                    let channel_owned = channel.to_owned();
                    let payload_owned = payload.to_owned();
                    self.buffer_notification(pid_owned, &channel_owned, &payload_owned);
                    continue; // read next message
                }
            }
            return proto::parse_backend_message(msg_type, &self.read_buf);
        }
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
                BackendMessage::NoticeResponse { .. } | BackendMessage::ParameterStatus { .. } => {
                    // Asynchronous messages — skip them
                    // (NotificationResponse is auto-buffered by read_one_message)
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
                BackendMessage::NoticeResponse { .. } | BackendMessage::ParameterStatus { .. } => {}
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

    /// Check if an error is SQLSTATE 26000 ("prepared statement does not exist").
    /// If so, remove the stale entry from the statement cache so the caller can retry.
    fn maybe_invalidate_stmt_cache(&mut self, fields: &proto::ErrorFields, sql_hash: u64) -> bool {
        if &*fields.code == "26000" {
            self.stmts.remove(&sql_hash);
            true
        } else {
            false
        }
    }

    /// Convert parsed ErrorFields into a DriverError::Server.
    #[cold]
    #[inline(never)]
    fn make_server_error(&self, fields: proto::ErrorFields) -> DriverError {
        DriverError::Server {
            code: fields.code,
            message: fields.message.into_boxed_str(),
            detail: fields.detail.map(String::into_boxed_str),
            hint: fields.hint.map(String::into_boxed_str),
            position: fields.position,
        }
    }

    /// Handle non-DataRow messages during for_each_raw inline parsing (async).
    ///
    /// Separated from the hot loop so the compiler keeps DataRow processing
    /// tight in the instruction cache.
    #[cold]
    async fn handle_non_datarow_async(
        &mut self,
        msg_type: u8,
        payload_start: usize,
        payload_end: usize,
        sql_hash: u64,
    ) -> Result<(), DriverError> {
        match msg_type {
            b'E' => {
                let fields =
                    proto::parse_error_response(&self.stream_buf[payload_start..payload_end]);
                self.maybe_invalidate_stmt_cache(&fields, sql_hash);
                self.drain_to_ready().await?;
                return Err(self.make_server_error(fields));
            }
            b'A' => {
                let msg = proto::parse_backend_message(
                    msg_type,
                    &self.stream_buf[payload_start..payload_end],
                )?;
                if let BackendMessage::NotificationResponse {
                    pid,
                    channel,
                    payload,
                } = msg
                {
                    let ch = channel.to_owned();
                    let pl = payload.to_owned();
                    self.buffer_notification(pid, &ch, &pl);
                }
            }
            _ => {} // NoticeResponse, ParameterStatus — skip
        }
        Ok(())
    }

    /// Flush the write buffer to the stream.
    ///
    /// Always flush after write_all for correctness. TCP_NODELAY only
    /// affects the kernel's Nagle algorithm; tokio's BufWriter (used internally
    /// by TcpStream) may still buffer. Always flushing ensures data reaches
    /// the wire immediately for both plain TCP and TLS.
    async fn flush_write(&mut self) -> Result<(), DriverError> {
        self.stream
            .write_all(&self.write_buf)
            .await
            .map_err(DriverError::Io)?;
        self.stream.flush().await.map_err(DriverError::Io)?;
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

        // the length (truncation or zeroes only new bytes beyond current len).
        // For the common case where read_buf was already large enough, the
        // zeroing cost is minimal. This is the price of safe Rust — we cannot
        // use set_len() without unsafe.
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

pub use crate::types::hash_sql;
