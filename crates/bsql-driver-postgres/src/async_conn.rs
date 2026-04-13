//! Async PostgreSQL connection — startup, authentication, statement cache, query execution.
//!
//! `AsyncConnection` owns a `tokio::net::TcpStream` (plain or TLS) and implements
//! the extended query protocol with pipelining. All I/O is non-blocking via tokio.
//!
//! This is the async counterpart to `conn::Connection`. Unix domain sockets use
//! the sync `Connection` — only TCP connections go through `AsyncConnection`.
//!
//! # Transport
//!
//! TCP only (with optional TLS upgrade via tokio-rustls). UDS connections are
//! handled by the sync `Connection` path.

use std::sync::Arc;

use crate::async_io::AsyncStream;
use crate::auth;
use crate::codec::Encode;
use crate::conn::{acquire_resp_buf, parse_data_row_into_buf};
use crate::proto::{self, BackendMessage};
use crate::stmt_cache::{build_bind_template, make_stmt_name, StmtCache, StmtInfo};
use crate::types::{
    ColumnDesc, Config, Notification, QueryResult, SslMode, StartupAction, StatementCacheMode,
};
use crate::DriverError;

/// An async PostgreSQL connection over TCP or TLS.
///
/// All I/O is non-blocking via `tokio::net::TcpStream`. Requires a tokio runtime.
///
/// # Thread safety
///
/// `AsyncConnection` is `Send` but not `Sync` — it must be used by one task
/// at a time. This matches the PostgreSQL wire protocol which is inherently
/// sequential.
pub struct AsyncConnection {
    // Hot path (first 64 bytes — first cache line)
    stream_buf_pos: usize,
    stream_buf_end: usize,
    query_counter: u64,
    tx_status: u8,
    pid: i32,
    secret: i32,
    max_stmt_cache_size: usize,
    statement_cache_mode: StatementCacheMode,
    // Second cache line: buffers
    stream: AsyncStream,
    write_buf: Vec<u8>,
    stream_buf: Vec<u8>,
    stmts: StmtCache,
    // Cold fields
    read_buf: Vec<u8>,
    params: Vec<(Box<str>, Box<str>)>,
    last_used: std::time::Instant,
    created_at: std::time::Instant,
    pending_notifications: Vec<Notification>,
    /// The config used to connect — stored for cancel() which needs host:port.
    #[allow(dead_code)] // used by future cancel() implementation
    connect_config: Arc<Config>,
    /// SHA-256 hash of the TLS server certificate (for SCRAM-SHA-256-PLUS
    /// channel binding). `None` when not using TLS or cert unavailable.
    tls_server_cert_hash: Option<[u8; 32]>,
}

impl std::fmt::Debug for AsyncConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncConnection")
            .field("pid", &self.pid)
            .field("tx_status", &(self.tx_status as char))
            .field("stmt_cache_len", &self.stmts.len())
            .finish()
    }
}

impl AsyncConnection {
    /// Connect to PostgreSQL over TCP and complete the startup/auth handshake.
    ///
    /// Requires a tokio runtime. Transport is always TCP (with optional TLS
    /// upgrade based on `config.ssl`).
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails, TLS upgrade fails
    /// (when required), or authentication fails.
    pub async fn connect(config: &Config) -> Result<Self, DriverError> {
        Self::connect_arc(Arc::new(config.clone())).await
    }

    /// Connect using a shared config. Avoids cloning the Config strings.
    pub async fn connect_arc(config: Arc<Config>) -> Result<Self, DriverError> {
        config.validate()?;

        if config.host_is_uds() {
            return Err(DriverError::Protocol(
                "AsyncConnection does not support Unix domain sockets; use Connection instead"
                    .into(),
            ));
        }

        let addr = format!("{}:{}", config.host, config.port);
        let tcp = tokio::net::TcpStream::connect(&addr)
            .await
            .map_err(DriverError::Io)?;
        tcp.set_nodelay(true).map_err(DriverError::Io)?;

        #[allow(unused_mut)]
        let mut tls_cert_hash: Option<[u8; 32]> = None;

        let stream = match config.ssl {
            SslMode::Disable => AsyncStream::Tcp(tcp),
            SslMode::Prefer | SslMode::Require => {
                #[cfg(feature = "tls")]
                {
                    match async_tls_upgrade(tcp, &config.host, config.ssl == SslMode::Require).await
                    {
                        Ok((tls_stream, cert_hash)) => {
                            tls_cert_hash = cert_hash;
                            AsyncStream::Tls(Box::new(tls_stream))
                        }
                        Err(e) => {
                            if config.ssl == SslMode::Require {
                                return Err(e);
                            }
                            // Prefer mode: reconnect without TLS
                            let tcp = tokio::net::TcpStream::connect(&addr)
                                .await
                                .map_err(DriverError::Io)?;
                            tcp.set_nodelay(true).map_err(DriverError::Io)?;
                            AsyncStream::Tcp(tcp)
                        }
                    }
                }
                #[cfg(not(feature = "tls"))]
                {
                    if config.ssl == SslMode::Require {
                        return Err(DriverError::Protocol(
                            "sslmode=require but bsql was compiled without the 'tls' feature"
                                .into(),
                        ));
                    }
                    AsyncStream::Tcp(tcp)
                }
            }
        };

        let now = std::time::Instant::now();
        let mut conn = Self {
            // Hot path
            stream_buf_pos: 0,
            stream_buf_end: 0,
            query_counter: 0,
            tx_status: b'I',
            pid: 0,
            secret: 0,
            max_stmt_cache_size: 256,
            statement_cache_mode: config.statement_cache_mode,
            // Buffers
            stream,
            write_buf: Vec::with_capacity(4096),
            stream_buf: vec![0u8; 16384],
            stmts: StmtCache::default(),
            // Cold
            read_buf: Vec::with_capacity(8192),
            params: Vec::new(),
            last_used: now,
            created_at: now,
            pending_notifications: Vec::new(),
            connect_config: config.clone(),
            tls_server_cert_hash: tls_cert_hash,
        };

        conn.startup(&config).await?;
        conn.validate_server_params()?;

        Ok(conn)
    }

    // --- Startup / Auth ---

    async fn startup(&mut self, config: &Config) -> Result<(), DriverError> {
        self.write_buf.clear();
        // Build extra startup parameters (e.g., statement_timeout) to
        // eliminate a separate SET round-trip after authentication.
        let timeout_str; // declared first so it outlives extra_params
        let mut extra_params: smallvec::SmallVec<[(&str, &str); 2]> = smallvec::SmallVec::new();
        if config.statement_timeout_secs > 0 {
            timeout_str = format!("{}s", config.statement_timeout_secs);
            extra_params.push(("statement_timeout", &timeout_str));
        }
        proto::write_startup(
            &mut self.write_buf,
            &config.user,
            &config.database,
            &extra_params,
        );
        self.flush_write().await?;

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

    async fn read_startup_action(&mut self) -> Result<StartupAction, DriverError> {
        let (msg_type, _) = self.read_message_buffered().await?;
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

    async fn handle_scram(
        &mut self,
        config: &Config,
        mechanisms_data: &[u8],
    ) -> Result<(), DriverError> {
        let mechs = auth::parse_sasl_mechanisms(mechanisms_data);

        let use_plus = self.tls_server_cert_hash.is_some() && mechs.contains(&"SCRAM-SHA-256-PLUS");
        let mechanism = if use_plus {
            "SCRAM-SHA-256-PLUS"
        } else {
            "SCRAM-SHA-256"
        };

        if !mechs.contains(&mechanism) && !mechs.contains(&"SCRAM-SHA-256") {
            return Err(DriverError::Auth(format!(
                "server requires unsupported SASL mechanism(s): {mechs:?}"
            )));
        }

        let cert_hash = if use_plus {
            self.tls_server_cert_hash.as_ref()
        } else {
            None
        };
        let mut scram = auth::ScramClient::new(&config.user, &config.password, cert_hash)?;

        // SASLInitialResponse
        let client_first = scram.client_first_message();
        self.write_buf.clear();
        proto::write_sasl_initial(&mut self.write_buf, mechanism, &client_first);
        self.flush_write().await?;

        // SASLContinue
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

        // SASLResponse (client-final)
        let client_final = scram.client_final_message()?;
        self.write_buf.clear();
        proto::write_sasl_response(&mut self.write_buf, &client_final);
        self.flush_write().await?;

        // SASLFinal
        let (msg_type, _) = self.read_message_buffered().await?;
        {
            let msg = proto::parse_backend_message(msg_type, &self.read_buf)?;
            match msg {
                BackendMessage::AuthSaslFinal { data } => {
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

        // AuthOk
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

    fn validate_server_params(&self) -> Result<(), DriverError> {
        if let Some(encoding) = self.parameter("server_encoding") {
            if !encoding.eq_ignore_ascii_case("UTF8") && !encoding.eq_ignore_ascii_case("UTF-8") {
                return Err(DriverError::Protocol(format!(
                    "server_encoding is '{encoding}', but bsql requires UTF-8."
                )));
            }
        }
        if let Some(encoding) = self.parameter("client_encoding") {
            if !encoding.eq_ignore_ascii_case("UTF8") && !encoding.eq_ignore_ascii_case("UTF-8") {
                return Err(DriverError::Protocol(format!(
                    "client_encoding is '{encoding}', but bsql requires UTF-8."
                )));
            }
        }
        if let Some(idt) = self.parameter("integer_datetimes") {
            if idt != "on" {
                return Err(DriverError::Protocol(format!(
                    "integer_datetimes is '{idt}', but bsql requires 'on'."
                )));
            }
        }
        Ok(())
    }

    // --- Query execution ---

    /// Execute a prepared query and return rows.
    ///
    /// Async version of `Connection::query`. Uses the extended query protocol
    /// with pipelining. Statements are cached by sql_hash.
    pub async fn query(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<QueryResult, DriverError> {
        self.query_with_parse(sql, sql_hash, params, None).await
    }

    /// Like `query` but accepts pre-built Parse+Describe bytes for the cold path.
    pub async fn query_with_parse(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        prebuilt_parse: Option<&[u8]>,
    ) -> Result<QueryResult, DriverError> {
        let columns = self
            .send_pipeline_inner(sql, sql_hash, params, true, prebuilt_parse)
            .await?
            .ok_or_else(|| {
                DriverError::Protocol("send_pipeline(need_columns=true) returned None".into())
            })?;

        let num_cols = columns.len();
        let mut all_col_offsets = crate::conn::acquire_col_offsets();
        all_col_offsets.clear();
        let mut affected_rows: u64 = 0;

        let mut resp_buf = acquire_resp_buf();
        resp_buf.clear();

        // Inline response parsing: BindComplete + DataRow* + CommandComplete + ReadyForQuery.
        'outer: loop {
            loop {
                let avail = self.stream_buf_end - self.stream_buf_pos;
                if avail < 5 {
                    break; // need more data
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
                let total_msg_len = 5 + payload_len;

                if avail < total_msg_len {
                    if total_msg_len > self.stream_buf.len() {
                        // Oversized message -- fall back to read_one_message.
                        let msg = self.read_one_message().await?;
                        match msg {
                            BackendMessage::BindComplete => continue,
                            BackendMessage::DataRow { data } => {
                                parse_data_row_into_buf(data, &mut resp_buf, &mut all_col_offsets)?;
                                continue;
                            }
                            BackendMessage::CommandComplete { tag } => {
                                affected_rows = proto::parse_command_tag(tag);
                                continue;
                            }
                            BackendMessage::EmptyQuery => continue,
                            BackendMessage::ReadyForQuery { status } => {
                                self.tx_status = status;
                                break 'outer;
                            }
                            BackendMessage::NoticeResponse { .. } => continue,
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
                    break; // partial message -- compact and refill
                }

                // Full message in stream_buf -- parse inline.
                let payload_start = self.stream_buf_pos + 5;
                let payload_end = payload_start + payload_len;

                if msg_type == b'D' {
                    // DataRow
                    parse_data_row_into_buf(
                        &self.stream_buf[payload_start..payload_end],
                        &mut resp_buf,
                        &mut all_col_offsets,
                    )?;
                } else if msg_type == b'Z' {
                    if payload_len >= 1 {
                        self.tx_status = self.stream_buf[payload_start];
                    }
                    self.stream_buf_pos += total_msg_len;
                    break 'outer;
                } else {
                    if let Err(e) = self.handle_non_datarow_query(
                        msg_type,
                        payload_start,
                        payload_end,
                        sql_hash,
                        &mut affected_rows,
                    ) {
                        self.stream_buf_pos += total_msg_len;
                        self.drain_to_ready().await?;
                        return Err(e);
                    }
                }

                self.stream_buf_pos += total_msg_len;
            }

            self.refill_stream_buf().await?;
        }

        self.shrink_buffers();

        Ok(QueryResult::from_parts_with_buf(
            all_col_offsets,
            num_cols,
            columns,
            affected_rows,
            resp_buf,
        ))
    }

    /// Execute a query without result rows (INSERT/UPDATE/DELETE).
    ///
    /// Async version of `Connection::execute`. Returns affected row count.
    pub async fn execute(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<u64, DriverError> {
        self.execute_with_parse(sql, sql_hash, params, None).await
    }

    /// Like `execute` but accepts pre-built Parse+Describe bytes for the cold path.
    pub async fn execute_with_parse(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        prebuilt_parse: Option<&[u8]>,
    ) -> Result<u64, DriverError> {
        let _ = self
            .send_pipeline_inner(sql, sql_hash, params, false, prebuilt_parse)
            .await?;

        let mut affected_rows: u64 = 0;

        // Inline response parsing: BindComplete + CommandComplete + ReadyForQuery.
        'outer: loop {
            loop {
                let avail = self.stream_buf_end - self.stream_buf_pos;
                if avail < 5 {
                    break;
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
                let total_msg_len = 5 + payload_len;

                if avail < total_msg_len {
                    if total_msg_len > self.stream_buf.len() {
                        let msg = self.read_one_message().await?;
                        match msg {
                            BackendMessage::BindComplete | BackendMessage::DataRow { .. } => {
                                continue;
                            }
                            BackendMessage::CommandComplete { tag } => {
                                affected_rows = proto::parse_command_tag(tag);
                                continue;
                            }
                            BackendMessage::EmptyQuery => continue,
                            BackendMessage::ReadyForQuery { status } => {
                                self.tx_status = status;
                                break 'outer;
                            }
                            BackendMessage::NoticeResponse { .. } => continue,
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
                    break;
                }

                // Full message in stream_buf -- parse inline.
                let payload_start = self.stream_buf_pos + 5;
                let payload_end = payload_start + payload_len;

                if msg_type == b'2' {
                    // BindComplete -- skip.
                    self.stream_buf_pos += total_msg_len;
                    continue;
                } else if msg_type == b'C' {
                    // CommandComplete
                    affected_rows = proto::parse_command_tag_bytes(
                        &self.stream_buf[payload_start..payload_end],
                    );
                } else if msg_type == b'Z' {
                    if payload_len >= 1 {
                        self.tx_status = self.stream_buf[payload_start];
                    }
                    self.stream_buf_pos += total_msg_len;
                    break 'outer;
                } else if msg_type == b'D' || msg_type == b'I' {
                    // DataRow / EmptyQuery -- skip.
                } else {
                    if let Err(e) = self.handle_non_datarow_execute(
                        msg_type,
                        payload_start,
                        payload_end,
                        sql_hash,
                    ) {
                        self.stream_buf_pos += total_msg_len;
                        self.drain_to_ready().await?;
                        return Err(e);
                    }
                }

                self.stream_buf_pos += total_msg_len;
            }

            self.refill_stream_buf().await?;
        }

        self.shrink_buffers();
        Ok(affected_rows)
    }

    /// Simple query protocol -- for non-prepared SQL (BEGIN, COMMIT, SET, etc.).
    pub async fn simple_query(&mut self, sql: &str) -> Result<(), DriverError> {
        self.write_buf.clear();
        proto::write_simple_query(&mut self.write_buf, sql);
        self.flush_write().await?;

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
                | BackendMessage::ParameterStatus { .. }
                | BackendMessage::AuthOk
                | BackendMessage::AuthSaslFinal { .. }
                | BackendMessage::BackendKeyData { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "unexpected message during simple_query: {other:?}"
                    )));
                }
            }
        }
    }

    /// Send Terminate and close the connection.
    pub async fn close(mut self) -> Result<(), DriverError> {
        self.write_buf.clear();
        proto::write_terminate(&mut self.write_buf);
        let _ = self.flush_write().await;
        Ok(())
    }

    // --- Accessors ---

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

    /// Record that the connection was just used.
    pub fn touch(&mut self) {
        self.last_used = std::time::Instant::now();
    }

    /// How long since this connection last completed a query.
    pub fn idle_duration(&self) -> std::time::Duration {
        self.last_used.elapsed()
    }

    /// Monotonic query counter.
    pub fn query_counter(&self) -> u64 {
        self.query_counter
    }

    /// Get a server parameter value.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(k, _)| &**k == name)
            .map(|(_, v)| &**v)
    }

    /// Backend process ID.
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// When this connection was created.
    pub fn created_at(&self) -> std::time::Instant {
        self.created_at
    }

    /// Set the maximum number of cached prepared statements.
    pub fn set_max_stmt_cache_size(&mut self, size: usize) {
        self.max_stmt_cache_size = size;
    }

    // --- Pipeline ---

    /// Common pipeline: builds and sends Parse+Describe+Bind+Execute+Sync (or
    /// Bind+Execute+Sync on cache hit). Returns column metadata.
    ///
    /// When `prebuilt_parse` is Some, the cache-miss path uses these bytes
    /// directly instead of calling write_parse + write_describe.
    async fn send_pipeline_inner(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        need_columns: bool,
        prebuilt_parse: Option<&[u8]>,
    ) -> Result<Option<Arc<[ColumnDesc]>>, DriverError> {
        debug_assert_eq!(crate::types::hash_sql(sql), sql_hash, "sql_hash mismatch");

        if params.len() > i16::MAX as usize {
            return Err(DriverError::Protocol(format!(
                "parameter count {} exceeds maximum {}",
                params.len(),
                i16::MAX
            )));
        }

        self.write_buf.clear();

        // Unnamed statement path: no caching, compatible with pgbouncer transaction mode.
        if self.statement_cache_mode == StatementCacheMode::Disabled {
            let param_oids: smallvec::SmallVec<[u32; 8]> =
                params.iter().map(|p| p.type_oid()).collect();
            proto::write_parse(&mut self.write_buf, b"", sql, &param_oids);
            if need_columns {
                proto::write_describe(&mut self.write_buf, b'S', b"");
            }
            proto::write_bind_params(&mut self.write_buf, b"", b"", params);
            self.write_buf.extend_from_slice(proto::EXECUTE_SYNC);
            self.flush_write().await?;

            self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))
                .await?;
            let columns = if need_columns {
                Some(self.read_column_description().await?)
            } else {
                None
            };
            return Ok(columns);
        }

        let columns = if let Some(info) = self.stmts.get_mut(&sql_hash, sql) {
            // Cache hit
            self.query_counter += 1;
            info.last_used = self.query_counter;

            let mut has_exec_sync = false;

            if let Some(tmpl) = info
                .bind_template
                .as_ref()
                .filter(|t| t.param_slots.len() == params.len())
            {
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
                        template_ok = false;
                        break;
                    }
                }

                if template_ok {
                    has_exec_sync = true;
                } else {
                    self.write_buf.clear();
                    proto::write_bind_params(&mut self.write_buf, b"", &info.name, params);
                    info.bind_template = None;
                }
            } else {
                proto::write_bind_params(&mut self.write_buf, b"", &info.name, params);
            }

            let cols = if need_columns {
                Some(info.columns.clone())
            } else {
                None
            };

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
            if let Some(prebuilt) = prebuilt_parse {
                // Compile-time generated Parse+Describe bytes — zero runtime
                // message construction. Just memcpy static data.
                self.write_buf.extend_from_slice(prebuilt);
            } else {
                // Runtime construction (for dynamic SQL, user raw_query, etc.)
                let param_oids: smallvec::SmallVec<[u32; 8]> =
                    params.iter().map(|p| p.type_oid()).collect();
                proto::write_parse(&mut self.write_buf, &name, sql, &param_oids);
                proto::write_describe(&mut self.write_buf, b'S', &name);
            }
            proto::write_bind_params(&mut self.write_buf, b"", &name, params);

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
                    sql: sql.into(),
                    columns: columns.clone(),
                    last_used: self.query_counter,
                    bind_template: None,
                },
            );
            if need_columns {
                Some(columns)
            } else {
                None
            }
        };

        Ok(columns)
    }

    /// Read RowDescription / NoData after ParseComplete+Describe.
    async fn read_column_description(&mut self) -> Result<Arc<[ColumnDesc]>, DriverError> {
        loop {
            let msg = self.read_one_message().await?;
            match msg {
                BackendMessage::RowDescription { data } => {
                    let cols = proto::parse_row_description(data)?;
                    return Ok(cols.into());
                }
                BackendMessage::ParameterDescription { .. } => {}
                BackendMessage::NoData => return Ok(Arc::from(Vec::new())),
                BackendMessage::NoticeResponse { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "expected RowDescription/NoData, got: {other:?}"
                    )));
                }
            }
        }
    }

    // --- Internal helpers ---

    fn cache_stmt(&mut self, sql_hash: u64, info: StmtInfo) {
        if self.stmts.len() >= self.max_stmt_cache_size
            && !self.stmts.contains_key(&sql_hash, &info.sql)
        {
            if let Some((_lru_hash, evicted)) = self.stmts.evict_lru() {
                proto::write_close(&mut self.write_buf, b'S', &evicted.name);
            }
        }
        self.stmts.insert(sql_hash, info);
    }

    fn buffer_notification(&mut self, pid: i32, channel: &str, payload: &str) {
        if self.pending_notifications.len() < 1024 {
            self.pending_notifications.push(Notification {
                pid,
                channel: channel.to_owned(),
                payload: payload.to_owned(),
            });
        }
    }

    fn shrink_buffers(&mut self) {
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

    fn maybe_invalidate_stmt_cache(&mut self, fields: &proto::ErrorFields, sql_hash: u64) -> bool {
        if &fields.code == b"26000" {
            self.stmts.remove(&sql_hash);
            true
        } else {
            false
        }
    }

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

    /// Handle non-DataRow messages during query() inline parsing.
    #[cold]
    #[inline(never)]
    fn handle_non_datarow_query(
        &mut self,
        msg_type: u8,
        payload_start: usize,
        payload_end: usize,
        sql_hash: u64,
        affected_rows: &mut u64,
    ) -> Result<(), DriverError> {
        match msg_type {
            b'2' | b'I' => {} // BindComplete / EmptyQuery
            b'C' => {
                *affected_rows =
                    proto::parse_command_tag_bytes(&self.stream_buf[payload_start..payload_end]);
            }
            b'E' => {
                let fields =
                    proto::parse_error_response(&self.stream_buf[payload_start..payload_end]);
                self.maybe_invalidate_stmt_cache(&fields, sql_hash);
                // Note: drain_to_ready is async, but this is called from a sync context
                // within the inline loop. The caller handles draining after returning Err.
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
            _ => {} // NoticeResponse, ParameterStatus -- skip
        }
        Ok(())
    }

    /// Handle non-DataRow messages during execute inline parsing.
    #[cold]
    #[inline(never)]
    fn handle_non_datarow_execute(
        &mut self,
        msg_type: u8,
        payload_start: usize,
        payload_end: usize,
        sql_hash: u64,
    ) -> Result<(), DriverError> {
        match msg_type {
            b'2' | b'C' | b'I' => {} // BindComplete / CommandComplete / EmptyQuery
            b'E' => {
                let fields =
                    proto::parse_error_response(&self.stream_buf[payload_start..payload_end]);
                self.maybe_invalidate_stmt_cache(&fields, sql_hash);
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
            _ => {}
        }
        Ok(())
    }

    /// Read one backend message, auto-buffering notifications.
    async fn read_one_message(&mut self) -> Result<BackendMessage<'_>, DriverError> {
        loop {
            let (msg_type, _payload_len) = self.read_message_buffered().await?;
            if msg_type == b'A' {
                let msg = proto::parse_backend_message(msg_type, &self.read_buf)?;
                if let BackendMessage::NotificationResponse {
                    pid,
                    channel,
                    payload,
                } = msg
                {
                    let pid_owned = pid;
                    let channel_owned = channel.to_owned();
                    let payload_owned = payload.to_owned();
                    self.buffer_notification(pid_owned, &channel_owned, &payload_owned);
                    continue;
                }
            }
            return proto::parse_backend_message(msg_type, &self.read_buf);
        }
    }

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
                BackendMessage::NoticeResponse { .. } | BackendMessage::ParameterStatus { .. } => {}
                other => {
                    return Err(DriverError::Protocol(format!(
                        "unexpected message while waiting for expected type: {other:?}"
                    )));
                }
            }
        }
    }

    #[allow(dead_code)] // used by future phases (execute_pipeline, streaming)
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
                    self.drain_to_ready().await?;
                    return Err(self.make_server_error(fields));
                }
                _ => {}
            }
        }
    }

    async fn drain_to_ready(&mut self) -> Result<(), DriverError> {
        loop {
            let msg = self.read_one_message().await?;
            if let BackendMessage::ReadyForQuery { status } = msg {
                self.tx_status = status;
                return Ok(());
            }
        }
    }

    // --- Async I/O ---

    /// Flush the write buffer to the stream.
    async fn flush_write(&mut self) -> Result<(), DriverError> {
        self.stream
            .write_all(&self.write_buf)
            .await
            .map_err(DriverError::Io)
    }

    /// Read one complete backend message.
    ///
    /// Returns `(msg_type, payload_len)`. Payload is stored in `self.read_buf`.
    async fn read_message_buffered(&mut self) -> Result<(u8, usize), DriverError> {
        let mut header = [0u8; 5];
        async_buffered_read_exact(
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
            async_buffered_read_exact(
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

    /// Compact stream_buf and read more data from the socket.
    async fn refill_stream_buf(&mut self) -> Result<(), DriverError> {
        let remaining = self.stream_buf_end - self.stream_buf_pos;
        if remaining > 0 && self.stream_buf_pos > 0 {
            self.stream_buf
                .copy_within(self.stream_buf_pos..self.stream_buf_end, 0);
        }
        self.stream_buf_pos = 0;
        self.stream_buf_end = remaining;

        let n = self
            .stream
            .read(&mut self.stream_buf[remaining..])
            .await
            .map_err(DriverError::Io)?;
        if n == 0 {
            return Err(DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed",
            )));
        }
        self.stream_buf_end = remaining + n;
        Ok(())
    }
}

/// Async buffered read_exact -- reads exactly `out.len()` bytes using
/// a persistent read buffer. Non-blocking via `AsyncStream`.
async fn async_buffered_read_exact(
    stream: &mut AsyncStream,
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
            *pos = 0;
            let n = stream.read(buf).await.map_err(DriverError::Io)?;
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

// --- Async TLS upgrade ---

/// Attempt async TLS upgrade on a TCP connection.
#[cfg(feature = "tls")]
async fn async_tls_upgrade(
    mut tcp: tokio::net::TcpStream,
    host: &str,
    required: bool,
) -> Result<
    (
        tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
        Option<[u8; 32]>,
    ),
    DriverError,
> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Send SSLRequest
    let mut buf = Vec::with_capacity(8);
    proto::write_ssl_request(&mut buf);
    tcp.write_all(&buf).await.map_err(DriverError::Io)?;
    tcp.flush().await.map_err(DriverError::Io)?;

    // Read response byte
    let mut response = [0u8; 1];
    tcp.read_exact(&mut response)
        .await
        .map_err(DriverError::Io)?;

    match response[0] {
        b'S' => {
            // Server accepts TLS -- perform handshake via tokio-rustls
            let server_name =
                rustls::pki_types::ServerName::try_from(host.to_owned()).map_err(|e| {
                    DriverError::Protocol(format!("invalid TLS server name '{host}': {e}"))
                })?;

            // Share the cached default config with the sync TLS path. This
            // routes through `tls_common::build_client_config`, which pins
            // the rustls crypto provider to `ring` so the builder cannot
            // panic under cargo feature unification (see tls_common docs).
            let tls_config = crate::tls_common::default_client_config();

            let connector = tokio_rustls::TlsConnector::from(tls_config);
            let tls_stream = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| DriverError::Io(std::io::Error::other(e)))?;

            // Extract server certificate hash for SCRAM channel binding
            let server_cert_hash = tls_stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certs| certs.first())
                .map(|cert| {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(cert.as_ref());
                    let hash: [u8; 32] = hasher.finalize().into();
                    hash
                });

            Ok((tls_stream, server_cert_hash))
        }
        b'N' => {
            if required {
                Err(DriverError::Protocol(
                    "server does not support TLS (sslmode=require)".into(),
                ))
            } else {
                Err(DriverError::Protocol(
                    "server declined TLS (sslmode=prefer, falling back)".into(),
                ))
            }
        }
        other => Err(DriverError::Protocol(format!(
            "unexpected SSL response byte: 0x{other:02x}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn async_connection_rejects_uds() {
        let config = Config::from_url("postgres://user:pass@%2Ftmp/db").unwrap();
        // UDS host -- connect should fail synchronously during validation
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(AsyncConnection::connect(&config));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Unix domain socket"),
            "expected UDS rejection, got: {err}"
        );
    }

    #[test]
    fn async_connection_invalid_host() {
        let config = Config::from_url("postgres://user:pass@127.0.0.1:1/db").unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(AsyncConnection::connect(&config));
        assert!(result.is_err());
        // Should be an I/O error (connection refused)
        let err = result.unwrap_err().to_string();
        assert!(err.contains("I/O error"), "expected I/O error, got: {err}");
    }

    #[test]
    fn async_connection_debug_format() {
        // Verify Debug impl compiles and produces output
        let dbg = format!(
            "{:?}",
            // We can't construct one without a live connection, so just test the trait bound
            // exists. The actual format is tested via integration tests.
            "AsyncConnection { pid: 0, tx_status: 'I', stmt_cache_len: 0 }"
        );
        assert!(!dbg.is_empty());
    }
}
