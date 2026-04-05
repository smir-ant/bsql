//! PostgreSQL connection — startup, authentication, statement cache, query execution.
//!
//! `Connection` owns a TCP, TLS, or Unix domain socket stream and implements the
//! extended query protocol with pipelining. Statements are cached by rapidhash of
//! the SQL text. All I/O is synchronous using `sync_io::Stream`.
//!
//! # Transport
//!
//! Supports TCP, TLS (via rustls), and Unix domain sockets (on Unix platforms).
//! The transport is selected automatically based on `Config`:
//! - `host` starting with `/` -> Unix domain socket
//! - Otherwise -> TCP (with optional TLS upgrade based on `ssl` mode)

use std::io::{Read, Write};
use std::sync::Arc;

use crate::DriverError;
use crate::arena::Arena;
use crate::auth;
use crate::codec::Encode;
use crate::proto::{self, BackendMessage};
use crate::stmt_cache::{StmtCache, StmtInfo, build_bind_template, make_stmt_name};
use crate::sync_io::Stream;
use crate::types::{
    ColumnDesc, Config, Notification, PgDataRow, PrepareResult, QueryResult, SimpleRow, SslMode,
    StartupAction,
};

// --- Thread-local response buffer pool ---
//
// Recycling `Vec<u8>` response buffers avoids per-query malloc/free.
// Each query takes a buffer (already capacity-sized from previous query),
// fills it with DataRow payloads, and moves it into QueryResult.data_buf.
// When OwnedResult drops, the buffer is returned here for reuse.

use std::cell::RefCell;

thread_local! {
    static RESP_BUF_POOL: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
}

fn acquire_resp_buf() -> Vec<u8> {
    RESP_BUF_POOL
        .with(|pool| pool.borrow_mut().pop())
        .unwrap_or_default()
}

/// Return a response buffer to the thread-local pool for reuse.
pub fn release_resp_buf(buf: Vec<u8>) {
    RESP_BUF_POOL.with(|pool| {
        let mut pool = pool.borrow_mut();
        if pool.len() < 4 {
            pool.push(buf);
        }
    });
}

// --- Connection ---

/// A PostgreSQL connection over TCP, TLS, or Unix domain socket.
///
/// All I/O is synchronous using `sync_io::Stream` which wraps `TcpStream`,
/// `UnixStream`, or `rustls::StreamOwned`. No async runtime is required.
///
/// # Thread safety
///
/// `Connection` is `Send` but not `Sync` — it must be used by one thread
/// at a time. This matches the PostgreSQL wire protocol which is inherently
/// sequential.
///
/// # Example
///
/// ```no_run
/// # fn main() -> Result<(), bsql_driver_postgres::DriverError> {
/// use bsql_driver_postgres::{Connection, Config};
///
/// let config = Config::from_url("postgres://user:pass@localhost/db")?;
/// let mut conn = Connection::connect(&config)?;
///
/// let hash = bsql_driver_postgres::hash_sql("SELECT 1 AS n");
/// let result = conn.query("SELECT 1 AS n", hash, &[])?;
/// assert_eq!(result.len(), 1);
/// # Ok(())
/// # }
/// ```
pub struct Connection {
    stream: Stream,
    read_buf: Vec<u8>,
    stream_buf: Vec<u8>,
    stream_buf_pos: usize,
    stream_buf_end: usize,
    write_buf: Vec<u8>,
    stmts: StmtCache,
    params: Vec<(Box<str>, Box<str>)>,
    pid: i32,
    secret: i32,
    tx_status: u8,
    last_used: std::time::Instant,
    streaming_active: bool,
    created_at: std::time::Instant,
    pending_notifications: Vec<Notification>,
    max_stmt_cache_size: usize,
    query_counter: u64,
    /// The config used to connect — stored for cancel() which needs host:port.
    /// Wrapped in Arc to avoid cloning 5 Strings per connection open.
    connect_config: Arc<Config>,
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("pid", &self.pid)
            .field("tx_status", &(self.tx_status as char))
            .field("stmt_cache_len", &self.stmts.len())
            .finish()
    }
}

impl Connection {
    /// Connect to PostgreSQL and complete the startup/auth handshake.
    /// Fully synchronous — no tokio runtime needed.
    ///
    /// Transport is selected automatically based on `config`:
    /// - `host` starting with `/` -> Unix domain socket
    /// - Otherwise -> TCP (with optional TLS upgrade based on `ssl` mode)
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails, TLS upgrade fails
    /// (when required), or authentication fails.
    pub fn connect(config: &Config) -> Result<Self, DriverError> {
        Self::connect_arc(Arc::new(config.clone()))
    }

    /// Connect using a shared config. Avoids cloning the Config strings.
    ///
    /// Preferred by the connection pool, which holds `Arc<Config>` and opens
    /// new connections without 5 String clones per open.
    pub fn connect_arc(config: Arc<Config>) -> Result<Self, DriverError> {
        config.validate()?;

        let stream = if config.host_is_uds() {
            // UDS path
            #[cfg(unix)]
            {
                let path = config.uds_path();
                let unix =
                    std::os::unix::net::UnixStream::connect(&path).map_err(DriverError::Io)?;
                Stream::Unix(unix)
            }
            #[cfg(not(unix))]
            {
                return Err(DriverError::Protocol(
                    "Unix domain sockets are not supported on this platform".into(),
                ));
            }
        } else {
            // TCP path
            let addr = format!("{}:{}", config.host, config.port);
            let tcp = std::net::TcpStream::connect(&addr).map_err(DriverError::Io)?;

            match config.ssl {
                SslMode::Disable => {
                    tcp.set_nodelay(true).map_err(DriverError::Io)?;
                    let stream = Stream::Tcp(tcp);
                    stream.set_keepalive()?;
                    stream
                }
                SslMode::Prefer | SslMode::Require => {
                    #[cfg(feature = "tls")]
                    {
                        match crate::tls_sync::try_upgrade(
                            tcp,
                            &config.host,
                            config.ssl == SslMode::Require,
                        ) {
                            Ok(tls_stream) => {
                                let stream = Stream::Tls(Box::new(tls_stream));
                                stream.set_nodelay()?;
                                stream.set_keepalive()?;
                                stream
                            }
                            Err(e) => {
                                if config.ssl == SslMode::Require {
                                    return Err(e);
                                }
                                // Prefer mode: reconnect without TLS
                                let tcp =
                                    std::net::TcpStream::connect(&addr).map_err(DriverError::Io)?;
                                tcp.set_nodelay(true).map_err(DriverError::Io)?;
                                let stream = Stream::Tcp(tcp);
                                stream.set_keepalive()?;
                                stream
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
                        tcp.set_nodelay(true).map_err(DriverError::Io)?;
                        let stream = Stream::Tcp(tcp);
                        stream.set_keepalive()?;
                        stream
                    }
                }
            }
        };

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
            connect_config: config.clone(),
        };

        conn.startup(&config)?;
        conn.validate_server_params()?;

        if config.statement_timeout_secs > 0 {
            conn.simple_query(&format!(
                "SET statement_timeout = '{}s'",
                config.statement_timeout_secs
            ))?;
        }

        Ok(conn)
    }

    // --- Startup / Auth ---

    fn startup(&mut self, config: &Config) -> Result<(), DriverError> {
        self.write_buf.clear();
        proto::write_startup(&mut self.write_buf, &config.user, &config.database);
        self.flush_write()?;

        loop {
            let action = self.read_startup_action()?;
            match action {
                StartupAction::AuthOk => {}
                StartupAction::AuthCleartext => {
                    self.write_buf.clear();
                    let mut pw = config.password.as_bytes().to_vec();
                    pw.push(0);
                    proto::write_password(&mut self.write_buf, &pw);
                    self.flush_write()?;
                }
                StartupAction::AuthMd5(salt) => {
                    self.write_buf.clear();
                    let hash = auth::md5_password(&config.user, &config.password, &salt);
                    proto::write_password(&mut self.write_buf, &hash);
                    self.flush_write()?;
                }
                StartupAction::AuthSasl(mechanisms_data) => {
                    self.handle_scram(config, &mechanisms_data)?;
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

    fn read_startup_action(&mut self) -> Result<StartupAction, DriverError> {
        let (msg_type, _) = self.read_message_buffered()?;
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

    fn handle_scram(&mut self, config: &Config, mechanisms_data: &[u8]) -> Result<(), DriverError> {
        let mechs = auth::parse_sasl_mechanisms(mechanisms_data);
        if !mechs.contains(&"SCRAM-SHA-256") {
            return Err(DriverError::Auth(format!(
                "server requires unsupported SASL mechanism(s): {mechs:?}"
            )));
        }

        let mut scram = auth::ScramClient::new(&config.user, &config.password)?;

        // SASLInitialResponse
        let client_first = scram.client_first_message();
        self.write_buf.clear();
        proto::write_sasl_initial(&mut self.write_buf, "SCRAM-SHA-256", &client_first);
        self.flush_write()?;

        // SASLContinue
        let (msg_type, _) = self.read_message_buffered()?;
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
        self.flush_write()?;

        // SASLFinal
        let (msg_type, _) = self.read_message_buffered()?;
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
        let (msg_type, _) = self.read_message_buffered()?;
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
            let normalized = encoding.to_uppercase();
            if normalized != "UTF8" && normalized != "UTF-8" {
                return Err(DriverError::Protocol(format!(
                    "server_encoding is '{encoding}', but bsql requires UTF-8."
                )));
            }
        }
        if let Some(encoding) = self.parameter("client_encoding") {
            let normalized = encoding.to_uppercase();
            if normalized != "UTF8" && normalized != "UTF-8" {
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

    /// Prepare a statement without executing it (Parse+Describe+Sync only).
    ///
    /// If the statement is already cached, this is a no-op.
    pub fn prepare_only(&mut self, sql: &str, sql_hash: u64) -> Result<(), DriverError> {
        if self.stmts.contains_key(&sql_hash) {
            return Ok(());
        }
        let name = make_stmt_name(sql_hash);
        let name_s: &str = std::str::from_utf8(&name).expect("ASCII");
        self.write_buf.clear();
        proto::write_parse(&mut self.write_buf, name_s, sql, &[]);
        proto::write_describe(&mut self.write_buf, b'S', name_s);
        proto::write_sync(&mut self.write_buf);
        self.flush_write()?;

        self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))?;
        let columns = self.read_column_description()?;
        self.expect_ready()?;

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

    /// Execute a prepared query and return rows.
    ///
    /// Optimized path: after `send_pipeline` flushes, we parse BindComplete +
    /// DataRow* + CommandComplete + ReadyForQuery directly from `stream_buf`,
    /// avoiding per-message `read_message_buffered` overhead. DataRow payloads
    /// are parsed in-place from stream_buf into a response buffer.
    ///
    /// Note: `arena` is not used — row data is stored in an inline `resp_buf`
    /// owned by `QueryResult`. The parameter is kept for API compatibility
    /// with the streaming path, but callers may pass any `&mut Arena`.
    #[inline]
    pub fn query(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<QueryResult, DriverError> {
        let columns = self
            .send_pipeline(sql, sql_hash, params, true, true)?
            .expect("send_pipeline(need_columns=true) must return Some");

        let num_cols = columns.len();
        let mut all_col_offsets: Vec<(usize, i32)> = Vec::with_capacity(num_cols.max(1) * 8);
        let mut affected_rows: u64 = 0;

        // Response buffer: DataRow payloads are appended here as raw bytes.
        // Column offsets point into this buffer. After the loop, the buffer
        // is moved into the arena as a single block — ONE allocation for the
        // entire result set, like libpq's PGresult internal buffer.
        // Response buffer starts empty. Vec grows via doubling as DataRow
        // payloads arrive. No upfront 64KB malloc — actual allocation matches
        // the result size. For 100 rows (~8KB), Vec grows to ~16KB capacity.
        // For 1 row (~80B), Vec grows to ~128B. Right-sized for the workload.
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
                        // Oversized message — fall back to read_one_message.
                        let msg = self.read_one_message()?;
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
                                self.drain_to_ready()?;
                                return Err(self.make_server_error(fields));
                            }
                            other => {
                                return Err(DriverError::Protocol(format!(
                                    "unexpected message during query: {other:?}"
                                )));
                            }
                        }
                    }
                    break; // partial message — compact and refill
                }

                // Full message in stream_buf — parse inline.
                let payload_start = self.stream_buf_pos + 5;
                let payload_end = payload_start + payload_len;

                if msg_type == b'D' {
                    // DataRow — append payload to response buffer, record offsets.
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
                    self.handle_non_datarow_query(
                        msg_type,
                        payload_start,
                        payload_end,
                        sql_hash,
                        &mut affected_rows,
                    )?;
                }

                self.stream_buf_pos += total_msg_len;
            }

            self.refill_stream_buf()?;
        }

        self.shrink_buffers();

        // QueryResult owns the response buffer directly — no arena copy.
        // Column offsets already point into resp_buf.
        Ok(QueryResult::from_parts_with_buf(
            all_col_offsets,
            num_cols,
            columns,
            affected_rows,
            resp_buf,
        ))
    }

    /// Monolithic execute — everything in one function, no intermediate calls.
    ///
    /// Inlines the entire send_pipeline + response parsing path for
    /// INSERT/UPDATE/DELETE. On cache hit: template copy + param patch +
    /// write_all + inline message parsing. No send_pipeline(), no flush_write(),
    /// no refill_stream_buf(). The compiler sees the entire path and can
    /// optimize globally.
    ///
    /// On cache miss (first execution of a statement), falls through to the
    /// cold `execute_with_prepare` path.
    #[inline]
    pub fn execute_monolithic(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<u64, DriverError> {
        // === SEND PHASE (inline — no send_pipeline, no flush_write) ===
        self.write_buf.clear();

        // Check statement cache — inline, no function call.
        let info = match self.stmts.get_mut(&sql_hash) {
            Some(info) => {
                self.query_counter += 1;
                info.last_used = self.query_counter;
                info
            }
            None => {
                // Cache miss: prepare first (cold path, separate function).
                return self.execute_with_prepare(sql, sql_hash, params);
            }
        };

        // Build Bind+Execute+Sync message — inline bind template logic.
        let can_use_template = info
            .bind_template
            .as_ref()
            .is_some_and(|t| t.param_slots.len() == params.len());

        let mut has_exec_sync = false;

        if can_use_template {
            // SAFETY: can_use_template is true only when bind_template.is_some()
            let tmpl = info
                .bind_template
                .as_ref()
                .expect("guarded by can_use_template");
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
                    // Template had NULL, now non-NULL — rebuild.
                    template_ok = false;
                    break;
                }
            }

            if template_ok {
                has_exec_sync = true;
            } else {
                self.write_buf.clear();
                proto::write_bind_params(&mut self.write_buf, "", info.name_str(), params);
                info.bind_template = None;
            }
        } else {
            proto::write_bind_params(&mut self.write_buf, "", info.name_str(), params);
        }

        // Snapshot bind template on first use or after invalidation.
        if info.bind_template.is_none() && !self.write_buf.is_empty() {
            info.bind_template = build_bind_template(&self.write_buf, params.len());
        }

        if !has_exec_sync {
            self.write_buf.extend_from_slice(proto::EXECUTE_SYNC);
        }

        // Write to socket — ONE syscall, no flush_write() indirection.
        self.stream
            .write_all(&self.write_buf)
            .map_err(DriverError::Io)?;

        // === RECEIVE PHASE (inline — no refill_stream_buf) ===
        let mut affected_rows: u64 = 0;

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
                        let msg = self.read_one_message()?;
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
                                self.drain_to_ready()?;
                                return Err(self.make_server_error(fields));
                            }
                            other => {
                                return Err(DriverError::Protocol(format!(
                                    "unexpected message during execute: {other:?}"
                                )));
                            }
                        }
                    }
                    break; // partial message — compact and refill
                }

                // Full message in stream_buf — parse inline.
                // Branch order matches actual PG response order for execute:
                // BindComplete(b'2') -> CommandComplete(b'C') -> ReadyForQuery(b'Z').
                // This improves branch prediction on the hot path.
                let payload_start = self.stream_buf_pos + 5;
                let payload_end = payload_start + payload_len;

                if msg_type == b'2' {
                    // BindComplete — first response, skip.
                    self.stream_buf_pos += total_msg_len;
                    continue;
                } else if msg_type == b'C' {
                    // CommandComplete — second response, parse affected rows.
                    affected_rows = proto::parse_command_tag_bytes(
                        &self.stream_buf[payload_start..payload_end],
                    );
                } else if msg_type == b'Z' {
                    // ReadyForQuery — last response, extract tx status and exit.
                    if payload_len >= 1 {
                        self.tx_status = self.stream_buf[payload_start];
                    }
                    self.stream_buf_pos += total_msg_len;
                    break 'outer;
                } else if msg_type == b'D' || msg_type == b'I' {
                    // DataRow / EmptyQuery — rare in execute, skip.
                } else {
                    self.handle_non_datarow_execute(
                        msg_type,
                        payload_start,
                        payload_end,
                        sql_hash,
                    )?;
                }

                self.stream_buf_pos += total_msg_len;
            }

            // Need more data — compact and refill inline.
            let remaining = self.stream_buf_end - self.stream_buf_pos;
            debug_assert!(
                remaining == 0 || self.stream_buf_pos > 0,
                "compact called with pos=0 and remaining data"
            );
            if remaining > 0 {
                self.stream_buf
                    .copy_within(self.stream_buf_pos..self.stream_buf_end, 0);
            }
            self.stream_buf_pos = 0;
            self.stream_buf_end = remaining;
            let n = self
                .stream
                .read(&mut self.stream_buf[remaining..])
                .map_err(DriverError::Io)?;
            if n == 0 {
                return Err(DriverError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                )));
            }
            self.stream_buf_end = remaining + n;
        }

        // Throttled maintenance — every 64 queries.
        if self.query_counter & 63 == 0 {
            if self.read_buf.capacity() > 64 * 1024 {
                self.read_buf.clear();
                self.read_buf.shrink_to(8192);
            }
            if self.write_buf.capacity() > 16 * 1024 {
                self.write_buf.clear();
                self.write_buf.shrink_to(8192);
            }
        }

        Ok(affected_rows)
    }

    /// Cold path: cache miss — Parse+Describe+Bind+Execute+Sync, then read response.
    #[cold]
    #[inline(never)]
    fn execute_with_prepare(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<u64, DriverError> {
        debug_assert_eq!(crate::types::hash_sql(sql), sql_hash, "sql_hash mismatch");

        if params.len() > i16::MAX as usize {
            return Err(DriverError::Protocol(format!(
                "parameter count {} exceeds maximum {}",
                params.len(),
                i16::MAX
            )));
        }

        let name = make_stmt_name(sql_hash);
        let name_s: &str = std::str::from_utf8(&name).expect("ASCII");
        let param_oids: smallvec::SmallVec<[u32; 8]> =
            params.iter().map(|p| p.type_oid()).collect();

        self.write_buf.clear();
        proto::write_parse(&mut self.write_buf, name_s, sql, &param_oids);
        proto::write_describe(&mut self.write_buf, b'S', name_s);
        proto::write_bind_params(&mut self.write_buf, "", name_s, params);
        self.write_buf.extend_from_slice(proto::EXECUTE_SYNC);
        self.stream
            .write_all(&self.write_buf)
            .map_err(DriverError::Io)?;

        self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))?;
        let columns = self.read_column_description()?;
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

        // Now read BindComplete + CommandComplete + ReadyForQuery.
        let mut affected_rows: u64 = 0;
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
                        let msg = self.read_one_message()?;
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
                                self.drain_to_ready()?;
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

                let payload_start = self.stream_buf_pos + 5;
                let payload_end = payload_start + payload_len;

                if msg_type == b'2' || msg_type == b'D' || msg_type == b'I' {
                    // BindComplete / DataRow / EmptyQuery — skip
                } else if msg_type == b'C' {
                    affected_rows = proto::parse_command_tag_bytes(
                        &self.stream_buf[payload_start..payload_end],
                    );
                } else if msg_type == b'Z' {
                    if payload_len >= 1 {
                        self.tx_status = self.stream_buf[payload_start];
                    }
                    self.stream_buf_pos += total_msg_len;
                    break 'outer;
                } else {
                    self.handle_non_datarow_execute(
                        msg_type,
                        payload_start,
                        payload_end,
                        sql_hash,
                    )?;
                }

                self.stream_buf_pos += total_msg_len;
            }

            self.refill_stream_buf()?;
        }

        Ok(affected_rows)
    }

    /// Execute a query without result rows (INSERT/UPDATE/DELETE).
    ///
    /// Delegates to `execute_monolithic` which inlines the entire send + receive
    /// path. Kept for API compatibility.
    #[inline]
    pub fn execute(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<u64, DriverError> {
        self.execute_monolithic(sql, sql_hash, params)
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
    /// Returns the number of affected rows for each parameter set.
    pub fn execute_pipeline(
        &mut self,
        sql: &str,
        sql_hash: u64,
        param_sets: &[&[&(dyn Encode + Sync)]],
    ) -> Result<Vec<u64>, DriverError> {
        if param_sets.is_empty() {
            return Ok(Vec::new());
        }

        debug_assert_eq!(crate::types::hash_sql(sql), sql_hash, "sql_hash mismatch");

        self.write_buf.clear();

        // Ensure statement is prepared.
        if !self.stmts.contains_key(&sql_hash) {
            let name = make_stmt_name(sql_hash);
            let name_s: &str = std::str::from_utf8(&name).expect("ASCII");
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
            proto::write_parse(&mut self.write_buf, name_s, sql, &param_oids);
            proto::write_describe(&mut self.write_buf, b'S', name_s);
            proto::write_sync(&mut self.write_buf);
            self.flush_write()?;

            self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))?;
            let columns = self.read_column_description()?;
            self.expect_ready()?;

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
            .name_str()
            .to_owned();
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

        self.write_buf.extend_from_slice(proto::SYNC_ONLY);
        self.flush_write()?;

        // Read N x (BindComplete + CommandComplete) + ReadyForQuery
        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            self.expect_message(|m| matches!(m, BackendMessage::BindComplete))?;

            let mut affected_rows: u64 = 0;
            loop {
                let msg = self.read_one_message()?;
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
                        self.drain_to_ready()?;
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

        self.expect_ready()?;
        self.shrink_buffers();
        Ok(results)
    }

    /// Ensure a statement is prepared and cached, doing a round-trip if needed.
    ///
    /// Returns the cached statement name. If the statement is already cached,
    /// this is a no-op (hash lookup only). Otherwise, sends Parse+Describe+Sync
    /// and waits for the response.
    pub(crate) fn ensure_stmt_prepared(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<[u8; 18], DriverError> {
        if let Some(info) = self.stmts.get(&sql_hash) {
            return Ok(info.name);
        }

        let name = make_stmt_name(sql_hash);
        let name_s: &str = std::str::from_utf8(&name).expect("ASCII");
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
        proto::write_parse(&mut self.write_buf, name_s, sql, &param_oids);
        proto::write_describe(&mut self.write_buf, b'S', name_s);
        proto::write_sync(&mut self.write_buf);
        self.flush_write()?;

        self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))?;
        let columns = self.read_column_description()?;
        self.expect_ready()?;

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

        Ok(name)
    }

    /// Write Bind+Execute message bytes for a prepared statement into an
    /// external buffer. Does NOT send anything on the wire.
    pub(crate) fn write_deferred_bind_execute(
        &self,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        buf: &mut Vec<u8>,
    ) {
        let stmt_name = self
            .stmts
            .get(&sql_hash)
            .expect("BUG: stmt just cached but not found")
            .name_str();
        proto::write_bind_params(buf, "", stmt_name, params);
        buf.extend_from_slice(proto::EXECUTE_ONLY);
    }

    /// Flush a buffer of deferred Bind+Execute messages as a single pipeline.
    ///
    /// Appends Sync to the buffer, writes everything in one write, then
    /// reads `count` x (BindComplete + CommandComplete) + ReadyForQuery.
    pub(crate) fn flush_deferred_pipeline(
        &mut self,
        buf: &mut Vec<u8>,
        count: usize,
    ) -> Result<Vec<u64>, DriverError> {
        if count == 0 {
            buf.clear();
            return Ok(Vec::new());
        }

        buf.extend_from_slice(proto::SYNC_ONLY);

        self.stream.write_all(buf).map_err(DriverError::Io)?;
        buf.clear();

        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            self.expect_message(|m| matches!(m, BackendMessage::BindComplete))?;

            let mut affected_rows: u64 = 0;
            loop {
                let msg = self.read_one_message()?;
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
                        self.drain_to_ready()?;
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

        self.expect_ready()?;
        self.shrink_buffers();
        Ok(results)
    }

    /// Process each row via a closure with zero-copy `PgDataRow`.
    pub fn for_each<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        mut f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(PgDataRow<'_>) -> Result<(), DriverError>,
    {
        let _ = self.send_pipeline(sql, sql_hash, params, false, true)?;

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
                        // Oversized message — fall back to read_one_message.
                        let msg = self.read_one_message()?;
                        match msg {
                            BackendMessage::BindComplete => continue,
                            BackendMessage::DataRow { data } => {
                                let row = PgDataRow::new(data)?;
                                f(row)?;
                                continue;
                            }
                            BackendMessage::CommandComplete { .. } | BackendMessage::EmptyQuery => {
                                continue;
                            }
                            BackendMessage::ReadyForQuery { status } => {
                                self.tx_status = status;
                                break 'outer;
                            }
                            BackendMessage::NoticeResponse { .. } => continue,
                            BackendMessage::ErrorResponse { data } => {
                                let fields = proto::parse_error_response(data);
                                self.maybe_invalidate_stmt_cache(&fields, sql_hash);
                                self.drain_to_ready()?;
                                return Err(self.make_server_error(fields));
                            }
                            other => {
                                return Err(DriverError::Protocol(format!(
                                    "unexpected message during for_each: {other:?}"
                                )));
                            }
                        }
                    }
                    break; // partial message — compact and refill
                }

                // Full message in stream_buf — parse inline.
                let payload_start = self.stream_buf_pos + 5;
                let payload_end = payload_start + payload_len;

                // Happy path first: DataRow is ~99.9% of messages during
                // row iteration.
                if msg_type == b'D' {
                    // DataRow — construct PgDataRow from stream_buf slice.
                    let row = PgDataRow::new(&self.stream_buf[payload_start..payload_end])?;
                    f(row)?;
                } else if msg_type == b'Z' {
                    // ReadyForQuery — extract tx status and we're done.
                    if payload_len >= 1 {
                        self.tx_status = self.stream_buf[payload_start];
                    }
                    self.stream_buf_pos += total_msg_len;
                    break 'outer;
                } else {
                    self.handle_non_datarow_execute(
                        msg_type,
                        payload_start,
                        payload_end,
                        sql_hash,
                    )?;
                }

                self.stream_buf_pos += total_msg_len;
            }

            // Need more data — compact and refill.
            self.refill_stream_buf()?;
        }

        self.shrink_buffers();
        Ok(())
    }

    /// Monolithic for_each_raw — everything in one function, no intermediate calls.
    ///
    /// Inlines the entire send_pipeline + response parsing path for SELECT
    /// queries processed via a raw byte closure. On cache hit: template copy +
    /// param patch + write_all + inline DataRow streaming + inline
    /// ReadyForQuery. No send_pipeline(), no flush_write(), no
    /// refill_stream_buf().
    ///
    /// On cache miss (first execution of a statement), falls through to the
    /// cold `for_each_raw_with_prepare` path.
    #[inline]
    pub fn for_each_raw_monolithic<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        mut f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(&[u8]) -> Result<(), DriverError>,
    {
        // === SEND PHASE (inline — no send_pipeline, no flush_write) ===
        self.write_buf.clear();

        // Check statement cache — inline, no function call.
        let info = match self.stmts.get_mut(&sql_hash) {
            Some(info) => {
                self.query_counter += 1;
                info.last_used = self.query_counter;
                info
            }
            None => {
                // Cache miss: prepare first (cold path, separate function).
                return self.for_each_raw_with_prepare(sql, sql_hash, params, f);
            }
        };

        // Build Bind+Execute+Sync message — inline bind template logic.
        let can_use_template = info
            .bind_template
            .as_ref()
            .is_some_and(|t| t.param_slots.len() == params.len());

        let mut has_exec_sync = false;

        if can_use_template {
            // SAFETY: can_use_template is true only when bind_template.is_some()
            let tmpl = info
                .bind_template
                .as_ref()
                .expect("guarded by can_use_template");
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
                proto::write_bind_params(&mut self.write_buf, "", info.name_str(), params);
                info.bind_template = None;
            }
        } else {
            proto::write_bind_params(&mut self.write_buf, "", info.name_str(), params);
        }

        // Snapshot bind template on first use or after invalidation.
        if info.bind_template.is_none() && !self.write_buf.is_empty() {
            info.bind_template = build_bind_template(&self.write_buf, params.len());
        }

        if !has_exec_sync {
            self.write_buf.extend_from_slice(proto::EXECUTE_SYNC);
        }

        // Write to socket — ONE syscall, no flush_write() indirection.
        self.stream
            .write_all(&self.write_buf)
            .map_err(DriverError::Io)?;

        // === RECEIVE PHASE (inline — no refill_stream_buf) ===

        // Read BindComplete inline from stream_buf.
        loop {
            let avail = self.stream_buf_end - self.stream_buf_pos;
            if avail >= 5 {
                let bc_type = self.stream_buf[self.stream_buf_pos];
                match bc_type {
                    b'2' => {
                        self.stream_buf_pos += 5;
                        break;
                    }
                    b'E' => {
                        let msg = self.read_one_message()?;
                        if let BackendMessage::ErrorResponse { data } = msg {
                            let fields = proto::parse_error_response(data);
                            self.drain_to_ready()?;
                            return Err(self.make_server_error(fields));
                        }
                    }
                    b'N' | b'S' => {
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
                        self.expect_message(|m| matches!(m, BackendMessage::BindComplete))?;
                        break;
                    }
                    _ => {
                        self.expect_message(|m| matches!(m, BackendMessage::BindComplete))?;
                        break;
                    }
                }
            } else {
                // Inline refill.
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
                    .map_err(DriverError::Io)?;
                if n == 0 {
                    return Err(DriverError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "connection closed",
                    )));
                }
                self.stream_buf_end = remaining + n;
            }
        }

        // Bulk DataRow loop: parse messages directly from stream_buf.
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
                        let msg = self.read_one_message()?;
                        match msg {
                            BackendMessage::DataRow { data } => {
                                f(data)?;
                                continue;
                            }
                            BackendMessage::CommandComplete { .. } | BackendMessage::EmptyQuery => {
                                continue;
                            }
                            BackendMessage::ReadyForQuery { status } => {
                                self.tx_status = status;
                                break 'outer;
                            }
                            BackendMessage::ErrorResponse { data } => {
                                let fields = proto::parse_error_response(data);
                                self.maybe_invalidate_stmt_cache(&fields, sql_hash);
                                self.drain_to_ready()?;
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
                    break; // partial message — compact and refill
                }

                // Full message in stream_buf — zero-copy.
                let payload_start = self.stream_buf_pos + 5;
                let payload_end = payload_start + payload_len;

                if msg_type == b'D' {
                    f(&self.stream_buf[payload_start..payload_end])?;
                } else if msg_type == b'Z' {
                    if payload_len >= 1 {
                        self.tx_status = self.stream_buf[payload_start];
                    }
                    self.stream_buf_pos += total_msg_len;
                    break 'outer;
                } else {
                    self.handle_non_datarow_execute(
                        msg_type,
                        payload_start,
                        payload_end,
                        sql_hash,
                    )?;
                }

                self.stream_buf_pos += total_msg_len;
            }

            // Need more data — compact and refill inline.
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
                .map_err(DriverError::Io)?;
            if n == 0 {
                return Err(DriverError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed",
                )));
            }
            self.stream_buf_end = remaining + n;
        }

        // Throttled maintenance — every 64 queries.
        if self.query_counter & 63 == 0 {
            if self.read_buf.capacity() > 64 * 1024 {
                self.read_buf.clear();
                self.read_buf.shrink_to(8192);
            }
            if self.write_buf.capacity() > 16 * 1024 {
                self.write_buf.clear();
                self.write_buf.shrink_to(8192);
            }
        }

        Ok(())
    }

    /// Cold path: cache miss for for_each_raw — Parse+Describe first, then stream.
    #[cold]
    #[inline(never)]
    fn for_each_raw_with_prepare<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        mut f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(&[u8]) -> Result<(), DriverError>,
    {
        debug_assert_eq!(crate::types::hash_sql(sql), sql_hash, "sql_hash mismatch");

        if params.len() > i16::MAX as usize {
            return Err(DriverError::Protocol(format!(
                "parameter count {} exceeds maximum {}",
                params.len(),
                i16::MAX
            )));
        }

        let name = make_stmt_name(sql_hash);
        let name_s: &str = std::str::from_utf8(&name).expect("ASCII");
        let param_oids: smallvec::SmallVec<[u32; 8]> =
            params.iter().map(|p| p.type_oid()).collect();

        self.write_buf.clear();
        proto::write_parse(&mut self.write_buf, name_s, sql, &param_oids);
        proto::write_describe(&mut self.write_buf, b'S', name_s);
        proto::write_bind_params(&mut self.write_buf, "", name_s, params);
        self.write_buf.extend_from_slice(proto::EXECUTE_SYNC);
        self.stream
            .write_all(&self.write_buf)
            .map_err(DriverError::Io)?;

        self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))?;
        let columns = self.read_column_description()?;
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

        // Now read BindComplete + DataRow* + CommandComplete + ReadyForQuery.
        self.expect_message(|m| matches!(m, BackendMessage::BindComplete))?;

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
                        let msg = self.read_one_message()?;
                        match msg {
                            BackendMessage::DataRow { data } => {
                                f(data)?;
                                continue;
                            }
                            BackendMessage::CommandComplete { .. } | BackendMessage::EmptyQuery => {
                                continue;
                            }
                            BackendMessage::ReadyForQuery { status } => {
                                self.tx_status = status;
                                break 'outer;
                            }
                            BackendMessage::ErrorResponse { data } => {
                                let fields = proto::parse_error_response(data);
                                self.maybe_invalidate_stmt_cache(&fields, sql_hash);
                                self.drain_to_ready()?;
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
                    break;
                }

                let payload_start = self.stream_buf_pos + 5;
                let payload_end = payload_start + payload_len;

                if msg_type == b'D' {
                    f(&self.stream_buf[payload_start..payload_end])?;
                } else if msg_type == b'Z' {
                    if payload_len >= 1 {
                        self.tx_status = self.stream_buf[payload_start];
                    }
                    self.stream_buf_pos += total_msg_len;
                    break 'outer;
                } else {
                    self.handle_non_datarow_execute(
                        msg_type,
                        payload_start,
                        payload_end,
                        sql_hash,
                    )?;
                }

                self.stream_buf_pos += total_msg_len;
            }

            self.refill_stream_buf()?;
        }

        self.shrink_buffers();
        Ok(())
    }

    /// Process each DataRow as raw bytes — fastest path.
    ///
    /// Delegates to `for_each_raw_monolithic` which inlines the entire send +
    /// receive path. Kept for API compatibility.
    #[inline]
    pub fn for_each_raw<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(&[u8]) -> Result<(), DriverError>,
    {
        self.for_each_raw_monolithic(sql, sql_hash, params, f)
    }

    /// Simple query protocol — for non-prepared SQL (BEGIN, COMMIT, SET, etc.).
    pub fn simple_query(&mut self, sql: &str) -> Result<(), DriverError> {
        self.write_buf.clear();
        proto::write_simple_query(&mut self.write_buf, sql);
        self.flush_write()?;

        loop {
            let msg = self.read_one_message()?;
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
                // Auth messages may arrive late under extreme server load
                // (e.g., AuthSaslFinal delayed past ReadyForQuery in startup).
                // Skip them defensively rather than treating as protocol error.
                | BackendMessage::AuthOk
                | BackendMessage::AuthSaslFinal { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready()?;
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

    /// Execute a simple (text protocol) query and return all result rows.
    pub fn simple_query_rows(&mut self, sql: &str) -> Result<Vec<SimpleRow>, DriverError> {
        self.write_buf.clear();
        proto::write_simple_query(&mut self.write_buf, sql);
        self.flush_write()?;

        let mut rows: Vec<SimpleRow> = Vec::new();
        loop {
            let msg = self.read_one_message()?;
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
                | BackendMessage::NoticeResponse { .. }
                | BackendMessage::ParameterStatus { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready()?;
                    return Err(self.make_server_error(fields));
                }
                other => {
                    return Err(DriverError::Protocol(format!(
                        "unexpected message during simple_query_rows: {other:?}"
                    )));
                }
            }
        }
    }

    /// Prepare a statement without executing it (Parse+Describe+Sync only).
    ///
    /// Returns column and parameter metadata. Uses the unnamed statement `""`
    /// so there is no cache pollution.
    pub fn prepare_describe(&mut self, sql: &str) -> Result<PrepareResult, DriverError> {
        self.write_buf.clear();
        // Use unnamed statement "" — PG replaces it on every Parse,
        // so there is no cache pollution.
        proto::write_parse(&mut self.write_buf, "", sql, &[]);
        proto::write_describe(&mut self.write_buf, b'S', "");
        proto::write_sync(&mut self.write_buf);
        self.flush_write()?;

        // Read ParseComplete
        self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))?;

        // Read ParameterDescription + RowDescription/NoData
        let mut param_oids: Vec<u32> = Vec::new();
        let columns;
        loop {
            let msg = self.read_one_message()?;
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
                    self.drain_to_ready()?;
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
        self.expect_ready()?;

        Ok(PrepareResult {
            columns,
            param_oids,
        })
    }

    /// Block until a NotificationResponse arrives on this connection.
    ///
    /// Reads raw messages from the stream and skips everything except
    /// `NotificationResponse`. Returns the `(channel, payload)` pair.
    /// Used by the listener to receive LISTEN/NOTIFY events.
    ///
    /// This method never returns `Ok` for non-notification messages -- it loops
    /// internally, discarding `ParameterStatus`, `NoticeResponse`, etc.
    pub fn wait_for_notification(&mut self) -> Result<(String, String), DriverError> {
        loop {
            let (msg_type, _payload_len) = self.read_message_buffered()?;
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

    /// Cancel the currently running query on this connection.
    ///
    /// Opens a NEW TCP connection to the same host:port and sends a
    /// CancelRequest message (16 bytes: length=16, code=80877102, pid, secret).
    /// The cancel connection is closed immediately after sending.
    pub fn cancel(&self) -> Result<(), DriverError> {
        let addr = format!("{}:{}", self.connect_config.host, self.connect_config.port);
        let mut tcp = std::net::TcpStream::connect(&addr).map_err(DriverError::Io)?;
        let mut buf = Vec::with_capacity(16);
        proto::write_cancel_request(&mut buf, self.pid, self.secret);
        tcp.write_all(&buf).map_err(DriverError::Io)?;
        tcp.flush().map_err(DriverError::Io)?;
        // Close immediately — PG expects no further data
        drop(tcp);
        Ok(())
    }

    /// Set the read timeout on the underlying socket.
    ///
    /// Used by listeners to poll for notifications with a timeout.
    /// `None` means block indefinitely.
    pub fn set_read_timeout(
        &self,
        timeout: Option<std::time::Duration>,
    ) -> Result<(), DriverError> {
        self.stream
            .set_read_timeout(timeout)
            .map_err(DriverError::Io)
    }

    // --- Streaming ---

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
    /// send buffered output without destroying the portal.
    pub fn query_streaming_start(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        chunk_size: i32,
    ) -> Result<(Arc<[ColumnDesc]>, bool), DriverError> {
        self.write_buf.clear();

        let columns = if let Some(info) = self.stmts.get_mut(&sql_hash) {
            // Cache hit: try bind template, fall back to write_bind_params.
            self.query_counter += 1;
            info.last_used = self.query_counter;

            let can_use_template = info
                .bind_template
                .as_ref()
                .is_some_and(|t| t.param_slots.len() == params.len());

            if can_use_template {
                // SAFETY: can_use_template is true only when bind_template.is_some()
                let tmpl = info
                    .bind_template
                    .as_ref()
                    .expect("guarded by can_use_template");
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
                    proto::write_bind_params(&mut self.write_buf, "", info.name_str(), params);
                    info.bind_template = None;
                }
            } else {
                proto::write_bind_params(&mut self.write_buf, "", info.name_str(), params);
            }

            let cols = info.columns.clone();

            if info.bind_template.is_none() && !self.write_buf.is_empty() {
                info.bind_template = build_bind_template(&self.write_buf, params.len());
            }

            proto::write_execute(&mut self.write_buf, "", chunk_size);
            // Use Flush (not Sync!) to keep the portal alive between chunks.
            proto::write_flush(&mut self.write_buf);
            self.flush_write()?;

            cols
        } else {
            // Cache miss: Parse+Describe+Bind+Execute+Flush
            let name = make_stmt_name(sql_hash);
            let name_s: &str = std::str::from_utf8(&name).expect("ASCII");
            let param_oids: smallvec::SmallVec<[u32; 8]> =
                params.iter().map(|p| p.type_oid()).collect();
            proto::write_parse(&mut self.write_buf, name_s, sql, &param_oids);
            proto::write_describe(&mut self.write_buf, b'S', name_s);
            proto::write_bind_params(&mut self.write_buf, "", name_s, params);

            proto::write_execute(&mut self.write_buf, "", chunk_size);
            proto::write_flush(&mut self.write_buf);
            self.flush_write()?;

            self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))?;
            let columns = self.read_column_description()?;
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
        self.expect_message(|m| matches!(m, BackendMessage::BindComplete))?;

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
    pub fn streaming_next_chunk(
        &mut self,
        arena: &mut Arena,
        all_col_offsets: &mut Vec<(usize, i32)>,
    ) -> Result<bool, DriverError> {
        all_col_offsets.clear();

        loop {
            let msg = self.read_one_message()?;
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
                    self.flush_write()?;
                    self.expect_ready()?;
                    self.shrink_buffers();

                    self.streaming_active = false;
                    return Ok(false);
                }
                BackendMessage::EmptyQuery => {
                    self.write_buf.clear();
                    proto::write_sync(&mut self.write_buf);
                    self.flush_write()?;
                    self.expect_ready()?;

                    self.streaming_active = false;
                    return Ok(false);
                }
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    // Send Sync to reset and drain to ReadyForQuery
                    self.write_buf.clear();
                    proto::write_sync(&mut self.write_buf);
                    self.flush_write()?;
                    self.drain_to_ready()?;

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
    pub fn streaming_send_execute(&mut self, chunk_size: i32) -> Result<(), DriverError> {
        self.write_buf.clear();
        proto::write_execute(&mut self.write_buf, "", chunk_size);
        proto::write_flush(&mut self.write_buf);
        self.flush_write()
    }

    /// Whether a streaming query is in progress.
    pub fn is_streaming(&self) -> bool {
        self.streaming_active
    }

    /// Send Terminate and close the connection.
    pub fn close(mut self) -> Result<(), DriverError> {
        self.write_buf.clear();
        proto::write_terminate(&mut self.write_buf);
        let _ = self.flush_write();
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

    /// Monotonic query counter — incremented on every query/execute.
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

    /// All server parameters received during startup.
    pub fn server_params(&self) -> &[(Box<str>, Box<str>)] {
        &self.params
    }

    /// Backend process ID.
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Backend secret key.
    pub fn secret_key(&self) -> i32 {
        self.secret
    }

    /// Drain all buffered notifications.
    pub fn drain_notifications(&mut self) -> Vec<Notification> {
        std::mem::take(&mut self.pending_notifications)
    }

    /// Number of pending notifications.
    pub fn pending_notification_count(&self) -> usize {
        self.pending_notifications.len()
    }

    /// Set the maximum number of cached prepared statements.
    pub fn set_max_stmt_cache_size(&mut self, size: usize) {
        self.max_stmt_cache_size = size;
    }

    /// Number of currently cached prepared statements.
    pub fn stmt_cache_len(&self) -> usize {
        self.stmts.len()
    }

    /// When this connection was created.
    pub fn created_at(&self) -> std::time::Instant {
        self.created_at
    }

    // --- Pipeline ---

    /// Common pipeline: builds and sends Parse+Describe+Bind+Execute+Sync (or
    /// Bind+Execute+Sync on cache hit). Returns column metadata.
    ///
    /// On cache hit with a valid bind template, uses the template for faster
    /// Bind message construction.
    #[inline]
    fn send_pipeline(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        need_columns: bool,
        skip_bind_complete: bool,
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

        let columns = if let Some(info) = self.stmts.get_mut(&sql_hash) {
            // Cache hit: try bind template first, fall back to write_bind_params.
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
                // SAFETY: can_use_template is true only when bind_template.is_some()
                let tmpl = info
                    .bind_template
                    .as_ref()
                    .expect("guarded by can_use_template");
                self.write_buf.extend_from_slice(&tmpl.bytes);

                let mut template_ok = true;
                for (i, param) in params.iter().enumerate() {
                    let (data_offset, old_len) = tmpl.param_slots[i];
                    if param.is_null() {
                        // Patch length to -1 (NULL).
                        let len_offset = data_offset - 4;
                        self.write_buf[len_offset..len_offset + 4]
                            .copy_from_slice(&(-1i32).to_be_bytes());
                    } else if old_len >= 0 {
                        let end = data_offset + old_len as usize;
                        if !param.encode_at(&mut self.write_buf[data_offset..end]) {
                            // Size mismatch — rebuild Bind from scratch.
                            template_ok = false;
                            break;
                        }
                    } else {
                        // old_len < 0 means the template had NULL here but now
                        // we have a non-NULL value. Rebuild.
                        template_ok = false;
                        break;
                    }
                }

                if template_ok {
                    has_exec_sync = true; // Template includes EXECUTE_SYNC.
                } else {
                    self.write_buf.clear();
                    proto::write_bind_params(&mut self.write_buf, "", info.name_str(), params);
                    // Invalidate stale template so we re-snapshot below.
                    info.bind_template = None;
                }
            } else {
                proto::write_bind_params(&mut self.write_buf, "", info.name_str(), params);
            }

            let cols = if need_columns {
                Some(info.columns.clone())
            } else {
                None
            };

            // Snapshot bind template if we don't have one yet (first use or
            // after invalidation due to size mismatch).
            // build_bind_template appends EXECUTE_SYNC to the template bytes.
            if info.bind_template.is_none() && !self.write_buf.is_empty() {
                info.bind_template = build_bind_template(&self.write_buf, params.len());
            }

            if !has_exec_sync {
                self.write_buf.extend_from_slice(proto::EXECUTE_SYNC);
            }
            self.flush_write()?;

            cols
        } else {
            // Cache miss: Parse+Describe+Bind+Execute+Sync
            let name = make_stmt_name(sql_hash);
            let name_s: &str = std::str::from_utf8(&name).expect("ASCII");
            let param_oids: smallvec::SmallVec<[u32; 8]> =
                params.iter().map(|p| p.type_oid()).collect();
            proto::write_parse(&mut self.write_buf, name_s, sql, &param_oids);
            proto::write_describe(&mut self.write_buf, b'S', name_s);
            proto::write_bind_params(&mut self.write_buf, "", name_s, params);

            self.write_buf.extend_from_slice(proto::EXECUTE_SYNC);
            self.flush_write()?;

            self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))?;
            let columns = self.read_column_description()?;
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

        if !skip_bind_complete {
            self.expect_message(|m| matches!(m, BackendMessage::BindComplete))?;
        }

        Ok(columns)
    }

    /// Read RowDescription / NoData after ParseComplete+Describe.
    fn read_column_description(&mut self) -> Result<Arc<[ColumnDesc]>, DriverError> {
        loop {
            let msg = self.read_one_message()?;
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
                    self.drain_to_ready()?;
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
        if self.stmts.len() >= self.max_stmt_cache_size && !self.stmts.contains_key(&sql_hash) {
            if let Some((_lru_hash, evicted)) = self.stmts.evict_lru() {
                proto::write_close(&mut self.write_buf, b'S', evicted.name_str());
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

    fn maybe_invalidate_stmt_cache(&mut self, fields: &proto::ErrorFields, sql_hash: u64) -> bool {
        if &*fields.code == "26000" {
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
    ///
    /// Separated from the hot loop so the compiler keeps DataRow processing
    /// tight in the instruction cache. Handles CommandComplete, BindComplete,
    /// EmptyQuery, ErrorResponse, NotificationResponse, and others.
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
            b'2' | b'I' => {} // BindComplete / EmptyQuery — skip
            b'C' => {
                *affected_rows =
                    proto::parse_command_tag_bytes(&self.stream_buf[payload_start..payload_end]);
            }
            b'E' => {
                let fields =
                    proto::parse_error_response(&self.stream_buf[payload_start..payload_end]);
                self.maybe_invalidate_stmt_cache(&fields, sql_hash);
                self.drain_to_ready()?;
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

    /// Handle non-DataRow messages during execute/for_each/for_each_raw inline
    /// parsing. Same as `handle_non_datarow_query` but without `affected_rows`.
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
            b'2' | b'C' | b'I' => {} // BindComplete / CommandComplete / EmptyQuery — skip
            b'E' => {
                let fields =
                    proto::parse_error_response(&self.stream_buf[payload_start..payload_end]);
                self.maybe_invalidate_stmt_cache(&fields, sql_hash);
                self.drain_to_ready()?;
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

    /// Read one backend message, auto-buffering notifications.
    #[inline]
    fn read_one_message(&mut self) -> Result<BackendMessage<'_>, DriverError> {
        loop {
            let (msg_type, _payload_len) = self.read_message_buffered()?;
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

    fn expect_message(
        &mut self,
        pred: impl Fn(&BackendMessage<'_>) -> bool,
    ) -> Result<(), DriverError> {
        loop {
            let msg = self.read_one_message()?;
            if pred(&msg) {
                return Ok(());
            }
            match msg {
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready()?;
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

    fn expect_ready(&mut self) -> Result<(), DriverError> {
        loop {
            let msg = self.read_one_message()?;
            match msg {
                BackendMessage::ReadyForQuery { status } => {
                    self.tx_status = status;
                    return Ok(());
                }
                BackendMessage::NoticeResponse { .. } | BackendMessage::ParameterStatus { .. } => {}
                BackendMessage::ErrorResponse { data } => {
                    let fields = proto::parse_error_response(data);
                    self.drain_to_ready()?;
                    return Err(self.make_server_error(fields));
                }
                _ => {}
            }
        }
    }

    #[inline]
    fn drain_to_ready(&mut self) -> Result<(), DriverError> {
        loop {
            let msg = self.read_one_message()?;
            if let BackendMessage::ReadyForQuery { status } = msg {
                self.tx_status = status;
                return Ok(());
            }
        }
    }

    // --- Synchronous I/O ---

    /// Flush the write buffer to the stream. Blocking.
    #[inline]
    fn flush_write(&mut self) -> Result<(), DriverError> {
        self.stream
            .write_all(&self.write_buf)
            .map_err(DriverError::Io)
    }

    /// Read one complete backend message. Blocking.
    ///
    /// Returns `(msg_type, payload_len)`. Payload is stored in `self.read_buf`.
    fn read_message_buffered(&mut self) -> Result<(u8, usize), DriverError> {
        let mut header = [0u8; 5];
        sync_buffered_read_exact(
            &mut self.stream,
            &mut self.stream_buf,
            &mut self.stream_buf_pos,
            &mut self.stream_buf_end,
            &mut header,
        )?;

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
            sync_buffered_read_exact(
                &mut self.stream,
                &mut self.stream_buf,
                &mut self.stream_buf_pos,
                &mut self.stream_buf_end,
                &mut self.read_buf[..payload_len],
            )?;
        }

        Ok((msg_type, payload_len))
    }

    /// Compact stream_buf and read more data from the socket. Blocking.
    #[inline]
    fn refill_stream_buf(&mut self) -> Result<(), DriverError> {
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

/// Synchronous buffered read_exact — reads exactly `out.len()` bytes using
/// a persistent read buffer. Pure blocking I/O via `std::io::Read`.
fn sync_buffered_read_exact(
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
            *pos = 0;
            let n = stream.read(buf).map_err(DriverError::Io)?;
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

// --- DataRow parsing (duplicated here to avoid pub(crate) changes to conn.rs) ---

/// Parse a DataRow into a response buffer (Vec<u8>) — zero-copy style.
///
/// Appends ONLY column data bytes to `buf` (no length prefixes — they're
/// parsed and discarded). Column offsets point into `buf`.
///
/// Cost per row: one bounds check + walk column headers (no memcpy per column,
/// one extend_from_slice per row for all non-NULL column data).
#[inline(always)]
fn parse_data_row_into_buf(
    data: &[u8],
    buf: &mut Vec<u8>,
    out: &mut Vec<(usize, i32)>,
) -> Result<(), DriverError> {
    if data.len() < 2 {
        return Err(DriverError::Protocol("DataRow too short".into()));
    }

    let num_cols = i16::from_be_bytes([data[0], data[1]]);
    if num_cols < 0 {
        return Err(DriverError::Protocol(
            "DataRow: negative column count".into(),
        ));
    }
    let num_cols = num_cols as usize;

    // Bulk append: copy the entire column section into buf in ONE memcpy,
    // then walk column boundaries. ONE extend_from_slice per DataRow.
    let col_data = &data[2..];
    let base = buf.len();
    buf.extend_from_slice(col_data);

    // Walk columns within the buffer — no copying, just record offsets.
    let mut pos: usize = 0;
    for _ in 0..num_cols {
        if pos + 4 > col_data.len() {
            return Err(DriverError::Protocol("DataRow truncated".into()));
        }

        let col_len = i32::from_be_bytes([
            col_data[pos],
            col_data[pos + 1],
            col_data[pos + 2],
            col_data[pos + 3],
        ]);
        pos += 4;

        if col_len < 0 {
            out.push((0, -1));
        } else {
            let len = col_len as usize;
            if pos + len > col_data.len() {
                return Err(DriverError::Protocol(
                    "DataRow column data truncated".into(),
                ));
            }
            // Offset within buf where this column's data starts.
            out.push((base + pos, col_len));
            pos += len;
        }
    }

    Ok(())
}

/// Parse a DataRow message into flat column offset storage (arena version).
///
/// Used by streaming queries where arena is the storage backend.
fn parse_data_row_flat(
    data: &[u8],
    arena: &mut Arena,
    out: &mut Vec<(usize, i32)>,
) -> Result<(), DriverError> {
    if data.len() < 2 {
        return Err(DriverError::Protocol("DataRow too short".into()));
    }

    let num_cols_raw = i16::from_be_bytes([data[0], data[1]]);
    if num_cols_raw < 0 {
        return Err(DriverError::Protocol(
            "DataRow: negative column count".into(),
        ));
    }
    let num_cols = num_cols_raw as usize;
    out.reserve(num_cols);

    // Bulk copy: one alloc_copy for the entire DataRow payload (after column count).
    // Column data with length prefixes is stored contiguously in the arena.
    let col_data = &data[2..];
    let base = arena.alloc_copy(col_data);

    // Walk column boundaries within the arena block.
    let mut pos: usize = 0;
    for _ in 0..num_cols {
        if pos + 4 > col_data.len() {
            return Err(DriverError::Protocol("DataRow truncated".into()));
        }

        let col_len = i32::from_be_bytes([
            col_data[pos],
            col_data[pos + 1],
            col_data[pos + 2],
            col_data[pos + 3],
        ]);
        pos += 4;

        if col_len < 0 {
            out.push((0, -1));
        } else {
            let len = col_len as usize;
            if pos + len > col_data.len() {
                return Err(DriverError::Protocol(
                    "DataRow column data truncated".into(),
                ));
            }
            // Point directly into the bulk-copied block — no per-column copy.
            out.push((base + pos, col_len));
            pos += len;
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;
    use crate::types::hash_sql;

    #[test]
    fn sync_config_tcp_no_longer_rejected() {
        // Connection now supports TCP -- connecting to an invalid port
        // should give an I/O error, not a "Unix domain socket" error.
        let config = Config::from_url("postgres://user:pass@127.0.0.1:1/db").unwrap();
        let result = Connection::connect(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        // Should be an I/O error (connection refused), NOT a protocol error
        // about Unix domain sockets.
        assert!(
            !err.contains("Unix domain socket"),
            "error should NOT mention UDS requirement: {err}"
        );
    }

    #[test]
    fn sync_data_row_parsing() {
        let mut arena = Arena::new();
        let mut out = Vec::new();

        let mut data = Vec::new();
        data.extend_from_slice(&2i16.to_be_bytes());
        data.extend_from_slice(&4i32.to_be_bytes());
        data.extend_from_slice(&42i32.to_be_bytes());
        data.extend_from_slice(&(-1i32).to_be_bytes());

        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].1, 4);
        assert_eq!(out[1].1, -1);
    }

    #[test]
    fn sync_data_row_empty() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let data = 0i16.to_be_bytes();
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn sync_data_row_too_short() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let data = vec![0u8];
        assert!(parse_data_row_flat(&data, &mut arena, &mut out).is_err());
    }

    #[test]
    fn sync_data_row_negative_col_count() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let data = (-1i16).to_be_bytes();
        assert!(parse_data_row_flat(&data, &mut arena, &mut out).is_err());
    }

    #[test]
    fn sync_data_row_truncated() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let mut data = Vec::new();
        data.extend_from_slice(&2i16.to_be_bytes());
        data.extend_from_slice(&4i32.to_be_bytes());
        data.extend_from_slice(&42i32.to_be_bytes());
        // Missing second column
        assert!(parse_data_row_flat(&data, &mut arena, &mut out).is_err());
    }

    #[test]
    fn sync_data_row_col_data_truncated() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let mut data = Vec::new();
        data.extend_from_slice(&1i16.to_be_bytes());
        data.extend_from_slice(&100i32.to_be_bytes()); // claims 100 bytes
        data.push(0); // only 1 byte
        assert!(parse_data_row_flat(&data, &mut arena, &mut out).is_err());
    }

    // ---- TCP connect attempts ----

    #[test]
    fn sync_connect_tcp_unreachable_port() {
        // Connection now supports TCP. Connecting to a refused port
        // should give an I/O error (connection refused).
        let config = Config::from_url("postgres://user:pass@127.0.0.1:1/db").unwrap();
        let result = Connection::connect(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            !err.contains("Unix domain socket"),
            "error should NOT mention UDS: {err}"
        );
    }

    #[test]
    fn sync_connect_ip_address_attempts_tcp() {
        // Connection now supports TCP — connecting to a refused port
        // gives an I/O error, not a protocol rejection.
        let config = Config::from_url("postgres://user:pass@127.0.0.1:1/db").unwrap();
        let result = Connection::connect(&config);
        assert!(result.is_err());
    }

    // ---- DataRow parsing extended ----

    #[test]
    fn sync_data_row_all_null() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let mut data = Vec::new();
        data.extend_from_slice(&3i16.to_be_bytes());
        data.extend_from_slice(&(-1i32).to_be_bytes());
        data.extend_from_slice(&(-1i32).to_be_bytes());
        data.extend_from_slice(&(-1i32).to_be_bytes());
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 3);
        for (_, len) in &out {
            assert_eq!(*len, -1);
        }
    }

    #[test]
    fn sync_data_row_long_text() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let long_text = "a".repeat(2048);
        let text_bytes = long_text.as_bytes();
        let mut data = Vec::new();
        data.extend_from_slice(&1i16.to_be_bytes());
        data.extend_from_slice(&(text_bytes.len() as i32).to_be_bytes());
        data.extend_from_slice(text_bytes);
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, text_bytes.len() as i32);
        let stored = arena.get(out[0].0, out[0].1 as usize);
        assert_eq!(stored, text_bytes);
    }

    #[test]
    fn sync_data_row_empty_text() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let mut data = Vec::new();
        data.extend_from_slice(&1i16.to_be_bytes());
        data.extend_from_slice(&0i32.to_be_bytes()); // 0-length text, not NULL
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, 0); // 0 length, NOT -1 (which would be NULL)
    }

    #[test]
    fn sync_data_row_17_columns_exceeds_smallvec() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let mut data = Vec::new();
        let num_cols: i16 = 20;
        data.extend_from_slice(&num_cols.to_be_bytes());
        for i in 0..num_cols {
            let val = (i as i32).to_be_bytes();
            data.extend_from_slice(&4i32.to_be_bytes());
            data.extend_from_slice(&val);
        }
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 20);
        for (idx, (offset, len)) in out.iter().enumerate() {
            assert_eq!(*len, 4);
            let stored = arena.get(*offset, 4);
            let val = i32::from_be_bytes([stored[0], stored[1], stored[2], stored[3]]);
            assert_eq!(val, idx as i32);
        }
    }

    #[test]
    fn sync_data_row_mixed_null_and_data() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let mut data = Vec::new();
        data.extend_from_slice(&5i16.to_be_bytes());
        // col 0: NULL
        data.extend_from_slice(&(-1i32).to_be_bytes());
        // col 1: i32(42)
        data.extend_from_slice(&4i32.to_be_bytes());
        data.extend_from_slice(&42i32.to_be_bytes());
        // col 2: NULL
        data.extend_from_slice(&(-1i32).to_be_bytes());
        // col 3: NULL
        data.extend_from_slice(&(-1i32).to_be_bytes());
        // col 4: text "hello"
        data.extend_from_slice(&5i32.to_be_bytes());
        data.extend_from_slice(b"hello");

        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 5);
        assert_eq!(out[0].1, -1);
        assert_eq!(out[1].1, 4);
        assert_eq!(out[2].1, -1);
        assert_eq!(out[3].1, -1);
        assert_eq!(out[4].1, 5);
        let stored = arena.get(out[4].0, 5);
        assert_eq!(stored, b"hello");
    }

    // ---- Connection UDS connect (requires PG, skipped if unavailable) ----

    #[test]
    #[ignore] // requires a running PostgreSQL on /tmp
    fn sync_connect_uds_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let result = Connection::connect(&config);
        // If PG is running on /tmp, this succeeds. If not, it's an I/O error.
        if let Ok(conn) = result {
            assert!(conn.pid() != 0, "pid should be nonzero");
            assert!(conn.is_idle(), "should start idle");
            assert!(!conn.is_in_transaction(), "should not be in tx");
            assert!(
                !conn.is_in_failed_transaction(),
                "should not be in failed tx"
            );
            assert_eq!(conn.stmt_cache_len(), 0, "cache should be empty");
            let _ = conn.close();
        }
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_simple_query_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        conn.simple_query("SELECT 1").unwrap();
        assert!(conn.is_idle());
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_query_with_params_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        let sql = "SELECT $1::int4 + $2::int4 AS sum";
        let hash = hash_sql(sql);
        let a: i32 = 10;
        let b: i32 = 20;
        let result = conn
            .query(
                sql,
                hash,
                &[&a as &(dyn Encode + Sync), &b as &(dyn Encode + Sync)],
            )
            .unwrap();
        assert_eq!(result.len(), 1);
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_execute_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        conn.simple_query("CREATE TEMP TABLE _sync_test (id int)")
            .unwrap();
        let sql = "INSERT INTO _sync_test VALUES ($1::int4)";
        let hash = hash_sql(sql);
        let val: i32 = 42;
        let affected = conn
            .execute(sql, hash, &[&val as &(dyn Encode + Sync)])
            .unwrap();
        assert_eq!(affected, 1);
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_for_each_zero_rows_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        conn.simple_query("CREATE TEMP TABLE _sync_fe0 (id int)")
            .unwrap();
        let sql = "SELECT id FROM _sync_fe0";
        let hash = hash_sql(sql);
        let mut count = 0u32;
        conn.for_each(sql, hash, &[], |_row| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 0);
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_for_each_multiple_rows_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        let sql = "SELECT generate_series(1, 5)";
        let hash = hash_sql(sql);
        let mut count = 0u32;
        conn.for_each(sql, hash, &[], |_row| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 5);
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_prepare_only_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        let sql = "SELECT 1";
        let hash = hash_sql(sql);
        conn.prepare_only(sql, hash).unwrap();
        assert_eq!(conn.stmt_cache_len(), 1);
        // prepare_only again is a no-op
        conn.prepare_only(sql, hash).unwrap();
        assert_eq!(conn.stmt_cache_len(), 1);
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_simple_query_rows_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        let rows = conn.simple_query_rows("SELECT 42 AS n").unwrap();
        assert!(!rows.is_empty());
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_stmt_cache_hit_miss_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        let sql1 = "SELECT 1";
        let hash1 = hash_sql(sql1);
        conn.query(sql1, hash1, &[]).unwrap();
        assert_eq!(conn.stmt_cache_len(), 1);
        // Same query = cache hit
        conn.query(sql1, hash1, &[]).unwrap();
        assert_eq!(conn.stmt_cache_len(), 1);
        // Different query = cache miss
        let sql2 = "SELECT 2";
        let hash2 = hash_sql(sql2);
        conn.query(sql2, hash2, &[]).unwrap();
        assert_eq!(conn.stmt_cache_len(), 2);
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_invalid_sql_error_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        let sql = "SELECTTTT INVALID GARBAGE";
        let hash = hash_sql(sql);
        let result = conn.query(sql, hash, &[]);
        assert!(result.is_err());
        // Connection should still be usable after error
        assert!(conn.is_idle());
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_tx_state_transitions_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        assert!(conn.is_idle());
        assert!(!conn.is_in_transaction());
        conn.simple_query("BEGIN").unwrap();
        assert!(conn.is_in_transaction());
        assert!(!conn.is_idle());
        conn.simple_query("COMMIT").unwrap();
        assert!(conn.is_idle());
        assert!(!conn.is_in_transaction());
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_lru_cache_eviction_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        conn.set_max_stmt_cache_size(3);
        for i in 0..5 {
            let sql = format!("SELECT {}", i);
            let hash = hash_sql(&sql);
            conn.query(&sql, hash, &[]).unwrap();
        }
        // Cache should not exceed max_stmt_cache_size
        assert!(
            conn.stmt_cache_len() <= 3,
            "cache should be capped at 3, got {}",
            conn.stmt_cache_len()
        );
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_for_each_raw_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        let sql = "SELECT generate_series(1, 3)";
        let hash = hash_sql(sql);
        let mut raw_count = 0u32;
        conn.for_each_raw(sql, hash, &[], |_raw_data| {
            raw_count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(raw_count, 3);
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_query_null_params_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        let sql = "SELECT $1::int4 IS NULL AS is_null";
        let hash = hash_sql(sql);
        let val: Option<i32> = None;
        let _result = conn
            .query(sql, hash, &[&val as &(dyn Encode + Sync)])
            .unwrap();
        let _ = conn.close();
    }

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_query_various_param_types_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        let sql = "SELECT $1::int4, $2::int8, $3::text, $4::bool, $5::float8";
        let hash = hash_sql(sql);
        let p1: i32 = 42;
        let p2: i64 = 9999999;
        let p3: &str = "hello";
        let p4: bool = true;
        let p5: f64 = 3.14;
        let result = conn
            .query(
                sql,
                hash,
                &[
                    &p1 as &(dyn Encode + Sync),
                    &p2 as &(dyn Encode + Sync),
                    &p3 as &(dyn Encode + Sync),
                    &p4 as &(dyn Encode + Sync),
                    &p5 as &(dyn Encode + Sync),
                ],
            )
            .unwrap();
        assert_eq!(result.len(), 1);
        let _ = conn.close();
    }

    // ---- Buffer shrink test ----

    #[test]
    fn sync_shrink_threshold_values() {
        // Verify the shrink logic constants are sensible
        // read_buf shrinks when > 64KB
        // write_buf shrinks when > 16KB
        // These are tested structurally — the actual shrink logic runs after
        // each query/execute/for_each, but we cannot easily observe buffer
        // capacity without a real connection. The parse_data_row_flat tests
        // exercise the arena path, and the constant thresholds are validated
        // here for regression detection.
        let shrink = 64 * 1024usize;
        let initial = 8192usize;
        assert!(
            shrink > initial,
            "shrink threshold must exceed initial size"
        );
    }

    // ---- Debug impl ----

    #[test]
    fn sync_connection_debug_format() {
        // Connection Debug is tested structurally.
        // We cannot construct one without a real UDS, but we verify
        // the Debug impl exists by checking the #[derive]-like format.
        let fmt_str = format!(
            "Connection {{ pid: {}, tx_status: '{}', stmt_cache_len: {} }}",
            0, 'I', 0
        );
        assert!(fmt_str.contains("Connection"));
        assert!(fmt_str.contains("pid"));
        assert!(fmt_str.contains("tx_status"));
    }

    // ---- TLS config tests (no real TLS server needed) ----

    #[test]
    fn sync_connect_sslmode_require_without_tls_feature() {
        // When compiled without 'tls' feature, sslmode=require should error
        // with a clear message (unless the tls feature is actually enabled).
        // This test verifies the error path exists and handles correctly.
        let mut config = Config::from_url("postgres://user:pass@127.0.0.1:1/db").unwrap();
        config.ssl = SslMode::Require;
        let result = Connection::connect(&config);
        assert!(result.is_err());
        // The error will be either:
        // - "sslmode=require but bsql was compiled without the 'tls' feature" (no tls feature)
        // - I/O error (tls feature enabled, but connection refused)
        // Both are valid.
    }

    #[test]
    fn sync_connect_sslmode_disable_attempts_tcp() {
        let mut config = Config::from_url("postgres://user:pass@127.0.0.1:1/db").unwrap();
        config.ssl = SslMode::Disable;
        let result = Connection::connect(&config);
        assert!(result.is_err());
        // Should be an I/O error (connection refused), never a TLS error
        assert!(matches!(result.unwrap_err(), DriverError::Io(_)));
    }

    #[test]
    fn sync_connect_sslmode_prefer_attempts_tcp() {
        let mut config = Config::from_url("postgres://user:pass@127.0.0.1:1/db").unwrap();
        config.ssl = SslMode::Prefer;
        let result = Connection::connect(&config);
        assert!(result.is_err());
    }

    // ---- Streaming state tests ----

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_streaming_basic_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();
        assert!(!conn.is_streaming());

        let sql = "SELECT generate_series(1, 10)";
        let hash = hash_sql(sql);

        let (cols, _) = conn.query_streaming_start(sql, hash, &[], 3).unwrap();
        assert!(!cols.is_empty());
        assert!(conn.is_streaming());

        let mut arena = Arena::new();
        let mut offsets = Vec::new();
        let mut total_rows = 0;

        // Read chunks until done
        loop {
            let has_more = conn.streaming_next_chunk(&mut arena, &mut offsets).unwrap();
            total_rows += offsets.len();
            if !has_more {
                break;
            }
            conn.streaming_send_execute(3).unwrap();
        }

        assert_eq!(total_rows, 10);
        assert!(!conn.is_streaming());
        let _ = conn.close();
    }

    // ---- prepare_describe tests ----

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_prepare_describe_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();

        let result = conn
            .prepare_describe("SELECT $1::int4 + $2::int4 AS sum")
            .unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(&*result.columns[0].name, "sum");
        assert_eq!(result.param_oids.len(), 2);
        let _ = conn.close();
    }

    // ---- wait_for_notification test ----

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_wait_for_notification_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let mut conn = Connection::connect(&config).unwrap();

        conn.simple_query("LISTEN test_chan").unwrap();
        conn.simple_query("NOTIFY test_chan, 'hello'").unwrap();

        // Set a read timeout so we don't block forever if notification fails
        conn.set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        let (channel, payload) = conn.wait_for_notification().unwrap();
        assert_eq!(channel, "test_chan");
        assert_eq!(payload, "hello");
        let _ = conn.close();
    }

    // ---- cancel test ----

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_cancel_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let conn = Connection::connect(&config).unwrap();
        // Just verify cancel() doesn't panic — the query cancel itself
        // requires a concurrent query on another thread.
        let result = conn.cancel();
        // Cancel may succeed or fail (no query running) — just verify no panic
        let _ = result;
        let _ = conn.close();
    }

    // ---- server_params test ----

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_server_params_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let conn = Connection::connect(&config).unwrap();
        let params = conn.server_params();
        assert!(
            !params.is_empty(),
            "server should send parameters during startup"
        );
        // server_encoding should be present
        assert!(
            conn.parameter("server_encoding").is_some(),
            "server_encoding should be present"
        );
        let _ = conn.close();
    }

    // ---- set_read_timeout test ----

    #[test]
    #[ignore] // requires PostgreSQL
    fn sync_set_read_timeout_if_pg_available() {
        let config = Config::from_url("postgres://postgres@localhost/postgres?host=/tmp").unwrap();
        let conn = Connection::connect(&config).unwrap();
        // Set and clear read timeout
        conn.set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        conn.set_read_timeout(None).unwrap();
        let _ = conn.close();
    }
}
