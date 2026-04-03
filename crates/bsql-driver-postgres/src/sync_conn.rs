//! Synchronous PostgreSQL connection over Unix domain sockets.
//!
//! `SyncConnection` provides the same query interface as `Connection` but uses
//! blocking `std::os::unix::net::UnixStream` instead of tokio async I/O. This
//! eliminates all async runtime overhead (future state machines, waker polling,
//! task scheduling) for UDS connections where I/O completes in microseconds.
//!
//! # When to use
//!
//! Use `SyncConnection` when:
//! - Connecting via Unix domain socket (localhost only)
//! - Maximum single-query latency matters (benchmarks, hot-path lookups)
//! - You are already on a blocking thread (e.g., `tokio::task::spawn_blocking`)
//!
//! For TCP connections, use the async `Connection` which integrates with tokio's
//! event loop and does not block the runtime.
//!
//! # Performance
//!
//! UDS write/read is kernel IPC (sub-microsecond). The async `Connection` adds
//! ~200ns per `.await` point due to the future state machine poll cycle. With
//! 2-3 await points per query, that is 400-600ns of pure async overhead.
//! `SyncConnection` eliminates this entirely.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::Arc;

use crate::DriverError;
use crate::arena::Arena;
use crate::auth;
use crate::codec::Encode;
use crate::conn::{ColumnDesc, Config, Notification, PgDataRow, QueryResult, SimpleRow};
use crate::proto::{self, BackendMessage};

// --- Identity hasher (shared concept with conn.rs) ---

/// Identity hasher for pre-hashed u64 keys. Same as `conn.rs::IdentityHasher`.
#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
    #[inline]
    fn write(&mut self, _: &[u8]) {
        debug_assert!(false, "IdentityHasher only supports u64 keys");
        self.0 = 0;
    }
}

type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;
type StmtCache = HashMap<u64, StmtInfo, IdentityBuildHasher>;

/// Cached information about a prepared statement.
struct StmtInfo {
    /// Statement name: `"s_{hash:016x}"`
    name: Box<str>,
    /// Column metadata from RowDescription.
    columns: Arc<[ColumnDesc]>,
    /// Monotonic counter value at last use for LRU eviction.
    last_used: u64,
    /// Pre-built Bind message template for the cached statement.
    ///
    /// On the first execution, we snapshot the Bind message bytes from
    /// `write_buf`. On subsequent executions with the same parameter count,
    /// we memcpy this template and only patch the parameter data bytes,
    /// avoiding the full `write_bind_params` rebuild (~100-200ns savings).
    ///
    /// `None` until the first execution populates it.
    bind_template: Option<BindTemplate>,
}

/// Pre-built Bind message template for fast re-execution.
///
/// Stores the complete Bind message bytes and the byte offsets where
/// each parameter's data begins (after the 4-byte length prefix).
/// On re-execution, we copy the template and overwrite param data in-place.
struct BindTemplate {
    /// Complete Bind message bytes (type 'B' + length + payload).
    bytes: Vec<u8>,
    /// For each parameter: `(data_offset, data_len)` within `bytes`.
    /// `data_offset` points to the first byte of param data (after the i32 length).
    /// `data_len` is the length of the param data. -1 means NULL.
    param_slots: Vec<(usize, i32)>,
}

/// Format a statement name from a hash: `"s_{hash:016x}"`.
#[inline]
fn make_stmt_name(hash: u64) -> Box<str> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut buf = [0u8; 18];
    buf[0] = b's';
    buf[1] = b'_';
    let bytes = hash.to_be_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        buf[2 + i * 2] = HEX[(b >> 4) as usize];
        buf[2 + i * 2 + 1] = HEX[(b & 0x0f) as usize];
    }
    let s = std::str::from_utf8(&buf).expect("BUG: stmt name buffer contains only ASCII hex");
    s.into()
}

/// Owned action from a startup message.
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

// --- SyncConnection ---

/// A synchronous PostgreSQL connection over a Unix domain socket.
///
/// This is the blocking counterpart to `Connection`. All I/O is synchronous
/// using `std::os::unix::net::UnixStream`. No tokio runtime is required.
///
/// # Thread safety
///
/// `SyncConnection` is `Send` but not `Sync` — it must be used by one thread
/// at a time. This matches the PostgreSQL wire protocol which is inherently
/// sequential.
///
/// # Example
///
/// ```no_run
/// use bsql_driver_postgres::{SyncConnection, Config, Arena};
///
/// let config = Config::from_url("postgres://user@localhost/db?host=/tmp").unwrap();
/// let mut conn = SyncConnection::connect(&config).unwrap();
/// let mut arena = Arena::new();
///
/// let hash = bsql_driver_postgres::hash_sql("SELECT 1 AS n");
/// let result = conn.query("SELECT 1 AS n", hash, &[], &mut arena).unwrap();
/// assert_eq!(result.len(), 1);
/// ```
pub struct SyncConnection {
    stream: UnixStream,
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
    created_at: std::time::Instant,
    pending_notifications: Vec<Notification>,
    max_stmt_cache_size: usize,
    query_counter: u64,
}

impl std::fmt::Debug for SyncConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncConnection")
            .field("pid", &self.pid)
            .field("tx_status", &(self.tx_status as char))
            .field("stmt_cache_len", &self.stmts.len())
            .finish()
    }
}

impl SyncConnection {
    /// Connect to PostgreSQL via Unix domain socket and complete the
    /// startup/auth handshake. Fully synchronous — no tokio runtime needed.
    ///
    /// `config.host` must start with `/` (UDS directory path).
    ///
    /// # Errors
    ///
    /// Returns an error if the host is not a UDS path, connection fails,
    /// or authentication fails.
    pub fn connect(config: &Config) -> Result<Self, DriverError> {
        if !config.host_is_uds() {
            return Err(DriverError::Protocol(
                "SyncConnection requires a Unix domain socket path (host starting with '/')".into(),
            ));
        }

        let path = config.uds_path();
        let stream = UnixStream::connect(&path).map_err(DriverError::Io)?;

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
            created_at: std::time::Instant::now(),
            pending_notifications: Vec::new(),
            max_stmt_cache_size: 256,
            query_counter: 0,
        };

        conn.startup(config)?;
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
        self.write_buf.clear();
        proto::write_parse(&mut self.write_buf, &name, sql, &[]);
        proto::write_describe(&mut self.write_buf, b'S', &name);
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

    /// Execute a prepared query and return rows in arena-allocated storage.
    pub fn query(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        arena: &mut Arena,
    ) -> Result<QueryResult, DriverError> {
        let columns = self
            .send_pipeline(sql, sql_hash, params, true, false)?
            .expect("send_pipeline(need_columns=true) must return Some");

        let num_cols = columns.len();
        let mut all_col_offsets: Vec<(usize, i32)> = Vec::with_capacity(num_cols.max(1) * 8);
        let mut affected_rows: u64 = 0;

        loop {
            let msg = self.read_one_message()?;
            match msg {
                BackendMessage::DataRow { data } => {
                    parse_data_row_flat(data, arena, &mut all_col_offsets)?;
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

        self.expect_ready()?;
        self.shrink_buffers();
        self.touch();

        Ok(QueryResult::from_parts(
            all_col_offsets,
            num_cols,
            columns,
            affected_rows,
        ))
    }

    /// Execute a query without result rows (INSERT/UPDATE/DELETE).
    pub fn execute(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<u64, DriverError> {
        let _ = self.send_pipeline(sql, sql_hash, params, false, false)?;

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
                        "unexpected message during execute: {other:?}"
                    )));
                }
            }
        }

        self.expect_ready()?;
        self.shrink_buffers();
        self.touch();
        Ok(affected_rows)
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
        let _ = self.send_pipeline(sql, sql_hash, params, false, false)?;

        loop {
            let msg = self.read_one_message()?;
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

        self.expect_ready()?;
        self.shrink_buffers();
        self.touch();
        Ok(())
    }

    /// Process each DataRow as raw bytes — fastest path.
    ///
    /// Same zero-copy stream_buf optimization as the async `Connection::for_each_raw`,
    /// but with blocking I/O. No futures, no wakers, no poll overhead.
    pub fn for_each_raw<F>(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        mut f: F,
    ) -> Result<(), DriverError>
    where
        F: FnMut(&[u8]) -> Result<(), DriverError>,
    {
        let _ = self.send_pipeline(sql, sql_hash, params, false, true)?;

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
                self.refill_stream_buf()?;
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
                        // Oversized message — fall back to read_message_buffered.
                        let msg = self.read_one_message()?;
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

                match msg_type {
                    b'D' => {
                        f(&self.stream_buf[payload_start..payload_end])?;
                    }
                    b'C' => {
                        self.stream_buf_pos += total_msg_len;
                        break 'outer;
                    }
                    b'E' => {
                        let fields = proto::parse_error_response(
                            &self.stream_buf[payload_start..payload_end],
                        );
                        self.stream_buf_pos += total_msg_len;
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
                    b'I' => {
                        self.stream_buf_pos += total_msg_len;
                        break 'outer;
                    }
                    _ => {
                        // NoticeResponse, ParameterStatus, etc. — skip.
                    }
                }

                self.stream_buf_pos += total_msg_len;
            }

            self.refill_stream_buf()?;
        }

        self.expect_ready()?;
        self.shrink_buffers();
        self.touch();
        Ok(())
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
                    self.touch();
                    return Ok(());
                }
                BackendMessage::CommandComplete { .. }
                | BackendMessage::RowDescription { .. }
                | BackendMessage::DataRow { .. }
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
                    self.touch();
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
    fn send_pipeline(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
        need_columns: bool,
        skip_bind_complete: bool,
    ) -> Result<Option<Arc<[ColumnDesc]>>, DriverError> {
        debug_assert_eq!(crate::conn::hash_sql(sql), sql_hash, "sql_hash mismatch");

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

            if can_use_template {
                // Fast path: copy template and patch param bytes in-place.
                let tmpl = info.bind_template.as_ref().unwrap();
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
                        // Encode into a scratch buffer to get new data.
                        let mut scratch = Vec::new();
                        param.encode_binary(&mut scratch);

                        if scratch.len() == old_len as usize {
                            // Same size — patch in place (common for fixed-size types).
                            self.write_buf[data_offset..data_offset + scratch.len()]
                                .copy_from_slice(&scratch);
                        } else {
                            // Different size — rebuild Bind from scratch and
                            // re-snapshot the template with new sizes.
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

                if !template_ok {
                    self.write_buf.clear();
                    proto::write_bind_params(&mut self.write_buf, "", &info.name, params);
                    // Invalidate stale template so we re-snapshot below.
                    info.bind_template = None;
                }
            } else {
                proto::write_bind_params(&mut self.write_buf, "", &info.name, params);
            }

            let cols = if need_columns {
                Some(info.columns.clone())
            } else {
                None
            };

            // Snapshot bind template if we don't have one yet (first use or
            // after invalidation due to size mismatch).
            if info.bind_template.is_none() && !self.write_buf.is_empty() {
                info.bind_template = build_bind_template(&self.write_buf, params.len());
            }

            self.write_buf.extend_from_slice(proto::EXECUTE_SYNC);
            self.flush_write()?;

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
            if let Some((&lru_hash, _)) = self.stmts.iter().min_by_key(|(_, info)| info.last_used) {
                if let Some(evicted) = self.stmts.remove(&lru_hash) {
                    proto::write_close(&mut self.write_buf, b'S', &evicted.name);
                }
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

    fn make_server_error(&self, fields: proto::ErrorFields) -> DriverError {
        DriverError::Server {
            code: fields.code,
            message: fields.message.into_boxed_str(),
            detail: fields.detail.map(String::into_boxed_str),
            hint: fields.hint.map(String::into_boxed_str),
            position: fields.position,
        }
    }

    /// Read one backend message, auto-buffering notifications.
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

    /// Flush the write buffer to the Unix domain socket. Blocking.
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
    stream: &mut UnixStream,
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

/// Parse a DataRow message into flat column offset storage.
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
    let mut pos = 2;

    for _ in 0..num_cols {
        if pos + 4 > data.len() {
            return Err(DriverError::Protocol("DataRow truncated".into()));
        }

        let col_len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;

        if col_len < 0 {
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

// --- Bind template builder ---

/// Build a `BindTemplate` from the current write_buf contents.
///
/// Parses the Bind message to locate each parameter's data offset and length.
/// Returns `None` if the Bind message cannot be parsed (e.g., write_buf is empty
/// or contains non-Bind data).
fn build_bind_template(write_buf: &[u8], param_count: usize) -> Option<BindTemplate> {
    // Bind message starts with 'B'.
    if write_buf.is_empty() || write_buf[0] != b'B' {
        return None;
    }

    if write_buf.len() < 5 {
        return None;
    }

    // Skip type byte (1) + length (4).
    let mut pos = 5;

    // Skip portal name (NUL-terminated).
    while pos < write_buf.len() && write_buf[pos] != 0 {
        pos += 1;
    }
    pos += 1; // skip NUL

    // Skip statement name (NUL-terminated).
    while pos < write_buf.len() && write_buf[pos] != 0 {
        pos += 1;
    }
    pos += 1; // skip NUL

    // Skip format codes.
    if pos + 2 > write_buf.len() {
        return None;
    }
    let num_fmt_codes = i16::from_be_bytes([write_buf[pos], write_buf[pos + 1]]);
    pos += 2;
    pos += num_fmt_codes.max(0) as usize * 2; // skip format code values

    // Parameter count.
    if pos + 2 > write_buf.len() {
        return None;
    }
    let wire_param_count = i16::from_be_bytes([write_buf[pos], write_buf[pos + 1]]) as usize;
    pos += 2;

    if wire_param_count != param_count {
        return None;
    }

    let mut param_slots = Vec::with_capacity(param_count);
    for _ in 0..param_count {
        if pos + 4 > write_buf.len() {
            return None;
        }
        let data_len = i32::from_be_bytes([
            write_buf[pos],
            write_buf[pos + 1],
            write_buf[pos + 2],
            write_buf[pos + 3],
        ]);
        pos += 4;

        if data_len < 0 {
            // NULL param — no data bytes.
            param_slots.push((pos, -1));
        } else {
            let data_offset = pos;
            param_slots.push((data_offset, data_len));
            pos += data_len as usize;
        }
    }

    Some(BindTemplate {
        bytes: write_buf.to_vec(),
        param_slots,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conn::hash_sql;

    #[test]
    fn sync_make_stmt_name() {
        let name = make_stmt_name(0);
        assert_eq!(&*name, "s_0000000000000000");
        let name = make_stmt_name(0xDEADBEEF12345678);
        assert_eq!(&*name, "s_deadbeef12345678");
    }

    #[test]
    fn sync_identity_hasher() {
        let mut h = IdentityHasher::default();
        h.write_u64(42);
        assert_eq!(h.finish(), 42);
    }

    #[test]
    fn sync_config_rejects_tcp() {
        let config = Config::from_url("postgres://user:pass@localhost/db").unwrap();
        let result = SyncConnection::connect(&config);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Unix domain socket"),
            "error should mention UDS requirement: {err}"
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

    #[test]
    fn build_bind_template_basic() {
        let mut buf = Vec::new();
        let val: i32 = 42;
        proto::write_bind_params(&mut buf, "", "s_test", &[&val as &(dyn Encode + Sync)]);

        let tmpl = build_bind_template(&buf, 1);
        assert!(tmpl.is_some());
        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.param_slots.len(), 1);
        // i32 is 4 bytes
        assert_eq!(tmpl.param_slots[0].1, 4);
    }

    #[test]
    fn build_bind_template_null_param() {
        let mut buf = Vec::new();
        let val: Option<i32> = None;
        proto::write_bind_params(&mut buf, "", "s_test", &[&val as &(dyn Encode + Sync)]);

        let tmpl = build_bind_template(&buf, 1);
        assert!(tmpl.is_some());
        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.param_slots.len(), 1);
        assert_eq!(tmpl.param_slots[0].1, -1); // NULL
    }

    #[test]
    fn build_bind_template_multiple_params() {
        let mut buf = Vec::new();
        let id: i32 = 1;
        let name: &str = "alice";
        proto::write_bind_params(
            &mut buf,
            "",
            "s_test",
            &[&id as &(dyn Encode + Sync), &name as &(dyn Encode + Sync)],
        );

        let tmpl = build_bind_template(&buf, 2);
        assert!(tmpl.is_some());
        let tmpl = tmpl.unwrap();
        assert_eq!(tmpl.param_slots.len(), 2);
        assert_eq!(tmpl.param_slots[0].1, 4); // i32 = 4 bytes
        assert_eq!(tmpl.param_slots[1].1, 5); // "alice" = 5 bytes
    }

    #[test]
    fn build_bind_template_empty_buf() {
        let tmpl = build_bind_template(&[], 0);
        assert!(tmpl.is_none());
    }

    #[test]
    fn build_bind_template_wrong_type() {
        let tmpl = build_bind_template(&[b'E', 0, 0, 0, 4], 0);
        assert!(tmpl.is_none());
    }

    #[test]
    fn build_bind_template_param_count_mismatch() {
        let mut buf = Vec::new();
        let val: i32 = 42;
        proto::write_bind_params(&mut buf, "", "s_test", &[&val as &(dyn Encode + Sync)]);

        // Ask for 2 params but only 1 in the message.
        let tmpl = build_bind_template(&buf, 2);
        assert!(tmpl.is_none());
    }

    #[test]
    fn hash_sql_consistency() {
        // Verify our module uses the same hash function as conn.rs.
        let h = hash_sql("SELECT 1");
        assert_eq!(h, hash_sql("SELECT 1"));
        assert_ne!(h, hash_sql("SELECT 2"));
    }
}
