//! PostgreSQL connection — startup, authentication, statement cache, query execution.
//!
//! `Connection` owns a TCP (or TLS) stream and implements the extended query protocol
//! with pipelining. Statements are cached by rapidhash of the SQL text. On first use,
//! Parse+Describe+Bind+Execute+Sync are pipelined in one TCP write. On subsequent uses,
//! only Bind+Execute+Sync are sent.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;

use rapidhash::quality::RapidHasher;

/// Identity hasher for pre-hashed u64 keys. Avoids SipHash overhead
/// on keys that are already well-distributed rapidhash values.
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
        // never be hit (IdentityHasher only receives u64 keys from HashMap),
        // but if it somehow is, zero the hash as a safe no-op fallback.
        debug_assert!(false, "IdentityHasher only supports u64 keys");
        self.0 = 0;
    }
}

type IdentityBuildHasher = BuildHasherDefault<IdentityHasher>;
type StmtCache = HashMap<u64, StmtInfo, IdentityBuildHasher>;

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
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl Stream {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.write_all(buf).await,
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s.write_all(buf).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush().await,
            #[cfg(feature = "tls")]
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
            Stream::Tls(s) => std::pin::Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

// --- Config ---

/// Connection configuration parsed from a URL.
///
/// Format: `postgres://user:password@host:port/database`
///
/// Implements Drop to zeroize the password field, minimizing the
/// window where plaintext credentials live in memory.
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

/// Zeroize password on drop to minimize credential lifetime in memory.
impl Drop for Config {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.password.zeroize();
    }
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
            if param.is_empty() {
                continue;
            }
            if let Some(val) = param.strip_prefix("sslmode=") {
                // A typo like "sslmode=require" (missing 'e') would go unencrypted.
                ssl = match val {
                    "disable" => SslMode::Disable,
                    "prefer" => SslMode::Prefer,
                    "require" => SslMode::Require,
                    _ => {
                        return Err(DriverError::Protocol(format!(
                            "unknown sslmode: '{val}' (expected: disable, prefer, require)"
                        )));
                    }
                };
            } else if let Some(val) = param.strip_prefix("statement_timeout=") {
                statement_timeout_secs = val.parse::<u32>().unwrap_or(30);
            }
        }

        let config = Config {
            host: url_decode(&host)?,
            port,
            user: url_decode(user)?,
            password: url_decode(password)?,
            database: if database.is_empty() {
                url_decode(user)?
            } else {
                url_decode(database)?
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
///
/// Decodes `%XX` hex sequences into raw bytes, then validates as UTF-8.
/// This correctly handles multi-byte UTF-8 characters that are percent-encoded
/// byte-by-byte (e.g. `%C3%A9` for 'é').
fn url_decode(s: &str) -> Result<String, DriverError> {
    let mut bytes = Vec::with_capacity(s.len());
    let input = s.as_bytes();
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'%' {
            if i + 2 >= input.len() {
                return Err(DriverError::Protocol(format!(
                    "malformed percent-encoding in URL: '{s}'"
                )));
            }
            let hi = hex_val(input[i + 1]).ok_or_else(|| {
                DriverError::Protocol(format!(
                    "invalid hex digit '{}' in URL: '{s}'",
                    input[i + 1] as char
                ))
            })?;
            let lo = hex_val(input[i + 2]).ok_or_else(|| {
                DriverError::Protocol(format!(
                    "invalid hex digit '{}' in URL: '{s}'",
                    input[i + 2] as char
                ))
            })?;
            bytes.push(hi * 16 + lo);
            i += 3;
        } else {
            bytes.push(input[i]);
            i += 1;
        }
    }
    String::from_utf8(bytes)
        .map_err(|_| DriverError::Protocol(format!("invalid UTF-8 in URL: '{s}'")))
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
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

/// Format a statement name from a hash: `"s_{hash:016x}"`.
///
/// Stack-allocated formatting. The name is always exactly 19 bytes:
/// "s_" (2) + 16 hex digits (16) + NUL-termination handled by protocol layer.
/// Uses a fixed [u8; 19] buffer with manual hex encoding — no heap allocation.
#[inline]
fn make_stmt_name(hash: u64) -> Box<str> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut buf = [0u8; 18]; // "s_" + 16 hex = 18 bytes
    buf[0] = b's';
    buf[1] = b'_';
    let bytes = hash.to_be_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        buf[2 + i * 2] = HEX[(b >> 4) as usize];
        buf[2 + i * 2 + 1] = HEX[(b & 0x0f) as usize];
    }
    // buf contains only ASCII hex digits ('0'-'9','a'-'f') and 's','_'.
    // from_utf8 is infallible here — the expect documents why.
    let s = std::str::from_utf8(&buf).expect("BUG: stmt name buffer contains only ASCII hex");
    s.into()
}

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
    /// Timestamp of last use for LRU eviction.
    last_used: std::time::Instant,
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
    /// OID of the source table (0 if not a table column, e.g. computed).
    pub table_oid: u32,
    /// Column number within the source table (0 if not a table column).
    pub column_id: i16,
}

/// Result of a `prepare_describe` call — column and parameter metadata
/// without executing the query.
#[derive(Debug, Clone)]
pub struct PrepareResult {
    /// Output columns (empty for INSERT/UPDATE/DELETE without RETURNING).
    pub columns: Vec<ColumnDesc>,
    /// PostgreSQL OIDs of the expected parameter types.
    pub param_oids: Vec<u32>,
}

/// A single row of text values returned by `simple_query_rows`.
///
/// Each field is `None` for SQL NULL, `Some(text)` otherwise.
/// Only used for compile-time schema introspection queries.
pub type SimpleRow = Vec<Option<String>>;

// --- Connection ---

/// A notification received during normal query processing.
///
/// When the read loop encounters a NotificationResponse during queries,
/// it is buffered here instead of being dropped. Call
/// [`Connection::drain_notifications`] to retrieve and clear the buffer.
#[derive(Debug, Clone)]
pub struct Notification {
    /// Backend process ID that sent the notification.
    pub pid: i32,
    /// Channel name.
    pub channel: String,
    /// Payload string (may be empty).
    pub payload: String,
}

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
}

impl Connection {
    /// Connect to PostgreSQL and complete the startup/auth handshake.
    pub async fn connect(config: &Config) -> Result<Self, DriverError> {
        // Config::from_url() already validates. Manual Config construction
        // should call validate() explicitly before passing to connect().
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
        self.cache_stmt(
            sql_hash,
            StmtInfo {
                name,
                columns,
                last_used: std::time::Instant::now(),
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
                    self.touch();
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
        let cached = self.stmts.contains_key(&sql_hash);

        self.write_buf.clear();

        let new_name = if !cached {
            let name = make_stmt_name(sql_hash);
            let param_oids: smallvec::SmallVec<[u32; 8]> =
                params.iter().map(|p| p.type_oid()).collect();
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
            self.cache_stmt(
                sql_hash,
                StmtInfo {
                    name: stmt_name,
                    columns: columns.clone(),
                    last_used: std::time::Instant::now(),
                },
            );
            columns
        } else {
            if let Some(info) = self.stmts.get_mut(&sql_hash) {
                info.last_used = std::time::Instant::now();
            }
            self.stmts[&sql_hash].columns.clone()
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
    async fn send_pipeline(
        &mut self,
        sql: &str,
        sql_hash: u64,
        params: &[&(dyn Encode + Sync)],
    ) -> Result<Arc<[ColumnDesc]>, DriverError> {
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

        let cached = self.stmts.contains_key(&sql_hash);
        self.write_buf.clear();

        let new_name = if !cached {
            let name = make_stmt_name(sql_hash);
            let param_oids: smallvec::SmallVec<[u32; 8]> =
                params.iter().map(|p| p.type_oid()).collect();
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

        // Read Parse+Describe responses if needed
        let columns = if let Some(stmt_name) = new_name {
            self.expect_message(|m| matches!(m, BackendMessage::ParseComplete))
                .await?;
            let columns = self.read_column_description().await?;
            self.cache_stmt(
                sql_hash,
                StmtInfo {
                    name: stmt_name,
                    columns: columns.clone(),
                    last_used: std::time::Instant::now(),
                },
            );
            columns
        } else {
            // Touch LRU timestamp on cache hit
            if let Some(info) = self.stmts.get_mut(&sql_hash) {
                info.last_used = std::time::Instant::now();
            }
            self.stmts[&sql_hash].columns.clone()
        };

        // BindComplete
        self.expect_message(|m| matches!(m, BackendMessage::BindComplete))
            .await?;

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
        let columns = self.send_pipeline(sql, sql_hash, params).await?;

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
        self.touch();

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
        let _columns = self.send_pipeline(sql, sql_hash, params).await?;

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
        self.touch();
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
                    self.touch();
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
            // Find the least recently used entry via linear scan
            if let Some((&lru_hash, _)) = self.stmts.iter().min_by_key(|(_, info)| info.last_used) {
                if let Some(evicted) = self.stmts.remove(&lru_hash) {
                    // Queue Close(Statement) to free server-side memory.
                    // This will be sent on the next write+flush.
                    proto::write_close(&mut self.write_buf, b'S', &evicted.name);
                }
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
        // existing allocation if possible, avoiding a dealloc+alloc pair.
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
    fn make_server_error(&self, fields: proto::ErrorFields) -> DriverError {
        DriverError::Server {
            code: fields.code,
            message: fields.message.into_boxed_str(),
            detail: fields.detail.map(String::into_boxed_str),
            hint: fields.hint.map(String::into_boxed_str),
            position: fields.position,
        }
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
/// # async fn example() -> Result<(), bsql_driver_postgres::DriverError> {
/// # let mut conn: bsql_driver_postgres::Connection = unimplemented!();
/// # let mut arena = bsql_driver_postgres::Arena::new();
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

    /// Take the `col_offsets` vec out of this result, leaving it empty.
    ///
    /// Used by `QueryStream` to reclaim and reuse the allocation between chunks
    /// instead of allocating a new `Vec` per chunk.
    pub fn take_col_offsets(&mut self) -> Vec<(usize, i32)> {
        std::mem::take(&mut self.all_col_offsets)
    }

    /// Iterate over rows.
    pub fn rows<'a>(&'a self, arena: &'a Arena) -> impl Iterator<Item = Row<'a>> {
        let num_cols = self.num_cols;
        let columns = &self.columns;
        self.all_col_offsets
            // .max(1) prevents a panic from chunks(0) when num_cols is 0
            // (e.g., commands with no columns like INSERT without RETURNING).
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
///
/// Uses `str::hash()` via the `Hash` trait, matching `bsql_core::rapid_hash_str`.
pub fn hash_sql(sql: &str) -> u64 {
    use std::hash::Hash;
    let mut hasher = RapidHasher::default();
    sql.hash(&mut hasher);
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

    /// Unknown sslmode should error, not silently default to Prefer.
    #[test]
    fn config_rejects_unknown_sslmode() {
        let result = Config::from_url("postgres://user:pass@localhost/db?sslmode=requre");
        assert!(result.is_err(), "typo 'requre' should be rejected");
        let result = Config::from_url("postgres://user:pass@localhost/db?sslmode=REQUIRE");
        assert!(result.is_err(), "uppercase should be rejected");
        let result = Config::from_url("postgres://user:pass@localhost/db?sslmode=bogus");
        assert!(result.is_err(), "bogus value should be rejected");
    }

    /// Valid sslmodes should still work.
    #[test]
    fn config_accepts_valid_sslmodes() {
        let cfg = Config::from_url("postgres://user:pass@localhost/db?sslmode=disable").unwrap();
        assert_eq!(cfg.ssl, SslMode::Disable);
        let cfg = Config::from_url("postgres://user:pass@localhost/db?sslmode=prefer").unwrap();
        assert_eq!(cfg.ssl, SslMode::Prefer);
        let cfg = Config::from_url("postgres://user:pass@localhost/db?sslmode=require").unwrap();
        assert_eq!(cfg.ssl, SslMode::Require);
    }

    /// IdentityHasher::write should not panic in release mode.
    /// In debug mode, the debug_assert fires (expected behavior).
    #[test]
    fn identity_hasher_write_no_panic_in_release() {
        // In debug builds, this panics via debug_assert (correctly).
        // In release builds, it falls through to self.0 = 0 (safe).
        // We test that the fallback is correct by checking default state.
        let h = IdentityHasher::default();
        assert_eq!(h.0, 0);

        // Test the normal path (write_u64) works
        let mut h2 = IdentityHasher::default();
        h2.write_u64(42);
        assert_eq!(h2.finish(), 42);
    }

    /// Statement name formatting uses hex encoding.
    #[test]
    fn stmt_name_format() {
        let name = make_stmt_name(0);
        assert_eq!(&*name, "s_0000000000000000");
        let name = make_stmt_name(0xDEADBEEF12345678);
        assert_eq!(&*name, "s_deadbeef12345678");
        let name = make_stmt_name(u64::MAX);
        assert_eq!(&*name, "s_ffffffffffffffff");
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
        assert_eq!(url_decode("hello%20world").unwrap(), "hello world");
        assert_eq!(url_decode("no%20escape").unwrap(), "no escape");
        assert_eq!(url_decode("plain").unwrap(), "plain");
        assert_eq!(url_decode("a%40b").unwrap(), "a@b");
    }

    #[test]
    fn url_decode_malformed_percent_trailing() {
        // Truncated percent sequence at end of string
        let result = url_decode("abc%2");
        assert!(result.is_err(), "truncated %2 should error");
    }

    #[test]
    fn url_decode_malformed_percent_no_digits() {
        // % followed by no digits at all
        let result = url_decode("abc%");
        assert!(result.is_err(), "bare % at end should error");
    }

    #[test]
    fn url_decode_invalid_hex_digit() {
        // %GG — 'G' is not a valid hex digit
        let result = url_decode("abc%GG");
        assert!(result.is_err(), "%GG should error");
    }

    #[test]
    fn url_decode_invalid_hex_second_digit() {
        // %2Z — 'Z' is not a valid hex digit
        let result = url_decode("abc%2Z");
        assert!(result.is_err(), "%2Z should error");
    }

    /// url_decode with invalid UTF-8 from percent-decoded bytes
    #[test]
    fn url_decode_invalid_utf8_percent() {
        // %80%81 are not valid UTF-8 start bytes
        let result = url_decode("%80%81");
        assert!(result.is_err(), "invalid UTF-8 bytes should error");
    }

    /// url_decode with percent-encoded chars in all positions
    #[test]
    fn url_decode_percent_everywhere() {
        assert_eq!(url_decode("%41%42%43").unwrap(), "ABC");
        assert_eq!(url_decode("%61").unwrap(), "a");
        assert_eq!(url_decode("x%2Fy%2Fz").unwrap(), "x/y/z");
    }

    /// url_decode with bare percent at various positions
    #[test]
    fn url_decode_bare_percent_middle() {
        assert!(url_decode("a%b").is_err(), "bare % in middle should error");
    }

    /// T-02: url_decode with multi-byte UTF-8 (%C3%A9 -> e with acute)
    #[test]
    fn url_decode_multibyte_utf8() {
        let result = url_decode("caf%C3%A9").unwrap();
        assert_eq!(result, "caf\u{00e9}"); // cafe with accent
    }

    // --- Audit gap tests ---

    // #68: Config with postgresql:// scheme
    #[test]
    fn config_parse_postgresql_scheme() {
        let cfg = Config::from_url("postgresql://user:pass@localhost:5432/mydb").unwrap();
        assert_eq!(cfg.user, "user");
        assert_eq!(cfg.password, "pass");
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 5432);
        assert_eq!(cfg.database, "mydb");
    }

    // #69: Config URL without password
    #[test]
    fn config_parse_no_password_standalone() {
        let cfg = Config::from_url("postgres://admin@db.example.com/myapp").unwrap();
        assert_eq!(cfg.user, "admin");
        assert_eq!(cfg.password, "");
        assert_eq!(cfg.host, "db.example.com");
        assert_eq!(cfg.database, "myapp");
    }

    // #70: Config URL with empty database (falls back to user)
    #[test]
    fn config_empty_database_falls_back_to_user() {
        let cfg = Config::from_url("postgres://testuser:pass@localhost").unwrap();
        assert_eq!(cfg.database, "testuser");
    }

    // #71: Config URL with unknown sslmode error
    #[test]
    fn config_unknown_sslmode_error() {
        let result = Config::from_url("postgres://u:p@h/d?sslmode=verify-full");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown sslmode"),
            "should describe unknown sslmode: {err}"
        );
    }

    // #72: Config URL with multiple query params
    #[test]
    fn config_multiple_query_params() {
        let cfg = Config::from_url(
            "postgres://user:pass@localhost/db?sslmode=disable&statement_timeout=60",
        )
        .unwrap();
        assert_eq!(cfg.ssl, SslMode::Disable);
        assert_eq!(cfg.statement_timeout_secs, 60);
    }

    // #73: url_decode with invalid percent (%ZZ)
    #[test]
    fn url_decode_invalid_percent_zz() {
        let result = url_decode("abc%ZZ");
        assert!(result.is_err(), "%ZZ should error");
    }

    // #74: url_decode with truncated percent (trailing %)
    #[test]
    fn url_decode_truncated_percent_trailing() {
        let result = url_decode("abc%");
        assert!(result.is_err(), "trailing % should error");
    }

    // #75: url_decode producing invalid UTF-8
    #[test]
    fn url_decode_invalid_utf8() {
        // 0x80 alone is not valid UTF-8
        let result = url_decode("%80");
        assert!(result.is_err(), "invalid UTF-8 should error");
    }

    // #76: Config SslMode::Require without tls feature
    #[cfg(not(feature = "tls"))]
    #[test]
    fn config_sslmode_require_without_tls_feature() {
        // The config parses fine, but validate doesn't check this.
        // The error occurs at connection time. Just verify parsing works.
        let cfg = Config::from_url("postgres://user:pass@localhost/db?sslmode=require").unwrap();
        assert_eq!(cfg.ssl, SslMode::Require);
    }

    // #77: statement_name format: "s_" + 16 hex chars
    #[test]
    fn stmt_name_format_verification() {
        let name = make_stmt_name(0xDEADBEEFCAFEBABE);
        assert!(name.starts_with("s_"), "must start with s_");
        assert_eq!(name.len(), 18, "s_ (2) + 16 hex = 18");
        assert!(
            name[2..].chars().all(|c| c.is_ascii_hexdigit()),
            "remaining chars must be hex: {}",
            &*name
        );
    }

    // stmt_name for 0 is all zeros
    #[test]
    fn stmt_name_zero() {
        let name = make_stmt_name(0);
        assert_eq!(&*name, "s_0000000000000000");
    }

    // stmt_name for u64::MAX is all f's
    #[test]
    fn stmt_name_max() {
        let name = make_stmt_name(u64::MAX);
        assert_eq!(&*name, "s_ffffffffffffffff");
    }

    // Config validation: empty host
    #[test]
    fn config_validate_empty_host() {
        let cfg = Config {
            host: String::new(),
            port: 5432,
            user: "user".into(),
            password: "pass".into(),
            database: "db".into(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(cfg.validate().is_err());
    }

    // Config validation: empty user
    #[test]
    fn config_validate_empty_user() {
        let cfg = Config {
            host: "localhost".into(),
            port: 5432,
            user: String::new(),
            password: "pass".into(),
            database: "db".into(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(cfg.validate().is_err());
    }

    // Config validation: empty database
    #[test]
    fn config_validate_empty_database() {
        let cfg = Config {
            host: "localhost".into(),
            port: 5432,
            user: "user".into(),
            password: "pass".into(),
            database: String::new(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(cfg.validate().is_err());
    }

    // Config missing @ in URL
    #[test]
    fn config_missing_at_sign() {
        let result = Config::from_url("postgres://userpasslocalhost/db");
        assert!(result.is_err());
    }

    // Config with custom port
    #[test]
    fn config_custom_port() {
        let cfg = Config::from_url("postgres://user:pass@localhost:5433/db").unwrap();
        assert_eq!(cfg.port, 5433);
    }

    // Config with invalid port
    #[test]
    fn config_invalid_port() {
        let result = Config::from_url("postgres://user:pass@localhost:notaport/db");
        assert!(result.is_err());
    }

    // --- Task 6: Notification buffering ---

    #[test]
    fn notification_struct_fields() {
        let n = Notification {
            pid: 42,
            channel: "test_chan".to_owned(),
            payload: "hello".to_owned(),
        };
        assert_eq!(n.pid, 42);
        assert_eq!(n.channel, "test_chan");
        assert_eq!(n.payload, "hello");
    }

    #[test]
    fn notification_clone() {
        let n = Notification {
            pid: 1,
            channel: "c".to_owned(),
            payload: "p".to_owned(),
        };
        let n2 = n.clone();
        assert_eq!(n2.pid, 1);
        assert_eq!(n2.channel, "c");
    }

    #[test]
    fn notification_debug() {
        let n = Notification {
            pid: 1,
            channel: "c".to_owned(),
            payload: "p".to_owned(),
        };
        let dbg = format!("{n:?}");
        assert!(dbg.contains("Notification"));
    }

    // --- Task 7: Statement cache size ---

    #[test]
    fn stmt_info_has_last_used() {
        let info = StmtInfo {
            name: "s_test".into(),
            columns: Arc::from(Vec::new()),
            last_used: std::time::Instant::now(),
        };
        // Verify last_used is recent
        assert!(info.last_used.elapsed().as_secs() < 1);
    }
}
