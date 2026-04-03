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

    #[expect(
        dead_code,
        reason = "kept for completeness; may be needed for non-TCP_NODELAY paths"
    )]
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
    ///
    /// # Unix domain sockets
    ///
    /// Use the `host` query parameter to specify a UDS directory (libpq convention):
    /// ```text
    /// postgres://user@localhost/dbname?host=/tmp
    /// postgres:///dbname?host=/var/run/postgresql
    /// ```
    /// When `host` starts with `/`, the driver connects via Unix domain socket at
    /// `{host}/.s.PGSQL.{port}` instead of TCP. TLS is skipped for UDS connections.
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
        let mut host_override: Option<String> = None;
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
            } else if let Some(val) = param.strip_prefix("host=") {
                host_override = Some(url_decode(val)?);
            }
        }

        // If ?host=/path was specified, override the URL hostname with it.
        // This follows the libpq convention: host=/tmp means UDS.
        let final_host = if let Some(h) = host_override {
            h
        } else {
            url_decode(&host)?
        };

        let config = Config {
            host: final_host,
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

    /// Returns `true` if the host is a Unix domain socket directory path.
    ///
    /// libpq convention: if `host` starts with `/`, the connection uses a
    /// Unix domain socket at `{host}/.s.PGSQL.{port}`.
    pub fn host_is_uds(&self) -> bool {
        self.host.starts_with('/')
    }

    /// Returns the Unix domain socket path: `{host}/.s.PGSQL.{port}`.
    ///
    /// Only meaningful when [`host_is_uds()`](Self::host_is_uds) returns `true`.
    pub fn uds_path(&self) -> String {
        format!("{}/.s.PGSQL.{}", self.host, self.port)
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
    /// Monotonic counter value at last use for LRU eviction.
    /// Cheaper than `Instant::now()` which is a syscall on macOS (~20-40ns).
    last_used: u64,
    /// Pre-built Bind message template for fast re-execution.
    ///
    /// On the first execution of a cached statement, we snapshot the complete
    /// Bind message bytes. On subsequent executions with fixed-size parameters,
    /// we memcpy the template and patch only the parameter data in-place,
    /// avoiding the full `write_bind_params` rebuild (~100-200ns savings per
    /// query on the hot path).
    ///
    /// `None` until the first execution populates it.
    bind_template: Option<BindTemplate>,
}

/// Pre-built Bind message template for fast re-execution.
///
/// Stores the complete Bind message bytes and the byte offsets where
/// each parameter's data begins. On re-execution with same-sized params,
/// we copy the template and overwrite param data in-place.
struct BindTemplate {
    /// Complete Bind message bytes (type 'B' + length + payload).
    bytes: Vec<u8>,
    /// For each parameter: `(data_offset, data_len)` within `bytes`.
    /// `data_offset` points to the first byte of param data (after the i32 length).
    /// `data_len` is the length of the param data. -1 means NULL.
    param_slots: Vec<(usize, i32)>,
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
                self.write_buf.extend_from_slice(&tmpl.bytes);

                let mut template_ok = true;
                for (i, param) in params.iter().enumerate() {
                    let (data_offset, old_len) = tmpl.param_slots[i];
                    if param.is_null() {
                        let len_offset = data_offset - 4;
                        self.write_buf[len_offset..len_offset + 4]
                            .copy_from_slice(&(-1i32).to_be_bytes());
                    } else if old_len >= 0 {
                        let mut scratch = Vec::new();
                        param.encode_binary(&mut scratch);
                        if scratch.len() == old_len as usize {
                            self.write_buf[data_offset..data_offset + scratch.len()]
                                .copy_from_slice(&scratch);
                        } else {
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

            if can_use_template {
                // Fast path: copy template and patch param bytes in-place.
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
                        let mut scratch = Vec::new();
                        param.encode_binary(&mut scratch);
                        if scratch.len() == old_len as usize {
                            self.write_buf[data_offset..data_offset + scratch.len()]
                                .copy_from_slice(&scratch);
                        } else {
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

            // Clone Arc only when caller needs columns (query path).
            // for_each_raw / execute skip this atomic increment.
            let cols = if need_columns {
                Some(info.columns.clone())
            } else {
                None
            };

            // Snapshot bind template on first use or after invalidation.
            if info.bind_template.is_none() && !self.write_buf.is_empty() {
                info.bind_template = build_bind_template(&self.write_buf, params.len());
            }

            self.write_buf.extend_from_slice(proto::EXECUTE_SYNC);
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
        self.touch();
        Ok(affected_rows)
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
        self.touch();
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

                match msg_type {
                    b'D' => {
                        // DataRow — ZERO COPY from stream_buf.
                        // Safety: payload_start..payload_end is within stream_buf bounds
                        // (checked by `avail < total_msg_len` above).
                        f(&self.stream_buf[payload_start..payload_end])?;
                    }
                    b'C' => {
                        // CommandComplete — done.
                        self.stream_buf_pos += total_msg_len;
                        break 'outer;
                    }
                    b'E' => {
                        // ErrorResponse — parse owned error fields from stream_buf slice,
                        // then advance position before calling drain_to_ready.
                        let fields = proto::parse_error_response(
                            &self.stream_buf[payload_start..payload_end],
                        );
                        self.stream_buf_pos += total_msg_len;
                        self.maybe_invalidate_stmt_cache(&fields, sql_hash);
                        self.drain_to_ready().await?;
                        return Err(self.make_server_error(fields));
                    }
                    b'A' => {
                        // NotificationResponse — buffer it.
                        // Parse from stream_buf, extract owned data, then advance.
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
                        // EmptyQuery — done.
                        self.stream_buf_pos += total_msg_len;
                        break 'outer;
                    }
                    _ => {
                        // NoticeResponse (b'N'), ParameterStatus (b'S'), etc. — skip.
                    }
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
        self.touch();
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

// --- Bind template builder ---

/// Build a `BindTemplate` from the current write_buf contents.
///
/// Parses the Bind message to locate each parameter's data offset and length.
/// Returns `None` if the message cannot be parsed.
fn build_bind_template(write_buf: &[u8], param_count: usize) -> Option<BindTemplate> {
    if write_buf.is_empty() || write_buf[0] != b'B' {
        return None;
    }
    if write_buf.len() < 5 {
        return None;
    }

    let mut pos = 5; // skip type byte (1) + length (4)

    // Skip portal name (NUL-terminated).
    while pos < write_buf.len() && write_buf[pos] != 0 {
        pos += 1;
    }
    pos += 1;

    // Skip statement name (NUL-terminated).
    while pos < write_buf.len() && write_buf[pos] != 0 {
        pos += 1;
    }
    pos += 1;

    // Skip format codes.
    if pos + 2 > write_buf.len() {
        return None;
    }
    let num_fmt_codes = i16::from_be_bytes([write_buf[pos], write_buf[pos + 1]]);
    pos += 2;
    pos += num_fmt_codes.max(0) as usize * 2;

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
            param_slots.push((pos, -1));
        } else {
            param_slots.push((pos, data_len));
            pos += data_len as usize;
        }
    }

    Some(BindTemplate {
        bytes: write_buf.to_vec(),
        param_slots,
    })
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

// --- PgDataRow (zero-copy row view for for_each) ---

/// A temporary view of a single PostgreSQL DataRow message.
///
/// Reads columns directly from the wire buffer — no arena copy.
/// Column offsets are pre-computed on construction using a `SmallVec`
/// that is stack-allocated for up to 16 columns (zero heap allocation
/// for the common case).
///
/// Lifetime `'a` borrows from `Connection::read_buf`.
pub struct PgDataRow<'a> {
    data: &'a [u8],
    /// Pre-scanned `(byte_offset, wire_len)` pairs for each column.
    /// `wire_len = -1` means NULL.
    offsets: smallvec::SmallVec<[(usize, i32); 16]>,
}

impl<'a> PgDataRow<'a> {
    /// Parse column boundaries from a raw DataRow payload.
    ///
    /// `data` is the DataRow message payload (after the 'D' type byte and
    /// 4-byte length prefix have been stripped by the framing layer).
    pub fn new(data: &'a [u8]) -> Result<Self, DriverError> {
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
        let mut offsets = smallvec::SmallVec::<[(usize, i32); 16]>::with_capacity(num_cols);
        let mut pos = 2usize;
        for _ in 0..num_cols {
            if pos + 4 > data.len() {
                return Err(DriverError::Protocol("DataRow truncated".into()));
            }
            let col_len =
                i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;
            offsets.push((pos, col_len));
            if col_len > 0 {
                pos += col_len as usize;
            }
        }
        Ok(Self { data, offsets })
    }

    /// Get the raw bytes for a column, or `None` if NULL.
    #[inline]
    pub fn get_raw(&self, idx: usize) -> Option<&'a [u8]> {
        let (offset, len) = self.offsets[idx];
        if len < 0 {
            None
        } else {
            Some(&self.data[offset..offset + len as usize])
        }
    }

    /// Whether a column is NULL.
    #[inline]
    pub fn is_null(&self, idx: usize) -> bool {
        self.offsets[idx].1 < 0
    }

    /// Number of columns.
    #[inline]
    pub fn column_count(&self) -> usize {
        self.offsets.len()
    }

    /// Get a boolean column value. Returns `None` on NULL or decode error.
    #[inline]
    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_bool(data).ok())
    }

    /// Get an i16 column value.
    #[inline]
    pub fn get_i16(&self, idx: usize) -> Option<i16> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_i16(data).ok())
    }

    /// Get an i32 column value.
    #[inline]
    pub fn get_i32(&self, idx: usize) -> Option<i32> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_i32(data).ok())
    }

    /// Get an i64 column value.
    #[inline]
    pub fn get_i64(&self, idx: usize) -> Option<i64> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_i64(data).ok())
    }

    /// Get an f32 column value.
    #[inline]
    pub fn get_f32(&self, idx: usize) -> Option<f32> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_f32(data).ok())
    }

    /// Get an f64 column value.
    #[inline]
    pub fn get_f64(&self, idx: usize) -> Option<f64> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_f64(data).ok())
    }

    /// Get a string column value (zero-copy borrow from the wire buffer).
    #[inline]
    pub fn get_str(&self, idx: usize) -> Option<&'a str> {
        self.get_raw(idx)
            .and_then(|data| crate::codec::decode_str(data).ok())
    }

    /// Get a byte slice column value (zero-copy borrow from the wire buffer).
    #[inline]
    pub fn get_bytes(&self, idx: usize) -> Option<&'a [u8]> {
        self.get_raw(idx)
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
#[allow(clippy::approx_constant)]
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
    fn stmt_info_has_last_used_counter() {
        let info = StmtInfo {
            name: "s_test".into(),
            columns: Arc::from(Vec::new()),
            last_used: 42,
            bind_template: None,
        };
        // Verify last_used counter is stored correctly
        assert_eq!(info.last_used, 42);
    }

    // --- PgDataRow tests ---

    /// Build a DataRow payload: [i16 num_cols] ([i32 len] [bytes])...
    /// len = -1 for NULL
    fn make_data_row(columns: &[Option<&[u8]>]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for col in columns {
            match col {
                Some(data) => {
                    buf.extend_from_slice(&(data.len() as i32).to_be_bytes());
                    buf.extend_from_slice(data);
                }
                None => {
                    buf.extend_from_slice(&(-1i32).to_be_bytes());
                }
            }
        }
        buf
    }

    #[test]
    fn pg_data_row_get_i32() {
        let data = make_data_row(&[Some(&42i32.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i32(0), Some(42));
        assert_eq!(row.column_count(), 1);
    }

    #[test]
    fn pg_data_row_get_i64() {
        let data = make_data_row(&[Some(&12345i64.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i64(0), Some(12345));
    }

    #[test]
    fn pg_data_row_get_str() {
        let data = make_data_row(&[Some(b"hello")]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_str(0), Some("hello"));
    }

    #[test]
    fn pg_data_row_get_bytes() {
        let data = make_data_row(&[Some(&[0xDE, 0xAD, 0xBE, 0xEF])]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_bytes(0), Some(&[0xDE, 0xAD, 0xBE, 0xEF][..]));
    }

    #[test]
    fn pg_data_row_get_bool() {
        let data = make_data_row(&[Some(&[1u8])]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_bool(0), Some(true));

        let data = make_data_row(&[Some(&[0u8])]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_bool(0), Some(false));
    }

    #[test]
    fn pg_data_row_get_f64() {
        let data = make_data_row(&[Some(&3.14f64.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert!((row.get_f64(0).unwrap() - 3.14).abs() < 1e-10);
    }

    #[test]
    fn pg_data_row_null_column() {
        let data = make_data_row(&[None]);
        let row = PgDataRow::new(&data).unwrap();
        assert!(row.is_null(0));
        assert_eq!(row.get_i32(0), None);
        assert_eq!(row.get_str(0), None);
    }

    #[test]
    fn pg_data_row_multiple_columns() {
        let data = make_data_row(&[
            Some(&42i32.to_be_bytes()),
            Some(b"alice"),
            Some(b"alice@example.com"),
            Some(&[1u8]),
            Some(&3.14f64.to_be_bytes()),
        ]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.column_count(), 5);
        assert_eq!(row.get_i32(0), Some(42));
        assert_eq!(row.get_str(1), Some("alice"));
        assert_eq!(row.get_str(2), Some("alice@example.com"));
        assert_eq!(row.get_bool(3), Some(true));
        assert!((row.get_f64(4).unwrap() - 3.14).abs() < 1e-10);
    }

    #[test]
    fn pg_data_row_mixed_null() {
        let data = make_data_row(&[Some(&42i32.to_be_bytes()), None, Some(b"text")]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i32(0), Some(42));
        assert!(row.is_null(1));
        assert_eq!(row.get_str(1), None);
        assert_eq!(row.get_str(2), Some("text"));
    }

    #[test]
    fn pg_data_row_empty() {
        let data = make_data_row(&[]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.column_count(), 0);
    }

    #[test]
    fn pg_data_row_too_short() {
        let data = vec![0u8]; // only 1 byte, need at least 2
        assert!(PgDataRow::new(&data).is_err());
    }

    #[test]
    fn pg_data_row_truncated() {
        // Declare 2 columns but only include 1
        let mut data = Vec::new();
        data.extend_from_slice(&2i16.to_be_bytes());
        data.extend_from_slice(&4i32.to_be_bytes());
        data.extend_from_slice(&42i32.to_be_bytes());
        // Missing second column
        assert!(PgDataRow::new(&data).is_err());
    }

    #[test]
    fn pg_data_row_get_i16() {
        let data = make_data_row(&[Some(&7i16.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i16(0), Some(7));
    }

    #[test]
    fn pg_data_row_get_f32() {
        let data = make_data_row(&[Some(&2.5f32.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert!((row.get_f32(0).unwrap() - 2.5).abs() < 1e-6);
    }

    #[test]
    fn pg_data_row_get_raw_null() {
        let data = make_data_row(&[None]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_raw(0), None);
    }

    #[test]
    fn pg_data_row_get_raw_data() {
        let data = make_data_row(&[Some(&[1, 2, 3])]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_raw(0), Some(&[1u8, 2, 3][..]));
    }

    #[test]
    fn pg_data_row_stack_alloc_16_columns() {
        // SmallVec<16> should not heap-allocate for <= 16 columns
        let cols: Vec<Option<&[u8]>> = (0..16).map(|_| Some(&[0u8][..])).collect();
        let data = make_data_row(&cols);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.column_count(), 16);
        // All columns should be accessible
        for i in 0..16 {
            assert_eq!(row.get_raw(i), Some(&[0u8][..]));
        }
    }

    // --- Inline sequential decode tests (validates the raw-bytes pattern) ---

    /// Validate inline sequential decode of a 5-column DataRow
    /// (i32, str, str, bool, f64) — the same pattern the generated code uses.
    #[test]
    fn inline_sequential_decode_five_columns() {
        let data = make_data_row(&[
            Some(&42i32.to_be_bytes()),
            Some(b"alice"),
            Some(b"alice@example.com"),
            Some(&[1u8]),
            Some(&3.14f64.to_be_bytes()),
        ]);

        // Simulate generated inline decode
        let mut pos: usize = 2; // skip i16 num_cols

        // Column 0: i32
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        assert_eq!(len, 4);
        let id = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += len as usize;
        assert_eq!(id, 42);

        // Column 1: str
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        assert_eq!(len, 5);
        let name = std::str::from_utf8(&data[pos..pos + len as usize]).unwrap();
        pos += len as usize;
        assert_eq!(name, "alice");

        // Column 2: str
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let email = std::str::from_utf8(&data[pos..pos + len as usize]).unwrap();
        pos += len as usize;
        assert_eq!(email, "alice@example.com");

        // Column 3: bool
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        assert_eq!(len, 1);
        let active = data[pos] != 0;
        pos += len as usize;
        assert!(active);

        // Column 4: f64
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        assert_eq!(len, 8);
        let score = f64::from_be_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]);
        pos += len as usize;
        assert!((score - 3.14).abs() < 1e-10);
        assert_eq!(pos, data.len());
    }

    /// Validate inline decode with NULL columns.
    #[test]
    fn inline_sequential_decode_with_nulls() {
        let data = make_data_row(&[
            Some(&42i32.to_be_bytes()),
            None, // NULL name
            Some(b"text"),
        ]);

        let mut pos: usize = 2;

        // Column 0: i32 NOT NULL
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let id = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += len as usize;
        assert_eq!(id, 42);

        // Column 1: str NULLABLE -> None
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let name: Option<&str> = if len < 0 {
            None
        } else {
            let s = std::str::from_utf8(&data[pos..pos + len as usize]).unwrap();
            pos += len as usize;
            Some(s)
        };
        assert!(name.is_none());

        // Column 2: str NOT NULL
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let txt = std::str::from_utf8(&data[pos..pos + len as usize]).unwrap();
        pos += len as usize;
        assert_eq!(txt, "text");
        assert_eq!(pos, data.len());
    }

    /// Validate inline decode with all supported scalar types.
    #[test]
    fn inline_sequential_decode_all_scalar_types() {
        let data = make_data_row(&[
            Some(&[1u8]),                  // bool
            Some(&7i16.to_be_bytes()),     // i16
            Some(&42i32.to_be_bytes()),    // i32
            Some(&12345i64.to_be_bytes()), // i64
            Some(&2.5f32.to_be_bytes()),   // f32
            Some(&3.14f64.to_be_bytes()),  // f64
        ]);

        let mut pos: usize = 2;

        // bool
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let v_bool = data[pos] != 0;
        pos += len as usize;
        assert!(v_bool);

        // i16
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let v_i16 = i16::from_be_bytes([data[pos], data[pos + 1]]);
        pos += len as usize;
        assert_eq!(v_i16, 7);

        // i32
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let v_i32 = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += len as usize;
        assert_eq!(v_i32, 42);

        // i64
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let v_i64 = i64::from_be_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]);
        pos += len as usize;
        assert_eq!(v_i64, 12345);

        // f32
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let v_f32 = f32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += len as usize;
        assert!((v_f32 - 2.5).abs() < 1e-6);

        // f64
        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let v_f64 = f64::from_be_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]);
        pos += len as usize;
        assert!((v_f64 - 3.14).abs() < 1e-10);
        assert_eq!(pos, data.len());
    }

    /// Validate PgDataRow::new is public (callable from external code).
    #[test]
    fn pg_data_row_new_is_public() {
        let data = make_data_row(&[Some(&42i32.to_be_bytes())]);
        // This compiles because PgDataRow::new is pub.
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i32(0), Some(42));
    }

    /// Inline decode produces identical results to PgDataRow for mixed data.
    #[test]
    fn inline_decode_matches_pgdatarow() {
        let data = make_data_row(&[
            Some(&99i32.to_be_bytes()),
            Some(b"hello world"),
            None,
            Some(&[0u8]),
            Some(&1.23f64.to_be_bytes()),
        ]);

        // PgDataRow results
        let row = PgDataRow::new(&data).unwrap();
        let dr_i32 = row.get_i32(0);
        let dr_str = row.get_str(1);
        let dr_null = row.get_str(2);
        let dr_bool = row.get_bool(3);
        let dr_f64 = row.get_f64(4);

        // Inline results
        let mut pos: usize = 2;

        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let in_i32 = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += len as usize;

        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let in_str = std::str::from_utf8(&data[pos..pos + len as usize]).unwrap();
        pos += len as usize;

        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let in_null: Option<&str> = if len < 0 { None } else { unreachable!() };

        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let in_bool = data[pos] != 0;
        pos += len as usize;

        let len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;
        let in_f64 = f64::from_be_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]);
        pos += len as usize;

        // Both paths must produce identical results
        assert_eq!(dr_i32, Some(in_i32));
        assert_eq!(dr_str, Some(in_str));
        assert_eq!(dr_null, in_null);
        assert_eq!(dr_bool, Some(in_bool));
        assert!((dr_f64.unwrap() - in_f64).abs() < 1e-15);
        assert_eq!(pos, data.len());
    }

    // --- Unix domain socket (UDS) tests ---

    #[test]
    fn config_host_is_uds_absolute_path() {
        let cfg = Config {
            host: "/tmp".into(),
            port: 5432,
            user: "user".into(),
            password: "".into(),
            database: "db".into(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(cfg.host_is_uds());
        assert_eq!(cfg.uds_path(), "/tmp/.s.PGSQL.5432");
    }

    #[test]
    fn config_host_is_uds_var_run() {
        let cfg = Config {
            host: "/var/run/postgresql".into(),
            port: 5433,
            user: "user".into(),
            password: "".into(),
            database: "db".into(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(cfg.host_is_uds());
        assert_eq!(cfg.uds_path(), "/var/run/postgresql/.s.PGSQL.5433");
    }

    #[test]
    fn config_host_is_not_uds_for_hostname() {
        let cfg = Config {
            host: "localhost".into(),
            port: 5432,
            user: "user".into(),
            password: "".into(),
            database: "db".into(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(!cfg.host_is_uds());
    }

    #[test]
    fn config_host_is_not_uds_for_ip() {
        let cfg = Config {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "user".into(),
            password: "".into(),
            database: "db".into(),
            ssl: SslMode::Disable,
            statement_timeout_secs: 30,
        };
        assert!(!cfg.host_is_uds());
    }

    #[test]
    fn config_parse_uds_host_query_param() {
        let cfg = Config::from_url("postgres://user@localhost/mydb?host=/tmp").unwrap();
        assert_eq!(cfg.host, "/tmp");
        assert!(cfg.host_is_uds());
        assert_eq!(cfg.uds_path(), "/tmp/.s.PGSQL.5432");
        assert_eq!(cfg.database, "mydb");
        assert_eq!(cfg.user, "user");
    }

    #[test]
    fn config_parse_uds_host_query_param_custom_port() {
        let cfg = Config::from_url("postgres://user@localhost:5433/mydb?host=/var/run/postgresql")
            .unwrap();
        assert_eq!(cfg.host, "/var/run/postgresql");
        assert_eq!(cfg.port, 5433);
        assert_eq!(cfg.uds_path(), "/var/run/postgresql/.s.PGSQL.5433");
    }

    #[test]
    fn config_parse_uds_host_with_other_params() {
        let cfg = Config::from_url(
            "postgres://user@localhost/db?host=/tmp&sslmode=disable&statement_timeout=60",
        )
        .unwrap();
        assert_eq!(cfg.host, "/tmp");
        assert!(cfg.host_is_uds());
        assert_eq!(cfg.ssl, SslMode::Disable);
        assert_eq!(cfg.statement_timeout_secs, 60);
    }

    #[test]
    fn config_parse_uds_host_percent_encoded() {
        // %2F = '/'
        let cfg = Config::from_url("postgres://user@localhost/db?host=%2Ftmp").unwrap();
        assert_eq!(cfg.host, "/tmp");
        assert!(cfg.host_is_uds());
    }

    #[test]
    fn config_parse_tcp_host_not_overridden_without_param() {
        // No ?host= param: hostname from URL is used (TCP)
        let cfg = Config::from_url("postgres://user@myserver/db").unwrap();
        assert_eq!(cfg.host, "myserver");
        assert!(!cfg.host_is_uds());
    }

    #[test]
    fn config_parse_uds_host_overrides_url_hostname() {
        // ?host= overrides even an explicit hostname
        let cfg = Config::from_url("postgres://user@db.example.com/mydb?host=/var/run/postgresql")
            .unwrap();
        assert_eq!(cfg.host, "/var/run/postgresql");
        assert!(cfg.host_is_uds());
    }

    #[test]
    fn config_parse_uds_empty_url_host() {
        // postgres:///dbname?host=/tmp — empty hostname before /, host from param
        let cfg = Config::from_url("postgres://user@/mydb?host=/tmp").unwrap();
        assert_eq!(cfg.host, "/tmp");
        assert!(cfg.host_is_uds());
        assert_eq!(cfg.database, "mydb");
    }

    // ===============================================================
    // PgDataRow — comprehensive tests
    // ===============================================================

    #[test]
    fn pg_data_row_all_null_columns() {
        let data = make_data_row(&[None, None, None, None, None]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.column_count(), 5);
        for i in 0..5 {
            assert!(row.is_null(i), "column {i} should be null");
            assert_eq!(row.get_raw(i), None);
            assert_eq!(row.get_i32(i), None);
            assert_eq!(row.get_i64(i), None);
            assert_eq!(row.get_str(i), None);
            assert_eq!(row.get_bool(i), None);
            assert_eq!(row.get_f64(i), None);
        }
    }

    #[test]
    fn pg_data_row_very_long_text() {
        let long_text = "x".repeat(2048);
        let data = make_data_row(&[Some(long_text.as_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_str(0), Some(long_text.as_str()));
    }

    #[test]
    fn pg_data_row_empty_text() {
        let data = make_data_row(&[Some(b"")]);
        let row = PgDataRow::new(&data).unwrap();
        assert!(!row.is_null(0));
        assert_eq!(row.get_str(0), Some(""));
        assert_eq!(row.get_bytes(0), Some(&[][..]));
    }

    #[test]
    fn pg_data_row_20_columns_exceeds_inline() {
        let col_data: Vec<[u8; 4]> = (0..20).map(|i: i32| i.to_be_bytes()).collect();
        let cols: Vec<Option<&[u8]>> = col_data.iter().map(|b| Some(b.as_slice())).collect();
        let data = make_data_row(&cols);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.column_count(), 20);
        for i in 0..20 {
            assert_eq!(row.get_i32(i), Some(i as i32));
        }
    }

    #[test]
    fn pg_data_row_is_null_each_position() {
        // 3 columns: data, null, data
        let data = make_data_row(&[Some(&1i32.to_be_bytes()), None, Some(&3i32.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert!(!row.is_null(0));
        assert!(row.is_null(1));
        assert!(!row.is_null(2));
    }

    #[test]
    fn pg_data_row_negative_column_count() {
        let data = (-1i16).to_be_bytes();
        assert!(PgDataRow::new(&data).is_err());
    }

    #[test]
    fn pg_data_row_get_str_invalid_utf8() {
        let invalid_utf8 = &[0xFF, 0xFE, 0x80];
        let data = make_data_row(&[Some(invalid_utf8)]);
        let row = PgDataRow::new(&data).unwrap();
        // get_str returns None for invalid UTF-8, but get_bytes returns the raw data
        assert_eq!(row.get_str(0), None);
        assert_eq!(row.get_bytes(0), Some(&[0xFF, 0xFE, 0x80][..]));
    }

    #[test]
    fn pg_data_row_get_i32_wrong_length() {
        // i32 needs exactly 4 bytes; give it 2
        let data = make_data_row(&[Some(&7i16.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i32(0), None); // 2 bytes != 4 bytes
        assert_eq!(row.get_i16(0), Some(7)); // but i16 works
    }

    #[test]
    fn pg_data_row_get_i64_wrong_length() {
        // i64 needs 8 bytes; give it 4
        let data = make_data_row(&[Some(&42i32.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_i64(0), None);
    }

    #[test]
    fn pg_data_row_get_f64_wrong_length() {
        let data = make_data_row(&[Some(&2.5f32.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_f64(0), None); // 4 bytes != 8 bytes
    }

    #[test]
    fn pg_data_row_get_f32_wrong_length() {
        let data = make_data_row(&[Some(&3.14f64.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_f32(0), None); // 8 bytes != 4 bytes
    }

    #[test]
    fn pg_data_row_get_bool_wrong_length() {
        // bool needs 1 byte; give it 4
        let data = make_data_row(&[Some(&42i32.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_bool(0), None);
    }

    #[test]
    fn pg_data_row_unicode_text() {
        let texts = [
            "\u{1F600}\u{1F4A9}\u{1F680}", // emoji
            "\u{4e16}\u{754c}",            // CJK
            "\u{0645}\u{0631}\u{062D}",    // Arabic
            "\u{1F468}\u{200D}\u{1F469}",  // ZWJ
        ];
        for text in &texts {
            let data = make_data_row(&[Some(text.as_bytes())]);
            let row = PgDataRow::new(&data).unwrap();
            assert_eq!(row.get_str(0), Some(*text));
        }
    }

    #[test]
    fn pg_data_row_i32_boundary_values() {
        for &val in &[i32::MIN, -1, 0, 1, i32::MAX] {
            let data = make_data_row(&[Some(&val.to_be_bytes())]);
            let row = PgDataRow::new(&data).unwrap();
            assert_eq!(row.get_i32(0), Some(val), "failed for {val}");
        }
    }

    #[test]
    fn pg_data_row_i64_boundary_values() {
        for &val in &[i64::MIN, -1, 0, 1, i64::MAX] {
            let data = make_data_row(&[Some(&val.to_be_bytes())]);
            let row = PgDataRow::new(&data).unwrap();
            assert_eq!(row.get_i64(0), Some(val), "failed for {val}");
        }
    }

    #[test]
    fn pg_data_row_f64_special_values() {
        let data = make_data_row(&[Some(&f64::INFINITY.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_f64(0), Some(f64::INFINITY));

        let data = make_data_row(&[Some(&f64::NEG_INFINITY.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_f64(0), Some(f64::NEG_INFINITY));

        let data = make_data_row(&[Some(&f64::NAN.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert!(row.get_f64(0).unwrap().is_nan());
    }

    #[test]
    fn pg_data_row_f32_special_values() {
        let data = make_data_row(&[Some(&f32::INFINITY.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert_eq!(row.get_f32(0), Some(f32::INFINITY));

        let data = make_data_row(&[Some(&f32::NAN.to_be_bytes())]);
        let row = PgDataRow::new(&data).unwrap();
        assert!(row.get_f32(0).unwrap().is_nan());
    }

    #[test]
    fn pg_data_row_i16_boundary_values() {
        for &val in &[i16::MIN, -1, 0, 1, i16::MAX] {
            let data = make_data_row(&[Some(&val.to_be_bytes())]);
            let row = PgDataRow::new(&data).unwrap();
            assert_eq!(row.get_i16(0), Some(val));
        }
    }

    // ===============================================================
    // DataRow flat parsing — comprehensive edge cases
    // ===============================================================

    #[test]
    fn data_row_flat_all_null() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let mut data = Vec::new();
        data.extend_from_slice(&4i16.to_be_bytes());
        for _ in 0..4 {
            data.extend_from_slice(&(-1i32).to_be_bytes());
        }
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out.len(), 4);
        for (_, len) in &out {
            assert_eq!(*len, -1);
        }
    }

    #[test]
    fn data_row_flat_long_text() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let long = vec![b'A'; 1024];
        let mut data = Vec::new();
        data.extend_from_slice(&1i16.to_be_bytes());
        data.extend_from_slice(&(long.len() as i32).to_be_bytes());
        data.extend_from_slice(&long);
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out[0].1, 1024);
        let stored = arena.get(out[0].0, 1024);
        assert!(stored.iter().all(|&b| b == b'A'));
    }

    #[test]
    fn data_row_flat_empty_text() {
        let mut arena = Arena::new();
        let mut out = Vec::new();
        let mut data = Vec::new();
        data.extend_from_slice(&1i16.to_be_bytes());
        data.extend_from_slice(&0i32.to_be_bytes()); // 0-length, not null
        parse_data_row_flat(&data, &mut arena, &mut out).unwrap();
        assert_eq!(out[0].1, 0);
    }

    // ===============================================================
    // QueryResult edge cases
    // ===============================================================

    #[test]
    fn query_result_from_parts() {
        let result = QueryResult::from_parts(vec![(0, 4), (0, -1)], 2, Arc::from(Vec::new()), 5);
        assert_eq!(result.len(), 1);
        assert_eq!(result.num_cols, 2);
        assert_eq!(result.affected_rows, 5);
    }

    #[test]
    fn query_result_affected_rows() {
        let result = QueryResult {
            all_col_offsets: vec![],
            num_cols: 0,
            columns: Arc::from(Vec::new()),
            affected_rows: 42,
        };
        assert_eq!(result.affected_rows, 42);
        assert!(result.is_empty());
    }

    // ===============================================================
    // DriverError edge cases
    // ===============================================================

    #[test]
    fn driver_error_server_with_hint() {
        let e = DriverError::Server {
            code: "42601".into(),
            message: "syntax error".into(),
            detail: None,
            hint: Some("check your SQL".into()),
            position: Some(10),
        };
        let s = e.to_string();
        assert!(s.contains("HINT: check your SQL"));
        assert!(s.contains("(at position 10)"));
    }

    #[test]
    fn driver_error_server_with_all_fields() {
        let e = DriverError::Server {
            code: "23505".into(),
            message: "unique violation".into(),
            detail: Some("Key (id)=(1) already exists.".into()),
            hint: Some("change the id".into()),
            position: Some(1),
        };
        let s = e.to_string();
        assert!(s.contains("23505"));
        assert!(s.contains("unique violation"));
        assert!(s.contains("Key (id)=(1) already exists."));
        assert!(s.contains("change the id"));
        assert!(s.contains("(at position 1)"));
    }

    // ===============================================================
    // Config edge cases
    // ===============================================================

    #[test]
    fn config_statement_timeout_default() {
        let cfg = Config::from_url("postgres://user:pass@localhost/db").unwrap();
        assert_eq!(cfg.statement_timeout_secs, 30);
    }

    #[test]
    fn config_statement_timeout_custom() {
        let cfg =
            Config::from_url("postgres://user:pass@localhost/db?statement_timeout=120").unwrap();
        assert_eq!(cfg.statement_timeout_secs, 120);
    }

    #[test]
    fn config_statement_timeout_zero() {
        let cfg =
            Config::from_url("postgres://user:pass@localhost/db?statement_timeout=0").unwrap();
        assert_eq!(cfg.statement_timeout_secs, 0);
    }

    #[test]
    fn config_statement_timeout_invalid_falls_back() {
        let cfg =
            Config::from_url("postgres://user:pass@localhost/db?statement_timeout=notanumber")
                .unwrap();
        assert_eq!(cfg.statement_timeout_secs, 30); // fallback
    }

    #[test]
    fn config_uds_path_format() {
        let cfg = Config::from_url("postgres://user@localhost/db?host=/tmp").unwrap();
        assert_eq!(cfg.uds_path(), "/tmp/.s.PGSQL.5432");
    }

    #[test]
    fn config_uds_path_custom_port() {
        let cfg = Config::from_url("postgres://user@localhost:5433/db?host=/tmp").unwrap();
        assert_eq!(cfg.uds_path(), "/tmp/.s.PGSQL.5433");
    }

    // ===============================================================
    // url_decode edge cases
    // ===============================================================

    #[test]
    fn url_decode_empty_string() {
        assert_eq!(url_decode("").unwrap(), "");
    }

    #[test]
    fn url_decode_no_encoding() {
        assert_eq!(url_decode("hello").unwrap(), "hello");
    }

    #[test]
    fn url_decode_all_ascii_hex() {
        // Uppercase hex
        assert_eq!(url_decode("%2F").unwrap(), "/");
        assert_eq!(url_decode("%2f").unwrap(), "/");
    }

    // ===============================================================
    // hash_sql edge cases
    // ===============================================================

    #[test]
    fn hash_sql_empty() {
        let _h = hash_sql(""); // should not panic
    }

    #[test]
    fn hash_sql_whitespace_only() {
        let h = hash_sql("   ");
        assert_ne!(h, hash_sql(""));
    }

    #[test]
    fn hash_sql_very_long() {
        let long_sql = "SELECT ".to_string() + &"x".repeat(10_000);
        let h = hash_sql(&long_sql);
        assert_eq!(h, hash_sql(&long_sql));
    }

    #[test]
    fn hash_sql_unicode() {
        let h = hash_sql("SELECT '\u{1F600}'");
        assert_ne!(h, hash_sql("SELECT 'x'"));
    }
}
