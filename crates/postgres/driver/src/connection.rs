use bsql_postgres_proto::{
    ActivePhase, FeedEvent, PgProtocol, WriteBuf,
    reply_id::PingKind,
};


/// Handle to a server-side prepared statement.
///
/// Created via [`Connection::prepare`]. Reuse across multiple
/// `query_prepared` / `execute_prepared` calls to avoid re-parsing.
/// Holds a cached `RowDesc` (from DescribeStatement) so the proto
/// can distinguish SELECT (returns rows) from DML (no rows).
#[derive(Debug)]
pub struct PreparedStatement {
    stmt_name: bsql_postgres_proto::StmtName,
    row_desc: Option<bsql_postgres_proto::decode::RowDesc>,
    column_names: Vec<String>,
}

impl PreparedStatement {

    /// Whether this statement returns rows (SELECT / RETURNING).
    pub fn returns_rows(&self) -> bool {
        self.row_desc.is_some()
    }
}

/// An async notification received via LISTEN/NOTIFY.
#[derive(Debug, Clone)]
pub struct Notification {
    /// Channel name.
    pub channel: String,
    /// Payload (may be empty).
    pub payload: String,
    /// PID of the backend that sent the notification.
    pub pid: i32,
}

/// Result of a query — rows + command tag + column count.
#[derive(Debug)]
#[must_use]
pub struct QueryResult {
    /// Rows. Each row provides typed column access via `Row::get`.
    pub rows: Vec<Row>,
    /// Command tag (e.g., "SELECT 3", "INSERT 0 1").
    pub command_tag: String,
    /// Number of columns in the result set.
    pub column_count: usize,
    /// Column names from RowDescription (empty for DML).
    pub column_names: Vec<String>,
}

/// A single result row. Column values are raw bytes decoded on access.
#[derive(Debug, Clone)]
#[must_use]
pub struct Row {
    columns: Vec<Option<Vec<u8>>>,
}

impl Row {
    /// Get column `idx` as `&str`. Returns `None` for NULL or out-of-range.
    pub fn get_str(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx)?.as_deref().and_then(|b| core::str::from_utf8(b).ok())
    }

    /// Get column `idx` as `i32`. Returns `None` for NULL, out-of-range, or parse error.
    pub fn get_i32(&self, idx: usize) -> Option<i32> {
        self.get_str(idx)?.parse().ok()
    }

    /// Get column `idx` as `i64`. Returns `None` for NULL, out-of-range, or parse error.
    pub fn get_i64(&self, idx: usize) -> Option<i64> {
        self.get_str(idx)?.parse().ok()
    }

    /// Get column `idx` as `f64`.
    pub fn get_f64(&self, idx: usize) -> Option<f64> {
        self.get_str(idx)?.parse().ok()
    }

    /// Get column `idx` as `bool`. PG text: "t"=true, "f"=false.
    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        match self.get_str(idx)? {
            "t" => Some(true),
            "f" => Some(false),
            _ => None,
        }
    }

    /// Get raw bytes for column `idx`. `None` = SQL NULL.
    pub fn get_raw(&self, idx: usize) -> Option<&[u8]> {
        self.columns.get(idx)?.as_deref()
    }

    /// Is column `idx` NULL?
    pub fn is_null(&self, idx: usize) -> bool {
        matches!(self.columns.get(idx), Some(None))
    }

    /// Number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether the row has zero columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Get column value by name. Requires `column_names` from QueryResult.
    pub fn get_by_name<'a>(&'a self, name: &str, column_names: &[String]) -> Option<&'a [u8]> {
        let idx = column_names.iter().position(|n| n == name)?;
        self.get_raw(idx)
    }

    /// Get column by index with generic FromText conversion.
    pub fn get<T: FromText>(&self, idx: usize) -> Option<T> {
        T::from_text(self.get_str(idx)?)
    }
}

/// Trait for converting PG text-format values to Rust types.
pub trait FromText: Sized {
    /// Parse from PG text representation.
    fn from_text(s: &str) -> Option<Self>;
}

impl FromText for i16 {
    fn from_text(s: &str) -> Option<Self> { s.parse().ok() }
}
impl FromText for i32 {
    fn from_text(s: &str) -> Option<Self> { s.parse().ok() }
}
impl FromText for i64 {
    fn from_text(s: &str) -> Option<Self> { s.parse().ok() }
}
impl FromText for f32 {
    fn from_text(s: &str) -> Option<Self> { s.parse().ok() }
}
impl FromText for f64 {
    fn from_text(s: &str) -> Option<Self> { s.parse().ok() }
}
impl FromText for bool {
    fn from_text(s: &str) -> Option<Self> {
        match s { "t" => Some(true), "f" => Some(false), _ => None }
    }
}
impl FromText for String {
    fn from_text(s: &str) -> Option<Self> { Some(s.to_string()) }
}
use tokio::net::TcpStream;

use crate::config::{ConnectConfig, SslMode};
use crate::error::DriverError;

enum Stream {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl Stream {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        match self {
            Self::Plain(s) => s.write_all(buf).await,
            Self::Tls(s) => s.write_all(buf).await,
        }
    }
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use tokio::io::AsyncReadExt;
        match self {
            Self::Plain(s) => s.read(buf).await,
            Self::Tls(s) => s.read(buf).await,
        }
    }
    async fn shutdown(&mut self) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        match self {
            Self::Plain(s) => s.shutdown().await,
            Self::Tls(s) => s.shutdown().await,
        }
    }
}

/// Async PostgreSQL connection.
///
/// Wraps `PgProtocol<ActivePhase>` with a TCP (or TLS) socket and
/// drives the sans-IO state machine via an event-pump loop.
pub struct Connection {
    proto: PgProtocol<ActivePhase>,
    stream: Stream,
    wb: WriteBuf,
    buf: Vec<u8>,
    stmt_counter: u32,
}

impl Connection {
    fn classify_error(&self, cause: bsql_postgres_proto::ProtocolError) -> DriverError {
        use crate::error::DbError;
        if let bsql_postgres_proto::ProtocolError::ServerErrorResponse {
            severity, code, details_ref,
        } = cause
        {
            let sev = severity
                .map(|s| s.as_str().to_string())
                .unwrap_or_else(|| "ERROR".to_string());
            let sqlstate = code.as_str().trim().to_string();
            let (msg, det, hnt) = match self.proto.get_server_error(details_ref) {
                Ok(bsql_postgres_proto::ErrorPayload::ServerError { message, detail, hint }) => {
                    let m = message.as_str().to_string();
                    let d = {
                        let s = detail.as_str();
                        if s.is_empty() { None } else { Some(s.to_string()) }
                    };
                    let h = {
                        let s = hint.as_str();
                        if s.is_empty() { None } else { Some(s.to_string()) }
                    };
                    (m, d, h)
                }
                _ => (String::new(), None, None),
            };
            return DriverError::Db(DbError {
                code: sqlstate,
                severity: sev,
                message: msg,
                detail: det,
                hint: hnt,
            });
        }
        DriverError::Protocol(cause)
    }

    /// Connect to PostgreSQL with Trust auth (no password).
    ///
    /// TCP → StartupMessage → auth handshake → Active.
    pub async fn connect(config: &ConnectConfig) -> Result<Self, DriverError> {
        let addr = format!("{}:{}", config.host, config.port);
        let timeout = std::time::Duration::from_secs(config.connect_timeout_secs);
        let tcp = tokio::time::timeout(timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "connection timed out",
            )))?
            ?;

        let mut stream = Self::negotiate_ssl(tcp, config).await?;

        let user = bsql_postgres_proto::Ident::try_from_str(&config.user)
            .map_err(|_| DriverError::Config("invalid user name"))?;
        let database = match &config.database {
            Some(d) => Some(
                bsql_postgres_proto::DatabaseName::try_from_str(d)
                    .map_err(|_| DriverError::Config("invalid database name"))?,
            ),
            None => None,
        };

        let mut proto = PgProtocol::new();
        let mut wb = WriteBuf::new();

        let reply = proto.next_reply_id::<bsql_postgres_proto::reply_id::StartupKind>();

        let credentials = match config.password_str() {
            Some(pw) => {
                let password = bsql_postgres_proto::Password::try_from_str(pw)
                    .map_err(|_| DriverError::Config("invalid password"))?;
                bsql_postgres_proto::Credentials::ScramPassword(
                    bsql_postgres_proto::sensitive::Sensitive::new(password),
                )
            }
            None => bsql_postgres_proto::password::Credentials::Trust,
        };

        let (actions, mut connecting) = proto
            .push_startup(
                user,
                database,
                None,
                credentials,
                reply,
                &mut wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;

        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                stream.write_all(bytes).await?;
            }
        }

        let mut buf = vec![0u8; 4096];

        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                return Err(DriverError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "server closed connection during handshake",
                )));
            }
            if connecting.feed_inbound(&buf[..n]).is_err() {
                return Err(DriverError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "read buffer full during handshake",
                )));
            }

            {
                let actions = connecting.feed_bytes(&[], &mut wb);
                for action in actions.as_slice() {
                    if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                        stream.write_all(bytes).await?;
                    }
                }
            }

            match connecting.into_active() {
                Ok(active) => {
                    return Ok(Self {
                        proto: active,
                        stream,
                        wb,
                        buf,
                        stmt_counter: 0,
                    });
                }
                Err(bsql_postgres_proto::IntoActiveError::StillConnecting(c)) => {
                    connecting = c;
                }
                Err(bsql_postgres_proto::IntoActiveError::Closed(_)) => {
                    return Err(DriverError::NotReady);
                }
            }
        }
    }

    async fn negotiate_ssl(
        mut tcp: TcpStream,
        config: &ConnectConfig,
    ) -> Result<Stream, DriverError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        if config.ssl_mode == SslMode::Disable {
            return Ok(Stream::Plain(tcp));
        }

        // SSL probe via protocol typestate
        let proto = bsql_postgres_proto::PgProtocol::new();
        let (ssl_bytes, ssl_proto) = proto.push_ssl_request();
        tcp.write_all(ssl_bytes).await?;

        let mut response = [0u8; 1];
        tcp.read_exact(&mut response).await?;

        let classified = ssl_proto.classify_ssl_response(response[0]);
        match classified {
            bsql_postgres_proto::SslClassified::Accepted(_) => {
                let mut root_store = rustls::RootCertStore::empty();
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

                let tls_config = rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth();

                let connector = tokio_rustls::TlsConnector::from(
                    std::sync::Arc::new(tls_config),
                );
                let server_name = rustls::pki_types::ServerName::try_from(
                    config.host.as_str(),
                )
                .map_err(|_| DriverError::Config("invalid server name for TLS"))?
                .to_owned();

                let tls_stream = connector.connect(server_name, tcp).await
                    .map_err(|e| DriverError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        format!("TLS handshake failed: {e}"),
                    )))?;

                Ok(Stream::Tls(tls_stream))
            }
            bsql_postgres_proto::SslClassified::Refused(_) => {
                if config.ssl_mode == SslMode::Require {
                    return Err(DriverError::SslRefused);
                }
                Ok(Stream::Plain(tcp))
            }
            bsql_postgres_proto::SslClassified::ErrorIncoming(_) => {
                Err(DriverError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "server sent ErrorResponse during SSL probe",
                )))
            }
            bsql_postgres_proto::SslClassified::InvalidByte { byte } => {
                Err(DriverError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid SSL response byte: 0x{byte:02x}"),
                )))
            }
            _ => Err(DriverError::Io(std::io::Error::other(
                "unexpected SSL classification",
            ))),
        }
    }

    /// Send a Ping and wait for Pong. Verifies the connection is alive.
    pub async fn ping(&mut self) -> Result<(), DriverError> {
        let reply = self.proto.next_reply_id::<PingKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard
            .push_command(
                bsql_postgres_proto::push_command::Ping { reply },
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;

        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.pump(None).await
    }

    /// Execute a Simple Query. Returns the command tag.
    ///
    /// Handles DML (CREATE/INSERT/UPDATE/DELETE) and SELECT (rows
    /// are silently drained). Command tag captures the result.
    pub async fn simple_query(&mut self, sql: &str) -> Result<String, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard
            .push_command(
                bsql_postgres_proto::push_command::SimpleQuery { sql, reply },
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.wb.clear();
        let mut command_tag = String::new();
        self.pump(None).await?;
        if let Some(tag) = self.proto.current_command_tag() {
            use core::fmt::Write;
            let _ = write!(command_tag, "{}", tag);
        }
        Ok(command_tag)
    }

    /// Execute a DML statement. Returns the number of affected rows.
    ///
    /// For INSERT/UPDATE/DELETE the tag is "INSERT 0 N" / "UPDATE N" /
    /// "DELETE N". This method parses N from the tag.
    /// For DDL (CREATE/DROP/ALTER) returns 0.
    pub async fn execute(&mut self, sql: &str) -> Result<u64, DriverError> {
        let tag = self.simple_query(sql).await?;
        let count = tag.rsplit(' ')
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        Ok(count)
    }

    /// Execute a query expecting zero or one row. Returns `None` if empty.
    pub async fn query_opt(&mut self, sql: &str) -> Result<Option<Row>, DriverError> {
        let result = self.query(sql).await?;
        let mut rows = result.rows;
        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rows.swap_remove(0)))
        }
    }

    /// Execute a query expecting exactly one row. Returns the row.
    pub async fn query_one(&mut self, sql: &str) -> Result<Row, DriverError> {
        let result = self.query(sql).await?;
        if result.rows.is_empty() {
            return Err(DriverError::NoRows);
        }
        let mut rows = result.rows;
        Ok(rows.swap_remove(0))
    }

    /// Execute a query and return rows as raw byte vectors.
    ///
    /// Each row is `Vec<Option<Vec<u8>>>` — one entry per column.
    /// `None` = SQL NULL. Caller decodes via `FromPgText` / `FromPgBinary`.
    pub async fn query(
        &mut self,
        sql: &str,
    ) -> Result<QueryResult, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard
            .push_command(
                bsql_postgres_proto::push_command::SimpleQuery { sql, reply },
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.wb.clear();

        let mut rows: Vec<Row> = Vec::new();
        let mut command_tag = String::new();

        self.pump(Some(&mut rows)).await?;

        if let Some(tag) = self.proto.current_command_tag() {
            use core::fmt::Write;
            let _ = write!(command_tag, "{}", tag);
        }

        let column_names = self.proto.current_column_names()
            .map(|s| s.to_vec())
            .unwrap_or_default();
        let column_count = rows.first().map_or(0, |r| r.len());
        Ok(QueryResult { rows, command_tag, column_count, column_names })
    }

    /// Execute a parameterized query and return rows.
    ///
    /// One-shot: Parse + Describe + BindExecute. For repeated queries,
    /// use `prepare` + `query_prepared` to avoid re-parsing.
    pub async fn query_params<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        let stmt = self.prepare(sql).await?;
        let result = self.query_prepared(&stmt, params).await?;
        let _ = self.close_statement(stmt).await;
        Ok(result)
    }

    /// Parameterized query returning exactly one row.
    pub async fn query_params_one<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<Row, DriverError> {
        let result = self.query_params(sql, params).await?;
        if result.rows.is_empty() {
            return Err(DriverError::NoRows);
        }
        let mut rows = result.rows;
        Ok(rows.swap_remove(0))
    }

    /// Parameterized query returning zero or one row.
    pub async fn query_params_opt<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<Option<Row>, DriverError> {
        let result = self.query_params(sql, params).await?;
        let mut rows = result.rows;
        if rows.is_empty() { Ok(None) } else { Ok(Some(rows.swap_remove(0))) }
    }

    /// Begin an explicit transaction.
    pub async fn begin(&mut self) -> Result<(), DriverError> {
        self.simple_query("BEGIN").await?;
        Ok(())
    }

    /// Commit the current transaction.
    pub async fn commit(&mut self) -> Result<(), DriverError> {
        self.simple_query("COMMIT").await?;
        Ok(())
    }

    /// Roll back the current transaction.
    pub async fn rollback(&mut self) -> Result<(), DriverError> {
        self.simple_query("ROLLBACK").await?;
        Ok(())
    }

    /// Subscribe to a LISTEN channel.
    pub async fn listen(&mut self, channel: &str) -> Result<(), DriverError> {
        self.simple_query(&format!("LISTEN {channel}")).await?;
        Ok(())
    }

    /// Unsubscribe from a LISTEN channel.
    pub async fn unlisten(&mut self, channel: &str) -> Result<(), DriverError> {
        self.simple_query(&format!("UNLISTEN {channel}")).await?;
        Ok(())
    }

    /// Wait for the next async notification. Reads from the socket
    /// until a NotificationResponse arrives or the timeout expires.
    pub async fn recv_notification(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<Option<Notification>, DriverError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            match tokio::time::timeout(remaining, self.stream.read(&mut self.buf)).await {
                Ok(Ok(0)) => return Err(DriverError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "server closed connection",
                ))),
                Ok(Ok(n)) => {
                    self.proto.feed_inbound(&self.buf[..n]).map_err(|_| {
                        DriverError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "read buffer full",
                        ))
                    })?;
                    let event = self.proto.advance_one_frame(&mut self.wb);
                    if let FeedEvent::Notify { notif_ref, pid } = event {
                        if let Ok(payload) = self.proto.get_notification(notif_ref) {
                            return Ok(Some(Notification {
                                channel: payload.channel.as_str().to_string(),
                                payload: String::from_utf8_lossy(&payload.payload).into_owned(),
                                pid,
                            }));
                        }
                    }
                }
                Ok(Err(e)) => return Err(DriverError::Io(e)),
                Err(_) => return Ok(None),
            }
        }
    }

    /// Execute a parameterized DML. Returns affected row count.
    ///
    /// Uses Extended Query (Parse + Bind + Execute). Prevents SQL injection.
    pub async fn execute_params<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self,
        sql: &str,
        params: &P,
    ) -> Result<u64, DriverError> {
        let parse_reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::ParseKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard
            .push_command(
                bsql_postgres_proto::push_command::Parse {
                    stmt_name: bsql_postgres_proto::StmtName::default(),
                    sql,
                    reply: parse_reply,
                },
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.wb.clear();
        self.pump(None).await?;

        let query_reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let portal = bsql_postgres_proto::PortalName::default();
        let stmt = bsql_postgres_proto::StmtName::default();
        let actions = guard
            .push_bind_execute(
                &portal,
                &stmt,
                params,
                None,
                bsql_postgres_proto::FetchRows::All,
                query_reply,
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.wb.clear();

        self.pump(None).await?;
        let mut tag = String::new();
        if let Some(t) = self.proto.current_command_tag() {
            use core::fmt::Write;
            let _ = write!(tag, "{}", t);
        }
        Ok(tag.rsplit(' ').next().and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    /// Parse a SQL statement on the server and return a reusable handle.
    ///
    /// The server caches the query plan. Use with `query_prepared` or
    /// `execute_prepared` to avoid re-parsing on repeated calls.
    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, DriverError> {
        let id = self.stmt_counter;
        self.stmt_counter = self.stmt_counter.wrapping_add(1);
        let stmt_name = bsql_postgres_proto::StmtName::try_from_str(&format!("_bsql_{id}"))
            .expect("generated stmt name always valid");

        // Phase 1: Parse
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::ParseKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard
            .push_command(
                bsql_postgres_proto::push_command::Parse {
                    stmt_name,
                    sql,
                    reply,
                },
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.wb.clear();
        self.pump(None).await?;

        // Phase 2: DescribeStatement — gets RowDesc (SELECT) or NoData (DML)
        let desc_reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::DescribeStatementKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard
            .push_command(
                bsql_postgres_proto::push_command::DescribeStatement {
                    stmt_name,
                    reply: desc_reply,
                },
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.wb.clear();
        self.pump(None).await?;

        let row_desc = match self.proto.current_described_rows() {
            bsql_postgres_proto::DescribedRows::Rows(borrow) => Some(borrow.to_owned()),
            bsql_postgres_proto::DescribedRows::NoData => None,
        };
        let column_names = self.proto.current_column_names()
            .map(|s| s.to_vec())
            .unwrap_or_default();

        Ok(PreparedStatement { stmt_name, row_desc, column_names })
    }

    /// Execute a prepared statement with parameters and return rows.
    pub async fn query_prepared<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<QueryResult, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let portal = bsql_postgres_proto::PortalName::default();
        let actions = guard
            .push_bind_execute(
                &portal,
                &stmt.stmt_name,
                params,
                stmt.row_desc.clone(),
                bsql_postgres_proto::FetchRows::All,
                reply,
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.wb.clear();

        let mut rows: Vec<Row> = Vec::new();
        let mut command_tag = String::new();
        self.pump(Some(&mut rows)).await?;
        if let Some(tag) = self.proto.current_command_tag() {
            use core::fmt::Write;
            let _ = write!(command_tag, "{}", tag);
        }
        let column_names = stmt.column_names.clone();
        let column_count = rows.first().map_or(0, |r| r.len());
        Ok(QueryResult { rows, command_tag, column_count, column_names })
    }

    /// Execute a prepared DML statement with parameters. Returns affected rows.
    pub async fn execute_prepared<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
    ) -> Result<u64, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let portal = bsql_postgres_proto::PortalName::default();
        let actions = guard
            .push_bind_execute(
                &portal,
                &stmt.stmt_name,
                params,
                None,
                bsql_postgres_proto::FetchRows::All,
                reply,
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.wb.clear();
        self.pump(None).await?;
        let mut tag = String::new();
        if let Some(t) = self.proto.current_command_tag() {
            use core::fmt::Write;
            let _ = write!(tag, "{}", t);
        }
        Ok(tag.rsplit(' ').next().and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    /// Close a server-side prepared statement, releasing its resources.
    pub async fn close_statement(
        &mut self,
        stmt: PreparedStatement,
    ) -> Result<(), DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::CloseKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard
            .push_command(
                bsql_postgres_proto::push_command::CloseStatement {
                    stmt_name: stmt.stmt_name,
                    reply,
                },
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.wb.clear();
        self.pump(None).await
    }

    /// Server version string (e.g., "15.4").
    pub fn server_version(&self) -> Option<&str> {
        self.proto.session_params()
            .server_version
            .as_ref()
            .map(|s| s.as_str())
    }

    /// Whether the connection is in a usable state (Ready, not Errored).
    pub fn is_healthy(&self) -> bool {
        matches!(
            self.proto.connection_status(),
            bsql_postgres_proto::ConnectionStatus::Ready
        )
    }

    /// Server process ID for this connection.
    pub fn backend_pid(&self) -> i32 {
        self.proto.with_cancel_request(|_bytes, pid| pid)
    }

    /// Bulk-insert rows via COPY FROM STDIN.
    ///
    /// `table` is the target table name. `rows` is an iterator of
    /// tab-separated text lines (no trailing newline). Uses PG text
    /// format with tab delimiter.
    ///
    /// Returns the number of rows copied.
    pub async fn copy_in(
        &mut self,
        table: &str,
        rows: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<u64, DriverError> {
        let sql = format!("COPY {table} FROM STDIN");
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard
            .push_command(
                bsql_postgres_proto::push_command::SimpleQuery { sql: &sql, reply },
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.wb.clear();

        self.read_from_socket().await?;
        let actions = self.proto.feed_bytes(&[], &mut self.wb);
        let had_fail = actions.as_slice().iter()
            .any(|a| matches!(a, bsql_postgres_proto::Action::FailReply { .. }));
        drop(actions);
        if had_fail {
            let err = self.proto.fail_cause()
                .map(|&c| self.classify_error(c))
                .unwrap_or(DriverError::NotReady);
            self.drain_to_idle().await;
            return Err(err);
        }

        for row in rows {
            let line = row.as_ref();
            let mut data = line.as_bytes().to_vec();
            data.push(b'\n');
            let bytes = self.proto.push_copy_data(&data, &mut self.wb)
                .map_err(|e| DriverError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("{e}"),
                )))?;
            self.stream.write_all(bytes).await?;
            self.wb.clear();
        }

        let done_bytes = self.proto.push_copy_done(&mut self.wb)
            .map_err(|e| DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{e}"),
            )))?;
        self.stream.write_all(done_bytes).await?;
        self.wb.clear();

        self.pump(None).await?;
        let mut tag = String::new();
        if let Some(t) = self.proto.current_command_tag() {
            use core::fmt::Write;
            let _ = write!(tag, "{}", t);
        }
        Ok(tag.rsplit(' ').next().and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    /// Gracefully close the connection.
    pub async fn close(mut self) -> Result<(), DriverError> {
        let mut wb = WriteBuf::new();
        match self.proto.terminate(&mut wb) {
            Ok((bytes, _closed)) => {
                self.stream.write_all(bytes).await?;
                self.stream.shutdown().await?;
                Ok(())
            }
            Err(_) => Err(DriverError::Io(std::io::Error::other(
                "write buffer full on terminate",
            ))),
        }
    }

}

impl Connection {
    /// Core event pump: advance frames until Idle. Handles:
    /// - NeedMoreBytes retry (silent dispatches leave unread frames)
    /// - Socket read when buffer truly empty
    fn is_streaming_state(state: &bsql_postgres_proto::ActiveState) -> bool {
        matches!(state,
            bsql_postgres_proto::ActiveState::SimpleQueryStreamingRows { .. }
            | bsql_postgres_proto::ActiveState::BindExecuteStreamingRows { .. }
            | bsql_postgres_proto::ActiveState::BindExecuteAwaitingDataOrCompleteSelect { .. })
    }

    async fn pump(
        &mut self,
        mut rows: Option<&mut Vec<Row>>,
    ) -> Result<(), DriverError> {
        loop {
            if Self::is_streaming_state(self.proto.state()) {
                match rows {
                    Some(ref mut r) => self.collect_streaming(r).await?,
                    None => self.drain_streaming()?,
                }
                continue;
            }

            match self.proto.connection_status() {
                bsql_postgres_proto::ConnectionStatus::Ready => return Ok(()),
                bsql_postgres_proto::ConnectionStatus::Errored(_) => {
                    return Err(DriverError::NotReady);
                }
                _ => {}
            }

            let actions = self.proto.feed_bytes(&[], &mut self.wb);
            let mut had_fail = false;
            let mut had_close = false;
            for action in actions.as_slice() {
                match action {
                    bsql_postgres_proto::Action::SendBytes(bytes) => {
                        self.stream.write_all(bytes).await?;
                    }
                    bsql_postgres_proto::Action::FailReply { .. } => { had_fail = true; }
                    bsql_postgres_proto::Action::CloseSocket => { had_close = true; }
                    bsql_postgres_proto::Action::DeliverReply { .. }
                    | bsql_postgres_proto::Action::Notice { .. }
                    | bsql_postgres_proto::Action::Notify { .. }
                    | bsql_postgres_proto::Action::CopyDataChunk { .. } => {}
                    _ => {}
                }
            }
            let was_empty = actions.as_slice().is_empty();
            drop(actions);

            if had_close { return Err(DriverError::NotReady); }
            if had_fail {
                let err = self.proto.fail_cause()
                    .map(|&c| self.classify_error(c))
                    .unwrap_or(DriverError::NotReady);
                self.drain_to_idle().await;
                return Err(err);
            }

            match self.proto.connection_status() {
                bsql_postgres_proto::ConnectionStatus::Ready => return Ok(()),
                bsql_postgres_proto::ConnectionStatus::Errored(_) => {
                    return Err(DriverError::NotReady);
                }
                _ => {
                    if Self::is_streaming_state(self.proto.state()) {
                        continue;
                    }
                    if was_empty {
                        self.read_from_socket().await?;
                    }
                }
            }
        }
    }

    async fn read_from_socket(&mut self) -> Result<(), DriverError> {
        let n = self.stream.read(&mut self.buf).await?;
        if n == 0 {
            return Err(DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "server closed connection",
            )));
        }
        self.proto.feed_inbound(&self.buf[..n]).map_err(|_| {
            DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "read buffer full",
            ))
        })
    }

    fn drain_streaming(&mut self) -> Result<(), DriverError> {
        self.proto.iter_rows(&mut self.wb, |stream| {
            loop {
                match stream.col_next() {
                    bsql_postgres_proto::ColEvent::EndQuery { .. } => return,
                    bsql_postgres_proto::ColEvent::NeedMore => continue,
                    _ => {}
                }
            }
        });
        Ok(())
    }

    async fn drain_to_idle(&mut self) {
        for _ in 0..10 {
            let _ = self.proto.feed_bytes(&[], &mut self.wb);
            if matches!(self.proto.connection_status(),
                bsql_postgres_proto::ConnectionStatus::Ready) { return; }
            if self.read_from_socket().await.is_err() { return; }
        }
    }

    async fn collect_streaming(
        &mut self,
        rows: &mut Vec<Row>,
    ) -> Result<(), DriverError> {
        let mut prebuf = Vec::new();
        let mut scan_from = 0usize;
        let probe = std::time::Duration::from_millis(10);
        match tokio::time::timeout(probe, self.stream.read(&mut self.buf)).await {
            Ok(Ok(n)) if n > 0 => {
                prebuf.extend_from_slice(&self.buf[..n]);
                while !prebuf.get(scan_from..).unwrap_or(&[])
                    .windows(5).any(|w| w[0] == b'Z' && w[1..5] == [0, 0, 0, 5])
                {
                    scan_from = prebuf.len().saturating_sub(4);
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        self.stream.read(&mut self.buf),
                    ).await {
                        Ok(Ok(n)) if n > 0 => prebuf.extend_from_slice(&self.buf[..n]),
                        _ => break,
                    }
                }
            }
            _ => {}
        }

        let mut pos = 0usize;
        let prebuf_slice = prebuf.as_slice();
        self.proto.iter_rows(&mut self.wb, |rs| {
            let n_cols = rs.current_row_desc().map_or(0, |d| d.len());
            let mut current_row: Vec<Option<Vec<u8>>> = Vec::with_capacity(n_cols);
            loop {
                match rs.col_next() {
                    bsql_postgres_proto::ColEvent::Got { bytes, .. } => {
                        current_row.push(Some(bytes.to_vec()));
                    }
                    bsql_postgres_proto::ColEvent::Null { .. } => {
                        current_row.push(None);
                    }
                    bsql_postgres_proto::ColEvent::EndRow => {
                        rows.push(Row { columns: core::mem::take(&mut current_row) });
                    }
                    bsql_postgres_proto::ColEvent::EndQuery { .. } => return,
                    bsql_postgres_proto::ColEvent::NeedMore => {
                        if pos < prebuf_slice.len() {
                            let end = (pos + 2048).min(prebuf_slice.len());
                            if rs.feed(&prebuf_slice[pos..end]).is_ok() {
                                pos = end;
                                continue;
                            }
                        }
                    }
                    bsql_postgres_proto::ColEvent::Chunk { bytes, .. }
                    | bsql_postgres_proto::ColEvent::ChunkEnd { bytes, .. } => {
                        if let Some(Some(v)) = current_row.last_mut() {
                            v.extend_from_slice(bytes);
                        } else {
                            current_row.push(Some(bytes.to_vec()));
                        }
                    }
                    _ => {}
                }
            }
        });
        Ok(())
    }
}
