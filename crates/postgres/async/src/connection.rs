use bsql_postgres_core::{
    ConnectConfig, DriverError, Notification, PreparedStatement,
    PumpAction, QueryResult, Row, Session, SslMode,
};
use bsql_postgres_proto::FeedEvent;
use tokio::net::TcpStream;

enum Stream {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl Stream {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        match self { Self::Plain(s) => s.write_all(buf).await, Self::Tls(s) => s.write_all(buf).await }
    }
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        use tokio::io::AsyncReadExt;
        match self { Self::Plain(s) => s.read(buf).await, Self::Tls(s) => s.read(buf).await }
    }
    async fn shutdown(&mut self) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        match self { Self::Plain(s) => s.shutdown().await, Self::Tls(s) => s.shutdown().await }
    }
}

/// Async PostgreSQL connection — thin adapter over Session.
pub struct Connection {
    session: Session,
    stream: Stream,
    read_buf: Vec<u8>,
    terminated: bool,
}

impl Connection {
    pub async fn connect(config: &ConnectConfig) -> Result<Self, DriverError> {
        let addr = format!("{}:{}", config.host, config.port);
        let timeout = std::time::Duration::from_secs(config.connect_timeout_secs);
        let tcp = tokio::time::timeout(timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut, "connection timed out",
            )))??;

        let mut stream = Self::negotiate_ssl(tcp, config).await?;

        let (startup_bytes, mut hs) = bsql_postgres_core::Handshake::begin(config)?;
        stream.write_all(&startup_bytes).await?;

        let mut buf = vec![0u8; 4096];
        loop {
            match hs.step() {
                bsql_postgres_core::HandshakeAction::Send => {
                    stream.write_all(hs.pending_bytes()).await?;
                }
                bsql_postgres_core::HandshakeAction::NeedRead => {
                    let n = stream.read(&mut buf).await?;
                    if n == 0 { return Err(DriverError::Io(std::io::Error::other("server closed"))); }
                    hs.feed(&buf[..n])?;
                }
                bsql_postgres_core::HandshakeAction::Done => {
                    let session = hs.finish()?;
                    return Ok(Self { session, stream, read_buf: buf, terminated: false });
                }
                bsql_postgres_core::HandshakeAction::Error(e) => return Err(e),
            }
        }
    }

    async fn negotiate_ssl(tcp: TcpStream, config: &ConnectConfig) -> Result<Stream, DriverError> {
        if config.ssl_mode == SslMode::Disable {
            return Ok(Stream::Plain(tcp));
        }
        let (ssl_bytes, ssl_proto) = bsql_postgres_core::ssl::ssl_request_bytes();
        let mut tcp = tcp;
        { use tokio::io::AsyncWriteExt; tcp.write_all(ssl_bytes).await?; }
        let mut response = [0u8; 1];
        { use tokio::io::AsyncReadExt; tcp.read_exact(&mut response).await?; }

        match bsql_postgres_core::ssl::classify_ssl_response(ssl_proto, response[0], config)? {
            bsql_postgres_core::ssl::SslProbe::Accepted { tls_config, server_name } => {
                let connector = tokio_rustls::TlsConnector::from(tls_config);
                let tls_stream = connector.connect(server_name, tcp).await
                    .map_err(|e| DriverError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused, format!("TLS: {e}"),
                    )))?;
                Ok(Stream::Tls(tls_stream))
            }
            bsql_postgres_core::ssl::SslProbe::PlainTcp => Ok(Stream::Plain(tcp)),
        }
    }

    // --- Pump adapter: 15-line async I/O loop ---

    async fn pump(&mut self, mut rows: Option<&mut Vec<Row>>) -> Result<(), DriverError> {
        loop {
            match self.session.pump_step() {
                PumpAction::Send => {
                    let bytes = self.session.pending_bytes().to_vec();
                    self.stream.write_all(&bytes).await?;
                }
                PumpAction::NeedRead => {
                    let n = self.stream.read(&mut self.read_buf).await?;
                    if n == 0 { return Err(DriverError::Io(std::io::Error::other("server closed"))); }
                    self.session.feed(&self.read_buf[..n])?;
                }
                PumpAction::Done => return Ok(()),
                PumpAction::Streaming => {
                    match rows {
                        Some(ref mut r) => self.collect_streaming(r).await?,
                        None => self.session.drain_streaming(),
                    }
                }
                PumpAction::Error(e) => {
                    // Try to drain trailing RFQ so connection recovers.
                    for _ in 0..5 {
                        if self.session.is_healthy() { break; }
                        if let Ok(n) = self.stream.read(&mut self.read_buf).await
                            && n > 0 { let _ = self.session.feed(&self.read_buf[..n]); }
                        self.session.drain_to_idle();
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn collect_streaming(&mut self, rows: &mut Vec<Row>) -> Result<(), DriverError> {
        // Pre-buffer: read remaining response bytes before entering iter_rows.
        // Proto's connection_status tells us when all data is buffered.
        let mut prebuf = Vec::new();
        let probe = std::time::Duration::from_millis(10);
        match tokio::time::timeout(probe, self.stream.read(&mut self.read_buf)).await {
            Ok(Ok(n)) if n > 0 => {
                prebuf.extend_from_slice(&self.read_buf[..n]);
                let mut scan_from = 0usize;
                while !prebuf.get(scan_from..).unwrap_or(&[])
                    .windows(5).any(|w| w[0] == b'Z' && w[1..5] == [0, 0, 0, 5])
                {
                    scan_from = prebuf.len().saturating_sub(4);
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        self.stream.read(&mut self.read_buf),
                    ).await {
                        Ok(Ok(n)) if n > 0 => prebuf.extend_from_slice(&self.read_buf[..n]),
                        _ => break,
                    }
                }
            }
            _ => {}
        }

        let mut pos = 0usize;
        let feed_cap = self.session.feed_capacity();
        let prebuf_slice = prebuf.as_slice();
        self.session.iter_rows(|rs| {
            let n_cols = rs.current_row_desc().map_or(0, |d| d.len());
            let mut ab = bsql_postgres_core::ArenaBuilder::new(n_cols);
            loop {
                match rs.col_next() {
                    bsql_postgres_proto::ColEvent::Got { bytes, .. } => {
                        ab.push_value(bytes);
                    }
                    bsql_postgres_proto::ColEvent::Null { .. } => {
                        ab.push_null();
                    }
                    bsql_postgres_proto::ColEvent::EndRow => {
                        ab.end_row();
                    }
                    bsql_postgres_proto::ColEvent::EndQuery { .. } => {
                        *rows = ab.finish();
                        return;
                    }
                    bsql_postgres_proto::ColEvent::NeedMore
                        if pos < prebuf_slice.len() => {
                            let end = (pos + feed_cap).min(prebuf_slice.len());
                            if rs.feed(&prebuf_slice[pos..end]).is_ok() {
                                pos = end;
                                continue;
                            }
                        }
                    bsql_postgres_proto::ColEvent::Chunk { bytes, .. }
                    | bsql_postgres_proto::ColEvent::ChunkEnd { bytes, .. } => {
                        ab.extend_last(bytes);
                    }
                    _ => {}
                }
            }
        });
        Ok(())
    }

    // --- Public API (thin wrappers over Session + pump) ---

    pub async fn ping(&mut self) -> Result<(), DriverError> {
        self.session.push_ping()?;
        self.stream.write_all(self.session.pending_bytes()).await?;
        self.pump(None).await
    }

    pub async fn simple_query(&mut self, sql: &str) -> Result<String, DriverError> {
        self.session.push_simple_query(sql)?;
        self.stream.write_all(self.session.pending_bytes()).await?;
        self.pump(None).await?;
        Ok(self.session.extract_command_tag())
    }

    pub async fn execute(&mut self, sql: &str) -> Result<u64, DriverError> {
        let tag = self.simple_query(sql).await?;
        Ok(tag.rsplit(' ').next().and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub async fn execute_params<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, sql: &str, params: &P,
    ) -> Result<u64, DriverError> {
        let stmt = self.prepare(sql).await?;
        let result = self.execute_prepared(&stmt, params).await?;
        let _ = self.close_statement(stmt).await;
        Ok(result)
    }

    pub async fn query(&mut self, sql: &str) -> Result<QueryResult, DriverError> {
        self.session.push_simple_query(sql)?;
        self.stream.write_all(self.session.pending_bytes()).await?;
        let mut rows = Vec::new();
        self.pump(Some(&mut rows)).await?;
        Ok(self.session.build_query_result(rows))
    }

    pub async fn query_one(&mut self, sql: &str) -> Result<Row, DriverError> {
        let r = self.query(sql).await?;
        r.rows.into_iter().next().ok_or(DriverError::NoRows)
    }

    pub async fn query_opt(&mut self, sql: &str) -> Result<Option<Row>, DriverError> {
        let r = self.query(sql).await?;
        Ok(r.rows.into_iter().next())
    }

    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, DriverError> {
        let (_, stmt_name) = self.session.push_prepare(sql)?;
        self.stream.write_all(self.session.pending_bytes()).await?;
        self.pump(None).await?;

        self.session.push_describe_statement(stmt_name)?;
        self.stream.write_all(self.session.pending_bytes()).await?;
        self.pump(None).await?;

        Ok(self.session.finish_prepare(stmt_name))
    }

    pub async fn query_prepared<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, stmt: &PreparedStatement, params: &P,
    ) -> Result<QueryResult, DriverError> {
        self.session.push_bind_execute(stmt, params)?;
        self.stream.write_all(self.session.pending_bytes()).await?;
        let mut rows = Vec::new();
        self.pump(Some(&mut rows)).await?;
        Ok(self.session.build_query_result_from_stmt(rows, stmt))
    }

    pub async fn execute_prepared<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, stmt: &PreparedStatement, params: &P,
    ) -> Result<u64, DriverError> {
        self.session.push_bind_execute(stmt, params)?;
        self.stream.write_all(self.session.pending_bytes()).await?;
        self.pump(None).await?;
        let tag = self.session.extract_command_tag();
        Ok(tag.rsplit(' ').next().and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub async fn query_params<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, sql: &str, params: &P,
    ) -> Result<QueryResult, DriverError> {
        let stmt = self.prepare(sql).await?;
        let result = self.query_prepared(&stmt, params).await?;
        let _ = self.close_statement(stmt).await;
        Ok(result)
    }

    pub async fn query_params_one<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, sql: &str, params: &P,
    ) -> Result<Row, DriverError> {
        let r = self.query_params(sql, params).await?;
        r.rows.into_iter().next().ok_or(DriverError::NoRows)
    }

    pub async fn query_params_opt<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, sql: &str, params: &P,
    ) -> Result<Option<Row>, DriverError> {
        let r = self.query_params(sql, params).await?;
        Ok(r.rows.into_iter().next())
    }

    pub async fn close_statement(&mut self, stmt: PreparedStatement) -> Result<(), DriverError> {
        self.session.push_close_statement(stmt)?;
        self.stream.write_all(self.session.pending_bytes()).await?;
        self.pump(None).await
    }

    pub async fn begin(&mut self) -> Result<(), DriverError> { self.simple_query("BEGIN").await?; Ok(()) }
    pub async fn commit(&mut self) -> Result<(), DriverError> { self.simple_query("COMMIT").await?; Ok(()) }
    pub async fn rollback(&mut self) -> Result<(), DriverError> { self.simple_query("ROLLBACK").await?; Ok(()) }

    /// Execute an async closure within a transaction. COMMIT on Ok, ROLLBACK on Err.
    /// Tier-1 safety: transaction boundary = closure scope.
    // Note: async closures with borrowed &mut self don't work in stable Rust
    // without Box<dyn Future>. Use begin()/commit()/rollback() for async
    // transactions. The sync driver has a proper closure-based transaction()
    // because sync closures don't have this limitation.

    pub async fn listen(&mut self, channel: &str) -> Result<(), DriverError> {
        self.simple_query(&format!("LISTEN {channel}")).await?; Ok(())
    }

    pub async fn unlisten(&mut self, channel: &str) -> Result<(), DriverError> {
        self.simple_query(&format!("UNLISTEN {channel}")).await?; Ok(())
    }

    pub async fn recv_notification(
        &mut self, timeout: std::time::Duration,
    ) -> Result<Option<Notification>, DriverError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() { return Ok(None); }
            match tokio::time::timeout(remaining, self.stream.read(&mut self.read_buf)).await {
                Ok(Ok(0)) => return Err(DriverError::Io(std::io::Error::other("server closed"))),
                Ok(Ok(n)) => {
                    self.session.feed(&self.read_buf[..n])?;
                    let event = self.session.proto.advance_one_frame(&mut self.session.wb);
                    if let FeedEvent::Notify { notif_ref, pid } = event
                        && let Ok(payload) = self.session.proto.get_notification(notif_ref) {
                            return Ok(Some(Notification {
                                channel: payload.channel.as_str().to_string(),
                                payload: String::from_utf8_lossy(&payload.payload).into_owned(),
                                pid,
                            }));
                        }
                }
                Ok(Err(e)) => return Err(DriverError::Io(e)),
                Err(_) => return Ok(None),
            }
        }
    }

    pub async fn copy_in(
        &mut self, table: &str,
        rows_data: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<u64, DriverError> {
        self.session.push_simple_query(&format!("COPY {table} FROM STDIN"))?;
        self.stream.write_all(self.session.pending_bytes()).await?;

        // Read CopyInResponse
        let n = self.stream.read(&mut self.read_buf).await?;
        if n == 0 { return Err(DriverError::Io(std::io::Error::other("server closed"))); }
        self.session.feed(&self.read_buf[..n])?;
        let actions = self.session.proto.feed_bytes(&[], &mut self.session.wb);
        let had_fail = actions.as_slice().iter()
            .any(|a| matches!(a, bsql_postgres_proto::Action::FailReply { .. }));
        drop(actions);
        if had_fail {
            let err = self.session.proto.fail_cause()
                .map(|&c| self.session.classify_error(c))
                .unwrap_or(DriverError::NotReady);
            self.session.drain_to_idle();
            return Err(err);
        }

        for row in rows_data {
            let line = row.as_ref();
            let mut data = line.as_bytes().to_vec();
            data.push(b'\n');
            let bytes = self.session.proto.push_copy_data(&data, &mut self.session.wb)
                .map_err(|e| DriverError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput, format!("{e}"),
                )))?;
            self.stream.write_all(bytes).await?;
            self.session.wb.clear();
        }

        let done_bytes = self.session.proto.push_copy_done(&mut self.session.wb)
            .map_err(|e| DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput, format!("{e}"),
            )))?;
        self.stream.write_all(done_bytes).await?;
        self.session.wb.clear();

        self.pump(None).await?;
        let tag = self.session.extract_command_tag();
        Ok(tag.rsplit(' ').next().and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub fn is_healthy(&self) -> bool { self.session.is_healthy() }
    pub fn server_version(&self) -> Option<&str> { self.session.server_version() }
    pub fn backend_pid(&self) -> i32 { self.session.backend_pid() }

    pub async fn close(&mut self) -> Result<(), DriverError> {
        if self.terminated { return Ok(()); }
        self.terminated = true;
        self.stream.write_all(&[b'X', 0, 0, 0, 4]).await?;
        self.stream.shutdown().await?;
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.terminated { return; }
        // Best-effort Terminate via non-blocking try_write.
        // Can't .await in Drop — this is the sync fallback.
        let terminate = [b'X', 0, 0, 0, 4];
        match &self.stream {
            Stream::Plain(tcp) => { let _ = tcp.try_write(&terminate); }
            Stream::Tls(_) => {} // TLS needs async — skip, OS RST will clean up
        }
    }
}
