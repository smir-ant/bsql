use std::io::{Read, Write};
use std::net::TcpStream;

use bsql_postgres_core::{
    ConnectConfig, DriverError, PreparedStatement,
    PumpAction, QueryResult, Row, Session, SslMode,
};
use bsql_postgres_proto::{ActivePhase, FeedEvent, PgProtocol, WriteBuf};

enum Stream {
    Plain(TcpStream),
    Tls(rustls::StreamOwned<rustls::ClientConnection, TcpStream>),
}

impl Stream {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => { s.write_all(buf)?; s.flush() }
            Self::Tls(s) => { s.write_all(buf)?; s.flush() }
        }
    }
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self { Self::Plain(s) => s.read(buf), Self::Tls(s) => s.read(buf) }
    }
    fn set_read_timeout(&self, dur: Option<std::time::Duration>) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.set_read_timeout(dur),
            Self::Tls(s) => s.sock.set_read_timeout(dur),
        }
    }
    fn shutdown(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.shutdown(std::net::Shutdown::Both),
            Self::Tls(s) => {
                s.conn.send_close_notify();
                let _ = s.conn.write_tls(&mut s.sock);
                s.sock.shutdown(std::net::Shutdown::Both)
            }
        }
    }
}

/// Sync PostgreSQL connection — thin adapter over Session.
pub struct Connection {
    session: Session,
    stream: Stream,
    read_buf: Vec<u8>,
    terminated: bool,
}

impl Connection {
    pub fn connect(config: &ConnectConfig) -> Result<Self, DriverError> {
        let addr = format!("{}:{}", config.host, config.port);
        let tcp = TcpStream::connect(&addr)?;
        tcp.set_read_timeout(Some(std::time::Duration::from_secs(config.connect_timeout_secs)))?;
        tcp.set_write_timeout(Some(std::time::Duration::from_secs(config.connect_timeout_secs)))?;

        let mut stream = if config.ssl_mode == SslMode::Disable {
            Stream::Plain(tcp)
        } else {
            Self::negotiate_ssl(tcp, config)?
        };

        let user = bsql_postgres_proto::Ident::try_from_str(&config.user)
            .map_err(|_| DriverError::Config("invalid user name"))?;
        let database = match &config.database {
            Some(d) => Some(bsql_postgres_proto::DatabaseName::try_from_str(d)
                .map_err(|_| DriverError::Config("invalid database name"))?),
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

        let (actions, mut connecting) = proto.push_startup(
            user, database, None, credentials, reply, &mut wb,
        ).map_err(|pf| DriverError::Protocol(*pf.cause))?;

        let startup_bytes: Vec<u8> = actions.as_slice().iter().filter_map(|a| {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = a { Some(*bytes) } else { None }
        }).flatten().copied().collect();
        drop(actions);
        match &mut stream {
            Stream::Plain(tcp) => { tcp.write_all(&startup_bytes)?; tcp.flush()?; }
            Stream::Tls(tls) => { tls.write_all(&startup_bytes)?; tls.flush()?; }
        }

        let mut buf = vec![0u8; 4096];
        loop {
            let n = stream.read(&mut buf)?;
            if n == 0 { return Err(DriverError::Io(std::io::Error::other("server closed"))); }
            if connecting.feed_inbound(&buf[..n]).is_err() {
                return Err(DriverError::Io(std::io::Error::other("read buffer full")));
            }
            let mut consecutive_need = 0u32;
            loop {
                let event = connecting.advance_one_frame(&mut wb);
                match event {
                    FeedEvent::Idle => {
                        consecutive_need = 0;
                        match connecting.into_active() {
                            Ok(active) => {
                                let session = Session::new(active, wb, vec![0u8; 4096]);
                                return Ok(Self { session, stream, read_buf: buf, terminated: false });
                            }
                            Err(bsql_postgres_proto::IntoActiveError::StillConnecting(c)) => {
                                connecting = c;
                            }
                            Err(_) => return Err(DriverError::NotReady),
                        }
                    }
                    FeedEvent::SendBytes(bytes) => { consecutive_need = 0; stream.write_all(bytes)?; }
                    FeedEvent::NeedMoreBytes => {
                        consecutive_need += 1;
                        if consecutive_need > 20 { break; }
                    }
                    FeedEvent::Fail(_) => return Err(DriverError::Io(std::io::Error::other("auth failed"))),
                    FeedEvent::Close => return Err(DriverError::NotReady),
                    _ => { consecutive_need = 0; }
                }
            }
        }
    }

    fn negotiate_ssl(tcp: TcpStream, config: &ConnectConfig) -> Result<Stream, DriverError> {
        let proto_disc = PgProtocol::<bsql_postgres_proto::DisconnectedPhase>::new();
        let (ssl_bytes, ssl_proto) = proto_disc.push_ssl_request();
        let mut tcp = tcp;
        tcp.write_all(ssl_bytes)?;
        let mut response = [0u8; 1];
        tcp.read_exact(&mut response)?;
        let classified = ssl_proto.classify_ssl_response(response[0]);
        match classified {
            bsql_postgres_proto::SslClassified::Accepted(_) => {
                let mut root_store = rustls::RootCertStore::empty();
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                let tls_config = rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth();
                let server_name: rustls::pki_types::ServerName<'_> = config.host.clone().try_into()
                    .map_err(|_| DriverError::Config("invalid server name for TLS"))?;
                let tls_conn = rustls::ClientConnection::new(
                    std::sync::Arc::new(tls_config), server_name,
                ).map_err(|e| DriverError::Io(std::io::Error::other(format!("TLS: {e}"))))?;
                Ok(Stream::Tls(rustls::StreamOwned::new(tls_conn, tcp)))
            }
            bsql_postgres_proto::SslClassified::Refused(_) => {
                if config.ssl_mode == SslMode::Require { return Err(DriverError::SslRefused); }
                Ok(Stream::Plain(tcp))
            }
            _ => Err(DriverError::Io(std::io::Error::other("unexpected SSL response"))),
        }
    }

    // --- Pump adapter: sync I/O loop ---

    fn pump(&mut self, mut rows: Option<&mut Vec<Row>>) -> Result<(), DriverError> {
        loop {
            match self.session.pump_step() {
                PumpAction::Send => {
                    let bytes = self.session.pending_bytes().to_vec();
                    self.stream.write_all(&bytes)?;
                }
                PumpAction::NeedRead => {
                    let n = self.stream.read(&mut self.read_buf)?;
                    if n == 0 { return Err(DriverError::Io(std::io::Error::other("server closed"))); }
                    self.session.feed(&self.read_buf[..n])?;
                }
                PumpAction::Done => return Ok(()),
                PumpAction::Streaming => {
                    match rows {
                        Some(ref mut r) => self.collect_streaming(r)?,
                        None => self.session.drain_streaming(),
                    }
                }
                PumpAction::Error(e) => {
                    for _ in 0..5 {
                        if self.session.is_healthy() { break; }
                        if let Ok(n) = self.stream.read(&mut self.read_buf) {
                            if n > 0 { let _ = self.session.feed(&self.read_buf[..n]); }
                        }
                        self.session.drain_to_idle();
                    }
                    return Err(e);
                }
            }
        }
    }

    fn collect_streaming(&mut self, rows: &mut Vec<Row>) -> Result<(), DriverError> {
        let feed_cap = self.session.feed_capacity();
        let stream = &mut self.stream;
        let read_buf = &mut self.read_buf;

        self.session.iter_rows(|rs| {
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
                        rows.push(Row::from_columns(core::mem::take(&mut current_row)));
                    }
                    bsql_postgres_proto::ColEvent::EndQuery { .. } => return,
                    bsql_postgres_proto::ColEvent::NeedMore => {
                        match rs.col_next() {
                            bsql_postgres_proto::ColEvent::EndQuery { .. } => return,
                            bsql_postgres_proto::ColEvent::Got { bytes, .. } => {
                                current_row.push(Some(bytes.to_vec()));
                                continue;
                            }
                            bsql_postgres_proto::ColEvent::NeedMore => {
                                let cap = feed_cap.max(1).min(read_buf.len());
                                let Ok(n) = stream.read(&mut read_buf[..cap]) else { return };
                                if n == 0 { return; }
                                let _ = rs.feed(&read_buf[..n]);
                            }
                            other => {
                                match other {
                                    bsql_postgres_proto::ColEvent::Null { .. } => current_row.push(None),
                                    bsql_postgres_proto::ColEvent::EndRow => {
                                        rows.push(Row::from_columns(core::mem::take(&mut current_row)));
                                    }
                                    _ => {}
                                }
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

    // --- Public API ---

    pub fn ping(&mut self) -> Result<(), DriverError> {
        self.session.push_ping()?;
        self.stream.write_all(self.session.pending_bytes())?;
        self.pump(None)
    }

    pub fn simple_query(&mut self, sql: &str) -> Result<String, DriverError> {
        self.session.push_simple_query(sql)?;
        self.stream.write_all(self.session.pending_bytes())?;
        self.pump(None)?;
        Ok(self.session.extract_command_tag())
    }

    pub fn execute(&mut self, sql: &str) -> Result<u64, DriverError> {
        let tag = self.simple_query(sql)?;
        Ok(tag.rsplit(' ').next().and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryResult, DriverError> {
        self.session.push_simple_query(sql)?;
        self.stream.write_all(self.session.pending_bytes())?;
        let mut rows = Vec::new();
        self.pump(Some(&mut rows))?;
        Ok(self.session.build_query_result(rows))
    }

    pub fn query_one(&mut self, sql: &str) -> Result<Row, DriverError> {
        self.query(sql)?.rows.into_iter().next().ok_or(DriverError::NoRows)
    }

    pub fn query_opt(&mut self, sql: &str) -> Result<Option<Row>, DriverError> {
        Ok(self.query(sql)?.rows.into_iter().next())
    }

    pub fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, DriverError> {
        let (_, stmt_name) = self.session.push_prepare(sql)?;
        self.stream.write_all(self.session.pending_bytes())?;
        self.pump(None)?;

        self.session.push_describe_statement(stmt_name)?;
        self.stream.write_all(self.session.pending_bytes())?;
        self.pump(None)?;

        Ok(self.session.finish_prepare(stmt_name))
    }

    pub fn query_prepared<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, stmt: &PreparedStatement, params: &P,
    ) -> Result<QueryResult, DriverError> {
        self.session.push_bind_execute(stmt, params)?;
        self.stream.write_all(self.session.pending_bytes())?;
        let mut rows = Vec::new();
        self.pump(Some(&mut rows))?;
        Ok(self.session.build_query_result_from_stmt(rows, stmt))
    }

    pub fn execute_prepared<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, stmt: &PreparedStatement, params: &P,
    ) -> Result<u64, DriverError> {
        self.session.push_bind_execute(stmt, params)?;
        self.stream.write_all(self.session.pending_bytes())?;
        self.pump(None)?;
        let tag = self.session.extract_command_tag();
        Ok(tag.rsplit(' ').next().and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub fn query_params<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, sql: &str, params: &P,
    ) -> Result<QueryResult, DriverError> {
        let stmt = self.prepare(sql)?;
        let result = self.query_prepared(&stmt, params)?;
        let _ = self.close_statement(stmt);
        Ok(result)
    }

    pub fn query_params_one<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, sql: &str, params: &P,
    ) -> Result<Row, DriverError> {
        self.query_params(sql, params)?.rows.into_iter().next().ok_or(DriverError::NoRows)
    }

    pub fn query_params_opt<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, sql: &str, params: &P,
    ) -> Result<Option<Row>, DriverError> {
        Ok(self.query_params(sql, params)?.rows.into_iter().next())
    }

    pub fn close_statement(&mut self, stmt: PreparedStatement) -> Result<(), DriverError> {
        self.session.push_close_statement(stmt)?;
        self.stream.write_all(self.session.pending_bytes())?;
        self.pump(None)
    }

    pub fn execute_params<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, sql: &str, params: &P,
    ) -> Result<u64, DriverError> {
        let stmt = self.prepare(sql)?;
        let result = self.execute_prepared(&stmt, params)?;
        let _ = self.close_statement(stmt);
        Ok(result)
    }

    pub fn begin(&mut self) -> Result<(), DriverError> { self.simple_query("BEGIN")?; Ok(()) }
    pub fn commit(&mut self) -> Result<(), DriverError> { self.simple_query("COMMIT")?; Ok(()) }
    pub fn rollback(&mut self) -> Result<(), DriverError> { self.simple_query("ROLLBACK")?; Ok(()) }

    pub fn listen(&mut self, channel: &str) -> Result<(), DriverError> {
        self.simple_query(&format!("LISTEN {channel}"))?; Ok(())
    }

    pub fn unlisten(&mut self, channel: &str) -> Result<(), DriverError> {
        self.simple_query(&format!("UNLISTEN {channel}"))?; Ok(())
    }

    pub fn recv_notification(
        &mut self, timeout: std::time::Duration,
    ) -> Result<Option<bsql_postgres_core::Notification>, DriverError> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() { return Ok(None); }
            self.stream.set_read_timeout(Some(remaining))?;
            match self.stream.read(&mut self.read_buf) {
                Ok(0) => return Err(DriverError::Io(std::io::Error::other("server closed"))),
                Ok(n) => {
                    self.session.feed(&self.read_buf[..n])?;
                    let event = self.session.proto.advance_one_frame(&mut self.session.wb);
                    if let FeedEvent::Notify { notif_ref, pid } = event {
                        if let Ok(payload) = self.session.proto.get_notification(notif_ref) {
                            self.stream.set_read_timeout(Some(std::time::Duration::from_secs(
                                10)))?;
                            return Ok(Some(bsql_postgres_core::Notification {
                                channel: payload.channel.as_str().to_string(),
                                payload: String::from_utf8_lossy(&payload.payload).into_owned(),
                                pid,
                            }));
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {
                    return Ok(None);
                }
                Err(e) => return Err(DriverError::Io(e)),
            }
        }
    }

    pub fn copy_in(
        &mut self, table: &str,
        rows_data: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<u64, DriverError> {
        self.session.push_simple_query(&format!("COPY {table} FROM STDIN"))?;
        self.stream.write_all(self.session.pending_bytes())?;

        let n = self.stream.read(&mut self.read_buf)?;
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
            self.stream.write_all(bytes)?;
            self.session.wb.clear();
        }

        let done_bytes = self.session.proto.push_copy_done(&mut self.session.wb)
            .map_err(|e| DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput, format!("{e}"),
            )))?;
        self.stream.write_all(done_bytes)?;
        self.session.wb.clear();

        self.pump(None)?;
        let tag = self.session.extract_command_tag();
        Ok(tag.rsplit(' ').next().and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub fn is_healthy(&self) -> bool { self.session.is_healthy() }
    pub fn server_version(&self) -> Option<&str> { self.session.server_version() }
    pub fn backend_pid(&self) -> i32 { self.session.backend_pid() }

    pub fn close(&mut self) -> Result<(), DriverError> {
        if self.terminated { return Ok(()); }
        self.terminated = true;
        self.stream.write_all(&[b'X', 0, 0, 0, 4])?;
        self.stream.shutdown()?;
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if self.terminated { return; }
        let _ = self.stream.write_all(&[b'X', 0, 0, 0, 4]);
    }
}
