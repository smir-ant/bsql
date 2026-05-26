use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU32, Ordering};

use bsql_postgres_proto::{
    ActivePhase, FeedEvent, PgProtocol, WriteBuf,
};

use crate::config::{ConnectConfig, SslMode};
use crate::error::{DbError, DriverError};

static STMT_COUNTER: AtomicU32 = AtomicU32::new(0);

#[derive(Debug)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub command_tag: String,
    pub column_count: usize,
    pub column_names: Vec<String>,
}

#[derive(Debug)]
pub struct Row {
    columns: Vec<Option<Vec<u8>>>,
}

impl Row {
    pub fn get_str(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx)?.as_deref().and_then(|b| core::str::from_utf8(b).ok())
    }
    pub fn get_i32(&self, idx: usize) -> Option<i32> { self.get_str(idx)?.parse().ok() }
    pub fn get_i64(&self, idx: usize) -> Option<i64> { self.get_str(idx)?.parse().ok() }
    pub fn get_f64(&self, idx: usize) -> Option<f64> { self.get_str(idx)?.parse().ok() }
    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        match self.get_str(idx)? { "t" => Some(true), "f" => Some(false), _ => None }
    }
    pub fn get_raw(&self, idx: usize) -> Option<&[u8]> { self.columns.get(idx)?.as_deref() }
    pub fn is_null(&self, idx: usize) -> bool { matches!(self.columns.get(idx), Some(None)) }
    pub fn len(&self) -> usize { self.columns.len() }
    pub fn is_empty(&self) -> bool { self.columns.is_empty() }
    pub fn get_by_name<'a>(&'a self, name: &str, column_names: &[String]) -> Option<&'a [u8]> {
        let idx = column_names.iter().position(|n| n == name)?;
        self.get_raw(idx)
    }
}

#[derive(Debug)]
pub struct PreparedStatement {
    stmt_name: bsql_postgres_proto::StmtName,
    row_desc: Option<bsql_postgres_proto::decode::RowDesc>,
    pub column_names: Vec<String>,
}

impl PreparedStatement {
    pub fn returns_rows(&self) -> bool { self.row_desc.is_some() }
}

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

pub struct Connection {
    proto: PgProtocol<ActivePhase>,
    stream: Stream,
    wb: WriteBuf,
    buf: Vec<u8>,
}

impl Connection {
    pub fn connect(config: &ConnectConfig) -> Result<Self, DriverError> {
        let addr = format!("{}:{}", config.host, config.port);
        let tcp = TcpStream::connect(&addr)?;
        tcp.set_read_timeout(Some(std::time::Duration::from_secs(config.connect_timeout_secs)))?;
        tcp.set_write_timeout(Some(std::time::Duration::from_secs(config.connect_timeout_secs)))?;

        let mut stream = Stream::Plain(tcp);

        let user = bsql_postgres_proto::Ident::try_from_str(&config.user)
            .map_err(|_| DriverError::Io(std::io::Error::other("invalid user name")))?;
        let database = match &config.database {
            Some(d) => Some(bsql_postgres_proto::DatabaseName::try_from_str(d)
                .map_err(|_| DriverError::Io(std::io::Error::other("invalid database name")))?),
            None => None,
        };

        let mut proto = PgProtocol::new();
        let mut wb = WriteBuf::new();
        let reply = proto.next_reply_id::<bsql_postgres_proto::reply_id::StartupKind>();

        let credentials = match &config.password {
            Some(pw) => {
                let password = bsql_postgres_proto::Password::try_from_str(pw)
                    .map_err(|_| DriverError::Io(std::io::Error::other("invalid password")))?;
                bsql_postgres_proto::Credentials::ScramPassword(
                    bsql_postgres_proto::sensitive::Sensitive::new(password),
                )
            }
            None => bsql_postgres_proto::password::Credentials::Trust,
        };

        let (actions, mut connecting) = proto.push_startup(
            user, database, None, credentials, reply, &mut wb,
        ).map_err(|pf| DriverError::Protocol(*pf.cause))?;

        // Write startup directly to the inner TCP stream to bypass Stream enum.
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
                            Ok(active) => return Ok(Self { proto: active, stream, wb, buf }),
                            Err(bsql_postgres_proto::IntoActiveError::StillConnecting(c)) => {
                                connecting = c;
                            }
                            Err(_) => return Err(DriverError::NotReady),
                        }
                    }
                    FeedEvent::SendBytes(bytes) => {
                        consecutive_need = 0;
                        stream.write_all(bytes)?;
                    }
                    FeedEvent::NeedMoreBytes => {
                        consecutive_need += 1;
                        if consecutive_need > 10 { break; }
                    }
                    FeedEvent::Fail(_) => {
                        return Err(DriverError::Io(std::io::Error::other("auth failed")));
                    }
                    FeedEvent::Close => return Err(DriverError::NotReady),
                    _ => { consecutive_need = 0; }
                }
            }
        }
    }

    fn negotiate_ssl(tcp: TcpStream, config: &ConnectConfig) -> Result<Stream, DriverError> {
        if config.ssl_mode == SslMode::Disable {
            return Ok(Stream::Plain(tcp));
        }
        let proto_disc = bsql_postgres_proto::PgProtocol::<bsql_postgres_proto::DisconnectedPhase>::new();
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
                let server_name = config.host.clone().try_into()
                    .unwrap_or_else(|_| "localhost".to_owned().try_into().expect("localhost"));
                let tls_conn = rustls::ClientConnection::new(
                    std::sync::Arc::new(tls_config), server_name,
                ).map_err(|e| DriverError::Io(std::io::Error::other(format!("TLS: {e}"))))?;
                Ok(Stream::Tls(rustls::StreamOwned::new(tls_conn, tcp)))
            }
            bsql_postgres_proto::SslClassified::Refused(_) => {
                if config.ssl_mode == SslMode::Require {
                    return Err(DriverError::SslRefused);
                }
                Ok(Stream::Plain(tcp))
            }
            _ => Err(DriverError::Io(std::io::Error::other("unexpected SSL response"))),
        }
    }

    fn classify_error(&self, cause: bsql_postgres_proto::ProtocolError) -> DriverError {
        if let bsql_postgres_proto::ProtocolError::ServerErrorResponse {
            severity, code, details_ref,
        } = cause {
            let sev = severity.map(|s| s.as_str().to_string()).unwrap_or_else(|| "ERROR".to_string());
            let sqlstate = code.as_str().trim().to_string();
            let (msg, det, hnt) = match self.proto.get_server_error(details_ref) {
                Ok(bsql_postgres_proto::ErrorPayload::ServerError { message, detail, hint }) => {
                    let m = message.as_str().to_string();
                    let d = { let s = detail.as_str(); if s.is_empty() { None } else { Some(s.to_string()) } };
                    let h = { let s = hint.as_str(); if s.is_empty() { None } else { Some(s.to_string()) } };
                    (m, d, h)
                }
                _ => (String::new(), None, None),
            };
            return DriverError::Db(DbError { code: sqlstate, severity: sev, message: msg, detail: det, hint: hnt });
        }
        DriverError::Protocol(cause)
    }

    fn read_from_socket(&mut self) -> Result<(), DriverError> {
        let n = self.stream.read(&mut self.buf)?;
        if n == 0 { return Err(DriverError::Io(std::io::Error::other("server closed"))); }
        self.proto.feed_inbound(&self.buf[..n]).map_err(|_|
            DriverError::Io(std::io::Error::other("read buffer full"))
        )
    }

    fn drain_to_idle(&mut self) {
        for _ in 0..10 {
            let _ = self.proto.feed_bytes(&[], &mut self.wb);
            if matches!(self.proto.connection_status(),
                bsql_postgres_proto::ConnectionStatus::Ready) { return; }
            if self.read_from_socket().is_err() { return; }
        }
    }

    fn is_streaming_state(state: &bsql_postgres_proto::ActiveState) -> bool {
        matches!(state,
            bsql_postgres_proto::ActiveState::SimpleQueryStreamingRows { .. }
            | bsql_postgres_proto::ActiveState::BindExecuteStreamingRows { .. }
            | bsql_postgres_proto::ActiveState::BindExecuteAwaitingDataOrCompleteSelect { .. })
    }

    fn pump(&mut self, mut rows: Option<&mut Vec<Row>>) -> Result<(), DriverError> {
        loop {
            if Self::is_streaming_state(self.proto.state()) {
                match rows {
                    Some(ref mut r) => self.collect_streaming(r)?,
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
                        self.stream.write_all(bytes)?;
                    }
                    bsql_postgres_proto::Action::FailReply { .. } => { had_fail = true; }
                    bsql_postgres_proto::Action::CloseSocket => { had_close = true; }
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
                self.drain_to_idle();
                return Err(err);
            }

            if was_empty && !matches!(self.proto.connection_status(),
                bsql_postgres_proto::ConnectionStatus::Ready)
            {
                self.read_from_socket()?;
            }
        }
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

    // Sync collect_streaming — blocking reads inside iter_rows work naturally.
    fn collect_streaming(&mut self, rows: &mut Vec<Row>) -> Result<(), DriverError> {
        let stream = &mut self.stream;
        let buf = &mut self.buf;

        self.proto.iter_rows(&mut self.wb, |rs| {
            let mut current_row: Vec<Option<Vec<u8>>> = Vec::new();
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
                        let Ok(n) = stream.read(buf) else { return };
                        if n == 0 { return; }
                        let _ = rs.feed(&buf[..n]);
                    }
                    bsql_postgres_proto::ColEvent::Chunk { bytes, .. } => {
                        if let Some(Some(v)) = current_row.last_mut() {
                            v.extend_from_slice(bytes);
                        } else {
                            current_row.push(Some(bytes.to_vec()));
                        }
                    }
                    bsql_postgres_proto::ColEvent::ChunkEnd { bytes, .. } => {
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

    fn push_and_send(&mut self, cmd: impl bsql_postgres_proto::push_command::PushCommand) -> Result<(), DriverError> {
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard.push_command(cmd, &mut self.wb)
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes)?;
            }
        }
        self.wb.clear();
        Ok(())
    }

    pub fn ping(&mut self) -> Result<(), DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::PingKind>();
        self.push_and_send(bsql_postgres_proto::push_command::Ping::new(reply))?;
        self.pump(None)
    }

    pub fn simple_query(&mut self, sql: &str) -> Result<String, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
        self.push_and_send(bsql_postgres_proto::push_command::SimpleQuery { sql, reply })?;
        self.pump(None)?;
        let mut tag = String::new();
        if let Some(t) = self.proto.current_command_tag() {
            use core::fmt::Write;
            let _ = write!(tag, "{}", t);
        }
        Ok(tag)
    }

    pub fn execute(&mut self, sql: &str) -> Result<u64, DriverError> {
        let tag = self.simple_query(sql)?;
        Ok(tag.rsplit(' ').next().and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub fn query(&mut self, sql: &str) -> Result<QueryResult, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
        self.push_and_send(bsql_postgres_proto::push_command::SimpleQuery { sql, reply })?;
        let mut rows = Vec::new();
        self.pump(Some(&mut rows))?;
        let column_names = self.proto.current_column_names().map(|s| s.to_vec()).unwrap_or_default();
        let column_count = rows.first().map_or(0, |r| r.len());
        let mut command_tag = String::new();
        if let Some(t) = self.proto.current_command_tag() {
            use core::fmt::Write;
            let _ = write!(command_tag, "{}", t);
        }
        Ok(QueryResult { rows, command_tag, column_count, column_names })
    }

    pub fn query_one(&mut self, sql: &str) -> Result<Row, DriverError> {
        let r = self.query(sql)?;
        r.rows.into_iter().next().ok_or(DriverError::Io(std::io::Error::other("no rows")))
    }

    pub fn query_opt(&mut self, sql: &str) -> Result<Option<Row>, DriverError> {
        let r = self.query(sql)?;
        Ok(r.rows.into_iter().next())
    }

    pub fn prepare(&mut self, sql: &str) -> Result<PreparedStatement, DriverError> {
        let stmt_name = {
            let id = STMT_COUNTER.fetch_add(1, Ordering::Relaxed);
            bsql_postgres_proto::StmtName::try_from_str(&format!("_bsql_s{id}"))
                .expect("generated name valid")
        };

        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::ParseKind>();
        self.push_and_send(bsql_postgres_proto::push_command::Parse { stmt_name, sql, reply })?;
        self.pump(None)?;

        let desc_reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::DescribeStatementKind>();
        self.push_and_send(bsql_postgres_proto::push_command::DescribeStatement { stmt_name, reply: desc_reply })?;
        self.pump(None)?;

        let row_desc = match self.proto.current_described_rows() {
            bsql_postgres_proto::DescribedRows::Rows(b) => Some(b.to_owned()),
            bsql_postgres_proto::DescribedRows::NoData => None,
        };
        let column_names = self.proto.current_column_names().map(|s| s.to_vec()).unwrap_or_default();
        Ok(PreparedStatement { stmt_name, row_desc, column_names })
    }

    pub fn query_prepared<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, stmt: &PreparedStatement, params: &P,
    ) -> Result<QueryResult, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
        let portal = bsql_postgres_proto::PortalName::default();
        {
            let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
            let actions = guard.push_bind_execute(
                &portal, &stmt.stmt_name, params, stmt.row_desc.clone(),
                bsql_postgres_proto::FetchRows::All, reply, &mut self.wb,
            ).map_err(|pf| DriverError::Protocol(*pf.cause))?;
            for action in actions.as_slice() {
                if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                    self.stream.write_all(bytes)?;
                }
            }
            self.wb.clear();
        }
        let mut rows = Vec::new();
        self.pump(Some(&mut rows))?;
        let column_names = stmt.column_names.clone();
        let column_count = rows.first().map_or(0, |r| r.len());
        let mut command_tag = String::new();
        if let Some(t) = self.proto.current_command_tag() {
            use core::fmt::Write;
            let _ = write!(command_tag, "{}", t);
        }
        Ok(QueryResult { rows, command_tag, column_count, column_names })
    }

    pub fn execute_prepared<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, stmt: &PreparedStatement, params: &P,
    ) -> Result<u64, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
        let portal = bsql_postgres_proto::PortalName::default();
        {
            let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
            let actions = guard.push_bind_execute(
                &portal, &stmt.stmt_name, params, None,
                bsql_postgres_proto::FetchRows::All, reply, &mut self.wb,
            ).map_err(|pf| DriverError::Protocol(*pf.cause))?;
            for action in actions.as_slice() {
                if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                    self.stream.write_all(bytes)?;
                }
            }
            self.wb.clear();
        }
        self.pump(None)?;
        let mut tag = String::new();
        if let Some(t) = self.proto.current_command_tag() {
            use core::fmt::Write;
            let _ = write!(tag, "{}", t);
        }
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
        let r = self.query_params(sql, params)?;
        r.rows.into_iter().next().ok_or(DriverError::Io(std::io::Error::other("no rows")))
    }

    pub fn close_statement(&mut self, stmt: PreparedStatement) -> Result<(), DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::CloseKind>();
        self.push_and_send(bsql_postgres_proto::push_command::CloseStatement { stmt_name: stmt.stmt_name, reply })?;
        self.pump(None)
    }

    pub fn begin(&mut self) -> Result<(), DriverError> { self.simple_query("BEGIN")?; Ok(()) }
    pub fn commit(&mut self) -> Result<(), DriverError> { self.simple_query("COMMIT")?; Ok(()) }
    pub fn rollback(&mut self) -> Result<(), DriverError> { self.simple_query("ROLLBACK")?; Ok(()) }

    pub fn is_healthy(&self) -> bool {
        matches!(self.proto.connection_status(), bsql_postgres_proto::ConnectionStatus::Ready)
    }

    pub fn server_version(&self) -> Option<&str> {
        self.proto.session_params().server_version.as_ref().map(|s| s.as_str())
    }

    pub fn close(mut self) -> Result<(), DriverError> {
        let mut wb = WriteBuf::new();
        match self.proto.terminate(&mut wb) {
            Ok((bytes, _)) => { self.stream.write_all(bytes)?; self.stream.shutdown()?; Ok(()) }
            Err(_) => Err(DriverError::Io(std::io::Error::other("terminate failed"))),
        }
    }
}
