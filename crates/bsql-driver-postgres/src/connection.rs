use bsql_pg_proto::{
    ActivePhase, FeedEvent, PgProtocol, WriteBuf,
    reply_id::PingKind,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Result of a query — rows + command tag.
#[derive(Debug)]
pub struct QueryResult {
    /// Rows. Each row provides typed column access via `Row::get`.
    pub rows: Vec<Row>,
    /// Command tag (e.g., "SELECT 3", "INSERT 0 1").
    pub command_tag: String,
}

/// A single result row. Column values are raw bytes decoded on access.
#[derive(Debug)]
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
}
use tokio::net::TcpStream;

use crate::config::ConnectConfig;
use crate::error::DriverError;

/// Async PostgreSQL connection.
///
/// Wraps `PgProtocol<ActivePhase>` with a TCP socket and drives
/// the sans-IO state machine via an event-pump loop.
pub struct Connection {
    proto: PgProtocol<ActivePhase>,
    stream: TcpStream,
    wb: WriteBuf,
    buf: Vec<u8>,
}

impl Connection {
    /// Connect to PostgreSQL with Trust auth (no password).
    ///
    /// TCP → StartupMessage → auth handshake → Active.
    pub async fn connect(config: &ConnectConfig) -> Result<Self, DriverError> {
        let addr = format!("{}:{}", config.host, config.port);
        let mut stream = TcpStream::connect(&addr).await?;

        let user = bsql_pg_proto::Ident::try_from_str(&config.user)
            .map_err(|_| DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid user name",
            )))?;
        let database = match &config.database {
            Some(d) => Some(
                bsql_pg_proto::DatabaseName::try_from_str(d)
                    .map_err(|_| DriverError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "invalid database name",
                    )))?,
            ),
            None => None,
        };

        let mut proto = PgProtocol::new();
        let mut wb = WriteBuf::new();

        let reply = proto.next_reply_id::<bsql_pg_proto::reply_id::StartupKind>();

        let credentials = match &config.password {
            Some(pw) => {
                let password = bsql_pg_proto::Password::try_from_str(pw)
                    .map_err(|_| DriverError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "invalid password",
                    )))?;
                bsql_pg_proto::Credentials::ScramPassword(
                    bsql_pg_proto::sensitive::Sensitive::new(password),
                )
            }
            None => bsql_pg_proto::password::Credentials::Trust,
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
            if let bsql_pg_proto::Action::SendBytes(bytes) = action {
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
                    if let bsql_pg_proto::Action::SendBytes(bytes) = action {
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
                    });
                }
                Err(bsql_pg_proto::IntoActiveError::StillConnecting(c)) => {
                    connecting = c;
                }
                Err(bsql_pg_proto::IntoActiveError::Closed(_)) => {
                    return Err(DriverError::NotReady);
                }
            }
        }
    }

    /// Send a Ping and wait for Pong. Verifies the connection is alive.
    pub async fn ping(&mut self) -> Result<(), DriverError> {
        let reply = self.proto.next_reply_id::<PingKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard
            .push_command(
                bsql_pg_proto::push_command::Ping { reply },
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;

        for action in actions.as_slice() {
            if let bsql_pg_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.pump_until_idle(|_, _| {}).await
    }

    /// Execute a Simple Query. Returns the command tag.
    ///
    /// Handles DML (CREATE/INSERT/UPDATE/DELETE) and SELECT (rows
    /// are silently drained). Command tag captures the result.
    pub async fn simple_query(&mut self, sql: &str) -> Result<String, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_pg_proto::reply_id::QueryKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard
            .push_command(
                bsql_pg_proto::push_command::SimpleQuery { sql, reply },
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_pg_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.wb.clear();
        let mut command_tag = String::new();
        self.pump_until_idle(|_id, reply| {
            if let bsql_pg_proto::Reply::QueryComplete(_) = reply {
                // Can't borrow self.proto inside closure — capture tag later.
            }
        }).await?;
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

    /// Execute a query expecting exactly one row. Returns the row.
    pub async fn query_one(&mut self, sql: &str) -> Result<Row, DriverError> {
        let result = self.query(sql).await?;
        if result.rows.is_empty() {
            return Err(DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "query returned no rows",
            )));
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
        let reply = self.proto.next_reply_id::<bsql_pg_proto::reply_id::QueryKind>();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard
            .push_command(
                bsql_pg_proto::push_command::SimpleQuery { sql, reply },
                &mut self.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let bsql_pg_proto::Action::SendBytes(bytes) = action {
                self.stream.write_all(bytes).await?;
            }
        }
        self.wb.clear();

        let mut rows: Vec<Row> = Vec::new();
        let mut command_tag = String::new();

        self.pump_until_idle_with_rows(|_id, reply| {
            if let bsql_pg_proto::Reply::QueryComplete(_) = reply {
            }
        }, &mut rows).await?;

        if let Some(tag) = self.proto.current_command_tag() {
            use core::fmt::Write;
            let _ = write!(command_tag, "{}", tag);
        }

        Ok(QueryResult { rows, command_tag })
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
            Err(_) => Err(DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "write buffer full on terminate",
            ))),
        }
    }

    /// Core event pump: advance frames until Idle. Handles:
    /// - NeedMoreBytes retry (silent dispatches leave unread frames)
    /// - Socket read when buffer truly empty
    /// - SendBytes → socket write
    /// - StreamingRows → iter_rows drain with async feed
    /// - Deliver → callback
    /// - Fail/Close → error
    async fn pump_until_idle(
        &mut self,
        mut on_deliver: impl FnMut(core::num::NonZeroU64, bsql_pg_proto::Reply),
    ) -> Result<(), DriverError> {
        loop {
            let event = self.proto.advance_one_frame(&mut self.wb);
            match event {
                FeedEvent::Idle => return Ok(()),
                FeedEvent::NeedMoreBytes => {
                    // Retry — buffer may have frames after silent dispatch.
                    let retry = self.proto.advance_one_frame(&mut self.wb);
                    match retry {
                        FeedEvent::Idle => return Ok(()),
                        FeedEvent::Deliver(id, reply) => {
                            on_deliver(id, reply);
                            continue;
                        }
                        FeedEvent::SendBytes(bytes) => {
                            self.stream.write_all(bytes).await?;
                            continue;
                        }
                        FeedEvent::StreamingRows => {
                            self.drain_streaming().await?;
                            continue;
                        }
                        FeedEvent::NeedMoreBytes => {
                            if matches!(self.proto.state(),
                                bsql_pg_proto::ActiveState::SimpleQueryStreamingRows { .. }
                                | bsql_pg_proto::ActiveState::BindExecuteStreamingRows { .. })
                            {
                                self.drain_streaming().await?;
                                continue;
                            }
                            self.read_from_socket().await?;
                        }
                        FeedEvent::Fail(_) => {
                            if let Some(&cause) = self.proto.fail_cause() {
                                return Err(DriverError::Protocol(cause));
                            }
                            return Err(DriverError::NotReady);
                        }
                        FeedEvent::Close => return Err(DriverError::NotReady),
                        _ => {}
                    }
                }
                FeedEvent::SendBytes(bytes) => {
                    self.stream.write_all(bytes).await?;
                }
                FeedEvent::Deliver(id, reply) => {
                    on_deliver(id, reply);
                }
                FeedEvent::StreamingRows => {
                    self.drain_streaming().await?;
                }
                FeedEvent::Notice(_) => {}
                FeedEvent::Fail(_) => {
                    // Capture error but continue pumping to drain
                    // the trailing RFQ and return to Idle.
                    // Without this, the connection is stuck in
                    // DrainRfqAfterError and unusable.
                    let err = if let Some(&cause) = self.proto.fail_cause() {
                        DriverError::Protocol(cause)
                    } else {
                        DriverError::NotReady
                    };
                    // Drain until Idle, then return the captured error.
                    loop {
                        let ev = self.proto.advance_one_frame(&mut self.wb);
                        match ev {
                            FeedEvent::Idle => return Err(err),
                            FeedEvent::NeedMoreBytes => {
                                // Retry before socket read (silent dispatch).
                                let ev2 = self.proto.advance_one_frame(&mut self.wb);
                                match ev2 {
                                    FeedEvent::Idle => return Err(err),
                                    FeedEvent::NeedMoreBytes => {
                                        if let Err(e) = self.read_from_socket().await {
                                            return Err(e);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            _ => {}
                        }
                    }
                }
                FeedEvent::Close => return Err(DriverError::NotReady),
                _ => {}
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

    async fn drain_streaming(&mut self) -> Result<(), DriverError> {
        self.proto.iter_rows(&mut self.wb, |stream| {
            loop {
                match stream.col_next() {
                    bsql_pg_proto::ColEvent::EndQuery { .. } => return,
                    bsql_pg_proto::ColEvent::NeedMore => {
                        // NeedMore after EndRow = CC dispatched silently,
                        // RFQ pending. Continue — next col_next dispatches RFQ.
                        continue;
                    }
                    _ => {}
                }
            }
        });
        Ok(())
    }

    async fn pump_until_idle_with_rows(
        &mut self,
        mut on_deliver: impl FnMut(core::num::NonZeroU64, bsql_pg_proto::Reply),
        rows: &mut Vec<Row>,
    ) -> Result<(), DriverError> {
        loop {
            let event = self.proto.advance_one_frame(&mut self.wb);
            match event {
                FeedEvent::Idle => return Ok(()),
                FeedEvent::NeedMoreBytes => {
                    let retry = self.proto.advance_one_frame(&mut self.wb);
                    match retry {
                        FeedEvent::Idle => return Ok(()),
                        FeedEvent::Deliver(id, reply) => {
                            on_deliver(id, reply);
                            continue;
                        }
                        FeedEvent::SendBytes(bytes) => {
                            self.stream.write_all(bytes).await?;
                            continue;
                        }
                        FeedEvent::StreamingRows => {
                            self.collect_streaming(rows).await?;
                            continue;
                        }
                        FeedEvent::NeedMoreBytes => {
                            if matches!(self.proto.state(),
                                bsql_pg_proto::ActiveState::SimpleQueryStreamingRows { .. }
                                | bsql_pg_proto::ActiveState::BindExecuteStreamingRows { .. })
                            {
                                self.collect_streaming(rows).await?;
                                continue;
                            }
                            self.read_from_socket().await?;
                        }
                        FeedEvent::Fail(_) => {
                            if let Some(&cause) = self.proto.fail_cause() {
                                return Err(DriverError::Protocol(cause));
                            }
                            return Err(DriverError::NotReady);
                        }
                        FeedEvent::Close => return Err(DriverError::NotReady),
                        _ => {}
                    }
                }
                FeedEvent::SendBytes(bytes) => {
                    self.stream.write_all(bytes).await?;
                }
                FeedEvent::Deliver(id, reply) => {
                    on_deliver(id, reply);
                }
                FeedEvent::StreamingRows => {
                    self.collect_streaming(rows).await?;
                }
                FeedEvent::Notice(_) => {}
                FeedEvent::Fail(_) => {
                    // Capture error but continue pumping to drain
                    // the trailing RFQ and return to Idle.
                    // Without this, the connection is stuck in
                    // DrainRfqAfterError and unusable.
                    let err = if let Some(&cause) = self.proto.fail_cause() {
                        DriverError::Protocol(cause)
                    } else {
                        DriverError::NotReady
                    };
                    // Drain until Idle, then return the captured error.
                    loop {
                        let ev = self.proto.advance_one_frame(&mut self.wb);
                        match ev {
                            FeedEvent::Idle => return Err(err),
                            FeedEvent::NeedMoreBytes => {
                                // Retry before socket read (silent dispatch).
                                let ev2 = self.proto.advance_one_frame(&mut self.wb);
                                match ev2 {
                                    FeedEvent::Idle => return Err(err),
                                    FeedEvent::NeedMoreBytes => {
                                        if let Err(e) = self.read_from_socket().await {
                                            return Err(e);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            _ => {}
                        }
                    }
                }
                FeedEvent::Close => return Err(DriverError::NotReady),
                _ => {}
            }
        }
    }

    async fn collect_streaming(
        &mut self,
        rows: &mut Vec<Row>,
    ) -> Result<(), DriverError> {
        loop {
            let done = self.proto.iter_rows(&mut self.wb, |stream| {
                let mut current_row: Vec<Option<Vec<u8>>> = Vec::new();
                let mut need_more_count = 0u32;
                loop {
                    match stream.col_next() {
                        bsql_pg_proto::ColEvent::Got { bytes, .. } => {
                            current_row.push(Some(bytes.to_vec()));
                            need_more_count = 0;
                        }
                        bsql_pg_proto::ColEvent::Null { .. } => {
                            current_row.push(None);
                            need_more_count = 0;
                        }
                        bsql_pg_proto::ColEvent::EndRow => {
                            rows.push(Row { columns: core::mem::take(&mut current_row) });
                            need_more_count = 0;
                        }
                        bsql_pg_proto::ColEvent::EndQuery { .. } => return true,
                        bsql_pg_proto::ColEvent::NeedMore => {
                            need_more_count = need_more_count.saturating_add(1);
                            if need_more_count > 2 {
                                return false;
                            }
                            continue;
                        }
                    _ => {}
                }
            }
        });
            if done {
                return Ok(());
            }
            // RowStream::Drop fired but we exited on NeedMore
            // (buffer truly empty). Read more bytes and retry.
            // Note: Drop may have errored the proto — but
            // feed_inbound + next iter_rows will see the Errored state
            // and return EndQuery immediately.
            self.read_from_socket().await?;
        }
    }
}
