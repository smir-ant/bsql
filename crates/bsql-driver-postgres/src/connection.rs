use bsql_pg_proto::{
    ActivePhase, FeedEvent, PgProtocol, WriteBuf,
    reply_id::PingKind,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

        let (actions, mut connecting) = proto
            .push_startup(
                user,
                database,
                None,
                bsql_pg_proto::password::Credentials::Trust,
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
                    if let Some(&cause) = self.proto.fail_cause() {
                        return Err(DriverError::Protocol(cause));
                    }
                    return Err(DriverError::NotReady);
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
}
