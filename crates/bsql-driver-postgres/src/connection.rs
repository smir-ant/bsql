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

        self.drain_until_idle().await
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

    async fn drain_until_idle(&mut self) -> Result<(), DriverError> {
        loop {
            let event = self.proto.advance_one_frame(&mut self.wb);
            match event {
                FeedEvent::Idle => return Ok(()),
                FeedEvent::NeedMoreBytes => {
                    let n = self.stream.read(&mut self.buf).await?;
                    if n == 0 {
                        return Err(DriverError::Io(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "server closed connection",
                        )));
                    }
                    if self.proto.feed_inbound(&self.buf[..n]).is_err() {
                        return Err(DriverError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "read buffer full",
                        )));
                    }
                }
                FeedEvent::SendBytes(bytes) => {
                    self.stream.write_all(bytes).await?;
                }
                FeedEvent::Deliver(_, _) => {}
                FeedEvent::Notice(_) => {}
                FeedEvent::Fail(_) => {
                    return Err(DriverError::NotReady);
                }
                FeedEvent::Close => {
                    return Err(DriverError::NotReady);
                }
                _ => {}
            }
        }
    }
}
