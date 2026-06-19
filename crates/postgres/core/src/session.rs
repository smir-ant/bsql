use std::sync::Arc;

use bsql_postgres_proto::{
    ActivePhase, ConnectingPhase, FeedEvent, PgProtocol, WriteBuf,
};

use crate::config::ConnectConfig;
use crate::error::{DbError, DriverError};
use crate::types::{PreparedStatement, QueryResult, Row};

/// Handshake step result — what the I/O adapter should do next.
pub enum HandshakeAction {
    /// Send bytes to server. Read them from `handshake.pending_bytes()`.
    Send,
    /// Need more bytes from server.
    NeedRead,
    /// Handshake complete — call `handshake.finish()` to get Session.
    Done,
    /// Error during handshake.
    Error(DriverError),
}

/// Pre-connect state machine. Created by `Handshake::begin()`,
/// driven by the I/O adapter via `step()` + `feed()`.
pub struct Handshake {
    connecting: Option<PgProtocol<ConnectingPhase>>,
    wb: WriteBuf,
    buf: Vec<u8>,
    consecutive_need: u32,
    session_parts: Option<(PgProtocol<ActivePhase>, WriteBuf, Vec<u8>)>,
}

impl Handshake {
    /// Start the handshake. Returns startup bytes to send + the Handshake state.
    pub fn begin(config: &ConnectConfig) -> Result<(Vec<u8>, Self), DriverError> {
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

        let (actions, connecting) = proto.push_startup(
            user, database, None, credentials, reply, &mut wb,
        ).map_err(|pf| DriverError::Protocol(*pf.cause))?;

        let startup_bytes: Vec<u8> = actions.as_slice().iter().filter_map(|a| {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = a { Some(*bytes) } else { None }
        }).flatten().copied().collect();

        Ok((startup_bytes, Self {
            connecting: Some(connecting), wb, buf: vec![0u8; 4096],
            consecutive_need: 0, session_parts: None,
        }))
    }

    /// Feed inbound bytes from the server.
    pub fn feed(&mut self, bytes: &[u8]) -> Result<(), DriverError> {
        let conn = self.connecting.as_mut().ok_or(DriverError::NotReady)?;
        conn.feed_inbound(bytes).map_err(|_|
            DriverError::Io(std::io::Error::other("read buffer full"))
        )
    }

    /// Bytes to send after a `Send` action.
    pub fn pending_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// Advance the handshake. Call in a loop with I/O between steps.
    pub fn step(&mut self) -> HandshakeAction {
        loop {
            let Some(ref mut connecting) = self.connecting else {
                return HandshakeAction::Error(DriverError::NotReady);
            };
            let event = connecting.advance_one_frame(&mut self.wb);
            match event {
                FeedEvent::Idle => {
                    self.consecutive_need = 0;
                    let Some(connecting) = self.connecting.take() else {
                        return HandshakeAction::Error(DriverError::NotReady);
                    };
                    match connecting.into_active() {
                        Ok(active) => {
                            let wb = core::mem::replace(&mut self.wb, WriteBuf::new());
                            self.buf = Vec::new();
                            let buf = vec![0u8; 4096];
                            self.session_parts = Some((active, wb, buf));
                            return HandshakeAction::Done;
                        }
                        Err(bsql_postgres_proto::IntoActiveError::StillConnecting(c)) => {
                            self.connecting = Some(c);
                        }
                        Err(_) => return HandshakeAction::Error(DriverError::NotReady),
                    }
                }
                FeedEvent::SendBytes(bytes) => {
                    self.consecutive_need = 0;
                    self.buf.clear();
                    self.buf.extend_from_slice(bytes);
                    return HandshakeAction::Send;
                }
                FeedEvent::NeedMoreBytes => {
                    self.consecutive_need += 1;
                    if self.consecutive_need > 20 {
                        self.consecutive_need = 0;
                        return HandshakeAction::NeedRead;
                    }
                    continue;
                }
                FeedEvent::Fail(_) => return HandshakeAction::Error(
                    DriverError::Io(std::io::Error::other("auth failed"))
                ),
                FeedEvent::Close => return HandshakeAction::Error(DriverError::NotReady),
                _ => { self.consecutive_need = 0; continue; }
            }
        }
    }

    /// Extract the Session after `Done`. Returns Err if handshake not complete.
    pub fn finish(mut self) -> Result<Session, DriverError> {
        let (active, wb, buf) = self.session_parts.take()
            .ok_or(DriverError::NotReady)?;
        Ok(Session::new(active, wb, buf))
    }
}

/// Pump step result — what the I/O adapter should do next.
pub enum PumpAction {
    /// Send bytes to server. Read them from `session.pending_bytes()`.
    Send,
    /// Need more bytes from server. Caller reads and calls `session.feed()`.
    NeedRead,
    /// Pump completed — connection is Ready.
    Done,
    /// Streaming rows — caller should call `session.iter_rows()`.
    Streaming,
    /// Error occurred.
    Error(DriverError),
}

/// Sans-IO session wrapping PgProtocol with all command logic.
/// Both async and sync drivers hold a Session and drive it via
/// the PumpAction state machine.
pub struct Session {
    pub proto: PgProtocol<ActivePhase>,
    pub wb: WriteBuf,
    pub buf: Vec<u8>,
    stmt_counter: u32,
}

impl Session {
    pub fn new(proto: PgProtocol<ActivePhase>, wb: WriteBuf, buf: Vec<u8>) -> Self {
        Self { proto, wb, buf, stmt_counter: 0 }
    }

    pub fn buf_mut(&mut self) -> &mut Vec<u8> { &mut self.buf }

    pub fn feed(&mut self, bytes: &[u8]) -> Result<(), DriverError> {
        self.proto.feed_inbound(bytes).map_err(|_|
            DriverError::Io(std::io::Error::other("read buffer full"))
        )
    }

    pub fn feed_capacity(&self) -> usize {
        self.proto.feed_capacity()
    }

    fn is_streaming(state: &bsql_postgres_proto::ActiveState) -> bool {
        matches!(state,
            bsql_postgres_proto::ActiveState::SimpleQueryStreamingRows { .. }
            | bsql_postgres_proto::ActiveState::BindExecuteStreamingRows { .. }
            | bsql_postgres_proto::ActiveState::BindExecuteAwaitingDataOrCompleteSelect { .. })
    }

    /// One step of the pump loop. Returns what the I/O adapter
    /// should do next. If `Send`, bytes are in `self.pending_bytes()`.
    pub fn pump_step(&mut self) -> PumpAction {
        if Self::is_streaming(self.proto.state()) {
            return PumpAction::Streaming;
        }

        match self.proto.connection_status() {
            bsql_postgres_proto::ConnectionStatus::Ready => return PumpAction::Done,
            bsql_postgres_proto::ConnectionStatus::Errored(_) => {
                return PumpAction::Error(DriverError::NotReady);
            }
            _ => {}
        }

        let actions = self.proto.feed_bytes(&[], &mut self.wb);
        let mut had_fail = false;
        let mut had_close = false;
        let mut has_send = false;
        self.buf.clear();
        for action in actions.as_slice() {
            match action {
                bsql_postgres_proto::Action::SendBytes(bytes) => {
                    self.buf.extend_from_slice(bytes);
                    has_send = true;
                }
                bsql_postgres_proto::Action::FailReply { .. } => { had_fail = true; }
                bsql_postgres_proto::Action::CloseSocket => { had_close = true; }
                _ => {}
            }
        }
        let was_empty = actions.as_slice().is_empty();

        if had_close { return PumpAction::Error(DriverError::NotReady); }
        if had_fail {
            let err = match self.proto.fail_cause() {
                Some(&c) => self.classify_error(c),
                // A failure definitely occurred (FailReply was emitted) but the
                // protocol parked no classified cause. Report that precise
                // condition rather than mislabelling it as "not ready".
                None => DriverError::UnclassifiedFailure,
            };
            // Best-effort recovery to Ready so the connection can be reused.
            // The server error is returned regardless; if the drain did not
            // reach Ready, `is_healthy()` reports false and the pool evicts the
            // connection — the outcome is observable, never silently assumed.
            let _drained_to_ready = self.drain_to_idle();
            return PumpAction::Error(err);
        }

        if has_send {
            return PumpAction::Send;
        }

        if Self::is_streaming(self.proto.state()) {
            return PumpAction::Streaming;
        }

        if was_empty && !matches!(self.proto.connection_status(),
            bsql_postgres_proto::ConnectionStatus::Ready)
        {
            return PumpAction::NeedRead;
        }

        PumpAction::Done
    }

    /// Pump empty bytes to advance toward `Ready` using only data already
    /// buffered in the protocol. Returns `true` if `Ready` was reached. The
    /// caller must treat a `false` result as "connection not drained" (e.g.
    /// mark it unhealthy so a pool evicts it) — never assume a clean drain.
    ///
    /// If a drain step wants to send bytes (this sans-IO Session cannot deliver
    /// them), the drain is reported as incomplete (`false`) rather than claiming
    /// success while silently discarding those bytes.
    #[must_use]
    pub fn drain_to_idle(&mut self) -> bool {
        for _ in 0..10 {
            let actions = self.proto.feed_bytes(&[], &mut self.wb);
            let wants_send = actions.as_slice().iter().any(|a|
                matches!(a, bsql_postgres_proto::Action::SendBytes(_)));
            if wants_send {
                // Bytes need to go to the wire, but draining is I/O-less here.
                // Do not pretend the connection is clean.
                return false;
            }
            if matches!(self.proto.connection_status(),
                bsql_postgres_proto::ConnectionStatus::Ready) { return true; }
        }
        false
    }

    /// Discard a streaming result without materialising rows, consuming only
    /// bytes already buffered in the protocol. Returns `true` when the stream
    /// ended (`EndQuery`); `false` when the protocol needs more bytes that the
    /// caller (which owns the socket) must read and feed before re-entering.
    ///
    /// This never blocks or spins: it is sans-IO, so on `NeedMore` it returns
    /// promptly instead of looping with no progress.
    #[must_use]
    pub fn drain_streaming(&mut self) -> bool {
        self.proto.iter_rows(&mut self.wb, |stream| {
            loop {
                // Snapshot buffered bytes before each step so a `NeedMore` can be
                // classified: a strict decrease means a frame was consumed and
                // more buffered frames remain (e.g. the trailing ReadyForQuery
                // after a just-consumed CommandComplete) — keep draining bytes
                // already in hand. No change means a wire read is genuinely
                // required. Without this, the first `NeedMore` would report
                // "need bytes" while the terminal frames sit unconsumed in the
                // buffer, and the caller's blocking read would stall on a wire
                // that owes nothing.
                let unread_before = stream.unread_len();
                match stream.col_next() {
                    bsql_postgres_proto::ColEvent::EndQuery { .. } => return true,
                    bsql_postgres_proto::ColEvent::NeedMore => {
                        // Strict decrease ⇒ progress on buffered bytes ⇒ re-enter
                        // (no I/O). Bounded: each re-entry consumes ≥1 byte, so
                        // this cannot spin. Otherwise report that more wire bytes
                        // are required; the driver reads and feeds, then re-enters.
                        if stream.unread_len() < unread_before {
                            continue;
                        }
                        return false;
                    }
                    _ => {}
                }
            }
        })
    }

    /// Enter iter_rows — caller provides the closure. Both sync and
    /// async adapters use this, with different NeedMore strategies.
    pub fn iter_rows<R, F>(&mut self, f: F) -> R
    where
        F: for<'p, 'w> FnOnce(&mut bsql_postgres_proto::row_stream::RowStream<'p, 'w>) -> R,
    {
        self.proto.iter_rows(&mut self.wb, f)
    }

    pub fn classify_error(&self, cause: bsql_postgres_proto::ProtocolError) -> DriverError {
        if let bsql_postgres_proto::ProtocolError::ServerErrorResponse {
            severity, code, details_ref,
        } = cause {
            let sev = severity.map(|s| s.as_str().to_string());
            let sqlstate = code.as_str().trim().to_string();
            let (msg, det, hnt) = match self.proto.get_server_error(details_ref) {
                Ok(bsql_postgres_proto::ErrorPayload::ServerError { message, detail, hint }) => {
                    let m = message.as_str().to_string();
                    let d = { let s = detail.as_str(); if s.is_empty() { None } else { Some(s.to_string()) } };
                    let h = { let s = hint.as_str(); if s.is_empty() { None } else { Some(s.to_string()) } };
                    (m, d, h)
                }
                // The server sent an ErrorResponse (we have a SQLSTATE), but the
                // detail payload could not be retrieved as the expected
                // ServerError shape (stale/empty arena, or a non-matching
                // payload variant). Surface a classified marker message rather
                // than a blank string that would masquerade as "the server sent
                // an empty message" — the absence of detail is itself a fact.
                Ok(_) | Err(_) => (
                    String::from("<error message unavailable>"),
                    None,
                    None,
                ),
            };
            return DriverError::Db(DbError { code: sqlstate, severity: sev, message: msg, detail: det, hint: hnt });
        }
        DriverError::Protocol(cause)
    }

    pub fn extract_command_tag(&self) -> String {
        // `ToString` is infallible for the tag's `Display`, so there is no
        // `write!` Result to discard here. No tag observed = empty string.
        match self.proto.current_command_tag() {
            Some(t) => t.to_string(),
            None => String::new(),
        }
    }

    pub fn extract_column_names(&self) -> Arc<[String]> {
        // No RowDescription observed = a command that describes no columns
        // (e.g. an INSERT/UPDATE without RETURNING). An empty name list is the
        // correct typed answer for that case, not a fallback hiding a failure.
        match self.proto.current_column_names() {
            Some(s) => Arc::from(s.to_vec().into_boxed_slice()),
            None => Arc::from(Vec::new().into_boxed_slice()),
        }
    }

    /// The number of rows affected by the most recent command, if the command
    /// reports a row count.
    ///
    /// Reads the typed [`CommandTag`] directly — no string re-parsing, so there
    /// is no parse step that could silently fail. Counted commands
    /// (`INSERT`/`UPDATE`/`DELETE`/`SELECT`/`FETCH`/`MOVE`/`COPY`) yield
    /// `Some(count)` with the server's exact `u64` count. Commands with no
    /// row-count semantics (DDL, transaction control), or no command observed
    /// yet, yield `None` — an honest "no count", never a fabricated zero.
    ///
    /// [`CommandTag`]: bsql_postgres_proto::command_tag::CommandTag
    #[must_use]
    pub fn affected_rows(&self) -> Option<u64> {
        self.proto.current_command_tag().and_then(|t| t.rows())
    }

    /// The number of rows affected by the most recent command, projecting a
    /// command that reports no row count (DDL, transaction control) to `0` —
    /// the SQL-standard "rows affected" for such a statement. This is a typed
    /// projection of [`Self::affected_rows`], not a fallback masking a failure:
    /// the parse cannot fail (the count comes from the typed `CommandTag`), and
    /// the absence of a count is itself a definite answer.
    #[must_use]
    pub fn affected_rows_or_zero(&self) -> u64 {
        // A countless command affected zero rows by SQL definition; the typed
        // `rows_or_zero` projection makes that an exhaustive mapping over the
        // tag variants, not a default-valued fallback. With no command observed
        // yet, zero rows have been affected so far — also a definite answer.
        self.proto.current_command_tag()
            .map_or(0, bsql_postgres_proto::command_tag::CommandTag::rows_or_zero)
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.proto.connection_status(),
            bsql_postgres_proto::ConnectionStatus::Ready)
    }

    pub fn server_version(&self) -> Option<&str> {
        self.proto.session_params().server_version.as_ref().map(|s| s.as_str())
    }

    pub fn backend_pid(&self) -> i32 {
        self.proto.with_cancel_request(|_bytes, pid| pid)
    }

    // --- Command helpers ---

    /// Push a command and collect the bytes to send. Returns them
    /// in `self.buf` (reused across calls to avoid allocation).
    fn push_and_collect(&mut self, cmd: impl bsql_postgres_proto::push_command::PushCommand) -> Result<usize, DriverError> {
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard.push_command(cmd, &mut self.wb)
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        let mut total = 0usize;
        self.buf.clear();
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.buf.extend_from_slice(bytes);
                total += bytes.len();
            }
        }
        self.wb.clear();
        Ok(total)
    }

    /// Bytes to send after the last `push_*` call.
    pub fn pending_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub fn push_simple_query(&mut self, sql: &str) -> Result<usize, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
        self.push_and_collect(bsql_postgres_proto::push_command::SimpleQuery::new(sql, reply))
    }

    pub fn push_ping(&mut self) -> Result<usize, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::PingKind>();
        self.push_and_collect(bsql_postgres_proto::push_command::Ping::new(reply))
    }

    pub fn push_prepare(&mut self, sql: &str) -> Result<(usize, bsql_postgres_proto::StmtName), DriverError> {
        let id = self.stmt_counter;
        self.stmt_counter = self.stmt_counter.wrapping_add(1);
        let stmt_name = bsql_postgres_proto::StmtName::try_from_str(&format!("_bsql_{id}"))
            .map_err(|_| DriverError::Config("generated stmt name invalid"))?;
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::ParseKind>();
        let n = self.push_and_collect(bsql_postgres_proto::push_command::Parse::new(stmt_name, sql, reply))?;
        Ok((n, stmt_name))
    }

    pub fn push_describe_statement(&mut self, stmt_name: bsql_postgres_proto::StmtName) -> Result<usize, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::DescribeStatementKind>();
        self.push_and_collect(bsql_postgres_proto::push_command::DescribeStatement { stmt_name, reply })
    }

    pub fn push_close_statement(&mut self, stmt: PreparedStatement) -> Result<usize, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::CloseKind>();
        self.push_and_collect(bsql_postgres_proto::push_command::CloseStatement { stmt_name: stmt.stmt_name, reply })
    }

    pub fn push_bind_execute<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, stmt: &PreparedStatement, params: &P,
    ) -> Result<usize, DriverError> {
        let reply = self.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
        let portal = bsql_postgres_proto::PortalName::default();
        let guard = self.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard.push_bind_execute(
            &portal, &stmt.stmt_name, params, stmt.row_desc.clone(),
            bsql_postgres_proto::FetchRows::All, reply, &mut self.wb,
        ).map_err(|pf| DriverError::Protocol(*pf.cause))?;
        let mut total = 0usize;
        self.buf.clear();
        for action in actions.as_slice() {
            if let bsql_postgres_proto::Action::SendBytes(bytes) = action {
                self.buf.extend_from_slice(bytes);
                total += bytes.len();
            }
        }
        self.wb.clear();
        Ok(total)
    }

    pub fn finish_prepare(&self, stmt_name: bsql_postgres_proto::StmtName) -> PreparedStatement {
        let row_desc = match self.proto.current_described_rows() {
            bsql_postgres_proto::DescribedRows::Rows(b) => Some(b.to_owned()),
            bsql_postgres_proto::DescribedRows::NoData => None,
        };
        let column_names = self.extract_column_names();
        PreparedStatement { stmt_name, row_desc, column_names }
    }

    pub fn build_query_result(&self, rows: Vec<Row>) -> QueryResult {
        let column_names = self.extract_column_names();
        let column_count = rows.first().map_or(0, |r| r.len());
        let command_tag = self.extract_command_tag();
        QueryResult { rows, command_tag, column_count, column_names }
    }

    pub fn build_query_result_from_stmt(&self, rows: Vec<Row>, stmt: &PreparedStatement) -> QueryResult {
        let column_names = stmt.column_names.clone();
        let column_count = rows.first().map_or(0, |r| r.len());
        let command_tag = self.extract_command_tag();
        QueryResult { rows, command_tag, column_count, column_names }
    }
}
