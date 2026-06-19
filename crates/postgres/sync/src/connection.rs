use std::io::{Read, Write};
use std::net::TcpStream;

use bsql_postgres_core::{
    ConnectConfig, DriverError, PreparedStatement,
    PumpAction, QueryResult, Row, Session, SslMode,
};
use bsql_postgres_proto::FeedEvent;

enum Stream {
    Plain(TcpStream),
    // Boxed: a rustls TLS stream is far larger than a bare TcpStream; boxing
    // the rare TLS variant keeps `Stream` (and `Connection`) small. The deref
    // is per-syscall, not per-row — negligible.
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
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

/// Map a read I/O error to a classified [`DriverError`].
///
/// A blocking read that fires its configured timeout returns
/// [`std::io::ErrorKind::WouldBlock`] (or `TimedOut` on some platforms). These
/// reads are only ever issued mid-command, after the protocol has drained every
/// buffered byte and is genuinely waiting on the wire — so a timeout here means
/// the server owes a reply that never arrived in time. That is a deadline, not a
/// broken connection, so it maps to [`DriverError::Timeout`] (tier-3 classified)
/// rather than being folded into a generic `Io`. The read loop never issues a
/// read once the protocol has signalled completion, so this timeout cannot fire
/// on the happy path.
fn classify_read_error(e: std::io::Error) -> DriverError {
    match e.kind() {
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => DriverError::Timeout,
        _ => DriverError::Io(e),
    }
}

/// Read into `buf`, classifying a timeout as [`DriverError::Timeout`].
fn read_classify_timeout(stream: &mut Stream, buf: &mut [u8]) -> Result<usize, DriverError> {
    stream.read(buf).map_err(classify_read_error)
}

/// Sync PostgreSQL connection — thin adapter over Session.
pub struct Connection {
    session: Session,
    stream: Stream,
    read_buf: Vec<u8>,
    terminated: bool,
    /// Set when a recovery action (e.g. transaction ROLLBACK) failed and the
    /// connection may be left in an indeterminate state. `is_healthy()` returns
    /// false while poisoned so a pool structurally cannot reuse it.
    poisoned: bool,
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

        let (startup_bytes, mut hs) = bsql_postgres_core::Handshake::begin(config)?;
        stream.write_all(&startup_bytes)?;

        let mut buf = vec![0u8; 4096];
        loop {
            match hs.step() {
                bsql_postgres_core::HandshakeAction::Send => {
                    stream.write_all(hs.pending_bytes())?;
                }
                bsql_postgres_core::HandshakeAction::NeedRead => {
                    let n = stream.read(&mut buf)?;
                    if n == 0 { return Err(DriverError::Io(std::io::Error::other("server closed"))); }
                    hs.feed(&buf[..n])?;
                }
                bsql_postgres_core::HandshakeAction::Done => {
                    let session = hs.finish()?;
                    return Ok(Self { session, stream, read_buf: buf, terminated: false, poisoned: false });
                }
                bsql_postgres_core::HandshakeAction::Error(e) => return Err(e),
            }
        }
    }

    fn negotiate_ssl(tcp: TcpStream, config: &ConnectConfig) -> Result<Stream, DriverError> {
        let (ssl_bytes, ssl_proto) = bsql_postgres_core::ssl::ssl_request_bytes();
        let mut tcp = tcp;
        tcp.write_all(ssl_bytes)?;
        let mut response = [0u8; 1];
        tcp.read_exact(&mut response)?;

        match bsql_postgres_core::ssl::classify_ssl_response(ssl_proto, response[0], config)? {
            bsql_postgres_core::ssl::SslProbe::Accepted { tls_config, server_name } => {
                let tls_conn = rustls::ClientConnection::new(tls_config, server_name)
                    .map_err(|e| DriverError::Io(std::io::Error::other(format!("TLS: {e}"))))?;
                Ok(Stream::Tls(Box::new(rustls::StreamOwned::new(tls_conn, tcp))))
            }
            bsql_postgres_core::ssl::SslProbe::PlainTcp => Ok(Stream::Plain(tcp)),
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
                    // A read is issued only when the protocol has exhausted its
                    // buffered bytes and the exchange is not yet complete, so a
                    // read that times out here is a genuine mid-exchange timeout
                    // (the server owes a reply that never arrived). Classify it
                    // rather than masquerading as a generic I/O error.
                    let n = read_classify_timeout(&mut self.stream, &mut self.read_buf)?;
                    if n == 0 { return Err(DriverError::Io(std::io::Error::other("server closed"))); }
                    self.session.feed(&self.read_buf[..n])?;
                }
                PumpAction::Done => return Ok(()),
                PumpAction::Streaming => {
                    match rows {
                        Some(ref mut r) => self.collect_streaming(r)?,
                        None => {
                            // Discard a streaming result we did not ask for.
                            // `drain_streaming` is sans-IO and consumes every
                            // buffered frame before reporting it needs more, so a
                            // read here is only issued when the wire genuinely owes
                            // bytes; a read-timeout is therefore a classified
                            // mid-exchange Timeout. A clean server close mid-drain
                            // is a stall, not a successful drain.
                            while !self.session.drain_streaming() {
                                let n = read_classify_timeout(&mut self.stream, &mut self.read_buf)?;
                                if n == 0 { return Err(DriverError::StreamStalled); }
                                self.session.feed(&self.read_buf[..n])?;
                            }
                        }
                    }
                }
                PumpAction::Error(e) => {
                    // Best-effort recovery; the drain outcome is observed via the
                    // next is_healthy() check and an unrecovered connection stays
                    // unhealthy for pool eviction. The error is returned anyway.
                    for _ in 0..5 {
                        if self.session.is_healthy() { break; }
                        if let Ok(n) = self.stream.read(&mut self.read_buf)
                            && n > 0
                            && self.session.feed(&self.read_buf[..n]).is_err()
                        {
                            break;
                        }
                        let _reached_ready = self.session.drain_to_idle();
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

        let collected: Result<(), DriverError> = self.session.iter_rows(|rs| {
            // The column schema sizes the arena and identifies each cell. If it
            // is absent, every row would silently decode as 0 columns (all
            // get_* return None). Fail loud rather than return hollow rows.
            let n_cols = match rs.current_row_desc() {
                Some(d) => d.len(),
                None => return Err(DriverError::RowDescriptionMissing),
            };
            let mut ab = bsql_postgres_core::ArenaBuilder::new(n_cols);
            let mut in_chunk = false;
            loop {
                // Snapshot of buffered bytes before this step, used by the
                // `NeedMore` arm to tell a silent state advance (the protocol
                // consumed a frame and can keep draining buffered bytes) apart
                // from a genuine need for more wire bytes (no progress possible
                // from the buffer alone).
                let unread_before = rs.unread_len();
                match rs.col_next() {
                    bsql_postgres_proto::ColEvent::Got { bytes, .. } => {
                        ab.push_value(bytes);
                    }
                    bsql_postgres_proto::ColEvent::Null { .. } => {
                        ab.push_null();
                    }
                    bsql_postgres_proto::ColEvent::EndRow => {
                        in_chunk = false;
                        ab.end_row();
                    }
                    bsql_postgres_proto::ColEvent::EndQuery { .. } => {
                        *rows = ab.finish()?;
                        return Ok(());
                    }
                    bsql_postgres_proto::ColEvent::NeedMore => {
                        // `NeedMore` is raised both when a frame was just consumed
                        // and more complete frames remain buffered (a silent state
                        // advance — e.g. the trailing ReadyForQuery sitting after a
                        // just-consumed CommandComplete), and when the buffer is
                        // exhausted or holds only a partial frame. A strict decrease
                        // in buffered bytes means the former: re-enter `col_next` to
                        // drain the buffer toward its terminal (`EndQuery`) without
                        // touching the socket — issuing a read here would block on a
                        // wire that owes nothing, since the completion bytes are
                        // already in hand. Stopping on the protocol's terminal — not
                        // on a read-timeout — is what keeps the result honest. The
                        // strict-decrease guard also makes re-entry terminating:
                        // each one consumes at least one byte, so the loop cannot
                        // spin on an incomplete frame.
                        if rs.unread_len() < unread_before {
                            continue;
                        }
                        // No progress was possible from the buffer, so more wire
                        // bytes are genuinely required. A read error, a clean
                        // mid-stream EOF, a read-timeout, or a rejected feed all
                        // fail loud (each a distinct classified error) rather than
                        // silently truncating the rows.
                        let cap = feed_cap.max(1).min(read_buf.len());
                        let n = read_classify_timeout(stream, &mut read_buf[..cap])?;
                        if n == 0 { return Err(DriverError::StreamStalled); }
                        if rs.feed(&read_buf[..n]).is_err() {
                            return Err(DriverError::StreamStalled);
                        }
                    }
                    bsql_postgres_proto::ColEvent::Chunk { bytes, .. } => {
                        if in_chunk { ab.extend_last(bytes); } else { ab.push_value(bytes); in_chunk = true; }
                    }
                    bsql_postgres_proto::ColEvent::ChunkEnd { bytes, .. } => {
                        ab.extend_last(bytes);
                        in_chunk = false;
                    }
                    // Forward-compat guard: `ColEvent` is `#[non_exhaustive]`.
                    // An unrecognised event cannot be decoded into a row safely,
                    // so fail loud instead of silently dropping it.
                    _ => return Err(DriverError::StreamStalled),
                }
            }
        });
        collected
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
        self.session.push_simple_query(sql)?;
        self.stream.write_all(self.session.pending_bytes())?;
        self.pump(None)?;
        Ok(self.session.affected_rows_or_zero())
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
        Ok(self.session.affected_rows_or_zero())
    }

    /// Execute a `prepared!` macro query in one Parse + Bind + Execute +
    /// Sync round trip. Params and results are uniformly binary, with the
    /// declared param format derived from the same `ParamsWriter` that
    /// encodes the values. Returns the affected-row count. Use for writes
    /// (`INSERT`/`UPDATE`/`DELETE`) with no `RETURNING` rows.
    pub fn execute_prepared_macro<P, R>(
        &mut self,
        q: &'static bsql_postgres_proto::PreparedQuery<P, R>,
        params: P,
    ) -> Result<u64, DriverError>
    where
        P: bsql_postgres_proto::params::ParamsWriter + 'static,
        R: bsql_postgres_proto::RowDecode + 'static,
    {
        self.session.push_execute_prepared_macro(q, params)?;
        self.stream.write_all(self.session.pending_bytes())?;
        self.pump(None)?;
        Ok(self.session.affected_rows_or_zero())
    }

    pub fn query_params<P: bsql_postgres_proto::params::ParamsWriter>(
        &mut self, sql: &str, params: &P,
    ) -> Result<QueryResult, DriverError> {
        let stmt = self.prepare(sql)?;
        let result = self.query_prepared(&stmt, params);
        // Always attempt the CLOSE so the statement is released. The primary op
        // error dominates by design: if `result` is Err, `result?` returns it
        // and the CLOSE Result is dropped. A CLOSE failure surfaces only when
        // the primary op SUCCEEDED, so it is never lost behind a real failure.
        let close = self.close_statement(stmt);
        let result = result?;
        close?;
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
        let result = self.execute_prepared(&stmt, params);
        // Always attempt the CLOSE, but never drop its Result.
        let close = self.close_statement(stmt);
        let count = result?;
        close?;
        Ok(count)
    }

    pub fn begin(&mut self) -> Result<(), DriverError> { self.simple_query("BEGIN")?; Ok(()) }
    pub fn commit(&mut self) -> Result<(), DriverError> { self.simple_query("COMMIT")?; Ok(()) }
    pub fn rollback(&mut self) -> Result<(), DriverError> { self.simple_query("ROLLBACK")?; Ok(()) }

    /// Execute a closure within a transaction. COMMIT on Ok, ROLLBACK on Err.
    /// Tier-1 safety: transaction boundary = closure scope. No object to leak.
    pub fn transaction<R>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<R, DriverError>,
    ) -> Result<R, DriverError> {
        self.simple_query("BEGIN")?;
        match f(self) {
            Ok(val) => { self.simple_query("COMMIT")?; Ok(val) }
            Err(e) => {
                // The caller's error is the primary cause to return. But a
                // failed ROLLBACK leaves the connection inside an open
                // transaction — poison it so a pool cannot silently reuse a
                // connection carrying a dangling transaction.
                if self.simple_query("ROLLBACK").is_err() {
                    self.poisoned = true;
                }
                Err(e)
            }
        }
    }

    pub fn listen(&mut self, channel: &str) -> Result<(), DriverError> {
        self.simple_query(&format!("LISTEN {channel}"))?; Ok(())
    }

    pub fn unlisten(&mut self, channel: &str) -> Result<(), DriverError> {
        self.simple_query(&format!("UNLISTEN {channel}"))?; Ok(())
    }

    pub fn recv_notification(
        &mut self, timeout: std::time::Duration,
    ) -> Result<Option<bsql_postgres_core::Notification>, DriverError> {
        // A near-MAX timeout would overflow `Instant + Duration` and panic;
        // classify it instead of crashing the thread.
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .ok_or(DriverError::TimeoutOverflow)?;
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
                        self.stream.set_read_timeout(Some(std::time::Duration::from_secs(
                            10)))?;
                        // The frame announced a notification; its payload must
                        // resolve. An unresolvable ref means the event would be
                        // lost silently — fail loud instead of dropping it.
                        let payload = self.session.proto.get_notification(notif_ref)
                            .map_err(|_| DriverError::NotificationUnavailable)?;
                        // A non-UTF-8 NOTIFY payload is surfaced as a
                        // classified error rather than silently rewritten
                        // with Unicode replacement characters.
                        let payload_text = core::str::from_utf8(&payload.payload)
                            .map_err(|_| DriverError::NonUtf8Payload)?;
                        return Ok(Some(bsql_postgres_core::Notification {
                            channel: payload.channel.as_str().to_string(),
                            payload: payload_text.to_string(),
                            pid,
                        }));
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
        if had_fail {
            let err = match self.session.proto.fail_cause() {
                Some(&c) => self.session.classify_error(c),
                // A failure occurred but no classified cause was parked; report
                // that precisely rather than mislabelling it as "not ready".
                None => DriverError::UnclassifiedFailure,
            };
            for _ in 0..5 {
                if self.session.is_healthy() { break; }
                if let Ok(n) = self.stream.read(&mut self.read_buf)
                    && n > 0
                    && self.session.feed(&self.read_buf[..n]).is_err()
                {
                    break;
                }
                let _reached_ready = self.session.drain_to_idle();
            }
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
        Ok(self.session.affected_rows_or_zero())
    }

    pub fn is_healthy(&self) -> bool { !self.poisoned && self.session.is_healthy() }
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

#[cfg(test)]
mod tests {
    use super::classify_read_error;
    use bsql_postgres_core::DriverError;
    use std::io::{Error, ErrorKind};

    #[test]
    fn read_timeout_classifies_as_timeout() {
        // A read-timeout surfaces as WouldBlock (most platforms) or TimedOut.
        // Both must map to the classified Timeout, never a generic Io — a
        // deadline is not a broken connection.
        for kind in [ErrorKind::WouldBlock, ErrorKind::TimedOut] {
            assert!(
                matches!(classify_read_error(Error::from(kind)), DriverError::Timeout),
                "{kind:?} must classify as Timeout",
            );
        }
    }

    #[test]
    fn other_read_errors_stay_io() {
        // A genuine I/O failure is not a timeout and must not be relabelled.
        let e = classify_read_error(Error::from(ErrorKind::ConnectionReset));
        assert!(matches!(e, DriverError::Io(_)), "ConnectionReset must stay Io");
    }
}
