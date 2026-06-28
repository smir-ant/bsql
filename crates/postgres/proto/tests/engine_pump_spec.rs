//! Behavioural gate for the active-phase pump ([`pump_active_to_boundary`]).
//!
//! Drives a scripted [`Transport`] (a read-script of server reply frames plus a
//! write/flush recorder) through the pump to each protocol terminal — `Idle`
//! (a SELECT row stream), `Suspended` (a row-limited Execute → `PortalSuspended`),
//! `Failed` (`ErrorResponse`), and `Closed` (an out-of-phase teardown frame) —
//! and the caller-requested `Stopped` early exit and the classified
//! `UnexpectedEof`. It witnesses the externally-observable behaviour on the
//! default [`NoObserver`]: the sink capturing the rows and the `Deliver`
//! projection read at the delivery, `NeedMore` driving transport reads (under a
//! partial-read script), and the entry flush draining an enqueued request
//! exactly once. The observer-hook firing counts (which need a non-default
//! sealed observer policy, nameable only inside the crate) are covered by a
//! `#[cfg(test)]` unit test in the engine's `pump` module.
//!
//! [`pump_active_to_boundary`]: bsql_postgres_proto::engine::pump_active_to_boundary
//! [`Transport`]: bsql_postgres_proto::engine::Transport
//! [`NoObserver`]: bsql_postgres_proto::engine::NoObserver

#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test helpers (the feed pump, handshake construction, the poll-once driver) use unwrap/expect/panic as the loud failure signal; clippy's allow-in-tests carve-out reaches #[test] fns but not the free helper fns this file factors out"
)]

use core::convert::Infallible;
use core::future::Future;
use core::ops::ControlFlow;

use bsql_postgres_proto::engine::{
    poll_once, pump_active_to_boundary, ActiveEngine, AuthEvent, Boundary, ConnectingEngine,
    EngineError, NoObserver, Observer, SendBuf, Surface, Transport,
};
use bsql_postgres_proto::wire::{
    TAG_BIND_COMPLETE, TAG_COMMAND_COMPLETE, TAG_DATA_ROW, TAG_ERROR_RESPONSE, TAG_NOTICE_RESPONSE,
    TAG_PORTAL_SUSPENDED, TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};
use bsql_postgres_proto::{Credentials, Ident};

// ─────────────────────────── frame builders ───────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn concat(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in parts {
        out.extend_from_slice(p);
    }
    out
}

fn auth_ok() -> Vec<u8> {
    frame(b'R', &0i32.to_be_bytes())
}

fn backend_key(pid: i32, secret: i32) -> Vec<u8> {
    let mut body = pid.to_be_bytes().to_vec();
    body.extend_from_slice(&secret.to_be_bytes());
    frame(b'K', &body)
}

fn ready_for_query(status: u8) -> Vec<u8> {
    frame(TAG_READY_FOR_QUERY.byte(), &[status])
}

fn row_description(cols: &[(&str, i32)]) -> Vec<u8> {
    let mut body = (i16::try_from(cols.len()).expect("col count")).to_be_bytes().to_vec();
    for (i, (name, oid)) in cols.iter().enumerate() {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes());
        body.extend_from_slice(&(i16::try_from(i + 1).expect("col idx")).to_be_bytes());
        body.extend_from_slice(&oid.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
    }
    frame(TAG_ROW_DESCRIPTION.byte(), &body)
}

fn data_row(cells: &[Option<&[u8]>]) -> Vec<u8> {
    let mut body = (i16::try_from(cells.len()).expect("cell count")).to_be_bytes().to_vec();
    for cell in cells {
        match cell {
            None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(b) => {
                body.extend_from_slice(&(i32::try_from(b.len()).expect("cell len")).to_be_bytes());
                body.extend_from_slice(b);
            }
        }
    }
    frame(TAG_DATA_ROW.byte(), &body)
}

fn command_complete(tag: &str) -> Vec<u8> {
    let mut body = tag.as_bytes().to_vec();
    body.push(0);
    frame(TAG_COMMAND_COMPLETE.byte(), &body)
}

fn bind_complete() -> Vec<u8> {
    frame(TAG_BIND_COMPLETE.byte(), &[])
}

fn portal_suspended() -> Vec<u8> {
    frame(TAG_PORTAL_SUSPENDED.byte(), &[])
}

fn error_response(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    frame(TAG_ERROR_RESPONSE.byte(), &diagnostic(severity, sqlstate, message))
}

fn notice(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    frame(TAG_NOTICE_RESPONSE.byte(), &diagnostic(severity, sqlstate, message))
}

fn diagnostic(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for (tag, text) in [(b'S', severity), (b'C', sqlstate), (b'M', message)] {
        body.push(tag);
        body.extend_from_slice(text.as_bytes());
        body.push(0);
    }
    body.push(0);
    body
}

/// The body (the payload after the 5-byte header) of a built frame.
fn body_of(frame_bytes: &[u8]) -> Vec<u8> {
    frame_bytes[5..].to_vec()
}

// ─────────────────────────── scripted transport ───────────────────────────

/// A scripted transport: a read-script (`inbound` bytes the pump reads) plus a
/// write/flush recorder. `read_chunk` caps each read so a multi-read partial
/// delivery can be modelled; `0` means unlimited (one read takes the offered
/// slot). Every operation resolves synchronously (`core::future::ready`), so a
/// future built over it is always-ready and resolves under one [`poll_once`].
struct ScriptedTransport {
    inbound: Vec<u8>,
    in_cursor: usize,
    read_chunk: usize,
    reads: usize,
    writes: Vec<u8>,
    flushes: usize,
}

impl ScriptedTransport {
    fn new(inbound: Vec<u8>, read_chunk: usize) -> Self {
        Self {
            inbound,
            in_cursor: 0,
            read_chunk,
            reads: 0,
            writes: Vec::new(),
            flushes: 0,
        }
    }
}

impl Transport for ScriptedTransport {
    type Error = Infallible;

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        self.reads += 1;
        let avail = self.inbound.len() - self.in_cursor;
        let limit = if self.read_chunk == 0 {
            buf.len()
        } else {
            buf.len().min(self.read_chunk)
        };
        let n = limit.min(avail);
        buf[..n].copy_from_slice(&self.inbound[self.in_cursor..self.in_cursor + n]);
        self.in_cursor += n;
        core::future::ready(Ok(n))
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        self.writes.extend_from_slice(buf);
        core::future::ready(Ok(buf.len()))
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        self.flushes += 1;
        core::future::ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        core::future::ready(Ok(()))
    }
}

// ─────────────────────────── sink capture ───────────────────────────

/// Owned mirror of every [`Surface`] the sink consumes, so assertions can run
/// after the borrow each surface carries has ended.
#[derive(Default)]
struct Captured {
    rows: Vec<Vec<u8>>,
    row_chunks: Vec<Vec<u8>>,
    row_chunk_ends: usize,
    /// `Some(tag)` recorded at the single `Deliver`; the inner `Option` mirrors
    /// the tagless-ack case.
    deliver_tag: Option<Option<String>>,
    deliver_oids: Vec<u32>,
    deliver_names: Vec<String>,
    fails: Vec<Vec<u8>>,
    notices: Vec<Vec<u8>>,
    notifies: Vec<Vec<u8>>,
    param_statuses: Vec<Vec<u8>>,
    copy_data: Vec<Vec<u8>>,
    copy_dones: usize,
}

impl Captured {
    /// A capturing sink that never breaks.
    fn sink(&mut self) -> impl FnMut(Surface<'_>) -> ControlFlow<()> + '_ {
        move |s: Surface<'_>| {
            self.record(s);
            ControlFlow::Continue(())
        }
    }

    fn record(&mut self, s: Surface<'_>) {
        match s {
            Surface::Row(b) => self.rows.push(b.to_vec()),
            Surface::Deliver { tag, oids, names } => {
                self.deliver_tag = Some(tag.map(alloc_to_string));
                self.deliver_oids = oids.to_vec();
                self.deliver_names = names.to_vec();
            }
            Surface::Fail(b) => self.fails.push(b.to_vec()),
            Surface::Notice(b) => self.notices.push(b.to_vec()),
            Surface::Notify(b) => self.notifies.push(b.to_vec()),
            Surface::ParamStatus(b) => self.param_statuses.push(b.to_vec()),
            Surface::RowChunk(b) => self.row_chunks.push(b.to_vec()),
            Surface::RowChunkEnd => self.row_chunk_ends += 1,
            Surface::CopyData(b) => self.copy_data.push(b.to_vec()),
            Surface::CopyDone => self.copy_dones += 1,
        }
    }
}

fn alloc_to_string(tag: &bsql_postgres_proto::command_tag::CommandTag) -> String {
    tag.to_string()
}

// ─────────────────────────── drivers ───────────────────────────

/// Reach an active engine through the canonical trust handshake.
fn active_engine() -> ActiveEngine {
    let user = Ident::try_from_str("corpus").expect("ident");
    let mut send_buf = SendBuf::new();
    let mut conn = ConnectingEngine::start(&mut send_buf, &user, None, None, Credentials::Trust)
        .expect("start handshake");
    let hs = concat(&[auth_ok(), backend_key(4321, 8765), ready_for_query(b'I')]);
    feed_conn(&mut conn, &hs);
    loop {
        match conn.next_auth_event(&mut send_buf) {
            AuthEvent::Ready => break,
            AuthEvent::NeedMore => panic!("handshake exhausted before Ready"),
            AuthEvent::Fail(_) => panic!("handshake failed"),
            AuthEvent::AuthCleartext
            | AuthEvent::AuthMd5 { .. }
            | AuthEvent::AuthSaslContinue(_)
            | AuthEvent::ParamStatus(_) => {}
        }
    }
    conn.into_active().expect("into_active after Ready")
}

fn feed_conn(engine: &mut ConnectingEngine, bytes: &[u8]) {
    let mut fed = 0usize;
    while fed < bytes.len() {
        let remaining = &bytes[fed..];
        let slot = engine.read_slot(remaining.len()).expect("conn slot");
        let n = slot.len().min(remaining.len());
        slot[..n].copy_from_slice(&remaining[..n]);
        engine.commit(n).expect("conn commit");
        fed += n;
    }
}

/// Drive the pump once over the always-ready scripted transport (one
/// [`poll_once`] resolves it). The sink is moved in, so on return its borrow of
/// any captured state has ended.
fn run<O: Observer, S: FnMut(Surface<'_>) -> ControlFlow<()>>(
    active: &mut ActiveEngine,
    transport: &mut ScriptedTransport,
    send_buf: &mut SendBuf,
    obs: &O,
    sink: S,
) -> Result<Boundary<()>, EngineError<Infallible>> {
    // A blocking transport never yields `Pending`, so `SpuriousPending` here is
    // a broken harness — a panic, not the returned protocol `Result`.
    match poll_once(pump_active_to_boundary(active, transport, send_buf, obs, sink)) {
        Ok(result) => result,
        Err(_) => panic!("blocking transport must resolve in a single poll"),
    }
}

// ─────────────────────────── tests ───────────────────────────

/// `Idle` terminal: a SELECT → rows → CommandComplete → ReadyForQuery. Witnesses
/// the sink capturing the rows and the `Deliver` projection (tag/oids/names)
/// read at the delivery, `NeedMore` driving multiple reads (partial-read
/// script), and the entry flush draining an enqueued request exactly once.
#[test]
fn idle_select_drives_rows_deliver_and_flushes_request_once() {
    let mut engine = active_engine();
    let d1 = data_row(&[Some(b"1"), Some(b"alpha")]);
    let d2 = data_row(&[Some(b"2"), None]);
    let reply = concat(&[
        row_description(&[("n", 23), ("v", 25)]),
        d1.clone(),
        d2.clone(),
        command_complete("SELECT 2"),
        ready_for_query(b'I'),
    ]);
    // A small read chunk forces NeedMore to drive many reads.
    let mut transport = ScriptedTransport::new(reply, 5);

    // Enqueue a request so the entry flush has something to drain.
    let request = frame(b'Q', b"SELECT n, v FROM t\0");
    let mut send_buf = SendBuf::new();
    send_buf.enqueue(&request);

    let obs = NoObserver;
    let mut captured = Captured::default();
    let boundary = run(
        &mut engine,
        &mut transport,
        &mut send_buf,
        &obs,
        captured.sink(),
    )
    .expect("no engine error");

    assert_eq!(boundary, Boundary::Idle);

    // Sink captured both rows in order, by their wire bodies.
    assert_eq!(captured.rows, vec![body_of(&d1), body_of(&d2)]);
    // Sink captured the Deliver projection read AT the delivery.
    assert_eq!(captured.deliver_tag, Some(Some("SELECT 2".to_string())));
    assert_eq!(captured.deliver_oids, vec![23, 25]);
    assert_eq!(
        captured.deliver_names,
        vec!["n".to_string(), "v".to_string()]
    );

    // NeedMore drove transport reads (multiple, under the 5-byte chunk).
    assert!(transport.reads >= 2, "reads = {}", transport.reads);

    // Entry flush drained the enqueued request exactly once.
    assert_eq!(transport.writes, request);
    assert_eq!(transport.flushes, 1);
}

/// The default [`NoObserver`] policy compiles and runs through the same flow.
#[test]
fn no_observer_path_compiles_and_runs() {
    let mut engine = active_engine();
    let d = data_row(&[Some(b"x")]);
    let reply = concat(&[
        row_description(&[("c", 25)]),
        d.clone(),
        command_complete("SELECT 1"),
        ready_for_query(b'I'),
    ]);
    let mut transport = ScriptedTransport::new(reply, 0);
    let mut send_buf = SendBuf::new();

    let obs = NoObserver;
    let mut captured = Captured::default();
    let boundary = run(
        &mut engine,
        &mut transport,
        &mut send_buf,
        &obs,
        captured.sink(),
    )
    .expect("no engine error");

    assert_eq!(boundary, Boundary::Idle);
    assert_eq!(captured.rows, vec![body_of(&d)]);
    assert_eq!(captured.deliver_tag, Some(Some("SELECT 1".to_string())));
    // The empty send buffer still flushes exactly once at entry.
    assert_eq!(transport.flushes, 1);
    assert!(transport.writes.is_empty());
}

/// `Suspended` terminal: a row-limited Bind+Execute whose portal hits its cap
/// (`PortalSuspended`). The rows fetched before the pause are surfaced; the
/// boundary is `Suspended`, distinct from a completed `Deliver`.
#[test]
fn suspended_terminal_on_portal_suspended() {
    let mut engine = active_engine();
    // Seat the engine to await the Bind+Execute reply (the issuer's job).
    engine.begin_bind_execute_row_limited(&[23]);

    let d = data_row(&[Some(b"42")]);
    let reply = concat(&[bind_complete(), d.clone(), portal_suspended()]);
    let mut transport = ScriptedTransport::new(reply, 0);
    let mut send_buf = SendBuf::new();

    let obs = NoObserver;
    let mut captured = Captured::default();
    let boundary = run(
        &mut engine,
        &mut transport,
        &mut send_buf,
        &obs,
        captured.sink(),
    )
    .expect("no engine error");

    assert_eq!(boundary, Boundary::Suspended);
    assert_eq!(captured.rows, vec![body_of(&d)]);
    // No completion at a suspend — no Deliver surfaced.
    assert_eq!(captured.deliver_tag, None);
}

/// `Failed` terminal: an `ErrorResponse` surfaces its raw bytes to the sink,
/// then the pump returns `Boundary::Failed`.
#[test]
fn failed_terminal_surfaces_error_then_returns_failed() {
    let mut engine = active_engine();
    let err = error_response("ERROR", "42P01", "relation does not exist");
    let mut transport = ScriptedTransport::new(err.clone(), 0);
    let mut send_buf = SendBuf::new();

    let obs = NoObserver;
    let mut captured = Captured::default();
    let boundary = run(
        &mut engine,
        &mut transport,
        &mut send_buf,
        &obs,
        captured.sink(),
    )
    .expect("no engine error");

    assert_eq!(boundary, Boundary::Failed);
    assert_eq!(captured.fails, vec![body_of(&err)]);
    // No rows or completion surfaced before the failure.
    assert!(captured.rows.is_empty());
    assert_eq!(captured.deliver_tag, None);
}

/// `Closed` terminal: an out-of-phase frame (a `BindComplete` with no Bind in
/// flight) tears the connection down.
#[test]
fn closed_terminal_on_out_of_phase_frame() {
    let mut engine = active_engine();
    // BindComplete at Idle is out-of-phase → classified teardown.
    let teardown = bind_complete();
    let mut transport = ScriptedTransport::new(teardown, 0);
    let mut send_buf = SendBuf::new();

    let obs = NoObserver;
    let mut captured = Captured::default();
    let boundary = run(
        &mut engine,
        &mut transport,
        &mut send_buf,
        &obs,
        captured.sink(),
    )
    .expect("no engine error");

    assert_eq!(boundary, Boundary::Closed);
    assert!(captured.rows.is_empty());
    assert_eq!(captured.deliver_tag, None);
}

/// A sink that returns `ControlFlow::Break` stops the pump early and returns
/// `Boundary::Stopped` — distinct from a clean `Idle`.
#[test]
fn sink_break_stops_early_with_stopped_boundary() {
    let mut engine = active_engine();
    let d1 = data_row(&[Some(b"1")]);
    let d2 = data_row(&[Some(b"2")]);
    let reply = concat(&[
        row_description(&[("n", 23)]),
        d1.clone(),
        d2.clone(),
        command_complete("SELECT 2"),
        ready_for_query(b'I'),
    ]);
    let mut transport = ScriptedTransport::new(reply, 0);
    let mut send_buf = SendBuf::new();

    let obs = NoObserver;
    let mut seen = 0usize;
    let boundary = run(&mut engine, &mut transport, &mut send_buf, &obs, |s| {
        if let Surface::Row(_) = s {
            seen += 1;
            // Stop after the first row.
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    })
    .expect("no engine error");

    assert_eq!(boundary, Boundary::Stopped(()));
    // The sink saw exactly one row before breaking; the pump stopped there.
    assert_eq!(seen, 1);
}

/// A zero-length read while a frame is incomplete (peer closed mid-frame) is
/// classified as `UnexpectedEof`, never looped or treated as a boundary.
#[test]
fn unexpected_eof_is_classified() {
    let mut engine = active_engine();
    // An incomplete frame header: the framing reports NeedMore, the next read
    // returns Ok(0) (script exhausted).
    let mut transport = ScriptedTransport::new(vec![TAG_DATA_ROW.byte(), 0, 0], 0);
    let mut send_buf = SendBuf::new();

    let obs = NoObserver;
    let mut captured = Captured::default();
    let err = run(
        &mut engine,
        &mut transport,
        &mut send_buf,
        &obs,
        captured.sink(),
    )
    .expect_err("incomplete frame + EOF must classify");

    assert!(matches!(err, EngineError::UnexpectedEof), "err = {err:?}");
}

/// An async `NoticeResponse` interleaved with a row stream is surfaced to the
/// sink without disturbing the command-state boundary.
#[test]
fn notice_surfaced_during_select() {
    let mut engine = active_engine();
    let d = data_row(&[Some(b"r")]);
    let notice_frame = notice("NOTICE", "00000", "heads up");
    let reply = concat(&[
        row_description(&[("c", 25)]),
        notice_frame.clone(),
        d.clone(),
        command_complete("SELECT 1"),
        ready_for_query(b'I'),
    ]);
    let mut transport = ScriptedTransport::new(reply, 0);
    let mut send_buf = SendBuf::new();

    let obs = NoObserver;
    let mut captured = Captured::default();
    let boundary = run(
        &mut engine,
        &mut transport,
        &mut send_buf,
        &obs,
        captured.sink(),
    )
    .expect("no engine error");

    assert_eq!(boundary, Boundary::Idle);
    assert_eq!(captured.notices, vec![body_of(&notice_frame)]);
    // The async notice is surfaced separately from the single row + delivery.
    assert_eq!(captured.rows, vec![body_of(&d)]);
    assert_eq!(captured.deliver_tag, Some(Some("SELECT 1".to_string())));
}
