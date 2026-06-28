//! Active-phase verb-surface behavioural spec.
//!
//! Drives each of the 15 token-threading verbs (the session-ending `terminate`
//! has its own spec, `engine_terminate_spec`) over a scripted (always-ready)
//! transport via the
//! synchronous single-poll helper, asserting the verb reaches the right boundary
//! and surfaces the right results. The row-count guards
//! ([`query_one`](bsql_postgres_proto::engine::Engine::query_one) /
//! [`query_opt`](bsql_postgres_proto::engine::Engine::query_opt)) are exercised at
//! their boundary cases (0/1/2 rows). The use-after-close compile error has its
//! own trybuild golden (`engine_verbs_compile_fail`).

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "test harness — a fixture/verb failure is a loud assertion, the sanctioned test-failure signal"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

use bsql_postgres_proto::engine::{
    poll_once, session, Engine, EngineError, ExpectedRowCount, Live, NoObserver, PreparedStatement,
    SpuriousPending, Surface, Transport,
};
use bsql_postgres_proto::wire::{
    TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_BIND_COMPLETE, TAG_COMMAND_COMPLETE,
    TAG_COPY_IN_RESPONSE, TAG_DATA_ROW, TAG_ERROR_RESPONSE, TAG_NOTIFICATION_RESPONSE, TAG_NO_DATA,
    TAG_PARAMETER_DESCRIPTION, TAG_PARSE_COMPLETE, TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};
use bsql_postgres_proto::{prepared, Credentials, Ident, PreparedQuery, StmtName};

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

/// The canonical trust handshake reply (AuthenticationOk + BackendKeyData +
/// ReadyForQuery), reaching an active session.
fn handshake() -> Vec<u8> {
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    concat(&[
        frame(TAG_AUTHENTICATION.byte(), &0_i32.to_be_bytes()),
        frame(TAG_BACKEND_KEY_DATA.byte(), &key),
        frame(TAG_READY_FOR_QUERY.byte(), b"I"),
    ])
}

fn rfq(status: u8) -> Vec<u8> {
    frame(TAG_READY_FOR_QUERY.byte(), &[status])
}

fn row_description(cols: &[(&str, i32)]) -> Vec<u8> {
    let mut body = i16::try_from(cols.len()).expect("cols").to_be_bytes().to_vec();
    for (i, (name, oid)) in cols.iter().enumerate() {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_i32.to_be_bytes());
        body.extend_from_slice(&i16::try_from(i + 1).expect("idx").to_be_bytes());
        body.extend_from_slice(&oid.to_be_bytes());
        body.extend_from_slice(&(-1_i16).to_be_bytes());
        body.extend_from_slice(&(-1_i32).to_be_bytes());
        body.extend_from_slice(&0_i16.to_be_bytes());
    }
    frame(TAG_ROW_DESCRIPTION.byte(), &body)
}

fn data_row(cells: &[Option<&[u8]>]) -> Vec<u8> {
    let mut body = i16::try_from(cells.len()).expect("cells").to_be_bytes().to_vec();
    for cell in cells {
        match cell {
            None => body.extend_from_slice(&(-1_i32).to_be_bytes()),
            Some(b) => {
                body.extend_from_slice(&i32::try_from(b.len()).expect("len").to_be_bytes());
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

fn parse_complete() -> Vec<u8> {
    frame(TAG_PARSE_COMPLETE.byte(), &[])
}

fn bind_complete() -> Vec<u8> {
    frame(TAG_BIND_COMPLETE.byte(), &[])
}

fn close_complete() -> Vec<u8> {
    // CloseComplete tag '3' (not re-exported as a const; the byte is the wire).
    frame(b'3', &[])
}

fn parameter_description(oids: &[i32]) -> Vec<u8> {
    let mut body = i16::try_from(oids.len()).expect("params").to_be_bytes().to_vec();
    for oid in oids {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    frame(TAG_PARAMETER_DESCRIPTION.byte(), &body)
}

fn no_data() -> Vec<u8> {
    frame(TAG_NO_DATA.byte(), &[])
}

fn copy_in_response() -> Vec<u8> {
    // overall format = 0 (text), num columns = 0.
    frame(TAG_COPY_IN_RESPONSE.byte(), &[0, 0, 0])
}

fn notification(pid: i32, channel: &str, payload: &str) -> Vec<u8> {
    let mut body = pid.to_be_bytes().to_vec();
    body.extend_from_slice(channel.as_bytes());
    body.push(0);
    body.extend_from_slice(payload.as_bytes());
    body.push(0);
    frame(TAG_NOTIFICATION_RESPONSE.byte(), &body)
}

fn error_response(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for (t, text) in [(b'S', severity), (b'C', sqlstate), (b'M', message)] {
        body.push(t);
        body.extend_from_slice(text.as_bytes());
        body.push(0);
    }
    body.push(0);
    frame(TAG_ERROR_RESPONSE.byte(), &body)
}

// ─────────────────────────── scripted server ───────────────────────────

/// Static cursor server: `read` drains a fixed reply; writes are accepted and
/// discarded; every op resolves synchronously (one-poll).
struct StaticServer {
    inbound: Vec<u8>,
    cursor: usize,
}

impl StaticServer {
    fn new(inbound: Vec<u8>) -> Self {
        Self { inbound, cursor: 0 }
    }
}

impl Transport for StaticServer {
    type Error = Infallible;
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        let n = (self.inbound.len() - self.cursor).min(buf.len());
        let end = self.cursor + n;
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), self.inbound.get(self.cursor..end)) {
            dst.copy_from_slice(src);
        }
        self.cursor = end;
        ready(Ok(n))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(buf.len()))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

// ─────────────────────────── surface capture ───────────────────────────

/// Owned mirror of the surfaces a verb emits.
#[derive(Default)]
struct Cap {
    rows: usize,
    row_cells: Vec<Vec<Option<Vec<u8>>>>,
    delivers: Vec<(Option<String>, Vec<u32>, Vec<String>)>,
    notices: usize,
    notifies: Vec<Vec<u8>>,
    param_statuses: usize,
    copy_data: Vec<Vec<u8>>,
    fails: usize,
}

impl Cap {
    fn sink(&mut self) -> impl FnMut(Surface<'_>) -> ControlFlow<bsql_postgres_proto::engine::Never> + '_ {
        move |surface: Surface<'_>| {
            match surface {
                Surface::Row(body) => {
                    self.rows += 1;
                    self.row_cells.push(parse_row(body));
                }
                Surface::Deliver { tag, oids, names } => self.delivers.push((
                    tag.map(|t| t.to_string()),
                    oids.to_vec(),
                    names.to_vec(),
                )),
                Surface::Notice(_) => self.notices += 1,
                Surface::Notify(body) => self.notifies.push(body.to_vec()),
                Surface::ParamStatus(_) => self.param_statuses += 1,
                Surface::CopyData(body) => self.copy_data.push(body.to_vec()),
                Surface::Fail(_) => self.fails += 1,
                Surface::RowChunk(_) | Surface::RowChunkEnd | Surface::CopyDone => {}
            }
            ControlFlow::Continue(())
        }
    }
}

/// Capture sink for `recv_notification`: breaks (`B = ()`) on the first
/// notification.
fn break_on_notify<'c>(
    cap: &'c mut Cap,
) -> impl FnMut(Surface<'_>) -> ControlFlow<()> + 'c {
    move |surface: Surface<'_>| match surface {
        Surface::Notify(body) => {
            cap.notifies.push(body.to_vec());
            ControlFlow::Break(())
        }
        Surface::Notice(_) => {
            cap.notices += 1;
            ControlFlow::Continue(())
        }
        _ => ControlFlow::Continue(()),
    }
}

fn parse_row(body: &[u8]) -> Vec<Option<Vec<u8>>> {
    let mut cells = Vec::new();
    let Some((count, mut rest)) = body.split_first_chunk::<2>() else {
        return cells;
    };
    let Ok(n) = usize::try_from(i16::from_be_bytes(*count)) else {
        return cells;
    };
    for _ in 0..n {
        let Some((len, after)) = rest.split_first_chunk::<4>() else {
            break;
        };
        rest = after;
        match usize::try_from(i32::from_be_bytes(*len)) {
            Ok(l) => {
                let Some(cell) = rest.get(..l) else { break };
                cells.push(Some(cell.to_vec()));
                let Some(next) = rest.get(l..) else { break };
                rest = next;
            }
            Err(_) => cells.push(None),
        }
    }
    cells
}

// ─────────────────────────── harness ───────────────────────────

fn flatten<'b>(
    polled: Result<Result<Live<'b>, EngineError<Infallible>>, SpuriousPending>,
) -> Result<Live<'b>, EngineError<Infallible>> {
    match polled {
        Ok(inner) => inner,
        Err(SpuriousPending) => panic!("blocking transport returned Pending"),
    }
}

/// Reach an active engine over a scripted server, then run `body`, returning
/// whatever it produces (owned).
fn run<R: 'static>(
    inbound: Vec<u8>,
    body: impl for<'b> FnOnce(&mut Engine<'b, StaticServer, NoObserver>, Live<'b>) -> R,
) -> R {
    let user = Ident::try_from_str("verbs").expect("ident");
    session(
        StaticServer::new(inbound),
        &user,
        None,
        None,
        Credentials::Trust,
        |mut engine, live| {
            let live = flatten(poll_once(engine.connect(live))).expect("connect");
            body(&mut engine, live)
        },
    )
    .expect("session assembles")
}

// ─────────────────────────── per-verb specs ───────────────────────────

#[test]
fn ping_reaches_idle() {
    // Sync → ReadyForQuery.
    let cap = run(concat(&[handshake(), rfq(b'I')]), |e, live| {
        let mut cap = Cap::default();
        let live = flatten(poll_once(e.ping(live, cap.sink()))).expect("ping");
        let _ = live;
        cap
    });
    assert_eq!(cap.rows, 0);
    assert_eq!(cap.delivers.len(), 0);
}

#[test]
fn simple_query_surfaces_rows_then_deliver() {
    let script = concat(&[
        handshake(),
        row_description(&[("n", 23), ("v", 25)]),
        data_row(&[Some(b"1"), Some(b"a")]),
        data_row(&[Some(b"2"), Some(b"b")]),
        command_complete("SELECT 2"),
        rfq(b'I'),
    ]);
    let cap = run(script, |e, live| {
        let mut cap = Cap::default();
        let live = flatten(poll_once(e.simple_query(live, "SELECT n, v FROM t", cap.sink())))
            .expect("simple_query");
        let _ = live;
        cap
    });
    assert_eq!(cap.rows, 2);
    assert_eq!(cap.delivers.len(), 1);
    assert_eq!(cap.delivers[0].0.as_deref(), Some("SELECT 2"));
    assert_eq!(cap.delivers[0].1, vec![23, 25]);
    assert_eq!(cap.delivers[0].2, vec!["n".to_string(), "v".to_string()]);
}

#[test]
fn query_one_accepts_exactly_one_row() {
    let script = concat(&[
        handshake(),
        row_description(&[("n", 23)]),
        data_row(&[Some(b"1")]),
        command_complete("SELECT 1"),
        rfq(b'I'),
    ]);
    let ok = run(script, |e, live| {
        let mut cap = Cap::default();
        let r = poll_once(e.query_one(live, "SELECT n FROM t WHERE id=1", cap.sink()));
        matches!(r, Ok(Ok(_)))
    });
    assert!(ok);
}

#[test]
fn query_one_rejects_zero_rows() {
    let script = concat(&[
        handshake(),
        row_description(&[("n", 23)]),
        command_complete("SELECT 0"),
        rfq(b'I'),
    ]);
    let violation = run(script, |e, live| {
        let mut cap = Cap::default();
        match poll_once(e.query_one(live, "SELECT n FROM t WHERE false", cap.sink())) {
            Ok(Err(EngineError::RowCount(v))) => Some((v.expected, v.got)),
            _ => None,
        }
    });
    assert_eq!(violation, Some((ExpectedRowCount::ExactlyOne, 0)));
}

#[test]
fn query_one_rejects_two_rows() {
    let script = concat(&[
        handshake(),
        row_description(&[("n", 23)]),
        data_row(&[Some(b"1")]),
        data_row(&[Some(b"2")]),
        command_complete("SELECT 2"),
        rfq(b'I'),
    ]);
    let violation = run(script, |e, live| {
        let mut cap = Cap::default();
        match poll_once(e.query_one(live, "SELECT n FROM t", cap.sink())) {
            Ok(Err(EngineError::RowCount(v))) => Some((v.expected, v.got)),
            _ => None,
        }
    });
    assert_eq!(violation, Some((ExpectedRowCount::ExactlyOne, 2)));
}

#[test]
fn query_opt_accepts_zero_and_one_but_rejects_two() {
    let zero = concat(&[
        handshake(),
        row_description(&[("n", 23)]),
        command_complete("SELECT 0"),
        rfq(b'I'),
    ]);
    assert!(run(zero, |e, live| {
        let mut cap = Cap::default();
        matches!(poll_once(e.query_opt(live, "q", cap.sink())), Ok(Ok(_)))
    }));

    let two = concat(&[
        handshake(),
        row_description(&[("n", 23)]),
        data_row(&[Some(b"1")]),
        data_row(&[Some(b"2")]),
        command_complete("SELECT 2"),
        rfq(b'I'),
    ]);
    let violation = run(two, |e, live| {
        let mut cap = Cap::default();
        match poll_once(e.query_opt(live, "q", cap.sink())) {
            Ok(Err(EngineError::RowCount(v))) => Some((v.expected, v.got)),
            _ => None,
        }
    });
    assert_eq!(violation, Some((ExpectedRowCount::AtMostOne, 2)));
}

#[test]
fn execute_surfaces_affected_rows_in_tag() {
    let script = concat(&[handshake(), command_complete("UPDATE 3"), rfq(b'I')]);
    let cap = run(script, |e, live| {
        let mut cap = Cap::default();
        let live = flatten(poll_once(e.execute(live, "UPDATE t SET x=1", cap.sink())))
            .expect("execute");
        let _ = live;
        cap
    });
    assert_eq!(cap.rows, 0);
    assert_eq!(cap.delivers.len(), 1);
    assert_eq!(cap.delivers[0].0.as_deref(), Some("UPDATE 3"));
}

#[test]
fn prepare_surfaces_recovered_schema() {
    // Parse + Describe(statement) + Sync → ParseComplete, ParameterDescription,
    // RowDescription, ReadyForQuery — one Deliver carrying the recovered schema.
    let script = concat(&[
        handshake(),
        parse_complete(),
        parameter_description(&[23]),
        row_description(&[("id", 23), ("name", 25)]),
        rfq(b'I'),
    ]);
    let cap = run(script, |e, live| {
        let name = StmtName::try_from_str("s1").expect("stmt name");
        let mut cap = Cap::default();
        let live = flatten(poll_once(e.prepare(live, &name, "SELECT id, name FROM t", cap.sink())))
            .expect("prepare");
        let _ = live;
        cap
    });
    assert_eq!(cap.delivers.len(), 1);
    assert_eq!(cap.delivers[0].1, vec![23, 25]);
    assert_eq!(cap.delivers[0].2, vec!["id".to_string(), "name".to_string()]);
}

#[test]
fn prepare_nodata_surfaces_empty_row_schema() {
    // A statement returning no rows: Parse + Describe → ParseComplete,
    // ParameterDescription, NoData, ReadyForQuery — one tagless Deliver, no OIDs.
    let script = concat(&[
        handshake(),
        parse_complete(),
        parameter_description(&[23]),
        no_data(),
        rfq(b'I'),
    ]);
    let cap = run(script, |e, live| {
        let name = StmtName::try_from_str("s2").expect("stmt name");
        let mut cap = Cap::default();
        let live = flatten(poll_once(e.prepare(live, &name, "UPDATE t SET x=$1", cap.sink())))
            .expect("prepare");
        let _ = live;
        cap
    });
    assert_eq!(cap.delivers.len(), 1);
    assert!(cap.delivers[0].1.is_empty());
}

#[test]
fn query_prepared_streams_rows() {
    // Bind + Execute + Sync → BindComplete, DataRow, CommandComplete, RFQ.
    let script = concat(&[
        handshake(),
        bind_complete(),
        data_row(&[Some(b"7"), Some(b"x")]),
        command_complete("SELECT 1"),
        rfq(b'I'),
    ]);
    let cap = run(script, |e, live| {
        let stmt = PreparedStatement::new(
            StmtName::try_from_str("s1").expect("stmt"),
            vec![23, 25],
        );
        let mut cap = Cap::default();
        let live = flatten(poll_once(e.query_prepared(live, &stmt, (7_i32,), cap.sink())))
            .expect("query_prepared");
        let _ = live;
        cap
    });
    assert_eq!(cap.rows, 1);
    assert_eq!(cap.delivers.len(), 1);
    // The Execute reply re-sends no RowDescription; OIDs come from the statement.
    assert_eq!(cap.delivers[0].1, vec![23, 25]);
}

#[test]
fn execute_prepared_completes_dml() {
    let script = concat(&[
        handshake(),
        bind_complete(),
        command_complete("DELETE 2"),
        rfq(b'I'),
    ]);
    let cap = run(script, |e, live| {
        let stmt =
            PreparedStatement::new(StmtName::try_from_str("s1").expect("stmt"), Vec::new());
        let mut cap = Cap::default();
        let live = flatten(poll_once(e.execute_prepared(live, &stmt, (1_i32,), cap.sink())))
            .expect("execute_prepared");
        let _ = live;
        cap
    });
    assert_eq!(cap.rows, 0);
    assert_eq!(cap.delivers[0].0.as_deref(), Some("DELETE 2"));
}

static Q_DEMO: PreparedQuery<(i32,), (i32, &'static str)> =
    prepared!("SELECT id::int4, name::text FROM demo WHERE id = $1::int4");

#[test]
fn query_params_runs_the_macro_path() {
    // Parse + Bind + Execute + Sync → ParseComplete, BindComplete, DataRow,
    // CommandComplete, RFQ.
    let script = concat(&[
        handshake(),
        parse_complete(),
        bind_complete(),
        data_row(&[Some(b"42"), Some(b"alice")]),
        command_complete("SELECT 1"),
        rfq(b'I'),
    ]);
    let cap = run(script, |e, live| {
        let mut cap = Cap::default();
        let live = flatten(poll_once(e.query_params(live, &Q_DEMO, (42_i32,), cap.sink())))
            .expect("query_params");
        let _ = live;
        cap
    });
    assert_eq!(cap.rows, 1);
    assert_eq!(cap.delivers.len(), 1);
    // The macro's compile-time row OIDs (int4, text).
    assert_eq!(cap.delivers[0].1, vec![23, 25]);
}

#[test]
fn close_statement_consumes_and_completes() {
    let script = concat(&[handshake(), close_complete(), rfq(b'I')]);
    let ok = run(script, |e, live| {
        let stmt =
            PreparedStatement::new(StmtName::try_from_str("s1").expect("stmt"), Vec::new());
        let mut cap = Cap::default();
        let r = poll_once(e.close_statement(live, stmt, cap.sink()));
        matches!(r, Ok(Ok(_)))
    });
    assert!(ok);
}

#[test]
fn copy_in_streams_data_and_completes() {
    let script = concat(&[
        handshake(),
        copy_in_response(),
        command_complete("COPY 2"),
        rfq(b'I'),
    ]);
    let cap = run(script, |e, live| {
        let chunks: [&[u8]; 2] = [b"row1\n", b"row2\n"];
        let mut it = chunks.into_iter();
        let mut cap = Cap::default();
        let live = flatten(poll_once(e.copy_in(
            live,
            "COPY t FROM STDIN",
            || it.next(),
            cap.sink(),
        )))
        .expect("copy_in");
        let _ = live;
        cap
    });
    assert_eq!(cap.delivers.len(), 1);
    assert_eq!(cap.delivers[0].0.as_deref(), Some("COPY 2"));
}

#[test]
fn listen_subscribes() {
    let script = concat(&[handshake(), command_complete("LISTEN"), rfq(b'I')]);
    let cap = run(script, |e, live| {
        let chan = Ident::try_from_str("events").expect("chan");
        let mut cap = Cap::default();
        let live = flatten(poll_once(e.listen(live, &chan, cap.sink()))).expect("listen");
        let _ = live;
        cap
    });
    assert_eq!(cap.delivers[0].0.as_deref(), Some("LISTEN"));
}

#[test]
fn recv_notification_breaks_on_first_notify() {
    let script = concat(&[handshake(), notification(99, "events", "payload")]);
    let cap = run(script, |e, live| {
        let mut cap = Cap::default();
        let live =
            flatten(poll_once(e.recv_notification(live, break_on_notify(&mut cap)))).expect("recv");
        let _ = live;
        cap
    });
    assert_eq!(cap.notifies.len(), 1);
}

#[test]
fn transaction_begin_body_commit() {
    let script = concat(&[
        handshake(),
        command_complete("BEGIN"),
        rfq(b'T'),
        command_complete("UPDATE 1"),
        rfq(b'T'),
        command_complete("COMMIT"),
        rfq(b'I'),
    ]);
    let committed = run(script, |e, live| {
        let (count, live) = e
            .transaction(live, |engine, live| {
                let mut cap = Cap::default();
                let live = flatten(poll_once(engine.execute(live, "UPDATE t SET x=1", cap.sink())))?;
                Ok((cap.delivers.len(), live))
            })
            .expect("transaction");
        let _ = live;
        count
    });
    assert_eq!(committed, 1);
}

#[test]
fn server_error_is_classified() {
    let script = concat(&[
        handshake(),
        error_response("ERROR", "42601", "syntax error"),
        rfq(b'I'),
    ]);
    let (is_server_err, fail_surfaced) = run(script, |e, live| {
        let mut cap = Cap::default();
        let result = poll_once(e.simple_query(live, "SELCT 1", cap.sink()));
        (
            matches!(result, Ok(Err(EngineError::ServerError))),
            cap.fails,
        )
    });
    assert!(is_server_err);
    assert_eq!(fail_surfaced, 1);
}

#[test]
fn verb_before_connect_is_wrong_phase() {
    // A verb on a still-connecting engine classifies WrongPhase before any I/O.
    let user = Ident::try_from_str("verbs").expect("ident");
    let is_wrong_phase = session(
        StaticServer::new(handshake()),
        &user,
        None,
        None,
        Credentials::Trust,
        |mut engine, live| {
            let mut cap = Cap::default();
            matches!(
                poll_once(engine.simple_query(live, "SELECT 1", cap.sink())),
                Ok(Err(EngineError::WrongPhase(_)))
            )
        },
    )
    .expect("session assembles");
    assert!(is_wrong_phase);
}
