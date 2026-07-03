//! Active-phase verb-surface behavioural spec.
//!
//! Drives each of the 14 token-threading verbs (the session-ending `terminate`
//! has its own spec, `engine_terminate_spec`) over a scripted (always-ready)
//! transport via the synchronous single-poll helper, asserting the verb reaches
//! the right boundary and surfaces the right results. Each returns the linear
//! token inside an `Outcome { live, status }` on an alive boundary — a clean
//! `Completed`, or a recoverable `ServerErrored` (the connection survived). The
//! row-count guards
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
    poll_once, session, Boundary, CommandStatus, Engine, EngineError, ExpectedRowCount, Live,
    NoObserver, NotifyStatus, Outcome, PreparedStatement, SpuriousPending, Surface, Transport,
};
use bsql_postgres_proto::wire::{
    TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_BIND_COMPLETE, TAG_COMMAND_COMPLETE,
    TAG_COPY_DATA, TAG_COPY_DONE, TAG_COPY_IN_RESPONSE, TAG_COPY_OUT_RESPONSE, TAG_DATA_ROW,
    TAG_ERROR_RESPONSE, TAG_NOTIFICATION_RESPONSE, TAG_NO_DATA,
    TAG_PARAMETER_DESCRIPTION, TAG_PARSE_COMPLETE, TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};
use bsql_postgres_proto::prepared::new_prepared_query;
use bsql_postgres_proto::{Credentials, Ident, PreparedQuery, StmtName};

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

fn copy_out_response() -> Vec<u8> {
    // overall format = 0 (text), num columns = 0.
    frame(TAG_COPY_OUT_RESPONSE.byte(), &[0, 0, 0])
}

fn copy_data(bytes: &[u8]) -> Vec<u8> {
    frame(TAG_COPY_DATA.byte(), bytes)
}

fn copy_done() -> Vec<u8> {
    frame(TAG_COPY_DONE.byte(), &[])
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
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
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

/// A read error carrying a would-block classification — the seam
/// `Transport::is_would_block` reads to decide a quiet deadline vs a fatal
/// failure. A struct (not `Infallible`) so the scripted server can actually
/// PRODUCE an error to exercise the would-block→Quiet path.
#[derive(Debug)]
struct ClassifiedReadErr {
    would_block: bool,
}

/// One scripted read step: serve bytes, or fail with a classified error.
enum ReadStep {
    Bytes(Vec<u8>),
    Fail { would_block: bool },
}

/// A transport whose reads follow a script of byte-serves and classified
/// failures, so the `recv_notification` would-block→Quiet (and fatal→Err) paths
/// are deterministic over a single-poll drive. write/flush/shutdown succeed.
struct PhasedReadServer {
    steps: std::collections::VecDeque<ReadStep>,
}

impl Transport for PhasedReadServer {
    type Error = ClassifiedReadErr;

    fn is_would_block(err: &Self::Error) -> bool {
        err.would_block
    }

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send + 'a {
        let result = match self.steps.front_mut() {
            Some(ReadStep::Bytes(bytes)) => {
                let n = bytes.len().min(buf.len());
                if let (Some(dst), Some(src)) = (buf.get_mut(..n), bytes.get(..n)) {
                    dst.copy_from_slice(src);
                }
                if n == bytes.len() {
                    self.steps.pop_front();
                } else {
                    bytes.drain(..n);
                }
                Ok(n)
            }
            Some(ReadStep::Fail { would_block }) => {
                let would_block = *would_block;
                self.steps.pop_front();
                Err(ClassifiedReadErr { would_block })
            }
            // Script exhausted ⇒ EOF.
            None => Ok(0),
        };
        ready(result)
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send + 'a {
        ready(Ok(buf.len()))
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
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

/// Flatten a single-poll [`Outcome`] verb result to the threaded token,
/// asserting a clean [`CommandStatus::Completed`]. A `ServerErrored` outcome or
/// any `EngineError` surfaces as an `Err` so the call site's `.expect(..)` fails
/// loudly — the helper is for tests that expect a clean completion.
fn flatten<'b>(
    polled: Result<Result<Outcome<'b, CommandStatus>, EngineError<Infallible>>, SpuriousPending>,
) -> Result<Live<'b>, EngineError<Infallible>> {
    match polled {
        Ok(Ok(Outcome {
            live,
            status: CommandStatus::Completed,
        })) => Ok(live),
        Ok(Ok(Outcome {
            status: CommandStatus::ServerErrored,
            ..
        })) => Err(EngineError::ServerError),
        Ok(Err(e)) => Err(e),
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
            // `connect` is the handshake verb — it returns the bare `Live`, not an
            // `Outcome` (no recoverable-server-error axis at handshake time).
            let live = match poll_once(engine.connect(live)) {
                Ok(Ok(live)) => live,
                Ok(Err(e)) => panic!("connect failed: {e:?}"),
                Err(SpuriousPending) => panic!("blocking transport returned Pending"),
            };
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
fn prepare_large_sql_does_not_overflow() {
    // A >2 KiB prepared SQL: the Parse path streams the SQL onto the send buffer
    // (build_parse_header + body), so it must NOT fail with FrameTooLong the way
    // the old whole-frame build_parse would. The reply is a normal prepare
    // completion; the verb reaches a clean Completed outcome.
    let big_sql = "SELECT id, name FROM t WHERE id = $1 -- ".to_string() + &"x".repeat(3000);
    let script = concat(&[
        handshake(),
        parse_complete(),
        parameter_description(&[23]),
        row_description(&[("id", 23), ("name", 25)]),
        rfq(b'I'),
    ]);
    let (completed, oids) = run(script, |e, live| {
        let name = StmtName::try_from_str("big").expect("stmt name");
        let mut cap = Cap::default();
        let outcome = poll_once(e.prepare(live, &name, &big_sql, cap.sink()));
        let completed = matches!(
            outcome,
            Ok(Ok(Outcome {
                status: CommandStatus::Completed,
                ..
            }))
        );
        let oids = match cap.delivers.first() {
            Some(deliver) => deliver.1.clone(),
            None => Vec::new(),
        };
        (completed, oids)
    });
    assert!(completed, "a >2 KiB prepared SQL must prepare without FrameTooLong");
    assert_eq!(oids, vec![23, 25], "the recovered schema must still surface");
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

// ── Demo prepared query, built through the sole validating constructor ──
//
// `new_prepared_query` is the ONLY way to mint a `PreparedQuery` (the seal —
// there is no unchecked twin). In consumer crates the compile-checked `query!`
// macro routes its `const` expansion through it; this proto-level engine test
// has no migration catalog, so it hands the constructor the wire bytes for the
// same demo query directly. The `build_parse_template` / `build_bind_prefix`
// helpers below re-derive the exact PG frame layout, and the constructor's
// const validator cross-checks the baked OID section against the declared
// parameter tuple — so a drifted template is a build error, not a silent lie.
// The statement name is the SHA-256-96 content address of the SQL (identical to
// what the query macro bakes), keeping the driven wire byte-for-byte authentic.
const DEMO_SQL: &str = "SELECT id::int4, name::text FROM demo WHERE id = $1::int4";
const DEMO_STMT: &str = "bsql_p_a6ff70d2d94bc34772d4a4ba";
const DEMO_PARAM_OIDS: &[u32] = &[23];
const DEMO_ROW_OIDS: &[u32] = &[23, 25];
const DEMO_PARSE_LEN: usize =
    1 + 4 + DEMO_STMT.len() + 1 + DEMO_SQL.len() + 1 + 2 + 4 * DEMO_PARAM_OIDS.len();
const DEMO_PARSE: [u8; DEMO_PARSE_LEN] =
    build_parse_template::<DEMO_PARSE_LEN>(DEMO_STMT, DEMO_SQL, DEMO_PARAM_OIDS);
const DEMO_BIND_LEN: usize = 1 + DEMO_STMT.len() + 1;
const DEMO_BIND: [u8; DEMO_BIND_LEN] = build_bind_prefix::<DEMO_BIND_LEN>(DEMO_STMT);

static Q_DEMO: PreparedQuery<(i32,), (i32, &'static str)> =
    new_prepared_query::<(i32,), (i32, &'static str)>(
        DEMO_SQL,
        DEMO_STMT,
        DEMO_PARAM_OIDS,
        DEMO_ROW_OIDS,
        &DEMO_PARSE,
        &DEMO_BIND,
    );

/// Re-derive the PG `Parse`-frame template bytes for a statement:
/// `b'P' | len_i32_be | stmt\0 | sql\0 | n_params_i16_be | oid_i32_be × n`.
/// The length field is self-inclusive (covers everything after the tag byte).
const fn build_parse_template<const N: usize>(stmt: &str, sql: &str, oids: &[u32]) -> [u8; N] {
    let mut buf = [0u8; N];
    let stmt_b = stmt.as_bytes();
    let sql_b = sql.as_bytes();
    let len_be = ((N - 1) as u32).to_be_bytes();
    buf[0] = b'P';
    buf[1] = len_be[0];
    buf[2] = len_be[1];
    buf[3] = len_be[2];
    buf[4] = len_be[3];
    let mut i = 5;
    let mut j = 0;
    while j < stmt_b.len() {
        buf[i] = stmt_b[j];
        i += 1;
        j += 1;
    }
    buf[i] = 0;
    i += 1;
    j = 0;
    while j < sql_b.len() {
        buf[i] = sql_b[j];
        i += 1;
        j += 1;
    }
    buf[i] = 0;
    i += 1;
    let n_be = (oids.len() as u16).to_be_bytes();
    buf[i] = n_be[0];
    i += 1;
    buf[i] = n_be[1];
    i += 1;
    j = 0;
    while j < oids.len() {
        let ob = oids[j].to_be_bytes();
        buf[i] = ob[0];
        buf[i + 1] = ob[1];
        buf[i + 2] = ob[2];
        buf[i + 3] = ob[3];
        i += 4;
        j += 1;
    }
    buf
}

/// Re-derive the `Bind`-frame prefix bytes: `empty_portal_NUL | stmt\0`. The
/// param format block, values, and result-format trailer are appended by the
/// engine at frame-build time from the argument tuple's `ParamsWriter`.
const fn build_bind_prefix<const N: usize>(stmt: &str) -> [u8; N] {
    let mut buf = [0u8; N];
    let stmt_b = stmt.as_bytes();
    // buf[0] is the empty-portal NUL (already 0).
    let mut i = 1;
    let mut j = 0;
    while j < stmt_b.len() {
        buf[i] = stmt_b[j];
        i += 1;
        j += 1;
    }
    // Final byte is the stmt-name NUL (already 0).
    buf
}

#[test]
fn query_params_runs_the_macro_path() {
    // First use is a cache MISS: Close + Parse + Bind + Execute + Sync →
    // CloseComplete, ParseComplete, BindComplete, DataRow, CommandComplete, RFQ.
    let script = concat(&[
        handshake(),
        close_complete(),
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
fn query_params_break_streams_all_reaches_idle() {
    // MISS wire, then 3 DataRows. The breakable sink never breaks, so the pump
    // reaches a clean Idle — the streaming peer's completion boundary.
    let script = concat(&[
        handshake(),
        close_complete(),
        parse_complete(),
        bind_complete(),
        data_row(&[Some(b"1"), Some(b"a")]),
        data_row(&[Some(b"2"), Some(b"b")]),
        data_row(&[Some(b"3"), Some(b"c")]),
        command_complete("SELECT 3"),
        rfq(b'I'),
    ]);
    let rows = run(script, |e, live| {
        let mut rows = 0usize;
        let outcome = poll_once(e.query_params_break(live, &Q_DEMO, (42_i32,), |s| {
            if matches!(s, Surface::Row(_)) {
                rows += 1;
            }
            ControlFlow::<()>::Continue(())
        }));
        match outcome {
            Ok(Ok(Outcome { live, status })) => {
                assert!(
                    matches!(status, Boundary::Idle),
                    "a fully-streamed result reaches Idle, got {status:?}"
                );
                let _ = live;
            }
            other => panic!("expected Ok(Idle), got {other:?}"),
        }
        rows
    });
    assert_eq!(rows, 3, "every row streamed to the sink");
}

#[test]
fn query_params_break_stops_early_then_drain_reclaims_and_reuses() {
    // MISS wire + 3 DataRows + CommandComplete + first RFQ; then a SECOND RFQ for
    // the follow-up ping that proves the connection is reusable after the drain.
    let script = concat(&[
        handshake(),
        close_complete(),
        parse_complete(),
        bind_complete(),
        data_row(&[Some(b"1"), Some(b"a")]),
        data_row(&[Some(b"2"), Some(b"b")]),
        data_row(&[Some(b"3"), Some(b"c")]),
        command_complete("SELECT 3"),
        rfq(b'I'),
        rfq(b'I'),
    ]);
    let (seen, drained, reused) = run(script, |e, live| {
        // Break on the FIRST row.
        let mut seen = 0usize;
        let outcome = poll_once(e.query_params_break(live, &Q_DEMO, (42_i32,), |s| {
            if matches!(s, Surface::Row(_)) {
                seen += 1;
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }));
        let live = match outcome {
            Ok(Ok(Outcome { live, status })) => {
                assert!(
                    matches!(status, Boundary::Stopped(())),
                    "an early break reaches Stopped, got {status:?}"
                );
                live
            }
            other => panic!("expected Ok(Stopped), got {other:?}"),
        };
        // The connection is DIRTY: drain the remaining 2 DataRows + CommandComplete
        // + RFQ to a clean idle, sending nothing.
        let live = match poll_once(e.drain(live)) {
            Ok(Ok(Outcome {
                live,
                status: CommandStatus::Completed,
            })) => live,
            other => panic!("drain must reclaim to a clean Completed idle, got {other:?}"),
        };
        let drained = true;
        // Reuse proof: a follow-up ping round-trips on the SAME connection.
        let reused = matches!(
            poll_once(e.ping(live, |_s| ControlFlow::Continue(()))),
            Ok(Ok(Outcome {
                status: CommandStatus::Completed,
                ..
            }))
        );
        (seen, drained, reused)
    });
    assert_eq!(seen, 1, "the sink broke after exactly the first row");
    assert!(drained, "the drain reclaimed the dirty connection");
    assert!(reused, "a follow-up verb succeeds on the drained connection");
}

/// A scripted server that ALSO records every byte the engine writes, into a
/// shared buffer the test reads between calls — so the outbound wire of each
/// `query_params` call can be inspected. Reads drain a fixed script (like
/// [`StaticServer`]); writes append to the shared recorder.
struct RecordingServer {
    inbound: Vec<u8>,
    cursor: usize,
    written: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

impl Transport for RecordingServer {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
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
        // Record synchronously, then yield a ready future (no lock across await).
        self.written.lock().expect("recorder lock").extend_from_slice(buf);
        ready(Ok(buf.len()))
    }
    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

/// The Close-statement frame for a name, built the same way the engine's
/// `frames::build_close_statement` does: `'C' | len | 'S' | name | NUL`.
fn close_statement_frame(name: &str) -> Vec<u8> {
    let mut body = vec![b'S'];
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    frame(b'C', &body)
}

#[test]
fn query_params_miss_close_parses_hit_reuses_and_reparses_after_clear() {
    // The per-connection prepared-statement cache, proven at the byte level:
    //   call 1 (MISS) -> Close + Parse + Bind + Execute + Sync
    //   call 2 (HIT)  -> Bind + Execute + Sync only  (no Close, no Parse)
    //   clear_statement_cache()
    //   call 3 (MISS again) -> Close + Parse + Bind + Execute + Sync  (== call 1)
    // The MISS leads with a Close so the re-Parse is idempotent (a Close of a
    // nonexistent statement is a wire no-op) — this is what eliminates 42P05 in
    // ALL cases, including a name first Parsed inside a since-committed
    // transaction. The HIT skips both Close and Parse, reusing the server plan.
    // The server replies match: MISS -> CloseComplete + ParseComplete + …;
    // HIT -> BindComplete + … (no CloseComplete, no ParseComplete).
    let user = Ident::try_from_str("verbs").expect("ident");
    let inbound = concat(&[
        handshake(),
        // call 1 (MISS): Close+Parse+Bind+Execute -> CloseComplete, ParseComplete, BindComplete, row, CC, RFQ
        close_complete(),
        parse_complete(),
        bind_complete(),
        data_row(&[Some(b"42"), Some(b"alice")]),
        command_complete("SELECT 1"),
        rfq(b'I'),
        // call 2 (HIT): Bind+Execute only -> BindComplete, row, CC, RFQ
        bind_complete(),
        data_row(&[Some(b"42"), Some(b"alice")]),
        command_complete("SELECT 1"),
        rfq(b'I'),
        // call 3 (MISS again, after clear): CloseComplete, ParseComplete, …
        close_complete(),
        parse_complete(),
        bind_complete(),
        data_row(&[Some(b"42"), Some(b"alice")]),
        command_complete("SELECT 1"),
        rfq(b'I'),
    ]);
    let recorder = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let server = RecordingServer {
        inbound,
        cursor: 0,
        written: std::sync::Arc::clone(&recorder),
    };
    // `from` is always a prior snapshot of the (monotonically growing) recorder
    // length, so the range is in-bounds by construction.
    let read_written = |from: usize| -> Vec<u8> {
        recorder.lock().expect("recorder lock")[from..].to_vec()
    };
    let len_written = || recorder.lock().expect("recorder lock").len();

    let (w1, w2, w3) = session(server, &user, None, None, Credentials::Trust, |mut e, live| {
        let live = match poll_once(e.connect(live)) {
            Ok(Ok(live)) => live,
            other => panic!("connect: {other:?}"),
        };
        // Each call's outbound wire is captured by slicing the recorder at call
        // boundaries (the Bind+Execute+Sync tail is byte-identical every call).
        let base = len_written();
        let mut cap1 = Cap::default();
        let live = flatten(poll_once(e.query_params(live, &Q_DEMO, (42_i32,), cap1.sink())))
            .expect("call 1 (miss)");
        let after1 = len_written();
        let w1 = read_written(base);

        let mut cap2 = Cap::default();
        let live = flatten(poll_once(e.query_params(live, &Q_DEMO, (42_i32,), cap2.sink())))
            .expect("call 2 (reuse) must succeed");
        let w2 = read_written(after1);
        let after2 = len_written();

        // Invalidate the cache (the session-reset hook) — call 3 must miss again.
        e.clear_statement_cache();
        let mut cap3 = Cap::default();
        let live = flatten(poll_once(e.query_params(live, &Q_DEMO, (42_i32,), cap3.sink())))
            .expect("call 3 (after clear)");
        let w3 = read_written(after2);
        let _ = live;
        (w1, w2, w3)
    })
    .expect("session assembles");

    let parse_template = Q_DEMO.parse_template_for_test();
    let close_frame = close_statement_frame(Q_DEMO.stmt_name());
    // MISS wire == Close ++ Parse ++ (the HIT's Bind+Execute+Sync tail).
    assert_eq!(
        w1,
        [close_frame.as_slice(), parse_template, w2.as_slice()].concat(),
        "call 1 (miss) == Close ++ Parse template ++ (Bind+Execute+Sync)"
    );
    assert!(w1.starts_with(close_frame.as_slice()), "miss leads with the Close frame");
    assert!(
        w1.windows(parse_template.len()).any(|w| w == parse_template),
        "miss carries the Parse template"
    );
    // HIT wire carries NEITHER a Close nor a Parse — a bare Bind+Execute+Sync.
    assert!(!w2.starts_with(close_frame.as_slice()), "hit must NOT send a Close");
    assert!(
        !w2.windows(parse_template.len()).any(|w| w == parse_template),
        "hit must NOT re-send the Parse (server plan reused)"
    );
    // After clear_statement_cache the next call is a MISS again: call 3 == call 1.
    assert_eq!(w3, w1, "after clear, the wire returns to the miss (Close+Parse+Bind+Execute)");
}

#[test]
fn query_params_reuse_error_evicts_so_next_use_reparses() {
    // EVICT-ON-REUSE-ServerErrored: a recorded statement dropped out of band
    // (DISCARD ALL) makes the next reuse (bare Bind) fail; the name is EVICTED so
    // the call AFTER that is a MISS (Close+Parse) that re-creates it — self-heal,
    // never a persistent poison.
    //   call 1 (MISS)  -> Close+Parse+Bind+Execute, records the name
    //   call 2 (HIT)   -> Bind+Execute, server ErrorResponse (stmt gone) -> evict
    //   call 3 (MISS)  -> Close+Parse+Bind+Execute again (re-created, healed)
    let user = Ident::try_from_str("verbs").expect("ident");
    let inbound = concat(&[
        handshake(),
        // call 1 (MISS): CloseComplete, ParseComplete, BindComplete, row, CC, RFQ
        close_complete(),
        parse_complete(),
        bind_complete(),
        data_row(&[Some(b"42"), Some(b"alice")]),
        command_complete("SELECT 1"),
        rfq(b'I'),
        // call 2 (HIT): the reused statement is gone -> ErrorResponse + RFQ
        error_response("ERROR", "26000", "prepared statement \"x\" does not exist"),
        rfq(b'I'),
        // call 3 (MISS after evict): CloseComplete, ParseComplete, BindComplete, row, CC, RFQ
        close_complete(),
        parse_complete(),
        bind_complete(),
        data_row(&[Some(b"42"), Some(b"alice")]),
        command_complete("SELECT 1"),
        rfq(b'I'),
    ]);
    let recorder = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
    let server = RecordingServer {
        inbound,
        cursor: 0,
        written: std::sync::Arc::clone(&recorder),
    };
    let read_written = |from: usize| -> Vec<u8> {
        recorder.lock().expect("recorder lock")[from..].to_vec()
    };
    let len_written = || recorder.lock().expect("recorder lock").len();

    let (call2_errored, w3, w1) = session(server, &user, None, None, Credentials::Trust, |mut e, live| {
        let live = match poll_once(e.connect(live)) {
            Ok(Ok(live)) => live,
            other => panic!("connect: {other:?}"),
        };
        let base = len_written();
        let mut cap1 = Cap::default();
        let live = flatten(poll_once(e.query_params(live, &Q_DEMO, (42_i32,), cap1.sink())))
            .expect("call 1 (miss) records");
        let after1 = len_written();
        let w1 = read_written(base);

        // call 2 (HIT) hits the dropped statement: a recoverable ServerErrored.
        let mut cap2 = Cap::default();
        let (call2_errored, live) = match poll_once(e.query_params(live, &Q_DEMO, (42_i32,), cap2.sink())) {
            Ok(Ok(Outcome { live, status: CommandStatus::ServerErrored })) => (true, live),
            other => panic!("expected call 2 ServerErrored (dropped stmt), got {other:?}"),
        };
        let after2 = len_written();

        // call 3 must be a MISS (Close+Parse) — the evict healed the cache.
        let mut cap3 = Cap::default();
        let live = flatten(poll_once(e.query_params(live, &Q_DEMO, (42_i32,), cap3.sink())))
            .expect("call 3 (miss, self-healed)");
        let w3 = read_written(after2);
        let _ = (after1, live);
        (call2_errored, w3, w1)
    })
    .expect("session assembles");

    assert!(call2_errored, "the reuse over a dropped statement is a recoverable ServerErrored");
    // call 3 re-creates via Close+Parse (== the original miss wire) — self-heal.
    assert_eq!(w3, w1, "after eviction the next use re-Parses (miss wire), not a poisoned bare Bind");
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
        let mut cap = Cap::default();
        // begin + per-chunk writes + finish — the streaming trio. `begin` and the
        // writes are token-less; `copy_in_finish` returns the token.
        poll_once(e.copy_in_begin("COPY t FROM STDIN"))
            .expect("poll")
            .expect("begin");
        poll_once(e.copy_in_write(b"row1\n"))
            .expect("poll")
            .expect("write1");
        poll_once(e.copy_in_write(b"row2\n"))
            .expect("poll")
            .expect("write2");
        let live = flatten(poll_once(e.copy_in_finish(live, cap.sink()))).expect("finish");
        let _ = live;
        cap
    });
    assert_eq!(cap.delivers.len(), 1);
    assert_eq!(cap.delivers[0].0.as_deref(), Some("COPY 2"));
}

#[test]
fn copy_in_abort_sends_copy_fail_and_recovers() {
    // After CopyFail the server replies ErrorResponse (echoing the reason) + RFQ;
    // the abort drains it to a clean idle and rides the token back in `Ok`.
    let script = concat(&[
        handshake(),
        copy_in_response(),
        error_response("ERROR", "57014", "COPY from stdin failed: client aborted COPY"),
        rfq(b'I'),
    ]);
    let (status, fails) = run(script, |e, live| {
        let mut cap = Cap::default();
        poll_once(e.copy_in_begin("COPY t FROM STDIN"))
            .expect("poll")
            .expect("begin");
        poll_once(e.copy_in_write(b"partial"))
            .expect("poll")
            .expect("write");
        let outcome = poll_once(e.copy_in_abort(live, b"client aborted COPY", cap.sink()))
            .expect("poll")
            .expect("abort");
        // A CopyFail always yields a ServerErrored recovery — the connection is
        // alive (token in Ok), so the abort is a successful reclaim, not a fault.
        (outcome.status, cap.fails)
    });
    assert_eq!(status, CommandStatus::ServerErrored);
    assert_eq!(fails, 1, "the server's abort ErrorResponse surfaced to the sink");
}

#[test]
fn copy_out_streams_each_chunk_then_completes() {
    let script = concat(&[
        handshake(),
        copy_out_response(),
        copy_data(b"row1\n"),
        copy_data(b"row2\n"),
        copy_done(),
        command_complete("COPY 2"),
        rfq(b'I'),
    ]);
    let cap = run(script, |e, live| {
        let mut cap = Cap::default();
        // Continue-only sink (B = Never): the unload streams to a clean idle.
        let outcome = poll_once(e.copy_out(live, "COPY t TO STDOUT", cap.sink()))
            .expect("poll")
            .expect("copy_out");
        assert!(matches!(outcome.status, Boundary::Idle));
        let _ = outcome.live;
        cap
    });
    assert_eq!(cap.copy_data, vec![b"row1\n".to_vec(), b"row2\n".to_vec()]);
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
    let (cap, received) = run(script, |e, live| {
        let mut cap = Cap::default();
        // A notification stops the pull: the outcome is `Received`, and the token
        // rides back in `Ok`.
        let received = match poll_once(e.recv_notification(live, break_on_notify(&mut cap))) {
            Ok(Ok(Outcome {
                live,
                status: NotifyStatus::Received,
            })) => {
                let _ = live;
                true
            }
            _ => false,
        };
        (cap, received)
    });
    assert!(received, "a notification must yield the Received outcome");
    assert_eq!(cap.notifies.len(), 1);
}

#[test]
fn recv_notification_would_block_is_quiet_and_recovers() {
    // The new `is_would_block` seam's main path: a would-block / timed-out read
    // makes `recv_notification` return `Ok(Outcome { Quiet })` (the token rides
    // back, the connection is alive) rather than consuming the token. After a
    // fresh active engine settles at idle, its first `next_event` is `NeedMore`
    // (an empty buffer drives a read — see dispatch_active::drive), so the
    // scripted would-block read IS reached.
    let user = Ident::try_from_str("verbs").expect("ident");
    let steps = std::collections::VecDeque::from(vec![
        ReadStep::Bytes(handshake()),         // connect consumes this
        ReadStep::Fail { would_block: true }, // recv_notification's read times out
        ReadStep::Bytes(rfq(b'I')),           // the follow-up ping's Sync reply
    ]);
    let (quiet, follow_ok) = session(
        PhasedReadServer { steps },
        &user,
        None,
        None,
        Credentials::Trust,
        |mut e, live| {
            let live = match poll_once(e.connect(live)) {
                Ok(Ok(live)) => live,
                other => panic!("connect: {other:?}"),
            };
            let mut cap = Cap::default();
            let (quiet, live) = match poll_once(e.recv_notification(live, break_on_notify(&mut cap)))
            {
                Ok(Ok(Outcome {
                    live,
                    status: NotifyStatus::Quiet,
                })) => (true, live),
                other => panic!("expected Quiet, got {other:?}"),
            };
            // The connection survived the quiet deadline — a follow-up verb works.
            let follow_ok = matches!(
                poll_once(e.ping(live, |_s: Surface<'_>| ControlFlow::Continue(()))),
                Ok(Ok(Outcome {
                    status: CommandStatus::Completed,
                    ..
                }))
            );
            (quiet, follow_ok)
        },
    )
    .expect("session assembles");
    assert!(quiet, "a would-block read must yield NotifyStatus::Quiet");
    assert!(follow_ok, "the connection must stay alive after a quiet deadline");
}

#[test]
fn recv_notification_fatal_read_error_is_err() {
    // Teeth for the seam: a read error that `is_would_block` classifies as NOT a
    // deadline is fatal — `recv_notification` returns `Err`, consuming the token.
    let user = Ident::try_from_str("verbs").expect("ident");
    let steps = std::collections::VecDeque::from(vec![
        ReadStep::Bytes(handshake()),
        ReadStep::Fail { would_block: false }, // a genuine transport failure
    ]);
    let is_fatal_err = session(
        PhasedReadServer { steps },
        &user,
        None,
        None,
        Credentials::Trust,
        |mut e, live| {
            let live = match poll_once(e.connect(live)) {
                Ok(Ok(live)) => live,
                other => panic!("connect: {other:?}"),
            };
            let mut cap = Cap::default();
            matches!(
                poll_once(e.recv_notification(live, break_on_notify(&mut cap))),
                Ok(Err(EngineError::Transport(ClassifiedReadErr { would_block: false })))
            )
        },
    )
    .expect("session assembles");
    assert!(is_fatal_err, "a non-would-block read error must be a fatal Err");
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
    // A recoverable server error is reported via the ALIVE outcome
    // (`Ok(Outcome { status: ServerErrored })`) — the token rides back, the
    // error details rode the sink. The verb itself drained the recovering RFQ.
    let script = concat(&[
        handshake(),
        error_response("ERROR", "42601", "syntax error"),
        rfq(b'I'),
    ]);
    let (is_server_errored, fail_surfaced) = run(script, |e, live| {
        let mut cap = Cap::default();
        let result = poll_once(e.simple_query(live, "SELCT 1", cap.sink()));
        let is_server_errored = matches!(
            result,
            Ok(Ok(Outcome {
                status: CommandStatus::ServerErrored,
                ..
            }))
        );
        (is_server_errored, cap.fails)
    });
    assert!(is_server_errored, "a syntax error must yield the ServerErrored outcome");
    assert_eq!(fail_surfaced, 1);
}

#[test]
fn server_error_returns_token_for_same_connection_followup() {
    // The tier-1 recovery: a recoverable server error returns the linear token
    // IN `Ok` (the verb drained the recovering RFQ to a clean idle itself), so a
    // follow-up command runs on the SAME connection with NO separate token mint —
    // the only token-minting surface is the session constructor.
    let script = concat(&[
        handshake(),
        error_response("ERROR", "42601", "syntax error"),
        rfq(b'I'),
        command_complete("SELECT 1"),
        rfq(b'I'),
    ]);
    let (first_server_errored, follow_ok, follow_delivers) = run(script, |e, live| {
        let mut cap1 = Cap::default();
        // The errored verb hands the token back inside `Ok`.
        let (first_server_errored, live) =
            match poll_once(e.simple_query(live, "SELCT", cap1.sink())) {
                Ok(Ok(Outcome {
                    live,
                    status: CommandStatus::ServerErrored,
                })) => (true, live),
                other => panic!("expected ServerErrored outcome, got {other:?}"),
            };
        // Reuse that very token — no `recover`, no re-mint — for the follow-up.
        let mut cap2 = Cap::default();
        let follow = poll_once(e.simple_query(live, "SELECT 1", cap2.sink()));
        let follow_ok = matches!(
            follow,
            Ok(Ok(Outcome {
                status: CommandStatus::Completed,
                ..
            }))
        );
        (first_server_errored, follow_ok, cap2.delivers.len())
    });
    assert!(first_server_errored, "the syntax error must yield ServerErrored");
    assert!(follow_ok, "the follow-up command must complete on the recovered connection");
    assert_eq!(follow_delivers, 1, "the follow-up command must deliver its result");
}

#[test]
fn teardown_consumes_token_no_resurrect() {
    // A protocol violation (a `BindComplete` with no command in flight) tears the
    // connection down: the verb returns `Err(ProtocolViolation)`, CONSUMING the
    // token (none rides back). With `recover` removed there is no token-minting
    // surface to resurrect a dead connection — the at-most-one-Live invariant is
    // back to tier-1, enforced by the absence of any free mint. A follow-up verb
    // is impossible: the linear token is gone, so this cannot even be expressed
    // (the compile-fail trybuild goldens pin the move-error half).
    let script = concat(&[handshake(), bind_complete()]);
    let is_proto_violation = run(script, |e, live| {
        let mut cap = Cap::default();
        let result = poll_once(e.simple_query(live, "SELECT 1", cap.sink()));
        // The token was moved into `simple_query` and not returned (fatal Err).
        matches!(result, Ok(Err(EngineError::ProtocolViolation)))
    });
    assert!(is_proto_violation, "an out-of-phase BindComplete must tear down (token consumed)");
}

#[test]
fn oversize_row_description_accumulates_and_decodes() {
    // A RowDescription wider than the bounded ingest buffer (READ_BUF_CAP = 4096)
    // is gathered whole via the Sub-C accumulator and parsed — every column's OID
    // and name surfaces, and the row decodes against it. 300 int4 columns is
    // ~7.2 KB, comfortably oversize.
    const N: usize = 300;
    let cols: Vec<(String, i32)> = (0..N).map(|i| (format!("col_{i}"), 23_i32)).collect();
    let col_refs: Vec<(&str, i32)> = cols.iter().map(|(name, oid)| (name.as_str(), *oid)).collect();
    let cells_owned: Vec<Vec<u8>> = (0..N).map(|i| i.to_string().into_bytes()).collect();
    let cells: Vec<Option<&[u8]>> = cells_owned.iter().map(|c| Some(c.as_slice())).collect();
    let script = concat(&[
        handshake(),
        row_description(&col_refs),
        data_row(&cells),
        command_complete("SELECT 1"),
        rfq(b'I'),
    ]);
    let (rows, deliver_oids, deliver_names, row_cells) = run(script, |e, live| {
        let mut cap = Cap::default();
        let outcome = poll_once(e.query(live, "SELECT wide", cap.sink()));
        assert!(matches!(outcome, Ok(Ok(_))), "the wide query must reach a clean Idle");
        let (oids, names) = match cap.delivers.last() {
            Some((_, oids, names)) => (oids.len(), names.len()),
            None => (0, 0),
        };
        let row_cells = match cap.row_cells.last() {
            Some(cells) => cells.len(),
            None => 0,
        };
        (cap.rows, oids, names, row_cells)
    });
    assert_eq!(rows, 1, "exactly one row surfaces");
    assert_eq!(deliver_oids, N, "all {N} column OIDs are recovered from the oversize RowDescription");
    assert_eq!(deliver_names, N, "all {N} column names are recovered");
    assert_eq!(row_cells, N, "the row decodes against all {N} columns");
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
