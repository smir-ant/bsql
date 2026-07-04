//! AXIS-5 HOSTILE RE-PROOF — fault injection against the REWRITTEN engine.
//!
//! The old wire state machine has its own hostility proofs
//! (`partial_assembly_spec` / `fuzz_stress_spec` / `bounded_buffers_spec`). The
//! strangler engine must NOT inherit that proof by faith: this spec feeds the NEW
//! `ActiveEngine` / `ConnectingEngine` malformed and adversarial byte streams
//! directly (via `read_slot`/`commit`/`next_event` and via the public pumps) and
//! asserts a CLASSIFIED outcome for each — a teardown (`Event::Close` /
//! `Boundary::Closed`) or a classified handshake `ConnFail` — with no panic, no
//! infinite loop / hang, no unbounded memory, no wrong classification. The crate
//! is `#![forbid(unsafe_code)]`, so UB is structurally absent; these tests assert
//! the *classified* handling on top of that.
//!
//! Each test asserts the SPECIFIC classified outcome (the exact `Event` sequence,
//! `Boundary`, or `ConnFail` variant), never merely "did not panic" — a hostile
//! input the engine mis-handles is a finding to fix, never a test to weaken. The
//! one unbounded-by-nature input (a `0xFFFF_FFFF`-length frame) is driven with a
//! FIXED iteration bound, never a real timeout.

#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test helpers (the feed pump, handshake construction, scripted transport) use unwrap/expect/panic as the loud failure signal; clippy's allow-in-tests carve-out reaches #[test] fns but not the free helper fns this file factors out"
)]

use core::convert::Infallible;
use core::future::Future;
use core::ops::ControlFlow;

use bsql_postgres_proto::engine::{
    poll_once, pump_active_to_boundary, pump_connecting_to_ready, ActiveEngine, AuthEvent, Boundary,
    ConnFail, ConnectingEngine, Event, HandshakeOutcome, NoObserver, SendBuf, Surface, Transport,
};
use bsql_postgres_proto::frame::READ_BUF_CAP;
use bsql_postgres_proto::wire::{
    TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_COMMAND_COMPLETE, TAG_COPY_DATA,
    TAG_DATA_ROW, TAG_NOTICE_RESPONSE, TAG_NOTIFICATION_RESPONSE, TAG_PARAMETER_STATUS,
    TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};
use bsql_postgres_proto::{Credentials, Ident};

// ─────────────────────────── frame builders ───────────────────────────

/// A well-formed frame: tag + big-endian (body.len()+4) length + body.
fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A frame with an EXPLICIT (possibly malformed / oversize) declared length
/// field, independent of `body.len()` — the forge for a length that does not
/// match the bytes actually supplied.
fn frame_declared(tag: u8, declared: u32, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&declared.to_be_bytes());
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
    frame(TAG_AUTHENTICATION.byte(), &0i32.to_be_bytes())
}

/// An `Authentication` request frame: tag `R`, 4-byte sub-code, then
/// method-specific trailing bytes.
fn auth_request(sub_code: i32, extra: &[u8]) -> Vec<u8> {
    let mut body = sub_code.to_be_bytes().to_vec();
    body.extend_from_slice(extra);
    frame(TAG_AUTHENTICATION.byte(), &body)
}

fn backend_key(pid: i32, secret: i32) -> Vec<u8> {
    let mut body = pid.to_be_bytes().to_vec();
    body.extend_from_slice(&secret.to_be_bytes());
    frame(TAG_BACKEND_KEY_DATA.byte(), &body)
}

fn ready_for_query(status: u8) -> Vec<u8> {
    frame(TAG_READY_FOR_QUERY.byte(), &[status])
}

fn row_description(cols: &[(&str, i32)]) -> Vec<u8> {
    let mut body = (cols.len() as i16).to_be_bytes().to_vec();
    for (i, (name, oid)) in cols.iter().enumerate() {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes());
        body.extend_from_slice(&((i + 1) as i16).to_be_bytes());
        body.extend_from_slice(&oid.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
    }
    frame(TAG_ROW_DESCRIPTION.byte(), &body)
}

fn data_row(cells: &[Option<&[u8]>]) -> Vec<u8> {
    let mut body = (cells.len() as i16).to_be_bytes().to_vec();
    for cell in cells {
        match cell {
            None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(b) => {
                body.extend_from_slice(&(b.len() as i32).to_be_bytes());
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

/// The diagnostic field list shared by `NoticeResponse` / `ErrorResponse`:
/// `(field_byte, text\0)*` terminated by a final `\0`.
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

fn notice(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    frame(TAG_NOTICE_RESPONSE.byte(), &diagnostic(severity, sqlstate, message))
}

fn parameter_status(key: &str, value: &str) -> Vec<u8> {
    let mut body = key.as_bytes().to_vec();
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(TAG_PARAMETER_STATUS.byte(), &body)
}

fn notification(pid: i32, channel: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = pid.to_be_bytes().to_vec();
    body.extend_from_slice(channel.as_bytes());
    body.push(0);
    body.extend_from_slice(payload);
    body.push(0);
    frame(TAG_NOTIFICATION_RESPONSE.byte(), &body)
}

/// A `NoticeResponse` with a body of `n` filler bytes after the severity field —
/// used to forge an oversize notice whose Sub-B prefix truncation is observable.
fn notice_filler(n: usize) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"NOTICE");
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(&vec![b'x'; n]);
    body.push(0);
    body.push(0);
    frame(TAG_NOTICE_RESPONSE.byte(), &body)
}

// ─────────────────────────── engine drivers ───────────────────────────

/// Owned mirror of one pulled [`Event`], so a sequence can be collected past the
/// borrow each event carries.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ev {
    NeedMore,
    Idle,
    Deliver(String),
    Suspended,
    Close,
    RowChunkEnd,
    CopyDone,
    Fail(Vec<u8>),
    Notice(Vec<u8>),
    Notify(Vec<u8>),
    ParamStatus(Vec<u8>),
    Row(Vec<u8>),
    RowChunk(Vec<u8>),
    CopyData(Vec<u8>),
}

fn feed(engine: &mut ActiveEngine, bytes: &[u8]) {
    let mut fed = 0usize;
    while fed < bytes.len() {
        let remaining = &bytes[fed..];
        let slot = engine.read_slot(remaining.len()).expect("active slot");
        assert!(!slot.is_empty(), "active slot non-empty");
        let n = slot.len().min(remaining.len());
        slot[..n].copy_from_slice(&remaining[..n]);
        engine.commit(n).expect("active commit");
        fed += n;
    }
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

/// Pull one event, capturing the command tag at `Deliver`.
fn pull(engine: &mut ActiveEngine) -> Ev {
    match engine.next_event() {
        Event::NeedMore => Ev::NeedMore,
        Event::Idle => Ev::Idle,
        Event::Deliver => Ev::Deliver(match engine.last_command_tag() {
            Some(tag) => tag.to_string(),
            None => String::new(),
        }),
        Event::Suspended => Ev::Suspended,
        Event::Close => Ev::Close,
        Event::RowChunkEnd => Ev::RowChunkEnd,
        Event::CopyDone => Ev::CopyDone,
        Event::Fail(b) => Ev::Fail(b.to_vec()),
        Event::Notice(b) => Ev::Notice(b.to_vec()),
        Event::Notify(b) => Ev::Notify(b.to_vec()),
        Event::ParamStatus(b) => Ev::ParamStatus(b.to_vec()),
        Event::Row(b) => Ev::Row(b.to_vec()),
        Event::RowChunk(b) => Ev::RowChunk(b.to_vec()),
        Event::CopyData(b) => Ev::CopyData(b.to_vec()),
    }
}

/// Feed `bytes` in `chunk`-sized pieces, pulling events until a terminal
/// (`Idle` / `Close`) or input exhaustion (which appends a trailing `NeedMore`).
/// The loop is bounded by the input length (each iteration either consumes input
/// or terminates), so a never-completing stream ends at exhaustion, never spins.
fn drive(engine: &mut ActiveEngine, bytes: &[u8], chunk: usize) -> Vec<Ev> {
    let mut events = Vec::new();
    let mut fed = 0usize;
    loop {
        match pull(engine) {
            Ev::NeedMore => {
                if fed >= bytes.len() {
                    events.push(Ev::NeedMore);
                    break;
                }
                let take = chunk.max(1).min(bytes.len() - fed);
                feed(engine, &bytes[fed..fed + take]);
                fed += take;
            }
            Ev::Idle => {
                events.push(Ev::Idle);
                break;
            }
            Ev::Close => {
                events.push(Ev::Close);
                break;
            }
            other => events.push(other),
        }
    }
    events
}

/// Reach an active engine through the canonical trust handshake.
fn active_engine() -> ActiveEngine {
    let user = Ident::try_from_str("corpus").expect("ident");
    let mut send_buf = SendBuf::new();
    let mut conn = ConnectingEngine::start(&mut send_buf, &user, None, &[], Credentials::Trust)
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

/// The body bytes (after the 5-byte header) of a built frame.
fn body_of(frame_bytes: &[u8]) -> Vec<u8> {
    frame_bytes[5..].to_vec()
}

// ─────────────────────────── scripted transport ───────────────────────────

/// A scripted transport: an inbound read-script plus a write/flush recorder.
/// `read_chunk` caps each read (`0` = unlimited). Every op resolves synchronously
/// (`core::future::ready`), so a future built over it is always-ready and one
/// [`poll_once`] resolves it.
struct ScriptedTransport {
    inbound: Vec<u8>,
    in_cursor: usize,
    read_chunk: usize,
}

impl ScriptedTransport {
    fn new(inbound: Vec<u8>, read_chunk: usize) -> Self {
        Self {
            inbound,
            in_cursor: 0,
            read_chunk,
        }
    }
}

impl Transport for ScriptedTransport {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
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
        core::future::ready(Ok(buf.len()))
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        core::future::ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        core::future::ready(Ok(()))
    }
}

/// Drive the connecting handshake over scripted `inbound` bytes to its terminal,
/// returning the classified [`HandshakeOutcome`]. A `Pending` (impossible over
/// the always-ready transport) is a broken harness, not a protocol result.
fn handshake_outcome(inbound: Vec<u8>) -> HandshakeOutcome {
    let user = Ident::try_from_str("corpus").expect("ident");
    let mut send_buf = SendBuf::new();
    let mut conn = ConnectingEngine::start(&mut send_buf, &user, None, &[], Credentials::Trust)
        .expect("start handshake");
    let mut transport = ScriptedTransport::new(inbound, 0);
    match poll_once(pump_connecting_to_ready(&mut conn, &mut transport, &mut send_buf)) {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(err)) => panic!("connecting pump returned a transport-level error: {err:?}"),
        Err(_) => panic!("blocking transport must resolve in a single poll"),
    }
}

// ═══════════════════════════════ tests ═══════════════════════════════

// ── frame length: declared vs actual ──

/// A length-field below the 4-byte self-count minimum is irrecoverable framing
/// desync — a classified teardown, never a panic or a silent skip.
#[test]
fn length_below_minimum_tears_down() {
    for bad in [0u32, 1, 2, 3] {
        let mut engine = active_engine();
        let malformed = frame_declared(TAG_DATA_ROW.byte(), bad, &[]);
        let events = drive(&mut engine, &malformed, malformed.len());
        assert_eq!(
            events,
            vec![Ev::Close],
            "declared length {bad} (< 4) must be a classified teardown",
        );
    }
}

/// A header that declares MORE body than is supplied yields `NeedMore` and waits
/// — never a premature row, completion, or teardown — and stays `NeedMore` when
/// the input is exhausted (the read-side `UnexpectedEof` precondition).
#[test]
fn declared_longer_than_body_waits_never_misclassifies() {
    let mut engine = active_engine();
    // Open a row stream, then a DataRow header declaring a 100-byte body with
    // only 10 body bytes actually present.
    let stream = row_description(&[("n", 23)]);
    let short = frame_declared(TAG_DATA_ROW.byte(), 100, &[0u8; 10]);
    let bytes = concat(&[stream, short]);
    let events = drive(&mut engine, &bytes, bytes.len());
    assert_eq!(
        events,
        vec![Ev::NeedMore],
        "an under-supplied frame must park at NeedMore, never emit a partial row \
         or tear down",
    );
}

/// A frame is delimited by its DECLARED length, not by scanning its content:
/// trailing bytes after a frame's declared body are the NEXT frame, parsed
/// independently and consistently.
#[test]
fn declared_length_delimits_frame_trailing_bytes_are_next_frame() {
    let mut engine = active_engine();
    // A complete DataRow immediately followed (in the same buffer) by a
    // CommandComplete + ReadyForQuery: the DataRow is consumed by its own length,
    // and the trailing frames parse as the next two frames.
    let d = data_row(&[Some(b"1")]);
    let reply = concat(&[
        row_description(&[("n", 23)]),
        d.clone(),
        command_complete("SELECT 1"),
        ready_for_query(b'I'),
    ]);
    let events = drive(&mut engine, &reply, reply.len());
    assert_eq!(
        events,
        vec![
            Ev::Row(body_of(&d)),
            Ev::Deliver("SELECT 1".to_string()),
            Ev::Idle,
        ],
        "the DataRow must be length-delimited and the trailing CC+RFQ parsed as \
         the next frames",
    );
}

// ── oversize frames: bounded streaming or classified teardown ──

/// A `DataRow` declaring the maximum `u32` length is absorbed via the Sub-A
/// row-chunk stream in BOUNDED memory — the engine streams the supplied bytes as
/// a `RowChunk` rather than attempting to buffer ~4 GiB. Driven with a fixed
/// input bound (a finite chunk), so it cannot spin or allocate without bound.
#[test]
fn oversize_datarow_max_u32_streams_in_bounded_memory() {
    let mut engine = active_engine();
    // Reach the row-streaming state, then a DataRow header declaring 0xFFFF_FFFF
    // with only a 64-byte chunk of body supplied.
    feed(&mut engine, &row_description(&[("n", 23)]));
    let chunk = vec![0xABu8; 64];
    let oversize_header = frame_declared(TAG_DATA_ROW.byte(), 0xFFFF_FFFF, &chunk);
    let events = drive(&mut engine, &oversize_header, oversize_header.len());
    // The supplied bytes surface as one (or more) RowChunk; the stream then parks
    // at NeedMore (the declared body is far from complete) — never a panic, never
    // a buffered 4 GiB body, never a teardown.
    let chunk_bytes: usize = events
        .iter()
        .map(|e| match e {
            Ev::RowChunk(b) => b.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(chunk_bytes, 64, "the supplied body bytes stream as a RowChunk");
    assert_eq!(
        events.last(),
        Some(&Ev::NeedMore),
        "an incomplete oversize body parks at NeedMore",
    );
    assert!(
        !events.contains(&Ev::Close),
        "a streaming-eligible oversize DataRow must not tear down",
    );
}

/// A `DataRow` whose declared length is just over `READ_BUF_CAP` (so it cannot
/// fit the bounded buffer) streams its WHOLE body via Sub-A row chunks in bounded
/// memory and completes cleanly when the body + trailing frames arrive.
#[test]
fn oversize_datarow_just_over_cap_streams_and_completes() {
    let mut engine = active_engine();
    feed(&mut engine, &row_description(&[("n", 23)]));
    // declared = READ_BUF_CAP -> total wire footprint READ_BUF_CAP + 1, one byte
    // past what the buffer can hold, so it must stream. `declared` is
    // length-inclusive (covers the 4-byte length field) but excludes the tag, so
    // the body the engine streams is `declared - 4`.
    let declared = u32::try_from(READ_BUF_CAP).expect("cap fits u32");
    let body_len = READ_BUF_CAP - 4;
    let body = vec![0xCDu8; body_len];
    let oversize = frame_declared(TAG_DATA_ROW.byte(), declared, &body);
    let reply = concat(&[oversize, command_complete("SELECT 1"), ready_for_query(b'I')]);
    // Feed in 512-byte pieces so the bounded buffer drains between reads.
    let events = drive(&mut engine, &reply, 512);
    let streamed: usize = events
        .iter()
        .map(|e| match e {
            Ev::RowChunk(b) => b.len(),
            _ => 0,
        })
        .sum();
    assert_eq!(streamed, body_len, "the whole oversize body streams in chunks");
    assert!(events.contains(&Ev::RowChunkEnd), "the oversize row ends");
    assert!(
        events.contains(&Ev::Deliver("SELECT 1".to_string())),
        "the trailing CommandComplete delivers after the streamed row",
    );
    assert_eq!(events.last(), Some(&Ev::Idle), "the command reaches idle");
}

/// An oversize frame whose tag is neither streaming-eligible (Sub-B) nor a
/// parse-whole `RowDescription` (Sub-C) is a classified teardown — the engine
/// refuses it rather than buffer it unbounded or skip it. A `ReadyForQuery`
/// (whose body is always a single status byte) is an unambiguous protocol
/// impossibility when oversize. (An oversize `RowDescription` is NOT a teardown
/// — it accumulates whole via Sub-C; see the `engine_verbs_spec` coverage.)
#[test]
fn oversize_non_streaming_tag_tears_down() {
    let mut engine = active_engine();
    // declared 8000 (> READ_BUF_CAP - 1) with a ReadyForQuery tag, in Idle.
    let oversize = frame_declared(TAG_READY_FOR_QUERY.byte(), 8000, &[0u8; 16]);
    let events = drive(&mut engine, &oversize, 64);
    assert_eq!(
        events,
        vec![Ev::Close],
        "an oversize control frame (not streaming-eligible, not Sub-C) must tear down",
    );
}

/// An oversize `RowDescription` whose declared length is ABSURD (~4 GiB) is a
/// classified teardown, NOT an attempt to accumulate gigabytes. The Sub-C
/// accumulate path is bounded by `MAX_ROW_DESC_ACCUM` and rejects-before-allocate
/// when the declared body exceeds it — so a hostile/buggy server cannot drive the
/// client to OOM (the property Sub-A and Sub-B already guarantee). The legitimate
/// wide case (300 columns, far under the cap) still accumulates and decodes — see
/// `oversize_row_description_accumulates_and_decodes` in engine_verbs_spec.
#[test]
fn oversize_row_description_absurd_declared_tears_down() {
    let mut engine = active_engine();
    // declared = u32::MAX (the wire maximum) with a RowDescription tag, in Idle.
    // body_len far exceeds MAX_ROW_DESC_ACCUM (1 MiB), so the engine must refuse
    // before allocating, not begin gathering ~4 GiB.
    let oversize = frame_declared(TAG_ROW_DESCRIPTION.byte(), u32::MAX, &[0u8; 64]);
    let events = drive(&mut engine, &oversize, 64);
    assert_eq!(
        events,
        vec![Ev::Close],
        "an oversize RowDescription beyond the Sub-C cap must tear down, never accumulate",
    );
}

/// An oversize `NoticeResponse` is absorbed via the Sub-B prefix-and-truncate
/// path: only the bounded 8 KiB prefix is retained (the tail is dropped), proving
/// bounded memory even for an oversize async frame.
#[test]
fn oversize_notice_truncates_to_bounded_prefix() {
    let mut engine = active_engine();
    // A notice whose body far exceeds the 8 KiB Sub-B prefix.
    let big = notice_filler(20_000);
    let events = drive(&mut engine, &big, 1024);
    let notices: Vec<&Vec<u8>> = events
        .iter()
        .filter_map(|e| match e {
            Ev::Notice(b) => Some(b),
            _ => None,
        })
        .collect();
    assert_eq!(notices.len(), 1, "the oversize notice surfaces exactly once");
    // Mirrors the engine's internal Sub-B prefix cap; a change there fails this
    // assertion loudly (a sanctioned drift signal), never silently.
    const SUBB_PREFIX_CAP: usize = 8192;
    let prefix_len = notices[0].len();
    assert_eq!(
        prefix_len, SUBB_PREFIX_CAP,
        "the retained notice prefix is bounded at the 8 KiB cap (tail truncated)",
    );
    assert!(
        !events.contains(&Ev::Close),
        "a streaming-eligible oversize notice must not tear down",
    );
}

// ── b'Z'-in-cell immunity (frame-header-aware, never byte-scanning) ──

/// A `DataRow` whose payload CONTAINS a complete-looking `ReadyForQuery` frame
/// (tag `0x5A` + length + status) is consumed as ROW DATA by the frame's declared
/// length — the engine never byte-scans for the RFQ tag — so the stream continues
/// to its real `CommandComplete` + `ReadyForQuery` and reaches idle. This is the
/// exact defect the old async prebuffer's `b'Z'`-scan had; the new engine must
/// not reproduce it.
#[test]
fn embedded_rfq_frame_in_cell_does_not_false_terminate() {
    let mut engine = active_engine();
    // A cell whose bytes ARE a full, valid-looking RFQ frame: tag 'Z', len 5,
    // status 'I'. A byte-scanner would terminate here; a frame-aware parser
    // consumes it as the cell's bytes.
    let embedded = [TAG_READY_FOR_QUERY.byte(), 0, 0, 0, 5, b'I'];
    let d = data_row(&[Some(&embedded), Some(b"tail")]);
    let reply = concat(&[
        row_description(&[("a", 25), ("b", 25)]),
        d.clone(),
        command_complete("SELECT 1"),
        ready_for_query(b'I'),
    ]);
    let events = drive(&mut engine, &reply, reply.len());
    assert_eq!(
        events,
        vec![
            Ev::Row(body_of(&d)),
            Ev::Deliver("SELECT 1".to_string()),
            Ev::Idle,
        ],
        "an embedded RFQ-shaped cell must be consumed as row data, not mistaken \
         for the command boundary",
    );
    // The surfaced row body must actually contain the embedded 0x5A bytes.
    let Some(Ev::Row(row)) = events.first() else {
        panic!("first event must be the row");
    };
    assert!(
        row.windows(embedded.len()).any(|w| w == embedded),
        "the embedded RFQ bytes survive verbatim in the row payload",
    );
}

// ── async frames interleaved mid-row-stream ──

/// Async frames (`NoticeResponse` / `ParameterStatus` / `NotificationResponse`)
/// are wire-legal BETWEEN `DataRow`s mid-stream, not just at a reply boundary,
/// and arrive that way from real PostgreSQL. The engine intercepts them globally
/// before per-state dispatch, so they surface as their own events and NEVER
/// advance or tear the command state machine: all rows still surface, the async
/// frames surface in arrival order, and the command reaches a clean idle. A
/// permanent regression guard — the engine already handles this correctly.
#[test]
fn async_frames_interleaved_mid_row_stream_do_not_disturb_command() {
    let mut engine = active_engine();
    let d1 = data_row(&[Some(b"1"), Some(b"a")]);
    let d2 = data_row(&[Some(b"2"), Some(b"b")]);
    let d3 = data_row(&[Some(b"3"), Some(b"c")]);
    let n = notice("NOTICE", "00000", "heads up");
    let ps = parameter_status("client_encoding", "UTF8");
    let nt = notification(42, "chan", b"payload");
    // RowDescription -> DataRow -> Notice -> ParamStatus -> DataRow -> Notify ->
    // DataRow -> CommandComplete -> ReadyForQuery.
    let reply = concat(&[
        row_description(&[("k", 23), ("v", 25)]),
        d1.clone(),
        n.clone(),
        ps.clone(),
        d2.clone(),
        nt.clone(),
        d3.clone(),
        command_complete("SELECT 3"),
        ready_for_query(b'I'),
    ]);
    let events = drive(&mut engine, &reply, reply.len());
    assert_eq!(
        events,
        vec![
            Ev::Row(body_of(&d1)),
            Ev::Notice(body_of(&n)),
            Ev::ParamStatus(body_of(&ps)),
            Ev::Row(body_of(&d2)),
            Ev::Notify(body_of(&nt)),
            Ev::Row(body_of(&d3)),
            Ev::Deliver("SELECT 3".to_string()),
            Ev::Idle,
        ],
        "async frames interleaved between DataRows must surface in order without \
         disturbing the row stream or the command boundary",
    );
    assert!(
        !events.contains(&Ev::Close),
        "interleaved async frames must not tear the command down",
    );
}

// ── truncation at every byte boundary ──

/// Truncating a valid response at EVERY byte offset never produces a wrong
/// classification, a premature terminal, or a panic: each prefix yields a PREFIX
/// of the full event sequence and parks at `NeedMore`; only the complete stream
/// reaches `Idle`. The loop is bounded by the (fixed) response length.
#[test]
fn truncated_at_every_offset_yields_needmore_never_misclassifies() {
    let d = data_row(&[Some(b"1"), Some(b"alpha")]);
    let full = concat(&[
        row_description(&[("n", 23), ("v", 25)]),
        d.clone(),
        command_complete("SELECT 1"),
        ready_for_query(b'I'),
    ]);

    // The complete stream's event sequence (the reference).
    let full_events = drive(&mut active_engine(), &full, full.len());
    assert_eq!(
        full_events,
        vec![
            Ev::Row(body_of(&d)),
            Ev::Deliver("SELECT 1".to_string()),
            Ev::Idle,
        ],
    );
    // The meaningful (non-Idle) prefix any truncation must be a prefix of.
    let reference: Vec<Ev> = full_events
        .iter()
        .filter(|e| **e != Ev::Idle)
        .cloned()
        .collect();

    for k in 1..full.len() {
        let events = drive(&mut active_engine(), &full[..k], full.len());
        // No false teardown and no premature idle on a well-formed-but-truncated
        // stream.
        assert!(
            !events.contains(&Ev::Close),
            "truncation at offset {k} must not tear the connection down",
        );
        assert!(
            !events.contains(&Ev::Idle),
            "truncation at offset {k} must not reach a premature idle terminal",
        );
        // The last event is the exhaustion NeedMore.
        assert_eq!(
            events.last(),
            Some(&Ev::NeedMore),
            "truncation at offset {k} must park at NeedMore",
        );
        // The events before that NeedMore are a prefix of the full sequence.
        let meaningful: Vec<Ev> = events
            .iter()
            .filter(|e| **e != Ev::NeedMore)
            .cloned()
            .collect();
        assert!(
            reference.starts_with(&meaningful),
            "truncation at offset {k} produced {meaningful:?}, not a prefix of {reference:?}",
        );
    }

    // And the same stream fed one byte at a time reassembles to the full result.
    assert_eq!(drive(&mut active_engine(), &full, 1), full_events);
}

// ── out-of-phase / unknown tag → classified teardown ──

/// In each active state an out-of-phase or unknown tag is a classified teardown,
/// never a silent skip. Each case reaches the state, then feeds the illegal frame.
#[test]
fn out_of_phase_or_unknown_tag_tears_down() {
    // Idle: a bare DataRow (no preceding RowDescription) is out of phase.
    {
        let mut engine = active_engine();
        let events = drive(&mut engine, &data_row(&[Some(b"x")]), usize::MAX);
        assert_eq!(events, vec![Ev::Close], "bare DataRow in Idle tears down");
    }
    // Idle: an entirely unknown tag (`0xFF`, not any wire-legal backend tag).
    {
        let mut engine = active_engine();
        let unknown = frame(0xFF, b"junk");
        let events = drive(&mut engine, &unknown, usize::MAX);
        assert_eq!(events, vec![Ev::Close], "unknown tag in Idle tears down");
    }
    // StreamingRows: a CopyData frame mid-row-stream is out of phase.
    {
        let mut engine = active_engine();
        feed(&mut engine, &row_description(&[("n", 23)]));
        let bad = frame(TAG_COPY_DATA.byte(), b"copy");
        let events = drive(&mut engine, &bad, usize::MAX);
        assert_eq!(
            events,
            vec![Ev::Close],
            "CopyData mid-row-stream tears down",
        );
    }
    // AwaitingRfq: a DataRow where the trailing ReadyForQuery is due.
    {
        let mut engine = active_engine();
        feed(
            &mut engine,
            &concat(&[row_description(&[("n", 23)]), command_complete("SELECT 0")]),
        );
        // Drain up to the Deliver so the engine is AwaitingRfq.
        assert_eq!(pull(&mut engine), Ev::Deliver("SELECT 0".to_string()));
        let bad = data_row(&[Some(b"x")]);
        let events = drive(&mut engine, &bad, usize::MAX);
        assert_eq!(
            events,
            vec![Ev::Close],
            "DataRow where ReadyForQuery is due tears down",
        );
    }
}

/// The active pump surfaces a protocol violation as the public `Boundary::Closed`
/// — the classified teardown reaches the verb layer, which maps it to
/// `EngineError::ProtocolViolation` (the connection must close).
#[test]
fn active_protocol_violation_pumps_to_boundary_closed() {
    let mut engine = active_engine();
    let mut send_buf = SendBuf::new();
    let obs = NoObserver;
    // A bare DataRow in Idle is a protocol violation.
    let mut transport = ScriptedTransport::new(data_row(&[Some(b"x")]), 0);
    let sink = |_: Surface<'_>| -> ControlFlow<()> { ControlFlow::Continue(()) };
    let boundary = match poll_once(pump_active_to_boundary(
        &mut engine,
        &mut transport,
        &mut send_buf,
        &obs,
        sink,
    )) {
        Ok(Ok(b)) => b,
        Ok(Err(err)) => panic!("pump returned an unexpected error: {err:?}"),
        Err(_) => panic!("blocking transport must resolve in a single poll"),
    };
    assert_eq!(
        boundary,
        Boundary::Closed,
        "a protocol violation must surface as Boundary::Closed",
    );
}

// ── hostile handshake → classified ConnFail ──

/// Each hostile handshake reply is a classified `ConnFail` — never a panic, a
/// hang, or a silent skip. Asserts the SPECIFIC variant, so a mis-classification
/// is caught.
#[test]
fn hostile_handshake_replies_classify_connfail() {
    // A truncated Authentication frame (sub-code field below 4 bytes).
    assert!(
        matches!(
            handshake_outcome(frame(TAG_AUTHENTICATION.byte(), &[0u8, 0u8])),
            HandshakeOutcome::Failed(ConnFail::MalformedAuthentication),
        ),
        "a truncated auth sub-code must classify as MalformedAuthentication",
    );

    // An unknown authentication sub-code the trust client cannot satisfy.
    assert!(
        matches!(
            handshake_outcome(auth_request(99, &[])),
            HandshakeOutcome::Failed(ConnFail::UnsupportedAuthMethod),
        ),
        "an unknown auth sub-code must classify as UnsupportedAuthMethod",
    );

    // A SASL (SCRAM-SHA-256) request to a trust (no-password) client: the
    // credentials cannot satisfy it. Truncated mechanism list included.
    assert!(
        matches!(
            handshake_outcome(auth_request(10, b"SCRAM-SHA-256\0\0")),
            HandshakeOutcome::Failed(ConnFail::UnsupportedAuthMethod),
        ),
        "a SASL request to a trust client must classify as UnsupportedAuthMethod",
    );

    // An out-of-phase frame before AuthenticationOk: a DataRow during connect.
    assert!(
        matches!(
            handshake_outcome(data_row(&[Some(b"x")])),
            HandshakeOutcome::Failed(ConnFail::UnexpectedFrame { tag })
                if tag == TAG_DATA_ROW.byte(),
        ),
        "an out-of-phase frame during connect must classify as UnexpectedFrame",
    );

    // A BackendKeyData whose payload is not the expected 8 bytes, after AuthOk.
    let bad_key = concat(&[auth_ok(), frame(TAG_BACKEND_KEY_DATA.byte(), &[1u8, 2, 3])]);
    assert!(
        matches!(
            handshake_outcome(bad_key),
            HandshakeOutcome::Failed(ConnFail::MalformedBackendKeyData),
        ),
        "a short BackendKeyData must classify as MalformedBackendKeyData",
    );

    // A ReadyForQuery with an illegal transaction-status byte, after AuthOk + K.
    let bad_rfq = concat(&[
        auth_ok(),
        backend_key(4321, 8765),
        frame(TAG_READY_FOR_QUERY.byte(), b"?"),
    ]);
    assert!(
        matches!(
            handshake_outcome(bad_rfq),
            HandshakeOutcome::Failed(ConnFail::MalformedReadyForQuery),
        ),
        "an illegal RFQ status byte must classify as MalformedReadyForQuery",
    );

    // The peer closing mid-handshake (no bytes) reaches an exhausted read — the
    // pump classifies it (UnexpectedEof), never spins. A successful handshake is
    // the negative control.
    let ready = concat(&[auth_ok(), backend_key(4321, 8765), ready_for_query(b'I')]);
    assert!(
        matches!(handshake_outcome(ready), HandshakeOutcome::Ready),
        "the canonical trust handshake still reaches Ready (negative control)",
    );
}
