//! Behavioural gate for the active-phase dispatch + the `next_event` pull
//! surface.
//!
//! Drives the new [`ActiveEngine`] the way the ingest pump will: reach an
//! active handle through the canonical trust handshake, feed scripted server
//! reply frames through `read_slot`/`commit`, and pull the active events.
//! Covers the single-pass borrow-through (R2: a `Row` event lends the whole
//! `DataRow` body in place), partial-frame reassembly under one-byte-per-read
//! (a frame split at every byte boundary reassembles via `NeedMore`-resume), a
//! `b'Z'`-inside-a-cell that does NOT false-terminate (the engine is
//! frame-header-aware, never byte-scanning), the active state transitions
//! (row stream, multi-statement delineation, COPY OUT, recoverable server
//! error, protocol-violation teardown, async notice/notify/param surfacing),
//! and both oversize paths (Sub-A row chunk streaming + Sub-B 8 KiB-prefix
//! stream-and-truncate, both in bounded memory).
//!
//! The no-escape wall (holding an `Event` across the next mutating call =
//! E0499) and the cross-phase / within-vocab exhaustiveness are gated in
//! `engine_active_compile_fail`.
//!
//! [`ActiveEngine`]: bsql_postgres_proto::engine::ActiveEngine

#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test helpers (the feed pump, handshake construction) use unwrap/expect/panic as the loud failure signal; clippy's allow-in-tests carve-out reaches #[test] fns but not the free helper fns this file factors out"
)]

use bsql_postgres_proto::engine::{ActiveEngine, AuthEvent, ConnectingEngine, Event, SendBuf};
use bsql_postgres_proto::frame::READ_BUF_CAP;
use bsql_postgres_proto::wire::{
    TAG_COMMAND_COMPLETE, TAG_COPY_DATA, TAG_COPY_DONE, TAG_COPY_OUT_RESPONSE, TAG_DATA_ROW,
    TAG_ERROR_RESPONSE, TAG_NOTICE_RESPONSE, TAG_PARAMETER_STATUS, TAG_READY_FOR_QUERY,
    TAG_ROW_DESCRIPTION,
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

/// A frame with an explicit (possibly oversize) declared length field,
/// independent of `body.len()` — used to forge an oversize header whose body is
/// then streamed in pieces.
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

fn notice(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    frame(TAG_NOTICE_RESPONSE.byte(), &diagnostic(severity, sqlstate, message))
}

fn error_response(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    frame(TAG_ERROR_RESPONSE.byte(), &diagnostic(severity, sqlstate, message))
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

fn parameter_status(key: &str, value: &str) -> Vec<u8> {
    let mut body = key.as_bytes().to_vec();
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(TAG_PARAMETER_STATUS.byte(), &body)
}

fn copy_out_response(n_cols: i16) -> Vec<u8> {
    let mut body = vec![0u8];
    body.extend_from_slice(&n_cols.to_be_bytes());
    for _ in 0..n_cols.max(0) {
        body.extend_from_slice(&0i16.to_be_bytes());
    }
    frame(TAG_COPY_OUT_RESPONSE.byte(), &body)
}

fn copy_data(bytes: &[u8]) -> Vec<u8> {
    frame(TAG_COPY_DATA.byte(), bytes)
}

fn copy_done() -> Vec<u8> {
    frame(TAG_COPY_DONE.byte(), &[])
}

// ─────────────────────────── engine drivers ───────────────────────────

/// Owned mirror of one pulled [`Event`], so a sequence can be collected past
/// the borrow each event carries.
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
        Event::Deliver => {
            // The borrow from `next_event` (none for `Deliver`) has ended; read
            // the just-stored command tag.
            Ev::Deliver(match engine.last_command_tag() {
                Some(tag) => tag.to_string(),
                None => String::new(),
            })
        }
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
/// (`Idle` / `Close`) or input exhaustion.
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

/// The `DataRow` body (the payload after the 5-byte header) of a built frame.
fn row_body(frame_bytes: &[u8]) -> Vec<u8> {
    frame_bytes[5..].to_vec()
}

// ─────────────────────────── tests ───────────────────────────

/// R2 single-pass borrow-through: a SELECT reply yields the row bodies in place
/// plus the command tag and a clean idle terminal.
#[test]
fn select_rows_single_pass_borrow_through() {
    let mut engine = active_engine();
    let d1 = data_row(&[Some(b"1"), Some(b"alpha")]);
    let d2 = data_row(&[Some(b"2"), None]);
    // Drive up to (and including) the Deliver, BEFORE the RFQ resets the
    // per-statement columns at the command boundary — the point at which a
    // consumer snapshots a statement's result set.
    let reply = concat(&[
        row_description(&[("n", 23), ("v", 25)]),
        d1.clone(),
        d2.clone(),
        command_complete("SELECT 2"),
    ]);
    let events = drive(&mut engine, &reply, reply.len());
    assert_eq!(
        events,
        vec![
            Ev::Row(row_body(&d1)),
            Ev::Row(row_body(&d2)),
            Ev::Deliver("SELECT 2".to_string()),
            Ev::NeedMore,
        ],
    );
    // Columns/OIDs are live at the Deliver point (consumed there).
    assert_eq!(engine.current_type_oids(), &[23, 25]);
    assert_eq!(
        engine.current_column_names(),
        &["n".to_string(), "v".to_string()],
    );
    // The trailing RFQ then returns the engine cleanly to idle.
    feed(&mut engine, &ready_for_query(b'I'));
    assert!(matches!(engine.next_event(), Event::Idle));
}

/// Partial-frame reassembly: the SAME reply split at EVERY byte boundary
/// reassembles to the identical event sequence (NeedMore-resume).
#[test]
fn partial_assembly_one_byte_per_read() {
    let mut whole = active_engine();
    let mut split = active_engine();
    let d = data_row(&[Some(b"chunky")]);
    let reply = concat(&[
        row_description(&[("v", 25)]),
        d.clone(),
        command_complete("SELECT 1"),
        ready_for_query(b'I'),
    ]);
    let whole_events = drive(&mut whole, &reply, reply.len());
    let split_events = drive(&mut split, &reply, 1);
    assert_eq!(whole_events, split_events, "byte-split reassembles identically");
    assert_eq!(
        whole_events,
        vec![Ev::Row(row_body(&d)), Ev::Deliver("SELECT 1".to_string()), Ev::Idle],
    );
}

/// A `b'Z'` (the `ReadyForQuery` tag byte) INSIDE a cell does not
/// false-terminate the row: the engine reads the length field, never scans for
/// a terminator. Exactly one `Row` carries the cell bytes intact.
#[test]
fn z_byte_inside_cell_does_not_false_terminate() {
    let mut engine = active_engine();
    // A cell whose bytes include 0x5A ('Z') and the RFQ-shaped tail.
    let cell: &[u8] = &[b'a', b'Z', 0x00, 0x00, 0x00, 0x05, b'I'];
    let d = data_row(&[Some(cell)]);
    let reply = concat(&[
        row_description(&[("v", 25)]),
        d.clone(),
        command_complete("SELECT 1"),
        ready_for_query(b'I'),
    ]);
    let events = drive(&mut engine, &reply, reply.len());
    assert_eq!(
        events,
        vec![Ev::Row(row_body(&d)), Ev::Deliver("SELECT 1".to_string()), Ev::Idle],
    );
    // The row body still carries the embedded 'Z'.
    match &events[0] {
        Ev::Row(body) => assert!(body.windows(1).any(|w| w == [b'Z']), "embedded Z preserved"),
        other => panic!("expected Row, got {other:?}"),
    }
}

/// Multi-statement batch with the row-bearing statement LAST: each DML
/// statement delineates its own command boundary (`Deliver`), then the SELECT.
#[test]
fn multi_statement_delineated() {
    let mut engine = active_engine();
    let d10 = data_row(&[Some(b"10")]);
    let d11 = data_row(&[Some(b"11")]);
    let reply = concat(&[
        command_complete("UPDATE 3"),
        command_complete("INSERT 0 1"),
        row_description(&[("id", 23)]),
        d10.clone(),
        d11.clone(),
        command_complete("SELECT 2"),
        ready_for_query(b'I'),
    ]);
    let events = drive(&mut engine, &reply, reply.len());
    assert_eq!(
        events,
        vec![
            Ev::Deliver("UPDATE 3".to_string()),
            Ev::Deliver("INSERT 0 1".to_string()),
            Ev::Row(row_body(&d10)),
            Ev::Row(row_body(&d11)),
            Ev::Deliver("SELECT 2".to_string()),
            Ev::Idle,
        ],
    );
}

/// COPY OUT sub-protocol: the per-frame copy chunks surface verbatim, then the
/// trailing `CommandComplete` + `ReadyForQuery`.
#[test]
fn copy_out_streams_chunks() {
    let mut engine = active_engine();
    let reply = concat(&[
        copy_out_response(1),
        copy_data(b"row1\n"),
        copy_data(b"row2\n"),
        copy_done(),
        command_complete("COPY 2"),
        ready_for_query(b'I'),
    ]);
    let events = drive(&mut engine, &reply, reply.len());
    assert_eq!(
        events,
        vec![
            Ev::CopyData(b"row1\n".to_vec()),
            Ev::CopyData(b"row2\n".to_vec()),
            Ev::CopyDone,
            Ev::Deliver("COPY 2".to_string()),
            Ev::Idle,
        ],
    );
}

/// A recoverable server error: `Fail` lends the error body, then the connection
/// drains to a clean `Idle` (it survives a query-level error).
#[test]
fn recoverable_server_error() {
    let mut engine = active_engine();
    let err = error_response("ERROR", "42601", "syntax error");
    let reply = concat(&[err.clone(), ready_for_query(b'I')]);
    let events = drive(&mut engine, &reply, reply.len());
    assert_eq!(events, vec![Ev::Fail(row_body(&err)), Ev::Idle]);
}

/// A second `RowDescription` mid-stream is a protocol violation: the engine
/// surfaces the rows so far, then tears down (`Close`).
#[test]
fn second_row_description_tears_down() {
    let mut engine = active_engine();
    let d = data_row(&[Some(b"row1")]);
    let reply = concat(&[
        row_description(&[("v", 25)]),
        d.clone(),
        row_description(&[("w", 25)]),
        data_row(&[Some(b"row2")]),
        command_complete("SELECT 2"),
        ready_for_query(b'I'),
    ]);
    let events = drive(&mut engine, &reply, reply.len());
    assert_eq!(events, vec![Ev::Row(row_body(&d)), Ev::Close]);
}

/// Async notice + parameter-status surface during a query reply, in arrival
/// order, without disturbing the command boundary.
#[test]
fn async_notice_and_param_status_surface() {
    let mut engine = active_engine();
    let n = notice("NOTICE", "00000", "heads up");
    let p = parameter_status("application_name", "demo");
    let reply = concat(&[n.clone(), p.clone(), command_complete("SET"), ready_for_query(b'I')]);
    let events = drive(&mut engine, &reply, reply.len());
    assert_eq!(
        events,
        vec![
            Ev::Notice(row_body(&n)),
            Ev::ParamStatus(row_body(&p)),
            Ev::Deliver("SET".to_string()),
            Ev::Idle,
        ],
    );
}

/// Sub-A: a `DataRow` whose body exceeds the bounded buffer streams as
/// `RowChunk` events terminated by `RowChunkEnd`, in bounded memory — the
/// reassembled chunks equal the original oversize body.
#[test]
fn oversize_row_streams_sub_a_bounded() {
    let mut engine = active_engine();
    // Open a row stream so the oversize 'D' is legal.
    feed(&mut engine, &row_description(&[("v", 25)]));
    assert!(matches!(engine.next_event(), Event::NeedMore));

    // Forge an oversize DataRow: declared length far beyond READ_BUF_CAP.
    let body_len = READ_BUF_CAP * 3 + 17;
    let body = vec![0x5Au8; body_len]; // all 'Z' bytes — must NOT false-terminate
    let declared = u32::try_from(body_len + 4).expect("declared fits u32");
    let oversize = frame_declared(TAG_DATA_ROW.byte(), declared, &body);
    let tail = concat(&[command_complete("SELECT 1"), ready_for_query(b'I')]);
    let wire = concat(&[oversize, tail]);

    // Drive in 1000-byte chunks so the buffer never exceeds its bound.
    let mut reassembled = Vec::new();
    let mut events_after = Vec::new();
    let mut fed = 0usize;
    let chunk = 1000usize;
    loop {
        match engine.next_event() {
            Event::RowChunk(b) => reassembled.extend_from_slice(b),
            Event::RowChunkEnd => {}
            Event::NeedMore => {
                if fed >= wire.len() {
                    break;
                }
                let take = chunk.min(wire.len() - fed);
                feed(&mut engine, &wire[fed..fed + take]);
                fed += take;
            }
            Event::Deliver => events_after.push(Ev::Deliver(match engine.last_command_tag() {
                Some(tag) => tag.to_string(),
                None => String::new(),
            })),
            Event::Idle => {
                events_after.push(Ev::Idle);
                break;
            }
            other => panic!("unexpected during oversize row: {other:?}"),
        }
    }
    assert_eq!(reassembled.len(), body_len, "all oversize body bytes streamed");
    assert_eq!(reassembled, body, "reassembled oversize row equals original");
    assert_eq!(
        events_after,
        vec![Ev::Deliver("SELECT 1".to_string()), Ev::Idle],
        "engine resumes normal framing after the oversize row",
    );
}

/// R6 Sub-B: an oversize streaming-eligible non-`D` frame (a huge
/// `NoticeResponse`) is absorbed via the 8 KiB prefix-and-truncate path in
/// bounded memory, surfaced truncated, and the engine resumes framing.
#[test]
fn oversize_notice_streams_sub_b_truncated() {
    /// Mirror of the engine-private Sub-B prefix cap.
    const PREFIX_CAP: usize = 8192;
    let mut engine = active_engine();

    let body_len = PREFIX_CAP * 2 + 123; // exceeds both READ_BUF_CAP and the prefix
    let mut body = Vec::with_capacity(body_len);
    body.extend_from_slice(b"Soversity\0");
    body.resize(body_len, b'x');
    let declared = u32::try_from(body_len + 4).expect("declared fits u32");
    let oversize = frame_declared(TAG_NOTICE_RESPONSE.byte(), declared, &body);
    let tail = concat(&[command_complete("CREATE TABLE"), ready_for_query(b'I')]);
    let wire = concat(&[oversize, tail]);

    let events = drive(&mut engine, &wire, 1000);
    // The Notice surfaces with a BOUNDED prefix (truncated to the cap), then the
    // engine resumes and completes the trailing command.
    match &events[0] {
        Ev::Notice(prefix) => {
            assert_eq!(prefix.len(), PREFIX_CAP, "Sub-B prefix truncated to the cap");
            assert!(prefix.starts_with(b"Soversity\0"), "prefix retains the frame head");
        }
        other => panic!("expected truncated Notice, got {other:?}"),
    }
    assert_eq!(
        &events[1..],
        &[Ev::Deliver("CREATE TABLE".to_string()), Ev::Idle],
        "engine resumes framing after the oversize Sub-B frame",
    );
}

/// An oversize frame whose tag is NOT streaming-eligible (a control frame) is a
/// classified teardown, never an unbounded buffer demand.
#[test]
fn oversize_control_frame_tears_down() {
    let mut engine = active_engine();
    let body_len = READ_BUF_CAP * 2;
    let declared = u32::try_from(body_len + 4).expect("declared fits u32");
    // 'Z' (ReadyForQuery) is a control frame — never legitimately oversize.
    let oversize = frame_declared(TAG_READY_FOR_QUERY.byte(), declared, &vec![b'I'; body_len]);
    let events = drive(&mut engine, &oversize, 1000);
    assert_eq!(events.last(), Some(&Ev::Close), "oversize control frame tears down");
}

/// An OVERSIZE `CopyData` OUTSIDE the COPY-OUT phase is out of phase: it must
/// tear down (`Close`), never surface a spurious truncated `CopyData` event.
/// The in-buffer path already tears a stray `CopyData` down in `step_idle`; the
/// oversize Sub-B path must mirror that phase gate — `CopyData` is
/// streaming-eligible, so WITHOUT the gate it is absorbed into the prefix and
/// surfaced out of phase. Reachable only from a hostile / non-compliant server;
/// bounded, no crash (the body is absorbed into the bounded prefix, then torn
/// down).
#[test]
fn oversize_copy_data_outside_copy_out_tears_down() {
    let mut engine = active_engine();
    // No COPY OUT was opened — the engine is idle. Forge an oversize CopyData
    // (declared length far beyond READ_BUF_CAP, so it takes the Sub-B path).
    let body_len = READ_BUF_CAP * 2 + 41;
    let declared = u32::try_from(body_len + 4).expect("declared fits u32");
    let oversize = frame_declared(TAG_COPY_DATA.byte(), declared, &vec![b'q'; body_len]);
    let events = drive(&mut engine, &oversize, 1000);
    assert_eq!(
        events.last(),
        Some(&Ev::Close),
        "an oversize CopyData outside COPY OUT tears down, got {events:?}",
    );
    assert!(
        !events.iter().any(|e| matches!(e, Ev::CopyData(_))),
        "no spurious out-of-phase CopyData event is surfaced, got {events:?}",
    );
}

/// The in-phase companion: an oversize `CopyData` DURING COPY OUT still surfaces
/// its truncated Sub-B prefix, then the engine resumes — proving the phase gate
/// preserves the legitimate path and rejects only the out-of-phase case.
#[test]
fn oversize_copy_data_in_copy_out_surfaces_truncated_prefix() {
    /// Mirror of the engine-private Sub-B prefix cap.
    const PREFIX_CAP: usize = 8192;
    let mut engine = active_engine();
    // Open COPY OUT so a CopyData is in phase.
    feed(&mut engine, &copy_out_response(1));
    assert!(matches!(engine.next_event(), Event::NeedMore));

    let body_len = PREFIX_CAP * 2 + 7; // exceeds both READ_BUF_CAP and the prefix
    let declared = u32::try_from(body_len + 4).expect("declared fits u32");
    let oversize = frame_declared(TAG_COPY_DATA.byte(), declared, &vec![b'd'; body_len]);
    let tail = concat(&[copy_done(), command_complete("COPY 1"), ready_for_query(b'I')]);
    let wire = concat(&[oversize, tail]);

    let events = drive(&mut engine, &wire, 1000);
    match events.first() {
        Some(Ev::CopyData(prefix)) => {
            assert_eq!(prefix.len(), PREFIX_CAP, "Sub-B prefix truncated to the cap");
        }
        other => panic!("expected a truncated in-phase CopyData, got {other:?}"),
    }
    assert_eq!(
        events.last(),
        Some(&Ev::Idle),
        "engine resumes after the oversize in-phase CopyData, got {events:?}",
    );
}
