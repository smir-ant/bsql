//! Offline spec for the PIPELINE receive-side multiplexer
//! ([`ActiveState::PipelineAwaitingNextOrRfq`]).
//!
//! A heterogeneous `pipeline((...))` flushes N compile-checked commands — each a
//! `[Close+Parse if cache-miss] + Bind + Execute` block — with ONE trailing
//! `Sync`, forming a SINGLE implicit transaction. This spec drives
//! [`pump_active_to_boundary`] over a SCRIPTED transport (no network, no
//! `PreparedQuery`) that replays a hand-built batch reply, seating the first
//! command's state exactly as the staging path does (`begin_bind_execute` /
//! `begin_close_parse_bind_execute` + `begin_pipeline`), and asserts:
//!
//! - N HIT commands under one Sync surface N `Row`s + N `Deliver`s then `Idle`;
//! - N MISS commands (each leading with `Close`+`Parse`) do the same, exercising
//!   the `'3'` → `ParseBindExecute…` → `'1'` → `'2'` per-command chain;
//! - N=1 is byte-for-byte the fused single-query shape (no regression);
//! - a mid-batch `ErrorResponse` surfaces the completed commands' `Deliver`s
//!   BEFORE the failure and then `Boundary::Failed`, and a follow-up drain reads
//!   the batch's single trailing `ReadyForQuery` to `Boundary::Idle` — the
//!   all-or-nothing recovery the driver's collector turns into "discard all".

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    reason = "offline spec helpers (handshake construction, wire-frame builders, the poll-once pump driver) use expect/panic and try_from(..).unwrap_or(0) on tiny test constants as the loud failure signal; clippy's allow-in-tests carve-out reaches #[test] fns but not the free helper fns this spec factors out"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

use bsql_postgres_proto::engine::{
    poll_once, pump_active_to_boundary, ActiveEngine, AuthEvent, Boundary, ConnectingEngine,
    SendBuf, Surface, Transport,
};
use bsql_postgres_proto::{Credentials, Ident};

/// A scripted transport that drains a fixed inbound queue on `read`; writes /
/// flush / shutdown are no-op ready, so the whole pump future resolves under one
/// `poll_once`.
struct ScriptReader {
    inbound: Vec<u8>,
}

impl Transport for ScriptReader {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        let n = self.inbound.len().min(buf.len());
        for (slot, byte) in buf.iter_mut().zip(self.inbound.drain(..n)) {
            *slot = byte;
        }
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

/// A tagged, length-prefixed wire frame.
fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(tag);
    let len = u32::try_from(body.len() + 4).unwrap_or(0);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// Reach an active engine through the canonical trust handshake (the public path;
/// the bare `from_handshake` constructor is crate-private).
fn active() -> ActiveEngine {
    let user = Ident::try_from_str("corpus").expect("ident");
    let mut send_buf = SendBuf::new();
    let mut conn = ConnectingEngine::start(&mut send_buf, &user, None, &[], Credentials::Trust)
        .expect("start handshake");
    let mut hs = frame(b'R', &0i32.to_be_bytes()); // AuthenticationOk
    let mut key_body = 4321i32.to_be_bytes().to_vec();
    key_body.extend_from_slice(&8765i32.to_be_bytes());
    hs.extend_from_slice(&frame(b'K', &key_body)); // BackendKeyData
    hs.extend_from_slice(&frame(b'Z', b"I")); // ReadyForQuery(Idle)
    let mut fed = 0usize;
    while fed < hs.len() {
        let remaining = &hs[fed..];
        let slot = conn.read_slot(remaining.len()).expect("conn slot");
        let n = slot.len().min(remaining.len());
        slot[..n].copy_from_slice(&remaining[..n]);
        conn.commit(n).expect("conn commit");
        fed += n;
    }
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

fn bind_complete() -> Vec<u8> {
    frame(b'2', &[])
}
fn parse_complete() -> Vec<u8> {
    frame(b'1', &[])
}
fn close_complete() -> Vec<u8> {
    frame(b'3', &[])
}
/// A one-column `DataRow` carrying the byte string `val`.
fn data_row(val: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1i16.to_be_bytes()); // 1 column
    body.extend_from_slice(&i32::try_from(val.len()).unwrap_or(0).to_be_bytes());
    body.extend_from_slice(val);
    frame(b'D', &body)
}
fn command_complete(tag: &str) -> Vec<u8> {
    let mut body = tag.as_bytes().to_vec();
    body.push(0);
    frame(b'C', &body)
}
fn ready_idle() -> Vec<u8> {
    frame(b'Z', b"I")
}
/// A minimal `ErrorResponse` (severity / SQLSTATE / message + terminator).
fn error_response(code: &str, msg: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(code.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(msg.as_bytes());
    body.push(0);
    body.push(0); // fields terminator
    frame(b'E', &body)
}

/// One HIT command's reply: `BindComplete` + one `DataRow` + `CommandComplete`.
fn hit_command(val: &[u8], tag: &str) -> Vec<u8> {
    let mut out = bind_complete();
    out.extend_from_slice(&data_row(val));
    out.extend_from_slice(&command_complete(tag));
    out
}

/// One MISS command's reply: `CloseComplete` + `ParseComplete` + `BindComplete` +
/// one `DataRow` + `CommandComplete` (the leading Close makes the Parse idempotent).
fn miss_command(val: &[u8], tag: &str) -> Vec<u8> {
    let mut out = close_complete();
    out.extend_from_slice(&parse_complete());
    out.extend_from_slice(&bind_complete());
    out.extend_from_slice(&data_row(val));
    out.extend_from_slice(&command_complete(tag));
    out
}

/// Drive one pump pass, tallying `Row` / `Deliver` / `Fail` surfaces.
fn drive(engine: &mut ActiveEngine, transport: &mut ScriptReader) -> (Boundary, usize, usize, usize) {
    let mut send_buf = SendBuf::new();
    let mut rows = 0usize;
    let mut delivers = 0usize;
    let mut fails = 0usize;
    let outcome: Result<Result<Boundary, _>, _> = poll_once(pump_active_to_boundary(
        engine,
        transport,
        &mut send_buf,
        |surface: Surface<'_>| {
            match surface {
                Surface::Row(_) => rows += 1,
                Surface::Deliver { .. } => delivers += 1,
                Surface::Fail(_) => fails += 1,
                _ => {}
            }
            ControlFlow::Continue(())
        },
    ));
    let boundary = match outcome {
        Ok(Ok(b)) => b,
        other => panic!("pump did not resolve to a boundary: {other:?}"),
    };
    (boundary, rows, delivers, fails)
}

#[test]
fn n_hit_commands_under_one_sync_surface_n_rows_n_delivers_then_idle() {
    for n in 1..=6usize {
        let mut engine = active();
        // Seat command 0 as a HIT (Bind+Execute) and arm pipeline mode, exactly as
        // the FIRST staged command does.
        engine.begin_bind_execute(&[25]);
        engine.begin_pipeline();

        let mut inbound = Vec::new();
        for i in 0..n {
            inbound.extend_from_slice(&hit_command(format!("r{i}").as_bytes(), "SELECT 1"));
        }
        inbound.extend_from_slice(&ready_idle());

        let mut transport = ScriptReader { inbound };
        let (boundary, rows, delivers, fails) = drive(&mut engine, &mut transport);
        assert_eq!(boundary, Boundary::Idle, "n={n}: clean batch boundary");
        assert_eq!(rows, n, "n={n}: one Row per command");
        assert_eq!(delivers, n, "n={n}: one Deliver per command");
        assert_eq!(fails, 0, "n={n}: no failure");
    }
}

#[test]
fn n_miss_commands_exercise_the_close_parse_bind_chain_per_command() {
    for n in 1..=4usize {
        let mut engine = active();
        // Command 0 is a MISS: leads with Close+Parse, so its seat is the
        // CloseParseBindExecute chain (the `begin_close_parse_bind_execute` seat).
        engine.begin_close_parse_bind_execute(&[25]);
        engine.begin_pipeline();

        let mut inbound = Vec::new();
        for i in 0..n {
            inbound.extend_from_slice(&miss_command(format!("m{i}").as_bytes(), "SELECT 1"));
        }
        inbound.extend_from_slice(&ready_idle());

        let mut transport = ScriptReader { inbound };
        let (boundary, rows, delivers, fails) = drive(&mut engine, &mut transport);
        assert_eq!(boundary, Boundary::Idle, "n={n}: clean batch boundary");
        assert_eq!(rows, n, "n={n}: one Row per MISS command");
        assert_eq!(delivers, n, "n={n}: one Deliver per MISS command");
        assert_eq!(fails, 0, "n={n}: no failure");
    }
}

#[test]
fn n1_pipeline_is_the_fused_single_query_shape() {
    // A one-command pipeline is byte-for-byte a single fused query on the wire:
    // BindComplete/DataRow/CommandComplete/ReadyForQuery. The pipeline path just
    // routes the boundary through PipelineAwaitingNextOrRfq → RFQ, reaching the
    // SAME Idle with the SAME one Row + one Deliver — no regression.
    let mut engine = active();
    engine.begin_bind_execute(&[25]);
    engine.begin_pipeline();
    let mut inbound = hit_command(b"only", "SELECT 1");
    inbound.extend_from_slice(&ready_idle());
    let mut transport = ScriptReader { inbound };
    let (boundary, rows, delivers, fails) = drive(&mut engine, &mut transport);
    assert_eq!(boundary, Boundary::Idle);
    assert_eq!((rows, delivers, fails), (1, 1, 0));
}

#[test]
fn mid_batch_error_surfaces_completed_delivers_then_failed_then_drains_to_idle() {
    // 3-command batch: cmd0 completes, cmd1 errors mid-rows, cmd2 is SKIPPED by the
    // server (no reply). The pump surfaces cmd0's Deliver + a Fail, then reaches
    // Boundary::Failed with the batch's single trailing RFQ still owed.
    let mut engine = active();
    engine.begin_bind_execute(&[25]);
    engine.begin_pipeline();

    let mut inbound = Vec::new();
    inbound.extend_from_slice(&hit_command(b"c0", "SELECT 1")); // cmd0 completes
    inbound.extend_from_slice(&bind_complete()); // cmd1 opens
    inbound.extend_from_slice(&data_row(b"c1")); // cmd1 streams a row
    inbound.extend_from_slice(&error_response("23505", "duplicate key")); // cmd1 errors
    // cmd2 is skipped: the server sends only the trailing RFQ.
    inbound.extend_from_slice(&ready_idle());

    let mut transport = ScriptReader { inbound };
    let (boundary, rows, delivers, fails) = drive(&mut engine, &mut transport);
    assert_eq!(boundary, Boundary::Failed, "pump stops at the failure boundary");
    assert_eq!(delivers, 1, "only cmd0 delivered before the failure");
    assert_eq!(fails, 1, "the server error surfaced exactly once");
    assert_eq!(rows, 2, "cmd0's row + cmd1's pre-error row both surfaced");

    // The connection owes the batch's single trailing ReadyForQuery: a follow-up
    // drain reads it to a clean idle, no further Deliver — this is what the driver
    // turns into "discard all results, connection reusable".
    let (boundary2, rows2, delivers2, fails2) = drive(&mut engine, &mut transport);
    assert_eq!(boundary2, Boundary::Idle, "drain reaches the clean batch RFQ");
    assert_eq!((rows2, delivers2, fails2), (0, 0, 0), "nothing more surfaces on the drain");
}

#[test]
fn error_at_the_next_commands_first_ack_is_classified_and_drains() {
    // cmd0 completes, then cmd1 errors at its FIRST step (a Bind type error) with
    // NO preceding BindComplete — the ErrorResponse arrives directly in
    // PipelineAwaitingNextOrRfq. It must be the recoverable failure, not a teardown.
    let mut engine = active();
    engine.begin_bind_execute(&[25]);
    engine.begin_pipeline();

    let mut inbound = hit_command(b"c0", "SELECT 1");
    inbound.extend_from_slice(&error_response("42804", "type mismatch")); // cmd1 Bind error
    inbound.extend_from_slice(&ready_idle());

    let mut transport = ScriptReader { inbound };
    let (boundary, _rows, delivers, fails) = drive(&mut engine, &mut transport);
    assert_eq!(boundary, Boundary::Failed);
    assert_eq!(delivers, 1, "cmd0 delivered; cmd1 never did");
    assert_eq!(fails, 1);

    let (boundary2, _r, d2, _f) = drive(&mut engine, &mut transport);
    assert_eq!(boundary2, Boundary::Idle, "drains to a clean idle");
    assert_eq!(d2, 0);
}
