//! Offline gate for the WINDOWED pipeline's result-OID guard BAIL.
//!
//! A heterogeneous `pipeline((...))` STREAMS its commands: it stages until the send
//! buffer crosses the batcher threshold, then `Flush`es and DRAINS that window's
//! responses before staging the next (constant send memory, deadlock-free — the
//! peer of `execute_batch`'s windowed drive). An INTERMEDIATE window ends with a
//! `Flush`, NOT a `Sync`, so it has NO trailing `ReadyForQuery`.
//!
//! The pipeline also GUARDS each cache-MISS command's result schema (it appends a
//! `Describe`(portal) and checks the runtime column OIDs against the carrier's
//! compile-time `row_oids`). A drifted MISS command PARKS a mismatch and enters the
//! silent swallow-to-`ReadyForQuery` drain — but in an INTERMEDIATE window that RFQ
//! is never coming, so the swallow would BLOCK FOREVER on the next read.
//!
//! The fix: the intermediate-window drive is [`Engine::run_pipeline_break_guarded`],
//! which BAILS (returns [`Boundary::Failed`]) the moment a mismatch is parked and the
//! drain would otherwise block on a read — so the driver can stage the batch's single
//! trailing `Sync` and drain the mismatch to the recovering RFQ. THIS gate pins that
//! bail directly:
//!
//! - the GUARDED drive over a drifted-then-truncated window returns `Boundary::Failed`
//!   (the bail) and the mismatch triple is retrievable — NO hang, NO read past the
//!   window;
//! - the UN-guarded `run_pipeline_break` over the SAME bytes reads PAST the window
//!   into EOF (`Err`) — proving the bail is exactly what a windowed guarded pipeline
//!   needs, and that `execute_batch`'s (unguarded) drive is deliberately inert here.
//!
//! Scripted transport (no network): the inbound queue is the drifted command's acks
//! WITHOUT a trailing `ReadyForQuery` (an intermediate window's `Flush` produces no
//! reply), and `read` returns `Ok(0)` once exhausted — the offline stand-in for "the
//! server sent nothing more until the (not-yet-staged) trailing `Sync`".

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    reason = "offline gate harness — expect/panic are the loud failure signal; the const wire builders are bounded by fixed string sizes"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

use bsql_postgres_proto::engine::{
    open_owned, poll_once, Boundary, Engine, Live, Outcome, Surface, Transport,
};
use bsql_postgres_proto::prepared::new_prepared_query;
use bsql_postgres_proto::{Credentials, Ident, PreparedQuery};

// ─────────────────────────── scripted transport ───────────────────────────

/// A scripted reader: `read` drains a fixed inbound queue and returns `Ok(0)` once
/// exhausted (EOF); writes / flush / shutdown are no-op. `Ok(0)` is the offline
/// stand-in for "no more bytes until the trailing `Sync`" — a drive that reads past
/// the window's bytes hits it and errors (`UnexpectedEof`), while the GUARDED drive
/// BAILS before that read.
struct Script {
    inbound: Vec<u8>,
    cursor: usize,
}

impl Script {
    fn new(inbound: Vec<u8>) -> Self {
        Self { inbound, cursor: 0 }
    }
}

impl Transport for Script {
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

// ─────────────────────────── frame builders ───────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame body fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn param_status(key: &str, value: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(key.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(b'S', &body)
}

fn handshake() -> Vec<u8> {
    let mut out = frame(b'R', &0_i32.to_be_bytes()); // AuthenticationOk
    out.extend_from_slice(&param_status("client_encoding", "UTF8"));
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key)); // BackendKeyData
    out.extend_from_slice(&frame(b'Z', b"I")); // ReadyForQuery(Idle)
    out
}

/// A one-column `RowDescription` reporting `type_oid` for its single column.
fn row_description_1col(type_oid: u32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1_i16.to_be_bytes()); // 1 column
    body.extend_from_slice(b"c0\0"); // column name
    body.extend_from_slice(&0_u32.to_be_bytes()); // table oid
    body.extend_from_slice(&0_u16.to_be_bytes()); // column attr
    body.extend_from_slice(&type_oid.to_be_bytes()); // TYPE OID (the guarded field)
    body.extend_from_slice(&0_u16.to_be_bytes()); // typlen
    body.extend_from_slice(&0_u32.to_be_bytes()); // typmod
    body.extend_from_slice(&1_u16.to_be_bytes()); // binary format
    frame(b'T', &body)
}

fn data_row_1col(val: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&1_i16.to_be_bytes());
    body.extend_from_slice(&i32::try_from(val.len()).expect("len").to_be_bytes());
    body.extend_from_slice(val);
    frame(b'D', &body)
}

fn command_complete() -> Vec<u8> {
    frame(b'C', b"SELECT 1\0")
}

/// The inbound bytes of ONE GUARDED cache-MISS command whose `Describe` answer
/// reports `runtime_oid`: `CloseComplete` + `ParseComplete` + `BindComplete` +
/// `RowDescription(runtime_oid)` + a `DataRow` + `CommandComplete`. NO trailing
/// `ReadyForQuery` — this is an INTERMEDIATE window (a `Flush`, no `Sync`).
fn drifted_miss_window(runtime_oid: u32) -> Vec<u8> {
    let mut out = handshake();
    out.extend_from_slice(&frame(b'3', &[])); // CloseComplete
    out.extend_from_slice(&frame(b'1', &[])); // ParseComplete
    out.extend_from_slice(&frame(b'2', &[])); // BindComplete
    out.extend_from_slice(&row_description_1col(runtime_oid)); // Describe answer (DRIFTED)
    out.extend_from_slice(&data_row_1col(b"AAAA")); // swallowed by the mismatch drain
    out.extend_from_slice(&command_complete()); // swallowed by the mismatch drain
    out
}

// ─────────────────────────── guarded carrier fixture ───────────────────────────
//
// A row-shaped carrier `(&'static str,)` → `row_oids = [25]` (text). One int4 param.
// A drifted `RowDescription` reporting int4 (23) is INCOMPATIBLE with the text (25)
// the carrier expects → the guard records a `ColumnOidMismatch { index: 0, expected:
// 25, found: 23 }`.

const SQL: &str = "SELECT $1::text AS s";
const STMT: &str = "bsql_pl_guard_gate";
const PARAM_OIDS: &[u32] = &[23];
const PARSE_LEN: usize = 1 + 4 + STMT.len() + 1 + SQL.len() + 1 + 2 + 4 * PARAM_OIDS.len();
const PARSE: [u8; PARSE_LEN] = build_parse_template::<PARSE_LEN>(STMT, SQL, PARAM_OIDS);
const BIND_LEN: usize = 1 + STMT.len() + 1;
const BIND: [u8; BIND_LEN] = build_bind_prefix::<BIND_LEN>(STMT);

static QUERY: PreparedQuery<(i32,), (&'static str,)> =
    new_prepared_query::<(i32,), (&'static str,)>(SQL, STMT, &PARSE, &BIND);

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

const fn build_bind_prefix<const N: usize>(stmt: &str) -> [u8; N] {
    let mut buf = [0u8; N];
    let stmt_b = stmt.as_bytes();
    let mut i = 1;
    let mut j = 0;
    while j < stmt_b.len() {
        buf[i] = stmt_b[j];
        i += 1;
        j += 1;
    }
    buf
}

/// Build an engine over `inbound`, past the handshake, and stage ONE guarded
/// cache-MISS pipeline command (`first = true`) + a window `Flush` — the exact
/// intermediate-window state the driver reaches before draining.
fn staged_intermediate_window(inbound: Vec<u8>) -> (Engine<'static, Script>, Live<'static>) {
    let user = Ident::try_from_str("gate").expect("ident");
    let (mut engine, live) =
        open_owned(Script::new(inbound), &user, None, &[], Credentials::Trust)
            .expect("session assembles");
    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake: {other:?}"),
    };
    engine
        .stage_pipeline_command(&QUERY, &(1_i32,), true)
        .expect("stage guarded MISS command 0");
    engine.stage_flush(); // intermediate window boundary
    (engine, live)
}

/// A never-breaking window sink (the mismatch surfaces NO `Deliver`, so a
/// delivery-counting break condition is never met — the BAIL is what stops the pump).
fn count_sink(surface: Surface<'_>) -> ControlFlow<()> {
    let _ = core::hint::black_box(surface);
    ControlFlow::Continue(())
}

/// THE BAIL. The GUARDED intermediate-window drive over a drifted-then-truncated
/// window returns `Boundary::Failed` (NOT a hang, NOT an EOF error) and the mismatch
/// triple `{ index: 0, expected: 25 (text), found: 23 (int4) }` is retrievable — so
/// the driver stops this window, stages the trailing `Sync`, and returns the
/// classified `BatchColumnOidMismatch`.
#[test]
fn guarded_window_bails_on_a_parked_mismatch_without_reading_past_the_window() {
    let (mut engine, live) = staged_intermediate_window(drifted_miss_window(23));

    let status = match poll_once(engine.run_pipeline_break_guarded::<_, ()>(live, count_sink)) {
        Ok(Ok(Outcome { status, .. })) => status,
        other => panic!("guarded window drive did not resolve: {other:?}"),
    };
    assert_eq!(
        status,
        Boundary::Failed,
        "the guarded drive BAILS (Failed) the moment a mismatch parks — never reads past the \
         intermediate window into EOF",
    );
    // The parked mismatch is the retrievable classified triple the driver's settle
    // turns into `BatchColumnOidMismatch`.
    assert_eq!(
        engine.take_result_oid_mismatch(),
        Some((0, 23, 25)),
        "the drifted column (int4 23 where text 25 was expected) is recorded as the mismatch",
    );
}

/// THE CONTRAST. The UN-guarded `run_pipeline_break` over the SAME bytes does NOT
/// bail — it reads PAST the window's frames into the scripted EOF and errors. This
/// is exactly why a windowed GUARDED pipeline needs `run_pipeline_break_guarded`
/// (and why the unguarded `execute_batch` — which can never park a mismatch — keeps
/// the inert `run_pipeline_break`).
#[test]
fn unguarded_window_reads_past_the_truncated_window_into_eof() {
    let (mut engine, live) = staged_intermediate_window(drifted_miss_window(23));

    // The mismatch still parks (the guard is a staging property), but the UNguarded
    // drive keeps reading after the drain exhausts the window's bytes → EOF.
    let outcome = poll_once(engine.run_pipeline_break::<_, ()>(live, count_sink));
    match outcome {
        Ok(Err(_)) | Err(_) => {} // an engine error (UnexpectedEof) — read past the window
        Ok(Ok(Outcome { status, .. })) => panic!(
            "the UNguarded drive must NOT resolve to a boundary over a truncated window — it read \
             past into EOF; got {status:?}",
        ),
    }
}

/// A guarded drive over a MATCHING window (the guard passes) reaches the window's
/// delivery and then EOF exactly like the unguarded one — the bail is scoped to a
/// PARKED MISMATCH only, never a false early stop on a clean window.
#[test]
fn guarded_window_does_not_bail_when_the_schema_matches() {
    // Runtime OID 25 (text) MATCHES the carrier's row_oids [25] → NO mismatch parks.
    let (mut engine, live) = staged_intermediate_window(drifted_miss_window(25));

    // With no parked mismatch, the guarded drive behaves like the unguarded one: it
    // delivers the command, then reads past the truncated window into EOF (there is
    // no trailing `Sync` in this fixture) — it must NOT resolve to a `Boundary::Failed`
    // via a (nonexistent) parked mismatch.
    // Delivered-then-EOF (Err) is expected (no `Sync` in this fixture); a
    // `Boundary::Failed` would mean the bail over-fired on a schema that MATCHED.
    if let Ok(Ok(Outcome { status: Boundary::Failed, .. })) =
        poll_once(engine.run_pipeline_break_guarded::<_, ()>(live, count_sink))
    {
        panic!("guarded drive bailed on a MATCHING window — the mismatch bail over-fired");
    }
    assert_eq!(
        engine.take_result_oid_mismatch(),
        None,
        "a matching schema parks NO mismatch — the bail is scoped to a real drift only",
    );
}
