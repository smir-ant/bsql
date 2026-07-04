//! Allocation gates for the `query_params` connection lifecycle: the handshake
//! budget and the cache-HIT steady state.
//!
//! Installs the workspace counting allocator and brackets two windows over a
//! session driven against an in-process scripted transport (no socket, no
//! thread). The claims PINNED here:
//!
//! 1. **Cache-HIT steady state is zero-alloc.** Once a statement is Parsed on
//!    the connection and the engine's send/ingest buffers are warm, a
//!    `query_params` cache HIT (Bind+Execute+Sync, reply framed and surfaced)
//!    performs ZERO allocations. This is the flagship "the hot query path does
//!    not allocate" claim, proven rather than asserted.
//! 2. **The handshake allocates a bounded, pinned budget.** Connecting from
//!    the connecting phase to active (AuthenticationOk → BackendKeyData →
//!    ReadyForQuery) allocates a fixed number of times. The current count is
//!    PINNED as a const: an honest baseline, not an aspirational zero. A later
//!    slice that trims it must lower the pin — a visible, reviewed number.
//!
//! # One test, on purpose
//!
//! The counting allocator is process-global and counts every thread. `cargo
//! test` runs `#[test]` fns in parallel, so all measured windows live in a
//! SINGLE `#[test]` fn run sequentially — no concurrent test thread can
//! allocate inside a measured window. (Other test binaries are separate
//! processes with their own allocator instance.)

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "alloc-gate harness — expect/panic are the loud test-failure signal, not production fallbacks"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

use bsql_devgates::CountingAllocator;
use bsql_postgres_proto::engine::{
    open_owned, poll_once, Never, Outcome, Surface, Transport,
};
use bsql_postgres_proto::prepared::new_prepared_query;
use bsql_postgres_proto::{Credentials, Ident, PreparedQuery};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::new();

// ─────────────────────────── scripted transport ───────────────────────────

struct Script {
    inbound: Vec<u8>,
    cursor: usize,
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
    let len = u32::try_from(body.len() + 4).expect("frame body fits u32 length");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A `ParameterStatus` frame: `'S' | len | key\0 value\0`.
fn param_status(key: &str, value: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(key.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(b'S', &body)
}

/// AuthenticationOk, a realistic run of startup GUC `ParameterStatus` frames
/// (the engine's `connect` captures `server_version` from these into one owned
/// `String` and ignores the rest — no consumer), BackendKeyData,
/// ReadyForQuery(Idle).
fn handshake() -> Vec<u8> {
    let mut out = frame(b'R', &0_i32.to_be_bytes());
    for (k, v) in [
        ("application_name", ""),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("server_encoding", "UTF8"),
        ("server_version", "16.2"),
        ("standard_conforming_strings", "on"),
        ("TimeZone", "UTC"),
    ] {
        out.extend_from_slice(&param_status(k, v));
    }
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn demo_row(id: i32, name: &str) -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    body.extend_from_slice(&4_i32.to_be_bytes());
    body.extend_from_slice(&id.to_be_bytes());
    let name_len = i32::try_from(name.len()).expect("name fits i32");
    body.extend_from_slice(&name_len.to_be_bytes());
    body.extend_from_slice(name.as_bytes());
    frame(b'D', &body)
}

fn command_tail(rows: usize) -> Vec<u8> {
    let mut cc = format!("SELECT {rows}").into_bytes();
    cc.push(0);
    let mut out = frame(b'C', &cc);
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

/// CloseComplete, ParseComplete, BindComplete, one DataRow, then the tail.
fn miss_reply() -> Vec<u8> {
    let mut out = frame(b'3', &[]);
    out.extend_from_slice(&frame(b'1', &[]));
    out.extend_from_slice(&frame(b'2', &[]));
    out.extend_from_slice(&demo_row(0, "hit"));
    out.extend_from_slice(&command_tail(1));
    out
}

/// BindComplete, one DataRow, then the tail (no Close/Parse — the HIT reply).
fn hit_reply() -> Vec<u8> {
    let mut out = frame(b'2', &[]);
    out.extend_from_slice(&demo_row(0, "hit"));
    out.extend_from_slice(&command_tail(1));
    out
}

// ─────────────────────────── prepared-query fixture ───────────────────────────

const DEMO_SQL: &str = "SELECT id::int4, name::text FROM demo WHERE id = $1::int4";
const DEMO_STMT: &str = "bsql_gate_demo";
const DEMO_PARAM_OIDS: &[u32] = &[23];
const DEMO_RESULT_OIDS: &[u32] = &[23, 25];

const DEMO_PARSE_LEN: usize =
    1 + 4 + DEMO_STMT.len() + 1 + DEMO_SQL.len() + 1 + 2 + 4 * DEMO_PARAM_OIDS.len();
const DEMO_PARSE: [u8; DEMO_PARSE_LEN] =
    build_parse_template::<DEMO_PARSE_LEN>(DEMO_STMT, DEMO_SQL, DEMO_PARAM_OIDS);
const DEMO_BIND_LEN: usize = 1 + DEMO_STMT.len() + 1;
const DEMO_BIND: [u8; DEMO_BIND_LEN] = build_bind_prefix::<DEMO_BIND_LEN>(DEMO_STMT);

static DEMO_QUERY: PreparedQuery<(i32,), (i32, &'static str)> =
    new_prepared_query::<(i32,), (i32, &'static str)>(
        DEMO_SQL,
        DEMO_STMT,
        DEMO_PARAM_OIDS,
        DEMO_RESULT_OIDS,
        &DEMO_PARSE,
        &DEMO_BIND,
    );

#[allow(
    clippy::as_conversions,
    reason = "const-fn wire builder in a test target — lengths are bounded by the const string sizes; the workspace floor forbids only cast_sign_loss (usize→u32/u16 is not sign-losing), never plain `as`"
)]
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

fn no_op_sink(surface: Surface<'_>) -> ControlFlow<Never> {
    let _ = core::hint::black_box(surface);
    ControlFlow::Continue(())
}

// ─────────────────────────── the gate ───────────────────────────

/// PINNED baseline: allocations charged to the trust handshake (connect →
/// active) over a REALISTIC reply (AuthenticationOk + eight startup GUC
/// `ParameterStatus` frames + BackendKeyData + ReadyForQuery).
///
/// The current value is **2**, both honest one-time costs:
///
/// 1. The connecting ingest buffer's ONE-TIME heap escape: a realistic
///    handshake reply exceeds the 128-byte inline ingest tier, so the buffer
///    escapes to a heap array exactly once. (A later slice that sizes the
///    connecting inline tier to fit a typical handshake would drive this to 0.)
/// 2. The captured `server_version` `String` — the value a `SHOW server_version`
///    would return, now carried from the handshake for free. This one owned
///    allocation replaces the old post-connect `SHOW` round-trip, which cost a
///    full network round-trip PLUS a whole `QueryResult` (four allocations) to
///    recover the same string. Trading one cold-path `String` for a round-trip
///    is a large net win, so this pin rises by one rather than the round-trip
///    being kept.
///
/// An honest baseline, not an aspirational zero. A later slice that trims either
/// cost must lower this pin — a visible, reviewed number.
const HANDSHAKE_ALLOC_PIN: usize = 2;

#[test]
fn handshake_budget_pinned_and_cache_hit_is_zero_alloc() {
    let user = Ident::try_from_str("gate").expect("valid ident");

    // Reply stream: handshake, priming MISS, warm-up HIT, then the MEASURED HIT.
    let mut inbound = handshake();
    inbound.extend_from_slice(&miss_reply());
    inbound.extend_from_slice(&hit_reply()); // warm-up
    inbound.extend_from_slice(&hit_reply()); // measured

    let (mut engine, live) =
        open_owned(Script { inbound, cursor: 0 }, &user, None, &[], Credentials::Trust)
            .expect("session assembles");

    // ---- (1) Handshake budget: bracket connect → active. ----
    let before = ALLOC.snapshot();
    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake must reach active, got {other:?}"),
    };
    let handshake_allocs = ALLOC.snapshot().delta(before).allocs;

    // ---- Prime the statement cache (MISS) + one warm-up HIT, UNTIMED. This
    // grows the send buffer once and records the statement, so the measured HIT
    // runs entirely on warm buffers. ----
    let live = match poll_once(engine.query_params(live, &DEMO_QUERY, (0,), no_op_sink)) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("priming MISS must complete, got {other:?}"),
    };
    let live = match poll_once(engine.query_params(live, &DEMO_QUERY, (0,), no_op_sink)) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("warm-up HIT must complete, got {other:?}"),
    };

    // ---- (2) The MEASURED cache HIT: must allocate nothing. ----
    let before = ALLOC.snapshot();
    let outcome = poll_once(engine.query_params(live, &DEMO_QUERY, (0,), no_op_sink));
    let hit_allocs = ALLOC.snapshot().delta(before).allocs;
    match outcome {
        Ok(Ok(Outcome { live, .. })) => {
            let _ = live;
        }
        other => panic!("measured HIT must complete, got {other:?}"),
    }

    // ---- Assertions. ----
    assert_eq!(
        hit_allocs, 0,
        "a warm cache-HIT query_params must not allocate (got {hit_allocs}) — \
         the send/ingest buffers are reused and the no-op sink accumulates nothing"
    );
    assert_eq!(
        handshake_allocs, HANDSHAKE_ALLOC_PIN,
        "handshake allocation budget drifted from its pin ({HANDSHAKE_ALLOC_PIN}): \
         got {handshake_allocs}. This is a pinned baseline — if a change legitimately \
         alters it, update HANDSHAKE_ALLOC_PIN with the new reviewed number."
    );
}
