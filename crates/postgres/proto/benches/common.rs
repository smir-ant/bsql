//! Shared fixtures for the engine hot-path benches.
//!
//! Everything here is offline and deterministic: an in-process, single-threaded,
//! synchronous scripted [`Transport`] (no socket, no server thread) plus a
//! [`PreparedQuery`] fixture built the same way the compile-checked `query!`
//! macro builds one — through [`new_prepared_query`], the sole validating
//! constructor, fed const-derived Parse/Bind wire the way a macro bakes it. The
//! benched routines therefore drive the REAL engine verbs over REAL wire; only
//! the socket is replaced by a canned reply cursor.
//!
//! The module is `mod common;`-included by each bench (not an auto-discovered
//! bench target — `autobenches = false` in `Cargo.toml`), so its helpers compile
//! once into the bench that uses them.

#![allow(
    dead_code,
    reason = "bench support module: each bench uses a subset of these helpers, so items unused by one bench are not dead across the module's consumers"
)]
#![allow(
    clippy::expect_used,
    reason = "bench harness — expect() is the loud fixture-failure signal; a bench is never a #[test] context, so the floor's allow-in-tests carve-out cannot reach it, and a bench fixture has no production data-fallback path"
)]
#![allow(
    clippy::panic,
    reason = "bench harness — panic on an impossible fixture state is the loud failure signal, never a production fallback; the in-tests carve-out cannot reach a bench"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

use bsql_postgres_proto::engine::{
    open_owned, poll_once, Engine, Live, Outcome, Surface, Transport,
};
use bsql_postgres_proto::prepared::new_prepared_query;
use bsql_postgres_proto::{Credentials, Ident, PreparedQuery};

// ─────────────────────────── scripted transport ───────────────────────────

/// A static cursor server: `read` drains a fixed reply buffer starting at
/// `cursor`; writes are accepted and discarded; every op resolves synchronously
/// (one `poll_once`). Constructing it (and its inbound `Vec`) happens OUTSIDE
/// any timed window.
pub struct Script {
    inbound: Vec<u8>,
    cursor: usize,
}

impl Script {
    /// Build a cursor server over a canned reply stream.
    #[must_use]
    pub fn new(inbound: Vec<u8>) -> Self {
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

/// A single length-prefixed backend frame: `tag | len_i32_be(self+body) | body`.
#[must_use]
pub fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame body fits u32 length prefix");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// AuthenticationOk + BackendKeyData + ReadyForQuery(Idle) — the trust handshake.
#[must_use]
pub fn handshake() -> Vec<u8> {
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    let mut out = frame(b'R', &0_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

/// One `[int4=id][text=name]` `DataRow` matching the demo query's `(i32, &str)`
/// row shape.
#[must_use]
pub fn demo_row(id: i32, name: &str) -> Vec<u8> {
    let mut body = 2_i16.to_be_bytes().to_vec();
    // int4 column: 4-byte length + big-endian value.
    body.extend_from_slice(&4_i32.to_be_bytes());
    body.extend_from_slice(&id.to_be_bytes());
    // text column: length + raw bytes.
    let name_len = i32::try_from(name.len()).expect("name fits i32");
    body.extend_from_slice(&name_len.to_be_bytes());
    body.extend_from_slice(name.as_bytes());
    frame(b'D', &body)
}

/// CommandComplete(`SELECT n`) + ReadyForQuery(Idle) — the tail of a result.
fn command_tail(rows: usize) -> Vec<u8> {
    let mut cc = format!("SELECT {rows}").into_bytes();
    cc.push(0);
    let mut out = frame(b'C', &cc);
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

/// The cache-MISS reply for `query_params`: the statement is not yet parsed, so
/// the verb sends Close+Parse+Bind+Execute+Sync and the server answers
/// CloseComplete, ParseComplete, BindComplete, `rows` DataRows, then the tail.
#[must_use]
pub fn miss_reply(rows: usize) -> Vec<u8> {
    let mut out = frame(b'3', &[]); // CloseComplete
    out.extend_from_slice(&frame(b'1', &[])); // ParseComplete
    out.extend_from_slice(&frame(b'2', &[])); // BindComplete
    for i in 0..rows {
        let id = i32::try_from(i).expect("row index fits i32");
        out.extend_from_slice(&demo_row(id, "bench"));
    }
    out.extend_from_slice(&command_tail(rows));
    out
}

/// The cache-HIT reply for `query_params`: the statement is already parsed, so
/// the verb sends only Bind+Execute+Sync and the server answers BindComplete,
/// `rows` DataRows, then the tail (no CloseComplete/ParseComplete).
#[must_use]
pub fn hit_reply(rows: usize) -> Vec<u8> {
    let mut out = frame(b'2', &[]); // BindComplete
    for i in 0..rows {
        let id = i32::try_from(i).expect("row index fits i32");
        out.extend_from_slice(&demo_row(id, "bench"));
    }
    out.extend_from_slice(&command_tail(rows));
    out
}

// ─────────────────────────── prepared-query fixture ───────────────────────────

/// The demo query's SQL. Two result columns (`int4`, `text`) and one `int4`
/// bind parameter — the same `(i32, &str)` row / `(i32,)` param shape the
/// corpus's differential fixture uses, so the wire is representative.
pub const DEMO_SQL: &str = "SELECT id::int4, name::text FROM demo WHERE id = $1::int4";
/// A stable statement name for the fixture. Its exact value is not load-bearing
/// here (the bench owns both the request and the canned reply); a real `query!`
/// uses the SHA-256-96 content address of the SQL.
pub const DEMO_STMT: &str = "bsql_bench_demo";
const DEMO_PARAM_OIDS: &[u32] = &[23]; // int4

const DEMO_PARSE_LEN: usize =
    1 + 4 + DEMO_STMT.len() + 1 + DEMO_SQL.len() + 1 + 2 + 4 * DEMO_PARAM_OIDS.len();
const DEMO_PARSE: [u8; DEMO_PARSE_LEN] =
    build_parse_template::<DEMO_PARSE_LEN>(DEMO_STMT, DEMO_SQL, DEMO_PARAM_OIDS);
const DEMO_BIND_LEN: usize = 1 + DEMO_STMT.len() + 1;
const DEMO_BIND: [u8; DEMO_BIND_LEN] = build_bind_prefix::<DEMO_BIND_LEN>(DEMO_STMT);

/// The benched prepared query, minted through the sole validating constructor.
/// The `const` validator inside `new_prepared_query` sources the param / row
/// OIDs from the `(i32,)` / `(i32, &str)` tuples and rejects a baked `Parse`
/// template whose OID section drifts from them.
pub static DEMO_QUERY: PreparedQuery<(i32,), (i32, &'static str)> =
    new_prepared_query::<(i32,), (i32, &'static str)>(
        DEMO_SQL,
        DEMO_STMT,
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
    let mut i = 1; // buf[0] is the empty-portal NUL (already 0).
    let mut j = 0;
    while j < stmt_b.len() {
        buf[i] = stmt_b[j];
        i += 1;
        j += 1;
    }
    // Final byte is the stmt-name NUL (already 0).
    buf
}

// ─────────────────────────── engine priming ───────────────────────────

/// The owned engine handle plus its linear token — the shape `open_owned`
/// returns, threaded through the benched routine.
pub type OwnedEngine = (Engine<'static, Script>, Live<'static>);

/// Build an active engine whose statement cache is PRIMED for [`DEMO_QUERY`],
/// with the transport cursor positioned at exactly one cache-HIT reply.
///
/// Setup work (all UNTIMED when called from an `iter_batched` setup closure):
/// open the owned handle, drive the trust handshake, then run ONE cache-MISS
/// `query_params` to Parse the statement and record it. On return the engine is
/// active + primed and the [`Script`] holds only the trailing HIT reply, so the
/// caller's next `query_params` is a pure cache HIT.
///
/// `hit_rows` is the row count of the trailing HIT reply the routine consumes.
#[must_use]
pub fn primed_engine(hit_rows: usize) -> OwnedEngine {
    let user = Ident::try_from_str("bench").expect("valid ident");
    let mut inbound = handshake();
    inbound.extend_from_slice(&miss_reply(1)); // prime: one-row MISS
    inbound.extend_from_slice(&hit_reply(hit_rows)); // the benched HIT
    let (mut engine, live) =
        open_owned(Script::new(inbound), &user, None, &[], Credentials::Trust)
            .expect("session assembles");

    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake must reach active, got {other:?}"),
    };

    // Prime the cache with one MISS (Parse recorded), consuming the MISS reply.
    let live = match poll_once(engine.query_params(live, &DEMO_QUERY, (0,), sink)) {
        Ok(Ok(Outcome { live, .. })) => live,
        other => panic!("priming MISS must complete, got {other:?}"),
    };

    (engine, live)
}

/// A no-op query sink that observes each surface without accumulating — the
/// minimal work every `query_params` sink must do.
pub fn sink(surface: Surface<'_>) -> ControlFlow<bsql_postgres_proto::engine::Never> {
    let _ = core::hint::black_box(surface);
    ControlFlow::Continue(())
}
