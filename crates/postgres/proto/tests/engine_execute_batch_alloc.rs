//! Constant-SEND-memory gate for the homogeneous `execute_batch` windowed drive.
//!
//! `execute_batch` streams ONE `query!` write carrier against N parameter sets,
//! Parse-once, flushing the send buffer at the batcher threshold (with a `Flush`,
//! then DRAINING the window's responses before staging the next — the deadlock-free
//! peer of the COPY batcher). The claim PINNED here:
//!
//! **Staged-bytes high-water is INDEPENDENT of N.** The send buffer never holds all
//! N `Bind` frames: it is bounded to strictly under `2 × THRESHOLD` regardless of
//! how many parameter sets the batch has, so streaming 10× the commands buffers the
//! SAME peak bytes. A regression that accumulated all N binds (a single unbounded
//! flush) would make the peak grow with N — AND deadlock a real server that answers
//! per command.
//!
//! This reproduces the driver's `Core::execute_batch` windowed loop over the engine's
//! own verbs (`stage_execute_batch_command` / `pending_send_len` / `stage_flush` /
//! `run_pipeline_break` / `stage_pipeline_seal` / `run_pipeline`) — exactly as
//! `engine_query_break_alloc` reproduces the driver's stream loop — against an
//! in-process fake that answers each command's `Bind`/`Execute` and each `Sync`.
//!
//! # One test, one binary, on purpose
//!
//! The counting allocator is process-global; this gate lives in its OWN binary with
//! a SINGLE `#[test]` fn so no sibling allocates inside a measured window.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    reason = "alloc-gate harness — expect/panic are the loud test-failure signal; the const wire builders and frame scanner are bounded by fixed string/frame sizes"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bsql_postgres_proto::engine::{open_owned, poll_once, Boundary, Outcome, Surface, Transport};
use bsql_postgres_proto::prepared::new_prepared_query;
use bsql_postgres_proto::{Credentials, Ident, PreparedQuery};

// The driver's `execute_batch` window threshold (mirrored here — the gate proves
// the send buffer stays under 2× this regardless of N).
const THRESHOLD: usize = 64 * 1024;

// ───────────────────── responding in-process fake ─────────────────────

/// A minimal bidirectional fake: on `write` it SCANS the client's frames and
/// queues the matching backend replies (`Parse`→ParseComplete, `Bind`→BindComplete,
/// `Close`→CloseComplete, `Execute`→CommandComplete, `Sync`→ReadyForQuery; `Flush`
/// queues nothing — it only makes the buffered replies readable). On `read` it serves
/// the queued bytes. The FIRST write (the startup packet, which is not a tagged frame
/// stream) is not parsed. `max_write` (the flushed send-buffer high-water) lives in a
/// shared `Arc` so the test reads it AFTER the fake is moved into the engine.
struct Fake {
    outbound: Vec<u8>, // pre-queued handshake replies + dynamically-queued batch replies
    read_cursor: usize,
    writes: usize,
    max_write: Arc<AtomicUsize>,
}

impl Fake {
    fn new(max_write: Arc<AtomicUsize>) -> Self {
        Self {
            outbound: handshake(),
            read_cursor: 0,
            writes: 0,
            max_write,
        }
    }

    /// Scan the client's just-written batch frames and queue the matching replies.
    fn absorb(&mut self, mut buf: &[u8]) {
        while buf.len() >= 5 {
            let tag = buf[0];
            let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            let frame_end = 1 + len; // tag byte + (length field, which includes itself)
            if len < 4 || buf.len() < frame_end {
                break;
            }
            match tag {
                b'P' => self.outbound.extend_from_slice(&frame(b'1', &[])), // ParseComplete
                b'B' => self.outbound.extend_from_slice(&frame(b'2', &[])), // BindComplete
                b'C' => self.outbound.extend_from_slice(&frame(b'3', &[])), // CloseComplete
                b'E' => self.outbound.extend_from_slice(&command_complete()), // per Execute
                b'S' => self.outbound.extend_from_slice(&frame(b'Z', b"I")), // ReadyForQuery(idle)
                b'H' => {}                                                   // Flush: no reply
                _ => break, // out of sync — never happens on a valid batch stream
            }
            buf = &buf[frame_end..];
        }
    }
}

impl Transport for Fake {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        let n = (self.outbound.len() - self.read_cursor).min(buf.len());
        let end = self.read_cursor + n;
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), self.outbound.get(self.read_cursor..end))
        {
            dst.copy_from_slice(src);
        }
        self.read_cursor = end;
        ready(Ok(n))
    }
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        self.writes += 1;
        // Skip the startup packet (write #1 — not a tagged frame stream). Every
        // subsequent write is a batch flush: record its size + queue its replies.
        if self.writes > 1 {
            let prev = self.max_write.load(Ordering::Relaxed);
            if buf.len() > prev {
                self.max_write.store(buf.len(), Ordering::Relaxed);
            }
            self.absorb(buf);
        }
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

fn command_complete() -> Vec<u8> {
    frame(b'C', b"UPDATE 1\0")
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
    let mut out = frame(b'R', &0_i32.to_be_bytes());
    out.extend_from_slice(&param_status("client_encoding", "UTF8"));
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &key));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

// ─────────────────────────── prepared-query fixture ───────────────────────────
//
// A non-row write carrier (`Row = ()`): `stage_execute_batch_command` puts
// Close+Parse (cmd 0) / bare Bind+Execute (subsequent) on the wire, exactly the
// driver's Parse-once shape. One int4 param per command.

const SQL: &str = "UPDATE eb SET v = v + $1::int4 WHERE id = $1::int4";
const STMT: &str = "bsql_eb_gate";
const PARAM_OIDS: &[u32] = &[23];
const PARSE_LEN: usize = 1 + 4 + STMT.len() + 1 + SQL.len() + 1 + 2 + 4 * PARAM_OIDS.len();
const PARSE: [u8; PARSE_LEN] = build_parse_template::<PARSE_LEN>(STMT, SQL, PARAM_OIDS);
const BIND_LEN: usize = 1 + STMT.len() + 1;
const BIND: [u8; BIND_LEN] = build_bind_prefix::<BIND_LEN>(STMT);

static QUERY: PreparedQuery<(i32,), ()> =
    new_prepared_query::<(i32,), ()>(SQL, STMT, &PARSE, &BIND);

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

/// Reproduce `Core::execute_batch`'s windowed drive at the engine level and return
/// the send-buffer high-water (the largest single `write`) for a batch of `n`
/// commands. Correctness of the drive is proven live; this measures ONLY the
/// staged-bytes peak, which the claim asserts is INDEPENDENT of `n`.
fn max_staged_bytes(n: usize) -> usize {
    assert!(n >= 1, "an empty batch does no wire I/O");
    let max_write = Arc::new(AtomicUsize::new(0));
    let user = Ident::try_from_str("gate").expect("valid ident");
    let (mut engine, live) = open_owned(
        Fake::new(Arc::clone(&max_write)),
        &user,
        None,
        &[],
        Credentials::Trust,
    )
    .expect("session assembles");
    let mut live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        other => panic!("handshake: {other:?}"),
    };

    // Command 0 (Parse-once first).
    engine
        .stage_execute_batch_command(&QUERY, &(0_i32,), true)
        .expect("stage first");
    let mut total = 1usize; // staged commands
    let mut current = 0usize; // globally delivered commands
    let mut i = 1usize;
    while i < n {
        // Fill the current window.
        let mut window_full = false;
        while i < n {
            let arg = i32::try_from(i % 1000).expect("small");
            engine
                .stage_execute_batch_command(&QUERY, &(arg,), false)
                .expect("stage next");
            total += 1;
            i += 1;
            if engine.pending_send_len() >= THRESHOLD {
                window_full = true;
                break;
            }
        }
        if !window_full {
            break; // final window handled below
        }
        // Intermediate window: Flush, then DRAIN this window's responses (break at
        // the global delivered target) so the buffer is reset before the next window.
        engine.stage_flush();
        let window_target = total;
        let outcome = poll_once(engine.run_pipeline_break::<_, ()>(live, |surface| {
            if matches!(surface, Surface::Deliver { .. }) {
                current += 1;
                if current >= window_target {
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        }));
        live = match outcome {
            Ok(Ok(Outcome {
                live,
                status: Boundary::Stopped(()),
            })) => live,
            other => panic!("window drive: {other:?}"),
        };
    }

    // Final window: the ONE trailing Sync, drive to the batch RFQ.
    engine.stage_pipeline_seal();
    match poll_once(engine.run_pipeline(live, |surface| {
        let _ = core::hint::black_box(surface);
        ControlFlow::Continue(())
    })) {
        Ok(Ok(Outcome { .. })) => {}
        other => panic!("final drive: {other:?}"),
    }
    max_write.load(Ordering::Relaxed)
}

/// PINNED: the send-buffer high-water is bounded to STRICTLY UNDER `2 × THRESHOLD`
/// AND is IDENTICAL for a small-N and a large-N (100×) batch — the staged-bytes peak
/// is independent of N. A regression that buffered all N binds (a single unbounded
/// flush) would make the large-N peak grow ~100× and blow the bound.
#[test]
fn staged_send_bytes_are_independent_of_n() {
    // Small enough to cross ONE window boundary, and 100× that.
    let small = max_staged_bytes(4_000);
    let large = max_staged_bytes(400_000);
    assert!(
        small < 2 * THRESHOLD,
        "small-N send high-water {small} must be < 2×THRESHOLD ({})",
        2 * THRESHOLD,
    );
    assert!(
        large < 2 * THRESHOLD,
        "large-N send high-water {large} must be < 2×THRESHOLD ({}) — a single unbounded flush would blow this",
        2 * THRESHOLD,
    );
    assert_eq!(
        small, large,
        "the staged-bytes high-water is INDEPENDENT of N (small={small}, large={large})",
    );
}
