//! Graceful-close verb spec — `<Engine>::terminate`.
//!
//! Drives a real connect → active → `terminate` over a scripted transport that
//! CAPTURES the outbound wire and counts `shutdown` calls, and asserts the
//! externally-observable contract of the session-ending verb:
//!
//! 1. **Wire** — `terminate` pushes exactly the 5-byte `Terminate` frame
//!    (`[b'X', 0, 0, 0, 4]` == `TERMINATE_WIRE_BYTES`) and nothing else.
//! 2. **Teardown** — the transport's `shutdown` (TLS `close_notify` / socket FIN)
//!    is driven exactly once, after the frame is flushed.
//! 3. **Closed phase** — after `terminate` the engine is in its closed phase, so
//!    every phase accessor (`backend_pid` / `tx_status`) is a classified
//!    `WrongPhase`, never a stale value from before the close.
//! 4. **Token consumed** — `terminate` returns `Result<(), _>`, not
//!    `Result<Live, _>`: there is no token to thread into a later verb, so a verb
//!    after `terminate` is a move error (the compile-time half of the invariant;
//!    this file proves the runtime closed-phase half).
//! 5. **Wrong-phase rejection** — `terminate` on a still-connecting engine is a
//!    classified `WrongPhase` before any byte reaches the wire.

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "test harness — a fixture/verb failure is a loud assertion, the sanctioned test-failure signal"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bsql_postgres_proto::engine::{
    poll_once, session, EngineError, Live, SpuriousPending, Transport,
};
use bsql_postgres_proto::wire::{
    TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_READY_FOR_QUERY, TERMINATE_WIRE_BYTES,
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

/// The canonical trust handshake reply (AuthenticationOk + BackendKeyData +
/// ReadyForQuery), reaching an active session with backend pid 4321.
fn handshake() -> Vec<u8> {
    let mut key = 4321_i32.to_be_bytes().to_vec();
    key.extend_from_slice(&8765_i32.to_be_bytes());
    let mut out = Vec::new();
    out.extend_from_slice(&frame(TAG_AUTHENTICATION.byte(), &0_i32.to_be_bytes()));
    out.extend_from_slice(&frame(TAG_BACKEND_KEY_DATA.byte(), &key));
    out.extend_from_slice(&frame(TAG_READY_FOR_QUERY.byte(), b"I"));
    out
}

// ─────────────────────────── capturing server ───────────────────────────

/// Scripted server that CAPTURES the outbound wire and counts `shutdown`s so the
/// test can assert the exact `Terminate` bytes were sent and the write side was
/// torn down. `read` drains a fixed reply; every op resolves synchronously.
struct CaptureServer {
    inbound: Vec<u8>,
    cursor: usize,
    writes: Arc<Mutex<Vec<u8>>>,
    shutdowns: Arc<AtomicUsize>,
}

impl CaptureServer {
    fn new(inbound: Vec<u8>, writes: Arc<Mutex<Vec<u8>>>, shutdowns: Arc<AtomicUsize>) -> Self {
        Self {
            inbound,
            cursor: 0,
            writes,
            shutdowns,
        }
    }
}

impl Transport for CaptureServer {
    type Error = Infallible;

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        let n = (self.inbound.len().saturating_sub(self.cursor)).min(buf.len());
        let end = self.cursor.saturating_add(n);
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
        // Capture synchronously before the future is created — no lock is held
        // across an await. A dropped write would surface as a wire mismatch in the
        // assertions, never a silent corruption.
        if let Ok(mut sink) = self.writes.lock() {
            sink.extend_from_slice(buf);
        }
        ready(Ok(buf.len()))
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        ready(Ok(()))
    }
}

// ─────────────────────────── failing server ───────────────────────────

/// A transport error distinct from `Infallible`, so `write` / `shutdown` can
/// report a classified failure (the error-path teeth need a fallible transport).
#[derive(Debug)]
struct TestIoError;

/// Scripted server whose `write` and `shutdown` failures are arm-able through
/// shared flags AFTER the handshake, so connect's startup write succeeds and only
/// the `terminate` flush / shutdown errors. `read` drains the handshake reply.
struct FailingServer {
    inbound: Vec<u8>,
    cursor: usize,
    fail_write: Arc<AtomicBool>,
    fail_shutdown: Arc<AtomicBool>,
    shutdowns: Arc<AtomicUsize>,
}

impl FailingServer {
    fn new(
        inbound: Vec<u8>,
        fail_write: Arc<AtomicBool>,
        fail_shutdown: Arc<AtomicBool>,
        shutdowns: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            inbound,
            cursor: 0,
            fail_write,
            fail_shutdown,
            shutdowns,
        }
    }
}

impl Transport for FailingServer {
    type Error = TestIoError;

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, TestIoError>> + Send + 'a {
        let n = (self.inbound.len().saturating_sub(self.cursor)).min(buf.len());
        let end = self.cursor.saturating_add(n);
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), self.inbound.get(self.cursor..end)) {
            dst.copy_from_slice(src);
        }
        self.cursor = end;
        ready(Ok(n))
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, TestIoError>> + Send + 'a {
        let out = if self.fail_write.load(Ordering::SeqCst) {
            Err(TestIoError)
        } else {
            Ok(buf.len())
        };
        ready(out)
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), TestIoError>> + Send + 'a {
        ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), TestIoError>> + Send + 'a {
        // Count the attempt FIRST (even on the failing path), so the best-effort
        // shutdown is observable regardless of its result.
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        let out = if self.fail_shutdown.load(Ordering::SeqCst) {
            Err(TestIoError)
        } else {
            Ok(())
        };
        ready(out)
    }
}

/// The error-path summary: did terminate propagate a classified transport error,
/// and is the engine in its closed phase afterwards (the TOTAL invariant)?
struct FailOutcome {
    terminate_is_transport_err: bool,
    closed_after: bool,
}

// ─────────────────────────── harness ───────────────────────────

fn flatten<'b, E>(
    polled: Result<Result<Live<'b>, EngineError<E>>, SpuriousPending>,
) -> Result<Live<'b>, EngineError<E>> {
    match polled {
        Ok(inner) => inner,
        Err(SpuriousPending) => panic!("blocking transport returned Pending"),
    }
}

/// The observable summary a terminate run produces — the engine-dependent facts
/// captured inside the session scope (the wire / shutdown facts are read from the
/// shared handles after the scope returns).
struct TermSummary {
    backend_pid_active: Option<i32>,
    terminate_ok: bool,
    backend_pid_after_is_wrong_phase: bool,
    tx_status_after_is_wrong_phase: bool,
}

// ─────────────────────────── specs ───────────────────────────

#[test]
fn terminate_sends_frame_shuts_down_and_closes() {
    let writes: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdowns: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let user = Ident::try_from_str("term").expect("ident");
    let body_writes = Arc::clone(&writes);

    let summary = session(
        CaptureServer::new(handshake(), Arc::clone(&writes), Arc::clone(&shutdowns)),
        &user,
        None,
        None,
        Credentials::Trust,
        |mut engine, live| {
            let live = flatten(poll_once(engine.connect(live))).expect("connect reaches active");
            // Active phase: the accessor surfaces the handshake's backend pid.
            let backend_pid_active = engine.backend_pid().ok();
            // Drop the handshake's outbound wire so the capture isolates the
            // terminate frame.
            if let Ok(mut w) = body_writes.lock() {
                w.clear();
            }
            // `terminate` consumes the token and returns no `Live`.
            let terminate_ok = matches!(poll_once(engine.terminate(live)), Ok(Ok(())));
            TermSummary {
                backend_pid_active,
                terminate_ok,
                backend_pid_after_is_wrong_phase: engine.backend_pid().is_err(),
                tx_status_after_is_wrong_phase: engine.tx_status().is_err(),
            }
        },
    )
    .expect("session assembles");

    assert_eq!(
        summary.backend_pid_active,
        Some(4321),
        "before terminate the engine is active and surfaces the handshake backend pid",
    );
    assert!(summary.terminate_ok, "terminate from active must succeed");
    assert!(
        summary.backend_pid_after_is_wrong_phase,
        "backend_pid after terminate must be a classified WrongPhase (closed phase)",
    );
    assert!(
        summary.tx_status_after_is_wrong_phase,
        "tx_status after terminate must be a classified WrongPhase (closed phase)",
    );

    let written = writes.lock().expect("writes lock").clone();
    assert_eq!(
        written, TERMINATE_WIRE_BYTES,
        "terminate must push exactly the 5-byte Terminate frame and nothing else",
    );
    assert_eq!(
        written,
        [b'X', 0, 0, 0, 4],
        "the Terminate frame is [tag='X', length-field = BE u32 4]",
    );
    assert_eq!(
        shutdowns.load(Ordering::SeqCst),
        1,
        "terminate must drive the transport shutdown exactly once",
    );
}

#[test]
fn accessor_after_terminate_is_wrong_phase() {
    // The closed-phase half of the use-after-terminate invariant, isolated: a
    // phase accessor (which needs no token) after a graceful close is classified
    // WrongPhase, never a stale active value. (The token-needing verbs are a move
    // error — the compile-time half, covered by the consumed `Live` token.)
    let writes: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdowns: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let user = Ident::try_from_str("term").expect("ident");

    let both_wrong_phase = session(
        CaptureServer::new(handshake(), Arc::clone(&writes), Arc::clone(&shutdowns)),
        &user,
        None,
        None,
        Credentials::Trust,
        |mut engine, live| {
            let live = flatten(poll_once(engine.connect(live))).expect("connect");
            let _ = poll_once(engine.terminate(live));
            engine.backend_pid().is_err() && engine.tx_status().is_err()
        },
    )
    .expect("session assembles");

    assert!(
        both_wrong_phase,
        "both phase accessors must classify WrongPhase after a graceful close",
    );
}

#[test]
fn terminate_from_connecting_is_wrong_phase() {
    // `terminate` before `connect` is a classified WrongPhase — and rejects
    // BEFORE any byte reaches the wire (the phase check precedes the frame).
    let writes: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let shutdowns: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let user = Ident::try_from_str("term").expect("ident");
    let body_writes = Arc::clone(&writes);

    let rejected = session(
        CaptureServer::new(handshake(), Arc::clone(&writes), Arc::clone(&shutdowns)),
        &user,
        None,
        None,
        Credentials::Trust,
        // Deliberately do NOT connect: the engine stays in its connecting phase.
        |mut engine, live| {
            // No flush has run, so the startup packet is queued but unsent.
            let pre_write_empty = body_writes.lock().expect("writes lock").is_empty();
            let r = poll_once(engine.terminate(live));
            matches!(r, Ok(Err(EngineError::WrongPhase(_)))) && pre_write_empty
        },
    )
    .expect("session assembles");

    assert!(
        rejected,
        "terminate on a still-connecting engine must classify WrongPhase",
    );
    assert!(
        writes.lock().expect("writes lock").is_empty(),
        "a wrong-phase terminate must not push any wire bytes",
    );
    assert_eq!(
        shutdowns.load(Ordering::SeqCst),
        0,
        "a wrong-phase terminate must not shut the transport down",
    );
}

#[test]
fn terminate_with_flush_error_still_closes_and_propagates() {
    // TOTAL invariant: once `terminate` is past the active check the connection is
    // dead, so a flush error must STILL leave the engine in its closed phase
    // (accessors → WrongPhase) AND propagate the classified transport error. The
    // best-effort shutdown is attempted even after the flush failed.
    let fail_write = Arc::new(AtomicBool::new(false));
    let fail_shutdown = Arc::new(AtomicBool::new(false));
    let shutdowns = Arc::new(AtomicUsize::new(0));
    let user = Ident::try_from_str("term").expect("ident");
    let arm_write = Arc::clone(&fail_write);

    let outcome = session(
        FailingServer::new(
            handshake(),
            Arc::clone(&fail_write),
            Arc::clone(&fail_shutdown),
            Arc::clone(&shutdowns),
        ),
        &user,
        None,
        None,
        Credentials::Trust,
        |mut engine, live| {
            let live = flatten(poll_once(engine.connect(live))).expect("connect reaches active");
            // Arm the write failure AFTER the handshake so connect's startup write
            // succeeds and only terminate's flush errors.
            arm_write.store(true, Ordering::SeqCst);
            let term = poll_once(engine.terminate(live));
            FailOutcome {
                terminate_is_transport_err: matches!(term, Ok(Err(EngineError::Transport(_)))),
                closed_after: engine.backend_pid().is_err() && engine.tx_status().is_err(),
            }
        },
    )
    .expect("session assembles");

    assert!(
        outcome.terminate_is_transport_err,
        "a flush error must propagate as EngineError::Transport",
    );
    assert!(
        outcome.closed_after,
        "Phase::Closed must be set even when terminate's flush errors (TOTAL invariant)",
    );
    assert_eq!(
        shutdowns.load(Ordering::SeqCst),
        1,
        "shutdown is best-effort: attempted exactly once even after a flush error",
    );
}

#[test]
fn terminate_with_shutdown_error_still_closes_and_propagates() {
    // The flush succeeds (write stays armed off); the shutdown errors. The engine
    // must STILL reach its closed phase and propagate the shutdown's transport
    // error — the TOTAL invariant on the shutdown leg.
    let fail_write = Arc::new(AtomicBool::new(false));
    let fail_shutdown = Arc::new(AtomicBool::new(false));
    let shutdowns = Arc::new(AtomicUsize::new(0));
    let user = Ident::try_from_str("term").expect("ident");
    let arm_shutdown = Arc::clone(&fail_shutdown);

    let outcome = session(
        FailingServer::new(
            handshake(),
            Arc::clone(&fail_write),
            Arc::clone(&fail_shutdown),
            Arc::clone(&shutdowns),
        ),
        &user,
        None,
        None,
        Credentials::Trust,
        |mut engine, live| {
            let live = flatten(poll_once(engine.connect(live))).expect("connect reaches active");
            arm_shutdown.store(true, Ordering::SeqCst);
            let term = poll_once(engine.terminate(live));
            FailOutcome {
                terminate_is_transport_err: matches!(term, Ok(Err(EngineError::Transport(_)))),
                closed_after: engine.backend_pid().is_err() && engine.tx_status().is_err(),
            }
        },
    )
    .expect("session assembles");

    assert!(
        outcome.terminate_is_transport_err,
        "a shutdown error must propagate as EngineError::Transport",
    );
    assert!(
        outcome.closed_after,
        "Phase::Closed must be set even when terminate's shutdown errors (TOTAL invariant)",
    );
    assert_eq!(
        shutdowns.load(Ordering::SeqCst),
        1,
        "shutdown is attempted exactly once",
    );
}
