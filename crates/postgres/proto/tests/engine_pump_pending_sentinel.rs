//! `poll_once` single-poll-sentinel gate.
//!
//! Proves the synchronous single-poll driver: (a) a future built from the pump
//! over a BLOCKING (always-ready) scripted transport resolves under one
//! [`poll_once`] — every leaf await is ready, so one poll suffices; and (b) a
//! deliberately-`Pending` future returns the classified [`SpuriousPending`]
//! WITHOUT spinning (`poll_once` polls exactly once and returns), both for a raw
//! always-pending future and for the realistic case of a pump whose transport
//! read would block.
//!
//! [`poll_once`]: bsql_postgres_proto::engine::poll_once
//! [`SpuriousPending`]: bsql_postgres_proto::engine::SpuriousPending

#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test helpers (handshake construction, the poll-once driver) use unwrap/expect/panic as the loud failure signal; clippy's allow-in-tests carve-out reaches #[test] fns but not the free helper fns this file factors out"
)]

use core::convert::Infallible;
use core::future::Future;
use core::ops::ControlFlow;

use bsql_postgres_proto::engine::{
    poll_once, pump_active_to_boundary, ActiveEngine, AuthEvent, Boundary, ConnectingEngine,
    NoObserver, SendBuf, SpuriousPending, Surface, Transport,
};
use bsql_postgres_proto::wire::TAG_READY_FOR_QUERY;
use bsql_postgres_proto::{Credentials, Ident};

// ─────────────────────────── frame helpers ───────────────────────────

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

fn active_engine() -> ActiveEngine {
    let user = Ident::try_from_str("corpus").expect("ident");
    let mut send_buf = SendBuf::new();
    let mut conn = ConnectingEngine::start(&mut send_buf, &user, None, &[], Credentials::Trust)
        .expect("start handshake");
    let hs = concat(&[auth_ok(), backend_key(4321, 8765), ready_for_query(b'I')]);
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

// ─────────────────────────── transports ───────────────────────────

/// Always-ready transport: `read` delivers `inbound` once then `Ok(0)`; every
/// op resolves synchronously, so a future over it is always-ready.
struct ReadyTransport {
    inbound: Vec<u8>,
    cursor: usize,
}

impl Transport for ReadyTransport {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        let n = (self.inbound.len() - self.cursor).min(buf.len());
        buf[..n].copy_from_slice(&self.inbound[self.cursor..self.cursor + n]);
        self.cursor += n;
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

/// A transport whose `read` would block: it returns a perpetually-`Pending`
/// future. `write`/`flush` resolve so the pump's entry flush completes and the
/// pump reaches the (blocking) read.
struct BlockingReadTransport;

impl Transport for BlockingReadTransport {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }
    fn read<'a>(
        &'a mut self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        core::future::pending::<Result<usize, Infallible>>()
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

// ─────────────────────────── tests ───────────────────────────

/// (a) core: an already-ready future resolves under a single poll.
#[test]
fn ready_future_resolves_in_one_poll() {
    let out = poll_once(core::future::ready(42u32)).expect("ready resolves in one poll");
    assert_eq!(out, 42);
}

/// (a) pump: a pump future over an always-ready transport (one ReadyForQuery
/// frame drives it straight to Idle) resolves under a single poll.
#[test]
fn pump_over_blocking_transport_resolves_in_one_poll() {
    let mut engine = active_engine();
    let mut transport = ReadyTransport {
        inbound: ready_for_query(b'I'),
        cursor: 0,
    };
    let mut send_buf = SendBuf::new();
    let obs = NoObserver;

    let boundary: Boundary<()> = poll_once(pump_active_to_boundary(
        &mut engine,
        &mut transport,
        &mut send_buf,
        &obs,
        |_s: Surface<'_>| ControlFlow::Continue(()),
    ))
    .expect("blocking transport resolves in a single poll")
    .expect("no engine error");

    assert_eq!(boundary, Boundary::Idle);
}

/// (b) core: a perpetually-pending future is classified, not spun on.
#[test]
fn pending_future_classifies_spurious_pending() {
    let result = poll_once(core::future::pending::<i32>());
    assert!(matches!(result, Err(SpuriousPending)));
}

/// (b) pump: a pump whose transport read would block surfaces SpuriousPending
/// at the read (the entry flush having completed), never spinning.
#[test]
fn pump_over_pending_read_classifies_spurious_pending() {
    let mut engine = active_engine();
    let mut transport = BlockingReadTransport;
    let mut send_buf = SendBuf::new();
    let obs = NoObserver;

    let result: Result<Result<Boundary<()>, _>, _> = poll_once(pump_active_to_boundary(
        &mut engine,
        &mut transport,
        &mut send_buf,
        &obs,
        |_s: Surface<'_>| ControlFlow::Continue(()),
    ));

    assert!(matches!(result, Err(SpuriousPending)));
}
