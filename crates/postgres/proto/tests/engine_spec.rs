//! Engine-scaffold behavioural + footprint gates.
//!
//! Proves the five seams *compose and run* on the installed compiler: a
//! linear token threads through sequential async verbs (the R4
//! self-referential-async shape), a non-default observer policy reuses the
//! identical verb surface, and the declared event vocabulary plus the
//! zero-cost seam types hold their pinned footprints at run time (mirroring
//! the build-time `const` gates in the engine source).

#![forbid(unsafe_code)]
#![allow(
    clippy::expect_used,
    reason = "test harness — a fixture/handshake failure is a loud assertion, the sanctioned test-failure signal"
)]

use bsql_postgres_proto::engine::{
    session, AuthEvent, EngineError, Event, Live, Surface, Transport,
};
use bsql_postgres_proto::wire::{TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_READY_FOR_QUERY};
use bsql_postgres_proto::{Credentials, Ident};
use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

/// Build a tagged, length-prefixed wire frame (`tag | len(self+body) | body`).
fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(tag);
    let len = u32::try_from(body.len() + 4).expect("frame body fits a u32 length");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// The canonical trust handshake reply (`AuthenticationOk` + `BackendKeyData` +
/// `ReadyForQuery`), followed by one extra `ReadyForQuery` answering the ping's
/// `Sync`, so a `connect` then a `ping` both drain to a clean idle.
fn handshake_then_ping_reply() -> Vec<u8> {
    let mut key_body = 4321_i32.to_be_bytes().to_vec();
    key_body.extend_from_slice(&8765_i32.to_be_bytes());
    let mut reply = Vec::new();
    reply.extend_from_slice(&frame(TAG_AUTHENTICATION.byte(), &0_i32.to_be_bytes()));
    reply.extend_from_slice(&frame(TAG_BACKEND_KEY_DATA.byte(), &key_body));
    reply.extend_from_slice(&frame(TAG_READY_FOR_QUERY.byte(), b"I"));
    reply.extend_from_slice(&frame(TAG_READY_FOR_QUERY.byte(), b"I"));
    reply
}

/// Static scripted server: `read` drains a fixed reply from a cursor; writes are
/// accepted and discarded; every op resolves synchronously (one-poll).
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

/// Dependency-free executor for the always-ready transport: the verb
/// futures never return `Pending`, so this never spins.
fn block_on<F: Future>(f: F) -> F::Output {
    use core::task::{Context, Poll};
    let mut f = core::pin::pin!(f);
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

/// A Continue-only sink — the R4 tests assert token threading, not surfaced
/// data.
fn drop_sink(_surface: Surface<'_>) -> ControlFlow<bsql_postgres_proto::engine::Never> {
    ControlFlow::Continue(())
}

/// R4: a single linear token threads through TWO sequential `await`s inside
/// one `async` scope that holds `&mut engine` across both — the shape that
/// would not compile if the verbs coupled the engine borrow to the brand
/// (`&'b mut Engine<'b>`). Decoupling (verbs take `&mut self`; the brand
/// lives only on the engine type + the token) is what makes this compile. The
/// verbs are real (`connect` then `ping`) driven over a scripted server.
#[test]
fn r4_two_sequential_async_verbs_thread_one_token() {
    let user = Ident::try_from_str("test").expect("ident");
    let server = StaticServer::new(handshake_then_ping_reply());
    let outcome: Result<(), EngineError<Infallible>> =
        session(server, &user, None, &[], Credentials::Trust, |mut e, live| {
            block_on(async move {
                let live = e.connect(live).await?;
                let live = e.ping(live, drop_sink).await?.live;
                let _ = live;
                Ok(())
            })
        })
        .expect("startup packet assembles");
    assert!(outcome.is_ok());
}

/// The same threading driven one `await` per `block_on` — the token is
/// returned by each verb and fed to the next.
#[test]
fn verbs_thread_token_one_await_each() {
    let user = Ident::try_from_str("test").expect("ident");
    let server = StaticServer::new(handshake_then_ping_reply());
    let threaded =
        session(server, &user, None, &[], Credentials::Trust, |mut e, live| {
            let live = block_on(e.connect(live)).expect("connect");
            let live = block_on(e.ping(live, drop_sink)).expect("ping").live;
            let _ = live;
            true
        })
        .expect("startup packet assembles");
    assert!(threaded);
}

/// The declared pull-event vocabulary holds its pinned 24-byte footprint.
#[test]
fn event_and_authevent_are_24_bytes() {
    assert_eq!(core::mem::size_of::<Event<'static>>(), 24);
    assert_eq!(core::mem::align_of::<Event<'static>>(), 8);
    assert_eq!(core::mem::size_of::<AuthEvent<'static>>(), 24);
    assert_eq!(core::mem::align_of::<AuthEvent<'static>>(), 8);
}

/// The branded liveness token is zero-sized.
#[test]
fn live_is_zero_sized() {
    assert_eq!(core::mem::size_of::<Live<'static>>(), 0);
}
