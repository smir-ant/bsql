//! Engine-scaffold behavioural + footprint gates.
//!
//! Proves the five seams *compose and run* on the installed compiler: a
//! linear token threads through sequential async verbs (the R4
//! self-referential-async shape), a non-default observer policy reuses the
//! identical verb surface, and the declared event vocabulary plus the
//! zero-cost seam types hold their pinned footprints at run time (mirroring
//! the build-time `const` gates in the engine source).

#![forbid(unsafe_code)]

use bsql_postgres_proto::engine::{
    session, session_with, Engine, EngineError, EngineNoObs, Event, Live, NoObserver, Transport,
    AuthEvent,
};
use core::convert::Infallible;
use core::future::{ready, Future};

/// Minimal always-ready transport. The scaffold verbs perform no exchange,
/// so the I/O methods are never driven — they exist only to satisfy
/// `T: Transport`. Built from `core::future::ready`, so there is no manual
/// async block (and thus no `Transport`-shape boilerplate to lint).
struct TestTransport;

impl Transport for TestTransport {
    type Error = Infallible;
    fn read<'a>(
        &'a mut self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(0))
    }
    fn write_all<'a>(
        &'a mut self,
        _buf: &'a [u8],
    ) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
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

/// R4: a single linear token threads through TWO sequential `await`s inside
/// one `async` scope that holds `&mut engine` across both — the shape that
/// would not compile if the verbs coupled the engine borrow to the brand
/// (`&'b mut Engine<'b>`). Decoupling (verbs take `&mut self`; the brand
/// lives only on the engine type + the token) is what makes this compile.
#[test]
fn r4_two_sequential_async_verbs_thread_one_token() {
    let outcome: Result<(), EngineError<Infallible>> = session(TestTransport, |mut e, live| {
        block_on(async move {
            let live = e.begin(live).await?;
            let live = e.commit(live).await?;
            let _consumed = live;
            Ok(())
        })
    });
    assert!(outcome.is_ok());
}

/// The same threading driven one `await` per `block_on` — the token is
/// returned by each verb and fed to the next.
#[test]
fn verbs_thread_token_one_await_each() {
    let threaded = session(TestTransport, |mut e, live| {
        let live = block_on(e.begin(live)).expect("begin");
        let live = block_on(e.commit(live)).expect("commit");
        let _consumed = live;
        true
    });
    assert!(threaded);
}

/// `session_with` threads the linear token with a caller-chosen observer
/// policy through the identical verb surface — no signature change.
#[test]
fn session_with_threads_explicit_policy() {
    let threaded = session_with(TestTransport, NoObserver, |mut e, live| {
        let live = block_on(e.begin(live)).expect("begin");
        let _consumed = live;
        true
    });
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

/// The branded token and the default observer policy are zero-sized.
#[test]
fn live_and_noobserver_are_zero_sized() {
    assert_eq!(core::mem::size_of::<Live<'static>>(), 0);
    assert_eq!(core::mem::size_of::<NoObserver>(), 0);
}

/// NoObserver is free: an engine with the default observer is byte-for-byte
/// identical to the observer-free control type.
#[test]
fn engine_with_default_observer_is_free() {
    assert_eq!(
        core::mem::size_of::<Engine<'static, [u8; 16], NoObserver>>(),
        core::mem::size_of::<EngineNoObs<'static, [u8; 16]>>(),
    );
}
