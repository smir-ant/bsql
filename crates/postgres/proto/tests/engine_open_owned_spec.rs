//! Owned-handle constructor gate for [`open_owned`] / [`open_owned_with`].
//!
//! [`session`](bsql_postgres_proto::engine::session) lends the engine and its
//! linear [`Live`] token to a `for<'b>` closure: the generative brand traps
//! both inside the scope, so neither can be returned or stored (that is a
//! lifetime error, pinned by the `engine_linearity` brand-escape goldens).
//! [`open_owned`] is the additional poolable-handle API — it primes the engine
//! identically but *returns* a `'static`-branded `(Engine, Live)` pair the
//! caller can own and store.
//!
//! This gate proves the two capabilities the owned form exists to provide,
//! over a scripted [`Transport`] (the verbs resolve synchronously, so a verb
//! future built over it is always-ready and drains under one `block_on`):
//!
//! 1. the returned owned engine threads the linear token through ≥2 SEQUENTIAL
//!    verbs inside one `async` scope that holds `&mut engine` across both
//!    `await`s (the self-referential-async shape, mirroring the `session`
//!    threading path), and
//! 2. the returned engine + token CAN be stored in a struct and driven across
//!    method calls — the poolable-connection pattern `session` forbids.
//!
//! The brand tradeoff is documented on [`open_owned`]: pinning the brand at
//! `'static` drops *cross-connection* isolation from tier-1 to
//! tier-2-by-encapsulation (the owner keeps the token private), while
//! *within*-connection linearity stays tier-1 — [`Live`] is non-`Clone`, every
//! verb consumes it and returns it only on a clean boundary, so the
//! at-most-one-command-in-flight discipline is still move-checked. The
//! `Holder` here models exactly that owner: the token lives in a private
//! `Option<Live<'static>>`, taken for the duration of a verb and returned after.
//!
//! [`open_owned`]: bsql_postgres_proto::engine::open_owned
//! [`open_owned_with`]: bsql_postgres_proto::engine::open_owned_with
//! [`Transport`]: bsql_postgres_proto::engine::Transport
//! [`Live`]: bsql_postgres_proto::engine::Live

#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test helpers (the scripted server, the block-on driver, the Holder methods) use unwrap/expect/panic as the loud failure signal; clippy's allow-in-tests carve-out reaches #[test] fns but not the free helper fns / impl methods this file factors out"
)]

use core::convert::Infallible;
use core::future::{ready, Future};
use core::ops::ControlFlow;

use bsql_postgres_proto::engine::{
    open_owned, open_owned_with, Engine, EngineError, Live, Never, NoObserver, Surface, Transport,
};
use bsql_postgres_proto::wire::{TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_READY_FOR_QUERY};
use bsql_postgres_proto::{Credentials, Ident};

// ─────────────────────────── frame builders ───────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// The trust handshake (`AuthenticationOk` + `BackendKeyData` +
/// `ReadyForQuery`) followed by one extra `ReadyForQuery` for the ping's
/// `Sync` — exactly enough script for `connect` then `ping`.
fn trust_then_ping_script() -> Vec<u8> {
    let mut key_body = 4321_i32.to_be_bytes().to_vec();
    key_body.extend_from_slice(&8765_i32.to_be_bytes());
    let mut inbound = Vec::new();
    inbound.extend_from_slice(&frame(TAG_AUTHENTICATION.byte(), &0_i32.to_be_bytes()));
    inbound.extend_from_slice(&frame(TAG_BACKEND_KEY_DATA.byte(), &key_body));
    inbound.extend_from_slice(&frame(TAG_READY_FOR_QUERY.byte(), b"I"));
    inbound.extend_from_slice(&frame(TAG_READY_FOR_QUERY.byte(), b"I"));
    inbound
}

// ─────────────────────────── scripted server ───────────────────────────

/// A transport whose reply is a fixed byte script (independent of what the
/// client writes). `read` drains the script; `write`/`flush`/`shutdown` are
/// no-op ready, so a future over it is always-ready.
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

// ─────────────────────────── helpers ───────────────────────────

/// Drive a synchronously-resolving future to completion by polling with a
/// no-op waker — the always-ready scripted transports never return `Pending`.
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

/// A sink that surfaces nothing (`B = Never`): the ping/connect paths here
/// drain quiet boundaries.
fn drop_sink(_surface: Surface<'_>) -> ControlFlow<Never> {
    ControlFlow::Continue(())
}

// ───────── the capability `open_owned` exists to provide: storage ─────────

/// A poolable connection: it OWNS the engine and parks the linear token in a
/// private `Option`, taking it for the duration of a verb and returning it
/// after. This is the shape [`session`] forbids (its `for<'b>` brand traps the
/// engine in-scope); `open_owned`'s `'static` brand makes the struct field
/// well-formed. At-most-one-command-in-flight stays move-checked by the engine
/// verb itself — it consumes the `Live` token by value, and `&mut self`
/// serialises verb calls at compile time. The private `Option` is the runtime
/// connection-dead health bit, not the move-check: it is `None` only after a
/// verb errored (the `?` returns before the token is re-parked), so a later
/// call finds no token. A real driver maps that `None` to a dead-connection
/// error and evicts; this fixture takes it directly to exercise the happy path.
struct Holder<T> {
    engine: Engine<'static, T>,
    live: Option<Live<'static>>,
}

impl<T: Transport<Error = Infallible>> Holder<T> {
    /// Open an owned handle and store it — proves the returned pair is storable.
    fn open(transport: T, user: &Ident) -> Self {
        let (engine, live) = open_owned(transport, user, None, None, Credentials::Trust)
            .expect("startup packet assembles");
        Self {
            engine,
            live: Some(live),
        }
    }

    /// Drive `connect` through the stored handle: take the parked token, run the
    /// verb over `&mut self.engine`, re-park the token on success.
    fn connect(&mut self) -> Result<(), EngineError<Infallible>> {
        let live = self.live.take().expect("token present between verbs");
        let live = block_on(self.engine.connect(live))?;
        self.live = Some(live);
        Ok(())
    }

    /// Drive `ping` through the stored handle — same take/run/re-park cycle.
    fn ping(&mut self) -> Result<(), EngineError<Infallible>> {
        let live = self.live.take().expect("token present between verbs");
        let live = block_on(self.engine.ping(live, drop_sink))?;
        self.live = Some(live);
        Ok(())
    }
}

// ─────────────────────────── const witnesses ───────────────────────────

/// Lock the `Transport::Error: Send` bound at a concrete transport: a wrapper
/// transport's error union is `Send` only when the inner `Error` is, so the
/// bound must hold for the transports the engine actually drives.
const _: fn() = || {
    fn assert_send<S: Send>() {}
    assert_send::<<StaticServer as Transport>::Error>();
    // The owned handle's brand is `'static`; the token must stay `Send` so the
    // async driver can park it across task boundaries.
    assert_send::<Live<'static>>();
};

// ─────────────────────────── tests ───────────────────────────

/// The returned owned engine threads ONE linear token through two SEQUENTIAL
/// verbs (`connect` then `ping`) inside a single `async` scope that holds
/// `&mut engine` across both `await`s — the same threading `session` supports,
/// now on a handle that was *returned* (moved out of the constructor) rather
/// than lent to a closure.
#[test]
fn open_owned_threads_two_sequential_verbs() {
    let user = Ident::try_from_str("owned").expect("ident");
    let server = StaticServer::new(trust_then_ping_script());
    let (mut engine, live) =
        open_owned(server, &user, None, None, Credentials::Trust).expect("startup packet assembles");

    // One `async` scope holds `&mut engine` across both `await`s while the single
    // token threads connect→ping; the borrow ends when `block_on` returns, so the
    // active state is read back afterwards.
    let threaded: Result<(), EngineError<Infallible>> = block_on(async {
        let live = engine.connect(live).await?;
        let live = engine.ping(live, drop_sink).await?;
        // Consume the token at the clean boundary; it must not escape the scope.
        let _ = live;
        Ok(())
    });
    threaded.expect("owned engine must thread the token through connect→ping");
    assert_eq!(
        engine.backend_pid(),
        Ok(4321),
        "owned engine must reach active after the threaded verbs",
    );
}

/// The returned engine + token CAN be stored in a struct (the capability
/// `session` forbids) and driven across separate method calls. Two sequential
/// verbs run through the stored handle, the token re-parked between each.
#[test]
fn open_owned_engine_can_be_stored_in_a_struct() {
    let user = Ident::try_from_str("owned").expect("ident");
    let server = StaticServer::new(trust_then_ping_script());
    let mut holder = Holder::open(server, &user);

    // Verb 1: connect through the stored engine.
    holder.connect().expect("stored-handle connect reaches active");
    // Verb 2: ping through the stored engine, the token re-parked between calls.
    holder.ping().expect("stored-handle ping completes");

    assert_eq!(
        holder.engine.backend_pid(),
        Ok(4321),
        "stored owned engine must be active after the threaded verbs",
    );
    assert!(
        holder.live.is_some(),
        "the linear token must be re-parked after the last verb's clean boundary",
    );
}

/// `open_owned_with` (the policy-carrying owned constructor) at the default
/// [`NoObserver`] policy primes and threads identically to `open_owned` —
/// proving the with-observer entry of the pair is wired end-to-end.
#[test]
fn open_owned_with_default_policy_threads() {
    let user = Ident::try_from_str("owned").expect("ident");
    let server = StaticServer::new(trust_then_ping_script());
    let (mut engine, live) =
        open_owned_with(server, NoObserver, &user, None, None, Credentials::Trust)
            .expect("startup packet assembles");

    let threaded: Result<(), EngineError<Infallible>> = block_on(async {
        let live = engine.connect(live).await?;
        let live = engine.ping(live, drop_sink).await?;
        let _ = live;
        Ok(())
    });
    threaded.expect("open_owned_with must thread the token through connect→ping");
    assert_eq!(
        engine.backend_pid(),
        Ok(4321),
        "open_owned_with must reach active after the threaded verbs",
    );
}
