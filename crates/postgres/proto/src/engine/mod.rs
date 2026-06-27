//! Strangler-engine scaffold — the session engine and its five seams.
//!
//! This module grows a new session engine *alongside* the existing wire
//! state machine without disturbing it: it is purely additive, adds no
//! dependency, and bakes in `#![no_std]` from the start (the parent
//! crate's forbid-bundle — no `unsafe`, no panic/index/arith/cast — and
//! `extern crate alloc` apply unchanged). Nothing here is wired into the
//! live `dispatch`/`protocol` path; that composition lands as later
//! additive steps.
//!
//! The five load-bearing seams:
//!
//! 1. [`Never`] + [`absurd`] — uninhabited carrier for phase-impossible
//!    frames (no wildcard `_` arms).
//! 2. [`Observer`] (sealed) + [`NoObserver`] — the zero-cost policy seam
//!    carried by every verb.
//! 3. [`Transport`] — the driver-facing I/O seam (RPITIT + `Send`, with an
//!    associated [`Error`](Transport::Error) type).
//! 4. [`Live`] — the branded, non-`Clone`, linear liveness token, minted
//!    by [`session`] / [`session_with`].
//! 5. [`Engine`] — the session shell that composes the above, carrying the
//!    observer as a defaulted, ZST type parameter.
//!
//! The [`Event`] / [`AuthEvent`] pull-event vocabulary is *declared* here
//! (and footprint-pinned); the producers that emit those events compose in
//! later additive steps.

mod dispatch_active;
mod dispatch_connecting;
mod error;
mod ingest;
mod seams;

pub use dispatch_active::ActiveEngine;
pub use dispatch_connecting::{ConnFail, ConnectingEngine};
pub use error::EngineError;
pub use ingest::{IngestBuf, IngestCommitOverflow, IngestFull};
pub use seams::{
    absurd, engine_observe_no_seam, engine_observe_via_seam, Live, Never, NoObserver, Observer,
    Transport,
};

use core::marker::PhantomData;

// ===========================================================================
// Pull-event vocabulary (declared; producers compose later)
// ===========================================================================

/// Connecting-phase pull-event surface — closed over startup/auth frames
/// only.
///
/// There is deliberately no `Row`/`Deliver`/`Notify` variant: those frames
/// are unrepresentable during the connecting phase by construction, not by
/// a runtime guard. Each payload-bearing variant carries exactly one
/// borrow of the read buffer, so the whole enum is one fat slice plus a
/// tag (footprint-pinned at 24 bytes).
#[derive(Clone, Copy, Debug)]
pub enum AuthEvent<'e> {
    /// The framing buffer is drained — the caller must read more bytes.
    NeedMore,
    /// Server requested cleartext-password authentication.
    AuthCleartext,
    /// Server requested MD5 authentication, lending the 4-byte salt.
    AuthMd5 {
        /// The server-chosen salt for the MD5 digest.
        salt: [u8; 4],
    },
    /// SASL continuation, lending the server's challenge bytes.
    AuthSaslContinue(&'e [u8]),
    /// A `ParameterStatus` report, lending its raw key/value payload.
    ParamStatus(&'e [u8]),
    /// Handshake complete — the connection is ready for queries.
    Ready,
    /// The server reported an error, lending its raw `ErrorResponse` body.
    Fail(&'e [u8]),
}

/// Active-phase pull-event surface — closed over every wire-legal active
/// frame, with no frequency-based exclusions.
///
/// Each payload-bearing variant carries exactly one borrow of the read
/// buffer (`Row` lends the whole `DataRow` payload), so the enum is one fat
/// slice plus a tag (footprint-pinned at 24 bytes).
#[derive(Clone, Copy, Debug)]
pub enum Event<'e> {
    /// The framing buffer is drained — the caller must read more bytes.
    NeedMore,
    /// Clean `ReadyForQuery` — the command boundary at which a verb
    /// returns the liveness token.
    Idle,
    /// A command completed; the tag is surfaced via the observer seam.
    Deliver,
    /// The server reported an error, lending its raw `ErrorResponse` body.
    Fail(&'e [u8]),
    /// The server closed the connection.
    Close,
    /// A `NoticeResponse`, lending its raw payload.
    Notice(&'e [u8]),
    /// A `NotificationResponse` (`LISTEN`/`NOTIFY`), lending its payload.
    Notify(&'e [u8]),
    /// A `ParameterStatus` report, lending its raw key/value payload.
    ParamStatus(&'e [u8]),
    /// A row-limited `Execute` paused at its cap: the server sent
    /// `PortalSuspended` instead of `CommandComplete`. The rows delivered
    /// before this are the prefix fetched so far; the portal stays open on
    /// the server (resumable with a bare `Execute`) and there is no command
    /// tag. A typed terminal distinct from [`Deliver`](Self::Deliver) — the
    /// pull analog of the live engine's `Reply::QuerySuspended` discriminator,
    /// not a side-channel flag.
    Suspended,
    /// One `DataRow`, lending the whole row payload as a single borrow.
    Row(&'e [u8]),
    /// One chunk of a row that exceeded the inline buffer, lending the
    /// chunk bytes.
    RowChunk(&'e [u8]),
    /// The final chunk of an oversized row has been delivered.
    RowChunkEnd,
    /// A `COPY` data frame, lending its payload bytes.
    CopyData(&'e [u8]),
    /// The `COPY` stream is complete.
    CopyDone,
}

crate::wire_pin!(Event<'static>, size = 24, align = 8);
crate::wire_pin!(AuthEvent<'static>, size = 24, align = 8);

// ===========================================================================
// 5. Engine shell + the session-scope minting functions
// ===========================================================================

/// The session engine: a transport, an observer policy, and the brand that
/// ties it to its session-scoped liveness token.
///
/// The observer `O` is a defaulted type parameter, so *every* verb is
/// observer-aware from day one through `self: &mut Engine<'b, T, O>` — no
/// per-verb generic and no second signature pass when a non-default policy
/// is introduced. With the default [`NoObserver`], the `obs` field is a
/// ZST and the engine is byte-identical to one with no observer concept at
/// all (see the `Engine == EngineNoObs` size-identity gate below).
///
/// The brand `'b` lives on the engine *type* (so a foreign token cannot
/// drive it) but the verbs borrow `&mut self`, never `&'b mut self` — the
/// engine borrow is released at each `await`, so a single async scope can
/// thread the linear token through any number of sequential verbs. Coupling
/// the engine *borrow* to the brand would over-constrain the engine to a
/// single borrow for its whole lifetime and make sequential verbs in one
/// `async` scope uncompilable.
#[derive(Debug)]
pub struct Engine<'b, T, O = NoObserver> {
    #[expect(
        dead_code,
        reason = "I/O seam stored at engine birth; the scaffold verbs perform no exchange, so the field is write-only until the ingest/flush pump reads it to drive the wire"
    )]
    transport: T,
    #[expect(
        dead_code,
        reason = "observer policy stored at engine birth; write-only until the row/complete dispatch hooks invoke it"
    )]
    obs: O,
    _brand: PhantomData<fn(&'b ()) -> &'b ()>,
}

/// Control type carrying every [`Engine`] field *except* the observer —
/// used only by the size-identity gate to prove the `obs: O` ZST field
/// costs zero bytes at the default policy.
#[derive(Debug)]
#[doc(hidden)]
pub struct EngineNoObs<'b, T> {
    #[expect(
        dead_code,
        reason = "size-identity control field mirroring Engine's transport; exists only so the ZST-observer-is-free size comparison has a like-for-like layout"
    )]
    transport: T,
    _brand: PhantomData<fn(&'b ()) -> &'b ()>,
}

// "NoObserver is free" — the ZST observer field adds zero bytes. A
// non-trivial transport stand-in (`[u8; 16]`) makes the identity
// load-bearing rather than the trivial `0 == 0`.
const _: () = assert!(
    core::mem::size_of::<Engine<'static, [u8; 16], NoObserver>>()
        == core::mem::size_of::<EngineNoObs<'static, [u8; 16]>>(),
    "ZST-observer-is-free invariant broken: Engine<_, NoObserver> must be \
     byte-identical to the observer-free control type. A non-ZST crept into \
     the observer seam, or a field was added to one but not the other.",
);

crate::wire_pin!(Live<'static>, size = 0, align = 1);
crate::wire_pin!(NoObserver, size = 0, align = 1);

impl<'b, T, O> Engine<'b, T, O> {
    /// Construct the engine shell from an already-prepared transport and
    /// observer policy, branded to the caller's session scope `'b`.
    #[inline(always)]
    fn new_in_scope(transport: T, obs: O) -> Self {
        Self {
            transport,
            obs,
            _brand: PhantomData,
        }
    }
}

impl<'b, T: Transport, O: Observer> Engine<'b, T, O> {
    /// Verb-shaped seam that consumes and returns the linear liveness
    /// token. The body performs no wire exchange — it pins the verb
    /// signature (`&mut self` plus `Live<'b>` in, `Result<Live<'b>,
    /// EngineError>` out) the I/O-bearing verbs are built on, and proves
    /// the token threads through `&mut self` without coupling the engine
    /// borrow to the brand.
    #[inline]
    pub async fn begin(&mut self, live: Live<'b>) -> Result<Live<'b>, EngineError<T::Error>> {
        Ok(live)
    }

    /// Verb-shaped seam mirroring [`begin`](Self::begin); together they
    /// witness a linear token threading through two sequential verbs in one
    /// `async` scope.
    #[inline]
    pub async fn commit(&mut self, live: Live<'b>) -> Result<Live<'b>, EngineError<T::Error>> {
        Ok(live)
    }
}

/// Open a session over `transport` with the default [`NoObserver`] policy.
///
/// `body` is `for<'b>`, so each call mints a *fresh, invariant* brand: the
/// [`Live`] token handed to the body cannot escape the scope (returning it
/// is a lifetime error) and cannot be confused with another session's
/// token (a foreign brand is a type error).
#[inline]
pub fn session<T, R>(
    transport: T,
    body: impl for<'b> FnOnce(Engine<'b, T, NoObserver>, Live<'b>) -> R,
) -> R
where
    T: Transport,
{
    let engine = Engine::new_in_scope(transport, NoObserver);
    let live = Live::new_in_scope();
    body(engine, live)
}

/// Open a session with a caller-chosen [`Observer`] policy.
///
/// Identical scoping to [`session`]; the same verb surface serves the
/// non-default policy with no signature change — only the constructed
/// engine's observer type differs.
#[inline]
pub fn session_with<T, O, R>(
    transport: T,
    observer: O,
    body: impl for<'b> FnOnce(Engine<'b, T, O>, Live<'b>) -> R,
) -> R
where
    T: Transport,
    O: Observer,
{
    let engine = Engine::new_in_scope(transport, observer);
    let live = Live::new_in_scope();
    body(engine, live)
}

// ===========================================================================
// Compile-time seam-composition gates
// ===========================================================================

/// Private witness transport for the compile-time gates below. Its I/O
/// methods are never driven (the scaffold verbs perform no exchange); it
/// exists only so the gates can name a concrete `T: Transport`.
struct WitnessTransport;

impl Transport for WitnessTransport {
    type Error = core::convert::Infallible;

    #[inline(always)]
    fn read<'a>(
        &'a mut self,
        _buf: &'a mut [u8],
    ) -> impl core::future::Future<Output = Result<usize, Self::Error>> + Send + 'a {
        core::future::ready(Ok(0))
    }

    #[inline(always)]
    fn write_all<'a>(
        &'a mut self,
        _buf: &'a [u8],
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send + 'a {
        core::future::ready(Ok(()))
    }
}

// Seam-composition + `Send` gate. The closure is never called; its body is
// type-checked at build time. The verb future must be `Send` (the
// load-bearing property for the async driver), and a linear token must
// thread through two SEQUENTIAL `await`s while one `async` scope holds
// `&mut self` across both — the form that would fail to compile if the
// verbs over-constrained to `&'b mut Engine<'b>` (the self-referential
// async footgun this seam is designed to avoid).
const _: fn() = || {
    fn assert_send<T: Send>() {}
    fn need_send<F: Send>(_: &F) {}

    assert_send::<WitnessTransport>();
    assert_send::<Live<'static>>();
    assert_send::<NoObserver>();
    assert_send::<EngineError<core::convert::Infallible>>();
    assert_send::<Engine<'static, WitnessTransport, NoObserver>>();

    let mut engine: Engine<'static, WitnessTransport, NoObserver> =
        Engine::new_in_scope(WitnessTransport, NoObserver);
    let live = Live::new_in_scope();

    let threaded = async move {
        let live = engine.begin(live).await?;
        let live = engine.commit(live).await?;
        // Consume the token at the clean boundary; the brand must not
        // escape the async scope.
        let _consumed = live;
        Ok::<(), EngineError<core::convert::Infallible>>(())
    };
    need_send(&threaded);
};
