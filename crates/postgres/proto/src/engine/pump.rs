//! The active-phase engine pump + the synchronous single-poll helper.
//!
//! # The active pump composes the engine's pieces into one I/O loop
//!
//! [`pump_active_to_boundary`] is the free function that turns the sans-I/O
//! active engine into a driven exchange: it flushes the outbound request once,
//! then repeatedly classifies one inbound frame and reads more bytes when the
//! framing is short, until the engine reaches a protocol [`Boundary`]. It owns
//! no state of its own — it borrows the [`ActiveEngine`], the
//! [`Transport`](super::Transport), and the [`SendBuf`] as parameters, so it
//! composes the I/O loop without being a method on any of them (the same
//! disjoint-`&mut` shape the outbound [`flush`](super::flush) free function
//! uses).
//!
//! # The borrow structure (why drive-to-boundary, not event-at-a-time)
//!
//! The engine's [`ActiveEngine::next_event`] returns an [`Event`] that *borrows*
//! the ingest buffer. A pump that returned that borrow to its caller and then
//! re-borrowed the engine to read more bytes on `NeedMore` would be an
//! `&mut`-aliasing conflict the borrow checker rejects. The drive-to-boundary
//! shape sidesteps it: the borrowing `Event` lives only *inside* one loop
//! iteration — its payload is consumed in the same call (handed to the sink, or
//! to a typed accessor read), never escaping — so by the time the next
//! iteration re-borrows the engine to read, no prior borrow is alive. The two
//! values that DO cross the pump boundary are non-borrowing by construction:
//! [`Boundary`] carries no buffer reference, and each [`Surface`] is consumed
//! within the sink call.
//!
//! # No fallbacks on the read path
//!
//! Every short-read outcome is classified, never looped on or skipped:
//! [`EngineError::UnexpectedEof`](super::EngineError::UnexpectedEof) (peer
//! closed mid-frame — a zero-length read while a frame is incomplete),
//! [`EngineError::IngestFull`](super::EngineError::IngestFull) (a frame larger
//! than the bounded buffer), and
//! [`EngineError::IngestCommitOverflow`](super::EngineError::IngestCommitOverflow)
//! (a transport that reported writing more than the lent slot held).
//!
//! # The single-poll helper
//!
//! [`poll_once`] drives a future built over a *blocking* transport. Every leaf
//! await in such a future (read / write / flush) blocks synchronously and so
//! resolves on the first poll — never `Pending` — so one poll suffices. A
//! `Pending` return is the single runtime invariant the type system cannot
//! prove; it is surfaced as the classified [`SpuriousPending`] error, never a
//! spin and never a deadlock.

use alloc::string::String;
use core::future::Future;
use core::ops::ControlFlow;
use core::task::{Context, Poll};

use crate::command_tag::CommandTag;

use super::flush::flush;
use super::seams::Transport;
use super::{
    ActiveEngine, ConnFail, ConnectingEngine, EngineError, Event, HandshakeProgress, SendBuf,
};

/// The constant per-read *minimum room* the pump requests from
/// [`ActiveEngine::read_slot`](super::ActiveEngine::read_slot).
///
/// It is not a read size — `read_slot` lends the active tier's *whole*
/// remaining spare so one `socket.read` fills as much as is available in a
/// single syscall (there is no doubling ramp). `want` serves one purpose: it
/// drives the inline→heap escape decision, which is `filled + want > inline
/// tier`.
///
/// Bound to the inline-tier width (single source of truth) so that escape fires
/// exactly when the inline tier proved insufficient: on the first read the
/// buffer is empty (`0 + want == inline cap`, not greater), so the read stays
/// inline and a whole response that fits inline never escapes and never
/// allocates; the next read that still needs more finds `filled > 0`
/// (`filled + inline cap > inline cap`) and escapes to the heap tier, where the
/// full-spare lend drains the rest in one read. A response overflowing the
/// inline tier therefore takes about two reads — one inline-fill, one heap-fill
/// — instead of a doubling ramp. Coupling to the const keeps that boundary exact
/// if the tier width is ever changed.
const READ_WANT: usize = super::ingest::INGEST_INLINE_CAP;

/// The protocol boundary at which [`pump_active_to_boundary`] returns.
///
/// Non-borrowing: it carries no reference into the ingest buffer, so the verb
/// that receives it can re-borrow the engine freely. `#[non_exhaustive]` so a
/// future boundary can be added without breaking a downstream `match`.
///
/// Generic over the sink's break payload `B` (see the `sink` parameter of
/// [`pump_active_to_boundary`]). The default [`Never`](super::Never) is the
/// *collect-all* shape: a sink that only ever [`Continue`](ControlFlow::Continue)s
/// makes [`Stopped`](Self::Stopped) carry an uninhabited value, so the variant is
/// statically unreachable and folds into the discriminant — `Boundary<Never>` is
/// one byte, and a verb consumes the impossible arm with
/// [`absurd`](super::absurd), never a `unreachable!()`/wildcard. A breakable verb
/// (e.g. notification receive) fixes `B = ()` so the sink can stop the pump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Boundary<B = super::Never> {
    /// A clean `ReadyForQuery` — the connection is idle and reusable.
    Idle,
    /// A row-limited `Execute` paused at its cap (`PortalSuspended`): the portal
    /// stays open on the server and is resumable with a bare `Execute`. The
    /// rows fetched so far were surfaced to the sink before this boundary.
    Suspended,
    /// The server reported an error (`ErrorResponse`); the raw error bytes were
    /// surfaced to the sink before this boundary. The verb maps this to its
    /// error; the connection awaits its recovering `ReadyForQuery`.
    Failed,
    /// The engine tore the connection down (a protocol violation / out-of-phase
    /// frame); the socket must be closed.
    Closed,
    /// The sink returned [`ControlFlow::Break`], requesting an early stop,
    /// carrying the break payload. The connection is NOT at a protocol boundary
    /// — unread frames may remain buffered or on the wire — so recovery (drain
    /// or close) is the caller's responsibility. Distinct from
    /// [`Idle`](Self::Idle), which alone means the connection is clean and
    /// reusable: reporting a caller-requested stop as `Idle` would falsely claim
    /// a reusable connection. At `B = Never` this variant is uninhabited.
    Stopped(B),
}

// Pinned at both real instantiations. `Boundary<Never>` is the *collect-all*
// shape the thirteen Continue-only verbs use — `Stopped(Never)` is uninhabited
// and folds into the discriminant, so it is one byte. `Boundary<()>` is the
// *breakable* shape `recv_notification` uses; `()` is a ZST so the discriminant
// still fits one byte. Generic over `B`, so there is no single canonical size —
// these two are every shape a verb actually constructs.
crate::wire_pin!(Boundary<super::Never>, size = 1, align = 1);
crate::wire_pin!(Boundary<()>, size = 1, align = 1);

/// The terminal of [`pump_connecting_to_ready`] — the handshake either
/// completed or failed.
///
/// Non-borrowing: it carries no reference into the ingest buffer, so the
/// [`connect`](super::Engine::connect) verb that receives it can re-borrow the
/// engine freely for the synchronous Connecting→Active swap. `#[non_exhaustive]`
/// so a future outcome can be added without breaking a downstream `match`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HandshakeOutcome {
    /// `AuthenticationOk` + `BackendKeyData` + a clean `ReadyForQuery` were
    /// observed: the connection is ready to transition to the active phase.
    Ready,
    /// The handshake failed, carrying the classified [`ConnFail`] cause.
    Failed(ConnFail),
}

// One fat-niche enum over `ConnFail` (its footprint dominates); the unit
// outcomes ride the discriminant. `ConnFail` is 8 B/4 with SCRAM on and 2 B/1
// with SCRAM off, so this shrinks in lock-step.
#[cfg(feature = "scram")]
crate::wire_pin!(HandshakeOutcome, size = 8, align = 4);
#[cfg(not(feature = "scram"))]
crate::wire_pin!(HandshakeOutcome, size = 2, align = 1);

/// One surfaceable active-phase event, lent to the pump's sink and consumed
/// within that call — the borrow never escapes the sink invocation, which is
/// what keeps the pump's per-iteration engine borrow from aliasing the next
/// read.
#[derive(Debug, Clone, Copy)]
pub enum Surface<'e> {
    /// One whole `DataRow`, lending the row payload.
    Row(&'e [u8]),
    /// A completed command's projection, read at the delivery — before the
    /// trailing `ReadyForQuery` clears it.
    Deliver {
        /// The typed `CommandComplete` tag, or `None` for the tagless
        /// extended-protocol acks and the `Describe` completion. Lent typed (not
        /// as raw bytes) so the consumer reads the row count without re-parsing.
        tag: Option<&'e CommandTag>,
        /// Result-column type OIDs of the delivered statement.
        oids: &'e [u32],
        /// Result-column names of the delivered statement (empty on the
        /// extended-execute path, which does not re-send them).
        names: &'e [String],
    },
    /// The server's raw `ErrorResponse` payload, surfaced before a
    /// [`Boundary::Failed`].
    Fail(&'e [u8]),
    /// A `NoticeResponse`, lending its raw payload.
    Notice(&'e [u8]),
    /// A `NotificationResponse` (`LISTEN`/`NOTIFY`), lending its payload.
    Notify(&'e [u8]),
    /// A `ParameterStatus` report, lending its raw key/value payload.
    ParamStatus(&'e [u8]),
    /// One chunk of an oversize `DataRow` that exceeded the inline buffer.
    RowChunk(&'e [u8]),
    /// The final chunk of an oversize `DataRow` has been delivered.
    RowChunkEnd,
    /// A `COPY` data frame, lending its payload bytes.
    CopyData(&'e [u8]),
    /// The `COPY` stream is complete.
    CopyDone,
}

// The widest variant is `Deliver { tag, oids, names }` — an `Option<&CommandTag>`
// (8) plus two slice fat-pointers (16 each) = 40 B body, tail-padded with the
// discriminant to 48. The single-borrow payload variants are one fat slice (16).
crate::wire_pin!(Surface<'static>, size = 48, align = 8);

/// Drive the active engine to its next protocol [`Boundary`] over `transport`.
///
/// Flushes `send_buf` exactly once at entry (draining the request a verb
/// enqueued — the read loop never enqueues, so there is no redundant per-read
/// flush), then loops: classify one inbound frame via
/// [`ActiveEngine::next_event`]; on `NeedMore`, read one chunk from `transport`
/// into the engine's ingest buffer; on a payload event, hand the [`Surface`] to
/// `sink`; on a boundary, return.
///
/// `sink` consumes each [`Surface`] in the call and returns
/// [`ControlFlow`]: [`ControlFlow::Break`] stops the pump early and returns
/// [`Boundary::Stopped`]. A whole row is surfaced as [`Surface::Row`]; an
/// oversize row that never resides whole is surfaced in pieces as
/// [`Surface::RowChunk`] / [`Surface::RowChunkEnd`] instead.
///
/// # Errors
///
/// - [`EngineError::Transport`](super::EngineError::Transport) — the transport
///   reported a read, write, or flush failure (its own error, carried verbatim).
/// - [`EngineError::WriteZero`](super::EngineError::WriteZero) /
///   [`EngineError::SendOverrun`](super::EngineError::SendOverrun) — from the
///   entry flush (see [`flush`](super::flush)).
/// - [`EngineError::UnexpectedEof`](super::EngineError::UnexpectedEof) — the
///   transport returned `Ok(0)` while a frame was still incomplete (peer closed).
/// - [`EngineError::IngestFull`](super::EngineError::IngestFull) — a wire frame
///   larger than the bounded ingest buffer.
/// - [`EngineError::IngestCommitOverflow`](super::EngineError::IngestCommitOverflow)
///   — a transport that reported reading more than the lent slot held.
pub async fn pump_active_to_boundary<T, S, B>(
    active: &mut ActiveEngine,
    transport: &mut T,
    send_buf: &mut SendBuf,
    mut sink: S,
) -> Result<Boundary<B>, EngineError<T::Error>>
where
    T: Transport,
    S: FnMut(Surface<'_>) -> ControlFlow<B>,
{
    // Drain the enqueued request once, before the first read.
    flush(send_buf, transport).await?;

    loop {
        // The borrowing `Event` is confined to this `match`: arms either diverge
        // (read / return a boundary) before binding any payload, or yield a
        // `Surface` that is consumed by the single `sink` call below. No borrow
        // crosses into the next iteration, so the next `read_slot` re-borrow is
        // free of an E0499 conflict.
        let surface = match active.next_event() {
            Event::NeedMore => {
                // `read_slot` lends the active tier's whole remaining spare, so
                // this one read fills as much as the socket has in a single
                // syscall — no doubling ramp. `READ_WANT` only drives the
                // inline→heap escape decision (see its doc).
                let slot = active.read_slot(READ_WANT).map_err(EngineError::IngestFull)?;
                let n = transport
                    .read(slot)
                    .await
                    .map_err(EngineError::Transport)?;
                if n == 0 {
                    // Zero bytes while a frame is incomplete: the peer closed
                    // before the response could complete. Never retried (would
                    // spin), never treated as a clean boundary (would truncate).
                    core::hint::cold_path();
                    return Err(EngineError::UnexpectedEof);
                }
                active.commit(n).map_err(EngineError::IngestCommitOverflow)?;
                continue;
            }
            Event::Idle => return Ok(Boundary::Idle),
            Event::Suspended => return Ok(Boundary::Suspended),
            Event::Close => return Ok(Boundary::Closed),
            Event::Fail(body) => {
                // Surface the raw error bytes, then report the failure boundary
                // the verb maps to its error. A sink break still wins as an
                // explicit caller stop.
                return match sink(Surface::Fail(body)) {
                    ControlFlow::Break(b) => {
                        core::hint::cold_path();
                        Ok(Boundary::Stopped(b))
                    }
                    ControlFlow::Continue(()) => Ok(Boundary::Failed),
                };
            }
            Event::Deliver => {
                // Read the just-completed command's projection HERE: the result
                // columns (oids/names) are reset by the next `next_event`'s
                // ReadyForQuery handling, and the tag is overwritten by the next
                // completion — so all three must be read at the delivery.
                // `Deliver` carries no buffer borrow, so re-borrowing the engine
                // for the typed accessors is sound.
                let tag = active.last_command_tag();
                let oids = active.current_type_oids();
                let names = active.current_column_names();
                Surface::Deliver { tag, oids, names }
            }
            Event::Row(body) => Surface::Row(body),
            Event::Notice(body) => Surface::Notice(body),
            Event::Notify(body) => Surface::Notify(body),
            Event::ParamStatus(body) => Surface::ParamStatus(body),
            Event::RowChunk(body) => Surface::RowChunk(body),
            Event::RowChunkEnd => Surface::RowChunkEnd,
            Event::CopyData(body) => Surface::CopyData(body),
            Event::CopyDone => Surface::CopyDone,
        };

        if let ControlFlow::Break(b) = sink(surface) {
            core::hint::cold_path();
            return Ok(Boundary::Stopped(b));
        }
    }
}

/// Drive the connecting engine to its handshake terminal over `transport`.
///
/// Flushes the startup packet (queued onto `send_buf` by
/// [`ConnectingEngine::start`](super::ConnectingEngine::start)), then loops on
/// [`ConnectingEngine::next_handshake_step`](super::ConnectingEngine::next_handshake_step):
/// on an [`AuthResponse`](HandshakeProgress::AuthResponse) drain the queued
/// response; on [`ParameterStatus`](HandshakeProgress::ParamStatus) keep
/// pulling; on [`NeedMore`](HandshakeProgress::NeedMore) flush any still-queued
/// response (the SASL initial response is queued without surfacing an auth
/// event) before reading one chunk into the connecting ingest buffer; on
/// [`Ready`](HandshakeProgress::Ready) / [`Failed`](HandshakeProgress::Failed)
/// return the [`HandshakeOutcome`]. The handshake carries no rows or
/// completions.
///
/// [`HandshakeProgress`] is non-borrowing, so each step's classification (and
/// the classified [`ConnFail`] on failure) outlives the `send_buf`/ingest
/// borrows with no borrow to end before the follow-on `flush`/`read`.
///
/// # Errors
///
/// - [`EngineError::Transport`](super::EngineError::Transport) — the transport
///   reported a read, write, or flush failure (its own error, carried verbatim).
/// - [`EngineError::WriteZero`](super::EngineError::WriteZero) /
///   [`EngineError::SendOverrun`](super::EngineError::SendOverrun) — from a
///   flush (see [`flush`](super::flush)).
/// - [`EngineError::UnexpectedEof`](super::EngineError::UnexpectedEof) — the
///   transport returned `Ok(0)` while the handshake was still incomplete (peer
///   closed mid-handshake).
/// - [`EngineError::IngestFull`](super::EngineError::IngestFull) — a connecting
///   frame larger than the bounded ingest buffer.
/// - [`EngineError::IngestCommitOverflow`](super::EngineError::IngestCommitOverflow)
///   — a transport that reported reading more than the lent slot held.
pub async fn pump_connecting_to_ready<T>(
    conn: &mut ConnectingEngine,
    transport: &mut T,
    send_buf: &mut SendBuf,
) -> Result<HandshakeOutcome, EngineError<T::Error>>
where
    T: Transport,
{
    // Drain the startup packet enqueued at construction, before the first read.
    flush(send_buf, transport).await?;

    loop {
        match conn.next_handshake_step(send_buf) {
            HandshakeProgress::Ready => return Ok(HandshakeOutcome::Ready),
            HandshakeProgress::Failed(reason) => {
                core::hint::cold_path();
                return Ok(HandshakeOutcome::Failed(reason));
            }
            HandshakeProgress::AuthResponse => flush(send_buf, transport).await?,
            // The startup `ParameterStatus` reports keep the pump pulling. The
            // connecting engine captures `server_version` from them as they pass
            // (in `drive_to_event`, the choke point `next_handshake_step` funnels
            // through), carries it across `into_active`, and exposes it via
            // `Engine::server_version` — so a driver reads the server version for
            // free from the handshake instead of a `SHOW server_version`
            // round-trip. The other GUCs are not captured here (no consumer); the
            // pump simply keeps pulling.
            HandshakeProgress::ParamStatus => {}
            HandshakeProgress::NeedMore => {
                // A response built during a silent intermediate (the SASL
                // initial response is queued without surfacing an auth event)
                // must reach the wire before we block on the server's reply.
                if !send_buf.is_drained() {
                    flush(send_buf, transport).await?;
                }
                // `read_slot` lends the whole remaining tier spare, so this one
                // read drains as much of the handshake burst as the socket has
                // in a single syscall — no doubling ramp. `READ_WANT` only
                // drives the inline→heap escape decision (see its doc).
                let slot = conn.read_slot(READ_WANT).map_err(EngineError::IngestFull)?;
                let n = transport.read(slot).await.map_err(EngineError::Transport)?;
                if n == 0 {
                    // Zero bytes while the handshake is incomplete: the peer
                    // closed before it could finish. Never retried, never a
                    // clean boundary.
                    core::hint::cold_path();
                    return Err(EngineError::UnexpectedEof);
                }
                conn.commit(n).map_err(EngineError::IngestCommitOverflow)?;
            }
        }
    }
}

/// Drive `fut` to completion in exactly one poll, for a future built over a
/// blocking transport.
///
/// A future composed from the engine over a blocking transport resolves on the
/// first poll: every leaf await (read / write / flush) blocks synchronously and
/// never returns `Pending`, so one poll suffices — no executor, no spin, no
/// dependency beyond `core::future` / `core::task`. A `Poll::Pending` return is
/// the single runtime invariant the type system cannot prove (the transport was
/// not, in fact, blocking); it is surfaced as the classified
/// [`SpuriousPending`] error rather than spun on or silently dropped.
///
/// Uses [`core::task::Waker::noop`] (a safe, `no_std` waker — no hand-rolled
/// `RawWaker`, so the crate's `#![forbid(unsafe_code)]` holds).
///
/// # Errors
///
/// [`SpuriousPending`] when `fut` returned `Poll::Pending` — the transport was
/// not blocking as this helper requires.
pub fn poll_once<F: Future>(fut: F) -> Result<F::Output, SpuriousPending> {
    let mut fut = core::pin::pin!(fut);
    let waker = core::task::Waker::noop();
    let mut cx = Context::from_waker(waker);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(value) => Ok(value),
        Poll::Pending => {
            core::hint::cold_path();
            Err(SpuriousPending)
        }
    }
}

/// Returned by [`poll_once`] when the future returned `Poll::Pending` — an
/// executor-invariant violation (the transport was not blocking), distinct from
/// any protocol [`EngineError`](super::EngineError): the sync driver maps it to
/// its own error.
///
/// A zero-sized marker; it carries no detail because the violation is binary
/// (the future suspended where it must not).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpuriousPending;

impl core::fmt::Display for SpuriousPending {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(
            "future over a blocking transport returned Poll::Pending under a single-poll executor",
        )
    }
}

impl core::error::Error for SpuriousPending {}

crate::wire_pin!(SpuriousPending, size = 0, align = 1);

#[cfg(test)]
mod hook_tests {
    //! Per-row / per-completion surfacing coverage: drives the pump over a
    //! scripted Bind+Execute reply and asserts it surfaces exactly one
    //! [`Surface::Row`] per whole row and one [`Surface::Deliver`] per completed
    //! command — the two counts read directly off the [`Surface`] sink the pump
    //! already feeds, so no separate instrumentation seam is needed.

    use super::{poll_once, pump_active_to_boundary, Boundary, Surface};
    use crate::action::TxStatus;
    use crate::engine::{ActiveEngine, IngestBuf, SendBuf, Transport};
    use crate::sensitive::Sensitive;
    use core::convert::Infallible;
    use core::future::{ready, Future};
    use core::ops::ControlFlow;

    /// Minimal scripted transport: `read` drains its inbound queue into the lent
    /// slot; write/flush/shutdown are no-op ready. Always-ready, so the pump
    /// future resolves under a single `poll_once`.
    struct ScriptReader {
        inbound: alloc::vec::Vec<u8>,
    }

    impl Transport for ScriptReader {
        type Error = Infallible;
        fn is_would_block(err: &Self::Error) -> bool {
            match *err {}
        }
        fn read<'a>(
            &'a mut self,
            buf: &'a mut [u8],
        ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
            let n = self.inbound.len().min(buf.len());
            for (slot, byte) in buf.iter_mut().zip(self.inbound.drain(..n)) {
                *slot = byte;
            }
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

    fn active() -> ActiveEngine {
        ActiveEngine::from_handshake(
            0_i32,
            Sensitive::new(0_i32),
            TxStatus::Idle,
            IngestBuf::new(),
            None,
        )
    }

    /// The pump surfaces one [`Surface::Row`] per whole row and one
    /// [`Surface::Deliver`] per completed command.
    #[test]
    fn pump_surfaces_one_row_and_one_completion() {
        let mut engine = active();
        // Extended Bind+Execute: the issuer seats the result schema; the reply
        // is BindComplete, one DataRow, CommandComplete, ReadyForQuery.
        engine.begin_bind_execute(&[25]);

        let mut inbound = alloc::vec::Vec::new();
        inbound.extend_from_slice(b"2\x00\x00\x00\x04"); // BindComplete
        inbound.extend_from_slice(b"D\x00\x00\x00\x0b\x00\x01\x00\x00\x00\x01x"); // DataRow ["x"]
        inbound.extend_from_slice(b"C\x00\x00\x00\x0dSELECT 1\x00"); // CommandComplete
        inbound.extend_from_slice(b"Z\x00\x00\x00\x05I"); // ReadyForQuery (Idle)

        let mut transport = ScriptReader { inbound };
        let mut send_buf = SendBuf::new();

        // Count the row and completion surfaces on the sink the pump already
        // feeds — the same events the deleted observer hooks used to tally,
        // now read straight off the public `Surface` stream. `saturating_add`
        // keeps the tally within the crate's checked-arithmetic wall.
        let mut rows = 0_usize;
        let mut completes = 0_usize;
        let outcome: Result<Result<Boundary<()>, _>, _> = poll_once(pump_active_to_boundary(
            &mut engine,
            &mut transport,
            &mut send_buf,
            |surface: Surface<'_>| {
                match surface {
                    Surface::Row(_) => rows = rows.saturating_add(1),
                    Surface::Deliver { .. } => completes = completes.saturating_add(1),
                    _ => {}
                }
                ControlFlow::Continue(())
            },
        ));

        assert!(matches!(outcome, Ok(Ok(Boundary::Idle))));
        assert_eq!(rows, 1);
        assert_eq!(completes, 1);
    }
}
