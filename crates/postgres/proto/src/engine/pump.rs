//! The active-phase engine pump + the synchronous single-poll helper.
//!
//! # The active pump composes the engine's pieces into one I/O loop
//!
//! [`pump_active_to_boundary`] is the free function that turns the sans-I/O
//! active engine into a driven exchange: it flushes the outbound request once,
//! then repeatedly classifies one inbound frame and reads more bytes when the
//! framing is short, until the engine reaches a protocol [`Boundary`]. It owns
//! no state of its own — it borrows the [`ActiveEngine`], the
//! [`Transport`](super::Transport), the [`SendBuf`], and the
//! [`Observer`](super::Observer) as parameters, so it composes the I/O loop
//! without being a method on any of them (the same disjoint-`&mut` shape the
//! outbound [`flush`](super::flush) free function uses).
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
use crate::frame::READ_BUF_CAP;

use super::flush::flush;
use super::seams::{Observer, Transport};
use super::{
    ActiveEngine, ConnFail, ConnectingEngine, EngineError, Event, HandshakeProgress, SendBuf,
};

/// Initial per-read request width handed to
/// [`ActiveEngine::read_slot`](super::ActiveEngine::read_slot).
///
/// Bound to the ingest buffer's inline-tier width (single source of truth) so a
/// command whose entire response fits inline is read in a single syscall and
/// never escapes to the heap tier; a read that fills the offered slot signals a
/// larger response, so the request doubles (saturating, capped at the heap tier)
/// to amortise the syscall cost of a row stream. Coupling to the const keeps the
/// inline-fit property intact if the tier width is ever changed. A mismatch
/// would only be a sizing nuance, never a correctness issue — a too-small start
/// costs an extra read, a too-large start escapes early.
const INITIAL_READ_WANT: usize = super::ingest::INGEST_INLINE_CAP;

/// The protocol boundary at which [`pump_active_to_boundary`] returns.
///
/// Non-borrowing: it carries no reference into the ingest buffer, so the verb
/// that receives it can re-borrow the engine freely. `#[non_exhaustive]` so a
/// future boundary can be added without breaking a downstream `match`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Boundary {
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
    /// The sink returned [`ControlFlow::Break`], requesting an early stop. The
    /// connection is NOT at a protocol boundary — unread frames may remain
    /// buffered or on the wire — so recovery (drain or close) is the caller's
    /// responsibility. Distinct from [`Idle`](Self::Idle), which alone means the
    /// connection is clean and reusable: reporting a caller-requested stop as
    /// `Idle` would falsely claim a reusable connection.
    Stopped,
}

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

/// Drive the active engine to its next protocol [`Boundary`] over `transport`.
///
/// Flushes `send_buf` exactly once at entry (draining the request a verb
/// enqueued — the read loop never enqueues, so there is no redundant per-read
/// flush), then loops: classify one inbound frame via
/// [`ActiveEngine::next_event`]; on `NeedMore`, read one chunk from `transport`
/// into the engine's ingest buffer; on a payload event, fire the observer hook
/// (rows / completions) and hand the [`Surface`] to `sink`; on a boundary,
/// return.
///
/// `sink` consumes each [`Surface`] in the call and returns
/// [`ControlFlow`]: [`ControlFlow::Break`] stops the pump early and returns
/// [`Boundary::Stopped`]. The observer's row hook fires for each *whole* row
/// only; an oversize row is surfaced as [`Surface::RowChunk`] /
/// [`Surface::RowChunkEnd`] and does not invoke the row hook (see
/// [`Observer::on_row`](super::Observer::on_row)).
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
pub async fn pump_active_to_boundary<T, O, S>(
    active: &mut ActiveEngine,
    transport: &mut T,
    send_buf: &mut SendBuf,
    obs: &O,
    mut sink: S,
) -> Result<Boundary, EngineError<T::Error>>
where
    T: Transport,
    O: Observer,
    S: FnMut(Surface<'_>) -> ControlFlow<()>,
{
    // Drain the enqueued request once, before the first read.
    flush(send_buf, transport).await?;

    let mut want = INITIAL_READ_WANT;

    loop {
        // The borrowing `Event` is confined to this `match`: arms either diverge
        // (read / return a boundary) before binding any payload, or yield a
        // `Surface` that is consumed by the single `sink` call below. No borrow
        // crosses into the next iteration, so the next `read_slot` re-borrow is
        // free of an E0499 conflict.
        let surface = match active.next_event() {
            Event::NeedMore => {
                let slot = active.read_slot(want).map_err(EngineError::IngestFull)?;
                let slot_len = slot.len();
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
                if n == slot_len {
                    // The slot filled — the response is larger than offered, so
                    // widen the next request to amortise reads on a stream.
                    want = want.saturating_mul(2).min(READ_BUF_CAP);
                }
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
                    ControlFlow::Break(()) => {
                        core::hint::cold_path();
                        Ok(Boundary::Stopped)
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
                obs.on_complete(tag);
                Surface::Deliver { tag, oids, names }
            }
            Event::Row(body) => {
                obs.on_row(body);
                Surface::Row(body)
            }
            Event::Notice(body) => Surface::Notice(body),
            Event::Notify(body) => Surface::Notify(body),
            Event::ParamStatus(body) => Surface::ParamStatus(body),
            Event::RowChunk(body) => Surface::RowChunk(body),
            Event::RowChunkEnd => Surface::RowChunkEnd,
            Event::CopyData(body) => Surface::CopyData(body),
            Event::CopyDone => Surface::CopyDone,
        };

        if let ControlFlow::Break(()) = sink(surface) {
            core::hint::cold_path();
            return Ok(Boundary::Stopped);
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
/// return the [`HandshakeOutcome`]. No observer is involved — the handshake
/// carries no rows or completions.
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

    let mut want = INITIAL_READ_WANT;

    loop {
        match conn.next_handshake_step(send_buf) {
            HandshakeProgress::Ready => return Ok(HandshakeOutcome::Ready),
            HandshakeProgress::Failed(reason) => {
                core::hint::cold_path();
                return Ok(HandshakeOutcome::Failed(reason));
            }
            HandshakeProgress::AuthResponse => flush(send_buf, transport).await?,
            HandshakeProgress::ParamStatus => {}
            HandshakeProgress::NeedMore => {
                // A response built during a silent intermediate (the SASL
                // initial response is queued without surfacing an auth event)
                // must reach the wire before we block on the server's reply.
                if !send_buf.is_drained() {
                    flush(send_buf, transport).await?;
                }
                let slot = conn.read_slot(want).map_err(EngineError::IngestFull)?;
                let slot_len = slot.len();
                let n = transport.read(slot).await.map_err(EngineError::Transport)?;
                if n == 0 {
                    // Zero bytes while the handshake is incomplete: the peer
                    // closed before it could finish. Never retried, never a
                    // clean boundary.
                    core::hint::cold_path();
                    return Err(EngineError::UnexpectedEof);
                }
                conn.commit(n).map_err(EngineError::IngestCommitOverflow)?;
                if n == slot_len {
                    // The slot filled — a larger burst (e.g. many
                    // `ParameterStatus` frames) follows; widen the next request.
                    want = want.saturating_mul(2).min(READ_BUF_CAP);
                }
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
    //! Hook-firing coverage for the observer seam: drives the pump with the
    //! crate-internal `CountingObserver` (a non-default sealed observer policy,
    //! which only crate-internal code can name) and asserts the row/complete
    //! hooks fire the expected number of times. The externally-observable
    //! behaviour — rows and the `Deliver` projection captured through the
    //! `Surface` sink — is covered by the `engine_pump_spec` integration test on
    //! the default `NoObserver`; only this instrumentation count lives here.

    use super::super::seams::CountingObserver;
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
        ActiveEngine::from_handshake(0_i32, Sensitive::new(0_i32), TxStatus::Idle, IngestBuf::new())
    }

    /// The pump fires `on_row` once per whole row and `on_complete` once per
    /// completed command.
    #[test]
    fn observer_hooks_fire_per_row_and_per_completion() {
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
        let obs = CountingObserver::new();

        let outcome = poll_once(pump_active_to_boundary(
            &mut engine,
            &mut transport,
            &mut send_buf,
            &obs,
            |_s: Surface<'_>| ControlFlow::Continue(()),
        ));

        assert!(matches!(outcome, Ok(Ok(Boundary::Idle))));
        assert_eq!(obs.rows(), 1);
        assert_eq!(obs.completes(), 1);
    }
}
