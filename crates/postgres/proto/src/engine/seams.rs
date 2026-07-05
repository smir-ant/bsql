//! The seam primitives the engine is built from.
//!
//! Four of the five load-bearing seams live here; the fifth (the
//! [`Engine`](super::Engine) shell) composes them in the module root.
//!
//! 1. [`Never`] + [`absurd`] — the uninhabited carrier for
//!    phase-impossible wire frames. A frame the type system cannot rule
//!    out by *omission* is funnelled through `absurd`, never a wildcard
//!    `_` arm.
//! 2. [`Observer`] (sealed) + [`NoObserver`] — the zero-cost policy seam
//!    carried by every verb. The default policy is a ZST whose hooks
//!    inline to nothing; a non-default policy reuses the identical verb
//!    surface (no second signature pass).
//! 3. [`Transport`] — the driver-facing I/O seam (RPITIT + `Send`, with
//!    a `Send`-bounded associated [`Error`](Transport::Error) type so the
//!    `#![no_std]` core never bakes in a concrete I/O error and a wrapper
//!    transport's error union stays `Send`).
//! 4. [`Live`] — the branded, non-`Clone`, linear liveness token.

use core::future::Future;
use core::marker::PhantomData;
#[cfg(test)]
use core::sync::atomic::{AtomicUsize, Ordering};

use crate::command_tag::CommandTag;

/// Private sealing witness. A trait in a private module cannot be named —
/// let alone implemented — by a downstream crate, so [`Observer`] is
/// closed to exactly the policies this crate blesses.
mod sealed {
    /// Implemented only for this crate's blessed observer policies.
    pub trait Sealed {}
}

// ===========================================================================
// 1. Never + absurd
// ===========================================================================

/// Uninhabited carrier for phase-impossible wire frames.
///
/// A frame that cannot occur in the current protocol phase has no
/// constructor in that phase's event enum; the residual catch routes the
/// impossible byte through [`absurd`] rather than a silent wildcard `_`,
/// so a genuinely-reachable new frame is a loud compile error, never a
/// dropped event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Never {}

/// Consume the impossible. The empty `match` is total because [`Never`]
/// has no inhabitants, so this can produce any `T` without fabricating a
/// value.
#[inline(always)]
pub fn absurd<T>(n: Never) -> T {
    match n {}
}

// Uninhabited carrier: zero-sized, the niche that lets `Boundary<Never>` fold
// its `Stopped` arm into the discriminant (see the `Boundary` pins in `pump`).
crate::wire_pin!(Never, size = 0, align = 1);

// ===========================================================================
// 2. Observer (sealed) + NoObserver
// ===========================================================================

/// Sealed observer-policy seam carried by every verb through the engine
/// type parameter.
///
/// Sealed via a private supertrait: a downstream crate can neither name
/// nor implement the private `sealed::Sealed` witness, so the set of
/// policies is closed to the ones this crate blesses. The bound has no
/// runtime footprint — a
/// generic verb monomorphised at [`NoObserver`] is identical to one with
/// no seam at all.
pub trait Observer: sealed::Sealed {
    /// Invoked once per inbound *whole* data row, lending the row's wire
    /// payload. An oversize row that streams as chunks never resides whole, so
    /// it is surfaced to the pump's sink in pieces and does not invoke this
    /// hook — whose contract lends a complete row a chunked one cannot honour.
    fn on_row(&self, row: &[u8]);
    /// Invoked once per completed command, lending the typed [`CommandTag`] —
    /// `None` for the tagless extended-protocol acknowledgements
    /// (`ParseComplete` / `CloseComplete`) and the `Describe` completion, which
    /// carry no `CommandComplete` tag. The typed tag is lent directly rather
    /// than its raw wire bytes so the hook reads the affected-row count without
    /// re-parsing — the crate's by-type discipline (the engine already parsed
    /// it once).
    fn on_complete(&self, tag: Option<&CommandTag>);
}

/// The default zero-cost policy: a ZST whose hooks are inlined no-ops.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoObserver;

impl sealed::Sealed for NoObserver {}

impl Observer for NoObserver {
    #[inline(always)]
    fn on_row(&self, _row: &[u8]) {}
    #[inline(always)]
    fn on_complete(&self, _tag: Option<&CommandTag>) {}
}

/// Crate-internal [`Observer`] policy that tallies the rows and completed
/// commands it observes — instrumentation for the pump's hook-firing test, NOT
/// part of the public surface ([`NoObserver`] is the only public policy).
///
/// The observer seam is sealed (a downstream crate can neither name nor
/// implement the private witness), so witnessing the hooks fire from outside
/// the crate is impossible; this `#[cfg(test)]`-only `pub(crate)` policy is how
/// the crate's own unit test does it, mirroring the crate's other
/// test-instrumentation (`drop_witness`). It counts
/// [`on_row`](Observer::on_row) and [`on_complete`](Observer::on_complete) via
/// relaxed atomics, so it is `Sync` — a future carrying `&CountingObserver`
/// stays `Send`. The tallies are monotonic and read with
/// [`rows`](Self::rows) / [`completes`](Self::completes).
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct CountingObserver {
    rows: AtomicUsize,
    completes: AtomicUsize,
}

#[cfg(test)]
impl CountingObserver {
    /// Construct a counter with both tallies at zero.
    #[inline]
    pub(crate) const fn new() -> Self {
        Self {
            rows: AtomicUsize::new(0),
            completes: AtomicUsize::new(0),
        }
    }

    /// The number of [`on_row`](Observer::on_row) invocations observed so far.
    #[inline]
    pub(crate) fn rows(&self) -> usize {
        self.rows.load(Ordering::Relaxed)
    }

    /// The number of [`on_complete`](Observer::on_complete) invocations observed
    /// so far.
    #[inline]
    pub(crate) fn completes(&self) -> usize {
        self.completes.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
impl sealed::Sealed for CountingObserver {}

#[cfg(test)]
impl Observer for CountingObserver {
    #[inline]
    fn on_row(&self, _row: &[u8]) {
        self.rows.fetch_add(1, Ordering::Relaxed);
    }
    #[inline]
    fn on_complete(&self, _tag: Option<&CommandTag>) {
        self.completes.fetch_add(1, Ordering::Relaxed);
    }
}

// ===========================================================================
// 3. Transport
// ===========================================================================

/// The driver-facing I/O seam.
///
/// Return-position `impl Future` (RPITIT) keeps the seam allocation- and
/// `dyn`-free; the explicit `+ Send` makes the verb futures `Send` without
/// an `async fn` trait method (whose `Send`-ness cannot be named at the
/// call site). The associated [`Error`](Self::Error) type lets the
/// `#![no_std]` core surface a transport failure without ever naming a
/// concrete I/O error — a `std`-backed socket, a TLS layer, or a scripted
/// in-memory transport each choose their own.
pub trait Transport: Send {
    /// The transport's own failure type. Travels through
    /// [`EngineError::Transport`](super::EngineError::Transport) so the
    /// core never bakes in `std::io::Error`.
    ///
    /// Bounded `Send`: the seam's I/O futures are themselves `+ Send` (the
    /// async driver polls them across task boundaries), and a generic wrapper
    /// transport layered over an inner `Transport` defines its own error union
    /// — `enum E<Inner> { Socket(Inner::Error), Layer(..) }` — which is `Send`
    /// only when the inner `Error` is `Send`. Without this bound such a wrapper
    /// fails to compile (`E0277`: the inner error cannot be sent between
    /// threads). The bound is free for every concrete transport: an infallible
    /// script's `Infallible` and a socket's I/O error are already `Send`.
    type Error: Send;

    /// Classify a transport error as a would-block / timed-out read — a *read
    /// deadline*, not a broken connection.
    ///
    /// An **associated function** (no `self`): would-block-ness is a property of
    /// the error VALUE, not of the transport instance, and the engine holds the
    /// error (returned from a failed [`read`](Self::read)) but not necessarily a
    /// live transport reference at the classification point, so the engine calls
    /// `T::is_would_block(&e)`. This is the one error question the `#![no_std]`
    /// core cannot answer itself — it never names a concrete I/O error, so each
    /// transport classifies its own. [`recv_notification`](super::Engine::recv_notification)
    /// uses it to distinguish a quiet deadline (return the liveness token in
    /// `Ok`, no notification — the connection is alive) from a genuine transport
    /// failure (consume the token — the connection is dead).
    ///
    /// Required, not defaulted (the [`flush`](Self::flush)/[`shutdown`](Self::shutdown)
    /// discipline): a `false` default would let a socket transport that CAN time
    /// out compile while silently misclassifying every read deadline as a fatal
    /// failure — a connection wrongly evicted, or `recv_notification` never
    /// reporting "no notification". Each transport states its semantics: a socket
    /// matches `WouldBlock`/`TimedOut`; an infallible script's `Infallible`
    /// answers the question vacuously (`match *err {}`); a wrapper delegates to
    /// its inner classifier.
    fn is_would_block(err: &Self::Error) -> bool;

    /// Read available bytes into `buf`, returning the count written.
    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send + 'a;

    /// One write attempt, mirroring a single `poll_write`.
    ///
    /// Returns the number of bytes the transport accepted, which **may be
    /// partial** (`0 < n <= buf.len()`), or stays `Pending` (would-block)
    /// having accepted **zero** bytes. The result is atomic by cancellation:
    /// `Ready(Ok(n))` means exactly `n` bytes are committed to the socket and
    /// `Pending` means none are, so a future dropped at this await never tears
    /// the engine's send cursor.
    ///
    /// This is deliberately **not** a write-the-whole-buffer call: looping
    /// until the buffer is drained is the engine's job (it owns the send
    /// cursor, so the loop is cancellation-safe), not the transport's. An
    /// implementation must not internally retry — one attempt, one result.
    /// `Ok(0)` for a non-empty `buf` signals a stalled/broken transport and
    /// the engine classifies it as an error.
    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send + 'a;

    /// Drive any transport-internal buffered bytes to the socket.
    ///
    /// [`write`](Self::write) moves bytes *into* the transport; a buffering
    /// transport (a TLS layer that encrypts plaintext into an internal
    /// record) may still hold bytes that have not reached the socket. The
    /// engine calls `flush` once after its send buffer is drained so a partial
    /// wire frame cannot be left dangling — without it, a buffering transport
    /// would silently truncate the last frame and the peer would hang. A
    /// plaintext socket transport has no internal buffer and returns `Ok(())`.
    ///
    /// Required, not defaulted: a no-op default would let a buffering
    /// transport compile while silently failing to drain its buffer (the
    /// truncation this method exists to prevent), so every implementation
    /// states its flush semantics explicitly.
    fn flush<'a>(
        &'a mut self,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;

    /// Begin an orderly teardown of the write side.
    ///
    /// For a TLS transport this sends `close_notify` (and flushes the
    /// resulting record), so the peer can distinguish a clean close from a
    /// truncation attack; for a plaintext socket it shuts down the write half
    /// (or returns `Ok(())`). Completing the read/write/flush/shutdown quartet
    /// here lets a TLS/socket implementation bind the whole seam once. Like
    /// [`flush`](Self::flush) it is required rather than defaulted, so a TLS
    /// implementation cannot silently omit `close_notify`.
    fn shutdown<'a>(
        &'a mut self,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

// ===========================================================================
// 4. Live — branded linear liveness token
// ===========================================================================

/// Branded, non-`Clone`, linear liveness token.
///
/// Invariant in `'b` (the `fn(&'b ()) -> &'b ()` brand): each session
/// scope mints a fresh, unforgeable `'b`, so a token cannot drive a
/// foreign session's engine. Every verb consumes the token and returns it
/// only on a clean protocol boundary, so the type system enforces
/// at-most-one in-flight command per connection: reuse after a verb
/// consumes it is a move error, not a runtime panic.
///
/// A ZST — the linearity and the brand are purely type-level, with zero
/// runtime footprint.
///
/// Deliberately not `Clone`/`Copy`: copying a token would let a consumed
/// one be reused, defeating the at-most-one-command-in-flight discipline.
#[derive(Debug)]
pub struct Live<'b> {
    _brand: PhantomData<fn(&'b ()) -> &'b ()>,
}

impl<'b> Live<'b> {
    /// Mint a token bound to the caller's brand `'b`. Crate-internal: only
    /// a session scope may mint a token, so downstream code cannot forge
    /// one out of thin air.
    #[inline(always)]
    pub(crate) fn new_in_scope() -> Self {
        Self { _brand: PhantomData }
    }
}

// ===========================================================================
// 5. Outcome — the alive-verb return carrier (token rides Ok)
// ===========================================================================

/// The successful return of a token-threading verb: the linear [`Live`] token
/// rides back on the `Ok` arm together with the protocol [`status`](Self::status)
/// the command reached.
///
/// # The tier-1 reason the token rides `Ok`
///
/// A verb consumes its `Live` and returns one ONLY when the connection is alive
/// and reusable. Two alive outcomes exist: a clean completion and a *recoverable*
/// server error (an `ErrorResponse` the server recovers from via the trailing
/// `ReadyForQuery`, leaving the connection at a clean idle). Both must hand the
/// token back. A linear token cannot be threaded through an `Err` arm — `?` drops
/// the error value, so a token returned in `Err` would be unreachable — so the
/// ONLY shape that keeps the "exactly one `Live` ⟺ at-most-one-command-in-flight"
/// invariant tier-1 (compile-enforced) across a recoverable error is to return
/// the token in `Ok`, tagged with whether the command completed or server-errored.
/// `Err(EngineError)` is then reserved for a FATAL outcome alone: the connection
/// is dead, the token is consumed and not returned. The previous shape minted a
/// fresh token through a separate, token-LESS `recover` verb, which structurally
/// permitted minting a second token for one engine — a tier-1→tier-4 hole this
/// carrier closes.
///
/// Generic over the status `St` so each verb names exactly its reachable outcome
/// set ([`CommandStatus`] for the collect-all command verbs, [`NotifyStatus`] for
/// `recv_notification`) — a verb's caller never faces an outcome the verb cannot
/// produce. [`Live`] is a ZST, so `Outcome<St>` is the size of `St` alone (one
/// byte for both status enums).
#[derive(Debug)]
#[must_use = "the linear Live token rides in the Outcome; dropping it drops the connection"]
pub struct Outcome<'b, St> {
    /// The linear liveness token, threaded back because the connection is alive.
    pub live: Live<'b>,
    /// The protocol status the command reached (completion vs recoverable error,
    /// or — for `recv_notification` — whether a notification arrived).
    pub status: St,
}

// `Live<'b>` is a ZST, so `Outcome<St>` is byte-identical to `St`. Pinned at both
// real instantiations (the verb-status families) — one byte each. Generic over
// `St`, so there is no single canonical size; these two are every shape a verb
// constructs.
crate::wire_pin!(Outcome<'static, CommandStatus>, size = 1, align = 1);
crate::wire_pin!(Outcome<'static, NotifyStatus>, size = 1, align = 1);

/// The protocol status of a collect-all command verb's alive outcome.
///
/// A closed (NOT `#[non_exhaustive]`) two-state tag: the driver and corpus match
/// it exhaustively, so a future variant forces a decision at every call site
/// rather than silently falling into a wildcard. Both outcomes mean the
/// connection is ALIVE — the distinction is whether the command completed cleanly
/// or hit a recoverable server error (whose details rode the verb's sink as
/// `Surface::Fail` before the verb drained the recovering `ReadyForQuery`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandStatus {
    /// A clean `ReadyForQuery` idle boundary — the command completed without a
    /// server error.
    Completed,
    /// A server `ErrorResponse` the connection recovered from: the verb drained
    /// the trailing `ReadyForQuery` to a clean idle before returning, so the
    /// token is reusable. The error details already reached the caller via the
    /// sink; this variant only SIGNALS the recoverable failure.
    ServerErrored,
}

// A two-state tag — one byte, no payload.
crate::wire_pin!(CommandStatus, size = 1, align = 1);

/// The protocol status of `recv_notification`'s alive outcome.
///
/// A closed two-state tag (matched exhaustively, like [`CommandStatus`]). The
/// notification PAYLOAD rides the verb's sink (the borrowed bytes cannot be
/// owned by the `#![no_std]` core); this status only signals whether the pull
/// stopped on a notification or reached a quiet boundary. A server error during
/// a notification wait is FATAL (the verb issues no command, so no recovering
/// `ReadyForQuery` is owed to drain), reported as an `Err`, not a status here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotifyStatus {
    /// A `NotificationResponse` stopped the pull (its payload rode the sink).
    Received,
    /// The pull reached a clean boundary or timed out (a would-block read
    /// deadline) before any notification — the connection is alive and quiet.
    Quiet,
}

// A two-state tag — one byte, no payload.
crate::wire_pin!(NotifyStatus, size = 1, align = 1);
