//! The seam primitives the engine is built from.
//!
//! Three of the four load-bearing seams live here; the fourth (the
//! [`Engine`](super::Engine) shell) composes them in the module root.
//!
//! 1. [`Never`] + [`absurd`] — the uninhabited carrier for
//!    phase-impossible wire frames. A frame the type system cannot rule
//!    out by *omission* is funnelled through `absurd`, never a wildcard
//!    `_` arm.
//! 2. [`Transport`] — the driver-facing I/O seam (RPITIT + `Send`, with
//!    a `Send`-bounded associated [`Error`](Transport::Error) type so the
//!    `#![no_std]` core never bakes in a concrete I/O error and a wrapper
//!    transport's error union stays `Send`).
//! 3. [`Live`] — the branded, non-`Clone`, linear liveness token.

use core::future::Future;
use core::marker::PhantomData;

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
// 2. Transport
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
// 3. Live — branded linear liveness token
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
// 4. Outcome — the alive-verb return carrier (token rides Ok)
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
