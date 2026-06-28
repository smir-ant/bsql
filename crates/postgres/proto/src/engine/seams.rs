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

/// The real engine pattern: a generic [`Observer`] hook threaded through a
/// hot loop. Monomorphised at [`NoObserver`] by
/// [`engine_observe_via_seam`], it must lower to the same instructions as
/// the seam-free [`engine_observe_no_seam`] baseline.
#[inline(always)]
fn observe_generic<O: Observer>(obs: &O, state: &mut usize, row: &[u8]) {
    *state = core::hint::black_box(*state).wrapping_add(row.len());
    obs.on_row(row);
}

/// Witness: the observer hook reached through the generic seam, fixed at
/// the [`NoObserver`] ZST policy. The asm-identity gate proves this is
/// instruction-for-instruction identical to [`engine_observe_no_seam`].
#[inline(never)]
pub fn engine_observe_via_seam(state: &mut usize, row: &[u8]) {
    observe_generic(&NoObserver, state, row);
}

/// Witness: the same computation with no observer seam at all — the
/// hand-written baseline the seam must match.
#[inline(never)]
pub fn engine_observe_no_seam(state: &mut usize, row: &[u8]) {
    *state = core::hint::black_box(*state).wrapping_add(row.len());
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
