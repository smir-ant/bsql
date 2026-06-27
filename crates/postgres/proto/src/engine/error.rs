//! The engine's error surface.
//!
//! The sans-I/O core is `#![no_std]`: it cannot bake in a concrete I/O
//! error type (there is no `std::io::Error` to reach for). Instead the
//! transport's own failure type travels as a type parameter, so the same
//! error enum serves a `std`-backed socket driver, a TLS driver, or a
//! scripted in-memory transport without the core ever naming `std`.

use super::flush::SendOverrun;

/// Failure surface returned by every engine verb.
///
/// Generic over the transport's own error type `E` (see
/// [`Transport::Error`](super::Transport::Error)): the core never names a
/// concrete I/O error, honouring the `#![no_std]` contract. Marked
/// `#[non_exhaustive]` so protocol- and decode-level variants can be added
/// without breaking a downstream `match`.
#[derive(Debug)]
#[non_exhaustive]
pub enum EngineError<E> {
    /// The underlying [`Transport`](super::Transport) reported a failure.
    /// Carries the transport's own error value verbatim — no lossy
    /// re-wrapping into a stringly-typed I/O error.
    Transport(E),
    /// The transport accepted zero bytes from a non-empty send buffer — a
    /// stalled or broken write side. The flush loop only ever offers a
    /// non-empty tail (a drained buffer ends the loop), so an `Ok(0)` is
    /// never an empty-buffer artefact; it is classified rather than looped on
    /// (which would spin forever) or skipped (which would silently drop
    /// bytes).
    WriteZero,
    /// The transport reported accepting more bytes than it was offered,
    /// which would push the send cursor past the end of the queued bytes.
    /// Carries the [`SendOverrun`](super::SendOverrun) detail from
    /// [`SendBuf::advance`](super::SendBuf::advance).
    SendOverrun(SendOverrun),
}
