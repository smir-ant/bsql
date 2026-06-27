//! The engine's error surface.
//!
//! The sans-I/O core is `#![no_std]`: it cannot bake in a concrete I/O
//! error type (there is no `std::io::Error` to reach for). Instead the
//! transport's own failure type travels as a type parameter, so the same
//! error enum serves a `std`-backed socket driver, a TLS driver, or a
//! scripted in-memory transport without the core ever naming `std`.

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
}
