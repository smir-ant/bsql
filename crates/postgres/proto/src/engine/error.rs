//! The engine's error surface.
//!
//! The sans-I/O core is `#![no_std]`: it cannot bake in a concrete I/O
//! error type (there is no `std::io::Error` to reach for). Instead the
//! transport's own failure type travels as a type parameter, so the same
//! error enum serves a `std`-backed socket driver, a TLS driver, or a
//! scripted in-memory transport without the core ever naming `std`.

use super::flush::SendOverrun;
use super::ingest::{IngestCommitOverflow, IngestFull};

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
    /// The inbound ingest buffer had no room for the next read even after
    /// reclaiming its consumed prefix and escaping to its heap tier — a wire
    /// frame larger than the bounded buffer. Carries the
    /// [`IngestFull`](super::IngestFull) detail from
    /// [`IngestBuf::read_slot`](super::IngestBuf::read_slot). The buffer cannot
    /// grow, so this is classified rather than looped on (which would spin) or
    /// skipped (which would desync the framing).
    IngestFull(IngestFull),
    /// A committed read count would push the ingest fill watermark past the
    /// buffer capacity — a transport that reported writing more bytes than the
    /// lent slot held. Carries the
    /// [`IngestCommitOverflow`](super::IngestCommitOverflow) detail from
    /// [`IngestBuf::commit`](super::IngestBuf::commit); classified rather than
    /// silently truncated.
    IngestCommitOverflow(IngestCommitOverflow),
    /// The transport read returned `Ok(0)` — the peer closed the connection —
    /// while a wire frame was still incomplete. The pump reads only when the
    /// framing reports it lacks a whole frame, so a zero-length read can only
    /// mean the in-flight response can never complete; it is classified as a
    /// broken connection, never retried (which would spin) and never treated as
    /// a clean boundary (which would silently truncate the response). The
    /// read-side mirror of [`WriteZero`](Self::WriteZero).
    UnexpectedEof,
}
