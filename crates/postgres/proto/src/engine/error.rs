//! The engine's error surface.
//!
//! The sans-I/O core is `#![no_std]`: it cannot bake in a concrete I/O
//! error type (there is no `std::io::Error` to reach for). Instead the
//! transport's own failure type travels as a type parameter, so the same
//! error enum serves a `std`-backed socket driver, a TLS driver, or a
//! scripted in-memory transport without the core ever naming `std`.

use super::flush::SendOverrun;
use super::ingest::{IngestCommitOverflow, IngestFull};
use super::ConnFail;

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
    /// The handshake terminated unsuccessfully — a server `ErrorResponse`
    /// during connect, an auth method the configured credentials cannot
    /// satisfy, a SCRAM/MD5 failure, or a wire-illegal connecting frame.
    /// Carries the classified [`ConnFail`] cause. Classified rather than
    /// retried (the handshake cannot recover) or swallowed.
    Handshake(ConnFail),
    /// A verb was invoked in a protocol phase that does not support it — e.g.
    /// [`connect`](super::Engine::connect) after the engine is already active,
    /// or an active-phase accessor before the handshake completed. A classified
    /// error, never a panic; carries the zero-sized [`WrongPhase`] marker.
    WrongPhase(WrongPhase),
    /// The server reported an `ErrorResponse` for the in-flight command (a
    /// recoverable query-level error: the connection drains to its recovering
    /// `ReadyForQuery`). The raw `ErrorResponse` body was surfaced to the verb's
    /// sink before this error (so the typed layer above reads its SQLSTATE);
    /// this variant marks the command as failed without re-wrapping those bytes.
    /// Distinct from [`ProtocolViolation`](Self::ProtocolViolation): a server
    /// error is recoverable, a protocol violation tears the connection down.
    ServerError,
    /// The engine tore the connection down on an out-of-phase / wire-illegal
    /// frame (the active pump returned [`Boundary::Closed`](super::Boundary::Closed)).
    /// The socket must be closed; the connection is not reusable. Classified
    /// rather than retried or silently ignored.
    ProtocolViolation,
    /// A `PortalSuspended` arrived for a command the verb did not row-limit — a
    /// row cap appeared where none was requested. Classified rather than treated
    /// as a clean completion (which would silently drop the open portal).
    UnexpectedSuspend,
    /// A row-count guard verb ([`query_one`](super::Engine::query_one) /
    /// [`query_opt`](super::Engine::query_opt)) observed a row count outside its
    /// contract. Carries the [`RowCountViolation`] detail (which guard, and the
    /// count actually seen). A reachable caller-facing misuse, not a defensive
    /// check of an impossible event.
    RowCount(RowCountViolation),
    /// An outbound request frame did not fit the bounded frame builder — the SQL
    /// text or the encoded parameters exceeded the engine's fixed outbound
    /// capacity. Classified rather than silently truncated.
    FrameTooLong,
    /// A synchronous single-poll verb ([`transaction`](super::Engine::transaction))
    /// drove a future that returned `Poll::Pending` — the transport was not
    /// blocking as the single-poll contract requires. The
    /// [`SpuriousPending`](super::SpuriousPending) read-side analog for the verb
    /// layer.
    SpuriousPending,
}

// Pinned at the representative `E = Infallible` — the witness type the engine's
// own `Send`/seam gates name, and the `#![no_std]` core's canonical
// instantiation (a real driver supplies its own `Transport::Error`). With the
// `Transport(E)` variant uninhabited, this captures the *non-transport* error
// envelope: the widest variant is `IngestFull(IngestFull)` (a 24 B body —
// IngestFull's three usize fields dominate IngestCommitOverflow/SendOverrun/
// RowCount's 16 B) plus the discriminant → 32. Generic over `E`, so there is no
// single canonical size; a driver's real `E` adds at most `size_of::<E>()`.
crate::wire_pin!(EngineError<core::convert::Infallible>, size = 32, align = 8);

/// The row-count contract a guard verb enforces — the expectation half of a
/// [`RowCountViolation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRowCount {
    /// [`query_one`](super::Engine::query_one): exactly one row.
    ExactlyOne,
    /// [`query_opt`](super::Engine::query_opt): at most one row.
    AtMostOne,
}

/// A row-count guard verb observed a count outside its contract.
///
/// Carries both the [`expected`](Self::expected) contract and the count actually
/// surfaced, so the caller's message names the exact violation without a
/// stringly-typed reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowCountViolation {
    /// The contract the verb enforces.
    pub expected: ExpectedRowCount,
    /// The number of rows the command actually surfaced.
    pub got: usize,
}

impl core::fmt::Display for RowCountViolation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let expected = match self.expected {
            ExpectedRowCount::ExactlyOne => "exactly one row",
            ExpectedRowCount::AtMostOne => "at most one row",
        };
        write!(f, "row-count guard expected {}, got {}", expected, self.got)
    }
}

impl core::error::Error for RowCountViolation {}

// A two-state tag — one byte, no payload.
crate::wire_pin!(ExpectedRowCount, size = 1, align = 1);
// `ExpectedRowCount` tag (1) + `got: usize` (8), aligned to 8 → 16 B.
crate::wire_pin!(RowCountViolation, size = 16, align = 8);

/// A verb or accessor was invoked in the wrong protocol phase.
///
/// Zero-sized: the violation is binary (the engine was not in the phase the
/// call requires), so there is no detail to carry. Returned directly by the
/// engine's phase-query accessors
/// ([`backend_pid`](super::Engine::backend_pid) /
/// [`tx_status`](super::Engine::tx_status)) and carried by
/// [`EngineError::WrongPhase`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WrongPhase;

impl core::fmt::Display for WrongPhase {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("engine verb invoked in the wrong protocol phase")
    }
}

impl core::error::Error for WrongPhase {}

crate::wire_pin!(WrongPhase, size = 0, align = 1);
