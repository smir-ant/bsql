//! Side-effect directives emitted by the protocol state machine.
//!
//! [`Action`]s are how the sans-I/O core communicates with whatever
//! sits outside it: "send these bytes", "deliver this reply", "fail
//! this reply", "close the socket". The async wrapper translates each
//! [`Action`] into the corresponding tokio call; a synchronous test
//! harness pattern-matches them directly. The protocol itself does
//! neither.
//!
//! Phase 1a ships only the actions the Ping flow produces:
//! [`Action::SendBytes`] (carrying a [`SendBuf`]) and
//! [`Action::DeliverReply`] / [`Action::FailReply`] (carrying
//! [`Reply::Pong`]). [`Action::CloseSocket`] is also shipped because
//! protocol-error paths in 1a require it.
//!
//! Other actions (`StreamRow`, additional `Reply` variants) land
//! with their drivers — reforge.md §3.5.

use crate::error::ProtocolError;
use crate::protocol::MAX_ACTIONS_PER_CALL;
use crate::write_buf::MAX_OWNED_SEND_LEN;
use core::num::NonZeroU64;
use heapless::Vec;

/// Bounded list of actions emitted by a single protocol entry-point
/// call.
///
/// `MAX_ACTIONS_PER_CALL` is intentionally tiny in Phase 1a — see its
/// definition for the per-method audit. The `heapless::Vec` returns
/// `Err` on overflow; our internal helper [`crate::protocol::push_action`]
/// turns that into a compile-time impossibility via per-call-site
/// `const _: () = assert!(MAX_ACTIONS_PER_CALL >= …)`.
pub type OutActions = Vec<Action, MAX_ACTIONS_PER_CALL>;

/// A directive from the protocol to its host.
///
/// `#[non_exhaustive]` because more variants land with later sub-phases
/// (`StreamRow`, etc.). Internal `match` over `Action` is *not*
/// `non_exhaustive` (we treat the type's own crate as authoritative
/// for the internal exhaustive check).
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
#[must_use = "an Action carries a side-effect that must be executed"]
#[expect(clippy::large_enum_variant, reason = "no_alloc crate: Box unavailable; Action is moved once per frame, not per row — the stack cost is amortised")]
pub enum Action {
    /// Send these bytes verbatim to the server.
    ///
    /// The buffer is owned by the [`SendBuf`] enum; the host pulls
    /// `as_bytes()` and writes them. Once the host has performed the
    /// write, it drops the [`Action`] — the buffer goes with it.
    SendBytes(SendBuf),

    /// Deliver a successful reply to the wrapper.
    ///
    /// The wrapper looks up its `oneshot::Sender` by `id` and forwards
    /// `value`. The protocol does not keep any record after emitting
    /// this action.
    ///
    /// The `id` here is the raw `NonZeroU64` the command's `ReplyId`
    /// was built from — the protocol state machine called
    /// [`crate::ReplyId::consume`] on the handle to produce this value,
    /// which marks the reply as delivered (see the Drop-guard on
    /// `ReplyId`). The wrapper only needs the raw value to route;
    /// the consume-tracking handle is an internal protocol concept.
    DeliverReply {
        /// The correlator the user originally supplied with their
        /// command.
        id: NonZeroU64,
        /// The typed payload.
        value: Reply,
    },

    /// Deliver a failure to the wrapper.
    ///
    /// Same routing as `DeliverReply`; the wrapper translates `cause`
    /// into its public error type.
    FailReply {
        /// The correlator the user originally supplied with their
        /// command.
        id: NonZeroU64,
        /// Why the protocol failed the in-flight command.
        cause: ProtocolError,
    },

    /// The socket is no longer safe to use; close it.
    ///
    /// Emitted alongside a failed reply when the connection is
    /// out-of-sync with the server (malformed framing, unexpected
    /// frame, etc.). The wrapper must close the underlying transport;
    /// the pool then discards this connection.
    CloseSocket,
}

/// A wire-bytes buffer for [`Action::SendBytes`].
///
/// Newtype wrapper around `heapless::Vec<u8, MAX_OWNED_SEND_LEN>`.
/// All outbound frames — whether 5-byte `Sync` or runtime-built
/// `StartupMessage` / SASL messages — flow through the same bounded
/// stack buffer. This is DEF-089: the previous two-arm enum
/// (`Static(&'static [u8])` / `Owned(heapless::Vec<u8, N>)`) had a
/// **tier-3 shield seam** in `as_bytes` — a silent swap of the two
/// match arms would compile and cross-wire every outbound message.
/// The collapsed single-path design eliminates the seam at tier-1
/// structural (no enum, no match, no swap).
///
/// Cost: a 5-byte `memcpy` for the const `Sync` message on every
/// ping (before: zero-copy via `&'static [u8]`). Negligible — Ping is
/// rare, 5 bytes fit in one cache line. Full zero-copy via
/// lifetime-bound `&'buf [u8]` references is the DEF-059 / Phase 1c
/// work where compute/apply split naturally threads the lifetime.
///
/// `#[non_exhaustive]` prevents external crates from constructing
/// `SendBuf(inner)` directly — construction must go through the
/// public `from_slice` constructor (which validates capacity) or the
/// `pub(crate)` `from_owned` (used when we already built a `WriteBuf`
/// and move its inner Vec).
///
/// `#[repr(transparent)]` guarantees ABI-identical layout with the
/// inner `heapless::Vec<u8, N>` — formal zero-cost abstraction.
#[derive(Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
#[repr(transparent)]
pub struct SendBuf {
    /// Backing storage. Named field (not tuple-struct positional `.0`)
    /// for consistency with every other newtype in the crate
    /// (`Ident.buf`, `DatabaseName.buf`, `ApplicationName.buf`,
    /// `SecretDigest.bytes`, `CappedServerNonce.buf`,
    /// `Sensitive.inner`). `#[repr(transparent)]` holds with one
    /// named field same as with a positional one.
    inner: heapless::Vec<u8, MAX_OWNED_SEND_LEN>,
}

/// Returned when a slice passed to [`SendBuf::from_slice`] exceeds
/// the bounded capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendBufFull {
    /// Actual byte length of the rejected input.
    pub attempted: usize,
}

impl core::fmt::Display for SendBufFull {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "send buffer full: tried to store {} bytes (max {})",
            self.attempted, MAX_OWNED_SEND_LEN,
        )
    }
}

impl SendBuf {
    /// Construct from a byte slice. Fails if the slice exceeds
    /// [`MAX_OWNED_SEND_LEN`].
    ///
    /// Error classification is the caller's responsibility — in
    /// protocol-internal call sites where input is compile-time-known
    /// to fit (e.g. the 5-byte `SYNC_WIRE_BYTES`), the Err branch is
    /// dead but surfaced honestly via `let-else` → `ProtocolError::ProtocolInvariantBroken`.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, SendBufFull> {
        let mut inner = heapless::Vec::new();
        inner
            .extend_from_slice(bytes)
            .map_err(|_| SendBufFull {
                attempted: bytes.len(),
            })?;
        Ok(Self { inner })
    }

    /// Construct from an already-owned bounded buffer. `pub(crate)`
    /// because the normal path through external callers is
    /// [`SendBuf::from_slice`]; this exists for internal use when
    /// `WriteBuf::into_inner()` has already produced the buffer.
    #[inline]
    pub(crate) const fn from_owned(inner: heapless::Vec<u8, MAX_OWNED_SEND_LEN>) -> Self {
        Self { inner }
    }

    /// Borrow the underlying wire bytes.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.inner
    }
}

/// A typed protocol reply payload.
///
/// `#[non_exhaustive]` because more variants (`QueryResult`, `Row`,
/// `BackendKeyData`, …) land with their drivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Reply {
    /// The server is alive and responsive.
    ///
    /// Carries the [transaction-status indicator] from the matching
    /// `ReadyForQuery` payload byte: `'I'` idle, `'T'` in-transaction,
    /// `'E'` failed transaction. In Phase 1a we surface it raw; the
    /// transaction state machine (1c) consumes it.
    ///
    /// [transaction-status indicator]: https://www.postgresql.org/docs/current/protocol-message-formats.html
    Pong {
        /// The single payload byte — `'I'`, `'T'`, or `'E'`.
        tx_status: u8,
    },

    /// The startup handshake completed successfully.
    ///
    /// The connection is now in [`crate::ProtoState::Idle`] and ready
    /// for queries. Carries the backend process ID and secret key
    /// (for cancel requests) and the transaction status byte from the
    /// final `ReadyForQuery`.
    StartupComplete {
        /// Backend process ID from `BackendKeyData`.
        pid: i32,
        /// Backend secret key from `BackendKeyData` (cancel key).
        secret_key: i32,
        /// Transaction status from `ReadyForQuery` (`'I'`, `'T'`, `'E'`).
        tx_status: u8,
    },
}
