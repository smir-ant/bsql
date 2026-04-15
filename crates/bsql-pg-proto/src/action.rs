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
//! [`Action::SendBytes`] (carrying [`SendBuf::Static`]) and
//! [`Action::DeliverReply`] / [`Action::FailReply`] (carrying
//! [`Reply::Pong`]). [`Action::CloseSocket`] is also shipped because
//! protocol-error paths in 1a require it.
//!
//! Other actions (`StreamRow`, `SendBuf::Owned`, additional `Reply`
//! variants) land with their drivers — reforge.md §3.5.

use crate::error::ProtocolError;
use crate::protocol::MAX_ACTIONS_PER_CALL;
use crate::reply_id::ReplyId;
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
    DeliverReply {
        /// The correlator the user originally supplied with their
        /// command.
        id: ReplyId,
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
        id: ReplyId,
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
/// Phase 1a ships only the [`Static`] variant because the only
/// outbound message in scope (`Sync`) is a const 5-byte payload.
/// [`Owned`] (a bounded `heapless::Vec`) lands with the first
/// runtime-built outbound message — startup, parse, bind. See
/// reforge.md §7.10 for the rationale of the enum shape.
///
/// `#[non_exhaustive]` lets us add `Owned` without breaking user
/// `match`es.
///
/// [`Static`]: SendBuf::Static
/// [`Owned`]: # "lands in 1b/1c"
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SendBuf {
    /// A compile-time const wire payload (`&'static [u8]`).
    ///
    /// Zero alloc, zero copy. The bytes live in the binary's read-only
    /// section.
    Static(&'static [u8]),
}

impl SendBuf {
    /// Borrow the underlying wire bytes.
    #[inline]
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Static(bytes) => bytes,
        }
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
}
