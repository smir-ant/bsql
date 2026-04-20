//! User-pushed commands.
//!
//! A [`PgCommand`] is the upstream wrapper's request to the protocol
//! state machine. Each variant carries a [`crate::ReplyId`] — the
//! correlator the wrapper later uses to route the reply back to the
//! correct caller's `oneshot::Sender`.
//!
//! Phase 1a ships exactly one variant: [`PgCommand::Ping`]. Other
//! variants (`Query`, `Execute`, `Begin`, …) land with their drivers
//! per reforge.md §3.5.

use crate::ident::{ApplicationName, DatabaseName, Ident};
use crate::password::Credentials;
use crate::reply_id::{PingKind, ReplyId, StartupKind};

/// A command pushed by the wrapper into the protocol state machine.
///
/// `#[non_exhaustive]` because new commands land in 1b–1d as their
/// driving paths come online; user `match` arms must accommodate
/// growth.
///
/// `#[must_use]` because constructing a command without pushing it
/// into [`crate::PgProtocol::push_command`] cannot deliver a reply —
/// the user's `oneshot::Receiver` would block forever.
///
/// # No `Clone`
///
/// `PgCommand` owns a [`ReplyId`] which is deliberately non-duplicable
/// (see [`ReplyId`] docstring). A cloneable `PgCommand` would imply a
/// cloneable id, which would break the tier-1 "no silent reply loss"
/// invariant. If a caller needs multiple commands they mint multiple
/// ids from the wrapper's monotonic counter and build multiple commands.
#[derive(Debug)]
#[non_exhaustive]
#[must_use = "a PgCommand has no effect until pushed via PgProtocol::push_command"]
#[expect(clippy::large_enum_variant, reason = "no_alloc crate: Box unavailable; PgCommand is constructed once per user request, not per row")]
pub enum PgCommand {
    /// Cheap server liveness probe.
    ///
    /// Translated by the protocol to a `Sync` frame (5 wire bytes); the
    /// matching `ReadyForQuery` arrives back as
    /// [`crate::Reply::Pong`] under the supplied `reply` id.
    ///
    /// **Precondition:** the protocol must be in [`crate::ProtoState::Idle`].
    /// In Phase 1a, that is the protocol's starting state. In later
    /// sub-phases (transactions, mid-stream queries), pushing a Ping
    /// outside `Idle` will be classified by the dispatcher.
    Ping {
        /// Correlator the wrapper will use to route the matching
        /// [`crate::Reply::Pong`] back to the caller.
        ///
        /// DEF-112: the type parameter `PingKind` binds the reply
        /// payload to [`crate::action::PongPayload`] at compile
        /// time — the dispatcher cannot produce any other payload
        /// for this id.
        reply: ReplyId<PingKind>,
    },

    /// Initiate the PostgreSQL startup handshake.
    ///
    /// Builds and sends a `StartupMessage` frame. The protocol then
    /// navigates the authentication exchange (trust or SCRAM-SHA-256)
    /// followed by the post-auth chain (ParameterStatus, BackendKeyData,
    /// ReadyForQuery) before transitioning to [`crate::ProtoState::Idle`]
    /// and emitting [`crate::Reply::StartupComplete`].
    Startup {
        /// The PostgreSQL user to authenticate as.
        user: Ident,
        /// Optional database name (defaults to user name on the server).
        database: Option<DatabaseName>,
        /// Optional application name for `application_name` parameter.
        app_name: Option<ApplicationName>,
        /// Authentication credentials.
        credentials: Credentials,
        /// Correlator for the Startup command.
        ///
        /// DEF-112: typed `ReplyId<StartupKind>` binds the reply
        /// payload to [`crate::action::StartupCompletePayload`].
        reply: ReplyId<StartupKind>,
    },
}
