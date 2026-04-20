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

use crate::ident::{ApplicationName, DatabaseName, Ident, Sql};
use crate::password::Credentials;
use crate::reply_id::{PingKind, QueryKind, ReplyId, StartupKind};

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

    /// Execute a single SQL statement via PG's Simple Query protocol
    /// (`Q`-frame). 1c-1b.
    ///
    /// **Precondition:** protocol must be in [`crate::ProtoState::Idle`].
    /// Any other state yields a `FailReply(CommandInProgress)` —
    /// the caller must wait for the current command to finish.
    ///
    /// # Response sequence
    ///
    /// - SELECT: `RowDescription` → 0..N `DataRow` (streamed via
    ///   [`crate::Action::StreamRow`]) → `CommandComplete` →
    ///   `ReadyForQuery` → [`crate::Reply::QueryComplete`] delivered.
    /// - DML (INSERT/UPDATE/DELETE): `CommandComplete` →
    ///   `ReadyForQuery` → `QueryComplete` (no rows).
    /// - Empty query (whitespace-only SQL): `EmptyQueryResponse` →
    ///   `ReadyForQuery` → `QueryComplete { command_tag: "" }`.
    /// - Error: `ErrorResponse` → `ReadyForQuery` →
    ///   `FailReply(ServerErrorResponse { ... })`. The connection
    ///   stays open — per PG spec, `Z` follows `E` in query-level
    ///   errors.
    ///
    /// # Multi-statement batches
    ///
    /// PG's Simple Query allows `;`-separated statement batches;
    /// each statement produces its own `C` response and they all
    /// share a single trailing `Z`. 1c-1b-MVP accepts a single
    /// statement; multi-statement batch support lands in 1c-1-multi.
    SimpleQuery {
        /// SQL text — bounded to [`crate::ident::MAX_SQL_LEN`] =
        /// 2048 bytes with explicit `"…"` truncation on overflow
        /// (no silent drop).
        sql: Sql,
        /// Correlator for the reply.
        ///
        /// DEF-112: typed `ReplyId<QueryKind>` binds the payload to
        /// [`crate::action::QueryCompletePayload`] at compile time.
        reply: ReplyId<QueryKind>,
    },
}
