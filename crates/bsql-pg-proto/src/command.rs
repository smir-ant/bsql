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

use crate::ident::{ApplicationName, DatabaseName, Ident, PortalName, Sql, StmtName};
use crate::password::Credentials;
use crate::reply_id::{
    DescribePortalKind, DescribeStatementKind, ParseKind, PingKind, QueryKind, ReplyId,
    StartupKind,
};

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
/// DEF-269 v2 (T): demoted to `pub(crate)` — external callers construct
/// per-command structs (`crate::push_command::{Ping, Startup, ...}`).
/// The enum is retained for the lib-internal `compute_push_tests` mod
/// and the legacy `impl PushCommand for PgCommand` blanket impl.
#[derive(Debug)]
#[non_exhaustive]
#[must_use = "a PgCommand has no effect until pushed via PgProtocol::push_command"]
#[allow(dead_code, reason = "DEF-269 v2: variants only constructed by lib-internal compute_push_tests mod + the blanket `impl PushCommand for PgCommand` (legacy slow-path)")]
pub(crate) enum PgCommand {
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

    /// Prepare a named SQL statement via PG's Extended Query protocol
    /// (`P`-frame + `S`-frame terminator). 1c-3a.
    ///
    /// **Precondition:** protocol must be in [`crate::ProtoState::Idle`].
    /// A Parse while busy yields `FailReply(CommandInProgress)`.
    ///
    /// # Response sequence
    ///
    /// - Success: `ParseComplete` (`'1'`) → `ReadyForQuery` (`'Z'`) →
    ///   [`crate::Reply::ParseComplete`] delivered.
    /// - Server-side syntax error: `ErrorResponse` (`'E'`) →
    ///   `ReadyForQuery` → `FailReply(ServerErrorResponse{…})`. The
    ///   connection stays open (same recoverable-error pattern as
    ///   SimpleQuery per PG §55.2.3).
    ///
    /// # Sync-bundling
    ///
    /// This command emits **two** outbound wire frames in one push:
    /// a Parse frame followed by a Sync frame. Without the Sync,
    /// PG buffers Extended Query responses indefinitely; a bare
    /// Parse would never reach the client. Bundling keeps the
    /// single-command API shape consistent with `Ping` / `SimpleQuery`;
    /// pipelining (many commands before one Sync) lands in 1c-3e.
    ///
    /// # Parameter types
    ///
    /// 1c-3a does not ship parameter-type hints — the `n_param_types`
    /// field is always zero on the wire. Type hints land in 1c-3b
    /// alongside `Bind` + `ParamsWriter`.
    ///
    /// # Stack cost per push
    ///
    /// `PgCommand::Parse` is the dominant variant of the `PgCommand`
    /// enum — ~2132 B (stmt_name 66 + sql 2050 + reply 16). Every
    /// `push_command(PgCommand::Parse { .. })` call allocates this
    /// on the caller's stack even for a 10-byte SQL. The cost is
    /// inherent to the `no_alloc` design: we cannot `Box<Sql>` the
    /// payload.
    ///
    /// Tradeoffs considered and rejected:
    /// 1. Shrink [`crate::ident::MAX_SQL_LEN`] from 2048 to 512 —
    ///    breaks users with larger SQL. 4× shrink for marginal stack
    ///    win.
    /// 2. Streaming API `push_parse_streamed(stmt, |w| write_sql_to(w))`
    ///    — more complex user API, not compatible with the unified
    ///    `PgCommand` enum; would require a separate method like
    ///    `push_bind_execute`.
    ///
    /// Accept: 2 KB of stack per Parse push is fine on tokio / std;
    /// more problematic on embedded `no_std` + thin stacks. Document
    /// and move on.
    Parse {
        /// The prepared-statement name. Empty (the "unnamed statement"
        /// per PG convention) or a validated `StmtName` up to
        /// [`crate::ident::MAX_PG_NAME_LEN`] bytes.
        stmt_name: StmtName,
        /// SQL text — bounded to [`crate::ident::MAX_SQL_LEN`] with
        /// truncating constructor.
        sql: Sql,
        /// Correlator for the reply.
        ///
        /// DEF-112: typed `ReplyId<ParseKind>` binds the payload to
        /// [`crate::action::ParseCompletePayload`] at compile time.
        reply: ReplyId<ParseKind>,
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

    /// Inspect a previously-[`PgCommand::Parse`]'d prepared
    /// statement via PG's Extended Query `Describe` + `Sync` bundle
    /// (PG §55.2.2). 1c-3c.
    ///
    /// **Precondition:** protocol must be in [`crate::ProtoState::Idle`].
    /// A Describe while busy yields `FailReply(CommandInProgress)`.
    ///
    /// # Response sequence — statement target
    ///
    /// Success flow:
    /// - `ParameterDescription` (`'t'`) — type OIDs for each `$N`
    ///   placeholder the statement declared.
    /// - `RowDescription` (`'T'`) **or** `NoData` (`'n'`) —
    ///   `RowDescription` for row-producing statements
    ///   (`SELECT`, `INSERT ... RETURNING`, …); `NoData` for DML
    ///   without `RETURNING`.
    /// - `ReadyForQuery` (`'Z'`) — delivers
    ///   [`crate::Reply::DescribeStatementComplete`] containing
    ///   `param_oids`, `rows: DescribedRows`, and `tx_status`.
    ///
    /// Error flow: `ErrorResponse` (e.g. invalid / unknown statement
    /// name) → `FailReply(ServerErrorResponse)` → `ReadyForQuery` →
    /// state back to `Idle`. Connection survives — query-level
    /// error pattern (same as SimpleQuery/Parse per PG §55.2.3).
    ///
    /// # Why split from [`Self::DescribePortal`] (tier-1 API shape)
    ///
    /// PG §55.2.2 specifies different response shapes per target:
    /// statement-describe emits `ParameterDescription`, portal-describe
    /// does not. Two separate command variants with two separate
    /// [`crate::ReplyKind`] markers give the caller a payload type
    /// that literally cannot carry the wrong shape — no
    /// `Option<ParamOids>` runtime ambiguity, no chance of receiving
    /// a `DescribePortalComplete` when you asked for a statement.
    /// DEF-112 kind-parameterisation binds the payload at the
    /// `Action::DeliverReply` construction site.
    ///
    /// # Sync-bundling
    ///
    /// Emits TWO outbound wire frames in one push: a `Describe`
    /// frame followed by a `Sync`. Without the Sync, PG buffers
    /// Extended Query responses indefinitely. Pipelining
    /// (`Parse + Describe + Sync`, `Bind + Describe + Execute + Sync`)
    /// lands in 1c-5 behind the witness-guard API.
    DescribeStatement {
        /// Prepared-statement name. Empty (the "unnamed statement"
        /// per PG) or a validated `StmtName` up to
        /// [`crate::ident::MAX_PG_NAME_LEN`] bytes.
        stmt_name: StmtName,
        /// Correlator for the reply.
        ///
        /// DEF-112: typed `ReplyId<DescribeStatementKind>` binds
        /// the payload to
        /// [`crate::action::DescribeStatementCompletePayload`] at
        /// compile time.
        reply: ReplyId<DescribeStatementKind>,
    },

    /// Inspect a previously-bound portal via PG's Extended Query
    /// `Describe` + `Sync` bundle (PG §55.2.2). 1c-3c.
    ///
    /// **Precondition:** protocol must be in [`crate::ProtoState::Idle`].
    ///
    /// # Response sequence — portal target
    ///
    /// Success flow:
    /// - `RowDescription` (`'T'`) **or** `NoData` (`'n'`) — same
    ///   rules as statement-describe. **No** `ParameterDescription`
    ///   precedes: portals are bound-state handles, parameters were
    ///   fixed at Bind time.
    /// - `ReadyForQuery` (`'Z'`) — delivers
    ///   [`crate::Reply::DescribePortalComplete`] containing `rows`
    ///   and `tx_status`.
    ///
    /// Error flow: identical to [`Self::DescribeStatement`] — query-
    /// level recoverable error, connection survives.
    DescribePortal {
        /// Portal name. Empty (the "unnamed portal" per PG) or a
        /// validated `PortalName` up to
        /// [`crate::ident::MAX_PG_NAME_LEN`] bytes.
        portal_name: PortalName,
        /// Correlator for the reply.
        ///
        /// DEF-112: typed `ReplyId<DescribePortalKind>` binds the
        /// payload to
        /// [`crate::action::DescribePortalCompletePayload`] at
        /// compile time.
        reply: ReplyId<DescribePortalKind>,
    },
}

/// How many rows a bound portal should produce before pausing.
///
/// Parameter of [`crate::PgProtocol::push_bind_execute`]. Encoded on
/// the wire in the PG `Execute` frame as a 4-byte `i32` — but the
/// type system narrows the user-facing API to the variants this
/// sub-phase supports.
///
/// # F83 (pass #6 audit, 2026-04-21)
///
/// Pre-F83 `push_bind_execute` took `max_rows: u32`. User supplying
/// any non-zero value caused the server to emit `PortalSuspended`
/// which the dispatcher classified as `UnexpectedFrame` → connection
/// teardown. Tier-3 runtime trap documented only in the method's
/// docstring; the compiler gave users no signal.
///
/// F83 replaces `u32` with this enum. In 1c-3b scope, only
/// [`Self::All`] exists — a user passing anything else is a build
/// error. The `#[non_exhaustive]` leaves room for [`Self::Chunked`]
/// in 1c-6 when the full chunked-fetch protocol flow lands (proper
/// `PortalSuspended` handling with subsequent `Execute` calls to
/// resume). When that ships, users transition from `All` to
/// `Chunked(NonZeroU32)` and the variant-level dispatch threads the
/// correct response shape.
///
/// # Tier uplift
///
/// `max_rows: u32` → tier-3 docs ("must be zero").
/// `FetchRows::All` → tier-1 compile ("only variant exists").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FetchRows {
    /// Fetch all rows the portal produces (no `PortalSuspended`).
    /// Maps to the wire value `max_rows = 0`.
    All,
    // Future (1c-6):
    //   /// Cap the per-Execute row count. Server emits
    //   /// `PortalSuspended` after the limit; resume with a
    //   /// subsequent Execute.
    //   Chunked(core::num::NonZeroU32),
}

impl FetchRows {
    /// Wire-encoding — the `i32` value PG expects in the Execute
    /// frame's `max_rows` field.
    #[inline]
    #[must_use]
    pub(crate) const fn as_wire_i32(self) -> i32 {
        match self {
            Self::All => 0,
        }
    }
}

// Compile-time drift-pin: `FetchRows::All` MUST map to wire value 0
// (PG §55.2.2 — `Execute` frame with `max_rows = 0` means fetch all
// rows without PortalSuspended). An arm-body edit in `as_wire_i32`
// that silently returned `1` (or any non-zero) would cause the
// server to emit PortalSuspended which the dispatcher classifies as
// UnexpectedFrame → connection teardown. Pin the literal at build.
const _: () = assert!(
    FetchRows::All.as_wire_i32() == 0,
    "FetchRows::All MUST wire-encode as 0 per PG §55.2.2",
);
