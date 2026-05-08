//! DEF-269 v2 (T) — type-level command dispatch.
//!
//! Replaces the runtime `PgCommand` enum (~2176 B sized to its largest
//! variant `Parse`) with per-command structs that each carry only their
//! own size. Each struct implements the sealed [`PushCommand`] trait;
//! [`crate::guard::ReadyGuard::push_command`] monomorphises per type.
//!
//! # Why
//!
//! Pre-DEF-269-v2: every `proto.as_ready().unwrap().push_command(
//! PgCommand::Ping { reply }, &mut wb)` call moved 2176 B by value
//! across the stack to dispatch on a single discriminant byte. Logical
//! Ping is ~16 B. Wasted 2160 B per call. The synthetic
//! `push_command/ping` bench (fresh `PgProtocol` per iter) paid this
//! cost on every iteration.
//!
//! Post-DEF-269-v2: [`Ping`] is 16 B, [`Flush`] is 0 B, [`BindExecute`]
//! is parameterised on `P: ParamsWriter` and carries only what its
//! actual parameters need. Each push pays its own size.
//!
//! # Tier-1 invariants
//!
//! - **Push from Idle**: `IdleStateProof` witness from `mod guard`,
//!   reachable only through [`crate::guard::ReadyGuard::push_command`].
//!   Same DEF-198 closure preserved.
//! - **Reply correlator typed**: Each impl statically knows its
//!   `ReplyId<K>` parameter type — DEF-112 kind-parameterisation
//!   surfaces at the impl boundary, no runtime command-kind tagging.
//! - **Sealed**: Implementations are crate-internal only. Adding a
//!   new command is a same-commit job (struct + impl + tests + bench
//!   probe).

// Sealed-trait pattern: `PushCommand::execute` references `pub(crate)`
// types (`StagedAction`, `BrandedWriteReserved`, `IdleStateProof`).
// External crates cannot construct any of these or implement the trait,
// so the visibility "leakage" is purely cosmetic. Suppressed at module
// scope so every per-command impl inherits the allow.
#![allow(private_interfaces, private_bounds)]

use crate::command::FetchRows;
use crate::ident::{ApplicationName, DatabaseName, Ident, PortalName, Sql, StmtName};
use crate::password::Credentials;
use crate::reply_id::{
    DescribePortalKind, DescribeStatementKind, ParseKind, PingKind, QueryKind, ReplyId,
    StartupKind,
};

mod sealed {
    /// Crate-internal sealed marker — only this crate's per-command
    /// structs may implement [`super::PushCommand`].
    pub trait PushCommandSealed {}
}

/// A client→server command, monomorphised at compile time per impl.
///
/// See module-level docs for the architectural rationale and tier-1
/// invariants. This trait is sealed; external crates cannot implement
/// it.
///
/// # Output
///
/// `Output` is `()` for all current impls. Reserved for `prepared!`
/// macro Phase 2 (DEF-244) typed handles (e.g.,
/// `BoundPortalHandle<P>` for chunked-fetch pre-conditions).
/// Including this associated type now is a 0-byte cost (phantom
/// associated type) and avoids a future breaking API change when
/// the macro lands.
///
/// # Sealed-trait visibility
///
/// `execute` references `pub(crate)` types (`StagedAction`,
/// `BrandedWriteReserved`, `IdleStateProof`). Per the sealed-trait
/// pattern, external crates cannot construct these types or implement
/// the trait — visibility leakage is purely cosmetic. Suppressed
/// crate-locally.
#[allow(private_interfaces, private_bounds, reason = "sealed-trait pattern: trait method takes pub(crate) types — external crates cannot construct them or implement the trait, so the leakage is cosmetic only")]
pub trait PushCommand: sealed::PushCommandSealed {
    /// Per-command output type. `()` for fire-and-forget commands;
    /// future handles for prepared-statement / chunked-fetch flows.
    type Output;

    /// Encode the command into `staged` / `reserved` and install the
    /// post-push state transition.
    ///
    /// # Failure
    ///
    /// Build failures (extremely rare — wire-frame size const-asserted
    /// against MAX_OWNED_SEND_LEN) emit `StagedAction::FailReply` via
    /// `try_builder!` macro internally, then `materialise_push` (run
    /// by the caller) translates the staged actions into
    /// `Result<(), PushFailure>`.
    ///
    /// # Tier-1 witness
    ///
    /// `_proof: IdleStateProof` is constructible only inside
    /// `mod guard` (DEF-198). The trait method requires it, so every
    /// `execute` call must come through `ReadyGuard::push_command` —
    /// which only constructs the witness after `as_ready()` confirmed
    /// `state == Idle`.
    fn execute(
        self,
        state: &mut crate::state::ProtoState,
        row_desc_slot: &mut Option<crate::decode::RowDesc>,
        staged: &mut crate::action::StagedActions,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
        _proof: crate::guard::IdleStateProof,
    ) -> Self::Output;
}

// ═════════════════════════════════════════════════════════════════════
// Per-command structs (replaces former PgCommand enum variants).
//
// Each struct carries only its own data. Struct sizes (aarch64-darwin):
// - Ping: 16 B (just ReplyId)
// - Startup: ~360 B (StartupCredentials + Ident + Option<DatabaseName> + Option<ApplicationName> + ReplyId)
// - SimpleQuery: ~2068 B (Sql + ReplyId)
// - Parse: ~2132 B (StmtName + Sql + ReplyId) — dominant
// - DescribeStatement: ~84 B (StmtName + ReplyId)
// - DescribePortal: ~84 B (PortalName + ReplyId)
// - BindExecute<'_, P>: ~58 B + sizeof::<P>() (refs + Option<RowDesc> + FetchRows + ReplyId)
//
// Compare to former PgCommand enum: 2176 B (sized to Parse).
//
// Phase 2 (M) will add per-command `const CAP: usize` for SendList
// sizing — enabling Ping's stack frame to shrink to 5 B SendList
// instead of carrying 2176 B for unused capacity.
// ═════════════════════════════════════════════════════════════════════

/// Cheap server liveness probe.
///
/// Translated by the protocol to a `Sync` frame (5 wire bytes); the
/// matching `ReadyForQuery` arrives back as [`crate::Reply::Pong`]
/// under the supplied `reply` id.
///
/// **Precondition:** the protocol must be in [`crate::ProtoState::Idle`]
/// (enforced by [`crate::guard::ReadyGuard`]).
///
/// # Size
///
/// `size_of::<Ping>()` = 16 B (just the [`ReplyId<PingKind>`]). Pre-
/// DEF-269-v2 the same Ping push moved 2176 B by value (PgCommand
/// enum sized to Parse).
#[derive(Debug)]
#[must_use = "a Ping has no effect until passed to push_command"]
pub struct Ping {
    /// Correlator the wrapper will use to route the matching
    /// [`crate::Reply::Pong`] back to the caller.
    pub reply: ReplyId<PingKind>,
}

impl Ping {
    /// Construct a new [`Ping`] with the given reply correlator.
    #[inline]
    pub const fn new(reply: ReplyId<PingKind>) -> Self {
        Self { reply }
    }
}

impl sealed::PushCommandSealed for Ping {}

impl PushCommand for Ping {
    type Output = ();

    #[inline]
    fn execute(
        self,
        state: &mut crate::state::ProtoState,
        _row_desc_slot: &mut Option<crate::decode::RowDesc>,
        staged: &mut crate::action::StagedActions,
        _reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
        _proof: crate::guard::IdleStateProof,
    ) {
        crate::protocol::compute_push_ping_idle_only(state, self.reply, staged);
    }
}

/// Initiate the PostgreSQL startup handshake.
///
/// Builds and sends a `StartupMessage` frame. The protocol then
/// navigates the authentication exchange (trust / SCRAM / Cleartext /
/// MD5) followed by the post-auth chain (ParameterStatus,
/// BackendKeyData, ReadyForQuery) before transitioning to
/// [`crate::ProtoState::Idle`] and emitting
/// [`crate::Reply::StartupComplete`].
#[derive(Debug)]
#[must_use = "a Startup has no effect until passed to push_command"]
pub struct Startup {
    /// The PostgreSQL user to authenticate as.
    pub user: Ident,
    /// Optional database name (defaults to user name on the server).
    pub database: Option<DatabaseName>,
    /// Optional application name for `application_name` parameter.
    pub app_name: Option<ApplicationName>,
    /// Authentication credentials.
    pub credentials: Credentials,
    /// Correlator for the Startup command.
    pub reply: ReplyId<StartupKind>,
}

impl sealed::PushCommandSealed for Startup {}

impl PushCommand for Startup {
    type Output = ();

    #[inline]
    fn execute(
        self,
        state: &mut crate::state::ProtoState,
        _row_desc_slot: &mut Option<crate::decode::RowDesc>,
        staged: &mut crate::action::StagedActions,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
        _proof: crate::guard::IdleStateProof,
    ) {
        crate::protocol::compute_push_startup_idle_only(
            state,
            self.user,
            self.database,
            self.app_name,
            self.credentials,
            self.reply,
            staged,
            reserved,
        );
    }
}

/// Execute a single SQL statement via PG's Simple Query protocol
/// (`Q`-frame).
#[derive(Debug)]
#[must_use = "a SimpleQuery has no effect until passed to push_command"]
pub struct SimpleQuery {
    /// SQL text — bounded to [`crate::ident::MAX_SQL_LEN`] = 2048 bytes.
    pub sql: Sql,
    /// Correlator for the reply.
    pub reply: ReplyId<QueryKind>,
}

impl sealed::PushCommandSealed for SimpleQuery {}

impl PushCommand for SimpleQuery {
    type Output = ();

    #[inline]
    fn execute(
        self,
        state: &mut crate::state::ProtoState,
        _row_desc_slot: &mut Option<crate::decode::RowDesc>,
        staged: &mut crate::action::StagedActions,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
        _proof: crate::guard::IdleStateProof,
    ) {
        crate::protocol::compute_push_simple_query_idle_only(
            state, &self.sql, self.reply, staged, reserved,
        );
    }
}

/// Prepare a named SQL statement via PG's Extended Query protocol
/// (`P`-frame + `S`-frame terminator).
#[derive(Debug)]
#[must_use = "a Parse has no effect until passed to push_command"]
pub struct Parse {
    /// Prepared-statement name. Empty (the "unnamed statement"
    /// per PG convention) or a validated `StmtName`.
    pub stmt_name: StmtName,
    /// SQL text — bounded to [`crate::ident::MAX_SQL_LEN`].
    pub sql: Sql,
    /// Correlator for the reply.
    pub reply: ReplyId<ParseKind>,
}

impl sealed::PushCommandSealed for Parse {}

impl PushCommand for Parse {
    type Output = ();

    #[inline]
    fn execute(
        self,
        state: &mut crate::state::ProtoState,
        _row_desc_slot: &mut Option<crate::decode::RowDesc>,
        staged: &mut crate::action::StagedActions,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
        _proof: crate::guard::IdleStateProof,
    ) {
        crate::protocol::compute_push_parse_idle_only(
            state,
            &self.stmt_name,
            &self.sql,
            self.reply,
            staged,
            reserved,
        );
    }
}

/// Inspect a previously-[`Parse`]'d prepared statement via PG's
/// Extended Query `Describe` + `Sync` bundle (PG §55.2.2).
#[derive(Debug)]
#[must_use = "a DescribeStatement has no effect until passed to push_command"]
pub struct DescribeStatement {
    /// Prepared-statement name.
    pub stmt_name: StmtName,
    /// Correlator for the reply.
    pub reply: ReplyId<DescribeStatementKind>,
}

impl sealed::PushCommandSealed for DescribeStatement {}

impl PushCommand for DescribeStatement {
    type Output = ();

    #[inline]
    fn execute(
        self,
        state: &mut crate::state::ProtoState,
        _row_desc_slot: &mut Option<crate::decode::RowDesc>,
        staged: &mut crate::action::StagedActions,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
        _proof: crate::guard::IdleStateProof,
    ) {
        crate::protocol::compute_push_describe_statement_idle_only(
            state,
            &self.stmt_name,
            self.reply,
            staged,
            reserved,
        );
    }
}

/// Inspect a previously-bound portal via PG's Extended Query
/// `Describe` + `Sync` bundle.
#[derive(Debug)]
#[must_use = "a DescribePortal has no effect until passed to push_command"]
pub struct DescribePortal {
    /// Portal name.
    pub portal_name: PortalName,
    /// Correlator for the reply.
    pub reply: ReplyId<DescribePortalKind>,
}

impl sealed::PushCommandSealed for DescribePortal {}

impl PushCommand for DescribePortal {
    type Output = ();

    #[inline]
    fn execute(
        self,
        state: &mut crate::state::ProtoState,
        _row_desc_slot: &mut Option<crate::decode::RowDesc>,
        staged: &mut crate::action::StagedActions,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
        _proof: crate::guard::IdleStateProof,
    ) {
        crate::protocol::compute_push_describe_portal_idle_only(
            state,
            &self.portal_name,
            self.reply,
            staged,
            reserved,
        );
    }
}

/// Extended-Query Bind+Execute pipeline.
///
/// Pipelines three frames in one push: Bind + Execute + Sync. Replaces
/// the pre-DEF-269-v2 `push_bind_execute` separate method on
/// [`crate::guard::ReadyGuard`].
///
/// # Generic over `P: ParamsWriter`
///
/// The parameters tuple type is parameterised — pre-DEF-269-v2 the
/// `PgCommand` enum could not carry this generic, so `push_bind_execute`
/// lived as a separate method. Post-DEF-269-v2, `BindExecute` is just
/// another [`PushCommand`] impl with its own type parameter.
#[derive(Debug)]
#[must_use = "a BindExecute has no effect until passed to push_command"]
pub struct BindExecute<'a, P: crate::params::ParamsWriter> {
    /// The name the server will use to address the bound portal.
    pub portal_name: &'a PortalName,
    /// Name of a previously-pushed [`Parse`] statement.
    pub stmt_name: &'a StmtName,
    /// Tuple of parameters implementing [`crate::params::ParamsWriter`].
    pub params: &'a P,
    /// Pre-provided result-set schema. `Some(desc)` for SELECT,
    /// `None` for DML / RETURNING-less statements.
    pub row_desc: Option<crate::decode::RowDesc>,
    /// Row-count scope.
    pub fetch: FetchRows,
    /// Typed correlator for the reply.
    pub reply: ReplyId<QueryKind>,
}

impl<P: crate::params::ParamsWriter> sealed::PushCommandSealed for BindExecute<'_, P> {}

impl<P: crate::params::ParamsWriter> PushCommand for BindExecute<'_, P> {
    type Output = ();

    #[inline]
    fn execute(
        self,
        state: &mut crate::state::ProtoState,
        row_desc_slot: &mut Option<crate::decode::RowDesc>,
        staged: &mut crate::action::StagedActions,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
        _proof: crate::guard::IdleStateProof,
    ) {
        crate::protocol::compute_push_bind_execute_idle_only(
            state,
            row_desc_slot,
            self.portal_name,
            self.stmt_name,
            self.params,
            self.row_desc,
            self.fetch,
            self.reply,
            staged,
            reserved,
        );
    }
}

// ═════════════════════════════════════════════════════════════════════
// Backwards-compat blanket impl for legacy `PgCommand` enum callers.
//
// During the T migration window, existing test/bench code still passes
// `PgCommand::Ping { reply }` etc. The blanket impl dispatches via the
// existing `compute_push_idle_only` runtime match. **Slow path** —
// pays the 2176-B PgCommand argument move on every push (the cost
// T was designed to eliminate). New callers should construct per-
// command structs directly to opt into the fast path.
//
// Removed in a follow-up commit (full PgCommand deletion) once all
// internal callers migrate to per-command structs.
// ═════════════════════════════════════════════════════════════════════

impl sealed::PushCommandSealed for crate::command::PgCommand {}

impl PushCommand for crate::command::PgCommand {
    type Output = ();

    #[inline]
    fn execute(
        self,
        state: &mut crate::state::ProtoState,
        _row_desc_slot: &mut Option<crate::decode::RowDesc>,
        staged: &mut crate::action::StagedActions,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
        _proof: crate::guard::IdleStateProof,
    ) {
        // Slow path: PgCommand enum dispatch via runtime match.
        // Each variant moves 2176 B by value into this method (sized
        // to Parse). Per-command structs bypass via direct dispatch.
        crate::protocol::compute_push_idle_only(self, state, staged, reserved);
    }
}

// ═════════════════════════════════════════════════════════════════════
// Size pins — DEF-269 v2 (T) drift guards
// ═════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod size_pins {
    use super::*;

    /// Tier-1 drift guard: `Ping` is the smallest command. Pin its
    /// size at the ReplyId baseline (16 B on aarch64-darwin); a
    /// regression that grew Ping by adding a field would surface here.
    #[test]
    fn ping_is_minimal() {
        assert_eq!(
            core::mem::size_of::<Ping>(),
            core::mem::size_of::<ReplyId<PingKind>>(),
            "Ping must carry only ReplyId — no padding/extra fields",
        );
    }

    /// Tier-1 drift guard: per-command structs collectively replace
    /// the former `PgCommand` enum. Pin that the **largest** non-
    /// generic command (Parse) is comparable to the former enum's
    /// dominant variant — but the others are dramatically smaller.
    /// This is the structural witness that T's value proposition is
    /// realised.
    #[test]
    fn ping_dramatically_smaller_than_parse() {
        let ping_size = core::mem::size_of::<Ping>();
        let parse_size = core::mem::size_of::<Parse>();
        // Parse is the dominant Pg-pushable command (~2132 B).
        // Ping is 16 B. The ratio must be at least 100×.
        assert!(
            parse_size >= ping_size.saturating_mul(100),
            "T's value proposition: Ping ({ping_size} B) << Parse ({parse_size} B). \
             If this ratio shrinks, the synthetic-init bench gain disappears.",
        );
    }
}
