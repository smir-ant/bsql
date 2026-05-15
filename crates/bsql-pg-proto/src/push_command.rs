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
//! - **Push from Idle**: enforced at the
//!   [`crate::PgProtocol::push_command_internal`] entry via the
//!   [`crate::state_setter::IdleState::try_from`] lifetime-bound
//!   typestate (DEF-272 cluster γ; supersedes the pre-γ `IdleStateProof`
//!   ZST witness). The typestate IS the `&mut state` borrow + the Idle
//!   proof, inseparable; pairing-with-different-state is impossible by
//!   lifetime ownership. ReadyGuard's `as_ready` runtime check is the
//!   upstream classifier; the typestate's `try_from` is belt-and-braces
//!   tier-1 enforcement.
//! - **Reply correlator typed**: Each impl statically knows its
//!   `ReplyId<K>` parameter type — DEF-112 kind-parameterisation
//!   surfaces at the impl boundary, no runtime command-kind tagging.
//! - **Sealed**: Implementations are crate-internal only. Adding a
//!   new command is a same-commit job (struct + impl + tests + bench
//!   probe).

// Sealed-trait pattern: `PushCommand::execute` references `pub(crate)`
// types (`StagedAction`, `BrandedWriteReserved`, `StateSetter<'_, W>`,
// `RowDescSlotCell`). External crates cannot construct any of these
// or implement the trait, so the visibility "leakage" is purely
// cosmetic. Suppressed at module scope so every per-command impl
// inherits the allow.
#![allow(private_interfaces, private_bounds)]

use crate::command::FetchRows;
use crate::ident::{ApplicationName, DatabaseName, Ident, PortalName, StmtName};
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
/// `BrandedWriteReserved`, `StateSetter<'_, W>`, `RowDescSlotCell`).
/// Per the sealed-trait pattern, external crates cannot construct
/// these types or implement the trait — visibility leakage is purely
/// cosmetic. Suppressed crate-locally.
#[allow(private_interfaces, private_bounds, reason = "sealed-trait pattern: trait method takes pub(crate) types — external crates cannot construct them or implement the trait, so the leakage is cosmetic only")]
pub trait PushCommand: sealed::PushCommandSealed {
    /// Per-command output type. `()` for fire-and-forget commands;
    /// future handles for prepared-statement / chunked-fetch flows.
    type Output;

    /// Per-command post-push state install witness.
    ///
    /// DEF-270 N-D (Phase 2): tier-1 by-construction pairing of
    /// command struct ↔ matching [`crate::state::ProtoState`] variant.
    /// Each impl pairs its command (e.g. [`Ping`]) to a single
    /// witness type (e.g. [`PingAwaitingRfqInstall`]) which carries
    /// exactly the data the variant requires. The
    /// [`crate::state_setter::StateSetter`] takes a
    /// `Self::PostState` proof at consumption time — there is no
    /// path to install a non-matching state variant from `execute()`.
    ///
    /// Pre-DEF-270-N-D `execute()` received `state: &mut ProtoState`
    /// and could write any variant; tier-3 by-discipline relied on
    /// reviewer attention + the `compute_push_tests` per-helper
    /// transition table.
    type PostState: crate::state_setter::PostStateProof;

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
    /// # Tier-1 witness — Idle state
    ///
    /// The `setter: StateSetter<'_, Self::PostState>` parameter
    /// inherits its `&mut state` borrow from the
    /// [`crate::state_setter::IdleState::try_from`] typestate
    /// constructed inside `push_command_internal` (DEF-272 cluster γ).
    /// The typestate IS the proof + the borrow; reaching `execute()`
    /// implies the runtime Idle classification succeeded. ReadyGuard's
    /// `as_ready` is the upstream classifier; the typestate is
    /// belt-and-braces enforcement at the boundary.
    ///
    /// # Tier-1 witness — post-state install
    ///
    /// DEF-270 N-D (Phase 2): `setter` is the **only** path to mutate
    /// [`crate::state::ProtoState`] from inside `execute()`. The raw
    /// `&mut ProtoState` lives privately inside
    /// [`crate::PgProtocol::push_command_internal`], never handed to
    /// the impl. The setter is consumed (linear) by exactly one of:
    /// - [`crate::state_setter::StateSetter::install_post_state`]
    ///   (happy path, takes `Self::PostState` witness)
    /// - [`crate::state_setter::StateSetter::install_errored`]
    ///   (failure path via `try_builder!` macro, takes
    ///   `StateErrorKind`)
    ///
    /// Failing to consume the setter triggers an unused-`#[must_use]`
    /// build warning at the impl site.
    /// DEF-160 Z2 (2026-05-11): `'sql` is the lifetime carried by
    /// `Self` for impls that borrow caller-owned bytes (currently
    /// [`Parse<'a>`] / [`SimpleQuery<'a>`] for the SQL string). The
    /// `where Self: 'sql` bound tells the borrow checker that `Self`
    /// outlives `'sql`, so an impl with `Self = Parse<'a>` can stage
    /// `&'a [u8]` into `StagedActions<'sql>` (covariance via the
    /// `'a >= 'sql` subtyping induced by the bound). Impls with no
    /// borrowed surface (e.g., [`Ping`]) ignore `'sql` — the bound
    /// is trivially satisfied by `Self: 'static`.
    fn execute<'sql>(
        self,
        setter: crate::state_setter::StateSetter<'_, Self::PostState>,
        row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
        staged: &mut crate::action::StagedActions<'sql>,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    ) -> Self::Output
    where
        Self: 'sql;
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
    type PostState = PingAwaitingRfqInstall;

    #[inline]
    fn execute<'sql>(
        self,
        setter: crate::state_setter::StateSetter<'_, Self::PostState>,
        _row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
        staged: &mut crate::action::StagedActions<'sql>,
        _reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    )
    where
        Self: 'sql,
    {
        crate::protocol::compute_push_ping_idle_only(setter, self.reply, staged);
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
    type PostState = StartupPostInstall;

    #[inline]
    fn execute<'sql>(
        self,
        setter: crate::state_setter::StateSetter<'_, Self::PostState>,
        _row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
        staged: &mut crate::action::StagedActions<'sql>,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    )
    where
        Self: 'sql,
    {
        crate::protocol::compute_push_startup_idle_only(
            setter,
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
///
/// DEF-160 Z2 (2026-05-11): `sql` is `&'a str` (was owned `Sql =
/// FixedStr<MAX_SQL_LEN, _>`). The bytes are streamed zero-copy via
/// [`StagedAction::SendBytesBorrowed`](crate::action::StagedAction)
/// — no protocol cap on SQL size, no truncation arena. Caller owns
/// the string allocation; for SQL containing secrets, hold it in
/// `Zeroizing<String>` (zeroize-on-drop happens at the caller, not
/// in our `WriteBuf::clear()`).
#[derive(Debug)]
#[must_use = "a SimpleQuery has no effect until passed to push_command"]
pub struct SimpleQuery<'a> {
    /// SQL text — borrowed; any length, zero-copy on the wire.
    pub sql: &'a str,
    /// Correlator for the reply.
    pub reply: ReplyId<QueryKind>,
}

impl sealed::PushCommandSealed for SimpleQuery<'_> {}

impl<'a> PushCommand for SimpleQuery<'a> {
    type Output = ();
    type PostState = SimpleQueryAwaitingFirstResponseInstall;

    #[inline]
    fn execute<'sql>(
        self,
        setter: crate::state_setter::StateSetter<'_, Self::PostState>,
        _row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
        staged: &mut crate::action::StagedActions<'sql>,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    )
    where
        Self: 'sql,
    {
        // Self = SimpleQuery<'a>; Self: 'sql means 'a >= 'sql, so
        // self.sql.as_bytes(): &'a [u8] coerces to &'sql [u8] safely.
        crate::protocol::compute_push_simple_query_idle_only(
            setter, self.sql.as_bytes(), self.reply, staged, reserved,
        );
    }
}

/// Prepare a named SQL statement via PG's Extended Query protocol
/// (`P`-frame + `S`-frame terminator).
///
/// DEF-160 Z2 (2026-05-11): `sql` is `&'a str` (was owned `Sql =
/// FixedStr<MAX_SQL_LEN, _>`). See [`SimpleQuery`] for the rationale
/// and zeroize-handoff contract.
#[derive(Debug)]
#[must_use = "a Parse has no effect until passed to push_command"]
pub struct Parse<'a> {
    /// Prepared-statement name. Empty (the "unnamed statement"
    /// per PG convention) or a validated `StmtName`.
    pub stmt_name: StmtName,
    /// SQL text — borrowed; any length, zero-copy on the wire.
    pub sql: &'a str,
    /// Correlator for the reply.
    pub reply: ReplyId<ParseKind>,
}

impl sealed::PushCommandSealed for Parse<'_> {}

impl<'a> PushCommand for Parse<'a> {
    type Output = ();
    type PostState = ParseAwaitingParseCompleteInstall;

    #[inline]
    fn execute<'sql>(
        self,
        setter: crate::state_setter::StateSetter<'_, Self::PostState>,
        _row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
        staged: &mut crate::action::StagedActions<'sql>,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    )
    where
        Self: 'sql,
    {
        // Self = Parse<'a>; Self: 'sql means 'a >= 'sql, so
        // self.sql.as_bytes(): &'a [u8] coerces to &'sql [u8] safely.
        crate::protocol::compute_push_parse_idle_only(
            setter,
            &self.stmt_name,
            self.sql.as_bytes(),
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
    type PostState = DescribeStatementAwaitingParamDescInstall;

    #[inline]
    fn execute<'sql>(
        self,
        setter: crate::state_setter::StateSetter<'_, Self::PostState>,
        _row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
        staged: &mut crate::action::StagedActions<'sql>,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    )
    where
        Self: 'sql,
    {
        crate::protocol::compute_push_describe_statement_idle_only(
            setter,
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
    type PostState = DescribePortalAwaitingRowDescOrNoDataInstall;

    #[inline]
    fn execute<'sql>(
        self,
        setter: crate::state_setter::StateSetter<'_, Self::PostState>,
        _row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
        staged: &mut crate::action::StagedActions<'sql>,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    )
    where
        Self: 'sql,
    {
        crate::protocol::compute_push_describe_portal_idle_only(
            setter,
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
    type PostState = BindExecutePostInstall;

    #[inline]
    fn execute<'sql>(
        self,
        setter: crate::state_setter::StateSetter<'_, Self::PostState>,
        row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
        staged: &mut crate::action::StagedActions<'sql>,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    )
    where
        Self: 'sql,
    {
        crate::protocol::compute_push_bind_execute_idle_only(
            setter,
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
// DEF-270 Phase 2 (N-D, 2026-05-10) — per-command post-state install
// witnesses.
//
// Each per-command struct above pairs (via `type PostState`) with one
// witness type below. Witnesses carry exactly the data the matching
// `ProtoState` variant requires; the trait's
// `setter.install_post_state(witness)` is the only mutation path.
// **Tier-1 by-construction**: a future refactor that paired the wrong
// post-state to a command would surface as a type mismatch at the
// `type PostState = ...` declaration in the impl block (the only
// constructor in scope at the impl's `execute` body for the
// post-state install is the matching witness type).
//
// # Pre-DEF-270-N-D PgCommand backwards-compat removed
//
// The transitional `impl PushCommand for crate::command::PgCommand`
// blanket impl + `compute_push_idle_only` slow-path dispatcher were
// deleted at this commit. Real call sites: zero (DEF-270 Phase 2
// audit 2026-05-10 grep — only doc comments referenced
// `push_command(PgCommand::...)`). The `PgCommand` enum itself
// survives for the `compute_push_tests` test-only 5-arm dispatchers;
// no production code path constructs it.
// ═════════════════════════════════════════════════════════════════════

use crate::state_setter::PostStateProof;
use crate::state_setter::sealed::Sealed as PostStateSealed;

/// Witness pairing [`Ping`] to
/// [`crate::state::ProtoState::PingAwaitingRfq`]. Carries exactly the
/// `ReplyId<PingKind>` the variant requires.
#[must_use = "a PingAwaitingRfqInstall has no effect until passed to StateSetter::install_post_state"]
#[allow(missing_debug_implementations, reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait")]
pub struct PingAwaitingRfqInstall {
    pub(crate) reply: ReplyId<PingKind>,
}
impl PostStateSealed for PingAwaitingRfqInstall {}
impl PostStateProof for PingAwaitingRfqInstall {
    #[inline]
    fn install_into(self, state: &mut crate::state::ProtoState) {
        *state = crate::state::ProtoState::PingAwaitingRfq(self.reply);
    }
}

/// Witness pairing [`Startup`] to one of four post-startup variants
/// (Trust / SCRAM / Cleartext / MD5). The split surfaces the
/// per-credential-type post-state pairing structurally — adding a
/// new credential variant fails the build until a matching enum
/// variant lands here AND `install_into` matches it.
///
/// Variant ordering mirrors [`crate::password::Credentials`] for
/// reviewability; the enum-tag is independent at the wire level.
#[must_use = "a StartupPostInstall has no effect until passed to StateSetter::install_post_state"]
#[allow(missing_debug_implementations, reason = "fields contain secret material (Box<ScramSession>, Box<Sensitive<Password>>, Box<Md5HandshakeState>); ZST witness flows by-value through one consumption path; Debug impl would require redacting the secrets — defer until a concrete diagnostic surface needs the trait")]
pub enum StartupPostInstall {
    /// Trust (no auth) → [`crate::state::ProtoState::ConnectingStartupTrust`].
    Trust {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
    },
    /// SCRAM-SHA-256 → [`crate::state::ProtoState::ConnectingStartupScram`].
    Scram {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// SCRAM session state, heap-boxed (mirror of state-variant
        /// pattern; ZeroizeOnDrop fires through Box's Drop).
        scram: alloc::boxed::Box<crate::scram::session::ScramSession>,
    },
    /// Cleartext password → [`crate::state::ProtoState::ConnectingStartupCleartext`].
    Cleartext {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// Password material, heap-boxed; ZeroizeOnDrop fires
        /// through Box's Drop on every transition exit path.
        password: alloc::boxed::Box<crate::sensitive::Sensitive<crate::password::Password>>,
    },
    /// MD5 password → [`crate::state::ProtoState::ConnectingStartupMd5`].
    Md5 {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// Bundled handshake state (password + username), heap-boxed
        /// in a single allocation. Mirrors SCRAM PERF-02 single-Box
        /// pattern.
        handshake: alloc::boxed::Box<crate::md5::Md5HandshakeState>,
    },
}
impl PostStateSealed for StartupPostInstall {}
impl PostStateProof for StartupPostInstall {
    #[inline]
    fn install_into(self, state: &mut crate::state::ProtoState) {
        *state = match self {
            Self::Trust { reply } => crate::state::ProtoState::ConnectingStartupTrust { reply },
            Self::Scram { reply, scram } => {
                crate::state::ProtoState::ConnectingStartupScram { reply, scram }
            }
            Self::Cleartext { reply, password } => {
                crate::state::ProtoState::ConnectingStartupCleartext { reply, password }
            }
            Self::Md5 { reply, handshake } => {
                crate::state::ProtoState::ConnectingStartupMd5 { reply, handshake }
            }
        };
    }
}

/// Witness pairing [`SimpleQuery`] to
/// [`crate::state::ProtoState::SimpleQueryAwaitingFirstResponse`].
#[must_use = "a SimpleQueryAwaitingFirstResponseInstall has no effect until passed to StateSetter::install_post_state"]
#[allow(missing_debug_implementations, reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait")]
pub struct SimpleQueryAwaitingFirstResponseInstall {
    pub(crate) reply: ReplyId<QueryKind>,
}
impl PostStateSealed for SimpleQueryAwaitingFirstResponseInstall {}
impl PostStateProof for SimpleQueryAwaitingFirstResponseInstall {
    #[inline]
    fn install_into(self, state: &mut crate::state::ProtoState) {
        *state = crate::state::ProtoState::SimpleQueryAwaitingFirstResponse(self.reply);
    }
}

/// Witness pairing [`Parse`] to
/// [`crate::state::ProtoState::ParseAwaitingParseComplete`].
#[must_use = "a ParseAwaitingParseCompleteInstall has no effect until passed to StateSetter::install_post_state"]
#[allow(missing_debug_implementations, reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait")]
pub struct ParseAwaitingParseCompleteInstall {
    pub(crate) reply: ReplyId<ParseKind>,
}
impl PostStateSealed for ParseAwaitingParseCompleteInstall {}
impl PostStateProof for ParseAwaitingParseCompleteInstall {
    #[inline]
    fn install_into(self, state: &mut crate::state::ProtoState) {
        *state = crate::state::ProtoState::ParseAwaitingParseComplete(self.reply);
    }
}

/// Witness pairing [`DescribeStatement`] to
/// [`crate::state::ProtoState::DescribeStatementAwaitingParamDesc`].
#[must_use = "a DescribeStatementAwaitingParamDescInstall has no effect until passed to StateSetter::install_post_state"]
#[allow(missing_debug_implementations, reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait")]
pub struct DescribeStatementAwaitingParamDescInstall {
    pub(crate) reply: ReplyId<DescribeStatementKind>,
}
impl PostStateSealed for DescribeStatementAwaitingParamDescInstall {}
impl PostStateProof for DescribeStatementAwaitingParamDescInstall {
    #[inline]
    fn install_into(self, state: &mut crate::state::ProtoState) {
        *state = crate::state::ProtoState::DescribeStatementAwaitingParamDesc(self.reply);
    }
}

/// Witness pairing [`DescribePortal`] to
/// [`crate::state::ProtoState::DescribePortalAwaitingRowDescOrNoData`].
#[must_use = "a DescribePortalAwaitingRowDescOrNoDataInstall has no effect until passed to StateSetter::install_post_state"]
#[allow(missing_debug_implementations, reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait")]
pub struct DescribePortalAwaitingRowDescOrNoDataInstall {
    pub(crate) reply: ReplyId<DescribePortalKind>,
}
impl PostStateSealed for DescribePortalAwaitingRowDescOrNoDataInstall {}
impl PostStateProof for DescribePortalAwaitingRowDescOrNoDataInstall {
    #[inline]
    fn install_into(self, state: &mut crate::state::ProtoState) {
        *state = crate::state::ProtoState::DescribePortalAwaitingRowDescOrNoData(self.reply);
    }
}

/// Witness pairing [`BindExecute<P>`] to one of two post-bind+execute
/// variants (DML / SELECT). The split surfaces the schema-bearing
/// vs schema-less path structurally — schema parking via
/// [`crate::schema_slot::RowDescSlotCell::park_at_be_select`] happens
/// BEFORE the install, inside `compute_push_bind_execute_idle_only`
/// (gated by the leaf-private `BeSelectToken` per DEF-272 cluster α);
/// this witness only captures the variant choice + reply correlator.
///
/// **Note (Phase 2 scope):** the witness does NOT carry the
/// `RowDesc` payload itself. The Phase 2 plan (`deferred.md` DEF-270)
/// noted folding row_desc into the SELECT variant as one closure
/// path; the chosen design keeps row_desc parking in
/// `RowDescSlotCell` (tier-1 within-crate by-construction post-DEF-272
/// cluster α) and only narrows the state-install pairing here.
/// Rationale: avoiding GAT-driven `Aux` machinery on the trait keeps
/// the surface clean; per-leaf concrete-token mints close the
/// pre-DEF-272 sealed-trait bypass surface.
#[must_use = "a BindExecutePostInstall has no effect until passed to StateSetter::install_post_state"]
#[allow(missing_debug_implementations, reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait")]
pub enum BindExecutePostInstall {
    /// Schema-less path → [`crate::state::ProtoState::BindExecuteAwaitingBindCompleteDml`].
    Dml {
        /// Correlator for the in-flight command.
        reply: ReplyId<QueryKind>,
    },
    /// Schema-bearing path → [`crate::state::ProtoState::BindExecuteAwaitingBindCompleteSelect`].
    /// `RowDesc` already parked in `PgProtocol::row_desc_slot` via
    /// [`crate::schema_slot::RowDescSlotCell::park_at_be_select`]
    /// (gated by the `BeSelectToken` leaf-private mint) before install.
    Select {
        /// Correlator for the in-flight command.
        reply: ReplyId<QueryKind>,
    },
}
impl PostStateSealed for BindExecutePostInstall {}
impl PostStateProof for BindExecutePostInstall {
    #[inline]
    fn install_into(self, state: &mut crate::state::ProtoState) {
        *state = match self {
            Self::Dml { reply } => crate::state::ProtoState::BindExecuteAwaitingBindCompleteDml(reply),
            Self::Select { reply } => {
                crate::state::ProtoState::BindExecuteAwaitingBindCompleteSelect { reply }
            }
        };
    }
}

// ═════════════════════════════════════════════════════════════════════
// DEF-244 (2026-05-13) — BindPrepared<'q, P, R> wraps a PreparedQuery
// + its argument tuple into a PushCommand impl. Caller-facing entry
// is `ReadyGuard::execute_prepared` (see `guard.rs`).
// ═════════════════════════════════════════════════════════════════════

/// Pair a [`crate::prepared::PreparedQuery`] with its argument tuple
/// for a single client→server execute cycle.
///
/// Implements [`PushCommand`]; dispatched via the existing
/// [`ReadyGuard::push_command`](crate::guard::ReadyGuard::push_command)
/// path so the Idle precondition (DEF-198) + post-state typed
/// witness (DEF-270 N-D) closures apply unchanged. The execute
/// helper on `ReadyGuard` is the ergonomic surface; this struct is
/// the underlying mechanism.
///
/// # Lifetimes
///
/// `'q` — the borrow of the `PreparedQuery`. In practice `'q ==
/// 'static` because the macro emits a `const`, but the generic
/// lifetime keeps the struct composable with future stmt-cache
/// flows that hold the query non-static.
///
/// # Fields
///
/// - `q`: borrowed prepared query (the macro's `const` artefact).
/// - `args`: tuple of parameter values, owned by value for the
///   move into `write_params`.
/// - `fetch`: row-count scope. v1 only supports `FetchRows::All`
///   (memo §5.3 + the existing closed-set enum from
///   `crate::command`).
/// - `reply`: typed correlator the wrapper routes the matching
///   reply through.
///
/// # Size pin (memo §10.5)
///
/// `size_of::<BindPrepared<'_, (i32,), (i32, &str)>>() <= 144`
/// (reference 16 B + tuple 16 B + FetchRows ≤ 16 B + reply 16 B +
/// padding). Pinned in `lib.rs`.
#[derive(Debug)]
#[must_use = "a BindPrepared has no effect until passed to push_command"]
pub struct BindPrepared<'q, P, R>
where
    P: crate::params::ParamsWriter,
    R: crate::prepared::RowDecode,
{
    /// Borrowed prepared query — typically `'q = 'static` since the
    /// macro emits a `const`.
    pub q: &'q crate::prepared::PreparedQuery<P, R>,
    /// Tuple of parameter values.
    pub args: P,
    /// Row-fetch scope. v1: `FetchRows::All`.
    pub fetch: crate::command::FetchRows,
    /// Typed correlator the wrapper routes the matching reply through.
    pub reply: ReplyId<QueryKind>,
}

impl<'q, P, R> sealed::PushCommandSealed for BindPrepared<'q, P, R>
where
    P: crate::params::ParamsWriter,
    R: crate::prepared::RowDecode,
{
}

impl<'q, P, R> PushCommand for BindPrepared<'q, P, R>
where
    P: crate::params::ParamsWriter,
    R: crate::prepared::RowDecode,
{
    type Output = ();
    /// Reuses `BindExecutePostInstall` (DEF-270 N-D). Prepared
    /// queries with result rows (`row_oids.is_empty() == false`)
    /// route through the SELECT branch with a synthetic RowDesc
    /// parked at push time; row-less queries route through DML.
    type PostState = BindExecutePostInstall;

    #[inline]
    fn execute<'sql>(
        self,
        setter: crate::state_setter::StateSetter<'_, Self::PostState>,
        row_desc_slot: &mut crate::schema_slot::RowDescSlotCell,
        staged: &mut crate::action::StagedActions<'sql>,
        reserved: &mut crate::write_buf::BrandedWriteReserved<'_>,
    ) where
        Self: 'sql,
    {
        crate::protocol::compute_push_bind_prepared_idle_only(
            setter,
            row_desc_slot,
            self.q,
            self.args,
            self.fetch,
            self.reply,
            staged,
            reserved,
        );
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

    /// Tier-1 drift guard: post-DEF-160 (Z2) Parse is small —
    /// `&'a str` SQL replaces the pre-Z2 owned `Sql = FixedStr<2048>`
    /// inline. Pin Parse + SimpleQuery sizes at "small" (≤ 128 B)
    /// — a regression that re-embeds owned SQL would surface here
    /// dramatically (jump from ~96 B to ~2132 B).
    ///
    /// Pre-DEF-160 the test pinned `parse_size >= ping_size * 100`
    /// (Parse 2132 ≥ Ping 8 × 100). Post-DEF-160 Parse is ~96 B and
    /// the structural value proposition shifts: small per-command
    /// structs **and** zero-copy SQL via `SendBytesBorrowed`. The
    /// per-call size win that DEF-269 v2 (T) measured (push_command
    /// /ping −83.4%) survives because PgCommand enum is gone — the
    /// 2176 B move was the enum's by-value payload, not Parse-the-
    /// struct's; T's win is preserved.
    #[test]
    fn parse_and_simple_query_carry_no_inline_sql() {
        let parse_size = core::mem::size_of::<Parse<'static>>();
        let simple_query_size = core::mem::size_of::<SimpleQuery<'static>>();
        assert!(
            parse_size <= 128,
            "Parse must be ≤ 128 B post-DEF-160 (no inline SQL); got {parse_size} B. \
             A regression to ~2132 B would mean the owned-Sql field came back.",
        );
        assert!(
            simple_query_size <= 64,
            "SimpleQuery must be ≤ 64 B post-DEF-160; got {simple_query_size} B.",
        );
    }
}
