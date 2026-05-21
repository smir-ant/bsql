//! Type-level command dispatch.
//!
//! Each client→server command is its own struct that carries only
//! its own size; each struct implements the sealed [`PushCommand`]
//! trait; [`crate::guard::ReadyGuard::push_command`] monomorphises
//! per type.
//!
//! # Why per-command structs and not a runtime enum
//!
//! A runtime `PgCommand` enum is sized to its largest variant
//! (`Parse` at ~2176 B). Every
//! `proto.as_ready().unwrap().push_command(PgCommand::Ping { reply },
//! &mut wb)` call would move 2176 B by value across the stack to
//! dispatch on a single discriminant byte. Logical Ping is ~16 B —
//! 2160 B wasted per call.
//!
//! With the per-command shape: [`Ping`] is 16 B, [`Flush`] is 0 B,
//! [`BindExecute`] is parameterised on `P: ParamsWriter` and carries
//! only what its actual parameters need. Each push pays its own size.
//!
//! # Tier-1 invariants
//!
//! - **Push from Idle**: enforced at the
//!   [`crate::PgProtocol::push_command_internal`] entry via the
//!   [`crate::state_setter::IdleState::try_from`] lifetime-bound
//!   typestate. The typestate IS the `&mut state` borrow + the Idle
//!   proof, inseparable; pairing-with-different-state is impossible
//!   by lifetime ownership. ReadyGuard's `as_ready` runtime check is
//!   the upstream classifier; the typestate's `try_from` is belt-
//!   and-braces tier-1 enforcement.
//! - **Reply correlator typed**: Each impl statically knows its
//!   `ReplyId<K>` parameter type — kind-parameterisation surfaces at
//!   the impl boundary, no runtime command-kind tagging.
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
use crate::ident::{PortalName, StmtName};
// `ApplicationName`, `DatabaseName`, `Ident`, `Credentials` are not
// imported here — the Startup command does not use the per-command
// `impl PushCommand` shape. The types live on
// `<DisconnectedPhase>::push_startup`'s signature directly
// (`use crate::ident::{ApplicationName, DatabaseName, Ident}` /
// `use crate::password::Credentials` inside `mod protocol`).
use crate::reply_id::{
    CloseKind, DescribePortalKind, DescribeStatementKind, ParseKind, PingKind, QueryKind, ReplyId,
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
/// `Output` is `()` for all current impls. Reserved for future
/// typed-handle returns (e.g., `BoundPortalHandle<P>` for chunked-
/// fetch pre-conditions). Including this associated type now is a
/// 0-byte cost (phantom associated type) and avoids a future
/// breaking API change.
///
/// # Sealed-trait visibility
///
/// `execute` references `pub(crate)` types (`StagedAction`,
/// `BrandedWriteReserved`, `StateSetter<'_, W>`, `RowDescSlotCell`).
/// Per the sealed-trait pattern, external crates cannot construct
/// these types or implement the trait — visibility leakage is purely
/// cosmetic. Suppressed crate-locally.
#[expect(private_interfaces, private_bounds, reason = "sealed-trait pattern: trait method takes pub(crate) types — external crates cannot construct them or implement the trait, so the leakage is cosmetic only. Migrated #[allow]→#[expect] (Rust 1.81): if the referenced types become `pub`, the lint no longer fires, prompting attribute removal.")]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a valid client→server command",
    label = "valid commands are the per-command structs in `push_command`: `Ping`, `Parse`, `DescribeStatement`, `DescribePortal`, `SimpleQuery`, `BindExecute`, `BindPrepared<P, R>`. For startup, see `PgProtocol::<DisconnectedPhase>::push_startup`.",
    note = "`PushCommand` is sealed — external crates cannot add command variants; extend the closed set inside `bsql-pg-proto::push_command` paired with the matching dispatcher arm and state-machine transition"
)]
pub trait PushCommand: sealed::PushCommandSealed {
    /// Per-command output type. `()` for fire-and-forget commands;
    /// future handles for prepared-statement / chunked-fetch flows.
    type Output;

    /// Per-command post-push state install witness.
    ///
    /// Tier-1 by-construction pairing of command struct ↔ matching
    /// [`crate::state::ProtoState`] variant. Each impl pairs its
    /// command (e.g. [`Ping`]) to a single witness type (e.g.
    /// [`PingAwaitingRfqInstall`]) which carries exactly the data
    /// the variant requires. The
    /// [`crate::state_setter::StateSetter`] takes a
    /// `Self::PostState` proof at consumption time — there is no
    /// path to install a non-matching state variant from `execute()`.
    /// A bare `execute(state: &mut ProtoState, …)` shape would be
    /// tier-3 by-discipline (reviewer attention + the
    /// `compute_push_tests` per-helper transition table); the
    /// witness-typed shape closes that surface.
    // Bound is `InstallBody`, not `PostStateProof` — closes the
    // declaration boundary: a future `impl PushCommand for X` with
    // `type PostState = HostileWitness` is rejected at the trait-impl
    // declaration site (E0277: HostileWitness: InstallBody not
    // satisfied), not just at the setter consumption call site.
    // `InstallBody`'s private supertrait
    // `install_body_seal::InstallBodySealed` confines impls to mod
    // state_setter, so HostileWitness cannot satisfy the bound.
    type PostState: crate::state_setter::InstallBody;

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
    /// constructed inside `push_command_internal`. The typestate IS
    /// the proof + the borrow; reaching `execute()` implies the
    /// runtime Idle classification succeeded. ReadyGuard's `as_ready`
    /// is the upstream classifier; the typestate is belt-and-braces
    /// enforcement at the boundary.
    ///
    /// # Tier-1 witness — post-state install
    ///
    /// `setter` is the **only** path to mutate
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
    ///
    /// `'sql` is the lifetime carried by `Self` for impls that borrow
    /// caller-owned bytes (currently [`Parse<'a>`] / [`SimpleQuery<'a>`]
    /// for the SQL string). The `where Self: 'sql` bound tells the
    /// borrow checker that `Self` outlives `'sql`, so an impl with
    /// `Self = Parse<'a>` can stage `&'a [u8]` into
    /// `StagedActions<'sql>` (covariance via the `'a >= 'sql`
    /// subtyping induced by the bound). Impls with no borrowed
    /// surface (e.g., [`Ping`]) ignore `'sql` — the bound is
    /// trivially satisfied by `Self: 'static`.
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
/// `size_of::<Ping>()` = 16 B (just the [`ReplyId<PingKind>`]). A
/// runtime `PgCommand` enum sized to its largest variant would move
/// 2176 B by value on every Ping push.
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

// There is no `pub struct Startup` + `impl PushCommand for Startup`.
// The handshake entry-point lives on
// `PgProtocol<DisconnectedPhase>::push_startup(...)` (consume-self
// → `<ConnectingPhase>`). Tier-1: the only legal path into a
// Connecting state is from `<DisconnectedPhase>`; pushing a Startup
// command from a Ready `<ActivePhase>` is method-absent E0599 by
// construction (the struct physically does not exist on the
// PushCommand path).

/// Execute a single SQL statement via PG's Simple Query protocol
/// (`Q`-frame).
///
/// `sql` is `&'a str` — the bytes are streamed zero-copy via
/// [`StagedAction::SendBytesBorrowed`](crate::action::StagedAction).
/// No protocol cap on SQL size, no truncation arena. Caller owns
/// the string allocation; for SQL containing secrets, hold it in
/// `Zeroizing<String>` (zeroize-on-drop happens at the caller, not
/// in `WriteBuf::clear()`).
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
/// `sql` is `&'a str`. See [`SimpleQuery`] for the rationale and
/// zeroize-handoff contract.
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

/// Close a prepared statement via PG's Extended Query `Close` + `Sync`
/// bundle (PG §55.7).
///
/// Closes the named prepared statement on the server side, freeing
/// the resources it referenced. It is NOT an error to close a name
/// that does not exist — the server emits `CloseComplete` either way.
///
/// Response shape: `'3'` (CloseComplete) → `'Z'` (ReadyForQuery).
/// Identical to [`ClosePortal`]'s response — the post-push state
/// machine treats both unified via [`crate::state::ProtoState::CloseAwaitingComplete`]
/// / [`crate::state::ProtoState::CloseAwaitingRfq`].
#[derive(Debug)]
#[must_use = "a CloseStatement has no effect until passed to push_command"]
pub struct CloseStatement {
    /// Prepared-statement name.
    pub stmt_name: StmtName,
    /// Correlator for the reply.
    pub reply: ReplyId<CloseKind>,
}

impl sealed::PushCommandSealed for CloseStatement {}

impl PushCommand for CloseStatement {
    type Output = ();
    type PostState = CloseAwaitingCompleteInstall;

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
        crate::protocol::compute_push_close_idle_only(
            setter,
            crate::wire::CloseTargetByte::Statement,
            &self.stmt_name,
            self.reply,
            staged,
            reserved,
        );
    }
}

/// Close a bound portal via PG's Extended Query `Close` + `Sync`
/// bundle (PG §55.7). Mirrors [`CloseStatement`] — the wire-level
/// target byte differs (`'P'` vs `'S'`) and the post-push state
/// variant is unified.
#[derive(Debug)]
#[must_use = "a ClosePortal has no effect until passed to push_command"]
pub struct ClosePortal {
    /// Portal name.
    pub portal_name: PortalName,
    /// Correlator for the reply.
    pub reply: ReplyId<CloseKind>,
}

impl sealed::PushCommandSealed for ClosePortal {}

impl PushCommand for ClosePortal {
    type Output = ();
    type PostState = CloseAwaitingCompleteInstall;

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
        crate::protocol::compute_push_close_idle_only(
            setter,
            crate::wire::CloseTargetByte::Portal,
            &self.portal_name,
            self.reply,
            staged,
            reserved,
        );
    }
}

/// Extended-Query Bind+Execute pipeline.
///
/// Pipelines three frames in one push: Bind + Execute + Sync.
///
/// # Generic over `P: ParamsWriter`
///
/// The parameters tuple type is parameterised on the
/// [`PushCommand`] impl. A runtime `PgCommand` enum could not carry
/// this generic — every parameterised command would have to live as
/// a separate method outside the trait.
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
// Per-command post-state install witnesses
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
// The `PgCommand` enum survives for the `compute_push_tests` test-
// only 5-arm dispatchers; no production code path constructs it.
// ═════════════════════════════════════════════════════════════════════

use crate::state_setter::PostStateProof;
use crate::state_setter::sealed::Sealed as PostStateSealed;

/// Witness pairing [`Ping`] to
/// [`crate::state::ProtoState::PingAwaitingRfq`]. Carries exactly the
/// `ReplyId<PingKind>` the variant requires.
#[must_use = "a PingAwaitingRfqInstall has no effect until passed to StateSetter::install_post_state"]
#[expect(missing_debug_implementations, reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait. Migrated #[allow]→#[expect] (Rust 1.81): if a Debug impl is later added, the lint no longer fires, prompting attribute removal.")]
pub struct PingAwaitingRfqInstall {
    pub(crate) reply: ReplyId<PingKind>,
}
impl PostStateSealed for PingAwaitingRfqInstall {}
// `impl PostStateProof` is an empty marker; the install body lives
// in `state_setter::InstallBody` impl (state_setter.rs). The trait
// split closes the within-crate hostile-witness hole — see
// state_setter.rs's `InstallBody` doc for details.
impl PostStateProof for PingAwaitingRfqInstall {}

/// Witness pairing [`Startup`] to one of four post-startup variants
/// (Trust / SCRAM / Cleartext / MD5). The split surfaces the
/// per-credential-type post-state pairing structurally — adding a
/// new credential variant fails the build until a matching enum
/// variant lands here AND `install_into` matches it.
///
/// Variant ordering mirrors [`crate::password::Credentials`] for
/// reviewability; the enum-tag is independent at the wire level.
#[must_use = "a StartupPostInstall has no effect until passed to StateSetter::install_post_state"]
#[expect(missing_debug_implementations, reason = "fields contain secret material (Box<ScramSession>, Box<Sensitive<Password>>, Box<Md5HandshakeState>); ZST witness flows by-value through one consumption path; Debug impl would require redacting the secrets — defer until a concrete diagnostic surface needs the trait. Migrated #[allow]→#[expect] (Rust 1.81): if a Debug impl is later added, the lint no longer fires, prompting attribute removal.")]
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
// Install body lives in `state_setter::InstallBody` impl.
impl PostStateProof for StartupPostInstall {}

/// Witness pairing [`SimpleQuery`] to
/// [`crate::state::ProtoState::SimpleQueryAwaitingFirstResponse`].
#[must_use = "a SimpleQueryAwaitingFirstResponseInstall has no effect until passed to StateSetter::install_post_state"]
#[expect(missing_debug_implementations, reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait. Migrated #[allow]→#[expect] (Rust 1.81): if a Debug impl is later added, the lint no longer fires, prompting attribute removal.")]
pub struct SimpleQueryAwaitingFirstResponseInstall {
    pub(crate) reply: ReplyId<QueryKind>,
}
impl PostStateSealed for SimpleQueryAwaitingFirstResponseInstall {}
// Install body lives in `state_setter::InstallBody` impl.
impl PostStateProof for SimpleQueryAwaitingFirstResponseInstall {}

/// Witness pairing [`Parse`] to
/// [`crate::state::ProtoState::ParseAwaitingParseComplete`].
#[must_use = "a ParseAwaitingParseCompleteInstall has no effect until passed to StateSetter::install_post_state"]
#[expect(missing_debug_implementations, reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait. Migrated #[allow]→#[expect] (Rust 1.81): if a Debug impl is later added, the lint no longer fires, prompting attribute removal.")]
pub struct ParseAwaitingParseCompleteInstall {
    pub(crate) reply: ReplyId<ParseKind>,
}
impl PostStateSealed for ParseAwaitingParseCompleteInstall {}
// Install body lives in `state_setter::InstallBody` impl.
impl PostStateProof for ParseAwaitingParseCompleteInstall {}

/// Witness pairing [`DescribeStatement`] to
/// [`crate::state::ProtoState::DescribeStatementAwaitingParamDesc`].
#[must_use = "a DescribeStatementAwaitingParamDescInstall has no effect until passed to StateSetter::install_post_state"]
#[expect(missing_debug_implementations, reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait. Migrated #[allow]→#[expect] (Rust 1.81): if a Debug impl is later added, the lint no longer fires, prompting attribute removal.")]
pub struct DescribeStatementAwaitingParamDescInstall {
    pub(crate) reply: ReplyId<DescribeStatementKind>,
}
impl PostStateSealed for DescribeStatementAwaitingParamDescInstall {}
// Install body lives in `state_setter::InstallBody` impl.
impl PostStateProof for DescribeStatementAwaitingParamDescInstall {}

/// Witness pairing [`DescribePortal`] to
/// [`crate::state::ProtoState::DescribePortalAwaitingRowDescOrNoData`].
#[must_use = "a DescribePortalAwaitingRowDescOrNoDataInstall has no effect until passed to StateSetter::install_post_state"]
#[expect(missing_debug_implementations, reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait. Migrated #[allow]→#[expect] (Rust 1.81): if a Debug impl is later added, the lint no longer fires, prompting attribute removal.")]
pub struct DescribePortalAwaitingRowDescOrNoDataInstall {
    pub(crate) reply: ReplyId<DescribePortalKind>,
}
impl PostStateSealed for DescribePortalAwaitingRowDescOrNoDataInstall {}
// Install body lives in `state_setter::InstallBody` impl.
impl PostStateProof for DescribePortalAwaitingRowDescOrNoDataInstall {}

/// Witness pairing [`CloseStatement`] / [`ClosePortal`] to
/// [`crate::state::ProtoState::CloseAwaitingComplete`]. Unified across
/// the two close targets — both produce identical post-push state
/// (the wire-level target byte distinction lives in the emitted Close
/// frame, not in the state machine).
#[must_use = "a CloseAwaitingCompleteInstall has no effect until passed to StateSetter::install_post_state"]
#[expect(
    missing_debug_implementations,
    reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait."
)]
pub struct CloseAwaitingCompleteInstall {
    pub(crate) reply: ReplyId<CloseKind>,
}
impl PostStateSealed for CloseAwaitingCompleteInstall {}
// Install body lives in `state_setter::InstallBody` impl.
impl PostStateProof for CloseAwaitingCompleteInstall {}

/// Witness pairing [`BindExecute<P>`] to one of two post-bind+execute
/// variants (DML / SELECT). The split surfaces the schema-bearing
/// vs schema-less path structurally — schema parking via
/// [`crate::schema_slot::RowDescSlotCell::park_at_be_select`] happens
/// BEFORE the install, inside `compute_push_bind_execute_idle_only`
/// (gated by the leaf-private `BeSelectToken`); this witness only
/// captures the variant choice + reply correlator.
///
/// **Note:** the witness does NOT carry the `RowDesc` payload
/// itself. Folding `row_desc` into the SELECT variant is one
/// possible shape; the chosen design keeps row_desc parking in
/// `RowDescSlotCell` (tier-1 within-crate by-construction via the
/// per-leaf token-gated write surface) and only narrows the state-
/// install pairing here. Avoiding GAT-driven `Aux` machinery on the
/// trait keeps the surface clean; per-leaf concrete-token mints
/// already close the sealed-trait bypass surface for row_desc.
#[must_use = "a BindExecutePostInstall has no effect until passed to StateSetter::install_post_state"]
#[expect(missing_debug_implementations, reason = "ZST witness flows by-value through one consumption path; Debug impl unused on this surface — defer until a concrete diagnostic surface needs the trait. Migrated #[allow]→#[expect] (Rust 1.81): if a Debug impl is later added, the lint no longer fires, prompting attribute removal.")]
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
// Install body lives in `state_setter::InstallBody` impl.
impl PostStateProof for BindExecutePostInstall {}

// ═════════════════════════════════════════════════════════════════════
// `BindPrepared<'q, P, R>` wraps a PreparedQuery + its argument tuple
// into a PushCommand impl. Caller-facing entry is
// `ReadyGuard::execute_prepared` (see `guard.rs`).
// ═════════════════════════════════════════════════════════════════════

/// Pair a [`crate::prepared::PreparedQuery`] with its argument tuple
/// for a single client→server execute cycle.
///
/// Implements [`PushCommand`]; dispatched via the existing
/// [`ReadyGuard::push_command`](crate::guard::ReadyGuard::push_command)
/// path so the Idle precondition + post-state typed witness closures
/// apply unchanged. The execute helper on `ReadyGuard` is the
/// ergonomic surface; this struct is the underlying mechanism.
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
///   (closed-set enum from `crate::command`).
/// - `reply`: typed correlator the wrapper routes the matching
///   reply through.
///
/// # Size pin
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
    /// Reuses `BindExecutePostInstall`. Prepared queries with result
    /// rows (`row_oids.is_empty() == false`) route through the SELECT
    /// branch with a synthetic RowDesc parked at push time; row-less
    /// queries route through DML.
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
// Size pins — per-command-struct drift guards
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

    /// Tier-1 drift guard: Parse + SimpleQuery sizes are bounded at
    /// "small" (≤ 128 B for Parse, ≤ 64 B for SimpleQuery). `&'a str`
    /// SQL is the load-bearing shape — re-embedding owned SQL would
    /// surface here dramatically (jump from ~96 B to ~2132 B for
    /// Parse).
    #[test]
    fn parse_and_simple_query_carry_no_inline_sql() {
        let parse_size = core::mem::size_of::<Parse<'static>>();
        let simple_query_size = core::mem::size_of::<SimpleQuery<'static>>();
        assert!(
            parse_size <= 128,
            "Parse must be ≤ 128 B (no inline SQL); got {parse_size} B. \
             A regression to ~2132 B would mean an owned-Sql field came back.",
        );
        assert!(
            simple_query_size <= 64,
            "SimpleQuery must be ≤ 64 B; got {simple_query_size} B.",
        );
    }
}

// ═════════════════════════════════════════════════════════════════════
// Hostile-witness seal pin
//
// Tier-1 within-crate closure of the install-body authority. This
// `#[cfg(test)] mod` lives in push_command (a SIBLING module to
// state_setter) — the exact in-crate position from which a hostile
// actor would attempt to mint a `HostileWitness` impl. The pin
// asserts at COMPILE TIME (via the no-dep ambiguous-blanket-impl
// trick mirroring `lib.rs:535`'s `assert_not_sync`) that:
//
//   1. HostileWitness can `impl Sealed` (pub(crate) — accessible).
//   2. HostileWitness can `impl PostStateProof` (empty marker —
//      accessible; PostStateProof carries no method).
//   3. HostileWitness CANNOT `impl InstallBody` (private supertrait
//      InstallBodySealed unreachable from this sibling module).
//   4. THEREFORE `setter.install_post_state(HostileWitness)` is E0277
//      at the bound (`W: InstallBody` not satisfied), AND
//      `idle.into_setter::<HostileWitness>()` is similarly E0277.
//
// If a future refactor accidentally re-opens the hole (e.g., promotes
// `mod install_body_seal` to `pub mod` or fuses `InstallBody` back
// into `PostStateProof`), the no-dep ambiguous check below fails to
// compile — the test target build breaks and the regression is caught.
// ═════════════════════════════════════════════════════════════════════

#[cfg(test)]
#[allow(dead_code, reason = "the impls + const block are compile-time pins; never invoked at runtime")]
mod hostile_witness_seal_probe {
    use super::sealed::PushCommandSealed;
    use crate::state_setter::sealed::Sealed as PostStateSealed;
    use crate::state_setter::{InstallBody, PostStateProof};

    /// Simulated hostile in-crate witness type. A real attacker would
    /// place this struct in any non-state_setter module to attempt
    /// arbitrary `ProtoState` writes via the install path.
    struct HostileWitness;

    /// (1) `impl Sealed` — succeeds. The sealed-supertrait module is
    /// `pub(crate)` (accessible to siblings); only EXTERNAL crates are
    /// blocked at this layer.
    impl PostStateSealed for HostileWitness {}

    /// (2) `impl PostStateProof` — succeeds. PostStateProof is a
    /// pure marker with no methods; nothing prevents a hostile
    /// in-crate type from implementing it. The closure is at the
    /// NEXT layer (InstallBody) where the install body actually
    /// lives.
    impl PostStateProof for HostileWitness {}

    /// (3) `impl InstallBody for HostileWitness {}` — CANNOT BE WRITTEN.
    /// Attempting it requires `HostileWitness: install_body_seal::InstallBodySealed`,
    /// which requires `impl install_body_seal::InstallBodySealed for HostileWitness {}`
    /// — and `mod install_body_seal` is PRIVATE to `mod state_setter`,
    /// so this sibling module fails with E0603 (module is private).
    ///
    /// The compile-time pin below asserts the resulting structural property:
    /// **HostileWitness does NOT impl InstallBody**. If a future refactor
    /// re-opens the seal (e.g., `pub mod install_body_seal`), someone
    /// somewhere adds the missing impl, and this overlapping-blanket-impl
    /// trick will detect it — method resolution becomes ambiguous and the
    /// build breaks.
    ///
    /// Mirror of `lib.rs:535`'s `assert_not_sync<PgProtocol>` no-dep trick.
    /// Zero runtime cost (typeck-only); the const block emits no code.
    const _: fn() = || {
        trait AmbiguousIfInstallBody<A> {
            #[allow(
                dead_code,
                reason = "Method exists only for typeck-time ambiguous-resolution check (overlapping-blanket-impl trick) — never invoked at runtime."
            )]
            fn assert_not_install_body() {}
        }
        impl<T: ?Sized> AmbiguousIfInstallBody<()> for T {}
        impl<T: ?Sized + InstallBody> AmbiguousIfInstallBody<u8> for T {}

        // If `HostileWitness: InstallBody`, the two blanket impls collide
        // on method resolution — compilation fails here. Closure stays
        // intact iff HostileWitness lacks an InstallBody impl, which is
        // the structural property the trait split enforces.
        <HostileWitness as AmbiguousIfInstallBody<_>>::assert_not_install_body();
    };

    /// (4) HostileWitness also cannot satisfy `PushCommand::PostState`
    /// (associated-type bound `: InstallBody`). The pin below would
    /// fail to compile if that bound regressed paired with (3);
    /// demonstrated by the negative-bound assertion above — the
    /// InstallBody pin transitively guards the PostState bound.
    /// Documented anchor; no separate runtime test needed.
    #[test]
    fn hostile_witness_install_body_absent_anchor() {
        // Anchor for `git grep "hostile_witness_seal_probe"` and
        // `git grep "AmbiguousIfInstallBody"` searches. The const block
        // above is the structural pin — this fn is the named test
        // surface for discoverability.
        //
        // Sanity: HostileWitness satisfies PostStateSealed + PostStateProof
        // (the two layers above InstallBody). Confirms the pin probes
        // the RIGHT layer (the install-body authority), not a layer
        // already closed by `pub(crate)` sealing. Const-context witness:
        // type-equality is checked at compile-time without `let _ =` form.
        const _: core::marker::PhantomData<HostileWitness> =
            core::marker::PhantomData;

        // PushCommandSealed import for `git grep` discoverability:
        // PushCommand's own seal is a separate concern.
        const _: core::marker::PhantomData<dyn PushCommandSealed> =
            core::marker::PhantomData;
    }
}
