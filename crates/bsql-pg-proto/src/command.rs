//! User-pushed commands.
//!
//! A `PgCommand` is the upstream wrapper's request to the protocol
//! state machine. Each variant carries a [`crate::ReplyId`] — the
//! correlator the wrapper later uses to route the reply back to the
//! correct caller's `oneshot::Sender`.

use crate::ident::{PortalName, Sql, StmtName};
use crate::reply_id::{
    DescribePortalKind, DescribeStatementKind, ParseKind, PingKind, QueryKind, ReplyId,
};

/// A command pushed by the wrapper into the protocol state machine.
///
/// `#[non_exhaustive]` because new commands land as their driving
/// paths come online; user `match` arms must accommodate growth.
///
/// `#[must_use]` because constructing a command without pushing it
/// into `crate::PgProtocol::push_command` cannot deliver a reply —
/// the user's `oneshot::Receiver` would block forever.
///
/// # No `Clone`
///
/// `PgCommand` owns a [`ReplyId`] which is deliberately non-duplicable
/// (see [`ReplyId`] docstring). A cloneable `PgCommand` would imply a
/// cloneable id, which would break the tier-1 "no silent reply loss"
/// invariant. If a caller needs multiple commands they mint multiple
/// ids from the wrapper's monotonic counter and build multiple commands.
///
/// `pub(crate)` — external callers construct per-command structs in
/// [`crate::push_command`] (`Ping`, `Parse`, `SimpleQuery`, etc.).
/// This enum is retained only for the lib-internal `compute_push_tests`
/// module and the blanket `impl PushCommand for PgCommand` slow-path.
#[derive(Debug)]
#[non_exhaustive]
#[must_use = "a PgCommand has no effect until pushed via PgProtocol::push_command"]
// `#[allow(dead_code)]` rather than `#[expect]` — the underlying
// `dead_code` lint fires ONLY in non-test builds. In `--cfg test` the
// `compute_push_tests` module + the blanket `impl PushCommand for
// PgCommand` consume every variant, so the lint doesn't fire and
// `#[expect]` would itself emit `unfulfilled_lint_expectations`.
// Cfg-conditional firing makes `#[expect]` the wrong tool (CREDO §B
// classify Skip + comment).
#[allow(dead_code, reason = "variants only constructed by lib-internal compute_push_tests mod + the blanket `impl PushCommand for PgCommand` slow-path; cfg-conditional dead-code")]
pub(crate) enum PgCommand {
    /// Cheap server liveness probe.
    ///
    /// Translated by the protocol to a `Sync` frame (5 wire bytes); the
    /// matching `ReadyForQuery` arrives back as
    /// [`crate::Reply::Pong`] under the supplied `reply` id.
    ///
    /// **Precondition:** the protocol must be in [`crate::ProtoState::Idle`].
    /// Pushing a Ping outside `Idle` is classified by the dispatcher.
    Ping {
        /// Correlator the wrapper will use to route the matching
        /// [`crate::Reply::Pong`] back to the caller.
        ///
        /// The type parameter `PingKind` binds the reply payload to
        /// [`crate::action::PongPayload`] at compile time — the
        /// dispatcher cannot produce any other payload for this id.
        reply: ReplyId<PingKind>,
    },

    /// Prepare a named SQL statement via PG's Extended Query protocol
    /// (`P`-frame + `S`-frame terminator).
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
    /// single-command API shape consistent with `Ping` / `SimpleQuery`.
    ///
    /// # Stack cost per push
    ///
    /// `PgCommand::Parse` is the dominant variant — ~2132 B
    /// (stmt_name 66 + sql 2050 + reply 16). Every
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
    ///    `PgCommand` enum.
    ///
    /// Accept: 2 KB of stack per Parse push is fine on tokio / std;
    /// more problematic on embedded `no_std` + thin stacks.
    Parse {
        /// The prepared-statement name. Empty (the "unnamed statement"
        /// per PG convention) or a validated `StmtName` up to
        /// [`crate::ident::MAX_PG_NAME_LEN`] bytes.
        stmt_name: StmtName,
        /// SQL text — bounded to [`crate::ident::MAX_SQL_LEN`] with
        /// truncating constructor.
        sql: Sql,
        /// Correlator for the reply. The typed `ReplyId<ParseKind>`
        /// binds the payload to [`crate::action::ParseCompletePayload`]
        /// at compile time.
        reply: ReplyId<ParseKind>,
    },

    /// Execute a single SQL statement via PG's Simple Query protocol
    /// (`Q`-frame).
    ///
    /// **Precondition:** protocol must be in [`crate::ProtoState::Idle`].
    /// Any other state yields a `FailReply(CommandInProgress)` —
    /// the caller must wait for the current command to finish.
    ///
    /// # Response sequence
    ///
    /// - SELECT: `RowDescription` → 0..N `DataRow` (streamed via
    ///   the row-streaming `ColEvent` pull API) → `CommandComplete` →
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
    /// share a single trailing `Z`. The current implementation
    /// accepts a single statement.
    SimpleQuery {
        /// SQL text — bounded to [`crate::ident::MAX_SQL_LEN`] =
        /// 2048 bytes with explicit `"…"` truncation on overflow
        /// (no silent drop).
        sql: Sql,
        /// Correlator for the reply. The typed `ReplyId<QueryKind>`
        /// binds the payload to [`crate::action::QueryCompletePayload`]
        /// at compile time.
        reply: ReplyId<QueryKind>,
    },

    /// Inspect a previously-[`PgCommand::Parse`]'d prepared
    /// statement via PG's Extended Query `Describe` + `Sync` bundle
    /// (PG §55.2.2).
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
    /// Kind-parameterisation binds the payload at the
    /// `Action::DeliverReply` construction site.
    ///
    /// # Sync-bundling
    ///
    /// Emits TWO outbound wire frames in one push: a `Describe`
    /// frame followed by a `Sync`. Without the Sync, PG buffers
    /// Extended Query responses indefinitely.
    DescribeStatement {
        /// Prepared-statement name. Empty (the "unnamed statement"
        /// per PG) or a validated `StmtName` up to
        /// [`crate::ident::MAX_PG_NAME_LEN`] bytes.
        stmt_name: StmtName,
        /// Correlator for the reply. The typed
        /// `ReplyId<DescribeStatementKind>` binds the payload to
        /// [`crate::action::DescribeStatementCompletePayload`] at
        /// compile time.
        reply: ReplyId<DescribeStatementKind>,
    },

    /// Inspect a previously-bound portal via PG's Extended Query
    /// `Describe` + `Sync` bundle (PG §55.2.2).
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
        /// Correlator for the reply. The typed
        /// `ReplyId<DescribePortalKind>` binds the payload to
        /// [`crate::action::DescribePortalCompletePayload`] at
        /// compile time.
        reply: ReplyId<DescribePortalKind>,
    },
}

/// How many rows a bound portal should produce before pausing.
///
/// Parameter of `crate::PgProtocol::push_bind_execute`. Encoded on
/// the wire in the PG `Execute` frame as a 4-byte `i32` — but the
/// type system narrows the user-facing API to the variants this
/// codebase supports.
///
/// # Tier-1 enum vs tier-3 `u32`
///
/// `max_rows: u32` would be a tier-3 runtime trap: any non-zero value
/// would cause the server to emit `PortalSuspended`, which the
/// dispatcher classifies as `UnexpectedFrame` → connection teardown.
/// The compiler would give the caller no signal.
///
/// `FetchRows::All` makes the only currently-supported policy a
/// compile-time constant — passing anything else is a build error.
/// `#[non_exhaustive]` leaves room for a future `Chunked(NonZeroU32)`
/// variant when chunked-fetch (`PortalSuspended` + subsequent
/// `Execute` calls) ships; users would then transition from `All`
/// to `Chunked(NonZeroU32)` and the variant-level dispatch threads
/// the correct response shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FetchRows {
    /// Fetch all rows the portal produces (no `PortalSuspended`).
    /// Maps to the wire value `max_rows = 0`.
    All,
    /// Fetch at most `N` rows; server pauses at the cap with
    /// `PortalSuspended` (PG §55.2.7). The portal stays open and can
    /// be resumed via [`crate::push_command::ExecutePortal`] for
    /// additional batches.
    ///
    /// `NonZeroU32` enforces «non-zero» at the type level — zero
    /// would semantically be `Self::All` and would dispatch through
    /// a different response shape (no `PortalSuspended`), so the
    /// caller MUST pick `Self::All` explicitly for that case rather
    /// than a `Chunked(0)` sentinel.
    ///
    /// Wire encoding: `i32` (PG's `Execute.max_rows` field is signed
    /// 32-bit but client-side values > i32::MAX are sub-spec). The
    /// `as_wire_i32` conversion narrows via `i32::try_from` with
    /// saturation at `i32::MAX` — a value > i32::MAX is honest
    /// «request capped at server-spec max» rather than wraparound.
    Chunked(core::num::NonZeroU32),
}

impl FetchRows {
    /// Wire-encoding — the `i32` value PG expects in the Execute
    /// frame's `max_rows` field.
    #[inline]
    #[must_use]
    pub(crate) const fn as_wire_i32(self) -> i32 {
        match self {
            Self::All => 0,
            Self::Chunked(n) => {
                let v: u32 = n.get();
                // `as` is forbidden; explicit narrow with saturation.
                // u32 > i32::MAX is sub-spec (PG max_rows is i32);
                // saturate to i32::MAX rather than wrap-negative.
                // Bound: i32::MAX as u32 is 0x7FFF_FFFF — compute
                // via `i32::MAX.to_le_bytes()` reinterpret to avoid
                // any `as`.
                const I32_MAX_AS_U32: u32 = u32::from_le_bytes(i32::MAX.to_le_bytes());
                if v > I32_MAX_AS_U32 {
                    i32::MAX
                } else {
                    // v <= i32::MAX so the cast is bound-checked
                    // but `as` is banned — use i32::from_le_bytes
                    // on u32::to_le_bytes for an `as`-free narrow.
                    i32::from_le_bytes(v.to_le_bytes())
                }
            }
        }
    }
}

// Compile-time drift-pin: `FetchRows::All` MUST map to wire value 0
// (PG §55.2.2 — `Execute` frame with `max_rows = 0` means fetch all
// rows without PortalSuspended). An arm-body edit in `as_wire_i32`
// that silently returned `1` (or any non-zero) would cause the
// server to emit PortalSuspended which the dispatcher classifies as
// UnexpectedFrame → connection teardown.
const _: () = assert!(
    FetchRows::All.as_wire_i32() == 0,
    "FetchRows::All MUST wire-encode as 0 per PG §55.2.2",
);

// Drift-pin for the Chunked variant. NonZeroU32::MIN is 1; passing 1
// must wire-encode as 1 (server returns 1 row + PortalSuspended). A
// future arm-body edit that returned 0 would silently degrade to
// «fetch all» semantics.
const _: () = assert!(
    matches!(
        FetchRows::Chunked(core::num::NonZeroU32::MIN).as_wire_i32(),
        1,
    ),
    "FetchRows::Chunked(1) MUST wire-encode as 1 per PG §55.2.7",
);

// Saturation pin: u32::MAX > i32::MAX must saturate (not wrap to
// negative). PG's max_rows is i32, so values > i32::MAX are
// sub-spec; honest cap rather than wraparound bug.
const _: () = {
    let max_u32 = u32::MAX;
    // NonZeroU32::new is const-stable on recent Rust.
    let nz_max = match core::num::NonZeroU32::new(max_u32) {
        Some(n) => n,
        // u32::MAX != 0, so this arm is dead.
        None => panic!("u32::MAX is non-zero"),
    };
    assert!(
        matches!(FetchRows::Chunked(nz_max).as_wire_i32(), i32::MAX),
        "FetchRows::Chunked(u32::MAX) MUST saturate at i32::MAX, not wrap negative",
    );
};
