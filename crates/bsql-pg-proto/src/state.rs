//! Protocol state — state-as-data.
//!
//! Each variant carries its in-flight correlator inline (reforge.md
//! §7.2). Consequence: a transition out of [`ProtoState::PingAwaitingRfq`]
//! that fails to consume the inner [`ReplyId`] is a build error — the
//! borrow / move checker forces every transition to handle the carried
//! data explicitly.
//!
//! Phase 1a ships only the variants the Ping flow drives. Per reforge.md
//! §3.5 / §4.6, manufactured variants ("ConnectingStartup", "InTransaction",
//! "Closed", …) are forbidden until their entry/exit code lands in a
//! later sub-phase.
//!
//! [`ProtoState::Errored`] is the one terminal variant — entered via
//! any classified failure in `feed_bytes` or `push_command`, never
//! left. Its presence prevents the state from lying about connection
//! health: a post-error frame arriving at `feed_bytes` observes
//! `Errored`, not `Idle`, and the dispatcher's arm keeps it that way
//! (no action, no state change — post-`CloseSocket` packet flushes
//! become true no-ops instead of silent mis-advances).

use crate::action::ParamOids;
use crate::error::BoundedStr;
use crate::error::StateErrorKind;
use crate::ident::PodBytes;
use crate::reply_id::{
    DescribePortalKind, DescribeStatementKind, ParseKind, PingKind, QueryKind, ReplyId,
    StartupKind,
};
use crate::scram::session::ScramSession;
use crate::scram::types::SecretDigest;

/// State-side counterpart to the public
/// [`crate::action::DescribedRows<'r>`].
///
/// # Post-DEF-189 (architect 2026-04-25): externalised slot
///
/// State variants do NOT carry a `RowDesc` payload. The schema, when
/// present, lives in `PgProtocol::row_desc_slot` — populated by the
/// `'T'` dispatch arm and read by terminal materialise via the
/// protocol's `current_row_desc()` accessor. This enum is a slim
/// Where the protocol is right now.
///
/// **Internal-use enum.** Not `#[non_exhaustive]`: exhaustive `match` in
/// internal dispatch is the load-bearing tier-1 invariant — a missed
/// (state, tag) combination is a build failure.
///
/// `Default` is `Idle`, which lets [`core::mem::take`] swap the state
/// out for owned-pattern transitions without ceremony. `take` on an
/// [`Errored`][ProtoState::Errored] state is a genuine hazard (it would
/// lose the stored cause and replace it with `Idle`, re-opening the
/// connection for commands); every caller that uses `mem::take` on the
/// state must explicitly preserve the `Errored` case — see the
/// `fail_inflight_and_close` and `handle_push_ping` bodies in
/// `protocol.rs`.
// Deliberately **not** `Copy`: moving out of `PingAwaitingRfq(id)`
// must consume the [`crate::ReplyId`] inline — the state-as-data
// invariant (reforge.md §7.2). `ProtoState` inherits non-Copy from
// the non-Copy `ReplyId` field, so the `missing_copy_implementations`
// lint does not fire here (there is no "could be Copy" suggestion to
// suppress).
#[derive(Default)]
pub enum ProtoState {
    /// Connection established and idle. Accepts new commands.
    #[default]
    Idle,

    /// A `Sync` was sent; awaiting the matching `ReadyForQuery` reply.
    ///
    /// The carried [`ReplyId`] is the only way the inner correlator can
    /// be reached. Any transition that abandons it without forwarding
    /// to a [`crate::Action::DeliverReply`] / [`crate::Action::FailReply`]
    /// will leave the user's `oneshot::Receiver` permanently pending —
    /// that is exactly the bug class the state-as-data pattern makes
    /// impossible to write.
    PingAwaitingRfq(ReplyId<PingKind>),

    // ---------------------------------------------------------------
    // Phase 1b: startup + auth handshake states (DEF-001..DEF-004)
    // ---------------------------------------------------------------

    /// A `StartupMessage` was sent by a Trust-auth connection;
    /// awaiting `AuthenticationOk`. DEF-001 + DEF-097.
    ///
    /// # Why split from the Scram variant
    ///
    /// Before DEF-097 a single `ConnectingStartup { reply, credentials }`
    /// variant carried the full [`crate::password::Credentials`] enum
    /// until the server responded. Two consequences:
    ///
    /// - ~1040 bytes of password buffer lived in state until the
    ///   first frame arrived (Trust connections paid the Scram-sized
    ///   stack footprint).
    /// - The "server requested SASL on a Trust connection" case was
    ///   classified at runtime (`UnsupportedAuthMethod`), not at
    ///   compile time.
    ///
    /// The Trust/Scram split moves discrimination to
    /// [`crate::PgProtocol::push_command`]. Each variant now only
    /// carries what its authentication path needs. A server frame
    /// of the wrong shape for the connection's credential type
    /// becomes a per-variant dispatcher arm — a missed arm is a
    /// build failure.
    ConnectingStartupTrust {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
    },

    /// A `StartupMessage` was sent by a SCRAM-auth connection;
    /// awaiting `AuthenticationSASL` offering SCRAM-SHA-256.
    /// DEF-001 + DEF-097.
    ///
    /// # Tier-1 compile — variant carries its data
    ///
    /// `scram: ScramSession` lives INSIDE this variant. The
    /// correlation "SCRAM-state has SCRAM data" is enforced
    /// structurally by Rust's type system — a future refactor
    /// cannot have `ConnectingStartupScram` without a valid
    /// `ScramSession`. `ZeroizeOnDrop` on `ScramSession` fires
    /// automatically when the variant drops, at EVERY exit path:
    ///
    /// - happy progression: `core::mem::replace(state, ProtoState::Idle)`
    ///   inside `dispatch()` moves the variant into the match
    ///   scrutinee; arm bodies either consume `scram` (passing to
    ///   the next dispatcher) or destructure `{ reply, .. }` which
    ///   drops the unbound `scram` at arm-body scope end;
    /// - fatal teardown: `core::mem::replace(state, Errored(kind))`
    ///   inside `fail_inflight_no_readbuf` drops the `prev` SCRAM
    ///   variant at function-return via RAII;
    /// - entry-point reshuffle: `core::mem::take(state)` inside
    ///   `push_command` moves into `compute_push` by value; if the
    ///   incoming state was NOT SCRAM the next state is reassigned
    ///   and the prev dropped; if it WAS SCRAM the classifier keeps
    ///   it (no transition), so no drop fires.
    ///
    /// Password material scrubbed immediately on transition — no
    /// separate "clear on Errored" step needed and none possible
    /// since there is no out-of-variant slot to clear.
    ConnectingStartupScram {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// SCRAM session (the password the user provided), heap-boxed.
        ///
        /// # DEF-187 (architect 2026-04-26): boxed
        ///
        /// Pre-DEF-187 the `ScramSession` (~520 B with full Password)
        /// lived inline in the variant, dominating `ProtoState` size at
        /// ~712 B and causing cache-locality damage on the per-row hot
        /// path (iter_rows_per_row +110% regression vs pre-A10/B22).
        ///
        /// Post-DEF-187: `Box<ScramSession>` reduces variant footprint
        /// to 8 + 16 = 24 B. Tier-1 preserved — Box can't be None,
        /// Box's Drop fires `ScramSession::Drop` (ZeroizeOnDrop) on
        /// every exit path. Cost in this variant: one heap alloc.
        ///
        /// **Per-handshake total — DEF-210 SR-07 doc-drift fix
        /// (audit 2026-04-28).** Pre-audit text claimed *"one heap
        /// alloc per SCRAM connection"*, which described the
        /// Phase-1 constellation accurately but missed the
        /// Phase-2 reality: the next variant
        /// [`Self::ConnectingScramAwaitingServerFirst`] adds two
        /// further `Box<PodBytes<…>>` fields (`client_first_bare`,
        /// `client_nonce_b64`), so the worst-case live-variant
        /// footprint during a handshake is **three** heap allocs,
        /// not one. Drop chain (Box → contents) still scrubs
        /// every secret-bearing byte on transition; tier
        /// classification unchanged. Consolidation into a single
        /// `Box<ScramHandshakeState>` is tracked under DEF-210
        /// REC-06 (alloc-count win + docstring becomes literally
        /// accurate again).
        scram: alloc::boxed::Box<ScramSession>,
    },

    /// SCRAM step 1 complete (client-first sent); awaiting
    /// `AuthenticationSASLContinue` (server-first-message). DEF-002.
    ConnectingScramAwaitingServerFirst {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// SCRAM session (heap-boxed, see [`Self::ConnectingStartupScram`]).
        scram: alloc::boxed::Box<ScramSession>,
        /// The `client-first-message-bare` (saved for AuthMessage).
        /// Heap-boxed per DEF-187 to keep the variant compact —
        /// boxed `PodBytes<128>` = 8 B in the variant vs 130 B inline.
        client_first_bare:
            alloc::boxed::Box<PodBytes<{ crate::scram::wire::MAX_CLIENT_FIRST_BARE_LEN }>>,
        /// The client nonce (base64-encoded, for prefix validation).
        /// Heap-boxed per DEF-187.
        client_nonce_b64:
            alloc::boxed::Box<PodBytes<{ crate::scram::wire::MAX_CLIENT_NONCE_B64_LEN }>>,
    },

    /// SCRAM step 2 complete (client-final sent); awaiting
    /// `AuthenticationSASLFinal` (server-final-message). DEF-002.
    ConnectingScramAwaitingServerFinal {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// Expected server signature for constant-time comparison.
        expected_server_sig: SecretDigest,
    },

    /// SCRAM step 3 complete (server signature verified); awaiting
    /// `AuthenticationOk`. DEF-002.
    ConnectingScramAwaitingAuthOk(ReplyId<StartupKind>),

    /// Authentication succeeded; waiting for `BackendKeyData`. DEF-003.
    ///
    /// `ParameterStatus` messages received in this state are recorded
    /// on [`crate::PgProtocol::session_params`] by the `feed_bytes`
    /// loop. `BackendKeyData` transitions to `ConnectingPostAuthHaveKey`.
    ConnectingPostAuthAwaitingKey(ReplyId<StartupKind>),

    /// `BackendKeyData` received; waiting for `ReadyForQuery`. DEF-004.
    ///
    /// Additional `ParameterStatus` messages may arrive before RFQ.
    ///
    /// # DEF-189 Q8-C2 — secret_key wrapped in Sensitive<i32>
    ///
    /// `secret_key: Sensitive<i32>` — the PG backend's CancelRequest
    /// authenticator. A leaked `secret_key` enables query cancellation
    /// on the same backend process (impersonation-within-session).
    /// `Sensitive<i32>` provides:
    ///
    /// - **Zero-on-drop**: when this variant transitions out (state
    ///   moves to Idle on the trailing RFQ, or to Errored on a fatal
    ///   frame), the inner `i32` is overwritten with zero before the
    ///   memory is reused by the next variant. Defense in depth
    ///   alongside ReadBuf/WriteBuf P0-B/C zeroize-on-clear.
    /// - **Debug redaction**: any future Debug print of `ProtoState`
    ///   prints `<REDACTED>` for the secret_key.
    ///
    /// Pre-DEF-189 (DEF-185 P2-C) was `secret_key: i32` (Copy); the
    /// audit accepted the residue trade-off in exchange for the
    /// `Sensitive` wrapper's `!Copy` cost cascading through
    /// match-destructure in dispatch. DEF-189 commits to the wrapper:
    /// the dispatch RFQ arm `match` extracts the inner via `.get()`
    /// (returns `&i32`), then copy-derefs into the
    /// `StartupCompletePayload` (which itself has manual Debug
    /// redaction per P1-C). The wrapper drops at the
    /// `mem::replace(state, Idle)` in dispatch, scrubbing the slot.
    ConnectingPostAuthHaveKey {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// The backend process ID.
        pid: i32,
        /// The backend secret key (for cancel requests).
        ///
        /// Wrapped in [`crate::sensitive::Sensitive`] for zero-on-drop
        /// scrub on state transitions. See variant docstring.
        secret_key: crate::sensitive::Sensitive<i32>,
    },

    // ---------------------------------------------------------------
    // Phase 1c-1b: Simple Query flow (PgCommand::SimpleQuery)
    // ---------------------------------------------------------------

    /// A `Query` frame was sent; awaiting the first response —
    /// which may be `RowDescription` (SELECT), `CommandComplete`
    /// (DML), `EmptyQueryResponse` (empty SQL), or `ErrorResponse`.
    ///
    /// The carried [`ReplyId<QueryKind>`] is the only path to the
    /// inner correlator; state-as-data invariant (§7.2). Subsequent
    /// transitions either consume it into a [`crate::Action::DeliverReply`]
    /// / [`crate::Action::FailReply`] or forward it into the next
    /// phase state.
    SimpleQueryAwaitingFirstResponse(ReplyId<QueryKind>),

    /// `RowDescription` received; now streaming `DataRow` frames.
    /// Terminal transitions: `DataRow` → fast-path `StreamItem::Row`,
    /// stay here; `CommandComplete` → [`Self::SimpleQueryAwaitingRfq`]
    /// with the parsed command tag.
    ///
    /// DEF-189: variant carries no schema field. The schema lives in
    /// `PgProtocol::row_desc_slot` (populated by the `'T'` arm BEFORE
    /// the transition into this variant). The per-row hot-path reads
    /// the desc via `proto.current_row_desc()` (single immutable
    /// borrow projection from the slot) — no per-variant payload,
    /// no per-row state match.
    SimpleQueryStreamingRows {
        /// Correlator for the in-flight query.
        reply: ReplyId<QueryKind>,
    },

    /// `CommandComplete` or `EmptyQueryResponse` received; awaiting
    /// the trailing `ReadyForQuery`. The command tag captured at `C`
    /// (empty for `EmptyQueryResponse`) ships in the final
    /// [`crate::Reply::QueryComplete`] payload.
    ///
    /// # DEF-210 SR-01 Path C (audit 2026-04-28): `schema_present` deleted
    ///
    /// Pre-Path-C this variant carried `schema_present: bool` —
    /// a duplicate of `PgProtocol::row_desc_slot.is_some()` kept
    /// in lockstep by dispatch-arm discipline. The duplication was
    /// **tier-2 structural** (same dispatch arm sets bool ↔
    /// populates slot atomically) but architecturally fragile: a
    /// future refactor that set `schema_present = true` without
    /// populating the slot would silently produce
    /// `Reply::QueryComplete.row_desc = None` for SELECTs ("DML
    /// done" instead of rows — silent corruption).
    ///
    /// Path C eliminates the duplicate. The single source of truth
    /// is `PgProtocol::row_desc_slot`; terminal materialise reads
    /// the slot directly via `into_public`. A future Path C audit
    /// can confirm: there is no second variable that can drift.
    /// **Tier-1 by-construction** — the invariant is "the slot
    /// equals itself", which is identity, not discipline.
    SimpleQueryAwaitingRfq {
        /// Correlator for the in-flight query.
        reply: ReplyId<QueryKind>,
        /// Command tag — `"SELECT 5"`, `"INSERT 0 3"`, or empty
        /// for empty-query responses. Capacity 32 bytes handles
        /// PG's documented tag shapes (the longest standard tag,
        /// `"INSERT <oid> <n>"` with 10-digit values, is ~23 bytes).
        command_tag: BoundedStr<32>,
    },

    /// `ErrorResponse` received mid-query; `FailReply` already
    /// emitted. Awaiting the trailing `ReadyForQuery` that PG sends
    /// after query-level errors — per spec, `Z` follows `E` and the
    /// connection stays open. This variant silently consumes that
    /// `Z` and transitions back to [`Self::Idle`].
    DrainRfqAfterError,

    // ---------------------------------------------------------------
    // Phase 1c-3a: Extended Query — Parse flow
    // ---------------------------------------------------------------

    /// A `Parse` + `Sync` frame pair was sent; awaiting `ParseComplete`
    /// (`'1'`). The inner [`ReplyId<ParseKind>`] is the only path to
    /// the correlator; state-as-data (§7.2).
    ///
    /// Next legitimate frames: `ParseComplete` → transition to
    /// [`Self::ParseAwaitingRfq`]; `ErrorResponse` → emit FailReply +
    /// transition to [`Self::DrainRfqAfterError`]
    /// (reused — both paths drain a trailing RFQ back to Idle).
    ParseAwaitingParseComplete(ReplyId<ParseKind>),

    /// `ParseComplete` received; awaiting the `ReadyForQuery` that
    /// follows the bundled `Sync`. On `Z` → deliver
    /// [`crate::Reply::ParseComplete`] and transition to Idle.
    ParseAwaitingRfq(ReplyId<ParseKind>),

    // ---------------------------------------------------------------
    // Phase 1c-3b: Extended Query — Bind + Execute flow
    // ---------------------------------------------------------------
    //
    // `push_bind_execute` emits `Bind` + `Execute` + `Sync` as one
    // bundle. Server response shape (PG §55.2.2):
    //
    //   '2' (BindComplete)     — server accepted params
    //   ['T'] (RowDescription) — ONLY if a prior Describe ran;
    //                            1c-3b doesn't auto-describe, so user-
    //                            supplied row_desc is threaded from
    //                            the push call
    //   'D'* (DataRow)         — result rows (zero rows for DML)
    //   'C' (CommandComplete)  — result-set boundary
    //   'Z' (ReadyForQuery)    — sync boundary
    //
    // The four state variants below mirror the SimpleQuery shape
    // with a `BindComplete` prefix state. Schema (row_desc) is
    // carried through state transitions same as F19 — no separate
    // slot on PgProtocol.

    // The BindExecute flow splits into TWO state families based on
    // whether the user provided a row_desc (SELECT with schema) or
    // not (DML / RETURNING-less). The split encodes the "can we
    // stream decoded rows?" decision at the VARIANT level rather
    // than runtime-matching on `Option<RowDesc>` at the 'D'
    // dispatch arm. Tier uplift: tier-2 runtime match → tier-1
    // structural dispatch.
    //
    // The decision is made once at `push_bind_execute` call time
    // (based on caller's `Option<RowDesc>`) and threaded through
    // the three-stage pipeline (BindComplete → Data/Complete →
    // AwaitingRfq). Six variants total.

    // ─── Schema-less path (DML / RETURNING-less) ───

    /// `Bind` + `Execute` + `Sync` bundle was sent; awaiting
    /// `BindComplete` (`'2'`). DML path — caller passed
    /// `row_desc = None`, meaning any server-emitted DataRow is
    /// an invariant break (user asked for DML, server shipped
    /// rows). The arm classifies `'D'` as UnexpectedFrame.
    BindExecuteAwaitingBindCompleteDml(ReplyId<QueryKind>),

    /// `BindComplete` received on the schema-less path; awaiting
    /// `CommandComplete` or server error. `DataRow` is a wire
    /// violation here (no schema was provided to decode rows).
    BindExecuteAwaitingCommandCompleteDml(ReplyId<QueryKind>),

    /// `CommandComplete` received on the schema-less path; awaiting
    /// the trailing `ReadyForQuery`. Terminal reply carries
    /// `row_desc: None`.
    BindExecuteAwaitingRfqDml {
        /// Correlator for the in-flight command.
        reply: ReplyId<QueryKind>,
        /// Command tag parsed from the `C` frame body.
        command_tag: BoundedStr<32>,
    },

    // ─── Schema-bearing path (SELECT with pre-provided schema) ───

    /// `Bind` + `Execute` + `Sync` bundle was sent; awaiting
    /// `BindComplete` (`'2'`). SELECT path — caller pre-provided
    /// the schema; it has been parked into `PgProtocol::row_desc_slot`
    /// at push time. The variant name `Select` is itself the tier-1
    /// signal that the slot is populated.
    BindExecuteAwaitingBindCompleteSelect {
        /// Correlator for the in-flight command.
        reply: ReplyId<QueryKind>,
    },

    /// `BindComplete` received on the schema-bearing path; awaiting
    /// either a `DataRow` (transition to [`Self::BindExecuteStreamingRows`])
    /// or `CommandComplete` (0-row SELECT, transition to
    /// [`Self::BindExecuteAwaitingRfqSelect`]). Schema lives in slot.
    BindExecuteAwaitingDataOrCompleteSelect {
        /// Correlator for the in-flight command.
        reply: ReplyId<QueryKind>,
    },

    /// Streaming `DataRow` frames on the schema-bearing path.
    /// Mirrors [`Self::SimpleQueryStreamingRows`] — schema lives in
    /// `PgProtocol::row_desc_slot`. The per-row hot-path reads the
    /// desc via `proto.current_row_desc()`.
    BindExecuteStreamingRows {
        /// Correlator for the in-flight command.
        reply: ReplyId<QueryKind>,
    },

    /// `CommandComplete` received on the schema-bearing path;
    /// awaiting the trailing `ReadyForQuery`. Terminal reply
    /// carries `row_desc: Some(...)` resolved from the slot.
    BindExecuteAwaitingRfqSelect {
        /// Correlator for the in-flight command.
        reply: ReplyId<QueryKind>,
        /// Command tag parsed from the `C` frame body.
        command_tag: BoundedStr<32>,
    },

    // ---------------------------------------------------------------
    // Phase 1c-3c: Extended Query — Describe flow
    // ---------------------------------------------------------------
    //
    // `push_command(PgCommand::DescribeStatement | DescribePortal)`
    // emits a `Describe` + `Sync` bundle. Server response shape
    // (PG §55.2.2):
    //
    //   STATEMENT target:
    //     't' (ParameterDescription) — always first
    //     'T' (RowDescription) or 'n' (NoData) — schema or nothing
    //     'Z' (ReadyForQuery) — sync boundary
    //
    //   PORTAL target:
    //     'T' (RowDescription) or 'n' (NoData) — schema or nothing
    //     'Z' (ReadyForQuery) — sync boundary
    //     (NO ParameterDescription — portals are bound-state)
    //
    // Error path (either target): 'E' (ErrorResponse) — e.g. invalid
    // stmt/portal name — is query-level recoverable: emit FailReply
    // + transition to DrainRfqAfterError. Connection survives.
    //
    // The five state variants below encode the per-target response
    // topology at the VARIANT level: statement-describe has 3
    // stages (AwaitingParamDesc → AwaitingRowDescOrNoData → AwaitingRfq);
    // portal-describe has 2 (AwaitingRowDescOrNoData → AwaitingRfq).
    // A dispatcher arm that receives `'T'` in `AwaitingParamDesc`
    // is UnexpectedFrame — server violated the described sequence.

    // ─── Statement-describe path ───

    /// A `Describe 'S'` + `Sync` bundle was sent; awaiting
    /// `ParameterDescription` (`'t'`). The inner
    /// [`ReplyId<DescribeStatementKind>`] is the only path to the
    /// correlator; state-as-data (§7.2).
    ///
    /// Next legitimate frames:
    /// - `'t'` → parse ParamOids, transition to
    ///   [`Self::DescribeStatementAwaitingRowDescOrNoData`].
    /// - `'E'` (ErrorResponse) → FailReply + `DrainRfqAfterError`
    ///   (recoverable — connection survives).
    /// - Anything else → UnexpectedFrame → teardown.
    DescribeStatementAwaitingParamDesc(ReplyId<DescribeStatementKind>),

    /// `ParameterDescription` parsed; awaiting either
    /// `RowDescription` (`'T'`) or `NoData` (`'n'`).
    ///
    /// Schema branch: `'T'` → [`DescribedRows::Rows(desc)`]; continue
    /// to [`Self::DescribeStatementAwaitingRfq`].
    /// No-data branch: `'n'` → [`DescribedRows::NoData`]; continue
    /// to [`Self::DescribeStatementAwaitingRfq`].
    DescribeStatementAwaitingRowDescOrNoData {
        /// Correlator for the Describe command.
        reply: ReplyId<DescribeStatementKind>,
        /// Parameter OIDs parsed from the preceding `'t'` frame.
        /// Threaded through to the terminal reply payload.
        param_oids: ParamOids,
    },

    /// Row-desc / no-data known; awaiting the trailing
    /// `ReadyForQuery` that closes the Sync boundary. On `'Z'` →
    /// deliver [`crate::Reply::DescribeStatementComplete`] and
    /// transition to Idle.
    ///
    /// # DEF-210 SR-01-D Path D (audit 2026-04-28)
    ///
    /// Pre-Path-D this variant carried a `rows: DescribedRowsStaged`
    /// discriminator — exactly the same architectural shape as the
    /// `schema_present: bool` removed by Path C from
    /// `SimpleQueryAwaitingRfq`. The discriminator was a duplicate
    /// of `PgProtocol::row_desc_slot.is_some()` (the `'T'` arm
    /// populated the slot AND set `Rows`; the `'n'` arm did neither).
    /// Materialise read the discriminator and projected from the slot
    /// — but if the discriminator and slot drifted, the projection
    /// silently swallowed the schema (manifested as a tier-3
    /// `debug_assert!(false)` arm in production code, CREDO §V banned
    /// pattern). Path D deletes the discriminator; materialise reads
    /// `row_desc_slot.map(...)` directly. **Tier-1 by-construction**:
    /// the slot equals itself (identity, not discipline).
    DescribeStatementAwaitingRfq {
        /// Correlator for the Describe command.
        reply: ReplyId<DescribeStatementKind>,
        /// Parameter OIDs captured at the `'t'` transition.
        param_oids: ParamOids,
    },

    // ─── Portal-describe path ───

    /// A `Describe 'P'` + `Sync` bundle was sent; awaiting either
    /// `RowDescription` (`'T'`) or `NoData` (`'n'`).
    ///
    /// **No** `ParameterDescription` precedes per PG §55.2.2 — a
    /// `'t'` frame here would be UnexpectedFrame.
    DescribePortalAwaitingRowDescOrNoData(ReplyId<DescribePortalKind>),

    /// Row-desc / no-data known; awaiting `ReadyForQuery`. On `'Z'`
    /// → deliver [`crate::Reply::DescribePortalComplete`] and
    /// transition to Idle.
    ///
    /// DEF-210 SR-01-D Path D: same as
    /// [`Self::DescribeStatementAwaitingRfq`] — discriminator removed,
    /// slot is the single source of truth.
    DescribePortalAwaitingRfq {
        /// Correlator for the Describe command.
        reply: ReplyId<DescribePortalKind>,
    },

    /// Terminal: the connection has been classified as unrecoverable.
    ///
    /// Entered by any path that calls `fail_inflight_and_close` or
    /// returns `DispatchOutcome::Errored` — these paths also emit the
    /// matching `FailReply` (full cause) + `CloseSocket` actions in the
    /// same call, so by the time the state is observable as `Errored`
    /// the wrapper has already received the diagnostic.
    ///
    /// Never left. DEF-061 + DEF-142: carries [`StateErrorKind`]
    /// (1 byte), the `AlreadyClosed`-free subset of
    /// [`crate::error::ErrorKind`]. The full cause went out once in
    /// the first `FailReply`; subsequent pushes get a compact
    /// [`crate::error::ProtocolError::ConnectionAlreadyClosed`]
    /// carrying the `prior_kind` for diagnostic context.
    ///
    /// # Why `StateErrorKind` and not `ErrorKind`
    ///
    /// DEF-142 (pass-#8 F-056) narrows the carried type from the
    /// full `ErrorKind` to the `StateErrorKind` newtype. The
    /// invariant "state never holds the `AlreadyClosed`
    /// pseudo-kind" was previously tier-3 audit (maintained by the
    /// `fail_inflight_and_close` early-return guard); now it's
    /// tier-1 compile — constructing `Errored(AlreadyClosed)` is a
    /// type error at the `StateErrorKind::try_from_kind` call site.
    Errored(StateErrorKind),
}

impl ProtoState {
    /// Consume `self` and return the raw `NonZeroU64` of the typed
    /// [`ReplyId<_>`] in flight for this state, or `None` if no
    /// reply is in flight ([`Self::Idle`], [`Self::DrainRfqAfterError`],
    /// [`Self::Errored`]).
    ///
    /// # Naming convention — `take_` prefix (DEF-138)
    ///
    /// The `take_` prefix follows Rust-stdlib convention for
    /// consuming-extraction methods (`Option::take`, `Vec::drain`,
    /// `core::mem::take`). The `self` by-value receiver already
    /// signals consumption, but the prefix makes the side-effect
    /// explicit at every call site: readers see
    /// `state.take_inflight_reply_raw_id()` and immediately know
    /// the returned Option represents a drained state — the
    /// carried `ReplyId<_>` has been `.consume()`d inside the match.
    ///
    /// # Tier-1 invariant
    ///
    /// Exhaustive match over every variant: adding a variant that
    /// carries a `ReplyId<_>` without routing it here is a build
    /// failure. Centralises the "every in-flight reply has exactly
    /// one consume-site on the tear-down path" rule in one place —
    /// previously open-coded inside `fail_inflight_and_close`.
    ///
    /// # 1c-5 blocker (audit2 A029)
    ///
    /// The `Option<NonZeroU64>` return carries AT MOST ONE
    /// correlator. Single-inflight invariant holds today — every
    /// non-Idle variant carries exactly one `ReplyId<K>`. Pipelining
    /// introduces multi-correlator states (multiple concurrent
    /// replies over one connection): the return type must widen to
    /// `heapless::Vec<NonZeroU64, N_INFLIGHT>` at 1c-5 time. Revisit
    /// per H021 witness-guard session.
    #[must_use]
    pub(crate) fn take_inflight_reply_raw_id(self) -> Option<core::num::NonZeroU64> {
        // DEF-186 P1-6 (audit 2026-04-24): the `Errored(_) => None` arm
        // is correct under single-inflight: an Errored variant carries
        // only the StateErrorKind discriminator byte, no `ReplyId<K>`.
        // At 1c-5 pipelining the return type widens to a Vec of
        // correlators, and the Errored arm must enumerate any
        // post-error in-flight replies that survived the transition.
        // Until then this `None` is correct (no embedded reply to
        // surface), but the trigger to re-audit is the type widening
        // itself — H021 witness-guard session.
        match self {
            Self::Idle | Self::DrainRfqAfterError | Self::Errored(_) => None,
            Self::PingAwaitingRfq(id) => Some(id.consume()),
            Self::ConnectingStartupTrust { reply }
            | Self::ConnectingStartupScram { reply, .. }
            | Self::ConnectingScramAwaitingServerFirst { reply, .. }
            | Self::ConnectingScramAwaitingServerFinal { reply, .. }
            | Self::ConnectingScramAwaitingAuthOk(reply)
            | Self::ConnectingPostAuthAwaitingKey(reply)
            | Self::ConnectingPostAuthHaveKey { reply, .. } => Some(reply.consume()),
            Self::SimpleQueryAwaitingFirstResponse(id) => Some(id.consume()),
            Self::SimpleQueryStreamingRows { reply, .. }
            | Self::SimpleQueryAwaitingRfq { reply, .. }
            | Self::BindExecuteAwaitingBindCompleteSelect { reply, .. }
            | Self::BindExecuteAwaitingDataOrCompleteSelect { reply, .. }
            | Self::BindExecuteStreamingRows { reply, .. }
            | Self::BindExecuteAwaitingRfqSelect { reply, .. }
            | Self::BindExecuteAwaitingRfqDml { reply, .. } => Some(reply.consume()),
            Self::BindExecuteAwaitingBindCompleteDml(reply)
            | Self::BindExecuteAwaitingCommandCompleteDml(reply) => Some(reply.consume()),
            Self::ParseAwaitingParseComplete(reply) | Self::ParseAwaitingRfq(reply) => {
                Some(reply.consume())
            }
            Self::DescribeStatementAwaitingParamDesc(reply)
            | Self::DescribeStatementAwaitingRowDescOrNoData { reply, .. }
            | Self::DescribeStatementAwaitingRfq { reply, .. } => Some(reply.consume()),
            Self::DescribePortalAwaitingRowDescOrNoData(reply)
            | Self::DescribePortalAwaitingRfq { reply, .. } => Some(reply.consume()),
        }
    }

    /// DEF-146: classify the current state for push-command dispatch.
    ///
    /// Pre-DEF-146, each of the 7 `compute_push_*` helpers in
    /// `protocol.rs` enumerated the same ~18 `ProtoState` variants in
    /// or-patterns to group them into the failure classes a push
    /// targets (CommandInProgress / StartupAlreadyInProgress). Adding
    /// a new `ProtoState` variant required synchronised edits in all
    /// 7 helpers.
    ///
    /// Post-DEF-146, the enumeration lives in ONE place (this method).
    /// Each `compute_push_*` matches the 5-variant [`StatePushClass`]
    /// exhaustively — no `_` fallback, so tier-1 compile shield
    /// preserved. Adding a new `ProtoState` variant requires exactly
    /// one edit (here) plus whatever per-helper logic the new variant
    /// needs.
    ///
    /// # Classes
    ///
    /// - [`StatePushClass::Idle`] — happy-path, accepts any command.
    /// - [`StatePushClass::Errored`] — terminal; carry `prior_kind`
    ///   for `ConnectionAlreadyClosed` emission.
    /// - [`StatePushClass::Connecting`] — any `Connecting*` variant
    ///   during startup handshake.
    /// - [`StatePushClass::PingAwaiting`] — the pre-ready `PingAwaitingRfq`.
    ///   Separate from Connecting because different commands classify
    ///   it differently: Ping/SimpleQuery/Parse/etc. see it as
    ///   "command in flight" (→ CommandInProgress); Startup groups
    ///   it with Connecting (→ StartupAlreadyInProgress) because from
    ///   Startup's perspective the connection is already past the
    ///   startup phase.
    /// - [`StatePushClass::BusyQuery`] — any post-startup in-flight
    ///   state: SimpleQuery/Parse/Describe/BindExecute/DrainRfqAfterError.
    ///
    /// # Tier
    ///
    /// Exhaustive match over every `ProtoState` variant — adding a
    /// variant without classifying it is a build error.
    ///
    /// # DEF-178 (audit2 A038) — `#[inline]` hint
    ///
    /// 7 hot call sites (compute_push_*) with a monomorphic ~25-line
    /// match body. `#[inline]` signals the inliner without forcing
    /// it; LLVM usually inlines anyway, but the hint helps with
    /// build-unit crossings when the crate becomes a dep.
    #[inline]
    #[must_use]
    pub(crate) const fn push_class(&self) -> StatePushClass {
        match self {
            Self::Idle => StatePushClass::Idle,
            Self::Errored(kind) => StatePushClass::Errored(*kind),
            Self::PingAwaitingRfq(_) => StatePushClass::PingAwaiting,
            Self::ConnectingStartupTrust { .. }
            | Self::ConnectingStartupScram { .. }
            | Self::ConnectingScramAwaitingServerFirst { .. }
            | Self::ConnectingScramAwaitingServerFinal { .. }
            | Self::ConnectingScramAwaitingAuthOk(_)
            | Self::ConnectingPostAuthAwaitingKey(_)
            | Self::ConnectingPostAuthHaveKey { .. } => StatePushClass::Connecting,
            Self::SimpleQueryAwaitingFirstResponse(_)
            | Self::SimpleQueryStreamingRows { .. }
            | Self::SimpleQueryAwaitingRfq { .. }
            | Self::DrainRfqAfterError
            | Self::ParseAwaitingParseComplete(_)
            | Self::ParseAwaitingRfq(_)
            | Self::BindExecuteAwaitingBindCompleteDml(_)
            | Self::BindExecuteAwaitingCommandCompleteDml(_)
            | Self::BindExecuteAwaitingRfqDml { .. }
            | Self::BindExecuteAwaitingBindCompleteSelect { .. }
            | Self::BindExecuteAwaitingDataOrCompleteSelect { .. }
            | Self::BindExecuteStreamingRows { .. }
            | Self::BindExecuteAwaitingRfqSelect { .. }
            | Self::DescribeStatementAwaitingParamDesc(_)
            | Self::DescribeStatementAwaitingRowDescOrNoData { .. }
            | Self::DescribeStatementAwaitingRfq { .. }
            | Self::DescribePortalAwaitingRowDescOrNoData(_)
            | Self::DescribePortalAwaitingRfq { .. } => StatePushClass::BusyQuery,
        }
    }
}

/// DEF-210 SR-03 (audit 2026-04-28): classifier output for
/// [`ProtoState::unsolicited_admit`]. Single source of truth for
/// "is this state allowed to accept an unsolicited `ParameterStatus`
/// or `NoticeResponse` frame?" — replaces the prior pair of
/// independent exhaustive matches in `protocol.rs`
/// (`allows_unsolicited_param_status` / `..._notice_response`)
/// which had identical state-lists but no compile-level guarantee
/// of synchronisation. With this struct, both bools come from one
/// match arm — drift between classifiers is structurally impossible.
///
/// Today the two bools always agree; the struct preserves the
/// distinction so a future PG-spec divergence (e.g. allowing
/// `NoticeResponse` in a pre-auth state but not `ParameterStatus`)
/// can be expressed by editing one match arm without re-introducing
/// the parallel-classifier drift surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnsolicitedAdmit {
    /// `true` iff an unsolicited `ParameterStatus` frame in this
    /// state should be silently consumed (current PG transactions
    /// can carry late session-param updates) rather than classified
    /// as `UnexpectedFrame`.
    pub allow_param_status: bool,
    /// `true` iff an unsolicited `NoticeResponse` frame in this
    /// state should be silently consumed (notices flow through to
    /// the wrapper's async logging channel) rather than classified
    /// as `UnexpectedFrame`. Pre-auth states reject to avoid
    /// operator-log contamination by attacker-controlled text.
    pub allow_notice_response: bool,
}

impl ProtoState {
    /// DEF-210 SR-03: single exhaustive classifier for unsolicited
    /// `ParameterStatus` / `NoticeResponse` admittance, replacing the
    /// pair of identical exhaustive matches in `protocol.rs`. Adding
    /// a new `ProtoState` variant fails the build here until the
    /// contributor decides both bools.
    #[inline]
    #[must_use]
    pub(crate) const fn unsolicited_admit(&self) -> UnsolicitedAdmit {
        // Pre-auth `Connecting*` states + terminal `Errored` reject
        // both PS and NR. NR rejection avoids operator-log contamination
        // by attacker-controlled text (PG §48.5 permissive-but-client-
        // tightened policy). PS rejection follows the same trust-only-
        // post-auth principle.
        //
        // All post-startup states (Idle, Ping*, SimpleQuery*, Parse*,
        // BindExecute*, Describe*, DrainRfqAfterError) accept both.
        match self {
            Self::Idle
            | Self::PingAwaitingRfq(_)
            | Self::ConnectingPostAuthAwaitingKey(_)
            | Self::ConnectingPostAuthHaveKey { .. }
            | Self::SimpleQueryAwaitingFirstResponse(_)
            | Self::SimpleQueryStreamingRows { .. }
            | Self::SimpleQueryAwaitingRfq { .. }
            | Self::DrainRfqAfterError
            | Self::ParseAwaitingParseComplete(_)
            | Self::ParseAwaitingRfq(_)
            | Self::BindExecuteAwaitingBindCompleteDml(_)
            | Self::BindExecuteAwaitingCommandCompleteDml(_)
            | Self::BindExecuteAwaitingRfqDml { .. }
            | Self::BindExecuteAwaitingBindCompleteSelect { .. }
            | Self::BindExecuteAwaitingDataOrCompleteSelect { .. }
            | Self::BindExecuteStreamingRows { .. }
            | Self::BindExecuteAwaitingRfqSelect { .. }
            | Self::DescribeStatementAwaitingParamDesc(_)
            | Self::DescribeStatementAwaitingRowDescOrNoData { .. }
            | Self::DescribeStatementAwaitingRfq { .. }
            | Self::DescribePortalAwaitingRowDescOrNoData(_)
            | Self::DescribePortalAwaitingRfq { .. } => UnsolicitedAdmit {
                allow_param_status: true,
                allow_notice_response: true,
            },
            Self::ConnectingStartupTrust { .. }
            | Self::ConnectingStartupScram { .. }
            | Self::ConnectingScramAwaitingServerFirst { .. }
            | Self::ConnectingScramAwaitingServerFinal { .. }
            | Self::ConnectingScramAwaitingAuthOk(_)
            | Self::Errored(_) => UnsolicitedAdmit {
                allow_param_status: false,
                allow_notice_response: false,
            },
        }
    }
}

/// DEF-146: classifier output for [`ProtoState::push_class`].
///
/// Used by the 7 `compute_push_*` helpers in `protocol.rs` to decide
/// what `FailReply.cause` to emit on a non-Idle push. Each helper's
/// exhaustive match on `StatePushClass` replaces the pre-DEF-146
/// per-variant or-patterns (B002).
///
/// Exhaustive variants — no `Other` / catch-all. Adding a new
/// `ProtoState` variant requires classifying it inside
/// [`ProtoState::push_class`] (build error if forgotten).
///
/// # DEF-178 (audit2 A005) — classifier carries a payload asymmetry
///
/// `Errored(StateErrorKind)` is the one variant carrying a payload;
/// the other four (Idle / Connecting / PingAwaiting / BusyQuery)
/// are ZST-discriminators. The payload flows the `prior_kind`
/// through to `ConnectionAlreadyClosed` emission WITHOUT a second
/// state match.
///
/// A future refactor that "cleans up" the asymmetry (making
/// StatePushClass a pure discriminator, re-matching state inside
/// each arm to extract prior_kind) would REGRESS the tier: the
/// re-match would need `if let Err = state { ... }` shape, which is
/// not exhaustive at the match level — a new Errored-like variant
/// could silently slip through. The current design is an
/// intentional tier-1 consolidation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatePushClass {
    /// Connection ready to accept new commands.
    Idle,
    /// Terminal — connection has been torn down. Carry the prior
    /// `StateErrorKind` for `ConnectionAlreadyClosed` emission.
    Errored(StateErrorKind),
    /// Any `Connecting*` variant during startup handshake.
    Connecting,
    /// `PingAwaitingRfq` — post-startup Ping response pending.
    /// Separated from `Connecting` / `BusyQuery` because callers
    /// disagree on whether to classify it as `CommandInProgress` or
    /// `StartupAlreadyInProgress` — see [`ProtoState::push_class`]
    /// docstring.
    PingAwaiting,
    /// Any post-startup in-flight state: `SimpleQuery*`, `Parse*`,
    /// `Describe*`, `BindExecute*`, or `DrainRfqAfterError`.
    BusyQuery,
}

impl core::fmt::Debug for ProtoState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Idle => f.write_str("Idle"),
            Self::PingAwaitingRfq(id) => write!(f, "PingAwaitingRfq({id:?})"),
            Self::ConnectingStartupTrust { reply } => f
                .debug_struct("ConnectingStartupTrust")
                .field("reply", reply)
                .finish(),
            Self::ConnectingStartupScram { reply, .. } => f
                .debug_struct("ConnectingStartupScram")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ConnectingScramAwaitingServerFirst { reply, .. } => f
                .debug_struct("ConnectingScramAwaitingServerFirst")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ConnectingScramAwaitingServerFinal { reply, .. } => f
                .debug_struct("ConnectingScramAwaitingServerFinal")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ConnectingScramAwaitingAuthOk(id) => {
                write!(f, "ConnectingScramAwaitingAuthOk({id:?})")
            }
            Self::ConnectingPostAuthAwaitingKey(id) => {
                write!(f, "ConnectingPostAuthAwaitingKey({id:?})")
            }
            Self::ConnectingPostAuthHaveKey { reply, pid, .. } => f
                .debug_struct("ConnectingPostAuthHaveKey")
                .field("reply", reply)
                .field("pid", pid)
                .finish_non_exhaustive(),
            Self::SimpleQueryAwaitingFirstResponse(id) => {
                write!(f, "SimpleQueryAwaitingFirstResponse({id:?})")
            }
            Self::SimpleQueryStreamingRows { reply, .. } => f
                .debug_struct("SimpleQueryStreamingRows")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::SimpleQueryAwaitingRfq { reply, command_tag } => f
                .debug_struct("SimpleQueryAwaitingRfq")
                .field("reply", reply)
                .field("command_tag", command_tag)
                .finish(),
            Self::BindExecuteAwaitingBindCompleteDml(id) => {
                write!(f, "BindExecuteAwaitingBindCompleteDml({id:?})")
            }
            Self::BindExecuteAwaitingCommandCompleteDml(id) => {
                write!(f, "BindExecuteAwaitingCommandCompleteDml({id:?})")
            }
            Self::BindExecuteAwaitingRfqDml { reply, command_tag } => f
                .debug_struct("BindExecuteAwaitingRfqDml")
                .field("reply", reply)
                .field("command_tag", command_tag)
                .finish(),
            Self::BindExecuteAwaitingBindCompleteSelect { reply, .. } => f
                .debug_struct("BindExecuteAwaitingBindCompleteSelect")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::BindExecuteAwaitingDataOrCompleteSelect { reply, .. } => f
                .debug_struct("BindExecuteAwaitingDataOrCompleteSelect")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::BindExecuteStreamingRows { reply, .. } => f
                .debug_struct("BindExecuteStreamingRows")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::BindExecuteAwaitingRfqSelect { reply, command_tag, .. } => f
                .debug_struct("BindExecuteAwaitingRfqSelect")
                .field("reply", reply)
                .field("command_tag", command_tag)
                .finish_non_exhaustive(),
            Self::DrainRfqAfterError => {
                f.write_str("DrainRfqAfterError")
            }
            Self::ParseAwaitingParseComplete(id) => {
                write!(f, "ParseAwaitingParseComplete({id:?})")
            }
            Self::ParseAwaitingRfq(id) => write!(f, "ParseAwaitingRfq({id:?})"),
            Self::DescribeStatementAwaitingParamDesc(id) => {
                write!(f, "DescribeStatementAwaitingParamDesc({id:?})")
            }
            Self::DescribeStatementAwaitingRowDescOrNoData { reply, .. } => f
                .debug_struct("DescribeStatementAwaitingRowDescOrNoData")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::DescribeStatementAwaitingRfq { reply, .. } => f
                .debug_struct("DescribeStatementAwaitingRfq")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::DescribePortalAwaitingRowDescOrNoData(id) => {
                write!(f, "DescribePortalAwaitingRowDescOrNoData({id:?})")
            }
            Self::DescribePortalAwaitingRfq { reply, .. } => f
                .debug_struct("DescribePortalAwaitingRfq")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::Errored(kind) => write!(f, "Errored({kind:?})"),
        }
    }
}

#[cfg(test)]
mod push_class_tests {
    //! DEF-169 (audit2 A003): per-variant pinning for the
    //! [`ProtoState::push_class`] classifier introduced by DEF-146.
    //!
    //! The exhaustive-match shield on `push_class` proves that every
    //! `ProtoState` variant is classified; it does NOT prove the
    //! classification is CORRECT. A swap at an arm body (e.g.,
    //! `ConnectingStartupTrust { .. } => StatePushClass::BusyQuery`
    //! instead of `Connecting`) compiles because every arm returns
    //! the same type. Without a per-variant table, such a drift
    //! would silently change the `FailReply` cause emitted on
    //! pushes against that state.
    //!
    //! Category (1) per reforge.md §4.11 — mirror of
    //! `protocol.rs::allows_unsolicited_param_status_tests::policy_per_variant`
    //! for its sibling classifier.
    //!
    //! # Forbid-bundle compliance
    //!
    //! `panic!`, `unwrap()`, `expect()`, `unreachable!()` all banned
    //! crate-wide. Fixture construction uses the `assert!(is_some) +
    //! unwrap_or(fallback)` idiom where needed; `nz()` asserts on
    //! `0` before falling back to `MIN`. RowDesc fixtures use
    //! `RowDesc::EMPTY` as the test-only zero-column sentinel.
    //!
    //! # Coverage
    //!
    //! Every ProtoState variant appears once. Adding a new variant
    //! requires updating `push_class` (build failure if forgotten)
    //! AND this test (the new variant needs an explicit assertion).
    //! The combination closes both halves: "every variant is
    //! classified" AND "every variant has the CORRECT classification."

    use super::*;
    use crate::error::{BoundedStr, ErrorKind};
    use crate::password::Password;
    use crate::reply_id::ReplyId;
    use crate::scram::session::ScramSession;
    use crate::scram::types::SecretDigest;
    use crate::sensitive::Sensitive;
    use core::num::NonZeroU64;

    fn nz(n: u64) -> NonZeroU64 {
        // DEF-145: assert on 0, forbid-bundle-safe fallback.
        assert!(n > 0, "nz(0) is a test bug — use nz(1..) for non-zero test correlators");
        NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN)
    }

    /// Consume the ReplyId carried by a state so Drop-guard doesn't
    /// trip at scope end. Delegates to `take_inflight_reply_raw_id`.
    fn consume_state(state: ProtoState) {
        match state.take_inflight_reply_raw_id() {
            Some(_) | None => {}
        }
    }

    // Helper: classify + consume + return the class the test expects.
    // The `expected` argument is checked against `state.push_class()`;
    // `state` is then consumed via `consume_state`.
    fn pin(state: ProtoState, expected: StatePushClass) {
        let actual = state.push_class();
        assert_eq!(
            actual, expected,
            "push_class classification drift: {state:?} expected {expected:?}, got {actual:?}",
        );
        consume_state(state);
    }

    /// Invariant (tier-1 shield for DEF-146): every ProtoState variant
    /// maps to exactly the StatePushClass declared here.
    #[test]
    fn every_variant_pinned() {
        // ─── Idle ───
        pin(ProtoState::Idle, StatePushClass::Idle);

        // ─── PingAwaiting ───
        pin(
            ProtoState::PingAwaitingRfq(ReplyId::from_raw(nz(1_001))),
            StatePushClass::PingAwaiting,
        );

        // ─── Connecting* (7 variants) ───
        pin(
            ProtoState::ConnectingStartupTrust {
                reply: ReplyId::from_raw(nz(2_001)),
            },
            StatePushClass::Connecting,
        );
        // A10/B22 revert 2026-04-24: SCRAM variants carry inline
        // data (tier-1 variant-carries-field restoration).
        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let scram = alloc::boxed::Box::new(ScramSession::from_password(Sensitive::new(pw)));
            pin(
                ProtoState::ConnectingStartupScram {
                    reply: ReplyId::from_raw(nz(2_002)),
                    scram,
                },
                StatePushClass::Connecting,
            );
        }
        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let scram = alloc::boxed::Box::new(ScramSession::from_password(Sensitive::new(pw)));
            pin(
                ProtoState::ConnectingScramAwaitingServerFirst {
                    reply: ReplyId::from_raw(nz(2_003)),
                    scram,
                    client_first_bare: alloc::boxed::Box::new(crate::ident::PodBytes::new()),
                    client_nonce_b64: alloc::boxed::Box::new(crate::ident::PodBytes::new()),
                },
                StatePushClass::Connecting,
            );
        }
        pin(
            ProtoState::ConnectingScramAwaitingServerFinal {
                reply: ReplyId::from_raw(nz(2_004)),
                expected_server_sig: SecretDigest::new([0_u8; 32]),
            },
            StatePushClass::Connecting,
        );
        pin(
            ProtoState::ConnectingScramAwaitingAuthOk(ReplyId::from_raw(nz(2_005))),
            StatePushClass::Connecting,
        );
        pin(
            ProtoState::ConnectingPostAuthAwaitingKey(ReplyId::from_raw(nz(2_006))),
            StatePushClass::Connecting,
        );
        pin(
            ProtoState::ConnectingPostAuthHaveKey {
                reply: ReplyId::from_raw(nz(2_007)),
                pid: 1,
                secret_key: crate::sensitive::Sensitive::new(1_i32),
            },
            StatePushClass::Connecting,
        );

        // ─── SimpleQuery* (4 variants — incl. DrainRfqAfterError) ───
        pin(
            ProtoState::SimpleQueryAwaitingFirstResponse(ReplyId::from_raw(nz(3_001))),
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::SimpleQueryStreamingRows {
                reply: ReplyId::from_raw(nz(3_002)),
            },
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::SimpleQueryAwaitingRfq {
                reply: ReplyId::from_raw(nz(3_003)),
                command_tag: BoundedStr::default(),
            },
            StatePushClass::BusyQuery,
        );
        pin(ProtoState::DrainRfqAfterError, StatePushClass::BusyQuery);

        // ─── Parse* (2 variants) ───
        pin(
            ProtoState::ParseAwaitingParseComplete(ReplyId::from_raw(nz(4_001))),
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::ParseAwaitingRfq(ReplyId::from_raw(nz(4_002))),
            StatePushClass::BusyQuery,
        );

        // ─── BindExecute* (7 variants: 3 DML + 4 SELECT) ───
        pin(
            ProtoState::BindExecuteAwaitingBindCompleteDml(ReplyId::from_raw(nz(5_001))),
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::BindExecuteAwaitingCommandCompleteDml(ReplyId::from_raw(nz(5_002))),
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::BindExecuteAwaitingRfqDml {
                reply: ReplyId::from_raw(nz(5_003)),
                command_tag: BoundedStr::default(),
            },
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::BindExecuteAwaitingBindCompleteSelect {
                reply: ReplyId::from_raw(nz(5_004)),
            },
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::BindExecuteAwaitingDataOrCompleteSelect {
                reply: ReplyId::from_raw(nz(5_005)),
            },
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::BindExecuteStreamingRows {
                reply: ReplyId::from_raw(nz(5_006)),
            },
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::BindExecuteAwaitingRfqSelect {
                reply: ReplyId::from_raw(nz(5_007)),
                command_tag: BoundedStr::default(),
            },
            StatePushClass::BusyQuery,
        );

        // ─── Describe* (5 variants: 3 Statement + 2 Portal) ───
        pin(
            ProtoState::DescribeStatementAwaitingParamDesc(ReplyId::from_raw(nz(6_001))),
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::DescribeStatementAwaitingRowDescOrNoData {
                reply: ReplyId::from_raw(nz(6_002)),
                param_oids: crate::action::ParamOids::EMPTY,
            },
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::DescribeStatementAwaitingRfq {
                reply: ReplyId::from_raw(nz(6_003)),
                param_oids: crate::action::ParamOids::EMPTY,
            },
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::DescribePortalAwaitingRowDescOrNoData(ReplyId::from_raw(nz(6_004))),
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::DescribePortalAwaitingRfq {
                reply: ReplyId::from_raw(nz(6_005)),
            },
            StatePushClass::BusyQuery,
        );

        // ─── Errored ───
        let errored_kind = StateErrorKind::from_kind_or_internal(ErrorKind::Framing);
        pin(
            ProtoState::Errored(errored_kind),
            StatePushClass::Errored(errored_kind),
        );
    }
}
