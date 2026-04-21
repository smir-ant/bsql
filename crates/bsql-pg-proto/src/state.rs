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

use crate::action::{DescribedRows, ParamOids};
use crate::decode::RowDesc;
use crate::error::BoundedStr;
use crate::error::StateErrorKind;
use crate::ident::PodBytes;
use crate::reply_id::{
    DescribePortalKind, DescribeStatementKind, ParseKind, PingKind, QueryKind, ReplyId,
    StartupKind,
};
use crate::scram::session::ScramSession;
use crate::scram::types::SecretDigest;

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
    ConnectingStartupScram {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// SCRAM session (the password the user provided). Tier-1
        /// typestate via [`ScramSession`] — `Credentials::Trust`
        /// cannot reach this variant by construction.
        scram: ScramSession,
    },

    /// SCRAM step 1 complete (client-first sent); awaiting
    /// `AuthenticationSASLContinue` (server-first-message). DEF-002.
    ConnectingScramAwaitingServerFirst {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// SCRAM session (password bundle). Tier-1 typestate via
        /// [`ScramSession`] — the `Credentials::Trust` variant
        /// cannot appear here by construction (audit A2).
        scram: ScramSession,
        /// The `client-first-message-bare` (saved for AuthMessage).
        /// Capacity pinned to [`crate::scram::wire::MAX_CLIENT_FIRST_BARE_LEN`]
        /// (DEF-095 const-generic drift guard). POD buffer — no
        /// `heapless::Vec` Drop propagation into the state enum
        /// (DEF-099).
        client_first_bare: PodBytes<{ crate::scram::wire::MAX_CLIENT_FIRST_BARE_LEN }>,
        /// The client nonce (base64-encoded, for prefix validation).
        /// Capacity pinned to [`crate::scram::wire::MAX_CLIENT_NONCE_B64_LEN`].
        client_nonce_b64: PodBytes<{ crate::scram::wire::MAX_CLIENT_NONCE_B64_LEN }>,
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
    ConnectingPostAuthHaveKey {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// The backend process ID.
        pid: i32,
        /// The backend secret key (for cancel requests).
        secret_key: i32,
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
    /// Terminal transitions: `DataRow` → emit `Action::StreamRow`,
    /// stay here; `CommandComplete` → [`Self::SimpleQueryAwaitingRfq`]
    /// with the parsed command tag.
    ///
    /// F19: carries the parsed `RowDesc` inline. Entering this variant
    /// ONLY via the 'T' dispatcher arm (which parses `RowDescription`)
    /// makes "StreamingRows implies schema" a tier-2 structural
    /// invariant — the variant shape itself requires a schema. Prior
    /// design held the schema in a separate `PgProtocol.row_desc` slot,
    /// leaving state-and-slot as two parallel facts that could drift
    /// (tier-3 audit pairing).
    SimpleQueryStreamingRows {
        /// Correlator for the in-flight query.
        reply: ReplyId<QueryKind>,
        /// Result-set schema parsed from `RowDescription`. Copied into
        /// each staged `StreamRowRange` on every `DataRow` frame; copied
        /// into `AwaitingRfq` on `CommandComplete`.
        row_desc: RowDesc,
    },

    /// `CommandComplete` or `EmptyQueryResponse` received; awaiting
    /// the trailing `ReadyForQuery`. The command tag captured at `C`
    /// (empty for `EmptyQueryResponse`) ships in the final
    /// [`crate::Reply::QueryComplete`] payload.
    ///
    /// F19: carries `row_desc: Option<RowDesc>` — `Some(desc)` when
    /// entered from `SimpleQueryStreamingRows` (SELECT path, schema
    /// preserved through terminal transitions), `None` when entered
    /// from `SimpleQueryAwaitingFirstResponse` via `CommandComplete`
    /// (DML path: no RowDescription was received) or
    /// `EmptyQueryResponse` (empty query: ditto). Preserves the
    /// public-API distinction "0-row SELECT (Some(empty)) vs DML (None)".
    SimpleQueryAwaitingRfq {
        /// Correlator for the in-flight query.
        reply: ReplyId<QueryKind>,
        /// Command tag — `"SELECT 5"`, `"INSERT 0 3"`, or empty
        /// for empty-query responses. Capacity 32 bytes handles
        /// PG's documented tag shapes (the longest standard tag,
        /// `"INSERT <oid> <n>"` with 10-digit values, is ~23 bytes).
        command_tag: BoundedStr<32>,
        /// Schema from the SELECT path (`Some`) or absence marker
        /// (`None`) from DML / empty-query paths.
        row_desc: Option<RowDesc>,
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
    /// `row_desc`. The schema is threaded through to
    /// [`Self::BindExecuteStreamingRows`] once `'2'` arrives.
    BindExecuteAwaitingBindCompleteSelect {
        /// Correlator for the in-flight command.
        reply: ReplyId<QueryKind>,
        /// User-supplied schema, required (variant shape guarantees
        /// its presence — tier-1 structural).
        row_desc: RowDesc,
    },

    /// `BindComplete` received on the schema-bearing path; awaiting
    /// either a `DataRow` (transition to [`Self::BindExecuteStreamingRows`])
    /// or `CommandComplete` (0-row SELECT, transition to
    /// [`Self::BindExecuteAwaitingRfqSelect`]). Schema threads through.
    BindExecuteAwaitingDataOrCompleteSelect {
        /// Correlator for the in-flight command.
        reply: ReplyId<QueryKind>,
        /// Schema (guaranteed present by variant shape — tier-1).
        row_desc: RowDesc,
    },

    /// Streaming `DataRow` frames on the schema-bearing path.
    /// Mirrors [`Self::SimpleQueryStreamingRows`] — schema required
    /// by variant shape.
    BindExecuteStreamingRows {
        /// Correlator for the in-flight command.
        reply: ReplyId<QueryKind>,
        /// Schema (guaranteed present by variant shape).
        row_desc: RowDesc,
    },

    /// `CommandComplete` received on the schema-bearing path;
    /// awaiting the trailing `ReadyForQuery`. Terminal reply
    /// carries `row_desc: Some(schema)`.
    BindExecuteAwaitingRfqSelect {
        /// Correlator for the in-flight command.
        reply: ReplyId<QueryKind>,
        /// Command tag parsed from the `C` frame body.
        command_tag: BoundedStr<32>,
        /// Schema to ship in the terminal `QueryComplete` reply.
        row_desc: RowDesc,
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
    DescribeStatementAwaitingRfq {
        /// Correlator for the Describe command.
        reply: ReplyId<DescribeStatementKind>,
        /// Parameter OIDs captured at the `'t'` transition.
        param_oids: ParamOids,
        /// Rows-or-no-data captured at the `'T'` / `'n'` transition.
        rows: DescribedRows,
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
    DescribePortalAwaitingRfq {
        /// Correlator for the Describe command.
        reply: ReplyId<DescribePortalKind>,
        /// Rows-or-no-data captured at the `'T'` / `'n'` transition.
        rows: DescribedRows,
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
    #[must_use]
    pub(crate) fn take_inflight_reply_raw_id(self) -> Option<core::num::NonZeroU64> {
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
            Self::SimpleQueryAwaitingRfq { reply, command_tag, row_desc } => f
                .debug_struct("SimpleQueryAwaitingRfq")
                .field("reply", reply)
                .field("command_tag", command_tag)
                .field("row_desc_is_some", &row_desc.is_some())
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
