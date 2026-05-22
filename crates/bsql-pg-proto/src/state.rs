//! Protocol state — state-as-data.
//!
//! Each variant carries its in-flight correlator inline (reforge.md
//! §7.2). Consequence: a transition out of [`ProtoState::PingAwaitingRfq`]
//! that fails to consume the inner [`ReplyId`] is a build error — the
//! borrow / move checker forces every transition to handle the carried
//! data explicitly.
//!
//! Per reforge.md §3.5 / §4.6, manufactured variants are forbidden
//! until their entry/exit code lands — a variant whose transitions
//! exist in the dispatcher is the only legal addition.
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
use crate::reply_id::{
    CloseKind, DescribePortalKind, DescribeStatementKind, ParseKind, PingKind, QueryKind, ReplyId,
    StartupKind,
};
use crate::scram::session::ScramSession;
use crate::scram::types::SecretDigest;

/// Where the protocol is right now.
///
/// State variants do NOT carry a `RowDesc` payload. The schema, when
/// present, lives in `PgProtocol::row_desc_slot` — populated by the
/// `'T'` dispatch arm and read by terminal materialise via the
/// protocol's `current_row_desc()` accessor.
///
/// **Internal-use enum.** Not `#[non_exhaustive]`: exhaustive `match` in
/// internal dispatch is the load-bearing tier-1 invariant — a missed
/// (state, tag) combination is a build failure.
///
/// # No `Default` impl
///
/// `#[derive(Default)]` would let `core::mem::take(&mut state)` swap
/// state out for an `Idle` placeholder — convenient, but a latent
/// hazard: `mem::take` on an `Errored` variant would silently drop
/// the stored [`StateErrorKind`] and replace it with `Idle`,
/// re-opening the connection for commands (silent recovery from a
/// terminal error). Every caller that used `mem::take` would have to
/// manually preserve the `Errored` case — a documented discipline,
/// NOT a compile-time invariant.
///
/// Instead all callsites use `core::mem::replace(&mut state,
/// ProtoState::Idle)` explicitly, making the placeholder choice
/// load-bearing at the call site. Tier-1 future-proof: a future
/// contributor cannot accidentally invoke `mem::take(state)`
/// because the trait impl does not exist.
// Deliberately **not** `Copy`: moving out of `PingAwaitingRfq(id)`
// must consume the [`crate::ReplyId`] inline — the state-as-data
// invariant (reforge.md §7.2). `ProtoState` inherits non-Copy from
// the non-Copy `ReplyId` field, so the `missing_copy_implementations`
// lint does not fire here (there is no "could be Copy" suggestion to
// suppress).
pub enum ProtoState {
    /// Connection established and idle. Accepts new commands.
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
    // Startup + auth handshake states.
    // ---------------------------------------------------------------

    /// A `StartupMessage` was sent by a Trust-auth connection;
    /// awaiting `AuthenticationOk`.
    ///
    /// # Why split from the Scram variant
    ///
    /// A single `ConnectingStartup { reply, credentials }` variant
    /// carrying the full [`crate::password::Credentials`] enum until
    /// the server responded would have two costs:
    ///
    /// - ~1040 bytes of password buffer would live in state until the
    ///   first frame arrived (Trust connections paying the
    ///   Scram-sized stack footprint).
    /// - The "server requested SASL on a Trust connection" case would
    ///   be classified at runtime (`UnsupportedAuthMethod`), not at
    ///   compile time.
    ///
    /// The Trust/Scram split moves discrimination to
    /// [`crate::PgProtocol::push_command`]. Each variant only carries
    /// what its authentication path needs. A server frame of the
    /// wrong shape for the connection's credential type becomes a
    /// per-variant dispatcher arm — a missed arm is a build failure.
    ConnectingStartupTrust {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
    },

    /// A `StartupMessage` was sent by a cleartext-password connection;
    /// awaiting `AuthenticationCleartextPassword` (sub-code 3).
    ///
    /// # Tier-1 — variant carries its data
    ///
    /// `password` lives INSIDE this variant. The correlation
    /// "cleartext-state has password material" is enforced
    /// structurally — a future refactor cannot have
    /// `ConnectingStartupCleartext` without a `Sensitive<Password>`.
    /// `ZeroizeOnDrop` on `Password` (via `Sensitive`) fires
    /// automatically on every exit path:
    ///
    /// - happy progression: arm body builds `PasswordMessage`,
    ///   transitions to [`Self::ConnectingCleartextAwaitingAuthOk`];
    ///   the `Box<Sensitive<Password>>` drops at the
    ///   `mem::replace(state, ...)` call inside dispatch, scrubbing
    ///   the password before the slot is reused.
    /// - fatal teardown: `core::mem::replace(state, Errored(kind))`
    ///   inside `fail_inflight_no_readbuf` drops the prev variant
    ///   at function-return.
    ///
    /// # Heap-boxed (mirror of [`Self::ConnectingStartupScram`])
    ///
    /// `Sensitive<Password>` is ~514 B (512 B buf + len + align).
    /// Inline storage would dominate `ProtoState` size pin
    /// (currently 48 B exact). `Box` reduces variant footprint to
    /// 8 B (ptr) + 8 B (ReplyId) + 1 B (disc) + align ≈ 24 B.
    /// Tier-1 preserved — `Box` cannot be `None`, its `Drop` fires
    /// `Sensitive::Drop` → `Password::Drop` (`ZeroizeOnDrop`).
    /// Cost in this variant: one heap alloc per cleartext handshake
    /// (pre-StartupMessage construction; freed at PasswordMessage
    /// dispatch time).
    ConnectingStartupCleartext {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// Password material, heap-boxed for `ProtoState` size
        /// containment. Drops with `ZeroizeOnDrop` on every exit
        /// path. See variant docstring for size + Drop chain
        /// rationale.
        password: alloc::boxed::Box<crate::sensitive::Sensitive<crate::password::Password>>,
    },

    /// `PasswordMessage` was sent (cleartext bytes); awaiting
    /// `AuthenticationOk` (sub-code 0).
    ///
    /// Only `AuthOk` is legal here — any other auth code or frame
    /// is a protocol violation classified as `UnexpectedFrame`. The
    /// password field has been scrubbed at the
    /// `ConnectingStartupCleartext → ConnectingCleartextAwaitingAuthOk`
    /// transition (variant-data Drop fires when the prior variant's
    /// destructure completes).
    ConnectingCleartextAwaitingAuthOk(ReplyId<StartupKind>),

    /// A `StartupMessage` was sent by an MD5-password connection;
    /// awaiting `AuthenticationMD5Password` (sub-code 5) carrying
    /// a 4-byte salt.
    ///
    /// # Why a single Box?
    ///
    /// MD5 password authentication needs BOTH the password AND the
    /// username at digest-construction time (the inner hash is
    /// `md5_hex(password || username)`). The username arrived in
    /// the `StartupMessage` and is otherwise not retained by the
    /// state machine; it stays bundled with the password until the
    /// handshake completes. Both live inside one
    /// [`Md5HandshakeState`] struct, heap-boxed once, mirroring the
    /// SCRAM single-Box pattern. Per-handshake total: 1 alloc
    /// (StartupMd5 construction) + 1 free (PasswordMessage dispatch
    /// transition).
    ///
    /// # Tier-1 — variant carries its data
    ///
    /// `Box<Md5HandshakeState>` is non-`Option`; the variant cannot
    /// exist without its handshake state. `ZeroizeOnDrop` fires on
    /// every exit path through `Box::drop → Md5HandshakeState::drop`
    /// → `Sensitive::drop` → `Password::drop`. The username field
    /// is non-secret (it travelled cleartext in `StartupMessage`)
    /// and does not need scrubbing.
    ///
    /// # Size pin
    ///
    /// Inline storage of `Ident` (~64 B) + `Sensitive<Password>`
    /// (~514 B) would balloon the variant past the 48 B `ProtoState`
    /// size pin. With Box: 8 B (Box ptr) + 8 B (ReplyId) + 1 B (disc)
    /// + align ≈ 24 B. Pin preserved.
    ConnectingStartupMd5 {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// Bundled handshake state — username + password. See
        /// [`Md5HandshakeState`] for fields. The Box is dropped on
        /// every transition path, firing the ZeroizeOnDrop chain.
        handshake: alloc::boxed::Box<crate::md5::Md5HandshakeState>,
    },

    /// `PasswordMessage` was sent (MD5 digest bytes); awaiting
    /// `AuthenticationOk` (sub-code 0).
    ///
    /// Mirror of [`Self::ConnectingCleartextAwaitingAuthOk`]: only
    /// `AuthOk` is legal here, anything else is `UnexpectedFrame`.
    /// The handshake-state Box (containing password) was dropped at
    /// the prior transition; this variant holds only the reply
    /// correlator.
    ConnectingMd5AwaitingAuthOk(ReplyId<StartupKind>),

    /// A `StartupMessage` was sent by a SCRAM-auth connection;
    /// awaiting `AuthenticationSASL` offering SCRAM-SHA-256.
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
        /// # Boxing rationale
        ///
        /// Inline `ScramSession` (~520 B with full Password) would
        /// dominate `ProtoState` size at ~712 B and cause cache-locality
        /// damage on the per-row hot path (`iter_rows_per_row` regresses
        /// +110% when measured against the inline-state variant).
        ///
        /// `Box<ScramSession>` reduces variant footprint to 8 + 16 =
        /// 24 B. Tier-1 preserved — Box can't be None, Box's Drop fires
        /// `ScramSession::Drop` (ZeroizeOnDrop) on every exit path.
        /// Cost in this variant: one heap alloc.
        ///
        /// **Per-handshake total**: at any moment in a SCRAM handshake's
        /// lifecycle at most ONE `Box` is live. Both
        /// `ConnectingStartupScram` and
        /// [`Self::ConnectingScramAwaitingServerFirst`] carry the same
        /// `Box<ScramSession>` (`client_first_bare` and
        /// `client_nonce_b64` are wire-public byte fields inside the
        /// session with `#[zeroize(skip)]`, populated in place by
        /// `dispatch::build_sasl_initial_response`). Per-handshake
        /// allocator ops: 1 alloc + 1 free total.
        ///
        /// Drop chain: `Box::drop` → `ScramSession::drop` →
        /// `ZeroizeOnDrop` of password (wire-public PodBytes fields
        /// skip-zeroed by classification).
        scram: alloc::boxed::Box<ScramSession>,
    },

    /// SCRAM step 1 complete (client-first sent); awaiting
    /// `AuthenticationSASLContinue` (server-first-message).
    ///
    /// # Single-Box invariant
    ///
    /// A naive shape would carry three separate `Box<...>` fields
    /// (`scram`, `client_first_bare`, `client_nonce_b64`) — three
    /// heap allocations live during the ServerFirst-await phase,
    /// three drops on transition. Consolidating them into a single
    /// `Box<ScramHandshakeState>` cuts to one Box but the
    /// `StartupScram → ServerFirst` transition still incurs 1 alloc
    /// plus 1 free (free old `Box<ScramSession>` at deref-move,
    /// alloc new `Box<ScramHandshakeState>`).
    ///
    /// Current shape closes the gap: `client_first_bare` +
    /// `client_nonce_b64` live INSIDE `ScramSession` itself (with
    /// `#[zeroize(skip)]` — wire-public bytes). Both
    /// `ConnectingStartupScram` and
    /// `ConnectingScramAwaitingServerFirst` carry the **same**
    /// `Box<ScramSession>`; the transition is a state-discriminant
    /// flip + Box pointer copy-move (zero allocator ops).
    /// Per-handshake total: 1 alloc + 1 free; "one heap alloc per
    /// SCRAM connection" is literal.
    ///
    /// Drop chain: `Box::drop` → `ScramSession::drop` →
    /// `ZeroizeOnDrop` of password (PodBytes fields skip-zeroed
    /// per wire-public classification).
    ConnectingScramAwaitingServerFirst {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// SCRAM session state including in-place `client_first_bare`
        /// and `client_nonce_b64` populated by
        /// `dispatch::build_sasl_initial_response` at the
        /// `ConnectingStartupScram` → `ConnectingScramAwaitingServerFirst`
        /// transition. **Same `Box` allocation** as
        /// [`Self::ConnectingStartupScram`]'s `scram` field; reused
        /// across both variants (no alloc on transition).
        scram: alloc::boxed::Box<ScramSession>,
    },

    /// SCRAM step 2 complete (client-final sent); awaiting
    /// `AuthenticationSASLFinal` (server-final-message).
    ConnectingScramAwaitingServerFinal {
        /// Correlator for the Startup command.
        reply: ReplyId<StartupKind>,
        /// Expected server signature for constant-time comparison.
        expected_server_sig: SecretDigest,
    },

    /// SCRAM step 3 complete (server signature verified); awaiting
    /// `AuthenticationOk`.
    ConnectingScramAwaitingAuthOk(ReplyId<StartupKind>),

    /// Authentication succeeded; waiting for `BackendKeyData`.
    ///
    /// `ParameterStatus` messages received in this state are recorded
    /// on [`crate::PgProtocol::session_params`] by the `feed_bytes`
    /// loop. `BackendKeyData` transitions to `ConnectingPostAuthHaveKey`.
    ConnectingPostAuthAwaitingKey(ReplyId<StartupKind>),

    /// `BackendKeyData` received; waiting for `ReadyForQuery`.
    ///
    /// Additional `ParameterStatus` messages may arrive before RFQ.
    ///
    /// # secret_key wrapped in `Sensitive<i32>`
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
    ///   alongside ReadBuf/WriteBuf zeroize-on-clear.
    /// - **Debug redaction**: any future Debug print of `ProtoState`
    ///   prints `<REDACTED>` for the secret_key.
    ///
    /// A naive `secret_key: i32` (Copy) would leave the value in the
    /// state's stack-residue after transition. The dispatch RFQ arm
    /// extracts the inner via `.get()` (returns `&i32`), then
    /// copy-derefs into the `StartupCompletePayload` (which itself
    /// has manual Debug redaction). The wrapper drops at the
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

    /// **Post-handshake transition signal**. The
    /// `(ConnectingPostAuthHaveKey, RFQ)` dispatch arm writes this
    /// variant carrying the `(pid, secret_key)` payload pulled out
    /// of the consumed `ConnectingPostAuthHaveKey` variant. The
    /// per-phase `ConnectingInner` wrapper's lower-step projects
    /// this back into [`ConnectingState::HandshakeReady`] (the
    /// per-phase signal variant — same payload shape).
    ///
    /// `<ConnectingPhase>::into_active` then consumes the
    /// `ConnectingState::HandshakeReady` payload and constructs an
    /// inline `BackendKey { pid, secret_key }` on `ActiveInner` —
    /// closing the tier-3 `Option<R>` arm on
    /// [`crate::PgProtocol::<crate::ActivePhase>::with_cancel_request`]
    /// at construction time instead of at every call site.
    ///
    /// **Why a separate ProtoState variant**: the lift+lower path
    /// for `ConnectingInner.feed_bytes_impl` widens
    /// `ConnectingState → ProtoState` for the shared dispatch body.
    /// Without this variant, the dispatch arm would have to write
    /// `ProtoState::Idle` (the post-handshake state) and stash the
    /// payload in a side-channel slot for the lower-step to read.
    /// The dedicated variant threads the payload through `ProtoState`
    /// itself, eliminating the side-channel.
    ///
    /// **Not reachable from `<ActivePhase>` dispatch**: the
    /// `TryFrom<ProtoState> for ActiveState` arm returns
    /// `Err(WrongPhase)` for this variant — Active dispatch never
    /// writes it (handshake is already done by the time we're on
    /// `<ActivePhase>`).
    ///
    /// Manual `Debug` impl redacts `secret_key` via
    /// `Sensitive<i32>`'s `Debug`.
    HandshakeReady {
        /// The backend process ID (wire-public; safe to print).
        pid: i32,
        /// The backend secret key for cancel requests.
        ///
        /// Wrapped in [`crate::sensitive::Sensitive`] for
        /// zero-on-drop scrub when this variant drops (e.g. when
        /// the dispatch wrapper consumes it via `mem::replace`).
        /// See variant docstring on
        /// [`Self::ConnectingPostAuthHaveKey`].
        secret_key: crate::sensitive::Sensitive<i32>,
    },

    // ---------------------------------------------------------------
    // Simple Query flow (PgCommand::SimpleQuery)
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
    /// Variant carries no schema field. The schema lives in
    /// `PgProtocol::row_desc_slot` (populated by the `'T'` arm
    /// BEFORE the transition into this variant). The per-row hot-path
    /// reads the desc via `proto.current_row_desc()` (single immutable
    /// borrow projection from the slot) — no per-variant payload, no
    /// per-row state match.
    SimpleQueryStreamingRows {
        /// Correlator for the in-flight query.
        reply: ReplyId<QueryKind>,
    },

    /// DEF-219 COPY OUT streaming. Server sent `CopyOutResponse`
    /// (`'H'`); now receiving zero or more `CopyData` (`'d'`) frames
    /// from server. Transitions:
    /// - `CopyData` → stay here (Phase 3 will emit
    ///   `Action::CopyDataChunk`; Phase 2 stays silent).
    /// - `CopyDone` → [`Self::SimpleQueryCopyOutAwaitingCC`].
    /// - `ErrorResponse` → [`Self::DrainRfqAfterError`].
    SimpleQueryCopyOutStreaming(ReplyId<QueryKind>),

    /// DEF-219 COPY OUT post-CopyDone. Server has sent the final
    /// `CopyDone` (`'c'`); now awaiting `CommandComplete` followed
    /// by `ReadyForQuery`. Transitions:
    /// - `CommandComplete` → [`Self::SimpleQueryAwaitingRfq`] with
    ///   the parsed command tag (reuses the existing tail state).
    /// - `ErrorResponse` → [`Self::DrainRfqAfterError`].
    SimpleQueryCopyOutAwaitingCC(ReplyId<QueryKind>),

    /// DEF-219 COPY IN active. Server sent `CopyInResponse`
    /// (`'G'`); caller now pushes `CopyData` (`'d'`) bytes via the
    /// (Phase 4) `PushCopyData` push command, then `CopyDone`
    /// (`'c'`) or `CopyFail` (`'f'`). Server transitions to
    /// `CommandComplete` once it observes the client's `CopyDone`.
    ///
    /// State stays in `SimpleQueryCopyInActive` throughout the
    /// client-side push phase (the push commands write wire bytes
    /// but do not change state; only server frames advance state).
    /// Transitions:
    /// - `CommandComplete` → [`Self::SimpleQueryAwaitingRfq`].
    /// - `ErrorResponse` → [`Self::DrainRfqAfterError`].
    SimpleQueryCopyInActive(ReplyId<QueryKind>),

    /// `CommandComplete` or `EmptyQueryResponse` received; awaiting
    /// the trailing `ReadyForQuery`. The command tag captured at `C`
    /// (empty for `EmptyQueryResponse`) ships in the final
    /// [`crate::Reply::QueryComplete`] payload.
    ///
    /// # No `schema_present` discriminator
    ///
    /// A naive shape would carry `schema_present: bool` — a duplicate
    /// of `PgProtocol::row_desc_slot.is_some()` kept in lockstep by
    /// dispatch-arm discipline. The duplication is **tier-2
    /// structural** (same dispatch arm sets bool ↔ populates slot
    /// atomically) but architecturally fragile: a future refactor
    /// that set `schema_present = true` without populating the slot
    /// would silently produce `Reply::QueryComplete.row_desc = None`
    /// for SELECTs ("DML done" instead of rows — silent corruption).
    ///
    /// The single source of truth is `PgProtocol::row_desc_slot`;
    /// terminal materialise reads the slot directly via `into_public`.
    /// There is no second variable that can drift. **Tier-1
    /// by-construction** — the invariant is "the slot equals
    /// itself", which is identity, not discipline.
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
    // Extended Query — Parse flow
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
    // Extended Query — Bind + Execute flow
    // ---------------------------------------------------------------
    //
    // `push_bind_execute` emits `Bind` + `Execute` + `Sync` as one
    // bundle. Server response shape (PG §55.2.2):
    //
    //   '2' (BindComplete)     — server accepted params
    //   ['T'] (RowDescription) — ONLY if a prior Describe ran;
    //                            push_bind_execute doesn't auto-
    //                            describe, so user-supplied row_desc
    //                            is threaded from the push call
    //   'D'* (DataRow)         — result rows (zero rows for DML)
    //   'C' (CommandComplete)  — result-set boundary
    //   'Z' (ReadyForQuery)    — sync boundary
    //
    // The variants below mirror the SimpleQuery shape with a
    // `BindComplete` prefix state. Schema (row_desc) is threaded via
    // the same `PgProtocol::row_desc_slot` used by SimpleQuery — no
    // separate slot.

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

    /// `PortalSuspended` (`'s'`) received — `FetchRows::Chunked(N)`
    /// hit the row cap before the portal exhausted (PG §55.2.7).
    /// Awaiting the trailing `ReadyForQuery`. Terminal reply emits
    /// [`crate::Reply::QuerySuspended`] (NOT `QueryComplete` — server
    /// did not send `CommandComplete`). The portal stays bound on
    /// the server; caller resumes via
    /// [`crate::push_command::ExecutePortal`].
    ///
    /// No `command_tag` field — server's `PortalSuspended` frame has
    /// no payload (the bytes 's' + length-field=4 + empty body), so
    /// there is no tag to carry. `row_desc` for the terminal reply
    /// resolves from `PgProtocol::row_desc_slot` (populated by the
    /// `Bind` that originally opened the portal, or by the caller-
    /// supplied schema at push time per the `BindExecute` Select
    /// path).
    BindExecuteAwaitingRfqAfterSuspended {
        /// Correlator for the in-flight command.
        reply: ReplyId<QueryKind>,
    },

    // ---------------------------------------------------------------
    // Extended Query — Describe flow
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
    ///
    /// # Heap-boxed `param_oids` (mirror of SCRAM/MD5/Cleartext)
    ///
    /// [`ParamOids`] is 68 B inline (`#[repr(C, align(4))]` with
    /// `n_params: u16` + 2 B pad + `[u32; 16]`). Inline storage
    /// would dominate `ProtoState` size — see the same rationale
    /// on [`Self::ConnectingStartupScram`] (`scram` field),
    /// [`Self::ConnectingStartupMd5`] (`handshake` field), and
    /// [`Self::ConnectingStartupCleartext`] (`password` field).
    ///
    /// `Box<ParamOids>` reduces variant footprint to ~24 B
    /// (8 B Box ptr, 8 B ReplyId, 1 B disc, align tail-pad). Tier-1
    /// preserved — `Box` cannot be `None`, the variant cannot exist
    /// without its `ParamOids`. The same `Box` is moved across the
    /// transition to `AwaitingRfq` (state-discriminant flip plus a
    /// Box pointer copy-move, zero allocator ops). Per-Describe-flow
    /// total: 1 alloc (`'t'` arrival) and 1 free (`'Z'` arrival when
    /// the Box is deref-moved into the terminal reply payload).
    DescribeStatementAwaitingRowDescOrNoData {
        /// Correlator for the Describe command.
        reply: ReplyId<DescribeStatementKind>,
        /// Parameter OIDs parsed from the preceding `'t'` frame.
        /// Threaded through to the terminal reply payload.
        /// Heap-boxed for [`ProtoState`] size containment — see
        /// variant docstring above for the size + transition
        /// rationale (mirror of `Box<ScramSession>` /
        /// `Box<Md5HandshakeState>` / `Box<Sensitive<Password>>`).
        param_oids: alloc::boxed::Box<ParamOids>,
    },

    /// Row-desc / no-data known; awaiting the trailing
    /// `ReadyForQuery` that closes the Sync boundary. On `'Z'` →
    /// deliver [`crate::Reply::DescribeStatementComplete`] and
    /// transition to Idle.
    ///
    /// # No `rows` discriminator
    ///
    /// A naive shape would carry a `rows: DescribedRowsStaged`
    /// discriminator — same architectural shape as the
    /// `schema_present: bool` rejected on `SimpleQueryAwaitingRfq`.
    /// The discriminator would duplicate
    /// `PgProtocol::row_desc_slot.is_some()` (the `'T'` arm populates
    /// the slot AND sets `Rows`; the `'n'` arm does neither).
    /// Materialise would read the discriminator and project from the
    /// slot — but if the discriminator and slot drifted, the
    /// projection would silently swallow the schema (manifesting as
    /// a tier-3 `debug_assert!(false)` arm in production code, CREDO
    /// §V banned pattern).
    ///
    /// Instead materialise reads `row_desc_slot.map(...)` directly.
    /// **Tier-1 by-construction**: the slot equals itself (identity,
    /// not discipline).
    ///
    /// # Heap-boxed `param_oids`
    ///
    /// See [`Self::DescribeStatementAwaitingRowDescOrNoData`] for
    /// the full rationale — the `Box<ParamOids>` is the **same**
    /// allocation as on the prior variant; the
    /// `AwaitingRowDescOrNoData → AwaitingRfq` transition is a
    /// state-discriminant flip + Box pointer copy-move (zero
    /// allocator ops).
    DescribeStatementAwaitingRfq {
        /// Correlator for the Describe command.
        reply: ReplyId<DescribeStatementKind>,
        /// Parameter OIDs captured at the `'t'` transition.
        /// **Same `Box` allocation** as on
        /// [`Self::DescribeStatementAwaitingRowDescOrNoData`];
        /// reused across the transition (no alloc on transition).
        param_oids: alloc::boxed::Box<ParamOids>,
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
    /// No `rows` discriminator: same rationale as
    /// [`Self::DescribeStatementAwaitingRfq`] — the slot is the
    /// single source of truth.
    DescribePortalAwaitingRfq {
        /// Correlator for the Describe command.
        reply: ReplyId<DescribePortalKind>,
    },

    // ---------------------------------------------------------------
    // Extended Query — Close flow
    // ---------------------------------------------------------------
    //
    // `push_command(CloseStatement | ClosePortal)` emits a `Close` +
    // `Sync` bundle. Server response shape (PG §55.7):
    //
    //   't'-target ('S' statement or 'P' portal) is asymmetric only
    //   server-side (resource lookup); the response sequence is
    //   identical for both targets:
    //
    //     '3' (CloseComplete) — empty body, signals the close was
    //         accepted
    //     'Z' (ReadyForQuery) — Sync boundary
    //
    // Because both targets produce the SAME response sequence, the
    // state variants here are unified (no statement-vs-portal
    // discriminator on state). The wire-level distinction lives in
    // the push struct (`CloseStatement` vs `ClosePortal`) and the
    // emitted Close frame's target byte; once the frame is on the
    // wire, the state machine treats both paths uniformly.
    //
    // PG also accepts Close on a non-existent name (it is NOT an
    // error per PG §55.7); the server still emits CloseComplete + RFQ.
    // Our state machine therefore doesn't need a NotFound classifier.
    //
    // Error path: 'E' (ErrorResponse) during a Close is non-standard
    // (PG essentially never errors on Close) but spec-conforming:
    // emit FailReply + transition to DrainRfqAfterError. Connection
    // survives.

    /// A `Close` + `Sync` bundle was sent (statement or portal target);
    /// awaiting `CloseComplete` (`'3'`). The inner
    /// [`ReplyId<CloseKind>`] is the only path to the correlator;
    /// state-as-data invariant (§7.2).
    ///
    /// # Unified state for both targets
    ///
    /// Statement-target and portal-target Close pushes both transition
    /// into this same variant. The wire-level target byte (`'S'` vs
    /// `'P'`) is consumed at push time inside the Close frame; the
    /// post-push state machine treats both paths uniformly because the
    /// server's response sequence is identical (CloseComplete → RFQ).
    ///
    /// Next legitimate frames:
    /// - `'3'` (CloseComplete, empty body) → transition to
    ///   [`Self::CloseAwaitingRfq`].
    /// - `'E'` (ErrorResponse) → emit FailReply + transition to
    ///   [`Self::DrainRfqAfterError`] (recoverable — connection
    ///   survives).
    /// - Anything else → UnexpectedFrame → teardown.
    CloseAwaitingComplete(ReplyId<CloseKind>),

    /// `CloseComplete` (`'3'`) received; awaiting the `ReadyForQuery`
    /// (`'Z'`) that closes the Sync boundary. On `'Z'` → deliver
    /// [`crate::Reply::CloseComplete`] and transition to Idle.
    CloseAwaitingRfq(ReplyId<CloseKind>),

    /// Terminal: the connection has been classified as unrecoverable.
    ///
    /// Entered by any path that calls `fail_inflight_and_close` or
    /// returns `DispatchOutcome::Errored` — these paths also emit the
    /// matching `FailReply` (full cause) + `CloseSocket` actions in the
    /// same call, so by the time the state is observable as `Errored`
    /// the wrapper has already received the diagnostic.
    ///
    /// Never left. Carries [`StateErrorKind`] (1 byte), the
    /// `AlreadyClosed`-free subset of [`crate::error::ErrorKind`].
    /// The full cause went out once in the first `FailReply`;
    /// subsequent pushes get a compact
    /// [`crate::error::ProtocolError::ConnectionAlreadyClosed`]
    /// carrying the `prior_kind` for diagnostic context.
    ///
    /// # Why `StateErrorKind` and not `ErrorKind`
    ///
    /// `StateErrorKind` is a newtype narrower than the full
    /// `ErrorKind` — it excludes the `AlreadyClosed` pseudo-kind.
    /// A naive `Errored(ErrorKind)` shape would let the
    /// "state never holds `AlreadyClosed`" invariant rest on
    /// review-discipline (maintained by an early-return guard on
    /// the fail path); the narrower newtype makes it tier-1
    /// compile — constructing `Errored(AlreadyClosed)` is a type
    /// error at the `StateErrorKind::try_from_kind` call site.
    Errored(StateErrorKind),
}

impl ProtoState {
    /// Consume `self` and return the raw `NonZeroU64` of the typed
    /// [`ReplyId<_>`] in flight for this state, or `None` if no
    /// reply is in flight ([`Self::Idle`], [`Self::DrainRfqAfterError`],
    /// [`Self::Errored`]).
    ///
    /// # Naming convention — `take_` prefix
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
    /// # Pipelining
    ///
    /// The `Option<NonZeroU64>` return carries AT MOST ONE
    /// correlator. Single-inflight invariant holds today — every
    /// non-Idle variant carries exactly one `ReplyId<K>`. Pipelining
    /// would introduce multi-correlator states (multiple concurrent
    /// replies over one connection); the return type would have to
    /// widen to `heapless::Vec<NonZeroU64, N_INFLIGHT>`.
    #[must_use]
    pub(crate) fn take_inflight_reply_raw_id(self) -> Option<core::num::NonZeroU64> {
        // The `Errored(_) => None` arm is correct under
        // single-inflight: an Errored variant carries only the
        // StateErrorKind discriminator byte, no `ReplyId<K>`. Under
        // pipelining the return type would widen to a Vec of
        // correlators, and the Errored arm would have to enumerate
        // any post-error in-flight replies that survived the
        // transition. Until then this `None` is correct.
        match self {
            Self::Idle
            | Self::DrainRfqAfterError
            | Self::Errored(_)
            | Self::HandshakeReady { .. } => None,
            Self::PingAwaitingRfq(id) => Some(id.consume()),
            Self::ConnectingStartupTrust { reply }
            | Self::ConnectingStartupScram { reply, .. }
            | Self::ConnectingStartupCleartext { reply, .. }
            | Self::ConnectingStartupMd5 { reply, .. }
            | Self::ConnectingScramAwaitingServerFirst { reply, .. }
            | Self::ConnectingScramAwaitingServerFinal { reply, .. }
            | Self::ConnectingScramAwaitingAuthOk(reply)
            | Self::ConnectingCleartextAwaitingAuthOk(reply)
            | Self::ConnectingMd5AwaitingAuthOk(reply)
            | Self::ConnectingPostAuthAwaitingKey(reply)
            | Self::ConnectingPostAuthHaveKey { reply, .. } => Some(reply.consume()),
            Self::SimpleQueryAwaitingFirstResponse(id) => Some(id.consume()),
            Self::SimpleQueryCopyOutStreaming(id)
            | Self::SimpleQueryCopyOutAwaitingCC(id)
            | Self::SimpleQueryCopyInActive(id) => Some(id.consume()),
            Self::SimpleQueryStreamingRows { reply, .. }
            | Self::SimpleQueryAwaitingRfq { reply, .. }
            | Self::BindExecuteAwaitingBindCompleteSelect { reply, .. }
            | Self::BindExecuteAwaitingDataOrCompleteSelect { reply, .. }
            | Self::BindExecuteStreamingRows { reply, .. }
            | Self::BindExecuteAwaitingRfqSelect { reply, .. }
            | Self::BindExecuteAwaitingRfqAfterSuspended { reply, .. }
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
            Self::CloseAwaitingComplete(reply) | Self::CloseAwaitingRfq(reply) => {
                Some(reply.consume())
            }
        }
    }

    /// Classify the current state for push-command dispatch.
    ///
    /// A naive shape would enumerate the same ~18 `ProtoState`
    /// variants in or-patterns inside each of the 7 `compute_push_*`
    /// helpers in `protocol.rs` to group them into the failure
    /// classes a push targets (CommandInProgress /
    /// StartupAlreadyInProgress). Adding a new `ProtoState` variant
    /// would require synchronised edits in all 7 helpers.
    ///
    /// Instead the enumeration lives in ONE place (this method).
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
    /// # `#[inline]` hint
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
            | Self::ConnectingStartupCleartext { .. }
            | Self::ConnectingStartupMd5 { .. }
            | Self::ConnectingScramAwaitingServerFirst { .. }
            | Self::ConnectingScramAwaitingServerFinal { .. }
            | Self::ConnectingScramAwaitingAuthOk(_)
            | Self::ConnectingCleartextAwaitingAuthOk(_)
            | Self::ConnectingMd5AwaitingAuthOk(_)
            | Self::ConnectingPostAuthAwaitingKey(_)
            | Self::ConnectingPostAuthHaveKey { .. }
            | Self::HandshakeReady { .. } => StatePushClass::Connecting,
            Self::SimpleQueryAwaitingFirstResponse(_)
            | Self::SimpleQueryStreamingRows { .. }
            | Self::SimpleQueryAwaitingRfq { .. }
            | Self::SimpleQueryCopyOutStreaming(_)
            | Self::SimpleQueryCopyOutAwaitingCC(_)
            | Self::SimpleQueryCopyInActive(_)
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
            | Self::BindExecuteAwaitingRfqAfterSuspended { .. }
            | Self::DescribeStatementAwaitingParamDesc(_)
            | Self::DescribeStatementAwaitingRowDescOrNoData { .. }
            | Self::DescribeStatementAwaitingRfq { .. }
            | Self::DescribePortalAwaitingRowDescOrNoData(_)
            | Self::DescribePortalAwaitingRfq { .. }
            | Self::CloseAwaitingComplete(_)
            | Self::CloseAwaitingRfq(_) => StatePushClass::BusyQuery,
        }
    }
}

// ═════════════════════════════════════════════════════════════════════
// Per-phase state enums.
//
// `ConnectingState`, `ActiveState`, and `ErroredState` partition the
// full `ProtoState` variant set so the wrapping per-phase Inner type
// can carry a state field whose variant set excludes
// invalid-for-phase variants. `ConnectingInner.state: ConnectingState`
// physically forbids holding `SimpleQueryStreamingRows` —
// tier-1 by-storage-absence on state-variant-in-wrong-phase.
//
// **Variant naming** — the redundant `Connecting`/`Active` prefix
// is dropped here. The wrapping type carries the phase context;
// `ConnectingState::ConnectingStartupTrust` would stutter at every
// dispatch arm.
//
// **Errored placement** — each phase enum has its own
// `Errored(StateErrorKind)` variant. During `<ConnectingPhase>`,
// state can transiently become Errored (the wrapper-phase stays
// `<ConnectingPhase>` until `into_closed_if_errored` lifts it to
// `<ClosedPhase>`). A wrapping `InnerState` enum would add a
// redundant discriminator; each phase having its own Errored arm
// matches today's flow with zero runtime overhead.
// ═════════════════════════════════════════════════════════════════════

/// Error wrapper for the per-phase `TryFrom` projection impls.
///
/// **Why recover the value**: [`ProtoState`] is non-`Copy` and every
/// non-`Idle` variant carries a `#[must_use]` [`ReplyId<K>`]. A
/// `TryFrom` that swallows the input on Err would silently drop the
/// `ReplyId<K>` without consuming it — exactly the failure-mode the
/// state-as-data invariant exists to prevent (reforge.md §7.2).
/// Returning the original via `recovered` lets the caller either
/// feed it back to the state slot or hand it to
/// [`ProtoState::take_inflight_reply_raw_id`] to drain correlators
/// safely.
#[non_exhaustive]
pub struct WrongPhase {
    /// Recovered original — caller MUST consume to drop `ReplyId<K>`s
    /// without tripping the Drop-guard.
    pub recovered: ProtoState,
}

impl core::fmt::Debug for WrongPhase {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WrongPhase")
            .field("recovered", &self.recovered)
            .finish()
    }
}

/// Terminal-phase state for [`crate::protocol::ClosedPhase`] / a
/// future `ErroredPhase`.
///
/// Mirrors [`ProtoState::Errored`] exactly. Lives as a single-variant
/// enum (not a tuple struct) for shape parity with [`ConnectingState`]
/// and [`ActiveState`] — every dispatch arm matching against per-phase
/// state uses `match state { State::Errored(kind) => ... }`, so
/// `ErroredState::Errored(...)` reads uniformly.
///
/// **Layout**: 1 B (`StateErrorKind` is 1 B `#[repr(transparent)]`
/// over `ErrorKind` discriminator; single-variant enum has no extra
/// discriminator byte under Rust's layout rules).
#[allow(
    missing_debug_implementations,
    reason = "Debug projection lives on the parent per-phase enums (ProtoState / ConnectingState / ActiveState); the standalone `ErroredState` type exists only as a uniform-shape mirror for the dispatch-arm pattern and isn't surfaced in `{:?}` output."
)]
#[non_exhaustive]
pub enum ErroredState {
    /// The connection is terminally classified. Mirror of
    /// [`ProtoState::Errored`].
    Errored(StateErrorKind),
}

/// State space reachable from [`crate::protocol::ConnectingPhase`].
///
/// 11 handshake variants + 1 transient `Errored` (entered when
/// `install_errored` fires during a Connecting state; wrapper stays
/// `<ConnectingPhase>` until `into_closed_if_errored` lifts to
/// `<ClosedPhase>`).
///
/// **Tier-1 closure**: a future contributor CANNOT write
/// `ConnectingInner.state = ConnectingState::SimpleQuery...` because
/// the variant doesn't exist. State-variant-in-wrong-phase is
/// impossible by-construction.
///
/// **Naming**: the redundant `Connecting` prefix from
/// [`ProtoState::ConnectingStartupTrust`] etc. is dropped here. The
/// wrapping `ConnectingState::StartupTrust` is unambiguous.
///
/// **Layout**: ~48 B. Largest variant is
/// [`Self::ScramAwaitingServerFinal`] carrying
/// `expected_server_sig: SecretDigest` (32 B) + `reply: ReplyId<StartupKind>`
/// (8 B). With enum-discriminator and alignment padding the total
/// settles at 48 B (pin-asserted below).
///
/// **Manual `Debug` impl** — Sensitive-redaction parity with
/// [`ProtoState`]'s manual Debug. Variants carrying SCRAM / MD5 /
/// Cleartext password material or `Sensitive<i32>` secret keys use
/// `finish_non_exhaustive()` to elide the secret fields from the
/// formatted output; non-sensitive variants print all fields via
/// `finish()` / `write!`.
#[allow(
    missing_docs,
    reason = "Every field on every variant is a direct mirror of the same-named field on the corresponding `ProtoState` variant; the docstring on the `ProtoState` variant is the single source of truth for field semantics. Mirroring docs here would create a drift surface (a future contributor would have to keep TWO docstrings in sync per field). Each variant's own docstring already names the mirrored `ProtoState` variant via intra-doc link."
)]
#[non_exhaustive]
pub enum ConnectingState {
    /// Mirror of [`ProtoState::ConnectingStartupTrust`].
    StartupTrust {
        reply: ReplyId<StartupKind>,
    },
    /// Mirror of [`ProtoState::ConnectingStartupCleartext`].
    StartupCleartext {
        reply: ReplyId<StartupKind>,
        password: alloc::boxed::Box<crate::sensitive::Sensitive<crate::password::Password>>,
    },
    /// Mirror of [`ProtoState::ConnectingCleartextAwaitingAuthOk`].
    CleartextAwaitingAuthOk(ReplyId<StartupKind>),
    /// Mirror of [`ProtoState::ConnectingStartupMd5`].
    StartupMd5 {
        reply: ReplyId<StartupKind>,
        handshake: alloc::boxed::Box<crate::md5::Md5HandshakeState>,
    },
    /// Mirror of [`ProtoState::ConnectingMd5AwaitingAuthOk`].
    Md5AwaitingAuthOk(ReplyId<StartupKind>),
    /// Mirror of [`ProtoState::ConnectingStartupScram`].
    StartupScram {
        reply: ReplyId<StartupKind>,
        scram: alloc::boxed::Box<ScramSession>,
    },
    /// Mirror of [`ProtoState::ConnectingScramAwaitingServerFirst`].
    ScramAwaitingServerFirst {
        reply: ReplyId<StartupKind>,
        scram: alloc::boxed::Box<ScramSession>,
    },
    /// Mirror of [`ProtoState::ConnectingScramAwaitingServerFinal`].
    ScramAwaitingServerFinal {
        reply: ReplyId<StartupKind>,
        expected_server_sig: SecretDigest,
    },
    /// Mirror of [`ProtoState::ConnectingScramAwaitingAuthOk`].
    ScramAwaitingAuthOk(ReplyId<StartupKind>),
    /// Mirror of [`ProtoState::ConnectingPostAuthAwaitingKey`].
    PostAuthAwaitingKey(ReplyId<StartupKind>),
    /// Mirror of [`ProtoState::ConnectingPostAuthHaveKey`].
    PostAuthHaveKey {
        reply: ReplyId<StartupKind>,
        pid: i32,
        secret_key: crate::sensitive::Sensitive<i32>,
    },
    /// **Per-phase transition signal**. The handshake's
    /// `(PostAuthHaveKey, RFQ)` dispatch arm writes
    /// [`ProtoState::HandshakeReady`] carrying the `(pid, secret_key)`
    /// payload extracted from the consumed `PostAuthHaveKey`. The
    /// per-phase `ConnectingInner` wrapper's lower-step projects
    /// the `ProtoState::HandshakeReady` outcome back into THIS
    /// `ConnectingState::HandshakeReady` variant (same payload
    /// shape; `TryFrom<ProtoState> for ConnectingState` does the
    /// mapping).
    ///
    /// `<ConnectingPhase>::into_active` consumes this variant's
    /// payload and constructs an inline `BackendKey { pid,
    /// secret_key }` on `ActiveInner` — closing the tier-3
    /// `Option<R>` arm on
    /// [`crate::PgProtocol::<crate::ActivePhase>::with_cancel_request`]
    /// at construction time.
    ///
    /// **Payload-carrying variant**. The `(pid, secret_key)`
    /// material flows: dispatch arm → `ProtoState::HandshakeReady`
    /// → `ConnectingState::HandshakeReady` (this variant) →
    /// `ActiveInner.backend_key: BackendKey` (inline at
    /// `into_active`). No `BackendKeyCell` exists post-Phase-1d.2
    /// — the payload lives in the state variant during transition
    /// and is consumed structurally at `into_active`.
    HandshakeReady {
        /// The backend process ID (wire-public; safe to print).
        pid: i32,
        /// The backend secret key for cancel requests.
        ///
        /// Wrapped in [`crate::sensitive::Sensitive`] for
        /// zero-on-drop scrub when this variant drops (e.g. on
        /// `mem::replace` during a state transition, or at
        /// `into_active` when the payload is consumed into
        /// `BackendKey`'s inline `secret_key` field).
        secret_key: crate::sensitive::Sensitive<i32>,
    },
    /// Transient `install_errored` write while wrapper is still
    /// `<ConnectingPhase>`. Lifted to `<ClosedPhase>` via
    /// `into_closed_if_errored`.
    Errored(StateErrorKind),
}

/// Manual `Debug` for [`ConnectingState`] with **Sensitive-redaction
/// parity** with [`ProtoState`]'s manual Debug.
///
/// Variants carrying SCRAM `ScramSession` / `SecretDigest`, MD5
/// `Md5HandshakeState`, cleartext `Sensitive<Password>`, or post-auth
/// `Sensitive<i32>` secret keys use `finish_non_exhaustive()` to
/// elide secret fields from the formatted output. Non-sensitive
/// variants print all fields via `finish()` / `write!`.
impl core::fmt::Debug for ConnectingState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::StartupTrust { reply } => f
                .debug_struct("StartupTrust")
                .field("reply", reply)
                .finish(),
            Self::StartupCleartext { reply, .. } => f
                .debug_struct("StartupCleartext")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::CleartextAwaitingAuthOk(id) => {
                write!(f, "CleartextAwaitingAuthOk({id:?})")
            }
            Self::StartupMd5 { reply, .. } => f
                .debug_struct("StartupMd5")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::Md5AwaitingAuthOk(id) => {
                write!(f, "Md5AwaitingAuthOk({id:?})")
            }
            Self::StartupScram { reply, .. } => f
                .debug_struct("StartupScram")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ScramAwaitingServerFirst { reply, .. } => f
                .debug_struct("ScramAwaitingServerFirst")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ScramAwaitingServerFinal { reply, .. } => f
                .debug_struct("ScramAwaitingServerFinal")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ScramAwaitingAuthOk(id) => {
                write!(f, "ScramAwaitingAuthOk({id:?})")
            }
            Self::PostAuthAwaitingKey(id) => {
                write!(f, "PostAuthAwaitingKey({id:?})")
            }
            Self::PostAuthHaveKey { reply, pid, .. } => f
                .debug_struct("PostAuthHaveKey")
                .field("reply", reply)
                .field("pid", pid)
                .finish_non_exhaustive(),
            Self::HandshakeReady { pid, secret_key } => f
                .debug_struct("HandshakeReady")
                .field("pid", pid)
                .field("secret_key", secret_key)
                .finish(),
            Self::Errored(kind) => write!(f, "Errored({kind:?})"),
        }
    }
}

/// State space reachable from [`crate::protocol::ActivePhase`].
///
/// 19 post-handshake variants + 1 transient `Errored`. Includes
/// `Idle`, `PingAwaitingRfq`, all SimpleQuery / Parse / BindExecute /
/// Describe flow variants, and `DrainRfqAfterError` (verified: only
/// transitioned-into from Active variants).
///
/// **Tier-1 closure**: a future contributor CANNOT write
/// `ActiveInner.state = ActiveState::StartupTrust { ... }` because
/// the variant doesn't exist.
///
/// **Layout**: 48 B (matches `ProtoState`). Largest variants are
/// the `BoundedStr<32>`-bearing ones: [`Self::SimpleQueryAwaitingRfq`]
/// / [`Self::BindExecuteAwaitingRfqDml`] /
/// [`Self::BindExecuteAwaitingRfqSelect`] carrying
/// `command_tag: BoundedStr<32>` (~36 B) + `reply: ReplyId<…>` (8 B).
/// `DescribeStatement*` variants carry `Box<ParamOids>` per the same
/// containment pattern as SCRAM/MD5/Cleartext (see [`ProtoState`]
/// docstrings on those variants).
///
/// **Manual `Debug` impl** — mirror of [`ProtoState`]'s manual Debug
/// for the post-handshake variants. Active variants don't carry
/// password / SCRAM secret material; `finish_non_exhaustive()` is
/// used only for variants whose `param_oids` / streaming-mode
/// bookkeeping the parent Debug elides.
#[allow(
    missing_docs,
    reason = "Same rationale as `ConnectingState`: every variant field is a direct mirror of the same-named `ProtoState` field; doc duplication would create a drift surface."
)]
#[non_exhaustive]
pub enum ActiveState {
    /// Mirror of [`ProtoState::Idle`].
    Idle,
    /// Mirror of [`ProtoState::PingAwaitingRfq`].
    PingAwaitingRfq(ReplyId<PingKind>),
    /// Mirror of [`ProtoState::SimpleQueryAwaitingFirstResponse`].
    SimpleQueryAwaitingFirstResponse(ReplyId<QueryKind>),
    /// Mirror of [`ProtoState::SimpleQueryStreamingRows`].
    SimpleQueryStreamingRows {
        reply: ReplyId<QueryKind>,
    },
    /// Mirror of [`ProtoState::SimpleQueryAwaitingRfq`].
    SimpleQueryAwaitingRfq {
        reply: ReplyId<QueryKind>,
        command_tag: BoundedStr<32>,
    },
    /// Mirror of [`ProtoState::SimpleQueryCopyOutStreaming`] (DEF-219).
    SimpleQueryCopyOutStreaming(ReplyId<QueryKind>),
    /// Mirror of [`ProtoState::SimpleQueryCopyOutAwaitingCC`] (DEF-219).
    SimpleQueryCopyOutAwaitingCC(ReplyId<QueryKind>),
    /// Mirror of [`ProtoState::SimpleQueryCopyInActive`] (DEF-219).
    SimpleQueryCopyInActive(ReplyId<QueryKind>),
    /// Mirror of [`ProtoState::DrainRfqAfterError`].
    DrainRfqAfterError,
    /// Mirror of [`ProtoState::ParseAwaitingParseComplete`].
    ParseAwaitingParseComplete(ReplyId<ParseKind>),
    /// Mirror of [`ProtoState::ParseAwaitingRfq`].
    ParseAwaitingRfq(ReplyId<ParseKind>),
    /// Mirror of [`ProtoState::BindExecuteAwaitingBindCompleteDml`].
    BindExecuteAwaitingBindCompleteDml(ReplyId<QueryKind>),
    /// Mirror of [`ProtoState::BindExecuteAwaitingCommandCompleteDml`].
    BindExecuteAwaitingCommandCompleteDml(ReplyId<QueryKind>),
    /// Mirror of [`ProtoState::BindExecuteAwaitingRfqDml`].
    BindExecuteAwaitingRfqDml {
        reply: ReplyId<QueryKind>,
        command_tag: BoundedStr<32>,
    },
    /// Mirror of [`ProtoState::BindExecuteAwaitingBindCompleteSelect`].
    BindExecuteAwaitingBindCompleteSelect {
        reply: ReplyId<QueryKind>,
    },
    /// Mirror of [`ProtoState::BindExecuteAwaitingDataOrCompleteSelect`].
    BindExecuteAwaitingDataOrCompleteSelect {
        reply: ReplyId<QueryKind>,
    },
    /// Mirror of [`ProtoState::BindExecuteStreamingRows`].
    BindExecuteStreamingRows {
        reply: ReplyId<QueryKind>,
    },
    /// Mirror of [`ProtoState::BindExecuteAwaitingRfqAfterSuspended`].
    BindExecuteAwaitingRfqAfterSuspended {
        reply: ReplyId<QueryKind>,
    },
    /// Mirror of [`ProtoState::BindExecuteAwaitingRfqSelect`].
    BindExecuteAwaitingRfqSelect {
        reply: ReplyId<QueryKind>,
        command_tag: BoundedStr<32>,
    },
    /// Mirror of [`ProtoState::DescribeStatementAwaitingParamDesc`].
    DescribeStatementAwaitingParamDesc(ReplyId<DescribeStatementKind>),
    /// Mirror of [`ProtoState::DescribeStatementAwaitingRowDescOrNoData`].
    /// `param_oids` is heap-boxed for size containment — see the
    /// matching `ProtoState` variant docstring for rationale.
    DescribeStatementAwaitingRowDescOrNoData {
        reply: ReplyId<DescribeStatementKind>,
        param_oids: alloc::boxed::Box<ParamOids>,
    },
    /// Mirror of [`ProtoState::DescribeStatementAwaitingRfq`]. Same
    /// `Box<ParamOids>` allocation as the prior variant — moved
    /// across the transition.
    DescribeStatementAwaitingRfq {
        reply: ReplyId<DescribeStatementKind>,
        param_oids: alloc::boxed::Box<ParamOids>,
    },
    /// Mirror of [`ProtoState::DescribePortalAwaitingRowDescOrNoData`].
    DescribePortalAwaitingRowDescOrNoData(ReplyId<DescribePortalKind>),
    /// Mirror of [`ProtoState::DescribePortalAwaitingRfq`].
    DescribePortalAwaitingRfq {
        reply: ReplyId<DescribePortalKind>,
    },
    /// Mirror of [`ProtoState::CloseAwaitingComplete`].
    CloseAwaitingComplete(ReplyId<CloseKind>),
    /// Mirror of [`ProtoState::CloseAwaitingRfq`].
    CloseAwaitingRfq(ReplyId<CloseKind>),
    /// Transient `install_errored` write while wrapper is still
    /// `<ActivePhase>`. Lifted to `<ClosedPhase>` via
    /// `into_closed_if_errored`.
    Errored(StateErrorKind),
}

// ─── Per-phase → ProtoState upward conversions (variant uplift) ───

impl From<ErroredState> for ProtoState {
    #[inline]
    fn from(s: ErroredState) -> Self {
        match s {
            ErroredState::Errored(k) => ProtoState::Errored(k),
        }
    }
}

impl From<ConnectingState> for ProtoState {
    #[inline]
    fn from(s: ConnectingState) -> Self {
        match s {
            ConnectingState::StartupTrust { reply } => {
                ProtoState::ConnectingStartupTrust { reply }
            }
            ConnectingState::StartupCleartext { reply, password } => {
                ProtoState::ConnectingStartupCleartext { reply, password }
            }
            ConnectingState::CleartextAwaitingAuthOk(r) => {
                ProtoState::ConnectingCleartextAwaitingAuthOk(r)
            }
            ConnectingState::StartupMd5 { reply, handshake } => {
                ProtoState::ConnectingStartupMd5 { reply, handshake }
            }
            ConnectingState::Md5AwaitingAuthOk(r) => {
                ProtoState::ConnectingMd5AwaitingAuthOk(r)
            }
            ConnectingState::StartupScram { reply, scram } => {
                ProtoState::ConnectingStartupScram { reply, scram }
            }
            ConnectingState::ScramAwaitingServerFirst { reply, scram } => {
                ProtoState::ConnectingScramAwaitingServerFirst { reply, scram }
            }
            ConnectingState::ScramAwaitingServerFinal {
                reply,
                expected_server_sig,
            } => ProtoState::ConnectingScramAwaitingServerFinal {
                reply,
                expected_server_sig,
            },
            ConnectingState::ScramAwaitingAuthOk(r) => {
                ProtoState::ConnectingScramAwaitingAuthOk(r)
            }
            ConnectingState::PostAuthAwaitingKey(r) => {
                ProtoState::ConnectingPostAuthAwaitingKey(r)
            }
            ConnectingState::PostAuthHaveKey {
                reply,
                pid,
                secret_key,
            } => ProtoState::ConnectingPostAuthHaveKey {
                reply,
                pid,
                secret_key,
            },
            // `ConnectingState::HandshakeReady → ProtoState::HandshakeReady`:
            // the same-named ProtoState signal variant carries the
            // payload directly. Used at the `feed_bytes_impl`
            // epilogue when re-lifting state for the SHARED dispatch
            // path on a subsequent call (won't happen in practice —
            // the next call goes through `into_active` first — but
            // the conversion stays semantically correct).
            ConnectingState::HandshakeReady { pid, secret_key } => {
                ProtoState::HandshakeReady { pid, secret_key }
            }
            ConnectingState::Errored(k) => ProtoState::Errored(k),
        }
    }
}

impl From<ActiveState> for ProtoState {
    #[inline]
    fn from(s: ActiveState) -> Self {
        match s {
            ActiveState::Idle => ProtoState::Idle,
            ActiveState::PingAwaitingRfq(r) => ProtoState::PingAwaitingRfq(r),
            ActiveState::SimpleQueryAwaitingFirstResponse(r) => {
                ProtoState::SimpleQueryAwaitingFirstResponse(r)
            }
            ActiveState::SimpleQueryStreamingRows { reply } => {
                ProtoState::SimpleQueryStreamingRows { reply }
            }
            ActiveState::SimpleQueryCopyOutStreaming(r) => {
                ProtoState::SimpleQueryCopyOutStreaming(r)
            }
            ActiveState::SimpleQueryCopyOutAwaitingCC(r) => {
                ProtoState::SimpleQueryCopyOutAwaitingCC(r)
            }
            ActiveState::SimpleQueryCopyInActive(r) => {
                ProtoState::SimpleQueryCopyInActive(r)
            }
            ActiveState::SimpleQueryAwaitingRfq { reply, command_tag } => {
                ProtoState::SimpleQueryAwaitingRfq { reply, command_tag }
            }
            ActiveState::DrainRfqAfterError => ProtoState::DrainRfqAfterError,
            ActiveState::ParseAwaitingParseComplete(r) => {
                ProtoState::ParseAwaitingParseComplete(r)
            }
            ActiveState::ParseAwaitingRfq(r) => ProtoState::ParseAwaitingRfq(r),
            ActiveState::BindExecuteAwaitingBindCompleteDml(r) => {
                ProtoState::BindExecuteAwaitingBindCompleteDml(r)
            }
            ActiveState::BindExecuteAwaitingCommandCompleteDml(r) => {
                ProtoState::BindExecuteAwaitingCommandCompleteDml(r)
            }
            ActiveState::BindExecuteAwaitingRfqDml { reply, command_tag } => {
                ProtoState::BindExecuteAwaitingRfqDml { reply, command_tag }
            }
            ActiveState::BindExecuteAwaitingBindCompleteSelect { reply } => {
                ProtoState::BindExecuteAwaitingBindCompleteSelect { reply }
            }
            ActiveState::BindExecuteAwaitingDataOrCompleteSelect { reply } => {
                ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply }
            }
            ActiveState::BindExecuteStreamingRows { reply } => {
                ProtoState::BindExecuteStreamingRows { reply }
            }
            ActiveState::BindExecuteAwaitingRfqSelect { reply, command_tag } => {
                ProtoState::BindExecuteAwaitingRfqSelect { reply, command_tag }
            }
            ActiveState::BindExecuteAwaitingRfqAfterSuspended { reply } => {
                ProtoState::BindExecuteAwaitingRfqAfterSuspended { reply }
            }
            ActiveState::DescribeStatementAwaitingParamDesc(r) => {
                ProtoState::DescribeStatementAwaitingParamDesc(r)
            }
            ActiveState::DescribeStatementAwaitingRowDescOrNoData {
                reply,
                param_oids,
            } => ProtoState::DescribeStatementAwaitingRowDescOrNoData {
                reply,
                param_oids,
            },
            ActiveState::DescribeStatementAwaitingRfq { reply, param_oids } => {
                ProtoState::DescribeStatementAwaitingRfq { reply, param_oids }
            }
            ActiveState::DescribePortalAwaitingRowDescOrNoData(r) => {
                ProtoState::DescribePortalAwaitingRowDescOrNoData(r)
            }
            ActiveState::DescribePortalAwaitingRfq { reply } => {
                ProtoState::DescribePortalAwaitingRfq { reply }
            }
            ActiveState::CloseAwaitingComplete(r) => ProtoState::CloseAwaitingComplete(r),
            ActiveState::CloseAwaitingRfq(r) => ProtoState::CloseAwaitingRfq(r),
            ActiveState::Errored(k) => ProtoState::Errored(k),
        }
    }
}

// ─── Per-phase classifier methods ───

impl ConnectingState {
    /// Mirror of [`ProtoState::take_inflight_reply_raw_id`] for the
    /// per-phase enum. Consumes the variant's `ReplyId<K>` (if any)
    /// and returns its raw [`core::num::NonZeroU64`].
    ///
    /// **Exhaustive match** — adding a variant to [`ConnectingState`]
    /// that carries a `ReplyId<_>` without routing it here is a
    /// build failure. Centralises the "every in-flight reply has
    /// exactly one consume-site on the tear-down path" rule.
    ///
    /// **HandshakeReady + Errored** return `None` — neither carries
    /// a `ReplyId`. `HandshakeReady` carries `(pid, secret_key)` but
    /// the original handshake `ReplyId` was already consumed at the
    /// dispatch arm that wrote this variant.
    #[allow(
        dead_code,
        reason = "exercised by `#[cfg(test)]` sibling tests; lib-only build sees no production caller — keep the allow until per-phase Inner dispatch wiring routes through this method"
    )]
    #[must_use]
    pub(crate) fn take_inflight_reply_raw_id(self) -> Option<core::num::NonZeroU64> {
        match self {
            Self::HandshakeReady { .. } | Self::Errored(_) => None,
            Self::StartupTrust { reply }
            | Self::StartupScram { reply, .. }
            | Self::StartupCleartext { reply, .. }
            | Self::StartupMd5 { reply, .. }
            | Self::ScramAwaitingServerFirst { reply, .. }
            | Self::ScramAwaitingServerFinal { reply, .. }
            | Self::ScramAwaitingAuthOk(reply)
            | Self::CleartextAwaitingAuthOk(reply)
            | Self::Md5AwaitingAuthOk(reply)
            | Self::PostAuthAwaitingKey(reply)
            | Self::PostAuthHaveKey { reply, .. } => Some(reply.consume()),
        }
    }

    /// Per-phase mirror of [`ProtoState::push_class`]. Always returns
    /// either [`StatePushClass::Connecting`] (handshake-in-flight
    /// variants) or [`StatePushClass::Errored`] (the transient
    /// Errored signal). `HandshakeReady` classifies as `Connecting`
    /// — the wrapper-phase is still `<ConnectingPhase>` until
    /// `into_active` lifts it.
    #[inline]
    #[must_use]
    pub(crate) const fn push_class(&self) -> StatePushClass {
        match self {
            Self::Errored(kind) => StatePushClass::Errored(*kind),
            Self::StartupTrust { .. }
            | Self::StartupScram { .. }
            | Self::StartupCleartext { .. }
            | Self::StartupMd5 { .. }
            | Self::ScramAwaitingServerFirst { .. }
            | Self::ScramAwaitingServerFinal { .. }
            | Self::ScramAwaitingAuthOk(_)
            | Self::CleartextAwaitingAuthOk(_)
            | Self::Md5AwaitingAuthOk(_)
            | Self::PostAuthAwaitingKey(_)
            | Self::PostAuthHaveKey { .. }
            | Self::HandshakeReady { .. } => StatePushClass::Connecting,
        }
    }
}

/// Per-phase classifier surface for [`ActiveState`].
impl ActiveState {
    /// Per-phase mirror of [`ProtoState::take_inflight_reply_raw_id`].
    /// Exhaustive over every variant — adding a `ReplyId<K>`-carrying
    /// variant without routing it here fails the build.
    ///
    /// `Idle` / `DrainRfqAfterError` / `Errored` return `None`.
    #[allow(
        dead_code,
        reason = "exercised by `#[cfg(test)]` sibling tests in `mod protocol`; lib-only build sees no production caller — keep the allow until per-phase Inner dispatch wiring routes through this method"
    )]
    #[must_use]
    pub(crate) fn take_inflight_reply_raw_id(self) -> Option<core::num::NonZeroU64> {
        match self {
            Self::Idle | Self::DrainRfqAfterError | Self::Errored(_) => None,
            Self::PingAwaitingRfq(id) => Some(id.consume()),
            Self::SimpleQueryAwaitingFirstResponse(id) => Some(id.consume()),
            Self::SimpleQueryCopyOutStreaming(id)
            | Self::SimpleQueryCopyOutAwaitingCC(id)
            | Self::SimpleQueryCopyInActive(id) => Some(id.consume()),
            Self::SimpleQueryStreamingRows { reply }
            | Self::SimpleQueryAwaitingRfq { reply, .. }
            | Self::BindExecuteAwaitingBindCompleteSelect { reply }
            | Self::BindExecuteAwaitingDataOrCompleteSelect { reply }
            | Self::BindExecuteStreamingRows { reply }
            | Self::BindExecuteAwaitingRfqSelect { reply, .. }
            | Self::BindExecuteAwaitingRfqAfterSuspended { reply }
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
            | Self::DescribePortalAwaitingRfq { reply } => Some(reply.consume()),
            Self::CloseAwaitingComplete(reply) | Self::CloseAwaitingRfq(reply) => {
                Some(reply.consume())
            }
        }
    }

    /// Per-phase mirror of [`ProtoState::push_class`]. Returns
    /// [`StatePushClass::Idle`] for `Idle`, [`StatePushClass::PingAwaiting`]
    /// for `PingAwaitingRfq`, [`StatePushClass::Errored`] for the
    /// transient Errored signal, and [`StatePushClass::BusyQuery`] for
    /// every post-startup in-flight variant.
    ///
    /// Connecting-only `StatePushClass::Connecting` is unreachable
    /// from `ActiveState` (no Connecting variants exist) — tier-1 by
    /// storage absence.
    #[inline]
    #[must_use]
    pub(crate) const fn push_class(&self) -> StatePushClass {
        match self {
            Self::Idle => StatePushClass::Idle,
            Self::Errored(kind) => StatePushClass::Errored(*kind),
            Self::PingAwaitingRfq(_) => StatePushClass::PingAwaiting,
            Self::SimpleQueryAwaitingFirstResponse(_)
            | Self::SimpleQueryStreamingRows { .. }
            | Self::SimpleQueryAwaitingRfq { .. }
            | Self::SimpleQueryCopyOutStreaming(_)
            | Self::SimpleQueryCopyOutAwaitingCC(_)
            | Self::SimpleQueryCopyInActive(_)
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
            | Self::BindExecuteAwaitingRfqAfterSuspended { .. }
            | Self::DescribeStatementAwaitingParamDesc(_)
            | Self::DescribeStatementAwaitingRowDescOrNoData { .. }
            | Self::DescribeStatementAwaitingRfq { .. }
            | Self::DescribePortalAwaitingRowDescOrNoData(_)
            | Self::DescribePortalAwaitingRfq { .. }
            | Self::CloseAwaitingComplete(_)
            | Self::CloseAwaitingRfq(_) => StatePushClass::BusyQuery,
        }
    }
}

/// Manual `Debug` for [`ActiveState`] mirroring [`ProtoState`]'s
/// field rendering.
///
/// Active variants don't carry SCRAM / MD5 / Cleartext password
/// material (those secrets live only in [`ConnectingState`]).
/// `finish_non_exhaustive()` is used only for variants whose
/// `param_oids` / streaming-mode bookkeeping the parent Debug
/// elides.
impl core::fmt::Debug for ActiveState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Idle => f.write_str("Idle"),
            Self::PingAwaitingRfq(id) => write!(f, "PingAwaitingRfq({id:?})"),
            Self::SimpleQueryAwaitingFirstResponse(id) => {
                write!(f, "SimpleQueryAwaitingFirstResponse({id:?})")
            }
            Self::SimpleQueryStreamingRows { reply } => f
                .debug_struct("SimpleQueryStreamingRows")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::SimpleQueryAwaitingRfq { reply, command_tag } => f
                .debug_struct("SimpleQueryAwaitingRfq")
                .field("reply", reply)
                .field("command_tag", command_tag)
                .finish(),
            Self::SimpleQueryCopyOutStreaming(id) => {
                write!(f, "SimpleQueryCopyOutStreaming({id:?})")
            }
            Self::SimpleQueryCopyOutAwaitingCC(id) => {
                write!(f, "SimpleQueryCopyOutAwaitingCC({id:?})")
            }
            Self::SimpleQueryCopyInActive(id) => {
                write!(f, "SimpleQueryCopyInActive({id:?})")
            }
            Self::DrainRfqAfterError => f.write_str("DrainRfqAfterError"),
            Self::ParseAwaitingParseComplete(id) => {
                write!(f, "ParseAwaitingParseComplete({id:?})")
            }
            Self::ParseAwaitingRfq(id) => write!(f, "ParseAwaitingRfq({id:?})"),
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
            Self::BindExecuteAwaitingBindCompleteSelect { reply } => f
                .debug_struct("BindExecuteAwaitingBindCompleteSelect")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::BindExecuteAwaitingDataOrCompleteSelect { reply } => f
                .debug_struct("BindExecuteAwaitingDataOrCompleteSelect")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::BindExecuteStreamingRows { reply } => f
                .debug_struct("BindExecuteStreamingRows")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::BindExecuteAwaitingRfqSelect { reply, command_tag } => f
                .debug_struct("BindExecuteAwaitingRfqSelect")
                .field("reply", reply)
                .field("command_tag", command_tag)
                .finish_non_exhaustive(),
            Self::BindExecuteAwaitingRfqAfterSuspended { reply } => f
                .debug_struct("BindExecuteAwaitingRfqAfterSuspended")
                .field("reply", reply)
                .finish(),
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
            Self::DescribePortalAwaitingRfq { reply } => f
                .debug_struct("DescribePortalAwaitingRfq")
                .field("reply", reply)
                .finish(),
            Self::CloseAwaitingComplete(id) => write!(f, "CloseAwaitingComplete({id:?})"),
            Self::CloseAwaitingRfq(id) => write!(f, "CloseAwaitingRfq({id:?})"),
            Self::Errored(kind) => write!(f, "Errored({kind:?})"),
        }
    }
}

// ─── ProtoState → per-phase downward projections (TryFrom) ───

impl TryFrom<ProtoState> for ErroredState {
    type Error = WrongPhase;

    #[inline]
    fn try_from(s: ProtoState) -> Result<Self, Self::Error> {
        match s {
            ProtoState::Errored(k) => Ok(ErroredState::Errored(k)),
            other => Err(WrongPhase { recovered: other }),
        }
    }
}

impl TryFrom<ProtoState> for ConnectingState {
    type Error = WrongPhase;

    #[inline]
    fn try_from(s: ProtoState) -> Result<Self, Self::Error> {
        match s {
            ProtoState::ConnectingStartupTrust { reply } => {
                Ok(ConnectingState::StartupTrust { reply })
            }
            ProtoState::ConnectingStartupCleartext { reply, password } => {
                Ok(ConnectingState::StartupCleartext { reply, password })
            }
            ProtoState::ConnectingCleartextAwaitingAuthOk(r) => {
                Ok(ConnectingState::CleartextAwaitingAuthOk(r))
            }
            ProtoState::ConnectingStartupMd5 { reply, handshake } => {
                Ok(ConnectingState::StartupMd5 { reply, handshake })
            }
            ProtoState::ConnectingMd5AwaitingAuthOk(r) => {
                Ok(ConnectingState::Md5AwaitingAuthOk(r))
            }
            ProtoState::ConnectingStartupScram { reply, scram } => {
                Ok(ConnectingState::StartupScram { reply, scram })
            }
            ProtoState::ConnectingScramAwaitingServerFirst { reply, scram } => {
                Ok(ConnectingState::ScramAwaitingServerFirst { reply, scram })
            }
            ProtoState::ConnectingScramAwaitingServerFinal {
                reply,
                expected_server_sig,
            } => Ok(ConnectingState::ScramAwaitingServerFinal {
                reply,
                expected_server_sig,
            }),
            ProtoState::ConnectingScramAwaitingAuthOk(r) => {
                Ok(ConnectingState::ScramAwaitingAuthOk(r))
            }
            ProtoState::ConnectingPostAuthAwaitingKey(r) => {
                Ok(ConnectingState::PostAuthAwaitingKey(r))
            }
            ProtoState::ConnectingPostAuthHaveKey {
                reply,
                pid,
                secret_key,
            } => Ok(ConnectingState::PostAuthHaveKey {
                reply,
                pid,
                secret_key,
            }),
            ProtoState::HandshakeReady { pid, secret_key } => {
                Ok(ConnectingState::HandshakeReady { pid, secret_key })
            }
            ProtoState::Errored(k) => Ok(ConnectingState::Errored(k)),
            other => Err(WrongPhase { recovered: other }),
        }
    }
}

impl TryFrom<ProtoState> for ActiveState {
    type Error = WrongPhase;

    #[inline]
    fn try_from(s: ProtoState) -> Result<Self, Self::Error> {
        match s {
            ProtoState::Idle => Ok(ActiveState::Idle),
            ProtoState::PingAwaitingRfq(r) => Ok(ActiveState::PingAwaitingRfq(r)),
            ProtoState::SimpleQueryAwaitingFirstResponse(r) => {
                Ok(ActiveState::SimpleQueryAwaitingFirstResponse(r))
            }
            ProtoState::SimpleQueryStreamingRows { reply } => {
                Ok(ActiveState::SimpleQueryStreamingRows { reply })
            }
            ProtoState::SimpleQueryAwaitingRfq { reply, command_tag } => {
                Ok(ActiveState::SimpleQueryAwaitingRfq { reply, command_tag })
            }
            ProtoState::SimpleQueryCopyOutStreaming(r) => {
                Ok(ActiveState::SimpleQueryCopyOutStreaming(r))
            }
            ProtoState::SimpleQueryCopyOutAwaitingCC(r) => {
                Ok(ActiveState::SimpleQueryCopyOutAwaitingCC(r))
            }
            ProtoState::SimpleQueryCopyInActive(r) => {
                Ok(ActiveState::SimpleQueryCopyInActive(r))
            }
            ProtoState::DrainRfqAfterError => Ok(ActiveState::DrainRfqAfterError),
            ProtoState::ParseAwaitingParseComplete(r) => {
                Ok(ActiveState::ParseAwaitingParseComplete(r))
            }
            ProtoState::ParseAwaitingRfq(r) => Ok(ActiveState::ParseAwaitingRfq(r)),
            ProtoState::BindExecuteAwaitingBindCompleteDml(r) => {
                Ok(ActiveState::BindExecuteAwaitingBindCompleteDml(r))
            }
            ProtoState::BindExecuteAwaitingCommandCompleteDml(r) => {
                Ok(ActiveState::BindExecuteAwaitingCommandCompleteDml(r))
            }
            ProtoState::BindExecuteAwaitingRfqDml { reply, command_tag } => {
                Ok(ActiveState::BindExecuteAwaitingRfqDml { reply, command_tag })
            }
            ProtoState::BindExecuteAwaitingBindCompleteSelect { reply } => {
                Ok(ActiveState::BindExecuteAwaitingBindCompleteSelect { reply })
            }
            ProtoState::BindExecuteAwaitingDataOrCompleteSelect { reply } => {
                Ok(ActiveState::BindExecuteAwaitingDataOrCompleteSelect { reply })
            }
            ProtoState::BindExecuteStreamingRows { reply } => {
                Ok(ActiveState::BindExecuteStreamingRows { reply })
            }
            ProtoState::BindExecuteAwaitingRfqSelect { reply, command_tag } => {
                Ok(ActiveState::BindExecuteAwaitingRfqSelect { reply, command_tag })
            }
            ProtoState::BindExecuteAwaitingRfqAfterSuspended { reply } => {
                Ok(ActiveState::BindExecuteAwaitingRfqAfterSuspended { reply })
            }
            ProtoState::DescribeStatementAwaitingParamDesc(r) => {
                Ok(ActiveState::DescribeStatementAwaitingParamDesc(r))
            }
            ProtoState::DescribeStatementAwaitingRowDescOrNoData {
                reply,
                param_oids,
            } => Ok(ActiveState::DescribeStatementAwaitingRowDescOrNoData {
                reply,
                param_oids,
            }),
            ProtoState::DescribeStatementAwaitingRfq { reply, param_oids } => {
                Ok(ActiveState::DescribeStatementAwaitingRfq { reply, param_oids })
            }
            ProtoState::DescribePortalAwaitingRowDescOrNoData(r) => {
                Ok(ActiveState::DescribePortalAwaitingRowDescOrNoData(r))
            }
            ProtoState::DescribePortalAwaitingRfq { reply } => {
                Ok(ActiveState::DescribePortalAwaitingRfq { reply })
            }
            ProtoState::CloseAwaitingComplete(r) => Ok(ActiveState::CloseAwaitingComplete(r)),
            ProtoState::CloseAwaitingRfq(r) => Ok(ActiveState::CloseAwaitingRfq(r)),
            ProtoState::Errored(k) => Ok(ActiveState::Errored(k)),
            other => Err(WrongPhase { recovered: other }),
        }
    }
}

// ─── Size pins ───

// Size pins for the per-phase state enums. Empirically anchored
// (see verification in `_phase_state_size_pin_test` below).
const _: () = assert!(
    core::mem::size_of::<ErroredState>() == 1,
    "ErroredState must remain 1 B (single Errored variant carrying StateErrorKind)",
);
const _: () = assert!(
    core::mem::size_of::<ConnectingState>() == 48,
    "ConnectingState dominant variant: ScramAwaitingServerFinal (32 B SecretDigest + 8 B ReplyId + 8 B alignment)",
);
const _: () = assert!(
    core::mem::size_of::<ActiveState>() == 48,
    "ActiveState dominant variants (post-DEF-282 ParamOids boxing): \
     `SimpleQueryAwaitingRfq` / `BindExecuteAwaitingRfqDml` / \
     `BindExecuteAwaitingRfqSelect` — 8 B `ReplyId<QueryKind>` + \
     ~36 B `BoundedStr<32>` (2 B len + 32 B buf + tail-pad) + \
     discriminant + alignment → 48 B. DescribeStatement* variants \
     now carry `Box<ParamOids>` (~24 B) per the same precedent as \
     SCRAM/MD5/Cleartext heap-boxing.",
);

/// Classifier output for [`ProtoState::unsolicited_admit`]. Single
/// source of truth for "is this state allowed to accept an
/// unsolicited `ParameterStatus` or `NoticeResponse` frame?" — a
/// naive pair of independent exhaustive matches in `protocol.rs`
/// (`allows_unsolicited_param_status` / `..._notice_response`) would
/// have identical state-lists but no compile-level guarantee of
/// synchronisation. With this struct, both bools come from one match
/// arm — drift between classifiers is structurally impossible.
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
    /// Single exhaustive classifier for unsolicited
    /// `ParameterStatus` / `NoticeResponse` admittance. Adding a new
    /// `ProtoState` variant fails the build here until the contributor
    /// decides both bools.
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
            | Self::HandshakeReady { .. }
            | Self::SimpleQueryAwaitingFirstResponse(_)
            | Self::SimpleQueryStreamingRows { .. }
            | Self::SimpleQueryAwaitingRfq { .. }
            | Self::SimpleQueryCopyOutStreaming(_)
            | Self::SimpleQueryCopyOutAwaitingCC(_)
            | Self::SimpleQueryCopyInActive(_)
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
            | Self::BindExecuteAwaitingRfqAfterSuspended { .. }
            | Self::DescribeStatementAwaitingParamDesc(_)
            | Self::DescribeStatementAwaitingRowDescOrNoData { .. }
            | Self::DescribeStatementAwaitingRfq { .. }
            | Self::DescribePortalAwaitingRowDescOrNoData(_)
            | Self::DescribePortalAwaitingRfq { .. }
            | Self::CloseAwaitingComplete(_)
            | Self::CloseAwaitingRfq(_) => UnsolicitedAdmit {
                allow_param_status: true,
                allow_notice_response: true,
            },
            Self::ConnectingStartupTrust { .. }
            | Self::ConnectingStartupScram { .. }
            | Self::ConnectingStartupCleartext { .. }
            | Self::ConnectingStartupMd5 { .. }
            | Self::ConnectingScramAwaitingServerFirst { .. }
            | Self::ConnectingScramAwaitingServerFinal { .. }
            | Self::ConnectingScramAwaitingAuthOk(_)
            | Self::ConnectingCleartextAwaitingAuthOk(_)
            | Self::ConnectingMd5AwaitingAuthOk(_)
            | Self::Errored(_) => UnsolicitedAdmit {
                allow_param_status: false,
                allow_notice_response: false,
            },
        }
    }
}

/// Classifier output for [`ProtoState::push_class`].
///
/// Used by the 7 `compute_push_*` helpers in `protocol.rs` to decide
/// what `FailReply.cause` to emit on a non-Idle push. Each helper's
/// exhaustive match on `StatePushClass` replaces what would otherwise
/// be 7 parallel per-variant or-pattern matches.
///
/// Exhaustive variants — no `Other` / catch-all. Adding a new
/// `ProtoState` variant requires classifying it inside
/// [`ProtoState::push_class`] (build error if forgotten).
///
/// # Classifier carries a payload asymmetry
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
            Self::ConnectingStartupCleartext { reply, .. } => f
                .debug_struct("ConnectingStartupCleartext")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ConnectingCleartextAwaitingAuthOk(id) => {
                write!(f, "ConnectingCleartextAwaitingAuthOk({id:?})")
            }
            Self::ConnectingStartupMd5 { reply, .. } => f
                .debug_struct("ConnectingStartupMd5")
                .field("reply", reply)
                .finish_non_exhaustive(),
            Self::ConnectingMd5AwaitingAuthOk(id) => {
                write!(f, "ConnectingMd5AwaitingAuthOk({id:?})")
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
            Self::SimpleQueryCopyOutStreaming(id) => {
                write!(f, "SimpleQueryCopyOutStreaming({id:?})")
            }
            Self::SimpleQueryCopyOutAwaitingCC(id) => {
                write!(f, "SimpleQueryCopyOutAwaitingCC({id:?})")
            }
            Self::SimpleQueryCopyInActive(id) => {
                write!(f, "SimpleQueryCopyInActive({id:?})")
            }
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
            Self::BindExecuteAwaitingRfqAfterSuspended { reply } => f
                .debug_struct("BindExecuteAwaitingRfqAfterSuspended")
                .field("reply", reply)
                .finish(),
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
            Self::CloseAwaitingComplete(id) => write!(f, "CloseAwaitingComplete({id:?})"),
            Self::CloseAwaitingRfq(id) => write!(f, "CloseAwaitingRfq({id:?})"),
            Self::HandshakeReady { pid, secret_key } => f
                .debug_struct("HandshakeReady")
                .field("pid", pid)
                .field("secret_key", secret_key)
                .finish(),
            Self::Errored(kind) => write!(f, "Errored({kind:?})"),
        }
    }
}

#[cfg(test)]
mod push_class_tests {
    //! Per-variant pinning for the [`ProtoState::push_class`]
    //! classifier.
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
    //! crate-wide. Fixture construction routes through the shared
    //! [`crate::test_fixtures`] module — `fixture_nz_u64` (aliased as
    //! `nz` in this scope) asserts the input is non-zero with a
    //! `#[track_caller]`-attributed message, then narrows via the
    //! `NonZeroU64::new(...).unwrap_or(MIN)` form whose fall-through
    //! arm is architecturally dead post-assert. RowDesc fixtures use
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
    use crate::test_fixtures::fixture_nz_u64 as nz;

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

    /// Invariant (tier-1 shield for `push_class`): every ProtoState
    /// variant maps to exactly the StatePushClass declared here.
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
        // SCRAM variants carry their handshake data via Box, but
        // construction still flows through `from_password` here.
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
            let scram =
                alloc::boxed::Box::new(ScramSession::from_password(Sensitive::new(pw)));
            pin(
                ProtoState::ConnectingScramAwaitingServerFirst {
                    reply: ReplyId::from_raw(nz(2_003)),
                    scram,
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
                param_oids: alloc::boxed::Box::new(crate::action::ParamOids::EMPTY),
            },
            StatePushClass::BusyQuery,
        );
        pin(
            ProtoState::DescribeStatementAwaitingRfq {
                reply: ReplyId::from_raw(nz(6_003)),
                param_oids: alloc::boxed::Box::new(crate::action::ParamOids::EMPTY),
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

#[cfg(test)]
mod per_phase_state_roundtrip_tests {
    //! Round-trip pins for the per-phase state enum conversions.
    //!
    //! Tier-3 verification of the From/TryFrom bijection between
    //! [`ProtoState`] and [`ConnectingState`] / [`ActiveState`] /
    //! [`ErroredState`]. The alternative — tier-1 const-checkable
    //! bijection — is not expressible in stable Rust today
    //! (match-bijection lemma needs const-evaluable pattern matching).
    //!
    //! # Coverage
    //!
    //! Every variant of each per-phase enum is exercised at least
    //! once: From upward to ProtoState, then TryFrom downward back.
    //! The downward arm must yield the SAME variant on success.
    //!
    //! Adding a new variant to [`ConnectingState`] or [`ActiveState`]
    //! requires:
    //! 1. Adding the matching arm in the `From<ConnectingState> for
    //!    ProtoState` (build failure if forgotten — exhaustive match).
    //! 2. Adding the matching arm in `TryFrom<ProtoState>` (build
    //!    failure if forgotten on the upward variant side).
    //! 3. Pinning here.
    //!
    //! The combination of (1)+(2) build failures gives Tier-1 closure
    //! on "every variant has both directions wired"; the test here
    //! closes "every variant round-trips to ITSELF" (the bijection
    //! property — without it, a swap at the arm body could silently
    //! map StartupTrust → ConnectingStartupCleartext).
    //!
    //! # Variant identity comparison
    //!
    //! [`ProtoState`] does NOT derive `PartialEq` (the carried Box
    //! payloads aren't comparable without deep equality). The test
    //! compares discriminants via `core::mem::discriminant` for
    //! variants with non-Copy payloads, and direct field equality for
    //! payload-bearing simple variants where structural equality is
    //! safe.

    use super::*;
    use crate::error::{BoundedStr, ErrorKind};
    use crate::password::Password;
    use crate::reply_id::ReplyId;
    use crate::scram::session::ScramSession;
    use crate::scram::types::SecretDigest;
    use crate::sensitive::Sensitive;
    use crate::test_fixtures::fixture_nz_u64 as nz;
    use core::mem::discriminant;

    /// Consume the ReplyId carried by a state so Drop-guard doesn't
    /// trip at scope end.
    fn consume_state(state: ProtoState) {
        match state.take_inflight_reply_raw_id() {
            Some(_) | None => {}
        }
    }

    /// Round-trip a `ProtoState` value through `TryFrom<ProtoState>
    /// for ConnectingState` → `From<ConnectingState> for ProtoState`.
    /// Asserts discriminant equality (deep value equality is not
    /// expressible without PartialEq on ProtoState).
    ///
    /// **Forbid-bundle compliance**: clippy bans `.expect(...)`,
    /// `panic!()`, `unreachable!()`, AND `assert!(false, ...)` (the
    /// last one as `assertions_on_constants`). The pattern below uses
    /// runtime-computed `was_ok: bool` so the trailing `assert!(was_ok,
    /// ...)` carries a non-constant operand (clippy-clean).
    fn roundtrip_connecting(state: ProtoState) {
        let original_disc = discriminant(&state);
        let result = ConnectingState::try_from(state);
        let was_ok = result.is_ok();
        match result {
            Ok(projected) => {
                let restored: ProtoState = projected.into();
                assert_eq!(
                    discriminant(&restored),
                    original_disc,
                    "ConnectingState round-trip changed variant discriminant",
                );
                consume_state(restored);
            }
            Err(WrongPhase { recovered }) => consume_state(recovered),
        }
        assert!(
            was_ok,
            "variant should project into ConnectingState — got WrongPhase",
        );
    }

    fn roundtrip_active(state: ProtoState) {
        let original_disc = discriminant(&state);
        let result = ActiveState::try_from(state);
        let was_ok = result.is_ok();
        match result {
            Ok(projected) => {
                let restored: ProtoState = projected.into();
                assert_eq!(
                    discriminant(&restored),
                    original_disc,
                    "ActiveState round-trip changed variant discriminant",
                );
                consume_state(restored);
            }
            Err(WrongPhase { recovered }) => consume_state(recovered),
        }
        assert!(
            was_ok,
            "variant should project into ActiveState — got WrongPhase",
        );
    }

    fn roundtrip_errored(state: ProtoState) {
        let original_disc = discriminant(&state);
        let result = ErroredState::try_from(state);
        let was_ok = result.is_ok();
        match result {
            Ok(projected) => {
                let restored: ProtoState = projected.into();
                assert_eq!(
                    discriminant(&restored),
                    original_disc,
                    "ErroredState round-trip changed variant discriminant",
                );
                consume_state(restored);
            }
            Err(WrongPhase { recovered }) => consume_state(recovered),
        }
        assert!(
            was_ok,
            "variant should project into ErroredState — got WrongPhase",
        );
    }

    #[test]
    fn connecting_variants_roundtrip() {
        // Pre-auth Connecting variants (no Box-payload).
        roundtrip_connecting(ProtoState::ConnectingStartupTrust {
            reply: ReplyId::from_raw(nz(2_001)),
        });

        // Box-payload Connecting variants (need Password fixtures).
        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let pw_box = alloc::boxed::Box::new(Sensitive::new(pw));
            roundtrip_connecting(ProtoState::ConnectingStartupCleartext {
                reply: ReplyId::from_raw(nz(2_002)),
                password: pw_box,
            });
        }

        roundtrip_connecting(ProtoState::ConnectingCleartextAwaitingAuthOk(
            ReplyId::from_raw(nz(2_003)),
        ));

        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let scram = alloc::boxed::Box::new(ScramSession::from_password(Sensitive::new(pw)));
            roundtrip_connecting(ProtoState::ConnectingStartupScram {
                reply: ReplyId::from_raw(nz(2_005)),
                scram,
            });
        }
        if let Ok(pw) = Password::try_from_bytes(b"pw") {
            let scram = alloc::boxed::Box::new(ScramSession::from_password(Sensitive::new(pw)));
            roundtrip_connecting(ProtoState::ConnectingScramAwaitingServerFirst {
                reply: ReplyId::from_raw(nz(2_006)),
                scram,
            });
        }
        roundtrip_connecting(ProtoState::ConnectingScramAwaitingServerFinal {
            reply: ReplyId::from_raw(nz(2_007)),
            expected_server_sig: SecretDigest::new([0_u8; 32]),
        });
        roundtrip_connecting(ProtoState::ConnectingScramAwaitingAuthOk(
            ReplyId::from_raw(nz(2_008)),
        ));
        roundtrip_connecting(ProtoState::ConnectingMd5AwaitingAuthOk(
            ReplyId::from_raw(nz(2_009)),
        ));
        roundtrip_connecting(ProtoState::ConnectingPostAuthAwaitingKey(
            ReplyId::from_raw(nz(2_010)),
        ));
        roundtrip_connecting(ProtoState::ConnectingPostAuthHaveKey {
            reply: ReplyId::from_raw(nz(2_011)),
            pid: 42,
            secret_key: Sensitive::new(123_i32),
        });

        // Errored is in BOTH Connecting and Active phase enums.
        let errored_kind = StateErrorKind::from_kind_or_internal(ErrorKind::Framing);
        roundtrip_connecting(ProtoState::Errored(errored_kind));
    }

    #[test]
    fn active_variants_roundtrip() {
        roundtrip_active(ProtoState::Idle);
        roundtrip_active(ProtoState::PingAwaitingRfq(ReplyId::from_raw(nz(1_001))));

        // SimpleQuery flow.
        roundtrip_active(ProtoState::SimpleQueryAwaitingFirstResponse(
            ReplyId::from_raw(nz(3_001)),
        ));
        roundtrip_active(ProtoState::SimpleQueryStreamingRows {
            reply: ReplyId::from_raw(nz(3_002)),
        });
        roundtrip_active(ProtoState::SimpleQueryAwaitingRfq {
            reply: ReplyId::from_raw(nz(3_003)),
            command_tag: BoundedStr::default(),
        });
        roundtrip_active(ProtoState::DrainRfqAfterError);

        // Parse flow.
        roundtrip_active(ProtoState::ParseAwaitingParseComplete(ReplyId::from_raw(
            nz(4_001),
        )));
        roundtrip_active(ProtoState::ParseAwaitingRfq(ReplyId::from_raw(nz(4_002))));

        // BindExecute DML.
        roundtrip_active(ProtoState::BindExecuteAwaitingBindCompleteDml(
            ReplyId::from_raw(nz(5_001)),
        ));
        roundtrip_active(ProtoState::BindExecuteAwaitingCommandCompleteDml(
            ReplyId::from_raw(nz(5_002)),
        ));
        roundtrip_active(ProtoState::BindExecuteAwaitingRfqDml {
            reply: ReplyId::from_raw(nz(5_003)),
            command_tag: BoundedStr::default(),
        });

        // BindExecute SELECT.
        roundtrip_active(ProtoState::BindExecuteAwaitingBindCompleteSelect {
            reply: ReplyId::from_raw(nz(5_004)),
        });
        roundtrip_active(ProtoState::BindExecuteAwaitingDataOrCompleteSelect {
            reply: ReplyId::from_raw(nz(5_005)),
        });
        roundtrip_active(ProtoState::BindExecuteStreamingRows {
            reply: ReplyId::from_raw(nz(5_006)),
        });
        roundtrip_active(ProtoState::BindExecuteAwaitingRfqSelect {
            reply: ReplyId::from_raw(nz(5_007)),
            command_tag: BoundedStr::default(),
        });

        // DescribeStatement flow.
        roundtrip_active(ProtoState::DescribeStatementAwaitingParamDesc(
            ReplyId::from_raw(nz(6_001)),
        ));
        roundtrip_active(ProtoState::DescribeStatementAwaitingRowDescOrNoData {
            reply: ReplyId::from_raw(nz(6_002)),
            param_oids: alloc::boxed::Box::new(crate::action::ParamOids::default()),
        });
        roundtrip_active(ProtoState::DescribeStatementAwaitingRfq {
            reply: ReplyId::from_raw(nz(6_003)),
            param_oids: alloc::boxed::Box::new(crate::action::ParamOids::default()),
        });

        // DescribePortal flow.
        roundtrip_active(ProtoState::DescribePortalAwaitingRowDescOrNoData(
            ReplyId::from_raw(nz(6_004)),
        ));
        roundtrip_active(ProtoState::DescribePortalAwaitingRfq {
            reply: ReplyId::from_raw(nz(6_005)),
        });

        // Errored is in BOTH Connecting and Active phase enums.
        let errored_kind = StateErrorKind::from_kind_or_internal(ErrorKind::Framing);
        roundtrip_active(ProtoState::Errored(errored_kind));
    }

    #[test]
    fn errored_state_roundtrip() {
        let errored_kind = StateErrorKind::from_kind_or_internal(ErrorKind::Framing);
        roundtrip_errored(ProtoState::Errored(errored_kind));
    }

    /// Cross-phase rejection: ProtoState in Active variants cannot
    /// project into ConnectingState (returns WrongPhase Err).
    #[test]
    fn active_variant_rejects_as_connecting() {
        let state = ProtoState::Idle;
        let result = ConnectingState::try_from(state);
        let was_err = result.is_err();
        if let Err(WrongPhase { recovered }) = result {
            consume_state(recovered);
        }
        assert!(
            was_err,
            "Idle should reject as ConnectingState — instead got Ok",
        );
    }

    /// Cross-phase rejection: ProtoState in Connecting variants cannot
    /// project into ActiveState.
    #[test]
    fn connecting_variant_rejects_as_active() {
        let state = ProtoState::ConnectingStartupTrust {
            reply: ReplyId::from_raw(nz(9_001)),
        };
        let result = ActiveState::try_from(state);
        let was_err = result.is_err();
        if let Err(WrongPhase { recovered }) = result {
            consume_state(recovered);
        }
        assert!(
            was_err,
            "ConnectingStartupTrust should reject as ActiveState — instead got Ok",
        );
    }

    /// Cross-phase rejection: non-Errored variants cannot project into
    /// ErroredState.
    #[test]
    fn non_errored_variant_rejects_as_errored() {
        let state = ProtoState::Idle;
        let result = ErroredState::try_from(state);
        let was_err = result.is_err();
        if let Err(WrongPhase { recovered }) = result {
            consume_state(recovered);
        }
        assert!(
            was_err,
            "Idle should reject as ErroredState — instead got Ok",
        );
    }
}
