//! Active-phase dispatch + the borrowing [`Event`] pull surface.
//!
//! This is the strangler engine's query brain — the post-handshake counterpart
//! to the connecting dispatch. It dispatches **directly** on the engine's own
//! per-phase [`ActiveState`]: there is no wide `ProtoState` mirror and no
//! `From`/`TryFrom` lift. A `(phase, frame)` pair the protocol cannot reach has
//! no arm by *omission*; a wire-illegal frame within a phase is a *classified*
//! teardown ([`Event::Close`]), never a silent skip. The module scopes
//! `#![deny(clippy::wildcard_enum_match_arm)]` so a contributor cannot paper
//! over a new state with a `_` arm — every [`ActiveState`] is enumerated.
//!
//! # Scope: every active-phase FRAME is classified
//!
//! Every wire-legal active-phase FRAME has either a classified transition or a
//! classified teardown ([`Event::Close`]) — never a silent skip. The
//! simple-query flow handles `RowDescription`, `DataRow`, `CommandComplete`,
//! `EmptyQueryResponse`, and COPY in/out. The extended query protocol handles
//! `ParseComplete`, the `Describe` answers (`ParameterDescription`, then
//! `RowDescription` or `NoData`), the `Bind`/`Execute` reply
//! (`BindComplete`, `DataRow`, `CommandComplete`, `PortalSuspended`), and
//! `CloseComplete` — including the combined Parse+Bind+Execute bundle (the
//! prepared-statement macro path) and a bare-`Execute` portal resume.
//!
//! Two boundaries are deliberately outside this surface:
//!
//! - Streaming-replication `CopyBothResponse` (`'W'`) is unhandled: this engine
//!   never issues `START_REPLICATION`, so the frame cannot arrive in phase, and
//!   it is a classified teardown like any out-of-phase frame.
//! - This is a SERIAL command driver: it carries at most one in-flight command
//!   per Sync and classifies one frame per pull. It does not provide the
//!   multi-in-flight, FIFO-correlated surface a pipelined driver would need;
//!   `ParseComplete` / `BindComplete` advance state silently (the response
//!   projection, not each ack, is what a single command observes).
//!
//! # Response-driven, with a state-entry seam
//!
//! The live engine is *push-driven*: a `push_*` seats the awaiting state before
//! the reply arrives. The pull engine is driven by the server bytes alone, so
//! [`ActiveState::Idle`] doubles as "awaiting the first simple-query response":
//! the first frame branches it (`T` → row stream, `C` → command complete, `H`
//! → COPY OUT, …). The extended-protocol exchanges, however, are *not*
//! self-identifying from their first reply frame (a `ParseComplete` and a
//! `BindComplete` are indistinguishable at `Idle`). A command-issuer therefore
//! seats the matching awaiting-state through the `begin_*` seam right after
//! emitting the request — the response-driven analog of the live engine's
//! push-seated post-states. Because the Execute reply re-sends no
//! `RowDescription`, [`begin_bind_execute`](ActiveEngine::begin_bind_execute)
//! threads the result-column type OIDs recovered from the statement's prior
//! `Describe` so executed rows surface against the same schema.
//!
//! # Honest scope caveat
//!
//! A `;`-separated simple-query batch whose row-bearing statement is *not* last
//! is delineated here cleanly (one statement boundary per `CommandComplete`).
//! The live engine instead FLATTENS a row-FIRST batch through its row-stream
//! pull, an engine-specific quirk the cleanly-delineated pull does not
//! reproduce; that single shape is the one place the two diverge by
//! construction.
//!
//! # The pull surface
//!
//! [`ActiveEngine::next_event`] locates one inbound frame in the
//! [`IngestBuf`], runs the consuming dispatch, and returns the next [`Event`]
//! borrowing the read buffer. Silent intermediate frames (`RowDescription`,
//! `CopyOutResponse`) loop internally; the caller sees only the surfaceable
//! events. The borrow shape is the **span split** (R2): a frame is classified
//! and its body span computed in one pass, the cursor advances, then the body
//! is re-borrowed in place via [`IngestBuf::frame_body`] — a single classify
//! pass, no re-parse, expressible on stable with no `unsafe`.
//!
//! # Oversize frames (bounded memory)
//!
//! A frame whose wire footprint exceeds [`READ_BUF_CAP`](crate::frame::READ_BUF_CAP) can never reside whole
//! in the bounded buffer. Two streaming paths absorb it without unbounded
//! growth, both frame-header-aware (the length field is read once; the body is
//! never scanned for a terminator):
//!
//! - **Sub-A** — an oversize `DataRow` streams as [`Event::RowChunk`] chunks
//!   (each ≤ one buffer fill) terminated by [`Event::RowChunkEnd`].
//! - **Sub-B** — an oversize streaming-eligible non-`D` frame keeps a bounded
//!   8 KiB prefix and counts-and-skips the tail, then surfaces the truncated
//!   prefix as its classified event.
//!
//! Any other oversize tag is a classified teardown.

#![deny(
    clippy::wildcard_enum_match_arm,
    reason = "the active dispatch must enumerate every ActiveState; a `_` arm would silently swallow a new phase instead of classifying its frames"
)]

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use super::{Event, IngestBuf, IngestCommitOverflow, IngestFull};
use crate::action::TxStatus;
use crate::command_tag::{parse_command_tag_bytes, CommandTag};
use crate::decode::{parse_column_names, parse_copy_response_header, parse_row_description};
use crate::frame::{HeaderParse, HEADER_LEN};
use crate::narrow::usize_from_u32;
use crate::sensitive::Sensitive;
use crate::wire::{
    TAG_BIND_COMPLETE, TAG_CLOSE_COMPLETE, TAG_COMMAND_COMPLETE, TAG_COPY_DATA, TAG_COPY_DONE,
    TAG_COPY_IN_RESPONSE, TAG_COPY_OUT_RESPONSE, TAG_DATA_ROW, TAG_EMPTY_QUERY_RESPONSE,
    TAG_ERROR_RESPONSE, TAG_NO_DATA, TAG_NOTICE_RESPONSE, TAG_NOTIFICATION_RESPONSE,
    TAG_PARAMETER_DESCRIPTION, TAG_PARAMETER_STATUS, TAG_PARSE_COMPLETE, TAG_PORTAL_SUSPENDED,
    TAG_READY_FOR_QUERY, TAG_ROW_DESCRIPTION,
};

// ── Inbound tag bytes, named once so the dispatch matches read like the wire ──
const T_ROW_DESC: u8 = TAG_ROW_DESCRIPTION.byte();
const T_DATA_ROW: u8 = TAG_DATA_ROW.byte();
const T_COMMAND_COMPLETE: u8 = TAG_COMMAND_COMPLETE.byte();
const T_EMPTY_QUERY: u8 = TAG_EMPTY_QUERY_RESPONSE.byte();
const T_READY_FOR_QUERY: u8 = TAG_READY_FOR_QUERY.byte();
const T_ERROR: u8 = TAG_ERROR_RESPONSE.byte();
const T_NOTICE: u8 = TAG_NOTICE_RESPONSE.byte();
const T_NOTIFY: u8 = TAG_NOTIFICATION_RESPONSE.byte();
const T_PARAM_STATUS: u8 = TAG_PARAMETER_STATUS.byte();
const T_COPY_OUT: u8 = TAG_COPY_OUT_RESPONSE.byte();
const T_COPY_IN: u8 = TAG_COPY_IN_RESPONSE.byte();
const T_COPY_DATA: u8 = TAG_COPY_DATA.byte();
const T_COPY_DONE: u8 = TAG_COPY_DONE.byte();
// Extended-query-protocol reply tags (Parse/Describe/Bind/Execute/Close).
const T_PARSE_COMPLETE: u8 = TAG_PARSE_COMPLETE.byte();
const T_BIND_COMPLETE: u8 = TAG_BIND_COMPLETE.byte();
const T_PARAM_DESC: u8 = TAG_PARAMETER_DESCRIPTION.byte();
const T_NO_DATA: u8 = TAG_NO_DATA.byte();
const T_PORTAL_SUSPENDED: u8 = TAG_PORTAL_SUSPENDED.byte();
const T_CLOSE_COMPLETE: u8 = TAG_CLOSE_COMPLETE.byte();

/// Bounded prefix retained for an oversize streaming-eligible frame (Sub-B).
/// The body beyond this is counted-and-skipped, so a multi-megabyte frame is
/// absorbed in constant memory — this prefix plus the [`READ_BUF_CAP`](crate::frame::READ_BUF_CAP) ingest
/// buffer is the whole footprint.
const OVERSIZE_PREFIX_CAP: usize = 8192;

/// Hard ceiling on the bytes the Sub-C accumulator gathers for an oversize
/// `RowDescription` before the declared length is rejected as hostile/buggy.
///
/// Unlike Sub-A (bounded `RowChunk` streaming) and Sub-B (bounded 8 KiB prefix),
/// the Sub-C accumulator MUST parse the whole frame, so an uncapped accumulate
/// would let a server's declared `u32` length (up to ~4 GiB) drive the client to
/// OOM. The legitimate ceiling is tiny: PostgreSQL caps a result at 1664
/// columns, and a `RowDescription` field is `name (<= 63 + 1 NUL) + 18 fixed`
/// bytes, so the absolute worst case is ~1664 * 82 ≈ 133 KiB. This 1 MiB bound
/// clears that with ~7x headroom while bounding a hostile declared length to a
/// small per-frame allocation; beyond it the frame is a classified teardown
/// (reject-before-allocate), exactly like any other oversize control frame.
const MAX_ROW_DESC_ACCUM: usize = 1 << 20;

// ===========================================================================
// Active-phase state machine
// ===========================================================================

/// The active engine's current command phase.
///
/// `Idle` doubles as "awaiting the first response frame of a command" (the
/// pull engine is response-driven — there is no push to seat a distinct
/// awaiting state). Each variant's legal frames are enumerated in
/// [`ActiveEngine::step_frame`]; an out-of-phase frame is a classified
/// teardown, not a silent skip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveState {
    /// Between commands, or awaiting a command's first response frame.
    Idle,
    /// A `RowDescription` opened a row stream; collecting `DataRow`s.
    StreamingRows,
    /// A statement completed; awaiting `ReadyForQuery` or the next statement's
    /// response in a multi-statement batch.
    AwaitingRfq,
    /// `CopyOutResponse` opened a COPY OUT; collecting `CopyData` frames.
    CopyOut,
    /// `CopyDone` closed the COPY data; awaiting the trailing `CommandComplete`.
    CopyOutAwaitingCc,
    /// `CopyInResponse` opened a COPY IN; awaiting the server `CommandComplete`.
    CopyInActive,
    /// A recoverable server error parked a drain; awaiting `ReadyForQuery`.
    DrainAfterError,
    /// A too-wide `RowDescription` (column count over
    /// [`MAX_ROW_COLUMNS`](crate::decode::MAX_ROW_COLUMNS)) was classified as a
    /// recoverable `TooManyColumns`; the in-flight result the client rejected is
    /// being SWALLOWED to the trailing `ReadyForQuery`. Distinct from
    /// [`DrainAfterError`](Self::DrainAfterError): a server error aborts the query
    /// (only the RFQ follows), whereas here the server streams a full result, so
    /// every frame until the RFQ — `DataRow`s, `CommandComplete`, a second
    /// statement's `RowDescription` — is discarded.
    DrainOvercapToRfq,

    // ── Extended query protocol (Parse / Describe / Bind+Execute / Close) ──
    //
    // The simple-query states above are reached from `Idle` by branching on
    // the server's first reply frame. The extended-protocol exchanges are not
    // self-identifying from their first reply frame alone (a `ParseComplete`
    // and a `BindComplete` look alike at `Idle`), so a command-issuer seats the
    // matching awaiting-state via the `begin_*` seam before the reply is pulled
    // — the response-driven analog of the live engine's push-seated post-state.
    /// A bare `Parse` was issued; awaiting `ParseComplete` (`'1'`).
    ParseAwaitingParseComplete,
    /// A combined `Parse`+`Describe`(statement)+`Sync` bundle (the runtime
    /// prepare path) was issued; awaiting `ParseComplete` (`'1'`), which is
    /// followed by the `Describe` answer (`ParameterDescription`, then
    /// `RowDescription`/`NoData`) rather than the Sync boundary — so it advances
    /// silently into the describe phase.
    ParseDescribeStmtAwaitingParseComplete,
    /// A combined Parse+Bind+Execute bundle (the prepared-statement macro path)
    /// was issued; awaiting `ParseComplete` (`'1'`), which is followed by
    /// `BindComplete` rather than the Sync boundary.
    ParseBindExecuteAwaitingParseComplete,
    /// A cache-MISS macro path led with a `Close`(statement)+Parse+Bind+Execute
    /// bundle; awaiting the leading `CloseComplete` (`'3'`), which is followed by
    /// the `ParseComplete` of the trailing `Parse` — so it advances silently into
    /// [`ParseBindExecuteAwaitingParseComplete`](Self::ParseBindExecuteAwaitingParseComplete).
    /// The leading `Close` makes the re-`Parse` idempotent (a `Close` of a
    /// nonexistent statement is a wire no-op), so a name the server may still
    /// hold — one first Parsed inside a since-committed transaction, hence not
    /// yet recorded — is re-created without a duplicate-statement error.
    CloseParseBindExecuteAwaitingCloseComplete,
    /// A statement `Describe` was issued; awaiting `ParameterDescription`
    /// (`'t'`), which always precedes the row-or-no-data answer.
    DescribeStmtAwaitingParamDesc,
    /// A `Describe` is awaiting its `RowDescription` (`'T'`) or `NoData`
    /// (`'n'`) answer.
    DescribeAwaitingRowDescOrNoData,
    /// A `Bind`+`Execute` bundle was issued; awaiting `BindComplete` (`'2'`).
    BindAwaitingBindComplete,
    /// `BindComplete` seen; awaiting the Execute's result — `DataRow`s then
    /// `CommandComplete`, a bare `CommandComplete` (DML / no rows), or a
    /// `PortalSuspended` (`'s'`) when a row-limited Execute hits its cap.
    BindAwaitingData,
    /// An extended-protocol command's body completed; awaiting the single
    /// `ReadyForQuery` of its Sync. Unlike [`AwaitingRfq`](Self::AwaitingRfq),
    /// no `CommandComplete`/`RowDescription` continuation is legal — one Sync
    /// closes one command.
    ExtendedAwaitingRfq,
    /// A `Close` was issued; awaiting `CloseComplete` (`'3'`).
    CloseAwaitingComplete,

    // ── Fused one-round-trip runtime-param path (Parse+Bind+Describe+Execute) ──
    //
    // The DYNAMIC (runtime-untyped) query path fuses the whole extended-protocol
    // exchange into ONE flush — `Parse`(unnamed) + `Bind` + `Describe`(portal) +
    // `Execute` + `Sync` — so a one-shot parameterised query costs ONE round trip
    // instead of the three the prepare / bind+execute / close sequence took. The
    // in-batch `Describe`(portal) makes the server surface the result schema
    // INLINE (after `BindComplete`, before the `DataRow`s), so the runtime
    // consumer recovers the OIDs + names it needs with no separate `prepare`
    // round trip. The unnamed statement is implicitly discarded at the next
    // `Parse`(unnamed), so no `Close` is needed. These three await-states are the
    // one-time setup chain before the (existing) `BindAwaitingData` row stream.
    /// The fused batch was issued; awaiting `ParseComplete` (`'1'`), which is
    /// followed by `BindComplete` — so it advances silently into the bind phase.
    FusedAwaitingParseComplete,
    /// `ParseComplete` seen in the fused batch; awaiting `BindComplete` (`'2'`),
    /// which is followed by the `Describe`(portal) answer — so it advances
    /// silently into the row-desc-or-no-data phase.
    FusedAwaitingBindComplete,
    /// `BindComplete` seen in the fused batch; awaiting the `Describe`(portal)
    /// answer — `RowDescription` (`'T'`, the query returns rows) or `NoData`
    /// (`'n'`, a DML / no-RETURNING command). Either way it captures the recovered
    /// schema and advances into the (existing) `BindAwaitingData` row stream, so
    /// the executed rows decode against the INLINE-recovered OIDs.
    FusedAwaitingRowDescOrNoData,

    /// A protocol violation tore the connection down — terminal.
    Failed,
}

/// Borrow source for a payload-lending [`ActiveOutcome`].
#[derive(Debug, Clone, Copy)]
enum Lend {
    /// Body resident in the ingest buffer (whole-frame / Sub-A chunk).
    Ingest,
    /// Body accumulated into the Sub-B truncation prefix.
    Prefix,
}

/// Non-borrowing dispatch outcome. The payload-lending variants carry the
/// borrow source plus the offset range re-borrowed in [`ActiveEngine::
/// next_event`]; keeping the drive loop non-borrowing lets per-frame mutation
/// (state, cols, command tag) compile without a returns-a-loop-borrow E0499.
#[derive(Debug, Clone, Copy)]
enum ActiveOutcome {
    /// An expected intermediate frame with no surfaceable event — keep pulling.
    Silent,
    NeedMore,
    Idle,
    Deliver,
    Suspended,
    Close,
    RowChunkEnd,
    CopyDone,
    /// A too-wide `RowDescription` classified as a recoverable `TooManyColumns`.
    /// The `count`/`max` ride the outcome (no buffer borrow) so the driver names
    /// the exact limit; the drain state was already parked by
    /// [`enter_overcap_recovery`](ActiveEngine::enter_overcap_recovery).
    Overcap { count: usize, max: usize },
    Fail(Lend, usize, usize),
    Notice(Lend, usize, usize),
    Notify(Lend, usize, usize),
    ParamStatus(Lend, usize, usize),
    Row(Lend, usize, usize),
    RowChunk(Lend, usize, usize),
    CopyData(Lend, usize, usize),
}

/// Sub-A (`DataRow`) vs Sub-B (streaming-eligible non-`D`) vs Sub-C
/// (parse-whole control frame) oversize handling.
#[derive(Debug, Clone, Copy)]
enum OversizeMode {
    /// Stream the row body as `RowChunk` / `RowChunkEnd`.
    SubA,
    /// Keep a bounded prefix, count-and-skip the tail, then surface the
    /// truncated event classified by `surfaced_tag`.
    SubB,
    /// Gather the whole body into the growable accumulator, then parse it (a
    /// wide `RowDescription`: every column's OID and name is load-bearing, so it
    /// can be neither truncated like Sub-B nor streamed piecewise like Sub-A).
    Accumulate,
    /// Consume and DISCARD the whole body, surfacing nothing. Entered only while
    /// [`DrainOvercapToRfq`](ActiveState::DrainOvercapToRfq): recovering from a
    /// too-wide result means every following frame (including a wide `DataRow`
    /// that itself overflows the buffer) is swallowed to reach the trailing RFQ,
    /// so an oversize one is skipped rather than streamed (Sub-A), accumulated
    /// (Sub-C), or truncated-and-surfaced (Sub-B).
    Skip,
}

/// State of an in-progress oversize frame. `Copy` so the drive loop can lift it
/// out of `self`, mutate the ingest/prefix freely, and write it back — no
/// borrow of `self.oversize` is held across the buffer mutations.
#[derive(Debug, Clone, Copy)]
struct OversizeStream {
    mode: OversizeMode,
    /// Body bytes still owed by the wire after the header was consumed.
    body_remaining: usize,
    /// The frame tag (for the Sub-B completion event).
    surfaced_tag: u8,
    /// Bytes accumulated into the Sub-B prefix so far.
    prefix_len: usize,
}

// ===========================================================================
// The active engine handle (post-handshake)
// ===========================================================================

/// The active-phase engine handle produced by
/// [`ConnectingEngine::into_active`](super::ConnectingEngine::into_active).
///
/// Carries the non-secret backend pid, the redacted cancel-key authenticator,
/// the live transaction status, and the single-residence ingest buffer plus the
/// active command state. The active-phase verbs
/// ([`backend_pid`](Self::backend_pid), [`tx_status`](Self::tx_status),
/// [`with_secret_key`](Self::with_secret_key), [`next_event`](Self::next_event))
/// are absent on the connecting handle — calling one there is a
/// method-not-found compile error (E0599), the typestate proof that a query
/// cannot be issued before the handshake completes.
pub struct ActiveEngine {
    backend_pid: i32,
    secret_key: Sensitive<i32>,
    tx_status: TxStatus,
    ingest: IngestBuf,
    state: ActiveState,
    oversize: Option<OversizeStream>,
    /// Bounded truncation prefix for Sub-B; allocated lazily on first oversize
    /// streaming-eligible frame.
    prefix: Option<Box<[u8; OVERSIZE_PREFIX_CAP]>>,
    /// Growable accumulator for a Sub-C parse-whole oversize frame (a wide
    /// `RowDescription`): the whole body is gathered here across reads, then
    /// parsed. Empty except while such a frame is in flight; scrubbed on drop
    /// (it holds raw inbound wire bytes — column names).
    oversize_accum: Vec<u8>,
    /// Per-column type OIDs of the current statement's `RowDescription`.
    col_oids: Vec<u32>,
    /// Per-column names of the current statement's `RowDescription`.
    col_names: Vec<String>,
    /// The most recent statement's `CommandComplete` tag.
    command_tag: Option<CommandTag>,
    /// The `server_version` GUC captured from the startup `ParameterStatus`
    /// reports during the handshake and carried across
    /// [`ConnectingEngine::into_active`](super::ConnectingEngine::into_active).
    /// Read via [`server_version`](Self::server_version); `None` if the server
    /// sent no such report. This is what lets the driver skip the post-connect
    /// `SHOW server_version` round-trip.
    ///
    /// Off the hot path: written once at `from_handshake` and read only through
    /// the cold accessor, never inside `next_event`/`drive`. Its footprint cost
    /// is therefore an offset shift with no hot-loop codegen effect — that
    /// coldness is what keeps the active path unperturbed.
    server_version: Option<String>,
    /// Content-addressed names of the prepared statements this connection has
    /// Parsed and that are DURABLE on the server.
    ///
    /// A name is recorded here IFF the server currently holds that prepared
    /// statement on this physical connection: it is added only after its `Parse`
    /// completed at a clean idle (so the implicit/explicit transaction wrapping
    /// the `Parse` committed and no rollback can have dropped it), and it is
    /// removed only by [`clear_statement_cache`](Self::clear_statement_cache)
    /// (the hook a session reset drives). A statement first Parsed inside an open
    /// transaction is deliberately NOT recorded, so the set can never name a
    /// statement a rollback removed. The membership therefore lets a repeat of
    /// the same content-addressed query reuse the server-side plan with a bare
    /// `Bind`+`Execute` (skipping the duplicate `Parse` the server would reject),
    /// while a recorded name can never point at a statement the server lacks.
    ///
    /// Bounded by the number of distinct compile-checked queries the binary runs
    /// on this connection (a statically finite set), so it cannot grow without
    /// bound; the names are short `'static` references into the consumer's
    /// `.rodata`, so the entries are bare fat pointers, not owned strings.
    parsed_statements: Vec<&'static str>,
    /// The simple-query SQL of a fused PRELUDE to prepend to the NEXT command's
    /// flush — today ONLY a deferred transaction `BEGIN`, fused with the
    /// transaction's first statement (a row-bearing prelude such as a pool RESET
    /// is a deferred capability the drain does not yet handle — see
    /// [`step_prelude`](Self::step_prelude)). `None` in steady state. Taken by the
    /// first request verb that runs, which enqueues the prelude's `'Q'` frame ahead
    /// of its own and then [`arm_prelude`](Self::arm_prelude)s the drain. The
    /// `'static` SQL is a bare `&str` into `.rodata`, never an owned allocation.
    ///
    /// Off the hot path: read only at a verb's send-path entry (never inside
    /// `next_event`/`drive`), so its footprint cost is an offset shift with no
    /// hot-loop codegen effect.
    pending_prelude: Option<&'static str>,
    /// Whether a fused prelude's response is still being drained AHEAD of the
    /// seated command's response. `false` in steady state — the drive loop then
    /// dispatches on [`state`](Self::state) directly, byte-identically to before
    /// this field existed.
    ///
    /// The ONLY deferred prelude is a transaction `BEGIN` (see
    /// [`set_pending_prelude`](Self::set_pending_prelude)), whose reply is exactly
    /// `CommandComplete` + `ReadyForQuery`, so a single drain shape suffices — no
    /// per-phase state machine. Read only on the SEPARATE
    /// [`next_prelude_event`](Self::next_prelude_event) drain path, so `next_event`
    /// is untouched.
    prelude_active: bool,
}

// Stack footprint of the active handle: the carried-forward `IngestBuf` (144)
// dominates, plus the pid/secret/tx-status scalars, the `Option<Box<…>>` /
// `Vec` handles (schema, oversize prefix, the Sub-C accumulator, the prepared-
// statement-name cache), the `command_tag`, the captured `server_version`
// (`Option<String>`, 24 B — the version text itself is off-stack behind the
// `String`), and the fused-prelude pair (`Option<&'static str>` + a 1-byte
// `bool`). The result-schema, oversize, and cached-name bytes
// live off-stack behind those handles. A field addition lands here as a reviewed
// footprint drift.
crate::wire_pin!(ActiveEngine, size = 368, align = 8);

impl ActiveEngine {
    /// Construct the active engine at handshake completion, carrying the
    /// connecting engine's ingest buffer forward (any pipelined active frames
    /// are already resident).
    pub(super) fn from_handshake(
        backend_pid: i32,
        secret_key: Sensitive<i32>,
        tx_status: TxStatus,
        ingest: IngestBuf,
        server_version: Option<String>,
    ) -> Self {
        Self {
            backend_pid,
            secret_key,
            tx_status,
            ingest,
            state: ActiveState::Idle,
            oversize: None,
            prefix: None,
            oversize_accum: Vec::new(),
            col_oids: Vec::new(),
            col_names: Vec::new(),
            command_tag: None,
            server_version,
            parsed_statements: Vec::new(),
            pending_prelude: None,
            prelude_active: false,
        }
    }

    /// The backend process id from `BackendKeyData` — the non-secret half of
    /// the cancel key, safe to surface.
    #[inline]
    #[must_use]
    pub fn backend_pid(&self) -> i32 {
        self.backend_pid
    }

    /// The `server_version` GUC reported during the handshake, or `None` if the
    /// server sent no `server_version` `ParameterStatus`.
    ///
    /// Captured for free from the startup reports, so a driver reads the server
    /// version here rather than issuing a `SHOW server_version` round-trip.
    #[inline]
    #[must_use]
    pub fn server_version(&self) -> Option<&str> {
        self.server_version.as_deref()
    }

    /// The current `ReadyForQuery` transaction-status indicator, updated at each
    /// command boundary.
    #[inline]
    #[must_use]
    pub fn tx_status(&self) -> TxStatus {
        self.tx_status
    }

    /// Closure-scope access to the backend cancel-key authenticator.
    ///
    /// The secret never escapes the call (HRTB-bounded, mirroring the crate's
    /// [`Sensitive::with_inner`] pattern); the cancel-request builder consumes
    /// it here. The observable capture deliberately never calls this — a leaked
    /// cancel authenticator is a capability leak.
    #[inline]
    pub fn with_secret_key<R>(&self, f: impl FnOnce(i32) -> R) -> R {
        self.secret_key.with_inner(|key| f(*key))
    }

    /// Lend a writable tail slice the socket/script reads inbound bytes into.
    /// Pair with [`commit`](Self::commit) of the count actually written.
    #[inline]
    pub fn read_slot(&mut self, want: usize) -> Result<&mut [u8], IngestFull> {
        self.ingest.read_slot(want)
    }

    /// Publish `n` inbound bytes written into the most recent
    /// [`read_slot`](Self::read_slot).
    #[inline]
    pub fn commit(&mut self, n: usize) -> Result<(), IngestCommitOverflow> {
        self.ingest.commit(n)
    }

    /// Per-column type OIDs from the current statement's `RowDescription`, or
    /// empty when none was observed for the in-flight statement.
    #[inline]
    #[must_use]
    pub fn current_type_oids(&self) -> &[u32] {
        &self.col_oids
    }

    /// Per-column names from the current statement's `RowDescription`, or empty
    /// when none was observed.
    #[inline]
    #[must_use]
    pub fn current_column_names(&self) -> &[String] {
        &self.col_names
    }

    /// The most recent statement's typed `CommandComplete` tag, set just before
    /// the [`Event::Deliver`] that surfaces it. `None` before the first
    /// statement of a command completes.
    #[inline]
    #[must_use]
    pub fn last_command_tag(&self) -> Option<&CommandTag> {
        self.command_tag.as_ref()
    }

    // ── Per-connection prepared-statement cache ───────────────────────────
    //
    // The membership invariant: a content-addressed name is present IFF the
    // server currently holds that prepared statement on this physical
    // connection. The recorder enforces one direction (record only a durable
    // Parse); nothing in this engine drops a server-side statement out of band,
    // so the other direction holds for the connection's whole life unless a
    // caller resets the session, in which case it drives `clear_statement_cache`.

    /// Whether a prepared statement with this content-addressed name has already
    /// been Parsed and is durable on this connection — so a fresh `Parse` of it
    /// would be a server-side `duplicate_prepared_statement`, and a bare
    /// `Bind`+`Execute` can reuse the existing server plan instead.
    #[inline]
    #[must_use]
    pub fn is_statement_parsed(&self, stmt_name: &'static str) -> bool {
        // Linear scan over a statically-bounded set (one entry per distinct
        // compile-checked query this connection runs); the names are short and
        // few, so this is cheaper than a hashed set and allocates nothing on the
        // hit path. `contains` compares str CONTENTS (not pointer identity), so
        // two call sites that emit the same content-addressed query share one
        // server-side statement.
        self.parsed_statements.contains(&stmt_name)
    }

    /// Record that the prepared statement with this content-addressed name is now
    /// Parsed and durable on this connection.
    ///
    /// The caller records ONLY a name whose `Parse` completed at a clean idle —
    /// a statement first Parsed inside an open transaction is left unrecorded (a
    /// rollback could drop it on some servers) — so the recorded set can never
    /// name a statement the server has dropped.
    #[inline]
    pub fn record_statement_parsed(&mut self, stmt_name: &'static str) {
        // The caller checks `is_statement_parsed` before issuing the `Parse`, so a
        // name reaches here at most once; a defensive de-dup would be dead code.
        self.parsed_statements.push(stmt_name);
    }

    /// Forget every recorded prepared-statement name — the hook a session reset
    /// (`DISCARD ALL` / `DEALLOCATE ALL`) drives so the cache cannot outlive the
    /// server-side statements it names.
    #[inline]
    pub fn clear_statement_cache(&mut self) {
        self.parsed_statements.clear();
    }

    /// Drop one recorded name from the cache.
    ///
    /// Driven when a REUSE (bare `Bind`+`Execute` over a recorded name) hit a
    /// server error — most likely the statement was dropped out of band
    /// (`DISCARD ALL` / `DEALLOCATE`). Evicting it means the NEXT use of the name
    /// is a cache MISS, which the Close-before-Parse miss path re-creates safely
    /// (idempotently) — so the connection self-heals instead of poisoning every
    /// later reuse. The error itself is still surfaced loudly; this only prunes
    /// the stale cache entry, never retries.
    #[inline]
    pub fn evict_statement(&mut self, stmt_name: &str) {
        self.parsed_statements.retain(|name| *name != stmt_name);
    }

    // ── Fused-prelude staging + drain ─────────────────────────────────────
    //
    // A transaction's `BEGIN` is DEFERRED and fused into the first following
    // command's flush: one flush carries the prelude simple-query AND the command,
    // and the prelude's own response is drained (swallowed) AHEAD of the command's,
    // removing the prelude's standalone round trip. `BEGIN` is the ONLY prelude
    // armed today — the drain (`step_prelude`) understands only its non-row-bearing
    // reply shape; a row-bearing prelude (a pool RESET returning a row) is deferred
    // (see `step_prelude`). The mechanism is confined to a SEPARATE drain path
    // ([`next_prelude_event`](Self::next_prelude_event) / `drive_prelude` /
    // `step_prelude`), so the inbound hot dispatch [`next_event`](Self::next_event)
    // is byte-identical whether or not a prelude is armed — read below only in the
    // cold verb send-path and the cold drain path, never in `next_event`.

    /// Arm a fused simple-query prelude to prepend to the NEXT command's flush.
    /// The SQL parameter is a general `'static &str`, but the ONLY prelude armed
    /// today is a transaction `BEGIN`, and the drain ([`step_prelude`](Self::step_prelude))
    /// handles only its non-row-bearing reply shape (`CommandComplete` +
    /// `ReadyForQuery`) — a ROW-bearing prelude would hit the fatal-teardown arm.
    /// Overwrites any previously-armed pending prelude — the caller (a transaction)
    /// arms exactly one at a time.
    #[inline]
    pub fn set_pending_prelude(&mut self, sql: &'static str) {
        self.pending_prelude = Some(sql);
    }

    /// Take the pending fused-prelude SQL, if any — the request verb enqueues its
    /// `'Q'` frame ahead of its own command frames, then
    /// [`arm_prelude`](Self::arm_prelude)s the drain.
    #[inline]
    #[must_use]
    pub fn take_pending_prelude(&mut self) -> Option<&'static str> {
        self.pending_prelude.take()
    }

    /// Arm the prelude DRAIN: the next inbound frames drain the prelude's
    /// simple-query response (swallowed) before the seated command's, then the
    /// seated [`ActiveState`] takes over. Paired with the verb enqueuing the
    /// prelude's frame ahead of its own command frames.
    #[inline]
    pub fn arm_prelude(&mut self) {
        self.prelude_active = true;
    }

    /// Whether a fused prelude is still being drained (its trailing
    /// `ReadyForQuery` not yet seen). The pump drains it via
    /// [`next_prelude_event`](Self::next_prelude_event) before the command loop.
    #[inline]
    #[must_use]
    pub fn draining_prelude(&self) -> bool {
        self.prelude_active
    }

    // ── Extended-query-protocol state-entry seam ──────────────────────────
    //
    // The pull engine is response-driven, so an extended-protocol exchange
    // cannot be recognised from its first reply frame alone. A command-issuer
    // seats the matching awaiting-state here just after putting the
    // corresponding request bytes on the wire (the bytes are the issuer's job;
    // these methods only move the state machine). Each mirrors one of the live
    // engine's push-seated post-states.

    /// Seat the engine to await a bare `Parse`'s `ParseComplete` then its Sync
    /// `ReadyForQuery`. The ack carries no command tag, so the boundary it
    /// surfaces is an empty-tag, no-rows delivery.
    #[inline]
    pub fn begin_parse(&mut self) {
        self.state = ActiveState::ParseAwaitingParseComplete;
    }

    /// Seat the engine to await a combined `Parse`+`Describe`(statement)+`Sync`
    /// bundle (the runtime prepare path): `ParseComplete`, then the statement
    /// `Describe`'s `ParameterDescription` + `RowDescription`/`NoData`, then the
    /// single Sync `ReadyForQuery`. The recovered result schema is surfaced via
    /// [`current_type_oids`](Self::current_type_oids) /
    /// [`current_column_names`](Self::current_column_names) at the describe's
    /// delivery, so a later `Bind`+`Execute` can thread the OIDs back in. Clears
    /// the per-statement columns so a prior statement's schema cannot leak.
    #[inline]
    pub fn begin_prepare(&mut self) {
        self.reset_columns();
        self.state = ActiveState::ParseDescribeStmtAwaitingParseComplete;
    }

    /// Seat the engine to await a statement `Describe`'s reply
    /// (`ParameterDescription`, then `RowDescription` or `NoData`). The
    /// recovered result schema is surfaced via
    /// [`current_type_oids`](Self::current_type_oids) /
    /// [`current_column_names`](Self::current_column_names) at the describe's
    /// delivery, before the trailing `ReadyForQuery` resets it.
    #[inline]
    pub fn begin_describe_statement(&mut self) {
        self.state = ActiveState::DescribeStmtAwaitingParamDesc;
    }

    /// Seat the engine to await a portal `Describe`'s reply. A portal describe
    /// answers with `RowDescription` or `NoData` and — unlike a statement
    /// describe — NO `ParameterDescription` (a portal is already bound), so this
    /// seats the row-or-no-data wait directly. A `ParameterDescription` arriving
    /// here is therefore out-of-phase and a classified teardown.
    #[inline]
    pub fn begin_describe_portal(&mut self) {
        self.state = ActiveState::DescribeAwaitingRowDescOrNoData;
    }

    /// Seat the engine to await a `Bind`+`Execute`'s reply, threading the
    /// result-column type OIDs recovered from the statement's prior `Describe`.
    ///
    /// The Execute reply re-sends no `RowDescription`, so without this thread
    /// the executed rows would surface with no type OIDs. Column *names* are
    /// not re-sent at execute time and stay empty — the pinned quirk of the
    /// extended-protocol execute path (OIDs drive decode; names do not).
    #[inline]
    pub fn begin_bind_execute(&mut self, result_oids: &[u32]) {
        self.seat_bind(result_oids);
    }

    /// Seat the engine to await a row-limited `Bind`+`Execute`'s reply.
    ///
    /// Identical seating to [`begin_bind_execute`](Self::begin_bind_execute):
    /// the row cap is a client-side Execute parameter, and the server reports
    /// the outcome on the wire — `CommandComplete` when the portal drains,
    /// `PortalSuspended` when it hits the cap. The response-driven dispatch
    /// branches on that frame, so no distinct awaiting-state is needed.
    #[inline]
    pub fn begin_bind_execute_row_limited(&mut self, result_oids: &[u32]) {
        self.seat_bind(result_oids);
    }

    /// Seat the engine to await a combined Parse+Bind+Execute bundle's reply
    /// (the prepared-statement macro path): `ParseComplete`, `BindComplete`,
    /// then the executed rows under one Sync. The result schema is the macro's
    /// compile-time row description, threaded for the same reason as
    /// [`begin_bind_execute`](Self::begin_bind_execute).
    #[inline]
    pub fn begin_parse_bind_execute(&mut self, result_oids: &[u32]) {
        self.seat_schema(result_oids);
        self.state = ActiveState::ParseBindExecuteAwaitingParseComplete;
    }

    /// Seat the engine to await the cache-MISS macro path's reply: a leading
    /// `Close`(statement)+Parse+Bind+Execute bundle → `CloseComplete`,
    /// `ParseComplete`, `BindComplete`, then the executed rows under one Sync.
    /// The leading `Close` makes the re-`Parse` idempotent (a `Close` of a
    /// nonexistent statement is a wire no-op), so a name the server may still
    /// hold is re-created without a duplicate-statement error. Threads the
    /// compile-time row schema exactly like
    /// [`begin_parse_bind_execute`](Self::begin_parse_bind_execute).
    #[inline]
    pub fn begin_close_parse_bind_execute(&mut self, result_oids: &[u32]) {
        self.seat_schema(result_oids);
        self.state = ActiveState::CloseParseBindExecuteAwaitingCloseComplete;
    }

    /// Seat the engine to await a bare `Execute`'s reply — a resume of an open
    /// portal (typically after a `PortalSuspended`). A resume sends no `Bind`,
    /// so there is no `BindComplete`: the reply is `DataRow`s then
    /// `CommandComplete`, or another `PortalSuspended`. This seats the
    /// post-bind data wait directly (a leading `BindComplete` here would be
    /// out-of-phase and a classified teardown). The portal's schema is
    /// re-threaded for the same reason as
    /// [`begin_bind_execute`](Self::begin_bind_execute).
    #[inline]
    pub fn begin_execute(&mut self, result_oids: &[u32]) {
        self.seat_schema(result_oids);
        self.state = ActiveState::BindAwaitingData;
    }

    /// Seat the engine to await a `Close`'s `CloseComplete` ack then its Sync
    /// `ReadyForQuery`. The ack carries no tag and no schema.
    #[inline]
    pub fn begin_close(&mut self) {
        self.state = ActiveState::CloseAwaitingComplete;
    }

    /// Seat the engine to drain a BATCHED `Close`+…+`Close`+`Sync` — any number
    /// of `CloseComplete` acks then the Sync's `ReadyForQuery`. REUSES the
    /// extended-tail RFQ waiter [`ExtendedAwaitingRfq`](ActiveState::ExtendedAwaitingRfq)
    /// (which now drains a `CloseComplete` silently), so NO new dispatch state is
    /// added — the inbound-hot `next_event` frame is byte-unchanged. No count is
    /// kept (the state drains until the RFQ), so it handles any number of closes.
    #[inline]
    pub fn begin_close_many(&mut self) {
        self.state = ActiveState::ExtendedAwaitingRfq;
    }

    /// Seat the engine to await the fused one-round-trip runtime-param batch's
    /// reply: `Parse`(unnamed) + `Bind` + `Describe`(portal) + `Execute` + `Sync`
    /// → `ParseComplete`, `BindComplete`, then the `Describe`(portal) answer
    /// (`RowDescription` or `NoData`), then the executed rows and the single Sync
    /// `ReadyForQuery`.
    ///
    /// Unlike [`begin_bind_execute`](Self::begin_bind_execute), this seats NO
    /// result OIDs — the schema is RECOVERED from the inline `Describe`(portal)
    /// answer, exactly as the dynamic `prepare` path recovered it from a separate
    /// `Describe` round trip. Clears the per-statement columns so a prior
    /// statement's schema cannot leak into a `NoData` (no-row) delivery.
    #[inline]
    pub fn begin_fused_parse_bind_describe_execute(&mut self) {
        self.reset_columns();
        self.state = ActiveState::FusedAwaitingParseComplete;
    }

    /// Shared bind/execute seating: thread the result schema and await
    /// `BindComplete`.
    #[inline]
    fn seat_bind(&mut self, result_oids: &[u32]) {
        self.seat_schema(result_oids);
        self.state = ActiveState::BindAwaitingBindComplete;
    }

    /// Seat the result-column type OIDs threaded from a `Describe` (or the
    /// macro's compile-time schema). Names are deliberately left empty — the
    /// execute path does not re-surface them. The existing buffers are reused
    /// (cleared then refilled) to avoid a fresh allocation per command.
    #[inline]
    fn seat_schema(&mut self, result_oids: &[u32]) {
        self.col_oids.clear();
        self.col_oids.extend_from_slice(result_oids);
        self.col_names.clear();
    }

    /// Pull the next active-phase event, borrowing the read buffer.
    ///
    /// Locates one inbound frame, runs the consuming dispatch, and returns the
    /// classified [`Event`]. Silent intermediate frames loop internally. The
    /// returned [`Event`] borrows `&mut self`, so holding it across the next
    /// mutating call (`read_slot` / `commit` / `next_event`) is E0499 — the
    /// no-escape wall.
    pub fn next_event(&mut self) -> Event<'_> {
        match self.drive() {
            // `Silent` is internal to `drive` (it loops on it) — it never
            // escapes; map it to `NeedMore` defensively rather than panicking.
            ActiveOutcome::Silent | ActiveOutcome::NeedMore => Event::NeedMore,
            ActiveOutcome::Idle => Event::Idle,
            ActiveOutcome::Deliver => Event::Deliver,
            ActiveOutcome::Suspended => Event::Suspended,
            ActiveOutcome::Close => Event::Close,
            ActiveOutcome::RowChunkEnd => Event::RowChunkEnd,
            ActiveOutcome::CopyDone => Event::CopyDone,
            ActiveOutcome::Overcap { count, max } => Event::Overcap { count, max },
            ActiveOutcome::Fail(l, s, e) => Event::Fail(self.lend(l, s, e)),
            ActiveOutcome::Notice(l, s, e) => Event::Notice(self.lend(l, s, e)),
            ActiveOutcome::Notify(l, s, e) => Event::Notify(self.lend(l, s, e)),
            ActiveOutcome::ParamStatus(l, s, e) => Event::ParamStatus(self.lend(l, s, e)),
            ActiveOutcome::Row(l, s, e) => Event::Row(self.lend(l, s, e)),
            ActiveOutcome::RowChunk(l, s, e) => Event::RowChunk(self.lend(l, s, e)),
            ActiveOutcome::CopyData(l, s, e) => Event::CopyData(self.lend(l, s, e)),
        }
    }

    /// Re-borrow a body span from its recorded source (the span-split second
    /// half: classify recorded the span, the cursor already advanced, this
    /// re-indexes — never re-parses).
    #[inline]
    #[must_use]
    fn lend(&self, src: Lend, start: usize, end: usize) -> &[u8] {
        match src {
            Lend::Ingest => self.ingest.frame_body(start, end),
            Lend::Prefix => match self.prefix.as_deref() {
                Some(prefix) => prefix.get(start..end).unwrap_or(&[]),
                None => &[],
            },
        }
    }

    /// Drive the framing + dispatch loop to the next surfaceable outcome.
    fn drive(&mut self) -> ActiveOutcome {
        loop {
            // Terminal short-circuit — enumerated, no wildcard.
            match self.state {
                ActiveState::Failed => return ActiveOutcome::Close,
                ActiveState::Idle
                | ActiveState::StreamingRows
                | ActiveState::AwaitingRfq
                | ActiveState::CopyOut
                | ActiveState::CopyOutAwaitingCc
                | ActiveState::CopyInActive
                | ActiveState::DrainAfterError
                | ActiveState::DrainOvercapToRfq
                | ActiveState::ParseAwaitingParseComplete
                | ActiveState::ParseDescribeStmtAwaitingParseComplete
                | ActiveState::ParseBindExecuteAwaitingParseComplete
                | ActiveState::CloseParseBindExecuteAwaitingCloseComplete
                | ActiveState::DescribeStmtAwaitingParamDesc
                | ActiveState::DescribeAwaitingRowDescOrNoData
                | ActiveState::BindAwaitingBindComplete
                | ActiveState::BindAwaitingData
                | ActiveState::ExtendedAwaitingRfq
                | ActiveState::CloseAwaitingComplete
                | ActiveState::FusedAwaitingParseComplete
                | ActiveState::FusedAwaitingBindComplete
                | ActiveState::FusedAwaitingRowDescOrNoData => {}
            }

            // Continue an in-progress oversize stream before any new framing.
            // `Silent` loops; everything else surfaces. `if matches!` keeps the
            // wildcard out of an explicit enum-match arm (the macro form is
            // exempt from the scoped wildcard deny).
            if self.oversize.is_some() {
                let outcome = self.step_oversize();
                if matches!(outcome, ActiveOutcome::Silent) {
                    continue;
                }
                return outcome;
            }

            match self.ingest.peek_header() {
                HeaderParse::Empty | HeaderParse::Incomplete => return ActiveOutcome::NeedMore,
                HeaderParse::MalformedLength { .. } => {
                    self.state = ActiveState::Failed;
                    return ActiveOutcome::Close;
                }
                HeaderParse::FrameTooLarge { declared } => {
                    let outcome = self.begin_oversize(declared);
                    if matches!(outcome, ActiveOutcome::Silent) {
                        continue;
                    }
                    return outcome;
                }
                HeaderParse::Ok { .. } => match self.ingest.take_frame() {
                    // Header parsed but the whole body is not yet buffered.
                    None => return ActiveOutcome::NeedMore,
                    Some((tag, start, end)) => {
                        // Asynchronous frames are surfaced regardless of command
                        // phase (active-phase steady state), and never advance
                        // the command state machine.
                        match tag {
                            T_NOTICE => return ActiveOutcome::Notice(Lend::Ingest, start, end),
                            T_NOTIFY => return ActiveOutcome::Notify(Lend::Ingest, start, end),
                            T_PARAM_STATUS => {
                                return ActiveOutcome::ParamStatus(Lend::Ingest, start, end)
                            }
                            _ => {
                                let outcome = self.step_frame(tag, start, end);
                                if matches!(outcome, ActiveOutcome::Silent) {
                                    continue;
                                }
                                return outcome;
                            }
                        }
                    }
                },
            }
        }
    }

    /// Per-frame transition, matching DIRECTLY on [`ActiveState`] (no
    /// `ProtoState` lift). Every state is enumerated; within a state an
    /// out-of-phase tag is a classified teardown.
    fn step_frame(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match self.state {
            ActiveState::Idle => self.step_idle(tag, start, end),
            ActiveState::StreamingRows => self.step_streaming(tag, start, end),
            ActiveState::AwaitingRfq => self.step_awaiting_rfq(tag, start, end),
            ActiveState::CopyOut => self.step_copy_out(tag, start, end),
            ActiveState::CopyOutAwaitingCc => self.step_copy_out_awaiting_cc(tag, start, end),
            ActiveState::CopyInActive => self.step_copy_in_active(tag, start, end),
            ActiveState::DrainAfterError => self.step_drain_after_error(tag, start, end),
            ActiveState::DrainOvercapToRfq => self.step_drain_overcap(tag, start, end),
            ActiveState::ParseAwaitingParseComplete => {
                self.step_parse_awaiting_parse_complete(tag, start, end)
            }
            ActiveState::ParseDescribeStmtAwaitingParseComplete => {
                self.step_parse_describe_awaiting_parse_complete(tag, start, end)
            }
            ActiveState::ParseBindExecuteAwaitingParseComplete => {
                self.step_parse_bind_execute_awaiting_parse_complete(tag, start, end)
            }
            ActiveState::CloseParseBindExecuteAwaitingCloseComplete => {
                self.step_close_parse_bind_execute_awaiting_close_complete(tag, start, end)
            }
            ActiveState::DescribeStmtAwaitingParamDesc => {
                self.step_describe_awaiting_param_desc(tag, start, end)
            }
            ActiveState::DescribeAwaitingRowDescOrNoData => {
                self.step_describe_awaiting_rowdesc_or_nodata(tag, start, end)
            }
            ActiveState::BindAwaitingBindComplete => {
                self.step_bind_awaiting_bind_complete(tag, start, end)
            }
            ActiveState::BindAwaitingData => self.step_bind_awaiting_data(tag, start, end),
            ActiveState::ExtendedAwaitingRfq => self.step_extended_awaiting_rfq(tag, start, end),
            ActiveState::CloseAwaitingComplete => {
                self.step_close_awaiting_complete(tag, start, end)
            }
            // The fused one-round-trip setup chain (ParseComplete → BindComplete →
            // RowDescription/NoData) routes to ONE cold handler that re-matches the
            // phase: the whole chain is a one-time-per-query setup before the row
            // stream, so keeping it a single `#[inline(never)]` call keeps
            // next_event's hot frame from carrying three more setup arms it never
            // runs on a DataRow.
            ActiveState::FusedAwaitingParseComplete
            | ActiveState::FusedAwaitingBindComplete
            | ActiveState::FusedAwaitingRowDescOrNoData => self.step_fused(tag, start, end),
            // Unreachable: the drive loop short-circuits `Failed` before
            // calling `step_frame`. Classified, never wildcarded.
            ActiveState::Failed => ActiveOutcome::Close,
        }
    }

    /// `Idle` — the first response frame of a command branches the phase.
    fn step_idle(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match tag {
            T_ROW_DESC => self.open_row_stream(start, end),
            T_COMMAND_COMPLETE => self.complete_command(start, end, ActiveState::AwaitingRfq),
            T_EMPTY_QUERY => self.complete_empty(ActiveState::AwaitingRfq),
            T_COPY_OUT => self.open_copy_out(start, end),
            T_COPY_IN => self.open_copy_in(start, end),
            T_ERROR => self.fail_recoverable(start, end),
            // A bare trailing `ReadyForQuery` with no command in flight is
            // benign — stay idle. (A real command always opens with a
            // payload frame.)
            T_READY_FOR_QUERY => self.parse_rfq(start, end),
            _ => self.teardown(),
        }
    }

    /// `StreamingRows` — `DataRow`s until `CommandComplete` / `ErrorResponse`.
    fn step_streaming(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match tag {
            T_DATA_ROW => self.emit_row(start, end),
            T_COMMAND_COMPLETE => self.complete_command(start, end, ActiveState::AwaitingRfq),
            T_ERROR => self.fail_recoverable(start, end),
            // A second `RowDescription` mid-stream re-describes the open
            // stream — a protocol violation, classified teardown.
            _ => self.teardown(),
        }
    }

    /// `AwaitingRfq` — the command boundary, or the next statement in a batch.
    fn step_awaiting_rfq(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match tag {
            T_READY_FOR_QUERY => self.parse_rfq(start, end),
            // Next statement in a `;`-separated batch.
            T_COMMAND_COMPLETE => self.complete_command(start, end, ActiveState::AwaitingRfq),
            T_ROW_DESC => self.open_row_stream(start, end),
            T_EMPTY_QUERY => self.complete_empty(ActiveState::AwaitingRfq),
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `CopyOut` — `CopyData` frames until `CopyDone` / `ErrorResponse`.
    fn step_copy_out(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match tag {
            T_COPY_DATA => ActiveOutcome::CopyData(Lend::Ingest, start, end),
            T_COPY_DONE => {
                self.state = ActiveState::CopyOutAwaitingCc;
                ActiveOutcome::CopyDone
            }
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `CopyOutAwaitingCc` — the trailing `CommandComplete` after `CopyDone`.
    fn step_copy_out_awaiting_cc(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match tag {
            T_COMMAND_COMPLETE => self.complete_command(start, end, ActiveState::AwaitingRfq),
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `CopyInActive` — the server `CommandComplete` once the client's COPY IN
    /// stream is acknowledged.
    fn step_copy_in_active(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match tag {
            T_COMMAND_COMPLETE => self.complete_command(start, end, ActiveState::AwaitingRfq),
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `DrainAfterError` — the trailing `ReadyForQuery` recovers to idle.
    fn step_drain_after_error(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match tag {
            T_READY_FOR_QUERY => self.parse_rfq(start, end),
            _ => self.teardown(),
        }
    }

    /// `DrainOvercapToRfq` — recovering from a too-wide result the client
    /// rejected: SWALLOW every frame until the trailing `ReadyForQuery`, which
    /// recovers to idle. Unlike [`step_drain_after_error`](Self::step_drain_after_error)
    /// (a server error aborts the query, so the next frame IS the RFQ and anything
    /// else is a teardown), the server here is actively streaming a full result
    /// the client cannot represent — its `DataRow`s, `CommandComplete`, and even a
    /// second statement's `RowDescription` in a simple-query batch must all be
    /// discarded, so every non-RFQ frame is a silent swallow, not a teardown. An
    /// oversize frame during the drain is skipped by
    /// [`begin_oversize`](Self::begin_oversize)'s
    /// [`Skip`](OversizeMode::Skip) mode; the asynchronous `Notice`/`Notify`/
    /// `ParameterStatus` frames still surface (handled above the per-state dispatch
    /// in [`drive`](Self::drive)), so a notification in the recovery window is
    /// captured, never dropped.
    ///
    /// `#[cold]` + `#[inline(never)]`: the over-cap drain is a rare recovery path
    /// (reached only from a nonconforming server), so its body is kept OUT of
    /// [`next_event`](Self::next_event)'s hot frame — the state dispatch reaches it
    /// through a call, never an inlined arm, so the per-`DataRow` hot path does not
    /// carry the drain's instructions.
    #[cold]
    #[inline(never)]
    fn step_drain_overcap(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match tag {
            T_READY_FOR_QUERY => self.parse_rfq(start, end),
            _ => ActiveOutcome::Silent,
        }
    }

    // ── extended query protocol (Parse / Describe / Bind+Execute / Close) ──

    /// `ParseAwaitingParseComplete` — a bare `Parse` awaits its `ParseComplete`
    /// ack (no tag, no rows), then the Sync's `ReadyForQuery`.
    fn step_parse_awaiting_parse_complete(
        &mut self,
        tag: u8,
        start: usize,
        end: usize,
    ) -> ActiveOutcome {
        match tag {
            T_PARSE_COMPLETE => self.deliver_empty(ActiveState::ExtendedAwaitingRfq),
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `ParseDescribeStmtAwaitingParseComplete` — a combined
    /// Parse+Describe(statement)+Sync bundle: `ParseComplete` here is followed by
    /// the statement `Describe` answer (`ParameterDescription`, …), not the Sync
    /// boundary, so it advances silently into the describe phase.
    fn step_parse_describe_awaiting_parse_complete(
        &mut self,
        tag: u8,
        start: usize,
        end: usize,
    ) -> ActiveOutcome {
        match tag {
            T_PARSE_COMPLETE => {
                self.state = ActiveState::DescribeStmtAwaitingParamDesc;
                ActiveOutcome::Silent
            }
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `ParseBindExecuteAwaitingParseComplete` — a combined Parse+Bind+Execute
    /// bundle: `ParseComplete` here is followed by `BindComplete`, not the Sync
    /// boundary, so it advances silently into the bind phase.
    fn step_parse_bind_execute_awaiting_parse_complete(
        &mut self,
        tag: u8,
        start: usize,
        end: usize,
    ) -> ActiveOutcome {
        match tag {
            T_PARSE_COMPLETE => {
                self.state = ActiveState::BindAwaitingBindComplete;
                ActiveOutcome::Silent
            }
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `CloseParseBindExecuteAwaitingCloseComplete` — the cache-MISS macro path
    /// leads with a `Close`(statement) so the trailing `Parse` is idempotent (a
    /// `Close` of a nonexistent statement is a wire no-op). The `CloseComplete`
    /// here is followed by the `ParseComplete` of that trailing `Parse`, so it
    /// advances silently into the existing Parse+Bind+Execute chain — reusing that
    /// whole proven state sequence rather than duplicating it.
    fn step_close_parse_bind_execute_awaiting_close_complete(
        &mut self,
        tag: u8,
        start: usize,
        end: usize,
    ) -> ActiveOutcome {
        match tag {
            T_CLOSE_COMPLETE => {
                self.state = ActiveState::ParseBindExecuteAwaitingParseComplete;
                ActiveOutcome::Silent
            }
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `DescribeStmtAwaitingParamDesc` — a statement `Describe` answers with
    /// `ParameterDescription` first. Its parameter OIDs are not part of the
    /// row-result observable, so the frame advances the phase silently.
    fn step_describe_awaiting_param_desc(
        &mut self,
        tag: u8,
        start: usize,
        end: usize,
    ) -> ActiveOutcome {
        match tag {
            T_PARAM_DESC => {
                self.state = ActiveState::DescribeAwaitingRowDescOrNoData;
                ActiveOutcome::Silent
            }
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `DescribeAwaitingRowDescOrNoData` — the `Describe` resolves to a
    /// `RowDescription` (the statement returns rows) or `NoData` (it does not).
    /// Either way the describe completes a tagless, no-rows boundary whose
    /// recovered schema is surfaced via the column accessors at the delivery.
    fn step_describe_awaiting_rowdesc_or_nodata(
        &mut self,
        tag: u8,
        start: usize,
        end: usize,
    ) -> ActiveOutcome {
        match tag {
            T_ROW_DESC => self.record_described_rows(start, end),
            T_NO_DATA => {
                self.reset_columns();
                self.deliver_empty(ActiveState::ExtendedAwaitingRfq)
            }
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `BindAwaitingBindComplete` — a `Bind`+`Execute` bundle awaits the
    /// server's `BindComplete` before any result data.
    fn step_bind_awaiting_bind_complete(
        &mut self,
        tag: u8,
        start: usize,
        end: usize,
    ) -> ActiveOutcome {
        match tag {
            T_BIND_COMPLETE => {
                self.state = ActiveState::BindAwaitingData;
                ActiveOutcome::Silent
            }
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `BindAwaitingData` — after `BindComplete` the Execute streams `DataRow`s
    /// (each lent in place; oversize rows reuse the Sub-A `RowChunk` path) and
    /// completes with `CommandComplete`, OR — for a schema-less DML Execute —
    /// goes straight to `CommandComplete` with no `DataRow`, OR pauses at a row
    /// cap with `PortalSuspended`. The reply frame branches the phase; there is
    /// no separate streaming state because the row reader is the same one
    /// `DataRow`s use everywhere.
    fn step_bind_awaiting_data(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match tag {
            T_DATA_ROW => self.emit_row(start, end),
            T_COMMAND_COMPLETE => self.complete_command(start, end, ActiveState::ExtendedAwaitingRfq),
            T_PORTAL_SUSPENDED => self.deliver_suspended(ActiveState::ExtendedAwaitingRfq),
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `ExtendedAwaitingRfq` — every extended-protocol command ends at the
    /// single `ReadyForQuery` of its Sync. No `CommandComplete`/`RowDescription`
    /// continuation is legal here (that is the simple-query batch shape); one
    /// Sync closes one command.
    fn step_extended_awaiting_rfq(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match tag {
            T_READY_FOR_QUERY => self.parse_rfq(start, end),
            // A batched `Close`+…+`Close`+`Sync` (the pool-reset cache clear,
            // seated here by `begin_close_many`) drains each `CloseComplete` ack
            // SILENTLY until the Sync's RFQ. On a NORMAL extended-query tail the
            // server never sends a `CloseComplete` here (the setup states consumed
            // the one for a single `Close`), so this arm fires ONLY on the batched
            // path — it does not loosen the single-statement tail's strictness.
            T_CLOSE_COMPLETE => ActiveOutcome::Silent,
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// `CloseAwaitingComplete` — a `Close` awaits its `CloseComplete` ack (no
    /// tag, no rows), then the Sync's `ReadyForQuery`.
    fn step_close_awaiting_complete(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match tag {
            T_CLOSE_COMPLETE => self.deliver_empty(ActiveState::ExtendedAwaitingRfq),
            T_ERROR => self.fail_recoverable(start, end),
            _ => self.teardown(),
        }
    }

    /// The fused one-round-trip setup chain, matching the fused sub-phase
    /// internally so `step_frame` carries ONE cold arm for the whole chain.
    ///
    /// - `FusedAwaitingParseComplete`: `ParseComplete` (`'1'`) → silent advance to
    ///   the bind wait. Any other non-error tag is out-of-phase (teardown).
    /// - `FusedAwaitingBindComplete`: `BindComplete` (`'2'`) → silent advance to
    ///   the row-desc-or-no-data wait.
    /// - `FusedAwaitingRowDescOrNoData`: the `Describe`(portal) answer —
    ///   `RowDescription` (`'T'`) → capture the recovered schema (OIDs + names)
    ///   and enter the (existing) `BindAwaitingData` row stream; `NoData` (`'n'`)
    ///   → clear the columns and enter `BindAwaitingData` for the bare
    ///   `CommandComplete` of a no-row command.
    ///
    /// Every sub-phase classifies a mid-fusion `ErrorResponse` as the recoverable
    /// [`fail_recoverable`](Self::fail_recoverable) (a `Parse` / `Bind` /
    /// describe-time error parks a drain to the recovering `ReadyForQuery`, so the
    /// connection survives), and any other tag as a classified teardown.
    ///
    /// `#[inline(never)]`: the whole chain runs at most once per query (before the
    /// row stream), and its `RowDescription`-capture arm pulls in the
    /// column-parsing + `Vec` allocation register pressure. Keeping it out of line
    /// keeps that pressure — and the setup instructions — OFF `next_event`'s hot
    /// frame: the per-row DataRow arm never reaches here, and next_event's frame
    /// stays at its lean 128-byte size rather than regrowing to fit this setup.
    /// Deliberately NOT `#[cold]`: on the pinned toolchain `#[cold]` only enlarges
    /// the outlined body; `#[inline(never)]` alone already lifts it off the hot
    /// frame.
    #[inline(never)]
    fn step_fused(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match self.state {
            ActiveState::FusedAwaitingParseComplete => match tag {
                T_PARSE_COMPLETE => {
                    self.state = ActiveState::FusedAwaitingBindComplete;
                    ActiveOutcome::Silent
                }
                T_ERROR => self.fail_recoverable(start, end),
                _ => self.teardown(),
            },
            ActiveState::FusedAwaitingBindComplete => match tag {
                T_BIND_COMPLETE => {
                    self.state = ActiveState::FusedAwaitingRowDescOrNoData;
                    ActiveOutcome::Silent
                }
                T_ERROR => self.fail_recoverable(start, end),
                _ => self.teardown(),
            },
            ActiveState::FusedAwaitingRowDescOrNoData => match tag {
                T_ROW_DESC => {
                    let parsed = parse_row_desc_owned(self.ingest.frame_body(start, end));
                    self.apply_fused_row_stream(parsed)
                }
                T_NO_DATA => {
                    self.reset_columns();
                    self.state = ActiveState::BindAwaitingData;
                    ActiveOutcome::Silent
                }
                T_ERROR => self.fail_recoverable(start, end),
                _ => self.teardown(),
            },
            // `step_frame` only routes the three fused states here; every other
            // state is dispatched by its own arm. Enumerated (no wildcard) so a
            // future state cannot silently fall into the fused handler.
            ActiveState::Idle
            | ActiveState::StreamingRows
            | ActiveState::AwaitingRfq
            | ActiveState::CopyOut
            | ActiveState::CopyOutAwaitingCc
            | ActiveState::CopyInActive
            | ActiveState::DrainAfterError
            | ActiveState::DrainOvercapToRfq
            | ActiveState::ParseAwaitingParseComplete
            | ActiveState::ParseDescribeStmtAwaitingParseComplete
            | ActiveState::ParseBindExecuteAwaitingParseComplete
            | ActiveState::CloseParseBindExecuteAwaitingCloseComplete
            | ActiveState::DescribeStmtAwaitingParamDesc
            | ActiveState::DescribeAwaitingRowDescOrNoData
            | ActiveState::BindAwaitingBindComplete
            | ActiveState::BindAwaitingData
            | ActiveState::ExtendedAwaitingRfq
            | ActiveState::CloseAwaitingComplete
            | ActiveState::Failed => self.teardown(),
        }
    }

    // ── transition leaves ──

    /// `RowDescription` → record columns/OIDs, open the row stream (silent).
    fn open_row_stream(&mut self, start: usize, end: usize) -> ActiveOutcome {
        let parsed = parse_row_desc_owned(self.ingest.frame_body(start, end));
        self.apply_open_row_stream(parsed)
    }

    /// Open the row stream from a parsed schema, RECOVER from a too-wide one, or
    /// tear down on a malformed frame. Shared by the in-buffer and
    /// Sub-C-accumulated `RowDescription` paths.
    fn apply_open_row_stream(
        &mut self,
        parsed: Result<(Vec<u32>, Vec<String>), crate::error::ProtocolError>,
    ) -> ActiveOutcome {
        match parsed {
            Ok((oids, names)) => {
                self.col_oids = oids;
                self.col_names = names;
                self.state = ActiveState::StreamingRows;
                ActiveOutcome::Silent
            }
            Err(crate::error::ProtocolError::TooManyColumns { count, max }) => {
                self.enter_overcap_recovery(count, max)
            }
            Err(_) => self.teardown(),
        }
    }

    /// Capture the fused batch's inline `Describe`(portal) schema (OIDs + names)
    /// and enter the (existing) `BindAwaitingData` row stream — the
    /// extended-protocol analog of [`apply_open_row_stream`](Self::apply_open_row_stream)
    /// (which enters the simple-query `StreamingRows` that completes to
    /// `AwaitingRfq`; the fused path completes to `ExtendedAwaitingRfq` via
    /// `BindAwaitingData`, one Sync closing one command). The captured columns
    /// survive the DataRow stream through the `CommandComplete` `Deliver` (reset
    /// only at the trailing `ReadyForQuery`), so the runtime consumer reads the
    /// recovered OIDs + names at the delivery exactly as the separate-`prepare`
    /// path surfaced them. Shared by the in-buffer and Sub-C-accumulated paths.
    fn apply_fused_row_stream(
        &mut self,
        parsed: Result<(Vec<u32>, Vec<String>), crate::error::ProtocolError>,
    ) -> ActiveOutcome {
        match parsed {
            Ok((oids, names)) => {
                self.col_oids = oids;
                self.col_names = names;
                self.state = ActiveState::BindAwaitingData;
                ActiveOutcome::Silent
            }
            Err(crate::error::ProtocolError::TooManyColumns { count, max }) => {
                self.enter_overcap_recovery(count, max)
            }
            Err(_) => self.teardown(),
        }
    }

    /// `DataRow` → lend the whole row body (it fit the buffer; the oversize
    /// path handles the buffer-overflow case before `step_frame`).
    fn emit_row(&mut self, start: usize, end: usize) -> ActiveOutcome {
        ActiveOutcome::Row(Lend::Ingest, start, end)
    }

    /// `CommandComplete` → parse + store the tag, move to `next`, surface
    /// `Deliver`.
    fn complete_command(&mut self, start: usize, end: usize, next: ActiveState) -> ActiveOutcome {
        let body = self.ingest.frame_body(start, end);
        match parse_command_tag_bytes(body) {
            Ok(tag) => {
                self.command_tag = Some(tag);
                self.state = next;
                ActiveOutcome::Deliver
            }
            Err(_) => self.teardown(),
        }
    }

    /// `EmptyQueryResponse` → an empty-tag statement boundary.
    fn complete_empty(&mut self, next: ActiveState) -> ActiveOutcome {
        self.command_tag = Some(CommandTag::EMPTY);
        self.state = next;
        ActiveOutcome::Deliver
    }

    /// Deliver a tagless, no-rows command boundary — the extended-protocol acks
    /// (`ParseComplete` / `CloseComplete`) and the describe completion, none of
    /// which carry a `CommandComplete` tag. Clears the tag so the boundary
    /// surfaces an empty tag, and moves to `next` (the trailing-RFQ wait).
    fn deliver_empty(&mut self, next: ActiveState) -> ActiveOutcome {
        self.command_tag = None;
        self.state = next;
        ActiveOutcome::Deliver
    }

    /// `PortalSuspended` (`'s'`) → a row-limited Execute paused at its cap. The
    /// portal stays open and no `CommandComplete` tag is produced, so this
    /// surfaces the rows fetched so far as the typed [`Event::Suspended`]
    /// terminal (distinct from a completed [`Event::Deliver`]), then awaits the
    /// trailing `ReadyForQuery`. The suspend is carried by the outcome's
    /// variant — there is no stateful flag to read after the event.
    fn deliver_suspended(&mut self, next: ActiveState) -> ActiveOutcome {
        self.state = next;
        ActiveOutcome::Suspended
    }

    /// `RowDescription` arriving for a `Describe` → record the recovered schema
    /// (column OIDs + names) and complete the describe as a tagless boundary.
    /// The schema is surfaced via [`current_type_oids`](Self::current_type_oids)
    /// / [`current_column_names`](Self::current_column_names) at the delivery so
    /// a later `Bind`+`Execute` against the same statement can thread the OIDs
    /// back in via [`begin_bind_execute`](Self::begin_bind_execute).
    fn record_described_rows(&mut self, start: usize, end: usize) -> ActiveOutcome {
        let parsed = parse_row_desc_owned(self.ingest.frame_body(start, end));
        self.apply_record_described_rows(parsed)
    }

    /// Record a `Describe`'s recovered schema (or tear down on a parse fail).
    /// Shared by the in-buffer and Sub-C-accumulated `RowDescription` paths.
    fn apply_record_described_rows(
        &mut self,
        parsed: Result<(Vec<u32>, Vec<String>), crate::error::ProtocolError>,
    ) -> ActiveOutcome {
        match parsed {
            Ok((oids, names)) => {
                self.col_oids = oids;
                self.col_names = names;
                self.deliver_empty(ActiveState::ExtendedAwaitingRfq)
            }
            Err(crate::error::ProtocolError::TooManyColumns { count, max }) => {
                self.enter_overcap_recovery(count, max)
            }
            Err(_) => self.teardown(),
        }
    }

    /// Append one drained chunk of an oversize Sub-C frame to the accumulator.
    /// Disjoint field borrows: the ingest read and the accumulator write touch
    /// different fields of `self`.
    ///
    /// `#[cold]` + `#[inline(never)]`: oversize handling is a rare control-frame
    /// path (a `RowDescription` wider than the ingest buffer), so it is kept OUT
    /// of [`next_event`](Self::next_event)'s hot frame rather than inlined into
    /// the per-row dispatch it never runs on.
    #[cold]
    #[inline(never)]
    fn append_oversize_accum(&mut self, start: usize, end: usize) {
        let src = self.ingest.frame_body(start, end);
        self.oversize_accum.extend_from_slice(src);
    }

    /// Dispatch a fully-accumulated oversize `RowDescription` by the current
    /// command phase — the Sub-C analog of the per-state `'T'` arms. Mirrors
    /// exactly where an in-buffer `RowDescription` is legal (open a row stream at
    /// `Idle`/`AwaitingRfq`, record a describe answer at the describe wait); any
    /// other phase is the same classified teardown as the in-buffer path.
    ///
    /// `#[cold]` + `#[inline(never)]`: the Sub-C oversize dispatch is a rare
    /// control-frame path, kept off [`next_event`](Self::next_event)'s hot frame.
    #[cold]
    #[inline(never)]
    fn dispatch_accumulated_row_desc(&mut self) -> ActiveOutcome {
        match self.state {
            ActiveState::Idle | ActiveState::AwaitingRfq => {
                let parsed = parse_row_desc_owned(&self.oversize_accum);
                self.apply_open_row_stream(parsed)
            }
            ActiveState::DescribeAwaitingRowDescOrNoData => {
                let parsed = parse_row_desc_owned(&self.oversize_accum);
                self.apply_record_described_rows(parsed)
            }
            // A wide `Describe`(portal) `RowDescription` in the fused batch: capture
            // the recovered schema and enter the row stream, mirroring the in-buffer
            // `FusedAwaitingRowDescOrNoData` `'T'` arm.
            ActiveState::FusedAwaitingRowDescOrNoData => {
                let parsed = parse_row_desc_owned(&self.oversize_accum);
                self.apply_fused_row_stream(parsed)
            }
            ActiveState::StreamingRows
            | ActiveState::CopyOut
            | ActiveState::CopyOutAwaitingCc
            | ActiveState::CopyInActive
            | ActiveState::DrainAfterError
            | ActiveState::DrainOvercapToRfq
            | ActiveState::ParseAwaitingParseComplete
            | ActiveState::ParseDescribeStmtAwaitingParseComplete
            | ActiveState::ParseBindExecuteAwaitingParseComplete
            | ActiveState::CloseParseBindExecuteAwaitingCloseComplete
            | ActiveState::DescribeStmtAwaitingParamDesc
            | ActiveState::BindAwaitingBindComplete
            | ActiveState::BindAwaitingData
            | ActiveState::ExtendedAwaitingRfq
            | ActiveState::CloseAwaitingComplete
            // A `RowDescription` is never legal while awaiting `'1'` / `'2'`.
            | ActiveState::FusedAwaitingParseComplete
            | ActiveState::FusedAwaitingBindComplete
            | ActiveState::Failed => self.teardown(),
        }
    }

    /// `CopyOutResponse` → validate the header, open COPY OUT (silent).
    fn open_copy_out(&mut self, start: usize, end: usize) -> ActiveOutcome {
        let body = self.ingest.frame_body(start, end);
        match parse_copy_response_header(body) {
            Ok(_) => {
                self.reset_columns();
                self.state = ActiveState::CopyOut;
                ActiveOutcome::Silent
            }
            Err(_) => self.teardown(),
        }
    }

    /// `CopyInResponse` → validate the header, open COPY IN (silent).
    fn open_copy_in(&mut self, start: usize, end: usize) -> ActiveOutcome {
        let body = self.ingest.frame_body(start, end);
        match parse_copy_response_header(body) {
            Ok(_) => {
                self.reset_columns();
                self.state = ActiveState::CopyInActive;
                ActiveOutcome::Silent
            }
            Err(_) => self.teardown(),
        }
    }

    /// `ErrorResponse` in a query phase → surface `Fail`, park a drain to the
    /// recovering `ReadyForQuery` (the connection survives a query-level error).
    fn fail_recoverable(&mut self, start: usize, end: usize) -> ActiveOutcome {
        self.state = ActiveState::DrainAfterError;
        ActiveOutcome::Fail(Lend::Ingest, start, end)
    }

    /// A well-formed but too-wide `RowDescription` (its column count exceeds
    /// [`MAX_ROW_COLUMNS`](crate::decode::MAX_ROW_COLUMNS)) → surface the
    /// classified `TooManyColumns` and park a drain that SWALLOWS the in-flight
    /// result to the trailing `ReadyForQuery`. This is the documented recoverable
    /// path (the connection survives; the caller retries with a narrower
    /// projection), distinct from a MALFORMED frame (a framing desync that
    /// tears the connection down via [`teardown`](Self::teardown)): the frame
    /// parsed cleanly, so the stream position is known and every following frame
    /// can be discarded to the recovering RFQ.
    ///
    /// Unlike [`fail_recoverable`](Self::fail_recoverable) (a server error aborts
    /// the query, so only the RFQ follows and the drain is
    /// [`DrainAfterError`](ActiveState::DrainAfterError)), the server here streams
    /// a FULL result the client rejected, so the drain
    /// ([`DrainOvercapToRfq`](ActiveState::DrainOvercapToRfq)) swallows every
    /// frame — including a wide `DataRow` that itself exceeds the ingest buffer —
    /// until the RFQ. `count`/`max` ride the outcome so the driver names the exact
    /// limit.
    ///
    /// `#[cold]` + `#[inline(never)]`: reached only from the (cold) RowDescription
    /// arms, never the hot `DataRow` frame, so it is kept out of line.
    #[cold]
    #[inline(never)]
    fn enter_overcap_recovery(&mut self, count: usize, max: usize) -> ActiveOutcome {
        self.state = ActiveState::DrainOvercapToRfq;
        ActiveOutcome::Overcap { count, max }
    }

    /// `ReadyForQuery` → validate the 1-byte transaction status, record it,
    /// reset the per-statement columns, return to idle.
    fn parse_rfq(&mut self, start: usize, end: usize) -> ActiveOutcome {
        let body = self.ingest.frame_body(start, end);
        match body {
            [byte] => match TxStatus::try_from_byte(*byte) {
                Ok(tx) => {
                    self.tx_status = tx;
                    self.reset_columns();
                    self.state = ActiveState::Idle;
                    ActiveOutcome::Idle
                }
                Err(_) => self.teardown(),
            },
            _ => self.teardown(),
        }
    }

    /// Classified teardown: a wire-illegal frame for the current phase. The
    /// connection goes terminal and the socket must close.
    fn teardown(&mut self) -> ActiveOutcome {
        self.state = ActiveState::Failed;
        ActiveOutcome::Close
    }

    /// Clear the per-statement column metadata at a command boundary.
    #[inline]
    fn reset_columns(&mut self) {
        self.col_oids.clear();
        self.col_names.clear();
    }

    // ── Fused-prelude drain (the swallowing twin of the main dispatch) ──────
    //
    // Kept SEPARATE from `next_event`/`drive`/`step_frame` so the inbound hot
    // dispatch is byte-identical whether or not a prelude is armed: the whole
    // subtree below is reachable only from the pump's cold prelude pre-drain, never
    // from `next_event`. `#[cold]` + `#[inline(never)]` so it cannot fold into any
    // caller's hot frame (it runs at most once per transaction / pool checkout).

    /// Pull the next event while DRAINING a fused prelude — the swallowing twin of
    /// [`next_event`](Self::next_event), used only while
    /// [`draining_prelude`](Self::draining_prelude).
    ///
    /// Routes frames through [`step_prelude`](Self::step_prelude) (which swallows
    /// the prelude's own rows/completions and never touches the seated command's
    /// columns) instead of the seated [`ActiveState`]. Only NeedMore / Idle (the
    /// prelude's trailing RFQ) / the async frames / a fatal Close can surface; the
    /// prelude's own row/deliver/copy frames are swallowed internally.
    #[cold]
    #[inline(never)]
    pub fn next_prelude_event(&mut self) -> Event<'_> {
        match self.drive_prelude() {
            ActiveOutcome::Silent | ActiveOutcome::NeedMore => Event::NeedMore,
            ActiveOutcome::Idle => Event::Idle,
            ActiveOutcome::Close => Event::Close,
            ActiveOutcome::Notice(l, s, e) => Event::Notice(self.lend(l, s, e)),
            ActiveOutcome::Notify(l, s, e) => Event::Notify(self.lend(l, s, e)),
            ActiveOutcome::ParamStatus(l, s, e) => Event::ParamStatus(self.lend(l, s, e)),
            // The prelude drain swallows its OWN results (rows, deliveries,
            // suspends, copies, oversize) — `drive_prelude` never returns any of
            // these. Classify each as a fatal teardown rather than a silent misframe
            // (Enumerated, no wildcard: a future outcome forces a decision here).
            ActiveOutcome::Deliver
            | ActiveOutcome::Suspended
            | ActiveOutcome::RowChunkEnd
            | ActiveOutcome::CopyDone
            | ActiveOutcome::Fail(..)
            // A fixed prelude returns no rows, so it cannot over-cap — an
            // `Overcap` here is as impossible as a `Row` and shares the teardown.
            | ActiveOutcome::Overcap { .. }
            | ActiveOutcome::Row(..)
            | ActiveOutcome::RowChunk(..)
            | ActiveOutcome::CopyData(..) => Event::Close,
        }
    }

    /// Drive the framing + prelude dispatch loop to the next surfaceable outcome —
    /// the prelude twin of [`drive`](Self::drive).
    ///
    /// A `BEGIN` / `COMMIT` / `RESET` reply is small and never oversize, so this
    /// omits `drive`'s oversize + `Failed`-short-circuit machinery: a malformed or
    /// oversize frame here is unexpected and a fatal teardown. Asynchronous frames
    /// (`NOTICE` / `NOTIFY` / `ParameterStatus`) surface regardless of phase,
    /// exactly as in `drive`, so a notification riding the prelude's response is
    /// never dropped.
    #[cold]
    #[inline(never)]
    fn drive_prelude(&mut self) -> ActiveOutcome {
        loop {
            match self.ingest.peek_header() {
                HeaderParse::Empty | HeaderParse::Incomplete => return ActiveOutcome::NeedMore,
                HeaderParse::MalformedLength { .. } | HeaderParse::FrameTooLarge { .. } => {
                    return self.prelude_teardown();
                }
                HeaderParse::Ok { .. } => match self.ingest.take_frame() {
                    None => return ActiveOutcome::NeedMore,
                    Some((tag, start, end)) => match tag {
                        T_NOTICE => return ActiveOutcome::Notice(Lend::Ingest, start, end),
                        T_NOTIFY => return ActiveOutcome::Notify(Lend::Ingest, start, end),
                        T_PARAM_STATUS => {
                            return ActiveOutcome::ParamStatus(Lend::Ingest, start, end)
                        }
                        _ => {
                            let outcome = self.step_prelude(tag, start, end);
                            if matches!(outcome, ActiveOutcome::Silent) {
                                continue;
                            }
                            return outcome;
                        }
                    },
                },
            }
        }
    }

    /// Per-frame prelude transition. The ONLY deferred prelude is a transaction
    /// `BEGIN`, whose reply is exactly `CommandComplete` + `ReadyForQuery`: swallow
    /// the `CommandComplete` (returning [`Silent`](ActiveOutcome::Silent)) and end
    /// the drain on the trailing `ReadyForQuery` via
    /// [`finish_prelude`](Self::finish_prelude). `BEGIN` at a clean boundary cannot
    /// error and returns NO rows, so EVERY other frame — an `ErrorResponse`, a
    /// `RowDescription`/`DataRow` (a row-returning reply `BEGIN` never sends), or
    /// any other tag — is a protocol-violating server and a fatal teardown, never a
    /// silently-recovered state (the connection is killed rather than left with an
    /// undrained command reply). An `EmptyQueryResponse` is likewise illegal for a
    /// non-empty `BEGIN`.
    ///
    /// Deliberately NARROW: it is the BEGIN shape only. Draining a row-bearing
    /// prelude (a pool-checkout `RESET`, whose `SELECT pg_advisory_unlock_all()`
    /// returns a row) is a DEFERRED capability — if it is ever built, it re-adds a
    /// swallowed-row phase here WITH its own tests, rather than carrying an
    /// unreachable, untested path now.
    fn step_prelude(&mut self, tag: u8, start: usize, end: usize) -> ActiveOutcome {
        match tag {
            // `BEGIN`'s `CommandComplete` — swallow; its `ReadyForQuery` follows.
            T_COMMAND_COMPLETE => ActiveOutcome::Silent,
            T_READY_FOR_QUERY => self.finish_prelude(start, end),
            _ => self.prelude_teardown(),
        }
    }

    /// The prelude's trailing `ReadyForQuery`: record its transaction-status
    /// indicator (a `BEGIN` moves the session to `InTransaction`) and clear the
    /// drain so the seated [`ActiveState`] takes over on the next frame.
    ///
    /// Deliberately does NOT reset the per-statement columns: the seated command's
    /// result OIDs — set by its `begin_*` seat BEFORE the prelude drain — must
    /// survive into the command's own response (the extended Execute re-sends no
    /// `RowDescription`). The RFQ's own frame body is re-borrowed off the ingest
    /// cursor `take_frame` just advanced.
    fn finish_prelude(&mut self, start: usize, end: usize) -> ActiveOutcome {
        let body = self.ingest.frame_body(start, end);
        match body {
            [byte] => match TxStatus::try_from_byte(*byte) {
                Ok(tx) => {
                    self.tx_status = tx;
                    self.prelude_active = false;
                    ActiveOutcome::Idle
                }
                Err(_) => self.prelude_teardown(),
            },
            _ => self.prelude_teardown(),
        }
    }

    /// A fatal prelude teardown: clear the drain and mark the command state
    /// `Failed` so the connection is killed (the pump maps the surfaced `Close` to
    /// [`EngineError::ProtocolViolation`](super::EngineError::ProtocolViolation)).
    fn prelude_teardown(&mut self) -> ActiveOutcome {
        self.prelude_active = false;
        self.state = ActiveState::Failed;
        ActiveOutcome::Close
    }

    // ── oversize streaming ──

    /// Begin streaming an oversize frame whose footprint exceeds the buffer.
    /// Consumes the header, classifies Sub-A / Sub-B / teardown.
    ///
    /// `#[cold]` + `#[inline(never)]`: an oversize frame (a body larger than the
    /// inline ingest tier) is a rare event. Pulling this and
    /// [`step_oversize`](Self::step_oversize) out of line keeps the whole oversize
    /// subtree — begin/step and the prefix/accumulator helpers they reach — OFF
    /// [`next_event`](Self::next_event)'s hot frame, so the per-row DataRow arm
    /// does not carry the oversize machinery's stack setup it never executes.
    #[cold]
    #[inline(never)]
    fn begin_oversize(&mut self, declared: u32) -> ActiveOutcome {
        // `FrameTooLarge` is only produced with a full header buffered, so the
        // tag byte is present; `0` (a never-legal tag) is the dead fallback.
        let tag = self.ingest.peek_tag().unwrap_or(0);
        // Consume the 5-byte header (tag + 4-byte length field), discarded.
        let _consumed = self.ingest.take_chunk(HEADER_LEN);
        // Body length = declared (length-inclusive) minus the 4 length bytes.
        let body_len = usize_from_u32(declared).saturating_sub(HEADER_LEN.saturating_sub(1));

        // Recovering from a too-wide result: SWALLOW every oversize frame
        // regardless of tag (a wide `DataRow`, or a second statement's wide
        // `RowDescription` in a simple-query batch), consuming its body without
        // surfacing, until the drain reaches the trailing `ReadyForQuery` (which
        // is tiny and never oversize). Checked BEFORE the tag classification so it
        // takes precedence over Sub-A/Sub-C for a `DataRow`/`RowDescription` here.
        if matches!(self.state, ActiveState::DrainOvercapToRfq) {
            self.oversize = Some(OversizeStream {
                mode: OversizeMode::Skip,
                body_remaining: body_len,
                surfaced_tag: tag,
                prefix_len: 0,
            });
            return ActiveOutcome::Silent;
        }

        if tag == T_DATA_ROW
            && matches!(
                self.state,
                ActiveState::StreamingRows | ActiveState::BindAwaitingData
            )
        {
            self.oversize = Some(OversizeStream {
                mode: OversizeMode::SubA,
                body_remaining: body_len,
                surfaced_tag: tag,
                prefix_len: 0,
            });
            ActiveOutcome::Silent
        } else if tag == T_ROW_DESC {
            // A wide `RowDescription` exceeded the bounded buffer. It cannot be
            // truncated (Sub-B) — every column's type OID and name drives decode
            // — nor streamed as chunks (Sub-A) — it is consumed internally, not
            // surfaced. Gather the whole body into the growable accumulator, then
            // parse it once complete.
            //
            // Reject-BEFORE-allocate: a declared length beyond the legitimate
            // ceiling is a hostile/buggy server, classified as a teardown rather
            // than driven into an unbounded allocation. The cap is checked here,
            // the sole place Accumulate is entered, so it covers every active
            // phase that reaches a RowDescription.
            if body_len > MAX_ROW_DESC_ACCUM {
                core::hint::cold_path();
                return self.teardown();
            }
            self.oversize_accum.clear();
            self.oversize = Some(OversizeStream {
                mode: OversizeMode::Accumulate,
                body_remaining: body_len,
                surfaced_tag: tag,
                prefix_len: 0,
            });
            ActiveOutcome::Silent
        } else if is_streaming_eligible(tag) {
            if self.prefix.is_none() {
                self.prefix = Some(Box::new([0u8; OVERSIZE_PREFIX_CAP]));
            }
            self.oversize = Some(OversizeStream {
                mode: OversizeMode::SubB,
                body_remaining: body_len,
                surfaced_tag: tag,
                prefix_len: 0,
            });
            ActiveOutcome::Silent
        } else {
            // Any other oversize tag is a correct teardown.
            self.teardown()
        }
    }

    /// Advance an in-progress oversize stream by one step.
    ///
    /// `#[cold]` + `#[inline(never)]`: see [`begin_oversize`](Self::begin_oversize)
    /// — the oversize continuation is kept off the hot frame.
    #[cold]
    #[inline(never)]
    fn step_oversize(&mut self) -> ActiveOutcome {
        let mut os = match self.oversize {
            Some(os) => os,
            // Unreachable: the caller only enters here when `oversize.is_some()`.
            None => return ActiveOutcome::Silent,
        };
        match os.mode {
            OversizeMode::SubA => {
                if os.body_remaining == 0 {
                    self.oversize = None;
                    return ActiveOutcome::RowChunkEnd;
                }
                match self.ingest.take_chunk(os.body_remaining) {
                    None => ActiveOutcome::NeedMore,
                    Some((start, end)) => {
                        let took = end.saturating_sub(start);
                        os.body_remaining = os.body_remaining.saturating_sub(took);
                        self.oversize = Some(os);
                        ActiveOutcome::RowChunk(Lend::Ingest, start, end)
                    }
                }
            }
            OversizeMode::Skip => {
                if os.body_remaining == 0 {
                    self.oversize = None;
                    // Body fully discarded; resume the drain, which reads the next
                    // frame (another swallowed body, or the recovering RFQ).
                    return ActiveOutcome::Silent;
                }
                match self.ingest.take_chunk(os.body_remaining) {
                    None => ActiveOutcome::NeedMore,
                    Some((start, end)) => {
                        let took = end.saturating_sub(start);
                        os.body_remaining = os.body_remaining.saturating_sub(took);
                        self.oversize = Some(os);
                        // Consumed + discarded — surface nothing.
                        ActiveOutcome::Silent
                    }
                }
            }
            OversizeMode::Accumulate => {
                if os.body_remaining == 0 {
                    self.oversize = None;
                    return self.dispatch_accumulated_row_desc();
                }
                match self.ingest.take_chunk(os.body_remaining) {
                    None => ActiveOutcome::NeedMore,
                    Some((start, end)) => {
                        let took = end.saturating_sub(start);
                        self.append_oversize_accum(start, end);
                        os.body_remaining = os.body_remaining.saturating_sub(took);
                        self.oversize = Some(os);
                        // Keep gathering — the frame is dispatched only at body end.
                        ActiveOutcome::Silent
                    }
                }
            }
            OversizeMode::SubB => {
                if os.body_remaining == 0 {
                    self.oversize = None;
                    return self.deliver_oversize_prefix(os.surfaced_tag, os.prefix_len);
                }
                match self.ingest.take_chunk(os.body_remaining) {
                    None => ActiveOutcome::NeedMore,
                    Some((start, end)) => {
                        let took = end.saturating_sub(start);
                        self.copy_into_prefix(start, end, os.prefix_len);
                        let room = OVERSIZE_PREFIX_CAP.saturating_sub(os.prefix_len);
                        os.prefix_len = os.prefix_len.saturating_add(took.min(room));
                        os.body_remaining = os.body_remaining.saturating_sub(took);
                        self.oversize = Some(os);
                        // Keep absorbing — the event surfaces only at body end.
                        ActiveOutcome::Silent
                    }
                }
            }
        }
    }

    /// Surface a completed Sub-B oversize frame from its accumulated prefix.
    ///
    /// The phase-independent async frames (`Notice`, `Notify`, `ParameterStatus`)
    /// do not advance the command state machine — their truncated prefix IS the
    /// observable, legal in any phase. `CopyData` is a COPY-OUT data frame, so it
    /// is phase-gated to the `CopyOut` state exactly as the in-buffer
    /// `step_copy_out` gates it; out of phase it is a classified teardown, never a
    /// spurious surfaced event. The state-advancing control frames
    /// (`ErrorResponse`, `CommandComplete`) must still run their command-state
    /// transition when oversize, exactly as the in-buffer path does; otherwise
    /// the trailing `ReadyForQuery` lands in the wrong phase and tears down.
    fn deliver_oversize_prefix(&mut self, tag: u8, n: usize) -> ActiveOutcome {
        match tag {
            T_NOTICE => ActiveOutcome::Notice(Lend::Prefix, 0, n),
            T_NOTIFY => ActiveOutcome::Notify(Lend::Prefix, 0, n),
            T_PARAM_STATUS => ActiveOutcome::ParamStatus(Lend::Prefix, 0, n),
            // Phase-gated: a CopyData is legal only during COPY OUT, mirroring the
            // in-buffer `step_copy_out`. In phase, surface the truncated prefix.
            T_COPY_DATA if matches!(self.state, ActiveState::CopyOut) => {
                ActiveOutcome::CopyData(Lend::Prefix, 0, n)
            }
            // An oversize CopyData outside COPY OUT (reachable only from a
            // hostile / non-compliant server) is out of phase: teardown, never a
            // spurious out-of-phase CopyData event. The body was already
            // bounded-absorbed into the prefix, so this is bounded and crash-free.
            T_COPY_DATA => self.teardown(),
            T_ERROR => {
                // Mirror `fail_recoverable`: park the drain so the trailing RFQ
                // recovers the connection (a query-level error is recoverable).
                self.state = ActiveState::DrainAfterError;
                ActiveOutcome::Fail(Lend::Prefix, 0, n)
            }
            T_COMMAND_COMPLETE => self.complete_command_from_prefix(n),
            // `is_streaming_eligible` admits exactly the tags above; any other
            // tag reaching here is an internal inconsistency, classified as a
            // teardown rather than a silent or stale delivery.
            _ => self.teardown(),
        }
    }

    /// Post-`CommandComplete` state for the current command phase, or `None`
    /// when a `CommandComplete` is wire-illegal in the current state (then it is
    /// a classified teardown). Mirrors the per-state `'C'` transitions so an
    /// oversize `CommandComplete` is dispatched exactly as an in-buffer one.
    fn command_complete_next_state(&self) -> Option<ActiveState> {
        match self.state {
            ActiveState::Idle
            | ActiveState::StreamingRows
            | ActiveState::AwaitingRfq
            | ActiveState::CopyOutAwaitingCc
            | ActiveState::CopyInActive => Some(ActiveState::AwaitingRfq),
            ActiveState::BindAwaitingData => Some(ActiveState::ExtendedAwaitingRfq),
            ActiveState::CopyOut
            | ActiveState::DrainAfterError
            // In the overcap drain an in-buffer `CommandComplete` is swallowed by
            // `step_drain_overcap` and an oversize one by `Skip` mode — neither
            // reaches this Sub-B completion path — so a `CommandComplete` arriving
            // here in that state is as wire-illegal as in `DrainAfterError`.
            | ActiveState::DrainOvercapToRfq
            | ActiveState::ParseAwaitingParseComplete
            | ActiveState::ParseDescribeStmtAwaitingParseComplete
            | ActiveState::ParseBindExecuteAwaitingParseComplete
            | ActiveState::CloseParseBindExecuteAwaitingCloseComplete
            | ActiveState::DescribeStmtAwaitingParamDesc
            | ActiveState::DescribeAwaitingRowDescOrNoData
            | ActiveState::BindAwaitingBindComplete
            | ActiveState::ExtendedAwaitingRfq
            | ActiveState::CloseAwaitingComplete
            // A `CommandComplete` is never legal in the fused setup chain — it
            // arrives only after the describe answer, in `BindAwaitingData`.
            | ActiveState::FusedAwaitingParseComplete
            | ActiveState::FusedAwaitingBindComplete
            | ActiveState::FusedAwaitingRowDescOrNoData
            | ActiveState::Failed => None,
        }
    }

    /// Complete an oversize `CommandComplete` from its accumulated Sub-B prefix:
    /// validate the phase, parse the tag from the prefix bytes, store it, and
    /// run the command-boundary transition — or a classified teardown when the
    /// tag is wire-illegal for the phase, unparseable, or truncated below its
    /// terminator (the body exceeded the bounded prefix, so the closing `\0` is
    /// absent and parsing fails). The tag comes from THIS frame's prefix, never
    /// a prior command's value.
    fn complete_command_from_prefix(&mut self, prefix_len: usize) -> ActiveOutcome {
        let Some(next) = self.command_complete_next_state() else {
            return self.teardown();
        };
        let parsed = match self.prefix.as_deref().and_then(|p| p.get(..prefix_len)) {
            Some(body) => parse_command_tag_bytes(body),
            None => return self.teardown(),
        };
        match parsed {
            Ok(tag) => {
                self.command_tag = Some(tag);
                self.state = next;
                ActiveOutcome::Deliver
            }
            Err(_) => self.teardown(),
        }
    }

    /// Copy `ingest[start..end]` into the Sub-B prefix at `dst_off`, clamped to
    /// the bounded prefix capacity (the tail beyond the prefix is dropped —
    /// stream-and-truncate). Disjoint field borrows: the ingest read and the
    /// prefix write touch different fields of `self`.
    fn copy_into_prefix(&mut self, start: usize, end: usize, dst_off: usize) {
        let Some(prefix) = self.prefix.as_deref_mut() else {
            return;
        };
        let src = self.ingest.frame_body(start, end);
        let room = OVERSIZE_PREFIX_CAP.saturating_sub(dst_off);
        let n = src.len().min(room);
        if let (Some(dst), Some(src)) = (
            prefix.get_mut(dst_off..dst_off.saturating_add(n)),
            src.get(..n),
        ) {
            dst.copy_from_slice(src);
        }
    }
}

/// Parse a `RowDescription` body into owned column type OIDs + names, or the
/// classified [`ProtocolError`](crate::error::ProtocolError) the wire decode
/// rejected it with. Owned (not borrowed), so the caller can mutate `self` after
/// the parse — and so the same parse serves both the in-buffer body
/// (`IngestBuf::frame_body`) and the Sub-C accumulator (a `Vec`).
///
/// The classification is THREADED, not flattened to `Option`: a well-formed but
/// too-wide frame is `Err(ProtocolError::TooManyColumns { .. })` — a RECOVERABLE
/// class the caller drains from, distinct from a malformed frame (a framing
/// desync that tears the connection down). Collapsing both to `None` would have
/// forced every caller to teardown, which is exactly the bug this preserves the
/// distinction to fix.
#[inline]
fn parse_row_desc_owned(
    body: &[u8],
) -> Result<(Vec<u32>, Vec<String>), crate::error::ProtocolError> {
    let rd = parse_row_description(body)?;
    let oids = rd.columns_iter().map(|c| c.type_oid).collect::<Vec<u32>>();
    let names = parse_column_names(body)?;
    Ok((oids, names))
}

/// Is `tag` a payload-bearing non-`DataRow` frame whose oversize is absorbed
/// via the Sub-B prefix-and-truncate path? Control frames (whose oversize is a
/// protocol impossibility) are excluded — those tear the connection down.
#[inline]
#[must_use]
fn is_streaming_eligible(tag: u8) -> bool {
    matches!(
        tag,
        T_NOTICE | T_ERROR | T_NOTIFY | T_COPY_DATA | T_COMMAND_COMPLETE | T_PARAM_STATUS
    )
}

impl core::fmt::Debug for ActiveEngine {
    /// Redacts the secret key and never prints buffer / prefix / row contents.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ActiveEngine")
            .field("backend_pid", &self.backend_pid)
            .field("tx_status", &self.tx_status)
            .field("state", &self.state)
            .field("n_columns", &self.col_oids.len())
            .finish_non_exhaustive()
    }
}

impl Drop for ActiveEngine {
    /// Scrub the Sub-B prefix on teardown — it holds raw inbound wire bytes
    /// (notice/error text, parameter values). The ingest buffer and the
    /// `Sensitive` secret key scrub via their own `Drop`.
    fn drop(&mut self) {
        use zeroize::Zeroize;
        if let Some(prefix) = &mut self.prefix {
            prefix.as_mut_slice().zeroize();
        }
        // The Sub-C accumulator holds raw inbound RowDescription bytes (column
        // names); scrub them like the Sub-B prefix.
        self.oversize_accum.zeroize();
    }
}
