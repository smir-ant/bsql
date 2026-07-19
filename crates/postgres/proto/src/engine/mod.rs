//! Strangler-engine scaffold — the session engine and its five seams.
//!
//! This module grows a new session engine *alongside* the existing wire
//! state machine without disturbing it: it is purely additive, adds no
//! dependency, and bakes in `#![no_std]` from the start (the parent
//! crate's forbid-bundle — no `unsafe`, no panic/index/arith/cast — and
//! `extern crate alloc` apply unchanged). Nothing here is wired into the
//! live `dispatch`/`protocol` path; that composition lands as later
//! additive steps.
//!
//! The four load-bearing seams:
//!
//! 1. [`Never`] + [`absurd`] — uninhabited carrier for phase-impossible
//!    frames (no wildcard `_` arms).
//! 2. [`Transport`] — the driver-facing I/O seam (RPITIT + `Send`, with a
//!    `Send`-bounded associated [`Error`](Transport::Error) type).
//! 3. [`Live`] — the branded, non-`Clone`, linear liveness token, minted
//!    by [`session`].
//! 4. [`Engine`] — the session shell that composes the above.
//!
//! The [`Event`] / [`AuthEvent`] pull-event vocabulary is *declared* here
//! (and footprint-pinned); the producers that emit those events compose in
//! later additive steps.
//!
//! # §7 edge-case discipline — the twelve-axis pass
//!
//! Every non-trivial engine component is walked across the project canon's
//! twelve edge-case axes, and each axis (and each material sub-point) is
//! resolved EITHER by a concrete green gate/test OR by an explicit, justified
//! "not applicable" — silence on an applicable axis is itself a violation. The
//! twelve axes are: **Cardinality**, **Presence**, **Concurrency**,
//! **Temporal**, **Trust level**, **Size**, **State lifecycle**, **Resource
//! pressure**, **Platform**, **Failure composition**, **Memory-leak /
//! ownership**, and **Fallback / recovery**.
//!
//! The pass is not free prose that can rot: the authoritative per-sub-point
//! citation table, and the tests that mechanically prove every cited test/gate
//! actually exists (so the table can never claim coverage that does not),
//! live in the `engine_axes_spec` integration test. That spec also re-derives
//! the engine's load-bearing census counts (footprint pins, error variants,
//! verbs, cold-classified branches, `#[non_exhaustive]` surfaces) from the
//! source so a drift in any of them fails the build.
//!
//! # Footprint pins
//!
//! Every engine surface type carries a co-located [`wire_pin!`](crate::wire_pin)
//! anchor (`size_of` + `align_of` together) at its FINAL size, so a layout
//! drift is an `E0080` build failure — even for a type constructed nowhere.
//! Types generic over a parameter with no canonical size are pinned at their
//! real instantiations ([`Boundary`] at `Never` and `()`, [`EngineError`] at
//! the witness `Infallible`); the [`Engine`] shell is generic over the
//! transport, so it has no single canonical size and is not footprint-pinned —
//! its field layout is exercised by the `Send`-composition gates below.

mod dispatch_active;
mod dispatch_connecting;
mod error;
mod flush;
// Crate-internal (never re-exported publicly): the active frame builders, exposed
// `pub(crate)` only so the in-crate byte-twin test can pin them byte-identical to
// the proven `protocol.rs` encoders. Not part of the public engine surface.
pub(crate) mod frames;
mod ingest;
mod pump;
mod seams;
mod verbs;

pub use dispatch_active::ActiveEngine;
pub use dispatch_connecting::{ConnFail, ConnectingEngine};
// Crate-internal: the non-borrowing handshake-step pull the connecting pump
// consumes (not part of the public surface).
pub(crate) use dispatch_connecting::HandshakeProgress;
pub use error::{EngineError, ExpectedRowCount, RowCountViolation, WrongPhase};
pub use flush::{flush, SendBuf, SendOverrun};
pub use ingest::{IngestBuf, IngestCommitOverflow, IngestFull};
pub use pump::{
    poll_once, pump_active_to_boundary, pump_connecting_to_ready, Boundary, HandshakeOutcome,
    SpuriousPending, Surface,
};
pub use seams::{absurd, CommandStatus, Live, Never, NotifyStatus, Outcome, Transport};
pub use verbs::PreparedStatement;

use core::marker::PhantomData;

use crate::action::TxStatus;
use crate::ident::{DatabaseName, Ident};
use crate::startup::StartupParam;
use crate::password::Credentials;

// ===========================================================================
// Pull-event vocabulary (declared; producers compose later)
// ===========================================================================

/// Connecting-phase pull-event surface — closed over startup/auth frames
/// only.
///
/// There is deliberately no `Row`/`Deliver`/`Notify` variant: those frames
/// are unrepresentable during the connecting phase by construction, not by
/// a runtime guard. Each payload-bearing variant carries exactly one
/// borrow of the read buffer, so the whole enum is one fat slice plus a
/// tag (footprint-pinned at 24 bytes).
#[derive(Clone, Copy, Debug)]
pub enum AuthEvent<'e> {
    /// The framing buffer is drained — the caller must read more bytes.
    NeedMore,
    /// Server requested cleartext-password authentication.
    AuthCleartext,
    /// Server requested MD5 authentication, lending the 4-byte salt.
    AuthMd5 {
        /// The server-chosen salt for the MD5 digest.
        salt: [u8; 4],
    },
    /// SASL continuation, lending the server's challenge bytes.
    AuthSaslContinue(&'e [u8]),
    /// A `ParameterStatus` report, lending its raw key/value payload.
    ParamStatus(&'e [u8]),
    /// Handshake complete — the connection is ready for queries.
    Ready,
    /// The server reported an error, lending its raw `ErrorResponse` body.
    Fail(&'e [u8]),
}

/// Active-phase pull-event surface — closed over every wire-legal active
/// frame, with no frequency-based exclusions.
///
/// Each payload-bearing variant carries exactly one borrow of the read
/// buffer (`Row` lends the whole `DataRow` payload), so the enum is one fat
/// slice plus a tag (footprint-pinned at 24 bytes).
#[derive(Clone, Copy, Debug)]
pub enum Event<'e> {
    /// The framing buffer is drained — the caller must read more bytes.
    NeedMore,
    /// Clean `ReadyForQuery` — the command boundary at which a verb
    /// returns the liveness token.
    Idle,
    /// A command completed; the tag is surfaced via the observer seam.
    Deliver,
    /// The server reported an error, lending its raw `ErrorResponse` body.
    Fail(&'e [u8]),
    /// The server closed the connection.
    Close,
    /// A `NoticeResponse`, lending its raw payload.
    Notice(&'e [u8]),
    /// A `NotificationResponse` (`LISTEN`/`NOTIFY`), lending its payload.
    Notify(&'e [u8]),
    /// A `ParameterStatus` report, lending its raw key/value payload.
    ParamStatus(&'e [u8]),
    /// A row-limited `Execute` paused at its cap: the server sent
    /// `PortalSuspended` instead of `CommandComplete`. The rows delivered
    /// before this are the prefix fetched so far; the portal stays open on
    /// the server (resumable with a bare `Execute`) and there is no command
    /// tag. A typed terminal distinct from [`Deliver`](Self::Deliver) — the
    /// pull analog of the live engine's `Reply::QuerySuspended` discriminator,
    /// not a side-channel flag.
    Suspended,
    /// One `DataRow`, lending the whole row payload as a single borrow.
    Row(&'e [u8]),
    /// One chunk of a row that exceeded the inline buffer, lending the
    /// chunk bytes.
    RowChunk(&'e [u8]),
    /// The final chunk of an oversized row has been delivered.
    RowChunkEnd,
    /// A `COPY` data frame, lending its payload bytes.
    CopyData(&'e [u8]),
    /// The `COPY` stream is complete.
    CopyDone,
    /// A well-formed but too-wide `RowDescription` — its column count exceeds
    /// [`MAX_ROW_COLUMNS`](crate::MAX_ROW_COLUMNS) — classified as a RECOVERABLE
    /// `TooManyColumns`. Carries the offending `count` and the supported `max` so
    /// the driver names the exact limit; the engine has parked a drain that
    /// swallows the in-flight result to the trailing `ReadyForQuery`, so the
    /// connection recovers to idle (distinct from [`Close`](Self::Close), which
    /// tears it down). Payload-free of any buffer borrow — the two counts are
    /// owned, so it does not widen the event past its fat-slice variants.
    Overcap {
        /// Column count the server's `RowDescription` declared.
        count: usize,
        /// Maximum supported — [`MAX_ROW_COLUMNS`](crate::MAX_ROW_COLUMNS).
        max: usize,
    },
}

crate::wire_pin!(Event<'static>, size = 24, align = 8);
crate::wire_pin!(AuthEvent<'static>, size = 24, align = 8);

// ===========================================================================
// 5. Engine shell + the session-scope minting functions
// ===========================================================================

/// The session engine: a transport plus the brand that ties it to its
/// session-scoped liveness token.
///
/// The brand `'b` lives on the engine *type* (so a foreign token cannot
/// drive it) but the verbs borrow `&mut self`, never `&'b mut self` — the
/// engine borrow is released at each `await`, so a single async scope can
/// thread the linear token through any number of sequential verbs. Coupling
/// the engine *borrow* to the brand would over-constrain the engine to a
/// single borrow for its whole lifetime and make sequential verbs in one
/// `async` scope uncompilable.
#[derive(Debug)]
pub struct Engine<'b, T> {
    transport: T,
    /// The single persistent outbound residence: the startup packet, every auth
    /// response, and (later) request frames are queued here and drained by the
    /// flush loop.
    send_buf: SendBuf,
    /// The engine's current protocol phase. Born [`Phase::Connecting`];
    /// [`connect`](Self::connect) drives it to [`Phase::Active`].
    phase: Phase,
    _brand: PhantomData<fn(&'b ()) -> &'b ()>,
}

/// The engine's protocol phase: the connecting handshake brain, the active
/// post-handshake handle, the move-out placeholder occupied only during the
/// synchronous Connecting→Active swap, or the terminal closed state a graceful
/// [`terminate`](Engine::terminate) leaves behind.
///
/// The two live phases overlay one another (a single [`IngestBuf`] is carried
/// across the transition by [`ConnectingEngine::into_active`], never doubled),
/// so the engine is sized by its larger phase, not their sum. [`Closed`](Phase::Closed)
/// is a unit variant — it adds no bytes (the discriminant already spans the live
/// phases) — and routes through the same cold wrong-phase arm as the non-active
/// phases, so a verb or accessor after a graceful close is a classified
/// [`WrongPhase`], never a silent no-op.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "Active(ActiveEngine) is DELIBERATELY inline — it is the connection's \
              hot per-verb state (144 B IngestBuf + the schema/param/cache handles). \
              Boxing it, as the lint suggests, would put the whole hot state behind a \
              pointer and add an indirection to EVERY verb — a real hot-path regression \
              to silence a cosmetic size-difference threshold. The two live phases \
              overlay one connection, so the enum is sized by its larger phase by \
              design; the Active/Connecting size gap is intrinsic, not accidental."
)]
enum Phase {
    /// Driving the startup/auth handshake.
    Connecting(ConnectingEngine),
    /// Handshake complete; ready for active-phase verbs.
    Active(ActiveEngine),
    /// The [`core::mem::replace`] placeholder held only between the move-out of
    /// the connecting engine and the move-in of the active engine, inside one
    /// synchronous tail. Never held across an `.await` and never observed by a
    /// caller (the swap completes before the next suspension point).
    Transitioning,
    /// The terminal state a graceful [`terminate`](Engine::terminate) leaves: the
    /// `Terminate` frame has been pushed and the transport shut down, so the
    /// connection is dead. Every verb consumed its linear token, so a verb after
    /// `terminate` is already a move error; this variant additionally classifies
    /// any phase accessor (`backend_pid` / `tx_status`) as [`WrongPhase`] rather
    /// than reviving a closed connection. Entered only by `terminate`; never left.
    Closed,
}

impl Phase {
    /// Borrow the active-phase handle, or classify a wrong-phase access.
    #[inline]
    fn as_active(&self) -> Result<&ActiveEngine, WrongPhase> {
        match self {
            Phase::Active(active) => Ok(active),
            Phase::Connecting(_) | Phase::Transitioning | Phase::Closed => {
                core::hint::cold_path();
                Err(WrongPhase)
            }
        }
    }

    /// Mutably borrow the active-phase handle, or classify a wrong-phase access.
    ///
    /// The verbs call this on the *destructured* `phase` field (never through a
    /// `&mut self` helper), so the returned `&mut ActiveEngine` is a borrow
    /// disjoint from the engine's `transport`/`send_buf` fields — the
    /// simultaneous disjoint borrows the active pump needs. A `self.active_mut()`
    /// helper would borrow the whole engine and alias them (E0499).
    #[inline]
    fn as_active_mut(&mut self) -> Result<&mut ActiveEngine, WrongPhase> {
        match self {
            Phase::Active(active) => Ok(active),
            Phase::Connecting(_) | Phase::Transitioning | Phase::Closed => {
                core::hint::cold_path();
                Err(WrongPhase)
            }
        }
    }
}

crate::wire_pin!(Live<'static>, size = 0, align = 1);

impl<'b, T> Engine<'b, T> {
    /// Construct the engine shell from an already-prepared transport, primed
    /// outbound buffer, and initial phase, branded to the caller's session
    /// scope `'b`.
    #[inline(always)]
    fn new_in_scope(transport: T, send_buf: SendBuf, phase: Phase) -> Self {
        Self {
            transport,
            send_buf,
            phase,
            _brand: PhantomData,
        }
    }

    /// The backend process id reported by `BackendKeyData` at handshake
    /// completion — the non-secret half of the cancel key.
    ///
    /// Returns [`WrongPhase`] before [`connect`](Self::connect) has driven the
    /// engine into its active phase: the value does not exist until the
    /// handshake completes, so the absence is a classified error, not a sentinel.
    #[inline]
    pub fn backend_pid(&self) -> Result<i32, WrongPhase> {
        Ok(self.phase.as_active()?.backend_pid())
    }

    /// Closure-scope access to the backend cancel-key authenticator (the SECRET
    /// half of the cancel key, captured from `BackendKeyData` at handshake
    /// completion).
    ///
    /// The secret is handed to `f` as an `i32` and never escapes the call — the
    /// HRTB `FnOnce(i32) -> R` closure mirrors [`Sensitive::with_inner`], so a
    /// caller can copy it out (to re-wrap it in its own [`Sensitive`]) but cannot
    /// retain a borrow into the engine's zeroize-on-drop store. The sole intended
    /// consumer is a driver capturing the cancel key for an out-of-band
    /// `CancelRequest` (see [`cancel_request_bytes`](crate::cancel_request_bytes)).
    ///
    /// Returns [`WrongPhase`] before [`connect`](Self::connect) has driven the
    /// engine active — the key does not exist until `BackendKeyData` arrives, so
    /// the absence is a classified error, not a sentinel.
    ///
    /// [`Sensitive`]: crate::Sensitive
    /// [`Sensitive::with_inner`]: crate::Sensitive::with_inner
    #[inline]
    pub fn with_secret_key<R>(&self, f: impl FnOnce(i32) -> R) -> Result<R, WrongPhase> {
        Ok(self.phase.as_active()?.with_secret_key(f))
    }

    /// The `server_version` GUC captured from the startup `ParameterStatus`
    /// reports during the handshake — the version string a `SHOW server_version`
    /// would return, recovered for free without the round-trip.
    ///
    /// Returns [`WrongPhase`] before [`connect`](Self::connect) has driven the
    /// engine active. Once active, `Ok(Some(_))` is the captured version and
    /// `Ok(None)` means the server sent no `server_version` report (honest
    /// absence — never a fabricated value).
    #[inline]
    pub fn server_version(&self) -> Result<Option<&str>, WrongPhase> {
        Ok(self.phase.as_active()?.server_version())
    }

    /// The current `ReadyForQuery` transaction-status indicator.
    ///
    /// Returns [`WrongPhase`] before the engine is active (see
    /// [`backend_pid`](Self::backend_pid)).
    #[inline]
    pub fn tx_status(&self) -> Result<TxStatus, WrongPhase> {
        Ok(self.phase.as_active()?.tx_status())
    }

    /// The parameter-type OIDs captured from the most recent statement
    /// `Describe`'s `ParameterDescription` (the `prepare` path), in `$1..$n`
    /// order — the types the server INFERRED (or the client DECLARED) for the
    /// prepared statement's placeholders. Read at a `prepare`'s settle so the
    /// driver retains them on the `PreparedStatement` and VERIFIES a later
    /// `Bind`'s encoded parameter types against them (a fixed-plan statement
    /// cannot coerce a differently-typed binary bind, so a wrong-typed `Bind`
    /// would otherwise be silently reinterpreted). Empty when no statement
    /// describe has run on this connection.
    ///
    /// Returns [`WrongPhase`] before the engine is active (see
    /// [`backend_pid`](Self::backend_pid)).
    #[inline]
    pub fn current_param_oids(&self) -> Result<&[u32], WrongPhase> {
        Ok(self.phase.as_active()?.current_param_oids())
    }

    /// TAKE the typed result-schema OID mismatch recorded during a compile-checked
    /// cache-MISS's `RowDescription` check (`Some((index, found, expected))`), or
    /// `None`. The driver reads this after a typed verb's pump returns — the over-cap
    /// drain the guard reused has already reclaimed the connection to a clean idle —
    /// and surfaces a classified
    /// [`DecodeError::ColumnOidMismatch`](crate::decode::DecodeError::ColumnOidMismatch).
    /// The `expected` OID rides the triple (the value the engine SEATED from the
    /// carrier's `row_oids` and checked against), so both the single-query settle and
    /// the batch-generic pipeline settle read the same checked pair from one source.
    ///
    /// Returns `None` when the engine is not active (there is no mismatch to report
    /// off the active path), so a caller never has to special-case the phase.
    #[inline]
    pub fn take_result_oid_mismatch(&mut self) -> Option<(u16, u32, u32)> {
        self.phase
            .as_active_mut()
            .ok()
            .and_then(|active| active.take_result_oid_mismatch())
    }

    /// Forget the per-connection prepared-statement cache (a no-op unless the
    /// engine is active).
    ///
    /// The cache records which content-addressed statements the compile-checked
    /// `query!` verbs have Parsed and that are durable on this physical connection,
    /// so a repeat reuses the server-side plan with a bare `Bind`+`Execute`. Drive
    /// this on every session reset: the drivers' pool resets on checkout, and
    /// dropping BOTH statement caches (this typed one AND the dynamic one) is a
    /// CORRECTNESS requirement — a prior user's promoted plan, whose relation names
    /// were resolved at its `Parse`, must NOT be reused by the next logical user
    /// (whose `CREATE TEMP TABLE` shadow of the same name would otherwise read the
    /// prior user's rows through the kept plan — a silent cross-user wrong result).
    /// The next typed query is then a fresh `Parse` (a MISS) resolving against the
    /// current user's schema. Use [`take_statement_cache`](Self::take_statement_cache)
    /// instead when the caller also wants the names, to Close their server-side
    /// statements in the same batch.
    #[inline]
    pub fn clear_statement_cache(&mut self) {
        if let Phase::Active(active) = &mut self.phase {
            active.clear_statement_cache();
        }
    }

    /// DRAIN the per-connection typed prepared-statement cache, CLEARING it and
    /// returning the recorded names (an empty `Vec`, allocation-free, unless the
    /// engine is active). The reset drives this WHEN the dynamic drain already pays
    /// for a `Close` round trip: the clear forces the next typed query to re-`Parse`
    /// fresh (the [`clear_statement_cache`](Self::clear_statement_cache) correctness
    /// guarantee), and the returned names let the caller FOLD their server-side
    /// `Close`s into that already-paid batch (see
    /// [`close_statements_bytes`](Self::close_statements_bytes)), so the drop of
    /// both caches costs ZERO extra round trips.
    #[inline]
    pub fn take_statement_cache(&mut self) -> alloc::vec::Vec<&'static str> {
        match &mut self.phase {
            Phase::Active(active) => active.take_statement_cache(),
            _ => alloc::vec::Vec::new(),
        }
    }

    /// Record a pipelined command's content-addressed statement as durable on this
    /// connection — the batch analog of the serial
    /// [`query_params`](Self::query_params) cache settle. De-duplicates against the
    /// current set (a HIT command's name is already present; two identical queries
    /// in one batch record once), so it is safe to call for EVERY referenced name.
    /// A no-op unless the engine is active.
    ///
    /// The driver calls this ONLY on a batch that completed at a clean idle with
    /// `TxStatus::Idle` (the implicit transaction committed), so a recorded name can
    /// never point at a statement a rollback removed.
    #[inline]
    pub fn record_pipeline_statement(&mut self, stmt_name: &'static str) {
        if let Phase::Active(active) = &mut self.phase
            && !active.is_statement_parsed(stmt_name)
        {
            active.record_statement_parsed(stmt_name);
        }
    }

    /// Evict a pipelined command's statement name after a FAILED batch — the
    /// self-heal for a plan dropped out of band (`DISCARD ALL` / `DEALLOCATE`): the
    /// next use of the name is a cache MISS that Close-before-Parse re-creates. A
    /// no-op for a name that was not cached (a MISS this batch), and a no-op unless
    /// the engine is active.
    #[inline]
    pub fn evict_pipeline_statement(&mut self, stmt_name: &str) {
        if let Phase::Active(active) = &mut self.phase {
            active.evict_statement(stmt_name);
        }
    }

    /// Release the outbound send buffer's backing allocation if it grew past the
    /// high-water mark — the bounded-pool-memory reclaim.
    ///
    /// A large `Bind` parameter block (a multi-megabyte `bytea` / `jsonb` /
    /// `text` parameter) streams UNCAPPED onto the engine's send buffer, and that
    /// grown capacity is otherwise retained for the connection's whole life — a
    /// steady-state memory bloat for a long-lived pooled connection. This drops it
    /// back to a small baseline, scrubbing the freed bytes first (a mid-life
    /// shrink frees the oversized block, which held prior outbound wire bytes).
    ///
    /// It is a pure memory-hygiene op INDEPENDENT of protocol phase (it touches
    /// only the outbound byte buffer, never the phase), so it takes `&mut self`
    /// and is infallible — no `WrongPhase`. The sole caller is a driver's
    /// `reset_session`, which runs it at a COLD pool-checkout settle point AFTER
    /// its reset round trip has drained the buffer. It is NEVER on the hot
    /// per-query / per-row path, and a normal small-query buffer (which never
    /// crosses the high-water mark) is left untouched — no thrash.
    #[inline]
    pub fn reclaim_send_buffer(&mut self) {
        self.send_buf.reclaim_if_oversized();
    }

    /// Re-mint the linear [`Live`] token AFTER a prior token was LOST to a dropped
    /// future — the one recovery seam a driver needs to un-brick a connection whose
    /// in-flight verb future was cancelled (`tokio::time::timeout` / `select!`).
    ///
    /// # The hole this closes
    ///
    /// Every active-phase verb MOVES the linear [`Live`] out of the driver's
    /// `Option<Live>` slot into its future and returns it only on a clean protocol
    /// boundary. If that future is DROPPED mid-command (the caller lost a `timeout`
    /// race), the token is dropped WITH the future — it is a ZST with no `Drop`, so
    /// nothing runs, but the token is simply GONE. The driver's slot stays `None`
    /// and, with no way to re-mint, the connection is permanently unusable even
    /// though the socket is fine. This seam re-mints one.
    ///
    /// # Soundness contract (CALLER-ENFORCED — the reason this is not [`session`])
    ///
    /// Minting a *second* live token for one engine would break the tier-1
    /// at-most-one-command-in-flight invariant (two tokens could drive two
    /// concurrent commands and tear the protocol). So the caller MUST guarantee, at
    /// the call site, that **no other `Live` for this engine currently exists** —
    /// i.e. the prior token was DROPPED, not returned. A driver proves this
    /// structurally: it calls this ONLY when its own `Option<Live>` slot is `None`
    /// AND its own dropped-future dirty marker is set, a state reachable only after
    /// a future-drop consumed the previous token. This is the exact
    /// tier-2-by-encapsulation posture [`open_owned`] already documents for the
    /// `'static` brand: the driver keeps the token private and never mints a second
    /// one while one is live.
    ///
    /// Re-minting is sound because a dropped future ABANDONS *driving* the engine
    /// but never *corrupts* its state: [`Transport::write`]'s cancellation
    /// atomicity means a drop mid-write tears no send cursor, and the ingest buffer
    /// holds only whole, well-framed bytes. The engine's [`Phase`] and dispatch
    /// state are exactly where the abandoned pump left them, so the re-minted token
    /// can drive the owed reply frames to a clean idle (a driver's `drain`) and
    /// resume, or — for a wait that owed nothing (`recv_notification`) — resume
    /// directly.
    ///
    /// `#[doc(hidden)]`: a driver-facing recovery seam, not a consumer API.
    #[doc(hidden)]
    #[inline]
    #[must_use = "the re-minted Live token is the connection's only handle; dropping it re-strands the connection"]
    pub fn reclaim_live_after_drop(&mut self) -> Live<'b> {
        Live::new_in_scope()
    }

    /// Arm a fused simple-query PRELUDE to prepend to the NEXT command's flush.
    /// Today the ONE armed prelude is a deferred transaction `BEGIN`, fused with
    /// the transaction's first statement so it costs no standalone round trip.
    ///
    /// The first request verb that runs enqueues the prelude's `'Q'` frame ahead of
    /// its own, so the single following flush carries BOTH — the prelude's
    /// standalone round trip is removed — and the pump drains the prelude's response
    /// (swallowed) before the command's. A no-op unless the engine is active (a
    /// prelude only makes sense post-handshake).
    ///
    /// The SQL parameter is a general `'static &str`, but the DRAIN
    /// ([`ActiveEngine::set_pending_prelude`](crate::engine::ActiveEngine::set_pending_prelude))
    /// currently understands only the BEGIN reply SHAPE — a non-row-bearing
    /// `CommandComplete` + `ReadyForQuery`. A ROW-bearing prelude (e.g. a
    /// pool-checkout session RESET whose `SELECT pg_advisory_unlock_all()` returns
    /// a row) is a DEFERRED capability: arming one today would hit the drain's
    /// fatal-teardown arm and kill the connection. Building it means adding a
    /// swallowed-row drain phase WITH its own tests, not widening this contract.
    #[inline]
    pub fn defer_command_prelude(&mut self, sql: &'static str) {
        if let Phase::Active(active) = &mut self.phase {
            active.set_pending_prelude(sql);
        }
    }
}

impl<'b, T: Transport> Engine<'b, T> {
    /// Drive the startup/auth handshake to completion, transitioning the engine
    /// from its connecting phase to active.
    ///
    /// The first verb a session body calls. It flushes the startup packet
    /// (primed onto the engine's send buffer at construction), drives the
    /// connecting pump over the transport until the server reports a clean
    /// `ReadyForQuery`, then — in one synchronous tail, never across an
    /// `.await` — swaps the connecting engine out for the active one. The linear
    /// liveness token is consumed and returned only on success; on any failure
    /// it is dropped (the connection is dead).
    ///
    /// The engine fields are split-borrowed disjointly (`transport` / `send_buf`
    /// / `phase`) so the pump can drive the in-place connecting engine over the
    /// transport while the phase still owns it; the `Transitioning` placeholder
    /// bridges the move-out/move-in pair in the synchronous tail.
    ///
    /// # Errors
    ///
    /// - [`EngineError::Handshake`] — the handshake failed on a client-side
    ///   classification (unsupported auth method, SCRAM/MD5 failure, wire-illegal
    ///   frame), carrying the classified cause.
    /// - [`EngineError::HandshakeServerError`] — the server answered the handshake
    ///   with an `ErrorResponse`, carrying its raw body up so the driver decodes
    ///   the full SQLSTATE + message.
    /// - [`EngineError::WrongPhase`] — `connect` was called when the engine was
    ///   not in its connecting phase (e.g. already active).
    /// - [`EngineError::Transport`] / [`EngineError::UnexpectedEof`] /
    ///   [`EngineError::WriteZero`] / [`EngineError::SendOverrun`] /
    ///   [`EngineError::IngestFull`] / [`EngineError::IngestCommitOverflow`] —
    ///   from the connecting pump (see
    ///   [`pump_connecting_to_ready`](crate::engine::pump_connecting_to_ready)).
    pub async fn connect(&mut self, live: Live<'b>) -> Result<Live<'b>, EngineError<T::Error>> {
        // Disjoint split-borrow: the pump drives the in-place connecting engine
        // over the transport + send buffer; routing through a `&mut self` phase
        // helper would alias these borrows (E0499).
        let Self {
            transport,
            send_buf,
            phase,
            ..
        } = self;

        let outcome = {
            let conn = match phase {
                Phase::Connecting(conn) => conn,
                Phase::Active(_) | Phase::Transitioning | Phase::Closed => {
                    core::hint::cold_path();
                    return Err(EngineError::WrongPhase(WrongPhase));
                }
            };
            pump_connecting_to_ready(conn, transport, send_buf).await?
        };

        match outcome {
            HandshakeOutcome::Failed(cause) => {
                core::hint::cold_path();
                Err(EngineError::Handshake(cause))
            }
            HandshakeOutcome::ServerError(body) => {
                // A server `ErrorResponse` during connect: carry the raw body up so
                // the driver decodes its full SQLSTATE + message with the active
                // path's `parse_error_response` — never an opaque string.
                core::hint::cold_path();
                Err(EngineError::HandshakeServerError(body))
            }
            HandshakeOutcome::Ready => {
                // Erase the handshake's secret-bearing outbound wire (the SCRAM
                // client proof / password message) from the send buffer NOW that
                // the handshake is complete — restoring the prompt scrub the
                // single-residence rework would otherwise defer to connection
                // close. The buffer is provably drained here: the pump flushes
                // every outbound frame before it reports `Ready`. This also drops
                // the accumulated handshake wire so it is not carried into the
                // active phase, while retaining the allocation for reuse.
                send_buf.scrub_drained();
                // Synchronous tail: move the connecting engine out behind the
                // `Transitioning` placeholder, convert it, move the active engine
                // in — no `.await` between, so `Transitioning` is never observed.
                match core::mem::replace(phase, Phase::Transitioning) {
                    Phase::Connecting(conn) => match conn.into_active() {
                        Ok(active) => {
                            *phase = Phase::Active(active);
                            Ok(live)
                        }
                        Err(conn) => {
                            // `into_active` fails only before `Ready`; the pump
                            // reported `Ready`, so this is unreachable — restore
                            // and classify rather than panic on the dead arm.
                            *phase = Phase::Connecting(conn);
                            core::hint::cold_path();
                            Err(EngineError::WrongPhase(WrongPhase))
                        }
                    },
                    // The phase was `Connecting` an instant ago (the pump borrowed
                    // it in place and never replaces it); these are unreachable
                    // but the `mem::replace` match must be exhaustive.
                    Phase::Active(active) => {
                        *phase = Phase::Active(active);
                        core::hint::cold_path();
                        Err(EngineError::WrongPhase(WrongPhase))
                    }
                    Phase::Transitioning | Phase::Closed => {
                        core::hint::cold_path();
                        Err(EngineError::WrongPhase(WrongPhase))
                    }
                }
            }
        }
    }
}

/// Open a session over `transport`.
///
/// Primes the engine in its connecting phase: the `StartupMessage` for
/// `user`/`database`/`credentials` plus any consumer [`StartupParam`]s is
/// queued onto the engine's send buffer, and the body's first verb is
/// [`connect`](Engine::connect), which drives the handshake.
///
/// `body` is `for<'b>`, so each call mints a *fresh, invariant* brand: the
/// [`Live`] token handed to the body cannot escape the scope (returning it
/// is a lifetime error) and cannot be confused with another session's
/// token (a foreign brand is a type error).
///
/// # Errors
///
/// [`ConnFail`] if assembling the startup packet overflows the bounded frame
/// assembler — structurally unreachable for the bounded identifier newtypes
/// (a `write_buf` const-assert proves it), but propagated honestly: the
/// constructor is `Result`-typed and the forbid wall bars discharging it with a
/// panic-able unwrap. On `Ok`, the inner value is the body's own return.
#[inline]
pub fn session<T, R>(
    transport: T,
    user: &Ident,
    database: Option<&DatabaseName>,
    params: &[StartupParam],
    credentials: Credentials,
    body: impl for<'b> FnOnce(Engine<'b, T>, Live<'b>) -> R,
) -> Result<R, ConnFail>
where
    T: Transport,
{
    let mut send_buf = SendBuf::new();
    let conn = ConnectingEngine::start(&mut send_buf, user, database, params, credentials)?;
    let engine = Engine::new_in_scope(transport, send_buf, Phase::Connecting(conn));
    let live = Live::new_in_scope();
    Ok(body(engine, live))
}

/// Open an owned session handle over `transport`: the same connecting-phase
/// priming as [`session`], but the primed [`Engine`] and its linear [`Live`]
/// token are *returned* to the caller instead of lent to a `for<'b>` closure.
///
/// Use this when the engine must be **stored** — held in a struct, parked in a
/// pool, moved between calls. A poolable connection that keeps the engine plus
/// an `Option<Live<'static>>` across method calls cannot use [`session`]: that
/// API's `for<'b>` brand is *generative* (each call mints a fresh, invariant
/// `'b`), so the token and the engine are trapped inside the closure scope —
/// returning or storing either is a lifetime error. `open_owned` instead pins
/// the brand at `'static`, the one lifetime that outlives a pool, so the handle
/// can be owned for as long as the caller needs.
///
/// # Brand tradeoff (a constraint, state it plainly)
///
/// The generative `for<'b>` brand of [`session`] makes *cross-connection*
/// isolation tier-1: a token minted in one session has a brand no other
/// session shares, so using connection A's token on connection B's engine is a
/// compile error. Pinning the brand at `'static` gives every `open_owned`
/// handle the *same* brand, so that compile-time cross-connection wall is gone
/// — cross-connection isolation drops to tier-2-by-encapsulation: the owner
/// (a driver connection) keeps the engine and its `Live` private in one struct
/// and never hands the bare token out, so no caller can mix two connections'
/// tokens in the first place. *Within* a connection the linearity stays tier-1:
/// [`Live`] is non-`Clone`, every verb consumes it and returns it only on a
/// clean boundary, so at-most-one-command-in-flight is still a move-checked
/// invariant, not a runtime guard. [`session`] remains the scoped tier-1 API
/// for callers that do *not* need to store the handle; prefer it when the
/// engine's whole life fits in one scope.
///
/// # Errors
///
/// As [`session`]: [`ConnFail`] if assembling the startup packet overflows the
/// bounded frame assembler (structurally unreachable for the bounded
/// identifier newtypes, but propagated honestly rather than discharged with a
/// panic-able unwrap).
#[inline]
pub fn open_owned<T>(
    transport: T,
    user: &Ident,
    database: Option<&DatabaseName>,
    params: &[StartupParam],
    credentials: Credentials,
) -> Result<(Engine<'static, T>, Live<'static>), ConnFail>
where
    T: Transport,
{
    let mut send_buf = SendBuf::new();
    let conn = ConnectingEngine::start(&mut send_buf, user, database, params, credentials)?;
    let engine = Engine::new_in_scope(transport, send_buf, Phase::Connecting(conn));
    let live = Live::new_in_scope();
    Ok((engine, live))
}

// ===========================================================================
// Compile-time seam-composition gates
// ===========================================================================

/// Private witness transport for the compile-time gates below. Its I/O
/// methods are never driven (the scaffold verbs perform no exchange); it
/// exists only so the gates can name a concrete `T: Transport`.
struct WitnessTransport;

impl Transport for WitnessTransport {
    type Error = core::convert::Infallible;

    #[inline(always)]
    fn is_would_block(err: &Self::Error) -> bool {
        // `Infallible` is uninhabited: no error value exists, so the question is
        // vacuous — the empty match proves the branch unreachable, no fabricated
        // bool.
        match *err {}
    }

    #[inline(always)]
    fn read<'a>(
        &'a mut self,
        _buf: &'a mut [u8],
    ) -> impl core::future::Future<Output = Result<usize, Self::Error>> + Send + 'a {
        core::future::ready(Ok(0))
    }

    #[inline(always)]
    fn write<'a>(
        &'a mut self,
        _buf: &'a [u8],
    ) -> impl core::future::Future<Output = Result<usize, Self::Error>> + Send + 'a {
        core::future::ready(Ok(0))
    }

    #[inline(always)]
    fn flush<'a>(
        &'a mut self,
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send + 'a {
        core::future::ready(Ok(()))
    }

    #[inline(always)]
    fn shutdown<'a>(
        &'a mut self,
    ) -> impl core::future::Future<Output = Result<(), Self::Error>> + Send + 'a {
        core::future::ready(Ok(()))
    }
}

// Seam-composition + `Send` gate. The closure is never called; its body is
// type-checked at build time. The verb future must be `Send` (the
// load-bearing property for the async driver), and a linear token must
// thread through two SEQUENTIAL `await`s while one `async` scope holds
// `&mut self` across both — the form that would fail to compile if the
// verbs over-constrained to `&'b mut Engine<'b>` (the self-referential
// async footgun this seam is designed to avoid).
const _: fn() = || {
    fn assert_send<T: Send>() {}
    fn need_send<F: Send>(_: &F) {}

    assert_send::<WitnessTransport>();
    assert_send::<Live<'static>>();
    assert_send::<EngineError<core::convert::Infallible>>();
    // The `Transport::Error: Send` bound, locked at a concrete transport: a
    // wrapper transport's error union is `Send` only when the inner `Error` is,
    // so the bound must hold for every blessed transport, not just by trait
    // declaration.
    assert_send::<<WitnessTransport as Transport>::Error>();
    assert_send::<Engine<'static, WitnessTransport>>();
    assert_send::<SendBuf>();
    assert_send::<SendOverrun>();

    // The outbound drain future must be `Send` — the async driver polls it
    // across task boundaries. It composes the (Send) transport write future
    // over a `&mut SendBuf`, so it is Send when the transport is.
    let mut send_buf = SendBuf::new();
    let mut flush_transport = WitnessTransport;
    need_send(&flush(&mut send_buf, &mut flush_transport));

    // The phase value is irrelevant to `Send`-ness (which depends only on the
    // field TYPES); `Transitioning` is the lightest placeholder for this
    // compile-only witness (the closure is never executed).
    let mut engine: Engine<'static, WitnessTransport> =
        Engine::new_in_scope(WitnessTransport, SendBuf::new(), Phase::Transitioning);
    let live = Live::new_in_scope();

    let threaded = async move {
        // Each verb's future must be `Send` (the async driver polls them across
        // task boundaries), and the linear token threads through three SEQUENTIAL
        // `await`s while this one `async` scope holds `&mut engine` across all of
        // them — the shape that would not compile if a verb coupled the engine
        // borrow to the brand.
        let live = engine.connect(live).await?;
        let live = engine
            .ping(live, |_s: Surface<'_>| core::ops::ControlFlow::Continue(()))
            .await?
            .live;
        let live = engine
            .simple_query(live, "SELECT 1", |_s: Surface<'_>| {
                core::ops::ControlFlow::Continue(())
            })
            .await?
            .live;
        // Consume the token at the clean boundary; the brand must not escape the
        // async scope.
        let _ = live;
        Ok::<(), EngineError<core::convert::Infallible>>(())
    };
    need_send(&threaded);
};

// PUMP-FUTURE-SEND gate. The active pump's future must be `Send` — the async
// driver polls it across task boundaries. The closure is never called; its body
// is type-checked at build time. It instantiates `pump_active_to_boundary` at
// the witness transport and a function-pointer sink (the simplest `Send` sink),
// isolating the future's `Send`-ness to the pump's own captures (engine,
// transport, send buffer).
const _: fn() = || {
    fn need_send<F: Send>(_: &F) {}

    fn witness_sink(_: Surface<'_>) -> core::ops::ControlFlow<()> {
        core::ops::ControlFlow::Continue(())
    }
    // Coerce the fn item to a fn pointer via the typed binding (not an `as`
    // cast — the forbid wall bars `as`); the anonymous lifetime makes it the
    // higher-ranked `for<'e> fn(Surface<'e>)` the sink bound requires.
    let sink: fn(Surface<'_>) -> core::ops::ControlFlow<()> = witness_sink;

    let mut active = ActiveEngine::from_handshake(
        0_i32,
        crate::sensitive::Sensitive::new(0_i32),
        crate::action::TxStatus::Idle,
        IngestBuf::new(),
        None,
    );
    let mut transport = WitnessTransport;
    let mut send_buf = SendBuf::new();

    need_send(&pump_active_to_boundary(
        &mut active,
        &mut transport,
        &mut send_buf,
        sink,
    ));

    // The connecting pump's future must be `Send` for the same reason. It is a
    // pub free function the drivers call directly, so witness it independently
    // of the active pump (a real connecting engine, seated for Trust auth).
    let user = match crate::ident::Ident::try_from_str("w") {
        Ok(user) => user,
        Err(_) => return,
    };
    let mut conn_send_buf = SendBuf::new();
    let mut conn = match ConnectingEngine::start(
        &mut conn_send_buf,
        &user,
        None,
        &[],
        crate::password::Credentials::Trust,
    ) {
        Ok(conn) => conn,
        Err(_) => return,
    };
    let mut conn_transport = WitnessTransport;
    need_send(&pump_connecting_to_ready(
        &mut conn,
        &mut conn_transport,
        &mut conn_send_buf,
    ));
};

// Gated on `md5-auth`: the fixture credential is MD5 (its reply does not depend
// on the client nonce, so a static script suffices — a SCRAM reply would). The
// `scrub_drained` call it exercises is credential-agnostic, so the SCRAM path's
// own drop witnesses cover the same scrub when this is compiled out.
#[cfg(all(test, feature = "md5-auth"))]
mod connect_scrub_tests {
    //! Connect call-site teeth for the handshake-completion secret scrub.
    //!
    //! After [`connect`](super::Engine::connect) reaches the active phase, the
    //! handshake's secret-bearing outbound wire must be gone from the send
    //! buffer's queued region — restoring the prompt scrub the single-residence
    //! rework would otherwise defer to connection close. This drives a real
    //! `connect` over a static scripted server and reads the engine's private
    //! `send_buf` directly (an integration test cannot: the field is private and
    //! the queued-region probe is `#[cfg(test)] pub(crate)`).
    //!
    //! The credential here is MD5 (its reply does not depend on the client
    //! nonce, so a static script suffices) and the secret is the MD5 password
    //! message; the `scrub_drained` call it exercises is credential-agnostic, so
    //! it clears the SCRAM client proof on the SCRAM path identically. Removing
    //! the `scrub_drained` call in `connect` leaves the queued wire resident and
    //! turns this RED.

    use alloc::vec::Vec;
    use core::convert::Infallible;
    use core::future::{ready, Future};

    use super::{poll_once, session, Transport};
    use crate::wire::{TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_READY_FOR_QUERY};
    use crate::{Credentials, Ident, Password, Sensitive};

    /// Build a tagged, length-prefixed wire frame. `try_from` is infallible for
    /// these tiny fixtures; the saturating dead arm keeps the helper free of an
    /// unwrap.
    fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(body.len().saturating_add(5));
        out.push(tag);
        let len = u32::try_from(body.len().saturating_add(4)).unwrap_or(0);
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    /// The canonical MD5 handshake reply: a salt challenge, then
    /// `AuthenticationOk` + `BackendKeyData` + `ReadyForQuery`.
    fn md5_reply() -> Vec<u8> {
        let mut salt_body = 5_i32.to_be_bytes().to_vec();
        salt_body.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        let mut key_body = 4321_i32.to_be_bytes().to_vec();
        key_body.extend_from_slice(&8765_i32.to_be_bytes());

        let mut reply = Vec::new();
        reply.extend_from_slice(&frame(TAG_AUTHENTICATION.byte(), &salt_body));
        reply.extend_from_slice(&frame(TAG_AUTHENTICATION.byte(), &0_i32.to_be_bytes()));
        reply.extend_from_slice(&frame(TAG_BACKEND_KEY_DATA.byte(), &key_body));
        reply.extend_from_slice(&frame(TAG_READY_FOR_QUERY.byte(), b"I"));
        reply
    }

    /// Static scripted server: `read` drains a fixed reply; writes are accepted
    /// and discarded; every op resolves synchronously (one-poll).
    struct StaticServer {
        inbound: Vec<u8>,
        cursor: usize,
    }

    impl Transport for StaticServer {
        type Error = Infallible;

        fn is_would_block(err: &Self::Error) -> bool {
            match *err {}
        }

        fn read<'a>(
            &'a mut self,
            buf: &'a mut [u8],
        ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
            let n = (self.inbound.len().saturating_sub(self.cursor)).min(buf.len());
            let end = self.cursor.saturating_add(n);
            if let (Some(dst), Some(src)) = (buf.get_mut(..n), self.inbound.get(self.cursor..end)) {
                dst.copy_from_slice(src);
            }
            self.cursor = end;
            ready(Ok(n))
        }

        fn write<'a>(
            &'a mut self,
            buf: &'a [u8],
        ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
            ready(Ok(buf.len()))
        }

        fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
            ready(Ok(()))
        }

        fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
            ready(Ok(()))
        }
    }

    #[test]
    fn connect_scrubs_secret_outbound_wire_at_handshake_completion() {
        // Result threading instead of unwrap/expect: a fixture or handshake
        // failure surfaces as a non-`Ok(true)` value the final assert rejects.
        let scrubbed = match (Ident::try_from_str("corpus"), Password::try_from_str("hunter2")) {
            (Ok(user), Ok(password)) => {
                let creds = Credentials::Md5Password(Sensitive::new(password));
                let server = StaticServer {
                    inbound: md5_reply(),
                    cursor: 0,
                };
                session(server, &user, None, &[], creds, |mut engine, live| {
                    match poll_once(engine.connect(live)) {
                        // Reached active: the secret-bearing handshake wire must be
                        // gone from the live queued region of the send buffer.
                        Ok(Ok(_live)) => engine.send_buf.queued().is_empty(),
                        _ => false,
                    }
                })
            }
            _ => Ok(false),
        };
        assert_eq!(
            scrubbed,
            Ok(true),
            "connect must reach active and scrub the secret-bearing handshake wire from send_buf",
        );
    }
}

#[cfg(test)]
mod reclaim_send_buffer_engine_tests {
    //! Wiring witness for [`Engine::reclaim_send_buffer`]: the engine hop that a
    //! driver's `reset_session` calls actually reaches the send buffer's
    //! oversized-reclaim, and is phase-independent (a pure memory-hygiene op).
    //! The reclaim POLICY itself — the high-water threshold, scrub-before-shrink,
    //! and the no-thrash small-buffer no-op — is proven at the buffer level by
    //! `flush::reclaim_tests`.

    use super::flush::SEND_BUF_HIGH_WATER;
    use super::{Engine, Phase, SendBuf, WitnessTransport};

    /// A 2 MiB stand-in for a large `Bind` parameter payload grown onto the
    /// engine's outbound buffer.
    const BIG_PAYLOAD_LEN: usize = 2 * 1024 * 1024;

    #[test]
    fn reclaim_send_buffer_releases_an_oversized_backing() {
        let mut engine: Engine<'static, WitnessTransport> =
            Engine::new_in_scope(WitnessTransport, SendBuf::new(), Phase::Transitioning);
        // Grow the engine's outbound buffer past the high-water mark, then mark
        // it drained (a completed verb leaves `sent == len`).
        let mut big = alloc::vec::Vec::new();
        big.resize(BIG_PAYLOAD_LEN, 0x5Au8);
        engine.send_buf.enqueue(&big);
        let pending = engine.send_buf.pending_len();
        assert!(engine.send_buf.advance(pending).is_ok());
        let grown = engine.send_buf.capacity();
        assert!(grown > SEND_BUF_HIGH_WATER, "precondition: the payload must have grown the backing (was {grown})");

        engine.reclaim_send_buffer();

        assert!(
            engine.send_buf.capacity() < grown,
            "the engine hop must reach the reclaim and shrink the oversized backing",
        );
        assert!(engine.send_buf.capacity() <= SEND_BUF_HIGH_WATER);
    }

    #[test]
    fn reclaim_send_buffer_leaves_a_small_buffer_untouched() {
        let mut engine: Engine<'static, WitnessTransport> =
            Engine::new_in_scope(WitnessTransport, SendBuf::new(), Phase::Transitioning);
        engine.send_buf.enqueue(b"SELECT id, name FROM users WHERE id = $1");
        let pending = engine.send_buf.pending_len();
        assert!(engine.send_buf.advance(pending).is_ok());
        let cap = engine.send_buf.capacity();
        assert!(cap <= SEND_BUF_HIGH_WATER);

        engine.reclaim_send_buffer();

        assert_eq!(
            engine.send_buf.capacity(),
            cap,
            "a small buffer must not shrink — no thrash on the normal path",
        );
    }
}
