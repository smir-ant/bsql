//! The active-phase verb surface.
//!
//! Each verb has the same shape: `&mut self` plus the linear [`Live`] token in,
//! `Result<Outcome<St>, EngineError>` out. The token rides the `Ok` arm inside
//! the [`Outcome`] whenever the connection is ALIVE — both on a clean completion
//! and on a *recoverable* server error (the verb drains the recovering
//! `ReadyForQuery` to a clean idle, then reports
//! [`CommandStatus::ServerErrored`]; the error details already reached the caller
//! via the sink). `Err(EngineError)` is reserved for a FATAL outcome: the
//! connection is dead and the token is consumed. This is the tier-1 model —
//! token returned in `Ok` ⟺ connection alive; token consumed (no return) ⟺
//! connection dead — and it is why there is NO separate token-minting recovery
//! verb: a verb returns its one token only inside `Ok`, so the
//! "exactly one `Live` ⟺ at-most-one-command-in-flight" invariant stays
//! compile-enforced. Results are surfaced through a [`Surface`] sink, not
//! returned: the sans-I/O core is `no_std` and cannot name a typed `Row`, so it
//! lends RAW wire bytes and the typed layer above (`query!` decode /
//! `QueryResult`) owns the typing.
//!
//! [`recv_notification`](Engine::recv_notification) returns an
//! `Outcome<NotifyStatus>` instead — it has no recoverable-server-error axis (it
//! issues no command), only whether a notification arrived
//! ([`Received`](NotifyStatus::Received)) or the wait was quiet/timed-out
//! ([`Quiet`](NotifyStatus::Quiet)).
//!
//! The sole exception to the return shape is [`terminate`](Engine::terminate),
//! the session-ending verb: it consumes the token and returns
//! `Result<(), EngineError>` — no token comes back, because there is no reusable
//! connection after a graceful close.
//!
//! # The disjoint split-borrow (mandatory)
//!
//! Every I/O verb destructures `&mut self` into its three fields
//! (`transport` / `phase` / `send_buf`) so the pump can drive the active
//! engine over the transport while the other fields are borrowed disjointly.
//! Routing through a `self.active_mut()` helper would alias the whole engine
//! (E0499); the field-level destructure is the only shape that compiles.
//!
//! # Per-command compaction
//!
//! Each request-issuing verb calls [`SendBuf::reset`](super::SendBuf::reset) at
//! entry: the prior command drained at its `Idle` boundary, so `reset` empties
//! the buffer while retaining the allocation — steady-state zero-alloc on the
//! send path. `reset` truncates rather than zeroing, so the just-sent prior
//! frame's bytes linger in the spare capacity until `SendBuf`'s `Drop` scrub.
//! Those residual bytes are the application's own query text / parameter values,
//! never auth material — the handshake's secret-bearing wire flows only through
//! the connecting phase, which `connect` already scrubs at handshake completion.
//!
//! # Collect-all vs breakable
//!
//! Thirteen verbs are *collect-all*: their sink only ever
//! [`Continue`](ControlFlow::Continue)s, so the pump runs at `B = Never` and the
//! caller-stop boundary is uninhabited — consumed via [`absurd`], never a
//! wildcard. The lone breakable verb is [`recv_notification`](Engine::recv_notification)
//! (`B = ()`), whose sink stops the pump on the first notification.
//! [`terminate`](Engine::terminate) is outside this taxonomy entirely: it drives
//! no pump and takes no sink (its frame elicits no server reply), so it has no
//! caller-stop boundary to classify.
//!
//! # Schema surfacing (cutover composition)
//!
//! A statement's recovered schema (column type OIDs + names) is surfaced in
//! [`Surface::Deliver`] at the `CommandComplete` boundary — i.e. AFTER the rows.
//! So a RUNTIME-untyped consumer (one without a compile-time row type) buffers
//! the raw [`Surface::Row`] payloads as they arrive and decodes them at
//! `Deliver`, once the OIDs are known. The compile-time `query!`
//! path is unaffected: it knows the row shape statically and decodes each row
//! against `R: RowDecode` as it arrives, never consulting the surfaced OIDs.
//!
//! # Deferred: server-side cursor (`fetch`)
//!
//! The active dispatch is already row-limit/resume-ready — the
//! `begin_bind_execute_row_limited` / `begin_execute` state seams classify
//! `PortalSuspended` and a bare-`Execute` portal resume — but no `fetch(n)`
//! verb (a row-capped `Execute` that returns [`Boundary::Suspended`] and a
//! resume verb) is surfaced here yet. It is a deferred verb, not a gap in the
//! framing: the dispatch handles the wire; only the verb wrapper is absent.
//!
//! # Graceful close (`terminate`)
//!
//! The PostgreSQL graceful close is a `Terminate` frame (`'X'`, a 5-byte
//! tag-only frame `[b'X', 0, 0, 0, 4]`) sent to the server, then a
//! transport-level shutdown. [`terminate`](Engine::terminate) issues exactly
//! that: it pushes the `Terminate` frame, drains it with
//! [`flush`](super::flush), calls
//! [`Transport::shutdown`](super::Transport::shutdown), and consumes the
//! [`Live`](super::Live) token into the engine's closed phase so the connection
//! cannot be re-driven — a verb after it is a move error, and any phase accessor
//! is a classified [`WrongPhase`](super::WrongPhase).

use alloc::vec::Vec;
use core::ops::ControlFlow;

use super::error::{EngineError, ExpectedRowCount, RowCountViolation};
use super::frames;
use super::pump::{poll_once, pump_active_to_boundary, Boundary, SpuriousPending, Surface};
use super::seams::{absurd, CommandStatus, Live, Never, NotifyStatus, Outcome, Transport};
use super::{ActiveEngine, Engine, Phase, SendBuf};
use crate::action::TxStatus;
use crate::ident::StmtName;
use crate::params::ParamsWriter;
use crate::prepared::{PreparedQuery, RowDecode};
use crate::write_buf::{WriteBuf, WriteBufFull};

/// A runtime-prepared statement handle, formed from a
/// [`prepare`](Engine::prepare)'s surfaced schema.
///
/// Carries the statement name (for the `Bind`/`Close` wire) and the result-column
/// type OIDs recovered by the `prepare`'s `Describe` (threaded back into the
/// `Bind`+`Execute` so executed rows surface against the same schema — the
/// Execute reply re-sends no `RowDescription`).
///
/// Carries result OIDs but NOT column names: the bind/execute reply re-surfaces
/// empty names by design (only the OIDs drive decode), so a name-based runtime
/// decoder must stash the names captured from `prepare`'s [`Surface::Deliver`].
///
/// Deliberately not `Clone`/`Copy`: [`close_statement`](Engine::close_statement)
/// consumes it by value, so using a closed statement is a move error (E0382), not
/// a runtime use-after-close — the compile-time half of the safety invariant.
#[derive(Debug)]
pub struct PreparedStatement {
    stmt_name: StmtName,
    result_oids: Vec<u32>,
}

impl PreparedStatement {
    /// Form a handle from a statement name and the result-column type OIDs a
    /// [`prepare`](Engine::prepare) surfaced (via [`Surface::Deliver`]).
    #[inline]
    #[must_use]
    pub fn new(stmt_name: StmtName, result_oids: Vec<u32>) -> Self {
        Self {
            stmt_name,
            result_oids,
        }
    }

    /// The prepared statement's name.
    #[inline]
    #[must_use]
    pub fn stmt_name(&self) -> &StmtName {
        &self.stmt_name
    }

    /// The recovered result-column type OIDs.
    #[inline]
    #[must_use]
    pub fn result_oids(&self) -> &[u32] {
        &self.result_oids
    }
}

// The `StmtName` fixed buffer (65) + the result-OID `Vec` handle (24), padded to
// 96. The OID bytes live behind the `Vec`, off-stack.
crate::wire_pin!(PreparedStatement, size = 96, align = 8);

/// Build one frame into a transient scrub-on-drop scratch buffer and queue it.
///
/// `WriteBuf` is `heapless` (a stack array), so this allocates nothing on the
/// heap. A builder overflow (the SQL/params exceeded the bounded capacity) is the
/// classified [`EngineError::FrameTooLong`], never a silent truncation.
#[inline]
fn enqueue_frame<E>(
    send_buf: &mut SendBuf,
    build: impl FnOnce(&mut WriteBuf) -> Result<(), WriteBufFull>,
) -> Result<(), EngineError<E>> {
    let mut wb = WriteBuf::new();
    build(&mut wb).map_err(|_| {
        core::hint::cold_path();
        EngineError::FrameTooLong
    })?;
    send_buf.enqueue(wb.as_bytes());
    Ok(())
}

/// Stage a fused simple-query PRELUDE at the FRONT of the current command's
/// batch, if one was armed — today ONLY a deferred transaction `BEGIN`, fused
/// with the transaction's first statement.
///
/// Called by each request verb right after [`SendBuf::reset`](super::SendBuf::reset)
/// and BEFORE it enqueues its own frames: the prelude's `'Q'` frame is enqueued
/// first (so the single following flush carries the prelude AND the command — one
/// round trip, not two), then the prelude DRAIN is armed so the pump consumes the
/// prelude's response before the command's. Because it runs on the freshly-reset
/// (empty) buffer, the prelude is a natural PREPEND with no memmove.
///
/// A no-op — one predict-not-taken branch — when no prelude is pending, the steady
/// state. The prelude SQL is a fixed short `'static` simple query (the `BEGIN`
/// armed today) that fits the bounded [`WriteBuf`]; a builder overflow is the
/// classified [`EngineError::FrameTooLong`], never a silent truncation. The drain
/// only understands `BEGIN`'s non-row-bearing reply, so a row-bearing prelude
/// (a pool RESET returning rows) is a deferred capability, not armed here.
#[inline]
fn stage_prelude<E>(
    active: &mut ActiveEngine,
    send_buf: &mut SendBuf,
) -> Result<(), EngineError<E>> {
    // PEEK, don't consume: the prelude stays armed until its reply is DRAINED. If a
    // LATER command's staging overflows (`FrameTooLong`) after this frame is
    // enqueued, `abort_pipeline_staging` discards the buffer but leaves
    // `pending_prelude` set, so the next verb re-fuses the deferred `BEGIN` — never
    // a lost transaction. A successful drain clears it (`finish_prelude`).
    if let Some(sql) = active.pending_prelude() {
        core::hint::cold_path();
        enqueue_frame(send_buf, |wb| frames::build_simple_query(wb, sql.as_bytes()))?;
        active.arm_prelude();
    }
    Ok(())
}

/// Stage a simple-query (`'Q'`) request onto the send buffer: reset, prelude,
/// then the header + streamed SQL body + NUL terminator.
///
/// SHARED by [`run_simple`](Engine::run_simple) (collect-all) and
/// [`query_break`](Engine::query_break) (breakable), exactly as
/// [`stage_compiled_query`] is shared by the two compiled verbs — so a collect
/// verb and its streaming peer cannot drift in their wire framing (a drift would
/// put different bytes on the wire for the same SQL).
///
/// The SQL body streams directly onto the (growable) send buffer rather than
/// being copied into the bounded [`WriteBuf`]: only the 5-byte header is built
/// into the scratch buffer, so a multi-kilobyte query is not capped at
/// `MAX_OWNED_SEND_LEN`. The flushed bytes — header, SQL, NUL — are contiguous
/// and byte-identical to the whole-frame builder's output.
#[inline]
fn stage_simple_query<E>(
    active: &mut ActiveEngine,
    send_buf: &mut SendBuf,
    sql: &str,
) -> Result<(), EngineError<E>> {
    send_buf.reset();
    stage_prelude(active, send_buf)?;
    let sql_bytes = sql.as_bytes();
    let sql_len = u32::try_from(sql_bytes.len()).map_err(|_| {
        core::hint::cold_path();
        EngineError::FrameTooLong
    })?;
    enqueue_frame(send_buf, |wb| frames::build_simple_query_header(wb, sql_len))?;
    send_buf.enqueue(sql_bytes);
    send_buf.enqueue(&[0]);
    Ok(())
}

/// Stage a fused unnamed extended-protocol request onto the send buffer:
/// `Parse`(unnamed) + `Bind` + `Describe`(portal) + `Execute` + `Sync`, and seat
/// the fused awaiting state.
///
/// SHARED by [`query_params_fused`](Engine::query_params_fused) (collect-all) and
/// [`query_params_fused_break`](Engine::query_params_fused_break) (breakable) so
/// the runtime one-round-trip framing cannot drift between the eager and the
/// streaming dynamic verb. See [`query_params_fused`](Engine::query_params_fused)
/// for the fusion rationale (one round trip, inline `Describe`, no `Close`).
#[inline]
fn stage_fused_params<P, E>(
    active: &mut ActiveEngine,
    send_buf: &mut SendBuf,
    sql: &str,
    params: &P,
) -> Result<(), EngineError<E>>
where
    P: ParamsWriter,
{
    send_buf.reset();
    stage_prelude(active, send_buf)?;
    // Parse(unnamed ""): stream the whole frame — SQL body AND the parameter-type
    // OID list from `P::OIDS` — onto the growable send buffer, so a multi-kilobyte
    // runtime query is not capped at MAX_OWNED_SEND_LEN. Declaring `P::OIDS` makes
    // the server decode each binary parameter AS the client's encoded type (a type
    // disagreement is then a LOUD server error, never a silent reinterpretation),
    // exactly as the compile-checked `query!` path bakes its OIDs into its Parse.
    frames::build_parse(&mut send_buf.frame(), b"", sql.as_bytes(), P::OIDS)
        .map_err(frame_too_long)?;
    // Bind(portal "" from the unnamed statement ""); Describe(portal "");
    // Execute(portal "", no row limit); Sync — one pipelined batch.
    frames::build_bind(&mut send_buf.frame(), b"", b"", params).map_err(frame_too_long)?;
    enqueue_frame(send_buf, |wb| frames::build_describe_portal(wb, b""))?;
    enqueue_frame(send_buf, |wb| frames::build_execute(wb, b"", 0))?;
    send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
    active.begin_fused_parse_bind_describe_execute();
    Ok(())
}

/// Classify the RAW [`Boundary`] a dynamic BREAKABLE verb's pump reached: an
/// ALIVE boundary (`Idle` / `Failed` / `Stopped`) passes through so the verb can
/// wrap it with its token into an `Ok` [`Outcome`]; a fatal boundary
/// (`Closed` / `Suspended`) is an `Err` and the token is consumed.
///
/// The dynamic (unnamed / simple-query) peer of
/// [`query_params_break`](Engine::query_params_break)'s post-pump match, WITHOUT
/// its statement-cache settle: the fused and simple-query dynamic paths hold no
/// engine-side statement cache, so there is nothing to record. A dirty
/// [`Stopped`](Boundary::Stopped) (caller break) or [`Failed`](Boundary::Failed)
/// (server error) still rides an alive boundary — the connection owes frames the
/// caller reclaims with [`drain`](Engine::drain). The token stays with the verb
/// (never threaded through this helper), matching the [`drive_to_outcome`] /
/// [`classify_drained`] helper shape.
#[inline]
fn classify_break_boundary<E, B>(boundary: Boundary<B>) -> Result<Boundary<B>, EngineError<E>> {
    match boundary {
        Boundary::Idle => Ok(Boundary::Idle),
        Boundary::Failed => {
            core::hint::cold_path();
            Ok(Boundary::Failed)
        }
        // The caller broke early: the connection is alive but DIRTY. The caller
        // drains to reclaim it.
        Boundary::Stopped(b) => Ok(Boundary::Stopped(b)),
        Boundary::Closed => {
            core::hint::cold_path();
            Err(EngineError::ProtocolViolation)
        }
        Boundary::Suspended => {
            core::hint::cold_path();
            Err(EngineError::UnexpectedSuspend)
        }
    }
}

/// Classify a streamed-`Bind` builder overflow as [`EngineError::FrameTooLong`].
///
/// The single cold-path landing for the STREAMING Bind assembly — the peer of
/// [`enqueue_frame`]'s bounded-builder overflow. A `Bind` streams its parameter
/// block straight onto the growable send buffer via [`SendBuf::frame`], so it
/// has NO fixed capacity cap; this maps the only remaining, architecturally-dead
/// overflow (a frame body exceeding the `u32` / `i32` wire length field — beyond
/// 2 GiB, an OOM long before then) to the same classified error a bounded
/// frame's overflow raises, never a panic or truncation.
#[inline]
fn frame_too_long<E>(_: WriteBufFull) -> EngineError<E> {
    core::hint::cold_path();
    EngineError::FrameTooLong
}

/// The COPY-in batched-flush threshold, in bytes.
///
/// Streamed `CopyData` accumulates in the send buffer and is flushed to the
/// socket only once the pending bytes reach this, so a bulk load of N small
/// chunks costs about `total_bytes / THRESHOLD` socket writes instead of N — a
/// 100–1000× reduction in write syscalls on a megarow COPY. A single chunk at or
/// above this is streamed DIRECTLY from the borrowed slice (never copied into the
/// buffer), so the buffer never holds a huge body. Because one sub-threshold
/// chunk is appended before the pending-length check, the buffer is bounded to
/// strictly under `2 * THRESHOLD` at all times — constant memory regardless of
/// the total COPY size. 64 KiB matches a typical socket send buffer.
const COPY_IN_FLUSH_THRESHOLD: usize = 64 * 1024;

/// Pump a *collect-all* command to its boundary and resolve the
/// [`CommandStatus`], draining a recoverable server error's owed
/// `ReadyForQuery` so the linear token can ride back in `Ok`.
///
/// - `Boundary::Idle` → [`Completed`](CommandStatus::Completed): the command
///   completed at a clean idle.
/// - `Boundary::Failed` → RECOVERABLE: the server sent an `ErrorResponse`
///   (whose raw bytes the sink already saw) and owes a trailing
///   `ReadyForQuery`. The verb DRAINS that one frame to a clean idle (the
///   request was already flushed, so the entry flush is a no-op) and reports
///   [`ServerErrored`](CommandStatus::ServerErrored) — the connection survives,
///   so the token rides `Ok`.
/// - `Boundary::Closed`/`Suspended`, or any non-`Idle` boundary while draining
///   → FATAL `Err`: the connection is dead and the token is consumed.
///
/// `Boundary` is `#[non_exhaustive]`, but this is a within-crate match, so every
/// arm is enumerated with no wildcard — a future boundary forces a decision. At
/// `B = Never` the [`Stopped`](Boundary::Stopped) value is uninhabited and is
/// discharged by [`absurd`], never a `unreachable!()`.
async fn drive_to_outcome<T, S>(
    active: &mut ActiveEngine,
    transport: &mut T,
    send_buf: &mut SendBuf,
    mut sink: S,
) -> Result<CommandStatus, EngineError<T::Error>>
where
    T: Transport,
    S: FnMut(Surface<'_>) -> ControlFlow<Never>,
{
    // Pass the caller's sink by `&mut` so it survives the first pump and can be
    // reused for the recovery drain below. `&mut S` is itself `FnMut`, so this is
    // the identical sink type at `B = Never`, not a fresh one.
    let boundary = pump_active_to_boundary(active, transport, send_buf, &mut sink).await?;
    match boundary {
        Boundary::Idle => Ok(CommandStatus::Completed),
        Boundary::Failed => {
            core::hint::cold_path();
            // Drain the recovering `ReadyForQuery` the server owes after the
            // `ErrorResponse`. The prior request was already flushed, so the
            // pump's entry flush is a no-op and only the trailing frames are read.
            // Thread the CALLER's sink (not a noop) through the drain: a
            // wire-legal `NoticeResponse` / `ParameterStatus` / `NotificationResponse`
            // arriving in the recovery window (after the error, before the RFQ)
            // must still surface, not be silently dropped. The sink is
            // `B = Never` (Continue-only), so it cannot `Break` and therefore
            // cannot change the drain's boundary — the drain still reaches `Idle`
            // for the `ServerErrored` outcome.
            let drained =
                pump_active_to_boundary(active, transport, send_buf, &mut sink).await?;
            classify_drained(drained)
        }
        Boundary::Closed => {
            core::hint::cold_path();
            Err(EngineError::ProtocolViolation)
        }
        Boundary::Suspended => {
            core::hint::cold_path();
            Err(EngineError::UnexpectedSuspend)
        }
        Boundary::Stopped(never) => absurd(never),
    }
}

/// Classify the boundary reached while DRAINING a recoverable error's owed
/// `ReadyForQuery`. A clean `Idle` is the recovered
/// [`ServerErrored`](CommandStatus::ServerErrored) outcome; any other boundary
/// means the recovery protocol was violated (a second error, a teardown, an
/// unexpected suspend) and is fatal — the connection is dead, the token is
/// consumed.
///
/// On which arm a malformed "second error during recovery" lands: the active
/// dispatch tears the connection down on an out-of-phase frame, so a wire-legal
/// frame the engine deems illegal in the recovering state reaches `Boundary::Closed`
/// → `ProtocolViolation`; only a genuine second `ErrorResponse` framed by the
/// server reaches `Boundary::Failed` → `ServerError`. Both are fatal here (the
/// token is consumed either way), so the arm distinction is descriptive, not a
/// behavioural fork.
#[inline]
fn classify_drained<E>(boundary: Boundary<Never>) -> Result<CommandStatus, EngineError<E>> {
    match boundary {
        Boundary::Idle => Ok(CommandStatus::ServerErrored),
        Boundary::Failed => {
            core::hint::cold_path();
            Err(EngineError::ServerError)
        }
        Boundary::Closed => {
            core::hint::cold_path();
            Err(EngineError::ProtocolViolation)
        }
        Boundary::Suspended => {
            core::hint::cold_path();
            Err(EngineError::UnexpectedSuspend)
        }
        Boundary::Stopped(never) => absurd(never),
    }
}

/// Flatten a single-poll [`Outcome`] result into the bare-token error surface
/// used by the synchronous [`transaction`](Engine::transaction) wrapper: a clean
/// [`Completed`](CommandStatus::Completed) yields the threaded token; a
/// [`ServerErrored`](CommandStatus::ServerErrored) is an error to the
/// transaction (a failed `BEGIN`/`COMMIT`/body aborts it), so it surfaces as
/// [`EngineError::ServerError`] and the connection is dropped.
#[inline]
fn flatten_poll<'b, E>(
    polled: Result<Result<Outcome<'b, CommandStatus>, EngineError<E>>, SpuriousPending>,
) -> Result<Live<'b>, EngineError<E>> {
    match polled {
        Ok(Ok(Outcome {
            live,
            status: CommandStatus::Completed,
        })) => Ok(live),
        Ok(Ok(Outcome {
            status: CommandStatus::ServerErrored,
            ..
        })) => {
            core::hint::cold_path();
            Err(EngineError::ServerError)
        }
        Ok(Err(e)) => Err(e),
        Err(SpuriousPending) => {
            core::hint::cold_path();
            Err(EngineError::SpuriousPending)
        }
    }
}

/// Stage a compile-checked query's request bytes and seat the awaiting state —
/// the pre-drive half SHARED by [`query_params`](Engine::query_params) (collect-
/// all, `B = Never`) and [`query_params_break`](Engine::query_params_break)
/// (breakable, `B = user`), so the two verbs cannot drift in their cache decision
/// or their wire framing (a drift would resurrect the duplicate-statement error
/// this path exists to prevent).
///
/// Decides the cache HIT/MISS from the recorded set, then builds the wire: on a
/// MISS a `Close`(statement) + the baked `Parse` template + `Bind` + `Execute` +
/// `Sync` — the leading `Close` makes the re-`Parse` idempotent (a `Close` of a
/// nonexistent statement is a wire no-op), so no duplicate-statement error is
/// possible in any case; on a HIT a bare `Bind` + `Execute` + `Sync` reusing the
/// server-side plan. Seats the matching awaiting state (a MISS awaits
/// `CloseComplete` first, a HIT awaits `BindComplete` directly — seating the wrong
/// one would deadlock on an ack that never comes). Returns whether this is a plan
/// REUSE (HIT), which the caller threads into [`settle_statement_cache`] for the
/// post-drive bookkeeping.
fn stage_compiled_query<P, R, E>(
    active: &mut ActiveEngine,
    send_buf: &mut SendBuf,
    q: &PreparedQuery<P, R>,
    args: &P,
) -> Result<bool, EngineError<E>>
where
    P: ParamsWriter,
    R: RowDecode,
{
    let reuse = active.is_statement_parsed(q.stmt_name);
    send_buf.reset();
    // Prepend a fused prelude (a deferred BEGIN) ahead of this command's frames,
    // if one was armed — one flush then carries both.
    stage_prelude(active, send_buf)?;
    if !reuse {
        enqueue_frame(send_buf, |wb| {
            frames::build_close_statement(wb, q.stmt_name.as_bytes())
        })?;
        send_buf.enqueue(q.parse_template);
    }
    // The Bind streams DIRECTLY onto the growable send buffer (unbounded params);
    // the Close/Parse above and Execute/Sync below are fixed-size and stay
    // bounded. `send_buf.frame()`'s borrow ends with this statement, freeing
    // `send_buf` for the following enqueues.
    frames::build_bind_prepared(&mut send_buf.frame(), q.bind_execute_prefix, args)
        .map_err(frame_too_long)?;
    // On a cache MISS (a FRESH Parse — where the resolved column types can diverge
    // from the migration schema the carrier was typed against), append a
    // `Describe`(portal) so the server returns a `RowDescription`; the result-schema
    // guard then verifies each runtime column OID against `q.row_oids`. A HIT reuses
    // an existing server-side plan whose result type PostgreSQL itself refuses to
    // change silently (`0A000`), so it needs no Describe — its wire + hot path stay
    // byte-identical.
    if !reuse {
        enqueue_frame(send_buf, |wb| frames::build_describe_portal(wb, b""))?;
    }
    send_buf.enqueue(&crate::prepared::EXECUTE_EMPTY_PORTAL_NO_LIMIT);
    send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
    if reuse {
        active.begin_bind_execute(q.row_oids);
    } else {
        active.begin_close_parse_bind_execute(q.row_oids);
        active.arm_result_guard();
    }
    Ok(reuse)
}

/// The post-drive statement-cache bookkeeping SHARED by the compile-checked query
/// verbs (see [`stage_compiled_query`]).
///
/// - A HIT (`reuse`) that server-errored means the recorded statement was dropped
///   out of band (`DISCARD ALL` / `DEALLOCATE`) → EVICT it, so the next use is a
///   self-healing MISS that re-creates it. The error itself already rode the sink.
/// - A MISS that completed at a clean idle (`completed_at_idle` AND the connection
///   is back at [`TxStatus::Idle`] — the wrapping transaction, if any, committed)
///   is durable → RECORD it for future HITs.
/// - Anything else (a MISS still inside a transaction, or a command that neither
///   completed cleanly nor server-errored — e.g. an early caller STOP mid-stream)
///   records nothing: correctness never depends on the cache (the MISS path
///   re-creates the statement), so it is simply left a future MISS.
fn settle_statement_cache<P, R>(
    active: &mut ActiveEngine,
    q: &PreparedQuery<P, R>,
    reuse: bool,
    completed_at_idle: bool,
    server_errored: bool,
) where
    P: ParamsWriter,
    R: RowDecode,
{
    if reuse {
        if server_errored {
            active.evict_statement(q.stmt_name);
        }
    } else if completed_at_idle
        && matches!(active.tx_status(), TxStatus::Idle)
        && !active.has_result_oid_mismatch()
    {
        // A MISS whose result-schema guard caught a mismatch is NOT recorded: the
        // query FAILED (its runtime column types diverged from the migration), so
        // recording it would make a REPEAT a cache HIT that skips the `Describe` +
        // guard and silently mis-decodes. Leaving it unrecorded makes the repeat a
        // fresh MISS that re-`Describe`s + re-guards (the leading idempotent `Close`
        // re-Parses cleanly).
        active.record_statement_parsed(q.stmt_name);
    }
}

/// Stage ONE pipelined command's request bytes onto the send buffer WITHOUT the
/// trailing `Sync` — the per-command half of a heterogeneous
/// `pipeline((...))` batch, hoisting the single `Sync` to the batch end
/// ([`stage_pipeline_seal`](Engine::stage_pipeline_seal)).
///
/// This is [`stage_compiled_query`] unrolled for the batch: the SAME cache decision
/// (`is_statement_parsed` → a MISS leads with `Close`+`Parse`, a HIT reuses the
/// server-side plan) and the SAME wire framing (`Close`/`Parse`/`Bind`/`Execute`),
/// so a pipelined command puts IDENTICAL bytes on the wire as its serial peer for
/// the same query — only the `Sync` moves. The leading `Close` on a MISS makes the
/// `Parse` idempotent, so two IDENTICAL queries in one batch (both MISS, both
/// `Close`+`Parse` the same content-addressed name) are safe in order.
///
/// - FIRST command: reset the buffer, stage any fused prelude (a deferred
///   transaction `BEGIN`), seat the leading [`ActiveState`](super::ActiveEngine)
///   (a MISS's `CloseParseBindExecute…` chain or a HIT's bind wait), and arm
///   [`begin_pipeline`](ActiveEngine::begin_pipeline) so the command boundary routes
///   through `PipelineAwaitingNextOrRfq`.
/// - SUBSEQUENT command: append only its frames; the receive FSM seats it as its
///   acks arrive (never a separate seat).
fn stage_pipeline_frames<P, R, E>(
    active: &mut ActiveEngine,
    send_buf: &mut SendBuf,
    q: &PreparedQuery<P, R>,
    args: &P,
    first: bool,
) -> Result<(), EngineError<E>>
where
    P: ParamsWriter,
    R: RowDecode,
{
    let reuse = active.is_statement_parsed(q.stmt_name);
    if first {
        send_buf.reset();
        stage_prelude(active, send_buf)?;
    }
    if !reuse {
        enqueue_frame(send_buf, |wb| {
            frames::build_close_statement(wb, q.stmt_name.as_bytes())
        })?;
        send_buf.enqueue(q.parse_template);
    }
    // The `Bind` streams its parameter block DIRECTLY onto the growable send buffer
    // (unbounded params), exactly as the serial path; `Execute` is fixed-size. NO
    // `Sync` — it is hoisted to the batch end so the whole batch is ONE implicit
    // transaction under ONE trailing Sync.
    frames::build_bind_prepared(&mut send_buf.frame(), q.bind_execute_prefix, args)
        .map_err(frame_too_long)?;
    send_buf.enqueue(&crate::prepared::EXECUTE_EMPTY_PORTAL_NO_LIMIT);
    if first {
        if reuse {
            active.begin_bind_execute(q.row_oids);
        } else {
            active.begin_close_parse_bind_execute(q.row_oids);
        }
        active.begin_pipeline();
    }
    Ok(())
}

/// Stage ONE command of a HOMOGENEOUS `execute_batch` — the PARSE-ONCE peer of
/// [`stage_pipeline_frames`]. The heterogeneous pipeline stages each element with
/// its OWN cache decision (a MISS re-`Parse`s every element), but an `execute_batch`
/// runs the SAME carrier `Q` against N parameter sets, so `Q` is `Parse`d at most
/// ONCE (command 0's MISS `Close`+`Parse`, or a prior-cached HIT) and every
/// SUBSEQUENT command is a BARE `Bind`+`Execute` referencing that one server-side
/// statement — the bulk win. Re-`Parse`ing the same content-addressed name N times
/// would be correct (the leading `Close` makes it idempotent) but would defeat the
/// whole point.
///
/// - `first = true` (command 0): IDENTICAL to a pipeline's first command — reset the
///   buffer, stage any fused prelude (a deferred transaction `BEGIN`), take the
///   MISS/HIT cache decision (`Close`+`Parse` on a MISS), stream `Bind`+`Execute`,
///   seat the leading state, and arm [`begin_pipeline`](ActiveEngine::begin_pipeline)
///   so the command boundary routes through `PipelineAwaitingNextOrRfq`. Delegates
///   to [`stage_pipeline_frames`] verbatim, so the two batch verbs cannot drift in
///   their first-command framing or cache decision.
/// - `first = false` (a subsequent command): a BARE `Bind`+`Execute` — NO
///   `Close`+`Parse` (command 0 already created / reused the server-side statement,
///   and the commands are pipelined in ORDER so command 0's `Parse` is processed
///   before this command's `Bind`), and NO seat (the receive FSM seats each
///   subsequent command from its own `BindComplete` at `PipelineAwaitingNextOrRfq`).
///   NO `Sync` — the single trailing `Sync` is hoisted to the batch end, so the
///   whole batch is ONE implicit transaction.
fn stage_batch_frames<P, R, E>(
    active: &mut ActiveEngine,
    send_buf: &mut SendBuf,
    q: &PreparedQuery<P, R>,
    args: &P,
    first: bool,
) -> Result<(), EngineError<E>>
where
    P: ParamsWriter,
    R: RowDecode,
{
    if first {
        return stage_pipeline_frames(active, send_buf, q, args, true);
    }
    // Parse-once: a subsequent command reuses the statement command 0 created. The
    // `Bind` streams its parameter block DIRECTLY onto the growable send buffer
    // (unbounded params), exactly as the serial / pipeline paths; `Execute` is
    // fixed-size. NO `Close`/`Parse`, NO seat, NO `Sync`.
    frames::build_bind_prepared(&mut send_buf.frame(), q.bind_execute_prefix, args)
        .map_err(frame_too_long)?;
    send_buf.enqueue(&crate::prepared::EXECUTE_EMPTY_PORTAL_NO_LIMIT);
    Ok(())
}

impl<'b, T: Transport> Engine<'b, T> {
    /// Drain a `Sync` and await the connection's `ReadyForQuery` — a liveness
    /// round trip. Surfaces nothing on a quiet connection; any asynchronous
    /// `ParameterStatus`/`NoticeResponse` reaches the sink. `B = Never`.
    ///
    /// # Errors
    ///
    /// [`EngineError`] per the pump (see [`simple_query`](Self::simple_query)).
    pub async fn ping<S>(
        &mut self,
        live: Live<'b>,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        stage_prelude(active, send_buf)?;
        send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
        let status = drive_to_outcome(active, transport, send_buf, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Issue a simple-query (`'Q'`) command. A `;`-separated batch surfaces one
    /// [`Surface::Deliver`] per statement and its rows as [`Surface::Row`].
    /// `B = Never`.
    ///
    /// # Errors
    ///
    /// - [`EngineError::FrameTooLong`] — the SQL exceeded the bounded builder.
    /// - [`EngineError::ServerError`] — a server `ErrorResponse` (raw bytes
    ///   surfaced via the sink first).
    /// - [`EngineError::ProtocolViolation`] — the connection was torn down.
    /// - the pump's transport / framing errors.
    pub async fn simple_query<S>(
        &mut self,
        live: Live<'b>,
        sql: &str,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let status = self.run_simple(sql, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Issue a single row-returning query (`'Q'`). Rows surface as
    /// [`Surface::Row`]; the schema and tag arrive in [`Surface::Deliver`]. Wire-
    /// identical to [`simple_query`](Self::simple_query); the distinct name marks
    /// the single-statement row intent. `B = Never`.
    ///
    /// # Errors
    ///
    /// As [`simple_query`](Self::simple_query).
    pub async fn query<S>(
        &mut self,
        live: Live<'b>,
        sql: &str,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let status = self.run_simple(sql, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Issue a query expected to return exactly one row. Identical wire to
    /// [`query`](Self::query); after the boundary the surfaced row count is
    /// checked. `B = Never`.
    ///
    /// # Errors
    ///
    /// [`EngineError::RowCount`] (with [`ExpectedRowCount::ExactlyOne`]) when the
    /// command surfaced a number of rows other than one; otherwise as
    /// [`query`](Self::query).
    pub async fn query_one<S>(
        &mut self,
        live: Live<'b>,
        sql: &str,
        mut sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let mut rows = 0usize;
        let status = self
            .run_simple(sql, |surface| {
                if matches!(surface, Surface::Row(_)) {
                    rows = rows.saturating_add(1);
                }
                sink(surface)
            })
            .await?;
        // A server error aborted the command before its rows: the failure (not a
        // row-count violation) is the outcome, so the guard is skipped. The
        // row-count contract applies only to a cleanly completed command.
        match status {
            CommandStatus::ServerErrored => Ok(Outcome { live, status }),
            CommandStatus::Completed if rows == 1 => Ok(Outcome { live, status }),
            CommandStatus::Completed => {
                core::hint::cold_path();
                Err(EngineError::RowCount(RowCountViolation {
                    expected: ExpectedRowCount::ExactlyOne,
                    got: rows,
                }))
            }
        }
    }

    /// Issue a query expected to return at most one row. Identical wire to
    /// [`query`](Self::query). `B = Never`.
    ///
    /// # Errors
    ///
    /// [`EngineError::RowCount`] (with [`ExpectedRowCount::AtMostOne`]) when the
    /// command surfaced more than one row; otherwise as [`query`](Self::query).
    pub async fn query_opt<S>(
        &mut self,
        live: Live<'b>,
        sql: &str,
        mut sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let mut rows = 0usize;
        let status = self
            .run_simple(sql, |surface| {
                if matches!(surface, Surface::Row(_)) {
                    rows = rows.saturating_add(1);
                }
                sink(surface)
            })
            .await?;
        // A server error aborts the row-count guard (see `query_one`).
        match status {
            CommandStatus::ServerErrored => Ok(Outcome { live, status }),
            CommandStatus::Completed if rows <= 1 => Ok(Outcome { live, status }),
            CommandStatus::Completed => {
                core::hint::cold_path();
                Err(EngineError::RowCount(RowCountViolation {
                    expected: ExpectedRowCount::AtMostOne,
                    got: rows,
                }))
            }
        }
    }

    /// Execute a non-row command (`'Q'`); the affected-row count arrives in
    /// [`Surface::Deliver`]'s tag. Wire-identical to
    /// [`simple_query`](Self::simple_query). `B = Never`.
    ///
    /// # Errors
    ///
    /// As [`simple_query`](Self::simple_query).
    pub async fn execute<S>(
        &mut self,
        live: Live<'b>,
        sql: &str,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let status = self.run_simple(sql, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Shared simple-query (`'Q'`) drive: compact, build, pump to a boundary,
    /// draining a recoverable error to a clean idle (see [`drive_to_outcome`]).
    async fn run_simple<S>(
        &mut self,
        sql: &str,
        sink: S,
    ) -> Result<CommandStatus, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        stage_simple_query(active, send_buf, sql)?;
        drive_to_outcome(active, transport, send_buf, sink).await
    }

    /// Prepare a statement: `Parse` + statement `Describe` + a single `Sync`. The
    /// recovered schema (column type OIDs + names) is surfaced via
    /// [`Surface::Deliver`] so the caller forms a [`PreparedStatement`] from the
    /// passed `stmt_name` and the OIDs. `B = Never`.
    ///
    /// The single-`Sync` bundling recovers the parameter and row descriptions in
    /// one round trip; the `query!` macro path recovers its schema at
    /// compile time and does not use this verb.
    ///
    /// # Errors
    ///
    /// As [`simple_query`](Self::simple_query) (`FrameTooLong` covers an oversize
    /// statement name or SQL).
    pub async fn prepare<S>(
        &mut self,
        live: Live<'b>,
        stmt_name: &StmtName,
        sql: &str,
        param_oids: &[u32],
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        stage_prelude(active, send_buf)?;
        // Stream the whole Parse frame — statement name, SQL, and the parameter-type
        // OID list — onto the (growable) send buffer with a back-patched length, so
        // a multi-kilobyte prepared SQL is not capped at `MAX_OWNED_SEND_LEN` (the
        // same streaming shape as `Bind`). `param_oids` is `P::OIDS` for the DYNAMIC
        // plan-cache PROMOTE (so a repeated `query_params` prepares its named plan
        // with the caller's encoded types) or `&[]` for the explicit `prepare`
        // handle (whose parameter types are only known at a later `Bind`).
        frames::build_parse(&mut send_buf.frame(), stmt_name.as_bytes(), sql.as_bytes(), param_oids)
            .map_err(frame_too_long)?;
        enqueue_frame(send_buf, |wb| {
            frames::build_describe_statement(wb, stmt_name.as_bytes())
        })?;
        send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
        active.begin_prepare();
        let status = drive_to_outcome(active, transport, send_buf, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Execute a prepared statement returning rows: `Bind` + `Execute` + `Sync`.
    /// Rows surface as [`Surface::Row`] against the statement's recovered OIDs.
    /// `B = Never`.
    ///
    /// # Errors
    ///
    /// As [`simple_query`](Self::simple_query) (`FrameTooLong` covers oversize
    /// encoded parameters).
    pub async fn query_prepared<P, S>(
        &mut self,
        live: Live<'b>,
        stmt: &PreparedStatement,
        params: &P,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        P: ParamsWriter,
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let status = self.run_bind_execute(stmt, params, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Execute a prepared statement for its side effect: `Bind` + `Execute` +
    /// `Sync`. Wire-identical to [`query_prepared`](Self::query_prepared); the
    /// distinct name marks the non-row intent (the affected count rides
    /// [`Surface::Deliver`]). `B = Never`.
    ///
    /// # Errors
    ///
    /// As [`query_prepared`](Self::query_prepared).
    pub async fn execute_prepared<P, S>(
        &mut self,
        live: Live<'b>,
        stmt: &PreparedStatement,
        params: &P,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        P: ParamsWriter,
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let status = self.run_bind_execute(stmt, params, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Shared `Bind` + `Execute` + `Sync` drive over a named prepared statement.
    async fn run_bind_execute<P, S>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
        sink: S,
    ) -> Result<CommandStatus, EngineError<T::Error>>
    where
        P: ParamsWriter,
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        stage_prelude(active, send_buf)?;
        // Unnamed portal; the named statement was parsed by `prepare`. The Bind
        // streams onto the growable send buffer (unbounded params); Execute+Sync
        // below stay bounded.
        frames::build_bind(&mut send_buf.frame(), b"", stmt.stmt_name().as_bytes(), params)
            .map_err(frame_too_long)?;
        enqueue_frame(send_buf, |wb| frames::build_execute(wb, b"", 0))?;
        send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
        active.begin_bind_execute(stmt.result_oids());
        drive_to_outcome(active, transport, send_buf, sink).await
    }

    /// Run a one-shot RUNTIME-param query in ONE round trip: `Parse`(unnamed) +
    /// `Bind` + `Describe`(portal) + `Execute` + `Sync`, fused into a single
    /// flush. `B = Never`.
    ///
    /// This is the dynamic (runtime-untyped) sibling of the compile-checked
    /// [`query_params`](Self::query_params) flagship. Where a consumer supplies
    /// SQL text plus params at run time — with no compile-time row type, so the
    /// result schema (OIDs + names) is only known from the wire — this fuses what
    /// was three round trips (`prepare` = Parse+Describe+Sync, then
    /// `Bind`+`Execute`+`Sync`, then `Close`+`Sync`) into ONE. The in-batch
    /// `Describe`(portal) makes the server surface the `RowDescription` INLINE
    /// (right after `BindComplete`, before the `DataRow`s), so the recovered
    /// schema reaches the sink at the [`Surface::Deliver`] exactly as the separate
    /// `prepare` round trip surfaced it — the materializer above is unchanged.
    ///
    /// No `Close`: the unnamed statement/portal are implicitly discarded at the
    /// next `Parse`(unnamed) / `Bind` (PG §55.2.2), so a following one-shot query
    /// (or a flagship named-statement query) is unaffected — no
    /// duplicate-statement error, no leaked server-side plan. Nothing touches the
    /// per-connection prepared-statement cache (that is the named-statement
    /// flagship's concern).
    ///
    /// A `RowDescription`-less command (a DML / no-RETURNING statement) answers
    /// the `Describe`(portal) with `NoData`; the dispatch handles both, and a
    /// mid-fusion `ErrorResponse` (a `Parse` / `Bind` error) is the recoverable
    /// [`ServerErrored`](CommandStatus::ServerErrored) — the verb drains the owed
    /// `ReadyForQuery` to a clean idle, so the connection survives and the token
    /// rides `Ok`.
    ///
    /// # Errors
    ///
    /// As [`query_prepared`](Self::query_prepared) (`FrameTooLong` covers oversize
    /// SQL or encoded parameters).
    pub async fn query_params_fused<P, S>(
        &mut self,
        live: Live<'b>,
        sql: &str,
        params: &P,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        P: ParamsWriter,
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        stage_fused_params(active, send_buf, sql, params)?;
        let status = drive_to_outcome(active, transport, send_buf, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Issue a simple-query (`'Q'`) with a BREAKABLE sink — the CONSTANT-MEMORY
    /// STREAMING peer of [`query`](Self::query).
    ///
    /// Identical wire to [`query`](Self::query) (both share
    /// [`stage_simple_query`], so the eager and the streaming raw-SQL verb cannot
    /// drift), but the sink may [`Break`](ControlFlow::Break) — carrying a user
    /// payload `B` — to stop the pump early. Rows surface as [`Surface::Row`] one
    /// at a time; nothing is accumulated, so a colossal result streams in bounded
    /// memory.
    ///
    /// Returns the RAW [`Boundary`] the pump reached, inside the [`Outcome`] whose
    /// token rides `Ok` because the connection is ALIVE (see
    /// [`query_params_break`](Self::query_params_break) for the full
    /// Idle/Failed/Stopped contract — this is its collect-all-free dynamic peer,
    /// with NO statement cache to settle). A dirty [`Stopped`](Boundary::Stopped)
    /// (caller break) or [`Failed`](Boundary::Failed) (server error) must be
    /// reclaimed with [`drain`](Self::drain) before reuse.
    ///
    /// # Errors
    ///
    /// - [`EngineError::ProtocolViolation`] / [`EngineError::UnexpectedSuspend`] —
    ///   a teardown or unexpected suspend; the connection is dead, token consumed.
    /// - As [`query`](Self::query) for the wire-building / transport faults.
    pub async fn query_break<S, B>(
        &mut self,
        live: Live<'b>,
        sql: &str,
        sink: S,
    ) -> Result<Outcome<'b, Boundary<B>>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<B>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        stage_simple_query(active, send_buf, sql)?;
        let boundary = pump_active_to_boundary(active, transport, send_buf, sink).await?;
        let status = classify_break_boundary(boundary)?;
        Ok(Outcome { live, status })
    }

    /// Issue a one-shot runtime-SQL query with params and a BREAKABLE sink — the
    /// CONSTANT-MEMORY STREAMING peer of
    /// [`query_params_fused`](Self::query_params_fused).
    ///
    /// Identical wire to [`query_params_fused`](Self::query_params_fused) (both
    /// share [`stage_fused_params`], so the eager and the streaming dynamic-param
    /// verb cannot drift), fused into ONE round trip, but the sink may
    /// [`Break`](ControlFlow::Break) — carrying a user payload `B`. Rows surface as
    /// [`Surface::Row`] one at a time against the recovered schema; nothing is
    /// accumulated. Like the fused collect verb it touches NO statement cache (the
    /// unnamed statement/portal are implicitly discarded at the next `Parse`).
    ///
    /// Returns the RAW [`Boundary`] in the [`Outcome`] exactly as
    /// [`query_break`](Self::query_break) does.
    ///
    /// # Errors
    ///
    /// As [`query_break`](Self::query_break) (`FrameTooLong` covers oversize SQL or
    /// encoded parameters).
    pub async fn query_params_fused_break<P, S, B>(
        &mut self,
        live: Live<'b>,
        sql: &str,
        params: &P,
        sink: S,
    ) -> Result<Outcome<'b, Boundary<B>>, EngineError<T::Error>>
    where
        P: ParamsWriter,
        S: FnMut(Surface<'_>) -> ControlFlow<B>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        stage_fused_params(active, send_buf, sql, params)?;
        let boundary = pump_active_to_boundary(active, transport, send_buf, sink).await?;
        let status = classify_break_boundary(boundary)?;
        Ok(Outcome { live, status })
    }

    /// Run a compile-checked query — the `query!` macro path —
    /// reusing this connection's already-Parsed server-side plan on a repeat.
    ///
    /// On a cache HIT (this connection has RECORDED this content-addressed
    /// statement as durable) this emits only `Bind`+`Execute`+`Sync` — skipping
    /// the `Parse` and reusing the server-side plan. On a cache MISS (first use,
    /// or a name evicted after a reuse error) it emits a `Close`(statement) +
    /// the baked `Parse` template + `Bind` + `Execute` + one `Sync`, as ONE
    /// pipelined batch (one round trip). The leading `Close` makes the re-`Parse`
    /// IDEMPOTENT: a `Close` of a nonexistent statement is a wire no-op, so the
    /// name is (re)created whether or not the server currently holds it — no
    /// duplicate-statement error is possible in any case (first use, repeat, or a
    /// name first Parsed inside a since-committed transaction). Rows surface
    /// against the query's compile-time row OIDs. `B = Never`.
    ///
    /// `q` is the `PreparedQuery` the macro produces from the SQL text — the
    /// project's sole parameterised-query entry point (no runtime SQL builder).
    ///
    /// # Cache soundness
    ///
    /// The MISS path is correct unconditionally (Close-before-Parse re-creates
    /// the statement regardless of prior state), so correctness never depends on
    /// the record rule. The record rule governs only the plan-reuse OPTIMIZATION:
    /// a name is recorded (→ future HIT) ONLY when its command completed and the
    /// connection is back at [`TxStatus::Idle`] — the wrapping transaction
    /// committed, so the statement is durable — so a HIT (which skips `Close` and
    /// `Parse`) can never `Bind` to a statement the server lacks. If a recorded
    /// statement is nonetheless dropped out of band (`DISCARD ALL` /
    /// `DEALLOCATE`), the resulting reuse error is surfaced loudly AND the name is
    /// EVICTED, so the next use is a MISS that re-creates it — a self-heal, never
    /// a silent retry. [`clear_statement_cache`](Self::clear_statement_cache) is
    /// the up-front hook for a known session reset.
    ///
    /// # Errors
    ///
    /// As [`query_prepared`](Self::query_prepared).
    pub async fn query_params<P, R, S>(
        &mut self,
        live: Live<'b>,
        q: &PreparedQuery<P, R>,
        args: P,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        P: ParamsWriter,
        R: RowDecode,
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        let reuse = stage_compiled_query(active, send_buf, q, &args)?;
        let status = drive_to_outcome(active, transport, send_buf, sink).await?;
        settle_statement_cache(
            active,
            q,
            reuse,
            matches!(status, CommandStatus::Completed),
            matches!(status, CommandStatus::ServerErrored),
        );
        Ok(Outcome { live, status })
    }

    /// Run a compile-checked query with a BREAKABLE sink — the CONSTANT-MEMORY
    /// STREAMING peer of [`query_params`](Self::query_params).
    ///
    /// Identical wire and statement-cache logic (both share
    /// [`stage_compiled_query`] / [`settle_statement_cache`], so neither the
    /// Close-before-Parse MISS path nor the plan-reuse HIT path can drift between
    /// the two verbs), but the sink may [`Break`](ControlFlow::Break) — carrying a
    /// user payload `B` — to stop the pump early. Rows surface as
    /// [`Surface::Row`] one at a time against the query's compile-time OIDs;
    /// nothing is accumulated, so a colossal result streams in bounded memory.
    ///
    /// Returns the RAW [`Boundary`] the pump reached, inside the [`Outcome`] whose
    /// token rides `Ok` because the connection is ALIVE (whether at a clean
    /// boundary or dirty):
    ///
    /// - [`Boundary::Idle`] — the result streamed to completion; the connection is
    ///   clean and reusable, and the statement is recorded for plan reuse.
    /// - [`Boundary::Failed`] — a server `ErrorResponse` arrived (its raw bytes
    ///   reached the sink first, which [`Continue`](ControlFlow::Continue)d); the
    ///   connection is DIRTY (the recovering `ReadyForQuery` is still owed) and the
    ///   caller must [`drain`](Self::drain) it before reuse.
    /// - [`Boundary::Stopped`] — the sink [`Break`](ControlFlow::Break)ed early;
    ///   the connection is DIRTY (unread rows + `CommandComplete` +
    ///   `ReadyForQuery` remain on the wire) and the caller must
    ///   [`drain`](Self::drain) it to reclaim it. The statement is NOT recorded
    ///   (the command has not completed), so a later use is a self-correcting MISS.
    ///
    /// [`Boundary::Closed`] / [`Boundary::Suspended`] are FATAL and surface as
    /// `Err` (the token is consumed), so they never ride an `Ok` [`Outcome`] — the
    /// tier-1 rule "token in `Ok` ⟺ connection alive" holds even for a dirty-but-
    /// alive connection.
    ///
    /// # Errors
    ///
    /// - [`EngineError::ProtocolViolation`] / [`EngineError::UnexpectedSuspend`] —
    ///   a teardown or unexpected suspend; the connection is dead, the token is
    ///   consumed.
    /// - As [`query_params`](Self::query_params) for the wire-building / transport
    ///   faults.
    pub async fn query_params_break<P, R, S, B>(
        &mut self,
        live: Live<'b>,
        q: &PreparedQuery<P, R>,
        args: P,
        sink: S,
    ) -> Result<Outcome<'b, Boundary<B>>, EngineError<T::Error>>
    where
        P: ParamsWriter,
        R: RowDecode,
        S: FnMut(Surface<'_>) -> ControlFlow<B>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        let reuse = stage_compiled_query(active, send_buf, q, &args)?;
        let boundary = pump_active_to_boundary(active, transport, send_buf, sink).await?;
        match boundary {
            Boundary::Idle => {
                settle_statement_cache(active, q, reuse, true, false);
                Ok(Outcome {
                    live,
                    status: Boundary::Idle,
                })
            }
            Boundary::Failed => {
                core::hint::cold_path();
                settle_statement_cache(active, q, reuse, false, true);
                Ok(Outcome {
                    live,
                    status: Boundary::Failed,
                })
            }
            // The caller broke early: the connection is alive but DIRTY. The
            // statement cache is left untouched (the command has not completed —
            // recording it would need the not-yet-seen `ReadyForQuery`), so a later
            // use is a self-correcting MISS. The caller drains to reclaim.
            Boundary::Stopped(b) => Ok(Outcome {
                live,
                status: Boundary::Stopped(b),
            }),
            Boundary::Closed => {
                core::hint::cold_path();
                Err(EngineError::ProtocolViolation)
            }
            Boundary::Suspended => {
                core::hint::cold_path();
                Err(EngineError::UnexpectedSuspend)
            }
        }
    }

    /// Reclaim a connection left DIRTY by an early stop of
    /// [`query_params_break`](Self::query_params_break) — a
    /// [`Boundary::Stopped`] (caller break) or [`Boundary::Failed`] (server error)
    /// — by draining its remaining reply frames to a clean idle boundary.
    /// `B = Never`.
    ///
    /// Sends NOTHING: the request (`Bind`+`Execute`+`Sync`, or the MISS's
    /// `Close`+`Parse`+…+`Sync`) was already flushed by the verb that left the
    /// connection dirty, so the trailing `ReadyForQuery` is already owed on the
    /// wire — this only READS the unread rows + `CommandComplete` +
    /// `ReadyForQuery` (or, after a `Failed`, the single recovering
    /// `ReadyForQuery`), draining a recoverable error to a clean idle exactly like
    /// the collect-all verbs.
    ///
    /// The `sink` sees every surface read during the reclaim. The discarded rows
    /// are of no interest, so the caller passes a Continue-only sink — but a
    /// wire-legal asynchronous frame (`NotificationResponse`, `NoticeResponse`,
    /// `ParameterStatus`) can ride the drained remainder, and it MUST still
    /// surface, exactly as it does through every other verb's sink and through
    /// [`drive_to_outcome`]'s own recovery drain: the reclaim reads real wire
    /// bytes, so a notification arriving in that window is not the driver's to
    /// silently drop. `B = Never`, so the sink is Continue-only and cannot change
    /// the drain's boundary.
    ///
    /// Returns the [`CommandStatus`] the drain reached inside an [`Outcome`]; a
    /// caller that only needs the connection back keeps the token and ignores the
    /// status. This makes the drain-to-idle a driver-visible reclaim step, not an
    /// engine-internal side effect.
    ///
    /// This is the O(remaining-rows) reclaim: an early break of a colossal result
    /// still reads (and discards) the remainder to reach the clean boundary. A
    /// true constant-time early abort (a row-limited `Execute` paused at
    /// [`Boundary::Suspended`], then a portal `Close`) is a distinct, deferred
    /// capability — the dispatch already classifies `PortalSuspended`, but no
    /// row-capped verb is surfaced yet.
    ///
    /// # Errors
    ///
    /// As [`ping`](Self::ping): a transport/framing fault during the drain is
    /// FATAL — the connection is dead and the token is consumed, never swallowed.
    pub async fn drain<S>(
        &mut self,
        live: Live<'b>,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        // No `reset`/`enqueue`: the request was already flushed by the verb that
        // left the connection dirty. `drive_to_outcome`'s entry flush drains the
        // (already-empty) send buffer as a no-op, then reads the owed reply frames
        // to a clean idle — threading the caller's sink so an async frame in the
        // drained remainder still surfaces, never a silent drop.
        let status = drive_to_outcome(active, transport, send_buf, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Stage ONE pipelined command's frames onto the send buffer (see
    /// [`stage_pipeline_frames`]). No token, no I/O — a pure send-buffer build the
    /// driver's generic `pipeline` verb calls once per batch element (the FIRST with
    /// `first = true`). Pair with [`stage_pipeline_seal`](Self::stage_pipeline_seal)
    /// after the last command, then [`run_pipeline`](Self::run_pipeline) to drive.
    ///
    /// # Errors
    ///
    /// - [`EngineError::WrongPhase`] — the engine is not in its active phase.
    /// - [`EngineError::FrameTooLong`] — a command's params/SQL exceeded the wire
    ///   length field. The caller must [`abort_pipeline_staging`](Self::abort_pipeline_staging)
    ///   (nothing was flushed, so the connection stays healthy).
    pub fn stage_pipeline_command<P, R>(
        &mut self,
        q: &PreparedQuery<P, R>,
        args: &P,
        first: bool,
    ) -> Result<(), EngineError<T::Error>>
    where
        P: ParamsWriter,
        R: RowDecode,
    {
        let Self {
            phase, send_buf, ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        stage_pipeline_frames(active, send_buf, q, args, first)
    }

    /// Append the batch's SINGLE trailing `Sync` after every command is staged —
    /// the one Sync that makes the whole batch ONE implicit transaction. No token,
    /// no I/O.
    #[inline]
    pub fn stage_pipeline_seal(&mut self) {
        self.send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
    }

    /// Discard a partially-staged pipeline after a staging build error
    /// ([`EngineError::FrameTooLong`] from [`stage_pipeline_command`](Self::stage_pipeline_command)):
    /// reset the send buffer and the engine to a clean `Idle`. Nothing was flushed,
    /// so the connection is healthy; this drops the half-seated state so the next
    /// verb starts fresh. No token (staging never took one).
    #[inline]
    pub fn abort_pipeline_staging(&mut self) {
        if let Phase::Active(active) = &mut self.phase {
            active.abort_pipeline_staging();
        }
        self.send_buf.reset();
    }

    /// Drive a staged pipeline batch to its boundary — the batch analog of
    /// [`run_simple`](Self::run_simple)'s drive.
    ///
    /// Reuses [`drive_to_outcome`], so a mid-batch server error surfaces its raw
    /// bytes through `sink` and then DRAINS the batch's single trailing
    /// `ReadyForQuery` to a clean idle, reported as the recoverable
    /// [`ServerErrored`](CommandStatus::ServerErrored) — the token rides `Ok` and
    /// the connection survives. The ONLY `Ok(Completed)` path is a clean
    /// `ReadyForQuery` after every command committed (the implicit transaction), so
    /// a rolled-back / failing / skipped command can never be reported as completed.
    /// `B = Never`.
    ///
    /// # Errors
    ///
    /// As [`simple_query`](Self::simple_query): a FATAL transport/protocol/EOF fault
    /// consumes the token (the connection is dead).
    pub async fn run_pipeline<S>(
        &mut self,
        live: Live<'b>,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        let status = drive_to_outcome(active, transport, send_buf, sink).await?;
        Ok(Outcome { live, status })
    }

    // ── Homogeneous execute_batch staging + windowed drive ──────────────────

    /// Stage ONE command of a homogeneous `execute_batch` — the PARSE-ONCE peer of
    /// [`stage_pipeline_command`](Self::stage_pipeline_command). `first = true`
    /// stages command 0 exactly as a pipeline's first command (reset + prelude +
    /// MISS/HIT cache decision + `Bind`+`Execute` + seat + `begin_pipeline`);
    /// `first = false` appends a BARE `Bind`+`Execute` reusing the statement command
    /// 0 created — NO re-`Parse`, NO seat (see [`stage_batch_frames`]). No token, no
    /// I/O — a pure send-buffer build the driver's `execute_batch` verb calls once
    /// per parameter set.
    ///
    /// # Errors
    ///
    /// - [`EngineError::WrongPhase`] — the engine is not in its active phase.
    /// - [`EngineError::FrameTooLong`] — a command's params exceeded the wire length
    ///   field. If NOTHING has been flushed yet the caller
    ///   [`abort_pipeline_staging`](Self::abort_pipeline_staging) (the connection is
    ///   healthy, a consumed deferred `BEGIN` preserved); after a window was already
    ///   flushed it is FATAL (the caller drops the connection, rolling back the
    ///   open implicit transaction — all-or-nothing preserved).
    pub fn stage_execute_batch_command<P, R>(
        &mut self,
        q: &PreparedQuery<P, R>,
        args: &P,
        first: bool,
    ) -> Result<(), EngineError<T::Error>>
    where
        P: ParamsWriter,
        R: RowDecode,
    {
        let Self {
            phase, send_buf, ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        stage_batch_frames(active, send_buf, q, args, first)
    }

    /// Append a `Flush` (`'H'`) after a WINDOW of staged `execute_batch` commands —
    /// forces the server to emit the window's buffered responses WITHOUT ending the
    /// implicit transaction (only [`stage_pipeline_seal`](Self::stage_pipeline_seal)'s
    /// `Sync` does that). The deadlock-free peer of the COPY batcher's threshold
    /// flush: unlike COPY (the server is silent while the client streams), an
    /// extended-protocol command emits a per-command response, so a large batch must
    /// DRAIN each window's responses before staging the next — the `Flush` makes the
    /// window's responses available to drain. No token, no I/O (the driver flushes +
    /// drains next).
    #[inline]
    pub fn stage_flush(&mut self) {
        self.send_buf.enqueue(&crate::wire::FLUSH_WIRE_BYTES);
    }

    /// Pending (staged-but-unflushed) send-buffer bytes — the driver's
    /// `execute_batch` reads this after each staged command to decide a WINDOW
    /// boundary (flush + drain when it crosses the batcher threshold), keeping the
    /// send buffer bounded regardless of N (constant memory). No token, no I/O.
    #[inline]
    #[must_use]
    pub fn pending_send_len(&self) -> usize {
        self.send_buf.pending_len()
    }

    /// Drive a staged `execute_batch` WINDOW to its boundary — the BREAKABLE peer of
    /// [`run_pipeline`](Self::run_pipeline). `sink` returns [`ControlFlow::Break`]
    /// once it has counted the window's expected deliveries, so the pump stops at
    /// the inter-command `PipelineAwaitingNextOrRfq` boundary
    /// ([`Boundary::Stopped`]) — the connection is left at a clean resumable point
    /// (the window ended with a `Flush`, so no frames remain buffered) and the
    /// driver stages the next window. A mid-window server error surfaces its raw
    /// bytes through `sink` and returns [`Boundary::Failed`] (no `Sync` was sent, so
    /// the RFQ is NOT drained here — the driver stages the trailing `Sync` and drains
    /// via [`run_pipeline`](Self::run_pipeline)). Mirrors
    /// [`query_params_break`](Self::query_params_break) WITHOUT the staging/settle
    /// (already staged by [`stage_execute_batch_command`](Self::stage_execute_batch_command)).
    ///
    /// # Errors
    ///
    /// - [`EngineError::WrongPhase`] — not in the active phase.
    /// - [`EngineError::ProtocolViolation`] / [`EngineError::UnexpectedSuspend`] — a
    ///   teardown / unexpected suspend; the connection is dead, the token consumed.
    /// - As [`run_pipeline`](Self::run_pipeline) for transport faults.
    pub async fn run_pipeline_break<S, B>(
        &mut self,
        live: Live<'b>,
        sink: S,
    ) -> Result<Outcome<'b, Boundary<B>>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<B>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        let boundary = pump_active_to_boundary(active, transport, send_buf, sink).await?;
        // Alive boundaries (`Idle` / `Failed` / `Stopped`) ride `Ok` with the token;
        // fatal ones (`Closed` / `Suspended`) consume it — the `classify_break_boundary`
        // rule the breakable dynamic verbs use.
        let status = classify_break_boundary(boundary)?;
        Ok(Outcome { live, status })
    }

    /// Close a prepared statement: `Close` + `Sync`. Consumes the
    /// [`PreparedStatement`] by value, so a later use is a move error (the
    /// compile-time half of the use-after-close invariant). `B = Never`.
    ///
    /// # Errors
    ///
    /// As [`simple_query`](Self::simple_query).
    pub async fn close_statement<S>(
        &mut self,
        live: Live<'b>,
        stmt: PreparedStatement,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        stage_prelude(active, send_buf)?;
        enqueue_frame(send_buf, |wb| {
            frames::build_close_statement(wb, stmt.stmt_name().as_bytes())
        })?;
        send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
        active.begin_close();
        let status = drive_to_outcome(active, transport, send_buf, sink).await?;
        // `stmt` is owned here and dropped at scope end — it cannot be reused by
        // the caller (it was moved in). The borrow above ended before the pump.
        Ok(Outcome { live, status })
    }

    /// Close MANY prepared statements in ONE round trip: `Close`+…+`Close` +
    /// a single `Sync`, then drain every `CloseComplete` ack (see
    /// [`begin_close_many`](ActiveEngine::begin_close_many)). This is the batched
    /// peer of [`close_statement`](Self::close_statement) — the pool reset's
    /// dynamic-cache clear closes up to a cacheful of statements without paying
    /// one round trip each. Takes the statement NAMES (not the owned
    /// [`PreparedStatement`]s), so the caller keeps ownership and drops them after
    /// (their server-side statements are already closed by the batch). A `Close`
    /// of an already-dropped statement is a wire no-op, so the batch is robust.
    /// `B = Never`.
    ///
    /// A zero-length `names` still sends a bare `Sync` (a liveness round trip);
    /// the caller should skip the verb when there is nothing to close.
    ///
    /// # Errors
    ///
    /// As [`simple_query`](Self::simple_query) (`FrameTooLong` covers an oversize
    /// statement name).
    pub async fn close_statements<S>(
        &mut self,
        live: Live<'b>,
        names: &[&StmtName],
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        stage_prelude(active, send_buf)?;
        for name in names {
            enqueue_frame(send_buf, |wb| frames::build_close_statement(wb, name.as_bytes()))?;
        }
        send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
        active.begin_close_many();
        let status = drive_to_outcome(active, transport, send_buf, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Open a `COPY … FROM STDIN`: issue the COPY command and flush it, entering
    /// the client-streaming phase. The token-less half of the streaming COPY-in
    /// trio ([`copy_in_write`](Self::copy_in_write) then
    /// [`copy_in_finish`](Self::copy_in_finish) / [`copy_in_abort`](Self::copy_in_abort)).
    ///
    /// Sends only the COPY command; it does NOT read the server's `CopyInResponse`
    /// (`'G'`). That optimistic write-ahead is sound: between `CopyInResponse` and
    /// the post-`CopyDone` `CommandComplete` the server produces nothing, and if
    /// the COPY command itself fails (bad table, parse error) the server sends
    /// `ErrorResponse` + `ReadyForQuery` and then IGNORES the client's stray
    /// `CopyData`/`CopyDone`/`CopyFail` (PG's frontend accepts-but-discards them
    /// out of copy mode), so the write-ahead cannot deadlock and the owed reply
    /// is consumed by whichever of `copy_in_finish` / `copy_in_abort` closes the
    /// stream.
    ///
    /// Takes no [`Live`] token: it does not reach a protocol boundary (the
    /// command spans the whole write-then-finish sequence), so the caller holds
    /// the token across the streaming writes and hands it to
    /// `copy_in_finish` / `copy_in_abort`. A wrong-phase call is a classified
    /// [`WrongPhase`](EngineError::WrongPhase), never a misframe.
    ///
    /// # Errors
    ///
    /// As [`simple_query`](Self::simple_query) (`FrameTooLong` covers an oversize
    /// COPY command; a transport fault while flushing is fatal).
    ///
    /// `sink` is threaded ONLY to the fused-prelude drain (a deferred `BEGIN` when a
    /// COPY is a transaction's FIRST statement): the COPY command's own reply is not
    /// read here (see above), but the prelude's `CommandComplete` + `ReadyForQuery`
    /// are — and a `NOTIFY` / `NOTICE` / `ParameterStatus` riding that reply must
    /// reach the driver's capture ledger like every other first statement, so the
    /// driver passes its real capture sink (never a silent drop). With no prelude
    /// pending the sink is untouched.
    pub async fn copy_in_begin<S>(
        &mut self,
        sql: &str,
        mut sink: S,
    ) -> Result<(), EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        // A fused prelude (a deferred BEGIN when a COPY is a transaction's FIRST
        // statement) needs the `active` handle both to stage its frame ahead of the
        // COPY command and to drain its response BEFORE the client-streaming phase
        // begins — so this verb keeps the handle rather than a bare phase gate.
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        stage_prelude(active, send_buf)?;
        enqueue_frame(send_buf, |wb| frames::build_simple_query(wb, sql.as_bytes()))?;
        super::flush::flush(send_buf, transport).await?;
        // Drain a fused prelude's response (e.g. a deferred BEGIN) NOW, so the COPY
        // stream that follows starts at the COPY command's own reply, not the
        // prelude's `CommandComplete` + `ReadyForQuery`. The caller's `sink` (the
        // driver's capture adapter) surfaces a NOTIFY / NOTICE / ParameterStatus
        // riding the prelude into the ledger — the no-drop guarantee every other
        // first statement gets — while the prelude's own frames are swallowed.
        if active.draining_prelude() {
            core::hint::cold_path();
            super::pump::drain_fused_prelude(active, transport, &mut sink).await?;
        }
        Ok(())
    }

    /// Stream one `CopyData` (`'d'`) frame for an open COPY-in, BATCHING the flush
    /// so the socket sees far fewer writes than there are chunks. Token-less: the
    /// caller holds the [`Live`] token across writes.
    ///
    /// The framed `CopyData` accumulates in the send buffer; the buffer is flushed
    /// to the socket only when its pending bytes cross
    /// [`COPY_IN_FLUSH_THRESHOLD`] (or at `copy_in_finish` / `copy_in_abort`). A
    /// megarow load of small chunks therefore costs about
    /// `total_bytes / THRESHOLD` write syscalls instead of one per chunk — a
    /// 100–1000× reduction — while staying CONSTANT MEMORY: the buffer is bounded
    /// to under `2 * THRESHOLD` (see the const), never O(rows).
    ///
    /// A single chunk at or above the threshold takes the PASSTHROUGH path: any
    /// accumulated frames plus this chunk's header are flushed, then the chunk
    /// body is streamed DIRECTLY from the borrowed slice via
    /// [`write_all`](super::flush::write_all) — it is never copied into the buffer,
    /// so even a gigabyte chunk costs no buffer growth (and no per-chunk memcpy).
    ///
    /// The bytes on the wire are byte-identical to flushing every chunk
    /// separately (a concatenation of the same `CopyData` frames); only the number
    /// of socket writes changes. Multiple `CopyData` frames in one write is valid
    /// PostgreSQL framing.
    ///
    /// # Errors
    ///
    /// [`FrameTooLong`](EngineError::FrameTooLong) if the chunk exceeds a `u32`
    /// body length; a transport fault while flushing is fatal (as
    /// [`simple_query`](Self::simple_query)).
    pub async fn copy_in_write(&mut self, chunk: &[u8]) -> Result<(), EngineError<T::Error>> {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        // Reclaim a DRAINED prefix (the COPY command from `copy_in_begin`, or a
        // prior threshold flush's frames) while PRESERVING any accumulated-but-
        // unflushed frames — `reset` is a no-op when nothing has been sent, so it
        // drops only fully-sent bytes and keeps the batch under way. This is what
        // holds the buffer bounded across the whole stream (the constant-memory
        // invariant); without it the backing store would grow by every drained
        // frame's bytes.
        send_buf.reset();
        let body_len = u32::try_from(chunk.len()).map_err(|_| {
            core::hint::cold_path();
            EngineError::FrameTooLong
        })?;
        // The `CopyData` header rides after any accumulated frames in BOTH paths.
        enqueue_frame(send_buf, |wb| frames::build_copy_data_header(wb, body_len))?;
        if chunk.len() >= COPY_IN_FLUSH_THRESHOLD {
            // Passthrough: never copy a large body into the buffer. Flush the
            // accumulated frames plus this header, then stream the borrowed body
            // straight to the socket.
            super::flush::flush(send_buf, transport).await?;
            super::flush::write_all(transport, chunk).await
        } else {
            // Batch: accumulate the framed `CopyData`; flush only when the buffer
            // crosses the threshold.
            send_buf.enqueue(chunk);
            if send_buf.pending_len() >= COPY_IN_FLUSH_THRESHOLD {
                super::flush::flush(send_buf, transport).await
            } else {
                Ok(())
            }
        }
    }

    /// Stream one PGCOPY BINARY row as a `CopyData` frame, BATCHING the flush
    /// exactly like [`copy_in_write`](Self::copy_in_write). The row body — an
    /// `int16` field-count followed by each field's `{len i32, bytes}` (or `-1`
    /// for a SQL NULL) — is encoded DIRECTLY onto the growable send buffer
    /// through the SAME [`ParamsWriter`] leaves the `query!` parameter path uses,
    /// with NO intermediate per-row scratch buffer (one copy per field). Frame
    /// boundaries are irrelevant to the PGCOPY stream, so each row rides its own
    /// `CopyData` and the whole stream stays byte-correct.
    ///
    /// Token-less: the caller holds the [`Live`] token across the streaming
    /// writes (issued between [`copy_in_begin`](Self::copy_in_begin) and
    /// [`copy_in_finish`](Self::copy_in_finish) / [`copy_in_abort`](Self::copy_in_abort)).
    /// The batching is identical to [`copy_in_write`](Self::copy_in_write): the
    /// framed row accumulates in the send buffer and flushes only when the
    /// pending bytes cross [`COPY_IN_FLUSH_THRESHOLD`] — a megarow load costs far
    /// fewer socket writes than rows while the buffer stays bounded (CONSTANT
    /// memory, never O(rows)).
    ///
    /// # Errors
    ///
    /// [`FrameTooLong`](EngineError::FrameTooLong) if the encoded row body exceeds
    /// a `u32` wire length; a transport fault while flushing is fatal (as
    /// [`copy_in_write`](Self::copy_in_write)).
    pub async fn copy_in_write_binary_row<P: ParamsWriter>(
        &mut self,
        row: &P,
    ) -> Result<(), EngineError<T::Error>> {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        // Reclaim a DRAINED prefix while PRESERVING any accumulated-but-unflushed
        // frames — the constant-memory invariant, exactly as `copy_in_write`.
        send_buf.reset();
        // Encode the whole `CopyData` frame in place onto the growable send
        // buffer (the length prefix is back-patched after the body), reusing the
        // shared `ParamsWriter` field encoders.
        frames::build_copy_binary_row(&mut send_buf.frame(), row).map_err(frame_too_long)?;
        // Flush only when the batch crosses the threshold.
        if send_buf.pending_len() >= COPY_IN_FLUSH_THRESHOLD {
            super::flush::flush(send_buf, transport).await?;
        }
        Ok(())
    }

    /// Close an open COPY-in cleanly: send `CopyDone` (`'c'`) and pump for the
    /// server's `CommandComplete` + `ReadyForQuery`, returning the [`Live`] token
    /// with the [`CommandStatus`] the command reached. `B = Never`.
    ///
    /// The trailing `CommandComplete` carries the `COPY n` tag (surfaced to
    /// `sink` at its `Deliver`), so the caller reads the affected-row count. A
    /// server `ErrorResponse` here — a constraint/type violation the server
    /// detected while ingesting the streamed rows — is the RECOVERABLE
    /// [`ServerErrored`](CommandStatus::ServerErrored): the verb drains the owed
    /// `ReadyForQuery` to a clean idle before returning the token, exactly like
    /// the collect-all command verbs.
    ///
    /// # Errors
    ///
    /// As [`simple_query`](Self::simple_query).
    pub async fn copy_in_finish<S>(
        &mut self,
        live: Live<'b>,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        // Flush the accumulated tail before `CopyDone` — no data left unsent.
        // `reset` reclaims only a fully-drained prefix (from the last threshold
        // flush) and PRESERVES any accumulated-but-unflushed `CopyData` (no-op
        // when nothing has been sent); `CopyDone` is appended after it, and
        // `drive_to_outcome`'s entry flush sends the accumulated frames followed
        // by `CopyDone` before reading the acks.
        send_buf.reset();
        send_buf.enqueue(&frames::COPY_DONE_WIRE);
        let status = drive_to_outcome(active, transport, send_buf, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Abort an open COPY-in: send `CopyFail` (`'f'`) carrying `reason`, then pump
    /// to reclaim the connection to a clean idle, returning the [`Live`] token.
    /// `B = Never`.
    ///
    /// The server responds to `CopyFail` with an `ErrorResponse` echoing `reason`
    /// and then a recovering `ReadyForQuery`; the verb drains that to
    /// [`ServerErrored`](CommandStatus::ServerErrored) — which for an abort is the
    /// EXPECTED outcome (the caller chose to fail the COPY), not a fault. The
    /// connection is alive and reusable, so the token rides `Ok`. This is the
    /// cancellation-recovery path: a client that abandons a COPY-in mid-stream
    /// calls this so the server tears down the COPY and the connection is
    /// immediately reusable rather than stranded in copy mode.
    ///
    /// # Errors
    ///
    /// As [`simple_query`](Self::simple_query) (`FrameTooLong` covers an oversize
    /// reason; a transport fault is fatal).
    pub async fn copy_in_abort<S>(
        &mut self,
        live: Live<'b>,
        reason: &[u8],
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        // DISCARD any accumulated-but-unflushed `CopyData`: a `CopyFail` aborts the
        // whole COPY, so buffered rows are moot — the server would only discard
        // them, and sending them risks it erroring on a stale row instead of
        // echoing the caller's abort reason. `clear` drops them (and any drained
        // prefix), then `CopyFail` is the only frame the entry flush sends.
        send_buf.clear();
        enqueue_frame(send_buf, |wb| frames::build_copy_fail(wb, reason))?;
        let status = drive_to_outcome(active, transport, send_buf, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Run a `COPY … TO STDOUT` with a BREAKABLE sink — the CONSTANT-MEMORY
    /// STREAMING reader. Each server `CopyData` (`'d'`) frame surfaces to `sink`
    /// as [`Surface::CopyData`] one at a time; nothing is accumulated, so a
    /// colossal unload streams in bounded memory. The trailing `CopyDone` (`'c'`)
    /// surfaces as [`Surface::CopyDone`] and the closing `CommandComplete` as
    /// [`Surface::Deliver`] (`COPY n`).
    ///
    /// Returns the RAW [`Boundary`] the pump reached inside the [`Outcome`], whose
    /// token rides `Ok` because the connection is ALIVE (clean or dirty),
    /// mirroring [`query_params_break`](Self::query_params_break):
    ///
    /// - [`Boundary::Idle`] — the unload streamed to completion; clean + reusable.
    /// - [`Boundary::Failed`] — a server `ErrorResponse` arrived (its bytes reached
    ///   the sink first); the connection is DIRTY and the caller must
    ///   [`drain`](Self::drain) it.
    /// - [`Boundary::Stopped`] — the sink [`Break`](ControlFlow::Break)ed early; the
    ///   connection is DIRTY (unread `CopyData` + `CopyDone` + acks remain) and the
    ///   caller must [`drain`](Self::drain) it.
    ///
    /// [`Boundary::Closed`] / [`Boundary::Suspended`] are FATAL and surface as `Err`.
    ///
    /// # Errors
    ///
    /// [`EngineError::ProtocolViolation`] / [`EngineError::UnexpectedSuspend`] for a
    /// teardown / unexpected suspend; otherwise as
    /// [`simple_query`](Self::simple_query) for the wire-building / transport faults.
    pub async fn copy_out<S, B>(
        &mut self,
        live: Live<'b>,
        sql: &str,
        sink: S,
    ) -> Result<Outcome<'b, Boundary<B>>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<B>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        stage_prelude(active, send_buf)?;
        enqueue_frame(send_buf, |wb| frames::build_simple_query(wb, sql.as_bytes()))?;
        let boundary = pump_active_to_boundary(active, transport, send_buf, sink).await?;
        match boundary {
            Boundary::Idle => Ok(Outcome {
                live,
                status: Boundary::Idle,
            }),
            Boundary::Failed => {
                core::hint::cold_path();
                Ok(Outcome {
                    live,
                    status: Boundary::Failed,
                })
            }
            Boundary::Stopped(b) => Ok(Outcome {
                live,
                status: Boundary::Stopped(b),
            }),
            Boundary::Closed => {
                core::hint::cold_path();
                Err(EngineError::ProtocolViolation)
            }
            Boundary::Suspended => {
                core::hint::cold_path();
                Err(EngineError::UnexpectedSuspend)
            }
        }
    }

    /// Receive the next asynchronous notification: a single pull that breaks on
    /// the first [`Surface::Notify`]. Issues no request; the deadline/retry loop
    /// stays driver-side. The caller's sink stops the pump on a notification
    /// (`B = ()`).
    ///
    /// Returns an [`Outcome`] (the token rides `Ok`) with a [`NotifyStatus`]:
    /// [`Received`](NotifyStatus::Received) when the pull stopped on a
    /// notification (its payload reached the sink), or
    /// [`Quiet`](NotifyStatus::Quiet) when the pull reached a clean boundary or
    /// the read TIMED OUT before any notification — a *would-block* read
    /// deadline. The deadline is detected via
    /// [`Transport::is_would_block`](super::Transport::is_would_block) (the
    /// `#![no_std]` core cannot inspect the opaque transport error itself) and is
    /// NOT a failure: the connection consumed nothing within the deadline, so it
    /// stays at its prior clean boundary and the token rides back — no separate
    /// token-minting recovery is needed.
    ///
    /// # Errors
    ///
    /// - [`EngineError::ServerError`] — a server `ErrorResponse` arrived while
    ///   waiting. This is FATAL here (unlike a command verb's recoverable error):
    ///   `recv_notification` issues no command, so no recovering `ReadyForQuery`
    ///   is owed to drain — the connection's command/response correlation is
    ///   broken, so the token is consumed.
    /// - [`EngineError::ProtocolViolation`] / [`EngineError::UnexpectedSuspend`]
    ///   — a teardown or unexpected suspend was observed.
    /// - the pump's transport / framing errors (other than a would-block read).
    pub async fn recv_notification<S>(
        &mut self,
        live: Live<'b>,
        sink: S,
    ) -> Result<Outcome<'b, NotifyStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<()>,
    {
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        let boundary = match pump_active_to_boundary(active, transport, send_buf, sink).await
        {
            Ok(boundary) => boundary,
            Err(e) => {
                // A would-block / timed-out READ is a quiet deadline, not a
                // failure: the deadline elapsed mid-read, so the connection stays
                // alive with its ingest state preserved — either a clean boundary
                // (nothing arrived) or a partial frame that the next read resumes
                // — and the token rides back. Any other transport/framing error
                // is fatal.
                if let EngineError::Transport(inner) = &e
                    && T::is_would_block(inner)
                {
                    return Ok(Outcome {
                        live,
                        status: NotifyStatus::Quiet,
                    });
                }
                return Err(e);
            }
        };
        match boundary {
            // A notification stopped the pull (its payload reached the sink).
            Boundary::Stopped(()) => Ok(Outcome {
                live,
                status: NotifyStatus::Received,
            }),
            // A clean boundary with no notification yet — reusable and quiet.
            Boundary::Idle => Ok(Outcome {
                live,
                status: NotifyStatus::Quiet,
            }),
            Boundary::Failed => {
                core::hint::cold_path();
                Err(EngineError::ServerError)
            }
            Boundary::Closed => {
                core::hint::cold_path();
                Err(EngineError::ProtocolViolation)
            }
            Boundary::Suspended => {
                core::hint::cold_path();
                Err(EngineError::UnexpectedSuspend)
            }
        }
    }

    /// Run `body` inside a transaction: `BEGIN`, the body, then `COMMIT`,
    /// threading the linear token through each. Synchronous (single-poll): with
    /// no suspension point between `BEGIN`, the body, and `COMMIT`, a cancellation
    /// cannot strand an open transaction on the server.
    ///
    /// The body returns `(R, Live)` on success (the token threads to `COMMIT`).
    /// If a verb inside the body fails, that verb consumes the token (no clean
    /// connection survives a failed command), so the body propagates the error
    /// and `transaction` returns it without a token: the connection is dropped
    /// and the server rolls the transaction back when the socket closes. To abort
    /// deliberately while keeping the connection, the caller commits a no-op or
    /// issues `ROLLBACK` as its own verb.
    ///
    /// Note the recoverable-error nuance: a verb returns its token in
    /// `Ok(Outcome { status: ServerErrored })` on a recoverable server error, but
    /// [`flatten_poll`] maps that to `Err(ServerError)` for `transaction`'s
    /// bare-token threading — so a recoverable error inside `BEGIN`/`COMMIT`/the
    /// body still aborts and DROPS the token here. This proto `transaction`
    /// therefore diverges from the sync DRIVER's `transaction`, which on a body
    /// error issues a best-effort `ROLLBACK` and KEEPS the connection pooled (its
    /// outcome rides the driver's health bit). The driver shape is the deliberate
    /// one for pooled connections; this proto form is the cancellation-safe
    /// single-poll primitive. The async transaction-*guard* (Drop → `ROLLBACK`)
    /// lands in a later slice.
    ///
    /// Sync-only in THIS form (a constraint of the closure-based shape, not a
    /// permanent impossibility). A closure-based async form would have to hold
    /// `&mut self` across the body's `await` then re-borrow `&mut self` for
    /// `COMMIT` (an overlapping-borrow / self-referential-future tangle), and a
    /// drop of that future mid-body would run neither `COMMIT` nor any teardown,
    /// leaking the open transaction. The standard async alternative for a later
    /// slice is a transaction *guard* whose `Drop` issues `ROLLBACK` (so a
    /// cancellation cannot strand an open transaction), not a body closure; the
    /// single-poll form sidesteps both hazards by having no suspension point.
    ///
    /// # Errors
    ///
    /// - the body's own error (the token is already consumed; the connection is
    ///   dropped), or
    /// - an [`EngineError`] from `BEGIN`/`COMMIT`, or
    /// - [`EngineError::SpuriousPending`] if the transport was not blocking.
    pub fn transaction<R>(
        &mut self,
        live: Live<'b>,
        body: impl FnOnce(&mut Self, Live<'b>) -> Result<(R, Live<'b>), EngineError<T::Error>>,
    ) -> Result<(R, Live<'b>), EngineError<T::Error>> {
        let live = flatten_poll(poll_once(self.simple_query(live, "BEGIN", noop_sink)))?;
        let (value, live) = body(self, live)?;
        let live = flatten_poll(poll_once(self.simple_query(live, "COMMIT", noop_sink)))?;
        Ok((value, live))
    }

    /// Gracefully close the session: push the PostgreSQL `Terminate` frame
    /// (`'X'`, the 5-byte tag-only frame), drain it to the wire, then shut the
    /// transport's write side down — the orderly close a driver issues on
    /// `close()` / `Drop`.
    ///
    /// Consumes the linear [`Live`](super::Live) token and returns NO token: the
    /// session is over, so there is no reusable connection. A verb after
    /// `terminate` is therefore a move error (no token to thread) AND — because
    /// the engine transitions to its closed phase — a classified
    /// [`EngineError::WrongPhase`] on any path that does not need the token (the
    /// [`backend_pid`](Self::backend_pid) / [`tx_status`](Self::tx_status)
    /// accessors). The two protections are independent: neither a stale token nor
    /// a `&mut self` reborrow can re-drive a closed connection.
    ///
    /// Unlike the request-issuing verbs this drives no pump and takes no sink: the
    /// `Terminate` frame elicits no server reply (the server closes the socket),
    /// so there is nothing to read. It flushes the frame and shuts down, nothing
    /// more.
    ///
    /// # Errors
    ///
    /// - [`EngineError::WrongPhase`] — `terminate` was called when the engine was
    ///   not active (already closed, still connecting, or mid-transition).
    /// - [`EngineError::Transport`] — the transport reported a write/flush failure
    ///   while draining the frame, or a failure shutting the write side down.
    /// - [`EngineError::WriteZero`] / [`EngineError::SendOverrun`] — from the
    ///   flush drain (see [`flush`](super::flush)).
    pub async fn terminate(&mut self, live: Live<'b>) -> Result<(), EngineError<T::Error>> {
        // The disjoint split-borrow the crate's verbs use. `phase` is borrowed
        // only for the wrong-phase classification (the reborrow ends immediately),
        // the flush/shutdown awaits borrow ONLY `transport` + `send_buf`, and the
        // closed-phase write in the synchronous tail then writes through the still-
        // live, disjoint `&mut phase` binding — no reborrow of `self` (it would
        // alias the field borrows held across the awaits) and no `&mut self`
        // helper (which would alias the whole engine, E0499). The token `live` is
        // a ZST moved in up front.
        let Self {
            transport,
            phase,
            send_buf,
            ..
        } = self;
        // Classify a non-active terminate before touching the wire. The reborrow
        // of `*phase` ends here (NLL), so the closed-phase write below cannot
        // alias it.
        phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        // The prior command drained at its idle boundary, so `reset` empties the
        // buffer while retaining the allocation. Queue the static `Terminate`
        // literal (the sole wire authority for this parameterless frame) and drain
        // it to the socket.
        send_buf.reset();
        send_buf.enqueue(&crate::wire::TERMINATE_WIRE_BYTES);
        // Past the active check the connection is dead REGARDLESS of a transport
        // error: the token is already committed to this close. So the flush and
        // (best-effort) shutdown are attempted without an early `?`, then
        // `Phase::Closed` is recorded UNCONDITIONALLY, and only THEN is the first
        // failure propagated. This makes the closed phase a TOTAL post-active
        // invariant — a failed `terminate` can never leave the engine `Active`
        // with stale accessors returning `Ok`.
        let flush_res = super::flush::flush(send_buf, transport).await;
        // Orderly write-side teardown (TLS `close_notify` / socket FIN), attempted
        // even if the flush errored — the socket is going away either way. A
        // transport failure here is classified, never swallowed.
        let shutdown_res = transport.shutdown().await.map_err(EngineError::Transport);
        // Synchronous tail: the connection is dead. Record the closed phase so a
        // post-close accessor classifies `WrongPhase`. `phase` is disjoint from
        // the (now-released) `transport` / `send_buf` borrows, so this write is
        // valid here.
        *phase = Phase::Closed;
        // The linear token is consumed, not returned — a verb after `terminate`
        // is a move error. `Live` is a ZST, so this is purely type-level.
        let _ = live;
        // Propagate the first failure (flush before shutdown); the engine is
        // already closed, so the error reports the cause without reviving it.
        flush_res?;
        shutdown_res?;
        Ok(())
    }
}

/// A Continue-only sink for the verbs that surface nothing of interest
/// (`BEGIN`/`COMMIT`/`ROLLBACK`). `B = Never`, so it cannot stop the pump.
#[inline]
fn noop_sink(_surface: Surface<'_>) -> ControlFlow<Never> {
    ControlFlow::Continue(())
}
