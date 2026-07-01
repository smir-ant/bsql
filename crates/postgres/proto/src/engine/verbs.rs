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
//! Every I/O verb destructures `&mut self` into its four fields
//! (`transport` / `obs` / `phase` / `send_buf`) so the pump can drive the active
//! engine over the transport while the other fields are borrowed disjointly.
//! Routing through a `self.active_mut()` helper would alias the whole engine
//! (E0499); the field-level destructure is the only shape that compiles, and it
//! is also what keeps the observer hook (`obs`) a borrow disjoint from the phase
//! it observes.
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
use super::seams::{absurd, CommandStatus, Live, Never, NotifyStatus, Observer, Outcome, Transport};
use super::{ActiveEngine, Engine, Phase, SendBuf};
use crate::action::TxStatus;
use crate::ident::{Ident, StmtName};
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
async fn drive_to_outcome<T, O, S>(
    active: &mut ActiveEngine,
    transport: &mut T,
    send_buf: &mut SendBuf,
    obs: &O,
    mut sink: S,
) -> Result<CommandStatus, EngineError<T::Error>>
where
    T: Transport,
    O: Observer,
    S: FnMut(Surface<'_>) -> ControlFlow<Never>,
{
    // Pass the caller's sink by `&mut` so it survives the first pump and can be
    // reused for the recovery drain below. `&mut S` is itself `FnMut`, so this is
    // the identical sink type at `B = Never`, not a fresh one.
    let boundary = pump_active_to_boundary(active, transport, send_buf, obs, &mut sink).await?;
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
                pump_active_to_boundary(active, transport, send_buf, obs, &mut sink).await?;
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
    if !reuse {
        enqueue_frame(send_buf, |wb| {
            frames::build_close_statement(wb, q.stmt_name.as_bytes())
        })?;
        send_buf.enqueue(q.parse_template);
    }
    enqueue_frame(send_buf, |wb| {
        frames::build_bind_prepared(wb, q.bind_execute_prefix, args)
    })?;
    send_buf.enqueue(&crate::prepared::EXECUTE_EMPTY_PORTAL_NO_LIMIT);
    send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
    if reuse {
        active.begin_bind_execute(q.row_oids);
    } else {
        active.begin_close_parse_bind_execute(q.row_oids);
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
    } else if completed_at_idle && matches!(active.tx_status(), TxStatus::Idle) {
        active.record_statement_parsed(q.stmt_name);
    }
}

impl<'b, T: Transport, O: Observer> Engine<'b, T, O> {
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
            obs,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
        let status = drive_to_outcome(active, transport, send_buf, &*obs, sink).await?;
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
            obs,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        // Stream the SQL body directly onto the (growable) send buffer rather
        // than copying it into the bounded `WriteBuf`: only the 5-byte header is
        // built into the scratch buffer, so a multi-kilobyte query (a large
        // literal INSERT, a wide column projection) is not capped at
        // `MAX_OWNED_SEND_LEN`. The flushed bytes — header, SQL, NUL — are
        // contiguous on the send buffer, byte-identical to the whole-frame
        // builder's output.
        let sql_bytes = sql.as_bytes();
        let sql_len = u32::try_from(sql_bytes.len()).map_err(|_| {
            core::hint::cold_path();
            EngineError::FrameTooLong
        })?;
        enqueue_frame(send_buf, |wb| frames::build_simple_query_header(wb, sql_len))?;
        send_buf.enqueue(sql_bytes);
        send_buf.enqueue(&[0]);
        drive_to_outcome(active, transport, send_buf, &*obs, sink).await
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
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            obs,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        // Stream the SQL body onto the (growable) send buffer rather than copying
        // it into the bounded `WriteBuf`: only the Parse header (tag + length +
        // statement name) is built into the scratch buffer, so a multi-kilobyte
        // prepared SQL is not capped at `MAX_OWNED_SEND_LEN`. The flushed bytes —
        // header, SQL, NUL + zero-param-types trailer — are contiguous and
        // byte-identical to the whole-frame builder (proven by the parse-stream
        // byte-twin).
        let sql_bytes = sql.as_bytes();
        let sql_len = u32::try_from(sql_bytes.len()).map_err(|_| {
            core::hint::cold_path();
            EngineError::FrameTooLong
        })?;
        enqueue_frame(send_buf, |wb| {
            frames::build_parse_header(wb, stmt_name.as_bytes(), sql_len)
        })?;
        send_buf.enqueue(sql_bytes);
        send_buf.enqueue(&frames::PARSE_SQL_TRAILER);
        enqueue_frame(send_buf, |wb| {
            frames::build_describe_statement(wb, stmt_name.as_bytes())
        })?;
        send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
        active.begin_prepare();
        let status = drive_to_outcome(active, transport, send_buf, &*obs, sink).await?;
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
        params: P,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        P: ParamsWriter,
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let status = self.run_bind_execute(stmt, &params, sink).await?;
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
        params: P,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        P: ParamsWriter,
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let status = self.run_bind_execute(stmt, &params, sink).await?;
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
            obs,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        // Unnamed portal; the named statement was parsed by `prepare`.
        enqueue_frame(send_buf, |wb| {
            frames::build_bind(wb, b"", stmt.stmt_name().as_bytes(), params)
        })?;
        enqueue_frame(send_buf, |wb| frames::build_execute(wb, b"", 0))?;
        send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
        active.begin_bind_execute(stmt.result_oids());
        drive_to_outcome(active, transport, send_buf, &*obs, sink).await
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
            obs,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        let reuse = stage_compiled_query(active, send_buf, q, &args)?;
        let status = drive_to_outcome(active, transport, send_buf, &*obs, sink).await?;
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
            obs,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        let reuse = stage_compiled_query(active, send_buf, q, &args)?;
        let boundary = pump_active_to_boundary(active, transport, send_buf, &*obs, sink).await?;
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
    /// `ReadyForQuery`) with a noop sink, draining a recoverable error to a clean
    /// idle exactly like the collect-all verbs.
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
    pub async fn drain(
        &mut self,
        live: Live<'b>,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>> {
        let Self {
            transport,
            obs,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        // No `reset`/`enqueue`: the request was already flushed by the verb that
        // left the connection dirty. `drive_to_outcome`'s entry flush drains the
        // (already-empty) send buffer as a no-op, then reads the owed reply frames
        // to a clean idle.
        let status = drive_to_outcome(active, transport, send_buf, &*obs, noop_sink).await?;
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
            obs,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        enqueue_frame(send_buf, |wb| {
            frames::build_close_statement(wb, stmt.stmt_name().as_bytes())
        })?;
        send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
        active.begin_close();
        let status = drive_to_outcome(active, transport, send_buf, &*obs, sink).await?;
        // `stmt` is owned here and dropped at scope end — it cannot be reused by
        // the caller (it was moved in). The borrow above ended before the pump.
        Ok(Outcome { live, status })
    }

    /// `COPY <table> FROM STDIN`: issue the COPY command, stream client
    /// `CopyData` frames (each chunk yielded by `data`, flushed as produced to
    /// bound the send buffer), then `CopyDone`, and pump for the server acks.
    /// `B = Never`.
    ///
    /// `data` yields successive chunks borrowing a caller-owned store; each is
    /// framed (the header into the bounded scratch buffer, the body queued
    /// directly so an oversize chunk needs no scratch room) and flushed. The
    /// server's `CopyInResponse` is consumed silently; the trailing
    /// `CommandComplete` + `ReadyForQuery` close the command.
    ///
    /// Constraint: each chunk is flushed as produced, so the send buffer stays
    /// bounded (no whole-COPY buffering); a batched-flush variant (coalesce N
    /// chunks per write) is a future throughput option, not a correctness one.
    /// Assumption: all client `CopyData` is written before any server reply is
    /// read. This is sound for `COPY … FROM STDIN` — the server produces nothing
    /// between `CopyInResponse` and the post-`CopyDone` `CommandComplete`, so the
    /// optimistic write-ahead cannot deadlock; it would NOT hold for a
    /// request/response interleaving (none exists on the COPY-in path).
    ///
    /// # Errors
    ///
    /// As [`simple_query`](Self::simple_query) (`FrameTooLong` covers an oversize
    /// COPY command).
    pub async fn copy_in<'d, S>(
        &mut self,
        live: Live<'b>,
        sql: &str,
        mut data: impl FnMut() -> Option<&'d [u8]>,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            obs,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        enqueue_frame(send_buf, |wb| frames::build_simple_query(wb, sql.as_bytes()))?;
        super::flush::flush(send_buf, transport).await?;
        while let Some(chunk) = data() {
            let body_len = u32::try_from(chunk.len()).map_err(|_| {
                core::hint::cold_path();
                EngineError::FrameTooLong
            })?;
            enqueue_frame(send_buf, |wb| frames::build_copy_data_header(wb, body_len))?;
            send_buf.enqueue(chunk);
            super::flush::flush(send_buf, transport).await?;
        }
        send_buf.enqueue(&frames::COPY_DONE_WIRE);
        let status = drive_to_outcome(active, transport, send_buf, &*obs, sink).await?;
        Ok(Outcome { live, status })
    }

    /// Subscribe to a notification channel: `LISTEN <channel>` (`'Q'`). The
    /// channel is a validated [`Ident`], so the name cannot inject SQL.
    /// `B = Never`.
    ///
    /// # Errors
    ///
    /// As [`simple_query`](Self::simple_query).
    pub async fn listen<S>(
        &mut self,
        live: Live<'b>,
        channel: &Ident,
        sink: S,
    ) -> Result<Outcome<'b, CommandStatus>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let Self {
            transport,
            obs,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        send_buf.reset();
        enqueue_frame(send_buf, |wb| frames::build_listen(wb, channel.as_bytes()))?;
        let status = drive_to_outcome(active, transport, send_buf, &*obs, sink).await?;
        Ok(Outcome { live, status })
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
            obs,
            phase,
            send_buf,
            ..
        } = self;
        let active = phase.as_active_mut().map_err(EngineError::WrongPhase)?;
        let boundary = match pump_active_to_boundary(active, transport, send_buf, &*obs, sink).await
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
