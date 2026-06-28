//! The active-phase verb surface.
//!
//! Each verb has the same shape: `&mut self` plus the linear [`Live`] token in,
//! `Result<Live, EngineError>` out (the error path consumes the token — a failed
//! command yields no reusable connection). Results are surfaced through a
//! [`Surface`] sink, not returned: the sans-I/O core is `no_std` and cannot name
//! a typed `Row`, so it lends RAW wire bytes and the typed layer above
//! (`query!` decode / `QueryResult`) owns the typing.
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
//!
//! # Schema surfacing (cutover composition)
//!
//! A statement's recovered schema (column type OIDs + names) is surfaced in
//! [`Surface::Deliver`] at the `CommandComplete` boundary — i.e. AFTER the rows.
//! So a RUNTIME-untyped consumer (one without a compile-time row type) buffers
//! the raw [`Surface::Row`] payloads as they arrive and decodes them at
//! `Deliver`, once the OIDs are known. The compile-time `query!`/`prepared!`
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
//! # Deferred: graceful close (`terminate`)
//!
//! No terminate/close verb is surfaced here yet. The PostgreSQL graceful close
//! is a `Terminate` frame (`'X'`, a 5-byte tag-only frame `[b'X', 0, 0, 0, 4]`)
//! sent to the server, then a transport-level shutdown; the engine currently
//! offers only [`Transport::shutdown`](super::Transport::shutdown) (the
//! transport-level close, no `Terminate` on the wire). To replicate a graceful
//! close, a terminate verb must push the `Terminate` frame, flush it, call
//! `Transport::shutdown`, and consume the [`Live`](super::Live) token (ideally
//! into a `Closed` phase so the connection cannot be re-driven). It is a deferred
//! verb, not a framing gap.

use alloc::vec::Vec;
use core::ops::ControlFlow;

use super::error::{EngineError, ExpectedRowCount, RowCountViolation};
use super::frames;
use super::pump::{poll_once, pump_active_to_boundary, Boundary, SpuriousPending, Surface};
use super::seams::{absurd, Live, Never, Observer, Transport};
use super::{Engine, SendBuf};
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

/// Map a *collect-all* pump boundary to the verb's idle result.
///
/// `Boundary` is `#[non_exhaustive]`, but this is a within-crate match, so every
/// arm is enumerated with no wildcard — a future boundary forces a decision here.
/// At `B = Never` the [`Stopped`](Boundary::Stopped) value is uninhabited and is
/// discharged by [`absurd`], never a `unreachable!()`.
#[inline]
fn classify_idle<E>(boundary: Boundary<Never>) -> Result<(), EngineError<E>> {
    match boundary {
        Boundary::Idle => Ok(()),
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

/// Flatten a single-poll result into the verb error surface.
#[inline]
fn flatten_poll<'b, E>(
    polled: Result<Result<Live<'b>, EngineError<E>>, SpuriousPending>,
) -> Result<Live<'b>, EngineError<E>> {
    match polled {
        Ok(inner) => inner,
        Err(SpuriousPending) => {
            core::hint::cold_path();
            Err(EngineError::SpuriousPending)
        }
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
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
        let boundary = pump_active_to_boundary(active, transport, send_buf, &*obs, sink).await?;
        classify_idle(boundary)?;
        Ok(live)
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        self.run_simple(sql, sink).await?;
        Ok(live)
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        self.run_simple(sql, sink).await?;
        Ok(live)
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let mut rows = 0usize;
        self.run_simple(sql, |surface| {
            if matches!(surface, Surface::Row(_)) {
                rows = rows.saturating_add(1);
            }
            sink(surface)
        })
        .await?;
        if rows == 1 {
            Ok(live)
        } else {
            core::hint::cold_path();
            Err(EngineError::RowCount(RowCountViolation {
                expected: ExpectedRowCount::ExactlyOne,
                got: rows,
            }))
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        let mut rows = 0usize;
        self.run_simple(sql, |surface| {
            if matches!(surface, Surface::Row(_)) {
                rows = rows.saturating_add(1);
            }
            sink(surface)
        })
        .await?;
        if rows <= 1 {
            Ok(live)
        } else {
            core::hint::cold_path();
            Err(EngineError::RowCount(RowCountViolation {
                expected: ExpectedRowCount::AtMostOne,
                got: rows,
            }))
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
    where
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        self.run_simple(sql, sink).await?;
        Ok(live)
    }

    /// Shared simple-query (`'Q'`) drive: compact, build, pump to a clean idle.
    async fn run_simple<S>(&mut self, sql: &str, sink: S) -> Result<(), EngineError<T::Error>>
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
        let boundary = pump_active_to_boundary(active, transport, send_buf, &*obs, sink).await?;
        classify_idle(boundary)
    }

    /// Prepare a statement: `Parse` + statement `Describe` + a single `Sync`. The
    /// recovered schema (column type OIDs + names) is surfaced via
    /// [`Surface::Deliver`] so the caller forms a [`PreparedStatement`] from the
    /// passed `stmt_name` and the OIDs. `B = Never`.
    ///
    /// The single-`Sync` bundling recovers the parameter and row descriptions in
    /// one round trip; the `prepared!`/`query!` macro path recovers its schema at
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
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
            frames::build_parse(wb, stmt_name.as_bytes(), sql.as_bytes())
        })?;
        enqueue_frame(send_buf, |wb| {
            frames::build_describe_statement(wb, stmt_name.as_bytes())
        })?;
        send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
        active.begin_prepare();
        let boundary = pump_active_to_boundary(active, transport, send_buf, &*obs, sink).await?;
        classify_idle(boundary)?;
        Ok(live)
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
    where
        P: ParamsWriter,
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        self.run_bind_execute(stmt, &params, sink).await?;
        Ok(live)
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
    where
        P: ParamsWriter,
        S: FnMut(Surface<'_>) -> ControlFlow<Never>,
    {
        self.run_bind_execute(stmt, &params, sink).await?;
        Ok(live)
    }

    /// Shared `Bind` + `Execute` + `Sync` drive over a named prepared statement.
    async fn run_bind_execute<P, S>(
        &mut self,
        stmt: &PreparedStatement,
        params: &P,
        sink: S,
    ) -> Result<(), EngineError<T::Error>>
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
        let boundary = pump_active_to_boundary(active, transport, send_buf, &*obs, sink).await?;
        classify_idle(boundary)
    }

    /// Parse, bind, execute and sync a compile-checked query in one round trip:
    /// the `prepared!`/`query!` macro path. Emits the baked `Parse` template, a
    /// `Bind` built from the argument tuple's [`ParamsWriter`] (binary params and
    /// binary result columns), the macro's `Execute`, and one `Sync` — byte-
    /// identical to the crate's macro-execute push. Rows surface against the
    /// query's compile-time row OIDs. `B = Never`.
    ///
    /// `q` is the `PreparedQuery` the macro produces from the SQL text — the
    /// project's sole parameterised-query entry point (no runtime SQL builder).
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
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
        send_buf.reset();
        // The macro bakes the complete Parse frame (named, content-addressed,
        // with param OIDs) and the Bind prefix; reuse them verbatim for byte
        // identity with the crate's macro-execute path.
        send_buf.enqueue(q.parse_template);
        enqueue_frame(send_buf, |wb| {
            frames::build_bind_prepared(wb, q.bind_execute_prefix, &args)
        })?;
        send_buf.enqueue(&crate::prepared::EXECUTE_EMPTY_PORTAL_NO_LIMIT);
        send_buf.enqueue(&crate::wire::SYNC_WIRE_BYTES);
        active.begin_parse_bind_execute(q.row_oids);
        let boundary = pump_active_to_boundary(active, transport, send_buf, &*obs, sink).await?;
        classify_idle(boundary)?;
        Ok(live)
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
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
        let boundary = pump_active_to_boundary(active, transport, send_buf, &*obs, sink).await?;
        classify_idle(boundary)?;
        // `stmt` is owned here and dropped at scope end — it cannot be reused by
        // the caller (it was moved in). The borrow above ended before the pump.
        Ok(live)
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
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
        let boundary = pump_active_to_boundary(active, transport, send_buf, &*obs, sink).await?;
        classify_idle(boundary)?;
        Ok(live)
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
    ) -> Result<Live<'b>, EngineError<T::Error>>
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
        let boundary = pump_active_to_boundary(active, transport, send_buf, &*obs, sink).await?;
        classify_idle(boundary)?;
        Ok(live)
    }

    /// Receive the next asynchronous notification: a single pull that breaks on
    /// the first [`Surface::Notify`]. Issues no request; the deadline/retry loop
    /// stays driver-side. The caller's sink stops the pump on a notification
    /// (`B = ()`).
    ///
    /// # Errors
    ///
    /// - [`EngineError::ServerError`] / [`EngineError::ProtocolViolation`] —
    ///   a server error or teardown was observed instead.
    /// - the pump's transport / framing errors.
    pub async fn recv_notification<S>(
        &mut self,
        live: Live<'b>,
        sink: S,
    ) -> Result<Live<'b>, EngineError<T::Error>>
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
        let boundary = pump_active_to_boundary(active, transport, send_buf, &*obs, sink).await?;
        match boundary {
            // A notification stopped the pull, or the connection reached a clean
            // boundary with none yet — either way the connection is reusable.
            Boundary::Stopped(()) | Boundary::Idle => Ok(live),
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
}

/// A Continue-only sink for the verbs that surface nothing of interest
/// (`BEGIN`/`COMMIT`/`ROLLBACK`). `B = Never`, so it cannot stop the pump.
#[inline]
fn noop_sink(_surface: Surface<'_>) -> ControlFlow<Never> {
    ControlFlow::Continue(())
}
