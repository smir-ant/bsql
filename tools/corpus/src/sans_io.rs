//! Adapter#1 — the bridge from a transcript to the CURRENT sans-IO engine.
//!
//! [`SansIoAdapter`] drives the public `bsql_postgres_core::Session` pump over
//! a scripted transport and reports only observable values. It is the
//! throwaway half of the differential seam: it freely names internal engine
//! types (the action stream, `ProtocolError`, `ActiveState`) because a future
//! engine rewrite replaces this whole file — but everything it RETURNS is the
//! engine-independent [`ObservedRun`].
//!
//! Two twins run the SAME `Session`: [`SansIoAdapter::sync`] drives a scripted
//! blocking chunk queue with no runtime; [`SansIoAdapter::async_twin`] drives a
//! scripted `AsyncRead`/`AsyncWrite` under a current-thread runtime via
//! `block_on`. The drain/collect/finalize logic is shared sync code (the
//! engine is sans-IO, so all of it is synchronous); the twins differ only in
//! the ~20-line outer loop that obtains bytes — a true pump mirror in two
//! shapes. Neither edits any shipped crate.

use std::collections::VecDeque;
use std::num::NonZeroU32;

use bsql_postgres_core::{
    ConnectConfig, DriverError, Handshake, HandshakeAction, PreparedStatement, Session,
};
use bsql_postgres_proto::{
    Action, ActiveState, ColEvent, CommandTagRef, ConnectionStatus, CopyChunkRef, FetchRows,
    NoticeRef, NotificationRef, PortalName, PreparedQuery, Reply, SessionParams, StmtName, TxStatus,
    params::ParamsWriter, prepared,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::adapter::Adapter;
use crate::frames;
use crate::observed::{
    ObservedErr, ObservedNotice, ObservedNotify, ObservedOk, ObservedResultSet, ObservedRun,
    ObservedStatus, ObservedTxStatus, ProtocolFailureKind, TerminalErrorKind,
};
use crate::transcript::{ChunkSchedule, ClientRequest, ParamSpec, Setup, Step, Transcript};
use crate::transport::{ChunkQueue, ScriptedReader, ScriptedWriter, split_into_chunks};

/// The corpus-local `prepared!` demo query exercised by
/// [`ClientRequest::ExecutePreparedDemo`]. A `static` (not `const`) so it can
/// be referenced as the `&'static PreparedQuery` the macro execute path
/// requires. The result tuple type is phantom — the corpus collects RAW
/// per-column bytes, not typed values.
static Q_DEMO: PreparedQuery<(i32,), (i32, &'static str)> =
    prepared!("SELECT id::int4, name::text FROM demo WHERE id = $1::int4");

/// Which twin a [`SansIoAdapter`] drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Twin {
    /// Scripted blocking chunk queue, no runtime.
    Sync,
    /// Scripted `AsyncRead`/`AsyncWrite` under a current-thread runtime.
    Async,
}

/// Adapter over the current sans-IO engine. Construct a twin with
/// [`Self::sync`] or [`Self::async_twin`].
#[derive(Debug, Clone, Copy)]
pub struct SansIoAdapter {
    twin: Twin,
}

impl SansIoAdapter {
    /// The synchronous scripted-transport twin.
    #[must_use]
    pub fn sync() -> Self {
        Self { twin: Twin::Sync }
    }

    /// The asynchronous (`block_on` + scripted `AsyncRead`/`AsyncWrite`) twin.
    #[must_use]
    pub fn async_twin() -> Self {
        Self { twin: Twin::Async }
    }
}

impl Adapter for SansIoAdapter {
    fn run(&self, transcript: &Transcript) -> ObservedRun {
        match self.twin {
            Twin::Sync => run_sync(transcript),
            Twin::Async => run_async(transcript),
        }
    }
}

/// The fixed trust-auth config the corpus connects with.
fn corpus_config() -> ConnectConfig {
    ConnectConfig::new("corpus.invalid", "corpus")
}

/// Canonical minimal trust handshake reply: `AuthenticationOk` +
/// `BackendKeyData` + `ReadyForQuery(idle)`. No `ParameterStatus`, so the
/// active session starts with empty session parameters.
fn canonical_handshake_reply() -> Vec<u8> {
    frames::concat(&[
        frames::auth_ok(),
        frames::backend_key_data(4321, 8765),
        frames::ready_for_query(frames::TX_IDLE),
    ])
}

// ─────────────────────────── shared run state ───────────────────────────

/// Accumulator threaded through a transcript's steps. Only its projection into
/// [`ObservedRun`] is observable; the `last_*` and `pending_*` fields are
/// driving bookkeeping.
struct RunState {
    client_bytes: Vec<u8>,
    notices: Vec<ObservedNotice>,
    notifications: Vec<ObservedNotify>,
    outcome: Result<ObservedOk, ObservedErr>,
    terminal: ObservedStatus,
    /// Connection-level observables captured from the active session at the
    /// end of the run (defaults stand for "no session became active").
    parameter_statuses: Vec<(String, String)>,
    unknown_parameter_status_count: u32,
    backend_pid: Option<i32>,
    tx_status: ObservedTxStatus,
    last_stmt_name: Option<StmtName>,
    last_prepared: Option<PreparedStatement>,
    /// Per-step scratch: the final statement's rows, the prior statements'
    /// result sets (multi-statement intermediates), the COPY-OUT chunks, and
    /// whether the final statement suspended at a row cap. Cleared at the
    /// start of each driven step; consumed by `finalize_step`.
    pending_rows: Vec<Vec<Option<Vec<u8>>>>,
    pending_intermediate_results: Vec<ObservedResultSet>,
    pending_copy_out: Vec<Vec<u8>>,
    pending_portal_suspended: bool,
}

impl RunState {
    fn new() -> Self {
        Self {
            client_bytes: Vec::new(),
            notices: Vec::new(),
            notifications: Vec::new(),
            outcome: Ok(ObservedOk::default()),
            terminal: ObservedStatus::Ready,
            parameter_statuses: Vec::new(),
            unknown_parameter_status_count: 0,
            backend_pid: None,
            tx_status: ObservedTxStatus::Idle,
            last_stmt_name: None,
            last_prepared: None,
            pending_rows: Vec::new(),
            pending_intermediate_results: Vec::new(),
            pending_copy_out: Vec::new(),
            pending_portal_suspended: false,
        }
    }

    /// Reset the per-step scratch before driving a step's reply, so a prior
    /// step's intermediate result sets / copy chunks never bleed into the next.
    fn reset_pending(&mut self) {
        self.pending_rows.clear();
        self.pending_intermediate_results.clear();
        self.pending_copy_out.clear();
        self.pending_portal_suspended = false;
    }

    fn into_observed(self) -> ObservedRun {
        ObservedRun {
            client_bytes: self.client_bytes,
            outcome: self.outcome,
            notices: self.notices,
            parameter_statuses: self.parameter_statuses,
            unknown_parameter_status_count: self.unknown_parameter_status_count,
            notifications: self.notifications,
            backend_pid: self.backend_pid,
            tx_status: self.tx_status,
            terminal: self.terminal,
        }
    }
}

/// Capture the connection-level observables from an active session into the
/// run state, just before projecting to [`ObservedRun`]. These read only the
/// public session surface (parameter set, unknown-key drop count, backend PID,
/// terminal transaction status).
fn capture_conn_observables(run: &mut RunState, session: &Session) {
    let params = session.proto.session_params();
    run.parameter_statuses = observe_param_statuses(params);
    run.unknown_parameter_status_count = u32::from(params.n_unknown_dropped);
    run.backend_pid = Some(session.backend_pid());
    run.tx_status = map_tx_status(session.proto.terminal_tx_status());
}

/// Map the engine's `TxStatus` to the observable transaction status. Closed by
/// spec to `{I, T, E}` (exhaustive — no wildcard fallback).
fn map_tx_status(t: TxStatus) -> ObservedTxStatus {
    match t {
        TxStatus::Idle => ObservedTxStatus::Idle,
        TxStatus::InTransaction => ObservedTxStatus::InTransaction,
        TxStatus::Failed => ObservedTxStatus::Failed,
    }
}

/// Per-column type OIDs from the engine's current `RowDescription`, or empty
/// when no description was observed for the most recent statement.
fn observe_type_oids(session: &Session) -> Vec<u32> {
    match session.proto.current_row_desc() {
        Some(rd) => rd.columns_iter().map(|c| c.type_oid).collect(),
        None => Vec::new(),
    }
}

/// Outcome of pushing one request (no I/O performed yet).
#[allow(
    clippy::large_enum_variant,
    reason = "the Failed(ObservedErr) variant carries the full server-diagnostic observable; this value lives briefly on the stack inside one push step and is matched immediately, so the size spread is immaterial — boxing would add an allocation to the push path for no benefit in a dev-only harness"
)]
enum PushOutcome {
    /// Bytes were staged; drive the scripted reply.
    Drive(Vec<u8>),
    /// Bytes were staged; there is no reply to drive (Terminate).
    NoReply(Vec<u8>),
    /// The push was rejected (connection not ready).
    Failed(ObservedErr),
}

/// One step of the drain loop's classification (mirrors the engine's
/// `PumpAction`, augmented with the notice/notify capture the pump drops).
enum DrainStep {
    /// The connection reached its ready terminal for this command.
    Ready,
    /// Row streaming began; switch to row collection.
    Streaming,
    /// No buffered progress possible; more wire bytes are required.
    NeedMore,
    /// The engine signalled socket close.
    Closed,
    /// The engine entered a terminal error state.
    Errored,
}

// ─────────────────────────── push (shared, sync) ───────────────────────────

/// Push `request` onto the session, returning the staged client bytes (or a
/// classified failure). Updates statement-tracking state but does NOT perform
/// I/O — the caller delivers the bytes through its twin's writer.
fn push_request(session: &mut Session, run: &mut RunState, request: &ClientRequest) -> PushOutcome {
    match request {
        ClientRequest::SimpleQuery(sql) => match session.push_simple_query(sql) {
            Ok(_) => PushOutcome::Drive(session.pending_bytes().to_vec()),
            Err(e) => PushOutcome::Failed(driver_error_to_observed(e)),
        },
        ClientRequest::Ping => match session.push_ping() {
            Ok(_) => PushOutcome::Drive(session.pending_bytes().to_vec()),
            Err(e) => PushOutcome::Failed(driver_error_to_observed(e)),
        },
        ClientRequest::Prepare(sql) => match session.push_prepare(sql) {
            Ok((_, stmt_name)) => {
                run.last_stmt_name = Some(stmt_name);
                PushOutcome::Drive(session.pending_bytes().to_vec())
            }
            Err(e) => PushOutcome::Failed(driver_error_to_observed(e)),
        },
        ClientRequest::DescribeStatement => match run.last_stmt_name {
            Some(name) => match session.push_describe_statement(name) {
                Ok(_) => PushOutcome::Drive(session.pending_bytes().to_vec()),
                Err(e) => PushOutcome::Failed(driver_error_to_observed(e)),
            },
            None => PushOutcome::Failed(ObservedErr::Protocol(ProtocolFailureKind::NotReady)),
        },
        ClientRequest::BindExecute(params) => match run.last_prepared.as_ref() {
            Some(stmt) => match push_bind(session, stmt, params) {
                Ok(()) => PushOutcome::Drive(session.pending_bytes().to_vec()),
                Err(e) => PushOutcome::Failed(driver_error_to_observed(e)),
            },
            None => PushOutcome::Failed(ObservedErr::Protocol(ProtocolFailureKind::NotReady)),
        },
        ClientRequest::BindExecuteRowLimited { params, max_rows } => {
            match run.last_prepared.as_ref() {
                Some(stmt) => match push_bind_row_limited(session, stmt, params, *max_rows) {
                    Ok(bytes) => PushOutcome::Drive(bytes),
                    Err(e) => PushOutcome::Failed(driver_error_to_observed(e)),
                },
                None => PushOutcome::Failed(ObservedErr::Protocol(ProtocolFailureKind::NotReady)),
            }
        }
        ClientRequest::CloseStatement => match run.last_prepared.take() {
            Some(stmt) => match session.push_close_statement(stmt) {
                Ok(_) => PushOutcome::Drive(session.pending_bytes().to_vec()),
                Err(e) => PushOutcome::Failed(driver_error_to_observed(e)),
            },
            None => PushOutcome::Failed(ObservedErr::Protocol(ProtocolFailureKind::NotReady)),
        },
        ClientRequest::ExecutePreparedDemo(id) => {
            match session.push_execute_prepared_macro(&Q_DEMO, (*id,)) {
                Ok(_) => PushOutcome::Drive(session.pending_bytes().to_vec()),
                Err(e) => PushOutcome::Failed(driver_error_to_observed(e)),
            }
        }
        ClientRequest::Terminate => {
            run.terminal = ObservedStatus::Closed;
            PushOutcome::NoReply(bsql_postgres_proto::TERMINATE_WIRE_BYTES.to_vec())
        }
    }
}

/// Dispatch a [`ParamSpec`] to the concrete `ParamsWriter` tuple it names.
fn push_bind(
    session: &mut Session,
    stmt: &PreparedStatement,
    params: &ParamSpec,
) -> Result<(), DriverError> {
    match params {
        ParamSpec::None => session.push_bind_execute(stmt, &()).map(|_| ()),
        ParamSpec::I32(n) => session.push_bind_execute(stmt, &(*n,)).map(|_| ()),
        ParamSpec::Text(s) => session.push_bind_execute(stmt, &(s.as_str(),)).map(|_| ()),
        ParamSpec::I32Text(n, s) => {
            session.push_bind_execute(stmt, &(*n, s.as_str())).map(|_| ())
        }
    }
}

/// Push a row-limited (`Execute.max_rows = N`) bind/execute by driving the
/// proto's `push_bind_execute` directly with `FetchRows::Chunked(N)` — the core
/// `Session` only exposes the unbounded `FetchRows::All` path, so the
/// row-limited / portal-suspend surface is reached through the public proto
/// guard. Returns the staged client bytes. A zero cap is rejected as not-ready
/// (zero is the unbounded `BindExecute` case, never a `Chunked(0)` sentinel).
fn push_bind_row_limited(
    session: &mut Session,
    stmt: &PreparedStatement,
    params: &ParamSpec,
    max_rows: u32,
) -> Result<Vec<u8>, DriverError> {
    let Some(cap) = NonZeroU32::new(max_rows) else {
        return Err(DriverError::NotReady);
    };
    match params {
        ParamSpec::None => bind_row_limited_emit(session, stmt, &(), cap),
        ParamSpec::I32(n) => bind_row_limited_emit(session, stmt, &(*n,), cap),
        ParamSpec::Text(s) => bind_row_limited_emit(session, stmt, &(s.as_str(),), cap),
        ParamSpec::I32Text(n, s) => bind_row_limited_emit(session, stmt, &(*n, s.as_str()), cap),
    }
}

/// Emit the Bind+Execute(max_rows)+Sync frames for a concrete param tuple and
/// collect the staged client bytes.
fn bind_row_limited_emit<P: ParamsWriter>(
    session: &mut Session,
    stmt: &PreparedStatement,
    params: &P,
    cap: NonZeroU32,
) -> Result<Vec<u8>, DriverError> {
    let reply = session.proto.next_reply_id::<bsql_postgres_proto::reply_id::QueryKind>();
    let portal = PortalName::default();
    let mut out = Vec::new();
    {
        let guard = session.proto.as_ready().ok_or(DriverError::NotReady)?;
        let actions = guard
            .push_bind_execute(
                &portal,
                &stmt.stmt_name,
                params,
                stmt.row_desc.clone(),
                FetchRows::Chunked(cap),
                reply,
                &mut session.wb,
            )
            .map_err(|pf| DriverError::Protocol(*pf.cause))?;
        for action in actions.as_slice() {
            if let Action::SendBytes(bytes) = action {
                out.extend_from_slice(bytes);
            }
        }
    }
    session.wb.clear();
    Ok(out)
}

// ─────────────────────────── drain (shared, sync) ───────────────────────────

/// Is the engine in a row-streaming state? Mirrors the (private) streaming
/// classifier on the engine's pump over the public `ActiveState`.
fn is_streaming(session: &Session) -> bool {
    matches!(
        session.proto.state(),
        ActiveState::SimpleQueryStreamingRows { .. }
            | ActiveState::BindExecuteStreamingRows { .. }
            | ActiveState::BindExecuteAwaitingDataOrCompleteSelect { .. }
    )
}

/// Drain bytes already buffered in the engine, capturing notices/notifications
/// the engine's own pump silently drops. Returns when a terminal is reached,
/// streaming begins, or more wire bytes are needed. `step_err` records a server
/// failure cause (the loop keeps draining toward the ready terminal afterwards,
/// so a recoverable server error still ends `Ready`).
fn drain_buffered(session: &mut Session, run: &mut RunState, step_err: &mut Option<ObservedErr>) -> DrainStep {
    loop {
        if is_streaming(session) {
            return DrainStep::Streaming;
        }
        match session.proto.connection_status() {
            ConnectionStatus::Ready => return DrainStep::Ready,
            ConnectionStatus::Errored(_) => return DrainStep::Errored,
            _ => {}
        }

        let unread_before = session.proto.unread().len();
        let mut had_fail = false;
        let mut had_close = false;
        let mut notice_refs: Vec<NoticeRef> = Vec::new();
        let mut notify_refs: Vec<(i32, NotificationRef)> = Vec::new();
        let mut tag_refs: Vec<CommandTagRef> = Vec::new();
        let mut copy_refs: Vec<CopyChunkRef> = Vec::new();
        {
            let actions = session.proto.feed_bytes(&[], &mut session.wb);
            for action in actions.as_slice() {
                match action {
                    Action::SendBytes(bytes) => run.client_bytes.extend_from_slice(bytes),
                    Action::FailReply { .. } => had_fail = true,
                    Action::CloseSocket => had_close = true,
                    Action::Notice { notice_ref } => notice_refs.push(*notice_ref),
                    Action::Notify { pid, notif_ref } => notify_refs.push((*pid, *notif_ref)),
                    // Multi-statement boundary: the prior statement's tag,
                    // surfaced as one result set per non-final statement.
                    Action::IntermediateCommandComplete { tag_ref } => tag_refs.push(*tag_ref),
                    // COPY OUT: each `CopyData` ('d') frame's body.
                    Action::CopyDataChunk { chunk_ref } => copy_refs.push(*chunk_ref),
                    _ => {}
                }
            }
        }
        // Resolve arena handles AFTER the action scope ends (the borrow on `wb`
        // is released; the arenas stay valid until the next `feed_bytes`).
        for r in notice_refs {
            if let Ok(payload) = session.proto.get_notice(r) {
                run.notices.push(ObservedNotice {
                    severity: payload.severity.as_str().to_string(),
                    sqlstate: payload.code.as_str().trim().to_string(),
                    message: payload.message.as_str().to_string(),
                });
            }
        }
        for (pid, r) in notify_refs {
            if let Ok(payload) = session.proto.get_notification(r) {
                run.notifications.push(ObservedNotify {
                    pid,
                    channel: payload.channel.as_str().to_string(),
                    payload: payload.payload.clone(),
                });
            }
        }
        for r in tag_refs {
            if let Ok(tag) = session.proto.get_command_tag(r) {
                // An intermediate statement's full per-statement result set:
                // the engine surfaces only its tag (and the row count the tag
                // encodes) — intermediate statements are non-row-streaming, so
                // their rows/columns/OIDs are not observable here and stay
                // empty, an honest "the engine does not surface these".
                run.pending_intermediate_results.push(ObservedResultSet {
                    command_tag: tag.to_string(),
                    column_names: Vec::new(),
                    type_oids: Vec::new(),
                    rows: Vec::new(),
                    affected_rows: tag.rows(),
                    portal_suspended: false,
                });
            }
        }
        for r in copy_refs {
            if let Ok(payload) = session.proto.get_copy_chunk(r) {
                run.pending_copy_out.push(payload.bytes.clone());
            }
        }

        if had_close {
            if step_err.is_none() {
                *step_err = Some(classify_fail(session));
            }
            return DrainStep::Closed;
        }
        if had_fail && step_err.is_none() {
            *step_err = Some(classify_fail(session));
            // Do not return: a server error parks a recoverable drain-to-RFQ.
            // Keep looping so the trailing ReadyForQuery returns us to Ready.
        }
        if is_streaming(session) {
            return DrainStep::Streaming;
        }
        if session.proto.unread().len() < unread_before {
            continue;
        }
        return DrainStep::NeedMore;
    }
}

/// Resolve the engine's parked failure cause into an observable error.
fn classify_fail(session: &Session) -> ObservedErr {
    match session.proto.fail_cause().copied() {
        Some(cause) => driver_error_to_observed(session.classify_error(cause)),
        None => ObservedErr::Protocol(ProtocolFailureKind::Unclassified),
    }
}

/// Map a driver error to an observable error (server SQLSTATE vs. a stable
/// protocol/transport class).
fn driver_error_to_observed(e: DriverError) -> ObservedErr {
    match e {
        DriverError::Db(db) => ObservedErr::Server {
            sqlstate: db.code,
            severity: db.severity,
            message: db.message,
            detail: db.detail,
            hint: db.hint,
            // The current engine parses only message/detail/hint (plus
            // code/severity); it never surfaces position/schema/table/column/
            // constraint even when the wire frame carried them. Pinning their
            // absence makes a future engine that begins surfacing one catchable.
            position: None,
            schema: None,
            table: None,
            column: None,
            constraint: None,
        },
        DriverError::UnclassifiedFailure => ObservedErr::Protocol(ProtocolFailureKind::Unclassified),
        DriverError::StreamStalled => ObservedErr::Protocol(ProtocolFailureKind::StreamStalled),
        DriverError::RowDescriptionMissing => {
            ObservedErr::Protocol(ProtocolFailureKind::RowDescriptionMissing)
        }
        DriverError::NotReady => ObservedErr::Protocol(ProtocolFailureKind::NotReady),
        _ => ObservedErr::Protocol(ProtocolFailureKind::Unclassified),
    }
}

// ─────────────────────────── row collection (shared, sync) ───────────────────

/// Collect raw per-column row bytes from a streaming reply, pulling more bytes
/// from `next_chunk` on demand. Must run to the stream's terminal (`EndQuery`)
/// before returning — exiting `iter_rows` un-drained would poison the engine.
fn collect_rows(
    session: &mut Session,
    step_err: &mut Option<ObservedErr>,
    suspended: &mut bool,
    mut next_chunk: impl FnMut() -> Option<Vec<u8>>,
) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
    session.iter_rows(|rs| {
        let mut current: Vec<Option<Vec<u8>>> = Vec::new();
        let mut chunk_cell: Vec<u8> = Vec::new();
        let mut in_chunk = false;
        loop {
            let unread_before = rs.unread_len();
            match rs.col_next() {
                ColEvent::Got { bytes, .. } => current.push(Some(bytes.to_vec())),
                ColEvent::Null { .. } => current.push(None),
                ColEvent::Chunk { bytes, .. } => {
                    if !in_chunk {
                        chunk_cell.clear();
                        in_chunk = true;
                    }
                    chunk_cell.extend_from_slice(bytes);
                }
                ColEvent::ChunkEnd { bytes, .. } => {
                    chunk_cell.extend_from_slice(bytes);
                    current.push(Some(core::mem::take(&mut chunk_cell)));
                    in_chunk = false;
                }
                ColEvent::EndRow => {
                    in_chunk = false;
                    rows.push(core::mem::take(&mut current));
                }
                ColEvent::EndQuery { outcome, .. } => {
                    match outcome {
                        // A row-limited Execute that hit its cap terminates the
                        // stream with `QuerySuspended` (portal still open)
                        // rather than `QueryComplete` — a distinct observable.
                        Ok(reply) => {
                            if matches!(reply, Reply::QuerySuspended(_)) {
                                *suspended = true;
                            }
                        }
                        Err(_) => {
                            if step_err.is_none() {
                                *step_err =
                                    Some(ObservedErr::Protocol(ProtocolFailureKind::Unclassified));
                            }
                        }
                    }
                    return;
                }
                ColEvent::NeedMore => {
                    if rs.unread_len() < unread_before {
                        continue;
                    }
                    match next_chunk() {
                        Some(chunk) => {
                            if rs.feed(&chunk).is_err() && step_err.is_none() {
                                *step_err =
                                    Some(ObservedErr::Protocol(ProtocolFailureKind::StreamStalled));
                                return;
                            }
                        }
                        None => {
                            if step_err.is_none() {
                                *step_err =
                                    Some(ObservedErr::Protocol(ProtocolFailureKind::StreamStalled));
                            }
                            return;
                        }
                    }
                }
                _ => {
                    if step_err.is_none() {
                        *step_err = Some(ObservedErr::Protocol(ProtocolFailureKind::StreamStalled));
                    }
                    return;
                }
            }
        }
    });
    rows
}

// ─────────────────────────── finalize (shared, sync) ───────────────────────

/// After a step's drive reaches its terminal, record the step's outcome.
fn finalize_step(session: &Session, run: &mut RunState, request: &ClientRequest, step_err: Option<ObservedErr>) {
    if let Some(err) = step_err {
        run.outcome = Err(err);
        return;
    }
    // DescribeStatement completes the prepared statement for a later bind.
    if matches!(request, ClientRequest::DescribeStatement)
        && let Some(name) = run.last_stmt_name
    {
        run.last_prepared = Some(session.finish_prepare(name));
    }
    // The final statement's result set: rows + tag + per-column type OIDs +
    // affected count + whether it suspended at a row cap. The prior statements
    // (multi-statement intermediates) were captured during the drive; the full
    // per-statement sequence is `[intermediates..., final]`.
    let final_result_set = ObservedResultSet {
        command_tag: session.extract_command_tag(),
        column_names: session.extract_column_names().to_vec(),
        type_oids: observe_type_oids(session),
        rows: core::mem::take(&mut run.pending_rows),
        affected_rows: session.affected_rows(),
        portal_suspended: run.pending_portal_suspended,
    };
    let mut result_sets = core::mem::take(&mut run.pending_intermediate_results);
    result_sets.push(final_result_set);
    run.outcome = Ok(ObservedOk {
        result_sets,
        copy_out: core::mem::take(&mut run.pending_copy_out),
    });
}

/// Render the engine's accumulated session parameters as an ordered observable
/// (key, value) list — the cross-engine view of `ParameterStatus` state.
fn observe_param_statuses(params: &SessionParams) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    if let Some(v) = params.server_version.as_ref() {
        out.push(("server_version".to_string(), v.as_str().to_string()));
    }
    if let Some(enc) = params.server_encoding.as_ref() {
        out.push(("server_encoding".to_string(), encoding_name(enc)));
    }
    if let Some(enc) = params.client_encoding.as_ref() {
        out.push(("client_encoding".to_string(), encoding_name(enc)));
    }
    if let Some(v) = params.application_name.as_ref() {
        out.push(("application_name".to_string(), v.as_str().to_string()));
    }
    if let Some(b) = params.is_superuser {
        out.push(("is_superuser".to_string(), bool_on_off(b)));
    }
    if let Some(v) = params.session_authorization.as_ref() {
        out.push(("session_authorization".to_string(), v.as_str().to_string()));
    }
    if let Some(v) = params.date_style.as_ref() {
        out.push(("DateStyle".to_string(), v.as_str().to_string()));
    }
    if let Some(b) = params.integer_datetimes {
        out.push(("integer_datetimes".to_string(), bool_on_off(b)));
    }
    if let Some(v) = params.time_zone.as_ref() {
        out.push(("TimeZone".to_string(), v.as_str().to_string()));
    }
    out
}

fn bool_on_off(b: bool) -> String {
    if b { "on".to_string() } else { "off".to_string() }
}

/// Canonical PG name for a parsed encoding. Covers every current variant; the
/// non-exhaustive wildcard yields a stable placeholder (never a silent drop).
fn encoding_name(enc: &bsql_postgres_proto::Encoding) -> String {
    use bsql_postgres_proto::Encoding;
    match enc {
        Encoding::Utf8 => "UTF8".to_string(),
        Encoding::SqlAscii => "SQL_ASCII".to_string(),
        Encoding::Latin1 => "LATIN1".to_string(),
        Encoding::Latin9 => "LATIN9".to_string(),
        Encoding::Win1252 => "WIN1252".to_string(),
        Encoding::EucJp => "EUC_JP".to_string(),
        Encoding::EucKr => "EUC_KR".to_string(),
        Encoding::Big5 => "BIG5".to_string(),
        Encoding::Gb18030 => "GB18030".to_string(),
        Encoding::Other(o) => String::from_utf8_lossy(o.as_bytes()).into_owned(),
        _ => "<unknown-encoding>".to_string(),
    }
}

// ─────────────────────────── SYNC twin ───────────────────────────

fn run_sync(transcript: &Transcript) -> ObservedRun {
    let mut run = RunState::new();
    let mut session = match sync_setup(transcript, &mut run) {
        SetupResult::Active(s) => s,
        // No session became active (disconnected / failed handshake). The
        // failure observables were recorded by the handshake driver; the
        // connection-level observables stay at their no-session defaults.
        SetupResult::NoSession => return run.into_observed(),
    };

    for step in &transcript.steps {
        if !sync_run_step(&mut session, &mut run, step, transcript.chunk_schedule) {
            break;
        }
    }

    capture_conn_observables(&mut run, &session);
    run.into_observed()
}

/// Result of driving a transcript's setup.
#[allow(
    clippy::large_enum_variant,
    reason = "the Active(Session) variant is the dominant case (every active-phase transcript) and the value lives briefly on the stack before its steps run; boxing it to shrink the unit NoSession variant would add an allocation to the common path for no benefit"
)]
enum SetupResult {
    /// An active session is ready for steps (a startup-only transcript runs
    /// zero steps and is finalised directly).
    Active(Session),
    /// No session was produced — a disconnected server or a failed handshake.
    /// The failure observables already live in the run state.
    NoSession,
}

fn sync_setup(transcript: &Transcript, run: &mut RunState) -> SetupResult {
    match &transcript.setup {
        Setup::ActiveViaTrustHandshake => {
            // Setup handshake bytes are NOT part of the observed client wire —
            // only the steps' requests are. Drive it discarding client bytes.
            let chunks = split_into_chunks(&canonical_handshake_reply(), ChunkSchedule::AllAtOnce);
            match sync_handshake(&mut ChunkQueue::new(chunks), run, false) {
                Ok(session) => SetupResult::Active(session),
                Err(()) => SetupResult::NoSession,
            }
        }
        Setup::StartupScript { server_bytes } => {
            let chunks = split_into_chunks(server_bytes, transcript.chunk_schedule);
            match sync_handshake(&mut ChunkQueue::new(chunks), run, true) {
                Ok(session) => SetupResult::Active(session),
                Err(()) => SetupResult::NoSession,
            }
        }
        Setup::Disconnected => {
            // Server supplies no bytes; the handshake cannot complete.
            match sync_handshake(&mut ChunkQueue::new(Vec::new()), run, true) {
                Ok(session) => SetupResult::Active(session),
                Err(()) => SetupResult::NoSession,
            }
        }
    }
}

/// Drive the connect handshake over a sync chunk queue. `record_client`
/// controls whether the startup/auth client bytes count toward the observable.
fn sync_handshake(queue: &mut ChunkQueue, run: &mut RunState, record_client: bool) -> Result<Session, ()> {
    let config = corpus_config();
    let (startup, mut hs) = match Handshake::begin(&config) {
        Ok(pair) => pair,
        Err(_) => {
            run.terminal = ObservedStatus::Errored(TerminalErrorKind::Handshake);
            run.outcome = Err(ObservedErr::Protocol(ProtocolFailureKind::HandshakeFailed));
            return Err(());
        }
    };
    if record_client {
        run.client_bytes.extend_from_slice(&startup);
    }
    loop {
        match hs.step() {
            HandshakeAction::Send => {
                if record_client {
                    run.client_bytes.extend_from_slice(hs.pending_bytes());
                }
            }
            HandshakeAction::NeedRead => match queue.next_chunk() {
                Some(chunk) => {
                    if hs.feed(&chunk).is_err() {
                        return handshake_failed(run);
                    }
                }
                None => return handshake_failed(run),
            },
            HandshakeAction::Done => match hs.finish() {
                Ok(session) => return Ok(session),
                Err(_) => return handshake_failed(run),
            },
            HandshakeAction::Error(_) => return handshake_failed(run),
        }
    }
}

fn handshake_failed(run: &mut RunState) -> Result<Session, ()> {
    run.terminal = ObservedStatus::Errored(TerminalErrorKind::Handshake);
    run.outcome = Err(ObservedErr::Protocol(ProtocolFailureKind::HandshakeFailed));
    Err(())
}

/// Record a mid-run client-write failure as a classified observable: the
/// connection reaches its error terminal carrying a transport-failure outcome.
/// Bytes the transport refuses to accept are a transport-side I/O failure — the
/// write-side analog of a read-side exhaustion — so a refused write must surface
/// here, never be dropped silently.
fn transport_write_failed(run: &mut RunState) {
    run.terminal = ObservedStatus::Errored(TerminalErrorKind::Protocol);
    run.outcome = Err(ObservedErr::Protocol(ProtocolFailureKind::TransportExhausted));
}

/// Drive one step over a sync chunk queue. Returns `false` to stop the
/// transcript (terminal reached / push failed).
fn sync_run_step(session: &mut Session, run: &mut RunState, step: &Step, schedule: ChunkSchedule) -> bool {
    match push_request(session, run, &step.request) {
        PushOutcome::Failed(err) => {
            run.outcome = Err(err);
            false
        }
        PushOutcome::NoReply(bytes) => {
            run.client_bytes.extend_from_slice(&bytes);
            false
        }
        PushOutcome::Drive(bytes) => {
            run.client_bytes.extend_from_slice(&bytes);
            run.reset_pending();
            let mut queue = ChunkQueue::new(split_into_chunks(&step.server_reply, schedule));
            let mut step_err: Option<ObservedErr> = None;
            loop {
                match drain_buffered(session, run, &mut step_err) {
                    DrainStep::Ready => break,
                    DrainStep::Closed => {
                        run.terminal = ObservedStatus::Closed;
                        break;
                    }
                    DrainStep::Errored => {
                        if step_err.is_none() {
                            step_err = Some(classify_fail(session));
                        }
                        run.terminal = ObservedStatus::Errored(TerminalErrorKind::Protocol);
                        break;
                    }
                    DrainStep::Streaming => {
                        let rows = collect_rows(
                            session,
                            &mut step_err,
                            &mut run.pending_portal_suspended,
                            || queue.next_chunk(),
                        );
                        run.pending_rows = rows;
                    }
                    DrainStep::NeedMore => match queue.next_chunk() {
                        Some(chunk) => {
                            if session.feed(&chunk).is_err() {
                                step_err = Some(ObservedErr::Protocol(
                                    ProtocolFailureKind::TransportExhausted,
                                ));
                                break;
                            }
                        }
                        None => {
                            step_err =
                                Some(ObservedErr::Protocol(ProtocolFailureKind::TransportExhausted));
                            break;
                        }
                    },
                }
            }
            finalize_step(session, run, &step.request, step_err);
            !matches!(run.terminal, ObservedStatus::Closed | ObservedStatus::Errored(_))
        }
    }
}

// ─────────────────────────── ASYNC twin ───────────────────────────

fn run_async(transcript: &Transcript) -> ObservedRun {
    let runtime = match tokio::runtime::Builder::new_current_thread().build() {
        Ok(rt) => rt,
        // A current-thread runtime build failure is an environment fault, not a
        // protocol observation — surface it as a handshake-class terminal so the
        // test fails loudly rather than silently producing a bogus comparison.
        Err(_) => {
            let mut run = RunState::new();
            run.terminal = ObservedStatus::Errored(TerminalErrorKind::Unclassified);
            run.outcome = Err(ObservedErr::Protocol(ProtocolFailureKind::Unclassified));
            return run.into_observed();
        }
    };
    runtime.block_on(async { run_async_inner(transcript).await })
}

async fn run_async_inner(transcript: &Transcript) -> ObservedRun {
    let mut run = RunState::new();
    let mut writer = ScriptedWriter::default();

    let mut session = match async_setup(transcript, &mut run, &mut writer).await {
        SetupResult::Active(s) => s,
        SetupResult::NoSession => {
            run.client_bytes = writer.captured;
            return run.into_observed();
        }
    };

    for step in &transcript.steps {
        if !async_run_step(&mut session, &mut run, &mut writer, step, transcript.chunk_schedule).await
        {
            break;
        }
    }

    capture_conn_observables(&mut run, &session);
    run.client_bytes = writer.captured;
    run.into_observed()
}

async fn async_setup(
    transcript: &Transcript,
    run: &mut RunState,
    writer: &mut ScriptedWriter,
) -> SetupResult {
    match &transcript.setup {
        Setup::ActiveViaTrustHandshake => {
            let mut reader =
                ScriptedReader::new(split_into_chunks(&canonical_handshake_reply(), ChunkSchedule::AllAtOnce));
            // Setup client bytes are discarded: route through a throwaway writer.
            let mut discard = ScriptedWriter::default();
            match async_handshake(&mut reader, &mut discard, run).await {
                Ok(session) => SetupResult::Active(session),
                Err(()) => SetupResult::NoSession,
            }
        }
        Setup::StartupScript { server_bytes } => {
            let mut reader =
                ScriptedReader::new(split_into_chunks(server_bytes, transcript.chunk_schedule));
            match async_handshake(&mut reader, writer, run).await {
                Ok(session) => SetupResult::Active(session),
                Err(()) => SetupResult::NoSession,
            }
        }
        Setup::Disconnected => {
            let mut reader = ScriptedReader::new(Vec::new());
            match async_handshake(&mut reader, writer, run).await {
                Ok(session) => SetupResult::Active(session),
                Err(()) => SetupResult::NoSession,
            }
        }
    }
}

/// Read one scripted chunk via the async reader; `None` at EOF.
async fn read_one(reader: &mut ScriptedReader) -> Option<Vec<u8>> {
    let mut buf = [0u8; 8192];
    match reader.read(&mut buf).await {
        Ok(0) => None,
        Ok(n) => buf.get(..n).map(<[u8]>::to_vec),
        Err(_) => None,
    }
}

async fn async_handshake(
    reader: &mut ScriptedReader,
    writer: &mut ScriptedWriter,
    run: &mut RunState,
) -> Result<Session, ()> {
    let config = corpus_config();
    let (startup, mut hs) = match Handshake::begin(&config) {
        Ok(pair) => pair,
        Err(_) => return handshake_failed(run),
    };
    if writer.write_all(&startup).await.is_err() {
        return handshake_failed(run);
    }
    loop {
        match hs.step() {
            HandshakeAction::Send => {
                if writer.write_all(hs.pending_bytes()).await.is_err() {
                    return handshake_failed(run);
                }
            }
            HandshakeAction::NeedRead => match read_one(reader).await {
                Some(chunk) => {
                    if hs.feed(&chunk).is_err() {
                        return handshake_failed(run);
                    }
                }
                None => return handshake_failed(run),
            },
            HandshakeAction::Done => match hs.finish() {
                Ok(session) => return Ok(session),
                Err(_) => return handshake_failed(run),
            },
            HandshakeAction::Error(_) => return handshake_failed(run),
        }
    }
}

async fn async_run_step(
    session: &mut Session,
    run: &mut RunState,
    writer: &mut ScriptedWriter,
    step: &Step,
    schedule: ChunkSchedule,
) -> bool {
    match push_request(session, run, &step.request) {
        PushOutcome::Failed(err) => {
            run.outcome = Err(err);
            false
        }
        PushOutcome::NoReply(bytes) => {
            // push_request already set the Closed terminal for Terminate; a
            // refused write overrides it with the transport-failure terminal.
            if writer.write_all(&bytes).await.is_err() {
                transport_write_failed(run);
            }
            false
        }
        PushOutcome::Drive(bytes) => {
            if writer.write_all(&bytes).await.is_err() {
                transport_write_failed(run);
                return false;
            }
            run.reset_pending();
            let mut reader = ScriptedReader::new(split_into_chunks(&step.server_reply, schedule));
            let mut step_err: Option<ObservedErr> = None;
            loop {
                match drain_buffered(session, run, &mut step_err) {
                    DrainStep::Ready => break,
                    DrainStep::Closed => {
                        run.terminal = ObservedStatus::Closed;
                        break;
                    }
                    DrainStep::Errored => {
                        if step_err.is_none() {
                            step_err = Some(classify_fail(session));
                        }
                        run.terminal = ObservedStatus::Errored(TerminalErrorKind::Protocol);
                        break;
                    }
                    DrainStep::Streaming => {
                        // Prebuffer the remaining scripted chunks via async reads,
                        // preserving the schedule's fragmentation, then collect
                        // rows synchronously (iter_rows must not be exited early).
                        let mut pending: VecDeque<Vec<u8>> = VecDeque::new();
                        while let Some(chunk) = read_one(&mut reader).await {
                            pending.push_back(chunk);
                        }
                        let rows = collect_rows(
                            session,
                            &mut step_err,
                            &mut run.pending_portal_suspended,
                            || pending.pop_front(),
                        );
                        run.pending_rows = rows;
                    }
                    DrainStep::NeedMore => match read_one(&mut reader).await {
                        Some(chunk) => {
                            if session.feed(&chunk).is_err() {
                                step_err = Some(ObservedErr::Protocol(
                                    ProtocolFailureKind::TransportExhausted,
                                ));
                                break;
                            }
                        }
                        None => {
                            step_err =
                                Some(ObservedErr::Protocol(ProtocolFailureKind::TransportExhausted));
                            break;
                        }
                    },
                }
            }
            finalize_step(session, run, &step.request, step_err);
            !matches!(run.terminal, ObservedStatus::Closed | ObservedStatus::Errored(_))
        }
    }
}
