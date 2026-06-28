//! Adapter#2 — the bridge from a transcript to the NEW strangler engine's
//! connecting path (handshake-only).
//!
//! [`EngineAdapter`] drives the new `bsql_postgres_proto::engine`
//! [`ConnectingEngine`] over a transcript's scripted server bytes and reports
//! the same observable [`ObservedRun`] that Adapter#1 returns — restricted to
//! the handshake-relevant fields (client startup/auth bytes, parameter
//! statuses, backend pid, terminal status, transaction status). It is the
//! analogue of `SansIoAdapter`'s drain, but instead of the live `Session`
//! pump it feeds the fixture bytes through the engine's single-residence
//! `read_slot`/`commit` ingest and drives `next_auth_event()` by hand.
//!
//! For non-connecting fixtures (those with client steps) this adapter is not
//! wired — it observes the handshake outcome only. The differential test
//! restricts itself to the handshake/startup subset accordingly.

use std::sync::{Arc, Mutex};

use bsql_postgres_proto::engine::{
    poll_once, session, ActiveEngine, AuthEvent, ConnectingEngine, Engine, EngineError, Event, Live,
    NoObserver, SendBuf, Surface,
};
use bsql_postgres_proto::{
    prepared, Credentials, Encoding, Ident, PreparedQuery, SessionParams, TxStatus,
};

use core::convert::Infallible;
use core::ops::ControlFlow;

use crate::engine_transport::{ClientCapture, EngineScriptTransport};

use bsql_corpus::adapter::Adapter;
use bsql_corpus::frames;
use bsql_corpus::observed::{
    ObservedErr, ObservedNotice, ObservedNotify, ObservedOk, ObservedResultSet, ObservedRun,
    ObservedStatus, ObservedTxStatus, ProtocolFailureKind, TerminalErrorKind,
};
use bsql_corpus::transcript::{ClientRequest, Setup, Step, Transcript};
use bsql_corpus::transport::split_into_chunks;

/// The backend PID pinned by the canonical trust handshake — mirrors the
/// constant Adapter#1 surfaces for `ActiveViaTrustHandshake` transcripts.
const TRUST_BACKEND_PID: i32 = 4321;

/// Result-column type OIDs of the corpus-local prepared-statement demo query
/// (`SELECT id::int4, name::text`): `int4` (23) then `text` (25). The macro
/// path re-sends no `RowDescription`, so the executed rows are surfaced against
/// this compile-time schema — threaded into the engine the same way a statement
/// `Describe`'s recovered schema is. A drift here is caught by the differential
/// (Adapter#1 surfaces the macro's real OIDs).
const DEMO_RESULT_OIDS: [u32; 2] = [23, 25];

/// Adapter over the new engine's connecting path. Handshake-only.
#[derive(Debug, Clone, Copy)]
pub struct EngineAdapter;

impl EngineAdapter {
    /// Construct the handshake-only engine adapter.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Drive a transcript's ACTIVE phase through the new engine's pull surface
    /// and project the observable outcome.
    ///
    /// The active-phase twin of [`SansIoAdapter`](bsql_corpus::SansIoAdapter)'s
    /// drain + `col_next`: it reaches an active handle through the canonical
    /// trust handshake, then feeds each step's scripted server bytes through the
    /// active engine's `read_slot`/`commit` ingest and drives `next_event()` by
    /// hand — no `Transport`, no pump. It captures the SAME [`ObservedRun`]
    /// response fields Adapter#1 does (per-statement result sets with rows /
    /// tags / OIDs, notices, notifications, parameter statuses, copy-out chunks,
    /// transaction status, terminal) — except `client_bytes`: the pull engine is
    /// response-driven and emits no request frames, so the differential
    /// compares the response projection.
    ///
    /// Scoped to `ActiveViaTrustHandshake` transcripts whose steps are the
    /// pull-drivable request kinds: the simple-query flow plus the extended
    /// query protocol (`Prepare`/`DescribeStatement`/`BindExecute`/
    /// `BindExecuteRowLimited`/`CloseStatement`/`ExecutePreparedDemo`). State is
    /// reconstructed from each request's TAG (seating the engine via its
    /// `begin_*` seam) plus the server frames — no client wire is encoded. A
    /// `Ping` (bare `ReadyForQuery`) or `Terminate` (no reply) is not
    /// pull-drivable and reports a failed run.
    #[must_use]
    pub fn pull(&self, transcript: &Transcript) -> ObservedRun {
        run_pull(transcript)
    }

    /// Drive a transcript through the new engine's REAL verbs over a scripted
    /// [`Transport`](bsql_postgres_proto::engine::Transport), capturing the FULL
    /// [`ObservedRun`] INCLUDING `client_bytes`.
    ///
    /// Unlike [`pull`](Self::pull) (which hand-feeds the engine's framing and
    /// emits no client wire), this calls `connect` then each step's verb via
    /// `poll_once` over an always-ready scripted transport, so the captured
    /// outbound wire is the verbs' actual request bytes. Scoped to the
    /// client-bytes-comparable subset — `ActiveViaTrustHandshake` transcripts
    /// whose every step is in `{SimpleQuery, Ping, ExecutePreparedDemo}`; the
    /// fine-grained extended fixtures (separate `Prepare`/`Describe`/`Bind` steps
    /// = three Syncs) do not map to the bundling verbs and stay on
    /// [`pull`](Self::pull). A transcript outside the subset reports a failed run.
    #[must_use]
    pub fn verb(&self, transcript: &Transcript) -> ObservedRun {
        run_verb(transcript)
    }
}

impl Default for EngineAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Adapter for EngineAdapter {
    fn run(&self, transcript: &Transcript) -> ObservedRun {
        run_handshake(transcript)
    }
}

/// The canonical minimal trust handshake reply (mirrors Adapter#1's): an
/// `AuthenticationOk` + `BackendKeyData` + `ReadyForQuery(idle)` chain with no
/// `ParameterStatus`, so the session's parameter set starts empty.
fn canonical_handshake_reply() -> Vec<u8> {
    frames::concat(&[
        frames::auth_ok(),
        frames::backend_key_data(TRUST_BACKEND_PID, 8765),
        frames::ready_for_query(frames::TX_IDLE),
    ])
}

/// Drive one transcript's handshake through the new engine and project the
/// observable outcome.
fn run_handshake(transcript: &Transcript) -> ObservedRun {
    // The server bytes to feed, and whether the client startup/auth bytes
    // count toward the observable (Adapter#1 discards them for the canonical
    // trust setup, records them for a scripted startup).
    let (server_bytes, record_client) = match &transcript.setup {
        Setup::ActiveViaTrustHandshake => (canonical_handshake_reply(), false),
        Setup::StartupScript { server_bytes } => (server_bytes.clone(), true),
        Setup::Disconnected => (Vec::new(), true),
    };

    // The corpus connects as user "corpus", no database / application_name,
    // Trust credentials (the corpus has no password configured) — identical to
    // Adapter#1's `corpus_config`.
    let user = match Ident::try_from_str("corpus") {
        Ok(user) => user,
        Err(_) => return failed_run(Vec::new()),
    };
    // The sole outbound residence: the startup packet and any auth response are
    // queued here. It is never flushed in this in-memory adapter, so `pending()`
    // accumulates the full client wire (startup ++ auth) the differential reads.
    let mut send_buf = SendBuf::new();
    let mut engine = match ConnectingEngine::start(&mut send_buf, &user, None, None, Credentials::Trust) {
        Ok(engine) => engine,
        Err(_) => return failed_run(Vec::new()),
    };

    let mut chunks = split_into_chunks(&server_bytes, transcript.chunk_schedule).into_iter();
    let mut params = SessionParams::new();
    let mut ready = false;

    loop {
        // The borrow of the lent frame ends with each match arm; capture only
        // the owned signal needed after it.
        let need_more = match engine.next_auth_event(&mut send_buf) {
            AuthEvent::Ready => {
                ready = true;
                break;
            }
            AuthEvent::Fail(_) => break,
            AuthEvent::ParamStatus(payload) => {
                if let Some((key, value)) = split_parameter_status(payload) {
                    params.set(key, value);
                }
                false
            }
            // The trust path never reaches these; the password paths build the
            // response into the engine's write buffer, so they are observed but
            // require no caller action here.
            AuthEvent::AuthCleartext
            | AuthEvent::AuthMd5 { .. }
            | AuthEvent::AuthSaslContinue(_) => false,
            AuthEvent::NeedMore => true,
        };
        if need_more {
            match chunks.next() {
                Some(chunk) => {
                    if !feed_chunk(&mut engine, &chunk) {
                        break;
                    }
                }
                // Input exhausted before the handshake completed.
                None => break,
            }
        }
    }

    // Capture the outbound client bytes AFTER driving (any auth response is
    // queued after the startup packet). The send buffer is never flushed here,
    // so `pending()` is the full client wire. Discarded for the canonical trust
    // setup.
    let client_bytes = if record_client {
        send_buf.pending().to_vec()
    } else {
        Vec::new()
    };

    if ready {
        match engine.into_active() {
            Ok(active) => ready_run(
                client_bytes,
                &params,
                active.backend_pid(),
                active.tx_status(),
            ),
            // `into_active` only fails before `Ready`; unreachable on this arm.
            Err(_) => failed_run(client_bytes),
        }
    } else {
        failed_run(client_bytes)
    }
}

/// Feed one scripted chunk into the engine's single-residence ingest buffer.
/// Returns `false` on a buffer-full / commit failure (a fatal connection
/// error — the handshake cannot make progress).
fn feed_chunk(engine: &mut ConnectingEngine, chunk: &[u8]) -> bool {
    let mut fed = 0usize;
    while fed < chunk.len() {
        let remaining = &chunk[fed..];
        let slot = match engine.read_slot(remaining.len()) {
            Ok(slot) => slot,
            Err(_) => return false,
        };
        if slot.is_empty() {
            return false;
        }
        let n = slot.len().min(remaining.len());
        slot[..n].copy_from_slice(&remaining[..n]);
        if engine.commit(n).is_err() {
            return false;
        }
        fed += n;
    }
    true
}

/// Split a `ParameterStatus` payload (`key\0value\0`) into its key/value byte
/// slices.
fn split_parameter_status(payload: &[u8]) -> Option<(&[u8], &[u8])> {
    let key_end = payload.iter().position(|&byte| byte == 0)?;
    let key = &payload[..key_end];
    let rest = &payload[key_end + 1..];
    let value_end = match rest.iter().position(|&byte| byte == 0) {
        Some(idx) => idx,
        None => rest.len(),
    };
    let value = &rest[..value_end];
    Some((key, value))
}

/// A `Ready` observed run reached via the handshake.
fn ready_run(
    client_bytes: Vec<u8>,
    params: &SessionParams,
    backend_pid: i32,
    tx_status: TxStatus,
) -> ObservedRun {
    ObservedRun {
        client_bytes,
        outcome: Ok(ObservedOk::default()),
        notices: Vec::new(),
        parameter_statuses: observe_param_statuses(params),
        unknown_parameter_status_count: u32::from(params.n_unknown_dropped),
        notifications: Vec::new(),
        backend_pid: Some(backend_pid),
        tx_status: map_tx_status(tx_status),
        terminal: ObservedStatus::Ready,
    }
}

/// A failed-handshake observed run: no session, no observables captured.
fn failed_run(client_bytes: Vec<u8>) -> ObservedRun {
    ObservedRun {
        client_bytes,
        outcome: Err(ObservedErr::Protocol(ProtocolFailureKind::HandshakeFailed)),
        notices: Vec::new(),
        parameter_statuses: Vec::new(),
        unknown_parameter_status_count: 0,
        notifications: Vec::new(),
        backend_pid: None,
        tx_status: ObservedTxStatus::Idle,
        terminal: ObservedStatus::Errored(TerminalErrorKind::Handshake),
    }
}

/// Map the engine's `TxStatus` to the observable transaction status. Closed by
/// spec to `{I, T, E}`.
fn map_tx_status(status: TxStatus) -> ObservedTxStatus {
    match status {
        TxStatus::Idle => ObservedTxStatus::Idle,
        TxStatus::InTransaction => ObservedTxStatus::InTransaction,
        TxStatus::Failed => ObservedTxStatus::Failed,
    }
}

/// Project the accumulated session parameters into the ordered observable
/// (key, value) list — the same fixed-key projection Adapter#1 applies, over
/// the same `SessionParams` ingest, so the two adapters agree by construction.
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
    if b {
        "on".to_string()
    } else {
        "off".to_string()
    }
}

// ===========================================================================
// Adapter#2 — active-phase pull twin
// ===========================================================================

/// An owned snapshot of one pulled [`Event`], taken before its borrow of the
/// engine ends — so a step's events can be consumed past the no-escape wall.
enum PullEvent {
    NeedMore,
    Idle,
    Close,
    /// Statement boundary: the command tag + the live per-statement columns/OIDs
    /// captured at the `Deliver` point (before the trailing RFQ resets them).
    Deliver {
        command_tag: String,
        affected_rows: Option<u64>,
        column_names: Vec<String>,
        type_oids: Vec<u32>,
    },
    /// A row-limited Execute paused at its cap (`PortalSuspended`): a typed
    /// terminal distinct from `Deliver`, carrying no command tag. The live
    /// per-statement columns/OIDs are captured for the suspended result set.
    Suspended {
        column_names: Vec<String>,
        type_oids: Vec<u32>,
    },
    Row(Vec<Option<Vec<u8>>>),
    /// Oversize row chunk / terminator — not produced by the active fixture
    /// subset; the chunk bytes are not part of the observable result set, so the
    /// payload is dropped at conversion.
    RowChunk,
    RowChunkEnd,
    Notice(Option<ObservedNotice>),
    Notify(Option<ObservedNotify>),
    ParamStatus(Vec<u8>),
    CopyData(Vec<u8>),
    CopyDone,
    Fail(ObservedErr),
}

/// Pull one event from the active engine and convert it to an owned
/// [`PullEvent`], releasing the borrow it carries. `Deliver` re-borrows the
/// engine's accessors after the (payload-free) event borrow ends.
fn pull_one(active: &mut ActiveEngine) -> PullEvent {
    match active.next_event() {
        Event::NeedMore => PullEvent::NeedMore,
        Event::Idle => PullEvent::Idle,
        Event::Close => PullEvent::Close,
        Event::Row(body) => PullEvent::Row(parse_data_row(body)),
        Event::RowChunk(_) => PullEvent::RowChunk,
        Event::RowChunkEnd => PullEvent::RowChunkEnd,
        Event::Notice(body) => PullEvent::Notice(parse_diagnostic_notice(body)),
        Event::Notify(body) => PullEvent::Notify(parse_notification(body)),
        Event::ParamStatus(body) => PullEvent::ParamStatus(body.to_vec()),
        Event::CopyData(body) => PullEvent::CopyData(body.to_vec()),
        Event::CopyDone => PullEvent::CopyDone,
        Event::Fail(body) => PullEvent::Fail(parse_server_error(body)),
        Event::Deliver => {
            let (command_tag, affected_rows) = match active.last_command_tag() {
                Some(tag) => (tag.to_string(), tag.rows()),
                None => (String::new(), None),
            };
            PullEvent::Deliver {
                command_tag,
                affected_rows,
                column_names: active.current_column_names().to_vec(),
                type_oids: active.current_type_oids().to_vec(),
            }
        }
        // A row-limited Execute paused at its cap: a typed terminal. The
        // (payload-free) event borrow has ended, so the per-statement
        // columns/OIDs are read here for the suspended result set.
        Event::Suspended => PullEvent::Suspended {
            column_names: active.current_column_names().to_vec(),
            type_oids: active.current_type_oids().to_vec(),
        },
    }
}

/// Per-step accumulator for the pull runner.
struct PullStep {
    result_sets: Vec<ObservedResultSet>,
    pending_rows: Vec<Vec<Option<Vec<u8>>>>,
    copy_out: Vec<Vec<u8>>,
    step_err: Option<ObservedErr>,
    terminal: Option<ObservedStatus>,
}

impl PullStep {
    fn new() -> Self {
        Self {
            result_sets: Vec::new(),
            pending_rows: Vec::new(),
            copy_out: Vec::new(),
            step_err: None,
            terminal: None,
        }
    }
}

/// Drive one transcript through the active pull surface.
fn run_pull(transcript: &Transcript) -> ObservedRun {
    // Only the canonical-trust-handshake active subset is pull-driven.
    if !matches!(transcript.setup, Setup::ActiveViaTrustHandshake) {
        return failed_run(Vec::new());
    }
    let mut active = match setup_active() {
        Some(active) => active,
        None => return failed_run(Vec::new()),
    };

    let backend_pid = active.backend_pid();
    let mut params = SessionParams::new();
    let mut notices: Vec<ObservedNotice> = Vec::new();
    let mut notifications: Vec<ObservedNotify> = Vec::new();
    let mut outcome: Result<ObservedOk, ObservedErr> = Ok(ObservedOk::default());
    let mut terminal = ObservedStatus::Ready;

    // The result-column type OIDs recovered from the most recent statement
    // `Describe`. The Execute reply re-sends no `RowDescription`, so the OIDs of
    // a `Bind`+`Execute`'s rows come from the preceding `Describe` — threaded
    // into the engine via `begin_bind_execute`.
    let mut described_oids: Vec<u32> = Vec::new();

    for step in &transcript.steps {
        // Seat the engine into the awaiting-state matching this request before
        // draining its reply — the response-driven analog of a push. SimpleQuery
        // needs no seat (`Idle` is the awaiting-first-response state); each
        // extended-protocol verb seats its matching state. `Ping` is a bare
        // `Sync` whose `ReadyForQuery` lands at `Idle` with no command boundary —
        // no seat, and the per-step degenerate-result-set synthesis below mirrors
        // Adapter#1's statement-less result set. `Terminate` (no server reply at
        // all) is not pull-drivable; reaching one reports not-ready.
        match &step.request {
            ClientRequest::SimpleQuery(_) | ClientRequest::Ping => {}
            ClientRequest::Prepare(_) => active.begin_parse(),
            ClientRequest::DescribeStatement => active.begin_describe_statement(),
            ClientRequest::DescribePortal => active.begin_describe_portal(),
            ClientRequest::BindExecute(_) => active.begin_bind_execute(&described_oids),
            ClientRequest::BindExecuteRowLimited { .. } => {
                active.begin_bind_execute_row_limited(&described_oids)
            }
            ClientRequest::ResumeExecute => active.begin_execute(&described_oids),
            ClientRequest::CloseStatement => active.begin_close(),
            ClientRequest::ExecutePreparedDemo(_) => {
                active.begin_parse_bind_execute(&DEMO_RESULT_OIDS)
            }
            ClientRequest::Terminate => return failed_run(Vec::new()),
        }

        let mut scratch = drive_step(
            &mut active,
            step,
            transcript.chunk_schedule,
            &mut params,
            &mut notices,
            &mut notifications,
        );

        // A `Describe` (statement or portal) recovers the result-column OIDs
        // into its degenerate (tagless) result set; thread them into the next
        // `Bind`+`Execute` or bare-`Execute` resume (the Execute reply re-sends
        // no `RowDescription`). Captured before `scratch.result_sets` is consumed.
        if matches!(
            step.request,
            ClientRequest::DescribeStatement | ClientRequest::DescribePortal
        ) {
            described_oids = match scratch.result_sets.last() {
                Some(rs) => rs.type_oids.clone(),
                None => Vec::new(),
            };
        }

        outcome = match scratch.step_err {
            Some(err) => Err(err),
            None => {
                // A step that reached its boundary with no command-complete (a
                // bare-`Sync` `Ping`) delivered no result set; synthesise the
                // degenerate one Adapter#1 produces for a statement-less
                // `ReadyForQuery`. Never fires for a step that completes a command
                // (every such step delivers at least one result set).
                if scratch.result_sets.is_empty() {
                    scratch.result_sets.push(ObservedResultSet::default());
                }
                Ok(ObservedOk {
                    result_sets: scratch.result_sets,
                    copy_out: scratch.copy_out,
                })
            }
        };
        if let Some(end) = scratch.terminal {
            terminal = end;
            break;
        }
    }

    ObservedRun {
        // The response-driven pull engine emits no request frames; the
        // differential compares the response projection, not client bytes.
        client_bytes: Vec::new(),
        outcome,
        notices,
        parameter_statuses: observe_param_statuses(&params),
        unknown_parameter_status_count: u32::from(params.n_unknown_dropped),
        notifications,
        backend_pid: Some(backend_pid),
        tx_status: map_tx_status(active.tx_status()),
        terminal,
    }
}

/// Drive one step's scripted reply through the active engine, folding events
/// into a [`PullStep`] (and the run-level notice/notify/param accumulators).
fn drive_step(
    active: &mut ActiveEngine,
    step: &Step,
    schedule: bsql_corpus::ChunkSchedule,
    params: &mut SessionParams,
    notices: &mut Vec<ObservedNotice>,
    notifications: &mut Vec<ObservedNotify>,
) -> PullStep {
    let mut scratch = PullStep::new();
    let mut chunks = split_into_chunks(&step.server_reply, schedule).into_iter();
    loop {
        match pull_one(active) {
            PullEvent::NeedMore => match chunks.next() {
                Some(chunk) => {
                    if !feed_chunk_active(active, &chunk) {
                        scratch.step_err =
                            Some(ObservedErr::Protocol(ProtocolFailureKind::TransportExhausted));
                        break;
                    }
                }
                None => {
                    scratch.step_err =
                        Some(ObservedErr::Protocol(ProtocolFailureKind::TransportExhausted));
                    break;
                }
            },
            PullEvent::Idle => break,
            PullEvent::Close => {
                if scratch.step_err.is_none() {
                    scratch.step_err = Some(ObservedErr::Protocol(ProtocolFailureKind::Unclassified));
                }
                scratch.terminal = Some(ObservedStatus::Errored(TerminalErrorKind::Protocol));
                break;
            }
            PullEvent::Deliver {
                command_tag,
                affected_rows,
                column_names,
                type_oids,
            } => {
                scratch.result_sets.push(ObservedResultSet {
                    command_tag,
                    column_names,
                    type_oids,
                    rows: core::mem::take(&mut scratch.pending_rows),
                    affected_rows,
                    portal_suspended: false,
                });
            }
            PullEvent::Suspended {
                column_names,
                type_oids,
            } => {
                // A suspended row-limited Execute: no command tag, no affected
                // count, the portal stays open. The rows fetched so far are the
                // prefix; the result set is flagged suspended.
                scratch.result_sets.push(ObservedResultSet {
                    command_tag: String::new(),
                    column_names,
                    type_oids,
                    rows: core::mem::take(&mut scratch.pending_rows),
                    affected_rows: None,
                    portal_suspended: true,
                });
            }
            PullEvent::Row(cells) => scratch.pending_rows.push(cells),
            PullEvent::RowChunk | PullEvent::RowChunkEnd | PullEvent::CopyDone => {}
            PullEvent::Notice(maybe) => {
                if let Some(notice) = maybe {
                    notices.push(notice);
                }
            }
            PullEvent::Notify(maybe) => {
                if let Some(notify) = maybe {
                    notifications.push(notify);
                }
            }
            PullEvent::ParamStatus(raw) => {
                if let Some((key, value)) = split_parameter_status(&raw) {
                    params.set(key, value);
                }
            }
            PullEvent::CopyData(bytes) => scratch.copy_out.push(bytes),
            // A recoverable server error: record the cause, keep draining to the
            // trailing RFQ (the connection survives a query-level error).
            PullEvent::Fail(err) => {
                if scratch.step_err.is_none() {
                    scratch.step_err = Some(err);
                }
            }
        }
    }
    scratch
}

/// Reach an active engine through the canonical trust handshake.
fn setup_active() -> Option<ActiveEngine> {
    let user = Ident::try_from_str("corpus").ok()?;
    let mut send_buf = SendBuf::new();
    let mut conn = ConnectingEngine::start(&mut send_buf, &user, None, None, Credentials::Trust).ok()?;
    let mut chunks =
        split_into_chunks(&canonical_handshake_reply(), bsql_corpus::ChunkSchedule::AllAtOnce)
            .into_iter();
    loop {
        match conn.next_auth_event(&mut send_buf) {
            AuthEvent::Ready => break,
            AuthEvent::Fail(_) => return None,
            AuthEvent::ParamStatus(_)
            | AuthEvent::AuthCleartext
            | AuthEvent::AuthMd5 { .. }
            | AuthEvent::AuthSaslContinue(_) => {}
            AuthEvent::NeedMore => match chunks.next() {
                Some(chunk) => {
                    if !feed_chunk(&mut conn, &chunk) {
                        return None;
                    }
                }
                None => return None,
            },
        }
    }
    conn.into_active().ok()
}

/// Feed one scripted chunk into the active engine's ingest buffer. Returns
/// `false` on a buffer-full / commit failure.
fn feed_chunk_active(active: &mut ActiveEngine, chunk: &[u8]) -> bool {
    let mut fed = 0usize;
    while fed < chunk.len() {
        let remaining = match chunk.get(fed..) {
            Some(rest) => rest,
            None => return false,
        };
        let slot = match active.read_slot(remaining.len()) {
            Ok(slot) => slot,
            Err(_) => return false,
        };
        if slot.is_empty() {
            return false;
        }
        let n = slot.len().min(remaining.len());
        let (Some(dst), Some(src)) = (slot.get_mut(..n), remaining.get(..n)) else {
            return false;
        };
        dst.copy_from_slice(src);
        if active.commit(n).is_err() {
            return false;
        }
        fed = fed.saturating_add(n);
    }
    true
}

/// Parse a `DataRow` body (column count `i16`, then per-column `(len i32,
/// bytes)`) into raw per-column cells. `len = -1` is SQL NULL (`None`); `len = 0`
/// is an empty-but-non-NULL `Some(Vec::new())`, distinct from NULL.
fn parse_data_row(body: &[u8]) -> Vec<Option<Vec<u8>>> {
    let mut cells = Vec::new();
    let Some((count_bytes, mut rest)) = body.split_first_chunk::<2>() else {
        return cells;
    };
    let Ok(n) = usize::try_from(i16::from_be_bytes(*count_bytes)) else {
        return cells;
    };
    for _ in 0..n {
        let Some((len_bytes, after)) = rest.split_first_chunk::<4>() else {
            break;
        };
        rest = after;
        match usize::try_from(i32::from_be_bytes(*len_bytes)) {
            // Non-negative length: a (possibly empty) present value.
            Ok(len) => {
                let Some(cell) = rest.get(..len) else {
                    break;
                };
                cells.push(Some(cell.to_vec()));
                let Some(next) = rest.get(len..) else {
                    break;
                };
                rest = next;
            }
            // Negative length: SQL NULL.
            Err(_) => cells.push(None),
        }
    }
    cells
}

/// Parse the field list shared by `ErrorResponse` / `NoticeResponse`:
/// `(field_byte, text\0)*` terminated by a `\0`.
fn parse_diagnostic_fields(body: &[u8]) -> Vec<(u8, String)> {
    let mut fields = Vec::new();
    let mut rest = body;
    while let Some((&field_tag, after)) = rest.split_first() {
        if field_tag == 0 {
            break; // field-list terminator
        }
        let Some(nul) = after.iter().position(|&byte| byte == 0) else {
            break;
        };
        let Some(text) = after.get(..nul) else {
            break;
        };
        fields.push((field_tag, String::from_utf8_lossy(text).into_owned()));
        let Some(next) = after.get(nul.saturating_add(1)..) else {
            break;
        };
        rest = next;
    }
    fields
}

/// Find the first value for a diagnostic field byte.
fn diagnostic_field(fields: &[(u8, String)], want: u8) -> Option<String> {
    fields
        .iter()
        .find(|(tag, _)| *tag == want)
        .map(|(_, text)| text.clone())
}

/// A required diagnostic field, or the empty string when absent. Written as an
/// explicit scan (not `unwrap_or_default`) — the silent-substitution combinator
/// family is banned project-wide.
fn diagnostic_field_or_empty(fields: &[(u8, String)], want: u8) -> String {
    for (tag, text) in fields {
        if *tag == want {
            return text.clone();
        }
    }
    String::new()
}

/// Parse a `NoticeResponse` body into the observable notice (severity `S`,
/// SQLSTATE `C`, message `M`).
fn parse_diagnostic_notice(body: &[u8]) -> Option<ObservedNotice> {
    let fields = parse_diagnostic_fields(body);
    Some(ObservedNotice {
        severity: diagnostic_field_or_empty(&fields, b'S'),
        sqlstate: diagnostic_field_or_empty(&fields, b'C').trim().to_string(),
        message: diagnostic_field_or_empty(&fields, b'M'),
    })
}

/// Parse an `ErrorResponse` body into the observable server error. The current
/// engine surfaces severity/SQLSTATE/message/detail/hint; the remaining PG
/// §55.7 fields are pinned absent (`None`) to match Adapter#1.
fn parse_server_error(body: &[u8]) -> ObservedErr {
    let fields = parse_diagnostic_fields(body);
    ObservedErr::Server {
        sqlstate: diagnostic_field_or_empty(&fields, b'C'),
        severity: diagnostic_field(&fields, b'S'),
        message: diagnostic_field_or_empty(&fields, b'M'),
        detail: diagnostic_field(&fields, b'D'),
        hint: diagnostic_field(&fields, b'H'),
        position: None,
        schema: None,
        table: None,
        column: None,
        constraint: None,
    }
}

/// Parse a `NotificationResponse` body: `pid i32`, `channel\0`, `payload\0`.
fn parse_notification(body: &[u8]) -> Option<ObservedNotify> {
    let (pid_bytes, rest) = body.split_first_chunk::<4>()?;
    let pid = i32::from_be_bytes(*pid_bytes);
    let nul = rest.iter().position(|&byte| byte == 0)?;
    let channel = String::from_utf8_lossy(rest.get(..nul)?).into_owned();
    let after = rest.get(nul.saturating_add(1)..)?;
    let payload_end = match after.iter().position(|&byte| byte == 0) {
        Some(idx) => idx,
        None => after.len(),
    };
    let payload = after.get(..payload_end)?.to_vec();
    Some(ObservedNotify { pid, channel, payload })
}

// ===========================================================================
// Adapter#2 — active-phase verb twin (real verbs over a scripted Transport)
// ===========================================================================

/// The corpus-local `prepared!` demo query — the SAME SQL text the live adapter
/// (`sans_io::Q_DEMO`) prepares, so the content-addressed statement name, baked
/// Parse template, and Bind prefix are byte-identical. `ExecutePreparedDemo` maps
/// to the `query_params` macro-execute verb over this query.
static Q_DEMO_VERB: PreparedQuery<(i32,), (i32, &'static str)> =
    prepared!("SELECT id::int4, name::text FROM demo WHERE id = $1::int4");

/// Per-step accumulator for the verb runner (the verb owns the pump loop, so its
/// sink folds surfaces into here rather than a manual pull loop).
struct VerbCapture {
    result_sets: Vec<ObservedResultSet>,
    pending_rows: Vec<Vec<Option<Vec<u8>>>>,
    copy_out: Vec<Vec<u8>>,
    fail: Option<ObservedErr>,
}

impl VerbCapture {
    fn new() -> Self {
        Self {
            result_sets: Vec::new(),
            pending_rows: Vec::new(),
            copy_out: Vec::new(),
            fail: None,
        }
    }
}

/// Whether every step of `transcript` is in the client-bytes-comparable verb
/// subset `{SimpleQuery, Ping, ExecutePreparedDemo}`.
fn all_steps_verb_drivable(transcript: &Transcript) -> bool {
    transcript.steps.iter().all(|s| {
        matches!(
            s.request,
            ClientRequest::SimpleQuery(_)
                | ClientRequest::Ping
                | ClientRequest::ExecutePreparedDemo(_)
        )
    })
}

/// Drive one transcript through the real verbs, capturing the full observable.
fn run_verb(transcript: &Transcript) -> ObservedRun {
    if !matches!(transcript.setup, Setup::ActiveViaTrustHandshake)
        || transcript.steps.is_empty()
        || !all_steps_verb_drivable(transcript)
    {
        return failed_run(Vec::new());
    }

    // The whole scripted reply stream: handshake ++ each step's reply, fragmented
    // per the transcript schedule (so partial-frame resumption is exercised
    // exactly as the pull/A1 twins do).
    let mut script = canonical_handshake_reply();
    for step in &transcript.steps {
        script.extend_from_slice(&step.server_reply);
    }
    let chunks = split_into_chunks(&script, transcript.chunk_schedule);

    let captured: ClientCapture = Arc::new(Mutex::new(Vec::new()));
    let transport = EngineScriptTransport::new(chunks, Arc::clone(&captured));

    let user = match Ident::try_from_str("corpus") {
        Ok(user) => user,
        Err(_) => return failed_run(Vec::new()),
    };
    let body_captured = Arc::clone(&captured);

    let observed = session(
        transport,
        &user,
        None,
        None,
        Credentials::Trust,
        move |mut engine, live| run_verb_body(&mut engine, live, transcript, &body_captured),
    );

    match observed {
        Ok(Some(run)) => run,
        _ => failed_run(Vec::new()),
    }
}

/// The session-scoped verb drive: connect, then each step's verb, then project.
fn run_verb_body<'b>(
    engine: &mut Engine<'b, EngineScriptTransport, NoObserver>,
    live: Live<'b>,
    transcript: &Transcript,
    captured: &ClientCapture,
) -> Option<ObservedRun> {
    let live = match poll_once(engine.connect(live)) {
        Ok(Ok(live)) => live,
        _ => return None,
    };
    // Drop the handshake's outbound wire; from here the capture is the verb wire
    // (mirrors Adapter#1 discarding the trust handshake's client bytes).
    if let Ok(mut sink) = captured.lock() {
        sink.clear();
    }

    let backend_pid = engine.backend_pid().ok();
    let mut params = SessionParams::new();
    let mut notices: Vec<ObservedNotice> = Vec::new();
    let mut notifications: Vec<ObservedNotify> = Vec::new();
    let mut outcome: Result<ObservedOk, ObservedErr> = Ok(ObservedOk::default());
    let mut terminal = ObservedStatus::Ready;

    let mut live = live;
    for step in &transcript.steps {
        let mut cap = VerbCapture::new();
        match drive_verb_step(
            engine,
            live,
            &step.request,
            &mut cap,
            &mut params,
            &mut notices,
            &mut notifications,
        ) {
            Ok(next) => {
                live = next;
                // A bare-`Sync` ping reaches its boundary with no command-complete,
                // so no result set was delivered; synthesise the degenerate result
                // set Adapter#1 produces for a `ReadyForQuery` with no statement.
                if cap.result_sets.is_empty() {
                    cap.result_sets.push(ObservedResultSet::default());
                }
                outcome = Ok(ObservedOk {
                    result_sets: cap.result_sets,
                    copy_out: cap.copy_out,
                });
            }
            Err(engine_err) => {
                let (err, end) = classify_verb_error(engine_err, cap.fail.take());
                outcome = Err(err);
                if let Some(end) = end {
                    terminal = end;
                }
                // The error consumed the linear token; no further steps run.
                break;
            }
        }
    }

    let tx_status = match engine.tx_status() {
        Ok(status) => map_tx_status(status),
        Err(_) => ObservedTxStatus::Idle,
    };
    let client_bytes = match captured.lock() {
        Ok(sink) => sink.clone(),
        Err(_) => Vec::new(),
    };

    Some(ObservedRun {
        client_bytes,
        outcome,
        notices,
        parameter_statuses: observe_param_statuses(&params),
        unknown_parameter_status_count: u32::from(params.n_unknown_dropped),
        notifications,
        backend_pid,
        tx_status,
        terminal,
    })
}

/// Call the verb matching `request`, threading the linear token; its sink folds
/// surfaces into `cap` and the run-level accumulators.
fn drive_verb_step<'b>(
    engine: &mut Engine<'b, EngineScriptTransport, NoObserver>,
    live: Live<'b>,
    request: &ClientRequest,
    cap: &mut VerbCapture,
    params: &mut SessionParams,
    notices: &mut Vec<ObservedNotice>,
    notifications: &mut Vec<ObservedNotify>,
) -> Result<Live<'b>, EngineError<Infallible>> {
    let sink = |surface: Surface<'_>| {
        fold_surface(surface, cap, params, notices, notifications);
        ControlFlow::Continue(())
    };
    match request {
        ClientRequest::SimpleQuery(sql) => flatten_verb(poll_once(engine.simple_query(live, sql, sink))),
        ClientRequest::Ping => flatten_verb(poll_once(engine.ping(live, sink))),
        ClientRequest::ExecutePreparedDemo(id) => {
            flatten_verb(poll_once(engine.query_params(live, &Q_DEMO_VERB, (*id,), sink)))
        }
        // `run_verb` filters to the three kinds above; the rest are unreachable
        // here. Return the token untouched rather than fabricate an error.
        _ => Ok(live),
    }
}

/// Fold one surfaced event into the per-step capture and run-level accumulators —
/// the verb-sink analog of the pull runner's `drive_step` arms.
fn fold_surface(
    surface: Surface<'_>,
    cap: &mut VerbCapture,
    params: &mut SessionParams,
    notices: &mut Vec<ObservedNotice>,
    notifications: &mut Vec<ObservedNotify>,
) {
    match surface {
        Surface::Row(body) => cap.pending_rows.push(parse_data_row(body)),
        Surface::Deliver { tag, oids, names } => {
            let (command_tag, affected_rows) = match tag {
                Some(t) => (t.to_string(), t.rows()),
                None => (String::new(), None),
            };
            cap.result_sets.push(ObservedResultSet {
                command_tag,
                column_names: names.to_vec(),
                type_oids: oids.to_vec(),
                rows: core::mem::take(&mut cap.pending_rows),
                affected_rows,
                portal_suspended: false,
            });
        }
        Surface::Notice(body) => {
            if let Some(notice) = parse_diagnostic_notice(body) {
                notices.push(notice);
            }
        }
        Surface::Notify(body) => {
            if let Some(notify) = parse_notification(body) {
                notifications.push(notify);
            }
        }
        Surface::ParamStatus(body) => {
            if let Some((key, value)) = split_parameter_status(body) {
                params.set(key, value);
            }
        }
        Surface::CopyData(body) => cap.copy_out.push(body.to_vec()),
        Surface::Fail(body) => {
            if cap.fail.is_none() {
                cap.fail = Some(parse_server_error(body));
            }
        }
        Surface::RowChunk(_) | Surface::RowChunkEnd | Surface::CopyDone => {}
    }
}

/// Map a verb error to the observable outcome + (optional) terminal override. A
/// server error is recoverable (terminal stays `Ready`, the surfaced raw error
/// drives the outcome); a protocol violation tears the connection down.
fn classify_verb_error(
    err: EngineError<Infallible>,
    captured_fail: Option<ObservedErr>,
) -> (ObservedErr, Option<ObservedStatus>) {
    match err {
        EngineError::ServerError => {
            let out = match captured_fail {
                Some(server) => server,
                None => ObservedErr::Protocol(ProtocolFailureKind::Unclassified),
            };
            (out, None)
        }
        EngineError::ProtocolViolation => (
            ObservedErr::Protocol(ProtocolFailureKind::Unclassified),
            Some(ObservedStatus::Errored(TerminalErrorKind::Protocol)),
        ),
        // Transport / framing / phase / row-count errors: a non-recoverable
        // failure of the command. Classified, never silently dropped.
        _ => (
            ObservedErr::Protocol(ProtocolFailureKind::Unclassified),
            Some(ObservedStatus::Errored(TerminalErrorKind::Protocol)),
        ),
    }
}

/// Flatten a single-poll verb result; a `Pending` from an always-ready transport
/// is a broken harness, surfaced as a protocol failure (never spun on).
fn flatten_verb(
    polled: Result<Result<Live<'_>, EngineError<Infallible>>, bsql_postgres_proto::engine::SpuriousPending>,
) -> Result<Live<'_>, EngineError<Infallible>> {
    match polled {
        Ok(inner) => inner,
        Err(_) => Err(EngineError::SpuriousPending),
    }
}

/// Canonical PG name for a parsed encoding. Mirrors Adapter#1's mapping; the
/// non-exhaustive wildcard yields a stable placeholder (never a silent drop).
fn encoding_name(enc: &Encoding) -> String {
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
