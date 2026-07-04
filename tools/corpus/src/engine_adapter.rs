//! The bridge from a transcript to the `bsql_postgres_proto::engine` under test.
//!
//! [`EngineAdapter`] exposes three observable surfaces, each driving a
//! transcript's scripted server bytes and returning the observable
//! [`ObservedRun`] the regression compares against the pinned golden:
//!
//! - [`EngineAdapter::run`] — the connecting path ([`ConnectingEngine`]):
//!   handshake-only, surfacing the client startup/auth bytes, parameter
//!   statuses, backend pid, transaction status, and terminal. It feeds the
//!   fixture bytes through the engine's single-residence `read_slot`/`commit`
//!   ingest and drives `next_auth_event()` by hand.
//! - [`EngineAdapter::pull`] — the active response surface: it reaches an active
//!   handle through the trust handshake then hand-feeds each step's server
//!   frames, surfacing the full response (result sets, notices, notifications,
//!   parameter statuses, copy-out, transaction status, terminal) but no client
//!   wire (it is response-driven).
//! - [`EngineAdapter::verb`] — the active verb surface: it calls the engine's
//!   real verbs over a scripted [`Transport`](bsql_postgres_proto::engine::Transport),
//!   so the captured outbound wire is the verbs' actual request bytes (the full
//!   observable, including `client_bytes`).

use std::sync::{Arc, Mutex};

use bsql_postgres_proto::engine::{
    poll_once, session, ActiveEngine, AuthEvent, CommandStatus, ConnectingEngine, Engine,
    EngineError, Event, Live, NoObserver, Outcome, SendBuf, Surface,
};
use bsql_postgres_proto::prepared::new_prepared_query;
use bsql_postgres_proto::{Credentials, Ident, PreparedQuery, TxStatus};

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
/// constant the pinned golden carries for `ActiveViaTrustHandshake` transcripts.
const TRUST_BACKEND_PID: i32 = 4321;

/// Result-column type OIDs of the corpus-local prepared-statement demo query
/// (`SELECT id::int4, name::text`): `int4` (23) then `text` (25). The macro
/// path re-sends no `RowDescription`, so the executed rows are surfaced against
/// this compile-time schema — threaded into the engine the same way a statement
/// `Describe`'s recovered schema is. A drift here is caught by the regression
/// (the pinned golden carries the macro's real OIDs).
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
    /// The active response surface: it reaches an active handle through the
    /// canonical trust handshake, then feeds each step's scripted server bytes
    /// through the active engine's `read_slot`/`commit` ingest and drives
    /// `next_event()` by hand — no `Transport`, no pump. It captures the
    /// [`ObservedRun`] response fields (per-statement result sets with rows /
    /// tags / OIDs, notices, notifications, parameter statuses, copy-out chunks,
    /// transaction status, terminal) — except `client_bytes`: the pull engine is
    /// response-driven and emits no request frames, so the regression compares
    /// the response projection.
    ///
    /// Scoped to `ActiveViaTrustHandshake` transcripts whose steps are the
    /// pull-drivable request kinds: the simple-query flow plus the extended
    /// query protocol (`Prepare`/`DescribeStatement`/`BindExecute`/
    /// `BindExecuteRowLimited`/`CloseStatement`/`ExecutePreparedDemo`). State is
    /// reconstructed from each request's TAG (seating the engine via its
    /// `begin_*` seam) plus the server frames — no client wire is encoded. A
    /// `Terminate` carries no server reply, so the pull surface records its
    /// `Closed` terminal directly (the observable is fully request-determined);
    /// it is the verb surface that puts the `Terminate` frame on the wire.
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

/// The canonical minimal trust handshake reply (the shape the trust goldens
/// pin): an `AuthenticationOk` + `BackendKeyData` + `ReadyForQuery(idle)` chain
/// with no `ParameterStatus`, so the session's parameter set starts empty.
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
    // count toward the observable (the golden discards them for the canonical
    // trust setup, records them for a scripted startup).
    let (server_bytes, record_client) = match &transcript.setup {
        Setup::ActiveViaTrustHandshake => (canonical_handshake_reply(), false),
        Setup::StartupScript { server_bytes } => (server_bytes.clone(), true),
        Setup::Disconnected => (Vec::new(), true),
    };

    // The corpus connects as user "corpus", no database / application_name,
    // Trust credentials (the corpus has no password configured) — the trust
    // connection the goldens were captured under.
    let user = match Ident::try_from_str("corpus") {
        Ok(user) => user,
        Err(_) => return failed_run(Vec::new()),
    };
    // The sole outbound residence: the startup packet and any auth response are
    // queued here. It is never flushed in this in-memory adapter, so `pending()`
    // accumulates the full client wire (startup ++ auth) the regression reads.
    let mut send_buf = SendBuf::new();
    let mut engine = match ConnectingEngine::start(&mut send_buf, &user, None, &[], Credentials::Trust) {
        Ok(engine) => engine,
        Err(_) => return failed_run(Vec::new()),
    };

    let mut chunks = split_into_chunks(&server_bytes, transcript.chunk_schedule).into_iter();
    let mut params: Vec<(String, String)> = Vec::new();
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
                if let Some(kv) = observe_parameter_status(payload) {
                    params.push(kv);
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
                params,
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
/// slices. `None` when the payload carries no NUL separator (a malformed frame
/// with no key/value structure — no fixture emits one, and there is no partial
/// key to surface).
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

/// Observe one raw `ParameterStatus` frame exactly as the engine lends it: the
/// key/value bytes decoded to owned strings, no normalization, no known-key
/// projection. This is the raw shape the shipped engine surfaces — every GUC the
/// server sends, in arrival order, with the wire spelling PG chose.
///
/// Text decode is `from_utf8_lossy` — the same idiom the sibling diagnostic /
/// notification parsers use: an invalid byte becomes a visible U+FFFD rather
/// than silently dropping the parameter (PG GUC values are ASCII in practice, so
/// this never fires for the fixtures). A payload with no NUL separator has no
/// (key, value) to surface, so `None` is skipped by the caller — mirroring how a
/// malformed `NotificationResponse` is skipped in [`parse_notification`].
fn observe_parameter_status(payload: &[u8]) -> Option<(String, String)> {
    let (key, value) = split_parameter_status(payload)?;
    Some((
        String::from_utf8_lossy(key).into_owned(),
        String::from_utf8_lossy(value).into_owned(),
    ))
}

/// A `Ready` observed run reached via the handshake. `parameter_statuses` is the
/// raw (key, value) list the engine lent, in arrival order.
fn ready_run(
    client_bytes: Vec<u8>,
    parameter_statuses: Vec<(String, String)>,
    backend_pid: i32,
    tx_status: TxStatus,
) -> ObservedRun {
    ObservedRun {
        client_bytes,
        outcome: Ok(ObservedOk::default()),
        notices: Vec::new(),
        parameter_statuses,
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

// ===========================================================================
// Active-phase pull surface
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
    let mut params: Vec<(String, String)> = Vec::new();
    let mut notices: Vec<ObservedNotice> = Vec::new();
    let mut notifications: Vec<ObservedNotify> = Vec::new();
    let mut outcome: Result<ObservedOk, ObservedErr> = Ok(ObservedOk::default());
    let mut terminal = ObservedStatus::Ready;

    // The result-column type OIDs recovered from the most recent statement
    // `Describe`. The Execute reply re-sends no `RowDescription`, so the OIDs of
    // a `Bind`+`Execute`'s rows come from the preceding `Describe` — threaded
    // into the engine via `begin_bind_execute`.
    let mut described_oids: Vec<u32> = Vec::new();

    // Mirror the engine's per-connection prepared-statement cache for the macro
    // path: the FIRST `ExecutePreparedDemo` on this connection is a cache MISS
    // (Close+Parse+Bind+Execute), later ones are HITs (bare Bind+Execute). The
    // verb surface makes this decision inside `query_params`; the pull surface
    // reconstructs it here so the two surfaces stay response-equivalent.
    let mut demo_parsed = false;

    for step in &transcript.steps {
        // Seat the engine into the awaiting-state matching this request before
        // draining its reply — the response-driven analog of a push. SimpleQuery
        // needs no seat (`Idle` is the awaiting-first-response state); each
        // extended-protocol verb seats its matching state. `Ping` is a bare
        // `Sync` whose `ReadyForQuery` lands at `Idle` with no command boundary —
        // no seat, and the per-step degenerate-result-set synthesis below mirrors
        // the golden's statement-less result set.
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
            // Cache MISS (first use) leads with a Close(statement) before the
            // Parse (idempotent re-Parse) → seat close-parse-bind-execute (reply
            // leads with CloseComplete then ParseComplete). A HIT (later use) is a
            // bare Bind+Execute → seat bind-execute (reply leads with BindComplete).
            ClientRequest::ExecutePreparedDemo(_) => {
                if demo_parsed {
                    active.begin_bind_execute(&DEMO_RESULT_OIDS);
                } else {
                    active.begin_close_parse_bind_execute(&DEMO_RESULT_OIDS);
                    demo_parsed = true;
                }
            }
            // A client-initiated graceful close carries no server reply, so there
            // is nothing to pull: the observable is fully determined by the
            // request. Record the Closed terminal directly (the response-side
            // analogue of the engine setting Closed on the terminate push) and end
            // the run — `outcome` stays `Ok(default)`, matching the golden's pin.
            ClientRequest::Terminate => {
                terminal = ObservedStatus::Closed;
                break;
            }
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
                // degenerate one the golden carries for a statement-less
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
        // regression compares the response projection, not client bytes.
        client_bytes: Vec::new(),
        outcome,
        notices,
        parameter_statuses: params,
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
    params: &mut Vec<(String, String)>,
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
                if let Some(kv) = observe_parameter_status(&raw) {
                    params.push(kv);
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
    let mut conn = ConnectingEngine::start(&mut send_buf, &user, None, &[], Credentials::Trust).ok()?;
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
/// §55.7 fields are pinned absent (`None`) to match the golden.
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
// Active-phase verb surface (real verbs over a scripted Transport)
// ===========================================================================

/// The corpus-local demo query — its SQL text fixes the content-addressed
/// statement name, baked Parse template, and Bind prefix the goldens pin.
/// `ExecutePreparedDemo` maps to the `query_params` macro-execute verb over this
/// query.
///
/// Minted through `new_prepared_query`, the sole validating constructor for a
/// `PreparedQuery` (the compile-checked `query!` macro routes through it in
/// consumer crates that have a migration catalog; this corpus has none, so it
/// hands the constructor the wire bytes directly). The `build_parse_template` /
/// `build_bind_prefix` helpers re-derive the exact PG frame layout, and the
/// constructor's const validator rejects any OID drift between the baked
/// template and the declared parameter tuple. The statement name is the
/// SHA-256-96 content address of the SQL, so the driven wire is byte-for-byte
/// identical to what a query macro bakes — which is why the goldens are stable.
const DEMO_SQL: &str = "SELECT id::int4, name::text FROM demo WHERE id = $1::int4";
const DEMO_STMT: &str = "bsql_p_a6ff70d2d94bc34772d4a4ba";
const DEMO_PARAM_OIDS: &[u32] = &[23];
const DEMO_PARSE_LEN: usize =
    1 + 4 + DEMO_STMT.len() + 1 + DEMO_SQL.len() + 1 + 2 + 4 * DEMO_PARAM_OIDS.len();
const DEMO_PARSE: [u8; DEMO_PARSE_LEN] =
    build_parse_template::<DEMO_PARSE_LEN>(DEMO_STMT, DEMO_SQL, DEMO_PARAM_OIDS);
const DEMO_BIND_LEN: usize = 1 + DEMO_STMT.len() + 1;
const DEMO_BIND: [u8; DEMO_BIND_LEN] = build_bind_prefix::<DEMO_BIND_LEN>(DEMO_STMT);

static Q_DEMO_VERB: PreparedQuery<(i32,), (i32, &'static str)> =
    new_prepared_query::<(i32,), (i32, &'static str)>(
        DEMO_SQL,
        DEMO_STMT,
        DEMO_PARAM_OIDS,
        &DEMO_RESULT_OIDS,
        &DEMO_PARSE,
        &DEMO_BIND,
    );

/// Re-derive the PG `Parse`-frame template bytes for a statement:
/// `b'P' | len_i32_be | stmt\0 | sql\0 | n_params_i16_be | oid_i32_be × n`.
/// The length field is self-inclusive (covers everything after the tag byte).
const fn build_parse_template<const N: usize>(stmt: &str, sql: &str, oids: &[u32]) -> [u8; N] {
    let mut buf = [0u8; N];
    let stmt_b = stmt.as_bytes();
    let sql_b = sql.as_bytes();
    let len_be = ((N - 1) as u32).to_be_bytes();
    buf[0] = b'P';
    buf[1] = len_be[0];
    buf[2] = len_be[1];
    buf[3] = len_be[2];
    buf[4] = len_be[3];
    let mut i = 5;
    let mut j = 0;
    while j < stmt_b.len() {
        buf[i] = stmt_b[j];
        i += 1;
        j += 1;
    }
    buf[i] = 0;
    i += 1;
    j = 0;
    while j < sql_b.len() {
        buf[i] = sql_b[j];
        i += 1;
        j += 1;
    }
    buf[i] = 0;
    i += 1;
    let n_be = (oids.len() as u16).to_be_bytes();
    buf[i] = n_be[0];
    i += 1;
    buf[i] = n_be[1];
    i += 1;
    j = 0;
    while j < oids.len() {
        let ob = oids[j].to_be_bytes();
        buf[i] = ob[0];
        buf[i + 1] = ob[1];
        buf[i + 2] = ob[2];
        buf[i + 3] = ob[3];
        i += 4;
        j += 1;
    }
    buf
}

/// Re-derive the `Bind`-frame prefix bytes: `empty_portal_NUL | stmt\0`. The
/// param format block, values, and result-format trailer are appended by the
/// engine at frame-build time from the argument tuple's `ParamsWriter`.
const fn build_bind_prefix<const N: usize>(stmt: &str) -> [u8; N] {
    let mut buf = [0u8; N];
    let stmt_b = stmt.as_bytes();
    // buf[0] is the empty-portal NUL (already 0).
    let mut i = 1;
    let mut j = 0;
    while j < stmt_b.len() {
        buf[i] = stmt_b[j];
        i += 1;
        j += 1;
    }
    // Final byte is the stmt-name NUL (already 0).
    buf
}

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
/// subset `{SimpleQuery, Ping, ExecutePreparedDemo, Terminate}`.
fn all_steps_verb_drivable(transcript: &Transcript) -> bool {
    transcript.steps.iter().all(|s| {
        matches!(
            s.request,
            ClientRequest::SimpleQuery(_)
                | ClientRequest::Ping
                | ClientRequest::ExecutePreparedDemo(_)
                | ClientRequest::Terminate
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
    // exactly as the pull surface does).
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
        &[],
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
    // (the trust handshake's client bytes are not part of the observable).
    if let Ok(mut sink) = captured.lock() {
        sink.clear();
    }

    let backend_pid = engine.backend_pid().ok();
    let mut params: Vec<(String, String)> = Vec::new();
    let mut notices: Vec<ObservedNotice> = Vec::new();
    let mut notifications: Vec<ObservedNotify> = Vec::new();
    let mut outcome: Result<ObservedOk, ObservedErr> = Ok(ObservedOk::default());
    let mut terminal = ObservedStatus::Ready;

    let mut live = live;
    for step in &transcript.steps {
        // A graceful close ends the session: the verb consumes the linear token
        // and returns no `Live`, so it cannot thread through `drive_verb_step`.
        // Drive it here, record the Closed terminal, and stop — the `Terminate`
        // frame is captured as the client wire (matching the golden's client bytes).
        if matches!(step.request, ClientRequest::Terminate) {
            terminal = match poll_once(engine.terminate(live)) {
                Ok(Ok(())) => ObservedStatus::Closed,
                // A transport/phase failure on close: classified, never dropped.
                _ => ObservedStatus::Errored(TerminalErrorKind::Protocol),
            };
            break;
        }
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
            Ok(Outcome { live: next, status }) => {
                // Either status returns the token — the connection is alive.
                live = next;
                match status {
                    CommandStatus::Completed => {
                        // A bare-`Sync` ping reaches its boundary with no
                        // command-complete, so no result set was delivered;
                        // synthesise the degenerate result set the golden carries
                        // for a `ReadyForQuery` with no statement.
                        if cap.result_sets.is_empty() {
                            cap.result_sets.push(ObservedResultSet::default());
                        }
                        outcome = Ok(ObservedOk {
                            result_sets: cap.result_sets,
                            copy_out: cap.copy_out,
                        });
                    }
                    CommandStatus::ServerErrored => {
                        // A RECOVERABLE server error: the verb drained the
                        // recovering RFQ and handed the token back, so the
                        // connection survives and the run CONTINUES to the next
                        // step (the recovery the regression must cover). The
                        // surfaced error becomes this step's outcome; a following
                        // step overwrites it (the run's outcome is the last
                        // step's, exactly as the golden projects it). `terminal`
                        // stays `Ready` — the connection is not torn down.
                        outcome = Err(match cap.fail.take() {
                            Some(server) => server,
                            None => ObservedErr::Protocol(ProtocolFailureKind::Unclassified),
                        });
                    }
                }
            }
            Err(engine_err) => {
                let (err, end) = classify_verb_error(engine_err, cap.fail.take());
                outcome = Err(err);
                if let Some(end) = end {
                    terminal = end;
                }
                // A FATAL error consumed the linear token; no further steps run.
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
        parameter_statuses: params,
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
    params: &mut Vec<(String, String)>,
    notices: &mut Vec<ObservedNotice>,
    notifications: &mut Vec<ObservedNotify>,
) -> Result<Outcome<'b, CommandStatus>, EngineError<Infallible>> {
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
        // here. Return the token untouched (a clean completion) rather than
        // fabricate an error.
        _ => Ok(Outcome {
            live,
            status: CommandStatus::Completed,
        }),
    }
}

/// Fold one surfaced event into the per-step capture and run-level accumulators —
/// the verb-sink analog of the pull runner's `drive_step` arms.
fn fold_surface(
    surface: Surface<'_>,
    cap: &mut VerbCapture,
    params: &mut Vec<(String, String)>,
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
            if let Some(kv) = observe_parameter_status(body) {
                params.push(kv);
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

/// Map a FATAL verb error to the observable outcome + terminal override. A
/// recoverable server error no longer reaches here — the verb returns it as
/// `Ok(Outcome { status: ServerErrored })`, handled by the caller (the run
/// continues). Every `Err` from a verb now means the connection was torn down,
/// so it sets the `Errored` terminal; a server-error cause surfaced on the sink
/// before the teardown is preferred over the generic protocol-failure marker.
fn classify_verb_error(
    err: EngineError<Infallible>,
    captured_fail: Option<ObservedErr>,
) -> (ObservedErr, Option<ObservedStatus>) {
    match err {
        // A `ServerError` here is the rare drain-during-recovery teardown (a
        // SECOND error while consuming the recovering RFQ), not the common
        // recoverable error (which returns `Ok(ServerErrored)`); a server cause
        // surfaced before the teardown is preferred over the generic marker.
        EngineError::ServerError => {
            let out = match captured_fail {
                Some(server) => server,
                None => ObservedErr::Protocol(ProtocolFailureKind::Unclassified),
            };
            (out, Some(ObservedStatus::Errored(TerminalErrorKind::Protocol)))
        }
        // Protocol teardown / framing / phase / row-count / spurious-pending: a
        // non-recoverable command failure. Classified, never silently dropped.
        _ => (
            ObservedErr::Protocol(ProtocolFailureKind::Unclassified),
            Some(ObservedStatus::Errored(TerminalErrorKind::Protocol)),
        ),
    }
}

/// Flatten a single-poll verb result; a `Pending` from an always-ready transport
/// is a broken harness, surfaced as a protocol failure (never spun on).
fn flatten_verb(
    polled: Result<
        Result<Outcome<'_, CommandStatus>, EngineError<Infallible>>,
        bsql_postgres_proto::engine::SpuriousPending,
    >,
) -> Result<Outcome<'_, CommandStatus>, EngineError<Infallible>> {
    match polled {
        Ok(inner) => inner,
        Err(_) => Err(EngineError::SpuriousPending),
    }
}
