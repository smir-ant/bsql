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

use bsql_postgres_proto::engine::{AuthEvent, ConnectingEngine};
use bsql_postgres_proto::{Credentials, Encoding, Ident, SessionParams, TxStatus};

use bsql_corpus::adapter::Adapter;
use bsql_corpus::frames;
use bsql_corpus::observed::{
    ObservedErr, ObservedOk, ObservedRun, ObservedStatus, ObservedTxStatus, ProtocolFailureKind,
    TerminalErrorKind,
};
use bsql_corpus::transcript::{Setup, Transcript};
use bsql_corpus::transport::split_into_chunks;

/// The backend PID pinned by the canonical trust handshake — mirrors the
/// constant Adapter#1 surfaces for `ActiveViaTrustHandshake` transcripts.
const TRUST_BACKEND_PID: i32 = 4321;

/// Adapter over the new engine's connecting path. Handshake-only.
#[derive(Debug, Clone, Copy)]
pub struct EngineAdapter;

impl EngineAdapter {
    /// Construct the handshake-only engine adapter.
    #[must_use]
    pub fn new() -> Self {
        Self
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
    let mut engine = match ConnectingEngine::start(&user, None, None, Credentials::Trust) {
        Ok(engine) => engine,
        Err(_) => return failed_run(Vec::new()),
    };

    let mut chunks = split_into_chunks(&server_bytes, transcript.chunk_schedule).into_iter();
    let mut params = SessionParams::new();
    let mut ready = false;

    loop {
        // The borrow of the lent frame ends with each match arm; capture only
        // the owned signal needed after it.
        let need_more = match engine.next_auth_event() {
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
    // appended to the startup packet). Discarded for the canonical trust setup.
    let client_bytes = if record_client {
        engine.client_bytes().to_vec()
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
