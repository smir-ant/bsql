//! HANDSHAKE DIFFERENTIAL — Adapter#1 (live engine) vs Adapter#2 (new engine).
//!
//! For the handshake/startup subset of the corpus, this asserts the four-way
//! agreement
//!
//! ```text
//! A1.sync == A1.async == A2.handshake == expect
//! ```
//!
//! where A1 is the live `SansIoAdapter` driving `bsql_postgres_core::Session`
//! and A2 is the new `EngineAdapter` driving the strangler engine's connecting
//! path (`bsql_postgres_proto::engine::ConnectingEngine`). Both directions of
//! A1 (sync + async twins) plus the new engine must produce the identical
//! observable `ObservedRun` AND match the committed/authored pin.
//!
//! Scope: only the connecting phase. The fixture set is the empty-step
//! (handshake-only) subset of the seed + adversarial corpus, plus locally
//! authored handshake fixtures that exercise the trust success path and the
//! cleartext / MD5 / SCRAM / server-error rejection paths against the corpus's
//! trust (no-password) credentials. Non-connecting fixtures (those carrying
//! client query steps) are not yet wired into A2 and are excluded here.

#![allow(
    clippy::panic,
    reason = "test harness — a fixture mismatch is a loud assertion failure, the sanctioned test-failure signal"
)]

#[path = "../src/engine_adapter.rs"]
mod engine_adapter;

use bsql_corpus::{
    Adapter, ClientRequest, ObservedErr, ObservedOk, ObservedRun, ObservedStatus, ObservedTxStatus,
    ProtocolFailureKind, SansIoAdapter, Setup, TerminalErrorKind, Transcript, corpus, frames,
};
use bsql_postgres_proto::wire::TAG_AUTHENTICATION;

use engine_adapter::EngineAdapter;

/// The backend PID the canonical trust handshake surfaces.
const TRUST_BACKEND_PID: i32 = 4321;

/// The exact `StartupMessage` wire for the corpus's `user=corpus` (no database,
/// no application_name) connection — the recorded client bytes for any scripted
/// startup transcript.
fn startup_wire() -> Vec<u8> {
    vec![
        0, 0, 0, 21, 0, 3, 0, 0, 117, 115, 101, 114, 0, 99, 111, 114, 112, 117, 115, 0, 0,
    ]
}

/// Build an `Authentication` request frame: tag `R`, 4-byte big-endian
/// sub-code, then method-specific trailing bytes.
fn auth_request(sub_code: i32, extra: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&sub_code.to_be_bytes());
    body.extend_from_slice(extra);
    frames::frame(TAG_AUTHENTICATION.byte(), &body)
}

/// A `Ready` handshake outcome reached via the canonical trust handshake, with
/// no recorded client bytes (the trust setup discards them) and no parameters.
fn ready_trust_expect() -> ObservedRun {
    ObservedRun {
        client_bytes: Vec::new(),
        outcome: Ok(ObservedOk::default()),
        notices: Vec::new(),
        parameter_statuses: Vec::new(),
        unknown_parameter_status_count: 0,
        notifications: Vec::new(),
        backend_pid: Some(TRUST_BACKEND_PID),
        tx_status: ObservedTxStatus::Idle,
        terminal: ObservedStatus::Ready,
    }
}

/// A failed-handshake outcome with the given recorded client bytes.
fn handshake_failed_expect(client_bytes: Vec<u8>) -> ObservedRun {
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

/// Locally authored handshake fixtures: the trust success path plus the
/// rejection paths the trust (no-password) corpus credentials produce when the
/// server demands cleartext / MD5 / SCRAM auth, sends a server error, or
/// disconnects.
fn local_handshake_fixtures() -> Vec<Transcript> {
    vec![
        // Trust handshake to a ready session (no steps): AuthenticationOk +
        // BackendKeyData + ReadyForQuery(idle). Setup bytes are not part of the
        // observable client wire.
        Transcript {
            name: "handshake_trust_ready",
            setup: Setup::ActiveViaTrustHandshake,
            steps: Vec::new(),
            chunk_schedule: bsql_corpus::ChunkSchedule::AllAtOnce,
            expect: ready_trust_expect(),
        },
        // Server demands SCRAM-SHA-256; the trust client cannot satisfy it —
        // the handshake fails identically in both engines.
        Transcript {
            name: "handshake_scram_rejected",
            setup: Setup::StartupScript {
                server_bytes: auth_request(10, b"SCRAM-SHA-256\0\0"),
            },
            steps: Vec::new(),
            chunk_schedule: bsql_corpus::ChunkSchedule::AllAtOnce,
            expect: handshake_failed_expect(startup_wire()),
        },
        // Server demands a cleartext password; the trust client carries none.
        Transcript {
            name: "handshake_cleartext_rejected",
            setup: Setup::StartupScript {
                server_bytes: auth_request(3, &[]),
            },
            steps: Vec::new(),
            chunk_schedule: bsql_corpus::ChunkSchedule::AllAtOnce,
            expect: handshake_failed_expect(startup_wire()),
        },
        // Server demands MD5; the trust client carries no password.
        Transcript {
            name: "handshake_md5_rejected",
            setup: Setup::StartupScript {
                server_bytes: auth_request(5, &[0x01, 0x02, 0x03, 0x04]),
            },
            steps: Vec::new(),
            chunk_schedule: bsql_corpus::ChunkSchedule::AllAtOnce,
            expect: handshake_failed_expect(startup_wire()),
        },
        // Server replies to startup with an ErrorResponse (e.g. role
        // rejection): a handshake-phase server error is classified as a
        // handshake failure (the SQLSTATE is not surfaced during connect) in
        // both engines.
        Transcript {
            name: "handshake_server_error",
            setup: Setup::StartupScript {
                server_bytes: frames::error_response(
                    "FATAL",
                    "28000",
                    "role \"corpus\" does not exist",
                ),
            },
            steps: Vec::new(),
            chunk_schedule: bsql_corpus::ChunkSchedule::AllAtOnce,
            expect: handshake_failed_expect(startup_wire()),
        },
        // Server supplies no bytes and closes: the handshake cannot complete.
        Transcript {
            name: "handshake_disconnected",
            setup: Setup::Disconnected,
            steps: Vec::new(),
            chunk_schedule: bsql_corpus::ChunkSchedule::AllAtOnce,
            expect: handshake_failed_expect(startup_wire()),
        },
    ]
}

/// The handshake/startup subset the differential covers: the empty-step
/// (handshake-only) fixtures from the seed + adversarial corpus, plus the
/// locally authored handshake fixtures.
fn handshake_corpus() -> Vec<Transcript> {
    let mut out: Vec<Transcript> = Vec::new();
    for t in corpus::seed().into_iter().chain(corpus::adversarial()) {
        if t.steps.is_empty() {
            out.push(t);
        }
    }
    out.extend(local_handshake_fixtures());
    out
}

/// Guard: a query-step fixture must NOT slip into the handshake corpus (the new
/// engine's A2 is handshake-only, so a stepped fixture would compare a
/// full-run A1 against a handshake-only A2 and spuriously diverge).
#[test]
fn handshake_corpus_is_step_free() {
    for t in handshake_corpus() {
        assert!(
            t.steps.is_empty(),
            "handshake corpus fixture `{}` carries client steps; A2 is handshake-only",
            t.name,
        );
    }
    // Sanity: the empty-step subset of the published corpus is actually present
    // (startup_with_params + the notice-during-auth adversarial), so the filter
    // is not silently empty.
    let published_empty_step = corpus::seed()
        .into_iter()
        .chain(corpus::adversarial())
        .filter(|t| t.steps.is_empty())
        .count();
    assert!(
        published_empty_step >= 2,
        "expected >=2 empty-step handshake fixtures in the published corpus, found {published_empty_step}",
    );
}

#[test]
fn handshake_differential_a1_a2_agree() {
    let sync = SansIoAdapter::sync();
    let async_twin = SansIoAdapter::async_twin();
    let engine = EngineAdapter::new();

    for t in handshake_corpus() {
        let a1_sync = sync.run(&t);
        let a1_async = async_twin.run(&t);
        let a2 = engine.run(&t);

        // A1 twin equivalence (the live engine agrees with itself).
        assert_eq!(
            a1_sync, a1_async,
            "A1 sync/async twin divergence on `{}`",
            t.name,
        );
        // A1 pins its committed/authored expectation.
        assert_eq!(a1_sync, t.expect, "A1 pin mismatch on `{}`", t.name);
        // The new engine agrees with the live engine on every handshake field.
        assert_eq!(a2, a1_sync, "A2 vs A1 handshake divergence on `{}`", t.name);
        // And therefore matches the pin.
        assert_eq!(a2, t.expect, "A2 pin mismatch on `{}`", t.name);
    }
}

// ===========================================================================
// ACTIVE DIFFERENTIAL — Adapter#1 (live drain/col_next) vs Adapter#2 (pull).
// ===========================================================================

/// The active subset the pull twin covers: `ActiveViaTrustHandshake`
/// transcripts whose every step is a *pull-drivable* request — the simple-query
/// flow plus the extended query protocol (`Prepare`/`DescribeStatement`/
/// `DescribePortal`/`BindExecute`/`BindExecuteRowLimited`/`ResumeExecute`/
/// `CloseStatement`/`ExecutePreparedDemo`). A bind/execute reply re-sends no
/// `RowDescription`, so its column OIDs come from the preceding `Describe` (or
/// the macro's compile-time schema), which the adapter threads from the request
/// tag, not the client wire.
///
/// Exclusions, each for a STRUCTURAL reason (never "rare"/"atypical"):
/// - `multi_statement_select`: the live engine FLATTENS a row-FIRST `;`-batch
///   through its `iter_rows` pull into a single result set, whereas the
///   cleanly-delineated pull surfaces one result set per statement — the two
///   disagree on result-set structure by construction (the documented A1-only
///   flattening quirk).
/// - `Ping` / `Terminate` steps: filtered out by the request-kind set. A `Ping`
///   reply is a bare `ReadyForQuery` with no `CommandComplete`, so the live
///   engine synthesises a degenerate result set the response-driven pull (which
///   emits a result set only per delivered command boundary) does not; a
///   `Terminate` has no server reply at all (a socket close, not a response).
fn active_pull_corpus() -> Vec<Transcript> {
    let mut out: Vec<Transcript> = Vec::new();
    for t in corpus::seed().into_iter().chain(corpus::adversarial()) {
        if !matches!(t.setup, Setup::ActiveViaTrustHandshake) || t.steps.is_empty() {
            continue;
        }
        let all_pull_drivable = t.steps.iter().all(|s| {
            matches!(
                s.request,
                ClientRequest::SimpleQuery(_)
                    | ClientRequest::Prepare(_)
                    | ClientRequest::DescribeStatement
                    | ClientRequest::DescribePortal
                    | ClientRequest::BindExecute(_)
                    | ClientRequest::BindExecuteRowLimited { .. }
                    | ClientRequest::ResumeExecute
                    | ClientRequest::CloseStatement
                    | ClientRequest::ExecutePreparedDemo(_)
            )
        });
        if all_pull_drivable && t.name != "multi_statement_select" {
            out.push(t);
        }
    }
    out
}

/// The response projection compared across A1 and A2(pull): everything an active
/// pull observes EXCEPT `client_bytes` — the response-driven pull engine emits
/// no request frames, so the outbound wire is not part of its observable.
fn response_view(run: &ObservedRun) -> ObservedRun {
    let mut view = run.clone();
    view.client_bytes = Vec::new();
    view
}

/// Guard: the active subset is non-empty and carries the ROW/NOTICE/NOTIFY/PARAM
/// /COPY representatives the pull twin is meant to exercise.
#[test]
fn active_pull_corpus_is_representative() {
    let corpus = active_pull_corpus();
    assert!(
        corpus.len() >= 12,
        "expected the active pull subset to be non-trivial, found {}",
        corpus.len(),
    );
    for required in [
        "simple_query_select_rows",     // ROW
        "notice_during_query",          // NOTICE
        "notification_during_query",    // NOTIFY
        "unknown_parameter_status",     // PARAM
        "copy_out",                     // COPY
        "multi_statement_delineated",   // multi-statement delineation
        "server_error_recovers",        // recoverable error
        // Extended query protocol — the completeness fix's new coverage.
        "prepare_describe_bind_select", // Parse + Describe(rows) + Bind/Execute SELECT
        "parse_describe_nodata",        // Parse + Describe(NoData)
        "prepare_bind_dml",             // Parse + Describe(NoData) + Bind/Execute DML
        "prepare_close",                // Parse + Describe + Close
        "prepared_macro",               // combined Parse+Bind+Execute macro path
        "portal_suspend_row_limited",   // row-limited Execute → PortalSuspended
        "portal_resume_after_suspend",  // Describe(PORTAL) + bare-Execute resume
        "oversize_command_complete",    // Sub-B oversize CommandComplete (tag from prefix)
    ] {
        assert!(
            corpus.iter().any(|t| t.name == required),
            "active pull subset missing representative fixture `{required}`",
        );
    }
    // The flattening quirk fixture is deliberately excluded.
    assert!(
        !corpus.iter().any(|t| t.name == "multi_statement_select"),
        "multi_statement_select (A1 flattening quirk) must stay out of the pull subset",
    );
    // Ping (bare RFQ, no command boundary) and Terminate (no server reply) are
    // not pull-drivable and must stay excluded.
    assert!(
        !corpus.iter().any(|t| t.name == "ping"),
        "ping (bare ReadyForQuery, no command boundary) must stay out of the pull subset",
    );
    assert!(
        !corpus.iter().any(|t| t.name == "terminate"),
        "terminate (no server reply) must stay out of the pull subset",
    );
}

#[test]
fn active_differential_a1_a2_pull_agree() {
    let sync = SansIoAdapter::sync();
    let async_twin = SansIoAdapter::async_twin();
    let engine = EngineAdapter::new();

    for t in active_pull_corpus() {
        let a1_sync = sync.run(&t);
        let a1_async = async_twin.run(&t);
        let a2_pull = engine.pull(&t);

        // A1 twin equivalence + pin (the full observable, including client bytes).
        assert_eq!(a1_sync, a1_async, "A1 sync/async divergence on `{}`", t.name);
        assert_eq!(a1_sync, t.expect, "A1 pin mismatch on `{}`", t.name);

        // The pull twin agrees with the live engine on the RESPONSE projection
        // (rows + tags + OIDs, notices, notifications, parameter statuses,
        // copy-out, transaction status, terminal, outcome) for every fixture.
        assert_eq!(
            response_view(&a2_pull),
            response_view(&a1_sync),
            "A2(pull) vs A1 response divergence on `{}`",
            t.name,
        );
        assert_eq!(
            response_view(&a2_pull),
            response_view(&t.expect),
            "A2(pull) vs pin response divergence on `{}`",
            t.name,
        );
    }
}

