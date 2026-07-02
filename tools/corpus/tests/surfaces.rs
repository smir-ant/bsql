//! ADAPTER SURFACE INTEGRITY — the new engine's three observable surfaces
//! (`run` connecting / `pull` response-driven / `verb` real-verb) together
//! cover the corpus, each surface partition stays representative, the verb
//! surface genuinely discriminates, and the authored handshake-rejection
//! fixtures reproduce their golden.
//!
//! The per-fixture `engine == golden` regression lives in `seed.rs` /
//! `adversarial.rs`; this file proves the partition INFRASTRUCTURE those rest on:
//!
//! - the handshake-rejection paths (trust success + cleartext / MD5 / SCRAM /
//!   server-error / disconnect) reproduce their pinned golden on the new engine;
//! - each surface partition carries the representatives it exists to exercise;
//! - the verb surface's full-equality comparison has teeth (a tampered request
//!   byte or result is NOT equal to the real run);
//! - every full-corpus fixture is under at least one surface OR is a documented
//!   structural exclusion — no fixture escapes the regression.

#![allow(
    clippy::panic,
    reason = "test harness — a fixture mismatch is a loud assertion failure, the sanctioned test-failure signal; some helper-built fixtures are asserted outside `#[test]` context"
)]
#![allow(
    clippy::print_stdout,
    reason = "the coverage guard prints the fixture -> surface table as its report"
)]

#[path = "../src/engine_transport.rs"]
mod engine_transport;
#[allow(
    dead_code,
    reason = "the surface-integrity tests drive only the connecting and verb surfaces; the shared adapter's pull-surface machinery is exercised by the seed / adversarial / falsifier_a2 crates, not this one — the established shared-`#[path]`-module / different-subset-per-crate pattern"
)]
#[path = "../src/engine_adapter.rs"]
mod engine_adapter;
#[path = "../src/falsify.rs"]
mod falsify;

use bsql_corpus::{
    Adapter, ObservedErr, ObservedOk, ObservedRun, ObservedStatus, ObservedTxStatus,
    ProtocolFailureKind, Setup, TerminalErrorKind, Transcript, corpus, frames,
};
use bsql_postgres_proto::wire::TAG_AUTHENTICATION;

use engine_adapter::EngineAdapter;

/// The backend PID the canonical trust handshake surfaces.
const TRUST_BACKEND_PID: i32 = 4321;

/// The exact `StartupMessage` wire for the corpus's `user=corpus` (no database,
/// no application_name) connection — the recorded client bytes for any scripted
/// startup transcript. Carries the always-sent `client_encoding=UTF8` parameter
/// after `user`.
fn startup_wire() -> Vec<u8> {
    vec![
        0, 0, 0, 42, // length prefix (includes itself)
        0, 3, 0, 0, // protocol version 3.0
        117, 115, 101, 114, 0, // "user\0"
        99, 111, 114, 112, 117, 115, 0, // "corpus\0"
        // "client_encoding\0"
        99, 108, 105, 101, 110, 116, 95, 101, 110, 99, 111, 100, 105, 110, 103, 0,
        85, 84, 70, 56, 0, // "UTF8\0"
        0, // trailing empty-key NUL
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
        parameter_statuses: Vec::new(),        notifications: Vec::new(),
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
        parameter_statuses: Vec::new(),        notifications: Vec::new(),
        backend_pid: None,
        tx_status: ObservedTxStatus::Idle,
        terminal: ObservedStatus::Errored(TerminalErrorKind::Handshake),
    }
}

/// Locally authored handshake fixtures: the trust success path plus the
/// rejection paths the trust (no-password) corpus credentials produce when the
/// server demands cleartext / MD5 / SCRAM auth, sends a server error, or
/// disconnects. These paths are not in the published corpus, so this is the only
/// place the new engine's handshake-rejection observable is pinned.
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
        // the handshake fails.
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
        // handshake failure (the SQLSTATE is not surfaced during connect).
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

/// The handshake/startup subset: the empty-step (handshake-only) fixtures from
/// the seed + adversarial corpus, plus the locally authored handshake fixtures.
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

/// Guard: a query-step fixture must NOT slip into the handshake corpus (the
/// engine's connecting surface is handshake-only, so a stepped fixture would
/// compare a full-run pin against a handshake-only observation and spuriously
/// diverge).
#[test]
fn handshake_corpus_is_step_free() {
    for t in handshake_corpus() {
        assert!(
            t.steps.is_empty(),
            "handshake corpus fixture `{}` carries client steps; the connecting surface is handshake-only",
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

/// The connecting surface reproduces every handshake fixture's pinned golden —
/// the trust success path and the authored cleartext / MD5 / SCRAM /
/// server-error / disconnect rejection paths (and the published empty-step
/// fixtures), so the new engine's handshake-rejection observable is regression
/// pinned even though those paths are not in the published corpus.
#[test]
fn handshake_surface_matches_golden() {
    let engine = EngineAdapter::new();
    for t in handshake_corpus() {
        assert_eq!(
            engine.run(&t),
            t.expect,
            "handshake surface: new engine != golden on `{}`",
            t.name,
        );
    }
}

// ===========================================================================
// SURFACE PARTITIONS — representativeness of the pull and verb subsets.
// ===========================================================================

/// The active subset the pull surface covers — the single source of truth lives
/// in [`falsify`], shared with the falsifier so the two tests measure the
/// identical partition.
fn active_pull_corpus() -> Vec<Transcript> {
    falsify::active_pull_corpus()
}

/// The client-bytes-comparable subset the verb surface covers — the single
/// source of truth lives in [`falsify`], shared with the falsifier.
fn verb_client_byte_corpus() -> Vec<Transcript> {
    falsify::verb_client_byte_corpus()
}

/// Guard: the active subset is non-empty and carries the ROW/NOTICE/NOTIFY/PARAM
/// /COPY representatives the pull surface is meant to exercise.
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
        // Extended query protocol.
        "prepare_describe_bind_select", // Parse + Describe(rows) + Bind/Execute SELECT
        "parse_describe_nodata",        // Parse + Describe(NoData)
        "prepare_bind_dml",             // Parse + Describe(NoData) + Bind/Execute DML
        "prepare_close",                // Parse + Describe + Close
        "prepared_macro",               // combined Parse+Bind+Execute macro path
        "portal_suspend_row_limited",   // row-limited Execute → PortalSuspended
        "portal_resume_after_suspend",  // Describe(PORTAL) + bare-Execute resume
        "oversize_command_complete",    // Sub-B oversize CommandComplete (tag from prefix)
        "large_simple_query_sql",       // SQL > the bounded outbound frame builder
        "oversize_wide_row_description", // Sub-C oversize RowDescription accumulate (chunked)
        "error_then_success_same_connection", // recover: error then success, same connection
        "recovery_window_notice_and_param", // notice + param in the recovery window surface
    ] {
        assert!(
            corpus.iter().any(|t| t.name == required),
            "active pull subset missing representative fixture `{required}`",
        );
    }
    // The flattening quirk fixture is deliberately excluded.
    assert!(
        !corpus.iter().any(|t| t.name == "multi_statement_select"),
        "multi_statement_select (row-first batch flattening quirk) must stay out of the pull subset",
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

/// Guard: the verb subset is non-empty and carries each representative the verb
/// surface exists to exercise — a row-returning SimpleQuery, a bare Ping (the
/// degenerate command boundary), the prepared-macro path, multi-statement
/// delineation, a recoverable server error, and a protocol teardown.
#[test]
fn verb_client_byte_corpus_is_representative() {
    let corpus = verb_client_byte_corpus();
    assert!(
        corpus.len() >= 8,
        "expected the verb subset to be non-trivial, found {}",
        corpus.len(),
    );
    for required in [
        "simple_query_select_rows",      // SimpleQuery rows
        "ping",                          // Ping (bare RFQ, degenerate boundary)
        "prepared_macro",                // ExecutePreparedDemo (query_params)
        "terminate",                     // graceful close (Terminate frame → Closed)
        "multi_statement_delineated",    // per-statement delineation
        "copy_out",                      // COPY OUT
        "server_error_recovers",         // recoverable server error (single step)
        "adversarial_second_row_description", // protocol teardown
        "large_simple_query_sql",        // SQL > the bounded outbound frame builder
        "oversize_wide_row_description", // Sub-C oversize RowDescription accumulate (chunked)
        "error_then_success_same_connection", // recover exercised via the real verb surface
        "recovery_window_notice_and_param", // recovery-window notice + param surface via verb drain
    ] {
        assert!(
            corpus.iter().any(|t| t.name == required),
            "verb subset missing representative fixture `{required}`",
        );
    }
    // The flattening quirk must stay out (it diverges by construction).
    assert!(
        !corpus.iter().any(|t| t.name == "multi_statement_select"),
        "multi_statement_select (row-first batch flattening quirk) must stay out of the verb subset",
    );
    // Ping IS in the verb subset (unlike the pull subset) — the verb path
    // synthesises the degenerate result set the frozen golden carries.
    assert!(
        corpus.iter().any(|t| t.name == "ping"),
        "ping must be in the verb subset (its client bytes are comparable)",
    );
}

/// Teeth: the verb surface's full-equality comparison is sensitive to BOTH the
/// request wire and the surfaced result. A tampered client-byte stream or a
/// tampered result set is NOT equal to the verb run — so an injected divergence
/// in a verb's wire or result fails the regression.
#[test]
fn verb_surface_has_teeth() {
    let engine = EngineAdapter::new();
    let select = corpus::seed()
        .into_iter()
        .find(|t| t.name == "simple_query_select_rows")
        .expect("simple_query_select_rows fixture present");

    let v = engine.verb(&select);

    // The verb path actually emitted the request wire (a simple-query frame).
    assert!(
        !v.client_bytes.is_empty(),
        "verb surface must capture the request wire",
    );
    assert_eq!(
        v.client_bytes.first().copied(),
        Some(b'Q'),
        "a simple-query verb must emit a 'Q' frame",
    );

    // Tooth 1: a single flipped client byte breaks the byte-identity comparison.
    let mut wire_tampered = v.clone();
    if let Some(last) = wire_tampered.client_bytes.last_mut() {
        *last ^= 0xFF;
    }
    assert_ne!(
        v, wire_tampered,
        "client-byte comparison must catch a flipped request byte",
    );

    // Tooth 2: a tampered command tag breaks the result comparison.
    let mut result_tampered = v.clone();
    if let Ok(ok) = &mut result_tampered.outcome
        && let Some(rs) = ok.result_sets.first_mut()
    {
        rs.command_tag.push_str("_TAMPERED");
    }
    assert_ne!(
        v, result_tampered,
        "result comparison must catch a tampered command tag",
    );
}

// ===========================================================================
// FULL-CORPUS COVERAGE GUARD — every fixture is under at least one surface, or
// is a DOCUMENTED structural exclusion. A future fixture that escapes every
// surface without an explicit structural reason fails here, so a new fixture
// cannot silently slip past the engine regression.
// ===========================================================================

/// The set of fixture names some surface covers — the union of the
/// handshake-only, pull, and verb subsets (all from the single source of truth
/// in [`falsify`]).
fn surface_covered_names() -> std::collections::BTreeSet<&'static str> {
    let mut covered = std::collections::BTreeSet::new();
    for t in falsify::handshake_only_corpus() {
        covered.insert(t.name);
    }
    for t in falsify::active_pull_corpus() {
        covered.insert(t.name);
    }
    for t in falsify::verb_client_byte_corpus() {
        covered.insert(t.name);
    }
    covered
}

#[test]
fn full_corpus_coverage_no_fixture_escapes() {
    let covered = surface_covered_names();
    let excluded: std::collections::BTreeSet<&str> = falsify::STRUCTURAL_EXCLUSIONS
        .iter()
        .map(|(name, _)| *name)
        .collect();

    // Per-surface membership, for the coverage table + the per-fixture audit.
    let pull: std::collections::BTreeSet<&str> = falsify::active_pull_corpus()
        .iter()
        .map(|t| t.name)
        .collect();
    let verb: std::collections::BTreeSet<&str> = falsify::verb_client_byte_corpus()
        .iter()
        .map(|t| t.name)
        .collect();
    let handshake: std::collections::BTreeSet<&str> = falsify::handshake_only_corpus()
        .iter()
        .map(|t| t.name)
        .collect();

    // The coverage report: fixture -> covering surfaces.
    println!("\n=== FULL-CORPUS SURFACE COVERAGE ===");
    println!("{:<36} engine surfaces", "fixture");
    println!("{}", "-".repeat(72));
    for t in falsify::full_corpus() {
        let mut tags: Vec<&str> = Vec::new();
        if handshake.contains(t.name) {
            tags.push("handshake");
        }
        if pull.contains(t.name) {
            tags.push("pull");
        }
        if verb.contains(t.name) {
            tags.push("verb");
        }
        let label = if tags.is_empty() {
            "EXCLUDED (structural)".to_string()
        } else {
            tags.join(" + ")
        };
        println!("{:<36} {label}", t.name);
    }
    println!("{}", "-".repeat(72));

    // 1. Every full-corpus fixture is covered by at least one surface OR is a
    //    documented structural exclusion — never neither (a silent escape).
    for t in falsify::full_corpus() {
        let is_covered = covered.contains(t.name);
        let is_excluded = excluded.contains(t.name);
        assert!(
            is_covered || is_excluded,
            "fixture `{}` is in NO engine surface subset and is not a documented \
             structural exclusion — it escapes the engine regression. Add it to a \
             surface subset, or add a STRUCTURAL (not convenience) exclusion to \
             falsify::STRUCTURAL_EXCLUSIONS.",
            t.name,
        );
        // 2. An exclusion must name a GENUINELY uncovered fixture: a fixture that
        //    is both covered and listed is a stale/redundant exclusion.
        assert!(
            !(is_covered && is_excluded),
            "fixture `{}` is both covered by a surface subset AND listed as a \
             structural exclusion — remove the stale exclusion",
            t.name,
        );
    }

    // 3. Every structural-exclusion entry names a real corpus fixture and carries
    //    a reason (no typo'd / unreasoned allowlist entry).
    let corpus_names: std::collections::BTreeSet<&str> =
        falsify::full_corpus().iter().map(|t| t.name).collect();
    for (name, reason) in falsify::STRUCTURAL_EXCLUSIONS {
        assert!(
            corpus_names.contains(name),
            "structural exclusion `{name}` names no corpus fixture (stale allowlist entry)",
        );
        assert!(
            !reason.trim().is_empty(),
            "structural exclusion `{name}` carries no reason",
        );
    }

    // 4. The guard is not vacuous: the corpus is non-trivial and the documented
    //    exclusions are exactly the one known structural gap.
    assert!(
        falsify::full_corpus().len() >= 30,
        "expected a non-trivial corpus, found {}",
        falsify::full_corpus().len(),
    );
    let expected_exclusions: std::collections::BTreeSet<&str> =
        ["multi_statement_select"].into_iter().collect();
    assert_eq!(
        excluded, expected_exclusions,
        "the documented structural exclusions changed; review the coverage report \
         and update this guard + the reasons in falsify::STRUCTURAL_EXCLUSIONS",
    );
}
