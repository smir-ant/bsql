//! Shared corpus-partition infrastructure: the canonical corpus partitions each
//! engine surface covers, the documented structural exclusions, and the response
//! projection.
//!
//! This is the SINGLE source of truth for which fixture is driven by which
//! engine surface. The surface-integrity tests (`surfaces.rs`), the per-fixture
//! regression (`seed.rs` / `adversarial.rs`), and the falsifier
//! (`falsifier_a2.rs`) all read these functions, so the falsifier measures
//! exactly the subsets the regression proves — a partition cannot drift between
//! the tests.
//!
//! It is a `src/` file compiled INTO each consuming test crate via
//! `#[path = "../src/falsify.rs"] mod falsify;` (the same pattern
//! `engine_adapter.rs` / `engine_transport.rs` use). It is deliberately NOT a
//! `lib.rs` module: nothing in the shipped/dev library depends on it, and
//! compiling it in the test crate keeps it in the test lint context.

#![allow(
    dead_code,
    reason = "shared corpus-partition infrastructure compiled into multiple test crates via `#[path]`; each crate uses a different subset (the regression / surface tests read the corpus partitions, the falsifier reads the mutation battery), so not every item is read in every crate — the established shared-test-helper-module pattern"
)]

use bsql_corpus::{
    corpus, ClientRequest, ObservedErr, ObservedOk, ObservedResultSet, ObservedRun, ObservedStatus,
    ObservedTxStatus, ProtocolFailureKind, Setup, TerminalErrorKind, Transcript,
};

// ===========================================================================
// Corpus + partitions
// ===========================================================================

/// The full corpus the engine validates against: every seed fixture plus the
/// adversarial fixtures, in that order.
#[must_use]
pub fn full_corpus() -> Vec<Transcript> {
    let mut all = corpus::seed();
    all.extend(corpus::adversarial());
    all
}

/// The handshake/startup subset: the empty-step (handshake-only) fixtures of the
/// full corpus, which the engine's connecting surface
/// ([`EngineAdapter::run`](crate::engine_adapter::EngineAdapter)) drives.
///
/// A stepped fixture is NOT here: the connecting surface observes only the
/// handshake outcome, so comparing it against a full-run pin would diverge by
/// construction. This is the real-corpus portion of the surface tests'
/// `handshake_corpus` (the locally authored handshake fixtures live only in
/// `surfaces.rs` and are not part of the validated corpus).
#[must_use]
pub fn handshake_only_corpus() -> Vec<Transcript> {
    full_corpus()
        .into_iter()
        .filter(|t| t.steps.is_empty())
        .collect()
}

/// The active subset the pull surface covers: `ActiveViaTrustHandshake`
/// transcripts whose every step is a *pull-drivable* request — the simple-query
/// flow plus the extended query protocol (`Prepare`/`DescribeStatement`/
/// `DescribePortal`/`BindExecute`/`BindExecuteRowLimited`/`ResumeExecute`/
/// `CloseStatement`/`ExecutePreparedDemo`). A bind/execute reply re-sends no
/// `RowDescription`, so its column OIDs come from the preceding `Describe` (or
/// the macro's compile-time schema), which the adapter threads from the request
/// tag, not the client wire.
///
/// Exclusions, each for a STRUCTURAL reason (never "rare"/"atypical"):
/// - `multi_statement_select`: the old engine FLATTENS a row-FIRST `;`-batch
///   through its `iter_rows` pull into a single result set (frozen in the
///   golden), whereas the cleanly-delineated pull surface produces one result
///   set per statement — the two disagree on result-set structure by
///   construction (the documented old-engine flattening quirk).
///   `multi_statement_delineated` covers clean delineation.
/// - `Ping` / `Terminate` steps: filtered out by the request-kind set. A `Ping`
///   reply is a bare `ReadyForQuery` with no `CommandComplete`, so the golden
///   carries a degenerate result set the response-driven pull does not; a
///   `Terminate` has no server reply at all (a socket close, not a response).
#[must_use]
pub fn active_pull_corpus() -> Vec<Transcript> {
    let mut out: Vec<Transcript> = Vec::new();
    for t in full_corpus() {
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

/// The client-bytes-comparable subset: `ActiveViaTrustHandshake` transcripts
/// whose every step is one of `{SimpleQuery, Ping, ExecutePreparedDemo, Terminate}`
/// — the requests that map 1:1 onto a single bundling verb, so the verbs'
/// outbound wire is byte-comparable against the golden's client bytes.
/// `Terminate` belongs here (not the pull subset): it IS a verb (`terminate`),
/// and the verb surface puts the byte-identical `Terminate` frame on the wire
/// then transitions to the closed phase — the `Closed` terminal the golden
/// carries. The pull surface, which emits no client wire, still reproduces the
/// response-side `Closed` observable for the verb/pull cross-check.
///
/// Exclusions, each STRUCTURAL (never "rare"/"atypical"):
/// - The fine-grained extended fixtures (separate `Prepare`/`DescribeStatement`/
///   `DescribePortal`/`BindExecute`/`ResumeExecute`/`CloseStatement` steps) are
///   filtered out by the request-kind set: each such step is its own Sync, while
///   the bundling verbs (`prepare` = Parse+Describe+1 Sync, `query_params` =
///   Parse+Bind+Execute+1 Sync) decompose the wire differently by construction.
///   Those fixtures stay on the pull surface, which compares the response only.
/// - `multi_statement_select`: the old engine FLATTENS a row-first `;`-batch
///   into one result set (frozen in the golden); the verb's clean per-statement
///   delineation produces several — the same documented old-engine flattening
///   quirk the pull subset excludes (a result divergence by construction, not a
///   wire one).
#[must_use]
pub fn verb_client_byte_corpus() -> Vec<Transcript> {
    let mut out: Vec<Transcript> = Vec::new();
    for t in full_corpus() {
        if !matches!(t.setup, Setup::ActiveViaTrustHandshake) || t.steps.is_empty() {
            continue;
        }
        let all_byte_comparable = t.steps.iter().all(|s| {
            matches!(
                s.request,
                ClientRequest::SimpleQuery(_)
                    | ClientRequest::Ping
                    | ClientRequest::ExecutePreparedDemo(_)
                    | ClientRequest::Terminate
            )
        });
        if all_byte_comparable && t.name != "multi_statement_select" {
            out.push(t);
        }
    }
    out
}

/// Fixtures NO engine surface can drive, each excluded for a STRUCTURAL
/// (construction) reason — never frequency/convenience. Each entry is
/// `(fixture name, reason)`; the coverage guard asserts every entry names a real
/// corpus fixture that is genuinely in none of the surface subsets, so a stale
/// or convenience exclusion fails the build.
pub const STRUCTURAL_EXCLUSIONS: &[(&str, &str)] = &[
    (
        "multi_statement_select",
        "old-engine row-first `;`-batch flattening quirk: the golden was \
         captured from the old engine, which flattens a row-FIRST batch into one \
         result set via iter_rows, while every cleanly-delineated engine surface \
         produces one result set per statement — a result-structure divergence \
         by construction, so no surface can reproduce this golden. Clean \
         multi-statement delineation IS covered (multi_statement_delineated, \
         under both pull and verb). CUTOVER BEHAVIOR CHANGE: the new engine \
         delineates one result set per statement (the more correct shape) where \
         the old engine flattened a row-first `;`-batch into a single result \
         set, so a consumer relying on the old flattened shape sees a different \
         result-set COUNT after the cutover.",
    ),
];

/// The response projection compared on the pull surface: everything an active
/// pull observes EXCEPT `client_bytes` — the response-driven pull engine emits
/// no request frames, so the outbound wire is not part of its observable.
#[must_use]
pub fn response_view(run: &ObservedRun) -> ObservedRun {
    let mut view = run.clone();
    view.client_bytes = Vec::new();
    view
}

// ===========================================================================
// Mutation battery — the falsifier's injected-divergence set
// ===========================================================================
//
// Each [`Mutation`] models one realistic engine defect as a transform on the
// observable [`ObservedRun`] the corpus compares against. The falsifier
// (`falsifier_a2.rs`) runs this battery against every engine surface, so the
// new engine's discriminating power is measured directly against the modeled
// defect set rather than inherited by faith.

/// One injected engine defect: a name, a class, whether it is a previously-MISSED
/// blind-spot class (which must now be CAUGHT), and the mutation transform.
pub struct Mutation {
    /// Stable identifier for the modeled defect.
    pub name: &'static str,
    /// The observable class the defect lives in.
    pub class: &'static str,
    /// `true` if this mutation models a class the earlier probe MISSED. After
    /// widening the corpus, every such mutation MUST now be caught.
    pub blind_spot_probe: bool,
    /// The transform that injects the defect into an observed run.
    pub apply: fn(&mut ObservedRun),
}

// ───────────────────── mutation helpers ─────────────────────

fn with_ok(run: &mut ObservedRun, f: impl FnOnce(&mut ObservedOk)) {
    if let Ok(ok) = run.outcome.as_mut() {
        f(ok);
    }
}

/// Apply `f` to the LAST result set (the final statement's), if any.
fn with_last_rs(run: &mut ObservedRun, f: impl FnOnce(&mut ObservedResultSet)) {
    with_ok(run, |ok| {
        if let Some(rs) = ok.result_sets.last_mut() {
            f(rs);
        }
    });
}

/// Apply `f` to the FIRST result set with at least one row.
fn with_rowful_rs(run: &mut ObservedRun, f: impl FnOnce(&mut ObservedResultSet)) {
    with_ok(run, |ok| {
        if let Some(rs) = ok.result_sets.iter_mut().find(|r| !r.rows.is_empty()) {
            f(rs);
        }
    });
}

/// The full mutation battery: representative behavioural divergences, each
/// modeling one realistic engine defect.
#[must_use]
pub fn battery() -> Vec<Mutation> {
    use Mutation as M;
    vec![
        // ── command tag (CommandComplete parsing) ──
        M { name: "tag_flip_last_char", class: "command_tag", blind_spot_probe: false,
            apply: |r| with_last_rs(r, |rs| { if !rs.command_tag.is_empty() { rs.command_tag.pop(); rs.command_tag.push('Z'); } }) },
        M { name: "tag_drop_trailing_count", class: "command_tag", blind_spot_probe: false,
            apply: |r| with_last_rs(r, |rs| { if let Some(i) = rs.command_tag.rfind(' ') { rs.command_tag.truncate(i); } }) },
        M { name: "tag_to_empty", class: "command_tag", blind_spot_probe: false,
            apply: |r| with_last_rs(r, |rs| { if !rs.command_tag.is_empty() { rs.command_tag.clear(); } }) },
        M { name: "tag_wrong_verb", class: "command_tag", blind_spot_probe: false,
            apply: |r| with_last_rs(r, |rs| { if rs.command_tag.starts_with("SELECT") { rs.command_tag = rs.command_tag.replacen("SELECT", "UPDATE", 1); } }) },

        // ── affected_rows ──
        M { name: "affected_plus_one", class: "affected_rows", blind_spot_probe: false,
            apply: |r| with_last_rs(r, |rs| { if let Some(n) = rs.affected_rows { rs.affected_rows = Some(n + 1); } }) },
        M { name: "affected_to_none", class: "affected_rows", blind_spot_probe: false,
            apply: |r| with_last_rs(r, |rs| { if rs.affected_rows.is_some() { rs.affected_rows = None; } }) },
        M { name: "affected_to_zero", class: "affected_rows", blind_spot_probe: false,
            apply: |r| with_last_rs(r, |rs| { if matches!(rs.affected_rows, Some(n) if n != 0) { rs.affected_rows = Some(0); } }) },

        // ── rows ──
        M { name: "drop_last_row", class: "rows", blind_spot_probe: false,
            apply: |r| with_rowful_rs(r, |rs| { rs.rows.pop(); }) },
        M { name: "drop_first_column_offbyone", class: "rows/columns", blind_spot_probe: false,
            apply: |r| with_rowful_rs(r, |rs| { for row in &mut rs.rows { if !row.is_empty() { row.remove(0); } } }) },
        M { name: "truncate_last_cell", class: "rows", blind_spot_probe: false,
            apply: |r| with_rowful_rs(r, |rs| { if let Some(last) = rs.rows.last_mut() { last.pop(); } }) },
        M { name: "corrupt_cell_byte", class: "rows", blind_spot_probe: false,
            apply: |r| with_rowful_rs(r, |rs| { for row in &mut rs.rows { for b in row.iter_mut().flatten() { if let Some(x) = b.first_mut() { *x ^= 0xFF; return; } } } }) },
        M { name: "duplicate_last_row", class: "rows", blind_spot_probe: false,
            apply: |r| with_rowful_rs(r, |rs| { if let Some(last) = rs.rows.last().cloned() { rs.rows.push(last); } }) },
        M { name: "swap_first_two_columns", class: "rows/columns", blind_spot_probe: false,
            apply: |r| with_rowful_rs(r, |rs| { for row in &mut rs.rows { if row.len() >= 2 { row.swap(0, 1); } } }) },
        M { name: "clear_all_rows", class: "rows", blind_spot_probe: false,
            apply: |r| with_rowful_rs(r, |rs| { if !rs.rows.is_empty() { rs.rows.clear(); } }) },

        // ── NULL / empty-string handling ──
        M { name: "null_to_empty_bytes", class: "null", blind_spot_probe: false,
            apply: |r| with_rowful_rs(r, |rs| { for row in &mut rs.rows { for c in row.iter_mut() { if c.is_none() { *c = Some(Vec::new()); return; } } } }) },
        M { name: "nonnull_first_cell_to_null", class: "null", blind_spot_probe: false,
            apply: |r| with_rowful_rs(r, |rs| { for row in &mut rs.rows { for c in row.iter_mut() { if c.is_some() { *c = None; return; } } } }) },
        // BLIND-SPOT (was MISSED): empty-string (len=0, NOT null) confused with NULL.
        M { name: "empty_bytes_to_null", class: "null/empty", blind_spot_probe: true,
            apply: |r| with_rowful_rs(r, |rs| { for row in &mut rs.rows { for c in row.iter_mut() { if matches!(c, Some(v) if v.is_empty()) { *c = None; return; } } } }) },

        // ── column names ──
        M { name: "drop_column_names", class: "column_names", blind_spot_probe: false,
            apply: |r| with_ok(r, |ok| { for rs in &mut ok.result_sets { if !rs.column_names.is_empty() { rs.column_names.clear(); return; } } }) },
        M { name: "uppercase_column_names", class: "column_names", blind_spot_probe: false,
            apply: |r| with_ok(r, |ok| { for rs in &mut ok.result_sets { let up: Vec<String> = rs.column_names.iter().map(|c| c.to_uppercase()).collect(); if up != rs.column_names { rs.column_names = up; return; } } }) },
        M { name: "reverse_column_names", class: "column_names", blind_spot_probe: false,
            apply: |r| with_ok(r, |ok| { for rs in &mut ok.result_sets { if rs.column_names.len() >= 2 { rs.column_names.reverse(); return; } } }) },

        // ── column type OIDs (BLIND-SPOT: was structurally unrepresentable) ──
        M { name: "drop_type_oids", class: "type_oids", blind_spot_probe: true,
            apply: |r| with_ok(r, |ok| { for rs in &mut ok.result_sets { if !rs.type_oids.is_empty() { rs.type_oids.clear(); return; } } }) },
        M { name: "change_type_oid", class: "type_oids", blind_spot_probe: true,
            apply: |r| with_ok(r, |ok| { for rs in &mut ok.result_sets { if let Some(o) = rs.type_oids.first_mut() { *o = o.wrapping_add(1); return; } } }) },
        M { name: "reverse_type_oids", class: "type_oids", blind_spot_probe: true,
            apply: |r| with_ok(r, |ok| { for rs in &mut ok.result_sets { if rs.type_oids.len() >= 2 { rs.type_oids.reverse(); return; } } }) },

        // ── per-statement result-set boundaries + intermediate tags ──
        // (BLIND-SPOT: multi-statement rows were flattened, only the final tag kept.)
        M { name: "flatten_to_final_result_set", class: "result_sets", blind_spot_probe: true,
            apply: |r| with_ok(r, |ok| { if ok.result_sets.len() >= 2 && let Some(last) = ok.result_sets.last().cloned() { ok.result_sets = vec![last]; } }) },
        M { name: "drop_first_result_set", class: "result_sets", blind_spot_probe: true,
            apply: |r| with_ok(r, |ok| { if ok.result_sets.len() >= 2 { ok.result_sets.remove(0); } }) },
        M { name: "swap_first_two_result_sets", class: "result_sets", blind_spot_probe: true,
            apply: |r| with_ok(r, |ok| { if ok.result_sets.len() >= 2 { ok.result_sets.swap(0, 1); } }) },
        M { name: "change_intermediate_tag", class: "intermediate_tag", blind_spot_probe: true,
            apply: |r| with_ok(r, |ok| { if ok.result_sets.len() >= 2 { ok.result_sets[0].command_tag = "MUTATED".to_string(); } }) },
        M { name: "intermediate_affected_plus_one", class: "intermediate_tag", blind_spot_probe: true,
            apply: |r| with_ok(r, |ok| { if ok.result_sets.len() >= 2 && let Some(n) = ok.result_sets[0].affected_rows { ok.result_sets[0].affected_rows = Some(n + 1); } }) },

        // ── portal-suspend / row-limited Execute (new vocabulary) ──
        M { name: "flip_portal_suspended", class: "portal_suspended", blind_spot_probe: true,
            apply: |r| with_ok(r, |ok| { for rs in &mut ok.result_sets { if rs.portal_suspended { rs.portal_suspended = false; return; } } }) },

        // ── COPY OUT sub-protocol (new vocabulary) ──
        M { name: "drop_copy_chunk", class: "copy_out", blind_spot_probe: true,
            apply: |r| with_ok(r, |ok| { ok.copy_out.pop(); }) },
        M { name: "change_copy_chunk", class: "copy_out", blind_spot_probe: true,
            apply: |r| with_ok(r, |ok| { if let Some(c) = ok.copy_out.first_mut() { c.push(b'!'); } }) },
        M { name: "reorder_copy_chunks", class: "copy_out", blind_spot_probe: true,
            apply: |r| with_ok(r, |ok| { if ok.copy_out.len() >= 2 { ok.copy_out.swap(0, 1); } }) },

        // ── RFQ transaction status (BLIND-SPOT: collapsed to Ready) ──
        M { name: "tx_idle_to_intransaction", class: "tx_status", blind_spot_probe: true,
            apply: |r| { if matches!(r.tx_status, ObservedTxStatus::Idle) { r.tx_status = ObservedTxStatus::InTransaction; } } },
        M { name: "tx_intransaction_to_idle", class: "tx_status", blind_spot_probe: true,
            apply: |r| { if matches!(r.tx_status, ObservedTxStatus::InTransaction) { r.tx_status = ObservedTxStatus::Idle; } } },
        M { name: "tx_failed_to_idle", class: "tx_status", blind_spot_probe: true,
            apply: |r| { if matches!(r.tx_status, ObservedTxStatus::Failed) { r.tx_status = ObservedTxStatus::Idle; } } },

        // ── BackendKeyData / cancellation key (BLIND-SPOT) ──
        M { name: "change_backend_pid", class: "backend_pid", blind_spot_probe: true,
            apply: |r| { if let Some(pid) = r.backend_pid { r.backend_pid = Some(pid.wrapping_add(1)); } } },
        M { name: "backend_pid_to_none", class: "backend_pid", blind_spot_probe: true,
            apply: |r| { if r.backend_pid.is_some() { r.backend_pid = None; } } },

        // ── notices ──
        M { name: "drop_all_notices", class: "notices", blind_spot_probe: false,
            apply: |r| { if !r.notices.is_empty() { r.notices.clear(); } } },
        M { name: "change_notice_severity", class: "notices", blind_spot_probe: false,
            apply: |r| { if let Some(n) = r.notices.first_mut() { n.severity = "WARNING_X".to_string(); } } },
        M { name: "change_notice_sqlstate", class: "notices", blind_spot_probe: false,
            apply: |r| { if let Some(n) = r.notices.first_mut() { n.sqlstate = "XX999".to_string(); } } },
        M { name: "change_notice_message", class: "notices", blind_spot_probe: false,
            apply: |r| { if let Some(n) = r.notices.first_mut() { n.message.push_str("<corrupted>"); } } },
        // BLIND-SPOT (was MISSED): dropping the SECOND of multiple notices.
        M { name: "drop_second_notice", class: "notices", blind_spot_probe: true,
            apply: |r| { if r.notices.len() >= 2 { r.notices.remove(1); } } },
        // BLIND-SPOT (was MISSED): notice ordering.
        M { name: "reorder_notices", class: "notices", blind_spot_probe: true,
            apply: |r| { if r.notices.len() >= 2 { r.notices.swap(0, 1); } } },

        // ── notifications ──
        M { name: "drop_all_notifications", class: "notifications", blind_spot_probe: false,
            apply: |r| { if !r.notifications.is_empty() { r.notifications.clear(); } } },
        M { name: "change_notify_payload", class: "notifications", blind_spot_probe: false,
            apply: |r| { if let Some(n) = r.notifications.first_mut() { n.payload.push(b'!'); } } },
        M { name: "change_notify_pid", class: "notifications", blind_spot_probe: false,
            apply: |r| { if let Some(n) = r.notifications.first_mut() { n.pid = n.pid.wrapping_add(1); } } },
        // BLIND-SPOT (was MISSED): dropping the SECOND of multiple notifications.
        M { name: "drop_second_notification", class: "notifications", blind_spot_probe: true,
            apply: |r| { if r.notifications.len() >= 2 { r.notifications.remove(1); } } },
        // BLIND-SPOT (was MISSED): notification ordering.
        M { name: "reorder_notifications", class: "notifications", blind_spot_probe: true,
            apply: |r| { if r.notifications.len() >= 2 { r.notifications.swap(0, 1); } } },

        // ── server errors / SQLSTATE + diagnostic fields ──
        M { name: "change_sqlstate", class: "error", blind_spot_probe: false,
            apply: |r| { if let Err(ObservedErr::Server { sqlstate, .. }) = &mut r.outcome { *sqlstate = "08006".to_string(); } } },
        M { name: "change_error_severity", class: "error", blind_spot_probe: false,
            apply: |r| { if let Err(ObservedErr::Server { severity, .. }) = &mut r.outcome { *severity = Some("PANIC".to_string()); } } },
        M { name: "change_error_message", class: "error", blind_spot_probe: false,
            apply: |r| { if let Err(ObservedErr::Server { message, .. }) = &mut r.outcome { message.push_str("<x>"); } } },
        M { name: "swallow_server_error", class: "error", blind_spot_probe: false,
            apply: |r| { if r.outcome.is_err() { r.outcome = Ok(ObservedOk::default()); } } },
        // BLIND-SPOT (was MISSED): error detail/hint were never observed.
        M { name: "drop_error_detail", class: "error_fields", blind_spot_probe: true,
            apply: |r| { if let Err(ObservedErr::Server { detail: d @ Some(_), .. }) = &mut r.outcome { *d = None; } } },
        M { name: "drop_error_hint", class: "error_fields", blind_spot_probe: true,
            apply: |r| { if let Err(ObservedErr::Server { hint: h @ Some(_), .. }) = &mut r.outcome { *h = None; } } },
        M { name: "change_error_detail", class: "error_fields", blind_spot_probe: true,
            apply: |r| { if let Err(ObservedErr::Server { detail: Some(d), .. }) = &mut r.outcome { d.push_str("<x>"); } } },

        // ── terminal status ──
        M { name: "errored_to_ready", class: "terminal", blind_spot_probe: false,
            apply: |r| { if matches!(r.terminal, ObservedStatus::Errored(_)) { r.terminal = ObservedStatus::Ready; } } },
        M { name: "closed_to_ready", class: "terminal", blind_spot_probe: false,
            apply: |r| { if matches!(r.terminal, ObservedStatus::Closed) { r.terminal = ObservedStatus::Ready; } } },
        M { name: "ready_to_closed", class: "terminal", blind_spot_probe: false,
            apply: |r| { if matches!(r.terminal, ObservedStatus::Ready) { r.terminal = ObservedStatus::Closed; } } },
        M { name: "terminalkind_protocol_to_handshake", class: "terminal", blind_spot_probe: false,
            apply: |r| { if matches!(r.terminal, ObservedStatus::Errored(TerminalErrorKind::Protocol)) { r.terminal = ObservedStatus::Errored(TerminalErrorKind::Handshake); } } },

        // ── protocol failure kind ──
        M { name: "protokind_unclassified_to_streamstalled", class: "protocol_kind", blind_spot_probe: false,
            apply: |r| { if let Err(ObservedErr::Protocol(k @ ProtocolFailureKind::Unclassified)) = &mut r.outcome { *k = ProtocolFailureKind::StreamStalled; } } },
        M { name: "protokind_handshakefailed_to_unclassified", class: "protocol_kind", blind_spot_probe: false,
            apply: |r| { if let Err(ObservedErr::Protocol(k @ ProtocolFailureKind::HandshakeFailed)) = &mut r.outcome { *k = ProtocolFailureKind::Unclassified; } } },

        // ── parameter statuses ──
        // The engine lends every ParameterStatus frame raw, in arrival order, so
        // these model dropping the set, collapsing a duplicate frame's distinct
        // value, corrupting a value, and reordering the arrival sequence.
        M { name: "drop_param_statuses", class: "param_status", blind_spot_probe: false,
            apply: |r| { if !r.parameter_statuses.is_empty() { r.parameter_statuses.clear(); } } },
        M { name: "dup_second_value_collapsed", class: "param_status", blind_spot_probe: false,
            apply: |r| { for (_, v) in &mut r.parameter_statuses { if v == "second" { *v = "first".to_string(); } } } },
        M { name: "change_param_value", class: "param_status", blind_spot_probe: false,
            apply: |r| { if let Some((_, v)) = r.parameter_statuses.first_mut() { v.push_str("_x"); } } },
        M { name: "reverse_param_order", class: "param_status", blind_spot_probe: false,
            apply: |r| { if r.parameter_statuses.len() >= 2 { r.parameter_statuses.reverse(); } } },

        // ── client wire bytes (outbound encoding) ──
        M { name: "flip_first_client_byte", class: "client_bytes", blind_spot_probe: false,
            apply: |r| { if let Some(b) = r.client_bytes.first_mut() { *b ^= 0xFF; } } },
        M { name: "drop_last_client_byte", class: "client_bytes", blind_spot_probe: false,
            apply: |r| { r.client_bytes.pop(); } },
        M { name: "clear_client_bytes", class: "client_bytes", blind_spot_probe: false,
            apply: |r| { if !r.client_bytes.is_empty() { r.client_bytes.clear(); } } },
    ]
}
