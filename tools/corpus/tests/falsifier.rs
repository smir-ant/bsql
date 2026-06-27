//! CORPUS FALSIFIER — strength measurement for the differential oracle.
//!
//! Method: run the REAL engine ([`SansIoAdapter`]) over every fixture (seed +
//! adversarial), confirm the baseline is green (`run == expect` on both twins),
//! then inject a battery of representative BEHAVIORAL DIVERGENCES — each modeling
//! one realistic engine defect, expressed as a transform on the observable
//! `ObservedRun` the corpus compares against — and count how many cause at least
//! one fixture to go red (`mutated != expect`).
//!
//! A divergence the corpus catches on >=1 fixture = CAUGHT. A divergence that is
//! a no-op on EVERY fixture = MISSED: the corpus has no input that exercises
//! that behavior with distinguishing values — a blind spot.
//!
//! This is the re-run of the earlier strangler-readiness probe after the corpus
//! was widened. The `blind_spot_probe` mutations are exactly the classes the
//! earlier probe MISSED (empty-vs-NULL confusion, dropping/reordering a second
//! notice/notification, and the structural classes that then had no
//! `ObservedRun` field at all: per-statement result-set boundaries, intermediate
//! command tags, column type OIDs, RFQ transaction status, BackendKeyData, error
//! detail/hint, and ParameterStatus keys beyond the projected set). Each must
//! now be CAUGHT — the test asserts it.
//!
//! This remains a CONSERVATIVE lower bound: it credits only PIN catches
//! (`run != expect`). The corpus's additional twin-equivalence and
//! schedule-invariance assertions add catching power not modeled here.

#![allow(clippy::panic, reason = "probe harness — loud failure is the signal")]
#![allow(clippy::print_stdout, reason = "probe harness — reports counts")]
#![allow(clippy::too_many_lines, reason = "the battery is a flat list")]

use bsql_corpus::{
    Adapter, ObservedErr, ObservedOk, ObservedResultSet, ObservedRun, ObservedStatus,
    ObservedTxStatus, ProtocolFailureKind, SansIoAdapter, TerminalErrorKind, Transcript, corpus,
};

/// The full corpus the strangler engine will validate against.
fn full_corpus() -> Vec<Transcript> {
    let mut all = corpus::seed();
    all.extend(corpus::adversarial());
    all
}

/// One injected engine defect: a name, a class, whether it is a previously-MISSED
/// blind-spot class (which must now be CAUGHT), and the mutation transform.
struct Mutation {
    name: &'static str,
    class: &'static str,
    /// `true` if this mutation models a class the earlier probe MISSED. After
    /// widening the corpus, every such mutation MUST now be caught.
    blind_spot_probe: bool,
    apply: fn(&mut ObservedRun),
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

fn battery() -> Vec<Mutation> {
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

        // ── ParameterStatus keys beyond the projected set (BLIND-SPOT) ──
        M { name: "unknown_param_count_to_zero", class: "unknown_param", blind_spot_probe: true,
            apply: |r| { if r.unknown_parameter_status_count != 0 { r.unknown_parameter_status_count = 0; } } },
        M { name: "unknown_param_count_plus_one", class: "unknown_param", blind_spot_probe: true,
            apply: |r| { r.unknown_parameter_status_count = r.unknown_parameter_status_count.wrapping_add(1); } },

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
        M { name: "drop_param_statuses", class: "param_status", blind_spot_probe: false,
            apply: |r| { if !r.parameter_statuses.is_empty() { r.parameter_statuses.clear(); } } },
        M { name: "dup_keep_first_not_latest", class: "param_status", blind_spot_probe: false,
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

#[test]
fn corpus_falsifier_catch_rate() {
    let corpus = full_corpus();
    let sync = SansIoAdapter::sync();
    let async_twin = SansIoAdapter::async_twin();

    // 1. Baseline must be green: the real engine matches every pin on both twins.
    let mut baseline: Vec<ObservedRun> = Vec::new();
    for t in &corpus {
        let r = sync.run(t);
        let a = async_twin.run(t);
        assert_eq!(r, a, "baseline twin divergence on `{}`", t.name);
        assert_eq!(r, t.expect, "baseline pin mismatch on `{}`", t.name);
        baseline.push(r);
    }
    println!("\n=== CORPUS FALSIFIER (re-run after widening) ===");
    println!("fixtures: {} ({} seed + 3 adversarial)", corpus.len(), corpus.len() - 3);

    // 2. Apply each mutation to every fixture's real run; CAUGHT if it diverges
    //    from the pin on >=1 fixture.
    let muts = battery();
    let mut caught = 0usize;
    let mut missed_names: Vec<&str> = Vec::new();
    let mut blind_caught = 0usize;
    let mut blind_total = 0usize;
    let mut blind_missed: Vec<&str> = Vec::new();
    println!("\n{:<38} {:<18} {:>7} {:>6} probe", "mutation", "class", "caught", "n_fix");
    println!("{}", "-".repeat(82));
    for m in &muts {
        let mut n_changed = 0usize;
        for (i, t) in corpus.iter().enumerate() {
            let mut r = baseline[i].clone();
            (m.apply)(&mut r);
            if r != t.expect {
                n_changed += 1;
            }
        }
        let is_caught = n_changed > 0;
        if is_caught {
            caught += 1;
        } else {
            missed_names.push(m.name);
        }
        if m.blind_spot_probe {
            blind_total += 1;
            if is_caught {
                blind_caught += 1;
            } else {
                blind_missed.push(m.name);
            }
        }
        let mark = if is_caught { "YES" } else { "no " };
        let probe = if m.blind_spot_probe { "BLIND-SPOT" } else { "" };
        println!("{:<38} {:<18} {:>7} {:>6} {}", m.name, m.class, mark, n_changed, probe);
    }

    let total = muts.len();
    let pct = (caught as f64) * 100.0 / (total as f64);
    println!("{}", "-".repeat(82));
    println!("CAUGHT {caught}/{total} = {pct:.1}%");
    println!("previously-MISSED blind-spot classes now CAUGHT: {blind_caught}/{blind_total}");
    println!("MISSED (no-op on every fixture): {missed_names:?}");
    if !blind_missed.is_empty() {
        println!("STILL-MISSED blind spots: {blind_missed:?}");
    }
    println!();

    // 3. Every previously-missed blind-spot class must now be caught — that is
    //    the deliverable: the corpus is now a strong oracle for exactly the
    //    behaviors a future engine rewrite changes.
    assert!(
        blind_missed.is_empty(),
        "blind-spot classes still uncaught after widening: {blind_missed:?}",
    );
    // 4. The widened corpus must catch every modeled defect class.
    assert_eq!(
        missed_names.len(),
        0,
        "the widened corpus left {} defect class(es) uncaught: {missed_names:?}",
        missed_names.len(),
    );
}
