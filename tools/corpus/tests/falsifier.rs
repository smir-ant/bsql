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
//!
//! The mutation battery itself lives in the shared `falsify` module so the
//! Adapter#2 falsifier (`falsifier_a2.rs`) runs the IDENTICAL defect set — the
//! two falsifiers cannot diverge on what they inject.

#![allow(clippy::panic, reason = "probe harness — loud failure is the signal")]
#![allow(clippy::print_stdout, reason = "probe harness — reports counts")]

#[path = "../src/falsify.rs"]
mod falsify;

use bsql_corpus::{Adapter, ObservedRun, SansIoAdapter};

use falsify::{battery, full_corpus};

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
