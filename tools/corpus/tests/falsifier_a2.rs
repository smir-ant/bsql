//! ADAPTER#2 FALSIFIER — strength measurement for the NEW engine's observations.
//!
//! The A1 falsifier (`falsifier.rs`) proves the corpus is a strong oracle for the
//! LIVE engine: it injects each modeled defect into the observed run and confirms
//! the corpus would go red. This test re-runs the SAME battery against the NEW
//! strangler engine's observations, so the rebuilt engine's discriminating power
//! is measured directly — never inherited from the old engine by faith.
//!
//! Method, per Adapter#2 surface (`pull` / `verb` / `handshake`), over the subset
//! that surface drives (the single source of truth in [`falsify`], shared with
//! the differentials so the partition cannot drift):
//!
//! 1. Establish the baseline by running the REAL new engine and asserting its
//!    projected observation EQUALS the pin — re-proving the differential through
//!    the engine, so the catch counts are tied to the actual engine, not to the
//!    `expect` literals.
//! 2. For each mutation, mark it CAUGHT for that surface if the mutated baseline
//!    diverges from the pin on >=1 fixture (the surface's observation is
//!    discriminating enough to notice the defect). A mutation that is a no-op on
//!    every in-subset fixture is MISSED by that surface.
//! 3. The A2 oracle is the UNION across surfaces: a defect a real new-engine bug
//!    would introduce is caught iff SOME Adapter#2 surface catches it.
//!
//! The `pull` surface uses the response projection ([`falsify::response_view`]) —
//! it emits no request frames, exactly as the pull differential compares — so the
//! outbound-wire mutations are structurally outside its observable (the verb /
//! handshake surfaces, which DO carry client bytes, catch those).

#![allow(clippy::panic, reason = "probe harness — loud failure is the signal")]
#![allow(clippy::print_stdout, reason = "probe harness — reports counts")]

#[path = "../src/engine_transport.rs"]
mod engine_transport;
#[path = "../src/engine_adapter.rs"]
mod engine_adapter;
#[path = "../src/falsify.rs"]
mod falsify;

use bsql_corpus::{Adapter, ObservedRun, Transcript};

use engine_adapter::EngineAdapter;
use falsify::{
    active_pull_corpus, battery, full_corpus, handshake_only_corpus, response_view,
    verb_client_byte_corpus, Mutation,
};

/// The full-observable projection — the identity, used by the surfaces (verb /
/// handshake) whose client wire IS part of their observable.
fn full_view(run: &ObservedRun) -> ObservedRun {
    run.clone()
}

/// Per-mutation CAUGHT mask for one Adapter#2 surface.
///
/// First asserts, per fixture, that the surface's REAL projected observation
/// equals the pin (the differential, re-proven through the engine — the teeth
/// that ties the catch counts to actual engine behaviour). Then a mutation is
/// CAUGHT iff the mutated baseline diverges from the pin on >=1 in-subset fixture.
fn surface_catch(
    label: &str,
    subset: &[Transcript],
    run: impl Fn(&Transcript) -> ObservedRun,
    project: impl Fn(&ObservedRun) -> ObservedRun,
    muts: &[Mutation],
) -> Vec<bool> {
    assert!(
        !subset.is_empty(),
        "{label}: empty subset — the surface measures nothing",
    );
    let mut baselines: Vec<ObservedRun> = Vec::new();
    for t in subset {
        let observed = run(t);
        assert_eq!(
            project(&observed),
            project(&t.expect),
            "{label}: the new engine's observation diverges from the pin on `{}` \
             (the differential must hold before the falsifier is meaningful)",
            t.name,
        );
        baselines.push(observed);
    }

    let mut mask = Vec::with_capacity(muts.len());
    for m in muts {
        let caught = subset.iter().enumerate().any(|(i, t)| {
            let mut mutated = baselines[i].clone();
            (m.apply)(&mut mutated);
            project(&mutated) != project(&t.expect)
        });
        mask.push(caught);
    }
    mask
}

fn count_true(mask: &[bool]) -> usize {
    mask.iter().filter(|&&b| b).count()
}

#[test]
fn a2_falsifier_catch_rate() {
    let engine = EngineAdapter::new();
    let muts = battery();
    let total = muts.len();

    let pull_subset = active_pull_corpus();
    let verb_subset = verb_client_byte_corpus();
    let handshake_subset = handshake_only_corpus();

    // The pull surface emits no request frames, so it observes the RESPONSE
    // projection (matching the pull differential). The verb / handshake surfaces
    // put client bytes on the wire, so they observe the full observable.
    let pull = surface_catch(
        "pull",
        &pull_subset,
        |t| engine.pull(t),
        response_view,
        &muts,
    );
    let verb = surface_catch("verb", &verb_subset, |t| engine.verb(t), full_view, &muts);
    let handshake = surface_catch(
        "handshake",
        &handshake_subset,
        |t| engine.run(t),
        full_view,
        &muts,
    );

    // The A2 oracle: a defect is caught iff SOME surface catches it.
    let union: Vec<bool> = (0..total)
        .map(|i| pull[i] || verb[i] || handshake[i])
        .collect();

    // ── report ──
    println!("\n=== ADAPTER#2 FALSIFIER (new engine discriminating power) ===");
    println!(
        "subsets: pull {} fixtures, verb {} fixtures, handshake {} fixtures (of {} total)",
        pull_subset.len(),
        verb_subset.len(),
        handshake_subset.len(),
        full_corpus().len(),
    );
    println!(
        "\n{:<40} {:<16} {:>5} {:>5} {:>4} {:>4} probe",
        "mutation", "class", "pull", "verb", "hs", "A2"
    );
    println!("{}", "-".repeat(86));
    for (i, m) in muts.iter().enumerate() {
        let mark = |b: bool| if b { "YES" } else { "no " };
        let probe = if m.blind_spot_probe { "BLIND-SPOT" } else { "" };
        println!(
            "{:<40} {:<16} {:>5} {:>5} {:>4} {:>4} {}",
            m.name,
            m.class,
            mark(pull[i]),
            mark(verb[i]),
            mark(handshake[i]),
            mark(union[i]),
            probe,
        );
    }
    println!("{}", "-".repeat(86));

    let pull_caught = count_true(&pull);
    let verb_caught = count_true(&verb);
    let hs_caught = count_true(&handshake);
    let union_caught = count_true(&union);
    let pct = |c: usize| (c as f64) * 100.0 / (total as f64);
    println!(
        "pull   CAUGHT {pull_caught}/{total} = {:.1}%",
        pct(pull_caught)
    );
    println!(
        "verb   CAUGHT {verb_caught}/{total} = {:.1}%",
        pct(verb_caught)
    );
    println!(
        "hs     CAUGHT {hs_caught}/{total} = {:.1}%",
        pct(hs_caught)
    );
    println!(
        "A2 UNION CAUGHT {union_caught}/{total} = {:.1}%",
        pct(union_caught)
    );

    let uncaught: Vec<&str> = muts
        .iter()
        .zip(&union)
        .filter(|(_, c)| !**c)
        .map(|(m, _)| m.name)
        .collect();
    println!("A2 union NOT caught: {uncaught:?}\n");

    // 1. The A2 oracle catches every modeled defect EXCEPT exactly one, and that
    //    one is the falsifier shadow of the documented `terminate` structural
    //    exclusion: `closed_to_ready` flips a `Closed` terminal, but no Adapter#2
    //    surface can produce a `Closed` observable (the strangler engine has no
    //    terminate verb yet, and a Terminate carries no server reply). When a
    //    terminate verb lands, this assertion fails and forces the gap closed.
    assert_eq!(
        uncaught,
        vec!["closed_to_ready"],
        "the A2 oracle's blind spots changed; investigate before relaxing \
         (a NEW miss is a real engine discrimination gap, not a test to weaken)",
    );

    // 2. The ≥92% floor (target 100% — match A1's 70/70). The single structural
    //    gap above leaves the union at total-1.
    assert!(
        union_caught.saturating_mul(100) >= total.saturating_mul(92),
        "A2 union catch rate {union_caught}/{total} below the 92% floor",
    );
    assert_eq!(
        union_caught,
        total - 1,
        "expected the A2 union to catch all but the one structural terminate gap",
    );

    // 2b. Every previously-MISSED blind-spot class the corpus was widened to catch
    //     is ALSO caught by the new engine's observations — the rebuild did not
    //     reintroduce any of the structural blind spots the corpus closed.
    let blind_uncaught: Vec<&str> = muts
        .iter()
        .zip(&union)
        .filter(|(m, c)| m.blind_spot_probe && !**c)
        .map(|(m, _)| m.name)
        .collect();
    assert!(
        blind_uncaught.is_empty(),
        "the A2 oracle misses widened blind-spot classes: {blind_uncaught:?}",
    );

    // 3. Teeth: the framework genuinely distinguishes CAUGHT from MISSED — the
    //    pull surface MISSES the outbound-wire mutations (it observes no client
    //    bytes), proving a no-op mutation is recorded as a miss, not a false hit.
    let pull_misses_client_bytes = ["flip_first_client_byte", "drop_last_client_byte", "clear_client_bytes"]
        .iter()
        .all(|name| {
            muts.iter()
                .zip(&pull)
                .any(|(m, caught)| m.name == *name && !*caught)
        });
    assert!(
        pull_misses_client_bytes,
        "the pull surface must MISS the client-byte mutations (it observes the \
         response projection only) — if it 'catches' them the framework is not \
         distinguishing caught from missed",
    );
    // And those same mutations ARE caught by the union (via verb / handshake),
    // so the miss is a per-surface projection boundary, never an A2 oracle gap.
    for name in ["flip_first_client_byte", "drop_last_client_byte", "clear_client_bytes"] {
        let caught_by_union = muts
            .iter()
            .zip(&union)
            .any(|(m, c)| m.name == name && *c);
        assert!(
            caught_by_union,
            "client-byte mutation `{name}` must be caught by the A2 union (verb / handshake observe client bytes)",
        );
    }
}
