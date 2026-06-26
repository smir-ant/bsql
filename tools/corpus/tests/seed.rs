//! Seed-corpus replay: every fixture runs on BOTH twins of Adapter#1, and
//! each must (a) match its pinned `expect` and (b) agree across twins — the
//! differential-equivalence property that generalises to a future engine.
//!
//! Each fixture is ALSO replayed under all three transport chunk schedules to
//! prove the observation is fragmentation-invariant (partial-frame resumption:
//! one byte per read, header/body split) — axis-5 coverage applied corpus-wide.

#![allow(
    clippy::panic,
    reason = "test harness — a fixture mismatch is the loud test-failure signal, not a production fallback; integration-test bodies are not in `#[test]` context so the in-tests carve-out cannot reach the assert helper"
)]

use bsql_corpus::{Adapter, ChunkSchedule, SansIoAdapter, Transcript, corpus};

/// Assert a transcript matches its pin on both twins and that the twins agree.
fn assert_pinned_and_equivalent(t: &Transcript) {
    let a = SansIoAdapter::sync().run(t);
    let b = SansIoAdapter::async_twin().run(t);
    assert_eq!(a, b, "twin divergence on `{}`", t.name);
    assert_eq!(a, t.expect, "pin mismatch (sync) on `{}`", t.name);
    assert_eq!(b, t.expect, "pin mismatch (async) on `{}`", t.name);
}

/// Replay `t` under each chunk schedule; the observation must be identical to
/// `t`'s own (so partial-frame fragmentation never changes the outcome) and
/// must agree across twins under every schedule.
fn assert_schedule_invariant(t: &Transcript) {
    let baseline = SansIoAdapter::sync().run(t);
    for schedule in [
        ChunkSchedule::AllAtOnce,
        ChunkSchedule::OneBytePerRead,
        ChunkSchedule::SplitHeaders,
    ] {
        let mut variant = t.clone();
        variant.chunk_schedule = schedule;
        let s = SansIoAdapter::sync().run(&variant);
        let a = SansIoAdapter::async_twin().run(&variant);
        assert_eq!(s, a, "twin divergence on `{}` under {schedule:?}", t.name);
        assert_eq!(
            s, baseline,
            "schedule {schedule:?} changed the observation on `{}`",
            t.name
        );
    }
}

#[test]
fn seed_corpus_green_on_both_twins() {
    let seed = corpus::seed();
    assert!(seed.len() >= 12, "seed corpus must be representative (>=12 fixtures)");
    for t in &seed {
        assert_pinned_and_equivalent(t);
    }
}

#[test]
fn seed_corpus_is_schedule_invariant() {
    for t in &corpus::seed() {
        assert_schedule_invariant(t);
    }
}
