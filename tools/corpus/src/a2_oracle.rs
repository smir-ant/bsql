//! The going-forward regression oracle over the NEW engine.
//!
//! For each fixture it drives the new engine through whichever observable
//! surface covers it — the connecting `run`, the response-driven `pull`, or the
//! client-byte-emitting `verb` — and asserts the engine's observation equals the
//! committed golden ([`Transcript::expect`]). A fixture that NO surface drives
//! must be a documented structural exclusion, so no fixture is ever silently
//! unobserved.
//!
//! The surface partitions are the single source of truth in [`crate::falsify`],
//! shared with the falsifier so the regression and the strength measurement
//! measure the identical partition and cannot drift apart.
//!
//! It is a `src/` file compiled INTO each consuming test crate via
//! `#[path = "../src/a2_oracle.rs"] mod a2_oracle;` (the same pattern
//! `engine_adapter.rs` / `falsify.rs` use), so it shares the engine adapter and
//! the corpus partitions without a runtime-library dependency on either.

#![allow(
    clippy::panic,
    reason = "test oracle — a fixture mismatch is the loud test-failure signal, not a production fallback; these helper methods are not in `#[test]` context, so the in-tests carve-out cannot reach the asserts"
)]
#![allow(
    dead_code,
    reason = "shared oracle compiled into multiple test crates via `#[path]`; the seed crate uses the schedule-invariance helper, the adversarial crate does not, so not every item is read in every crate — the established shared-test-helper-module pattern"
)]

use std::collections::BTreeSet;

use bsql_corpus::adapter::Adapter;
use bsql_corpus::{ChunkSchedule, Transcript};

use crate::engine_adapter::EngineAdapter;
use crate::falsify::{
    active_pull_corpus, handshake_only_corpus, response_view, verb_client_byte_corpus,
    STRUCTURAL_EXCLUSIONS,
};

/// Routes a transcript to the new engine's covering surface(s) and asserts the
/// engine's observation reproduces the frozen golden.
///
/// The surface membership is precomputed once from the [`crate::falsify`]
/// partitions so each fixture is routed by name, the same way the falsifier and
/// the coverage guard classify it.
pub struct A2Oracle {
    engine: EngineAdapter,
    handshake: BTreeSet<&'static str>,
    pull: BTreeSet<&'static str>,
    verb: BTreeSet<&'static str>,
    excluded: BTreeSet<&'static str>,
}

impl A2Oracle {
    /// Build the oracle, precomputing surface membership from the shared
    /// partitions.
    #[must_use]
    pub fn new() -> Self {
        Self {
            engine: EngineAdapter::new(),
            handshake: handshake_only_corpus().iter().map(|t| t.name).collect(),
            pull: active_pull_corpus().iter().map(|t| t.name).collect(),
            verb: verb_client_byte_corpus().iter().map(|t| t.name).collect(),
            excluded: STRUCTURAL_EXCLUSIONS.iter().map(|(n, _)| *n).collect(),
        }
    }

    /// Assert the new engine reproduces `t`'s golden through every surface that
    /// drives it. A fixture no surface drives must be a documented structural
    /// exclusion (the new engine deliberately diverges from the frozen old
    /// behaviour there) — never a silent gap.
    pub fn assert_matches_golden(&self, t: &Transcript) {
        let mut driven = false;

        // Connecting surface: the FULL observable (client startup/auth bytes,
        // parameter statuses, backend pid, transaction status, terminal).
        if self.handshake.contains(t.name) {
            assert_eq!(
                self.engine.run(t),
                t.expect,
                "handshake surface: new engine != golden on `{}`",
                t.name,
            );
            driven = true;
        }

        // Pull surface: response-driven, emits no request frames, so it is
        // compared on the response projection only (the outbound wire is not part
        // of its observable).
        if self.pull.contains(t.name) {
            assert_eq!(
                response_view(&self.engine.pull(t)),
                response_view(&t.expect),
                "pull surface: new engine != golden (response) on `{}`",
                t.name,
            );
            driven = true;
        }

        // Verb surface: the real verbs put the request wire on the socket, so the
        // FULL observable (INCLUDING `client_bytes`) is compared; and the two
        // active surfaces must agree on the response projection — the
        // surface-equivalence property, proven across the engine's own surfaces.
        if self.verb.contains(t.name) {
            let v = self.engine.verb(t);
            assert_eq!(v, t.expect, "verb surface: new engine != golden on `{}`", t.name);
            assert_eq!(
                response_view(&v),
                response_view(&self.engine.pull(t)),
                "verb/pull surface response divergence on `{}`",
                t.name,
            );
            driven = true;
        }

        assert!(
            driven || self.excluded.contains(t.name),
            "fixture `{}` is driven by no engine surface and is not a documented \
             structural exclusion — it would be silently unobserved (coverage loss)",
            t.name,
        );
    }

    /// Assert the observation is fragmentation-invariant: replaying `t` under
    /// every transport chunk schedule still reproduces the golden through its
    /// covering surface (partial-frame resumption never changes the outcome).
    pub fn assert_schedule_invariant(&self, t: &Transcript) {
        for schedule in [
            ChunkSchedule::AllAtOnce,
            ChunkSchedule::OneBytePerRead,
            ChunkSchedule::SplitHeaders,
        ] {
            let mut variant = t.clone();
            variant.chunk_schedule = schedule;
            self.assert_matches_golden(&variant);
        }
    }
}

impl Default for A2Oracle {
    fn default() -> Self {
        Self::new()
    }
}
