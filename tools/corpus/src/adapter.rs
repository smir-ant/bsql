//! The one-method [`Adapter`] seam.
//!
//! An `Adapter` is a bridge from a transcript to an engine: it replays the
//! transcript's scripted server bytes against some engine implementation and
//! returns what that engine made observable. The trait names ONLY the
//! observable [`crate::ObservedRun`] / [`crate::Transcript`] data types, never
//! an internal engine type — so a future engine rewrite supplies a NEW adapter
//! without touching the corpus or the observable contract.

use crate::observed::ObservedRun;
use crate::transcript::Transcript;

/// Replay a transcript against one engine and report the observable result.
///
/// The single entry point of the replay oracle. The implementor over the
/// engine under test lives in the test crates (`src/engine_adapter.rs`,
/// compiled in via `#[path]`); a future rebuilt engine supplies its own. The
/// corpus asserts `adapter.run(t) == t.expect` (the pinned golden); across two
/// adapters, `a.run(t) == b.run(t)` proves two engines agree (the differential
/// that gated the engine cutover).
pub trait Adapter {
    /// Replay `transcript` and return the observable outcome.
    fn run(&self, transcript: &Transcript) -> ObservedRun;
}
