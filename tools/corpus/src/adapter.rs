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
/// The single entry point of the differential-replay oracle. For the current
/// engine the implementor is [`crate::SansIoAdapter`]; a future rebuilt engine
/// supplies its own implementor. The corpus asserts both
/// `adapter.run(t) == t.expect` (pin) and, across two adapters,
/// `a.run(t) == b.run(t)` (equivalence).
pub trait Adapter {
    /// Replay `transcript` and return the observable outcome.
    fn run(&self, transcript: &Transcript) -> ObservedRun;
}
