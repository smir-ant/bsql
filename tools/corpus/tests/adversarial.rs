//! The 3 mandatory adversarial fixtures, each PINNING the current engine's
//! real observed behaviour (captured by replay, not assumed):
//!
//! 1. duplicate `ParameterStatus` for one key — latest value wins, command
//!    completes Ready;
//! 2. a second `RowDescription` before `CommandComplete` in a SELECT flow —
//!    protocol violation: the command fails and the connection goes errored;
//! 3. `NoticeResponse` during the authentication phase — the connecting state
//!    rejects it (not surfaced), so the handshake fails.
//!
//! Each runs on BOTH twins and must match its pin and agree across twins.

#![allow(
    clippy::panic,
    reason = "test harness — a fixture mismatch is the loud test-failure signal, not a production fallback; integration-test bodies are not in `#[test]` context so the in-tests carve-out cannot reach the assert helper"
)]

use bsql_corpus::{Adapter, SansIoAdapter, Transcript, corpus};

fn assert_pinned_and_equivalent(t: &Transcript) {
    let a = SansIoAdapter::sync().run(t);
    let b = SansIoAdapter::async_twin().run(t);
    assert_eq!(a, b, "twin divergence on `{}`", t.name);
    assert_eq!(a, t.expect, "pin mismatch (sync) on `{}`", t.name);
    assert_eq!(b, t.expect, "pin mismatch (async) on `{}`", t.name);
}

#[test]
fn adversarial_fixtures_pinned_on_both_twins() {
    let adversarial = corpus::adversarial();
    assert_eq!(adversarial.len(), 3, "exactly the 3 mandatory adversarial fixtures");
    for t in &adversarial {
        assert_pinned_and_equivalent(t);
    }
}
