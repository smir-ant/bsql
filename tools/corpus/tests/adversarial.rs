//! The 4 mandatory adversarial fixtures, each replayed through the NEW engine's
//! covering surface and asserted to reproduce its committed golden:
//!
//! 1. duplicate `ParameterStatus` for one key — latest value wins, command
//!    completes Ready;
//! 2. a second `RowDescription` before `CommandComplete` in a SELECT flow —
//!    protocol violation: the command fails and the connection goes errored;
//! 3. `NoticeResponse` during the authentication phase — the connecting engine
//!    ABSORBS it (not surfaced) per PG protocol §55.2.7, so the handshake still
//!    reaches Ready and the connection becomes active;
//! 4. `NegotiateProtocolVersion` before Authentication — bsql requests exactly
//!    3.0 with no options, so a compliant server never sends 'v'; the engine
//!    classifies it as a LOUD handshake failure rather than swallowing it.

#[path = "../src/engine_transport.rs"]
mod engine_transport;
#[path = "../src/engine_adapter.rs"]
mod engine_adapter;
#[path = "../src/falsify.rs"]
mod falsify;
#[path = "../src/a2_oracle.rs"]
mod a2_oracle;

use bsql_corpus::corpus;

use a2_oracle::A2Oracle;

#[test]
fn adversarial_fixtures_match_golden_on_new_engine() {
    let oracle = A2Oracle::new();
    let adversarial = corpus::adversarial();
    assert_eq!(adversarial.len(), 4, "exactly the 4 mandatory adversarial fixtures");
    for t in &adversarial {
        oracle.assert_matches_golden(t);
    }
}
