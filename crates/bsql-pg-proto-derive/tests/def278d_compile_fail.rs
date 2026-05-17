//! DEF-278 Bundle D / D' (2026-05-17 / 2026-05-18) — `trybuild`
//! golden harness for the CancelRequest mechanism's tier-1 closure
//! probes.
//!
//! Each probe is a self-contained `.rs` file under
//! `tests/def278d_compile_fail/`. Trybuild attempts to compile each
//! file and compares the stderr output against the matching
//! `.stderr` golden. Mismatch = test failure with a coloured diff.
//!
//! # Probes
//!
//! - **P-D278D-1** `<DisconnectedPhase>::with_cancel_request()`
//!   → E0599 (method-absent — phase has no `with_cancel_request`).
//! - **P-D278D-2** `<ConnectingPhase>::with_cancel_request()`
//!   → E0599 (method-absent — per §8.5 decision, a driver must drive
//!   handshake to completion or drop the connection).
//! - **P-D278D-3** `<ClosedPhase>::with_cancel_request()` →
//!   E0599 (method-absent — terminal phase, no cancel surface).
//! - **P-D278D-4** `BackendKeyInstallToken` field-private struct
//!   literal → E0603 (the leaf submodule is `pub(crate)` so external
//!   code cannot name it; behind that the tuple-struct field is also
//!   private to the leaf).
//! - **P-D278D-5** `CancelRequestCredentials` no longer publicly
//!   exported post-Bundle-D' → E0432 (unresolved import). Bundle D'
//!   eliminated the public struct entirely; the closure-scoped
//!   `with_cancel_request` lends `&[u8; 16]` directly.
//! - **P-D278D-6** `&[u8; 16]` lent into the closure cannot escape
//!   past the call → `E0521` "borrowed data escapes outside of
//!   closure" (HRTB-quantified lifetime). This is the **closure-
//!   scope retention guarantee** — the tier elevation that
//!   Bundle D' lands.
//!
//! # Regenerating goldens
//!
//! ```sh
//! TRYBUILD=overwrite cargo test --test def278d_compile_fail
//! ```
//!
//! Overwrites every `.stderr` golden with the current compiler's
//! actual diagnostic. After regenerating, **review every diff** —
//! the test fails on stderr drift, so an `error[E0599]` re-shape in
//! a future rustc release surfaces as a noisy diff for review.

#![forbid(unsafe_code)]

/// **P-D278D-1** — `<DisconnectedPhase>::with_cancel_request()`
/// is method-absent. Expected: E0599.
#[test]
fn p_d278d_1_cancel_credentials_on_disconnected_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def278d_compile_fail/p_d278d_1_cancel_credentials_on_disconnected_absent.rs");
}

/// **P-D278D-2** — `<ConnectingPhase>::with_cancel_request()`
/// is method-absent. Expected: E0599.
#[test]
fn p_d278d_2_cancel_credentials_on_connecting_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def278d_compile_fail/p_d278d_2_cancel_credentials_on_connecting_absent.rs");
}

/// **P-D278D-3** — `<ClosedPhase>::with_cancel_request()` is
/// method-absent. Expected: E0599.
#[test]
fn p_d278d_3_cancel_credentials_on_closed_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def278d_compile_fail/p_d278d_3_cancel_credentials_on_closed_absent.rs");
}

/// **P-D278D-4** — minting a `BackendKeyInstallToken` from outside
/// the leaf submodule is rejected: the leaf is `pub(crate)`.
/// Expected: E0603 (module-private path) + secondary diagnostic on
/// the tuple-struct field privacy.
#[test]
fn p_d278d_4_backend_key_install_token_field_private() {
    trybuild::TestCases::new()
        .compile_fail("tests/def278d_compile_fail/p_d278d_4_backend_key_install_token_field_private.rs");
}

/// **P-D278D-5** — `CancelRequestCredentials` is no longer publicly
/// exported. Expected: E0432 (unresolved import).
#[test]
fn p_d278d_5_backend_key_cell_field_private() {
    trybuild::TestCases::new()
        .compile_fail("tests/def278d_compile_fail/p_d278d_5_backend_key_cell_field_private.rs");
}

/// **P-D278D-6** (Bundle D' new probe) — the `&[u8; 16]` lent into
/// the `with_cancel_request` closure cannot escape past the call.
/// Expected: `E0521`-class diagnostic ("borrowed data escapes
/// outside of closure" / "lifetime may not live long enough").
#[test]
fn p_d278d_6_lifetime_escape_from_closure() {
    trybuild::TestCases::new()
        .compile_fail("tests/def278d_compile_fail/p_d278d_6_lifetime_escape_from_closure.rs");
}
