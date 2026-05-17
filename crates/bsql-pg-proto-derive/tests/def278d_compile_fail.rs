//! DEF-278 Bundle D (2026-05-17) — `trybuild` golden harness for the
//! CancelRequest mechanism's tier-1 closure probes.
//!
//! Each probe is a self-contained `.rs` file under
//! `tests/def278d_compile_fail/`. Trybuild attempts to compile each
//! file and compares the stderr output against the matching
//! `.stderr` golden. Mismatch = test failure with a coloured diff.
//!
//! # Probes
//!
//! - **P-D278D-1** `<DisconnectedPhase>::cancel_request_credentials()`
//!   → E0599 (method-absent — phase has no `cancel_request_credentials`).
//! - **P-D278D-2** `<ConnectingPhase>::cancel_request_credentials()`
//!   → E0599 (method-absent — per §8.5 decision, a driver must drive
//!   handshake to completion or drop the connection).
//! - **P-D278D-3** `<ClosedPhase>::cancel_request_credentials()` →
//!   E0599 (method-absent — terminal phase, no cancel surface).
//! - **P-D278D-4** `BackendKeyInstallToken` field-private struct
//!   literal → E0451 (the inner `()` tuple-struct field is private to
//!   `_backend_key_install_leaf`, so external code cannot mint a token).
//! - **P-D278D-5** `BackendKeyCell` field-private direct access →
//!   E0616 (the inner `Option<BackendKey>` is private to `mod cancel`,
//!   so external code cannot read/write the cell's payload directly).
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

/// **P-D278D-1** — `<DisconnectedPhase>::cancel_request_credentials()`
/// is method-absent. Expected: E0599.
#[test]
fn p_d278d_1_cancel_credentials_on_disconnected_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def278d_compile_fail/p_d278d_1_cancel_credentials_on_disconnected_absent.rs");
}

/// **P-D278D-2** — `<ConnectingPhase>::cancel_request_credentials()`
/// is method-absent. Expected: E0599.
#[test]
fn p_d278d_2_cancel_credentials_on_connecting_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def278d_compile_fail/p_d278d_2_cancel_credentials_on_connecting_absent.rs");
}

/// **P-D278D-3** — `<ClosedPhase>::cancel_request_credentials()` is
/// method-absent. Expected: E0599.
#[test]
fn p_d278d_3_cancel_credentials_on_closed_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def278d_compile_fail/p_d278d_3_cancel_credentials_on_closed_absent.rs");
}

/// **P-D278D-4** — minting a `BackendKeyInstallToken` from outside
/// the leaf submodule is rejected: the inner `()` tuple-struct field
/// is private. Expected: E0451 / E0603 (field-private literal +
/// possibly module-private type).
#[test]
fn p_d278d_4_backend_key_install_token_field_private() {
    trybuild::TestCases::new()
        .compile_fail("tests/def278d_compile_fail/p_d278d_4_backend_key_install_token_field_private.rs");
}

/// **P-D278D-5** — accessing `BackendKeyCell`'s inner field from
/// outside `mod cancel` is rejected: the field is private.
/// Expected: E0616 (or E0603 on the module itself if pub(crate)).
#[test]
fn p_d278d_5_backend_key_cell_field_private() {
    trybuild::TestCases::new()
        .compile_fail("tests/def278d_compile_fail/p_d278d_5_backend_key_cell_field_private.rs");
}
