//! DEF-246 Phase 2/3/4 (2026-05-16) — `trybuild` golden harness for the
//! consume-self phase-typed protocol's tier-1 closure probes.
//!
//! Each probe is a self-contained `.rs` file under
//! `tests/def246_phase2-4_compile_fail/`. Trybuild attempts to compile
//! each file and compares the stderr output against the matching
//! `.stderr` golden. Mismatch = test failure with a coloured diff.
//!
//! # Probes
//!
//! - **P-E-1** `new_active_for_test()` → E0599 (method does not exist
//!   on any phase — Approach E deletes the bypass surface entirely).
//! - **P-E-2** `__test_bypass_into_active()` → E0599 (same).
//! - **P-E-3** `use bsql_pg_proto::push_command::Startup` → E0432
//!   (the struct is deleted; only the per-phase `push_startup`
//!   consume-self method exists).
//! - **P-E-4** `<DisconnectedPhase>::push_command(Ping)` → E0599
//!   (Phase 2 elevation #1 — Disconnected lacks `push_command`).
//! - **P-E-5** `<ConnectingPhase>::push_command(Ping)` → E0599
//!   (Phase 3 elevation #2 — Connecting lacks `push_command`).
//! - **P-E-6** `<ClosedPhase>::push_command(Ping)` → E0599
//!   (Phase 4 elevation #3 — Closed lacks `push_command`).
//! - **P-E-7** `<ClosedPhase>::feed_inbound(...)` → E0599 (same).
//! - **P-E-8** struct-literal `PgProtocol { inner, phase_marker }` →
//!   E0451 (the `inner` and `phase_marker` fields are
//!   module-private to `mod protocol`).
//! - **P-E-9** discarded `feed_inbound`'s `Result<(), ProtocolError>` →
//!   `unused_must_use` + `-D warnings` = compile error
//!   (Phase 4 elevation #4 — `#[must_use]` on the return).
//!
//! # Regenerating goldens
//!
//! ```sh
//! TRYBUILD=overwrite cargo test --test def246_phase2-4_compile_fail
//! ```
//!
//! Overwrites every `.stderr` golden with the current compiler's
//! actual diagnostic. After regenerating, **review every diff**.

#![forbid(unsafe_code)]

/// **P-E-1** — `proto.new_active_for_test()` does not exist
/// (Approach E deleted the bypass surface). Expected: E0599.
#[test]
fn p_e_1_new_active_for_test_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def246_phase2-4_compile_fail/p_e_1_new_active_for_test_absent.rs");
}

/// **P-E-2** — `proto.__test_bypass_into_active()` does not exist.
/// Expected: E0599.
#[test]
fn p_e_2_test_bypass_into_active_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def246_phase2-4_compile_fail/p_e_2_test_bypass_into_active_absent.rs");
}

/// **P-E-3** — `push_command::Startup` struct is deleted; importing
/// it fails with E0432.
#[test]
fn p_e_3_push_command_startup_struct_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def246_phase2-4_compile_fail/p_e_3_push_command_startup_struct_absent.rs");
}

/// **P-E-4** — `<DisconnectedPhase>::push_command(Ping)` is method-
/// absent. Expected: E0599.
#[test]
fn p_e_4_disconnected_push_command_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def246_phase2-4_compile_fail/p_e_4_disconnected_push_command_absent.rs");
}

/// **P-E-5** — `<ConnectingPhase>::push_command(Ping)` is method-
/// absent. Expected: E0599.
#[test]
fn p_e_5_connecting_push_command_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def246_phase2-4_compile_fail/p_e_5_connecting_push_command_absent.rs");
}

/// **P-E-6** — `<ClosedPhase>::push_command(Ping)` is method-absent.
/// Expected: E0599.
#[test]
fn p_e_6_closed_push_command_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def246_phase2-4_compile_fail/p_e_6_closed_push_command_absent.rs");
}

/// **P-E-7** — `<ClosedPhase>::feed_inbound(...)` is method-absent.
/// Expected: E0599.
#[test]
fn p_e_7_closed_feed_inbound_absent() {
    trybuild::TestCases::new()
        .compile_fail("tests/def246_phase2-4_compile_fail/p_e_7_closed_feed_inbound_absent.rs");
}

/// **P-E-8** — struct-literal `PgProtocol { inner, phase_marker }`
/// is blocked by field privacy. Expected: E0451.
#[test]
fn p_e_8_struct_literal_field_private() {
    trybuild::TestCases::new()
        .compile_fail("tests/def246_phase2-4_compile_fail/p_e_8_struct_literal_field_private.rs");
}

/// **P-E-9** — discarding `feed_inbound`'s `Result<(), ProtocolError>`
/// trips `unused_must_use` + `-D warnings`. Expected: compile error.
#[test]
fn p_e_9_feed_inbound_result_must_use() {
    trybuild::TestCases::new()
        .compile_fail("tests/def246_phase2-4_compile_fail/p_e_9_feed_inbound_result_must_use.rs");
}
