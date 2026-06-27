//! Active-phase pull-surface compile-fail goldens.
//!
//! Three properties the active engine enforces at the type/borrow level:
//!
//! - `fail_event_held.rs`        → E0499 — a borrow-through `Event` cannot
//!   outlive the next mutating call (the no-escape wall).
//! - `fail_cross_phase_event.rs` → E0308 — the active `Event` and connecting
//!   `AuthEvent` vocabularies are distinct; a cross-phase frame is a type
//!   mismatch by omission, never a runtime guard.
//! - `fail_nonexhaustive_event.rs` → E0004 — the `Event` vocabulary is closed;
//!   dropping a within-vocabulary arm without a wildcard fails exhaustiveness.
//!
//! Regenerate goldens after an intentional diagnostic change:
//! ```sh
//! TRYBUILD=overwrite cargo test -p bsql-postgres-proto \
//!     --test engine_active_compile_fail
//! ```
//! Then review every `.stderr` diff.

#![forbid(unsafe_code)]

/// A borrow-through `Event` cannot outlive the next mutating call — E0499.
#[test]
fn borrow_through_event_across_read_slot_is_e0499() {
    trybuild::TestCases::new().compile_fail("tests/engine_active/fail_event_held.rs");
}

/// An active-phase `Event` is not a connecting-phase `AuthEvent` — E0308.
#[test]
fn active_event_is_not_auth_event_is_e0308() {
    trybuild::TestCases::new().compile_fail("tests/engine_active/fail_cross_phase_event.rs");
}

/// A within-vocabulary `Event` match missing an arm (no wildcard) — E0004.
#[test]
fn nonexhaustive_event_match_is_e0004() {
    trybuild::TestCases::new().compile_fail("tests/engine_active/fail_nonexhaustive_event.rs");
}
