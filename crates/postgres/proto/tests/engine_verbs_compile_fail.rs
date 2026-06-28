//! Active-phase verb compile-fail goldens.
//!
//! - `fail_use_after_close.rs` → E0382 — a `PreparedStatement` is consumed by
//!   `close_statement`, so using it after close is a use-of-moved-value error
//!   (the compile-time half of the use-after-close safety invariant).
//!
//! Regenerate goldens after an intentional diagnostic change:
//! ```sh
//! TRYBUILD=overwrite cargo test -p bsql-postgres-proto \
//!     --test engine_verbs_compile_fail
//! ```
//! Then review every `.stderr` diff.

#![forbid(unsafe_code)]

/// A `PreparedStatement` used after `close_statement` consumes it — E0382.
#[test]
fn use_after_close_is_e0382() {
    trybuild::TestCases::new().compile_fail("tests/engine_verbs/fail_use_after_close.rs");
}
