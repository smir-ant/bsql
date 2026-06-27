//! Linearity + brand seam — `trybuild` golden harness.
//!
//! The engine's [`Live`](bsql_postgres_proto::engine::Live) token is a
//! branded, non-`Clone`, linear value: each `session(..)` scope mints a
//! fresh invariant brand, and every verb consumes the token and returns it
//! only at a clean protocol boundary. These goldens pin the three failure
//! modes that make the discipline tier-1 (a compile error, not a runtime
//! check), plus the sanctioned positive path:
//!
//! - `fail_reuse.rs`        → E0382 — reuse after a verb consumes the token.
//! - `fail_brand_cross.rs`  → a foreign-brand token cannot drive another
//!   session's engine.
//! - `fail_brand_escape.rs` → the brand cannot escape its `session()` scope.
//! - `pass_threading.rs`    → the linear token threads through two
//!   sequential async verbs in one scope (the R4 shape) and compiles.
//!
//! Regenerate goldens after an intentional diagnostic change:
//! ```sh
//! TRYBUILD=overwrite cargo test -p bsql-postgres-proto \
//!     --test engine_linearity_compile_fail
//! ```
//! Then review every `.stderr` diff.

#![forbid(unsafe_code)]

/// Reusing a `Live` token after a verb has consumed it is E0382.
#[test]
fn reuse_after_consume_is_e0382() {
    trybuild::TestCases::new().compile_fail("tests/engine_linearity/fail_reuse.rs");
}

/// A token branded to one session cannot drive another session's engine.
#[test]
fn foreign_brand_is_rejected() {
    trybuild::TestCases::new().compile_fail("tests/engine_linearity/fail_brand_cross.rs");
}

/// The generative brand cannot escape its `session()` scope.
#[test]
fn brand_cannot_escape_scope() {
    trybuild::TestCases::new().compile_fail("tests/engine_linearity/fail_brand_escape.rs");
}

/// The sanctioned path — sequential async verbs threading one token —
/// compiles.
#[test]
fn sequential_threading_compiles() {
    trybuild::TestCases::new().pass("tests/engine_linearity/pass_threading.rs");
}
