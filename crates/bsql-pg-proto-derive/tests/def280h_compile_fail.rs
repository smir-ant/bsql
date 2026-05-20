//! `trybuild` golden harness for the `RowStream: !Send` tier-1
//! closure.
//!
//! Each probe is a self-contained `.rs` file under
//! `tests/def280h_compile_fail/`. Trybuild attempts to compile each
//! file and compares the stderr output against the matching
//! `.stderr` golden. Mismatch = test failure with a coloured diff.
//!
//! # Probes
//!
//! - **P-D280H-1** `RowStream<'_, '_>` does NOT implement `Send` →
//!   `E0277` "the trait bound `RowStream<...>: Send` is not satisfied".
//!   `RowStream` carries a private `PhantomData<*const ()>` field;
//!   `*const ()` is the canonical non-`Send` / non-`Sync` witness in
//!   `core::marker`. Auto-trait propagation flows through `&mut
//!   RowStream`, futures capturing such borrows, and any user-built
//!   container with a `RowStream` field — closing the
//!   `tokio::spawn(...&mut RowStream...)` race-with-Drop class by
//!   construction.
//!
//! # Regenerating goldens
//!
//! ```sh
//! TRYBUILD=overwrite cargo test --test def280h_compile_fail
//! ```
//!
//! Overwrites every `.stderr` golden with the current compiler's
//! actual diagnostic. After regenerating, **review every diff** —
//! the test fails on stderr drift, so an `error[E0277]` re-shape in
//! a future rustc release surfaces as a noisy diff for review.

#![forbid(unsafe_code)]

/// **P-D280H-1** — `RowStream<'_, '_>` is `!Send`. Expected: E0277.
#[test]
fn p_d280h_1_rowstream_not_send() {
    trybuild::TestCases::new()
        .compile_fail("tests/def280h_compile_fail/p_d280h_1_rowstream_not_send.rs");
}
