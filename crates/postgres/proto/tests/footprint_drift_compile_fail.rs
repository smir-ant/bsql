//! Footprint-drift contract — `trybuild` golden harness.
//!
//! [`bsql_postgres_proto::wire_pin!`] pins the `size_of` AND `align_of`
//! of a wire type with a free-standing `const _: () = { … }` item. A
//! drift turns one of those `assert!`s false and the build aborts with
//! `E0080` const-eval failure — **at `cargo check`, including for a type
//! constructed nowhere**, not as a `cargo test`-only catch.
//!
//! This harness pins the build-failure CONTRACT from a downstream crate's
//! point of view (the doc-tests on the macro pin the same thing in-crate;
//! these prove it across the crate boundary, which is where real
//! consumers and new wire types live). Two legs, one per dimension:
//!
//! - `size_drift.rs`  → E0080 — a pin with the wrong `size`.
//! - `align_drift.rs` → E0080 — a SIZE-PRESERVING `align` drift, the
//!   dimension the historical size-only anchors could not see.
//!
//! Regenerate goldens after an intentional diagnostic change:
//! ```sh
//! TRYBUILD=overwrite cargo test -p bsql-postgres-proto \
//!     --test footprint_drift_compile_fail
//! ```
//! Then review every `.stderr` diff.

#![forbid(unsafe_code)]

/// A `wire_pin!` whose pinned `size` does not match the type's actual
/// `size_of` is an `E0080` build failure (the size dimension).
#[test]
fn size_drift_is_e0080() {
    trybuild::TestCases::new()
        .compile_fail("tests/footprint_drift_compile_fail/size_drift.rs");
}

/// A `wire_pin!` whose pinned `align` does not match the type's actual
/// `align_of` is an `E0080` build failure — even when `size` is
/// preserved. This is the dimension a bare `size_of` anchor misses.
#[test]
fn align_drift_is_e0080() {
    trybuild::TestCases::new()
        .compile_fail("tests/footprint_drift_compile_fail/align_drift.rs");
}
