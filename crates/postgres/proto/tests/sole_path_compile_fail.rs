//! Sole-path injection seam — `trybuild` golden harness.
//!
//! The macro-minted typed query / Fragment builder is the intended
//! construction path for a wire-bound SQL command. The `sql` field on
//! [`bsql_postgres_proto::push_command::SimpleQuery`] and `Parse` is
//! `pub(crate)`, so an EXTERNAL crate cannot write a bare `SimpleQuery {
//! sql, .. }` struct literal — it is a compile error (E0451). There is
//! therefore no struct-literal back-door that lets arbitrary runtime text
//! reach the wire; the only construction route is the explicit `::new`
//! seam.
//!
//! Two legs, matching the make-or-break design:
//! - `struct_literal_sealed.rs` → E0451 — the tier-1-against-struct-literal
//!   leg (an external crate physically cannot construct the text-bearing
//!   command by struct literal).
//! - `new_constructor_passes.rs` → compiles — the sanctioned `::new` seam
//!   (tier-3-by-discipline: hand-callable, but the single explicit,
//!   greppable raw-SQL entry point the macro / Fragment builder routes
//!   through).
//!
//! Regenerate goldens after an intentional diagnostic change:
//! ```sh
//! TRYBUILD=overwrite cargo test -p bsql-postgres-proto --test sole_path_compile_fail
//! ```
//! Then review every `.stderr` diff.

#![forbid(unsafe_code)]

/// The `sql` field is `pub(crate)`: an external struct literal of a
/// text-bearing `PushCommand` is E0451. This is the seam that makes the
/// macro / Fragment builder the only construction path.
#[test]
fn struct_literal_construction_is_sealed() {
    trybuild::TestCases::new()
        .compile_fail("tests/sole_path_compile_fail/struct_literal_sealed.rs");
}

/// The sanctioned `::new` seam compiles from a downstream crate — proving
/// the macro / Fragment builder has a construction path (and documenting
/// that the constructor itself is tier-3-by-discipline, not unreachable).
#[test]
fn new_constructor_is_the_sanctioned_path() {
    trybuild::TestCases::new()
        .pass("tests/sole_path_compile_fail/new_constructor_passes.rs");
}
