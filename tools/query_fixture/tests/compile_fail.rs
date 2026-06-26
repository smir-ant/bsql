//! trybuild goldens: an unknown table / column reference is a
//! `compile_error!`, not a silent pass.
//!
//! trybuild compiles each `compile_fail/*.rs` as its own crate via a
//! spawned `cargo`. Those crates do NOT have this fixture's `build.rs`,
//! so they would not, on their own, see the `BSQL_SCHEMA_CATALOG`
//! rustc-env channel. We forward it: `env!("BSQL_SCHEMA_CATALOG")`
//! resolves HERE at this test crate's own compile time (the rustc-env
//! set by our `build.rs` applies to every rustc invocation of this
//! crate, including its test targets), and we re-export it into the
//! child compile's environment. The macro then reaches a real catalog
//! and emits the precise "unknown table/column" diagnostic the golden
//! pins — proving the rejection is schema-driven, not a missing-catalog
//! artifact.

#[test]
fn unknown_reference_is_compile_error() {
    // SAFETY: `set_var` is `unsafe` in edition 2024 because concurrent
    // env access is a data race. This single-test file runs serially
    // (one test, no threads spawned) and sets the var once before any
    // trybuild child is spawned, so there is no concurrent reader. The
    // value is this crate's own catalog path, baked in as a rustc-env by
    // our `build.rs` and captured at compile time via `env!`.
    unsafe {
        std::env::set_var("BSQL_SCHEMA_CATALOG", env!("BSQL_SCHEMA_CATALOG"));
    }
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/unknown_column.rs");
    t.compile_fail("tests/compile_fail/unknown_table.rs");
    // A table renamed away by `ALTER TABLE ... RENAME TO` no longer
    // resolves under its OLD name — the freshness guarantee for renames.
    t.compile_fail("tests/compile_fail/renamed_away_table.rs");

    // `query!` compile-fail surface. Two are schema-typing errors the
    // inference engine surfaces as `compile_error!` (an unknown column, an
    // uncast parameter); two are the typed record doing its job (a missing
    // field is E0609, a wrong-typed field is E0308) — proving the emitted
    // record is genuinely typed, not a `Vec<String>` escape hatch.
    t.compile_fail("tests/compile_fail/query_unknown_column.rs");
    t.compile_fail("tests/compile_fail/query_uncast_param.rs");
    t.compile_fail("tests/compile_fail/query_wrong_field.rs");
    t.compile_fail("tests/compile_fail/query_type_mismatch.rs");
    // A duplicate output column name cannot become two record fields of
    // one name — surfaced as a compile_error, never silently collapsed.
    t.compile_fail("tests/compile_fail/query_duplicate_column.rs");
}
