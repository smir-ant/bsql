//! trybuild goldens: a `query!` against an unknown table / column is a
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

    // `query!` schema-resolution surface: a reference the inference engine
    // cannot resolve against the migration-replayed catalog is a
    // `compile_error!` at the SQL literal, never a silent pass.
    //   * An unknown table is `InferError::UnknownRelation`.
    //   * A table renamed away by `ALTER TABLE ... RENAME TO` no longer
    //     resolves under its OLD name — the same `UnknownRelation`, which is
    //     the freshness guarantee for renames (the old name was re-keyed out
    //     of the catalog).
    //   * An unknown column is `InferError::UnknownColumn`.
    //   * An uncast parameter is a schema-typing error.
    //   * A missing / wrong-typed record field is the typed record doing its
    //     job (E0609 / E0308) — proving the emitted record is genuinely
    //     typed, not a `Vec<String>` escape hatch.
    t.compile_fail("tests/compile_fail/query_unknown_table.rs");
    t.compile_fail("tests/compile_fail/query_renamed_away_table.rs");
    t.compile_fail("tests/compile_fail/query_unknown_column.rs");
    t.compile_fail("tests/compile_fail/query_uncast_param.rs");
    t.compile_fail("tests/compile_fail/query_wrong_field.rs");
    t.compile_fail("tests/compile_fail/query_type_mismatch.rs");
    // The widened `{f32, f64, bytea}` types keep the wrong-type wall: a
    // `float4` column's record field is `f32`, and using it where an `f64`
    // is expected is E0308 — widening did not weaken the type safety.
    t.compile_fail("tests/compile_fail/query_float_type_mismatch.rs");
    // A duplicate output column name cannot become two record fields of
    // one name — surfaced as a compile_error, never silently collapsed.
    t.compile_fail("tests/compile_fail/query_duplicate_column.rs");

    // `query!` const wire-artifact + fingerprint-seal surface.
    //   * The validating constructor rejects a param-OID drift
    //     (E0080) — there is no unchecked twin.
    //   * The SCHEMA_PIN check rejects a baked Parse template whose OID
    //     section drifts from the declared param OIDs (E0080).
    //   * Layer 1 of the seal: a direct struct-literal fabrication is
    //     E0451 (private fields).
    //   * Layer 2 of the seal: a hand-written fingerprint carrier that
    //     lies about its shape fails through the `run` boundary (E0080).
    t.compile_fail("tests/compile_fail/query_wire_oid_drift.rs");
    t.compile_fail("tests/compile_fail/query_wire_schema_pin_drift.rs");
    t.compile_fail("tests/compile_fail/query_hostile_construction.rs");
    t.compile_fail("tests/compile_fail/query_hostile_fingerprint.rs");

    // `PreparedQuery` seal hostile-probe matrix. The type is minted only by
    // the validating constructor the `query!` macro routes through; these
    // pin that every other minting / mutation / fabrication path is a
    // compile error. Field privacy is `error[E0616]`, the absence of a
    // public `new` is `error[E0599]`, an external struct literal is
    // `error[E0451]`, a hostile `ParamsWriter` impl is `error[E0277]`
    // (sealed super-trait), and every `unsafe` fabrication / mutation route
    // is barred by each probe's `#![forbid(unsafe_code)]` (the language half
    // of the OS-boundary closure). `query!` requiring a string literal —
    // a runtime string is rejected at expansion — closes the injection
    // class at the macro input.
    t.compile_fail("tests/compile_fail/query_seal_no_public_new.rs");
    t.compile_fail("tests/compile_fail/query_seal_field_read.rs");
    t.compile_fail("tests/compile_fail/query_seal_unsafe_field_mutate.rs");
    t.compile_fail("tests/compile_fail/query_seal_unsafe_fabricate.rs");
    t.compile_fail("tests/compile_fail/query_seal_hostile_paramswriter.rs");
    t.compile_fail("tests/compile_fail/query_seal_box_leak.rs");
    t.compile_fail("tests/compile_fail/query_seal_mem_transmute.rs");
    t.compile_fail("tests/compile_fail/query_seal_stmt_name_read.rs");
    t.compile_fail("tests/compile_fail/query_seal_wire_template.rs");
    t.compile_fail("tests/compile_fail/query_arg_type_mismatch.rs");
    t.compile_fail("tests/compile_fail/query_runtime_string.rs");

    // `query!` DYNAMIC-form surface.
    //   * The optional-filter budget is a const-eval cap: nine
    //     `OPTIONAL(...)` filters exceed `MAX_OPTIONAL_FILTERS = 8`
    //     (`error[E0080]`) — never a silent truncation.
    //   * The runtime ORDER BY budget is the matching const-eval cap:
    //     seventeen orderings exceed `MAX_ORDER_BY_VARIANTS = 16`
    //     (`error[E0080]`) — never a silent truncation of orderings.
    //   * A runtime ORDER BY option naming a non-existent column is a
    //     schema-typing `compile_error!` (the ordering is inference-
    //     validated, so it cannot fall "outside" the real columns).
    //   * The runtime ORDER BY selector is a CLOSED enum: an undeclared
    //     ordering is `error[E0599]` (no such variant) — unrepresentable,
    //     so no SQL string is built and there is no injection surface.
    t.compile_fail("tests/compile_fail/query_optional_budget_exceeded.rs");
    t.compile_fail("tests/compile_fail/query_order_by_budget_exceeded.rs");
    t.compile_fail("tests/compile_fail/query_order_by_unknown_column.rs");
    t.compile_fail("tests/compile_fail/query_order_by_outside_allow_set.rs");

    // `query!` typed-execution surface (the bounded `Rows<Q>` path).
    //   * A borrowed record from `Rows::iter()` borrows the `Rows` buffer, so it
    //     cannot outlive it: dropping the `Rows` while a borrowed record is held
    //     is `error[E0505]` — the compiler-enforced escape wall.
    //   * The STREAMING peer: a record handed to a `query_each` closure cannot
    //     escape it — stashing it in an outer `Vec` violates the `for<'q>` HRTB
    //     (borrowed data escapes) — the streaming escape wall.
    t.compile_fail("tests/compile_fail/query_rows_escape.rs");
    t.compile_fail("tests/compile_fail/query_each_escape.rs");

    // Every valid dynamic form type-checks at macro expansion.
    t.pass("tests/compile_pass/query_dynamics_ok.rs");
}
