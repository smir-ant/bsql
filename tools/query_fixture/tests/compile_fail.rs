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
        // The user-defined-types channel, forwarded the same way so a
        // `bsql::user_types!()` in a child compile reaches the real enum set
        // (the generated-enum variant-set guarantee golden below).
        std::env::set_var("BSQL_USER_TYPES", env!("BSQL_USER_TYPES"));
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
    //   * An unknown name within one typo of a real one is enriched with a
    //     "did you mean `X`?" suggestion (a bounded restricted
    //     Damerau-Levenshtein match against the queried table's columns / the
    //     catalog's table names) — a transposed column and a one-key table
    //     typo. An unrelated name (the `nope` / `widgets` cases above) stays
    //     bare: no candidate is within threshold, so no misleading guess.
    t.compile_fail("tests/compile_fail/query_unknown_column_suggestion.rs");
    t.compile_fail("tests/compile_fail/query_unknown_table_suggestion.rs");
    t.compile_fail("tests/compile_fail/query_uncast_param.rs");
    t.compile_fail("tests/compile_fail/query_wrong_field.rs");
    t.compile_fail("tests/compile_fail/query_type_mismatch.rs");
    // The one-name collapse: a PLAIN `query!(Foo, "…")` makes the record `Foo`
    // itself the runnable carrier, so `conn.query::<Foo>()` is CORRECT (proven by
    // `compile_pass/query_one_name_ok.rs` below) and the former "record vs carrier"
    // footgun is unrepresentable. The `#[diagnostic::on_unimplemented]` on
    // `TypedQuery` now guards the residual misuse: a runtime `ORDER BY { ... }`
    // query's RECORD is not a carrier (each ordering is a separate `Foo...Query`
    // carrier picked via the `FooOrderBy` selector — one record cannot carry N
    // orderings' distinct prepared plans), so turbofishing it names the fix rather
    // than a raw unsatisfied-trait-bound wall.
    t.compile_fail("tests/compile_fail/query_not_a_carrier.rs");
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
    //   * It ALSO rejects a SAME-WIDTH ROW-OID drift (a `(u32,)` decoder with
    //     an `int4` row OID — both 4 bytes, different type) at E0080. This is
    //     the compile-side witness that the single-source unification closed
    //     the silent mis-decode class: the record decode and the wire OID now
    //     derive from the ONE row-tuple marker, so a same-width divergence
    //     cannot be silent — it is a const-eval failure here or an E0308 at
    //     the record. (Runtime half: `query_same_width_decode.rs`.)
    //   * The SCHEMA_PIN check rejects a baked Parse template whose OID
    //     section drifts from the declared param OIDs (E0080).
    //   * Layer 1 of the seal: a direct struct-literal fabrication is
    //     E0451 (private fields).
    //   * Layer 2 of the seal: a hand-written fingerprint carrier that
    //     lies about its shape fails through the `run` boundary (E0080).
    t.compile_fail("tests/compile_fail/query_wire_oid_drift.rs");
    t.compile_fail("tests/compile_fail/query_wire_row_oid_drift.rs");
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

    // `bsql::user_types!()` generated-enum surface: the generated `Mood` has
    // EXACTLY the migration's variant set, so naming a variant the migration did
    // not declare — the same situation as a variant renamed / deleted by a later
    // migration — is a compile error at the use site (E0599). Drift is a BUILD
    // error, exactly as a dropped column is on the `query!` path.
    t.compile_fail("tests/compile_fail/query_enum_unknown_variant.rs");

    // `bsql::user_types!()` generated-COMPOSITE surface: the generated `Addr`
    // struct has EXACTLY the migration's attribute set, so naming a field the
    // migration did not declare — the same situation as a field renamed /
    // dropped / retyped by a later `ALTER TYPE ... {DROP|RENAME|ALTER}
    // ATTRIBUTE` migration — is a compile error at the use site (E0609). Drift is
    // a BUILD error, the exact peer of the enum's variant-set guarantee.
    t.compile_fail("tests/compile_fail/query_composite_removed_field.rs");

    // A composite `$N` PARAMETER (the row-type binary ENCODE) is a STAGED
    // follow-up — decode is the high-value half and ships now. The blocker is
    // architectural: `record_recv` needs the composite's own + each field's
    // concrete type OID, which are server-dynamic, and bsql does no connect-time
    // OID resolution. So a composite parameter is a LOUD, located compile error,
    // never a half-correct encoder.
    t.compile_fail("tests/compile_fail/query_composite_param_unsupported.rs");

    // `copy!` + `copy_in_typed` compile-checked binary bulk-insert surface.
    //   * A row whose column TYPE does not match the catalog is a type mismatch
    //     at the `copy_in_typed` call (the row tuple does not match `Row<'q>`).
    //   * A row with the wrong ARITY (field count) is the same — the column list
    //     pins the tuple shape.
    //   * A `copy!` naming a column the catalog does not have is a
    //     `compile_error!` at the macro, never a silent pass that would only fail
    //     at COPY time.
    t.compile_fail("tests/compile_fail/copy_wrong_column_type.rs");
    t.compile_fail("tests/compile_fail/copy_wrong_arity.rs");
    t.compile_fail("tests/compile_fail/copy_unknown_column.rs");
    //   * A carrier naming MORE than 32 columns is a TAILORED compile error
    //     naming the cap + the escape hatch, not a raw ParamsWriter trait-bound
    //     failure (the row tuple's arity ceiling).
    t.compile_fail("tests/compile_fail/copy_over_column_cap.rs");

    // A heterogeneous `pipeline((...))` batch element is a `Bound<Q>` bound with
    // `Q::Params`; binding the WRONG parameter tuple to a carrier is `error[E0308]`
    // at the `bind`, so a mistyped command cannot ride a batch (the typed-per-
    // element guarantee holds at the batch boundary, same as a single `query`).
    t.compile_fail("tests/compile_fail/pipeline_wrong_param.rs");

    // A homogeneous `execute_batch` whose parameter tuples do not match the carrier's
    // `Params` is a type error at the call — a bulk write cannot carry a mistyped set.
    t.compile_fail("tests/compile_fail/execute_batch_wrong_param.rs");

    // The typed-RETURNING `query_batch` peer: a mistyped parameter set is the SAME
    // E0271 at the call — a bulk QUERY cannot carry a mistyped set either.
    t.compile_fail("tests/compile_fail/query_batch_wrong_param.rs");

    // SQL VIEWS (`0022_views.sql`):
    //   * a `query!` INSERT/UPDATE/DELETE ... RETURNING targeting a VIEW is a loud
    //     `WriteToView` — a view is not writable, so accepting the write at build
    //     time would be a build-passes / run-fails gap ("target the base table").
    t.compile_fail("tests/compile_fail/query_write_to_view.rs");
    //   * a column the view does NOT project does not resolve, even though the base
    //     table has it — the drift guarantee (a column a later `CREATE OR REPLACE
    //     VIEW` drops from the view stops compiling), surfaced as `UnknownColumn`.
    t.compile_fail("tests/compile_fail/query_view_dropped_column.rs");

    // The one-name collapse type-checks: `conn.query::<User>()` runs the record
    // directly (record IS carrier), and `query_one`/`query_opt` return the OWNED
    // `User` — the GREEN peer of the `query_not_a_carrier` residual-misuse golden.
    t.pass("tests/compile_pass/query_one_name_ok.rs");
    // Every valid dynamic form type-checks at macro expansion.
    t.pass("tests/compile_pass/query_dynamics_ok.rs");
    // The valid `copy!` + `copy_in_typed` happy path type-checks (GREEN peer of
    // the `copy_wrong_*` goldens).
    t.pass("tests/compile_pass/copy_typed_ok.rs");
}
