//! `Col` identifier vocabulary — `trybuild` golden compile-fail harness.
//!
//! The closed identifier universe, the closed column-type set, the
//! mandatory-enum runtime carrier, enum completeness, the zero-sized
//! footprint pin, and the `&'static str` return-type moat are all tier-1
//! compiler invariants. This harness is their durable documentation
//! (CREDO Part III-C: compile-time invariant documentation — no runtime
//! test for what the compiler enforces).
//!
//! Each case below maps to one invariant and one rustc diagnostic. The
//! locked `.stderr` goldens fail the build if a diagnostic drifts — for
//! example if a contributor weakens a seal or relaxes the `as_sql`
//! return type from `&'static str` to `&str`.
//!
//! Regenerate goldens after an intentional diagnostic change, then review
//! every `.stderr` diff:
//! ```sh
//! TRYBUILD=overwrite cargo test -p bsql-postgres-core --test col_compile_fail
//! ```

#![forbid(unsafe_code)]

/// Leg-a headline: a raw `&str` is not a column identifier. `impl Col for
/// &str` is `E0117` (orphan rule — `str` is foreign). There is no
/// raw-`&str` -> identifier path.
#[test]
fn impl_col_for_str_is_orphan_rejected() {
    trybuild::TestCases::new().compile_fail("tests/ui/impl_col_for_str.rs");
}

/// Leg-a companion: a local un-sealed type cannot be a `Col`. `impl Col
/// for Bogus` is `E0277` (the `col_seal::Sealed` supertrait is not
/// satisfied); rustc lists the declared columns as the only impls.
#[test]
fn impl_col_local_without_seal_is_rejected() {
    trybuild::TestCases::new().compile_fail("tests/ui/impl_col_local_no_seal.rs");
}

/// A raw `&str` is unusable where an identifier (`AsIdent`/`Col`) is
/// required — `E0277`. Closes the "just pass the string" hole.
#[test]
fn raw_str_is_not_usable_as_identifier() {
    trybuild::TestCases::new().compile_fail("tests/ui/raw_str_as_ident.rs");
}

/// Closed column-type set, via `columns!`: a column declared with an
/// unsupported value type (`f64`) is `E0277` (`ColType` unsatisfied).
#[test]
fn columns_macro_rejects_unsupported_column_type() {
    trybuild::TestCases::new().compile_fail("tests/ui/bad_col_type.rs");
}

/// Closed column-type set, directly: `impl ColType for f64` is `E0117`
/// (orphan — `f64` is a foreign primitive). A seventh column type is
/// impossible.
#[test]
fn impl_coltype_for_foreign_primitive_is_rejected() {
    trybuild::TestCases::new().compile_fail("tests/ui/impl_coltype_f64.rs");
}

/// The MOAT CEILING — and the load-bearing proof. The injection
/// guarantee rests on `Col::as_sql` returning `&'static str`: an
/// `as_sql` that tries to return a *runtime* `String`'s slice is `E0515`
/// (cannot return a value referencing a local). A hostile downstream
/// impl cannot smuggle runtime text into identifier position.
#[test]
fn runtime_string_in_as_sql_is_rejected() {
    trybuild::TestCases::new().compile_fail("tests/ui/runtime_escape_as_sql.rs");
}

/// The runtime carrier MUST be an enum: `Col: Copy` forces `Self: Sized`,
/// so `Col` is not dyn-compatible and `&dyn Col` is `E0038`. `DynCol` is
/// not a stylistic choice — it is the only legal runtime carrier.
#[test]
fn col_is_not_dyn_compatible() {
    trybuild::TestCases::new().compile_fail("tests/ui/col_not_dyn.rs");
}

/// Column-completeness at the match site: `DynCol` is not
/// `#[non_exhaustive]`, so a downstream `match` that forgets a column is
/// `E0004` (non-exhaustive patterns).
#[test]
fn dyncol_match_must_be_exhaustive() {
    trybuild::TestCases::new().compile_fail("tests/ui/dyncol_missing_arm.rs");
}

/// Build-time footprint pin (tier-1 degradation): the macro emits
/// `const _: () = assert!(size_of::<$col>() == 0)`, so a regression that
/// makes a column marker field-bearing is `E0080` — fired for a type
/// that need never be instantiated.
#[test]
fn nonzero_sized_column_marker_is_rejected() {
    trybuild::TestCases::new().compile_fail("tests/ui/zst_pin_field.rs");
}
