//! `Fragment` builder — `trybuild` golden compile-fail harness (slice 2).
//!
//! The injection wall (`Chunk::Rodata: &'static str`), the closed bind
//! carrier (`BoundValue`), the non-bindable-hole rejection (`IntoBound`),
//! the absence of any raw-`&str` -> `Fragment` constructor, and the
//! `fragment!` macro's literal-skeleton + arity walls are all tier-1
//! compiler invariants. This harness is their durable documentation
//! (CREDO Part III-C: compile-time invariant documentation — no runtime
//! test for what the compiler enforces).
//!
//! Each case maps to one invariant and one rustc diagnostic. The locked
//! `.stderr` goldens fail the build if a diagnostic drifts — for example
//! if a contributor adds a `Raw` variant to `BoundValue`, relaxes
//! `Chunk::Rodata` from `&'static str` to `&str`, or adds a
//! `From<&str>`/`from_str` constructor.
//!
//! Regenerate goldens after an intentional diagnostic change, then review
//! every `.stderr` diff:
//! ```sh
//! TRYBUILD=overwrite cargo test -p bsql-postgres-core --test fragment_compile_fail
//! ```

#![forbid(unsafe_code)]

/// NON-BINDABLE HOLE = E0277. `f64` does not implement `IntoBound`; a `{}`
/// hole accepts exactly i16, i32, i64, u32, bool, &str, String.
#[test]
fn non_bindable_f64_hole_is_e0277() {
    trybuild::TestCases::new().compile_fail("tests/ui/fragment_hole_f64.rs");
}

/// NON-BINDABLE HOLE = E0277. A local struct does not implement
/// `IntoBound`.
#[test]
fn non_bindable_struct_hole_is_e0277() {
    trybuild::TestCases::new().compile_fail("tests/ui/fragment_hole_struct.rs");
}

/// NO `Fragment::from_str` = E0599. There is no runtime-string -> SQL
/// path; a `Fragment` is constructed only via `fragment!`.
#[test]
fn no_fragment_from_str() {
    trybuild::TestCases::new().compile_fail("tests/ui/fragment_no_from_str.rs");
}

/// NO `From<&str>` for `Fragment` = E0277. A runtime `&str` cannot be
/// converted into a `Fragment`.
#[test]
fn no_fragment_from_str_via_from() {
    trybuild::TestCases::new().compile_fail("tests/ui/fragment_no_from.rs");
}

/// THE INJECTION WALL = E0597. A runtime `String`'s slice cannot enter a
/// `Chunk::Rodata` (`&'static str`), even via the doc-hidden
/// `__from_chunks` — and it holds cross-crate (this test file is a
/// separate crate).
#[test]
fn runtime_string_in_rodata_chunk_is_e0597() {
    trybuild::TestCases::new().compile_fail("tests/ui/fragment_runtime_rodata.rs");
}

/// CLOSED CARRIER = E0599. `BoundValue` has no `Raw` variant; a value can
/// only ever become a binary `$N` block, never spine text.
#[test]
fn boundvalue_has_no_raw_variant() {
    trybuild::TestCases::new().compile_fail("tests/ui/boundvalue_no_raw.rs");
}

/// THE MACRO LITERAL WALL. The `fragment!` skeleton must be a string
/// literal; a runtime `String` (non-literal first argument) is a compile
/// error — no runtime-string -> SQL skeleton path through the macro.
#[test]
fn macro_rejects_runtime_string_skeleton() {
    trybuild::TestCases::new().compile_fail("tests/ui/fragment_runtime_skeleton.rs");
}

/// THE MACRO ARITY WALL. Every `{}` hole consumes exactly one positional
/// argument; a hole/argument-count mismatch is a compile error.
#[test]
fn macro_rejects_arity_mismatch() {
    trybuild::TestCases::new().compile_fail("tests/ui/fragment_arity_mismatch.rs");
}

// ---------------------------------------------------------------------
// SLICE 3 — the typed combinator surface.
// ---------------------------------------------------------------------

/// THE MAKE-OR-BREAK GUARD = E0308. A `&str` value bound against an `i16`
/// column (`users::age.gt("oops")`) does not unify with the column's value
/// type and is a compile error. This is the "better than sqlx" payoff:
/// compile-time column↔value type checking.
#[test]
fn wrong_typed_predicate_value_str_on_i16_is_e0308() {
    trybuild::TestCases::new().compile_fail("tests/ui/pred_str_on_i16.rs");
}

/// TYPED GUARD = E0308. An `i32` value bound against a `Text` column (value
/// type `&str`) is a compile error.
#[test]
fn wrong_typed_predicate_value_i32_on_text_is_e0308() {
    trybuild::TestCases::new().compile_fail("tests/ui/pred_i32_on_text.rs");
}

/// TYPED GUARD = E0308. A `Text` column's value type is `&str`, not an owned
/// `String`; binding a `String` is a compile error.
#[test]
fn owned_string_on_text_column_is_e0308() {
    trybuild::TestCases::new().compile_fail("tests/ui/pred_string_on_text.rs");
}

/// TYPED GUARD = E0308. An `i32` value bound against a `bool` column is a
/// compile error.
#[test]
fn int_on_bool_column_is_e0308() {
    trybuild::TestCases::new().compile_fail("tests/ui/pred_int_on_bool.rs");
}

/// NO SILENT WIDENING = E0308 (literal form). An `i32` *literal* bound
/// against an `i16` column is a compile error — the guard never widens.
#[test]
fn wrong_int_width_literal_is_e0308() {
    trybuild::TestCases::new().compile_fail("tests/ui/pred_i32_literal_on_i16.rs");
}

/// NO SILENT WIDENING = E0308 (binding form). An `i32` *variable* bound
/// against an `i16` column is a compile error; rustc suggests `try_into`.
#[test]
fn wrong_int_width_binding_is_e0308() {
    trybuild::TestCases::new().compile_fail("tests/ui/pred_i32_var_on_i16.rs");
}

/// OUT-OF-RANGE LITERAL. An unsuffixed literal that infers to the column's
/// value type (`i16`) but does not fit is rejected by `overflowing_literals`.
#[test]
fn out_of_range_literal_for_column_type_is_rejected() {
    trybuild::TestCases::new().compile_fail("tests/ui/pred_overflow_i16.rs");
}

/// NO-RAW-STR WALL = E0277. `order_by` accepts `impl AsIdent` — a `Col`
/// marker or a `DynCol`, never a raw `&str`. There is no raw-`&str` ->
/// identifier path through ordering.
#[test]
fn order_by_raw_str_is_e0277() {
    trybuild::TestCases::new().compile_fail("tests/ui/order_by_raw_str.rs");
}

/// NO-RAW-STR WALL = E0277. An owned `String` is likewise not an `AsIdent`.
#[test]
fn order_by_owned_string_is_e0277() {
    trybuild::TestCases::new().compile_fail("tests/ui/order_by_string.rs");
}

/// THE DROPPED PRECEDENCE FOOTGUN = E0599. There is intentionally no
/// `or_where` on the builder (it would silently mis-precedence
/// `A AND B OR C`); `OR` is expressed via `Predicate::or`, which wraps.
#[test]
fn no_or_where_on_builder_is_e0599() {
    trybuild::TestCases::new().compile_fail("tests/ui/no_or_where.rs");
}

/// A PREDICATE IS NOT A STATEMENT = E0599. A `Predicate` (boolean
/// expression) cannot be `build()`-assembled; only a `Fragment` can.
#[test]
fn predicate_has_no_build_is_e0599() {
    trybuild::TestCases::new().compile_fail("tests/ui/predicate_not_buildable.rs");
}
