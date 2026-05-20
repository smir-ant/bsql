//! Spec tests for `#[derive(Pristine)]`.
//!
//! Covers all 4 supported field-type categories (Option<T>, bool,
//! integer, PhantomData<T>) plus boundary cases (empty struct,
//! single-field, multi-field conjunction, mutated fields).
//!
//! Negative tests (unsupported types, enums, unions, generics,
//! tuple structs) live as `compile_fail` doctests on the derive
//! macro itself — exercised by `cargo test --doc`.
//!
//! # Why these tests matter
//!
//! The derive macro is the load-bearing translator that emits the
//! `is_pristine()` body. Tier-1 by-construction is the claim the
//! macro lands; this file pins the matching test coverage of its
//! tier-1 invariants (CREDO §4.11).

#![forbid(unsafe_code)]

use bsql_pg_proto::Pristine; // brings BOTH trait (type ns) and derive (macro ns) into scope
use core::marker::PhantomData;

// ═══════════════════════════════════════════════════════════════════
// (A) Cardinality — empty / single / many
// ═══════════════════════════════════════════════════════════════════

/// Empty struct → trivially pristine (neutral element of conjunction).
#[derive(Default, Pristine)]
struct Empty {}

#[test]
fn empty_struct_is_always_pristine() {
    let e = Empty::default();
    assert!(e.is_pristine(), "empty struct must be trivially pristine");
    // Const-eval path — verifies __pristine_const compiles + is const.
    const _: () = assert!(Empty {}.__pristine_const());
}

/// Single integer field — pristine iff field == 0.
#[derive(Default, Pristine)]
struct OneInt {
    x: u32,
}

#[test]
fn single_integer_pristine_when_zero() {
    let pristine = OneInt::default();
    assert!(pristine.is_pristine(), "u32 default 0 must be pristine");
    const _: () = assert!(OneInt { x: 0 }.__pristine_const());

    let mutated = OneInt { x: 1 };
    assert!(!mutated.is_pristine(), "u32 != 0 must NOT be pristine");
}

/// Single bool field — pristine iff field == false (i.e., !field).
#[derive(Default, Pristine)]
struct OneBool {
    flag: bool,
}

#[test]
fn single_bool_pristine_when_false() {
    let pristine = OneBool::default();
    assert!(pristine.is_pristine(), "bool default false must be pristine");
    const _: () = assert!(OneBool { flag: false }.__pristine_const());

    let mutated = OneBool { flag: true };
    assert!(!mutated.is_pristine(), "bool true must NOT be pristine");
}

/// Single Option<T> field — pristine iff is_none.
#[derive(Default, Pristine)]
struct OneOpt {
    val: Option<u32>,
}

#[test]
fn single_option_pristine_when_none() {
    let pristine = OneOpt::default();
    assert!(pristine.is_pristine(), "Option default None must be pristine");
    const _: () = assert!(OneOpt { val: None }.__pristine_const());

    let mutated = OneOpt { val: Some(42) };
    assert!(!mutated.is_pristine(), "Option Some(_) must NOT be pristine");
}

/// PhantomData<T> field — always trivially pristine (ZST).
#[derive(Default, Pristine)]
struct WithPhantom {
    _marker: PhantomData<fn() -> u32>,
}

#[test]
fn phantom_data_field_is_trivially_pristine() {
    let p = WithPhantom::default();
    assert!(p.is_pristine(), "PhantomData<_> field must be trivially pristine");
    const _: () = assert!(
        WithPhantom { _marker: PhantomData }.__pristine_const(),
    );
}

// ═══════════════════════════════════════════════════════════════════
// (B) Multi-field conjunction
// ═══════════════════════════════════════════════════════════════════

/// Mixed-field type — exercises the conjunction logic across all
/// supported field shapes.
#[derive(Default, Pristine)]
struct Mixed {
    counter: u32,
    flag: bool,
    payload: Option<u64>,
    _marker: PhantomData<()>,
}

#[test]
fn mixed_struct_pristine_at_default() {
    let m = Mixed::default();
    assert!(m.is_pristine(), "all-default mixed struct must be pristine");
}

#[test]
fn mixed_struct_not_pristine_when_any_field_mutated() {
    // Each non-default field individually trips pristine to false.
    let only_counter = Mixed {
        counter: 1,
        flag: false,
        payload: None,
        _marker: PhantomData,
    };
    assert!(
        !only_counter.is_pristine(),
        "non-zero counter must violate pristine",
    );

    let only_flag = Mixed {
        counter: 0,
        flag: true,
        payload: None,
        _marker: PhantomData,
    };
    assert!(
        !only_flag.is_pristine(),
        "true flag must violate pristine",
    );

    let only_payload = Mixed {
        counter: 0,
        flag: false,
        payload: Some(0),
        _marker: PhantomData,
    };
    assert!(
        !only_payload.is_pristine(),
        "Some(_) payload must violate pristine even with inner = 0 \
         (Option pristine semantic = is_none, not inner == default)",
    );
}

// ═══════════════════════════════════════════════════════════════════
// (C) All integer types — every supported width works
// ═══════════════════════════════════════════════════════════════════

#[derive(Default, Pristine)]
struct AllInts {
    a: u8,
    b: u16,
    c: u32,
    d: u64,
    e: u128,
    f: usize,
    g: i8,
    h: i16,
    i: i32,
    j: i64,
    k: i128,
    l: isize,
}

#[test]
fn all_integer_widths_pristine_at_zero() {
    let z = AllInts::default();
    assert!(z.is_pristine(), "all-zero integers across widths must be pristine");
}

#[test]
fn any_nonzero_integer_violates_pristine() {
    let x = AllInts { c: 1, ..Default::default() };
    assert!(!x.is_pristine(), "single non-zero u32 violates pristine");

    let y = AllInts { j: -1, ..Default::default() };
    assert!(!y.is_pristine(), "single non-zero i64 (negative) violates pristine");
}

// ═══════════════════════════════════════════════════════════════════
// (D) Trait dispatch consistency
// ═══════════════════════════════════════════════════════════════════

/// `<T as Pristine>::is_pristine` and `T::__pristine_const` must
/// produce the same boolean result for any concrete instance.
/// Pin against accidental drift (e.g., one path uses a different
/// field check shape).
#[test]
fn trait_method_and_const_inherent_agree() {
    let cases = [
        (OneInt { x: 0 }, true),
        (OneInt { x: 1 }, false),
    ];
    for (case, expected) in cases {
        // Trait method (runtime polymorphic).
        let via_trait = <OneInt as bsql_pg_proto::pristine::Pristine>::is_pristine(&case);
        // Inherent const fn (compile-time pin path).
        let via_const = case.__pristine_const();
        assert_eq!(
            via_trait, via_const,
            "trait + const-inherent must agree for OneInt {{ x: {} }}",
            case.x,
        );
        assert_eq!(via_trait, expected, "expected pristine = {expected}");
    }
}

// ═══════════════════════════════════════════════════════════════════
// (E) Const evaluation — pin via `const _: () = assert!(...)`
// ═══════════════════════════════════════════════════════════════════

// All these compile only if __pristine_const is genuinely const fn
// and the conjunction body is const-callable. A regression to non-const
// (e.g., adding a `&&` over a non-const subexpression) would break
// the build here.

const _ASSERT_EMPTY_CONST: () = assert!(
    Empty {}.__pristine_const(),
    "Empty {{}}.__pristine_const() must be true at compile time",
);

const _ASSERT_INT_ZERO_CONST: () = assert!(
    OneInt { x: 0 }.__pristine_const(),
    "OneInt {{ x: 0 }}.__pristine_const() must be true at compile time",
);

const _ASSERT_BOOL_FALSE_CONST: () = assert!(
    OneBool { flag: false }.__pristine_const(),
    "OneBool {{ flag: false }}.__pristine_const() must be true at compile time",
);

const _ASSERT_OPT_NONE_CONST: () = assert!(
    OneOpt { val: None }.__pristine_const(),
    "OneOpt {{ val: None }}.__pristine_const() must be true at compile time",
);

const _ASSERT_PHANTOM_CONST: () = assert!(
    WithPhantom { _marker: PhantomData }.__pristine_const(),
    "WithPhantom is trivially pristine at compile time",
);

const _ASSERT_INT_NONZERO_CONST: () = assert!(
    !OneInt { x: 1 }.__pristine_const(),
    "OneInt {{ x: 1 }}.__pristine_const() must be FALSE at compile time",
);

const _ASSERT_BOOL_TRUE_CONST: () = assert!(
    !OneBool { flag: true }.__pristine_const(),
    "OneBool {{ flag: true }}.__pristine_const() must be FALSE at compile time",
);

const _ASSERT_OPT_SOME_CONST: () = assert!(
    !OneOpt { val: Some(0) }.__pristine_const(),
    "OneOpt {{ val: Some(0) }}.__pristine_const() must be FALSE \
     (Option pristine = is_none, not inner == default)",
);
