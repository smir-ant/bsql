//! Phase 1a — frame-header parser spec-conformance.
//!
//! Per architect.txt Part III, a test exists only for:
//!
//! - **(A) Spec conformance** — externally observable API behaviour on
//!   valid or invalid input matches spec.
//! - **(B) Tier-3 invariant** — property the compiler / architecture
//!   cannot express; verified by harness (proptest, fuzz, Loom).
//! - **(C) Compile-time documentation** — `compile_fail` / trybuild.
//!
//! Tests covering tier-1 or tier-2 invariants have no place here.
//!
//! # Tier-1 closures (no test required)
//!
//! The following invariants are held **architecturally** by the parser
//! source — a test would be duplicate verification of what the compiler
//! or a const-assert already enforces:
//!
//! - *Parser never panics on arbitrary input.* Every panic-able
//!   expression in [`bsql_pg_proto::parse_header`] is a build error
//!   under the crate's forbid-bundle (`unwrap_used`, `indexing_slicing`,
//!   `arithmetic_side_effects`, …). Slice patterns carry compiler-
//!   enforced bounds; `u32::from_be_bytes([u8; 4])` is total;
//!   `usize::try_from` returns `Result`; `saturating_add` cannot
//!   overflow; `NonZeroU32::new` returns `Option`.
//! - *`parse_header(&[])` → `Empty`*, *1..=4 bytes → `Incomplete`*,
//!   *≥ 5 bytes with trailing bytes unchanged → same classification*.
//!   Held by one-line slice patterns `[] => HeaderParse::Empty`,
//!   `[_] | [_, _] | [_, _, _] | [_, _, _, _] => HeaderParse::Incomplete`,
//!   `[tag, l0, l1, l2, l3, ..] => …` — the rest-pattern `..` is the
//!   "ignore trailing bytes" contract.
//! - *`Ok.total_len == declared + 1` for in-range `declared`.* Pinned
//!   by `total_len = n.saturating_add(1)` plus the `READ_BUF_CAP ==
//!   MAX_FRAME_LEN_FIELD + 1` const-assert: saturation cannot occur for
//!   any accepted declared length, so the formula is exact.
//!
//! Tests below pin **classification boundaries** and **full-frame
//! happy-path composition** — the values that the compiler cannot see
//! are load-bearing for external callers.

#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::mem_forget,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::integer_division
)]
#![deny(unused_must_use, unused_lifetimes, unused_variables)]

use bsql_pg_proto::{HeaderParse, MAX_FRAME_LEN_FIELD, parse_header};

/// Invariant (spec): a well-formed header with `declared = 4` (header-
/// only frame, empty payload) parses as `Ok` with `total_len = 5`,
/// `tag` round-tripped, and `declared_len` packaged into `NonZeroU32`.
///
/// This is the **full-frame happy path** composition: BE decode +
/// `NonZeroU32::new` + the declared→total formula. None of those
/// compose at the type level; their behaviour is only observable via
/// the returned variant's fields.
#[test]
fn minimal_legal_header_parses_ok() {
    let header = [b'X', 0, 0, 0, 4];
    match parse_header(&header) {
        HeaderParse::Ok {
            tag,
            declared_len,
            total_len,
        } => {
            assert_eq!(tag, b'X');
            assert_eq!(declared_len.get(), 4);
            assert_eq!(total_len, 5);
        }
        other => panic!("expected Ok, got {other:?}"),
    }
}

/// Invariant (spec): `declared < 4` is the `MalformedLength` boundary.
///
/// The `4` is a spec choice encoded as a literal comparison
/// (`if declared < 4`). Nothing structural pins `4` as the correct
/// value — a bug that set it to `3` or `5` would compile and silently
/// shift the boundary. This test catches such a drift.
#[test]
fn length_below_minimum_is_malformed() {
    for declared in 0_u8..=3 {
        let header = [b'X', 0, 0, 0, declared];
        let expected_declared = u32::from(declared);
        assert_eq!(
            parse_header(&header),
            HeaderParse::MalformedLength {
                declared: expected_declared,
            },
            "declared={declared} must be MalformedLength",
        );
    }
}

/// Invariant (spec): `declared > MAX_FRAME_LEN_FIELD` → `FrameTooLarge`.
///
/// This is the structural DoS cap from reforge.md §53 (length-
/// amplification rejected before the buffer grows). The cap value is a
/// wire-level commitment that must round-trip exactly in the reported
/// error; a regression that clamped `declared` to the cap, for
/// instance, would hide attacker-chosen values from the wrapper.
#[test]
fn length_above_max_is_frame_too_large() {
    // One above the cap.
    let declared = MAX_FRAME_LEN_FIELD.saturating_add(1);
    let bytes = declared.to_be_bytes();
    let header = [b'X', bytes[0], bytes[1], bytes[2], bytes[3]];
    assert_eq!(
        parse_header(&header),
        HeaderParse::FrameTooLarge { declared },
        "declared={declared} must be FrameTooLarge",
    );

    // u32::MAX — the pathological attacker value.
    let header = [b'X', 0xFF, 0xFF, 0xFF, 0xFF];
    assert_eq!(
        parse_header(&header),
        HeaderParse::FrameTooLarge {
            declared: u32::MAX,
        },
    );
}
