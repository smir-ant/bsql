//! Phase 1a — frame-header parser spec-conformance.
//!
//! Per reforge.md §4.11, a test exists only if it pins:
//!
//! - **(1) Functional spec-conformance** — externally observable API
//!   behaviour on valid or invalid input matches the contract.
//! - **(2) Tier-3 verification** — property the compiler / architecture
//!   cannot express (verified by proptest / fuzz / Loom).
//! - **(3) Compile-time invariant documentation** — `assert_send::<T>()`
//!   style, `compile_fail` doctests, trybuild.
//!
//! Tier-1 or tier-2 invariants have no place here.
//!
//! # Tier-1 closures (no test required)
//!
//! The following invariants are held architecturally and a test would
//! be duplicate verification:
//!
//! - *Parser never panics on arbitrary input.* Every panic-able
//!   expression in [`bsql_pg_proto::parse_header`] is a build error
//!   under the crate's forbid-bundle (`unwrap_used`, `indexing_slicing`,
//!   `arithmetic_side_effects`, …). Slice patterns bound every byte
//!   access; `u32::from_be_bytes([u8; 4])` is total; `usize::try_from`
//!   returns `Result`; `saturating_add` cannot overflow; `NonZeroU32::new`
//!   returns `Option`.
//!
//! All tests below are category (1): they pin **classification
//! boundaries**, **literal formula constants**, and **happy-path
//! composition** — the parts of the parser's behaviour that the source
//! expresses as literals or arm-body code (not as structural patterns).
//! Regressing any of them requires editing code that the compiler will
//! accept; only these tests will catch the shift.

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

/// Category (1) — classification pin.
///
/// Invariant (spec): `parse_header(&[])` returns `HeaderParse::Empty`,
/// distinct from `HeaderParse::Incomplete`.
///
/// The source is `[] => HeaderParse::Empty` — a one-line slice pattern.
/// The compiler enforces that an empty slice matches this arm, but a
/// future edit could swap the returned variant (`[] =>
/// HeaderParse::Incomplete`) and still compile. Today the feed-bytes
/// caller treats `Empty | Incomplete` as one arm, so the regression is
/// informational only; `HeaderParse` is `pub`, though, and external
/// consumers may legitimately distinguish "no data" from "partial
/// header" for diagnostics. Test catches the swap.
#[test]
fn empty_input_yields_empty() {
    assert_eq!(parse_header(&[]), HeaderParse::Empty);
}

/// Category (1) — classification pin.
///
/// Invariant (spec): slices of length 1..=4 return `HeaderParse::Incomplete`.
///
/// Held today by the slice patterns `[_] | [_, _] | [_, _, _] | [_, _,
/// _, _] => HeaderParse::Incomplete`. A future edit could swap any of
/// those return values for `Empty` or for `MalformedLength`. Compiler
/// does not catch; this test does.
#[test]
fn one_to_four_bytes_yield_incomplete() {
    for n in 1_usize..=4 {
        let buf = [0xAA_u8; 4];
        let slice = match buf.get(..n) {
            Some(s) => s,
            None => panic!("buf has 4 bytes; .get(..{n}) must be Some"),
        };
        assert_eq!(
            parse_header(slice),
            HeaderParse::Incomplete,
            "input of {n} byte(s) must be Incomplete",
        );
    }
}

/// Category (1) — full-frame happy-path composition.
///
/// Invariant (spec): a well-formed header with `declared = 4` (header-
/// only frame, empty payload) parses as `Ok` with `total_len = 5`,
/// `tag` round-tripped. F-058 (pass-#8): `declared_len` field dropped
/// from `HeaderParse::Ok` — derived from `total_len - 1` when tests
/// need it.
///
/// Ties together BE decode + the declared→total formula. Neither
/// composes at the type level; the aggregate behaviour is observable
/// via the returned variant's `total_len`.
#[test]
fn minimal_legal_header_parses_ok() {
    let header = [b'X', 0, 0, 0, 4];
    match parse_header(&header) {
        HeaderParse::Ok { tag, total_len } => {
            assert_eq!(tag.byte(), b'X');
            assert_eq!(total_len, 5);
            // Derived `declared_len`: `total_len - 1` always, per the
            // `parse_header` invariant. Test retains the implicit check
            // via the `5` literal above.
            let declared_len = total_len.saturating_sub(1);
            assert_eq!(declared_len, 4);
        }
        other => panic!("expected Ok, got {other:?}"),
    }
}

/// Category (1) — boundary pin.
///
/// Invariant (spec): `declared < 4` is the `MalformedLength` cut-off.
///
/// Held by the literal comparison `if declared < 4`. Nothing structural
/// pins `4` as the correct threshold — a bug setting it to `3` or `5`
/// would compile and silently shift the boundary. This test fails on
/// such drift.
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

/// Category (1) — DoS cap pin.
///
/// Invariant (spec): `declared > MAX_FRAME_LEN_FIELD` → `FrameTooLarge`,
/// with the attacker-chosen value round-tripped unchanged in the error.
///
/// This is the structural DoS defence from reforge.md §53 (length-
/// amplification rejected before the buffer grows). The cap value is
/// a wire-level commitment; a regression that clamped `declared` to
/// the cap inside the error would hide attacker values from the
/// wrapper's logs.
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

/// Category (1) — output-stability pin.
///
/// Invariant (spec): the parser's return value depends only on the
/// first 5 bytes of `unread`; any trailing bytes do not affect it.
///
/// The slice pattern `[tag, l0, l1, l2, l3, ..]` binds only the first
/// five bytes, but the arm body still sees the original `unread` slice
/// in scope. A future refactor could add code inside that arm reading
/// `unread.get(5)` or later, and return a different `HeaderParse`
/// variant based on those bytes — it would compile. Today's behaviour
/// is "trailing bytes do not reach the parser's output"; this test
/// pins that by comparing outputs from two slices with identical 5-byte
/// prefixes and different tails.
#[test]
fn trailing_bytes_do_not_affect_header_parse() {
    let header_a = [b'Z', 0, 0, 0, 5, 0xAA, 0xBB];
    let header_b = [b'Z', 0, 0, 0, 5, 0xCC, 0xDD, 0xEE, 0xFF];
    assert_eq!(parse_header(&header_a), parse_header(&header_b));
}

/// Category (1) — algebraic-formula pin + inclusive-boundary pin.
///
/// Invariant (spec): `total_len == declared + 1` for every successful
/// parse (the `+1` accounts for the tag byte, which the declared length
/// excludes per the PG protocol).
///
/// Held by `n.saturating_add(1)` in the parser body. The `1` is a
/// literal; a regression that changed it to `0`, `2`, or any
/// non-literal formula would compile. The `READ_BUF_CAP ==
/// MAX_FRAME_LEN_FIELD + 1` const-assert pins consistency between two
/// *constants*, not the arithmetic in `parse_header`. Only this test
/// catches the formula drift.
///
/// **Bonus: `>` vs `>=` boundary pin.** The sweep includes
/// `MAX_FRAME_LEN_FIELD` as the last input and expects `Ok`. If the
/// parser's comparison `declared > MAX_FRAME_LEN_FIELD` were changed
/// to `declared >= MAX_FRAME_LEN_FIELD`, this input would return
/// `FrameTooLarge` instead of `Ok` and this test would fail on the
/// last iteration. So the inclusive nature of the cap boundary is
/// pinned here too — not only by `length_above_max_is_frame_too_large`
/// (which tests strictly-above values).
#[test]
fn total_len_equals_one_plus_declared_len() {
    for declared in [4_u32, 5, 6, 100, 1024, MAX_FRAME_LEN_FIELD] {
        let bytes = declared.to_be_bytes();
        let header = [b'X', bytes[0], bytes[1], bytes[2], bytes[3]];
        match parse_header(&header) {
            HeaderParse::Ok { total_len, .. } => {
                // F-058 (pass-#8): `declared_len` no longer returned —
                // derive it from `total_len - 1` per the
                // `total_len = 1 + declared` invariant. The
                // formula itself is what this test was always
                // exercising; dropping the redundant field just
                // surfaces that fact.
                // DEF-154 (G): total_len is now u16 (bounded by
                // READ_BUF_CAP <= u16::MAX). Compare in usize to
                // match the u32 declared value from the test vec.
                let total_len_usize = usize::from(total_len);
                let derived_declared = total_len_usize.saturating_sub(1);
                let declared_usize = usize::try_from(declared).unwrap_or(0);
                assert_eq!(derived_declared, declared_usize);
                let expected = declared_usize.saturating_add(1);
                assert_eq!(total_len_usize, expected);
            }
            other => panic!("declared={declared}: expected Ok, got {other:?}"),
        }
    }
}
