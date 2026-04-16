//! Phase 1a — frame-header parser spec conformance.
//!
//! Per architect.txt Part III, a test exists only for:
//!
//! - **(A) Spec conformance** — the observable API behaviour on legal
//!   input matches the PostgreSQL wire spec.
//! - **(B) Tier-3 invariants** — properties the compiler / architecture
//!   cannot verify (parsers on arbitrary bytes, concurrent interleavings).
//! - **(C) Compile-time invariant docs** — `compile_fail` doctests.
//!
//! Tests covering tier-1 or tier-2 invariants have no place here.
//!
//! **Tier-1 closures** (invariants covered by architecture, NOT by a
//! test in this file):
//!
//! - *Parser never panics on arbitrary input.* Every panic-able
//!   expression in [`bsql_pg_proto::parse_header`] is a compile error
//!   under the crate's forbid-bundle (`clippy::unwrap_used`,
//!   `clippy::indexing_slicing`, `clippy::arithmetic_side_effects`, …).
//!   Slice patterns (`[tag, l0, l1, l2, l3, ..]`) carry compiler-
//!   enforced bounds; `u32::from_be_bytes` on `[u8; 4]` is total;
//!   `usize::try_from` returns `Result`; `saturating_add` cannot
//!   overflow; `NonZeroU32::new` returns `Option`. The previous
//!   randomized 100 000-iteration fuzz loop (SplitMix64-driven) was
//!   **overspec** — it exercised no path the forbid-bundle does not
//!   already close at compile time. Removed; DEF-018 closed.
//! - *`total_len ≤ READ_BUF_CAP` for every `HeaderParse::Ok`.* Pinned
//!   by `const _: () = assert!(READ_BUF_CAP == MAX_FRAME_LEN_FIELD + 1);`
//!   in `src/frame.rs` plus the saturating arithmetic above: any
//!   declared length within the cap is ≤ cap-1, so `declared + 1 ≤
//!   cap`. The previous fuzz sweep was overspec; the spec-conformance
//!   case (`total_len == declared + 1` for known declared values)
//!   remains covered by [`total_len_equals_one_plus_declared_len`].

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

/// Invariant (spec): empty input yields `Empty`.
///
/// The caller must distinguish "no data yet" from "less than a header
/// worth of data" — we want classification to be precise, not "one
/// incomplete catch-all".
#[test]
fn empty_input_yields_empty() {
    assert_eq!(parse_header(&[]), HeaderParse::Empty);
}

/// Invariant (spec): 1..=4 bytes yield `Incomplete`. The header is 5
/// bytes and we need all of them before we can classify.
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

/// Invariant (spec): a well-formed header with `declared = 4` (header-
/// only frame, empty payload) parses as `Ok` with `total_len = 5`.
///
/// This is the *framing-level* minimum — whether the semantic payload
/// is legal (e.g. RFQ demands 1 byte) is a dispatcher concern, not the
/// parser's. The parser's contract stops at framing.
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

/// Invariant (spec): a length-field below 4 is `MalformedLength` —
/// even 3 (one byte short of self-length). The connection is out of
/// sync; no semantic interpretation possible.
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

/// Invariant (spec): a length-field exceeding `MAX_FRAME_LEN_FIELD` is
/// `FrameTooLarge`. This is the structural cap defended against length
/// amplification DoS (reforge.md §53).
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

/// Invariant (spec): a valid header is parsed identically regardless
/// of trailing bytes — the parser is byte-exact on the first 5 bytes
/// and does not validate beyond.
#[test]
fn trailing_bytes_do_not_affect_header_parse() {
    // Two buffers with identical 5-byte prefix, different tails.
    let header_a = [b'Z', 0, 0, 0, 5, 0xAA, 0xBB];
    let header_b = [b'Z', 0, 0, 0, 5, 0xCC, 0xDD, 0xEE, 0xFF];
    assert_eq!(parse_header(&header_a), parse_header(&header_b));
}

/// Invariant (spec): `total_len` is always `1 + declared_len` for a
/// successfully parsed header. This is the contract the dispatcher
/// relies on when advancing the read buffer.
#[test]
fn total_len_equals_one_plus_declared_len() {
    // Sweep a spread of legal lengths.
    for declared in [4_u32, 5, 6, 100, 1024, MAX_FRAME_LEN_FIELD] {
        let bytes = declared.to_be_bytes();
        let header = [b'X', bytes[0], bytes[1], bytes[2], bytes[3]];
        match parse_header(&header) {
            HeaderParse::Ok {
                declared_len,
                total_len,
                ..
            } => {
                assert_eq!(declared_len.get(), declared);
                let expected = usize::try_from(declared)
                    .ok()
                    .and_then(|n| n.checked_add(1))
                    .unwrap_or(0);
                assert_eq!(total_len, expected);
            }
            other => panic!("declared={declared}: expected Ok, got {other:?}"),
        }
    }
}
