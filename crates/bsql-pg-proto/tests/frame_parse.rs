//! Phase 1a — frame-header parser spec + tier-3 randomized fuzz.
//!
//! Two categories of tests, each category named by the invariant it
//! defends (per architect.txt Part III):
//!
//! - **(A) Spec conformance** — `parse_header` on specific inputs
//!   returns the spec-dictated [`HeaderParse`] variant. Pins the
//!   externally observable behaviour.
//! - **(B) Tier-3 invariant** — `parse_header` never panics on
//!   arbitrary bytes. The forbid-bundle in the crate root makes
//!   `panic!` / `unwrap` / indexing compile errors; this test gives
//!   empirical confidence by running 100 000 pseudo-random inputs
//!   through the parser and observing that every call returns a
//!   classified result. When Phase 1 verification infrastructure lands
//!   (§111), this loop is replaced by `proptest` and its corpus is
//!   cargo-fuzz managed.
//!
//! No runtime dep on `proptest` / `quickcheck` in Phase 1a — a
//! hand-rolled SplitMix-style PRNG gives us deterministic coverage
//! with zero additional build cost.

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
#![deny(unused_must_use, unused_lifetimes)]

use bsql_pg_proto::{HeaderParse, MAX_FRAME_LEN_FIELD, parse_header};

// ------------------------------------------------------------------
// (A) Spec conformance.
// ------------------------------------------------------------------

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

// ------------------------------------------------------------------
// (B) Tier-3 invariant: parser never panics on arbitrary input.
// ------------------------------------------------------------------

/// A tiny SplitMix64-style PRNG. Deterministic, seed-driven, zero-dep.
/// Quality is sufficient for byte-level fuzz; not cryptographic.
struct Rng(u64);

impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        // SplitMix64 — https://xoshiro.di.unimi.it/splitmix64.c
        let mut z = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        self.0 = z;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_u8(&mut self) -> u8 {
        // Take the low byte of the u64 stream.
        (self.next_u64() & 0xFF)
            .try_into()
            .unwrap_or_else(|_| panic!("0..=0xFF always fits u8 — unreachable"))
    }
}

/// Invariant (tier 3): `parse_header` never panics, regardless of the
/// byte content or length of the input. Any classification is
/// acceptable; the invariant is merely "returns".
///
/// Strategy: 100 000 iterations × pseudo-random slices of length
/// uniformly distributed in 0..=32 bytes. The Phase 1a parser's only
/// length-sensitive branches are the slice patterns for 0/1/2/3/4
/// bytes and the fall-through ≥ 5 bytes; iterations spend enough
/// time in each. Every `declared_len` value from the fuzzed u32
/// range hits both the `MalformedLength` (< 4) and `FrameTooLarge`
/// (> cap) boundaries.
///
/// When Phase 1 verification infrastructure (§111) lands, this loop is
/// rewritten as `proptest!` with `100_000` configured per-case and a
/// cargo-fuzz harness added as a separate target.
#[test]
fn parse_header_never_panics_on_random_bytes() {
    const ITERATIONS: u32 = 100_000;
    let mut rng = Rng::new(0x_DEAD_BEEF_CAFE_F00D);

    for _iter in 0..ITERATIONS {
        // Length uniformly in 0..=32. We cap at 32 because the parser
        // only inspects the first 5 bytes; longer inputs exercise
        // no new paths. `checked_rem` avoids the arithmetic-side-effects
        // forbid on a bare `%`.
        let len = usize::from(rng.next_u8())
            .checked_rem(33)
            .unwrap_or(0);
        let mut buf = [0_u8; 32];
        let slice = match buf.get_mut(..len) {
            Some(s) => s,
            None => panic!("len <= 32 and buf has 32 bytes — unreachable"),
        };
        for byte in slice.iter_mut() {
            *byte = rng.next_u8();
        }
        // Parse. We do not inspect the output — the test is only
        // "the call returns without panicking".
        match parse_header(slice) {
            HeaderParse::Empty
            | HeaderParse::Incomplete
            | HeaderParse::MalformedLength { .. }
            | HeaderParse::FrameTooLarge { .. }
            | HeaderParse::Ok { .. } => {}
        }
    }
}

/// Invariant (tier 3): `parse_header` preserves `total_len <= buffer
/// capacity` for every `Ok` return. This is the critical safety
/// property the dispatcher relies on when calling `advance(total_len)`.
///
/// Randomized check: 100 000 pseudo-random headers (fixed 5-byte slice
/// length, random tag, random length-field). Every `Ok` return must
/// have `total_len <= READ_BUF_CAP`.
#[test]
fn parse_ok_always_yields_total_len_within_cap() {
    const ITERATIONS: u32 = 100_000;
    const READ_BUF_CAP: usize = bsql_pg_proto::frame::READ_BUF_CAP;
    let mut rng = Rng::new(0x_0123_4567_89AB_CDEF);

    for _iter in 0..ITERATIONS {
        let tag = rng.next_u8();
        let l0 = rng.next_u8();
        let l1 = rng.next_u8();
        let l2 = rng.next_u8();
        let l3 = rng.next_u8();
        let header = [tag, l0, l1, l2, l3];
        if let HeaderParse::Ok { total_len, .. } = parse_header(&header) {
            assert!(
                total_len <= READ_BUF_CAP,
                "total_len {total_len} exceeds READ_BUF_CAP {READ_BUF_CAP} for header {header:?}",
            );
        }
    }
}
