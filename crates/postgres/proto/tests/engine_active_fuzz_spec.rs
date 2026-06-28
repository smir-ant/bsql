//! Randomised fuzz against the still-live active framing / ingest targets.
//!
//! Ports the deleted `fuzz_stress_spec`'s xorshift PRNG + iteration counts to
//! the sans-IO engine. The forbid bundle already closes the panic / UB / alloc
//! classes at compile time (`#![forbid(unsafe_code)]`, the no-arith/no-index
//! wall, `no_std + no-alloc` bounded buffers). What randomised testing still
//! catches — and what this gate asserts on every adversarial input — is:
//!
//! - **No panic / hang** in the pure framing primitive
//!   ([`parse_header`]) on arbitrary bytes.
//! - **Termination + classification** of the active ingest framer
//!   ([`IngestBuf::next_event`]): every random byte stream drains to
//!   [`Event::NeedMore`] in a bounded number of steps — never an infinite
//!   loop and never a silent non-advancing dead-end.
//!
//! Reproducible via fixed seeds; runs in the standard `cargo test` pipeline.

#![allow(
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration fuzz harness — expect() is the loud failure signal; the PRNG's `try_from(..).unwrap_or(..)` is the sanctioned dead-arm shape (same as scram_fuzz_spec), not a production data fallback."
)]

use bsql_postgres_proto::engine::{Event, IngestBuf};
use bsql_postgres_proto::{parse_header, HeaderParse};

/// Iterations mirror the retired `fuzz_stress_spec`: 100K for the pure header
/// parser, 10K for the stateful ingest framer.
const PARSE_HEADER_ITERS: usize = 100_000;
const INGEST_ITERS: usize = 10_000;

/// Deterministic xorshift PRNG — mirrors `fuzz_stress_spec` / `scram_fuzz_spec`.
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0xDEAD_BEEF_CAFE_BABE } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn fill(&mut self, out: &mut [u8]) {
        let mut i = 0;
        while i < out.len() {
            let word = self.next_u64().to_le_bytes();
            let take = (out.len() - i).min(8);
            if let (Some(dst), Some(src)) =
                (out.get_mut(i..i.saturating_add(take)), word.get(..take))
            {
                dst.copy_from_slice(src);
            }
            i = i.saturating_add(take);
        }
    }

    fn len_up_to(&mut self, max: usize) -> usize {
        if max == 0 {
            return 0;
        }
        let r = self.next_u64();
        let modulus = u64::try_from(max.saturating_add(1)).unwrap_or(1);
        usize::try_from(r % modulus).unwrap_or(0)
    }
}

/// `parse_header` is a total function: it must classify every byte slice
/// (empty / incomplete / a complete header / over-cap) and never panic.
#[test]
fn parse_header_never_panics_on_random_bytes() {
    let mut rng = XorShift64::new(0x9E37_79B9_7F4A_7C15);
    for _ in 0..PARSE_HEADER_ITERS {
        // 0..=16 bytes spans the empty, sub-header (<5), and full-header
        // (>=5, including over-cap length fields) classifications.
        let len = rng.len_up_to(16);
        let mut bytes = vec![0u8; len];
        rng.fill(&mut bytes);
        // The call itself is the property — a panic here fails the test. The
        // result is consumed (and the call kept) via black_box.
        let parsed: HeaderParse = parse_header(&bytes);
        core::hint::black_box(parsed);
    }
}

/// `IngestBuf::next_event` must terminate on every random byte stream: each
/// non-`NeedMore` event consumes a whole frame (the cursor advances by at
/// least one byte), so the drain is bounded by the committed byte count and
/// always reaches `NeedMore` — never an infinite loop, never a silent
/// non-advancing dead-end.
#[test]
fn ingest_next_event_terminates_on_random_streams() {
    let mut rng = XorShift64::new(0xD1B5_4A32_D192_ED03);
    for _ in 0..INGEST_ITERS {
        let mut buf = IngestBuf::new();

        // Feed one random chunk (1..=512 bytes) through read_slot + commit.
        let want = 1usize.saturating_add(rng.len_up_to(511));
        let mut chunk = vec![0u8; want];
        rng.fill(&mut chunk);
        let slot = buf.read_slot(want).expect("inline/heap slot lent");
        let n = slot.len().min(chunk.len());
        slot[..n].copy_from_slice(&chunk[..n]);
        buf.commit(n).expect("commit");

        // Drain. The bound is generous: a frame is >= 1 byte, so at most `n`
        // events can be produced from `n` committed bytes. Exceeding it would
        // be a non-advancing spin (a hang) — the property under test.
        let bound = n.saturating_add(1);
        let mut steps = 0usize;
        loop {
            if let Event::NeedMore = buf.next_event() {
                break;
            }
            steps = steps.saturating_add(1);
            assert!(
                steps <= bound,
                "next_event spun past the committed-byte bound ({bound}) on a \
                 random stream — non-termination",
            );
        }
        // Once drained, the framer is idempotent: still NeedMore, no panic.
        assert!(matches!(buf.next_event(), Event::NeedMore));
    }
}
