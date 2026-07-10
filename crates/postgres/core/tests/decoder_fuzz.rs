//! Universal-coverage total-function proof for every wire/text decoder.
//!
//! # What this proves
//!
//! Every decoder that turns UNTRUSTED server or text bytes into a Rust value is
//! a TOTAL function: on ANY input — well-formed, malformed, truncated, random,
//! or hostile — it returns `Ok(value)` or a CLASSIFIED `Err`, and NEVER panics
//! or aborts. A panicking decoder is a real vulnerability: a hostile server (or
//! a corrupt/compromised connection) could send a byte sequence that crashes the
//! driver. This test is the machine proof that no such byte sequence exists in
//! the exercised corpus — not just on the handful of inputs the unit tests pin,
//! but across a broad, deterministic sweep of the whole length/content space.
//!
//! # Surface covered
//!
//! * `bsql_postgres_proto::decode` — `Cell<BinaryFmt>::decode` and
//!   `Cell<TextFmt>::decode` for every supported scalar (`i16`/`i32`/`i64`/`u32`/
//!   `f32`/`f64`/`bool`/`&str`/`&[u8]`/`Uuid`/`Timestamptz`/`Timestamp`/`Date`/
//!   `Time`/`Interval`/`Json`/`Jsonb`/`Numeric`), the one-dimensional array
//!   decoders (`Vec<Option<T>>` for the full element set, including `text[]` and
//!   `bytea[]`), and the opt-in SWAR fast-paths.
//! * `bsql_postgres_proto::pgtypes` — `FromStr` for `Uuid`/`Date`/`Time`/`Numeric`.
//! * `bsql_postgres_proto::decode` — `parse_row_description` and
//!   `parse_column_names`, the two parsers that turn an untrusted server
//!   `RowDescription` (`'T'`) payload into the result schema (OID/format) and the
//!   column names (reached by the active dispatch and the fused runtime-param
//!   path's `parse_row_desc_owned`).
//! * `bsql_postgres_core::materialize::parse_notification`.
//!
//! # Design
//!
//! * **Dep-free, deterministic.** A hand-rolled xorshift64 PRNG with a fixed seed
//!   constant — no `rand`/`arbitrary`/`proptest`, no `SystemTime`/`thread_rng`.
//!   Same seed => byte-identical inputs on every run => the test never flakes and
//!   any finding is reproducible from the printed input.
//! * **Panic capture.** `cargo test` runs the test profile (inherits `dev`, which
//!   sets no `panic` key => unwind), so `std::panic::catch_unwind` catches a
//!   decoder panic instead of aborting. A recording panic hook (installed for the
//!   duration, restored on scope exit) captures each panic's message + location
//!   into a thread-local WITHOUT spamming stderr, so a caught panic is reported
//!   precisely (decoder name + input hex + panic message). The committed teeth
//!   self-check — a deliberately-planted panic routed through the SAME harness —
//!   proves the capture works (and, since it would ABORT under a `panic="abort"`
//!   test profile, doubles as the runtime confirmation that the profile unwinds).

use std::cell::RefCell;
use std::panic::{self, AssertUnwindSafe};

use bsql_postgres_core::materialize::parse_notification;
use bsql_postgres_proto::{
    BinaryFmt, Cell, Date, Interval, Json, Jsonb, Numeric, Time, Timestamp, Timestamptz, Uuid,
    parse_long_uint_swar, parse_pg_bool_swar, parse_short_uint_swar, validate_utf8_swar,
};
use core::str::FromStr as _;

/// A decoder adapted to a uniform shape: untrusted bytes in, `is_ok` / `is_some`
/// out. Every entry in the fuzz tables has this type so one harness drives them.
type DecodeFn = fn(&[u8]) -> bool;
/// A named decoder entry (the label used in a panic report + the adapter).
type NamedDecoder = (&'static str, DecodeFn);
/// The panic-hook box shape (matches `std::panic::take_hook`'s return type).
type PanicHook = Box<dyn Fn(&panic::PanicHookInfo<'_>) + Sync + Send + 'static>;

// ══════════════════════════════════════════════════════════════════════════
// Deterministic xorshift64 PRNG. No dependency, no clock, no thread_rng.
// ══════════════════════════════════════════════════════════════════════════

/// Fixed nonzero seed (golden-ratio constant). Xorshift64 fixes `0` (it maps to
/// itself), so a nonzero seed is required; this one makes every run identical.
const SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// A minimal, self-contained xorshift64 generator. Every method is total (no
/// `unwrap`/`panic`/indexing), so the generator itself can never crash the test
/// harness outside the fuzzed decode calls.
struct Rng(u64);

impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// One xorshift64 step. Uses only shift/xor (no arithmetic that could
    /// overflow), so it is defined for every state.
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Overwrite `out` with `len` pseudo-random bytes. Draws 8 bytes per step
    /// and stops exactly at `len` — no indexing, no per-byte state.
    fn fill(&mut self, out: &mut Vec<u8>, len: usize) {
        out.clear();
        out.reserve(len);
        while out.len() < len {
            let word = self.next_u64().to_le_bytes();
            for &b in word.iter() {
                if out.len() >= len {
                    break;
                }
                out.push(b);
            }
        }
    }

    /// An index in `0..bound` (or `0` for an empty bound). Reduces a 16-bit draw
    /// modulo `bound`; every call site passes a small bound (a pool length or a
    /// short loop count ≤ a few dozen), so a `u16` span gives ample spread. Uses
    /// remainder (`%`), which is not the forbidden integer-division (`/`) and
    /// cannot divide by zero (the bound is guarded nonzero). `usize::from(u16)`
    /// is a lossless widening — no `as`, no fallible conversion.
    fn bounded(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        usize::from(self.u16()) % bound
    }

    /// Pick a reference out of `pool` (or `None` for an empty pool).
    fn pick<'p, T>(&mut self, pool: &'p [T]) -> Option<&'p T> {
        let idx = self.bounded(pool.len());
        pool.get(idx)
    }

    /// A random `u16` (two fresh bytes, host-independent).
    fn u16(&mut self) -> u16 {
        let w = self.next_u64().to_le_bytes();
        u16::from_le_bytes([w[0], w[1]])
    }

    /// A random `u32` (four fresh bytes).
    fn u32(&mut self) -> u32 {
        let w = self.next_u64().to_le_bytes();
        u32::from_le_bytes([w[0], w[1], w[2], w[3]])
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Panic capture — recording hook + RAII restore, and the per-input probe.
// ══════════════════════════════════════════════════════════════════════════

thread_local! {
    /// The most recent captured panic (message + location), set by the recording
    /// hook and drained by `probe` right after each `catch_unwind`. The whole
    /// fuzz runs on one thread inside a single `#[test]`, so a thread-local
    /// avoids any cross-thread contention on a global mutex.
    static LAST_PANIC: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Installs a recording panic hook on construction and restores the previous
/// hook on drop (so a panic during the assertions after the fuzz — or a `Drop`
/// unwind — prints normally again).
struct HookGuard {
    prev: Option<PanicHook>,
}

impl HookGuard {
    fn install() -> Self {
        let prev = panic::take_hook();
        panic::set_hook(Box::new(|info| {
            let loc = match info.location() {
                Some(l) => format!("{}:{}:{}", l.file(), l.line(), l.column()),
                None => String::from("<unknown location>"),
            };
            let payload = info.payload();
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                String::from("<non-string panic payload>")
            };
            LAST_PANIC.with(|slot| {
                *slot.borrow_mut() = Some(format!("{msg} @ {loc}"));
            });
        }));
        Self { prev: Some(prev) }
    }
}

impl Drop for HookGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.prev.take() {
            panic::set_hook(prev);
        }
    }
}

/// The outcome of running one decoder on one input.
enum Probe {
    /// The decoder returned `Ok` — the input happened to be well-formed.
    Ok,
    /// The decoder returned a classified `Err` — the honest total-function path.
    Classified,
    /// The decoder PANICKED — a real finding. Carries the captured message.
    Panicked(String),
}

/// Run one decoder (`fn(&[u8]) -> bool`, returning `is_ok`) on one input under
/// `catch_unwind`, classifying the outcome. Total: never panics itself.
fn probe(decode: DecodeFn, input: &[u8]) -> Probe {
    LAST_PANIC.with(|slot| {
        *slot.borrow_mut() = None;
    });
    match panic::catch_unwind(AssertUnwindSafe(|| decode(input))) {
        Ok(true) => Probe::Ok,
        Ok(false) => Probe::Classified,
        Err(_) => {
            let captured = LAST_PANIC.with(|slot| slot.borrow_mut().take());
            let msg = match captured {
                Some(m) => m,
                None => String::from("<no panic message captured>"),
            };
            Probe::Panicked(msg)
        }
    }
}

/// A recorded panic finding: which decoder, on which input, with what message.
struct Finding {
    decoder: &'static str,
    input: Vec<u8>,
    message: String,
}

/// Running tally over the whole fuzz: total probes, the Ok/classified-Err split
/// (proof the sweep reached real decode paths, not only length guards), and the
/// list of panic findings (which must stay empty).
struct Tally {
    total: u64,
    ok: u64,
    classified: u64,
    findings: Vec<Finding>,
}

impl Tally {
    fn new() -> Self {
        Self {
            total: 0,
            ok: 0,
            classified: 0,
            findings: Vec::new(),
        }
    }

    fn record(&mut self, decoder: &'static str, input: &[u8], outcome: Probe) {
        self.total = self.total.saturating_add(1);
        match outcome {
            Probe::Ok => self.ok = self.ok.saturating_add(1),
            Probe::Classified => self.classified = self.classified.saturating_add(1),
            Probe::Panicked(message) => self.findings.push(Finding {
                decoder,
                input: input.to_vec(),
                message,
            }),
        }
    }
}

/// Lowercase hex of a byte slice, capped to keep a report readable when a huge
/// input triggers a finding (the seed + length still reproduce it exactly).
fn to_hex_capped(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    const CAP: usize = 256;
    let show = match bytes.get(..CAP) {
        Some(s) => s,
        None => bytes,
    };
    let mut s = String::with_capacity(show.len().saturating_mul(2));
    for &b in show {
        let hi = usize::from(b >> 4);
        let lo = usize::from(b & 0x0f);
        if let Some(&c) = HEX.get(hi) {
            s.push(char::from(c));
        }
        if let Some(&c) = HEX.get(lo) {
            s.push(char::from(c));
        }
    }
    if bytes.len() > CAP {
        s.push_str(&format!("… ({} bytes total)", bytes.len()));
    }
    s
}

// ══════════════════════════════════════════════════════════════════════════
// The decoder tables. Each entry adapts one decoder to `fn(&[u8]) -> bool`
// (`is_ok`/`is_some`), so a single harness drives them uniformly. The bool is
// genuinely consumed (Ok/Err tally), so no `#[must_use]` result is discarded.
// ══════════════════════════════════════════════════════════════════════════

/// Wrap a `FromStr` decoder: non-UTF-8 bytes cannot form a `&str` and so are
/// out of that parser's domain (reported as "survived"); UTF-8 bytes reach it.
macro_rules! from_str_dec {
    ($t:ty) => {
        |b: &[u8]| -> bool {
            match core::str::from_utf8(b) {
                Ok(s) => <$t>::from_str(s).is_ok(),
                Err(_) => true,
            }
        }
    };
}

/// Every binary-format scalar `Cell` decoder.
fn binary_scalar_decoders() -> Vec<NamedDecoder> {
    vec![
        ("i16:binary", |b| <i16 as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("i32:binary", |b| <i32 as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("i64:binary", |b| <i64 as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("u32:binary", |b| <u32 as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("f32:binary", |b| <f32 as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("f64:binary", |b| <f64 as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("bool:binary", |b| <bool as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("&str:binary", |b| <&str as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("&[u8]:binary", |b| <&[u8] as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Uuid:binary", |b| <Uuid as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Timestamptz:binary", |b| <Timestamptz as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Timestamp:binary", |b| <Timestamp as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Date:binary", |b| <Date as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Time:binary", |b| <Time as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Interval:binary", |b| <Interval as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Json:binary", |b| <Json as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Jsonb:binary", |b| <Jsonb as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Numeric:binary", |b| <Numeric as Cell<BinaryFmt>>::decode(b).is_ok()),
    ]
}

/// Every one-dimensional array `Cell` decoder (`Vec<Option<T>>`).
fn array_decoders() -> Vec<NamedDecoder> {
    vec![
        ("Vec<i16>:binary", |b| <Vec<Option<i16>> as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<i32>:binary", |b| <Vec<Option<i32>> as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<i64>:binary", |b| <Vec<Option<i64>> as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<u32>:binary", |b| <Vec<Option<u32>> as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<bool>:binary", |b| <Vec<Option<bool>> as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<f32>:binary", |b| <Vec<Option<f32>> as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<f64>:binary", |b| <Vec<Option<f64>> as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<Uuid>:binary", |b| <Vec<Option<Uuid>> as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<Timestamptz>:binary", |b| {
            <Vec<Option<Timestamptz>> as Cell<BinaryFmt>>::decode(b).is_ok()
        }),
        ("Vec<Timestamp>:binary", |b| {
            <Vec<Option<Timestamp>> as Cell<BinaryFmt>>::decode(b).is_ok()
        }),
        ("Vec<Json>:binary", |b| <Vec<Option<Json>> as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<Jsonb>:binary", |b| <Vec<Option<Jsonb>> as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<Numeric>:binary", |b| {
            <Vec<Option<Numeric>> as Cell<BinaryFmt>>::decode(b).is_ok()
        }),
        ("Vec<Date>:binary", |b| <Vec<Option<Date>> as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<Time>:binary", |b| <Vec<Option<Time>> as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<Interval>:binary", |b| {
            <Vec<Option<Interval>> as Cell<BinaryFmt>>::decode(b).is_ok()
        }),
        ("Vec<String>(text[]):binary", |b| {
            <Vec<Option<String>> as Cell<BinaryFmt>>::decode(b).is_ok()
        }),
        ("Vec<Vec<u8>>(bytea[]):binary", |b| {
            <Vec<Option<Vec<u8>>> as Cell<BinaryFmt>>::decode(b).is_ok()
        }),
    ]
}

/// Fully drive a [`CompositeReader`] over `bytes` for a fixed declared arity:
/// `new` (checks the field-count header), then one `next_field` per declared
/// field, then `finish` (rejects surplus). Returns whether the whole walk was
/// `Ok` — the point is that on ANY input it must return, never panic (the probe
/// harness catches a panic and fails the gate).
fn drive_composite_reader(bytes: &[u8], arity: u32) -> bool {
    use bsql_postgres_proto::CompositeReader;
    let mut r = match CompositeReader::new(bytes, arity) {
        Ok(r) => r,
        Err(_) => return false,
    };
    for _ in 0..arity {
        if r.next_field().is_err() {
            return false;
        }
    }
    r.finish().is_ok()
}

/// The composite (row-type) frame reader, driven at several declared arities so
/// the fuzz exercises the count header, the per-field `{oid, len, body}` walk,
/// the NULL sentinel, and the surplus / truncation classifications.
fn composite_reader_decoders() -> Vec<NamedDecoder> {
    vec![
        ("CompositeReader:arity0", |b| drive_composite_reader(b, 0)),
        ("CompositeReader:arity1", |b| drive_composite_reader(b, 1)),
        ("CompositeReader:arity2", |b| drive_composite_reader(b, 2)),
        ("CompositeReader:arity3", |b| drive_composite_reader(b, 3)),
    ]
}

/// The text-format scalar `Cell` decoders, the `FromStr` parsers, the SWAR
/// fast-paths, and `parse_notification` — everything that parses text or scans
/// framed bytes. These also receive the ASCII-biased corpus.
fn text_and_misc_decoders() -> Vec<NamedDecoder> {
    vec![
        ("i16:text", |b| <i16 as Cell<bsql_postgres_proto::TextFmt>>::decode(b).is_ok()),
        ("i32:text", |b| <i32 as Cell<bsql_postgres_proto::TextFmt>>::decode(b).is_ok()),
        ("i64:text", |b| <i64 as Cell<bsql_postgres_proto::TextFmt>>::decode(b).is_ok()),
        ("u32:text", |b| <u32 as Cell<bsql_postgres_proto::TextFmt>>::decode(b).is_ok()),
        ("bool:text", |b| <bool as Cell<bsql_postgres_proto::TextFmt>>::decode(b).is_ok()),
        ("&str:text", |b| <&str as Cell<bsql_postgres_proto::TextFmt>>::decode(b).is_ok()),
        ("Uuid:from_str", from_str_dec!(Uuid)),
        ("Date:from_str", from_str_dec!(Date)),
        ("Time:from_str", from_str_dec!(Time)),
        ("Numeric:from_str", from_str_dec!(Numeric)),
        ("parse_short_uint_swar", |b| parse_short_uint_swar(b).is_some()),
        ("parse_long_uint_swar", |b| parse_long_uint_swar(b).is_some()),
        ("parse_pg_bool_swar", |b| parse_pg_bool_swar(b).is_some()),
        ("validate_utf8_swar", |b| validate_utf8_swar(b).is_some()),
        ("parse_notification", |b| parse_notification(b).is_ok()),
        // The two `RowDescription` (`'T'`) parsers: both turn UNTRUSTED server
        // bytes into a Rust value (the OID/format schema, and the column names) —
        // reached in production by the active dispatch and the fused runtime-param
        // path's `parse_row_desc_owned`. Total-by-construction today (no indexing /
        // unwrap), so they pass immediately; fuzzing them closes the coverage gap
        // so a FUTURE indexing/unwrap regression (a hostile-server crash) is caught.
        ("parse_row_description", |b| {
            bsql_postgres_proto::decode::parse_row_description(b).is_ok()
        }),
        ("parse_column_names", |b| {
            bsql_postgres_proto::decode::parse_column_names(b).is_ok()
        }),
    ]
}

// ══════════════════════════════════════════════════════════════════════════
// Input generation.
// ══════════════════════════════════════════════════════════════════════════

/// Small lengths swept densely: `0..=SMALL_MAX`. Covers every fixed decoder
/// width (2/4/8/16) and its off-by-ones.
const SMALL_MAX: usize = 40;

/// Larger lengths sampled around powers of two and the wire's `u16` frame cap.
const LARGE_LENGTHS: &[usize] = &[
    48, 56, 63, 64, 65, 72, 100, 127, 128, 200, 255, 256, 300, 511, 512, 1000, 1024, 4096, 16_384,
    65_535, 65_536,
];

/// Random-content variants per length (fewer for very large lengths to bound
/// total work while still exercising them).
fn variants_for(len: usize) -> usize {
    if len <= 512 { 48 } else { 6 }
}

/// A 32-byte alphabet biasing the ASCII corpus toward the bytes the text
/// parsers branch on: decimal digits, sign/point/colon/hyphen/space, hex
/// letters, and the special-word letters (`inf`, `nan`, `Infinity`, ` BC`).
const TEXT_ALPHABET: [u8; 32] = *b"0123456789-:.+ eENafIntybcdBCxAz";

const _: () = assert!(TEXT_ALPHABET.len() == 32);

/// The real element OIDs, taken from the public `Cell<BinaryFmt>::OID` consts so
/// the array header can be crafted to PASS a given decoder's element-OID gate
/// (reaching the per-element framing loop) without hard-coding any OID number.
fn real_element_oids() -> Vec<u32> {
    vec![
        <i16 as Cell<BinaryFmt>>::OID,
        <i32 as Cell<BinaryFmt>>::OID,
        <i64 as Cell<BinaryFmt>>::OID,
        <u32 as Cell<BinaryFmt>>::OID,
        <bool as Cell<BinaryFmt>>::OID,
        <f32 as Cell<BinaryFmt>>::OID,
        <f64 as Cell<BinaryFmt>>::OID,
        <Uuid as Cell<BinaryFmt>>::OID,
        <Timestamptz as Cell<BinaryFmt>>::OID,
        <Timestamp as Cell<BinaryFmt>>::OID,
        <Json as Cell<BinaryFmt>>::OID,
        <Jsonb as Cell<BinaryFmt>>::OID,
        <Numeric as Cell<BinaryFmt>>::OID,
        <Date as Cell<BinaryFmt>>::OID,
        <Time as Cell<BinaryFmt>>::OID,
        <Interval as Cell<BinaryFmt>>::OID,
        <&str as Cell<BinaryFmt>>::OID,
        <&[u8] as Cell<BinaryFmt>>::OID,
    ]
}

/// Emit an ASCII-biased byte string of `len` bytes drawn from `TEXT_ALPHABET`.
fn fill_ascii(rng: &mut Rng, out: &mut Vec<u8>, len: usize) {
    out.clear();
    out.reserve(len);
    for _ in 0..len {
        let idx = usize::from(rng.u16() & 0x1f);
        match TEXT_ALPHABET.get(idx) {
            Some(&b) => out.push(b),
            None => out.push(b'0'),
        }
    }
}

/// Craft a semi-structured PG binary `numeric` frame so the fuzz reaches the
/// deep parser branches (dscale mask, sign classification, digit-group loop,
/// out-of-range group, no-swallow trailing check) rather than only the leading
/// length guard. Header fields are drawn from pools that include the valid
/// specials and hostile edges; the digit tail is random and sometimes truncated
/// or surplus.
fn craft_numeric(rng: &mut Rng, out: &mut Vec<u8>) {
    out.clear();
    // ndigits pool: exact-small, huge (forces the alloc cap + early truncation),
    // and fully random.
    let ndigits_pool: [u16; 7] = [0, 1, 2, 3, 8, 0xFFFF, rng.u16()];
    let ndigits = match rng.pick(&ndigits_pool) {
        Some(&v) => v,
        None => 0,
    };
    // sign pool: the five valid words plus a random (often invalid) sign.
    let sign_pool: [u16; 6] = [0x0000, 0x4000, 0xC000, 0xD000, 0xF000, rng.u16()];
    let sign = match rng.pick(&sign_pool) {
        Some(&v) => v,
        None => 0,
    };
    // dscale pool: a valid low scale, a full-range value (exercises the high-bit
    // reject), and random.
    let dscale_pool: [u16; 3] = [rng.u16() & 0x3FFF, rng.u16(), 0xFFFF];
    let dscale = match rng.pick(&dscale_pool) {
        Some(&v) => v,
        None => 0,
    };
    let weight = rng.u16();
    out.extend_from_slice(&ndigits.to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    // Emit up to a bounded number of digit groups (some ≥ NBASE to hit the
    // out-of-range arm); the count deliberately may not match `ndigits`, so the
    // truncated and surplus paths are both reached.
    let group_count = rng.bounded(20);
    for _ in 0..group_count {
        let g = if rng.u16() & 1 == 0 {
            rng.u16() % 10_000
        } else {
            rng.u16()
        };
        out.extend_from_slice(&g.to_be_bytes());
    }
    // Sometimes append a stray trailing byte to reach the no-swallow check.
    if rng.u16() & 3 == 0 {
        out.push(0xAB);
    }
}

/// Craft a semi-structured PG binary array frame. `elem_oid` is drawn from a
/// pool that includes ALL real element OIDs, so every array decoder receives
/// headers that PASS its OID gate and reach the per-element length framing.
fn craft_array(rng: &mut Rng, out: &mut Vec<u8>, oid_pool: &[u32]) {
    out.clear();
    let ndim_pool: [i32; 5] = [0, 1, 2, -1, rng_i32(rng)];
    let ndim = match rng.pick(&ndim_pool) {
        Some(&v) => v,
        None => 0,
    };
    let flags = rng_i32(rng);
    // Bias two-thirds toward a real OID (reach the deep loop), one-third random.
    let elem_oid = if rng.u16().is_multiple_of(3) {
        rng.u32()
    } else {
        match rng.pick(oid_pool) {
            Some(&v) => v,
            None => 0,
        }
    };
    out.extend_from_slice(&ndim.to_be_bytes());
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(&elem_oid.to_be_bytes());
    // For ndim==1 the header carries a dimension pair; craft it (and, for other
    // ndim values, still append bytes so those branches see a realistic tail).
    let dim_len_pool: [i32; 8] = [0, 1, 2, 3, -1, 0x7FFF_FFFF, rng_i32(rng), rng_i32(rng)];
    let dim_len = match rng.pick(&dim_len_pool) {
        Some(&v) => v,
        None => 0,
    };
    let lower = rng_i32(rng);
    out.extend_from_slice(&dim_len.to_be_bytes());
    out.extend_from_slice(&lower.to_be_bytes());
    // Element bodies: a bounded number of `[len i32][body]` groups, len from a
    // pool including the NULL sentinel (-1), zero, small widths, and hostile
    // negative/huge values; the body is random and sometimes truncated.
    let elem_count = rng.bounded(12);
    for _ in 0..elem_count {
        let len_pool: [i32; 9] = [-1, 0, 1, 2, 4, 8, 16, -5, 0x7FFF_FFFF];
        let elem_len = match rng.pick(&len_pool) {
            Some(&v) => v,
            None => 0,
        };
        out.extend_from_slice(&elem_len.to_be_bytes());
        // Append an actual body only for a sane positive length, capped so the
        // frame stays small while still driving `split_at_checked`/`decode_elem`.
        if elem_len > 0 {
            let body_len = usize::from(rng.u16() & 0x3f);
            let mut body = Vec::new();
            rng.fill(&mut body, body_len);
            out.extend_from_slice(&body);
        }
    }
}

/// Craft a semi-structured composite (row-type) binary frame so the fuzz
/// reaches the `CompositeReader` field walk: a leading `int32` field count from
/// a pool biased toward the driven arities (0..=3) plus hostile / negative
/// values, then a bounded number of `{oid i32, len i32, body}` field groups with
/// the length from a pool including the `-1` NULL sentinel, zero, small widths,
/// and hostile negative / huge values (the body random and sometimes truncated).
fn craft_composite(rng: &mut Rng, out: &mut Vec<u8>) {
    out.clear();
    let count_pool: [i32; 8] = [0, 1, 2, 3, 4, -1, rng_i32(rng), 0x7FFF_FFFF];
    let nfields = match rng.pick(&count_pool) {
        Some(&v) => v,
        None => 0,
    };
    out.extend_from_slice(&nfields.to_be_bytes());
    // Append a bounded number of field groups regardless of the declared count,
    // so both the "too few" and "surplus" tails are exercised.
    let field_groups = rng.bounded(6);
    for _ in 0..field_groups {
        let oid = rng.u32();
        out.extend_from_slice(&oid.to_be_bytes());
        let len_pool: [i32; 9] = [-1, 0, 1, 2, 4, 8, 16, -5, 0x7FFF_FFFF];
        let field_len = match rng.pick(&len_pool) {
            Some(&v) => v,
            None => 0,
        };
        out.extend_from_slice(&field_len.to_be_bytes());
        if field_len > 0 {
            let body_len = usize::from(rng.u16() & 0x3f);
            let mut body = Vec::new();
            rng.fill(&mut body, body_len);
            out.extend_from_slice(&body);
        }
    }
}

/// A random `i32` (four fresh bytes, reinterpreted — covers the full signed
/// range including the negatives the array/`numeric` headers must survive).
fn rng_i32(rng: &mut Rng) -> i32 {
    i32::from_le_bytes(rng.u32().to_le_bytes())
}

// ══════════════════════════════════════════════════════════════════════════
// The proof.
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn no_wire_decoder_panics_on_any_input() {
    // Install the recording hook for the whole fuzz (restored on scope exit).
    let guard = HookGuard::install();

    // ── Teeth: a deliberately-planted panic MUST be caught + captured by the
    // exact same harness. Proves the fuzz has teeth AND (since it would abort,
    // not report, under a `panic="abort"` test profile) confirms the profile
    // unwinds so `catch_unwind` is a real net. The closure is lexically inside
    // this `#[test]` fn, so `panic!` is in test context.
    let planted: fn(&[u8]) -> bool = |_b| panic!("planted teeth panic — intentional");
    let teeth = probe(planted, b"teeth-seed");
    let teeth_ok = match teeth {
        Probe::Panicked(ref msg) => msg.contains("planted teeth panic"),
        _ => false,
    };

    // ── Assemble the decoder tables.
    let binary_scalars = binary_scalar_decoders();
    let arrays = array_decoders();
    let composites = composite_reader_decoders();
    let text_misc = text_and_misc_decoders();
    let oid_pool = real_element_oids();

    // Every decoder receives the broad random length/content sweep.
    let mut all_random: Vec<NamedDecoder> = Vec::new();
    all_random.extend_from_slice(&binary_scalars);
    all_random.extend_from_slice(&arrays);
    all_random.extend_from_slice(&composites);
    all_random.extend_from_slice(&text_misc);

    let mut rng = Rng::new(SEED);
    let mut tally = Tally::new();

    // ── Sweep 1: broad random length/content across every decoder.
    let mut buf = Vec::new();
    let mut lengths: Vec<usize> = (0..=SMALL_MAX).collect();
    lengths.extend_from_slice(LARGE_LENGTHS);
    for &len in &lengths {
        for _ in 0..variants_for(len) {
            rng.fill(&mut buf, len);
            for &(name, dec) in &all_random {
                tally.record(name, &buf, probe(dec, &buf));
            }
        }
    }

    // ── Sweep 2: ASCII-biased corpus for the text/FromStr/SWAR/notification
    // decoders (reaches digit loops, sign/point handling, special words).
    for len in 0..=48usize {
        for _ in 0..48 {
            fill_ascii(&mut rng, &mut buf, len);
            for &(name, dec) in &text_misc {
                tally.record(name, &buf, probe(dec, &buf));
            }
        }
    }

    // ── Sweep 3: semi-structured numeric frames → the numeric decoders.
    let numeric_targets: [NamedDecoder; 2] = [
        ("Numeric:binary", |b| <Numeric as Cell<BinaryFmt>>::decode(b).is_ok()),
        ("Vec<Numeric>:binary", |b| <Vec<Option<Numeric>> as Cell<BinaryFmt>>::decode(b).is_ok()),
    ];
    for _ in 0..4000 {
        craft_numeric(&mut rng, &mut buf);
        for &(name, dec) in &numeric_targets {
            tally.record(name, &buf, probe(dec, &buf));
        }
    }

    // ── Sweep 4: semi-structured array frames → every array decoder (the
    // OID pool guarantees each one gets headers that reach its element loop).
    for _ in 0..2500 {
        craft_array(&mut rng, &mut buf, &oid_pool);
        for &(name, dec) in &arrays {
            tally.record(name, &buf, probe(dec, &buf));
        }
    }

    // ── Sweep 5: semi-structured composite frames → the composite reader at
    // every driven arity (reaches the field-count check, the per-field
    // `{oid, len, body}` walk, the NULL sentinel, and the surplus classification).
    for _ in 0..2500 {
        craft_composite(&mut rng, &mut buf);
        for &(name, dec) in &composites {
            tally.record(name, &buf, probe(dec, &buf));
        }
    }

    // Restore the normal panic hook BEFORE the assertions below, so any failure
    // message prints as usual.
    drop(guard);

    // ── The teeth assertion (uses the result captured while the hook was live).
    assert!(
        teeth_ok,
        "harness has NO TEETH: a deliberately-planted panic was not caught + captured — \
         catch_unwind/hook capture is broken, so a green run would prove nothing",
    );

    // ── Guard against a vacuous pass: the corpus must actually have run.
    assert!(
        tally.total >= 150_000,
        "fuzz corpus shrank to {} probes (<150k) — the sweep is not exercising the surface",
        tally.total,
    );

    // ── The universal-coverage claim: ZERO decoder panicked on ANY input.
    if !tally.findings.is_empty() {
        let mut report = format!(
            "\n{} DECODER PANIC(S) FOUND across {} total probes — a hostile server byte \
             could crash the driver:\n",
            tally.findings.len(),
            tally.total,
        );
        for f in &tally.findings {
            report.push_str(&format!(
                "  • decoder = {}\n    input   = {}\n    panic   = {}\n",
                f.decoder,
                to_hex_capped(&f.input),
                f.message,
            ));
        }
        panic!("{report}");
    }

    // Visible with `--nocapture`: the shape of the proof.
    eprintln!(
        "decoder_fuzz: {} probes, {} Ok + {} classified Err, 0 panics across {} decoders \
         (seed {SEED:#018x})",
        tally.total,
        tally.ok,
        tally.classified,
        all_random.len(),
    );
}
