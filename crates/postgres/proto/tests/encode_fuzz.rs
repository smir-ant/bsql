//! Outbound encode-tree total-function proof — the OUTBOUND mirror of
//! `decoder_fuzz`.
//!
//! # What this proves
//!
//! Every encoder that turns a Rust value into PostgreSQL wire bytes is a TOTAL
//! function: on ANY value — hostile, huge, NaN, subnormal, an edge `numeric`, a
//! multi-kilobyte `text`/`bytea` — it returns `Ok(())` or the classified
//! `WriteBufFull`, and NEVER panics or aborts. `decoder_fuzz` proves the INBOUND
//! (untrusted server bytes → value) side is panic-free; this is its outbound
//! (value → wire bytes) mirror, whose universal-coverage claim was asserted
//! nowhere before.
//!
//! # Surface covered
//!
//! * `bsql_postgres_proto::decode::EncodeBinary::encode_to` for every binary
//!   leaf: the scalars (`i16`/`i32`/`i64`/`u32`/`f32`/`f64`/`bool`), the
//!   length-prefixed `&str`/`&[u8]`, the temporal/JSON types
//!   (`Uuid`/`Timestamptz`/`Timestamp`/`Date`/`Time`/`Interval`/`Json`/`Jsonb`),
//!   and — the deep target — `Numeric::encode_to` (the base-10000 digit-group
//!   emission), fed edge `numeric` texts (huge digit runs, big/negative
//!   exponents, the `NaN`/`Infinity` specials).
//! * `bsql_postgres_proto::params::ParamsWriter::write_params` for several tuple
//!   arities mixing scalars, text, `Numeric`, and `Option` (SQL NULL) — the real
//!   `Bind`-frame parameter path.
//!
//! # Design
//!
//! Identical to `decoder_fuzz`: a dep-free xorshift64 PRNG with a FIXED seed (no
//! `rand`/`proptest`, no clock — reproducible, never flaky), each encode run
//! under `catch_unwind` with a recording panic hook, teeth (a planted panic
//! routed through the same harness) plus a `total >= 150_000` floor against a
//! vacuous pass. The sink is the BOUNDED [`WriteBuf`]: an over-large value can
//! only ever yield the classified `WriteBufFull`, so this gate cannot OOM — the
//! encode LOGIC (the branchy `numeric`/text/array paths) is byte-identical over
//! the bounded twin and the growable production sink, so its panic-safety is what
//! is proven here.

#![forbid(unsafe_code)]

use core::str::FromStr as _;
use std::cell::RefCell;
use std::panic::{self, AssertUnwindSafe};

use bsql_postgres_proto::decode::EncodeBinary;
use bsql_postgres_proto::{
    Date, Interval, Json, Jsonb, Numeric, ParamsWriter, Time, Timestamp, Timestamptz, Uuid,
    WriteBuf,
};

// ══════════════════════════════════════════════════════════════════════════
// Deterministic xorshift64 PRNG. No dependency, no clock, no thread_rng.
// ══════════════════════════════════════════════════════════════════════════

/// Fixed nonzero seed (golden-ratio constant) — makes every run identical.
const SEED: u64 = 0x9E37_79B9_7F4A_7C15;

struct Rng(u64);

impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn u8(&mut self) -> u8 {
        self.next_u64().to_le_bytes()[0]
    }

    fn u16(&mut self) -> u16 {
        let w = self.next_u64().to_le_bytes();
        u16::from_le_bytes([w[0], w[1]])
    }

    fn u32(&mut self) -> u32 {
        let w = self.next_u64().to_le_bytes();
        u32::from_le_bytes([w[0], w[1], w[2], w[3]])
    }

    fn i16(&mut self) -> i16 {
        i16::from_le_bytes(self.u16().to_le_bytes())
    }

    fn i32(&mut self) -> i32 {
        i32::from_le_bytes(self.u32().to_le_bytes())
    }

    fn i64(&mut self) -> i64 {
        i64::from_le_bytes(self.next_u64().to_le_bytes())
    }

    /// A `f32` from raw bits — covers NaN / ±inf / subnormal / normal uniformly.
    fn f32(&mut self) -> f32 {
        f32::from_bits(self.u32())
    }

    /// A `f64` from raw bits — same full coverage as [`Self::f32`].
    fn f64(&mut self) -> f64 {
        f64::from_bits(self.next_u64())
    }

    /// An index in `0..bound` (or `0` for an empty bound). Remainder (`%`), not
    /// the forbidden integer-division (`/`); the nonzero guard rules out div-by-0.
    fn bounded(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        usize::from(self.u16()) % bound
    }

    fn pick<'p, T>(&mut self, pool: &'p [T]) -> Option<&'p T> {
        let idx = self.bounded(pool.len());
        pool.get(idx)
    }

    /// `len` pseudo-random bytes.
    fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            for &b in self.next_u64().to_le_bytes().iter() {
                if out.len() >= len {
                    break;
                }
                out.push(b);
            }
        }
        out
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Panic capture — recording hook + RAII restore, and the per-value probe.
// ══════════════════════════════════════════════════════════════════════════

thread_local! {
    static LAST_PANIC: RefCell<Option<String>> = const { RefCell::new(None) };
}

type PanicHook = Box<dyn Fn(&panic::PanicHookInfo<'_>) + Sync + Send + 'static>;

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

enum Probe {
    Ok,
    Classified,
    Panicked(String),
}

/// Run one encode closure (`FnOnce() -> bool`, returning `is_ok`) under
/// `catch_unwind`, classifying the outcome. Total: never panics itself.
fn probe(f: impl FnOnce() -> bool) -> Probe {
    LAST_PANIC.with(|slot| {
        *slot.borrow_mut() = None;
    });
    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(true) => Probe::Ok,
        Ok(false) => Probe::Classified,
        Err(_) => {
            let captured = LAST_PANIC.with(|slot| slot.borrow_mut().take());
            let msg = match captured {
                Some(m) => m,
                None => String::from("<no message>"),
            };
            Probe::Panicked(msg)
        }
    }
}

struct Finding {
    encoder: &'static str,
    message: String,
}

struct Tally {
    total: u64,
    ok: u64,
    classified: u64,
    findings: Vec<Finding>,
}

impl Tally {
    fn new() -> Self {
        Self { total: 0, ok: 0, classified: 0, findings: Vec::new() }
    }

    fn record(&mut self, encoder: &'static str, outcome: Probe) {
        self.total = self.total.saturating_add(1);
        match outcome {
            Probe::Ok => self.ok = self.ok.saturating_add(1),
            Probe::Classified => self.classified = self.classified.saturating_add(1),
            Probe::Panicked(message) => self.findings.push(Finding { encoder, message }),
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Value builders + the single generic encode helper.
// ══════════════════════════════════════════════════════════════════════════

/// Encode one binary leaf into a FRESH bounded buffer; `true` = `Ok`, `false` =
/// the classified `WriteBufFull` (an over-large value). A panic is caught by
/// [`probe`], never returned here.
fn enc<T: EncodeBinary>(v: T) -> bool {
    let mut wb = WriteBuf::default();
    v.encode_to(&mut wb).is_ok()
}

/// Encode a parameter tuple through the real `Bind` param path.
fn enc_params<P: ParamsWriter>(p: P) -> bool {
    let mut wb = WriteBuf::default();
    p.write_params(&mut wb).is_ok()
}

/// A `String` from random bytes (via lossy UTF-8), capped so the corpus stays
/// small; a length past the bounded sink's capacity exercises the `WriteBufFull`
/// path, never an OOM.
fn fuzz_string(rng: &mut Rng, cap: usize) -> String {
    let len = rng.bounded(cap.saturating_add(1));
    String::from_utf8_lossy(&rng.bytes(len)).into_owned()
}

/// A crafted `numeric` text reaching the deep encode branches: pure-random ASCII
/// from the numeric alphabet, huge digit runs (force the base-10000 group loop
/// + weight range), big/negative exponents, and the `NaN`/`Infinity` specials.
fn fuzz_numeric_text(rng: &mut Rng) -> String {
    const ALPHABET: &[u8; 17] = b"0123456789.-+eEn ";
    let shape = rng.bounded(7);
    match shape {
        0 => {
            // Pure-random from the numeric alphabet (mostly rejected by FromStr,
            // but the accepted ones reach encode).
            let len = rng.bounded(40);
            let mut s = String::with_capacity(len);
            for _ in 0..len {
                let idx = usize::from(rng.u16()) % ALPHABET.len();
                if let Some(&b) = ALPHABET.get(idx) {
                    s.push(char::from(b));
                }
            }
            s
        }
        1 => "9".repeat(rng.bounded(3000)),                       // huge integer
        2 => format!("0.{}", "9".repeat(rng.bounded(3000))),     // huge fraction
        3 => format!("{}e{}", rng.bounded(50), rng.i16()),       // exponent
        4 => format!("-{}.{}", rng.bounded(9999), rng.bounded(9999)),
        5 => {
            let specials = ["NaN", "Infinity", "-Infinity", "nan", "inf", "1E-1000", "1E1000"];
            match rng.pick(&specials) {
                Some(&s) => s.to_string(),
                None => String::new(),
            }
        }
        _ => format!("{}", rng.i64()),
    }
}

/// A hyphenated UUID text from 16 random bytes (always parses → reaches encode).
fn fuzz_uuid_text(rng: &mut Rng) -> String {
    let b = rng.bytes(16);
    let mut hex = String::with_capacity(32);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &byte in &b {
        if let Some(&c) = HEX.get(usize::from(byte >> 4)) {
            hex.push(char::from(c));
        }
        if let Some(&c) = HEX.get(usize::from(byte & 0x0f)) {
            hex.push(char::from(c));
        }
    }
    // Insert the 8-4-4-4-12 hyphens.
    let mut out = String::with_capacity(36);
    for (i, ch) in hex.chars().enumerate() {
        if i == 8 || i == 12 || i == 16 || i == 20 {
            out.push('-');
        }
        out.push(ch);
    }
    out
}

// ══════════════════════════════════════════════════════════════════════════
// The proof.
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn no_encoder_panics_on_any_value() {
    let guard = HookGuard::install();

    // Teeth: a deliberately-planted panic MUST be caught + captured — proves the
    // harness works AND (since it would abort under `panic="abort"`) that the test
    // profile unwinds so `catch_unwind` is a real net.
    let teeth = probe(|| panic!("planted teeth panic — intentional"));
    let teeth_ok = matches!(teeth, Probe::Panicked(ref m) if m.contains("planted teeth panic"));

    let mut rng = Rng::new(SEED);
    let mut tally = Tally::new();

    for _ in 0..9000u32 {
        // ── Scalars (any bit pattern is a valid value).
        let a16 = rng.i16();
        tally.record("i16", probe(move || enc(a16)));
        let a32 = rng.i32();
        tally.record("i32", probe(move || enc(a32)));
        let a64 = rng.i64();
        tally.record("i64", probe(move || enc(a64)));
        let au32 = rng.u32();
        tally.record("u32", probe(move || enc(au32)));
        let af32 = rng.f32();
        tally.record("f32", probe(move || enc(af32)));
        let af64 = rng.f64();
        tally.record("f64", probe(move || enc(af64)));
        let ab = rng.u16() & 1 == 0;
        tally.record("bool", probe(move || enc(ab)));

        // ── Length-prefixed text / bytes (huge → WriteBufFull, never OOM).
        let s = fuzz_string(&mut rng, 4096);
        tally.record("&str", probe(move || enc(s.as_str())));
        let by_len = rng.bounded(4096);
        let by = rng.bytes(by_len);
        tally.record("&[u8]", probe(move || enc(by.as_slice())));

        // ── Temporal / JSON leaves.
        let ts = Timestamptz::from_micros(rng.i64());
        tally.record("Timestamptz", probe(move || enc(ts)));
        let tsp = Timestamp::from_micros(rng.i64());
        tally.record("Timestamp", probe(move || enc(tsp)));
        let tm = Time::from_micros(rng.i64());
        tally.record("Time", probe(move || enc(tm)));
        let iv = Interval::new(rng.i32(), rng.i32(), rng.i64());
        tally.record("Interval", probe(move || enc(iv)));
        // `Date::from_civil` is `Option`; a bad civil date (month/day 0, or an
        // out-of-range combination) is a builder reject, not an encode target, so
        // the encoder is fed only a real Date. `% 14`/`% 32` keep month/day in a
        // plausible band (some invalid, exercising the reject path too).
        let month = rng.u8() % 14;
        let day = rng.u8() % 32;
        let year = rng.i32();
        if let Some(d) = Date::from_civil(year, month, day) {
            tally.record("Date", probe(move || enc(d)));
        }
        let js = Json::new(fuzz_string(&mut rng, 512));
        tally.record("Json", probe(move || enc(js)));
        let jb = Jsonb::new(fuzz_string(&mut rng, 512));
        tally.record("Jsonb", probe(move || enc(jb)));
        let ut = fuzz_uuid_text(&mut rng);
        if let Ok(u) = Uuid::from_str(&ut) {
            tally.record("Uuid", probe(move || enc(u)));
        }

        // ── Numeric — the deep base-10000 emission, fed edge texts.
        let nt = fuzz_numeric_text(&mut rng);
        tally.record(
            "Numeric::encode_to",
            probe(move || match Numeric::from_str(&nt) {
                Ok(n) => enc(n),
                // A parse rejection is `decoder_fuzz`'s surface, not this encode
                // gate's — count it as a non-panic pass.
                Err(_) => true,
            }),
        );

        // ── ParamsWriter — the real Bind param path, mixed arities + NULL.
        let p_a = rng.i32();
        tally.record("params(i32,)", probe(move || enc_params((p_a,))));
        let p_s = fuzz_string(&mut rng, 256);
        let p_b = rng.i64();
        tally.record("params(i64,&str)", probe(move || enc_params((p_b, p_s.as_str()))));
        let opt: Option<i64> = if rng.u16() & 1 == 0 { Some(rng.i64()) } else { None };
        let p_f = rng.f64();
        let p_bool = rng.u16() & 1 == 0;
        tally.record(
            "params(Option<i64>,f64,bool)",
            probe(move || enc_params((opt, p_f, p_bool))),
        );
        // A Numeric param routes Numeric::encode_to through the length-prefixed
        // param path (its production caller).
        let pnt = fuzz_numeric_text(&mut rng);
        let pby_len = rng.bounded(300);
        let pby = rng.bytes(pby_len);
        tally.record(
            "params(Numeric?,&[u8])",
            probe(move || match Numeric::from_str(&pnt) {
                Ok(n) => enc_params((n, pby.as_slice())),
                Err(_) => true,
            }),
        );
    }

    drop(guard);

    assert!(
        teeth_ok,
        "harness has NO TEETH: a planted panic was not caught + captured — the gate would prove nothing"
    );
    assert!(
        tally.total >= 150_000,
        "encode fuzz corpus shrank to {} probes (<150k) — the sweep is not exercising the surface",
        tally.total
    );
    if !tally.findings.is_empty() {
        let mut report = format!(
            "\n{} ENCODER PANIC(S) FOUND across {} probes — a hostile/edge param could crash the driver:\n",
            tally.findings.len(),
            tally.total
        );
        for f in &tally.findings {
            report.push_str(&format!("  • encoder = {}\n    panic   = {}\n", f.encoder, f.message));
        }
        panic!("{report}");
    }

    eprintln!(
        "encode_fuzz: {} probes, {} Ok + {} classified (WriteBufFull), 0 panics (seed {SEED:#018x})",
        tally.total, tally.ok, tally.classified
    );
}
