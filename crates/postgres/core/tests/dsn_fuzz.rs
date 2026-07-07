//! DSN-parser total-function proof for `ConnectConfig::from_dsn`.
//!
//! # What this proves
//!
//! `from_dsn` turns an UNTRUSTED connection string (a `DATABASE_URL`, a CLI arg,
//! a config value) into a `ConnectConfig`. On ANY input — non-UTF-8 (via lossy
//! decode), truncated, huge, percent-encoded, an IPv6 bracket edge, a malformed
//! `host=`/`sslmode=`/`connect_timeout=` query param — it must return `Ok` or a
//! CLASSIFIED `Err(String)`, and NEVER panic. A panicking DSN parser is a
//! denial-of-service on any service that parses a user-supplied URL. This gate
//! is the machine proof, and a permanent regression wall (it would have caught
//! the bracketed-IPv6-without-port mis-parse).
//!
//! # Design
//!
//! The `decoder_fuzz` pattern: a dep-free xorshift64 PRNG with a FIXED seed (no
//! `rand`, no clock — reproducible), each parse under `catch_unwind` with a
//! recording hook, teeth (a planted panic through the same harness) + a
//! `total >= 150_000` floor. Two corpora: purely random bytes (lossy-decoded to a
//! `&str`, almost all rejected at the scheme check — the outer guard), and
//! STRUCTURED `postgres://…` strings that reach the deep userinfo / host:port /
//! IPv6-bracket / query-param branches. The query part is crafted only from the
//! PURE-PARSE keys (`host`/`sslmode`/`connect_timeout`); `sslrootcert` is
//! deliberately never generated because it reads a FILE at parse time (I/O side
//! effect), and a random string cannot reach it (it never starts with
//! `postgres://`).

#![forbid(unsafe_code)]

use std::cell::RefCell;
use std::panic::{self, AssertUnwindSafe};

use bsql_postgres_core::ConnectConfig;

// ══════════════════════════════════════════════════════════════════════════
// Deterministic xorshift64 PRNG.
// ══════════════════════════════════════════════════════════════════════════

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

    fn u16(&mut self) -> u16 {
        let w = self.next_u64().to_le_bytes();
        u16::from_le_bytes([w[0], w[1]])
    }

    fn i16(&mut self) -> i16 {
        i16::from_le_bytes(self.u16().to_le_bytes())
    }

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
// Panic capture.
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

/// Parse one DSN under `catch_unwind`. `Ok`/`Err` are both non-panic; only a
/// panic is a finding.
fn probe(dsn: &str) -> Probe {
    LAST_PANIC.with(|slot| {
        *slot.borrow_mut() = None;
    });
    let input = dsn.to_string();
    match panic::catch_unwind(AssertUnwindSafe(move || ConnectConfig::from_dsn(&input).is_ok())) {
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
    input: String,
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

    fn record(&mut self, input: &str, outcome: Probe) {
        self.total = self.total.saturating_add(1);
        match outcome {
            Probe::Ok => self.ok = self.ok.saturating_add(1),
            Probe::Classified => self.classified = self.classified.saturating_add(1),
            Probe::Panicked(message) => {
                self.findings.push(Finding { input: input.to_string(), message });
            }
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════
// DSN builders.
// ══════════════════════════════════════════════════════════════════════════

/// A short ASCII-ish token from a pool biased toward DSN-significant characters
/// (`@ : / ? & = [ ] % .`), so structured DSNs reach the delimiter branches.
fn token(rng: &mut Rng, max: usize) -> String {
    const ALPHABET: &[u8; 24] = b"abcAB012@:/?&=[].%- \t_~+";
    let len = rng.bounded(max.saturating_add(1));
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        let idx = usize::from(rng.u16()) % ALPHABET.len();
        if let Some(&b) = ALPHABET.get(idx) {
            s.push(char::from(b));
        }
    }
    s
}

/// A structured `postgres://…` DSN reaching the deep parse branches: userinfo
/// (with/without password, sometimes empty), a host from a pool that includes
/// bare names, empty, loopback, and well-/mal-formed IPv6 literals, an optional
/// port (empty / valid / out-of-range / non-numeric / bare colon), an optional
/// database, and an optional query string of pure-parse params.
fn structured_dsn(rng: &mut Rng) -> String {
    let scheme = match rng.pick(&["postgres://", "postgresql://"]) {
        Some(&s) => s,
        None => "postgres://",
    };
    // userinfo
    let user = token(rng, 6);
    let userinfo = match rng.bounded(3) {
        0 => user,
        1 => format!("{user}:{}", token(rng, 6)),
        _ => String::new(),
    };
    // host
    let host_pool = [
        "h",
        "",
        "localhost",
        "127.0.0.1",
        "[::1]",
        "[2001:db8::1]",
        "[::1", // unterminated
        "]",
        "[]",
        "example.com",
    ];
    let host = match rng.pick(&host_pool) {
        Some(&h) => h.to_string(),
        None => String::new(),
    };
    // port
    let port_pool = ["", ":5432", ":0", ":99999", ":-1", ":abc", ":", ":65535"];
    let port = match rng.pick(&port_pool) {
        Some(&p) => p.to_string(),
        None => String::new(),
    };
    // database
    let db = match rng.bounded(3) {
        0 => String::new(),
        1 => "/mydb".to_string(),
        _ => format!("/{}", token(rng, 6)),
    };
    // query (pure-parse keys only; never sslrootcert — it reads a file)
    let query = if rng.bounded(2) == 0 {
        String::new()
    } else {
        let param_pool = [
            "host=/tmp",
            "host=realhost",
            "host=",
            "sslmode=require",
            "sslmode=prefer",
            "sslmode=disable",
            "sslmode=bogus",
            "connect_timeout=5",
            "connect_timeout=abc",
            "connect_timeout=",
            "k",
            "k=v",
            "=",
            "&",
        ];
        let count = rng.bounded(4).saturating_add(1);
        let mut parts: Vec<String> = Vec::with_capacity(count);
        for _ in 0..count {
            match rng.pick(&param_pool) {
                Some(&p) => parts.push(p.to_string()),
                None => parts.push(token(rng, 8)),
            }
        }
        format!("?{}", parts.join("&"))
    };
    format!("{scheme}{userinfo}@{host}{port}{db}{query}")
}

// ══════════════════════════════════════════════════════════════════════════
// The proof.
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn from_dsn_never_panics_on_any_input() {
    let guard = HookGuard::install();

    // Teeth: a planted panic must be caught + captured.
    let teeth_ok = {
        LAST_PANIC.with(|slot| {
            *slot.borrow_mut() = None;
        });
        let caught =
            panic::catch_unwind(AssertUnwindSafe(|| panic!("planted teeth panic — intentional")));
        let captured = LAST_PANIC.with(|slot| slot.borrow_mut().take());
        let captured_ok = match captured {
            Some(m) => m.contains("planted teeth panic"),
            None => false,
        };
        caught.is_err() && captured_ok
    };

    let mut rng = Rng::new(SEED);
    let mut tally = Tally::new();

    for _ in 0..90_000u32 {
        // Corpus 1: purely random bytes, lossy-decoded (outer scheme guard).
        let raw_len = rng_len(&mut rng);
        let raw = rng.bytes(raw_len);
        let lossy = String::from_utf8_lossy(&raw).into_owned();
        tally.record(&lossy, probe(&lossy));

        // Corpus 2: structured postgres:// strings (deep branches).
        let dsn = structured_dsn(&mut rng);
        tally.record(&dsn, probe(&dsn));
    }

    drop(guard);

    assert!(
        teeth_ok,
        "harness has NO TEETH: a planted panic was not caught + captured"
    );
    assert!(
        tally.total >= 150_000,
        "dsn fuzz corpus shrank to {} probes (<150k)",
        tally.total
    );
    if !tally.findings.is_empty() {
        let mut report = format!(
            "\n{} DSN PARSER PANIC(S) FOUND across {} probes:\n",
            tally.findings.len(),
            tally.total
        );
        for f in &tally.findings {
            report.push_str(&format!("  • input = {:?}\n    panic = {}\n", f.input, f.message));
        }
        panic!("{report}");
    }

    eprintln!(
        "dsn_fuzz: {} probes, {} Ok + {} classified Err, 0 panics (seed {SEED:#018x})",
        tally.total, tally.ok, tally.classified
    );
}

/// A length biased small with an occasional large draw (truncation + huge-input
/// coverage). Split out so the two `&mut rng` uses in the caller do not nest.
fn rng_len(rng: &mut Rng) -> usize {
    let big = rng.i16() & 0x7 == 0;
    if big {
        rng.bounded(2048)
    } else {
        rng.bounded(48)
    }
}
