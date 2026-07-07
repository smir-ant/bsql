//! Build-time inference / replay total-function proof.
//!
//! # What this proves
//!
//! `bsql-build` runs in a consumer's `build.rs`: it replays the migration set
//! into a schema catalog, and parses the catalog / user-types / bridge channels.
//! On ANY input — arbitrary or hostile migration SQL, a corrupt catalog blob, a
//! malformed user-types or bridge spec — it must LOUD-ERROR (a classified
//! `BuildError` / parse error), and NEVER panic, hang, or OOM. A build-time panic
//! is bad developer experience (an opaque `build.rs` crash with no source line);
//! a hang or OOM stalls every build. This gate is the machine proof and a
//! permanent regression wall.
//!
//! # Surface covered
//!
//! * `bsql_build::catalog_from_dir` — the migration replay engine (the heavy
//!   path: file walk → `sqlparser` parse → inference), fed crafted hostile SQL
//!   written to a real temp file.
//! * `bsql_build::parse_catalog` / `parse_user_types` / `parse_bridges` — the
//!   three public channel parsers, fed arbitrary bytes (lossy-decoded) and
//!   semi-structured snippets.
//!
//! # Design
//!
//! The `decoder_fuzz` pattern: a dep-free xorshift64 PRNG with a FIXED seed, each
//! call under `catch_unwind` with a recording hook, teeth (a planted panic) + a
//! `total >= 150_000` floor, PLUS a separate floor on the heavy replay path so it
//! cannot be starved. Input SIZE is capped (≤ 2 KiB) so a hostile parse is
//! bounded — no OOM, no unbounded work.

#![forbid(unsafe_code)]

use std::cell::RefCell;
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;

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

/// Run one closure (`FnOnce() -> bool`, returning `is_ok`) under `catch_unwind`.
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
    target: &'static str,
    input: String,
    message: String,
}

struct Tally {
    total: u64,
    ok: u64,
    classified: u64,
    replay: u64,
    findings: Vec<Finding>,
}

impl Tally {
    fn new() -> Self {
        Self { total: 0, ok: 0, classified: 0, replay: 0, findings: Vec::new() }
    }

    fn record(&mut self, target: &'static str, input: &str, outcome: Probe) {
        self.total = self.total.saturating_add(1);
        match outcome {
            Probe::Ok => self.ok = self.ok.saturating_add(1),
            Probe::Classified => self.classified = self.classified.saturating_add(1),
            Probe::Panicked(message) => self.findings.push(Finding {
                target,
                input: input.chars().take(200).collect(),
                message,
            }),
        }
    }
}

// ══════════════════════════════════════════════════════════════════════════
// SQL / channel-text builders.
// ══════════════════════════════════════════════════════════════════════════

/// Concatenate a random selection of DDL fragments + random tokens into one
/// migration file body, capped at 2 KiB (bounds any hostile parse — no OOM).
fn craft_sql(rng: &mut Rng) -> String {
    const FRAGMENTS: &[&str] = &[
        "CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL, qty BIGINT)",
        "CREATE TABLE u (a INT REFERENCES t(id), b TEXT)",
        "CREATE TYPE mood AS ENUM ('happy', 'sad')",
        "ALTER TYPE mood ADD VALUE 'meh'",
        "ALTER TYPE mood ADD VALUE 'x' BEFORE 'sad'",
        "ALTER TYPE mood RENAME VALUE 'happy' TO 'glad'",
        "ALTER TYPE mood RENAME TO feeling",
        "CREATE DOMAIN age AS INT CHECK (VALUE >= 0)",
        "CREATE TYPE addr AS (street TEXT, num INT)",
        "ALTER TABLE t ADD COLUMN extra NUMERIC(10,2)",
        "ALTER TABLE t DROP COLUMN qty",
        "DROP TABLE t",
        "DROP TYPE mood",
        "CREATE INDEX idx ON t (name)",
        "SELECT 1;",
        "-- a comment\n",
        "((((",
        "'unterminated",
        "$$dollar quoted$$",
        ";;;;",
        "CREATE TABLE",
        "CREATE TYPE mood AS ENUM (",
    ];
    let mut sql = String::new();
    let pieces = rng.bounded(8).saturating_add(1);
    for _ in 0..pieces {
        if sql.len() >= 2048 {
            break;
        }
        match rng.bounded(4) {
            0 => {
                // A random raw token.
                let len = rng.bounded(24);
                let raw = rng.bytes(len);
                sql.push_str(&String::from_utf8_lossy(&raw));
            }
            _ => {
                if let Some(&frag) = rng.pick(FRAGMENTS) {
                    sql.push_str(frag);
                }
            }
        }
        sql.push(';');
        sql.push('\n');
    }
    sql
}

/// Arbitrary channel text: random bytes lossy-decoded, capped.
fn craft_channel_text(rng: &mut Rng) -> String {
    let len = rng.bounded(512);
    let raw = rng.bytes(len);
    String::from_utf8_lossy(&raw).into_owned()
}

// ══════════════════════════════════════════════════════════════════════════
// The proof.
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn build_inference_never_panics_on_any_input() {
    let guard = HookGuard::install();

    // Teeth: a planted panic must be caught + captured.
    let teeth = probe(|| panic!("planted teeth panic — intentional"));
    let teeth_ok = matches!(teeth, Probe::Panicked(ref m) if m.contains("planted teeth panic"));

    let mut rng = Rng::new(SEED);
    let mut tally = Tally::new();

    // ── The heavy path: replay crafted hostile SQL through `catalog_from_dir`.
    // One temp dir, one `.sql` file rewritten per probe.
    let dir = temp_dir();
    std::fs::create_dir_all(&dir).expect("create temp migrations dir");
    let file = dir.join("0001_fuzz.sql");
    for _ in 0..3_000u32 {
        let sql = craft_sql(&mut rng);
        std::fs::write(&file, sql.as_bytes()).expect("write fuzz migration");
        let d = dir.clone();
        let outcome = probe(move || bsql_build::catalog_from_dir(&d).is_ok());
        tally.replay = tally.replay.saturating_add(1);
        tally.record("catalog_from_dir", &sql, outcome);
    }
    // Best-effort cleanup (a leftover temp dir is harmless).
    drop(std::fs::remove_dir_all(&dir));

    // ── The light path: the three channel parsers at high volume.
    for _ in 0..50_000u32 {
        let t1 = craft_channel_text(&mut rng);
        tally.record("parse_catalog", &t1, probe(|| bsql_build::parse_catalog(&t1).is_ok()));
        let t2 = craft_channel_text(&mut rng);
        tally.record("parse_user_types", &t2, probe(|| bsql_build::parse_user_types(&t2).is_ok()));
        let t3 = craft_channel_text(&mut rng);
        tally.record("parse_bridges", &t3, probe(|| bsql_build::parse_bridges(&t3).is_ok()));
    }

    drop(guard);

    assert!(
        teeth_ok,
        "harness has NO TEETH: a planted panic was not caught + captured"
    );
    assert!(
        tally.total >= 150_000,
        "build fuzz corpus shrank to {} probes (<150k)",
        tally.total
    );
    assert!(
        tally.replay >= 2_000,
        "the heavy replay path ran only {} times (<2k) — it is being starved",
        tally.replay
    );
    if !tally.findings.is_empty() {
        let mut report = format!(
            "\n{} BUILD-TIME PANIC(S) FOUND across {} probes — a hostile migration could crash build.rs:\n",
            tally.findings.len(),
            tally.total
        );
        for f in &tally.findings {
            report.push_str(&format!(
                "  • target = {}\n    input  = {:?}\n    panic  = {}\n",
                f.target, f.input, f.message
            ));
        }
        panic!("{report}");
    }

    eprintln!(
        "infer_fuzz: {} probes ({} replay), {} Ok + {} classified, 0 panics (seed {SEED:#018x})",
        tally.total, tally.replay, tally.ok, tally.classified
    );
}

/// A process-unique temp dir for the replay corpus.
fn temp_dir() -> PathBuf {
    let mut p: PathBuf = std::env::temp_dir();
    p.push(format!("bsql_infer_fuzz_{}", std::process::id()));
    p
}
