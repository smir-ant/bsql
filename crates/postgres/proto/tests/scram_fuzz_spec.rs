//! ENGINE-LEVEL SCRAM never-Ready witnesses — the production SCRAM dispatch
//! path, driven through the connecting-phase engine.
//!
//! # Scope + division of labour
//!
//! This file proves ONE invariant end-to-end through
//! [`ConnectingEngine::next_auth_event`] (the same code path the live driver
//! drives): **adversarial server bytes never complete the handshake** — a
//! garbage `server-first` / `server-final` can only reach
//! [`AuthEvent::Fail`] (a classified terminal), never [`AuthEvent::Ready`].
//!
//! The complementary PANIC-SAFETY proof of the two `pub(crate)` wire parsers
//! ([`parse_server_first`] / [`parse_server_final`]) and the constant-time
//! verifier lives in an in-crate `#[cfg(test)]` module
//! (`scram::wire::total_function_fuzz`): fuzzed CRYPTO-FREE over 50k+ inputs,
//! it runs in well under a second because it does not route through the
//! engine's PBKDF2 key derivation. This file therefore keeps ONLY the
//! genuinely engine-bound work — a small, crafted witness table (not thousands
//! of random samples). Reaching a `server-final` witness costs exactly one
//! derivation apiece, so the table is deliberately curated, not random-swept.
//!
//! # Methodology
//!
//! - The two RANDOM cheap tests below (`server_first`, `arbitrary_bytes`) still
//!   sweep random bytes through a fresh engine — those never advance to the
//!   PBKDF2 path (a random `server-first` almost never parses), so they cost
//!   almost nothing while proving the engine tolerates arbitrary junk.
//! - The CRAFTED witness test drives a curated table of adversarial
//!   `server-first` (parse-fail, 0 crypto) and `server-final` (post-derivation,
//!   1 crypto apiece) messages; each is asserted to classify as
//!   [`AuthEvent::Fail`] and never reach [`AuthEvent::Ready`]. The teeth: every
//!   `server-final` witness must ACTUALLY reach the `server-final` parser
//!   (`reached_final == count`), so a broken setup that never gets past
//!   `server-first` fails loudly rather than passing vacuously.
//!
//! A full CORRECT SCRAM exchange reaching [`AuthEvent::Ready`] is proven
//! separately by `engine_connect_spec.rs`'s `ScramServer`, so the never-Ready
//! assertions here are non-vacuous (the engine CAN reach Ready — just not on
//! adversarial input).
//!
//! [`parse_server_first`]: bsql_postgres_proto::scram
//! [`parse_server_final`]: bsql_postgres_proto::scram
//! [`ConnectingEngine::next_auth_event`]: bsql_postgres_proto::engine::ConnectingEngine
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::disallowed_methods,
    reason = "integration-test fuzz harness — unwrap/panic are the loud failure signals on the offline setup + invariant-violation paths; clippy's allow-in-tests carve-out reaches #[test] fns but not the free helper fns this file factors out (the never-Ready invariant panics from a helper)."
)]

use bsql_postgres_proto::engine::{AuthEvent, ConnectingEngine, SendBuf};
use bsql_postgres_proto::ident::Ident;
use bsql_postgres_proto::password::{Credentials, Password};
use bsql_postgres_proto::sensitive::Sensitive;

const SCRAM_FUZZ_ITERS: u32 = 5_000;

/// Byte length of the `user=corpus` startup packet — the offset at which the
/// `SASLInitialResponse` begins in the client wire (mirrors
/// `engine_connecting_spec`'s constant; the username below is `corpus`).
/// Includes the always-sent `client_encoding=UTF8` parameter.
const STARTUP_LEN: usize = 42;

// ─────────────────────────── PRNG ───────────────────────────

/// Deterministic xorshift PRNG — mirrors `fuzz_stress_spec.rs`.
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

// ─────────────────────────── frame + engine helpers ───────────────────────────

/// Build a backend frame: 1-byte tag + i32 length (incl. itself) + body.
fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len().saturating_add(4)).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// Build an `Authentication` (`'R'`) frame: i32 sub-code + extra payload.
fn auth(sub_code: i32, extra: &[u8]) -> Vec<u8> {
    let mut body = sub_code.to_be_bytes().to_vec();
    body.extend_from_slice(extra);
    frame(b'R', &body)
}

fn user() -> Ident {
    Ident::try_from_str("corpus").unwrap()
}

fn scram_creds() -> Credentials {
    Credentials::ScramPassword(Sensitive::new(Password::try_from_str("pencil").unwrap()))
}

/// Feed scripted server bytes through the ingest slot. Returns `false` if the
/// bounded ingest buffer rejected the bytes (an over-cap fuzz body) — the
/// caller skips that iteration rather than treating it as a parser outcome.
fn feed(engine: &mut ConnectingEngine, bytes: &[u8]) -> bool {
    let mut fed = 0usize;
    while fed < bytes.len() {
        let remaining = match bytes.get(fed..) {
            Some(r) => r,
            None => return false,
        };
        let slot = match engine.read_slot(remaining.len()) {
            Ok(s) => s,
            Err(_) => return false,
        };
        let n = slot.len().min(remaining.len());
        if let (Some(dst), Some(src)) = (slot.get_mut(..n), remaining.get(..n)) {
            dst.copy_from_slice(src);
        }
        if engine.commit(n).is_err() {
            return false;
        }
        fed = fed.saturating_add(n);
    }
    true
}

/// Drive the engine to a settled auth outcome after a fuzzed frame, asserting
/// the handshake never reaches [`AuthEvent::Ready`] from adversarial input.
/// Returns `true` if the run classified a failure ([`AuthEvent::Fail`]).
///
/// The bounded loop is safe: every terminal phase (`Failed`/`Ready`) is
/// idempotent and `AuthSaslContinue` is followed by `NeedMore` (no further
/// scripted frames), so a handful of polls reaches a fixpoint.
fn drain_assert_never_ready(engine: &mut ConnectingEngine, sb: &mut SendBuf) -> bool {
    let mut classified = false;
    for _ in 0..6 {
        match engine.next_auth_event(sb) {
            AuthEvent::Ready => panic!("adversarial SCRAM input drove the handshake to Ready"),
            AuthEvent::Fail(_) => {
                classified = true;
                break;
            }
            AuthEvent::NeedMore => break,
            AuthEvent::AuthSaslContinue(_)
            | AuthEvent::ParamStatus(_)
            | AuthEvent::AuthCleartext
            | AuthEvent::AuthMd5 { .. } => {}
        }
    }
    classified
}

/// Extract the client nonce from the queued `SASLInitialResponse` so a fuzz
/// path can build a VALID `server-first` to reach the `server-final` parser.
fn extract_client_nonce(client_bytes: &[u8]) -> Vec<u8> {
    let Some(frame) = client_bytes.get(STARTUP_LEN..) else {
        return Vec::new();
    };
    let Some(body) = frame.get(5..) else {
        return Vec::new();
    };
    let mech_end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
    let msg_start = mech_end.saturating_add(1).saturating_add(4);
    let Some(client_first) = body.get(msg_start..) else {
        return Vec::new();
    };
    let Ok(text) = core::str::from_utf8(client_first) else {
        return Vec::new();
    };
    for part in text.split(',') {
        if let Some(nonce) = part.strip_prefix("r=") {
            return nonce.as_bytes().to_vec();
        }
    }
    Vec::new()
}

/// Start a fresh engine and drive it past `AuthenticationSASL` so the next
/// scripted frame is the `server-first` (SASLContinue). Returns the engine +
/// its send buffer, or `None` if the offline setup did not settle as expected.
fn engine_awaiting_server_first() -> Option<(ConnectingEngine, SendBuf)> {
    let mut sb = SendBuf::new();
    let mut engine = ConnectingEngine::start(&mut sb, &user(), None, &[], scram_creds()).ok()?;
    if !feed(&mut engine, &auth(10, b"SCRAM-SHA-256\0\0")) {
        return None;
    }
    // The engine queues the SASLInitialResponse silently and awaits the
    // server-first: the settled event is NeedMore.
    if !matches!(engine.next_auth_event(&mut sb), AuthEvent::NeedMore) {
        return None;
    }
    Some((engine, sb))
}

// ───────────────────────────────────────────────────────────────────
// Invariant: arbitrary server-first body never panics, never silently passes
// ───────────────────────────────────────────────────────────────────

#[test]
fn scram_server_first_fuzz_never_panics_never_silent_pass() {
    let mut rng = XorShift64::new(0xA55A_0001);
    let mut classified: u32 = 0;
    let mut advanced: u32 = 0;

    for _ in 0..SCRAM_FUZZ_ITERS {
        let Some((mut engine, mut sb)) = engine_awaiting_server_first() else {
            continue;
        };

        let body_len = rng.len_up_to(1024);
        let mut body = vec![0u8; body_len];
        rng.fill(&mut body);

        if !feed(&mut engine, &auth(11, &body)) {
            continue;
        }
        if drain_assert_never_ready(&mut engine, &mut sb) {
            classified = classified.saturating_add(1);
        } else {
            advanced = advanced.saturating_add(1);
        }
    }

    // Either outcome is legal (a classified Fail, or a lucky parse that
    // advanced to await server-final). The load-bearing assertions are inside
    // `drain_assert_never_ready` (never Ready) + the absence of any panic.
    assert!(
        classified.saturating_add(advanced) > 0,
        "expected the fuzz run to reach the server-first parser at least once",
    );
}

// ───────────────────────────────────────────────────────────────────
// Crafted engine-level never-Ready witnesses (replacing the old 5000× random
// server-final loop that recomputed a bit-identical PBKDF2 every iteration).
// ───────────────────────────────────────────────────────────────────

/// Base64-encode raw bytes to an owned ASCII `Vec` for building crafted
/// verifier / salt fields (dev-dep `base64ct`, the same encoder the crate
/// uses). Bounded 256-byte scratch covers every witness below.
fn b64(bytes: &[u8]) -> Vec<u8> {
    use base64ct::{Base64, Encoding};
    let mut buf = [0u8; 256];
    match Base64::encode(bytes, &mut buf) {
        Ok(s) => s.as_bytes().to_vec(),
        Err(_) => Vec::new(),
    }
}

/// A `v=<base64(verifier)>` server-final body from raw verifier bytes.
fn v_witness(verifier: &[u8]) -> Vec<u8> {
    let mut out = b"v=".to_vec();
    out.extend_from_slice(&b64(verifier));
    out
}

/// A `server-first` witness builder: takes the engine's client nonce (base64)
/// and returns the message body fed as `auth(11, …)`.
type FirstWitness = (&'static str, fn(&str) -> Vec<u8>);

/// Crafted adversarial `server-first` messages. Each fails to PARSE (nonce
/// mismatch, bad/empty salt, out-of-range iterations, reserved mandatory
/// extension, missing fields, wrong field order, non-UTF-8) BEFORE the engine
/// reaches PBKDF2 — 0 crypto — and must classify as `Fail`, never advancing.
fn first_witnesses() -> Vec<FirstWitness> {
    vec![
        ("empty", |_n| Vec::new()),
        ("no_scram_shape", |_n| b"garbage-not-scram".to_vec()),
        ("only_commas", |_n| b",,,".to_vec()),
        ("r_field_only", |n| format!("r={n}").into_bytes()),
        ("nonce_prefix_mismatch", |_n| {
            b"r=WRONGNONCE,s=Wg==,i=4096".to_vec()
        }),
        ("empty_salt", |n| format!("r={n}Srv,s=,i=4096").into_bytes()),
        ("bad_base64_salt", |n| {
            format!("r={n}Srv,s=@@@@,i=4096").into_bytes()
        }),
        ("iterations_too_low", |n| {
            format!("r={n}Srv,s=Wg==,i=1").into_bytes()
        }),
        ("iterations_zero", |n| {
            format!("r={n}Srv,s=Wg==,i=0").into_bytes()
        }),
        ("iterations_too_high", |n| {
            format!("r={n}Srv,s=Wg==,i=999999999").into_bytes()
        }),
        ("iterations_overflow", |n| {
            format!("r={n}Srv,s=Wg==,i=99999999999999999999").into_bytes()
        }),
        ("non_numeric_iterations", |n| {
            format!("r={n}Srv,s=Wg==,i=abc").into_bytes()
        }),
        ("missing_iterations", |n| {
            format!("r={n}Srv,s=Wg==").into_bytes()
        }),
        ("missing_salt_and_iters", |n| format!("r={n}Srv").into_bytes()),
        ("field_order_s_before_r", |n| {
            format!("s=Wg==,r={n}Srv,i=4096").into_bytes()
        }),
        ("reserved_mext_mandatory", |n| {
            format!("m=unsupported,r={n}Srv,s=Wg==,i=4096").into_bytes()
        }),
        ("no_r_prefix", |_n| b"x=y,s=Wg==,i=4096".to_vec()),
        ("non_utf8_body", |_n| {
            let mut v = b"r=".to_vec();
            v.push(0xFF);
            v.push(0xFE);
            v.extend_from_slice(b"Srv,s=Wg==,i=4096");
            v
        }),
        ("bare_equals", |_n| b"=".to_vec()),
        ("huge_body", |n| {
            let mut v = format!("r={n}").into_bytes();
            v.extend(std::iter::repeat_n(b'A', 4096));
            v
        }),
    ]
}

/// Crafted adversarial `server-final` messages driven AFTER a valid
/// `server-first` (so each pays exactly one PBKDF2). Every one must classify as
/// `Fail` — a wrong/short/long/absent verifier cannot match the expected
/// signature, and an `e=` reply is a server-reported error — never `Ready`.
fn final_witnesses() -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = vec![
        ("empty".into(), Vec::new()),
        ("no_prefix".into(), b"garbage".to_vec()),
        ("only_v_prefix".into(), b"v=".to_vec()),
        ("v_not_base64".into(), b"v=@@@@".to_vec()),
        ("v_odd_len".into(), b"v=x".to_vec()),
        (
            "v_zero32_wrong_sig".into(),
            b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_vec(),
        ),
        (
            "v_valid32_wrong_sig_plus_ext".into(),
            {
                let mut v = v_witness(&[0x5A; 32]);
                v.extend_from_slice(b",ext=trailing");
                v
            },
        ),
        ("server_error_invalid_proof".into(), b"e=invalid-proof".to_vec()),
        ("server_error_empty".into(), b"e=".to_vec()),
        (
            "server_error_long".into(),
            {
                let mut v = b"e=".to_vec();
                v.extend(std::iter::repeat_n(b'x', 200));
                v
            },
        ),
        ("no_v_no_e".into(), b"x=whatever".to_vec()),
        ("only_comma".into(), b",".to_vec()),
        ("non_utf8".into(), {
            let mut v = b"v=".to_vec();
            v.push(0xFF);
            v.push(0xFE);
            v.push(0xFD);
            v
        }),
    ];
    // Parametric wrong-length verifier family, densely bracketing the 32-byte
    // boundary (every length 0..=33) plus over-length samples. Only length 32
    // reaches the constant-time compare (and fails it, `0xA5` ≠ the real
    // signature); every other length is a structural `MalformedServerFinal`.
    // All → Fail. These lift the engine-level witness count into the dozens the
    // never-Ready invariant needs, at one derivation apiece.
    for len in (0usize..=33).chain([47, 48, 64]) {
        out.push((format!("v_len_{len}"), v_witness(&vec![0xA5u8; len])));
    }
    out
}

#[test]
fn scram_engine_never_reaches_ready_on_crafted_witnesses() {
    // ── Stage 1: adversarial `server-first` → parse-fail before crypto.
    let firsts = first_witnesses();
    let mut first_classified: u32 = 0;
    for (name, build) in &firsts {
        let Some((mut engine, mut sb)) = engine_awaiting_server_first() else {
            panic!("offline SASL setup did not settle for first-witness `{name}`");
        };
        let nonce = extract_client_nonce(sb.pending());
        let nonce_str = core::str::from_utf8(&nonce).unwrap_or("");
        let body = build(nonce_str);
        if !feed(&mut engine, &auth(11, &body)) {
            // Over-cap ingest (the `huge_body` witness may exceed the bounded
            // buffer) — a rejected feed is itself never-Ready. Count it settled.
            first_classified = first_classified.saturating_add(1);
            continue;
        }
        assert!(
            drain_assert_never_ready(&mut engine, &mut sb),
            "adversarial server-first `{name}` must classify as Fail (never Ready, never advance)",
        );
        first_classified = first_classified.saturating_add(1);
    }

    // ── Stage 2: adversarial `server-final` AFTER a valid `server-first`.
    let finals = final_witnesses();
    let mut reached_final: u32 = 0;
    for (name, body) in &finals {
        let Some((mut engine, mut sb)) = engine_awaiting_server_first() else {
            panic!("offline SASL setup did not settle for final-witness `{name}`");
        };
        let nonce = extract_client_nonce(sb.pending());
        assert!(!nonce.is_empty(), "client nonce must be extractable for `{name}`");
        let nonce_str = core::str::from_utf8(&nonce).unwrap_or("");
        // A VALID server-first → the engine derives the client proof (one PBKDF2)
        // and advances to await the server-final.
        let server_first = format!("r={nonce_str}SRVNONCE,s=QSXCR+Q6sek8bf92,i=4096");
        assert!(
            feed(&mut engine, &auth(11, server_first.as_bytes())),
            "valid server-first must feed for `{name}`",
        );
        assert!(
            matches!(engine.next_auth_event(&mut sb), AuthEvent::AuthSaslContinue(_)),
            "valid server-first must reach AuthSaslContinue for `{name}`",
        );
        reached_final = reached_final.saturating_add(1);

        assert!(
            feed(&mut engine, &auth(12, body)),
            "server-final witness `{name}` must feed",
        );
        assert!(
            drain_assert_never_ready(&mut engine, &mut sb),
            "adversarial server-final `{name}` must classify as Fail (never Ready)",
        );
    }

    // ── Teeth: every crafted witness ran, and every final witness ACTUALLY
    // reached the server-final parser (a broken setup that never advanced past
    // server-first would fail here rather than pass vacuously).
    assert_eq!(
        first_classified,
        u32::try_from(firsts.len()).unwrap_or(u32::MAX),
        "every server-first witness must settle",
    );
    assert_eq!(
        reached_final,
        u32::try_from(finals.len()).unwrap_or(u32::MAX),
        "every server-final witness must reach the server-final parser (non-vacuous)",
    );
    assert!(
        firsts.len() >= 16 && finals.len() >= 16,
        "witness tables shrank below their floor ({} first, {} final)",
        firsts.len(),
        finals.len(),
    );
}

// ───────────────────────────────────────────────────────────────────
// Invariant: fully-random bytes never desync into a post-auth state
// ───────────────────────────────────────────────────────────────────

#[test]
fn scram_arbitrary_bytes_never_panic() {
    let mut rng = XorShift64::new(0xFEED_0003);

    for _ in 0..SCRAM_FUZZ_ITERS {
        let Some((mut engine, mut sb)) = engine_awaiting_server_first() else {
            continue;
        };

        let len = rng.len_up_to(200);
        let mut random = vec![0u8; len];
        rng.fill(&mut random);
        if !feed(&mut engine, &random) {
            continue;
        }
        // Random bytes at the await-server-first state must never drive the
        // handshake to Ready (no valid AuthOk + BackendKey + RFQ sequence).
        drain_assert_never_ready(&mut engine, &mut sb);
    }
}
