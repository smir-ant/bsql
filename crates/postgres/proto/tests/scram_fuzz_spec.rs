//! Deterministic fuzz tests for the SCRAM wire parsers, driven through the
//! connecting-phase engine — the production SCRAM dispatch path.
//!
//! # Scope
//!
//! ~5K random adversarial payloads per parser exercise
//! [`scram::wire::parse_server_first`] / [`parse_server_final`] via
//! [`ConnectingEngine::next_auth_event`] (the same code path the live driver
//! drives). Covers the gap a happy-path + hand-crafted-negative-vectors suite
//! alone cannot reach.
//!
//! # Methodology
//!
//! Same xorshift PRNG pattern as `fuzz_stress_spec.rs` (stable-Rust, no
//! nightly / cargo-fuzz requirement). Each iteration drives fresh bytes
//! through a fresh [`ConnectingEngine`]; post-iteration assertions verify the
//! crate invariants:
//!
//! - **No panic reached.** The forbid-bundle bars unwrap/expect/panic in the
//!   parsers, so any panic is a test failure with a backtrace.
//! - **No silent pass.** Adversarial input never drives the handshake to
//!   [`AuthEvent::Ready`] — a single garbage `server-first` / `server-final`
//!   cannot complete the handshake. A parse failure surfaces as
//!   [`AuthEvent::Fail`] (the terminal `ConnPhase::Failed`), classified.
//!
//! [`scram::wire::parse_server_first`]: bsql_postgres_proto::scram
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
    let mut engine = ConnectingEngine::start(&mut sb, &user(), None, None, scram_creds()).ok()?;
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
// Invariant: arbitrary server-final body never panics, never silently passes
// ───────────────────────────────────────────────────────────────────

#[test]
fn scram_server_final_fuzz_never_panics() {
    let mut rng = XorShift64::new(0xBEEF_0002);
    let mut reached_final: u32 = 0;

    for _ in 0..SCRAM_FUZZ_ITERS {
        let Some((mut engine, mut sb)) = engine_awaiting_server_first() else {
            continue;
        };

        // Build a VALID server-first (echo client nonce + a server nonce part +
        // a base64 salt + an RFC-7677-legal iteration count) so the engine
        // parses it and advances to await the server-final.
        let nonce = extract_client_nonce(sb.pending());
        if nonce.is_empty() {
            continue;
        }
        let Ok(nonce_str) = core::str::from_utf8(&nonce) else {
            continue;
        };
        let server_first = format!("r={nonce_str}SRVNONCE,s=QSXCR+Q6sek8bf92,i=4096");
        if !feed(&mut engine, &auth(11, server_first.as_bytes())) {
            continue;
        }
        // A valid server-first parses → AuthSaslContinue (client proof queued).
        if !matches!(
            engine.next_auth_event(&mut sb),
            AuthEvent::AuthSaslContinue(_)
        ) {
            continue;
        }
        reached_final = reached_final.saturating_add(1);

        // Random server-final body → exercises `parse_server_final`.
        let final_len = rng.len_up_to(256);
        let mut final_body = vec![0u8; final_len];
        rng.fill(&mut final_body);
        if !feed(&mut engine, &auth(12, &final_body)) {
            continue;
        }
        // A random server-final cannot produce the expected server signature, so
        // it must never reach Ready (the invariant) — it classifies as Fail.
        drain_assert_never_ready(&mut engine, &mut sb);
    }

    assert!(
        reached_final > 0,
        "the valid-server-first setup must reach the server-final parser",
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
