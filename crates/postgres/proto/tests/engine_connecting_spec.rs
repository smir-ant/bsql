//! Behavioural gate for the connecting-phase dispatch + the `next_auth_event`
//! pull surface.
//!
//! Drives the new [`ConnectingEngine`] the way the flush/ingest pump does:
//! seat the startup for some credentials (queuing it onto a local
//! [`SendBuf`]), feed scripted server auth frames through `read_slot`/`commit`,
//! and pull the connecting events (each queuing any auth response onto the same
//! [`SendBuf`]). Covers the trust success path, the cleartext / MD5
//! password-response paths (the engine queues the `PasswordMessage` and
//! surfaces the auth event), the SCRAM initial-response path, `ParameterStatus`
//! surfacing during post-auth, the `into_active` consuming move (success +
//! still-connecting), and the rejection paths (a trust client cannot satisfy a
//! SASL challenge).
//!
//! The local [`SendBuf`] is never flushed here, so its [`pending`] tail is the
//! full client wire (startup ++ auth responses) — the same bytes the old
//! persistent write buffer accumulated.
//!
//! The byte-exact trust handshake (vs the live engine) is gated in the corpus
//! `differential` test; this gate covers the password paths the trust-only
//! corpus cannot reach.
//!
//! [`ConnectingEngine`]: bsql_postgres_proto::engine::ConnectingEngine
//! [`SendBuf`]: bsql_postgres_proto::engine::SendBuf
//! [`pending`]: bsql_postgres_proto::engine::SendBuf::pending

#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    reason = "integration-test helpers (the feed pump, credential construction) use unwrap as the loud failure signal; clippy's allow-unwrap-in-tests carve-out reaches #[test] fns but not the free helper fns this file factors out"
)]

use bsql_postgres_proto::engine::{AuthEvent, ConnectingEngine, SendBuf};
use bsql_postgres_proto::wire::{
    TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_PARAMETER_STATUS, TAG_READY_FOR_QUERY,
    TAG_SASL_RESPONSE,
};
use bsql_postgres_proto::{Credentials, Ident, Password, Sensitive, TxStatus};

/// The exact byte length of the `user=corpus` startup packet — the offset at
/// which any outbound auth response begins in the client wire.
const STARTUP_LEN: usize = 21;

// ─────────────────────────── frame builders ───────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = (body.len() + 4) as u32;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn auth(sub_code: i32, extra: &[u8]) -> Vec<u8> {
    let mut body = sub_code.to_be_bytes().to_vec();
    body.extend_from_slice(extra);
    frame(TAG_AUTHENTICATION.byte(), &body)
}

fn auth_ok() -> Vec<u8> {
    auth(0, &[])
}

fn backend_key(pid: i32, secret: i32) -> Vec<u8> {
    let mut body = pid.to_be_bytes().to_vec();
    body.extend_from_slice(&secret.to_be_bytes());
    frame(TAG_BACKEND_KEY_DATA.byte(), &body)
}

fn ready_for_query(status: u8) -> Vec<u8> {
    frame(TAG_READY_FOR_QUERY.byte(), &[status])
}

fn parameter_status(key: &str, value: &str) -> Vec<u8> {
    let mut body = key.as_bytes().to_vec();
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    frame(TAG_PARAMETER_STATUS.byte(), &body)
}

// ─────────────────────────── engine drivers ───────────────────────────

fn user() -> Ident {
    Ident::try_from_str("corpus").unwrap()
}

fn password(secret: &str) -> Sensitive<Password> {
    Sensitive::new(Password::try_from_str(secret).unwrap())
}

/// Feed scripted server bytes into the engine's ingest buffer.
fn feed(engine: &mut ConnectingEngine, bytes: &[u8]) {
    let mut fed = 0usize;
    while fed < bytes.len() {
        let remaining = &bytes[fed..];
        let slot = engine.read_slot(remaining.len()).unwrap();
        let n = slot.len().min(remaining.len());
        slot[..n].copy_from_slice(&remaining[..n]);
        engine.commit(n).unwrap();
        fed += n;
    }
}

/// The wire tag of the first outbound auth response (right after the startup
/// packet) in the queued client wire, or `None` when none has been written yet.
fn first_response_tag(send_buf: &SendBuf) -> Option<u8> {
    send_buf.pending().get(STARTUP_LEN).copied()
}

/// Extract the client nonce from the `SASLInitialResponse` the engine queued
/// (mirrors the live SCRAM test): the `'p'` frame body is
/// `mechanism\0 + i32 len + "n,,n=,r=<nonce>"`.
fn extract_client_nonce(client_bytes: &[u8]) -> Vec<u8> {
    let Some(frame) = client_bytes.get(STARTUP_LEN..) else {
        return Vec::new();
    };
    let Some(body) = frame.get(5..) else {
        return Vec::new();
    };
    let mech_end = match body.iter().position(|b| *b == 0) {
        Some(idx) => idx,
        None => body.len(),
    };
    let msg_start = mech_end + 1 + 4;
    let Some(client_first) = body.get(msg_start..) else {
        return Vec::new();
    };
    let Ok(text) = std::str::from_utf8(client_first) else {
        return Vec::new();
    };
    for part in text.split(',') {
        if let Some(nonce) = part.strip_prefix("r=") {
            return nonce.as_bytes().to_vec();
        }
    }
    Vec::new()
}

// ─────────────────────────── tests ───────────────────────────

#[test]
fn trust_handshake_reaches_ready() {
    let mut sb = SendBuf::new();
    let mut engine =
        ConnectingEngine::start(&mut sb, &user(), None, None, Credentials::Trust).unwrap();
    // Startup packet is queued immediately; no auth response yet.
    assert_eq!(sb.pending().len(), STARTUP_LEN);

    feed(&mut engine, &auth_ok());
    feed(&mut engine, &backend_key(4321, 8765));
    feed(&mut engine, &ready_for_query(b'I'));

    assert!(matches!(engine.next_auth_event(&mut sb), AuthEvent::Ready));
    // Trust sends nothing back: the queued client wire stays the startup packet.
    assert_eq!(sb.pending().len(), STARTUP_LEN);

    let active = match engine.into_active() {
        Ok(active) => active,
        Err(_) => panic!("into_active must succeed after Ready"),
    };
    assert_eq!(active.backend_pid(), 4321);
    assert!(matches!(active.tx_status(), TxStatus::Idle));
}

#[test]
fn cleartext_builds_password_message_and_completes() {
    let creds = Credentials::CleartextPassword(password("hunter2"));
    let mut sb = SendBuf::new();
    let mut engine = ConnectingEngine::start(&mut sb, &user(), None, None, creds).unwrap();

    feed(&mut engine, &auth(3, &[]));
    assert!(matches!(
        engine.next_auth_event(&mut sb),
        AuthEvent::AuthCleartext
    ));
    // The PasswordMessage frame (tag 'p') was queued after the startup packet.
    assert_eq!(first_response_tag(&sb), Some(TAG_SASL_RESPONSE.byte()));

    feed(&mut engine, &auth_ok());
    feed(&mut engine, &backend_key(7, 11));
    feed(&mut engine, &ready_for_query(b'T'));
    assert!(matches!(engine.next_auth_event(&mut sb), AuthEvent::Ready));

    let active = match engine.into_active() {
        Ok(active) => active,
        Err(_) => panic!("into_active must succeed after Ready"),
    };
    assert_eq!(active.backend_pid(), 7);
    assert!(matches!(active.tx_status(), TxStatus::InTransaction));
}

#[test]
fn md5_builds_password_message_and_completes() {
    let creds = Credentials::Md5Password(password("hunter2"));
    let mut sb = SendBuf::new();
    let mut engine = ConnectingEngine::start(&mut sb, &user(), None, None, creds).unwrap();

    feed(&mut engine, &auth(5, &[0xde, 0xad, 0xbe, 0xef]));
    assert!(matches!(
        engine.next_auth_event(&mut sb),
        AuthEvent::AuthMd5 { salt } if salt == [0xde, 0xad, 0xbe, 0xef]
    ));
    assert_eq!(first_response_tag(&sb), Some(TAG_SASL_RESPONSE.byte()));

    feed(&mut engine, &auth_ok());
    feed(&mut engine, &backend_key(42, 99));
    feed(&mut engine, &ready_for_query(b'I'));
    assert!(matches!(engine.next_auth_event(&mut sb), AuthEvent::Ready));

    let active = match engine.into_active() {
        Ok(active) => active,
        Err(_) => panic!("into_active must succeed after Ready"),
    };
    assert_eq!(active.backend_pid(), 42);
}

#[test]
fn md5_wrong_length_salt_is_classified_fail() {
    let creds = Credentials::Md5Password(password("hunter2"));
    let mut sb = SendBuf::new();
    let mut engine = ConnectingEngine::start(&mut sb, &user(), None, None, creds).unwrap();
    // A 3-byte salt is malformed (PG §55.4 mandates exactly 4).
    feed(&mut engine, &auth(5, &[0x01, 0x02, 0x03]));
    assert!(matches!(engine.next_auth_event(&mut sb), AuthEvent::Fail(_)));
}

#[test]
fn scram_initial_response_built() {
    let creds = Credentials::ScramPassword(password("hunter2"));
    let mut sb = SendBuf::new();
    let mut engine = ConnectingEngine::start(&mut sb, &user(), None, None, creds).unwrap();

    // AuthenticationSASL offering SCRAM-SHA-256. The client builds the
    // SASLInitialResponse silently and awaits the server-first-message, so the
    // pull yields NeedMore (no surfaceable event yet) while the response lands
    // on the send buffer.
    feed(&mut engine, &auth(10, b"SCRAM-SHA-256\0\0"));
    assert!(matches!(engine.next_auth_event(&mut sb), AuthEvent::NeedMore));
    assert_eq!(first_response_tag(&sb), Some(TAG_SASL_RESPONSE.byte()));
}

#[test]
fn scram_continue_builds_sasl_response() {
    let creds = Credentials::ScramPassword(password("pencil"));
    let mut sb = SendBuf::new();
    let mut engine = ConnectingEngine::start(&mut sb, &user(), None, None, creds).unwrap();
    feed(&mut engine, &auth(10, b"SCRAM-SHA-256\0\0"));
    assert!(matches!(engine.next_auth_event(&mut sb), AuthEvent::NeedMore));
    let init_len = sb.pending().len();
    assert!(init_len > STARTUP_LEN, "SASLInitialResponse must be queued");

    // Simulate the server: echo the client nonce, add a server nonce part, a
    // base64 salt, and an RFC-7677-legal iteration count.
    let nonce = extract_client_nonce(sb.pending());
    assert!(!nonce.is_empty(), "client nonce must be extractable");
    let nonce_str = std::str::from_utf8(&nonce).unwrap();
    let server_first = format!("r={nonce_str}SRVNONCE,s=QSXCR+Q6sek8bf92,i=4096");
    feed(&mut engine, &auth(11, server_first.as_bytes()));

    assert!(matches!(
        engine.next_auth_event(&mut sb),
        AuthEvent::AuthSaslContinue(_)
    ));
    // A second 'p' frame (the SASLResponse carrying the client proof) was
    // queued after the SASLInitialResponse.
    assert!(sb.pending().len() > init_len);
    assert_eq!(
        sb.pending().get(init_len).copied(),
        Some(TAG_SASL_RESPONSE.byte())
    );
}

#[test]
fn scram_final_signature_mismatch_fails() {
    let creds = Credentials::ScramPassword(password("pencil"));
    let mut sb = SendBuf::new();
    let mut engine = ConnectingEngine::start(&mut sb, &user(), None, None, creds).unwrap();
    feed(&mut engine, &auth(10, b"SCRAM-SHA-256\0\0"));
    assert!(matches!(engine.next_auth_event(&mut sb), AuthEvent::NeedMore));
    let nonce = extract_client_nonce(sb.pending());
    let nonce_str = std::str::from_utf8(&nonce).unwrap();
    let server_first = format!("r={nonce_str}SRVNONCE,s=QSXCR+Q6sek8bf92,i=4096");
    feed(&mut engine, &auth(11, server_first.as_bytes()));
    assert!(matches!(
        engine.next_auth_event(&mut sb),
        AuthEvent::AuthSaslContinue(_)
    ));

    // A server-final whose signature cannot match the expected one — the
    // constant-time compare rejects it, fail-closed.
    feed(
        &mut engine,
        &auth(12, b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
    );
    assert!(matches!(engine.next_auth_event(&mut sb), AuthEvent::Fail(_)));
}

#[test]
fn scram_offered_without_supported_mechanism_fails() {
    let creds = Credentials::ScramPassword(password("hunter2"));
    let mut sb = SendBuf::new();
    let mut engine = ConnectingEngine::start(&mut sb, &user(), None, None, creds).unwrap();
    // Server offers only a mechanism the client does not implement.
    feed(&mut engine, &auth(10, b"SCRAM-SHA-256-PLUS\0\0"));
    assert!(matches!(engine.next_auth_event(&mut sb), AuthEvent::Fail(_)));
}

#[test]
fn trust_rejects_sasl_challenge() {
    let mut sb = SendBuf::new();
    let mut engine =
        ConnectingEngine::start(&mut sb, &user(), None, None, Credentials::Trust).unwrap();
    feed(&mut engine, &auth(10, b"SCRAM-SHA-256\0\0"));
    assert!(matches!(engine.next_auth_event(&mut sb), AuthEvent::Fail(_)));
}

#[test]
fn unexpected_frame_during_connect_is_classified_fail() {
    let mut sb = SendBuf::new();
    let mut engine =
        ConnectingEngine::start(&mut sb, &user(), None, None, Credentials::Trust).unwrap();
    // A bare ReadyForQuery before AuthenticationOk is wire-illegal for the
    // startup-trust phase.
    feed(&mut engine, &ready_for_query(b'I'));
    assert!(matches!(engine.next_auth_event(&mut sb), AuthEvent::Fail(_)));
}

#[test]
fn parameter_status_surfaces_during_post_auth() {
    let mut sb = SendBuf::new();
    let mut engine =
        ConnectingEngine::start(&mut sb, &user(), None, None, Credentials::Trust).unwrap();
    feed(&mut engine, &auth_ok());
    feed(&mut engine, &parameter_status("application_name", "demo"));
    feed(&mut engine, &backend_key(1, 2));
    feed(&mut engine, &ready_for_query(b'I'));

    let mut param_payloads: Vec<Vec<u8>> = Vec::new();
    loop {
        match engine.next_auth_event(&mut sb) {
            AuthEvent::Ready => break,
            AuthEvent::ParamStatus(payload) => param_payloads.push(payload.to_vec()),
            AuthEvent::NeedMore => panic!("ran out of bytes before Ready"),
            AuthEvent::Fail(_) => panic!("handshake failed unexpectedly"),
            AuthEvent::AuthCleartext
            | AuthEvent::AuthMd5 { .. }
            | AuthEvent::AuthSaslContinue(_) => {}
        }
    }
    assert_eq!(param_payloads.len(), 1, "exactly one ParameterStatus expected");
    assert_eq!(param_payloads[0], b"application_name\0demo\0");
}

#[test]
fn into_active_before_ready_returns_still_connecting() {
    let mut sb = SendBuf::new();
    let engine =
        ConnectingEngine::start(&mut sb, &user(), None, None, Credentials::Trust).unwrap();
    // No bytes fed: the handshake has not completed.
    match engine.into_active() {
        Ok(_) => panic!("into_active must not succeed before the handshake completes"),
        Err(still) => {
            // The returned connecting engine is intact and re-drivable; the
            // queued startup packet is unchanged on the send buffer.
            assert_eq!(sb.pending().len(), STARTUP_LEN);
            let _still = still;
        }
    }
}

#[test]
fn need_more_when_buffer_drained() {
    let mut sb = SendBuf::new();
    let mut engine =
        ConnectingEngine::start(&mut sb, &user(), None, None, Credentials::Trust).unwrap();
    // Nothing fed yet.
    assert!(matches!(engine.next_auth_event(&mut sb), AuthEvent::NeedMore));
}
