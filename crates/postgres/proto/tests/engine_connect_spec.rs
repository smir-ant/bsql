//! Engine-level handshake gate for the [`connect`] verb.
//!
//! Drives [`Engine::connect`] end-to-end over a scripted [`Transport`] — the
//! verb flushes the startup packet, runs the connecting pump to a clean
//! `ReadyForQuery`, and swaps the engine into its active phase — and asserts the
//! engine reaches active (`backend_pid` / `tx_status` readable) on the trust,
//! MD5, and SCRAM-SHA-256 happy paths, that a server error during connect is a
//! classified [`EngineError::Handshake`], and that calling `connect` on an
//! already-active engine is a classified [`EngineError::WrongPhase`].
//!
//! Each transport resolves synchronously, so a `connect` future built over it is
//! always-ready and the body drives it under one [`poll_once`] (mirroring the
//! `engine_pump_spec` scripted-transport style). The byte-exact wire vs the live
//! engine is gated by the corpus `differential`; this gate proves the verb's
//! phase transition and error classification.
//!
//! [`connect`]: bsql_postgres_proto::engine::Engine::connect
//! [`Engine::connect`]: bsql_postgres_proto::engine::Engine::connect
//! [`Transport`]: bsql_postgres_proto::engine::Transport
//! [`poll_once`]: bsql_postgres_proto::engine::poll_once

#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "integration-test helpers (the scripted servers, credential construction, the poll-once driver) use unwrap/expect/panic as the loud failure signal; clippy's allow-in-tests carve-out reaches #[test] fns but not the free helper fns this file factors out"
)]

use core::convert::Infallible;
use core::future::{ready, Future};

use base64ct::{Base64, Encoding};
use bsql_postgres_proto::engine::{poll_once, session, ConnFail, EngineError, Transport};
use bsql_postgres_proto::scram::crypto::compute_client_proof;
use bsql_postgres_proto::wire::{
    TAG_AUTHENTICATION, TAG_BACKEND_KEY_DATA, TAG_ERROR_RESPONSE, TAG_PARAMETER_STATUS,
    TAG_READY_FOR_QUERY,
};
use bsql_postgres_proto::{Credentials, Ident, Password, Sensitive, TxStatus};

// ─────────────────────────── frame builders ───────────────────────────

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let len = u32::try_from(body.len() + 4).expect("frame fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn concat(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in parts {
        out.extend_from_slice(p);
    }
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

fn error_response(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for (tag, text) in [(b'S', severity), (b'C', sqlstate), (b'M', message)] {
        body.push(tag);
        body.extend_from_slice(text.as_bytes());
        body.push(0);
    }
    body.push(0);
    frame(TAG_ERROR_RESPONSE.byte(), &body)
}

// ─────────────────────────── static scripted server ───────────────────────────

/// A transport whose server reply is a fixed byte script (independent of what
/// the client sends): trust / MD5 / server-error handshakes. `read` drains the
/// script; `write`/`flush`/`shutdown` are no-op ready, so a future over it is
/// always-ready.
struct StaticServer {
    inbound: Vec<u8>,
    cursor: usize,
}

impl StaticServer {
    fn new(inbound: Vec<u8>) -> Self {
        Self { inbound, cursor: 0 }
    }
}

impl Transport for StaticServer {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        let n = (self.inbound.len() - self.cursor).min(buf.len());
        buf[..n].copy_from_slice(&self.inbound[self.cursor..self.cursor + n]);
        self.cursor += n;
        ready(Ok(n))
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        ready(Ok(buf.len()))
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

// ─────────────────────────── SCRAM server simulator ───────────────────────────

/// A minimal SCRAM-SHA-256 server: its replies depend on the client's randomly
/// generated nonce, so a static script cannot model them. It records each client
/// frame written and, on each read, emits the next server message computed from
/// the recorded client bytes plus the server's own salt/iterations — the same
/// crypto the engine uses (`compute_client_proof`), so a correct exchange yields
/// a verifiable server signature and the handshake completes.
struct ScramServer {
    password: Vec<u8>,
    /// Each distinct client frame the engine flushed (startup, SASL initial
    /// response, SASL response-with-proof), in order.
    client_frames: Vec<Vec<u8>>,
    /// The current outbound server message and how much has been read.
    out: Vec<u8>,
    out_cursor: usize,
    /// How many server messages have been emitted (the phase counter).
    served: u8,
    /// State carried from server-first to server-final.
    client_first_bare: Vec<u8>,
    server_first: String,
    server_nonce: String,
}

/// The server salt + iteration count (RFC-7677-legal).
const SCRAM_SALT: [u8; 16] = [
    0x5B, 0x6D, 0x99, 0x68, 0x9D, 0x12, 0x35, 0x8E, 0xEC, 0xA0, 0x4B, 0x14, 0x12, 0x36, 0xFA, 0x81,
];
const SCRAM_ITERATIONS: u32 = 4096;
const SCRAM_BACKEND_PID: i32 = 909;

impl ScramServer {
    fn new(password: &str) -> Self {
        Self {
            password: password.as_bytes().to_vec(),
            client_frames: Vec::new(),
            out: Vec::new(),
            out_cursor: 0,
            served: 0,
            client_first_bare: Vec::new(),
            server_first: String::new(),
            server_nonce: String::new(),
        }
    }

    /// Compute the next server message once the current one is fully read.
    fn advance(&mut self) {
        match self.served {
            // Phase 0: respond to the startup with the SASL mechanism offer.
            0 => {
                self.out = auth(10, b"SCRAM-SHA-256\0\0");
            }
            // Phase 1: the client has sent its SASL initial response; build the
            // server-first echoing the client nonce.
            1 => {
                let sasl_initial = &self.client_frames[1];
                let bare = extract_client_first_bare(sasl_initial);
                let client_nonce = extract_nonce(&bare);
                let mut server_nonce = client_nonce;
                server_nonce.push_str("ScramServerSuffix");
                let salt_b64 = encode_b64(&SCRAM_SALT);
                let server_first =
                    format!("r={server_nonce},s={salt_b64},i={SCRAM_ITERATIONS}");
                self.out = auth(11, server_first.as_bytes());
                self.client_first_bare = bare.into_bytes();
                self.server_first = server_first;
                self.server_nonce = server_nonce;
            }
            // Phase 2: the client has sent its proof; verify by computing the
            // expected server signature, then complete the handshake.
            2 => {
                let client_final_without_proof = format!("c=biws,r={}", self.server_nonce);
                let proof = compute_client_proof(
                    &self.password,
                    &SCRAM_SALT,
                    SCRAM_ITERATIONS,
                    &self.client_first_bare,
                    self.server_first.as_bytes(),
                    client_final_without_proof.as_bytes(),
                )
                .expect("compute_client_proof on well-formed inputs");
                let sig_b64 = encode_b64(proof.1.as_bytes());
                let server_final = format!("v={sig_b64}");
                self.out = concat(&[
                    auth(12, server_final.as_bytes()),
                    auth_ok(),
                    backend_key(SCRAM_BACKEND_PID, 4242),
                    ready_for_query(b'I'),
                ]);
            }
            // The handshake is complete; nothing more to send.
            _ => self.out = Vec::new(),
        }
        self.out_cursor = 0;
        self.served = self.served.saturating_add(1);
    }
}

impl Transport for ScramServer {
    type Error = Infallible;
    fn is_would_block(err: &Self::Error) -> bool {
        match *err {}
    }

    fn read<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        if self.out_cursor >= self.out.len() {
            self.advance();
        }
        let n = (self.out.len() - self.out_cursor).min(buf.len());
        buf[..n].copy_from_slice(&self.out[self.out_cursor..self.out_cursor + n]);
        self.out_cursor += n;
        ready(Ok(n))
    }

    fn write<'a>(
        &'a mut self,
        buf: &'a [u8],
    ) -> impl Future<Output = Result<usize, Infallible>> + Send + 'a {
        // Each flush hands one whole client frame; record it for the simulator.
        self.client_frames.push(buf.to_vec());
        ready(Ok(buf.len()))
    }

    fn flush<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }

    fn shutdown<'a>(&'a mut self) -> impl Future<Output = Result<(), Infallible>> + Send + 'a {
        ready(Ok(()))
    }
}

/// Extract the client-first-message-bare (`n=...,r=<nonce>`) from a
/// `SASLInitialResponse` frame (`'p'`, len, mechanism NUL, i32 body_len, body).
fn extract_client_first_bare(frame_bytes: &[u8]) -> String {
    let body = &frame_bytes[5..];
    let mech_end = body.iter().position(|b| *b == 0).expect("mechanism NUL");
    let msg_start = mech_end + 1 + 4;
    let client_first = &body[msg_start..];
    let bare = match client_first.strip_prefix(b"n,,") {
        Some(bare) => bare,
        None => panic!("client-first-message must begin with the gs2 header `n,,`"),
    };
    std::str::from_utf8(bare).expect("client-first-bare is UTF-8").to_string()
}

/// Extract the nonce (`r=<nonce>`) from a client-first-message-bare.
fn extract_nonce(bare: &str) -> String {
    for part in bare.split(',') {
        if let Some(nonce) = part.strip_prefix("r=") {
            return nonce.to_string();
        }
    }
    panic!("client-first-bare must carry an r=<nonce> part");
}

fn encode_b64(bytes: &[u8]) -> String {
    let mut buf = [0u8; 128];
    Base64::encode(bytes, &mut buf)
        .expect("base64 buffer large enough")
        .to_string()
}

// ─────────────────────────── drivers ───────────────────────────
//
// These return the active projection / the verb error directly (not a
// `Result`), so the forbid floor's `unwrap_in_result` lint stays satisfied while
// the panic-class test carve-out covers the `expect`s.

/// Drive `connect` over `transport`, asserting it reaches the active phase, and
/// project the active `(backend_pid, tx_status)`.
fn connect_active<T>(transport: T, creds: Credentials) -> (i32, TxStatus)
where
    T: Transport<Error = Infallible>,
{
    let user = Ident::try_from_str("corpus").expect("ident");
    session(transport, &user, None, &[], creds, |mut engine, live| {
        let _live = poll_once(engine.connect(live))
            .expect("blocking transport resolves in a single poll")
            .expect("handshake reaches active");
        let pid = engine.backend_pid().expect("backend_pid readable once active");
        let tx = engine.tx_status().expect("tx_status readable once active");
        (pid, tx)
    })
    .expect("startup packet assembles")
}

/// Drive `connect` over `transport`, asserting the handshake fails, and return
/// the classified verb error.
fn connect_error<T>(transport: T, creds: Credentials) -> EngineError<Infallible>
where
    T: Transport<Error = Infallible>,
{
    let user = Ident::try_from_str("corpus").expect("ident");
    session(transport, &user, None, &[], creds, |mut engine, live| {
        poll_once(engine.connect(live))
            .expect("blocking transport resolves in a single poll")
            .expect_err("handshake must fail")
    })
    .expect("startup packet assembles")
}

fn scram_password(secret: &str) -> Sensitive<Password> {
    Sensitive::new(Password::try_from_str(secret).expect("password"))
}

// ─────────────────────────── tests ───────────────────────────

/// Trust handshake: `connect` drives `AuthenticationOk` + `BackendKeyData` +
/// `ReadyForQuery` to the active phase; `backend_pid`/`tx_status` read it back.
#[test]
fn trust_connect_reaches_active() {
    let server = StaticServer::new(concat(&[
        auth_ok(),
        backend_key(4321, 8765),
        ready_for_query(b'I'),
    ]));
    assert_eq!(connect_active(server, Credentials::Trust), (4321, TxStatus::Idle));
}

/// Realistic PG handshake tail: `AuthenticationOk`, several `ParameterStatus`
/// frames, `BackendKeyData`, then `ReadyForQuery`. This exercises the
/// `HandshakeProgress::ParamStatus` arm on the shipped `connect` path (every
/// real PG handshake sends `server_version` / `client_encoding` / … before the
/// key), proving the connecting pump keeps pulling past parameter reports and
/// still reaches active.
#[test]
fn trust_connect_with_parameter_status_tail_reaches_active() {
    let server = StaticServer::new(concat(&[
        auth_ok(),
        parameter_status("server_version", "17.2"),
        parameter_status("client_encoding", "UTF8"),
        parameter_status("DateStyle", "ISO, MDY"),
        backend_key(4321, 8765),
        ready_for_query(b'I'),
    ]));
    assert_eq!(connect_active(server, Credentials::Trust), (4321, TxStatus::Idle));
}

/// The connecting engine captures `server_version` from the startup
/// `ParameterStatus` reports and exposes it via `Engine::server_version` once
/// active — the value a `SHOW server_version` would return, recovered for free
/// from the handshake. `client_encoding` / `DateStyle` are sent too but not
/// captured (no consumer), and no `SHOW` is ever issued by the engine.
#[test]
fn connect_captures_server_version_from_handshake() {
    let server = StaticServer::new(concat(&[
        auth_ok(),
        parameter_status("server_version", "17.2 (Debian 17.2-1.pgdg120+1)"),
        parameter_status("client_encoding", "UTF8"),
        parameter_status("DateStyle", "ISO, MDY"),
        backend_key(4321, 8765),
        ready_for_query(b'I'),
    ]));
    let user = Ident::try_from_str("corpus").expect("ident");
    let version = session(server, &user, None, &[], Credentials::Trust, |mut engine, live| {
        let _live = poll_once(engine.connect(live))
            .expect("blocking transport resolves in a single poll")
            .expect("handshake reaches active");
        engine
            .server_version()
            .expect("server_version readable once active")
            .map(str::to_owned)
    })
    .expect("startup packet assembles");
    assert_eq!(version.as_deref(), Some("17.2 (Debian 17.2-1.pgdg120+1)"));
}

/// A handshake with no `server_version` report leaves `server_version` as
/// `None` — honest absence, never a fabricated value (and never a hidden `SHOW`
/// fallback that would resurrect the round-trip).
#[test]
fn connect_without_server_version_report_is_none() {
    let server = StaticServer::new(concat(&[
        auth_ok(),
        parameter_status("client_encoding", "UTF8"),
        backend_key(4321, 8765),
        ready_for_query(b'I'),
    ]));
    let user = Ident::try_from_str("corpus").expect("ident");
    let version = session(server, &user, None, &[], Credentials::Trust, |mut engine, live| {
        let _live = poll_once(engine.connect(live))
            .expect("blocking transport resolves in a single poll")
            .expect("handshake reaches active");
        engine
            .server_version()
            .expect("server_version readable once active")
            .map(str::to_owned)
    })
    .expect("startup packet assembles");
    assert_eq!(version, None);
}

/// MD5 handshake: the server's salt is fixed, so the client's MD5 response is
/// deterministic and the reply is a static script. `connect` reaches active.
#[test]
fn md5_connect_reaches_active() {
    let creds = Credentials::Md5Password(scram_password("hunter2"));
    let server = StaticServer::new(concat(&[
        auth(5, &[0xde, 0xad, 0xbe, 0xef]),
        auth_ok(),
        backend_key(77, 88),
        ready_for_query(b'T'),
    ]));
    assert_eq!(connect_active(server, creds), (77, TxStatus::InTransaction));
}

/// SCRAM-SHA-256 handshake: the simulated server echoes the client's nonce and
/// returns a verifiable server signature, so the full
/// offer→initial→continue→final→ok exchange completes and `connect` reaches
/// active.
#[test]
fn scram_connect_reaches_active() {
    let creds = Credentials::ScramPassword(scram_password("pencil"));
    let server = ScramServer::new("pencil");
    assert_eq!(connect_active(server, creds), (SCRAM_BACKEND_PID, TxStatus::Idle));
}

/// A server `ErrorResponse` during connect is a classified
/// [`EngineError::Handshake`] carrying [`ConnFail::ServerError`] — never a
/// panic, never a silent active transition.
#[test]
fn server_error_during_connect_is_classified_handshake() {
    let server = StaticServer::new(error_response("FATAL", "28000", "role does not exist"));
    let err = connect_error(server, Credentials::Trust);
    assert!(
        matches!(err, EngineError::Handshake(ConnFail::ServerError)),
        "expected Handshake(ServerError), got {err:?}",
    );
}

/// Calling `connect` after the engine is already active is a classified
/// [`EngineError::WrongPhase`], not a re-handshake or a panic.
#[test]
fn connect_when_already_active_is_wrong_phase() {
    let server = StaticServer::new(concat(&[
        auth_ok(),
        backend_key(1, 2),
        ready_for_query(b'I'),
    ]));
    let user = Ident::try_from_str("corpus").expect("ident");
    let err = session(server, &user, None, &[], Credentials::Trust, |mut engine, live| {
        let live = poll_once(engine.connect(live))
            .expect("first connect resolves in one poll")
            .expect("first connect succeeds");
        // The engine is active now; a second connect is out of phase.
        poll_once(engine.connect(live))
            .expect("second connect resolves in one poll")
            .expect_err("connect on an active engine must be WrongPhase")
    })
    .expect("startup packet assembles");
    assert!(matches!(err, EngineError::WrongPhase(_)), "got {err:?}");
}
