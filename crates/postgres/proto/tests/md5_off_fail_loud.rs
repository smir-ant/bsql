// This witness proves the `md5-auth`-OFF fail-loud contract, so it exists ONLY
// when the `md5-auth` feature is off. Under the default (md5-auth-on) build it
// compiles to an empty binary (no tests). Run it with:
//   cargo test -p bsql-postgres-proto --no-default-features --test md5_off_fail_loud
#![cfg(not(feature = "md5-auth"))]
#![forbid(unsafe_code)]
#![allow(
    clippy::panic,
    clippy::expect_used,
    reason = "offline scripted-transport witness — panic/expect are the loud test-failure signals, not production fallbacks"
)]

//! Fail-loud witness for a build WITHOUT the `md5-auth` feature.
//!
//! With MD5 auth compiled out, `Credentials::Md5Password` cannot be built (it is
//! feature-gated), so a client can never OPT IN to MD5. What remains to prove is
//! the other direction: a server that DEMANDS MD5 (`AuthenticationMD5Password`,
//! sub-code 5) must be rejected LOUD — a classified
//! [`EngineError::Handshake`]`(`[`ConnFail::UnsupportedAuthMethod`]`)` — never a
//! panic, a hang, or a silent stall. The `AuthSubCode::Md5Password` wire
//! classification stays compiled (it is only a decode of the server's sub-code);
//! the always-present dispatch arms map it to `UnsupportedAuthMethod` for a Trust
//! (or cleartext / SCRAM) client. This drives the real public engine
//! [`session`] + [`ConnectingEngine::connect`](bsql_postgres_proto::engine) path
//! over a scripted in-memory server, so no MD5 code (which is not compiled) is
//! named.

use core::convert::Infallible;
use core::future::{ready, Future};

use bsql_postgres_proto::engine::{poll_once, session, ConnFail, EngineError, Transport};
use bsql_postgres_proto::{Credentials, Ident};

/// Build a tagged, length-prefixed wire frame (`tag`, 4-byte BE length that
/// counts itself + body, then body). The `try_from` is infallible for these tiny
/// fixtures; a fixture that somehow overflowed a `u32` length is a loud test
/// failure, never a silently-substituted value.
fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(tag);
    let len = u32::try_from(body.len() + 4).expect("frame body fits a u32 length");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// An `AuthenticationMD5Password` challenge: tag `'R'`, sub-code 5, 4-byte salt.
fn md5_challenge() -> Vec<u8> {
    let mut body = 5_i32.to_be_bytes().to_vec();
    body.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
    frame(b'R', &body)
}

/// Static scripted server: `read` drains a fixed reply; writes are accepted and
/// discarded; every op resolves synchronously (one-poll).
struct StaticServer {
    inbound: Vec<u8>,
    cursor: usize,
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
        let n = (self.inbound.len().saturating_sub(self.cursor)).min(buf.len());
        let end = self.cursor.saturating_add(n);
        if let (Some(dst), Some(src)) = (buf.get_mut(..n), self.inbound.get(self.cursor..end)) {
            dst.copy_from_slice(src);
        }
        self.cursor = end;
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

#[test]
fn trust_client_rejects_an_md5_demanding_server_loud() {
    let user = match Ident::try_from_str("corpus") {
        Ok(user) => user,
        Err(err) => panic!("fixture ident must build: {err:?}"),
    };
    let server = StaticServer {
        inbound: md5_challenge(),
        cursor: 0,
    };
    // A Trust client (no password) — the only credential a server-demanded MD5
    // frame can reach when `md5-auth` is off; the client never opts into MD5. The
    // connect result is classified to a `Result<(), String>` INSIDE the closure
    // so the generative `Live<'b>` (invariant, brand-scoped) never escapes it.
    let verdict = session(server, &user, None, &[], Credentials::Trust, |mut engine, live| {
        match poll_once(engine.connect(live)) {
            // The classified fail-loud: the always-present dispatch answered the
            // MD5 sub-code with `UnsupportedAuthMethod`.
            Ok(Err(EngineError::Handshake(ConnFail::UnsupportedAuthMethod))) => Ok(()),
            Ok(Err(other)) => Err(format!(
                "an MD5-demanding server (md5-auth off) must be \
                 Handshake(UnsupportedAuthMethod), got: {other:?}"
            )),
            Ok(Ok(_live)) => Err(
                "a Trust client MUST NOT complete an MD5 handshake — there is no MD5 code \
                 compiled and no client mechanism to satisfy the challenge"
                    .to_owned(),
            ),
            Err(pending) => Err(format!(
                "the scripted transport resolves synchronously and must not return Pending: \
                 {pending:?}"
            )),
        }
    });

    // session `Ok` (startup packet assembled) → the closure's classification.
    match verdict {
        Ok(Ok(())) => {}
        Ok(Err(message)) => panic!("{message}"),
        Err(conn_fail) => {
            panic!("startup-packet assembly must not fail for Trust: {conn_fail:?}")
        }
    }
}
